//! The depth feed client: one subscription to the connector's top-N depth stream, folding each
//! update into a shared map of the **entire `DepthView` per contract** — all top-N levels,
//! verbatim. The crossing loop reduces a view to its top of book at cross time (via
//! [`top_of_book`]) and prices from [`TopOfBook::mid`]; keeping the full view costs nothing and
//! leaves the deeper levels available (diagnostics, future sizing) without another subscription.
//!
//! **Stream discipline.** Seed-then-deltas: the first update of a session carries the current
//! view of every book, then each update carries the books one frame changed. Every `DepthView`
//! is full top-N state for its book (not a delta), so the fold is a plain overwrite keyed by
//! contract; the connector already drops deltas with `seq <=` the seed's, so no client-side seq
//! filtering is needed.
//!
//! **Liveness discipline.** A mid must be *live*: pricing a cross off the last mid seen before
//! an outage is exactly what the design forbids. So the map is cleared on the seed (books that
//! vanished during a disconnect must not linger) **and on session end** — while the feed is
//! down there is no top of book at all, every `mid()` is `None`, and the crossing loop
//! withholds; the reconnect's seed repopulates everything at once.
//!
//! **Failure discipline.** Mirrors the strategy feed: a `NOT_FOUND` subscription rejection means
//! the configured delivery area is not the one this connector serves (the request is a guard,
//! not a filter) — a permanent config mismatch, so [`run`] returns it and the boot fails fast.
//! Everything else backs off and reconnects, escalating the log level on repeated failures. A
//! malformed view (no market type) is skipped with a warning: the book simply stays unpriceable,
//! which withholds crossing — the safe direction.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use anyhow::anyhow;
use flow_util::feed::Backoff;
use tokio::time::Instant;
use tracing::{error, info, trace, warn};

use eic::EicCode;

use crate::config::FeedConfig;
use crate::crosser::{ContractKey, MarketType, TopOfBook};
use crate::marketdata;
use crate::marketdata::depth_book_service_client::DepthBookServiceClient;

/// How many consecutive failed sessions escalate the log level from warn to error — a connector
/// restart heals in one or two reconnects; anything still failing after this many means no live
/// mid exists and nothing can cross.
const FAILURES_BEFORE_ERROR: u32 = 3;

/// The live depth per contract — each contract's latest full `DepthView`, verbatim off the wire:
/// the depth feed's one output, read by the crossing loop at cross time (reduced via
/// [`top_of_book`] there, not here). A plain mutex — every access is a brief, non-async read or
/// overwrite. An absent key means "no book" (never seen, or the feed is down), which prices to
/// no mid.
pub type SharedDepth = Arc<Mutex<HashMap<ContractKey, marketdata::DepthView>>>;

/// Reduce a stored view to its top of book — the head of each side, since levels arrive
/// best-first; a side with no resting orders is `None`. This is the read-time boundary where the
/// wire view becomes domain: the crossing loop calls it at cross time and prices from
/// [`TopOfBook::mid`].
pub fn top_of_book(view: &marketdata::DepthView) -> TopOfBook {
    TopOfBook {
        bid: view.bids.first().map(|level| level.price),
        ask: view.asks.first().map(|level| level.price),
    }
}

/// How a subscription session ended.
enum SessionError {
    /// Permanent: the connector serves a different delivery area than configured. Retrying
    /// cannot fix it — [`run`] returns the error.
    Fatal(anyhow::Error),
    /// Transient: transport failure or terminal stream status — back off and reconnect.
    Retry(anyhow::Error),
}

/// Runs the depth subscription forever, reconnecting with backoff (tuned by [`FeedConfig`]) and
/// maintaining `books`. Returns only on a *fatal* error — the connector rejecting the configured
/// delivery area — which the caller should treat as a boot failure and abort.
pub async fn run(
    endpoint: String,
    delivery_area_eic: EicCode,
    tuning: FeedConfig,
    books: SharedDepth,
) -> anyhow::Error {
    let mut backoff = Backoff::new(&tuning);
    let mut consecutive_failures: u32 = 0;
    loop {
        let session_start = Instant::now();
        let result = subscribe(&endpoint, delivery_area_eic, &tuning, &books).await;
        // No live feed means no live mid: forget every book, so the crossing loop withholds
        // instead of pricing off the last pre-outage state. The reconnect's seed re-fills.
        books.lock().expect("depth map poisoned").clear();
        consecutive_failures += 1;
        match result {
            Ok(()) => warn!(endpoint, "depth stream closed by server"),
            Err(SessionError::Fatal(error)) => return error,
            Err(SessionError::Retry(error)) if consecutive_failures >= FAILURES_BEFORE_ERROR => {
                error!(
                    endpoint,
                    consecutive_failures,
                    error = format!("{error:#}"),
                    "depth subscription failing repeatedly; no live mid, nothing can cross"
                )
            }
            Err(SessionError::Retry(error)) => warn!(
                endpoint,
                error = format!("{error:#}"),
                "depth subscription failed"
            ),
        }
        if backoff.session_ended(session_start.elapsed()) {
            consecutive_failures = 1; // this session's own failure only
        }
        tokio::time::sleep(backoff.next_delay()).await;
    }
}

/// One session: connect, subscribe (the area is a guard the connector enforces), then fold
/// updates until the stream ends. A clean server close returns `Ok(())`; errors are split
/// fatal/retry per the module's failure discipline.
async fn subscribe(
    endpoint: &str,
    delivery_area_eic: EicCode,
    tuning: &FeedConfig,
    books: &SharedDepth,
) -> Result<(), SessionError> {
    let channel = flow_util::feed::endpoint(endpoint, tuning)
        .map_err(SessionError::Retry)?
        .connect()
        .await
        .map_err(|error| {
            SessionError::Retry(anyhow!(error).context("connecting to the connector"))
        })?;
    let mut client = DepthBookServiceClient::new(channel);
    let mut stream = match client
        .subscribe(marketdata::SubscribeDepthRequest {
            eic_code: delivery_area_eic.to_string(),
        })
        .await
    {
        Ok(response) => response.into_inner(),
        // The area is a guard: NOT_FOUND means this connector serves a different delivery area
        // — a permanent config mismatch, so fail fast instead of retrying into never pricing.
        Err(status) if status.code() == tonic::Code::NotFound => {
            return Err(SessionError::Fatal(anyhow!(
                "connector rejected the depth subscription for delivery area {delivery_area_eic}: \
                 {} (the crosser is pointed at a connector serving a different area — not \
                 retrying)",
                status.message()
            )));
        }
        Err(status) => {
            return Err(SessionError::Retry(
                anyhow!(status).context("subscribing to the depth stream"),
            ));
        }
    };
    info!(endpoint, %delivery_area_eic, "subscribed to depth");
    // The first update of a session is the seed: the full current view of every book. Fold it
    // over an empty map so books that vanished while disconnected do not linger.
    let mut seeded = false;
    loop {
        match stream.message().await {
            Err(status) => {
                return Err(SessionError::Retry(
                    anyhow!(status).context("reading the depth stream"),
                ));
            }
            Ok(None) => return Ok(()),
            Ok(Some(update)) => {
                fold(update, !seeded, books);
                seeded = true;
            }
        }
    }
}

/// Fold one update's views into the shared map — a plain overwrite per contract, since each view
/// is the full top-N state of its book, stored verbatim. `reset` (the session's seed) replaces
/// the whole map.
fn fold(update: marketdata::DepthUpdate, reset: bool, books: &SharedDepth) {
    let mut books = books.lock().expect("depth map poisoned");
    if reset {
        books.clear();
    }
    for view in update.views {
        let market_type = match view.market_type() {
            marketdata::MarketType::Local => MarketType::Local,
            marketdata::MarketType::Xbid => MarketType::Xbid,
            // Half the contract identity is missing: skip the view. The book just stays
            // unpriceable, which withholds crossing — the safe direction.
            marketdata::MarketType::None => {
                warn!(
                    contract_id = view.contract_id,
                    "skipping depth view with no market type"
                );
                continue;
            }
        };
        let key = ContractKey {
            market_type,
            contract_id: view.contract_id,
        };
        let top = top_of_book(&view);
        trace!(
            contract = %key,
            bid = top.bid,
            ask = top.ask,
            bid_levels = view.bids.len(),
            ask_levels = view.asks.len(),
            seq = update.seq,
            "depth view updated"
        );
        books.insert(key, view);
    }
}
