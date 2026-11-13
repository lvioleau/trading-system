//! The depth feed — the book's upstream: one reconnecting subscription to the connector's
//! top-N depth stream for the executor's single delivery area (area-shared: one book gates
//! and prices every portfolio's order on a contract), streaming the domain [`Book`]s onto a
//! channel the eval loop reads back through [`DepthBooks`] (this module's [`BookSource`]). The
//! crosser's module, re-cut onto the executor's event queue — the only module that knows both
//! the connector wire types and the domain [`Book`]. One deliberate
//! divergence from the crosser's use of this same feed: the executor reads the **opposite
//! side** of the book, not the mid — the best opposite level's price becomes the IOC limit
//! and its visible size caps the order (see [`Book`]); a mid is logged for cost diagnostics
//! only.
//!
//! **Stream discipline — one book at a time.** Each update the connector sends carries the
//! full top-N view of every book that changed in one frame; the feed splits it and sends the
//! [`Book`]s **one at a time**, seed and deltas alike — each a complete view, nothing to
//! merge. The engine evaluates a contract's pairs against the book the instant it arrives and
//! keeps nothing between books, so there is no seed/delta distinction and no store to seed or
//! clear. The connector already drops deltas with `seq <=` the seed's, so no client-side
//! sequence filtering is needed.
//!
//! **Liveness needs no marker.** A dropped feed simply stops delivering books — and a pair is
//! only ever evaluated when its own book arrives, so during an outage nothing is evaluated
//! and nothing prices off stale depth. There is no disconnect event: [`run`] logs the session
//! end (and escalates on repeated failures) for ops, and the engine learns of the outage only
//! by the books going quiet, which *is* the withhold. The reconnect's stream repopulates
//! everything book by book.
//!
//! **Failure discipline.** A `NOT_FOUND` subscription rejection means the configured
//! delivery area is not the one this connector serves (the request is a guard, not a
//! filter) — a permanent config mismatch, so [`run`] returns it and the boot fails fast.
//! Everything else backs off and reconnects, escalating the log level on repeated failures.
//! A malformed view (no market type) is skipped with a warning: that book simply stays
//! absent, which withholds its orders — the safe direction.

use anyhow::anyhow;
use flow_util::feed::Backoff;
use tokio::sync::mpsc;
use tokio::time::Instant;
use tracing::{error, info, warn};

use eic::EicCode;

use crate::config::FeedConfig;
use crate::executor::{Book, BookSource, ContractKey, Level, MarketType};
use crate::marketdata;
use crate::marketdata::depth_book_service_client::DepthBookServiceClient;

/// How many consecutive failed sessions escalate the log level from warn to error — a
/// connector restart heals in one or two reconnects; anything still failing after this many
/// means no live book exists and no order can send.
const FAILURES_BEFORE_ERROR: u32 = 3;

/// The eval loop's [`BookSource`] over this feed: the receiving end of [`run`]'s channel,
/// drained one book at a time. The blocking receive is what parks the decision thread between
/// market frames. (The produce side is [`run`]; this is the consume side, kept together.)
pub struct DepthBooks {
    books_rx: mpsc::Receiver<Book>,
}

impl DepthBooks {
    pub fn new(books_rx: mpsc::Receiver<Book>) -> Self {
        Self { books_rx }
    }
}

impl BookSource for DepthBooks {
    fn next_book(&mut self) -> Option<Book> {
        self.books_rx.blocking_recv()
    }
}

/// How a subscription session ended.
enum SessionError {
    /// Permanent: the connector serves a different delivery area than configured (or the
    /// event channel closed — the engine is gone). Retrying cannot fix either — [`run`]
    /// returns the error.
    Fatal(anyhow::Error),
    /// Transient: transport failure or terminal stream status — back off and reconnect.
    Retry(anyhow::Error),
}

/// Runs the depth subscription forever, reconnecting with backoff (tuned by [`FeedConfig`])
/// and streaming each [`Book`] into `books_tx`. Returns only on a *fatal* error — the
/// connector rejecting the configured delivery area (wrong connector), or the book channel
/// closing (the engine is gone) — which the caller treats as a boot failure. A session end is
/// logged (there is no disconnect event; the engine withholds simply by the books going
/// quiet) and the loop reconnects.
pub async fn run(
    endpoint: String,
    delivery_area_eic: EicCode,
    tuning: FeedConfig,
    books_tx: mpsc::Sender<Book>,
) -> anyhow::Error {
    let mut backoff = Backoff::new(&tuning);
    let mut consecutive_failures: u32 = 0;
    loop {
        let session_start = Instant::now();
        let result = subscribe(&endpoint, delivery_area_eic, &tuning, &books_tx).await;
        consecutive_failures += 1;
        match result {
            Ok(()) => warn!(endpoint, "depth stream closed by server"),
            Err(SessionError::Fatal(error)) => return error,
            Err(SessionError::Retry(error)) if consecutive_failures >= FAILURES_BEFORE_ERROR => {
                error!(
                    endpoint,
                    consecutive_failures,
                    error = format!("{error:#}"),
                    "depth subscription failing repeatedly; no live book, no order can send"
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

/// One session: connect, subscribe (the area is a guard the connector enforces), then stream
/// each [`Book`] of every update — seed and deltas alike — one at a time until the stream
/// ends. A clean server close returns `Ok(())`; errors split fatal/retry per the module's
/// failure discipline.
async fn subscribe(
    endpoint: &str,
    delivery_area_eic: EicCode,
    tuning: &FeedConfig,
    books_tx: &mpsc::Sender<Book>,
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
        // The area is a guard: NOT_FOUND means this connector serves a different delivery
        // area — a permanent config mismatch, so fail fast instead of retrying into a feed
        // that can never price the executor's contracts.
        Err(status) if status.code() == tonic::Code::NotFound => {
            return Err(SessionError::Fatal(anyhow!(
                "connector rejected the depth subscription for delivery area \
                 {delivery_area_eic}: {} (the executor is pointed at a connector serving a \
                 different area — not retrying)",
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
    // Every update — the session's seed and every later delta alike — is a set of full top-N
    // views; stream each book on its own, the seed is not special.
    loop {
        match stream.message().await {
            Err(status) => {
                return Err(SessionError::Retry(
                    anyhow!(status).context("reading the depth stream"),
                ));
            }
            Ok(None) => return Ok(()),
            Ok(Some(update)) => {
                for book in to_domain_books(update.views) {
                    // An error means the receiver is dropped — the engine is gone, and with
                    // it the process's reason to exist. Fatal, so the boot fails instead of
                    // the subscription spinning on unread books.
                    if books_tx.send(book).await.is_err() {
                        return Err(SessionError::Fatal(anyhow!(
                            "depth book channel closed (the engine is gone)"
                        )));
                    }
                }
            }
        }
    }
}

/// Translate a batch of views, skipping malformed entries (no market type — half the
/// contract identity) with a warning: that book stays absent, which withholds its orders —
/// the safe direction.
fn to_domain_books(views: Vec<marketdata::DepthView>) -> Vec<Book> {
    views
        .into_iter()
        .filter_map(|view| {
            let market_type = match view.market_type() {
                marketdata::MarketType::Local => MarketType::Local,
                marketdata::MarketType::Xbid => MarketType::Xbid,
                marketdata::MarketType::None => {
                    warn!(
                        contract_id = view.contract_id,
                        "skipping depth view with no market type"
                    );
                    return None;
                }
            };
            Some(Book {
                key: ContractKey {
                    market_type,
                    contract_id: view.contract_id,
                },
                bids: view.bids.iter().map(to_domain_level).collect(),
                asks: view.asks.iter().map(to_domain_level).collect(),
            })
        })
        .collect()
}

/// Wire → domain, keeping what gating and pricing need: the level's price (the limit
/// candidate) and its visible quantity (the size cap) — venue-native integers verbatim.
fn to_domain_level(level: &marketdata::Level) -> Level {
    Level {
        price: level.price,
        quantity: level.total_quantity,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn view(market_type: marketdata::MarketType) -> marketdata::DepthView {
        marketdata::DepthView {
            contract_id: 7,
            market_type: market_type as i32,
            revision: 1,
            bids: vec![marketdata::Level {
                price: 4_200,
                total_quantity: 5_000,
                order_count: 2,
                created_at_nanos: 1_000,
                updated_at_nanos: 2_000,
            }],
            asks: vec![],
        }
    }

    #[test]
    fn views_map_onto_domain_books() {
        let books = to_domain_books(vec![view(marketdata::MarketType::Local)]);
        assert_eq!(books.len(), 1);
        assert_eq!(
            books[0].best_bid(),
            Some(&Level {
                price: 4_200,
                quantity: 5_000
            })
        );
        assert_eq!(books[0].best_ask(), None);
    }

    #[test]
    fn a_view_with_no_market_type_is_skipped() {
        // Half the contract identity is missing: the book stays absent (withholds) rather
        // than folding under a guessed key.
        assert!(to_domain_books(vec![view(marketdata::MarketType::None)]).is_empty());
    }
}
