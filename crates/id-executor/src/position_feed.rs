//! The position feed — the gap map's upstream: one reconnecting Position Service
//! `Subscribe(portfolio)` task per configured execution portfolio, each translating its
//! stream into [`PositionEvent`]s on the shared engine channel. The crosser's
//! `position_client.rs` subscription shape minus the `Transfer` caller: the executor is
//! *not* a Position Service writer — its only write path is the venue, and its effect on
//! positions arrives through the gateway's own-trades feed, folded by the Position Service.
//! The only module that knows both the position wire types and the domain [`Position`].
//!
//! **Three events, no special snapshot.** The service streams snapshot-then-deltas, but the
//! feed does not treat the snapshot as special data: every batch — snapshot or delta — is a
//! [`PositionEvent::Positions`], folded identically, each position carrying the write
//! attribution ([`Cause`](crate::executor::Cause)) of the trade that changed it (invariant
//! 6's match key). Liveness is a separate marker:
//!
//! - [`PositionEvent::Online`] is emitted once per session, right *after* the first
//!   (snapshot) batch is forwarded — so "online" means "the baseline has arrived", and the
//!   [`PositionStore`](crate::position_manager::PositionStore) is current before the manager flips
//!   the portfolio live.
//! - [`PositionEvent::Offline`] is emitted on every session end, before the backoff sleep.
//!   The manager clears the portfolio's slice on it (a dropped baseline is stale) and
//!   withholds until the next `Online` — the stale-state interlock.
//!
//! Deduplication is not done here: every position is a full current value and the store
//! keeps the highest revision, absorbing the snapshot/stream overlap and the re-seed after
//! a lag drop (`RESOURCE_EXHAUSTED`) or transport failure. There is no fatal subscription
//! error — an unknown portfolio seeds empty on the service side — so [`run_subscriptions`]
//! returns only when the event channel closes: the engine is gone and the boot should fail.

use anyhow::{Result, anyhow};
use flow_util::feed::Backoff;
use tokio::sync::mpsc;
use tokio::task::JoinSet;
use tokio::time::Instant;
use tracing::{error, info, warn};

use crate::config::FeedConfig;
use crate::executor::{Cause, ContractKey, MarketType, PortfolioId, Position};
use crate::proto;
use crate::proto::position_service_client::PositionServiceClient;

/// How many consecutive failed sessions escalate the log level from warn to error — a
/// service restart heals in one or two reconnects; anything still failing after this many
/// means the gap baseline is going stale and needs eyes.
const FAILURES_BEFORE_ERROR: u32 = 3;

/// One message from a position subscription to the engine.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PositionEvent {
    /// The portfolio's baseline has arrived: emitted once per session, after the first
    /// (snapshot) batch is forwarded. The manager flips the portfolio live — the store is
    /// already current, so from here an absent position means flat. Carries the portfolio
    /// explicitly (an empty snapshot has no position to carry it).
    Online { portfolio: PortfolioId },
    /// A batch of full current positions — snapshot or delta, folded identically, each
    /// carrying its own portfolio and the write attribution of the trade that caused it.
    Positions(Vec<Position>),
    /// The portfolio's session ended (cleanly or not). The manager clears its slice (a
    /// dropped baseline is stale) and withholds all trading on it
    /// (`POSITION_SERVICE_DISCONNECTED`) until the next `Online` re-seeds it.
    Offline { portfolio: PortfolioId },
}

/// Runs one `Subscribe(portfolio)` per configured execution portfolio, forever, each
/// reconnecting with backoff (tuned by [`FeedConfig`]) and sending its events into
/// `events_tx`. Returns only when a subscription hits its one fatal condition — the event
/// channel closed, i.e. the engine is gone — which the caller treats as a boot failure.
pub async fn run_subscriptions(
    endpoint: String,
    portfolios: Vec<PortfolioId>,
    tuning: FeedConfig,
    events_tx: mpsc::Sender<PositionEvent>,
) -> anyhow::Error {
    let mut tasks = JoinSet::new();
    for portfolio in portfolios {
        tasks.spawn(run_one(
            endpoint.clone(),
            portfolio,
            tuning,
            events_tx.clone(),
        ));
    }
    match tasks.join_next().await {
        Some(Ok(error)) => error,
        Some(Err(join_error)) => anyhow!("position subscription task exited: {join_error}"),
        None => anyhow!("no position subscriptions configured"),
    }
}

/// One portfolio's subscription loop: connect, seed, forward updates; back off and
/// resubscribe on any session end (the service's lag policy is drop-and-resubscribe, and a
/// fresh snapshot re-seeds harmlessly — positions fold keep-highest and never vanish).
async fn run_one(
    endpoint: String,
    portfolio: PortfolioId,
    tuning: FeedConfig,
    events_tx: mpsc::Sender<PositionEvent>,
) -> anyhow::Error {
    let mut backoff = Backoff::new(&tuning);
    let mut consecutive_failures: u32 = 0;
    loop {
        let session_start = Instant::now();
        let result = subscribe(&endpoint, &portfolio, &tuning, &events_tx).await;
        // Whatever ended the session, the baseline is now stale: tell the engine before the
        // backoff sleep, so it clears the slice and withholds for the whole gap rather than
        // learning at reconnect. (A closed channel means the engine is gone — the fatal arm
        // below reports it.)
        if events_tx
            .send(PositionEvent::Offline {
                portfolio: portfolio.clone(),
            })
            .await
            .is_err()
        {
            return anyhow!("position event channel closed (the engine is gone)");
        }
        consecutive_failures += 1;
        match result {
            Ok(()) => warn!(endpoint, portfolio, "position stream closed by server"),
            Err(SessionError::Fatal(error)) => return error,
            Err(SessionError::Retry(error)) if consecutive_failures >= FAILURES_BEFORE_ERROR => {
                error!(
                    endpoint,
                    portfolio,
                    consecutive_failures,
                    error = format!("{error:#}"),
                    "position subscription failing repeatedly; the gap baseline is going stale"
                )
            }
            Err(SessionError::Retry(error)) => warn!(
                endpoint,
                portfolio,
                error = format!("{error:#}"),
                "position subscription failed"
            ),
        }
        if backoff.session_ended(session_start.elapsed()) {
            consecutive_failures = 1; // this session's own failure only
        }
        tokio::time::sleep(backoff.next_delay()).await;
    }
}

/// How a subscription session ended.
enum SessionError {
    /// The event channel is gone — the engine stopped. Nothing to reconnect for.
    Fatal(anyhow::Error),
    /// Transport failure, terminal stream status, or a lag drop — back off and resubscribe.
    Retry(anyhow::Error),
}

/// One session: connect, subscribe, send the snapshot as [`PositionEvent::Seeded`], then
/// every batch as [`PositionEvent::Updated`] until the stream ends.
async fn subscribe(
    endpoint: &str,
    portfolio: &str,
    tuning: &FeedConfig,
    events_tx: &mpsc::Sender<PositionEvent>,
) -> Result<(), SessionError> {
    let channel = flow_util::feed::endpoint(endpoint, tuning)
        .map_err(SessionError::Retry)?
        .connect()
        .await
        .map_err(|error| {
            SessionError::Retry(anyhow!(error).context("connecting to the position service"))
        })?;
    let mut client = PositionServiceClient::new(channel);
    // The subscribe itself has no permanent failure mode, so every rejection is a retry: an
    // unknown portfolio is deliberately not an error on the service side (it seeds empty) —
    // a config mismatch here surfaces as a portfolio that never trades, not a crash.
    let mut stream = client
        .subscribe(proto::SubscribeRequest {
            portfolio: portfolio.to_owned(),
        })
        .await
        .map_err(|status| SessionError::Retry(anyhow!(status).context("subscribing to positions")))?
        .into_inner();
    // Emit `Online` once, right after the first (snapshot) batch is forwarded: "online"
    // means the baseline has arrived, so the manager flips live only once the store is
    // current. A reconnect builds a new stream whose first message is a fresh snapshot,
    // folded keep-highest — attribution missed during the gap needs nothing here (the
    // snapshot replays every fold with its attribution).
    let mut announced_online = false;
    loop {
        match stream.message().await {
            // Includes the service's lag policy (`RESOURCE_EXHAUSTED`: this subscriber fell
            // behind the broadcast and was dropped): the recovery for every terminal status
            // is the same — resubscribe and let the fresh snapshot re-seed.
            Err(status) => {
                return Err(SessionError::Retry(
                    anyhow!(status).context("reading the position stream"),
                ));
            }
            // A clean server close (e.g. a graceful drain); the caller reconnects.
            Ok(None) => return Ok(()),
            Ok(Some(update)) => {
                let positions = to_domain_positions(update.positions);
                // The batch first (the store must be current before the portfolio goes
                // live), then the one-time `Online` marker. A send error means the receiver
                // is dropped — the engine is gone, so the boot fails rather than the
                // subscription spinning on unread events. (Backpressure is the non-error
                // path: a full channel parks the send until the engine catches up.)
                if events_tx
                    .send(PositionEvent::Positions(positions))
                    .await
                    .is_err()
                {
                    return Err(SessionError::Fatal(anyhow!(
                        "position event channel closed (the engine is gone)"
                    )));
                }
                if !announced_online {
                    announced_online = true;
                    info!(portfolio, "position baseline online");
                    if events_tx
                        .send(PositionEvent::Online {
                            portfolio: portfolio.to_owned(),
                        })
                        .await
                        .is_err()
                    {
                        return Err(SessionError::Fatal(anyhow!(
                            "position event channel closed (the engine is gone)"
                        )));
                    }
                }
            }
        }
    }
}

/// Translate a batch, skipping malformed entries with a warning — a skipped position only
/// delays the fold until the next update or re-seed; the revision guard keeps it safe.
fn to_domain_positions(positions: Vec<proto::Position>) -> Vec<Position> {
    positions
        .into_iter()
        .filter_map(|position| match to_domain_position(position) {
            Ok(position) => Some(position),
            Err(error) => {
                warn!(
                    error = format!("{error:#}"),
                    "skipping malformed position update"
                );
                None
            }
        })
        .collect()
}

/// Wire → domain, integer units verbatim. The write attribution maps onto [`Cause`]: per
/// the proto contract exactly one of `venue_trade_id` / `transfer_id` is set on every
/// entry, streamed change and snapshot history entry alike — both set is malformed (there
/// is no right side to attribute to, so the entry is skipped rather than guessed), and
/// both empty is tolerated as `Cause: None`. `Err` also covers a missing market type
/// (half the contract identity) or a missing portfolio.
fn to_domain_position(wire: proto::Position) -> Result<Position> {
    let market_type = match wire.market_type() {
        proto::MarketType::Local => MarketType::Local,
        proto::MarketType::Xbid => MarketType::Xbid,
        proto::MarketType::None => {
            anyhow::bail!(
                "position for contract {} carries no market type",
                wire.contract_id
            )
        }
    };
    if wire.portfolio.is_empty() {
        anyhow::bail!(
            "position for contract {} carries no portfolio",
            wire.contract_id
        );
    }
    let cause = match (wire.venue_trade_id.is_empty(), wire.transfer_id.is_empty()) {
        (true, true) => None,
        (false, true) => Some(Cause::Fill {
            venue_trade_id: wire.venue_trade_id,
            cl_order_id: wire.cl_order_id,
        }),
        (true, false) => Some(Cause::Transfer {
            transfer_id: wire.transfer_id,
        }),
        (false, false) => anyhow::bail!(
            "position for contract {} carries both venue_trade_id and transfer_id",
            wire.contract_id
        ),
    };
    Ok(Position {
        portfolio: wire.portfolio,
        contract: ContractKey {
            market_type,
            contract_id: wire.contract_id,
        },
        net_quantity: wire.net_quantity,
        revision: wire.revision,
        cause,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn wire(venue_trade_id: &str, transfer_id: &str) -> proto::Position {
        proto::Position {
            portfolio: "execution-a".to_owned(),
            market_type: proto::MarketType::Local as i32,
            contract_id: 7,
            net_quantity: 10_000,
            revision: 3,
            venue_trade_id: venue_trade_id.to_owned(),
            transfer_id: transfer_id.to_owned(),
            // Carried onto Cause::Fill for the trace; matching still keys on venue_trade_id.
            cl_order_id: "CL-1".to_owned(),
            source: "test".to_owned(),
            executed_at_nanos: 1_000,
        }
    }

    #[test]
    fn attribution_maps_onto_the_cause() {
        // An unattributed entry (tolerated), a fill, and a transfer leg each map cleanly.
        assert_eq!(to_domain_position(wire("", "")).unwrap().cause, None);
        assert_eq!(
            to_domain_position(wire("VT-1", "")).unwrap().cause,
            Some(Cause::Fill {
                venue_trade_id: "VT-1".to_owned(),
                cl_order_id: "CL-1".to_owned()
            })
        );
        assert_eq!(
            to_domain_position(wire("", "c3a1…")).unwrap().cause,
            Some(Cause::Transfer {
                transfer_id: "c3a1…".to_owned()
            })
        );
    }

    #[test]
    fn both_attributions_set_is_malformed() {
        // Exactly one side may be set; both means there is no right side to attribute to —
        // skip (via the batch translator's warn) rather than guess.
        assert!(to_domain_position(wire("VT-1", "c3a1…")).is_err());
    }

    #[test]
    fn missing_identity_is_malformed() {
        let mut no_market_type = wire("", "");
        no_market_type.market_type = proto::MarketType::None as i32;
        assert!(to_domain_position(no_market_type).is_err());

        let mut no_portfolio = wire("", "");
        no_portfolio.portfolio.clear();
        assert!(to_domain_position(no_portfolio).is_err());
    }
}
