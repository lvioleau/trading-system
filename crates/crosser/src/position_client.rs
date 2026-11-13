//! The Position Service clients — the crosser's third upstream and its only downstream in one
//! module: a `Subscribe(portfolio)` task per configured strategy portfolio maintaining the
//! *actual* position baseline, and the `Transfer` caller the crossing loop emits through. The
//! only module that knows both the position wire types and the domain [`Position`].
//!
//! **Subscription discipline.** Snapshot-then-updates, per the service's contract: each session's
//! first message is the portfolio's full current positions (possibly none) and becomes a
//! [`PositionEvent::Seeded`] — the crossing loop's signal that an absent position now means flat,
//! so crossing may proceed. Every later batch is a [`PositionEvent::Updated`]. Deduplication is
//! not done here: every position is a full current value and the domain keeps the highest
//! revision, which absorbs the snapshot/stream overlap and the `Transfer`-ack echo alike. A
//! subscriber dropped for lag (`RESOURCE_EXHAUSTED`) or a transport failure backs off and
//! resubscribes — the fresh snapshot re-seeds. There is no fatal subscription error: an unknown
//! portfolio deliberately seeds empty on the service side (config mismatches surface loudly on
//! the first transfer instead), so [`run_subscriptions`] returns only when the event channel
//! closes — the crossing loop is gone and the boot should fail.
//!
//! **Transfer discipline.** One attempt per call — retry policy belongs to the crossing loop,
//! which reuses the transfer id across resends of the same computed gap (so an ambiguous
//! failure resolves as an idempotent replay, never a double-apply). The outcome split mirrors
//! what the loop must do: apply the ack, refresh-and-recompute on the OCC abort, log a permanent
//! rejection loudly, or hold the gap for the next heartbeat on anything transient.

use anyhow::{Context, Result, anyhow};
use flow_util::feed::Backoff;
use tokio::sync::mpsc;
use tokio::task::JoinSet;
use tokio::time::{Duration, Instant};
use tracing::{error, info, warn};
use uuid::Uuid;

use crate::config::FeedConfig;
use crate::crosser::{ContractKey, MarketType, PortfolioId, Position, TransferIntent};
use crate::proto;
use crate::proto::position_service_client::PositionServiceClient;

/// How many consecutive failed sessions escalate the log level from warn to error — a service
/// restart heals in one or two reconnects; anything still failing after this many means the
/// actual-position baseline is going stale and needs eyes.
const FAILURES_BEFORE_ERROR: u32 = 3;

/// Per-request deadline on `Transfer`: the crossing loop is deliberately serialized, so a hung
/// call would stall every portfolio — bound it and let the loop's resend discipline (same
/// transfer id) resolve whether it committed.
const TRANSFER_TIMEOUT: Duration = Duration::from_secs(10);

/// One message from a position subscription to the crossing loop.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PositionEvent {
    /// A session's snapshot — the portfolio's baseline (or a re-seed after a drop). Marks the
    /// portfolio ready to cross: from here on an absent position means flat.
    Seeded {
        portfolio: PortfolioId,
        positions: Vec<Position>,
    },
    /// The positions one mutation changed — the echoes the crossing loop folds (keep-highest)
    /// and uses to trigger a recompute after an OCC abort.
    Updated(Vec<Position>),
}

/// Runs one `Subscribe(portfolio)` per configured strategy portfolio, forever, each reconnecting
/// with backoff (tuned by [`FeedConfig`]) and sending its events into `events_tx`. Returns only when
/// a subscription hits its one fatal condition — the event channel closed, i.e. the crossing
/// loop is gone — which the caller treats as a boot failure.
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

/// One portfolio's subscription loop: connect, seed, forward updates; back off and resubscribe
/// on any session end (the service's lag policy is drop-and-resubscribe, and a fresh snapshot
/// re-seeds harmlessly — positions fold keep-highest and never vanish).
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
                    "position subscription failing repeatedly; the actual-position baseline \
                     is going stale"
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
    /// The event channel is gone — the crossing loop stopped. Nothing to reconnect for.
    Fatal(anyhow::Error),
    /// Transport failure, terminal stream status, or a lag drop — back off and resubscribe.
    Retry(anyhow::Error),
}

/// One session: connect, subscribe, send the snapshot as [`PositionEvent::Seeded`], then every
/// batch as [`PositionEvent::Updated`] until the stream ends.
///
/// The `Seeded`/`Updated` split rides on the service's stream contract: the first message of a
/// `Subscribe` is always the portfolio's full current positions, everything after is the
/// positions single mutations changed. That first message is the only point where "the crosser
/// heard nothing about a contract" turns into "the portfolio is flat there" — the fact the
/// crossing loop's seed gate needs, and one the message *content* cannot carry (a never-traded
/// portfolio's snapshot is an empty batch; only its position in the stream marks it as the
/// complete baseline).
async fn subscribe(
    endpoint: &str,
    portfolio: &str,
    tuning: &FeedConfig,
    events_tx: &mpsc::Sender<PositionEvent>,
) -> Result<(), SessionError> {
    let channel = connect(endpoint, tuning)
        .await
        .map_err(SessionError::Retry)?;
    let mut client = PositionServiceClient::new(channel);
    // The subscribe itself has no permanent failure mode, so every rejection is a retry: an
    // unknown portfolio is deliberately not an error on the service side (it seeds empty), and
    // a crosser/service portfolio mismatch surfaces loudly on the first transfer instead.
    let mut stream = client
        .subscribe(proto::SubscribeRequest {
            portfolio: portfolio.to_owned(),
        })
        .await
        .map_err(|status| SessionError::Retry(anyhow!(status).context("subscribing to positions")))?
        .into_inner();
    // Tracks the snapshot boundary within *this* session only: a reconnect builds a new stream
    // whose first message is a fresh snapshot, re-sent as another `Seeded` — harmless by design,
    // since seed positions fold exactly like echoes (keep-highest-revision) and positions never
    // vanish (flat still exists), so a re-seed can only refresh, never regress.
    let mut seeded = false;
    loop {
        match stream.message().await {
            // Includes the service's lag policy (`RESOURCE_EXHAUSTED`: this subscriber fell
            // behind the broadcast and was dropped): the recovery for every terminal status is
            // the same — resubscribe and let the fresh snapshot re-seed.
            Err(status) => {
                return Err(SessionError::Retry(
                    anyhow!(status).context("reading the position stream"),
                ));
            }
            // A clean server close (e.g. a graceful drain); the caller reconnects.
            Ok(None) => return Ok(()),
            Ok(Some(update)) => {
                let positions = to_domain_positions(update.positions);
                let event = if seeded {
                    info!(
                        portfolio,
                        positions = positions.len(),
                        "position update received"
                    );
                    PositionEvent::Updated(positions)
                } else {
                    info!(
                        portfolio,
                        positions = positions.len(),
                        "position baseline seeded"
                    );
                    // `Seeded` names the portfolio explicitly because it must survive an empty
                    // snapshot: the event's meaning is "this portfolio's baseline is complete",
                    // and with no positions to read it from, the field is the only carrier.
                    PositionEvent::Seeded {
                        portfolio: portfolio.to_owned(),
                        positions,
                    }
                };
                seeded = true;
                // An error means the receiver is dropped — the crossing loop is gone, and with
                // it the process's reason to exist. Fatal, so the boot fails instead of the
                // subscription spinning on unread events. (Backpressure is the non-error path:
                // a full channel parks this send until the loop catches up.)
                if events_tx.send(event).await.is_err() {
                    return Err(SessionError::Fatal(anyhow!(
                        "position event channel closed; the crossing loop is gone"
                    )));
                }
            }
        }
    }
}

/// How one `Transfer` attempt ended, split by what the crossing loop must do next.
#[derive(Debug)]
pub enum TransferOutcome {
    /// Committed (or an idempotent replay of a commit): the two resulting positions, to fold
    /// back immediately — reconciling against the just-committed actual without waiting for
    /// the `Subscribe` echo.
    Applied { from: Position, to: Position },
    /// The OCC guard tripped: the from position moved since the gap was computed. Refresh from
    /// the subscription, recompute, retry with a fresh id — a recomputed gap is a new intent.
    Aborted { message: String },
    /// `INVALID_ARGUMENT` — a permanent mismatch (a portfolio the service does not accept, per
    /// the config's "logged loudly" contract). Resending the same transfer cannot succeed.
    Rejected { message: String },
    /// Anything else — the service down, the commit unavailable, the contract not yet in the
    /// reference data, or an ambiguous deadline. The gap stands; the next heartbeat resends
    /// with the *same* transfer id, so an attempt that did commit resolves as a replay.
    Failed(anyhow::Error),
}

/// The `Transfer` caller. Built on a lazy channel: nothing connects until the first cross, and
/// tonic re-establishes the connection underneath after outages — the loop's resend discipline
/// handles per-call failures.
pub struct TransferClient {
    client: PositionServiceClient<tonic::transport::Channel>,
    /// Recorded on both legs of every transfer (the ledger's attribution).
    source: String,
}

impl TransferClient {
    pub fn new(endpoint: &str, tuning: &FeedConfig, source: String) -> Result<Self> {
        let channel = flow_util::feed::endpoint(endpoint, tuning)?
            .timeout(TRANSFER_TIMEOUT)
            .connect_lazy();
        Ok(Self {
            client: PositionServiceClient::new(channel),
            source,
        })
    }

    /// One transfer attempt: `intent` under idempotency key `transfer_id`, stamped now.
    pub async fn transfer(
        &mut self,
        transfer_id: Uuid,
        intent: &TransferIntent,
    ) -> TransferOutcome {
        let request = proto::TransferRequest {
            transfer_id: transfer_id.to_string(),
            from_portfolio: intent.from.clone(),
            to_portfolio: intent.to.clone(),
            market_type: to_wire_market_type(intent.contract.market_type) as i32,
            contract_id: intent.contract.contract_id,
            quantity: intent.quantity,
            price: intent.price,
            source: self.source.clone(),
            executed_at_nanos: now_nanos(),
            expected_from_revision: Some(intent.expected_from_revision),
        };
        let response = match self.client.transfer(request).await {
            Ok(response) => response.into_inner(),
            Err(status) => {
                return match status.code() {
                    tonic::Code::Aborted => TransferOutcome::Aborted {
                        message: status.message().to_owned(),
                    },
                    tonic::Code::InvalidArgument => TransferOutcome::Rejected {
                        message: status.message().to_owned(),
                    },
                    _ => TransferOutcome::Failed(anyhow!(status).context("calling Transfer")),
                };
            }
        };
        match parse_ack(response) {
            Ok((from, to)) => TransferOutcome::Applied { from, to },
            // A malformed ack cannot be folded, but the transfer may have committed — treat it
            // as transient: the resend (same id) replays idempotently and re-fetches the ack.
            Err(error) => TransferOutcome::Failed(error.context("parsing the Transfer ack")),
        }
    }
}

/// Both resulting positions from the ack — required: the loop folds `from` immediately so a
/// burst of targets reconciles against the just-committed actual.
fn parse_ack(response: proto::TransferResponse) -> Result<(Position, Position)> {
    let from = response
        .from_position
        .context("ack carries no from_position")?;
    let to = response.to_position.context("ack carries no to_position")?;
    Ok((
        to_domain_position(from).context("ack from_position")?,
        to_domain_position(to).context("ack to_position")?,
    ))
}

/// Translate a batch, skipping malformed entries with a warning — a skipped echo only delays
/// the fold until the next update or heartbeat re-read; the revision guard keeps it safe.
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

/// Wire → domain, integer units verbatim. `Err` means malformed — no market type (half the
/// contract identity) or no portfolio.
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
    Ok(Position {
        portfolio: wire.portfolio,
        contract: ContractKey {
            market_type,
            contract_id: wire.contract_id,
        },
        net_quantity: wire.net_quantity,
        revision: wire.revision,
    })
}

fn to_wire_market_type(market_type: MarketType) -> proto::MarketType {
    match market_type {
        MarketType::Local => proto::MarketType::Local,
        MarketType::Xbid => proto::MarketType::Xbid,
    }
}

async fn connect(endpoint: &str, tuning: &FeedConfig) -> Result<tonic::transport::Channel> {
    flow_util::feed::endpoint(endpoint, tuning)?
        .connect()
        .await
        .context("connecting to the position service")
}

/// Wall-clock now as unix nanos — the transfer's `executed_at` stamp.
fn now_nanos() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|elapsed| elapsed.as_nanos() as i64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn wire(portfolio: &str, market_type: proto::MarketType) -> proto::Position {
        proto::Position {
            portfolio: portfolio.to_owned(),
            market_type: market_type as i32,
            contract_id: 7,
            net_quantity: -10_000,
            revision: 3,
            // The write-attribution fields are ignored by the crosser (it folds by
            // revision, not by cause).
            ..Default::default()
        }
    }

    #[test]
    fn translates_verbatim_integer_units() {
        let position = to_domain_position(wire("strat-batt-1", proto::MarketType::Xbid)).unwrap();
        assert_eq!(
            position,
            Position {
                portfolio: "strat-batt-1".to_owned(),
                contract: ContractKey {
                    market_type: MarketType::Xbid,
                    contract_id: 7,
                },
                net_quantity: -10_000,
                revision: 3,
            }
        );
    }

    #[test]
    fn a_malformed_position_is_skipped_not_fatal() {
        // No market type (half the contract identity) and no portfolio are both unusable; the
        // batch translation drops them and keeps the good entries.
        let batch = to_domain_positions(vec![
            wire("strat-batt-1", proto::MarketType::None),
            wire("", proto::MarketType::Local),
            wire("strat-batt-1", proto::MarketType::Local),
        ]);
        assert_eq!(batch.len(), 1);
        assert_eq!(batch[0].portfolio, "strat-batt-1");
    }
}
