//! The strategy feed client: one subscription to `SubscribeTargets(strategy, portfolios)` for
//! exactly the configured strategy portfolios, reconnecting with backoff (the services' feed
//! shape). The only module that knows both the strategy wire types and the domain
//! [`TargetRecord`].
//!
//! **Stream discipline.** Continuous records, no seed and no snapshot — each record is
//! self-contained, and the crossing loop reconciles the actual position to the *latest* target,
//! so a gap during an outage self-heals on the next record for that contract (bounded by the
//! strategy's heartbeat). There is nothing to replay on reconnect.
//!
//! **Failure discipline.** Two distinct modes, deliberately kept apart:
//! - A **subscription rejection** (`INVALID_ARGUMENT` — the strategy does not produce a requested
//!   portfolio) is a permanent config mismatch. Retrying cannot fix it, so [`run`] returns the
//!   error and the process fails fast — at boot or on the reconnect that first hits it — instead
//!   of silently never crossing.
//! - A **transport error** or terminal stream status (strategy down, restarting, subscriber
//!   dropped for lag) is transient: back off and reconnect, escalating the log level when
//!   sessions fail repeatedly.
//!
//! A **malformed record** (no market type, no portfolio) is skipped with a warning, not fatal:
//! targets are absolute, so the next good record for that contract supersedes whatever the bad
//! one carried — unlike a dropped fill, nothing is lost. And distinct from the transport
//! keepalive, the strategy's heartbeat doubles as a **feed-liveness signal**: a live stream that
//! delivers nothing for several heartbeat intervals trips a stalled-feed warning.
//!
//! Every translated record is logged in full at the boundary — the crosser has no store, so the
//! structured log *is* the input audit trail.

use anyhow::{Result, anyhow, bail};
use flow_util::feed::Backoff;
use tokio::sync::mpsc;
use tokio::time::{Duration, Instant};
use tracing::{error, info, warn};

use crate::config::Config;
use crate::crosser::{ContractKey, MarketType, TargetRecord};
use crate::proto;
use crate::proto::strategy_service_client::StrategyServiceClient;

/// How many consecutive failed sessions escalate the log level from warn to error — a strategy
/// restart heals in one or two reconnects; anything still failing after this many means targets
/// are not arriving and needs eyes.
const FAILURES_BEFORE_ERROR: u32 = 3;

/// The stalled-feed tripwire: a live stream that delivers no record at all for this long — three
/// nominal ~30s heartbeat intervals — is warned about. The transport keepalive catches a dead
/// connection; this catches a connected strategy that has stopped emitting.
const STALE_AFTER: Duration = Duration::from_secs(90);

/// How a subscription session ended.
enum SessionError {
    /// Permanent: the subscription was rejected (config/strategy portfolio mismatch) or the
    /// target channel is gone. Retrying cannot fix it — [`run`] returns the error.
    Fatal(anyhow::Error),
    /// Transient: transport failure or terminal stream status — back off and reconnect.
    Retry(anyhow::Error),
}

/// Runs the strategy subscription forever, reconnecting with backoff (tuned by the config's
/// [`FeedConfig`](crate::config::FeedConfig)) and sending each translated record into
/// `targets_tx`.
/// Returns only on a *fatal* error — a subscription rejection (permanent config mismatch) or a
/// closed target channel — which the caller should treat as a boot failure and abort.
pub async fn run(config: Config, targets_tx: mpsc::Sender<TargetRecord>) -> anyhow::Error {
    let tuning = config.feed;
    let mut backoff = Backoff::new(&tuning);
    let mut consecutive_failures: u32 = 0;
    loop {
        let session_start = Instant::now();
        let result = subscribe(&config, &targets_tx).await;
        consecutive_failures += 1;
        match result {
            Ok(()) => warn!(
                endpoint = config.strategy_endpoint,
                "strategy stream closed by server"
            ),
            Err(SessionError::Fatal(error)) => return error,
            Err(SessionError::Retry(error)) if consecutive_failures >= FAILURES_BEFORE_ERROR => {
                error!(
                    endpoint = config.strategy_endpoint,
                    consecutive_failures,
                    error = format!("{error:#}"),
                    "strategy subscription failing repeatedly; targets are not arriving"
                )
            }
            Err(SessionError::Retry(error)) => warn!(
                endpoint = config.strategy_endpoint,
                error = format!("{error:#}"),
                "strategy subscription failed"
            ),
        }
        if backoff.session_ended(session_start.elapsed()) {
            consecutive_failures = 1; // this session's own failure only
        }
        tokio::time::sleep(backoff.next_delay()).await;
    }
}

/// One session: connect, subscribe for the configured portfolios, then translate and forward
/// records until the stream ends. A clean server close returns `Ok(())` (reconnect, fresh
/// session); errors are split fatal/retry per the module's failure discipline.
async fn subscribe(
    config: &Config,
    targets_tx: &mpsc::Sender<TargetRecord>,
) -> Result<(), SessionError> {
    let channel = flow_util::feed::endpoint(&config.strategy_endpoint, &config.feed)
        .map_err(SessionError::Retry)?
        .connect()
        .await
        .map_err(|error| {
            SessionError::Retry(anyhow!(error).context("connecting to the strategy"))
        })?;
    let mut client = StrategyServiceClient::new(channel);
    let portfolios: Vec<String> = config.portfolios.iter().map(|p| p.name.clone()).collect();
    let request = proto::SubscribeTargetsRequest {
        strategy: config.strategy.clone(),
        portfolios: portfolios.clone(),
    };
    let mut stream = match client.subscribe_targets(request).await {
        Ok(response) => response.into_inner(),
        // The strategy does not produce a requested portfolio: a permanent config mismatch —
        // fail fast instead of retrying into silently-never-crossing.
        Err(status) if status.code() == tonic::Code::InvalidArgument => {
            return Err(SessionError::Fatal(anyhow!(
                "strategy {:?} rejected the subscription for portfolios {:?}: {} \
                 (crosser config / strategy portfolio mismatch — not retrying)",
                config.strategy,
                portfolios,
                status.message()
            )));
        }
        Err(status) => {
            return Err(SessionError::Retry(
                anyhow!(status).context("subscribing to strategy targets"),
            ));
        }
    };
    info!(
        endpoint = config.strategy_endpoint,
        strategy = config.strategy,
        portfolios = ?portfolios,
        "subscribed to strategy targets"
    );
    loop {
        match tokio::time::timeout(STALE_AFTER, stream.message()).await {
            // The liveness tripwire: the connection is alive (keepalive would have erred) but
            // the strategy has emitted nothing for several heartbeat intervals. Warn and keep
            // waiting — reconnecting would not conjure records from a stalled strategy.
            Err(_elapsed) => warn!(
                stale_secs = STALE_AFTER.as_secs(),
                "no target record for several heartbeat intervals; strategy feed may be stalled"
            ),
            Ok(Err(status)) => {
                return Err(SessionError::Retry(
                    anyhow!(status).context("reading the target stream"),
                ));
            }
            Ok(Ok(None)) => return Ok(()),
            Ok(Ok(Some(wire))) => match to_domain(wire) {
                Ok(record) => {
                    log_record(&record);
                    if targets_tx.send(record).await.is_err() {
                        return Err(SessionError::Fatal(anyhow!(
                            "target channel closed; the crossing loop is gone"
                        )));
                    }
                }
                // Skipped, not fatal: the target is absolute, so the next good record for the
                // contract supersedes whatever this one carried — nothing is lost, unlike a
                // dropped fill.
                Err(error) => warn!(
                    error = format!("{error:#}"),
                    "skipping malformed target record"
                ),
            },
        }
    }
}

/// Translate one wire record into the domain, integer units verbatim. `Err` means malformed —
/// no market type (half the contract identity) or no portfolio (unmappable) — and the caller
/// skips it.
fn to_domain(wire: proto::TargetRecord) -> Result<TargetRecord> {
    let market_type = match wire.market_type() {
        proto::MarketType::Local => MarketType::Local,
        proto::MarketType::Xbid => MarketType::Xbid,
        proto::MarketType::None => {
            bail!(
                "target for contract {} carries no market type",
                wire.contract_id
            )
        }
    };
    if wire.portfolio.is_empty() {
        bail!(
            "target for contract {} carries no portfolio",
            wire.contract_id
        );
    }
    Ok(TargetRecord {
        portfolio: wire.portfolio,
        contract: ContractKey {
            market_type,
            contract_id: wire.contract_id,
        },
        target_quantity: wire.target_quantity,
        current_quantity: wire.current_quantity,
        delta_quantity: wire.delta_quantity,
        mid_price: wire.mid_price,
    })
}

/// The input audit record: every target received is logged in full — the crosser keeps no store,
/// so the structured log is what makes a run auditable. The strategy's `current`/`delta`/`mid`
/// are its own booking (logged, never acted on); the live mid joins the log at step 4.
fn log_record(record: &TargetRecord) {
    info!(
        portfolio = record.portfolio,
        contract = %record.contract,
        target_kw = record.target_quantity,
        strategy_current_kw = record.current_quantity,
        strategy_delta_kw = record.delta_quantity,
        strategy_mid = record.mid_price,
        "target record received"
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    fn wire(portfolio: &str, market_type: proto::MarketType) -> proto::TargetRecord {
        proto::TargetRecord {
            portfolio: portfolio.to_owned(),
            market_type: market_type as i32,
            contract_id: 7,
            target_quantity: -10_000,
            current_quantity: -9_500,
            delta_quantity: -500,
            mid_price: 4_200,
        }
    }

    #[test]
    fn translates_verbatim_integer_units() {
        let record = to_domain(wire("strat-batt-1", proto::MarketType::Xbid)).unwrap();
        assert_eq!(
            record,
            TargetRecord {
                portfolio: "strat-batt-1".to_owned(),
                contract: ContractKey {
                    market_type: MarketType::Xbid,
                    contract_id: 7,
                },
                target_quantity: -10_000,
                current_quantity: -9_500,
                delta_quantity: -500,
                mid_price: 4_200,
            }
        );
    }

    #[test]
    fn missing_market_type_is_malformed() {
        // Half the contract identity — the record cannot be keyed.
        assert!(to_domain(wire("strat-batt-1", proto::MarketType::None)).is_err());
    }

    #[test]
    fn missing_portfolio_is_malformed() {
        // Unmappable to an execution portfolio.
        assert!(to_domain(wire("", proto::MarketType::Local)).is_err());
    }
}
