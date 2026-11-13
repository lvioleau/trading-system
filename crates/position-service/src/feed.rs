//! Gateway feed clients: one own-trades task per configured gateway, each folding that
//! gateway's executions into the ledger — writing every batch through to Postgres, applying
//! it to the shared [`PositionLedger`](crate::ledger::PositionLedger), and broadcasting the
//! changed positions — durable before visible, in that order. The only module that knows
//! both the gateway wire types and the domain trade.
//!
//! Own-trades is the *only* stream this service folds: fills are fills. What a fill is —
//! including shadow executions, which a PowerBot shadow instance reports as *internal*
//! trades — is the gateway's concern; a shadow gateway normalizes those onto its own-trades
//! stream before they reach us (docs/powerbot-gateway/design.md, "Shadow instances").
//! Each gateway is its own task with its own reconnect loop, so one gateway's outage never
//! disturbs the others.
//!
//! **Stream discipline.** The gateway streams are seed-then-deltas with a per-stream `seq`:
//! the first frame is a snapshot (folded — it may carry fills executed while this service
//! was down), and only frames with `seq > seed_seq` are forwarded after it; the deterministic
//! fill ids make any residual overlap a no-op anyway. Any terminal status means reconnect,
//! which re-seeds.
//!
//! **Failure discipline.** A failed Postgres append fails the whole session and reconnects:
//! nothing was made visible, and the re-seed replays the batch against the idempotent ids.
//! Unlike the contract feed, an FK rejection here (a fill racing the contract sync) is
//! *expected to heal* — the contract service catches up and the replay lands — so
//! reconnect-and-replay is the recovery mechanism, not just a tripwire. A poison frame that
//! keeps failing surfaces as escalating logs: session failures are warned, and repeated
//! consecutive failures of one subscription are errors (a dropped fill would be silent
//! position loss, so the feed never skips a failing batch — it retries forever, loudly).
//!
//! Quantities and prices arrive in the venue-native integer encoding the domain and the API
//! already use (kW, minor currency units per MWh), so fills fold verbatim — no conversion
//! anywhere. The wire carries `market_type` and the bare integer `contract_id`, mapping
//! directly onto [`ContractKey`]; the composite FK validates the contract at write time.

use anyhow::{Context, Result, bail};
use flow_util::feed::Backoff;
use tokio::time::Instant;
use tokio_stream::{Stream, StreamExt};
use tracing::{debug, error, info, warn};

use crate::channel::{PositionChange, PositionSender, PositionUpdate, SharedLedger, log_position};
use crate::config::{FeedConfig, GatewayConfig};
use crate::gateway;
use crate::gateway::powerbot_execution_service_client::PowerbotExecutionServiceClient;
use crate::ledger::{self, ContractKey, MarketType, Trade};
use crate::storage::PositionStorage;

/// The exchange this deployment folds — the wire `Trade.exchange` must match, because the
/// storage layer stamps every row with its own venue constant (see `storage::VENUE`) and a
/// mismatched exchange would be recorded under the wrong venue.
const EXCHANGE: &str = "nordpool";

/// How many consecutive failed sessions of one subscription escalate the log level from
/// warn to error — the "repeated failure" tripwire (an FK race heals in one or two
/// reconnects; anything still failing after this many is stuck on a poison batch or a dead
/// gateway and needs eyes).
const FAILURES_BEFORE_ERROR: u32 = 3;

/// One translated stream frame: the stream `seq` and the batch's domain fills.
struct Frame {
    seq: u64,
    fills: Vec<Trade>,
}

/// Runs one gateway's own-trades subscription forever: connect, subscribe, fold (Postgres,
/// then ledger, then broadcast); on stream end or error — including a failed database
/// write — back off and reconnect (tuned by [`FeedConfig`]). Callers spawn one `run` per
/// gateway.
///
/// `storage: None` runs memory-only — the mock-gateway tests; the service always writes
/// through.
pub async fn run(
    config: GatewayConfig,
    tuning: FeedConfig,
    storage: Option<PositionStorage>,
    ledger: SharedLedger,
    updates: PositionSender,
) {
    let mut backoff = Backoff::new(&tuning);
    let mut consecutive_failures: u32 = 0;
    loop {
        let session_start = Instant::now();
        let result = subscribe(&config, &tuning, storage.as_ref(), &ledger, &updates).await;
        consecutive_failures += 1;
        match result {
            Ok(()) => warn!(gateway = config.name, "gateway stream closed by server"),
            Err(error) if consecutive_failures >= FAILURES_BEFORE_ERROR => error!(
                gateway = config.name,
                consecutive_failures,
                error = format!("{error:#}"),
                "gateway subscription failing repeatedly; fills are not folding"
            ),
            Err(error) => warn!(
                gateway = config.name,
                error = format!("{error:#}"),
                "gateway subscription failed"
            ),
        }
        if backoff.session_ended(session_start.elapsed()) {
            consecutive_failures = 1; // this session's own failure only
        }
        tokio::time::sleep(backoff.next_delay()).await;
    }
}

async fn subscribe(
    config: &GatewayConfig,
    tuning: &FeedConfig,
    storage: Option<&PositionStorage>,
    ledger: &SharedLedger,
    updates: &PositionSender,
) -> Result<()> {
    let channel = flow_util::feed::endpoint(&config.endpoint, tuning)?
        .connect()
        .await?;
    let mut client = PowerbotExecutionServiceClient::new(channel);
    // The subscribe RPC requires the portfolio scope: the gateway validates it against its
    // own configured portfolio and rejects a mismatch with INVALID_ARGUMENT — a config-drift
    // check between this service and the gateway, surfacing as a loud subscribe failure
    // instead of misattributed fills. Map each wire update to a translated Frame at the
    // boundary.
    let stream = client
        .subscribe_own_trades(gateway::SubscribeOwnTradesRequest {
            portfolio_id: config.powerbot_portfolio_id.clone(),
        })
        .await?
        .into_inner();
    info!(gateway = config.name, "subscribed to own-trades");
    let frames = stream.map(|update| {
        let update = update?;
        Ok(Frame {
            seq: update.seq,
            fills: own_trade_fills(&update.trades, config)?,
        })
    });
    drive(frames, config, storage, ledger, updates).await
}

/// The session loop: fold the seed (the first frame — it may carry fills executed while
/// this service was down), then only deltas with `seq > seed_seq`, dropping the seed/delta
/// overlap per the gateway's stream contract.
async fn drive(
    mut frames: impl Stream<Item = Result<Frame>> + Unpin,
    config: &GatewayConfig,
    storage: Option<&PositionStorage>,
    ledger: &SharedLedger,
    updates: &PositionSender,
) -> Result<()> {
    let mut seed_seq = None;
    while let Some(frame) = frames.next().await {
        let frame = frame?;
        match seed_seq {
            None => seed_seq = Some(frame.seq),
            Some(seed) if frame.seq <= seed => {
                debug!(
                    gateway = config.name,
                    seq = frame.seq,
                    seed,
                    "dropping seed/delta overlap"
                );
                continue;
            }
            Some(_) => {}
        }
        apply(frame, config, storage, ledger, updates).await?;
    }
    Ok(())
}

/// Fold one batch: Postgres, then ledger, then broadcast — durable before visible. A failed
/// write fails the session; nothing was made visible, and the reconnect re-seed replays the
/// batch against the idempotent ids (fills already folded re-apply as no-ops).
async fn apply(
    frame: Frame,
    config: &GatewayConfig,
    storage: Option<&PositionStorage>,
    ledger: &SharedLedger,
    updates: &PositionSender,
) -> Result<()> {
    if frame.fills.is_empty() {
        return Ok(());
    }
    if let Some(storage) = storage {
        storage
            .append(&frame.fills)
            .await
            .context("writing fill batch through to postgres")?;
    }
    let received = frame.fills.len();
    let changed: Vec<PositionChange> = {
        let mut ledger = ledger.write().await;
        frame
            .fills
            .into_iter()
            .filter_map(|fill| match ledger.apply(fill.clone()) {
                // Record every folded fill in full, then the position it produced — the
                // trade log is the system of record, so each fill that moves a position is
                // logged entirely (identity, contract, signed quantity, price, attribution)
                // and the resulting position is logged after the update.
                Some(position) => {
                    log_fill(config, frame.seq, &fill);
                    log_position(&position);
                    Some(PositionChange::new(&fill, position))
                }
                // A replay (already-applied id) is a deliberate no-op, not a lost fill — note
                // it at debug so the audit trail still shows the fill was seen.
                None => {
                    debug!(
                        gateway = config.name,
                        seq = frame.seq,
                        %fill,
                        "skipping already-applied fill (replay)"
                    );
                    None
                }
            })
            .collect()
    };
    debug!(
        gateway = config.name,
        seq = frame.seq,
        received,
        changed = changed.len(),
        "applied fill batch"
    );
    if !changed.is_empty() {
        // Ledger written above, broadcast second (the snapshot-then-updates ordering rule).
        // Send only errs when no subscriber is listening, which is fine.
        let _ = updates.send(PositionUpdate { changes: changed });
    }
    Ok(())
}

/// Log one folded fill entirely — every field of the domain [`Trade`]. This is the per-fill
/// audit record: the trade log is the system of record, so a fill that changes a position is
/// never summarized away. The position the fill produced is logged separately (via
/// [`log_position`]) right after.
fn log_fill(config: &GatewayConfig, seq: u64, fill: &Trade) {
    info!(
        gateway = config.name,
        seq,
        %fill,
        "folded fill"
    );
}

/// Translate one batch of wire own-trades into domain fills. A malformed trade fails the
/// batch (and with it the session): the alternative is dropping it, and a dropped fill is
/// silent position loss — reconnect-and-replay keeps failing loudly until someone looks.
/// The one deliberate skip is a self-trade (both legs ours): it nets to zero on the single
/// portfolio this gateway folds into, so skipping *is* the correct fold — logged loudly,
/// since IOC-only order entry should never self-match (the gateway alarms on it too).
fn own_trade_fills(trades: &[gateway::Trade], config: &GatewayConfig) -> Result<Vec<Trade>> {
    let mut fills = Vec::with_capacity(trades.len());
    for trade in trades {
        if trade.self_trade {
            warn!(
                gateway = config.name,
                trade_id = trade.trade_id,
                "skipping self-trade (both legs ours): nets to zero, should never occur"
            );
            continue;
        }
        validate_identity(&trade.trade_id, &trade.exchange)?;
        let quantity = match trade.side() {
            gateway::OrderSide::Buy => trade.quantity,
            gateway::OrderSide::Sell => -trade.quantity,
            gateway::OrderSide::None => {
                bail!("own trade {:?} carries no side", trade.trade_id)
            }
        };
        fills.push(Trade {
            id: ledger::fill_id(&trade.exchange, &trade.trade_id),
            portfolio: config.portfolio.clone(),
            contract: contract_key(trade.market_type(), trade.contract_id, &trade.trade_id)?,
            quantity,
            price: trade.price,
            executed_at_nanos: trade.exec_time_nanos,
            venue_trade_id: Some(trade.trade_id.clone()),
            transfer_id: None,
            // The order's client order id (nonce), echoed onto the trade; an empty value
            // means the order carried none.
            cl_order_id: (!trade.cl_order_id.is_empty()).then(|| trade.cl_order_id.clone()),
            source: config.name.clone(),
        });
    }
    Ok(fills)
}

/// The wire market type and bare contract id as the domain [`ContractKey`] — the wire
/// carries the natural key directly (the gateway strips PowerBot's `NX_`/`NI_` prefix into
/// `market_type`). An unset market type is half an identity and fails the batch.
fn contract_key(
    market_type: gateway::MarketType,
    contract_id: i64,
    trade_id: &str,
) -> Result<ContractKey> {
    let market_type = match market_type {
        gateway::MarketType::Local => MarketType::Local,
        gateway::MarketType::Xbid => MarketType::Xbid,
        gateway::MarketType::None => {
            bail!("trade {trade_id:?} carries no market type")
        }
    };
    Ok(ContractKey {
        market_type,
        contract_id,
    })
}

/// A fill's wire identity must be present (the ledger id derives from it) and on the
/// exchange this deployment folds (the storage layer stamps rows with its venue constant).
fn validate_identity(trade_id: &str, exchange: &str) -> Result<()> {
    if trade_id.is_empty() {
        bail!("own trade without a trade id");
    }
    if exchange != EXCHANGE {
        bail!("own trade {trade_id:?} executed on {exchange:?}, expected {EXCHANGE:?}");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config() -> GatewayConfig {
        GatewayConfig {
            name: "nordpool-a".to_owned(),
            endpoint: "http://localhost:50061".to_owned(),
            portfolio: "execution-a".to_owned(),
            powerbot_portfolio_id: "PB-A".to_owned(),
        }
    }

    fn wire_trade(trade_id: &str, side: gateway::OrderSide, quantity: i64) -> gateway::Trade {
        gateway::Trade {
            trade_id: trade_id.to_owned(),
            state: gateway::TradeState::Acti as i32,
            exchange: "nordpool".to_owned(),
            delivery_area_eic: "10YFI-1--------U".to_owned(),
            api_timestamp_nanos: 2_000,
            exec_time_nanos: 1_000,
            side: side as i32,
            order_id: "O-1".to_owned(),
            portfolio_id: "PB-A".to_owned(),
            contract_id: 7,
            market_type: gateway::MarketType::Local as i32,
            contract_name: "PH-7".to_owned(),
            price: 4_250,
            quantity,
            self_trade: false,
            strategy_id: String::new(),
            unit_id: String::new(),
            cl_order_id: String::new(),
        }
    }

    #[test]
    fn own_trade_signs_quantity_by_side() {
        // Given a buy and a sell of 5 MW
        let trades = [
            wire_trade("T-1", gateway::OrderSide::Buy, 5_000),
            wire_trade("T-2", gateway::OrderSide::Sell, 5_000),
        ];

        // When translated
        let fills = own_trade_fills(&trades, &config()).unwrap();

        // Then buy folds positive and sell negative, onto the configured portfolio, with
        // the deterministic id and the venue trade id carried for reconciliation
        assert_eq!(fills.len(), 2);
        assert_eq!(fills[0].quantity, 5_000);
        assert_eq!(fills[1].quantity, -5_000);
        assert_eq!(fills[0].portfolio, "execution-a");
        assert_eq!(fills[0].id, ledger::fill_id("nordpool", "T-1"));
        assert_eq!(fills[0].venue_trade_id.as_deref(), Some("T-1"));
        assert_eq!(fills[0].source, "nordpool-a");
        assert_eq!(fills[0].contract.contract_id, 7);
        assert_eq!(fills[0].contract.market_type, MarketType::Local);
        assert_eq!(fills[0].executed_at_nanos, 1_000);
        // No order label on the wire trade → no client order id
        assert_eq!(fills[0].cl_order_id, None);
    }

    #[test]
    fn own_trade_carries_the_order_label_as_client_order_id() {
        // A trade whose leg echoes the order's free-text label carries it as cl_order_id;
        // an empty label stays None
        let mut labelled = wire_trade("T-1", gateway::OrderSide::Buy, 5_000);
        labelled.cl_order_id = "N-1".to_owned();
        let trades = [labelled, wire_trade("T-2", gateway::OrderSide::Buy, 1_000)];

        let fills = own_trade_fills(&trades, &config()).unwrap();

        assert_eq!(fills[0].cl_order_id.as_deref(), Some("N-1"));
        assert_eq!(fills[1].cl_order_id, None);
    }

    #[test]
    fn own_trade_without_side_or_market_type_fails_the_batch() {
        // A missing side means an unknown sign — dropping would be silent position loss,
        // so the batch (and the session) fails instead
        let trades = [wire_trade("T-1", gateway::OrderSide::None, 5_000)];
        assert!(own_trade_fills(&trades, &config()).is_err());

        // Same for a missing market type — half the contract identity
        let mut no_market = wire_trade("T-2", gateway::OrderSide::Buy, 5_000);
        no_market.market_type = gateway::MarketType::None as i32;
        assert!(own_trade_fills(&[no_market], &config()).is_err());

        // And for a foreign exchange — storage stamps rows with the nordpool venue
        let mut foreign = wire_trade("T-3", gateway::OrderSide::Buy, 5_000);
        foreign.exchange = "epex".to_owned();
        assert!(own_trade_fills(&[foreign], &config()).is_err());

        // And for a missing trade id — the ledger id derives from it
        let mut no_id = wire_trade("", gateway::OrderSide::Buy, 5_000);
        no_id.trade_id = String::new();
        assert!(own_trade_fills(&[no_id], &config()).is_err());
    }

    #[test]
    fn self_trade_is_skipped_not_failed() {
        // Given a self-trade (both legs ours — nets to zero on this single portfolio)
        // alongside a normal fill
        let mut self_trade = wire_trade("T-1", gateway::OrderSide::Buy, 5_000);
        self_trade.self_trade = true;
        let trades = [
            self_trade,
            wire_trade("T-2", gateway::OrderSide::Buy, 1_000),
        ];

        // When translated
        let fills = own_trade_fills(&trades, &config()).unwrap();

        // Then the self-trade is skipped (the correct zero fold) and the rest survive
        assert_eq!(fills.len(), 1);
        assert_eq!(fills[0].venue_trade_id.as_deref(), Some("T-2"));
    }
}
