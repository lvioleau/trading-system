//! The PowerBot execution feeds — the executor's fast return path: **per portfolio**, a
//! reconnecting own-orders + own-trades subscription pair over `PowerbotExecutionService`,
//! each subscribed with `portfolio_id` = the portfolio (one identifier end to end — a
//! portfolio id *is* the PowerBot portfolio id). The two streams carry different truth and go
//! to different owners: own-orders becomes [`GatewayEvent`]s (revisions + seed markers) for
//! the order manager, while own-trades goes **straight to the FillMatcher** as domain
//! [`Fill`]s on its own channel — the manager's fill input is the *matched* output, so raw
//! fills never route through it. The write path (`AddOrder`) lives in `order_sender` and
//! reports on its own channel. This is the only module that knows both the gateway wire types
//! and the domain [`Fill`] / [`OrderRevision`].
//!
//! **Subscription discipline** — seed-then-deltas, per-stream `seq`:
//! - The first frame is the snapshot; every frame with `seq >` the seed's is live.
//! - The seed is a **dedupe baseline only** (seed fills are the Position Service's to have
//!   folded) — recorded, never emitted. Its [`GatewayEvent::OwnOrdersSeeded`] /
//!   [`GatewayEvent::OwnTradesSeeded`] marker is the boot gate's signal (invariant 9),
//!   re-emitted on every re-seed.
//! - Fills dedupe by venue trade id for the whole process lifetime, so a reconnect's overlap
//!   replays as no-ops. IOC never rests, so an own-orders seed has nothing to recover.
//!
//! **Failure discipline:**
//! - `INVALID_ARGUMENT` on any portfolio's subscribe = the gateway does not serve it — a
//!   permanent config mismatch, so the boot fails (fail fast; don't retry a stream that can
//!   never be right). Transport errors back off and reconnect that pair alone.
//! - A fill/revision whose `portfolio_id` isn't the stream's scope is a gateway-bug tripwire
//!   (invariant 7): warn, ignore for state. Other malformed entries are skipped with a warn —
//!   the executor is a stateless projection, so a missed fill self-heals through the position
//!   reconcile (unlike the Position Service, where a dropped fill is silent position loss).

use std::collections::HashSet;

use anyhow::{Result, anyhow};
use flow_util::feed::Backoff;
use tokio::sync::mpsc;
use tokio::task::JoinSet;
use tokio::time::Instant;
use tracing::{debug, error, info, warn};

use crate::config::FeedConfig;
use crate::executor::{ContractKey, Fill, MarketType, OrderRevision, PortfolioId};
use crate::gateway_proto as wire;
use crate::gateway_proto::powerbot_execution_service_client::PowerbotExecutionServiceClient;

/// How many consecutive failed sessions escalate the log level from warn to error — a
/// gateway restart heals in one or two reconnects; anything still failing after this many
/// means order resolution and fill tracking are blind and need eyes.
const FAILURES_BEFORE_ERROR: u32 = 3;

/// One own-orders message from the gateway feeds to the order manager. (Own-trades fills go
/// straight to the FillMatcher on their own channel; the `AddOrder` sender reports separately,
/// as an `order_sender::OrderResponse`.)
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum GatewayEvent {
    /// A portfolio's own-orders stream (re)seeded — the boot-gate marker (invariant 9). A
    /// re-seed may have skipped revisions; the resolve timeout covers them.
    OwnOrdersSeeded { portfolio: PortfolioId },
    /// A portfolio's own-trades stream (re)seeded — the dedupe baseline is set.
    OwnTradesSeeded { portfolio: PortfolioId },
    /// A live own-order revision, terminal-classified at the boundary (invariant 5).
    OrderRevision(OrderRevision),
}

/// Which of the gateway's execution streams a subscription task drives.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum StreamKind {
    OwnOrders,
    OwnTrades,
}

impl StreamKind {
    fn name(self) -> &'static str {
        match self {
            StreamKind::OwnOrders => "own-orders",
            StreamKind::OwnTrades => "own-trades",
        }
    }
}

/// Runs one own-orders + own-trades subscription pair per configured portfolio, forever,
/// each reconnecting with backoff (tuned by [`FeedConfig`]). Own-orders events go into
/// `events_tx` (to the order manager); own-trades fills go into `fills_tx` (straight to the
/// FillMatcher). Returns on a fatal condition: **any** pair's `INVALID_ARGUMENT` subscribe
/// rejection (the gateway does not serve that portfolio — a permanent config mismatch) or
/// either output channel closing (the engine is gone) — either way the caller treats it as a
/// boot failure.
pub async fn run_subscriptions(
    endpoint: String,
    portfolios: Vec<PortfolioId>,
    tuning: FeedConfig,
    events_tx: mpsc::Sender<GatewayEvent>,
    fills_tx: mpsc::Sender<Fill>,
) -> anyhow::Error {
    let mut tasks = JoinSet::new();
    for portfolio in portfolios {
        for kind in [StreamKind::OwnOrders, StreamKind::OwnTrades] {
            tasks.spawn(run_one(
                endpoint.clone(),
                portfolio.clone(),
                kind,
                tuning,
                events_tx.clone(),
                fills_tx.clone(),
            ));
        }
    }
    match tasks.join_next().await {
        Some(Ok(error)) => error,
        Some(Err(join_error)) => anyhow!("gateway subscription task exited: {join_error}"),
        None => anyhow!("no gateway subscriptions configured"),
    }
}

/// One portfolio's subscription loop for one stream kind. The own-trades dedupe set lives
/// here — process-lifetime, across reconnects — so a re-seed's overlap replays as no-ops.
async fn run_one(
    endpoint: String,
    portfolio: PortfolioId,
    kind: StreamKind,
    tuning: FeedConfig,
    events_tx: mpsc::Sender<GatewayEvent>,
    fills_tx: mpsc::Sender<Fill>,
) -> anyhow::Error {
    let mut backoff = Backoff::new(&tuning);
    let mut consecutive_failures: u32 = 0;
    let mut seen_trades: HashSet<String> = HashSet::new();
    loop {
        let session_start = Instant::now();
        let result = subscribe(
            &endpoint,
            &portfolio,
            kind,
            &tuning,
            &events_tx,
            &fills_tx,
            &mut seen_trades,
        )
        .await;
        consecutive_failures += 1;
        match result {
            Ok(()) => warn!(
                endpoint,
                portfolio,
                stream = kind.name(),
                "gateway stream closed by server"
            ),
            Err(SessionError::Fatal(error)) => return error,
            Err(SessionError::Retry(error)) if consecutive_failures >= FAILURES_BEFORE_ERROR => {
                error!(
                    endpoint,
                    portfolio,
                    stream = kind.name(),
                    consecutive_failures,
                    error = format!("{error:#}"),
                    "gateway subscription failing repeatedly; order resolution is blind"
                )
            }
            Err(SessionError::Retry(error)) => warn!(
                endpoint,
                portfolio,
                stream = kind.name(),
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

/// How a subscription session ended.
enum SessionError {
    /// The gateway does not serve this portfolio (`INVALID_ARGUMENT`), or the event channel
    /// is gone. Neither heals by retrying.
    Fatal(anyhow::Error),
    /// Transport failure, terminal stream status, or a lag drop — back off and resubscribe.
    Retry(anyhow::Error),
}

/// Split a subscribe rejection: `INVALID_ARGUMENT` is the gateway saying it does not serve
/// this portfolio — permanent, fail the boot; anything else retries.
fn subscribe_error(status: tonic::Status, portfolio: &str, kind: StreamKind) -> SessionError {
    if status.code() == tonic::Code::InvalidArgument {
        SessionError::Fatal(anyhow!(
            "gateway rejected the {} subscription for portfolio {portfolio:?}: {} — a \
             permanent config mismatch (this gateway does not serve that portfolio); not \
             retrying",
            kind.name(),
            status.message()
        ))
    } else {
        SessionError::Retry(anyhow!(status).context(format!(
            "subscribing to {} for portfolio {portfolio:?}",
            kind.name()
        )))
    }
}

/// One session of one stream: connect, subscribe under the portfolio id (the PowerBot
/// portfolio id — the same string), treat the first frame as the seed (baseline only),
/// then translate every `seq >` seed frame into events.
async fn subscribe(
    endpoint: &str,
    portfolio: &str,
    kind: StreamKind,
    tuning: &FeedConfig,
    events_tx: &mpsc::Sender<GatewayEvent>,
    fills_tx: &mpsc::Sender<Fill>,
    seen_trades: &mut HashSet<String>,
) -> Result<(), SessionError> {
    let channel = flow_util::feed::endpoint(endpoint, tuning)
        .map_err(SessionError::Retry)?
        .connect()
        .await
        .map_err(|error| {
            SessionError::Retry(anyhow!(error).context("connecting to the gateway"))
        })?;
    let mut client = PowerbotExecutionServiceClient::new(channel);
    match kind {
        StreamKind::OwnOrders => {
            let stream = client
                .subscribe_own_orders(wire::SubscribeOwnOrdersRequest {
                    portfolio_id: portfolio.to_owned(),
                })
                .await
                .map_err(|status| subscribe_error(status, portfolio, kind))?
                .into_inner();
            drive_own_orders(stream, portfolio, events_tx).await
        }
        StreamKind::OwnTrades => {
            let stream = client
                .subscribe_own_trades(wire::SubscribeOwnTradesRequest {
                    portfolio_id: portfolio.to_owned(),
                })
                .await
                .map_err(|status| subscribe_error(status, portfolio, kind))?
                .into_inner();
            drive_own_trades(stream, portfolio, events_tx, fills_tx, seen_trades).await
        }
    }
}

/// Send one event, mapping a closed channel to the fatal session error (the engine is
/// gone, and with it the process's reason to exist).
async fn send_event(
    events_tx: &mpsc::Sender<GatewayEvent>,
    event: GatewayEvent,
) -> Result<(), SessionError> {
    events_tx.send(event).await.map_err(|_| {
        SessionError::Fatal(anyhow!("gateway event channel closed (the engine is gone)"))
    })
}

/// Send one fill straight to the FillMatcher, mapping a closed channel to the fatal session
/// error (the matcher is gone, and with it the process's reason to exist).
async fn send_fill(fills_tx: &mpsc::Sender<Fill>, fill: Fill) -> Result<(), SessionError> {
    fills_tx.send(fill).await.map_err(|_| {
        SessionError::Fatal(anyhow!(
            "gateway fill channel closed (the fill matcher is gone)"
        ))
    })
}

/// The own-orders session loop: the seed is a baseline only (IOC leaves nothing resting to
/// recover), then every live revision is translated and forwarded, terminal-classified.
async fn drive_own_orders(
    mut stream: tonic::Streaming<wire::OwnOrdersUpdate>,
    portfolio: &str,
    events_tx: &mpsc::Sender<GatewayEvent>,
) -> Result<(), SessionError> {
    let mut seed_seq = None;
    loop {
        match stream.message().await {
            Err(status) => {
                return Err(SessionError::Retry(
                    anyhow!(status).context("reading the own-orders stream"),
                ));
            }
            Ok(None) => return Ok(()),
            Ok(Some(update)) => match seed_seq {
                None => {
                    seed_seq = Some(update.seq);
                    info!(
                        portfolio,
                        orders = update.orders.len(),
                        "own-orders seeded (baseline only; IOC leaves nothing to recover)"
                    );
                    send_event(
                        events_tx,
                        GatewayEvent::OwnOrdersSeeded {
                            portfolio: portfolio.to_owned(),
                        },
                    )
                    .await?;
                }
                Some(seed) if update.seq <= seed => {
                    debug!(
                        portfolio,
                        seq = update.seq,
                        seed,
                        "dropping own-orders seed/delta overlap"
                    );
                }
                Some(_) => {
                    for order in update.orders {
                        if let Some(revision) = to_domain_revision(order, portfolio) {
                            info!(
                                cl_order_id = revision.cl_order_id,
                                ?revision,
                                "own-order revision"
                            );
                            send_event(events_tx, GatewayEvent::OrderRevision(revision)).await?;
                        }
                    }
                }
            },
        }
    }
}

/// The own-trades session loop: the seed's trade ids enter the dedupe set and are never
/// emitted (they are the Position Service's to have folded already); live fills are
/// translated, deduped by venue trade id, and forwarded.
async fn drive_own_trades(
    mut stream: tonic::Streaming<wire::OwnTradesUpdate>,
    portfolio: &str,
    events_tx: &mpsc::Sender<GatewayEvent>,
    fills_tx: &mpsc::Sender<Fill>,
    seen_trades: &mut HashSet<String>,
) -> Result<(), SessionError> {
    let mut seed_seq = None;
    loop {
        match stream.message().await {
            Err(status) => {
                return Err(SessionError::Retry(
                    anyhow!(status).context("reading the own-trades stream"),
                ));
            }
            Ok(None) => return Ok(()),
            Ok(Some(update)) => match seed_seq {
                None => {
                    seed_seq = Some(update.seq);
                    for trade in &update.trades {
                        if !trade.trade_id.is_empty() {
                            seen_trades.insert(trade.trade_id.clone());
                        }
                    }
                    info!(
                        portfolio,
                        trades = update.trades.len(),
                        "own-trades seeded (dedupe baseline only)"
                    );
                    send_event(
                        events_tx,
                        GatewayEvent::OwnTradesSeeded {
                            portfolio: portfolio.to_owned(),
                        },
                    )
                    .await?;
                }
                Some(seed) if update.seq <= seed => {
                    debug!(
                        portfolio,
                        seq = update.seq,
                        seed,
                        "dropping own-trades seed/delta overlap"
                    );
                }
                Some(_) => {
                    for trade in update.trades {
                        let Some(fill) = to_domain_fill(trade, portfolio) else {
                            continue;
                        };
                        if !seen_trades.insert(fill.venue_trade_id.clone()) {
                            debug!(
                                portfolio,
                                venue_trade_id = fill.venue_trade_id,
                                "skipping already-seen fill (replay)"
                            );
                            continue;
                        }
                        info!(cl_order_id = fill.cl_order_id, ?fill, "own trade");
                        send_fill(fills_tx, fill).await?;
                    }
                }
            },
        }
    }
}

/// Wire → domain for one own-order revision. `None` skips (each warned):
/// - a foreign `portfolio_id` on this scoped stream (the invariant-7 gateway-bug tripwire),
/// - no order id (nothing to resolve against),
/// - no market type (half the contract identity — nothing to key the pair by).
fn to_domain_revision(order: wire::OwnOrder, portfolio: &str) -> Option<OrderRevision> {
    if order.portfolio_id != portfolio {
        warn!(
            portfolio,
            foreign = order.portfolio_id,
            order_id = order.order_id,
            "own-order revision for a foreign portfolio id on a scoped stream — \
             gateway bug; ignoring for state"
        );
        return None;
    }
    if order.order_id.is_empty() {
        warn!(portfolio, "skipping own-order revision without an order id");
        return None;
    }
    let market_type = match order.market_type() {
        wire::MarketType::Local => MarketType::Local,
        wire::MarketType::Xbid => MarketType::Xbid,
        wire::MarketType::None => {
            warn!(
                portfolio,
                order_id = order.order_id,
                "skipping own-order revision without a market type"
            );
            return None;
        }
    };
    let (action, state) = (order.action(), order.state());
    Some(OrderRevision {
        portfolio: portfolio.to_owned(),
        contract: ContractKey {
            market_type,
            contract_id: order.contract_id,
        },
        order_id: order.order_id,
        cl_order_id: order.cl_order_id,
        revision_no: order.revision_no,
        terminal: is_terminal(action, state),
        quantity: order.quantity,
        action: short_name(action.as_str_name(), "ORDER_ACTION_"),
        state: short_name(state.as_str_name(), "ORDER_STATE_"),
    })
}

/// Invariant 5's terminal classification: for an IOC order, life ends with full execution
/// (`FEXE`), a deletion (`UDEL`/`ADEL`/`SDEL` — the kill after a partial or unfilled IOC),
/// a rejection (`UREJ`/`AREJ`/`SREJ`), a venue/system error (`SERR`), a PowerBot limit
/// violation (`RLVI`), or a dead order state (`IACT`). `SNAV`/`UNKW` are *unknown*, not
/// terminal — the resolve timeout covers them.
fn is_terminal(action: wire::OrderAction, state: wire::OrderState) -> bool {
    use wire::OrderAction as A;
    matches!(
        action,
        A::Fexe | A::Udel | A::Adel | A::Sdel | A::Urej | A::Arej | A::Srej | A::Serr | A::Rlvi
    ) || state == wire::OrderState::Iact
}

/// Wire → domain for one own trade. `None` skips (each warned):
/// - a foreign `portfolio_id` (the invariant-7 tripwire),
/// - a self-trade (both legs ours — nets to zero; shouldn't occur under IOC-only entry, and
///   the Position Service skips it too, so recording it would wait forever for a fold that
///   never comes),
/// - a malformed entry (missing id, side, or market type).
///
/// A skipped fill self-heals: the position reconcile recomputes the gap from `P` either way.
fn to_domain_fill(trade: wire::Trade, portfolio: &str) -> Option<Fill> {
    if trade.portfolio_id != portfolio {
        warn!(
            portfolio,
            foreign = trade.portfolio_id,
            trade_id = trade.trade_id,
            "fill for a foreign portfolio id on a scoped stream — gateway bug; \
             ignoring for state"
        );
        return None;
    }
    if trade.self_trade {
        warn!(
            portfolio,
            trade_id = trade.trade_id,
            "skipping self-trade (both legs ours): should never occur under IOC-only entry"
        );
        return None;
    }
    if trade.trade_id.is_empty() {
        warn!(portfolio, "skipping own trade without a trade id");
        return None;
    }
    let quantity = match trade.side() {
        wire::OrderSide::Buy => trade.quantity,
        wire::OrderSide::Sell => -trade.quantity,
        wire::OrderSide::None => {
            warn!(
                portfolio,
                trade_id = trade.trade_id,
                "skipping own trade without a side"
            );
            return None;
        }
    };
    let market_type = match trade.market_type() {
        wire::MarketType::Local => MarketType::Local,
        wire::MarketType::Xbid => MarketType::Xbid,
        wire::MarketType::None => {
            warn!(
                portfolio,
                trade_id = trade.trade_id,
                "skipping own trade without a market type"
            );
            return None;
        }
    };
    Some(Fill {
        portfolio: portfolio.to_owned(),
        contract: ContractKey {
            market_type,
            contract_id: trade.contract_id,
        },
        order_id: trade.order_id,
        cl_order_id: trade.cl_order_id,
        venue_trade_id: trade.trade_id,
        exchange: trade.exchange,
        quantity,
        price: trade.price,
        executed_at_nanos: trade.exec_time_nanos,
    })
}

/// `ORDER_ACTION_FEXE` → `FEXE`: the wire enum name without its prefix, for the trace.
fn short_name(name: &str, prefix: &str) -> String {
    name.strip_prefix(prefix).unwrap_or(name).to_owned()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The portfolio id is also the PowerBot portfolio id — one string.
    const PORTFOLIO: &str = "execution-a";

    fn wire_trade(trade_id: &str, side: wire::OrderSide, quantity: i64) -> wire::Trade {
        wire::Trade {
            trade_id: trade_id.to_owned(),
            state: wire::TradeState::Acti as i32,
            exchange: "nordpool".to_owned(),
            delivery_area_eic: "10YFR-RTE------C".to_owned(),
            api_timestamp_nanos: 2_000,
            exec_time_nanos: 1_000,
            side: side as i32,
            order_id: "O-1".to_owned(),
            portfolio_id: PORTFOLIO.to_owned(),
            contract_id: 7,
            market_type: wire::MarketType::Local as i32,
            contract_name: "PH-7".to_owned(),
            price: 4_250,
            quantity,
            self_trade: false,
            strategy_id: String::new(),
            unit_id: String::new(),
            cl_order_id: "id-executor:test:execution-a".to_owned(),
        }
    }

    fn wire_order(action: wire::OrderAction, state: wire::OrderState) -> wire::OwnOrder {
        wire::OwnOrder {
            order_id: "O-1".to_owned(),
            revision_no: 2,
            state: state as i32,
            action: action as i32,
            side: wire::OrderSide::Sell as i32,
            contract_id: 7,
            market_type: wire::MarketType::Local as i32,
            delivery_area_eic: "10YFR-RTE------C".to_owned(),
            portfolio_id: PORTFOLIO.to_owned(),
            price: 1_990,
            quantity: 10_000,
            cl_order_id: String::new(),
            api_timestamp_nanos: 3_000,
            errors: vec![],
        }
    }

    #[test]
    fn fills_sign_quantity_by_side_and_route_to_the_executor_portfolio() {
        let buy =
            to_domain_fill(wire_trade("T-1", wire::OrderSide::Buy, 5_000), PORTFOLIO).unwrap();
        let sell =
            to_domain_fill(wire_trade("T-2", wire::OrderSide::Sell, 5_000), PORTFOLIO).unwrap();
        assert_eq!(buy.quantity, 5_000);
        assert_eq!(sell.quantity, -5_000);
        assert_eq!(buy.portfolio, "execution-a");
        assert_eq!(buy.venue_trade_id, "T-1");
        assert_eq!(buy.exchange, "nordpool");
        assert_eq!(buy.order_id, "O-1");
        assert_eq!(buy.contract.contract_id, 7);
    }

    #[test]
    fn foreign_self_trade_and_malformed_fills_are_skipped() {
        // A foreign portfolio id on the scoped stream — the invariant-7 tripwire
        let mut foreign = wire_trade("T-1", wire::OrderSide::Buy, 5_000);
        foreign.portfolio_id = "PB-X".to_owned();
        assert!(to_domain_fill(foreign, PORTFOLIO).is_none());

        // A self-trade nets to zero on the Position Service side too — never recorded
        let mut self_trade = wire_trade("T-2", wire::OrderSide::Buy, 5_000);
        self_trade.self_trade = true;
        assert!(to_domain_fill(self_trade, PORTFOLIO).is_none());

        // Malformed: no id, no side, no market type
        assert!(to_domain_fill(wire_trade("", wire::OrderSide::Buy, 1), PORTFOLIO).is_none());
        assert!(to_domain_fill(wire_trade("T-3", wire::OrderSide::None, 1), PORTFOLIO).is_none());
        let mut no_market = wire_trade("T-4", wire::OrderSide::Buy, 1);
        no_market.market_type = wire::MarketType::None as i32;
        assert!(to_domain_fill(no_market, PORTFOLIO).is_none());
    }

    #[test]
    fn terminal_classification_follows_invariant_5() {
        use wire::{OrderAction as A, OrderState as S};
        // Terminal: full execution, deletions, rejections, errors, limit violations, and
        // a dead state whatever the action says.
        for action in [
            A::Fexe,
            A::Udel,
            A::Adel,
            A::Sdel,
            A::Urej,
            A::Arej,
            A::Srej,
            A::Serr,
            A::Rlvi,
        ] {
            assert!(is_terminal(action, S::Acti), "{action:?} must be terminal");
        }
        assert!(is_terminal(A::Uadd, S::Iact), "a dead state is terminal");

        // Not terminal: entry, a partial still working (IOC kills follow separately as a
        // deletion), and the unknown states (the resolve timeout covers those).
        for action in [A::Uadd, A::Pexe, A::Snav] {
            assert!(
                !is_terminal(action, S::Acti),
                "{action:?} must not be terminal"
            );
        }
        assert!(!is_terminal(A::Uadd, S::Unkw));
    }

    #[test]
    fn revisions_carry_the_trace_names_and_the_portfolio() {
        let revision = to_domain_revision(
            wire_order(wire::OrderAction::Fexe, wire::OrderState::Iact),
            PORTFOLIO,
        )
        .unwrap();
        assert_eq!(revision.portfolio, "execution-a");
        assert_eq!(
            revision.contract,
            ContractKey {
                market_type: MarketType::Local,
                contract_id: 7
            }
        );
        assert_eq!(revision.order_id, "O-1");
        assert_eq!(revision.revision_no, 2);
        assert!(revision.terminal);
        // The remaining quantity rides along — the resolve trigger's reconcile input.
        assert_eq!(revision.quantity, 10_000);
        assert_eq!(revision.action, "FEXE");
        assert_eq!(revision.state, "IACT");

        // A foreign portfolio id is the same tripwire as on fills
        let mut foreign = wire_order(wire::OrderAction::Fexe, wire::OrderState::Iact);
        foreign.portfolio_id = "PB-X".to_owned();
        assert!(to_domain_revision(foreign, PORTFOLIO).is_none());
    }
}
