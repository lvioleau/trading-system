//! Periodic REST reconciliation — the stores' self-correction.
//!
//! Where nordpool-connectors lets its upstream feed re-push snapshot frames to
//! heal a lagged store, PowerBot will not: its websocket does not guarantee
//! delivery, and REST is authoritative (docs/design.md, "Reconciliation is the
//! store's self-correction"). So this task re-fetches `getOwnOrdersHistory`,
//! `getTrades`, and `getInternalTrades` over a trailing window and pushes them
//! through the same guarded upsert as live deltas — idempotent, never-regress —
//! so a trade or order the feed dropped without a detectable gap still reaches
//! the stores. It runs once at startup (seeding a fresh gateway before the feed
//! has repopulated it) and then on an interval. Fetched items past the eviction
//! horizon are filtered out before publishing (they'd only be evicted anyway),
//! and each pass evicts aged entries to keep the stores bounded to
//! future-delivery contracts.

use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use tokio::sync::watch;

use crate::grpc::{
    ExecutionChannels, alert_self_trades, internal_trade_to_domain, own_order_to_domain,
    trade_to_domain,
};
use crate::rest::RestClient;
use crate::storage::ExecutionStorage;
use crate::stores::Seedable;
use crate::time::{now_millis, now_nanos, now_secs, rfc3339};

/// How far past `now` to set the exclusive `to_api_timestamp` of the order-history
/// window, so the freshest revisions (whose `api_timestamp` may equal or slightly
/// lead our clock) are still included.
const TO_BUFFER: Duration = Duration::from_secs(3600);

/// What to reconcile and how often. `exchange` / `portfolio_ids` scope the REST
/// snapshots to this gateway's configured feed — the trade snapshots are fetched
/// once per portfolio (the REST endpoints are portfolio-scoped).
pub struct ReconcileConfig {
    pub exchange: String,
    pub portfolio_ids: Vec<String>,
    /// How often to re-sweep (and evict).
    pub interval: Duration,
    /// How far back to fetch trades — the `from_execution_time` window.
    pub trade_window: Duration,
    /// Retain a contract's entries this long past `delivery_end` before eviction,
    /// so a Position Service recovering after a restart still seeds a
    /// just-delivered contract.
    pub eviction_grace: Duration,
}

/// Run the reconciliation loop until `shutdown` flips: sweep, evict, wait. The
/// first sweep is immediate (startup seed). A failed sweep is logged and retried
/// on the next tick — the feed keeps the stores current in between.
pub async fn run(
    rest: RestClient,
    channels: Arc<ExecutionChannels>,
    storage: Option<ExecutionStorage>,
    cfg: ReconcileConfig,
    mut shutdown: watch::Receiver<bool>,
) {
    loop {
        // One horizon for the whole pass: filter past-delivery items out of the
        // sweep's publishes AND prune aged store entries with the same cutoff, so
        // we never broadcast a trade the eviction is about to drop.
        let horizon_millis = now_millis() - cfg.eviction_grace.as_millis() as i64;
        if let Err(e) = sweep(&rest, &channels, storage.as_ref(), &cfg, horizon_millis).await {
            tracing::warn!(error = %e, "reconciliation sweep failed; retrying next tick");
        }
        let evicted = channels.evict_delivered(horizon_millis);
        if evicted > 0 {
            tracing::debug!(evicted, horizon_millis, "evicted past-delivery entries");
        }

        tokio::select! {
            _ = tokio::time::sleep(cfg.interval) => {}
            _ = shutdown.wait_for(|&stop| stop) => break,
        }
    }
    tracing::info!("reconciliation stopped");
}

/// Fetch the three REST snapshots, persist the full window to Postgres (the
/// durable record keeps everything), then publish only the within-horizon subset
/// to the in-memory stores (memory is the live window). Publishing a whole
/// snapshot as one batch broadcasts only what changed, so live subscribers see
/// just the corrections. A persistence failure is logged, not fatal — the stores
/// are still updated, and the next sweep retries.
async fn sweep(
    rest: &RestClient,
    channels: &ExecutionChannels,
    storage: Option<&ExecutionStorage>,
    cfg: &ReconcileConfig,
    horizon_millis: i64,
) -> Result<(), crate::rest::RestError> {
    let received_at_nanos = now_nanos();
    let now = now_secs();
    let from = rfc3339(now.saturating_sub(cfg.trade_window.as_secs()));
    let to = rfc3339(now + TO_BUFFER.as_secs());

    // orders/history is not portfolio-scoped at the API, so filter to ours first.
    let orders = rest
        .get_own_orders_history(&cfg.exchange, &from, &to)
        .await?
        .into_iter()
        .filter(|o| {
            o.portfolio_id
                .as_deref()
                .is_some_and(|p| cfg.portfolio_ids.iter().any(|ours| ours == p))
        })
        .collect::<Vec<_>>();
    // The trade endpoints are portfolio-scoped: one fetch per served portfolio,
    // concatenated — the stores key by trade id, so the merge is order-free.
    let mut trades = Vec::new();
    let mut internal = Vec::new();
    for portfolio_id in &cfg.portfolio_ids {
        trades.extend(rest.get_trades(&cfg.exchange, portfolio_id, &from).await?);
        internal.extend(
            rest.get_internal_trades(&cfg.exchange, portfolio_id, &from)
                .await?,
        );
    }
    let (fetched_orders, fetched_trades, fetched_internal) =
        (orders.len(), trades.len(), internal.len());

    // Durable record: persist the whole fetched window (past-delivery included).
    if let Some(s) = storage {
        if let Err(e) = s.record_order_revisions(&orders).await {
            tracing::warn!(error = %e, "persisting order revisions failed");
        }
        if let Err(e) = s.record_trades(&trades).await {
            tracing::warn!(error = %e, "persisting trades failed");
        }
        if let Err(e) = s.record_internal_trades(&internal).await {
            tracing::warn!(error = %e, "persisting internal trades failed");
        }
    }

    // Live window: curate to the domain form the stores hold (same as the feed
    // does, so a re-fetch of an unchanged trade compares equal), then publish only
    // what's still within the eviction horizon (future delivery, or recently
    // delivered inside the grace window). The trade window is by execution time
    // and reaches back a day, so most of what it returns has already delivered —
    // publishing that would broadcast trades the eviction pass immediately drops.
    let orders: Vec<_> = orders
        .into_iter()
        .map(own_order_to_domain)
        .filter(|o| within_horizon(o, horizon_millis))
        .collect();
    let trades: Vec<_> = trades
        .into_iter()
        .map(trade_to_domain)
        .filter(|t| within_horizon(t, horizon_millis))
        .collect();
    let internal: Vec<_> = internal
        .into_iter()
        .map(internal_trade_to_domain)
        .filter(|t| within_horizon(t, horizon_millis))
        .collect();

    tracing::info!(
        orders = orders.len(),
        trades = trades.len(),
        internal_trades = internal.len(),
        fetched_orders,
        fetched_trades,
        fetched_internal,
        persisted = storage.is_some(),
        %from,
        %to,
        "reconciliation sweep"
    );

    channels.orders.publish(orders, received_at_nanos);
    // Alarm on the applied delta only: the same window is re-fetched every tick,
    // and the guarded upsert absorbing an unchanged self-trade must not re-fire it.
    alert_self_trades(&channels.trades.publish(trades, received_at_nanos));
    // Through the same choke point as the feed, so a shadow gateway's sweep also
    // normalizes internal trades onto the own-trades store/stream. `alarm_live =
    // false`: the sweep re-fetches the same window each tick, so a live gateway
    // records the internal trade (above) but must not re-alarm it every sweep —
    // the websocket feed owns that alarm.
    channels.ingest_internal_trades(internal, received_at_nanos, false);
    Ok(())
}

/// Whether an item is still within the eviction horizon — future delivery, or
/// recently delivered inside the grace window. Items with no delivery end are
/// kept (they can't be evicted either).
fn within_horizon<T: Seedable>(item: &T, horizon_millis: i64) -> bool {
    item.delivery_end_millis()
        .is_none_or(|end| end > horizon_millis)
}
