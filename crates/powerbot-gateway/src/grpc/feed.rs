//! Producer→server channels for the execution feeds — the nordpool-connectors
//! "channel" shape: each store paired with the broadcast it seeds.
//!
//! A [`Channel`] co-locates a [`SeedStore`](crate::stores::SeedStore) with the
//! broadcast of its deltas, so the load-bearing invariant — apply to the store
//! *before* sending on the broadcast — is [`Channel::publish`]'s job, not
//! something every writer has to remember to order. The gRPC feed subscribes to
//! `updates` and (once seeding lands) snapshots `seed`; the `seq > seed_seq`
//! filter drops the overlap. See docs/design.md, "Stores and gRPC streams".
//!
//! [`ExecutionChannels`] bundles the two exposed streams — external trades and
//! own orders — behind one outer `Arc` shared between the websocket driver
//! (writer) and the tonic server (reader). `broadcast::Sender::send` is a
//! non-blocking sync call, so the driver thread feeds it directly — no bridge.
//! Internal trades are not a stream: they are recorded durably, normalized onto
//! the own-trades stream on a shadow gateway, and alarmed (never relayed) on a
//! live one — see [`ExecutionChannels::ingest_internal_trades`].

use std::sync::{Mutex, MutexGuard, PoisonError};

use powerbot_models::models::{OwnOrderChanges, OwnTradeChanges};
use tokio::sync::{broadcast, mpsc};

use super::{internal_trade_to_domain, own_order_to_domain, trade_to_domain};
use crate::domain::{InternalTrade, OwnOrder, Trade};
use crate::halts::SelfMatchHalts;
use crate::storage::Persist;
use crate::stores::{SeedStore, Seedable};
use crate::time::now_nanos;

/// One broadcast batch: the delta a [`Channel`]'s guarded upsert produced,
/// stamped with the gateway receive time and the store `seq` it was applied at.
#[derive(Debug, Clone)]
pub struct Batch<T> {
    pub received_at_nanos: i64,
    pub seq: u64,
    pub items: Vec<T>,
}

/// A store paired with the broadcast of its deltas — the nordpool
/// `AreaDepthChannel` shape. The `Mutex` is plain: sharing comes from the
/// [`ExecutionChannels`] outer `Arc`, the writer holds it for one brief upsert
/// per batch, and readers only clone a seed on subscribe.
pub struct Channel<T> {
    pub seed: Mutex<SeedStore<T>>,
    pub updates: broadcast::Sender<Batch<T>>,
}

impl<T: Seedable + Send + 'static> Channel<T> {
    fn new(capacity: usize) -> Self {
        Self {
            seed: Mutex::new(SeedStore::new()),
            updates: broadcast::channel(capacity).0,
        }
    }

    /// Attach to the delta broadcast. Call this *before* [`seed_snapshot`] so any
    /// change applied after the seed is guaranteed to be waiting on the receiver
    /// — the writer applies to the store before it sends here.
    ///
    /// [`seed_snapshot`]: Self::seed_snapshot
    pub fn subscribe(&self) -> broadcast::Receiver<Batch<T>> {
        self.updates.subscribe()
    }

    /// Snapshot the store for a new subscriber's seed: the `seq` it is current as
    /// of, a serve-time receive stamp, and the full current state. Call *after*
    /// [`subscribe`](Self::subscribe); the reader then drops the seed/delta
    /// overlap by forwarding only deltas with `seq > seed_seq`.
    pub fn seed_snapshot(&self) -> (u64, i64, Vec<T>) {
        let (seq, items) = lock(&self.seed).seed();
        (seq, now_nanos(), items)
    }

    /// Apply a batch to the store, then broadcast the delta — in that order, so a
    /// subscriber that seeds after attaching to `updates` never misses a change.
    /// A batch the guarded upsert wholly absorbed (a duplicate or regression)
    /// broadcasts nothing. Returns the applied delta (empty when nothing changed)
    /// so callers can inspect what a batch actually changed — e.g. the self-trade
    /// alarm fires once per state change, not once per redundant sweep re-fetch.
    ///
    /// The store lock is held **across** the broadcast send, not just the apply:
    /// with two producers on one channel (the ws driver and the REST
    /// reconciliation task) publishing concurrently, releasing the lock between
    /// the `seq` bump and the send would let their frames reorder on the wire —
    /// a client keyed-upserting could then settle on a stale value. Holding it
    /// makes send order == `seq` order. `broadcast::send` is non-blocking (no
    /// await, no IO), so the critical section stays microseconds long.
    pub fn publish<I: IntoIterator<Item = T>>(&self, items: I, received_at_nanos: i64) -> Vec<T> {
        let mut store = lock(&self.seed);
        let (seq, items) = store.apply(items);
        if items.is_empty() {
            return items;
        }
        // A send error just means no subscribers are attached — the store still
        // holds the state to seed the next one.
        let _ = self.updates.send(Batch {
            received_at_nanos,
            seq,
            items: items.clone(),
        });
        items
    }

    /// Evict entries whose delivery ended at or before `horizon_millis` (the
    /// caller passes `now - grace`); returns how many were dropped. Silent — an
    /// evicted entry is past delivery, so it is neither seeded nor a delta.
    pub fn evict_delivered(&self, horizon_millis: i64) -> usize {
        lock(&self.seed).evict_delivered(horizon_millis)
    }
}

/// The gateway identity a shadow gateway stamps on normalized internal trades —
/// see [`InternalTrade::shadow_trades`] and docs/design.md, "Shadow instances".
/// Present iff the gateway is configured `shadow = true`.
#[derive(Debug, Clone)]
pub struct ShadowNormalization {
    /// Stamped as the normalized trades' `exchange` (the internal-trade relay
    /// carries none) — the id namespace Position Service fills live in.
    pub exchange: String,
    /// Stamped as their delivery area (one gateway serves one area).
    pub delivery_area_eic: String,
    /// The portfolios that are ours — selects which leg(s) of an internal trade
    /// to emit.
    pub portfolio_ids: Vec<String>,
}

/// The three execution channels, shared as `Arc<ExecutionChannels>` between the
/// websocket driver (writer) and the tonic server (reader). When persistence is
/// configured, `persist` carries each websocket batch to the live-persistence
/// task; the DB's own guards dedup against the reconciliation sweep.
pub struct ExecutionChannels {
    pub trades: Channel<Trade>,
    pub orders: Channel<OwnOrder>,
    persist: Option<mpsc::Sender<Persist>>,
    shadow: Option<ShadowNormalization>,
    /// Contracts halted after a live-gateway self-match; the order gate reads
    /// this to refuse further entry (see [`ingest_internal_trades`] and
    /// `crate::halts`). Empty on a shadow gateway (internal trades normalize
    /// there instead of halting).
    ///
    /// [`ingest_internal_trades`]: Self::ingest_internal_trades
    self_match_halts: SelfMatchHalts,
}

impl ExecutionChannels {
    /// Fresh channels, each broadcast holding up to `capacity` unread batches
    /// before a slow subscriber is dropped. No persistence — see
    /// [`with_persistence`](Self::with_persistence).
    pub fn new(capacity: usize) -> Self {
        Self {
            trades: Channel::new(capacity),
            orders: Channel::new(capacity),
            persist: None,
            shadow: None,
            self_match_halts: SelfMatchHalts::default(),
        }
    }

    /// Route each websocket batch to the live-persistence task as well as the
    /// stores, so a fresh fill is written to Postgres immediately rather than at
    /// the next reconciliation sweep.
    pub fn with_persistence(mut self, persist: mpsc::Sender<Persist>) -> Self {
        self.persist = Some(persist);
        self
    }

    /// Mark these channels as a shadow gateway's: every internal-trade batch is
    /// normalized onto the own-trades stream (see
    /// [`ingest_internal_trades`](Self::ingest_internal_trades)).
    pub fn with_shadow(mut self, shadow: ShadowNormalization) -> Self {
        self.shadow = Some(shadow);
        self
    }

    /// A handle on the self-match halt set, for the order gate to read (it
    /// refuses entry on a halted `(portfolio, contract)`). Clones share one set.
    pub fn self_match_halts(&self) -> SelfMatchHalts {
        self.self_match_halts.clone()
    }

    /// Ingest one decoded `OwnTradeChanges`: enqueue for live persistence (if on),
    /// then apply its external trades to the own-trades store/stream and hand its
    /// internal trades to [`ingest_internal_trades`](Self::ingest_internal_trades)
    /// (normalized on shadow, alarmed on live — never streamed), all stamped with
    /// the one receive time.
    pub fn own_trades(&self, changes: &OwnTradeChanges) {
        let received_at_nanos = now_nanos();
        let trades = changes.trades.clone().unwrap_or_default();
        let internal = changes.internal_trades.clone().unwrap_or_default();
        // Persist the raw wire models (the durable record mirrors PowerBot's
        // columns); the stores hold the curated domain form.
        if self.persist.is_some() {
            if !trades.is_empty() {
                self.enqueue(Persist::Trades(trades.clone()));
            }
            if !internal.is_empty() {
                self.enqueue(Persist::InternalTrades(internal.clone()));
            }
        }
        let trades: Vec<Trade> = trades.into_iter().map(trade_to_domain).collect();
        let internal: Vec<InternalTrade> =
            internal.into_iter().map(internal_trade_to_domain).collect();
        // The whole curated trade, including its cl_order_id — one info line per
        // fill, so an order logged at entry can be correlated to its fills by the
        // same key. (The full raw batch is also logged at debug in the sink.)
        for trade in &trades {
            tracing::info!(?trade, "own trade");
        }
        alert_self_trades(&self.trades.publish(trades, received_at_nanos));
        self.ingest_internal_trades(internal, received_at_nanos, true);
    }

    /// Handle one batch of internal trades. Internal trades are **never exposed
    /// outside the gateway** — there is no stream; the concept stops at this
    /// boundary. The caller persists them durably (the raw record); this decides
    /// what, if anything, becomes visible:
    ///
    /// - **shadow gateway** — each our-side leg is normalized onto the own-trades
    ///   store/stream as an ordinary [`Trade`] ([`InternalTrade::shadow_trades`]),
    ///   so the Position Service and the executors consume shadow executions as
    ///   ordinary fills with no shadow-awareness. Both ingress routes (the feed and
    ///   the reconciliation sweep) come through here, so seeds, live deltas, and
    ///   sweep corrections all dedup against each other in the trades store.
    /// - **live gateway** — an internal trade is a self-match between two of our
    ///   own orders, impossible under IOC-only order entry: alarmed here, never
    ///   normalized or streamed. `alarm_live` gates the alarm to the live websocket
    ///   feed (`true`); the reconciliation sweep (`false`) re-fetches the same
    ///   window every tick, so it records but does not re-alarm.
    pub fn ingest_internal_trades(
        &self,
        internal: Vec<InternalTrade>,
        received_at_nanos: i64,
        alarm_live: bool,
    ) {
        let Some(shadow) = &self.shadow else {
            for trade in &internal {
                // Halt every ours-side leg's (portfolio, contract): the order gate
                // refuses further entry on it. Applied on both ingress routes
                // (feed and sweep) so a restart re-establishes the halt from the
                // durable record; the insert is idempotent, so the sweep's
                // re-ingest is a no-op. A non-served portfolio leg is harmless to
                // halt (the gate rejects it anyway).
                for portfolio_id in [&trade.buy_portfolio_id, &trade.sell_portfolio_id] {
                    if !portfolio_id.is_empty() {
                        self.self_match_halts.halt(
                            portfolio_id,
                            trade.market_type as i32,
                            trade.contract_id,
                        );
                    }
                }
                // Alarm on the live feed only: the sweep re-fetches the same
                // window each tick, so it re-halts (idempotent) but must not
                // re-alarm.
                if alarm_live {
                    crate::metrics::internal_trade_on_live();
                    tracing::error!(
                        internal_trade_id = %trade.internal_trade_id,
                        contract_id = trade.contract_id,
                        buy_portfolio_id = %trade.buy_portfolio_id,
                        sell_portfolio_id = %trade.sell_portfolio_id,
                        "internal trade on a live gateway (self-match) — unexpected under \
                         IOC-only order entry; trading halted for the (portfolio, contract), \
                         recorded, not relayed"
                    );
                }
            }
            return;
        };
        let mut normalized = Vec::new();
        for trade in &internal {
            let legs = trade.shadow_trades(
                &shadow.exchange,
                &shadow.delivery_area_eic,
                &shadow.portfolio_ids,
            );
            if legs.len() == 2 {
                // Not the own-trades self-trade alarm (these legs are complete
                // records, self_trade = false), but the same anomaly: nothing
                // of ours should rest in the book under IOC-only entry.
                tracing::warn!(
                    internal_trade_id = %trade.internal_trade_id,
                    "shadow self-match (both legs ours) normalized as two offsetting \
                     fills — unexpected under IOC-only order entry"
                );
            }
            normalized.extend(legs);
        }
        self.trades.publish(normalized, received_at_nanos);
    }

    /// Ingest one decoded `OwnOrderChanges`.
    pub fn own_orders(&self, changes: &OwnOrderChanges) {
        let orders = changes.orders.clone().unwrap_or_default();
        if self.persist.is_some() && !orders.is_empty() {
            self.enqueue(Persist::Orders(orders.clone()));
        }
        let orders: Vec<OwnOrder> = orders.into_iter().map(own_order_to_domain).collect();
        self.orders.publish(orders, now_nanos());
    }

    /// Hand a batch to the live-persistence task without blocking the (sync) ws
    /// driver thread. A full queue means the DB can't keep up — drop it and let
    /// the reconciliation sweep catch it; a closed queue means the task is gone
    /// (shutdown).
    fn enqueue(&self, msg: Persist) {
        let Some(persist) = &self.persist else { return };
        match persist.try_send(msg) {
            Ok(()) => {}
            Err(mpsc::error::TrySendError::Full(_)) => {
                tracing::warn!("live persist queue full; reconciliation will catch up")
            }
            Err(mpsc::error::TrySendError::Closed(_)) => {}
        }
    }

    /// Evict past-delivery entries from all three stores at `horizon_millis`
    /// (`now - grace`); returns the count dropped across them. Run periodically to
    /// keep the stores bounded to future-delivery contracts.
    pub fn evict_delivered(&self, horizon_millis: i64) -> usize {
        self.trades.evict_delivered(horizon_millis) + self.orders.evict_delivered(horizon_millis)
    }
}

/// Alarm on self-trades in an applied delta. Both legs of a self-trade are ours,
/// which the gateway's IOC-only order entry should make impossible (nothing of
/// ours ever rests in the book to match against). One is still relayed as-is —
/// `side` and the order identifiers carry the buy leg — but counted and logged
/// loudly: a real one means an order reached the market outside this gateway,
/// and the stream is missing its sell leg.
pub(crate) fn alert_self_trades(delta: &[Trade]) {
    for trade in delta.iter().filter(|t| t.self_trade) {
        crate::metrics::self_trade();
        tracing::warn!(
            trade_id = %trade.trade_id,
            contract_id = trade.contract_id,
            "self-trade reported by PowerBot (both legs ours) — unexpected under IOC-only order entry"
        );
    }
}

/// Lock a store, recovering the guard if a previous holder panicked — a poisoned
/// store is still usable, the worst case being a half-applied batch the next
/// guarded upsert (or the reconciliation sweep) heals.
fn lock<T>(store: &Mutex<T>) -> MutexGuard<'_, T> {
    store.lock().unwrap_or_else(PoisonError::into_inner)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::thread;

    use super::*;

    fn trade(id: &str) -> Trade {
        Trade {
            trade_id: id.to_string(),
            ..Trade::default()
        }
    }

    #[test]
    fn publish_returns_the_applied_delta_and_absorbs_duplicates() {
        let channel = Channel::<Trade>::new(8);

        // A fresh trade is applied and returned as the delta.
        let delta = channel.publish(vec![trade("T1")], 0);
        assert_eq!(delta.len(), 1);

        // Re-publishing the identical trade (the sweep re-fetching its window)
        // is wholly absorbed: no delta, so no re-broadcast and no re-alarm.
        let delta = channel.publish(vec![trade("T1")], 0);
        assert!(delta.is_empty());
    }

    fn internal(id: &str, buy_portfolio: &str, sell_portfolio: &str) -> InternalTrade {
        InternalTrade {
            internal_trade_id: id.to_string(),
            buy_portfolio_id: buy_portfolio.to_string(),
            sell_portfolio_id: sell_portfolio.to_string(),
            quantity: 5000,
            ..InternalTrade::default()
        }
    }

    fn shadow() -> ShadowNormalization {
        ShadowNormalization {
            exchange: "nordpool".to_string(),
            delivery_area_eic: "10YDK-1--------W".to_string(),
            portfolio_ids: vec!["PB-A".to_string()],
        }
    }

    #[test]
    fn a_shadow_gateway_normalizes_internal_trades_onto_the_own_trades_stream() {
        let channels = ExecutionChannels::new(8).with_shadow(shadow());
        let mut trades_rx = channels.trades.subscribe();

        channels.ingest_internal_trades(
            vec![
                internal("I-1", "PB-A", "REPLICA"),
                internal("I-2", "R", "X"),
            ],
            42,
            true,
        );

        // Our leg lands on the own-trades stream as an ordinary trade (the
        // replica-vs-replica match does not).
        let batch = trades_rx.try_recv().expect("normalized trade broadcast");
        assert_eq!(batch.items.len(), 1);
        assert_eq!(batch.items[0].trade_id, "I-1-b");
        assert_eq!(batch.items[0].exchange, "nordpool");

        // And the trades *store* holds it too — a later subscriber's seed
        // carries the normalized fill, not just the live delta.
        let (_, _, seeded) = channels.trades.seed_snapshot();
        assert_eq!(seeded.len(), 1);
        assert_eq!(seeded[0].trade_id, "I-1-b");
    }

    #[test]
    fn a_live_gateway_halts_the_pair_and_keeps_internal_trades_off_the_own_trades_stream() {
        let channels = ExecutionChannels::new(8);
        let mut trades_rx = channels.trades.subscribe();
        let halts = channels.self_match_halts();

        // Internal trades on a live gateway are never normalized or streamed —
        // they are recorded by the caller and alarmed here, and they halt every
        // ours-side leg's (portfolio, contract) so the order gate refuses entry.
        let xbid = crate::grpc::proto::MarketType::Xbid as i32;
        let mut trade = internal("I-1", "PB-A", "PB-B");
        trade.contract_id = 7;
        trade.market_type = crate::grpc::proto::MarketType::Xbid;
        channels.ingest_internal_trades(vec![trade], 42, true);

        assert!(trades_rx.try_recv().is_err(), "no normalization when live");
        // Both legs are halted for that contract; an unrelated pair is not.
        assert!(halts.is_halted("PB-A", xbid, 7));
        assert!(halts.is_halted("PB-B", xbid, 7));
        assert!(!halts.is_halted("PB-A", xbid, 8));

        // The sweep re-ingests the same trade (alarm_live = false): still halted,
        // no panic, no stream.
        let mut again = internal("I-1", "PB-A", "PB-B");
        again.contract_id = 7;
        again.market_type = crate::grpc::proto::MarketType::Xbid;
        channels.ingest_internal_trades(vec![again], 43, false);
        assert!(halts.is_halted("PB-A", xbid, 7));
    }

    #[test]
    fn a_shadow_gateway_does_not_halt() {
        // On a shadow gateway internal trades normalize onto own-trades; nothing
        // is halted (self-matches there are expected, not anomalies).
        let channels = ExecutionChannels::new(8).with_shadow(shadow());
        let halts = channels.self_match_halts();
        let mut trade = internal("I-1", "PB-A", "REPLICA");
        trade.contract_id = 7;
        channels.ingest_internal_trades(vec![trade], 42, true);
        assert!(!halts.is_halted("PB-A", crate::grpc::proto::MarketType::Xbid as i32, 7));
    }

    #[test]
    fn publish_keeps_the_broadcast_monotonic_under_concurrent_producers() {
        // Two producers (the ws driver and the REST reconciliation task, here two
        // OS threads) hammer one channel with distinct keys — so every apply
        // changes state and bumps `seq`. Because `publish` holds the store lock
        // across the send, the frames must arrive strictly in `seq` order; without
        // that, the two producers could reorder between the seq bump and the send.
        const PER_PRODUCER: usize = 2000;
        let channel = Arc::new(Channel::<Trade>::new(4 * PER_PRODUCER));
        // Subscribe before any send so the receiver captures every frame.
        let mut rx = channel.subscribe();

        let producers: Vec<_> = ["a", "b"]
            .into_iter()
            .map(|prefix| {
                let channel = Arc::clone(&channel);
                thread::spawn(move || {
                    for i in 0..PER_PRODUCER {
                        channel.publish(vec![trade(&format!("{prefix}{i}"))], 0);
                    }
                })
            })
            .collect();
        for p in producers {
            p.join().unwrap();
        }

        // Drain the broadcast: the seqs must come out strictly increasing, and
        // cover exactly 1..=2*PER_PRODUCER (one bump per distinct-key publish).
        let mut prev = 0u64;
        let mut count = 0usize;
        loop {
            match rx.try_recv() {
                Ok(batch) => {
                    assert!(
                        batch.seq > prev,
                        "seq {} arrived after {} — frames reordered",
                        batch.seq,
                        prev
                    );
                    prev = batch.seq;
                    count += 1;
                }
                Err(broadcast::error::TryRecvError::Empty)
                | Err(broadcast::error::TryRecvError::Closed) => break,
                Err(broadcast::error::TryRecvError::Lagged(n)) => {
                    panic!("receiver lagged by {n}; raise the channel capacity")
                }
            }
        }
        assert_eq!(count, 2 * PER_PRODUCER);
        assert_eq!(prev, 2 * PER_PRODUCER as u64);
    }
}
