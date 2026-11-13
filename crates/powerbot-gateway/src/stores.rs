//! In-memory seed stores for the execution feeds.
//!
//! Three stores hold the state a new gRPC subscriber is seeded with, split by
//! how the Position Service folds them: **external trades** (one side ours —
//! they move net external position → PE), **internal trades** (both sides ours —
//! a transfer between our own portfolios), and **own orders**. Each is the
//! nordpool-connectors store shape: a map keyed by identity plus a monotonic
//! `seq`. The `seq` lets a subscriber snapshot the store and then drop the
//! seed/delta overlap (`seq > seed_seq`) without a lock held across snapshot and
//! subscribe — see the writer side in the ingest task and docs/design.md,
//! "Stores and gRPC streams".
//!
//! This layer is pure and lock-free: the `Mutex` guarding a store and the
//! broadcast it feeds live one layer up (ingest/feed), so everything here is
//! directly unit-testable with no clock and no async.

use std::collections::HashMap;

use crate::domain::{InternalTrade, OwnOrder, Trade};

/// External trades, keyed by `trade_id`.
pub type TradeStore = SeedStore<Trade>;
/// Internal trades (portfolio-to-portfolio transfers), keyed by
/// `internal_trade_id`.
pub type InternalTradeStore = SeedStore<InternalTrade>;
/// Own orders, keyed by `order_id`, never-regressing on `revision_no`.
pub type OrderStore = SeedStore<OwnOrder>;

/// An item a [`SeedStore`] can hold: a stable identity to key on, a monotone
/// version for the never-regress guard, and a delivery end for future-delivery
/// eviction.
pub trait Seedable: Clone + PartialEq {
    /// Stable identity; an item without one can't be keyed and is dropped.
    fn key(&self) -> Option<&str>;

    /// Monotone version for the never-regress guard. An incoming item is
    /// rejected only when both it and the stored one carry a version and the
    /// incoming is strictly lower; `None` on either side means last-write-wins.
    fn version(&self) -> Option<i64>;

    /// Delivery end as unix milliseconds, for eviction once delivery is in the
    /// past. `None` (no delivery end) is never evicted.
    fn delivery_end_millis(&self) -> Option<i64>;

    /// Whether `other` carries no change worth broadcasting relative to `self` —
    /// the same item modulo volatile metadata a reconciliation re-fetch can
    /// restate without any real change. It defaults to strict equality; the trade
    /// types relax it because PowerBot's REST snapshots round `api_timestamp` /
    /// `exec_time` to microseconds while the websocket feed carries nanoseconds,
    /// so a re-fetched but otherwise-identical trade would otherwise look changed
    /// and be needlessly re-broadcast (the never-regress `version()` already sees
    /// them as equal at millisecond granularity).
    fn unchanged(&self, other: &Self) -> bool {
        self == other
    }
}

/// A map of the latest [`Seedable`] per key plus the monotonic `seq` that stamps
/// each state change. Guarded upsert (never-regress, duplicate-suppressing) so
/// every ingress route — websocket deltas, REST snapshots, the reconciliation
/// sweep — converges idempotently on the same state.
#[derive(Debug)]
pub struct SeedStore<T> {
    by_key: HashMap<String, T>,
    seq: u64,
}

impl<T: Seedable> Default for SeedStore<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T: Seedable> SeedStore<T> {
    /// An empty store at `seq` 0 (the seed a subscriber seeing nothing yet gets;
    /// the first applied change advances it to 1).
    pub fn new() -> Self {
        Self {
            by_key: HashMap::new(),
            seq: 0,
        }
    }

    /// The current sequence number — the value a seed taken now is current as of.
    pub fn seq(&self) -> u64 {
        self.seq
    }

    /// Number of items currently held.
    pub fn len(&self) -> usize {
        self.by_key.len()
    }

    /// Whether the store holds no items.
    pub fn is_empty(&self) -> bool {
        self.by_key.is_empty()
    }

    /// Guarded upsert of one batch. Keyless items, regressions, and re-delivered
    /// duplicates are skipped. `seq` is bumped **once** iff at least one item
    /// actually changed state, so a wholly redundant batch (feed/sweep overlap)
    /// broadcasts nothing. Returns the post-apply `seq` and the items that were
    /// inserted or updated — the delta the caller broadcasts (empty ⇒ nothing to
    /// send).
    ///
    /// The caller applies under the store's mutex and broadcasts *after* this
    /// returns; that "apply-then-broadcast" ordering is what makes the
    /// seed/delta handoff gap-free (docs/design.md).
    pub fn apply<I: IntoIterator<Item = T>>(&mut self, items: I) -> (u64, Vec<T>) {
        let mut changed = Vec::new();
        for item in items {
            let Some(key) = item.key().map(str::to_owned) else {
                continue;
            };
            if let Some(existing) = self.by_key.get(&key)
                && (regresses(existing, &item) || existing.unchanged(&item))
            {
                continue;
            }
            self.by_key.insert(key, item.clone());
            changed.push(item);
        }
        if !changed.is_empty() {
            self.seq += 1;
        }
        (self.seq, changed)
    }

    /// The full current state for a new subscriber's seed, paired with the `seq`
    /// it is current as of. The subscriber forwards only later deltas
    /// (`seq > seed_seq`), so seed and deltas together cover every change with no
    /// gap and no duplicate.
    pub fn seed(&self) -> (u64, Vec<T>) {
        (self.seq, self.by_key.values().cloned().collect())
    }

    /// Evict entries whose delivery ended at or before `horizon_millis` — the
    /// caller passes `now_millis - grace`, keeping just-delivered contracts for
    /// the grace window (the Position Service still wants a hour that finished
    /// minutes ago). Items with no delivery end are kept. Returns how many were
    /// evicted.
    ///
    /// Eviction does **not** touch `seq` or broadcast: an evicted item is past
    /// delivery, so it is neither seeded to a new subscriber nor a delta a live
    /// one needs — it carries its own `delivery_end` and can prune on its clock.
    pub fn evict_delivered(&mut self, horizon_millis: i64) -> usize {
        let before = self.by_key.len();
        self.by_key
            .retain(|_, item| match item.delivery_end_millis() {
                Some(end) => end > horizon_millis,
                None => true,
            });
        before - self.by_key.len()
    }
}

/// Whether `incoming` would regress `existing`: both carry a version and the
/// incoming is strictly older. Without a version on either side there is no
/// ordering to enforce, so it does not regress (last-write-wins).
fn regresses<T: Seedable>(existing: &T, incoming: &T) -> bool {
    match (existing.version(), incoming.version()) {
        (Some(cur), Some(new)) => new < cur,
        _ => false,
    }
}

// The `Seedable` implementations for the stored types live with them in
// [`crate::domain`] (they're pure field access).

#[cfg(test)]
mod tests {
    use super::*;
    use crate::grpc::proto::{OrderAction, OrderSide, OrderState};

    /// Nanoseconds per millisecond, so tests can spell delivery/version times in
    /// round milliseconds.
    const MS: i64 = 1_000_000;

    /// `api_nanos` = 0 means "no version" (the never-regress guard sees `None`).
    fn trade(id: &str, api_nanos: i64, delivery_end_nanos: Option<i64>) -> Trade {
        Trade {
            trade_id: id.to_string(),
            api_timestamp_nanos: api_nanos,
            delivery_end_nanos,
            quantity: 5000,
            side: OrderSide::Buy,
            ..Trade::default()
        }
    }

    fn order(id: &str, revision: i64, delivery_end_nanos: Option<i64>) -> OwnOrder {
        OwnOrder {
            order_id: id.to_string(),
            revision_no: revision,
            delivery_end_nanos,
            state: OrderState::Acti,
            side: OrderSide::Buy,
            action: OrderAction::Uadd,
            delivery_area_eic: "10YFR-RTE------C".to_string(),
            ..OwnOrder::default()
        }
    }

    #[test]
    fn apply_inserts_and_keys_by_id() {
        let mut store = TradeStore::new();
        let (seq, changed) = store.apply([trade("T1", 0, None), trade("T2", 0, None)]);
        assert_eq!(seq, 1);
        assert_eq!(changed.len(), 2);
        assert_eq!(store.len(), 2);
    }

    #[test]
    fn apply_drops_keyless_items() {
        let mut store = TradeStore::new();
        let keyless = Trade {
            trade_id: String::new(),
            ..Trade::default()
        };
        let (seq, changed) = store.apply([keyless]);
        // Nothing could be keyed, so nothing changed and seq stayed put.
        assert_eq!(seq, 0);
        assert!(changed.is_empty());
        assert!(store.is_empty());
    }

    #[test]
    fn apply_bumps_seq_once_per_batch_that_changes_state() {
        let mut store = TradeStore::new();
        store.apply([trade("T1", 0, None)]);
        assert_eq!(store.seq(), 1);
        store.apply([trade("T2", 0, None), trade("T3", 0, None)]);
        // One bump for the whole batch, not one per item.
        assert_eq!(store.seq(), 2);
    }

    #[test]
    fn re_delivered_duplicate_is_suppressed() {
        let mut store = TradeStore::new();
        store.apply([trade("T1", 5000 * MS, None)]);
        let (seq, changed) = store.apply([trade("T1", 5000 * MS, None)]);
        // Identical item: no delta, no seq bump.
        assert_eq!(seq, 1);
        assert!(changed.is_empty());
    }

    #[test]
    fn orders_never_regress_on_revision() {
        let mut store = OrderStore::new();
        store.apply([order("O1", 5, None)]);
        // A lower revision (e.g. a stale sweep) is rejected.
        let (seq, changed) = store.apply([order("O1", 3, None)]);
        assert_eq!(seq, 1);
        assert!(changed.is_empty());
        // The same or higher revision is applied.
        let (seq, changed) = store.apply([order("O1", 6, None)]);
        assert_eq!(seq, 2);
        assert_eq!(changed.len(), 1);
        assert_eq!(changed[0].revision_no, 6);
    }

    #[test]
    fn trades_guard_against_a_stale_api_timestamp() {
        let mut store = TradeStore::new();
        store.apply([trade("T1", 5000 * MS, None)]);
        // An older api_timestamp for the same trade — a sweep that raced a feed
        // update — must not overwrite the fresher state.
        let (_, changed) = store.apply([trade("T1", 1000 * MS, None)]);
        assert!(changed.is_empty());
    }

    #[test]
    fn a_refetch_differing_only_in_timestamp_precision_is_absorbed() {
        let mut store = TradeStore::new();
        // Live feed: nanosecond api_timestamp and exec_time.
        let live = Trade {
            exec_time_nanos: 1_450_424_892,
            ..trade("T1", 1_450_424_892, None)
        };
        let (seq, changed) = store.apply([live]);
        assert_eq!(seq, 1);
        assert_eq!(changed.len(), 1);

        // REST reconciliation re-fetch of the same trade: identical but the
        // timestamps are rounded to microseconds (same millisecond). It must be
        // absorbed — no delta, no seq bump — so the sweep doesn't re-broadcast it.
        let refetch = Trade {
            exec_time_nanos: 1_450_425_000,
            ..trade("T1", 1_450_425_000, None)
        };
        let (seq, changed) = store.apply([refetch]);
        assert_eq!(seq, 1, "a precision-only re-fetch bumps no seq");
        assert!(changed.is_empty(), "and is not re-broadcast");

        // The stored value keeps the original nanosecond precision (the re-fetch
        // was dropped, not applied).
        let (_, items) = store.seed();
        assert_eq!(items[0].exec_time_nanos, 1_450_424_892);
    }

    #[test]
    fn a_real_change_still_broadcasts_despite_timestamp_precision_drift() {
        let mut store = TradeStore::new();
        store.apply([Trade {
            exec_time_nanos: 1_450_424_892,
            ..trade("T1", 1_450_424_892, None)
        }]);
        // Same trade id, timestamps drift to µs, but the quantity is corrected —
        // a genuine change that must still produce a delta.
        let corrected = Trade {
            quantity: 9000,
            exec_time_nanos: 1_450_425_000,
            ..trade("T1", 1_450_425_000, None)
        };
        let (_, changed) = store.apply([corrected]);
        assert_eq!(changed.len(), 1, "a real field change is still broadcast");
        assert_eq!(changed[0].quantity, 9000);
    }

    #[test]
    fn without_a_version_last_write_wins() {
        let mut store = TradeStore::new();
        store.apply([trade("T1", 0, None)]);
        // No api_timestamp on either side, but a genuinely different item
        // (changed quantity) still applies.
        let updated = Trade {
            quantity: 9000,
            ..trade("T1", 0, None)
        };
        let (_, changed) = store.apply([updated]);
        assert_eq!(changed.len(), 1);
        assert_eq!(changed[0].quantity, 9000);
    }

    #[test]
    fn seed_returns_current_state_and_seq() {
        let mut store = OrderStore::new();
        store.apply([order("O1", 1, None), order("O2", 1, None)]);
        let (seq, items) = store.seed();
        assert_eq!(seq, 1);
        assert_eq!(items.len(), 2);
    }

    #[test]
    fn evict_removes_past_delivery_keeps_future_and_undated() {
        let mut store = TradeStore::new();
        store.apply([
            trade("PAST", 0, Some(10_000 * MS)),
            trade("FUTURE", 0, Some(14_000 * MS)),
            trade("UNDATED", 0, None),
        ]);
        let seq_before = store.seq();
        // Horizon at 12_000ms — PAST is delivered, FUTURE is not, UNDATED is kept.
        let evicted = store.evict_delivered(12_000);
        assert_eq!(evicted, 1);
        assert_eq!(store.len(), 2);
        // Eviction is not a delta: seq is untouched.
        assert_eq!(store.seq(), seq_before);
    }

    #[test]
    fn evict_keeps_delivery_end_exactly_at_grace_horizon_out_by_strict_gt() {
        let mut store = TradeStore::new();
        store.apply([trade("EDGE", 0, Some(12_000 * MS))]);
        // `end > horizon` is strict, so an item ending exactly at the horizon is
        // evicted (delivery is over).
        assert_eq!(store.evict_delivered(12_000), 1);
    }
}
