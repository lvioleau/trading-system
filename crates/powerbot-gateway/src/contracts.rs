//! Contract tradability: the store the order gate checks, fed by REST sweeps
//! and the STOMP `contracts` feed.
//!
//! PowerBot's view of whether a contract can be traded right now — active in
//! our delivery area, inside its trading window, delivery not over. The Flow
//! side has its own contract data (contract service / DB), so this store is
//! deliberately the *PowerBot* view: an independent second check that fails an
//! order fast at the gateway instead of round-tripping REST for an order the
//! exchange must refuse.
//!
//! Two writers, one map:
//! - the periodic REST sweep ([`run`]) fetches `findContracts` scoped to the
//!   configured delivery area and upserts everything it can see, including the
//!   per-area state and the `trading_start`/`trading_end` window — fields the
//!   STOMP event does not carry. The first sweep is the startup seed.
//! - the driver thread applies each `ContractChangedEvent` as it arrives
//!   ([`ContractStore::apply_event`]), keeping `trading_enabled` and the
//!   delivery window live between sweeps while leaving the REST-only trading
//!   window untouched.
//!
//! The gate ([`ContractStore::check_tradable`]) rejects only on **definitive
//! knowledge** and forwards everything uncertain — an unknown contract (not
//! swept yet, new since, or older than the sweep window) or one with no
//! negative signal — so a stale or cold store can never block a good order.
//! PowerBot stays the authority for what we can't see. This is also why the
//! store survives a websocket session drop unreset: contracts change rarely,
//! the sweep re-baselines every interval, and staleness at worst forwards an
//! order PowerBot then rejects.
//!
//! Contract ids are held split as `(MarketType, id)` — the wire's
//! representation — with PowerBot's `NX_`/`NI_` prefixed string confined to
//! the boundary via [`split_contract_id`] / [`join_contract_id`].

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use powerbot_models::models::{ContractChangedEvent, ContractItem, delivery_area_state::State};
use tokio::sync::watch;

use crate::grpc::proto::MarketType;
use crate::rest::RestClient;
use crate::time::{millis_rfc3339, now_millis, now_secs, parse_millis, rfc3339};

/// A contract identity as the wire carries it: the market book and the bare
/// nordpool contract id.
pub type ContractKey = (MarketType, i64);

/// Split PowerBot's contract id into its market type and bare nordpool id.
/// PowerBot prefixes the id by book — `NX_` for XBID, `NI_` for local — over
/// the bare integer id the connector's contract feed uses; the wire carries the
/// two apart. An absent, unprefixed, or unparseable id yields
/// `(UNSPECIFIED, 0)`.
pub fn split_contract_id(id: Option<String>) -> ContractKey {
    let s = id.unwrap_or_default();
    let (market_type, digits) = match s.split_once('_') {
        Some(("NX", rest)) => (MarketType::Xbid, rest),
        Some(("NI", rest)) => (MarketType::Local, rest),
        _ => (MarketType::None, s.as_str()),
    };
    (market_type, digits.parse().unwrap_or(0))
}

/// Join a market type and bare nordpool id back into PowerBot's prefixed
/// contract id (`NX_<id>` / `NI_<id>`). `None` for an unspecified market type,
/// which would leave the prefix — and so the target book — ambiguous.
pub fn join_contract_id(market_type: MarketType, id: i64) -> Option<String> {
    let prefix = match market_type {
        MarketType::Xbid => "NX",
        MarketType::Local => "NI",
        MarketType::None => return None,
    };
    Some(format!("{prefix}_{id}"))
}

/// What PowerBot last said about one contract, merged from both sources. Every
/// field is optional — either source may not have reported it yet — and an
/// absent field never rejects.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct ContractInfo {
    /// PowerBot's "tradable in this delivery area right now" flag: the feed's
    /// `trading_enabled`, or the sweep's delivery-area state folded to a bool
    /// (ACTIVE ⇒ true, INACTIVE/HIBERNATE ⇒ false, STANDBY ⇒ left as-is: not
    /// definitive either way). Last writer wins — the feed pushes transitions
    /// live, the sweep re-baselines every interval.
    pub trading_enabled: Option<bool>,
    /// Delivery window (unix millis), from either source.
    pub delivery_start: Option<i64>,
    pub delivery_end: Option<i64>,
    /// Trading window (unix millis) for our delivery area — REST-only (the
    /// feed event doesn't carry it): populated by the sweep, never touched by
    /// a feed update.
    pub trading_start: Option<i64>,
    pub trading_end: Option<i64>,
}

impl ContractInfo {
    /// Why this contract definitively can't trade at `now_millis`, or `None`
    /// when nothing negative is known. Deliberately *not* checking
    /// `delivery_start`: whether trading runs into the delivery period is an
    /// exchange/product rule, and `trading_end`/`trading_enabled` already
    /// carry PowerBot's answer.
    fn not_tradable(&self, now_millis: i64) -> Option<String> {
        if self.trading_enabled == Some(false) {
            return Some("trading disabled in the delivery area".to_string());
        }
        if let Some(start) = self.trading_start
            && now_millis < start
        {
            return Some(format!("trading opens at {}", millis_rfc3339(start)));
        }
        if let Some(end) = self.trading_end
            && now_millis >= end
        {
            return Some(format!("trading closed at {}", millis_rfc3339(end)));
        }
        if let Some(end) = self.delivery_end
            && now_millis >= end
        {
            return Some(format!("delivery ended at {}", millis_rfc3339(end)));
        }
        None
    }
}

/// Shared map of what PowerBot last said about each contract. Clones share one
/// map. The mutex is held briefly by the driver thread (feed updates), the
/// sweep task, and the order gate — never across an await.
#[derive(Debug, Clone, Default)]
pub struct ContractStore {
    by_key: Arc<Mutex<HashMap<ContractKey, ContractInfo>>>,
}

impl ContractStore {
    /// Number of contracts currently known.
    pub fn len(&self) -> usize {
        self.lock().len()
    }

    pub fn is_empty(&self) -> bool {
        self.lock().is_empty()
    }

    /// Apply one STOMP `ContractChangedEvent`: upsert the live fields
    /// (`trading_enabled`, delivery window), leave the REST-only trading
    /// window untouched. A keyless event (no id, or an unprefixed id) is
    /// dropped — it can't be looked up at order entry either.
    pub fn apply_event(&self, event: &ContractChangedEvent) {
        let key = split_contract_id(event.contract_id.clone());
        if key.0 == MarketType::None || key.1 == 0 {
            tracing::warn!(contract_id = ?event.contract_id, "contract event without a usable id; dropped");
            return;
        }
        let mut map = self.lock();
        let info = map.entry(key).or_default();
        if let Some(enabled) = event.trading_enabled {
            info.trading_enabled = Some(enabled);
        }
        if let Some(ms) = parse_millis(event.delivery_start.as_deref()) {
            info.delivery_start = Some(ms);
        }
        if let Some(ms) = parse_millis(event.delivery_end.as_deref()) {
            info.delivery_end = Some(ms);
        }
    }

    /// Apply one REST `findContracts` snapshot: upsert everything, including
    /// the state and trading window of the `delivery_area_eic` entry (the sweep is
    /// area-scoped, but each item still lists per-area states — pick ours).
    pub fn apply_snapshot(&self, items: &[ContractItem], delivery_area_eic: &str) {
        let mut map = self.lock();
        for item in items {
            let key = split_contract_id(item.contract_id.clone());
            if key.0 == MarketType::None || key.1 == 0 {
                tracing::warn!(contract_id = ?item.contract_id, "contract item without a usable id; skipped");
                continue;
            }
            let info = map.entry(key).or_default();
            if let Some(ms) = parse_millis(item.delivery_start.as_deref()) {
                info.delivery_start = Some(ms);
            }
            if let Some(ms) = parse_millis(item.delivery_end.as_deref()) {
                info.delivery_end = Some(ms);
            }
            let area = item
                .delivery_area_states
                .iter()
                .flatten()
                .find(|s| s.delivery_area.as_deref() == Some(delivery_area_eic));
            if let Some(area) = area {
                match area.state {
                    Some(State::Active) => info.trading_enabled = Some(true),
                    Some(State::Inactive | State::Hibernate) => {
                        info.trading_enabled = Some(false);
                    }
                    // STANDBY says nothing definitive; keep what the feed said.
                    Some(State::Standby) | None => {}
                }
                if let Some(ms) = parse_millis(area.trading_start.as_deref()) {
                    info.trading_start = Some(ms);
                }
                if let Some(ms) = parse_millis(area.trading_end.as_deref()) {
                    info.trading_end = Some(ms);
                }
            }
        }
    }

    /// The order gate: `Err(reason)` when PowerBot has definitively said the
    /// contract can't trade at `now_millis`; `Ok` otherwise — including for a
    /// contract the store has never heard of, which forwards so PowerBot can
    /// decide.
    pub fn check_tradable(&self, key: ContractKey, now_millis: i64) -> Result<(), String> {
        match self.lock().get(&key) {
            Some(info) => info.not_tradable(now_millis).map_or(Ok(()), Err),
            None => Ok(()),
        }
    }

    /// Evict contracts whose delivery ended at or before `horizon_millis`
    /// (`now - grace`, same horizon as the execution stores). Contracts with
    /// no known delivery end are kept. Returns how many were evicted.
    pub fn evict_delivered(&self, horizon_millis: i64) -> usize {
        let mut map = self.lock();
        let before = map.len();
        map.retain(|_, info| match info.delivery_end {
            Some(end) => end > horizon_millis,
            None => true,
        });
        before - map.len()
    }

    /// A copy of one contract's info, for tests and debugging.
    pub fn get(&self, key: ContractKey) -> Option<ContractInfo> {
        self.lock().get(&key).cloned()
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, HashMap<ContractKey, ContractInfo>> {
        // Poisoning can't leave the map half-written (every write is a whole
        // field), so a panicked writer doesn't invalidate the data.
        self.by_key
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

/// What to sweep: the exchange path segment, the delivery area scoping the
/// fetch (and whose per-area state the store folds), the cadence, and the
/// eviction horizon shared with the execution stores.
pub struct ContractPollConfig {
    pub exchange: String,
    pub delivery_area_eic: String,
    /// How often to re-fetch `findContracts` (and evict).
    pub interval: Duration,
    /// Fetch window lookback and eviction horizon: contracts whose delivery
    /// ended longer ago than this are neither fetched nor retained.
    pub eviction_grace: Duration,
}

/// Run the contract sweep until `shutdown` flips: fetch, apply, evict, wait.
/// The first sweep is immediate — the startup seed for the order gate's
/// contract check. A failed sweep is logged and counted, then retried on the
/// next tick; the feed keeps the store's live fields current in between, and
/// the gate's forward-on-unknown policy makes a missed sweep harmless.
pub async fn run(
    rest: RestClient,
    store: ContractStore,
    cfg: ContractPollConfig,
    mut shutdown: watch::Receiver<bool>,
) {
    loop {
        let from = rfc3339(now_secs().saturating_sub(cfg.eviction_grace.as_secs()));
        match rest
            .find_contracts(&cfg.exchange, &cfg.delivery_area_eic, &from)
            .await
        {
            Ok(items) => {
                store.apply_snapshot(&items, &cfg.delivery_area_eic);
                let horizon = now_millis() - cfg.eviction_grace.as_millis() as i64;
                let evicted = store.evict_delivered(horizon);
                crate::metrics::set_contracts_known(store.len());
                tracing::info!(
                    fetched = items.len(),
                    evicted,
                    known = store.len(),
                    "contract sweep applied"
                );
            }
            Err(error) => {
                crate::metrics::contract_poll_failure();
                tracing::warn!(exchange = %cfg.exchange, %error, "contract sweep failed; retrying next tick");
            }
        }
        tokio::select! {
            _ = tokio::time::sleep(cfg.interval) => {}
            _ = shutdown.wait_for(|&stop| stop) => break,
        }
    }
    tracing::info!("contract sweep stopped");
}

#[cfg(test)]
mod tests {
    use super::*;
    use powerbot_models::models::DeliveryAreaState;

    const AREA: &str = "10YFR-RTE------C";
    const HOUR: i64 = 3_600_000;

    /// 2026-07-10T12:00:00Z in unix millis — inside the fixtures' trading
    /// window, before their delivery.
    fn noon() -> i64 {
        parse_millis(Some("2026-07-10T12:00:00Z")).unwrap()
    }

    fn event(id: &str, enabled: Option<bool>) -> ContractChangedEvent {
        ContractChangedEvent {
            contract_id: Some(id.to_string()),
            trading_enabled: enabled,
            delivery_start: Some("2026-07-10T14:00:00Z".to_string()),
            delivery_end: Some("2026-07-10T15:00:00Z".to_string()),
            ..ContractChangedEvent::new()
        }
    }

    fn item(id: &str, state: Option<State>) -> ContractItem {
        ContractItem {
            contract_id: Some(id.to_string()),
            delivery_start: Some("2026-07-10T14:00:00Z".to_string()),
            delivery_end: Some("2026-07-10T15:00:00Z".to_string()),
            delivery_area_states: Some(vec![DeliveryAreaState {
                delivery_area: Some(AREA.to_string()),
                state,
                trading_start: Some("2026-07-09T14:00:00Z".to_string()),
                trading_end: Some("2026-07-10T13:55:00Z".to_string()),
            }]),
            ..ContractItem::new()
        }
    }

    #[test]
    fn contract_ids_split_and_join() {
        assert_eq!(
            split_contract_id(Some("NX_12345".to_string())),
            (MarketType::Xbid, 12345)
        );
        assert_eq!(
            split_contract_id(Some("NI_678".to_string())),
            (MarketType::Local, 678)
        );
        // Absent or unprefixed ids degrade to (UNSPECIFIED, 0) rather than panic.
        assert_eq!(split_contract_id(None), (MarketType::None, 0));
        assert_eq!(
            split_contract_id(Some("weird".to_string())),
            (MarketType::None, 0)
        );
        // Round-trips with the join used on order entry.
        assert_eq!(
            join_contract_id(MarketType::Xbid, 12345).as_deref(),
            Some("NX_12345")
        );
        assert_eq!(
            join_contract_id(MarketType::Local, 678).as_deref(),
            Some("NI_678")
        );
        // An unspecified market type has no unambiguous prefix.
        assert_eq!(join_contract_id(MarketType::None, 678), None);
    }

    #[test]
    fn unknown_contract_forwards() {
        let store = ContractStore::default();
        assert_eq!(store.check_tradable((MarketType::Xbid, 1), noon()), Ok(()));
    }

    #[test]
    fn feed_disable_rejects_and_reenable_forwards() {
        let store = ContractStore::default();
        store.apply_event(&event("NX_1", Some(false)));
        let err = store
            .check_tradable((MarketType::Xbid, 1), noon())
            .unwrap_err();
        assert!(err.contains("disabled"), "{err}");

        store.apply_event(&event("NX_1", Some(true)));
        assert_eq!(store.check_tradable((MarketType::Xbid, 1), noon()), Ok(()));
    }

    #[test]
    fn feed_event_without_the_flag_leaves_it_unchanged() {
        let store = ContractStore::default();
        store.apply_event(&event("NX_1", Some(false)));
        // A later event that doesn't carry trading_enabled must not erase the
        // known-disabled state.
        store.apply_event(&event("NX_1", None));
        assert!(store.check_tradable((MarketType::Xbid, 1), noon()).is_err());
    }

    #[test]
    fn snapshot_populates_the_trading_window_and_the_feed_preserves_it() {
        let store = ContractStore::default();
        store.apply_snapshot(&[item("NX_1", Some(State::Active))], AREA);

        let info = store.get((MarketType::Xbid, 1)).unwrap();
        assert_eq!(info.trading_enabled, Some(true));
        assert!(info.trading_start.is_some() && info.trading_end.is_some());

        // A feed update flips the live flag but leaves the REST-only window.
        store.apply_event(&event("NX_1", Some(false)));
        let after = store.get((MarketType::Xbid, 1)).unwrap();
        assert_eq!(after.trading_enabled, Some(false));
        assert_eq!(after.trading_start, info.trading_start);
        assert_eq!(after.trading_end, info.trading_end);
    }

    #[test]
    fn snapshot_area_states_fold_to_the_tradable_flag() {
        for (state, expect) in [
            (Some(State::Active), Ok(())),
            (
                Some(State::Inactive),
                Err("trading disabled in the delivery area".to_string()),
            ),
            (
                Some(State::Hibernate),
                Err("trading disabled in the delivery area".to_string()),
            ),
        ] {
            let store = ContractStore::default();
            // Check at a time inside the item's trading window so only the
            // area state decides.
            let store_check = {
                store.apply_snapshot(&[item("NX_1", state)], AREA);
                store.check_tradable((MarketType::Xbid, 1), noon())
            };
            assert_eq!(store_check, expect, "{state:?}");
        }
    }

    #[test]
    fn standby_is_not_definitive_and_keeps_the_previous_flag() {
        let store = ContractStore::default();
        store.apply_event(&event("NX_1", Some(false)));
        store.apply_snapshot(&[item("NX_1", Some(State::Standby))], AREA);
        // STANDBY neither enables nor disables: the feed's `false` stands.
        assert!(store.check_tradable((MarketType::Xbid, 1), noon()).is_err());
    }

    #[test]
    fn a_foreign_delivery_areas_state_is_ignored() {
        let store = ContractStore::default();
        store.apply_snapshot(&[item("NX_1", Some(State::Inactive))], "10YDK-1--------W");
        // The item's only area entry is for FR; sweeping for DK folds nothing.
        assert_eq!(store.check_tradable((MarketType::Xbid, 1), noon()), Ok(()));
    }

    #[test]
    fn trading_window_bounds_reject_outside_and_forward_inside() {
        let store = ContractStore::default();
        store.apply_snapshot(&[item("NX_1", Some(State::Active))], AREA);
        let key = (MarketType::Xbid, 1);
        let info = store.get(key).unwrap();
        let (start, end) = (info.trading_start.unwrap(), info.trading_end.unwrap());

        // Before the window opens.
        let err = store.check_tradable(key, start - HOUR).unwrap_err();
        assert!(err.contains("opens"), "{err}");
        // Inside the window.
        assert_eq!(store.check_tradable(key, start), Ok(()));
        // At/after the close (inclusive end).
        let err = store.check_tradable(key, end).unwrap_err();
        assert!(err.contains("closed"), "{err}");
    }

    #[test]
    fn past_delivery_rejects_even_without_a_trading_window() {
        let store = ContractStore::default();
        // Feed-only knowledge: delivery 14:00–15:00, no trading window.
        store.apply_event(&event("NX_1", Some(true)));
        let end = store
            .get((MarketType::Xbid, 1))
            .unwrap()
            .delivery_end
            .unwrap();
        let err = store
            .check_tradable((MarketType::Xbid, 1), end)
            .unwrap_err();
        assert!(err.contains("delivery ended"), "{err}");
    }

    #[test]
    fn evict_drops_past_delivery_keeps_future_and_undated() {
        let store = ContractStore::default();
        store.apply_event(&event("NX_1", Some(true))); // delivery ends 15:00
        store.apply_event(&ContractChangedEvent {
            contract_id: Some("NX_2".to_string()),
            ..ContractChangedEvent::new()
        }); // no delivery end
        let end = store
            .get((MarketType::Xbid, 1))
            .unwrap()
            .delivery_end
            .unwrap();

        assert_eq!(store.evict_delivered(end - 1), 0);
        assert_eq!(store.evict_delivered(end), 1);
        assert_eq!(store.len(), 1); // the undated one is kept
    }

    #[test]
    fn keyless_events_and_items_are_dropped() {
        let store = ContractStore::default();
        store.apply_event(&event("weird", Some(false)));
        store.apply_event(&ContractChangedEvent::new());
        store.apply_snapshot(&[ContractItem::new()], AREA);
        assert!(store.is_empty());
    }
}
