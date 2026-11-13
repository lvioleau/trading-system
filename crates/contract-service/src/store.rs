//! The in-memory contract index — full history, all configured delivery areas. Pure domain:
//! no protobuf, no IO; the feed client translates at the boundary.
//!
//! Unlike the connector's live set (where a feed snapshot replaces the whole set), this store
//! is upsert-only: contracts never disappear, they change state and their delivery window
//! passes. Out-of-order and duplicate frames are guarded by the venue's `updated_at` — intrinsic
//! fields and each per-area state carry their own guard, because the same contract can arrive
//! from more than one source (reconnect re-seeds today; multiple area-connectors later), each
//! lagging independently.

use std::collections::{BTreeMap, HashMap, hash_map::Entry};

use eic::EicCode;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum MarketType {
    Local,
    Xbid,
}

/// The 15/30/60-minute periodic products; every other wire shape (blocks, custom shapes)
/// collapses to [`Other`](ProductType::Other).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ProductType {
    P15Min,
    P30Min,
    P60Min,
    Other,
}

/// A contract's lifecycle state in one delivery area.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ContractState {
    Unknown,
    Active,
    Hibernated,
    Inactive,
    Frozen,
}

/// A contract's natural identity. Nord Pool contract ids are stable per venue but not assumed
/// unique across the Local and Xbid market segments, so `market_type` is part of the key.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ContractKey {
    pub market_type: MarketType,
    pub contract_id: i64,
}

/// Lifecycle state in one delivery area, stamped with the venue update time of the frame that
/// set it — the per-area upsert guard.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AreaState {
    pub state: ContractState,
    pub updated_at_ms: i64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Contract {
    pub key: ContractKey,
    pub name: String,
    pub market_id: String,
    pub product_type: ProductType,
    pub delivery_start_ms: i64,
    pub delivery_end_ms: i64,
    pub duration_seconds: i32,
    /// Venue update time — the upsert guard for the intrinsic fields above.
    pub updated_at_ms: i64,
    /// Lifecycle state per delivery area, each entry guarded by its own update time.
    pub areas: BTreeMap<EicCode, AreaState>,
}

/// Every contract ever seen, keyed by natural identity. Insert-only: neither contracts nor
/// area states are removed.
#[derive(Debug, Default)]
pub struct ContractStore {
    contracts: HashMap<ContractKey, Contract>,
}

impl ContractStore {
    pub fn new() -> Self {
        Self::default()
    }

    /// Fold one upsert in: intrinsic fields apply when strictly newer than the stored contract,
    /// each area state when strictly newer than that area's stored state (an unseen area is
    /// always added). Replays and reconnect re-seeds are therefore no-ops, and a stale frame
    /// can't regress state. Returns whether anything was applied.
    pub fn upsert(&mut self, incoming: Contract) -> bool {
        match self.contracts.entry(incoming.key) {
            Entry::Vacant(entry) => {
                entry.insert(incoming);
                true
            }
            Entry::Occupied(mut entry) => {
                let current = entry.get_mut();
                let mut changed = false;
                if incoming.updated_at_ms > current.updated_at_ms {
                    current.name = incoming.name;
                    current.market_id = incoming.market_id;
                    current.product_type = incoming.product_type;
                    current.delivery_start_ms = incoming.delivery_start_ms;
                    current.delivery_end_ms = incoming.delivery_end_ms;
                    current.duration_seconds = incoming.duration_seconds;
                    current.updated_at_ms = incoming.updated_at_ms;
                    changed = true;
                }
                for (area, state) in incoming.areas {
                    let applied = match current.areas.get(&area) {
                        None => true,
                        Some(existing) => state.updated_at_ms > existing.updated_at_ms,
                    };
                    if applied {
                        current.areas.insert(area, state);
                        changed = true;
                    }
                }
                changed
            }
        }
    }

    pub fn get(&self, key: &ContractKey) -> Option<&Contract> {
        self.contracts.get(key)
    }

    pub fn len(&self) -> usize {
        self.contracts.len()
    }

    pub fn is_empty(&self) -> bool {
        self.contracts.is_empty()
    }

    /// Iteration order is unspecified (backed by a `HashMap`).
    pub fn iter(&self) -> impl Iterator<Item = &Contract> {
        self.contracts.values()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const FR: EicCode = EicCode::AREA_FR;
    const CH: EicCode = EicCode::AREA_CH;

    fn key(contract_id: i64) -> ContractKey {
        ContractKey {
            market_type: MarketType::Local,
            contract_id,
        }
    }

    fn contract(
        contract_id: i64,
        updated_at_ms: i64,
        areas: &[(EicCode, ContractState)],
    ) -> Contract {
        Contract {
            key: key(contract_id),
            name: format!("PH-{contract_id}"),
            market_id: "M1".to_owned(),
            product_type: ProductType::P15Min,
            delivery_start_ms: 1_000,
            delivery_end_ms: 1_900,
            duration_seconds: 900,
            updated_at_ms,
            areas: areas
                .iter()
                .map(|&(area, state)| {
                    (
                        area,
                        AreaState {
                            state,
                            updated_at_ms,
                        },
                    )
                })
                .collect(),
        }
    }

    #[test]
    fn upsert_inserts_unknown_contract() {
        // Given an empty store
        let mut store = ContractStore::new();

        // When a contract arrives
        let changed = store.upsert(contract(1, 10, &[(FR, ContractState::Active)]));

        // Then it is stored whole
        assert!(changed);
        assert_eq!(store.len(), 1);
        assert_eq!(store.get(&key(1)).unwrap().updated_at_ms, 10);
    }

    #[test]
    fn newer_frame_updates_intrinsics() {
        // Given a stored contract
        let mut store = ContractStore::new();
        store.upsert(contract(1, 10, &[(FR, ContractState::Active)]));

        // When a strictly newer frame arrives with changed fields
        let mut newer = contract(1, 20, &[(FR, ContractState::Hibernated)]);
        newer.name = "renamed".to_owned();
        let changed = store.upsert(newer);

        // Then intrinsics and the area state both advance
        assert!(changed);
        let stored = store.get(&key(1)).unwrap();
        assert_eq!(stored.name, "renamed");
        assert_eq!(stored.updated_at_ms, 20);
        assert_eq!(stored.areas[&FR].state, ContractState::Hibernated);
    }

    #[test]
    fn stale_frame_is_ignored() {
        // Given a stored contract at t=20
        let mut store = ContractStore::new();
        store.upsert(contract(1, 20, &[(FR, ContractState::Hibernated)]));

        // When an older frame arrives
        let changed = store.upsert(contract(1, 10, &[(FR, ContractState::Active)]));

        // Then nothing regresses
        assert!(!changed);
        let stored = store.get(&key(1)).unwrap();
        assert_eq!(stored.updated_at_ms, 20);
        assert_eq!(stored.areas[&FR].state, ContractState::Hibernated);
    }

    #[test]
    fn reapplying_the_same_frame_is_a_noop() {
        // Given a stored contract (e.g. from the seed before a reconnect)
        let mut store = ContractStore::new();
        store.upsert(contract(1, 10, &[(FR, ContractState::Active)]));

        // When the same frame is re-applied (the reconnect re-seed)
        let changed = store.upsert(contract(1, 10, &[(FR, ContractState::Active)]));

        // Then it is a no-op
        assert!(!changed);
        assert_eq!(store.len(), 1);
    }

    #[test]
    fn area_states_merge_across_subscriptions() {
        // Given the same contract seen by the FR subscription
        let mut store = ContractStore::new();
        store.upsert(contract(1, 10, &[(FR, ContractState::Active)]));

        // When the CH subscription delivers it too
        let changed = store.upsert(contract(1, 10, &[(CH, ContractState::Hibernated)]));

        // Then both area states are kept
        assert!(changed);
        let stored = store.get(&key(1)).unwrap();
        assert_eq!(stored.areas[&FR].state, ContractState::Active);
        assert_eq!(stored.areas[&CH].state, ContractState::Hibernated);
    }

    #[test]
    fn older_frame_adds_unseen_area_without_touching_newer_state() {
        // Given a contract whose FR state was updated at t=20
        let mut store = ContractStore::new();
        store.upsert(contract(1, 20, &[(FR, ContractState::Hibernated)]));

        // When an older frame from a lagging subscription carries FR and an unseen CH
        let changed = store.upsert(contract(
            1,
            10,
            &[(FR, ContractState::Active), (CH, ContractState::Active)],
        ));

        // Then the unseen area is added but FR keeps its newer state
        assert!(changed);
        let stored = store.get(&key(1)).unwrap();
        assert_eq!(stored.areas[&FR].state, ContractState::Hibernated);
        assert_eq!(stored.areas[&CH].state, ContractState::Active);
        assert_eq!(stored.updated_at_ms, 20);
    }

    #[test]
    fn contracts_with_the_same_id_in_different_markets_are_distinct() {
        // Given a Local contract
        let mut store = ContractStore::new();
        store.upsert(contract(1, 10, &[(FR, ContractState::Active)]));

        // When an Xbid contract with the same id arrives
        let mut xbid = contract(1, 10, &[(FR, ContractState::Active)]);
        xbid.key.market_type = MarketType::Xbid;
        store.upsert(xbid);

        // Then both are stored
        assert_eq!(store.len(), 2);
    }
}
