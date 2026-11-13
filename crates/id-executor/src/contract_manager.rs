//! The contract manager — the async owner of tradeability's feed half
//! (`docs/refactor.md`): it runs the area's Contract Service subscription (via
//! `contract_feed`, which hides the reconnect/backoff mechanics), folds every event into a
//! [`ContractStore`], and exposes the sync read the eval loop prepares from —
//! [`ContractHandle`], the [`ContractSource`]: present ∧ `ACTIVE` plus the delivery start
//! the cutoff is checked against. `None` means the live set has never carried the
//! contract — not tradeable, safe by absence, which also covers the not-yet-seeded boot
//! window without any extra state (nothing is cleared on a session end either: a lifecycle
//! state does not go stale the way a book does — see `contract_feed`).

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use anyhow::anyhow;
use tokio::sync::mpsc;

use eic::EicCode;

use crate::config::FeedConfig;
use crate::contract_feed::{self, ContractEvent};
use crate::executor::{Contract, ContractKey, ContractSource, ContractState, Tradeability};

/// Capacity of the internal feed-event channel; backpressure, not sizing.
const CONTRACT_EVENT_CAPACITY: usize = 256;

/// The live contract set, area-shared: one lookup serves every portfolio's gap on a contract.
/// Upserts by contract identity (contracts are never removed, they change state), guarded by
/// the venue's `updated_at_ms`: a replay or the re-seed of a reconnect can only refresh, never
/// regress. Owned by the manager; the eval loop reads it only through [`ContractSource`].
///
/// This is tradeability's *feed half* — present ∧ `ACTIVE` in the area ([`Self::active`]). The
/// time half (delivery start more than the trading cutoff away) is evaluated in `evaluate`
/// against its per-cycle `now`, since it changes without any feed data changing.
#[derive(Debug, Default)]
pub struct ContractStore {
    contracts: HashMap<ContractKey, Contract>,
}

impl ContractStore {
    pub fn new() -> Self {
        Self::default()
    }

    /// Fold one contract upsert (a seed entry or a streamed change), keeping the newest venue
    /// `updated_at_ms`. Returns whether the store changed — `false` means a stale or replayed
    /// record (the re-seed overlap of a reconnect) and callers need not re-evaluate anything.
    pub fn fold(&mut self, contract: Contract) -> bool {
        match self.contracts.get(&contract.key) {
            Some(existing) if existing.updated_at_ms >= contract.updated_at_ms => false,
            _ => {
                self.contracts.insert(contract.key, contract);
                true
            }
        }
    }

    /// The stored contract, if the live set has carried it.
    pub fn get(&self, key: &ContractKey) -> Option<&Contract> {
        self.contracts.get(key)
    }

    /// The feed's tradeability contribution: the contract is in the live set and `ACTIVE` in
    /// the executor's area. Not the whole gate — invariant 2 also requires delivery start more
    /// than the trading cutoff away, which `evaluate` checks against `now`.
    pub fn active(&self, key: &ContractKey) -> bool {
        self.get(key)
            .is_some_and(|contract| contract.state == ContractState::Active)
    }
}

/// The sync world's read handle — cheap to clone, lock-per-read. Implements
/// [`ContractSource`] for the eval loop.
#[derive(Clone, Default)]
pub struct ContractHandle {
    contracts: Arc<Mutex<ContractStore>>,
}

impl ContractSource for ContractHandle {
    fn tradeability(&self, contract: &ContractKey) -> Option<Tradeability> {
        self.contracts
            .lock()
            .expect("contract state poisoned")
            .get(contract)
            .map(|record| Tradeability {
                active: record.state == ContractState::Active,
                delivery_start_ms: record.delivery_start_ms,
            })
    }
}

/// Build the handle the eval loop reads and [`run`] maintains.
pub fn handle() -> ContractHandle {
    ContractHandle::default()
}

/// Run the manager: the area's contract subscription folding into `handle`'s shared
/// store. Returns only on a fatal condition — the feed task dying — which the caller
/// treats as a boot failure.
pub async fn run(
    endpoint: String,
    delivery_area_eic: EicCode,
    tuning: FeedConfig,
    handle: ContractHandle,
) -> anyhow::Error {
    let (contract_events_tx, mut contract_events_rx) = mpsc::channel(CONTRACT_EVENT_CAPACITY);
    let mut feed_task = tokio::spawn(contract_feed::run(
        endpoint,
        delivery_area_eic,
        tuning,
        contract_events_tx,
    ));
    loop {
        tokio::select! {
            feed_error = &mut feed_task => {
                return match feed_error {
                    Ok(error) => error.context("contract feed stopped"),
                    Err(join_error) => anyhow!("contract feed task exited: {join_error}"),
                };
            }
            contract_event = contract_events_rx.recv() => {
                let Some(contract_event) = contract_event else {
                    // The sender lives in the feed task; the branch above reports why.
                    continue;
                };
                let mut contracts = handle.contracts.lock().expect("contract state poisoned");
                match contract_event {
                    // Seed and delta fold identically: upsert-by-identity, guarded by the
                    // venue's updated_at_ms — a re-seed refreshes, never regresses.
                    ContractEvent::Seeded(records) | ContractEvent::Updated(records) => {
                        for record in records {
                            contracts.fold(record);
                        }
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::executor::{Contract, MarketType};

    fn contract() -> ContractKey {
        ContractKey {
            market_type: MarketType::Local,
            contract_id: 7,
        }
    }

    #[test]
    fn tradeability_reads_the_folded_state() {
        let handle = handle();
        assert_eq!(
            handle.tradeability(&contract()),
            None,
            "unknown to the live set — not tradeable, safe by absence"
        );

        handle.contracts.lock().unwrap().fold(Contract {
            key: contract(),
            state: ContractState::Active,
            delivery_start_ms: 1_760_000_000_000,
            updated_at_ms: 100,
        });
        assert_eq!(
            handle.tradeability(&contract()),
            Some(Tradeability {
                active: true,
                delivery_start_ms: 1_760_000_000_000,
            })
        );

        // The venue's state flip lands as active = false — the feed half of invariant 2.
        handle.contracts.lock().unwrap().fold(Contract {
            key: contract(),
            state: ContractState::Inactive,
            delivery_start_ms: 1_760_000_000_000,
            updated_at_ms: 101,
        });
        assert_eq!(
            handle.tradeability(&contract()),
            Some(Tradeability {
                active: false,
                delivery_start_ms: 1_760_000_000_000,
            })
        );
    }

    // -----------------------------------------------------------------------------------
    // ContractStore — the live set the manager folds into
    // -----------------------------------------------------------------------------------

    fn contract_record(state: ContractState, updated_at_ms: i64) -> Contract {
        Contract {
            key: contract(),
            state,
            delivery_start_ms: 1_760_000_000_000,
            updated_at_ms,
        }
    }

    #[test]
    fn contract_folds_never_regress_on_the_venue_update_time() {
        // Given a contract folded ACTIVE at venue time 100
        let mut contracts = ContractStore::new();
        assert!(contracts.fold(contract_record(ContractState::Active, 100)));
        assert!(contracts.active(&contract()));

        // When a stale and a replayed record fold (a reconnect's re-seed overlap)
        let stale = contracts.fold(contract_record(ContractState::Inactive, 99));
        let replay = contracts.fold(contract_record(ContractState::Inactive, 100));

        // Then neither applies — the stored state is untouched
        assert!(!stale);
        assert!(!replay);
        assert!(contracts.active(&contract()));

        // And a newer venue timestamp applies — the ACTIVE → INACTIVE flip lands
        assert!(contracts.fold(contract_record(ContractState::Inactive, 101)));
        assert!(!contracts.active(&contract()));
    }

    #[test]
    fn only_a_present_and_active_contract_is_tradeable_feed_side() {
        // Absent from the live set ⇒ not tradeable
        let empty = ContractStore::new();
        assert!(!empty.active(&contract()));

        // Present in any non-ACTIVE state ⇒ not tradeable
        for state in [
            ContractState::None,
            ContractState::Hibernated,
            ContractState::Inactive,
            ContractState::Frozen,
        ] {
            let mut contracts = ContractStore::new();
            contracts.fold(contract_record(state, 100));
            assert!(
                !contracts.active(&contract()),
                "{state} must not be tradeable"
            );
        }
    }

    #[test]
    fn contract_folds_carry_the_delivery_start_for_the_cutoff() {
        let mut contracts = ContractStore::new();
        contracts.fold(contract_record(ContractState::Active, 100));
        let stored = contracts.get(&contract()).unwrap();
        assert_eq!(stored.delivery_start_ms, 1_760_000_000_000);
    }
}
