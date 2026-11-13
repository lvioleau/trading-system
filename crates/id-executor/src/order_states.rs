//! The eval loop's [`OrderTracker`]: the per-`(portfolio, contract)` [`OrderState`] map,
//! advanced by the order manager's [`OrderResolved`] resolutions over a channel. Loop-owned
//! and synchronous — `InFlight` is set on a successful submit (the double-send guard: the loop
//! reads its own `InFlight` before the manager has processed the request), and a pair returns
//! to `Idle` when the manager reports its order finished. The resolutions channel is drained
//! once per cycle, never blocked on (the depth feed is the loop's only wake). Absent ⇒ `Idle`
//! (no order ever sent).

use std::collections::HashMap;

use tokio::sync::mpsc;

use crate::executor::{OrderState, OrderTracker, PairKey};
use crate::order_manager::OrderResolved;

/// The concrete [`OrderTracker`] the loop runs on (see the module doc).
pub struct OrderStates {
    states: HashMap<PairKey, OrderState>,
    resolutions_rx: mpsc::Receiver<OrderResolved>,
}

impl OrderStates {
    pub fn new(resolutions_rx: mpsc::Receiver<OrderResolved>) -> Self {
        Self {
            states: HashMap::new(),
            resolutions_rx,
        }
    }
}

impl OrderTracker for OrderStates {
    /// Apply every pending resolution — each frees its pair back to `Idle`. Does not
    /// re-trigger anything; a freed pair is re-evaluated on the next book tick on its
    /// contract, like any other change.
    fn refresh(&mut self) {
        while let Ok(resolved) = self.resolutions_rx.try_recv() {
            self.states.insert(
                resolved.key,
                OrderState::Idle {
                    last_send_rejected: resolved.last_send_rejected,
                },
            );
        }
    }

    fn get(&self, key: &PairKey) -> OrderState {
        self.states.get(key).copied().unwrap_or(OrderState::Idle {
            last_send_rejected: false,
        })
    }

    fn mark_in_flight(&mut self, key: PairKey) {
        self.states.insert(key, OrderState::InFlight);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::executor::{ContractKey, MarketType};

    fn key(contract_id: i64) -> PairKey {
        (
            "execution-a".to_owned(),
            ContractKey {
                market_type: MarketType::Local,
                contract_id,
            },
        )
    }

    #[test]
    fn an_untracked_pair_is_idle_and_never_rejected() {
        let (_tx, rx) = mpsc::channel(4);
        let states = OrderStates::new(rx);
        assert_eq!(
            states.get(&key(1)),
            OrderState::Idle {
                last_send_rejected: false,
            }
        );
    }

    #[test]
    fn mark_in_flight_is_the_double_send_guard() {
        let (_tx, rx) = mpsc::channel(4);
        let mut states = OrderStates::new(rx);
        // Set on a successful submit, before the manager has processed the request — the loop
        // reads its own InFlight and will not send again.
        states.mark_in_flight(key(1));
        assert_eq!(states.get(&key(1)), OrderState::InFlight);
    }

    #[test]
    fn refresh_drains_resolutions_freeing_the_pair_and_carrying_the_reject_flag() {
        let (tx, rx) = mpsc::channel(4);
        let mut states = OrderStates::new(rx);
        states.mark_in_flight(key(1));
        // The order manager resolves the order, flagging that it ended in a rejection.
        tx.try_send(OrderResolved {
            key: key(1),
            last_send_rejected: true,
        })
        .unwrap();
        states.refresh();
        assert_eq!(
            states.get(&key(1)),
            OrderState::Idle {
                last_send_rejected: true,
            }
        );
    }

    #[test]
    fn refresh_applies_every_queued_resolution_last_one_winning_per_pair() {
        let (tx, rx) = mpsc::channel(8);
        let mut states = OrderStates::new(rx);
        // Two pairs resolved, plus a later resolution for pair 1 that supersedes the first.
        for resolved in [
            OrderResolved {
                key: key(1),
                last_send_rejected: true,
            },
            OrderResolved {
                key: key(2),
                last_send_rejected: false,
            },
            OrderResolved {
                key: key(1),
                last_send_rejected: false,
            },
        ] {
            tx.try_send(resolved).unwrap();
        }
        states.refresh();
        assert_eq!(
            states.get(&key(1)),
            OrderState::Idle {
                last_send_rejected: false,
            },
            "the last resolution for a pair wins"
        );
        assert_eq!(
            states.get(&key(2)),
            OrderState::Idle {
                last_send_rejected: false,
            }
        );
    }

    #[test]
    fn refresh_with_no_resolutions_leaves_state_untouched() {
        let (_tx, rx) = mpsc::channel(4);
        let mut states = OrderStates::new(rx);
        states.mark_in_flight(key(1));
        states.refresh(); // channel empty — a no-op
        assert_eq!(states.get(&key(1)), OrderState::InFlight);
    }
}
