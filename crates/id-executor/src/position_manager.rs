//! The position manager — the async owner of the gap map (`docs/refactor.md`): it runs the
//! per-portfolio Position Service subscriptions (via `position_feed`, which hides the
//! reconnect/backoff mechanics), folds every event into a [`PositionStore`], tracks each
//! portfolio's feed liveness, and exposes exactly two things:
//!
//! - **the sync read** — [`PositionHandle`], the [`PositionSource`] the eval loop prepares
//!   its state from: just the pair's gap, or [`PositionView::Unavailable`] when there is no
//!   live baseline. No revision, no attribution — those never leave the async side.
//! - **the fold stream** — every fill-attributed fold, streamed change and snapshot
//!   history entry alike, is forwarded as a [`PositionFold`] to the FillMatcher, which
//!   pairs it with the gateway fill of the same `venue_trade_id`. Transfer folds are not
//!   forwarded (a transfer is not ours to match). Forwarding the seed's replay is the
//!   subscription-gap recovery: a fill that folded while the position stream was down is
//!   waiting in the matcher, and the reconnect's replayed fold completes it — no point
//!   query, no watermark bookkeeping.
//!
//! **Liveness and the clear-on-disconnect discipline.** A session drop clears the
//! portfolio's store slice and flips it to [`FeedState::NoBaseline`], so a stale gap can
//! never be read: the withhold is driven by the liveness flag, and the clear makes the
//! stale read structurally impossible on top of it. This makes "not seeded yet" and
//! "session dropped" the same condition — one `NoBaseline` state, no frozen-gap variant —
//! and the reconnect's fresh snapshot simply re-seeds the (empty) slice and flips back to
//! `Live`.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use anyhow::anyhow;
use tokio::sync::mpsc;
use tracing::info;

use crate::config::FeedConfig;
use crate::executor::{Cause, ContractKey, PortfolioId, Position, PositionSource, PositionView};
use crate::fill_matcher::PositionFold;
use crate::position_feed::{self, PositionEvent};

/// Capacity of the internal feed-event channel; backpressure, not sizing.
const POSITION_EVENT_CAPACITY: usize = 256;

/// The position store: each subscribed portfolio's net position per contract — the gap map,
/// since `G[p,c] = P[p,c]`, the standing instructions this executor works. Folds keep the
/// highest revision, which absorbs the snapshot/stream overlap of a (re)subscribe and any
/// reordering: a re-seed can only refresh, never regress, and an already-seen change is a
/// no-op. Owned by the manager; the eval loop reads it only through [`PositionSource`].
#[derive(Debug, Default)]
pub struct PositionStore {
    /// Positions per portfolio, per contract.
    positions: HashMap<PortfolioId, HashMap<ContractKey, Position>>,
}

impl PositionStore {
    pub fn new() -> Self {
        Self::default()
    }

    /// Fold one position (a snapshot entry or a streamed change), keeping the highest
    /// revision. Returns whether the map changed — `false` means the fold was a stale or
    /// duplicate value (the snapshot/stream overlap, a re-seed of known state) and callers
    /// need not re-evaluate anything.
    pub fn fold(&mut self, position: Position) -> bool {
        let by_contract = self
            .positions
            .entry(position.portfolio.clone())
            .or_default();
        match by_contract.get(&position.contract) {
            Some(existing) if existing.revision >= position.revision => false,
            _ => {
                by_contract.insert(position.contract, position);
                true
            }
        }
    }

    /// The gap to flatten for one `(portfolio, contract)`: the net position, `0` when never
    /// seen (a position the service has not streamed is flat — but note only a seeded
    /// portfolio makes that reading valid; without a live baseline the caller reads
    /// [`PositionView::Unavailable`] instead, so the gap is never taken off a stale slice).
    pub fn gap(&self, portfolio: &str, contract: &ContractKey) -> i64 {
        self.get(portfolio, contract)
            .map_or(0, |position| position.net_quantity)
    }

    /// Drop one portfolio's entire slice — the manager's clear-on-disconnect: a dropped
    /// session's baseline is stale, so it is discarded rather than kept and guarded. The
    /// withhold interlock is driven by the manager's liveness flag; clearing makes a stale-gap
    /// read structurally impossible on top of it, and the fresh snapshot re-seeds the slice on
    /// reconnect.
    pub fn clear_portfolio(&mut self, portfolio: &str) {
        self.positions.remove(portfolio);
    }

    /// The stored position for one `(portfolio, contract)`, if any fold has carried it.
    pub fn get(&self, portfolio: &str, contract: &ContractKey) -> Option<&Position> {
        self.positions.get(portfolio)?.get(contract)
    }
}

#[derive(Debug)]
struct Shared {
    positions: PositionStore,
    /// Per-portfolio liveness: `true` once the baseline is online, `false` when offline
    /// (never connected, or dropped and cleared). Offline reads [`PositionView::Unavailable`]
    /// and withholds; the store slice is only ever read while online, so it is never stale.
    online: HashMap<PortfolioId, bool>,
}

/// The sync world's read handle — cheap to clone, lock-per-read. Implements
/// [`PositionSource`] for the eval loop.
#[derive(Clone)]
pub struct PositionHandle {
    shared: Arc<Mutex<Shared>>,
}

impl PositionSource for PositionHandle {
    fn position(&self, portfolio: &str, contract: &ContractKey) -> PositionView {
        let shared = self.shared.lock().expect("position state poisoned");
        match shared.online.get(portfolio) {
            Some(true) => PositionView::Live(shared.positions.gap(portfolio, contract)),
            // Offline, or an unconfigured portfolio that can never go live.
            _ => PositionView::Unavailable,
        }
    }
}

/// Build the handle the eval loop reads and [`run`] maintains — every configured portfolio
/// starts offline.
pub fn handle(portfolios: &[PortfolioId]) -> PositionHandle {
    PositionHandle {
        shared: Arc::new(Mutex::new(Shared {
            positions: PositionStore::new(),
            online: portfolios
                .iter()
                .map(|portfolio| (portfolio.clone(), false))
                .collect(),
        })),
    }
}

/// Run the manager: one `position_feed` subscription per portfolio folding into `handle`'s
/// shared state, fill-attributed folds forwarded to the FillMatcher. Returns only on a
/// fatal condition — the feed task dying or a channel closing — which the caller treats as
/// a boot failure.
pub async fn run(
    endpoint: String,
    portfolios: Vec<PortfolioId>,
    tuning: FeedConfig,
    handle: PositionHandle,
    folds_tx: mpsc::Sender<PositionFold>,
) -> anyhow::Error {
    let (position_events_tx, mut position_events_rx) = mpsc::channel(POSITION_EVENT_CAPACITY);
    let mut feed_task = tokio::spawn(position_feed::run_subscriptions(
        endpoint,
        portfolios,
        tuning,
        position_events_tx,
    ));
    loop {
        tokio::select! {
            feed_error = &mut feed_task => {
                return match feed_error {
                    Ok(error) => error.context("position feed stopped"),
                    Err(join_error) => anyhow!("position feed task exited: {join_error}"),
                };
            }
            position_event = position_events_rx.recv() => {
                let Some(position_event) = position_event else {
                    // The sender lives in the feed task; the branch above reports why.
                    continue;
                };
                // Fold under the lock, then forward the attributed folds without it (an
                // awaited send must never hold the sync world's read lock).
                let folds = apply(&handle, position_event);
                for fold in folds {
                    if folds_tx.send(fold).await.is_err() {
                        return anyhow!("position-fold channel closed; the fill matcher is gone");
                    }
                }
            }
        }
    }
}

/// Apply one feed event to the shared state; returns the fill-attributed folds to forward.
fn apply(handle: &PositionHandle, position_event: PositionEvent) -> Vec<PositionFold> {
    let mut shared = handle.shared.lock().expect("position state poisoned");
    match position_event {
        // The baseline batch was folded (it arrived just before this marker); flip live.
        PositionEvent::Online { portfolio } => {
            info!(portfolio, "position baseline online");
            shared.online.insert(portfolio, true);
            Vec::new()
        }
        PositionEvent::Positions(positions) => positions
            .into_iter()
            .filter_map(|position| {
                let fold = match &position.cause {
                    Some(Cause::Fill { venue_trade_id, .. }) => Some(PositionFold {
                        venue_trade_id: venue_trade_id.clone(),
                        revision: position.revision,
                    }),
                    // A transfer leg (the crosser's) or a tolerated unattributed entry:
                    // fold the position, forward nothing — no fill to match.
                    _ => None,
                };
                shared.positions.fold(position);
                fold
            })
            .collect(),
        PositionEvent::Offline { portfolio } => {
            // Clear the slice: a dropped session's baseline is stale, so it is discarded
            // rather than kept and guarded — a stale gap becomes structurally unreadable,
            // and the reconnect's snapshot re-seeds. Offline withholds until then.
            shared.positions.clear_portfolio(&portfolio);
            shared.online.insert(portfolio, false);
            Vec::new()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::executor::{MarketType, Position};

    fn contract() -> ContractKey {
        ContractKey {
            market_type: MarketType::Local,
            contract_id: 7,
        }
    }

    fn position(net_quantity: i64, revision: u64, cause: Option<Cause>) -> Position {
        Position {
            portfolio: "execution-a".to_owned(),
            contract: contract(),
            net_quantity,
            revision,
            cause,
        }
    }

    /// A snapshot: the batch is folded, then `Online` flips the portfolio live — the feed's
    /// ordering (batch before its `Online` marker).
    fn seed(handle: &PositionHandle, portfolio: &str, positions: Vec<Position>) {
        apply(handle, PositionEvent::Positions(positions));
        apply(
            handle,
            PositionEvent::Online {
                portfolio: portfolio.to_owned(),
            },
        );
    }

    #[test]
    fn the_view_tracks_liveness_and_clears_on_offline() {
        let handle = handle(&["execution-a".to_owned()]);

        // Offline before connect — for stored and unknown pairs alike.
        assert_eq!(
            handle.position("execution-a", &contract()),
            PositionView::Unavailable
        );
        // An unconfigured portfolio reads Unavailable forever — it can never go live.
        assert_eq!(
            handle.position("execution-x", &contract()),
            PositionView::Unavailable
        );

        // Online: the gap reads live, and a contract the baseline never mentioned is flat.
        seed(&handle, "execution-a", vec![position(10_000, 1, None)]);
        assert_eq!(
            handle.position("execution-a", &contract()),
            PositionView::Live(10_000)
        );
        let other = ContractKey {
            market_type: MarketType::Local,
            contract_id: 8,
        };
        assert_eq!(
            handle.position("execution-a", &other),
            PositionView::Live(0)
        );

        // Offline: no baseline is readable, and the stale slice is cleared.
        apply(
            &handle,
            PositionEvent::Offline {
                portfolio: "execution-a".to_owned(),
            },
        );
        assert_eq!(
            handle.position("execution-a", &contract()),
            PositionView::Unavailable
        );
        // Reconnect with an empty snapshot: the pre-drop +10 was cleared, so it reads flat —
        // a stale gap can never survive a disconnect.
        seed(&handle, "execution-a", vec![]);
        assert_eq!(
            handle.position("execution-a", &contract()),
            PositionView::Live(0),
            "clear-on-offline: the dropped session's baseline does not survive reconnect"
        );
    }

    #[test]
    fn only_fill_attributed_folds_are_forwarded() {
        let handle = handle(&["execution-a".to_owned()]);
        let folds = apply(
            &handle,
            PositionEvent::Positions(vec![
                position(
                    4_000,
                    2,
                    Some(Cause::Fill {
                        venue_trade_id: "T-1".to_owned(),
                        cl_order_id: "CL-1".to_owned(),
                    }),
                ),
                position(
                    5_000,
                    3,
                    Some(Cause::Transfer {
                        transfer_id: "c3a1".to_owned(),
                    }),
                ),
                position(6_000, 4, None),
            ]),
        );
        // The fill fold reaches the matcher; the transfer and the bare fold do not —
        // but all three folded into the store (highest revision wins).
        assert_eq!(
            folds,
            vec![PositionFold {
                venue_trade_id: "T-1".to_owned(),
                revision: 2,
            }]
        );
        apply(
            &handle,
            PositionEvent::Online {
                portfolio: "execution-a".to_owned(),
            },
        );
        assert_eq!(
            handle.position("execution-a", &contract()),
            PositionView::Live(6_000)
        );
    }

    // -----------------------------------------------------------------------------------
    // PositionStore — the gap map the manager folds into
    // -----------------------------------------------------------------------------------

    fn stored(portfolio: &str, net_quantity: i64, revision: u64) -> Position {
        Position {
            portfolio: portfolio.to_owned(),
            contract: contract(),
            net_quantity,
            revision,
            cause: None,
        }
    }

    #[test]
    fn folds_keep_the_highest_revision() {
        // Given a folded position at revision 2
        let mut gaps = PositionStore::new();
        assert!(gaps.fold(stored("execution-a", 10_000, 2)));

        // When an older and an equal revision fold (the snapshot/stream overlap)
        let stale = gaps.fold(stored("execution-a", 99, 1));
        let duplicate = gaps.fold(stored("execution-a", 99, 2));

        // Then neither applies — the stored value is untouched
        assert!(!stale);
        assert!(!duplicate);
        assert_eq!(gaps.gap("execution-a", &contract()), 10_000);

        // And a higher revision replaces it
        assert!(gaps.fold(stored("execution-a", 4_000, 3)));
        assert_eq!(gaps.gap("execution-a", &contract()), 4_000);
    }

    #[test]
    fn a_never_seen_pair_reads_as_flat() {
        let gaps = PositionStore::new();
        assert_eq!(gaps.gap("execution-a", &contract()), 0);
        assert!(gaps.get("execution-a", &contract()).is_none());
    }

    #[test]
    fn portfolios_fold_independently() {
        // Given the same contract held by two portfolios
        let mut gaps = PositionStore::new();
        gaps.fold(stored("execution-a", 10_000, 5));
        gaps.fold(stored("execution-b", -2_000, 1));

        // When one updates
        gaps.fold(stored("execution-b", 0, 2));

        // Then the other's slice is untouched, and revisions are per-pair (b's revision 2
        // did not have to beat a's 5)
        assert_eq!(gaps.gap("execution-a", &contract()), 10_000);
        assert_eq!(gaps.gap("execution-b", &contract()), 0);
    }
}
