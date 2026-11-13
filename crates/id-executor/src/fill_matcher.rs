//! The FillMatcher — the trade↔position join (`docs/refactor.md`): it consumes the
//! gateway's fill stream (the fast path) and the Position Service's attributed folds (the
//! authoritative path), pairs them by `venue_trade_id`, and emits a **matched fill** —
//! a fill *proven folded into the position*, carrying the fold's revision.
//!
//! This one abstraction is what lets the order manager wait correctly: it accumulates
//! matched fills only, so when an order's executed quantity is fully matched, the position
//! *already reflects every fill* — the next gap read is settled with respect to our own
//! trades, and no separate echo-wait bookkeeping exists anywhere else.
//!
//! The two inputs race freely (the fold can outrun the fill and vice versa): whichever
//! side arrives first is buffered, and the other side's arrival completes the match. A
//! transfer fold never reaches this module (the position manager forwards fill-attributed
//! folds only), so nothing here can mis-attribute a crosser transfer.
//!
//! The fold input includes the seed: the position snapshot replays each pair's attributed
//! history on every (re)subscribe, so most folds arriving here are historical — their
//! fills matched long ago or predate this process entirely. That is deliberate (a fill
//! waiting out a position-stream gap is completed by the reconnect's replay — the
//! subscription-gap recovery), and it is why the fold buffer is **capped, oldest evicted
//! first**: a live fold-before-fill race resolves within moments, so only the newest
//! unmatched folds can still complete a match, while replayed history would otherwise
//! accrete forever. Matching is exact by id, so nothing evicted or buffered can ever pair
//! with the wrong fill.
//!
//! [`FillMatcher`] is a pure state machine; [`run`] is the thin task that wires it between
//! its channels.

use std::collections::{BTreeMap, HashMap};

use anyhow::anyhow;
use tokio::sync::mpsc;
use tracing::debug;

use crate::executor::Fill;

/// A position-stream fold attributed to a market fill: the `venue_trade_id` it carried and
/// the position revision it produced. The position manager forwards exactly these — never
/// transfers — from streamed changes and the snapshot's history replay alike.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PositionFold {
    pub venue_trade_id: String,
    pub revision: u64,
}

/// A fill observed folded into the position: the gateway fill plus the revision its fold
/// produced. The order manager's only fill input.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MatchedFill {
    pub fill: Fill,
    pub revision: u64,
}

/// Cap on the unmatched-fold buffer. The gateway-fill side is bounded by in-flight order
/// volume (every fill's fold eventually arrives and removes it), but the fold side is
/// not: the seed replays whole histories, so unmatched folds must be shed. A live
/// fold-before-fill race resolves within moments — an entry old enough to be evicted
/// under this cap was history, not a race. Sized far above any plausible number of
/// genuinely in-flight folds.
const UNMATCHED_FOLD_CAP: usize = 1024;

/// The pure join state: whichever side of a `venue_trade_id` arrived first, awaiting the
/// other. A matched id leaves both sides. The fill side is naturally bounded by in-flight
/// volume; the fold side is capped ([`UNMATCHED_FOLD_CAP`]), evicting oldest-first.
#[derive(Debug, Default)]
pub struct FillMatcher {
    unmatched_fills: HashMap<String, Fill>,
    /// `venue_trade_id` → (fold revision, insertion generation). `fold_order` mirrors the
    /// generations for oldest-first eviction; the two are kept in exact lockstep.
    unmatched_folds: HashMap<String, (u64, u64)>,
    fold_order: BTreeMap<u64, String>,
    next_fold_generation: u64,
}

impl FillMatcher {
    pub fn new() -> Self {
        Self::default()
    }

    /// A gateway fill arrived: matched if its fold was already seen, else buffered.
    pub fn on_fill(&mut self, fill: Fill) -> Option<MatchedFill> {
        match self.unmatched_folds.remove(&fill.venue_trade_id) {
            Some((revision, generation)) => {
                self.fold_order.remove(&generation);
                Some(MatchedFill { fill, revision })
            }
            None => {
                self.unmatched_fills
                    .insert(fill.venue_trade_id.clone(), fill);
                None
            }
        }
    }

    /// An attributed position fold arrived: matched if its fill was already seen, else
    /// buffered (the authoritative path outran the fast one — no special case), shedding
    /// the oldest buffered fold beyond the cap.
    pub fn on_fold(&mut self, fold: PositionFold) -> Option<MatchedFill> {
        match self.unmatched_fills.remove(&fold.venue_trade_id) {
            Some(fill) => Some(MatchedFill {
                fill,
                revision: fold.revision,
            }),
            None => {
                let generation = self.next_fold_generation;
                self.next_fold_generation += 1;
                if let Some((_, replaced)) = self
                    .unmatched_folds
                    .insert(fold.venue_trade_id.clone(), (fold.revision, generation))
                {
                    // A replayed id refreshes in place — its old generation slot goes.
                    self.fold_order.remove(&replaced);
                }
                self.fold_order.insert(generation, fold.venue_trade_id);
                while self.unmatched_folds.len() > UNMATCHED_FOLD_CAP {
                    let Some((_, evicted)) = self.fold_order.pop_first() else {
                        break;
                    };
                    self.unmatched_folds.remove(&evicted);
                }
                None
            }
        }
    }
}

/// Run the matcher between its channels: fills in, folds in, matched fills out. Returns
/// only when a channel closes — its producer (or the order manager) is gone, which the
/// caller treats as fatal.
pub async fn run(
    mut fills_rx: mpsc::Receiver<Fill>,
    mut folds_rx: mpsc::Receiver<PositionFold>,
    matched_fills_tx: mpsc::Sender<MatchedFill>,
) -> anyhow::Error {
    let mut matcher = FillMatcher::new();
    loop {
        let matched = tokio::select! {
            fill = fills_rx.recv() => {
                let Some(fill) = fill else {
                    return anyhow!("fill channel closed; the gateway feed is gone");
                };
                debug!(
                    venue_trade_id = fill.venue_trade_id,
                    cl_order_id = fill.cl_order_id,
                    "fill awaiting its position fold"
                );
                matcher.on_fill(fill)
            }
            fold = folds_rx.recv() => {
                let Some(fold) = fold else {
                    return anyhow!("position-fold channel closed; the position manager is gone");
                };
                matcher.on_fold(fold)
            }
        };
        if let Some(matched) = matched
            && matched_fills_tx.send(matched).await.is_err()
        {
            return anyhow!("matched-fill channel closed; the order manager is gone");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::executor::{ContractKey, MarketType};

    fn fill(venue_trade_id: &str, quantity: i64) -> Fill {
        Fill {
            portfolio: "execution-a".to_owned(),
            contract: ContractKey {
                market_type: MarketType::Local,
                contract_id: 7,
            },
            order_id: "O-1".to_owned(),
            cl_order_id: "CL-1".to_owned(),
            venue_trade_id: venue_trade_id.to_owned(),
            exchange: "nordpool".to_owned(),
            quantity,
            price: 1_990,
            executed_at_nanos: 1_000,
        }
    }

    fn fold(venue_trade_id: &str, revision: u64) -> PositionFold {
        PositionFold {
            venue_trade_id: venue_trade_id.to_owned(),
            revision,
        }
    }

    #[test]
    fn matches_whichever_side_arrives_first() {
        let mut matcher = FillMatcher::new();

        // Fast path first: the fill buffers, the fold completes it.
        assert_eq!(matcher.on_fill(fill("T-1", -10_000)), None);
        let matched = matcher.on_fold(fold("T-1", 4)).unwrap();
        assert_eq!(matched.fill.venue_trade_id, "T-1");
        assert_eq!(matched.revision, 4);

        // Authoritative path first: the fold buffers, the fill completes it.
        assert_eq!(matcher.on_fold(fold("T-2", 5)), None);
        let matched = matcher.on_fill(fill("T-2", 3_000)).unwrap();
        assert_eq!(matched.fill.quantity, 3_000);
        assert_eq!(matched.revision, 5);
    }

    #[test]
    fn a_matched_id_leaves_no_residue() {
        // Ids match exactly once: after the pair completes, both sides are forgotten, so
        // nothing can double-match or accrete.
        let mut matcher = FillMatcher::new();
        matcher.on_fill(fill("T-1", -10_000));
        assert!(matcher.on_fold(fold("T-1", 4)).is_some());
        assert_eq!(
            matcher.on_fold(fold("T-1", 4)),
            None,
            "replayed fold buffers anew"
        );
        assert!(matcher.unmatched_fills.is_empty());

        // And a match via the fold-first path clears the eviction order too — the
        // mirrored maps stay in lockstep.
        assert!(matcher.on_fill(fill("T-1", -10_000)).is_some());
        assert!(matcher.unmatched_folds.is_empty());
        assert!(matcher.fold_order.is_empty());
    }

    #[test]
    fn the_fold_buffer_evicts_oldest_first_at_the_cap() {
        // Given a full fold buffer plus one — a seed replaying more history than the cap
        let mut matcher = FillMatcher::new();
        for i in 0..=UNMATCHED_FOLD_CAP {
            assert_eq!(matcher.on_fold(fold(&format!("T-{i}"), i as u64 + 1)), None);
        }
        assert_eq!(matcher.unmatched_folds.len(), UNMATCHED_FOLD_CAP);

        // Then the oldest fold was shed — its late fill buffers instead of matching —
        // while the newest still matches: eviction keeps the racing tail, not the history
        assert_eq!(matcher.on_fill(fill("T-0", 1_000)), None);
        let newest = format!("T-{UNMATCHED_FOLD_CAP}");
        let matched = matcher.on_fill(fill(&newest, 2_000)).unwrap();
        assert_eq!(matched.revision, UNMATCHED_FOLD_CAP as u64 + 1);
    }

    #[test]
    fn a_replayed_fold_refreshes_in_place_without_growing_the_buffer() {
        // Given the same id folded twice (a re-seed replaying a still-unmatched fold),
        // the second carrying a newer revision
        let mut matcher = FillMatcher::new();
        assert_eq!(matcher.on_fold(fold("T-1", 4)), None);
        assert_eq!(matcher.on_fold(fold("T-1", 9)), None);

        // Then one entry remains, at the refreshed revision, order map in lockstep
        assert_eq!(matcher.unmatched_folds.len(), 1);
        assert_eq!(matcher.fold_order.len(), 1);
        assert_eq!(matcher.on_fill(fill("T-1", 500)).unwrap().revision, 9);
    }

    #[test]
    fn distinct_ids_never_cross_match() {
        let mut matcher = FillMatcher::new();
        assert_eq!(matcher.on_fill(fill("T-1", -10_000)), None);
        assert_eq!(matcher.on_fold(fold("T-2", 9)), None);
        // Each waits for its own counterpart.
        assert!(matcher.on_fold(fold("T-1", 10)).is_some());
        assert!(matcher.on_fill(fill("T-2", 500)).is_some());
    }
}
