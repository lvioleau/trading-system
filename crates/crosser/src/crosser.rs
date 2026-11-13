//! Pure domain for the crossing loop — the analogue of the services' `ledger.rs`/`store.rs`: no
//! IO, no protobuf. The feeds translate their wire types into these at the boundary, and
//! [`CrossingState`] is the heart of the component: latest target + actual position per
//! `(portfolio, contract)`, the gap-to-target computation, and the transfer construction.
//!
//! The one property everything hangs off (see `docs/crosser-implementation.md`): the crosser
//! **reconciles positions, not trades**. [`CrossingState::decide`] sizes each cross as
//! `gap = target − actual`, where the target is the strategy's latest *absolute*
//! `target_quantity` and the actual is the Position Service's own view of the strategy
//! portfolio — never the strategy message's `current`/`delta`. A dropped or reordered target
//! record therefore costs nothing: the next record for the contract drives the actual position
//! to the newest target regardless of history.

use std::collections::{HashMap, HashSet};
use std::fmt;

/// A portfolio identifier — a plain configured string, exactly as the Position Service ledgers
/// it (see the position-service deviations): strategy portfolios are what targets arrive keyed
/// by and what the crosser reconciles; execution portfolios are where the crossed gap lands.
pub type PortfolioId = String;

/// The Nord Pool market segment — half a contract's identity. Mirrored here (as the
/// position-service ledger and the mock strategy's driver do) so the domain stays free of
/// protobuf; the feeds map their wire enums onto it.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum MarketType {
    Local,
    Xbid,
}

impl fmt::Display for MarketType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            MarketType::Local => f.write_str("local"),
            MarketType::Xbid => f.write_str("xbid"),
        }
    }
}

/// A contract's natural key across the stack: the market segment plus the venue's contract id
/// (stable per venue, not assumed unique across segments).
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ContractKey {
    pub market_type: MarketType,
    pub contract_id: i64,
}

impl fmt::Display for ContractKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}:{}", self.market_type, self.contract_id)
    }
}

/// One contract's top of book — best bid and best ask (level 1), each `None` when that side has
/// no resting orders — maintained by the depth feed and read by the crossing loop at cross time.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct TopOfBook {
    /// Best bid price (minor currency units per MWh; negative is legitimate).
    pub bid: Option<i64>,
    /// Best ask price (same encoding).
    pub ask: Option<i64>,
}

impl TopOfBook {
    /// The live mid every cross is priced at: `(best_bid + best_ask) / 2` rounded to the integer
    /// minor unit — and only from a *clean* book, two-sided and uncrossed (`best_bid <
    /// best_ask`). A crossed or one-sided book yields `None`: the crossing loop withholds the
    /// cross and the next heartbeat re-attempts, rather than pricing off a broken book.
    ///
    /// The half-unit case rounds **half-to-even** (3.5 → 4, 4.5 → 4, symmetric for negative
    /// mids): unlike half-up or floor, it carries no systematic half-unit bias across many
    /// crosses — the rounding convention deliberately settled in
    /// `docs/crosser-implementation.md` open question 3.
    pub fn mid(&self) -> Option<i64> {
        match (self.bid, self.ask) {
            (Some(bid), Some(ask)) if bid < ask => Some(midpoint_half_to_even(bid, ask)),
            _ => None,
        }
    }
}

/// `(bid + ask) / 2` rounded half-to-even. Exact halves only ever arise from an odd sum; the
/// euclidean quotient (floor) is then either the even neighbour (kept) or the odd one (bumped).
fn midpoint_half_to_even(bid: i64, ask: i64) -> i64 {
    let sum = bid + ask;
    let half = sum.div_euclid(2);
    if sum.rem_euclid(2) == 0 || half % 2 == 0 {
        half
    } else {
        half + 1
    }
}

/// One target-position record as received from the strategy, translated at the feed boundary.
/// `target_quantity` (absolute kW, signed) is the only field the crossing loop acts on — it
/// crosses whatever reaches it from the *actual* Position Service position. The
/// `current`/`delta`/`mid` triple is the strategy's own booking, carried for the input audit log
/// and the drift metric, never for sizing or pricing.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TargetRecord {
    /// The strategy portfolio this target is for; the crossing loop maps it to its configured
    /// execution portfolio. Always one of the configured portfolios — the subscription is scoped.
    pub portfolio: PortfolioId,
    pub contract: ContractKey,
    /// Absolute target net quantity in kW, signed (long > 0, short < 0).
    pub target_quantity: i64,
    /// The strategy's view of the position before this trade (advisory).
    pub current_quantity: i64,
    /// The change the strategy intends, `target - current`; 0 on an unchanged heartbeat
    /// (advisory — the crosser computes its own gap against the actual position).
    pub delta_quantity: i64,
    /// The mid the strategy valued the trade at (minor currency units per MWh; negative is
    /// legitimate). The backtest's pricing baseline — production prices at the live depth mid.
    pub mid_price: i64,
}

/// The actual current value of one `(portfolio, contract)` position as the Position Service
/// serves it — the truth targets are reconciled against. Carried by both the `Subscribe` stream
/// and the `Transfer` ack (which returns the two resulting positions), translated at the client
/// boundary.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Position {
    pub portfolio: PortfolioId,
    pub contract: ContractKey,
    /// Net quantity in kW, signed (long > 0, short < 0). Zero still exists — flat is a value.
    pub net_quantity: i64,
    /// Monotonic per `(portfolio, contract)`; 0 means the position does not exist yet. The
    /// state keeps the highest seen, which dedupes the `Subscribe` echo against the ack.
    pub revision: u64,
}

/// One cross, fully sized and priced — everything a `Transfer` needs except its idempotency id
/// and timestamp, which are the IO layer's to mint (the id once per computed gap, reused across
/// resends of that same gap).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TransferIntent {
    /// The strategy portfolio — the *from* side, because the quantity was derived from reading
    /// its position, so it carries the optimistic-concurrency guard.
    pub from: PortfolioId,
    /// Its configured execution portfolio — the *to* side, where the gap lands for the
    /// executor to flatten.
    pub to: PortfolioId,
    pub contract: ContractKey,
    /// `−gap = actual − target`, signed kW: `to` receives it, `from` receives its negation —
    /// so the strategy portfolio lands exactly at the target.
    pub quantity: i64,
    /// The live top-of-book mid at cross time (minor currency units per MWh).
    pub price: i64,
    /// The actual position's revision the gap was computed from — the OCC guard.
    pub expected_from_revision: u64,
}

/// What [`CrossingState::on_target`] did with a received record.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TargetOutcome {
    /// Stored as the contract's latest target — worth a [`decide`](CrossingState::decide) now.
    Stored,
    /// Dropped — the portfolio's baseline snapshot has not folded yet, so there is no actual
    /// position to size a gap against. Deliberate, not queued: targets are absolute and the
    /// strategy re-emits every contract's target within a heartbeat, so the drop costs at most
    /// one interval at boot and buys the invariant that a stored target implies a seeded
    /// portfolio.
    NotSeeded,
    /// Dropped — not a configured portfolio. The scoped subscription makes this unreachable;
    /// the caller logs it loudly.
    Unmapped,
}

/// What [`CrossingState::decide`] concluded for one `(portfolio, contract)`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CrossDecision {
    /// The actual position differs from the target and the book is clean: emit this transfer.
    Cross(TransferIntent),
    /// The actual position already equals the target — nothing to do. A re-sent or unchanged
    /// heartbeat lands here, which is what makes reconciliation idempotent by construction.
    AtTarget,
    /// The book is crossed, one-sided, or absent — no live mid, so the cross is withheld (the
    /// standing gap is carried for the tripwire log) and the next heartbeat re-attempts. Never
    /// price from the strategy's `mid_price`.
    NoMid { gap: i64 },
    /// No target is stored for this `(portfolio, contract)` (or the portfolio is not
    /// configured) — nothing to reconcile toward.
    NoTarget,
}

/// The crossing loop's entire in-memory state — a projection of the three upstreams, rebuilt on
/// restart: the latest absolute target per `(portfolio, contract)` (strategy feed), the actual
/// position and its revision (Position Service `Subscribe` snapshots/echoes and `Transfer`
/// acks), and the configured strategy → execution portfolio map. The live mid deliberately does
/// *not* live here — the depth feed owns it, and [`decide`](Self::decide) takes the top of book
/// read at cross time.
#[derive(Debug)]
pub struct CrossingState {
    /// Strategy portfolio → its configured execution portfolio.
    executions: HashMap<PortfolioId, PortfolioId>,
    /// Portfolios whose `Subscribe` baseline snapshot has folded; crossing is withheld until
    /// then (an absent actual position only means 0 *after* the seed).
    seeded: HashSet<PortfolioId>,
    /// The latest target record per `(portfolio, contract)` — absolute, so newest simply wins;
    /// only ever for seeded portfolios ([`on_target`](Self::on_target) drops earlier arrivals).
    /// The whole record is kept: `target_quantity` is what is acted on; the strategy's
    /// `current`/`delta`/`mid` ride along for the decision log and the drift metric.
    targets: HashMap<(PortfolioId, ContractKey), TargetRecord>,
    /// The actual position and revision per `(portfolio, contract)`, highest revision kept.
    actuals: HashMap<(PortfolioId, ContractKey), (i64, u64)>,
}

impl CrossingState {
    /// One `(strategy portfolio, execution portfolio)` pair per configured portfolio.
    pub fn new(portfolios: impl IntoIterator<Item = (PortfolioId, PortfolioId)>) -> Self {
        Self {
            executions: portfolios.into_iter().collect(),
            seeded: HashSet::new(),
            targets: HashMap::new(),
            actuals: HashMap::new(),
        }
    }

    /// Store a target record — the newest absolute target simply replaces the previous one.
    /// A record arriving before its portfolio's baseline seed is dropped, not queued (see
    /// [`TargetOutcome::NotSeeded`]); one for an unmapped portfolio is refused.
    pub fn on_target(&mut self, record: TargetRecord) -> TargetOutcome {
        if !self.executions.contains_key(&record.portfolio) {
            return TargetOutcome::Unmapped;
        }
        if !self.seeded.contains(&record.portfolio) {
            return TargetOutcome::NotSeeded;
        }
        self.targets
            .insert((record.portfolio.clone(), record.contract), record);
        TargetOutcome::Stored
    }

    /// Fold one portfolio's `Subscribe` snapshot as its baseline and mark it seeded — from here
    /// on its targets are accepted, and an absent actual position means flat (0 at revision 0).
    /// A re-seed after a dropped subscription folds the same way: positions apply keep-highest,
    /// and positions never vanish (flat still exists), so nothing needs clearing.
    pub fn seed(&mut self, portfolio: &str, positions: Vec<Position>) {
        for position in positions {
            self.apply_position(&position);
        }
        self.seeded.insert(portfolio.to_owned());
    }

    /// Fold one position — a `Subscribe` echo or a `Transfer` ack leg — keeping the highest
    /// revision (the two overlap by design; equal or lower is a duplicate, not a regression).
    /// Positions for unmapped portfolios (e.g. the ack's execution-side leg) are ignored: only
    /// the strategy portfolios are reconciled.
    pub fn apply_position(&mut self, position: &Position) {
        if !self.executions.contains_key(&position.portfolio) {
            return;
        }
        let key = (position.portfolio.clone(), position.contract);
        let entry = self.actuals.entry(key).or_insert((0, 0));
        if position.revision > entry.1 {
            *entry = (position.net_quantity, position.revision);
        }
    }

    /// The crossing algorithm's core (`docs/crosser-implementation.md`, *The crossing
    /// algorithm*): `gap = target − actual` against the *actual* position, priced at the live
    /// top-of-book mid or withheld, oriented with the strategy portfolio as `from` carrying the
    /// OCC guard at the revision the gap was computed from.
    pub fn decide(&self, portfolio: &str, contract: ContractKey, book: TopOfBook) -> CrossDecision {
        let key = (portfolio.to_owned(), contract);
        let Some(execution) = self.executions.get(portfolio) else {
            return CrossDecision::NoTarget;
        };
        let Some(target) = self.targets.get(&key) else {
            return CrossDecision::NoTarget;
        };
        // A stored target implies a seeded portfolio (`on_target` drops earlier arrivals), so
        // an absent actual position here provably means flat: the baseline was complete and
        // carried nothing for this contract — revision 0, "does not exist yet", guardable.
        let (actual, revision) = self.actuals.get(&key).copied().unwrap_or((0, 0));
        let gap = target.target_quantity - actual;
        if gap == 0 {
            return CrossDecision::AtTarget;
        }
        let Some(mid) = book.mid() else {
            return CrossDecision::NoMid { gap };
        };
        CrossDecision::Cross(TransferIntent {
            from: portfolio.to_owned(),
            to: execution.clone(),
            contract,
            quantity: -gap,
            price: mid,
            expected_from_revision: revision,
        })
    }

    /// The stored latest target for `(portfolio, contract)` — the decision log reads the
    /// strategy's own booking (`current`/`delta`/`mid`) from here.
    pub fn target(&self, portfolio: &str, contract: ContractKey) -> Option<&TargetRecord> {
        self.targets.get(&(portfolio.to_owned(), contract))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn book(bid: Option<i64>, ask: Option<i64>) -> TopOfBook {
        TopOfBook { bid, ask }
    }

    #[test]
    fn mid_is_the_midpoint_of_a_clean_book() {
        assert_eq!(book(Some(4_200), Some(4_300)).mid(), Some(4_250));
        // Negative mids are legitimate (negative prices happen).
        assert_eq!(book(Some(-4_300), Some(-4_200)).mid(), Some(-4_250));
        // A mid spanning zero.
        assert_eq!(book(Some(-100), Some(100)).mid(), Some(0));
    }

    #[test]
    fn mid_rounds_the_half_unit_to_even() {
        // 3.5 → 4 and 4.5 → 4: no systematic half-unit bias across many crosses.
        assert_eq!(book(Some(3), Some(4)).mid(), Some(4));
        assert_eq!(book(Some(4), Some(5)).mid(), Some(4));
        // Symmetric for negative mids: -3.5 → -4, -4.5 → -4.
        assert_eq!(book(Some(-4), Some(-3)).mid(), Some(-4));
        assert_eq!(book(Some(-5), Some(-4)).mid(), Some(-4));
    }

    #[test]
    fn a_crossed_book_yields_no_mid() {
        // best_bid >= best_ask is a broken book — withhold rather than price it.
        assert_eq!(book(Some(4_300), Some(4_200)).mid(), None);
        assert_eq!(book(Some(4_200), Some(4_200)).mid(), None);
    }

    #[test]
    fn a_one_sided_or_empty_book_yields_no_mid() {
        assert_eq!(book(Some(4_200), None).mid(), None);
        assert_eq!(book(None, Some(4_300)).mid(), None);
        assert_eq!(book(None, None).mid(), None);
        assert_eq!(TopOfBook::default().mid(), None);
    }

    const CONTRACT: ContractKey = ContractKey {
        market_type: MarketType::Local,
        contract_id: 7,
    };

    /// A state mapping `p → e`, seeded flat (empty snapshot) unless a baseline is given.
    fn state(baseline: Option<(i64, u64)>) -> CrossingState {
        let mut state = CrossingState::new([("p".to_owned(), "e".to_owned())]);
        let positions = baseline
            .map(|(net_quantity, revision)| {
                vec![Position {
                    portfolio: "p".to_owned(),
                    contract: CONTRACT,
                    net_quantity,
                    revision,
                }]
            })
            .unwrap_or_default();
        state.seed("p", positions);
        state
    }

    fn target(quantity: i64) -> TargetRecord {
        TargetRecord {
            portfolio: "p".to_owned(),
            contract: CONTRACT,
            target_quantity: quantity,
            // The strategy's own booking is deliberately nonsense: sizing must never read it.
            current_quantity: 999_999,
            delta_quantity: -999_999,
            mid_price: 1,
        }
    }

    const CLEAN: TopOfBook = TopOfBook {
        bid: Some(4_200),
        ask: Some(4_300),
    };

    #[test]
    fn the_worked_example_sizes_both_crosses_against_the_actual_position() {
        // The architecture proposal's sequence: target −10 from flat, then revised to −9.
        let mut state = state(None);
        assert_eq!(state.on_target(target(-10_000)), TargetOutcome::Stored);

        // gap = −10 − 0 = −10 ⇒ quantity +10, guard at revision 0 (no position yet).
        let CrossDecision::Cross(intent) = state.decide("p", CONTRACT, CLEAN) else {
            panic!("expected a cross from flat");
        };
        assert_eq!(
            intent,
            TransferIntent {
                from: "p".to_owned(),
                to: "e".to_owned(),
                contract: CONTRACT,
                quantity: 10_000,
                price: 4_250,
                expected_from_revision: 0,
            }
        );

        // The ack folds back: p landed at the target.
        state.apply_position(&Position {
            portfolio: "p".to_owned(),
            contract: CONTRACT,
            net_quantity: -10_000,
            revision: 1,
        });
        assert_eq!(state.decide("p", CONTRACT, CLEAN), CrossDecision::AtTarget);

        // Revised to −9: gap = −9 − (−10) = +1 ⇒ quantity −1, guard at the new revision.
        assert_eq!(state.on_target(target(-9_000)), TargetOutcome::Stored);
        let CrossDecision::Cross(intent) = state.decide("p", CONTRACT, CLEAN) else {
            panic!("expected the reconcile leg");
        };
        assert_eq!(intent.quantity, -1_000);
        assert_eq!(intent.expected_from_revision, 1);
    }

    #[test]
    fn a_pre_existing_baseline_is_reconciled_against() {
        // Restart safety: the boot fold found p already short 10 at revision 3.
        let mut state = state(Some((-10_000, 3)));
        state.on_target(target(-9_000));
        let CrossDecision::Cross(intent) = state.decide("p", CONTRACT, CLEAN) else {
            panic!("expected a cross against the folded baseline");
        };
        assert_eq!(intent.quantity, -1_000);
        assert_eq!(intent.expected_from_revision, 3);
    }

    #[test]
    fn a_target_before_the_baseline_seed_is_dropped() {
        // A target before the snapshot must not be sized against a presumed-flat position — it
        // is dropped outright (the heartbeat re-delivers), never queued.
        let mut state = CrossingState::new([("p".to_owned(), "e".to_owned())]);
        assert_eq!(state.on_target(target(-10_000)), TargetOutcome::NotSeeded);
        assert_eq!(state.decide("p", CONTRACT, CLEAN), CrossDecision::NoTarget);

        // Once seeded, the re-delivered target is accepted and reconciles against the baseline.
        state.seed(
            "p",
            vec![Position {
                portfolio: "p".to_owned(),
                contract: CONTRACT,
                net_quantity: -4_000,
                revision: 2,
            }],
        );
        assert_eq!(state.on_target(target(-10_000)), TargetOutcome::Stored);
        let CrossDecision::Cross(intent) = state.decide("p", CONTRACT, CLEAN) else {
            panic!("expected a cross once seeded");
        };
        assert_eq!(intent.quantity, 6_000);
        assert_eq!(intent.expected_from_revision, 2);
    }

    #[test]
    fn a_broken_book_withholds_and_carries_the_standing_gap() {
        let mut state = state(None);
        state.on_target(target(-10_000));
        assert_eq!(
            state.decide("p", CONTRACT, book(None, Some(4_300))),
            CrossDecision::NoMid { gap: -10_000 }
        );
        // At target, a broken book is irrelevant — there is nothing to price.
        state.apply_position(&Position {
            portfolio: "p".to_owned(),
            contract: CONTRACT,
            net_quantity: -10_000,
            revision: 1,
        });
        assert_eq!(
            state.decide("p", CONTRACT, book(None, None)),
            CrossDecision::AtTarget
        );
    }

    #[test]
    fn positions_fold_keep_highest_revision() {
        // The Transfer ack applies first; the Subscribe echo of the same mutation (equal
        // revision) and any stale replay (lower) are duplicates, never regressions.
        let mut state = state(None);
        let ack = Position {
            portfolio: "p".to_owned(),
            contract: CONTRACT,
            net_quantity: -10_000,
            revision: 2,
        };
        state.apply_position(&ack);
        state.apply_position(&Position {
            net_quantity: -4_000,
            revision: 1,
            ..ack.clone()
        });
        state.apply_position(&Position {
            net_quantity: 0,
            revision: 2,
            ..ack.clone()
        });
        state.on_target(target(-10_000));
        assert_eq!(state.decide("p", CONTRACT, CLEAN), CrossDecision::AtTarget);
        // The execution-side ack leg is not tracked: only strategy portfolios reconcile.
        state.apply_position(&Position {
            portfolio: "e".to_owned(),
            ..ack
        });
        assert_eq!(state.decide("e", CONTRACT, CLEAN), CrossDecision::NoTarget);
    }

    #[test]
    fn unmapped_and_untargeted_contracts_decide_to_nothing() {
        let mut state = state(None);
        assert_eq!(
            state.decide("p", CONTRACT, CLEAN),
            CrossDecision::NoTarget,
            "no target received yet"
        );
        // A record for an unmapped portfolio is refused (the scoped subscription means this
        // never happens; the loop logs it loudly).
        assert_eq!(
            state.on_target(TargetRecord {
                portfolio: "stray".to_owned(),
                ..target(-10_000)
            }),
            TargetOutcome::Unmapped
        );
        assert_eq!(
            state.decide("stray", CONTRACT, CLEAN),
            CrossDecision::NoTarget
        );
    }
}
