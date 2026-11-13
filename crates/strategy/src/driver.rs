//! The strategy **cradle**: the harness around the strategy proper. Every tick it gathers the
//! state — every live contract, the portfolio's actual positions, every live book — into one
//! [`StrategyInput`], hands it to the injected [`Strategy`] (in production the Python module
//! behind [`py_strategy`](crate::py_strategy)), and broadcasts each returned target as a
//! [`TargetRecord`] the server fans out to its subscribers, booking the live top-of-book mid.
//! **What to trade is the strategy's decision** — the cradle knows nothing of the strategy's
//! logic, imposes no tradable set, and withholds only what it cannot do safely (a targeted
//! contract with no live mid, and the whole tick without a live position baseline). Pure
//! domain: no protobuf, no gRPC.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use tokio::sync::broadcast;
use tokio::time::{self, MissedTickBehavior};
use tracing::{debug, info};

/// The Nord Pool market segment — half a contract's identity. Mirrored here (like the
/// position-service ledger mirrors it) so the driver stays free of protobuf; the server maps it
/// to the wire enum at the boundary.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum MarketType {
    Local,
    Xbid,
}

/// The strategy's booking of one trade — what the crosser subscribes to. `delta = target -
/// current`, where `current` is the previously emitted target for the contract.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TargetRecord {
    pub portfolio: String,
    pub market_type: MarketType,
    pub contract_id: i64,
    pub target_quantity: i64,
    pub current_quantity: i64,
    pub delta_quantity: i64,
    pub mid_price: i64,
}

/// Broadcast sender the driver publishes to and each subscriber reads from (filtered to its
/// requested portfolios by the server).
pub type RecordSender = broadcast::Sender<TargetRecord>;

/// A contract's identity: the market segment and its venue id.
pub type ContractId = (MarketType, i64);

/// What the cradle knows about a live contract: when its delivery starts and whether it is
/// currently active for trading in the subscribed area — the [`StrategyInput`]'s reference view
/// of each contract.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ContractView {
    pub delivery_start_ms: i64,
    pub active: bool,
}

/// The cradle's read of the live contract set: each tick, every contract it has heard of — the
/// [`StrategyInput`]'s `contracts`, from which the strategy chooses what to trade. Injecting it
/// as a trait keeps the cradle testable against a scripted set and lets a simulation swap the
/// live subscription for replayed reference data behind the same seam.
pub trait ContractSource {
    /// Every live contract, keyed by identity. Empty when nothing has been heard yet — which
    /// leaves the strategy with nothing to target.
    fn contracts(&self) -> HashMap<ContractId, ContractView>;
}

/// The live contract set, maintained by the [`contract_feed`](crate::contract_feed) and read by
/// the cradle at tick time — the live [`ContractSource`]. A plain mutex — every access is a
/// brief, non-async read or merge.
pub type SharedContracts = Arc<Mutex<HashMap<ContractId, ContractView>>>;

impl ContractSource for SharedContracts {
    fn contracts(&self) -> HashMap<ContractId, ContractView> {
        self.lock().expect("contract view poisoned").clone()
    }
}

/// One resting price level of a book — venue-native integers verbatim (minor currency units per
/// MWh; kW).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Level {
    pub price: i64,
    pub quantity: i64,
}

/// A contract's live book, as the cradle sees it at tick time: all top-N levels, best-first per
/// side — the strategy input's market view. Mirrored from the executor's domain (like
/// [`MarketType`]) so the cradle stays free of wire types; the depth feed translates the
/// connector's view to this at the boundary.
#[derive(Clone, Debug, PartialEq, Eq, Default)]
pub struct Book {
    pub bids: Vec<Level>,
    pub asks: Vec<Level>,
}

impl Book {
    /// The best resting bid — levels arrive best-first; `None` when no bids rest.
    pub fn best_bid(&self) -> Option<&Level> {
        self.bids.first()
    }

    /// The best resting ask — `None` when no asks rest.
    pub fn best_ask(&self) -> Option<&Level> {
        self.asks.first()
    }

    /// The mid every emitted target books: `(best_bid + best_ask) / 2` rounded to the integer
    /// minor unit — and only from a *clean* book, two-sided and uncrossed (`best_bid <
    /// best_ask`). A crossed or one-sided book yields `None`: the cradle withholds the
    /// contract's emission rather than booking a mid off a broken book.
    ///
    /// The half-unit case rounds **half-to-even** (3.5 → 4, 4.5 → 4, symmetric for negative
    /// mids) — the same convention the crosser prices crosses with, so the booked and executed
    /// mids never differ by rounding alone.
    pub fn mid(&self) -> Option<i64> {
        match (self.best_bid(), self.best_ask()) {
            (Some(bid), Some(ask)) if bid.price < ask.price => {
                Some(midpoint_half_to_even(bid.price, ask.price))
            }
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

/// The cradle's read of the live depth: each tick, every live book — the [`StrategyInput`]'s
/// market view, and where the booked mids come from. Injecting it as a trait keeps the cradle
/// testable against a scripted book and lets a simulation swap the live subscription for a
/// replayed one (e.g. a parquet reader) without touching the cradle.
pub trait DepthSource {
    /// Every live book, keyed by contract. Empty when the feed is down — which withholds every
    /// emission (no book, no mid).
    fn books(&self) -> HashMap<ContractId, Book>;
}

/// The live books per contract, maintained by the [`depth_feed`](crate::depth_feed) and read by
/// the cradle at tick time — the live [`DepthSource`]. A plain mutex — every access is a brief,
/// non-async read or overwrite. An absent key means "no live book" (never seen, or the feed is
/// down).
pub type SharedBooks = Arc<Mutex<HashMap<ContractId, Book>>>;

impl DepthSource for SharedBooks {
    fn books(&self) -> HashMap<ContractId, Book> {
        self.lock().expect("depth map poisoned").clone()
    }
}

/// A contract's actual net position as the Position Service streams it: the current value and
/// its ledger revision (the fold guard — a snapshot/stream overlap keeps the highest).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ActualPosition {
    /// Net position, signed kW (buy > 0).
    pub net_quantity: i64,
    /// The position's ledger revision — monotonic per (portfolio, contract).
    pub revision: u64,
}

/// The cradle's read of the portfolio's actual positions: the strategy must see where the
/// portfolio really is before it targets where it should be. Injecting it as a trait keeps the
/// cradle testable against a scripted baseline and lets a simulation replay recorded positions
/// behind the same seam.
pub trait PositionSource {
    /// The portfolio's live net position (kW) per contract, or `None` when no live baseline
    /// exists (not seeded yet, or the session dropped) — which withholds the whole tick: the
    /// strategy is never run without knowing the actual positions. A seeded absence is flat:
    /// once `Some`, a missing contract provably holds 0.
    fn positions(&self) -> Option<HashMap<ContractId, i64>>;
}

/// The portfolio's live positions, maintained by the [`position_feed`](crate::position_feed) and
/// read by the cradle at tick time — the live [`PositionSource`]. `None` until the session's
/// snapshot seeds it, and again while the feed is down (no live baseline). A plain mutex — every
/// access is a brief, non-async read or merge.
pub type SharedPositions = Arc<Mutex<Option<HashMap<ContractId, ActualPosition>>>>;

impl PositionSource for SharedPositions {
    fn positions(&self) -> Option<HashMap<ContractId, i64>> {
        self.lock().expect("position map poisoned").as_ref().map(
            |positions: &HashMap<ContractId, ActualPosition>| {
                positions
                    .iter()
                    .map(|(id, position)| (*id, position.net_quantity))
                    .collect()
            },
        )
    }
}

/// Everything one strategy evaluation reads, assembled by the cradle each tick — the clean seam
/// between "gather the state" and "decide". The decide half must need nothing beyond this
/// struct: it is the surface the strategy proper (maintained in its own repo) is handed, so
/// anything it wants has to arrive here first.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StrategyInput {
    /// Wall-clock unix ms at assembly.
    pub now_ms: i64,
    /// Every live contract's reference view ([`ContractSource::contracts`]) — delivery start
    /// and active state. The strategy chooses what to trade from this; the cradle pre-selects
    /// nothing.
    pub contracts: HashMap<ContractId, ContractView>,
    /// The portfolio's actual net position (signed kW) per contract — the seeded baseline, so a
    /// missing contract provably holds 0. The cradle never assembles an input without one.
    pub positions: HashMap<ContractId, i64>,
    /// Every live book, full depth ([`DepthSource::books`]).
    pub books: HashMap<ContractId, Book>,
}

/// The strategy proper: one evaluation, [`StrategyInput`] → absolute target positions (signed
/// kW) for whatever contracts it decides to trade — which contracts, how many, and when to stop
/// are all its decisions, internal to it. The cradle owns everything around it — gathering the
/// input, booking mids, withholding, the wire fan-out — so an implementation is a pure decision
/// function plus whatever state it carries between ticks. In production that is the Python
/// module behind [`PyStrategy`](crate::py_strategy::PyStrategy).
pub trait Strategy {
    /// Targets for this tick. The cradle emits every target it can price (a live mid),
    /// withholding the rest; omitting a contract simply emits nothing for it this tick.
    fn targets(&mut self, input: &StrategyInput) -> Vec<(ContractId, i64)>;
}
/// The cradle's three upstream views — what [`run_feed`] gathers into every tick's
/// [`StrategyInput`] — each behind its trait: live feeds in production, replayed data in a
/// simulation.
pub struct Sources<C: ContractSource, D: DepthSource, P: PositionSource> {
    /// The live contract set ([`ContractSource`]).
    pub contracts: C,
    /// Every live book ([`DepthSource`]).
    pub depth: D,
    /// The portfolio's actual-position baseline ([`PositionSource`]).
    pub positions: P,
}

/// Run the cradle forever: every `tick`, assemble the [`StrategyInput`] (the full contract
/// view, positions, books), run the `strategy` — one call, all contracts; it trades what it
/// decides to trade — and emit each returned target, booking the live mid. Two withhold gates,
/// safe by absence:
///
/// - **No live position baseline → the whole tick is withheld.** The strategy is never run
///   blind; the input's `positions` is always a seeded baseline.
/// - **No live mid for a targeted contract → that target is withheld.** Nothing books a
///   fabricated mid; `last` stays untouched so the next emission's delta spans the gap. (The
///   crosser prices from its own depth subscription of the same connector, so a withheld tick
///   loses nothing it could have crossed.)
///
/// A contract the strategy stops targeting is simply no longer emitted (its position settles at
/// delivery — no flatten). Returns only if the process is shutting down.
pub async fn run_feed(
    portfolio: String,
    mut strategy: impl Strategy,
    sources: Sources<impl ContractSource, impl DepthSource, impl PositionSource>,
    tick: Duration,
    sender: RecordSender,
) {
    // Last emitted target per contract, so each record carries the delta from the prior one.
    let mut last: HashMap<ContractId, i64> = HashMap::new();

    let mut ticker = time::interval(tick);
    ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
    loop {
        ticker.tick().await;
        let now_ms = now_ms();

        // Gate one: no live baseline, no evaluation — the strategy never runs blind.
        let Some(actuals) = sources.positions.positions() else {
            debug!("no live position baseline; tick withheld");
            continue;
        };
        let input = StrategyInput {
            now_ms,
            contracts: sources.contracts.contracts(),
            positions: actuals,
            books: sources.depth.books(),
        };
        let targets = strategy.targets(&input);

        // Forget contracts the strategy no longer targets — its set is its own, and a contract
        // it has dropped simply stops being emitted. (A target withheld below for lack of a mid
        // is still *targeted*, so its `last` survives the gap.)
        let targeted: HashSet<ContractId> = targets.iter().map(|(id, _)| *id).collect();
        last.retain(|id, _| targeted.contains(id));

        for (id, target) in targets {
            // Gate two: the record books the live mid; no live two-sided book means nothing to
            // book, so the contract is withheld this tick.
            let Some(mid) = input.books.get(&id).and_then(Book::mid) else {
                debug!(?id, "no live mid; target withheld this tick");
                continue;
            };
            let current = last.insert(id, target).unwrap_or(0);
            emit(&sender, &portfolio, id, target, current, mid);
        }
    }
}

/// Broadcast one target for the cradle's portfolio.
fn emit(
    sender: &RecordSender,
    portfolio: &str,
    id: ContractId,
    target: i64,
    current: i64,
    mid: i64,
) {
    let (market_type, contract_id) = id;
    info!(
        portfolio,
        ?market_type,
        contract_id,
        target_quantity = target,
        current_quantity = current,
        delta_quantity = target - current,
        mid_price = mid,
        "target published"
    );
    let _ = sender.send(TargetRecord {
        portfolio: portfolio.to_owned(),
        market_type,
        contract_id,
        target_quantity: target,
        current_quantity: current,
        delta_quantity: target - current,
        mid_price: mid,
    });
}

/// Wall-clock unix milliseconds — the tradable-set expiry cutoff is against real time.
fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn view(entries: &[(ContractId, i64, bool)]) -> HashMap<ContractId, ContractView> {
        entries
            .iter()
            .map(|(id, delivery_start_ms, active)| {
                (
                    *id,
                    ContractView {
                        delivery_start_ms: *delivery_start_ms,
                        active: *active,
                    },
                )
            })
            .collect()
    }

    /// A book with one level per given side price.
    fn book(bid: Option<i64>, ask: Option<i64>) -> Book {
        let level = |price| Level {
            price,
            quantity: 5_000,
        };
        Book {
            bids: bid.map(level).into_iter().collect(),
            asks: ask.map(level).into_iter().collect(),
        }
    }

    #[test]
    fn mid_is_the_midpoint_of_a_clean_book() {
        assert_eq!(book(Some(4_100), Some(4_300)).mid(), Some(4_200));
        // The half-unit case rounds half-to-even: (3+4)/2 → 4, (4+5)/2 → 4.
        assert_eq!(book(Some(3), Some(4)).mid(), Some(4));
        assert_eq!(book(Some(4), Some(5)).mid(), Some(4));
        assert_eq!(book(Some(-5), Some(-4)).mid(), Some(-4)); // symmetric for negative mids
        // One-sided or crossed books yield no mid — the cradle withholds.
        assert_eq!(book(Some(4_100), None).mid(), None);
        assert_eq!(book(None, Some(4_300)).mid(), None);
        assert_eq!(book(Some(4_300), Some(4_100)).mid(), None);
        assert_eq!(book(Some(4_200), Some(4_200)).mid(), None); // locked book
    }

    #[tokio::test]
    async fn run_feed_gates_on_the_baseline_and_books_the_live_mid() {
        let priced = (MarketType::Xbid, 100);
        let unpriced = (MarketType::Xbid, 101);
        // Both eligible far into the future; only `priced` has a live book.
        let far_out = i64::MAX / 2;
        let contracts: SharedContracts = Arc::new(Mutex::new(view(&[
            (priced, far_out, true),
            (unpriced, far_out, true),
        ])));
        let books: SharedBooks = Arc::new(Mutex::new(HashMap::from([(
            priced,
            book(Some(4_100), Some(4_300)),
        )])));
        let positions: SharedPositions = Arc::new(Mutex::new(None));
        let (sender, mut records) = broadcast::channel(64);
        tokio::spawn(run_feed(
            "strat".to_owned(),
            TargetEverything,
            Sources {
                contracts,
                depth: books.clone(),
                positions: positions.clone(),
            },
            Duration::from_millis(5),
            sender,
        ));

        // Gate one: no position baseline — the whole tick is withheld, live mid or not.
        assert!(
            time::timeout(Duration::from_millis(100), records.recv())
                .await
                .is_err(),
            "emitted a record without a position baseline"
        );

        // Seed the baseline (an empty snapshot: provably flat). Gate two: only the priced
        // contract emits, booking the live mid; the bookless one is withheld.
        *positions.lock().unwrap() = Some(HashMap::new());
        let record = time::timeout(Duration::from_secs(5), records.recv())
            .await
            .expect("no record after the baseline seeded")
            .unwrap();
        assert_eq!(record.portfolio, "strat");
        assert_eq!(record.contract_id, priced.1);
        assert_eq!(record.mid_price, 4_200);

        // Give the withheld contract a book: its records start flowing at the new mid.
        books
            .lock()
            .unwrap()
            .insert(unpriced, book(Some(10), Some(20)));
        let record = time::timeout(Duration::from_secs(5), async {
            loop {
                let record = records.recv().await.unwrap();
                if record.contract_id == unpriced.1 {
                    return record;
                }
                assert_eq!(record.mid_price, 4_200); // the priced contract keeps its mid
            }
        })
        .await
        .expect("the newly priced contract never emitted");
        assert_eq!(record.mid_price, 15);
    }

    /// The target every test strategy here emits, kW — an arbitrary constant.
    const TEST_TARGET_KW: i64 = 500;

    /// The simplest decision: target every contract in the input. What the strategy targets is
    /// its own business — the cradle tests only exercise the gates around it.
    struct TargetEverything;

    impl Strategy for TargetEverything {
        fn targets(&mut self, input: &StrategyInput) -> Vec<(ContractId, i64)> {
            input
                .contracts
                .keys()
                .map(|id| (*id, TEST_TARGET_KW))
                .collect()
        }
    }

    /// [`TargetEverything`] that also records every input it is handed, to assert what the
    /// cradle gathered.
    struct Recorder {
        inputs: Arc<Mutex<Vec<StrategyInput>>>,
    }

    impl Strategy for Recorder {
        fn targets(&mut self, input: &StrategyInput) -> Vec<(ContractId, i64)> {
            self.inputs.lock().unwrap().push(input.clone());
            TargetEverything.targets(input)
        }
    }

    #[tokio::test]
    async fn the_strategy_input_carries_the_gathered_state() {
        let id = (MarketType::Xbid, 100);
        let far_out = i64::MAX / 2;
        let contracts: SharedContracts = Arc::new(Mutex::new(view(&[(id, far_out, true)])));
        let books: SharedBooks = Arc::new(Mutex::new(HashMap::from([(
            id,
            book(Some(4_100), Some(4_300)),
        )])));
        let positions: SharedPositions = Arc::new(Mutex::new(Some(HashMap::from([(
            id,
            ActualPosition {
                net_quantity: -9_000,
                revision: 4,
            },
        )]))));
        let inputs = Arc::new(Mutex::new(Vec::new()));
        let (sender, mut records) = broadcast::channel(64);
        tokio::spawn(run_feed(
            "strat".to_owned(),
            Recorder {
                inputs: inputs.clone(),
            },
            Sources {
                contracts,
                depth: books,
                positions,
            },
            Duration::from_millis(5),
            sender,
        ));

        // The recorder's target reaches the wire...
        let record = time::timeout(Duration::from_secs(5), records.recv())
            .await
            .expect("no record emitted")
            .unwrap();
        assert_eq!(record.target_quantity, TEST_TARGET_KW);
        assert_eq!(record.mid_price, 4_200);

        // ...and the input it decided from carried all three gathered views.
        let input = inputs
            .lock()
            .unwrap()
            .first()
            .cloned()
            .expect("no input recorded");
        assert!(input.now_ms > 0);
        assert_eq!(
            input.contracts[&id],
            ContractView {
                delivery_start_ms: far_out,
                active: true
            }
        );
        assert_eq!(input.positions[&id], -9_000);
        assert_eq!(input.books[&id].mid(), Some(4_200));
    }
}
