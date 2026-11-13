//! Pure domain for the ID Executor — no IO, no protobuf, no channels (the analogue of the
//! crosser's `crosser.rs` and the services' `ledger.rs`/`store.rs`).
//!
//! Two layers live here. The domain value types the feeds translate onto and the managers
//! fold into their own stores: the [`Position`] with its write attribution ([`Cause`]), the
//! [`Contract`] tradeability view, the [`Book`], and the order-path types the gateway module
//! produces — [`Side`], [`Fill`], [`OrderRevision`]. (The stores themselves live with the
//! managers that own them — `PositionStore` in `position_manager`, `ContractStore` in
//! `contract_manager`; this file holds no state.) **The gap is the position — nothing else**
//! (invariant 1 of `docs/id-executor-implementation.md`): each nonzero `(portfolio, contract)`
//! net quantity is a standing instruction to drive it to zero — long (`net > 0`) means sell,
//! short (`net < 0`) means buy.
//!
//! On top of those types, the evaluation: [`evaluate`] is a pure function of one prepared
//! [`EvalInput`] and one caller-stamped `now` — the order to send or exactly one
//! [`BlockReason`], deterministic under a scripted clock. The infra stays out by design
//! (`docs/refactor.md`): the async managers own the streams, the reconnects, and the order
//! lifecycle, and surface into the evaluation only through the three narrow traits here —
//! [`PositionSource`] (the gap, or why it is unavailable), [`ContractSource`]
//! (tradeability's feed half), and [`OrderBroker`] (the forwarder that submits an order).
//! The pair's [`OrderState`] (is an order in flight?) is loop-owned state, advanced by the
//! order manager's resolutions. The sync loop in `execution.rs` prepares an `EvalInput` from
//! those handles per triggered pair, calls `evaluate`, and submits what it decides. The send
//! bookkeeping the throttling gates read (cooldown and rate windows) is a decision concern, so
//! it lives on the deciding side (`throttling`), not in the order manager.

use std::fmt;
use std::time::Duration;

use crate::config::ExecutionConfig;

/// A portfolio identifier — a plain string, the repo-wide portfolio-as-string decision.
pub type PortfolioId = String;

/// One `(portfolio, contract)` — the executor's unit of state, decision, and order.
pub type PairKey = (PortfolioId, ContractKey);

/// The market segment half of a contract's identity, mirroring the Position Service's
/// domain enum (Nord Pool contract ids are stable per venue but not assumed unique across
/// the Local and Xbid segments).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
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

/// A contract's natural identity.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ContractKey {
    pub market_type: MarketType,
    pub contract_id: i64,
}

impl fmt::Display for ContractKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}:{}", self.market_type, self.contract_id)
    }
}

/// The write attribution a streamed position change carries — which trade caused it.
/// The fill-matching key: the FillMatcher pairs a gateway fill with the position fold
/// carrying the same `venue_trade_id`; a concurrent crosser transfer folds as a
/// [`Cause::Transfer`] no fill ever matches, so it is ignored — no mis-attribution.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Cause {
    /// A market fill, by its venue trade id (the own-trades match key). Also carries the
    /// order's `cl_order_id` (threaded through the Position Service from the gateway's
    /// `Trade.cl_order_id`) — the trace join key; matching stays on `venue_trade_id`.
    Fill {
        venue_trade_id: String,
        cl_order_id: String,
    },
    /// A transfer leg, by the submitter's transfer id.
    Transfer { transfer_id: String },
}

/// The full current value of one `(portfolio, contract)` position, as the Position Service
/// streams it. Every message is the full current value — there are no deltas to fold: the
/// map replaces its copy, keeping the highest revision.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Position {
    pub portfolio: PortfolioId,
    pub contract: ContractKey,
    /// Net quantity in kW, signed: long > 0, short < 0 (1 MW = 1000 kW).
    pub net_quantity: i64,
    /// Monotonic per (portfolio, contract), stable across service restarts.
    pub revision: u64,
    /// The trade that caused this change. Set on streamed changes *and* snapshot entries
    /// alike — the seed replays each pair's attributed history, one entry per fold —
    /// so `None` only covers a defensively-tolerated unattributed entry.
    pub cause: Option<Cause>,
}

/// A contract's lifecycle state in the executor's delivery area, mirroring the Contract
/// Service's wire enum. `None` covers both an unset wire state and a contract that does not
/// list the area at all — never tradeable either way, so the executor needs no distinction.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ContractState {
    None,
    Active,
    Hibernated,
    Inactive,
    Frozen,
}

impl fmt::Display for ContractState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            ContractState::None => "none",
            ContractState::Active => "active",
            ContractState::Hibernated => "hibernated",
            ContractState::Inactive => "inactive",
            ContractState::Frozen => "frozen",
        })
    }
}

/// One contract's tradeability view: the slice of the Contract Service's record the
/// executor needs — the venue's lifecycle state in the executor's one delivery area, and
/// the delivery window's start (the trading-cutoff input, invariant 2).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Contract {
    pub key: ContractKey,
    /// The venue's word on the contract in the executor's area.
    pub state: ContractState,
    /// When delivery starts (unix ms) — the executor stops working the contract
    /// `trading_cutoff` before this.
    pub delivery_start_ms: i64,
    /// Venue-side update time — the out-of-order upsert guard.
    pub updated_at_ms: i64,
}

/// One resting price level of a book side: price in minor currency units per MWh, visible
/// quantity in kW — venue-native integers, verbatim off the connector wire.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Level {
    pub price: i64,
    pub quantity: i64,
}

/// One contract's live book: the connector's full top-N view, best-first per side (bids
/// high→low, asks low→high), empty when a side has no resting orders.
///
/// The executor reads the **opposite side**, not the mid — where it diverges from the
/// crosser's use of the same feed: a sell (`G > 0`) takes [`Self::best_bid`], a buy
/// (`G < 0`) takes [`Self::best_ask`]; the best opposite price becomes the IOC limit and
/// that level's visible quantity caps the order (invariants 3–4). The mid, when one exists,
/// is logged for cost diagnostics only — the executor never needs it to trade.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Book {
    pub key: ContractKey,
    pub bids: Vec<Level>,
    pub asks: Vec<Level>,
}

impl Book {
    /// The best resting bid — what a sell takes; `None` when no bids rest (`BOOK_EMPTY`
    /// for a sell).
    pub fn best_bid(&self) -> Option<&Level> {
        self.bids.first()
    }

    /// The best resting ask — what a buy takes; `None` when no asks rest (`BOOK_EMPTY`
    /// for a buy).
    pub fn best_ask(&self) -> Option<&Level> {
        self.asks.first()
    }
}

/// Which way an order takes the market. The sign convention everywhere else is signed kW
/// (buy > 0); `Side` appears only where the wire keeps direction and magnitude apart — the
/// order entry, whose quantity is positive with the side carrying the sign.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Side {
    Buy,
    Sell,
}

impl fmt::Display for Side {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Side::Buy => f.write_str("buy"),
            Side::Sell => f.write_str("sell"),
        }
    }
}

/// A fill of one of our IOC orders, as the gateway's own-trades stream reports it — the
/// fast return path. The FillMatcher pairs it with the Position Service fold carrying the
/// same `venue_trade_id`; only the matched result reaches the order manager. The portfolio
/// is the executor's own name, routed from the stream's `portfolio_id` scope at the
/// boundary.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Fill {
    pub portfolio: PortfolioId,
    pub contract: ContractKey,
    /// Our order the fill executed against — invariant 5's order↔fill link.
    pub order_id: String,
    /// The order's `cl_order_id`, echoed by the gateway from the placing order
    /// (`Trade.cl_order_id`) — the join key that stitches this fill to its send, its
    /// own-order revisions, and the Position Service fold across the trace.
    pub cl_order_id: String,
    /// The venue's trade id — the fill↔fold match key.
    pub venue_trade_id: String,
    /// The exchange the trade executed on (PowerBot's path segment, e.g. "nordpool").
    pub exchange: String,
    /// Signed kW: buy > 0, sell < 0 — the wire side folded into the sign at the boundary.
    pub quantity: i64,
    /// Minor currency units per MWh.
    pub price: i64,
    pub executed_at_nanos: i64,
}

/// One own-order revision from the gateway's own-orders stream, reduced to what order
/// lifecycle management needs (invariant 5): which order on which pair, whether the
/// revision is terminal (IOC is always terminal fast — fully executed, killed after a
/// partial, killed unfilled, or rejected), the remaining quantity the terminal fixes, and
/// the wire action/state names for the decision trace. The terminal classification happens
/// at the wire boundary — this type carries its result.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OrderRevision {
    pub portfolio: PortfolioId,
    /// The revised order's contract — with the portfolio, the pair the revision resolves.
    pub contract: ContractKey,
    pub order_id: String,
    /// The order's `cl_order_id`, echoed by the gateway from the placing order
    /// (`OwnOrder.cl_order_id`) — the join key stitching this revision to its send, its
    /// fills, and the Position Service fold across the trace.
    pub cl_order_id: String,
    pub revision_no: i64,
    /// Invariant 5: this revision ends the order's life on the venue — the in-flight order
    /// clears on seeing it (once the matched fills reconcile the executed quantity).
    pub terminal: bool,
    /// The revision's *remaining* quantity in kW — what a terminal revision fixes as
    /// unexecuted (executed = sent − remaining on a kill or reject; 0 remains on `FEXE`).
    pub quantity: i64,
    /// The wire `OrderAction` short name (`FEXE`, `SDEL`, ...), for the decision trace.
    pub action: String,
    /// The wire `OrderState` short name (`ACTI`, `IACT`, ...), for the decision trace.
    pub state: String,
}

// =============================================================================================
// Evaluation — pure: one prepared input, one clock read, one decision
// =============================================================================================

/// Why an evaluation withheld (or, for [`BlockReason::AtTargetPosition`], why there was
/// nothing to do — reported as `OK`). Two tiers with a fixed precedence — the first that
/// trips wins, keeping the reported reason stable and the logs legible.
///
/// The executor-wide tier short-circuits every gap: the toggle (step 8), the position
/// stale-state interlock (an unavailable baseline withholds *all* trading rather than acting
/// on a frozen gap), and the saturated global order budget. A dead depth feed needs no
/// executor-wide interlock — a pair is only evaluated when its book streams in, so a quiet
/// feed simply stops triggering it (nothing prices off stale depth). The per-pair tier is the
/// declaration order below. `OrderInFlight`
/// covers the whole wait for an outstanding order — from send until it is terminal *and*
/// its fills have been observed folded into the position (the executor draws no distinction;
/// that is the order manager's business). `OrderRejected` names the retry pause after a
/// gateway rejection (where a plain cooldown would misattribute the wait); `OrderTooSmall`
/// is the sub-minimum residual withhold of invariant 4.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum BlockReason {
    // -- executor-wide --
    TradingDisabled,
    PositionServiceDisconnected,
    GlobalOrderThrottling,
    // -- per (portfolio, contract) --
    OrderInFlight,
    AtTargetPosition,
    ContractInactive,
    OrderRejected,
    OrderCooldown,
    OrderThrottling,
    BookCrossed,
    BookEmpty,
    OrderTooSmall,
}

impl fmt::Display for BlockReason {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            BlockReason::TradingDisabled => "TRADING_DISABLED",
            BlockReason::PositionServiceDisconnected => "POSITION_SERVICE_DISCONNECTED",
            BlockReason::GlobalOrderThrottling => "GLOBAL_ORDER_THROTTLING",
            BlockReason::OrderInFlight => "ORDER_IN_FLIGHT",
            BlockReason::AtTargetPosition => "AT_TARGET_POSITION",
            BlockReason::ContractInactive => "CONTRACT_INACTIVE",
            BlockReason::OrderRejected => "ORDER_REJECTED",
            BlockReason::OrderCooldown => "ORDER_COOLDOWN",
            BlockReason::OrderThrottling => "ORDER_THROTTLING",
            BlockReason::BookCrossed => "BOOK_CROSSED",
            BlockReason::BookEmpty => "BOOK_EMPTY",
            BlockReason::OrderTooSmall => "ORDER_TOO_SMALL",
        })
    }
}

/// One evaluation's outcome: the order to send, or exactly one block reason.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Decision {
    Send(OrderIntent),
    Block(BlockReason),
}

/// The order an evaluation decided to send — still in domain terms (invariant 4's shape:
/// positive quantity, the side carrying the sign) plus the decision context the trace
/// logs alongside it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OrderIntent {
    pub portfolio: PortfolioId,
    pub contract: ContractKey,
    pub side: Side,
    /// kW, positive — the side carries the sign.
    pub quantity: i64,
    /// The IOC limit: the best opposite level's price, minor currency units per MWh.
    pub price: i64,
    /// The gap the order works (signed kW) — trace context.
    pub gap: i64,
    /// The book at decision time — trace context (the mid, when one exists, is derivable;
    /// the executor itself never needs it).
    pub best_bid: Option<Level>,
    pub best_ask: Option<Level>,
}

/// The position manager's answer for one pair: the gap, or that no current baseline can be
/// read. `Unavailable` evaluates to `POSITION_SERVICE_DISCONNECTED` — trading without a
/// current baseline is the thing the interlock exists to prevent. "Not seeded yet" and
/// "session dropped" are the same condition here: no live baseline. The manager clears the
/// store on a drop, so there is no frozen-gap variant to distinguish — an absent baseline is
/// an absent baseline.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PositionView {
    /// The portfolio is seeded and its stream is live: the pair's net position (0 when
    /// the seeded portfolio never mentioned the contract — a seeded absence is flat).
    Live(i64),
    /// No live baseline — the portfolio has not seeded yet, or its session dropped (the
    /// store slice was cleared on the drop). Never act on it.
    Unavailable,
}

/// What the eval loop needs from the order manager: whether the pair has an order **in
/// flight**. "In flight" spans the *entire* wait — from the order's send until it is both
/// terminal **and** its fills have been observed folded into the position — because until
/// the gap reflects our own trade a new order would size off an unsettled position and
/// double-execute it (invariants 4–6). The manager's internal split of that wait (waiting
/// for the terminal vs. waiting for the fold) never leaves the manager; the loop cares only
/// that *an order is outstanding*. Only [`OrderState::Idle`] may send.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OrderState {
    /// An order is outstanding — still waiting before another may be sent.
    InFlight,
    /// No order outstanding; free to send. `last_send_rejected` names the pause after a
    /// gateway rejection (`ORDER_REJECTED` rather than `ORDER_COOLDOWN`).
    Idle { last_send_rejected: bool },
}

/// Tradeability's feed half for one contract, as [`ContractSource`] serves it; the time
/// half (the cutoff) is [`evaluate`]'s, since it moves with `now` alone.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Tradeability {
    /// In the live set and `ACTIVE` in the executor's delivery area.
    pub active: bool,
    /// When delivery starts (unix ms) — the cutoff input.
    pub delivery_start_ms: i64,
}

/// The sync loop's read of the position manager (see `docs/refactor.md`): just the gap or
/// its unavailability — no revision, no attribution; those stay inside the managers.
pub trait PositionSource {
    fn position(&self, portfolio: &str, contract: &ContractKey) -> PositionView;
}

/// The sync loop's read of the contract manager: tradeability's feed half, `None` when the
/// live set has never carried the contract (not tradeable — safe by absence).
pub trait ContractSource {
    fn tradeability(&self, contract: &ContractKey) -> Option<Tradeability>;
}

/// The sync loop's interface to the order manager: a pure forwarder. `submit` hands the
/// intent to the manager task and returns `false` only when that channel is unavailable
/// (the manager is wedged or gone) — nothing to do with pair state. The loop owns the
/// pair's [`OrderState`] itself (set `InFlight` on a successful submit, returned to `Idle`
/// by the manager's resolutions), so the broker holds no state and reads nothing.
pub trait OrderBroker {
    fn submit(&self, intent: &OrderIntent) -> bool;
}

/// The sync loop's per-pair [`OrderState`] view — loop-owned and mutable, unlike the
/// read-only [`PositionSource`]/[`ContractSource`] handles. The loop [`Self::mark_in_flight`]s
/// a pair on a successful submit (the double-send guard: it reads its own `InFlight` before
/// the order manager has processed the request), [`Self::refresh`]es the view from the
/// manager's resolutions once per cycle, and reads it with [`Self::get`] when preparing an
/// evaluation. Injecting it as a trait keeps the loop testable against a scripted tracker.
pub trait OrderTracker {
    /// Bring the view up to date with the order manager's published resolutions.
    fn refresh(&mut self);
    /// The pair's order state — `Idle` when it has never sent an order.
    fn get(&self, key: &PairKey) -> OrderState;
    /// Mark the pair's order in flight — called on a successful submit.
    fn mark_in_flight(&mut self, key: PairKey);
}

/// The sync loop's upstream of live [`Book`]s — the depth feed, seen one book at a time. The
/// loop blocks on [`Self::next_book`] (the market feed is its only trigger), evaluating each
/// book's contract as it arrives; `None` means the feed is gone, which the loop treats as
/// fatal. Injecting it as a trait keeps the loop testable against a scripted book source.
pub trait BookSource {
    /// Block until the next book arrives; `None` once the feed is gone.
    fn next_book(&mut self) -> Option<Book>;
}

/// Everything one evaluation reads, prepared by the sync loop from the manager handles —
/// the clean seam between "gather the state" and "decide" (`docs/refactor.md`): [`evaluate`]
/// never touches a handle, a lock, or a clock.
#[derive(Debug, Clone)]
pub struct EvalInput<'a> {
    pub portfolio: &'a str,
    pub contract: ContractKey,
    /// The step-8 toggle's seam; `true` until the toggle lands.
    pub trading_enabled: bool,
    pub position: PositionView,
    pub order: OrderState,
    pub tradeability: Option<Tradeability>,
    /// The contract's live book, if the feed has one.
    pub book: Option<&'a Book>,
    /// The pair's cooldown since its last send is still running (`throttling`).
    pub in_cooldown: bool,
    /// The pair's rate window is full (`throttling`).
    pub pair_window_full: bool,
    /// The executor-wide rate window is full.
    pub global_window_full: bool,
}

/// Evaluate one pair (invariants 1–4 over the phase the order manager maintains): the
/// order to send, or exactly one block reason, in tier precedence order.
pub fn evaluate(input: &EvalInput<'_>, tuning: &ExecutionConfig, now_ms: i64) -> Decision {
    use BlockReason::*;
    // Executor-wide tier.
    if !input.trading_enabled {
        return Decision::Block(TradingDisabled);
    }
    let gap = match input.position {
        PositionView::Live(gap) => gap,
        // No current baseline (never seeded, or dropped and cleared) — never act on it.
        PositionView::Unavailable => {
            return Decision::Block(PositionServiceDisconnected);
        }
    };
    // No depth interlock here: the loop evaluates a pair only when its contract's book streams
    // in, so a dropped feed simply stops triggering it — nothing prices off stale depth, and
    // there is no book-feed state to inspect.
    if input.global_window_full {
        return Decision::Block(GlobalOrderThrottling);
    }
    // Per-pair tier.
    let last_send_rejected = match input.order {
        // In flight — an order is outstanding until it is terminal and its fills are folded.
        OrderState::InFlight => return Decision::Block(OrderInFlight),
        OrderState::Idle { last_send_rejected } => last_send_rejected,
    };
    if gap == 0 {
        return Decision::Block(AtTargetPosition);
    }
    let tradeable = input.tradeability.is_some_and(|tradeability| {
        tradeability.active && tradeability.delivery_start_ms - now_ms > ms(tuning.trading_cutoff)
    });
    if !tradeable {
        return Decision::Block(ContractInactive);
    }
    if input.in_cooldown {
        // One pause, named by its cause: after a rejection the wait is the rejection's,
        // not a routine cooldown — the trace must say which.
        return Decision::Block(if last_send_rejected {
            OrderRejected
        } else {
            OrderCooldown
        });
    }
    if input.pair_window_full {
        return Decision::Block(OrderThrottling);
    }
    let Some(book) = input.book else {
        return Decision::Block(BookEmpty);
    };
    if let (Some(bid), Some(ask)) = (book.best_bid(), book.best_ask())
        && bid.price >= ask.price
    {
        return Decision::Block(BookCrossed);
    }
    // The order takes the opposite side: a sell needs bids, a buy needs asks — the best
    // level only (invariant 3): its price is the limit, its visible size caps the order.
    let (side, level) = if gap > 0 {
        (Side::Sell, book.best_bid())
    } else {
        (Side::Buy, book.best_ask())
    };
    let Some(level) = level else {
        return Decision::Block(BookEmpty);
    };
    let quantity = gap
        .abs()
        .min(tuning.max_order_quantity_kw)
        .min(level.quantity);
    let quantity = quantity - quantity % tuning.quantity_step_kw;
    if quantity < tuning.min_order_quantity_kw {
        return Decision::Block(OrderTooSmall);
    }
    Decision::Send(OrderIntent {
        portfolio: input.portfolio.to_owned(),
        contract: input.contract,
        side,
        quantity,
        price: level.price,
        gap,
        best_bid: book.best_bid().copied(),
        best_ask: book.best_ask().copied(),
    })
}

/// A `Duration` in whole milliseconds, saturating — the unit every temporal gate compares in.
pub(crate) fn ms(duration: Duration) -> i64 {
    i64::try_from(duration.as_millis()).unwrap_or(i64::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn contract() -> ContractKey {
        ContractKey {
            market_type: MarketType::Local,
            contract_id: 7,
        }
    }

    fn book(bids: &[(i64, i64)], asks: &[(i64, i64)]) -> Book {
        let level = |&(price, quantity): &(i64, i64)| Level { price, quantity };
        Book {
            key: contract(),
            bids: bids.iter().map(level).collect(),
            asks: asks.iter().map(level).collect(),
        }
    }

    #[test]
    fn a_book_is_a_full_view_the_best_opposite_level_reads_off() {
        // Each Book is the complete top-N state of its contract — the loop reads the best
        // opposite level's price (the limit) and its visible size (the cap).
        let two_level = book(&[(4_250, 3_000), (4_100, 8_000)], &[]);
        assert_eq!(
            two_level.best_bid(),
            Some(&Level {
                price: 4_250,
                quantity: 3_000
            })
        );
        // The emptied side reads as nothing to take.
        assert_eq!(two_level.best_ask(), None);
    }

    // -----------------------------------------------------------------------------------
    // evaluate — pure, against a scripted clock
    // -----------------------------------------------------------------------------------

    /// A scripted "now" well before the test contract's delivery start (see
    /// `contract_record`): tradeability's time half holds by a wide margin.
    const NOW: i64 = 1_750_000_000_000;

    /// An input that evaluates to a plain sell: +10 MW gap, ACTIVE contract far from
    /// delivery, a two-sided book (bid 1990×10MW / ask 2010×8MW), everything live, no
    /// pacing engaged. Each test breaks exactly the piece it is about.
    fn ready_input<'a>(book: &'a Book, tradeability: &Tradeability) -> EvalInput<'a> {
        EvalInput {
            portfolio: "execution-a",
            contract: contract(),
            trading_enabled: true,
            position: PositionView::Live(10_000),
            order: OrderState::Idle {
                last_send_rejected: false,
            },
            tradeability: Some(*tradeability),
            book: Some(book),
            in_cooldown: false,
            pair_window_full: false,
            global_window_full: false,
        }
    }

    fn ready_book() -> Book {
        book(&[(1_990, 10_000)], &[(2_010, 8_000)])
    }

    fn ready_tradeability() -> Tradeability {
        Tradeability {
            active: true,
            delivery_start_ms: 1_760_000_000_000,
        }
    }

    fn tuning() -> ExecutionConfig {
        ExecutionConfig::default()
    }

    #[test]
    fn a_ready_pair_sells_its_gap_at_the_best_bid() {
        let book = ready_book();
        let input = ready_input(&book, &ready_tradeability());
        let Decision::Send(intent) = evaluate(&input, &tuning(), NOW) else {
            panic!("expected a send: {:?}", evaluate(&input, &tuning(), NOW));
        };
        assert_eq!(intent.side, Side::Sell);
        assert_eq!(intent.quantity, 10_000);
        assert_eq!(intent.price, 1_990);
        assert_eq!(intent.gap, 10_000);
        assert_eq!(intent.portfolio, "execution-a");
    }

    #[test]
    fn executor_wide_gates_trip_in_precedence_order() {
        let book = ready_book();
        let mut input = ready_input(&book, &ready_tradeability());

        // Every executor-wide gate tripped at once: the toggle wins…
        input.trading_enabled = false;
        input.position = PositionView::Unavailable;
        input.global_window_full = true;
        assert_eq!(
            evaluate(&input, &tuning(), NOW),
            Decision::Block(BlockReason::TradingDisabled)
        );

        // …then the position interlock (no live baseline)…
        input.trading_enabled = true;
        assert_eq!(
            evaluate(&input, &tuning(), NOW),
            Decision::Block(BlockReason::PositionServiceDisconnected)
        );

        // …then the global budget.
        input.position = PositionView::Live(10_000);
        assert_eq!(
            evaluate(&input, &tuning(), NOW),
            Decision::Block(BlockReason::GlobalOrderThrottling)
        );
        input.global_window_full = false;
        assert!(matches!(
            evaluate(&input, &tuning(), NOW),
            Decision::Send(_)
        ));
    }

    #[test]
    fn an_order_in_flight_gates_before_everything_per_pair() {
        let book = ready_book();
        let mut input = ready_input(&book, &ready_tradeability());
        // Even with a flat position and a dead contract, an outstanding order reports first:
        // a pair mid-order is not re-decided.
        input.position = PositionView::Live(0);
        input.tradeability = None;

        input.order = OrderState::InFlight;
        assert_eq!(
            evaluate(&input, &tuning(), NOW),
            Decision::Block(BlockReason::OrderInFlight)
        );
        input.order = OrderState::Idle {
            last_send_rejected: false,
        };
        assert_eq!(
            evaluate(&input, &tuning(), NOW),
            Decision::Block(BlockReason::AtTargetPosition),
            "a flat pair is OK — nothing to do"
        );
    }

    #[test]
    fn tradeability_needs_the_active_state_and_the_cutoff_margin() {
        let book = ready_book();
        let mut input = ready_input(&book, &ready_tradeability());

        // Unknown to the live set ⇒ not tradeable.
        input.tradeability = None;
        assert_eq!(
            evaluate(&input, &tuning(), NOW),
            Decision::Block(BlockReason::ContractInactive)
        );

        // Present but not ACTIVE ⇒ not tradeable.
        input.tradeability = Some(Tradeability {
            active: false,
            delivery_start_ms: 1_760_000_000_000,
        });
        assert_eq!(
            evaluate(&input, &tuning(), NOW),
            Decision::Block(BlockReason::ContractInactive)
        );

        // ACTIVE but inside the cutoff (delivery − 1h) ⇒ the executor has given up on it.
        input.tradeability = Some(ready_tradeability());
        let inside_cutoff = 1_760_000_000_000 - 3_600_000;
        assert_eq!(
            evaluate(&input, &tuning(), inside_cutoff),
            Decision::Block(BlockReason::ContractInactive)
        );
        // Exactly at the margin is still blocked (strictly-more-than); one ms earlier is not.
        assert!(matches!(
            evaluate(&input, &tuning(), inside_cutoff - 1),
            Decision::Send(_)
        ));
    }

    #[test]
    fn the_pause_after_a_send_names_its_cause() {
        let book = ready_book();
        let mut input = ready_input(&book, &ready_tradeability());
        input.in_cooldown = true;
        assert_eq!(
            evaluate(&input, &tuning(), NOW),
            Decision::Block(BlockReason::OrderCooldown)
        );
        // After a gateway rejection the same pause is the rejection's — the trace says so.
        input.order = OrderState::Idle {
            last_send_rejected: true,
        };
        assert_eq!(
            evaluate(&input, &tuning(), NOW),
            Decision::Block(BlockReason::OrderRejected)
        );
        // Once the pause elapses the standing gap retries either way.
        input.in_cooldown = false;
        assert!(matches!(
            evaluate(&input, &tuning(), NOW),
            Decision::Send(_)
        ));
    }

    #[test]
    fn the_rate_window_throttles_past_the_cooldown() {
        let book = ready_book();
        let mut input = ready_input(&book, &ready_tradeability());
        input.pair_window_full = true;
        assert_eq!(
            evaluate(&input, &tuning(), NOW),
            Decision::Block(BlockReason::OrderThrottling)
        );
    }

    #[test]
    fn book_gates_crossed_then_empty_by_side() {
        let mut input = ready_input(
            // Leaked boxes are fine in tests; simpler than a self-referential holder.
            Box::leak(Box::new(book(&[(2_020, 5_000)], &[(2_010, 5_000)]))),
            &ready_tradeability(),
        );
        assert_eq!(
            evaluate(&input, &tuning(), NOW),
            Decision::Block(BlockReason::BookCrossed),
            "bid ≥ ask is a broken book — withhold, never price off it"
        );

        let no_bids = book(&[], &[(2_010, 5_000)]);
        input.book = Some(&no_bids);
        assert_eq!(
            evaluate(&input, &tuning(), NOW),
            Decision::Block(BlockReason::BookEmpty),
            "a sell with no bids has nothing to take"
        );

        input.book = None;
        assert_eq!(
            evaluate(&input, &tuning(), NOW),
            Decision::Block(BlockReason::BookEmpty),
            "no live view at all"
        );

        // A buy gap needs asks: bids alone are still BOOK_EMPTY.
        let no_asks = book(&[(1_990, 5_000)], &[]);
        input.book = Some(&no_asks);
        input.position = PositionView::Live(-5_000);
        assert_eq!(
            evaluate(&input, &tuning(), NOW),
            Decision::Block(BlockReason::BookEmpty)
        );
    }

    #[test]
    fn orders_are_sized_by_gap_cap_and_visible_liquidity_on_the_grid() {
        // The top level caps the order below the gap and the per-order cap, floored to
        // the quantity grid.
        let level_capped = book(&[(1_990, 7_350)], &[(2_010, 8_000)]);
        let mut input = ready_input(&level_capped, &ready_tradeability());
        input.position = PositionView::Live(25_000);
        let Decision::Send(intent) = evaluate(&input, &tuning(), NOW) else {
            panic!("expected a send");
        };
        assert_eq!(intent.quantity, 7_300, "level-capped, floored to 100 kW");

        // The per-order cap binds when the level is deep.
        let deep = book(&[(1_990, 50_000)], &[]);
        input.book = Some(&deep);
        let Decision::Send(intent) = evaluate(&input, &tuning(), NOW) else {
            panic!("expected a send");
        };
        assert_eq!(intent.quantity, 10_000, "max_order_quantity_kw");

        // A buy takes the ask side at its price.
        let two_sided = ready_book();
        input.book = Some(&two_sided);
        input.position = PositionView::Live(-4_000);
        let Decision::Send(intent) = evaluate(&input, &tuning(), NOW) else {
            panic!("expected a buy");
        };
        assert_eq!(intent.side, Side::Buy);
        assert_eq!(intent.price, 2_010);
        assert_eq!(intent.quantity, 4_000);

        // A sub-minimum residual withholds — whether from the gap or a tiny top level.
        input.position = PositionView::Live(-50);
        assert_eq!(
            evaluate(&input, &tuning(), NOW),
            Decision::Block(BlockReason::OrderTooSmall)
        );
        let thin = book(&[(1_990, 90)], &[]);
        input.book = Some(&thin);
        input.position = PositionView::Live(10_000);
        assert_eq!(
            evaluate(&input, &tuning(), NOW),
            Decision::Block(BlockReason::OrderTooSmall)
        );
    }
}
