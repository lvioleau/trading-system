//! The engine: [`run`] wires the managers together and drives the synchronous eval loop —
//! the whole running executor behind one future (`main.rs` races it against the shutdown
//! signal; the black-box tests spawn it, so what they exercise is exactly what production
//! runs).
//!
//! **The split** (`docs/refactor.md`): everything with a stream, a reconnect, or an order
//! lifecycle lives in the async managers — the position manager (the gap map), the
//! contract manager (tradeability), the order manager (sending + lifecycle), and the
//! FillMatcher between them. The decision lives on one dedicated thread, the sync loop
//! here, which reads the position and contract managers through their sync handles
//! ([`PositionSource`], [`ContractSource`]), submits orders through [`OrderBroker`] (a pure
//! forwarder), and owns each pair's [`OrderState`] itself (is an order in flight?) — set
//! `InFlight` on submit, returned to `Idle` by the order manager's resolutions. No async
//! interleaving can split a decision.
//!
//! **The trigger is the market feed — and only that.** The loop blocks on the book channel,
//! receiving one [`Book`] at a time — a full top-N view of one contract — and evaluates every
//! configured portfolio's pair on that contract against the view *in hand*. It keeps no book
//! between messages, because a pair is only ever evaluated when its own book arrives. It also
//! drains the order manager's resolutions to keep its order-state view current, but a
//! resolution does *not* trigger anything. There is no sweep tick: a pair paused on a cooldown,
//! or freed by a resolution, is picked up by the next book tick on its contract, which the live
//! market supplies continuously. One clock read per cycle stamps every temporal gate.
//!
//! **The decision trace** is the audit trail (stateless — no store): every submitted
//! order is logged in full; the blocked/idle evaluations are logged per pair on a reason
//! change and at most once per `eval_log_throttle` otherwise, so a burst of book ticks
//! cannot drown the steady state.

use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::anyhow;
use tokio::sync::mpsc;
use tracing::{debug, info};

use crate::config::{Config, ExecutionConfig};
use crate::contract_manager;
use crate::depth_feed;
use crate::executor::{
    Book, BookSource, ContractSource, Decision, EvalInput, OrderBroker, OrderTracker, PairKey,
    PortfolioId, PositionSource, evaluate,
};
use crate::fill_matcher;
use crate::gateway;
use crate::order_manager;
use crate::order_sender::OrderSender;
use crate::order_states::OrderStates;
use crate::position_manager;
use crate::throttling::Throttling;
use crate::trace::EvalTrace;

/// Capacity of the cross-manager channels (fills/folds → matcher, matched fills → order
/// manager, depth → the loop). Backpressure, not sizing: a full channel parks the sender.
const CHANNEL_CAPACITY: usize = 256;

/// Run the whole executor: spawn the managers and drive the eval loop until something
/// fails fatally — a rejected subscription (permanent config mismatch), a dead task, or a
/// closed channel. Never returns otherwise.
pub async fn run(config: Config) -> anyhow::Error {
    // The instance discriminator in every cl_order_id this run stamps: the boot time in
    // nanos — unique across restarts, so a resurrected executor can never join a previous
    // run's orders.
    let instance = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock before the epoch")
        .as_nanos()
        .to_string();

    let position_handle = position_manager::handle(&config.portfolios);
    let contract_handle = contract_manager::handle();
    let (broker, order_requests_rx) = order_manager::broker();

    let (fills_tx, fills_rx) = mpsc::channel(CHANNEL_CAPACITY);
    let (position_folds_tx, position_folds_rx) = mpsc::channel(CHANNEL_CAPACITY);
    let (matched_fills_tx, matched_fills_rx) = mpsc::channel(CHANNEL_CAPACITY);
    let (resolutions_tx, resolutions_rx) = mpsc::channel(CHANNEL_CAPACITY);
    let (responses_tx, responses_rx) = mpsc::channel(CHANNEL_CAPACITY);
    let (gateway_events_tx, gateway_events_rx) = mpsc::channel(CHANNEL_CAPACITY);
    let (books_tx, books_rx) = mpsc::channel::<Book>(CHANNEL_CAPACITY);

    let mut position_manager_task = tokio::spawn(position_manager::run(
        config.position_endpoint.clone(),
        config.portfolios.clone(),
        config.feed,
        position_handle.clone(),
        position_folds_tx,
    ));
    let mut contract_manager_task = tokio::spawn(contract_manager::run(
        config.contract_endpoint.clone(),
        config.delivery_area_eic,
        config.feed,
        contract_handle.clone(),
    ));
    let mut fill_matcher_task = tokio::spawn(fill_matcher::run(
        fills_rx,
        position_folds_rx,
        matched_fills_tx,
    ));
    // The gateway directions, all wired here: the `AddOrder` sender (write path) reporting
    // classified outcomes on `responses` and injected into the order manager; the own-orders
    // subscription feeding `gateway_events` to the manager; and the own-trades subscription
    // feeding fills straight to the FillMatcher on `fills` — separate channels, separate owners.
    let order_sender = match OrderSender::new(
        &config.gateway_endpoint,
        config.exchange.clone(),
        config.delivery_area_eic.to_string(),
        &config.feed,
        responses_tx,
    ) {
        Ok(sender) => sender,
        Err(error) => return error.context("building the order sender"),
    };
    let mut gateway_subscriptions_task = tokio::spawn(gateway::run_subscriptions(
        config.gateway_endpoint.clone(),
        config.portfolios.clone(),
        config.feed,
        gateway_events_tx,
        fills_tx,
    ));
    let mut order_manager_task = tokio::spawn(order_manager::run(
        config.execution.order_inflight_warn,
        config.execution.order_resolve_timeout,
        instance,
        order_sender,
        order_requests_rx,
        matched_fills_rx,
        resolutions_tx,
        responses_rx,
        gateway_events_rx,
    ));
    let mut depth_feed_task = tokio::spawn(depth_feed::run(
        config.connector_endpoint.clone(),
        config.delivery_area_eic,
        config.feed,
        books_tx,
    ));

    // The decision thread: blocking-recv on the book channel, no async inside.
    let portfolios = config.portfolios.clone();
    let tuning = config.execution;
    let book_source = depth_feed::DepthBooks::new(books_rx);
    let order_states = OrderStates::new(resolutions_rx);
    let mut eval_loop_task = tokio::task::spawn_blocking(move || {
        eval_loop(
            book_source,
            order_states,
            position_handle,
            contract_handle,
            broker,
            portfolios,
            tuning,
        )
    });

    // Every task runs forever; the first fatal error wins.
    tokio::select! {
        result = &mut eval_loop_task => task_error("eval loop", result),
        result = &mut position_manager_task => task_error("position manager", result),
        result = &mut contract_manager_task => task_error("contract manager", result),
        result = &mut order_manager_task => task_error("order manager", result),
        result = &mut gateway_subscriptions_task => task_error("gateway subscriptions", result),
        result = &mut fill_matcher_task => task_error("fill matcher", result),
        result = &mut depth_feed_task => task_error("depth feed", result),
    }
}

fn task_error(task: &str, result: Result<anyhow::Error, tokio::task::JoinError>) -> anyhow::Error {
    match result {
        Ok(error) => error.context(format!("{task} failed")),
        Err(join_error) => anyhow!("{task} task exited: {join_error}"),
    }
}

/// The synchronous eval loop: receive one book, evaluate each portfolio's pair on its
/// contract against that book, submit what passes.
fn eval_loop(
    mut books: impl BookSource,
    mut order_states: impl OrderTracker,
    positions: impl PositionSource,
    contracts: impl ContractSource,
    broker: impl OrderBroker,
    portfolios: Vec<PortfolioId>,
    tuning: ExecutionConfig,
) -> anyhow::Error {
    let mut throttling = Throttling::new(&tuning);
    let mut trace = EvalTrace::default();

    loop {
        // One book at a time — a full top-N view of its contract. A pair is only ever
        // evaluated when its own book arrives, so the just-arrived view is the only one it
        // needs and nothing is kept between books; a dropped feed simply goes quiet, and
        // nothing prices off stale depth.
        let Some(book) = books.next_book() else {
            return anyhow!("depth book channel closed; the depth feed is gone");
        };
        let now_ms = unix_ms();
        let contract = book.key;

        // Apply the order manager's resolutions before evaluating. A freed pair is not
        // re-triggered: like every other change, it is picked up on the next book tick on its
        // contract — the market feed is the loop's only trigger.
        order_states.refresh();

        for portfolio in &portfolios {
            let key: PairKey = (portfolio.clone(), contract);
            // Prepare: gather one consistent input from the manager handles, this contract's
            // just-arrived book, and the loop-owned throttling — then decide. The seam
            // `evaluate` never crosses.
            let input = EvalInput {
                portfolio,
                contract,
                trading_enabled: true, // the step-8 toggle's seam
                position: positions.position(portfolio, &contract),
                order: order_states.get(&key),
                tradeability: contracts.tradeability(&contract),
                book: Some(&book),
                in_cooldown: throttling.in_cooldown(&key, now_ms),
                pair_window_full: throttling.pair_window_full(&key, now_ms),
                global_window_full: throttling.global_window_full(now_ms),
            };
            match evaluate(&input, &tuning, now_ms) {
                Decision::Send(intent) => {
                    // Forward to the manager, then mark the pair `InFlight` locally so
                    // the next cycle can't double-send before the manager processes it.
                    // A refusal means the manager's request channel is wedged — leave
                    // the pair `Ready`; the standing gap re-decides on the next tick.
                    if broker.submit(&intent) {
                        order_states.mark_in_flight(key.clone());
                        throttling.record_send(&key, now_ms);
                        trace.sent(&key);
                        info!(?intent, "order submitted");
                    } else {
                        debug!(
                            portfolio,
                            contract = %contract,
                            "order request channel unavailable; submit dropped"
                        );
                    }
                }
                Decision::Block(reason) => trace.blocked(&key, reason, &input, now_ms, &tuning),
            }
        }
    }
}

fn unix_ms() -> i64 {
    i64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock before the epoch")
            .as_millis(),
    )
    .expect("unix time overflows i64 ms")
}

#[cfg(test)]
mod tests {
    use std::cell::RefCell;
    use std::collections::{HashMap, VecDeque};
    use std::rc::Rc;
    use std::time::Duration;

    use super::{eval_loop, task_error};
    use crate::config::ExecutionConfig;
    use crate::executor::{
        Book, BookSource, ContractKey, ContractSource, Level, MarketType, OrderBroker, OrderIntent,
        OrderState, OrderTracker, PairKey, PositionSource, PositionView, Side, Tradeability,
    };

    /// A shared, mutable recording handle onto what a fake captured — clonable so the test
    /// keeps one end after the other is moved into `eval_loop`.
    type Recorder<T> = Rc<RefCell<Vec<T>>>;

    fn contract() -> ContractKey {
        ContractKey {
            market_type: MarketType::Local,
            contract_id: 7,
        }
    }

    /// A two-sided book on `contract()`: bid 1990×10MW, ask 2010×8MW — a sell takes the bid.
    fn a_book() -> Book {
        Book {
            key: contract(),
            bids: vec![Level {
                price: 1_990,
                quantity: 10_000,
            }],
            asks: vec![Level {
                price: 2_010,
                quantity: 8_000,
            }],
        }
    }

    fn tuning() -> ExecutionConfig {
        ExecutionConfig::default()
    }

    /// The depth feed, scripted: hands back each book in turn, then `None` (feed gone), which
    /// is what stops the loop so a test can inspect the recording fakes.
    struct ScriptedBooks(VecDeque<Book>);
    impl BookSource for ScriptedBooks {
        fn next_book(&mut self) -> Option<Book> {
            self.0.pop_front()
        }
    }

    /// A tracker that behaves like the real `OrderStates` (Idle-by-default, InFlight on mark)
    /// while recording the marks and refresh calls, and optionally applying one scripted
    /// resolution delta per refresh — the stand-in for the order manager's resolutions.
    struct RecordingTracker {
        states: HashMap<PairKey, OrderState>,
        marked: Recorder<PairKey>,
        refreshes: Rc<RefCell<usize>>,
        deltas: VecDeque<Vec<(PairKey, OrderState)>>,
    }
    impl OrderTracker for RecordingTracker {
        fn refresh(&mut self) {
            *self.refreshes.borrow_mut() += 1;
            if let Some(delta) = self.deltas.pop_front() {
                for (key, state) in delta {
                    self.states.insert(key, state);
                }
            }
        }
        fn get(&self, key: &PairKey) -> OrderState {
            self.states.get(key).copied().unwrap_or(OrderState::Idle {
                last_send_rejected: false,
            })
        }
        fn mark_in_flight(&mut self, key: PairKey) {
            self.marked.borrow_mut().push(key.clone());
            self.states.insert(key, OrderState::InFlight);
        }
    }

    /// A tracker with fresh recorders and no scripted deltas, plus handles onto its marks and
    /// refresh count.
    fn tracker() -> (RecordingTracker, Recorder<PairKey>, Rc<RefCell<usize>>) {
        let marked = Rc::new(RefCell::new(Vec::new()));
        let refreshes = Rc::new(RefCell::new(0));
        (
            RecordingTracker {
                states: HashMap::new(),
                marked: marked.clone(),
                refreshes: refreshes.clone(),
                deltas: VecDeque::new(),
            },
            marked,
            refreshes,
        )
    }

    /// Positions by pair, with a default for the unmapped (the position-service interlock:
    /// `Unavailable` withholds all trading on that pair).
    struct MapPositions(HashMap<PairKey, PositionView>, PositionView);
    impl PositionSource for MapPositions {
        fn position(&self, portfolio: &str, contract: &ContractKey) -> PositionView {
            self.0
                .get(&(portfolio.to_owned(), *contract))
                .copied()
                .unwrap_or(self.1)
        }
    }
    fn positions(entries: &[(&str, PositionView)]) -> MapPositions {
        let map = entries
            .iter()
            .map(|(portfolio, view)| (((*portfolio).to_owned(), contract()), *view))
            .collect();
        MapPositions(map, PositionView::Unavailable)
    }

    /// Every contract active and far from delivery — tradeability's time/state half never the
    /// thing under test here.
    struct AlwaysTradeable;
    impl ContractSource for AlwaysTradeable {
        fn tradeability(&self, _contract: &ContractKey) -> Option<Tradeability> {
            Some(Tradeability {
                active: true,
                delivery_start_ms: 10_000_000_000_000,
            })
        }
    }

    /// Records every submitted intent; `accept` is the manager-channel-available answer the
    /// broker returns.
    struct RecordingBroker {
        intents: Recorder<OrderIntent>,
        accept: bool,
    }
    impl OrderBroker for RecordingBroker {
        fn submit(&self, intent: &OrderIntent) -> bool {
            self.intents.borrow_mut().push(intent.clone());
            self.accept
        }
    }
    fn broker(accept: bool) -> (RecordingBroker, Recorder<OrderIntent>) {
        let intents = Rc::new(RefCell::new(Vec::new()));
        (
            RecordingBroker {
                intents: intents.clone(),
                accept,
            },
            intents,
        )
    }

    #[test]
    fn a_ready_pair_submits_marks_in_flight_and_refreshes_once_per_book() {
        let key = ("execution-a".to_owned(), contract());
        let (tracker, marked, refreshes) = tracker();
        let (broker, intents) = broker(true);

        let error = eval_loop(
            ScriptedBooks(VecDeque::from(vec![a_book()])),
            tracker,
            positions(&[("execution-a", PositionView::Live(10_000))]),
            AlwaysTradeable,
            broker,
            vec!["execution-a".to_owned()],
            tuning(),
        );

        // The loop treats the closed depth channel as fatal and returns.
        assert!(error.to_string().contains("depth book channel closed"));
        let intents = intents.borrow();
        assert_eq!(intents.len(), 1);
        assert_eq!(intents[0].side, Side::Sell);
        assert_eq!(intents[0].quantity, 10_000);
        assert_eq!(intents[0].price, 1_990);
        assert_eq!(
            *marked.borrow(),
            vec![key],
            "a submit marks the pair in flight"
        );
        assert_eq!(
            *refreshes.borrow(),
            1,
            "resolutions drained once per book tick"
        );
    }

    #[test]
    fn every_configured_portfolio_is_evaluated_on_each_book() {
        let (tracker, ..) = tracker();
        let (broker, intents) = broker(true);

        let _ = eval_loop(
            ScriptedBooks(VecDeque::from(vec![a_book()])),
            tracker,
            positions(&[
                ("execution-a", PositionView::Live(10_000)), // long ⇒ sell
                ("execution-b", PositionView::Live(-4_000)), // short ⇒ buy
            ]),
            AlwaysTradeable,
            broker,
            vec!["execution-a".to_owned(), "execution-b".to_owned()],
            tuning(),
        );

        let intents = intents.borrow();
        assert_eq!(
            intents.len(),
            2,
            "one book evaluates both portfolios' pairs"
        );
        let by_portfolio: HashMap<_, _> = intents
            .iter()
            .map(|intent| (intent.portfolio.clone(), intent.side))
            .collect();
        assert_eq!(by_portfolio["execution-a"], Side::Sell);
        assert_eq!(by_portfolio["execution-b"], Side::Buy);
    }

    #[test]
    fn the_in_flight_guard_stops_a_second_send_before_the_manager_resolves() {
        let (tracker, marked, _) = tracker();
        let (broker, intents) = broker(true);
        // Cooldown to zero, so the *only* thing that can hold the second book's send is the
        // in-flight mark the first book set — not a routine cooldown.
        let mut tuning = tuning();
        tuning.order_cooldown = Duration::ZERO;

        let _ = eval_loop(
            ScriptedBooks(VecDeque::from(vec![a_book(), a_book()])),
            tracker,
            positions(&[("execution-a", PositionView::Live(10_000))]),
            AlwaysTradeable,
            broker,
            vec!["execution-a".to_owned()],
            tuning,
        );

        assert_eq!(
            intents.borrow().len(),
            1,
            "the in-flight pair is not re-sent"
        );
        assert_eq!(marked.borrow().len(), 1);
    }

    #[test]
    fn a_refused_submit_leaves_the_pair_ready_to_retry() {
        let (tracker, marked, _) = tracker();
        // The manager's request channel is unavailable: submit returns false.
        let (broker, intents) = broker(false);

        let _ = eval_loop(
            ScriptedBooks(VecDeque::from(vec![a_book(), a_book()])),
            tracker,
            positions(&[("execution-a", PositionView::Live(10_000))]),
            AlwaysTradeable,
            broker,
            vec!["execution-a".to_owned()],
            tuning(),
        );

        assert_eq!(
            intents.borrow().len(),
            2,
            "a refused submit is retried on the next book — the standing gap re-decides"
        );
        assert!(
            marked.borrow().is_empty(),
            "a refused submit never marks the pair in flight"
        );
    }

    #[test]
    fn a_pair_with_no_baseline_submits_nothing() {
        let (tracker, marked, _) = tracker();
        let (broker, intents) = broker(true);

        let _ = eval_loop(
            ScriptedBooks(VecDeque::from(vec![a_book()])),
            tracker,
            // No mapping ⇒ the default Unavailable ⇒ POSITION_SERVICE_DISCONNECTED.
            positions(&[]),
            AlwaysTradeable,
            broker,
            vec!["execution-a".to_owned()],
            tuning(),
        );

        assert!(intents.borrow().is_empty(), "a blocked pair sends nothing");
        assert!(marked.borrow().is_empty());
    }

    #[test]
    fn a_resolution_applied_on_refresh_frees_the_pair_to_send() {
        let key = ("execution-a".to_owned(), contract());
        let marked = Rc::new(RefCell::new(Vec::new()));
        // The pair starts in flight; the order manager's resolution — applied on the first
        // refresh, before this book is evaluated — frees it back to Idle, so it sends.
        let tracker = RecordingTracker {
            states: HashMap::from([(key.clone(), OrderState::InFlight)]),
            marked: marked.clone(),
            refreshes: Rc::new(RefCell::new(0)),
            deltas: VecDeque::from(vec![vec![(
                key.clone(),
                OrderState::Idle {
                    last_send_rejected: false,
                },
            )]]),
        };
        let (broker, intents) = broker(true);

        let _ = eval_loop(
            ScriptedBooks(VecDeque::from(vec![a_book()])),
            tracker,
            positions(&[("execution-a", PositionView::Live(10_000))]),
            AlwaysTradeable,
            broker,
            vec!["execution-a".to_owned()],
            tuning(),
        );

        assert_eq!(
            intents.borrow().len(),
            1,
            "the pair, freed to Idle on refresh, sends this tick"
        );
        assert_eq!(*marked.borrow(), vec![key]);
    }

    #[test]
    fn task_error_wraps_the_task_name_around_its_cause() {
        let error = task_error("eval loop", Ok(anyhow::anyhow!("boom")));
        let chain = format!("{error:#}");
        assert!(chain.contains("eval loop failed"), "chain: {chain}");
        assert!(chain.contains("boom"), "chain: {chain}");
    }
}
