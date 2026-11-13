//! Black-box tests of the gateway clients (`gateway::run_subscriptions` and
//! `order_sender::OrderSender` — exactly what production boots) against a scripted mock gateway
//! over a local socket, serving both `PowerbotOrderService` and
//! `PowerbotExecutionService` from this crate's own generated server stubs (kept for
//! exactly this purpose). Covered per the step-5 plan: an order round-trips under the
//! right portfolio id and its advisory response arrives as an event; a terminal own-order
//! revision resolves under the right portfolio; fills accrue signed to the right portfolio
//! and dedupe on replay; the seed contributes no fill state; a gateway rejection is a
//! distinct event; an unserved portfolio fails the boot instead of retrying; a foreign
//! portfolio id is ignored; two portfolios' fill streams stay independent; and a transport
//! error on `AddOrder` stays ambiguous — resolution belongs to the feeds.

// Mocking a tonic service means returning `tonic::Status` in `Result` throughout; the large
// Err variant is tonic's, and it only ever goes back over the wire.
#![expect(clippy::result_large_err)]

use std::collections::{HashMap, VecDeque};
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::net::TcpListener;
use tokio::sync::{broadcast, mpsc, watch};
use tokio::time::timeout;
use tokio_stream::wrappers::errors::BroadcastStreamRecvError;
use tokio_stream::wrappers::{BroadcastStream, TcpListenerStream};
use tokio_stream::{Stream, StreamExt};
use tonic::{Request, Response, Status};

use id_executor::config::FeedConfig;
use id_executor::executor::{ContractKey, Fill, MarketType, Side};
use id_executor::gateway::{self, GatewayEvent};
use id_executor::gateway_proto as wire;
use id_executor::gateway_proto::powerbot_execution_service_server::{
    PowerbotExecutionService, PowerbotExecutionServiceServer,
};
use id_executor::gateway_proto::powerbot_order_service_server::{
    PowerbotOrderService, PowerbotOrderServiceServer,
};
use id_executor::order_sender::{
    OrderOutcome, OrderRequest, OrderResponse, OrderSender, OrderSink,
};

const EXCHANGE: &str = "nordpool";
const EIC: &str = "10YFR-RTE------C";
const CONTRACT: i64 = 7;

// ---------------------------------------------------------------------------------------------
// Mock gateway
// ---------------------------------------------------------------------------------------------

/// Scripted `AddOrder`: every request is recorded for wire-shape assertions and answered
/// from the front of the script (a plain accept when the script is empty).
struct MockOrders {
    requests: Arc<Mutex<Vec<wire::AddOrderRequest>>>,
    script: Arc<Mutex<VecDeque<Result<wire::AddOrderResponse, Status>>>>,
}

#[tonic::async_trait]
impl PowerbotOrderService for MockOrders {
    async fn add_order(
        &self,
        request: Request<wire::AddOrderRequest>,
    ) -> Result<Response<wire::AddOrderResponse>, Status> {
        let request = request.into_inner();
        self.requests.lock().unwrap().push(request);
        match self.script.lock().unwrap().pop_front() {
            Some(Ok(response)) => Ok(Response::new(response)),
            Some(Err(status)) => Err(status),
            None => Ok(Response::new(wire::AddOrderResponse {
                order: Some(own_order("O-DEFAULT", wire::OrderAction::Uadd)),
            })),
        }
    }
}

/// The execution streams, portfolio-scoped like the real gateway: a subscription for an
/// unserved portfolio is rejected with INVALID_ARGUMENT; a served one gets its seed, then
/// broadcast update tagged with its scope — **verbatim**, so a test can push a payload
/// whose inner portfolio id is foreign (the gateway-bug tripwire the client must catch).
struct MockExecution {
    served: Vec<String>,
    trades_seed: HashMap<String, Vec<wire::Trade>>,
    trades_tx: broadcast::Sender<(String, wire::OwnTradesUpdate)>,
    orders_tx: broadcast::Sender<(String, wire::OwnOrdersUpdate)>,
    shutdown: watch::Receiver<bool>,
}

impl MockExecution {
    fn check_scope(&self, portfolio: &str) -> Result<(), Status> {
        if self.served.iter().any(|p| p == portfolio) {
            Ok(())
        } else {
            Err(Status::invalid_argument(format!(
                "portfolio_id {portfolio:?} is not served by this gateway (it serves {:?})",
                self.served
            )))
        }
    }
}

type EventStream<T> = Pin<Box<dyn Stream<Item = Result<T, Status>> + Send>>;

#[tonic::async_trait]
impl PowerbotExecutionService for MockExecution {
    type SubscribeOwnTradesStream = EventStream<wire::OwnTradesUpdate>;
    type SubscribeOwnOrdersStream = EventStream<wire::OwnOrdersUpdate>;

    async fn subscribe_own_trades(
        &self,
        request: Request<wire::SubscribeOwnTradesRequest>,
    ) -> Result<Response<Self::SubscribeOwnTradesStream>, Status> {
        let portfolio = request.into_inner().portfolio_id;
        self.check_scope(&portfolio)?;
        let seed = wire::OwnTradesUpdate {
            received_at_nanos: 0,
            seq: 1,
            trades: self
                .trades_seed
                .get(&portfolio)
                .cloned()
                .unwrap_or_default(),
        };
        let deltas = BroadcastStream::new(self.trades_tx.subscribe()).filter_map(
            move |received: Result<(String, wire::OwnTradesUpdate), BroadcastStreamRecvError>| {
                match received {
                    Ok((scope, update)) if scope == portfolio => Some(Ok(update)),
                    _ => None,
                }
            },
        );
        let mut shutdown = self.shutdown.clone();
        Ok(Response::new(Box::pin(
            futures_util::StreamExt::take_until(
                tokio_stream::once(Ok(seed)).chain(deltas),
                async move {
                    let _ = shutdown.changed().await;
                },
            ),
        )))
    }

    async fn subscribe_own_orders(
        &self,
        request: Request<wire::SubscribeOwnOrdersRequest>,
    ) -> Result<Response<Self::SubscribeOwnOrdersStream>, Status> {
        let portfolio = request.into_inner().portfolio_id;
        self.check_scope(&portfolio)?;
        // IOC leaves nothing resting: the seed is always empty here, as in production.
        let seed = wire::OwnOrdersUpdate {
            received_at_nanos: 0,
            seq: 1,
            orders: vec![],
        };
        let deltas = BroadcastStream::new(self.orders_tx.subscribe()).filter_map(
            move |received: Result<(String, wire::OwnOrdersUpdate), BroadcastStreamRecvError>| {
                match received {
                    Ok((scope, update)) if scope == portfolio => Some(Ok(update)),
                    _ => None,
                }
            },
        );
        let mut shutdown = self.shutdown.clone();
        Ok(Response::new(Box::pin(
            futures_util::StreamExt::take_until(
                tokio_stream::once(Ok(seed)).chain(deltas),
                async move {
                    let _ = shutdown.changed().await;
                },
            ),
        )))
    }
}

/// The running mock's handles.
struct MockGateway {
    endpoint: String,
    requests: Arc<Mutex<Vec<wire::AddOrderRequest>>>,
    script: Arc<Mutex<VecDeque<Result<wire::AddOrderResponse, Status>>>>,
    trades_tx: broadcast::Sender<(String, wire::OwnTradesUpdate)>,
    orders_tx: broadcast::Sender<(String, wire::OwnOrdersUpdate)>,
    _shutdown: watch::Sender<bool>,
    _task: tokio::task::JoinHandle<()>,
}

async fn start_mock(served: &[&str], trades_seed: &[(&str, Vec<wire::Trade>)]) -> MockGateway {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    let requests = Arc::new(Mutex::new(Vec::new()));
    let script = Arc::new(Mutex::new(VecDeque::new()));
    let (trades_tx, _) = broadcast::channel(16);
    let (orders_tx, _) = broadcast::channel(16);
    let (shutdown, shutdown_rx) = watch::channel(false);
    let orders = MockOrders {
        requests: requests.clone(),
        script: script.clone(),
    };
    let execution = MockExecution {
        served: served.iter().map(|p| p.to_string()).collect(),
        trades_seed: trades_seed
            .iter()
            .map(|(portfolio, trades)| (portfolio.to_string(), trades.clone()))
            .collect(),
        trades_tx: trades_tx.clone(),
        orders_tx: orders_tx.clone(),
        shutdown: shutdown_rx,
    };
    let task = tokio::spawn(async move {
        let _ = tonic::transport::Server::builder()
            .add_service(PowerbotOrderServiceServer::new(orders))
            .add_service(PowerbotExecutionServiceServer::new(execution))
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await;
    });
    MockGateway {
        endpoint,
        requests,
        script,
        trades_tx,
        orders_tx,
        _shutdown: shutdown,
        _task: task,
    }
}

// ---------------------------------------------------------------------------------------------
// Harness
// ---------------------------------------------------------------------------------------------

fn fast_tuning() -> FeedConfig {
    FeedConfig {
        initial_backoff: Duration::from_millis(50),
        max_backoff: Duration::from_millis(200),
        ..FeedConfig::default()
    }
}

fn portfolios(names: &[&str]) -> Vec<String> {
    names.iter().map(|name| name.to_string()).collect()
}

fn start_feeds(
    endpoint: &str,
    names: &[&str],
) -> (mpsc::Receiver<GatewayEvent>, mpsc::Receiver<Fill>) {
    let (events_tx, events_rx) = mpsc::channel(64);
    let (fills_tx, fills_rx) = mpsc::channel(64);
    tokio::spawn(gateway::run_subscriptions(
        endpoint.to_owned(),
        portfolios(names),
        fast_tuning(),
        events_tx,
        fills_tx,
    ));
    (events_rx, fills_rx)
}

fn order_sender(endpoint: &str) -> (OrderSender, mpsc::Receiver<OrderResponse>) {
    let (responses_tx, responses_rx) = mpsc::channel(64);
    let sender = OrderSender::new(
        endpoint,
        EXCHANGE.to_owned(),
        EIC.to_owned(),
        &fast_tuning(),
        responses_tx,
    )
    .unwrap();
    (sender, responses_rx)
}

async fn next_event(events: &mut mpsc::Receiver<GatewayEvent>) -> GatewayEvent {
    timeout(Duration::from_secs(5), events.recv())
        .await
        .expect("timed out waiting for a gateway event")
        .expect("the feed dropped its event sender")
}

async fn next_fill(fills: &mut mpsc::Receiver<Fill>) -> Fill {
    timeout(Duration::from_secs(5), fills.recv())
        .await
        .expect("timed out waiting for a fill")
        .expect("the feed dropped its fill sender")
}

async fn next_response(responses: &mut mpsc::Receiver<OrderResponse>) -> OrderResponse {
    timeout(Duration::from_secs(5), responses.recv())
        .await
        .expect("timed out waiting for an order response")
        .expect("the sender dropped its response sender")
}

/// Wait for both streams of every portfolio to seed (2·N events, order unspecified);
/// panics if anything else arrives first — nothing live precedes the seeds.
async fn await_seeds(events: &mut mpsc::Receiver<GatewayEvent>, expected: &[&str]) {
    let mut pending: Vec<(String, &'static str)> = expected
        .iter()
        .flat_map(|p| [(p.to_string(), "orders"), (p.to_string(), "trades")])
        .collect();
    while !pending.is_empty() {
        let (portfolio, stream) = match next_event(events).await {
            GatewayEvent::OwnOrdersSeeded { portfolio } => (portfolio, "orders"),
            GatewayEvent::OwnTradesSeeded { portfolio } => (portfolio, "trades"),
            other => panic!("expected only seed markers before live events: {other:?}"),
        };
        let index = pending
            .iter()
            .position(|(p, s)| *p == portfolio && *s == stream)
            .unwrap_or_else(|| panic!("unexpected duplicate seed: {portfolio} {stream}"));
        pending.remove(index);
    }
}

// ---------------------------------------------------------------------------------------------
// Wire helpers
// ---------------------------------------------------------------------------------------------

fn contract() -> ContractKey {
    ContractKey {
        market_type: MarketType::Local,
        contract_id: CONTRACT,
    }
}

fn own_order(order_id: &str, action: wire::OrderAction) -> wire::OwnOrder {
    wire::OwnOrder {
        order_id: order_id.to_owned(),
        revision_no: 1,
        state: wire::OrderState::Acti as i32,
        action: action as i32,
        side: wire::OrderSide::Sell as i32,
        contract_id: CONTRACT,
        market_type: wire::MarketType::Local as i32,
        delivery_area_eic: EIC.to_owned(),
        portfolio_id: "execution-a".to_owned(),
        price: 1_990,
        quantity: 10_000,
        cl_order_id: String::new(),
        api_timestamp_nanos: 3_000,
        errors: vec![],
    }
}

fn trade(portfolio: &str, trade_id: &str, side: wire::OrderSide, quantity: i64) -> wire::Trade {
    wire::Trade {
        trade_id: trade_id.to_owned(),
        state: wire::TradeState::Acti as i32,
        exchange: EXCHANGE.to_owned(),
        delivery_area_eic: EIC.to_owned(),
        api_timestamp_nanos: 2_000,
        exec_time_nanos: 1_000,
        side: side as i32,
        order_id: "O-1".to_owned(),
        portfolio_id: portfolio.to_owned(),
        contract_id: CONTRACT,
        market_type: wire::MarketType::Local as i32,
        contract_name: "PH-7".to_owned(),
        price: 1_990,
        quantity,
        self_trade: false,
        strategy_id: String::new(),
        unit_id: String::new(),
        cl_order_id: "id-executor:test".to_owned(),
    }
}

fn trades_update(seq: u64, trades: Vec<wire::Trade>) -> wire::OwnTradesUpdate {
    wire::OwnTradesUpdate {
        received_at_nanos: 123,
        seq,
        trades,
    }
}

fn orders_update(seq: u64, orders: Vec<wire::OwnOrder>) -> wire::OwnOrdersUpdate {
    wire::OwnOrdersUpdate {
        received_at_nanos: 123,
        seq,
        orders,
    }
}

fn order_request() -> OrderRequest {
    OrderRequest {
        portfolio: "execution-a".to_owned(),
        contract: contract(),
        side: Side::Sell,
        quantity: 10_000,
        price: 1_990,
        cl_order_id: "id-executor:test:execution-a".to_owned(),
    }
}

// ---------------------------------------------------------------------------------------------
// Order-path tests
// ---------------------------------------------------------------------------------------------

#[tokio::test]
async fn an_order_round_trips_under_the_right_portfolio_id_and_the_response_is_an_event() {
    // Given a gateway that will accept with venue order id O-9
    let mock = start_mock(&["execution-a"], &[]).await;
    mock.script
        .lock()
        .unwrap()
        .push_back(Ok(wire::AddOrderResponse {
            order: Some(own_order("O-9", wire::OrderAction::Uadd)),
        }));
    let (sender, mut responses) = order_sender(&mock.endpoint);

    // When the executor places a sell
    sender.send(order_request());

    // Then the advisory outcome arrives on the sender's channel, keyed by the pair, with the id
    assert_eq!(
        next_response(&mut responses).await,
        OrderResponse {
            portfolio: "execution-a".to_owned(),
            contract: contract(),
            cl_order_id: "id-executor:test:execution-a".to_owned(),
            outcome: OrderOutcome::Accepted {
                order_id: "O-9".to_owned()
            },
        }
    );

    // And the wire request was stamped with the portfolio id and the shared identity
    let requests = mock.requests.lock().unwrap();
    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].exchange, EXCHANGE);
    let order = requests[0].order.as_ref().unwrap();
    assert_eq!(order.portfolio_id, "execution-a");
    assert_eq!(order.delivery_area_eic, EIC);
    assert_eq!(order.side(), wire::OrderSide::Sell);
    assert_eq!(order.quantity, 10_000);
    assert_eq!(order.price, 1_990);
    assert_eq!(order.contract_id, CONTRACT);
    assert_eq!(order.market_type(), wire::MarketType::Local);
    assert_eq!(order.cl_order_id, "id-executor:test:execution-a");
}

#[tokio::test]
async fn a_gateway_rejection_is_a_distinct_event() {
    // Given a gateway whose gates refuse the first order and whose PowerBot refuses the
    // second via response errors
    let mock = start_mock(&["execution-a"], &[]).await;
    {
        let mut script = mock.script.lock().unwrap();
        script.push_back(Err(Status::failed_precondition("contract not tradable")));
        let mut refused = own_order("O-9", wire::OrderAction::Uadd);
        refused.errors.push(wire::OrderError {
            code: "E42".to_owned(),
            message: "limit exceeded".to_owned(),
        });
        script.push_back(Ok(wire::AddOrderResponse {
            order: Some(refused),
        }));
    }
    let (sender, mut responses) = order_sender(&mock.endpoint);

    // When two orders go out
    sender.send(order_request());
    let first = next_response(&mut responses).await;
    sender.send(order_request());
    let second = next_response(&mut responses).await;

    // Then both come back as definitive rejections (feeding ORDER_REJECTED), not failures
    for (response, needle) in [(first, "tradable"), (second, "E42")] {
        let OrderResponse {
            outcome: OrderOutcome::Rejected { reason },
            ..
        } = response
        else {
            panic!("expected a rejection, got {response:?}")
        };
        assert!(reason.contains(needle), "{reason}");
    }
}

#[tokio::test]
async fn a_transport_error_on_add_order_leaves_resolution_to_the_feeds() {
    // Given an endpoint nothing listens on (the port was bound once, then released)
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    drop(listener);
    let (sender, mut responses) = order_sender(&endpoint);

    // When an order goes out
    sender.send(order_request());

    // Then the outcome is ambiguous — Failed, never Rejected: the order may have reached
    // the venue in the general case, so the feeds (and the resolve timeout) own the truth
    let OrderResponse {
        outcome: OrderOutcome::Failed { .. },
        ..
    } = next_response(&mut responses).await
    else {
        panic!("expected an ambiguous failure")
    };
}

// ---------------------------------------------------------------------------------------------
// Subscription tests
// ---------------------------------------------------------------------------------------------

#[tokio::test]
async fn a_terminal_own_order_revision_resolves_the_right_portfolio() {
    // Given both portfolios' streams seeded
    let mock = start_mock(&["execution-a", "execution-b"], &[]).await;
    let (mut events, _fills) = start_feeds(&mock.endpoint, &["execution-a", "execution-b"]);
    await_seeds(&mut events, &["execution-a", "execution-b"]).await;

    // When A's stream shows a full execution and B's a partial still working
    mock.orders_tx
        .send((
            "execution-a".to_owned(),
            orders_update(2, vec![own_order("O-1", wire::OrderAction::Fexe)]),
        ))
        .unwrap();
    let GatewayEvent::OrderRevision(terminal) = next_event(&mut events).await else {
        panic!("expected A's revision")
    };
    let mut partial_order = own_order("O-2", wire::OrderAction::Pexe);
    partial_order.portfolio_id = "execution-b".to_owned();
    mock.orders_tx
        .send((
            "execution-b".to_owned(),
            orders_update(2, vec![partial_order]),
        ))
        .unwrap();
    let GatewayEvent::OrderRevision(partial) = next_event(&mut events).await else {
        panic!("expected B's revision")
    };

    // Then each resolves under its own portfolio, terminal-classified per invariant 5
    assert_eq!(terminal.portfolio, "execution-a");
    assert_eq!(terminal.order_id, "O-1");
    assert!(terminal.terminal);
    assert_eq!(terminal.action, "FEXE");
    assert_eq!(partial.portfolio, "execution-b");
    assert_eq!(partial.order_id, "O-2");
    assert!(!partial.terminal);
}

#[tokio::test]
async fn fills_accrue_signed_to_the_right_portfolio_and_dedupe_on_replay() {
    // Given both portfolios' streams seeded
    let mock = start_mock(&["execution-a", "execution-b"], &[]).await;
    let (mut events, mut fills) = start_feeds(&mock.endpoint, &["execution-a", "execution-b"]);
    await_seeds(&mut events, &["execution-a", "execution-b"]).await;

    // When A buys 5 MW and B sells 3 MW (independent streams — arrival order is theirs)
    mock.trades_tx
        .send((
            "execution-a".to_owned(),
            trades_update(
                2,
                vec![trade("execution-a", "T-1", wire::OrderSide::Buy, 5_000)],
            ),
        ))
        .unwrap();
    mock.trades_tx
        .send((
            "execution-b".to_owned(),
            trades_update(
                2,
                vec![trade("execution-b", "T-2", wire::OrderSide::Sell, 3_000)],
            ),
        ))
        .unwrap();
    let mut collected = Vec::new();
    for _ in 0..2 {
        collected.push(next_fill(&mut fills).await);
    }
    collected.sort_by(|a, b| a.portfolio.cmp(&b.portfolio));

    // Then each lands signed under its own portfolio — no cross-attribution
    assert_eq!(collected[0].portfolio, "execution-a");
    assert_eq!(collected[0].quantity, 5_000);
    assert_eq!(collected[0].venue_trade_id, "T-1");
    assert_eq!(collected[1].portfolio, "execution-b");
    assert_eq!(collected[1].quantity, -3_000);
    assert_eq!(collected[1].venue_trade_id, "T-2");

    // And when T-1 replays (a reconnect overlap), only the fresh marker fill follows it
    mock.trades_tx
        .send((
            "execution-a".to_owned(),
            trades_update(
                3,
                vec![trade("execution-a", "T-1", wire::OrderSide::Buy, 5_000)],
            ),
        ))
        .unwrap();
    mock.trades_tx
        .send((
            "execution-a".to_owned(),
            trades_update(
                4,
                vec![trade("execution-a", "T-3", wire::OrderSide::Buy, 1_000)],
            ),
        ))
        .unwrap();
    let fill = next_fill(&mut fills).await;
    assert_eq!(fill.venue_trade_id, "T-3");
}

#[tokio::test]
async fn the_seed_contributes_no_fill_state() {
    // Given a seed already carrying T-0 (a fill from before this executor booted — the
    // Position Service's to have folded already)
    let mock = start_mock(
        &["execution-a"],
        &[(
            "execution-a",
            vec![trade("execution-a", "T-0", wire::OrderSide::Buy, 5_000)],
        )],
    )
    .await;
    let (mut events, mut fills) = start_feeds(&mock.endpoint, &["execution-a"]);

    // When the feed seeds, only the markers arrive — never a Fill for T-0
    await_seeds(&mut events, &["execution-a"]).await;

    // And a delta replaying T-0 next to a fresh fill emits only the fresh one: the seed
    // was folded as a dedupe baseline
    mock.trades_tx
        .send((
            "execution-a".to_owned(),
            trades_update(
                2,
                vec![
                    trade("execution-a", "T-0", wire::OrderSide::Buy, 5_000),
                    trade("execution-a", "T-1", wire::OrderSide::Sell, 2_000),
                ],
            ),
        ))
        .unwrap();
    let fill = next_fill(&mut fills).await;
    assert_eq!(fill.venue_trade_id, "T-1");
    assert_eq!(fill.quantity, -2_000);
}

#[tokio::test]
async fn a_foreign_portfolio_id_is_ignored_for_state() {
    // Given A's seeded stream
    let mock = start_mock(&["execution-a"], &[]).await;
    let (mut events, mut fills) = start_feeds(&mock.endpoint, &["execution-a"]);
    await_seeds(&mut events, &["execution-a"]).await;

    // When a gateway bug delivers a foreign portfolio's fill on A's scoped stream,
    // followed by a valid marker fill
    mock.trades_tx
        .send((
            "execution-a".to_owned(),
            trades_update(2, vec![trade("PB-X", "T-1", wire::OrderSide::Buy, 5_000)]),
        ))
        .unwrap();
    mock.trades_tx
        .send((
            "execution-a".to_owned(),
            trades_update(
                3,
                vec![trade("execution-a", "T-2", wire::OrderSide::Buy, 1_000)],
            ),
        ))
        .unwrap();

    // Then the foreign fill is dropped (warned, not folded) and only the marker arrives
    let fill = next_fill(&mut fills).await;
    assert_eq!(fill.venue_trade_id, "T-2");
    assert_eq!(fill.portfolio, "execution-a");
}

#[tokio::test]
async fn an_unserved_portfolio_fails_the_boot_instead_of_retrying() {
    // Given a gateway serving only execution-a
    let mock = start_mock(&["execution-a"], &[]).await;

    // When the executor's configured set claims execution-b too, run_subscriptions returns
    // fatal error promptly — a permanent config mismatch, not a reconnect loop
    let error = timeout(
        Duration::from_secs(5),
        gateway::run_subscriptions(
            mock.endpoint.clone(),
            portfolios(&["execution-a", "execution-b"]),
            fast_tuning(),
            mpsc::channel(64).0,
            mpsc::channel(64).0,
        ),
    )
    .await
    .expect("an unserved portfolio must fail fast, not retry");
    assert!(error.to_string().contains("execution-b"), "{error}");
}
