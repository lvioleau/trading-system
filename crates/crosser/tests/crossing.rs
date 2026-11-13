//! Black-box tests of the whole crossing engine (`crossing::run` — exactly what production
//! boots) against its three upstreams over local sockets: the real `crates/strategy` mock (the
//! target feed), a mock connector (the depth feed), and the Position Service — the *real*
//! position-service server run memory-only for the end-to-end reconciliation semantics, and a
//! scripted recording mock for the wire-level assertions the real server cannot expose (the
//! priced request, the OCC guard, id freshness across an abort).
//!
//! The tests re-send a target while awaiting its effect, mirroring the strategy's heartbeat:
//! reconciliation is idempotent by construction (a re-sent target at target is a no-op), so
//! driving the loop this way never over-crosses — several tests assert exact ledger trade
//! counts on top of the resulting positions to prove it.

// Mocking a tonic service means returning `tonic::Status` in `Result` throughout; the large Err
// variant is tonic's, and it only ever goes back over the wire.
#![expect(clippy::result_large_err)]

use std::collections::HashSet;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::net::TcpListener;
use tokio::sync::{RwLock, broadcast, watch};
use tokio::time::timeout;
use tokio_stream::wrappers::{BroadcastStream, TcpListenerStream};
use tokio_stream::{Stream, StreamExt};
use tonic::{Request, Response, Status};
use uuid::Uuid;

use crosser::config::{Config, FeedConfig, PortfolioConfig};
use crosser::crossing;
use crosser::marketdata;
use crosser::marketdata::depth_book_service_server::{DepthBookService, DepthBookServiceServer};
use eic::EicCode;
use position_service::channel::{PositionSender, SharedLedger};
use position_service::ledger::{
    ContractKey as LedgerContractKey, MarketType as LedgerMarketType, PositionLedger, Trade,
};
use position_service::proto as ps;
use position_service::proto::position_service_client::PositionServiceClient;
use position_service::proto::position_service_server::{PositionService, PositionServiceServer};
use position_service::server as ps_server;
use strategy::driver::{MarketType as MockMarketType, RecordSender, TargetRecord as MockRecord};
use strategy::server as strategy_server;

const AREA: EicCode = EicCode::AREA_FR;
const CONTRACT: i64 = 7;

// ---------------------------------------------------------------------------------------------
// Upstream harness
// ---------------------------------------------------------------------------------------------

/// A crosser config pointed at the three upstreams, with fast reconnect tuning.
fn config(
    strategy_endpoint: &str,
    position_endpoint: &str,
    connector_endpoint: &str,
    portfolios: &[(&str, &str)],
) -> Config {
    Config {
        strategy_endpoint: strategy_endpoint.to_owned(),
        strategy: "strategy-x".to_owned(),
        position_endpoint: position_endpoint.to_owned(),
        connector_endpoint: connector_endpoint.to_owned(),
        delivery_area_eic: AREA,
        portfolios: portfolios
            .iter()
            .map(|(name, execution)| PortfolioConfig {
                name: (*name).to_owned(),
                execution: (*execution).to_owned(),
            })
            .collect(),
        listen_addr: "127.0.0.1:0".parse().unwrap(),
        feed: FeedConfig {
            initial_backoff: Duration::from_millis(20),
            max_backoff: Duration::from_millis(200),
            stable_session: Duration::from_secs(30),
            keepalive_interval: Duration::from_secs(30),
            keepalive_timeout: Duration::from_secs(20),
        },
    }
}

/// Start the mock strategy producing `portfolios`; returns its endpoint and the record sender
/// the tests publish targets (and heartbeats) through.
async fn start_strategy(portfolios: &[&str]) -> (String, RecordSender, watch::Sender<bool>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    let (records, _) = broadcast::channel(64);
    let (shutdown, shutdown_rx) = watch::channel(false);
    let set: HashSet<String> = portfolios.iter().map(|p| p.to_string()).collect();
    let router = strategy_server::router(set, records.clone(), shutdown_rx)
        .await
        .unwrap();
    tokio::spawn(async move {
        router
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await
            .unwrap();
    });
    (endpoint, records, shutdown)
}

/// A mock connector whose depth stream is driven live by the test — each update published on
/// the broadcast reaches every subscribed session (the first one a session sees is its seed).
struct MockConnector {
    served_area: EicCode,
    updates: broadcast::Sender<marketdata::DepthUpdate>,
}

#[tonic::async_trait]
impl DepthBookService for MockConnector {
    type SubscribeStream =
        Pin<Box<dyn Stream<Item = Result<marketdata::DepthUpdate, Status>> + Send>>;

    async fn subscribe(
        &self,
        request: Request<marketdata::SubscribeDepthRequest>,
    ) -> Result<Response<Self::SubscribeStream>, Status> {
        let requested = request.into_inner().eic_code;
        if requested != self.served_area.eic_code() {
            return Err(Status::not_found(format!(
                "this connector serves delivery area {}, not {requested}",
                self.served_area.eic_code()
            )));
        }
        let stream = BroadcastStream::new(self.updates.subscribe()).filter_map(|update| {
            // A lagged mock subscriber cannot happen at test volumes; skip rather than error.
            update.ok().map(Ok)
        });
        Ok(Response::new(Box::pin(stream)))
    }
}

/// Start the mock connector; returns its endpoint and the depth update publisher.
async fn start_connector() -> (String, broadcast::Sender<marketdata::DepthUpdate>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    let (updates, _) = broadcast::channel(64);
    let mock = MockConnector {
        served_area: AREA,
        updates: updates.clone(),
    };
    tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(DepthBookServiceServer::new(mock))
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await
            .unwrap();
    });
    (endpoint, updates)
}

/// Start the real position-service server, memory-only (`storage: None`), configured with
/// `portfolios`; returns its endpoint plus the shared ledger (pre-seeding and inspection) and
/// the broadcast sender (manual echoes, exactly as the service's own writers send them).
async fn start_positions(
    portfolios: &[&str],
) -> (String, SharedLedger, PositionSender, watch::Sender<bool>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    let ledger: SharedLedger = Arc::new(RwLock::new(PositionLedger::new()));
    let (updates, _) = broadcast::channel(64);
    let (shutdown, shutdown_rx) = watch::channel(false);
    let set: HashSet<String> = portfolios.iter().map(|p| p.to_string()).collect();
    let router = ps_server::router(set, ledger.clone(), updates.clone(), None, shutdown_rx)
        .await
        .unwrap();
    tokio::spawn(async move {
        router
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await
            .unwrap();
    });
    (endpoint, ledger, updates, shutdown)
}

/// One booted stack: the three upstreams plus the crosser running against them.
struct Stack {
    records: RecordSender,
    depth: broadcast::Sender<marketdata::DepthUpdate>,
    ledger: SharedLedger,
    #[expect(dead_code, reason = "keeps the broadcast channel open")]
    ps_updates: PositionSender,
    ps_endpoint: String,
    _strategy_shutdown: watch::Sender<bool>,
    _ps_shutdown: watch::Sender<bool>,
    _crosser: tokio::task::JoinHandle<anyhow::Error>,
}

/// Boot everything against the real position service: strategy portfolios and their mapped
/// execution portfolios all configured on the service.
async fn boot(portfolios: &[(&str, &str)]) -> Stack {
    boot_preseeded(portfolios, vec![]).await
}

/// Like [`boot`], with `fills` applied to the position service's ledger *before* the crosser
/// starts — pre-existing positions its baseline snapshot must carry (applying them after boot
/// would race the snapshot, and a direct ledger write sends no subscription echo).
async fn boot_preseeded(portfolios: &[(&str, &str)], fills: Vec<Trade>) -> Stack {
    let strategy_portfolios: Vec<&str> = portfolios.iter().map(|(name, _)| *name).collect();
    let mut ps_portfolios = strategy_portfolios.clone();
    ps_portfolios.extend(portfolios.iter().map(|(_, execution)| *execution));
    let (strategy_endpoint, records, _strategy_shutdown) =
        start_strategy(&strategy_portfolios).await;
    let (connector_endpoint, depth) = start_connector().await;
    let (ps_endpoint, ledger, ps_updates, _ps_shutdown) = start_positions(&ps_portfolios).await;
    {
        let mut ledger = ledger.write().await;
        for fill in fills {
            ledger.apply(fill);
        }
    }
    let crosser = tokio::spawn(crossing::run(config(
        &strategy_endpoint,
        &ps_endpoint,
        &connector_endpoint,
        portfolios,
    )));
    let stack = Stack {
        records,
        depth,
        ledger,
        ps_updates,
        ps_endpoint,
        _strategy_shutdown,
        _ps_shutdown,
        _crosser: crosser,
    };
    // Wait until the crosser's feeds are attached: its strategy subscription joined the mock's
    // broadcast, and its depth subscription joined the connector's.
    await_subscribers(&stack.records, 1).await;
    timeout(Duration::from_secs(5), async {
        while stack.depth.receiver_count() == 0 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("the depth feed never subscribed");
    stack
}

async fn await_subscribers(records: &RecordSender, count: usize) {
    timeout(Duration::from_secs(5), async {
        while records.receiver_count() < count {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("the strategy feed never subscribed");
}

// ---------------------------------------------------------------------------------------------
// Wire and domain helpers
// ---------------------------------------------------------------------------------------------

/// A target record as the mock strategy books it. The `current`/`delta` it carries are its own
/// (often deliberately wrong in these tests): the crosser must size from `target` vs actual.
fn record(portfolio: &str, target: i64, current: i64) -> MockRecord {
    MockRecord {
        portfolio: portfolio.to_owned(),
        market_type: MockMarketType::Local,
        contract_id: CONTRACT,
        target_quantity: target,
        current_quantity: current,
        delta_quantity: target - current,
        // The strategy's own mid is deliberately absurd: pricing must come from the depth feed.
        mid_price: 999_999,
    }
}

fn level(price: i64) -> marketdata::Level {
    marketdata::Level {
        price,
        total_quantity: 5_000,
        order_count: 1,
        created_at_nanos: 1_000,
        updated_at_nanos: 1_000,
    }
}

/// One depth update carrying a single local book for [`CONTRACT`].
fn depth_update(bids: &[i64], asks: &[i64]) -> marketdata::DepthUpdate {
    marketdata::DepthUpdate {
        views: vec![marketdata::DepthView {
            contract_id: CONTRACT,
            market_type: marketdata::MarketType::Local as i32,
            revision: 1,
            bids: bids.iter().copied().map(level).collect(),
            asks: asks.iter().copied().map(level).collect(),
        }],
        received_at_nanos: 123_456,
        seq: 1,
    }
}

fn ledger_contract() -> LedgerContractKey {
    LedgerContractKey {
        market_type: LedgerMarketType::Local,
        contract_id: CONTRACT,
    }
}

/// A pre-seeded fill applied straight to the ledger (the position-server test pattern) — the
/// pre-existing position a restarted crosser must fold and reconcile against.
fn fill(id: u128, portfolio: &str, quantity: i64) -> Trade {
    Trade {
        id: Uuid::from_u128(id),
        portfolio: portfolio.to_owned(),
        contract: ledger_contract(),
        quantity,
        price: 4_250,
        executed_at_nanos: 1_000_000_000,
        venue_trade_id: Some(format!("VT-{id}")),
        transfer_id: None,
        cl_order_id: None,
        source: "test-feed".to_owned(),
    }
}

/// The current `(net_quantity, revision)` of a `(portfolio, CONTRACT)` position, if it exists.
async fn position(ledger: &SharedLedger, portfolio: &str) -> Option<(i64, u64)> {
    ledger
        .read()
        .await
        .get(portfolio, &ledger_contract())
        .map(|position| (position.net_quantity, position.revision))
}

/// Publish `target` as the strategy — re-sending on an interval, exactly like the heartbeat —
/// until the ledger shows `portfolio` at `expected` kW. Safe by design: a re-sent target is a
/// no-op once the position is reached.
async fn drive_to(stack: &Stack, target: MockRecord, portfolio: &str, expected: i64) {
    let result = timeout(Duration::from_secs(5), async {
        loop {
            // Send errs only while no subscriber is attached (e.g. mid-reconnect) — keep going.
            let _ = stack.records.send(target.clone());
            if position(&stack.ledger, portfolio).await.map(|(q, _)| q) == Some(expected) {
                return;
            }
            tokio::time::sleep(Duration::from_millis(30)).await;
        }
    })
    .await;
    assert!(
        result.is_ok(),
        "{portfolio} never reached {expected} kW: found {:?}",
        position(&stack.ledger, portfolio).await
    );
}

/// Total ledger trades (transfer legs count two each).
async fn applied_trades(ledger: &SharedLedger) -> usize {
    ledger.read().await.applied_trades()
}

// ---------------------------------------------------------------------------------------------
// End-to-end reconciliation against the real position service
// ---------------------------------------------------------------------------------------------

#[tokio::test]
async fn a_first_target_from_flat_crosses_the_gap_into_the_execution_portfolio() {
    // Given a booted stack with a clean book
    let stack = boot(&[("p1", "exec-a")]).await;
    stack.depth.send(depth_update(&[4_200], &[4_300])).unwrap();

    // When the strategy targets short 10 MW from flat
    drive_to(&stack, record("p1", -10_000, 0), "p1", -10_000).await;

    // Then one zero-sum transfer did both jobs: the strategy portfolio records the target and
    // the execution portfolio picks up the opposite gap for the executor to flatten
    assert_eq!(position(&stack.ledger, "p1").await, Some((-10_000, 1)));
    assert_eq!(position(&stack.ledger, "exec-a").await, Some((10_000, 1)));
    assert_eq!(applied_trades(&stack.ledger).await, 2);
}

#[tokio::test]
async fn a_revised_target_produces_the_reconcile_leg_sized_against_the_actual_position() {
    // Given a crosser that already crossed to the first target (the architecture proposal's
    // worked example: target −10, then revised to −9)
    let stack = boot(&[("p1", "exec-a")]).await;
    stack.depth.send(depth_update(&[4_200], &[4_300])).unwrap();
    drive_to(&stack, record("p1", -10_000, 0), "p1", -10_000).await;

    // When the strategy revises the target to −9 — with a deliberately wrong current/delta
    // (as after a dropped record: its own booking has drifted from what was crossed)
    drive_to(&stack, record("p1", -9_000, -55_000), "p1", -9_000).await;

    // Then the reconcile leg was sized against the ACTUAL position (+1 MW back), not the
    // message's current/delta, and the execution portfolio received the opposite
    assert_eq!(position(&stack.ledger, "p1").await, Some((-9_000, 2)));
    assert_eq!(position(&stack.ledger, "exec-a").await, Some((9_000, 2)));
    assert_eq!(applied_trades(&stack.ledger).await, 4);
}

#[tokio::test]
async fn targets_on_two_portfolios_cross_into_their_mapped_execution_portfolios() {
    // Given a crosser handling two strategy portfolios with distinct execution mappings
    let stack = boot(&[("p1", "exec-a"), ("p2", "exec-b")]).await;
    stack.depth.send(depth_update(&[4_200], &[4_300])).unwrap();

    // When each portfolio receives its own target
    drive_to(&stack, record("p1", -10_000, 0), "p1", -10_000).await;
    drive_to(&stack, record("p2", 5_000, 0), "p2", 5_000).await;

    // Then each cross landed in its own mapped execution portfolio
    assert_eq!(position(&stack.ledger, "exec-a").await, Some((10_000, 1)));
    assert_eq!(position(&stack.ledger, "exec-b").await, Some((-5_000, 1)));
}

#[tokio::test]
async fn a_target_already_met_is_a_no_op() {
    // Given a position service already holding p1 exactly at the target (pre-seeded before the
    // crosser boots — its snapshot is the baseline)
    let stack = boot_preseeded(&[("p1", "exec-a")], vec![fill(1, "p1", -10_000)]).await;
    stack.depth.send(depth_update(&[4_200], &[4_300])).unwrap();

    // When the strategy (re-)emits that same target
    let _ = stack.records.send(record("p1", -10_000, -10_000));
    tokio::time::sleep(Duration::from_millis(300)).await;

    // Then nothing was transferred: the pre-seeded fill is the only ledger trade and no
    // execution position was ever created
    assert_eq!(applied_trades(&stack.ledger).await, 1);
    assert_eq!(position(&stack.ledger, "p1").await, Some((-10_000, 1)));
    assert_eq!(position(&stack.ledger, "exec-a").await, None);
}

#[tokio::test]
async fn a_boot_with_pre_existing_positions_folds_them_and_reconciles_against_them() {
    // Given a position service where p1 is already short 10 MW (a previous crosser run)
    let stack = boot_preseeded(&[("p1", "exec-a")], vec![fill(1, "p1", -10_000)]).await;
    stack.depth.send(depth_update(&[4_200], &[4_300])).unwrap();

    // When the strategy targets −9 — restart safety: the baseline comes from the snapshot fold
    drive_to(&stack, record("p1", -9_000, -10_000), "p1", -9_000).await;

    // Then only the 1 MW difference was crossed
    assert_eq!(position(&stack.ledger, "p1").await, Some((-9_000, 2)));
    assert_eq!(position(&stack.ledger, "exec-a").await, Some((-1_000, 1)));
    assert_eq!(applied_trades(&stack.ledger).await, 3);
}

#[tokio::test]
async fn a_heartbeat_is_a_no_op_at_target_but_re_crosses_after_drift() {
    // Given a crosser at target
    let stack = boot(&[("p1", "exec-a")]).await;
    stack.depth.send(depth_update(&[4_200], &[4_300])).unwrap();
    drive_to(&stack, record("p1", -10_000, 0), "p1", -10_000).await;
    assert_eq!(applied_trades(&stack.ledger).await, 2);

    // When an unchanged heartbeat arrives (delta = 0), it is a pure no-op
    let heartbeat = record("p1", -10_000, -10_000);
    let _ = stack.records.send(heartbeat.clone());
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(applied_trades(&stack.ledger).await, 2);

    // And when a trader nudges the strategy portfolio off its target directly through the
    // Position Service API (the design's manual-intervention path)…
    let mut trader = PositionServiceClient::connect(stack.ps_endpoint.clone())
        .await
        .unwrap();
    trader
        .transfer(ps::TransferRequest {
            transfer_id: Uuid::from_u128(999).to_string(),
            from_portfolio: "p1".to_owned(),
            to_portfolio: "exec-a".to_owned(),
            market_type: ps::MarketType::Local as i32,
            contract_id: CONTRACT,
            quantity: 2_000,
            price: 4_250,
            source: "test-trader".to_owned(),
            executed_at_nanos: 2_000_000_000,
            expected_from_revision: None,
        })
        .await
        .unwrap();
    assert_eq!(position(&stack.ledger, "p1").await, Some((-12_000, 2)));

    // …then the same 0-delta heartbeat computes a non-zero gap against the drifted actual and
    // crosses the strategy portfolio back to its target
    drive_to(&stack, heartbeat, "p1", -10_000).await;
    assert_eq!(position(&stack.ledger, "p1").await, Some((-10_000, 3)));
}

#[tokio::test]
async fn an_unpriceable_book_withholds_the_cross_until_it_heals() {
    // Given a crossed book (best bid ≥ best ask — broken, never priced)
    let stack = boot(&[("p1", "exec-a")]).await;
    stack.depth.send(depth_update(&[4_300], &[4_200])).unwrap();

    // When a target arrives, the cross is withheld — no transfer, however long we wait
    let _ = stack.records.send(record("p1", -10_000, 0));
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(applied_trades(&stack.ledger).await, 0);

    // And once the book heals, the next heartbeat crosses the standing gap
    stack.depth.send(depth_update(&[4_200], &[4_300])).unwrap();
    drive_to(&stack, record("p1", -10_000, 0), "p1", -10_000).await;
    assert_eq!(position(&stack.ledger, "exec-a").await, Some((10_000, 1)));
}

// ---------------------------------------------------------------------------------------------
// Wire-level assertions against a scripted recording mock
// ---------------------------------------------------------------------------------------------

/// The `Transfer` behavior of a [`ScriptedPositions`] mock: a pure function of the request, so
/// tests stay deterministic however heartbeats interleave with echoes (a response *queue* would
/// pair responses to requests by arrival order, which the heartbeat-driven loop cannot promise).
type TransferScript =
    Box<dyn Fn(&ps::TransferRequest) -> Result<ps::TransferResponse, Status> + Send + Sync>;

/// A scripted `PositionService`: `Subscribe` seeds from a fixed snapshot then streams whatever
/// the test broadcasts; `Transfer` records every request verbatim and answers from the script.
/// What the real server cannot expose — the priced request on the wire, the OCC guard value,
/// id freshness across an abort — is asserted here.
struct ScriptedPositions {
    snapshot: Vec<ps::Position>,
    requests: Arc<Mutex<Vec<ps::TransferRequest>>>,
    respond: TransferScript,
    updates: broadcast::Sender<ps::PositionUpdate>,
}

#[tonic::async_trait]
impl PositionService for ScriptedPositions {
    type SubscribeStream = Pin<Box<dyn Stream<Item = Result<ps::PositionUpdate, Status>> + Send>>;

    async fn transfer(
        &self,
        request: Request<ps::TransferRequest>,
    ) -> Result<Response<ps::TransferResponse>, Status> {
        let request = request.into_inner();
        let response = (self.respond)(&request);
        self.requests.lock().unwrap().push(request);
        response.map(Response::new)
    }

    async fn subscribe(
        &self,
        request: Request<ps::SubscribeRequest>,
    ) -> Result<Response<Self::SubscribeStream>, Status> {
        let portfolio = request.into_inner().portfolio;
        fn for_portfolio(positions: &[ps::Position], portfolio: &str) -> Vec<ps::Position> {
            positions
                .iter()
                .filter(|position| position.portfolio == portfolio)
                .cloned()
                .collect()
        }
        let snapshot = ps::PositionUpdate {
            positions: for_portfolio(&self.snapshot, &portfolio),
        };
        let deltas = BroadcastStream::new(self.updates.subscribe()).filter_map(move |update| {
            let batch = for_portfolio(&update.ok()?.positions, &portfolio);
            (!batch.is_empty()).then_some(Ok(ps::PositionUpdate { positions: batch }))
        });
        Ok(Response::new(Box::pin(
            tokio_stream::once(Ok(snapshot)).chain(deltas),
        )))
    }
}

/// Everything a scripted-mock test drives and observes.
struct ScriptedStack {
    records: RecordSender,
    depth: broadcast::Sender<marketdata::DepthUpdate>,
    requests: Arc<Mutex<Vec<ps::TransferRequest>>>,
    updates: broadcast::Sender<ps::PositionUpdate>,
    _strategy_shutdown: watch::Sender<bool>,
    _crosser: tokio::task::JoinHandle<anyhow::Error>,
}

/// Boot the crosser (portfolio `p1` → `exec-a`) against the scripted position service.
async fn boot_scripted(
    snapshot: Vec<ps::Position>,
    respond: impl Fn(&ps::TransferRequest) -> Result<ps::TransferResponse, Status>
    + Send
    + Sync
    + 'static,
) -> ScriptedStack {
    let (strategy_endpoint, records, _strategy_shutdown) = start_strategy(&["p1"]).await;
    let (connector_endpoint, depth) = start_connector().await;

    let requests = Arc::new(Mutex::new(Vec::new()));
    let (updates, _) = broadcast::channel(64);
    let mock = ScriptedPositions {
        snapshot,
        requests: requests.clone(),
        respond: Box::new(respond),
        updates: updates.clone(),
    };
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let ps_endpoint = format!("http://{}", listener.local_addr().unwrap());
    tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(PositionServiceServer::new(mock))
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await
            .unwrap();
    });

    let crosser = tokio::spawn(crossing::run(config(
        &strategy_endpoint,
        &ps_endpoint,
        &connector_endpoint,
        &[("p1", "exec-a")],
    )));
    let stack = ScriptedStack {
        records,
        depth,
        requests,
        updates,
        _strategy_shutdown,
        _crosser: crosser,
    };
    await_subscribers(&stack.records, 1).await;
    timeout(Duration::from_secs(5), async {
        while stack.depth.receiver_count() == 0 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("the depth feed never subscribed");
    stack
}

fn wire_position(portfolio: &str, net_quantity: i64, revision: u64) -> ps::Position {
    ps::Position {
        portfolio: portfolio.to_owned(),
        market_type: ps::MarketType::Local as i32,
        contract_id: CONTRACT,
        net_quantity,
        revision,
        // The write-attribution fields are ignored by the crosser (it folds by revision,
        // not by cause).
        ..Default::default()
    }
}

fn ack(from: ps::Position, to: ps::Position) -> ps::TransferResponse {
    ps::TransferResponse {
        from_position: Some(from),
        to_position: Some(to),
    }
}

/// Re-send `target` (the heartbeat) until the mock has recorded `count` transfer requests.
async fn drive_to_requests(stack: &ScriptedStack, target: MockRecord, count: usize) {
    let result = timeout(Duration::from_secs(5), async {
        loop {
            let _ = stack.records.send(target.clone());
            if stack.requests.lock().unwrap().len() >= count {
                return;
            }
            tokio::time::sleep(Duration::from_millis(30)).await;
        }
    })
    .await;
    assert!(
        result.is_ok(),
        "never saw {count} transfer requests: got {:?}",
        stack.requests.lock().unwrap()
    );
}

#[tokio::test]
async fn the_transfer_is_priced_at_the_live_mid_and_guarded_at_the_read_revision() {
    // Given p1 seeded at −8 MW (revision 3) and a clean book whose mid is 4 250 — while the
    // strategy's own mid is deliberately absurd (999 999)
    let stack = boot_scripted(vec![wire_position("p1", -8_000, 3)], |_| {
        Ok(ack(
            wire_position("p1", -10_000, 4),
            wire_position("exec-a", 2_000, 1),
        ))
    })
    .await;
    stack.depth.send(depth_update(&[4_200], &[4_300])).unwrap();

    // When the strategy targets −10
    drive_to_requests(&stack, record("p1", -10_000, -8_000), 1).await;

    // Then the wire request carries the whole crossing contract: the strategy portfolio as
    // `from` (it holds the guard), the mapped execution portfolio as `to`, quantity = −gap,
    // the price from the depth feed's top-of-book mid (never the strategy's), the guard at the
    // revision the gap was read from, and a real UUID as idempotency key
    let request = stack.requests.lock().unwrap()[0].clone();
    assert_eq!(request.from_portfolio, "p1");
    assert_eq!(request.to_portfolio, "exec-a");
    assert_eq!(request.market_type, ps::MarketType::Local as i32);
    assert_eq!(request.contract_id, CONTRACT);
    assert_eq!(request.quantity, 2_000); // gap = −10 − (−8) = −2 MW ⇒ quantity +2 MW
    assert_eq!(request.price, 4_250);
    assert_eq!(request.expected_from_revision, Some(3));
    assert_eq!(request.source, "crosser:strategy-x");
    assert!(!Uuid::try_parse(&request.transfer_id).unwrap().is_nil());
}

#[tokio::test]
async fn an_occ_abort_abandons_the_cross_and_the_next_heartbeat_recomputes() {
    // Given a crosser seeded flat against a service whose position has actually moved to
    // +2 MW at revision 1 (a concurrent writer): the mock enforces real OCC semantics, so any
    // transfer guarded at a stale revision aborts and only a revision-1 guard succeeds
    let stack = boot_scripted(vec![], |request| {
        if request.expected_from_revision == Some(1) {
            Ok(ack(
                wire_position("p1", -10_000, 2),
                wire_position("exec-a", 12_000, 1),
            ))
        } else {
            Err(Status::aborted("expected revision 1"))
        }
    })
    .await;
    stack.depth.send(depth_update(&[4_200], &[4_300])).unwrap();

    // When the target arrives, the cross (sized from the stale flat baseline) aborts — and is
    // abandoned: no retry happens until new information arrives
    drive_to_requests(&stack, record("p1", -10_000, 0), 1).await;

    // …then the moved position arrives on the subscription and folds in
    stack
        .updates
        .send(ps::PositionUpdate {
            positions: vec![wire_position("p1", 2_000, 1)],
        })
        .unwrap();

    // Then the next heartbeat recomputes the gap against the refreshed actual (−10 − 2 =
    // −12 MW ⇒ quantity +12 MW), guarded at the refreshed revision, under a FRESH transfer id
    // — a recomputed gap is a new intent, never a replay of the aborted one
    let succeeded = timeout(Duration::from_secs(5), async {
        loop {
            let _ = stack.records.send(record("p1", -10_000, 0)); // the heartbeat
            let done = {
                let requests = stack.requests.lock().unwrap();
                requests
                    .iter()
                    .any(|request| request.expected_from_revision == Some(1))
            };
            if done {
                return;
            }
            tokio::time::sleep(Duration::from_millis(30)).await;
        }
    })
    .await;
    assert!(
        succeeded.is_ok(),
        "no heartbeat ever recomputed against the refreshed position: {:?}",
        stack.requests.lock().unwrap()
    );
    let requests = stack.requests.lock().unwrap().clone();
    // Every attempt before the refresh folded was sized from flat and guarded stale.
    assert_eq!(requests[0].quantity, 10_000);
    assert_eq!(requests[0].expected_from_revision, Some(0));
    // The recomputed intent is re-sized, re-guarded, and freshly identified.
    let recomputed = requests
        .iter()
        .find(|request| request.expected_from_revision == Some(1))
        .unwrap();
    assert_eq!(recomputed.quantity, 12_000);
    assert_ne!(requests[0].transfer_id, recomputed.transfer_id);
}
