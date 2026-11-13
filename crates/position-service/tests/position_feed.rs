//! Black-box tests of the gateway feed clients against in-process mock gateways: subscribe,
//! fold fills into the shared ledger through the seed/delta discipline, and broadcast the
//! changed positions. Memory-only (`storage: None`) — the write-through path is covered by
//! the position_storage tests.

use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use tokio::net::TcpListener;
use tokio::sync::{RwLock, broadcast};
use tokio_stream::wrappers::TcpListenerStream;
use tokio_stream::{Stream, StreamExt};
use tonic::{Request, Response, Status};

use position_service::channel::SharedLedger;
use position_service::config::{FeedConfig, GatewayConfig};
use position_service::feed;
use position_service::gateway;
use position_service::gateway::powerbot_execution_service_server::{
    PowerbotExecutionService, PowerbotExecutionServiceServer,
};
use position_service::ledger::{ContractKey, MarketType, PositionLedger};

/// A mock gateway replaying fixed update sequences, then holding each stream open like the
/// real one. Reconnects replay from the top (the re-seed).
#[derive(Default)]
struct MockGateway {
    own_updates: Vec<gateway::OwnTradesUpdate>,
}

type UpdateStream<T> = Pin<Box<dyn Stream<Item = Result<T, Status>> + Send>>;

fn replay<T: Clone + Send + 'static>(updates: &[T]) -> UpdateStream<T> {
    let items: Vec<Result<T, Status>> = updates.iter().cloned().map(Ok).collect();
    Box::pin(tokio_stream::iter(items).chain(tokio_stream::pending()))
}

/// The PowerBot portfolio id every mock gateway serves — the required subscription scope
/// the real gateway validates; the mock rejects any other, like the real one, so these
/// tests also prove the feed client sends its configured scope.
const SERVED_PORTFOLIO: &str = "PB-OURS";

#[allow(clippy::result_large_err)] // tonic::Status is what the RPC signature returns anyway
fn check_scope(portfolio_id: &str) -> Result<(), Status> {
    if portfolio_id == SERVED_PORTFOLIO {
        Ok(())
    } else {
        Err(Status::invalid_argument(format!(
            "portfolio_id {portfolio_id:?} is not served by this gateway"
        )))
    }
}

#[tonic::async_trait]
impl PowerbotExecutionService for MockGateway {
    type SubscribeOwnTradesStream = UpdateStream<gateway::OwnTradesUpdate>;
    type SubscribeOwnOrdersStream = UpdateStream<gateway::OwnOrdersUpdate>;

    async fn subscribe_own_trades(
        &self,
        request: Request<gateway::SubscribeOwnTradesRequest>,
    ) -> Result<Response<Self::SubscribeOwnTradesStream>, Status> {
        check_scope(&request.get_ref().portfolio_id)?;
        Ok(Response::new(replay(&self.own_updates)))
    }

    async fn subscribe_own_orders(
        &self,
        _request: Request<gateway::SubscribeOwnOrdersRequest>,
    ) -> Result<Response<Self::SubscribeOwnOrdersStream>, Status> {
        Err(Status::unimplemented("not used by the position service"))
    }
}

/// Serve `mock` on an ephemeral port, returning its endpoint.
async fn start_mock(mock: MockGateway) -> String {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    tokio::spawn(
        tonic::transport::Server::builder()
            .add_service(PowerbotExecutionServiceServer::new(mock))
            .serve_with_incoming(TcpListenerStream::new(listener)),
    );
    endpoint
}

fn gateway_config(name: &str, endpoint: &str, portfolio: &str) -> GatewayConfig {
    GatewayConfig {
        name: name.to_owned(),
        endpoint: endpoint.to_owned(),
        portfolio: portfolio.to_owned(),
        powerbot_portfolio_id: SERVED_PORTFOLIO.to_owned(),
    }
}

fn wire_trade(trade_id: &str, side: gateway::OrderSide, quantity: i64) -> gateway::Trade {
    gateway::Trade {
        trade_id: trade_id.to_owned(),
        state: gateway::TradeState::Acti as i32,
        exchange: "nordpool".to_owned(),
        delivery_area_eic: "10YFI-1--------U".to_owned(),
        api_timestamp_nanos: 2_000,
        exec_time_nanos: 1_000,
        side: side as i32,
        order_id: "O-1".to_owned(),
        portfolio_id: "PB-OURS".to_owned(),
        contract_id: 7,
        market_type: gateway::MarketType::Local as i32,
        contract_name: "PH-7".to_owned(),
        price: 4_250,
        quantity,
        self_trade: false,
        strategy_id: String::new(),
        unit_id: String::new(),
        cl_order_id: String::new(),
    }
}

fn own_update(seq: u64, trades: Vec<gateway::Trade>) -> gateway::OwnTradesUpdate {
    gateway::OwnTradesUpdate {
        received_at_nanos: 42,
        seq,
        trades,
    }
}

const CONTRACT: ContractKey = ContractKey {
    market_type: MarketType::Local,
    contract_id: 7,
};

/// Poll the ledger until `condition` holds, panicking after five seconds.
async fn wait_for(ledger: &SharedLedger, what: &str, condition: impl Fn(&PositionLedger) -> bool) {
    let reached = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if condition(&*ledger.read().await) {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await;
    assert!(reached.is_ok(), "timed out waiting for: {what}");
}

#[tokio::test]
async fn two_gateways_fold_into_their_configured_portfolios() {
    // Given two mock gateways, each with one seed fill on the same contract
    let endpoint_a = start_mock(MockGateway {
        own_updates: vec![own_update(
            1,
            vec![wire_trade("T-A", gateway::OrderSide::Buy, 5_000)],
        )],
    })
    .await;
    let endpoint_b = start_mock(MockGateway {
        own_updates: vec![own_update(
            1,
            vec![wire_trade("T-B", gateway::OrderSide::Sell, 3_000)],
        )],
    })
    .await;

    // When one own-trades feed runs per gateway, mapped to different portfolios
    let ledger: SharedLedger = Arc::new(RwLock::new(PositionLedger::new()));
    let (updates, mut changes) = broadcast::channel(16);
    for (name, endpoint, portfolio) in [
        ("nordpool-a", &endpoint_a, "execution-a"),
        ("nordpool-b", &endpoint_b, "execution-b"),
    ] {
        tokio::spawn(feed::run(
            gateway_config(name, endpoint, portfolio),
            FeedConfig::default(),
            None,
            ledger.clone(),
            updates.clone(),
        ));
    }

    // Then each gateway's fill folds into its configured portfolio
    wait_for(&ledger, "both seeds folded", |l: &PositionLedger| {
        l.len() == 2
    })
    .await;
    let ledger_read = ledger.read().await;
    assert_eq!(
        ledger_read
            .get("execution-a", &CONTRACT)
            .unwrap()
            .net_quantity,
        5_000
    );
    assert_eq!(
        ledger_read
            .get("execution-b", &CONTRACT)
            .unwrap()
            .net_quantity,
        -3_000
    );
    drop(ledger_read);

    // And an API subscriber saw both position updates (order between gateways is free),
    // each attributed to the fill that caused it — the venue trade id, no transfer id, and
    // the folding gateway as source
    let mut seen = Vec::new();
    for _ in 0..2 {
        let batch = tokio::time::timeout(Duration::from_secs(5), changes.recv())
            .await
            .expect("no position update was broadcast in time")
            .unwrap();
        for change in batch.changes {
            assert!(change.transfer_id.is_none());
            let venue_trade_id = change.venue_trade_id.expect("fill attribution missing");
            seen.push((change.position.portfolio, venue_trade_id, change.source));
        }
    }
    seen.sort();
    assert_eq!(
        seen,
        [
            (
                "execution-a".to_owned(),
                "T-A".to_owned(),
                "nordpool-a".to_owned()
            ),
            (
                "execution-b".to_owned(),
                "T-B".to_owned(),
                "nordpool-b".to_owned()
            ),
        ]
    );
}

#[tokio::test]
async fn seed_overlap_is_dropped_and_redelivered_fills_dedupe() {
    // Given a gateway whose stream replays: a seed at seq 10, a stale frame still at seq 10
    // carrying a trade the seed never contained (the seed/delta overlap — must be dropped
    // by seq, not folded), and a delta at seq 11 re-delivering the seed's fill (must dedupe
    // by id) alongside one genuinely new fill
    let endpoint = start_mock(MockGateway {
        own_updates: vec![
            own_update(10, vec![wire_trade("T-1", gateway::OrderSide::Buy, 5_000)]),
            own_update(10, vec![wire_trade("T-2", gateway::OrderSide::Buy, 50_000)]),
            own_update(
                11,
                vec![
                    wire_trade("T-1", gateway::OrderSide::Buy, 5_000),
                    wire_trade("T-3", gateway::OrderSide::Buy, 1_000),
                ],
            ),
        ],
    })
    .await;

    // When the feed folds it
    let ledger: SharedLedger = Arc::new(RwLock::new(PositionLedger::new()));
    let (updates, _keep_open) = broadcast::channel(16);
    tokio::spawn(feed::run(
        gateway_config("nordpool-a", &endpoint, "execution-a"),
        FeedConfig::default(),
        None,
        ledger.clone(),
        updates,
    ));

    // Then the position is exactly seed + new delta: the overlap frame's trade never
    // folded (no 50 MW), and the re-delivered T-1 folded once (revision 2, not 3)
    wait_for(&ledger, "seed and delta folded", |l: &PositionLedger| {
        l.get("execution-a", &CONTRACT)
            .is_some_and(|p| p.revision >= 2)
    })
    .await;
    let ledger_read = ledger.read().await;
    let position = ledger_read.get("execution-a", &CONTRACT).unwrap();
    assert_eq!(position.net_quantity, 6_000);
    assert_eq!(position.revision, 2);
    assert_eq!(ledger_read.applied_trades(), 2);
}

#[tokio::test]
async fn one_gateway_failing_does_not_stall_the_other() {
    // Given one gateway that is down (nothing listens on its endpoint) and one that works
    let dead = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let dead_endpoint = format!("http://{}", dead.local_addr().unwrap());
    drop(dead); // connection refused from here on
    let live_endpoint = start_mock(MockGateway {
        own_updates: vec![own_update(
            1,
            vec![wire_trade("T-B", gateway::OrderSide::Buy, 2_000)],
        )],
    })
    .await;

    // When both feeds run
    let ledger: SharedLedger = Arc::new(RwLock::new(PositionLedger::new()));
    let (updates, _keep_open) = broadcast::channel(16);
    for (name, endpoint, portfolio) in [
        ("nordpool-dead", &dead_endpoint, "execution-a"),
        ("nordpool-live", &live_endpoint, "execution-b"),
    ] {
        tokio::spawn(feed::run(
            gateway_config(name, endpoint, portfolio),
            FeedConfig::default(),
            None,
            ledger.clone(),
            updates.clone(),
        ));
    }

    // Then the live gateway's fill folds while the dead one keeps reconnecting
    wait_for(
        &ledger,
        "the live gateway's fill folded",
        |l: &PositionLedger| l.get("execution-b", &CONTRACT).is_some(),
    )
    .await;
    let ledger_read = ledger.read().await;
    assert_eq!(
        ledger_read
            .get("execution-b", &CONTRACT)
            .unwrap()
            .net_quantity,
        2_000
    );
    assert!(ledger_read.get("execution-a", &CONTRACT).is_none());
}
