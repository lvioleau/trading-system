//! Black-box tests of the crosser's strategy feed against the real `crates/strategy` mock over a
//! local socket: records flow for the requested portfolios, a rejected subscription fails
//! fatally (no retry loop), heartbeat re-emits pass through undeduped, a transport drop
//! reconnects, and a malformed record is skipped without killing the stream (that one driven by
//! a raw hand-rolled server, since the mock cannot emit malformed records).

use std::collections::HashSet;
use std::net::SocketAddr;
use std::pin::Pin;
use std::time::Duration;

use tokio::net::TcpListener;
use tokio::sync::{broadcast, mpsc, watch};
use tokio::time::timeout;
use tokio_stream::wrappers::TcpListenerStream;
use tokio_stream::{Stream, StreamExt};
use tonic::{Request, Response, Status};

use crosser::config::{Config, FeedConfig, PortfolioConfig};
use crosser::crosser::{ContractKey, MarketType, TargetRecord};
use crosser::strategy_feed;
use eic::EicCode;
use strategy::driver::{MarketType as MockMarketType, RecordSender, TargetRecord as MockRecord};
use strategy::proto as mock_proto;
use strategy::proto::strategy_service_server::{
    StrategyService as MockStrategyService, StrategyServiceServer,
};
use strategy::server;

/// A crosser config pointed at `endpoint`, handling `portfolios`, with fast reconnect tuning so
/// the tests exercise the backoff loop in milliseconds.
fn config(endpoint: &str, portfolios: &[&str]) -> Config {
    Config {
        strategy_endpoint: endpoint.to_owned(),
        strategy: "strategy-x".to_owned(),
        position_endpoint: "http://localhost:1".to_owned(),
        connector_endpoint: "http://localhost:1".to_owned(),
        delivery_area_eic: EicCode::AREA_FR,
        portfolios: portfolios
            .iter()
            .map(|p| PortfolioConfig {
                name: p.to_string(),
                execution: format!("exec-{p}"),
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

/// Start the mock strategy on `listener`, producing `portfolios`. Returns the record sender the
/// test publishes to, the shutdown handle (send `true` to drain), and the server task (await it
/// to know the listener is released).
async fn start_mock(
    listener: TcpListener,
    portfolios: &[&str],
) -> (
    RecordSender,
    watch::Sender<bool>,
    tokio::task::JoinHandle<()>,
) {
    let (records, _) = broadcast::channel(64);
    let (shutdown, shutdown_rx) = watch::channel(false);
    let set: HashSet<String> = portfolios.iter().map(|p| p.to_string()).collect();
    let router = server::router(set, records.clone(), shutdown_rx)
        .await
        .unwrap();
    let mut drain = shutdown.subscribe();
    let task = tokio::spawn(async move {
        router
            .serve_with_incoming_shutdown(TcpListenerStream::new(listener), async move {
                let _ = drain.changed().await;
            })
            .await
            .unwrap();
    });
    (records, shutdown, task)
}

/// Wait until the feed's subscription has joined the mock's broadcast — records published before
/// that are lost (no seed), so tests publish only once a subscriber is attached.
async fn await_subscriber(records: &RecordSender) {
    timeout(Duration::from_secs(5), async {
        while records.receiver_count() == 0 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("no subscription arrived in time");
}

fn mock_record(portfolio: &str, contract_id: i64, target: i64, current: i64) -> MockRecord {
    MockRecord {
        portfolio: portfolio.to_owned(),
        market_type: MockMarketType::Local,
        contract_id,
        target_quantity: target,
        current_quantity: current,
        delta_quantity: target - current,
        mid_price: 4_200,
    }
}

async fn next(received: &mut mpsc::Receiver<TargetRecord>) -> TargetRecord {
    timeout(Duration::from_secs(5), received.recv())
        .await
        .expect("no record arrived in time")
        .expect("target channel closed unexpectedly")
}

#[tokio::test]
async fn records_flow_through_for_the_requested_portfolios() {
    // Given a mock strategy producing p1 and p2, and a crosser feed configured for p1 only
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    let (records, _shutdown, _server) = start_mock(listener, &["p1", "p2"]).await;
    let (targets, mut received) = mpsc::channel(64);
    let _feed = tokio::spawn(strategy_feed::run(config(&endpoint, &["p1"]), targets));
    await_subscriber(&records).await;

    // When records for both portfolios are published, the unrequested one first
    records.send(mock_record("p2", 8, 5_000, 0)).unwrap();
    records.send(mock_record("p1", 7, -10_000, -9_500)).unwrap();

    // Then only p1's record arrives — the subscription is scoped server-side — translated
    // verbatim into the domain (integer units untouched, market type mapped)
    let record = next(&mut received).await;
    assert_eq!(
        record,
        TargetRecord {
            portfolio: "p1".to_owned(),
            contract: ContractKey {
                market_type: MarketType::Local,
                contract_id: 7,
            },
            target_quantity: -10_000,
            current_quantity: -9_500,
            delta_quantity: -500,
            mid_price: 4_200,
        }
    );
    // And nothing else follows — the p2 record was suppressed, not queued
    assert!(
        timeout(Duration::from_millis(200), received.recv())
            .await
            .is_err(),
        "received a record for an unrequested portfolio"
    );
}

#[tokio::test]
async fn an_unproduced_portfolio_fails_the_feed_fatally() {
    // Given a mock strategy producing only p1
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    let (_records, _shutdown, _server) = start_mock(listener, &["p1"]).await;
    let (targets, _received) = mpsc::channel(64);

    // When the crosser subscribes for a portfolio the strategy does not produce, run returns the
    // fatal error promptly — a permanent config mismatch, not a retry loop
    let error = timeout(
        Duration::from_secs(5),
        strategy_feed::run(config(&endpoint, &["p1", "nope"]), targets),
    )
    .await
    .expect("a rejected subscription must fail fast, not retry");
    assert!(error.to_string().contains("nope"), "{error}");
}

#[tokio::test]
async fn heartbeat_re_emits_pass_through_undeduped() {
    // Given a subscribed feed
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    let (records, _shutdown, _server) = start_mock(listener, &["p1"]).await;
    let (targets, mut received) = mpsc::channel(64);
    let _feed = tokio::spawn(strategy_feed::run(config(&endpoint, &["p1"]), targets));
    await_subscriber(&records).await;

    // When the strategy re-emits an unchanged target (the heartbeat: delta = 0)
    let heartbeat = mock_record("p1", 7, -10_000, -10_000);
    records.send(heartbeat.clone()).unwrap();
    records.send(heartbeat).unwrap();

    // Then both deliveries arrive — deduplication is not the feed's job: the crossing loop must
    // see every heartbeat to re-reconcile a drifted position
    let first = next(&mut received).await;
    let second = next(&mut received).await;
    assert_eq!(first, second);
    assert_eq!(first.delta_quantity, 0);
}

#[tokio::test]
async fn a_transport_drop_reconnects_and_resumes() {
    // Given a subscribed feed that has already received a record
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let endpoint = format!("http://{addr}");
    let (records, shutdown, server) = start_mock(listener, &["p1"]).await;
    let (targets, mut received) = mpsc::channel(64);
    let _feed = tokio::spawn(strategy_feed::run(config(&endpoint, &["p1"]), targets));
    await_subscriber(&records).await;
    records.send(mock_record("p1", 7, -10_000, 0)).unwrap();
    assert_eq!(next(&mut received).await.contract.contract_id, 7);

    // When the strategy goes away entirely (drained, listener released)…
    shutdown.send(true).unwrap();
    server.await.unwrap();
    // …and later comes back on the same address
    let listener = rebind(addr).await;
    let (records, _shutdown, _server) = start_mock(listener, &["p1"]).await;

    // Then the feed reconnects by itself and records flow again
    await_subscriber(&records).await;
    records.send(mock_record("p1", 9, 5_000, 0)).unwrap();
    assert_eq!(next(&mut received).await.contract.contract_id, 9);
}

/// Rebind `addr` after its previous listener shut down, retrying briefly — the OS may take a
/// moment to release the port.
async fn rebind(addr: SocketAddr) -> TcpListener {
    timeout(Duration::from_secs(5), async {
        loop {
            match TcpListener::bind(addr).await {
                Ok(listener) => return listener,
                Err(_) => tokio::time::sleep(Duration::from_millis(20)).await,
            }
        }
    })
    .await
    .expect("could not rebind the mock's address")
}

/// A raw `StrategyService` streaming a fixed script of wire records, then staying open. The mock
/// cannot emit malformed records (its domain types forbid them), so the malformed-record test
/// serves the wire directly.
struct RawStrategy {
    script: Vec<mock_proto::TargetRecord>,
}

#[tonic::async_trait]
impl MockStrategyService for RawStrategy {
    type SubscribeTargetsStream =
        Pin<Box<dyn Stream<Item = Result<mock_proto::TargetRecord, Status>> + Send>>;

    async fn subscribe_targets(
        &self,
        _request: Request<mock_proto::SubscribeTargetsRequest>,
    ) -> Result<Response<Self::SubscribeTargetsStream>, Status> {
        let stream = tokio_stream::iter(self.script.clone().into_iter().map(Ok))
            .chain(tokio_stream::pending());
        Ok(Response::new(Box::pin(stream)))
    }
}

#[tokio::test]
async fn a_malformed_record_is_skipped_without_killing_the_stream() {
    // Given a raw strategy whose stream opens with a malformed record (no market type — half the
    // contract identity), followed by a good one
    let malformed = mock_proto::TargetRecord {
        portfolio: "p1".to_owned(),
        market_type: mock_proto::MarketType::None as i32,
        contract_id: 7,
        target_quantity: -10_000,
        current_quantity: 0,
        delta_quantity: -10_000,
        mid_price: 4_200,
    };
    let good = mock_proto::TargetRecord {
        market_type: mock_proto::MarketType::Local as i32,
        contract_id: 8,
        ..malformed.clone()
    };
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    tokio::spawn(
        tonic::transport::Server::builder()
            .add_service(StrategyServiceServer::new(RawStrategy {
                script: vec![malformed, good],
            }))
            .serve_with_incoming(TcpListenerStream::new(listener)),
    );

    // When the feed consumes the stream
    let (targets, mut received) = mpsc::channel(64);
    let _feed = tokio::spawn(strategy_feed::run(config(&endpoint, &["p1"]), targets));

    // Then the malformed record is skipped and the good one still arrives — the session survived
    let record = next(&mut received).await;
    assert_eq!(record.contract.contract_id, 8);
}
