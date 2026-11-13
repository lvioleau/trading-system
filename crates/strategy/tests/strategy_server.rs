//! Black-box tests of the mock strategy's gRPC API through a real client over a local socket: the
//! `SubscribeTargets` scoping (only the requested portfolios stream) and rejection (a portfolio
//! the strategy does not produce). Records are published directly to the broadcast — the driver's
//! target generation is covered by the driver's own tests.

use std::collections::HashSet;
use std::time::Duration;

use tokio::net::TcpListener;
use tokio::sync::{broadcast, watch};
use tokio_stream::wrappers::TcpListenerStream;

use strategy::driver::{MarketType, RecordSender, TargetRecord};
use strategy::proto;
use strategy::proto::strategy_service_client::StrategyServiceClient;
use strategy::server;

/// Start the server on an ephemeral port, returning a connected client, the shutdown handle
/// (dropping it drains the server, so tests keep it alive), and the record sender the test
/// publishes to.
async fn start_server(
    portfolios: &[&str],
) -> (
    StrategyServiceClient<tonic::transport::Channel>,
    watch::Sender<bool>,
    RecordSender,
) {
    let (records, _keep_open) = broadcast::channel(64);
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    let (shutdown, shutdown_rx) = watch::channel(false);
    let set: HashSet<String> = portfolios.iter().map(|p| p.to_string()).collect();
    let router = server::router(set, records.clone(), shutdown_rx)
        .await
        .unwrap();
    tokio::spawn(router.serve_with_incoming(TcpListenerStream::new(listener)));
    let client = StrategyServiceClient::connect(endpoint).await.unwrap();
    (client, shutdown, records)
}

fn request(strategy: &str, portfolios: &[&str]) -> proto::SubscribeTargetsRequest {
    proto::SubscribeTargetsRequest {
        strategy: strategy.to_owned(),
        portfolios: portfolios.iter().map(|p| p.to_string()).collect(),
    }
}

fn record(portfolio: &str, contract_id: i64, target: i64) -> TargetRecord {
    TargetRecord {
        portfolio: portfolio.to_owned(),
        market_type: MarketType::Local,
        contract_id,
        target_quantity: target,
        current_quantity: 0,
        delta_quantity: target,
        mid_price: 4_200,
    }
}

async fn next(stream: &mut tonic::Streaming<proto::TargetRecord>) -> proto::TargetRecord {
    tokio::time::timeout(Duration::from_secs(5), stream.message())
        .await
        .expect("no record arrived in time")
        .unwrap()
        .expect("stream ended unexpectedly")
}

#[tokio::test]
async fn subscription_streams_only_the_requested_portfolios() {
    // Given a strategy producing two portfolios, and a client subscribed to just one
    let (mut client, _shutdown, records) = start_server(&["p1", "p2"]).await;
    let mut stream = client
        .subscribe_targets(request("strategy-x", &["p1"]))
        .await
        .unwrap()
        .into_inner();

    // When records for both portfolios are published, interleaved
    records.send(record("p1", 7, -10_000)).unwrap();
    records.send(record("p2", 8, 5_000)).unwrap(); // filtered out
    records.send(record("p1", 9, -9_000)).unwrap();

    // Then the subscriber sees only p1's records, in order — the p2 record is suppressed
    let first = next(&mut stream).await;
    assert_eq!(first.portfolio, "p1");
    assert_eq!(first.contract_id, 7);
    let second = next(&mut stream).await;
    assert_eq!(second.portfolio, "p1");
    assert_eq!(second.contract_id, 9);
}

#[tokio::test]
async fn subscribing_to_an_unproduced_portfolio_is_rejected() {
    // Given a strategy producing only p1
    let (mut client, _shutdown, _records) = start_server(&["p1"]).await;

    // When a subscription names a portfolio the strategy does not produce, the whole request is
    // rejected (the crosser treats this as a fatal boot error)
    let status = client
        .subscribe_targets(request("strategy-x", &["p1", "nope"]))
        .await
        .unwrap_err();
    assert_eq!(status.code(), tonic::Code::InvalidArgument);

    // And an empty portfolio list is rejected too
    let status = client
        .subscribe_targets(request("strategy-x", &[]))
        .await
        .unwrap_err();
    assert_eq!(status.code(), tonic::Code::InvalidArgument);
}
