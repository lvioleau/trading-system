//! Black-box test of the service's own gRPC API through a real client over a local socket:
//! the `Transfer` mutation (validation, idempotent replay, the optimistic-concurrency
//! guard) and the snapshot-then-updates `Subscribe` stream. Positions are pre-seeded by
//! applying fills directly to the ledger — the same write-then-broadcast contract the
//! gateway feeds will honor from step 5 on.

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use tokio::net::TcpListener;
use tokio::sync::{RwLock, broadcast, watch};
use tokio_stream::wrappers::TcpListenerStream;
use uuid::Uuid;

use position_service::channel::{PositionChange, PositionSender, PositionUpdate, SharedLedger};
use position_service::ledger::{ContractKey, MarketType, PositionLedger, Quantity, Trade};
use position_service::proto;
use position_service::proto::position_service_client::PositionServiceClient;
use position_service::server;

/// The portfolios every test server is configured with.
const PORTFOLIOS: [&str; 2] = ["execution-a", "strategy-x"];

fn fill(id: u128, portfolio: &str, contract_id: i64, quantity: Quantity) -> Trade {
    Trade {
        id: Uuid::from_u128(id),
        portfolio: portfolio.to_owned(),
        contract: ContractKey {
            market_type: MarketType::Local,
            contract_id,
        },
        quantity,
        price: 4_250,
        executed_at_nanos: 1_000_000_000,
        venue_trade_id: Some(format!("VT-{id}")),
        transfer_id: None,
        cl_order_id: Some(format!("N-{id}")),
        source: "test-feed".to_owned(),
    }
}

fn transfer(
    id: u128,
    from: &str,
    to: &str,
    contract_id: i64,
    quantity: i64,
) -> proto::TransferRequest {
    proto::TransferRequest {
        transfer_id: Uuid::from_u128(id).to_string(),
        from_portfolio: from.to_owned(),
        to_portfolio: to.to_owned(),
        market_type: proto::MarketType::Local as i32,
        contract_id,
        quantity,
        price: 4_250,
        source: "test-crosser".to_owned(),
        executed_at_nanos: 2_000_000_000,
        expected_from_revision: None,
    }
}

/// Starts the server and returns a connected client plus the shutdown handle — dropping the
/// handle drains the server, so tests must keep it alive for the duration.
async fn start_server(
    ledger: SharedLedger,
    updates: PositionSender,
) -> (
    PositionServiceClient<tonic::transport::Channel>,
    watch::Sender<bool>,
) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    let (shutdown, shutdown_rx) = watch::channel(false);
    let portfolios: HashSet<String> = PORTFOLIOS.iter().map(|p| p.to_string()).collect();
    let router = server::router(portfolios, ledger, updates, None, shutdown_rx)
        .await
        .unwrap();
    tokio::spawn(router.serve_with_incoming(TcpListenerStream::new(listener)));
    let client = PositionServiceClient::connect(endpoint).await.unwrap();
    (client, shutdown)
}

async fn next(stream: &mut tonic::Streaming<proto::PositionUpdate>) -> proto::PositionUpdate {
    tokio::time::timeout(Duration::from_secs(5), stream.message())
        .await
        .expect("no update arrived in time")
        .unwrap()
        .expect("stream ended unexpectedly")
}

#[tokio::test]
async fn transfer_moves_position_and_acks_both_revisions() {
    // Given a ledger holding a 10 MW fill on the execution portfolio
    let ledger: SharedLedger = Arc::new(RwLock::new(PositionLedger::new()));
    ledger
        .write()
        .await
        .apply(fill(1, "execution-a", 7, 10_000));
    let (updates, _keep_open) = broadcast::channel(16);
    let (mut client, _shutdown) = start_server(ledger, updates).await;

    // When 4 MW are transferred to the strategy portfolio
    let response = client
        .transfer(transfer(100, "execution-a", "strategy-x", 7, 4_000))
        .await
        .unwrap()
        .into_inner();

    // Then the ack carries both resulting positions: the fill's revision advanced on the
    // from side, and a fresh position on the to side
    let from = response.from_position.unwrap();
    let to = response.to_position.unwrap();
    assert_eq!(from.portfolio, "execution-a");
    assert_eq!(from.net_quantity, 6_000);
    assert_eq!(from.revision, 2);
    assert_eq!(to.portfolio, "strategy-x");
    assert_eq!(to.net_quantity, 4_000);
    assert_eq!(to.revision, 1);
    assert_eq!(to.market_type(), proto::MarketType::Local);
    assert_eq!(to.contract_id, 7);
}

#[tokio::test]
async fn replaying_a_transfer_acks_idempotently_with_unchanged_positions() {
    // Given an applied transfer
    let ledger: SharedLedger = Arc::new(RwLock::new(PositionLedger::new()));
    ledger
        .write()
        .await
        .apply(fill(1, "execution-a", 7, 10_000));
    let (updates, _keep_open) = broadcast::channel(16);
    let (mut client, _shutdown) = start_server(ledger, updates).await;
    let request = transfer(100, "execution-a", "strategy-x", 7, 4_000);
    let first = client.transfer(request.clone()).await.unwrap().into_inner();

    // When the same transfer id is submitted again (a crash-safe retry)
    let replay = client.transfer(request).await.unwrap().into_inner();

    // Then it acks with the current positions, unchanged — nothing double-applied
    assert_eq!(replay, first);
    let from = replay.from_position.unwrap();
    assert_eq!(from.net_quantity, 6_000);
    assert_eq!(from.revision, 2);
}

#[tokio::test]
async fn transfer_with_stale_expected_from_revision_is_aborted() {
    // Given a reconciled portfolio guarded as the from side (the by-convention
    // orientation), starting from "never seen" (expected revision 0) — the negative quantity
    // pulls 4 MW in from the execution portfolio
    let ledger: SharedLedger = Arc::new(RwLock::new(PositionLedger::new()));
    ledger
        .write()
        .await
        .apply(fill(1, "execution-a", 7, 10_000));
    let (updates, _keep_open) = broadcast::channel(16);
    let (mut client, _shutdown) = start_server(ledger, updates).await;
    let mut guarded = transfer(100, "strategy-x", "execution-a", 7, -4_000);
    guarded.expected_from_revision = Some(0);
    let response = client.transfer(guarded).await.unwrap().into_inner();
    assert_eq!(response.from_position.unwrap().net_quantity, 4_000);

    // When a second transfer still expects revision 0 (a delta computed from a stale read)
    let mut stale = transfer(101, "strategy-x", "execution-a", 7, 1_000);
    stale.expected_from_revision = Some(0);
    let status = client.transfer(stale).await.unwrap_err();

    // Then it is aborted — the caller re-reads and retries
    assert_eq!(status.code(), tonic::Code::Aborted);

    // And a retry carrying the current revision applies: 4 MW - 1 MW back out = 3 MW
    let mut retry = transfer(102, "strategy-x", "execution-a", 7, 1_000);
    retry.expected_from_revision = Some(1);
    let response = client.transfer(retry).await.unwrap().into_inner();
    assert_eq!(response.from_position.unwrap().net_quantity, 3_000);
}

#[tokio::test]
async fn negative_quantity_reverses_the_direction() {
    // Given a 10 MW execution position
    let ledger: SharedLedger = Arc::new(RwLock::new(PositionLedger::new()));
    ledger
        .write()
        .await
        .apply(fill(1, "execution-a", 7, 10_000));
    let (updates, _keep_open) = broadcast::channel(16);
    let (mut client, _shutdown) = start_server(ledger, updates).await;

    // When a transfer with from = execution carries a negative quantity (flow reversed: the
    // from side receives)
    let response = client
        .transfer(transfer(100, "execution-a", "strategy-x", 7, -4_000))
        .await
        .unwrap()
        .into_inner();

    // Then it is exactly the mirrored transfer: execution gains, strategy goes short
    assert_eq!(response.from_position.unwrap().net_quantity, 14_000);
    assert_eq!(response.to_position.unwrap().net_quantity, -4_000);
}

#[tokio::test]
async fn transfer_rejects_invalid_requests() {
    let ledger: SharedLedger = Arc::new(RwLock::new(PositionLedger::new()));
    let (updates, _keep_open) = broadcast::channel(16);
    let (client, _shutdown) = start_server(ledger, updates).await;
    let invalid = |request: proto::TransferRequest| async {
        client
            .clone()
            .transfer(request)
            .await
            .expect_err("request should be rejected")
            .code()
    };
    let valid = || transfer(100, "execution-a", "strategy-x", 7, 4_000);

    // An unconfigured portfolio — on either side
    let mut request = valid();
    request.from_portfolio = "unknown".to_owned();
    assert_eq!(invalid(request).await, tonic::Code::InvalidArgument);
    let mut request = valid();
    request.to_portfolio = "unknown".to_owned();
    assert_eq!(invalid(request).await, tonic::Code::InvalidArgument);

    // A self-transfer
    let mut request = valid();
    request.to_portfolio = "execution-a".to_owned();
    assert_eq!(invalid(request).await, tonic::Code::InvalidArgument);

    // A malformed or nil transfer id
    let mut request = valid();
    request.transfer_id = "not-a-uuid".to_owned();
    assert_eq!(invalid(request).await, tonic::Code::InvalidArgument);
    let mut request = valid();
    request.transfer_id = Uuid::nil().to_string();
    assert_eq!(invalid(request).await, tonic::Code::InvalidArgument);

    // An unset market type — half the contract's identity
    let mut request = valid();
    request.market_type = proto::MarketType::None as i32;
    assert_eq!(invalid(request).await, tonic::Code::InvalidArgument);

    // A zero quantity — a transfer that moves nothing (the quantity is signed, so zero is the
    // only invalid value)
    let mut request = valid();
    request.quantity = 0;
    assert_eq!(invalid(request).await, tonic::Code::InvalidArgument);

    // A missing source
    let mut request = valid();
    request.source = String::new();
    assert_eq!(invalid(request).await, tonic::Code::InvalidArgument);
}

#[tokio::test]
async fn subscribe_seeds_then_streams_deltas_for_the_portfolio() {
    // Given a ledger holding positions in two portfolios
    let ledger: SharedLedger = Arc::new(RwLock::new(PositionLedger::new()));
    {
        let mut ledger = ledger.write().await;
        ledger.apply(fill(1, "execution-a", 7, 10_000));
        ledger.apply(fill(2, "strategy-x", 8, 1_000));
    }
    let (updates, _keep_open) = broadcast::channel(16);
    let (mut client, _shutdown) = start_server(ledger, updates).await;

    // When a client subscribes to the execution portfolio
    let mut stream = client
        .subscribe(proto::SubscribeRequest {
            portfolio: "execution-a".to_owned(),
        })
        .await
        .unwrap()
        .into_inner();

    // Then the snapshot carries only that portfolio's history — here a single fold,
    // attributed to the fill that produced it
    let snapshot = next(&mut stream).await;
    assert_eq!(snapshot.positions.len(), 1);
    assert_eq!(snapshot.positions[0].portfolio, "execution-a");
    assert_eq!(snapshot.positions[0].net_quantity, 10_000);
    assert_eq!(snapshot.positions[0].revision, 1);
    assert_eq!(snapshot.positions[0].venue_trade_id, "VT-1");
    assert_eq!(snapshot.positions[0].transfer_id, "");
    assert_eq!(snapshot.positions[0].source, "test-feed");
    assert_eq!(snapshot.positions[0].executed_at_nanos, 1_000_000_000);

    // When a transfer touches both portfolios
    client
        .transfer(transfer(100, "execution-a", "strategy-x", 7, 4_000))
        .await
        .unwrap();

    // Then the subscriber sees only its portfolio's updated position, attributed to the
    // transfer that caused it: the submitter's transfer id and source, no venue trade id
    let delta = next(&mut stream).await;
    assert_eq!(delta.positions.len(), 1);
    assert_eq!(delta.positions[0].portfolio, "execution-a");
    assert_eq!(delta.positions[0].net_quantity, 6_000);
    assert_eq!(delta.positions[0].revision, 2);
    assert_eq!(
        delta.positions[0].transfer_id,
        Uuid::from_u128(100).to_string()
    );
    assert_eq!(delta.positions[0].venue_trade_id, "");
    assert_eq!(delta.positions[0].cl_order_id, "");
    assert_eq!(delta.positions[0].source, "test-crosser");
    assert_eq!(delta.positions[0].executed_at_nanos, 2_000_000_000);
}

#[tokio::test]
async fn snapshot_replays_the_attributed_position_history() {
    // Given one pair folded from two fills, then a transfer
    let ledger: SharedLedger = Arc::new(RwLock::new(PositionLedger::new()));
    {
        let mut ledger = ledger.write().await;
        ledger.apply(fill(1, "execution-a", 7, 2_000));
        ledger.apply(fill(2, "execution-a", 7, 10_000));
    }
    let (updates, _keep_open) = broadcast::channel(16);
    let (mut client, _shutdown) = start_server(ledger, updates).await;
    client
        .transfer(transfer(100, "execution-a", "strategy-x", 7, 4_000))
        .await
        .unwrap();

    // When a client subscribes after all three folded
    let mut stream = client
        .subscribe(proto::SubscribeRequest {
            portfolio: "execution-a".to_owned(),
        })
        .await
        .unwrap()
        .into_inner();

    // Then the snapshot replays the pair's whole history, oldest first — one Position
    // per fold, each the value that fold produced with the causing trade's attribution,
    // exactly the wire shape of a live change. The keep-highest-revision fold lands on
    // the current value, and a re-subscribing consumer retires any trade it was
    // awaiting straight from the seed.
    let snapshot = next(&mut stream).await;
    assert_eq!(snapshot.positions.len(), 3);
    let [first, second, third] = &snapshot.positions[..] else {
        panic!("expected three history entries");
    };
    assert_eq!(first.venue_trade_id, "VT-1");
    assert_eq!(first.net_quantity, 2_000);
    assert_eq!(first.revision, 1);
    assert_eq!(second.venue_trade_id, "VT-2");
    assert_eq!(second.cl_order_id, "N-2");
    assert_eq!(second.transfer_id, "");
    assert_eq!(second.source, "test-feed");
    assert_eq!(second.executed_at_nanos, 1_000_000_000);
    assert_eq!(second.net_quantity, 12_000);
    assert_eq!(second.revision, 2);
    assert_eq!(third.transfer_id, Uuid::from_u128(100).to_string());
    assert_eq!(third.venue_trade_id, "");
    assert_eq!(third.source, "test-crosser");
    assert_eq!(third.net_quantity, 8_000);
    assert_eq!(third.revision, 3);

    // And the stream continues seamlessly: the next change is the next attributed
    // Position, indistinguishable in shape from the seed's entries
    client
        .transfer(transfer(101, "execution-a", "strategy-x", 7, 1_000))
        .await
        .unwrap();
    let delta = next(&mut stream).await;
    assert_eq!(delta.positions.len(), 1);
    assert_eq!(
        delta.positions[0].transfer_id,
        Uuid::from_u128(101).to_string()
    );
    assert_eq!(delta.positions[0].net_quantity, 7_000);
    assert_eq!(delta.positions[0].revision, 4);
}

#[tokio::test]
async fn streamed_fill_batch_carries_one_attribution_per_changed_position() {
    // Given a subscriber that has its (empty) snapshot
    let ledger: SharedLedger = Arc::new(RwLock::new(PositionLedger::new()));
    let (updates, _keep_open) = broadcast::channel(16);
    let (mut client, _shutdown) = start_server(ledger.clone(), updates.clone()).await;
    let mut stream = client
        .subscribe(proto::SubscribeRequest {
            portfolio: "execution-a".to_owned(),
        })
        .await
        .unwrap()
        .into_inner();
    next(&mut stream).await;

    // When a multi-fill batch folds — two fills on the subscribed portfolio (distinct
    // contracts) plus one on another portfolio — and is broadcast the way the gateway
    // feeds do (each changed position paired with its fill)
    let fills = [
        fill(1, "execution-a", 7, 5_000),
        fill(2, "execution-a", 8, 1_000),
        fill(3, "strategy-x", 7, 2_000),
    ];
    let changes: Vec<PositionChange> = {
        let mut ledger = ledger.write().await;
        fills
            .iter()
            .map(|fill| PositionChange::new(fill, ledger.apply(fill.clone()).unwrap()))
            .collect()
    };
    updates.send(PositionUpdate { changes }).unwrap();

    // Then the per-portfolio trim keeps both of the subscriber's positions, each with the
    // attribution of its own fill — the venue trade id and source, no transfer id
    let delta = next(&mut stream).await;
    assert_eq!(delta.positions.len(), 2);
    for (position, fill) in delta.positions.iter().zip(&fills) {
        assert_eq!(position.portfolio, "execution-a");
        assert_eq!(position.contract_id, fill.contract.contract_id);
        assert_eq!(position.venue_trade_id, format!("VT-{}", fill.id.as_u128()));
        assert_eq!(position.transfer_id, "");
        assert_eq!(position.cl_order_id, format!("N-{}", fill.id.as_u128()));
        assert_eq!(position.source, "test-feed");
        assert_eq!(position.executed_at_nanos, fill.executed_at_nanos);
    }
}

#[tokio::test]
async fn subscribe_to_an_unknown_portfolio_seeds_empty_without_error() {
    // Given a server whose ledger holds nothing for "strategy-x"
    let ledger: SharedLedger = Arc::new(RwLock::new(PositionLedger::new()));
    let (updates, _keep_open) = broadcast::channel(16);
    let (mut client, _shutdown) = start_server(ledger, updates).await;

    // When a client subscribes to it (or to any portfolio the service never saw)
    let mut stream = client
        .subscribe(proto::SubscribeRequest {
            portfolio: "never-seen".to_owned(),
        })
        .await
        .unwrap()
        .into_inner();

    // Then the snapshot is empty — not an error, matching the stack's filter semantics
    let snapshot = next(&mut stream).await;
    assert!(snapshot.positions.is_empty());
}

#[tokio::test]
async fn subscribe_rejects_an_empty_portfolio() {
    let ledger: SharedLedger = Arc::new(RwLock::new(PositionLedger::new()));
    let (updates, _keep_open) = broadcast::channel(16);
    let (mut client, _shutdown) = start_server(ledger, updates).await;

    let status = client
        .subscribe(proto::SubscribeRequest {
            portfolio: String::new(),
        })
        .await
        .unwrap_err();
    assert_eq!(status.code(), tonic::Code::InvalidArgument);
}

#[tokio::test]
async fn lagged_subscriber_is_dropped_with_resource_exhausted() {
    // Given a tiny broadcast (capacity 1) and a subscriber that has its snapshot but stops
    // reading
    let ledger: SharedLedger = Arc::new(RwLock::new(PositionLedger::new()));
    let (updates, _keep_open) = broadcast::channel(1);
    let (mut client, _shutdown) = start_server(ledger.clone(), updates.clone()).await;
    let mut stream = client
        .subscribe(proto::SubscribeRequest {
            portfolio: "execution-a".to_owned(),
        })
        .await
        .unwrap()
        .into_inner();
    next(&mut stream).await;

    // When the writers outrun it: two broadcasts with no await between them, so on this
    // single-threaded test runtime the subscriber's stream cannot poll in between and the
    // capacity-1 channel evicts the first batch — a guaranteed lag
    {
        let mut ledger = ledger.write().await;
        ledger.apply(fill(1, "execution-a", 7, 1_000));
        ledger.apply(fill(2, "execution-a", 7, 1_000));
    }
    for _ in 0..2 {
        let position = {
            let ledger = ledger.read().await;
            ledger
                .get(
                    "execution-a",
                    &ContractKey {
                        market_type: MarketType::Local,
                        contract_id: 7,
                    },
                )
                .unwrap()
                .clone()
        };
        updates
            .send(PositionUpdate {
                changes: vec![PositionChange::new(
                    &fill(99, "execution-a", 7, 0),
                    position,
                )],
            })
            .unwrap();
    }

    // Then the stream ends with RESOURCE_EXHAUSTED after the buffered updates, and a
    // reconnect is accepted and re-seeds
    let status = loop {
        match tokio::time::timeout(Duration::from_secs(5), stream.message())
            .await
            .expect("stream neither erred nor ended")
        {
            Ok(Some(_)) => continue,
            Ok(None) => panic!("stream ended cleanly instead of erroring"),
            Err(status) => break status,
        }
    };
    assert_eq!(status.code(), tonic::Code::ResourceExhausted);

    let mut stream = client
        .subscribe(proto::SubscribeRequest {
            portfolio: "execution-a".to_owned(),
        })
        .await
        .unwrap()
        .into_inner();
    // The re-seed replays the pair's history — both folds — ending on the current value,
    // so the keep-highest fold recovers everything the lag dropped
    let snapshot = next(&mut stream).await;
    assert_eq!(snapshot.positions.len(), 2);
    assert_eq!(snapshot.positions[1].net_quantity, 2_000);
    assert_eq!(snapshot.positions[1].revision, 2);
}

#[tokio::test]
async fn shutdown_ends_subscriber_streams() {
    // Given a subscriber that has received its snapshot
    let ledger: SharedLedger = Arc::new(RwLock::new(PositionLedger::new()));
    let (updates, _keep_open) = broadcast::channel(16);
    let (mut client, shutdown) = start_server(ledger, updates).await;
    let mut stream = client
        .subscribe(proto::SubscribeRequest {
            portfolio: "execution-a".to_owned(),
        })
        .await
        .unwrap()
        .into_inner();
    next(&mut stream).await;

    // When the server is asked to drain
    shutdown.send(true).unwrap();

    // Then the infinite subscription stream ends cleanly instead of holding shutdown open
    let end = tokio::time::timeout(Duration::from_secs(5), stream.message())
        .await
        .expect("stream did not end on shutdown");
    assert!(end.unwrap().is_none());
}
