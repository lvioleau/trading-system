//! Black-box test of the service's own gRPC API through a real client over a local socket:
//! the seed-then-deltas subscription (live contracts only) and the full-history
//! `ListContracts` query. The feed is simulated by writing the store and broadcasting, in
//! that order — the same contract the real feed client honors.
//!
//! The subscription filters out contracts whose delivery window has passed against the real
//! clock, so subscription tests place delivery windows relative to now; `ListContracts` tests
//! keep small absolute timestamps — deep in the past, which is exactly the point: the query
//! serves the full history.

use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use tokio::net::TcpListener;
use tokio::sync::{RwLock, broadcast, watch};
use tokio_stream::wrappers::TcpListenerStream;

use contract_service::channel::{ContractSender, ContractUpdate, SharedContracts};
use contract_service::proto;
use contract_service::proto::contract_service_client::ContractServiceClient;
use contract_service::server;
use contract_service::store::{
    AreaState, Contract, ContractKey, ContractState, ContractStore, MarketType, ProductType,
};
use eic::EicCode;

const HOUR_MS: i64 = 3_600_000;

const FR: EicCode = EicCode::AREA_FR;
const CH: EicCode = EicCode::AREA_CH;

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64
}

fn contract(contract_id: i64, area: EicCode, start_ms: i64, end_ms: i64) -> Contract {
    Contract {
        key: ContractKey {
            market_type: MarketType::Local,
            contract_id,
        },
        name: format!("PH-{contract_id}"),
        market_id: "M1".to_owned(),
        product_type: ProductType::P15Min,
        delivery_start_ms: start_ms,
        delivery_end_ms: end_ms,
        duration_seconds: 900,
        updated_at_ms: 42,
        areas: [(
            area,
            AreaState {
                state: ContractState::Active,
                updated_at_ms: 42,
            },
        )]
        .into(),
    }
}

/// Starts the server and returns a connected client plus the shutdown handle — dropping the
/// handle drains the server, so tests must keep it alive for the duration.
async fn start_server(
    contracts: SharedContracts,
    updates: ContractSender,
) -> (
    ContractServiceClient<tonic::transport::Channel>,
    watch::Sender<bool>,
) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    let (shutdown, shutdown_rx) = watch::channel(false);
    let router = server::router(contracts, updates, None, shutdown_rx)
        .await
        .unwrap();
    tokio::spawn(router.serve_with_incoming(TcpListenerStream::new(listener)));
    let client = ContractServiceClient::connect(endpoint).await.unwrap();
    (client, shutdown)
}

/// Simulate one feed frame: write the store, then broadcast the changed batch.
async fn feed_frame(
    contracts: &SharedContracts,
    updates: &ContractSender,
    changed: Vec<Contract>,
    received_at_nanos: i64,
) {
    {
        let mut store = contracts.write().await;
        for contract in &changed {
            store.upsert(contract.clone());
        }
    }
    updates
        .send(ContractUpdate {
            contracts: changed,
            received_at_nanos,
        })
        .unwrap();
}

async fn next(stream: &mut tonic::Streaming<proto::ContractUpdate>) -> proto::ContractUpdate {
    tokio::time::timeout(Duration::from_secs(5), stream.message())
        .await
        .expect("no update arrived in time")
        .unwrap()
        .expect("stream ended unexpectedly")
}

#[tokio::test]
async fn subscribe_seeds_then_streams_deltas_for_the_area() {
    // Given a server whose store already holds a live contract in area 40 and one in area 41
    let now = now_ms();
    let contracts: SharedContracts = Arc::new(RwLock::new(ContractStore::new()));
    contracts
        .write()
        .await
        .upsert(contract(1, FR, now + HOUR_MS, now + 2 * HOUR_MS));
    contracts
        .write()
        .await
        .upsert(contract(2, CH, now + HOUR_MS, now + 2 * HOUR_MS));
    let (updates, _keep_open) = broadcast::channel(16);
    let (mut client, _shutdown) = start_server(contracts.clone(), updates.clone()).await;

    // When a client subscribes to area 40
    let mut stream = client
        .subscribe_contracts(proto::SubscribeContractsRequest {
            delivery_area_eic: FR.to_string(),
        })
        .await
        .unwrap()
        .into_inner();

    // Then the seed carries only the area-40 contract, with the seed's zero receive time
    let seed = next(&mut stream).await;
    assert_eq!(seed.received_at_nanos, 0);
    assert_eq!(seed.contracts.len(), 1);
    assert_eq!(seed.contracts[0].contract_id, 1);

    // When the feed applies a frame touching only area 41, then one touching area 40
    feed_frame(
        &contracts,
        &updates,
        vec![contract(3, CH, now + 2 * HOUR_MS, now + 3 * HOUR_MS)],
        111,
    )
    .await;
    feed_frame(
        &contracts,
        &updates,
        vec![contract(4, FR, now + 2 * HOUR_MS, now + 3 * HOUR_MS)],
        222,
    )
    .await;

    // Then the subscriber sees only the area-40 delta, stamped with its frame's receive time
    let delta = next(&mut stream).await;
    assert_eq!(delta.received_at_nanos, 222);
    assert_eq!(delta.contracts.len(), 1);
    assert_eq!(delta.contracts[0].contract_id, 4);
}

#[tokio::test]
async fn subscribe_excludes_contracts_whose_delivery_has_passed() {
    // Given a store holding a delivered contract and a live one, both in area 40
    let now = now_ms();
    let contracts: SharedContracts = Arc::new(RwLock::new(ContractStore::new()));
    {
        let mut store = contracts.write().await;
        store.upsert(contract(1, FR, now - 2 * HOUR_MS, now - HOUR_MS)); // delivered
        store.upsert(contract(2, FR, now + HOUR_MS, now + 2 * HOUR_MS)); // live
    }
    let (updates, _keep_open) = broadcast::channel(16);
    let (mut client, _shutdown) = start_server(contracts.clone(), updates.clone()).await;

    // When a client subscribes to area 40
    let mut stream = client
        .subscribe_contracts(proto::SubscribeContractsRequest {
            delivery_area_eic: FR.to_string(),
        })
        .await
        .unwrap()
        .into_inner();

    // Then the seed carries only the live contract
    let seed = next(&mut stream).await;
    let ids: Vec<i64> = seed.contracts.iter().map(|c| c.contract_id).collect();
    assert_eq!(ids, vec![2]);

    // When the feed applies a frame touching a delivered contract and a live one — e.g. a
    // late state flip on the old contract arriving alongside a fresh one
    feed_frame(
        &contracts,
        &updates,
        vec![
            contract(3, FR, now - 2 * HOUR_MS, now - HOUR_MS),
            contract(4, FR, now + 2 * HOUR_MS, now + 3 * HOUR_MS),
        ],
        333,
    )
    .await;

    // Then the delta carries only the live contract
    let delta = next(&mut stream).await;
    let ids: Vec<i64> = delta.contracts.iter().map(|c| c.contract_id).collect();
    assert_eq!(ids, vec![4]);

    // And the delivered contracts stay queryable — history is ListContracts's job
    let response = client
        .list_contracts(proto::ListContractsRequest {
            delivery_area_eics: vec![FR.to_string()],
            delivery_start_ms: now - 2 * HOUR_MS,
            delivery_end_ms: now,
            contract_id: 0,
        })
        .await
        .unwrap()
        .into_inner();
    let ids: Vec<i64> = response.contracts.iter().map(|c| c.contract_id).collect();
    assert_eq!(ids, vec![1, 3]);
}

#[tokio::test]
async fn lagged_subscriber_is_dropped_with_resource_exhausted() {
    // Given a tiny broadcast (capacity 1) and a subscriber that has its seed but stops reading
    let contracts: SharedContracts = Arc::new(RwLock::new(ContractStore::new()));
    let (updates, _keep_open) = broadcast::channel(1);
    let (mut client, _shutdown) = start_server(contracts.clone(), updates.clone()).await;
    let mut stream = client
        .subscribe_contracts(proto::SubscribeContractsRequest {
            delivery_area_eic: FR.to_string(),
        })
        .await
        .unwrap()
        .into_inner();
    next(&mut stream).await;

    // When the feed outruns it: two broadcasts with no await between them, so on this
    // single-threaded test runtime the subscriber's stream cannot poll in between and the
    // capacity-1 channel evicts the first batch — a guaranteed lag
    let now = now_ms();
    {
        let mut store = contracts.write().await;
        store.upsert(contract(1, FR, now + HOUR_MS, now + 2 * HOUR_MS));
        store.upsert(contract(2, FR, now + 2 * HOUR_MS, now + 3 * HOUR_MS));
    }
    for i in 1..=2 {
        updates
            .send(ContractUpdate {
                contracts: vec![contract(i, FR, now + HOUR_MS, now + 2 * HOUR_MS)],
                received_at_nanos: i,
            })
            .unwrap();
    }

    // Then the stream ends with RESOURCE_EXHAUSTED after the buffered updates, and a reconnect
    // is accepted and re-seeds
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
        .subscribe_contracts(proto::SubscribeContractsRequest {
            delivery_area_eic: FR.to_string(),
        })
        .await
        .unwrap()
        .into_inner();
    let seed = next(&mut stream).await;
    assert_eq!(seed.received_at_nanos, 0);
    assert!(!seed.contracts.is_empty());
}

#[tokio::test]
async fn shutdown_ends_subscriber_streams() {
    // Given a subscriber that has received its seed
    let contracts: SharedContracts = Arc::new(RwLock::new(ContractStore::new()));
    let (updates, _keep_open) = broadcast::channel(16);
    let (mut client, shutdown) = start_server(contracts, updates).await;
    let mut stream = client
        .subscribe_contracts(proto::SubscribeContractsRequest {
            delivery_area_eic: FR.to_string(),
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

#[tokio::test]
async fn subscribe_rejects_unset_delivery_area() {
    let contracts: SharedContracts = Arc::new(RwLock::new(ContractStore::new()));
    let (updates, _keep_open) = broadcast::channel(16);
    let (mut client, _shutdown) = start_server(contracts, updates).await;

    let status = client
        .subscribe_contracts(proto::SubscribeContractsRequest {
            delivery_area_eic: String::new(),
        })
        .await
        .unwrap_err();
    assert_eq!(status.code(), tonic::Code::InvalidArgument);
}

#[tokio::test]
async fn list_contracts_answers_over_full_history_with_filters() {
    // Given a store holding contracts across two delivery windows and two areas
    let contracts: SharedContracts = Arc::new(RwLock::new(ContractStore::new()));
    {
        let mut store = contracts.write().await;
        store.upsert(contract(1, FR, 1_000, 1_900)); // in window
        store.upsert(contract(2, FR, 5_000, 5_900)); // outside window
        store.upsert(contract(3, FR, 1_000, 4_600)); // overlaps the window
        store.upsert(contract(4, CH, 1_000, 1_900)); // other area
    }
    let (updates, _keep_open) = broadcast::channel(16);
    let (mut client, _shutdown) = start_server(contracts, updates).await;

    // When querying area 40 for [0, 2000) with no filter
    let response = client
        .list_contracts(proto::ListContractsRequest {
            delivery_area_eics: vec![FR.to_string()],
            delivery_start_ms: 0,
            delivery_end_ms: 2_000,
            contract_id: 0,
        })
        .await
        .unwrap()
        .into_inner();

    // Then both overlapping area-40 contracts come back, sorted by delivery start then id
    let ids: Vec<i64> = response.contracts.iter().map(|c| c.contract_id).collect();
    assert_eq!(ids, vec![1, 3]);

    // And the contract-id filter narrows to at most one contract
    let response = client
        .list_contracts(proto::ListContractsRequest {
            delivery_area_eics: vec![FR.to_string()],
            delivery_start_ms: 0,
            delivery_end_ms: 2_000,
            contract_id: 3,
        })
        .await
        .unwrap()
        .into_inner();
    let ids: Vec<i64> = response.contracts.iter().map(|c| c.contract_id).collect();
    assert_eq!(ids, vec![3]);
}

#[tokio::test]
async fn get_contract_history_validates_identity_and_needs_storage() {
    // Given a server without a database (storage: None) whose store holds one contract
    let now = now_ms();
    let contracts: SharedContracts = Arc::new(RwLock::new(ContractStore::new()));
    contracts
        .write()
        .await
        .upsert(contract(1, FR, now + HOUR_MS, now + 2 * HOUR_MS));
    let (updates, _keep_open) = broadcast::channel(16);
    let (mut client, _shutdown) = start_server(contracts, updates).await;

    // Then an unset market type is rejected — it is half of the contract's identity
    let status = client
        .get_contract_history(proto::GetContractHistoryRequest {
            market_type: proto::MarketType::None as i32,
            contract_id: 1,
        })
        .await
        .unwrap_err();
    assert_eq!(status.code(), tonic::Code::InvalidArgument);

    // And a contract the service has never seen answers NOT_FOUND, from the index alone
    let status = client
        .get_contract_history(proto::GetContractHistoryRequest {
            market_type: proto::MarketType::Xbid as i32,
            contract_id: 999,
        })
        .await
        .unwrap_err();
    assert_eq!(status.code(), tonic::Code::NotFound);

    // And the known contract answers UNAVAILABLE, since its history lives in the database
    let status = client
        .get_contract_history(proto::GetContractHistoryRequest {
            market_type: proto::MarketType::Local as i32,
            contract_id: 1,
        })
        .await
        .unwrap_err();
    assert_eq!(status.code(), tonic::Code::Unavailable);
}

#[tokio::test]
async fn find_contracts_answers_by_containment_with_optional_filters() {
    // Given a store holding contracts across windows and areas (small absolute timestamps —
    // deep in the past, since the query serves the full history)
    let contracts: SharedContracts = Arc::new(RwLock::new(ContractStore::new()));
    {
        let mut store = contracts.write().await;
        store.upsert(contract(1, FR, 1_000, 1_900));
        store.upsert(contract(2, FR, 2_000, 2_900));
        store.upsert(contract(3, CH, 1_000, 1_900)); // contract 1's window, other area
        store.upsert(contract(4, FR, 500, 1_900)); // starts before the bounds below
    }
    let (updates, _keep_open) = broadcast::channel(16);
    let (mut client, _shutdown) = start_server(contracts, updates).await;
    let find = |contract_id, delivery_area_eics, from_ms, to_ms| proto::FindContractsRequest {
        delivery_area_eics,
        delivery_from_ms: from_ms,
        delivery_to_ms: to_ms,
        contract_id,
        product_type: proto::ProductType::None as i32,
    };
    let ids = |response: proto::FindContractsResponse| {
        response
            .contracts
            .iter()
            .map(|c| c.contract_id)
            .collect::<Vec<i64>>()
    };

    // Then a wide window returns every contained contract (4 starts before the bounds),
    // sorted by delivery start then id
    let response = client.find_contracts(find(0, vec![], 1_000, 2_900)).await;
    assert_eq!(ids(response.unwrap().into_inner()), vec![1, 3, 2]);

    // And exact bounds match exactly that window
    let response = client.find_contracts(find(0, vec![], 1_000, 1_900)).await;
    assert_eq!(ids(response.unwrap().into_inner()), vec![1, 3]);

    // Narrowed by delivery area
    let response = client
        .find_contracts(find(0, vec![CH.to_string()], 1_000, 1_900))
        .await;
    assert_eq!(ids(response.unwrap().into_inner()), vec![3]);

    // Narrowed by contract id: at most one contract
    let response = client.find_contracts(find(1, vec![], 1_000, 1_900)).await;
    assert_eq!(ids(response.unwrap().into_inner()), vec![1]);

    // And an empty window is rejected
    let status = client
        .find_contracts(find(0, vec![], 2_000, 2_000))
        .await
        .unwrap_err();
    assert_eq!(status.code(), tonic::Code::InvalidArgument);
}

#[tokio::test]
async fn list_contracts_rejects_an_empty_window() {
    let contracts: SharedContracts = Arc::new(RwLock::new(ContractStore::new()));
    let (updates, _keep_open) = broadcast::channel(16);
    let (mut client, _shutdown) = start_server(contracts, updates).await;

    let status = client
        .list_contracts(proto::ListContractsRequest {
            delivery_area_eics: vec![FR.to_string()],
            delivery_start_ms: 2_000,
            delivery_end_ms: 2_000,
            contract_id: 0,
        })
        .await
        .unwrap_err();
    assert_eq!(status.code(), tonic::Code::InvalidArgument);
}
