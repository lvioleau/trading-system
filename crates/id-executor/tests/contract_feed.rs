//! Black-box tests of the contract feed (`contract_feed::run` — exactly what production
//! boots) against a mock `ContractService` over a local socket: the seed carries the live
//! set, a state flip streamed as a delta flips tradeability, a stale record never
//! regresses the stored state, an absent contract is never tradeable, `delivery_start_ms`
//! is carried for the loop's cutoff gate, and a reconnect re-seeds from current state.
//! The mock serves contract-service's generated server traits (this crate generates client
//! stubs only); the wire types are the same `flow.v1` package either way.
//!
//! The tests fold the feed's events into a [`ContractStore`] by hand — the engine that
//! will own that fold arrives at step 6; until then the store's fold discipline *is* the
//! consumer contract these tests pin down.

use std::net::SocketAddr;
use std::pin::Pin;
use std::time::Duration;

use tokio::net::TcpListener;
use tokio::sync::{broadcast, mpsc, watch};
use tokio::time::timeout;
use tokio_stream::wrappers::errors::BroadcastStreamRecvError;
use tokio_stream::wrappers::{BroadcastStream, TcpListenerStream};
use tokio_stream::{Stream, StreamExt};
use tonic::{Request, Response, Status};

use contract_service::proto as cs;
use contract_service::proto::contract_service_server::{ContractService, ContractServiceServer};
use eic::EicCode;
use id_executor::config::FeedConfig;
use id_executor::contract_feed::{self, ContractEvent};
use id_executor::contract_manager::ContractStore;
use id_executor::executor::{ContractKey, MarketType};

const AREA: EicCode = EicCode::AREA_FR;
const CONTRACT: i64 = 7;
const DELIVERY_START_MS: i64 = 1_760_000_000_000;

// ---------------------------------------------------------------------------------------------
// Mock upstream
// ---------------------------------------------------------------------------------------------

/// A mock Contract Service for one delivery area: each subscription gets `seed` as its
/// first message, then every update broadcast through `updates` — the service's own
/// seed-then-deltas shape. Streams stay open until the shutdown watch fires (ending open
/// streams is what makes a "server restart" visible to the subscribed client — tonic
/// serves each connection on its own task, so aborting the accept loop alone is not
/// enough).
struct MockContractService {
    seed: Vec<cs::Contract>,
    updates: broadcast::Sender<cs::ContractUpdate>,
    shutdown: watch::Receiver<bool>,
}

#[tonic::async_trait]
impl ContractService for MockContractService {
    type SubscribeContractsStream =
        Pin<Box<dyn Stream<Item = Result<cs::ContractUpdate, Status>> + Send>>;

    async fn subscribe_contracts(
        &self,
        request: Request<cs::SubscribeContractsRequest>,
    ) -> Result<Response<Self::SubscribeContractsStream>, Status> {
        assert_eq!(
            request.into_inner().delivery_area_eic,
            AREA.eic_code(),
            "the feed must subscribe with its configured delivery area"
        );
        let seed = cs::ContractUpdate {
            contracts: self.seed.clone(),
            received_at_nanos: 0,
        };
        let deltas = BroadcastStream::new(self.updates.subscribe()).filter_map(
            |received: Result<cs::ContractUpdate, BroadcastStreamRecvError>| received.ok().map(Ok),
        );
        let mut shutdown = self.shutdown.clone();
        let stream = futures_util::StreamExt::take_until(
            tokio_stream::once(Ok(seed)).chain(deltas),
            async move {
                let _ = shutdown.changed().await;
            },
        );
        Ok(Response::new(Box::pin(stream)))
    }

    // The unary queries are not part of the feed.
    async fn list_contracts(
        &self,
        _: Request<cs::ListContractsRequest>,
    ) -> Result<Response<cs::ListContractsResponse>, Status> {
        Err(Status::unimplemented("not part of the feed"))
    }

    async fn find_contracts(
        &self,
        _: Request<cs::FindContractsRequest>,
    ) -> Result<Response<cs::FindContractsResponse>, Status> {
        Err(Status::unimplemented("not part of the feed"))
    }

    async fn get_contract_history(
        &self,
        _: Request<cs::GetContractHistoryRequest>,
    ) -> Result<Response<cs::GetContractHistoryResponse>, Status> {
        Err(Status::unimplemented("not part of the feed"))
    }
}

/// The running mock's handles: the broadcast sender scripts live deltas; stop + task tear
/// the server down for the reconnect test.
struct MockServer {
    endpoint: String,
    addr: SocketAddr,
    updates: broadcast::Sender<cs::ContractUpdate>,
    shutdown: watch::Sender<bool>,
    task: tokio::task::JoinHandle<()>,
}

fn start_mock(listener: TcpListener, seed: Vec<cs::Contract>) -> MockServer {
    let addr = listener.local_addr().unwrap();
    let endpoint = format!("http://{addr}");
    let (updates, _) = broadcast::channel(16);
    let (shutdown, shutdown_rx) = watch::channel(false);
    let mock = MockContractService {
        seed,
        updates: updates.clone(),
        shutdown: shutdown_rx,
    };
    let task = tokio::spawn(async move {
        let _ = tonic::transport::Server::builder()
            .add_service(ContractServiceServer::new(mock))
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await;
    });
    MockServer {
        endpoint,
        addr,
        updates,
        shutdown,
        task,
    }
}

impl MockServer {
    /// Take the mock down entirely: stop accepting (freeing the address) and end every open
    /// stream, so the subscribed client sees its session close.
    async fn stop(self) -> SocketAddr {
        self.task.abort();
        let _ = self.task.await;
        let _ = self.shutdown.send(true);
        self.addr
    }
}

/// Rebind `addr` after its previous listener shut down, retrying briefly — the OS may take
/// a moment to release the port.
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

/// Boot the feed under test with fast reconnect tuning.
fn start_feed(endpoint: &str) -> mpsc::Receiver<ContractEvent> {
    let (events_tx, events_rx) = mpsc::channel(64);
    tokio::spawn(contract_feed::run(
        endpoint.to_owned(),
        AREA,
        FeedConfig {
            initial_backoff: Duration::from_millis(50),
            max_backoff: Duration::from_millis(200),
            ..FeedConfig::default()
        },
        events_tx,
    ));
    events_rx
}

async fn next_event(events: &mut mpsc::Receiver<ContractEvent>) -> ContractEvent {
    timeout(Duration::from_secs(5), events.recv())
        .await
        .expect("timed out waiting for a contract event")
        .expect("the feed dropped its event sender")
}

// ---------------------------------------------------------------------------------------------
// Wire and domain helpers
// ---------------------------------------------------------------------------------------------

fn key(contract_id: i64) -> ContractKey {
    ContractKey {
        market_type: MarketType::Local,
        contract_id,
    }
}

fn wire_contract(contract_id: i64, state: cs::ContractState, updated_at_ms: i64) -> cs::Contract {
    cs::Contract {
        contract_id,
        name: format!("PH-{contract_id}"),
        market_id: "10".to_owned(),
        market_type: cs::MarketType::Local as i32,
        product_type: cs::ProductType::P60Min as i32,
        delivery_start_ms: DELIVERY_START_MS,
        delivery_end_ms: DELIVERY_START_MS + 3_600_000,
        duration_seconds: 3_600,
        updated_at_ms,
        delivery_areas: vec![cs::ContractDeliveryArea {
            delivery_area_eic: AREA.to_string(),
            state: state as i32,
        }],
    }
}

fn delta(contracts: Vec<cs::Contract>) -> cs::ContractUpdate {
    cs::ContractUpdate {
        contracts,
        received_at_nanos: 123_456,
    }
}

/// Receive the session's seed and fold it, asserting it is tagged `Seeded`.
async fn await_seed(events: &mut mpsc::Receiver<ContractEvent>, store: &mut ContractStore) {
    let ContractEvent::Seeded(contracts) = next_event(events).await else {
        panic!("a session's first message must be the seed")
    };
    for contract in contracts {
        store.fold(contract);
    }
}

// ---------------------------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------------------------

#[tokio::test]
async fn the_seed_carries_the_live_set_and_a_state_flip_updates_tradeability() {
    // Given a live set of one ACTIVE contract
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mock = start_mock(
        listener,
        vec![wire_contract(CONTRACT, cs::ContractState::Active, 100)],
    );
    let mut events = start_feed(&mock.endpoint);

    // When the feed seeds
    let mut store = ContractStore::new();
    await_seed(&mut events, &mut store).await;

    // Then the contract is tradeable feed-side and carries its delivery start (the loop's
    // cutoff input)
    assert!(store.active(&key(CONTRACT)));
    assert_eq!(
        store.get(&key(CONTRACT)).unwrap().delivery_start_ms,
        DELIVERY_START_MS
    );

    // And when the venue flips it ACTIVE → INACTIVE in a delta
    mock.updates
        .send(delta(vec![wire_contract(
            CONTRACT,
            cs::ContractState::Inactive,
            101,
        )]))
        .unwrap();
    let ContractEvent::Updated(updated) = next_event(&mut events).await else {
        panic!("expected the state flip as an update")
    };

    // Then folding the flip removes tradeability
    for contract in updated {
        assert!(store.fold(contract));
    }
    assert!(!store.active(&key(CONTRACT)));
}

#[tokio::test]
async fn a_contract_absent_from_the_live_set_is_not_tradeable() {
    // Given a live set that has never carried contract 8
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mock = start_mock(
        listener,
        vec![wire_contract(CONTRACT, cs::ContractState::Active, 100)],
    );
    let mut events = start_feed(&mock.endpoint);
    let mut store = ContractStore::new();
    await_seed(&mut events, &mut store).await;

    // Then the absent contract reads as not tradeable — the executor can never prove it
    assert!(!store.active(&key(8)));
    assert!(store.get(&key(8)).is_none());
}

#[tokio::test]
async fn a_stale_record_never_regresses_the_stored_state() {
    // Given a contract seeded INACTIVE at venue time 200 (the newest word)
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mock = start_mock(
        listener,
        vec![wire_contract(CONTRACT, cs::ContractState::Inactive, 200)],
    );
    let mut events = start_feed(&mock.endpoint);
    let mut store = ContractStore::new();
    await_seed(&mut events, &mut store).await;
    assert!(!store.active(&key(CONTRACT)));

    // When an out-of-order ACTIVE record from venue time 150 streams (a replayed frame)
    mock.updates
        .send(delta(vec![wire_contract(
            CONTRACT,
            cs::ContractState::Active,
            150,
        )]))
        .unwrap();
    let ContractEvent::Updated(stale) = next_event(&mut events).await else {
        panic!("expected the stale record as an update")
    };

    // Then folding it is a no-op — a dead contract can never look tradeable again through
    // a replay
    assert!(!store.fold(stale[0].clone()));
    assert!(!store.active(&key(CONTRACT)));
}

#[tokio::test]
async fn a_reconnect_reseeds_the_live_set_from_current_state() {
    // Given a seeded feed
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mock = start_mock(
        listener,
        vec![wire_contract(CONTRACT, cs::ContractState::Active, 100)],
    );
    let mut events = start_feed(&mock.endpoint);
    let mut store = ContractStore::new();
    await_seed(&mut events, &mut store).await;
    assert!(store.active(&key(CONTRACT)));

    // When the service goes away and returns with a moved-on live set: the contract went
    // INACTIVE while the subscription was severed, and a new contract appeared
    let addr = mock.stop().await;
    let listener = rebind(addr).await;
    let _mock = start_mock(
        listener,
        vec![
            wire_contract(CONTRACT, cs::ContractState::Inactive, 300),
            wire_contract(8, cs::ContractState::Active, 300),
        ],
    );

    // Then the feed reconnects and re-seeds — the missed flip lands and the new contract
    // joins the set (upsert-by-identity: nothing is cleared, everything refreshes)
    await_seed(&mut events, &mut store).await;
    assert!(!store.active(&key(CONTRACT)));
    assert!(store.active(&key(8)));
}
