//! Black-box test of the feed client against an in-process mock connector: subscribe, receive
//! a seed batch, and fold it into the shared store.

use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use tokio::net::TcpListener;
use tokio::sync::{RwLock, broadcast};
use tokio_stream::wrappers::TcpListenerStream;
use tokio_stream::{Stream, StreamExt};
use tonic::{Request, Response, Status};

use contract_service::channel::SharedContracts;
use contract_service::config::FeedConfig;
use contract_service::marketdata;
use contract_service::marketdata::contract_service_server::{
    ContractService, ContractServiceServer,
};
use contract_service::store::{ContractKey, ContractState, ContractStore, MarketType};

use eic::EicCode;

const FR: EicCode = EicCode::AREA_FR;
const CH: EicCode = EicCode::AREA_CH;
use contract_service::{feed, store};

struct MockConnector {
    expected_areas: Vec<String>,
    update: marketdata::ContractUpdate,
}

#[tonic::async_trait]
impl ContractService for MockConnector {
    type SubscribeContractsStream =
        Pin<Box<dyn Stream<Item = Result<marketdata::ContractUpdate, Status>> + Send>>;

    async fn subscribe_contracts(
        &self,
        request: Request<marketdata::SubscribeContractsRequest>,
    ) -> Result<Response<Self::SubscribeContractsStream>, Status> {
        let requested = request.into_inner().eic_codes;
        if requested.is_empty() {
            return Err(Status::invalid_argument("empty delivery area filter"));
        }
        if requested != self.expected_areas {
            return Err(Status::invalid_argument("unexpected delivery area filter"));
        }
        // One seed batch, then hold the stream open like a live connector.
        let stream =
            tokio_stream::iter(vec![Ok(self.update.clone())]).chain(tokio_stream::pending());
        Ok(Response::new(Box::pin(stream)))
    }
}

fn wire_contract(contract_id: i64, area: EicCode) -> marketdata::Contract {
    marketdata::Contract {
        contract_id,
        name: format!("PH-{contract_id}"),
        market_id: "M1".to_owned(),
        market_type: marketdata::MarketType::Local as i32,
        product_type: marketdata::ProductType::P15Min as i32,
        delivery_start_ms: 1_000,
        delivery_end_ms: 1_900,
        duration_seconds: 900,
        updated_at_ms: 42,
        delivery_areas: vec![marketdata::ContractDeliveryArea {
            eic_code: area.eic_code().to_owned(),
            state: marketdata::ContractState::Active as i32,
        }],
    }
}

#[tokio::test]
async fn subscribes_and_folds_the_seed_into_the_store() {
    // Given a mock connector expecting an area-[FR, CH] filter, with a two-contract seed
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    let mock = MockConnector {
        expected_areas: vec![FR.to_string(), CH.to_string()],
        update: marketdata::ContractUpdate {
            contracts: vec![wire_contract(1, FR), wire_contract(2, CH)],
            received_at_nanos: 123_456,
            seq: 1,
        },
    };
    tokio::spawn(
        tonic::transport::Server::builder()
            .add_service(ContractServiceServer::new(mock))
            .serve_with_incoming(TcpListenerStream::new(listener)),
    );

    // When the feed client subscribes for areas FR and CH
    let contracts: SharedContracts = Arc::new(RwLock::new(ContractStore::new()));
    let (updates, mut changes) = broadcast::channel(16);
    tokio::spawn(feed::run(
        endpoint,
        vec![FR, CH],
        FeedConfig::default(),
        None, // memory-only: the write-through is covered by the contract_storage tests
        contracts.clone(),
        updates,
    ));

    // Then the seed ends up in the store
    let deadline = Duration::from_secs(5);
    let filled = tokio::time::timeout(deadline, async {
        loop {
            if contracts.read().await.len() == 2 {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await;
    assert!(filled.is_ok(), "seed was not folded into the store in time");

    let store = contracts.read().await;
    let stored = store
        .get(&ContractKey {
            market_type: MarketType::Local,
            contract_id: 1,
        })
        .unwrap();
    assert_eq!(stored.name, "PH-1");
    assert_eq!(stored.areas[&FR].state, ContractState::Active);
    assert_eq!(stored.product_type, store::ProductType::P15Min);
    let stored = store
        .get(&ContractKey {
            market_type: MarketType::Local,
            contract_id: 2,
        })
        .unwrap();
    assert_eq!(stored.areas[&CH].state, ContractState::Active);
    drop(store);

    // And the changed batch was broadcast for API subscribers, stamped with the frame's
    // connector receive time
    let batch = tokio::time::timeout(deadline, changes.recv())
        .await
        .expect("no change batch was broadcast in time")
        .unwrap();
    assert_eq!(batch.contracts.len(), 2);
    assert_eq!(batch.received_at_nanos, 123_456);
}
