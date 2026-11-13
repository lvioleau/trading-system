//! Black-box test of the contract feed against an in-process mock Contract Service: subscribe,
//! fold the streamed contracts into the shared view, keyed by identity, with the delivery end and
//! per-area active flag the tradable-set logic needs.

use std::collections::HashMap;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::net::TcpListener;
use tokio_stream::wrappers::TcpListenerStream;
use tokio_stream::{Stream, StreamExt};
use tonic::{Request, Response, Status};

use eic::EicCode;
use strategy::contract;
use strategy::contract::contract_service_server::{ContractService, ContractServiceServer};
use strategy::contract_feed;
use strategy::driver::{ContractId, MarketType, SharedContracts};

/// A mock Contract Service replaying one subscription seed, then holding the stream open.
struct MockContracts {
    seed: Vec<contract::Contract>,
}

type UpdateStream = Pin<Box<dyn Stream<Item = Result<contract::ContractUpdate, Status>> + Send>>;

#[tonic::async_trait]
impl ContractService for MockContracts {
    type SubscribeContractsStream = UpdateStream;

    async fn subscribe_contracts(
        &self,
        _request: Request<contract::SubscribeContractsRequest>,
    ) -> Result<Response<Self::SubscribeContractsStream>, Status> {
        let update = contract::ContractUpdate {
            received_at_nanos: 0,
            contracts: self.seed.clone(),
        };
        let stream = tokio_stream::once(Ok(update)).chain(tokio_stream::pending());
        Ok(Response::new(Box::pin(stream)))
    }

    async fn list_contracts(
        &self,
        _request: Request<contract::ListContractsRequest>,
    ) -> Result<Response<contract::ListContractsResponse>, Status> {
        Err(Status::unimplemented("not used by the strategy"))
    }

    async fn find_contracts(
        &self,
        _request: Request<contract::FindContractsRequest>,
    ) -> Result<Response<contract::FindContractsResponse>, Status> {
        Err(Status::unimplemented("not used by the strategy"))
    }

    async fn get_contract_history(
        &self,
        _request: Request<contract::GetContractHistoryRequest>,
    ) -> Result<Response<contract::GetContractHistoryResponse>, Status> {
        Err(Status::unimplemented("not used by the strategy"))
    }
}

async fn start_mock(mock: MockContracts) -> String {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    tokio::spawn(
        tonic::transport::Server::builder()
            .add_service(ContractServiceServer::new(mock))
            .serve_with_incoming(TcpListenerStream::new(listener)),
    );
    endpoint
}

/// A wire contract in one delivery area with the given state.
fn wire_contract(
    contract_id: i64,
    area: EicCode,
    delivery_start_ms: i64,
    state: contract::ContractState,
) -> contract::Contract {
    contract::Contract {
        contract_id,
        name: format!("PH-{contract_id}"),
        market_id: "M1".to_owned(),
        market_type: contract::MarketType::Local as i32,
        product_type: contract::ProductType::P15Min as i32,
        delivery_start_ms,
        delivery_end_ms: delivery_start_ms + 900_000,
        duration_seconds: 900,
        updated_at_ms: 42,
        delivery_areas: vec![contract::ContractDeliveryArea {
            delivery_area_eic: area.to_string(),
            state: state as i32,
        }],
    }
}

/// Poll the shared view until `condition` holds, panicking after five seconds.
async fn wait_for(
    view: &SharedContracts,
    what: &str,
    condition: impl Fn(&HashMap<ContractId, strategy::driver::ContractView>) -> bool,
) {
    let reached = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if condition(&view.lock().unwrap()) {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await;
    assert!(reached.is_ok(), "timed out waiting for: {what}");
}

#[tokio::test]
async fn contract_feed_folds_the_live_set_into_the_view() {
    let area = EicCode::AREA_FR;
    // Active in our area, inactive in our area, and active only in a *different* area.
    let endpoint = start_mock(MockContracts {
        seed: vec![
            wire_contract(100, area, 2_000_000, contract::ContractState::Active),
            wire_contract(101, area, 3_000_000, contract::ContractState::Inactive),
            wire_contract(
                102,
                EicCode::AREA_AT,
                4_000_000,
                contract::ContractState::Active,
            ),
        ],
    })
    .await;

    let view: SharedContracts = Arc::new(Mutex::new(HashMap::new()));
    tokio::spawn(contract_feed::run(endpoint, area, view.clone()));

    wait_for(&view, "all three contracts folded", |v| v.len() == 3).await;
    let v = view.lock().unwrap();
    // Active in our area → active, with its delivery start.
    let a = v[&(MarketType::Local, 100)];
    assert!(a.active);
    assert_eq!(a.delivery_start_ms, 2_000_000);
    // Present in our area but inactive → not active.
    assert!(!v[&(MarketType::Local, 101)].active);
    // Active only in another area → not active for us.
    assert!(!v[&(MarketType::Local, 102)].active);
}
