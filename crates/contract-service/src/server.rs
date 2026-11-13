//! The service's own gRPC API (`flow.v1.ContractService`) — the serving side of the
//! in-memory index — plus the stack's stream-server conventions: gRPC health (`grpc.health.v1`,
//! backing Datadog health checks) and server reflection (grpcurl/grpcui exploration; Datadog
//! behavior checks discover the schema from the running server).
//!
//! `SubscribeContracts` follows the seed-then-deltas ordering rules: the feed writes the store
//! before broadcasting, and each subscriber joins the broadcast before reading its seed, so an
//! overlap only duplicates (clients dedupe by `updated_at`), never drops. A subscriber that
//! falls behind the broadcast is dropped with `RESOURCE_EXHAUSTED` and reconnects to re-seed,
//! matching the connector's own lag policy.
//!
//! The subscription serves the *live* set: contracts whose delivery window has already passed
//! are excluded from the seed and from delta batches — a hot client tracks what is (still)
//! tradeable, and on a boot-fold the seed would otherwise open with years of history. The full
//! history is the unary queries' job — `ListContracts` (window overlap) and `FindContracts`
//! (window containment) — answered from the in-memory index, so the DB stays off the hot read
//! path. The exception is `GetContractHistory` (one contract's change history), which reads
//! Postgres directly: unbounded, cold-path data.

use std::net::SocketAddr;
use std::pin::Pin;
use std::time::{SystemTime, UNIX_EPOCH};

use eic::EicCode;
use tokio::sync::watch;
use tokio_stream::wrappers::BroadcastStream;
use tokio_stream::wrappers::errors::BroadcastStreamRecvError;
use tokio_stream::{Stream, StreamExt};
use tonic::{Request, Response, Status};
use tracing::{info, warn};

use crate::channel::{ContractSender, SharedContracts};
use crate::proto;
use crate::proto::contract_service_server::{ContractService, ContractServiceServer};
use crate::storage::{ContractStorage, StateChange};
use crate::store;

/// Serve the API on `addr` until `shutdown` fires, then drain gracefully: stop accepting,
/// end every subscriber stream, and return once in-flight requests have completed.
pub async fn serve(
    addr: SocketAddr,
    contracts: SharedContracts,
    updates: ContractSender,
    storage: Option<ContractStorage>,
    shutdown: watch::Receiver<bool>,
) -> anyhow::Result<()> {
    info!(%addr, "serving contract api");
    let mut drain = shutdown.clone();
    router(contracts, updates, storage, shutdown)
        .await?
        .serve_with_shutdown(addr, async move {
            // Err means the sender is gone, i.e. the process is exiting anyway.
            let _ = drain.changed().await;
        })
        .await?;
    Ok(())
}

/// The assembled tonic router: the contract API plus health and reflection. Split from
/// [`serve`] so tests can run it on an arbitrary listener. `storage: None` (tests without a
/// database) serves everything but `GetContractHistory`, which answers `UNAVAILABLE`.
pub async fn router(
    contracts: SharedContracts,
    updates: ContractSender,
    storage: Option<ContractStorage>,
    shutdown: watch::Receiver<bool>,
) -> anyhow::Result<tonic::transport::server::Router> {
    let reflection = tonic_reflection::server::Builder::configure()
        .register_encoded_file_descriptor_set(proto::FILE_DESCRIPTOR_SET)
        .register_encoded_file_descriptor_set(tonic_health::pb::FILE_DESCRIPTOR_SET)
        .build_v1()?;
    let (mut health_reporter, health) = tonic_health::server::health_reporter();
    health_reporter
        .set_serving::<ContractServiceServer<ContractApi>>()
        .await;
    // Flip health to NOT_SERVING the moment a drain starts, so health checks stop routing
    // to a server that no longer accepts connections.
    {
        let mut shutdown = shutdown.clone();
        let mut health_reporter = health_reporter.clone();
        tokio::spawn(async move {
            if shutdown.changed().await.is_ok() {
                health_reporter
                    .set_not_serving::<ContractServiceServer<ContractApi>>()
                    .await;
            }
        });
    }
    Ok(tonic::transport::Server::builder()
        .add_service(ContractServiceServer::new(ContractApi {
            contracts,
            updates,
            storage,
            shutdown,
        }))
        .add_service(health)
        .add_service(reflection))
}

pub struct ContractApi {
    contracts: SharedContracts,
    updates: ContractSender,
    storage: Option<ContractStorage>,
    shutdown: watch::Receiver<bool>,
}

#[tonic::async_trait]
impl ContractService for ContractApi {
    type SubscribeContractsStream =
        Pin<Box<dyn Stream<Item = Result<proto::ContractUpdate, Status>> + Send>>;

    async fn subscribe_contracts(
        &self,
        request: Request<proto::SubscribeContractsRequest>,
    ) -> Result<Response<Self::SubscribeContractsStream>, Status> {
        let requested = request.into_inner().delivery_area_eic;
        let Some(area) = EicCode::from_eic(&requested) else {
            return Err(Status::invalid_argument(format!(
                "delivery_area_eic {requested:?} is not a known EIC code"
            )));
        };
        // Join the broadcast before reading the seed: a change broadcast after this point is
        // received; one broadcast before it is already in the seed (the feed writes the store
        // first). An area the service is not configured for is not an error, matching the
        // connector's own filter semantics — it just seeds empty and never updates.
        let updates = self.updates.subscribe();
        let seed = seed(&self.contracts, area).await;
        let mut shutdown = self.shutdown.clone();

        // Every broadcast batch trimmed to the live contracts trading in the subscribed area
        // (a batch that touched no such contract is suppressed, like the connector's own
        // filtered stream). A subscriber that stops reading leaves batches queuing in its
        // broadcast receiver — memory stays bounded by the channel's capacity either way.
        let deltas = BroadcastStream::new(updates).filter_map(move |received| match received {
            Ok(batch) => {
                let now_ms = now_ms();
                let for_area: Vec<proto::Contract> = batch
                    .contracts
                    .iter()
                    .filter(|contract| contract.areas.contains_key(&area))
                    .filter(|contract| is_live(contract, now_ms))
                    .map(to_wire_contract)
                    .collect();
                if for_area.is_empty() {
                    return None;
                }
                Some(Ok(proto::ContractUpdate {
                    contracts: for_area,
                    received_at_nanos: batch.received_at_nanos,
                }))
            }
            // A lag means the client can't keep up; re-sending the full set would only push
            // it more data it can't drain. End the stream with a terminal status (tonic
            // stops at the first Err) — a live client reconnects and re-seeds.
            Err(BroadcastStreamRecvError::Lagged(skipped)) => {
                warn!(
                    area = %area.eic_code(),
                    skipped, "subscriber lagged behind the contract broadcast; dropping"
                );
                Some(Err(Status::resource_exhausted("subscriber lagged")))
            }
        });
        // Cut the stream at shutdown — subscriptions are infinite, so ending them here is
        // what lets the server's graceful drain complete. (Err from `changed` means the
        // sender is gone, i.e. the process is going down too.)
        let stream = futures_util::StreamExt::take_until(
            tokio_stream::once(Ok(seed)).chain(deltas),
            async move {
                let _ = shutdown.changed().await;
            },
        );
        Ok(Response::new(Box::pin(stream)))
    }

    /// The contract itself comes from the in-memory index (which also answers "never seen"
    /// with `NOT_FOUND`); the transitions are the one read answered from Postgres,
    /// deliberately — history is unbounded, cold-path data (debugging, attribution), and
    /// holding it in the index would bloat every boot for no hot-path gain.
    async fn get_contract_history(
        &self,
        request: Request<proto::GetContractHistoryRequest>,
    ) -> Result<Response<proto::GetContractHistoryResponse>, Status> {
        let request = request.into_inner();
        let market_type = proto::MarketType::try_from(request.market_type)
            .map_err(|_| Status::invalid_argument("unknown market_type"))?;
        let key = store::ContractKey {
            market_type: market_type_filter(market_type)
                .ok_or_else(|| Status::invalid_argument("market_type is required"))?,
            contract_id: request.contract_id,
        };
        let contract = {
            let store = self.contracts.read().await;
            store
                .get(&key)
                .map(to_wire_contract)
                .ok_or_else(|| Status::not_found("contract has never been seen"))?
        };
        let Some(storage) = &self.storage else {
            return Err(Status::unavailable(
                "contract history storage is not configured",
            ));
        };
        let state_changes = storage.history(&key).await.map_err(|error| {
            warn!(%error, ?key, "loading contract history failed");
            Status::internal("loading contract history failed")
        })?;
        Ok(Response::new(proto::GetContractHistoryResponse {
            contract: Some(contract),
            state_changes: state_changes.iter().map(to_wire_state_change).collect(),
        }))
    }

    async fn list_contracts(
        &self,
        request: Request<proto::ListContractsRequest>,
    ) -> Result<Response<proto::ListContractsResponse>, Status> {
        let query = ListQuery::parse(&request.into_inner())?;
        let contracts = query_index(&self.contracts, |c| query.matches(c)).await;
        Ok(Response::new(proto::ListContractsResponse { contracts }))
    }

    async fn find_contracts(
        &self,
        request: Request<proto::FindContractsRequest>,
    ) -> Result<Response<proto::FindContractsResponse>, Status> {
        let query = FindQuery::parse(&request.into_inner())?;
        let contracts = query_index(&self.contracts, |c| query.matches(c)).await;
        Ok(Response::new(proto::FindContractsResponse { contracts }))
    }
}

/// Answer a full-history query from the index: every contract matching `matches`, sorted by
/// delivery start then contract id — the store iterates in hash order, so both unary queries
/// sort here to keep responses deterministic.
async fn query_index(
    contracts: &SharedContracts,
    matches: impl Fn(&store::Contract) -> bool,
) -> Vec<proto::Contract> {
    let store = contracts.read().await;
    let mut matched: Vec<&store::Contract> = store.iter().filter(|c| matches(c)).collect();
    matched.sort_by_key(|c| (c.delivery_start_ms, c.key.contract_id));
    matched.into_iter().map(to_wire_contract).collect()
}

/// The live set for one area as a seed message — `received_at_nanos` 0, since it is
/// assembled from the state of many frames.
async fn seed(contracts: &SharedContracts, area: EicCode) -> proto::ContractUpdate {
    let now_ms = now_ms();
    let store = contracts.read().await;
    let contracts = store
        .iter()
        .filter(|contract| contract.areas.contains_key(&area))
        .filter(|contract| is_live(contract, now_ms))
        .map(to_wire_contract)
        .collect();
    proto::ContractUpdate {
        contracts,
        received_at_nanos: 0,
    }
}

/// Whether a contract's delivery window has not fully passed. The subscription's filter: a
/// contract mid-delivery still counts (its window is not over), one whose delivery ended at
/// or before `now_ms` is history — stored and queryable, but not streamed.
fn is_live(contract: &store::Contract, now_ms: i64) -> bool {
    contract.delivery_end_ms > now_ms
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock before the unix epoch")
        .as_millis() as i64
}

/// A validated `ListContractsRequest`.
#[derive(Debug)]
struct ListQuery {
    delivery_areas: Vec<EicCode>,
    delivery_start_ms: i64,
    delivery_end_ms: i64,
    contract_id: Option<i64>,
}

impl ListQuery {
    // tonic::Status is just big; it goes straight back over the wire, never onto a hot path.
    #[expect(clippy::result_large_err)]
    fn parse(request: &proto::ListContractsRequest) -> Result<Self, Status> {
        if request.delivery_area_eics.is_empty() {
            return Err(Status::invalid_argument(
                "delivery_area_eics must list at least one delivery area",
            ));
        }
        if request.delivery_end_ms <= request.delivery_start_ms {
            return Err(Status::invalid_argument(
                "delivery window [delivery_start_ms, delivery_end_ms) is empty",
            ));
        }
        Ok(Self {
            delivery_areas: parse_eics(&request.delivery_area_eics)?,
            delivery_start_ms: request.delivery_start_ms,
            delivery_end_ms: request.delivery_end_ms,
            contract_id: (request.contract_id != 0).then_some(request.contract_id),
        })
    }

    /// Overlap semantics on the window (delivery ∩ [start, end) ≠ ∅), restricted to contracts
    /// trading in any of the queried areas; the optional contract-id filter matches
    /// everything when unset.
    fn matches(&self, contract: &store::Contract) -> bool {
        self.delivery_areas
            .iter()
            .any(|area| contract.areas.contains_key(area))
            && contract.delivery_start_ms < self.delivery_end_ms
            && contract.delivery_end_ms > self.delivery_start_ms
            && self
                .contract_id
                .is_none_or(|id| id == contract.key.contract_id)
    }
}

/// A validated `FindContractsRequest`.
#[derive(Debug)]
struct FindQuery {
    /// Empty is no filter at all — every area matches.
    delivery_areas: Vec<EicCode>,
    delivery_from_ms: i64,
    delivery_to_ms: i64,
    contract_id: Option<i64>,
    product_type: Option<store::ProductType>,
}

impl FindQuery {
    // tonic::Status is just big; it goes straight back over the wire, never onto a hot path.
    #[expect(clippy::result_large_err)]
    fn parse(request: &proto::FindContractsRequest) -> Result<Self, Status> {
        if request.delivery_to_ms <= request.delivery_from_ms {
            return Err(Status::invalid_argument(
                "delivery window [delivery_from_ms, delivery_to_ms] is empty",
            ));
        }
        // Decode the enum strictly: an unknown value is a client speaking a newer proto, not
        // an unset filter.
        let product_type = proto::ProductType::try_from(request.product_type)
            .map_err(|_| Status::invalid_argument("unknown product_type"))?;
        Ok(Self {
            delivery_areas: parse_eics(&request.delivery_area_eics)?,
            delivery_from_ms: request.delivery_from_ms,
            delivery_to_ms: request.delivery_to_ms,
            contract_id: (request.contract_id != 0).then_some(request.contract_id),
            product_type: product_type_filter(product_type),
        })
    }

    /// Containment semantics: the contract's whole window lies between the bounds — equal
    /// bounds are an exact window match. The optional filters match everything when unset;
    /// the area filter tests where the contract trades (any listed area), not a state.
    fn matches(&self, contract: &store::Contract) -> bool {
        (self.delivery_areas.is_empty()
            || self
                .delivery_areas
                .iter()
                .any(|area| contract.areas.contains_key(area)))
            && contract.delivery_start_ms >= self.delivery_from_ms
            && contract.delivery_end_ms <= self.delivery_to_ms
            && self
                .contract_id
                .is_none_or(|id| id == contract.key.contract_id)
            && self
                .product_type
                .is_none_or(|product| product == contract.product_type)
    }
}

/// Resolve a request's EIC strings. An unknown EIC is a malformed request (a typo would
/// otherwise silently never match), spelled out per offending value.
// tonic::Status is just big; it goes straight back over the wire, never onto a hot path.
#[expect(clippy::result_large_err)]
fn parse_eics(eics: &[String]) -> Result<Vec<EicCode>, Status> {
    eics.iter()
        .map(|eic| {
            EicCode::from_eic(eic).ok_or_else(|| {
                Status::invalid_argument(format!("{eic:?} is not a known delivery-area EIC code"))
            })
        })
        .collect()
}

fn product_type_filter(product_type: proto::ProductType) -> Option<store::ProductType> {
    match product_type {
        proto::ProductType::None => None,
        proto::ProductType::P15Min => Some(store::ProductType::P15Min),
        proto::ProductType::P30Min => Some(store::ProductType::P30Min),
        proto::ProductType::P60Min => Some(store::ProductType::P60Min),
        proto::ProductType::Other => Some(store::ProductType::Other),
    }
}

fn market_type_filter(market_type: proto::MarketType) -> Option<store::MarketType> {
    match market_type {
        proto::MarketType::None => None,
        proto::MarketType::Local => Some(store::MarketType::Local),
        proto::MarketType::Xbid => Some(store::MarketType::Xbid),
    }
}

fn to_wire_state_change(change: &StateChange) -> proto::StateChange {
    proto::StateChange {
        delivery_area_eic: change.delivery_area_eic.eic_code().to_owned(),
        state: to_wire_state(change.state) as i32,
        updated_at_ms: change.updated_at_ms,
        recorded_at_ms: change.recorded_at_ms,
    }
}

fn to_wire_contract(contract: &store::Contract) -> proto::Contract {
    proto::Contract {
        contract_id: contract.key.contract_id,
        name: contract.name.clone(),
        market_id: contract.market_id.clone(),
        market_type: to_wire_market_type(contract.key.market_type) as i32,
        product_type: to_wire_product_type(contract.product_type) as i32,
        delivery_start_ms: contract.delivery_start_ms,
        delivery_end_ms: contract.delivery_end_ms,
        duration_seconds: contract.duration_seconds,
        updated_at_ms: contract.updated_at_ms,
        delivery_areas: contract
            .areas
            .iter()
            .map(|(delivery_area_eic, area)| proto::ContractDeliveryArea {
                delivery_area_eic: delivery_area_eic.eic_code().to_owned(),
                state: to_wire_state(area.state) as i32,
            })
            .collect(),
    }
}

fn to_wire_market_type(market_type: store::MarketType) -> proto::MarketType {
    match market_type {
        store::MarketType::Local => proto::MarketType::Local,
        store::MarketType::Xbid => proto::MarketType::Xbid,
    }
}

fn to_wire_product_type(product_type: store::ProductType) -> proto::ProductType {
    match product_type {
        store::ProductType::P15Min => proto::ProductType::P15Min,
        store::ProductType::P30Min => proto::ProductType::P30Min,
        store::ProductType::P60Min => proto::ProductType::P60Min,
        store::ProductType::Other => proto::ProductType::Other,
    }
}

fn to_wire_state(state: store::ContractState) -> proto::ContractState {
    match state {
        // Unknown is the domain image of the wire's unset NONE; it round-trips back.
        store::ContractState::Unknown => proto::ContractState::None,
        store::ContractState::Active => proto::ContractState::Active,
        store::ContractState::Hibernated => proto::ContractState::Hibernated,
        store::ContractState::Inactive => proto::ContractState::Inactive,
        store::ContractState::Frozen => proto::ContractState::Frozen,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::{AreaState, Contract, ContractKey, ContractState, MarketType, ProductType};

    const FR: EicCode = EicCode::AREA_FR;
    const CH: EicCode = EicCode::AREA_CH;

    fn contract(areas: &[(EicCode, ContractState)], start_ms: i64, end_ms: i64) -> Contract {
        Contract {
            key: ContractKey {
                market_type: MarketType::Local,
                contract_id: 1,
            },
            name: "PH-1".to_owned(),
            market_id: "M1".to_owned(),
            product_type: ProductType::P15Min,
            delivery_start_ms: start_ms,
            delivery_end_ms: end_ms,
            duration_seconds: 900,
            updated_at_ms: 42,
            areas: areas
                .iter()
                .map(|&(area, state)| {
                    (
                        area,
                        AreaState {
                            state,
                            updated_at_ms: 42,
                        },
                    )
                })
                .collect(),
        }
    }

    fn list_query(area: EicCode, delivery_start_ms: i64, delivery_end_ms: i64) -> ListQuery {
        ListQuery {
            delivery_areas: vec![area],
            delivery_start_ms,
            delivery_end_ms,
            contract_id: None,
        }
    }

    #[test]
    fn window_overlap_is_half_open() {
        // Given a contract delivering [1000, 1900) in area 40
        let c = contract(&[(FR, ContractState::Active)], 1_000, 1_900);

        // Then any overlapping window matches, and merely touching windows do not
        assert!(list_query(FR, 1_500, 1_600).matches(&c)); // window inside delivery
        assert!(list_query(FR, 0, 5_000).matches(&c)); // delivery inside window
        assert!(list_query(FR, 1_899, 5_000).matches(&c)); // one ms of overlap
        assert!(!list_query(FR, 1_900, 5_000).matches(&c)); // window starts at delivery end
        assert!(!list_query(FR, 0, 1_000).matches(&c)); // window ends at delivery start
    }

    #[test]
    fn live_filter_cuts_at_delivery_end() {
        // Given a contract delivering [1000, 1900)
        let c = contract(&[(FR, ContractState::Active)], 1_000, 1_900);

        // Then it is live before and during delivery, and history from the window's end on
        assert!(is_live(&c, 0)); // not yet delivering
        assert!(is_live(&c, 1_899)); // mid-delivery
        assert!(!is_live(&c, 1_900)); // window just passed
        assert!(!is_live(&c, 5_000)); // long gone
    }

    #[test]
    fn contract_not_trading_in_any_queried_area_never_matches() {
        let c = contract(&[(FR, ContractState::Active)], 1_000, 1_900);
        assert!(!list_query(CH, 0, 5_000).matches(&c));

        // Any listed area suffices
        let mut q = list_query(CH, 0, 5_000);
        q.delivery_areas = vec![CH, FR];
        assert!(q.matches(&c));
    }

    #[test]
    fn list_contract_id_filter_matches_on_equality() {
        // Given contract 1 (the helper's id)
        let c = contract(&[(FR, ContractState::Active)], 1_000, 1_900);

        let mut q = list_query(FR, 0, 5_000);
        q.contract_id = Some(1);
        assert!(q.matches(&c));
        q.contract_id = Some(2);
        assert!(!q.matches(&c));
    }

    #[test]
    fn list_matches_regardless_of_area_state() {
        // Given a contract whose area state is Unknown (wire NONE tripwire)
        let c = contract(&[(FR, ContractState::Unknown)], 1_000, 1_900);

        // Then it lists like any other — the query tests where a contract trades, not a state
        assert!(list_query(FR, 0, 5_000).matches(&c));
    }

    #[test]
    fn list_parse_rejects_empty_areas_and_empty_window() {
        let valid = proto::ListContractsRequest {
            delivery_area_eics: vec![FR.to_string()],
            delivery_start_ms: 0,
            delivery_end_ms: 5_000,
            contract_id: 0,
        };
        // The unset contract id is no filter at all
        assert_eq!(ListQuery::parse(&valid).unwrap().contract_id, None);

        let no_areas = proto::ListContractsRequest {
            delivery_area_eics: vec![],
            ..valid.clone()
        };
        assert_eq!(
            ListQuery::parse(&no_areas).unwrap_err().code(),
            tonic::Code::InvalidArgument
        );

        let empty_window = proto::ListContractsRequest {
            delivery_end_ms: 0,
            ..valid
        };
        assert_eq!(
            ListQuery::parse(&empty_window).unwrap_err().code(),
            tonic::Code::InvalidArgument
        );
    }

    fn find_query(delivery_from_ms: i64, delivery_to_ms: i64) -> FindQuery {
        FindQuery {
            delivery_areas: vec![],
            delivery_from_ms,
            delivery_to_ms,
            contract_id: None,
            product_type: None,
        }
    }

    #[test]
    fn find_matches_on_containment_with_inclusive_bounds() {
        // Given a contract delivering [1000, 1900)
        let c = contract(&[(FR, ContractState::Active)], 1_000, 1_900);

        // Then only windows containing the whole delivery match, the exact window included
        assert!(find_query(1_000, 1_900).matches(&c)); // exact window
        assert!(find_query(0, 5_000).matches(&c)); // delivery inside the bounds
        assert!(!find_query(1_001, 1_900).matches(&c)); // starts before the lower bound
        assert!(!find_query(1_000, 1_899).matches(&c)); // ends past the upper bound
        assert!(!find_query(1_500, 1_600).matches(&c)); // mere overlap is not containment
    }

    #[test]
    fn find_filters_by_contract_id_area_and_product() {
        // Given contract 1, a P15 trading in area 40
        let c = contract(&[(FR, ContractState::Active)], 1_000, 1_900);

        // The contract-id filter matches on equality
        let mut q = find_query(0, 5_000);
        q.contract_id = Some(1);
        assert!(q.matches(&c));
        q.contract_id = Some(2);
        assert!(!q.matches(&c));

        // The area filter tests where the contract trades (any listed area), regardless
        // of state
        let mut q = find_query(0, 5_000);
        q.delivery_areas = vec![FR];
        assert!(q.matches(&c));
        q.delivery_areas = vec![CH];
        assert!(!q.matches(&c));
        q.delivery_areas = vec![CH, FR];
        assert!(q.matches(&c));

        // The product filter matches on equality
        let mut q = find_query(0, 5_000);
        q.product_type = Some(ProductType::P15Min);
        assert!(q.matches(&c));
        q.product_type = Some(ProductType::P60Min);
        assert!(!q.matches(&c));
    }

    #[test]
    fn find_parse_treats_zero_filters_as_unset_and_rejects_bad_requests() {
        let request = proto::FindContractsRequest {
            delivery_area_eics: vec![],
            delivery_from_ms: 1_000,
            delivery_to_ms: 1_900,
            contract_id: 0,
            product_type: 0,
        };
        let query = FindQuery::parse(&request).unwrap();
        assert_eq!(query.contract_id, None);
        assert!(query.delivery_areas.is_empty());
        assert_eq!(query.product_type, None);

        let empty = proto::FindContractsRequest {
            delivery_to_ms: 1_000,
            ..request.clone()
        };
        assert_eq!(
            FindQuery::parse(&empty).unwrap_err().code(),
            tonic::Code::InvalidArgument
        );

        // An unknown enum value is a client speaking a newer proto, not an unset filter
        let unknown_product = proto::FindContractsRequest {
            product_type: 99,
            ..request
        };
        assert_eq!(
            FindQuery::parse(&unknown_product).unwrap_err().code(),
            tonic::Code::InvalidArgument
        );
    }

    #[test]
    fn wire_contract_carries_identity_and_all_area_states() {
        let c = contract(
            &[(FR, ContractState::Active), (CH, ContractState::Hibernated)],
            1_000,
            1_900,
        );

        let wire = to_wire_contract(&c);

        assert_eq!(wire.contract_id, 1);
        assert_eq!(wire.market_type(), proto::MarketType::Local);
        assert_eq!(wire.product_type(), proto::ProductType::P15Min);
        assert_eq!(wire.updated_at_ms, 42);
        assert_eq!(wire.delivery_areas.len(), 2);
        // The area list follows the store's BTreeMap order — EicCode's declaration order,
        // so FR precedes CH.
        assert_eq!(wire.delivery_areas[0].delivery_area_eic, FR.eic_code());
        assert_eq!(wire.delivery_areas[0].state(), proto::ContractState::Active);
        assert_eq!(wire.delivery_areas[1].delivery_area_eic, CH.eic_code());
        assert_eq!(
            wire.delivery_areas[1].state(),
            proto::ContractState::Hibernated
        );
    }
}
