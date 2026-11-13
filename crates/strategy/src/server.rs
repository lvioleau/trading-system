//! The mock's gRPC API (`flow.v1.StrategyService`) — the serving side of the driver's
//! target stream — plus the stack's stream-server conventions: gRPC health (`grpc.health.v1`) and
//! server reflection.
//!
//! `SubscribeTargets` is scoped to the requested portfolios: a request naming a portfolio this
//! strategy does not produce is rejected with `INVALID_ARGUMENT` (a misconfigured crosser fails
//! fast), and the stream carries only the requested portfolios. There is no seed and no snapshot
//! — each subscriber joins the driver's broadcast and receives records from that point on; the
//! driver's heartbeat is what lets a late subscriber catch up within one interval. A subscriber
//! that falls behind the broadcast is dropped with `RESOURCE_EXHAUSTED`, matching the stack's lag
//! policy.

use std::collections::HashSet;
use std::net::SocketAddr;
use std::pin::Pin;

use tokio::sync::watch;
use tokio_stream::wrappers::BroadcastStream;
use tokio_stream::wrappers::errors::BroadcastStreamRecvError;
use tokio_stream::{Stream, StreamExt};
use tonic::{Request, Response, Status};
use tracing::{info, warn};

use crate::driver::{self, RecordSender};
use crate::proto;
use crate::proto::strategy_service_server::{StrategyService, StrategyServiceServer};

/// Serve the API on `addr` until `shutdown` fires, then drain gracefully: stop accepting, end
/// every subscriber stream, and return once in-flight requests have completed.
pub async fn serve(
    addr: SocketAddr,
    portfolios: HashSet<String>,
    records: RecordSender,
    shutdown: watch::Receiver<bool>,
) -> anyhow::Result<()> {
    info!(%addr, "serving strategy api");
    let mut drain = shutdown.clone();
    router(portfolios, records, shutdown)
        .await?
        .serve_with_shutdown(addr, async move {
            // Err means the sender is gone, i.e. the process is exiting anyway.
            let _ = drain.changed().await;
        })
        .await?;
    Ok(())
}

/// The assembled tonic router: the strategy API plus health and reflection. Split from [`serve`]
/// so tests can run it on an arbitrary listener.
pub async fn router(
    portfolios: HashSet<String>,
    records: RecordSender,
    shutdown: watch::Receiver<bool>,
) -> anyhow::Result<tonic::transport::server::Router> {
    let reflection = tonic_reflection::server::Builder::configure()
        .register_encoded_file_descriptor_set(proto::FILE_DESCRIPTOR_SET)
        .register_encoded_file_descriptor_set(tonic_health::pb::FILE_DESCRIPTOR_SET)
        .build_v1()?;
    let (mut health_reporter, health) = tonic_health::server::health_reporter();
    health_reporter
        .set_serving::<StrategyServiceServer<StrategyApi>>()
        .await;
    // Flip health to NOT_SERVING the moment a drain starts, so health checks stop routing to a
    // server that no longer accepts connections.
    {
        let mut shutdown = shutdown.clone();
        let mut health_reporter = health_reporter.clone();
        tokio::spawn(async move {
            if shutdown.changed().await.is_ok() {
                health_reporter
                    .set_not_serving::<StrategyServiceServer<StrategyApi>>()
                    .await;
            }
        });
    }
    Ok(tonic::transport::Server::builder()
        .add_service(StrategyServiceServer::new(StrategyApi {
            portfolios,
            records,
            shutdown,
        }))
        .add_service(health)
        .add_service(reflection))
}

pub struct StrategyApi {
    /// The portfolios this strategy produces; a subscription for any other is rejected.
    portfolios: HashSet<String>,
    records: RecordSender,
    shutdown: watch::Receiver<bool>,
}

#[tonic::async_trait]
impl StrategyService for StrategyApi {
    type SubscribeTargetsStream =
        Pin<Box<dyn Stream<Item = Result<proto::TargetRecord, Status>> + Send>>;

    async fn subscribe_targets(
        &self,
        request: Request<proto::SubscribeTargetsRequest>,
    ) -> Result<Response<Self::SubscribeTargetsStream>, Status> {
        let request = request.into_inner();
        if request.portfolios.is_empty() {
            return Err(Status::invalid_argument(
                "at least one portfolio is required",
            ));
        }
        // Reject the whole subscription if it names a portfolio this strategy does not produce —
        // the crosser treats this as a fatal boot error rather than silently receiving nothing.
        for portfolio in &request.portfolios {
            if !self.portfolios.contains(portfolio) {
                return Err(Status::invalid_argument(format!(
                    "portfolio {portfolio:?} is not produced by this strategy"
                )));
            }
        }
        let requested: HashSet<String> = request.portfolios.into_iter().collect();
        info!(
            strategy = request.strategy,
            portfolios = ?requested,
            "target subscription opened"
        );

        // Join the driver's broadcast; there is no snapshot — the heartbeat re-emits the current
        // targets, so a late subscriber catches up within one interval.
        let records = BroadcastStream::new(self.records.subscribe()).filter_map(move |received| {
            match received {
                // Trim to the requested portfolios; a record for another is suppressed.
                Ok(record) if requested.contains(&record.portfolio) => {
                    Some(Ok(to_wire_record(&record)))
                }
                Ok(_) => None,
                // A lag means the client can't keep up; end the stream with a terminal status
                // (tonic stops at the first Err) — a live client reconnects.
                Err(BroadcastStreamRecvError::Lagged(skipped)) => {
                    warn!(
                        skipped,
                        "target subscriber lagged behind the broadcast; dropping"
                    );
                    Some(Err(Status::resource_exhausted("subscriber lagged")))
                }
            }
        });
        // Cut the stream at shutdown — subscriptions are infinite, so ending them here is what
        // lets the server's graceful drain complete.
        let mut shutdown = self.shutdown.clone();
        let stream = futures_util::StreamExt::take_until(records, async move {
            let _ = shutdown.changed().await;
        });
        Ok(Response::new(Box::pin(stream)))
    }
}

fn to_wire_market_type(market_type: driver::MarketType) -> proto::MarketType {
    match market_type {
        driver::MarketType::Local => proto::MarketType::Local,
        driver::MarketType::Xbid => proto::MarketType::Xbid,
    }
}

fn to_wire_record(record: &driver::TargetRecord) -> proto::TargetRecord {
    proto::TargetRecord {
        portfolio: record.portfolio.clone(),
        market_type: to_wire_market_type(record.market_type) as i32,
        contract_id: record.contract_id,
        target_quantity: record.target_quantity,
        current_quantity: record.current_quantity,
        delta_quantity: record.delta_quantity,
        mid_price: record.mid_price,
    }
}
