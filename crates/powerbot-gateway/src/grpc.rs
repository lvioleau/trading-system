//! The gRPC server: the executor's window onto PowerBot.
//!
//! Two services, composed onto one runtime by [`serve`]:
//! - [`orders`] — `OrderService`, the write path (place IOC orders over REST);
//! - [`execution`] — `ExecutionFeed`, the read path (stream own external trades,
//!   own internal trades, and own-order revisions).
//!
//! Both translate PowerBot's model to protos owned by us (docs/design.md, "gRPC
//! API"). The execution feeds are seed-then-deltas over the [`ExecutionChannels`]
//! the websocket driver thread writes — a snapshot of current state first, then
//! live deltas, deduped by `seq`; one receiver per subscriber, slow subscribers
//! dropped; following the nordpool-stream-server shape.

use std::net::SocketAddr;
use std::sync::Arc;

use powerbot_models::models;
use tokio::sync::watch;

use crate::contracts::ContractStore;
use crate::rest::RestClient;

mod convert;
mod execution;
mod feed;
mod orders;
mod subscriber;

/// Generated protobuf types and service stubs. `pub` items are unreachable
/// inside this private module, which is expected for generated code.
#[allow(unreachable_pub, clippy::all)]
pub mod proto {
    tonic::include_proto!("flow.v1");

    /// Compiled protobuf schema, served over gRPC reflection so tools like
    /// `grpcurl` can discover the API without being handed the `.proto`.
    pub(crate) const FILE_DESCRIPTOR_SET: &[u8] =
        tonic::include_file_descriptor_set!("powerbot_gateway_descriptor");
}

pub(crate) use convert::{internal_trade_to_domain, own_order_to_domain, trade_to_domain};
pub(crate) use feed::alert_self_trades;
pub use feed::{Batch, Channel, ExecutionChannels, ShadowNormalization};

/// Latest market state observed on the websocket `marketstatus` feed, shared
/// from the driver thread into the order service. `None` until the first update
/// arrives, and reset to `None` whenever the session ends — a state the feed can
/// no longer confirm is stale, not current. The driver publishes with the sender
/// ([`watch::Sender::send_replace`], callable from its sync thread); the order
/// service reads with the receiver.
pub type MarketStatus = watch::Receiver<Option<models::MarketState>>;

use proto::powerbot_execution_service_server::PowerbotExecutionServiceServer;
use proto::powerbot_order_service_server::PowerbotOrderServiceServer;

/// Serve both gRPC services on `addr` until `shutdown` fires, then drain: the
/// order service (write path) and the execution streams (read path: own external
/// trades, own internal trades, own-order revisions), plus the standard gRPC
/// health service and server reflection. `market` is the shared market-status
/// and `contracts` the shared contract-tradability store the order service
/// gates on; `channels` are the store-backed broadcasts the driver feeds and
/// subscribers stream from; `portfolio_ids` are the portfolios this gateway
/// serves (every execution subscription must name one, and order entry is
/// gated on membership — see the execution and orders modules);
/// `max_subscribers` caps each stream.
///
/// `shutdown` doubles as the stream cut: every subscription stream ends itself
/// when it flips to `true`, which is what lets tonic's graceful drain complete
/// (the streams are otherwise infinite). Runs on the caller's tokio runtime;
/// `await` it. Blocking-free.
#[allow(clippy::too_many_arguments)]
pub async fn serve(
    addr: SocketAddr,
    rest: RestClient,
    market: MarketStatus,
    contracts: ContractStore,
    channels: Arc<ExecutionChannels>,
    portfolio_ids: Vec<String>,
    max_subscribers: usize,
    shutdown: watch::Receiver<bool>,
) -> anyhow::Result<()> {
    let reflection = tonic_reflection::server::Builder::configure()
        .register_encoded_file_descriptor_set(proto::FILE_DESCRIPTOR_SET)
        .build_v1()?;

    let (mut health_reporter, health_service) = tonic_health::server::health_reporter();
    health_reporter
        .set_serving::<PowerbotOrderServiceServer<orders::OrderService>>()
        .await;
    health_reporter
        .set_serving::<PowerbotExecutionServiceServer<execution::ExecutionFeed>>()
        .await;

    // The order gate reads the same self-match halt set the execution feed's
    // internal-trade ingest writes, so a live-gateway self-match on a
    // (portfolio, contract) disables further entry on it.
    let self_match_halts = channels.self_match_halts();
    let order_service = PowerbotOrderServiceServer::new(orders::OrderService::new(
        rest,
        market,
        contracts,
        portfolio_ids.clone(),
        self_match_halts,
    ));
    let execution_service = PowerbotExecutionServiceServer::new(execution::ExecutionFeed::new(
        channels,
        portfolio_ids,
        max_subscribers,
        shutdown.clone(),
    ));

    let mut drain = shutdown;
    tracing::info!(%addr, max_subscribers, "gRPC server listening");
    tonic::transport::Server::builder()
        .add_service(order_service)
        .add_service(execution_service)
        .add_service(health_service)
        .add_service(reflection)
        // wait_for, not changed(): resolves even if shutdown was already
        // requested. The drain completes only because each trade stream cuts
        // itself on the same watch — they are infinite otherwise.
        .serve_with_shutdown(addr, async move {
            let _ = drain.wait_for(|&stop| stop).await;
        })
        .await?;
    tracing::info!("gRPC server drained");
    Ok(())
}
