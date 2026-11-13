//! The service's own gRPC API (`flow.v1.PositionService`) — the mutation and
//! serving side of the in-memory ledger — plus the stack's stream-server conventions: gRPC
//! health (`grpc.health.v1`) and server reflection.
//!
//! `Transfer` is the one mutation: validated against the configured portfolios, applied as
//! two zero-sum legs under a single ledger write lock, then broadcast — writes before
//! sends, the ordering `Subscribe` relies on. Idempotency comes from the deterministic leg
//! ids (a replay acks with the current positions, unchanged); the optional
//! `expected_from_revision` guard turns a stale read-derived transfer into `ABORTED`
//! instead of a lost update (by convention the from side is the portfolio the caller's
//! signed quantity was derived from).
//!
//! With storage attached (production), the ack means durable: both legs commit to the
//! trade log *before* the in-memory fold and the broadcast — commit, then memory, then
//! broadcast — so a failed commit leaves memory untouched and returns `UNAVAILABLE` (or
//! `FAILED_PRECONDITION` when the contract is missing from the reference data; a legs-only
//! partial write cannot exist, the append is one transaction). The ledger write lock is
//! held across the commit: the concurrency check, the commit, and the fold must be atomic
//! against other mutations, and mutations are rare enough that stalling readers for one
//! database round trip is the simple correct trade. `storage: None` runs memory-only — the
//! pure API tests.
//!
//! `Subscribe` follows the snapshot-then-updates ordering rules: each subscriber joins the
//! broadcast before reading its snapshot, and writers update the ledger before
//! broadcasting, so an overlap only duplicates (clients dedupe by `revision`), never drops.
//! The snapshot replays each pair's attributed history — one `Position` per fold, the
//! causing trade's attribution on each, the same wire shape as a live change — so a
//! re-subscribing consumer retires still-awaited trades directly from the seed and the
//! keep-highest-revision fold lands on the current value. A subscriber that falls behind
//! the broadcast is dropped with `RESOURCE_EXHAUSTED` and reconnects to re-seed, matching
//! the stack's lag policy.
//!
//! There is deliberately no fold point-query: the seed carries every fold's attribution
//! and a fold not yet applied arrives with attribution on the open stream (join-before-
//! snapshot), so the `Subscribe` stream is the API's one carrier of position state.

use std::collections::HashSet;
use std::net::SocketAddr;
use std::pin::Pin;

use tokio::sync::watch;
use tokio_stream::wrappers::BroadcastStream;
use tokio_stream::wrappers::errors::BroadcastStreamRecvError;
use tokio_stream::{Stream, StreamExt};
use tonic::{Request, Response, Status};
use tracing::{debug, error, info, warn};
use uuid::Uuid;

use crate::channel::{PositionChange, PositionSender, PositionUpdate, SharedLedger, log_position};
use crate::ledger::{self, ContractKey, MarketType, PortfolioId, Position, Price, Quantity, Trade};
use crate::proto;
use crate::proto::position_service_server::{PositionService, PositionServiceServer};
use crate::storage::{self, PositionStorage};

/// Serve the API on `addr` until `shutdown` fires, then drain gracefully: stop accepting,
/// end every subscriber stream, and return once in-flight requests have completed.
pub async fn serve(
    addr: SocketAddr,
    portfolios: HashSet<PortfolioId>,
    ledger: SharedLedger,
    updates: PositionSender,
    storage: Option<PositionStorage>,
    shutdown: watch::Receiver<bool>,
) -> anyhow::Result<()> {
    info!(%addr, "serving position api");
    let mut drain = shutdown.clone();
    router(portfolios, ledger, updates, storage, shutdown)
        .await?
        .serve_with_shutdown(addr, async move {
            // Err means the sender is gone, i.e. the process is exiting anyway.
            let _ = drain.changed().await;
        })
        .await?;
    Ok(())
}

/// The assembled tonic router: the position API plus health and reflection. Split from
/// [`serve`] so tests can run it on an arbitrary listener.
pub async fn router(
    portfolios: HashSet<PortfolioId>,
    ledger: SharedLedger,
    updates: PositionSender,
    storage: Option<PositionStorage>,
    shutdown: watch::Receiver<bool>,
) -> anyhow::Result<tonic::transport::server::Router> {
    let reflection = tonic_reflection::server::Builder::configure()
        .register_encoded_file_descriptor_set(proto::FILE_DESCRIPTOR_SET)
        .register_encoded_file_descriptor_set(tonic_health::pb::FILE_DESCRIPTOR_SET)
        .build_v1()?;
    let (mut health_reporter, health) = tonic_health::server::health_reporter();
    health_reporter
        .set_serving::<PositionServiceServer<PositionApi>>()
        .await;
    // Flip health to NOT_SERVING the moment a drain starts, so health checks stop routing
    // to a server that no longer accepts connections.
    {
        let mut shutdown = shutdown.clone();
        let mut health_reporter = health_reporter.clone();
        tokio::spawn(async move {
            if shutdown.changed().await.is_ok() {
                health_reporter
                    .set_not_serving::<PositionServiceServer<PositionApi>>()
                    .await;
            }
        });
    }
    Ok(tonic::transport::Server::builder()
        .add_service(PositionServiceServer::new(PositionApi {
            portfolios,
            ledger,
            updates,
            storage,
            shutdown,
        }))
        .add_service(health)
        .add_service(reflection))
}

pub struct PositionApi {
    portfolios: HashSet<PortfolioId>,
    ledger: SharedLedger,
    updates: PositionSender,
    /// The durable trade log; `None` runs memory-only (the pure API tests). With storage,
    /// a mutation acks only after its Postgres commit.
    storage: Option<PositionStorage>,
    shutdown: watch::Receiver<bool>,
}

#[tonic::async_trait]
impl PositionService for PositionApi {
    type SubscribeStream =
        Pin<Box<dyn Stream<Item = Result<proto::PositionUpdate, Status>> + Send>>;

    async fn transfer(
        &self,
        request: Request<proto::TransferRequest>,
    ) -> Result<Response<proto::TransferResponse>, Status> {
        let command = TransferCommand::parse(&request.into_inner(), &self.portfolios)?;
        let (from_leg_id, to_leg_id) = ledger::transfer_leg_ids(command.transfer_id);
        let (from_position, to_position, changes) = {
            let mut ledger = self.ledger.write().await;
            if ledger.is_applied(&from_leg_id) {
                // Idempotent replay: the original already folded (both legs, under this
                // lock) — ack with the current positions, unchanged. No concurrency check:
                // the guard belongs to the transfer's first application.
                let missing =
                    || Status::internal("replayed transfer's position missing from the ledger");
                let from = ledger
                    .get(&command.from_portfolio, &command.contract)
                    .cloned()
                    .ok_or_else(missing)?;
                let to = ledger
                    .get(&command.to_portfolio, &command.contract)
                    .cloned()
                    .ok_or_else(missing)?;
                (from, to, None)
            } else {
                if let Some(expected_revision) = command.expected_from_revision {
                    // The optimistic-concurrency guard: the caller's quantity was derived
                    // from the from side at this revision (the by-convention orientation —
                    // signed quantity makes it always possible); any concurrent change aborts.
                    let current = ledger
                        .get(&command.from_portfolio, &command.contract)
                        .map(|position| position.revision)
                        .unwrap_or(0);
                    if current != expected_revision {
                        return Err(Status::aborted(format!(
                            "expected revision {expected_revision} of ({}, contract {}), \
                             found {current} — re-read and retry",
                            command.from_portfolio, command.contract.contract_id
                        )));
                    }
                }
                let from_leg = command.leg(from_leg_id, &command.from_portfolio, -command.quantity);
                let to_leg = command.leg(to_leg_id, &command.to_portfolio, command.quantity);
                // Commit, then memory, then broadcast: the ack must mean durable, so both
                // legs land in the log (one transaction) before the fold — a failed commit
                // leaves memory untouched and the client retries the same transfer_id.
                if let Some(storage) = &self.storage {
                    storage
                        .append(&[from_leg.clone(), to_leg.clone()])
                        .await
                        .map_err(commit_failure)?;
                }
                let from = ledger
                    .apply(from_leg.clone())
                    .expect("fresh from-leg id was already applied");
                let to = ledger
                    .apply(to_leg.clone())
                    .expect("fresh to-leg id was already applied");
                let changes = vec![
                    PositionChange::new(&from_leg, from.clone()),
                    PositionChange::new(&to_leg, to.clone()),
                ];
                (from, to, Some(changes))
            }
        };
        if let Some(changes) = changes {
            // Record the committed transfer in full — the one ledger mutation on the API
            // side, so both legs are logged entirely (identity, contract, signed quantity,
            // price, attribution). The two positions it produced are logged after.
            info!(
                transfer_id = %command.transfer_id,
                from_portfolio = command.from_portfolio,
                to_portfolio = command.to_portfolio,
                contract = %command.contract,
                quantity = format!("{} kW", command.quantity),
                price = format!("{} EUR/MWh", ledger::eur(command.price)),
                executed_at_nanos = command.executed_at_nanos,
                source = command.source,
                "committed transfer"
            );
            log_position(&from_position);
            log_position(&to_position);
            // Ledger written above, broadcast second (the snapshot-then-updates ordering
            // rule). Send only errs when no subscriber is listening, which is fine.
            let _ = self.updates.send(PositionUpdate { changes });
        } else {
            // Idempotent replay: nothing folded, positions returned unchanged.
            debug!(
                transfer_id = %command.transfer_id,
                "idempotent transfer replay; positions unchanged"
            );
        }
        Ok(Response::new(proto::TransferResponse {
            from_position: Some(to_wire_position(&from_position)),
            to_position: Some(to_wire_position(&to_position)),
        }))
    }

    async fn subscribe(
        &self,
        request: Request<proto::SubscribeRequest>,
    ) -> Result<Response<Self::SubscribeStream>, Status> {
        let portfolio = request.into_inner().portfolio;
        if portfolio.is_empty() {
            return Err(Status::invalid_argument("portfolio is required"));
        }
        // Join the broadcast before reading the snapshot: a change broadcast after this
        // point is received; one broadcast before it is already in the snapshot (writers
        // update the ledger first). An unconfigured portfolio is not an error — it just
        // seeds empty and never updates.
        let updates = self.updates.subscribe();
        let snapshot = snapshot(&self.ledger, &portfolio).await;
        let mut shutdown = self.shutdown.clone();

        // Every broadcast batch trimmed to the subscribed portfolio (a batch that touched
        // no such position is suppressed). A subscriber that stops reading leaves batches
        // queuing in its broadcast receiver — memory stays bounded by the channel's
        // capacity either way.
        let deltas = BroadcastStream::new(updates).filter_map(move |received| match received {
            Ok(batch) => {
                let for_portfolio: Vec<proto::Position> = batch
                    .changes
                    .iter()
                    .filter(|change| change.position.portfolio == portfolio)
                    .map(to_wire_position_with_attribution)
                    .collect();
                if for_portfolio.is_empty() {
                    return None;
                }
                Some(Ok(proto::PositionUpdate {
                    positions: for_portfolio,
                }))
            }
            // A lag means the client can't keep up; re-sending would only push it more
            // data it can't drain. End the stream with a terminal status (tonic stops at
            // the first Err) — a live client reconnects and re-seeds.
            Err(BroadcastStreamRecvError::Lagged(skipped)) => {
                warn!(
                    skipped,
                    "subscriber lagged behind the position broadcast; dropping"
                );
                Some(Err(Status::resource_exhausted("subscriber lagged")))
            }
        });
        // Cut the stream at shutdown — subscriptions are infinite, so ending them here is
        // what lets the server's graceful drain complete.
        let stream = futures_util::StreamExt::take_until(
            tokio_stream::once(Ok(snapshot)).chain(deltas),
            async move {
                let _ = shutdown.changed().await;
            },
        );
        Ok(Response::new(Box::pin(stream)))
    }
}

/// One portfolio's attributed position history as the snapshot message — for each pair,
/// every fold's resulting value with its causing trade's attribution, sorted by contract
/// and then by revision (fold order; the ledger iterates in hash order). The last entry
/// per pair is the current value, so the client's keep-highest-revision fold lands on it —
/// replaying the history makes a (re-)subscribe indistinguishable from having been
/// subscribed since the pair's first trade, which is what lets a consumer retire any
/// trade it was still awaiting straight from the seed.
async fn snapshot(ledger: &SharedLedger, portfolio: &str) -> proto::PositionUpdate {
    let ledger = ledger.read().await;
    let mut positions: Vec<proto::Position> = ledger
        .positions(portfolio)
        .flat_map(|position| ledger.history(&position.portfolio, &position.contract))
        .map(to_wire_position_with_attribution)
        .collect();
    positions.sort_by_key(|position| {
        (
            position.contract_id,
            position.market_type,
            position.revision,
        )
    });
    proto::PositionUpdate { positions }
}

/// A validated `TransferRequest`.
#[derive(Debug)]
struct TransferCommand {
    transfer_id: Uuid,
    from_portfolio: PortfolioId,
    to_portfolio: PortfolioId,
    contract: ContractKey,
    /// kW, signed, non-zero; the to leg receives it, the from leg its negation.
    quantity: Quantity,
    /// Minor currency units per MWh.
    price: Price,
    source: String,
    executed_at_nanos: i64,
    /// The optional concurrency guard on the from side's revision.
    expected_from_revision: Option<u64>,
}

impl TransferCommand {
    // tonic::Status is just big; it goes straight back over the wire, never onto a hot path.
    #[expect(clippy::result_large_err)]
    fn parse(
        request: &proto::TransferRequest,
        portfolios: &HashSet<PortfolioId>,
    ) -> Result<Self, Status> {
        let transfer_id = Uuid::try_parse(&request.transfer_id)
            .map_err(|_| Status::invalid_argument("transfer_id must be a UUID"))?;
        if transfer_id.is_nil() {
            return Err(Status::invalid_argument("transfer_id is required"));
        }
        let from_portfolio = configured(&request.from_portfolio, "from_portfolio", portfolios)?;
        let to_portfolio = configured(&request.to_portfolio, "to_portfolio", portfolios)?;
        if from_portfolio == to_portfolio {
            return Err(Status::invalid_argument(
                "from_portfolio and to_portfolio must differ",
            ));
        }
        let market_type = proto::MarketType::try_from(request.market_type)
            .map_err(|_| Status::invalid_argument("unknown market_type"))?;
        let market_type = market_type_filter(market_type)
            .ok_or_else(|| Status::invalid_argument("market_type is required"))?;
        if request.quantity == 0 {
            return Err(Status::invalid_argument(
                "quantity must be non-zero (signed, kW: to receives quantity, from receives -quantity)",
            ));
        }
        if request.source.is_empty() {
            return Err(Status::invalid_argument("source is required"));
        }
        Ok(Self {
            transfer_id,
            from_portfolio,
            to_portfolio,
            contract: ContractKey {
                market_type,
                contract_id: request.contract_id,
            },
            quantity: request.quantity,
            price: request.price,
            source: request.source.clone(),
            executed_at_nanos: request.executed_at_nanos,
            expected_from_revision: request.expected_from_revision,
        })
    }

    /// One ledger leg of this transfer. Both legs record the originating transfer id (the
    /// canonical UUID string) — the submitter's match key on the Subscribe stream.
    fn leg(&self, id: Uuid, portfolio: &str, quantity: Quantity) -> Trade {
        Trade {
            id,
            portfolio: portfolio.to_owned(),
            contract: self.contract,
            quantity,
            price: self.price,
            executed_at_nanos: self.executed_at_nanos,
            venue_trade_id: None,
            transfer_id: Some(self.transfer_id.to_string()),
            cl_order_id: None,
            source: self.source.clone(),
        }
    }
}

/// A failed transfer commit, mapped for the wire. The composite FK rejecting an unknown
/// contract is the one failure that is the caller's to fix (`FAILED_PRECONDITION`: the
/// contract is not in the reference data — yet; the contract sync may still deliver it).
/// Everything else is the database being unavailable: the ledger is untouched, so the
/// caller retries the same `transfer_id` and idempotency does the rest.
fn commit_failure(error: anyhow::Error) -> Status {
    if storage::is_foreign_key_violation(&error) {
        return Status::failed_precondition(
            "contract is not in the contract reference data (yet); re-check and retry",
        );
    }
    error!(
        error = format!("{error:#}"),
        "transfer commit failed; ledger unchanged"
    );
    Status::unavailable("could not commit the transfer; retry with the same transfer_id")
}

/// A portfolio field: present and configured.
#[expect(clippy::result_large_err)]
fn configured(
    value: &str,
    field: &str,
    portfolios: &HashSet<PortfolioId>,
) -> Result<PortfolioId, Status> {
    if value.is_empty() {
        return Err(Status::invalid_argument(format!("{field} is required")));
    }
    if !portfolios.contains(value) {
        return Err(Status::invalid_argument(format!(
            "{field} {value:?} is not a configured portfolio"
        )));
    }
    Ok(value.to_owned())
}

fn market_type_filter(market_type: proto::MarketType) -> Option<MarketType> {
    match market_type {
        proto::MarketType::None => None,
        proto::MarketType::Local => Some(MarketType::Local),
        proto::MarketType::Xbid => Some(MarketType::Xbid),
    }
}

fn to_wire_market_type(market_type: MarketType) -> proto::MarketType {
    match market_type {
        MarketType::Local => proto::MarketType::Local,
        MarketType::Xbid => proto::MarketType::Xbid,
    }
}

/// The attribution-less wire position — the `TransferResponse` path (the response already
/// identifies its transfer). The attribution fields stay empty; the snapshot and delta
/// paths set them, via [`to_wire_position_with_attribution`].
fn to_wire_position(position: &Position) -> proto::Position {
    proto::Position {
        portfolio: position.portfolio.clone(),
        market_type: to_wire_market_type(position.contract.market_type) as i32,
        contract_id: position.contract.contract_id,
        net_quantity: position.net_quantity,
        revision: position.revision,
        venue_trade_id: String::new(),
        transfer_id: String::new(),
        cl_order_id: String::new(),
        source: String::new(),
        executed_at_nanos: 0,
    }
}

/// The attributed wire position — the delta path and the snapshot's history replay: the
/// position value one trade's fold produced, plus that trade's write attribution
/// (`Option<String>` → value or empty).
fn to_wire_position_with_attribution(change: &PositionChange) -> proto::Position {
    proto::Position {
        venue_trade_id: change.venue_trade_id.clone().unwrap_or_default(),
        transfer_id: change.transfer_id.clone().unwrap_or_default(),
        cl_order_id: change.cl_order_id.clone().unwrap_or_default(),
        source: change.source.clone(),
        executed_at_nanos: change.executed_at_nanos,
        ..to_wire_position(&change.position)
    }
}
