//! The PowerBot order sender — the executor's one write path: a shared [`OrderSender`] over
//! `PowerbotOrderService.AddOrder`, fire-and-forget, each call stamped with the ordering
//! portfolio's id (a portfolio id *is* the PowerBot portfolio id, one identifier end to end).
//!
//! **Order discipline (invariant 5).** The synchronous `AddOrderResponse` is *advisory* — the
//! own-orders/own-trades feeds (`gateway`) are the source of truth, and nothing here trusts
//! the response beyond what it can definitively say. The [`OrderOutcome`] split mirrors what
//! the order manager must do: `Accepted` hands over the venue order id to watch on own-orders;
//! `Rejected` (the gateway's own gates — unserved portfolio, market closed, untradable
//! contract, a future risk limit — or PowerBot's definitive refusal / response errors)
//! resolves the pair immediately, feeding `ORDER_REJECTED`; and `Failed` is *ambiguous* — the
//! order may have reached the venue — so the manager treats it exactly like a resolve timeout
//! and lets the feeds carry the truth. The sender never blocks the loop: [`OrderSink::send`]
//! spawns the call on the tokio side and the classified [`OrderResponse`] returns on its own
//! channel — separate from the gateway feeds, which carry the resolution truth.

use anyhow::Result;
use tokio::sync::mpsc;
use tokio::time::Duration;
use tonic::transport::Channel;

use crate::config::FeedConfig;
use crate::executor::{ContractKey, MarketType, PortfolioId, Side};
use crate::gateway_proto as wire;
use crate::gateway_proto::powerbot_order_service_client::PowerbotOrderServiceClient;

/// Per-call deadline on `AddOrder`. Expiry is ambiguous by definition (the order may have
/// reached the venue), so it surfaces as [`OrderOutcome::Failed`] — the feeds resolve it.
const ADD_ORDER_TIMEOUT: Duration = Duration::from_secs(10);

/// One order for [`OrderSink::send`] — the engine's output, still in domain terms:
/// positive quantity with the side carrying the direction (invariant 4's `AddOrder` shape);
/// the portfolio id goes onto the wire as-is (it is the PowerBot portfolio id).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OrderRequest {
    pub portfolio: PortfolioId,
    pub contract: ContractKey,
    pub side: Side,
    /// kW, positive — the side carries the sign.
    pub quantity: i64,
    /// Limit price, minor currency units per MWh.
    pub price: i64,
    /// Client order id (nonce): the order↔trade correlation key, echoed on the resulting
    /// fills. The gateway forwards it to PowerBot as `txt`.
    pub cl_order_id: String,
}

/// How one `AddOrder` came back — the advisory response, classified (invariant 5).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum OrderOutcome {
    /// Forwarded — `order_id` is the venue id to watch on own-orders. Advisory: acceptance
    /// proves nothing about execution.
    Accepted { order_id: String },
    /// Definitively refused, never entered the venue (a gateway gate or PowerBot's response
    /// errors) — resolves the pair immediately; persistent ⇒ `ORDER_REJECTED`.
    Rejected { reason: String },
    /// Ambiguous (transport failure or deadline) — may have reached the venue, so it is
    /// treated exactly like a resolve timeout and the feeds carry the truth.
    Failed { reason: String },
}

/// One `AddOrder`'s [`OrderOutcome`] routed back to the order manager on the sender's own
/// channel: the pair it was sent for (one order in flight per pair) plus the order's
/// `cl_order_id` nonce, so the send↔outcome hop is traceable even when the response is lost
/// and the feeds resolve the order (flow E).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OrderResponse {
    pub portfolio: PortfolioId,
    pub contract: ContractKey,
    pub cl_order_id: String,
    pub outcome: OrderOutcome,
}

/// The order manager's write handle: fire one order, get nothing back synchronously (the
/// classified [`OrderResponse`] returns later on the sender's channel). Injected as a trait
/// so the manager never needs to know how a sender is built — [`OrderSender`] is the
/// production impl; a test can substitute a recording sink.
pub trait OrderSink {
    /// Place one IOC order, fire-and-forget: it never blocks the caller.
    fn send(&self, request: OrderRequest);
}

/// The shared `AddOrder` caller: one lazily-connected channel, cheap to clone, safe to call
/// from anywhere on the tokio side. [`OrderSink::send`] is fire-and-forget — the classified
/// [`OrderResponse`] returns on `responses_tx`, so the eval loop never blocks on order IO.
#[derive(Clone)]
pub struct OrderSender {
    channel: Channel,
    exchange: String,
    delivery_area_eic: String,
    responses_tx: mpsc::Sender<OrderResponse>,
}

impl OrderSender {
    /// Build the sender over a lazy channel: no IO here, and a gateway that is down at
    /// boot surfaces per-call as [`OrderOutcome::Failed`], not as a boot failure.
    pub fn new(
        endpoint: &str,
        exchange: String,
        delivery_area_eic: String,
        tuning: &FeedConfig,
        responses_tx: mpsc::Sender<OrderResponse>,
    ) -> Result<Self> {
        let channel = flow_util::feed::endpoint(endpoint, tuning)?.connect_lazy();
        Ok(Self {
            channel,
            exchange,
            delivery_area_eic,
            responses_tx,
        })
    }
}

impl OrderSink for OrderSender {
    /// The call runs on its own task (bounded by [`ADD_ORDER_TIMEOUT`]) and its classified
    /// outcome comes back as an [`OrderResponse`] for the request's pair.
    fn send(&self, request: OrderRequest) {
        let channel = self.channel.clone();
        let exchange = self.exchange.clone();
        let delivery_area_eic = self.delivery_area_eic.clone();
        let responses_tx = self.responses_tx.clone();
        tokio::spawn(async move {
            let outcome = add_order(channel, exchange, delivery_area_eic, &request).await;
            tracing::info!(cl_order_id = %request.cl_order_id, ?outcome, "order send response");
            // An error means the manager is gone; nothing left to notify.
            let _ = responses_tx
                .send(OrderResponse {
                    portfolio: request.portfolio,
                    contract: request.contract,
                    cl_order_id: request.cl_order_id,
                    outcome,
                })
                .await;
        });
    }
}

/// One `AddOrder` round trip, classified per invariant 5.
async fn add_order(
    channel: Channel,
    exchange: String,
    delivery_area_eic: String,
    request: &OrderRequest,
) -> OrderOutcome {
    tracing::info!(cl_order_id = %request.cl_order_id, ?request, "sending order to the gateway");
    let mut client = PowerbotOrderServiceClient::new(channel);
    let mut wire_request = tonic::Request::new(wire::AddOrderRequest {
        exchange,
        order: Some(wire::OrderEntry {
            portfolio_id: request.portfolio.clone(),
            delivery_area_eic,
            side: match request.side {
                Side::Buy => wire::OrderSide::Buy,
                Side::Sell => wire::OrderSide::Sell,
            } as i32,
            quantity: request.quantity,
            price: request.price,
            contract_id: request.contract.contract_id,
            market_type: match request.contract.market_type {
                MarketType::Local => wire::MarketType::Local,
                MarketType::Xbid => wire::MarketType::Xbid,
            } as i32,
            cl_order_id: request.cl_order_id.clone(),
        }),
    });
    wire_request.set_timeout(ADD_ORDER_TIMEOUT);
    match client.add_order(wire_request).await {
        Ok(response) => classify_response(response.into_inner().order),
        Err(status) => classify_status(&status),
    }
}

/// Classify a gRPC error status: the gateway's own gates reject definitively with
/// `INVALID_ARGUMENT` (bad order, unserved portfolio, PowerBot's 4xx refusal) or
/// `FAILED_PRECONDITION` (market closed, untradable contract, a future risk limit) — the
/// order did not enter the venue. Every other status is ambiguous transport.
fn classify_status(status: &tonic::Status) -> OrderOutcome {
    match status.code() {
        tonic::Code::InvalidArgument | tonic::Code::FailedPrecondition => OrderOutcome::Rejected {
            reason: status.message().to_owned(),
        },
        _ => OrderOutcome::Failed {
            reason: format!("{}: {}", status.code(), status.message()),
        },
    }
}

/// Classify an OK response: errors set is PowerBot's refusal (definitive); a response
/// carrying no order or no order id is ambiguous — something was forwarded, but there is
/// nothing to watch, so let the timeout/feeds resolve it.
fn classify_response(order: Option<wire::OwnOrder>) -> OrderOutcome {
    match order {
        None => OrderOutcome::Failed {
            reason: "gateway response carried no order".to_owned(),
        },
        Some(order) if !order.errors.is_empty() => OrderOutcome::Rejected {
            reason: order
                .errors
                .iter()
                .map(|e| format!("{}: {}", e.code, e.message))
                .collect::<Vec<_>>()
                .join("; "),
        },
        Some(order) if order.order_id.is_empty() => OrderOutcome::Failed {
            reason: "gateway response order carries no order id".to_owned(),
        },
        Some(order) => OrderOutcome::Accepted {
            order_id: order.order_id,
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn own_order(order_id: &str, errors: Vec<wire::OrderError>) -> wire::OwnOrder {
        wire::OwnOrder {
            order_id: order_id.to_owned(),
            errors,
            ..Default::default()
        }
    }

    #[test]
    fn response_classification_follows_invariant_5() {
        // A clean order is an accept carrying the id to watch
        assert_eq!(
            classify_response(Some(own_order("O-1", vec![]))),
            OrderOutcome::Accepted {
                order_id: "O-1".to_owned()
            }
        );

        // Errors set is PowerBot's definitive refusal
        assert!(matches!(
            classify_response(Some(own_order("O-1", vec![wire::OrderError {
                code: "E42".to_owned(),
                message: "limit exceeded".to_owned(),
            }]))),
            OrderOutcome::Rejected { reason } if reason.contains("E42")
        ));

        // No order / no id is ambiguous — nothing to watch, feeds and timeout resolve it
        assert!(matches!(
            classify_response(None),
            OrderOutcome::Failed { .. }
        ));
        assert!(matches!(
            classify_response(Some(own_order("", vec![]))),
            OrderOutcome::Failed { .. }
        ));

        // Gateway gates are definitive; everything else is ambiguous transport
        assert!(matches!(
            classify_status(&tonic::Status::failed_precondition("market closed")),
            OrderOutcome::Rejected { .. }
        ));
        assert!(matches!(
            classify_status(&tonic::Status::invalid_argument("bad portfolio")),
            OrderOutcome::Rejected { .. }
        ));
        assert!(matches!(
            classify_status(&tonic::Status::unavailable("gateway down")),
            OrderOutcome::Failed { .. }
        ));
        assert!(matches!(
            classify_status(&tonic::Status::deadline_exceeded("timed out")),
            OrderOutcome::Failed { .. }
        ));
    }
}
