//! Background gRPC work: five subscription tasks (contracts, depth, own-orders,
//! own-trades, internal-trades) that each reconnect on failure, and an
//! order-entry task that turns [`OrderCommand`]s into `AddOrder` calls.
//! Everything they learn is pushed to the UI as an [`AppEvent`] over one channel,
//! so the render loop drives a single receiver.

use std::time::Duration;

use futures_util::StreamExt;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};

use crate::config::Config;
use crate::proto::gateway::powerbot_execution_service_client::PowerbotExecutionServiceClient;
use crate::proto::gateway::powerbot_order_service_client::PowerbotOrderServiceClient;
use crate::proto::gateway::{
    AddOrderRequest, OrderEntry, OwnOrder, SubscribeOwnOrdersRequest, SubscribeOwnTradesRequest,
    Trade,
};
use crate::proto::marketdata::contract_service_client::ContractServiceClient;
use crate::proto::marketdata::depth_book_service_client::DepthBookServiceClient;
use crate::proto::marketdata::{
    Contract, DepthView, SubscribeContractsRequest, SubscribeDepthRequest,
};
use crate::proto::position::position_service_client::PositionServiceClient;
use crate::proto::position::{Position, SubscribeRequest};

/// Which server a connection-status update is about.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Conn {
    Gateway,
    Connector,
    Position,
}

/// Connection state shown as a status light per server.
#[derive(Clone, PartialEq, Eq)]
pub enum ConnState {
    Connecting,
    Connected,
    Disconnected(String),
}

/// Everything the background tasks push to the UI, merged onto one channel.
pub enum AppEvent {
    Contracts(Vec<Contract>),
    Depth(Vec<DepthView>),
    Orders {
        received_at_nanos: i64,
        /// When this tester received the gRPC frame (unix-nanos), for latency.
        ui_recv_nanos: i64,
        orders: Vec<OwnOrder>,
    },
    Trades {
        received_at_nanos: i64,
        ui_recv_nanos: i64,
        trades: Vec<Trade>,
    },
    /// A Position Service batch for one tracked portfolio: the snapshot on
    /// subscribe, then each changed position. Every `Position` is a full current
    /// value the UI keeps by highest revision.
    Positions {
        /// The portfolio this batch was subscribed under.
        portfolio: String,
        positions: Vec<Position>,
    },
    /// Result of an `AddOrder` the user sent: the relayed OwnOrder or an error.
    OrderResult(Result<OwnOrder, String>),
    Status(Conn, ConnState),
}

/// A send request from the order ticket, handed to the order-entry task.
pub struct OrderCommand {
    /// Bare nordpool contract id and its market type; the gateway rejoins them
    /// into PowerBot's prefixed id (NX_/NI_).
    pub contract_id: i64,
    pub market_type: i32,
    pub side: i32,
    /// Limit price in minor currency units per MWh (currency/MWh ×100), as the
    /// gateway's `OrderEntry` expects.
    pub price: i64,
    /// Quantity in kilowatts (MW ×1000), as the gateway's `OrderEntry` expects.
    pub quantity: i64,
    pub txt: String,
}

/// How long to wait before re-dialing after a stream drops or connect fails.
const RECONNECT_DELAY: Duration = Duration::from_secs(2);

/// Spawn every background task. Returns the sender the order ticket uses to
/// request `AddOrder` calls.
pub fn spawn_all(cfg: &Config, tx: UnboundedSender<AppEvent>) -> UnboundedSender<OrderCommand> {
    tokio::spawn(run_contracts(cfg.clone(), tx.clone()));
    tokio::spawn(run_depth(cfg.clone(), tx.clone()));
    tokio::spawn(run_own_orders(cfg.clone(), tx.clone()));
    tokio::spawn(run_own_trades(cfg.clone(), tx.clone()));
    // One Position Service subscription per tracked portfolio.
    for portfolio in &cfg.position_portfolios {
        tokio::spawn(run_positions(cfg.clone(), portfolio.clone(), tx.clone()));
    }

    let (order_tx, order_rx) = tokio::sync::mpsc::unbounded_channel();
    tokio::spawn(run_order_sender(cfg.clone(), order_rx, tx));
    order_tx
}

/// Subscribe to the connector's contract feed, filtered to our delivery area,
/// forwarding each batch of upserts. Reconnects forever.
async fn run_contracts(cfg: Config, tx: UnboundedSender<AppEvent>) {
    loop {
        let _ = tx.send(AppEvent::Status(Conn::Connector, ConnState::Connecting));
        let attempt = async {
            let mut client = ContractServiceClient::connect(cfg.connector_addr.clone()).await?;
            let mut stream = client
                .subscribe_contracts(SubscribeContractsRequest {
                    eic_codes: vec![cfg.delivery_area_eic.to_string()],
                })
                .await?
                .into_inner();
            let _ = tx.send(AppEvent::Status(Conn::Connector, ConnState::Connected));
            while let Some(item) = stream.next().await {
                let update = item?;
                let _ = tx.send(AppEvent::Contracts(update.contracts));
            }
            Ok::<(), anyhow::Error>(())
        }
        .await;
        report(&tx, Conn::Connector, attempt);
        tokio::time::sleep(RECONNECT_DELAY).await;
    }
}

/// Subscribe to the connector's depth feed for our delivery area, forwarding each
/// batch of changed books. Reconnects forever.
async fn run_depth(cfg: Config, tx: UnboundedSender<AppEvent>) {
    loop {
        let attempt = async {
            let mut client = DepthBookServiceClient::connect(cfg.connector_addr.clone()).await?;
            let mut stream = client
                .subscribe(SubscribeDepthRequest {
                    eic_code: cfg.delivery_area_eic.to_string(),
                })
                .await?
                .into_inner();
            while let Some(item) = stream.next().await {
                let update = item?;
                let _ = tx.send(AppEvent::Depth(update.views));
            }
            Ok::<(), anyhow::Error>(())
        }
        .await;
        // Depth shares the connector light with contracts; only report failures.
        if let Err(e) = attempt {
            let _ = tx.send(AppEvent::Status(
                Conn::Connector,
                ConnState::Disconnected(e.to_string()),
            ));
        }
        tokio::time::sleep(RECONNECT_DELAY).await;
    }
}

/// Subscribe to the gateway's own-order execution feed. Reconnects forever.
async fn run_own_orders(cfg: Config, tx: UnboundedSender<AppEvent>) {
    loop {
        let _ = tx.send(AppEvent::Status(Conn::Gateway, ConnState::Connecting));
        let attempt = async {
            let mut client =
                PowerbotExecutionServiceClient::connect(cfg.gateway_addr.clone()).await?;
            let mut stream = client
                .subscribe_own_orders(SubscribeOwnOrdersRequest {
                    portfolio_id: cfg.portfolio_id.clone(),
                })
                .await?
                .into_inner();
            let _ = tx.send(AppEvent::Status(Conn::Gateway, ConnState::Connected));
            while let Some(item) = stream.next().await {
                let update = item?;
                let _ = tx.send(AppEvent::Orders {
                    received_at_nanos: update.received_at_nanos,
                    ui_recv_nanos: crate::app::now_nanos(),
                    orders: update.orders,
                });
            }
            Ok::<(), anyhow::Error>(())
        }
        .await;
        report(&tx, Conn::Gateway, attempt);
        tokio::time::sleep(RECONNECT_DELAY).await;
    }
}

/// Subscribe to the gateway's external own-trade execution feed. Reconnects
/// forever. It shares the gateway light with the own-orders task, so it only
/// reports errors.
async fn run_own_trades(cfg: Config, tx: UnboundedSender<AppEvent>) {
    loop {
        let attempt = async {
            let mut client =
                PowerbotExecutionServiceClient::connect(cfg.gateway_addr.clone()).await?;
            let mut stream = client
                .subscribe_own_trades(SubscribeOwnTradesRequest {
                    portfolio_id: cfg.portfolio_id.clone(),
                })
                .await?
                .into_inner();
            tracing::info!(portfolio = %cfg.portfolio_id, "own-trades stream subscribed");
            while let Some(item) = stream.next().await {
                let update = item?;
                let _ = tx.send(AppEvent::Trades {
                    received_at_nanos: update.received_at_nanos,
                    ui_recv_nanos: crate::app::now_nanos(),
                    trades: update.trades,
                });
            }
            Ok::<(), anyhow::Error>(())
        }
        .await;
        report_error_only(&tx, Conn::Gateway, "own-trades", attempt);
        tokio::time::sleep(RECONNECT_DELAY).await;
    }
}

/// Subscribe to the Position Service for one tracked portfolio, forwarding each
/// batch (snapshot then changes) to the UI. Reconnects forever. The first
/// subscription drives the position light; the rest only report errors, so one
/// light reflects the shared endpoint.
async fn run_positions(cfg: Config, portfolio: String, tx: UnboundedSender<AppEvent>) {
    // Only the first configured portfolio owns the status light, so the others
    // don't clobber a healthy "connected" with their own connecting/errors.
    let owns_light = cfg.position_portfolios.first() == Some(&portfolio);
    loop {
        if owns_light {
            let _ = tx.send(AppEvent::Status(Conn::Position, ConnState::Connecting));
        }
        let attempt = async {
            let mut client = PositionServiceClient::connect(cfg.position_addr.clone()).await?;
            let mut stream = client
                .subscribe(SubscribeRequest {
                    portfolio: portfolio.clone(),
                })
                .await?
                .into_inner();
            if owns_light {
                let _ = tx.send(AppEvent::Status(Conn::Position, ConnState::Connected));
            }
            while let Some(item) = stream.next().await {
                let update = item?;
                let _ = tx.send(AppEvent::Positions {
                    portfolio: portfolio.clone(),
                    positions: update.positions,
                });
            }
            Ok::<(), anyhow::Error>(())
        }
        .await;
        if owns_light {
            report(&tx, Conn::Position, attempt);
        } else if let Err(e) = attempt {
            let _ = tx.send(AppEvent::Status(
                Conn::Position,
                ConnState::Disconnected(e.to_string()),
            ));
        }
        tokio::time::sleep(RECONNECT_DELAY).await;
    }
}

/// Turn each queued [`OrderCommand`] into an `AddOrder` call, relaying the result
/// (or error string) back to the UI. Dials per-order — cheap enough for a manual
/// test tool and it means a dead gateway never wedges the queue.
async fn run_order_sender(
    cfg: Config,
    mut rx: UnboundedReceiver<OrderCommand>,
    tx: UnboundedSender<AppEvent>,
) {
    while let Some(cmd) = rx.recv().await {
        let result = send_one(&cfg, &cmd).await;
        let _ = tx.send(AppEvent::OrderResult(result.map_err(|e| e.to_string())));
    }
}

async fn send_one(cfg: &Config, cmd: &OrderCommand) -> anyhow::Result<OwnOrder> {
    let mut client = PowerbotOrderServiceClient::connect(cfg.gateway_addr.clone()).await?;
    let entry = OrderEntry {
        portfolio_id: cfg.portfolio_id.clone(),
        delivery_area_eic: cfg.delivery_area_eic.to_string(),
        side: cmd.side,
        quantity: cmd.quantity,
        price: cmd.price,
        contract_id: cmd.contract_id,
        market_type: cmd.market_type,
        cl_order_id: cmd.txt.clone(),
    };
    let resp = client
        .add_order(AddOrderRequest {
            exchange: cfg.exchange.clone(),
            order: Some(entry),
        })
        .await?
        .into_inner();
    resp.order
        .ok_or_else(|| anyhow::anyhow!("gateway returned an empty AddOrderResponse"))
}

/// Push the terminal state of a subscription attempt as a status light update.
fn report(tx: &UnboundedSender<AppEvent>, conn: Conn, attempt: anyhow::Result<()>) {
    let msg = match attempt {
        Ok(()) => "stream ended".to_string(),
        Err(e) => e.to_string(),
    };
    tracing::warn!(?conn, reason = %msg, "stream ended; reconnecting");
    let _ = tx.send(AppEvent::Status(conn, ConnState::Disconnected(msg)));
}

/// Like [`report`] but for streams that share a status light with a sibling (so a
/// healthy peer isn't clobbered by this one's blip): always log the drop — the
/// audit trail wants every reconnect — but only push a status change on error.
fn report_error_only(
    tx: &UnboundedSender<AppEvent>,
    conn: Conn,
    stream: &str,
    attempt: anyhow::Result<()>,
) {
    match attempt {
        Ok(()) => tracing::warn!(?conn, stream, "stream ended; reconnecting"),
        Err(e) => {
            tracing::warn!(?conn, stream, error = %e, "stream dropped; reconnecting");
            let _ = tx.send(AppEvent::Status(
                conn,
                ConnState::Disconnected(e.to_string()),
            ));
        }
    }
}
