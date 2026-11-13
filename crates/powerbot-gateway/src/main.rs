//! The PowerBot gateway process — see `docs/design.md`.
//!
//! Two runtimes, one process (the design's "Two runtimes, one process"):
//! a dedicated OS thread runs the sync websocket driver (connect → subscribe →
//! read → reconnect), and a tokio runtime serves the order gRPC service. The
//! two are bridged by a `watch<MarketState>` the driver publishes and the order
//! service gates order entry on — so an order the exchange would refuse is
//! rejected locally before the REST round trip.
//!
//! Configuration is one INI file, `powerbot.ini` (path via `POWERBOT_CONFIG`); see
//! [`config`](powerbot_gateway::config) and the shipped file for the keys.

use std::sync::Arc;

use anyhow::{Context, anyhow};
use powerbot_models::models::MarketState;
use tokio::sync::{mpsc, watch};

use powerbot_gateway::config::{self, Config};
use powerbot_gateway::contracts::{self, ContractPollConfig, ContractStore};
use powerbot_gateway::driver::{self, DriverConfig};
use powerbot_gateway::grpc::{self, ExecutionChannels};
use powerbot_gateway::market::{self, MarketPollConfig};
use powerbot_gateway::metrics;
use powerbot_gateway::reconcile::{self, ReconcileConfig};
use powerbot_gateway::rest::RestClient;
use powerbot_gateway::sink::GatewaySink;
use powerbot_gateway::storage::{ExecutionStorage, Persist};
use powerbot_gateway::transport::{self, TransportConfig};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Install JSON-to-stdout logging first, before the config load, so the load itself is
    // logged; the guard owns the non-blocking writer thread and must outlive the process.
    let _log_guard = config::logging::init();

    let cfg = Config::load().context("loading configuration")?;

    // Install the global DogStatsD recorder before anything emits, so the `powerbot.*` macros
    // are live from the first frame (docs/design.md, "Observability").
    metrics::init(&cfg.metrics.dogstatsd_addr, &cfg.metrics.tags)
        .context("initializing metrics")?;

    let gw = &cfg.gateway;

    let ws_url = gw.wss_url.clone();
    let api_key = gw.api_key.clone();
    let grpc_addr = cfg.server.listen_addr;
    let host = ws_url
        .host_str()
        .context("[gateway] wss_url has no host")?
        .to_string();

    let driver_config = DriverConfig {
        host,
        exchange: gw.exchange.clone(),
        delivery_area_eic: gw.delivery_area_eic.clone(),
        portfolio_ids: gw.portfolio_ids.clone(),
        connected_timeout: gw.connected_timeout,
        initial_backoff: gw.initial_backoff,
        max_backoff: gw.max_backoff,
        backoff_reset_after: gw.backoff_reset_after,
    };
    let transport_config = TransportConfig {
        handshake_timeout: gw.handshake_timeout,
        read_timeout: gw.read_timeout,
        write_timeout: gw.write_timeout,
        heartbeat_interval: gw.heartbeat_interval,
        heartbeat_timeout: gw.heartbeat_timeout,
    };

    let rest = RestClient::new(&gw.rest_url, &api_key).context("building PowerBot REST client")?;

    // Latest market state, read by the order service. Two writers: the driver
    // thread pushes each `marketstatus` transition as it arrives, and the REST
    // market poll re-publishes the polled state each tick. `None` until the
    // first update from either — and again whenever the session drops, until
    // the next poll or the new session's first push refills it.
    let (market_tx, market_rx) = watch::channel::<Option<MarketState>>(None);

    // Contract tradability store, read by the order service's contract gate.
    // Two writers, like the market watch: the driver thread applies each
    // `contracts` feed event, and the REST sweep seeds and re-baselines it
    // (including the trading windows the feed doesn't carry).
    let contract_store = ContractStore::default();

    // Durable execution record is required (M7): connect the `execution` schema up front so a
    // misconfigured database fails fast at startup rather than silently dropping the record.
    let storage = ExecutionStorage::connect(&cfg.storage.database_url)
        .await
        .context("connecting execution storage")?;

    // Live-persistence queue: the ws driver enqueues each batch here (non-blocking), a task
    // drains it to Postgres.
    let (persist_tx, persist_rx) = mpsc::channel::<Persist>(cfg.storage.persist_buffer);

    // Execution stores + feeds: the driver thread applies each decoded batch to
    // the in-memory stores, broadcasts the delta, and enqueues it for
    // persistence; the gRPC execution service streams it. Shared as one `Arc`.
    // On a shadow instance, internal trades (shadow executions) are additionally
    // normalized onto the own-trades store/stream under this gateway's identity.
    let mut channels =
        ExecutionChannels::new(cfg.server.broadcast_capacity).with_persistence(persist_tx);
    if cfg.gateway.shadow {
        channels = channels.with_shadow(grpc::ShadowNormalization {
            exchange: cfg.gateway.exchange.clone(),
            delivery_area_eic: cfg.gateway.delivery_area_eic.clone(),
            portfolio_ids: cfg.gateway.portfolio_ids.clone(),
        });
    }
    let channels = Arc::new(channels);

    // Shutdown fan-out: flipped to `true` by the signal handler or when the
    // driver thread exits, whichever comes first. The gRPC server drains on it,
    // and the signal handler also stops the driver via `request_shutdown`.
    let (shutdown_tx, shutdown_rx) = watch::channel(false);

    let shutdown_tx_sig = shutdown_tx.clone();
    ctrlc::set_handler(move || {
        driver::request_shutdown();
        let _ = shutdown_tx_sig.send(true);
    })
    .context("install signal handler")?;

    // Live persistence: drain the queue to Postgres as batches arrive (the
    // low-latency companion to the reconciliation sweep). Its final flush is
    // gated on `persist_drain`, fired only after the driver thread — the sole
    // producer — has been joined, so no batch can be enqueued behind the flush.
    let (persist_drain_tx, persist_drain_rx) = watch::channel(false);
    let persist_task = tokio::spawn(
        storage
            .clone()
            .run_persistence(persist_rx, persist_drain_rx),
    );

    // The websocket driver on its own OS thread: sync, blocking, so it never
    // competes with the tokio workers for scheduling (docs/design.md).
    let ws_key = api_key.clone();
    let shutdown_tx_driver = shutdown_tx;
    let sink = GatewaySink::new(
        market_tx.clone(),
        contract_store.clone(),
        Arc::clone(&channels),
    );
    tracing::info!(version = metrics::VERSION, %ws_url, exchange = %driver_config.exchange, shadow = cfg.gateway.shadow, %grpc_addr, "starting PowerBot gateway");
    let driver_thread = std::thread::Builder::new()
        .name("powerbot-ws".into())
        .spawn(move || {
            let connect = || {
                transport::connect(&ws_url, &ws_key, transport_config.clone()).map_err(Into::into)
            };
            let result = driver::run_with_reconnect(&driver_config, connect, sink);
            // Wake the gRPC drain if the driver exits on its own (fatal error).
            let _ = shutdown_tx_driver.send(true);
            result
        })
        .context("spawning ws driver thread")?;

    // Reconciliation: seed the stores from REST at startup, then re-sweep and
    // evict on an interval — the stores' self-correction for what the lossy
    // websocket drops (docs/design.md). Runs on the tokio runtime alongside the
    // server; stops itself on shutdown.
    tokio::spawn(reconcile::run(
        rest.clone(),
        Arc::clone(&channels),
        Some(storage),
        ReconcileConfig {
            exchange: gw.exchange.clone(),
            portfolio_ids: gw.portfolio_ids.clone(),
            interval: gw.reconcile_interval,
            trade_window: gw.trade_window,
            eviction_grace: gw.eviction_grace,
        },
        shutdown_rx.clone(),
    ));

    // Contract sweep: seed the contract store from REST at startup, then
    // re-fetch and evict on an interval — the source of the trading windows
    // and the self-correction for feed drops; the `contracts` feed keeps the
    // store live in between (docs/design.md, "Contract tradability").
    tokio::spawn(contracts::run(
        rest.clone(),
        contract_store.clone(),
        ContractPollConfig {
            exchange: gw.exchange.clone(),
            delivery_area_eic: gw.delivery_area_eic.clone(),
            interval: gw.contract_poll_interval,
            eviction_grace: gw.eviction_grace,
        },
        shutdown_rx.clone(),
    ));

    // Market poll: periodically fetch the full market/server status over REST,
    // log it, and export the `powerbot.market.*` metrics. Each successful poll
    // also publishes into the market-state watch — the websocket feed's backup
    // (startup seed, and at-most-one-interval-stale cover across a ws outage).
    tokio::spawn(market::run(
        rest.clone(),
        MarketPollConfig {
            exchange: gw.exchange.clone(),
            interval: gw.market_poll_interval,
        },
        market_tx,
        shutdown_rx.clone(),
    ));

    // Serve until shutdown is requested (signal) or the driver thread exits;
    // `shutdown_rx` also cuts every trade stream so the drain can complete.
    let serve_result = grpc::serve(
        grpc_addr,
        rest,
        market_rx,
        contract_store,
        channels,
        gw.portfolio_ids.clone(),
        cfg.server.max_subscribers,
        shutdown_rx,
    )
    .await;

    // Whichever side ended, stop the other and reap the driver thread.
    driver::request_shutdown();
    let driver_result = driver_thread
        .join()
        .unwrap_or_else(|_| Err(anyhow!("ws driver thread panicked")));

    // The driver has exited, so nothing can enqueue behind the persistence
    // task's final flush any more: let it flush and wait for it to finish.
    let _ = persist_drain_tx.send(true);
    if let Err(e) = persist_task.await {
        tracing::warn!(error = %e, "live persistence task failed");
    }

    // Surface a real error from either side; a driver `ShutdownRequested` is the
    // expected clean stop, not a failure.
    serve_result.context("order gRPC server")?;
    match driver_result {
        Err(e) if e.is::<driver::ShutdownRequested>() => {
            tracing::info!("shutdown complete");
            Ok(())
        }
        other => other,
    }
}
