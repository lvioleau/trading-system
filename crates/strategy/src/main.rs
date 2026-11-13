//! Boot the strategy cradle: load the configuration, subscribe to the Contract Service (the
//! live contract set), the connector's depth stream (the input's books and the emitted mids),
//! and the Position Service (the input's actual-position baseline), run the cradle around the
//! configured Python strategy module, then serve the gRPC API until shutdown, draining
//! gracefully. See `docs/crosser-implementation.md` step 1.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::sync::{broadcast, watch};
use tracing::{info, warn};

use strategy::config::Config;
use strategy::contract_feed;
use strategy::depth_feed;
use strategy::driver;
use strategy::position_feed;
use strategy::py_strategy::PyStrategy;
use strategy::server;

/// How long a drain may take after a shutdown signal — enough for subscriber streams to end —
/// before remaining connections are dropped.
const SHUTDOWN_GRACE: Duration = Duration::from_secs(10);

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();

    let config = Config::load()?;
    info!(
        strategy = config.strategy,
        portfolio = config.portfolio,
        listen = %config.listen_addr,
        contract_endpoint = config.contract_endpoint,
        connector_endpoint = config.connector_endpoint,
        position_endpoint = config.position_endpoint,
        delivery_area_eic = %config.delivery_area_eic,
        contracts = config.contracts,
        amplitude = config.amplitude,
        min_time_to_delivery_ms = config.min_time_to_delivery.as_millis(),
        tick_ms = config.tick.as_millis(),
        python = config.python,
        "starting strategy cradle"
    );

    // The strategy proper behind the cradle's seam: the configured Python module — what to
    // trade is its decision, tuned by the config's knobs. Refusing to initialize (a load
    // failure, or strategy_create returning None) is a fatal boot error.
    let strategy = PyStrategy::new(
        &config.python,
        config.amplitude,
        config.contracts,
        config.min_time_to_delivery.as_millis() as i64,
    )?;

    // The cradle publishes to the broadcast; each subscription reads a filtered view. Capacity in
    // records; a subscriber that falls this far behind is dropped and reconnects.
    let (records, _) = broadcast::channel(1024);

    // The three upstream views the cradle reads each tick, each maintained by a feed task that
    // reconnects forever, so all are spawned detached: the live tradable set, every live book
    // (the input's market view and the emitted mids), and the portfolio's actual positions (the
    // input's baseline).
    let contracts: driver::SharedContracts = Arc::new(Mutex::new(HashMap::new()));
    tokio::spawn(contract_feed::run(
        config.contract_endpoint,
        config.delivery_area_eic,
        contracts.clone(),
    ));
    let books: driver::SharedBooks = Arc::new(Mutex::new(HashMap::new()));
    tokio::spawn(depth_feed::run(
        config.connector_endpoint,
        config.delivery_area_eic,
        books.clone(),
    ));
    let positions: driver::SharedPositions = Arc::new(Mutex::new(None));
    tokio::spawn(position_feed::run(
        config.position_endpoint,
        config.portfolio.clone(),
        positions.clone(),
    ));
    let mut driver = tokio::spawn(driver::run_feed(
        config.portfolio.clone(),
        strategy,
        driver::Sources {
            contracts,
            depth: books,
            positions,
        },
        config.tick,
        records.clone(),
    ));

    let (shutdown, shutdown_rx) = watch::channel(false);
    let serve = server::serve(
        config.listen_addr,
        HashSet::from([config.portfolio]),
        records,
        shutdown_rx,
    );
    tokio::pin!(serve);

    tokio::select! {
        signal = shutdown_signal() => {
            info!(signal, "shutdown requested; draining");
            let _ = shutdown.send(true);
            match tokio::time::timeout(SHUTDOWN_GRACE, &mut serve).await {
                Ok(result) => {
                    result?;
                    info!("strategy api drained");
                }
                Err(_) => warn!(
                    grace_secs = SHUTDOWN_GRACE.as_secs(),
                    "grace period expired; dropping remaining connections"
                ),
            }
            Ok(())
        }
        // The driver loops forever; finishing means it panicked.
        _ = &mut driver => {
            anyhow::bail!("strategy driver task exited unexpectedly")
        }
        result = &mut serve => {
            result?;
            anyhow::bail!("strategy api exited unexpectedly")
        }
    }
}

/// Resolves when the process is asked to stop: SIGINT (ctrl-c) or SIGTERM — what Docker and
/// Kubernetes send first.
async fn shutdown_signal() -> &'static str {
    let mut sigterm = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        .expect("installing SIGTERM handler");
    tokio::select! {
        _ = tokio::signal::ctrl_c() => "SIGINT",
        _ = sigterm.recv() => "SIGTERM",
    }
}
