use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use tokio::sync::{RwLock, broadcast, watch};
use tracing::{info, warn};

use contract_service::channel;
use contract_service::config::Config;
use contract_service::feed;
use contract_service::server;
use contract_service::storage::ContractStorage;
use contract_service::store::ContractStore;

/// How long a drain may take after a shutdown signal — enough for subscriber streams to end
/// and in-flight requests to complete — before remaining connections are dropped.
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
        endpoint = %config.connector_endpoint,
        areas = ?config.delivery_area_eics,
        listen = %config.listen_addr,
        "starting contract service"
    );

    // The DB is the durable record; memory is the serving authority. Connect (running any
    // pending migrations) and fold the whole contracts table into the index *before*
    // subscribing — the feed's re-seed then overlaps harmlessly on the updated_at guards.
    let storage = ContractStorage::connect(&config.database_url)
        .await
        .context("connecting contract storage")?;
    let mut boot = ContractStore::new();
    for contract in storage.load_all().await.context("boot-folding contracts")? {
        boot.upsert(contract);
    }
    info!(contracts = boot.len(), "boot-folded the contracts table");

    let store: channel::SharedContracts = Arc::new(RwLock::new(boot));
    // Capacity in change batches; an API subscriber that falls this far behind is re-seeded.
    let (updates, _) = broadcast::channel(1024);
    let feed = tokio::spawn(feed::run(
        config.connector_endpoint.clone(),
        config.delivery_area_eics.clone(),
        config.feed,
        Some(storage.clone()),
        store.clone(),
        updates.clone(),
    ));

    let (shutdown, shutdown_rx) = watch::channel(false);
    let serve = server::serve(
        config.listen_addr,
        store.clone(),
        updates,
        Some(storage),
        shutdown_rx,
    );
    tokio::pin!(serve);

    tokio::select! {
        signal = shutdown_signal() => {
            info!(signal, "shutdown requested; draining");
            let _ = shutdown.send(true);
            match tokio::time::timeout(SHUTDOWN_GRACE, &mut serve).await {
                Ok(result) => {
                    result.context("draining contract api")?;
                    info!("contract api drained");
                }
                Err(_) => warn!(
                    grace_secs = SHUTDOWN_GRACE.as_secs(),
                    "grace period expired; dropping remaining connections"
                ),
            }
            Ok(())
        }
        // feed::run reconnects forever, so the task finishing at all means it panicked.
        _ = feed => anyhow::bail!("contract feed task exited unexpectedly"),
        result = &mut serve => {
            result.context("serving contract api")?;
            anyhow::bail!("contract api exited unexpectedly")
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
