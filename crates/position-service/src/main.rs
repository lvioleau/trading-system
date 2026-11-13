//! Boot: load the configuration, connect the durable trade log (running migrations), fold
//! it into the in-memory ledger, start one feed task per gateway subscription, then serve
//! the gRPC API until shutdown, draining gracefully. See
//! `docs/position-service-implementation.md`.

use std::collections::{BTreeSet, HashSet};
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use tokio::sync::{RwLock, broadcast, watch};
use tokio::task::JoinSet;
use tracing::{info, warn};

use position_service::channel;
use position_service::config::Config;
use position_service::feed;
use position_service::ledger::PositionLedger;
use position_service::server;
use position_service::storage::PositionStorage;

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
        portfolios = ?config.portfolios,
        listen = %config.listen_addr,
        "starting position service"
    );
    for gateway in &config.gateways {
        info!(
            name = gateway.name,
            endpoint = gateway.endpoint,
            portfolio = gateway.portfolio,
            "configured order gateway"
        );
    }

    // The DB is the durable record; memory is the serving authority. Connect (running any
    // pending migrations) and replay the whole trade log, in `seq` order, into the ledger
    // *before* serving — the fold is deterministic, so every position comes back at the
    // same revision it was last acked with.
    let storage = PositionStorage::connect(&config.database_url)
        .await
        .context("connecting position storage")?;
    let mut boot = PositionLedger::new();
    // The log is the truth: any portfolio found in it folds (conservation must hold over
    // all of it), but one missing from the configured list is a tripwire worth flagging —
    // it can no longer be transferred against or receive fills.
    let configured: HashSet<&str> = config.portfolios.iter().map(String::as_str).collect();
    let mut unconfigured = BTreeSet::new();
    for trade in storage
        .load_log()
        .await
        .context("boot-folding the trade log")?
    {
        if !configured.contains(trade.portfolio.as_str()) {
            unconfigured.insert(trade.portfolio.clone());
        }
        boot.apply(trade);
    }
    for portfolio in unconfigured {
        warn!(
            portfolio,
            "the ledger holds a portfolio that is not in the configured portfolios"
        );
    }
    info!(
        trades = boot.applied_trades(),
        positions = boot.len(),
        "boot-folded the trade log"
    );

    let ledger: channel::SharedLedger = Arc::new(RwLock::new(boot));
    // Capacity in change batches; an API subscriber that falls this far behind is re-seeded.
    let (updates, _) = broadcast::channel(1024);

    // One own-trades feed task per gateway. Each task reconnects forever with its own
    // backoff, so one gateway's outage never disturbs the others; the boot fold above ran
    // first, so the seeds overlap harmlessly on the idempotent ids.
    let mut feeds = JoinSet::new();
    for gateway in &config.gateways {
        feeds.spawn(feed::run(
            gateway.clone(),
            config.feed,
            Some(storage.clone()),
            ledger.clone(),
            updates.clone(),
        ));
    }

    let (shutdown, shutdown_rx) = watch::channel(false);
    let serve = server::serve(
        config.listen_addr,
        config.portfolios.iter().cloned().collect(),
        ledger,
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
                    result.context("draining position api")?;
                    info!("position api drained");
                }
                Err(_) => warn!(
                    grace_secs = SHUTDOWN_GRACE.as_secs(),
                    "grace period expired; dropping remaining connections"
                ),
            }
            Ok(())
        }
        // feed::run reconnects forever, so any feed task finishing means it panicked. The
        // `if` guard keeps this arm inert for a transfers-only deployment (no gateways),
        // where join_next() would otherwise resolve None immediately.
        Some(_) = feeds.join_next(), if !feeds.is_empty() => {
            anyhow::bail!("gateway feed task exited unexpectedly")
        }
        result = &mut serve => {
            result.context("serving position api")?;
            anyhow::bail!("position api exited unexpectedly")
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
