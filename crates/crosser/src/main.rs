//! Boot the crosser: load the configuration and run the crossing engine — the strategy feed,
//! the depth feed, the Position Service subscriptions, and the crossing loop reconciling actual
//! positions to the latest targets — until shutdown or a fatal error.

use tracing::info;

use crosser::config::Config;
use crosser::crossing;

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
        strategy_endpoint = config.strategy_endpoint,
        position_endpoint = config.position_endpoint,
        connector_endpoint = config.connector_endpoint,
        delivery_area_eic = %config.delivery_area_eic,
        portfolios = ?config.portfolios,
        listen = %config.listen_addr,
        "starting crosser"
    );

    tokio::select! {
        signal = shutdown_signal() => {
            info!(signal, "shutdown requested");
            Ok(())
        }
        error = crossing::run(config) => Err(error.context("crosser failed")),
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
