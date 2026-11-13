//! Boot the ID Executor: load the configuration and run the engine — the managers (gap
//! map, tradeability, order lifecycle, fill matching) around the synchronous eval loop —
//! racing it against the shutdown signal. See `docs/id-executor-implementation.md` and
//! `docs/refactor.md`.

use anyhow::Context;
use tracing::info;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::{EnvFilter, Layer, fmt};

use id_executor::config::Config;
use id_executor::execution;

/// Prefix of the per-run file mirror of the logs. Same lines as stdout, in plain text.
/// One file per run: `id-executor-<boot-unix-nanos>.log` — the same boot-nanos discriminator
/// that stamps every `cl_order_id`, so a log file and the order ids inside it share the run's
/// identity, restarts never interleave, and `ls` sorts them by start time.
/// The directory is overridable via `ID_EXECUTOR_LOG_DIR` (default `/tmp`) — the container
/// launcher points it at a bind-mounted dir so the file survives the `--rm` container.
const LOG_FILE_PREFIX: &str = "id-executor";
const DEFAULT_LOG_DIR: &str = "/tmp";

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Logs go to stdout (unchanged) and are mirrored to a file — one fresh file per run. The
    // file appender runs through a non-blocking background thread; `_log_guard` owns it and must
    // stay alive for the whole process or buffered lines are lost on exit.
    //
    // The log file is required: a log dir we cannot open (missing, read-only, wrong owner) is a
    // boot-time misconfiguration, so fail fast — exit nonzero for the supervisor to restart —
    // rather than run half-observable. We open it ourselves (`?` on the error) rather than via
    // `rolling::never`, which would *panic* instead of returning a clean error. `create_new`
    // guarantees a run never lands on a prior run's file (the boot nanos already make that
    // essentially impossible).
    let filter = || EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    let boot_nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system clock before the epoch")
        .as_nanos();
    let log_dir = std::env::var("ID_EXECUTOR_LOG_DIR").unwrap_or_else(|_| DEFAULT_LOG_DIR.into());
    let log_path =
        std::path::Path::new(&log_dir).join(format!("{LOG_FILE_PREFIX}-{boot_nanos}.log"));
    let log_file = std::fs::OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(&log_path)
        .with_context(|| format!("opening the log file {}", log_path.display()))?;
    let (file_writer, _log_guard) = tracing_appender::non_blocking(log_file);
    tracing_subscriber::registry()
        .with(
            fmt::layer()
                .with_writer(std::io::stdout)
                .with_filter(filter()),
        )
        .with(
            fmt::layer()
                .with_ansi(false)
                .with_writer(file_writer)
                .with_filter(filter()),
        )
        .init();
    info!(log_file = %log_path.display(), "logging to a per-run file");

    let config = Config::load()?;
    // Dump the whole resolved config via its Debug impl — every field (endpoints, delivery
    // area, portfolios, and the feed/execution tuning), so shipped configs are fully visible
    // in the boot log. Each portfolio id is also its PowerBot portfolio id — one identifier
    // end to end.
    info!(?config, "starting id-executor");

    let engine = execution::run(config);
    tokio::pin!(engine);
    tokio::select! {
        signal = shutdown_signal() => {
            info!(signal, "shutdown requested");
            Ok(())
        }
        // The engine resolves only to a fatal error — a permanent config mismatch, a dead
        // task, or a closed channel; exit nonzero so the supervisor restarts us.
        error = &mut engine => Err(error.context("the executor engine stopped")),
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
