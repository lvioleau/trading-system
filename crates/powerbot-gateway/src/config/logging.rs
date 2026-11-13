//! Logging setup. The level filter comes from `RUST_LOG` (default
//! `info,powerbot_gateway=debug`); every event is emitted as JSON (one object per line,
//! fields flattened to top-level keys) to stdout for a log shipper to parse. Settings come
//! straight from the environment, not the INI, because tracing is installed before the config
//! file / secrets load — so the load itself can be logged.

use tracing_appender::non_blocking::WorkerGuard;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::{EnvFilter, Layer, fmt};

/// Install the global tracing subscriber: JSON to stdout, level from `RUST_LOG` (default
/// `info,powerbot_gateway=debug`).
///
/// Stdout runs through a non-blocking (lossy) writer: a background thread drains a bounded
/// queue, and lines are *dropped* under back-pressure rather than blocking the caller. This is
/// load-bearing — a stalled stdout consumer (a paused terminal back-pressuring the pipe, or a
/// wedged log driver) would otherwise block whatever thread is logging inside `write()`, and
/// since the ws driver logs on its hot path that freezes the whole gateway. The returned guard
/// owns the background thread; keep it alive for the whole process or buffered lines are lost
/// on exit.
pub fn init() -> WorkerGuard {
    let filter = std::env::var("RUST_LOG").unwrap_or_else(|_| "info,powerbot_gateway=debug".into());
    let (writer, guard) = tracing_appender::non_blocking(std::io::stdout());
    tracing_subscriber::registry()
        .with(
            fmt::layer()
                .json()
                .flatten_event(true)
                .with_writer(writer)
                .with_filter(EnvFilter::new(filter)),
        )
        .init();
    guard
}
