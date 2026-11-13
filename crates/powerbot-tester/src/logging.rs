//! File logging for the tester. The ratatui TUI owns stdout, so tracing events
//! go to a **file** instead — writing them to the terminal would corrupt the
//! rendered UI. The file is a durable record of what the tester received: each
//! order and trade frame, whether it replaced an already-shown row, and when a
//! stream dropped and reconnected — the trail this repo lacked when a trade
//! showed up twice.
//!
//! Path from `POWERBOT_TESTER_LOG` (default `/tmp/powerbot-tester.log`); level
//! from `RUST_LOG` (default `info,powerbot_tester=debug`).

use std::ffi::OsStr;
use std::path::Path;

use tracing_appender::non_blocking::WorkerGuard;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::{EnvFilter, Layer, fmt};

/// Install the global tracing subscriber, writing to the log file. Never touches
/// stdout/stderr — those belong to the TUI. Returns the appender's worker guard;
/// keep it alive for the whole process or buffered lines are lost on exit.
pub fn init() -> WorkerGuard {
    let path = std::env::var("POWERBOT_TESTER_LOG")
        .unwrap_or_else(|_| "/tmp/powerbot-tester.log".to_string());
    let path = Path::new(&path);
    // Split into (dir, file) for the appender; an empty parent (bare filename)
    // means the current directory.
    let dir = path
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    let file = path
        .file_name()
        .unwrap_or_else(|| OsStr::new("powerbot-tester.log"));

    // A non-blocking writer, matching the gateway: a stalled disk never blocks
    // the render loop or a gRPC task inside `write()`; lines drop under
    // back-pressure instead.
    let (writer, guard) =
        tracing_appender::non_blocking(tracing_appender::rolling::never(dir, file));
    let filter = std::env::var("RUST_LOG").unwrap_or_else(|_| "info,powerbot_tester=debug".into());
    tracing_subscriber::registry()
        .with(
            fmt::layer()
                .with_ansi(false)
                .with_writer(writer)
                .with_filter(EnvFilter::new(filter)),
        )
        .init();
    guard
}
