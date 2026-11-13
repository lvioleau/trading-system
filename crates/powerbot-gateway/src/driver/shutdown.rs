//! Cooperative shutdown: a process-wide flag set from a signal handler, polled by
//! the driver loops, and the [`ShutdownRequested`] sentinel they raise so the
//! reconnect machinery stops and `main` can exit cleanly. Copied from
//! nordpool-connectors' driver, minus the async watch twin — that returns with
//! the gRPC server, which will need an awaitable signal.

/// Production: a process-wide atomic flag the signal handler and the loops share across
/// threads. Set by [`request_shutdown`] (from the SIGINT/SIGTERM handler in `main`).
#[cfg(not(test))]
mod flag {
    use std::sync::atomic::{AtomicBool, Ordering};

    static SHUTDOWN: AtomicBool = AtomicBool::new(false);

    pub(super) fn set() {
        SHUTDOWN.store(true, Ordering::Relaxed);
    }
    pub(super) fn get() -> bool {
        SHUTDOWN.load(Ordering::Relaxed)
    }
}

/// Tests: a thread-local flag. libtest runs each test on its own thread, so this isolates a
/// shutdown test's writes from the rest of the parallel suite without any locking.
#[cfg(test)]
mod flag {
    use std::cell::Cell;

    thread_local! {
        static SHUTDOWN: Cell<bool> = const { Cell::new(false) };
    }

    pub(super) fn set() {
        SHUTDOWN.with(|f| f.set(true));
    }
    pub(super) fn get() -> bool {
        SHUTDOWN.with(Cell::get)
    }
    pub(super) fn clear() {
        SHUTDOWN.with(|f| f.set(false));
    }
}

/// Ask the driver to stop at the next loop iteration. Idempotent.
pub fn request_shutdown() {
    tracing::info!("shutdown requested");
    flag::set();
}

pub(crate) fn shutdown_requested() -> bool {
    flag::get()
}

/// Clear the flag (tests only) so a test thread reused by libtest starts clean.
#[cfg(test)]
pub(crate) fn reset() {
    flag::clear();
}

/// Sentinel that an attempt aborted because a shutdown was requested. It is non-recoverable
/// (so `retry_with_backoff` propagates it rather than retrying) and is translated back to a
/// graceful `Ok(())` by `main` — the one place shutdown becomes success.
#[derive(Debug, thiserror::Error)]
#[error("shutdown requested")]
pub struct ShutdownRequested;
