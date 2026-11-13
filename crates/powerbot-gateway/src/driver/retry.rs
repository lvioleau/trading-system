//! Generic exponential-backoff retry loop, decoupled from what is being retried — it knows
//! nothing about transports, sessions, or the driver's error taxonomy; callers inject those.
//! Copied from nordpool-connectors' driver.
//!
//! [`retry_with_backoff`] runs an attempt repeatedly with growing backoff until it returns
//! an error, which is propagated to stop the loop.

use std::time::{Duration, Instant};

use anyhow::Result;

/// Exponential backoff policy; values come from [`DriverConfig`](super::DriverConfig).
pub(crate) struct Backoff {
    /// Wait before the first retry, and the value the backoff resets to.
    pub initial: Duration,
    /// Upper bound the wait grows toward, doubling after each failed attempt.
    pub max: Duration,
    /// An attempt that ran at least this long is treated as healthy: the backoff resets
    /// to `initial` afterwards, so a long-lived session that finally drops doesn't inherit
    /// a grown delay.
    pub reset_after: Duration,
}

pub(crate) fn retry_with_backoff<A, P>(
    backoff: &Backoff,
    mut sleep: P,
    mut attempt: A,
) -> Result<()>
where
    A: FnMut() -> Result<()>,
    P: FnMut(Duration),
{
    let mut wait = backoff.initial;
    loop {
        let started = Instant::now();
        attempt()?;
        if started.elapsed() >= backoff.reset_after {
            wait = backoff.initial;
        }
        tracing::warn!(
            backoff_ms = wait.as_millis() as u64,
            "attempt ended, retrying after backoff"
        );
        sleep(wait);
        wait = (wait * 2).min(backoff.max);
    }
}
