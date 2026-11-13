//! The reconnect mechanics under [`FeedConfig`](crate::config::FeedConfig): the
//! keepalive-configured endpoint every subscription dials through, and the exponential
//! backoff-with-stable-reset policy every reconnect loop sleeps by.
//!
//! The loops themselves stay in the consuming crates — they genuinely differ (failure
//! escalation, fatal-versus-retry classification, seed/delta disciplines). What is shared
//! here is pure policy: how an endpoint keeps a quiet stream from hanging forever, and how
//! long to wait before dialing again.

use std::time::Duration;

use anyhow::{Context, Result};
use tonic::transport::Endpoint;

use crate::config::FeedConfig;

/// A tonic endpoint for `url` with the keepalive tuning applied: HTTP/2 PINGs (even while
/// idle) so an upstream that vanished without closing (network partition, hard power-off)
/// leaves the stream erroring into the reconnect loop instead of waiting forever. Callers
/// `.connect().await` it (or hand it to a lazily-connecting client).
pub fn endpoint(url: &str, tuning: &FeedConfig) -> Result<Endpoint> {
    Ok(Endpoint::from_shared(url.to_owned())
        .with_context(|| format!("invalid endpoint url {url:?}"))?
        .http2_keep_alive_interval(tuning.keepalive_interval)
        .keep_alive_timeout(tuning.keepalive_timeout)
        .keep_alive_while_idle(true))
}

/// The reconnect delay policy: exponential from `initial_backoff`, doubling per attempt up
/// to `max_backoff`, reset to the initial once a session has proven stable (lived at least
/// `stable_session`) — so a flapping upstream backs off, while a long-lived session's
/// eventual drop reconnects promptly.
///
/// Usage in a session loop:
///
/// ```ignore
/// let mut backoff = Backoff::new(&tuning);
/// loop {
///     let session_start = Instant::now();
///     run_session(...).await; // however the crate classifies the outcome
///     backoff.session_ended(session_start.elapsed());
///     tokio::time::sleep(backoff.next_delay()).await;
/// }
/// ```
#[derive(Debug)]
pub struct Backoff {
    initial: Duration,
    max: Duration,
    stable_session: Duration,
    next: Duration,
}

impl Backoff {
    pub fn new(tuning: &FeedConfig) -> Self {
        Self {
            initial: tuning.initial_backoff,
            max: tuning.max_backoff,
            stable_session: tuning.stable_session,
            next: tuning.initial_backoff,
        }
    }

    /// Record that a session ended after living `session_length`: a stable session resets
    /// the backoff to the initial delay. Returns whether it was stable, so callers can
    /// reset their own per-session state (failure counters and the like) on the same
    /// judgment.
    pub fn session_ended(&mut self, session_length: Duration) -> bool {
        let stable = session_length >= self.stable_session;
        if stable {
            self.next = self.initial;
        }
        stable
    }

    /// The delay to sleep before the next connection attempt; each call doubles the
    /// following one, capped at the maximum.
    pub fn next_delay(&mut self) -> Duration {
        let delay = self.next;
        self.next = (self.next * 2).min(self.max);
        delay
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tuning() -> FeedConfig {
        FeedConfig {
            initial_backoff: Duration::from_millis(500),
            max_backoff: Duration::from_secs(4),
            stable_session: Duration::from_secs(30),
            ..FeedConfig::default()
        }
    }

    #[test]
    fn backoff_doubles_to_the_cap() {
        // Given consecutive short-lived sessions (never stable)
        let mut backoff = Backoff::new(&tuning());
        let mut delays = Vec::new();
        for _ in 0..5 {
            backoff.session_ended(Duration::from_secs(1));
            delays.push(backoff.next_delay().as_millis());
        }
        // Then the delay doubles per attempt and saturates at the max
        assert_eq!(delays, [500, 1000, 2000, 4000, 4000]);
    }

    #[test]
    fn a_stable_session_resets_the_backoff() {
        // Given a backoff already escalated by flapping sessions
        let mut backoff = Backoff::new(&tuning());
        for _ in 0..4 {
            backoff.session_ended(Duration::ZERO);
            backoff.next_delay();
        }

        // When a session lives past the stable threshold
        let stable = backoff.session_ended(Duration::from_secs(30));

        // Then the judgment is reported (callers reset their own counters on it) and the
        // next delay is back to the initial
        assert!(stable);
        assert_eq!(backoff.next_delay(), Duration::from_millis(500));
    }
}
