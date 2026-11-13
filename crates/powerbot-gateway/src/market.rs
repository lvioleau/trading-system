//! Periodic REST poll of PowerBot's market/server status (`getStatus`).
//!
//! The STOMP `marketstatus` topic pushes only status *changes*; this task
//! fetches the full `MarketStatus` document — server versions, exchange
//! session, heartbeats, exchange limits, order-action quota — on an interval,
//! logs it whole, and exports the tradable state and quota as gauges
//! (docs/design.md, "Observability").
//!
//! Each successful poll also publishes the state into the market-state `watch`
//! the order gate reads, as the websocket feed's backup: it fills the unknown
//! window between startup and the feed's first push, and keeps the gate fed
//! (at most one poll interval stale) across a websocket outage. The feed
//! remains the fresh source — it publishes transitions immediately — and its
//! session-loss reset to `None` stands until the next update from either
//! source. A *failed* poll leaves the watch untouched: it says nothing about
//! the market, and must not wipe a state the live feed just published.

use std::time::Duration;

use tokio::sync::watch;

use powerbot_models::models::{MarketState, MarketStatus};

use crate::metrics;
use crate::rest::RestClient;

/// What to poll: the exchange path segment and the cadence.
pub struct MarketPollConfig {
    pub exchange: String,
    /// How often to re-fetch `getStatus`.
    pub interval: Duration,
}

/// Run the poll loop until `shutdown` flips: fetch, log, export, publish into
/// `market_tx`, wait. The first poll is immediate — the startup seed for the
/// order gate. A failed poll is logged and counted
/// (`powerbot.market.poll_failures`), then retried on the next tick, leaving
/// the watch untouched.
pub async fn run(
    rest: RestClient,
    cfg: MarketPollConfig,
    market_tx: watch::Sender<Option<MarketState>>,
    mut shutdown: watch::Receiver<bool>,
) {
    loop {
        match rest.get_market_status(&cfg.exchange).await {
            Ok(status) => observe(&cfg.exchange, status, &market_tx),
            Err(error) => {
                metrics::market_poll_failure();
                tracing::warn!(exchange = %cfg.exchange, %error, "market status poll failed; retrying next tick");
            }
        }
        tokio::select! {
            _ = tokio::time::sleep(cfg.interval) => {}
            _ = shutdown.wait_for(|&stop| stop) => break,
        }
    }
    tracing::info!("market status poll stopped");
}

/// Log the whole document (redacted), export its health onto the
/// `powerbot.market.*` gauges, and publish the state into the order gate's
/// watch.
fn observe(exchange: &str, status: MarketStatus, market_tx: &watch::Sender<Option<MarketState>>) {
    let status = redacted(status);
    tracing::info!(exchange, market_status = ?status, "PowerBot market status");
    market_tx.send_replace(Some(status.status));
    metrics::set_market_state(state_label(status.status));
    if let Some(quota) = status.order_action_quota {
        metrics::set_order_action_quota(quota);
    }
}

/// Strip what must not reach the logs: `mfa_secret` (the document carries a
/// freshly generated MFA secret when one was just created).
fn redacted(mut status: MarketStatus) -> MarketStatus {
    status.mfa_secret = None;
    status
}

/// The `state` tag value for the `powerbot.market.state` gauge.
fn state_label(state: MarketState) -> &'static str {
    match state {
        MarketState::Ok => "ok",
        MarketState::Warning => "warning",
        MarketState::Failure => "failure",
        MarketState::LoggedOut => "logged_out",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn observe_publishes_the_state_into_the_watch() {
        let (tx, rx) = watch::channel::<Option<MarketState>>(None);
        observe("nordpool", MarketStatus::new(MarketState::Failure), &tx);
        assert_eq!(*rx.borrow(), Some(MarketState::Failure));
    }

    #[test]
    fn observe_exports_the_quota_and_publishes_the_state() {
        let (tx, rx) = watch::channel::<Option<MarketState>>(None);
        let mut status = MarketStatus::new(MarketState::Warning);
        status.order_action_quota = Some(0.75); // exercises the Some(quota) export arm
        observe("nordpool", status, &tx);
        assert_eq!(*rx.borrow(), Some(MarketState::Warning));
    }

    #[test]
    fn state_label_covers_every_market_state() {
        assert_eq!(state_label(MarketState::Ok), "ok");
        assert_eq!(state_label(MarketState::Warning), "warning");
        assert_eq!(state_label(MarketState::Failure), "failure");
        assert_eq!(state_label(MarketState::LoggedOut), "logged_out");
    }

    #[test]
    fn redaction_strips_the_mfa_secret() {
        let mut status = MarketStatus::new(MarketState::Ok);
        status.mfa_secret = Some("TOTP-SEED".to_string());
        status.server_version = Some("2.32.14".to_string());

        let status = redacted(status);
        // The secret is gone; everything else survives to be logged.
        assert_eq!(status.mfa_secret, None);
        assert_eq!(status.server_version.as_deref(), Some("2.32.14"));
    }
}
