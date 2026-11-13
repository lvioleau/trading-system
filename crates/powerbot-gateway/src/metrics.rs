//! DogStatsD metrics — the `powerbot.*` surface (docs/design.md, "Observability").
//!
//! Same stack as the sibling nordpool-connectors: the [`metrics`] facade emits at
//! the call sites, and [`metrics_exporter_dogstatsd`] aggregates client-side and
//! flushes over UDP from a background thread, so emission sites never do socket IO.
//! The recorder is installed once at boot by [`init`]; the `metrics::` macros are
//! no-ops until then (and stay cheap no-ops in unit tests, which never install one).
//!
//! Every metric name lives here rather than scattered as string literals, so the
//! `powerbot.*` surface is one auditable list. Call the typed helpers from the
//! emission sites (`driver`, `rest`, `grpc`). Referenced as `::metrics::` because
//! this module shares the facade crate's name.

use std::time::Duration;

use anyhow::{Context, Result};
use metrics_exporter_dogstatsd::DogStatsDBuilder;

/// The build's short git SHA, stamped by `build.rs`; tags every series so deploys
/// draw a `version:` boundary through the dashboards.
pub const VERSION: &str = env!("GIT_SHA");

/// Install the global DogStatsD recorder pointed at `dogstatsd_addr`, stamping every series with
/// the build `version:` plus the configured global `tags`. Always installed: DogStatsD is UDP, so
/// with no agent listening (a local run) the aggregated packets drop silently — no need to gate
/// emission, and the macros stay live rather than compiling to no-ops. Client-side global tags
/// (not agent-level tags) so the gateway self-identifies even when it shares an agent with another
/// app. Fails only on an unparseable address, at startup.
pub fn init(dogstatsd_addr: &str, tags: &[(String, String)]) -> Result<()> {
    describe();
    // Every series carries the build's version tag — a deploy then shows up as one tag value's
    // series ending where the next begins — plus any configured identifying tags (e.g. service:).
    let mut labels = vec![::metrics::Label::new("version", VERSION)];
    labels.extend(
        tags.iter()
            .map(|(k, v)| ::metrics::Label::new(k.clone(), v.clone())),
    );
    DogStatsDBuilder::default()
        .with_remote_address(dogstatsd_addr)
        .with_context(|| format!("invalid dogstatsd_addr {dogstatsd_addr:?}"))?
        .with_global_labels(labels)
        .install()
        .context("installing DogStatsD metrics exporter")?;
    tracing::info!(addr = %dogstatsd_addr, version = VERSION, ?tags, "DogStatsD metrics export enabled");
    Ok(())
}

/// One-line units/help for the metrics, registered before the recorder installs.
fn describe() {
    use ::metrics::Unit;
    ::metrics::describe_counter!(
        "powerbot.websocket.frames_received",
        "STOMP MESSAGE application frames received, by topic"
    );
    ::metrics::describe_counter!(
        "powerbot.websocket.bytes_received",
        Unit::Bytes,
        "STOMP MESSAGE body bytes received, by topic"
    );
    ::metrics::describe_counter!(
        "powerbot.decode.failures",
        "Received frames dropped without decoding (unknown subscription / messageClass / bad body)"
    );
    ::metrics::describe_counter!(
        "powerbot.sequence.gaps",
        "Forward sequence-number jumps (missed messages) that tore the session down, by topic"
    );
    ::metrics::describe_counter!(
        "powerbot.sequence.rebaselines",
        "Backwards/repeated sequence numbers (PowerBot restart or duplicate), by topic"
    );
    ::metrics::describe_counter!(
        "powerbot.reconnects",
        "Session reconnects, by the recoverable reason that ended the session"
    );
    ::metrics::describe_gauge!(
        "powerbot.connected",
        "1 while a PowerBot STOMP session is up, 0 otherwise"
    );
    ::metrics::describe_counter!(
        "powerbot.trades.self_trades",
        "External trades flagged self_trade (both legs ours); should never occur with IOC-only order entry"
    );
    ::metrics::describe_counter!(
        "powerbot.trades.internal_trades_live",
        "Internal trades (self-matches) seen on a live gateway; unexpected under IOC-only order entry, never streamed"
    );
    ::metrics::describe_histogram!(
        "powerbot.order.rest_latency_ms",
        Unit::Milliseconds,
        "AddOrder REST round-trip latency, by outcome"
    );
    ::metrics::describe_gauge!(
        "powerbot.market.state",
        "REST-polled market state, one-hot by state tag: 1 for the current state, 0 for the others"
    );
    ::metrics::describe_gauge!(
        "powerbot.market.order_action_quota",
        "Consumed order entry/modification quota (0.0-1.0) from the REST-polled market status"
    );
    ::metrics::describe_counter!(
        "powerbot.market.poll_failures",
        "getStatus REST polls that failed (transport error or non-success status)"
    );
    ::metrics::describe_gauge!(
        "powerbot.contracts.known",
        "Contracts currently held by the tradability store the order gate checks"
    );
    ::metrics::describe_counter!(
        "powerbot.contracts.poll_failures",
        "findContracts REST sweeps that failed (transport error or non-success status)"
    );
    ::metrics::describe_gauge!(
        "powerbot.grpc.subscribers",
        "Live execution-stream subscribers, by feed"
    );
    ::metrics::describe_counter!(
        "powerbot.grpc.subscribers_dropped",
        "Execution-stream subscribers dropped, by feed and reason (limit / lagged)"
    );
}

/// A STOMP MESSAGE was received on `topic` carrying `bytes` of body.
pub fn frame_received(topic: &'static str, bytes: usize) {
    ::metrics::counter!("powerbot.websocket.frames_received", "topic" => topic).increment(1);
    ::metrics::counter!("powerbot.websocket.bytes_received", "topic" => topic)
        .increment(bytes as u64);
}

/// A received frame was dropped without decoding into a typed event.
pub fn decode_failure() {
    ::metrics::counter!("powerbot.decode.failures").increment(1);
}

/// A forward sequence-number jump on `topic` tore the session down.
pub fn sequence_gap(topic: &'static str) {
    ::metrics::counter!("powerbot.sequence.gaps", "topic" => topic).increment(1);
}

/// A backwards/repeated sequence number on `topic` re-baselined the tracker.
pub fn sequence_rebaseline(topic: &'static str) {
    ::metrics::counter!("powerbot.sequence.rebaselines", "topic" => topic).increment(1);
}

/// A session ended recoverably and is being reconnected; `reason` classifies why.
pub fn reconnect(reason: &'static str) {
    ::metrics::counter!("powerbot.reconnects", "reason" => reason).increment(1);
}

/// An external trade arrived flagged `self_trade` — both legs ours, which the
/// gateway's IOC-only order entry should make impossible. Alert-worthy.
pub fn self_trade() {
    ::metrics::counter!("powerbot.trades.self_trades").increment(1);
}

/// An internal trade (a self-match between two of our own orders) arrived on a
/// *live* (non-shadow) gateway — impossible under IOC-only entry. Recorded and
/// alarmed, never streamed. Alert-worthy.
pub fn internal_trade_on_live() {
    ::metrics::counter!("powerbot.trades.internal_trades_live").increment(1);
}

/// Whether a PowerBot STOMP session is currently up.
pub fn set_connected(connected: bool) {
    ::metrics::gauge!("powerbot.connected").set(if connected { 1.0 } else { 0.0 });
}

/// Record an AddOrder REST round trip's latency; `outcome` is `success`,
/// `rejected` (PowerBot 4xx), or `error`.
pub fn order_action_latency(outcome: &'static str, elapsed: Duration) {
    ::metrics::histogram!("powerbot.order.rest_latency_ms", "outcome" => outcome)
        .record(elapsed.as_secs_f64() * 1000.0);
}

/// Every `state` tag value of the `powerbot.market.state` gauge — the market
/// poll sets the whole family one-hot so a dashboard can read (and alert on)
/// any state without a stale series lingering at 1 after a transition.
const MARKET_STATES: [&str; 4] = ["ok", "warning", "failure", "logged_out"];

/// The REST-polled market state is `current` (a [`MARKET_STATES`] label).
pub fn set_market_state(current: &'static str) {
    for state in MARKET_STATES {
        let value = if state == current { 1.0 } else { 0.0 };
        ::metrics::gauge!("powerbot.market.state", "state" => state).set(value);
    }
}

/// The REST-polled consumed order-action quota (0.0–1.0).
pub fn set_order_action_quota(quota: f64) {
    ::metrics::gauge!("powerbot.market.order_action_quota").set(quota);
}

/// A `getStatus` market poll failed.
pub fn market_poll_failure() {
    ::metrics::counter!("powerbot.market.poll_failures").increment(1);
}

/// How many contracts the tradability store currently holds (set per sweep).
pub fn set_contracts_known(count: usize) {
    ::metrics::gauge!("powerbot.contracts.known").set(count as f64);
}

/// A `findContracts` contract sweep failed.
pub fn contract_poll_failure() {
    ::metrics::counter!("powerbot.contracts.poll_failures").increment(1);
}

/// A subscriber attached to `feed`'s execution stream.
pub fn subscriber_added(feed: &'static str) {
    ::metrics::gauge!("powerbot.grpc.subscribers", "feed" => feed).increment(1.0);
}

/// A subscriber's `feed` stream ended (disconnect or drop).
pub fn subscriber_removed(feed: &'static str) {
    ::metrics::gauge!("powerbot.grpc.subscribers", "feed" => feed).decrement(1.0);
}

/// A subscribe on `feed` was refused or a live subscriber cut; `reason` is
/// `limit` (subscriber cap) or `lagged` (fell behind the broadcast).
pub fn subscriber_dropped(feed: &'static str, reason: &'static str) {
    ::metrics::counter!("powerbot.grpc.subscribers_dropped", "feed" => feed, "reason" => reason)
        .increment(1);
}
