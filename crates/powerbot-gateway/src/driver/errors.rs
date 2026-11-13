//! The driver's error taxonomy and [`is_recoverable`], which decides whether the
//! reconnect loop retries. Per docs/design.md: transport staleness, STOMP ERROR,
//! peer close, or a sequence gap are recoverable — the outer loop backs off and
//! reconnects. Structural protocol disagreements (malformed frames, binary
//! frames) are fatal: they mean a bug, not a blip, and a crash-loop would only
//! hide it.

use crate::stomp::StompError;
use crate::transport::TransportError;

/// A subscription's `sequence-number` jumped forward: PowerBot does not queue or
/// replay, so the skipped messages are gone. Recoverable — the reconnect
/// resubscribes (and, once the REST client exists, re-snapshots).
#[derive(Debug, thiserror::Error)]
#[error("sequence gap on {subscription}: expected {expected}, received {received}")]
pub struct SequenceGap {
    pub subscription: String,
    pub expected: u64,
    pub received: u64,
}

/// Whether the reconnect loop should retry after `e` rather than give up.
pub(crate) fn is_recoverable(e: &anyhow::Error) -> bool {
    if let Some(te) = e.downcast_ref::<TransportError>() {
        return transport_recoverable(te);
    }
    if let Some(se) = e.downcast_ref::<StompError>() {
        return match se {
            StompError::Transport(te) => transport_recoverable(te),
            // A server ERROR frame ends the session on the broker side; the
            // design calls for reconnect. A persistent cause (bad api key)
            // shows up as an ERROR on every attempt, capped at max backoff.
            StompError::Server { .. } => true,
            StompError::ConnectedTimeout(_) => true,
            StompError::Protocol(_) => false,
        };
    }
    e.downcast_ref::<SequenceGap>().is_some()
}

fn transport_recoverable(te: &TransportError) -> bool {
    matches!(
        te,
        TransportError::Closed { .. }
            | TransportError::Stale(_)
            | TransportError::Ws(_)
            | TransportError::Io(_)
            | TransportError::Handshake(_)
    )
}

/// A short, stable label for why a session ended recoverably — the `reason` tag on the
/// `powerbot.reconnects` metric. Only called on the errors [`is_recoverable`] accepts, so the
/// fatal-only arms (`protocol`, `transport`) don't arise in practice but keep the match total.
pub(crate) fn reconnect_reason(e: &anyhow::Error) -> &'static str {
    if let Some(te) = e.downcast_ref::<TransportError>() {
        return transport_reason(te);
    }
    if let Some(se) = e.downcast_ref::<StompError>() {
        return match se {
            StompError::Transport(te) => transport_reason(te),
            StompError::Server { .. } => "stomp_error",
            StompError::ConnectedTimeout(_) => "connected_timeout",
            StompError::Protocol(_) => "protocol",
        };
    }
    if e.downcast_ref::<SequenceGap>().is_some() {
        return "sequence_gap";
    }
    "other"
}

fn transport_reason(te: &TransportError) -> &'static str {
    match te {
        TransportError::Closed { .. } => "peer_close",
        TransportError::Stale(_) => "stale",
        TransportError::Ws(_) => "ws",
        TransportError::Io(_) => "io",
        TransportError::Handshake(_) => "handshake",
        TransportError::UnexpectedMessageType => "transport",
    }
}
