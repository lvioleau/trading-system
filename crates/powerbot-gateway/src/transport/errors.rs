//! Transport-level error taxonomy, shared by the `Transport` trait and the connect path.

use std::time::Duration;

#[derive(Debug, thiserror::Error)]
pub enum TransportError {
    #[error("handshake failed: {0}")]
    Handshake(String),
    #[error("websocket error: {0}")]
    Ws(String),
    #[error("io error: {0}")]
    Io(String),
    #[error("unexpected message type")]
    UnexpectedMessageType,
    #[error("connection stale: no server frame for {0:?}")]
    Stale(Duration),
    #[error("peer closed connection: code={code:?} reason={reason:?}")]
    Closed { code: Option<u16>, reason: String },
}

impl TransportError {
    pub(crate) fn handshake<E: std::fmt::Display>(e: E) -> Self {
        Self::Handshake(e.to_string())
    }
    pub(crate) fn ws<E: std::fmt::Display>(e: E) -> Self {
        Self::Ws(e.to_string())
    }
}

/// Whether an IO error is just the read timeout firing rather than a real failure. The
/// socket has a read timeout set (see `apply_socket_timeouts`), so a tick with no data
/// surfaces as `WouldBlock` or `TimedOut` depending on the platform; both mean "nothing
/// to read yet", which `recv` maps to `Ok(None)` so the caller loops again.
pub(super) fn would_block(e: &std::io::Error) -> bool {
    matches!(
        e.kind(),
        std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    // `would_block` and the Stale/Closed Display arms are covered by transport.rs's tests;
    // this fills the gap — the string constructors and the remaining Display arms.
    #[test]
    fn the_constructors_and_remaining_display_arms() {
        // The generic constructors stringify their source into the variant.
        assert!(
            TransportError::handshake("bad upgrade")
                .to_string()
                .contains("handshake failed: bad upgrade")
        );
        assert!(
            TransportError::ws("frame error")
                .to_string()
                .contains("websocket error: frame error")
        );
        assert!(
            TransportError::Io("pipe".to_owned())
                .to_string()
                .contains("io error: pipe")
        );
        assert_eq!(
            TransportError::UnexpectedMessageType.to_string(),
            "unexpected message type"
        );
    }
}
