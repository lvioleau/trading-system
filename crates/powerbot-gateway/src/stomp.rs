//! STOMP session over a [`Transport`], the subset PowerBot speaks:
//! `CONNECT`/`CONNECTED`, `SUBSCRIBE`, `MESSAGE`, `ERROR`, heart-beats.
//! The frame codec is `stomp-parser`; this module owns the session choreography
//! and header access (`sequence-number`).
//!
//! Heart-beats themselves live in the transport: bare EOL frames are absorbed
//! there and the client-side cadence is sent from its keepalive slot. This
//! module's `CONNECT` negotiates `heart-beat:10000,10000` to match
//! `TransportConfig`'s defaults.

use std::str::FromStr;
use std::time::{Duration, Instant};

use stomp_parser::client::{ConnectFrameBuilder, SubscribeFrameBuilder};
use stomp_parser::error::StompParseError;
use stomp_parser::headers::{HeaderValue, HeartBeatIntervals, StompVersions};
use stomp_parser::server::{ErrorFrame, MessageFrame, ServerFrame};

use crate::transport::{Transport, TransportError};

#[derive(Debug, thiserror::Error)]
pub enum StompError {
    #[error(transparent)]
    Transport(#[from] TransportError),
    #[error("protocol error: {0}")]
    Protocol(String),
    #[error("server ERROR frame: {message} {body:?}")]
    Server { message: String, body: String },
    /// Typed separately from `Protocol` because the driver classifies it
    /// recoverable (slow/unreachable broker) while protocol errors are fatal.
    #[error("timed out waiting for CONNECTED after {0:?}")]
    ConnectedTimeout(Duration),
}

impl StompError {
    fn server(frame: &ErrorFrame) -> Self {
        Self::Server {
            message: frame
                .message()
                .map(|m| m.value().to_string())
                .unwrap_or_default(),
            body: body_string(frame.body()),
        }
    }
}

impl From<StompParseError> for StompError {
    fn from(e: StompParseError) -> Self {
        Self::Protocol(e.message().to_string())
    }
}

/// A received `MESSAGE` frame, with the headers the driver cares about lifted out.
#[derive(Debug, Clone)]
pub struct StompMessage {
    /// The subscription id given to `subscribe` — maps the message back to its topic.
    pub subscription: String,
    pub destination: String,
    /// PowerBot's per-topic gap-detection counter (a custom STOMP header).
    /// Resets when the PowerBot application restarts.
    pub sequence_number: Option<u64>,
    pub body: String,
}

impl StompMessage {
    fn from_frame(frame: &MessageFrame) -> Self {
        Self {
            subscription: frame.subscription().value().to_string(),
            destination: frame.destination().value().to_string(),
            sequence_number: frame
                .custom
                .iter()
                .find(|h| h.header_name() == "sequence-number")
                .and_then(|h| h.value().parse().ok()),
            body: body_string(frame.body()),
        }
    }
}

/// Frame bodies here are JSON, so UTF-8; lossy conversion never actually loses
/// anything and avoids failing on a body we'd want to see in the logs anyway.
fn body_string(body: Option<&[u8]>) -> String {
    body.map(|b| String::from_utf8_lossy(b).into_owned())
        .unwrap_or_default()
}

/// A serialized frame is UTF-8 text on the wire (the transport sends ws text frames).
fn frame_text(frame: impl Into<Vec<u8>>) -> Result<String, StompError> {
    String::from_utf8(frame.into())
        .map_err(|e| StompError::Protocol(format!("frame is not UTF-8: {e}")))
}

/// A STOMP client session over a [`Transport`]: connect, subscribe, receive.
pub struct StompSession<T: Transport> {
    transport: T,
}

impl<T: Transport> StompSession<T> {
    pub fn new(transport: T) -> Self {
        Self { transport }
    }

    /// Send `CONNECT` (negotiating `heart-beat:10000,10000`, matching
    /// `TransportConfig`'s cadence) and wait up to `timeout` for `CONNECTED`.
    pub fn connect(&mut self, host: &str, timeout: Duration) -> Result<(), StompError> {
        let connect = ConnectFrameBuilder::new(host.to_string(), StompVersions::from_str("1.2")?)
            .heartbeat(HeartBeatIntervals::new(10000, 10000))
            .build();
        tracing::debug!(host, "sending STOMP CONNECT");
        self.transport.send(frame_text(connect)?)?;

        let deadline = Instant::now() + timeout;
        loop {
            let Some(raw) = self.transport.recv()? else {
                if Instant::now() >= deadline {
                    return Err(StompError::ConnectedTimeout(timeout));
                }
                continue;
            };
            // CONNECTED is parsed leniently here, not by stomp-parser: its model
            // rejects headers outside {version, heart-beat, session, server}, and
            // real brokers add more (PowerBot's sends `user-name`). MESSAGE/ERROR
            // stay on stomp-parser, which preserves their custom headers.
            let headers = if let Some(headers) = connected_headers(&raw) {
                headers
            } else {
                return match parse_server_frame(raw)? {
                    ServerFrame::Error(frame) => Err(StompError::server(&frame)),
                    other => Err(StompError::Protocol(format!(
                        "expected CONNECTED, got {}",
                        frame_name(&other)
                    ))),
                };
            };
            let header = |name| headers.iter().find(|(n, _)| *n == name).map(|(_, v)| *v);
            tracing::debug!(
                version = header("version"),
                heart_beat = header("heart-beat"),
                "STOMP session connected"
            );
            return Ok(());
        }
    }

    /// Subscribe to `destination`; `id` is echoed back on every `MESSAGE` and
    /// maps it to its topic. Ack mode is the default `auto` — PowerBot pushes,
    /// we never ack.
    pub fn subscribe(&mut self, id: &str, destination: &str) -> Result<(), StompError> {
        tracing::debug!(id, destination, "sending STOMP SUBSCRIBE");
        let subscribe = SubscribeFrameBuilder::new(destination.to_string(), id.to_string()).build();
        self.transport.send(frame_text(subscribe)?)?;
        Ok(())
    }

    /// Read one message. `None` means a quiet tick (read timeout or absorbed
    /// keep-alive) — call again. A server `ERROR` frame or a transport failure
    /// is an `Err`, tearing the session down for the reconnect loop upstream.
    pub fn recv(&mut self) -> Result<Option<StompMessage>, StompError> {
        let Some(raw) = self.transport.recv()? else {
            return Ok(None);
        };
        match parse_server_frame(raw)? {
            ServerFrame::Message(frame) => Ok(Some(StompMessage::from_frame(&frame))),
            ServerFrame::Error(frame) => Err(StompError::server(&frame)),
            other => Err(StompError::Protocol(format!(
                "unexpected frame: {}",
                frame_name(&other)
            ))),
        }
    }
}

/// Parse via stomp-parser, attaching the frame's leading bytes to any error —
/// its own message ("Error parsing frame") says nothing about what failed.
fn parse_server_frame(raw: String) -> Result<ServerFrame, StompError> {
    let context: String = raw.chars().take(256).collect();
    ServerFrame::try_from(raw.into_bytes())
        .map_err(|e| StompError::Protocol(format!("{} in frame {:?}", e.message(), context)))
}

/// If `raw` is a `CONNECTED` frame, its header lines as `(name, value)` pairs.
fn connected_headers(raw: &str) -> Option<Vec<(&str, &str)>> {
    let mut lines = raw
        .trim_start_matches(['\r', '\n'])
        .lines()
        .map(|l| l.strip_suffix('\r').unwrap_or(l));
    (lines.next() == Some("CONNECTED")).then(|| {
        lines
            .take_while(|l| !l.is_empty())
            .filter_map(|l| l.split_once(':'))
            .collect()
    })
}

fn frame_name(frame: &ServerFrame) -> &'static str {
    match frame {
        ServerFrame::Connected(_) => "CONNECTED",
        ServerFrame::Receipt(_) => "RECEIPT",
        ServerFrame::Error(_) => "ERROR",
        ServerFrame::Message(_) => "MESSAGE",
    }
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;

    use super::*;

    /// Scripted in-memory transport: recv() hands out `incoming` in order
    /// (`None` entries model quiet ticks), send() records outbound frames.
    #[derive(Default)]
    struct FakeTransport {
        incoming: VecDeque<Option<String>>,
        sent: Vec<String>,
    }

    impl Transport for FakeTransport {
        fn recv(&mut self) -> Result<Option<String>, TransportError> {
            Ok(self.incoming.pop_front().flatten())
        }
        fn send(&mut self, text: String) -> Result<(), TransportError> {
            self.sent.push(text);
            Ok(())
        }
    }

    fn session_with(incoming: &[Option<&str>]) -> StompSession<FakeTransport> {
        StompSession::new(FakeTransport {
            incoming: incoming.iter().map(|f| f.map(str::to_string)).collect(),
            sent: Vec::new(),
        })
    }

    #[test]
    fn connect_sends_connect_and_accepts_connected() {
        // Given a server that answers with CONNECTED after one quiet tick.
        let mut session = session_with(&[None, Some("CONNECTED\nversion:1.2\n\n\0")]);

        // When
        session
            .connect("example.com", Duration::from_secs(1))
            .expect("connect");

        // Then the CONNECT frame went out with the negotiation headers.
        let sent = &session.transport.sent[0];
        assert!(sent.starts_with("CONNECT\n"), "{sent:?}");
        assert!(sent.contains("accept-version:1.2\n"), "{sent:?}");
        assert!(sent.contains("host:example.com\n"), "{sent:?}");
        assert!(sent.contains("heart-beat:10000,10000\n"), "{sent:?}");
        assert!(sent.ends_with("\n\n\0"), "{sent:?}");
    }

    #[test]
    fn connect_accepts_connected_with_broker_specific_headers() {
        // Regression: PowerBot's broker adds headers (user-name) that stomp-parser's
        // CONNECTED model rejects; the lenient CONNECTED path must accept them.
        let mut session = session_with(&[Some(
            "CONNECTED\nversion:1.2\nheart-beat:10000,10000\nuser-name:trader\n\n\0",
        )]);

        session
            .connect("example.com", Duration::from_secs(1))
            .expect("connect");
    }

    #[test]
    fn connect_surfaces_server_error_frame() {
        let mut session = session_with(&[Some("ERROR\nmessage:bad credentials\n\ndetails here\0")]);

        let err = session
            .connect("example.com", Duration::from_secs(1))
            .expect_err("must fail");

        match err {
            StompError::Server { message, body } => {
                assert_eq!(message, "bad credentials");
                assert_eq!(body, "details here");
            }
            other => panic!("expected Server error, got {other}"),
        }
    }

    #[test]
    fn connect_times_out_waiting_for_connected() {
        // A transport that only ever produces quiet ticks.
        let mut session = session_with(&[None, None, None]);

        let err = session
            .connect("example.com", Duration::ZERO)
            .expect_err("must time out");

        assert!(matches!(err, StompError::ConnectedTimeout(_)), "{err}");
    }

    #[test]
    fn subscribe_sends_subscribe_frame() {
        let mut session = session_with(&[]);

        session
            .subscribe("sub-0", "/topic/marketstatus-epex")
            .expect("subscribe");

        let sent = &session.transport.sent[0];
        assert!(sent.starts_with("SUBSCRIBE\n"), "{sent:?}");
        assert!(sent.contains("id:sub-0\n"), "{sent:?}");
        assert!(
            sent.contains("destination:/topic/marketstatus-epex\n"),
            "{sent:?}"
        );
        assert!(sent.contains("ack:auto\n"), "{sent:?}");
        assert!(sent.ends_with("\n\n\0"), "{sent:?}");
    }

    #[test]
    fn recv_yields_message_with_lifted_headers() {
        let mut session = session_with(&[
            None,
            Some(
                "MESSAGE\nmessage-id:m-1\ndestination:/topic/marketstatus-epex\nsubscription:sub-0\nsequence-number:7\n\n{\"a\":1}\0",
            ),
        ]);

        assert!(session.recv().expect("quiet tick").is_none());
        let message = session.recv().expect("recv").expect("message");

        assert_eq!(message.subscription, "sub-0");
        assert_eq!(message.destination, "/topic/marketstatus-epex");
        assert_eq!(message.sequence_number, Some(7));
        assert_eq!(message.body, "{\"a\":1}");
    }

    #[test]
    fn recv_without_sequence_number_yields_none() {
        let mut session = session_with(&[Some(
            "MESSAGE\nmessage-id:m-1\ndestination:/topic/x\nsubscription:sub-0\n\nbody\0",
        )]);

        let message = session.recv().expect("recv").expect("message");

        assert_eq!(message.sequence_number, None);
        assert_eq!(message.body, "body");
    }

    #[test]
    fn recv_surfaces_error_frame() {
        let mut session = session_with(&[Some("ERROR\nmessage:subscription failed\n\n\0")]);

        let err = session.recv().expect_err("must fail");

        assert!(matches!(err, StompError::Server { .. }), "{err}");
    }

    #[test]
    fn recv_rejects_malformed_frame_with_context() {
        let mut session = session_with(&[Some("MESSAGE\nmissing-required-headers:yes\n\nbody\0")]);

        let err = session.recv().expect_err("must fail");

        // The error must carry the offending frame — stomp-parser's own message
        // ("Error parsing frame") is useless in a log line on its own.
        match err {
            StompError::Protocol(msg) => {
                assert!(msg.contains("missing-required-headers"), "{msg}")
            }
            other => panic!("expected Protocol error, got {other}"),
        }
    }

    #[test]
    fn recv_rejects_unexpected_connected_frame() {
        let mut session = session_with(&[Some("CONNECTED\nversion:1.2\n\n\0")]);

        let err = session.recv().expect_err("must fail");

        match err {
            StompError::Protocol(msg) => assert!(msg.contains("CONNECTED"), "{msg}"),
            other => panic!("expected Protocol error, got {other}"),
        }
    }
}
