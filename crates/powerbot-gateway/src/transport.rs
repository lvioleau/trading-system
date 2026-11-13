//! WebSocket transport to PowerBot: sync tungstenite over rustls, mirroring
//! `nordpool-connectors/crates/connector/src/transport.rs` with two differences
//! (see docs/design.md): frames are UTF-8 text (STOMP), and liveness is STOMP
//! heart-beats rather than ws ping/pong — the keepalive slot sends the bare EOL
//! client heart-beat, any inbound frame refreshes liveness, and the connection
//! is declared stale after a missed-heartbeat budget of server silence.

use std::time::{Duration, Instant};

use tungstenite::stream::MaybeTlsStream;
use tungstenite::{Message, WebSocket};

mod connect;
mod errors;
pub use connect::connect;
pub use errors::TransportError;
use errors::would_block;

/// Abstraction over a text frame transport, so the STOMP session and driver are
/// testable against an in-memory fake.
pub trait Transport {
    /// Read one text application frame. Returns `None` on read timeout, a ws
    /// control frame, or a bare STOMP heart-beat — the caller's loop is expected
    /// to call again. Returns `Err(TransportError::Closed { .. })` when the peer
    /// initiates a close, and `Err(TransportError::Stale(_))` if the server has
    /// gone silent past the heart-beat budget.
    fn recv(&mut self) -> Result<Option<String>, TransportError>;
    /// Send one text application frame.
    fn send(&mut self, text: String) -> Result<(), TransportError>;
}

/// Transport tunables. The heart-beat values must agree with what the STOMP
/// session negotiates in its CONNECT frame (`heart-beat:10000,10000`, per the
/// PowerBot FAQ — the server heartbeats every 10 s and expects ours every 10 s).
#[derive(Debug, Clone)]
pub struct TransportConfig {
    /// Bounds the transport connect: the TCP `connect_timeout` and each blocking
    /// read/write during the TLS + WebSocket-upgrade handshake. Without it a peer
    /// that accepts TCP then stalls hangs the connect attempt forever — the
    /// steady-state timeouts are only applied *after* the handshake returns. A
    /// trip surfaces as a recoverable `Handshake` error. Per-read bound, not a
    /// whole-handshake deadline.
    pub handshake_timeout: Duration,
    /// How long a single `recv()` blocks before returning `None` — the cadence at
    /// which the read loop wakes to run the keepalive check (and, later, notice
    /// shutdown).
    pub read_timeout: Duration,
    /// Bounds a single `send`. Without it a write to a silently-dead connection
    /// (no FIN/RST, kernel send buffer full) blocks forever, freezing the read
    /// loop with no staleness check and hence no reconnect.
    pub write_timeout: Duration,
    /// Client-side heart-beat cadence: how often we owe the server a frame. Any
    /// outbound frame counts (STOMP 1.2 heart-beating), so this only fires on an
    /// otherwise-idle connection.
    pub heartbeat_interval: Duration,
    /// Server-silence budget: how long we go without *any* inbound frame — data,
    /// heart-beat, or ws control — before declaring the connection stale. The FAQ
    /// promises a server heart-beat every 10 s, so 2–3 intervals of silence means
    /// the session is gone.
    pub heartbeat_timeout: Duration,
}

pub struct WsTransport {
    socket: WebSocket<MaybeTlsStream<std::net::TcpStream>>,
    config: TransportConfig,
    /// When we last received anything from the server. Any frame proves the
    /// session is alive; silence past `heartbeat_timeout` means it isn't.
    last_inbound: Instant,
    /// When we last sent anything (frame or heart-beat). Drives the client
    /// heart-beat cadence.
    last_outbound: Instant,
}

impl WsTransport {
    pub(crate) fn new(
        socket: WebSocket<MaybeTlsStream<std::net::TcpStream>>,
        config: TransportConfig,
    ) -> Self {
        Self {
            socket,
            config,
            last_inbound: Instant::now(),
            last_outbound: Instant::now(),
        }
    }

    pub fn recv(&mut self) -> Result<Option<String>, TransportError> {
        // Keepalive before the read: a staleness trip or a failed heart-beat
        // send must not swallow a data frame the read just returned.
        self.keepalive()?;
        self.read_message()
    }

    fn read_message(&mut self) -> Result<Option<String>, TransportError> {
        match self.socket.read() {
            Ok(Message::Text(t)) => {
                self.last_inbound = Instant::now();
                if is_heartbeat(&t) {
                    tracing::debug!("server STOMP heart-beat");
                    Ok(None)
                } else {
                    tracing::debug!(bytes = t.len(), "text frame received");
                    Ok(Some(t))
                }
            }
            Ok(Message::Close(frame)) => {
                let (code, reason) = match frame {
                    Some(f) => (Some(u16::from(f.code)), f.reason.to_string()),
                    None => (None, String::new()),
                };
                tracing::info!(?code, %reason, "peer sent Close");
                Err(TransportError::Closed { code, reason })
            }
            Ok(Message::Ping(_)) => {
                self.last_inbound = Instant::now();
                tracing::debug!("remote websocket ping (auto-pong queued by tungstenite)");
                Ok(None)
            }
            Ok(Message::Pong(_)) => {
                self.last_inbound = Instant::now();
                tracing::debug!("remote websocket pong");
                Ok(None)
            }
            Ok(_) => Err(TransportError::UnexpectedMessageType),
            Err(tungstenite::Error::Io(e)) if would_block(&e) => Ok(None),
            Err(e) => Err(TransportError::ws(e)),
        }
    }

    fn keepalive(&mut self) -> Result<(), TransportError> {
        let silence = self.last_inbound.elapsed();
        if silence > self.config.heartbeat_timeout {
            return Err(TransportError::Stale(silence));
        }
        if self.last_outbound.elapsed() >= self.config.heartbeat_interval {
            tracing::debug!("sending client STOMP heart-beat");
            self.socket
                .send(Message::Text("\n".into()))
                .map_err(TransportError::ws)?;
            self.last_outbound = Instant::now();
        }
        Ok(())
    }

    pub fn send(&mut self, text: String) -> Result<(), TransportError> {
        tracing::debug!(bytes = text.len(), "text frame sent");
        self.socket
            .send(Message::Text(text))
            .map_err(TransportError::ws)?;
        self.last_outbound = Instant::now();
        Ok(())
    }
}

/// A bare STOMP heart-beat (EOL) or SockJS heart-beat (`h`) frame — liveness
/// only, no payload.
fn is_heartbeat(t: &str) -> bool {
    matches!(t, "\n" | "\r\n" | "h")
}

impl Drop for WsTransport {
    /// Send a graceful WebSocket Close so the peer sees a proper closing handshake
    /// rather than a bare TCP FIN. Runs on normal scope exit and during panic
    /// unwinding. Any error from `close` is ignored — the connection is going away
    /// either way.
    fn drop(&mut self) {
        let _ = self.socket.close(None);
    }
}

impl Transport for WsTransport {
    fn recv(&mut self) -> Result<Option<String>, TransportError> {
        WsTransport::recv(self)
    }
    fn send(&mut self, text: String) -> Result<(), TransportError> {
        WsTransport::send(self, text)
    }
}

#[cfg(test)]
mod tests {
    use std::io::{Error as IoError, ErrorKind};
    use std::net::TcpListener;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer};
    use rustls::{ClientConfig, RootCertStore, ServerConfig, ServerConnection, StreamOwned};
    use tungstenite::{Connector, Message};

    use super::*;

    #[test]
    fn would_block_matches_would_block_kind() {
        assert!(would_block(&IoError::from(ErrorKind::WouldBlock)));
    }

    #[test]
    fn would_block_matches_timed_out_kind() {
        assert!(would_block(&IoError::from(ErrorKind::TimedOut)));
    }

    #[test]
    fn would_block_rejects_other_kinds() {
        for kind in [
            ErrorKind::ConnectionReset,
            ErrorKind::ConnectionAborted,
            ErrorKind::Interrupted,
            ErrorKind::BrokenPipe,
            ErrorKind::UnexpectedEof,
        ] {
            assert!(
                !would_block(&IoError::from(kind)),
                "expected not would-block for {kind:?}"
            );
        }
    }

    #[test]
    fn transport_error_stale_display() {
        assert_eq!(
            TransportError::Stale(Duration::from_secs(30)).to_string(),
            "connection stale: no server frame for 30s"
        );
    }

    #[test]
    fn transport_error_closed_display_with_payload() {
        assert_eq!(
            TransportError::Closed {
                code: Some(1000),
                reason: "bye".into(),
            }
            .to_string(),
            "peer closed connection: code=Some(1000) reason=\"bye\""
        );
    }

    /// Short timeouts so tests spin fast; heart-beat values match the default.
    fn test_config() -> TransportConfig {
        TransportConfig {
            handshake_timeout: Duration::from_secs(5),
            read_timeout: Duration::from_millis(200),
            write_timeout: Duration::from_secs(5),
            heartbeat_interval: Duration::from_secs(10),
            heartbeat_timeout: Duration::from_secs(30),
        }
    }

    /// Type of a tungstenite WebSocket sitting on top of our rustls server side. Spelled
    /// out so the test helper below can name it in its `FnOnce(ServerWs)` parameter.
    type ServerWs = tungstenite::WebSocket<StreamOwned<ServerConnection, std::net::TcpStream>>;

    /// Spin up a local wss:// server with a self-signed cert and connect to it using
    /// `connect_with_connector` with a trust store that accepts that cert. Returns the
    /// client-side `WsTransport` and the server thread's join handle. The caller
    /// supplies the server-side behavior as a closure receiving the accepted WebSocket.
    fn connect_to_local_wss<F>(server_fn: F) -> (WsTransport, std::thread::JoinHandle<()>)
    where
        F: FnOnce(ServerWs) + Send + 'static,
    {
        let cert = rcgen::generate_simple_self_signed(vec!["localhost".to_string()])
            .expect("generate self-signed cert");
        let cert_der: CertificateDer<'static> = cert.cert.der().clone();
        let key_der: PrivateKeyDer<'static> =
            PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(cert.key_pair.serialize_der()));

        // Bind 127.0.0.1 explicitly so "wss://localhost:port/..." resolves to the same
        // address regardless of how localhost is configured in /etc/hosts.
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind listener");
        let port = listener.local_addr().expect("local_addr").port();

        // Select the provider explicitly, matching connect::client_config — see the
        // comment there for why the bare `builder()` is avoided.
        let server_config = Arc::new(
            ServerConfig::builder_with_provider(Arc::new(
                rustls::crypto::aws_lc_rs::default_provider(),
            ))
            .with_protocol_versions(rustls::DEFAULT_VERSIONS)
            .expect("server protocol versions")
            .with_no_client_auth()
            .with_single_cert(vec![cert_der.clone()], key_der)
            .expect("build server config"),
        );

        let server = std::thread::spawn(move || {
            let (tcp, _) = listener.accept().expect("accept");
            let conn = ServerConnection::new(server_config).expect("server conn");
            let stream = StreamOwned::new(conn, tcp);
            let ws = tungstenite::accept(stream).expect("ws accept");
            server_fn(ws);
        });

        let mut roots = RootCertStore::empty();
        roots.add(cert_der).expect("add cert to roots");
        let client_config = ClientConfig::builder_with_provider(Arc::new(
            rustls::crypto::aws_lc_rs::default_provider(),
        ))
        .with_protocol_versions(rustls::DEFAULT_VERSIONS)
        .expect("client protocol versions")
        .with_root_certificates(roots)
        .with_no_client_auth();
        let connector = Connector::Rustls(Arc::new(client_config));

        let url = url::Url::parse(&format!("wss://localhost:{port}/test")).expect("parse url");
        let transport =
            super::connect::connect_with_connector(&url, "test-key", test_config(), connector)
                .expect("client connect");

        (transport, server)
    }

    /// End-to-end check: stand up a local wss:// echo server, drive
    /// `connect_with_connector` against it, and verify text frames round-trip.
    #[test]
    fn connect_send_recv_against_local_wss_server() {
        // Given a real local wss:// server that echoes every text frame back.
        let (mut transport, server) = connect_to_local_wss(|mut ws| {
            loop {
                match ws.read() {
                    Ok(Message::Text(t)) => {
                        let _ = ws.send(Message::Text(t));
                    }
                    Ok(Message::Close(_)) | Err(_) => break,
                    Ok(_) => {}
                }
            }
        });
        let payload = "MESSAGE\n\nhello\0".to_string();

        // When we send a text frame and poll recv() for the echo — exercising the full
        // send + receive round-trip over TLS, the way the read loop drives the transport.
        transport.send(payload.clone()).expect("client send");
        let mut echoed = None;
        for _ in 0..10 {
            match transport.recv() {
                Ok(Some(text)) => {
                    echoed = Some(text);
                    break;
                }
                Ok(None) => continue,
                Err(e) => panic!("recv error: {e}"),
            }
        }

        // Then the text round-trips unchanged.
        assert_eq!(echoed.as_deref(), Some(payload.as_str()));

        drop(transport);
        server.join().expect("server thread");
    }

    /// The api_key from the connect call is presented as a header on the ws
    /// upgrade request — PowerBot's authentication scheme.
    #[test]
    // The accept_hdr callback's Result type (large Err) is tungstenite's, not ours.
    #[allow(clippy::result_large_err)]
    fn connect_presents_api_key_header() {
        use tungstenite::handshake::server::{Request, Response};

        // Given a server that captures the upgrade request's api_key header. The accept
        // happens inside connect_to_local_wss's plain `tungstenite::accept`, so here we
        // do our own accept with a callback instead.
        let cert = rcgen::generate_simple_self_signed(vec!["localhost".to_string()])
            .expect("generate self-signed cert");
        let cert_der: CertificateDer<'static> = cert.cert.der().clone();
        let key_der: PrivateKeyDer<'static> =
            PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(cert.key_pair.serialize_der()));
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind listener");
        let port = listener.local_addr().expect("local_addr").port();
        let server_config = Arc::new(
            ServerConfig::builder_with_provider(Arc::new(
                rustls::crypto::aws_lc_rs::default_provider(),
            ))
            .with_protocol_versions(rustls::DEFAULT_VERSIONS)
            .expect("server protocol versions")
            .with_no_client_auth()
            .with_single_cert(vec![cert_der.clone()], key_der)
            .expect("build server config"),
        );
        let (seen_key_tx, seen_key_rx) = std::sync::mpsc::channel::<Option<String>>();
        let server = std::thread::spawn(move || {
            let (tcp, _) = listener.accept().expect("accept");
            let conn = ServerConnection::new(server_config).expect("server conn");
            let stream = StreamOwned::new(conn, tcp);
            let _ws = tungstenite::accept_hdr(stream, |req: &Request, resp: Response| {
                let key = req
                    .headers()
                    .get("api_key")
                    .and_then(|v| v.to_str().ok())
                    .map(str::to_string);
                seen_key_tx.send(key).expect("send seen key");
                Ok(resp)
            })
            .expect("ws accept");
        });

        let mut roots = RootCertStore::empty();
        roots.add(cert_der).expect("add cert to roots");
        let client_config = ClientConfig::builder_with_provider(Arc::new(
            rustls::crypto::aws_lc_rs::default_provider(),
        ))
        .with_protocol_versions(rustls::DEFAULT_VERSIONS)
        .expect("client protocol versions")
        .with_root_certificates(roots)
        .with_no_client_auth();
        let connector = Connector::Rustls(Arc::new(client_config));
        let url = url::Url::parse(&format!("wss://localhost:{port}/test")).expect("parse url");

        // When the client connects with an api key.
        let transport =
            super::connect::connect_with_connector(&url, "sekrit", test_config(), connector)
                .expect("client connect");

        // Then the upgrade request carried it in the api_key header.
        assert_eq!(
            seen_key_rx.recv().expect("seen key").as_deref(),
            Some("sekrit")
        );

        drop(transport);
        server.join().expect("server thread");
    }

    /// Bare heart-beat frames (`\n` from STOMP, `h` from SockJS) are absorbed —
    /// recv returns None, not the frame, and not an error — while a following data
    /// frame still comes through.
    #[test]
    fn heartbeat_frames_are_absorbed() {
        // Given a server that sends two heart-beat frames and then a data frame.
        let (mut transport, server) = connect_to_local_wss(|mut ws| {
            ws.send(Message::Text("\n".into())).expect("send hb");
            ws.send(Message::Text("h".into())).expect("send sockjs hb");
            ws.send(Message::Text("data".into())).expect("send data");
            // Hold the connection open until the client closes.
            loop {
                match ws.read() {
                    Ok(Message::Close(_)) | Err(_) => break,
                    Ok(_) => {}
                }
            }
        });

        // When we poll recv() through the heart-beats.
        let mut frames = Vec::new();
        for _ in 0..20 {
            match transport.recv() {
                Ok(Some(text)) => {
                    frames.push(text);
                    break;
                }
                Ok(None) => continue,
                Err(e) => panic!("recv error: {e}"),
            }
        }

        // Then the only frame surfaced is the data frame — the heart-beats were
        // absorbed without tripping UnexpectedMessageType.
        assert_eq!(frames, vec!["data".to_string()]);

        drop(transport);
        server.join().expect("server thread");
    }

    /// When the server has been silent past `heartbeat_timeout`, the next `recv()`
    /// returns `TransportError::Stale(_)`. Any inbound frame would have reset the
    /// clock; none arrived.
    #[test]
    fn recv_returns_stale_when_server_silent_past_budget() {
        // Given a connected transport whose liveness clock is hand-set 31s into the
        // past (past the 30s heartbeat_timeout); the server sends nothing.
        let (mut transport, server) = connect_to_local_wss(|mut ws| {
            loop {
                match ws.read() {
                    Ok(Message::Close(_)) | Err(_) => break,
                    Ok(_) => {}
                }
            }
        });
        transport.last_inbound = Instant::now() - Duration::from_secs(31);

        // When recv() is called — the keepalive check runs before the read.
        let result = transport.recv();

        // Then recv() reports the connection as Stale (driving a reconnect upstream),
        // with at least the 31s of silence on the clock.
        match result {
            Err(TransportError::Stale(d)) => {
                assert!(d >= Duration::from_secs(31), "stale duration: {d:?}");
            }
            other => panic!("expected Stale, got {other:?}"),
        }

        drop(transport);
        server.join().expect("server thread");
    }

    /// When the client heart-beat cadence is due, `recv()` sends the bare EOL
    /// heart-beat frame.
    #[test]
    fn recv_sends_client_heartbeat_when_due() {
        // Given a server that counts inbound bare-EOL frames.
        let heartbeats = Arc::new(AtomicUsize::new(0));
        let counter = heartbeats.clone();
        let (mut transport, server) = connect_to_local_wss(move |mut ws| {
            loop {
                match ws.read() {
                    Ok(Message::Text(t)) if t == "\n" => {
                        counter.fetch_add(1, Ordering::SeqCst);
                    }
                    Ok(Message::Close(_)) | Err(_) => break,
                    Ok(_) => {}
                }
            }
        });
        // Force the cadence due by aging last_outbound past heartbeat_interval.
        transport.last_outbound = Instant::now() - Duration::from_secs(11);

        // When recv() runs (keepalive fires, then the read times out).
        let _ = transport.recv();

        // Then — give the server a moment to receive the frame, then assert it did.
        std::thread::sleep(Duration::from_millis(200));
        assert_eq!(heartbeats.load(Ordering::SeqCst), 1);
        // And the cadence clock was reset, so an immediate second recv sends nothing.
        assert!(transport.last_outbound.elapsed() < Duration::from_secs(5));

        drop(transport);
        server.join().expect("server thread");
    }

    /// When the server initiates a Close, `recv` should surface `TransportError::Closed`
    /// carrying the peer's code and reason — not a generic Ws error.
    #[test]
    fn recv_returns_closed_with_peer_code_and_reason() {
        use tungstenite::protocol::CloseFrame;
        use tungstenite::protocol::frame::coding::CloseCode;

        // Given a server that initiates a graceful WebSocket Close carrying a specific
        // code (1011) and reason, then drives its state machine to complete the handshake.
        let (mut transport, server) = connect_to_local_wss(|mut ws| {
            let frame = CloseFrame {
                code: CloseCode::Error, // 1011
                reason: "server going away".into(),
            };
            let _ = ws.close(Some(frame));
            while ws.read().is_ok() {}
        });

        // When the client polls recv() until it gets an error.
        let mut got = None;
        for _ in 0..10 {
            match transport.recv() {
                Err(e) => {
                    got = Some(e);
                    break;
                }
                Ok(_) => continue,
            }
        }

        // Then the error is specifically TransportError::Closed carrying the peer's
        // exact code and reason, so reconnect logic can tell a clean close from a fault.
        match got {
            Some(TransportError::Closed { code, reason }) => {
                assert_eq!(code, Some(1011));
                assert_eq!(reason, "server going away");
            }
            other => panic!("expected TransportError::Closed, got {other:?}"),
        }

        drop(transport);
        server.join().expect("server thread");
    }
}
