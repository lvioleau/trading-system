//! Session driver: connect → STOMP CONNECT → subscribe → read → dispatch, with
//! reconnect/backoff around it — the same skeleton as nordpool-connectors'
//! driver (docs/design.md, "Driver"). One session at a time:
//!
//! 1. `connect()` opens the wss transport (API key on the upgrade).
//! 2. STOMP `CONNECT` → `CONNECTED`, then one `SUBSCRIBE` per [`Topic`].
//! 3. Read loop: track per-topic `sequence-number`s, decode `messageClass` →
//!    generated struct, hand [`PowerbotEvent`]s to the sink.
//! 4. On staleness, peer close, STOMP `ERROR`, or a sequence gap: return a
//!    recoverable error; the outer loop backs off (500 ms → 30 s) and reconnects.
//!
//! Snapshot-after-subscribe (REST) joins the loop with the ingest milestone.

use std::collections::HashMap;
use std::time::Duration;

use anyhow::Result;

use crate::stomp::{StompMessage, StompSession};
use crate::transport::Transport;

mod errors;
mod events;
mod retry;
mod shutdown;

pub use errors::SequenceGap;
use errors::{is_recoverable, reconnect_reason};
pub use events::PowerbotEvent;
use events::Topic;
use retry::{Backoff, retry_with_backoff};
use shutdown::shutdown_requested;
pub use shutdown::{ShutdownRequested, request_shutdown};

/// Session parameters, built from the INI config in `main` (or literally in tests).
/// The timing fields have no baked-in defaults — every value comes from `[gateway]`.
#[derive(Debug, Clone)]
pub struct DriverConfig {
    /// Host for the STOMP CONNECT frame (the ws URL's host).
    pub host: String,
    pub exchange: String,
    /// EIC area code scoping the contracts topic.
    pub delivery_area_eic: String,
    /// The served portfolios; each gets its own ownorders/owntrades topic pair.
    pub portfolio_ids: Vec<String>,
    /// How long to wait for the server's CONNECTED frame.
    pub connected_timeout: Duration,
    pub initial_backoff: Duration,
    pub max_backoff: Duration,
    /// A session that lived at least this long resets the backoff to initial.
    pub backoff_reset_after: Duration,
}

/// Where decoded events go. The ingest pipeline (bounded channel into tokio)
/// implements this later; for now `main` passes a logging closure.
pub trait EventSink {
    /// An error is fatal to the process — it means the downstream pipeline is
    /// broken, which reconnecting cannot fix.
    fn handle(&mut self, event: PowerbotEvent) -> Result<()>;

    /// A connect attempt or session ended (staleness, error, gap, shutdown — a
    /// reconnect may follow). Any state derived from the feed (e.g. the market
    /// status gating order entry) is stale from here until the next session
    /// delivers fresh updates. The default does nothing.
    fn session_ended(&mut self) {}
}

impl<F: FnMut(PowerbotEvent) -> Result<()>> EventSink for F {
    fn handle(&mut self, event: PowerbotEvent) -> Result<()> {
        self(event)
    }
}

/// Run sessions forever, reconnecting with exponential backoff on recoverable
/// errors. Returns only on a fatal error — including [`ShutdownRequested`],
/// which `main` translates back to a clean exit.
pub fn run_with_reconnect<T, F, S>(config: &DriverConfig, mut connect: F, mut sink: S) -> Result<()>
where
    T: Transport,
    F: FnMut() -> Result<T>,
    S: EventSink,
{
    let backoff = Backoff {
        initial: config.initial_backoff,
        max: config.max_backoff,
        reset_after: config.backoff_reset_after,
    };
    retry_with_backoff(&backoff, backoff_sleep, || {
        // A shutdown flipped during the backoff sleep ends the loop here, before
        // another connect attempt reaches out to PowerBot.
        if shutdown_requested() {
            return Err(ShutdownRequested.into());
        }
        let result = connect_and_run(config, &mut connect, &mut sink);
        // The session is over (however it ended): reflect it in the gauge and tell
        // the sink before we classify and back off, so `powerbot.connected` tracks
        // liveness across the backoff sleep and feed-derived state (market status)
        // reads as unknown rather than stale while disconnected.
        crate::metrics::set_connected(false);
        sink.session_ended();
        match result {
            Err(_) if shutdown_requested() => Err(ShutdownRequested.into()),
            Err(e) if is_recoverable(&e) => {
                crate::metrics::reconnect(reconnect_reason(&e));
                tracing::warn!(error = format!("{e:#}"), "recoverable error, reconnecting");
                Ok(())
            }
            other => other,
        }
    })
}

/// Sleep out a backoff wait in short slices, returning early once shutdown is
/// requested — a SIGTERM during a max-backoff wait must not stall process exit
/// for the remainder of the sleep (`main` joins this thread before exiting).
fn backoff_sleep(total: Duration) {
    // Shutdown-poll cadence while backing off — internal responsiveness, not an
    // operational tunable like the configured backoff itself.
    const SLICE: Duration = Duration::from_millis(100);
    let deadline = std::time::Instant::now() + total;
    while !shutdown_requested() {
        let remaining = deadline.saturating_duration_since(std::time::Instant::now());
        if remaining.is_zero() {
            return;
        }
        std::thread::sleep(remaining.min(SLICE));
    }
}

fn connect_and_run<T, F, S>(config: &DriverConfig, connect: &mut F, sink: &mut S) -> Result<()>
where
    T: Transport,
    F: FnMut() -> Result<T>,
    S: EventSink,
{
    let transport = connect()?;
    let mut session = StompSession::new(transport);
    run(config, &mut session, sink)
}

/// Drive one connected session to its end. Only returns `Err`: every session
/// ends in an error, and the reconnect wrapper classifies it.
pub fn run<T: Transport, S: EventSink>(
    config: &DriverConfig,
    session: &mut StompSession<T>,
    sink: &mut S,
) -> Result<()> {
    session.connect(&config.host, config.connected_timeout)?;
    tracing::info!(exchange = %config.exchange, "session connected");
    crate::metrics::set_connected(true);

    for subscription in events::subscriptions(config) {
        session.subscribe(&subscription.id, &subscription.destination)?;
    }

    let mut sequences = SequenceTracker::default();
    loop {
        if shutdown_requested() {
            tracing::info!("shutdown requested; ending session");
            return Err(ShutdownRequested.into());
        }
        let Some(message) = session.recv()? else {
            continue;
        };
        tracing::debug!(
            subscription = %message.subscription,
            destination = %message.destination,
            sequence_number = ?message.sequence_number,
            body = %message.body,
            "message received"
        );
        let topic = Topic::from_subscription_id(&message.subscription).map(Topic::label);
        crate::metrics::frame_received(topic.unwrap_or("unknown"), message.body.len());
        sequences.check(&message)?;
        if let Some(event) = events::decode(&message) {
            sink.handle(event)?;
        } else {
            // Unknown subscription, wrong messageClass, or a body the model rejects — all
            // logged by decode; count them so a spike is visible without log scraping.
            crate::metrics::decode_failure();
        }
    }
}

/// Per-subscription `sequence-number` continuity tracking (docs/design.md,
/// "Driver"). PowerBot's counters are per topic — and a per-portfolio topic is
/// one subscription here, so tracking keys on the subscription id, not the topic
/// kind: two portfolios' owntrades run independent counters. Counters reset when
/// PowerBot's application restarts and only run while at least one subscriber
/// exists — so:
///
/// - the first number seen on a subscription is a baseline, not checked;
/// - a forward jump means missed messages, unrecoverable in-session (no replay):
///   [`SequenceGap`] tears the session down and the reconnect starts fresh;
/// - backwards (or repeated) numbers mean a PowerBot restart or a duplicate
///   delivery, both documented behaviors: re-baseline and keep going —
///   downstream consumers are idempotent by design, and a reconnect could not
///   recover anything a restart dropped anyway (the REST snapshot, once built,
///   is what heals that).
#[derive(Default)]
struct SequenceTracker {
    last_seen: HashMap<String, u64>,
}

impl SequenceTracker {
    fn check(&mut self, message: &StompMessage) -> Result<(), SequenceGap> {
        let Some(topic) = Topic::from_subscription_id(&message.subscription) else {
            return Ok(()); // unknown subscription; decode logs the drop
        };
        let Some(received) = message.sequence_number else {
            return Ok(());
        };
        let last = self
            .last_seen
            .insert(message.subscription.clone(), received);
        let Some(last) = last else {
            return Ok(()); // baseline
        };
        let expected = last + 1;
        if received > expected {
            crate::metrics::sequence_gap(topic.label());
            return Err(SequenceGap {
                subscription: message.subscription.clone(),
                expected,
                received,
            });
        }
        if received < expected {
            crate::metrics::sequence_rebaseline(topic.label());
            tracing::warn!(
                subscription = %message.subscription,
                last,
                received,
                "sequence number went backwards (PowerBot restart or duplicate); re-baselined"
            );
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::cell::RefCell;
    use std::collections::VecDeque;
    use std::rc::Rc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};

    use crate::transport::TransportError;

    use super::*;

    /// Clears the thread-local shutdown flag when a shutdown test ends (even on
    /// panic), so a test thread reused by libtest doesn't inherit a set flag.
    struct ShutdownOnDrop;
    impl Drop for ShutdownOnDrop {
        fn drop(&mut self) {
            super::shutdown::reset();
        }
    }

    /// Queue of frames (or transport errors) the fake transport hands back from `recv`.
    type IncomingQueue = Arc<Mutex<VecDeque<Result<Option<String>, TransportError>>>>;

    /// Scripted transport: recv() hands out the queue in order (`None` models a
    /// quiet tick); an empty queue optionally requests shutdown, else ticks quietly.
    #[derive(Clone, Default)]
    struct FakeTransport {
        incoming: IncomingQueue,
        sent: Arc<Mutex<Vec<String>>>,
        shutdown_on_drain: bool,
    }

    impl FakeTransport {
        fn push_frame(&self, frame: &str) {
            self.incoming
                .lock()
                .unwrap()
                .push_back(Ok(Some(frame.to_string())));
        }
        fn push_error(&self, e: TransportError) {
            self.incoming.lock().unwrap().push_back(Err(e));
        }
        fn sent(&self) -> Vec<String> {
            self.sent.lock().unwrap().clone()
        }
    }

    impl Transport for FakeTransport {
        fn recv(&mut self) -> Result<Option<String>, TransportError> {
            match self.incoming.lock().unwrap().pop_front() {
                Some(result) => result,
                None => {
                    if self.shutdown_on_drain {
                        request_shutdown();
                    }
                    Ok(None)
                }
            }
        }
        fn send(&mut self, text: String) -> Result<(), TransportError> {
            self.sent.lock().unwrap().push(text);
            Ok(())
        }
    }

    /// Sink recording every event it is handed.
    fn recording_sink() -> (
        Rc<RefCell<Vec<PowerbotEvent>>>,
        impl FnMut(PowerbotEvent) -> Result<()>,
    ) {
        let events = Rc::new(RefCell::new(Vec::new()));
        let sink_events = events.clone();
        (events, move |event| {
            sink_events.borrow_mut().push(event);
            Ok(())
        })
    }

    fn config() -> DriverConfig {
        DriverConfig {
            host: "host.example".into(),
            exchange: "epex".into(),
            delivery_area_eic: "10YDK-1--------W".into(),
            portfolio_ids: vec!["pf-1".into()],
            // Sessions against the fake transport never wait on a real socket; keep
            // the CONNECTED deadline tight so timeout tests are fast.
            connected_timeout: Duration::from_millis(50),
            initial_backoff: Duration::from_millis(500),
            max_backoff: Duration::from_secs(30),
            backoff_reset_after: Duration::from_secs(60),
        }
    }

    const CONNECTED: &str = "CONNECTED\nversion:1.2\nheart-beat:10000,10000\n\n\0";

    fn message(subscription: &str, sequence: u64, body: &str) -> String {
        format!(
            "MESSAGE\nmessage-id:m-1\ndestination:/topic/x\nsubscription:{subscription}\nsequence-number:{sequence}\n\n{body}\0"
        )
    }

    const MARKET_STATUS_BODY: &str =
        r#"{"messageClass":"powerbot.v2.model.MarketStatusChangedEvent"}"#;
    const OWN_ORDERS_BODY: &str =
        r#"{"messageClass":"powerbot.v2.model.OwnOrderChanges","orders":[]}"#;

    /// A transport scripted with a CONNECTED handshake and a trailing peer Close
    /// so the session ends after consuming the frames pushed in between.
    fn connected_transport() -> FakeTransport {
        let transport = FakeTransport::default();
        transport.push_frame(CONNECTED);
        transport
    }

    fn push_close(transport: &FakeTransport) {
        transport.push_error(TransportError::Closed {
            code: Some(1000),
            reason: "bye".into(),
        });
    }

    #[test]
    fn run_subscribes_to_all_topics_after_connected() {
        // Given a session that completes the handshake then closes immediately.
        let transport = connected_transport();
        push_close(&transport);
        let (_, mut sink) = recording_sink();

        // When the session runs to its end.
        let mut session = StompSession::new(transport.clone());
        let _ = run(&config(), &mut session, &mut sink);

        // Then after the CONNECT frame, one SUBSCRIBE per subscription went out,
        // each with its id and its destination filled from config.
        let sent = transport.sent();
        assert!(sent[0].starts_with("CONNECT\n"), "{:?}", sent[0]);
        let subscribes: Vec<&String> = sent
            .iter()
            .filter(|f| f.starts_with("SUBSCRIBE\n"))
            .collect();
        assert_eq!(subscribes.len(), 4);
        for (id, destination) in [
            ("sub-marketstatus", "/topic/marketstatus-epex"),
            ("sub-contracts", "/topic/contracts-epex.10YDK-1--------W"),
            ("sub-ownorders.pf-1", "/topic/ownorders-epex.pf-1"),
            ("sub-owntrades.pf-1", "/topic/owntrades-epex.pf-1"),
        ] {
            assert!(
                subscribes
                    .iter()
                    .any(|f| f.contains(&format!("\nid:{id}\n"))
                        && f.contains(&format!("\ndestination:{destination}\n"))),
                "missing SUBSCRIBE for {id} → {destination} in {subscribes:?}"
            );
        }
    }

    #[test]
    fn run_subscribes_one_topic_pair_per_portfolio() {
        // Given a config serving two portfolios and a session that closes right
        // after the handshake.
        let transport = connected_transport();
        push_close(&transport);
        let (_, mut sink) = recording_sink();
        let mut config = config();
        config.portfolio_ids = vec!["pf-1".into(), "pf-2".into()];

        // When the session runs to its end.
        let mut session = StompSession::new(transport.clone());
        let _ = run(&config, &mut session, &mut sink);

        // Then the shared topics subscribed once and the per-portfolio topics
        // once per portfolio — 2 + 2 × 2 SUBSCRIBEs.
        let subscribes: Vec<String> = transport
            .sent()
            .into_iter()
            .filter(|f| f.starts_with("SUBSCRIBE\n"))
            .collect();
        assert_eq!(subscribes.len(), 6);
        for (id, destination) in [
            ("sub-ownorders.pf-2", "/topic/ownorders-epex.pf-2"),
            ("sub-owntrades.pf-2", "/topic/owntrades-epex.pf-2"),
        ] {
            assert!(
                subscribes
                    .iter()
                    .any(|f| f.contains(&format!("\nid:{id}\n"))
                        && f.contains(&format!("\ndestination:{destination}\n"))),
                "missing SUBSCRIBE for {id} → {destination} in {subscribes:?}"
            );
        }
    }

    #[test]
    fn run_decodes_and_dispatches_events_to_the_sink() {
        // Given a connected session delivering one market-status and one
        // own-orders message before the peer closes.
        let transport = connected_transport();
        transport.push_frame(&message("sub-marketstatus", 1, MARKET_STATUS_BODY));
        transport.push_frame(&message("sub-ownorders", 1, OWN_ORDERS_BODY));
        push_close(&transport);
        let (events, mut sink) = recording_sink();

        // When the session runs to its end.
        let mut session = StompSession::new(transport);
        let err = run(&config(), &mut session, &mut sink).unwrap_err();

        // Then both messages were decoded into typed events, in order, and the
        // session ended on the peer close (recoverable), not on a decode error.
        let events = events.borrow();
        assert_eq!(events.len(), 2);
        assert!(matches!(events[0], PowerbotEvent::MarketStatus(_)));
        assert!(matches!(events[1], PowerbotEvent::OwnOrders(_)));
        assert!(is_recoverable(&err), "{err:#}");
    }

    #[test]
    fn run_drops_undecodable_messages_without_ending_the_session() {
        // Given a connected session delivering, in order: a message on an unknown
        // subscription, one with the wrong messageClass for its topic, one with a
        // body its model rejects, and finally a good one.
        let transport = connected_transport();
        transport.push_frame(&message("sub-mystery", 1, MARKET_STATUS_BODY));
        transport.push_frame(&message("sub-marketstatus", 1, OWN_ORDERS_BODY));
        transport.push_frame(&message(
            "sub-ownorders",
            1,
            r#"{"messageClass":"powerbot.v2.model.OwnOrderChanges","orders":42}"#,
        ));
        transport.push_frame(&message("sub-marketstatus", 2, MARKET_STATUS_BODY));
        push_close(&transport);
        let (events, mut sink) = recording_sink();

        // When the session runs to its end.
        let mut session = StompSession::new(transport);
        let err = run(&config(), &mut session, &mut sink).unwrap_err();

        // Then only the good message reached the sink — the three bad ones were
        // dropped (logged) without killing the session, which ended on the Close.
        assert_eq!(events.borrow().len(), 1);
        assert!(is_recoverable(&err), "{err:#}");
    }

    mod sequence_numbers {
        use super::*;

        #[test]
        fn a_forward_jump_is_a_gap_that_ends_the_session() {
            // Given a topic whose sequence numbers jump 1 → 3: message 2 was lost,
            // and PowerBot cannot replay it.
            let transport = connected_transport();
            transport.push_frame(&message("sub-marketstatus", 1, MARKET_STATUS_BODY));
            transport.push_frame(&message("sub-marketstatus", 3, MARKET_STATUS_BODY));
            let (events, mut sink) = recording_sink();

            // When the session runs.
            let mut session = StompSession::new(transport);
            let err = run(&config(), &mut session, &mut sink).unwrap_err();

            // Then the session ended on a SequenceGap naming the subscription and
            // both numbers, classified recoverable so the driver reconnects; the
            // pre-gap message was still dispatched.
            let gap = err.downcast_ref::<SequenceGap>().expect("SequenceGap");
            assert_eq!(gap.subscription, "sub-marketstatus");
            assert_eq!(gap.expected, 2);
            assert_eq!(gap.received, 3);
            assert!(is_recoverable(&err), "{err:#}");
            assert_eq!(events.borrow().len(), 1);
        }

        #[test]
        fn sequences_are_tracked_per_subscription_across_portfolios() {
            // Given two portfolios' owntrades subscriptions interleaved, each
            // with its own consistent counter — PowerBot's counters are per
            // topic, and a per-portfolio topic is one subscription here.
            const OWN_TRADES_BODY: &str =
                r#"{"messageClass":"powerbot.v2.model.OwnTradeChanges","trades":[]}"#;
            let transport = connected_transport();
            transport.push_frame(&message("sub-owntrades.pf-1", 5, OWN_TRADES_BODY));
            transport.push_frame(&message("sub-owntrades.pf-2", 9, OWN_TRADES_BODY));
            transport.push_frame(&message("sub-owntrades.pf-1", 6, OWN_TRADES_BODY));
            transport.push_frame(&message("sub-owntrades.pf-2", 10, OWN_TRADES_BODY));
            push_close(&transport);
            let (events, mut sink) = recording_sink();
            let mut config = config();
            config.portfolio_ids = vec!["pf-1".into(), "pf-2".into()];

            // When the session runs to its end.
            let mut session = StompSession::new(transport);
            let err = run(&config, &mut session, &mut sink).unwrap_err();

            // Then no gap was declared — the counters were judged per
            // subscription, not pooled across the shared topic kind — and all
            // four events dispatched.
            assert!(err.downcast_ref::<SequenceGap>().is_none(), "{err:#}");
            assert_eq!(events.borrow().len(), 4);
        }

        #[test]
        fn sequences_are_tracked_per_topic() {
            // Given two topics interleaved, each with its own consistent counter —
            // continuity must be judged per topic, not across the stream.
            let transport = connected_transport();
            transport.push_frame(&message("sub-marketstatus", 5, MARKET_STATUS_BODY));
            transport.push_frame(&message("sub-ownorders", 9, OWN_ORDERS_BODY));
            transport.push_frame(&message("sub-marketstatus", 6, MARKET_STATUS_BODY));
            transport.push_frame(&message("sub-ownorders", 10, OWN_ORDERS_BODY));
            push_close(&transport);
            let (events, mut sink) = recording_sink();

            // When the session runs to its end.
            let mut session = StompSession::new(transport);
            let err = run(&config(), &mut session, &mut sink).unwrap_err();

            // Then no gap was declared and all four events were dispatched.
            assert!(err.downcast_ref::<SequenceGap>().is_none(), "{err:#}");
            assert_eq!(events.borrow().len(), 4);
        }

        #[test]
        fn backwards_or_repeated_numbers_rebaseline_instead_of_erroring() {
            // Given a topic whose counter goes 7, 7 (duplicate delivery), then 2
            // (PowerBot restarted; counter reset), then 3 (consistent again).
            let transport = connected_transport();
            for sequence in [7, 7, 2, 3] {
                transport.push_frame(&message("sub-marketstatus", sequence, MARKET_STATUS_BODY));
            }
            push_close(&transport);
            let (events, mut sink) = recording_sink();

            // When the session runs to its end.
            let mut session = StompSession::new(transport);
            let err = run(&config(), &mut session, &mut sink).unwrap_err();

            // Then the session survived to the Close (no gap): backwards numbers
            // re-baseline — a reconnect could not recover a restart's losses, and
            // duplicates are documented behavior handled by idempotent consumers.
            assert!(err.downcast_ref::<SequenceGap>().is_none(), "{err:#}");
            assert_eq!(events.borrow().len(), 4);
        }
    }

    mod session_end {
        use super::*;
        use crate::stomp::StompError;

        #[test]
        fn a_server_error_frame_ends_the_session_recoverably() {
            // Given a connected session whose broker sends a STOMP ERROR frame.
            let transport = connected_transport();
            transport.push_frame("ERROR\nmessage:broker restarting\n\n\0");
            let (_, mut sink) = recording_sink();

            // When the session runs.
            let mut session = StompSession::new(transport);
            let err = run(&config(), &mut session, &mut sink).unwrap_err();

            // Then it surfaced as StompError::Server and is recoverable — the
            // design's contract that a STOMP ERROR drives a reconnect.
            assert!(matches!(
                err.downcast_ref::<StompError>(),
                Some(StompError::Server { .. })
            ));
            assert!(is_recoverable(&err), "{err:#}");
        }

        #[test]
        fn connected_timeout_is_recoverable() {
            // Given a transport that never answers the CONNECT (only quiet ticks).
            let transport = FakeTransport::default();
            let (_, mut sink) = recording_sink();

            // When the session runs against the tight test deadline.
            let mut session = StompSession::new(transport);
            let err = run(&config(), &mut session, &mut sink).unwrap_err();

            // Then the CONNECTED timeout is classified recoverable: a slow or
            // unreachable broker is a retry, not a crash.
            assert!(matches!(
                err.downcast_ref::<StompError>(),
                Some(StompError::ConnectedTimeout(_))
            ));
            assert!(is_recoverable(&err), "{err:#}");
        }

        #[test]
        fn a_malformed_frame_is_fatal() {
            // Given a connected session that then receives gibberish — a codec
            // disagreement, which reconnecting would only turn into a crash-loop.
            let transport = connected_transport();
            transport.push_frame("GIBBERISH\n\n\0");
            let (_, mut sink) = recording_sink();

            // When the session runs.
            let mut session = StompSession::new(transport);
            let err = run(&config(), &mut session, &mut sink).unwrap_err();

            // Then the protocol error is not recoverable.
            assert!(!is_recoverable(&err), "{err:#}");
        }

        #[test]
        fn shutdown_ends_the_session_with_the_sentinel() {
            let _reset = ShutdownOnDrop;
            // Given a connected session whose transport requests shutdown once
            // its scripted frames drain, as a SIGTERM mid-session would.
            let mut transport = connected_transport();
            transport.shutdown_on_drain = true;
            let (_, mut sink) = recording_sink();

            // When the session runs.
            let mut session = StompSession::new(transport);
            let err = run(&config(), &mut session, &mut sink).unwrap_err();

            // Then it ended with the ShutdownRequested sentinel, which is not
            // recoverable — the reconnect loop must stop, not retry.
            assert!(err.downcast_ref::<ShutdownRequested>().is_some());
            assert!(!is_recoverable(&err), "{err:#}");
        }
    }

    mod reconnect {
        use super::*;

        #[test]
        fn retries_recoverable_sessions_and_stops_on_fatal() {
            // Given three staged connect attempts: a recoverable connect failure,
            // a session ending in a recoverable peer Close, and a session hitting
            // a fatal malformed frame.
            let session_2 = connected_transport();
            push_close(&session_2);
            let session_3 = connected_transport();
            session_3.push_frame("GIBBERISH\n\n\0");
            let attempts = Arc::new(AtomicUsize::new(0));
            let attempts_in_connect = attempts.clone();
            let connect = move || -> Result<FakeTransport> {
                match attempts_in_connect.fetch_add(1, Ordering::SeqCst) {
                    0 => Err(TransportError::Handshake("dns blip".into()).into()),
                    1 => Ok(session_2.clone()),
                    _ => Ok(session_3.clone()),
                }
            };
            let (_, sink) = recording_sink();
            let mut config = config();
            config.initial_backoff = Duration::from_millis(1);

            // When the reconnect loop runs across all three attempts.
            let err = run_with_reconnect(&config, connect, sink).unwrap_err();

            // Then it retried through the two recoverable failures and stopped on
            // the fatal one — three connects in total.
            assert!(!is_recoverable(&err), "{err:#}");
            assert_eq!(attempts.load(Ordering::SeqCst), 3);
        }

        #[test]
        fn session_ended_is_signaled_to_the_sink_after_every_attempt() {
            // Given a sink that counts session_ended calls, and two connect
            // attempts: a session ending in a recoverable peer Close, then one
            // hitting a fatal malformed frame.
            struct CountingSink {
                ended: Rc<RefCell<usize>>,
            }
            impl EventSink for CountingSink {
                fn handle(&mut self, _event: PowerbotEvent) -> Result<()> {
                    Ok(())
                }
                fn session_ended(&mut self) {
                    *self.ended.borrow_mut() += 1;
                }
            }
            let ended = Rc::new(RefCell::new(0));
            let session_1 = connected_transport();
            push_close(&session_1);
            let session_2 = connected_transport();
            session_2.push_frame("GIBBERISH\n\n\0");
            let attempts = Arc::new(AtomicUsize::new(0));
            let attempts_in_connect = attempts.clone();
            let connect = move || -> Result<FakeTransport> {
                match attempts_in_connect.fetch_add(1, Ordering::SeqCst) {
                    0 => Ok(session_1.clone()),
                    _ => Ok(session_2.clone()),
                }
            };
            let mut config = config();
            config.initial_backoff = Duration::from_millis(1);

            // When the reconnect loop runs both attempts.
            let sink = CountingSink {
                ended: ended.clone(),
            };
            let _ = run_with_reconnect(&config, connect, sink).unwrap_err();

            // Then the sink was told the session ended after each attempt, so
            // feed-derived state (e.g. market status) reads as stale/unknown
            // across the backoff, not just at process exit.
            assert_eq!(*ended.borrow(), 2);
        }

        #[test]
        fn stops_on_shutdown_without_retrying() {
            let _reset = ShutdownOnDrop;
            // Given a connect that requests shutdown and then fails recoverably:
            // the shutdown arm must win over the is_recoverable arm.
            let attempts = Arc::new(AtomicUsize::new(0));
            let attempts_in_connect = attempts.clone();
            let connect = move || -> Result<FakeTransport> {
                attempts_in_connect.fetch_add(1, Ordering::SeqCst);
                request_shutdown();
                Err(TransportError::Handshake("blip".into()).into())
            };
            let (_, sink) = recording_sink();

            // When the reconnect loop runs.
            let err = run_with_reconnect(&config(), connect, sink).unwrap_err();

            // Then it stopped with ShutdownRequested after a single attempt.
            assert!(err.downcast_ref::<ShutdownRequested>().is_some());
            assert_eq!(attempts.load(Ordering::SeqCst), 1);
        }
    }
}
