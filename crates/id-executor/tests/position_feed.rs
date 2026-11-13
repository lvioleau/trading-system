//! Black-box tests of the position feed (`position_feed::run_subscriptions` — exactly what
//! production boots) against the *real* position-service server run memory-only (the
//! crosser's test pattern): each portfolio's snapshot seeds its slice of the gap map, a
//! transfer's update lands under the right portfolio with its write attribution, revisions
//! dedupe the snapshot/stream overlap, a service restart re-seeds each portfolio from
//! current state without disturbing the others' slices, and two portfolios' gaps on one
//! contract stay independent.
//!
//! The tests fold the feed's events into a [`PositionStore`] by hand — in production the
//! position manager owns that fold; the store's fold discipline *is* the consumer contract
//! these tests pin down.

use std::collections::HashSet;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use tokio::net::TcpListener;
use tokio::sync::{RwLock, broadcast, mpsc, watch};
use tokio::time::timeout;
use tokio_stream::wrappers::TcpListenerStream;
use uuid::Uuid;

use id_executor::config::FeedConfig;
use id_executor::executor::{Cause, ContractKey, MarketType};
use id_executor::position_feed::{self, PositionEvent};
use id_executor::position_manager::PositionStore;
use id_executor::proto as pb;
use id_executor::proto::position_service_client::PositionServiceClient;
use position_service::channel::{PositionChange, PositionSender, PositionUpdate, SharedLedger};
use position_service::ledger::{
    ContractKey as LedgerContractKey, MarketType as LedgerMarketType, Position as LedgerPosition,
    PositionLedger, Trade,
};
use position_service::server as ps_server;

const CONTRACT: i64 = 7;

// ---------------------------------------------------------------------------------------------
// Upstream harness
// ---------------------------------------------------------------------------------------------

/// The real position-service server, memory-only (`storage: None`), plus the handles the
/// tests drive it by: the shared ledger (pre-seeding), the broadcast sender (manual echoes,
/// exactly as the service's own writers send them), and the shutdown/serve pair a restart
/// needs (the drain must complete before the port can be rebound).
struct PositionServer {
    endpoint: String,
    addr: SocketAddr,
    portfolios: Vec<String>,
    ledger: SharedLedger,
    updates: PositionSender,
    shutdown: watch::Sender<bool>,
    serve: tokio::task::JoinHandle<()>,
}

async fn start_positions(portfolios: &[&str]) -> PositionServer {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let ledger: SharedLedger = Arc::new(RwLock::new(PositionLedger::new()));
    let (updates, _) = broadcast::channel(64);
    serve_on(
        listener,
        portfolios.iter().map(|p| p.to_string()).collect(),
        ledger,
        updates,
    )
    .await
}

/// Serve on `listener` over the given shared state — the restart path reuses the ledger and
/// broadcast sender, exactly like the real service's boot fold rebuilding from its log.
async fn serve_on(
    listener: TcpListener,
    portfolios: Vec<String>,
    ledger: SharedLedger,
    updates: PositionSender,
) -> PositionServer {
    let addr = listener.local_addr().unwrap();
    let endpoint = format!("http://{addr}");
    let (shutdown, shutdown_rx) = watch::channel(false);
    let set: HashSet<String> = portfolios.iter().cloned().collect();
    let router = ps_server::router(
        set,
        ledger.clone(),
        updates.clone(),
        None,
        shutdown_rx.clone(),
    )
    .await
    .unwrap();
    // Drive the drain like production `serve` does: the shutdown watch both ends every
    // subscriber stream (the router's take_until) and stops the accept loop, so the serve
    // future returns and the port frees for a restart.
    let mut drain = shutdown_rx;
    let serve = tokio::spawn(async move {
        router
            .serve_with_incoming_shutdown(TcpListenerStream::new(listener), async move {
                let _ = drain.changed().await;
            })
            .await
            .unwrap();
    });
    PositionServer {
        endpoint,
        addr,
        portfolios,
        ledger,
        updates,
        shutdown,
        serve,
    }
}

impl PositionServer {
    /// Stop the server and wait for its drain to complete — every subscriber stream ends
    /// cleanly and the port frees. The ledger and broadcast channel survive for [`restart`].
    async fn stop(&mut self) {
        self.shutdown.send(true).unwrap();
        (&mut self.serve).await.unwrap();
    }

    /// Serve again on the same address over the same ledger — the restarted service the
    /// feed's reconnect finds.
    async fn restart(&mut self) {
        let listener = TcpListener::bind(self.addr).await.unwrap();
        *self = serve_on(
            listener,
            self.portfolios.clone(),
            self.ledger.clone(),
            self.updates.clone(),
        )
        .await;
    }
}

/// Boot the feed under test: one subscription per portfolio, fast reconnect tuning.
fn start_feed(endpoint: &str, portfolios: &[&str]) -> mpsc::Receiver<PositionEvent> {
    let (events_tx, events_rx) = mpsc::channel(64);
    tokio::spawn(position_feed::run_subscriptions(
        endpoint.to_owned(),
        portfolios.iter().map(|p| p.to_string()).collect(),
        FeedConfig {
            initial_backoff: Duration::from_millis(50),
            max_backoff: Duration::from_millis(200),
            ..FeedConfig::default()
        },
        events_tx,
    ));
    events_rx
}

async fn next_event(events: &mut mpsc::Receiver<PositionEvent>) -> PositionEvent {
    timeout(Duration::from_secs(5), events.recv())
        .await
        .expect("timed out waiting for a position event")
        .expect("the feed dropped its event sender")
}

/// Wait for one `Online` per portfolio in `expected` (their order across subscriptions is
/// unspecified — each portfolio's task races its own connect), folding every `Positions`
/// batch into `store` along the way. `Online` follows its snapshot batch on the same ordered
/// channel, so when it arrives that portfolio's baseline is already folded.
async fn await_online(
    events: &mut mpsc::Receiver<PositionEvent>,
    store: &mut PositionStore,
    expected: &[&str],
) {
    let mut pending: HashSet<String> = expected.iter().map(|p| p.to_string()).collect();
    while !pending.is_empty() {
        match next_event(events).await {
            PositionEvent::Positions(positions) => {
                for position in positions {
                    store.fold(position);
                }
            }
            PositionEvent::Online { portfolio } => {
                pending.remove(&portfolio);
            }
            // Session-end noise is legitimate here: the restart test's reconnect attempts
            // emit one per failed session until the server is back.
            PositionEvent::Offline { .. } => {}
        }
    }
}

// ---------------------------------------------------------------------------------------------
// Wire and domain helpers
// ---------------------------------------------------------------------------------------------

fn contract() -> ContractKey {
    ContractKey {
        market_type: MarketType::Local,
        contract_id: CONTRACT,
    }
}

fn ledger_contract() -> LedgerContractKey {
    LedgerContractKey {
        market_type: LedgerMarketType::Local,
        contract_id: CONTRACT,
    }
}

/// A pre-seeded fill applied straight to the ledger (the position-server test pattern) —
/// the pre-existing position a snapshot must carry. Direct ledger writes broadcast nothing,
/// so they are only applied while the affected subscriptions are down (before boot, or
/// between [`PositionServer::stop`] and [`PositionServer::restart`]).
fn fill(id: u128, portfolio: &str, quantity: i64) -> Trade {
    Trade {
        id: Uuid::from_u128(id),
        portfolio: portfolio.to_owned(),
        contract: ledger_contract(),
        quantity,
        price: 2_000,
        executed_at_nanos: 1_000_000_000,
        venue_trade_id: Some(format!("VT-{id}")),
        transfer_id: None,
        cl_order_id: None,
        source: "test-feed".to_owned(),
    }
}

/// One position change as the service's writers broadcast it — the manual echo the overlap
/// test replays through the live stream.
fn fill_change(portfolio: &str, net_quantity: i64, revision: u64, trade: &str) -> PositionChange {
    PositionChange {
        position: LedgerPosition {
            portfolio: portfolio.to_owned(),
            contract: ledger_contract(),
            net_quantity,
            revision,
        },
        venue_trade_id: Some(trade.to_owned()),
        transfer_id: None,
        cl_order_id: None,
        source: "test-feed".to_owned(),
        executed_at_nanos: 1_000_000_000,
    }
}

/// Book a transfer through the real RPC — the one live-update writer the service has today,
/// and the same wire path the crosser's injections take in production.
async fn transfer(endpoint: &str, id: u128, from: &str, to: &str, quantity: i64) {
    let mut client = PositionServiceClient::connect(endpoint.to_owned())
        .await
        .unwrap();
    client
        .transfer(pb::TransferRequest {
            transfer_id: Uuid::from_u128(id).to_string(),
            from_portfolio: from.to_owned(),
            to_portfolio: to.to_owned(),
            market_type: pb::MarketType::Local as i32,
            contract_id: CONTRACT,
            quantity,
            price: 2_000,
            source: "test".to_owned(),
            executed_at_nanos: 1_000_000_000,
            expected_from_revision: None,
        })
        .await
        .unwrap();
}

// ---------------------------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------------------------

#[tokio::test]
async fn each_portfolios_snapshot_seeds_its_slice_of_the_map() {
    // Given a position service already holding a position for each execution portfolio
    let server = start_positions(&["execution-a", "execution-b"]).await;
    {
        let mut ledger = server.ledger.write().await;
        ledger.apply(fill(1, "execution-a", 10_000));
        ledger.apply(fill(2, "execution-b", -2_000));
    }

    // When the feed boots one subscription per portfolio
    let mut events = start_feed(&server.endpoint, &["execution-a", "execution-b"]);
    let mut store = PositionStore::new();
    await_online(&mut events, &mut store, &["execution-a", "execution-b"]).await;

    // Then each portfolio's snapshot seeded exactly its own slice — the seed replays the
    // pair's attributed history, so the entry carries its causing fill like a live change
    let a = store.get("execution-a", &contract()).unwrap();
    assert_eq!((a.net_quantity, a.revision), (10_000, 1));
    assert_eq!(
        a.cause,
        Some(Cause::Fill {
            venue_trade_id: "VT-1".to_owned(),
            cl_order_id: String::new()
        })
    );
    assert_eq!(store.gap("execution-b", &contract()), -2_000);
}

#[tokio::test]
async fn a_transfers_update_lands_under_the_right_portfolio() {
    // Given both execution portfolios seeded empty (plus the transfer's source portfolio,
    // which the feed does not subscribe)
    let server = start_positions(&["source", "execution-a", "execution-b"]).await;
    let mut events = start_feed(&server.endpoint, &["execution-a", "execution-b"]);
    let mut store = PositionStore::new();
    await_online(&mut events, &mut store, &["execution-a", "execution-b"]).await;

    // When a transfer books 5 MW onto execution-a
    transfer(&server.endpoint, 9, "source", "execution-a", 5_000).await;

    // Then exactly one update arrives — execution-a's leg, attributed to the transfer id
    // (the source leg belongs to no subscribed portfolio, so no stream carries it)
    let PositionEvent::Positions(positions) = next_event(&mut events).await else {
        panic!("expected an update after the transfer")
    };
    assert_eq!(positions.len(), 1);
    assert_eq!(positions[0].portfolio, "execution-a");
    assert_eq!(
        positions[0].cause,
        Some(Cause::Transfer {
            transfer_id: Uuid::from_u128(9).to_string()
        })
    );
    for position in positions {
        assert!(store.fold(position));
    }
    assert_eq!(store.gap("execution-a", &contract()), 5_000);

    // And execution-b saw nothing — no event follows and its slice was never created
    assert!(
        timeout(Duration::from_millis(300), events.recv())
            .await
            .is_err(),
        "no other portfolio's stream should carry this transfer"
    );
    assert!(store.get("execution-b", &contract()).is_none());
}

#[tokio::test]
async fn revisions_dedupe_the_snapshot_stream_overlap() {
    // Given execution-a seeded long 10 MW at revision 1
    let server = start_positions(&["execution-a"]).await;
    server
        .ledger
        .write()
        .await
        .apply(fill(1, "execution-a", 10_000));
    let mut events = start_feed(&server.endpoint, &["execution-a"]);
    let mut store = PositionStore::new();
    await_online(&mut events, &mut store, &["execution-a"]).await;

    // When the revision-1 change is re-broadcast (the snapshot/stream overlap: a writer's
    // broadcast racing the subscriber's snapshot delivers the same fold on both paths) —
    // with a corrupted quantity, to prove the revision guard alone rejects it
    server
        .updates
        .send(PositionUpdate {
            changes: vec![fill_change("execution-a", 999, 1, "VT-1")],
        })
        .unwrap();
    let PositionEvent::Positions(stale) = next_event(&mut events).await else {
        panic!("expected the re-broadcast to stream")
    };

    // Then folding it is a no-op — the store keeps the snapshot's value
    assert!(!store.fold(stale[0].clone()));
    assert_eq!(store.gap("execution-a", &contract()), 10_000);

    // And a genuinely newer revision folds, carrying its fill attribution
    server
        .updates
        .send(PositionUpdate {
            changes: vec![fill_change("execution-a", 14_000, 2, "VT-2")],
        })
        .unwrap();
    let PositionEvent::Positions(fresh) = next_event(&mut events).await else {
        panic!("expected the new revision to stream")
    };
    assert_eq!(
        fresh[0].cause,
        Some(Cause::Fill {
            venue_trade_id: "VT-2".to_owned(),
            cl_order_id: String::new()
        })
    );
    assert!(store.fold(fresh[0].clone()));
    assert_eq!(store.gap("execution-a", &contract()), 14_000);
}

#[tokio::test]
async fn a_restart_reseeds_each_portfolio_without_disturbing_the_others() {
    // Given both portfolios seeded
    let mut server = start_positions(&["execution-a", "execution-b"]).await;
    {
        let mut ledger = server.ledger.write().await;
        ledger.apply(fill(1, "execution-a", 10_000));
        ledger.apply(fill(2, "execution-b", -2_000));
    }
    let mut events = start_feed(&server.endpoint, &["execution-a", "execution-b"]);
    let mut store = PositionStore::new();
    await_online(&mut events, &mut store, &["execution-a", "execution-b"]).await;

    // When the service goes down, folds a fill while the subscriptions are severed (its
    // attribution will never stream again — the re-seed must carry the result), and returns
    server.stop().await;
    server
        .ledger
        .write()
        .await
        .apply(fill(3, "execution-a", 5_000));
    server.restart().await;

    // Then every portfolio's subscription reconnects and re-seeds from current state:
    // execution-a shows the fold it missed...
    await_online(&mut events, &mut store, &["execution-a", "execution-b"]).await;
    let a = store.get("execution-a", &contract()).unwrap();
    assert_eq!((a.net_quantity, a.revision), (15_000, 2));

    // ...and execution-b's slice is exactly as before — a re-seed refreshes, never regresses
    let b = store.get("execution-b", &contract()).unwrap();
    assert_eq!((b.net_quantity, b.revision), (-2_000, 1));
}

#[tokio::test]
async fn two_portfolios_gaps_on_the_same_contract_stay_independent() {
    // Given both portfolios holding the same contract
    let server = start_positions(&["source", "execution-a", "execution-b"]).await;
    {
        let mut ledger = server.ledger.write().await;
        ledger.apply(fill(1, "execution-a", 10_000));
        ledger.apply(fill(2, "execution-b", -2_000));
    }
    let mut events = start_feed(&server.endpoint, &["execution-a", "execution-b"]);
    let mut store = PositionStore::new();
    await_online(&mut events, &mut store, &["execution-a", "execution-b"]).await;

    // When each portfolio receives its own transfer on that contract
    transfer(&server.endpoint, 11, "source", "execution-a", 1_000).await;
    let PositionEvent::Positions(to_a) = next_event(&mut events).await else {
        panic!("expected execution-a's update")
    };
    transfer(&server.endpoint, 12, "source", "execution-b", 500).await;
    let PositionEvent::Positions(to_b) = next_event(&mut events).await else {
        panic!("expected execution-b's update")
    };

    // Then each update carried its own portfolio and each slice moved independently —
    // revisions are per (portfolio, contract), so both landed as revision 2
    assert_eq!(to_a[0].portfolio, "execution-a");
    assert_eq!(to_b[0].portfolio, "execution-b");
    for position in to_a.into_iter().chain(to_b) {
        assert!(store.fold(position));
    }
    let a = store.get("execution-a", &contract()).unwrap();
    let b = store.get("execution-b", &contract()).unwrap();
    assert_eq!((a.net_quantity, a.revision), (11_000, 2));
    assert_eq!((b.net_quantity, b.revision), (-1_500, 2));
}
