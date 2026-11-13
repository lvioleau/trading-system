//! Black-box tests of the depth feed (`depth_feed::run` — exactly what production boots)
//! against a mock connector over a local socket: each contract's full top-N view streams as a
//! single [`Book`] (seed and deltas alike — the seed is not special, a delta is a complete
//! overwrite), a reconnect resumes the stream, and a wrong-delivery-area subscription fails
//! fatally (the guard). The mock serves this crate's own generated `marketdata` server stubs
//! (kept for exactly this purpose — nothing in the workspace serves the connector's API).
//!
//! The feed streams one [`Book`] at a time; the engine (arriving at step 6) evaluates each as
//! it lands and keeps no store, so these tests pin down the *stream* — which books arrive,
//! carrying which views — not any accumulated state.

use std::net::SocketAddr;
use std::pin::Pin;
use std::time::Duration;

use tokio::net::TcpListener;
use tokio::sync::{broadcast, mpsc, watch};
use tokio::time::timeout;
use tokio_stream::wrappers::errors::BroadcastStreamRecvError;
use tokio_stream::wrappers::{BroadcastStream, TcpListenerStream};
use tokio_stream::{Stream, StreamExt};
use tonic::{Request, Response, Status};

use eic::EicCode;
use id_executor::config::FeedConfig;
use id_executor::depth_feed;
use id_executor::executor::{Book, ContractKey, Level, MarketType};
use id_executor::marketdata;
use id_executor::marketdata::depth_book_service_server::{
    DepthBookService, DepthBookServiceServer,
};

const AREA: EicCode = EicCode::AREA_FR;

// ---------------------------------------------------------------------------------------------
// Mock upstream
// ---------------------------------------------------------------------------------------------

/// A mock connector serving one delivery area: a subscription for any other is rejected
/// with NOT_FOUND (the request is a guard, not a filter). Each subscription gets `seed` as
/// its first message, then every update broadcast through `updates`. Streams stay open
/// until the shutdown watch fires (ending open streams is what makes the connector's
/// disappearance visible to the subscribed client — tonic serves each connection on its own
/// task, so aborting the accept loop alone is not enough).
struct MockConnector {
    served_area: EicCode,
    seed: marketdata::DepthUpdate,
    updates: broadcast::Sender<marketdata::DepthUpdate>,
    shutdown: watch::Receiver<bool>,
}

#[tonic::async_trait]
impl DepthBookService for MockConnector {
    type SubscribeStream =
        Pin<Box<dyn Stream<Item = Result<marketdata::DepthUpdate, Status>> + Send>>;

    async fn subscribe(
        &self,
        request: Request<marketdata::SubscribeDepthRequest>,
    ) -> Result<Response<Self::SubscribeStream>, Status> {
        let requested = request.into_inner().eic_code;
        if requested != self.served_area.eic_code() {
            return Err(Status::not_found(format!(
                "this connector serves delivery area {}, not {requested}",
                self.served_area.eic_code()
            )));
        }
        let deltas = BroadcastStream::new(self.updates.subscribe()).filter_map(
            |received: Result<marketdata::DepthUpdate, BroadcastStreamRecvError>| {
                received.ok().map(Ok)
            },
        );
        let mut shutdown = self.shutdown.clone();
        let stream = futures_util::StreamExt::take_until(
            tokio_stream::once(Ok(self.seed.clone())).chain(deltas),
            async move {
                let _ = shutdown.changed().await;
            },
        );
        Ok(Response::new(Box::pin(stream)))
    }
}

/// The running mock's handles: the broadcast sender scripts live deltas; stop tears the
/// server down for the reconnect test.
struct MockServer {
    endpoint: String,
    addr: SocketAddr,
    updates: broadcast::Sender<marketdata::DepthUpdate>,
    shutdown: watch::Sender<bool>,
    task: tokio::task::JoinHandle<()>,
}

fn start_mock(
    listener: TcpListener,
    served_area: EicCode,
    seed: marketdata::DepthUpdate,
) -> MockServer {
    let addr = listener.local_addr().unwrap();
    let endpoint = format!("http://{addr}");
    let (updates, _) = broadcast::channel(16);
    let (shutdown, shutdown_rx) = watch::channel(false);
    let mock = MockConnector {
        served_area,
        seed,
        updates: updates.clone(),
        shutdown: shutdown_rx,
    };
    let task = tokio::spawn(async move {
        let _ = tonic::transport::Server::builder()
            .add_service(DepthBookServiceServer::new(mock))
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await;
    });
    MockServer {
        endpoint,
        addr,
        updates,
        shutdown,
        task,
    }
}

impl MockServer {
    /// Take the mock down entirely: stop accepting (freeing the address) and end every open
    /// stream, so the subscribed client sees its session close.
    async fn stop(self) -> SocketAddr {
        self.task.abort();
        let _ = self.task.await;
        let _ = self.shutdown.send(true);
        self.addr
    }
}

/// Rebind `addr` after its previous listener shut down, retrying briefly — the OS may take
/// a moment to release the port.
async fn rebind(addr: SocketAddr) -> TcpListener {
    timeout(Duration::from_secs(5), async {
        loop {
            match TcpListener::bind(addr).await {
                Ok(listener) => return listener,
                Err(_) => tokio::time::sleep(Duration::from_millis(20)).await,
            }
        }
    })
    .await
    .expect("could not rebind the mock's address")
}

fn fast_tuning() -> FeedConfig {
    FeedConfig {
        initial_backoff: Duration::from_millis(50),
        max_backoff: Duration::from_millis(200),
        ..FeedConfig::default()
    }
}

/// Boot the feed under test with fast reconnect tuning.
fn start_feed(endpoint: &str) -> mpsc::Receiver<Book> {
    let (books_tx, books_rx) = mpsc::channel(64);
    tokio::spawn(depth_feed::run(
        endpoint.to_owned(),
        AREA,
        fast_tuning(),
        books_tx,
    ));
    books_rx
}

async fn next_book(books: &mut mpsc::Receiver<Book>) -> Book {
    timeout(Duration::from_secs(5), books.recv())
        .await
        .expect("timed out waiting for a book")
        .expect("the feed dropped its book sender")
}

// ---------------------------------------------------------------------------------------------
// Wire and domain helpers
// ---------------------------------------------------------------------------------------------

fn key(contract_id: i64) -> ContractKey {
    ContractKey {
        market_type: MarketType::Local,
        contract_id,
    }
}

fn level((price, quantity): (i64, i64)) -> marketdata::Level {
    marketdata::Level {
        price,
        total_quantity: quantity,
        order_count: 1,
        created_at_nanos: 1_000,
        updated_at_nanos: 1_000,
    }
}

/// A full top-N view for one local book: `(price, quantity)` levels best-first, as the
/// connector sends them.
fn view(contract_id: i64, bids: &[(i64, i64)], asks: &[(i64, i64)]) -> marketdata::DepthView {
    marketdata::DepthView {
        contract_id,
        market_type: marketdata::MarketType::Local as i32,
        revision: 1,
        bids: bids.iter().copied().map(level).collect(),
        asks: asks.iter().copied().map(level).collect(),
    }
}

fn update(seq: i64, views: Vec<marketdata::DepthView>) -> marketdata::DepthUpdate {
    marketdata::DepthUpdate {
        views,
        received_at_nanos: 123_456,
        seq,
    }
}

/// The next book, asserting which contract it carries — the seed streams its views in order.
async fn next_book_for(books: &mut mpsc::Receiver<Book>, contract_id: i64) -> Book {
    let book = next_book(books).await;
    assert_eq!(
        book.key,
        key(contract_id),
        "unexpected contract in the stream"
    );
    book
}

// ---------------------------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------------------------

#[tokio::test]
async fn each_book_streams_as_a_full_view() {
    // Given a connector whose seed carries two books — one two-sided and two levels deep,
    // one bid-less
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mock = start_mock(
        listener,
        AREA,
        update(
            1,
            vec![
                view(7, &[(4_200, 5_000), (4_100, 8_000)], &[(4_300, 2_000)]),
                view(8, &[], &[(4_500, 1_000)]),
            ],
        ),
    );
    let mut books = start_feed(&mock.endpoint);

    // The seed streams book by book (view order). The executor's read — the opposite side's
    // best level, price and visible size — serves book 7, and the bid-less book 8 reads as
    // nothing to take.
    let book_7 = next_book_for(&mut books, 7).await;
    assert_eq!(
        book_7.best_bid(),
        Some(&Level {
            price: 4_200,
            quantity: 5_000
        })
    );
    assert_eq!(book_7.bids.len(), 2);
    let book_8 = next_book_for(&mut books, 8).await;
    assert_eq!(book_8.best_bid(), None);
    assert_eq!(
        book_8.best_ask(),
        Some(&Level {
            price: 4_500,
            quantity: 1_000
        })
    );

    // A delta replaces book 7 with a one-level view — the whole view wins, not a merge
    mock.updates
        .send(update(2, vec![view(7, &[(4_250, 3_000)], &[(4_350, 900)])]))
        .unwrap();
    let delta_7 = next_book_for(&mut books, 7).await;
    assert_eq!(
        delta_7.bids.len(),
        1,
        "full state per view — the deeper bid is gone"
    );
    assert_eq!(
        delta_7.best_bid(),
        Some(&Level {
            price: 4_250,
            quantity: 3_000
        })
    );
}

#[tokio::test]
async fn a_reconnect_resumes_the_stream() {
    // Given a feed streaming a two-book seed
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mock = start_mock(
        listener,
        AREA,
        update(
            1,
            vec![
                view(7, &[(4_200, 5_000)], &[(4_300, 2_000)]),
                view(8, &[(4_000, 1_000)], &[(4_100, 1_000)]),
            ],
        ),
    );
    let mut books = start_feed(&mock.endpoint);
    next_book_for(&mut books, 7).await;
    next_book_for(&mut books, 8).await;

    // When the connector goes away and comes back with a fresh seed (no disconnect event —
    // the stream simply goes quiet, then resumes on reconnect)
    let addr = mock.stop().await;
    let listener = rebind(addr).await;
    let _mock = start_mock(
        listener,
        AREA,
        update(9, vec![view(7, &[(4_260, 4_000)], &[(4_340, 1_500)])]),
    );

    // Then the feed reconnects and resumes streaming — the new seed's book 7 arrives fresh
    let reconnected_7 = next_book_for(&mut books, 7).await;
    assert_eq!(
        reconnected_7.best_bid(),
        Some(&Level {
            price: 4_260,
            quantity: 4_000
        })
    );
}

#[tokio::test]
async fn a_wrong_delivery_area_fails_the_feed_fatally() {
    // Given a connector serving area 40
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mock = start_mock(listener, AREA, update(1, vec![]));

    // When the feed subscribes for area CH, run returns the fatal error promptly — a
    // permanent config mismatch (wrong connector), not a retry loop
    let (books_tx, _books_rx) = mpsc::channel(64);
    let error = timeout(
        Duration::from_secs(5),
        depth_feed::run(
            mock.endpoint.clone(),
            EicCode::AREA_CH,
            fast_tuning(),
            books_tx,
        ),
    )
    .await
    .expect("a rejected area must fail fast, not retry");
    assert!(error.to_string().contains("10YCH-SWISSGRIDZ"), "{error}");
}
