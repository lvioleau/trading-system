//! Black-box tests of the crosser's depth feed against a mock connector over a local socket:
//! the seed and deltas fold each contract's entire `DepthView` into the shared map, the
//! read-time top-of-book reduction prices (or refuses to price) it, a wrong-delivery-area
//! subscription fails fatally (the guard), and a reconnect re-seeds — dropping books that
//! vanished while disconnected (and everything, while down: no live feed means no mid). The mid
//! arithmetic itself is unit-tested in the domain module.

use std::net::SocketAddr;
use std::pin::Pin;
use std::time::Duration;

use tokio::net::TcpListener;
use tokio::sync::watch;
use tokio::time::timeout;
use tokio_stream::wrappers::TcpListenerStream;
use tokio_stream::{Stream, StreamExt};
use tonic::{Request, Response, Status};

use crosser::config::FeedConfig;
use crosser::crosser::{ContractKey, MarketType};
use crosser::depth_feed::{self, SharedDepth, top_of_book};
use crosser::marketdata;
use crosser::marketdata::depth_book_service_server::{DepthBookService, DepthBookServiceServer};
use eic::EicCode;

const AREA: EicCode = EicCode::AREA_FR;

/// A mock connector serving one delivery area: a subscription for any other is rejected with
/// NOT_FOUND (the request is a guard, not a filter). Each subscription streams the fixed script,
/// then stays open until the shutdown watch fires.
struct MockConnector {
    served_area: EicCode,
    script: Vec<marketdata::DepthUpdate>,
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
        let stream = tokio_stream::iter(self.script.clone().into_iter().map(Ok))
            .chain(tokio_stream::pending());
        // Cut the stream when the test shuts the mock down — a depth stream never ends on its
        // own, and tonic serves each connection on its own task, so ending the stream (not just
        // stopping the accept loop) is what makes the client see the connector go away.
        let mut shutdown = self.shutdown.clone();
        let stream = futures_util::StreamExt::take_until(stream, async move {
            let _ = shutdown.changed().await;
        });
        Ok(Response::new(Box::pin(stream)))
    }
}

/// Serve on `listener`, producing `script` for area subscriptions. To stop the mock, call
/// [`stop_mock`] with the returned handles: aborting the task alone frees the port but leaves
/// live per-connection tasks serving the open streams.
fn start_mock(
    listener: TcpListener,
    served_area: EicCode,
    script: Vec<marketdata::DepthUpdate>,
) -> (watch::Sender<bool>, tokio::task::JoinHandle<()>) {
    let (shutdown, shutdown_rx) = watch::channel(false);
    let mock = MockConnector {
        served_area,
        script,
        shutdown: shutdown_rx,
    };
    let task = tokio::spawn(async move {
        let _ = tonic::transport::Server::builder()
            .add_service(DepthBookServiceServer::new(mock))
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await;
    });
    (shutdown, task)
}

/// Take the mock down entirely: stop accepting (freeing the address) and end every open stream,
/// so the subscribed client sees its session close.
async fn stop_mock(shutdown: watch::Sender<bool>, task: tokio::task::JoinHandle<()>) {
    task.abort();
    let _ = task.await;
    let _ = shutdown.send(true);
}

fn fast_tuning() -> FeedConfig {
    FeedConfig {
        initial_backoff: Duration::from_millis(20),
        max_backoff: Duration::from_millis(200),
        stable_session: Duration::from_secs(30),
        keepalive_interval: Duration::from_secs(30),
        keepalive_timeout: Duration::from_secs(20),
    }
}

fn level(price: i64) -> marketdata::Level {
    marketdata::Level {
        price,
        total_quantity: 5_000,
        order_count: 1,
        created_at_nanos: 1_000,
        updated_at_nanos: 1_000,
    }
}

/// A full top-N view for one local book: prices best-first, as the connector sends them.
fn view(contract_id: i64, bids: &[i64], asks: &[i64]) -> marketdata::DepthView {
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

fn key(contract_id: i64) -> ContractKey {
    ContractKey {
        market_type: MarketType::Local,
        contract_id,
    }
}

/// Poll until the shared map's stored view of `contract_id` equals `expected` (None = absent).
async fn await_view(
    books: &SharedDepth,
    contract_id: i64,
    expected: Option<marketdata::DepthView>,
) {
    let result = timeout(Duration::from_secs(5), async {
        loop {
            if books.lock().unwrap().get(&key(contract_id)).cloned() == expected {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await;
    assert!(
        result.is_ok(),
        "view for contract {contract_id} never became {expected:?}: found {:?}",
        books.lock().unwrap().get(&key(contract_id))
    );
}

#[tokio::test]
async fn seed_and_deltas_fold_the_full_views() {
    // Given a connector whose seed carries two books — one two-sided and two levels deep, one
    // bid-less — and a later delta replacing the first book
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    let (_shutdown, _server) = start_mock(
        listener,
        AREA,
        vec![
            update(
                1,
                vec![
                    view(7, &[4_200, 4_100], &[4_300, 4_400]),
                    view(8, &[], &[4_500]),
                ],
            ),
            update(2, vec![view(7, &[4_250], &[4_350])]),
        ],
    );

    // When the feed subscribes
    let books = SharedDepth::default();
    let _feed = tokio::spawn(depth_feed::run(
        endpoint,
        AREA,
        fast_tuning(),
        books.clone(),
    ));

    // Then the delta's whole view wins for contract 7 (full state per view, not a merge)…
    await_view(&books, 7, Some(view(7, &[4_250], &[4_350]))).await;
    // …and the bid-less seed view is stored verbatim for contract 8
    let view_8 = books.lock().unwrap().get(&key(8)).cloned().unwrap();
    assert_eq!(view_8, view(8, &[], &[4_500]));

    // And the read-time reduction prices the clean book at its midpoint, while the one-sided
    // book yields no mid — the crossing loop will withhold
    let view_7 = books.lock().unwrap().get(&key(7)).cloned().unwrap();
    assert_eq!(top_of_book(&view_7).mid(), Some(4_300));
    assert_eq!(top_of_book(&view_8).mid(), None);
}

#[tokio::test]
async fn a_wrong_delivery_area_fails_the_feed_fatally() {
    // Given a connector serving area 40
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    let (_shutdown, _server) = start_mock(listener, AREA, vec![]);

    // When the feed subscribes for area CH, run returns the fatal error promptly — a permanent
    // config mismatch (wrong connector), not a retry loop
    let error = timeout(
        Duration::from_secs(5),
        depth_feed::run(
            endpoint,
            EicCode::AREA_CH,
            fast_tuning(),
            SharedDepth::default(),
        ),
    )
    .await
    .expect("a rejected area must fail fast, not retry");
    assert!(error.to_string().contains("10YCH-SWISSGRIDZ"), "{error}");
}

#[tokio::test]
async fn a_reconnect_reseeds_and_drops_vanished_books() {
    // Given a feed that has folded a two-book seed
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let endpoint = format!("http://{addr}");
    let (shutdown, server) = start_mock(
        listener,
        AREA,
        vec![update(
            1,
            vec![view(7, &[4_200], &[4_300]), view(8, &[4_000], &[4_100])],
        )],
    );
    let books = SharedDepth::default();
    let _feed = tokio::spawn(depth_feed::run(
        endpoint,
        AREA,
        fast_tuning(),
        books.clone(),
    ));
    await_view(&books, 8, Some(view(8, &[4_000], &[4_100]))).await;

    // When the connector goes away, every book is forgotten — no live feed means no live mid,
    // so nothing can be priced off pre-outage state
    stop_mock(shutdown, server).await;
    await_view(&books, 7, None).await;
    await_view(&books, 8, None).await;

    // And when it comes back with a seed that no longer carries contract 8…
    let listener = rebind(addr).await;
    let (_shutdown, _server) = start_mock(
        listener,
        AREA,
        vec![update(9, vec![view(7, &[4_260], &[4_340])])],
    );

    // …then the re-seed restores contract 7 with the fresh view and contract 8 stays gone
    await_view(&books, 7, Some(view(7, &[4_260], &[4_340]))).await;
    assert!(books.lock().unwrap().get(&key(8)).is_none());
}

/// Rebind `addr` after its previous listener shut down, retrying briefly — the OS may take a
/// moment to release the port.
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
