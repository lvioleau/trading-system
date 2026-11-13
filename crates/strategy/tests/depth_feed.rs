//! Black-box test of the depth feed against an in-process mock connector: subscribe, translate
//! each streamed view to its domain book keyed by contract identity, and — the liveness
//! discipline — replace the whole map on a reconnect's seed so no pre-outage book lingers.

use std::collections::HashMap;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::net::TcpListener;
use tokio_stream::wrappers::TcpListenerStream;
use tokio_stream::{Stream, StreamExt};
use tonic::{Request, Response, Status};

use eic::EicCode;
use strategy::depth_feed;
use strategy::driver::{Book, ContractId, Level, MarketType, SharedBooks};
use strategy::marketdata;
use strategy::marketdata::depth_book_service_server::{DepthBookService, DepthBookServiceServer};

/// A mock connector serving one seed per subscription session: session N replays `seeds[N]`
/// (the last one repeating). The first session then stays open only while the test holds
/// `first_session_hold`'s sender — dropping it closes the stream, forcing the feed through its
/// reconnect path at a moment the test controls; later sessions hold open forever.
struct MockDepth {
    seeds: Vec<Vec<marketdata::DepthView>>,
    sessions: AtomicUsize,
    first_session_hold: Mutex<Option<tokio::sync::mpsc::Receiver<UpdateItem>>>,
}

type UpdateItem = Result<marketdata::DepthUpdate, Status>;
type UpdateStream = Pin<Box<dyn Stream<Item = UpdateItem> + Send>>;

#[tonic::async_trait]
impl DepthBookService for MockDepth {
    type SubscribeStream = UpdateStream;

    async fn subscribe(
        &self,
        _request: Request<marketdata::SubscribeDepthRequest>,
    ) -> Result<Response<Self::SubscribeStream>, Status> {
        let session = self.sessions.fetch_add(1, Ordering::SeqCst);
        let update = marketdata::DepthUpdate {
            views: self.seeds[session.min(self.seeds.len() - 1)].clone(),
            received_at_nanos: 0,
            seq: session as i64,
        };
        let seed = tokio_stream::once(Ok(update));
        let stream: UpdateStream = match self.first_session_hold.lock().unwrap().take() {
            // The first session: open until the test drops the sender (nothing is ever sent).
            Some(hold) => Box::pin(seed.chain(tokio_stream::wrappers::ReceiverStream::new(hold))),
            None => Box::pin(seed.chain(tokio_stream::pending())),
        };
        Ok(Response::new(stream))
    }
}

async fn start_mock(mock: MockDepth) -> String {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    tokio::spawn(
        tonic::transport::Server::builder()
            .add_service(DepthBookServiceServer::new(mock))
            .serve_with_incoming(TcpListenerStream::new(listener)),
    );
    endpoint
}

/// A wire view with one level per given side price — best-first is trivially satisfied.
fn wire_view(contract_id: i64, bid: Option<i64>, ask: Option<i64>) -> marketdata::DepthView {
    let level = |price| marketdata::Level {
        price,
        total_quantity: 5_000,
        order_count: 1,
        created_at_nanos: 1_000,
        updated_at_nanos: 2_000,
    };
    marketdata::DepthView {
        contract_id,
        market_type: marketdata::MarketType::Xbid as i32,
        revision: 1,
        bids: bid.map(level).into_iter().collect(),
        asks: ask.map(level).into_iter().collect(),
    }
}

/// The domain book `wire_view` translates to.
fn domain_book(bid: Option<i64>, ask: Option<i64>) -> Book {
    let level = |price| Level {
        price,
        quantity: 5_000,
    };
    Book {
        bids: bid.map(level).into_iter().collect(),
        asks: ask.map(level).into_iter().collect(),
    }
}

/// Poll the shared view until `condition` holds, panicking after five seconds.
async fn wait_for(
    books: &SharedBooks,
    what: &str,
    condition: impl Fn(&HashMap<ContractId, Book>) -> bool,
) {
    let reached = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if condition(&books.lock().unwrap()) {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await;
    assert!(reached.is_ok(), "timed out waiting for: {what}");
}

#[tokio::test]
async fn depth_feed_folds_books_and_a_reconnect_seed_replaces_them() {
    // First session: two books. Once the test drops `hold` it closes, and the reconnect's seed
    // carries only one — the book that vanished during the outage must not linger.
    let (hold, first_session_hold) = tokio::sync::mpsc::channel(1);
    let endpoint = start_mock(MockDepth {
        seeds: vec![
            vec![
                wire_view(100, Some(4_100), Some(4_300)),
                wire_view(101, Some(10), None),
            ],
            vec![wire_view(101, Some(10), Some(20))],
        ],
        sessions: AtomicUsize::new(0),
        first_session_hold: Mutex::new(Some(first_session_hold)),
    })
    .await;

    let books: SharedBooks = Arc::new(Mutex::new(HashMap::new()));
    tokio::spawn(depth_feed::run(endpoint, EicCode::AREA_FR, books.clone()));

    wait_for(&books, "the first seed folded", |b| b.len() == 2).await;
    {
        let b = books.lock().unwrap();
        assert_eq!(
            b[&(MarketType::Xbid, 100)],
            domain_book(Some(4_100), Some(4_300))
        );
        assert_eq!(b[&(MarketType::Xbid, 101)], domain_book(Some(10), None));
    }

    // Close the first session; the reconnect's seed replaces the whole map — contract 100 is
    // gone.
    drop(hold);
    wait_for(&books, "the reconnect seed to replace the map", |b| {
        b.len() == 1 && b.contains_key(&(MarketType::Xbid, 101))
    })
    .await;
    assert_eq!(
        books.lock().unwrap()[&(MarketType::Xbid, 101)],
        domain_book(Some(10), Some(20))
    );
}
