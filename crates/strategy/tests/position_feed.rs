//! Black-box test of the position feed against an in-process mock Position Service: subscribe
//! for the configured portfolio, seed the baseline from the snapshot (empty means provably
//! flat), merge updates — and, the liveness discipline, drop the baseline to `None` on a
//! disconnect so the cradle withholds until the reconnect's snapshot re-seeds.

use std::collections::HashMap;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::net::TcpListener;
use tokio_stream::wrappers::TcpListenerStream;
use tokio_stream::{Stream, StreamExt};
use tonic::{Request, Response, Status};

use strategy::driver::{ActualPosition, ContractId, MarketType, SharedPositions};
use strategy::position;
use strategy::position::position_service_server::{PositionService, PositionServiceServer};
use strategy::position_feed;

/// A mock Position Service serving one scripted session per subscription: session N streams
/// `sessions[N]`'s updates (the last one repeating), the first message being the snapshot. The
/// first session then stays open only while the test holds `first_session_hold`'s sender —
/// dropping it closes the stream, forcing the feed through its reconnect path at a moment the
/// test controls; later sessions hold open forever.
struct MockPositions {
    sessions: Vec<Vec<position::PositionUpdate>>,
    opened: AtomicUsize,
    first_session_hold: Mutex<Option<tokio::sync::mpsc::Receiver<UpdateItem>>>,
}

type UpdateItem = Result<position::PositionUpdate, Status>;
type UpdateStream = Pin<Box<dyn Stream<Item = UpdateItem> + Send>>;

#[tonic::async_trait]
impl PositionService for MockPositions {
    type SubscribeStream = UpdateStream;

    async fn subscribe(
        &self,
        request: Request<position::SubscribeRequest>,
    ) -> Result<Response<Self::SubscribeStream>, Status> {
        assert_eq!(request.into_inner().portfolio, "strat-batt-1");
        let session = self.opened.fetch_add(1, Ordering::SeqCst);
        let updates = self.sessions[session.min(self.sessions.len() - 1)].clone();
        let scripted = tokio_stream::iter(updates.into_iter().map(Ok));
        let stream: UpdateStream = match self.first_session_hold.lock().unwrap().take() {
            // The first session: open until the test drops the sender (nothing is ever sent).
            Some(hold) => {
                Box::pin(scripted.chain(tokio_stream::wrappers::ReceiverStream::new(hold)))
            }
            None => Box::pin(scripted.chain(tokio_stream::pending())),
        };
        Ok(Response::new(stream))
    }

    async fn transfer(
        &self,
        _request: Request<position::TransferRequest>,
    ) -> Result<Response<position::TransferResponse>, Status> {
        Err(Status::unimplemented("not used by the strategy"))
    }
}

async fn start_mock(mock: MockPositions) -> String {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    tokio::spawn(
        tonic::transport::Server::builder()
            .add_service(PositionServiceServer::new(mock))
            .serve_with_incoming(TcpListenerStream::new(listener)),
    );
    endpoint
}

fn wire(contract_id: i64, net_quantity: i64, revision: u64) -> position::Position {
    position::Position {
        portfolio: "strat-batt-1".to_owned(),
        market_type: position::MarketType::Xbid as i32,
        contract_id,
        net_quantity,
        revision,
        ..Default::default()
    }
}

fn update(positions: Vec<position::Position>) -> position::PositionUpdate {
    position::PositionUpdate { positions }
}

/// Poll the shared baseline until `condition` holds, panicking after five seconds.
async fn wait_for(
    positions: &SharedPositions,
    what: &str,
    condition: impl Fn(&Option<HashMap<ContractId, ActualPosition>>) -> bool,
) {
    let reached = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if condition(&positions.lock().unwrap()) {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await;
    assert!(reached.is_ok(), "timed out waiting for: {what}");
}

#[tokio::test]
async fn position_feed_seeds_merges_and_drops_the_baseline_on_disconnect() {
    let contract = (MarketType::Xbid, 7);
    // First session: a snapshot then a delta. Once the test drops `hold` it closes; the
    // reconnect's session seeds an *empty* snapshot — a provably flat baseline, not `None`.
    let (hold, first_session_hold) = tokio::sync::mpsc::channel(1);
    let endpoint = start_mock(MockPositions {
        sessions: vec![
            vec![
                update(vec![wire(7, -10_000, 3)]),
                update(vec![wire(7, -9_000, 4)]),
            ],
            vec![update(vec![])],
        ],
        opened: AtomicUsize::new(0),
        first_session_hold: Mutex::new(Some(first_session_hold)),
    })
    .await;

    let positions: SharedPositions = Arc::new(Mutex::new(None));
    tokio::spawn(position_feed::run(
        endpoint,
        "strat-batt-1".to_owned(),
        positions.clone(),
    ));

    // The snapshot seeds the baseline, the delta merges into it.
    wait_for(&positions, "the delta folded over the seed", |baseline| {
        baseline.as_ref().is_some_and(|map| {
            map.get(&contract)
                == Some(&ActualPosition {
                    net_quantity: -9_000,
                    revision: 4,
                })
        })
    })
    .await;

    // Close the first session. The feed drops the baseline (no live feed → no baseline; the
    // brief None gap is invisible here) and the reconnect's empty snapshot re-seeds it flat:
    // Some and empty — contract 7's pre-outage value must not linger.
    drop(hold);
    wait_for(
        &positions,
        "the reconnect's empty snapshot to re-seed flat",
        |baseline| baseline.as_ref().is_some_and(HashMap::is_empty),
    )
    .await;
}
