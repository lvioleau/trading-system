//! gRPC `PowerbotExecutionService`: streams own external trades and own-order
//! revisions, seed-then-deltas. (Internal trades are not exposed — they are
//! recorded durably and, on a shadow gateway, normalized onto the own-trades
//! stream at ingest; see [`ExecutionChannels::ingest_internal_trades`](super::feed::ExecutionChannels::ingest_internal_trades).)
//!
//! Each stream is backed by its own [`Channel`](super::feed::Channel) — one
//! store, one broadcast, one `seq`. A subscribe first attaches to the broadcast,
//! then snapshots the store; the snapshot is emitted as the first frame (the
//! seed) and live deltas follow, with the `seq > seed_seq` filter dropping the
//! overlap between the two — gap-free and duplicate-free without a lock held
//! across both (docs/design.md, "Stores and gRPC streams"). A subscriber that
//! can't keep up (lags the broadcast) has its stream ended with
//! `RESOURCE_EXHAUSTED`; on shutdown every stream ends with `UNAVAILABLE`. The
//! client's one rule: any terminal status ⇒ reconnect. Frames are upserts keyed
//! by id, so a reconnect re-seeds idempotently. Follows nordpool-stream-server.
//!
//! Every subscription carries a required `portfolio_id` scope, which must name
//! one of the portfolios this gateway serves — anything else, including empty, is
//! rejected at subscribe time with `INVALID_ARGUMENT`. Each subscriber states
//! its scope explicitly, so a caller whose portfolio expectation the gateway
//! cannot meet has a config mismatch that fails fast instead of a stream it
//! misreads (for a position consumer, misattributed fills are position
//! corruption). The seed and every delta are trimmed to the scoped portfolio —
//! on a multi-portfolio gateway that trim is what hands each subscriber exactly
//! its portfolio's stream; a delta batch left empty by the trim is not sent at
//! all (`seq` is an overlap dedupe, not a gap detector, so skipped values are
//! fine).

#![allow(clippy::result_large_err)]

use std::pin::Pin;
use std::sync::Arc;

use tokio::sync::{broadcast, watch};
use tokio_stream::wrappers::BroadcastStream;
use tokio_stream::wrappers::errors::BroadcastStreamRecvError;
use tokio_stream::{Stream, StreamExt};
use tonic::{Request, Response, Status};

use super::feed::{Batch, ExecutionChannels};
use super::proto::powerbot_execution_service_server::PowerbotExecutionService;
use super::proto::{
    OwnOrdersUpdate, OwnTradesUpdate, SubscribeOwnOrdersRequest, SubscribeOwnTradesRequest,
};
use super::subscriber::{SubscriberGuard, SubscriberLimit};
use crate::domain;

/// Stable `feed` metric tags for the three execution streams (subscriber gauge
/// and drop counter).
const FEED_OWN_TRADES: &str = "own_trades";
const FEED_OWN_ORDERS: &str = "own_orders";

/// Serves the three execution streams off the channels the driver publishes
/// into. Each stream has its own subscriber cap; all cut themselves on the
/// shared shutdown watch so the server's graceful drain completes.
pub(crate) struct ExecutionFeed {
    channels: Arc<ExecutionChannels>,
    /// The portfolios this gateway serves (config `portfolio_ids` — they scope
    /// the websocket topics and the reconciliation sweep, so the stores hold
    /// nothing else). A subscribe requesting any other portfolio is rejected;
    /// see the module docs.
    served_portfolio_ids: Vec<String>,
    trade_limit: SubscriberLimit,
    order_limit: SubscriberLimit,
    shutdown: watch::Receiver<bool>,
}

impl ExecutionFeed {
    pub(crate) fn new(
        channels: Arc<ExecutionChannels>,
        served_portfolio_ids: Vec<String>,
        max_subscribers: usize,
        shutdown: watch::Receiver<bool>,
    ) -> Self {
        Self {
            channels,
            served_portfolio_ids,
            trade_limit: SubscriberLimit::new(
                max_subscribers,
                FEED_OWN_TRADES,
                "own-trades subscriber limit reached",
            ),
            order_limit: SubscriberLimit::new(
                max_subscribers,
                FEED_OWN_ORDERS,
                "own-orders subscriber limit reached",
            ),
            shutdown,
        }
    }

    /// Validate a subscribe request's portfolio scope: required, and it must be
    /// one of the served portfolios. Anything else — empty included — is a
    /// config mismatch on the caller's side, rejected fast rather than served
    /// as a stream the caller would misread.
    fn portfolio_scope(&self, requested: &str) -> Result<String, Status> {
        if requested.is_empty() {
            Err(Status::invalid_argument(
                "portfolio_id is required: name the portfolio to subscribe to",
            ))
        } else if self.served_portfolio_ids.iter().any(|p| p == requested) {
            Ok(requested.to_owned())
        } else {
            Err(Status::invalid_argument(format!(
                "portfolio_id {requested:?} is not served by this gateway \
                 (it serves {:?})",
                self.served_portfolio_ids
            )))
        }
    }
}

type TradeStream = Pin<Box<dyn Stream<Item = Result<OwnTradesUpdate, Status>> + Send>>;
type OrderStream = Pin<Box<dyn Stream<Item = Result<OwnOrdersUpdate, Status>> + Send>>;

#[tonic::async_trait]
impl PowerbotExecutionService for ExecutionFeed {
    type SubscribeOwnTradesStream = TradeStream;
    type SubscribeOwnOrdersStream = OrderStream;

    async fn subscribe_own_trades(
        &self,
        request: Request<SubscribeOwnTradesRequest>,
    ) -> Result<Response<Self::SubscribeOwnTradesStream>, Status> {
        let portfolio = self.portfolio_scope(&request.get_ref().portfolio_id)?;
        tracing::info!(portfolio, "own-trades Subscribe");
        let guard = self.trade_limit.acquire()?;
        // Subscribe before snapshotting: any change applied after the seed is
        // then guaranteed to be waiting on `rx`, and the seq filter drops the
        // overlap.
        let rx = self.channels.trades.subscribe();
        let (seed_seq, received_at_nanos, mut items) = self.channels.trades.seed_snapshot();
        items.retain(|t| trade_matches(t, &portfolio));
        let seed = trades_update(Batch {
            received_at_nanos,
            seq: seed_seq,
            items,
        });
        let stream = seeded_stream(
            rx,
            seed_seq,
            seed,
            guard,
            self.shutdown.clone(),
            FEED_OWN_TRADES,
            move |batch| filter_batch(batch, &portfolio, trade_matches).map(trades_update),
        );
        Ok(Response::new(stream))
    }

    async fn subscribe_own_orders(
        &self,
        request: Request<SubscribeOwnOrdersRequest>,
    ) -> Result<Response<Self::SubscribeOwnOrdersStream>, Status> {
        let portfolio = self.portfolio_scope(&request.get_ref().portfolio_id)?;
        tracing::info!(portfolio, "own-orders Subscribe");
        let guard = self.order_limit.acquire()?;
        let rx = self.channels.orders.subscribe();
        let (seed_seq, received_at_nanos, mut items) = self.channels.orders.seed_snapshot();
        items.retain(|o| order_matches(o, &portfolio));
        let seed = orders_update(Batch {
            received_at_nanos,
            seq: seed_seq,
            items,
        });
        let stream = seeded_stream(
            rx,
            seed_seq,
            seed,
            guard,
            self.shutdown.clone(),
            FEED_OWN_ORDERS,
            move |batch| filter_batch(batch, &portfolio, order_matches).map(orders_update),
        );
        Ok(Response::new(stream))
    }
}

/// Trim a delta batch to the subscription's portfolio scope. A batch left empty
/// by the trim is dropped entirely (`None`) rather than sent as an empty frame —
/// subscribers use `seq` only to dedupe the seed/delta overlap, so a skipped
/// `seq` is fine.
fn filter_batch<T>(
    mut batch: Batch<T>,
    portfolio_id: &str,
    matches: fn(&T, &str) -> bool,
) -> Option<Batch<T>> {
    batch.items.retain(|item| matches(item, portfolio_id));
    if batch.items.is_empty() {
        None
    } else {
        Some(batch)
    }
}

/// An external trade's portfolio is already curated to the leg that is ours (the
/// domain conversion collapsed the buy/sell legs).
fn trade_matches(t: &domain::Trade, portfolio_id: &str) -> bool {
    t.portfolio_id == portfolio_id
}

fn order_matches(o: &domain::OwnOrder, portfolio_id: &str) -> bool {
    o.portfolio_id == portfolio_id
}

/// Build a seed-then-deltas stream: emit `seed` as the first frame, then live
/// deltas from the broadcast — dropping any batch the seed already reflects
/// (`seq <= seed_seq`), ending with `RESOURCE_EXHAUSTED` if the subscriber lags,
/// cutting on shutdown, and chaining a terminal `UNAVAILABLE`. Shared by all
/// three execution streams. `guard` rides along so the subscriber's concurrency
/// permit is released when the stream is dropped. `to_update` may drop a batch
/// (`None`) — how a portfolio-scoped subscription skips batches its trim left
/// empty.
fn seeded_stream<M, U>(
    rx: broadcast::Receiver<Batch<M>>,
    seed_seq: u64,
    seed: U,
    guard: SubscriberGuard,
    mut shutdown: watch::Receiver<bool>,
    feed: &'static str,
    to_update: impl Fn(Batch<M>) -> Option<U> + Send + 'static,
) -> Pin<Box<dyn Stream<Item = Result<U, Status>> + Send>>
where
    M: Clone + Send + 'static,
    U: Send + 'static,
{
    let deltas = BroadcastStream::new(rx).filter_map(move |result| {
        // The guard lives as long as this closure (and so the stream),
        // releasing the permit (and decrementing the gauge) on disconnect.
        let _ = &guard;
        match result {
            // Drop the seed/delta overlap: a batch the seed already reflects.
            Ok(batch) if batch.seq <= seed_seq => None,
            Ok(batch) => to_update(batch).map(Ok),
            Err(BroadcastStreamRecvError::Lagged(skipped)) => {
                // The client can't keep up; end the stream and free the slot —
                // a live client reconnects and re-seeds.
                crate::metrics::subscriber_dropped(feed, "lagged");
                tracing::warn!(skipped, feed, "execution subscriber lagged; dropping");
                Some(Err(Status::resource_exhausted("subscriber lagged")))
            }
        }
    });
    let live = futures_util::StreamExt::take_until(deltas, async move {
        let _ = shutdown.wait_for(|&stop| stop).await;
    })
    .chain(tokio_stream::once(Err(Status::unavailable(
        "server shutting down",
    ))));
    // Seed first, then the live deltas.
    Box::pin(tokio_stream::once(Ok(seed)).chain(live))
}

fn trades_update(batch: Batch<domain::Trade>) -> OwnTradesUpdate {
    OwnTradesUpdate {
        received_at_nanos: batch.received_at_nanos,
        seq: batch.seq,
        trades: batch.items.iter().map(domain::Trade::to_proto).collect(),
    }
}

fn orders_update(batch: Batch<domain::OwnOrder>) -> OwnOrdersUpdate {
    OwnOrdersUpdate {
        received_at_nanos: batch.received_at_nanos,
        seq: batch.seq,
        orders: batch.items.iter().map(domain::OwnOrder::to_proto).collect(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::grpc::proto;

    /// A shutdown receiver that never fires: the sender is leaked to keep the
    /// watch open (a closed watch reads as shutdown and would cut every stream).
    fn no_shutdown() -> watch::Receiver<bool> {
        let (tx, rx) = watch::channel(false);
        std::mem::forget(tx);
        rx
    }

    /// The portfolio every test feed serves (the config `portfolio_id`).
    const SERVED: &str = "PF-1";

    fn channels(capacity: usize) -> Arc<ExecutionChannels> {
        Arc::new(ExecutionChannels::new(capacity))
    }

    fn feed(channels: Arc<ExecutionChannels>, max: usize) -> ExecutionFeed {
        ExecutionFeed::new(channels, vec![SERVED.to_string()], max, no_shutdown())
    }

    /// A trade of the served portfolio — subscriptions are always scoped, so the
    /// default fixture must be in scope to reach the stream. The domain form is
    /// already curated to our side (`side` + `portfolio_id`).
    fn trade(id: &str) -> domain::Trade {
        domain::Trade {
            trade_id: id.to_string(),
            quantity: 5000,
            side: proto::OrderSide::Buy,
            portfolio_id: SERVED.to_string(),
            ..domain::Trade::default()
        }
    }

    /// A trade whose our-side leg carries `portfolio`.
    fn portfolio_trade(id: &str, bought: bool, portfolio: &str) -> domain::Trade {
        domain::Trade {
            side: if bought {
                proto::OrderSide::Buy
            } else {
                proto::OrderSide::Sell
            },
            portfolio_id: portfolio.to_string(),
            ..trade(id)
        }
    }

    /// An order of the served portfolio (in scope).
    fn order(id: &str) -> domain::OwnOrder {
        domain::OwnOrder {
            order_id: id.to_string(),
            portfolio_id: SERVED.to_string(),
            state: proto::OrderState::Acti,
            side: proto::OrderSide::Buy,
            action: proto::OrderAction::Uadd,
            delivery_area_eic: "AREA".to_string(),
            ..domain::OwnOrder::default()
        }
    }

    fn portfolio_order(id: &str, portfolio: &str) -> domain::OwnOrder {
        domain::OwnOrder {
            portfolio_id: portfolio.to_string(),
            ..order(id)
        }
    }

    #[tokio::test]
    async fn subscribe_own_trades_seeds_current_state_then_streams_deltas() {
        // A trade already in the store (published before anyone subscribed) is
        // part of the seed — the whole point of seed-then-deltas.
        let channels = channels(16);
        let feed = feed(channels.clone(), 8);
        channels.trades.publish(vec![trade("SEEDED")], 0);

        let mut stream = feed
            .subscribe_own_trades(Request::new(SubscribeOwnTradesRequest {
                portfolio_id: SERVED.to_string(),
            }))
            .await
            .expect("subscribe accepted")
            .into_inner();

        // First frame is the seed carrying the pre-existing trade.
        let seed = stream.next().await.unwrap().unwrap();
        assert_eq!(
            seed.trades.iter().map(|t| &t.trade_id).collect::<Vec<_>>(),
            vec!["SEEDED"]
        );
        assert_eq!(seed.trades[0].side, proto::OrderSide::Buy as i32);

        // A later publish arrives as a delta, at a higher seq than the seed.
        channels.trades.publish(vec![trade("NEW")], 0);
        let delta = stream.next().await.unwrap().unwrap();
        assert_eq!(
            delta.trades.iter().map(|t| &t.trade_id).collect::<Vec<_>>(),
            vec!["NEW"]
        );
        assert!(delta.seq > seed.seq);
    }

    #[tokio::test]
    async fn subscribe_emits_an_empty_seed_when_the_store_is_empty() {
        let channels = channels(16);
        let feed = feed(channels.clone(), 8);
        let mut stream = feed
            .subscribe_own_orders(Request::new(SubscribeOwnOrdersRequest {
                portfolio_id: SERVED.to_string(),
            }))
            .await
            .expect("subscribe accepted")
            .into_inner();

        // The seed is still the first frame, just empty at seq 0.
        let seed = stream.next().await.unwrap().unwrap();
        assert!(seed.orders.is_empty());
        assert_eq!(seed.seq, 0);

        channels.orders.publish(vec![order("O1"), order("O2")], 0);
        let delta = stream.next().await.unwrap().unwrap();
        assert_eq!(
            delta.orders.iter().map(|o| &o.order_id).collect::<Vec<_>>(),
            vec!["O1", "O2"]
        );
        assert_eq!(delta.orders[0].action, proto::OrderAction::Uadd as i32);
    }

    #[tokio::test]
    async fn subscribe_drops_the_seed_delta_overlap_by_seq() {
        // A trade seeded at seq 1; a duplicate delivered on the broadcast at the
        // same seq (the subscribe/seed race) must be dropped, while a genuine
        // later delta at a higher seq is forwarded.
        let channels = channels(16);
        let feed = feed(channels.clone(), 8);
        channels.trades.publish(vec![trade("SEEDED")], 0); // seq 1

        let mut stream = feed
            .subscribe_own_trades(Request::new(SubscribeOwnTradesRequest {
                portfolio_id: SERVED.to_string(),
            }))
            .await
            .expect("subscribe accepted")
            .into_inner();
        let seed = stream.next().await.unwrap().unwrap();
        assert_eq!(seed.seq, 1);

        // An overlap frame at seq <= seed_seq (dropped) then a fresh one (kept).
        channels
            .trades
            .updates
            .send(Batch {
                received_at_nanos: 0,
                seq: 1,
                items: vec![trade("DUP")],
            })
            .unwrap();
        channels
            .trades
            .updates
            .send(Batch {
                received_at_nanos: 0,
                seq: 2,
                items: vec![trade("FRESH")],
            })
            .unwrap();

        let delta = stream.next().await.unwrap().unwrap();
        assert_eq!(
            delta.trades.iter().map(|t| &t.trade_id).collect::<Vec<_>>(),
            vec!["FRESH"]
        );
    }

    #[tokio::test]
    async fn subscribe_drops_a_lagged_subscriber_with_resource_exhausted() {
        // A tiny broadcast (capacity 1): a subscriber that reads its seed but not
        // the deltas falls behind after more sends than the buffer holds.
        let channels = channels(1);
        let feed = feed(channels.clone(), 8);
        let mut stream = feed
            .subscribe_own_trades(Request::new(SubscribeOwnTradesRequest {
                portfolio_id: SERVED.to_string(),
            }))
            .await
            .expect("subscribe accepted")
            .into_inner();
        // Consume the (empty) seed; the broadcast is untouched by it.
        let _seed = stream.next().await.unwrap().unwrap();

        // Distinct ids so the guarded upsert doesn't suppress any of them.
        channels.trades.publish(vec![trade("T1")], 0);
        channels.trades.publish(vec![trade("T2")], 0);
        channels.trades.publish(vec![trade("T3")], 0);

        let end = stream.next().await.unwrap();
        assert_eq!(
            end.expect_err("terminal status").code(),
            tonic::Code::ResourceExhausted
        );
    }

    #[tokio::test]
    async fn subscribe_ends_the_stream_with_unavailable_on_shutdown() {
        let channels = channels(16);
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let feed = ExecutionFeed::new(channels, vec![SERVED.to_string()], 8, shutdown_rx);
        let mut stream = feed
            .subscribe_own_orders(Request::new(SubscribeOwnOrdersRequest {
                portfolio_id: SERVED.to_string(),
            }))
            .await
            .expect("subscribe accepted")
            .into_inner();
        let _seed = stream.next().await.unwrap().unwrap();

        shutdown_tx.send(true).unwrap();

        let end = stream.next().await.unwrap();
        assert_eq!(
            end.expect_err("terminal status").code(),
            tonic::Code::Unavailable
        );
        assert!(stream.next().await.is_none());
    }

    #[tokio::test]
    async fn subscribe_enforces_the_max_subscriber_cap_per_stream() {
        let channels = channels(16);
        let feed = ExecutionFeed::new(channels, vec![SERVED.to_string()], 1, no_shutdown());
        let req = || {
            Request::new(SubscribeOwnTradesRequest {
                portfolio_id: SERVED.to_string(),
            })
        };

        let first = feed.subscribe_own_trades(req()).await;
        assert!(first.is_ok());
        let second = feed.subscribe_own_trades(req()).await;
        assert_eq!(
            second.err().expect("second rejected").code(),
            tonic::Code::ResourceExhausted
        );
        // The order stream has its own independent permit, so it is unaffected.
        assert!(
            feed.subscribe_own_orders(Request::new(SubscribeOwnOrdersRequest {
                portfolio_id: SERVED.to_string()
            }))
            .await
            .is_ok()
        );
        drop(first);
        assert!(feed.subscribe_own_trades(req()).await.is_ok());
    }

    #[tokio::test]
    async fn subscribe_trims_the_seed_and_skips_foreign_delta_batches() {
        // Seeded state: one trade ours, one foreign. Foreign entries should not
        // occur while the websocket topics are scoped by config — the trim is
        // what makes the subscription's scope a guarantee rather than a
        // convention.
        let channels = channels(16);
        let feed = feed(channels.clone(), 8);
        channels
            .trades
            .publish(vec![portfolio_trade("OURS", true, SERVED)], 0);
        channels
            .trades
            .publish(vec![portfolio_trade("FOREIGN", true, "PF-2")], 0);

        let mut stream = feed
            .subscribe_own_trades(Request::new(SubscribeOwnTradesRequest {
                portfolio_id: SERVED.to_string(),
            }))
            .await
            .expect("subscribe accepted")
            .into_inner();

        // The seed carries only our trade.
        let seed = stream.next().await.unwrap().unwrap();
        assert_eq!(
            seed.trades.iter().map(|t| &t.trade_id).collect::<Vec<_>>(),
            vec!["OURS"]
        );

        // A delta batch that trims to empty is skipped outright (no empty
        // frame); the next frame delivered is the following matching batch — a
        // sell-leg trade of ours this time — at a seq that skips the foreign
        // batch's.
        channels
            .trades
            .publish(vec![portfolio_trade("FOREIGN-2", true, "PF-2")], 0);
        channels
            .trades
            .publish(vec![portfolio_trade("OURS-2", false, SERVED)], 0);
        let delta = stream.next().await.unwrap().unwrap();
        assert_eq!(
            delta.trades.iter().map(|t| &t.trade_id).collect::<Vec<_>>(),
            vec!["OURS-2"]
        );
        assert!(
            delta.seq > seed.seq + 1,
            "the foreign batch's seq is skipped"
        );
    }

    #[tokio::test]
    async fn two_served_portfolios_stream_independently() {
        // Given a gateway serving two portfolios with a trade of each in the
        // store, two scoped subscribers each see exactly their own — the
        // multi-portfolio serving path.
        let channels = channels(16);
        let feed = ExecutionFeed::new(
            channels.clone(),
            vec![SERVED.to_string(), "PF-9".to_string()],
            8,
            no_shutdown(),
        );
        channels
            .trades
            .publish(vec![portfolio_trade("T-ONE", true, SERVED)], 0);
        channels
            .trades
            .publish(vec![portfolio_trade("T-NINE", true, "PF-9")], 0);

        for (scope, expected) in [(SERVED, "T-ONE"), ("PF-9", "T-NINE")] {
            let mut stream = feed
                .subscribe_own_trades(Request::new(SubscribeOwnTradesRequest {
                    portfolio_id: scope.to_string(),
                }))
                .await
                .expect("subscribe accepted")
                .into_inner();
            let seed = stream.next().await.unwrap().unwrap();
            assert_eq!(
                seed.trades.iter().map(|t| &t.trade_id).collect::<Vec<_>>(),
                vec![expected],
                "scope {scope}"
            );
        }

        // A portfolio outside the served pair is still rejected.
        let err = feed
            .subscribe_own_trades(Request::new(SubscribeOwnTradesRequest {
                portfolio_id: "PF-2".to_string(),
            }))
            .await
            .err()
            .expect("unserved rejected");
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
    }

    #[tokio::test]
    async fn subscribe_naming_an_unserved_portfolio_is_rejected() {
        // A caller asking for a portfolio this gateway does not serve has a
        // config mismatch: reject at subscribe time (all three streams), never
        // serve a stream the caller would misread.
        let channels = channels(16);
        let feed = feed(channels, 8);

        let err = feed
            .subscribe_own_trades(Request::new(SubscribeOwnTradesRequest {
                portfolio_id: "PF-2".to_string(),
            }))
            .await
            .err()
            .expect("own-trades rejected");
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
        // The message names both sides of the mismatch.
        assert!(err.message().contains("PF-2"), "{err}");
        assert!(err.message().contains(SERVED), "{err}");

        let err = feed
            .subscribe_own_orders(Request::new(SubscribeOwnOrdersRequest {
                portfolio_id: "PF-2".to_string(),
            }))
            .await
            .err()
            .expect("own-orders rejected");
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
    }

    #[tokio::test]
    async fn subscribe_without_a_portfolio_is_rejected() {
        // The scope is required: an empty portfolio_id (an old or careless
        // client) is rejected, not treated as "everything".
        let channels = channels(16);
        let feed = feed(channels, 8);

        let err = feed
            .subscribe_own_trades(Request::new(SubscribeOwnTradesRequest::default()))
            .await
            .err()
            .expect("own-trades rejected");
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
        assert!(err.message().contains("required"), "{err}");

        let err = feed
            .subscribe_own_orders(Request::new(SubscribeOwnOrdersRequest::default()))
            .await
            .err()
            .expect("own-orders rejected");
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
    }

    #[tokio::test]
    async fn own_orders_are_trimmed_to_the_scoped_portfolio() {
        let channels = channels(16);
        let feed = feed(channels.clone(), 8);
        channels
            .orders
            .publish(vec![portfolio_order("O-OURS", SERVED)], 0);
        channels
            .orders
            .publish(vec![portfolio_order("O-FOREIGN", "PF-2")], 0);

        let mut stream = feed
            .subscribe_own_orders(Request::new(SubscribeOwnOrdersRequest {
                portfolio_id: SERVED.to_string(),
            }))
            .await
            .expect("subscribe accepted")
            .into_inner();

        let seed = stream.next().await.unwrap().unwrap();
        assert_eq!(
            seed.orders.iter().map(|o| &o.order_id).collect::<Vec<_>>(),
            vec!["O-OURS"]
        );

        // Foreign order revisions are skipped; ours arrive.
        channels
            .orders
            .publish(vec![portfolio_order("O-FOREIGN-2", "PF-2")], 0);
        channels
            .orders
            .publish(vec![portfolio_order("O-OURS-2", SERVED)], 0);
        let delta = stream.next().await.unwrap().unwrap();
        assert_eq!(
            delta.orders.iter().map(|o| &o.order_id).collect::<Vec<_>>(),
            vec!["O-OURS-2"]
        );
    }
}
