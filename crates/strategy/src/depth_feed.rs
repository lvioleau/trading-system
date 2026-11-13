//! The depth feed: a subscription to the connector's top-N depth stream for the strategy's
//! delivery area, translated at the boundary to the domain [`Book`] (all top-N levels, verbatim)
//! and folded into the shared [`SharedBooks`] view — the live
//! [`DepthSource`](crate::driver::DepthSource) whose books ride every [`StrategyInput`] and whose
//! mids the cradle books on each emitted target. The only module that knows both the connector
//! wire types and the domain book: the crosser's depth feed re-cut onto the mock's simpler
//! reconnect (like `contract_feed`, a plain capped backoff — this is a mock's dependency).
//!
//! **Stream discipline.** Seed-then-deltas: the first update of a session carries the current
//! view of every book, then each update carries the books one frame changed. Every `DepthView`
//! is full top-N state for its book (not a delta), so the fold is a plain overwrite keyed by
//! contract. The connector already drops deltas with `seq <=` the seed's, so no client-side seq
//! filtering is needed.
//!
//! **Liveness discipline.** A book in a strategy input must be *live*: the map is cleared on the
//! seed (books that vanished during a disconnect must not linger) **and on session end** — while
//! the feed is down there are no books at all and the cradle withholds its emissions; the
//! reconnect's seed repopulates everything at once. A malformed view (no market type) is skipped
//! with a warning: the book simply stays absent, which withholds — the safe direction.
//!
//! [`Book`]: crate::driver::Book
//! [`StrategyInput`]: crate::driver::StrategyInput

use anyhow::Result;
use flow_util::config::FeedConfig;
use tokio::time::Duration;
use tokio_stream::StreamExt;
use tracing::{info, warn};

use eic::EicCode;

use crate::driver::{Book, Level, MarketType, SharedBooks};
use crate::marketdata;
use crate::marketdata::depth_book_service_client::DepthBookServiceClient;

const INITIAL_BACKOFF: Duration = Duration::from_millis(500);
const MAX_BACKOFF: Duration = Duration::from_secs(30);

/// Subscribe to the connector's depth stream forever, folding each contract's book for
/// `delivery_area_eic` into `books`; reconnect with capped backoff on any stream end or error.
pub async fn run(endpoint: String, delivery_area_eic: EicCode, books: SharedBooks) {
    let mut backoff = INITIAL_BACKOFF;
    loop {
        let result = subscribe(&endpoint, delivery_area_eic, &books).await;
        // No live feed means no live books: forget them all, so the cradle withholds instead of
        // feeding the strategy the last pre-outage market. The reconnect's seed re-fills.
        books.lock().expect("depth map poisoned").clear();
        match result {
            Ok(()) => {
                warn!(endpoint, "depth stream closed by server");
                backoff = INITIAL_BACKOFF; // a clean session; treat the next attempt as fresh
            }
            Err(error) => warn!(
                endpoint,
                error = format!("{error:#}"),
                "depth subscription failed"
            ),
        }
        tokio::time::sleep(backoff).await;
        backoff = (backoff * 2).min(MAX_BACKOFF);
    }
}

/// One session: connect, subscribe (the area is a guard the connector enforces — NOT_FOUND means
/// the wrong connector, retried like everything else here), then fold updates until the stream
/// ends. The first update of a session is the seed: the full current view of every book, folded
/// over a cleared map so books that vanished while disconnected do not linger.
async fn subscribe(endpoint: &str, delivery_area_eic: EicCode, books: &SharedBooks) -> Result<()> {
    // The mock is not tunable — the shared keepalive defaults are exactly what it wants.
    let channel = flow_util::feed::endpoint(endpoint, &FeedConfig::default())?
        .connect()
        .await?;
    let mut client = DepthBookServiceClient::new(channel);
    let mut stream = client
        .subscribe(marketdata::SubscribeDepthRequest {
            eic_code: delivery_area_eic.to_string(),
        })
        .await?
        .into_inner();
    info!(endpoint, %delivery_area_eic, "subscribed to depth");
    let mut seeded = false;
    while let Some(update) = stream.next().await {
        fold(update?, !seeded, books);
        seeded = true;
    }
    Ok(())
}

/// Fold one update's views into the shared map — each translated to its domain [`Book`] (all
/// top-N levels, best-first, verbatim) and overwritten in place, since every view is the full
/// state of its book. `reset` (the session's seed) replaces the whole map.
fn fold(update: marketdata::DepthUpdate, reset: bool, books: &SharedBooks) {
    let mut books = books.lock().expect("depth map poisoned");
    if reset {
        books.clear();
    }
    for view in update.views {
        let market_type = match view.market_type() {
            marketdata::MarketType::Local => MarketType::Local,
            marketdata::MarketType::Xbid => MarketType::Xbid,
            // Half the contract identity is missing: skip the view. The book just stays absent,
            // which withholds the contract's emission — the safe direction.
            marketdata::MarketType::None => {
                warn!(
                    contract_id = view.contract_id,
                    "skipping depth view with no market type"
                );
                continue;
            }
        };
        let book = Book {
            bids: view.bids.iter().map(to_domain_level).collect(),
            asks: view.asks.iter().map(to_domain_level).collect(),
        };
        books.insert((market_type, view.contract_id), book);
    }
}

/// Wire → domain: the level's price and visible quantity, venue-native integers verbatim.
fn to_domain_level(level: &marketdata::Level) -> Level {
    Level {
        price: level.price,
        quantity: level.total_quantity,
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::{Arc, Mutex};

    use super::*;

    fn view(market_type: marketdata::MarketType) -> marketdata::DepthView {
        let level = |price, total_quantity| marketdata::Level {
            price,
            total_quantity,
            order_count: 1,
            created_at_nanos: 1_000,
            updated_at_nanos: 2_000,
        };
        marketdata::DepthView {
            contract_id: 7,
            market_type: market_type as i32,
            revision: 1,
            bids: vec![level(4_100, 5_000), level(4_000, 9_000)],
            asks: vec![],
        }
    }

    #[test]
    fn views_translate_to_full_domain_books() {
        let books: SharedBooks = Arc::new(Mutex::new(HashMap::new()));
        fold(
            marketdata::DepthUpdate {
                views: vec![view(marketdata::MarketType::Xbid)],
                received_at_nanos: 0,
                seq: 1,
            },
            true,
            &books,
        );
        let book = books.lock().unwrap()[&(MarketType::Xbid, 7)].clone();
        // Every level survives, best-first; a side with no resting orders is empty.
        assert_eq!(
            book.bids,
            vec![
                Level {
                    price: 4_100,
                    quantity: 5_000
                },
                Level {
                    price: 4_000,
                    quantity: 9_000
                },
            ]
        );
        assert!(book.asks.is_empty());
        assert_eq!(
            book.best_bid(),
            Some(&Level {
                price: 4_100,
                quantity: 5_000
            })
        );
    }

    #[test]
    fn the_seed_replaces_the_whole_map_and_a_delta_merges() {
        let stale = (MarketType::Local, 999);
        let books: SharedBooks = Arc::new(Mutex::new(HashMap::from([(stale, Book::default())])));
        // A seed clears the pre-outage state; the stale book must not linger.
        fold(
            marketdata::DepthUpdate {
                views: vec![view(marketdata::MarketType::Xbid)],
                received_at_nanos: 0,
                seq: 1,
            },
            true,
            &books,
        );
        assert!(!books.lock().unwrap().contains_key(&stale));
        // A delta overwrites in place, keeping the rest.
        fold(
            marketdata::DepthUpdate {
                views: vec![view(marketdata::MarketType::Local)],
                received_at_nanos: 0,
                seq: 2,
            },
            false,
            &books,
        );
        let books = books.lock().unwrap();
        assert_eq!(books.len(), 2);
        assert!(books.contains_key(&(MarketType::Xbid, 7)));
        assert!(books.contains_key(&(MarketType::Local, 7)));
    }

    #[test]
    fn a_view_with_no_market_type_is_skipped() {
        // Half the contract identity is missing: the book stays absent (withholds) rather than
        // folding under a guessed key.
        let books: SharedBooks = Arc::new(Mutex::new(HashMap::new()));
        fold(
            marketdata::DepthUpdate {
                views: vec![view(marketdata::MarketType::None)],
                received_at_nanos: 0,
                seq: 1,
            },
            true,
            &books,
        );
        assert!(books.lock().unwrap().is_empty());
    }
}
