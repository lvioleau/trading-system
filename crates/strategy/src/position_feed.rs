//! The position feed: one `Subscribe(portfolio)` to the Position Service for the strategy's
//! portfolio, folded into the shared [`SharedPositions`] baseline — the live
//! [`PositionSource`](crate::driver::PositionSource) every [`StrategyInput`] carries. The only
//! module that knows both the position wire types and the domain [`ActualPosition`]: the
//! crosser's subscription re-cut onto the mock's simpler reconnect (like `contract_feed`, a
//! plain capped backoff — this is a mock's dependency).
//!
//! **Stream discipline.** Snapshot-then-updates, per the service's contract: a session's first
//! message is the portfolio's full current positions (possibly none) and *replaces* the map —
//! that is the moment `None` becomes `Some`, i.e. "an absent contract provably holds 0". Every
//! later batch carries full current values, merged in with a keep-highest-revision guard (the
//! same discipline the crosser folds with), which absorbs any snapshot/stream overlap. An
//! unknown portfolio deliberately seeds empty on the service side, so there is no fatal
//! subscription error — a strategy/service config mismatch shows up as a permanently flat
//! baseline, visible in the logs.
//!
//! **Liveness discipline.** The baseline must be *live*: it is reset to `None` on session end —
//! while the feed is down there is no baseline at all and the cradle withholds every tick (the
//! strategy is never run blind); the reconnect's snapshot re-seeds.
//!
//! [`StrategyInput`]: crate::driver::StrategyInput

use anyhow::Result;
use flow_util::config::FeedConfig;
use tokio::time::Duration;
use tokio_stream::StreamExt;
use tracing::{info, warn};

use crate::driver::{ActualPosition, ContractId, MarketType, SharedPositions};
use crate::position;
use crate::position::position_service_client::PositionServiceClient;

const INITIAL_BACKOFF: Duration = Duration::from_millis(500);
const MAX_BACKOFF: Duration = Duration::from_secs(30);

/// Subscribe to the Position Service forever, maintaining `positions` as the portfolio's live
/// baseline; reconnect with capped backoff on any stream end or error.
pub async fn run(endpoint: String, portfolio: String, positions: SharedPositions) {
    let mut backoff = INITIAL_BACKOFF;
    loop {
        let result = subscribe(&endpoint, &portfolio, &positions).await;
        // No live feed means no live baseline: drop it, so the cradle withholds instead of
        // running the strategy against pre-outage positions. The reconnect's snapshot re-seeds.
        *positions.lock().expect("position map poisoned") = None;
        match result {
            Ok(()) => {
                warn!(endpoint, portfolio, "position stream closed by server");
                backoff = INITIAL_BACKOFF; // a clean session; treat the next attempt as fresh
            }
            Err(error) => warn!(
                endpoint,
                portfolio,
                error = format!("{error:#}"),
                "position subscription failed"
            ),
        }
        tokio::time::sleep(backoff).await;
        backoff = (backoff * 2).min(MAX_BACKOFF);
    }
}

/// One session: connect, subscribe, seed the baseline from the snapshot (the first message),
/// then merge every later batch until the stream ends.
async fn subscribe(endpoint: &str, portfolio: &str, positions: &SharedPositions) -> Result<()> {
    // The mock is not tunable — the shared keepalive defaults are exactly what it wants.
    let channel = flow_util::feed::endpoint(endpoint, &FeedConfig::default())?
        .connect()
        .await?;
    let mut client = PositionServiceClient::new(channel);
    let mut stream = client
        .subscribe(position::SubscribeRequest {
            portfolio: portfolio.to_owned(),
        })
        .await?
        .into_inner();
    info!(endpoint, portfolio, "subscribed to positions");
    let mut seeded = false;
    while let Some(update) = stream.next().await {
        fold(update?, portfolio, !seeded, positions);
        seeded = true;
    }
    Ok(())
}

/// Fold one update into the baseline. The snapshot (`reset`) replaces the whole map — the only
/// point where "heard nothing about a contract" becomes "provably flat"; later batches merge
/// keep-highest-revision, since every position is a full current value.
fn fold(
    update: position::PositionUpdate,
    portfolio: &str,
    reset: bool,
    positions: &SharedPositions,
) {
    let mut baseline = positions.lock().expect("position map poisoned");
    let map = if reset {
        info!(
            portfolio,
            positions = update.positions.len(),
            "position baseline seeded"
        );
        baseline.insert(Default::default())
    } else {
        // Between the seed and a session end the baseline is always Some; a delta can only merge.
        baseline.get_or_insert_with(Default::default)
    };
    for wire in update.positions {
        let Some((id, actual)) = to_domain_position(&wire) else {
            continue;
        };
        // Keep-highest: a snapshot/stream overlap may deliver an older value after a newer one.
        let entry = map.entry(id).or_insert(actual);
        if actual.revision >= entry.revision {
            *entry = actual;
        }
    }
}

/// Wire → domain, integer units verbatim: the contract identity and its current value. A
/// position with no market type (half the contract identity) is skipped with a warning — the
/// contract just keeps its previous value (or stays provably flat).
fn to_domain_position(wire: &position::Position) -> Option<(ContractId, ActualPosition)> {
    let market_type = match wire.market_type() {
        position::MarketType::Local => MarketType::Local,
        position::MarketType::Xbid => MarketType::Xbid,
        position::MarketType::None => {
            warn!(
                contract_id = wire.contract_id,
                "skipping position with no market type"
            );
            return None;
        }
    };
    Some((
        (market_type, wire.contract_id),
        ActualPosition {
            net_quantity: wire.net_quantity,
            revision: wire.revision,
        },
    ))
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use super::*;

    fn wire(contract_id: i64, net_quantity: i64, revision: u64) -> position::Position {
        position::Position {
            portfolio: "strat".to_owned(),
            market_type: position::MarketType::Xbid as i32,
            contract_id,
            net_quantity,
            revision,
            // The write-attribution fields are ignored here (the fold is by revision).
            ..Default::default()
        }
    }

    fn update(positions: Vec<position::Position>) -> position::PositionUpdate {
        position::PositionUpdate { positions }
    }

    #[test]
    fn the_snapshot_seeds_and_an_empty_one_is_provably_flat() {
        let positions: SharedPositions = Arc::new(Mutex::new(None));
        fold(update(vec![]), "strat", true, &positions);
        // Seeded empty: Some (crossing may read it as all-flat), not None (no baseline).
        assert_eq!(*positions.lock().unwrap(), Some(Default::default()));
    }

    #[test]
    fn updates_merge_keep_highest_revision() {
        let positions: SharedPositions = Arc::new(Mutex::new(None));
        fold(update(vec![wire(7, -10_000, 3)]), "strat", true, &positions);
        // A newer value replaces; a stale overlap (lower revision) does not regress it.
        fold(update(vec![wire(7, -9_000, 4)]), "strat", false, &positions);
        fold(
            update(vec![wire(7, -10_000, 3)]),
            "strat",
            false,
            &positions,
        );
        assert_eq!(
            positions.lock().unwrap().as_ref().unwrap()[&(MarketType::Xbid, 7)],
            ActualPosition {
                net_quantity: -9_000,
                revision: 4
            }
        );
    }

    #[test]
    fn a_position_with_no_market_type_is_skipped() {
        let positions: SharedPositions = Arc::new(Mutex::new(None));
        let mut malformed = wire(7, -10_000, 3);
        malformed.market_type = position::MarketType::None as i32;
        fold(
            update(vec![malformed, wire(8, 500, 1)]),
            "strat",
            true,
            &positions,
        );
        let baseline = positions.lock().unwrap();
        let map = baseline.as_ref().unwrap();
        assert_eq!(map.len(), 1);
        assert!(map.contains_key(&(MarketType::Xbid, 8)));
    }
}
