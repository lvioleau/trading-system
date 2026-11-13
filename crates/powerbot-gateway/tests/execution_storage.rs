//! Integration test for the `execution` storage layer, contract-service style:
//! gated on `DATABASE_URL` (skips when unset), run against the throwaway dev
//! Postgres (`trading-system/scripts/dev-stack.sh`, the `postgres` service). Exercises the enum
//! columns and the never-regress guard end to end.
//!
//! The storage layer is write-only in production (the stores are seeded from the
//! REST sweep and live feed, not from Postgres), so these tests read back with a
//! raw query rather than through a load method.

use powerbot_gateway::storage::ExecutionStorage;
use powerbot_models::models::{OrderAction, OrderSide, OrderState, OwnOrder, Trade, TradeState};
use sqlx::Row;
use sqlx::postgres::PgPool;

/// Skip helper: returns the DB url, or `None` (with a note) when unset.
fn db_url() -> Option<String> {
    match std::env::var("DATABASE_URL") {
        Ok(url) => Some(url),
        Err(_) => {
            eprintln!("DATABASE_URL unset; skipping execution_storage integration test");
            None
        }
    }
}

/// A trade round-trips through Postgres — including its `state` enum — and the
/// never-regress guard keeps a stale `api_timestamp` from overwriting a fresher
/// one.
#[tokio::test]
async fn trades_round_trip_with_enum_and_guard() {
    let Some(url) = db_url() else { return };
    let storage = ExecutionStorage::connect(&url).await.expect("connect");

    // A unique id per run so concurrent/repeated runs don't collide.
    let id = format!("test-trade-{}", std::process::id());
    let mut fresh = trade(&id, "2026-07-09T10:00:05Z", 5.0);
    fresh.state = Some(TradeState::Acti);
    storage.record_trades(&[fresh]).await.expect("record fresh");

    // An older api_timestamp (and different state) must not overwrite.
    let mut stale = trade(&id, "2026-07-09T10:00:00Z", 9.0);
    stale.state = Some(TradeState::Cncl);
    storage.record_trades(&[stale]).await.expect("record stale");

    let pool = PgPool::connect(&url).await.expect("read-back pool");
    let row = sqlx::query(
        "select quantity, state::text as state from execution.trades where trade_id = $1",
    )
    .bind(&id)
    .fetch_one(&pool)
    .await
    .expect("the recorded trade is present");
    let quantity: Option<f64> = row.try_get("quantity").expect("quantity");
    let state: Option<String> = row.try_get("state").expect("state");
    assert_eq!(quantity, Some(5.0)); // stale write rejected
    assert_eq!(state.as_deref(), Some("ACTI")); // enum round-trips
}

/// An order revision round-trips, including its `state` / `action` / `side` enum
/// columns and `revision_no`.
#[tokio::test]
async fn orders_round_trip_with_enums() {
    let Some(url) = db_url() else { return };
    let storage = ExecutionStorage::connect(&url).await.expect("connect");

    let id = format!("test-order-{}", std::process::id());
    let mut o = OwnOrder::new(
        OrderState::Acti,
        "10YFR-RTE------C".to_string(),
        OrderSide::Buy,
        OrderAction::Uadd,
    );
    o.order_id = Some(id.clone());
    o.revision_no = Some(7);
    o.delivery_end = Some("2999-01-01T00:00:00Z".to_string());
    o.price = Some(42.0);
    storage
        .record_order_revisions(&[o])
        .await
        .expect("record order");

    let pool = PgPool::connect(&url).await.expect("read-back pool");
    let row = sqlx::query(
        "select state::text as state, side::text as side, action::text as action,
                revision_no, price
         from execution.order_revisions where order_id = $1
         order by revision_no desc limit 1",
    )
    .bind(&id)
    .fetch_one(&pool)
    .await
    .expect("the recorded order is present");
    let state: String = row.try_get("state").expect("state");
    let side: String = row.try_get("side").expect("side");
    let action: String = row.try_get("action").expect("action");
    let revision_no: Option<i64> = row.try_get("revision_no").expect("revision_no");
    let price: Option<f64> = row.try_get("price").expect("price");
    assert_eq!(state, "ACTI");
    assert_eq!(side, "BUY");
    assert_eq!(action, "UADD");
    assert_eq!(revision_no, Some(7));
    assert_eq!(price, Some(42.0));
}

fn trade(id: &str, api_timestamp: &str, quantity: f64) -> Trade {
    Trade {
        trade_id: Some(id.to_string()),
        api_timestamp: Some(api_timestamp.to_string()),
        delivery_end: Some("2999-01-01T00:00:00Z".to_string()),
        quantity: Some(quantity),
        buy: Some(true),
        ..Trade::new()
    }
}
