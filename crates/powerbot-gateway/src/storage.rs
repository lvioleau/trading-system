//! Durable execution record in Postgres — the gateway's transaction log,
//! following the trading-system/contract-service storage pattern (sqlx, embedded
//! migrations run at boot). Off the hot path: the in-memory stores are the
//! serving authority (memory leads, Postgres trails), and this exists as the
//! durable, write-only record settlement/drift investigations read — it is not
//! read back into the stores at boot. A restarted gateway repopulates its stores
//! from the REST reconciliation sweep and the live feed, and the Position Service
//! is the system of record for fills, so the stores need no local seed.
//!
//! Reconciliation is the writer: each sweep persists the full REST snapshot here
//! (the authoritative source), so we never persist off the possibly-lossy live
//! feed. Columns mirror the PowerBot models (queryable), with state/action/side
//! as Postgres enum types carrying the wire codes. Upserts carry the same guards
//! as the in-memory stores: trades never-regress on `api_timestamp`, order
//! revisions are append-only.
//!
//! It shares the `trading` database with contract-service; the pool's
//! `search_path` is set to `execution` so the tables — and sqlx's own
//! `_sqlx_migrations` — never collide with contract-service's `reference`/`public`.

use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use powerbot_models::models::{InternalTrade, OwnOrder, Trade};
use serde::Serialize;
use sqlx::postgres::{PgConnectOptions, PgPool, PgPoolOptions};
use tokio::sync::{mpsc, watch};

/// A handle on the `execution` tables. Cheap to clone (wraps a connection pool).
#[derive(Debug, Clone)]
pub struct ExecutionStorage {
    pool: PgPool,
}

/// One batch handed to the live-persistence task from the websocket driver — the
/// low-latency companion to the reconciliation sweep, so a fresh fill hits the DB
/// in ~a round trip rather than by the next sweep.
#[derive(Debug)]
pub enum Persist {
    Trades(Vec<Trade>),
    InternalTrades(Vec<InternalTrade>),
    Orders(Vec<OwnOrder>),
}

impl ExecutionStorage {
    /// Connect, create the `execution` schema, pin the connection `search_path`
    /// to it (so `_sqlx_migrations` lands there, isolated from contract-service),
    /// and run the embedded migrations. Idempotent; safe on every boot.
    pub async fn connect(url: &str) -> Result<Self> {
        let options: PgConnectOptions = url.parse().context("parsing database url")?;
        // The schema must exist before sqlx creates `execution._sqlx_migrations`,
        // so create it and set search_path on every connection, before migrating.
        let pool = PgPoolOptions::new()
            .after_connect(|conn, _meta| {
                Box::pin(async move {
                    sqlx::query("create schema if not exists execution")
                        .execute(&mut *conn)
                        .await?;
                    sqlx::query("set search_path to execution")
                        .execute(&mut *conn)
                        .await?;
                    Ok(())
                })
            })
            .connect_with(options)
            .await
            .context("connecting to postgres")?;
        sqlx::migrate!("./migrations")
            .run(&pool)
            .await
            .context("running execution migrations")?;
        Ok(Self { pool })
    }

    /// Upsert external trades, keyed by `trade_id`, never-regressing on
    /// `api_timestamp`. Keyless rows are skipped. One transaction per batch.
    pub async fn record_trades(&self, trades: &[Trade]) -> Result<()> {
        let mut tx = self.pool.begin().await.context("begin record_trades")?;
        for t in trades {
            let Some(id) = t.trade_id.as_deref() else {
                continue;
            };
            sqlx::query(
                "insert into execution.trades as x
                   (trade_id, state, exchange, delivery_area, contract_id, contract_name,
                    delivery_start, delivery_end, price, quantity, buy, sell, self_trade,
                    buy_order_id, sell_order_id, buy_portfolio_id, sell_portfolio_id,
                    buy_strategy_id, sell_strategy_id, buy_unit_id, sell_unit_id,
                    exec_time, api_timestamp)
                 values
                   ($1, $2::execution.trade_state, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12,
                    $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23)
                 on conflict (trade_id) do update set
                   state = excluded.state, exchange = excluded.exchange,
                   delivery_area = excluded.delivery_area, contract_id = excluded.contract_id,
                   contract_name = excluded.contract_name, delivery_start = excluded.delivery_start,
                   delivery_end = excluded.delivery_end, price = excluded.price,
                   quantity = excluded.quantity, buy = excluded.buy, sell = excluded.sell,
                   self_trade = excluded.self_trade, buy_order_id = excluded.buy_order_id,
                   sell_order_id = excluded.sell_order_id, buy_portfolio_id = excluded.buy_portfolio_id,
                   sell_portfolio_id = excluded.sell_portfolio_id, buy_strategy_id = excluded.buy_strategy_id,
                   sell_strategy_id = excluded.sell_strategy_id, buy_unit_id = excluded.buy_unit_id,
                   sell_unit_id = excluded.sell_unit_id, exec_time = excluded.exec_time,
                   api_timestamp = excluded.api_timestamp, recorded_at = now()
                 where x.api_timestamp is null
                    or excluded.api_timestamp is null
                    or excluded.api_timestamp >= x.api_timestamp",
            )
            .bind(id)
            .bind(enum_text_opt(&t.state))
            .bind(t.exchange.as_deref())
            .bind(t.delivery_area.as_deref())
            .bind(t.contract_id.as_deref())
            .bind(t.contract_name.as_deref())
            .bind(to_utc(&t.delivery_start))
            .bind(to_utc(&t.delivery_end))
            .bind(t.price)
            .bind(t.quantity)
            .bind(t.buy)
            .bind(t.sell)
            .bind(t.self_trade)
            .bind(t.buy_order_id.as_deref())
            .bind(t.sell_order_id.as_deref())
            .bind(t.buy_portfolio_id.as_deref())
            .bind(t.sell_portfolio_id.as_deref())
            .bind(t.buy_strategy_id.as_deref())
            .bind(t.sell_strategy_id.as_deref())
            .bind(t.buy_unit_id.as_deref())
            .bind(t.sell_unit_id.as_deref())
            .bind(to_utc(&t.exec_time))
            .bind(to_utc(&t.api_timestamp))
            .execute(&mut *tx)
            .await
            .context("upserting trade")?;
        }
        tx.commit().await.context("commit record_trades")?;
        Ok(())
    }

    /// Upsert internal trades, keyed by `internal_trade_id`, same guard.
    pub async fn record_internal_trades(&self, trades: &[InternalTrade]) -> Result<()> {
        let mut tx = self
            .pool
            .begin()
            .await
            .context("begin record_internal_trades")?;
        for t in trades {
            let Some(id) = t.internal_trade_id.as_deref() else {
                continue;
            };
            sqlx::query(
                "insert into execution.internal_trades as x
                   (internal_trade_id, contract_id, contract_name, delivery_start, delivery_end,
                    price, quantity, buy_portfolio_id, sell_portfolio_id, buy_order_id,
                    sell_order_id, exec_time, api_timestamp)
                 values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
                 on conflict (internal_trade_id) do update set
                   contract_id = excluded.contract_id, contract_name = excluded.contract_name,
                   delivery_start = excluded.delivery_start, delivery_end = excluded.delivery_end,
                   price = excluded.price, quantity = excluded.quantity,
                   buy_portfolio_id = excluded.buy_portfolio_id,
                   sell_portfolio_id = excluded.sell_portfolio_id,
                   buy_order_id = excluded.buy_order_id, sell_order_id = excluded.sell_order_id,
                   exec_time = excluded.exec_time, api_timestamp = excluded.api_timestamp,
                   recorded_at = now()
                 where x.api_timestamp is null
                    or excluded.api_timestamp is null
                    or excluded.api_timestamp >= x.api_timestamp",
            )
            .bind(id)
            .bind(t.contract_id.as_deref())
            .bind(t.contract_name.as_deref())
            .bind(to_utc(&t.delivery_start))
            .bind(to_utc(&t.delivery_end))
            .bind(t.price)
            .bind(t.quantity)
            .bind(t.buy_portfolio_id.as_deref())
            .bind(t.sell_portfolio_id.as_deref())
            .bind(t.buy_order_id.as_deref())
            .bind(t.sell_order_id.as_deref())
            .bind(to_utc(&t.exec_time))
            .bind(to_utc(&t.api_timestamp))
            .execute(&mut *tx)
            .await
            .context("upserting internal trade")?;
        }
        tx.commit().await.context("commit record_internal_trades")?;
        Ok(())
    }

    /// Append order revisions, keyed by `(order_id, revision_no)`. A revision is
    /// immutable, so conflicts are dropped. Rows without an order id are skipped.
    pub async fn record_order_revisions(&self, orders: &[OwnOrder]) -> Result<()> {
        let mut tx = self
            .pool
            .begin()
            .await
            .context("begin record_order_revisions")?;
        for o in orders {
            let Some(id) = o.order_id.as_deref() else {
                continue;
            };
            sqlx::query(
                "insert into execution.order_revisions
                   (order_id, revision_no, state, action, side, delivery_area, contract_id,
                    contract_name, delivery_start, delivery_end, portfolio_id, price, quantity,
                    txt, api_timestamp)
                 values
                   ($1, $2, $3::execution.order_state, $4::execution.order_action,
                    $5::execution.order_side, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)
                 on conflict (order_id, revision_no) do nothing",
            )
            .bind(id)
            .bind(o.revision_no.unwrap_or(0))
            .bind(enum_text(&o.state))
            .bind(enum_text(&o.action))
            .bind(enum_text(&o.side))
            .bind(o.delivery_area.as_str())
            .bind(o.contract_id.as_deref())
            .bind(o.contract_name.as_deref())
            .bind(to_utc(&o.delivery_start))
            .bind(to_utc(&o.delivery_end))
            .bind(o.portfolio_id.as_deref())
            .bind(o.price)
            .bind(o.quantity)
            .bind(o.txt.as_deref())
            .bind(to_utc(&o.api_timestamp))
            .execute(&mut *tx)
            .await
            .context("inserting order revision")?;
        }
        tx.commit().await.context("commit record_order_revisions")?;
        Ok(())
    }

    /// Drain the live-persistence queue, writing each batch to Postgres as it
    /// arrives. A write failure is logged, not fatal — the reconciliation sweep
    /// re-persists the full window, so the durable record heals regardless. Stops
    /// when `drain` flips, flushing what's already queued first. The caller fires
    /// `drain` only after the producer (the ws driver thread) has exited — an
    /// early shutdown signal would race a still-running driver's final batches —
    /// and awaits the returned future so the flush isn't cut off by runtime
    /// shutdown.
    pub async fn run_persistence(
        self,
        mut rx: mpsc::Receiver<Persist>,
        mut drain: watch::Receiver<bool>,
    ) {
        loop {
            // Resolve the select without awaiting inside a branch — holding the
            // `wait_for` guard across the `write().await` would make this future
            // non-Send (it's spawned).
            let msg = tokio::select! {
                msg = rx.recv() => msg,
                _ = drain.wait_for(|&stop| stop) => break,
            };
            match msg {
                Some(msg) => self.write(msg).await,
                None => break,
            }
        }
        // The producer has exited, so this flush sees everything it enqueued.
        while let Ok(msg) = rx.try_recv() {
            self.write(msg).await;
        }
        tracing::info!("live persistence stopped");
    }

    async fn write(&self, msg: Persist) {
        let result = match &msg {
            Persist::Trades(t) => self.record_trades(t).await,
            Persist::InternalTrades(t) => self.record_internal_trades(t).await,
            Persist::Orders(o) => self.record_order_revisions(o).await,
        };
        if let Err(e) = result {
            tracing::warn!(error = %e, "live persist failed; reconciliation will retry");
        }
    }
}

/// A model enum → its PowerBot wire code (`TradeState::Acti` → `"ACTI"`), via the
/// serde rename the generated models carry.
fn enum_text<T: Serialize>(value: &T) -> Option<String> {
    serde_json::to_value(value)
        .ok()
        .and_then(|v| v.as_str().map(str::to_string))
}

/// [`enum_text`] over an optional enum field.
fn enum_text_opt<T: Serialize>(value: &Option<T>) -> Option<String> {
    value.as_ref().and_then(enum_text)
}

/// PowerBot timestamp (RFC3339, or offset-less assumed UTC — the shared parser,
/// so the DB reads a timestamp exactly as the stores do) → `timestamptz`,
/// `NULL` when absent/unparseable.
fn to_utc(ts: &Option<String>) -> Option<DateTime<Utc>> {
    crate::time::parse_ts(ts.as_deref())
}
