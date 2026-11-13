//! Persistence layer — the durable trade ledger in Postgres (`positions.trades`, created by
//! the embedded migrations). The DB is the durable record; memory is the serving authority:
//! every mutation follows *commit, then memory, then broadcast* — a trade lands here before
//! the in-memory fold — and at boot [`load_log`](PositionStorage::load_log) replays the
//! whole log in `seq` order into the ledger, after which the DB stays off the read path.
//!
//! The log is append-only: rows are inserted with `on conflict (id) do nothing`, never
//! updated or deleted. Idempotency is the trade id primary key — the durable twin of the
//! ledger's seen-id guard — so replays and reconnect re-seeds are no-ops and no
//! out-of-order merge problem exists (unlike the contract-service's `updated_at` guards).
//! `seq` is the total fold order: the service is the schema's single writer and serializes
//! mutations in-process, so insert order, `seq` order, and fold order agree, which is what
//! makes revisions deterministic across restarts.
//!
//! **The shared `trading` database.** This service owns schema `positions`, next to the
//! contract-service's `reference` (its migration ledger stays in `public`) and the
//! powerbot-gateway's `execution`. Every connection creates the schema and pins its
//! `search_path` to it *before* migrating — the powerbot-gateway pattern — so sqlx's own
//! `_sqlx_migrations` lands in `positions.` and the per-crate migration ledgers coexist.
//! Queries still schema-qualify every table: the search_path exists for the migration
//! ledger, not for name resolution.
//!
//! **The contract foreign key.** `positions.trades` references `reference.contracts` — the
//! write-time integrity link to the Contract Service (shared database, no runtime
//! dependency). Two consequences: the contract-service migrations must have run before this
//! crate's (the FK target must exist — boot order on a fresh database), and an append
//! racing the contract sync fails loudly instead of silently recording a position on an
//! unknown contract. [`is_foreign_key_violation`] classifies that case for callers: the
//! server maps it to `FAILED_PRECONDITION`; the gateway feeds (step 5) treat it as
//! *expected to heal* — reconnect and replay once the contract sync catches up.
//!
//! `executed_at` is stored as `timestamptz`, which Postgres keeps at microsecond precision:
//! the wire's nanosecond timestamps lose their sub-microsecond digits on the round trip.
//! Harmless — the fold never reads event time; it rides along for attribution and queries.

use anyhow::{Context, Result, bail};
use chrono::{DateTime, Utc};
use sqlx::Row;
use sqlx::postgres::{PgConnectOptions, PgPool, PgPoolOptions, PgRow};

use crate::ledger::{ContractKey, MarketType, Trade};

/// The venue every row belongs to. Single-venue today; a column (not a constant) in the
/// schema so a second venue is a data change, not a migration — mirroring the
/// contract-service, whose `reference.contracts` rows this FK-references.
const VENUE: &str = "nordpool";

/// A handle on the trade log. Cheap to clone (wraps a connection pool).
#[derive(Debug, Clone)]
pub struct PositionStorage {
    pool: PgPool,
}

impl PositionStorage {
    /// Connect and bring the schema up to date (embedded `sqlx` migrations, idempotent).
    pub async fn connect(url: &str) -> Result<Self> {
        let options: PgConnectOptions = url.parse().context("parsing database url")?;
        Self::connect_with(options).await
    }

    /// [`connect`](Self::connect), from already-parsed options (tests point them at a
    /// per-test database). Creates the `positions` schema and pins the connection
    /// `search_path` to it on every connection — before migrating, so the migration ledger
    /// lands in `positions._sqlx_migrations` (see the module docs).
    pub async fn connect_with(options: PgConnectOptions) -> Result<Self> {
        let pool = PgPoolOptions::new()
            .after_connect(|conn, _meta| {
                Box::pin(async move {
                    sqlx::query("create schema if not exists positions")
                        .execute(&mut *conn)
                        .await?;
                    sqlx::query("set search_path to positions")
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
            .context("running position migrations")?;
        Ok(Self { pool })
    }

    /// Append one batch of trades atomically: all rows commit or none do (a transfer's two
    /// legs are one batch — legs can never land alone). Idempotent by trade id
    /// (`on conflict (id) do nothing`), and per row: a batch overlapping already-stored
    /// trades inserts only the unseen ones, which is exactly what a feed re-seed needs.
    ///
    /// A trade naming a contract missing from `reference.contracts` fails the whole batch
    /// (see [`is_foreign_key_violation`]); nothing persists.
    pub async fn append(&self, trades: &[Trade]) -> Result<()> {
        let mut tx = self.pool.begin().await.context("beginning append")?;
        for trade in trades {
            sqlx::query(
                "insert into positions.trades
                   (id, portfolio, venue, market_type, venue_contract_id, quantity, price,
                    venue_trade_id, transfer_id, cl_order_id, source, executed_at)
                 values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
                 on conflict (id) do nothing",
            )
            .bind(trade.id)
            .bind(&trade.portfolio)
            .bind(VENUE)
            .bind(market_type_sql(trade.contract.market_type))
            .bind(trade.contract.contract_id)
            .bind(trade.quantity)
            .bind(trade.price)
            .bind(trade.venue_trade_id.as_deref())
            .bind(trade.transfer_id.as_deref())
            .bind(trade.cl_order_id.as_deref())
            .bind(&trade.source)
            .bind(DateTime::from_timestamp_nanos(trade.executed_at_nanos))
            .execute(&mut *tx)
            .await
            .context("appending trade")?;
        }
        tx.commit().await.context("committing append")?;
        Ok(())
    }

    /// The whole log in `seq` order — the boot fold's input. Folding it into a fresh
    /// [`PositionLedger`](crate::ledger::PositionLedger) reproduces every position with the
    /// same revisions the trades were acked with (the fold is deterministic and `seq` is
    /// the original apply order).
    pub async fn load_log(&self) -> Result<Vec<Trade>> {
        let rows = sqlx::query(
            "select id, portfolio, market_type, venue_contract_id, quantity, price,
                    venue_trade_id, transfer_id, cl_order_id, source, executed_at
             from positions.trades where venue = $1 order by seq",
        )
        .bind(VENUE)
        .fetch_all(&self.pool)
        .await
        .context("loading the trade log")?;
        rows.iter().map(trade_from_row).collect()
    }
}

/// Whether `error` (an [`append`](PositionStorage::append) failure) is the composite
/// foreign key into `reference.contracts` rejecting an unknown contract — the one append
/// failure with a meaning beyond "the database is unavailable": the server answers it with
/// `FAILED_PRECONDITION`, and a feed session retries it by reconnecting (the contract sync
/// is expected to catch up).
pub fn is_foreign_key_violation(error: &anyhow::Error) -> bool {
    error
        .downcast_ref::<sqlx::Error>()
        .and_then(|error| error.as_database_error())
        .is_some_and(|db| matches!(db.kind(), sqlx::error::ErrorKind::ForeignKeyViolation))
}

fn trade_from_row(row: &PgRow) -> Result<Trade> {
    let executed_at: DateTime<Utc> = row.try_get("executed_at")?;
    Ok(Trade {
        id: row.try_get("id")?,
        portfolio: row.try_get("portfolio")?,
        contract: ContractKey {
            market_type: market_type_from_sql(row.try_get("market_type")?)?,
            contract_id: row.try_get("venue_contract_id")?,
        },
        quantity: row.try_get("quantity")?,
        price: row.try_get("price")?,
        executed_at_nanos: executed_at
            .timestamp_nanos_opt()
            .with_context(|| format!("executed_at out of nanosecond range: {executed_at}"))?,
        venue_trade_id: row.try_get("venue_trade_id")?,
        transfer_id: row.try_get("transfer_id")?,
        cl_order_id: row.try_get("cl_order_id")?,
        source: row.try_get("source")?,
    })
}

fn market_type_sql(market_type: MarketType) -> &'static str {
    match market_type {
        MarketType::Local => "LOCAL",
        MarketType::Xbid => "XBID",
    }
}

fn market_type_from_sql(value: String) -> Result<MarketType> {
    Ok(match value.as_str() {
        "LOCAL" => MarketType::Local,
        "XBID" => MarketType::Xbid,
        other => bail!("unknown market_type in trades table: {other:?}"),
    })
}
