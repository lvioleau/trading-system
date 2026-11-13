//! Persistence layer — the durable home of contracts in Postgres (`reference.contracts` +
//! `reference.contract_areas`, plus the append-only `reference.contract_state_changes`
//! lifecycle history; all created by the embedded migrations). The DB is the durable record;
//! memory is the serving authority: the feed upserts every batch here *before* the in-memory
//! index (durable before visible), and at boot the live tables are folded back into the
//! index — the DB stays off the hot read path ([`history`](ContractStorage::history) is the
//! deliberate cold-path exception).
//!
//! The SQL upserts carry the same `updated_at` guards as [`ContractStore::upsert`]
//! (intrinsics and each per-area state guarded independently), so replays and reconnect
//! re-seeds are no-ops and a stale frame can't regress persisted state.
//!
//! [`ContractStore::upsert`]: crate::store::ContractStore::upsert

use std::collections::{BTreeMap, HashMap};

use anyhow::{Context, Result, bail};
use chrono::{DateTime, Utc};
use eic::EicCode;
use sqlx::Row;
use sqlx::postgres::{PgConnectOptions, PgPool};

use crate::store::{AreaState, Contract, ContractKey, ContractState, MarketType, ProductType};

/// The venue every row belongs to. Single-venue today; a column (not a constant) in the
/// schema so a second venue is a data change, not a migration.
const VENUE: &str = "nordpool";

/// A handle on the contract tables. Cheap to clone (wraps a connection pool).
#[derive(Debug, Clone)]
pub struct ContractStorage {
    pool: PgPool,
}

/// One recorded lifecycle transition in one delivery area, including the contract's first
/// appearance there.
#[derive(Debug, Clone, PartialEq)]
pub struct StateChange {
    pub delivery_area_eic: EicCode,
    /// The state the area changed *to*.
    pub state: ContractState,
    /// Venue update time of the frame that carried the transition.
    pub updated_at_ms: i64,
    /// When this service recorded it (the migration backfill time for history predating the
    /// history table).
    pub recorded_at_ms: i64,
}

impl ContractStorage {
    /// Connect and bring the schema up to date (embedded `sqlx` migrations, idempotent).
    pub async fn connect(url: &str) -> Result<Self> {
        let options: PgConnectOptions = url.parse().context("parsing database url")?;
        Self::connect_with(options).await
    }

    /// [`connect`](Self::connect), from already-parsed options (tests point them at a
    /// per-test database).
    pub async fn connect_with(options: PgConnectOptions) -> Result<Self> {
        let pool = PgPool::connect_with(options)
            .await
            .context("connecting to postgres")?;
        sqlx::migrate!("./migrations")
            .run(&pool)
            .await
            .context("running contract migrations")?;
        Ok(Self { pool })
    }

    /// Upsert one feed batch, atomically. Idempotent on the natural key: intrinsics apply
    /// only when strictly newer than the stored row, each area state only when strictly newer
    /// than that area's stored row (an unseen area is always added) — the SQL twin of the
    /// in-memory fold. An applied area state whose *value* differs from the stored one also
    /// lands in the append-only `contract_state_changes` history, in the same statement; a
    /// guard-rejected frame or a mere re-stamp of an unchanged state records nothing.
    pub async fn upsert(&self, contracts: &[Contract]) -> Result<()> {
        let mut tx = self.pool.begin().await.context("beginning upsert")?;
        for contract in contracts {
            sqlx::query(
                "insert into reference.contracts as c
                   (venue, market_type, venue_contract_id, name, market_id, product_type,
                    delivery_start, delivery_end, duration_seconds, updated_at)
                 values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
                 on conflict (venue, market_type, venue_contract_id) do update set
                   name = excluded.name,
                   market_id = excluded.market_id,
                   product_type = excluded.product_type,
                   delivery_start = excluded.delivery_start,
                   delivery_end = excluded.delivery_end,
                   duration_seconds = excluded.duration_seconds,
                   updated_at = excluded.updated_at
                 where excluded.updated_at > c.updated_at",
            )
            .bind(VENUE)
            .bind(market_type_sql(contract.key.market_type))
            .bind(contract.key.contract_id)
            .bind(&contract.name)
            .bind(&contract.market_id)
            .bind(product_type_sql(contract.product_type))
            .bind(timestamp(contract.delivery_start_ms)?)
            .bind(timestamp(contract.delivery_end_ms)?)
            .bind(contract.duration_seconds)
            .bind(timestamp(contract.updated_at_ms)?)
            .execute(&mut *tx)
            .await
            .context("upserting contract")?;
            for (area, state) in &contract.areas {
                // `before` snapshots the area's state as of the statement's start, `applied`
                // is the guarded live upsert (which advances the guard even when the state
                // value is unchanged — the venue re-stamps every area on any contract
                // change), and the final insert records a history row only when the upsert
                // applied *and* the value actually changed (or the area is new).
                sqlx::query(
                    "with before as (
                       select state from reference.contract_areas
                       where venue = $1 and market_type = $2 and venue_contract_id = $3
                         and delivery_area = $4
                     ),
                     applied as (
                       insert into reference.contract_areas as a
                         (venue, market_type, venue_contract_id, delivery_area, state,
                          updated_at)
                       values ($1, $2, $3, $4, $5, $6)
                       on conflict (venue, market_type, venue_contract_id, delivery_area)
                       do update set
                         state = excluded.state,
                         updated_at = excluded.updated_at
                       where excluded.updated_at > a.updated_at
                       returning venue
                     )
                     insert into reference.contract_state_changes
                       (venue, market_type, venue_contract_id, delivery_area, state, updated_at)
                     select $1, $2, $3, $4, $5, $6 from applied
                     where not exists (select 1 from before where before.state = $5)",
                )
                .bind(VENUE)
                .bind(market_type_sql(contract.key.market_type))
                .bind(contract.key.contract_id)
                .bind(area.eic_code())
                .bind(state_sql(state.state))
                .bind(timestamp(state.updated_at_ms)?)
                .execute(&mut *tx)
                .await
                .context("upserting contract area state")?;
            }
        }
        tx.commit().await.context("committing upsert")?;
        Ok(())
    }

    /// The whole table, as domain contracts — the boot fold. Row order is unspecified, like
    /// the store's own iteration.
    pub async fn load_all(&self) -> Result<Vec<Contract>> {
        let rows = sqlx::query(
            "select market_type, venue_contract_id, name, market_id, product_type,
                    delivery_start, delivery_end, duration_seconds, updated_at
             from reference.contracts where venue = $1",
        )
        .bind(VENUE)
        .fetch_all(&self.pool)
        .await
        .context("loading contracts")?;
        let mut contracts = HashMap::new();
        for row in rows {
            let key = ContractKey {
                market_type: market_type_from_sql(row.try_get("market_type")?)?,
                contract_id: row.try_get("venue_contract_id")?,
            };
            let contract = Contract {
                key,
                name: row.try_get("name")?,
                market_id: row.try_get("market_id")?,
                product_type: product_type_from_sql(row.try_get("product_type")?)?,
                delivery_start_ms: millis(row.try_get("delivery_start")?),
                delivery_end_ms: millis(row.try_get("delivery_end")?),
                duration_seconds: row.try_get("duration_seconds")?,
                updated_at_ms: millis(row.try_get("updated_at")?),
                areas: BTreeMap::new(),
            };
            contracts.insert(key, contract);
        }
        let rows = sqlx::query(
            "select market_type, venue_contract_id, delivery_area, state, updated_at
             from reference.contract_areas where venue = $1",
        )
        .bind(VENUE)
        .fetch_all(&self.pool)
        .await
        .context("loading contract area states")?;
        for row in rows {
            let key = ContractKey {
                market_type: market_type_from_sql(row.try_get("market_type")?)?,
                contract_id: row.try_get("venue_contract_id")?,
            };
            let contract = contracts
                .get_mut(&key)
                .context("area state row without its contract row")?;
            contract.areas.insert(
                eic_from_sql(row.try_get("delivery_area")?)?,
                AreaState {
                    state: state_from_sql(row.try_get("state")?)?,
                    updated_at_ms: millis(row.try_get("updated_at")?),
                },
            );
        }
        Ok(contracts.into_values().collect())
    }

    /// One contract's recorded state transitions, ascending by venue update time (ties: by
    /// area EIC — one frame can flip several areas). Empty for a contract that was never
    /// stored. The one read the DB serves directly: unbounded, cold-path data that would
    /// bloat the in-memory index for no hot-path gain.
    pub async fn history(&self, key: &ContractKey) -> Result<Vec<StateChange>> {
        let rows = sqlx::query(
            "select delivery_area, state, updated_at, recorded_at
             from reference.contract_state_changes
             where venue = $1 and market_type = $2 and venue_contract_id = $3
             order by updated_at, delivery_area",
        )
        .bind(VENUE)
        .bind(market_type_sql(key.market_type))
        .bind(key.contract_id)
        .fetch_all(&self.pool)
        .await
        .context("loading contract state change history")?;
        let mut changes = Vec::with_capacity(rows.len());
        for row in rows {
            changes.push(StateChange {
                delivery_area_eic: eic_from_sql(row.try_get("delivery_area")?)?,
                state: state_from_sql(row.try_get("state")?)?,
                updated_at_ms: millis(row.try_get("updated_at")?),
                recorded_at_ms: millis(row.try_get("recorded_at")?),
            });
        }
        Ok(changes)
    }
}

/// Millisecond epoch → `timestamptz`. Milliseconds fit Postgres's microsecond precision
/// exactly, so [`millis`] inverts this without loss.
fn timestamp(ms: i64) -> Result<DateTime<Utc>> {
    DateTime::from_timestamp_millis(ms).with_context(|| format!("timestamp out of range: {ms}ms"))
}

fn millis(timestamp: DateTime<Utc>) -> i64 {
    timestamp.timestamp_millis()
}

/// Decode a stored EIC. Unknown means the vocabulary regressed (a variant was removed, or the
/// row predates it) — fail loudly like the other decoders; the enum is append-only precisely
/// so this cannot happen in normal operation.
fn eic_from_sql(value: String) -> Result<EicCode> {
    EicCode::from_eic(&value)
        .with_context(|| format!("unknown delivery-area EIC in contract tables: {value:?}"))
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
        other => bail!("unknown market_type in contracts table: {other:?}"),
    })
}

fn product_type_sql(product_type: ProductType) -> &'static str {
    match product_type {
        ProductType::P15Min => "P15",
        ProductType::P30Min => "P30",
        ProductType::P60Min => "P60",
        ProductType::Other => "OTHER",
    }
}

fn product_type_from_sql(value: String) -> Result<ProductType> {
    Ok(match value.as_str() {
        "P15" => ProductType::P15Min,
        "P30" => ProductType::P30Min,
        "P60" => ProductType::P60Min,
        "OTHER" => ProductType::Other,
        other => bail!("unknown product_type in contracts table: {other:?}"),
    })
}

fn state_sql(state: ContractState) -> &'static str {
    match state {
        // Unknown is the domain image of the wire's unset NONE; it round-trips.
        ContractState::Unknown => "UNKNOWN",
        ContractState::Active => "ACTIVE",
        ContractState::Hibernated => "HIBERNATED",
        ContractState::Inactive => "INACTIVE",
        ContractState::Frozen => "FROZEN",
    }
}

fn state_from_sql(value: String) -> Result<ContractState> {
    Ok(match value.as_str() {
        "UNKNOWN" => ContractState::Unknown,
        "ACTIVE" => ContractState::Active,
        "HIBERNATED" => ContractState::Hibernated,
        "INACTIVE" => ContractState::Inactive,
        "FROZEN" => ContractState::Frozen,
        other => bail!("unknown state in contract_areas table: {other:?}"),
    })
}
