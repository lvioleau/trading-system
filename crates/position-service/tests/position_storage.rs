//! Integration tests of the durable trade ledger against a real Postgres — the container
//! from `scripts/dev-stack.sh` locally (the `postgres` service), the postgres service in CI. `DATABASE_URL`
//! points at the server; when it is unset the tests skip (with a note), so plain
//! `cargo test` still passes without a container — except in CI, where skipping would
//! silently drop the suite, so an unset `DATABASE_URL` fails instead.
//!
//! Each test drops and recreates its own database and runs the *contract service's*
//! migrations there first, seeding the `reference.contracts` rows the trade log's foreign
//! key targets — the production boot order on the shared `trading` database. That fixture
//! is itself half the point: it proves the two crates' sqlx migration ledgers coexist on
//! one database (contract-service's in `public.`, this crate's in `positions.` — the
//! pinned-`search_path` isolation).

use std::collections::{BTreeMap, HashSet};
use std::sync::Arc;

use sqlx::Row;
use sqlx::postgres::{PgConnectOptions, PgPool};
use tokio::net::TcpListener;
use tokio::sync::{RwLock, broadcast, watch};
use tokio_stream::wrappers::TcpListenerStream;
use uuid::Uuid;

use contract_service::storage::ContractStorage;
use contract_service::store as reference;
use position_service::channel::SharedLedger;
use position_service::ledger::{ContractKey, MarketType, PositionLedger, Quantity, Trade};
use position_service::proto;
use position_service::proto::position_service_client::PositionServiceClient;
use position_service::server;
use position_service::storage::{PositionStorage, is_foreign_key_violation};

/// Connection options for a recreated per-test database, or `None` (skip) without
/// `DATABASE_URL`.
async fn test_database(database: &str) -> Option<PgConnectOptions> {
    let Ok(url) = std::env::var("DATABASE_URL") else {
        // Skipping shows as `ok` in the test summary, so refuse to skip in CI — a lost
        // DATABASE_URL there would silently drop the whole storage suite.
        assert!(
            std::env::var_os("CI").is_none(),
            "DATABASE_URL is not set in CI; the postgres tests must not silently skip there"
        );
        eprintln!("DATABASE_URL is not set; skipping postgres test (see scripts/dev-stack.sh)");
        return None;
    };
    let options: PgConnectOptions = url.parse().expect("parsing DATABASE_URL");
    let admin = PgPool::connect_with(options.clone())
        .await
        .expect("connecting to postgres");
    sqlx::query(&format!("drop database if exists {database} with (force)"))
        .execute(&admin)
        .await
        .expect("dropping the per-test database");
    sqlx::query(&format!("create database {database}"))
        .execute(&admin)
        .await
        .expect("creating the per-test database");
    Some(options.database(database))
}

/// A fresh [`PositionStorage`] on a per-test database whose `reference.contracts` already
/// holds the test contracts — the contract-service migrations run first, as in production
/// (the FK target must exist before this crate's migration references it). Also returns
/// the options, for tests that reconnect (a simulated restart).
async fn test_storage(database: &str) -> Option<(PositionStorage, PgConnectOptions)> {
    let options = test_database(database).await?;
    let contracts = ContractStorage::connect_with(options.clone())
        .await
        .expect("connecting contract storage");
    contracts
        .upsert(&[
            reference_contract(reference::MarketType::Local, 7),
            reference_contract(reference::MarketType::Xbid, 7),
            reference_contract(reference::MarketType::Local, 8),
        ])
        .await
        .expect("seeding reference contracts");
    let storage = PositionStorage::connect_with(options.clone())
        .await
        .expect("connecting position storage");
    Some((storage, options))
}

/// Realistic epoch milliseconds (2026-06-27T00:53:20Z) for the reference contracts.
const T0_MS: i64 = 1_782_000_000_000;

/// The same instant in nanoseconds for trade event times — microsecond-aligned, since
/// `timestamptz` keeps microseconds and the round-trip tests compare exactly.
const T0_NANOS: i64 = T0_MS * 1_000_000;

fn reference_contract(market_type: reference::MarketType, contract_id: i64) -> reference::Contract {
    reference::Contract {
        key: reference::ContractKey {
            market_type,
            contract_id,
        },
        name: format!("PH-{contract_id}"),
        market_id: "M1".to_owned(),
        product_type: reference::ProductType::P15Min,
        delivery_start_ms: T0_MS,
        delivery_end_ms: T0_MS + 900_000,
        duration_seconds: 900,
        updated_at_ms: T0_MS,
        areas: BTreeMap::new(),
    }
}

/// A market fill: `venue_trade_id` set, Local market unless overridden by the caller.
fn fill(id: u128, portfolio: &str, contract_id: i64, quantity: Quantity) -> Trade {
    Trade {
        id: Uuid::from_u128(id),
        portfolio: portfolio.to_owned(),
        contract: ContractKey {
            market_type: MarketType::Local,
            contract_id,
        },
        quantity,
        price: 4_250,
        // Per-trade offset in whole microseconds: timestamptz keeps microsecond precision,
        // and the round-trip assertions compare exactly.
        executed_at_nanos: T0_NANOS + id as i64 * 1_000,
        venue_trade_id: Some(format!("VT-{id}")),
        transfer_id: None,
        cl_order_id: Some(format!("N-{id}")),
        source: "test-feed".to_owned(),
    }
}

/// A transfer leg: its transfer id instead of a venue trade id (the two natural external
/// keys, each null for the other kind); negative prices and quantities legitimate. No client
/// order id — that is fill-only attribution.
fn leg(id: u128, portfolio: &str, contract_id: i64, quantity: Quantity) -> Trade {
    Trade {
        venue_trade_id: None,
        transfer_id: Some(Uuid::from_u128(1_000 + id).to_string()),
        cl_order_id: None,
        source: "test-crosser".to_owned(),
        ..fill(id, portfolio, contract_id, quantity)
    }
}

#[tokio::test]
async fn round_trips_the_trade_log_in_seq_order() {
    let Some((storage, _)) = test_storage("position_test_round_trip").await else {
        return;
    };

    // Given fills across both market segments and a transfer's two legs, appended in two
    // batches (fill ids share a contract id across markets — distinct FK targets)
    let mut xbid = fill(2, "execution-a", 7, -3_000);
    xbid.contract.market_type = MarketType::Xbid;
    xbid.price = -150; // negative prices are legitimate on the intraday market
    let first = vec![fill(1, "execution-a", 7, 10_000), xbid];
    let second = vec![
        leg(3, "execution-a", 7, -4_000),
        leg(4, "strategy-x", 7, 4_000),
    ];
    storage.append(&first).await.expect("appending fills");
    storage.append(&second).await.expect("appending legs");

    // When the log is loaded back (the boot fold's input)
    let log = storage.load_log().await.expect("loading the log");

    // Then every trade round-trips exactly, in append (= seq = fold) order
    let expected: Vec<Trade> = first.into_iter().chain(second).collect();
    assert_eq!(log, expected);
}

#[tokio::test]
async fn replayed_and_overlapping_batches_are_noops() {
    let Some((storage, _)) = test_storage("position_test_replay").await else {
        return;
    };

    // Given a stored batch
    let batch = vec![
        fill(1, "execution-a", 7, 10_000),
        fill(2, "execution-a", 8, 2_000),
    ];
    storage.append(&batch).await.unwrap();

    // When the same batch is re-applied (a reconnect re-seed)...
    storage.append(&batch).await.unwrap();

    // ...and a batch overlapping it arrives (re-seed plus one genuinely new fill)
    let overlap = vec![
        fill(2, "execution-a", 8, 2_000),
        fill(3, "strategy-x", 7, 500),
    ];
    storage.append(&overlap).await.unwrap();

    // Then only the unseen trade was added, after the originals
    let log = storage.load_log().await.unwrap();
    let expected: Vec<Trade> = batch
        .into_iter()
        .chain([fill(3, "strategy-x", 7, 500)])
        .collect();
    assert_eq!(log, expected);
}

#[tokio::test]
async fn transfer_legs_land_atomically() {
    let Some((storage, _)) = test_storage("position_test_atomic_legs").await else {
        return;
    };

    // Given a transfer whose second leg fails the contract FK (contract 999 is not in
    // reference.contracts) — the injected mid-transaction failure
    let legs = vec![
        leg(1, "execution-a", 7, -4_000),
        leg(2, "strategy-x", 999, 4_000),
    ];

    // When the batch is appended
    let error = storage.append(&legs).await.expect_err("bad leg must fail");

    // Then nothing persisted — not even the valid first leg (one transaction, so a
    // transfer can never half-land)
    assert!(is_foreign_key_violation(&error), "{error:#}");
    assert_eq!(storage.load_log().await.unwrap(), vec![]);
}

#[tokio::test]
async fn a_fill_on_an_unknown_contract_is_rejected_by_the_foreign_key() {
    let Some((storage, _)) = test_storage("position_test_unknown_contract").await else {
        return;
    };

    // Given a fill racing the contract sync (its contract not yet in reference.contracts)
    let racing = fill(1, "execution-a", 999, 1_000);

    // When it is appended
    let error = storage
        .append(std::slice::from_ref(&racing))
        .await
        .expect_err("unknown contract must fail");

    // Then the failure is classified as the FK rejection (the feed's expected-to-heal
    // case, the server's FAILED_PRECONDITION) and nothing persisted
    assert!(is_foreign_key_violation(&error), "{error:#}");
    assert_eq!(storage.load_log().await.unwrap(), vec![]);

    // And an unrelated (e.g. connection) failure is not misclassified — a plain error
    // is not a foreign key violation
    assert!(!is_foreign_key_violation(&anyhow::anyhow!(
        "connection reset"
    )));
}

#[tokio::test]
async fn boot_fold_reproduces_revisions_across_a_restart() {
    let Some((storage, options)) = test_storage("position_test_boot_fold").await else {
        return;
    };

    // Given a live ledger folded while its trades were appended (the running service),
    // spread over several batches and positions
    let batches = [
        vec![
            fill(1, "execution-a", 7, 10_000),
            fill(2, "strategy-x", 7, 2_000),
        ],
        vec![
            leg(3, "execution-a", 7, -4_000),
            leg(4, "strategy-x", 7, 4_000),
        ],
        vec![
            fill(5, "execution-a", 8, 1_000),
            fill(6, "execution-a", 7, 500),
        ],
    ];
    let mut live = PositionLedger::new();
    for batch in &batches {
        storage.append(batch).await.unwrap();
        for trade in batch {
            live.apply(trade.clone());
        }
    }

    // When the service restarts: a new connection (re-running migrations, idempotently)
    // replays the log into a fresh ledger
    let reconnected = PositionStorage::connect_with(options)
        .await
        .expect("reconnecting runs migrations idempotently");
    let mut boot = PositionLedger::new();
    for trade in reconnected.load_log().await.unwrap() {
        boot.apply(trade);
    }

    // Then every position matches the live ledger — quantity and revision — so clients'
    // revision-keyed dedup survives the restart
    assert_eq!(boot.applied_trades(), live.applied_trades());
    assert_eq!(boot.len(), live.len());
    for batch in &batches {
        for trade in batch {
            assert_eq!(
                boot.get(&trade.portfolio, &trade.contract),
                live.get(&trade.portfolio, &trade.contract),
            );
        }
    }
}

#[tokio::test]
async fn the_two_migration_ledgers_coexist_on_one_database() {
    let Some((_storage, options)) = test_storage("position_test_migration_ledgers").await else {
        return;
    };

    // Given both services' storages connected to one database (the fixture), when both
    // reconnect (both services restarting against the shared `trading` database)
    ContractStorage::connect_with(options.clone())
        .await
        .expect("contract storage re-migrates idempotently");
    PositionStorage::connect_with(options.clone())
        .await
        .expect("position storage re-migrates idempotently");

    // Then each crate's sqlx migration ledger lives in its own schema — the pinned
    // search_path kept them from colliding
    let pool = PgPool::connect_with(options).await.unwrap();
    let ledgers: Vec<String> = sqlx::query(
        "select table_schema from information_schema.tables
         where table_name = '_sqlx_migrations' order by table_schema",
    )
    .fetch_all(&pool)
    .await
    .unwrap()
    .iter()
    .map(|row| row.get(0))
    .collect();
    assert_eq!(ledgers, vec!["positions", "public"]);
}

/// Starts a server whose ledger writes through `storage`, returning a connected client and
/// the shutdown handle (dropping it drains the server, so tests keep it alive).
async fn start_server(
    storage: PositionStorage,
) -> (
    PositionServiceClient<tonic::transport::Channel>,
    watch::Sender<bool>,
) {
    let ledger: SharedLedger = Arc::new(RwLock::new(PositionLedger::new()));
    let (updates, _) = broadcast::channel(16);
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    let (shutdown, shutdown_rx) = watch::channel(false);
    let portfolios: HashSet<String> = ["execution-a", "strategy-x"]
        .iter()
        .map(|p| p.to_string())
        .collect();
    let router = server::router(portfolios, ledger, updates, Some(storage), shutdown_rx)
        .await
        .unwrap();
    tokio::spawn(router.serve_with_incoming(TcpListenerStream::new(listener)));
    let client = PositionServiceClient::connect(endpoint).await.unwrap();
    (client, shutdown)
}

fn transfer(id: u128, contract_id: i64, quantity: i64) -> proto::TransferRequest {
    proto::TransferRequest {
        transfer_id: Uuid::from_u128(id).to_string(),
        from_portfolio: "execution-a".to_owned(),
        to_portfolio: "strategy-x".to_owned(),
        market_type: proto::MarketType::Local as i32,
        contract_id,
        quantity,
        price: 4_250,
        source: "test-crosser".to_owned(),
        executed_at_nanos: T0_NANOS,
        expected_from_revision: None,
    }
}

#[tokio::test]
async fn transfer_acks_only_after_the_commit() {
    let Some((storage, options)) = test_storage("position_test_transfer_commit").await else {
        return;
    };
    let (mut client, _shutdown) = start_server(storage.clone()).await;

    // Given an acked transfer: both legs are already durable — commit precedes the ack —
    // and both record the originating transfer id (the mirror of a fill's venue_trade_id),
    // no venue trade id
    let first = client
        .transfer(transfer(100, 7, 4_000))
        .await
        .unwrap()
        .into_inner();
    let legs = storage.load_log().await.unwrap();
    assert_eq!(legs.len(), 2);
    for leg in &legs {
        assert_eq!(
            leg.transfer_id.as_deref(),
            Some(Uuid::from_u128(100).to_string().as_str())
        );
        assert_eq!(leg.venue_trade_id, None);
    }

    // When the transfer is replayed, nothing new commits (idempotent by leg id) and the
    // ack carries the unchanged positions
    let replay = client
        .transfer(transfer(100, 7, 4_000))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(replay, first);
    assert_eq!(storage.load_log().await.unwrap().len(), 2);

    // And a transfer whose contract the reference data lacks fails as the caller's error —
    // FAILED_PRECONDITION — leaving both the log and the in-memory ledger untouched...
    let status = client
        .transfer(transfer(101, 999, 1_000))
        .await
        .unwrap_err();
    assert_eq!(status.code(), tonic::Code::FailedPrecondition);
    assert_eq!(storage.load_log().await.unwrap().len(), 2);

    // ...so the next transfer folds onto clean state: the from side's revision advances by
    // exactly one from the first ack
    let second = client
        .transfer(transfer(102, 7, 1_000))
        .await
        .unwrap()
        .into_inner();
    let from = second.from_position.unwrap();
    assert_eq!(from.net_quantity, -5_000);
    assert_eq!(from.revision, first.from_position.unwrap().revision + 1);

    // And after a restart, the boot fold serves the same positions the acks reported
    let reconnected = PositionStorage::connect_with(options).await.unwrap();
    let mut boot = PositionLedger::new();
    for trade in reconnected.load_log().await.unwrap() {
        boot.apply(trade);
    }
    let contract = ContractKey {
        market_type: MarketType::Local,
        contract_id: 7,
    };
    let from_boot = boot.get("execution-a", &contract).unwrap();
    let to_boot = boot.get("strategy-x", &contract).unwrap();
    assert_eq!(from_boot.net_quantity, from.net_quantity);
    assert_eq!(from_boot.revision, from.revision);
    assert_eq!(
        to_boot.net_quantity,
        second.to_position.as_ref().unwrap().net_quantity
    );
    assert_eq!(to_boot.revision, second.to_position.unwrap().revision);
}
