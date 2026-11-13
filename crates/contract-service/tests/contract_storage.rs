//! Integration tests of the Postgres persistence layer against a real server — the container
//! from `scripts/dev-stack.sh` locally (the `postgres` service), the postgres service in CI. `DATABASE_URL` points
//! at the server; when it is unset the tests skip (with a note), so plain `cargo test` still
//! passes without a container — except in CI, where skipping would silently drop the suite,
//! so an unset `DATABASE_URL` fails instead.
//!
//! Each test drops and recreates its own database (named after the test) on that server, so
//! tests are isolated, parallel-safe, and leave nothing behind that a rerun can't overwrite.

use std::collections::BTreeMap;
use std::sync::Arc;

use sqlx::postgres::{PgConnectOptions, PgPool};
use tokio::net::TcpListener;
use tokio::sync::{RwLock, broadcast, watch};
use tokio_stream::wrappers::TcpListenerStream;

use contract_service::proto;
use contract_service::server;
use contract_service::storage::{ContractStorage, StateChange};
use contract_service::store::{
    AreaState, Contract, ContractKey, ContractState, ContractStore, MarketType, ProductType,
};
use eic::EicCode;

/// A fresh [`ContractStorage`] on a recreated per-test database, or `None` (skip) without
/// `DATABASE_URL`.
async fn test_storage(database: &str) -> Option<ContractStorage> {
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
    Some(
        ContractStorage::connect_with(options.database(database))
            .await
            .expect("connecting contract storage"),
    )
}

/// Realistic epoch milliseconds (2026-06-27T00:53:20Z), so the timestamptz round-trip is
/// exercised on real-world magnitudes.
const T0_MS: i64 = 1_782_000_000_000;

const FR: EicCode = EicCode::AREA_FR;
const CH: EicCode = EicCode::AREA_CH;

fn contract(
    market_type: MarketType,
    contract_id: i64,
    updated_at_ms: i64,
    areas: &[(EicCode, ContractState, i64)],
) -> Contract {
    Contract {
        key: ContractKey {
            market_type,
            contract_id,
        },
        name: format!("PH-{contract_id}"),
        market_id: "M1".to_owned(),
        product_type: ProductType::P15Min,
        delivery_start_ms: T0_MS,
        delivery_end_ms: T0_MS + 900_000,
        duration_seconds: 900,
        updated_at_ms,
        areas: areas
            .iter()
            .map(|&(area, state, updated_at_ms)| {
                (
                    area,
                    AreaState {
                        state,
                        updated_at_ms,
                    },
                )
            })
            .collect(),
    }
}

/// Load everything back as a key-indexed map, for order-independent comparison.
async fn loaded(storage: &ContractStorage) -> BTreeMap<i64, Contract> {
    storage
        .load_all()
        .await
        .expect("loading contracts")
        .into_iter()
        .map(|contract| (contract.key.contract_id, contract))
        .collect()
}

#[tokio::test]
async fn round_trips_contracts_and_area_states() {
    let Some(storage) = test_storage("contract_test_round_trip").await else {
        return;
    };

    // Given a batch spanning both market types, several area states, and the Unknown
    // state tripwire
    let mut other = contract(
        MarketType::Xbid,
        2,
        T0_MS + 5,
        &[(CH, ContractState::Unknown, T0_MS + 5)],
    );
    other.product_type = ProductType::Other;
    let batch = vec![
        contract(
            MarketType::Local,
            1,
            T0_MS,
            &[
                (FR, ContractState::Active, T0_MS),
                (CH, ContractState::Hibernated, T0_MS + 1),
            ],
        ),
        other,
    ];

    // When it is upserted and loaded back
    storage.upsert(&batch).await.expect("upserting contracts");
    let stored = loaded(&storage).await;

    // Then every field round-trips exactly
    assert_eq!(stored.len(), 2);
    assert_eq!(stored[&1], batch[0]);
    assert_eq!(stored[&2], batch[1]);
}

#[tokio::test]
async fn stale_frame_cannot_regress_persisted_state() {
    let Some(storage) = test_storage("contract_test_stale_frame").await else {
        return;
    };

    // Given a stored contract at t+10
    let newer = contract(
        MarketType::Local,
        1,
        T0_MS + 10,
        &[(FR, ContractState::Hibernated, T0_MS + 10)],
    );
    storage.upsert(std::slice::from_ref(&newer)).await.unwrap();

    // When an older frame arrives carrying area 40 and an unseen area 41
    let mut older = contract(
        MarketType::Local,
        1,
        T0_MS,
        &[
            (FR, ContractState::Active, T0_MS),
            (CH, ContractState::Active, T0_MS),
        ],
    );
    older.name = "stale-rename".to_owned();
    storage.upsert(&[older]).await.unwrap();

    // Then intrinsics and area 40 keep the newer state, and only the unseen area is added —
    // the same merge the in-memory store performs
    let stored = loaded(&storage).await;
    let mut expected = newer;
    expected.areas.insert(
        CH,
        AreaState {
            state: ContractState::Active,
            updated_at_ms: T0_MS,
        },
    );
    assert_eq!(stored[&1], expected);
}

#[tokio::test]
async fn reapplying_a_batch_is_a_noop() {
    let Some(storage) = test_storage("contract_test_replay").await else {
        return;
    };

    // Given a stored batch
    let batch = vec![contract(
        MarketType::Local,
        1,
        T0_MS,
        &[(FR, ContractState::Active, T0_MS)],
    )];
    storage.upsert(&batch).await.unwrap();

    // When the same batch is re-applied (a reconnect re-seed)
    storage.upsert(&batch).await.unwrap();

    // Then nothing changed
    let stored = loaded(&storage).await;
    assert_eq!(stored.len(), 1);
    assert_eq!(stored[&1], batch[0]);
}

#[tokio::test]
async fn contracts_with_the_same_id_in_different_markets_are_distinct_rows() {
    let Some(storage) = test_storage("contract_test_market_identity").await else {
        return;
    };

    // Given the same contract id in both market segments
    storage
        .upsert(&[
            contract(
                MarketType::Local,
                1,
                T0_MS,
                &[(FR, ContractState::Active, T0_MS)],
            ),
            contract(
                MarketType::Xbid,
                1,
                T0_MS,
                &[(FR, ContractState::Active, T0_MS)],
            ),
        ])
        .await
        .unwrap();

    // Then both are stored — market_type is part of the natural key
    let stored = storage.load_all().await.unwrap();
    assert_eq!(stored.len(), 2);
}

#[tokio::test]
async fn history_records_state_transitions_and_nothing_else() {
    let Some(storage) = test_storage("contract_test_history").await else {
        return;
    };
    let key = ContractKey {
        market_type: MarketType::Local,
        contract_id: 1,
    };
    let frame = |updated_at_ms, areas: &[(EicCode, ContractState, i64)]| {
        vec![contract(MarketType::Local, 1, updated_at_ms, areas)]
    };
    let transitions = |history: &[StateChange]| {
        history
            .iter()
            .map(|change| (change.updated_at_ms, change.delivery_area_eic, change.state))
            .collect::<Vec<_>>()
    };

    // Given a contract's first appearance, replayed (records nothing)
    let first = frame(T0_MS, &[(FR, ContractState::Active, T0_MS)]);
    storage.upsert(&first).await.unwrap();
    storage.upsert(&first).await.unwrap();

    // And a newer frame re-stamping the unchanged state (the venue re-stamps every area on
    // any contract change): no transition, but the guard must advance
    storage
        .upsert(&frame(
            T0_MS + 10,
            &[(FR, ContractState::Active, T0_MS + 10)],
        ))
        .await
        .unwrap();

    // And an out-of-order frame from between: older than the advanced guard, so its different
    // state must be rejected — not a transition either
    storage
        .upsert(&frame(T0_MS + 5, &[(FR, ContractState::Frozen, T0_MS + 5)]))
        .await
        .unwrap();

    // And a real flip, plus an older lagging frame carrying an unseen area
    storage
        .upsert(&frame(
            T0_MS + 20,
            &[(FR, ContractState::Hibernated, T0_MS + 20)],
        ))
        .await
        .unwrap();
    storage
        .upsert(&frame(
            T0_MS + 15,
            &[(CH, ContractState::Active, T0_MS + 15)],
        ))
        .await
        .unwrap();

    // Then the history holds exactly the transitions: appearance, new area, and the flip
    let history = storage.history(&key).await.expect("loading history");
    assert_eq!(
        transitions(&history),
        vec![
            (T0_MS, FR, ContractState::Active),
            (T0_MS + 15, CH, ContractState::Active),
            (T0_MS + 20, FR, ContractState::Hibernated),
        ]
    );

    // And the live table matches the last transitions
    let stored = loaded(&storage).await;
    assert_eq!(stored[&1].areas[&FR].state, ContractState::Hibernated);
    assert_eq!(stored[&1].areas[&CH].state, ContractState::Active);

    // And an unknown contract has no history
    let unknown = ContractKey {
        market_type: MarketType::Xbid,
        contract_id: 1,
    };
    assert!(storage.history(&unknown).await.unwrap().is_empty());
}

#[tokio::test]
async fn get_contract_history_serves_the_recorded_transitions() {
    let Some(storage) = test_storage("contract_test_history_api").await else {
        return;
    };

    // Given a stored contract with a state flip, served by a real server whose store is
    // boot-folded from the database (as the service boots)
    storage
        .upsert(&[contract(
            MarketType::Local,
            1,
            T0_MS,
            &[(FR, ContractState::Active, T0_MS)],
        )])
        .await
        .unwrap();
    storage
        .upsert(&[contract(
            MarketType::Local,
            1,
            T0_MS + 10,
            &[(FR, ContractState::Frozen, T0_MS + 10)],
        )])
        .await
        .unwrap();
    let mut boot = ContractStore::new();
    for stored in storage.load_all().await.unwrap() {
        boot.upsert(stored);
    }
    let contracts = Arc::new(RwLock::new(boot));
    let (updates, _keep_open) = broadcast::channel(16);
    let (_shutdown, shutdown_rx) = watch::channel(false);
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    let router = server::router(contracts, updates, Some(storage), shutdown_rx)
        .await
        .unwrap();
    tokio::spawn(router.serve_with_incoming(TcpListenerStream::new(listener)));
    let mut client = proto::contract_service_client::ContractServiceClient::connect(endpoint)
        .await
        .unwrap();

    // When the contract's history is requested
    let response = client
        .get_contract_history(proto::GetContractHistoryRequest {
            market_type: proto::MarketType::Local as i32,
            contract_id: 1,
        })
        .await
        .unwrap()
        .into_inner();

    // Then it carries the current contract record...
    let current = response.contract.expect("current contract record");
    assert_eq!(current.contract_id, 1);
    assert_eq!(current.updated_at_ms, T0_MS + 10);
    assert_eq!(current.delivery_areas.len(), 1);
    assert_eq!(
        current.delivery_areas[0].state(),
        proto::ContractState::Frozen
    );

    // ...and both transitions in order: the appearance, then the Frozen flip
    let transitions: Vec<(i64, String, proto::ContractState)> = response
        .state_changes
        .iter()
        .map(|change| {
            (
                change.updated_at_ms,
                change.delivery_area_eic.to_string(),
                change.state(),
            )
        })
        .collect();
    assert_eq!(
        transitions,
        vec![
            (T0_MS, FR.to_string(), proto::ContractState::Active),
            (T0_MS + 10, FR.to_string(), proto::ContractState::Frozen),
        ]
    );

    // And a contract the service has never seen answers NOT_FOUND
    let status = client
        .get_contract_history(proto::GetContractHistoryRequest {
            market_type: proto::MarketType::Xbid as i32,
            contract_id: 999,
        })
        .await
        .unwrap_err();
    assert_eq!(status.code(), tonic::Code::NotFound);
}

#[tokio::test]
async fn reconnecting_reruns_migrations_idempotently() {
    let Some(storage) = test_storage("contract_test_migrations").await else {
        return;
    };
    storage
        .upsert(&[contract(
            MarketType::Local,
            1,
            T0_MS,
            &[(FR, ContractState::Active, T0_MS)],
        )])
        .await
        .unwrap();

    // When a second instance connects to the same database (a service restart)
    let url = std::env::var("DATABASE_URL").unwrap();
    let options: PgConnectOptions = url.parse().unwrap();
    let reconnected = ContractStorage::connect_with(options.database("contract_test_migrations"))
        .await
        .expect("reconnecting runs migrations idempotently");

    // Then the data is still there — the boot fold a restart performs
    assert_eq!(loaded(&reconnected).await.len(), 1);
}
