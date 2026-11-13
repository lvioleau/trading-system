//! Feed client: one gRPC subscription to the connector's `SubscribeContracts`, filtered to the
//! configured delivery areas, writing every update through to Postgres, folding it into the
//! shared [`ContractStore`](crate::store::ContractStore), and broadcasting each changed batch
//! to the API's subscribers — durable before visible, in that order. The only module that
//! knows both the wire and domain contract types.

use anyhow::Context;
use eic::EicCode;
use flow_util::feed::Backoff;
use tokio::time::Instant;
use tracing::{debug, info, warn};

use crate::channel::{ContractSender, ContractUpdate, SharedContracts};
use crate::config::FeedConfig;
use crate::marketdata;
use crate::marketdata::contract_service_client::ContractServiceClient;
use crate::storage::ContractStorage;
use crate::store::{AreaState, Contract, ContractKey, ContractState, MarketType, ProductType};

/// Runs the subscription forever: connect, subscribe filtered to the configured delivery
/// areas, apply updates (Postgres, then store, then broadcast); on stream end or error —
/// including a failed database write — back off and reconnect (tuned by [`FeedConfig`]).
/// The connector re-seeds the filtered set on subscribe, and the `updated_at` guards in both
/// the store and the SQL upserts make the overlap a no-op, so a reconnect can duplicate but
/// never regress — which is also what makes retry-by-reconnect safe after a partial failure.
///
/// `storage: None` runs memory-only — the mock-connector tests; the service always writes
/// through.
pub async fn run(
    endpoint: String,
    delivery_area_eics: Vec<EicCode>,
    tuning: FeedConfig,
    storage: Option<ContractStorage>,
    store: SharedContracts,
    updates: ContractSender,
) {
    let mut backoff = Backoff::new(&tuning);
    loop {
        let session_start = Instant::now();
        match subscribe(
            &endpoint,
            &delivery_area_eics,
            &tuning,
            storage.as_ref(),
            &store,
            &updates,
        )
        .await
        {
            Ok(()) => warn!(?delivery_area_eics, "contract stream closed by server"),
            Err(error) => warn!(%error, ?delivery_area_eics, "contract subscription failed"),
        }
        backoff.session_ended(session_start.elapsed());
        tokio::time::sleep(backoff.next_delay()).await;
    }
}

async fn subscribe(
    endpoint: &str,
    delivery_area_eics: &[EicCode],
    tuning: &FeedConfig,
    storage: Option<&ContractStorage>,
    store: &SharedContracts,
    updates: &ContractSender,
) -> anyhow::Result<()> {
    let channel = flow_util::feed::endpoint(endpoint, tuning)?
        .connect()
        .await?;
    let mut client = ContractServiceClient::new(channel);
    let request = marketdata::SubscribeContractsRequest {
        eic_codes: delivery_area_eics
            .iter()
            .map(|area| area.eic_code().to_owned())
            .collect(),
    };
    let mut stream = client.subscribe_contracts(request).await?.into_inner();
    info!(?delivery_area_eics, "subscribed to contract feed");
    // The connector guarantees seq is non-decreasing within a session (deltas past the seed are
    // strictly newer; gaps are frames that changed nothing). A regression is a connector bug —
    // warn (tripwire) but still apply: the store's updated_at guards make a stale batch a no-op.
    let mut last_seq = i64::MIN;
    while let Some(update) = stream.message().await? {
        if update.seq < last_seq {
            warn!(seq = update.seq, last_seq, "contract update seq regressed");
        }
        last_seq = last_seq.max(update.seq);
        apply_update(storage, store, updates, &update).await?;
    }
    Ok(())
}

async fn apply_update(
    storage: Option<&ContractStorage>,
    store: &SharedContracts,
    updates: &ContractSender,
    update: &marketdata::ContractUpdate,
) -> anyhow::Result<()> {
    let contracts: Vec<Contract> = update
        .contracts
        .iter()
        .filter_map(to_domain_contract)
        .collect();
    // Durable before visible: the batch reaches Postgres before the store and the broadcast.
    // A failed write fails the session — nothing was made visible, and the reconnect re-seed
    // replays the batch against the idempotent upserts.
    if let Some(storage) = storage {
        storage
            .upsert(&contracts)
            .await
            .context("writing contract batch through to postgres")?;
    }
    let received = update.contracts.len();
    let mut changed = Vec::new();
    {
        let mut store = store.write().await;
        for contract in contracts {
            let key = contract.key;
            if store.upsert(contract) {
                changed.push(store.get(&key).expect("just upserted").clone());
            }
        }
    }
    debug!(
        seq = update.seq,
        received,
        changed = changed.len(),
        "applied contract update"
    );
    if !changed.is_empty() {
        // Store written above, broadcast second (the seed-then-deltas ordering rule). Send only
        // errs when no subscriber is listening, which is fine.
        let _ = updates.send(ContractUpdate {
            contracts: changed,
            received_at_nanos: update.received_at_nanos,
        });
    }
    Ok(())
}

/// Translate one wire contract into the domain [`Contract`]. Dropped with a warn (tripwire):
/// a contract without a recognizable market type (`market_type` is part of the identity, so
/// there is no correct place to store it), or one carrying a timestamp Postgres cannot
/// represent — persisting that would fail the whole batch, and since the reconnect re-seed
/// replays the same frame, wedge the feed forever on one poison contract.
fn to_domain_contract(contract: &marketdata::Contract) -> Option<Contract> {
    let market_type = match contract.market_type() {
        marketdata::MarketType::Local => MarketType::Local,
        marketdata::MarketType::Xbid => MarketType::Xbid,
        marketdata::MarketType::None => {
            warn!(
                contract_id = contract.contract_id,
                "dropping contract without market type"
            );
            return None;
        }
    };
    // The area states are stamped with updated_at_ms below, so these three cover every
    // timestamp the storage layer converts.
    for (field, ms) in [
        ("updated_at_ms", contract.updated_at_ms),
        ("delivery_start_ms", contract.delivery_start_ms),
        ("delivery_end_ms", contract.delivery_end_ms),
    ] {
        if chrono::DateTime::from_timestamp_millis(ms).is_none() {
            warn!(
                contract_id = contract.contract_id,
                field, ms, "dropping contract with out-of-range timestamp"
            );
            return None;
        }
    }
    let areas = contract
        .delivery_areas
        .iter()
        .filter_map(|area| {
            // An EIC we don't know is a tripwire (the vocabulary predates a new area, or the
            // feed is misbehaving): drop the area state, keep the contract — never as skipped
            // whole, which could hide the areas we do know.
            let Some(eic) = EicCode::from_eic(&area.eic_code) else {
                warn!(
                    contract_id = contract.contract_id,
                    eic = %area.eic_code,
                    "dropping area state with unknown EIC"
                );
                return None;
            };
            let state = AreaState {
                state: map_contract_state(area.state()),
                updated_at_ms: contract.updated_at_ms,
            };
            Some((eic, state))
        })
        .collect();
    Some(Contract {
        key: ContractKey {
            market_type,
            contract_id: contract.contract_id,
        },
        name: contract.name.clone(),
        market_id: contract.market_id.clone(),
        product_type: map_product_type(contract.product_type()),
        delivery_start_ms: contract.delivery_start_ms,
        delivery_end_ms: contract.delivery_end_ms,
        duration_seconds: contract.duration_seconds,
        updated_at_ms: contract.updated_at_ms,
        areas,
    })
}

fn map_product_type(product_type: marketdata::ProductType) -> ProductType {
    match product_type {
        marketdata::ProductType::P15Min => ProductType::P15Min,
        marketdata::ProductType::P30Min => ProductType::P30Min,
        marketdata::ProductType::P60Min => ProductType::P60Min,
        marketdata::ProductType::Other | marketdata::ProductType::None => ProductType::Other,
    }
}

fn map_contract_state(state: marketdata::ContractState) -> ContractState {
    match state {
        marketdata::ContractState::Active => ContractState::Active,
        marketdata::ContractState::Hibernated => ContractState::Hibernated,
        marketdata::ContractState::Inactive => ContractState::Inactive,
        marketdata::ContractState::Frozen => ContractState::Frozen,
        marketdata::ContractState::None => ContractState::Unknown,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn wire_contract() -> marketdata::Contract {
        marketdata::Contract {
            contract_id: 7,
            name: "PH-20260704-14".to_owned(),
            market_id: "M1".to_owned(),
            market_type: marketdata::MarketType::Local as i32,
            product_type: marketdata::ProductType::P15Min as i32,
            delivery_start_ms: 1_000,
            delivery_end_ms: 1_900,
            duration_seconds: 900,
            updated_at_ms: 42,
            delivery_areas: vec![marketdata::ContractDeliveryArea {
                eic_code: "10YFR-RTE------C".to_owned(),
                state: marketdata::ContractState::Active as i32,
            }],
        }
    }

    #[test]
    fn translates_wire_contract() {
        // Given a wire contract with one delivery area
        let wire = wire_contract();

        // When it is translated
        let domain = to_domain_contract(&wire).unwrap();

        // Then identity, intrinsics, and the area state carry over, stamped with the frame's
        // update time
        assert_eq!(domain.key.market_type, MarketType::Local);
        assert_eq!(domain.key.contract_id, 7);
        assert_eq!(domain.name, "PH-20260704-14");
        assert_eq!(domain.product_type, ProductType::P15Min);
        assert_eq!(domain.updated_at_ms, 42);
        assert_eq!(
            domain.areas[&EicCode::AREA_FR],
            AreaState {
                state: ContractState::Active,
                updated_at_ms: 42,
            }
        );
    }

    #[test]
    fn drops_contract_without_market_type() {
        // Given a wire contract with the unset market type
        let mut wire = wire_contract();
        wire.market_type = marketdata::MarketType::None as i32;

        // When it is translated
        let domain = to_domain_contract(&wire);

        // Then it is dropped — market type is part of the identity
        assert!(domain.is_none());
    }

    #[test]
    fn drops_contract_with_out_of_range_timestamp() {
        // Given a wire contract whose delivery start Postgres cannot represent
        let mut wire = wire_contract();
        wire.delivery_start_ms = i64::MAX;

        // When it is translated
        let domain = to_domain_contract(&wire);

        // Then it is dropped — persisting it would fail its batch on every reconnect replay
        assert!(domain.is_none());
    }

    #[test]
    fn folds_unset_product_type_to_other() {
        // Given a wire contract with the unset product type
        let mut wire = wire_contract();
        wire.product_type = marketdata::ProductType::None as i32;

        // When it is translated
        let domain = to_domain_contract(&wire).unwrap();

        // Then the product collapses to Other
        assert_eq!(domain.product_type, ProductType::Other);
    }
}
