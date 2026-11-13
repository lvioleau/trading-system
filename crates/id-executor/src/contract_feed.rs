//! The contract feed — tradeability's upstream: one reconnecting Contract Service
//! `SubscribeContracts(delivery_area_eic)` task for the executor's single delivery area
//! (area-shared: every portfolio's gaps gate on the same live set), translating the stream
//! into [`ContractEvent`]s on the shared engine channel. The only module that knows both
//! the contract wire types and the domain [`Contract`].
//!
//! **Subscription discipline.** Seed-then-deltas, per the service's contract: each
//! session's first message is the area's full current live set and becomes a
//! [`ContractEvent::Seeded`]; every later batch is a [`ContractEvent::Updated`] carrying
//! the contracts one feed frame changed. Every message is a batch of upserts keyed by
//! contract identity — contracts are never removed, they change state — and the
//! [`ContractStore`](crate::contract_manager::ContractStore) folds them guarded by the venue's
//! `updated_at_ms`, so a reconnect's re-seed (and any replay) refreshes and never
//! regresses. Unlike the depth feed (step 4), nothing is cleared on a session end: a
//! lifecycle state does not go stale the way a book does, the trading cutoff keeps running
//! on the local clock either way, and a closure missed while disconnected is caught by the
//! venue's own rejection of the order. There is no fatal subscription error: an area the
//! service is not configured for deliberately seeds empty on the service side (matching the
//! connector's filter semantics — the mismatch surfaces as a live set that never fills), so
//! [`run`] returns only when the event channel closes — the engine is gone and the boot
//! should fail.

use anyhow::{Result, anyhow};
use flow_util::feed::Backoff;
use tokio::sync::mpsc;
use tokio::time::Instant;
use tracing::{error, info, warn};

use eic::EicCode;

use crate::config::FeedConfig;
use crate::executor::{Contract, ContractKey, ContractState, MarketType};
use crate::proto;
use crate::proto::contract_service_client::ContractServiceClient;

/// How many consecutive failed sessions escalate the log level from warn to error — a
/// service restart heals in one or two reconnects; anything still failing after this many
/// means tradeability is going stale and needs eyes.
const FAILURES_BEFORE_ERROR: u32 = 3;

/// One message from the contract subscription to the engine.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ContractEvent {
    /// A session's seed — the area's full current live set (or a re-seed after a drop).
    /// Marks the tradeability baseline complete: from here on an absent contract means
    /// not tradeable, rather than not yet known.
    Seeded(Vec<Contract>),
    /// The contracts one feed frame changed — full current records, upserted by identity.
    Updated(Vec<Contract>),
}

/// Runs the `SubscribeContracts(delivery_area_eic)` subscription forever, reconnecting with
/// backoff (tuned by [`FeedConfig`]) and sending its events into `events_tx`. Returns only
/// when the subscription hits its one fatal condition — the event channel closed, i.e. the
/// engine is gone — which the caller treats as a boot failure.
pub async fn run(
    endpoint: String,
    delivery_area_eic: EicCode,
    tuning: FeedConfig,
    events_tx: mpsc::Sender<ContractEvent>,
) -> anyhow::Error {
    let mut backoff = Backoff::new(&tuning);
    let mut consecutive_failures: u32 = 0;
    loop {
        let session_start = Instant::now();
        let result = subscribe(&endpoint, delivery_area_eic, &tuning, &events_tx).await;
        consecutive_failures += 1;
        match result {
            Ok(()) => warn!(
                endpoint,
                %delivery_area_eic, "contract stream closed by server"
            ),
            Err(SessionError::Fatal(error)) => return error,
            Err(SessionError::Retry(error)) if consecutive_failures >= FAILURES_BEFORE_ERROR => {
                error!(
                    endpoint,
                    %delivery_area_eic,
                    consecutive_failures,
                    error = format!("{error:#}"),
                    "contract subscription failing repeatedly; tradeability is going stale"
                )
            }
            Err(SessionError::Retry(error)) => warn!(
                endpoint,
                %delivery_area_eic,
                error = format!("{error:#}"),
                "contract subscription failed"
            ),
        }
        if backoff.session_ended(session_start.elapsed()) {
            consecutive_failures = 1; // this session's own failure only
        }
        tokio::time::sleep(backoff.next_delay()).await;
    }
}

/// How a subscription session ended.
enum SessionError {
    /// The event channel is gone — the engine stopped. Nothing to reconnect for.
    Fatal(anyhow::Error),
    /// Transport failure, terminal stream status, or a lag drop — back off and resubscribe.
    Retry(anyhow::Error),
}

/// One session: connect, subscribe, send the seed as [`ContractEvent::Seeded`], then every
/// batch as [`ContractEvent::Updated`] until the stream ends.
async fn subscribe(
    endpoint: &str,
    delivery_area_eic: EicCode,
    tuning: &FeedConfig,
    events_tx: &mpsc::Sender<ContractEvent>,
) -> Result<(), SessionError> {
    let channel = flow_util::feed::endpoint(endpoint, tuning)
        .map_err(SessionError::Retry)?
        .connect()
        .await
        .map_err(|error| {
            SessionError::Retry(anyhow!(error).context("connecting to the contract service"))
        })?;
    let mut client = ContractServiceClient::new(channel);
    // The subscribe itself has no permanent failure mode, so every rejection is a retry: an
    // unconfigured area is deliberately not an error on the service side (it seeds empty) —
    // a config mismatch here surfaces as a live set that never fills, not a crash.
    let mut stream = client
        .subscribe_contracts(proto::SubscribeContractsRequest {
            delivery_area_eic: delivery_area_eic.to_string(),
        })
        .await
        .map_err(|status| SessionError::Retry(anyhow!(status).context("subscribing to contracts")))?
        .into_inner();
    // Tracks the seed boundary within *this* session only: a reconnect builds a new stream
    // whose first message is a fresh seed, re-sent as another `Seeded` — harmless by design,
    // since folds are guarded by the venue's `updated_at_ms` and can only refresh.
    let mut seeded = false;
    loop {
        match stream.message().await {
            // Includes the service's lag policy (this subscriber fell behind the broadcast
            // and was dropped): the recovery for every terminal status is the same —
            // resubscribe and let the fresh seed re-seed.
            Err(status) => {
                return Err(SessionError::Retry(
                    anyhow!(status).context("reading the contract stream"),
                ));
            }
            // A clean server close (e.g. a graceful drain); the caller reconnects.
            Ok(None) => return Ok(()),
            Ok(Some(update)) => {
                let contracts = to_domain_contracts(update.contracts, delivery_area_eic);
                let event = if seeded {
                    ContractEvent::Updated(contracts)
                } else {
                    info!(
                        %delivery_area_eic,
                        contracts = contracts.len(),
                        "contract live set seeded"
                    );
                    ContractEvent::Seeded(contracts)
                };
                seeded = true;
                // An error means the receiver is dropped — the engine is gone, and with it
                // the process's reason to exist. Fatal, so the boot fails instead of the
                // subscription spinning on unread events.
                if events_tx.send(event).await.is_err() {
                    return Err(SessionError::Fatal(anyhow!(
                        "contract event channel closed (the engine is gone)"
                    )));
                }
            }
        }
    }
}

/// Translate a batch, skipping malformed entries with a warning — a skipped contract only
/// delays the fold until the next update or re-seed; the `updated_at_ms` guard keeps it
/// safe (and an unknown contract is simply not tradeable meanwhile, the safe side).
fn to_domain_contracts(contracts: Vec<proto::Contract>, area: EicCode) -> Vec<Contract> {
    contracts
        .into_iter()
        .filter_map(|contract| match to_domain_contract(contract, area) {
            Ok(contract) => Some(contract),
            Err(error) => {
                warn!(
                    error = format!("{error:#}"),
                    "skipping malformed contract update"
                );
                None
            }
        })
        .collect()
}

/// Wire → domain, keeping only what tradeability needs: the state in the executor's area
/// (the subscription filter should make every served contract carry it; one that does not
/// folds as [`ContractState::None`] — never tradeable, the safe reading) and the delivery
/// window's start. `Err` covers a missing market type (half the contract identity).
fn to_domain_contract(wire: proto::Contract, area: EicCode) -> Result<Contract> {
    let market_type = match wire.market_type() {
        proto::MarketType::Local => MarketType::Local,
        proto::MarketType::Xbid => MarketType::Xbid,
        proto::MarketType::None => {
            anyhow::bail!("contract {} carries no market type", wire.contract_id)
        }
    };
    let state = wire
        .delivery_areas
        .iter()
        .find(|delivery_area| delivery_area.delivery_area_eic == area.eic_code())
        .map_or(ContractState::None, |delivery_area| {
            match delivery_area.state() {
                proto::ContractState::Active => ContractState::Active,
                proto::ContractState::Hibernated => ContractState::Hibernated,
                proto::ContractState::Inactive => ContractState::Inactive,
                proto::ContractState::Frozen => ContractState::Frozen,
                proto::ContractState::None => ContractState::None,
            }
        });
    Ok(Contract {
        key: ContractKey {
            market_type,
            contract_id: wire.contract_id,
        },
        state,
        delivery_start_ms: wire.delivery_start_ms,
        updated_at_ms: wire.updated_at_ms,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    const AREA: EicCode = EicCode::AREA_FR;

    fn wire(state: proto::ContractState, area: EicCode) -> proto::Contract {
        proto::Contract {
            contract_id: 7,
            name: "PH-2026-07-14-15".to_owned(),
            market_id: "10".to_owned(),
            market_type: proto::MarketType::Local as i32,
            product_type: proto::ProductType::P60Min as i32,
            delivery_start_ms: 1_760_000_000_000,
            delivery_end_ms: 1_760_003_600_000,
            duration_seconds: 3_600,
            updated_at_ms: 100,
            delivery_areas: vec![proto::ContractDeliveryArea {
                delivery_area_eic: area.to_string(),
                state: state as i32,
            }],
        }
    }

    #[test]
    fn the_subscribed_areas_state_maps_onto_the_domain() {
        let contract = to_domain_contract(wire(proto::ContractState::Active, AREA), AREA).unwrap();
        assert_eq!(contract.state, ContractState::Active);
        assert_eq!(contract.delivery_start_ms, 1_760_000_000_000);
        assert_eq!(contract.updated_at_ms, 100);

        let inactive =
            to_domain_contract(wire(proto::ContractState::Inactive, AREA), AREA).unwrap();
        assert_eq!(inactive.state, ContractState::Inactive);
    }

    #[test]
    fn a_contract_not_listing_the_area_folds_as_state_none() {
        // The subscription filter should prevent this; if it happens anyway, the contract
        // must read as not tradeable — never as skipped (a skip could leave a stale ACTIVE).
        let contract =
            to_domain_contract(wire(proto::ContractState::Active, EicCode::AREA_CH), AREA);
        assert_eq!(contract.unwrap().state, ContractState::None);
    }

    #[test]
    fn missing_market_type_is_malformed() {
        let mut no_market_type = wire(proto::ContractState::Active, AREA);
        no_market_type.market_type = proto::MarketType::None as i32;
        assert!(to_domain_contract(no_market_type, AREA).is_err());
    }
}
