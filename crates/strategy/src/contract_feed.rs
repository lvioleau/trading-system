//! The tradable-contracts feed: a subscription to the Contract Service's live set for one
//! delivery area, folded into the shared [`SharedContracts`] view the feed driver reads. The only
//! module that knows both the contract wire types and the driver's contract view.
//!
//! `SubscribeContracts` is seed-then-deltas — the first message is the full current set, then each
//! change — and every message is a batch of upserts keyed by contract identity (contracts are
//! never removed, they change state). So the fold is a plain merge: each contract's delivery
//! window and its state in the subscribed area, overwritten in place. A terminal status means
//! reconnect, which re-seeds. This is a mock's dependency, so the reconnect is a simple capped
//! backoff rather than the services' tunable one.

use anyhow::Result;
use flow_util::config::FeedConfig;
use tokio::time::Duration;
use tokio_stream::StreamExt;
use tracing::{info, warn};

use eic::EicCode;

use crate::contract;
use crate::contract::contract_service_client::ContractServiceClient;
use crate::driver::{ContractId, ContractView, MarketType, SharedContracts};

const INITIAL_BACKOFF: Duration = Duration::from_millis(500);
const MAX_BACKOFF: Duration = Duration::from_secs(30);

/// Subscribe to the Contract Service forever, folding the live set for `delivery_area_eic` into
/// `contracts`; reconnect with capped backoff on any stream end or error.
pub async fn run(endpoint: String, delivery_area_eic: EicCode, contracts: SharedContracts) {
    let mut backoff = INITIAL_BACKOFF;
    loop {
        match subscribe(&endpoint, delivery_area_eic, &contracts).await {
            Ok(()) => {
                warn!(endpoint, "contract stream closed by server");
                backoff = INITIAL_BACKOFF; // a clean session; treat the next attempt as fresh
            }
            Err(error) => warn!(
                endpoint,
                error = format!("{error:#}"),
                "contract subscription failed"
            ),
        }
        tokio::time::sleep(backoff).await;
        backoff = (backoff * 2).min(MAX_BACKOFF);
    }
}

async fn subscribe(
    endpoint: &str,
    delivery_area_eic: EicCode,
    contracts: &SharedContracts,
) -> Result<()> {
    // The mock is not tunable — the shared keepalive defaults are exactly what it wants.
    let channel = flow_util::feed::endpoint(endpoint, &FeedConfig::default())?
        .connect()
        .await?;
    let mut client = ContractServiceClient::new(channel);
    let mut stream = client
        .subscribe_contracts(contract::SubscribeContractsRequest {
            delivery_area_eic: delivery_area_eic.to_string(),
        })
        .await?
        .into_inner();
    info!(endpoint, %delivery_area_eic, "subscribed to contracts");
    while let Some(update) = stream.next().await {
        let update = update?;
        let mut view = contracts.lock().expect("contract view poisoned");
        for wire in &update.contracts {
            if let Some((id, contract_view)) = to_view(wire, delivery_area_eic) {
                view.insert(id, contract_view);
            }
        }
    }
    Ok(())
}

/// Translate one wire contract into the driver's view: its identity, delivery start, and whether
/// it is active for trading in the subscribed area. A contract with no market segment is skipped.
fn to_view(wire: &contract::Contract, area: EicCode) -> Option<(ContractId, ContractView)> {
    let market_type = match wire.market_type() {
        contract::MarketType::Local => MarketType::Local,
        contract::MarketType::Xbid => MarketType::Xbid,
        contract::MarketType::None => return None,
    };
    let active = wire.delivery_areas.iter().any(|delivery_area| {
        delivery_area.delivery_area_eic == area.eic_code()
            && delivery_area.state() == contract::ContractState::Active
    });
    Some((
        (market_type, wire.contract_id),
        ContractView {
            delivery_start_ms: wire.delivery_start_ms,
            active,
        },
    ))
}
