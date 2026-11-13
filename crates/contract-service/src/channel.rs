//! The shared runtime state connecting the feed to the API: the in-memory contract index
//! behind a read-write lock, and the broadcast channel fanning each changed batch out to the
//! API's subscribers. Types only — the ordering rules that make them safe (store written
//! before broadcast, subscribe before seed) live with the feed and API that honor them.

use std::sync::Arc;

use tokio::sync::{RwLock, broadcast};

use crate::store::{Contract, ContractStore};

/// The in-memory index, shared between the feed (its writer) and the API (its readers).
pub type SharedContracts = Arc<RwLock<ContractStore>>;

/// The contracts one feed frame changed, fanned out to the API's subscribers — the merged
/// stored state (not the raw frame: a partially-stale frame can change an area state while its
/// older intrinsics are ignored), stamped with the frame's connector receive time. The domain
/// twin of the wire `proto::ContractUpdate`, named like the connector's own broadcast payload.
#[derive(Debug, Clone)]
pub struct ContractUpdate {
    pub contracts: Vec<Contract>,
    pub received_at_nanos: i64,
}

/// Sender half of the change broadcast. The feed writes the store *before* sending — the
/// ordering that lets an API subscriber join the channel, seed from the store, and know the
/// overlap only duplicates, never drops.
pub type ContractSender = broadcast::Sender<ContractUpdate>;
