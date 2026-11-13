//! The shared runtime state connecting the ledger's writers (the `Transfer` RPC today, the
//! gateway feeds from step 5 on) to the API's subscribers: the position ledger behind a
//! read-write lock, and the broadcast channel fanning each mutation's changed positions
//! out. Types only — the ordering rules that make them safe (ledger written before
//! broadcast, subscribe before snapshot) live with the writers and the server.

use std::sync::Arc;

use tokio::sync::{RwLock, broadcast};
use tracing::info;

use crate::ledger::{Position, PositionLedger};

// The change type lives with the ledger (it is also the snapshot's history entry);
// re-exported here because it is equally the broadcast's payload.
pub use crate::ledger::PositionChange;

/// The in-memory projection, shared between the writers and the API's readers.
pub type SharedLedger = Arc<RwLock<PositionLedger>>;

/// The positions one mutation changed — full current values (a transfer carries both legs'
/// positions), each with its write attribution, fanned out to the API's subscribers, who
/// each trim to their portfolio.
#[derive(Debug, Clone)]
pub struct PositionUpdate {
    pub changes: Vec<PositionChange>,
}

/// Sender half of the change broadcast. Writers update the ledger *before* sending — the
/// ordering that lets a subscriber join the channel, snapshot the ledger, and know the
/// overlap only duplicates (deduped by revision), never drops.
pub type PositionSender = broadcast::Sender<PositionUpdate>;

/// Log one position at its post-update value — its full current quantity and new revision.
/// Writers call this for every position a mutation changed, after folding it and before (or
/// as) they broadcast it, so the log carries exactly what the subscribers receive. Both the
/// feed's fills and the server's transfers share it, so the "position updated" record reads
/// the same whatever moved it.
pub(crate) fn log_position(position: &Position) {
    info!(%position, "position updated");
}
