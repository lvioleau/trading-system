//! Self-match halts — the order gate's tripwire for a live-gateway internal
//! trade.
//!
//! An internal trade (a match between two of our own orders) is impossible under
//! the gateway's IOC-only order entry: nothing of ours rests in the book to
//! cross against. On a *live* gateway one means something is badly wrong — an
//! order reached the market outside this gateway's control, or the IOC guarantee
//! broke — so we stop trading the affected `(portfolio, contract)` outright
//! rather than keep sending orders into a book we evidently don't understand.
//!
//! [`SelfMatchHalts`] is the shared set of halted pairs: the execution feed's
//! internal-trade ingest (`grpc::feed`) writes it on a live gateway, and the
//! order gate (`grpc::orders`) reads it and rejects entry with
//! `FAILED_PRECONDITION`. Clones share one set (the `Arc`); the mutex is held
//! only for a single insert or lookup, never across an await. It is a manual
//! tripwire, not a transient: a halt stays until the process restarts (and the
//! reconciliation sweep re-establishes it from the durable internal-trade record
//! for anything still in its window), so a halted pair needs operator eyes.

use std::collections::HashSet;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

/// A halted `(portfolio, contract)` pair. The contract is keyed by its market
/// type's wire discriminant (`MarketType as i32`, shared identically between the
/// proto and domain enums) plus the bare nordpool id, so writer (domain
/// `InternalTrade`) and reader (proto `OrderEntry`) agree without an enum
/// conversion.
type HaltKey = (String, i32, i64);

/// The set of `(portfolio, contract)` pairs no longer accepting orders after a
/// live-gateway self-match. Cheap to clone; clones share one set.
#[derive(Clone, Default)]
pub struct SelfMatchHalts {
    halted: Arc<Mutex<HashSet<HaltKey>>>,
}

impl SelfMatchHalts {
    fn lock(&self) -> MutexGuard<'_, HashSet<HaltKey>> {
        self.halted.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Halt further order entry for `(portfolio_id, market_type, contract_id)`.
    /// Idempotent — returns `true` only when this pair was newly halted, so the
    /// caller can alarm once rather than on every re-observation (the sweep
    /// re-ingests the same internal trade each tick).
    pub fn halt(&self, portfolio_id: &str, market_type: i32, contract_id: i64) -> bool {
        self.lock()
            .insert((portfolio_id.to_owned(), market_type, contract_id))
    }

    /// Whether order entry for `(portfolio_id, market_type, contract_id)` is
    /// halted.
    pub fn is_halted(&self, portfolio_id: &str, market_type: i32, contract_id: i64) -> bool {
        self.lock()
            .contains(&(portfolio_id.to_owned(), market_type, contract_id))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn halts_are_per_portfolio_and_contract_and_idempotent() {
        let halts = SelfMatchHalts::default();
        assert!(!halts.is_halted("pf", 2, 7));

        // First halt is newly-inserted; a repeat is a no-op (so the sweep's
        // re-ingest doesn't re-alarm).
        assert!(halts.halt("pf", 2, 7));
        assert!(!halts.halt("pf", 2, 7));
        assert!(halts.is_halted("pf", 2, 7));

        // A different portfolio, market type, or contract is a distinct pair.
        assert!(!halts.is_halted("pf-2", 2, 7));
        assert!(!halts.is_halted("pf", 1, 7));
        assert!(!halts.is_halted("pf", 2, 8));
    }
}
