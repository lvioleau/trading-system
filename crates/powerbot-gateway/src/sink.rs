//! The websocket driver thread's [`EventSink`]: the bridge from decoded
//! PowerBot push events to the in-memory state the gRPC side serves.
//!
//! It fans each event out to the right destination — market status into the
//! order gate's watch, contract changes into the tradability store, own
//! trades/orders onto the execution channels — and, on session loss, resets the
//! feed-derived state that has become stale. It holds no logic of its own beyond
//! that routing and the [`session_ended`](GatewaySink::session_ended) fallback;
//! the stores it writes decide what to keep.

use std::sync::Arc;

use powerbot_models::models::MarketState;
use tokio::sync::watch;

use crate::contracts::ContractStore;
use crate::driver::{EventSink, PowerbotEvent};
use crate::grpc::ExecutionChannels;

/// The driver thread's event sink: publishes market status into the order
/// gate's watch, contract changes into the tradability store, and applies own
/// trades/orders to the execution channels.
pub struct GatewaySink {
    market_tx: watch::Sender<Option<MarketState>>,
    contracts: ContractStore,
    channels: Arc<ExecutionChannels>,
}

impl GatewaySink {
    /// Wire the sink to the three destinations `main` shares with the async
    /// side: the market-state watch the order service gates on, the contract
    /// tradability store, and the execution channels the gRPC streams serve.
    pub fn new(
        market_tx: watch::Sender<Option<MarketState>>,
        contracts: ContractStore,
        channels: Arc<ExecutionChannels>,
    ) -> Self {
        Self {
            market_tx,
            contracts,
            channels,
        }
    }
}

impl EventSink for GatewaySink {
    fn handle(&mut self, event: PowerbotEvent) -> anyhow::Result<()> {
        match &event {
            // Publish market status so the order service can gate on it.
            PowerbotEvent::MarketStatus(event) => {
                if let Some(status) = &event.market_status {
                    self.market_tx.send_replace(Some(status.status));
                }
            }
            // Keep the contract store's live fields current between sweeps.
            PowerbotEvent::ContractChanged(event) => {
                self.contracts.apply_event(event);
            }
            // Apply own trades / orders to the stores, then broadcast the
            // delta onto the feeds the gRPC streams serve (apply-then-
            // broadcast; a duplicate/regression the store absorbs sends
            // nothing, and no subscribers attached is fine too).
            PowerbotEvent::OwnTrades(changes) => {
                self.channels.own_trades(changes);
            }
            PowerbotEvent::OwnOrders(changes) => {
                self.channels.own_orders(changes);
            }
        }
        // Debug, not info: this is every decoded batch in full — the
        // metrics carry the per-topic volumes for production.
        tracing::debug!(kind = event.name(), ?event, "event");
        Ok(())
    }

    /// The session is down: the market status derived from it is stale, so the
    /// order gate falls back to "unknown" until the next session's first update
    /// (docs/design.md open question — reject locally when disconnected). The
    /// contract store is deliberately *not* reset: contracts change rarely, the
    /// REST sweep re-baselines it, and its forward-on-unknown gate makes
    /// staleness harmless — wiping it would only widen the forward-everything
    /// window.
    fn session_ended(&mut self) {
        self.market_tx.send_replace(None);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use powerbot_models::models::{ContractChangedEvent, MarketStatus, MarketStatusChangedEvent};

    fn sink() -> (
        GatewaySink,
        watch::Receiver<Option<MarketState>>,
        ContractStore,
    ) {
        let (tx, rx) = watch::channel::<Option<MarketState>>(None);
        let contracts = ContractStore::default();
        let channels = Arc::new(ExecutionChannels::new(16));
        let sink = GatewaySink::new(tx, contracts.clone(), channels);
        (sink, rx, contracts)
    }

    fn market_event(status: MarketState) -> PowerbotEvent {
        PowerbotEvent::MarketStatus(Box::new(MarketStatusChangedEvent {
            market_status: Some(MarketStatus::new(status)),
            ..MarketStatusChangedEvent::new()
        }))
    }

    fn contract_event(id: &str) -> PowerbotEvent {
        PowerbotEvent::ContractChanged(ContractChangedEvent {
            contract_id: Some(id.to_string()),
            trading_enabled: Some(true),
            delivery_start: Some("2026-07-10T14:00:00Z".to_string()),
            delivery_end: Some("2026-07-10T15:00:00Z".to_string()),
            ..ContractChangedEvent::new()
        })
    }

    #[test]
    fn market_status_is_published_to_the_watch() {
        let (mut sink, rx, _contracts) = sink();
        assert_eq!(*rx.borrow(), None);

        sink.handle(market_event(MarketState::Ok)).unwrap();
        assert_eq!(*rx.borrow(), Some(MarketState::Ok));

        sink.handle(market_event(MarketState::Failure)).unwrap();
        assert_eq!(*rx.borrow(), Some(MarketState::Failure));
    }

    #[test]
    fn session_ended_clears_market_status() {
        let (mut sink, rx, _contracts) = sink();
        sink.handle(market_event(MarketState::Ok)).unwrap();
        assert_eq!(*rx.borrow(), Some(MarketState::Ok));

        sink.session_ended();
        assert_eq!(
            *rx.borrow(),
            None,
            "the feed-derived market status goes unknown while disconnected"
        );
    }

    #[test]
    fn session_ended_preserves_the_contract_store() {
        let (mut sink, _rx, contracts) = sink();
        sink.handle(contract_event("NX_12345")).unwrap();
        assert_eq!(contracts.len(), 1);

        sink.session_ended();
        assert_eq!(
            contracts.len(),
            1,
            "contracts survive a session drop; the REST sweep re-baselines them"
        );
    }
}
