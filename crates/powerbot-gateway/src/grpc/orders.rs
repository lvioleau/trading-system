//! The gRPC order service: the executor's write path.
//!
//! [`OrderService::add_order`] translates the gRPC request into a PowerBot
//! [`OrderEntry`], fixes it to Immediate-Or-Cancel, forwards it over REST via
//! [`RestClient`](crate::rest::RestClient), and relays PowerBot's [`OwnOrder`]
//! response back. Per docs/design.md the order-action RPCs are unary and
//! synchronous over the REST round trip.

// `tonic::Status` is a large error type, but returning `Result<_, Status>` is
// the idiom at every gRPC boundary in this module — accept the size.
#![allow(clippy::result_large_err)]

use powerbot_models::models::{self, OrderEntry};
use tonic::{Request, Response, Status};

use super::MarketStatus;
use super::proto::powerbot_order_service_server::PowerbotOrderService;
use super::proto::{self, AddOrderRequest, AddOrderResponse, OrderSide};
use crate::contracts::{ContractKey, ContractStore, join_contract_id};
use crate::halts::SelfMatchHalts;
use crate::rest::{RestClient, RestError};
use crate::time::{now_millis, parse_ts};

/// Wire price scale: the proto carries prices as integer minor currency units
/// per MWh (e.g. euro cents/MWh), while PowerBot's REST models are `f64` in the
/// contract currency per MWh. Multiply model → wire, divide wire → model.
const PRICE_MINOR_UNITS_PER_MAJOR: f64 = 100.0;
/// Wire quantity scale: the proto carries quantities as integer kilowatts, while
/// PowerBot's REST models are `f64` megawatts. Multiply model → wire, divide the
/// other way. Mirrors the nordpool-connectors marketdata feeds.
const QUANTITY_KW_PER_MW: f64 = 1000.0;

/// PowerBot price (currency/MWh) → wire minor units per MWh, rounded to nearest;
/// absent prices collapse to 0.
pub(super) fn price_to_wire(price: Option<f64>) -> i64 {
    (price.unwrap_or_default() * PRICE_MINOR_UNITS_PER_MAJOR).round() as i64
}

/// PowerBot quantity (MW) → wire kW, rounded to nearest; absent collapses to 0.
pub(super) fn quantity_to_wire(quantity: Option<f64>) -> i64 {
    (quantity.unwrap_or_default() * QUANTITY_KW_PER_MW).round() as i64
}

/// Wire minor units per MWh → PowerBot price (currency/MWh). Inverse of
/// [`price_to_wire`]. The single division is the correctly-rounded nearest f64
/// and the wire integer is exact for any realistic price, so no precision is
/// lost feeding PowerBot's `f64` REST model.
fn price_from_wire(price: i64) -> f64 {
    price as f64 / PRICE_MINOR_UNITS_PER_MAJOR
}

/// Wire kW → PowerBot quantity (MW). Inverse of [`quantity_to_wire`].
fn quantity_from_wire(quantity: i64) -> f64 {
    quantity as f64 / QUANTITY_KW_PER_MW
}

/// The order service: forwards each `AddOrder` to PowerBot's `addOrders`
/// endpoint as an IOC order, after gating on the served portfolios, the
/// websocket-derived market status, the self-match halt set, and contract
/// tradability, and running pre-trade risk checks.
pub(crate) struct OrderService {
    rest: RestClient,
    /// Latest market state from the websocket feed; read to gate order entry.
    market: MarketStatus,
    /// PowerBot's view of each contract (REST sweep + `contracts` feed); read
    /// to gate order entry on contract tradability.
    contracts: ContractStore,
    /// The portfolios this gateway serves (config `portfolio_ids`); read to
    /// gate order entry — see [`check_portfolio`](Self::check_portfolio).
    portfolio_ids: Vec<String>,
    /// Contracts halted after a live-gateway self-match; read to refuse entry —
    /// see [`check_self_match_halt`](Self::check_self_match_halt). Written by the
    /// execution feed's internal-trade ingest.
    halts: SelfMatchHalts,
}

impl OrderService {
    pub(crate) fn new(
        rest: RestClient,
        market: MarketStatus,
        contracts: ContractStore,
        portfolio_ids: Vec<String>,
        halts: SelfMatchHalts,
    ) -> Self {
        Self {
            rest,
            market,
            contracts,
            portfolio_ids,
            halts,
        }
    }

    /// Reject an order on a portfolio this gateway does not serve. PowerBot
    /// might well accept it (the API key decides), but its fills would arrive
    /// on a topic this gateway never subscribed — executed quantity no
    /// execution stream ever reports, which downstream is silent position
    /// loss. Same fail-fast rationale as the execution streams' subscription
    /// scope.
    fn check_portfolio(&self, portfolio_id: &str) -> Result<(), Status> {
        if self.portfolio_ids.iter().any(|p| p == portfolio_id) {
            Ok(())
        } else {
            Err(Status::invalid_argument(format!(
                "portfolio_id {portfolio_id:?} is not served by this gateway \
                 (it serves {:?}); its fills would never reach the execution streams",
                self.portfolio_ids
            )))
        }
    }

    /// Reject order entry when PowerBot's market status says trading isn't
    /// possible, failing fast locally instead of round-tripping an order the
    /// exchange will refuse (docs/design.md open question: "reject locally when
    /// disconnected / market down, forward otherwise"). We forward on `OK` and
    /// `WARNING` ("trading *might* not be possible" — let the exchange decide)
    /// and reject `FAILURE` / `LOGGED_OUT`, and the unknown state — before the
    /// first update from the feed or the REST market poll, or right after a
    /// session drop, both of which the poll refills within one interval.
    fn check_market_status(&self) -> Result<(), Status> {
        match *self.market.borrow() {
            Some(models::MarketState::Ok) => Ok(()),
            Some(models::MarketState::Warning) => {
                tracing::warn!("market status WARNING; forwarding order anyway");
                Ok(())
            }
            Some(state) => Err(Status::failed_precondition(format!(
                "market not open for trading: status {state}"
            ))),
            None => Err(Status::failed_precondition(
                "market status unknown (no update from the marketstatus feed or market poll yet)",
            )),
        }
    }

    /// Reject an order for a contract PowerBot has definitively said can't
    /// trade right now — trading disabled in the delivery area, outside its
    /// trading window, or delivery already over — failing fast locally like
    /// the market-status gate. A contract the store doesn't know (not swept
    /// yet, or new since) forwards: the store is a fail-fast second check on
    /// PowerBot's own data, not the authority, so a cold or stale store never
    /// blocks a good order — PowerBot rejects what we can't see.
    fn check_contract(&self, key: ContractKey) -> Result<(), Status> {
        self.contracts
            .check_tradable(key, now_millis())
            .map_err(|reason| {
                let id = join_contract_id(key.0, key.1).unwrap_or_else(|| key.1.to_string());
                Status::failed_precondition(format!("contract {id} not tradable: {reason}"))
            })
    }

    /// Reject an order for a `(portfolio, contract)` halted after a live-gateway
    /// self-match — an internal trade (two of our own orders crossing) that
    /// IOC-only entry should make impossible. Once one is seen we stop trading
    /// the pair outright rather than send more orders into a book we evidently
    /// don't understand; the halt is a manual tripwire that clears only on
    /// restart (and needs operator eyes). Written by the execution feed's
    /// internal-trade ingest (`crate::halts`).
    fn check_self_match_halt(
        &self,
        portfolio_id: &str,
        contract: ContractKey,
    ) -> Result<(), Status> {
        if self
            .halts
            .is_halted(portfolio_id, contract.0 as i32, contract.1)
        {
            let id =
                join_contract_id(contract.0, contract.1).unwrap_or_else(|| contract.1.to_string());
            Err(Status::failed_precondition(format!(
                "trading halted for portfolio {portfolio_id:?} contract {id}: a self-match \
                 (internal trade) was seen on this live gateway — order entry for this \
                 (portfolio, contract) is disabled pending investigation"
            )))
        } else {
            Ok(())
        }
    }

    /// Pre-trade risk checks — placeholder seam. PowerBot runs its own
    /// server-side risk management (cash / position / order-rate limits) today,
    /// so the gateway adds none yet (docs/design.md open question: "Risk
    /// checks"). When gateway-side limits arrive, enforce them here — reading
    /// shared position / limit state — and reject before the order reaches
    /// PowerBot with `failed_precondition` (a limit breach) or
    /// `resource_exhausted` (an order-rate cap).
    fn check_risk(&self, _exchange: &str, _order: &OrderEntry) -> Result<(), Status> {
        Ok(())
    }
}

#[tonic::async_trait]
impl PowerbotOrderService for OrderService {
    async fn add_order(
        &self,
        request: Request<AddOrderRequest>,
    ) -> Result<Response<AddOrderResponse>, Status> {
        let AddOrderRequest { exchange, order } = request.into_inner();
        let order = order.ok_or_else(|| Status::invalid_argument("missing order"))?;

        if exchange.is_empty() {
            return Err(Status::invalid_argument("exchange is required"));
        }

        // Gate on the served portfolios, the websocket-derived market status,
        // and the self-match halt set, then build the order and gate on contract
        // tradability and risk before it leaves the gateway. The key is captured
        // before `to_order_entry` consumes the proto (which also validates it: a
        // zero/unspecified key errors there).
        self.check_portfolio(&order.portfolio_id)?;
        self.check_market_status()?;
        let contract = (order.market_type(), order.contract_id);
        self.check_self_match_halt(&order.portfolio_id, contract)?;
        let entry = to_order_entry(order)?;
        self.check_contract(contract)?;
        self.check_risk(&exchange, &entry)?;

        tracing::info!(
            %exchange,
            portfolio_id = %entry.portfolio_id,
            order = ?entry,
            "forwarding IOC order to PowerBot"
        );

        let own_order = self
            .rest
            .add_order(&exchange, entry)
            .await
            .map_err(rest_error_to_status)?;

        Ok(Response::new(AddOrderResponse {
            order: Some(from_own_order(own_order)),
        }))
    }
}

/// Translate the gRPC request into a PowerBot [`OrderEntry`], fixing the fields
/// that make it an IOC order:
/// - `ordr_exe_restriction = IOC` — execute immediately, cancel the remainder;
/// - `validity_res = NON` — mandatory for IOC per the spec;
/// - `type = O` — a regular limit order.
fn to_order_entry(order: proto::OrderEntry) -> Result<OrderEntry, Status> {
    let side = match order.side() {
        OrderSide::Buy => models::OrderSide::Buy,
        OrderSide::Sell => models::OrderSide::Sell,
        OrderSide::None => {
            return Err(Status::invalid_argument("side must be BUY or SELL"));
        }
    };

    // Quantity must be positive; the price is unconstrained — negative prices are
    // legitimate in intraday power markets. A non-positive quantity is a cheap
    // local rejection of an order PowerBot would refuse after a REST round trip.
    if order.quantity <= 0 {
        return Err(Status::invalid_argument("quantity must be positive"));
    }

    // Join the bare id + market type into PowerBot's prefixed contract id before
    // consuming `order`'s owned fields below. An unset market type is rejected —
    // the prefix, and so the target book, would be ambiguous.
    if order.contract_id <= 0 {
        return Err(Status::invalid_argument("contract_id is required"));
    }
    let contract_id = join_contract_id(order.market_type(), order.contract_id)
        .ok_or_else(|| Status::invalid_argument("market_type must be LOCAL or XBID"))?;

    // Wire units (kW, minor-unit/MWh) → PowerBot's model units (MW, currency/MWh).
    let quantity = quantity_from_wire(order.quantity);
    let price = price_from_wire(order.price);
    let mut entry = OrderEntry::new(order.portfolio_id, quantity, price);
    entry.side = Some(side);
    entry.r#type = Some(models::OrderType::O);
    entry.ordr_exe_restriction = Some(models::OrderExecutionRestriction::Ioc);
    entry.validity_res = Some(models::ValidityRestriction::Non);

    entry.delivery_area = non_empty(order.delivery_area_eic);
    // Our cl_order_id goes onto the wire as PowerBot's `txt`, echoed back on the
    // resulting trade so consumers can correlate order → fills.
    entry.txt = non_empty(order.cl_order_id);
    entry.contract_id = Some(contract_id);

    Ok(entry)
}

/// Relay PowerBot's [`OwnOrder`] into the wire type for the `addOrder` response.
/// The synchronous response is advisory, so it goes through the same wire→domain
/// curation the feed uses ([`convert`](super::convert)) — one wire mapping, not
/// two.
pub(super) fn from_own_order(o: models::OwnOrder) -> proto::OwnOrder {
    super::convert::own_order_to_domain(o).to_proto()
}

/// Map a REST failure to a gRPC status. A PowerBot 4xx is the caller's fault
/// (bad order), so it maps to `INVALID_ARGUMENT`; everything else is upstream
/// trouble the caller can retry, so `UNAVAILABLE`.
fn rest_error_to_status(err: RestError) -> Status {
    match &err {
        RestError::Api { status, message } if status.is_client_error() => {
            Status::invalid_argument(format!("PowerBot rejected the order: {message}"))
        }
        _ => Status::unavailable(err.to_string()),
    }
}

/// Turn an empty proto string (the default for an unset field) into `None`, so
/// optional PowerBot fields are omitted from the JSON rather than sent blank.
fn non_empty(s: String) -> Option<String> {
    if s.is_empty() { None } else { Some(s) }
}

/// PowerBot timestamp → unix epoch nanoseconds; 0 when absent or unparseable.
/// Used for event times (exec/api), matching the feed's `received_at_nanos`.
pub(super) fn to_epoch_nanos(ts: Option<String>) -> i64 {
    parse_ts(ts.as_deref())
        .and_then(|dt| dt.timestamp_nanos_opt())
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn timestamps_parse_and_scale() {
        // 2020-06-18T17:00:00Z is 1_592_499_600 seconds since the epoch.
        const EPOCH_NANOS: i64 = 1_592_499_600 * 1_000_000_000;
        let z = Some("2020-06-18T17:00:00Z".to_string());
        assert_eq!(to_epoch_nanos(z), EPOCH_NANOS);
        // An offset-less timestamp is read as UTC.
        assert_eq!(
            to_epoch_nanos(Some("2020-06-18T17:00:00".to_string())),
            EPOCH_NANOS
        );
        // Sub-second precision survives.
        assert_eq!(
            to_epoch_nanos(Some("2020-06-18T17:00:00.123Z".to_string())),
            EPOCH_NANOS + 123_000_000
        );
        // Unset / unparseable collapse to the proto default.
        assert_eq!(to_epoch_nanos(None), 0);
        assert_eq!(to_epoch_nanos(Some("not a date".to_string())), 0);
    }

    fn service(market: MarketStatus, contracts: ContractStore) -> OrderService {
        service_with_halts(market, contracts, SelfMatchHalts::default())
    }

    fn service_with_halts(
        market: MarketStatus,
        contracts: ContractStore,
        halts: SelfMatchHalts,
    ) -> OrderService {
        let rest = RestClient::new("https://localhost/tenant", "test-key").unwrap();
        OrderService::new(rest, market, contracts, vec!["pf".to_string()], halts)
    }

    #[test]
    fn portfolio_gate_rejects_an_unserved_portfolio() {
        // An order on a portfolio outside the served set would trade at
        // PowerBot but its fills would never reach the execution streams
        // (topics unsubscribed) — reject locally, naming both sides.
        let (_tx, rx) = tokio::sync::watch::channel(None);
        let service = service(rx, ContractStore::default());

        assert!(service.check_portfolio("pf").is_ok());
        let err = service.check_portfolio("other-pf").unwrap_err();
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
        assert!(err.message().contains("other-pf"), "{err}");
        assert!(err.message().contains("pf"), "{err}");
    }

    #[test]
    fn market_status_gate_rejects_unknown_and_untradable_states() {
        use models::MarketState;
        use tokio::sync::watch;

        let (tx, rx) = watch::channel::<Option<MarketState>>(None);
        let service = service(rx, ContractStore::default());

        // Unknown (no update from the feed or the market poll yet) fails fast.
        let err = service.check_market_status().unwrap_err();
        assert_eq!(err.code(), tonic::Code::FailedPrecondition);

        // OK and WARNING forward; FAILURE and LOGGED_OUT reject locally.
        for (state, forwards) in [
            (MarketState::Ok, true),
            (MarketState::Warning, true),
            (MarketState::Failure, false),
            (MarketState::LoggedOut, false),
        ] {
            tx.send_replace(Some(state));
            let result = service.check_market_status();
            assert_eq!(result.is_ok(), forwards, "{state}");
        }
    }

    #[test]
    fn to_order_entry_validates_quantity_and_scales_units() {
        let order = |quantity: i64, price: i64| proto::OrderEntry {
            portfolio_id: "pf".to_string(),
            side: OrderSide::Buy as i32,
            quantity,
            price,
            contract_id: 1,
            market_type: proto::MarketType::Local as i32,
            ..Default::default()
        };

        // A negative price is a valid limit in intraday power markets, and wire
        // units (kW, minor-unit/MWh) are scaled into PowerBot's model units
        // (MW, currency/MWh): 1000 kW → 1.0 MW, -550 minor → -5.5 /MWh.
        let entry = to_order_entry(order(1000, -550)).expect("valid order");
        assert_eq!(entry.quantity, 1.0);
        assert_eq!(entry.price, -5.5);
        // The bare id + market type are joined into PowerBot's prefixed id.
        assert_eq!(entry.contract_id.as_deref(), Some("NI_1"));

        // Zero or negative quantities are rejected locally.
        for quantity in [0, -1000] {
            let err = to_order_entry(order(quantity, 1000)).unwrap_err();
            assert_eq!(err.code(), tonic::Code::InvalidArgument, "{quantity}");
        }
    }

    #[test]
    fn to_order_entry_requires_a_market_type() {
        // Without a market type the target book (NX_/NI_ prefix) is ambiguous.
        let order = proto::OrderEntry {
            portfolio_id: "pf".to_string(),
            side: OrderSide::Buy as i32,
            quantity: 1000,
            price: 1000,
            contract_id: 1,
            market_type: proto::MarketType::None as i32,
            ..Default::default()
        };
        let err = to_order_entry(order).unwrap_err();
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
    }

    #[test]
    fn contract_gate_rejects_a_known_untradable_contract_and_forwards_unknown() {
        use powerbot_models::models::ContractChangedEvent;
        use proto::MarketType;
        use tokio::sync::watch;

        let store = ContractStore::default();
        store.apply_event(&ContractChangedEvent {
            contract_id: Some("NX_7".to_string()),
            trading_enabled: Some(false),
            ..ContractChangedEvent::new()
        });
        let (_tx, rx) = watch::channel(None);
        let service = service(rx, store);

        // PowerBot said NX_7 can't trade: reject locally, naming the contract.
        let err = service.check_contract((MarketType::Xbid, 7)).unwrap_err();
        assert_eq!(err.code(), tonic::Code::FailedPrecondition);
        assert!(err.message().contains("NX_7"), "{}", err.message());

        // A contract the store has never heard of forwards (PowerBot decides).
        assert!(service.check_contract((MarketType::Xbid, 8)).is_ok());
    }

    #[test]
    fn self_match_halt_rejects_the_halted_pair_only() {
        use proto::MarketType;
        use tokio::sync::watch;

        // A live-gateway self-match halted (pf, XBID 7); everything else trades.
        let halts = SelfMatchHalts::default();
        halts.halt("pf", MarketType::Xbid as i32, 7);
        let (_tx, rx) = watch::channel(Some(models::MarketState::Ok));
        let service = service_with_halts(rx, ContractStore::default(), halts);

        // The halted pair is refused at the gate before the order leaves.
        let err = service
            .check_self_match_halt("pf", (MarketType::Xbid, 7))
            .unwrap_err();
        assert_eq!(err.code(), tonic::Code::FailedPrecondition);
        assert!(err.message().contains("NX_7"), "{}", err.message());
        assert!(err.message().contains("self-match"), "{}", err.message());

        // A different contract, market type, or portfolio is unaffected.
        assert!(
            service
                .check_self_match_halt("pf", (MarketType::Xbid, 8))
                .is_ok()
        );
        assert!(
            service
                .check_self_match_halt("pf", (MarketType::Local, 7))
                .is_ok()
        );
        assert!(
            service
                .check_self_match_halt("pf-2", (MarketType::Xbid, 7))
                .is_ok()
        );
    }

    #[test]
    fn wire_and_model_units_round_trip() {
        // Wire → model → wire is identity, pinning both scale directions so an
        // inverted divisor can't slip through: 1500 kW ↔ 1.5 MW, 4551 minor ↔
        // 45.51 currency/MWh (including a negative price).
        for (wire_qty, model_qty) in [(1500_i64, 1.5_f64), (1000, 1.0)] {
            assert_eq!(quantity_from_wire(wire_qty), model_qty);
            assert_eq!(quantity_to_wire(Some(model_qty)), wire_qty);
        }
        for (wire_price, model_price) in [(4551_i64, 45.51_f64), (-550, -5.5)] {
            assert_eq!(price_from_wire(wire_price), model_price);
            assert_eq!(price_to_wire(Some(model_price)), wire_price);
        }
    }

    #[test]
    fn own_order_price_and_quantity_scale_to_wire_units() {
        // PowerBot model units (MW, currency/MWh) → wire units (kW, minor/MWh).
        let own = models::OwnOrder {
            price: Some(-5.5),
            quantity: Some(1.0),
            ..models::OwnOrder::new(
                models::OrderState::Acti,
                "AREA".to_string(),
                models::OrderSide::Buy,
                models::OrderAction::Uadd,
            )
        };
        let wire = from_own_order(own);
        assert_eq!(wire.price, -550);
        assert_eq!(wire.quantity, 1000);
    }
}
