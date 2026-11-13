//! Domain representation of the execution feeds — what the in-memory stores hold.
//!
//! PowerBot's generated models (`powerbot_models`) are the wire shape at ingest;
//! they are converted to these domain structs **at the feed boundary** (see
//! [`crate::grpc::convert`]) before anything is applied to a store. That keeps the
//! store canonical *by construction*: the live websocket feed and the REST
//! reconciliation sweep both curate to the identical representation — our side of
//! the trade, prices/quantities as scaled integers, contract ids split from
//! PowerBot's `NX_`/`NI_` prefix, and timestamps as `i64` epoch nanos — so a
//! sweep re-fetch of an unchanged trade compares equal and is not re-broadcast.
//!
//! These mirror the served proto messages (and reuse the proto value-enums)
//! **plus** `delivery_end_nanos`, which the wire drops but the store needs to
//! evict past-delivery entries. `to_proto` is the trivial reverse used on serve.

use crate::grpc::proto::{self, MarketType, OrderAction, OrderSide, OrderState, TradeState};
use crate::stores::Seedable;

/// Floor a nanosecond timestamp to whole milliseconds. Used by the `unchanged`
/// dedup: the live feed carries nanoseconds and PowerBot's REST snapshot rounds
/// to microseconds, so the same instant differs below the millisecond — compare
/// at millisecond granularity so a sweep re-fetch isn't mistaken for a change.
fn floor_ms(nanos: i64) -> i64 {
    nanos - nanos.rem_euclid(1_000_000)
}

/// An external trade, curated to our side (see the proto `Trade`).
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Trade {
    pub trade_id: String,
    pub state: TradeState,
    pub exchange: String,
    pub delivery_area_eic: String,
    pub api_timestamp_nanos: i64,
    pub exec_time_nanos: i64,
    pub side: OrderSide,
    pub order_id: String,
    pub portfolio_id: String,
    pub contract_id: i64,
    pub market_type: MarketType,
    pub contract_name: String,
    pub price: i64,
    pub quantity: i64,
    pub self_trade: bool,
    pub strategy_id: String,
    pub unit_id: String,
    pub cl_order_id: String,
    /// Delivery end as unix epoch nanos, for eviction; `None` when unknown.
    pub delivery_end_nanos: Option<i64>,
}

impl Trade {
    pub fn to_proto(&self) -> proto::Trade {
        proto::Trade {
            trade_id: self.trade_id.clone(),
            state: self.state as i32,
            exchange: self.exchange.clone(),
            delivery_area_eic: self.delivery_area_eic.clone(),
            api_timestamp_nanos: self.api_timestamp_nanos,
            exec_time_nanos: self.exec_time_nanos,
            side: self.side as i32,
            order_id: self.order_id.clone(),
            portfolio_id: self.portfolio_id.clone(),
            contract_id: self.contract_id,
            market_type: self.market_type as i32,
            contract_name: self.contract_name.clone(),
            price: self.price,
            quantity: self.quantity,
            self_trade: self.self_trade,
            strategy_id: self.strategy_id.clone(),
            unit_id: self.unit_id.clone(),
            cl_order_id: self.cl_order_id.clone(),
        }
    }
}

/// An internal trade (a match between two of our own portfolios).
#[derive(Clone, Debug, Default, PartialEq)]
pub struct InternalTrade {
    pub internal_trade_id: String,
    pub contract_id: i64,
    pub market_type: MarketType,
    pub contract_name: String,
    pub price: i64,
    pub quantity: i64,
    pub buy_portfolio_id: String,
    pub sell_portfolio_id: String,
    pub buy_order_id: String,
    pub sell_order_id: String,
    pub exec_time_nanos: i64,
    pub api_timestamp_nanos: i64,
    pub cl_order_id: String,
    pub delivery_end_nanos: Option<i64>,
}

impl InternalTrade {
    /// Normalize this internal trade onto the own-trades shape — the shadow-mode
    /// merge (docs/design.md, "Shadow instances"): one ordinary [`Trade`] per leg
    /// whose portfolio is in `ours`, so shadow executions — our orders matching
    /// replicated market data, which a shadow PowerBot instance reports as
    /// *internal* trades — reach consumers as ordinary fills. `side` is the leg's
    /// side; the trade id is the internal id suffixed per leg (`-b` / `-s`) — one
    /// uniform rule, so a both-legs-ours match (two of our orders crossing in the
    /// shadow book) yields two offsetting fills instead of a special case.
    /// `self_trade` is deliberately `false`: PowerBot's flag marks a single record
    /// whose other leg is missing from the stream, whereas here every our-leg is
    /// emitted as its own complete trade (consumers skip self-trades; these must
    /// fold).
    ///
    /// The gateway's own identity fills what the internal-trade relay lacks: its
    /// configured `exchange` and `delivery_area_eic`. A trade without an id
    /// cannot be keyed and yields nothing (the raw relay's store drops it too).
    pub fn shadow_trades(
        &self,
        exchange: &str,
        delivery_area_eic: &str,
        ours: &[String],
    ) -> Vec<Trade> {
        if self.internal_trade_id.is_empty() {
            return Vec::new();
        }
        let legs = [
            (
                OrderSide::Buy,
                &self.buy_portfolio_id,
                &self.buy_order_id,
                "-b",
            ),
            (
                OrderSide::Sell,
                &self.sell_portfolio_id,
                &self.sell_order_id,
                "-s",
            ),
        ];
        legs.into_iter()
            .filter(|(_, portfolio_id, _, _)| ours.iter().any(|p| p == *portfolio_id))
            .map(|(side, portfolio_id, order_id, suffix)| Trade {
                trade_id: format!("{}{suffix}", self.internal_trade_id),
                state: TradeState::Acti,
                exchange: exchange.to_owned(),
                delivery_area_eic: delivery_area_eic.to_owned(),
                api_timestamp_nanos: self.api_timestamp_nanos,
                exec_time_nanos: self.exec_time_nanos,
                side,
                order_id: order_id.clone(),
                portfolio_id: portfolio_id.clone(),
                contract_id: self.contract_id,
                market_type: self.market_type,
                contract_name: self.contract_name.clone(),
                price: self.price,
                quantity: self.quantity,
                self_trade: false,
                strategy_id: String::new(),
                unit_id: String::new(),
                cl_order_id: self.cl_order_id.clone(),
                delivery_end_nanos: self.delivery_end_nanos,
            })
            .collect()
    }
}

/// An error attached to an order revision.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct OrderError {
    pub code: String,
    pub message: String,
}

/// One own-order revision.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct OwnOrder {
    pub order_id: String,
    pub revision_no: i64,
    pub state: OrderState,
    pub action: OrderAction,
    pub side: OrderSide,
    pub contract_id: i64,
    pub market_type: MarketType,
    pub delivery_area_eic: String,
    pub portfolio_id: String,
    pub price: i64,
    pub quantity: i64,
    pub cl_order_id: String,
    pub api_timestamp_nanos: i64,
    pub errors: Vec<OrderError>,
    pub delivery_end_nanos: Option<i64>,
}

impl OwnOrder {
    pub fn to_proto(&self) -> proto::OwnOrder {
        proto::OwnOrder {
            order_id: self.order_id.clone(),
            revision_no: self.revision_no,
            state: self.state as i32,
            action: self.action as i32,
            side: self.side as i32,
            contract_id: self.contract_id,
            market_type: self.market_type as i32,
            delivery_area_eic: self.delivery_area_eic.clone(),
            portfolio_id: self.portfolio_id.clone(),
            price: self.price,
            quantity: self.quantity,
            cl_order_id: self.cl_order_id.clone(),
            api_timestamp_nanos: self.api_timestamp_nanos,
            errors: self
                .errors
                .iter()
                .map(|e| proto::OrderError {
                    code: e.code.clone(),
                    message: e.message.clone(),
                })
                .collect(),
        }
    }
}

/// `key` non-empty helper: an item without an identity can't be keyed.
fn keyed(id: &str) -> Option<&str> {
    (!id.is_empty()).then_some(id)
}

/// Milliseconds a nanosecond timestamp represents, or `None` for the 0 sentinel
/// (absent). Used for the never-regress `version` at millisecond granularity, so
/// a feed (ns) and REST (µs) copy of the same instant never look like a regress.
fn millis(nanos: i64) -> Option<i64> {
    (nanos != 0).then_some(nanos / 1_000_000)
}

impl Seedable for Trade {
    fn key(&self) -> Option<&str> {
        keyed(&self.trade_id)
    }

    fn version(&self) -> Option<i64> {
        millis(self.api_timestamp_nanos)
    }

    fn delivery_end_millis(&self) -> Option<i64> {
        self.delivery_end_nanos.map(|n| n / 1_000_000)
    }

    fn unchanged(&self, other: &Self) -> bool {
        let norm = |t: &Trade| Trade {
            api_timestamp_nanos: floor_ms(t.api_timestamp_nanos),
            exec_time_nanos: floor_ms(t.exec_time_nanos),
            delivery_end_nanos: t.delivery_end_nanos.map(floor_ms),
            ..t.clone()
        };
        norm(self) == norm(other)
    }
}

impl Seedable for InternalTrade {
    fn key(&self) -> Option<&str> {
        keyed(&self.internal_trade_id)
    }

    fn version(&self) -> Option<i64> {
        millis(self.api_timestamp_nanos)
    }

    fn delivery_end_millis(&self) -> Option<i64> {
        self.delivery_end_nanos.map(|n| n / 1_000_000)
    }

    fn unchanged(&self, other: &Self) -> bool {
        let norm = |t: &InternalTrade| InternalTrade {
            api_timestamp_nanos: floor_ms(t.api_timestamp_nanos),
            exec_time_nanos: floor_ms(t.exec_time_nanos),
            delivery_end_nanos: t.delivery_end_nanos.map(floor_ms),
            ..t.clone()
        };
        norm(self) == norm(other)
    }
}

impl Seedable for OwnOrder {
    fn key(&self) -> Option<&str> {
        keyed(&self.order_id)
    }

    fn version(&self) -> Option<i64> {
        Some(self.revision_no)
    }

    fn delivery_end_millis(&self) -> Option<i64> {
        self.delivery_end_nanos.map(|n| n / 1_000_000)
    }

    fn unchanged(&self, other: &Self) -> bool {
        let norm = |o: &OwnOrder| OwnOrder {
            api_timestamp_nanos: floor_ms(o.api_timestamp_nanos),
            delivery_end_nanos: o.delivery_end_nanos.map(floor_ms),
            ..o.clone()
        };
        norm(self) == norm(other)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn internal(id: &str, buy_portfolio: &str, sell_portfolio: &str) -> InternalTrade {
        InternalTrade {
            internal_trade_id: id.to_owned(),
            contract_id: 12345,
            market_type: MarketType::Local,
            contract_name: "PH-20260713-14".to_owned(),
            price: 4210,
            quantity: 5000,
            buy_portfolio_id: buy_portfolio.to_owned(),
            sell_portfolio_id: sell_portfolio.to_owned(),
            buy_order_id: "BO-1".to_owned(),
            sell_order_id: "SO-1".to_owned(),
            exec_time_nanos: 77,
            api_timestamp_nanos: 99,
            cl_order_id: "tag".to_owned(),
            delivery_end_nanos: Some(1234),
        }
    }

    fn ours() -> Vec<String> {
        vec!["PB-A".to_owned(), "PB-B".to_owned()]
    }

    #[test]
    fn shadow_trades_emits_our_buy_leg_as_an_ordinary_trade() {
        let trades = internal("I-1", "PB-A", "REPLICA").shadow_trades(
            "nordpool",
            "10YDK-1--------W",
            &ours(),
        );
        assert_eq!(trades.len(), 1);
        let t = &trades[0];
        // The leg's identity, the gateway's identity, and the copied economics.
        assert_eq!(t.trade_id, "I-1-b");
        assert_eq!(t.side, OrderSide::Buy);
        assert_eq!(t.order_id, "BO-1");
        assert_eq!(t.portfolio_id, "PB-A");
        assert_eq!(t.exchange, "nordpool");
        assert_eq!(t.delivery_area_eic, "10YDK-1--------W");
        assert_eq!(t.state, TradeState::Acti);
        assert_eq!((t.price, t.quantity), (4210, 5000));
        assert_eq!((t.exec_time_nanos, t.api_timestamp_nanos), (77, 99));
        assert_eq!(t.contract_id, 12345);
        assert_eq!(t.market_type, MarketType::Local);
        assert_eq!(t.delivery_end_nanos, Some(1234));
        // Every our-leg is a complete record of its own — never the self-trade
        // form consumers skip.
        assert!(!t.self_trade);
    }

    #[test]
    fn shadow_trades_emits_our_sell_leg_with_the_sell_identifiers() {
        let trades = internal("I-2", "REPLICA", "PB-A").shadow_trades("nordpool", "AREA", &ours());
        assert_eq!(trades.len(), 1);
        assert_eq!(trades[0].trade_id, "I-2-s");
        assert_eq!(trades[0].side, OrderSide::Sell);
        assert_eq!(trades[0].order_id, "SO-1");
        assert_eq!(trades[0].portfolio_id, "PB-A");
    }

    #[test]
    fn shadow_trades_emits_both_legs_of_a_self_match_as_offsetting_fills() {
        // Both legs ours — even across two of our portfolios — yield one complete
        // trade each: portfolio-scoped consumers see exactly their own leg.
        let trades = internal("I-3", "PB-A", "PB-B").shadow_trades("nordpool", "AREA", &ours());
        assert_eq!(trades.len(), 2);
        assert_eq!(trades[0].trade_id, "I-3-b");
        assert_eq!(trades[0].portfolio_id, "PB-A");
        assert_eq!(trades[1].trade_id, "I-3-s");
        assert_eq!(trades[1].portfolio_id, "PB-B");
        assert!(trades.iter().all(|t| !t.self_trade));
    }

    #[test]
    fn shadow_trades_skips_replica_matches_and_unkeyed_trades() {
        // Neither leg ours (replica-vs-replica in the shadow book): not our execution.
        assert!(
            internal("I-4", "REPLICA", "OTHER")
                .shadow_trades("nordpool", "AREA", &ours())
                .is_empty()
        );
        // No id: cannot be keyed (a bare "-b" id would be a lie); the raw relay's
        // store drops the source item too.
        assert!(
            internal("", "PB-A", "REPLICA")
                .shadow_trades("nordpool", "AREA", &ours())
                .is_empty()
        );
    }
}
