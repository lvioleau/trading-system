//! Wire → domain conversion at the feed boundary.
//!
//! PowerBot's generated models arrive from two ingress routes — the websocket
//! feed ([`crate::sink`]) and the REST reconciliation sweep ([`crate::reconcile`])
//! — and both curate to the same [`crate::domain`] representation here, *before*
//! anything is applied to a store. Curating once, at the boundary, is what keeps
//! the store canonical: our side of the trade, prices/quantities as scaled
//! integers, contract ids split from PowerBot's prefix, timestamps as epoch nanos.
//!
//! The reverse (domain → proto, on serve) is trivial and lives on the domain
//! types (`to_proto`).

use powerbot_models::models;

use super::orders::{price_to_wire, quantity_to_wire, to_epoch_nanos};
use super::proto;
use crate::contracts::split_contract_id;
use crate::domain;
use crate::time::parse_ts;

/// Delivery/other timestamp → epoch nanos, or `None` when absent/unparseable
/// (unlike [`to_epoch_nanos`], which collapses absent to the 0 sentinel).
fn epoch_nanos_opt(ts: Option<String>) -> Option<i64> {
    parse_ts(ts.as_deref()).and_then(|dt| dt.timestamp_nanos_opt())
}

/// Curate a PowerBot `Trade` to our side: the order-level identifiers
/// (order_id, portfolio_id, strategy_id, unit_id, txt) are the leg that is ours.
/// A self-trade (both legs ours) collapses to the buy leg — tolerated because
/// IOC-only order entry should make one impossible, and ingest alarms on any.
pub(crate) fn trade_to_domain(t: models::Trade) -> domain::Trade {
    let bought = t.buy.unwrap_or(false);
    let side = if bought {
        proto::OrderSide::Buy
    } else if t.sell.unwrap_or(false) {
        proto::OrderSide::Sell
    } else {
        proto::OrderSide::None
    };
    let (order_id, portfolio_id, strategy_id, unit_id, txt) = if bought {
        (
            t.buy_order_id,
            t.buy_portfolio_id,
            t.buy_strategy_id,
            t.buy_unit_id,
            t.buy_txt,
        )
    } else {
        (
            t.sell_order_id,
            t.sell_portfolio_id,
            t.sell_strategy_id,
            t.sell_unit_id,
            t.sell_txt,
        )
    };
    let (market_type, contract_id) = split_contract_id(t.contract_id);
    domain::Trade {
        trade_id: t.trade_id.unwrap_or_default(),
        state: to_proto_trade_state(t.state),
        exchange: t.exchange.unwrap_or_default(),
        delivery_area_eic: t.delivery_area.unwrap_or_default(),
        api_timestamp_nanos: to_epoch_nanos(t.api_timestamp),
        exec_time_nanos: to_epoch_nanos(t.exec_time),
        side,
        order_id: order_id.unwrap_or_default(),
        portfolio_id: portfolio_id.unwrap_or_default(),
        contract_id,
        market_type,
        contract_name: t.contract_name.unwrap_or_default(),
        price: price_to_wire(t.price),
        quantity: quantity_to_wire(t.quantity),
        self_trade: t.self_trade.unwrap_or_default(),
        strategy_id: strategy_id.unwrap_or_default(),
        unit_id: unit_id.unwrap_or_default(),
        cl_order_id: txt.unwrap_or_default(),
        delivery_end_nanos: epoch_nanos_opt(t.delivery_end),
    }
}

pub(crate) fn internal_trade_to_domain(t: models::InternalTrade) -> domain::InternalTrade {
    let (market_type, contract_id) = split_contract_id(t.contract_id);
    // The label of whichever leg is ours: with a shadow portfolio one leg is
    // populated and the other empty, so prefer the buy leg's txt when it has an
    // order id, else the sell leg's.
    let txt = if t.buy_order_id.is_some() {
        t.buy_txt
    } else {
        t.sell_txt
    };
    domain::InternalTrade {
        internal_trade_id: t.internal_trade_id.unwrap_or_default(),
        contract_id,
        market_type,
        contract_name: t.contract_name.unwrap_or_default(),
        price: price_to_wire(t.price),
        quantity: quantity_to_wire(t.quantity),
        buy_portfolio_id: t.buy_portfolio_id.unwrap_or_default(),
        sell_portfolio_id: t.sell_portfolio_id.unwrap_or_default(),
        buy_order_id: t.buy_order_id.unwrap_or_default(),
        sell_order_id: t.sell_order_id.unwrap_or_default(),
        exec_time_nanos: to_epoch_nanos(t.exec_time),
        api_timestamp_nanos: to_epoch_nanos(t.api_timestamp),
        cl_order_id: txt.unwrap_or_default(),
        delivery_end_nanos: epoch_nanos_opt(t.delivery_end),
    }
}

pub(crate) fn own_order_to_domain(o: models::OwnOrder) -> domain::OwnOrder {
    let (market_type, contract_id) = split_contract_id(o.contract_id);
    domain::OwnOrder {
        order_id: o.order_id.unwrap_or_default(),
        revision_no: o.revision_no.unwrap_or_default(),
        state: to_proto_state(o.state),
        action: to_proto_action(o.action),
        side: to_proto_side(o.side),
        contract_id,
        market_type,
        delivery_area_eic: o.delivery_area,
        portfolio_id: o.portfolio_id.unwrap_or_default(),
        price: price_to_wire(o.price),
        quantity: quantity_to_wire(o.quantity),
        cl_order_id: o.txt.unwrap_or_default(),
        api_timestamp_nanos: to_epoch_nanos(o.api_timestamp),
        errors: o
            .errors
            .unwrap_or_default()
            .into_iter()
            .map(|e| domain::OrderError {
                code: e.code.map(|c| c.to_string()).unwrap_or_default(),
                message: e.message,
            })
            .collect(),
        delivery_end_nanos: epoch_nanos_opt(o.delivery_end),
    }
}

/// Map PowerBot's `TradeState` onto our proto enum; `UNSPECIFIED` when absent.
fn to_proto_trade_state(state: Option<models::TradeState>) -> proto::TradeState {
    use models::TradeState as M;
    use proto::TradeState as P;
    match state {
        None => P::None,
        Some(M::Cncl) => P::Cncl,
        Some(M::Rrej) => P::Rrej,
        Some(M::Rgra) => P::Rgra,
        Some(M::Rreq) => P::Rreq,
        Some(M::Acti) => P::Acti,
        Some(M::Creq) => P::Creq,
        Some(M::Crej) => P::Crej,
        Some(M::Rsfa) => P::Rsfa,
    }
}

fn to_proto_side(side: models::OrderSide) -> proto::OrderSide {
    match side {
        models::OrderSide::Buy => proto::OrderSide::Buy,
        models::OrderSide::Sell => proto::OrderSide::Sell,
    }
}

/// Map PowerBot's `OrderState` onto our proto enum. Total over the model's
/// closed set, so the proto's `UNSPECIFIED` zero value is never produced here.
fn to_proto_state(state: models::OrderState) -> proto::OrderState {
    use models::OrderState as M;
    use proto::OrderState as P;
    match state {
        M::Acti => P::Acti,
        M::Hibe => P::Hibe,
        M::Iact => P::Iact,
        M::Unkw => P::Unkw,
    }
}

/// Map PowerBot's `OrderAction` onto our proto enum. Total over the model's
/// closed set — a code the model can't parse fails earlier, at JSON decode, so
/// the proto's `UNSPECIFIED` zero value is never produced here.
fn to_proto_action(action: models::OrderAction) -> proto::OrderAction {
    use models::OrderAction as M;
    use proto::OrderAction as P;
    match action {
        M::Uadd => P::Uadd,
        M::Uhib => P::Uhib,
        M::Umod => P::Umod,
        M::Udel => P::Udel,
        M::Urej => P::Urej,
        M::Aadd => P::Aadd,
        M::Ahib => P::Ahib,
        M::Amod => P::Amod,
        M::Adel => P::Adel,
        M::Arej => P::Arej,
        M::Sadd => P::Sadd,
        M::Shib => P::Shib,
        M::Smod => P::Smod,
        M::Sdel => P::Sdel,
        M::Srej => P::Srej,
        M::Fexe => P::Fexe,
        M::Pexe => P::Pexe,
        M::Iadd => P::Iadd,
        M::Serr => P::Serr,
        M::Snav => P::Snav,
        M::Qadd => P::Qadd,
        M::Afex => P::Afex,
        M::Qpex => P::Qpex,
        M::Rlvi => P::Rlvi,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::grpc::proto::MarketType;

    #[test]
    fn a_buy_trade_curates_to_the_buy_leg() {
        let trade = models::Trade {
            trade_id: Some("T-1".to_owned()),
            state: Some(models::TradeState::Acti),
            buy: Some(true),
            buy_order_id: Some("O-buy".to_owned()),
            buy_portfolio_id: Some("pf-1".to_owned()),
            buy_txt: Some("cl-buy".to_owned()),
            // The sell leg is the counterparty's — it must not leak into our record.
            sell_order_id: Some("O-sell".to_owned()),
            sell_portfolio_id: Some("pf-them".to_owned()),
            sell_txt: Some("cl-sell".to_owned()),
            contract_id: Some("NX_12345".to_owned()),
            contract_name: Some("PH-2026".to_owned()),
            price: Some(12.5),
            quantity: Some(2.0),
            self_trade: Some(false),
            exec_time: Some("2026-01-01T10:00:00Z".to_owned()),
            api_timestamp: Some("2026-01-01T10:00:01Z".to_owned()),
            delivery_end: Some("2026-01-01T11:00:00Z".to_owned()),
            ..Default::default()
        };

        let domain = trade_to_domain(trade);
        assert_eq!(domain.side, proto::OrderSide::Buy);
        assert_eq!(domain.order_id, "O-buy");
        assert_eq!(domain.portfolio_id, "pf-1");
        assert_eq!(domain.cl_order_id, "cl-buy");
        assert_eq!(domain.state, proto::TradeState::Acti);
        assert_eq!(
            (domain.market_type, domain.contract_id),
            (MarketType::Xbid, 12345)
        );
        assert_eq!(domain.price, 1_250, "scaled to minor units per MWh (×100)");
        assert_eq!(domain.quantity, 2_000, "scaled to kW (×1000)");
        assert!(domain.delivery_end_nanos.is_some());
    }

    #[test]
    fn a_sell_trade_curates_to_the_sell_leg() {
        let trade = models::Trade {
            sell: Some(true),
            sell_order_id: Some("O-sell".to_owned()),
            sell_portfolio_id: Some("pf-2".to_owned()),
            sell_txt: Some("cl-sell".to_owned()),
            contract_id: Some("NI_777".to_owned()),
            ..Default::default()
        };

        let domain = trade_to_domain(trade);
        assert_eq!(domain.side, proto::OrderSide::Sell);
        assert_eq!(domain.order_id, "O-sell");
        assert_eq!(domain.portfolio_id, "pf-2");
        assert_eq!(
            (domain.market_type, domain.contract_id),
            (MarketType::Local, 777)
        );
    }

    #[test]
    fn a_trade_that_is_neither_buy_nor_sell_has_no_side_and_takes_the_sell_leg() {
        // Neither flag set: side collapses to None, and the else arm reads the sell leg.
        let trade = models::Trade {
            sell_order_id: Some("O-x".to_owned()),
            // An unparseable / prefixless contract id degrades to (None, 0), not a panic.
            contract_id: Some("garbage".to_owned()),
            ..Default::default()
        };

        let domain = trade_to_domain(trade);
        assert_eq!(domain.side, proto::OrderSide::None);
        assert_eq!(domain.order_id, "O-x");
        assert_eq!(
            (domain.market_type, domain.contract_id),
            (MarketType::None, 0)
        );
        assert_eq!(domain.trade_id, "", "an absent id defaults, never panics");
        assert_eq!(domain.delivery_end_nanos, None);
    }

    #[test]
    fn an_internal_trade_takes_the_buy_leg_label_when_it_has_a_buy_order() {
        let with_buy = models::InternalTrade {
            internal_trade_id: Some("IT-1".to_owned()),
            buy_order_id: Some("O-buy".to_owned()),
            buy_txt: Some("cl-buy".to_owned()),
            sell_txt: Some("cl-sell".to_owned()),
            contract_id: Some("NX_9".to_owned()),
            ..Default::default()
        };
        assert_eq!(internal_trade_to_domain(with_buy).cl_order_id, "cl-buy");

        // No buy order id ⇒ the sell leg's txt is the label of the leg that is ours.
        let without_buy = models::InternalTrade {
            sell_txt: Some("cl-sell".to_owned()),
            ..Default::default()
        };
        assert_eq!(internal_trade_to_domain(without_buy).cl_order_id, "cl-sell");
    }

    #[test]
    fn an_own_order_maps_its_enums_and_flattens_its_errors() {
        let order = models::OwnOrder {
            order_id: Some("O-1".to_owned()),
            revision_no: Some(3),
            state: models::OrderState::Iact,
            action: models::OrderAction::Fexe,
            side: models::OrderSide::Sell,
            contract_id: Some("NI_42".to_owned()),
            portfolio_id: Some("pf-1".to_owned()),
            price: Some(1.0),
            quantity: Some(1.5),
            txt: Some("cl-1".to_owned()),
            errors: Some(vec![
                models::Error {
                    code: Some(models::ErrorCode::PbInternalError),
                    message: "boom".to_owned(),
                    variables: None,
                },
                // A code-less error keeps its message, code defaults to empty.
                models::Error {
                    code: None,
                    message: "no code".to_owned(),
                    variables: None,
                },
            ]),
            ..Default::default()
        };

        let domain = own_order_to_domain(order);
        assert_eq!(domain.order_id, "O-1");
        assert_eq!(domain.revision_no, 3);
        assert_eq!(domain.state, proto::OrderState::Iact);
        assert_eq!(domain.action, proto::OrderAction::Fexe);
        assert_eq!(domain.side, proto::OrderSide::Sell);
        assert_eq!(
            (domain.market_type, domain.contract_id),
            (MarketType::Local, 42)
        );
        assert_eq!(domain.quantity, 1_500);
        assert_eq!(domain.errors.len(), 2);
        assert!(
            !domain.errors[0].code.is_empty(),
            "a coded error carries its code"
        );
        assert_eq!(domain.errors[0].message, "boom");
        assert_eq!(
            domain.errors[1].code, "",
            "a code-less error defaults to empty"
        );
    }

    #[test]
    fn trade_state_maps_none_to_none_and_every_present_state_off_the_zero_value() {
        assert_eq!(to_proto_trade_state(None), proto::TradeState::None);
        use models::TradeState::*;
        for state in [Cncl, Rrej, Rgra, Rreq, Acti, Creq, Crej, Rsfa] {
            assert_ne!(
                to_proto_trade_state(Some(state)) as i32,
                proto::TradeState::None as i32,
                "a present {state:?} must not collapse to the UNSPECIFIED/None zero value"
            );
        }
    }

    #[test]
    fn order_state_and_side_never_produce_the_unspecified_zero_value() {
        use models::OrderState::*;
        for state in [Acti, Hibe, Iact, Unkw] {
            assert_ne!(
                to_proto_state(state) as i32,
                0,
                "{state:?} mapped to UNSPECIFIED"
            );
        }
        assert_ne!(to_proto_side(models::OrderSide::Buy) as i32, 0);
        assert_ne!(to_proto_side(models::OrderSide::Sell) as i32, 0);
    }

    #[test]
    fn every_order_action_maps_off_the_unspecified_zero_value() {
        use models::OrderAction::*;
        // The whole closed set — the mapper is total, so the proto UNSPECIFIED (0) is
        // never produced (see the fn doc). Iterating exercises every arm.
        let all = [
            Uadd, Uhib, Umod, Udel, Urej, Aadd, Ahib, Amod, Adel, Arej, Sadd, Shib, Smod, Sdel,
            Srej, Fexe, Pexe, Iadd, Serr, Snav, Qadd, Afex, Qpex, Rlvi,
        ];
        for action in all {
            assert_ne!(
                to_proto_action(action) as i32,
                0,
                "{action:?} mapped to UNSPECIFIED"
            );
        }
    }
}
