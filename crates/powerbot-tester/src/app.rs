//! UI state and input handling. The [`App`] owns the merged view the background
//! tasks feed (contracts, depth, order/trade streams, connection status) plus the
//! order-ticket editor, and it turns key presses into state changes and outgoing
//! [`OrderCommand`]s.

use std::collections::{BTreeMap, HashMap, VecDeque};

use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
use tokio::sync::mpsc::UnboundedSender;

use crate::config::Config;
use crate::grpc::{AppEvent, Conn, ConnState, OrderCommand};
use crate::proto::gateway::{OrderAction, OrderSide, OrderState, OwnOrder, Trade};
use crate::proto::marketdata::{Contract, ContractState, ProductType};
use crate::proto::position::Position;
// Both packages define a `MarketType`; keep them module-qualified
// (`gateway::MarketType` vs `marketdata::MarketType`) at the use sites below.
use crate::proto::{gateway, marketdata};

/// Price scale: both the connector depth feed and the gateway carry prices as
/// integer minor currency units per MWh (currency/MWh ×100). Divide for display
/// as a decimal price, multiply a hand-typed decimal back to send.
pub const PRICE_SCALE: f64 = 100.0;

/// Quantity scale: the gateway carries quantities as integer kilowatts (MW
/// ×1000). Divide for display in MW, multiply a hand-typed MW quantity to send.
pub const QTY_SCALE: f64 = 1000.0;

/// Positions below this magnitude (MW) count as flat: quantities are integer kW,
/// so anything under half a kW is a rounding artifact, not a live position.
const FLAT_EPS_MW: f64 = 0.0005;

/// How many rows the own-order and own-trade tables retain (newest kept).
const MAX_STREAM_ROWS: usize = 500;

/// Best bid/ask for one contract, distilled from its depth book.
#[derive(Default, Clone, Copy)]
pub struct DepthTop {
    /// (price_raw, size_raw) of the best bid, if any resting.
    pub bid: Option<(i64, i64)>,
    pub ask: Option<(i64, i64)>,
}

/// Which top-level pane has focus.
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum Pane {
    Contracts,
    Ticket,
}

/// Which order-ticket field is focused.
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum TicketField {
    Side,
    AutoPrice,
    Price,
    Qty,
    Send,
}

impl TicketField {
    fn next(self) -> Self {
        match self {
            TicketField::Side => TicketField::AutoPrice,
            TicketField::AutoPrice => TicketField::Price,
            TicketField::Price => TicketField::Qty,
            TicketField::Qty => TicketField::Send,
            TicketField::Send => TicketField::Side,
        }
    }
    fn prev(self) -> Self {
        match self {
            TicketField::Side => TicketField::Send,
            TicketField::AutoPrice => TicketField::Side,
            TicketField::Price => TicketField::AutoPrice,
            TicketField::Qty => TicketField::Price,
            TicketField::Send => TicketField::Qty,
        }
    }
}

/// Contract filter on market type.
#[derive(Clone, Copy)]
pub enum MarketFilter {
    Any,
    Local,
    Xbid,
}

impl MarketFilter {
    fn cycle(self) -> Self {
        match self {
            MarketFilter::Any => MarketFilter::Local,
            MarketFilter::Local => MarketFilter::Xbid,
            MarketFilter::Xbid => MarketFilter::Any,
        }
    }
    pub fn label(self) -> &'static str {
        match self {
            MarketFilter::Any => "Any",
            MarketFilter::Local => "LOCAL",
            MarketFilter::Xbid => "XBID",
        }
    }
    fn matches(self, market_type: i32) -> bool {
        match self {
            MarketFilter::Any => true,
            MarketFilter::Local => market_type == marketdata::MarketType::Local as i32,
            MarketFilter::Xbid => market_type == marketdata::MarketType::Xbid as i32,
        }
    }
}

/// Contract filter on product type.
#[derive(Clone, Copy)]
pub enum ProductFilter {
    Any,
    P15,
    P30,
    P60,
    Other,
}

impl ProductFilter {
    fn cycle(self) -> Self {
        match self {
            ProductFilter::Any => ProductFilter::P15,
            ProductFilter::P15 => ProductFilter::P30,
            ProductFilter::P30 => ProductFilter::P60,
            ProductFilter::P60 => ProductFilter::Other,
            ProductFilter::Other => ProductFilter::Any,
        }
    }
    pub fn label(self) -> &'static str {
        match self {
            ProductFilter::Any => "Any",
            ProductFilter::P15 => "P15",
            ProductFilter::P30 => "P30",
            ProductFilter::P60 => "P60",
            ProductFilter::Other => "Other",
        }
    }
    fn matches(self, product_type: i32) -> bool {
        match self {
            ProductFilter::Any => true,
            ProductFilter::P15 => product_type == ProductType::P15Min as i32,
            ProductFilter::P30 => product_type == ProductType::P30Min as i32,
            ProductFilter::P60 => product_type == ProductType::P60Min as i32,
            ProductFilter::Other => product_type == ProductType::Other as i32,
        }
    }
}

/// The three timestamps a stream row carries, plus the round-trip if we sent it.
/// `exec` is the exchange/PowerBot event time, `gw_recv` the gateway's websocket
/// receive stamp, `ui_recv` when this tester received the gRPC frame; comparing
/// them localizes latency (exch→gateway→ui), and `rtt` (ui_recv − order send) is
/// the full round trip for orders/trades we originated.
pub struct RowTimes {
    /// When this tester sent the order behind the row (matched by `txt`); "-" if
    /// we didn't originate it. `rtt` is `ui_recv − sent`.
    pub sent: String,
    pub exec: String,
    pub gw_recv: String,
    pub ui_recv: String,
    pub rtt: String,
    /// Sort key (unix-nanos): the feed is a keyed-upsert set with no ordering
    /// guarantee (a seed arrives in the store's HashMap order, deltas by arrival),
    /// so rows are sorted here to read chronologically.
    pub sort_nanos: i64,
}

/// One rendered row of the own-order stream.
pub struct OrderRow {
    pub times: RowTimes,
    /// Free-text tag set on the order (e.g. our `tester-N`).
    pub txt: String,
    pub order_id: String,
    pub side: String,
    pub state: String,
    pub action: String,
    pub price: f64,
    pub quantity: f64,
    /// Market type label (XBID/LOCAL) of the order's contract.
    pub market: String,
    /// Bare nordpool contract id.
    pub contract_id: i64,
    pub error: String,
}

/// One rendered row of the own-trade stream.
pub struct TradeRow {
    pub times: RowTimes,
    /// Free-text tag echoed from the order that produced our leg.
    pub txt: String,
    pub trade_id: String,
    pub side: String,
    pub price: f64,
    pub quantity: f64,
    /// Market type label (XBID/LOCAL) of the traded contract.
    pub market: String,
    /// Bare nordpool contract id.
    pub contract_id: i64,
    pub contract: String,
    pub portfolio: String,
    pub order_id: String,
}

pub struct App {
    pub cfg: Config,

    pub gateway_status: ConnState,
    pub connector_status: ConnState,
    pub position_status: ConnState,

    /// All contracts seen, keyed by id (upsert-merged, never removed).
    pub contracts: BTreeMap<i64, Contract>,
    /// Best bid/ask per contract id.
    pub depth: HashMap<i64, DepthTop>,

    pub filter_market: MarketFilter,
    pub filter_product: ProductFilter,
    pub active_only: bool,

    /// Contract ids passing the filters, sorted by delivery start then name.
    pub visible: Vec<i64>,
    /// Currently selected contract id (survives list churn when still visible).
    pub selected: Option<i64>,

    pub pane: Pane,
    pub field: TicketField,
    pub side: i32,
    /// When set, the price field tracks the book's best bid/ask live and is not
    /// hand-editable.
    pub auto_price: bool,
    pub price_input: String,
    pub qty_input: String,
    pub last_order_result: Option<Result<OwnOrder, String>>,
    pub status: String,
    order_seq: u64,
    /// Send instant (unix-nanos) of each order we've fired, keyed by its `txt`
    /// tag, so a resulting order/trade frame can report the full round trip.
    pending_sends: HashMap<String, i64>,

    pub orders: VecDeque<OrderRow>,
    pub trades: VecDeque<TradeRow>,

    /// Per-(portfolio, contract) Position Service state, keyed by (portfolio
    /// index into [`Config::position_portfolios`], market type, contract id).
    /// Holds the net quantity (kW) and its revision, so a snapshot/stream overlap
    /// or a re-seed on reconnect resolves by keeping the highest revision.
    position_entries: HashMap<(usize, i32, i64), PosEntry>,
    /// Net position (kW) aggregated per (portfolio index, contract id) for O(1)
    /// column lookup — the running fold of [`Self::position_entries`], summed
    /// across a contract's market types (the contract list keys on bare id).
    net_by_contract: HashMap<(usize, i64), i64>,

    pub should_quit: bool,
}

/// One (portfolio, contract, market-type) position as last seen from the service.
struct PosEntry {
    /// Net quantity in kW, signed (long > 0, short < 0).
    net_kw: i64,
    /// Monotonic per identity; a lower revision is a stale duplicate, ignored.
    revision: u64,
}

impl App {
    pub fn new(cfg: Config) -> Self {
        Self {
            cfg,
            gateway_status: ConnState::Connecting,
            connector_status: ConnState::Connecting,
            position_status: ConnState::Connecting,
            contracts: BTreeMap::new(),
            depth: HashMap::new(),
            filter_market: MarketFilter::Any,
            filter_product: ProductFilter::Any,
            active_only: true,
            visible: Vec::new(),
            selected: None,
            pane: Pane::Contracts,
            field: TicketField::Side,
            side: OrderSide::Buy as i32,
            auto_price: true,
            price_input: String::new(),
            qty_input: "1".to_string(),
            last_order_result: None,
            status: "Tab: switch pane · ↑/↓: move · Enter: select/send · Esc: quit".to_string(),
            order_seq: 0,
            pending_sends: HashMap::new(),
            orders: VecDeque::new(),
            trades: VecDeque::new(),
            position_entries: HashMap::new(),
            net_by_contract: HashMap::new(),
            should_quit: false,
        }
    }

    // ---- background events -------------------------------------------------

    pub fn on_event(&mut self, event: AppEvent) {
        match event {
            AppEvent::Contracts(list) => {
                for c in list {
                    self.contracts.insert(c.contract_id, c);
                }
                self.recompute_visible();
            }
            AppEvent::Depth(views) => {
                for v in views {
                    let top = DepthTop {
                        bid: v.bids.first().map(|l| (l.price, l.total_quantity)),
                        ask: v.asks.first().map(|l| (l.price, l.total_quantity)),
                    };
                    self.depth.insert(v.contract_id, top);
                }
                // Keep the ticket price on the book when auto-price is armed.
                if self.auto_price {
                    self.prefill_price();
                }
            }
            AppEvent::Orders {
                received_at_nanos,
                ui_recv_nanos,
                orders,
            } => {
                for o in orders {
                    // The order's own last exchange-update time, not the (possibly
                    // seed/sweep) batch time.
                    let exec = first_time(&[o.api_timestamp_nanos]);
                    self.push_order_row(exec, received_at_nanos, ui_recv_nanos, o);
                }
            }
            AppEvent::Trades {
                received_at_nanos,
                ui_recv_nanos,
                trades,
            } => {
                for t in trades {
                    let exec = first_time(&[t.exec_time_nanos, t.api_timestamp_nanos]);
                    self.push_trade_row(exec, received_at_nanos, ui_recv_nanos, t);
                }
            }
            AppEvent::Positions {
                portfolio,
                positions,
            } => {
                let Some(idx) = self
                    .cfg
                    .position_portfolios
                    .iter()
                    .position(|p| *p == portfolio)
                else {
                    return;
                };
                for p in positions {
                    self.apply_position(idx, &p);
                }
            }
            AppEvent::OrderResult(result) => {
                self.status = match &result {
                    Ok(o) => format!("Order accepted: id={} state={}", o.order_id, o.state),
                    Err(e) => format!("Order failed: {e}"),
                };
                self.last_order_result = Some(result);
            }
            AppEvent::Status(conn, state) => match conn {
                Conn::Gateway => self.gateway_status = state,
                Conn::Connector => self.connector_status = state,
                Conn::Position => self.position_status = state,
            },
        }
    }

    fn push_order_row(&mut self, exec: i64, gw_recv: i64, ui_recv: i64, o: OwnOrder) {
        let error = o
            .errors
            .iter()
            .map(|e| format!("{}:{}", e.code, e.message))
            .collect::<Vec<_>>()
            .join("; ");
        let row = OrderRow {
            times: self.row_times(exec, gw_recv, ui_recv, &o.cl_order_id),
            txt: o.cl_order_id,
            order_id: o.order_id,
            side: side_label(o.side),
            state: state_label(o.state),
            action: action_label(o.action),
            price: o.price as f64 / PRICE_SCALE,
            quantity: o.quantity as f64 / QTY_SCALE,
            market: gw_market_label(o.market_type),
            contract_id: o.contract_id,
            error,
        };
        // Upsert by order id, like trades: a stream reconnect re-seeds the full
        // order state, and the gateway store keeps only the latest revision per
        // order, so replacing the prior row keeps one row per order at its
        // newest revision rather than stacking a duplicate on every re-seed.
        let replaced = if row.order_id.is_empty() {
            false
        } else {
            let before = self.orders.len();
            self.orders.retain(|r| r.order_id != row.order_id);
            before != self.orders.len()
        };
        tracing::info!(
            order_id = %row.order_id,
            side = %row.side,
            state = %row.state,
            action = %row.action,
            gw_recv = %row.times.gw_recv,
            ui_recv = %row.times.ui_recv,
            replaced,
            "order received"
        );
        self.orders.push_front(row);
        sort_desc_by_time(&mut self.orders, |r| r.times.sort_nanos);
        self.orders.truncate(MAX_STREAM_ROWS);
    }

    fn push_trade_row(&mut self, exec: i64, gw_recv: i64, ui_recv: i64, t: Trade) {
        let row = TradeRow {
            times: self.row_times(exec, gw_recv, ui_recv, &t.cl_order_id),
            txt: t.cl_order_id,
            trade_id: t.trade_id,
            side: side_label(t.side),
            price: t.price as f64 / PRICE_SCALE,
            quantity: t.quantity as f64 / QTY_SCALE,
            market: gw_market_label(t.market_type),
            contract_id: t.contract_id,
            contract: t.contract_name,
            portfolio: t.portfolio_id,
            order_id: t.order_id,
        };
        self.push_trade(row, "external");
    }

    /// Build a row's timestamp set: format the three stamps, sort by the earliest
    /// available (exec, else gateway-receive, else ui-receive), and — if `txt` is
    /// one of our sends — the round trip from that send to this ui receipt.
    fn row_times(&self, exec: i64, gw_recv: i64, ui_recv: i64, txt: &str) -> RowTimes {
        let sent = self
            .pending_sends
            .get(txt)
            .filter(|_| !txt.is_empty())
            .copied();
        let rtt = sent.map(|sent| ui_recv - sent);
        RowTimes {
            sent: sent.map(fmt_time).unwrap_or_else(|| "-".to_string()),
            exec: fmt_time(exec),
            gw_recv: fmt_time(gw_recv),
            ui_recv: fmt_time(ui_recv),
            rtt: fmt_rtt(rtt),
            sort_nanos: first_time(&[exec, gw_recv, ui_recv]),
        }
    }

    /// Add an own-trade row and keep the table newest-first by event time —
    /// arrival order isn't event-time order.
    ///
    /// The feed is a keyed-upsert set, not an append log: a stream reconnect
    /// re-seeds the full current state, re-delivering trades already shown (with
    /// a fresh gateway-receive stamp from the seed). So upsert by trade id — drop
    /// any prior row for this id before pushing the fresh one — or a reconnect
    /// leaves the same trade on two rows. Keyless rows (no trade id) can't be
    /// reconciled, so they're kept as-is.
    fn push_trade(&mut self, row: TradeRow, source: &str) {
        let replaced = if row.trade_id.is_empty() {
            false
        } else {
            let before = self.trades.len();
            self.trades.retain(|r| r.trade_id != row.trade_id);
            before != self.trades.len()
        };
        tracing::info!(
            source,
            trade_id = %row.trade_id,
            side = %row.side,
            qty = row.quantity,
            price = row.price,
            contract_id = row.contract_id,
            gw_recv = %row.times.gw_recv,
            ui_recv = %row.times.ui_recv,
            replaced,
            "trade received"
        );
        self.trades.push_front(row);
        sort_desc_by_time(&mut self.trades, |r| r.times.sort_nanos);
        self.trades.truncate(MAX_STREAM_ROWS);
    }

    // ---- positions ---------------------------------------------------------

    /// Fold one Position Service value into `portfolio_idx`'s state. Each Position
    /// is the full current value for its (portfolio, market type, contract);
    /// revisions are monotonic per identity, so a stale duplicate (the
    /// snapshot/stream overlap, or a re-seed on reconnect) is ignored, and a fresh
    /// value updates the per-contract net by its delta.
    fn apply_position(&mut self, portfolio_idx: usize, p: &Position) {
        let key = (portfolio_idx, p.market_type, p.contract_id);
        let prev_net = match self.position_entries.get(&key) {
            Some(e) if e.revision >= p.revision => return, // stale/duplicate
            Some(e) => e.net_kw,
            None => 0,
        };
        let agg = self
            .net_by_contract
            .entry((portfolio_idx, p.contract_id))
            .or_insert(0);
        *agg += p.net_quantity - prev_net;
        self.position_entries.insert(
            key,
            PosEntry {
                net_kw: p.net_quantity,
                revision: p.revision,
            },
        );
    }

    /// Net position (MW) held on `contract_id` by the tracked portfolio at
    /// `portfolio_idx`, summed across the contract's market types; 0.0 when none.
    pub fn position_for(&self, contract_id: i64, portfolio_idx: usize) -> f64 {
        self.net_by_contract
            .get(&(portfolio_idx, contract_id))
            .copied()
            .unwrap_or(0) as f64
            / QTY_SCALE
    }

    // ---- filtering / selection --------------------------------------------

    fn recompute_visible(&mut self) {
        let mut ids: Vec<i64> = self
            .contracts
            .values()
            .filter(|c| self.matches(c))
            .map(|c| c.contract_id)
            .collect();
        ids.sort_by(|a, b| {
            let ca = &self.contracts[a];
            let cb = &self.contracts[b];
            ca.delivery_start_ms
                .cmp(&cb.delivery_start_ms)
                .then_with(|| ca.name.cmp(&cb.name))
        });
        self.visible = ids;

        // Keep the selection if it is still visible, else fall back to the first.
        if let Some(sel) = self.selected {
            if !self.visible.contains(&sel) {
                self.selected = self.visible.first().copied();
            }
        } else {
            self.selected = self.visible.first().copied();
        }
    }

    fn matches(&self, c: &Contract) -> bool {
        self.filter_market.matches(c.market_type)
            && self.filter_product.matches(c.product_type)
            && (!self.active_only || self.is_active(c))
    }

    /// A contract is tradable here if it is ACTIVE in our configured area.
    fn is_active(&self, c: &Contract) -> bool {
        c.delivery_areas.iter().any(|a| {
            a.eic_code == self.cfg.delivery_area_eic.eic_code()
                && a.state == ContractState::Active as i32
        })
    }

    /// Index of the selected contract within `visible`, for the table cursor.
    pub fn selected_index(&self) -> Option<usize> {
        let sel = self.selected?;
        self.visible.iter().position(|&id| id == sel)
    }

    pub fn selected_contract(&self) -> Option<&Contract> {
        self.contracts.get(&self.selected?)
    }

    pub fn depth_for(&self, contract_id: i64) -> DepthTop {
        self.depth.get(&contract_id).copied().unwrap_or_default()
    }

    /// Contract display name for a bare nordpool id, resolved from the connector
    /// reference data. Empty when the contract isn't (yet) known — the gateway's
    /// `OwnOrder` carries only the id, so the name is a local lookup.
    pub fn contract_name(&self, contract_id: i64) -> &str {
        self.contracts
            .get(&contract_id)
            .map(|c| c.name.as_str())
            .unwrap_or("")
    }

    fn move_selection(&mut self, delta: isize) {
        if self.visible.is_empty() {
            return;
        }
        let cur = self.selected_index().unwrap_or(0) as isize;
        let next = (cur + delta).clamp(0, self.visible.len() as isize - 1) as usize;
        self.selected = Some(self.visible[next]);
        // Arm the ticket on the new contract to flatten its execution-portfolio
        // position: reducing side, outstanding quantity, price on the book.
        self.sync_ticket_to_position();
    }

    /// Index of the single execution portfolio (`cfg.portfolio_id`) within the
    /// tracked `position_portfolios`, so its net position can be read for the
    /// flatten helpers. `None` when the execution portfolio isn't tracked.
    pub fn exec_portfolio_idx(&self) -> Option<usize> {
        self.cfg
            .position_portfolios
            .iter()
            .position(|p| *p == self.cfg.portfolio_id)
    }

    /// Point the ticket at flattening the execution portfolio's net position on
    /// the selected contract: the side that reduces it and the outstanding
    /// quantity (MW). Leaves side/qty untouched when flat or when the execution
    /// portfolio isn't tracked; always refreshes the price from the book.
    fn sync_ticket_to_position(&mut self) {
        if let (Some(idx), Some(id)) = (self.exec_portfolio_idx(), self.selected) {
            let net = self.position_for(id, idx);
            if net.abs() >= FLAT_EPS_MW {
                self.side = reduce_side(net);
                self.qty_input = format!("{}", net.abs());
            }
        }
        self.prefill_price();
    }

    /// Flatten the execution portfolio's net position on the selected contract:
    /// pick the reducing side, size the ticket to the outstanding quantity, cross
    /// the book, and fire it as an ordinary IOC order.
    fn flatten_selected(&mut self, tx: &UnboundedSender<OrderCommand>) {
        let Some(idx) = self.exec_portfolio_idx() else {
            self.status = format!(
                "Execution portfolio {:?} not tracked — can't flatten",
                self.cfg.portfolio_id
            );
            return;
        };
        let Some(id) = self.selected else {
            self.status = "No contract selected".to_string();
            return;
        };
        let net = self.position_for(id, idx);
        if net.abs() < FLAT_EPS_MW {
            self.status = "Position already flat".to_string();
            return;
        }
        self.side = reduce_side(net);
        self.qty_input = format!("{}", net.abs());
        self.prefill_price();
        self.send_order(tx);
    }

    // ---- order ticket ------------------------------------------------------

    /// Prefill the price from the book: a buy crosses the best ask, a sell hits
    /// the best bid. Leaves the field untouched when there is no depth.
    fn prefill_price(&mut self) {
        let Some(c) = self.selected_contract() else {
            return;
        };
        let top = self.depth_for(c.contract_id);
        let raw = if self.side == OrderSide::Buy as i32 {
            top.ask.map(|(p, _)| p)
        } else {
            top.bid.map(|(p, _)| p)
        };
        if let Some(raw) = raw {
            self.price_input = format!("{:.2}", raw as f64 / PRICE_SCALE);
        }
    }

    fn send_order(&mut self, tx: &UnboundedSender<OrderCommand>) {
        let Some(c) = self.selected_contract() else {
            self.status = "No contract selected".to_string();
            return;
        };
        // The gateway takes the bare nordpool id + market type and rejoins the
        // PowerBot NX_/NI_ prefix itself, so pass them apart.
        let contract_id = c.contract_id;
        let market_type = gateway_market_type(c.market_type) as i32;
        // The ticket is edited in human units (decimal price per MWh, MW
        // quantity); the gateway takes integer minor-unit/MWh and kW.
        let price: f64 = match self.price_input.trim().parse() {
            Ok(p) => p,
            Err(_) => {
                self.status = format!("Invalid price: {:?}", self.price_input);
                return;
            }
        };
        let quantity: f64 = match self.qty_input.trim().parse() {
            Ok(q) if q > 0.0 => q,
            _ => {
                self.status = format!("Invalid quantity: {:?}", self.qty_input);
                return;
            }
        };
        self.order_seq += 1;
        let txt = format!("tester-{}", self.order_seq);
        // Stamp the send instant so a resulting order/trade frame carrying this
        // txt can report the full round trip (ui-receive − send).
        self.pending_sends.insert(txt.clone(), now_nanos());
        let cmd = OrderCommand {
            contract_id,
            market_type,
            side: self.side,
            price: (price * PRICE_SCALE).round() as i64,
            quantity: (quantity * QTY_SCALE).round() as i64,
            txt,
        };
        if tx.send(cmd).is_err() {
            self.status = "Order sender is gone".to_string();
        } else {
            self.status = format!(
                "Sent {} {} MW @ {} …",
                side_label(self.side),
                quantity,
                price
            );
        }
    }

    // ---- key handling ------------------------------------------------------

    pub fn on_key(&mut self, key: KeyEvent, order_tx: &UnboundedSender<OrderCommand>) {
        // Global keys first.
        if key.modifiers.contains(KeyModifiers::CONTROL) && key.code == KeyCode::Char('c') {
            self.should_quit = true;
            return;
        }
        match key.code {
            KeyCode::Esc => {
                self.should_quit = true;
                return;
            }
            KeyCode::Tab => {
                self.pane = match self.pane {
                    Pane::Contracts => Pane::Ticket,
                    Pane::Ticket => Pane::Contracts,
                };
                return;
            }
            _ => {}
        }

        match self.pane {
            Pane::Contracts => self.on_key_contracts(key, order_tx),
            Pane::Ticket => self.on_key_ticket(key, order_tx),
        }
    }

    fn on_key_contracts(&mut self, key: KeyEvent, order_tx: &UnboundedSender<OrderCommand>) {
        match key.code {
            KeyCode::Up | KeyCode::Char('k') => self.move_selection(-1),
            KeyCode::Down | KeyCode::Char('j') => self.move_selection(1),
            KeyCode::Char('f') => self.flatten_selected(order_tx),
            KeyCode::Char('m') => {
                self.filter_market = self.filter_market.cycle();
                self.recompute_visible();
            }
            KeyCode::Char('p') => {
                self.filter_product = self.filter_product.cycle();
                self.recompute_visible();
            }
            KeyCode::Char('a') => {
                self.active_only = !self.active_only;
                self.recompute_visible();
            }
            KeyCode::Enter if self.selected.is_some() => {
                self.prefill_price();
                self.pane = Pane::Ticket;
                self.field = TicketField::Side;
            }
            _ => {}
        }
    }

    fn on_key_ticket(&mut self, key: KeyEvent, order_tx: &UnboundedSender<OrderCommand>) {
        // 'f' refills the price from the book regardless of the focused field.
        if key.code == KeyCode::Char('f') {
            self.prefill_price();
            return;
        }
        match key.code {
            KeyCode::Up => self.field = self.field.prev(),
            KeyCode::Down => self.field = self.field.next(),
            KeyCode::Enter => self.send_order(order_tx),
            _ => match self.field {
                TicketField::Side => match key.code {
                    KeyCode::Left
                    | KeyCode::Right
                    | KeyCode::Char('b')
                    | KeyCode::Char('s')
                    | KeyCode::Char(' ') => {
                        self.side = if self.side == OrderSide::Buy as i32 {
                            OrderSide::Sell as i32
                        } else {
                            OrderSide::Buy as i32
                        };
                        self.prefill_price();
                    }
                    _ => {}
                },
                TicketField::AutoPrice => match key.code {
                    KeyCode::Left | KeyCode::Right | KeyCode::Char(' ') | KeyCode::Char('x') => {
                        self.auto_price = !self.auto_price;
                        if self.auto_price {
                            self.prefill_price();
                        }
                    }
                    _ => {}
                },
                // Price is book-driven and read-only while auto-price is armed.
                TicketField::Price if !self.auto_price => {
                    edit_number(&mut self.price_input, key.code)
                }
                TicketField::Price => {}
                TicketField::Qty => edit_number(&mut self.qty_input, key.code),
                TicketField::Send => {}
            },
        }
    }
}

/// Numeric text-field editing: accept digits and a single decimal point.
fn edit_number(buf: &mut String, code: KeyCode) {
    match code {
        KeyCode::Char(ch) if ch.is_ascii_digit() => buf.push(ch),
        KeyCode::Char('.') if !buf.contains('.') => buf.push('.'),
        KeyCode::Backspace => {
            buf.pop();
        }
        _ => {}
    }
}

/// PowerBot's contract id for a connector contract: the numeric nordpool id
/// prefixed by market type — `NX_` for XBID, `NI_` for local. PowerBot's order
/// path keys on this string, not the bare nordpool contract id.
pub fn powerbot_contract_id(c: &Contract) -> String {
    let prefix = match marketdata::MarketType::try_from(c.market_type) {
        Ok(marketdata::MarketType::Xbid) => "NX_",
        _ => "NI_",
    };
    format!("{prefix}{}", c.contract_id)
}

/// Map the connector's marketdata `MarketType` onto the gateway's own (identical
/// values, distinct proto packages) so an order carries the target book.
fn gateway_market_type(market_type: i32) -> gateway::MarketType {
    match marketdata::MarketType::try_from(market_type) {
        Ok(marketdata::MarketType::Xbid) => gateway::MarketType::Xbid,
        Ok(marketdata::MarketType::Local) => gateway::MarketType::Local,
        _ => gateway::MarketType::None,
    }
}

/// Short market-type label (XBID/LOCAL) for a gateway `MarketType` value; "-"
/// when unset.
fn gw_market_label(market_type: i32) -> String {
    match gateway::MarketType::try_from(market_type) {
        Ok(gateway::MarketType::Xbid) => "XBID".to_string(),
        Ok(gateway::MarketType::Local) => "LOCAL".to_string(),
        _ => "-".to_string(),
    }
}

/// Short PowerBot action code (UADD, FEXE, …) for a proto `OrderAction` value;
/// "-" for the unset/unknown zero value.
pub fn action_label(action: i32) -> String {
    match OrderAction::try_from(action) {
        Ok(OrderAction::None) | Err(_) => "-".to_string(),
        Ok(a) => a
            .as_str_name()
            .trim_start_matches("ORDER_ACTION_")
            .to_string(),
    }
}

/// Short PowerBot state code (ACTI, HIBE, …) for a proto `OrderState` value;
/// "-" for the unset/unknown zero value.
pub fn state_label(state: i32) -> String {
    match OrderState::try_from(state) {
        Ok(OrderState::None) | Err(_) => "-".to_string(),
        Ok(s) => s
            .as_str_name()
            .trim_start_matches("ORDER_STATE_")
            .to_string(),
    }
}

/// The order side that reduces a signed net position (MW): sell down a long,
/// buy back a short.
fn reduce_side(net_mw: f64) -> i32 {
    if net_mw > 0.0 {
        OrderSide::Sell as i32
    } else {
        OrderSide::Buy as i32
    }
}

pub fn side_label(side: i32) -> String {
    match OrderSide::try_from(side) {
        Ok(OrderSide::Buy) => "BUY".to_string(),
        Ok(OrderSide::Sell) => "SELL".to_string(),
        _ => "-".to_string(),
    }
}

/// Sort a stream deque newest-first by event time. A stable sort, so rows with
/// the same timestamp keep insertion order (the just-pushed-front one stays on
/// top). Cheap: the deques are capped at [`MAX_STREAM_ROWS`].
fn sort_desc_by_time<T>(rows: &mut VecDeque<T>, key: impl Fn(&T) -> i64) {
    rows.make_contiguous()
        .sort_by_key(|r| std::cmp::Reverse(key(r)));
}

/// First non-zero of the candidate timestamps — prefer an item's own time
/// (execution / last exchange update) over the gateway batch time, so a seeded or
/// reconciliation-swept row shows when it happened, not when we swept it.
fn first_time(candidates: &[i64]) -> i64 {
    candidates.iter().copied().find(|&t| t != 0).unwrap_or(0)
}

/// Current unix time in nanoseconds. Same `SystemTime`/`UNIX_EPOCH` basis as the
/// gateway's `now_nanos`, so send/receive stamps compare across the two processes
/// (they run on the same host).
pub fn now_nanos() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos() as i64
}

/// Format a round-trip duration (nanos) as whole milliseconds; "-" when this row
/// wasn't one of our sends (a seed row, or another client's order).
fn fmt_rtt(rtt: Option<i64>) -> String {
    match rtt {
        Some(ns) => format!("{}ms", ns / 1_000_000),
        None => "-".to_string(),
    }
}

/// Format unix-nanos as a local wall-clock time; "-" for the unset/zero seed.
pub fn fmt_time(nanos: i64) -> String {
    if nanos <= 0 {
        return "-".to_string();
    }
    let secs = nanos / 1_000_000_000;
    let sub = (nanos % 1_000_000_000) as u32;
    match chrono::DateTime::from_timestamp(secs, sub) {
        Some(dt) => dt
            .with_timezone(&chrono::Local)
            .format("%H:%M:%S%.3f")
            .to_string(),
        None => "-".to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn trade_row(trade_id: &str, sort_nanos: i64) -> TradeRow {
        TradeRow {
            times: RowTimes {
                sent: "-".to_string(),
                exec: "-".to_string(),
                gw_recv: "-".to_string(),
                ui_recv: "-".to_string(),
                rtt: "-".to_string(),
                sort_nanos,
            },
            txt: String::new(),
            trade_id: trade_id.to_string(),
            side: "BUY".to_string(),
            price: 1.0,
            quantity: 1.0,
            market: "XBID".to_string(),
            contract_id: 1,
            contract: String::new(),
            portfolio: String::new(),
            order_id: String::new(),
        }
    }

    #[test]
    fn re_seeding_a_trade_upserts_by_id_instead_of_duplicating() {
        let mut app = App::new(Config::default());
        // A live delta, then the same trade re-seeded after a stream reconnect
        // with a later gateway-receive stamp — the shape that duplicated before.
        app.push_trade(trade_row("T1", 100), "external");
        app.push_trade(trade_row("T1", 200), "external");
        assert_eq!(app.trades.len(), 1, "the re-seed replaces its prior row");
        assert_eq!(
            app.trades[0].times.sort_nanos, 200,
            "the freshest copy of the trade is the one kept"
        );
    }

    #[test]
    fn keyless_trades_are_not_collapsed() {
        let mut app = App::new(Config::default());
        // No trade id means nothing to reconcile against — keep both rows rather
        // than collapse unrelated events.
        app.push_trade(trade_row("", 100), "external");
        app.push_trade(trade_row("", 200), "external");
        assert_eq!(app.trades.len(), 2);
    }
}
