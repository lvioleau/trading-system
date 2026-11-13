//! ratatui rendering. Four regions: a header status line, a contract picker +
//! order ticket row, the own-order and own-trade stream tables, and a help/status
//! footer.

use ratatui::prelude::*;
use ratatui::widgets::{Block, Borders, Cell, Paragraph, Row, Table, TableState};

use eic::EicCode;

use crate::app::{
    App, PRICE_SCALE, Pane, TicketField, action_label, powerbot_contract_id, state_label,
};
use crate::grpc::ConnState;
use crate::proto::gateway::OrderSide;
use crate::proto::marketdata::{Contract, ContractState, MarketType, ProductType};

const SEL: Color = Color::Cyan;
const DIM: Color = Color::DarkGray;
/// Highlight for the round-trip latency column.
const ACCENT: Color = Color::Yellow;
/// Width of each per-portfolio position column (also caps the header name).
const POS_COL_WIDTH: u16 = 12;

pub fn draw(f: &mut Frame, app: &App) {
    let rows = Layout::vertical([
        Constraint::Length(1),      // header
        Constraint::Percentage(46), // contracts + ticket
        Constraint::Min(6),         // streams
        Constraint::Length(1),      // footer
    ])
    .split(f.area());

    draw_header(f, rows[0], app);

    let mid =
        Layout::horizontal([Constraint::Percentage(64), Constraint::Percentage(36)]).split(rows[1]);
    draw_contracts(f, mid[0], app);
    draw_ticket(f, mid[1], app);

    let streams =
        Layout::vertical([Constraint::Percentage(50), Constraint::Percentage(50)]).split(rows[2]);
    draw_orders(f, streams[0], app);
    draw_trades(f, streams[1], app);

    draw_footer(f, rows[3], app);
}

fn draw_header(f: &mut Frame, area: Rect, app: &App) {
    let mut spans = vec![
        Span::styled("PowerBot Tester", Style::new().add_modifier(Modifier::BOLD)),
        Span::raw("  "),
    ];
    spans.extend(status_light(
        "gateway",
        &app.gateway_status,
        &app.cfg.gateway_addr,
    ));
    spans.push(Span::raw("  "));
    spans.extend(status_light(
        "connector",
        &app.connector_status,
        &app.cfg.connector_addr,
    ));
    if !app.cfg.position_portfolios.is_empty() {
        spans.push(Span::raw("  "));
        spans.extend(status_light(
            "positions",
            &app.position_status,
            &app.cfg.position_addr,
        ));
    }
    spans.push(Span::styled(
        format!(
            "   portfolio:{}  area:{}  exch:{}",
            app.cfg.portfolio_id,
            app.cfg.delivery_area_eic.area_code(),
            app.cfg.exchange
        ),
        Style::new().fg(DIM),
    ));
    f.render_widget(Paragraph::new(Line::from(spans)), area);
}

fn status_light(name: &str, state: &ConnState, addr: &str) -> Vec<Span<'static>> {
    let (glyph, color) = match state {
        ConnState::Connected => ("●", Color::Green),
        ConnState::Connecting => ("◐", Color::Yellow),
        ConnState::Disconnected(_) => ("○", Color::Red),
    };
    vec![
        Span::styled(glyph.to_string(), Style::new().fg(color)),
        Span::styled(
            format!(" {name} {}", addr.trim_start_matches("http://")),
            Style::new().fg(DIM),
        ),
    ]
}

fn draw_contracts(f: &mut Frame, area: Rect, app: &App) {
    let block = pane_block("Contracts", app.pane == Pane::Contracts);
    let inner = block.inner(area);
    f.render_widget(block, area);

    let parts = Layout::vertical([Constraint::Length(1), Constraint::Min(0)]).split(inner);

    // Filter row.
    let filters = Line::from(vec![
        Span::styled("[m]arket:", Style::new().fg(DIM)),
        Span::styled(app.filter_market.label(), Style::new().fg(SEL)),
        Span::styled("  [p]roduct:", Style::new().fg(DIM)),
        Span::styled(app.filter_product.label(), Style::new().fg(SEL)),
        Span::styled("  [a]ctive-only:", Style::new().fg(DIM)),
        Span::styled(
            if app.active_only { "on" } else { "off" },
            Style::new().fg(SEL),
        ),
        Span::styled("  [f]latten", Style::new().fg(DIM)),
    ]);
    f.render_widget(Paragraph::new(filters), parts[0]);

    // Contract table. A fixed set of columns, then one net-position column per
    // tracked portfolio (headed by the portfolio name).
    let portfolios = &app.cfg.position_portfolios;
    let mut header_cells: Vec<Cell> = [
        "Name", "Prod", "Mkt", "Delivery", "Win", "State", "Bid", "Ask",
    ]
    .into_iter()
    .map(Cell::from)
    .collect();
    for p in portfolios {
        header_cells.push(Cell::from(trunc(p, POS_COL_WIDTH as usize)));
    }
    let header = Row::new(header_cells).style(Style::new().fg(DIM).add_modifier(Modifier::BOLD));

    let rows = app.visible.iter().filter_map(|id| {
        let c = app.contracts.get(id)?;
        let top = app.depth_for(*id);
        let mut cells = vec![
            Cell::from(c.name.clone()),
            Cell::from(product_label(c.product_type)),
            Cell::from(market_label(c.market_type)),
            Cell::from(format!(
                "{}-{}",
                fmt_hm(c.delivery_start_ms),
                fmt_hm(c.delivery_end_ms)
            )),
            Cell::from(format!("{}m", window_minutes(c))),
            Cell::from(area_state_label(c, app.cfg.delivery_area_eic)),
            Cell::from(top_cell(top.bid)).style(Style::new().fg(Color::Green)),
            Cell::from(top_cell(top.ask)).style(Style::new().fg(Color::Red)),
        ];
        for idx in 0..portfolios.len() {
            let (text, color) = pos_cell(app.position_for(*id, idx));
            cells.push(Cell::from(text).style(Style::new().fg(color)));
        }
        Some(Row::new(cells))
    });

    let mut widths = vec![
        Constraint::Min(16),
        Constraint::Length(5),
        Constraint::Length(5),
        Constraint::Length(11),
        Constraint::Length(4),
        Constraint::Length(7),
        Constraint::Length(12),
        Constraint::Length(12),
    ];
    widths.extend(portfolios.iter().map(|_| Constraint::Length(POS_COL_WIDTH)));
    let table = Table::new(rows, widths)
        .header(header)
        .row_highlight_style(Style::new().bg(SEL).fg(Color::Black))
        .highlight_symbol("▸ ");

    let mut state = TableState::default();
    state.select(app.selected_index());
    f.render_stateful_widget(table, parts[1], &mut state);
}

fn draw_ticket(f: &mut Frame, area: Rect, app: &App) {
    let active = app.pane == Pane::Ticket;
    let block = pane_block("Order Ticket", active);
    let inner = block.inner(area);
    f.render_widget(block, area);

    let mut lines: Vec<Line> = Vec::new();

    match app.selected_contract() {
        Some(c) => {
            let top = app.depth_for(c.contract_id);
            lines.push(Line::from(vec![
                Span::styled("Contract ", Style::new().fg(DIM)),
                Span::styled(c.name.clone(), Style::new().add_modifier(Modifier::BOLD)),
                Span::styled(
                    format!("  → {}", powerbot_contract_id(c)),
                    Style::new().fg(Color::Yellow),
                ),
            ]));
            lines.push(Line::styled(
                format!(
                    "{} {} {}-{}",
                    product_label(c.product_type),
                    market_label(c.market_type),
                    fmt_hm(c.delivery_start_ms),
                    fmt_hm(c.delivery_end_ms)
                ),
                Style::new().fg(DIM),
            ));
            lines.push(Line::from(vec![
                Span::styled("Bid ", Style::new().fg(DIM)),
                Span::styled(top_cell(top.bid), Style::new().fg(Color::Green)),
                Span::styled("   Ask ", Style::new().fg(DIM)),
                Span::styled(top_cell(top.ask), Style::new().fg(Color::Red)),
            ]));
            // Execution-portfolio net position, and the [f]latten affordance that
            // arms this ticket to reduce it.
            if let Some(idx) = app.exec_portfolio_idx() {
                let (txt, color) = pos_cell(app.position_for(c.contract_id, idx));
                lines.push(Line::from(vec![
                    Span::styled("Pos ", Style::new().fg(DIM)),
                    Span::styled(txt, Style::new().fg(color)),
                    Span::styled(
                        format!(" MW {}   [f]latten", app.cfg.portfolio_id),
                        Style::new().fg(DIM),
                    ),
                ]));
            }
        }
        None => lines.push(Line::styled(
            "No contract selected — pick one on the left.",
            Style::new().fg(DIM),
        )),
    }

    lines.push(Line::raw(""));

    let focused = |field: TicketField| active && app.field == field;
    let field_style = |on: bool| {
        if on {
            Style::new().bg(SEL).fg(Color::Black)
        } else {
            Style::new()
        }
    };

    let side_txt = match OrderSide::try_from(app.side) {
        Ok(OrderSide::Sell) => "SELL",
        _ => "BUY",
    };
    let side_color = if side_txt == "BUY" {
        Color::Green
    } else {
        Color::Red
    };
    lines.push(Line::from(vec![
        Span::styled("Side  ", Style::new().fg(DIM)),
        Span::styled(
            format!(" {side_txt} "),
            field_style(focused(TicketField::Side)).fg(side_color),
        ),
        Span::styled("  (←/→ or b/s)", Style::new().fg(DIM)),
    ]));
    let auto_box = if app.auto_price { "[x]" } else { "[ ]" };
    lines.push(Line::from(vec![
        Span::styled("Auto  ", Style::new().fg(DIM)),
        Span::styled(
            format!(" {auto_box} best bid/ask "),
            field_style(focused(TicketField::AutoPrice)),
        ),
        Span::styled("  (space)", Style::new().fg(DIM)),
    ]));
    lines.push(Line::from(vec![
        Span::styled("Price ", Style::new().fg(DIM)),
        Span::styled(
            format!(" {:<10}", app.price_input),
            field_style(focused(TicketField::Price)),
        ),
        Span::styled(
            if app.auto_price { " (auto)" } else { "" },
            Style::new().fg(DIM),
        ),
    ]));
    lines.push(Line::from(vec![
        Span::styled("Qty   ", Style::new().fg(DIM)),
        Span::styled(
            format!(" {:<10}", app.qty_input),
            field_style(focused(TicketField::Qty)),
        ),
        Span::styled(" MW", Style::new().fg(DIM)),
    ]));
    lines.push(Line::from(vec![Span::styled(
        " [ Send Order ] ",
        field_style(focused(TicketField::Send)).add_modifier(Modifier::BOLD),
    )]));

    lines.push(Line::raw(""));
    match &app.last_order_result {
        Some(Ok(o)) => {
            lines.push(Line::styled(
                format!(
                    "✓ id={} state={} action={}",
                    o.order_id,
                    state_label(o.state),
                    action_label(o.action)
                ),
                Style::new().fg(Color::Green),
            ));
            if !o.errors.is_empty() {
                let errs = o
                    .errors
                    .iter()
                    .map(|e| format!("{}:{}", e.code, e.message))
                    .collect::<Vec<_>>()
                    .join("; ");
                lines.push(Line::styled(errs, Style::new().fg(Color::Red)));
            }
        }
        Some(Err(e)) => lines.push(Line::styled(format!("✗ {e}"), Style::new().fg(Color::Red))),
        None => {}
    }

    f.render_widget(Paragraph::new(lines), inner);
}

fn draw_orders(f: &mut Frame, area: Rect, app: &App) {
    let block = pane_block(&format!("Own Orders ({})", app.orders.len()), false);
    let inner = block.inner(area);
    f.render_widget(block, area);

    let header = Row::new([
        "Sent", "Exec", "GwRecv", "UiRecv", "RTT", "OrderId", "Txt", "Side", "State", "Action",
        "Price", "Qty", "Market", "CId", "Contract", "Error",
    ])
    .style(Style::new().fg(DIM).add_modifier(Modifier::BOLD));

    let rows = app.orders.iter().map(|o| {
        Row::new([
            Cell::from(o.times.sent.clone()),
            Cell::from(o.times.exec.clone()),
            Cell::from(o.times.gw_recv.clone()),
            Cell::from(o.times.ui_recv.clone()),
            Cell::from(o.times.rtt.clone()).style(Style::new().fg(ACCENT)),
            Cell::from(trunc(&o.order_id, 10)),
            Cell::from(trunc(&o.txt, 10)),
            Cell::from(o.side.clone()).style(side_style(&o.side)),
            Cell::from(o.state.clone()),
            Cell::from(o.action.clone()),
            Cell::from(format!("{:.2}", o.price)),
            Cell::from(format!("{:.2}", o.quantity)),
            Cell::from(o.market.clone()),
            Cell::from(o.contract_id.to_string()),
            Cell::from(trunc(app.contract_name(o.contract_id), 16)),
            Cell::from(o.error.clone()).style(Style::new().fg(Color::Red)),
        ])
    });

    let widths = [
        Constraint::Length(12),
        Constraint::Length(12),
        Constraint::Length(12),
        Constraint::Length(12),
        Constraint::Length(7),
        Constraint::Length(10),
        Constraint::Length(10),
        Constraint::Length(4),
        Constraint::Length(5),
        Constraint::Length(6),
        Constraint::Length(9),
        Constraint::Length(7),
        Constraint::Length(6),
        Constraint::Length(10),
        Constraint::Length(16),
        Constraint::Min(6),
    ];
    f.render_widget(Table::new(rows, widths).header(header), inner);
}

fn draw_trades(f: &mut Frame, area: Rect, app: &App) {
    let block = pane_block(&format!("Own Trades ({})", app.trades.len()), false);
    let inner = block.inner(area);
    f.render_widget(block, area);

    let header = Row::new([
        "Exec",
        "GwRecv",
        "UiRecv",
        "RTT",
        "TradeId",
        "Txt",
        "Side",
        "Price",
        "Qty",
        "Market",
        "CId",
        "Contract",
        "Portfolio",
        "OrderId",
    ])
    .style(Style::new().fg(DIM).add_modifier(Modifier::BOLD));

    let rows = app.trades.iter().map(|t| {
        Row::new([
            Cell::from(t.times.exec.clone()),
            Cell::from(t.times.gw_recv.clone()),
            Cell::from(t.times.ui_recv.clone()),
            Cell::from(t.times.rtt.clone()).style(Style::new().fg(ACCENT)),
            Cell::from(trunc(&t.trade_id, 12)),
            Cell::from(trunc(&t.txt, 10)),
            Cell::from(t.side.clone()).style(side_style(&t.side)),
            Cell::from(format!("{:.2}", t.price)),
            Cell::from(format!("{:.2}", t.quantity)),
            Cell::from(t.market.clone()),
            Cell::from(t.contract_id.to_string()),
            Cell::from(trunc(&t.contract, 16)),
            Cell::from(trunc(&t.portfolio, 10)),
            Cell::from(trunc(&t.order_id, 16)),
        ])
    });

    let widths = [
        Constraint::Length(12),
        Constraint::Length(12),
        Constraint::Length(12),
        Constraint::Length(7),
        Constraint::Length(12),
        Constraint::Length(10),
        Constraint::Length(4),
        Constraint::Length(9),
        Constraint::Length(7),
        Constraint::Length(6),
        Constraint::Length(10),
        Constraint::Length(16),
        Constraint::Length(10),
        Constraint::Min(10),
    ];
    f.render_widget(Table::new(rows, widths).header(header), inner);
}

fn draw_footer(f: &mut Frame, area: Rect, app: &App) {
    f.render_widget(
        Paragraph::new(Line::styled(
            format!(" {}", app.status),
            Style::new().fg(Color::White),
        )),
        area,
    );
}

// ---- small helpers --------------------------------------------------------

fn pane_block(title: &str, focused: bool) -> Block<'static> {
    let border = if focused { SEL } else { DIM };
    Block::default()
        .borders(Borders::ALL)
        .border_style(Style::new().fg(border))
        .title(Span::styled(
            format!(" {title} "),
            Style::new().fg(if focused { SEL } else { Color::Gray }),
        ))
}

fn side_style(side: &str) -> Style {
    match side {
        "BUY" => Style::new().fg(Color::Green),
        "SELL" => Style::new().fg(Color::Red),
        _ => Style::new().fg(DIM),
    }
}

/// Render a net position (MW): a dim dash when flat, green long / red short
/// otherwise, signed to one decimal.
fn pos_cell(mw: f64) -> (String, Color) {
    if mw.abs() < 0.05 {
        ("-".to_string(), DIM)
    } else if mw > 0.0 {
        (format!("+{mw:.1}"), Color::Green)
    } else {
        (format!("{mw:.1}"), Color::Red)
    }
}

fn top_cell(v: Option<(i64, i64)>) -> String {
    match v {
        Some((price, size)) => format!("{:.2}x{}", price as f64 / PRICE_SCALE, size),
        None => "-".to_string(),
    }
}

fn trunc(s: &str, max: usize) -> String {
    if s.chars().count() <= max {
        s.to_string()
    } else {
        format!(
            "{}…",
            s.chars().take(max.saturating_sub(1)).collect::<String>()
        )
    }
}

fn window_minutes(c: &Contract) -> i64 {
    if c.duration_seconds > 0 {
        c.duration_seconds as i64 / 60
    } else {
        ((c.delivery_end_ms - c.delivery_start_ms) / 60_000).max(0)
    }
}

fn product_label(pt: i32) -> &'static str {
    match ProductType::try_from(pt) {
        Ok(ProductType::P15Min) => "P15",
        Ok(ProductType::P30Min) => "P30",
        Ok(ProductType::P60Min) => "P60",
        Ok(ProductType::Other) => "Other",
        _ => "-",
    }
}

fn market_label(mt: i32) -> &'static str {
    match MarketType::try_from(mt) {
        Ok(MarketType::Local) => "LOCAL",
        Ok(MarketType::Xbid) => "XBID",
        _ => "-",
    }
}

fn area_state_label(c: &Contract, area: EicCode) -> &'static str {
    let state = c
        .delivery_areas
        .iter()
        .find(|a| a.eic_code == area.eic_code())
        .map(|a| a.state)
        .unwrap_or(ContractState::None as i32);
    match ContractState::try_from(state) {
        Ok(ContractState::Active) => "ACTIVE",
        Ok(ContractState::Hibernated) => "HIBER",
        Ok(ContractState::Inactive) => "INACT",
        Ok(ContractState::Frozen) => "FROZEN",
        _ => "-",
    }
}

fn fmt_hm(ms: i64) -> String {
    if ms <= 0 {
        return "--:--".to_string();
    }
    match chrono::DateTime::from_timestamp_millis(ms) {
        Some(dt) => dt.with_timezone(&chrono::Local).format("%H:%M").to_string(),
        None => "--:--".to_string(),
    }
}
