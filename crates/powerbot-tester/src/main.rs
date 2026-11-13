//! powerbot-tester: a ratatui TUI for exercising the gateway by hand. It is a
//! gRPC client of the gateway (order entry + own-order/own-trade feeds) and of
//! the nordpool connector's stream-server (contract reference data + depth), so
//! you can pick a live contract, see its best bid/ask, fire an order, and watch
//! the order and trade streams react. See README.md ("Tester TUI") for flags.

mod app;
mod config;
mod grpc;
mod logging;
mod proto;
mod ui;

use std::io::{self, Stdout};
use std::time::{Duration, Instant};

use anyhow::Result;
use crossterm::event::{Event, EventStream};
use crossterm::terminal::{
    EnterAlternateScreen, LeaveAlternateScreen, disable_raw_mode, enable_raw_mode,
};
use crossterm::{event::KeyEventKind, execute};
use futures_util::StreamExt;
use ratatui::Terminal;
use ratatui::backend::CrosstermBackend;

use crate::app::App;
use crate::config::Config;

#[tokio::main]
async fn main() -> Result<()> {
    // File logging (never stdout — the TUI owns it). Hold the guard for the whole
    // process so the appender's background thread flushes on exit.
    let _log_guard = logging::init();
    let cfg = Config::load()?;

    let (event_tx, mut event_rx) = tokio::sync::mpsc::unbounded_channel();
    let order_tx = grpc::spawn_all(&cfg, event_tx);

    let mut terminal = setup_terminal()?;
    let result = run(&mut terminal, App::new(cfg), &mut event_rx, &order_tx).await;
    restore_terminal(&mut terminal)?;
    result
}

async fn run(
    terminal: &mut Terminal<CrosstermBackend<Stdout>>,
    mut app: App,
    event_rx: &mut tokio::sync::mpsc::UnboundedReceiver<grpc::AppEvent>,
    order_tx: &tokio::sync::mpsc::UnboundedSender<grpc::OrderCommand>,
) -> Result<()> {
    let mut input = EventStream::new();
    // Render clock: wake up to ~60fps so a burst is drawn promptly but never more
    // often than the frame budget; also the beat that keeps the header clock fresh
    // when nothing streams.
    let frame_budget = Duration::from_millis(1000 / 60);
    let mut tick = tokio::time::interval(frame_budget);
    // Force a redraw at least this often even with no state change, so the
    // wall-clock/status in the header stay live while idle.
    let idle_redraw = Duration::from_millis(250);

    let mut dirty = true;
    let mut last_draw = Instant::now() - idle_redraw;
    loop {
        tokio::select! {
            // Terminal input.
            maybe_event = input.next() => {
                match maybe_event {
                    Some(Ok(Event::Key(key))) if key.kind != KeyEventKind::Release => {
                        app.on_key(key, order_tx);
                        dirty = true;
                    }
                    Some(Ok(_)) => dirty = true, // resize, mouse, focus — redraw below
                    Some(Err(_)) | None => break,
                }
            }
            // Background gRPC updates.
            maybe_app_event = event_rx.recv() => {
                match maybe_app_event {
                    Some(event) => { app.on_event(event); dirty = true; }
                    None => break,
                }
            }
            _ = tick.tick() => {}
        }

        // Coalesce: drain everything already queued before drawing. The connector
        // depth feed is a firehose, so one-draw-per-event would back the queue up
        // for seconds and an order exec would render long after it arrived — even
        // though it was received promptly (RTT is stamped on arrival, not render).
        // Draining the backlog into one render keeps the tables current.
        while let Ok(event) = event_rx.try_recv() {
            app.on_event(event);
            dirty = true;
        }

        if app.should_quit {
            break;
        }
        // Throttle to the frame budget (≤60fps); redraw regardless once idle long
        // enough to keep the header clock ticking.
        let elapsed = last_draw.elapsed();
        if (dirty && elapsed >= frame_budget) || elapsed >= idle_redraw {
            terminal.draw(|f| ui::draw(f, &app))?;
            last_draw = Instant::now();
            dirty = false;
        }
    }
    Ok(())
}

fn setup_terminal() -> Result<Terminal<CrosstermBackend<Stdout>>> {
    enable_raw_mode()?;
    let mut stdout = io::stdout();
    execute!(stdout, EnterAlternateScreen)?;
    let terminal = Terminal::new(CrosstermBackend::new(stdout))?;
    Ok(terminal)
}

fn restore_terminal(terminal: &mut Terminal<CrosstermBackend<Stdout>>) -> Result<()> {
    disable_raw_mode()?;
    execute!(terminal.backend_mut(), LeaveAlternateScreen)?;
    terminal.show_cursor()?;
    Ok(())
}
