//! The crossing engine: the whole running crosser behind one future. [`run`] wires the three
//! upstreams — the strategy feed (target records), the depth feed (the live top of book), and
//! the Position Service subscriptions (the actual-position baseline) — into the serialized
//! crossing loop that emits transfers, and resolves to the first fatal error. `main.rs` races it
//! against the shutdown signal; the black-box tests spawn it against mock upstreams, so what
//! they exercise is exactly what production runs.
//!
//! **The loop is deliberately in-process serialized** (the design's single-writer rule): one
//! event at a time — a target record, a baseline seed, or a position echo — each possibly
//! producing one awaited `Transfer`. And the state has a single writer per input: **the
//! subscription is the sole writer of the actual position** — the `Transfer` ack's resulting
//! positions are logged, never folded, so `A`/`r` always mean "what the `Subscribe` stream has
//! delivered". A target processed inside the echo's in-process delivery window (a rapid burst
//! for one contract) therefore sizes off the pre-commit actual — and aborts on the OCC guard,
//! healed by the next heartbeat, like every other stale attempt.
//!
//! **When does a cross happen?** On a target record, and *only* on a target record. The ~30s
//! heartbeat is the standing re-reconciliation pulse, and it is also the sole recovery path for
//! everything that did not cross the first time: a broken book, a failed transfer, a target
//! that preceded its portfolio's baseline (**dropped, not queued** — targets are absolute, so
//! the re-delivery carries everything the dropped record did), and an OCC abort (**abandoned,
//! not retried** — the moved position folds in from the subscription, and the next heartbeat
//! recomputes the gap from fresh state as a new intent). Position events only fold state;
//! depth updates only move the map — neither is a reason to trade. Every deferred-work
//! mechanism the loop could have grown is replaced by the same answer: the next heartbeat.
//!
//! **A fresh transfer id per attempt — the OCC guard is what makes resends safe.** Every cross
//! carries `expected_from_revision`, so an ambiguous failure — a timeout that may have
//! committed — cannot double-apply: if the attempt did commit, the revision moved, and the next
//! heartbeat's resend (still sized off the stale read) aborts on the guard; the echo then folds
//! the truth and the heartbeat after settles at target. The crosser therefore does not track
//! minted ids across attempts. Consequence to keep in mind: the guard is load-bearing here, not
//! belt-and-suspenders — an *unguarded* crosser transfer would need id reuse back.
//!
//! The crosser keeps no store: the structured log here — every decision and every transfer,
//! with both mids and the resulting revisions — *is* the audit trail of a run.

use anyhow::anyhow;
use tokio::sync::mpsc;
use tracing::{debug, error, info, warn};
use uuid::Uuid;

use crate::config::Config;
use crate::crosser::{
    ContractKey, CrossDecision, CrossingState, PortfolioId, TargetOutcome, TargetRecord,
    TransferIntent,
};
use crate::depth_feed::{self, SharedDepth, top_of_book};
use crate::position_client::{self, PositionEvent, TransferClient, TransferOutcome};
use crate::strategy_feed;

/// Capacity of the target-record channel between the strategy feed and the crossing loop, in
/// records. The consumer is in-process, so this only smooths bursts (e.g. a heartbeat re-emit of
/// every contract at once); the feed awaiting a full channel is backpressure, not loss.
const TARGET_CHANNEL_CAPACITY: usize = 1024;

/// Capacity of the position-event channel — same discipline: bursts queue, nothing is dropped.
const POSITION_CHANNEL_CAPACITY: usize = 1024;

/// Run the whole crosser: spawn the feeds and the position subscriptions, then drive the
/// crossing loop until something fails fatally — a rejected subscription (permanent config
/// mismatch), a dead task, or a closed channel. Never returns otherwise.
pub async fn run(config: Config) -> anyhow::Error {
    // The depth feed maintains the live depth view per contract; the loop reduces it to the top
    // of book and prices each cross from it at cross time.
    let books = SharedDepth::default();
    let mut depth = tokio::spawn(depth_feed::run(
        config.connector_endpoint.clone(),
        config.delivery_area_eic,
        config.feed,
        books.clone(),
    ));

    let (position_events_tx, position_events_rx) = mpsc::channel(POSITION_CHANNEL_CAPACITY);
    let mut subscriptions = tokio::spawn(position_client::run_subscriptions(
        config.position_endpoint.clone(),
        config.portfolios.iter().map(|p| p.name.clone()).collect(),
        config.feed,
        position_events_tx,
    ));

    let transfers = match TransferClient::new(
        &config.position_endpoint,
        &config.feed,
        // The ledger attribution on both legs of every cross; the strategy names the crosser
        // instance well enough while there is one crosser per strategy.
        format!("crosser:{}", config.strategy),
    ) {
        Ok(transfers) => transfers,
        Err(error) => return error.context("building the transfer client"),
    };

    let portfolios = config
        .portfolios
        .iter()
        .map(|p| (p.name.clone(), p.execution.clone()))
        .collect::<Vec<_>>();
    let (targets_tx, targets_rx) = mpsc::channel(TARGET_CHANNEL_CAPACITY);
    let mut feed = tokio::spawn(strategy_feed::run(config, targets_tx));

    // All the feeds reconnect forever and return only fatal errors; first failure wins.
    tokio::select! {
        error = crossing_loop(portfolios, targets_rx, position_events_rx, books, transfers) => error,
        result = &mut feed => task_error("strategy feed", result),
        result = &mut depth => task_error("depth feed", result),
        result = &mut subscriptions => task_error("position subscriptions", result),
    }
}

fn task_error(task: &str, result: Result<anyhow::Error, tokio::task::JoinError>) -> anyhow::Error {
    match result {
        Ok(error) => error.context(format!("{task} failed")),
        Err(join_error) => anyhow!("{task} task exited: {join_error}"),
    }
}

/// The serialized crossing loop — see the module docs for the event → cross triggers.
async fn crossing_loop(
    portfolios: Vec<(PortfolioId, PortfolioId)>,
    mut targets_rx: mpsc::Receiver<TargetRecord>,
    mut position_events_rx: mpsc::Receiver<PositionEvent>,
    books: SharedDepth,
    mut transfers: TransferClient,
) -> anyhow::Error {
    let mut state = CrossingState::new(portfolios);
    loop {
        tokio::select! {
            record = targets_rx.recv() => {
                let Some(record) = record else {
                    return anyhow!("target channel closed; the strategy feed is gone");
                };
                let portfolio = record.portfolio.clone();
                let contract = record.contract;
                match state.on_target(record) {
                    TargetOutcome::Stored => {
                        attempt(&state, &books, &mut transfers, &portfolio, contract).await;
                    }
                    // Expected at boot while the baseline snapshot is still in flight: there
                    // is no actual position to size a gap against, so the record is dropped —
                    // the heartbeat re-delivers it within one interval.
                    TargetOutcome::NotSeeded => info!(
                        portfolio,
                        contract = %contract,
                        "dropping target that preceded the portfolio's position baseline; \
                         the heartbeat re-delivers"
                    ),
                    // Unreachable while the subscription is scoped to the configured
                    // portfolios — loud, because it means the strategy broke that contract.
                    TargetOutcome::Unmapped => error!(
                        portfolio,
                        contract = %contract,
                        "dropping target for an unmapped portfolio (scoped subscription \
                         violated?)"
                    ),
                }
            }
            position_event = position_events_rx.recv() => {
                let Some(position_event) = position_event else {
                    return anyhow!("position event channel closed; the subscriptions are gone");
                };
                // Position events only fold state — they never trigger a cross. Whatever they
                // change (a drifted actual, the refresh after an OCC abort) is picked up by
                // the next heartbeat's recompute.
                match position_event {
                    PositionEvent::Seeded { portfolio, positions } => {
                        state.seed(&portfolio, positions);
                    }
                    PositionEvent::Updated(positions) => {
                        for position in positions {
                            state.apply_position(&position);
                        }
                    }
                }
            }
        }
    }
}

/// One reconciliation attempt for `(portfolio, contract)`: decide against the current state and
/// the top of book read now, and if a cross is due, emit it. Read-only over the state — the
/// outcome is logged, and the state moves only when the subscription delivers the result.
async fn attempt(
    state: &CrossingState,
    books: &SharedDepth,
    transfers: &mut TransferClient,
    portfolio: &str,
    contract: ContractKey,
) {
    let book = books
        .lock()
        .expect("depth map poisoned")
        .get(&contract)
        .map(top_of_book)
        .unwrap_or_default();
    match state.decide(portfolio, contract, book) {
        CrossDecision::NoTarget => {}
        // The actual position equals the target — a heartbeat at steady state, or the gap
        // closed by other means. Nothing to do; this is what makes the pulse free.
        CrossDecision::AtTarget => {
            debug!(portfolio, contract = %contract, "at target; nothing to cross");
        }
        // The standing-gap-with-no-mid tripwire: the target cannot be crossed until the book
        // heals. The next heartbeat re-attempts; nothing is forgotten.
        CrossDecision::NoMid { gap } => warn!(
            portfolio,
            contract = %contract,
            gap_kw = gap,
            bid = book.bid,
            ask = book.ask,
            "book is unpriceable (crossed, one-sided, or absent); withholding the cross"
        ),
        CrossDecision::Cross(intent) => {
            let strategy_mid = state.target(portfolio, contract).map(|t| t.mid_price);
            cross(transfers, intent, strategy_mid).await;
        }
    }
}

/// Emit one transfer and log its outcome. Deliberately stateless: the resulting positions reach
/// the state through the subscription echo, its single writer.
async fn cross(transfers: &mut TransferClient, intent: TransferIntent, strategy_mid: Option<i64>) {
    // A fresh idempotency id per attempt — nothing is reused across resends, because the OCC
    // guard already makes a resend of a committed-but-unacked attempt abort (see the module
    // docs). The id still lets the Position Service dedupe a literally duplicated submission.
    let transfer_id = Uuid::new_v4();
    info!(
        transfer_id = %transfer_id,
        from = intent.from,
        to = intent.to,
        contract = %intent.contract,
        gap_kw = -intent.quantity,
        quantity_kw = intent.quantity,
        live_mid = intent.price,
        strategy_mid = strategy_mid,
        expected_from_revision = intent.expected_from_revision,
        "crossing the gap to target"
    );
    match transfers.transfer(transfer_id, &intent).await {
        // Logged only, not folded: the committed positions reach the state through the
        // subscription echo, its single writer. The next heartbeat reconciles against them.
        TransferOutcome::Applied { from, to } => info!(
            transfer_id = %transfer_id,
            from_portfolio = from.portfolio,
            from_quantity_kw = from.net_quantity,
            from_revision = from.revision,
            to_portfolio = to.portfolio,
            to_quantity_kw = to.net_quantity,
            to_revision = to.revision,
            "transfer applied"
        ),
        // Abandoned, not retried: the moved position folds in from the subscription, and the
        // next heartbeat recomputes the gap from fresh state. Under a single crosser another
        // writer should not exist — a tripwire — but the guard also fires benignly when a
        // resend races its own committed-but-unacked predecessor (the ambiguous-failure case).
        TransferOutcome::Aborted { message } => warn!(
            transfer_id = %transfer_id,
            message,
            "transfer aborted on the concurrency guard; abandoning — the next heartbeat \
             recomputes against the refreshed position"
        ),
        TransferOutcome::Rejected { message } => error!(
            transfer_id = %transfer_id,
            from = intent.from,
            to = intent.to,
            message,
            "transfer rejected as invalid — crosser/position-service config mismatch; the gap \
             stands and cannot be crossed until the config is fixed"
        ),
        // The gap stands; the next heartbeat re-attempts under a fresh id. If this attempt
        // actually committed (an ambiguous timeout), the resend aborts on the OCC guard and
        // the echo settles the position — never a double-apply.
        TransferOutcome::Failed(error) => warn!(
            transfer_id = %transfer_id,
            error = format!("{error:#}"),
            "transfer failed; holding the gap for the next heartbeat"
        ),
    }
}
