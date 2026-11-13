//! The order manager — the async owner of order sending and the whole order lifecycle
//! (`docs/refactor.md`): it wraps the gateway clients (the `AddOrder` sender and the
//! per-portfolio own-orders/own-trades subscriptions), tracks one in-flight order per
//! `(portfolio, contract)`, and owns that lifecycle state **privately** — no lock, no
//! sharing with the sync world. The eval loop reaches it through two one-way channels:
//! [`Broker`] forwards order intents *in*, and [`OrderResolved`] flows *out* when a pair's
//! order finishes.
//!
//! - **[`Broker`] is a pure forwarder.** `submit` hands the intent to this task's request
//!   channel and returns whether it was accepted (the manager mints the `cl_order_id`,
//!   records the in-flight order, and fires the `AddOrder`). The loop owns the pair's
//!   [`OrderState`]: it sets `InFlight` itself on a successful submit — the double-send
//!   guard is local, not a shared mark.
//! - **The manager reports only resolutions.** From the executor's side a pair is either in
//!   flight (an order outstanding — wait) or not; the internal split of that wait is the
//!   manager's alone. So the one thing it publishes is [`OrderResolved`] — "this pair's
//!   order finished, it is free again" — which the loop applies (and which doubles as a
//!   re-evaluation trigger). A transition *into* flight is never the manager's to report.
//!
//! **The lifecycle** (invariants 5–6 of `docs/id-executor-implementation.md`, restructured
//! onto matched fills):
//!
//! - The advisory `AddOrder` response is classified by the gateway module; an acceptance
//!   only contributes the venue order id (a secondary correlator — nothing waits on it), a
//!   definitive rejection clears the pair immediately (the one case with no feed truth to
//!   wait for), an ambiguous failure changes nothing.
//! - The own-orders feed carries the truth: the order stays in flight until a terminal
//!   revision (IOC is always terminal fast), then until the accumulated **matched-fill**
//!   quantity equals what the terminal fixed as executed (the full sent quantity on `FEXE`
//!   — remaining is 0 — else sent minus the revision's remaining). A matched fill is one the
//!   FillMatcher has proven folded into the position, so a resolved pair means the next gap
//!   read is settled with respect to our own trades — no double-execution, with no separate
//!   echo bookkeeping. (This terminal-vs-fold split stays inside [`try_clear`]; the loop
//!   never sees it.)
//! - `order_inflight_warn` is a soft, earlier heads-up: an order still outstanding by then
//!   is warned about (no terminal yet, or terminal seen but its position fold still due),
//!   without touching state — the resolve timeout still owns the verdict.
//! - `order_resolve_timeout` is the alarmed backstop: an order with no reconciled
//!   terminal by then is treated as dead and the pair freed — the one window a late fill
//!   can double-execute, which is why it is generous and alarmed.
//! - A gateway rejection paces: the resolution carries `last_send_rejected: true` (the eval
//!   loop's cooldown then reports `ORDER_REJECTED`), and a run of consecutive rejections is
//!   alarmed — it otherwise looks exactly like a gap that never shrinks.
//!
//! Raw own-trade fills never reach this module: the gateway feeds them straight to the
//! FillMatcher, whose matched output is this module's only fill input.

use std::collections::HashMap;

use anyhow::anyhow;
use tokio::sync::mpsc;
use tokio::time::Duration;
use tracing::{debug, error, info, warn};

use crate::executor::{
    ContractKey, OrderBroker, OrderIntent, OrderRevision, OrderState, PairKey, PortfolioId,
};
use crate::fill_matcher::MatchedFill;
use crate::gateway::GatewayEvent;
use crate::order_sender::{OrderOutcome, OrderRequest, OrderResponse, OrderSink};

/// Capacity of the broker→manager order-request channel. Submits `try_send`: a full
/// channel means the manager task is wedged, and the submit fails rather than blocking
/// the eval loop.
const ORDER_REQUEST_CAPACITY: usize = 64;

/// Capacity of the internal gateway-event and timeout channels; backpressure, not sizing.
const EVENT_CAPACITY: usize = 256;

/// Consecutive gateway rejections on one pair before each further one alarms — one or two
/// are a limit blip healing on the cooldown-paced retry; a run of them is a gap that will
/// never shrink by itself.
const REJECTIONS_BEFORE_ALARM: u32 = 3;

/// One outstanding order: the join accumulators the lifecycle needs. The join key is the
/// order's `cl_order_id`, stamped at submit and echoed by the gateway on revisions and
/// fills; the venue order id is the secondary correlator once the response hands it over.
#[derive(Debug)]
struct InFlightOrder {
    cl_order_id: String,
    order_id: Option<String>,
    /// Sent quantity, kW positive — the executed-quantity reconcile's baseline.
    quantity: i64,
    /// Σ|matched fills| accrued so far (each already proven folded into the position).
    matched_quantity: i64,
    /// The latest own-order revision, kept by revision number; terminal once
    /// `terminal == true`.
    latest_revision: Option<OrderRevision>,
}

impl InFlightOrder {
    /// `cl_order_id` first (stamped at submit, echoed everywhere), the venue order id
    /// once known. Empty keys never match — they mean "not set" on the wire.
    fn matches(&self, cl_order_id: &str, order_id: &str) -> bool {
        (!cl_order_id.is_empty() && cl_order_id == self.cl_order_id)
            || (!order_id.is_empty() && self.order_id.as_deref() == Some(order_id))
    }
}

/// One pair's lifecycle record: the in-flight order (if any) and the rejection pacing the
/// eval loop reads back through [`OrderResolved`].
#[derive(Debug, Default)]
struct PairRecord {
    in_flight: Option<InFlightOrder>,
    last_send_rejected: bool,
    consecutive_rejections: u32,
}

/// A pair's outstanding order has resolved — the manager telling the eval loop it is free to
/// send again (which the loop applies and re-evaluates the pair on). The manager only ever
/// reports this direction: the loop owns the `InFlight` mark (set on submit), so the only
/// transition the manager publishes is the return to idle.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OrderResolved {
    pub key: PairKey,
    /// Whether that order ended in a gateway rejection — paces the retry (`ORDER_REJECTED`).
    pub last_send_rejected: bool,
}

/// The order manager's lifecycle state — owned by the [`run`] task alone (no lock, no
/// sharing): one in-flight order per pair, the `cl_order_id → pair` index the feeds and
/// timeouts resolve through, and the `cl_order_id` minting counter.
#[derive(Debug)]
struct Manager {
    pairs: HashMap<PortfolioId, HashMap<ContractKey, PairRecord>>,
    /// `cl_order_id` → its pair: how feed revisions and timeouts locate their order.
    by_cl_order_id: HashMap<String, PairKey>,
    /// Discriminates this run in every `cl_order_id` (`id-executor:<instance>:<pf>:<seq>`).
    instance: String,
    /// Monotonic per-run order counter — a plain `u64`; this task is the only minter.
    sequence: u64,
}

impl Manager {
    fn new(instance: &str) -> Self {
        Self {
            pairs: HashMap::new(),
            by_cl_order_id: HashMap::new(),
            instance: instance.to_owned(),
            sequence: 0,
        }
    }

    /// Mint the `cl_order_id`, record the in-flight order, and return the gateway request to
    /// fire. The loop has already marked the pair `InFlight`; this is the manager taking
    /// ownership of the lifecycle — nothing is published, since the loop owns that mark.
    fn record(&mut self, intent: &OrderIntent) -> OrderRequest {
        let cl_order_id = format!(
            "id-executor:{}:{}:{}",
            self.instance, intent.portfolio, self.sequence
        );
        self.sequence += 1;
        let key: PairKey = (intent.portfolio.clone(), intent.contract);
        let record = self
            .pairs
            .entry(key.0.clone())
            .or_default()
            .entry(key.1)
            .or_default();
        record.in_flight = Some(InFlightOrder {
            cl_order_id: cl_order_id.clone(),
            order_id: None,
            quantity: intent.quantity,
            matched_quantity: 0,
            latest_revision: None,
        });
        self.by_cl_order_id.insert(cl_order_id.clone(), key);
        OrderRequest {
            portfolio: intent.portfolio.clone(),
            contract: intent.contract,
            side: intent.side,
            quantity: intent.quantity,
            price: intent.price,
            cl_order_id,
        }
    }

    /// The [`OrderResolved`] to publish if applying an event freed the pair — i.e. its order
    /// went from in flight to idle. Any other net change (or none) publishes nothing: the
    /// loop owns the `InFlight` mark, so a transition *into* flight is never the manager's to
    /// report.
    fn resolution(&self, key: PairKey, before: OrderState) -> Option<OrderResolved> {
        match (before, self.order_state(&key.0, &key.1)) {
            (OrderState::InFlight, OrderState::Idle { last_send_rejected }) => {
                Some(OrderResolved {
                    key,
                    last_send_rejected,
                })
            }
            _ => None,
        }
    }

    fn record_mut(&mut self, portfolio: &str, contract: &ContractKey) -> Option<&mut PairRecord> {
        self.pairs.get_mut(portfolio)?.get_mut(contract)
    }

    /// The pair's order state as the loop sees it: `InFlight` while an order is outstanding
    /// (regardless of whether it is terminal yet — the terminal-vs-fold distinction is
    /// internal to [`try_clear`]), else `Idle`.
    fn order_state(&self, portfolio: &str, contract: &ContractKey) -> OrderState {
        match self
            .pairs
            .get(portfolio)
            .and_then(|contracts| contracts.get(contract))
        {
            Some(record) if record.in_flight.is_some() => OrderState::InFlight,
            Some(record) => OrderState::Idle {
                last_send_rejected: record.last_send_rejected,
            },
            None => OrderState::Idle {
                last_send_rejected: false,
            },
        }
    }

    /// An own-order revision arrived: keep it as its order's latest (by revision number)
    /// and try to clear — the trigger is the terminal *plus* the matched-quantity
    /// reconcile, never the terminal alone (`FEXE` can outrun the fills' folds).
    fn on_revision(&mut self, revision: OrderRevision) -> Option<OrderResolved> {
        let key: PairKey = (revision.portfolio.clone(), revision.contract);
        let before = self.order_state(&key.0, &key.1);
        let Some(record) = self.record_mut(&key.0, &key.1) else {
            debug!(
                cl_order_id = revision.cl_order_id,
                order_id = revision.order_id,
                "own-order revision for no tracked pair (an old order's tail); ignoring"
            );
            return None;
        };
        let Some(order) = record.in_flight.as_mut() else {
            debug!(
                cl_order_id = revision.cl_order_id,
                order_id = revision.order_id,
                "own-order revision past resolution (an old order's tail); ignoring"
            );
            return None;
        };
        if !order.matches(&revision.cl_order_id, &revision.order_id) {
            debug!(
                cl_order_id = revision.cl_order_id,
                in_flight = order.cl_order_id,
                "own-order revision for a different order than the pair's in-flight; ignoring"
            );
            return None;
        }
        // The own-orders feed is the source of truth for the venue order id (the
        // synchronous AddOrder response is advisory and can arrive after a terminal
        // revision). Adopt it here so resolution never logs an empty order_id.
        if order.order_id.is_none() && !revision.order_id.is_empty() {
            order.order_id = Some(revision.order_id.clone());
        }
        if order
            .latest_revision
            .as_ref()
            .is_none_or(|latest| latest.revision_no <= revision.revision_no)
        {
            order.latest_revision = Some(revision);
        }
        self.try_clear(&key);
        self.resolution(key, before)
    }

    /// A matched fill arrived — proven folded into the position: accrue it to its pair's
    /// in-flight order and try to clear. A fill matching no in-flight order is a late
    /// fill (a timed-out order's, or a duplicate past clearing): warned, ignored — its
    /// fold already moved the position, so the gap is settled either way.
    fn on_matched_fill(&mut self, matched: &MatchedFill) -> Option<OrderResolved> {
        let fill = &matched.fill;
        let key: PairKey = (fill.portfolio.clone(), fill.contract);
        let before = self.order_state(&key.0, &key.1);
        let accrued = self
            .record_mut(&key.0, &key.1)
            .and_then(|record| record.in_flight.as_mut())
            .filter(|order| order.matches(&fill.cl_order_id, &fill.order_id))
            .map(|order| {
                order.matched_quantity += fill.quantity.abs();
                (order.matched_quantity, order.quantity)
            });
        match accrued {
            Some((matched_quantity, sent)) => {
                info!(
                    portfolio = key.0,
                    contract = %key.1,
                    cl_order_id = fill.cl_order_id,
                    venue_trade_id = fill.venue_trade_id,
                    quantity = fill.quantity,
                    price = fill.price,
                    revision = matched.revision,
                    matched_quantity,
                    sent,
                    "fill matched to its position fold"
                );
                self.try_clear(&key);
            }
            None => warn!(
                portfolio = key.0,
                contract = %key.1,
                cl_order_id = fill.cl_order_id,
                venue_trade_id = fill.venue_trade_id,
                "late matched fill (no in-flight order it belongs to); its fold already \
                 moved the position — ignoring for lifecycle state"
            ),
        }
        self.resolution(key, before)
    }

    /// The advisory `AddOrder` outcome: acceptance contributes the venue order id; a
    /// definitive rejection clears the pair immediately — a refused order produces no
    /// feed evidence, so the response is its only voice; an ambiguous failure changes
    /// nothing (the feeds or the resolve timeout own it).
    fn on_outcome(
        &mut self,
        portfolio: &str,
        contract: ContractKey,
        cl_order_id: &str,
        outcome: &OrderOutcome,
    ) -> Option<OrderResolved> {
        let key: PairKey = (portfolio.to_owned(), contract);
        let before = self.order_state(&key.0, &key.1);
        let record = self.record_mut(&key.0, &key.1)?;
        let Some(order) = record.in_flight.as_mut() else {
            debug!(cl_order_id, "order outcome past resolution; ignoring");
            return None;
        };
        if order.cl_order_id != cl_order_id {
            info!(
                cl_order_id,
                in_flight = order.cl_order_id,
                "order outcome for a different order than the pair's in-flight; ignoring"
            );
            return None;
        }
        match outcome {
            OrderOutcome::Accepted { order_id } => {
                info!(
                    portfolio = key.0,
                    contract = %key.1,
                    cl_order_id,
                    order_id,
                    "order accepted by the gateway"
                );
                order.order_id = Some(order_id.clone());
            }
            OrderOutcome::Rejected { reason } => {
                if order.matched_quantity != 0 {
                    // Pathological: a "definitive" rejection alongside real fills. Trust
                    // the fills — leave resolution to the feeds and the timeout.
                    warn!(
                        cl_order_id,
                        reason,
                        "rejection carried for an order with matched fills; \
                                 leaving resolution to the feeds"
                    );
                    return None;
                }
                warn!(
                    portfolio = key.0,
                    contract = %key.1,
                    cl_order_id,
                    reason,
                    "order rejected by the gateway"
                );
                self.clear(&key, ClearedAs::Rejected { reason });
            }
            OrderOutcome::Failed { reason } => {
                // Ambiguous — the order may have reached the venue: no transition; the
                // join key was stamped at submit, so the feeds resolve it either way.
                warn!(
                    cl_order_id,
                    reason, "AddOrder outcome ambiguous; the feeds carry the truth"
                );
            }
        }
        self.resolution(key, before)
    }

    /// The resolve-timeout backstop: if the order is still in flight, treat it as dead
    /// and free the pair, loudly.
    fn on_timeout(
        &mut self,
        cl_order_id: &str,
        resolve_timeout: Duration,
    ) -> Option<OrderResolved> {
        let key = self.by_cl_order_id.get(cl_order_id).cloned()?; // resolved in time — common
        let before = self.order_state(&key.0, &key.1);
        let still_in_flight = self
            .record_mut(&key.0, &key.1)
            .and_then(|record| record.in_flight.as_ref())
            .is_some_and(|order| order.cl_order_id == cl_order_id);
        if still_in_flight {
            error!(
                portfolio = key.0,
                contract = %key.1,
                cl_order_id,
                timeout_ms = resolve_timeout.as_millis(),
                "order unresolved at the resolve timeout — treating it as dead; a late \
                 fill in this window is the one thing that can double-execute"
            );
            self.clear(&key, ClearedAs::Dead);
        }
        self.resolution(key, before)
    }

    /// The soft in-flight warn: if the order named by `cl_order_id` is still outstanding when
    /// its warn timer fires, log it — an early heads-up (well before [`on_timeout`]'s
    /// dead-alarm) that a send has not reconciled, distinguishing an order still awaiting its
    /// terminal from one waiting only on the position fold. Purely observational: no state
    /// change, nothing published; the resolve timeout still owns the verdict.
    fn on_inflight_warn(&self, cl_order_id: &str, warn_after: Duration) {
        let Some(key) = self.by_cl_order_id.get(cl_order_id) else {
            return; // resolved before the warn threshold — the common case
        };
        let Some(order) = self
            .pairs
            .get(&key.0)
            .and_then(|contracts| contracts.get(&key.1))
            .and_then(|record| record.in_flight.as_ref())
            .filter(|order| order.cl_order_id == cl_order_id)
        else {
            return; // already cleared, or a fresh order replaced this one on the pair
        };
        let awaiting = match order.latest_revision.as_ref().filter(|r| r.terminal) {
            Some(revision) => {
                let executed = executed_quantity(order.quantity, revision);
                format!(
                    "terminal {} seen — awaiting the position fold ({} of {} kW folded)",
                    revision.action, order.matched_quantity, executed
                )
            }
            None => "no terminal own-orders revision yet".to_owned(),
        };
        warn!(
            portfolio = key.0,
            contract = %key.1,
            cl_order_id,
            order_id = order.order_id.as_deref().unwrap_or(""),
            waiting_ms = warn_after.as_millis(),
            awaiting,
            "order still in flight past the warn threshold"
        );
    }

    /// Clear the in-flight order once its terminal revision has arrived *and* the matched
    /// quantity equals what it fixed as executed. A shortfall holds — the missing fill is
    /// still in flight on its stream or not yet folded — with the timeout as backstop.
    fn try_clear(&mut self, key: &PairKey) {
        let Some(order) = self
            .record_mut(&key.0, &key.1)
            .and_then(|record| record.in_flight.as_ref())
        else {
            return;
        };
        let Some(revision) = order
            .latest_revision
            .as_ref()
            .filter(|revision| revision.terminal)
        else {
            return;
        };
        let executed = executed_quantity(order.quantity, revision);
        if order.matched_quantity != executed {
            debug!(
                portfolio = key.0,
                contract = %key.1,
                cl_order_id = order.cl_order_id,
                executed,
                matched_quantity = order.matched_quantity,
                action = revision.action,
                "terminal seen; holding until the matched quantity reconciles"
            );
            return;
        }
        // Owned copies: `clear` needs the record mutably, so the revision borrow ends here.
        let (action, state) = (revision.action.clone(), revision.state.clone());
        if is_rejection_action(&action) && executed == 0 {
            let reason = format!("{action} ({state})");
            self.clear(key, ClearedAs::Rejected { reason: &reason });
        } else {
            self.clear(
                key,
                ClearedAs::Done {
                    executed,
                    action: &action,
                },
            );
        }
    }

    /// Drop the pair's in-flight order and set the rejection pacing accordingly.
    fn clear(&mut self, key: &PairKey, cleared_as: ClearedAs<'_>) {
        let Some(record) = self.record_mut(&key.0, &key.1) else {
            return;
        };
        let Some(order) = record.in_flight.take() else {
            return;
        };
        match cleared_as {
            ClearedAs::Done { executed, action } => {
                record.last_send_rejected = false;
                record.consecutive_rejections = 0;
                info!(
                    portfolio = key.0,
                    contract = %key.1,
                    cl_order_id = order.cl_order_id,
                    order_id = order.order_id.as_deref().unwrap_or(""),
                    sent = order.quantity,
                    executed,
                    action,
                    "order resolved; every fill folded into the position"
                );
            }
            ClearedAs::Rejected { reason } => {
                record.last_send_rejected = true;
                record.consecutive_rejections += 1;
                if record.consecutive_rejections >= REJECTIONS_BEFORE_ALARM {
                    error!(
                        portfolio = key.0,
                        contract = %key.1,
                        consecutive_rejections = record.consecutive_rejections,
                        reason,
                        "orders rejected back-to-back — this gap will not shrink until \
                         the cause clears"
                    );
                }
            }
            // Dead (timeout): no pacing verdict either way — the alarm already fired.
            ClearedAs::Dead => {}
        }
        self.by_cl_order_id.remove(&order.cl_order_id);
    }
}

/// How an in-flight order left the books.
enum ClearedAs<'a> {
    /// Terminal + reconciled: the normal end (includes a zero-fill kill).
    Done { executed: i64, action: &'a str },
    /// Definitively refused (response or terminal reject action, nothing executed).
    Rejected { reason: &'a str },
    /// The resolve timeout expired — state unknown, pair freed.
    Dead,
}

/// What a terminal revision fixes as executed: the full sent quantity on `FEXE`
/// (fully-executed by definition), sent minus the revision's remaining quantity otherwise
/// (a clean kill leaves remaining = sent ⇒ executed 0; a kill after `PEXE` leaves the
/// unfilled remainder), clamped against venue nonsense.
fn executed_quantity(sent: i64, revision: &OrderRevision) -> i64 {
    if revision.action == "FEXE" {
        sent
    } else {
        (sent - revision.quantity).clamp(0, sent)
    }
}

/// Terminal actions that mean the venue (or PowerBot) refused the order rather than
/// executing or killing it: user/API/system rejections, a system error, a PowerBot
/// risk-limit violation.
fn is_rejection_action(action: &str) -> bool {
    matches!(action, "UREJ" | "AREJ" | "SREJ" | "SERR" | "RLVI")
}

/// The sync loop's handle — a pure forwarder over the request channel, cheap to clone and
/// holding no state. [`OrderBroker::submit`] hands the intent to the manager task; the loop
/// owns the pair's phase (it marks `InFlight` on a successful submit).
#[derive(Clone)]
pub struct Broker {
    requests_tx: mpsc::Sender<OrderIntent>,
}

impl OrderBroker for Broker {
    fn submit(&self, intent: &OrderIntent) -> bool {
        // `try_send`: a full channel means the manager task is wedged, so the submit fails
        // rather than blocking the eval loop — the loop leaves the pair `Ready` and the
        // standing gap re-decides on the next trigger. No state to roll back (the manager
        // mints the `cl_order_id` and records only on receipt).
        if self.requests_tx.try_send(intent.clone()).is_err() {
            warn!(
                portfolio = intent.portfolio,
                "order request channel unavailable; dropping the submit"
            );
            return false;
        }
        true
    }
}

/// Build the broker and the request receiver [`run`] consumes.
pub fn broker() -> (Broker, mpsc::Receiver<OrderIntent>) {
    let (requests_tx, requests_rx) = mpsc::channel(ORDER_REQUEST_CAPACITY);
    (Broker { requests_tx }, requests_rx)
}

/// Run the manager: consume the eval loop's order intents (via the [`Broker`]), the gateway
/// feeds' own-orders events, the sender's classified [`OrderResponse`]s, and the FillMatcher's
/// matched fills; fire orders through the injected [`OrderSink`]. It owns the lifecycle state
/// privately and publishes an [`OrderResolved`] on `resolutions_tx` whenever a pair's order
/// finishes. `instance` discriminates this run's `cl_order_id`s across restarts. The gateway
/// sender, its subscriptions, and all the channels are built and monitored by the caller.
/// Returns only on a fatal condition — a closed channel — which the caller treats as a boot
/// failure.
#[expect(
    clippy::too_many_arguments,
    reason = "the wiring seam; every input is distinct"
)]
pub async fn run(
    inflight_warn: Duration,
    resolve_timeout: Duration,
    instance: String,
    order_sender: impl OrderSink,
    mut requests_rx: mpsc::Receiver<OrderIntent>,
    mut matched_fills_rx: mpsc::Receiver<MatchedFill>,
    resolutions_tx: mpsc::Sender<OrderResolved>,
    mut responses_rx: mpsc::Receiver<OrderResponse>,
    mut gateway_events_rx: mpsc::Receiver<GatewayEvent>,
) -> anyhow::Error {
    let mut manager = Manager::new(&instance);
    // Each submitted order arms two one-shot timers keyed by its `cl_order_id`: a soft warn
    // and the hard resolve timeout. Either only matters if the order is still in flight when
    // it fires.
    let (warnings_tx, mut warnings_rx) = mpsc::channel::<String>(EVENT_CAPACITY);
    let (timeouts_tx, mut timeouts_rx) = mpsc::channel::<String>(EVENT_CAPACITY);
    loop {
        tokio::select! {
            intent = requests_rx.recv() => {
                let Some(intent) = intent else {
                    return anyhow!("order request channel closed; the eval loop is gone");
                };
                // Mint the id, record the in-flight order, fire it, and arm its two timers. No
                // phase publish — the loop already marked the pair InFlight on submit.
                let request = manager.record(&intent);
                arm(warnings_tx.clone(), request.cl_order_id.clone(), inflight_warn);
                arm(timeouts_tx.clone(), request.cl_order_id.clone(), resolve_timeout);
                order_sender.send(request);
            }
            gateway_event = gateway_events_rx.recv() => {
                let Some(gateway_event) = gateway_event else {
                    return anyhow!("gateway event channel closed; the gateway feeds are gone");
                };
                let resolved = match gateway_event {
                    GatewayEvent::OwnOrdersSeeded { portfolio } => {
                        info!(portfolio, "own-orders seeded (baseline only)");
                        None
                    }
                    GatewayEvent::OwnTradesSeeded { portfolio } => {
                        info!(portfolio, "own-trades seeded (dedupe baseline only)");
                        None
                    }
                    GatewayEvent::OrderRevision(revision) => manager.on_revision(revision),
                };
                if let Some(resolved) = resolved
                    && resolutions_tx.send(resolved).await.is_err()
                {
                    return anyhow!("resolution channel closed; the eval loop is gone");
                }
            }
            response = responses_rx.recv() => {
                let Some(response) = response else {
                    return anyhow!("order response channel closed; the sender is gone");
                };
                // The advisory outcome resolves or paces the pair (invariant 5).
                if let Some(resolved) = manager.on_outcome(
                    &response.portfolio,
                    response.contract,
                    &response.cl_order_id,
                    &response.outcome,
                ) && resolutions_tx.send(resolved).await.is_err()
                {
                    return anyhow!("resolution channel closed; the eval loop is gone");
                }
            }
            matched = matched_fills_rx.recv() => {
                let Some(matched) = matched else {
                    return anyhow!("matched-fill channel closed; the fill matcher is gone");
                };
                if let Some(resolved) = manager.on_matched_fill(&matched)
                    && resolutions_tx.send(resolved).await.is_err()
                {
                    return anyhow!("resolution channel closed; the eval loop is gone");
                }
            }
            expired = timeouts_rx.recv() => {
                let Some(cl_order_id) = expired else {
                    continue; // a sender always lives on this stack
                };
                if let Some(resolved) = manager.on_timeout(&cl_order_id, resolve_timeout)
                    && resolutions_tx.send(resolved).await.is_err()
                {
                    return anyhow!("resolution channel closed; the eval loop is gone");
                }
            }
            warned = warnings_rx.recv() => {
                let Some(cl_order_id) = warned else {
                    continue; // a sender always lives on this stack
                };
                // Purely observational — no state change, nothing to publish.
                manager.on_inflight_warn(&cl_order_id, inflight_warn);
            }
        }
    }
}

/// Arm a one-shot timer that fires `cl_order_id` onto `tx` after `after` — the per-order
/// warn and resolve-timeout backstops share this shape (each only matters if the order is
/// still in flight when it fires).
fn arm(tx: mpsc::Sender<String>, cl_order_id: String, after: Duration) {
    tokio::spawn(async move {
        tokio::time::sleep(after).await;
        let _ = tx.send(cl_order_id).await;
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::executor::{Fill, MarketType, Side};

    const PORTFOLIO: &str = "execution-a";

    fn contract() -> ContractKey {
        ContractKey {
            market_type: MarketType::Local,
            contract_id: 7,
        }
    }

    fn key() -> PairKey {
        (PORTFOLIO.to_owned(), contract())
    }

    /// The resolution the manager publishes when the pair's order finishes.
    fn resolved(last_send_rejected: bool) -> Option<OrderResolved> {
        Some(OrderResolved {
            key: key(),
            last_send_rejected,
        })
    }

    /// A Manager tracking one in-flight 10 MW order under CL-1 — the pair mid-lifecycle.
    fn in_flight_manager() -> Manager {
        let mut manager = Manager::new("test");
        manager
            .pairs
            .entry(PORTFOLIO.to_owned())
            .or_default()
            .insert(
                contract(),
                PairRecord {
                    in_flight: Some(InFlightOrder {
                        cl_order_id: "CL-1".to_owned(),
                        order_id: None,
                        quantity: 10_000,
                        matched_quantity: 0,
                        latest_revision: None,
                    }),
                    ..PairRecord::default()
                },
            );
        manager.by_cl_order_id.insert("CL-1".to_owned(), key());
        manager
    }

    fn revision(action: &str, remaining: i64, terminal: bool) -> OrderRevision {
        OrderRevision {
            portfolio: PORTFOLIO.to_owned(),
            contract: contract(),
            order_id: "O-1".to_owned(),
            cl_order_id: "CL-1".to_owned(),
            revision_no: 2,
            terminal,
            quantity: remaining,
            action: action.to_owned(),
            state: "IACT".to_owned(),
        }
    }

    fn matched(venue_trade_id: &str, quantity: i64) -> MatchedFill {
        MatchedFill {
            fill: Fill {
                portfolio: PORTFOLIO.to_owned(),
                contract: contract(),
                order_id: "O-1".to_owned(),
                cl_order_id: "CL-1".to_owned(),
                venue_trade_id: venue_trade_id.to_owned(),
                exchange: "nordpool".to_owned(),
                quantity,
                price: 1_990,
                executed_at_nanos: 1_000,
            },
            revision: 2,
        }
    }

    fn state(manager: &Manager) -> OrderState {
        manager.order_state(PORTFOLIO, &contract())
    }

    fn in_flight_order_id(manager: &Manager) -> Option<String> {
        manager.pairs[PORTFOLIO][&contract()]
            .in_flight
            .as_ref()
            .and_then(|order| order.order_id.clone())
    }

    #[test]
    fn the_full_lifecycle_resolves_on_terminal_plus_matched_reconcile() {
        let mut manager = in_flight_manager();
        assert_eq!(state(&manager), OrderState::InFlight);

        // The FEXE terminal alone must not resolve — the fills' folds are still due, so the
        // pair stays in flight and nothing is published.
        assert_eq!(manager.on_revision(revision("FEXE", 0, true)), None);
        assert_eq!(state(&manager), OrderState::InFlight);

        // One of two matched fills is a shortfall — still in flight, nothing published.
        assert_eq!(manager.on_matched_fill(&matched("T-1", -4_000)), None);
        assert_eq!(state(&manager), OrderState::InFlight);

        // The second completes the reconcile: resolved, index dropped.
        assert_eq!(
            manager.on_matched_fill(&matched("T-2", -6_000)),
            resolved(false)
        );
        assert!(manager.by_cl_order_id.is_empty());
    }

    #[test]
    fn matched_fills_can_precede_the_terminal() {
        // The two streams race freely: both folds landing before FEXE is the same resolve —
        // and no resolution until the terminal, so nothing is published early.
        let mut manager = in_flight_manager();
        assert_eq!(manager.on_matched_fill(&matched("T-1", -4_000)), None);
        assert_eq!(manager.on_matched_fill(&matched("T-2", -6_000)), None);
        assert_eq!(state(&manager), OrderState::InFlight, "no terminal yet");
        assert_eq!(
            manager.on_revision(revision("FEXE", 0, true)),
            resolved(false)
        );
    }

    #[test]
    fn a_zero_fill_kill_resolves_straight_away() {
        // Killed unfilled: remaining = sent ⇒ executed 0 — nothing to wait for, and the
        // kill is not a rejection (the book simply moved). Resolves directly.
        let mut manager = in_flight_manager();
        assert_eq!(
            manager.on_revision(revision("SDEL", 10_000, true)),
            resolved(false)
        );
    }

    #[test]
    fn a_partial_kill_waits_for_exactly_the_executed_quantity() {
        // Killed after a 3 MW partial: remaining 7 ⇒ executed 3 — still in flight until the
        // fill folds.
        let mut manager = in_flight_manager();
        assert_eq!(manager.on_revision(revision("SDEL", 7_000, true)), None);
        assert_eq!(state(&manager), OrderState::InFlight);
        assert_eq!(
            manager.on_matched_fill(&matched("T-1", -3_000)),
            resolved(false)
        );
    }

    #[test]
    fn a_rejection_outcome_clears_with_rejected_pacing_and_alarms_when_persistent() {
        let mut manager = in_flight_manager();
        assert_eq!(
            manager.on_outcome(
                PORTFOLIO,
                contract(),
                "CL-1",
                &OrderOutcome::Rejected {
                    reason: "position limit".to_owned(),
                },
            ),
            resolved(true)
        );
        let record = &manager.pairs[PORTFOLIO][&contract()];
        assert_eq!(record.consecutive_rejections, 1);

        // A terminal reject action through the feed counts the same way.
        let mut manager = in_flight_manager();
        manager
            .pairs
            .get_mut(PORTFOLIO)
            .unwrap()
            .get_mut(&contract())
            .unwrap()
            .consecutive_rejections = 2;
        assert_eq!(
            manager.on_revision(revision("RLVI", 10_000, true)),
            resolved(true)
        );
        let record = &manager.pairs[PORTFOLIO][&contract()];
        assert!(record.last_send_rejected);
        assert_eq!(record.consecutive_rejections, 3, "the alarmed run");
    }

    #[test]
    fn an_accepted_outcome_only_contributes_the_order_id() {
        let mut manager = in_flight_manager();
        assert_eq!(
            manager.on_outcome(
                PORTFOLIO,
                contract(),
                "CL-1",
                &OrderOutcome::Accepted {
                    order_id: "O-9".to_owned(),
                },
            ),
            None,
            "advisory — no transition"
        );
        assert_eq!(state(&manager), OrderState::InFlight);

        // …which then joins a revision whose cl_order_id is missing (order-id fallback); it
        // is terminal but unfolded, so the pair stays in flight — no resolution.
        let mut fexe = revision("FEXE", 0, true);
        fexe.cl_order_id = String::new();
        fexe.order_id = "O-9".to_owned();
        assert_eq!(manager.on_revision(fexe), None);
        assert_eq!(state(&manager), OrderState::InFlight);
    }

    #[test]
    fn a_terminal_revision_adopts_the_venue_order_id_before_the_advisory_response() {
        // The own-orders feed is the source of truth for the venue order id, and a terminal
        // revision can outrun the advisory AddOrder response (the field race). The order must
        // take its order_id from the revision rather than wait for that response, or the
        // resolution log carries an empty order_id.
        let mut manager = in_flight_manager();
        assert_eq!(in_flight_order_id(&manager), None, "no id before any feed");

        // A terminal partial kill (executed 3 of 10) lands first: still awaiting the fill
        // fold, so no resolution — but the venue id the revision brought is already adopted.
        assert_eq!(manager.on_revision(revision("SDEL", 7_000, true)), None);
        assert_eq!(state(&manager), OrderState::InFlight);
        assert_eq!(in_flight_order_id(&manager), Some("O-1".to_owned()));

        // The later Accepted response is then a pure no-op — the id already matches.
        assert_eq!(
            manager.on_outcome(
                PORTFOLIO,
                contract(),
                "CL-1",
                &OrderOutcome::Accepted {
                    order_id: "O-1".to_owned(),
                },
            ),
            None,
        );
        assert_eq!(in_flight_order_id(&manager), Some("O-1".to_owned()));
    }

    #[test]
    fn an_ambiguous_failure_changes_nothing_and_the_timeout_frees_the_pair() {
        let mut manager = in_flight_manager();
        assert_eq!(
            manager.on_outcome(
                PORTFOLIO,
                contract(),
                "CL-1",
                &OrderOutcome::Failed {
                    reason: "transport".to_owned(),
                },
            ),
            None,
            "the feeds own the truth"
        );
        assert_eq!(state(&manager), OrderState::InFlight);

        // The backstop: still unresolved at the timeout ⇒ dead, pair freed (resolution
        // published), no rejection pacing (the gap retries on the ordinary cooldown).
        assert_eq!(
            manager.on_timeout("CL-1", Duration::from_secs(10)),
            resolved(false)
        );
        assert!(manager.by_cl_order_id.is_empty());

        // A timeout firing after a normal clear is a no-op — nothing to publish.
        assert_eq!(manager.on_timeout("CL-1", Duration::from_secs(10)), None);
    }

    #[test]
    fn foreign_and_late_events_never_disturb_the_in_flight_order() {
        let mut manager = in_flight_manager();
        // A revision for some other order (neither cl nor venue id match) is ignored.
        let mut foreign = revision("FEXE", 0, true);
        foreign.cl_order_id = "CL-OTHER".to_owned();
        foreign.order_id = "O-OTHER".to_owned();
        assert_eq!(manager.on_revision(foreign), None);
        assert_eq!(state(&manager), OrderState::InFlight);

        // A late matched fill (wrong cl) is warned and ignored for lifecycle state.
        let mut late = matched("T-9", -1_000);
        late.fill.cl_order_id = "CL-OLD".to_owned();
        late.fill.order_id = "O-OLD".to_owned();
        assert_eq!(manager.on_matched_fill(&late), None);
        assert_eq!(state(&manager), OrderState::InFlight);
        let order = manager.pairs[PORTFOLIO][&contract()]
            .in_flight
            .as_ref()
            .unwrap();
        assert_eq!(order.matched_quantity, 0);
    }

    #[test]
    fn the_inflight_warn_only_fires_while_the_order_is_still_outstanding() {
        // Observational only: it never touches state, and it silently no-ops once the order
        // has cleared or been replaced. The `awaiting` classification is Display-logged, so
        // we assert the state guards, not the message text.
        let warn = Duration::from_secs(3);

        // Still in flight (no terminal yet) — warns, state untouched.
        let manager = in_flight_manager();
        manager.on_inflight_warn("CL-1", warn);
        assert_eq!(state(&manager), OrderState::InFlight);

        // Terminal seen but the fold is still due (the "waiting for position" case) — warns.
        let mut manager = in_flight_manager();
        assert_eq!(manager.on_revision(revision("FEXE", 0, true)), None);
        manager.on_inflight_warn("CL-1", warn);
        assert_eq!(state(&manager), OrderState::InFlight);

        // Resolved before the warn fires — the index is gone, so it is a pure no-op.
        let mut manager = in_flight_manager();
        assert_eq!(
            manager.on_revision(revision("SDEL", 10_000, true)),
            resolved(false)
        );
        assert!(manager.by_cl_order_id.is_empty());
        manager.on_inflight_warn("CL-1", warn); // must not panic
    }

    fn sell_ten() -> OrderIntent {
        OrderIntent {
            portfolio: PORTFOLIO.to_owned(),
            contract: contract(),
            side: Side::Sell,
            quantity: 10_000,
            price: 1_990,
            gap: 10_000,
            best_bid: None,
            best_ask: None,
        }
    }

    #[test]
    fn the_broker_only_forwards_the_intent() {
        // No state, no phase, no mark — submit is a channel send that reports acceptance.
        let (broker, mut requests_rx) = broker();
        assert!(broker.submit(&sell_ten()));
        let forwarded = requests_rx.try_recv().unwrap();
        assert_eq!(forwarded.portfolio, PORTFOLIO);
        assert_eq!(forwarded.quantity, 10_000);
    }

    #[test]
    fn record_mints_the_cl_order_id_and_marks_in_flight() {
        // The manager owns the id and the in-flight state — on receipt, not at submit.
        let mut manager = Manager::new("test");
        let request = manager.record(&sell_ten());
        assert_eq!(request.cl_order_id, "id-executor:test:execution-a:0");
        assert_eq!(request.quantity, 10_000);
        assert_eq!(state(&manager), OrderState::InFlight);

        // A second order gets a fresh, monotonic sequence.
        let other = OrderIntent {
            contract: ContractKey {
                market_type: MarketType::Local,
                contract_id: 8,
            },
            ..sell_ten()
        };
        assert_eq!(
            manager.record(&other).cl_order_id,
            "id-executor:test:execution-a:1"
        );
    }
}
