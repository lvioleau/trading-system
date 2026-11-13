//! The decision trace's blocked/idle half — the audit trail for evaluations that did *not*
//! send (`docs/id-executor-implementation.md`). Every *sent* order is logged in full by the
//! eval loop inline; this is the other side: a pair that blocks (or is at target) is logged
//! per pair on a **reason change** immediately, and on an unchanged reason at most once per
//! `eval_log_throttle` — a steady heartbeat of the block reason, book, and gap, so a burst of
//! book ticks can never drown the steady state where nothing changed.
//!
//! Stateless — no store: [`EvalTrace`] keeps only the last (time, reason) per pair to drive
//! the throttle, and a send resets it so the order's follow-up state always logs.

use std::collections::HashMap;

use tracing::{debug, info, warn};

use crate::config::ExecutionConfig;
use crate::executor::{BlockReason, EvalInput, PairKey, PositionView};

/// The blocked/idle half of the decision trace, throttled per pair: a reason *change* logs
/// immediately; an unchanged reason re-logs at most once per `eval_log_throttle` — a steady
/// heartbeat instead of a log line per book tick.
#[derive(Default)]
pub struct EvalTrace {
    last: HashMap<PairKey, (i64, BlockReason)>,
}

impl EvalTrace {
    /// A send just happened for this pair: drop its throttle state, so the order's follow-up
    /// block (its next evaluation) always logs.
    pub fn sent(&mut self, key: &PairKey) {
        self.last.remove(key);
    }

    /// Whether this `(key, reason)` is due to log now — recording it as the pair's latest
    /// when it is. A reason *change* is always due; an unchanged reason re-logs only once its
    /// `throttle_ms` window has elapsed. The recording is the throttle's entire state.
    fn due(&mut self, key: &PairKey, reason: BlockReason, now_ms: i64, throttle_ms: i64) -> bool {
        let due = self.last.get(key).is_none_or(|(logged_at, logged_reason)| {
            *logged_reason != reason || now_ms - logged_at >= throttle_ms
        });
        if due {
            self.last.insert(key.clone(), (now_ms, reason));
        }
        due
    }

    /// Log one blocked/idle evaluation, throttled per pair by reason and `eval_log_throttle`.
    pub fn blocked(
        &mut self,
        key: &PairKey,
        reason: BlockReason,
        input: &EvalInput<'_>,
        now_ms: i64,
        tuning: &ExecutionConfig,
    ) {
        let throttle_ms = i64::try_from(tuning.eval_log_throttle.as_millis()).unwrap_or(i64::MAX);
        if !self.due(key, reason, now_ms, throttle_ms) {
            return;
        }
        let (portfolio, contract) = (&key.0, key.1);
        let gap = match input.position {
            PositionView::Live(gap) => Some(gap),
            _ => None,
        };
        let best_bid = input.book.and_then(|book| book.best_bid().copied());
        let best_ask = input.book.and_then(|book| book.best_ask().copied());
        match reason {
            // The flat pair is the steady state — OK, at debug so it never drowns a log.
            BlockReason::AtTargetPosition => debug!(
                portfolio,
                contract = %contract,
                decision = "OK",
                "eval"
            ),
            // The standing-gap alarms: a gap that cannot be worked must not be silent.
            // A contract past its trading cutoff (or otherwise inactive) is expected
            // end-of-life, not an alarm — it logs at info with the rest of the blocks.
            BlockReason::OrderRejected | BlockReason::OrderTooSmall => warn!(
                portfolio,
                contract = %contract,
                decision = "BLOCK",
                block_reason = %reason,
                gap,
                best_bid = ?best_bid,
                best_ask = ?best_ask,
                "eval"
            ),
            _ => info!(
                portfolio,
                contract = %contract,
                decision = "BLOCK",
                block_reason = %reason,
                gap,
                best_bid = ?best_bid,
                best_ask = ?best_ask,
                "eval"
            ),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::executor::{Book, ContractKey, Level, MarketType, OrderState, Tradeability};

    fn key() -> PairKey {
        (
            "execution-a".to_owned(),
            ContractKey {
                market_type: MarketType::Local,
                contract_id: 7,
            },
        )
    }

    fn input(book: Option<&Book>, position: PositionView) -> EvalInput<'_> {
        EvalInput {
            portfolio: "execution-a",
            contract: key().1,
            trading_enabled: true,
            position,
            order: OrderState::Idle {
                last_send_rejected: false,
            },
            tradeability: Some(Tradeability {
                active: true,
                delivery_start_ms: 1_760_000_000_000,
            }),
            book,
            in_cooldown: false,
            pair_window_full: false,
            global_window_full: false,
        }
    }

    #[test]
    fn a_reason_change_logs_immediately_an_unchanged_reason_waits_out_the_window() {
        let mut trace = EvalTrace::default();
        let key = key();
        let throttle = 1_000;

        // First sighting logs.
        assert!(trace.due(&key, BlockReason::OrderCooldown, 0, throttle));
        // The same reason inside the window is throttled.
        assert!(!trace.due(&key, BlockReason::OrderCooldown, 500, throttle));
        // A reason change logs immediately, even inside the window.
        assert!(trace.due(&key, BlockReason::BookEmpty, 600, throttle));
        // Now BookEmpty is the recorded reason; still throttled inside its own fresh window.
        assert!(!trace.due(&key, BlockReason::BookEmpty, 900, throttle));
        // Once the window elapses (the boundary is inclusive), the unchanged reason re-logs.
        assert!(trace.due(&key, BlockReason::BookEmpty, 1_600, throttle));
    }

    #[test]
    fn a_send_resets_the_throttle_so_the_follow_up_block_always_logs() {
        let mut trace = EvalTrace::default();
        let key = key();
        assert!(trace.due(&key, BlockReason::OrderCooldown, 0, 1_000));
        assert!(!trace.due(&key, BlockReason::OrderCooldown, 100, 1_000));
        // A send drops the pair's throttle state…
        trace.sent(&key);
        // …so its follow-up block logs again immediately — same reason, inside the window.
        assert!(trace.due(&key, BlockReason::OrderCooldown, 200, 1_000));
    }

    #[test]
    fn blocked_records_the_throttle_across_every_reason_tier() {
        // Drives blocked() through its debug/warn/info reason arms and its book/gap
        // extraction (both the Some and None sides); the throttle it applies is observable
        // through `due`. eval_log_throttle defaults to 1s.
        let mut trace = EvalTrace::default();
        let key = key();
        let tuning = ExecutionConfig::default();
        let book = Book {
            key: key.1,
            bids: vec![Level {
                price: 1_990,
                quantity: 10_000,
            }],
            asks: vec![Level {
                price: 2_010,
                quantity: 8_000,
            }],
        };

        // AtTargetPosition → debug, flat pair.
        trace.blocked(
            &key,
            BlockReason::AtTargetPosition,
            &input(Some(&book), PositionView::Live(0)),
            0,
            &tuning,
        );
        assert!(
            !trace.due(&key, BlockReason::AtTargetPosition, 500, 1_000),
            "blocked recorded the pair, so an immediate repeat is throttled"
        );

        // warn arm, with a live gap and a two-sided book (best_bid/best_ask both Some).
        trace.blocked(
            &key,
            BlockReason::OrderRejected,
            &input(Some(&book), PositionView::Live(10_000)),
            2_000,
            &tuning,
        );
        // info arm, with no book and no baseline (best_bid/best_ask/gap all None).
        trace.blocked(
            &key,
            BlockReason::OrderInFlight,
            &input(None, PositionView::Unavailable),
            3_000,
            &tuning,
        );
        assert!(
            !trace.due(&key, BlockReason::OrderInFlight, 3_500, 1_000),
            "the last blocked reason is the one now recorded"
        );
    }
}
