//! Order-rate throttling — all the pacing the eval loop owns, in one place: the per-`(portfolio,
//! contract)` cooldown and rate window, plus the executor-wide rate window. It reads the tuning
//! once, answers the three gates an evaluation needs
//! ([`in_cooldown`](Throttling::in_cooldown), [`pair_window_full`](Throttling::pair_window_full),
//! [`global_window_full`](Throttling::global_window_full)), and records each accepted send
//! against both the pair's window and the global one. The [`Pacing`] scopes hold the send
//! bookkeeping; this is their aggregation with the configured limits.

use std::collections::{HashMap, VecDeque};
use std::time::Duration;

use crate::config::ExecutionConfig;
use crate::executor::{PairKey, ms};

/// The loop's order-rate pacing across every scope (see the module doc).
pub struct Throttling {
    per_pair: HashMap<PairKey, Pacing>,
    global: Pacing,
    cooldown: Duration,
    pair_window: Duration,
    pair_max: u32,
    global_window: Duration,
    global_max: u32,
}

impl Throttling {
    pub fn new(tuning: &ExecutionConfig) -> Self {
        Self {
            per_pair: HashMap::new(),
            global: Pacing::default(),
            cooldown: tuning.order_cooldown,
            pair_window: tuning.order_rate_window,
            pair_max: tuning.order_rate_max,
            global_window: tuning.global_order_rate_window,
            global_max: tuning.global_order_rate_max,
        }
    }

    /// The pair's cooldown since its last send is still running (`ORDER_COOLDOWN` /
    /// `ORDER_REJECTED`).
    pub fn in_cooldown(&self, key: &PairKey, now_ms: i64) -> bool {
        self.per_pair
            .get(key)
            .is_some_and(|pacing| pacing.in_cooldown(now_ms, self.cooldown))
    }

    /// The pair's rate window is full (`ORDER_THROTTLING`).
    pub fn pair_window_full(&self, key: &PairKey, now_ms: i64) -> bool {
        self.per_pair
            .get(key)
            .is_some_and(|pacing| pacing.window_full(now_ms, self.pair_window, self.pair_max))
    }

    /// The executor-wide rate window is full (`GLOBAL_ORDER_THROTTLING`).
    pub fn global_window_full(&self, now_ms: i64) -> bool {
        self.global
            .window_full(now_ms, self.global_window, self.global_max)
    }

    /// Record one accepted send against both the pair's window and the global one.
    pub fn record_send(&mut self, key: &PairKey, now_ms: i64) {
        self.per_pair
            .entry(key.clone())
            .or_default()
            .record_send(now_ms, self.pair_window);
        self.global.record_send(now_ms, self.global_window);
    }
}

/// Send bookkeeping for one scope (a pair, or the executor globally): the cooldown anchor and
/// the sends inside the rate window. The per-scope building block [`Throttling`] instantiates
/// (once per pair, plus one global) — it knows nothing of "pair" vs "global" or the config
/// values; the window / cooldown / max are passed in per call.
#[derive(Debug, Default)]
struct Pacing {
    last_send_ms: Option<i64>,
    sends: VecDeque<i64>,
}

impl Pacing {
    /// Record one accepted send: anchors the cooldown and consumes a window slot (pruning
    /// what has slid out of `window`).
    fn record_send(&mut self, now_ms: i64, window: Duration) {
        self.last_send_ms = Some(now_ms);
        let window_start = now_ms - ms(window);
        while self.sends.front().is_some_and(|t| *t <= window_start) {
            self.sends.pop_front();
        }
        self.sends.push_back(now_ms);
    }

    /// Whether the cooldown since the last send is still running — the guard against a
    /// send→zero-fill→send hot loop on a slipping book (spacing is send-to-send).
    fn in_cooldown(&self, now_ms: i64, cooldown: Duration) -> bool {
        self.last_send_ms
            .is_some_and(|last| now_ms < last + ms(cooldown))
    }

    /// Whether `max` sends already sit inside the rate window ending at `now`.
    fn window_full(&self, now_ms: i64, window: Duration, max: u32) -> bool {
        let window_start = now_ms - ms(window);
        let recent = self
            .sends
            .iter()
            .filter(|sent| **sent > window_start)
            .count();
        recent >= max as usize
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::executor::{ContractKey, MarketType};

    const NOW: i64 = 1_750_000_000_000;

    fn pair(portfolio: &str, contract_id: i64) -> PairKey {
        (
            portfolio.to_owned(),
            ContractKey {
                market_type: MarketType::Local,
                contract_id,
            },
        )
    }

    #[test]
    fn per_pair_gates_are_independent_and_start_open() {
        // Defaults: order_cooldown 2s, order_rate_max 5.
        let mut throttling = Throttling::new(&ExecutionConfig::default());
        let a = pair("execution-a", 7);
        let b = pair("execution-b", 7);

        // Never sent: neither gate trips for either pair.
        assert!(!throttling.in_cooldown(&a, NOW));
        assert!(!throttling.pair_window_full(&a, NOW));

        // A send on `a` cools `a` for order_cooldown (2s) and leaves `b` untouched.
        throttling.record_send(&a, NOW);
        assert!(throttling.in_cooldown(&a, NOW + 1_999));
        assert!(
            !throttling.in_cooldown(&a, NOW + 2_000),
            "cooldown is order_cooldown = 2s"
        );
        assert!(!throttling.in_cooldown(&b, NOW), "b is independent of a");
    }

    #[test]
    fn the_global_window_counts_sends_across_every_pair() {
        // Pair max 2, global max 3 — deliberately different so a scope swap in `new` fails.
        let tuning = ExecutionConfig {
            order_rate_max: 2,
            global_order_rate_max: 3,
            ..ExecutionConfig::default()
        };
        let mut throttling = Throttling::new(&tuning);
        let (a, b, c) = (
            pair("execution-a", 7),
            pair("execution-b", 7),
            pair("execution-c", 7),
        );

        assert!(!throttling.global_window_full(NOW));
        throttling.record_send(&a, NOW);
        throttling.record_send(&b, NOW);
        assert!(
            !throttling.global_window_full(NOW),
            "two sends < global max 3"
        );

        // A third send on yet another pair fills the executor-wide window — the global scope
        // counts across pairs, which is the whole point.
        throttling.record_send(&c, NOW);
        assert!(throttling.global_window_full(NOW));

        // …and no single pair's window (max 2) is close — each pair sent exactly once.
        assert!(!throttling.pair_window_full(&a, NOW));
    }

    #[test]
    fn the_pair_window_fills_at_the_pair_max_and_slides() {
        // Pair max 2; global max high so only the pair scope gates here.
        let tuning = ExecutionConfig {
            order_rate_max: 2,
            global_order_rate_max: 100,
            ..ExecutionConfig::default()
        };
        let mut throttling = Throttling::new(&tuning);
        let a = pair("execution-a", 7);

        throttling.record_send(&a, NOW);
        assert!(
            !throttling.pair_window_full(&a, NOW),
            "one send < pair max 2"
        );
        throttling.record_send(&a, NOW);
        assert!(
            throttling.pair_window_full(&a, NOW),
            "two sends fill the pair max-2 window"
        );
        // Past order_rate_window (10s) the early sends fall out of the window.
        assert!(
            !throttling.pair_window_full(&a, NOW + 10_001),
            "the pair window slid"
        );
    }

    #[test]
    fn pacing_tracks_the_cooldown_and_the_sliding_window() {
        let mut pacing = Pacing::default();
        let cooldown = Duration::from_millis(2_000);
        let window = Duration::from_millis(10_000);

        // Never sent: nothing gates.
        assert!(!pacing.in_cooldown(NOW, cooldown));
        assert!(!pacing.window_full(NOW, window, 2));

        // One send: cooled until +2s, one window slot used.
        pacing.record_send(NOW, window);
        assert!(pacing.in_cooldown(NOW + 1_999, cooldown));
        assert!(!pacing.in_cooldown(NOW + 2_000, cooldown));
        assert!(!pacing.window_full(NOW + 2_000, window, 2));

        // Two sends fill a max-2 window until the first slides out.
        pacing.record_send(NOW + 2_000, window);
        assert!(pacing.window_full(NOW + 4_000, window, 2));
        assert!(!pacing.window_full(NOW + 10_001, window, 2), "slid out");
        assert!(pacing.window_full(NOW + 4_000, window, 1), "tighter cap");
    }
}
