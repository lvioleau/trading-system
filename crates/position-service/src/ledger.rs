//! The position ledger — the fold whose result *is* the positions. Pure domain: no
//! protobuf, no IO, no config; the boundaries (gRPC server, gateway feeds) translate and
//! validate before anything reaches here.
//!
//! Every mutation is one [`Trade`]: a signed quantity of one contract on one portfolio, at
//! a price. Applying a trade adds its quantity to the `(portfolio, contract)` position and
//! bumps that position's `revision`. The ledger is append-only and idempotent by trade id —
//! re-applying a seen id is a no-op, the in-memory mirror of the database's
//! `on conflict (id) do nothing` — which makes the boot log-replay and a feed re-seed
//! overlap harmless. Folding the same trades in the same order always yields the same
//! revisions, so revisions are stable across restarts (the boot fold replays the log in
//! `seq` order).
//!
//! The ledger is portfolio-agnostic: any portfolio string folds (the log is the truth, and
//! conservation must hold over all of it); restricting mutations to the *configured*
//! portfolios is the server boundary's job. Price, event time, and the venue trade id ride
//! along for persistence and attribution but do not participate in the fold — a position is
//! a pure quantity; P&L is a downstream concern.
//!
//! Quantities and prices are venue-native fixed-point integers — [`Quantity`] in kW,
//! [`Price`] in market currency minor units per MWh — the encoding every wire this service
//! touches already uses. The fold is signed addition, under which integers are closed, so
//! exactness is inherent: no decimals, no floats, no scale conversions anywhere.
//!
//! [`ContractKey`] and [`MarketType`] deliberately mirror `contract_service::store` rather
//! than import it: the types are two lines each, and depending on the contract-service
//! crate would drag its proto build (tonic, protoc) into this one for no domain gain. The
//! venue is not part of the domain key — like the contract-service, it is a storage-layer
//! constant until a second venue exists.

use std::collections::{HashMap, HashSet};
use std::fmt;

use uuid::Uuid;

/// A signed quantity in kW (1 MW = 1000 kW) — the venue's native unit, kept end to end.
pub type Quantity = i64;

/// A unit price in market currency minor units per MWh (EUR cents today) — the venue's
/// native encoding, kept end to end. May be negative. Recorded, never folded.
pub type Price = i64;

/// A portfolio identifier — a plain configured string (e.g. `execution-a`), the ledger's
/// aggregation boundary. An alias, not a newtype: validation lives at the config and server
/// boundaries, and the ledger folds any portfolio the log contains.
pub type PortfolioId = String;

/// Fixed namespace for the deterministic UUIDv5 ids of transfer legs. Arbitrary but
/// immutable: changing it would re-mint ids for the same transfers and double-count their
/// replays. Each id producer gets its own namespace (see [`FILL_ID_NAMESPACE`]) so the id
/// spaces cannot collide even for adversarially-chosen names.
const TRADE_ID_NAMESPACE: Uuid = Uuid::from_u128(0x6e5a_1c9d_3b74_4f02_9a88_c4d5_e6f7_0a1b);

/// Namespace for fill ids ([`fill_id`]).
const FILL_ID_NAMESPACE: Uuid = Uuid::from_u128(0x2b91_7f36_5d08_4ac1_b3e7_9d20_5c48_f16e);

/// The two deterministic leg ids of one transfer — UUIDv5 of the transfer id, one per side.
/// The same transfer id always yields the same pair, so a replayed transfer folds once
/// (its legs hit the ledger's seen-id guard and the database's `on conflict do nothing`).
pub fn transfer_leg_ids(transfer_id: Uuid) -> (Uuid, Uuid) {
    let leg = |side: &[u8]| {
        let mut name = transfer_id.as_bytes().to_vec();
        name.extend_from_slice(side);
        Uuid::new_v5(&TRADE_ID_NAMESPACE, &name)
    };
    (leg(b"/from"), leg(b"/to"))
}

/// The deterministic ledger id of one market fill — UUIDv5 over the trade's *own*
/// identity, `(exchange, trade_id)`, never over anything from this service's config: a
/// renamed gateway cannot mint new ids and double-count replays. `exchange` is PowerBot's
/// path segment (`nordpool`, `epex`, ...), which never contains `/`, so the joined name is
/// unambiguous.
pub fn fill_id(exchange: &str, trade_id: &str) -> Uuid {
    Uuid::new_v5(
        &FILL_ID_NAMESPACE,
        format!("{exchange}/{trade_id}").as_bytes(),
    )
}

/// Mirror of `contract_service::store::MarketType` (see the module docs on why a mirror).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum MarketType {
    Local,
    Xbid,
}

impl fmt::Display for MarketType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            MarketType::Local => f.write_str("local"),
            MarketType::Xbid => f.write_str("xbid"),
        }
    }
}

/// A contract's natural identity, mirroring `contract_service::store::ContractKey`:
/// Nord Pool contract ids are stable per venue but not assumed unique across the Local and
/// Xbid market segments, so `market_type` is part of the key.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ContractKey {
    pub market_type: MarketType,
    pub contract_id: i64,
}

impl fmt::Display for ContractKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}:{}", self.market_type, self.contract_id)
    }
}

/// One ledger entry: a signed quantity of one contract on one portfolio, at a price. A
/// transfer is two trades (`-quantity` on `from`, `+quantity` on `to`); a market fill is a single
/// trade with `venue_trade_id` set.
#[derive(Debug, Clone, PartialEq)]
pub struct Trade {
    /// Idempotency key. Transfer leg ids derive from the transfer id, fill ids from the
    /// wire trade's identity — deterministic either way, so replays dedupe.
    pub id: Uuid,
    pub portfolio: PortfolioId,
    pub contract: ContractKey,
    /// kW, signed: buy > 0, sell < 0 (1 MW = 1000 kW).
    pub quantity: Quantity,
    /// Minor currency units per MWh — fill price, or the transfer's price. Recorded, not
    /// folded.
    pub price: Price,
    /// When the fill executed on the exchange / the cross decided (unix nanos, matching
    /// the gateway wire's `exec_time_nanos`). Recorded, not folded.
    pub executed_at_nanos: i64,
    /// External fills: the venue's trade id; `None` for transfer legs.
    pub venue_trade_id: Option<String>,
    /// Transfer legs: the submitter's transfer id (canonical UUID string) — the mirror of
    /// `venue_trade_id`; `None` for fills. Not the idempotency key (the leg `id` already
    /// derives from it) — recorded for the Subscribe stream's write attribution and for
    /// queryability.
    pub transfer_id: Option<String>,
    /// External fills: the client order id the submitter stamped on the order that produced
    /// the fill (the gateway's `Trade.cl_order_id`, the ID Executor's per-order nonce); `None` for
    /// transfer legs. Additional write attribution, not a key — plays no part in id derivation
    /// or idempotency; carried so a submitter can recognize a fold as its own order's on the
    /// Subscribe stream.
    pub cl_order_id: Option<String>,
    /// Submitting component (crosser id, gateway name, ...).
    pub source: String,
}

/// Render a [`Price`] (EUR cents per MWh) as EUR: `4250` → `42.50`, `-50` → `-0.50`. Integer
/// arithmetic only — no float, so the two decimals are exact.
pub(crate) fn eur(cents: Price) -> String {
    let sign = if cents < 0 { "-" } else { "" };
    let abs = cents.unsigned_abs();
    format!("{sign}{}.{:02}", abs / 100, abs % 100)
}

impl fmt::Display for Trade {
    /// Every field, for the fill/transfer-leg audit log — a replayed id shows the same text.
    /// Quantity in kW, price in EUR per MWh (from the native cents).
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "id={} portfolio={} contract={} quantity={} kW price={} EUR/MWh \
             executed_at_nanos={} venue_trade_id={} transfer_id={} cl_order_id={} source={}",
            self.id,
            self.portfolio,
            self.contract,
            self.quantity,
            eur(self.price),
            self.executed_at_nanos,
            self.venue_trade_id.as_deref().unwrap_or("-"),
            self.transfer_id.as_deref().unwrap_or("-"),
            self.cl_order_id.as_deref().unwrap_or("-"),
            self.source,
        )
    }
}

/// The full current value of one `(portfolio, contract)` position. Subscribers replace by
/// highest `revision` — there are no deltas to fold client-side.
#[derive(Debug, Clone, PartialEq)]
pub struct Position {
    pub portfolio: PortfolioId,
    pub contract: ContractKey,
    /// kW, signed. A position that nets to zero still exists (with its revision) — going
    /// flat is a change subscribers must see.
    pub net_quantity: Quantity,
    /// Monotonic per position, starting at 1, bumped by every applied trade. Deterministic
    /// across restarts because the boot fold replays the log in `seq` order.
    pub revision: u64,
}

impl fmt::Display for Position {
    /// The full post-update value, for the "position updated" log. Net quantity in kW.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "portfolio={} contract={} net_quantity={} kW revision={}",
            self.portfolio, self.contract, self.net_quantity, self.revision,
        )
    }
}

/// One applied trade's effect: the full position value its fold produced, paired with the
/// causing trade's write attribution. The one shape every reader sees — the broadcast fans
/// a mutation's changes out as these, and the subscribe snapshot replays each pair's
/// history of them ([`PositionLedger::history`]), so a seed entry and a live change are
/// indistinguishable. Exactly one of `venue_trade_id` / `transfer_id` is set, matching
/// [`Trade`].
#[derive(Debug, Clone)]
pub struct PositionChange {
    pub position: Position,
    /// The fill's venue trade id; `None` for a transfer leg.
    pub venue_trade_id: Option<String>,
    /// The originating transfer id (canonical UUID string); `None` for a fill.
    pub transfer_id: Option<String>,
    /// The fill's originating client order id (nonce); `None` for a transfer leg.
    pub cl_order_id: Option<String>,
    /// Submitting component (gateway name, crosser id, ...).
    pub source: String,
    /// When the causing trade executed on the exchange / the cross decided (unix nanos).
    pub executed_at_nanos: i64,
}

impl PositionChange {
    /// Pair an applied trade with the position it produced — the one way both writers (the
    /// fill feeds and the transfer RPC) build a change, so no per-writer special case.
    pub fn new(trade: &Trade, position: Position) -> Self {
        Self {
            position,
            venue_trade_id: trade.venue_trade_id.clone(),
            transfer_id: trade.transfer_id.clone(),
            cl_order_id: trade.cl_order_id.clone(),
            source: trade.source.clone(),
            executed_at_nanos: trade.executed_at_nanos,
        }
    }
}

/// The in-memory projection: every position, folded from every trade ever applied.
#[derive(Debug, Default)]
pub struct PositionLedger {
    /// Positions per portfolio, per contract.
    positions: HashMap<PortfolioId, HashMap<ContractKey, Position>>,
    /// Every trade id ever applied — the idempotency guard, the in-memory mirror of the
    /// database's `on conflict (id) do nothing`.
    seen: HashSet<Uuid>,
    /// Every fold's [`PositionChange`] per position, in fold order — the history the
    /// subscribe snapshot replays. Unbounded on purpose, like `positions` and `seen`:
    /// these are intraday contracts, so a pair stops accruing when its contract expires —
    /// the log's own natural bound.
    history: HashMap<(PortfolioId, ContractKey), Vec<PositionChange>>,
}

impl PositionLedger {
    pub fn new() -> Self {
        Self::default()
    }

    /// Fold one trade in: add its signed quantity to the `(portfolio, contract)` position
    /// (creating it at zero first) and bump the position's revision. Returns the updated
    /// position, or `None` if the trade id was already applied — replays and re-seed
    /// overlaps are no-ops.
    pub fn apply(&mut self, trade: Trade) -> Option<Position> {
        if !self.seen.insert(trade.id) {
            return None;
        }
        let position = self
            .positions
            .entry(trade.portfolio.clone())
            .or_default()
            .entry(trade.contract)
            .or_insert_with(|| Position {
                portfolio: trade.portfolio.clone(),
                contract: trade.contract,
                net_quantity: 0,
                revision: 0,
            });
        position.net_quantity += trade.quantity;
        position.revision += 1;
        let position = position.clone();
        self.history
            .entry((position.portfolio.clone(), position.contract))
            .or_default()
            .push(PositionChange::new(&trade, position.clone()));
        Some(position)
    }

    pub fn get(&self, portfolio: &str, contract: &ContractKey) -> Option<&Position> {
        self.positions.get(portfolio)?.get(contract)
    }

    /// One portfolio's positions — the subscription snapshot source. Iteration order is
    /// unspecified (backed by a `HashMap`); an unknown portfolio yields nothing, matching
    /// the seed-empty subscribe semantics.
    pub fn positions(&self, portfolio: &str) -> impl Iterator<Item = &Position> {
        self.positions
            .get(portfolio)
            .into_iter()
            .flat_map(HashMap::values)
    }

    /// Total positions across all portfolios.
    pub fn len(&self) -> usize {
        self.positions.values().map(HashMap::len).sum()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Trades ever applied — boot-fold logging.
    pub fn applied_trades(&self) -> usize {
        self.seen.len()
    }

    /// Whether a trade id was already applied — how the server recognizes a replayed
    /// transfer before deciding to run its concurrency guard.
    pub fn is_applied(&self, id: &Uuid) -> bool {
        self.seen.contains(id)
    }

    /// The applied history of `(portfolio, contract)`, in fold order — each entry the
    /// position value one trade's fold produced, with that trade's attribution. Complete
    /// for the pair's lifetime (see the `history` field); the last entry is the current
    /// value, so replaying it seeds a subscriber exactly as if it had been subscribed
    /// since the pair's first trade.
    pub fn history(
        &self,
        portfolio: &str,
        contract: &ContractKey,
    ) -> impl Iterator<Item = &PositionChange> {
        self.history
            .get(&(portfolio.to_owned(), *contract))
            .into_iter()
            .flatten()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key(contract_id: i64) -> ContractKey {
        ContractKey {
            market_type: MarketType::Local,
            contract_id,
        }
    }

    fn trade(id: u128, portfolio: &str, contract_id: i64, quantity: Quantity) -> Trade {
        Trade {
            id: Uuid::from_u128(id),
            portfolio: portfolio.to_owned(),
            contract: key(contract_id),
            quantity,
            price: 4_250,
            executed_at_nanos: 1_000_000_000,
            venue_trade_id: None,
            transfer_id: None,
            cl_order_id: None,
            source: "test".to_owned(),
        }
    }

    #[test]
    fn apply_creates_the_position_at_revision_one() {
        // Given an empty ledger
        let mut ledger = PositionLedger::new();

        // When a first trade arrives
        let position = ledger.apply(trade(1, "execution-a", 7, 5_000)).unwrap();

        // Then the position holds the signed quantity at revision 1
        assert_eq!(position.portfolio, "execution-a");
        assert_eq!(position.contract, key(7));
        assert_eq!(position.net_quantity, 5_000);
        assert_eq!(position.revision, 1);
        assert_eq!(ledger.len(), 1);
    }

    #[test]
    fn reapplying_the_same_trade_is_a_noop() {
        // Given a ledger that already applied a trade (e.g. from the boot fold)
        let mut ledger = PositionLedger::new();
        ledger.apply(trade(1, "execution-a", 7, 5_000));

        // When the same trade id arrives again (a feed re-seed overlap)
        let replay = ledger.apply(trade(1, "execution-a", 7, 5_000));

        // Then nothing changes
        assert_eq!(replay, None);
        let stored = ledger.get("execution-a", &key(7)).unwrap();
        assert_eq!(stored.net_quantity, 5_000);
        assert_eq!(stored.revision, 1);
        assert_eq!(ledger.applied_trades(), 1);
    }

    #[test]
    fn revisions_are_monotonic_per_position() {
        // Given trades landing on one position, with another position in between
        let mut ledger = PositionLedger::new();
        ledger.apply(trade(1, "execution-a", 7, 5_000));
        ledger.apply(trade(2, "execution-a", 8, 1_000)); // other contract
        let second = ledger.apply(trade(3, "execution-a", 7, -2_000)).unwrap();
        let third = ledger.apply(trade(4, "execution-a", 7, 500)).unwrap();

        // Then the position's revision counts only its own trades
        assert_eq!(second.revision, 2);
        assert_eq!(third.revision, 3);
        assert_eq!(third.net_quantity, 3_500);
        assert_eq!(ledger.get("execution-a", &key(8)).unwrap().revision, 1);
    }

    #[test]
    fn transfer_legs_conserve_the_total_position() {
        // Given a position built from a fill on the execution portfolio
        let mut ledger = PositionLedger::new();
        ledger.apply(trade(1, "execution-a", 7, 10_000));

        // When a transfer moves 4 MW to a strategy portfolio (two zero-sum legs)
        ledger.apply(trade(2, "execution-a", 7, -4_000));
        ledger.apply(trade(3, "strategy-x", 7, 4_000));

        // Then the per-portfolio positions moved but the total is conserved
        let execution = ledger.get("execution-a", &key(7)).unwrap();
        let strategy = ledger.get("strategy-x", &key(7)).unwrap();
        assert_eq!(execution.net_quantity, 6_000);
        assert_eq!(strategy.net_quantity, 4_000);
        assert_eq!(execution.net_quantity + strategy.net_quantity, 10_000);
    }

    #[test]
    fn a_position_that_nets_to_zero_still_exists() {
        // Given a bought-then-sold contract
        let mut ledger = PositionLedger::new();
        ledger.apply(trade(1, "execution-a", 7, 5_000));
        let flat = ledger.apply(trade(2, "execution-a", 7, -5_000)).unwrap();

        // Then going flat is a visible change, not a disappearance
        assert_eq!(flat.net_quantity, 0);
        assert_eq!(flat.revision, 2);
        assert_eq!(ledger.len(), 1);
    }

    #[test]
    fn contracts_in_different_markets_are_distinct_positions() {
        // Given the same contract id trading in both market segments
        let mut ledger = PositionLedger::new();
        ledger.apply(trade(1, "execution-a", 7, 5_000));
        let mut xbid = trade(2, "execution-a", 7, 3_000);
        xbid.contract.market_type = MarketType::Xbid;
        ledger.apply(xbid);

        // Then they fold separately — market_type is part of the key
        assert_eq!(ledger.len(), 2);
        assert_eq!(
            ledger.get("execution-a", &key(7)).unwrap().net_quantity,
            5_000
        );
    }

    #[test]
    fn the_fold_is_deterministic() {
        // Given the same trade log applied to two ledgers in the same order (the boot
        // replay in `seq` order)
        let log = [
            trade(1, "execution-a", 7, 5_000),
            trade(2, "strategy-x", 7, 2_000),
            trade(3, "execution-a", 7, -1_500),
            trade(4, "execution-a", 8, 3_000),
        ];
        let mut first = PositionLedger::new();
        let mut second = PositionLedger::new();
        for t in &log {
            first.apply(t.clone());
            second.apply(t.clone());
        }

        // Then every position — quantity and revision — matches
        for t in &log {
            assert_eq!(
                first.get(&t.portfolio, &t.contract),
                second.get(&t.portfolio, &t.contract)
            );
        }
    }

    #[test]
    fn history_replays_each_fold_with_its_attribution() {
        // Given a fill and a transfer leg folding into one pair, and a fill on another
        let mut ledger = PositionLedger::new();
        let mut fill = trade(1, "execution-a", 7, 5_000);
        fill.venue_trade_id = Some("T-1".to_owned());
        fill.cl_order_id = Some("N-1".to_owned());
        ledger.apply(fill);
        let mut leg = trade(2, "execution-a", 7, -2_000);
        leg.transfer_id = Some("X-1".to_owned());
        leg.executed_at_nanos = 2_000_000_000;
        ledger.apply(leg);
        ledger.apply(trade(3, "strategy-x", 7, 1_000));

        // Then the pair's history holds one entry per fold, oldest first — each the
        // position value that fold produced, with the causing trade's attribution
        let history: Vec<_> = ledger.history("execution-a", &key(7)).collect();
        assert_eq!(history.len(), 2);
        assert_eq!(history[0].venue_trade_id.as_deref(), Some("T-1"));
        assert_eq!(history[0].cl_order_id.as_deref(), Some("N-1"));
        assert_eq!(history[0].position.net_quantity, 5_000);
        assert_eq!(history[0].position.revision, 1);
        assert_eq!(history[1].transfer_id.as_deref(), Some("X-1"));
        assert_eq!(history[1].position.net_quantity, 3_000);
        assert_eq!(history[1].position.revision, 2);
        // And the last entry is the pair's current value
        assert_eq!(
            &history[1].position,
            ledger.get("execution-a", &key(7)).unwrap()
        );
        // And the other pair holds only its own trade
        assert_eq!(ledger.history("strategy-x", &key(7)).count(), 1);
    }

    #[test]
    fn a_replayed_trade_records_no_history() {
        // Given a trade applied twice (a feed re-seed overlap)
        let mut ledger = PositionLedger::new();
        ledger.apply(trade(1, "execution-a", 7, 5_000));
        ledger.apply(trade(1, "execution-a", 7, 5_000));

        // Then its fold is in the history once — the no-op replay records nothing
        assert_eq!(ledger.history("execution-a", &key(7)).count(), 1);
    }

    #[test]
    fn transfer_leg_ids_are_deterministic_and_distinct() {
        // Given one transfer id
        let transfer_id = Uuid::from_u128(42);

        // Then its leg ids are stable across calls (replay dedup) and differ per side
        let (from_first, to_first) = transfer_leg_ids(transfer_id);
        let (from_second, to_second) = transfer_leg_ids(transfer_id);
        assert_eq!(from_first, from_second);
        assert_eq!(to_first, to_second);
        assert_ne!(from_first, to_first);
        // And a different transfer mints different legs
        let (other_from, _) = transfer_leg_ids(Uuid::from_u128(43));
        assert_ne!(from_first, other_from);
    }

    #[test]
    fn fill_ids_are_deterministic_and_namespaced_per_producer() {
        // Given the same wire identity
        // Then the fill id is stable across calls (replay dedup)...
        assert_eq!(fill_id("nordpool", "T-1"), fill_id("nordpool", "T-1"));
        // ...and distinct per identity and per exchange
        assert_ne!(fill_id("nordpool", "T-1"), fill_id("nordpool", "T-2"));
        assert_ne!(fill_id("nordpool", "T-1"), fill_id("epex", "T-1"));
    }

    #[test]
    fn display_carries_units_and_exact_eur() {
        // Whole, sub-euro, and negative prices all render with two exact decimals
        assert_eq!(eur(4_250), "42.50");
        assert_eq!(eur(5), "0.05");
        assert_eq!(eur(-4_250), "-42.50");
        assert_eq!(eur(-5), "-0.05");

        // A fill renders every field with its unit (kW, EUR/MWh)
        let mut fill = trade(1, "execution-a", 7, 5_000);
        fill.venue_trade_id = Some("T-1".to_owned());
        fill.cl_order_id = Some("N-1".to_owned());
        let shown = fill.to_string();
        assert!(shown.contains("quantity=5000 kW"), "{shown}");
        assert!(shown.contains("price=42.50 EUR/MWh"), "{shown}");
        assert!(shown.contains("contract=local:7"), "{shown}");
        assert!(shown.contains("venue_trade_id=T-1"), "{shown}");
        assert!(shown.contains("transfer_id=-"), "{shown}");
        assert!(shown.contains("cl_order_id=N-1"), "{shown}");

        // A transfer leg shows its transfer id and the venue-id placeholder; a position
        // shows kW
        let mut leg = trade(2, "execution-a", 7, -5_000);
        leg.transfer_id = Some("X-1".to_owned());
        let shown = leg.to_string();
        assert!(shown.contains("venue_trade_id=-"), "{shown}");
        assert!(shown.contains("transfer_id=X-1"), "{shown}");
        assert!(shown.contains("cl_order_id=-"), "{shown}");
        let position = Position {
            portfolio: "execution-a".to_owned(),
            contract: key(7),
            net_quantity: -5_000,
            revision: 3,
        };
        assert_eq!(
            position.to_string(),
            "portfolio=execution-a contract=local:7 net_quantity=-5000 kW revision=3"
        );
    }

    #[test]
    fn positions_iterates_one_portfolio_and_unknown_seeds_empty() {
        // Given positions across two portfolios
        let mut ledger = PositionLedger::new();
        ledger.apply(trade(1, "execution-a", 7, 5_000));
        ledger.apply(trade(2, "execution-a", 8, 1_000));
        ledger.apply(trade(3, "strategy-x", 7, 2_000));

        // Then iteration is scoped to the asked portfolio
        assert_eq!(ledger.positions("execution-a").count(), 2);
        assert_eq!(ledger.positions("strategy-x").count(), 1);
        // And an unknown portfolio yields nothing (the seed-empty subscribe semantics)
        assert_eq!(ledger.positions("unknown").count(), 0);
    }
}
