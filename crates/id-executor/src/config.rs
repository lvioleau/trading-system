//! Configuration for the ID Executor, loaded once from an INI file (same scheme as the
//! services and the nordpool-connector).
//!
//! The file path is `ID_EXECUTOR_CONFIG` (default `id-executor.ini`). Every value may embed
//! `${VAR}` or `${VAR:-default}` references, substituted at load time by the shared
//! `flow-util` config mechanics from the process environment.
//!
//! The parsed shape is **one executor, one delivery area, N execution portfolios**: one
//! `[id-executor]` section, with `portfolios` a plain list. A portfolio id and the PowerBot
//! portfolio id are the same thing — one identifier end to end: the Position Service
//! subscription scope, the id stamped on the portfolio's orders, the scope of its gateway
//! subscriptions, and the key attributing its returning fills. A duplicate entry is fatal
//! (a double-subscribed portfolio would double-execute its gap). Portfolio strings are not
//! validated against the Position Service: an unknown one subscribes, seeds empty, and
//! never updates — a configured portfolio that never trades, caught by observation, not at
//! load.

use std::net::SocketAddr;
use std::time::Duration;

use anyhow::{Context, Result, bail};
use configparser::ini::Ini;
use eic::EicCode;
use flow_util::config::{RawConfig, config_path, load_ini, opt_ms, parse_list, req_non_empty};

pub use flow_util::config::FeedConfig;

/// A portfolio identifier — a plain string, the repo-wide portfolio-as-string decision.
pub type PortfolioId = String;

#[derive(Debug, Clone)]
pub struct Config {
    /// Position Service gRPC endpoint — one `Subscribe(portfolio)` per configured portfolio
    /// (the gaps to flatten), e.g. `http://localhost:50053`. The executor is *not* a
    /// Position Service writer: no `Transfer` client anywhere.
    pub position_endpoint: String,
    /// Contract Service gRPC endpoint — `SubscribeContracts(delivery_area_eic)`, the
    /// tradeability gate, e.g. `http://localhost:50054`.
    pub contract_endpoint: String,
    /// PowerBot Gateway gRPC endpoint — `AddOrder` plus the per-portfolio own-orders /
    /// own-trades pairs, e.g. `http://localhost:50052`. One executor, one gateway.
    pub gateway_endpoint: String,
    /// nordpool-connector gRPC endpoint serving the depth stream the orders are priced from,
    /// e.g. `http://localhost:50051`.
    pub connector_endpoint: String,
    /// The exchange stamped on every `AddOrder` (the gateway serves one venue).
    pub exchange: String,
    /// The executor's single delivery area, numeric id — the connector's depth subscribe
    /// guard (`NOT_FOUND` at boot means the wrong connector) and the contract subscription
    /// filter, and the gateway's `OrderEntry.delivery_area` — one ENTSO-E EIC code end to
    /// end. Shared by every portfolio; a second area is a second executor.
    pub delivery_area_eic: EicCode,
    /// The execution portfolios this executor owns (≥1, all in the one area), config order
    /// preserved. A portfolio id is also the PowerBot portfolio id — the same string scopes
    /// the Position Service subscription, is stamped on the portfolio's orders, scopes its
    /// gateway subscriptions, and attributes its returning fills. Each portfolio is traded
    /// by exactly one executor — a deployment invariant stated here, enforced nowhere.
    pub portfolios: Vec<PortfolioId>,
    /// Listen address for the executor's own endpoint — health + the enable/disable admin
    /// (step 8), e.g. `0.0.0.0:50073`. Nothing is served yet; parsed and validated from the
    /// start so the key cannot rot in shipped configs.
    pub listen_addr: SocketAddr,
    /// Feed tuning, shared by every subscription (position, contract, depth, gateway); every
    /// key is optional in the INI, defaulting per [`FeedConfig`].
    pub feed: FeedConfig,
    /// Execution tuning — area-wide: one set, applied to every portfolio; every key is
    /// optional in the INI, defaulting per [`ExecutionConfig`].
    pub execution: ExecutionConfig,
}

/// `[id-executor]` execution tuning (`docs/id-executor-implementation.md`, invariants 2–6).
/// Area-wide — one set for every portfolio. Quantities are venue-native integer kW; every
/// duration is a positive millisecond count.
#[derive(Debug, Clone, Copy)]
pub struct ExecutionConfig {
    /// Stop trading a contract this long before its delivery start (`trading_cutoff_ms`);
    /// past it a standing gap is alarmed as going to imbalance, never worked.
    pub trading_cutoff: Duration,
    /// Per-order quantity cap in kW (`max_order_quantity_kw`).
    pub max_order_quantity_kw: i64,
    /// Venue minimum order quantity in kW (`min_order_quantity_kw`); a smaller gap is
    /// withheld (and alarmed if it persists against a liquid book).
    pub min_order_quantity_kw: i64,
    /// Order quantities are floored to this grid (`quantity_step_kw`).
    pub quantity_step_kw: i64,
    /// Per-(portfolio, contract) minimum spacing since that pair's last send
    /// (`order_cooldown_ms`) — the guard against a send→zero-fill→send hot loop.
    pub order_cooldown: Duration,
    /// Per-(portfolio, contract) max orders per window (`order_rate_max`).
    pub order_rate_max: u32,
    /// The window `order_rate_max` is measured over (`order_rate_window_ms`).
    pub order_rate_window: Duration,
    /// Executor-wide max orders per window (`global_order_rate_max`) — the shared
    /// gateway/PowerBot-REST budget.
    pub global_order_rate_max: u32,
    /// The window `global_order_rate_max` is measured over (`global_order_rate_window_ms`).
    pub global_order_rate_window: Duration,
    /// An in-flight order still outstanding for this long is warned about — a soft,
    /// state-changing-nothing heads-up (`order_inflight_warn_ms`) that a send has not
    /// reconciled (no terminal yet, or terminal seen but its position fold still due), well
    /// before `order_resolve_timeout` declares it dead. Must be ≤ that timeout.
    pub order_inflight_warn: Duration,
    /// An in-flight order without a terminal own-orders revision for this long is resolved
    /// as dead, with an alarm (`order_resolve_timeout_ms`).
    pub order_resolve_timeout: Duration,
    /// Blocked/idle eval-snapshot log cadence per (portfolio, contract)
    /// (`eval_log_throttle_ms`).
    pub eval_log_throttle: Duration,
}

impl Default for ExecutionConfig {
    fn default() -> Self {
        Self {
            trading_cutoff: Duration::from_secs(3600),
            max_order_quantity_kw: 10_000,
            min_order_quantity_kw: 100,
            quantity_step_kw: 100,
            order_cooldown: Duration::from_millis(2_000),
            order_rate_max: 5,
            order_rate_window: Duration::from_millis(10_000),
            global_order_rate_max: 50,
            global_order_rate_window: Duration::from_millis(10_000),
            order_inflight_warn: Duration::from_millis(3_000),
            order_resolve_timeout: Duration::from_millis(10_000),
            eval_log_throttle: Duration::from_millis(1_000),
        }
    }
}

impl Config {
    /// Load from the path in `ID_EXECUTOR_CONFIG`, defaulting to `id-executor.ini`.
    pub fn load() -> Result<Self> {
        Self::from_path(&config_path("ID_EXECUTOR_CONFIG", "id-executor.ini"))
    }

    /// Load from a specific INI file path.
    pub fn from_path(path: &str) -> Result<Self> {
        Self::from_ini(load_ini(path)?).with_context(|| format!("parsing config from {path}"))
    }

    fn from_ini(ini: Ini) -> Result<Self> {
        let r = RawConfig::new(ini);
        let s = "id-executor";
        // A portfolio id is also the PowerBot portfolio id — the fill-attribution key —
        // and `parse_list` already rejects a duplicate (a double-subscribed portfolio
        // would double-execute its gap).
        let portfolios =
            parse_list(&r.req(s, "portfolios")?).context("parsing [id-executor] portfolios")?;
        if portfolios.is_empty() {
            bail!("[id-executor] portfolios must list at least one execution portfolio");
        }
        Ok(Self {
            position_endpoint: req_non_empty(&r, s, "position_endpoint")?,
            contract_endpoint: req_non_empty(&r, s, "contract_endpoint")?,
            gateway_endpoint: req_non_empty(&r, s, "gateway_endpoint")?,
            connector_endpoint: req_non_empty(&r, s, "connector_endpoint")?,
            exchange: req_non_empty(&r, s, "exchange")?,
            delivery_area_eic: {
                let raw = req_non_empty(&r, s, "delivery_area_eic")?;
                EicCode::from_eic(&raw).with_context(|| {
                    format!("[id-executor] delivery_area_eic {raw:?} is not a known EIC")
                })?
            },
            portfolios,
            listen_addr: r
                .req(s, "listen_addr")?
                .parse()
                .context("parsing [id-executor] listen_addr as a socket address")?,
            feed: FeedConfig::parse(&r, s)?,
            execution: parse_execution(&r, s)?,
        })
    }
}

/// Parse the optional execution tuning keys, defaulting per [`ExecutionConfig`], then check
/// the cross-key orderings: `min_order_quantity_kw ≤ max_order_quantity_kw` (no order size
/// could satisfy both otherwise) and `quantity_step_kw ≤ min_order_quantity_kw` (flooring to
/// a grid coarser than the minimum could strand every sub-max gap below it).
fn parse_execution(r: &RawConfig, s: &str) -> Result<ExecutionConfig> {
    let d = ExecutionConfig::default();
    let cfg = ExecutionConfig {
        trading_cutoff: opt_ms(r, s, "trading_cutoff_ms", d.trading_cutoff)?,
        max_order_quantity_kw: opt_pos(r, s, "max_order_quantity_kw", d.max_order_quantity_kw)?,
        min_order_quantity_kw: opt_pos(r, s, "min_order_quantity_kw", d.min_order_quantity_kw)?,
        quantity_step_kw: opt_pos(r, s, "quantity_step_kw", d.quantity_step_kw)?,
        order_cooldown: opt_ms(r, s, "order_cooldown_ms", d.order_cooldown)?,
        order_rate_max: opt_pos(r, s, "order_rate_max", i64::from(d.order_rate_max))?
            .try_into()
            .with_context(|| format!("[{s}] order_rate_max is out of range"))?,
        order_rate_window: opt_ms(r, s, "order_rate_window_ms", d.order_rate_window)?,
        global_order_rate_max: opt_pos(
            r,
            s,
            "global_order_rate_max",
            i64::from(d.global_order_rate_max),
        )?
        .try_into()
        .with_context(|| format!("[{s}] global_order_rate_max is out of range"))?,
        global_order_rate_window: opt_ms(
            r,
            s,
            "global_order_rate_window_ms",
            d.global_order_rate_window,
        )?,
        order_inflight_warn: opt_ms(r, s, "order_inflight_warn_ms", d.order_inflight_warn)?,
        order_resolve_timeout: opt_ms(r, s, "order_resolve_timeout_ms", d.order_resolve_timeout)?,
        eval_log_throttle: opt_ms(r, s, "eval_log_throttle_ms", d.eval_log_throttle)?,
    };
    if cfg.min_order_quantity_kw > cfg.max_order_quantity_kw {
        bail!(
            "[{s}] min_order_quantity_kw ({}) exceeds max_order_quantity_kw ({}) — \
             no order size could satisfy both",
            cfg.min_order_quantity_kw,
            cfg.max_order_quantity_kw,
        );
    }
    if cfg.quantity_step_kw > cfg.min_order_quantity_kw {
        bail!(
            "[{s}] quantity_step_kw ({}) exceeds min_order_quantity_kw ({}) — \
             flooring to a grid coarser than the minimum makes the minimum unreachable",
            cfg.quantity_step_kw,
            cfg.min_order_quantity_kw,
        );
    }
    if cfg.order_inflight_warn > cfg.order_resolve_timeout {
        bail!(
            "[{s}] order_inflight_warn_ms ({} ms) exceeds order_resolve_timeout_ms ({} ms) — \
             the soft warn must fire before the order is declared dead, or it never fires",
            cfg.order_inflight_warn.as_millis(),
            cfg.order_resolve_timeout.as_millis(),
        );
    }
    Ok(cfg)
}

/// Parse an optional `[section] key` as a positive integer, falling back to `default` when
/// the key is absent. A present-but-non-integer value is a fatal config error, and so is
/// zero or negative: every count and quantity here is a cap or a grid where 0 means "no
/// order can ever send", never a sensible setting.
fn opt_pos(r: &RawConfig, section: &str, key: &str, default: i64) -> Result<i64> {
    match r.opt(section, key)? {
        Some(value) => {
            let n = value
                .parse::<i64>()
                .with_context(|| format!("parsing [{section}] {key} as an integer"))?;
            if n <= 0 {
                bail!("[{section}] {key} must be a positive integer");
            }
            Ok(n)
        }
        None => Ok(default),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn from_str(contents: &str) -> Result<Config> {
        let mut ini = Ini::new();
        ini.read(contents.to_string()).unwrap();
        Config::from_ini(ini)
    }

    #[test]
    fn parses_a_full_config_using_env_defaults() {
        // Given the full SAMPLE INI, whose `${VAR}` references carry `:-` defaults (so the
        // test resolves without touching the process environment).
        let cfg = from_str(SAMPLE).unwrap();
        assert_eq!(cfg.position_endpoint, "http://localhost:50053");
        assert_eq!(cfg.contract_endpoint, "http://localhost:50054");
        assert_eq!(cfg.gateway_endpoint, "http://localhost:50052");
        assert_eq!(cfg.connector_endpoint, "http://localhost:50051");
        assert_eq!(cfg.exchange, "nordpool");
        assert_eq!(cfg.delivery_area_eic, EicCode::AREA_FR);
        assert_eq!(
            cfg.listen_addr,
            "0.0.0.0:50073".parse::<SocketAddr>().unwrap()
        );
        // The portfolios parse as a plain list in config order — each id is also the
        // PowerBot portfolio id, so there is nothing else to map.
        assert_eq!(cfg.portfolios, vec!["execution-a", "execution-b"]);
        // The tuning keys are absent, so the code defaults apply.
        assert_eq!(cfg.feed.initial_backoff, Duration::from_millis(500));
        assert_eq!(cfg.execution.trading_cutoff, Duration::from_secs(3600));
        assert_eq!(cfg.execution.max_order_quantity_kw, 10_000);
        assert_eq!(cfg.execution.min_order_quantity_kw, 100);
        assert_eq!(cfg.execution.quantity_step_kw, 100);
        assert_eq!(cfg.execution.order_cooldown, Duration::from_secs(2));
        assert_eq!(cfg.execution.order_rate_max, 5);
        assert_eq!(cfg.execution.order_rate_window, Duration::from_secs(10));
        assert_eq!(cfg.execution.global_order_rate_max, 50);
        assert_eq!(
            cfg.execution.global_order_rate_window,
            Duration::from_secs(10)
        );
        assert_eq!(cfg.execution.order_inflight_warn, Duration::from_secs(3));
        assert_eq!(cfg.execution.order_resolve_timeout, Duration::from_secs(10));
        assert_eq!(cfg.execution.eval_log_throttle, Duration::from_secs(1));
    }

    #[test]
    fn tuning_keys_override_the_defaults() {
        // The tuning keys live in [id-executor], so insert before another of its keys
        // (appending after the portfolio sections would land them in the wrong section).
        let cfg = from_str(&SAMPLE.replace(
            "listen_addr        =",
            "trading_cutoff_ms = 1800000\nmax_order_quantity_kw = 5000\n\
             order_rate_max = 2\nfeed_initial_backoff_ms = 100\nlisten_addr        =",
        ))
        .unwrap();
        assert_eq!(cfg.execution.trading_cutoff, Duration::from_secs(1800));
        assert_eq!(cfg.execution.max_order_quantity_kw, 5_000);
        assert_eq!(cfg.execution.order_rate_max, 2);
        assert_eq!(cfg.feed.initial_backoff, Duration::from_millis(100));
        // Untouched keys keep their defaults.
        assert_eq!(cfg.execution.min_order_quantity_kw, 100);
        assert_eq!(cfg.feed.max_backoff, Duration::from_secs(30));
    }

    #[test]
    fn empty_portfolios_is_a_fatal_config_error() {
        let err = from_str(&SAMPLE.replace(
            "portfolios         = ${PORTFOLIOS_CFG_UNSET:-execution-a, execution-b}",
            "portfolios         = ${PORTFOLIOS_CFG_UNSET:-, }",
        ))
        .unwrap_err();
        assert!(err.to_string().contains("portfolios"), "{err}");
    }

    #[test]
    fn a_duplicated_portfolio_is_a_fatal_config_error() {
        // The portfolio id is also the fill-attribution key; listed twice, the executor
        // would double-subscribe and double-execute its gap. Always a typo.
        let err = from_str(&SAMPLE.replace(
            "portfolios         = ${PORTFOLIOS_CFG_UNSET:-execution-a, execution-b}",
            "portfolios         = ${PORTFOLIOS_CFG_UNSET:-execution-a, execution-a}",
        ))
        .unwrap_err();
        // The cause ("duplicate entry ...") sits under the parse context — read the chain.
        let message = format!("{err:#}");
        assert!(message.contains("duplicate"), "{message}");
        assert!(message.contains("execution-a"), "{message}");
    }

    #[test]
    fn empty_delivery_area_eic_is_a_fatal_config_error() {
        let err = from_str(&SAMPLE.replace(
            "delivery_area_eic  = ${EIC_CFG_UNSET:-10YFR-RTE------C}",
            "delivery_area_eic  = ${EIC_CFG_UNSET:-}",
        ))
        .unwrap_err();
        assert!(err.to_string().contains("delivery_area_eic"), "{err}");
    }

    #[test]
    fn empty_exchange_is_a_fatal_config_error() {
        let err = from_str(&SAMPLE.replace(
            "exchange           = nordpool",
            "exchange           = ${EXCHANGE_CFG_UNSET:-}",
        ))
        .unwrap_err();
        assert!(err.to_string().contains("exchange"), "{err}");
    }

    #[test]
    fn min_order_quantity_above_max_is_a_fatal_config_error() {
        // No order size could satisfy both bounds.
        let err = from_str(&SAMPLE.replace(
            "listen_addr        =",
            "min_order_quantity_kw = 20000\nlisten_addr        =",
        ))
        .unwrap_err();
        assert!(err.to_string().contains("min_order_quantity_kw"), "{err}");
    }

    #[test]
    fn quantity_step_above_min_order_quantity_is_a_fatal_config_error() {
        // A grid coarser than the venue minimum makes the minimum unreachable.
        let err = from_str(&SAMPLE.replace(
            "listen_addr        =",
            "quantity_step_kw = 500\nmin_order_quantity_kw = 100\nlisten_addr        =",
        ))
        .unwrap_err();
        assert!(err.to_string().contains("quantity_step_kw"), "{err}");
    }

    #[test]
    fn inflight_warn_above_resolve_timeout_is_a_fatal_config_error() {
        // A warn scheduled after the order is already declared dead would never fire.
        let err = from_str(&SAMPLE.replace(
            "listen_addr        =",
            "order_inflight_warn_ms = 15000\norder_resolve_timeout_ms = 10000\n\
             listen_addr        =",
        ))
        .unwrap_err();
        assert!(err.to_string().contains("order_inflight_warn_ms"), "{err}");
    }

    #[test]
    fn zero_tuning_value_is_a_fatal_config_error() {
        // Zero is never sensible here: a zero interval is a hot loop, a zero cap or grid
        // means no order can ever send. One duration, one quantity, one count.
        for (key, zero) in [
            ("order_cooldown_ms", "order_cooldown_ms = 0"),
            ("max_order_quantity_kw", "max_order_quantity_kw = 0"),
            ("order_rate_max", "order_rate_max = 0"),
        ] {
            let err = from_str(&SAMPLE.replace(
                "listen_addr        =",
                &format!("{zero}\nlisten_addr        ="),
            ))
            .unwrap_err();
            assert!(err.to_string().contains(key), "{key}: {err}");
        }
    }

    #[test]
    fn missing_key_is_a_fatal_config_error() {
        let err = from_str("[id-executor]\nportfolios = p\n").unwrap_err();
        assert!(err.to_string().contains("position_endpoint"), "{err}");
    }

    const SAMPLE: &str = "\
[id-executor]
position_endpoint  = ${POSITION_ENDPOINT_CFG_UNSET:-http://localhost:50053}
contract_endpoint  = ${CONTRACT_ENDPOINT_CFG_UNSET:-http://localhost:50054}
gateway_endpoint   = ${GATEWAY_ENDPOINT_CFG_UNSET:-http://localhost:50052}
connector_endpoint = ${CONNECTOR_ENDPOINT_CFG_UNSET:-http://localhost:50051}
exchange           = nordpool
delivery_area_eic  = ${EIC_CFG_UNSET:-10YFR-RTE------C}
portfolios         = ${PORTFOLIOS_CFG_UNSET:-execution-a, execution-b}
listen_addr        = ${LISTEN_ADDR_CFG_UNSET:-0.0.0.0:50073}
";
}
