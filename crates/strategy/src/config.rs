//! Configuration for the mock strategy, loaded once from an INI file (same scheme as the
//! services and the nordpool-connector).
//!
//! The file path is `STRATEGY_CONFIG` (default `strategy.ini`). Every value may embed `${VAR}` or
//! `${VAR:-default}` references, substituted at load time by the shared `flow-util` config
//! mechanics from the process environment.
//!
//! The cradle subscribes to the Contract Service for one delivery area, to the connector's
//! depth stream (the strategy input's books and the emitted targets' mids), and to the Position
//! Service (the input's actual-position baseline); it maintains a configured number of tradable
//! contracts (active and expiring far enough out, replaced as they expire) and runs the strategy
//! each tick. It produces exactly one `portfolio` — one strategy per portfolio, by design — and
//! a `SubscribeTargets` request naming any other is rejected.

use std::net::SocketAddr;
use std::time::Duration;

use anyhow::{Context, Result, bail};
use configparser::ini::Ini;
use eic::EicCode;
use flow_util::config::{RawConfig, config_path, load_ini, opt_ms};

#[derive(Debug, Clone)]
pub struct Config {
    /// This strategy's identity — logged, and what the crosser names in its subscribe request.
    pub strategy: String,
    /// The one portfolio this strategy produces (one strategy per portfolio); a subscription for
    /// any other portfolio is rejected.
    pub portfolio: String,
    /// Listen address for the mock's gRPC API, e.g. `0.0.0.0:50071`.
    pub listen_addr: SocketAddr,
    /// How often the tradable set is refreshed and each contract's target re-sampled and emitted.
    pub tick: Duration,
    /// Contract Service gRPC endpoint serving `SubscribeContracts`.
    pub contract_endpoint: String,
    /// nordpool-connector gRPC endpoint serving the depth stream — the strategy input's books,
    /// and the live top-of-book mid every emitted target books.
    pub connector_endpoint: String,
    /// Position Service gRPC endpoint serving `Subscribe` — the strategy input's actual-position
    /// baseline for the produced portfolio.
    pub position_endpoint: String,
    /// The delivery area whose live contract set and depth stream are subscribed.
    pub delivery_area_eic: EicCode,
    /// How many tradable contracts to maintain (the target count).
    pub contracts: usize,
    /// The oscillation amplitude (kW) applied to every tradable contract.
    pub amplitude: i64,
    /// A contract is tradable only while its delivery starts at least this far out: the strategy
    /// stops moving the position (no more transfers) once delivery is within this window. Defaults
    /// to 1h.
    pub min_time_to_delivery: Duration,
    /// Path to the Python strategy module run behind the [`Strategy`](crate::driver::Strategy)
    /// seam — `py/strategy.py` documents the contract and is the reference mock.
    pub python: String,
}

impl Config {
    /// Load from the path in `STRATEGY_CONFIG`, defaulting to `strategy.ini`.
    pub fn load() -> Result<Self> {
        Self::from_path(&config_path("STRATEGY_CONFIG", "strategy.ini"))
    }

    /// Load from a specific INI file path.
    pub fn from_path(path: &str) -> Result<Self> {
        Self::from_ini(load_ini(path)?).with_context(|| format!("parsing config from {path}"))
    }

    fn from_ini(ini: Ini) -> Result<Self> {
        let r = RawConfig::new(ini);
        let s = "strategy";
        let strategy = r.req(s, "strategy")?;
        let portfolio = r.req(s, "portfolio")?;
        if portfolio.trim().is_empty() {
            bail!("[strategy] portfolio must not be empty");
        }
        let listen_addr = r
            .req(s, "listen_addr")?
            .parse()
            .context("parsing [strategy] listen_addr as a socket address")?;
        let tick = opt_ms(&r, s, "tick_ms", Duration::from_secs(30))?;
        let contract_endpoint = r.req(s, "contract_endpoint")?;
        let connector_endpoint = r.req(s, "connector_endpoint")?;
        let position_endpoint = r.req(s, "position_endpoint")?;
        let raw_eic = r.req(s, "delivery_area_eic")?;
        let delivery_area_eic = EicCode::from_eic(&raw_eic).with_context(|| {
            format!("[strategy] delivery_area_eic {raw_eic:?} is not a known EIC")
        })?;
        let contracts = r
            .req(s, "contracts")?
            .parse::<usize>()
            .context("parsing [strategy] contracts")?;
        if contracts == 0 {
            bail!("[strategy] contracts must be at least 1");
        }
        let amplitude = r
            .req(s, "amplitude")?
            .parse::<i64>()
            .context("parsing [strategy] amplitude")?;
        // No-trade window before delivery start; 0 (trade right up to delivery) is allowed, so a
        // dedicated parse.
        let min_time_to_delivery = match r.opt(s, "min_time_to_delivery_ms")? {
            Some(value) => Duration::from_millis(
                value
                    .parse::<u64>()
                    .context("parsing [strategy] min_time_to_delivery_ms as milliseconds")?,
            ),
            None => Duration::from_secs(3600),
        };
        let python = r.req(s, "python")?;
        Ok(Self {
            strategy,
            portfolio,
            listen_addr,
            tick,
            contract_endpoint,
            connector_endpoint,
            position_endpoint,
            delivery_area_eic,
            contracts,
            amplitude,
            min_time_to_delivery,
            python,
        })
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
        let cfg = from_str(SAMPLE).unwrap();
        assert_eq!(cfg.strategy, "strategy-x");
        assert_eq!(cfg.portfolio, "strat-batt-1");
        assert_eq!(
            cfg.listen_addr,
            "0.0.0.0:50071".parse::<SocketAddr>().unwrap()
        );
        assert_eq!(cfg.contract_endpoint, "http://localhost:50052");
        assert_eq!(cfg.connector_endpoint, "http://localhost:50051");
        assert_eq!(cfg.position_endpoint, "http://localhost:50053");
        assert_eq!(cfg.delivery_area_eic, EicCode::AREA_FR);
        assert_eq!(cfg.contracts, 5);
        assert_eq!(cfg.amplitude, 10_000);
        assert_eq!(cfg.python, "crates/strategy/py/strategy.py");
        // tick_ms and min_time_to_delivery_ms are absent → their defaults.
        assert_eq!(cfg.tick, Duration::from_secs(30));
        assert_eq!(cfg.min_time_to_delivery, Duration::from_secs(3600));
    }

    #[test]
    fn missing_python_module_is_a_fatal_config_error() {
        let err = from_str(&SAMPLE.replace(
            "python            = ${STRATEGY_PYTHON_CFG_UNSET:-crates/strategy/py/strategy.py}\n",
            "",
        ))
        .unwrap_err();
        assert!(err.to_string().contains("python"), "{err}");
    }

    #[test]
    fn tick_and_min_time_to_delivery_override_the_defaults() {
        let cfg = from_str(&SAMPLE.replace(
            "contracts         = 5",
            "tick_ms = 500\nmin_time_to_delivery_ms = 900000\ncontracts         = 5",
        ))
        .unwrap();
        assert_eq!(cfg.tick, Duration::from_millis(500));
        assert_eq!(cfg.min_time_to_delivery, Duration::from_millis(900_000));
    }

    #[test]
    fn missing_contract_endpoint_is_a_fatal_config_error() {
        let err = from_str(&SAMPLE.replace(
            "contract_endpoint = ${CONTRACT_ENDPOINT_CFG_UNSET:-http://localhost:50052}\n",
            "",
        ))
        .unwrap_err();
        assert!(err.to_string().contains("contract_endpoint"), "{err}");
    }

    #[test]
    fn missing_connector_endpoint_is_a_fatal_config_error() {
        let err = from_str(&SAMPLE.replace(
            "connector_endpoint = ${CONNECTOR_ENDPOINT_CFG_UNSET:-http://localhost:50051}\n",
            "",
        ))
        .unwrap_err();
        assert!(err.to_string().contains("connector_endpoint"), "{err}");
    }

    #[test]
    fn missing_position_endpoint_is_a_fatal_config_error() {
        let err = from_str(&SAMPLE.replace(
            "position_endpoint = ${POSITION_ENDPOINT_CFG_UNSET:-http://localhost:50053}\n",
            "",
        ))
        .unwrap_err();
        assert!(err.to_string().contains("position_endpoint"), "{err}");
    }

    #[test]
    fn zero_contracts_is_a_fatal_config_error() {
        let err = from_str(&SAMPLE.replace("contracts         = 5", "contracts         = 0"))
            .unwrap_err();
        assert!(err.to_string().contains("contracts"), "{err}");
    }

    #[test]
    fn empty_portfolio_is_a_fatal_config_error() {
        let err = from_str(&SAMPLE.replace(
            "portfolio         = ${PORTFOLIO_CFG_UNSET:-strat-batt-1}",
            "portfolio         = ${PORTFOLIO_CFG_UNSET:- }",
        ))
        .unwrap_err();
        assert!(err.to_string().contains("portfolio"), "{err}");
    }

    #[test]
    fn missing_key_is_a_fatal_config_error() {
        let err = from_str("[strategy]\nstrategy = s\nportfolio = p\n").unwrap_err();
        assert!(err.to_string().contains("listen_addr"), "{err}");
    }

    const SAMPLE: &str = "\
[strategy]
strategy          = ${STRATEGY_CFG_UNSET:-strategy-x}
portfolio         = ${PORTFOLIO_CFG_UNSET:-strat-batt-1}
listen_addr       = ${LISTEN_ADDR_CFG_UNSET:-0.0.0.0:50071}
contract_endpoint = ${CONTRACT_ENDPOINT_CFG_UNSET:-http://localhost:50052}
connector_endpoint = ${CONNECTOR_ENDPOINT_CFG_UNSET:-http://localhost:50051}
position_endpoint = ${POSITION_ENDPOINT_CFG_UNSET:-http://localhost:50053}
delivery_area_eic = 10YFR-RTE------C
contracts         = 5
amplitude         = 10000
python            = ${STRATEGY_PYTHON_CFG_UNSET:-crates/strategy/py/strategy.py}
";
}
