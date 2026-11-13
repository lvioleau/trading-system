//! Configuration for the crosser, loaded once from an INI file (same scheme as the services
//! and the nordpool-connector).
//!
//! The file path is `CROSSER_CONFIG` (default `crosser.ini`). Every value may embed `${VAR}` or
//! `${VAR:-default}` references, substituted at load time by the shared `flow-util` config
//! mechanics from the process environment.
//!
//! The parsed shape is a strategy-portfolio → execution-portfolio map: `portfolios` lists the
//! strategy portfolios this crosser handles (exactly what it subscribes for, on the strategy and
//! on the Position Service), and each `[crosser.portfolio.<name>]` section names the execution
//! portfolio its crosses transfer into. A `portfolios` entry without its section is fatal, as is
//! an execution portfolio equal to its strategy portfolio (a transfer onto itself). Several
//! strategy portfolios may share one execution portfolio (a pooled book). Portfolio strings are
//! not validated against the Position Service here — an unaccepted name surfaces as
//! `INVALID_ARGUMENT` on the first transfer.

use std::net::SocketAddr;

use anyhow::{Context, Result, bail};
use configparser::ini::Ini;
use eic::EicCode;
use flow_util::config::{RawConfig, config_path, load_ini, parse_list, req_non_empty};

use crate::crosser::PortfolioId;

pub use flow_util::config::FeedConfig;

#[derive(Debug, Clone)]
pub struct Config {
    /// Strategy gRPC endpoint serving `SubscribeTargets`, e.g. `http://localhost:50071`.
    pub strategy_endpoint: String,
    /// The strategy to subscribe to — named in the subscribe request. One strategy per crosser.
    pub strategy: String,
    /// Position Service gRPC endpoint — `Subscribe` (the actual position baseline) and
    /// `Transfer` (the downstream), e.g. `http://localhost:50053`.
    pub position_endpoint: String,
    /// nordpool-connector gRPC endpoint serving the depth stream the live mid is priced from,
    /// e.g. `http://localhost:50051`.
    pub connector_endpoint: String,
    /// The delivery area named in the depth subscription — a guard, not a filter: the connector
    /// serves a single area and rejects a mismatch (`NOT_FOUND`, fatal at boot — the crosser is
    /// pointed at the wrong connector).
    pub delivery_area_eic: EicCode,
    /// The strategy portfolios this crosser handles, each mapped to its execution portfolio —
    /// exactly the portfolios named in the strategy subscription (the strategy rejects the
    /// subscription if it does not produce one of them — a fatal boot error) and subscribed on
    /// the Position Service. Config order, preserved.
    pub portfolios: Vec<PortfolioConfig>,
    /// Listen address for the crosser's own endpoint — health + the enable/disable admin
    /// (step 7), e.g. `0.0.0.0:50072`. Nothing is served yet; parsed and validated from the
    /// start so the key cannot rot in shipped configs.
    pub listen_addr: SocketAddr,
    /// Feed tuning, shared by every subscription (strategy, depth, Position Service); every key
    /// is optional in the INI, defaulting per [`FeedConfig`].
    pub feed: FeedConfig,
}

/// One handled strategy portfolio: the `[crosser.portfolio.<name>]` section for an entry in
/// `portfolios`.
#[derive(Debug, Clone)]
pub struct PortfolioConfig {
    /// The strategy portfolio — the `portfolios` entry and section suffix; targets arrive keyed
    /// by it, and it is the `from` side of every cross.
    pub name: PortfolioId,
    /// The execution portfolio its crosses transfer into (the `to` side) — must differ from
    /// `name`; several strategy portfolios may share one (a pooled book).
    pub execution: PortfolioId,
}

impl Config {
    /// Load from the path in `CROSSER_CONFIG`, defaulting to `crosser.ini`.
    pub fn load() -> Result<Self> {
        Self::from_path(&config_path("CROSSER_CONFIG", "crosser.ini"))
    }

    /// Load from a specific INI file path.
    pub fn from_path(path: &str) -> Result<Self> {
        Self::from_ini(load_ini(path)?).with_context(|| format!("parsing config from {path}"))
    }

    fn from_ini(ini: Ini) -> Result<Self> {
        let r = RawConfig::new(ini);
        let s = "crosser";
        let portfolio_names =
            parse_list(&r.req(s, "portfolios")?).context("parsing [crosser] portfolios")?;
        if portfolio_names.is_empty() {
            bail!("[crosser] portfolios must list at least one strategy portfolio");
        }
        let portfolios = portfolio_names
            .into_iter()
            .map(|name| parse_portfolio(&r, name))
            .collect::<Result<Vec<_>>>()?;
        Ok(Self {
            strategy_endpoint: req_non_empty(&r, s, "strategy_endpoint")?,
            strategy: req_non_empty(&r, s, "strategy")?,
            position_endpoint: req_non_empty(&r, s, "position_endpoint")?,
            connector_endpoint: req_non_empty(&r, s, "connector_endpoint")?,
            delivery_area_eic: {
                let raw = req_non_empty(&r, s, "delivery_area_eic")?;
                EicCode::from_eic(&raw).with_context(|| {
                    format!("[crosser] delivery_area_eic {raw:?} is not a known EIC")
                })?
            },
            portfolios,
            listen_addr: r
                .req(s, "listen_addr")?
                .parse()
                .context("parsing [crosser] listen_addr as a socket address")?,
            feed: FeedConfig::parse(&r, s)?,
        })
    }
}

/// Parse one `[crosser.portfolio.<name>]` section. A `portfolios` entry without its section is
/// fatal (the missing `execution` names the section), as is an execution portfolio equal to its
/// strategy portfolio — that transfer would move quantity onto the very portfolio it reconciles,
/// never closing a gap.
fn parse_portfolio(r: &RawConfig, name: PortfolioId) -> Result<PortfolioConfig> {
    let section = format!("crosser.portfolio.{name}");
    let execution = req_non_empty(r, &section, "execution")?;
    if execution == name {
        bail!(
            "[{section}] execution {execution:?} equals its strategy portfolio \
             (a cross must move the gap onto a distinct execution portfolio)"
        );
    }
    Ok(PortfolioConfig { name, execution })
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

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
        assert_eq!(cfg.strategy_endpoint, "http://localhost:50071");
        assert_eq!(cfg.strategy, "strategy-x");
        assert_eq!(cfg.position_endpoint, "http://localhost:50053");
        assert_eq!(cfg.connector_endpoint, "http://localhost:50051");
        assert_eq!(cfg.delivery_area_eic, EicCode::AREA_FR);
        assert_eq!(
            cfg.listen_addr,
            "0.0.0.0:50072".parse::<SocketAddr>().unwrap()
        );
        // Both portfolio sections parse into the strategy → execution map, in config order.
        assert_eq!(cfg.portfolios.len(), 2);
        assert_eq!(cfg.portfolios[0].name, "strat-batt-1");
        assert_eq!(cfg.portfolios[0].execution, "execution-a");
        assert_eq!(cfg.portfolios[1].name, "strat-batt-2");
        assert_eq!(cfg.portfolios[1].execution, "execution-b");
        // The feed tuning keys are absent, so the code defaults apply.
        assert_eq!(cfg.feed.initial_backoff, Duration::from_millis(500));
        assert_eq!(cfg.feed.max_backoff, Duration::from_secs(30));
    }

    #[test]
    fn two_strategy_portfolios_may_share_an_execution_portfolio() {
        // A pooled book: both strategy portfolios cross into execution-a.
        let cfg = from_str(&SAMPLE.replace("execution = execution-b", "execution = execution-a"))
            .unwrap();
        assert_eq!(cfg.portfolios[0].execution, "execution-a");
        assert_eq!(cfg.portfolios[1].execution, "execution-a");
    }

    #[test]
    fn portfolio_entry_without_its_section_is_a_fatal_config_error() {
        let err = from_str(&SAMPLE.replace(
            "portfolios         = ${PORTFOLIOS_CFG_UNSET:-strat-batt-1, strat-batt-2}",
            "portfolios         = ${PORTFOLIOS_CFG_UNSET:-strat-batt-1, strat-batt-2, strat-batt-3}",
        ))
        .unwrap_err();
        assert!(
            err.to_string().contains("crosser.portfolio.strat-batt-3"),
            "{err}"
        );
    }

    #[test]
    fn execution_equal_to_its_strategy_portfolio_is_a_fatal_config_error() {
        // A self-transfer can never close a gap — reject it at load.
        let err = from_str(&SAMPLE.replace("execution = execution-a", "execution = strat-batt-1"))
            .unwrap_err();
        assert!(err.to_string().contains("strat-batt-1"), "{err}");
    }

    #[test]
    fn missing_endpoint_is_a_fatal_config_error() {
        let err = from_str(&SAMPLE.replace(
            "position_endpoint  = ${POSITION_ENDPOINT_CFG_UNSET:-http://localhost:50053}\n",
            "",
        ))
        .unwrap_err();
        assert!(err.to_string().contains("position_endpoint"), "{err}");
    }

    #[test]
    fn empty_endpoint_is_a_fatal_config_error() {
        // A `${VAR}` resolving to an empty string is present-but-blank; reject it at load
        // instead of failing obscurely at connect time.
        let err = from_str(&SAMPLE.replace(
            "connector_endpoint = ${CONNECTOR_ENDPOINT_CFG_UNSET:-http://localhost:50051}",
            "connector_endpoint = ${CONNECTOR_ENDPOINT_CFG_UNSET:-}",
        ))
        .unwrap_err();
        assert!(err.to_string().contains("connector_endpoint"), "{err}");
    }

    #[test]
    fn empty_portfolios_is_a_fatal_config_error() {
        let err = from_str(&SAMPLE.replace(
            "portfolios         = ${PORTFOLIOS_CFG_UNSET:-strat-batt-1, strat-batt-2}",
            "portfolios         = ${PORTFOLIOS_CFG_UNSET:-, }",
        ))
        .unwrap_err();
        assert!(err.to_string().contains("portfolios"), "{err}");
    }

    #[test]
    fn duplicate_portfolio_is_a_fatal_config_error() {
        let err = from_str(&SAMPLE.replace(
            "portfolios         = ${PORTFOLIOS_CFG_UNSET:-strat-batt-1, strat-batt-2}",
            "portfolios         = ${PORTFOLIOS_CFG_UNSET:-strat-batt-1, strat-batt-1}",
        ))
        .unwrap_err();
        // The duplicate entry is the root cause, under the "parsing … portfolios" context.
        assert!(format!("{err:#}").contains("strat-batt-1"), "{err}");
    }

    #[test]
    fn feed_tuning_keys_override_the_defaults() {
        // The tuning keys live in [crosser], so insert before another of its keys (appending
        // after the portfolio sections would land them in the wrong section).
        let cfg = from_str(&SAMPLE.replace(
            "listen_addr        =",
            "feed_initial_backoff_ms = 100\nlisten_addr        =",
        ))
        .unwrap();
        assert_eq!(cfg.feed.initial_backoff, Duration::from_millis(100));
        // Untouched keys keep their defaults.
        assert_eq!(cfg.feed.max_backoff, Duration::from_secs(30));
    }

    #[test]
    fn zero_feed_tuning_value_is_a_fatal_config_error() {
        // A zero backoff would reconnect in a hot loop; reject it at boot instead.
        let err = from_str(&SAMPLE.replace(
            "listen_addr        =",
            "feed_initial_backoff_ms = 0\nlisten_addr        =",
        ))
        .unwrap_err();
        assert!(err.to_string().contains("feed_initial_backoff_ms"), "{err}");
    }

    #[test]
    fn missing_key_is_a_fatal_config_error() {
        let err = from_str(
            "[crosser]\nstrategy = s\nportfolios = p\n\n[crosser.portfolio.p]\nexecution = e\n",
        )
        .unwrap_err();
        assert!(err.to_string().contains("strategy_endpoint"), "{err}");
    }

    const SAMPLE: &str = "\
[crosser]
strategy_endpoint  = ${STRATEGY_ENDPOINT_CFG_UNSET:-http://localhost:50071}
strategy           = ${STRATEGY_CFG_UNSET:-strategy-x}
position_endpoint  = ${POSITION_ENDPOINT_CFG_UNSET:-http://localhost:50053}
connector_endpoint = ${CONNECTOR_ENDPOINT_CFG_UNSET:-http://localhost:50051}
delivery_area_eic  = 10YFR-RTE------C
portfolios         = ${PORTFOLIOS_CFG_UNSET:-strat-batt-1, strat-batt-2}
listen_addr        = ${LISTEN_ADDR_CFG_UNSET:-0.0.0.0:50072}

[crosser.portfolio.strat-batt-1]
execution = execution-a

[crosser.portfolio.strat-batt-2]
execution = execution-b
";
}
