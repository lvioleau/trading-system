//! Application configuration, loaded once from an INI file (same scheme as the
//! contract-service and the nordpool-connector).
//!
//! The file path is `POSITION_SERVICE_CONFIG` (default `position-service.ini`). Every value
//! may embed `${VAR}` or `${VAR:-default}` references, substituted at load time by the
//! shared `flow-util` config mechanics from the process environment. This module (through
//! `flow-util`) is the *only* place the service reads the environment (besides `RUST_LOG`).
//!
//! The accepted portfolios and the order gateways are both configuration: `portfolios`
//! names every portfolio the service handles, and each `[position-service.gateway.<name>]`
//! section maps one gateway's execution stream to the portfolio its fills fold into.

use std::net::SocketAddr;

use anyhow::{Context, Result, bail};
use configparser::ini::Ini;
use flow_util::config::{RawConfig, config_path, load_ini, parse_list, req_non_empty};

use crate::ledger::PortfolioId;

pub use flow_util::config::FeedConfig;

#[derive(Debug, Clone)]
pub struct Config {
    /// Portfolios this service accepts — the ledger's aggregation boundaries. `Transfer`
    /// legs and gateway mappings must name a member; the boot fold tolerates (and warns on)
    /// ledger portfolios that are no longer listed.
    pub portfolios: Vec<PortfolioId>,
    /// Order gateways whose execution streams are folded into the ledger — one feed
    /// subscription each. Empty is a valid transfers-only deployment.
    pub gateways: Vec<GatewayConfig>,
    /// Listen address for the service's own gRPC API, e.g. `0.0.0.0:50053`.
    pub listen_addr: SocketAddr,
    /// Postgres holding the durable trade ledger (schema `positions`), e.g.
    /// `postgres://postgres:postgres@localhost:5433/trading`.
    pub database_url: String,
    /// Feed tuning, shared by every gateway feed; every key is optional in the INI,
    /// defaulting per [`FeedConfig`].
    pub feed: FeedConfig,
}

/// One order gateway: the `[position-service.gateway.<name>]` section for an entry in
/// `gateways`.
#[derive(Debug, Clone)]
pub struct GatewayConfig {
    /// The `gateways` entry and section suffix — config identity only, never part of a
    /// trade's identity (fill ids derive from the wire trade, so renaming a gateway is
    /// safe).
    pub name: String,
    /// Gateway gRPC endpoint serving the execution streams, e.g. `http://localhost:50061`.
    pub endpoint: String,
    /// Portfolio this gateway's fills fold into; must be a member of
    /// [`portfolios`](Config::portfolios).
    pub portfolio: PortfolioId,
    /// The gateway's PowerBot portfolio id. Every gateway subscription names its portfolio
    /// scope on the wire (the gateway requires it and rejects a mismatch with
    /// `INVALID_ARGUMENT` — a fail-fast config-drift check), so the key is required on every
    /// gateway.
    pub powerbot_portfolio_id: String,
}

impl Config {
    /// Load from the path in `POSITION_SERVICE_CONFIG`, defaulting to
    /// `position-service.ini`.
    pub fn load() -> Result<Self> {
        Self::from_path(&config_path(
            "POSITION_SERVICE_CONFIG",
            "position-service.ini",
        ))
    }

    /// Load from a specific INI file path.
    pub fn from_path(path: &str) -> Result<Self> {
        Self::from_ini(load_ini(path)?).with_context(|| format!("parsing config from {path}"))
    }

    fn from_ini(ini: Ini) -> Result<Self> {
        let r = RawConfig::new(ini);
        let s = "position-service";
        let portfolios = parse_list(&r.req(s, "portfolios")?)
            .context("parsing [position-service] portfolios")?;
        if portfolios.is_empty() {
            bail!("[position-service] portfolios must list at least one portfolio");
        }
        // An absent or empty gateway list is a valid transfers-only deployment.
        let gateway_names = match r.opt(s, "gateways")? {
            Some(value) => parse_list(&value).context("parsing [position-service] gateways")?,
            None => Vec::new(),
        };
        let gateways = gateway_names
            .into_iter()
            .map(|name| parse_gateway(&r, name, &portfolios))
            .collect::<Result<Vec<_>>>()?;
        // A PowerBot portfolio may be subscribed by only one gateway section. Fill ids
        // derive from (exchange, trade_id), so two sections scoped to the same venue
        // portfolio would receive identical trades — folded once, into whichever section's
        // feed applies first: with different `portfolio` targets that is a race for which
        // book gets each fill, and with the same target a pointless double subscription.
        // (Sharing a fold `portfolio` across *different* venue portfolios — a pooled book —
        // remains fine.)
        for (i, gateway) in gateways.iter().enumerate() {
            if let Some(other) = gateways[..i]
                .iter()
                .find(|g| g.powerbot_portfolio_id == gateway.powerbot_portfolio_id)
            {
                bail!(
                    "powerbot_portfolio_id {:?} is subscribed by both \
                     [position-service.gateway.{}] and [position-service.gateway.{}] — \
                     a venue portfolio must be subscribed by exactly one gateway section",
                    gateway.powerbot_portfolio_id,
                    other.name,
                    gateway.name,
                );
            }
        }
        Ok(Self {
            portfolios,
            gateways,
            listen_addr: r
                .req(s, "listen_addr")?
                .parse()
                .context("parsing [position-service] listen_addr as a socket address")?,
            database_url: r.req(s, "database_url")?,
            feed: FeedConfig::parse(&r, s)?,
        })
    }
}

/// Parse one `[position-service.gateway.<name>]` section. A `gateways` entry without its
/// section is fatal (the missing `endpoint` names the section), as is a `portfolio` outside
/// the configured set.
fn parse_gateway(r: &RawConfig, name: String, portfolios: &[PortfolioId]) -> Result<GatewayConfig> {
    let section = format!("position-service.gateway.{name}");
    let endpoint = r.req(&section, "endpoint")?;
    let portfolio = r.req(&section, "portfolio")?;
    if !portfolios.contains(&portfolio) {
        bail!("[{section}] portfolio {portfolio:?} is not listed in [position-service] portfolios");
    }
    // Required on every gateway: each subscription names its portfolio scope on the wire,
    // and the gateway rejects a subscribe that does not match its own configured portfolio
    // — so an empty or missing id here could never subscribe at all.
    let powerbot_portfolio_id = req_non_empty(r, &section, "powerbot_portfolio_id")?;
    Ok(GatewayConfig {
        name,
        endpoint,
        portfolio,
        powerbot_portfolio_id,
    })
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
        assert_eq!(
            cfg.portfolios,
            vec!["execution-a", "execution-b", "strategy-x"]
        );
        assert_eq!(
            cfg.listen_addr,
            "0.0.0.0:50053".parse::<SocketAddr>().unwrap()
        );
        assert_eq!(
            cfg.database_url,
            "postgres://postgres:postgres@localhost:5433/trading"
        );
        // Both gateway sections parse, mapped to their portfolios.
        assert_eq!(cfg.gateways.len(), 2);
        assert_eq!(cfg.gateways[0].name, "nordpool-a");
        assert_eq!(cfg.gateways[0].endpoint, "http://localhost:50061");
        assert_eq!(cfg.gateways[0].portfolio, "execution-a");
        assert_eq!(cfg.gateways[0].powerbot_portfolio_id, "PB-A");
        assert_eq!(cfg.gateways[1].name, "nordpool-b");
        assert_eq!(cfg.gateways[1].portfolio, "execution-b");
        assert_eq!(cfg.gateways[1].powerbot_portfolio_id, "PB-B");
        // The feed tuning keys are absent, so the code defaults apply.
        assert_eq!(cfg.feed.initial_backoff, Duration::from_millis(500));
        assert_eq!(cfg.feed.max_backoff, Duration::from_secs(30));
    }

    #[test]
    fn no_gateways_is_a_valid_transfers_only_config() {
        // Given a config whose gateway list resolves empty (the sections are then ignored)
        let cfg = from_str(&SAMPLE.replace(
            "gateways     = ${GATEWAYS_CFG_UNSET:-nordpool-a, nordpool-b}",
            "gateways     = ${GATEWAYS_CFG_UNSET:-}",
        ))
        .unwrap();
        assert!(cfg.gateways.is_empty());

        // And an absent key means the same
        let cfg = from_str(&SAMPLE.replace(
            "gateways     = ${GATEWAYS_CFG_UNSET:-nordpool-a, nordpool-b}\n",
            "",
        ))
        .unwrap();
        assert!(cfg.gateways.is_empty());
    }

    #[test]
    fn gateway_with_unlisted_portfolio_is_a_fatal_config_error() {
        let err =
            from_str(&SAMPLE.replace("portfolio = execution-b", "portfolio = unknown-portfolio"))
                .unwrap_err();
        assert!(err.to_string().contains("unknown-portfolio"), "{err}");
    }

    #[test]
    fn gateway_entry_without_its_section_is_a_fatal_config_error() {
        let err = from_str(&SAMPLE.replace(
            "gateways     = ${GATEWAYS_CFG_UNSET:-nordpool-a, nordpool-b}",
            "gateways     = ${GATEWAYS_CFG_UNSET:-nordpool-a, nordpool-b, nordpool-c}",
        ))
        .unwrap_err();
        assert!(
            err.to_string()
                .contains("position-service.gateway.nordpool-c"),
            "{err}"
        );
    }

    #[test]
    fn missing_powerbot_portfolio_id_is_a_fatal_config_error() {
        // Required on every gateway: the subscription names its portfolio scope on the
        // wire, so a gateway without the id could never subscribe.
        let err = from_str(&SAMPLE.replace("powerbot_portfolio_id = PB-B\n", "")).unwrap_err();
        assert!(err.to_string().contains("powerbot_portfolio_id"), "{err}");
    }

    #[test]
    fn a_powerbot_portfolio_subscribed_by_two_gateways_is_a_fatal_config_error() {
        // Fill ids derive from (exchange, trade_id): two sections scoped to the same venue
        // portfolio would race the same fills between their fold portfolios.
        let err = from_str(&SAMPLE.replace(
            "powerbot_portfolio_id = PB-B",
            "powerbot_portfolio_id = PB-A",
        ))
        .unwrap_err();
        let message = err.to_string();
        assert!(message.contains("PB-A"), "{err}");
        // The error names both offending sections.
        assert!(message.contains("nordpool-a"), "{err}");
        assert!(message.contains("nordpool-b"), "{err}");
    }

    #[test]
    fn two_gateways_may_fold_into_one_portfolio() {
        // A pooled book: distinct venue portfolios, same fold portfolio — allowed.
        let cfg = from_str(&SAMPLE.replace("portfolio = execution-b", "portfolio = execution-a"))
            .unwrap();
        assert_eq!(cfg.gateways[0].portfolio, "execution-a");
        assert_eq!(cfg.gateways[1].portfolio, "execution-a");
    }

    #[test]
    fn empty_powerbot_portfolio_id_is_a_fatal_config_error() {
        // A `${VAR}` resolving to an empty string is present-but-blank; the gateway would
        // reject the subscribe — fail at load instead.
        let err = from_str(&SAMPLE.replace(
            "powerbot_portfolio_id = PB-A",
            "powerbot_portfolio_id = ${PB_A_CFG_UNSET:-}",
        ))
        .unwrap_err();
        assert!(err.to_string().contains("powerbot_portfolio_id"), "{err}");
    }

    #[test]
    fn duplicate_portfolio_is_a_fatal_config_error() {
        let err = from_str(&SAMPLE.replace(
            "portfolios   = ${PORTFOLIOS_CFG_UNSET:-execution-a, execution-b, strategy-x}",
            "portfolios   = ${PORTFOLIOS_CFG_UNSET:-execution-a, execution-a}",
        ))
        .unwrap_err();
        // The duplicate entry is the root cause, under the "parsing … portfolios" context.
        let chain = format!("{err:#}");
        assert!(chain.contains("execution-a"), "{chain}");
    }

    #[test]
    fn empty_portfolios_is_a_fatal_config_error() {
        let err = from_str(&SAMPLE.replace(
            "portfolios   = ${PORTFOLIOS_CFG_UNSET:-execution-a, execution-b, strategy-x}",
            "portfolios   = ${PORTFOLIOS_CFG_UNSET:-, }",
        ))
        .unwrap_err();
        assert!(err.to_string().contains("portfolios"), "{err}");
    }

    #[test]
    fn feed_tuning_keys_override_the_defaults() {
        // The tuning keys live in [position-service], so insert before another of its keys
        // (appending after the gateway sections would land them in the wrong section).
        let cfg = from_str(&SAMPLE.replace(
            "listen_addr  =",
            "feed_initial_backoff_ms = 100\nlisten_addr  =",
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
            "listen_addr  =",
            "feed_initial_backoff_ms = 0\nlisten_addr  =",
        ))
        .unwrap_err();
        assert!(err.to_string().contains("feed_initial_backoff_ms"), "{err}");
    }

    #[test]
    fn missing_key_is_a_fatal_config_error() {
        let err = from_str("[position-service]\nportfolios = execution\n").unwrap_err();
        assert!(err.to_string().contains("listen_addr"), "{err}");
    }

    const SAMPLE: &str = "\
[position-service]
portfolios   = ${PORTFOLIOS_CFG_UNSET:-execution-a, execution-b, strategy-x}
gateways     = ${GATEWAYS_CFG_UNSET:-nordpool-a, nordpool-b}
listen_addr  = ${LISTEN_ADDR_CFG_UNSET:-0.0.0.0:50053}
database_url = ${DATABASE_URL_CFG_UNSET:-postgres://postgres:postgres@localhost:5433/trading}

[position-service.gateway.nordpool-a]
endpoint  = http://localhost:50061
portfolio = execution-a
powerbot_portfolio_id = PB-A

[position-service.gateway.nordpool-b]
endpoint  = http://localhost:50062
portfolio = execution-b
powerbot_portfolio_id = PB-B
";
}
