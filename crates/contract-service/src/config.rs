//! Application configuration, loaded once from an INI file (same scheme as the
//! nordpool-connector).
//!
//! The file path is `CONTRACT_SERVICE_CONFIG` (default `contract-service.ini`). Every value
//! may embed `${VAR}` or `${VAR:-default}` references, substituted at load time by the
//! shared `flow-util` config mechanics from the process environment. This module (through
//! `flow-util`) is the *only* place the service reads the environment (besides `RUST_LOG`).

use std::net::SocketAddr;

use anyhow::{Context, Result, bail};
use configparser::ini::Ini;
use eic::EicCode;
use flow_util::config::{RawConfig, config_path, load_ini};

pub use flow_util::config::FeedConfig;

#[derive(Debug, Clone)]
pub struct Config {
    /// Connector gRPC endpoint serving the contract feed, e.g. `http://localhost:50051`.
    pub connector_endpoint: String,
    /// Delivery areas to subscribe to — one feed subscription, filtered to all of them.
    pub delivery_area_eics: Vec<EicCode>,
    /// Listen address for the service's own gRPC API, e.g. `0.0.0.0:50052`.
    pub listen_addr: SocketAddr,
    /// Postgres holding the durable contract tables, e.g.
    /// `postgres://postgres:postgres@localhost:5433/trading`.
    pub database_url: String,
    /// Feed tuning; every key is optional in the INI, defaulting per [`FeedConfig`].
    pub feed: FeedConfig,
}

impl Config {
    /// Load from the path in `CONTRACT_SERVICE_CONFIG`, defaulting to `contract-service.ini`.
    pub fn load() -> Result<Self> {
        Self::from_path(&config_path(
            "CONTRACT_SERVICE_CONFIG",
            "contract-service.ini",
        ))
    }

    /// Load from a specific INI file path.
    pub fn from_path(path: &str) -> Result<Self> {
        Self::from_ini(load_ini(path)?).with_context(|| format!("parsing config from {path}"))
    }

    fn from_ini(ini: Ini) -> Result<Self> {
        let r = RawConfig::new(ini);
        let s = "contract-service";
        Ok(Self {
            connector_endpoint: r.req(s, "connector_endpoint")?,
            delivery_area_eics: parse_delivery_area_eics(&r.req(s, "delivery_area_eics")?)?,
            listen_addr: r
                .req(s, "listen_addr")?
                .parse()
                .context("parsing [contract-service] listen_addr as a socket address")?,
            database_url: r.req(s, "database_url")?,
            feed: FeedConfig::parse(&r, s)?,
        })
    }
}

/// Parse `[contract-service] delivery_area_eics`: a comma-separated list of ENTSO-E EIC codes
/// (a single code is the common case). Blank entries between commas are skipped; an unknown
/// EIC or an empty list is a fatal config error — a typo'd area would otherwise subscribe to
/// nothing and surface as a feed that never fills.
fn parse_delivery_area_eics(s: &str) -> Result<Vec<EicCode>> {
    let eics = s
        .split(',')
        .map(str::trim)
        .filter(|entry| !entry.is_empty())
        .map(|entry| {
            EicCode::from_eic(entry).with_context(|| {
                format!("[contract-service] delivery_area_eics entry {entry:?} is not a known EIC")
            })
        })
        .collect::<Result<Vec<_>>>()?;
    if eics.is_empty() {
        bail!("[contract-service] delivery_area_eics must list at least one delivery area");
    }
    Ok(eics)
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
        assert_eq!(cfg.connector_endpoint, "http://localhost:50051");
        assert_eq!(cfg.delivery_area_eics, vec![EicCode::AREA_FR]);
        assert_eq!(
            cfg.listen_addr,
            "0.0.0.0:50052".parse::<SocketAddr>().unwrap()
        );
        assert_eq!(
            cfg.database_url,
            "postgres://postgres:postgres@localhost:5433/trading"
        );
        // The feed tuning keys are absent, so the code defaults apply.
        assert_eq!(cfg.feed.initial_backoff, Duration::from_millis(500));
        assert_eq!(cfg.feed.max_backoff, Duration::from_secs(30));
        assert_eq!(cfg.feed.stable_session, Duration::from_secs(30));
        assert_eq!(cfg.feed.keepalive_interval, Duration::from_secs(30));
        assert_eq!(cfg.feed.keepalive_timeout, Duration::from_secs(20));
    }

    #[test]
    fn feed_tuning_keys_override_the_defaults() {
        let cfg = from_str(&format!(
            "{SAMPLE}feed_initial_backoff_ms = 100\nfeed_keepalive_timeout_ms = 5000\n"
        ))
        .unwrap();
        assert_eq!(cfg.feed.initial_backoff, Duration::from_millis(100));
        assert_eq!(cfg.feed.keepalive_timeout, Duration::from_secs(5));
        // Untouched keys keep their defaults.
        assert_eq!(cfg.feed.max_backoff, Duration::from_secs(30));
    }

    #[test]
    fn non_integer_feed_tuning_value_is_a_fatal_config_error() {
        let err = from_str(&format!("{SAMPLE}feed_max_backoff_ms = fast\n")).unwrap_err();
        assert!(err.to_string().contains("feed_max_backoff_ms"), "{err}");
    }

    #[test]
    fn zero_feed_tuning_value_is_a_fatal_config_error() {
        // A zero backoff would reconnect in a hot loop; reject it at boot instead.
        let err = from_str(&format!("{SAMPLE}feed_initial_backoff_ms = 0\n")).unwrap_err();
        assert!(err.to_string().contains("feed_initial_backoff_ms"), "{err}");
    }

    #[test]
    fn delivery_area_eics_parse_a_comma_separated_list() {
        let cfg = from_str(&SAMPLE.replace(
            "delivery_area_eics = ${DELIVERY_AREA_EICS_CFG_UNSET:-10YFR-RTE------C}",
            "delivery_area_eics = 10YFR-RTE------C, 10YCH-SWISSGRIDZ ,10YAT-APG------L",
        ))
        .unwrap();
        assert_eq!(
            cfg.delivery_area_eics,
            vec![EicCode::AREA_FR, EicCode::AREA_CH, EicCode::AREA_AT]
        );
    }

    #[test]
    fn empty_delivery_area_eics_is_a_fatal_config_error() {
        let err = from_str(&SAMPLE.replace(
            "delivery_area_eics = ${DELIVERY_AREA_EICS_CFG_UNSET:-10YFR-RTE------C}",
            "delivery_area_eics = , ",
        ))
        .unwrap_err();
        assert!(err.to_string().contains("delivery_area_eics"), "{err}");
    }

    #[test]
    fn invalid_listen_addr_is_a_fatal_config_error() {
        let err = from_str(&SAMPLE.replace(
            "listen_addr        = ${LISTEN_ADDR_CFG_UNSET:-0.0.0.0:50052}",
            "listen_addr        = not-an-address",
        ))
        .unwrap_err();
        assert!(err.to_string().contains("listen_addr"), "{err}");
    }

    #[test]
    fn missing_key_is_a_fatal_config_error() {
        let err = from_str("[contract-service]\nconnector_endpoint = http://x\n").unwrap_err();
        assert!(err.to_string().contains("delivery_area_eics"), "{err}");
    }

    const SAMPLE: &str = "\
[contract-service]
connector_endpoint = ${CONNECTOR_ENDPOINT_CFG_UNSET:-http://localhost:50051}
delivery_area_eics = ${DELIVERY_AREA_EICS_CFG_UNSET:-10YFR-RTE------C}
listen_addr        = ${LISTEN_ADDR_CFG_UNSET:-0.0.0.0:50052}
database_url       = ${DATABASE_URL_CFG_UNSET:-postgres://postgres:postgres@localhost:5433/trading}
";
}
