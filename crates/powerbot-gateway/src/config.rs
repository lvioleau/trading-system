//! Application configuration, loaded once from an INI file.
//!
//! The file path is `POWERBOT_CONFIG` (default `powerbot.ini`). Every value may embed
//! `${VAR}` or `${VAR:-default}` references, substituted at load time by the shared
//! `flow-util` config mechanics from the process environment — the same scheme as
//! `nordpool.ini` and `contract-service.ini`. This module (through `flow-util`) and its
//! submodule [`logging`] are the *only* places the gateway reads the environment for
//! configuration. There is no `Default` impl and
//! no operational value baked into the code: the INI is the single source of truth, and the
//! bundled `config/defaults.ini` (compiled in under `cfg(test)`) supplies the same keys so unit
//! tests that never call [`load`](Config::load) still parse a full [`Config`].

use std::net::SocketAddr;
use std::time::Duration;

use anyhow::{Context, Result, bail};
use configparser::ini::Ini;
use flow_util::config::{RawConfig, config_path, load_ini};
use url::Url;

pub mod logging;

/// The whole gateway configuration, one struct per INI section.
#[derive(Debug, Clone)]
pub struct Config {
    pub gateway: GatewayConfig,
    pub storage: StorageConfig,
    pub server: ServerConfig,
    pub metrics: MetricsConfig,
}

/// `[gateway]` — the PowerBot connection, its session/transport tunables, and the
/// reconciliation cadence.
#[derive(Debug, Clone)]
pub struct GatewayConfig {
    /// STOMP-over-wss subscription endpoint. The `api_key` rides the upgrade; the host is
    /// derived from this for the STOMP `host` header.
    pub wss_url: Url,
    /// Base REST URL (normalized to end in `/` by the REST client).
    pub rest_url: String,
    /// PowerBot API key — a secret, injected via `${POWERBOT_API_KEY}`; used for both the wss
    /// upgrade and REST auth.
    pub api_key: String,
    /// PowerBot exchange path segment (e.g. `nordpool`); one gateway instance per exchange.
    pub exchange: String,
    /// EIC delivery-area code (16 chars, e.g. `10YDK-1--------W`); scopes the contracts topic.
    pub delivery_area_eic: String,
    /// The portfolios this gateway serves (comma-separated `portfolio_ids`, at least one).
    /// Each gets its own `ownorders` / `owntrades` topic subscription and reconciliation
    /// sweep; order entry and the execution subscriptions are validated against this set.
    pub portfolio_ids: Vec<String>,
    /// Whether this gateway points at a PowerBot *shadow* instance. Shadow executions —
    /// our orders matching replicated market data — are reported as *internal* trades;
    /// `true` normalizes each onto the own-trades stream as an ordinary trade
    /// (docs/design.md, "Shadow instances"), so consumers need no shadow-awareness of
    /// their own. The raw internal-trades relay is unaffected.
    pub shadow: bool,
    /// Bounds each blocking read/write during the TLS + WebSocket-upgrade handshake.
    pub handshake_timeout: Duration,
    /// How long a single `recv()` blocks before returning `None` — the read loop's wake cadence
    /// for keepalive and shutdown checks.
    pub read_timeout: Duration,
    /// Bounds a single `send` so a write to a silently-dead connection can't block forever.
    pub write_timeout: Duration,
    /// Client-side STOMP heart-beat cadence: how often we owe the server a frame.
    pub heartbeat_interval: Duration,
    /// Server-silence budget: how long without any inbound frame before the session is stale.
    pub heartbeat_timeout: Duration,
    /// How long to wait for the STOMP `CONNECTED` frame after the socket opens.
    pub connected_timeout: Duration,
    /// First reconnect backoff; doubles up to `max_backoff`.
    pub initial_backoff: Duration,
    /// Ceiling on the reconnect backoff.
    pub max_backoff: Duration,
    /// Reset the backoff to `initial_backoff` after a session has stayed up this long.
    pub backoff_reset_after: Duration,
    /// How often the reconciliation sweep re-fetches REST snapshots and evicts past-delivery
    /// entries.
    pub reconcile_interval: Duration,
    /// How far back the trade snapshots reach (`from_execution_time`).
    pub trade_window: Duration,
    /// How often the market poll re-fetches `getStatus` to log the full market/server
    /// status and export the `powerbot.market.*` metrics.
    pub market_poll_interval: Duration,
    /// How often the contract sweep re-fetches `findContracts` to seed/re-baseline the
    /// contract-tradability store the order gate checks.
    pub contract_poll_interval: Duration,
    /// Retain a contract's entries this long past `delivery_end` before eviction — sized to the
    /// Position Service restart/reconcile window.
    pub eviction_grace: Duration,
}

/// `[gateway.storage]` — the durable execution record. Persistence is required; a blank
/// `database_url` is a fatal config error.
#[derive(Debug, Clone)]
pub struct StorageConfig {
    /// Postgres URL for the `execution` schema — a secret, injected via
    /// `${POWERBOT_DATABASE_URL}`.
    pub database_url: String,
    /// How many websocket batches may await live persistence before the queue is full and
    /// batches are dropped (the reconciliation sweep still catches them).
    pub persist_buffer: usize,
}

/// `[server]` — the tonic gRPC surface.
#[derive(Debug, Clone)]
pub struct ServerConfig {
    /// Address the gRPC server listens on.
    pub listen_addr: SocketAddr,
    /// Per-topic broadcast channel capacity feeding the execution streams; a subscriber that
    /// falls this far behind is dropped.
    pub broadcast_capacity: usize,
    /// Maximum concurrent execution-stream subscribers; a new subscribe over the cap is
    /// rejected with `RESOURCE_EXHAUSTED`.
    pub max_subscribers: usize,
}

/// `[gateway.metrics]` — DogStatsD export. The recorder is always installed (docs/design.md,
/// "Observability"); a local run with no agent listening points at the INI's local default and
/// the UDP packets drop silently.
#[derive(Debug, Clone)]
pub struct MetricsConfig {
    /// `host:port` of a DogStatsD agent. Kept as a string (not a `SocketAddr`) because a
    /// hostname like `datadog-agent:8125` is legitimate; the exporter resolves it at install
    /// time, which still fails startup on a bad value. DogStatsD is UDP, so a local run with no
    /// agent can point at the INI default (`127.0.0.1:8125`) and the packets drop silently.
    pub dogstatsd_addr: String,
    /// Extra global tags stamped on every `powerbot.*` series (alongside the built-in
    /// `version:`), as `key:value` pairs. Lets the gateway self-identify its metrics when it
    /// shares a DogStatsD agent with another app — agent-level tags apply to every series through
    /// that agent, so an app-identifying tag belongs on the client, not the shared agent. Empty
    /// = none.
    pub tags: Vec<(String, String)>,
}

impl Config {
    /// Load from the path in `POWERBOT_CONFIG`, defaulting to `powerbot.ini`.
    pub fn load() -> Result<Self> {
        Self::from_path(&config_path("POWERBOT_CONFIG", "powerbot.ini"))
    }

    /// Load from a specific INI file path.
    pub fn from_path(path: &str) -> Result<Self> {
        Self::from_ini(load_ini(path)?).with_context(|| format!("parsing config from {path}"))
    }

    fn from_ini(ini: Ini) -> Result<Self> {
        let raw = RawConfig::new(ini);
        Ok(Self {
            gateway: gateway_from(&raw)?,
            storage: storage_from(&raw)?,
            server: server_from(&raw)?,
            metrics: metrics_from(&raw)?,
        })
    }
}

fn gateway_from(r: &RawConfig) -> Result<GatewayConfig> {
    let s = "gateway";
    Ok(GatewayConfig {
        wss_url: r
            .req(s, "wss_url")?
            .parse()
            .context("parsing [gateway] wss_url")?,
        rest_url: r.req(s, "rest_url")?,
        api_key: r.req(s, "api_key")?,
        exchange: r.req(s, "exchange")?,
        delivery_area_eic: r.req(s, "delivery_area_eic")?,
        portfolio_ids: parse_portfolio_ids(&r.req(s, "portfolio_ids")?)?,
        shadow: r
            .req(s, "shadow")?
            .parse()
            .context("parsing [gateway] shadow as a boolean (true/false)")?,
        handshake_timeout: secs(&r.req(s, "handshake_timeout_secs")?)?,
        read_timeout: millis(&r.req(s, "read_timeout_ms")?)?,
        write_timeout: secs(&r.req(s, "write_timeout_secs")?)?,
        heartbeat_interval: secs(&r.req(s, "heartbeat_interval_secs")?)?,
        heartbeat_timeout: secs(&r.req(s, "heartbeat_timeout_secs")?)?,
        connected_timeout: secs(&r.req(s, "connected_timeout_secs")?)?,
        initial_backoff: millis(&r.req(s, "initial_backoff_ms")?)?,
        max_backoff: secs(&r.req(s, "max_backoff_secs")?)?,
        backoff_reset_after: secs(&r.req(s, "backoff_reset_after_secs")?)?,
        reconcile_interval: secs(&r.req(s, "reconcile_interval_secs")?)?,
        trade_window: secs(&r.req(s, "trade_window_secs")?)?,
        market_poll_interval: secs(&r.req(s, "market_poll_interval_secs")?)?,
        contract_poll_interval: secs(&r.req(s, "contract_poll_interval_secs")?)?,
        eviction_grace: secs(&r.req(s, "eviction_grace_secs")?)?,
    })
}

fn storage_from(r: &RawConfig) -> Result<StorageConfig> {
    // Persistence is required — an absent or empty database_url gets a clearer message than the
    // generic missing-key error.
    let s = "gateway.storage";
    let database_url = r.req(s, "database_url")?;
    if database_url.is_empty() {
        bail!("[gateway.storage] database_url must be set (the durable record is required)");
    }
    Ok(StorageConfig {
        database_url,
        persist_buffer: r
            .req(s, "persist_buffer")?
            .parse()
            .context("parsing [gateway.storage] persist_buffer")?,
    })
}

fn server_from(r: &RawConfig) -> Result<ServerConfig> {
    let s = "server";
    Ok(ServerConfig {
        listen_addr: r
            .req(s, "listen_addr")?
            .parse()
            .context("parsing [server] listen_addr")?,
        broadcast_capacity: r
            .req(s, "broadcast_capacity")?
            .parse()
            .context("parsing [server] broadcast_capacity")?,
        max_subscribers: r
            .req(s, "max_subscribers")?
            .parse()
            .context("parsing [server] max_subscribers")?,
    })
}

fn metrics_from(r: &RawConfig) -> Result<MetricsConfig> {
    // Required — the default lives in the INI, not here. Metrics export is always installed; the
    // INI's dogstatsd_addr default (127.0.0.1:8125) is a UDP no-op without an agent.
    let s = "gateway.metrics";
    Ok(MetricsConfig {
        dogstatsd_addr: r.req(s, "dogstatsd_addr")?,
        tags: parse_tags(&r.req(s, "tags")?).context("parsing [gateway.metrics] tags")?,
    })
}

/// Parse the comma-separated `[gateway] portfolio_ids` list: at least one entry (an empty list
/// would subscribe no own-order/own-trade topics — never a deployment anyone means), blanks
/// between commas skipped, duplicates fatal (a duplicated portfolio is always a typo and would
/// double-subscribe its topics).
fn parse_portfolio_ids(raw: &str) -> Result<Vec<String>> {
    let mut ids = Vec::new();
    for entry in raw
        .split(',')
        .map(str::trim)
        .filter(|entry| !entry.is_empty())
    {
        if ids.iter().any(|seen| seen == entry) {
            bail!("[gateway] portfolio_ids lists {entry:?} twice");
        }
        ids.push(entry.to_string());
    }
    if ids.is_empty() {
        bail!("[gateway] portfolio_ids must list at least one portfolio");
    }
    Ok(ids)
}

/// Parse comma-separated `key:value` global metric tags; an empty string yields none. Each token
/// must be `key:value` — a malformed one fails config load rather than silently dropping a tag.
fn parse_tags(raw: &str) -> Result<Vec<(String, String)>> {
    raw.split(',')
        .map(str::trim)
        .filter(|t| !t.is_empty())
        .map(|t| {
            t.split_once(':')
                .map(|(k, v)| (k.trim().to_string(), v.trim().to_string()))
                .with_context(|| format!("metric tag {t:?} is not in key:value form"))
        })
        .collect()
}

fn secs(s: &str) -> Result<Duration> {
    Ok(Duration::from_secs(
        s.parse()
            .with_context(|| format!("invalid seconds value {s:?}"))?,
    ))
}

fn millis(s: &str) -> Result<Duration> {
    Ok(Duration::from_millis(s.parse().with_context(|| {
        format!("invalid milliseconds value {s:?}")
    })?))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The built-in defaults, whose every `${VAR}` reference carries a `:-` default so the file
    /// resolves without touching the process environment. There is no `Default` impl on the
    /// config structs; these values are here purely so unit tests parse a full [`Config`].
    const DEFAULTS_INI: &str = include_str!("config/defaults.ini");

    fn defaults() -> Config {
        let mut ini = Ini::new();
        ini.read(DEFAULTS_INI.to_string())
            .expect("bundled config/defaults.ini is valid INI");
        Config::from_ini(ini).expect("bundled config/defaults.ini parses to a valid Config")
    }

    #[test]
    fn parses_the_bundled_defaults() {
        // Every kind of field round-trips: plain string literals, the parsed URL/addr, and the
        // seconds/millis durations all reach their expected typed values.
        let cfg = defaults();
        assert_eq!(cfg.gateway.exchange, "nordpool");
        assert_eq!(cfg.gateway.portfolio_ids, vec!["testpf"]);
        assert!(!cfg.gateway.shadow);
        assert_eq!(cfg.gateway.read_timeout, Duration::from_secs(1));
        assert_eq!(cfg.gateway.initial_backoff, Duration::from_millis(500));
        assert_eq!(cfg.gateway.reconcile_interval, Duration::from_secs(300));
        assert_eq!(cfg.gateway.market_poll_interval, Duration::from_secs(60));
        assert_eq!(cfg.gateway.contract_poll_interval, Duration::from_secs(300));
        assert_eq!(cfg.gateway.wss_url.host_str(), Some("localhost"));
        assert_eq!(cfg.storage.persist_buffer, 8192);
        assert_eq!(cfg.server.listen_addr, "127.0.0.1:50052".parse().unwrap());
        assert_eq!(cfg.server.max_subscribers, 64);
        // The dogstatsd address is kept verbatim as a string (a hostname is legitimate, so it
        // isn't parsed to a SocketAddr here — the exporter resolves it at install time).
        assert_eq!(cfg.metrics.dogstatsd_addr, "127.0.0.1:8125");
        // Global tags parse into key/value pairs stamped on every series.
        assert_eq!(
            cfg.metrics.tags,
            vec![("service".to_string(), "powerbot-gateway".to_string())]
        );
    }

    #[test]
    fn parse_tags_splits_pairs_and_rejects_malformed() {
        // Empty (the "no extra tags" case) yields none.
        assert_eq!(parse_tags("").unwrap(), vec![]);
        // Comma-separated key:value pairs, trimmed, empties skipped.
        assert_eq!(
            parse_tags("service:powerbot-gateway, env:dev").unwrap(),
            vec![
                ("service".to_string(), "powerbot-gateway".to_string()),
                ("env".to_string(), "dev".to_string()),
            ]
        );
        // A token without a `:` is a config error, not a silently-dropped tag.
        let err = parse_tags("service:gw,bare").unwrap_err();
        assert!(err.to_string().contains("bare"), "{err:#}");
    }

    #[test]
    fn portfolio_ids_parse_as_a_list_and_reject_empty_and_duplicates() {
        // A comma-separated list parses in order, trimmed.
        let mut ini = Ini::new();
        ini.read(DEFAULTS_INI.replace(
            "portfolio_ids               = testpf",
            "portfolio_ids               = pf-1, pf-2",
        ))
        .unwrap();
        let cfg = Config::from_ini(ini).unwrap();
        assert_eq!(cfg.gateway.portfolio_ids, vec!["pf-1", "pf-2"]);

        // An empty list is fatal: no portfolios means no own-order/own-trade topics.
        let mut ini = Ini::new();
        ini.read(DEFAULTS_INI.replace(
            "portfolio_ids               = testpf",
            "portfolio_ids               = , ",
        ))
        .unwrap();
        let err = Config::from_ini(ini).unwrap_err();
        assert!(err.to_string().contains("portfolio_ids"), "{err:#}");

        // A duplicate is always a typo and would double-subscribe its topics.
        let mut ini = Ini::new();
        ini.read(DEFAULTS_INI.replace(
            "portfolio_ids               = testpf",
            "portfolio_ids               = pf-1, pf-1",
        ))
        .unwrap();
        let err = Config::from_ini(ini).unwrap_err();
        assert!(err.to_string().contains("pf-1"), "{err:#}");
    }

    #[test]
    fn shadow_parses_true_and_rejects_non_booleans() {
        let mut ini = Ini::new();
        ini.read(DEFAULTS_INI.replace(
            "shadow                      = false",
            "shadow                      = true",
        ))
        .unwrap();
        assert!(Config::from_ini(ini).unwrap().gateway.shadow);

        // A non-boolean must fail load, not silently run a shadow gateway live
        // (or vice versa).
        let mut ini = Ini::new();
        ini.read(DEFAULTS_INI.replace(
            "shadow                      = false",
            "shadow                      = yes",
        ))
        .unwrap();
        let err = Config::from_ini(ini).unwrap_err();
        assert!(err.to_string().contains("shadow"), "{err:#}");
    }

    #[test]
    fn blank_database_url_is_a_fatal_config_error() {
        // Persistence is required; a blank database_url must fail load rather than fall back to
        // an in-memory run.
        let mut ini = Ini::new();
        ini.read(DEFAULTS_INI.replace("${POWERBOT_CFG_UNSET:-postgres://localhost/trading}", ""))
            .unwrap();
        let err = Config::from_ini(ini).unwrap_err();
        assert!(err.to_string().contains("database_url"), "{err:#}");
    }
}
