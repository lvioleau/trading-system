//! The shared INI + `${VAR}` configuration scheme (the same scheme as the
//! nordpool-connector): [`RawConfig`] resolves string-level values with environment
//! substitution, the free functions parse the recurring value shapes, and [`FeedConfig`]
//! carries the five `feed_*_ms` tuning keys every gRPC subscription shares.
//!
//! Each consuming crate keeps its own typed `Config` and its own sections and keys — this
//! module is the *mechanics* under them, so the config surface (which keys exist, what is
//! required, what defaults) stays visible in each crate's `config.rs`.

use std::collections::HashSet;
use std::time::Duration;

use anyhow::{Context, Result, anyhow, bail};
use configparser::ini::Ini;

/// Looks up `[section] key` values in the INI and substitutes their `${VAR}` references
/// from the environment.
pub struct RawConfig {
    ini: Ini,
}

impl RawConfig {
    pub fn new(ini: Ini) -> Self {
        Self { ini }
    }

    /// A resolved value that must be present in the file. Trimmed: edge whitespace can
    /// only come from a substituted env var, where it's an accident (e.g. a trailing
    /// newline) — never meaning.
    pub fn req(&self, section: &str, key: &str) -> Result<String> {
        self.opt(section, key)?
            .with_context(|| format!("missing [{section}] {key}"))
    }

    /// A resolved value that may be absent from the file. Present values resolve and trim
    /// exactly like [`Self::req`].
    pub fn opt(&self, section: &str, key: &str) -> Result<Option<String>> {
        let Some(raw) = self.ini.get(section, key) else {
            return Ok(None);
        };
        let value = resolve(&raw).with_context(|| format!("resolving [{section}] {key}"))?;
        Ok(Some(value.trim().to_string()))
    }

    /// The section names present in the INI (lowercased by the parser's default
    /// case-insensitivity). For crates whose config lists sub-sections both ways: only the
    /// INI itself knows what sections exist, so this is what makes an *orphan* sub-section
    /// (present in the file, absent from the list — a typo) detectable at load.
    pub fn sections(&self) -> Vec<String> {
        self.ini.sections()
    }
}

/// Substitute `${VAR}` / `${VAR:-default}` references in `raw`. Lookup order: process
/// environment, then the `:-` default; missing from both is an error.
fn resolve(raw: &str) -> Result<String> {
    let mut out = String::new();
    let mut rest = raw;
    while let Some(start) = rest.find("${") {
        out.push_str(&rest[..start]);
        let after = &rest[start + 2..];
        let end = after
            .find('}')
            .with_context(|| format!("unterminated ${{ in {raw:?}"))?;
        let (name, default) = match after[..end].split_once(":-") {
            Some((n, d)) => (n, Some(d)),
            None => (&after[..end], None),
        };
        let value = match std::env::var(name) {
            Ok(v) => v,
            Err(_) => default.map(str::to_string).ok_or_else(|| {
                anyhow!("{name} is not set in the environment and has no default (in {raw:?})")
            })?,
        };
        out.push_str(&value);
        rest = &after[end + 1..];
    }
    out.push_str(rest);
    Ok(out)
}

/// Load and parse an INI file.
pub fn load_ini(path: &str) -> Result<Ini> {
    let mut ini = Ini::new();
    ini.load(path)
        .map_err(|e| anyhow!("loading config from {path}: {e}"))?;
    Ok(ini)
}

/// The config file path from `env_var`, defaulting to `default` when unset — every crate's
/// `<NAME>_CONFIG` convention.
pub fn config_path(env_var: &str, default: &str) -> String {
    std::env::var(env_var).unwrap_or_else(|_| default.to_string())
}

/// A required value that must also resolve non-empty: an endpoint or name whose `${VAR}`
/// resolved to an empty string would otherwise slip through as present-but-blank and fail
/// obscurely at connect (or subscribe) time instead of at load.
pub fn req_non_empty(r: &RawConfig, section: &str, key: &str) -> Result<String> {
    let value = r.req(section, key)?;
    if value.is_empty() {
        bail!("[{section}] {key} must be non-empty");
    }
    Ok(value)
}

/// Parse a comma-separated list of names. Blank entries between commas are skipped; a
/// duplicate is a fatal config error (a duplicated portfolio or gateway is always a typo).
pub fn parse_list(s: &str) -> Result<Vec<String>> {
    let mut seen = HashSet::new();
    let mut names = Vec::new();
    for entry in s
        .split(',')
        .map(str::trim)
        .filter(|entry| !entry.is_empty())
    {
        if !seen.insert(entry.to_owned()) {
            bail!("duplicate entry {entry:?}");
        }
        names.push(entry.to_owned());
    }
    Ok(names)
}

/// Parse an optional `[section] key` as a millisecond count, falling back to `default` when
/// the key is absent. A present-but-non-integer value is a fatal config error, and so is
/// zero: every tuned duration is an interval where 0 means a hot loop (backoff) or
/// pathological probing (keepalive), never a sensible setting.
pub fn opt_ms(r: &RawConfig, section: &str, key: &str, default: Duration) -> Result<Duration> {
    match r.opt(section, key)? {
        Some(value) => {
            let ms = value
                .parse::<u64>()
                .with_context(|| format!("parsing [{section}] {key} as milliseconds"))?;
            if ms == 0 {
                bail!("[{section}] {key} must be a positive millisecond count");
            }
            Ok(Duration::from_millis(ms))
        }
        None => Ok(default),
    }
}

/// Feed client tuning: reconnect backoff and HTTP/2 keepalive, set by the optional
/// `feed_*_ms` keys of the consuming crate's section and shared by every subscription it
/// runs.
///
/// The defaults mirror the connector: exponential backoff 500ms → 30s, reset once a session
/// has proven stable, and keepalive PINGs so an upstream that vanished without closing
/// (network partition, hard power-off) leaves the stream erroring into the reconnect loop
/// instead of waiting forever.
#[derive(Debug, Clone, Copy)]
pub struct FeedConfig {
    /// Backoff before the first reconnect attempt (`feed_initial_backoff_ms`).
    pub initial_backoff: Duration,
    /// Ceiling for the exponential backoff (`feed_max_backoff_ms`).
    pub max_backoff: Duration,
    /// Session length after which the backoff resets (`feed_stable_session_ms`).
    pub stable_session: Duration,
    /// HTTP/2 keepalive PING interval (`feed_keepalive_interval_ms`).
    pub keepalive_interval: Duration,
    /// Time to wait for a keepalive PING ack before dropping the connection
    /// (`feed_keepalive_timeout_ms`).
    pub keepalive_timeout: Duration,
}

impl Default for FeedConfig {
    fn default() -> Self {
        Self {
            initial_backoff: Duration::from_millis(500),
            max_backoff: Duration::from_secs(30),
            stable_session: Duration::from_secs(30),
            keepalive_interval: Duration::from_secs(30),
            keepalive_timeout: Duration::from_secs(20),
        }
    }
}

impl FeedConfig {
    /// Parse the five optional `feed_*_ms` keys from `[section]`, each defaulting per
    /// [`FeedConfig::default`].
    pub fn parse(r: &RawConfig, section: &str) -> Result<Self> {
        let defaults = Self::default();
        Ok(Self {
            initial_backoff: opt_ms(
                r,
                section,
                "feed_initial_backoff_ms",
                defaults.initial_backoff,
            )?,
            max_backoff: opt_ms(r, section, "feed_max_backoff_ms", defaults.max_backoff)?,
            stable_session: opt_ms(
                r,
                section,
                "feed_stable_session_ms",
                defaults.stable_session,
            )?,
            keepalive_interval: opt_ms(
                r,
                section,
                "feed_keepalive_interval_ms",
                defaults.keepalive_interval,
            )?,
            keepalive_timeout: opt_ms(
                r,
                section,
                "feed_keepalive_timeout_ms",
                defaults.keepalive_timeout,
            )?,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resolve_passes_through_literals() {
        assert_eq!(resolve("plain value").unwrap(), "plain value");
    }

    #[test]
    fn resolve_uses_default_when_var_unset() {
        // Given a `${VAR:-default}` reference whose variable is guaranteed unset (the
        // FLOW_CFG_UNSET name exists only to never be in the environment).
        // When / Then resolving falls through to the `:-` default rather than erroring, which is
        // how the shipped INI keeps local-dev defaults when their env var isn't injected.
        assert_eq!(resolve("${FLOW_CFG_UNSET:-fallback}").unwrap(), "fallback");
    }

    #[test]
    fn resolve_composes_multiple_refs() {
        assert_eq!(
            resolve("${A_CFG_UNSET:-http}://${B_CFG_UNSET:-host}/x").unwrap(),
            "http://host/x"
        );
    }

    #[test]
    fn resolve_errors_on_missing_var_without_default() {
        let err = resolve("${FLOW_CFG_UNSET}").unwrap_err();
        assert!(err.to_string().contains("FLOW_CFG_UNSET"));
    }

    #[test]
    fn resolve_reads_from_the_environment() {
        // PATH is always set in the test environment.
        assert_eq!(resolve("${PATH}").unwrap(), std::env::var("PATH").unwrap());
    }

    fn raw(contents: &str) -> RawConfig {
        let mut ini = Ini::new();
        ini.read(contents.to_string()).unwrap();
        RawConfig::new(ini)
    }

    #[test]
    fn req_trims_and_opt_distinguishes_absent_from_present() {
        let r = raw("[s]\nkey = ${FLOW_CFG_UNSET:- padded }\n");
        assert_eq!(r.req("s", "key").unwrap(), "padded");
        assert_eq!(r.opt("s", "key").unwrap().as_deref(), Some("padded"));
        assert_eq!(r.opt("s", "absent").unwrap(), None);
        assert!(
            r.req("s", "absent")
                .unwrap_err()
                .to_string()
                .contains("[s]")
        );
    }

    #[test]
    fn req_non_empty_rejects_present_but_blank() {
        // A `${VAR}` resolving to an empty string is present-but-blank; reject it at load
        // instead of failing obscurely at connect time.
        let r = raw("[s]\nkey = ${FLOW_CFG_UNSET:-}\n");
        assert!(
            req_non_empty(&r, "s", "key")
                .unwrap_err()
                .to_string()
                .contains("non-empty")
        );
    }

    #[test]
    fn parse_list_skips_blanks_and_rejects_duplicates() {
        assert_eq!(parse_list("a, b ,, c,").unwrap(), vec!["a", "b", "c"]);
        assert!(parse_list("a, a").unwrap_err().to_string().contains("a"));
        assert!(parse_list(", ").unwrap().is_empty());
    }

    #[test]
    fn feed_config_parses_overrides_and_defaults() {
        // Present keys override; absent keys keep the code defaults.
        let r = raw("[s]\nfeed_initial_backoff_ms = 100\n");
        let feed = FeedConfig::parse(&r, "s").unwrap();
        assert_eq!(feed.initial_backoff, Duration::from_millis(100));
        assert_eq!(feed.max_backoff, Duration::from_secs(30));
        assert_eq!(feed.stable_session, Duration::from_secs(30));
        assert_eq!(feed.keepalive_interval, Duration::from_secs(30));
        assert_eq!(feed.keepalive_timeout, Duration::from_secs(20));
    }

    #[test]
    fn zero_or_non_integer_feed_tuning_value_is_a_fatal_config_error() {
        // A zero backoff would reconnect in a hot loop; reject it at boot instead.
        let err = FeedConfig::parse(&raw("[s]\nfeed_initial_backoff_ms = 0\n"), "s").unwrap_err();
        assert!(err.to_string().contains("feed_initial_backoff_ms"), "{err}");
        let err = FeedConfig::parse(&raw("[s]\nfeed_max_backoff_ms = fast\n"), "s").unwrap_err();
        assert!(err.to_string().contains("feed_max_backoff_ms"), "{err}");
    }
}
