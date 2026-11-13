//! Time helpers shared across the gateway: PowerBot timestamp parsing and
//! `SystemTime`-based "now" stamps. One parser for every layer (stores, gRPC
//! translation, storage), so an offset-less timestamp is read the same way
//! everywhere.
//!
//! chrono is built without the `clock` feature (no wall clock), so "now" comes
//! from `SystemTime` and conversions go through `chrono::DateTime::from_timestamp*`
//! — no `Utc::now`.

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use chrono::{DateTime, Utc};

/// Parse a PowerBot timestamp — RFC3339, or an offset-less datetime assumed UTC —
/// to a UTC instant; `None` when absent or unparseable.
pub(crate) fn parse_ts(ts: Option<&str>) -> Option<DateTime<Utc>> {
    let s = ts?;
    if let Ok(dt) = DateTime::parse_from_rfc3339(s) {
        return Some(dt.to_utc());
    }
    // Fallback: no offset present — assume UTC.
    chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S%.f")
        .ok()
        .map(|dt| dt.and_utc())
}

/// PowerBot timestamp → unix milliseconds. Returns `None` (not `0`) on
/// absence/parse failure so callers can tell "no timestamp" from "the epoch".
pub(crate) fn parse_millis(ts: Option<&str>) -> Option<i64> {
    parse_ts(ts).map(|dt| dt.timestamp_millis())
}

/// Current unix time in nanoseconds — the gateway receive stamp for a batch.
pub(crate) fn now_nanos() -> i64 {
    since_epoch().as_nanos() as i64
}

/// Current unix time in milliseconds — the base for eviction horizons.
pub(crate) fn now_millis() -> i64 {
    since_epoch().as_millis() as i64
}

/// Current unix time in whole seconds — the base for REST window bounds.
pub(crate) fn now_secs() -> u64 {
    since_epoch().as_secs()
}

fn since_epoch() -> Duration {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
}

/// A unix-seconds instant as an RFC3339 UTC string for PowerBot's `date-time`
/// query params.
pub(crate) fn rfc3339(unix_secs: u64) -> String {
    DateTime::from_timestamp(unix_secs as i64, 0)
        .unwrap_or_default()
        .to_rfc3339()
}

/// A unix-milliseconds instant as an RFC3339 UTC string, truncated to whole
/// seconds (plenty for human-facing messages); pre-epoch clamps to the epoch.
pub(crate) fn millis_rfc3339(millis: i64) -> String {
    rfc3339((millis / 1000).max(0) as u64)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_millis_handles_offset_and_naive_forms() {
        let z = parse_millis(Some("2020-06-18T17:00:00Z")).unwrap();
        let naive = parse_millis(Some("2020-06-18T17:00:00")).unwrap();
        assert_eq!(z, 1_592_499_600_000);
        assert_eq!(z, naive);
        assert_eq!(parse_millis(None), None);
        assert_eq!(parse_millis(Some("not a timestamp")), None);
    }

    #[test]
    fn rfc3339_formats_unix_seconds_as_utc() {
        // 2020-06-18T17:00:00Z = 1_592_499_600.
        assert_eq!(rfc3339(1_592_499_600), "2020-06-18T17:00:00+00:00");
    }
}
