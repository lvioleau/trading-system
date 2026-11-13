//! Shared plumbing for the Flow services and workers — the mechanical, stable pieces every
//! crate in this workspace carried as a copy until the fifth consumer forced the extraction
//! (the id-executor plan's step 0; see `docs/id-executor-implementation.md`).
//!
//! Two modules, deliberately small:
//!
//! - [`config`] — the INI + `${VAR}` configuration scheme: [`config::RawConfig`] (the
//!   string-level lookup with environment substitution), the shared value parsers, and
//!   [`config::FeedConfig`] (the five `feed_*_ms` tuning keys every gRPC subscription
//!   shares).
//! - [`feed`] — the reconnect mechanics under those keys: [`feed::endpoint`] (a
//!   keepalive-configured tonic endpoint) and [`feed::Backoff`] (the exponential
//!   backoff-with-stable-reset policy).
//!
//! What does **not** belong here: domain types, protobuf, storage, and the feed *loops*
//! themselves. The loops genuinely differ per crate — failure escalation, fatal-versus-retry
//! classification, seed/delta disciplines — and forcing them under one abstraction would
//! couple what only rhymes. This crate holds the parts that are identical because they are
//! policy, not behavior.

pub mod config;
pub mod feed;
