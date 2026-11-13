//! PowerBot gateway — see `docs/design.md`.
//!
//! Library side of the binary: the websocket transport and the STOMP session it
//! carries. The driver loop, ingest pipeline, and gRPC server come on top.

pub mod config;
pub mod contracts;
pub(crate) mod domain;
pub mod driver;
pub mod grpc;
pub mod halts;
pub mod market;
pub mod metrics;
pub mod reconcile;
pub mod rest;
pub mod sink;
pub mod stomp;
pub mod storage;
pub mod stores;
pub(crate) mod time;
pub mod transport;
