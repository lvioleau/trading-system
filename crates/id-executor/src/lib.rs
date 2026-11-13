//! The ID Executor — the intraday executor that trades an execution portfolio's gap to flat
//! on the Nord Pool continuous intraday market: it reads each configured portfolio's net
//! position from the Position Service as a standing instruction (drive it to zero) and sends
//! IOC order deltas to the PowerBot Gateway until the gap is flat; its fills flow back
//! gateway → Position Service, closing the loop the crosser opened. An aggressive IOC taker —
//! never a resting order. Authoritative plan: `docs/id-executor-implementation.md`; the
//! infra/decision split: `docs/refactor.md`.
//!
//! The layering, bottom to top:
//!
//! - **feeds** (`position_feed`, `contract_feed`, `depth_feed`, `gateway`) — the stream
//!   layer: reconnect with backoff, wire → domain at the boundary, typed events out.
//! - **managers** (`position_manager`, `contract_manager`, `order_manager`,
//!   `fill_matcher`) — the async owners: they hide the streams and their reconnect
//!   consequences, maintain the stores, run the order lifecycle, and surface narrow sync
//!   handles ([`executor::PositionSource`], [`executor::ContractSource`],
//!   [`executor::OrderBroker`]).
//! - **decision** (`executor`) — pure: one prepared [`executor::EvalInput`] and one clock
//!   read → an order or exactly one block reason. No IO, no channels.
//! - **engine** (`execution`) — the wiring plus the synchronous eval loop on its own
//!   thread, triggered by the market feed.

pub mod config;
pub mod contract_feed;
pub mod contract_manager;
pub mod depth_feed;
pub mod execution;
pub mod executor;
pub mod fill_matcher;
pub mod gateway;
pub mod order_manager;
pub mod order_sender;
pub mod order_states;
pub mod position_feed;
pub mod position_manager;
pub mod throttling;
pub mod trace;

/// Generated wire types for the `flow.v1` APIs the executor consumes
/// (position.proto and contract.proto — one protobuf package, one generated module) —
/// client stubs only: the executor serves no API of its own, so there is no server trait and
/// no reflection descriptor.
pub mod proto {
    tonic::include_proto!("flow.v1");
}

/// Generated wire types for the connector's depth feed, from the vendored
/// `nordpool.marketdata.v1` protos (owned by the nordpool-connector repo). Unlike
/// [`proto`], the server stubs are kept: nothing in this workspace serves the connector's
/// API, so the feed tests mock it from these (the crosser's pattern).
pub mod marketdata {
    tonic::include_proto!("nordpool.marketdata.v1");
}

/// Generated wire types for the PowerBot gateway (`powerbot_gateway.proto`, served by
/// `crates/powerbot-gateway`). It shares package `flow.v1` with [`proto`], so it
/// compiles into its own out_dir and lands as this distinct module (the position-service
/// wrinkle). Server stubs kept — the mock-gateway tests serve from them.
pub mod gateway_proto {
    include!(concat!(env!("OUT_DIR"), "/gateway/flow.v1.rs"));
}
