//! The Crosser: the stateless worker that reconciles the Strategy Layer's target positions
//! against the actual Position Service positions and injects the delta as transfers — strategy
//! portfolio → its configured execution portfolio — priced at the connector's live top-of-book
//! mid. No Postgres, no read API: its state is a projection of its three upstreams, rebuilt on
//! restart.
//!
//! The design and the step plan live in `docs/crosser-implementation.md`. This crate is at
//! step 5: config, the strategy feed, the depth feed (the live mid), the Position Service
//! clients, and the crossing loop reconciling the actual positions to the latest targets.

pub mod config;
pub mod crosser;
pub mod crossing;
pub mod depth_feed;
pub mod position_client;
pub mod strategy_feed;

/// Generated wire types for the `flow.v1` APIs the crosser consumes (strategy.proto and
/// position.proto — one protobuf package, one generated module — plus the shared types.proto
/// they import) — client stubs only: the crosser serves no API of its own, so there is no server
/// trait and no reflection descriptor.
pub mod proto {
    tonic::include_proto!("flow.v1");
}

/// Generated wire types for the connector's depth feed, from the vendored
/// `nordpool.marketdata.v1` protos (owned by the nordpool-connector repo). Unlike
/// [`proto`], the server stubs are kept: nothing in this workspace serves the connector's API,
/// so the feed tests mock it from these.
pub mod marketdata {
    tonic::include_proto!("nordpool.marketdata.v1");
}
