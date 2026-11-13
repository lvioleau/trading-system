//! Mock Strategy: the reference `flow.v1.StrategyService` server and the crosser's test
//! double. It subscribes to the Contract Service, maintains a configured number of tradable
//! contracts, and streams an oscillating target-position record per contract to subscribers —
//! each booking the live top-of-book mid from the connector's depth feed — the executable
//! definition of the strategy → crosser wire contract until the real Strategy Layer repo owns it.
//!
//! The design and the step plan live in `docs/crosser-implementation.md` (step 1).

pub mod config;
pub mod contract_feed;
pub mod depth_feed;
pub mod driver;
pub mod position_feed;
pub mod py_strategy;
pub mod server;

/// Generated wire types for this service's `flow.v1` API (strategy.proto plus the shared
/// types.proto it imports).
pub mod proto {
    tonic::include_proto!("flow.v1");

    /// Descriptor set for gRPC server reflection.
    pub const FILE_DESCRIPTOR_SET: &[u8] =
        tonic::include_file_descriptor_set!("strategy_descriptor");
}

/// Generated wire types for the Contract Service's API (contract.proto), consumed here as the
/// tradable-contracts feed. Same protobuf package as [`proto`] (`flow.v1`), so it is
/// compiled into its own out_dir and included as a distinct module — see build.rs.
pub mod contract {
    include!(concat!(env!("OUT_DIR"), "/contract/flow.v1.rs"));
}

/// Generated wire types for the Position Service's API (position.proto), consumed here as the
/// actual-position feed. Same out_dir arrangement as [`contract`], and for the same reason.
pub mod position {
    include!(concat!(env!("OUT_DIR"), "/position/flow.v1.rs"));
}

/// Generated wire types for the connector's depth feed, from the vendored
/// `nordpool.marketdata.v1` protos (owned by the nordpool-connector repo). The server
/// stubs are kept: nothing in this workspace serves the connector's API, so the feed tests mock
/// it from these.
pub mod marketdata {
    tonic::include_proto!("nordpool.marketdata.v1");
}
