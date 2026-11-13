//! The Position Service: system of record for portfolio positions on the continuous
//! intraday market. An append-only priced trade ledger — transfers between configured
//! portfolios plus market fills folded from the order gateways' execution streams — whose
//! fold is the in-memory position projection that answers every read, served over gRPC
//! (`flow.v1.PositionService`): a unary `Transfer` mutation and a
//! snapshot-then-updates `Subscribe` stream per portfolio.
//!
//! Implementation in progress; the step plan and the design deviations (portfolio as a
//! configured string, multiple gateways) live in
//! `docs/position-service-implementation.md`.

pub mod channel;
pub mod config;
pub mod feed;
pub mod ledger;
pub mod server;
pub mod storage;

/// Generated wire types for this service's own `flow.v1` API (position.proto plus
/// the shared types.proto it imports).
pub mod proto {
    tonic::include_proto!("flow.v1");

    /// Descriptor set for gRPC server reflection.
    pub const FILE_DESCRIPTOR_SET: &[u8] =
        tonic::include_file_descriptor_set!("position_descriptor");
}

/// Generated wire types for the PowerBot gateway's API (powerbot_gateway.proto).
/// Same protobuf package as [`proto`] (`flow.v1`), so it is compiled into
/// its own out_dir and included as a distinct module — see build.rs.
pub mod gateway {
    include!(concat!(env!("OUT_DIR"), "/gateway/flow.v1.rs"));
}
