//! The Contract Service: system of record for Nord Pool contract reference data. Syncs the
//! connector's contract feed (one subscription, filtered to the configured delivery areas)
//! into Postgres — the durable record, boot-folded into an in-memory serving index that
//! answers every read — and serves it over gRPC (`flow.v1.ContractService`): a
//! seed-then-deltas subscription per delivery area carrying the live (not yet delivered)
//! set, plus a unary delivery-window query answered over the full history.

pub mod channel;
pub mod config;
pub mod feed;
pub mod server;
pub mod storage;
pub mod store;

/// Generated wire types for this service's own `flow.v1` API.
pub mod proto {
    tonic::include_proto!("flow.v1");

    /// Descriptor set for gRPC server reflection.
    pub const FILE_DESCRIPTOR_SET: &[u8] = tonic::include_file_descriptor_set!("flow_descriptor");
}

/// Generated wire types for the connector's contract feed, from the vendored
/// `nordpool.marketdata.v1` protos (owned by the nordpool-connector repo).
pub mod marketdata {
    tonic::include_proto!("nordpool.marketdata.v1");
}
