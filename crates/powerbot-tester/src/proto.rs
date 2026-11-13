//! Generated gRPC stubs. Two packages, compiled together in `build.rs`:
//! `flow.v1` (the gateway's order + execution services) and
//! `nordpool.marketdata.v1` (the connector's contract + depth feeds).

/// Powerbot gateway: order entry and own-order/own-trade execution feeds.
pub mod gateway {
    tonic::include_proto!("flow.v1");
}

/// Nordpool connector market data: contract reference data and depth books.
pub mod marketdata {
    tonic::include_proto!("nordpool.marketdata.v1");
}

/// Position Service: per-portfolio net positions (`flow.v1.PositionService`).
/// Same protobuf package as [`gateway`], so it is compiled into its own out_dir
/// and included as a distinct module — see build.rs. Only its `PositionService`
/// client and `Position`/`PositionUpdate` types are used here.
pub mod position {
    include!(concat!(env!("OUT_DIR"), "/position/flow.v1.rs"));
}
