fn main() {
    // This service's own proto plus the shared types.proto it imports; both are rebuild triggers.
    let protos = [
        "../../proto/flow/v1/strategy.proto",
        "../../proto/flow/v1/types.proto",
    ];
    // The Contract Service proto, consumed here as a client (the tradable-contracts feed). Owned
    // by the contract-service crate; compiled separately below.
    let contract_protos = ["../../proto/flow/v1/contract.proto"];
    // The Position Service proto, consumed here as a client (the actual-position feed). Owned by
    // the position-service crate; compiled separately below, like contract.proto.
    let position_protos = ["../../proto/flow/v1/position.proto"];
    // The connector's depth feed (vendored protos, owned by nordpool-connector), whose top of
    // book the emitted mids are booked from. Its own package, so it compiles alongside without
    // colliding.
    let marketdata = [
        "../../proto/nordpool/marketdata/v1/types.proto",
        "../../proto/nordpool/marketdata/v1/depth.proto",
    ];
    for proto in protos
        .iter()
        .chain(&contract_protos)
        .chain(&position_protos)
        .chain(&marketdata)
    {
        println!("cargo:rerun-if-changed={proto}");
    }

    // Fall back to a vendored protoc when the environment doesn't provide one (e.g. CI without
    // protobuf-compiler installed). Locally the Nix/direnv shell supplies its own on PATH.
    if std::env::var_os("PROTOC").is_none() && !protoc_on_path() {
        let protoc = protoc_bin_vendored::protoc_bin_path().expect("vendored protoc not available");
        // SAFETY: single-threaded build script; set before any proto compilation.
        unsafe { std::env::set_var("PROTOC", protoc) };
    }

    let out_dir = std::path::PathBuf::from(std::env::var("OUT_DIR").unwrap());
    tonic_build::configure()
        .file_descriptor_set_path(out_dir.join("strategy_descriptor.bin"))
        .compile_protos(&protos[..1], &["../../proto"])
        .expect("failed to compile strategy protos");

    // contract.proto shares package `flow.v1` with strategy.proto, so compiling both into
    // OUT_DIR would overwrite one `flow.v1.rs` with the other. It gets its own out_dir and
    // lands in a distinct module (`crate::contract`); no descriptor set — reflection serves this
    // service's own API only.
    let contract_out = out_dir.join("contract");
    std::fs::create_dir_all(&contract_out).expect("creating the contract proto out_dir");
    tonic_build::configure()
        .out_dir(&contract_out)
        .compile_protos(&contract_protos, &["../../proto"])
        .expect("failed to compile the contract-service proto");

    // position.proto: same package-collision arrangement as contract.proto — its own out_dir,
    // included as `crate::position`.
    let position_out = out_dir.join("position");
    std::fs::create_dir_all(&position_out).expect("creating the position proto out_dir");
    tonic_build::configure()
        .out_dir(&position_out)
        .compile_protos(&position_protos, &["../../proto"])
        .expect("failed to compile the position-service proto");

    // The depth proto keeps its server stub: nothing in this workspace serves the connector's
    // API, so the black-box feed tests mock it from these. No descriptor set — reflection serves
    // this service's own API only.
    tonic_build::configure()
        .compile_protos(&marketdata, &["../../proto"])
        .expect("failed to compile the marketdata protos");
}

/// Whether a `protoc` executable is resolvable on `PATH`.
fn protoc_on_path() -> bool {
    let Some(path) = std::env::var_os("PATH") else {
        return false;
    };
    std::env::split_paths(&path)
        .any(|dir| dir.join("protoc").is_file() || dir.join("protoc.exe").is_file())
}
