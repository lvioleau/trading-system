fn main() {
    // The Position Service's proto (the gap feed) and the Contract Service's (the
    // tradeability feed), plus the shared types.proto they import; the connector's vendored
    // depth proto (the book feed) and the PowerBot gateway's proto (orders out, executions
    // back) compile separately below — the depth proto is its own protobuf package, and the
    // gateway proto shares package `flow.v1` with position/contract, so it needs
    // its own out_dir (the position-service wrinkle). All are rebuild triggers.
    let flow_protos = [
        "../../proto/flow/v1/position.proto",
        "../../proto/flow/v1/contract.proto",
        "../../proto/flow/v1/types.proto",
    ];
    let marketdata_protos = [
        "../../proto/nordpool/marketdata/v1/depth.proto",
        "../../proto/nordpool/marketdata/v1/types.proto",
    ];
    let gateway_protos = ["../../proto/flow/v1/powerbot_gateway.proto"];
    for proto in flow_protos
        .iter()
        .chain(&marketdata_protos)
        .chain(&gateway_protos)
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

    // Client stubs only: the executor is a client of every API it touches and serves none of
    // its own — no server trait, no reflection, so no descriptor set either.
    tonic_build::configure()
        .build_server(false)
        .compile_protos(&flow_protos[..2], &["../../proto"])
        .expect("failed to compile the flow protos");

    // The connector's depth proto keeps its server stubs: nothing in this workspace serves
    // the connector's API, so the feed tests mock it from these (the crosser's pattern).
    tonic_build::configure()
        .compile_protos(&marketdata_protos[..1], &["../../proto"])
        .expect("failed to compile the marketdata protos");

    // The gateway proto shares package `flow.v1` with position.proto and
    // contract.proto, so compiling it into OUT_DIR would overwrite their generated file.
    // It gets its own out_dir and lands in a distinct module (`crate::gateway_proto`);
    // server stubs kept — the mock-gateway tests serve from them.
    let out_dir = std::path::PathBuf::from(std::env::var("OUT_DIR").unwrap());
    let gateway_out = out_dir.join("gateway");
    std::fs::create_dir_all(&gateway_out).expect("creating the gateway proto out_dir");
    tonic_build::configure()
        .out_dir(&gateway_out)
        .compile_protos(&gateway_protos, &["../../proto"])
        .expect("failed to compile the gateway protos");
}

/// Whether a `protoc` executable is resolvable on `PATH`.
fn protoc_on_path() -> bool {
    let Some(path) = std::env::var_os("PATH") else {
        return false;
    };
    std::env::split_paths(&path)
        .any(|dir| dir.join("protoc").is_file() || dir.join("protoc.exe").is_file())
}
