fn main() {
    // The strategy feed's and the Position Service's protos, plus the shared types.proto they
    // both import; all are rebuild triggers. Both APIs live in package `flow.v1`, so a
    // single compile pass emits them into one generated module.
    let protos = [
        "../../proto/flow/v1/strategy.proto",
        "../../proto/flow/v1/position.proto",
        "../../proto/flow/v1/types.proto",
    ];
    // The connector's depth feed (vendored protos, owned by nordpool-connector), priced from as
    // the live mid. Its own package, so it compiles alongside without colliding.
    let marketdata = [
        "../../proto/nordpool/marketdata/v1/types.proto",
        "../../proto/nordpool/marketdata/v1/depth.proto",
    ];
    for proto in protos.iter().chain(&marketdata) {
        println!("cargo:rerun-if-changed={proto}");
    }

    // Fall back to a vendored protoc when the environment doesn't provide one (e.g. CI without
    // protobuf-compiler installed). Locally the Nix/direnv shell supplies its own on PATH.
    if std::env::var_os("PROTOC").is_none() && !protoc_on_path() {
        let protoc = protoc_bin_vendored::protoc_bin_path().expect("vendored protoc not available");
        // SAFETY: single-threaded build script; set before any proto compilation.
        unsafe { std::env::set_var("PROTOC", protoc) };
    }

    // Client stubs only: the crosser is a client of every API it touches and serves none of its
    // own — no server trait, no reflection, so no descriptor set either.
    tonic_build::configure()
        .build_server(false)
        .compile_protos(&protos[..2], &["../../proto"])
        .expect("failed to compile the flow protos");

    // The depth proto keeps its server stub: the strategy has `crates/strategy` as its reference
    // server, but nothing in this workspace serves the connector's API — the black-box feed
    // tests mock it from these stubs.
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
