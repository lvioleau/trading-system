fn main() {
    // The tester consumes three APIs; it hosts none, so generate clients only.
    // The gateway proto lives in this repo; the marketdata protos are vendored
    // copies of the nordpool connector's stream-server contract (see proto/).
    let protos = [
        "../../proto/flow/v1/powerbot_gateway.proto",
        "../../proto/flow/v1/types.proto",
        "../../proto/nordpool/marketdata/v1/types.proto",
        "../../proto/nordpool/marketdata/v1/depth.proto",
        "../../proto/nordpool/marketdata/v1/contract.proto",
    ];
    // The Position Service protos share package `flow.v1` with
    // powerbot_gateway.proto, so compiling both into OUT_DIR would overwrite one
    // `flow.v1.rs` with the other: they land in their own out_dir / module
    // (`crate::proto::position`). Mirrors position-service's build.rs.
    let position_protos = ["../../proto/flow/v1/position.proto"];
    for proto in protos.iter().chain(&position_protos) {
        println!("cargo:rerun-if-changed={proto}");
    }

    // Fall back to a vendored protoc when the environment doesn't provide one (e.g. CI without
    // protobuf-compiler installed). Locally the Nix/direnv shell supplies its own on PATH.
    if std::env::var_os("PROTOC").is_none() && !protoc_on_path() {
        let protoc = protoc_bin_vendored::protoc_bin_path().expect("vendored protoc not available");
        // SAFETY: single-threaded build script; set before any proto compilation.
        unsafe { std::env::set_var("PROTOC", protoc) };
    }

    tonic_build::configure()
        .build_server(false)
        .compile_protos(&protos, &["../../proto"])
        .expect("failed to compile tester protos");

    // Separate out_dir so its `flow.v1.rs` doesn't clobber the gateway's.
    let out_dir = std::path::PathBuf::from(std::env::var("OUT_DIR").unwrap());
    let position_out = out_dir.join("position");
    std::fs::create_dir_all(&position_out).expect("creating the position proto out_dir");
    tonic_build::configure()
        .build_server(false)
        .out_dir(&position_out)
        .compile_protos(&position_protos, &["../../proto"])
        .expect("failed to compile position protos");
}

/// Whether a `protoc` binary is discoverable on `PATH` (each `PATH` entry joined
/// with `protoc`, plus `.exe` on Windows). Lets us skip the vendored fallback
/// when the environment already provides one.
fn protoc_on_path() -> bool {
    let Some(path) = std::env::var_os("PATH") else {
        return false;
    };
    let names = if cfg!(windows) {
        &["protoc.exe", "protoc"][..]
    } else {
        &["protoc"][..]
    };
    std::env::split_paths(&path)
        .flat_map(|dir| names.iter().map(move |name| dir.join(name)))
        .any(|candidate| candidate.is_file())
}
