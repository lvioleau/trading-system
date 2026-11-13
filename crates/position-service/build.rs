fn main() {
    // Compiled entry point plus its import (the shared types.proto), both rebuild triggers.
    let protos = [
        "../../proto/flow/v1/position.proto",
        "../../proto/flow/v1/types.proto",
    ];
    // The order-gateway protos (owned by crates/powerbot-gateway), compiled separately
    // below.
    let gateway_protos = ["../../proto/flow/v1/powerbot_gateway.proto"];
    for proto in protos.iter().chain(&gateway_protos) {
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
        .file_descriptor_set_path(out_dir.join("position_descriptor.bin"))
        .compile_protos(&protos[..1], &["../../proto"])
        .expect("failed to compile position protos");

    // The gateway protos share package `flow.v1` with position.proto, so
    // compiling both into OUT_DIR would overwrite one `flow.v1.rs` with the other.
    // They get their own out_dir and land in a distinct module (`crate::gateway`); no
    // descriptor set — reflection serves this service's own API only.
    let gateway_out = out_dir.join("gateway");
    std::fs::create_dir_all(&gateway_out).expect("creating the gateway proto out_dir");
    tonic_build::configure()
        .out_dir(&gateway_out)
        .compile_protos(&gateway_protos, &["../../proto"])
        .expect("failed to compile gateway protos");
}

/// Whether a `protoc` executable is resolvable on `PATH`.
fn protoc_on_path() -> bool {
    let Some(path) = std::env::var_os("PATH") else {
        return false;
    };
    std::env::split_paths(&path)
        .any(|dir| dir.join("protoc").is_file() || dir.join("protoc.exe").is_file())
}
