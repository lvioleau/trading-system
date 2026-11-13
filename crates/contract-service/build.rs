fn main() {
    // Compiled entry points; flow imports the shared types.proto, which must also trigger
    // a rebuild.
    let flow = ["../../proto/flow/v1/contract.proto"];
    let flow_imports = ["../../proto/flow/v1/types.proto"];
    let marketdata = [
        "../../proto/nordpool/marketdata/v1/types.proto",
        "../../proto/nordpool/marketdata/v1/contract.proto",
    ];
    for proto in flow.iter().chain(&flow_imports).chain(&marketdata) {
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
    // The flow proto is compiled on its own so the reflection descriptor set advertises only
    // the service actually served here — not the vendored connector protos.
    tonic_build::configure()
        .file_descriptor_set_path(out_dir.join("flow_descriptor.bin"))
        .compile_protos(&flow, &["../../proto"])
        .expect("failed to compile flow protos");
    tonic_build::configure()
        .compile_protos(&marketdata, &["../../proto"])
        .expect("failed to compile marketdata protos");
}

/// Whether a `protoc` executable is resolvable on `PATH`.
fn protoc_on_path() -> bool {
    let Some(path) = std::env::var_os("PATH") else {
        return false;
    };
    std::env::split_paths(&path)
        .any(|dir| dir.join("protoc").is_file() || dir.join("protoc.exe").is_file())
}
