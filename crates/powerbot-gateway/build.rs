fn main() {
    // Compiled entry point plus its import (the shared types.proto), both rebuild triggers.
    let protos = [
        "../../proto/flow/v1/powerbot_gateway.proto",
        "../../proto/flow/v1/types.proto",
    ];
    for proto in protos {
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
        // Emit the descriptor set so the server can expose gRPC reflection.
        .file_descriptor_set_path(out_dir.join("powerbot_gateway_descriptor.bin"))
        .compile_protos(&protos, &["../../proto"])
        .expect("failed to compile powerbot gateway protos");

    // Bake the build's git SHA into the binary (`env!("GIT_SHA")`) for the Datadog
    // `version:` tag — the crate version doesn't change per deploy, the SHA does.
    // "unknown" keeps non-git builds (tarballs, vendored CI) working. Mirrors the
    // sibling nordpool-connectors' build.rs.
    let sha = std::process::Command::new("git")
        .args(["rev-parse", "--short", "HEAD"])
        .output()
        .ok()
        .filter(|out| out.status.success())
        .map(|out| String::from_utf8_lossy(&out.stdout).trim().to_string())
        .unwrap_or_else(|| "unknown".to_string());
    println!("cargo:rustc-env=GIT_SHA={sha}");
    // Re-stamp when the checked-out commit moves (HEAD ref or the branch it points at).
    println!("cargo:rerun-if-changed=../../.git/HEAD");
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
