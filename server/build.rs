//! Generates the tonic gRPC bindings from `proto/evento/v1/store.proto`.
//!
//! We compile the schema with [`protox`] (a pure-Rust protobuf compiler) into a
//! `FileDescriptorSet` and hand that to `tonic-build`. This keeps the build
//! hermetic — no system `protoc` binary is required on the build host.

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let proto = "proto/evento/v1/store.proto";
    let include = "proto";

    let fds = protox::compile([proto], [include])?;

    tonic_build::configure()
        .build_server(true)
        .build_client(true)
        .compile_fds(fds)?;

    println!("cargo:rerun-if-changed={proto}");
    Ok(())
}
