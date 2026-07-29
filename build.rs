use std::io;

fn main() -> io::Result<()> {
    // Parse .proto sources into descriptors without requiring `protoc`.
    let file_descriptors = protox::compile(
        &[
            "proto/valkey_timeseries/fanout/v1/request.proto",
            "proto/valkey_timeseries/fanout/v1/response.proto",
        ],
        &["proto/"],
    )
    .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;

    let mut config = prost_build::Config::new();
    // prost-build defaults strip_enum_prefix=true, so adding enum-name
    // prefixes in the .proto files (AGGREGATION_TYPE_ALL, etc.) produces
    // zero Rust-side diff — the generated variants stay `All`, `Min`, etc.
    config
        .compile_fds(file_descriptors)
        .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;

    // Re-run only when a .proto file changes, not on every Rust edit.
    println!("cargo:rerun-if-changed=proto/valkey_timeseries/fanout/v1/common.proto");
    println!("cargo:rerun-if-changed=proto/valkey_timeseries/fanout/v1/filters.proto");
    println!("cargo:rerun-if-changed=proto/valkey_timeseries/fanout/v1/request.proto");
    println!("cargo:rerun-if-changed=proto/valkey_timeseries/fanout/v1/response.proto");

    Ok(())
}
