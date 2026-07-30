/// This module exists to perform conversions between local types and the
/// generated protobuf types.
mod chunks;
mod conversions;
pub(crate) mod filters;
#[cfg(test)]
mod proptest_roundtrip;
pub(crate) mod symbol_table;

/// Generated protobuf types for the fanout wire contract.
///
/// Checked in beside the schema at `proto/v1/generated/` rather than pulled from
/// `OUT_DIR` via `include!`, so the types are navigable in an IDE and a schema
/// change is a reviewable diff. `build.rs` regenerates the file and fails the
/// build if it falls out of step with `proto/v1/`.
///
/// The `#[path]` is needed because the file lives outside `src/`; it is relative
/// to this directory, and the filename comes from the protobuf package name.
#[path = "../../../proto/v1/generated/valkey_timeseries.fanout.v1.rs"]
pub mod generated;

pub use conversions::*;
pub use generated::*;
