pub mod binops;
mod common;
pub mod engine;
mod error;
mod exec;
mod functions;
mod hashers;
mod model;
mod time;
mod utils;

pub mod optimizer;
#[cfg(test)]
pub(crate) mod promqltest;

/// The PromQL push-down messages are part of the one fanout wire contract
/// (`proto/v1/promql.proto`), so they land in the same generated module as the
/// TS.* fanout types and share `Label`, `Sample` and `SeriesSelector` with them.
/// Re-exported here so the promql-side imports stay short.
pub(crate) use crate::commands::fanout_codec::generated;

// Re-export commonly used types from submodules for public API convenience.
pub use engine::QueryOptions;

use crate::promql::engine::register_fanout_commands;
pub use error::*;
pub use exec::*;
pub use model::*;
use valkey_module::ValkeyResult;

pub(crate) fn register_promql() -> ValkeyResult<()> {
    register_fanout_commands()
}
