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

#[cfg(test)]
pub(crate) mod promqltest;

pub mod generated {
    include!(concat!(env!("OUT_DIR"), "/valkey_timeseries.promql.rs"));
}

use crate::promql::engine::register_fanout_commands;
pub use error::*;
pub use exec::*;
pub use model::*;
use valkey_module::ValkeyResult;

pub(crate) fn register_promql() -> ValkeyResult<()> {
    register_fanout_commands()
}
