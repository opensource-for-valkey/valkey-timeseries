//! Test and benchmark support code.
//!
//! Compiled for unit tests, and for benchmarks / tools via the `test-utils`
//! feature (see the `[dev-dependencies]` self-dependency in `Cargo.toml`).

pub mod chunk_utils;
pub mod generators;
#[cfg(test)]
pub mod test_utils;
#[cfg(test)]
pub(crate) use test_utils::assertions::*;
