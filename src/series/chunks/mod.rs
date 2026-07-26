mod chunk;
mod elf64;
mod dexor;
mod gorilla;
mod merge;
#[cfg(test)]
mod encoding_latency_tests;
#[cfg(test)]
mod proptest_roundtrip;
mod serialization;
mod stream;
mod timeseries_chunk;
#[cfg(test)]
mod timeseries_chunk_tests;
mod tsxor;
mod uncompressed;
pub(crate) mod utils;
mod xor2;
mod chimp;

pub use tsxor::*;
pub use chimp::*;
pub use chunk::*;
pub use dexor::*;
pub use gorilla::*;
pub use merge::*;
// pub use serialization::*;
pub use timeseries_chunk::*;
pub use uncompressed::*;
pub(crate) use xor2::*;
