//! ElfOnChimp: the ELF erasing layer over the Chimp XOR float codec.
//!
//! [`ChimpCompressor`] / [`ChimpDecompressor`] are the raw codec;
//! [`ChimpChunk`] wraps them as a [`TimeSeriesChunk`](crate::series::chunks::TimeSeriesChunk)
//! variant.

mod chimp_chunk;
mod chimp_iterator;
mod compressor;
mod encoder;

#[cfg(test)]
mod chimp_chunk_tests;

pub use chimp_chunk::*;
pub use chimp_iterator::*;
pub use compressor::*;
pub use encoder::*;
