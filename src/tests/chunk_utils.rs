//! Chunk construction helpers shared by benchmarks and the `compression_report`
//! tool.

use crate::common::Sample;
use crate::config::DEFAULT_CHUNK_SIZE_BYTES;
use crate::series::chunks::{ChunkEncoding, ChunkOps, TimeSeriesChunk};

pub const CHUNK_SIZE_1K: usize = 1024;
pub const CHUNK_SIZE_4K: usize = DEFAULT_CHUNK_SIZE_BYTES;
pub const CHUNK_SIZE_64K: usize = 64 * 1024;

pub fn chunk_size_id(chunk_size: usize) -> &'static str {
    match chunk_size {
        CHUNK_SIZE_1K => "1k",
        CHUNK_SIZE_4K => "4k",
        CHUNK_SIZE_64K => "64k",
        _ => "custom",
    }
}

/// Fill a chunk from `data`, stopping once it reports full.
pub fn build_chunk(encoding: ChunkEncoding, chunk_size: usize, data: &[Sample]) -> TimeSeriesChunk {
    let mut chunk = TimeSeriesChunk::new(encoding, chunk_size);
    for sample in data {
        if chunk.is_full() {
            break;
        }
        chunk
            .add_sample(sample)
            .expect("sample should append to benchmark chunk");
    }
    chunk
}

/// As [`build_chunk`], but also reports how many samples were consumed.
pub fn build_chunk_until_full(
    encoding: ChunkEncoding,
    chunk_size: usize,
    data: &[Sample],
) -> (TimeSeriesChunk, usize) {
    let mut chunk = TimeSeriesChunk::new(encoding, chunk_size);
    let mut count = 0;
    for sample in data {
        if chunk.is_full() {
            break;
        }
        chunk
            .add_sample(sample)
            .expect("sample should append to benchmark chunk");
        count += 1;
    }
    (chunk, count)
}

/// The longest prefix of `data` that fits in a chunk of `chunk_size` bytes.
pub fn filled_prefix(data: &[Sample], encoding: ChunkEncoding, chunk_size: usize) -> &[Sample] {
    &data[..filled_prefix_len(data, encoding, chunk_size)]
}

/// Length of the longest prefix of `data` that fits in a chunk of `chunk_size`
/// bytes.
pub fn filled_prefix_len(data: &[Sample], encoding: ChunkEncoding, chunk_size: usize) -> usize {
    let mut chunk = TimeSeriesChunk::new(encoding, chunk_size);
    let mut count = 0;
    for sample in data {
        if chunk.is_full() {
            break;
        }
        chunk
            .add_sample(sample)
            .expect("sample should append to benchmark chunk");
        count += 1;
    }
    count
}

/// The number of bytes a chunk has actually written, for compression reporting.
///
/// This existed to work around [`ChunkOps::size`] not being comparable across encodings: gorilla
/// and chimp used to return a `get_size()` heap footprint, counting the bit stream's *capacity*
/// and so jumping to the next power of two as it grew, while uncompressed returned the bytes in
/// use. A ratio built on that compared allocator slack rather than compression — at a 64 KiB
/// budget gorilla reported ~128 KiB against uncompressed's exact ~64 KiB.
///
/// `size()` now reports used bytes for every encoding, so this is a thin alias kept for the
/// reporting tools that name it. Its assertion in `test_chunk_size_reports_encoded_bytes` is what
/// keeps the two definitions from drifting apart again.
pub fn encoded_size(chunk: &TimeSeriesChunk) -> usize {
    chunk.size()
}
