use crate::common::Sample;
use crate::series::chunks::{ChunkOps, GorillaChunk, TimeSeriesChunk, UncompressedChunk};
use valkey_module::{ValkeyError, ValkeyResult};

// for future compatibility
const VERSION: u32 = 1;

pub fn samples_to_chunk(samples: &[Sample]) -> ValkeyResult<TimeSeriesChunk> {
    let mut chunk = if samples.len() >= 100 {
        TimeSeriesChunk::Chimp(super::ChimpChunk::default())
    } else if samples.len() >= 20 {
        TimeSeriesChunk::Gorilla(GorillaChunk::default())
    } else {
        TimeSeriesChunk::Uncompressed(UncompressedChunk::default())
    };
    chunk
        .set_data(samples)
        .map_err(|e| ValkeyError::String(format!("Failed to set chunk data: {e}")))?;
    Ok(chunk)
}
