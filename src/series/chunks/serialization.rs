use crate::common::Sample;
use crate::series::chunks::{ChimpChunk, ChunkOps, TimeSeriesChunk, UncompressedChunk};
use valkey_module::{ValkeyError, ValkeyResult};

// for future compatibility
const VERSION: u32 = 1;

/// Sample count from which compressing a fan-out payload starts to pay for
/// itself.
///
/// Below this a compressed chunk can be *larger* than the raw samples: the
/// chunk header and the XOR encoders' warm-up dominate before the per-sample
/// savings arrive. 16 is the first count at which no measured workload shape
/// inflates — against the worst shape (`periodic`, full-mantissa values) Chimp
/// sits at 1.07x the raw size at 10 samples, 1.00x at 12 and 0.91x at 16.
///
/// Re-derive with `tools/wire_report.sh`.
pub const WIRE_COMPRESSION_MIN_SAMPLES: usize = 16;

/// Builds the chunk that carries `samples` across the network.
///
/// Chimp is both smaller and cheaper to decode than Gorilla from roughly 12 samples up,
/// so Gorilla has no window in which it is the best choice. Chimp encodes ~1.4x slower,
/// which is the right way round for fan-out — shards encode their own slice in parallel, while the
/// coordinator decodes every shard's response serially.
///
/// This is for the wire only. Results that stay on one node are not worth
/// compressing, since nothing reads them but the reply serializer.
pub fn samples_to_chunk(samples: &[Sample]) -> ValkeyResult<TimeSeriesChunk> {
    let mut chunk = if samples.len() >= WIRE_COMPRESSION_MIN_SAMPLES {
        TimeSeriesChunk::Chimp(ChimpChunk::default())
    } else {
        TimeSeriesChunk::Uncompressed(UncompressedChunk::default())
    };
    chunk
        .set_data(samples)
        .map_err(|e| ValkeyError::String(format!("Failed to set chunk data: {e}")))?;
    Ok(chunk)
}

/// [`samples_to_chunk`], falling back to an uncompressed chunk if the encoder
/// rejects the samples.
///
/// Encoding writes to an in-memory buffer and has no failure mode in practice,
/// but a fan-out response is the wrong place to give up: shipping the samples
/// uncompressed costs bandwidth, whereas dropping them corrupts the result.
pub fn samples_to_chunk_lossless(samples: Vec<Sample>) -> TimeSeriesChunk {
    match samples_to_chunk(&samples) {
        Ok(chunk) => chunk,
        Err(e) => {
            crate::common::logging::log_warning(format!(
                "failed to compress {} samples for fanout, sending uncompressed: {e}",
                samples.len()
            ));
            TimeSeriesChunk::Uncompressed(UncompressedChunk::from_vec(samples))
        }
    }
}
