use crate::common::{Sample, Timestamp};
use crate::error::{TsdbError, TsdbResult};
use crate::error_consts;
use crate::iterators::SampleIter;
use crate::series::{DuplicatePolicy, SampleAddResult};
use enum_dispatch::enum_dispatch;
use get_size2::GetSize;
use std::fmt::Display;
use valkey_module::digest::Digest;
use valkey_module::{ValkeyError, ValkeyResult, raw};

/// Smallest allowed chunk size in bytes for `CHUNK_SIZE`.
pub const MIN_CHUNK_SIZE: usize = 48;
/// Largest allowed chunk size in bytes for `CHUNK_SIZE`.
pub const MAX_CHUNK_SIZE: usize = 1048576;

/// The minimum number of samples before `bytes_per_sample()` can produce a
/// reliable amortized estimate for compressed encodings. Below this
/// threshold, header overhead dominates, and the ratio is inflated;
/// implementations should fall back to a conservative constant.
pub const MIN_SAMPLES_FOR_BPS_ESTIMATE: usize = 16;

#[derive(Copy, Clone, Debug, Default, PartialEq, GetSize, Hash)]
#[non_exhaustive]
#[repr(u8)]
pub enum ChunkEncoding {
    Uncompressed = 1,
    Gorilla = 2,
    #[default]
    Chimp = 3,
}

impl ChunkEncoding {
    pub const fn name(&self) -> &'static str {
        match self {
            ChunkEncoding::Uncompressed => "uncompressed",
            ChunkEncoding::Gorilla => "gorilla",
            ChunkEncoding::Chimp => "chimp",
        }
    }

    pub fn is_compressed(&self) -> bool {
        *self != ChunkEncoding::Uncompressed
    }
}

impl Display for ChunkEncoding {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.name())
    }
}

impl TryFrom<u8> for ChunkEncoding {
    type Error = ValkeyError;
    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            1 => Ok(ChunkEncoding::Uncompressed),
            2 => Ok(ChunkEncoding::Gorilla),
            3 => Ok(ChunkEncoding::Chimp),
            _ => Err(ValkeyError::Str(error_consts::INVALID_CHUNK_ENCODING)),
        }
    }
}

impl TryFrom<&str> for ChunkEncoding {
    type Error = ValkeyError;
    fn try_from(s: &str) -> Result<Self, Self::Error> {
        if let Some(compression) = parse_encoding(s) {
            return Ok(compression);
        }
        Err(ValkeyError::Str(error_consts::INVALID_CHUNK_ENCODING))
    }
}

impl TryFrom<String> for ChunkEncoding {
    type Error = ValkeyError;
    fn try_from(s: String) -> Result<Self, Self::Error> {
        ChunkEncoding::try_from(&s[..])
    }
}

fn parse_encoding(encoding: &str) -> Option<ChunkEncoding> {
    // `fnc_map` rather than `map`: `map` keeps its values in a `static`, and the default
    // encoding comes from a (non-const) `Default` impl.
    hashify::fnc_map_ignore_case!(
        encoding.as_bytes(),
        "compressed" => Some(ChunkEncoding::default()),
        "uncompressed" => Some(ChunkEncoding::Uncompressed),
        "gorilla" => Some(ChunkEncoding::Gorilla),
        "chimp" => Some(ChunkEncoding::Chimp),
        _ => None
    )
}

/// Core chunk operations, forwarded to each encoding by `enum_dispatch`.
///
/// Every method here is pure forwarding at the enum level; anything needing `Sized` or
/// custom enum logic lives on [`Chunk`] or on `TimeSeriesChunk` itself.
///
/// # Capacity
///
/// - `size()` is encoded payload bytes, what `TS.INFO DEBUG` reports as a chunk's size.
///   Allocation slack is excluded; `TimeSeriesChunk::memory_usage` counts it.
/// - `max_size()` is the byte budget the chunk was created with (`CHUNK_SIZE`).
/// - `is_full()` is `size() >= max_size()` for compressed encodings and
///   `len() >= max_elements` for uncompressed.
/// - `bytes_per_sample()` is `size() / len()` floored to 1 for compressed encodings, or
///   `SAMPLE_SIZE / 2` below [`MIN_SAMPLES_FOR_BPS_ESTIMATE`] samples; uncompressed is
///   always `SAMPLE_SIZE`.
/// - `TimeSeriesChunk::utilization` and `estimate_remaining_sample_capacity` derive from
///   the above.
/// - Two split rules exist. The upsert path splits a chunk once
///   `utilization() >= SPLIT_FACTOR` (1.2), via `TimeSeriesChunk::should_split`. The merge
///   path splits once `is_full() && size() > max + max / SPLIT_SLACK_DIVISOR` (just over
///   1.25), via `TimeSeries::needs_split`. The thresholds are nearly equal and no reason for
///   keeping two is recorded; they are a candidate for unification.
#[enum_dispatch]
pub trait ChunkOps {
    /// The chunk's samples with timestamps in `[start, end]`, ascending. A decode error ends
    /// the iteration early (logged in debug builds).
    fn range_iter(&self, start: Timestamp, end: Timestamp) -> SampleIter<'_>;
    /// Every sample in the chunk, ascending.
    fn iter(&self) -> SampleIter<'_> {
        self.range_iter(i64::MIN, i64::MAX)
    }
    /// `0` when the chunk is empty; check `is_empty()` before relying on it.
    fn first_timestamp(&self) -> Timestamp;
    /// `0` when the chunk is empty; check `is_empty()` before relying on it.
    fn last_timestamp(&self) -> Timestamp;
    fn len(&self) -> usize;
    fn is_empty(&self) -> bool {
        self.len() == 0
    }
    /// `0.0` when the chunk is empty; prefer `TimeSeriesChunk::last_sample()`.
    fn last_value(&self) -> f64;
    fn size(&self) -> usize;
    fn max_size(&self) -> usize;
    fn remove_range(&mut self, start_ts: Timestamp, end_ts: Timestamp) -> TsdbResult<usize>;
    /// Append `sample`, which must be newer than every sample in the chunk.
    ///
    /// Uncompressed refuses once `max_elements` is reached (`CapacityFull`); compressed
    /// encodings never refuse. The series checks `is_full()` first (`add_sample_internal`),
    /// and `merge::append_samples` treats `CapacityFull` as a batch-level failure.
    fn add_sample(&mut self, sample: &Sample) -> TsdbResult<()>;
    fn get_range(&self, start: Timestamp, end: Timestamp) -> TsdbResult<Vec<Sample>>;
    /// Insert `sample`, or merge it into an existing sample with the same timestamp per
    /// `dp_policy`. Returns `Ok(true)` when the chunk grew by one sample, `Ok(false)` when an
    /// existing sample at that timestamp was updated.
    fn upsert_sample(&mut self, sample: Sample, dp_policy: DuplicatePolicy) -> TsdbResult<bool>;

    /// Efficiently merge a slice of Sample objects into the chunk.
    ///
    /// * `samples` - A slice of Sample objects to be merged into the chunk.
    /// * `dp_policy` - An optional DuplicatePolicy that specifies how to handle duplicate samples.
    ///
    /// ### Returns
    /// The method returns a TsdbResult containing a vector of SampleAddResult objects, which indicate
    /// the result of adding each sample.
    fn merge_samples(
        &mut self,
        samples: &[Sample],
        dp_policy: Option<DuplicatePolicy>,
    ) -> TsdbResult<Vec<SampleAddResult>>;

    fn optimize(&mut self) -> TsdbResult<()> {
        Ok(())
    }

    fn is_full(&self) -> bool;

    /// Returns the average number of bytes consumed per sample in this chunk.
    ///
    /// For compressed encodings this is an amortized estimate (`data_size() / len()`).
    /// When the chunk contains fewer than [`MIN_SAMPLES_FOR_BPS_ESTIMATE`] samples,
    /// header overhead dominates, and the ratio is unreliable; implementations
    /// should fall back to a conservative constant in that case.
    fn bytes_per_sample(&self) -> usize;
    fn clear(&mut self);
    /// Replace the chunk's contents with `samples` (sorted ascending, may be empty — an empty
    /// slice empties the chunk). `max_size` is not enforced here; see `TimeSeries::needs_split`.
    fn set_data(&mut self, samples: &[Sample]) -> TsdbResult<()>;
}

/// Extended chunk trait for persistence and operations that return `Self`.
///
/// Methods in this trait require `Sized` or have custom dispatch logic at the
/// enum level (e.g. prepending an encoding byte), so they cannot be
/// auto-dispatched by `enum_dispatch`.
pub trait Chunk: ChunkOps + Sized {
    fn split(&mut self) -> TsdbResult<Self>;

    fn save_rdb(&self, rdb: *mut raw::RedisModuleIO);
    fn load_rdb(rdb: *mut raw::RedisModuleIO, enc_ver: i32) -> ValkeyResult<Self>;

    fn serialize(&self, dest: &mut Vec<u8>);

    fn deserialize(buf: &[u8]) -> TsdbResult<Self>;

    fn debug_digest(&self, dig: &mut Digest);
}

/// Validates a configured chunk size (in bytes).
///
/// The size must be in the inclusive range [`MIN_CHUNK_SIZE`, `MAX_CHUNK_SIZE`]
/// and be a multiple of 8. The multiple-of-8 requirement preserves word alignment
/// expected by bit-packed chunk encoders.
pub(crate) fn validate_chunk_size(chunk_size_bytes: usize) -> TsdbResult<()> {
    fn get_error_result() -> TsdbResult<()> {
        let msg = format!(
            "TSDB: CHUNK_SIZE value must be a multiple of 8 in the range [{MIN_CHUNK_SIZE} .. {MAX_CHUNK_SIZE}]"
        );
        Err(TsdbError::InvalidConfiguration(msg))
    }

    if !(MIN_CHUNK_SIZE..=MAX_CHUNK_SIZE).contains(&chunk_size_bytes) {
        return get_error_result();
    }

    if !chunk_size_bytes.is_multiple_of(8) {
        return get_error_result();
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::ChunkEncoding;

    #[test]
    fn chunk_encoding_u8_roundtrip() {
        let encodings = [
            ChunkEncoding::Uncompressed,
            ChunkEncoding::Gorilla,
            ChunkEncoding::Chimp,
        ];

        for &enc in &encodings {
            let b = enc as u8;
            let decoded = ChunkEncoding::try_from(b).expect("valid encoding byte");
            assert_eq!(decoded, enc, "roundtrip failed for encoding {:?}", enc);
        }
    }

    #[test]
    fn chunk_encoding_invalid_u8_returns_err() {
        // The defined discriminants are contiguous over 1..=3; everything else is invalid.
        for invalid in [0u8, 4u8, 5u8, 255u8] {
            assert!(
                ChunkEncoding::try_from(invalid).is_err(),
                "byte {invalid} should not decode to an encoding"
            );
        }
    }
}
