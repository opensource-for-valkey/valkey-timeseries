use super::{GorillaEncoder, GorillaIterator};
use crate::common::encoding::{try_read_uvarint, write_uvarint};
use crate::common::logging::log_warning;
use crate::common::rdb::{rdb_load_usize, rdb_save_usize};
use crate::common::{Sample, Timestamp};
use crate::config::DEFAULT_CHUNK_SIZE_BYTES;
use crate::error::{TsdbError, TsdbResult};
use crate::iterators::SampleIter;
use crate::series::chunks::chunk::{Chunk, ChunkOps};
use crate::series::chunks::merge::{append_samples, merge_chunk_samples};
use crate::series::{DuplicatePolicy, SampleAddResult};
use get_size2::GetSize;
use std::mem::size_of;
use valkey_module::digest::Digest;
use valkey_module::{RedisModuleIO, ValkeyResult};

/// `GorillaChunk` is a chunk of timeseries data encoded using Gorilla XOR encoding.
#[derive(Debug, Clone, PartialEq, Hash, GetSize)]
pub struct GorillaChunk {
    pub(crate) encoder: GorillaEncoder,
    pub max_size: usize,
}

impl Default for GorillaChunk {
    fn default() -> Self {
        Self::with_max_size(DEFAULT_CHUNK_SIZE_BYTES)
    }
}

impl GorillaChunk {
    pub fn with_max_size(max_size: usize) -> Self {
        Self {
            encoder: GorillaEncoder::new(),
            max_size,
        }
    }

    fn compress(&mut self, samples: &[Sample]) -> TsdbResult {
        let mut encoder = GorillaEncoder::new();
        for sample in samples {
            push_sample(&mut encoder, sample)?;
        }
        self.encoder = encoder;
        Ok(())
    }

    pub fn compression_ratio(&self) -> f64 {
        if self.is_empty() {
            return 0.0;
        }
        let compressed_size = self.encoder.buf().len();
        let uncompressed_size = self.len() * (size_of::<i64>() + size_of::<f64>());
        (uncompressed_size / compressed_size) as f64
    }

    /// Bytes of encoded data. This is what `max_size` bounds, what `TS.INFO DEBUG` reports as a
    /// chunk's `size`, and what `bytes_per_sample` amortizes.
    ///
    /// It deliberately excludes the writer's spare capacity and the encoder's own stack bytes.
    /// The bit stream grows by doubling, so counting `Vec::capacity` reported up to ~2x the data
    /// actually held: a chunk created with `CHUNK_SIZE 4096` reported 8288. `is_full` has always
    /// measured the payload, and `UncompressedChunk::size` is likewise `len`-based, so the three
    /// now agree. It also fed `utilization`, and so `should_split` -- which only governs an
    /// upsert into an already-sealed chunk; the append path is gated by `is_full` and was
    /// unaffected. The full allocation is still reported by `memory_usage`, which is what
    /// `MEMORY USAGE` and `TS.INFO memoryUsage` read.
    pub fn data_size(&self) -> usize {
        self.encoder.buf().len()
    }

    /// estimate remaining capacity based on the current data size and chunk max_size
    pub fn remaining_capacity(&self) -> usize {
        // Saturating: a merge can push a chunk past `max_size`, and a plain subtraction would
        // wrap (this crate builds release without `overflow-checks`). `ChimpChunk` already
        // saturated here.
        self.max_size.saturating_sub(self.data_size())
    }

    /// Estimate the number of samples that can be stored in the remaining capacity
    /// Note that for low sample counts this will be very inaccurate
    pub fn remaining_samples(&self) -> usize {
        if self.len() == 0 {
            return 0;
        }
        self.remaining_capacity() / self.bytes_per_sample()
    }

    pub fn memory_usage(&self) -> usize {
        size_of::<Self>() + self.get_heap_size()
    }

    pub fn iter(&'_ self) -> SampleIter<'_> {
        self.range_iter(i64::MIN, i64::MAX)
    }

    pub fn range_iter(&'_ self, start_ts: Timestamp, end_ts: Timestamp) -> SampleIter<'_> {
        GorillaChunkIterator::new(self, start_ts, end_ts).into()
    }
}

impl ChunkOps for GorillaChunk {
    fn first_timestamp(&self) -> Timestamp {
        self.encoder.first_ts
    }
    fn last_timestamp(&self) -> Timestamp {
        self.encoder.last_ts
    }
    fn len(&self) -> usize {
        self.encoder.num_samples
    }
    fn is_empty(&self) -> bool {
        self.len() == 0
    }
    fn last_value(&self) -> f64 {
        self.encoder.last_value
    }
    fn size(&self) -> usize {
        self.data_size()
    }
    fn max_size(&self) -> usize {
        self.max_size
    }

    fn remove_range(&mut self, start_ts: Timestamp, end_ts: Timestamp) -> TsdbResult<usize> {
        if self.is_empty() || start_ts > self.last_timestamp() || end_ts < self.first_timestamp() {
            return Ok(0);
        }

        let mut new_encoder = GorillaEncoder::new();
        let saved_count = self.len();

        for value in self.encoder.iter() {
            let sample = value?;
            if sample.timestamp >= start_ts && sample.timestamp <= end_ts {
                continue;
            }
            push_sample(&mut new_encoder, &sample)?;
        }

        self.encoder = new_encoder;
        Ok(saved_count - self.len())
    }

    fn add_sample(&mut self, sample: &Sample) -> TsdbResult {
        // if self.is_full() {
        //     return Err(TsdbError::CapacityFull(self.max_size));
        // }
        push_sample(&mut self.encoder, sample)
    }

    fn get_range(&self, start: Timestamp, end: Timestamp) -> TsdbResult<Vec<Sample>> {
        if self.is_empty() {
            return Ok(vec![]);
        }

        let samples = self.range_iter(start, end).collect();
        Ok(samples)
    }

    fn upsert_sample(&mut self, sample: Sample, dp_policy: DuplicatePolicy) -> TsdbResult<usize> {
        let ts = sample.timestamp;
        let mut duplicate_found = false;
        let count = self.len();
        if count == 0 {
            self.add_sample(&sample)?;
            return Ok(1);
        }
        let mut xor_encoder = GorillaEncoder::new();
        let mut iter = self.encoder.iter();
        if ts < self.first_timestamp() {
            // add a sample to the beginning
            push_sample(&mut xor_encoder, &sample)?;
            // Add all existing samples after the new one
            for item in iter {
                let current = item?;
                push_sample(&mut xor_encoder, &current)?;
            }
        } else {
            let mut current = Sample::default();
            // add previous samples
            for item in iter.by_ref() {
                current = item?;
                if current.timestamp >= ts {
                    break;
                }
                push_sample(&mut xor_encoder, &current)?;
            }
            if current.timestamp == ts {
                duplicate_found = true;
                current.value = dp_policy.duplicate_value(ts, current.value, sample.value)?;
                push_sample(&mut xor_encoder, &current)?;
            } else {
                push_sample(&mut xor_encoder, &sample)?;
                // Add the current sample that caused the break (if it exists and is valid)
                if current.timestamp > ts {
                    push_sample(&mut xor_encoder, &current)?;
                }
            }

            for item in iter {
                let current = item?;
                push_sample(&mut xor_encoder, &current)?;
            }
        }

        self.encoder = xor_encoder;
        let size = if duplicate_found { count } else { count + 1 };
        Ok(size)
    }

    fn merge_samples(
        &mut self,
        samples: &[Sample],
        dp_policy: Option<DuplicatePolicy>,
    ) -> TsdbResult<Vec<SampleAddResult>> {
        if samples.is_empty() {
            return Ok(Vec::new());
        }

        // We assume that samples are sorted. Try to optimize by seeing if all samples are past the
        // current chunk's last timestamp.
        let first = samples[0];
        if self.is_empty() || first.timestamp > self.last_timestamp() {
            return append_samples(self, samples);
        }

        let mut encoder = GorillaEncoder::new();
        let result = merge_chunk_samples(self.iter(), samples, dp_policy, |sample| {
            push_sample(&mut encoder, &sample)
        })?;

        self.encoder = encoder;
        Ok(result)
    }

    fn optimize(&mut self) -> TsdbResult {
        self.encoder.shrink_to_fit();
        Ok(())
    }

    fn is_full(&self) -> bool {
        let data_size = self.encoder.buf().len();
        data_size >= self.max_size
    }

    fn bytes_per_sample(&self) -> usize {
        use crate::series::chunks::MIN_SAMPLES_FOR_BPS_ESTIMATE;
        let count = self.len();
        if count < MIN_SAMPLES_FOR_BPS_ESTIMATE {
            return size_of::<Sample>() / 2;
        }
        self.data_size() / count
    }

    fn clear(&mut self) {
        self.encoder.clear();
    }

    fn set_data(&mut self, samples: &[Sample]) -> TsdbResult<()> {
        debug_assert!(!samples.is_empty());
        self.compress(samples)?;
        // todo: complain if size > max_size
        Ok(())
    }
}

impl Chunk for GorillaChunk {
    fn split(&mut self) -> TsdbResult<Self>
    where
        Self: Sized,
    {
        let mut left_chunk = GorillaEncoder::new();
        // `with_max_size`, not `default()`: the upper half has to inherit the budget the series
        // was created with. `default()` stamps `DEFAULT_CHUNK_SIZE_BYTES` on it, silently
        // discarding the user's `CHUNK_SIZE` -- and `save_rdb` writes `max_size` out, so the
        // wrong budget survives a reload. `ChimpChunk::split` already inherits it.
        let mut right_chunk = GorillaChunk::with_max_size(self.max_size);

        if self.is_empty() {
            return Ok(self.clone());
        }

        let mid = self.len() / 2;
        for (i, value) in self.encoder.iter().enumerate() {
            let sample = value?;
            if i < mid {
                // todo: handle min and max timestamps
                push_sample(&mut left_chunk, &sample)?;
            } else {
                push_sample(&mut right_chunk.encoder, &sample)?;
            }
        }
        self.encoder = left_chunk;
        Ok(right_chunk)
    }

    fn save_rdb(&self, rdb: *mut RedisModuleIO) {
        rdb_save_usize(rdb, self.max_size);
        self.encoder.rdb_save(rdb);
    }

    fn load_rdb(rdb: *mut RedisModuleIO, _enc_ver: i32) -> ValkeyResult<Self> {
        let max_size = rdb_load_usize(rdb)?;
        let encoder = GorillaEncoder::rdb_load(rdb)?;
        let chunk = GorillaChunk { encoder, max_size };
        Ok(chunk)
    }

    fn serialize(&self, dest: &mut Vec<u8>) {
        write_uvarint(dest, self.max_size as u64);
        self.encoder.serialize(dest);
    }

    fn deserialize(buf: &[u8]) -> TsdbResult<Self> {
        let mut buf = buf;
        let max_size = try_read_uvarint(&mut buf).map_err(|_| TsdbError::ChunkDecoding)?;
        let encoder = GorillaEncoder::deserialize(buf)?;
        Ok(GorillaChunk {
            encoder,
            max_size: max_size as usize,
        })
    }

    fn debug_digest(&self, dig: &mut Digest) {
        self.encoder.debug_digest(dig);
        dig.add_long_long(self.max_size as i64);
    }
}

#[inline]
fn push_sample(encoder: &mut GorillaEncoder, sample: &Sample) -> TsdbResult {
    encoder.add_sample(sample).map_err(|e| {
        // replace debug stderr output with structured logging
        log_warning(format!("Error adding sample: {e:?}"));
        TsdbError::CannotAddSample(*sample)
    })
}

pub struct GorillaChunkIterator<'a> {
    inner: GorillaIterator<'a>,
    start: Timestamp,
    end: Timestamp,
    init: bool,
}

impl<'a> GorillaChunkIterator<'a> {
    pub fn new(chunk: &'a GorillaChunk, start: Timestamp, end: Timestamp) -> Self {
        let inner = GorillaIterator::new(&chunk.encoder);
        Self {
            inner,
            start,
            end,
            init: false,
        }
    }

    fn next_internal(&mut self) -> Option<Sample> {
        match self.inner.next() {
            Some(Ok(sample)) => {
                if sample.timestamp > self.end {
                    return None;
                }
                Some(sample)
            }
            #[cfg(debug_assertions)]
            Some(Err(err)) => {
                // use structured logging instead of printing to stderr in debug builds
                log_warning(format!("Error decoding sample: {err:?}"));
                None
            }
            #[cfg(not(debug_assertions))]
            Some(Err(_)) => None,
            None => None,
        }
    }
}

impl Iterator for GorillaChunkIterator<'_> {
    type Item = Sample;

    fn next(&mut self) -> Option<Self::Item> {
        if !self.init {
            self.init = true;

            while let Some(sample) = self.next_internal() {
                if sample.timestamp < self.start {
                    continue;
                }
                if sample.timestamp <= self.end {
                    return Some(sample);
                }
            }

            return None;
        }
        self.next_internal()
    }
}

#[cfg(test)]
mod tests {
    use crate::common::Sample;
    use crate::series::DuplicatePolicy;
    use crate::series::chunks::ChunkOps;
    use crate::series::chunks::chunk::Chunk;
    use crate::series::chunks::gorilla::gorilla_chunk::GorillaChunk;
    use crate::tests::generators::DataGenerator;
    use std::time::Duration;

    fn generate_samples(count: usize) -> Vec<Sample> {
        DataGenerator::builder()
            .samples(count)
            .start(1000)
            .decimal_digits(3)
            .interval(Duration::from_millis(1000))
            .build()
            .generate()
    }

    /// A split half inherits the source's `max_size`.
    ///
    /// This used to build the upper half with `default()`, stamping `DEFAULT_CHUNK_SIZE_BYTES`
    /// on it and discarding whatever `CHUNK_SIZE` the series was created with -- measured at 4096
    /// for a source of 1024. Since `save_rdb` persists `max_size`, the wrong budget also survived
    /// a reload. `ChimpChunk` and `UncompressedChunk` both already inherited it.
    #[test]
    fn test_split_inherits_the_source_max_size() {
        for max_size in [256usize, 1024, 16384, 65536] {
            let mut chunk = GorillaChunk::with_max_size(max_size);
            for sample in generate_samples(100).iter() {
                chunk.add_sample(sample).unwrap();
            }

            let right = chunk.split().unwrap();

            assert_eq!(chunk.max_size, max_size, "source lost its budget");
            assert_eq!(
                right.max_size, max_size,
                "the upper half of a {max_size}-byte chunk was given a {}-byte budget",
                right.max_size,
            );
        }
    }

    /// Splitting must not lose or reorder samples while it redistributes them.
    #[test]
    fn test_split_preserves_every_sample() {
        let expected = generate_samples(101);
        let mut chunk = GorillaChunk::with_max_size(8192);
        for sample in expected.iter() {
            chunk.add_sample(sample).unwrap();
        }

        let right = chunk.split().unwrap();

        let actual: Vec<Sample> = chunk.iter().chain(right.iter()).collect();
        assert_eq!(actual, expected);
        assert_eq!(chunk.len() + right.len(), expected.len());
    }

    /// Iterating an empty chunk yields nothing rather than panicking.
    ///
    /// `GorillaIterator::new` computed `num_samples - 1` unguarded. `get_range` checks
    /// `is_empty` first, but `iter`/`range_iter` do not -- and an empty gorilla chunk is
    /// ordinary: `TimeSeries::append_chunk` creates one before the first sample lands. In a
    /// debug build that subtraction panicked with "attempt to subtract with overflow"; with no
    /// FFI panic barrier in the module, that is a server abort.
    #[test]
    fn test_iterating_an_empty_chunk_is_not_an_underflow() {
        let chunk = GorillaChunk::with_max_size(1024);
        assert!(chunk.is_empty());

        assert_eq!(chunk.iter().count(), 0);
        assert_eq!(chunk.range_iter(i64::MIN, i64::MAX).count(), 0);
        assert_eq!(chunk.range_iter(0, 1000).count(), 0);
        assert_eq!(chunk.get_range(0, 1000).unwrap(), vec![]);
    }

    /// Splitting a single-sample chunk leaves one half empty, and callers iterate both.
    ///
    /// Which half is empty differs by encoding, so this deliberately does not assert a side:
    /// `mid` is `len / 2` = 0 here, so gorilla and chimp send the only sample to the *upper*
    /// half and leave the source empty, while `UncompressedChunk::split` special-cases `len == 1`
    /// and returns an empty upper half instead. Both halves must iterate without underflowing
    /// and the sample must survive exactly once.
    ///
    /// (`split` is only reached from `should_split`/`is_full`, which need a chunk at or past
    /// `max_size` >= 48 bytes, so a one-sample split does not arise in practice. That asymmetry
    /// is untested elsewhere and left as-is; this only pins the iterator behaviour.)
    #[test]
    fn test_iterating_the_empty_half_of_a_split_is_not_an_underflow() {
        let sample = Sample {
            timestamp: 1000,
            value: 1.0,
        };
        let mut chunk = GorillaChunk::with_max_size(1024);
        chunk.add_sample(&sample).unwrap();

        let right = chunk.split().unwrap();

        assert!(
            chunk.is_empty() || right.is_empty(),
            "a one-sample split should leave one half empty"
        );
        let all: Vec<Sample> = chunk.iter().chain(right.iter()).collect();
        assert_eq!(all, vec![sample]);
    }

    fn decompress(chunk: &GorillaChunk) -> Vec<Sample> {
        chunk.iter().collect()
    }

    #[test]
    fn test_chunk_compress() {
        let mut chunk = GorillaChunk::with_max_size(16384);
        let data = generate_samples(1000);

        for sample in data.iter() {
            chunk.add_sample(sample).unwrap();
        }
        assert_eq!(chunk.len(), data.len());
        assert_eq!(chunk.first_timestamp(), data[0].timestamp);
        assert_eq!(chunk.last_timestamp(), data[data.len() - 1].timestamp);
        assert_eq!(chunk.last_value(), data[data.len() - 1].value);
    }

    #[test]
    fn test_clear() {
        let mut chunk = GorillaChunk::with_max_size(16384);
        let data = generate_samples(500);

        for datum in data.iter() {
            chunk.add_sample(datum).unwrap();
        }

        assert_eq!(chunk.len(), data.len());
        chunk.clear();
        assert_eq!(chunk.len(), 0);
        assert_eq!(chunk.first_timestamp(), 0);
        assert_eq!(chunk.last_timestamp(), 0);
    }

    #[test]
    fn test_upsert() {
        for chunk_size in (64..8192).step_by(64) {
            const SAMPLE_COUNT: usize = 200;
            let samples = generate_samples(SAMPLE_COUNT);
            let mut chunk = GorillaChunk::with_max_size(chunk_size);

            let sample_count = samples.len();
            for sample in samples.into_iter() {
                chunk
                    .upsert_sample(sample, DuplicatePolicy::KeepLast)
                    .unwrap();
            }
            assert_eq!(chunk.len(), sample_count);
        }
    }

    #[test]
    fn test_split() {
        const COUNT: usize = 500;
        let samples = generate_samples(COUNT);
        let mut chunk = GorillaChunk::with_max_size(16384);

        for sample in samples.iter() {
            chunk.add_sample(sample).unwrap();
        }

        let count = samples.len();
        let mid = count / 2;

        let right = chunk.split().unwrap();
        assert_eq!(chunk.len(), mid);
        assert_eq!(right.len(), mid);

        let (left_samples, right_samples) = samples.split_at(mid);

        let right_decompressed = decompress(&right);
        assert_eq!(right_decompressed, right_samples);

        let left_decompressed = decompress(&chunk);
        assert_eq!(left_decompressed, left_samples);
    }

    #[test]
    fn test_split_odd() {
        const COUNT: usize = 51;
        let samples = generate_samples(COUNT);
        let mut chunk = GorillaChunk::default();

        for sample in samples.iter() {
            chunk.add_sample(sample).unwrap();
        }

        let count = samples.len();
        let mid = count / 2;

        let right = chunk.split().unwrap();
        assert_eq!(chunk.len(), mid);
        assert_eq!(right.len(), mid + 1);

        let (left_samples, right_samples) = samples.split_at(mid);

        let right_decompressed = decompress(&right);
        assert_eq!(right_decompressed, right_samples);

        let left_decompressed = decompress(&chunk);
        assert_eq!(left_decompressed, left_samples);
    }

    #[test]
    fn test_iter() {
        let mut chunk = GorillaChunk::default();
        let data = generate_samples(1000);

        chunk.set_data(&data).unwrap();

        let actual: Vec<_> = chunk.iter().collect();
        assert_eq!(actual, data);
    }

    #[test]
    fn test_remove_range() {
        let mut chunk = GorillaChunk::with_max_size(16384);
        let samples = generate_samples(100);

        for sample in samples.iter() {
            chunk.add_sample(sample).unwrap();
        }

        // Remove a range that covers the first half of the samples
        let mid = samples.len() / 2;
        let start_ts = samples[0].timestamp;
        let mid_ts = samples[mid].timestamp;
        let removed_count = chunk.remove_range(start_ts, mid_ts).unwrap();
        // range is inclusive, so we would have deleted mid + 1
        assert_eq!(removed_count, mid + 1);

        // Ensure the remaining samples are correct
        let remaining_samples: Vec<_> = chunk.iter().collect();
        let expected_samples = &samples[mid + 1..];
        assert_eq!(remaining_samples, expected_samples);

        // Remove a range that covers the remaining samples
        let end_ts = samples[samples.len() - 1].timestamp;
        let removed_count = chunk.remove_range(mid_ts, end_ts).unwrap();
        assert_eq!(removed_count, mid - 1);

        // Ensure the chunk is empty
        assert!(chunk.is_empty());
    }

    #[test]
    fn test_remove_range_no_overlap() {
        let mut chunk = GorillaChunk::with_max_size(16384);
        let samples = generate_samples(100);

        for sample in samples.iter() {
            chunk.add_sample(sample).unwrap();
        }

        // Attempt to remove a range that does not overlap with any samples
        let start_ts = samples[samples.len() - 1].timestamp + 1;
        let end_ts = start_ts + 1000;
        let removed_count = chunk.remove_range(start_ts, end_ts).unwrap();
        assert_eq!(removed_count, 0);

        // Ensure all samples are still present
        let remaining_samples: Vec<_> = chunk.iter().collect();
        assert_eq!(remaining_samples, samples);
    }
}
