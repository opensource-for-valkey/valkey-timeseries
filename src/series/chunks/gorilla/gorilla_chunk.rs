//! Gorilla XOR encoding as a [`SampleCodec`]; the chunk itself is [`XorChunk`].

use super::{GorillaEncoder, GorillaIterator};
use crate::common::{Sample, Timestamp};
use crate::error::TsdbResult;
use crate::iterators::SampleIter;
use crate::series::chunks::chunk::ChunkEncoding;
use crate::series::chunks::range_iter::RangeFiltered;
use crate::series::chunks::xor_chunk::{SampleCodec, XorChunk};
use valkey_module::digest::Digest;
use valkey_module::{RedisModuleIO, ValkeyResult};

/// A chunk of timeseries data encoded using Gorilla XOR encoding.
pub type GorillaChunk = XorChunk<GorillaEncoder>;

pub type GorillaChunkIterator<'a> = RangeFiltered<GorillaIterator<'a>>;

impl SampleCodec for GorillaEncoder {
    type Iter<'a> = GorillaIterator<'a>;
    const ENCODING: ChunkEncoding = ChunkEncoding::Gorilla;

    fn with_soft_cap(max_size: usize) -> Self {
        GorillaEncoder::with_soft_cap(max_size)
    }
    fn set_soft_cap(&mut self, max_size: usize) {
        GorillaEncoder::set_soft_cap(self, max_size)
    }
    #[inline]
    fn push(&mut self, sample: &Sample) -> std::io::Result<()> {
        self.add_sample(sample)
    }
    fn iter(&self) -> GorillaIterator<'_> {
        GorillaEncoder::iter(self)
    }
    fn bytes(&self) -> &[u8] {
        self.buf()
    }
    fn len(&self) -> usize {
        self.num_samples
    }
    fn first_timestamp(&self) -> Timestamp {
        self.first_ts
    }
    fn last_timestamp(&self) -> Timestamp {
        self.last_ts
    }
    fn last_value(&self) -> f64 {
        self.last_value
    }
    fn clear(&mut self) {
        GorillaEncoder::clear(self)
    }
    fn shrink_to_fit(&mut self) {
        GorillaEncoder::shrink_to_fit(self)
    }
    fn rdb_save(&self, rdb: *mut RedisModuleIO) {
        GorillaEncoder::rdb_save(self, rdb)
    }
    fn rdb_load(rdb: *mut RedisModuleIO) -> ValkeyResult<Self> {
        Ok(GorillaEncoder::rdb_load(rdb)?)
    }
    fn serialize(&self, dest: &mut Vec<u8>) {
        GorillaEncoder::serialize(self, dest)
    }
    fn deserialize(buf: &[u8]) -> TsdbResult<Self> {
        GorillaEncoder::deserialize(buf)
    }
    fn debug_digest(&self, dig: &mut Digest) {
        GorillaEncoder::debug_digest(self, dig)
    }
    fn into_sample_iter<'a>(iter: RangeFiltered<GorillaIterator<'a>>) -> SampleIter<'a>
    where
        Self: 'a,
    {
        SampleIter::Gorilla(iter)
    }
}

#[cfg(test)]
mod tests {
    use crate::common::Sample;
    use crate::error::TsdbError;
    use crate::series::DuplicatePolicy;
    use crate::series::chunks::ChunkOps;
    use crate::series::chunks::chunk::Chunk;
    use crate::series::chunks::gorilla::gorilla_chunk::GorillaChunk;
    use crate::tests::generators::DataGenerator;
    use get_size2::GetSize;
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

    /// A corrupt persisted sample count must surface as an error, not an abort.
    ///
    /// `get_range` sizes its result from `num_samples`, which comes straight out of the RDB
    /// or serialized stream. `Vec::with_capacity(usize::MAX)` aborts the process on
    /// capacity overflow (and a merely huge count aborts on allocation failure); the
    /// fallible reserve turns both into a `DecodingError` the caller can report.
    #[test]
    fn test_get_range_with_a_corrupt_sample_count_is_an_error_not_an_abort() {
        let mut chunk = GorillaChunk::with_max_size(1024);
        for sample in generate_samples(10).iter() {
            chunk.add_sample(sample).unwrap();
        }
        chunk.encoder.num_samples = usize::MAX;

        // Direct read, and a read after the count has round-tripped through `deserialize`.
        let mut buf = Vec::new();
        chunk.serialize(&mut buf);
        let loaded = GorillaChunk::deserialize(&buf).unwrap();
        assert_eq!(loaded.len(), usize::MAX);

        for (label, chunk) in [("in-memory", &chunk), ("deserialized", &loaded)] {
            match chunk.get_range(i64::MIN, i64::MAX) {
                Err(TsdbError::DecodingError(msg)) => {
                    assert!(msg.contains("cannot reserve"), "{label}: {msg}")
                }
                other => panic!("{label}: expected a decoding error, got {other:?}"),
            }
        }
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

    /// A sealed chunk's allocation stays within `max_size` plus a few bytes of slack.
    ///
    /// Plain `Vec` doubling used to leave every full chunk at ~2x its data: a 4096-byte
    /// chunk crossed `max_size` on its last append and jumped to an 8192-byte allocation,
    /// which `memory_usage` (and so `TS.INFO memoryUsage`) then reported. Growth is still
    /// geometric below the cap, so filling is not quadratic.
    #[test]
    fn test_sealed_chunk_allocation_is_capped_at_max_size() {
        for max_size in [1024usize, 4096, 16384] {
            let mut chunk = GorillaChunk::with_max_size(max_size);
            let samples = generate_samples(max_size); // more than enough to fill it
            let mut reallocations = 0;
            let mut cap = 0;
            for sample in samples.iter() {
                if chunk.is_full() {
                    break;
                }
                chunk.add_sample(sample).unwrap();
                let c = chunk.encoder.get_heap_size();
                if c != cap {
                    reallocations += 1;
                    cap = c;
                }
            }
            assert!(chunk.is_full());
            let heap = chunk.encoder.get_heap_size();
            assert!(
                heap <= max_size + 32,
                "{max_size}-byte chunk holds a {heap}-byte allocation"
            );
            assert!(heap >= chunk.data_size());
            // 8, 16, 32, ... doubles up to the cap: well under a reallocation per sample.
            assert!(
                reallocations <= 16,
                "{max_size}-byte chunk reallocated {reallocations} times"
            );
        }
    }

    /// Appending past `max_size` (a merge can) keeps doubling rather than reallocating on
    /// every sample.
    #[test]
    fn test_growth_past_max_size_stays_geometric() {
        let mut chunk = GorillaChunk::with_max_size(256);
        let samples = generate_samples(2000);
        let mut reallocations = 0;
        let mut cap = 0;
        for sample in samples.iter() {
            chunk.add_sample(sample).unwrap();
            let c = chunk.encoder.get_heap_size();
            if c != cap {
                reallocations += 1;
                cap = c;
            }
        }
        assert!(chunk.data_size() > 4 * 256);
        assert!(reallocations <= 24, "reallocated {reallocations} times");
    }

    /// The cap survives a serialize/deserialize round trip and a rebuild (split), so a
    /// chunk that came off the wire or out of an RDB is budgeted like a fresh one.
    #[test]
    fn test_loaded_and_rebuilt_chunks_keep_the_cap() {
        let max_size = 1024;
        let mut chunk = GorillaChunk::with_max_size(max_size);
        for sample in generate_samples(40).iter() {
            chunk.add_sample(sample).unwrap();
        }
        let mut buf = Vec::new();
        chunk.serialize(&mut buf);
        let mut loaded = GorillaChunk::deserialize(&buf).unwrap();
        for sample in generate_samples(4000).iter().skip(40) {
            if loaded.is_full() {
                break;
            }
            loaded.add_sample(sample).unwrap();
        }
        assert!(loaded.is_full());
        assert!(loaded.encoder.get_heap_size() <= max_size + 32);

        let right = loaded.split().unwrap();
        assert!(loaded.encoder.get_heap_size() <= max_size + 32);
        assert!(right.encoder.get_heap_size() <= max_size + 32);
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
