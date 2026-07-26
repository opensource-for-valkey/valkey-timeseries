//! `ChimpChunk`: a timeseries chunk whose values go through the ELF erasing
//! layer on top of the Chimp XOR codec, with delta-of-delta timestamps
//! interleaved in the same bit stream.
//!
//! Structurally a twin of [`DeXorChunk`](crate::series::chunks::DeXorChunk) —
//! the codec is sequential, so every mutation (`remove_range`, `upsert`,
//! `merge`, `split`) re-encodes from an iterator rather than patching in place.

use super::chimp_iterator::ChimpIterator;
use super::compressor::ChimpCompressor;
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

#[derive(Debug, Clone, PartialEq, Hash, GetSize)]
pub struct ChimpChunk {
    pub(crate) encoder: ChimpCompressor,
    pub max_size: usize,
}

impl Default for ChimpChunk {
    fn default() -> Self {
        Self::with_max_size(DEFAULT_CHUNK_SIZE_BYTES)
    }
}

impl ChimpChunk {
    pub fn with_max_size(max_size: usize) -> Self {
        Self {
            encoder: ChimpCompressor::new(),
            max_size,
        }
    }

    fn compress(&mut self, samples: &[Sample]) -> TsdbResult {
        let mut encoder = ChimpCompressor::new();
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
        let compressed_size = self.encoder.bytes().len();
        if compressed_size == 0 {
            return 0.0;
        }
        let uncompressed_size = self.len() * (size_of::<i64>() + size_of::<f64>());
        (uncompressed_size / compressed_size) as f64
    }

    pub fn data_size(&self) -> usize {
        self.encoder.get_size()
    }

    /// Estimate the remaining capacity based on the current data size and `max_size`.
    pub fn remaining_capacity(&self) -> usize {
        self.max_size.saturating_sub(self.data_size())
    }

    /// Estimate how many more samples fit in the remaining capacity.
    ///
    /// Very inaccurate at low sample counts, where header overhead dominates.
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
        ChimpChunkIterator::new(self, start_ts, end_ts).into()
    }
}

impl ChunkOps for ChimpChunk {
    fn first_timestamp(&self) -> Timestamp {
        self.encoder.first_timestamp()
    }

    fn last_timestamp(&self) -> Timestamp {
        self.encoder.last_timestamp()
    }

    fn len(&self) -> usize {
        self.encoder.count() as usize
    }

    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    fn last_value(&self) -> f64 {
        self.encoder.last_value()
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

        let mut new_encoder = ChimpCompressor::new();
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
        push_sample(&mut self.encoder, sample)
    }

    fn get_range(&self, start: Timestamp, end: Timestamp) -> TsdbResult<Vec<Sample>> {
        if self.is_empty() {
            return Ok(vec![]);
        }
        Ok(self.range_iter(start, end).collect())
    }

    fn upsert_sample(&mut self, sample: Sample, dp_policy: DuplicatePolicy) -> TsdbResult<usize> {
        let ts = sample.timestamp;
        let mut duplicate_found = false;
        let count = self.len();
        if count == 0 {
            self.add_sample(&sample)?;
            return Ok(1);
        }

        let mut encoder = ChimpCompressor::new();
        let mut iter = self.encoder.iter();

        if ts < self.first_timestamp() {
            push_sample(&mut encoder, &sample)?;
            for item in iter {
                let current = item?;
                push_sample(&mut encoder, &current)?;
            }
        } else {
            let mut current = Sample::default();
            for item in iter.by_ref() {
                current = item?;
                if current.timestamp >= ts {
                    break;
                }
                push_sample(&mut encoder, &current)?;
            }
            if current.timestamp == ts {
                duplicate_found = true;
                current.value = dp_policy.duplicate_value(ts, current.value, sample.value)?;
                push_sample(&mut encoder, &current)?;
            } else {
                push_sample(&mut encoder, &sample)?;
                if current.timestamp > ts {
                    push_sample(&mut encoder, &current)?;
                }
            }

            for item in iter {
                let current = item?;
                push_sample(&mut encoder, &current)?;
            }
        }

        self.encoder = encoder;
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

        // Samples are assumed sorted; the common append-only case skips the
        // full re-encode.
        let first = samples[0];
        if self.is_empty() || first.timestamp > self.last_timestamp() {
            return append_samples(self, samples);
        }

        let mut encoder = ChimpCompressor::new();
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
        self.encoder.bytes().len() >= self.max_size
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
        self.compress(samples)
    }
}

impl Chunk for ChimpChunk {
    fn split(&mut self) -> TsdbResult<Self> {
        if self.is_empty() {
            return Ok(self.clone());
        }

        let mut left = ChimpCompressor::new();
        let mut right = ChimpChunk::with_max_size(self.max_size);

        let mid = self.len() / 2;
        for (i, value) in self.encoder.iter().enumerate() {
            let sample = value?;
            if i < mid {
                push_sample(&mut left, &sample)?;
            } else {
                push_sample(&mut right.encoder, &sample)?;
            }
        }

        self.encoder = left;
        Ok(right)
    }

    fn save_rdb(&self, rdb: *mut RedisModuleIO) {
        rdb_save_usize(rdb, self.max_size);
        self.encoder.rdb_save(rdb);
    }

    fn load_rdb(rdb: *mut RedisModuleIO, _enc_ver: i32) -> ValkeyResult<Self> {
        let max_size = rdb_load_usize(rdb)?;
        let encoder = ChimpCompressor::rdb_load(rdb)?;
        Ok(ChimpChunk { encoder, max_size })
    }

    fn serialize(&self, dest: &mut Vec<u8>) {
        write_uvarint(dest, self.max_size as u64);
        self.encoder.serialize(dest);
    }

    fn deserialize(buf: &[u8]) -> TsdbResult<Self> {
        let mut buf = buf;
        let max_size = try_read_uvarint(&mut buf).map_err(|_| TsdbError::ChunkDecoding)?;
        let encoder = ChimpCompressor::deserialize(buf)?;
        Ok(ChimpChunk {
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
fn push_sample(encoder: &mut ChimpCompressor, sample: &Sample) -> TsdbResult {
    encoder.add(*sample).map_err(|e| {
        log_warning(format!("chimp: error adding sample: {e:?}"));
        TsdbError::CannotAddSample(*sample)
    })
}

pub struct ChimpChunkIterator<'a> {
    inner: ChimpIterator<'a>,
    start: Timestamp,
    end: Timestamp,
    init: bool,
}

impl<'a> ChimpChunkIterator<'a> {
    pub fn new(chunk: &'a ChimpChunk, start: Timestamp, end: Timestamp) -> Self {
        Self {
            inner: ChimpIterator::new(&chunk.encoder),
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
                log_warning(format!("chimp: error decoding sample: {err:?}"));
                None
            }
            #[cfg(not(debug_assertions))]
            Some(Err(_)) => None,
            None => None,
        }
    }
}

impl Iterator for ChimpChunkIterator<'_> {
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
