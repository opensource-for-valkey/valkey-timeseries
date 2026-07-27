use crate::common::rdb::{rdb_load_u8, rdb_save_u8};
use crate::common::{Sample, Timestamp};
use crate::config::SPLIT_FACTOR;
use crate::error::{TsdbError, TsdbResult};
use crate::error_consts;
use crate::iterators::{FilteredSampleIterator, SampleIter};
use crate::series::chunks::chimp::ChimpChunk;
use crate::series::chunks::dexor::DeXorChunk;
use crate::series::chunks::utils::{filter_samples_by_value, filter_timestamp_slice};
use crate::series::types::ValueFilter;
use crate::series::{
    DuplicatePolicy, SampleAddResult,
    chunks::{Chunk, ChunkEncoding, ChunkOps, GorillaChunk, UncompressedChunk},
};
use core::mem::size_of;
use enum_dispatch::enum_dispatch;
use get_size2::GetSize;
use std::cmp::Ordering;
use valkey_module::digest::Digest;
use valkey_module::{RedisModuleIO, ValkeyResult};

#[derive(Debug, Clone, Hash, PartialEq, GetSize)]
#[enum_dispatch(ChunkOps)]
pub enum TimeSeriesChunk {
    Uncompressed(UncompressedChunk),
    Gorilla(GorillaChunk),
    DeXor(DeXorChunk),
    Chimp(ChimpChunk),
}

impl TimeSeriesChunk {
    pub fn new(compression: ChunkEncoding, chunk_size: usize) -> Self {
        use TimeSeriesChunk::*;
        match compression {
            ChunkEncoding::Uncompressed => {
                let chunk = UncompressedChunk::with_max_size(chunk_size);
                Uncompressed(chunk)
            }
            ChunkEncoding::Gorilla => {
                let chunk = GorillaChunk::with_max_size(chunk_size);
                Gorilla(chunk)
            }
            ChunkEncoding::DeXor => DeXor(DeXorChunk::with_max_size(chunk_size)),
            ChunkEncoding::Chimp => Chimp(ChimpChunk::with_max_size(chunk_size)),
        }
    }

    pub fn get_encoding(&self) -> ChunkEncoding {
        match self {
            TimeSeriesChunk::Uncompressed(_) => ChunkEncoding::Uncompressed,
            TimeSeriesChunk::Gorilla(_) => ChunkEncoding::Gorilla,
            TimeSeriesChunk::DeXor(_) => ChunkEncoding::DeXor,
            TimeSeriesChunk::Chimp(_) => ChunkEncoding::Chimp,
        }
    }

    pub fn utilization(&self) -> f64 {
        let total = self.max_size();
        if total == 0 {
            return 0.0;
        }
        self.size() as f64 / total as f64
    }

    /// Get an estimate of the remaining capacity in the number of samples.
    ///
    /// **Accuracy note:** For compressed encodings, this estimate relies on
    /// `bytes_per_sample()`, which is an amortized ratio of `data_size() / len()`.
    /// The estimate is unreliable when the chunk contains fewer than
    /// [`MIN_SAMPLES_FOR_BPS_ESTIMATE`](super::MIN_SAMPLES_FOR_BPS_ESTIMATE) samples
    /// (header overhead dominates); in that case `bytes_per_sample()` falls back to
    /// a conservative constant (`SAMPLE_SIZE / 2`). Callers that use this for
    /// pre-allocation should treat the result as a rough upper bound, not an exact count.
    pub fn estimate_remaining_sample_capacity(&self) -> usize {
        let used = self.size();
        let total = self.max_size();
        if used >= total {
            return 0;
        }
        let remaining = total - used;
        let bytes_per_sample = self.bytes_per_sample();
        if bytes_per_sample == 0 {
            return 0;
        }
        remaining / bytes_per_sample
    }

    pub fn is_timestamp_in_range(&self, ts: Timestamp) -> bool {
        ts >= self.first_timestamp() && ts <= self.last_timestamp()
    }

    pub fn is_contained_by_range(&self, start_ts: Timestamp, end_ts: Timestamp) -> bool {
        self.first_timestamp() >= start_ts && self.last_timestamp() <= end_ts
    }

    pub fn overlaps(&self, start_time: i64, end_time: i64) -> bool {
        let first_time = self.first_timestamp();
        let last_time = self.last_timestamp();
        first_time <= end_time && last_time >= start_time
    }

    pub fn has_samples_in_range(&self, start_time: Timestamp, end_time: Timestamp) -> bool {
        if self.is_empty() || !self.overlaps(start_time, end_time) {
            return false;
        }

        if self.range_iter(start_time, end_time).next().is_some() {
            return true;
        }

        false
    }

    // todo: make this a trait method
    pub fn iter(&self) -> Box<dyn Iterator<Item = Sample> + '_> {
        use TimeSeriesChunk::*;
        match self {
            Uncompressed(chunk) => Box::new(chunk.iter()),
            Gorilla(chunk) => Box::new(chunk.iter()),
            DeXor(chunk) => Box::new(chunk.iter()),
            Chimp(chunk) => Box::new(chunk.iter()),
        }
    }

    pub fn range_iter(&'_ self, start: Timestamp, end: Timestamp) -> SampleIter<'_> {
        use TimeSeriesChunk::*;
        match self {
            Uncompressed(chunk) => chunk.range_iter(start, end),
            Gorilla(chunk) => chunk.range_iter(start, end),
            DeXor(chunk) => chunk.range_iter(start, end),
            Chimp(chunk) => chunk.range_iter(start, end),
        }
    }

    // NOTE! this function assumes that timestamps are sorted ascending and de-duped
    pub fn samples_by_timestamps(&self, timestamps: &[Timestamp]) -> TsdbResult<Vec<Sample>> {
        if self.len() == 0 || timestamps.is_empty() {
            return Ok(vec![]);
        }

        let mut samples = Vec::with_capacity(timestamps.len());

        let first_ts = timestamps[0];
        let mut index: usize = 0;

        let first_timestamp = first_ts.max(self.first_timestamp());
        let last_timestamp = timestamps[timestamps.len() - 1].min(self.last_timestamp());

        for sample in self.range_iter(first_timestamp, last_timestamp) {
            if index >= timestamps.len() {
                break;
            }
            let mut first_ts = timestamps[index];
            match sample.timestamp.cmp(&first_ts) {
                Ordering::Less => continue,
                Ordering::Equal => {
                    samples.push(sample);
                    index += 1;
                }
                Ordering::Greater => {
                    // Advance the index while ensuring we don't go out of bounds.
                    // Use a flag to avoid pushing the same sample twice (when we break out of the loop
                    // after a match and then perform the post-loop equality check).
                    let mut matched = false;
                    while index + 1 < timestamps.len() && first_ts < sample.timestamp {
                        index += 1;
                        first_ts = timestamps[index];
                        if first_ts == sample.timestamp {
                            samples.push(sample);
                            index += 1;
                            matched = true;
                            break;
                        }
                    }
                    // If we reached the last timestamp (or didn't match in the loop), check equality once more
                    // without advancing past bounds. Only push if we haven't already matched above.
                    if !matched && index < timestamps.len() && first_ts == sample.timestamp {
                        samples.push(sample);
                        index += 1;
                    }
                }
            }
        }

        Ok(samples)
    }

    pub(crate) fn get_range_filtered(
        &self,
        start_timestamp: Timestamp,
        end_timestamp: Timestamp,
        timestamp_filter: &Option<Vec<Timestamp>>,
        value_filter: Option<ValueFilter>,
    ) -> Vec<Sample> {
        let mut samples = if let Some(ts_filter) = timestamp_filter {
            let filtered_ts = filter_timestamp_slice(ts_filter, start_timestamp, end_timestamp);
            self.samples_by_timestamps(&filtered_ts)
                .unwrap_or_default()
                .into_iter()
                .collect()
        } else {
            self.get_range(start_timestamp, end_timestamp)
                .unwrap_or_default()
                .into_iter()
                .collect()
        };

        if let Some(value_filter) = value_filter {
            filter_samples_by_value(&mut samples, &value_filter);
        }
        samples
    }

    /// Merge a range of samples into this chunk.
    /// If the chunk is full or the other chunk is empty, it returns 0.
    /// Duplicate values are handled according to `duplicate_policy`.
    /// Samples with timestamps before `retention_threshold` will be ignored, whether
    /// they fall with the given range [start_ts..end_ts].
    /// Returns the number of samples merged.
    pub fn merge_range(
        &mut self,
        sample_iter: impl Iterator<Item = Sample>,
        duplicate_policy: Option<DuplicatePolicy>,
    ) -> TsdbResult<usize> {
        if self.is_full() {
            return Ok(0);
        }
        let samples = sample_iter.collect::<Vec<Sample>>();
        let res = self.merge_samples(&samples, duplicate_policy)?;
        Ok(res.iter().filter(|s| s.is_ok()).count())
    }

    pub fn memory_usage(&self) -> usize {
        size_of::<Self>() + self.get_heap_size()
    }

    pub fn should_split(&self) -> bool {
        self.utilization() >= SPLIT_FACTOR
    }

    pub(crate) fn upsert(
        &mut self,
        sample: Sample,
        policy: DuplicatePolicy,
    ) -> (usize, SampleAddResult) {
        match self.upsert_sample(sample, policy) {
            Ok(size) => (size, SampleAddResult::Ok(sample)),
            Err(TsdbError::DuplicateSample(_)) => (0, SampleAddResult::Duplicate),
            Err(_) => (0, SampleAddResult::Error(error_consts::CANNOT_ADD_SAMPLE)),
        }
    }

    pub fn last_sample(&self) -> Option<Sample> {
        if self.is_empty() {
            None
        } else {
            Some(Sample {
                timestamp: self.last_timestamp(),
                value: self.last_value(),
            })
        }
    }

    pub fn filtered_iter(
        &'_ self,
        start_timestamp: Timestamp,
        end_timestamp: Timestamp,
        timestamp_filter: Option<&[Timestamp]>,
        value_filter: Option<ValueFilter>,
    ) -> FilteredSampleIterator<SampleIter<'_>> {
        // determine the range of timestamps to filter
        let (start_timestamp, end_timestamp) = if let Some(ts_filter) = timestamp_filter {
            if ts_filter.is_empty() {
                (start_timestamp, end_timestamp)
            } else {
                let first_ts = ts_filter[0];
                let last_ts = ts_filter[ts_filter.len() - 1];
                (start_timestamp.max(first_ts), end_timestamp.min(last_ts))
            }
        } else {
            (start_timestamp, end_timestamp)
        };

        FilteredSampleIterator::new(
            self.range_iter(start_timestamp, end_timestamp),
            value_filter,
            timestamp_filter,
        )
    }

    pub fn is_compressed(&self) -> bool {
        !matches!(self, TimeSeriesChunk::Uncompressed(_))
    }
}

impl Default for TimeSeriesChunk {
    fn default() -> Self {
        TimeSeriesChunk::Uncompressed(UncompressedChunk::default())
    }
}

impl Chunk for TimeSeriesChunk {
    fn split(&mut self) -> TsdbResult<Self>
    where
        Self: Sized,
    {
        use TimeSeriesChunk::*;
        match self {
            Uncompressed(chunk) => Ok(Uncompressed(chunk.split()?)),
            Gorilla(chunk) => Ok(Gorilla(chunk.split()?)),
            DeXor(chunk) => Ok(DeXor(chunk.split()?)),
            Chimp(chunk) => Ok(Chimp(chunk.split()?)),
        }
    }

    fn save_rdb(&self, rdb: *mut RedisModuleIO) {
        use TimeSeriesChunk::*;
        save_chunk_type(self, rdb);
        match self {
            Uncompressed(chunk) => chunk.save_rdb(rdb),
            Gorilla(chunk) => chunk.save_rdb(rdb),
            DeXor(chunk) => chunk.save_rdb(rdb),
            Chimp(chunk) => chunk.save_rdb(rdb),
        }
    }

    fn load_rdb(rdb: *mut RedisModuleIO, enc_ver: i32) -> ValkeyResult<Self> {
        use TimeSeriesChunk::*;
        let chunk_type_byte = rdb_load_u8(rdb)?;
        let chunk_type: ChunkEncoding = chunk_type_byte.try_into()?;
        let chunk = match chunk_type {
            ChunkEncoding::Uncompressed => Uncompressed(UncompressedChunk::load_rdb(rdb, enc_ver)?),
            ChunkEncoding::Gorilla => Gorilla(GorillaChunk::load_rdb(rdb, enc_ver)?),
            ChunkEncoding::DeXor => DeXor(DeXorChunk::load_rdb(rdb, enc_ver)?),
            ChunkEncoding::Chimp => Chimp(ChimpChunk::load_rdb(rdb, enc_ver)?),
        };
        Ok(chunk)
    }

    fn serialize(&self, dest: &mut Vec<u8>) {
        use TimeSeriesChunk::*;
        match self {
            Uncompressed(chunk) => {
                dest.push(ChunkEncoding::Uncompressed as u8);
                chunk.serialize(dest)
            }
            Gorilla(chunk) => {
                dest.push(ChunkEncoding::Gorilla as u8);
                chunk.serialize(dest)
            }
            DeXor(chunk) => {
                dest.push(ChunkEncoding::DeXor as u8);
                chunk.serialize(dest)
            }
            Chimp(chunk) => {
                dest.push(ChunkEncoding::Chimp as u8);
                chunk.serialize(dest)
            }
        }
    }

    fn deserialize(buf: &[u8]) -> TsdbResult<Self> {
        use TimeSeriesChunk::*;
        if buf.is_empty() {
            return Err(TsdbError::DecodingError(
                "Empty buffer deserializing chunk".to_string(),
            ));
        }
        // The first byte indicates the chunk type
        let chunk_type = ChunkEncoding::try_from(buf[0])?;
        match chunk_type {
            ChunkEncoding::Uncompressed => {
                let chunk = UncompressedChunk::deserialize(&buf[1..])?;
                Ok(Uncompressed(chunk))
            }
            ChunkEncoding::Gorilla => {
                let chunk = GorillaChunk::deserialize(&buf[1..])?;
                Ok(Gorilla(chunk))
            }
            ChunkEncoding::DeXor => {
                let chunk = DeXorChunk::deserialize(&buf[1..])?;
                Ok(DeXor(chunk))
            }
            ChunkEncoding::Chimp => {
                let chunk = ChimpChunk::deserialize(&buf[1..])?;
                Ok(Chimp(chunk))
            }
        }
    }

    fn debug_digest(&self, dig: &mut Digest) {
        use TimeSeriesChunk::*;
        match self {
            Uncompressed(chunk) => chunk.debug_digest(dig),
            Gorilla(chunk) => chunk.debug_digest(dig),
            DeXor(chunk) => chunk.debug_digest(dig),
            Chimp(chunk) => chunk.debug_digest(dig),
        }
    }
}

fn save_chunk_type(chunk: &TimeSeriesChunk, rdb: *mut RedisModuleIO) {
    let chunk_type: u8 = chunk.get_encoding() as u8;
    rdb_save_u8(rdb, chunk_type);
}

impl PartialOrd for TimeSeriesChunk {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.first_timestamp().partial_cmp(&other.first_timestamp())
    }
}
