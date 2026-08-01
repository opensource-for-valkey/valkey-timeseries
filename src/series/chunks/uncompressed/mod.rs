use crate::common::encoding::write_f64_le;
use crate::common::encoding::{try_read_f64_le, try_read_uvarint, write_uvarint};
use crate::common::rdb::{rdb_load_len, rdb_load_u8, rdb_load_usize, rdb_save_u8, rdb_save_usize};
use crate::common::{SAMPLE_SIZE, Sample, Timestamp};
use crate::error::{TsdbError, TsdbResult};
use crate::iterators::SampleIter;
use crate::series::chunks::merge::merge_chunk_samples;
use crate::series::chunks::{Chunk, ChunkOps, MAX_CHUNK_SIZE};
use crate::series::{DuplicatePolicy, SampleAddResult};
use core::mem::size_of;
use get_size2::{GetSize, GetSizeTracker};
use std::hash::Hash;
use valkey_module::digest::Digest;
use valkey_module::{RedisModuleIO, ValkeyResult, raw};

pub const MAX_UNCOMPRESSED_SAMPLES: usize = 256;
const FLAG_SERIALIZE_UNCOMPRESSED: u8 = 0b00000001;
const FLAG_SERIALIZE_GORILLA: u8 = 0b00000010;

#[derive(Clone, Debug, PartialEq)]
pub struct UncompressedChunk {
    pub max_size: usize,
    pub samples: Vec<Sample>,
    pub(crate) max_elements: usize,
}

impl Default for UncompressedChunk {
    fn default() -> Self {
        Self {
            samples: Vec::default(),
            max_size: MAX_UNCOMPRESSED_SAMPLES * SAMPLE_SIZE,
            max_elements: MAX_UNCOMPRESSED_SAMPLES,
        }
    }
}

impl GetSize for UncompressedChunk {
    // See `GorillaEncoder`: containers call `get_heap_size_with_tracker`, so
    // that is what a manual impl has to provide. `Vec`'s own impl accounts for
    // reserved-but-unused capacity.
    fn get_heap_size_with_tracker<T: GetSizeTracker>(&self, tracker: T) -> (usize, T) {
        self.samples.get_heap_size_with_tracker(tracker)
    }
}

impl Hash for UncompressedChunk {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.max_size.hash(state);
        self.max_elements.hash(state);
        for sample in &self.samples {
            sample.hash(state);
        }
    }
}

impl UncompressedChunk {
    pub fn new(size: usize, samples: &[Sample]) -> Self {
        let max_elements = size / SAMPLE_SIZE;
        Self {
            samples: samples.to_vec(),
            max_size: size,
            max_elements,
        }
    }

    pub fn from_vec(samples: Vec<Sample>) -> Self {
        let max_elements = samples.len();
        let size = max_elements * SAMPLE_SIZE;
        Self {
            samples,
            max_size: size,
            max_elements,
        }
    }

    pub fn with_max_size(size: usize) -> Self {
        Self {
            max_size: size,
            max_elements: size / SAMPLE_SIZE,
            ..Default::default()
        }
    }

    fn handle_insert(&mut self, sample: Sample, policy: DuplicatePolicy) -> TsdbResult<()> {
        let ts = sample.timestamp;

        let (idx, found) = self.get_sample_index(ts);
        if found {
            // update value in case timestamp exists
            let current = self.samples.get_mut(idx).unwrap(); // todo: get_mut_unchecked
            current.value = policy.duplicate_value(ts, current.value, sample.value)?;
        } else if idx < self.samples.len() {
            self.samples.insert(idx, sample);
        } else {
            self.samples.push(sample);
        }
        Ok(())
    }

    pub fn iter(&self) -> impl Iterator<Item = Sample> + '_ {
        self.samples.iter().cloned()
    }

    pub fn range_iter(&self, start_ts: Timestamp, end_ts: Timestamp) -> SampleIter<'_> {
        let slice = self.get_range_slice(start_ts, end_ts);
        SampleIter::vec(slice)
    }

    pub fn samples_by_timestamps(&self, timestamps: &[Timestamp]) -> TsdbResult<Vec<Sample>> {
        if self.len() == 0 || timestamps.is_empty() {
            return Ok(vec![]);
        }
        let first_timestamp = timestamps[0];
        if first_timestamp > self.last_timestamp() {
            return Ok(vec![]);
        }

        let mut samples = Vec::with_capacity(timestamps.len());
        for ts in timestamps.iter() {
            let (pos, found) = self.get_sample_index(*ts);
            if found {
                // todo: we know that the index in bounds, so use get_unchecked
                let sample = self.samples.get(pos).unwrap();
                samples.push(*sample);
            }
        }
        Ok(samples)
    }

    fn get_sample_index(&self, ts: Timestamp) -> (usize, bool) {
        get_sample_index(&self.samples, ts)
    }

    pub(crate) fn get_sample(&self, ts: Timestamp) -> Option<Sample> {
        let (idx, found) = self.get_sample_index(ts);
        if found { Some(self.samples[idx]) } else { None }
    }

    fn get_range_slice(&self, start_ts: Timestamp, end_ts: Timestamp) -> Vec<Sample> {
        let Some(res) = self.get_range_as_ref(start_ts, end_ts) else {
            return Vec::new();
        };
        res.to_vec()
    }

    pub(crate) fn get_range_as_ref(
        &self,
        start_ts: Timestamp,
        end_ts: Timestamp,
    ) -> Option<&[Sample]> {
        self.get_index_bounds(start_ts, end_ts)
            .map(|(start_idx, end_idx)| &self.samples[start_idx..=end_idx])
    }

    /// Finds the start and end chunk indices (inclusive) for a date range.
    ///
    /// #### Parameters
    ///
    /// * `start`: The lower bound of the range to search for.
    /// * `end`: The upper bound of the range to search for.
    ///
    /// #### Returns
    ///
    /// Returns `Option<(usize, usize)>`:
    /// * `Some((start_idx, end_idx))` if valid indices are found within the range.
    /// * `None` if the series is empty, if all samples are less than `start`,
    ///   or if `start` and `end` are equal and greater than the sample at the found index.
    ///
    /// Used to get an inclusive bound for series chunks (all chunks containing samples in the range [start_index...=end_index])
    fn get_index_bounds(&self, start: Timestamp, end: Timestamp) -> Option<(usize, usize)> {
        let len = self.samples.len();
        if len == 0 {
            return None;
        }
        let last_ts = self.samples[len - 1].timestamp;
        if end < self.samples[0].timestamp || start > last_ts {
            return None;
        }

        let (start_idx, _) = binary_search_samples_by_timestamp(&self.samples, start);
        if start_idx >= len {
            return None;
        }

        let (mut end_idx, found) = binary_search_samples_by_timestamp(&self.samples, end);
        if !found && end_idx > 0 {
            // if the end timestamp is not found, we need to include the previous sample
            // to ensure that the end timestamp is included in the range.
            let right = &self.samples[end_idx - 1];
            if right.timestamp < end {
                end_idx -= 1;
            }
        }

        // imagine this scenario:
        // sample start timestamps = [10, 20, 30, 40]
        // start = 25, end = 25
        // we have a situation where start_index == end_index (2), yet samples[2] is greater than end,
        if start_idx == end_idx {
            // todo: get_unchecked
            if self.samples[start_idx].timestamp > end {
                return None;
            }
        }

        Some((start_idx, end_idx))
    }

    fn serialize_raw(&self, buf: &mut Vec<u8>) {
        write_uvarint(buf, self.max_size as u64);
        write_uvarint(buf, self.max_elements as u64);
        write_uvarint(buf, self.samples.len() as u64);

        for sample in &self.samples {
            // assume non-zero timestamps
            write_uvarint(buf, sample.timestamp as u64);
            write_f64_le(buf, sample.value);
        }
    }

    fn deserialize_raw(buf: &[u8]) -> TsdbResult<Self> {
        let mut buf = buf;

        let max_size = read_usize(&mut buf)?;
        let max_elements = read_usize(&mut buf)?;
        let len = read_usize(&mut buf)?;
        // `len` is untrusted (corrupt/malicious peer, truncated RDB, ...): every
        // sample needs at least 1 byte (uvarint timestamp) + 8 bytes (f64 value),
        // so capping the reservation to what the remaining buffer could possibly
        // hold prevents an attacker-controlled `Vec::with_capacity` from
        // requesting an unbounded allocation, which aborts the process rather
        // than returning an `Err`.
        let capacity = len.min(buf.len() / 9);
        let mut samples = Vec::with_capacity(capacity);
        for _ in 0..len {
            let ts = try_read_uvarint(&mut buf).map_err(|_| TsdbError::ChunkDecoding)? as i64;
            let val = try_read_f64_le(&mut buf).map_err(|_| TsdbError::ChunkDecoding)?;
            samples.push(Sample {
                timestamp: ts,
                value: val,
            });
        }
        Ok(UncompressedChunk {
            max_size,
            samples,
            max_elements,
        })
    }
}

fn read_usize(buf: &mut &[u8]) -> TsdbResult<usize> {
    let value = try_read_uvarint(buf)
        .map_err(|_| TsdbError::DecodingError("Failed to read usize".into()))?;
    Ok(value as usize)
}

fn binary_search_samples_by_timestamp(samples: &[Sample], ts: Timestamp) -> (usize, bool) {
    match samples.binary_search_by(|probe| match ts.cmp(&probe.timestamp) {
        std::cmp::Ordering::Less => std::cmp::Ordering::Greater,
        std::cmp::Ordering::Greater => std::cmp::Ordering::Less,
        std::cmp::Ordering::Equal => std::cmp::Ordering::Equal,
    }) {
        Ok(pos) => (pos, true),
        Err(pos) => (pos, false),
    }
}

fn get_sample_index(samples: &[Sample], ts: Timestamp) -> (usize, bool) {
    match samples.binary_search_by(|x| x.timestamp.cmp(&ts)) {
        Ok(pos) => (pos, true),
        Err(idx) => (idx, false),
    }
}

impl ChunkOps for UncompressedChunk {
    fn first_timestamp(&self) -> Timestamp {
        if self.samples.is_empty() {
            return 0;
        }
        self.samples[0].timestamp
    }

    fn last_timestamp(&self) -> Timestamp {
        if self.samples.is_empty() {
            return i64::MAX;
        }
        self.samples[self.samples.len() - 1].timestamp
    }

    fn len(&self) -> usize {
        self.samples.len()
    }

    fn last_value(&self) -> f64 {
        if self.samples.is_empty() {
            return f64::MAX;
        }
        self.samples[self.samples.len() - 1].value
    }

    fn size(&self) -> usize {
        self.samples.len() * size_of::<Sample>()
    }

    fn max_size(&self) -> usize {
        self.max_size
    }

    fn remove_range(&mut self, start_ts: Timestamp, end_ts: Timestamp) -> TsdbResult<usize> {
        let count = self.samples.len();
        if let Some((start_idx, end_idx)) = self.get_index_bounds(start_ts, end_ts) {
            let _ = self.samples.drain(start_idx..=end_idx);
        };
        Ok(count - self.samples.len())
    }

    fn add_sample(&mut self, sample: &Sample) -> TsdbResult<()> {
        if self.is_full() {
            return Err(TsdbError::CapacityFull(MAX_UNCOMPRESSED_SAMPLES));
        }
        self.samples.push(*sample);
        Ok(())
    }

    fn get_range(&self, start: Timestamp, end: Timestamp) -> TsdbResult<Vec<Sample>> {
        let slice = self.get_range_slice(start, end);
        Ok(slice)
    }

    fn upsert_sample(&mut self, sample: Sample, dp_policy: DuplicatePolicy) -> TsdbResult<usize> {
        let ts = sample.timestamp;

        let count = self.samples.len();
        if self.is_empty() {
            self.samples.push(sample);
        } else {
            let last_sample = self.samples[count - 1];
            let last_ts = last_sample.timestamp;
            if ts > last_ts {
                self.add_sample(&sample)?;
            } else {
                self.handle_insert(sample, dp_policy)?;
            }
        }

        Ok(self.len() - count)
    }

    fn merge_samples(
        &mut self,
        samples: &[Sample],
        dp_policy: Option<DuplicatePolicy>,
    ) -> TsdbResult<Vec<SampleAddResult>> {
        if samples.is_empty() {
            return Ok(Vec::new());
        }
        let first = samples[0];

        if samples.len() == 1 {
            if self.is_empty() {
                self.add_sample(&first)?;
                return Ok(vec![SampleAddResult::Ok(first)]);
            }
            let policy = dp_policy.unwrap_or(DuplicatePolicy::KeepLast);
            return match self.upsert_sample(first, policy) {
                Ok(_) => Ok(vec![SampleAddResult::Ok(first)]),
                Err(TsdbError::DuplicateSample(_)) => Ok(vec![SampleAddResult::Duplicate]),
                Err(e) => Err(e),
            };
        }

        if self.is_empty() || first.timestamp > self.last_timestamp() {
            self.samples.extend_from_slice(samples);
            let result = samples
                .iter()
                .map(|sample| SampleAddResult::Ok(*sample))
                .collect();
            return Ok(result);
        }

        let mut dest: Vec<Sample> = Vec::with_capacity(self.samples.len() + samples.len());
        let result = merge_chunk_samples(
            SampleIter::Slice(self.samples.iter()),
            samples,
            dp_policy,
            |sample| {
                dest.push(sample);
                Ok(())
            },
        )?;

        self.samples = dest;
        Ok(result)
    }

    fn optimize(&mut self) -> TsdbResult<()> {
        self.samples.shrink_to_fit();
        Ok(())
    }

    fn is_full(&self) -> bool {
        self.len() >= self.max_elements
    }

    fn bytes_per_sample(&self) -> usize {
        SAMPLE_SIZE
    }

    fn clear(&mut self) {
        self.samples.clear();
    }

    fn set_data(&mut self, samples: &[Sample]) -> TsdbResult<()> {
        self.samples = samples.to_vec();
        // todo: complain if size > max_size
        Ok(())
    }
}

impl Chunk for UncompressedChunk {
    fn split(&mut self) -> TsdbResult<Self>
    where
        Self: Sized,
    {
        if self.samples.is_empty() {
            return Ok(self.clone());
        }

        if self.samples.len() == 1 {
            let mut result = self.clone();
            result.samples.clear();
            return Ok(result);
        }

        let half = self.samples.len() / 2;
        let samples = std::mem::take(&mut self.samples);
        let (left, right) = samples.split_at(half);
        self.samples = left.to_vec();

        Ok(Self {
            max_size: self.max_size,
            samples: right.to_vec(),
            max_elements: self.max_elements,
        })
    }

    fn save_rdb(&self, rdb: *mut RedisModuleIO) {
        // todo: compress ?
        // forward compat - write encoding flag
        rdb_save_u8(rdb, FLAG_SERIALIZE_UNCOMPRESSED);
        rdb_save_usize(rdb, self.max_size);
        rdb_save_usize(rdb, self.max_elements);
        rdb_save_usize(rdb, self.samples.len());
        for Sample { timestamp, value } in self.samples.iter() {
            raw::save_signed(rdb, *timestamp);
            raw::save_double(rdb, *value);
        }
    }

    fn load_rdb(rdb: *mut RedisModuleIO, _enc_ver: i32) -> ValkeyResult<Self> {
        // forward compat - read encoding flag
        let _flag = rdb_load_u8(rdb)?;
        let max_size = rdb_load_usize(rdb)?;
        // No chunk can legitimately declare more elements than the largest allowed chunk size
        // permits; bounding on that closes off a corrupt/hostile `max_elements` before it's used
        // as the ceiling for `len` below.
        let max_elements = rdb_load_len(rdb, MAX_CHUNK_SIZE / SAMPLE_SIZE)?;
        let len = rdb_load_len(rdb, max_elements)?;
        let mut samples = Vec::with_capacity(len);
        for _ in 0..len {
            let ts = raw::load_signed(rdb)?;
            let val = raw::load_double(rdb)?;
            samples.push(Sample {
                timestamp: ts,
                value: val,
            });
        }
        Ok(UncompressedChunk {
            max_size,
            samples,
            max_elements,
        })
    }

    fn serialize(&self, dest: &mut Vec<u8>) {
        dest.push(FLAG_SERIALIZE_UNCOMPRESSED);
        self.serialize_raw(dest);
    }

    fn deserialize(buf: &[u8]) -> TsdbResult<Self> {
        if buf.len() < 2 {
            return Err(TsdbError::ChunkDecoding);
        }
        let _ = buf[0]; // forward compat - encoding flag placeholder
        Self::deserialize_raw(&buf[1..])
    }

    fn debug_digest(&self, dig: &mut Digest) {
        dig.add_long_long(self.max_size as i64);
        dig.add_long_long(self.max_elements as i64);
        for sample in &self.samples {
            let value = sample.value.to_bits().to_le_bytes();
            dig.add_long_long(sample.timestamp);
            dig.add_string_buffer(&value);
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::common::{SAMPLE_SIZE, Sample};
    use crate::series::chunks::{Chunk, ChunkOps, UncompressedChunk};
    use crate::series::{DuplicatePolicy, SampleAddResult};

    #[test]
    fn test_get_range_slice_start_equals_end() {
        let samples = vec![
            Sample {
                timestamp: 10,
                value: 1.0,
            },
            Sample {
                timestamp: 20,
                value: 2.0,
            },
            Sample {
                timestamp: 30,
                value: 3.0,
            },
            Sample {
                timestamp: 40,
                value: 4.0,
            },
            Sample {
                timestamp: 50,
                value: 5.0,
            },
        ];
        let chunk = UncompressedChunk::new(1000, &samples);
        let result = chunk.get_range_slice(30, 30);
        assert_eq!(
            result,
            vec![Sample {
                timestamp: 30,
                value: 3.0
            },]
        );
    }

    #[test]
    fn test_get_range_slice_within_bounds() {
        let samples = vec![
            Sample {
                timestamp: 10,
                value: 1.0,
            },
            Sample {
                timestamp: 20,
                value: 2.0,
            },
            Sample {
                timestamp: 30,
                value: 3.0,
            },
            Sample {
                timestamp: 40,
                value: 4.0,
            },
            Sample {
                timestamp: 50,
                value: 5.0,
            },
        ];
        let chunk = UncompressedChunk::new(1000, &samples);
        let result = chunk.get_range_slice(20, 40);
        assert_eq!(
            result,
            vec![
                Sample {
                    timestamp: 20,
                    value: 2.0
                },
                Sample {
                    timestamp: 30,
                    value: 3.0
                },
                Sample {
                    timestamp: 40,
                    value: 4.0
                },
            ]
        );
    }

    #[test]
    fn test_get_range_slice_out_of_bounds() {
        let samples = vec![
            Sample {
                timestamp: 10,
                value: 1.0,
            },
            Sample {
                timestamp: 20,
                value: 2.0,
            },
            Sample {
                timestamp: 30,
                value: 3.0,
            },
            Sample {
                timestamp: 40,
                value: 4.0,
            },
            Sample {
                timestamp: 50,
                value: 5.0,
            },
        ];
        let chunk = UncompressedChunk::new(1000, &samples);
        let result = chunk.get_range_slice(60, 70);
        assert!(result.is_empty());
    }

    #[test]
    fn test_get_range_slice_partial_overlap() {
        let samples = vec![
            Sample {
                timestamp: 10,
                value: 1.0,
            },
            Sample {
                timestamp: 20,
                value: 2.0,
            },
            Sample {
                timestamp: 30,
                value: 3.0,
            },
            Sample {
                timestamp: 40,
                value: 4.0,
            },
            Sample {
                timestamp: 50,
                value: 5.0,
            },
        ];
        let chunk = UncompressedChunk::new(1000, &samples);
        let result = chunk.get_range_slice(35, 45);
        assert_eq!(
            result,
            vec![Sample {
                timestamp: 40,
                value: 4.0
            },]
        );
    }

    #[test]
    fn test_get_range_slice_empty_chunk() {
        let chunk = UncompressedChunk::default();
        let result = chunk.get_range_slice(10, 20);
        assert!(result.is_empty());
    }

    #[test]
    fn test_remove_range() {
        let samples = vec![
            Sample {
                timestamp: 10,
                value: 1.0,
            },
            Sample {
                timestamp: 20,
                value: 2.0,
            },
            Sample {
                timestamp: 30,
                value: 3.0,
            },
            Sample {
                timestamp: 40,
                value: 4.0,
            },
            Sample {
                timestamp: 50,
                value: 5.0,
            },
        ];
        let mut chunk = UncompressedChunk::new(1000, &samples);

        // Test 1: Remove the middle range
        let removed = chunk.remove_range(25, 45).unwrap();
        assert_eq!(removed, 2);
        assert_eq!(
            chunk.samples,
            vec![
                Sample {
                    timestamp: 10,
                    value: 1.0
                },
                Sample {
                    timestamp: 20,
                    value: 2.0
                },
                Sample {
                    timestamp: 50,
                    value: 5.0
                },
            ]
        );

        // Test 2: Remove range at the beginning
        let mut chunk = UncompressedChunk::new(1000, &samples);
        let removed = chunk.remove_range(0, 15).unwrap();
        assert_eq!(removed, 1);
        assert_eq!(
            chunk.samples,
            vec![
                Sample {
                    timestamp: 20,
                    value: 2.0
                },
                Sample {
                    timestamp: 30,
                    value: 3.0
                },
                Sample {
                    timestamp: 40,
                    value: 4.0
                },
                Sample {
                    timestamp: 50,
                    value: 5.0
                },
            ]
        );

        // Test 3: Remove range at the end
        let mut chunk = UncompressedChunk::new(1000, &samples);
        let removed = chunk.remove_range(45, 60).unwrap();
        assert_eq!(removed, 1);
        assert_eq!(
            chunk.samples,
            vec![
                Sample {
                    timestamp: 10,
                    value: 1.0
                },
                Sample {
                    timestamp: 20,
                    value: 2.0
                },
                Sample {
                    timestamp: 30,
                    value: 3.0
                },
                Sample {
                    timestamp: 40,
                    value: 4.0
                },
            ]
        );

        // Test 4: Remove the entire range
        let mut chunk = UncompressedChunk::new(1000, &samples);
        let removed = chunk.remove_range(0, 60).unwrap();
        assert_eq!(removed, 5);
        assert!(chunk.samples.is_empty());

        // Test 5: Remove range outside of samples
        let mut chunk = UncompressedChunk::new(1000, &samples);
        let removed = chunk.remove_range(60, 70).unwrap();
        assert_eq!(removed, 0);
        assert_eq!(chunk.samples, samples);

        // Test 6: Remove range with no overlap
        let mut chunk = UncompressedChunk::new(1000, &samples);
        let removed = chunk.remove_range(31, 39).unwrap();
        assert_eq!(removed, 0);
        assert_eq!(chunk.samples, samples);

        // Test 7: Remove range from empty chunk
        let mut empty_chunk = UncompressedChunk::default();
        let removed = empty_chunk.remove_range(10, 20).unwrap();
        assert_eq!(removed, 0);
        assert!(empty_chunk.samples.is_empty());
    }

    #[test]
    fn test_upsert_sample() {
        let samples = vec![
            Sample {
                timestamp: 10,
                value: 1.0,
            },
            Sample {
                timestamp: 30,
                value: 3.0,
            },
            Sample {
                timestamp: 50,
                value: 5.0,
            },
        ];
        let mut chunk = UncompressedChunk::new(1000, &samples);

        // Test 1: Upsert a new sample at the end
        let result = chunk
            .upsert_sample(
                Sample {
                    timestamp: 60,
                    value: 6.0,
                },
                DuplicatePolicy::KeepLast,
            )
            .unwrap();
        assert_eq!(result, 1);
        assert_eq!(
            chunk.samples,
            vec![
                Sample {
                    timestamp: 10,
                    value: 1.0
                },
                Sample {
                    timestamp: 30,
                    value: 3.0
                },
                Sample {
                    timestamp: 50,
                    value: 5.0
                },
                Sample {
                    timestamp: 60,
                    value: 6.0
                },
            ]
        );

        // Test 2: Upsert a new sample in the middle
        let result = chunk
            .upsert_sample(
                Sample {
                    timestamp: 40,
                    value: 4.0,
                },
                DuplicatePolicy::KeepLast,
            )
            .unwrap();
        assert_eq!(result, 1);
        assert_eq!(
            chunk.samples,
            vec![
                Sample {
                    timestamp: 10,
                    value: 1.0
                },
                Sample {
                    timestamp: 30,
                    value: 3.0
                },
                Sample {
                    timestamp: 40,
                    value: 4.0
                },
                Sample {
                    timestamp: 50,
                    value: 5.0
                },
                Sample {
                    timestamp: 60,
                    value: 6.0
                },
            ]
        );

        // Test 3: Upsert an existing sample with KeepLast policy
        let result = chunk
            .upsert_sample(
                Sample {
                    timestamp: 30,
                    value: 3.5,
                },
                DuplicatePolicy::KeepLast,
            )
            .unwrap();
        assert_eq!(result, 0);
        assert_eq!(
            chunk.samples,
            vec![
                Sample {
                    timestamp: 10,
                    value: 1.0
                },
                Sample {
                    timestamp: 30,
                    value: 3.5
                },
                Sample {
                    timestamp: 40,
                    value: 4.0
                },
                Sample {
                    timestamp: 50,
                    value: 5.0
                },
                Sample {
                    timestamp: 60,
                    value: 6.0
                },
            ]
        );

        // Test 4: Upsert an existing sample with KeepFirst policy
        let result = chunk
            .upsert_sample(
                Sample {
                    timestamp: 40,
                    value: 4.5,
                },
                DuplicatePolicy::KeepFirst,
            )
            .unwrap();
        assert_eq!(result, 0);
        assert_eq!(
            chunk.samples,
            vec![
                Sample {
                    timestamp: 10,
                    value: 1.0
                },
                Sample {
                    timestamp: 30,
                    value: 3.5
                },
                Sample {
                    timestamp: 40,
                    value: 4.0
                },
                Sample {
                    timestamp: 50,
                    value: 5.0
                },
                Sample {
                    timestamp: 60,
                    value: 6.0
                },
            ]
        );

        // Test 5: Upsert a sample at the beginning
        let result = chunk
            .upsert_sample(
                Sample {
                    timestamp: 5,
                    value: 0.5,
                },
                DuplicatePolicy::KeepLast,
            )
            .unwrap();
        assert_eq!(result, 1);
        assert_eq!(
            chunk.samples,
            vec![
                Sample {
                    timestamp: 5,
                    value: 0.5
                },
                Sample {
                    timestamp: 10,
                    value: 1.0
                },
                Sample {
                    timestamp: 30,
                    value: 3.5
                },
                Sample {
                    timestamp: 40,
                    value: 4.0
                },
                Sample {
                    timestamp: 50,
                    value: 5.0
                },
                Sample {
                    timestamp: 60,
                    value: 6.0
                },
            ]
        );

        // Test 6: Upsert a sample into an empty chunk
        let mut empty_chunk = UncompressedChunk::default();
        let result = empty_chunk
            .upsert_sample(
                Sample {
                    timestamp: 10,
                    value: 1.0,
                },
                DuplicatePolicy::KeepLast,
            )
            .unwrap();
        assert_eq!(result, 1);
        assert_eq!(
            empty_chunk.samples,
            vec![Sample {
                timestamp: 10,
                value: 1.0
            },]
        );

        // Test 7: Attempt to upsert when the chunk is full
        let mut full_chunk = UncompressedChunk::new(SAMPLE_SIZE * 2, &samples);
        let result = full_chunk.upsert_sample(
            Sample {
                timestamp: 70,
                value: 7.0,
            },
            DuplicatePolicy::KeepLast,
        );
        assert!(result.is_err());
        assert_eq!(full_chunk.samples, samples);
    }

    #[test]
    fn test_samples_by_timestamps() {
        let samples = vec![
            Sample {
                timestamp: 10,
                value: 1.0,
            },
            Sample {
                timestamp: 20,
                value: 2.0,
            },
            Sample {
                timestamp: 30,
                value: 3.0,
            },
            Sample {
                timestamp: 40,
                value: 4.0,
            },
            Sample {
                timestamp: 50,
                value: 5.0,
            },
        ];
        let chunk = UncompressedChunk::new(1000, &samples);

        // Test 1: Get existing timestamps
        let timestamps = vec![10, 30, 50];
        let result = chunk.samples_by_timestamps(&timestamps).unwrap();
        assert_eq!(
            result,
            vec![
                Sample {
                    timestamp: 10,
                    value: 1.0
                },
                Sample {
                    timestamp: 30,
                    value: 3.0
                },
                Sample {
                    timestamp: 50,
                    value: 5.0
                },
            ]
        );

        // Test 2: Get non-existing timestamps
        let timestamps = vec![15, 25, 35];
        let result = chunk.samples_by_timestamps(&timestamps).unwrap();
        assert_eq!(result, vec![]);

        // Test 3: Mix of existing and non-existing timestamps
        let timestamps = vec![10, 15, 30, 35, 50];
        let result = chunk.samples_by_timestamps(&timestamps).unwrap();
        assert_eq!(
            result,
            vec![
                Sample {
                    timestamp: 10,
                    value: 1.0
                },
                Sample {
                    timestamp: 30,
                    value: 3.0
                },
                Sample {
                    timestamp: 50,
                    value: 5.0
                },
            ]
        );

        // Test 4: Empty timestamps
        let timestamps = vec![];
        let result = chunk.samples_by_timestamps(&timestamps).unwrap();
        assert_eq!(result, vec![]);

        // Test 5: All timestamps after the last sample
        let timestamps = vec![60, 70, 80];
        let result = chunk.samples_by_timestamps(&timestamps).unwrap();
        assert_eq!(result, vec![]);

        // Test 6: All timestamps before the first sample
        let timestamps = vec![1, 5, 9];
        let result = chunk.samples_by_timestamps(&timestamps).unwrap();
        assert_eq!(result, vec![]);

        // Test 7: Empty chunk
        let empty_chunk = UncompressedChunk::default();
        let timestamps = vec![10, 20, 30];
        let result = empty_chunk.samples_by_timestamps(&timestamps).unwrap();
        assert_eq!(result, vec![]);
    }

    #[test]
    fn test_merge_samples() {
        let samples = vec![
            Sample {
                timestamp: 10,
                value: 1.0,
            },
            Sample {
                timestamp: 30,
                value: 3.0,
            },
            Sample {
                timestamp: 50,
                value: 5.0,
            },
        ];
        let mut chunk = UncompressedChunk::new(1000, &samples);

        // Test 1: Merge non-overlapping samples
        let new_samples = vec![
            Sample {
                timestamp: 20,
                value: 2.0,
            },
            Sample {
                timestamp: 40,
                value: 4.0,
            },
        ];
        let result = chunk.merge_samples(&new_samples, None).unwrap();
        assert_eq!(
            result,
            vec![
                SampleAddResult::Ok(new_samples[0]),
                SampleAddResult::Ok(new_samples[1])
            ]
        );
        assert_eq!(
            chunk.samples,
            vec![
                Sample {
                    timestamp: 10,
                    value: 1.0
                },
                Sample {
                    timestamp: 20,
                    value: 2.0
                },
                Sample {
                    timestamp: 30,
                    value: 3.0
                },
                Sample {
                    timestamp: 40,
                    value: 4.0
                },
                Sample {
                    timestamp: 50,
                    value: 5.0
                },
            ]
        );

        // Test 2: Merge overlapping samples with KeepLast policy
        let new_samples = vec![
            Sample {
                timestamp: 30,
                value: 3.5,
            },
            Sample {
                timestamp: 60,
                value: 6.0,
            },
        ];
        let result = chunk
            .merge_samples(&new_samples, Some(DuplicatePolicy::KeepLast))
            .unwrap();
        assert_eq!(
            result,
            vec![
                SampleAddResult::Ok(new_samples[0]),
                SampleAddResult::Ok(new_samples[1])
            ]
        );
        assert_eq!(
            chunk.samples,
            vec![
                Sample {
                    timestamp: 10,
                    value: 1.0
                },
                Sample {
                    timestamp: 20,
                    value: 2.0
                },
                Sample {
                    timestamp: 30,
                    value: 3.5
                },
                Sample {
                    timestamp: 40,
                    value: 4.0
                },
                Sample {
                    timestamp: 50,
                    value: 5.0
                },
                Sample {
                    timestamp: 60,
                    value: 6.0
                },
            ]
        );

        // Test 3: Merge samples into an empty chunk
        let mut empty_chunk = UncompressedChunk::default();
        let new_samples = vec![
            Sample {
                timestamp: 10,
                value: 1.0,
            },
            Sample {
                timestamp: 20,
                value: 2.0,
            },
        ];
        let result = empty_chunk.merge_samples(&new_samples, None).unwrap();
        assert_eq!(
            result,
            vec![
                SampleAddResult::Ok(new_samples[0]),
                SampleAddResult::Ok(new_samples[1])
            ]
        );
        assert_eq!(empty_chunk.samples, new_samples);

        // Test 4: Merge samples with all timestamps greater than the last timestamp in the chunk
        let mut chunk = UncompressedChunk::new(1000, &samples);
        let new_samples = vec![
            Sample {
                timestamp: 60,
                value: 6.0,
            },
            Sample {
                timestamp: 70,
                value: 7.0,
            },
        ];
        let result = chunk.merge_samples(&new_samples, None).unwrap();
        assert_eq!(
            result,
            vec![
                SampleAddResult::Ok(new_samples[0]),
                SampleAddResult::Ok(new_samples[1])
            ]
        );
        assert_eq!(
            chunk.samples,
            vec![
                Sample {
                    timestamp: 10,
                    value: 1.0
                },
                Sample {
                    timestamp: 30,
                    value: 3.0
                },
                Sample {
                    timestamp: 50,
                    value: 5.0
                },
                Sample {
                    timestamp: 60,
                    value: 6.0
                },
                Sample {
                    timestamp: 70,
                    value: 7.0
                },
            ]
        );

        // Test 5: Merge a single sample
        let mut chunk = UncompressedChunk::new(1000, &samples);
        let new_samples = vec![Sample {
            timestamp: 40,
            value: 4.0,
        }];
        let result = chunk.merge_samples(&new_samples, None).unwrap();
        assert_eq!(result, vec![SampleAddResult::Ok(new_samples[0])]);
        assert_eq!(
            chunk.samples,
            vec![
                Sample {
                    timestamp: 10,
                    value: 1.0
                },
                Sample {
                    timestamp: 30,
                    value: 3.0
                },
                Sample {
                    timestamp: 40,
                    value: 4.0
                },
                Sample {
                    timestamp: 50,
                    value: 5.0
                },
            ]
        );
    }

    #[test]
    fn test_split() {
        let samples = vec![
            Sample {
                timestamp: 10,
                value: 1.0,
            },
            Sample {
                timestamp: 20,
                value: 2.0,
            },
            Sample {
                timestamp: 30,
                value: 3.0,
            },
            Sample {
                timestamp: 40,
                value: 4.0,
            },
            Sample {
                timestamp: 50,
                value: 5.0,
            },
        ];
        let mut chunk = UncompressedChunk::new(1000, &samples);

        // Test 1: Split a chunk with an odd number of samples
        let new_chunk = chunk.split().unwrap();
        assert_eq!(
            chunk.samples,
            vec![
                Sample {
                    timestamp: 10,
                    value: 1.0
                },
                Sample {
                    timestamp: 20,
                    value: 2.0
                },
            ]
        );
        assert_eq!(
            new_chunk.samples,
            vec![
                Sample {
                    timestamp: 30,
                    value: 3.0
                },
                Sample {
                    timestamp: 40,
                    value: 4.0
                },
                Sample {
                    timestamp: 50,
                    value: 5.0
                },
            ]
        );

        // Test 2: Split a chunk with an even number of samples
        let samples = vec![
            Sample {
                timestamp: 10,
                value: 1.0,
            },
            Sample {
                timestamp: 20,
                value: 2.0,
            },
            Sample {
                timestamp: 30,
                value: 3.0,
            },
            Sample {
                timestamp: 40,
                value: 4.0,
            },
        ];
        let mut chunk = UncompressedChunk::new(1000, &samples);
        let new_chunk = chunk.split().unwrap();
        assert_eq!(
            chunk.samples,
            vec![
                Sample {
                    timestamp: 10,
                    value: 1.0
                },
                Sample {
                    timestamp: 20,
                    value: 2.0
                },
            ]
        );
        assert_eq!(
            new_chunk.samples,
            vec![
                Sample {
                    timestamp: 30,
                    value: 3.0
                },
                Sample {
                    timestamp: 40,
                    value: 4.0
                },
            ]
        );

        // Test 3: Split a chunk with a single sample
        let samples = vec![Sample {
            timestamp: 10,
            value: 1.0,
        }];
        let mut chunk = UncompressedChunk::new(1000, &samples);
        let new_chunk = chunk.split().unwrap();
        assert_eq!(
            chunk.samples,
            vec![Sample {
                timestamp: 10,
                value: 1.0
            },]
        );
        assert!(new_chunk.samples.is_empty());

        // Test 4: Split an empty chunk
        let mut empty_chunk = UncompressedChunk::default();
        let new_chunk = empty_chunk.split().unwrap();
        assert!(empty_chunk.samples.is_empty());
        assert!(new_chunk.samples.is_empty());
    }

    /// A corrupt/malicious peer can declare an absurd sample count in the
    /// `len` header while sending only a handful of bytes. `deserialize_raw`
    /// must reject this via the normal EOF error path rather than reserving
    /// `len` elements up front: a multi-terabyte `Vec::with_capacity` request
    /// aborts the process (allocation failure is not a catchable panic), which
    /// turns a malformed fan-out payload into a crash.
    #[test]
    fn test_deserialize_raw_rejects_oversized_len_without_huge_allocation() {
        use crate::common::encoding::write_uvarint;

        let mut buf = Vec::new();
        write_uvarint(&mut buf, 0); // max_size
        write_uvarint(&mut buf, 0); // max_elements
        write_uvarint(&mut buf, u64::MAX); // len: wildly larger than the buffer could hold
        // No sample payload follows, so decoding must fail on the first read.

        let result = super::UncompressedChunk::deserialize_raw(&buf);
        assert!(result.is_err(), "oversized len must be rejected, not honored");
    }
}
