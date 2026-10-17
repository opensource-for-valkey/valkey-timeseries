//! `XorChunk<C>`: the chunk shared by the compressed encodings.
//!
//! Gorilla and Chimp differ only in their codec: how a sample is packed into the bit stream
//! and read back. Everything a chunk does on top of that — range reads, re-encoding on
//! mutation, splitting, persistence framing, capacity accounting — is identical, so it lives
//! here once, generic over a [`SampleCodec`].
//!
//! The codecs are sequential, so every mutation (`remove_range`, `upsert_sample`,
//! `merge_samples`, `split`) re-encodes from an iterator rather than patching in place.

use crate::common::encoding::{try_read_uvarint, write_uvarint};
use crate::common::logging::log_warning;
use crate::common::rdb::{rdb_load_usize, rdb_save_usize};
use crate::common::{Sample, Timestamp};
use crate::config::DEFAULT_CHUNK_SIZE_BYTES;
use crate::error::{TsdbError, TsdbResult};
use crate::iterators::SampleIter;
use crate::series::chunks::MIN_SAMPLES_FOR_BPS_ESTIMATE;
use crate::series::chunks::chunk::{Chunk, ChunkEncoding, ChunkOps};
use crate::series::chunks::merge::{append_samples, merge_chunk_samples};
use crate::series::chunks::range_iter::RangeFiltered;
use crate::series::{DuplicatePolicy, SampleAddResult};
use get_size2::{GetSize, GetSizeTracker};
use std::fmt::Debug;
use std::hash::Hash;
use std::mem::size_of;
use valkey_module::digest::Digest;
use valkey_module::{RedisModuleIO, ValkeyResult};

/// The most a partial range read reserves before decoding, in samples (4 KiB).
///
/// A partial window's size is estimated from its share of the chunk's time span, which
/// assumes evenly spread samples. Uneven timestamps break that: a window inside a long gap
/// holds nothing yet covers most of the span, and an uncapped estimate reserved ~1.28 MB for
/// an empty result on a 64 KiB chunk. Past the cap the vector grows by doubling, so a large
/// partial read pays a few amortized regrowths instead of a worst-case speculative reserve.
pub const PARTIAL_RANGE_RESERVE_CAP: usize = 256;

/// A sequential sample codec: what a compressed encoding supplies to [`XorChunk`].
pub trait SampleCodec: Clone + Debug + PartialEq + Hash + GetSize + Sized {
    /// Decodes every sample from the start of the stream, reporting errors in-band.
    type Iter<'a>: Iterator<Item = TsdbResult<Sample>> + ExactSizeIterator
    where
        Self: 'a;

    /// The encoding this codec implements.
    const ENCODING: ChunkEncoding;

    /// An empty codec whose stream will not allocate past `max_size` (plus a few bytes of
    /// slack) while the payload stays within budget; past it, growth resumes doubling. See
    /// `BitStream::set_soft_cap`.
    fn with_soft_cap(max_size: usize) -> Self;
    /// Set the allocation budget of a codec loaded from storage.
    fn set_soft_cap(&mut self, max_size: usize);
    /// Append one sample, which must be newer than every sample already encoded.
    fn push(&mut self, sample: &Sample) -> std::io::Result<()>;
    fn iter(&self) -> Self::Iter<'_>;
    /// The encoded bytes written so far.
    fn bytes(&self) -> &[u8];
    fn len(&self) -> usize;
    fn is_empty(&self) -> bool {
        self.len() == 0
    }
    /// `0` when empty.
    fn first_timestamp(&self) -> Timestamp;
    /// `0` when empty.
    fn last_timestamp(&self) -> Timestamp;
    /// `0.0` when empty.
    fn last_value(&self) -> f64;
    fn clear(&mut self);
    fn shrink_to_fit(&mut self);
    fn rdb_save(&self, rdb: *mut RedisModuleIO);
    fn rdb_load(rdb: *mut RedisModuleIO) -> ValkeyResult<Self>;
    fn serialize(&self, dest: &mut Vec<u8>);
    fn deserialize(buf: &[u8]) -> TsdbResult<Self>;
    fn debug_digest(&self, dig: &mut Digest);
    /// Wrap a range-filtered decoder in the `SampleIter` variant for this encoding.
    fn into_sample_iter<'a>(iter: RangeFiltered<Self::Iter<'a>>) -> SampleIter<'a>
    where
        Self: 'a;
}

/// A chunk of samples encoded by the codec `C`.
#[derive(Debug, Clone, PartialEq, Hash)]
pub struct XorChunk<C: SampleCodec> {
    pub(crate) encoder: C,
    pub max_size: usize,
}

impl<C: SampleCodec> GetSize for XorChunk<C> {
    // Containers call `get_heap_size_with_tracker`, so that is what a manual impl provides.
    // The codec owns the only allocation; `max_size` is inline.
    fn get_heap_size_with_tracker<T: GetSizeTracker>(&self, tracker: T) -> (usize, T) {
        self.encoder.get_heap_size_with_tracker(tracker)
    }
}

impl<C: SampleCodec> Default for XorChunk<C> {
    fn default() -> Self {
        Self::with_max_size(DEFAULT_CHUNK_SIZE_BYTES)
    }
}

impl<C: SampleCodec> XorChunk<C> {
    pub fn with_max_size(max_size: usize) -> Self {
        Self {
            encoder: C::with_soft_cap(max_size),
            max_size,
        }
    }

    fn compress(&mut self, samples: &[Sample]) -> TsdbResult {
        let mut encoder = C::with_soft_cap(self.max_size);
        for sample in samples {
            push_sample(&mut encoder, sample)?;
        }
        self.encoder = encoder;
        Ok(())
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
        self.encoder.bytes().len()
    }

    /// How many of this chunk's samples fall in `[start, end]`, assuming they are spread
    /// evenly over its span. Exact for a window that covers the chunk; an estimate for a
    /// partial one, and never more than `len`.
    fn estimate_samples_in(&self, start: Timestamp, end: Timestamp) -> usize {
        let len = self.len();
        let first = self.first_timestamp();
        let last = self.last_timestamp();
        if start <= first && end >= last {
            return len;
        }
        let lo = start.max(first);
        let hi = end.min(last);
        if lo > hi {
            return 0;
        }
        let span = last.saturating_sub(first).max(1) as u128;
        let part = (hi - lo) as u128 + 1;
        ((len as u128 * part / span) as usize + 1).min(len)
    }
}

impl<C: SampleCodec> ChunkOps for XorChunk<C> {
    fn range_iter(&self, start_ts: Timestamp, end_ts: Timestamp) -> SampleIter<'_> {
        // The codecs decode from the start of the stream, so a window that misses the chunk
        // would otherwise read every sample to find nothing.
        if self.is_empty()
            || start_ts > end_ts
            || end_ts < self.first_timestamp()
            || start_ts > self.last_timestamp()
        {
            return SampleIter::Empty;
        }
        C::into_sample_iter(RangeFiltered::new(self.encoder.iter(), start_ts, end_ts))
    }

    fn first_timestamp(&self) -> Timestamp {
        self.encoder.first_timestamp()
    }

    fn last_timestamp(&self) -> Timestamp {
        self.encoder.last_timestamp()
    }

    fn len(&self) -> usize {
        self.encoder.len()
    }

    fn last_value(&self) -> f64 {
        self.encoder.last_value()
    }

    /// The trait's name for [`data_size`](XorChunk::data_size), which documents what it counts.
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

        let mut new_encoder = C::with_soft_cap(self.max_size);
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
        // A window that misses the chunk needs neither a decode nor an allocation.
        if self.is_empty()
            || start > end
            || end < self.first_timestamp()
            || start > self.last_timestamp()
        {
            return Ok(vec![]);
        }

        // A full-chunk read reserves exactly the sample count, so it allocates once instead
        // of regrowing ~10 times. A partial read only knows an estimate, which uneven
        // timestamps can inflate up to the whole chunk, so it reserves at most
        // `PARTIAL_RANGE_RESERVE_CAP` and grows from there.
        //
        // The sample count is persisted, so a corrupt RDB or serialized chunk can carry an
        // absurd count. `Vec::with_capacity` aborts the process on capacity overflow or
        // allocation failure; `try_reserve_exact` turns both into a decoding error that
        // `load_rdb`, `deserialize` and the range readers already propagate.
        let covers_chunk = start <= self.first_timestamp() && end >= self.last_timestamp();
        let estimate = if covers_chunk {
            self.len()
        } else {
            self.estimate_samples_in(start, end)
                .min(PARTIAL_RANGE_RESERVE_CAP)
        };
        let mut samples = Vec::new();
        samples.try_reserve_exact(estimate).map_err(|e| {
            TsdbError::DecodingError(format!(
                "cannot reserve {estimate} samples for {} chunk range: {e}",
                C::ENCODING.name()
            ))
        })?;
        samples.extend(self.range_iter(start, end));
        Ok(samples)
    }

    fn upsert_sample(&mut self, sample: Sample, dp_policy: DuplicatePolicy) -> TsdbResult<bool> {
        let ts = sample.timestamp;
        let mut duplicate_found = false;
        let count = self.len();
        if count == 0 {
            self.add_sample(&sample)?;
            return Ok(true);
        }

        let mut encoder = C::with_soft_cap(self.max_size);
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
                // The sample that ended the scan, if it lies past the new one.
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
        Ok(!duplicate_found)
    }

    fn merge_samples(
        &mut self,
        samples: &[Sample],
        dp_policy: Option<DuplicatePolicy>,
    ) -> TsdbResult<Vec<SampleAddResult>> {
        if samples.is_empty() {
            return Ok(Vec::new());
        }

        // Samples are assumed sorted; the common append-only case skips the full re-encode.
        let first = samples[0];
        if self.is_empty() || first.timestamp > self.last_timestamp() {
            return append_samples(self, samples);
        }

        let mut encoder = C::with_soft_cap(self.max_size);
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
        self.data_size() >= self.max_size
    }

    fn bytes_per_sample(&self) -> usize {
        let count = self.len();
        if count < MIN_SAMPLES_FOR_BPS_ESTIMATE {
            return size_of::<Sample>() / 2;
        }
        // At least one byte: a flat series compresses to well under a byte per sample, and a
        // ratio floored to zero made every capacity estimate zero — so each MADD/ADDBULK batch
        // on such a series opened a chunk of its own.
        (self.data_size() / count).max(1)
    }

    fn clear(&mut self) {
        self.encoder.clear();
    }

    fn set_data(&mut self, samples: &[Sample]) -> TsdbResult<()> {
        self.compress(samples)
    }
}

impl<C: SampleCodec> Chunk for XorChunk<C> {
    fn split(&mut self) -> TsdbResult<Self> {
        if self.is_empty() {
            return Ok(self.clone());
        }

        let mut left = C::with_soft_cap(self.max_size);
        let mut right = C::with_soft_cap(self.max_size);

        let mid = self.len() / 2;
        for (i, value) in self.encoder.iter().enumerate() {
            let sample = value?;
            if i < mid {
                push_sample(&mut left, &sample)?;
            } else {
                push_sample(&mut right, &sample)?;
            }
        }

        self.encoder = left;
        // The upper half inherits this chunk's `max_size`, not `DEFAULT_CHUNK_SIZE_BYTES`:
        // `save_rdb` writes it out, so a wrong budget would survive a reload.
        Ok(Self {
            encoder: right,
            max_size: self.max_size,
        })
    }

    fn save_rdb(&self, rdb: *mut RedisModuleIO) {
        rdb_save_usize(rdb, self.max_size);
        self.encoder.rdb_save(rdb);
    }

    fn load_rdb(rdb: *mut RedisModuleIO, _enc_ver: i32) -> ValkeyResult<Self> {
        let max_size = rdb_load_usize(rdb)?;
        let mut encoder = C::rdb_load(rdb)?;
        encoder.set_soft_cap(max_size);
        Ok(Self { encoder, max_size })
    }

    fn serialize(&self, dest: &mut Vec<u8>) {
        write_uvarint(dest, self.max_size as u64);
        self.encoder.serialize(dest);
    }

    fn deserialize(buf: &[u8]) -> TsdbResult<Self> {
        let mut buf = buf;
        let max_size = try_read_uvarint(&mut buf).map_err(|_| TsdbError::ChunkDecoding)?;
        let mut encoder = C::deserialize(buf)?;
        encoder.set_soft_cap(max_size as usize);
        Ok(Self {
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
fn push_sample<C: SampleCodec>(encoder: &mut C, sample: &Sample) -> TsdbResult {
    encoder.push(sample).map_err(|e| {
        log_warning(format!(
            "{}: error adding sample: {e:?}",
            C::ENCODING.name()
        ));
        TsdbError::CannotAddSample(*sample)
    })
}
