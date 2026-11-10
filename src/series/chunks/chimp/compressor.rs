//! The ELF erasing layer: `AbstractElfCompressor` / `AbstractElfDecompressor`.
//!
//! This layer sits in front of the Chimp XOR codec. For each value it decides
//! whether the low mantissa bits can be safely erased (and later recovered from
//! the value's decimal places, `alpha`), emits a 1-, 2- or 6-bit case marker,
//! and hands the (possibly erased) raw bit pattern to Chimp. All markers and
//! Chimp values share one bit stream.
//!
//! ```text
//! 0  [chimp]             erased to the same alpha as the last `11` (or a repeat)
//! 10 [chimp]             raw bit pattern
//! 11 [alpha: 4] [chimp]  erased to `alpha` (0..=15) decimal places
//! ```
//!
//! An erased `v'` is recovered as `copysign(ceil(|v'| * 10^alpha) / 10^alpha, v')`,
//! and the encoder erases only when that gives `v` back bit for bit.
//!
//! Samples are stored as `(timestamp, value)` pairs. Each sample is written as
//! a delta-of-delta encoded timestamp followed by the ELF/Chimp encoded value:
//!
//! ```text
//! [varint first timestamp | dod timestamp][elf case marker][chimp value]
//! ```
//!
//! The stream carries no terminator: the decoder is told how many samples to
//! read (`ChimpDecompressor::new(bytes, count)`), so the trailing bit padding
//! is never mistaken for another sample and no bit pattern — NaN included —
//! has to be reserved as a sentinel.
//!
//! # Repeated values
//!
//! A value identical to its predecessor bypasses ELF entirely and is written as
//! Elf case `0` plus Chimp's `xor == 0` flag — 3 bits, against the 4 a flat
//! series used to pay by falling through to the raw `10` marker.
//!
//! That pairing is reserved for repeats, which is not free: a *distinct* value
//! can erase onto the bit pattern Chimp already holds, and the encoder has to
//! push those onto the raw path so they cannot be read back as a repeat. See
//! `write_value`.

use super::chimp_iterator::ChimpIterator;
use super::elf64::{MAX_ALPHA, alpha_scale, get_alpha, get_f_alpha, is_short_dyadic};
use super::encoder::{ChimpDec, ChimpEnc, ChimpEncState};
use crate::common::encoding::{
    try_read_f64_le, try_read_signed_varint, try_read_uvarint, write_f64_le, write_signed_varint,
    write_uvarint, zigzag_decode, zigzag_encode,
};
use crate::common::hash::hash_f64;
use crate::common::rdb::{
    RdbSerializable, rdb_load_timestamp, rdb_load_usize, rdb_save_timestamp, rdb_save_usize,
};
use crate::common::{Sample, Timestamp};
use crate::error::{TsdbError, TsdbResult};
use crate::series::chunks::stream::bitstream::BitStream;
use crate::series::chunks::stream::bitstream_reader::BitStreamReader;
use get_size2::{GetSize, GetSizeTracker};
use std::ffi::c_longlong;
use std::hash::Hash;
use std::io;
use valkey_module::digest::Digest;
use valkey_module::error::Error as ValkeyError;
use valkey_module::raw;

/// `last_alpha` before the first `11` header (and after `clear`).
const NO_ALPHA: i32 = -1;

/// Every this many samples the encoder counts decimal places even when `last_alpha` would do;
/// see `write_value`.
const HINT_RECHECK: u64 = 16;

/// How `write_value` encoded each sample, for tests.
#[cfg(test)]
#[derive(Debug, Clone, Default)]
pub(crate) struct ElfStats {
    /// The 3-bit repeat.
    pub repeats: u32,
    /// Elf case `10`.
    pub raw: u32,
    /// Elf case `0` with an erased value.
    pub same_alpha: u32,
    /// Elf case `11`.
    pub new_alpha: u32,
    /// Chimp flag `10`: the value's XOR reused the stored window. Counted in the Chimp
    /// encoder and copied here after each sample.
    pub window_reuse: u32,
}

/// Streaming ElfOnChimp compressor.
///
/// Compress one sample at a time with [`add_sample`](Self::add_sample); the
/// packed bytes are readable at any point via [`bytes`](Self::bytes) or
/// [`into_bytes`](Self::into_bytes). Decoding them needs [`count`](Self::count)
/// as well, since the stream has no terminator.
#[derive(Debug, Clone)]
pub struct ChimpCompressor {
    writer: BitStream,
    chimp: ChimpEnc,
    last_alpha: i32,
    count: u64,
    first_timestamp: Timestamp,
    last_timestamp: Timestamp,
    last_delta: i64,
    last_value: f64,
    /// Not part of the codec state: excluded from `Hash`, `PartialEq` and persistence.
    #[cfg(test)]
    pub(crate) stats: ElfStats,
}

impl Default for ChimpCompressor {
    fn default() -> Self {
        Self::new()
    }
}

impl GetSize for ChimpCompressor {
    // See `GorillaEncoder`: containers call `get_heap_size_with_tracker`, so
    // that is what a manual impl has to provide. Only the bit stream is on the
    // heap; every other field is inline.
    fn get_heap_size_with_tracker<T: GetSizeTracker>(&self, tracker: T) -> (usize, T) {
        self.writer.get_heap_size_with_tracker(tracker)
    }
}

impl Hash for ChimpCompressor {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.writer.hash(state);
        self.chimp.hash(state);
        self.last_alpha.hash(state);
        self.count.hash(state);
        self.first_timestamp.hash(state);
        self.last_timestamp.hash(state);
        self.last_delta.hash(state);
        hash_f64(self.last_value, state);
    }
}

impl PartialEq for ChimpCompressor {
    fn eq(&self, other: &Self) -> bool {
        self.count == other.count
            && self.first_timestamp == other.first_timestamp
            && self.last_timestamp == other.last_timestamp
            && self.last_delta == other.last_delta
            && self.last_alpha == other.last_alpha
            && self.last_value.to_bits() == other.last_value.to_bits()
            && self.chimp == other.chimp
            && self.writer == other.writer
    }
}

impl Eq for ChimpCompressor {}

impl ChimpCompressor {
    pub fn new() -> Self {
        Self {
            writer: BitStream::new(),
            chimp: ChimpEnc::new(),
            last_alpha: NO_ALPHA,
            count: 0,
            first_timestamp: 0,
            last_timestamp: 0,
            last_delta: 0,
            last_value: 0.0,
            #[cfg(test)]
            stats: ElfStats::default(),
        }
    }

    /// An empty compressor whose bit stream will not allocate past `max_size` (plus a few
    /// bytes of slack) while it is within budget; see `BitStream::set_soft_cap`.
    pub fn with_soft_cap(max_size: usize) -> Self {
        let mut c = Self::new();
        c.writer.set_soft_cap(max_size);
        c
    }

    /// See `BitStream::set_soft_cap`. A loaded chunk calls this once it knows its budget.
    pub fn set_soft_cap(&mut self, max_size: usize) {
        self.writer.set_soft_cap(max_size);
    }

    /// Number of samples added so far.
    pub fn count(&self) -> u64 {
        self.count
    }

    /// Timestamp of the first sample, or `0` if empty.
    pub fn first_timestamp(&self) -> Timestamp {
        self.first_timestamp
    }

    /// Timestamp of the most recently added sample, or `0` if empty.
    pub fn last_timestamp(&self) -> Timestamp {
        self.last_timestamp
    }

    /// Value of the most recently added sample, or `0.0` if empty.
    pub fn last_value(&self) -> f64 {
        self.last_value
    }

    /// Drop every sample and reset the codec state.
    pub fn clear(&mut self) {
        self.writer.clear();
        self.chimp = ChimpEnc::new();
        self.last_alpha = NO_ALPHA;
        self.count = 0;
        self.first_timestamp = 0;
        self.last_timestamp = 0;
        self.last_delta = 0;
        self.last_value = 0.0;
        #[cfg(test)]
        {
            self.stats = ElfStats::default();
        }
    }

    /// Release the bit stream's unused capacity.
    pub fn shrink_to_fit(&mut self) {
        self.writer.shrink_to_fit();
    }

    /// The persisted header, writable, as a forged payload could set it:
    /// `(count, first_timestamp, last_timestamp, last_value, last_delta, last_alpha)`.
    #[cfg(test)]
    pub(crate) fn header_mut_for_test(
        &mut self,
    ) -> (
        &mut u64,
        &mut Timestamp,
        &mut Timestamp,
        &mut f64,
        &mut i64,
        &mut i32,
    ) {
        (
            &mut self.count,
            &mut self.first_timestamp,
            &mut self.last_timestamp,
            &mut self.last_value,
            &mut self.last_delta,
            &mut self.last_alpha,
        )
    }

    /// Overwrites Chimp's stored value, keeping the rest of its window.
    #[cfg(test)]
    pub(crate) fn set_stored_val_for_test(&mut self, stored_val: u64) {
        let mut state = self.chimp.state();
        state.stored_val = stored_val;
        self.chimp = ChimpEnc::from_state(state).expect("window unchanged");
    }

    /// The bit stream, for tests that forge one.
    #[cfg(test)]
    pub(crate) fn stream_mut_for_test(&mut self) -> &mut BitStream {
        &mut self.writer
    }

    /// Iterate the samples written so far.
    pub fn iter(&'_ self) -> ChimpIterator<'_> {
        ChimpIterator::new(self)
    }

    /// Bits written so far (excluding the final terminator/flush).
    pub fn len_bits(&self) -> u64 {
        // `count` is the number of *free* bits left in the trailing byte.
        (self.writer.len() as u64) * 8 - self.writer.count as u64
    }

    /// Add the next sample: the timestamp is delta-of-delta encoded and the
    /// value goes through the ELF/Chimp path. The codec is causal (each point
    /// is compressed using only the previous point).
    ///
    /// Every `f64` is accepted, including `NaN`: NaNs bypass ELF erasure and
    /// are stored as raw bit patterns, so payload bits (a Prometheus stale
    /// marker, say) survive the round trip unchanged.
    pub fn add_sample(&mut self, timestamp: Timestamp, value: f64) -> io::Result<()> {
        self.write_timestamp(timestamp)?;
        self.write_value(value)?;
        self.last_value = value;
        self.count += 1;
        #[cfg(test)]
        {
            self.stats.window_reuse = self.chimp.window_reuse.0;
        }
        Ok(())
    }

    /// [`add_sample`](Self::add_sample) for an existing [`Sample`].
    pub fn add(&mut self, sample: Sample) -> io::Result<()> {
        self.add_sample(sample.timestamp, sample.value)
    }

    /// Bytes written so far. Pair them with [`count`](Self::count) to decode.
    pub fn bytes(&self) -> &[u8] {
        self.writer.bytes()
    }

    /// Consume the compressor and take the packed bytes.
    pub fn into_bytes(self) -> Vec<u8> {
        self.writer.into_bytes()
    }

    pub fn rdb_save(&self, rdb: *mut raw::RedisModuleIO) {
        let state = self.chimp.state();
        rdb_save_usize(rdb, self.count as usize);
        rdb_save_timestamp(rdb, self.first_timestamp);
        rdb_save_timestamp(rdb, self.last_timestamp);
        raw::save_double(rdb, self.last_value);
        raw::save_signed(rdb, self.last_delta);
        raw::save_signed(rdb, self.last_alpha as i64);
        raw::save_signed(rdb, state.stored_lz as i64);
        raw::save_signed(rdb, state.stored_tz as i64);
        raw::save_unsigned(rdb, state.stored_val);
        raw::save_unsigned(rdb, state.first as u64);
        self.writer.rdb_save(rdb);
    }

    pub fn rdb_load(rdb: *mut raw::RedisModuleIO) -> Result<Self, ValkeyError> {
        let count = rdb_load_usize(rdb)? as u64;
        let first_timestamp = rdb_load_timestamp(rdb)?;
        let last_timestamp = rdb_load_timestamp(rdb)?;
        let last_value = raw::load_double(rdb)?;
        let last_delta = raw::load_signed(rdb)?;
        let last_alpha = raw::load_signed(rdb)?;
        let stored_lz = raw::load_signed(rdb)?;
        let stored_tz = raw::load_signed(rdb)?;
        let stored_val = raw::load_unsigned(rdb)?;
        let first = raw::load_unsigned(rdb)? != 0;
        let (last_alpha, chimp) = codec_state(last_alpha, stored_lz, stored_tz, stored_val, first)
            .ok_or_else(|| ValkeyError::generic("Invalid chimp chunk codec state"))?;
        let writer = BitStream::rdb_load(rdb)?;

        Ok(Self {
            writer,
            chimp,
            last_alpha,
            count,
            first_timestamp,
            last_timestamp,
            last_delta,
            last_value,
            #[cfg(test)]
            stats: ElfStats::default(),
        })
    }

    pub fn serialize(&self, buf: &mut Vec<u8>) {
        let state = self.chimp.state();

        write_uvarint(buf, self.count);
        // Timestamps and the deltas are signed (out-of-order samples are legal
        // for this codec), so they go out zig-zagged.
        write_signed_varint(buf, self.first_timestamp);
        write_signed_varint(buf, self.last_timestamp);
        write_signed_varint(buf, self.last_delta);
        write_f64_le(buf, self.last_value);
        write_signed_varint(buf, self.last_alpha as i64);
        write_signed_varint(buf, state.stored_lz as i64);
        write_signed_varint(buf, state.stored_tz as i64);
        write_uvarint(buf, state.stored_val);
        write_uvarint(buf, state.first as u64);

        self.writer.serialize(buf);
    }

    pub fn deserialize(buf: &[u8]) -> TsdbResult<Self> {
        let mut buf = buf;

        let count = read_uvarint(&mut buf)?;
        let first_timestamp = read_signed_varint(&mut buf)?;
        let last_timestamp = read_signed_varint(&mut buf)?;
        let last_delta = read_signed_varint(&mut buf)?;
        let last_value = try_read_f64_le(&mut buf).map_err(|_| TsdbError::ChunkDecoding)?;
        let last_alpha = read_signed_varint(&mut buf)?;
        let stored_lz = read_signed_varint(&mut buf)?;
        let stored_tz = read_signed_varint(&mut buf)?;
        let stored_val = read_uvarint(&mut buf)?;
        let first = read_uvarint(&mut buf)? != 0;
        let (last_alpha, chimp) = codec_state(last_alpha, stored_lz, stored_tz, stored_val, first)
            .ok_or(TsdbError::ChunkDecoding)?;

        let writer = BitStream::deserialize(&mut buf).map_err(|_| TsdbError::ChunkDecoding)?;

        Ok(Self {
            writer,
            chimp,
            last_alpha,
            count,
            first_timestamp,
            last_timestamp,
            last_delta,
            last_value,
            #[cfg(test)]
            stats: ElfStats::default(),
        })
    }

    pub fn debug_digest(&self, dig: &mut Digest) {
        let state = self.chimp.state();
        self.writer.debug_digest(dig);
        dig.add_long_long(self.count as c_longlong);
        dig.add_long_long(self.first_timestamp);
        dig.add_long_long(self.last_timestamp);
        dig.add_long_long(self.last_delta);
        dig.add_long_long(self.last_value.to_bits() as c_longlong);
        dig.add_long_long(self.last_alpha.into());
        dig.add_long_long(state.stored_lz.into());
        dig.add_long_long(state.stored_tz.into());
        dig.add_long_long(state.stored_val as c_longlong);
        dig.add_long_long(state.first as c_longlong);
    }

    /// Encode a value identical to the one before it: Elf case `0` plus Chimp's
    /// `xor == 0` flag, 3 bits.
    ///
    /// Comparison is on the bit pattern, so `-0.0` never counts as a repeat of
    /// `0.0` and a NaN repeats only itself.
    fn write_repeat(&mut self) -> io::Result<()> {
        #[cfg(test)]
        {
            self.stats.repeats += 1;
        }
        // Elf case `0` folded into the Chimp flag write: one 3-bit write.
        self.chimp.add_repeat_prefixed(&mut self.writer, 0, 1)
    }

    /// Elf case `10`: the bit pattern stored verbatim.
    #[inline(always)]
    fn write_raw(&mut self, v_long: u64) -> io::Result<()> {
        #[cfg(test)]
        {
            self.stats.raw += 1;
        }
        self.chimp
            .add_value_prefixed(&mut self.writer, 0b10, 2, v_long)
    }

    fn write_value(&mut self, v: f64) -> io::Result<()> {
        if self.count > 0 && v.to_bits() == self.last_value.to_bits() {
            return self.write_repeat();
        }

        let v_long = v.to_bits();

        if v == 0.0 || !v.is_finite() || is_short_dyadic(v_long) {
            // Zero, the infinities and NaN have no decimal significant digits to erase.
            // Integers and short binary fractions have digits but no erasable bits (proof on
            // `is_short_dyadic`): the digit count would end in this same branch, so skip it.
            return self.write_raw(v_long);
        }

        let e = ((v_long >> 52) & 0x7ff) as i32;

        // Try the previous value's decimal places before counting this one's: quantized data
        // keeps one alpha, so this usually skips the count. It also keeps a value with fewer
        // places (100.2 after 100.23) under case `0`, which beats a header there and another
        // back. The gate is the search path's: erase only what the decoder's `recover` gives
        // back bit for bit, and never onto the stored Chimp value (that spells a repeat).
        //
        // The hint is skipped after a raw value: in full-precision data nearly every value is
        // raw and the hint nearly always misses, while quantized data rarely misses there.
        // Chimp holds the stored bit pattern of the last non-repeat value, which differs from
        // that value only if it was erased (`delta != 0`).
        //
        // A hit says nothing about how many places the value really has, so a stream whose
        // precision drops (nine places, then two) would stay at the larger alpha and keep
        // mantissa bits it could erase. Every `HINT_RECHECK`th sample skips the hint and
        // counts, which bounds that to one period.
        //
        // Both conditions read persisted state only (`count`, `last_value`, the Chimp state),
        // so the stream stays a function of the codec state and the input: an encoder
        // reloaded mid-chunk writes the same bytes as one that never stopped.
        let hinted = self.last_alpha != NO_ALPHA
            && self.chimp.state().stored_val != self.last_value.to_bits()
            && !self.count.is_multiple_of(HINT_RECHECK);
        if hinted && let Some((mask, _)) = erase_mask(self.last_alpha, e) {
            let v_prime_long = mask & v_long;
            if v_prime_long != v_long
                && v_prime_long != self.chimp.state().stored_val
                && try_recover(v_prime_long, self.last_alpha) == Some(v)
            {
                return self.write_same_alpha(v_prime_long);
            }
        }

        // Normal or subnormal: count the decimal places. A negative alpha means an integer,
        // which `is_short_dyadic` already sent down the raw path (kept because `get_f_alpha`
        // rejects it); more than `MAX_ALPHA` places do not fit the header; `Err` is a value the
        // digit count cannot handle. All of those are stored raw, losslessly.
        let alpha = match get_alpha(v) {
            Ok(alpha) if (0..=MAX_ALPHA).contains(&alpha) => alpha,
            _ => return self.write_raw(v_long),
        };
        let same_alpha = alpha == self.last_alpha;
        if same_alpha && hinted {
            // The hint ran every check below for this alpha, and one failed.
            return self.write_raw(v_long);
        }
        let Some((mask, _)) = erase_mask(alpha, e) else {
            return self.write_raw(v_long);
        };
        let v_prime_long = mask & v_long;

        // Erase only when it is provably reversible, with the decoder's own `recover`.
        if v_prime_long == v_long || try_recover(v_prime_long, alpha) != Some(v) {
            return self.write_raw(v_long);
        }

        if same_alpha {
            if v_prime_long == self.chimp.state().stored_val {
                // Case `0` over an unchanged Chimp value is how a repeat is spelled, and
                // this value is not one — a distinct `v` can erase onto the previous raw bit
                // pattern. Store it raw so the decoder cannot misread it as a repeat.
                return self.write_raw(v_long);
            }
            return self.write_same_alpha(v_prime_long);
        }

        // case `11` + 4-bit alpha
        debug_assert!(
            (0..=MAX_ALPHA).contains(&alpha),
            "alpha must fit the 4-bit header"
        );
        #[cfg(test)]
        {
            self.stats.new_alpha += 1;
        }
        self.last_alpha = alpha;
        self.chimp
            .add_value_prefixed(&mut self.writer, (alpha as u64) | 0x30, 6, v_prime_long)
    }

    /// Elf case `0`: `v_prime_long` erased to `last_alpha` decimal places.
    #[inline(always)]
    fn write_same_alpha(&mut self, v_prime_long: u64) -> io::Result<()> {
        #[cfg(test)]
        {
            self.stats.same_alpha += 1;
        }
        self.chimp
            .add_value_prefixed(&mut self.writer, 0, 1, v_prime_long)
    }

    /// Writes the timestamp of the next sample. The first is a varint; after that, the
    /// zig-zagged delta-of-delta `d` (`n` bits wide) is written as
    ///
    /// ```text
    /// 0                  d = 0
    /// 10     [d: 7]      n <= 7
    /// 110    [d: 9]      n <= 9
    /// 1110   [d: 12]     n <= 12
    /// 11110  [d: 16]     n <= 16
    /// 111110 [d: 32]     n <= 32
    /// 111111 [d: 64]     otherwise
    /// ```
    ///
    /// Every tier but the last is one `write_bits`: the prefix is OR-ed above the payload, so
    /// the bits `d` does not use are the prefix's. Timestamps may go backwards; the
    /// delta-of-delta is signed, so an out-of-order point costs bits but still round-trips.
    fn write_timestamp(&mut self, timestamp: Timestamp) -> io::Result<()> {
        if self.count == 0 {
            self.writer.write_varint(timestamp)?;
            self.first_timestamp = timestamp;
            self.last_timestamp = timestamp;
            self.last_delta = 0;
            return Ok(());
        }
        let new_delta = timestamp.wrapping_sub(self.last_timestamp);
        let delta_d = new_delta.wrapping_sub(self.last_delta);

        if delta_d == 0 {
            self.writer.write_bit(false);
        } else {
            let enc = zigzag_encode(delta_d);
            // A nonzero `delta_d` zig-zags to a nonzero value, so `length` is at least 1.
            let length = 64 - enc.leading_zeros();

            if length <= 7 {
                self.writer.write_bits(9, enc | (0b10 << 7))?;
            } else if length <= 9 {
                self.writer.write_bits(12, enc | (0b110 << 9))?;
            } else if length <= 12 {
                self.writer.write_bits(16, enc | (0b1110 << 12))?;
            } else if length <= 16 {
                self.writer.write_bits(21, enc | (0b11110 << 16))?;
            } else if length <= 32 {
                self.writer.write_bits(38, enc | (0b111110 << 32))?;
            } else {
                self.writer.write_bits(6, 0b111111)?;
                self.writer.write_bits(64, enc)?;
            }
        }

        self.last_delta = new_delta;
        self.last_timestamp = timestamp;
        Ok(())
    }
}

/// Streaming ElfOnChimp decompressor.
///
/// Constructed with the sample count the matching [`ChimpCompressor`] reported,
/// since the stream itself is unterminated.
pub struct ChimpDecompressor<'a> {
    reader: BitStreamReader<'a>,
    chimp: ChimpDec,
    last_alpha: i32,
    count: u64,
    sample_count: u64,
    last_timestamp: Timestamp,
    last_delta: i64,
    /// The previously decoded value, returned verbatim for a repeat.
    last_value: f64,
}

impl<'a> ChimpDecompressor<'a> {
    pub fn new(data: &'a [u8], sample_count: u64) -> Self {
        Self {
            reader: BitStreamReader::new(data),
            chimp: ChimpDec::new(),
            last_alpha: NO_ALPHA,
            count: 0,
            sample_count,
            last_timestamp: 0,
            last_delta: 0,
            last_value: 0.0,
        }
    }

    /// Read the next sample. Returns `Ok(None)` once all `sample_count`
    /// samples have been read.
    #[inline]
    pub fn next_sample(&mut self) -> io::Result<Option<Sample>> {
        if self.count == self.sample_count {
            return Ok(None);
        }
        let timestamp = self.read_timestamp()?;
        let value = self.read_value()?;
        self.count += 1;
        Ok(Some(Sample { timestamp, value }))
    }

    /// Reads the delta-of-delta encoded timestamp of the next sample.
    #[inline]
    fn read_timestamp(&mut self) -> io::Result<Timestamp> {
        if self.count == 0 {
            let timestamp = self.reader.read_varint()?;
            self.last_timestamp = timestamp;
            self.last_delta = 0;
            return Ok(timestamp);
        }

        // The prefix is 1-6 bits, terminated by the first zero (`0`, `10`, `110`, `1110`,
        // `11110`, `111110`) or by `111111`. One peek of six bits decides it, in place of a
        // bit-by-bit loop; past the end of the stream the peek pads with zeros, and `skip`
        // then reports the truncation.
        let head = self.reader.peek_upto(6);
        let delta_d = if head < 0b10_0000 {
            self.reader.skip(1)?;
            0
        } else if head < 0b11_0000 {
            self.reader.skip(2)?;
            zigzag_decode(self.reader.read_bits(7)?)
        } else if head < 0b11_1000 {
            self.reader.skip(3)?;
            zigzag_decode(self.reader.read_bits(9)?)
        } else if head < 0b11_1100 {
            self.reader.skip(4)?;
            zigzag_decode(self.reader.read_bits(12)?)
        } else if head < 0b11_1110 {
            self.reader.skip(5)?;
            zigzag_decode(self.reader.read_bits(16)?)
        } else if head == 0b11_1110 {
            self.reader.skip(6)?;
            zigzag_decode(self.reader.read_bits(32)?)
        } else {
            self.reader.skip(6)?;
            zigzag_decode(self.reader.read_bits(64)?)
        };

        self.last_delta = self.last_delta.wrapping_add(delta_d);
        self.last_timestamp = self.last_timestamp.wrapping_add(self.last_delta);
        Ok(self.last_timestamp)
    }

    #[inline]
    fn read_value(&mut self) -> io::Result<f64> {
        // Cases `0`, `10`, `11`: one two-bit peek instead of one or two reads.
        let head = self.reader.peek_upto(2);
        let value = if head < 0b10 {
            // case `0`: a repeat, or a value sharing the previous alpha.
            self.reader.skip(1)?;
            let (bits, repeat) = self.chimp.read_value_flagged(&mut self.reader)?;
            if repeat {
                // Returning `last_value` verbatim rather than recovering it from
                // the Chimp bits is what makes the 3-bit repeat legal after a raw
                // (case `10`) value, whose stored bits are not an erased
                // `v_prime` and would not survive `recover`.
                return Ok(self.last_value);
            }
            recover(bits, self.last_alpha)?
        } else if head == 0b10 {
            // case `10`: raw value (zero, infinities, NaN, or anything ELF
            // could not erase reversibly)
            self.reader.skip(2)?;
            let bits = self.chimp.read_value(&mut self.reader)?;
            f64::from_bits(bits)
        } else {
            // case `11`: new 4-bit alpha
            self.reader.skip(2)?;
            self.last_alpha = self.reader.read_bits(4)? as i32;
            let bits = self.chimp.read_value(&mut self.reader)?;
            recover(bits, self.last_alpha)?
        };

        self.last_value = value;
        Ok(value)
    }

    /// Consume the rest of the stream into a `Vec<Sample>`.
    pub fn collect(mut self) -> io::Result<Vec<Sample>> {
        let mut out = Vec::new();
        while let Some(sample) = self.next_sample()? {
            out.push(sample);
        }
        Ok(out)
    }
}

/// The mask that keeps the mantissa bits `alpha` decimal places need in a value whose biased
/// exponent is `exponent`, and how many low bits it erases. `None` when that is 4 bits or
/// fewer: too few to pay for erasing.
#[inline(always)]
fn erase_mask(alpha: i32, exponent: i32) -> Option<(u64, i32)> {
    let erase_bits = 52 - (get_f_alpha(alpha) + exponent - 1023);
    if erase_bits <= 4 {
        return None;
    }
    // Java: `0xffffffffffffffffL << eraseBits` (shift is mod 64).
    Some((u64::MAX.wrapping_shl(erase_bits as u32), erase_bits))
}

/// Recover a value erased to `alpha` decimal places: round `|v'|` up to the next multiple
/// of `10^-alpha` and restore the sign, `copysign(ceil(|v'| * 10^alpha) / 10^alpha, v')`.
///
/// `Err` for an `alpha` outside `0..=MAX_ALPHA`, which only a corrupt stream produces (case
/// `0` before any `11` header leaves `NO_ALPHA`).
#[inline(always)]
fn recover(v_prime_bits: u64, alpha: i32) -> io::Result<f64> {
    let Some(scale) = alpha_scale(alpha) else {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "ELF decimal places out of range",
        ));
    };
    let v_prime = f64::from_bits(v_prime_bits);
    Ok(((v_prime.abs() * scale).ceil() / scale).copysign(v_prime))
}

/// Encode-side recoverability check: returns the value the decoder would
/// reconstruct from an erased `v_prime` and `alpha`, or `None` if the
/// recovery failed (so the caller stores the raw value instead).
pub(crate) fn try_recover(v_prime_long: u64, alpha: i32) -> Option<f64> {
    recover(v_prime_long, alpha).ok()
}

/// Validate persisted codec state: `last_alpha` is `NO_ALPHA` or fits the 4-bit header, and
/// the Chimp window is one the encoder writes. `None` rejects the chunk. Range checks run on
/// the persisted `i64`s, so a value that only truncates to a valid `i32` is rejected too.
fn codec_state(
    last_alpha: i64,
    stored_lz: i64,
    stored_tz: i64,
    stored_val: u64,
    first: bool,
) -> Option<(i32, ChimpEnc)> {
    let last_alpha = i32::try_from(last_alpha).ok()?;
    if last_alpha != NO_ALPHA && !(0..=MAX_ALPHA).contains(&last_alpha) {
        return None;
    }
    let chimp = ChimpEnc::from_state(ChimpEncState {
        stored_lz: i32::try_from(stored_lz).ok()?,
        stored_tz: i32::try_from(stored_tz).ok()?,
        stored_val,
        first,
    })?;
    Some((last_alpha, chimp))
}

fn read_uvarint(buf: &mut &[u8]) -> TsdbResult<u64> {
    try_read_uvarint(buf).map_err(|_| TsdbError::ChunkDecoding)
}

fn read_signed_varint(buf: &mut &[u8]) -> TsdbResult<i64> {
    try_read_signed_varint(buf).map_err(|_| TsdbError::ChunkDecoding)
}

#[cfg(test)]
mod tests {
    use super::super::elf64::boundary_corpus;
    use super::*;

    fn round_trip(samples: &[Sample]) {
        let mut compressor = ChimpCompressor::new();
        for sample in samples {
            compressor.add(*sample).unwrap();
        }
        let count = compressor.count();
        assert_eq!(count, samples.len() as u64);
        let bytes = compressor.into_bytes();

        let decoded = ChimpDecompressor::new(&bytes, count).collect().unwrap();
        assert_eq!(decoded.len(), samples.len(), "sample count mismatch");
        for (actual, expected) in decoded.iter().zip(samples) {
            assert_eq!(actual.timestamp, expected.timestamp, "timestamp mismatch");
            assert_eq!(
                actual.value.to_bits(),
                expected.value.to_bits(),
                "value mismatch at ts {}",
                expected.timestamp
            );
        }
    }

    #[test]
    fn empty_stream() {
        round_trip(&[]);
    }

    #[test]
    fn single_sample() {
        round_trip(&[Sample::new(1_700_000_000_000, 123.456)]);
    }

    #[test]
    fn regular_interval() {
        let samples: Vec<Sample> = (0..500)
            .map(|i| Sample::new(1_700_000_000_000 + i * 1000, 100.5 + (i as f64) * 0.25))
            .collect();
        round_trip(&samples);
    }

    #[test]
    fn special_values() {
        let values = [
            0.0,
            -0.0,
            f64::INFINITY,
            f64::NEG_INFINITY,
            1.0,
            -1.0,
            f64::MIN_POSITIVE,
            f64::MAX,
            f64::MIN,
            1e-300,
            123.456,
            -0.001,
        ];
        let samples: Vec<Sample> = values
            .iter()
            .enumerate()
            .map(|(i, v)| Sample::new(1000 + i as Timestamp * 7, *v))
            .collect();
        round_trip(&samples);
    }

    #[test]
    fn irregular_and_out_of_order_timestamps() {
        let timestamps = [
            0i64,
            1,
            100,
            101,
            5_000_000,
            5_000_001,
            // backwards
            4_000_000,
            4_000_100,
            // delta-of-delta wider than 32 bits
            i64::from(i32::MAX) * 4,
            1,
            i64::MAX / 4,
        ];
        let samples: Vec<Sample> = timestamps
            .iter()
            .enumerate()
            .map(|(i, ts)| Sample::new(*ts, i as f64 * 1.5))
            .collect();
        round_trip(&samples);
    }

    /// A delta-of-delta of `dod` after two regular samples, repeating the value: the stream
    /// holds the timestamp code plus the 3-bit value repeat, so subtracting 3 leaves the
    /// timestamp code's length.
    fn timestamp_code_bits(dod: i64) -> u64 {
        let mut c = ChimpCompressor::new();
        c.add_sample(0, 1.0).unwrap();
        c.add_sample(1000, 1.0).unwrap();
        let before = c.len_bits();
        c.add_sample(2000 + dod, 1.0).unwrap();
        c.len_bits() - before - 3
    }

    #[test]
    fn timestamp_tiers_cost_their_documented_bits() {
        // (delta-of-delta, bits) at each bucket's upper edge and one past it.
        for (dod, bits) in [
            (0i64, 1),
            (63, 9),
            (-64, 9),
            (64, 12),
            (255, 12),
            (256, 16),
            (2047, 16),
            (2048, 21),
            (32767, 21),
            (32768, 38),
            (i64::from(i32::MAX), 38),
            (i64::from(i32::MAX) + 1, 70),
        ] {
            assert_eq!(timestamp_code_bits(dod), bits, "dod {dod}");
        }
    }

    /// The stream after the first sample is the timestamp codes spelled out in the grammar
    /// (prefix, then the zig-zagged delta-of-delta in the tier's width), each followed by the
    /// `000` of a repeated value. Spelling the bits out pins the prefixes, not just the lengths.
    #[test]
    fn timestamp_tiers_write_their_documented_bits() {
        // (delta-of-delta, expected code); the payload is the zig-zag in the tier's width.
        let cases: [(i64, &str, usize, u64); 12] = [
            (1, "10", 7, 2),
            (-1, "10", 7, 1),
            (64, "110", 9, 128),
            (-65, "110", 9, 129),
            (256, "1110", 12, 512),
            (-2048, "1110", 12, 4095),
            (2048, "11110", 16, 4096),
            (-32768, "11110", 16, 65535),
            (32768, "111110", 32, 65536),
            (-i64::from(i32::MAX) - 1, "111110", 32, (1 << 32) - 1),
            (i64::from(i32::MAX) + 1, "111111", 64, 1 << 32),
            (-i64::from(i32::MAX) - 2, "111111", 64, (1 << 32) + 1),
        ];
        let mut c = ChimpCompressor::new();
        c.add_sample(1_000_000, 1.0).unwrap();
        let start = c.len_bits() as usize;
        // The second sample's delta-of-delta is its whole delta, 1000: zig-zag 2000, 12 bits.
        c.add_sample(1_001_000, 1.0).unwrap();
        let mut expected = format!("1110{:012b}000", zigzag_encode(1000));
        let (mut ts, mut delta) = (1_001_000i64, 1000i64);
        for (dod, prefix, width, zig) in cases {
            assert_eq!(zigzag_encode(dod), zig, "dod {dod}");
            delta += dod;
            ts += delta;
            c.add_sample(ts, 1.0).unwrap();
            expected.push_str(&format!("{prefix}{zig:0width$b}000"));
        }
        let bytes = c.bytes();
        let actual: String = (start..c.len_bits() as usize)
            .map(|i| {
                if bytes[i / 8] >> (7 - i % 8) & 1 == 1 {
                    '1'
                } else {
                    '0'
                }
            })
            .collect();
        assert_eq!(actual, expected);
    }

    #[test]
    fn timestamp_tier_edges_round_trip() {
        // Every edge of every tier, both signs, in one stream so the delta state carries
        // across tier changes. The deltas are cumulative, so each step lands on the intended
        // delta-of-delta.
        let edges: [i64; 13] = [
            1,
            63,
            64,
            255,
            256,
            2047,
            2048,
            32767,
            32768,
            i64::from(i32::MAX),
            i64::from(i32::MAX) + 1,
            1 << 40,
            i64::MAX / 8,
        ];
        let mut dods = vec![0i64];
        for e in edges {
            dods.extend([e, -e, 0, e - 1, -(e - 1), -e - 1]);
        }
        let mut samples = vec![Sample::new(1_700_000_000_000, 1.0)];
        let (mut ts, mut delta) = (1_700_000_000_000i64, 0i64);
        for (i, dod) in dods.iter().enumerate() {
            delta = delta.wrapping_add(*dod);
            ts = ts.wrapping_add(delta);
            samples.push(Sample::new(ts, i as f64));
        }
        round_trip(&samples);
    }

    #[test]
    fn truncated_timestamp_escape_reports_eof() {
        let mut c = ChimpCompressor::new();
        c.add_sample(0, 1.0).unwrap();
        c.add_sample(1000, 1.0).unwrap();
        c.add_sample(2000 + (1i64 << 40), 1.0).unwrap();
        let bytes = c.bytes();
        let cut = &bytes[..bytes.len() - 4];
        assert!(ChimpDecompressor::new(cut, 3).collect().is_err());
    }

    #[test]
    fn pseudo_random_samples() {
        // Deterministic LCG so failures are reproducible.
        let mut state = 0x2545_F491_4F6C_DD1Du64;
        let mut next = || {
            state = state
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            state
        };

        let mut timestamp = 1_600_000_000_000i64;
        let samples: Vec<Sample> = (0..2000)
            .map(|_| {
                timestamp += (next() % 60_000) as i64;
                // Values with a small number of decimal digits, the shape ELF
                // is designed for, mixed with coarser magnitudes.
                let value = (next() % 1_000_000) as f64 / 100.0 - 5000.0;
                Sample::new(timestamp, value)
            })
            .collect();
        round_trip(&samples);
    }

    #[test]
    fn nan_round_trips_bit_exactly() {
        // Every NaN payload must survive, including the canonical quiet NaN
        // that the reference codec reserves as its end-of-stream sentinel and
        // the Prometheus stale marker.
        let values = [
            f64::NAN,
            f64::from_bits(0x7ff8_0000_0000_0000), // canonical quiet NaN
            f64::from_bits(0xfff8_0000_0000_0000), // negative quiet NaN
            f64::from_bits(0x7ff0_0000_0000_0002), // Prometheus stale NaN
            f64::from_bits(0x7ff0_0000_0000_0001), // signalling NaN
            1.5,
            f64::NAN,
            f64::NAN,
            0.0,
            f64::NAN,
        ];
        let samples: Vec<Sample> = values
            .iter()
            .enumerate()
            .map(|(i, v)| Sample::new(1000 + i as Timestamp * 100, *v))
            .collect();
        round_trip(&samples);
    }

    #[test]
    fn nan_leading_a_stream() {
        // The first value goes through Chimp's verbatim 64-bit path.
        round_trip(&[
            Sample::new(1, f64::NAN),
            Sample::new(2, 42.0),
            Sample::new(3, f64::NEG_INFINITY),
        ]);
    }

    #[test]
    fn repeated_values_round_trip() {
        for run in [1usize, 2, 3, 4, 17, 500] {
            let mut samples = vec![Sample::new(1000, 1.25)];
            for i in 0..run {
                samples.push(Sample::new(1010 + i as Timestamp * 10, 1.25));
            }
            samples.push(Sample::new(9_000_000, -3.5));
            samples.push(Sample::new(9_000_010, -3.5));
            round_trip(&samples);
        }
    }

    #[test]
    fn repeats_of_special_values_round_trip_bit_exactly() {
        // `-0.0` must not count as a repeat of `0.0`, and a NaN repeats only
        // the exact payload before it.
        let values = [
            0.0,
            0.0,
            0.0,
            -0.0,
            -0.0,
            0.0,
            f64::NAN,
            f64::NAN,
            f64::NAN,
            f64::from_bits(0x7ff0_0000_0000_0002),
            f64::from_bits(0x7ff0_0000_0000_0002),
            f64::NAN,
            f64::INFINITY,
            f64::INFINITY,
            f64::NEG_INFINITY,
        ];
        let samples: Vec<Sample> = values
            .iter()
            .enumerate()
            .map(|(i, v)| Sample::new(1000 + i as Timestamp * 10, *v))
            .collect();
        round_trip(&samples);
    }

    #[test]
    fn a_long_flat_stretch_round_trips() {
        let n = 8_192;
        let mut samples: Vec<Sample> = (0..n).map(|i| Sample::new(1000 + i * 10, 8.75)).collect();
        samples.push(Sample::new(1000 + n * 10, 8.76));
        round_trip(&samples);
    }

    #[test]
    fn repeats_interleaved_with_distinct_values_round_trip() {
        // Walks the state machine through None -> One -> Run and back out
        // again at every position.
        let mut state = 0x9E37_79B9_7F4A_7C15u64;
        let mut next = || {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state
        };

        let mut samples = Vec::new();
        let mut ts = 1_600_000_000_000i64;
        let mut value = 100.0f64;
        for _ in 0..5000 {
            // Three samples in four repeat the one before them.
            if next() % 4 == 0 {
                value = (next() % 100_000) as f64 / 100.0;
            }
            ts += 1000;
            samples.push(Sample::new(ts, value));
        }
        round_trip(&samples);
    }

    #[test]
    fn near_miss_values_do_not_masquerade_as_repeats() {
        // A distinct value can erase onto the bit pattern the Chimp layer
        // already holds; the encoder falls back to a raw write so the decoder
        // cannot read the result as a repeat.
        let mut samples = Vec::new();
        let mut ts = 0;
        for k in 1..2000u64 {
            let nice = k as f64 / 100.0;
            let bits = nice.to_bits();
            for v in [
                nice,
                f64::from_bits(bits - 1),
                f64::from_bits(bits + 1),
                nice,
            ] {
                ts += 10;
                samples.push(Sample::new(ts, v));
            }
        }
        round_trip(&samples);
    }

    #[test]
    fn every_repeat_costs_four_bits() {
        // Settle the timestamp delta first, so from here each sample's
        // delta-of-delta is zero and costs exactly one bit. On top of that a
        // repeat is Elf case `0` plus Chimp's zero-XOR flag.
        let mut compressor = ChimpCompressor::new();
        compressor.add_sample(1000, 12.5).unwrap();
        compressor.add_sample(2000, 13.5).unwrap();
        compressor.add_sample(3000, 14.5).unwrap();

        let mut ts = 4000;
        for _ in 0..8 {
            let before = compressor.len_bits();
            compressor.add_sample(ts, 14.5).unwrap();
            assert_eq!(compressor.len_bits() - before, 1 + 3);
            ts += 1000;
        }
    }

    #[test]
    fn a_flat_series_costs_half_a_byte_per_sample() {
        let n = 4096;
        let mut compressor = ChimpCompressor::new();
        for i in 0..n {
            compressor
                .add_sample(1_700_000_000_000 + i * 1000, 19.5)
                .unwrap();
        }
        let bits_per_sample = compressor.len_bits() as f64 / n as f64;
        assert!(
            bits_per_sample < 4.1,
            "flat series cost {bits_per_sample} bits/sample"
        );
    }

    #[test]
    fn appending_after_serialization_still_detects_repeats() {
        // Repeat detection reads `last_value`, so it has to survive a
        // serialization round trip: split at every point and keep going.
        let samples: Vec<Sample> = (0..24).map(|i| Sample::new(1000 + i * 10, 7.5)).collect();

        for split in 1..samples.len() {
            let mut compressor = ChimpCompressor::new();
            for sample in &samples[..split] {
                compressor.add(*sample).unwrap();
            }

            let mut buf = Vec::new();
            compressor.serialize(&mut buf);
            let mut restored = ChimpCompressor::deserialize(&buf).unwrap();
            assert_eq!(restored, compressor, "split at {split}");

            for sample in &samples[split..] {
                restored.add(*sample).unwrap();
            }

            let count = restored.count();
            let bytes = restored.into_bytes();
            let decoded = ChimpDecompressor::new(&bytes, count).collect().unwrap();
            assert_eq!(decoded.len(), samples.len(), "split at {split}");
            for (actual, want) in decoded.iter().zip(&samples) {
                assert_eq!(actual.timestamp, want.timestamp, "split at {split}");
                assert_eq!(
                    actual.value.to_bits(),
                    want.value.to_bits(),
                    "split at {split}"
                );
            }
        }
    }

    #[test]
    fn negative_quantized_values_recover_bit_exactly() {
        round_trip(&[
            Sample::new(1, -100.23),
            Sample::new(2, -0.01),
            Sample::new(3, -99.9),
            Sample::new(4, -1e-5),
            Sample::new(5, 0.01),
            Sample::new(6, -0.01),
        ]);
    }

    #[test]
    fn boundary_corpus_round_trips() {
        let samples: Vec<Sample> = boundary_corpus()
            .into_iter()
            .enumerate()
            .map(|(i, v)| Sample::new(i as Timestamp * 10, v))
            .collect();
        round_trip(&samples);
    }

    #[test]
    fn recover_rejects_out_of_range_alpha() {
        assert!(recover(1.5f64.to_bits(), -1).is_err());
        assert!(recover(1.5f64.to_bits(), 16).is_err());
    }

    /// Recovery works on the magnitude and puts the sign back, so a zero result keeps the
    /// sign of `v'` (only reachable on corrupt input: an erased nonzero value is never zero).
    #[test]
    fn recover_restores_the_sign() {
        assert_eq!(
            recover((-0.0f64).to_bits(), 2).unwrap().to_bits(),
            (-0.0f64).to_bits()
        );
        assert_eq!(
            recover(0.0f64.to_bits(), 2).unwrap().to_bits(),
            0.0f64.to_bits()
        );
        let v_prime = f64::from_bits(100.23f64.to_bits() & !0xff);
        assert_eq!(recover(v_prime.to_bits(), 2).unwrap(), 100.23);
        assert_eq!(recover((-v_prime).to_bits(), 2).unwrap(), -100.23);
    }

    #[test]
    fn fixed_decimal_data_keeps_alpha_across_a_magnitude_boundary() {
        // 99.81 .. 100.19, every one with two decimal places (the integer 100.00 and the
        // one-place x.x0 values are skipped): one `11` header, then case `0` throughout.
        let mut c = ChimpCompressor::new();
        let mut ts = 1000;
        for m in 9981..10020 {
            if m % 10 == 0 {
                continue;
            }
            c.add_sample(ts, m as f64 / 100.0).unwrap();
            ts += 10;
        }
        assert_eq!(c.stats.new_alpha, 1);
        assert_eq!(c.stats.raw, 0);
    }

    #[test]
    fn hint_reuses_alpha_for_values_with_fewer_digits() {
        // 100.2 (one place) under last_alpha 2 must be case `0`, not a new header.
        let mut c = ChimpCompressor::new();
        c.add_sample(1000, 100.23).unwrap();
        c.add_sample(2000, 100.2).unwrap();
        c.add_sample(3000, 100.27).unwrap();
        assert_eq!(c.stats.new_alpha, 1);
        assert_eq!(c.stats.same_alpha, 2);
        round_trip(&[
            Sample::new(1000, 100.23),
            Sample::new(2000, 100.2),
            Sample::new(3000, 100.27),
        ]);
    }

    /// A value with more places than `last_alpha` does not recover under it: the search runs
    /// and writes a new header.
    #[test]
    fn hint_falls_back_to_the_search_for_more_digits() {
        let mut c = ChimpCompressor::new();
        c.add_sample(1000, 100.23).unwrap();
        c.add_sample(2000, 100.234).unwrap();
        c.add_sample(3000, 100.2).unwrap();
        assert_eq!(c.stats.new_alpha, 2);
        assert_eq!(c.stats.same_alpha, 1);
        assert_eq!(c.last_alpha, 3);
    }

    /// After a raw value the hint is skipped and the count decides: 100.2 gets its own header.
    #[test]
    fn hint_is_skipped_after_a_raw_value() {
        let mut c = ChimpCompressor::new();
        c.add_sample(1000, 100.23).unwrap();
        c.add_sample(2000, 100.5).unwrap();
        c.add_sample(3000, 100.2).unwrap();
        assert_eq!(c.stats.raw, 1);
        assert_eq!(c.stats.new_alpha, 2);
        assert_eq!(c.last_alpha, 1);
    }

    /// The hint reads only persisted state, so a reloaded encoder writes the same bytes as one
    /// that never stopped (the digest covers the bytes).
    #[test]
    fn reloading_mid_stream_does_not_change_the_bytes() {
        let mut values = mixed_dyadic_values();
        values.truncate(400);
        for block in 0..4 {
            for i in 1..60 {
                values.push(if block % 2 == 0 {
                    100.0 + i as f64 * 0.01
                } else {
                    100.0 + i as f64 * 1e-9
                });
            }
        }
        let samples: Vec<Sample> = values
            .iter()
            .enumerate()
            .map(|(i, v)| Sample::new(i as Timestamp * 1000, *v))
            .collect();
        let mut whole = ChimpCompressor::new();
        for s in &samples {
            whole.add(*s).unwrap();
        }
        for split in (1..samples.len()).step_by(7) {
            let mut c = ChimpCompressor::new();
            for s in &samples[..split] {
                c.add(*s).unwrap();
            }
            let mut buf = Vec::new();
            c.serialize(&mut buf);
            let mut restored = ChimpCompressor::deserialize(&buf).unwrap();
            for s in &samples[split..] {
                restored.add(*s).unwrap();
            }
            assert_eq!(restored.bytes(), whole.bytes(), "split at {split}");
        }
    }

    /// Two-place values recover under nine places too, so only the periodic re-count moves
    /// the encoder down to alpha 2, within one `HINT_RECHECK` period.
    #[test]
    fn precision_drop_is_picked_up_by_the_recheck() {
        let mut samples = vec![Sample::new(0, 100.123456789)];
        samples.extend(
            (1..60)
                .filter(|i| i % 5 != 0)
                .map(|i| Sample::new(i * 1000, 100.0 + i as f64 * 0.01)),
        );
        let mut c = ChimpCompressor::new();
        for s in &samples {
            c.add(*s).unwrap();
        }
        assert_eq!(c.stats.new_alpha, 2);
        assert_eq!(c.stats.raw, 0);
        assert_eq!(c.last_alpha, 2);
        round_trip(&samples);
    }

    #[test]
    fn mixed_precision_stream_round_trips() {
        let mut samples = Vec::new();
        let mut ts = 0;
        for block in 0..6 {
            for i in 0..200 {
                ts += 1000;
                let v = if block % 2 == 0 {
                    100.0 + (i as f64) * 0.01
                } else {
                    100.0 + (i as f64) * 1e-9
                };
                samples.push(Sample::new(ts, v));
            }
        }
        round_trip(&samples);
    }

    /// More decimal places than the 4-bit header holds: stored raw, and still lossless.
    #[test]
    fn values_with_more_than_fifteen_decimal_places_are_stored_raw() {
        let values = [1.23e-14, -4.5e-16, 7e-17, 7e-17];
        let mut c = ChimpCompressor::new();
        for (i, v) in values.iter().enumerate() {
            c.add_sample(i as Timestamp, *v).unwrap();
        }
        assert_eq!(c.stats.raw, 3);
        assert_eq!(c.stats.repeats, 1);
        assert_eq!(c.stats.new_alpha + c.stats.same_alpha, 0);
        let samples: Vec<Sample> = values
            .iter()
            .enumerate()
            .map(|(i, v)| Sample::new(i as Timestamp, *v))
            .collect();
        round_trip(&samples);
    }

    /// Every sample is counted exactly once: a repeat, a raw value, or an erased value under
    /// the previous or a new alpha.
    #[test]
    fn stats_account_for_every_sample() {
        let mut c = ChimpCompressor::new();
        for (i, v) in mixed_dyadic_values().into_iter().enumerate() {
            c.add_sample(i as Timestamp * 1000, v).unwrap();
        }
        let s = &c.stats;
        assert!(
            s.repeats > 0 && s.raw > 0 && s.same_alpha > 0 && s.new_alpha > 0,
            "{s:?}"
        );
        assert_eq!(
            u64::from(s.repeats + s.raw + s.same_alpha + s.new_alpha),
            c.count()
        );
    }

    #[test]
    fn append_after_reload_reuses_persisted_window() {
        // Two 2-place values open a trailing window (flag 01); serialize mid-window, reload,
        // append a third that fits, decode everything bit-exactly, and see one reuse.
        // Erased to two places, 100.23 ^ 100.31 has 19 leading and 40 trailing zeros (window
        // lz 18, tz 40); 100.31 ^ 100.26 has 22 and 40, so it fits. (100.27 would not: its XOR
        // has 39 trailing zeros.)
        let mut c = ChimpCompressor::new();
        c.add_sample(1000, 100.23).unwrap();
        c.add_sample(2000, 100.31).unwrap();
        let mut buf = Vec::new();
        c.serialize(&mut buf);
        let mut r = ChimpCompressor::deserialize(&buf).unwrap();
        assert_eq!(r, c);
        r.add_sample(3000, 100.26).unwrap();
        assert_eq!(r.stats.window_reuse, 1);
        let got = ChimpDecompressor::new(r.bytes(), r.count())
            .collect()
            .unwrap();
        assert_eq!(
            got.iter().map(|s| s.value.to_bits()).collect::<Vec<_>>(),
            [100.23f64, 100.31, 100.26].map(f64::to_bits)
        );
    }

    #[test]
    fn quantized_drift_reuses_windows_and_shrinks() {
        use crate::tests::generators::{
            DatasetKey, TimestampModel, ValueWorkload, dataset_seed, generate_dataset,
        };
        let key = DatasetKey::new(ValueWorkload::DriftQuantized, TimestampModel::Regular);
        let samples = generate_dataset(key, 1000, dataset_seed(key));
        let mut c = ChimpCompressor::new();
        for s in &samples {
            c.add(*s).unwrap();
        }
        assert!(c.stats.window_reuse > 500, "{}", c.stats.window_reuse);
        assert!(c.bytes().len() < 1_200, "{} bytes", c.bytes().len()); // probe: 960 B
        round_trip(&samples);
    }

    /// Counter + 2^50: consecutive XORs are a few low bits under ~60 leading zeros, which a
    /// leading-zero code topping out at 24 pads by up to 36 bits per value.
    #[test]
    fn shifted_counter_payload_is_bounded() {
        use crate::tests::generators::{
            DatasetKey, TimestampModel, ValueWorkload, dataset_seed, generate_dataset,
        };
        // 5,636 B with the {0, 8, .., 24} table alone; 2,160 B with extended windows.
        let key = DatasetKey::new(ValueWorkload::Counter, TimestampModel::Regular);
        let mut samples = generate_dataset(key, 1000, dataset_seed(key));
        for s in &mut samples {
            s.value += (1u64 << 50) as f64;
        }
        let mut c = ChimpCompressor::new();
        for s in &samples {
            c.add(*s).unwrap();
        }
        assert!(c.bytes().len() < 2_500, "{} bytes", c.bytes().len());
        round_trip(&samples);
    }

    /// The leading-zero counts of the XORs the encoder actually writes, per header, over the
    /// gorilla_report matrix (12 workloads x 3 timestamp models, plus counters offset by
    /// 2^30, 2^40 and 2^50). Input for choosing `LZ_BUCKETS` and the extended counts.
    #[test]
    #[ignore = "diagnostic: cargo test print_leading_zero_histogram -- --ignored --nocapture"]
    fn print_leading_zero_histogram() {
        use super::super::encoder::{LEADING_ZERO_LOG, LZ_BUCKETS, LeadingZeroLog};
        use crate::tests::generators::{
            DatasetKey, TimestampModel, ValueWorkload, dataset_seed, generate_dataset,
        };
        // nlz ranges for the summary columns.
        const RANGES: [(usize, usize); 10] = [
            (0, 7),
            (8, 11),
            (12, 15),
            (16, 19),
            (20, 23),
            (24, 31),
            (32, 39),
            (40, 47),
            (48, 55),
            (56, 63),
        ];
        let mut cases = Vec::new();
        for &w in ValueWorkload::workloads() {
            for &t in TimestampModel::all() {
                let offsets: &[u32] = if w == ValueWorkload::Counter {
                    &[0, 30, 40, 50]
                } else {
                    &[0]
                };
                for &shift in offsets {
                    cases.push((w, t, shift));
                }
            }
        }
        println!("LZ_BUCKETS = {LZ_BUCKETS:?}");
        print!("case,bytes,xors,reuse,trailing,plain,pad_reuse,pad_trailing,pad_plain");
        for (lo, hi) in RANGES {
            print!(",nlz{lo}-{hi}");
        }
        println!();
        let mut totals = [[0u64; 64]; 2];
        for (w, t, shift) in cases {
            let key = DatasetKey::new(w, t);
            let mut samples = generate_dataset(key, 1000, dataset_seed(key));
            if shift > 0 {
                for s in &mut samples {
                    s.value += (1u64 << shift) as f64;
                }
            }
            LEADING_ZERO_LOG.set(Some(Box::new(LeadingZeroLog {
                by_header: [[0; 64]; 3],
                padding_bits: [0; 3],
            })));
            let mut c = ChimpCompressor::new();
            for s in &samples {
                c.add(*s).unwrap();
            }
            let log = LEADING_ZERO_LOG.take().unwrap();
            let per_nlz: Vec<u64> = (0..64)
                .map(|n| log.by_header.iter().map(|h| u64::from(h[n])).sum())
                .collect();
            for (n, count) in per_nlz.iter().enumerate() {
                totals[usize::from(shift > 0)][n] += count;
            }
            let headers = log
                .by_header
                .map(|h| h.iter().map(|&x| u64::from(x)).sum::<u64>());
            let name = if shift > 0 {
                format!("{}/{}/2^{shift}", w.id(), t.id())
            } else {
                format!("{}/{}", w.id(), t.id())
            };
            print!(
                "{name},{},{},{},{},{},{},{},{}",
                c.bytes().len(),
                per_nlz.iter().sum::<u64>(),
                headers[0],
                headers[1],
                headers[2],
                log.padding_bits[0],
                log.padding_bits[1],
                log.padding_bits[2]
            );
            for (lo, hi) in RANGES {
                print!(",{}", per_nlz[lo..=hi].iter().sum::<u64>());
            }
            println!();
        }
        for (label, total) in ["unshifted", "shifted counters"].iter().zip(totals) {
            println!("{label}: nonzero XORs by nlz (nlz:count)");
            let line: Vec<String> = total
                .iter()
                .enumerate()
                .filter(|(_, c)| **c > 0)
                .map(|(n, c)| format!("{n}:{c}"))
                .collect();
            println!("  {}", line.join(" "));
        }
    }

    #[test]
    fn tracks_first_and_last_timestamp() {
        let mut compressor = ChimpCompressor::new();
        for i in 0..10 {
            compressor.add_sample(500 + i * 250, i as f64).unwrap();
        }
        assert_eq!(compressor.first_timestamp(), 500);
        assert_eq!(compressor.last_timestamp(), 500 + 9 * 250);
        assert!(compressor.len_bits() > 0);
    }

    /// FNV-1a over the packed bytes: a stable fingerprint for the golden streams below.
    fn fnv1a(bytes: &[u8]) -> u64 {
        bytes.iter().fold(0xcbf2_9ce4_8422_2325u64, |h, b| {
            (h ^ u64::from(*b)).wrapping_mul(0x0000_0100_0000_01b3)
        })
    }

    fn pack(values: &[f64]) -> Vec<u8> {
        let mut compressor = ChimpCompressor::new();
        for (i, v) in values.iter().enumerate() {
            compressor
                .add_sample(1_700_000_000_000 + i as Timestamp * 1000, *v)
                .unwrap();
        }
        compressor.into_bytes()
    }

    /// Integers and short binary fractions interleaved with decimals, repeats and values the
    /// ELF search does erase, so the bypass sits between `last_alpha` updates.
    fn mixed_dyadic_values() -> Vec<f64> {
        let mut state = 0x2545_f491_4f6c_dd1du64;
        let mut next = move || {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state
        };
        let mut values = vec![
            1.0,
            2.0,
            3.0,
            0.5,
            12.5,
            -7.0,
            123456789.0,
            1e15 + 0.5,
            0.0625,
            4.0,
            4.0,
            100.23,
            7.0,
            0.25,
            17582400172190.2,
            1e18,
            9430104406.3555,
        ];
        for _ in 0..2000 {
            let r = next();
            let n = (r >> 8) % 100_000;
            values.push(match r % 7 {
                0 => n as f64,
                1 => -(n as f64),
                2 => 1e6 + (n % 4096) as f64 / 16.0,
                3 => n as f64 / 100.0,
                4 => (n as f64) / 8.0 + 3.0,
                5 => 1e11 + n as f64 / 1000.0,
                _ => f64::from_bits(r),
            });
        }
        values
    }

    /// Pins the Chimp stream byte for byte as of extended windows (2026-10-29): past the top
    /// leading-zero bucket (24), a `01` header can name an exact count of 32..=54. Chimp's
    /// `10` reuses the stored leading *and* trailing zeros; underneath, the ELF case `11`
    /// carries decimal places. Integers and short binary fractions interleaved with decimals
    /// ELF erases.
    ///
    /// History of `mixed`: 13617 -> 13591 bytes with the alpha header, 13591 -> 13569 with the
    /// previous-alpha hint, 13569 -> 13169 with the full window (`small` 58 -> 57 bytes),
    /// 13169 -> 13100 with extended windows.
    /// The stream is the format: change it on purpose and re-record.
    #[test]
    fn streams_match_the_extended_window_encoder_byte_for_byte() {
        let small = pack(&[
            1.0,
            2.0,
            3.0,
            0.5,
            12.5,
            -7.0,
            1e15 + 0.5,
            0.0625,
            4.0,
            4.0,
            100.23,
        ]);
        assert_eq!(
            small,
            [
                0x80, 0xa0, 0xab, 0xfe, 0xf9, 0x62, 0x8f, 0xfc, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
                0x39, 0xf4, 0x24, 0x18, 0xff, 0xe9, 0x40, 0xd2, 0x0d, 0x7f, 0xea, 0x42, 0x0f, 0xf9,
                0x2a, 0x80, 0x35, 0x58, 0x83, 0x10, 0x6b, 0xf5, 0x26, 0x34, 0x00, 0x04, 0x53, 0xe5,
                0xe3, 0x5f, 0xa9, 0x31, 0xa0, 0x00, 0x22, 0x41, 0x6f, 0xf4, 0x19, 0x25, 0x14, 0x90,
                0xe8
            ]
        );

        let mixed = pack(&mixed_dyadic_values());
        assert_eq!(mixed.len(), 13100);
        assert_eq!(fnv1a(&mixed), 0x4916_c241_acf0_c977);
    }

    /// Pins the timestamp grammar byte for byte (2026-10-29): delta-of-delta tiers of 7, 9, 12,
    /// 16 and 32 bits, each written in one call, and the 64-bit escape. The values are the
    /// `mixed` stream above, so only the timestamps differ from it. That stream's own
    /// timestamps are regular (one 12-bit code, then dod `0`), which is why it did not move
    /// when the 16-bit tier was added. The stream is the format: change it on purpose and
    /// re-record.
    #[test]
    fn irregular_timestamp_stream_matches_the_tiered_encoder_byte_for_byte() {
        let mut state = 0x9e37_79b9_7f4a_7c15u64;
        let mut next = move || {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state
        };
        // Each delta is 1000 plus a jitter spanning 2^scale values (none for scale 0), widths
        // chosen so the delta-of-deltas land in every tier, `0` included.
        const SCALES: [u32; 8] = [0, 5, 8, 10, 14, 30, 40, 5];
        let mut compressor = ChimpCompressor::new();
        let mut timestamp = 1_700_000_000_000i64;
        let mut last_delta = 0i64;
        // Which code each delta-of-delta takes: 0, then the 7/9/12/16/32/64-bit tiers.
        let mut tiers = [0u32; 7];
        for v in mixed_dyadic_values() {
            let r = next();
            let scale = SCALES[(r % 8) as usize];
            let jitter = if scale == 0 {
                0
            } else {
                ((r >> 8) % (1u64 << scale)) as i64 - (1i64 << (scale - 1))
            };
            let delta = 1000 + jitter;
            let width = 64 - zigzag_encode(delta - last_delta).leading_zeros();
            tiers[[0, 7, 9, 12, 16, 32]
                .iter()
                .take_while(|w| width > **w)
                .count()] += 1;
            last_delta = delta;
            timestamp += delta;
            compressor.add_sample(timestamp, v).unwrap();
        }
        assert!(
            tiers.iter().all(|n| *n > 20),
            "every tier is exercised: {tiers:?}"
        );
        let bytes = compressor.into_bytes();
        assert_eq!(bytes.len(), 20559);
        assert_eq!(fnv1a(&bytes), 0xf314_d841_933e_830a);
    }
}
