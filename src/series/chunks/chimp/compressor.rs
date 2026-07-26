//! The ELF erasing layer: `AbstractElfCompressor` / `AbstractElfDecompressor`.
//!
//! This layer sits in front of the Chimp XOR codec. For each value it decides
//! whether the low mantissa bits can be safely erased (and later recovered via
//! `beta_star`), emits a 1- or 6-bit case marker, and hands the (possibly
//! erased) raw bit pattern to Chimp. All markers and Chimp values share one
//! bit stream.
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

use super::chimp_iterator::ChimpIterator;
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
use crate::series::chunks::elf64::{get_10in, get_alpha_and_beta_star, get_f_alpha, get_sp, round_up};
use crate::series::chunks::stream::bitstream::BitStream;
use crate::series::chunks::stream::bitstream_reader::BitStreamReader;
use get_size2::{GetSize, GetSizeTracker};
use std::ffi::c_longlong;
use std::hash::Hash;
use std::io;
use valkey_module::digest::Digest;
use valkey_module::error::Error as ValkeyError;
use valkey_module::raw;

/// Prefix of the timestamp escape case (32- or 64-bit payload).
const TS_ESCAPE_PREFIX: u64 = 0x0F;

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
    last_beta_star: i32,
    count: u64,
    first_timestamp: Timestamp,
    last_timestamp: Timestamp,
    last_delta: i64,
    last_value: f64,
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
        self.last_beta_star.hash(state);
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
            && self.last_beta_star == other.last_beta_star
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
            last_beta_star: i32::MAX,
            count: 0,
            first_timestamp: 0,
            last_timestamp: 0,
            last_delta: 0,
            last_value: 0.0,
        }
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
        self.last_beta_star = i32::MAX;
        self.count = 0;
        self.first_timestamp = 0;
        self.last_timestamp = 0;
        self.last_delta = 0;
        self.last_value = 0.0;
    }

    /// Release the bit stream's unused capacity.
    pub fn shrink_to_fit(&mut self) {
        self.writer.shrink_to_fit();
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
        raw::save_signed(rdb, self.last_beta_star as i64);
        raw::save_signed(rdb, state.stored_lz as i64);
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
        let last_beta_star = raw::load_signed(rdb)? as i32;
        let state = ChimpEncState {
            stored_lz: raw::load_signed(rdb)? as i32,
            stored_val: raw::load_unsigned(rdb)?,
            first: raw::load_unsigned(rdb)? != 0,
        };
        let writer = BitStream::rdb_load(rdb)?;

        Ok(Self {
            writer,
            chimp: ChimpEnc::from_state(state),
            last_beta_star,
            count,
            first_timestamp,
            last_timestamp,
            last_delta,
            last_value,
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
        write_signed_varint(buf, self.last_beta_star as i64);
        write_signed_varint(buf, state.stored_lz as i64);
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
        let last_beta_star = read_signed_varint(&mut buf)? as i32;
        let state = ChimpEncState {
            stored_lz: read_signed_varint(&mut buf)? as i32,
            stored_val: read_uvarint(&mut buf)?,
            first: read_uvarint(&mut buf)? != 0,
        };

        let writer = BitStream::deserialize(&mut buf).map_err(|_| TsdbError::ChunkDecoding)?;

        Ok(Self {
            writer,
            chimp: ChimpEnc::from_state(state),
            last_beta_star,
            count,
            first_timestamp,
            last_timestamp,
            last_delta,
            last_value,
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
        dig.add_long_long(self.last_beta_star.into());
        dig.add_long_long(state.stored_lz.into());
        dig.add_long_long(state.stored_val as c_longlong);
        dig.add_long_long(state.first as c_longlong);
    }

    fn write_value(&mut self, v: f64) -> io::Result<()> {
        let v_long = v.to_bits();

        if v == 0.0 || !v.is_finite() {
            // Zero, the infinities and NaN: Elf case `10`, bit pattern stored
            // verbatim. ELF erasure is defined in terms of decimal
            // significant digits, which none of these have.
            self.writer.write_bits(2, 0b10)?;
            self.chimp.add_value(&mut self.writer, v_long)?;
        } else {
            // Normal or subnormal: attempt ELF erasure.
            match get_alpha_and_beta_star(v, self.last_beta_star) {
                Ok((alpha, beta_star)) => {
                    if alpha < 0 {
                        // Negative alpha can arise from the significant-count
                        // "bug cap" (beta = 17) for large magnitudes; such
                        // values cannot be erased reversibly, so store raw.
                        self.writer.write_bits(2, 0b10)?;
                        self.chimp.add_value(&mut self.writer, v_long)?;
                    } else {
                        let e = ((v_long >> 52) & 0x7ff) as i32;
                        let g_alpha = get_f_alpha(alpha) + e - 1023;
                        let erase_bits = 52 - g_alpha;
                        // Java: `0xffffffffffffffffL << eraseBits` (shift is mod 64).
                        let mask = u64::MAX.wrapping_shl(erase_bits as u32);
                        let delta = (!mask) & v_long;
                        let v_prime_long = mask & v_long;

                        // Erase only when it is provably reversible; otherwise
                        // store the raw value losslessly (Elf case `10`).
                        if delta != 0
                            && erase_bits > 4
                            && try_recover(v_prime_long, beta_star) == Some(v)
                        {
                            if beta_star == self.last_beta_star {
                                self.writer.write_bit(false); // case `0`
                            } else {
                                // case `11` + 4-bit beta_star
                                self.writer.write_bits(6,(beta_star as u64) | 0x30)?;
                                self.last_beta_star = beta_star;
                            }
                            self.chimp.add_value(&mut self.writer, v_prime_long)?;
                        } else {
                            self.writer.write_bits(2, 0b10)?;
                            self.chimp.add_value(&mut self.writer, v_long)?;
                        }
                    }
                }
                Err(()) => {
                    // Unsupported magnitude (the reference would throw):
                    // store raw, losslessly.
                    self.writer.write_bits(2, 0b10)?;
                    self.chimp.add_value(&mut self.writer, v_long)?;
                }
            }
        }
        Ok(())
    }

    fn write_timestamp(&mut self, timestamp: Timestamp) -> io::Result<()> {
        if self.count == 0 {
            self.writer.write_varint(timestamp)?;
            self.first_timestamp = timestamp;
            self.last_timestamp = timestamp;
            self.last_delta = 0;
            return Ok(());
        }
        // Timestamps may go backwards; the delta-of-delta is signed, so an
        // out-of-order point costs bits but still round-trips.
        let new_delta = timestamp.wrapping_sub(self.last_timestamp);
        let delta_d = new_delta.wrapping_sub(self.last_delta);

        if delta_d == 0 {
            self.writer.write_bit(false);
        } else {
            let mut enc = zigzag_encode(delta_d);
            let mut length = 64 - enc.leading_zeros();

            if length == 0 {
                length = 1;
            }

            if length <= 7 {
                enc |= 0x02u64 << 7;
                self.writer.write_bits(9, enc)?;
            } else if length <= 9 {
                enc |= 0x06u64 << 9;
                self.writer.write_bits(12, enc)?;
            } else if length <= 12 {
                enc |= 0x0Eu64 << 12;
                self.writer.write_bits(16, enc)?;
            } else {
                self.writer.write_bits(4, TS_ESCAPE_PREFIX)?;
                if length <= 32 {
                    self.writer.write_bit(false);
                    self.writer.write_bits(32, enc)?;
                } else {
                    self.writer.write_bit(true);
                    self.writer.write_bits(64, enc)?;
                }
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
    last_beta_star: i32,
    count: u64,
    sample_count: u64,
    last_timestamp: Timestamp,
    last_delta: i64,
}

impl<'a> ChimpDecompressor<'a> {
    pub fn new(data: &'a [u8], sample_count: u64) -> Self {
        Self {
            reader: BitStreamReader::new(data),
            chimp: ChimpDec::new(),
            last_beta_star: i32::MAX,
            count: 0,
            sample_count,
            last_timestamp: 0,
            last_delta: 0,
        }
    }

    /// Read the next sample. Returns `Ok(None)` once all `sample_count`
    /// samples have been read.
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
    fn read_timestamp(&mut self) -> io::Result<Timestamp> {
        if self.count == 0 {
            let timestamp = self.reader.read_varint()?;
            self.last_timestamp = timestamp;
            self.last_delta = 0;
            return Ok(timestamp);
        }

        // Read up to 4 bits, stopping at the first zero.
        let mut prefix = 0u64;
        for _ in 0..4 {
            let bit = self.reader.read_bit()?;
            prefix = (prefix << 1) | bit as u64;
            if !bit {
                break;
            }
        }

        let delta_d = match prefix {
            0x00 => 0,
            0x02 => zigzag_decode(self.reader.read_bits(7)?),
            0x06 => zigzag_decode(self.reader.read_bits(9)?),
            0x0E => zigzag_decode(self.reader.read_bits(12)?),
            TS_ESCAPE_PREFIX => {
                if self.reader.read_bit()? {
                    zigzag_decode(self.reader.read_bits(64)?)
                } else {
                    zigzag_decode(self.reader.read_bits(32)?)
                }
            }
            _ => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "Invalid timestamp prefix",
                ));
            }
        };

        self.last_delta = self.last_delta.wrapping_add(delta_d);
        self.last_timestamp = self.last_timestamp.wrapping_add(self.last_delta);
        Ok(self.last_timestamp)
    }

    fn read_value(&mut self) -> io::Result<f64> {
        if !self.reader.read_bit()? {
            // case `0`: reuse last beta_star
            self.recover_v_by_beta_star()
        } else if !self.reader.read_bit()? {
            // case `10`: raw value (zero, infinities, NaN, or anything ELF
            // could not erase reversibly)
            let bits = self.chimp.read_value(&mut self.reader)?;
            Ok(f64::from_bits(bits))
        } else {
            // case `11`: new 4-bit beta_star
            self.last_beta_star = self.reader.read_bits(4)? as i32;
            self.recover_v_by_beta_star()
        }
    }

    fn recover_v_by_beta_star(&mut self) -> io::Result<f64> {
        let v_prime_bits = self.chimp.read_value(&mut self.reader)?;
        recover(v_prime_bits, self.last_beta_star)
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

/// Exact mirror of `AbstractElfDecompressor.recoverVByBetaStar`, but returns
/// `Err` instead of throwing for unsupported recovery cases.
fn recover(v_prime_bits: u64, beta_star: i32) -> io::Result<f64> {
    let v_prime = f64::from_bits(v_prime_bits);
    let sp = get_sp(v_prime.abs());
    let v = if beta_star == 0 {
        let idx = -(sp as i64) - 1;
        if idx < 0 {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "Invalid recovery index"));
        }
        let mut r = get_10in(idx as i32).map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "Invalid recovery value"))?;
        if v_prime < 0.0 {
            r = -r;
        }
        r
    } else {
        let alpha = (beta_star as i64) - (sp as i64) - 1;
        if alpha < 0 {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "Invalid recovery alpha"));
        }
        round_up(v_prime, alpha as i32).map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "Invalid recovery value"))?
    };
    Ok(v)
}

/// Encode-side recoverability check: returns the value the decoder would
/// reconstruct from an erased `v_prime` and `beta_star`, or `None` if the
/// recovery failed (so the caller stores the raw value instead).
fn try_recover(v_prime_long: u64, beta_star: i32) -> Option<f64> {
    recover(v_prime_long, beta_star).ok()
}

fn read_uvarint(buf: &mut &[u8]) -> TsdbResult<u64> {
    try_read_uvarint(buf).map_err(|_| TsdbError::ChunkDecoding)
}

fn read_signed_varint(buf: &mut &[u8]) -> TsdbResult<i64> {
    try_read_signed_varint(buf).map_err(|_| TsdbError::ChunkDecoding)
}

#[cfg(test)]
mod tests {
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
    fn tracks_first_and_last_timestamp() {
        let mut compressor = ChimpCompressor::new();
        for i in 0..10 {
            compressor.add_sample(500 + i * 250, i as f64).unwrap();
        }
        assert_eq!(compressor.first_timestamp(), 500);
        assert_eq!(compressor.last_timestamp(), 500 + 9 * 250);
        assert!(compressor.len_bits() > 0);
    }
}
