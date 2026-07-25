//! Sample-level DeXOR encoder.
//!
//! Mirrors [`GorillaEncoder`](crate::series::chunks::GorillaEncoder): timestamps
//! and values are interleaved in a single bit stream, one record per sample.
//! Timestamps use the same delta-of-delta varbit scheme as Gorilla; only the
//! value coder differs.
//!
//! | sample | timestamp                        | value              |
//! |--------|----------------------------------|--------------------|
//! | first  | zig-zag varint, absolute         | DeXOR              |
//! | second | uvarint delta                    | DeXOR              |
//! | nth    | varbit delta-of-delta            | DeXOR              |
//!
//! Unlike Gorilla, the first value is *not* stored as a raw `f64`: the DeXOR
//! value coder starts from a predictor of `0.0` on both sides, so the first
//! sample codes like any other and a leading integer costs a handful of bits
//! rather than 64.
//!
//! The chunk fixes the value coder to [`DeXorMode::Native`] with the default
//! `rho`. The `Skippable` and `Buffered` modes remain available through
//! [`DeXorValueEncoder`] but are not persisted, since selecting them would need
//! a per-series knob and extra state in every chunk header.
//!
//! [`DeXorMode::Native`]: super::DeXorMode::Native

use super::dexor_iterator::DeXorIterator;
use super::{CodecSnapshot, DeXorConfig, DeXorValueEncoder};
use crate::common::Sample;
use crate::common::encoding::{try_read_f64_le, try_read_uvarint, write_f64_le, write_uvarint};
use crate::common::hash::hash_f64;
use crate::common::logging::log_warning;
use crate::common::rdb::{
    RdbSerializable, rdb_load_timestamp, rdb_load_usize, rdb_save_timestamp, rdb_save_usize,
};
use crate::error::{TsdbError, TsdbResult};
use crate::series::chunks::stream::bitstream::BitStream;
use crate::series::chunks::stream::varbit::write_varbit;
use get_size2::GetSize;
use std::ffi::c_longlong;
use std::hash::Hash;
use std::mem::size_of_val;
use valkey_module::digest::Digest;
use valkey_module::error::Error as ValkeyError;
use valkey_module::raw;

/// Configuration used by every persisted DeXOR chunk.
///
/// Fixed rather than per-series so a chunk header does not have to carry it and
/// so the format stays stable across releases.
pub(super) const CHUNK_CODEC_CONFIG: DeXorConfig = DeXorConfig {
    mode: super::DeXorMode::Native,
    rho: 8,
};

#[derive(Debug, Clone)]
pub struct DeXorEncoder {
    writer: BitStream,
    values: DeXorValueEncoder,
    timestamp_delta: i64,
    pub num_samples: usize,
    pub first_ts: i64,
    pub last_ts: i64,
    pub last_value: f64,
}

impl GetSize for DeXorEncoder {
    fn get_size(&self) -> usize {
        self.writer.get_size()
            + self.values.get_size()
            + size_of_val(&self.num_samples)
            + size_of_val(&self.first_ts)
            + size_of_val(&self.last_ts)
            + size_of_val(&self.last_value)
            + size_of_val(&self.timestamp_delta)
    }
}

impl Hash for DeXorEncoder {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.writer.hash(state);
        self.values.hash(state);
        self.num_samples.hash(state);
        self.first_ts.hash(state);
        self.last_ts.hash(state);
        self.timestamp_delta.hash(state);
        hash_f64(self.last_value, state);
    }
}

impl Default for DeXorEncoder {
    fn default() -> Self {
        Self::new()
    }
}

impl DeXorEncoder {
    pub fn new() -> DeXorEncoder {
        DeXorEncoder {
            writer: BitStream::new(),
            values: DeXorValueEncoder::new(CHUNK_CODEC_CONFIG),
            timestamp_delta: 0,
            num_samples: 0,
            first_ts: 0,
            last_ts: 0,
            last_value: 0.0,
        }
    }

    pub fn clear(&mut self) {
        self.writer.clear();
        self.values.reset();
        self.timestamp_delta = 0;
        self.num_samples = 0;
        self.first_ts = 0;
        self.last_ts = 0;
        self.last_value = 0.0;
    }

    pub fn add_sample(&mut self, sample: &Sample) -> std::io::Result<()> {
        match self.num_samples {
            0 => self.write_first_sample(sample),
            1 => self.write_second_sample(sample),
            _ => self.write_nth_sample(sample),
        }
    }

    fn write_first_sample(&mut self, sample: &Sample) -> std::io::Result<()> {
        self.writer.write_varint(sample.timestamp)?;
        self.values.encode(&mut self.writer, sample.value);

        self.first_ts = sample.timestamp;
        self.last_ts = sample.timestamp;
        self.last_value = sample.value;
        self.num_samples += 1;
        Ok(())
    }

    fn write_second_sample(&mut self, sample: &Sample) -> std::io::Result<()> {
        let timestamp_delta = self.timestamp_delta(sample.timestamp)?;
        self.writer.write_uvarint(timestamp_delta as u64)?;
        self.values.encode(&mut self.writer, sample.value);

        self.last_ts = sample.timestamp;
        self.last_value = sample.value;
        self.timestamp_delta = timestamp_delta;
        self.num_samples += 1;
        Ok(())
    }

    fn write_nth_sample(&mut self, sample: &Sample) -> std::io::Result<()> {
        let timestamp_delta = self.timestamp_delta(sample.timestamp)?;
        let delta_of_delta = timestamp_delta - self.timestamp_delta;

        write_varbit(&mut self.writer, delta_of_delta)?;
        self.values.encode(&mut self.writer, sample.value);

        self.last_ts = sample.timestamp;
        self.last_value = sample.value;
        self.timestamp_delta = timestamp_delta;
        self.num_samples += 1;
        Ok(())
    }

    fn timestamp_delta(&self, timestamp: i64) -> std::io::Result<i64> {
        let delta = timestamp - self.last_ts;
        if delta < 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "samples aren't sorted by timestamp ascending",
            ));
        }
        Ok(delta)
    }

    pub fn iter(&'_ self) -> DeXorIterator<'_> {
        DeXorIterator::new(self)
    }

    pub(crate) fn buf(&self) -> &[u8] {
        self.writer.get_ref()
    }

    pub(crate) fn shrink_to_fit(&mut self) {
        self.writer.shrink_to_fit()
    }

    pub fn rdb_save(&self, rdb: *mut raw::RedisModuleIO) {
        let snapshot = self.values.snapshot();
        rdb_save_usize(rdb, self.num_samples);
        rdb_save_timestamp(rdb, self.first_ts);
        rdb_save_timestamp(rdb, self.last_ts);
        raw::save_double(rdb, self.last_value);
        raw::save_signed(rdb, self.timestamp_delta);
        raw::save_unsigned(rdb, snapshot.previous_value_bits);
        raw::save_signed(rdb, snapshot.previous_q as i64);
        raw::save_unsigned(rdb, snapshot.previous_delta as u64);
        raw::save_unsigned(rdb, snapshot.previous_exp);
        raw::save_unsigned(rdb, snapshot.exponent_len as u64);
        raw::save_unsigned(rdb, snapshot.contract_step as u64);
        self.writer.rdb_save(rdb);
    }

    pub fn rdb_load(rdb: *mut raw::RedisModuleIO) -> Result<DeXorEncoder, ValkeyError> {
        let num_samples = rdb_load_usize(rdb)?;
        let first_ts = rdb_load_timestamp(rdb)?;
        let last_ts = rdb_load_timestamp(rdb)?;
        let last_value = raw::load_double(rdb)?;
        let timestamp_delta = raw::load_signed(rdb)?;
        let snapshot = CodecSnapshot {
            previous_value_bits: raw::load_unsigned(rdb)?,
            previous_q: raw::load_signed(rdb)? as i32,
            previous_delta: raw::load_unsigned(rdb)? as u32,
            previous_exp: raw::load_unsigned(rdb)?,
            exponent_len: raw::load_unsigned(rdb)? as u32,
            contract_step: raw::load_unsigned(rdb)? as u32,
        };
        let writer = BitStream::rdb_load(rdb)?;

        let values = DeXorValueEncoder::restore(CHUNK_CODEC_CONFIG, snapshot)
            .ok_or_else(|| ValkeyError::generic("dexor: invalid codec state in RDB"))?;

        Ok(DeXorEncoder {
            writer,
            values,
            timestamp_delta,
            num_samples,
            first_ts,
            last_ts,
            last_value,
        })
    }

    pub fn serialize(&self, buf: &mut Vec<u8>) {
        let snapshot = self.values.snapshot();

        write_uvarint(buf, self.num_samples as u64);
        write_uvarint(buf, self.first_ts as u64);
        write_uvarint(buf, self.last_ts as u64);
        write_f64_le(buf, self.last_value);

        // Verified non-negative when adding samples.
        write_uvarint(buf, self.timestamp_delta as u64);

        write_uvarint(buf, snapshot.previous_value_bits);
        // Biased so the varint stays small for the common negative exponents.
        write_uvarint(buf, (snapshot.previous_q - super::Q_MIN) as u64);
        write_uvarint(buf, snapshot.previous_delta as u64);
        write_uvarint(buf, snapshot.previous_exp);
        write_uvarint(buf, snapshot.exponent_len as u64);
        write_uvarint(buf, snapshot.contract_step as u64);

        self.writer.serialize(buf);
    }

    pub fn deserialize(buf: &[u8]) -> TsdbResult<DeXorEncoder> {
        let mut buf = buf;

        let num_samples = read_uvarint(&mut buf)? as usize;
        let first_ts = read_uvarint(&mut buf)? as i64;
        let last_ts = read_uvarint(&mut buf)? as i64;
        let last_value = try_read_f64_le(&mut buf).map_err(|_| TsdbError::ChunkDecoding)?;
        let timestamp_delta = read_uvarint(&mut buf)? as i64; // yes, this is intended

        let snapshot = CodecSnapshot {
            previous_value_bits: read_uvarint(&mut buf)?,
            previous_q: read_uvarint(&mut buf)? as i32 + super::Q_MIN,
            previous_delta: read_uvarint(&mut buf)? as u32,
            previous_exp: read_uvarint(&mut buf)?,
            exponent_len: read_uvarint(&mut buf)? as u32,
            contract_step: read_uvarint(&mut buf)? as u32,
        };

        let writer = BitStream::deserialize(&mut buf).map_err(|_| TsdbError::ChunkDecoding)?;

        let values = DeXorValueEncoder::restore(CHUNK_CODEC_CONFIG, snapshot).ok_or_else(|| {
            log_warning("dexor: invalid codec state in serialized chunk");
            TsdbError::ChunkDecoding
        })?;

        Ok(DeXorEncoder {
            writer,
            values,
            timestamp_delta,
            num_samples,
            first_ts,
            last_ts,
            last_value,
        })
    }

    pub fn debug_digest(&self, dig: &mut Digest) {
        let snapshot = self.values.snapshot();
        self.writer.debug_digest(dig);
        dig.add_long_long(self.num_samples as c_longlong);
        dig.add_long_long(self.first_ts);
        dig.add_long_long(self.last_ts);
        dig.add_long_long(self.timestamp_delta);
        dig.add_long_long(self.last_value.to_bits() as c_longlong);
        dig.add_long_long(snapshot.previous_value_bits as c_longlong);
        dig.add_long_long(snapshot.previous_q.into());
        dig.add_long_long(snapshot.previous_delta.into());
        dig.add_long_long(snapshot.previous_exp as c_longlong);
        dig.add_long_long(snapshot.exponent_len.into());
        dig.add_long_long(snapshot.contract_step.into());
    }
}

impl PartialEq<Self> for DeXorEncoder {
    fn eq(&self, other: &Self) -> bool {
        self.num_samples == other.num_samples
            && self.last_ts == other.last_ts
            && self.last_value == other.last_value
            && self.timestamp_delta == other.timestamp_delta
            && self.values == other.values
            && self.writer == other.writer
    }
}

impl Eq for DeXorEncoder {}

fn read_uvarint(buf: &mut &[u8]) -> TsdbResult<u64> {
    try_read_uvarint(buf).map_err(|_| TsdbError::ChunkDecoding)
}
