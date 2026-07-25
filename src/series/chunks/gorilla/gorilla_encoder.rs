use super::GorillaIterator;
use super::varbit_xor::write_varbit_xor;
use crate::common::Sample;
use crate::common::encoding::{
    try_read_f64_le, try_read_signed_varint as read_varint, try_read_uvarint, write_f64_le,
    write_uvarint,
};
use crate::common::hash::hash_f64;
use crate::common::logging::log_warning;
use crate::common::rdb::{
    RdbSerializable, rdb_load_timestamp, rdb_load_u8, rdb_load_usize, rdb_save_timestamp,
    rdb_save_u8, rdb_save_usize,
};
use crate::error::{TsdbError, TsdbResult};
use crate::series::chunks::stream::bitstream::BitStream;
use crate::series::chunks::stream::varbit::write_varbit;
use get_size2::{GetSize, GetSizeTracker};
use std::ffi::c_longlong;
use std::hash::Hash;
use valkey_module::digest::Digest;
use valkey_module::error::Error as ValkeyError;
use valkey_module::raw;

#[derive(Debug, Clone)]
pub struct GorillaEncoder {
    writer: BitStream,
    leading_bits: u8,
    trailing_bits: u8,
    timestamp_delta: i64,
    pub num_samples: usize,
    pub first_ts: i64,
    pub last_ts: i64,
    pub last_value: f64,
}

impl GetSize for GorillaEncoder {
    // `get_heap_size_with_tracker` is the extension point a containing type's
    // derived impl calls; overriding only `get_size` would report zero heap
    // whenever this encoder is nested inside another `GetSize` type.
    fn get_heap_size_with_tracker<T: GetSizeTracker>(&self, tracker: T) -> (usize, T) {
        // `writer` owns the only allocation; every other field is a scalar.
        self.writer.get_heap_size_with_tracker(tracker)
    }
}

impl Hash for GorillaEncoder {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.writer.hash(state);
        self.num_samples.hash(state);
        self.first_ts.hash(state);
        self.leading_bits.hash(state);
        self.trailing_bits.hash(state);
        self.timestamp_delta.hash(state);
        self.last_ts.hash(state);
        hash_f64(self.num_samples as f64, state);
    }
}

impl Default for GorillaEncoder {
    fn default() -> Self {
        Self::new()
    }
}

impl GorillaEncoder {
    pub fn new() -> GorillaEncoder {
        let writer = BitStream::new();

        GorillaEncoder {
            writer,
            num_samples: 0,
            last_ts: 0,
            last_value: 0.0,
            leading_bits: 0,
            trailing_bits: 0,
            timestamp_delta: 0,
            first_ts: 0,
        }
    }

    pub fn clear(&mut self) {
        self.writer.clear();
        self.num_samples = 0;
        self.last_ts = 0;
        self.last_value = 0.0;
        self.leading_bits = 0;
        self.trailing_bits = 0;
        self.timestamp_delta = 0;
        self.first_ts = 0;
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
        // Classic Float64 for the value
        self.writer.write_f64(sample.value);
        self.first_ts = sample.timestamp;
        self.last_ts = sample.timestamp;
        self.last_value = sample.value;
        self.num_samples += 1;
        Ok(())
    }

    fn write_second_sample(&mut self, sample: &Sample) -> std::io::Result<()> {
        let timestamp = sample.timestamp;
        let value = sample.value;

        let timestamp_delta = timestamp - self.last_ts;
        if timestamp_delta < 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "samples aren't sorted by timestamp ascending",
            ));
        }

        self.writer.write_uvarint(timestamp_delta as u64)?;

        let (leading, trailing) =
            write_varbit_xor(value, self.last_value, 0xff, 0, &mut self.writer)?;

        self.last_ts = timestamp;
        self.last_value = value;
        self.leading_bits = leading;
        self.trailing_bits = trailing;
        self.timestamp_delta = timestamp_delta;

        self.num_samples += 1;

        Ok(())
    }

    fn write_nth_sample(&mut self, sample: &Sample) -> std::io::Result<()> {
        let timestamp = sample.timestamp;
        let value = sample.value;

        let timestamp_delta = timestamp - self.last_ts;
        if timestamp_delta < 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "samples aren't sorted by timestamp ascending",
            ));
        }
        let delta_of_delta = timestamp_delta - self.timestamp_delta;

        write_varbit(&mut self.writer, delta_of_delta)?;

        let (leading_bits, trailing_bits) = write_varbit_xor(
            value,
            self.last_value,
            self.leading_bits,
            self.trailing_bits,
            &mut self.writer,
        )?;

        self.last_ts = timestamp;
        self.last_value = value;
        self.leading_bits = leading_bits;
        self.trailing_bits = trailing_bits;
        self.timestamp_delta = timestamp_delta;

        self.num_samples += 1;

        Ok(())
    }

    pub fn iter(&'_ self) -> GorillaIterator<'_> {
        GorillaIterator::new(self)
    }

    pub(crate) fn buf(&self) -> &[u8] {
        self.writer.get_ref()
    }

    pub(crate) fn shrink_to_fit(&mut self) {
        self.writer.shrink_to_fit()
    }

    pub fn rdb_save(&self, rdb: *mut raw::RedisModuleIO) {
        rdb_save_usize(rdb, self.num_samples);
        rdb_save_timestamp(rdb, self.first_ts);
        rdb_save_timestamp(rdb, self.last_ts);
        raw::save_double(rdb, self.last_value);
        raw::save_signed(rdb, self.timestamp_delta);
        rdb_save_u8(rdb, self.leading_bits);
        rdb_save_u8(rdb, self.trailing_bits);
        self.writer.rdb_save(rdb);
    }

    pub fn rdb_load(rdb: *mut raw::RedisModuleIO) -> Result<GorillaEncoder, ValkeyError> {
        let num_samples = rdb_load_usize(rdb)?;
        let first_ts = rdb_load_timestamp(rdb)?;
        let last_ts = rdb_load_timestamp(rdb)?;
        let value = raw::load_double(rdb)?;
        let timestamp_delta = raw::load_signed(rdb)?;
        let leading_bits = rdb_load_u8(rdb)?;
        let trailing_bits = rdb_load_u8(rdb)?;
        let writer = BitStream::rdb_load(rdb)?;

        Ok(GorillaEncoder {
            writer,
            num_samples,
            first_ts,
            last_ts,
            last_value: value,
            leading_bits,
            trailing_bits,
            timestamp_delta,
        })
    }

    pub fn serialize(&self, buf: &mut Vec<u8>) {
        // Serialize scalar fields
        write_uvarint(buf, self.num_samples as u64);
        write_uvarint(buf, self.first_ts as u64);
        write_uvarint(buf, self.last_ts as u64);
        write_f64_le(buf, self.last_value);

        // delta is verified to be > 0 when adding samples
        write_uvarint(buf, self.timestamp_delta as u64);

        buf.push(self.leading_bits);
        buf.push(self.trailing_bits);

        self.writer.serialize(buf);
    }

    pub fn deserialize(buf: &[u8]) -> TsdbResult<GorillaEncoder> {
        let mut buf = buf;

        // Deserialize scalar fields
        let num_samples = read_usize(&mut buf)?;
        let first_ts = read_timestamp(&mut buf)?;
        let last_ts = read_timestamp(&mut buf)?;
        let last_value = try_read_f64_le(&mut buf).map_err(|_| TsdbError::ChunkDecoding)?;
        let timestamp_delta = read_unsigned_varint(&mut buf)? as i64; // yes, this is intended

        if buf.len() < 2 {
            log_warning("gorilla: buffer too short for leading_bits");
            return Err(TsdbError::ChunkDecoding);
        }
        let leading_bits = buf[0];
        let trailing_bits = buf[1];

        buf = &buf[2..];

        // Deserialize buffered writer data
        let writer = BitStream::deserialize(&mut buf).map_err(|_| TsdbError::ChunkDecoding)?;

        Ok(GorillaEncoder {
            writer,
            leading_bits,
            trailing_bits,
            timestamp_delta,
            num_samples,
            first_ts,
            last_ts,
            last_value,
        })
    }

    pub fn debug_digest(&self, dig: &mut Digest) {
        self.writer.debug_digest(dig);
        dig.add_long_long(self.num_samples as c_longlong);
        dig.add_long_long(self.first_ts);
        dig.add_long_long(self.last_ts);
        dig.add_long_long(self.leading_bits.into());
        dig.add_long_long(self.trailing_bits.into());
        dig.add_long_long(self.timestamp_delta);
        let bits = self.last_value.to_bits();
        dig.add_long_long(bits as c_longlong);
    }
}

impl PartialEq<Self> for GorillaEncoder {
    fn eq(&self, other: &Self) -> bool {
        if self.num_samples != other.num_samples {
            return false;
        }
        if self.last_ts != other.last_ts {
            return false;
        }
        if self.last_value != other.last_value {
            return false;
        }
        if self.leading_bits != other.leading_bits {
            return false;
        }
        if self.trailing_bits != other.trailing_bits {
            return false;
        }
        if self.timestamp_delta != other.timestamp_delta {
            return false;
        }
        self.writer == other.writer
    }
}

fn read_signed_varint(buf: &mut &[u8]) -> TsdbResult<i64> {
    read_varint(buf).map_err(|_| TsdbError::ChunkDecoding)
}
fn read_unsigned_varint(buf: &mut &[u8]) -> TsdbResult<u64> {
    try_read_uvarint(buf).map_err(|_| TsdbError::ChunkDecoding)
}

fn read_timestamp(buf: &mut &[u8]) -> TsdbResult<i64> {
    read_unsigned_varint(buf).map(|delta| delta as i64)
}

fn read_usize(buf: &mut &[u8]) -> TsdbResult<usize> {
    read_unsigned_varint(buf).map(|v| v as usize)
}

impl Eq for GorillaEncoder {}

#[cfg(test)]
mod tests {}
