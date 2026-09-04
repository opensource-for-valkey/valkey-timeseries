use crate::common::encoding::{try_read_uvarint, write_uvarint};
use crate::common::rdb::RdbSerializable;
use crate::series::chunks::stream::traits::BitWrite;
use get_size2::GetSize;
use num_traits::PrimInt;
use std::io;
use valkey_module::digest::Digest;
use valkey_module::{RedisModuleIO, ValkeyError, ValkeyResult, raw};

pub(in crate::series::chunks) const ZERO: bool = false;
pub(in crate::series::chunks) const ONE: bool = true;

/// A stream of bits for writing.
#[derive(Clone, Debug, Default, Hash, PartialEq, Eq, GetSize)]
pub struct BitStream {
    /// The data stream.
    pub(in crate::series::chunks) stream: Vec<u8>,
    /// How many right-most bits are available for writing in the current byte.
    pub(in crate::series::chunks) count: u8,
}

fn validate_rdb_state(bytes: &[u8], count: u64) -> ValkeyResult<u8> {
    if count > 8 {
        return Err(ValkeyError::String(format!("Invalid chunk bits: {count}")));
    }

    // A partial byte can only exist after at least one byte has been
    // written. Without this check, the next append indexes the last byte of
    // an empty stream and panics.
    if count != 0 && bytes.is_empty() {
        return Err(ValkeyError::String(
            "Invalid chunk bits: non-zero count with empty stream".to_owned(),
        ));
    }

    Ok(count as u8)
}

impl BitStream {
    pub fn new() -> Self {
        Self {
            stream: Vec::new(),
            count: 0,
        }
    }

    /// Reset the stream around the provided byte slice.
    pub fn reset(&mut self, stream: Vec<u8>) {
        self.stream = stream;
        self.count = 0;
    }

    /// Hydrate a stream from bytes and bit position.
    pub(crate) fn hydrate(stream: Vec<u8>, count: u8) -> Self {
        Self { stream, count }
    }

    pub(crate) fn serialize(&self, dest: &mut Vec<u8>) {
        write_uvarint(dest, self.count as u64);
        write_uvarint(dest, self.stream.len() as u64);
        dest.extend_from_slice(&self.stream);
    }

    pub(crate) fn deserialize(src: &mut &[u8]) -> io::Result<Self> {
        let count = try_read_uvarint(src)
            .map_err(|_| io::Error::new(io::ErrorKind::UnexpectedEof, "EOF"))?;

        if count > 8 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Invalid bit count",
            ));
        }

        let len = try_read_uvarint(src)
            .map_err(|_| io::Error::new(io::ErrorKind::UnexpectedEof, "EOF"))?
            as usize;

        if src.len() < len {
            return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "EOF"));
        }

        if count != 0 && len == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Invalid chunk bits: non-zero count with empty stream",
            ));
        }

        let stream = src[..len].to_vec();
        *src = &src[len..];

        Ok(Self {
            stream,
            count: count as u8,
        })
    }

    /// Get the underlying bytes.
    pub fn bytes(&self) -> &[u8] {
        &self.stream
    }

    /// Alias used by Gorilla call sites.
    pub fn get_ref(&self) -> &[u8] {
        self.bytes()
    }

    pub fn into_bytes(self) -> Vec<u8> {
        self.stream
    }

    pub fn clear(&mut self) {
        self.stream.clear();
        self.count = 0;
    }

    pub fn len(&self) -> usize {
        self.stream.len()
    }

    pub fn is_empty(&self) -> bool {
        self.stream.is_empty()
    }

    pub fn position(&self) -> u8 {
        // Return the number of "used" bits in the last byte to match the
        if self.count == 0 { 0 } else { 8 - self.count }
    }

    pub fn shrink_to_fit(&mut self) {
        self.stream.shrink_to_fit();
    }

    pub fn debug_digest(&self, dig: &mut Digest) {
        dig.add_string_buffer(&self.stream);
        dig.add_long_long(self.count as i64);
    }

    /// Write a single bit to the stream.
    pub fn write_bit_raw(&mut self, bit: bool) {
        if self.count == 0 {
            self.stream.push(0);
            self.count = 8;
        }

        let i = self.stream.len() - 1;

        if bit {
            self.stream[i] |= 1 << (self.count - 1);
        }

        self.count -= 1;
    }

    /// Write a single bit to the stream.
    pub fn write_bit(&mut self, bit: bool) {
        self.write_bit_raw(bit);
    }

    /// Write a single byte to the stream.
    pub(in crate::series::chunks) fn write_byte_raw(&mut self, byte: u8) {
        if self.count == 0 {
            self.stream.push(byte);
            return;
        }

        let i = self.stream.len() - 1;

        // Complete the last byte with the leftmost count bits from byte.
        self.stream[i] |= byte >> (8 - self.count);

        // Write the remainder.
        self.stream.push(byte << self.count);
    }

    /// Write a single byte to the stream.
    pub fn write_byte(&mut self, byte: u8) {
        self.write_byte_raw(byte);
    }

    /// Write the right-most `bits` bits of `value` in left-to-right order.
    pub fn write_bits(&mut self, bits: u32, value: u64) -> io::Result<()> {
        let mut nbits = bits.min(64) as usize;
        let mut u = value << (64 - nbits);

        while nbits >= 8 {
            let byte = (u >> 56) as u8;
            self.write_byte_raw(byte);
            u <<= 8;
            nbits -= 8;
        }

        while nbits > 0 {
            self.write_bit_raw((u >> 63) == 1);
            u <<= 1;
            nbits -= 1;
        }

        Ok(())
    }

    /// Like write_bits but handles the partial last byte inline for fewer calls.
    pub fn write_bits_fast(&mut self, mut u: u64, mut nbits: usize) {
        nbits = nbits.min(64);
        u <<= 64 - nbits;

        if self.count > 0 {
            let free = self.count as usize;
            let last = self.stream.len() - 1;
            self.stream[last] |= (u >> (64 - free)) as u8;
            if nbits < free {
                self.count = (free - nbits) as u8;
                return;
            }
            u <<= free;
            nbits -= free;
            self.count = 0;
        }

        while nbits >= 8 {
            self.stream.push((u >> 56) as u8);
            u <<= 8;
            nbits -= 8;
        }

        if nbits > 0 {
            self.stream.push((u >> 56) as u8);
            self.count = (8 - nbits) as u8;
        }
    }

    pub(in crate::series::chunks) fn write_unsigned_int(&mut self, mut n: u64) {
        while n >= 0x80 {
            self.write_byte_raw((n as u8) | 0x80);
            n >>= 7;
        }
        self.write_byte_raw(n as u8);
    }

    pub(in crate::series::chunks) fn write_signed_int(&mut self, n: i64) {
        let zigzag = ((n << 1) ^ (n >> 63)) as u64;
        let mut remaining = zigzag;
        while remaining >= 0x80 {
            let byte = ((remaining & 0x7F) | 0x80) as u8;
            self.write_byte_raw(byte);
            remaining >>= 7;
        }
        self.write_byte_raw(remaining as u8);
    }

    pub fn write_u64(&mut self, value: u64) {
        for byte in value.to_be_bytes() {
            self.write_byte_raw(byte);
        }
    }

    pub fn write_f64(&mut self, value: f64) {
        self.write_u64(value.to_bits());
    }

    pub fn write_uvarint(&mut self, mut value: u64) -> io::Result<()> {
        while value >= 0x80 {
            self.write_byte_raw((value as u8) | 0x80);
            value >>= 7;
        }
        self.write_byte_raw(value as u8);
        Ok(())
    }

    pub fn write_varint(&mut self, value: i64) -> io::Result<()> {
        let n: u64 = ((value << 1) ^ (value >> 63)) as u64;
        self.write_uvarint(n)
    }
}

impl BitWrite for BitStream {
    fn write_bit(&mut self, bit: bool) -> io::Result<()> {
        self.write_bit_raw(bit);
        Ok(())
    }

    fn write<U>(&mut self, bits: u32, value: U) -> io::Result<()>
    where
        U: PrimInt,
    {
        self.write_bits(bits, value.to_u64().expect("Invalid u64 cast"))
    }

    fn write_byte(&mut self, byte: u8) {
        self.write_byte_raw(byte);
    }
}

impl RdbSerializable for BitStream {
    fn rdb_save(&self, rdb: *mut RedisModuleIO) {
        raw::save_slice(rdb, self.bytes());
        raw::save_unsigned(rdb, self.count as u64);
    }

    fn rdb_load(rdb: *mut RedisModuleIO) -> ValkeyResult<Self>
    where
        Self: Sized,
    {
        let bytes = raw::load_string_buffer(rdb)?.as_ref().to_vec();
        let count = raw::load_unsigned(rdb)?;
        let count = validate_rdb_state(&bytes, count)?;

        Ok(Self {
            stream: bytes,
            count: count as u8,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::{BitStream, validate_rdb_state};
    use crate::common::encoding::write_uvarint;

    #[test]
    fn rdb_load_rejects_nonzero_count_with_empty_stream() {
        // This state makes the first append index stream[len - 1].
        assert!(validate_rdb_state(&[], 1).is_err());
    }

    #[test]
    fn rdb_load_accepts_byte_aligned_and_partial_streams() {
        assert!(validate_rdb_state(&[], 0).is_ok());
        assert!(validate_rdb_state(&[0], 0).is_ok());
        assert!(validate_rdb_state(&[0], 1).is_ok());
    }

    #[test]
    fn deserialize_rejects_nonzero_count_with_empty_stream() {
        let mut buf = Vec::new();
        write_uvarint(&mut buf, 1); // count
        write_uvarint(&mut buf, 0); // len
        let mut src = buf.as_slice();
        assert!(BitStream::deserialize(&mut src).is_err());
    }

    #[test]
    fn deserialize_rejects_out_of_range_count_without_truncating() {
        let mut buf = Vec::new();
        // 264 truncates to 8 as u8, which would otherwise pass the `> 8` check.
        write_uvarint(&mut buf, 264); // count
        write_uvarint(&mut buf, 0); // len
        let mut src = buf.as_slice();
        assert!(BitStream::deserialize(&mut src).is_err());
    }

    #[test]
    fn deserialize_accepts_byte_aligned_and_partial_streams() {
        let mut buf = Vec::new();
        write_uvarint(&mut buf, 3); // count
        write_uvarint(&mut buf, 2); // len
        buf.extend_from_slice(&[0xAB, 0xCD]);
        let mut src = buf.as_slice();
        let bs = BitStream::deserialize(&mut src).expect("valid partial stream");
        assert_eq!(bs.count, 3);
        assert_eq!(bs.stream, vec![0xAB, 0xCD]);
    }
}
