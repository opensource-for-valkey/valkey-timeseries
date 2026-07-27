use crate::common::logging::log_warning;
use crate::series::chunks::stream::traits::BitRead;
use std::io;

const MAX_VARINT_LEN64: usize = 10;

/// BitStreamReader reads bits from a byte stream.
#[derive(Debug, Clone)]
pub struct BitStreamReader<'a> {
    stream: &'a [u8],
    stream_offset: usize,

    pub(crate) buffer: u64,
    pub(crate) valid: u8,
    last: u8,
}

impl<'a> BitStreamReader<'a> {
    /// Creates a new BitStreamReader from a byte slice.
    pub fn new(b: &'a [u8]) -> Self {
        let last = if b.is_empty() { 0 } else { b[b.len() - 1] };

        Self {
            stream: b,
            stream_offset: 0,
            buffer: 0,
            valid: 0,
            last,
        }
    }

    /// Reads a single bit and returns it as a boolean.
    pub fn read_bit(&mut self) -> io::Result<bool> {
        if self.valid == 0 && !self.load_next_buffer(1) {
            return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "EOF"));
        }
        self.read_bit_fast()
    }

    /// Fast path for reading a bit.
    pub(super) fn read_bit_fast(&mut self) -> io::Result<bool> {
        if self.valid == 0 {
            return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "EOF"));
        }

        self.valid -= 1;
        let bitmask = 1u64 << self.valid;
        Ok((self.buffer & bitmask) != 0)
    }

    /// Reads the specified number of bits and returns them as a u64.
    pub fn read_bits(&mut self, nbits: u8) -> io::Result<u64> {
        if nbits > 64 {
            log_warning(format!("bitstream read_bits overflow request: {nbits}"));
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "bit count exceeds 64",
            ));
        }

        if self.valid == 0 && !self.load_next_buffer(nbits) {
            return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "EOF"));
        }

        if nbits <= self.valid {
            return self.read_bits_fast(nbits);
        }

        // Read remaining valid bits from current buffer and part from next one.
        let bitmask = (1u64 << self.valid) - 1;
        let nbits_remaining = nbits - self.valid;
        let mut v = (self.buffer & bitmask) << nbits_remaining;
        self.valid = 0;

        if !self.load_next_buffer(nbits_remaining) {
            return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "EOF"));
        }

        if self.valid < nbits_remaining {
            return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "EOF"));
        }

        let bitmask = (1u64 << nbits_remaining) - 1;
        v |= (self.buffer >> (self.valid - nbits_remaining)) & bitmask;
        self.valid -= nbits_remaining;

        Ok(v)
    }

    /// Fast path for reading bits.
    pub(super) fn read_bits_fast(&mut self, nbits: u8) -> io::Result<u64> {
        if nbits > self.valid {
            return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "EOF"));
        }

        // Handle the 64-bit special case without performing a 1 << 64 shift
        // which would overflow. When nbits == 64 we want the full buffer.
        self.valid -= nbits;
        if nbits == 64 {
            return Ok(self.buffer);
        }

        let bitmask = (1u64 << nbits) - 1;
        Ok((self.buffer >> self.valid) & bitmask)
    }

    /// Reads a single byte (8 bits).
    pub fn read_byte(&mut self) -> io::Result<u8> {
        let v = self.read_bits(8)?;
        Ok(v as u8)
    }

    pub fn read_u64(&mut self) -> io::Result<u64> {
        let mut bytes = [0u8; 8];
        for b in &mut bytes {
            *b = self.read_byte()?;
        }
        Ok(u64::from_be_bytes(bytes))
    }

    pub fn read_f64(&mut self) -> io::Result<f64> {
        Ok(f64::from_bits(self.read_u64()?))
    }

    /// Reads a varint-encoded u64.
    pub fn read_uvarint(&mut self) -> io::Result<u64> {
        let mut x: u64 = 0;
        let mut s: u32 = 0;

        for _ in 0..MAX_VARINT_LEN64 {
            let byte = self.read_byte()?;

            if byte < 0x80 {
                return Ok(x | ((byte as u64) << s));
            }

            x |= (byte as u64 & 0x7f) << s;
            s += 7;
        }

        log_warning("bitstream varint overflow");
        Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "varint overflow",
        ))
    }

    /// Reads a varint-encoded i64.
    pub fn read_varint(&mut self) -> io::Result<i64> {
        let ux = self.read_uvarint()?;
        let mut x = (ux >> 1) as i64;

        if (ux & 1) != 0 {
            x = !x;
        }

        Ok(x)
    }

    /// Loads the next bytes from the stream into the internal buffer.
    fn load_next_buffer(&mut self, nbits: u8) -> bool {
        if self.stream_offset >= self.stream.len() {
            return false;
        }

        if self.stream_offset + 8 < self.stream.len() {
            let mut bytes = [0u8; 8];
            bytes.copy_from_slice(&self.stream[self.stream_offset..self.stream_offset + 8]);
            self.buffer = u64::from_be_bytes(bytes);
            self.stream_offset += 8;
            self.valid = 64;
            return true;
        }

        let mut nbytes = ((nbits / 8) + 1) as usize;
        if self.stream_offset + nbytes > self.stream.len() {
            nbytes = self.stream.len() - self.stream_offset;
        }

        let mut buffer: u64 = 0;
        let mut skip = 0;

        if self.stream_offset + nbytes == self.stream.len() {
            // Use cached last byte to handle concurrent writes.
            buffer |= self.last as u64;
            skip = 1;
        }

        for i in 0..(nbytes - skip) {
            let byte = self.stream[self.stream_offset + i] as u64;
            buffer |= byte << (8 * (nbytes - i - 1));
        }

        self.buffer = buffer;
        self.stream_offset += nbytes;
        self.valid = (nbytes * 8) as u8;

        true
    }
}

impl BitRead for BitStreamReader<'_> {
    fn read_bit(&mut self) -> io::Result<bool> {
        BitStreamReader::read_bit(self)
    }

    fn read_byte(&mut self) -> io::Result<u8> {
        BitStreamReader::read_byte(self)
    }

    fn read_bits(&mut self, num: u32) -> io::Result<u64> {
        if num > 64 {
            log_warning(format!("bitstream trait read_bits overflow request: {num}"));
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "bit count exceeds 64",
            ));
        }
        BitStreamReader::read_bits(self, num as u8)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_read_single_bit() {
        let data = vec![0b10000000u8];
        let mut reader = BitStreamReader::new(&data);

        assert!(reader.read_bit().unwrap());
        assert!(!reader.read_bit().unwrap());
    }

    #[test]
    fn test_read_byte() {
        let data = vec![0xA5u8];
        let mut reader = BitStreamReader::new(&data);

        assert_eq!(reader.read_byte().unwrap(), 0xA5);
    }

    #[test]
    fn test_read_bits() {
        let data = vec![0b11110000u8];
        let mut reader = BitStreamReader::new(&data);

        let bits = reader.read_bits(4).unwrap();
        assert_eq!(bits, 0xF);
    }

    #[test]
    fn test_read_uvarint() {
        let data = vec![0xACu8, 0x02u8];
        let mut reader = BitStreamReader::new(&data);

        let val = reader.read_uvarint().unwrap();
        assert_eq!(val, 300);
    }
}
