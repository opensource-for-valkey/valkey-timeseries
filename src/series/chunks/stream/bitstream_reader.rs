use crate::common::logging::log_warning;
use crate::series::chunks::stream::traits::BitRead;
use std::io;

const MAX_VARINT_LEN64: usize = 10;

/// BitStreamReader reads bits from a byte stream.
///
/// The bits not yet consumed sit in the low `valid` bits of `buffer`, most
/// significant first, so the next bit is bit `valid - 1`. `refill` tops the
/// window up to at least 57 bits whenever the stream has bytes left, which
/// makes every read of up to 32 bits a single shift-and-mask; wider reads go
/// in two halves. The decoders call this several times per sample, so the
/// common path is kept branch-light and inlined.
#[derive(Debug, Clone)]
pub struct BitStreamReader<'a> {
    stream: &'a [u8],
    stream_offset: usize,

    buffer: u64,
    valid: u8,
    last: u8,
}

#[cold]
#[inline(never)]
fn eof() -> io::Error {
    io::Error::new(io::ErrorKind::UnexpectedEof, "EOF")
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

    /// Loads as many whole bytes as fit into the window. After this, `valid`
    /// is at least 57 unless the stream is exhausted.
    ///
    /// The common case -- more than a word of stream left, so the final byte
    /// is not involved -- is one 8-byte load and a shift, kept small enough to
    /// inline into every `read_bits`. The last-word case, where the byte
    /// captured at construction has to be patched in, is out of line.
    #[inline(always)]
    fn refill(&mut self) {
        let want = ((64 - self.valid) / 8) as usize;
        if want == 0 {
            return;
        }
        let avail = self.stream.len() - self.stream_offset;
        if avail > 8 {
            let off = self.stream_offset;
            let w = u64::from_be_bytes(self.stream[off..off + 8].try_into().unwrap());
            let bits = (want * 8) as u32;
            self.buffer = if bits == 64 {
                w
            } else {
                (self.buffer << bits) | (w >> (64 - bits))
            };
            self.valid += bits as u8;
            self.stream_offset += want;
            return;
        }
        self.refill_tail(want, avail);
    }

    #[cold]
    #[inline(never)]
    fn refill_tail(&mut self, want: usize, avail: usize) {
        if avail == 0 {
            return;
        }
        let take = want.min(avail);
        let off = self.stream_offset;
        let mut word = [0u8; 8];
        if avail >= 8 {
            word.copy_from_slice(&self.stream[off..off + 8]);
        } else {
            word[..avail].copy_from_slice(&self.stream[off..]);
        }
        if take == avail {
            // The final byte comes from the copy taken at construction (see
            // `last`): a writer appending to the same buffer may still be
            // filling it in.
            word[take - 1] = self.last;
        }
        let w = u64::from_be_bytes(word);
        let bits = (take * 8) as u32;
        self.buffer = if bits == 64 {
            w
        } else {
            (self.buffer << bits) | (w >> (64 - bits))
        };
        self.valid += bits as u8;
        self.stream_offset += take;
    }

    /// Reads a single bit and returns it as a boolean.
    #[inline(always)]
    pub fn read_bit(&mut self) -> io::Result<bool> {
        if self.valid == 0 {
            self.refill();
            if self.valid == 0 {
                return Err(eof());
            }
        }
        self.valid -= 1;
        Ok((self.buffer >> self.valid) & 1 != 0)
    }

    /// Reads the specified number of bits and returns them as a u64.
    ///
    /// `inline(always)`, like `refill`: the decoders call this two or three
    /// times per sample and LLVM was leaving it out of line in the Chimp value
    /// loop, where the call and the `Result` return cost more than the read.
    #[inline(always)]
    pub fn read_bits(&mut self, nbits: u8) -> io::Result<u64> {
        if nbits > self.valid {
            if nbits > 64 {
                return Err(too_many_bits(nbits as u32));
            }
            self.refill();
            if nbits > self.valid {
                return self.read_bits_straddling(nbits);
            }
        }
        self.valid -= nbits;
        let v = self.buffer >> self.valid;
        Ok(if nbits >= 64 {
            v
        } else {
            v & ((1u64 << nbits) - 1)
        })
    }

    /// A read wider than the window holds after a refill: only possible for
    /// a 58–64-bit read landing within seven bytes of a refill boundary, or at
    /// the end of the stream.
    #[cold]
    #[inline(never)]
    fn read_bits_straddling(&mut self, nbits: u8) -> io::Result<u64> {
        let first = self.valid;
        if first == 0 || self.stream_offset >= self.stream.len() {
            return Err(eof());
        }
        let hi = self.read_bits(first)?;
        let rest = nbits - first;
        self.refill();
        if rest > self.valid {
            return Err(eof());
        }
        let lo = self.read_bits(rest)?;
        Ok((hi << rest) | lo)
    }

    /// The next `nbits` (at most 32) bits without consuming them, as they would
    /// be read; missing bits past the end of the stream read as zero. A
    /// decoder peeks a variable-length header in one go, then [`skip`]s the
    /// length it turned out to have.
    #[inline(always)]
    pub fn peek_upto(&mut self, nbits: u8) -> u64 {
        debug_assert!(nbits <= 32);
        if nbits > self.valid {
            self.refill();
            if nbits > self.valid {
                // Zero-pad what is left.
                let mask = (1u64 << self.valid) - 1;
                return (self.buffer & mask) << (nbits - self.valid);
            }
        }
        (self.buffer >> (self.valid - nbits)) & ((1u64 << nbits) - 1)
    }

    /// Consumes `nbits` bits, which must have been [`peek_upto`]ed.
    #[inline(always)]
    pub fn skip(&mut self, nbits: u8) -> io::Result<()> {
        if nbits > self.valid {
            return Err(eof());
        }
        self.valid -= nbits;
        Ok(())
    }

    /// Reads a single byte (8 bits).
    #[inline]
    pub fn read_byte(&mut self) -> io::Result<u8> {
        Ok(self.read_bits(8)? as u8)
    }

    pub fn read_u64(&mut self) -> io::Result<u64> {
        self.read_bits(64)
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
}

#[cold]
#[inline(never)]
fn too_many_bits(num: u32) -> io::Error {
    log_warning(format!("bitstream read_bits overflow request: {num}"));
    io::Error::new(io::ErrorKind::InvalidData, "bit count exceeds 64")
}

impl BitRead for BitStreamReader<'_> {
    fn read_bit(&mut self) -> io::Result<bool> {
        BitStreamReader::read_bit(self)
    }

    fn read_bits(&mut self, num: u32) -> io::Result<u64> {
        if num > 64 {
            return Err(too_many_bits(num));
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
    fn reads_across_refills_and_to_the_last_bit() {
        // 20 bytes: every read size crosses the 8-byte refill boundary somewhere.
        let data: Vec<u8> = (0..20u8).map(|i| i.wrapping_mul(37) ^ 0x5A).collect();
        for nbits in [1u8, 3, 7, 8, 13, 17, 31, 32, 33, 47, 63, 64] {
            let mut reader = BitStreamReader::new(&data);
            let mut bit_pos = 0usize;
            let total = data.len() * 8;
            while bit_pos + nbits as usize <= total {
                let got = reader.read_bits(nbits).unwrap();
                let mut expect = 0u64;
                for i in 0..nbits as usize {
                    let p = bit_pos + i;
                    let bit = (data[p / 8] >> (7 - p % 8)) & 1;
                    expect = (expect << 1) | bit as u64;
                }
                assert_eq!(got, expect, "nbits {nbits} at bit {bit_pos}");
                bit_pos += nbits as usize;
            }
            // Whatever is left is fewer than `nbits` bits: a full read fails,
            // single bits still come out until the very end.
            assert!(reader.read_bits(nbits).is_err() || total - bit_pos >= nbits as usize);
            let mut rest = 0;
            while reader.read_bit().is_ok() {
                rest += 1;
            }
            assert!(rest < nbits as usize + 32);
        }
    }

    #[test]
    fn peek_then_skip_matches_reads() {
        let data = vec![0b1011_0010u8, 0b1111_0000];
        let mut reader = BitStreamReader::new(&data);
        assert_eq!(reader.peek_upto(4), 0b1011);
        reader.skip(1).unwrap(); // consume the leading 1
        assert_eq!(reader.peek_upto(4), 0b0110);
        reader.skip(4).unwrap();
        assert_eq!(reader.read_bits(3).unwrap(), 0b010);
        // 8 bits left: peeking 4 is exact; after 5 more, the peek pads with zeros.
        assert_eq!(reader.peek_upto(4), 0b1111);
        reader.skip(5).unwrap();
        assert_eq!(reader.peek_upto(4), 0b0000);
        reader.skip(3).unwrap();
        assert!(reader.skip(1).is_err());
        assert_eq!(reader.peek_upto(4), 0);
    }

    #[test]
    fn test_read_uvarint() {
        let data = vec![0xACu8, 0x02u8];
        let mut reader = BitStreamReader::new(&data);

        let val = reader.read_uvarint().unwrap();
        assert_eq!(val, 300);
    }
}
