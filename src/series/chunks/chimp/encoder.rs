//! Port of `gr.aueb.delorean.chimp.Chimp` / `ChimpDecompressor`.
//!
//! Chimp is a streaming XOR-based floating-point codec. The ELF layer feeds it
//! the (possibly erased) raw bit patterns of `f64` values and reads them back.

use crate::series::chunks::stream::bitstream::BitStream;
use crate::series::chunks::stream::bitstream_reader::BitStreamReader;
use std::io;

pub const THRESHOLD: i32 = 6;

/// 3-bit codes for the *rounded* number of leading zeros (encoder side).
const LEADING_REPR_ENC: [u64; 64] = [
    0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 1, 1, 2, 2, 2, 2, 3, 3, 4, 4, 5, 5, 6, 6, 7, 7, 7, 7, 7, 7, 7, 7,
    7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7,
];

/// Rounded number of leading zeros, indexed by `leading_zeros(xor)` (encoder).
const LEADING_ROUND_ENC: [i32; 64] = [
    // 0..7
    0, 0, 0, 0, 0, 0, 0, 0, // 8..15
    8, 8, 8, 8, 12, 12, 12, 12, // 16..23
    16, 16, 18, 18, 20, 20, 22, 22, // 24..63
    24, 24, 24, 24, 24, 24, 24, 24, 24, 24, 24, 24, 24, 24, 24, 24, 24, 24, 24, 24, 24, 24, 24, 24,
    24, 24, 24, 24, 24, 24, 24, 24, 24, 24, 24, 24, 24, 24, 24, 24,
];

/// Inverse of `LEADING_REPR_ENC`: 3-bit code -> rounded leading zeros (decoder).
const LEADING_REPR_DEC: [i32; 8] = [0, 8, 12, 16, 18, 20, 22, 24];

/// Streaming Chimp encoder. Writes into a shared `BitWriter`.
///
/// The three fields are the whole codec state, so a chunk can persist and
/// restore an encoder mid-stream via [`state`](Self::state) /
/// [`from_state`](Self::from_state).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ChimpEnc {
    stored_lz: i32,
    stored_val: u64,
    first: bool,
}

impl Default for ChimpEnc {
    fn default() -> Self {
        Self::new()
    }
}

/// Serializable snapshot of a [`ChimpEnc`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ChimpEncState {
    pub stored_lz: i32,
    pub stored_val: u64,
    pub first: bool,
}

impl ChimpEnc {
    pub fn new() -> Self {
        Self {
            stored_lz: i32::MAX,
            stored_val: 0,
            first: true,
        }
    }

    /// The codec state, for serialization.
    pub fn state(&self) -> ChimpEncState {
        ChimpEncState {
            stored_lz: self.stored_lz,
            stored_val: self.stored_val,
            first: self.first,
        }
    }

    /// Rebuild an encoder from a [`state`](Self::state) snapshot.
    pub fn from_state(state: ChimpEncState) -> Self {
        Self {
            stored_lz: state.stored_lz,
            stored_val: state.stored_val,
            first: state.first,
        }
    }

    pub fn add_value(&mut self, out: &mut BitStream, value: u64) -> io::Result<()> {
        if self.first {
            self.first = false;
            self.stored_val = value;
            out.write_bits(64, value)?;
        } else {
            self.compress_value(out, value)?;
        }
        Ok(())
    }

    fn compress_value(&mut self, out: &mut BitStream, value: u64) -> io::Result<()> {
        let xor = self.stored_val ^ value;
        if xor == 0 {
            out.write_bits(2, 0b00)?;
            self.stored_lz = 65;
        } else {
            let nlz = xor.leading_zeros() as i32; // 0..=63 for xor != 0
            let lz = LEADING_ROUND_ENC[nlz as usize];
            let tz = xor.trailing_zeros() as i32;

            if tz > THRESHOLD {
                let sig = 64 - lz - tz;
                out.write_bits(2, 0b01)?;
                out.write_bits(3, LEADING_REPR_ENC[lz as usize])?;
                out.write_bits(6, sig as u64)?;
                out.write_bits(sig as u32, xor >> tz)?;
                self.stored_lz = 65;
            } else if lz == self.stored_lz {
                let sig = 64 - lz;
                out.write_bits(2, 0b10)?;
                out.write_bits(sig as u32, xor)?;
            } else {
                self.stored_lz = lz;
                let sig = 64 - lz;
                out.write_bits(2, 0b11)?;
                out.write_bits(3, LEADING_REPR_ENC[lz as usize])?;
                out.write_bits(sig as u32, xor)?;
            }
        }
        self.stored_val = value;
        Ok(())
    }
}

/// Streaming Chimp decoder. Reads from a shared `BitReader`.
///
/// The reference implementation reserves the canonical quiet NaN as an
/// end-of-stream sentinel. This port does not: the caller knows how many
/// values the stream holds, which leaves every bit pattern — NaN included —
/// usable as a data value.
pub struct ChimpDec {
    stored_lz: i32,
    stored_tz: i32,
    stored_val: u64,
    first: bool,
}

impl Default for ChimpDec {
    fn default() -> Self {
        Self::new()
    }
}

impl ChimpDec {
    pub fn new() -> Self {
        Self {
            stored_lz: i32::MAX,
            stored_tz: 0,
            stored_val: 0,
            first: true,
        }
    }

    /// Reads the next raw value, or `Err(UnexpectedEof)` if the bit stream
    /// runs out.
    pub fn read_value(&mut self, inp: &mut BitStreamReader) -> io::Result<u64> {
        self.next(inp)?;
        Ok(self.stored_val)
    }

    fn next(&mut self, inp: &mut BitStreamReader) -> io::Result<()> {
        if self.first {
            self.first = false;
            self.stored_val = inp.read_bits(64)?;
        } else {
            self.next_value(inp)?;
        }
        Ok(())
    }

    fn next_value(&mut self, inp: &mut BitStreamReader) -> io::Result<()> {
        let flag = inp.read_bits(2)? as i32;
        match flag {
            3 => {
                // New leading zeros.
                self.stored_lz = LEADING_REPR_DEC[inp.read_bits(3)? as usize];
                let value = inp.read_bits((64 - self.stored_lz) as u8)?;
                self.stored_val ^= value;
            }
            2 => {
                // Reuse stored leading zeros.
                let value = inp.read_bits((64 - self.stored_lz) as u8)?;
                self.stored_val ^= value;
            }
            1 => {
                // Trailing-zeros case.
                self.stored_lz = LEADING_REPR_DEC[inp.read_bits(3)? as usize];
                let mut sig = inp.read_bits(6)? as i32;
                if sig == 0 {
                    sig = 64;
                }
                self.stored_tz = 64 - sig - self.stored_lz;
                let mut value = inp.read_bits((64 - self.stored_lz - self.stored_tz) as u8)?;
                value <<= self.stored_tz as u8;
                self.stored_val ^= value;
            }
            _ => {
                // flag == 0: xor was zero, value unchanged.
            }
        }
        Ok(())
    }
}
