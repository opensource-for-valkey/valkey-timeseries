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
        self.add_value_prefixed(out, 0, 0, value)
    }

    /// [`add_value`](Self::add_value) with `prefix_len` bits of `prefix` written
    /// immediately before the value's own header, in the same writer call.
    ///
    /// The ELF layer owns the 1–6-bit case marker that precedes every Chimp value;
    /// handing it in here lets marker, Chimp flag, leading-zero code and width go
    /// out as one write, and for most values the payload too. The bit layout is
    /// exactly what the separate writes produced.
    #[inline]
    pub fn add_value_prefixed(
        &mut self,
        out: &mut BitStream,
        prefix: u64,
        prefix_len: u32,
        value: u64,
    ) -> io::Result<()> {
        if self.first {
            self.first = false;
            self.stored_val = value;
            if prefix_len > 0 {
                out.write_bits(prefix_len, prefix)?;
            }
            out.write_bits(64, value)?;
        } else {
            self.compress_value(out, prefix, prefix_len, value)?;
        }
        Ok(())
    }

    /// Emit the `xor == 0` flag for a value the caller has already established
    /// is unchanged.
    ///
    /// Equivalent to `add_value` with the stored value, but the Elf layer
    /// recognises repeats before erasure, so it no longer knows which bit
    /// pattern (raw or erased) was last fed in here.
    pub fn add_repeat(&mut self, out: &mut BitStream) -> io::Result<()> {
        self.add_repeat_prefixed(out, 0, 0)
    }

    /// [`add_repeat`](Self::add_repeat) with a caller prefix, as
    /// [`add_value_prefixed`](Self::add_value_prefixed).
    #[inline]
    pub fn add_repeat_prefixed(
        &mut self,
        out: &mut BitStream,
        prefix: u64,
        prefix_len: u32,
    ) -> io::Result<()> {
        debug_assert!(!self.first, "a repeat needs a preceding value");
        out.write_bits(prefix_len + 2, prefix << 2)?;
        self.stored_lz = 65;
        Ok(())
    }

    /// Write `header` (`header_len` bits, prefix already folded in) followed by the
    /// `sig`-bit `payload`, as one write when both fit in 64 bits.
    #[inline(always)]
    fn emit(
        out: &mut BitStream,
        header: u64,
        header_len: u32,
        payload: u64,
        sig: u32,
    ) -> io::Result<()> {
        if header_len + sig <= 64 {
            out.write_bits(header_len + sig, (header << sig) | payload)
        } else {
            out.write_bits(header_len, header)?;
            out.write_bits(sig, payload)
        }
    }

    #[inline]
    fn compress_value(
        &mut self,
        out: &mut BitStream,
        prefix: u64,
        prefix_len: u32,
        value: u64,
    ) -> io::Result<()> {
        let xor = self.stored_val ^ value;
        if xor == 0 {
            out.write_bits(prefix_len + 2, prefix << 2)?;
            self.stored_lz = 65;
        } else {
            let nlz = xor.leading_zeros() as i32; // 0..=63 for xor != 0
            let lz = LEADING_ROUND_ENC[nlz as usize];
            let tz = xor.trailing_zeros() as i32;

            if tz > THRESHOLD {
                // flag `01`, 3-bit leading code, 6-bit width, then `sig` bits.
                let sig = (64 - lz - tz) as u32;
                let header = (prefix << 11)
                    | (0b01 << 9)
                    | (LEADING_REPR_ENC[lz as usize] << 6)
                    | sig as u64;
                Self::emit(out, header, prefix_len + 11, xor >> tz, sig)?;
                self.stored_lz = 65;
            } else if lz == self.stored_lz {
                // flag `10`, then 64 - lz bits.
                let sig = (64 - lz) as u32;
                let header = (prefix << 2) | 0b10;
                Self::emit(out, header, prefix_len + 2, xor, sig)?;
            } else {
                // flag `11`, 3-bit leading code, then 64 - lz bits.
                self.stored_lz = lz;
                let sig = (64 - lz) as u32;
                let header = (prefix << 5) | (0b11 << 3) | LEADING_REPR_ENC[lz as usize];
                Self::emit(out, header, prefix_len + 5, xor, sig)?;
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
    #[inline(always)]
    pub fn read_value(&mut self, inp: &mut BitStreamReader) -> io::Result<u64> {
        self.next(inp)?;
        Ok(self.stored_val)
    }

    /// [`read_value`](Self::read_value), also reporting whether the value was
    /// unchanged (the `xor == 0` case). The Elf layer needs that flag to tell a
    /// repeat apart from a value that merely shares the previous `beta_star`.
    #[inline(always)]
    pub fn read_value_flagged(&mut self, inp: &mut BitStreamReader) -> io::Result<(u64, bool)> {
        let repeat = self.next(inp)?;
        Ok((self.stored_val, repeat))
    }

    #[inline(always)]
    fn next(&mut self, inp: &mut BitStreamReader) -> io::Result<bool> {
        if self.first {
            self.first = false;
            self.stored_val = inp.read_bits(64)?;
            Ok(false)
        } else {
            self.next_value(inp)
        }
    }

    /// Returns `true` when the encoded XOR was zero, i.e. the value repeats.
    ///
    /// The header — a 2-bit flag, then for two of the cases a 3-bit leading-zero
    /// index and for one of those a 6-bit significant-bit count — is read with a
    /// single 11-bit peek and one `skip`, so a value costs one header read plus
    /// the payload read instead of up to three.
    #[inline(always)]
    fn next_value(&mut self, inp: &mut BitStreamReader) -> io::Result<bool> {
        let head = inp.peek_upto(11);
        let flag = (head >> 9) as i32;
        match flag {
            3 => {
                // New leading zeros: flag(2) + lz(3), then 64 − lz payload bits.
                self.stored_lz = LEADING_REPR_DEC[((head >> 6) & 7) as usize];
                inp.skip(5)?;
                let value = inp.read_bits((64 - self.stored_lz) as u8)?;
                self.stored_val ^= value;
            }
            2 => {
                // Reuse stored leading zeros.
                inp.skip(2)?;
                let value = inp.read_bits((64 - self.stored_lz) as u8)?;
                self.stored_val ^= value;
            }
            1 => {
                // Trailing-zeros case: flag(2) + lz(3) + sig(6), then the significant bits.
                self.stored_lz = LEADING_REPR_DEC[((head >> 6) & 7) as usize];
                let mut sig = (head & 63) as i32;
                if sig == 0 {
                    sig = 64;
                }
                inp.skip(11)?;
                self.stored_tz = 64 - sig - self.stored_lz;
                let mut value = inp.read_bits((64 - self.stored_lz - self.stored_tz) as u8)?;
                value <<= self.stored_tz as u8;
                self.stored_val ^= value;
            }
            _ => {
                // flag == 0: xor was zero, value unchanged.
                inp.skip(2)?;
                return Ok(true);
            }
        }
        Ok(false)
    }
}
