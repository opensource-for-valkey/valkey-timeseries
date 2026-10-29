//! Port of `gr.aueb.delorean.chimp.Chimp` / `ChimpDecompressor`.
//!
//! Chimp is a streaming XOR-based floating-point codec. The ELF layer feeds it
//! the (possibly erased) raw bit patterns of `f64` values and reads them back.
//!
//! Each value after the first is its XOR with the previous one, under a 2-bit flag:
//!
//! ```text
//! 00                              xor == 0: the value repeats
//! 01 [lz: 3] [sig: 6] [sig bits]  new window: rounded leading zeros and significant width
//! 10 [64 - lz - tz bits]          the stored (lz, tz) window again
//! 11 [lz: 3] [64 - lz bits]       new window with no trailing zeros
//! ```
//!
//! Unlike the reference, `10` reuses the whole window, trailing zeros included, and the
//! encoder takes it whenever the XOR fits and it is no longer than a fresh header would be
//! (Gorilla's window rule). A repeat leaves the window alone.

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

/// `stored_lz` before the first `01`/`11` header. No XOR has 64 leading zeros, so the
/// encoder's fit test `nlz >= stored_lz` fails on it without a separate check.
pub(crate) const NO_WINDOW: i32 = 64;

/// A test-only counter that takes no part in equality or hashing, so counting does not make
/// an encoder differ from its reloaded copy.
#[cfg(test)]
#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct Tally(pub u32);

#[cfg(test)]
impl PartialEq for Tally {
    fn eq(&self, _: &Self) -> bool {
        true
    }
}

#[cfg(test)]
impl Eq for Tally {}

#[cfg(test)]
impl std::hash::Hash for Tally {
    fn hash<H: std::hash::Hasher>(&self, _: &mut H) {}
}

/// Streaming Chimp encoder. Writes into a shared `BitWriter`.
///
/// The four fields are the whole codec state, so a chunk can persist and
/// restore an encoder mid-stream via [`state`](Self::state) /
/// [`from_state`](Self::from_state).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ChimpEnc {
    /// Rounded leading zeros of the current window, or [`NO_WINDOW`]. `u8`s, so the window
    /// fits the padding beside `first` and the encoder stays 16 bytes.
    stored_lz: u8,
    /// Trailing zeros of the current window (0 after a `11` header).
    stored_tz: u8,
    stored_val: u64,
    first: bool,
    /// `10` writes, for tests.
    #[cfg(test)]
    pub(crate) window_reuse: Tally,
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
    pub stored_tz: i32,
    pub stored_val: u64,
    pub first: bool,
}

impl ChimpEncState {
    /// Whether an encoder could have reached this state: no window, or a rounded leading
    /// count from the header table and a trailing count that leaves at least one bit.
    pub fn is_valid(&self) -> bool {
        if self.stored_lz == NO_WINDOW {
            return self.stored_tz == 0;
        }
        LEADING_REPR_DEC.contains(&self.stored_lz)
            && (0..64).contains(&self.stored_tz)
            && self.stored_lz + self.stored_tz < 64
    }
}

impl ChimpEnc {
    pub fn new() -> Self {
        Self {
            stored_lz: NO_WINDOW as u8,
            stored_tz: 0,
            stored_val: 0,
            first: true,
            #[cfg(test)]
            window_reuse: Tally::default(),
        }
    }

    /// The codec state, for serialization.
    pub fn state(&self) -> ChimpEncState {
        ChimpEncState {
            stored_lz: self.stored_lz.into(),
            stored_tz: self.stored_tz.into(),
            stored_val: self.stored_val,
            first: self.first,
        }
    }

    /// Rebuild an encoder from a [`state`](Self::state) snapshot, or `None` for a state no
    /// encoder writes (see [`ChimpEncState::is_valid`]): appending under a window the decoder
    /// would read differently corrupts the stream.
    pub fn from_state(state: ChimpEncState) -> Option<Self> {
        state.is_valid().then_some(Self {
            // In range: `is_valid` holds both to 0..=64.
            stored_lz: state.stored_lz as u8,
            stored_tz: state.stored_tz as u8,
            stored_val: state.stored_val,
            first: state.first,
            #[cfg(test)]
            window_reuse: Tally::default(),
        })
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
        // The window stays: the decoder keeps it across a repeat too.
        out.write_bits(prefix_len + 2, prefix << 2)
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
            // The window stays, as for `add_repeat`.
            out.write_bits(prefix_len + 2, prefix << 2)?;
        } else {
            let nlz = xor.leading_zeros() as i32; // 0..=63 for xor != 0
            let lz = LEADING_ROUND_ENC[nlz as usize];
            let tz = xor.trailing_zeros() as i32;
            let code = LEADING_REPR_ENC[lz as usize];

            // The fresh header: `01` (3-bit leading code, 6-bit width) when the trailing zeros
            // pay for the width field, else `11` (3-bit leading code, no trailing zeros).
            let trailing = tz > THRESHOLD;
            let fresh_tz = if trailing { tz } else { 0 };
            let fresh_sig = 64 - lz - fresh_tz;
            let (fresh_header, fresh_len) = if trailing {
                ((0b01 << 9) | (code << 6) | fresh_sig as u64, 11)
            } else {
                ((0b11 << 3) | code, 5)
            };

            // Reuse the stored window (`10`) when the XOR fits it and that costs no more bits
            // than the fresh header. `NO_WINDOW` fails `nlz >= slz`. The choice is close to
            // random on quantized data, so it is written as selects: as three branches it cost
            // quantized rows 14–25 % encode time (2026-10-29), as selects 0–6 %.
            let slz = i32::from(self.stored_lz);
            let stz = i32::from(self.stored_tz);
            let reuse_sig = 64 - slz - stz;
            let reuse = (nlz >= slz) & (tz >= stz) & (2 + reuse_sig <= fresh_len + fresh_sig);
            #[cfg(test)]
            {
                self.window_reuse.0 += u32::from(reuse);
            }
            let (header, header_len, shift, sig) = if reuse {
                (0b10, 2, stz, reuse_sig)
            } else {
                (fresh_header, fresh_len, fresh_tz, fresh_sig)
            };
            self.stored_lz = if reuse { slz } else { lz } as u8;
            self.stored_tz = shift as u8;
            Self::emit(
                out,
                (prefix << header_len) | header,
                prefix_len + header_len as u32,
                xor >> shift,
                sig as u32,
            )?;
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
    /// Payload width of the current window, `64 - lz - tz`; 0 before the first `01`/`11`.
    stored_width: u32,
    /// Trailing zeros of the current window.
    stored_tz: u32,
    stored_val: u64,
    first: bool,
}

impl Default for ChimpDec {
    fn default() -> Self {
        Self::new()
    }
}

/// Error for a `01` header whose leading count plus significant width exceeds 64 bits.
/// Kept out of line so the hot decode arm stays small.
#[cold]
#[inline(never)]
fn corrupt_trailing_window() -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidData,
        "corrupt chimp chunk: leading + width exceed 64",
    )
}

/// Error for a `10` with no window to reuse: one before any `01`/`11` header.
#[cold]
#[inline(never)]
fn reuse_without_window() -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidData,
        "corrupt chimp chunk: window reuse before any window",
    )
}

impl ChimpDec {
    pub fn new() -> Self {
        Self {
            stored_width: 0,
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
    /// repeat apart from a value that merely shares the previous `alpha`.
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
                // New window, no trailing zeros: flag(2) + lz(3), then 64 − lz payload bits.
                let lz = LEADING_REPR_DEC[((head >> 6) & 7) as usize] as u32;
                inp.skip(5)?;
                self.stored_width = 64 - lz;
                self.stored_tz = 0;
                let value = inp.read_bits(self.stored_width as u8)?;
                self.stored_val ^= value;
            }
            2 => {
                // Reuse the stored window: 64 − lz − tz bits, shifted back by tz. Arms `1`
                // and `3` only store widths in 1..=64; 0 means no window yet.
                if self.stored_width == 0 {
                    return Err(reuse_without_window());
                }
                inp.skip(2)?;
                let value = inp.read_bits(self.stored_width as u8)?;
                self.stored_val ^= value << self.stored_tz;
            }
            1 => {
                // Trailing-zeros case: flag(2) + lz(3) + sig(6), then the significant bits.
                let lz = LEADING_REPR_DEC[((head >> 6) & 7) as usize] as u32;
                let mut sig = (head & 63) as u32;
                if sig == 0 {
                    sig = 64;
                }
                // A valid header has lz + sig <= 64; reject a corrupt one before the
                // subtraction below wraps and the shift overflows, and before the window moves.
                if sig + lz > 64 {
                    return Err(corrupt_trailing_window());
                }
                inp.skip(11)?;
                self.stored_width = sig;
                self.stored_tz = 64 - sig - lz;
                let value = inp.read_bits(sig as u8)?;
                self.stored_val ^= value << self.stored_tz;
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn corrupt_trailing_window_header_is_rejected() {
        // ELF-free Chimp stream: first value 64 raw bits, then flag 01, lz code 7 (=24),
        // sig 0 (=64): 24 + 64 > 64 must be an error, not a shift by 232.
        let mut w = BitStream::new();
        w.write_bits(64, 1.5f64.to_bits()).unwrap();
        w.write_bits(11, (0b01 << 9) | (7 << 6)).unwrap();
        w.write_bits(64, 0).unwrap();
        let mut r = BitStreamReader::new(w.bytes());
        let mut dec = ChimpDec::new();
        dec.read_value(&mut r).unwrap();
        let err = dec.read_value(&mut r).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
    }

    fn bit_len(w: &BitStream) -> usize {
        w.len() * 8 - w.count as usize
    }

    /// The two-bit Chimp flag at bit `pos` of `w`.
    fn flag_at(w: &BitStream, pos: usize) -> u64 {
        BitStreamReader::from_bit_position(w.bytes(), pos)
            .unwrap()
            .peek_upto(2)
    }

    /// A repeat leaves the window alone: 1.0, then 1.5 opens a one-bit trailing window
    /// (`01`), a repeat, then 1.0 again fits that window and is written as `10`.
    #[test]
    fn repeats_keep_the_window() {
        let mut w = BitStream::new();
        let mut enc = ChimpEnc::new();
        enc.add_value(&mut w, 1.0f64.to_bits()).unwrap();
        let a = bit_len(&w);
        enc.add_value(&mut w, 1.5f64.to_bits()).unwrap();
        let rep = bit_len(&w);
        enc.add_repeat(&mut w).unwrap();
        let b = bit_len(&w);
        enc.add_value(&mut w, 1.0f64.to_bits()).unwrap();
        let end = bit_len(&w);

        assert_eq!(flag_at(&w, a), 0b01);
        assert_eq!(flag_at(&w, rep), 0b00);
        assert_eq!(flag_at(&w, b), 0b10);
        // Flag plus the one significant bit.
        assert_eq!(end - b, 3);

        let mut r = BitStreamReader::new(w.bytes());
        let mut dec = ChimpDec::new();
        let got: Vec<u64> = (0..4).map(|_| dec.read_value(&mut r).unwrap()).collect();
        assert_eq!(got, [1.0f64, 1.5, 1.5, 1.0].map(f64::to_bits));
    }

    /// A value whose XOR has more trailing zeros than the window keeps `10` when that is
    /// cheaper, and the decoder shifts the payload back by the stored trailing count.
    #[test]
    fn reuse_covers_narrower_xors_and_round_trips() {
        let values: Vec<u64> = [100.25f64, 100.75, 100.5, 101.25, 100.25, 100.0, 103.75]
            .map(f64::to_bits)
            .to_vec();
        let mut w = BitStream::new();
        let mut enc = ChimpEnc::new();
        for v in &values {
            enc.add_value(&mut w, *v).unwrap();
        }
        // 101.25 -> 100.25 (one bit, 46 trailing zeros) reuses the 45-trailing-zero window
        // 100.5 -> 101.25 opened.
        assert!(enc.window_reuse.0 >= 1, "{}", enc.window_reuse.0);
        let mut r = BitStreamReader::new(w.bytes());
        let mut dec = ChimpDec::new();
        let got: Vec<u64> = values
            .iter()
            .map(|_| dec.read_value(&mut r).unwrap())
            .collect();
        assert_eq!(got, values);
    }

    /// `10` reuses a window, so one before any `01`/`11` header is corrupt.
    #[test]
    fn reuse_before_any_window_is_rejected() {
        let mut w = BitStream::new();
        w.write_bits(64, 1.5f64.to_bits()).unwrap();
        w.write_bits(2, 0b10).unwrap();
        w.write_bits(64, 0).unwrap();
        let mut r = BitStreamReader::new(w.bytes());
        let mut dec = ChimpDec::new();
        dec.read_value(&mut r).unwrap();
        let err = dec.read_value(&mut r).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
    }

    /// A rejected `01` header leaves the decoder's window as it was.
    #[test]
    fn rejected_trailing_header_does_not_move_the_window() {
        let mut w = BitStream::new();
        w.write_bits(64, 1.0f64.to_bits()).unwrap();
        // `11`, lz code 2 (= 12): a 52-bit window.
        w.write_bits(5, (0b11 << 3) | 2).unwrap();
        w.write_bits(52, 1).unwrap();
        // A corrupt `01`: lz 24 + sig 64.
        w.write_bits(11, (0b01 << 9) | (7 << 6)).unwrap();
        let mut r = BitStreamReader::new(w.bytes());
        let mut dec = ChimpDec::new();
        dec.read_value(&mut r).unwrap();
        dec.read_value(&mut r).unwrap();
        let before = (dec.stored_width, dec.stored_tz);
        assert!(dec.read_value(&mut r).is_err());
        assert_eq!((dec.stored_width, dec.stored_tz), before);
    }

    /// The window must not grow the encoder, which sits in every Chimp chunk (and so in the
    /// size of every chunk enum variant).
    #[test]
    fn encoder_stays_sixteen_bytes() {
        assert_eq!(std::mem::size_of::<ChimpEnc>(), 16);
    }

    #[test]
    fn from_state_rejects_inconsistent_windows() {
        let ok = ChimpEnc::new().state();
        assert!(ChimpEnc::from_state(ok).is_some());
        for lz in LEADING_REPR_DEC {
            for tz in [0, 63 - lz] {
                let s = ChimpEncState {
                    stored_lz: lz,
                    stored_tz: tz,
                    ..ok
                };
                assert!(ChimpEnc::from_state(s).is_some(), "lz {lz} tz {tz}");
            }
        }
        for (lz, tz) in [
            (1, 0),
            (25, 0),
            (-1, 0),
            (65, 0),
            (i32::MAX, 0),
            (NO_WINDOW, 1),
            (8, 56),
            (0, 64),
            (0, -1),
        ] {
            let s = ChimpEncState {
                stored_lz: lz,
                stored_tz: tz,
                ..ok
            };
            assert!(ChimpEnc::from_state(s).is_none(), "lz {lz} tz {tz}");
        }
    }
}
