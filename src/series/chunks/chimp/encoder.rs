//! Port of `gr.aueb.delorean.chimp.Chimp` / `ChimpDecompressor`.
//!
//! Chimp is a streaming XOR-based floating-point codec. The ELF layer feeds it
//! the (possibly erased) raw bit patterns of `f64` values and reads them back.
//!
//! Each value after the first is its XOR with the previous one, under a 2-bit flag:
//!
//! ```text
//! 00                              xor == 0: the value repeats
//! 01 [lz code] [sig: 6] [sig bits]  new window: rounded leading zeros and significant width
//! 10 [64 - lz - tz bits]            the stored (lz, tz) window again
//! 11 [lz code] [64 - lz bits]       new window with no trailing zeros
//! ```
//!
//! The leading-zero code is `LZ_CODE_BITS` wide and names one of `LZ_BUCKETS`; an XOR's
//! leading-zero count rounds down to a bucket. The widths the top bucket (24) cannot hold,
//! 41..=63, are an escape to an exact leading-zero count of 32..=54 (Gorilla's extended
//! window, the same trick):
//!
//! ```text
//! 01 [top code] [ext: 6] [sig: 6] [sig bits]  extended window: lz = ext - 41 + 32
//! ```
//!
//! Unlike the reference, `10` reuses the whole window, trailing zeros included, and the
//! encoder takes it whenever the XOR fits and it is no longer than a fresh header would be
//! (Gorilla's window rule). A repeat leaves the window alone.

use crate::series::chunks::stream::bitstream::BitStream;
use crate::series::chunks::stream::bitstream_reader::BitStreamReader;
use std::io;

pub const THRESHOLD: i32 = 6;

/// Width of the leading-zero code in the `01` and `11` headers.
pub(crate) const LZ_CODE_BITS: u32 = 3;

/// The leading-zero counts a header can name, ascending from 0. An XOR's count rounds down
/// to a bucket, and the difference is padding carried in the payload.
pub(crate) const LZ_BUCKETS: [i32; 1 << LZ_CODE_BITS] = [0, 8, 12, 16, 18, 20, 22, 24];

/// `01` header: flag, leading-zero code, 6-bit significant width.
const TRAILING_HEADER_BITS: u32 = 2 + LZ_CODE_BITS + 6;
/// `11` header: flag, leading-zero code.
const PLAIN_HEADER_BITS: u32 = 2 + LZ_CODE_BITS;

/// The top leading-zero code and the count it names.
const TOP_CODE: u64 = (1 << LZ_CODE_BITS) - 1;
const TOP_LZ: i32 = LZ_BUCKETS[TOP_CODE as usize];

/// Extended windows. A `01` header whose width field the top bucket cannot hold
/// (`TOP_LZ + width > 64`, fields `EXT_FIELD_BASE..=63`) is an escape: the field names the
/// exact leading-zero count `EXT_FIRST + (field - EXT_FIELD_BASE)`, and a second 6-bit width
/// follows. Slowly changing large values (a counter far above zero) then carry little padding
/// past `TOP_LZ`, and the common headers stay as narrow as they were. Against a top-bucket
/// `01`, the six extra header bits pay from 31 leading zeros up; the counts start at 32.
const EXT_FIRST: i32 = 32;
/// Encoder policy, not format: the extended count written is the XOR's, rounded down to
/// `EXT_FIRST + k * EXT_ROUNDING` and capped at `EXT_LAST`. The headroom keeps later XORs
/// inside the window. Exact counts (0.3 % fewer bytes over the gorilla_report matrix)
/// reopened a window on a quarter of counter + 2^30's values, and its decode, before then
/// one predictable `10` after another, slowed 28 %; rounding by 3 keeps it within 2 %, by 2
/// did not (2026-10-29).
const EXT_ROUNDING: i32 = 3;
/// The first width field the top bucket cannot hold.
const EXT_FIELD_BASE: i32 = 64 - TOP_LZ + 1;
const EXT_LAST: i32 = EXT_FIRST + 63 - EXT_FIELD_BASE;
/// Extended `01` header: the `01` header, then the 6-bit width.
const EXT_HEADER_BITS: u32 = TRAILING_HEADER_BITS + 6;

const _: () = {
    assert!(EXT_FIRST > TOP_LZ && EXT_LAST < 64);
    assert!(EXT_HEADER_BITS <= 32);
    assert!(LZ_BUCKETS[0] == 0);
    assert!(LZ_BUCKETS[LZ_BUCKETS.len() - 1] < 64);
    let mut i = 1;
    while i < LZ_BUCKETS.len() {
        assert!(LZ_BUCKETS[i - 1] < LZ_BUCKETS[i]);
        i += 1;
    }
    // The decoder reads a whole header with one `peek_upto`.
    assert!(TRAILING_HEADER_BITS <= 32);
};

/// Code of the largest bucket at or below each `leading_zeros(xor)`.
const fn leading_codes() -> [u8; 64] {
    let mut table = [0u8; 64];
    let mut nlz = 0;
    while nlz < 64 {
        let mut code = 0;
        while code + 1 < LZ_BUCKETS.len() && LZ_BUCKETS[code + 1] <= nlz as i32 {
            code += 1;
        }
        table[nlz] = code as u8;
        nlz += 1;
    }
    table
}

/// Leading-zero code, indexed by `leading_zeros(xor)` (encoder).
const LEADING_REPR_ENC: [u8; 64] = leading_codes();

/// Rounded number of leading zeros, indexed by `leading_zeros(xor)` (encoder).
const LEADING_ROUND_ENC: [i32; 64] = {
    let mut table = [0i32; 64];
    let mut nlz = 0;
    while nlz < 64 {
        table[nlz] = LZ_BUCKETS[LEADING_REPR_ENC[nlz] as usize];
        nlz += 1;
    }
    table
};

/// The extended count the encoder writes, indexed by `leading_zeros(xor)`; 0 below
/// `EXT_FIRST`.
const EXT_ROUND_ENC: [u8; 64] = {
    let mut table = [0u8; 64];
    let mut nlz = EXT_FIRST;
    while nlz < 64 {
        let rounded = nlz - (nlz - EXT_FIRST) % EXT_ROUNDING;
        table[nlz as usize] = if rounded < EXT_LAST {
            rounded
        } else {
            EXT_LAST
        } as u8;
        nlz += 1;
    }
    table
};

/// The extended header before its count field: `01` and the top code.
const EXT_HEAD: u64 = (0b01 << (LZ_CODE_BITS + 6)) | (TOP_CODE << 6);

/// Leading-zero code -> rounded leading zeros (decoder).
const LEADING_REPR_DEC: [i32; 1 << LZ_CODE_BITS] = LZ_BUCKETS;

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

/// What the encoder wrote for each nonzero XOR, per header (`10`, `01`, `11`): how many
/// had each `leading_zeros(xor)`, and the leading-zero padding the payloads carried.
#[cfg(test)]
#[derive(Debug, Clone)]
pub(crate) struct LeadingZeroLog {
    pub by_header: [[u32; 64]; 3],
    pub padding_bits: [u64; 3],
}

#[cfg(test)]
thread_local! {
    /// Set by a test to log the encoder's header decisions on this thread.
    pub(crate) static LEADING_ZERO_LOG: std::cell::RefCell<Option<Box<LeadingZeroLog>>> =
        const { std::cell::RefCell::new(None) };
}

#[cfg(test)]
fn log_leading_zeros(header: u64, nlz: i32, lz: i32) {
    LEADING_ZERO_LOG.with_borrow_mut(|log| {
        if let Some(log) = log {
            let h = [0, 1, 0, 2][header as usize & 3]; // 10 -> 0, 01 -> 1, 11 -> 2
            log.by_header[h][nlz as usize] += 1;
            log.padding_bits[h] += (nlz - lz) as u64;
        }
    });
}

/// Streaming Chimp encoder. Writes into a shared `BitWriter`.
///
/// The four fields are the whole codec state, so a chunk can persist and
/// restore an encoder mid-stream via [`state`](Self::state) /
/// [`from_state`](Self::from_state).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ChimpEnc {
    /// Leading zeros of the current window (a bucket or an extended count), or
    /// [`NO_WINDOW`]. `u8`s, so the window fits the padding beside `first` and the encoder
    /// stays 16 bytes.
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
    /// Whether an encoder could have reached this state: no window, or a leading count a
    /// header can name (a bucket or an extended count) and a trailing count that leaves at
    /// least one bit.
    pub fn is_valid(&self) -> bool {
        if self.stored_lz == NO_WINDOW {
            return self.stored_tz == 0;
        }
        (LZ_BUCKETS.contains(&self.stored_lz) || (EXT_FIRST..=EXT_LAST).contains(&self.stored_lz))
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
            let code = u64::from(LEADING_REPR_ENC[nlz as usize]);

            // The fresh header: `01` (leading code, 6-bit width) when the trailing zeros pay
            // for the width field, else `11` (leading code, no trailing zeros).
            let trailing = tz > THRESHOLD;
            let fresh_tz = if trailing { tz } else { 0 };
            let fresh_sig = 64 - lz - fresh_tz;
            let (fresh_header, fresh_len) = if trailing {
                (
                    (0b01 << (LZ_CODE_BITS + 6)) | (code << 6) | fresh_sig as u64,
                    TRAILING_HEADER_BITS as i32,
                )
            } else {
                ((0b11 << LZ_CODE_BITS) | code, PLAIN_HEADER_BITS as i32)
            };

            // An extended window instead, when that is strictly cheaper. Whether the XOR has
            // `EXT_FIRST` leading zeros is a property of the series, so the branch predicts well
            // and costs the common case one compare.
            let (fresh_header, fresh_len, fresh_tz, fresh_sig, fresh_lz) = if nlz >= EXT_FIRST {
                let ext_lz = i32::from(EXT_ROUND_ENC[nlz as usize]);
                let ext_sig = 64 - ext_lz - tz;
                let field = (EXT_FIELD_BASE + ext_lz - EXT_FIRST) as u64;
                if (EXT_HEADER_BITS as i32 + ext_sig) < fresh_len + fresh_sig {
                    (
                        (((EXT_HEAD | field) << 6) | ext_sig as u64),
                        EXT_HEADER_BITS as i32,
                        tz,
                        ext_sig,
                        ext_lz,
                    )
                } else {
                    (fresh_header, fresh_len, fresh_tz, fresh_sig, lz)
                }
            } else {
                (fresh_header, fresh_len, fresh_tz, fresh_sig, lz)
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
            #[cfg(test)]
            log_leading_zeros(
                header >> (header_len - 2),
                nlz,
                if reuse { slz } else { fresh_lz },
            );
            self.stored_lz = if reuse { slz } else { fresh_lz } as u8;
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
    /// The header — a 2-bit flag, then for two of the cases an `LZ_CODE_BITS`
    /// leading-zero code and for one of those a 6-bit significant-bit count — is
    /// read with a single `TRAILING_HEADER_BITS` peek and one `skip`, so a value
    /// costs one header read plus the payload read instead of up to three.
    #[inline(always)]
    fn next_value(&mut self, inp: &mut BitStreamReader) -> io::Result<bool> {
        const CODE_MASK: u64 = (1 << LZ_CODE_BITS) - 1;
        let head = inp.peek_upto(TRAILING_HEADER_BITS as u8);
        let flag = (head >> (LZ_CODE_BITS + 6)) as i32;
        match flag {
            3 => {
                // New window, no trailing zeros: flag + code, then 64 − lz payload bits.
                let lz = LEADING_REPR_DEC[((head >> 6) & CODE_MASK) as usize] as u32;
                inp.skip(PLAIN_HEADER_BITS as u8)?;
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
                // Trailing-zeros case: flag + code + sig(6), then the significant bits.
                let code = (head >> 6) & CODE_MASK;
                let mut lz = LEADING_REPR_DEC[code as usize] as u32;
                let field = (head & 63) as u32;
                let mut sig = if field == 0 { 64 } else { field };
                let mut header_len = TRAILING_HEADER_BITS;
                // lz + sig > 64 is an extended header or corrupt; either way it is resolved
                // before the subtraction below wraps and the shift overflows, and before the
                // window moves. Inline: as an out-of-line call it slowed decode 5–9 % on series
                // that never take it (2026-10-29).
                if sig + lz > 64 {
                    if code != TOP_CODE || field < EXT_FIELD_BASE as u32 {
                        return Err(corrupt_trailing_window());
                    }
                    lz = EXT_FIRST as u32 + field - EXT_FIELD_BASE as u32;
                    sig = (inp.peek_upto(EXT_HEADER_BITS as u8) & 63) as u32;
                    // Width 0 would mean 64, which no extended count leaves room for.
                    if sig == 0 || lz + sig > 64 {
                        return Err(corrupt_trailing_window());
                    }
                    header_len = EXT_HEADER_BITS;
                }
                inp.skip(header_len as u8)?;
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

    /// A `01` header with leading-zero `code` and width field `sig` (0 = 64).
    fn trailing_header(code: u64, sig: u64) -> u64 {
        (0b01 << (LZ_CODE_BITS + 6)) | (code << 6) | sig
    }

    #[test]
    fn corrupt_trailing_window_header_is_rejected() {
        // ELF-free Chimp stream: first value 64 raw bits, then flag 01, the top lz code and
        // sig 0 (=64): lz + 64 > 64 must be an error, not an overflowing shift.
        let mut w = BitStream::new();
        w.write_bits(64, 1.5f64.to_bits()).unwrap();
        w.write_bits(TRAILING_HEADER_BITS, trailing_header(TOP_CODE, 0))
            .unwrap();
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
        // `11`, lz code 2: a 64 − LZ_BUCKETS[2] bit window.
        w.write_bits(PLAIN_HEADER_BITS, (0b11 << LZ_CODE_BITS) | 2)
            .unwrap();
        w.write_bits(64 - LZ_BUCKETS[2] as u32, 1).unwrap();
        // A corrupt `01`: the top lz bucket + sig 64.
        w.write_bits(TRAILING_HEADER_BITS, trailing_header(TOP_CODE, 0))
            .unwrap();
        let mut r = BitStreamReader::new(w.bytes());
        let mut dec = ChimpDec::new();
        dec.read_value(&mut r).unwrap();
        dec.read_value(&mut r).unwrap();
        let before = (dec.stored_width, dec.stored_tz);
        assert!(dec.read_value(&mut r).is_err());
        assert_eq!((dec.stored_width, dec.stored_tz), before);
    }

    /// A counter far above zero: each XOR is a few low bits under ~60 leading zeros. The first
    /// opens an extended window (capped at `EXT_LAST`) instead of padding 36 bits past the top
    /// bucket, and the stream round-trips.
    #[test]
    fn large_slowly_changing_values_open_an_extended_window() {
        let base = (1u64 << 50) as f64;
        let values: Vec<u64> = [0.0, 3.0, 5.0, 6.0, 9.0, 12.0, 14.0]
            .map(|d| (base + d).to_bits())
            .to_vec();
        let mut w = BitStream::new();
        let mut enc = ChimpEnc::new();
        enc.add_value(&mut w, values[0]).unwrap();
        let second = bit_len(&w);
        enc.add_value(&mut w, values[1]).unwrap();
        // 2^50 ^ (2^50 + 3): bits 2 and 3 of the mantissa, 60 leading zeros (capped at the
        // last extended count) and 2 trailing.
        let xor = values[0] ^ values[1];
        assert_eq!((xor.leading_zeros(), xor.trailing_zeros()), (60, 2));
        let head = BitStreamReader::from_bit_position(w.bytes(), second)
            .unwrap()
            .peek_upto(EXT_HEADER_BITS as u8);
        let field = (head >> 6) & 63;
        assert_eq!(head >> 12, (0b01 << LZ_CODE_BITS) | TOP_CODE);
        assert_eq!(field as i32, EXT_FIELD_BASE + EXT_LAST - EXT_FIRST);
        assert_eq!(head & 63, (64 - EXT_LAST - 2) as u64);
        assert_eq!(bit_len(&w) - second, EXT_HEADER_BITS as usize + 8);
        assert_eq!(i32::from(enc.stored_lz), EXT_LAST);
        for v in &values[2..] {
            enc.add_value(&mut w, *v).unwrap();
        }
        let mut r = BitStreamReader::new(w.bytes());
        let mut dec = ChimpDec::new();
        let got: Vec<u64> = values
            .iter()
            .map(|_| dec.read_value(&mut r).unwrap())
            .collect();
        assert_eq!(got, values);
    }

    /// Every leading-zero count from the top bucket up, with every trailing count, takes the
    /// cheapest header and round-trips, through a fresh encoder and after a reload.
    #[test]
    fn every_leading_count_round_trips() {
        for nlz in 0..64u32 {
            for tz in 0..64 - nlz {
                // A one at both ends of the window, so the XOR has exactly nlz/tz zeros.
                let xor = (1u64 << (63 - nlz)) | (1u64 << tz);
                let values = [0x4000_0000_0000_0000u64, 0x4000_0000_0000_0000 ^ xor, 0];
                let mut w = BitStream::new();
                let mut enc = ChimpEnc::new();
                for v in values {
                    enc.add_value(&mut w, v).unwrap();
                    let reloaded = ChimpEnc::from_state(enc.state()).expect("valid state");
                    assert_eq!(reloaded, enc);
                }
                let mut r = BitStreamReader::new(w.bytes());
                let mut dec = ChimpDec::new();
                for v in values {
                    assert_eq!(dec.read_value(&mut r).unwrap(), v, "nlz {nlz} tz {tz}");
                }
            }
        }
    }

    /// Escapes the decoder must reject: width 64 (field 0) under the top code, an impossible
    /// width under any other code, an extended width of 0, and one past 64 bits. None moves
    /// the window.
    #[test]
    fn corrupt_extended_headers_are_rejected() {
        let top = EXT_FIELD_BASE as u64;
        let last = (EXT_FIELD_BASE + EXT_LAST - EXT_FIRST) as u64;
        let cases: [(u64, u64, u32, u64); 4] = [
            (TOP_CODE, 0, TRAILING_HEADER_BITS, 0),
            (TOP_CODE - 1, 63, TRAILING_HEADER_BITS, 0),
            (TOP_CODE, top, EXT_HEADER_BITS, 0),
            (TOP_CODE, last, EXT_HEADER_BITS, (64 - EXT_LAST + 1) as u64),
        ];
        for (code, field, len, sig) in cases {
            let mut w = BitStream::new();
            w.write_bits(64, 1.0f64.to_bits()).unwrap();
            w.write_bits(PLAIN_HEADER_BITS, (0b11 << LZ_CODE_BITS) | 2)
                .unwrap();
            w.write_bits(64 - LZ_BUCKETS[2] as u32, 1).unwrap();
            let head = trailing_header(code, field);
            if len == EXT_HEADER_BITS {
                w.write_bits(len, (head << 6) | sig).unwrap();
            } else {
                w.write_bits(len, head).unwrap();
            }
            w.write_bits(64, 0).unwrap();
            let mut r = BitStreamReader::new(w.bytes());
            let mut dec = ChimpDec::new();
            dec.read_value(&mut r).unwrap();
            dec.read_value(&mut r).unwrap();
            let before = (dec.stored_width, dec.stored_tz);
            let err = dec.read_value(&mut r).unwrap_err();
            assert_eq!(
                err.kind(),
                io::ErrorKind::InvalidData,
                "{code} {field} {sig}"
            );
            assert_eq!((dec.stored_width, dec.stored_tz), before);
        }
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
        for lz in LZ_BUCKETS.into_iter().chain(EXT_FIRST..=EXT_LAST) {
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
            (EXT_FIRST - 1, 0),
            (EXT_LAST + 1, 0),
            (EXT_LAST, 64 - EXT_LAST),
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
