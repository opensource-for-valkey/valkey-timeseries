//! The DeXOR "exception" path: a bit-exact fallback coder for values the
//! decimal path cannot represent.
//!
//! Instead of emitting a bare 64-bit double, the fallback delta-codes the IEEE
//! exponent field against the previous exception's exponent and writes the sign
//! and mantissa verbatim. The width of the delta field (`EL`) adapts: it grows
//! on every overflow and shrinks again after `rho` consecutive deltas that
//! would have fit in one bit less.
//!
//! Encoder and decoder must evolve `EL` identically or the streams desync, so
//! both drive the single state machine in this module.

use super::stream_io::{read_bits, write_bits};
use crate::series::chunks::stream::bitstream::BitStream;
use crate::series::chunks::stream::bitstream_reader::BitStreamReader;
use get_size2::GetSize;
use std::io;

/// IEEE-754 exponent of `1.0`; the reference seeds its predictor with this.
const INITIAL_EXPONENT: u64 = 1023;

/// Widest delta field. Caps growth so a pathological series cannot spend more
/// bits on the prefix than the 64-bit escape it guards.
const MAX_EXPONENT_LEN: u32 = 10;

const EXPONENT_MASK: u64 = 0x7FF;
const MANTISSA_MASK: u64 = (1u64 << 52) - 1;

#[derive(Debug, Clone, PartialEq, Eq, Hash, GetSize)]
pub(super) struct ExponentCoder {
    previous_exp: u64,
    /// Current width, in bits, of the exponent-delta field.
    len: u32,
    /// How many consecutive deltas would have fit in `len - 1` bits.
    contract_step: u32,
    rho: u32,
}

impl ExponentCoder {
    pub(super) fn new(rho: u32) -> Self {
        Self {
            previous_exp: INITIAL_EXPONENT,
            len: 1,
            contract_step: 0,
            rho,
        }
    }

    /// `(previous_exp, len, contract_step)` — everything a resumed encoder needs.
    pub(super) fn snapshot(&self) -> (u64, u32, u32) {
        (self.previous_exp, self.len, self.contract_step)
    }

    /// Rebuild from a snapshot, rejecting values that would make `bias()`
    /// shift out of range. Persisted state is untrusted input.
    pub(super) fn restore(
        rho: u32,
        previous_exp: u64,
        len: u32,
        contract_step: u32,
    ) -> Option<Self> {
        if previous_exp > EXPONENT_MASK
            || !(1..=MAX_EXPONENT_LEN).contains(&len)
            || contract_step > rho
        {
            return None;
        }
        Some(Self {
            previous_exp,
            len,
            contract_step,
            rho,
        })
    }

    /// Largest magnitude a delta field of the current width can carry. The
    /// remaining code point, `2^len - 1`, is the escape.
    #[inline]
    fn bias(&self) -> i64 {
        (1i64 << (self.len - 1)) - 1
    }

    /// Field-width update applied after a delta-coded exponent.
    fn on_delta(&mut self, delta: i64) {
        if self.len <= 1 {
            return;
        }
        let narrow_bias = (1i64 << (self.len - 2)) - 1;
        if (-narrow_bias..=narrow_bias).contains(&delta) {
            self.contract_step += 1;
        } else {
            self.contract_step = 0;
        }
        if self.contract_step == self.rho {
            self.len -= 1;
            self.contract_step = 0;
        }
    }

    /// Field-width update applied after a 64-bit escape.
    ///
    /// The reference decoder resets `contract_step` only inside its `EL < 10`
    /// branch while its encoder resets unconditionally, so the two desync once
    /// `EL` saturates. This follows the encoder, which is the side that decides
    /// the layout.
    fn on_escape(&mut self) {
        self.contract_step = 0;
        if self.len < MAX_EXPONENT_LEN {
            self.len += 1;
        }
    }

    pub(super) fn encode(&mut self, out: &mut BitStream, value: f64) {
        let bits = value.to_bits();
        let exp = ((bits >> 52) & EXPONENT_MASK) as i64;
        let delta = exp - self.previous_exp as i64;
        let bias = self.bias();

        if (-bias..=bias).contains(&delta) {
            write_bits(out, self.len, (delta + bias) as u64);
            out.write_bit((bits >> 63) == 1);
            write_bits(out, 52, bits & MANTISSA_MASK);
            self.on_delta(delta);
        } else {
            write_bits(out, self.len, (1u64 << self.len) - 1);
            write_bits(out, 64, bits);
            self.on_escape();
        }

        self.previous_exp = exp as u64;
    }

    pub(super) fn decode(&mut self, input: &mut BitStreamReader) -> io::Result<f64> {
        let bias = self.bias();
        let delta = read_bits(input, self.len)? as i64 - bias;

        if (-bias..=bias).contains(&delta) {
            let exp = (self.previous_exp as i64 + delta) as u64 & EXPONENT_MASK;
            self.previous_exp = exp;
            let sign = u64::from(input.read_bit()?);
            let mantissa = read_bits(input, 52)?;
            self.on_delta(delta);
            Ok(f64::from_bits((sign << 63) | (exp << 52) | mantissa))
        } else {
            let bits = read_bits(input, 64)?;
            self.previous_exp = (bits >> 52) & EXPONENT_MASK;
            self.on_escape();
            Ok(f64::from_bits(bits))
        }
    }
}
