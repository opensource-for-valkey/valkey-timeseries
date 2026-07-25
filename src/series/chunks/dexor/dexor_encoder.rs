//! DeXOR encoder.
//!
//! Port of `algorithms.DeXOR.encoder.DoubleDeXOREncoder`, with the three
//! reference strategies (`Native`, `Skippable`, `Buffered`) collapsed into
//! [`DeXorMode`] rather than a subclass hierarchy.

use super::exponent_coder::ExponentCoder;
use super::stream_io::write_bits;
use super::tools::{approx_zero, decimal_bits, last_digit_exponent, p10, truncate};
use super::{
    CONTROL_BITS, CONTROL_EXCEPTION, CONTROL_NEW_DELTA, CONTROL_NEW_Q, CONTROL_REPEAT, DELTA_BITS,
    DeXorConfig, MAX_DELTA, Q_BIAS, Q_BITS, Q_MAX, Q_MIN, SharedState,
};
use crate::series::chunks::stream::bitstream::BitStream;

/// A decimal encoding the encoder has committed to for one sample.
struct Plan {
    q: i32,
    delta: u32,
    /// The shared decimal prefix, which the decoder recomputes rather than reads.
    alpha: f64,
    /// Magnitude of the residual, in units of `10^q`.
    beta: u64,
    /// Dictionary slot the prefix came from; always 0 outside `Buffered` mode.
    slot: usize,
}

#[derive(Debug, Clone)]
pub struct DeXorEncoder {
    out: BitStream,
    exceptions: ExponentCoder,
    state: SharedState,
    count: usize,
}

impl Default for DeXorEncoder {
    fn default() -> Self {
        Self::new(DeXorConfig::default())
    }
}

impl DeXorEncoder {
    pub fn new(config: DeXorConfig) -> Self {
        Self {
            out: BitStream::new(),
            exceptions: ExponentCoder::new(config.rho),
            state: SharedState::new(config),
            count: 0,
        }
    }

    /// Encode a whole slice in one call.
    pub fn encode_slice(values: &[f64], config: DeXorConfig) -> Vec<u8> {
        let mut encoder = Self::new(config);
        for &value in values {
            encoder.push(value);
        }
        encoder.finish()
    }

    /// Number of samples written so far.
    pub fn len(&self) -> usize {
        self.count
    }

    pub fn is_empty(&self) -> bool {
        self.count == 0
    }

    /// The bytes produced so far. The final byte may be partially filled; the
    /// stream is not self-terminating, so a decoder needs the sample count.
    pub fn bytes(&self) -> &[u8] {
        self.out.bytes()
    }

    pub fn finish(self) -> Vec<u8> {
        self.out.into_bytes()
    }

    pub fn clear(&mut self) {
        let config = self.state.config;
        self.out.clear();
        self.exceptions = ExponentCoder::new(config.rho);
        self.state = SharedState::new(config);
        self.count = 0;
    }

    /// Append one sample.
    pub fn push(&mut self, value: f64) {
        self.count += 1;

        if self.state.skipping {
            self.exceptions.encode(&mut self.out, value);
            return;
        }

        match self.plan(value) {
            Some(plan) => self.write_decimal(value, plan),
            None => {
                write_bits(&mut self.out, CONTROL_BITS, CONTROL_EXCEPTION);
                self.exceptions.encode(&mut self.out, value);
                self.state.reject();
            }
        }
    }

    /// Decide how `value` can be coded on the decimal path, or `None` if it
    /// cannot be coded there at all.
    ///
    /// The final check is the important one: it replays the decoder's exact
    /// floating-point arithmetic and rejects anything that would not come back
    /// bit-identical. The reference instead accepts any reconstruction within
    /// `10^q` of the input, which makes it lossy in the last decimal place.
    fn plan(&self, value: f64) -> Option<Plan> {
        if !value.is_finite() {
            return None;
        }

        let q = last_digit_exponent(value, self.state.previous_q)?;
        if !(Q_MIN..=Q_MAX).contains(&q) {
            return None;
        }

        let (delta, alpha, slot) = if self.state.window.is_empty() {
            let (delta, alpha) = shared_prefix(value, self.state.previous_value, q)?;
            (delta, alpha, 0)
        } else {
            self.search_window(value, q)?
        };

        let pow_q = p10(q)?;
        let scaled = (value - alpha) / pow_q;
        if !scaled.is_finite() {
            return None;
        }

        let beta = scaled.round().abs();
        // Keep the cast below well-defined; the field-width check that follows
        // is the one that actually binds.
        if beta >= u64::MAX as f64 {
            return None;
        }
        let beta = beta as u64;

        let width = decimal_bits(delta);
        debug_assert!(width < 64, "delta < 16 bounds the residual to 50 bits");
        if beta >> width != 0 {
            return None;
        }

        // Replay the decoder.
        let sign: i64 = if approx_zero(alpha) {
            if value > 0.0 { 1 } else { -1 }
        } else if alpha > 0.0 {
            1
        } else {
            -1
        };
        let reconstructed = alpha + ((sign * beta as i64) as f64) * pow_q;
        if reconstructed.to_bits() != value.to_bits() {
            return None;
        }

        Some(Plan {
            q,
            delta,
            alpha,
            beta,
            slot,
        })
    }

    /// [`DeXorMode::Buffered`] prefix search: start from the window's first
    /// slot, then walk the rest looking for a slot that shares strictly more
    /// decimal digits.
    ///
    /// [`DeXorMode::Buffered`]: super::DeXorMode::Buffered
    fn search_window(&self, value: f64, q: i32) -> Option<(u32, f64, usize)> {
        let window = &self.state.window;

        let (mut delta, mut alpha) = shared_prefix(value, window[0], q).unwrap_or((MAX_DELTA, 0.0));
        let mut slot = 0usize;

        let mut pow = p10(q + delta as i32 - 1)?;
        let mut a = truncate(value / pow);

        for (index, &candidate) in window.iter().enumerate().skip(1) {
            if delta == 0 {
                break;
            }
            let mut b = truncate(candidate / pow);
            while a == b {
                alpha = a as f64 * pow;
                slot = index;
                delta -= 1;
                if delta == 0 {
                    break;
                }
                pow = p10(q + delta as i32 - 1)?;
                a = truncate(value / pow);
                b = truncate(candidate / pow);
            }
        }

        (delta < MAX_DELTA).then_some((delta, alpha, slot))
    }

    fn write_decimal(&mut self, value: f64, plan: Plan) {
        let same_q = plan.q == self.state.previous_q;
        let repeat = same_q && plan.delta == self.state.previous_delta;

        let control = if repeat {
            CONTROL_REPEAT
        } else if same_q {
            CONTROL_NEW_DELTA
        } else {
            CONTROL_NEW_Q
        };
        write_bits(&mut self.out, CONTROL_BITS, control);
        write_bits(
            &mut self.out,
            self.state.config.slot_bits(),
            plan.slot as u64,
        );

        if !repeat {
            if !same_q {
                write_bits(&mut self.out, Q_BITS, (plan.q + Q_BIAS) as u64);
                self.state.previous_q = plan.q;
            }
            write_bits(&mut self.out, DELTA_BITS, u64::from(plan.delta));
            self.state.previous_delta = plan.delta;
        }

        // The prefix carries the sign unless it is zero.
        if approx_zero(plan.alpha) {
            self.out.write_bit(value > 0.0);
        }

        write_bits(&mut self.out, decimal_bits(plan.delta), plan.beta);
        self.state.accept(value);
    }
}

/// Shortest `delta` for which `value` and `reference` truncate to the same
/// multiple of `10^(q + delta)`, together with that shared prefix.
fn shared_prefix(value: f64, reference: f64, q: i32) -> Option<(u32, f64)> {
    for delta in 0..MAX_DELTA {
        let pow = p10(q + delta as i32)?;
        let a = truncate(value / pow);
        if a == truncate(reference / pow) {
            return Some((delta, a as f64 * pow));
        }
    }
    None
}
