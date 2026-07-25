//! DeXOR decoder.
//!
//! Port of `algorithms.DeXOR.decoder.DoubleDeXORDecoder`.

use super::exponent_coder::ExponentCoder;
use super::stream_io::read_bits;
use super::tools::{approx_zero, decimal_bits, p10, truncate};
use super::{
    CONTROL_BITS, CONTROL_EXCEPTION, CONTROL_NEW_Q, CONTROL_REPEAT, DELTA_BITS, DeXorConfig,
    Q_BIAS, Q_BITS, SharedState,
};
use crate::series::chunks::stream::bitstream_reader::BitStreamReader;
use std::io;

#[derive(Debug, Clone)]
pub struct DeXorDecoder<'a> {
    input: BitStreamReader<'a>,
    exceptions: ExponentCoder,
    state: SharedState,
    /// Shared decimal prefix of the last decoded sample.
    ///
    /// Under a `10` control code neither `q` nor `delta` changed, so the prefix
    /// is carried over rather than recomputed — which is only sound because the
    /// run of `10` codes is, by construction, preceded by a `00`/`01` code with
    /// the same `q` and `delta`.
    alpha: f64,
}

impl<'a> DeXorDecoder<'a> {
    pub fn new(bytes: &'a [u8], config: DeXorConfig) -> Self {
        Self {
            input: BitStreamReader::new(bytes),
            exceptions: ExponentCoder::new(config.rho),
            state: SharedState::new(config),
            alpha: 0.0,
        }
    }

    /// Decode exactly `count` samples. The stream carries no length, so the
    /// count has to come from the caller.
    pub fn decode_all(bytes: &[u8], count: usize, config: DeXorConfig) -> io::Result<Vec<f64>> {
        let mut decoder = DeXorDecoder::new(bytes, config);
        let mut values = Vec::with_capacity(count);
        for _ in 0..count {
            values.push(decoder.next_value()?);
        }
        Ok(values)
    }

    pub fn next_value(&mut self) -> io::Result<f64> {
        if self.state.skipping {
            return self.exceptions.decode(&mut self.input);
        }

        let control = read_bits(&mut self.input, CONTROL_BITS)?;
        if control == CONTROL_EXCEPTION {
            self.state.reject();
            return self.exceptions.decode(&mut self.input);
        }

        let slot = read_bits(&mut self.input, self.state.config.slot_bits())? as usize;

        if control == CONTROL_NEW_Q {
            self.state.previous_q = read_bits(&mut self.input, Q_BITS)? as i32 - Q_BIAS;
        }
        if control != CONTROL_REPEAT {
            self.state.previous_delta = read_bits(&mut self.input, DELTA_BITS)? as u32;
        }

        let buffered = !self.state.window.is_empty();
        if buffered || control != CONTROL_REPEAT {
            let reference = if buffered {
                *self
                    .state
                    .window
                    .get(slot)
                    .ok_or_else(|| corrupt("dictionary slot out of range"))?
            } else {
                self.state.previous_value
            };
            let pow = p10(self.state.previous_q + self.state.previous_delta as i32)
                .ok_or_else(|| corrupt("decimal exponent out of range"))?;
            self.alpha = truncate(reference / pow) as f64 * pow;
        }

        let sign: i64 = if approx_zero(self.alpha) {
            if self.input.read_bit()? { 1 } else { -1 }
        } else if self.alpha > 0.0 {
            1
        } else {
            -1
        };

        let magnitude = read_bits(&mut self.input, decimal_bits(self.state.previous_delta))?;
        let pow_q =
            p10(self.state.previous_q).ok_or_else(|| corrupt("decimal exponent out of range"))?;

        let value = self.alpha + ((sign * magnitude as i64) as f64) * pow_q;
        self.state.accept(value);

        Ok(value)
    }
}

fn corrupt(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}
