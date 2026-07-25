//! DeXOR: XOR-style float compression carried out in decimal space.
//!
//! A Rust port of the reference Java implementation at
//! <https://github.com/SuDIS-ZJU/DeXOR> (`algorithms.DeXOR`).
//!
//! # How it works
//!
//! Binary XOR coders (Gorilla, Chimp, XOR2) exploit shared *binary* prefixes
//! between neighbouring samples. Real-world sensor readings are decimal by
//! origin — `21.5`, `21.6`, `21.7` — and share a decimal prefix that has no
//! clean binary counterpart. DeXOR works on the decimal representation instead.
//!
//! For each value it derives two numbers:
//!
//! * `q`, the decimal exponent of the value's last significant digit
//!   (`21.53` → `-2`). Consecutive samples in a series almost always agree on
//!   `q`, so it is usually free to transmit.
//! * `delta`, the count of trailing decimal digits by which the value differs
//!   from its predecessor. The shared prefix `alpha = trunc(v / 10^(q+delta))
//!   * 10^(q+delta)` is *not* transmitted — the decoder recomputes it from the
//!   value it already holds — and only the `delta`-digit residual
//!   `beta = (v - alpha) / 10^q` goes on the wire, in `ceil(delta*log2(10))`
//!     bits.
//!
//! # Wire format
//!
//! Every record opens with a two-bit control code:
//!
//! | code | meaning                                            |
//! |------|----------------------------------------------------|
//! | `00` | new `q` (5 bits, biased by 20) and new `delta` (4 bits) |
//! | `01` | `q` unchanged, new `delta` (4 bits)                |
//! | `10` | `q` and `delta` both unchanged                     |
//! | `11` | exception — see [`exponent_coder`]                 |
//!
//! In [`DeXorMode::Buffered`] a dictionary slot id follows the control code.
//! A sign bit follows when — and only when — the shared prefix is zero, since
//! otherwise the prefix already carries the sign. The residual comes last.
//!
//! # Divergences from the reference
//!
//! The reference is a research benchmark; a few of its behaviours are unsafe
//! for a database that must hand back exactly the bits it was given.
//!
//! * **Lossless.** The reference accepts a decimal reconstruction whose error
//!   is under `10^q` — up to a full unit in the last decimal place. This port
//!   requires the reconstruction to match the input bit for bit, and routes
//!   anything else to the exception path. See [`DeXorEncoder`].
//! * **Non-finite values.** `NaN` and the infinities defeat the reference's
//!   epsilon comparisons and silently encode as `0`. Here they take the
//!   exception path, which stores raw bits.
//! * **Range checks.** `q` is transmitted in a biased 5-bit field, and the
//!   power-of-ten table spans `1e-23..=1e23`. The reference checks neither and
//!   corrupts (or panics on) out-of-range magnitudes; this port falls back to
//!   the exception path.
//! * **`EL` contraction.** The reference decoder fails to reset its contraction
//!   counter on an escape once `EL` has saturated, desyncing it from the
//!   encoder. See [`exponent_coder`].
//! * **Last-digit exponent.** The reference's high-precision path misreads
//!   values that `Double.toString` renders in scientific notation. See
//!   [`tools`].
//!
//! # Layers
//!
//! [`DeXorValueEncoder`] / [`DeXorValueDecoder`] are the value codec proper.
//! They borrow the bit stream rather than owning it, so a chunk can interleave
//! timestamps between values:
//!
//! ```ignore
//! let values = [21.5, 21.6, 21.7, 21.65];
//! let bytes = encode_values(&values, DeXorConfig::default());
//! let decoded = decode_values(&bytes, values.len(), DeXorConfig::default())?;
//! assert_eq!(decoded, values);
//! ```
//!
//! [`DeXorEncoder`] and [`DeXorChunk`] build the [`ChunkEncoding::DeXor`]
//! storage format on top: DeXOR values interleaved with Gorilla's
//! delta-of-delta varbit timestamps.
//!
//! [`ChunkEncoding::DeXor`]: crate::series::chunks::ChunkEncoding::DeXor

mod dexor_chunk;
#[cfg(test)]
mod dexor_chunk_tests;
mod dexor_encoder;
mod dexor_iterator;
mod exponent_coder;
mod stream_io;
#[cfg(test)]
mod tests;
mod tools;
mod value_decoder;
mod value_encoder;

pub use dexor_chunk::{DeXorChunk, DeXorChunkIterator};
pub use dexor_encoder::DeXorEncoder;
pub use dexor_iterator::DeXorIterator;
pub use value_decoder::DeXorValueDecoder;
pub use value_encoder::DeXorValueEncoder;

use crate::common::hash::hash_f64;
use crate::series::chunks::stream::bitstream::BitStream;
use crate::series::chunks::stream::bitstream_reader::BitStreamReader;
use get_size2::GetSize;
use std::hash::{Hash, Hasher};
use std::io;

/// Encode `values` into a standalone bit stream.
///
/// Useful for testing and for callers that only want the value codec; the
/// chunk encoder interleaves timestamps instead.
pub fn encode_values(values: &[f64], config: DeXorConfig) -> Vec<u8> {
    let mut encoder = DeXorValueEncoder::new(config);
    let mut out = BitStream::new();
    for &value in values {
        encoder.encode(&mut out, value);
    }
    out.into_bytes()
}

/// Decode exactly `count` values written by [`encode_values`].
///
/// The stream carries no length, so the count has to come from the caller.
pub fn decode_values(bytes: &[u8], count: usize, config: DeXorConfig) -> io::Result<Vec<f64>> {
    let mut decoder = DeXorValueDecoder::new(config);
    let mut input = BitStreamReader::new(bytes);
    let mut values = Vec::with_capacity(count);
    for _ in 0..count {
        values.push(decoder.decode(&mut input)?);
    }
    Ok(values)
}

/// Width of the leading control code.
const CONTROL_BITS: u32 = 2;
/// `00`: `q` and `delta` both follow.
const CONTROL_NEW_Q: u64 = 0;
/// `01`: `q` is unchanged, `delta` follows.
const CONTROL_NEW_DELTA: u64 = 1;
/// `10`: `q` and `delta` are both unchanged.
const CONTROL_REPEAT: u64 = 2;
/// `11`: the decimal path could not represent this value.
const CONTROL_EXCEPTION: u64 = 3;

/// Width of the biased `q` field.
const Q_BITS: u32 = 5;
/// Bias applied to `q` so that the field is unsigned.
const Q_BIAS: i32 = 20;
/// Smallest transmissible last-digit exponent.
const Q_MIN: i32 = -Q_BIAS;
/// Largest transmissible last-digit exponent.
const Q_MAX: i32 = (1 << Q_BITS) - 1 - Q_BIAS;

/// Width of the `delta` field.
const DELTA_BITS: u32 = 4;
/// Exclusive upper bound on `delta`; a wider divergence takes the exception path.
const MAX_DELTA: u32 = 1 << DELTA_BITS;

/// How the encoder picks the value each sample is coded against.
#[derive(Copy, Clone, Debug, Default, PartialEq, Eq, Hash, GetSize)]
pub enum DeXorMode {
    /// Code every sample against its immediate predecessor.
    #[default]
    Native,

    /// Like [`DeXorMode::Native`], but once `after` samples in a row have
    /// fallen through to the exception path the stream gives up on the decimal
    /// path permanently and codes the remainder as raw exceptions.
    ///
    /// This pays off on binary-origin data — simulation output, normalised
    /// floats — where the decimal path never fires and its two control bits per
    /// sample are pure overhead.
    Skippable { after: u32 },

    /// Search a circular window of the last `1 << bits` samples for the one
    /// sharing the longest decimal prefix, and transmit its slot id.
    ///
    /// Helps on interleaved or oscillating series where the best match is not
    /// the immediate predecessor, at a cost of `bits` per sample.
    Buffered { bits: u32 },
}

/// Tuning knobs for a DeXOR stream.
///
/// The encoder and decoder must be constructed with identical configuration;
/// none of it is carried in the bit stream.
#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash, GetSize)]
pub struct DeXorConfig {
    pub mode: DeXorMode,

    /// How many consecutive well-behaved exponent deltas must pass before the
    /// exception coder narrows its delta field by one bit.
    pub rho: u32,
}

impl Default for DeXorConfig {
    fn default() -> Self {
        Self {
            mode: DeXorMode::default(),
            rho: 8,
        }
    }
}

impl DeXorConfig {
    /// Width of the dictionary slot id, in bits. Zero outside
    /// [`DeXorMode::Buffered`].
    #[inline]
    fn slot_bits(&self) -> u32 {
        match self.mode {
            DeXorMode::Buffered { bits } => bits,
            _ => 0,
        }
    }
}

/// The per-stream state that the encoder and decoder must keep in lockstep.
#[derive(Debug, Clone, PartialEq, GetSize)]
struct SharedState {
    config: DeXorConfig,
    /// The value the next sample is coded against in the non-buffered modes.
    previous_value: f64,
    previous_q: i32,
    previous_delta: u32,
    /// Circular dictionary; empty outside [`DeXorMode::Buffered`].
    window: Vec<f64>,
    /// Next slot to overwrite in `window`.
    window_next: usize,
    /// Consecutive exceptions seen so far, for [`DeXorMode::Skippable`].
    exception_run: u32,
    /// Set once [`DeXorMode::Skippable`] has abandoned the decimal path.
    skipping: bool,
}

impl SharedState {
    fn new(config: DeXorConfig) -> Self {
        let window = match config.mode {
            DeXorMode::Buffered { bits } => vec![0.0; 1usize << bits],
            _ => Vec::new(),
        };

        Self {
            config,
            previous_value: 0.0,
            previous_q: 0,
            previous_delta: 0,
            window,
            window_next: 0,
            exception_run: 0,
            skipping: false,
        }
    }

    /// Record a value that was coded on the decimal path.
    #[inline]
    fn accept(&mut self, value: f64) {
        if self.window.is_empty() {
            self.previous_value = value;
        } else {
            self.window[self.window_next] = value;
            self.window_next = (self.window_next + 1) % self.window.len();
        }
        self.exception_run = 0;
    }

    /// Record a value that fell through to the exception path.
    ///
    /// Neither `previous_value` nor the dictionary is updated: an exception
    /// leaves the decimal predictor exactly as it was.
    #[inline]
    fn reject(&mut self) {
        if let DeXorMode::Skippable { after } = self.config.mode {
            self.exception_run += 1;
            if self.exception_run >= after {
                self.skipping = true;
            }
        }
    }
}

impl Hash for SharedState {
    fn hash<H: Hasher>(&self, hasher: &mut H) {
        self.config.hash(hasher);
        hash_f64(self.previous_value, hasher);
        self.previous_q.hash(hasher);
        self.previous_delta.hash(hasher);
        for &value in &self.window {
            hash_f64(value, hasher);
        }
        self.window_next.hash(hasher);
        self.exception_run.hash(hasher);
        self.skipping.hash(hasher);
    }
}

/// The predictor state a paused encoder must carry across persistence in order
/// to keep appending to a stream it did not itself write.
///
/// Only [`DeXorMode::Native`] state is captured: the chunk encoder fixes the
/// mode, so the dictionary and skip counters are always at their defaults.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub(super) struct CodecSnapshot {
    pub previous_value_bits: u64,
    pub previous_q: i32,
    pub previous_delta: u32,
    pub previous_exp: u64,
    pub exponent_len: u32,
    pub contract_step: u32,
}
