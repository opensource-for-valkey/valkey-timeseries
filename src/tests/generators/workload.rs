//! Shape-oriented sample generation.
//!
//! Where [`crate::tests::generators::generator`] produces range-bounded random
//! noise, the helpers here produce recognisable *shapes* (drift, periodic,
//! bursty, counters, ...) together with the timestamp spacing models used to
//! exercise the timestamp compressors. Values are absolute: the generator's
//! value range does not apply to these workloads.

use crate::common::Timestamp;
use crate::common::rounding::round_to_decimal_digits;
use rand::prelude::{IndexedRandom, StdRng};
use rand_distr::{Distribution, Exp, Normal, Poisson, Uniform};

/// Decimal places kept by the `*_quantized` workloads. Two decimals is the
/// precision most collection agents report at, and it clears the low mantissa
/// bits that the XOR family would otherwise have to carry.
pub const QUANTIZED_DECIMALS: u8 = 2;

/// Spacing model applied to generated timestamps.
#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash, Default)]
pub enum TimestampModel {
    /// Fixed interval between samples.
    #[default]
    Regular,
    /// Fixed interval perturbed by +/- 50ms.
    Jitter,
    /// Exponentially distributed gaps with a mean of the sample interval.
    Irregular,
}

impl TimestampModel {
    pub const fn id(self) -> &'static str {
        match self {
            Self::Regular => "regular",
            Self::Jitter => "jitter",
            Self::Irregular => "irregular",
        }
    }

    pub const fn all() -> &'static [TimestampModel] {
        &[Self::Regular, Self::Jitter, Self::Irregular]
    }
}

pub fn generate_timestamps_with_model(
    model: TimestampModel,
    count: usize,
    start: Timestamp,
    interval_millis: i64,
    rng: &mut StdRng,
) -> Vec<Timestamp> {
    let mut timestamps = Vec::with_capacity(count);
    if count == 0 {
        return timestamps;
    }

    let mut ts = start;
    timestamps.push(ts);

    let jitter = Uniform::new_inclusive(-50_i64, 50_i64).expect("valid uniform distribution");
    let irregular = Exp::new(1.0 / interval_millis.max(1) as f64).expect("valid exp lambda");

    for _ in 1..count {
        let delta = match model {
            TimestampModel::Regular => interval_millis.max(1),
            TimestampModel::Jitter => (interval_millis + jitter.sample(rng)).max(1),
            TimestampModel::Irregular => irregular.sample(rng).round().max(1.0) as i64,
        };
        ts += delta;
        timestamps.push(ts);
    }
    timestamps
}

/// A value that never changes.
pub fn constant_values(count: usize) -> Vec<f64> {
    vec![42.0; count]
}

/// A value that never changes and is integral.
pub fn constant_int_values(count: usize) -> Vec<f64> {
    vec![1000.0; count]
}

/// A slowly wandering value bounded to a narrow band.
pub fn drift_values(count: usize, rng: &mut StdRng) -> Vec<f64> {
    let noise = Normal::new(0.0, 0.01).expect("valid normal");
    let mut value = 100.0;
    let min: f64 = 95.0;
    let max: f64 = 105.0;
    let mut out = Vec::with_capacity(count);
    for _ in 0..count {
        let next: f64 = value + noise.sample(rng);
        value = next.clamp(min, max);
        out.push(value);
    }
    out
}

/// Alternating sine and sawtooth segments with a little noise.
pub fn periodic_values(count: usize, rng: &mut StdRng) -> Vec<f64> {
    let amplitude = 50.0;
    let period = 3600.0;
    let noise = Normal::new(0.0, amplitude / 100.0).expect("valid normal");
    let sawtooth_every = 5_000;
    let mut out = Vec::with_capacity(count);
    for idx in 0..count {
        let base = if (idx / sawtooth_every) % 2 == 0 {
            (2.0 * std::f64::consts::PI * (idx as f64) / period).sin() * amplitude
        } else {
            let pos = (idx % 512) as f64 / 512.0;
            (2.0 * pos - 1.0) * amplitude
        };
        out.push(base + noise.sample(rng));
    }
    out
}

/// Normally distributed values with a wide spread.
pub fn noisy_values(count: usize, rng: &mut StdRng) -> Vec<f64> {
    let dist = Normal::new(100.0, 25.0).expect("valid normal");
    (0..count).map(|_| dist.sample(rng)).collect()
}

/// Long quiet stretches punctuated by short high-amplitude bursts.
pub fn bursty_values(count: usize, rng: &mut StdRng) -> Vec<f64> {
    let quiet_noise = Normal::new(0.0, 0.01).expect("valid normal");
    let burst_noise = Normal::new(0.0, 20.0).expect("valid normal");
    let burst_length =
        Uniform::new_inclusive(200_usize, 500_usize).expect("valid uniform distribution");
    let mut out = Vec::with_capacity(count);
    let mut value: f64 = 100.0;
    let mut idx = 0;

    while idx < count {
        let quiet_segment = ((count - idx) as f64 * 0.8).round() as usize;
        let quiet_segment = quiet_segment.clamp(50, 4_000).min(count - idx);
        for _ in 0..quiet_segment {
            value = (value + quiet_noise.sample(rng)).clamp(95.0, 105.0);
            out.push(value);
        }
        idx += quiet_segment;
        if idx >= count {
            break;
        }

        let len = burst_length.sample(rng).min(count - idx);
        for _ in 0..len {
            out.push((value * 10.0) + burst_noise.sample(rng));
        }
        idx += len;
    }

    out
}

/// A monotonically increasing counter that periodically resets to zero.
pub fn counter_values(count: usize, rng: &mut StdRng) -> Vec<f64> {
    let inc = Poisson::new(10.0).expect("valid poisson");
    let reset_every = 50_000;
    let mut value = 0.0;
    let mut out = Vec::with_capacity(count);
    for i in 0..count {
        if i > 0 && i % reset_every == 0 {
            value = 0.0;
        }
        value += inc.sample(rng);
        out.push(value);
    }
    out
}

/// Values drawn from a small fixed set.
pub fn discrete_values(count: usize, rng: &mut StdRng) -> Vec<f64> {
    let choices = [0.0, 0.25, 0.5, 1.0];
    (0..count)
        .map(|_| *choices.choose(rng).expect("choices non-empty"))
        .collect()
}

/// Round every value to [`QUANTIZED_DECIMALS`] decimal places.
fn quantize(mut values: Vec<f64>) -> Vec<f64> {
    for value in values.iter_mut() {
        *value = round_to_decimal_digits(*value, QUANTIZED_DECIMALS);
    }
    values
}

/// [`drift_values`] reported at two decimal places.
pub fn drift_quantized_values(count: usize, rng: &mut StdRng) -> Vec<f64> {
    quantize(drift_values(count, rng))
}

/// [`periodic_values`] reported at two decimal places.
pub fn periodic_quantized_values(count: usize, rng: &mut StdRng) -> Vec<f64> {
    quantize(periodic_values(count, rng))
}

/// [`noisy_values`] reported at two decimal places.
pub fn noisy_quantized_values(count: usize, rng: &mut StdRng) -> Vec<f64> {
    quantize(noisy_values(count, rng))
}

/// [`bursty_values`] reported at two decimal places.
pub fn bursty_quantized_values(count: usize, rng: &mut StdRng) -> Vec<f64> {
    quantize(bursty_values(count, rng))
}
