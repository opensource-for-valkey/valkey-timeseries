use crate::common::rounding::{round_to_decimal_digits, round_to_sig_figs};
use crate::common::time::current_time_millis;
use crate::common::{Sample, Timestamp};
use crate::tests::generators::create_rng;
use crate::tests::generators::generator::{
    DerivativeGenerator, MackeyGlassGenerator, StdNormalGenerator, UniformGenerator,
};
use crate::tests::generators::workload::{
    TimestampModel, bursty_quantized_values, bursty_values, constant_int_values, constant_values,
    counter_values, discrete_values, drift_quantized_values, drift_values,
    generate_timestamps_with_model, noisy_quantized_values, noisy_values,
    periodic_quantized_values, periodic_values,
};
use bon::bon;
use rand::prelude::StdRng;
use std::ops::Range;
use std::time::Duration;

/// Default start timestamp used when none is supplied (2023-11-14T22:13:20Z).
pub const DEFAULT_START_TS: Timestamp = 1_700_000_000_000;

#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash, Default)]
pub enum ValueWorkload {
    #[default]
    Uniform,
    StdNorm,
    MackeyGlass,
    Deriv,
    /// Constant floating point value.
    Constant,
    /// Constant integral value.
    ConstantInt,
    /// Slow random walk within a narrow band.
    Drift,
    /// [`Self::Drift`] rounded to two decimal places.
    DriftQuantized,
    /// Alternating sine / sawtooth segments.
    Periodic,
    /// [`Self::Periodic`] rounded to two decimal places.
    PeriodicQuantized,
    /// Wide-spread gaussian noise.
    Noisy,
    /// [`Self::Noisy`] rounded to two decimal places.
    NoisyQuantized,
    /// Quiet stretches punctuated by bursts.
    Bursty,
    /// [`Self::Bursty`] rounded to two decimal places.
    BurstyQuantized,
    /// Monotonically increasing counter with resets.
    Counter,
    /// Values drawn from a small fixed set.
    Discrete,
}

impl ValueWorkload {
    pub const fn id(self) -> &'static str {
        match self {
            Self::Uniform => "uniform",
            Self::StdNorm => "std_norm",
            Self::MackeyGlass => "mackey_glass",
            Self::Deriv => "deriv",
            Self::Constant => "constant",
            Self::ConstantInt => "constant_int",
            Self::Drift => "drift",
            Self::DriftQuantized => "drift_q2",
            Self::Periodic => "periodic",
            Self::PeriodicQuantized => "periodic_q2",
            Self::Noisy => "noisy",
            Self::NoisyQuantized => "noisy_q2",
            Self::Bursty => "bursty",
            Self::BurstyQuantized => "bursty_q2",
            Self::Counter => "counter",
            Self::Discrete => "discrete",
        }
    }

    pub const fn all() -> &'static [ValueWorkload] {
        &[
            Self::Uniform,
            Self::StdNorm,
            Self::MackeyGlass,
            Self::Deriv,
            Self::Constant,
            Self::ConstantInt,
            Self::Drift,
            Self::DriftQuantized,
            Self::Periodic,
            Self::PeriodicQuantized,
            Self::Noisy,
            Self::NoisyQuantized,
            Self::Bursty,
            Self::BurstyQuantized,
            Self::Counter,
            Self::Discrete,
        ]
    }

    /// The shape-oriented workloads, in declaration order. This order drives
    /// report row ordering only; dataset seeds are derived from the key, not
    /// from a position here.
    pub const fn workloads() -> &'static [ValueWorkload] {
        &[
            Self::Constant,
            Self::ConstantInt,
            Self::Drift,
            Self::DriftQuantized,
            Self::Periodic,
            Self::PeriodicQuantized,
            Self::Noisy,
            Self::NoisyQuantized,
            Self::Bursty,
            Self::BurstyQuantized,
            Self::Counter,
            Self::Discrete,
        ]
    }

    /// The decimal-quantized counterpart of a full-precision shape, if it has
    /// one.
    pub const fn quantized(self) -> Option<ValueWorkload> {
        match self {
            Self::Drift => Some(Self::DriftQuantized),
            Self::Periodic => Some(Self::PeriodicQuantized),
            Self::Noisy => Some(Self::NoisyQuantized),
            Self::Bursty => Some(Self::BurstyQuantized),
            _ => None,
        }
    }

    /// Whether this workload rounds its values to a fixed decimal precision.
    pub const fn is_quantized(self) -> bool {
        matches!(
            self,
            Self::DriftQuantized
                | Self::PeriodicQuantized
                | Self::NoisyQuantized
                | Self::BurstyQuantized
        )
    }

    /// Shape-oriented workloads ignore the generator's value range and are
    /// produced in bulk rather than as an unbounded iterator.
    pub const fn is_workload(self) -> bool {
        !matches!(
            self,
            Self::Uniform | Self::StdNorm | Self::MackeyGlass | Self::Deriv
        )
    }
}

/// GeneratorOptions contains the parameters for generating random time series data.
#[derive(Debug, Clone)]
pub struct DataGenerator {
    start: Timestamp,
    end: Option<Timestamp>,
    /// Interval between samples.
    interval: Option<Duration>,
    /// Range of values.
    values: Range<f64>,
    /// Number of samples.
    samples: usize,
    /// Seed for random number generator.
    seed: Option<u64>,
    /// Type of random number generator.
    pub typ: ValueWorkload,
    /// Spacing model applied to generated timestamps.
    pub timestamp_model: TimestampModel,
    /// Number of significant digits.
    pub significant_digits: Option<usize>,
    /// Number of decimal digits.
    pub decimal_digits: Option<usize>,
}

#[bon]
impl DataGenerator {
    #[builder]
    pub fn new(
        start: Timestamp,
        end: Option<Timestamp>,
        interval: Option<Duration>,
        #[builder(default = 0.0..1.0)] values: Range<f64>,
        samples: usize,
        seed: Option<u64>,
        #[builder(default = ValueWorkload::StdNorm)] algorithm: ValueWorkload,
        #[builder(default)] timestamp_model: TimestampModel,
        significant_digits: Option<usize>,
        decimal_digits: Option<usize>,
    ) -> Self {
        let mut res = DataGenerator {
            start,
            end,
            samples,
            seed,
            interval,
            values,
            typ: algorithm,
            timestamp_model,
            significant_digits,
            decimal_digits,
        };
        res.fixup();
        res
    }

    fn fixup(&mut self) {
        if self.samples == 0 {
            self.samples = 10;
        }
        if let Some(end) = self.end
            && end < self.start
        {
            // swap start and end
            let tmp = self.start;
            self.start = end;
            self.end = Some(tmp);
        }
        if self.values.is_empty() {
            self.values = 0.0..1.0;
        }
        self.start = self.start / 10 * 10;
    }

    pub fn generate(&self) -> Vec<Sample> {
        generate_series_data(self)
    }

    /// Generate a deterministic dataset of the given shape, starting at
    /// [`DEFAULT_START_TS`] and sampled once per second.
    pub fn dataset(
        algorithm: ValueWorkload,
        timestamp_model: TimestampModel,
        samples: usize,
        seed: u64,
    ) -> Vec<Sample> {
        DataGenerator::builder()
            .start(DEFAULT_START_TS)
            .interval(Duration::from_secs(1))
            .samples(samples)
            .seed(seed)
            .algorithm(algorithm)
            .timestamp_model(timestamp_model)
            .build()
            .generate()
    }
}

const ONE_DAY_IN_SECS: u64 = 24 * 60 * 60;

impl Default for DataGenerator {
    fn default() -> Self {
        let now = current_time_millis() / 10 * 10;
        let start = now - Duration::from_secs(ONE_DAY_IN_SECS).as_millis() as i64;
        let mut res = Self {
            start,
            end: None,
            interval: None,
            values: 0.0..1.0,
            samples: 100,
            seed: None,
            typ: ValueWorkload::StdNorm,
            timestamp_model: TimestampModel::Regular,
            significant_digits: None,
            decimal_digits: None,
        };
        res.fixup();
        res
    }
}

fn get_generator_impl(
    typ: ValueWorkload,
    seed: Option<u64>,
    range: &Range<f64>,
) -> Box<dyn Iterator<Item = f64>> {
    match typ {
        ValueWorkload::Uniform => Box::new(UniformGenerator::new(seed, range)),
        ValueWorkload::StdNorm => Box::new(StdNormalGenerator::new(seed, range)),
        ValueWorkload::Deriv => Box::new(DerivativeGenerator::new(seed, range)),
        ValueWorkload::MackeyGlass => Box::new(MackeyGlassGenerator::new(17, seed, range)),
        _ => unreachable!("{:?} is a workload, not an iterator generator", typ),
    }
}

fn generate_values(options: &DataGenerator, rng: &mut StdRng) -> Vec<f64> {
    match options.typ {
        ValueWorkload::Constant => constant_values(options.samples),
        ValueWorkload::ConstantInt => constant_int_values(options.samples),
        ValueWorkload::Drift => drift_values(options.samples, rng),
        ValueWorkload::DriftQuantized => drift_quantized_values(options.samples, rng),
        ValueWorkload::Periodic => periodic_values(options.samples, rng),
        ValueWorkload::PeriodicQuantized => periodic_quantized_values(options.samples, rng),
        ValueWorkload::Noisy => noisy_values(options.samples, rng),
        ValueWorkload::NoisyQuantized => noisy_quantized_values(options.samples, rng),
        ValueWorkload::Bursty => bursty_values(options.samples, rng),
        ValueWorkload::BurstyQuantized => bursty_quantized_values(options.samples, rng),
        ValueWorkload::Counter => counter_values(options.samples, rng),
        ValueWorkload::Discrete => discrete_values(options.samples, rng),
        _ => get_generator_impl(options.typ, options.seed, &options.values)
            .take(options.samples)
            .collect(),
    }
}

// Generates time series data from the given type.
pub fn generate_series_data(options: &DataGenerator) -> Vec<Sample> {
    let interval = if let Some(interval) = options.interval {
        interval.as_millis() as i64
    } else {
        let end = if let Some(end) = options.end {
            end
        } else {
            options.start + (options.samples * 1000 * 60) as i64
        };
        (end - options.start) / options.samples as i64
    };

    let mut rng = create_rng(options.seed);
    let timestamps = generate_timestamps_with_model(
        options.timestamp_model,
        options.samples,
        options.start,
        interval,
        &mut rng,
    );
    let mut values = generate_values(options, &mut rng);

    if let Some(significant_digits) = options.significant_digits {
        for v in values.iter_mut() {
            let rounded = round_to_sig_figs(*v, significant_digits as u8);
            *v = rounded;
        }
    } else if let Some(decimal_digits) = options.decimal_digits {
        for v in values.iter_mut() {
            let rounded = round_to_decimal_digits(*v, decimal_digits as u8);
            *v = rounded;
        }
    }

    timestamps
        .iter()
        .cloned()
        .zip(values.iter().cloned())
        .map(|(timestamp, value)| Sample { timestamp, value })
        .collect::<Vec<Sample>>()
}

pub fn generate_timestamps(count: usize, start: Timestamp, interval: Duration) -> Vec<Timestamp> {
    let interval_millis = interval.as_millis() as i64;
    let mut res = Vec::with_capacity(count);
    let mut t = start;

    for _ in 0..count {
        res.push(t);
        t += interval_millis;
    }
    res
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tests::generators::workload::QUANTIZED_DECIMALS;

    const SAMPLE_COUNT: usize = 2_048;

    #[test]
    fn test_workloads_and_timestamp_models() {
        for algo in ValueWorkload::all() {
            for model in TimestampModel::all() {
                let samples = DataGenerator::dataset(*algo, *model, SAMPLE_COUNT, 42);
                assert_eq!(
                    samples.len(),
                    SAMPLE_COUNT,
                    "{}/{} sample count",
                    algo.id(),
                    model.id()
                );
                assert_eq!(samples[0].timestamp, DEFAULT_START_TS);
                for pair in samples.windows(2) {
                    assert!(
                        pair[1].timestamp > pair[0].timestamp,
                        "{}/{} timestamps must be strictly increasing",
                        algo.id(),
                        model.id()
                    );
                    assert!(
                        pair[0].value.is_finite(),
                        "{}/{} produced a non-finite value",
                        algo.id(),
                        model.id()
                    );
                }
            }
        }
    }

    #[test]
    fn test_regular_model_uses_fixed_interval() {
        let samples = DataGenerator::dataset(ValueWorkload::Drift, TimestampModel::Regular, 100, 7);
        for pair in samples.windows(2) {
            assert_eq!(pair[1].timestamp - pair[0].timestamp, 1_000);
        }
    }

    #[test]
    fn test_seed_is_deterministic() {
        let first =
            DataGenerator::dataset(ValueWorkload::Bursty, TimestampModel::Jitter, 1_000, 99);
        let second =
            DataGenerator::dataset(ValueWorkload::Bursty, TimestampModel::Jitter, 1_000, 99);
        assert_eq!(first, second);
    }

    #[test]
    fn test_quantized_workloads_round_to_two_decimals() {
        for workload in ValueWorkload::workloads()
            .iter()
            .filter(|w| w.is_quantized())
        {
            let samples =
                DataGenerator::dataset(*workload, TimestampModel::Regular, SAMPLE_COUNT, 5);
            for sample in &samples {
                assert_eq!(
                    sample.value,
                    round_to_decimal_digits(sample.value, QUANTIZED_DECIMALS),
                    "{} produced a value below the quantization step",
                    workload.id()
                );
            }
        }
    }

    #[test]
    fn test_quantized_workloads_track_their_base_shape() {
        for base in ValueWorkload::workloads() {
            let Some(quantized) = base.quantized() else {
                continue;
            };
            let plain = DataGenerator::dataset(*base, TimestampModel::Regular, SAMPLE_COUNT, 11);
            let rounded =
                DataGenerator::dataset(quantized, TimestampModel::Regular, SAMPLE_COUNT, 11);
            assert_eq!(plain.len(), rounded.len());
            for (p, q) in plain.iter().zip(rounded.iter()) {
                assert_eq!(p.timestamp, q.timestamp);
                assert_eq!(
                    round_to_decimal_digits(p.value, QUANTIZED_DECIMALS),
                    q.value,
                    "{} must be {} rounded to {QUANTIZED_DECIMALS} decimals",
                    quantized.id(),
                    base.id()
                );
            }
        }
    }

    #[test]
    fn test_constant_workloads() {
        let samples =
            DataGenerator::dataset(ValueWorkload::Constant, TimestampModel::Regular, 64, 1);
        assert!(samples.iter().all(|s| s.value == 42.0));

        let samples =
            DataGenerator::dataset(ValueWorkload::ConstantInt, TimestampModel::Regular, 64, 1);
        assert!(samples.iter().all(|s| s.value == 1000.0));
    }
}
