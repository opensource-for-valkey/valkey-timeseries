// This code is a Rust adaptation of the C# HyndmanFanQuantileEstimator class from perfolizer.
// The Hyndman-Fan quantile estimator supports nine algorithms described in:
// Hyndman, Rob J., and Yanan Fan. "Sample quantiles in statistical packages."
// The American Statistician 50, no. 4 (1996): 361-365.
// https://doi.org/10.2307/2684934

use crate::analysis::quantile_estimators::{QuantileEstimator, Samples};
use std::fmt;

pub type Probability = f64;

// The nine definitions from Hyndman & Fan (1996); only Type7 is selected today.
#[allow(dead_code)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HyndmanFanType {
    Type1 = 1,
    Type2,
    Type3,
    Type4,
    Type5,
    Type6,
    Type7,
    Type8,
    Type9,
}

impl HyndmanFanType {
    /// Returns true if the type supports weighted samples
    pub fn supports_weighted_samples(&self) -> bool {
        match self {
            HyndmanFanType::Type1 => false,
            HyndmanFanType::Type2 => false,
            HyndmanFanType::Type3 => false,
            HyndmanFanType::Type4 => true,
            HyndmanFanType::Type5 => true,
            HyndmanFanType::Type6 => true,
            HyndmanFanType::Type7 => true,
            HyndmanFanType::Type8 => true,
            HyndmanFanType::Type9 => true,
        }
    }

    /// Returns 1-based real index estimation (h)
    fn get_h(&self, n: f64, p: Probability) -> f64 {
        match self {
            HyndmanFanType::Type1 => n * p + 0.5,
            HyndmanFanType::Type2 => n * p + 0.5,
            HyndmanFanType::Type3 => n * p,
            HyndmanFanType::Type4 => n * p,
            HyndmanFanType::Type5 => n * p + 0.5,
            HyndmanFanType::Type6 => (n + 1.0) * p,
            HyndmanFanType::Type7 => (n - 1.0) * p + 1.0,
            HyndmanFanType::Type8 => (n + 1.0 / 3.0) * p + 1.0 / 3.0,
            HyndmanFanType::Type9 => (n + 0.25) * p + 0.375,
        }
    }

    /// Computes the quantile value using the specified type.
    ///
    /// - `n`: Number of samples (integer, must be >= 1)
    /// - `p`: Probability (0.0 <= p <= 1.0)
    /// - `get_value`: Closure that takes a 1-based index and returns the corresponding value
    fn evaluate<F>(self, n: usize, p: Probability, mut get_value: F) -> f64
    where
        F: FnMut(usize) -> f64,
    {
        let h = self.get_h(n as f64, p);

        fn linear_interpolation(h: f64, n: usize, get_value: &mut impl FnMut(usize) -> f64) -> f64 {
            let h_floor = h.floor() as usize;
            let fraction = h - (h_floor as f64);
            if h_floor < n {
                get_value(h_floor) * (1.0 - fraction) + get_value(h_floor + 1) * fraction
            } else {
                get_value(h_floor)
            }
        }

        match self {
            HyndmanFanType::Type1 => get_value((h - 0.5).ceil() as usize),
            HyndmanFanType::Type2 => {
                (get_value((h - 0.5).ceil() as usize) + get_value((h + 0.5).floor() as usize)) / 2.0
            }
            HyndmanFanType::Type3 => {
                let idx = h.round() as usize;
                get_value(idx)
            }
            _ => linear_interpolation(h, n, &mut get_value),
        }
    }
}

impl fmt::Display for HyndmanFanType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "HF{}", *self as u8)
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct HyndmanFanQuantileEstimator {
    pub typ: HyndmanFanType,
}

impl HyndmanFanQuantileEstimator {
    pub const fn new(typ: HyndmanFanType) -> Self {
        Self { typ }
    }

    /// Returns 1-based real index estimation
    fn get_h(&self, n: usize, p: Probability) -> f64 {
        self.typ.get_h(n as f64, p)
    }

    pub fn quantile(&self, sample: &Samples, probability: Probability) -> f64 {
        if !self.supports_weighted_samples() && sample.is_weighted() {
            panic!("This estimator does not support weighted samples.");
        }

        if sample.is_weighted() {
            self.get_quantile_for_weighted_sample(sample, probability)
        } else {
            self.get_quantile_for_nonweighted_sample(sample, probability)
        }
    }

    // See https://aakinshin.net/posts/weighted-quantiles/
    fn get_quantile_for_weighted_sample(&self, sample: &Samples, probability: Probability) -> f64 {
        let n = sample.len();
        let p = probability;
        let h = self.get_h(n, p).clamp(1.0, n as f64);
        let left = (h - 1.0) / n as f64;
        let right = h / n as f64;

        let cdf = |x: f64| -> f64 {
            if x <= left {
                0.0
            } else if x >= right {
                1.0
            } else {
                x * n as f64 - h + 1.0
            }
        };

        let total_weight = sample.total_weight;
        let mut result = 0.0;
        let mut current = 0.0;
        let values = &sample.values;
        let weights = sample
            .sorted_weights
            .as_ref()
            .expect("Weights required for weighted sample");

        for i in 0..n {
            let next = current + weights[i] / total_weight;
            result += values[i] * (cdf(next) - cdf(current));
            current = next;
        }

        result
    }

    fn get_quantile_for_nonweighted_sample(
        &self,
        sample: &Samples,
        probability: Probability,
    ) -> f64 {
        let sorted_values = &sample.values;
        let n = sample.len();

        let get_value = |index: usize| -> f64 {
            let idx = index.saturating_sub(1); // Adapt one-based formula to zero-based index
            if idx == 0 {
                sorted_values[0]
            } else if idx >= n {
                sorted_values[n - 1]
            } else {
                sorted_values[idx]
            }
        };

        self.typ.evaluate(n, probability, get_value)
    }
}

impl QuantileEstimator for HyndmanFanQuantileEstimator {
    fn quantile(&self, samples: &Samples, probability: Probability) -> f64 {
        // Call the inherent method explicitly via the type to avoid recursively
        // calling this trait method implementation.
        HyndmanFanQuantileEstimator::quantile(self, samples, probability)
    }

    fn supports_weighted_samples(&self) -> bool {
        self.typ.supports_weighted_samples()
    }
}
