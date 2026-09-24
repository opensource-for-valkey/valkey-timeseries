// Harrell-Davis quantile estimator in Rust
// Reference: Harrell, Frank E., and C. E. Davis. "A new distribution-free quantile estimator." Biometrika 69, no. 3 (1982): 635-640.
// https://doi.org/10.1093/biomet/69.3.635

use crate::analysis::math::BetaDistribution;
use crate::analysis::quantile_estimators::{QuantileEstimator, Samples};
use std::f64;

pub struct HarrellDavisQuantileEstimator;

impl QuantileEstimator for HarrellDavisQuantileEstimator {
    fn quantile(&self, sample: &Samples, probability: f64) -> f64 {
        get_moments(sample, probability, false).0
    }

    fn supports_weighted_samples(&self) -> bool {
        true
    }
}

/// Returns (c1, c2) moments. If calc_second_moment is false, c2 is NaN.
fn get_moments(samples: &Samples, probability: f64, calc_second_moment: bool) -> (f64, f64) {
    debug_assert!(!samples.values.is_empty(), "Sample cannot be empty");

    let n = samples.weighted_size();
    let a = (n + 1.0) * probability;
    let b = (n + 1.0) * (1.0 - probability);
    let distribution = BetaDistribution::new(a, b);

    let mut c1 = 0.0;
    let mut c2 = if calc_second_moment { 0.0 } else { f64::NAN };
    let mut beta_cdf_right = 0.0;
    let mut current_probability = 0.0;

    for (j, &v) in samples.values.iter().enumerate() {
        let beta_cdf_left = beta_cdf_right;
        current_probability += if samples.is_weighted() {
            samples.sorted_weights.as_ref().unwrap()[j] / samples.total_weight
        } else {
            1.0 / samples.len() as f64
        };

        let cdf_value = distribution.cdf(current_probability.min(1.0));
        beta_cdf_right = cdf_value;
        let w = beta_cdf_right - beta_cdf_left;
        c1 += w * v;
        if calc_second_moment {
            c2 += w * v * v;
        }
    }

    (c1, c2)
}
