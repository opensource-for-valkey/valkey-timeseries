mod harrell_davis_quantile_estimator;
mod hyndman_fan_quantile_estimator;
mod samples;
mod simple_quantile_estimator;
mod trimmed_hd_estimator;

pub use harrell_davis_quantile_estimator::*;
pub use hyndman_fan_quantile_estimator::*;
pub use samples::*;
pub use simple_quantile_estimator::*;

/// Trait for quantile estimators.
pub trait QuantileEstimator {
    /// Calculates the requested quantile estimation based on the given sample.
    ///
    /// # Arguments
    ///
    /// * `sample` - A reference to a sample.
    /// * `probability` - A value in range [0.0, 1.0] that describes the requested quantile.
    ///
    /// # Returns
    ///
    /// Quantile estimation for the given sample.
    fn quantile(&self, sample: &Samples, probability: f64) -> f64;

    // Median function
    fn median(&self, sample: &Samples) -> f64 {
        self.quantile(sample, 0.5)
    }

    /// Indicates whether the estimator supports weighted samples.
    fn supports_weighted_samples(&self) -> bool;
}
