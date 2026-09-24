use crate::analysis::math::beta_function::beta_regularized_incomplete_value;
use std::fmt;

pub struct BetaDistribution {
    pub alpha: f64,
    pub beta: f64,
}

impl BetaDistribution {
    pub fn new(alpha: f64, beta: f64) -> Self {
        debug_assert!(alpha >= 0.0, "Alpha must be non-negative");
        debug_assert!(beta >= 0.0, "Beta must be non-negative");
        BetaDistribution { alpha, beta }
    }

    /// Cumulative distribution function
    pub fn cdf(&self, x: f64) -> f64 {
        beta_regularized_incomplete_value(self.alpha, self.beta, x)
    }
}

impl fmt::Display for BetaDistribution {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Beta({},{})", self.alpha, self.beta)
    }
}
