mod beta_distribution;
mod beta_function;
mod confidence_interval_estimator;
mod gamma_function;
mod stats;
mod student_distribution;

pub use beta_distribution::BetaDistribution;
pub use confidence_interval_estimator::*;
pub use stats::*;
pub(super) use student_distribution::StudentDistribution;
