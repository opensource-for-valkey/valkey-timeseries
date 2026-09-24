//! Mstl (Multiple Seasonal-Trend decomposition using LOESS) implementation.
//!
//! Mstl extends Stl to handle multiple seasonal periods, such as daily and weekly
//! patterns in hourly data.

use super::stl::Stl;
#[cfg(test)]
use crate::analysis::math::calculate_variance;

/// Result of Mstl decomposition.
#[derive(Debug, Clone)]
pub struct MSTLResult {
    /// Trend component.
    #[cfg(test)]
    pub trend: Vec<f64>,
    /// Seasonal components (one for each period).
    #[cfg(test)]
    pub seasonal_components: Vec<Vec<f64>>,
    /// The seasonal periods corresponding to each component.
    #[cfg(test)]
    pub seasonal_periods: Vec<usize>,
    /// Remainder component.
    pub remainder: Vec<f64>,
}

#[cfg(test)]
impl MSTLResult {
    /// Get the total seasonal component (sum of all seasonal components).
    pub fn total_seasonal(&self) -> Vec<f64> {
        if self.seasonal_components.is_empty() {
            return vec![0.0; self.trend.len()];
        }

        let n = self.trend.len();
        let mut total = vec![0.0; n];
        for component in &self.seasonal_components {
            for i in 0..n {
                total[i] += component[i];
            }
        }
        total
    }

    /// Get seasonal strength for a specific period.
    pub fn seasonal_strength(&self, period_idx: usize) -> Option<f64> {
        if period_idx >= self.seasonal_components.len() {
            return None;
        }

        let seasonal = &self.seasonal_components[period_idx];

        let var_remainder = calculate_variance(&self.remainder);
        let seasonal_plus_remainder: Vec<f64> = seasonal
            .iter()
            .zip(self.remainder.iter())
            .map(|(s, r)| s + r)
            .collect();
        let var_sr = calculate_variance(&seasonal_plus_remainder);

        if var_sr < 1e-10 {
            return Some(0.0);
        }

        let strength = 1.0 - var_remainder / var_sr;
        Some(strength.clamp(0.0, 1.0))
    }

    /// Get trend strength.
    pub fn trend_strength(&self) -> f64 {
        let var_remainder = calculate_variance(&self.remainder);
        let trend_plus_remainder: Vec<f64> = self
            .trend
            .iter()
            .zip(self.remainder.iter())
            .map(|(t, r)| t + r)
            .collect();
        let var_tr = calculate_variance(&trend_plus_remainder);

        if var_tr < 1e-10 {
            return 0.0;
        }

        let strength = 1.0 - var_remainder / var_tr;
        strength.clamp(0.0, 1.0)
    }
}

/// Mstl decomposition for multiple seasonal periods.
#[derive(Debug, Clone)]
pub struct Mstl {
    /// Seasonal periods (should be sorted in increasing order).
    seasonal_periods: Vec<usize>,
    /// Number of iterations.
    iterations: usize,
    /// Use robust fitting.
    robust: bool,
}

impl Mstl {
    /// Create a new Mstl decomposer with the given seasonal periods.
    pub fn new(seasonal_periods: Vec<usize>) -> Self {
        let mut periods = seasonal_periods;
        periods.sort();
        periods.dedup();

        Self {
            seasonal_periods: periods,
            iterations: 2,
            robust: true, // Enable robustness by default
        }
    }

    /// Set number of iterations.
    #[cfg(test)]
    pub fn with_iterations(mut self, n: usize) -> Self {
        self.iterations = n;
        self
    }

    /// Enable robust fitting.
    pub fn robust(mut self) -> Self {
        self.robust = true;
        self
    }

    /// Get the seasonal periods.
    #[cfg(test)]
    pub fn seasonal_periods(&self) -> &[usize] {
        &self.seasonal_periods
    }

    /// Decompose the time series.
    pub fn decompose(&self, series: &[f64]) -> Option<MSTLResult> {
        let n = series.len();

        if self.seasonal_periods.is_empty() {
            return None;
        }

        // Check minimum length
        let max_period = *self.seasonal_periods.last()?;
        if n < 2 * max_period {
            return None;
        }

        let num_seasonals = self.seasonal_periods.len();
        let mut seasonal_components: Vec<Vec<f64>> = vec![vec![0.0; n]; num_seasonals];
        let mut trend = vec![0.0; n];

        // Iterative decomposition
        for _ in 0..self.iterations {
            // Deseasonalize by removing all seasonal components
            let mut deseasonalized: Vec<f64> = series.to_vec();
            for seasonal in &seasonal_components {
                for i in 0..n {
                    deseasonalized[i] -= seasonal[i];
                }
            }

            // Extract trend using Stl with the longest period
            let mut stl_trend = if self.robust {
                Stl::new(max_period).robust().with_outer_iterations(5)
            } else {
                Stl::new(max_period)
            };

            // Ensure trend is smooth and does not absorb spikes
            stl_trend = stl_trend.with_trend_smoothness(n | 1);

            if let Some(trend_result) = stl_trend.decompose(&deseasonalized) {
                trend = trend_result.trend;
            }

            // Extract each seasonal component
            for (s_idx, &period) in self.seasonal_periods.iter().enumerate() {
                // Remove other seasonal components from the series
                let mut adjusted: Vec<f64> = series.to_vec();
                for (other_idx, other_seasonal) in seasonal_components.iter().enumerate() {
                    if other_idx != s_idx {
                        for i in 0..n {
                            adjusted[i] -= other_seasonal[i];
                        }
                    }
                }
                // Also remove trend
                for i in 0..n {
                    adjusted[i] -= trend[i];
                }

                // Extract this seasonal component using Stl
                let mut stl_seasonal = if self.robust {
                    Stl::new(period).robust().with_outer_iterations(5) // Reduced from 10 to 5 for efficiency
                } else {
                    Stl::new(period)
                };

                // Ensure seasonal components are smooth
                // For daily/weekly patterns, we want enough smoothness to not follow noise/spikes
                stl_seasonal = stl_seasonal.with_seasonal_smoothness((period * 10 + 1).min(n | 1));

                if let Some(seasonal_result) = stl_seasonal.decompose(&adjusted) {
                    seasonal_components[s_idx] = seasonal_result.seasonal;
                }
            }
        }

        // Compute remainder
        let mut remainder: Vec<f64> = series.to_vec();
        for i in 0..n {
            remainder[i] -= trend[i];
            for seasonal in &seasonal_components {
                remainder[i] -= seasonal[i];
            }
        }

        Some(MSTLResult {
            #[cfg(test)]
            trend,
            #[cfg(test)]
            seasonal_components,
            #[cfg(test)]
            seasonal_periods: self.seasonal_periods.clone(),
            remainder,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn generate_multi_seasonal_series(n: usize, periods: &[usize]) -> Vec<f64> {
        (0..n)
            .map(|i| {
                let trend = 0.05 * i as f64;
                let mut seasonal = 0.0;
                for (idx, &period) in periods.iter().enumerate() {
                    let amplitude = 5.0 / (idx + 1) as f64; // Decreasing amplitude
                    seasonal +=
                        amplitude * ((2.0 * std::f64::consts::PI * i as f64 / period as f64).sin());
                }
                trend + seasonal
            })
            .collect()
    }

    #[test]
    fn mstl_single_period() {
        let period = 12;
        let series = generate_multi_seasonal_series(120, &[period]);

        let mstl = Mstl::new(vec![period]);
        let result = mstl.decompose(&series).unwrap();

        assert_eq!(result.seasonal_components.len(), 1);
        assert_eq!(result.seasonal_periods, vec![period]);
        assert_eq!(result.trend.len(), series.len());
    }

    #[test]
    fn mstl_two_periods() {
        // Daily and weekly seasonality (simulated)
        let periods = vec![7, 24];
        let series = generate_multi_seasonal_series(200, &periods);

        let mstl = Mstl::new(periods.clone());
        let result = mstl.decompose(&series).unwrap();

        assert_eq!(result.seasonal_components.len(), 2);
        assert_eq!(result.seasonal_periods, vec![7, 24]);

        // Verify additive decomposition
        for (i, &s) in series.iter().enumerate() {
            let reconstructed = result.trend[i]
                + result.seasonal_components[0][i]
                + result.seasonal_components[1][i]
                + result.remainder[i];
            assert!(
                (s - reconstructed).abs() < 1e-6,
                "Reconstruction failed at index {}",
                i
            );
        }
    }

    #[test]
    fn mstl_total_seasonal() {
        let periods = vec![7, 24];
        let series = generate_multi_seasonal_series(200, &periods);

        let mstl = Mstl::new(periods);
        let result = mstl.decompose(&series).unwrap();

        let total = result.total_seasonal();
        assert_eq!(total.len(), series.len());

        // Total should be sum of components
        for (i, &t) in total.iter().enumerate() {
            let expected = result.seasonal_components[0][i] + result.seasonal_components[1][i];
            assert!(
                (t - expected).abs() < 1e-10,
                "Total seasonal mismatch at index {}",
                i
            );
        }
    }

    #[test]
    fn mstl_insufficient_data() {
        let periods = vec![7, 24];
        let series = vec![1.0; 30]; // Less than 2 * max_period

        let mstl = Mstl::new(periods);
        assert!(mstl.decompose(&series).is_none());
    }

    #[test]
    fn mstl_empty_periods() {
        let series = vec![1.0; 100];
        let mstl = Mstl::new(vec![]);
        assert!(mstl.decompose(&series).is_none());
    }

    #[test]
    fn mstl_robust() {
        let periods = vec![12];
        let mut series = generate_multi_seasonal_series(120, &periods);
        // Add outliers
        series[30] = 100.0;
        series[60] = -100.0;

        let mstl = Mstl::new(periods).robust();
        let result = mstl.decompose(&series);
        assert!(result.is_some());
    }

    #[test]
    fn mstl_duplicate_periods_removed() {
        let mstl = Mstl::new(vec![12, 12, 7, 7]);
        assert_eq!(mstl.seasonal_periods(), &[7, 12]);
    }

    #[test]
    fn mstl_periods_sorted() {
        let mstl = Mstl::new(vec![24, 7, 12]);
        assert_eq!(mstl.seasonal_periods(), &[7, 12, 24]);
    }

    #[test]
    fn mstl_seasonal_strength() {
        let periods = vec![12];
        let series = generate_multi_seasonal_series(120, &periods);

        let mstl = Mstl::new(periods);
        let result = mstl.decompose(&series).unwrap();

        let strength = result.seasonal_strength(0).unwrap();
        assert!(
            (0.0..=1.0).contains(&strength),
            "Seasonal strength should be in [0, 1]: {}",
            strength
        );
    }

    #[test]
    fn mstl_trend_strength() {
        let periods = vec![12];
        let series = generate_multi_seasonal_series(120, &periods);

        let mstl = Mstl::new(periods);
        let result = mstl.decompose(&series).unwrap();

        let strength = result.trend_strength();
        assert!(
            (0.0..=1.0).contains(&strength),
            "Trend strength should be in [0, 1]: {}",
            strength
        );
    }

    #[test]
    fn mstl_with_iterations() {
        let periods = vec![12];
        let series = generate_multi_seasonal_series(120, &periods);

        let mstl = Mstl::new(periods).with_iterations(3);
        let result = mstl.decompose(&series);
        assert!(result.is_some());
    }

    #[test]
    fn mstl_invalid_period_index() {
        let periods = vec![12];
        let series = generate_multi_seasonal_series(120, &periods);

        let mstl = Mstl::new(periods);
        let result = mstl.decompose(&series).unwrap();

        assert!(result.seasonal_strength(5).is_none());
    }
}
