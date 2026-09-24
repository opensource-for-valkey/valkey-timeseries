//! Stl (Seasonal-Trend decomposition using LOESS) implementation.
//!
//! Stl decomposes a time series into three components:
//! - Trend: The underlying long-term pattern
//! - Seasonal: The repeating seasonal pattern
//! - Remainder: The residual after removing trend and seasonal

#[cfg(test)]
use crate::analysis::math::calculate_variance;

/// Result of Stl decomposition.
#[derive(Debug, Clone)]
pub struct STLResult {
    /// Trend component.
    pub trend: Vec<f64>,
    /// Seasonal component.
    pub seasonal: Vec<f64>,
    /// Remainder component.
    pub remainder: Vec<f64>,
}

#[cfg(test)]
impl STLResult {
    /// Get the seasonal strength (0 to 1).
    /// Values close to 1 indicate strong seasonality.
    pub fn seasonal_strength(&self) -> f64 {
        let var_remainder = calculate_variance(&self.remainder);
        let seasonal_plus_remainder: Vec<f64> = self
            .seasonal
            .iter()
            .zip(self.remainder.iter())
            .map(|(s, r)| s + r)
            .collect();
        let var_sr = calculate_variance(&seasonal_plus_remainder);

        if var_sr < 1e-10 {
            return 0.0;
        }

        (1.0 - var_remainder / var_sr).max(0.0)
    }

    /// Get the trend strength (0 to 1).
    /// Values close to 1 indicate strong trend.
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

        (1.0 - var_remainder / var_tr).max(0.0)
    }
}

/// Stl decomposition configuration and algorithm.
#[derive(Debug, Clone)]
pub struct Stl {
    /// Seasonal period.
    seasonal_period: usize,
    /// Seasonal LOESS smoothing parameter (ns).
    seasonal_smoothness: usize,
    /// Trend LOESS smoothing parameter (nt).
    trend_smoothness: usize,
    /// Low-pass filter parameter (nl).
    low_pass_smoothness: usize,
    /// Number of inner iterations.
    inner_iterations: usize,
    /// Number of outer (robustness) iterations.
    outer_iterations: usize,
    /// Use robust fitting.
    robust: bool,
}

impl Stl {
    /// Create a new Stl decomposer with the given seasonal period.
    pub fn new(seasonal_period: usize) -> Self {
        // Default parameters following Cleveland et al. (1990)
        let ns = seasonal_period;
        let nt = (1.5 * seasonal_period as f64 / (1.0 - 1.5 / ns as f64)).ceil() as usize;
        let nt = if nt.is_multiple_of(2) { nt + 1 } else { nt }; // Must be odd
        let nl = seasonal_period;
        let nl = if nl.is_multiple_of(2) { nl + 1 } else { nl }; // Must be odd

        Self {
            seasonal_period,
            seasonal_smoothness: ns | 1, // Ensure odd
            trend_smoothness: nt,
            low_pass_smoothness: nl,
            inner_iterations: 2,
            outer_iterations: 0,
            robust: false,
        }
    }

    /// Set custom seasonal smoothness (ns parameter).
    pub fn with_seasonal_smoothness(mut self, ns: usize) -> Self {
        self.seasonal_smoothness = if ns.is_multiple_of(2) { ns + 1 } else { ns };
        self
    }

    /// Set custom trend smoothness (nt parameter).
    pub fn with_trend_smoothness(mut self, nt: usize) -> Self {
        self.trend_smoothness = if nt.is_multiple_of(2) { nt + 1 } else { nt };
        self
    }

    /// Enable robust fitting with default iterations.
    pub fn robust(mut self) -> Self {
        self.robust = true;
        self.outer_iterations = 6;
        self
    }

    /// Set number of outer (robustness) iterations.
    pub fn with_outer_iterations(mut self, n: usize) -> Self {
        self.outer_iterations = n;
        if n > 0 {
            self.robust = true;
        }
        self
    }

    /// Set number of inner iterations.
    #[cfg(test)]
    pub fn with_inner_iterations(mut self, n: usize) -> Self {
        self.inner_iterations = n;
        self
    }

    /// Decompose the time series.
    pub fn decompose(&self, series: &[f64]) -> Option<STLResult> {
        let n = series.len();
        if n < 2 * self.seasonal_period {
            return None;
        }

        // Initialize components
        let mut seasonal = vec![0.0; n];
        let mut trend = vec![0.0; n];
        let mut weights = vec![1.0; n];

        // Outer loop (robustness)
        let outer_iters = if self.robust {
            self.outer_iterations.max(1)
        } else {
            1
        };

        for o in 0..outer_iters {
            // Inner loop
            for _ in 0..self.inner_iterations {
                // Step 1: Detrending
                let detrended: Vec<f64> = series
                    .iter()
                    .zip(trend.iter())
                    .map(|(y, t)| y - t)
                    .collect();

                // Step 2: Cycle-subseries smoothing
                let cycle_subseries = self.smooth_cycle_subseries(&detrended, &weights);

                // Step 3: Low-pass filter of smoothed cycle-subseries
                let low_pass = self.low_pass_filter(&cycle_subseries);

                // Step 4: Detrending of smoothed cycle-subseries
                for i in 0..n {
                    seasonal[i] = cycle_subseries[i] - low_pass[i];
                }

                // Step 5: Deseasonalizing
                let deseasonalized: Vec<f64> = series
                    .iter()
                    .zip(seasonal.iter())
                    .map(|(y, s)| y - s)
                    .collect();

                // Step 6: Trend smoothing
                trend = self.loess_smooth(&deseasonalized, self.trend_smoothness, &weights);
            }

            // Update robustness weights
            if self.robust && o < outer_iters - 1 {
                let remainder: Vec<f64> = series
                    .iter()
                    .zip(seasonal.iter())
                    .zip(trend.iter())
                    .map(|((y, s), t)| y - s - t)
                    .collect();
                weights = self.compute_robustness_weights(&remainder);
            }
        }

        // Compute final remainder
        let remainder: Vec<f64> = series
            .iter()
            .zip(seasonal.iter())
            .zip(trend.iter())
            .map(|((y, s), t)| y - s - t)
            .collect();

        Some(STLResult {
            trend,
            seasonal,
            remainder,
        })
    }

    /// Smooth cycle-subseries (seasonal component estimation).
    fn smooth_cycle_subseries(&self, detrended: &[f64], weights: &[f64]) -> Vec<f64> {
        let n = detrended.len();
        let period = self.seasonal_period;
        let mut result = vec![0.0; n];

        // Process each cycle-subseries (one for each position in the seasonal cycle)
        for cycle_pos in 0..period {
            // Extract subseries for this cycle position
            let mut subseries_values = Vec::new();
            let mut subseries_weights = Vec::new();
            let mut subseries_indices = Vec::new();

            for (i, (&val, &w)) in detrended.iter().zip(weights.iter()).enumerate() {
                if i % period == cycle_pos {
                    subseries_values.push(val);
                    subseries_weights.push(w);
                    subseries_indices.push(i);
                }
            }

            // Smooth the subseries
            let smoothed = self.loess_smooth_subseries(
                &subseries_values,
                self.seasonal_smoothness,
                &subseries_weights,
            );

            // Put smoothed values back
            for (&idx, &smooth_val) in subseries_indices.iter().zip(smoothed.iter()) {
                result[idx] = smooth_val;
            }
        }

        result
    }

    /// LOESS smoothing for a subseries.
    fn loess_smooth_subseries(&self, values: &[f64], span: usize, weights: &[f64]) -> Vec<f64> {
        let n = values.len();
        if n == 0 {
            return Vec::new();
        }

        let half_span = span / 2;
        let mut result = vec![0.0; n];

        for i in 0..n {
            // Determine window
            let start = i.saturating_sub(half_span);
            let end = (i + half_span + 1).min(n);

            // Compute weighted average (simplified LOESS)
            let mut sum_weights = 0.0;
            let mut sum_values = 0.0;

            for j in start..end {
                let dist = (i as f64 - j as f64).abs();
                let max_dist = half_span as f64 + 1.0;
                let u = dist / max_dist;
                let tricube = if u < 1.0 {
                    (1.0 - u.powi(3)).powi(3)
                } else {
                    0.0
                };
                let w = tricube * weights[j];
                sum_weights += w;
                sum_values += w * values[j];
            }

            result[i] = if sum_weights > 0.0 {
                sum_values / sum_weights
            } else {
                values[i]
            };
        }

        result
    }

    /// Low-pass filter using moving averages.
    fn low_pass_filter(&self, series: &[f64]) -> Vec<f64> {
        let n = series.len();
        let period = self.seasonal_period;

        // Apply three moving averages: MA(period), MA(period), MA(3)
        let ma1 = self.moving_average(series, period);
        let ma2 = self.moving_average(&ma1, period);
        let ma3 = self.moving_average(&ma2, 3);

        // Apply LOESS to the result
        let weights = vec![1.0; n];
        self.loess_smooth(&ma3, self.low_pass_smoothness, &weights)
    }

    /// Simple centered moving average.
    fn moving_average(&self, series: &[f64], window: usize) -> Vec<f64> {
        let n = series.len();
        let half = window / 2;
        let mut result = vec![0.0; n];

        for (i, res) in result.iter_mut().enumerate() {
            let start = i.saturating_sub(half);
            let end = (i + half + 1).min(n);
            let sum: f64 = series[start..end].iter().sum();
            *res = sum / (end - start) as f64;
        }

        result
    }

    /// LOESS smoothing.
    fn loess_smooth(&self, values: &[f64], span: usize, weights: &[f64]) -> Vec<f64> {
        let n = values.len();
        if n == 0 {
            return Vec::new();
        }

        let half_span = span / 2;
        let mut result = vec![0.0; n];

        for i in 0..n {
            // Determine window
            let start = i.saturating_sub(half_span);
            let end = (i + half_span + 1).min(n);

            // Compute weighted local linear regression
            let mut sw = 0.0;
            let mut swx = 0.0;
            let mut swx2 = 0.0;
            let mut swy = 0.0;
            let mut swxy = 0.0;

            for j in start..end {
                let dist = (i as f64 - j as f64).abs();
                let max_dist = (half_span as f64 + 1.0).max(dist);
                let u = dist / max_dist;
                let tricube = if u < 1.0 {
                    (1.0 - u.powi(3)).powi(3)
                } else {
                    0.0
                };
                let w = tricube * weights[j];
                let x = j as f64;
                let y = values[j];

                sw += w;
                swx += w * x;
                swx2 += w * x * x;
                swy += w * y;
                swxy += w * x * y;
            }

            if sw <= 1e-10 {
                result[i] = values[i];
                continue;
            }

            let denom = sw * swx2 - swx * swx;
            if denom.abs() < 1e-10 {
                // If the denominator is zero, fall back to weighted mean
                result[i] = swy / sw;
            } else {
                let a = (swy * swx2 - swx * swxy) / denom;
                let b = (sw * swxy - swx * swy) / denom;
                result[i] = a + b * (i as f64);
            }
        }

        result
    }

    /// Compute robustness weights based on remainder.
    fn compute_robustness_weights(&self, remainder: &[f64]) -> Vec<f64> {
        let n = remainder.len();
        let abs_remainder: Vec<f64> = remainder.iter().map(|r| r.abs()).collect();

        // Compute median absolute deviation
        let mut sorted = abs_remainder.clone();
        sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
        let median = if n.is_multiple_of(2) {
            (sorted[n / 2 - 1] + sorted[n / 2]) / 2.0
        } else {
            sorted[n / 2]
        };

        let h = 6.0 * median; // Tuning constant

        // Compute bisquare weights
        remainder
            .iter()
            .map(|r| {
                if h < 1e-10 {
                    return 1.0;
                }
                let u = r.abs() / h;
                if u < 1.0 { (1.0 - u * u).powi(2) } else { 0.0 }
            })
            .collect()
    }
}

impl Default for Stl {
    fn default() -> Self {
        Self::new(12) // Monthly seasonality default
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn generate_seasonal_series(n: usize, period: usize) -> Vec<f64> {
        (0..n)
            .map(|i| {
                let trend = 0.1 * i as f64;
                let seasonal =
                    10.0 * ((2.0 * std::f64::consts::PI * i as f64 / period as f64).sin());
                trend + seasonal
            })
            .collect()
    }

    #[test]
    fn stl_basic_decomposition() {
        let period = 12;
        let series = generate_seasonal_series(120, period);

        let stl = Stl::new(period);
        let result = stl.decompose(&series).unwrap();

        assert_eq!(result.trend.len(), series.len());
        assert_eq!(result.seasonal.len(), series.len());
        assert_eq!(result.remainder.len(), series.len());

        // Verify additive decomposition: y = trend + seasonal + remainder
        for (i, &s) in series.iter().enumerate() {
            let reconstructed = result.trend[i] + result.seasonal[i] + result.remainder[i];
            assert!(
                (s - reconstructed).abs() < 1e-10,
                "Reconstruction failed at index {}: {} vs {}",
                i,
                s,
                reconstructed
            );
        }
    }

    #[test]
    fn stl_detects_seasonality() {
        let period = 12;
        let series = generate_seasonal_series(120, period);

        let stl = Stl::new(period);
        let result = stl.decompose(&series).unwrap();

        // Should detect strong seasonality
        let strength = result.seasonal_strength();
        assert!(
            strength > 0.5,
            "Expected strong seasonality, got {}",
            strength
        );
    }

    #[test]
    fn stl_detects_trend() {
        let n = 120;
        let period = 12;
        // Strong trend with weak seasonality
        let series: Vec<f64> = (0..n)
            .map(|i| {
                let trend = 2.0 * i as f64;
                let seasonal =
                    0.1 * ((2.0 * std::f64::consts::PI * i as f64 / period as f64).sin());
                trend + seasonal
            })
            .collect();

        let stl = Stl::new(period);
        let result = stl.decompose(&series).unwrap();

        let strength = result.trend_strength();
        assert!(strength > 0.9, "Expected strong trend, got {}", strength);
    }

    #[test]
    fn stl_trend_only() {
        let n = 100;
        let period = 10;
        // Only trend, no seasonality
        let series: Vec<f64> = (0..n).map(|i| 5.0 + 0.5 * i as f64).collect();

        let stl = Stl::new(period);
        let result = stl.decompose(&series).unwrap();

        // Seasonal component should be small
        let seasonal_var = calculate_variance(&result.seasonal);
        let series_var = calculate_variance(&series);
        assert!(
            seasonal_var < series_var * 0.1,
            "Seasonal variance {} should be small compared to series variance {}",
            seasonal_var,
            series_var
        );
    }

    #[test]
    fn stl_constant_series() {
        let n = 100;
        let period = 10;
        let series = vec![5.0; n];

        let stl = Stl::new(period);
        let result = stl.decompose(&series).unwrap();

        // All components should be flat/zero
        for &s in &result.seasonal {
            assert!(s.abs() < 1e-6, "Seasonal should be near zero");
        }
        for &r in &result.remainder {
            assert!(r.abs() < 1e-6, "Remainder should be near zero");
        }
    }

    #[test]
    fn stl_insufficient_data() {
        let period = 12;
        let series = vec![1.0; 10]; // Less than 2 * period

        let stl = Stl::new(period);
        assert!(stl.decompose(&series).is_none());
    }

    #[test]
    fn stl_robust_decomposition() {
        let period = 12;
        let mut series = generate_seasonal_series(120, period);
        // Add outliers
        series[30] = 100.0;
        series[60] = -100.0;

        let stl = Stl::new(period).robust();
        let result = stl.decompose(&series).unwrap();

        // Robust fitting should still capture some pattern
        let strength = result.seasonal_strength();
        assert!(
            strength > 0.1,
            "Robust STL should still detect seasonality: {}",
            strength
        );
    }

    #[test]
    fn stl_custom_smoothness() {
        let period = 12;
        let series = generate_seasonal_series(120, period);

        let stl = Stl::new(period)
            .with_seasonal_smoothness(7)
            .with_trend_smoothness(21)
            .with_inner_iterations(3);

        let result = stl.decompose(&series).unwrap();
        assert_eq!(result.trend.len(), series.len());
    }

    #[test]
    fn stl_different_periods() {
        // Weekly (period 7)
        let series_weekly = generate_seasonal_series(70, 7);
        let stl_weekly = Stl::new(7);
        assert!(stl_weekly.decompose(&series_weekly).is_some());

        // Quarterly (period 4)
        let series_quarterly = generate_seasonal_series(40, 4);
        let stl_quarterly = Stl::new(4);
        assert!(stl_quarterly.decompose(&series_quarterly).is_some());
    }

    #[test]
    fn stl_result_seasonal_strength_range() {
        let period = 12;
        let series = generate_seasonal_series(120, period);

        let stl = Stl::new(period);
        let result = stl.decompose(&series).unwrap();

        let strength = result.seasonal_strength();
        assert!(
            (0.0..=1.0).contains(&strength),
            "Seasonal strength should be in [0, 1]: {}",
            strength
        );
    }

    #[test]
    fn stl_result_trend_strength_range() {
        let period = 12;
        let series = generate_seasonal_series(120, period);

        let stl = Stl::new(period);
        let result = stl.decompose(&series).unwrap();

        let strength = result.trend_strength();
        assert!(
            (0.0..=1.0).contains(&strength),
            "Trend strength should be in [0, 1]: {}",
            strength
        );
    }
}
