use super::utils::{normalize_evidence, normalize_value};
use crate::analysis::TimeSeriesAnalysisResult;
use crate::analysis::math::{calculate_mean, calculate_std_dev};
use crate::analysis::outliers::{
    Anomaly, AnomalyDetector, AnomalyMethod, AnomalyResult, AnomalySignal, MethodInfo,
};

/// Default alpha for Ewma SPC
pub const EWMA_DEFAULT_ALPHA: f64 = 0.3;
pub const EWMA_DEFAULT_MULTIPLIER: f64 = 3.0;

/// SPC Exponentially Weighted Moving Average (EWMA) outlier detector
#[derive(Debug)]
pub struct EwmaOutlierDetector {
    /// smoothing factor
    alpha: f64,
    /// target (process mean)
    target: f64,
    /// standard deviation of the process
    sigma: f64,
    /// number of standard deviations for control limits
    multiplier: f64,
}

impl EwmaOutlierDetector {
    pub fn new(alpha: f64, target: f64, sigma: f64) -> Self {
        EwmaOutlierDetector {
            alpha,
            target,
            sigma,
            multiplier: EWMA_DEFAULT_MULTIPLIER,
        }
    }

    pub fn from_series(ts: &[f64], alpha: f64) -> Self {
        let training_size = (ts.len() as f64 * 0.5).min(100.0) as usize;
        let training_data = &ts[0..training_size];

        let target = calculate_mean(training_data);
        let sigma = calculate_std_dev(training_data);
        let multiplier = 3.0;

        EwmaOutlierDetector {
            alpha,
            target,
            sigma,
            multiplier,
        }
    }

    pub fn detect(&self, ts: &[f64]) -> TimeSeriesAnalysisResult<AnomalyResult> {
        let n = ts.len();
        let mut scores = Vec::with_capacity(n);
        let mut anomalies: Vec<Anomaly> = Vec::with_capacity(4);

        let mut ewma = self.target;

        for (i, &v) in ts.iter().enumerate() {
            let value = normalize_value(v);
            ewma = self.alpha * value + (1.0 - self.alpha) * ewma;

            let ewma_variance = self.sigma * self.sigma * self.alpha / (2.0 - self.alpha)
                * (1.0 - (1.0 - self.alpha).powi(2 * (i as i32 + 1)));
            let ewma_std = ewma_variance.sqrt();

            // The smoothed average's departure from target is the evidence; the
            // control limit that departure is tested against is the boundary.
            // Both grow with the observation index, which is why the limits are
            // recomputed each step rather than fixed at `multiplier * sigma`.
            let deviation = ewma - self.target;
            let boundary = self.multiplier * ewma_std;

            // A control limit at or below rounding noise is not a limit. The
            // EWMA recurrence carries its own error — `0.3*3.0 + 0.7*3.0` is not
            // exactly `3.0` — so a zero-width limit would flag every point of a
            // constant series on floating-point dust. NaN is the module's
            // "no boundary to be past" sentinel: it scores 0.0 and fails the
            // comparison below, so both agree that nothing is testable here.
            let boundary = if boundary <= f64::EPSILON {
                f64::NAN
            } else {
                boundary
            };

            let score = normalize_evidence(deviation.abs(), boundary);
            scores.push(score);

            // A NaN limit — the degenerate case above — fails this comparison,
            // which is how it stays unflagged.
            let signal = if deviation.abs() > boundary {
                if deviation > 0.0 {
                    AnomalySignal::Positive
                } else {
                    AnomalySignal::Negative
                }
            } else {
                AnomalySignal::None
            };

            if signal != AnomalySignal::None {
                anomalies.push(Anomaly {
                    index: i,
                    signal,
                    value,
                    score,
                });
            }
        }

        let distance = self.multiplier * self.sigma;
        Ok(AnomalyResult {
            scores,
            anomalies,
            threshold: self.multiplier,
            method: AnomalyMethod::Ewma,
            method_info: Some(MethodInfo::Spc {
                control_limits: (self.target - distance, self.target + distance),
                center_line: self.target,
            }),
        })
    }
}

/// EWMA anomaly detection
pub(super) fn detect_anomalies_spc_ewma(
    ts: &[f64],
    alpha: Option<f64>,
) -> TimeSeriesAnalysisResult<AnomalyResult> {
    // Ewma control chart implementation
    let alpha = alpha.unwrap_or(EWMA_DEFAULT_ALPHA);
    let detector = EwmaOutlierDetector::from_series(ts, alpha);
    detector.detect(ts)
}

/// EWMA implements only [`AnomalyDetector`], not [`PointDetector`]. It tests the
/// smoothed average against control limits that widen with the observation
/// index, so both the statistic and the limits depend on position in the series.
impl AnomalyDetector for EwmaOutlierDetector {
    fn method(&self) -> AnomalyMethod {
        AnomalyMethod::Ewma
    }

    fn model_info(&self) -> Option<MethodInfo> {
        let distance = self.multiplier * self.sigma;
        Some(MethodInfo::Spc {
            control_limits: (self.target - distance, self.target + distance),
            center_line: self.target,
        })
    }

    fn detect(&mut self, ts: &[f64]) -> TimeSeriesAnalysisResult<AnomalyResult> {
        EwmaOutlierDetector::detect(self, ts)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ewma_basic_anomaly_detection() {
        // Create baseline data with normal variation
        let mut ts: Vec<f64> = (0..50)
            .map(|i| 1.0 + 0.1 * (i as f64 * 0.1).sin())
            .collect();
        // Add clear anomalies
        ts[30] = 5.0; // Positive spike
        ts[40] = -3.0; // Negative spike

        let result = detect_anomalies_spc_ewma(&ts, Some(0.3)).unwrap();

        // Should detect both anomalies
        assert!(result.anomalies.len() >= 2);

        // find the anomalies at index 30 and 40
        let anomalies: Vec<Anomaly> = result
            .anomalies
            .iter()
            .filter(|a| a.index == 30 || a.index == 40)
            .cloned()
            .collect();

        assert!(
            anomalies[0].is_positive(),
            "Should detect positive anomaly at index 30"
        );
        assert!(
            anomalies[1].is_negative(),
            "Should detect negative anomaly at index 40"
        );

        // Anomalies should have high scores
        assert!(
            anomalies[0].score > 0.8,
            "Positive anomaly score should be > 3.0"
        );
        assert!(
            anomalies[1].score > 0.8,
            "Negative anomaly score should be > 3.0"
        );
    }

    #[test]
    fn test_ewma_varying_alpha_sensitivity() {
        // Test how different alpha values affect sensitivity
        let mut ts: Vec<f64> = (0..40)
            .map(|i| 2.0 + 0.2 * (i as f64 * 0.2).cos())
            .collect();
        ts[20] = 4.5; // Moderate outlier

        // Low alpha (more smoothing, less reactive)
        let low_alpha_result = detect_anomalies_spc_ewma(&ts, Some(0.1)).unwrap();

        // High alpha (less smoothing, more reactive)
        let high_alpha_result = detect_anomalies_spc_ewma(&ts, Some(0.8)).unwrap();

        // High alpha should be more sensitive (higher score)
        assert!(
            high_alpha_result.scores[20] >= low_alpha_result.scores[20],
            "High alpha should produce higher or equal anomaly score"
        );
    }

    #[test]
    fn test_ewma_gradual_shift() {
        // Create a time series with a gradual shift (process drift)
        let mut ts: Vec<f64> = Vec::new();

        // Normal baseline (0-29)
        for i in 0..30 {
            ts.push(1.0 + 0.05 * (i as f64).sin());
        }

        // Gradual shift (30-59)
        for i in 30..60 {
            let shift = (i - 30) as f64 * 0.1; // Gradual increase
            ts.push(1.0 + shift + 0.05 * (i as f64).sin());
        }

        let result = detect_anomalies_spc_ewma(&ts, Some(0.2)).unwrap();

        // Should detect anomalies in the shifted region
        assert!(
            result.anomalies.len() > 10,
            "Should detect anomalies in shifted region, found {}",
            result.anomalies.len()
        );
        // find the first anomaly index
        let first_anomaly_index = result.anomalies.first().map(|a| a.index).unwrap_or(0);
        assert!(
            first_anomaly_index >= 30,
            "First anomaly should be in the shifted region"
        );
    }

    /// Build a deterministic step-change series with small jitter so training
    /// variance is non-zero (avoids zero-sigma in `from_series`).
    fn make_step_change_ts(
        pre_len: usize,
        post_len: usize,
        pre_value: f64,
        post_value: f64,
        jitter_amplitude: f64,
    ) -> Vec<f64> {
        let mut ts = Vec::with_capacity(pre_len + post_len);
        for i in 0..pre_len {
            let jitter = jitter_amplitude * ((i as f64 * 0.37).sin()); // deterministic small jitter
            ts.push(pre_value + jitter);
        }
        for i in 0..post_len {
            let jitter = jitter_amplitude * ((i as f64 * 0.73).cos()); // different phase for step region
            ts.push(post_value + jitter);
        }
        ts
    }

    #[test]
    fn test_ewma_step_change() {
        // Test with abrupt step change
        let mut ts = make_step_change_ts(40, 40, 5.0, 8.0, 0.02);

        // Step change (40-79)
        ts.extend(std::iter::repeat_n(8.0, 40));

        let result = detect_anomalies_spc_ewma(&ts, Some(0.3)).unwrap();

        // Verify we detected at least one anomaly in the first few points after the step.
        // Note: `result.anomalies` is sparse (only flagged points), so filter by `index`.
        let anomalies_after_step = result
            .anomalies
            .iter()
            .filter(|a| (40..50).contains(&a.index) && a.is_anomaly())
            .count();

        assert!(
            anomalies_after_step > 0,
            "Should detect anomalies after step change"
        );

        // Optional: ensure we did not flag anomalies before the step.
        let anomalies_before_step = result
            .anomalies
            .iter()
            .filter(|a| a.index < 40 && a.is_anomaly())
            .count();

        assert_eq!(
            anomalies_before_step, 0,
            "Should not detect anomalies before step change"
        );
    }

    #[test]
    fn test_ewma_constant_series() {
        // Test with perfectly constant series (no variation)
        let ts = vec![3.0; 50];

        let result = detect_anomalies_spc_ewma(&ts, Some(0.3)).unwrap();

        // Should detect no anomalies in constant series
        assert_eq!(
            result.anomalies.len(),
            0,
            "Should detect no anomalies in constant series"
        );
    }

    #[test]
    fn test_ewma_small_sample_size() {
        // Test with minimum viable sample size
        let ts = vec![1.0, 1.1, 1.0, 5.0, 1.0, 1.1];

        let result = detect_anomalies_spc_ewma(&ts, Some(0.5)).unwrap();

        // Should detect the spike at index 3
        assert_eq!(result.anomalies.len(), 3);

        assert!(
            result.anomalies[0].is_positive(),
            "Should detect anomaly at index 3"
        );
        assert_eq!(result.anomalies[0].value, 5.0);
    }

    #[test]
    fn test_ewma_with_trend() {
        // Test with a linear upward trend
        let ts: Vec<f64> = (0..60)
            .map(|i| i as f64 * 0.1 + 0.05 * (i as f64).sin())
            .collect();

        let result = detect_anomalies_spc_ewma(&ts, Some(0.30)).unwrap();

        // Gradual trend may cause some anomalies, but not excessive
        let anomaly_count = result.anomalies.len();

        assert!(
            anomaly_count < 35,
            "Gradual trend should not cause excessive anomalies"
        );
    }

    #[test]
    fn test_ewma_method_info() {
        let ts = vec![1.0, 1.1, 1.0, 1.2, 1.1, 1.0];

        let result = detect_anomalies_spc_ewma(&ts, Some(0.3)).unwrap();

        // Verify method info is populated
        assert!(
            result.method_info.is_some(),
            "Method info should be present"
        );

        if let Some(MethodInfo::Spc {
            control_limits,
            center_line,
        }) = result.method_info
        {
            assert!(
                control_limits.0 < control_limits.1,
                "Lower limit should be less than upper"
            );
            assert!(center_line > 0.0, "Center line should be positive");
        } else {
            panic!("Expected SPC method info");
        }
    }

    #[test]
    fn test_ewma_multiple_anomalies() {
        // Test with multiple scattered anomalies
        let mut ts: Vec<f64> = (0..100)
            .map(|i| 2.0 + 0.1 * (i as f64 * 0.1).sin())
            .collect();
        // Add multiple anomalies
        ts[10] = 6.0;
        ts[30] = -2.0;
        ts[50] = 7.0;
        ts[80] = -1.0;

        let result = detect_anomalies_spc_ewma(&ts, Some(0.3)).unwrap();

        // Count detected anomalies
        let anomaly_count = result.anomalies.len();

        assert!(
            anomaly_count >= 3,
            "Should detect at least 3 anomalies, found {anomaly_count}"
        );
    }

    #[test]
    fn test_ewma_nan_handling() {
        // Test with NaN values (should be normalized to 0.0)
        let ts = vec![1.0, 1.1, f64::NAN, 1.2, 1.0, f64::NAN, 1.1];

        let result = detect_anomalies_spc_ewma(&ts, Some(0.3));

        // Should not panic and should complete successfully
        assert!(result.is_ok(), "Should handle NaN values gracefully");
    }

    #[test]
    fn test_ewma_extreme_alpha_values() {
        let ts: Vec<f64> = (0..20).map(|i| 1.0 + 0.1 * i as f64).collect();

        // Very low alpha (0.01) - highly smoothed
        let low_result = detect_anomalies_spc_ewma(&ts, Some(0.01));
        assert!(low_result.is_ok(), "Should handle very low alpha");

        // Very high alpha (0.99) - minimal smoothing
        let high_result = detect_anomalies_spc_ewma(&ts, Some(0.99));
        assert!(high_result.is_ok(), "Should handle very high alpha");
    }
}
