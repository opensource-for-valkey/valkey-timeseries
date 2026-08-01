use super::utils::{normalize_evidence, normalize_value};
use crate::analysis::TimeSeriesAnalysisResult;
use crate::analysis::math::calculate_mean_std_dev;
use crate::analysis::outliers::{
    Anomaly, AnomalyDetector, AnomalyMethod, AnomalyResult, AnomalySignal, MethodInfo,
};

/// Statistical Process Control (Spc) cusum anomaly detection
///
/// The cusum statistic is accumulated in standardized units, so `k` and `h` are
/// dimensionless multiples of the standard deviation rather than raw data units.
/// This keeps the detector scale-free: fitting it to a series with a standard
/// deviation of 1 or of 1000 yields the same sensitivity to a shift of a given
/// number of sigmas.
pub struct CusumOutlierDetector {
    target: f64,
    std_dev: f64,
    /// Reference value ("slack"), in multiples of `std_dev`. Conventionally half
    /// the shift the chart is tuned to detect.
    k: f64,
    /// Decision interval, in multiples of `std_dev`.
    h: f64,
    is_trained: bool,
}

/// Reference value, in sigmas. Tuned to detect a sustained shift of 1 sigma.
const CUSUM_DEFAULT_K: f64 = 0.5;
/// Decision interval, in sigmas. With k = 0.5 this gives an in-control ARL of ~465.
const CUSUM_DEFAULT_H: f64 = 5.0;

impl Default for CusumOutlierDetector {
    fn default() -> Self {
        CusumOutlierDetector {
            target: 0.0,
            std_dev: 1.0,
            k: CUSUM_DEFAULT_K,
            h: CUSUM_DEFAULT_H,
            is_trained: false,
        }
    }
}

impl CusumOutlierDetector {
    pub fn with_params(mean: f64, std_dev: f64) -> Self {
        CusumOutlierDetector {
            target: mean,
            std_dev,
            is_trained: true,
            ..Default::default()
        }
    }

    pub fn from_series(ts: &[f64]) -> Self {
        let (target, std_dev) = fit_baseline(ts);
        Self::with_params(target, std_dev)
    }

    /// True when the fitted baseline supports a meaningful standardized cusum.
    /// A constant (or degenerate) baseline has no scale to measure shifts against.
    #[inline]
    fn has_valid_scale(&self) -> bool {
        self.std_dev.is_finite() && self.std_dev > f64::EPSILON && self.target.is_finite()
    }

    pub fn detect(&self, ts: &[f64]) -> TimeSeriesAnalysisResult<AnomalyResult> {
        let n = ts.len();

        // Both cusum arms and the decision interval are in sigmas, so the
        // decision interval is used directly as the threshold.
        let threshold = self.h;

        // Without a usable scale every point standardizes to infinity or NaN;
        // report a flat, anomaly-free result rather than propagating that.
        if !self.has_valid_scale() {
            return Ok(AnomalyResult {
                scores: vec![0.0; n],
                anomalies: Vec::new(),
                threshold,
                method: AnomalyMethod::Cusum,
                method_info: Some(self.method_info()),
            });
        }

        let mut scores = Vec::with_capacity(n);
        let mut anomalies: Vec<Anomaly> = Vec::with_capacity(4);

        let mut cusum_pos = 0.0;
        let mut cusum_neg = 0.0;

        for (index, &v) in ts.iter().enumerate() {
            let value = normalize_value(v);
            let deviation = (value - self.target) / self.std_dev;
            cusum_pos = f64::max(0.0, cusum_pos + deviation - self.k);
            cusum_neg = f64::max(0.0, cusum_neg - deviation - self.k);

            // Both arms and the decision interval are in sigmas, so the
            // accumulated drift is the evidence and `h` is the boundary it is
            // tested against — the same comparison the signal below makes.
            let score = normalize_evidence(f64::max(cusum_pos, cusum_neg), threshold);
            scores.push(score);

            let signal = if cusum_pos > threshold {
                AnomalySignal::Positive
            } else if cusum_neg > threshold {
                AnomalySignal::Negative
            } else {
                AnomalySignal::None
            };

            if signal.is_anomaly() {
                let outlier = Anomaly {
                    index,
                    value: v,
                    signal,
                    score,
                };
                anomalies.push(outlier);
            }
        }

        Ok(AnomalyResult {
            scores,
            anomalies,
            threshold,
            method: AnomalyMethod::Cusum,
            method_info: Some(self.method_info()),
        })
    }

    /// Control limits are reported as raw-unit offsets from the center line, so
    /// they stay on the same scale as the `center_line` reported alongside them.
    fn method_info(&self) -> MethodInfo {
        let limit = self.h * self.std_dev;
        MethodInfo::Spc {
            control_limits: (-limit, limit),
            center_line: self.target,
        }
    }
}

/// Fit the baseline mean and standard deviation from the head of the series.
fn fit_baseline(ts: &[f64]) -> (f64, f64) {
    let training_size = (ts.len() as f64 * 0.5).min(100.0) as usize;
    calculate_mean_std_dev(&ts[0..training_size])
}

/// Statistical Process Control (Spc) cusum anomaly detection
pub(super) fn detect_anomalies_spc_cusum(ts: &[f64]) -> TimeSeriesAnalysisResult<AnomalyResult> {
    let mut detector = CusumOutlierDetector::from_series(ts);
    detector.train(ts)?;
    detector.detect(ts)
}

/// CUSUM implements only [`AnomalyDetector`], not [`PointDetector`]. Its whole
/// purpose is to accumulate small deviations until they add up, so a point is
/// flagged because of the drift preceding it — a per-point test would answer a
/// different question and disagree with `detect`.
impl AnomalyDetector for CusumOutlierDetector {
    fn method(&self) -> AnomalyMethod {
        AnomalyMethod::Cusum
    }

    fn model_info(&self) -> Option<MethodInfo> {
        Some(CusumOutlierDetector::method_info(self))
    }

    fn train(&mut self, data: &[f64]) -> TimeSeriesAnalysisResult<()> {
        // `k` and `h` are in sigmas and so remain valid across any rescaling of
        // the baseline; only the baseline itself is refitted here.
        let (target, std_dev) = fit_baseline(data);
        self.target = target;
        self.std_dev = std_dev;
        self.is_trained = true;
        Ok(())
    }

    fn detect(&mut self, ts: &[f64]) -> TimeSeriesAnalysisResult<AnomalyResult> {
        CusumOutlierDetector::detect(self, ts)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_detect_empty_series() {
        let detector = CusumOutlierDetector::with_params(0.0, 1.0);
        let anomaly_result = detector.detect(&[]).unwrap();

        assert!(anomaly_result.scores.is_empty());
        assert!(anomaly_result.anomalies.is_empty());
    }

    #[test]
    fn test_detect_constant_series() {
        let ts = vec![5.0; 10];
        let detector = CusumOutlierDetector::from_series(&ts);

        let anomaly_result = detector.detect(&ts).unwrap();

        assert_eq!(anomaly_result.scores.len(), 10);
        // All scores should be low (near zero) for constant values
        for score in &anomaly_result.scores {
            assert!(*score < 0.1);
        }

        // No anomalies should be detected
        assert_eq!(anomaly_result.anomalies.len(), 0);
    }

    #[test]
    fn test_detect_positive_shift() {
        let detector = CusumOutlierDetector::with_params(0.0, 1.0);
        let mut ts = vec![0.0; 10];
        ts.extend(vec![10.0; 10]); // Positive shift

        let anomaly_result = detector.detect(&ts).unwrap();

        // Should detect positive anomalies after the shift
        let positive_anomalies = anomaly_result
            .anomalies
            .iter()
            .filter(|&&a| a.is_positive())
            .count();

        assert!(positive_anomalies > 0);
    }

    #[test]
    fn test_detect_negative_shift() {
        let detector = CusumOutlierDetector::with_params(0.0, 1.0);
        let mut ts = vec![0.0; 10];
        ts.extend(vec![-10.0; 10]); // Negative shift

        let anomaly_result = detector.detect(&ts).unwrap();

        // Should detect negative anomalies after the shift
        let negative_anomalies = anomaly_result
            .anomalies
            .iter()
            .filter(|&&a| a.is_negative())
            .count();

        assert!(negative_anomalies > 0);
    }

    #[test]
    fn test_detect_anomalies_cusum_with_positive_spike() {
        // Add variance to baseline to get valid std dev
        let mut ts = vec![10.0, 9.0, 11.0, 10.5, 9.5];
        ts.extend(vec![10.0, 9.0, 11.0, 10.5, 9.5, 9.0]);
        ts.extend(vec![50.0; 10]); // Positive spike - much larger relative to baseline
        ts.extend(vec![10.0, 9.0, 11.0, 10.5, 9.5, 8.0]);

        let result = detect_anomalies_spc_cusum(&ts);

        assert!(result.is_ok());
        let anomaly_result = result.unwrap();

        // Should detect positive anomalies during the spike
        let positive_count = anomaly_result
            .anomalies
            .iter()
            .filter(|&&a| a.is_positive())
            .count();

        assert!(
            positive_count > 0,
            "Expected positive anomalies to be detected"
        );
    }

    #[test]
    fn test_detect_anomalies_cusum_with_negative_spike() {
        // Add variance to baseline
        let mut ts = vec![10.0, 9.0, 11.0, 10.5, 9.5];
        ts.extend(vec![10.0, 9.0, 11.0, 10.5, 9.5, 9.0]);
        ts.extend(vec![-30.0; 10]); // Negative spike - much larger deviation
        ts.extend(vec![10.0, 9.0, 11.0, 10.5, 9.5, 8.0]);

        let anomaly_result = detect_anomalies_spc_cusum(&ts).unwrap();

        // Should detect negative anomalies during the spike
        let negative_count = anomaly_result
            .anomalies
            .iter()
            .filter(|&&a| a.is_negative())
            .count();

        assert!(
            negative_count > 0,
            "Expected negative anomalies to be detected"
        );
    }

    #[test]
    fn test_detect_with_zero_std_dev() {
        let ts = vec![5.0, 5.0, 5.0, 5.0, 5.0, 4.9, 5.1, 5.0, 5.0];
        let detector = CusumOutlierDetector::from_series(&ts);

        let anomaly_result = detector.detect(&ts).unwrap();

        // All scores should be zero when std_dev is invalid
        for score in &anomaly_result.scores {
            assert_eq!(*score, 0.0);
        }
    }

    #[test]
    fn test_detect_with_nan_values() {
        let detector = CusumOutlierDetector::with_params(0.0, 1.0);
        let ts = vec![0.0, f64::NAN, 0.0];

        let anomaly_result = detector.detect(&ts).unwrap();

        // Should handle NaN values gracefully
        assert_eq!(anomaly_result.scores.len(), 3);
    }

    #[test]
    fn test_detect_scores_normalized() {
        let detector = CusumOutlierDetector::with_params(0.0, 1.0);
        let ts = vec![0.0, 5.0, 10.0, 15.0, 20.0];

        let anomaly_result = detector.detect(&ts).unwrap();

        // All scores should be in [0, 1] range
        for score in &anomaly_result.scores {
            assert!(*score >= 0.0 && *score <= 1.0);
        }
    }

    #[test]
    fn test_detect_method_metadata() {
        let detector = CusumOutlierDetector::with_params(10.0, 2.0);
        let ts = vec![10.0; 5];

        let anomaly_result = detector.detect(&ts).unwrap();

        assert_eq!(anomaly_result.method, AnomalyMethod::Cusum);
        assert!(anomaly_result.method_info.is_some());

        if let Some(MethodInfo::Spc {
            control_limits,
            center_line,
        }) = anomaly_result.method_info
        {
            assert_eq!(center_line, 10.0);
            // Control limits are raw-unit offsets: h sigmas scaled by std_dev.
            assert_eq!(control_limits.0, -detector.h * detector.std_dev);
            assert_eq!(control_limits.1, detector.h * detector.std_dev);
        } else {
            panic!("Expected Spc method info");
        }
    }

    #[test]
    fn test_detect_threshold_calculation() {
        let std_dev = 2.0;
        let detector = CusumOutlierDetector::with_params(0.0, std_dev);
        let ts = vec![0.0; 5];

        let result = detector.detect(&ts);
        assert!(result.is_ok());
        let anomaly_result = result.unwrap();

        // The threshold is the decision interval in sigmas, independent of scale.
        assert_eq!(anomaly_result.threshold, CUSUM_DEFAULT_H);
    }

    #[test]
    fn test_detect_anomalies_cusum_with_gradual_shift() {
        let mut ts: Vec<f64> = (0..50).map(|i| i as f64).collect();
        ts.extend((50..100).map(|i| (i + 50) as f64)); // Gradual upward shift

        let anomaly_result = detect_anomalies_spc_cusum(&ts).unwrap();
        assert_eq!(anomaly_result.scores.len(), 100);

        // Should detect the shift
        let positive_count = anomaly_result
            .anomalies
            .iter()
            .filter(|&&a| a.is_positive())
            .count();

        assert!(positive_count > 0);
    }

    #[test]
    fn test_detect_anomalies_cusum_normal_distribution() {
        // Test with normally distributed data
        let ts = vec![
            100.0, 102.0, 98.0, 101.0, 99.0, 100.0, 103.0, 97.0, 100.0, 101.0, 99.0, 100.0, 102.0,
            98.0, 100.0, 101.0, 99.0, 100.0,
        ];

        let anomaly_result = detect_anomalies_spc_cusum(&ts).unwrap();

        let anomaly_count = anomaly_result.anomalies.len();
        // Most values should not be anomalies in normal distribution
        let no_anomaly_count = ts.len() - anomaly_count;

        assert!(no_anomaly_count > ts.len() / 2);
    }

    /// Build a baseline with unit-ish variation followed by a sustained shift,
    /// then scale the whole series by `scale`.
    fn shifted_series(scale: f64) -> Vec<f64> {
        let mut ts: Vec<f64> = (0..40)
            .map(|i| if i % 2 == 0 { 1.0 } else { -1.0 })
            .collect();
        ts.extend((0..20).map(|i| if i % 2 == 0 { 5.0 } else { 3.0 }));
        ts.iter().map(|v| v * scale).collect()
    }

    /// Regression: `train` used to refit the baseline while leaving `k`/`h` at
    /// whatever the constructor set, so a detector built via `default()` kept
    /// absolute limits and its sensitivity tracked the scale of the data.
    #[test]
    fn test_detect_is_scale_invariant_after_train() {
        let counts: Vec<usize> = [1.0, 100.0, 10_000.0]
            .iter()
            .map(|&scale| {
                let ts = shifted_series(scale);
                let mut detector = CusumOutlierDetector::default();
                detector.train(&ts).unwrap();
                detector.detect(&ts).unwrap().anomalies.len()
            })
            .collect();

        assert!(
            counts[0] > 0,
            "expected the sustained shift to be detected at unit scale"
        );
        assert!(
            counts.iter().all(|&c| c == counts[0]),
            "anomaly count must not depend on the scale of the data, got {counts:?}"
        );
    }

    /// The same series fitted through `default() + train` and through
    /// `from_series` must agree; previously the two paths disagreed because only
    /// `from_series` scaled `k`/`h` by the standard deviation.
    #[test]
    fn test_train_and_from_series_agree() {
        let ts = shifted_series(7.0);

        let mut trained = CusumOutlierDetector::default();
        trained.train(&ts).unwrap();

        let fitted = CusumOutlierDetector::from_series(&ts);

        assert_eq!(trained.target, fitted.target);
        assert_eq!(trained.std_dev, fitted.std_dev);
        assert_eq!(trained.k, fitted.k);
        assert_eq!(trained.h, fitted.h);
        assert_eq!(
            trained.detect(&ts).unwrap().anomalies.len(),
            fitted.detect(&ts).unwrap().anomalies.len()
        );
    }

    #[test]
    fn test_train_marks_detector_trained() {
        let mut detector = CusumOutlierDetector::default();
        assert!(!detector.is_trained);
        detector.train(&shifted_series(1.0)).unwrap();
        assert!(detector.is_trained);
    }

    #[test]
    fn test_detect_anomalies_cusum_method_info() {
        let ts = vec![1.0, 2.0, 3.0, 4.0, 5.0];

        let anomaly_result = detect_anomalies_spc_cusum(&ts).unwrap();

        assert_eq!(anomaly_result.method, AnomalyMethod::Cusum);
        assert!(anomaly_result.method_info.is_some());

        if let Some(MethodInfo::Spc {
            control_limits,
            center_line,
        }) = anomaly_result.method_info
        {
            assert!(control_limits.0 < 0.0);
            assert!(control_limits.1 > 0.0);
            assert!(center_line.is_finite());
        } else {
            panic!("Expected Spc method info");
        }
    }
}
