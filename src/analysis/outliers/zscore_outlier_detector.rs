use crate::analysis::TimeSeriesAnalysisResult;
use crate::analysis::math::calculate_mean_std_dev;
use crate::analysis::outliers::utils::{deviation_and_fence_distance, normalize_evidence};
use crate::analysis::outliers::{
    AnomalyDetector, AnomalyMethod, AnomalyResult, AnomalySignal, MethodInfo, PointDetector,
    detect_pointwise,
};

/// Outlier detector based on the Z-Score method.
/// Considers all values outside [mean - k * std_dev, mean + k * std_dev] as outliers.
#[derive(Debug)]
pub struct ZScoreOutlierDetector {
    threshold: f64,
    mean: f64,
    std_dev: f64,
    lower_fence: f64,
    upper_fence: f64,
    is_trained: bool,
}

impl Default for ZScoreOutlierDetector {
    fn default() -> Self {
        ZScoreOutlierDetector {
            threshold: Self::DEFAULT_THRESHOLD,
            mean: 0.0,
            std_dev: 0.0,
            lower_fence: f64::NAN,
            upper_fence: f64::NAN,
            is_trained: false,
        }
    }
}

impl ZScoreOutlierDetector {
    pub const DEFAULT_THRESHOLD: f64 = 3.0;

    pub fn new(threshold: f64) -> Self {
        ZScoreOutlierDetector {
            threshold,
            ..Default::default()
        }
    }

    /// Guards on `EPSILON` rather than exact zero: a standard deviation that is
    /// merely denormal still divides into an arbitrarily large z-score, which
    /// would flag every point of a series that is constant to within rounding.
    #[inline]
    fn get_zscore(&self, value: f64) -> f64 {
        if self.std_dev < f64::EPSILON {
            return 0.0;
        }
        (value - self.mean) / self.std_dev
    }

    /// Deviation from the mean, and the distance out to the fence.
    ///
    /// `|z| > T` and `|value - mean| > T * sigma` are the same test, but only
    /// the second is expressed in the units `model_info` reports as fences. The
    /// distance is read off the fences rather than recomputed, so a value
    /// sitting exactly on a reported fence produces evidence and boundary from
    /// the identical subtraction and scores exactly `0.5`.
    #[inline]
    fn deviation_and_boundary(&self, value: f64) -> (f64, f64) {
        if self.std_dev < f64::EPSILON {
            // Constant to within rounding: no scale, so nothing to be past.
            return (value - self.mean, f64::NAN);
        }
        deviation_and_fence_distance(value, self.mean, self.lower_fence, self.upper_fence)
    }

    pub fn detect(&mut self, ts: &[f64]) -> TimeSeriesAnalysisResult<AnomalyResult> {
        if !self.is_trained {
            self.train(ts)?;
        }
        Ok(detect_pointwise(self, ts, self.threshold))
    }
}

impl AnomalyDetector for ZScoreOutlierDetector {
    fn method(&self) -> AnomalyMethod {
        AnomalyMethod::ZScore
    }

    fn model_info(&self) -> Option<MethodInfo> {
        Some(MethodInfo::Fenced {
            lower_fence: self.lower_fence,
            upper_fence: self.upper_fence,
            center_line: None,
        })
    }

    fn train(&mut self, data: &[f64]) -> TimeSeriesAnalysisResult<()> {
        // maybe use welford here ?
        let (mean, std_dev) = calculate_mean_std_dev(data);

        // store computed stats
        self.mean = mean;
        self.std_dev = std_dev;

        self.lower_fence = mean - self.threshold * std_dev;
        self.upper_fence = mean + self.threshold * std_dev;
        self.is_trained = true;
        Ok(())
    }

    fn detect(&mut self, ts: &[f64]) -> TimeSeriesAnalysisResult<AnomalyResult> {
        ZScoreOutlierDetector::detect(self, ts)
    }
}

impl PointDetector for ZScoreOutlierDetector {
    /// Evidence is the departure from the mean and the boundary is the fence it
    /// is tested against, so the score crosses 0.5 exactly where `classify`
    /// starts flagging — for any threshold, rather than at 0.75 for `T=3` and
    /// 0.857 for `T=6`.
    fn score(&self, value: f64) -> f64 {
        let (deviation, boundary) = self.deviation_and_boundary(value);
        normalize_evidence(deviation.abs(), boundary)
    }

    fn classify(&self, value: f64) -> AnomalySignal {
        let (deviation, boundary) = self.deviation_and_boundary(value);
        // A NaN on either side — a missing reading, or a scale that was never
        // fitted — fails this comparison, which is how it stays unflagged.
        if deviation.abs() > boundary {
            if deviation > 0.0 {
                AnomalySignal::Positive
            } else {
                AnomalySignal::Negative
            }
        } else {
            AnomalySignal::None
        }
    }
}

/// Z-score based analysis detection using sample standard deviation
pub(super) fn detect_anomalies_zscore(
    ts: &[f64],
    threshold: Option<f64>,
) -> TimeSeriesAnalysisResult<AnomalyResult> {
    let mut detector =
        ZScoreOutlierDetector::new(threshold.unwrap_or(ZScoreOutlierDetector::DEFAULT_THRESHOLD));
    detector.train(ts)?;
    detector.detect(ts)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::analysis::outliers::{AnomalyOptions, detect_anomalies};

    #[test]
    fn test_zscore_anomaly_detection() {
        // Create a time series with clear anomalies
        let mut ts: Vec<f64> = (0..100).map(|i| (i as f64 / 10.0).sin()).collect();
        ts[25] = 5.0; // Clear anomaly
        ts[75] = -5.0; // Clear anomaly

        let result = detect_anomalies_zscore(&ts, Some(3.0)).unwrap();

        // Should detect the two anomalies
        let anomaly_count = result.anomalies.iter().filter(|&&x| x.is_anomaly()).count();
        assert!(
            anomaly_count >= 2,
            "Should detect at least 2 anomalies, found {anomaly_count}"
        );

        // Anomalies score above the 0.5 boundary. The absolute magnitude is not
        // the interesting part and used to be read as one: `|z| ≈ 5` against
        // `T = 3` is `r ≈ 1.67`, which is ~0.625 — a firm anomaly, even though
        // it looks unimpressive next to the old scale, where any point at all
        // past `T = 3` already scored 0.75.
        let score_25 = result.scores[25];
        let score_75 = result.scores[75];
        assert!(
            score_25 > 0.5,
            "index 25 should read as an anomaly: {score_25}"
        );
        assert!(
            score_75 > 0.5,
            "index 75 should read as an anomaly: {score_75}"
        );

        // And they must outrank the ordinary points around them.
        let typical = result.scores[10];
        assert!(
            typical < 0.5 && typical < score_25.min(score_75),
            "a baseline point scored {typical}, against {score_25} and {score_75}"
        );
    }

    #[test]
    fn test_zscore_anomaly_detection_strong() {
        // Most values are ~0..1; anomalies are far away.
        const STRONG_ANOMALIES: [f64; 32] = [
            0.10, 0.05, 0.12, 0.08, 0.11, 0.09, 0.07, 0.10, 0.06, 0.08, 0.09, 0.11, 0.10, 0.07,
            0.08, 0.12, 0.09, 0.10, 0.08, 0.07, 6.00, // strong positive anomaly
            0.09, 0.11, 0.08, 0.10, 0.07, 0.09, 0.10, -6.00, // strong negative anomaly
            0.08, 0.09, 0.10,
        ];

        let result = detect_anomalies_zscore(&STRONG_ANOMALIES, Some(3.0)).unwrap();
        assert_eq!(result.anomalies.len(), 2);

        let first = result.anomalies[0];
        let second = result.anomalies[1];
        assert!(first.is_positive());
        assert_eq!(first.value, 6.00);
        assert!(second.is_negative());
        assert_eq!(second.value, -6.00);
    }

    #[test]
    fn test_zscore_anomaly_detection_constant() {
        // Constant series (std dev = 0). No anomalies.
        const CONSTANT: [f64; 40] = [1.0; 40];

        let result = detect_anomalies_zscore(&CONSTANT, Some(3.0)).unwrap();

        // Should detect no anomalies
        let anomaly_count = result.anomalies.iter().filter(|&&x| x.is_anomaly()).count();
        assert_eq!(
            anomaly_count, 0,
            "Should detect no anomalies in constant series"
        );
    }

    #[test]
    fn test_zscore_anomaly_detection_noisy_spike() {
        // “Mostly normal” Gaussian-ish values with a single spike.
        const NOISY_SPIKE: [f64; 30] = [
            -0.30, 0.05, 0.12, -0.18, 0.22, 0.09, -0.11, 0.04, 0.15, -0.07, 0.08, -0.02, 0.10,
            0.01, -0.05, 0.06, 0.00, 0.11, -0.09, 0.03, 3.50, // spike
            0.07, -0.04, 0.02, 0.09, -0.08, 0.05, 0.01, -0.03, 0.04,
        ];

        let result = detect_anomalies_zscore(&NOISY_SPIKE, Some(3.0)).unwrap();

        assert_eq!(
            result.anomalies.len(),
            1,
            "Should detect exactly one anomaly"
        );
        assert_eq!(
            result.anomalies[0].value, 3.5,
            "Anomaly should be at index 20"
        );
    }

    #[test]
    fn test_zcore_anomaly_detection_small_sample_size() {
        // Small n but valid (n >= 3). One outlier.
        const SMALL_SAMPLE_SIZE: [f64; 4] = [0.0, 0.1, 0.05, 5.0];

        // because of a small sample size, use a lower threshold.
        let result = detect_anomalies_zscore(&SMALL_SAMPLE_SIZE, Some(1.3)).unwrap();

        assert_eq!(
            result.anomalies.len(),
            1,
            "Should detect exactly one anomaly"
        );
        assert_eq!(
            result.anomalies[0].value, 5.0,
            "Anomaly should be at index 3"
        );
    }

    #[test]
    fn test_scores_are_normalized_0_to_1() {
        // Any output score must be in [0..1], even for extreme values.
        let ts = [0.0, 0.1, 0.05, 1000.0, -1000.0, 0.02, 0.03];
        let result = detect_anomalies_zscore(&ts, Some(3.0)).unwrap();

        assert_eq!(result.scores.len(), ts.len());
        for (i, &s) in result.scores.iter().enumerate() {
            assert!(
                (0.0..=1.0).contains(&s),
                "Score out of range at index {i}: {s}"
            );
        }
    }

    #[test]
    fn test_anomalies_have_high_normalized_scores() {
        // Keep the original intent: anomalies should score "high" on the normalized scale.
        let mut ts: Vec<f64> = (0..100).map(|i| (i as f64 / 10.0).sin()).collect();
        ts[25] = 5.0;
        ts[75] = -5.0;

        let result = detect_anomalies_zscore(&ts, Some(3.0)).unwrap();

        let anomaly_count = result.anomalies.iter().filter(|&&x| x.is_anomaly()).count();
        assert!(
            anomaly_count >= 2,
            "Should detect at least 2 anomalies, found {anomaly_count}"
        );

        // On the normalized scale "high" means past the 0.5 boundary, which is
        // where the detector itself draws the line.
        assert!(
            result.scores[25] > 0.5,
            "Expected an above-boundary score at index 25, got {}",
            result.scores[25]
        );
        assert!(
            result.scores[75] > 0.5,
            "Expected an above-boundary score at index 75, got {}",
            result.scores[75]
        );
    }

    #[test]
    fn test_direction_and_score_zero_at_mean_like_points() {
        // Basic sanity: non-anomalous points near the mean should have low scores;
        // anomaly direction should still reflect the z-score sign.
        const TS: [f64; 6] = [0.0, 0.02, -0.01, 0.01, 6.0, -6.0];

        let result = detect_anomalies_zscore(&TS, Some(1.5)).unwrap();

        // Near-mean points should be low.
        for i in 0..4 {
            assert!(
                result.scores[i] < 0.5,
                "Expected low score near mean at index {i}, got {}",
                result.scores[i]
            );
        }

        // Extremes should be anomalies with directional signals.
        assert_eq!(
            result.anomalies[0].signal,
            AnomalySignal::Positive,
            "Expected positive anomaly at index 4"
        );
        assert_eq!(
            result.anomalies[1].signal,
            AnomalySignal::Negative,
            "Expected negative anomaly at index 5"
        );

        // And score past the boundary, which is what "high" means on this scale.
        assert!(
            result.scores[4] > 0.5,
            "Expected an above-boundary score at index 4, got {}",
            result.scores[4]
        );
        assert!(
            result.scores[5] > 0.5,
            "Expected an above-boundary score at index 5, got {}",
            result.scores[5]
        );
    }

    #[test]
    fn test_edge_cases() {
        // Test with a very short time series
        let ts = vec![1.0, 2.0];
        let options = AnomalyOptions::default();

        let result = detect_anomalies(&ts, &options);
        assert!(result.is_err());

        // Test with constant time series
        let ts = vec![1.0; 50];

        let result = detect_anomalies_zscore(&ts, Some(3.0)).unwrap();
        // Should detect no anomalies in constant series
        let anomaly_count = result.anomalies.iter().filter(|x| x.is_anomaly()).count();
        assert_eq!(
            anomaly_count, 0,
            "Should detect no anomalies in constant series"
        );
    }
}
