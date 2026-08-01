use super::utils::{deviation_and_fence_distance, normalize_evidence, normalize_value};
use crate::analysis::TimeSeriesAnalysisResult;
use crate::analysis::outliers::{
    AnomalyDetector, AnomalyMethod, AnomalyResult, AnomalySignal, MethodInfo, PointDetector,
    detect_pointwise,
};

pub const IQR_DEFAULT_THRESHOLD: f64 = 1.5;

/// Interquartile Range (IQR) outlier detector
#[derive(Debug)]
pub struct IQROutlierDetector {
    lower_fence: f64,
    upper_fence: f64,
    /// Quartile midpoint `(Q1 + Q3) / 2`. Both fences extend from their own
    /// quartile by the same `T * IQR`, so they sit symmetrically about this
    /// point — which makes it the center evidence is measured from.
    center: f64,
    iqr: f64,
    threshold: f64,
}

impl IQROutlierDetector {
    pub fn new(ts: &[f64], threshold: f64) -> Self {
        let n = ts.len();

        // Calculate quartiles
        let mut sorted_values: Vec<f64> = ts.iter().map(|&x| normalize_value(x)).collect();
        sorted_values.sort_by(|&a, &b| a.partial_cmp(&b).unwrap_or(std::cmp::Ordering::Equal));

        let q1_idx = n / 4;
        let q3_idx = 3 * n / 4;
        let q1 = sorted_values[q1_idx];
        let q3 = sorted_values[q3_idx];
        let iqr = q3 - q1;

        let lower_fence = q1 - threshold * iqr;
        let upper_fence = q3 + threshold * iqr;

        IQROutlierDetector {
            lower_fence,
            upper_fence,
            center: (q1 + q3) / 2.0,
            iqr,
            threshold,
        }
    }

    /// Deviation from the quartile midpoint, and the distance out to the fence.
    ///
    /// `upper_fence - center == center - lower_fence == IQR * (0.5 + T)`, since
    /// each fence extends `T * IQR` past its own quartile. The distance is read
    /// off the fences rather than recomputed, so a value sitting exactly on a
    /// reported fence produces evidence and boundary from the identical
    /// subtraction and scores exactly `0.5`. Scoring and classification both
    /// read this pair, so they cannot disagree about which side a value is on.
    #[inline]
    fn deviation_and_boundary(&self, value: f64) -> (f64, f64) {
        deviation_and_fence_distance(value, self.center, self.lower_fence, self.upper_fence)
    }

    pub fn detect(&mut self, ts: &[f64]) -> TimeSeriesAnalysisResult<AnomalyResult> {
        Ok(detect_pointwise(self, ts, self.threshold))
    }
}

impl AnomalyDetector for IQROutlierDetector {
    fn method(&self) -> AnomalyMethod {
        AnomalyMethod::InterquartileRange
    }

    fn model_info(&self) -> Option<MethodInfo> {
        Some(MethodInfo::Fenced {
            lower_fence: self.lower_fence,
            upper_fence: self.upper_fence,
            center_line: Some(self.center),
        })
    }

    fn detect(&mut self, ts: &[f64]) -> TimeSeriesAnalysisResult<AnomalyResult> {
        IQROutlierDetector::detect(self, ts)
    }
}

impl PointDetector for IQROutlierDetector {
    /// Evidence is the distance from the quartile midpoint, not the distance
    /// *past* the fence.
    ///
    /// Measuring past the fence meant every in-range sample scored exactly
    /// `0.0` — the majority of any series, and the whole purpose of `FULL`
    /// output. A near-miss was indistinguishable from a sample sitting on the
    /// median.
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

/// Interquartile Range (IQR) anomaly detection
pub(super) fn detect_anomalies_iqr(
    ts: &[f64],
    threshold: Option<f64>,
) -> TimeSeriesAnalysisResult<AnomalyResult> {
    let mut detector: IQROutlierDetector =
        IQROutlierDetector::new(ts, threshold.unwrap_or(IQR_DEFAULT_THRESHOLD));
    detector.detect(ts)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::analysis::outliers::MethodInfo;
    use crate::analysis::outliers::iqr_outlier_detector::detect_anomalies_iqr;

    /// A negative threshold inverts the fences. Before `deviation_and_boundary`
    /// mapped a negative fence distance to NaN, this flagged essentially every
    /// point — including the quartile midpoint itself — while still scoring it
    /// `0.0`, since `normalize_evidence` already treated the negative boundary
    /// as unusable. Both must now agree that there is nothing to be past.
    #[test]
    fn negative_threshold_does_not_flag_the_center() {
        let values: Vec<f64> = (0..24).map(|i| 40.0 + (i % 6) as f64).collect();
        let detector = IQROutlierDetector::new(&values, -1.5);

        assert_eq!(detector.classify(detector.center), AnomalySignal::None);
        assert_eq!(detector.score(detector.center), 0.0);
    }

    #[test]
    fn test_iqr_anomaly_detection() {
        let mut ts = vec![1.0; 100];
        ts[50] = 10.0; // Clear outlier

        let result = detect_anomalies_iqr(&ts, Some(1.5)).unwrap();

        assert_eq!(result.anomalies.len(), 1);
        assert!(
            result.anomalies[0].is_anomaly(),
            "Should detect anomaly at index 50"
        );
        assert_eq!(
            result.anomalies[0].index, 50,
            "Should detect anomaly at index 50"
        );
    }

    #[test]
    fn test_iqr_constant_series() {
        // All values are the same - no outliers
        let values = vec![5.0; 100];

        let result = detect_anomalies_iqr(&values, Some(1.5)).unwrap();

        assert_eq!(
            result.anomalies.len(),
            0,
            "Constant series should have no outliers"
        );
    }

    #[test]
    fn test_iqr_score_normalization() {
        let values = vec![1.0, 2.0, 1.5, 2.2, 1.8, 10.0, 2.1, 1.9];

        let result = detect_anomalies_iqr(&values, Some(1.5)).unwrap();

        // All scores should be non-negative and finite
        for score in &result.scores {
            assert!(score.is_finite(), "Score should be finite");
            assert!(*score >= 0.0, "Score should be non-negative");
        }
    }

    /// In-range samples must be graded, not flattened.
    ///
    /// Evidence used to be the distance *past* the fence, so every sample inside
    /// the fences scored exactly `0.0` — the majority of any series, and the
    /// whole reason `FULL` output has a score column. A near-miss was
    /// indistinguishable from a sample sitting on the median.
    #[test]
    fn test_iqr_grades_samples_inside_the_fences() {
        let values: Vec<f64> = (0..24).map(|i| 40.0 + (i % 6) as f64).collect();

        let detector = IQROutlierDetector::new(&values, 1.5);
        let result = detect_anomalies_iqr(&values, Some(1.5)).unwrap();

        assert!(
            result.anomalies.is_empty(),
            "no sample in this spread is outside the fences, got {:?}",
            result.anomalies
        );

        // Nothing was flagged, so every score sits at or below the boundary...
        for (i, &score) in result.scores.iter().enumerate() {
            assert!(
                score <= 0.5,
                "unflagged sample {i} scored {score}, above the boundary"
            );
        }

        // ...and they are still ranked by distance from the center, which used
        // to be a column of identical zeros.
        let center = detector.center;
        let at_center = detector.score(center);
        let near = detector.score(center + 1.0);
        let far = detector.score(center + 2.0);

        assert_eq!(at_center, 0.0, "a sample at the center is maximally normal");
        assert!(
            at_center < near && near < far && far < 0.5,
            "expected in-range samples to be ranked, got {at_center} < {near} < {far}"
        );
    }

    /// The score crosses 0.5 exactly at the fences the method reports.
    #[test]
    fn test_iqr_boundary_sits_at_one_half() {
        let values: Vec<f64> = (0..24).map(|i| 40.0 + (i % 6) as f64).collect();

        let mut detector = IQROutlierDetector::new(&values, 1.5);
        let result = detector.detect(&values).unwrap();

        let Some(MethodInfo::Fenced {
            lower_fence,
            upper_fence,
            ..
        }) = result.method_info
        else {
            panic!("IQR reports fences");
        };

        for fence in [lower_fence, upper_fence] {
            assert!(
                (detector.score(fence) - 0.5).abs() < 1e-12,
                "the fence at {fence} scored {}, expected exactly 0.5",
                detector.score(fence)
            );
            assert_eq!(
                detector.classify(fence),
                AnomalySignal::None,
                "the fence belongs to the not-flagged side"
            );
        }
    }

    #[test]
    fn test_iqr_both_direction_outliers() {
        // Series with both high and low outliers
        let values = vec![
            50.0, 51.0, 49.0, 52.0, 48.0,  // Normal range
            100.0, // High outlier
            50.0, 49.0, 51.0, // Normal range
            5.0,  // Low outlier
            50.0, 52.0, // Normal range
        ];

        let result = detect_anomalies_iqr(&values, Some(1.5)).unwrap();

        assert_eq!(result.anomalies.len(), 2, "Should detect two outliers");
        // Check high outlier
        assert!(
            result.anomalies[0].is_positive(),
            "Expected positive outlier at index 5"
        );
        assert_eq!(
            result.anomalies[0].index, 5,
            "Outlier should have occurred at index 5"
        );
        assert_eq!(
            result.anomalies[0].value, 100.0,
            "Should detect 100.0 as high outlier"
        );

        // Check low outlier
        assert!(
            result.anomalies[1].is_negative(),
            "Expected negative outlier at index 9"
        );
        assert_eq!(
            result.anomalies[1].index, 9,
            "Outlier should have occurred at index 9"
        );
        assert_eq!(
            result.anomalies[1].value, 5.0,
            "Should detect 5.0 as low outlier"
        );
    }

    #[test]
    fn test_iqr_edge_values_near_fences() {
        // Values just inside the fences should not be anomalies
        let values = vec![1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0];

        let result = detect_anomalies_iqr(&values, Some(1.5)).unwrap();

        if let Some(MethodInfo::Fenced {
            lower_fence,
            upper_fence,
            ..
        }) = result.method_info
        {
            // Check that extreme values in the dataset are classified correctly
            // relative to the fences
            for (i, &value) in values.iter().enumerate() {
                let is_outside = value < lower_fence || value > upper_fence;
                if is_outside {
                    assert!(
                        result.anomalies.iter().any(|a| a.index == i),
                        "Value {value} at index {i} should be an anomaly",
                    );
                    continue;
                } else {
                    assert!(
                        result.anomalies.iter().all(|a| a.index != i),
                        "Value {value} at index {i} should not be an anomaly"
                    );
                }
            }
        }
    }

    #[test]
    fn test_iqr_large_dataset() {
        // Test with a larger dataset (100+ points)
        let mut values: Vec<f64> = (0..100).map(|i| 50.0 + (i as f64 * 0.1).sin()).collect();

        // Add outliers
        values[25] = 100.0;
        values[75] = 0.0;

        let result = detect_anomalies_iqr(&values, Some(1.5)).unwrap();

        assert_eq!(result.anomalies.len(), 2);
        assert!(
            result.anomalies[0].is_positive(),
            "Expected anomaly at index 25"
        );
        assert_eq!(
            result.anomalies[0].index, 25,
            "Expected anomaly at index 25"
        );
        assert!(
            result.anomalies[1].is_negative(),
            "Expected anomaly at index 75"
        );
        assert_eq!(
            result.anomalies[1].index, 75,
            "Expected anomaly at index 75"
        );
    }

    #[test]
    fn test_iqr_zero_iqr_edge_case() {
        // When all values are the same, IQR = 0
        // This tests the degenerate case handling
        let values = vec![42.0; 50];

        let result = detect_anomalies_iqr(&values, Some(1.5)).unwrap();

        // Should handle gracefully without division by zero
        for score in &result.scores {
            assert!(score.is_finite());
            assert_eq!(*score, 0.0, "All scores should be zero for constant series");
        }
    }
}
