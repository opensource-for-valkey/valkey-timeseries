use crate::analysis::TimeSeriesAnalysisResult;
use crate::analysis::outliers::mad_estimator::{
    HarrellDavisNormalizedEstimator, InvariantMADEstimator, MedianAbsoluteDeviationEstimator,
    SimpleNormalizedEstimator,
};
use crate::analysis::outliers::utils::normalize_evidence;
use crate::analysis::outliers::{
    AnomalyDetector, AnomalyMADEstimator, AnomalyMethod, AnomalyResult, AnomalySignal, MethodInfo,
    PointDetector, detect_pointwise,
};
use crate::analysis::quantile_estimators::QuantileEstimator;
use crate::analysis::quantile_estimators::Samples;

/// Outlier detector based on the median absolute deviation.
/// Considers all values outside [median - k * Mad, median + k * Mad] as outliers.
#[derive(Debug)]
pub struct MadOutlierDetector {
    is_trained: bool,
    estimator: AnomalyMADEstimator,
    lower_fence: f64,
    upper_fence: f64,
    mad: f64,
    median: f64,
    k: f64,
}

impl Default for MadOutlierDetector {
    fn default() -> Self {
        MadOutlierDetector {
            is_trained: false,
            estimator: AnomalyMADEstimator::Simple,
            lower_fence: f64::NAN,
            upper_fence: f64::NAN,
            mad: f64::NAN,
            median: f64::NAN,
            k: Self::DEFAULT_K,
        }
    }
}

impl MadOutlierDetector {
    pub const DEFAULT_K: f64 = 3.0;

    pub fn new(k: f64, estimator: AnomalyMADEstimator) -> Self {
        Self {
            k,
            estimator,
            ..Default::default()
        }
    }

    pub fn with_estimator(estimator: AnomalyMADEstimator) -> Self {
        Self::new(Self::DEFAULT_K, estimator)
    }

    /// Returns whether a value is an outlier, according to the detector.
    pub fn is_outlier(&self, value: f64) -> bool {
        self.classify(value).is_anomaly()
    }

    /// Returns the lower fence.
    pub fn lower_fence(&self) -> f64 {
        self.lower_fence
    }

    /// Returns the upper fence.
    pub fn upper_fence(&self) -> f64 {
        self.upper_fence
    }

    /// Deviation from the median, and the distance out to the fence on the
    /// value's own side.
    ///
    /// The single source of truth for scoring and classification, so the two
    /// cannot disagree about which side of the fence a value falls on. The
    /// boundary is taken as `fence - median` rather than the algebraically equal
    /// `k * mad` so that a value sitting *exactly on the reported fence* yields
    /// evidence and boundary from the identical subtraction, and therefore
    /// scores exactly `0.5`. Recomputing `k * mad` instead leaves the two
    /// differing by a rounding step, which lands the fence on the flagged side.
    #[inline]
    fn deviation_and_boundary(&self, value: f64) -> (f64, f64) {
        let deviation = value - self.median;
        let boundary = if deviation >= 0.0 {
            self.upper_fence - self.median
        } else {
            self.median - self.lower_fence
        };
        (deviation, boundary)
    }

    /// Returns a normalized anomaly score in `[0..1]` describing how "anomalous" `value` is.
    ///
    /// Interpretation:
    /// - `0.0` means "at the median" (no deviation).
    /// - `0.5` means "exactly on the configured MAD fence" (i.e., `k * mad` away
    ///   from the median).
    /// - values approaching `1.0` lie progressively further beyond the fence.
    ///
    /// This used to `clamp(|value - median| / (k * mad), 0..1)`, which saturated
    /// at the fence: a point 3.1 MADs out and one 300 MADs out both scored
    /// exactly `1.0`, so the field advertised as a score carried no ranking at
    /// all among the samples it had flagged.
    pub fn get_anomaly_score(&self, value: f64) -> f64 {
        let (deviation, boundary) = self.deviation_and_boundary(value);
        normalize_evidence(deviation.abs(), boundary)
    }

    pub fn detect(&mut self, ts: &[f64]) -> TimeSeriesAnalysisResult<AnomalyResult> {
        Ok(detect_pointwise(self, ts, self.k))
    }
}

impl AnomalyDetector for MadOutlierDetector {
    fn method(&self) -> AnomalyMethod {
        AnomalyMethod::Mad
    }

    fn model_info(&self) -> Option<MethodInfo> {
        Some(MethodInfo::Fenced {
            lower_fence: self.lower_fence,
            upper_fence: self.upper_fence,
            center_line: None,
        })
    }

    fn train(&mut self, data: &[f64]) -> TimeSeriesAnalysisResult<()> {
        debug_assert!(!data.is_empty(), "Sample cannot be empty");
        fn get_mad_median(
            data: &[f64],
            estimator: impl MedianAbsoluteDeviationEstimator,
        ) -> (f64, f64) {
            let samples = Samples::from(data.to_vec());
            let median = estimator.quantile_estimator().median(&samples);
            let mad = estimator.mad(&samples);
            (median, mad)
        }

        let (median, mad) = match self.estimator {
            AnomalyMADEstimator::Simple => {
                let estimator = SimpleNormalizedEstimator::new();
                get_mad_median(data, estimator)
            }
            AnomalyMADEstimator::Invariant => {
                let estimator = InvariantMADEstimator::new();
                get_mad_median(data, estimator)
            }
            AnomalyMADEstimator::HarrellDavis => {
                let estimator = HarrellDavisNormalizedEstimator;
                get_mad_median(data, estimator)
            }
        };

        let k = self.k;
        self.median = median;
        self.mad = mad;
        self.lower_fence = median - k * mad;
        self.upper_fence = median + k * mad;
        self.is_trained = true;
        Ok(())
    }

    fn detect(&mut self, ts: &[f64]) -> TimeSeriesAnalysisResult<AnomalyResult> {
        MadOutlierDetector::detect(self, ts)
    }
}

impl PointDetector for MadOutlierDetector {
    fn score(&self, value: f64) -> f64 {
        MadOutlierDetector::get_anomaly_score(self, value)
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_mad_outlier_detector() {
        let data = [1.0, 2.0, 2.0, 2.0, 3.0, 14.0];
        let mut detector = MadOutlierDetector::default();
        detector.train(&data).unwrap();

        // 14.0 is an outlier
        assert!(detector.is_outlier(14.0));
        // 1.0 is not an outlier for k=3
        assert!(!detector.is_outlier(1.0));
        // 2.0 is not an outlier
        assert!(!detector.is_outlier(2.0));
    }

    #[test]
    fn test_get_anomaly_score_is_normalized() {
        let data = [1.0, 2.0, 2.0, 2.0, 3.0, 14.0];
        let mut detector = MadOutlierDetector::default();
        detector.train(&data).unwrap();

        let score_at_median = detector.get_anomaly_score(2.0);
        assert_eq!(score_at_median, 0.0);

        // Exactly at the upper fence is the detection boundary, so 0.5.
        let score_at_upper_fence = detector.get_anomaly_score(detector.upper_fence());
        assert!(
            (score_at_upper_fence - 0.5).abs() < 1e-9,
            "the fence is the boundary and must score 0.5, got {score_at_upper_fence}"
        );

        // Beyond the fence the score keeps climbing without ever reaching 1.0.
        let score_beyond = detector.get_anomaly_score(1e9);
        assert!(
            score_beyond > 0.5 && score_beyond < 1.0,
            "expected a strictly-inside-(0.5, 1.0) score past the fence, got {score_beyond}"
        );

        // A missing reading is not evidence of an anomaly.
        let score_nan = detector.get_anomaly_score(f64::NAN);
        assert_eq!(score_nan, 0.0);

        // An infinite reading is maximally anomalous, and is flagged as such.
        assert_eq!(detector.get_anomaly_score(f64::INFINITY), 1.0);
        assert!(detector.is_outlier(f64::INFINITY));
    }

    /// The saturation this replaced made every flagged sample score exactly
    /// `1.0`, so `FULL` output could not tell a marginal outlier from an extreme
    /// one.
    #[test]
    fn test_scores_rank_samples_beyond_the_fence() {
        let data = [1.0, 2.0, 2.0, 2.0, 3.0, 14.0];
        let mut detector = MadOutlierDetector::default();
        detector.train(&data).unwrap();

        let fence = detector.upper_fence();
        let span = fence - detector.median;

        let just_past = detector.get_anomaly_score(fence + span * 0.1);
        let well_past = detector.get_anomaly_score(fence + span * 10.0);
        let far_past = detector.get_anomaly_score(fence + span * 1000.0);

        assert!(
            0.5 < just_past && just_past < well_past && well_past < far_past && far_past < 1.0,
            "expected a strict ranking, got {just_past} < {well_past} < {far_past}"
        );
    }
}
