use super::utils::{normalize_evidence, normalize_value};
use crate::analysis::TimeSeriesAnalysisResult;
use crate::analysis::math::calculate_median_sorted;
use crate::analysis::outliers::{
    AnomalyDetector, AnomalyMethod, AnomalyResult, AnomalySignal, MethodInfo, PointDetector,
    detect_pointwise,
};

pub const MODIFIED_ZSCORE_DEFAULT_THRESHOLD: f64 = 3.5;

/// Modified Z-score outlier detector
#[derive(Debug, Clone, Copy)]
pub struct ModifiedZScoreOutlierDetector {
    median: f64,
    mad: f64,
    mad_scaled: f64,
    threshold: f64,
    is_trained: bool,
}

impl Default for ModifiedZScoreOutlierDetector {
    fn default() -> Self {
        ModifiedZScoreOutlierDetector {
            median: 0.0,
            mad: 0.0,
            mad_scaled: 0.0,
            threshold: MODIFIED_ZSCORE_DEFAULT_THRESHOLD,
            is_trained: false,
        }
    }
}
impl ModifiedZScoreOutlierDetector {
    pub fn new(threshold: f64) -> Self {
        ModifiedZScoreOutlierDetector {
            threshold,
            ..Default::default()
        }
    }

    /// NaN propagates rather than being substituted with `0.0`: the substitute
    /// is a real position in the data, so it scores and classifies as if the
    /// missing reading were a genuine observation at zero. A NaN statistic
    /// scores `0.0` and fails every `>` comparison in `classify`, which is the
    /// treatment a missing reading should get.
    #[inline]
    fn get_modified_zscore(&self, value: f64) -> f64 {
        if self.mad_scaled > 1e-10 {
            0.6745 * (value - self.median) / self.mad
        } else {
            0.0
        }
    }

    /// Deviation from the median, and the distance out to the fence.
    ///
    /// `|0.6745 * (v - med) / MAD| > T` is the same test as
    /// `|v - med| > T * MAD/0.6745`, but only the second form is what
    /// `model_info` reports as a fence. Deriving scoring, classification, and
    /// the reported fences from this one pair keeps them from disagreeing by a
    /// rounding step about a value sitting exactly on the fence.
    #[inline]
    fn deviation_and_boundary(&self, value: f64) -> (f64, f64) {
        let deviation = value - self.median;
        if self.mad_scaled <= 1e-10 {
            // No usable scale: nothing to be past.
            return (deviation, f64::NAN);
        }
        let (lower_fence, upper_fence) = self.fences();
        let boundary = if deviation >= 0.0 {
            upper_fence - self.median
        } else {
            self.median - lower_fence
        };
        (deviation, boundary)
    }

    /// The fences `model_info` reports, and the ones `deviation_and_boundary`
    /// measures against. One definition, so they cannot drift apart.
    #[inline]
    fn fences(&self) -> (f64, f64) {
        let delta = if self.mad_scaled > 1e-10 {
            self.threshold * self.mad_scaled
        } else {
            0.0
        };
        (self.median - delta, self.median + delta)
    }

    pub fn detect(&self, ts: &[f64]) -> TimeSeriesAnalysisResult<AnomalyResult> {
        Ok(detect_pointwise(self, ts, self.threshold))
    }
}

impl AnomalyDetector for ModifiedZScoreOutlierDetector {
    fn method(&self) -> AnomalyMethod {
        AnomalyMethod::ModifiedZScore
    }

    fn model_info(&self) -> Option<MethodInfo> {
        // Modified Z-score threshold |z| > T translates to:
        // x < median - T * (MAD / 0.6745)  OR  x > median + T * (MAD / 0.6745)
        let (lower_fence, upper_fence) = self.fences();

        Some(MethodInfo::Fenced {
            lower_fence,
            upper_fence,
            center_line: Some(self.median),
        })
    }

    fn train(&mut self, data: &[f64]) -> TimeSeriesAnalysisResult<()> {
        // Calculate median
        let mut sorted_values: Vec<f64> = data.iter().map(|&x| normalize_value(x)).collect();
        sorted_values.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
        let median = calculate_median_sorted(&sorted_values);

        // Calculate Mad (Median Absolute Deviation)
        let mut abs_deviations: Vec<f64> = data
            .iter()
            .map(|&x| (normalize_value(x) - median).abs())
            .collect();
        abs_deviations.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));

        let mad = calculate_median_sorted(&abs_deviations);

        // Scale Mad for consistency with normal distribution
        let mad_scaled = mad / 0.6745;
        self.mad_scaled = mad_scaled;
        self.median = median;
        self.mad = mad;
        self.is_trained = true;
        Ok(())
    }

    fn detect(&mut self, ts: &[f64]) -> TimeSeriesAnalysisResult<AnomalyResult> {
        ModifiedZScoreOutlierDetector::detect(self, ts)
    }
}

impl PointDetector for ModifiedZScoreOutlierDetector {
    /// Evidence is the departure from the median against the fence distance it
    /// is tested against, so the score crosses 0.5 exactly where `classify`
    /// starts flagging — for any threshold, rather than at 0.778 for `T=3.5`.
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

/// Modified Z-score using median absolute deviation
fn detect_anomalies_modified_zscore(
    ts: &[f64],
    threshold: Option<f64>,
) -> TimeSeriesAnalysisResult<AnomalyResult> {
    let threshold = threshold.unwrap_or(MODIFIED_ZSCORE_DEFAULT_THRESHOLD);
    let mut detector = ModifiedZScoreOutlierDetector::new(threshold);
    detector.train(ts)?;
    detector.detect(ts)
}

#[cfg(test)]
mod tests {
    use super::detect_anomalies_modified_zscore;

    #[test]
    fn test_modified_zscore() {
        let ts = vec![1.0, 2.0, 1.5, 2.2, 1.8, 10.0, 2.1, 1.9]; // 10.0 is an outlier

        let result = detect_anomalies_modified_zscore(&ts, Some(3.5)).unwrap();

        assert_eq!(result.anomalies.len(), 1, "Should detect one anomaly");
        // Should detect the outlier at index 5
        assert_eq!(result.anomalies[0].value, 10.0, "Should detect one anomaly");
        assert!(
            result.anomalies[0].score > 0.5,
            "an outlier must score past the 0.5 boundary, got {}",
            result.anomalies[0].score
        );
        // ...and outrank every sample that was not flagged.
        let highest_normal = result
            .scores
            .iter()
            .enumerate()
            .filter(|(i, _)| *i != 5)
            .map(|(_, s)| *s)
            .fold(0.0f64, f64::max);
        assert!(
            highest_normal < 0.5 && highest_normal < result.anomalies[0].score,
            "the outlier ({}) must outrank every in-range sample ({highest_normal})",
            result.anomalies[0].score
        );
    }
}
