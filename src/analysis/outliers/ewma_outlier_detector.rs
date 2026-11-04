use super::utils::{
    detect_own_departures, normalize_evidence, snap_dust, snap_dust_if_collapsed,
    usable_fence_distance,
};
use crate::analysis::math::{calculate_mean, calculate_std_dev, is_constant};
use crate::analysis::outliers::{
    Anomaly, AnomalyDetector, AnomalyMethod, AnomalyResult, AnomalySignal, MethodInfo,
};
use crate::analysis::{TimeSeriesAnalysisError, TimeSeriesAnalysisResult};

/// Default alpha for Ewma SPC
pub const EWMA_DEFAULT_ALPHA: f64 = 0.3;

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
    /// Fits the control chart to the head of `ts`.
    ///
    /// `alpha` is a smoothing weight and must lie in `(0, 1]`: at `0` the average
    /// never moves, and above `1` the update overshoots the sample it is pulled
    /// toward (at `2` the variance factor `alpha / (2 - alpha)` is not finite).
    pub fn from_series(ts: &[f64], alpha: f64) -> TimeSeriesAnalysisResult<Self> {
        if !(alpha > 0.0 && alpha <= 1.0) {
            return Err(TimeSeriesAnalysisError::InvalidParameter {
                name: "alpha".to_string(),
                message: format!("alpha must be in the range (0, 1], got {alpha}"),
            });
        }

        // At least two samples: one fits a zero scale by construction.
        let training_size = ((ts.len() as f64 * 0.5).min(100.0) as usize)
            .max(2)
            .min(ts.len());
        let training_data = &ts[0..training_size];

        // A constant head has an exact answer. The computed mean is a few ULPs off, which
        // makes the standard deviation that same rounding error rather than zero, and puts
        // the target beside the value it was fitted to.
        let (target, sigma) = match training_data.first() {
            Some(&first) if is_constant(training_data) => (first, 0.0),
            _ => (
                calculate_mean(training_data),
                calculate_std_dev(training_data),
            ),
        };
        // The floor stays as a backstop for a head that is constant but for rounding.
        let sigma = snap_dust(sigma, target);
        let multiplier = 3.0;

        Ok(EwmaOutlierDetector {
            alpha,
            target,
            sigma,
            multiplier,
        })
    }

    pub fn detect(&self, ts: &[f64]) -> TimeSeriesAnalysisResult<AnomalyResult> {
        let (scores, anomalies) = if self.is_collapsed() {
            detect_own_departures(ts, self.target)
        } else {
            self.detect_smoothed(ts)
        };

        Ok(AnomalyResult {
            scores,
            anomalies,
            threshold: self.multiplier,
            method: AnomalyMethod::Ewma,
            method_info: self.model_info(),
        })
    }

    /// Half-width of the control limits once the smoothed average has been running
    /// long enough for its variance to settle: `multiplier * sigma * sqrt(alpha / (2 - alpha))`.
    ///
    /// The limits applied at each position are narrower than this and converge to it
    /// (see `detect_smoothed`), so this is the width the chart tests against in the
    /// long run, and the one `model_info` reports.
    fn steady_state_half_width(&self) -> f64 {
        self.multiplier * (self.sigma * (self.alpha / (2.0 - self.alpha)).sqrt())
    }

    /// True when the fitted sigma leaves the control limits with zero width.
    fn is_collapsed(&self) -> bool {
        self.target.is_finite()
            && usable_fence_distance(self.target, self.steady_state_half_width()) == 0.0
    }

    fn detect_smoothed(&self, ts: &[f64]) -> (Vec<f64>, Vec<Anomaly>) {
        let mut scores = Vec::with_capacity(ts.len());
        let mut anomalies: Vec<Anomaly> = Vec::with_capacity(4);

        let mut ewma = self.target;
        // Observations folded into `ewma` so far. A missing reading is not one: it
        // leaves the average alone and the control limits where they were, rather
        // than widening them for a sample that never informed the average.
        let mut observed: i32 = 0;

        for (i, &v) in ts.iter().enumerate() {
            if v.is_nan() {
                scores.push(0.0);
                continue;
            }
            observed += 1;
            // The update form, not `alpha * value + (1 - alpha) * ewma`: at a small
            // alpha that form's rounding bias accumulates to hundreds of ULPs on a
            // constant series, whereas here a sample equal to the average moves it
            // by exactly nothing.
            ewma += self.alpha * (v - ewma);

            // Sigma scales the root rather than being squared inside it: `sigma * sigma`
            // underflows below ~1e-154 and overflows above ~1e154, whatever the
            // limit it describes.
            let ewma_std = self.sigma
                * (self.alpha / (2.0 - self.alpha) * (1.0 - (1.0 - self.alpha).powi(2 * observed)))
                    .sqrt();

            // The smoothed average's departure from target is the evidence; the
            // control limit that departure is tested against is the boundary.
            // Both grow with the observation index, which is why the limits are
            // recomputed each step rather than fixed at `multiplier * sigma`.
            //
            // Even with a real sigma, the limit can be lost to rounding in the
            // target while it is still narrow. Rounding error in the average must
            // not read as a departure then.
            let boundary = usable_fence_distance(self.target, self.multiplier * ewma_std);
            let deviation = snap_dust_if_collapsed(ewma - self.target, boundary, self.target);

            let score = normalize_evidence(deviation.abs(), boundary);
            scores.push(score);

            // A NaN limit — no usable sigma — fails this comparison, which is how
            // it stays unflagged.
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
                    value: v,
                    score,
                });
            }
        }

        (scores, anomalies)
    }
}

/// EWMA implements only [`AnomalyDetector`], not [`PointDetector`]. It tests the
/// smoothed average against control limits that widen with the observation
/// index, so both the statistic and the limits depend on position in the series.
impl AnomalyDetector for EwmaOutlierDetector {
    fn method(&self) -> AnomalyMethod {
        AnomalyMethod::Ewma
    }

    fn model_info(&self) -> Option<MethodInfo> {
        let half_width = self.steady_state_half_width();
        Some(MethodInfo::Spc {
            control_limits: (self.target - half_width, self.target + half_width),
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

    /// EWMA anomaly detection
    fn detect_anomalies_spc_ewma(
        ts: &[f64],
        alpha: Option<f64>,
    ) -> TimeSeriesAnalysisResult<AnomalyResult> {
        // Ewma control chart implementation
        let alpha = alpha.unwrap_or(EWMA_DEFAULT_ALPHA);
        let detector = EwmaOutlierDetector::from_series(ts, alpha)?;
        detector.detect(ts)
    }

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
        let ts = vec![1.0, 1.1, f64::NAN, 1.2, 1.0, f64::NAN, 1.1];

        let result = detect_anomalies_spc_ewma(&ts, Some(0.3));

        // Should not panic and should complete successfully
        assert!(result.is_ok(), "Should handle NaN values gracefully");
    }

    /// A missing reading is not an observation: the average keeps its value and the
    /// control limits do not widen for it. Scores and flags after the gap are those
    /// of the series with the gap removed.
    #[test]
    fn test_ewma_nan_leaves_the_state_untouched() {
        let baseline: Vec<f64> = (0..40)
            .map(|i| 1.0 + 0.1 * (i as f64 * 0.7).sin())
            .collect();
        let detector = EwmaOutlierDetector::from_series(&baseline, 0.3).unwrap();

        // A sustained shift, so the average is mid-move when the gap arrives.
        let shifted = vec![1.6; 12];
        let mut with_gap = shifted.clone();
        with_gap.insert(4, f64::NAN);

        let clean = detector.detect(&shifted).unwrap();
        let gapped = detector.detect(&with_gap).unwrap();

        assert!(
            !clean.anomalies.is_empty(),
            "the shift must leave the limits"
        );
        let mut expected_scores = clean.scores.clone();
        expected_scores.insert(4, 0.0);
        assert_eq!(gapped.scores, expected_scores);

        let place = |index: usize| if index < 4 { index } else { index + 1 };
        let expected_flags: Vec<_> = clean
            .anomalies
            .iter()
            .map(|a| (place(a.index), a.signal, a.score))
            .collect();
        let flags: Vec<_> = gapped
            .anomalies
            .iter()
            .map(|a| (a.index, a.signal, a.score))
            .collect();
        assert_eq!(flags, expected_flags);
        assert!(gapped.anomalies.iter().all(|a| a.index != 4));
    }

    /// Zero-width limits take the same view of a gap: nothing to score, nothing to flag.
    #[test]
    fn test_ewma_nan_is_not_a_departure_when_the_scale_has_collapsed() {
        let detector = EwmaOutlierDetector::from_series(&[5.0; 10], 0.3).unwrap();

        let result = detector.detect(&[5.0, f64::NAN, 5.0]).unwrap();

        assert_eq!(result.scores, vec![0.0; 3]);
        assert!(result.anomalies.is_empty());
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

    fn spc_limits(info: Option<MethodInfo>) -> ((f64, f64), f64) {
        let Some(MethodInfo::Spc {
            control_limits,
            center_line,
        }) = info
        else {
            panic!("EWMA must report SPC method info, got {info:?}");
        };
        (control_limits, center_line)
    }

    #[test]
    fn reported_limits_equal_the_steady_state_detection_limit() {
        let ts: Vec<f64> = (0..400)
            .map(|i| 1.0 + 0.1 * (i as f64 * 0.1).sin())
            .collect();
        let d = EwmaOutlierDetector::from_series(&ts, 0.3).unwrap();
        let Some(MethodInfo::Spc {
            control_limits,
            center_line,
        }) = d.model_info()
        else {
            panic!()
        };
        let w = d.multiplier * d.sigma * (0.3f64 / 1.7).sqrt();
        assert!((control_limits.1 - (center_line + w)).abs() < 1e-12);
        assert!((control_limits.0 - (center_line - w)).abs() < 1e-12);
    }

    #[test]
    fn detect_reports_the_limits_model_info_reports() {
        let smooth: Vec<f64> = (0..100)
            .map(|i| 1.0 + 0.1 * (i as f64 * 0.1).sin())
            .collect();
        let constant = vec![3.0; 20];
        for ts in [&smooth, &constant] {
            let d = EwmaOutlierDetector::from_series(ts, 0.3).unwrap();
            let from_detect = spc_limits(d.detect(ts).unwrap().method_info);
            let from_info = spc_limits(d.model_info());
            assert_eq!(from_detect, from_info);
        }
    }

    #[test]
    fn collapsed_chart_reports_zero_width_limits_and_flags_any_departure() {
        let mut ts = vec![3.0; 20];
        ts[12] = 3.5;
        let d = EwmaOutlierDetector::from_series(&ts[..10], 0.3).unwrap();
        assert!(d.is_collapsed());

        let result = d.detect(&ts).unwrap();
        let ((lo, hi), center) = spc_limits(result.method_info);
        assert_eq!((lo, hi, center), (3.0, 3.0, 3.0));
        let flagged: Vec<usize> = result.anomalies.iter().map(|a| a.index).collect();
        assert_eq!(flagged, vec![12]);
    }

    /// The per-step limit takes no square of sigma, so a spike is found at a
    /// scale where `sigma * sigma` underflows to zero (flagging every wobble) or
    /// overflows to infinity (flagging nothing).
    #[test]
    fn spike_is_found_at_extreme_scales() {
        fn flagged_at(scale: f64) -> Vec<usize> {
            let mut ts: Vec<f64> = (0..100)
                .map(|i| scale * (1.0 + 0.02 * (i as f64 * 0.1).sin()))
                .collect();
            ts[60] = scale * 5.0;
            // The fitted sigma is built directly: `calculate_variance` squares its
            // deviations, which is out of range at these scales on its own.
            let d = EwmaOutlierDetector {
                alpha: 0.3,
                target: scale,
                sigma: scale * 0.035,
                multiplier: 3.0,
            };
            assert!(!d.is_collapsed(), "scale {scale}");
            d.detect(&ts)
                .unwrap()
                .anomalies
                .iter()
                .map(|a| a.index)
                .collect()
        }

        let reference = flagged_at(1.0);
        assert!(reference.contains(&60), "reference: {reference:?}");
        assert!(
            reference.iter().all(|&i| i >= 60),
            "reference: {reference:?}"
        );
        for scale in [1e-200, 1e200] {
            assert_eq!(flagged_at(scale), reference, "scale {scale:e}");
        }
    }

    /// A constant head has an exact answer: its own value, with no scale. The computed
    /// mean of such a head is a few ULPs off, which the dust floor would hide in the
    /// sigma but not in the target.
    #[test]
    fn from_series_fits_a_constant_head_exactly() {
        for value in [0.1, 1.0 / 3.0, 1e-3, 123_456.789, -7.7] {
            let mut ts = vec![value; 100];
            ts.extend((0..100).map(|i| value + (i as f64 * 0.1).sin()));
            let d = EwmaOutlierDetector::from_series(&ts, EWMA_DEFAULT_ALPHA).unwrap();
            assert_eq!((d.target, d.sigma), (value, 0.0), "value {value}");
        }
    }

    #[test]
    fn from_series_rejects_alpha_outside_the_half_open_unit_interval() {
        for alpha in [0.0, -1.0, 1.5, f64::NAN, f64::INFINITY] {
            let err = EwmaOutlierDetector::from_series(&[1.0, 2.0, 3.0, 4.0], alpha)
                .expect_err(&format!("alpha {alpha} must be rejected"));
            assert!(
                matches!(
                    err,
                    TimeSeriesAnalysisError::InvalidParameter { ref name, .. } if name == "alpha"
                ),
                "{alpha}: {err:?}"
            );
        }
        assert!(EwmaOutlierDetector::from_series(&[1.0, 2.0, 3.0, 4.0], 1.0).is_ok());
    }
}
