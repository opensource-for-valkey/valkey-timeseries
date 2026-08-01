use super::utils::{deviation_and_fence_distance, normalize_evidence};
use crate::analysis::TimeSeriesAnalysisResult;
use crate::analysis::outliers::mad_estimator::{
    HarrellDavisNormalizedEstimator, InvariantMADEstimator, MedianAbsoluteDeviationEstimator,
    SimpleNormalizedEstimator,
};
use crate::analysis::outliers::{
    AnomalyDetector, AnomalyMADEstimator, AnomalyMethod, AnomalyResult, AnomalySignal,
    MADAnomalyOptions, MethodInfo, PointDetector, detect_pointwise,
};
use crate::analysis::quantile_estimators::QuantileEstimator;
use crate::analysis::quantile_estimators::Samples;

/// Outlier detector based on the double median absolute deviation.
/// Consider all values outside [median - k * LowerMAD, median + k * UpperMAD] as outliers.
///
/// https://eurekastatistics.com/using-the-median-absolute-deviation-to-find-outliers/
/// https://aakinshin.net/posts/harrell-davis-double-mad-outlier-detector/
#[derive(Debug)]
pub struct DoubleMadOutlierDetector {
    /// Fitted location and the two one-sided scale estimates, or `None` until
    /// the detector has been trained.
    fitted: Option<Fitted>,
    /// Configured threshold (k)
    threshold: f64,
    /// Options describing estimator (kept so the detector can be trained later)
    estimator: AnomalyMADEstimator,
}

/// What training produces: the median, and one MAD per side.
#[derive(Debug, Clone, Copy)]
struct Fitted {
    median: f64,
    lower_mad: f64,
    upper_mad: f64,
}

impl Fitted {
    fn lower_fence(&self, k: f64) -> f64 {
        self.median - k * self.lower_mad
    }

    fn upper_fence(&self, k: f64) -> f64 {
        self.median + k * self.upper_mad
    }
}

impl DoubleMadOutlierDetector {
    pub(crate) const DEFAULT_K: f64 = 3.0;

    pub fn new(threshold: f64, estimator: AnomalyMADEstimator) -> Self {
        DoubleMadOutlierDetector {
            fitted: None,
            threshold,
            estimator,
        }
    }

    /// Construct an untrained detector with the provided options.
    /// Fences will be computed when `train` is called (or on-demand in `detect`).
    pub fn with_options(options: &MADAnomalyOptions) -> Self {
        Self::new(options.k, options.estimator)
    }

    /// Returns true when the detector has been trained and fences have been computed.
    /// Callers may use this to decide whether to call `train` prior to scoring/classifying.
    pub fn is_trained(&self) -> bool {
        self.fitted.is_some()
    }

    pub fn lower_fence(&self) -> Option<f64> {
        self.fitted.map(|f| f.lower_fence(self.threshold))
    }

    pub fn upper_fence(&self) -> Option<f64> {
        self.fitted.map(|f| f.upper_fence(self.threshold))
    }

    fn fit(estimator: impl MedianAbsoluteDeviationEstimator, sample: &Samples) -> Fitted {
        Fitted {
            median: estimator.quantile_estimator().median(sample),
            lower_mad: estimator.lower_mad(sample),
            upper_mad: estimator.upper_mad(sample),
        }
    }

    /// Train the detector using explicit Samples and optional estimator.
    fn train_from_samples<E: MedianAbsoluteDeviationEstimator>(
        &mut self,
        samples: &Samples,
        k: f64,
        estimator: Option<E>,
    ) {
        self.fitted = Some(match estimator {
            Some(est) => Self::fit(est, samples),
            None => match self.estimator {
                AnomalyMADEstimator::Simple => {
                    Self::fit(SimpleNormalizedEstimator::default(), samples)
                }
                AnomalyMADEstimator::HarrellDavis => {
                    Self::fit(HarrellDavisNormalizedEstimator, samples)
                }
                AnomalyMADEstimator::Invariant => {
                    Self::fit(InvariantMADEstimator::default(), samples)
                }
            },
        });
        self.threshold = k;
    }

    /// Deviation from the median, and the distance out to the fence on the
    /// sample's *own* side.
    ///
    /// The fences are asymmetric by construction, so evidence has to be measured
    /// against the one the value is actually approaching. `r = 1` at either
    /// fence and `r = 0` at the median, whichever side the value falls on.
    ///
    /// The distance is taken as `fence - median` rather than the algebraically
    /// equal `k * mad`, so that a value sitting exactly on a reported fence
    /// produces evidence and boundary from the identical subtraction and scores
    /// exactly `0.5`.
    ///
    /// Returns `None` when the detector has not been trained, in which case
    /// there is no model to measure against.
    #[inline]
    fn deviation_and_boundary(&self, value: f64) -> Option<(f64, f64)> {
        let fitted = self.fitted?;
        let lower_fence = fitted.lower_fence(self.threshold);
        let upper_fence = fitted.upper_fence(self.threshold);
        Some(deviation_and_fence_distance(
            value,
            fitted.median,
            lower_fence,
            upper_fence,
        ))
    }

    /// Calculates a normalized anomaly score in [0, 1].
    ///
    /// - `0.0` at the median (least anomalous).
    /// - `0.5` exactly on the fence for the value's own side.
    /// - approaching `1.0` as the value moves further beyond that fence.
    pub fn get_anomaly_score(&self, value: f64) -> f64 {
        match self.deviation_and_boundary(value) {
            Some((deviation, boundary)) => normalize_evidence(deviation.abs(), boundary),
            None => 0.0,
        }
    }

    pub fn is_outlier(&self, value: f64) -> bool {
        self.classify(value).is_anomaly()
    }

    pub fn detect(&mut self, ts: &[f64]) -> TimeSeriesAnalysisResult<AnomalyResult> {
        // Ensure detector is trained: compute fences if needed
        if !self.is_trained() {
            // build Samples and train using stored options
            let samples = Samples::new_unweighted(ts.to_vec());
            self.train_from_samples(&samples, self.threshold, None::<SimpleNormalizedEstimator>);
        }

        Ok(detect_pointwise(self, ts, self.threshold))
    }
}

impl AnomalyDetector for DoubleMadOutlierDetector {
    fn method(&self) -> AnomalyMethod {
        AnomalyMethod::DoubleMAD
    }

    fn model_info(&self) -> Option<MethodInfo> {
        Some(MethodInfo::Fenced {
            lower_fence: self.lower_fence().unwrap_or(f64::NAN),
            upper_fence: self.upper_fence().unwrap_or(f64::NAN),
            center_line: self.fitted.map(|f| f.median),
        })
    }

    fn train(&mut self, ts: &[f64]) -> TimeSeriesAnalysisResult<()> {
        if ts.is_empty() {
            return Ok(());
        }
        let samples = Samples::new_unweighted(ts.to_vec());
        self.train_from_samples(&samples, self.threshold, None::<SimpleNormalizedEstimator>);
        Ok(())
    }

    fn detect(&mut self, ts: &[f64]) -> TimeSeriesAnalysisResult<AnomalyResult> {
        DoubleMadOutlierDetector::detect(self, ts)
    }
}

impl PointDetector for DoubleMadOutlierDetector {
    fn score(&self, value: f64) -> f64 {
        DoubleMadOutlierDetector::get_anomaly_score(self, value)
    }

    fn classify(&self, value: f64) -> AnomalySignal {
        // If the detector is untrained, there is no fence to be outside of.
        let Some((deviation, boundary)) = self.deviation_and_boundary(value) else {
            return AnomalySignal::None;
        };
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
