mod anomalies;
mod cusum_outlier_detector;
mod detector;
mod double_mad_outlier_detector;
#[cfg(test)]
mod double_mad_outlier_detector_tests;
mod esd_outlier_detector;
mod ewma_outlier_detector;
mod iqr_outlier_detector;
pub mod mad_estimator;
mod mad_outlier_detector;
#[cfg(test)]
mod mad_outlier_detector_tests;
mod modified_zscore_outlier_detector;
#[cfg(test)]
mod outlier_test_data;
mod rcf_outlier_detector;
#[cfg(test)]
mod score_contract_tests;
mod smoothed_zscores;
mod utils;
mod zscore_outlier_detector;

pub use anomalies::*;
pub use detector::Detector;
pub use esd_outlier_detector::*;
pub use ewma_outlier_detector::*;
pub use rcf_outlier_detector::*;
pub use smoothed_zscores::*;

use crate::analysis::TimeSeriesAnalysisResult;
use std::fmt::Display;

use std::str::FromStr;
use valkey_module::{ValkeyError, ValkeyResult, ValkeyString, ValkeyValue};

/// Method for outlier detection
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AnomalyMethod {
    /// Ewma-based statistical process control
    Ewma,
    /// CUSUM-based statistical process control
    Cusum,
    /// Z-score based detection
    ZScore,
    /// Modified Z-score using median absolute deviation
    ModifiedZScore,
    /// The Smoothed Z-Score algorithm
    SmoothedZScore,
    /// Mean Absolute Deviation (Mad) method
    Mad,
    /// Double median absolute deviation (Mad) method
    DoubleMAD,
    /// Interquartile range (IQR) method
    InterquartileRange,
    /// Random Cut Forest (Rcf) method
    RandomCutForest,
    /// ESD (Extreme Studentized Deviate) method
    Esd,
}

impl AnomalyMethod {
    /// Returns a human-readable name for the method
    pub fn name(&self) -> &'static str {
        match self {
            AnomalyMethod::Ewma => "EWMA",
            AnomalyMethod::Cusum => "CUSUM",
            AnomalyMethod::ZScore => "Z-Score",
            AnomalyMethod::ModifiedZScore => "Modified Z-Score",
            AnomalyMethod::SmoothedZScore => "Smoothed Z-Score",
            AnomalyMethod::Mad => "Median Absolute Deviation (MAD)",
            AnomalyMethod::DoubleMAD => "Double MAD",
            AnomalyMethod::InterquartileRange => "Interquartile Range (IQR)",
            AnomalyMethod::RandomCutForest => "Random Cut Forest",
            AnomalyMethod::Esd => "Extreme Studentized Deviate (ESD)",
        }
    }

    pub fn short_name(&self) -> &'static str {
        match self {
            AnomalyMethod::Ewma => "ewma",
            AnomalyMethod::Cusum => "cusum",
            AnomalyMethod::ZScore => "zscore",
            AnomalyMethod::ModifiedZScore => "modified-zscore",
            AnomalyMethod::SmoothedZScore => "smoothed-zscore",
            AnomalyMethod::Mad => "mad",
            AnomalyMethod::DoubleMAD => "double-mad",
            AnomalyMethod::InterquartileRange => "iqr",
            AnomalyMethod::RandomCutForest => "rcf",
            AnomalyMethod::Esd => "esd",
        }
    }
}

impl FromStr for AnomalyMethod {
    type Err = ValkeyError;

    fn from_str(s: &str) -> ValkeyResult<Self> {
        let res = hashify::tiny_map_ignore_case! {
            s.as_bytes(),
            "esd" => Ok(AnomalyMethod::Esd),
            "ewma" => Ok(AnomalyMethod::Ewma),
            "cusum" => Ok(AnomalyMethod::Cusum),
            "zscore" => Ok(AnomalyMethod::ZScore),
            "modified-zscore" => Ok(AnomalyMethod::ModifiedZScore),
            "smoothed-zscore" => Ok(AnomalyMethod::SmoothedZScore),
            "mad" => Ok(AnomalyMethod::Mad),
            "double-mad" => Ok(AnomalyMethod::DoubleMAD),
            "iqr" => Ok(AnomalyMethod::InterquartileRange),
            "rcf" => Ok(AnomalyMethod::RandomCutForest),
        };
        res.unwrap_or(Err(ValkeyError::Str(
            "TSDB: unknown anomaly detection method",
        )))
    }
}

impl TryFrom<&ValkeyString> for AnomalyMethod {
    type Error = ValkeyError;

    fn try_from(s: &ValkeyString) -> ValkeyResult<Self> {
        let str = s.to_string_lossy();
        Self::from_str(&str)
    }
}

/// Direction of anomalies to detect
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AnomalyDirection {
    /// Detect anomalies in both directions (high and low)
    Both,
    /// Detect only high anomalies
    Positive,
    /// Detect only low anomalies
    Negative,
}

impl AnomalyDirection {
    /// Returns a human-readable name for the direction
    pub fn name(&self) -> &'static str {
        match self {
            AnomalyDirection::Both => "both",
            AnomalyDirection::Positive => "positive",
            AnomalyDirection::Negative => "negative",
        }
    }
}

impl FromStr for AnomalyDirection {
    type Err = ValkeyError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let res = hashify::tiny_map_ignore_case! {
            s.as_bytes(),
            "+" => AnomalyDirection::Positive,
            "-" => AnomalyDirection::Negative,
            "both" => AnomalyDirection::Both,
            "positive" => AnomalyDirection::Positive,
            "negative" => AnomalyDirection::Negative
        };
        match res {
            Some(direction) => Ok(direction),
            None => {
                let msg = format!("TSDB: unknown analysis direction: {s}");
                Err(ValkeyError::String(msg))
            }
        }
    }
}

impl TryFrom<&ValkeyString> for AnomalyDirection {
    type Error = ValkeyError;

    fn try_from(s: &ValkeyString) -> ValkeyResult<Self> {
        let str = s.to_string_lossy();
        Self::from_str(&str)
    }
}

impl Display for AnomalyDirection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.name())
    }
}

/// Result of analysis direction detection
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AnomalySignal {
    /// No anomalies detected
    None = 0,
    /// Indicates that a particular value is a positive peak.
    Positive = 1,
    /// Indicates that a particular value is a negative peak.
    Negative = -1,
}

impl AnomalySignal {
    pub fn is_anomaly(&self) -> bool {
        *self != AnomalySignal::None
    }

    pub fn matches_direction(&self, direction: AnomalyDirection) -> bool {
        match direction {
            AnomalyDirection::Both => self.is_anomaly(),
            AnomalyDirection::Positive => *self == AnomalySignal::Positive,
            AnomalyDirection::Negative => *self == AnomalySignal::Negative,
        }
    }

    pub fn is_positive(&self) -> bool {
        *self == AnomalySignal::Positive
    }

    pub fn is_negative(&self) -> bool {
        *self == AnomalySignal::Negative
    }
}

impl From<AnomalySignal> for ValkeyValue {
    fn from(signal: AnomalySignal) -> Self {
        ValkeyValue::from(signal as i64)
    }
}

/// Method-specific information
#[derive(Debug, Clone, Copy)]
pub enum MethodInfo {
    /// For methods like ZScore, MAD and IQR
    Fenced {
        lower_fence: f64,
        upper_fence: f64,
        center_line: Option<f64>,
    },
    /// Spc-specific information
    Spc {
        /// Control limits (lower, upper)
        control_limits: (f64, f64),
        /// Center line value
        center_line: f64,
    },
}

#[derive(Debug, Copy, Clone)]
pub struct Anomaly {
    pub signal: AnomalySignal,
    pub value: f64,
    pub score: f64,
    pub index: usize,
}

impl Anomaly {
    pub fn is_anomaly(&self) -> bool {
        self.signal.is_anomaly()
    }

    pub fn is_negative(&self) -> bool {
        self.signal.is_negative()
    }

    pub fn is_positive(&self) -> bool {
        self.signal.is_positive()
    }
}

/// Result of anomaly detection
#[derive(Debug, Clone)]
pub struct AnomalyResult {
    /// Anomaly score for each point, in `[0, 1]`, higher being more anomalous.
    ///
    /// On the score scale the detection boundary is always `0.5` — see the
    /// contract on [`AnomalyDetector::detect`]. Scores are therefore comparable
    /// across methods and across threshold settings, which raw statistics are
    /// not.
    ///
    /// When `SEASONALITY` is requested, detection runs on the seasonally
    /// adjusted residuals and only [`Anomaly::value`] is restored to the
    /// original observation. These scores stay in the adjusted domain: they
    /// describe the residual, which is what was actually tested. Two scores are
    /// comparable only when computed over the same domain.
    pub scores: Vec<f64>,
    /// Detected anomalies
    pub anomalies: Vec<Anomaly>,
    /// Threshold used for binary classification, on the *method's own* scale —
    /// z-score multiples for `zscore`, `k` for `mad`, the decision interval `h`
    /// for `cusum`, a contamination fraction for `rcf`, and so on. It is echoed
    /// to clients in `parameters` to document what was requested.
    ///
    /// This is deliberately *not* rescaled onto the score scale. On the score
    /// scale the boundary is always `0.5`, which is what makes this field
    /// unnecessary as a comparison anchor.
    pub threshold: f64,
    /// Method used for detection
    pub method: AnomalyMethod,
    /// Additional information specific to the method
    pub method_info: Option<MethodInfo>,
}

impl AnomalyResult {
    /// Count the number of detected anomalies
    pub fn count_anomalies(&self) -> usize {
        self.anomalies.len()
    }

    /// Get outlier percentage.
    pub fn outlier_percentage(&self) -> f64 {
        let count = self.count_anomalies();
        if count == 0 {
            0.0
        } else {
            100.0 * count as f64 / self.scores.len() as f64
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AnomalyMADEstimator {
    Simple,
    HarrellDavis,
    Invariant,
}

impl AnomalyMADEstimator {
    pub fn alias(&self) -> &'static str {
        match self {
            AnomalyMADEstimator::Simple => "Simple",
            AnomalyMADEstimator::HarrellDavis => "Harrell-Davis",
            AnomalyMADEstimator::Invariant => "Invariant",
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            AnomalyMADEstimator::Simple => "simple",
            AnomalyMADEstimator::HarrellDavis => "harrelldavis",
            AnomalyMADEstimator::Invariant => "invariant",
        }
    }
}

impl FromStr for AnomalyMADEstimator {
    type Err = ValkeyError;

    fn from_str(s: &str) -> ValkeyResult<Self> {
        let res = hashify::tiny_map_ignore_case! {
            s.as_bytes(),
            "simple" => AnomalyMADEstimator::Simple,
            "harrell-davis" => AnomalyMADEstimator::HarrellDavis,
            "harrelldavis" => AnomalyMADEstimator::HarrellDavis,
            "hd" => AnomalyMADEstimator::HarrellDavis,
            "invariant" => AnomalyMADEstimator::Invariant,
        };
        match res {
            Some(estimator) => Ok(estimator),
            None => {
                let msg = format!("Unknown quantile estimator: {s}");
                Err(ValkeyError::String(msg))
            }
        }
    }
}

/// Options for MAD-based anomaly detection
#[derive(Debug, Clone, Copy)]
pub struct MADAnomalyOptions {
    /// Multiplier for MAD to set thresholds
    pub k: f64,
    /// Estimator to use for MAD calculation
    pub estimator: AnomalyMADEstimator,
}

impl Default for MADAnomalyOptions {
    fn default() -> Self {
        Self {
            k: 3.0,
            estimator: AnomalyMADEstimator::Invariant,
        }
    }
}

/// The contract every detector satisfies: consume a batch of observations and
/// produce a per-point score plus the subset judged anomalous.
///
/// Batch detection is deliberately the *only* universal operation. Whether a
/// point is anomalous is not always a question that can be asked of that point
/// alone — ESD's verdict is defined relative to the whole sample, and CUSUM's
/// against accumulated drift. Detectors that *can* answer narrower questions
/// opt in via [`PointDetector`], so no detector is ever forced to invent an
/// answer its method cannot express.
pub trait AnomalyDetector {
    /// The detector family/method.
    fn method(&self) -> AnomalyMethod;

    /// Fit model parameters to `data`.
    ///
    /// Implementations must be idempotent — the dispatch path calls this before
    /// every `detect`. Detectors that fit at construction, or that have nothing
    /// to fit, keep the default no-op.
    fn train(&mut self, _data: &[f64]) -> TimeSeriesAnalysisResult<()> {
        Ok(())
    }

    /// Fitted fences or control limits, when the method has them to report.
    /// Reported to clients as `method_info`.
    fn model_info(&self) -> Option<MethodInfo> {
        None
    }

    /// Score and classify a batch.
    ///
    /// Implementations must return exactly one score per element of `ts`, each
    /// in `[0, 1]`, and every `Anomaly::index` must index into `ts`.
    ///
    /// # The 0.5 contract
    ///
    /// A sample scores exactly `0.5` at the method's detection boundary, so
    /// `score > 0.5` ⟺ flagged and `score <= 0.5` ⟺ not flagged. The boundary
    /// belongs to the not-flagged side, matching the strict comparisons every
    /// classifier here uses. Scores are strictly monotone in evidence; `0.0` and
    /// `1.0` are asymptotic, reached only by degenerate or non-finite input.
    ///
    /// Implementations get this by reducing their verdict to an
    /// evidence/boundary pair and handing it to
    /// [`normalize_evidence`](utils::normalize_evidence) — never by normalizing
    /// raw evidence, which would anchor the boundary wherever the threshold
    /// happened to put it and make scores incomparable across methods.
    ///
    /// Two windows are exempt, because they are scored without ever being
    /// classified:
    ///
    /// - RCF's warmup window (`0..output_after`),
    /// - the smoothed z-score's first `lag` samples.
    ///
    /// `±∞` observations are maximally anomalous: they score `1.0` and are
    /// flagged. `NaN` observations score `0.0` and are never flagged — a missing
    /// reading is not evidence of an anomaly.
    fn detect(&mut self, ts: &[f64]) -> TimeSeriesAnalysisResult<AnomalyResult>;
}

/// Detectors whose verdict for a point is a pure function of the fitted model —
/// independent of where the point sits in the series, and of what else the
/// series contains.
///
/// This covers the fence-based methods (z-score, modified z-score, MAD, double
/// MAD, IQR). Sequential and whole-sample methods must not implement it: for
/// them a per-point answer either doesn't exist or silently disagrees with
/// [`AnomalyDetector::detect`].
pub trait PointDetector: AnomalyDetector {
    /// Normalized anomaly score in `[0, 1]`. Higher is more anomalous.
    fn score(&self, value: f64) -> f64;

    /// Which side of the fences `value` falls on, if any.
    fn classify(&self, value: f64) -> AnomalySignal;
}

/// Assemble an [`AnomalyResult`] by scoring each observation independently.
///
/// This is the shared batch loop for every [`PointDetector`]; implementations
/// delegate to it rather than repeating the accumulate-and-assemble dance.
///
/// NaN observations are scored `0.0` and never flagged — a missing reading is
/// not evidence of an anomaly. (Substituting a placeholder, as some detectors
/// used to, lets the placeholder itself trip the fences and reports the
/// original NaN as the offending value.)
pub fn detect_pointwise<D>(detector: &D, ts: &[f64], threshold: f64) -> AnomalyResult
where
    D: PointDetector + ?Sized,
{
    let mut scores = Vec::with_capacity(ts.len());
    let mut anomalies: Vec<Anomaly> = Vec::with_capacity(4);

    for (index, &value) in ts.iter().enumerate() {
        if value.is_nan() {
            scores.push(0.0);
            continue;
        }

        let score = detector.score(value);
        scores.push(score);

        let signal = detector.classify(value);
        if signal.is_anomaly() {
            anomalies.push(Anomaly {
                index,
                signal,
                value,
                score,
            });
        }
    }

    AnomalyResult {
        scores,
        anomalies,
        threshold,
        method: detector.method(),
        method_info: detector.model_info(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Flags anything outside [-1, 1]; scores by distance past the fence.
    struct FenceDetector;

    impl AnomalyDetector for FenceDetector {
        fn method(&self) -> AnomalyMethod {
            AnomalyMethod::ZScore
        }

        fn model_info(&self) -> Option<MethodInfo> {
            Some(MethodInfo::Fenced {
                lower_fence: -1.0,
                upper_fence: 1.0,
                center_line: None,
            })
        }

        fn detect(&mut self, ts: &[f64]) -> TimeSeriesAnalysisResult<AnomalyResult> {
            Ok(detect_pointwise(self, ts, 1.0))
        }
    }

    impl PointDetector for FenceDetector {
        fn score(&self, value: f64) -> f64 {
            utils::normalize_evidence((value.abs() - 1.0).max(0.0), 1.0)
        }

        fn classify(&self, value: f64) -> AnomalySignal {
            utils::get_anomaly_direction(-1.0, 1.0, value)
        }
    }

    #[test]
    fn pointwise_emits_one_score_per_observation() {
        let ts = [0.0, 5.0, -0.5, f64::NAN, -9.0];
        let result = detect_pointwise(&FenceDetector, &ts, 1.0);

        assert_eq!(result.scores.len(), ts.len());
        assert!(result.scores.iter().all(|s| (0.0..=1.0).contains(s)));
    }

    #[test]
    fn pointwise_reports_index_value_and_direction() {
        let ts = [0.0, 5.0, -0.5, -9.0];
        let result = detect_pointwise(&FenceDetector, &ts, 1.0);

        let found: Vec<(usize, f64, AnomalySignal)> = result
            .anomalies
            .iter()
            .map(|a| (a.index, a.value, a.signal))
            .collect();

        assert_eq!(
            found,
            vec![
                (1, 5.0, AnomalySignal::Positive),
                (3, -9.0, AnomalySignal::Negative),
            ]
        );
        // Each anomaly's score must match the score recorded at its index.
        for anomaly in &result.anomalies {
            assert_eq!(anomaly.score, result.scores[anomaly.index]);
        }
    }

    /// A missing reading is not evidence of an anomaly. Detectors used to
    /// substitute 0.0 for NaN, which let the substitute trip the fences and
    /// reported the original NaN back as the offending value.
    #[test]
    fn pointwise_never_flags_nan() {
        // Fences that exclude the old 0.0 placeholder, so a substitution would show up.
        struct OffsetFences;

        impl AnomalyDetector for OffsetFences {
            fn method(&self) -> AnomalyMethod {
                AnomalyMethod::InterquartileRange
            }
            fn detect(&mut self, ts: &[f64]) -> TimeSeriesAnalysisResult<AnomalyResult> {
                Ok(detect_pointwise(self, ts, 1.0))
            }
        }

        impl PointDetector for OffsetFences {
            fn score(&self, _value: f64) -> f64 {
                1.0
            }
            fn classify(&self, value: f64) -> AnomalySignal {
                utils::get_anomaly_direction(10.0, 20.0, value)
            }
        }

        let result = detect_pointwise(&OffsetFences, &[15.0, f64::NAN, 15.0], 1.0);

        assert!(
            result.anomalies.is_empty(),
            "NaN must not be flagged, got {:?}",
            result.anomalies
        );
        assert_eq!(result.scores, vec![1.0, 0.0, 1.0]);
    }

    #[test]
    fn pointwise_carries_method_and_model_info() {
        let result = detect_pointwise(&FenceDetector, &[0.0], 1.0);

        assert_eq!(result.method, AnomalyMethod::ZScore);
        assert!(matches!(
            result.method_info,
            Some(MethodInfo::Fenced {
                lower_fence: -1.0,
                upper_fence: 1.0,
                center_line: None,
            })
        ));
    }
}
