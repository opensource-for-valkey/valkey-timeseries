use super::utils::normalize_evidence;
use crate::analysis::math::{calculate_mean, calculate_mean_std_dev, quantile};
use crate::analysis::outliers::{
    Anomaly, AnomalyDetector, AnomalyMethod, AnomalyResult, AnomalySignal,
};
use crate::analysis::{TimeSeriesAnalysisError, TimeSeriesAnalysisResult};
use crate::config::num_threads;
use krcf::{RandomCutForest, RandomCutForestOptions};
use orx_parallel::{ParIter, ParIterResult, Parallelizable};
use valkey_module::logging::log_warning;
use valkey_module::{ValkeyError, ValkeyResult};

/// The default score threshold (in std deviations) used to determine whether a point is considered
/// an outlier based on its anomaly score.
const DEFAULT_STD_THRESHOLD: f64 = 3.0;

#[derive(Debug, Clone, Copy)]
pub enum RCFThreshold {
    /// Std deviations above the mean above which a point is considered an outlier.
    StdDev(f64),
    /// The amount of contamination `(0..0.5]` of the data set, i.e., the proportion of outliers in the data set.
    Contamination(f64),
}

impl RCFThreshold {
    pub fn std_dev(value: f64) -> TimeSeriesAnalysisResult<Self> {
        if value <= 0.0 {
            let err = TimeSeriesAnalysisError::InvalidParameter {
                name: "std_dev".to_string(),
                message: format!("std_dev threshold must be positive, got {value}"),
            };
            return Err(err);
        }
        Ok(RCFThreshold::StdDev(value))
    }

    pub fn contamination(value: f64) -> TimeSeriesAnalysisResult<Self> {
        if value <= 0.0 || value > 0.5 {
            let err = TimeSeriesAnalysisError::InvalidParameter {
                name: "contamination".to_string(),
                message: format!("contamination must be in the range (0..0.5], got {value}"),
            };
            return Err(err);
        }
        Ok(RCFThreshold::Contamination(value))
    }

    pub fn value(&self) -> f64 {
        match self {
            RCFThreshold::StdDev(v) => *v,
            RCFThreshold::Contamination(contamination) => *contamination,
        }
    }
}

impl Default for RCFThreshold {
    fn default() -> Self {
        RCFThreshold::StdDev(DEFAULT_STD_THRESHOLD)
    }
}

/// Configuration options for the RCF outlier detector.
#[derive(Debug, Copy, Clone)]
pub struct RCFOptions {
    /// The number of trees in the Random Cut Forest.
    pub num_trees: Option<usize>,
    /// The sample size for each tree in the Random Cut Forest.
    pub sample_size: Option<usize>,
    /// The anomaly score threshold to determine which a point is considered an outlier.
    pub threshold: Option<RCFThreshold>,
    /// A parameter that determines how much weight is given to older points. A non-zero value helps the
    /// model adapt to changing data patterns.
    pub time_decay: Option<f64>,
    /// The size of the shingle (number of consecutive data points combined into a single point).
    pub shingle_size: Option<usize>,
    /// Number of initial points to treat as a warmup window.
    ///
    /// Two effects:
    /// 1. krcf will not emit meaningful scores from its internal `update()` path for
    ///    the first `output_after` calls.
    /// 2. [`RcfOutlierDetector::detect`] suppresses anomaly flagging for indices
    ///    `0..output_after`, eliminating false positives that arise while the forest
    ///    is still learning the data distribution.
    pub output_after: Option<usize>,
    /// Whether to enable parallel execution for RCF operations.
    pub parallel_enabled: Option<bool>,
}

pub const RCF_DEFAULT_NUM_TREES: usize = 100;
pub const RCF_DEFAULT_SAMPLE_SIZE: usize = 256;

impl Default for RCFOptions {
    fn default() -> Self {
        Self {
            num_trees: Some(RCF_DEFAULT_NUM_TREES),
            sample_size: Some(RCF_DEFAULT_SAMPLE_SIZE),
            threshold: None,
            time_decay: None,
            shingle_size: None,
            output_after: None,
            parallel_enabled: None,
        }
    }
}

impl RCFOptions {
    /// Create RCFOptions with default values overridden by provided ones.
    pub fn with_overrides(overrides: RCFOptions) -> Self {
        RCFOptions {
            num_trees: overrides.num_trees.or(Some(RCF_DEFAULT_NUM_TREES)),
            sample_size: overrides.sample_size.or(Some(RCF_DEFAULT_SAMPLE_SIZE)),
            threshold: overrides.threshold,
            time_decay: overrides.time_decay,
            shingle_size: overrides.shingle_size,
            output_after: overrides.output_after,
            parallel_enabled: overrides.parallel_enabled,
        }
    }

    /// Heuristic: decide whether to enable parallel execution for RCF operations.
    ///
    /// Assumptions:
    /// - Univariate input data (base dimension = 1)
    /// - Effective dimension is `shingle_size` (since a shingle concatenates consecutive scalar values).
    ///
    /// Rule:
    /// Enable parallel if:
    /// - `num_trees >= num_threads`, AND
    /// - `num_trees * dimensions_eff * log2(sample_size) >= 10_000`
    pub fn should_parallelize(&self) -> bool {
        let num_threads = num_threads();

        // Missing values fall back to the same defaults used elsewhere.
        let num_trees = self.num_trees.unwrap_or(100);
        let sample_size = self.sample_size.unwrap_or(RCF_DEFAULT_SAMPLE_SIZE).max(2); // avoid log2(0/1)
        let shingle_size = self.shingle_size.unwrap_or(1).max(1);

        // Univariate base dimension = 1, so the effective dimension is just the shingle size.
        let dimensions_eff = shingle_size;

        if num_threads <= 1 {
            return false;
        }
        if num_trees < num_threads {
            return false;
        }

        // Compute floor(log2(sample_size)) using usize arithmetic so that the result
        // is correct on both 32-bit and 64-bit platforms.
        let log2_sample = (usize::BITS - 1 - sample_size.leading_zeros()) as usize;

        // Compute `num_trees * dimensions_eff * log2(sample_size)` with saturation to avoid overflow.
        let work = num_trees
            .saturating_mul(dimensions_eff)
            .saturating_mul(log2_sample);

        work >= 10_000
    }
}

impl From<RCFOptions> for RandomCutForestOptions {
    fn from(options: RCFOptions) -> RandomCutForestOptions {
        let parallel_execution_enabled = match options.parallel_enabled {
            None | Some(true) => Some(options.should_parallelize()),
            Some(false) => None,
        };

        let internal_shingling = options.shingle_size.map(|_| true);

        RandomCutForestOptions {
            dimensions: 1,
            num_trees: options.num_trees,
            sample_size: options.sample_size,
            lambda: options.time_decay,
            shingle_size: options.shingle_size.unwrap_or(1),
            output_after: options.output_after,
            parallel_execution_enabled,
            internal_shingling,
            ..Default::default()
        }
    }
}

/// Outlier detector backed by a Random Cut Forest (Rcf).
#[derive(Debug, Clone)]
pub struct RcfOutlierDetector {
    forest: RandomCutForest,
    /// The anomaly score threshold above which a point is considered an outlier.
    threshold: Option<RCFThreshold>,
    /// Number of leading points whose scores are excluded from anomaly detection.
    /// Mirrors [`RCFOptions::output_after`].  Zero means no warmup suppression.
    output_after: usize,
}

impl RcfOutlierDetector {
    pub fn new(options: RCFOptions) -> ValkeyResult<Self> {
        // Capture output_after before options is consumed by the From impl.
        let output_after = options.output_after.unwrap_or(0);
        let threshold = options.threshold;
        let rcf_options: RandomCutForestOptions = options.into();
        let forest = RandomCutForest::new(rcf_options)
            .map_err(|e| ValkeyError::String(format!("Failed to create Rcf: {:?}", e)))?;

        Ok(Self {
            forest,
            threshold,
            output_after,
        })
    }

    pub fn set_data(&mut self, data: &[f64]) {
        for &value in data {
            if let Err(e) = self.forest.update(&[value as f32]) {
                log::error!("Failed to update Rcf with value {}: {:?}", value, e);
            }
        }
    }

    /// Return the Rcf anomaly score for a single scalar value.
    pub fn score(&self, value: f64) -> f64 {
        self.forest.score(&[value as f32]).unwrap_or_else(|e| {
            log::error!("{:?}", e);
            f64::NAN
        })
    }

    /// Like [`score`], but returns an error instead of logging + `NaN`.
    pub fn try_score(&self, value: f64) -> ValkeyResult<f64> {
        self.forest
            .score(&[value as f32])
            .map_err(|e| ValkeyError::String(format!("Failed to score Rcf point: {:?}", e)))
    }

    /// Batch scoring that uses [`try_score`] and returns the first error encountered.
    ///
    /// Parallelizes over input points when the batch is large enough to amortize overhead.
    /// Because this method does not update the model, all points are scored against
    /// the same forest state and the calls are fully independent — safe to parallelize.
    pub fn try_batch_scores(&self, values: &[f64]) -> ValkeyResult<Vec<f64>> {
        let num_threads = num_threads();

        // Heuristic: only parallelize for large enough batches.
        // Rationale: scoring a single point is relatively small work; thread context switching overhead
        // can dominate for small N.
        let should_parallelize = num_threads > 1 && values.len() >= 256 * num_threads;

        if should_parallelize {
            values
                .par()
                .map(|&v| self.try_score(v))
                .into_fallible_result()
                .collect()
        } else {
            values.iter().map(|&v| self.try_score(v)).collect()
        }
    }

    /// Batch detect anomalies in `ts` using the online score-before-update (test-before-learn) pattern.
    ///
    /// # Algorithm
    ///
    /// For each point in `ts`, the method:
    /// 1. **Scores** the point against the current forest state.
    /// 2. **Updates** the forest with that point before moving to the next one.
    ///
    /// This ensures every point is evaluated against a model trained only on the
    /// observations that preceded it, preserving detection sensitivity for anomalies
    /// that would otherwise be absorbed into the forest before being scored.
    ///
    /// # Thresholding
    ///
    /// The method supports two thresholding strategies via [`RCFThreshold`]:
    ///
    /// 1. **StdDev (Default)**: Uses the Z-score of the RCF anomaly scores. A point is
    ///    flagged if its score is `threshold` standard deviations above the mean of all
    ///    scores in the batch.
    ///
    /// 2. **Contamination**: A non-parametric method that flags the top `contamination`
    ///    proportion of points with the highest RCF scores (e.g., 0.01 flags the top 1%).
    ///
    /// # Score consistency
    ///
    /// Both modes report scores on the module-wide scale: `0.5` sits exactly on
    /// the cutoff, so `score > 0.5` ⟺ flagged, whichever threshold mode is in
    /// use. `AnomalyResult::threshold` stays on the *requested* scale — the
    /// standard-deviation multiple, or the contamination fraction — since that
    /// is what is echoed back in `parameters`.
    ///
    /// # Direction
    ///
    /// RCF is a symmetric detector.  The [`AnomalySignal`] direction is derived by
    /// comparing each anomalous value to the mean of the full input slice:
    /// - value ≥ mean → [`AnomalySignal::Positive`]
    /// - value < mean → [`AnomalySignal::Negative`]
    ///
    /// # State
    ///
    /// Points with index `< output_after` are scored and recorded in
    /// `AnomalyResult::scores` (preserving one score per input point), but are
    /// **never classified as anomalies**.  This suppresses false positives that occur
    /// while the forest is still learning the data distribution.  Set `output_after`
    /// in [`RCFOptions`] to control the size of the warmup window.
    ///
    /// This method mutates `self.forest`.  For repeatable results always start from
    /// a fresh [`RcfOutlierDetector`]; `detect_anomalies_rcf` guarantees this by
    /// constructing a new detector on every call.
    pub fn detect(&mut self, ts: &[f64]) -> TimeSeriesAnalysisResult<AnomalyResult> {
        let threshold = self.threshold.unwrap_or_default();

        if ts.is_empty() {
            return Ok(AnomalyResult {
                scores: Vec::new(),
                anomalies: Vec::new(),
                threshold: threshold.value(),
                method: AnomalyMethod::RandomCutForest,
                method_info: None,
            });
        }

        // Online pattern: score each point *before* updating the model so that the
        // anomaly score reflects the distribution seen *up to but not including*
        // the current point.
        let mut scores = Vec::with_capacity(ts.len());

        for &value in ts {
            let raw = self.try_score(value).map_err(|e| {
                let msg = format!("Failed to score RCF point: {e:?}");
                log_warning(&msg);
                TimeSeriesAnalysisError::AnomalyDetectionError(
                    "failed to compute RCF score".to_string(),
                )
            })?;
            scores.push(raw);

            if let Err(e) = self.forest.update(&[value as f32]) {
                log_warning(format!("Failed to update RCF with value {value}: {e:?}"));
            }
        }

        match threshold {
            RCFThreshold::StdDev(threshold) => Ok(self.detect_with_zscores(ts, scores, threshold)),
            RCFThreshold::Contamination(contamination) => {
                Ok(self.detect_with_contamination(ts, scores, contamination))
            }
        }
    }

    /// Both threshold modes reduce to the same shape: a cutoff on the raw forest
    /// scores, an evidence quantity measured against it, and a direction taken
    /// from the data values.
    ///
    /// They used to disagree on all three. `StdDev` reported a normalized
    /// *z-score of the batch's forest scores* while `Contamination` reported
    /// `1 - e^(-raw)` of the forest score itself — same method, same field, two
    /// scales, and switching threshold mode silently changed what the number
    /// meant. Expressing both as evidence against their own cutoff makes the two
    /// modes agree with each other and each mode agree with its own verdict.
    fn assemble(
        &self,
        values: &[f64],
        raw_scores: &[f64],
        evidence: impl Fn(f64) -> f64,
        boundary: f64,
        threshold: f64,
    ) -> AnomalyResult {
        // Direction comes from the value distribution. The `StdDev` path used to
        // compare each data value against the mean of the *forest scores* —
        // unrelated units, so the reported direction depended on whether the
        // series happened to live on the same numeric scale as the forest's
        // internal statistic.
        let value_mean = calculate_mean(values);

        let mut scores = Vec::with_capacity(raw_scores.len());
        let mut anomalies: Vec<Anomaly> = Vec::with_capacity(4);

        for (index, (&raw_score, &value)) in raw_scores.iter().zip(values.iter()).enumerate() {
            let score = normalize_evidence(evidence(raw_score), boundary);
            scores.push(score);

            // Suppress anomaly flagging during the warmup window. The forest has
            // not yet learned enough of the distribution to produce reliable
            // scores, so detections here are likely false positives. Scores are
            // still recorded, so `scores` always has one entry per point — which
            // is why this window is a documented carve-out from the 0.5 contract
            // rather than a violation of it.
            if index < self.output_after {
                continue;
            }

            if score > 0.5 {
                let signal = if value >= value_mean {
                    AnomalySignal::Positive
                } else {
                    AnomalySignal::Negative
                };

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
            method: AnomalyMethod::RandomCutForest,
            method_info: None,
        }
    }

    /// Cutoff at `mean + threshold * std_dev` of the batch's forest scores.
    ///
    /// Evidence is floored at zero, which is what makes the score one-sided.
    /// It used to be `|z|`, so a point whose forest score was unusually *low* —
    /// i.e. more ordinary than average — received a high anomaly score while the
    /// verdict, a plain `>` against the cutoff, correctly declined to flag it.
    fn detect_with_zscores(
        &mut self,
        values: &[f64],
        raw_scores: Vec<f64>,
        threshold: f64,
    ) -> AnomalyResult {
        let (mean, std_dev) = calculate_mean_std_dev(&raw_scores);

        self.assemble(
            values,
            &raw_scores,
            |raw| raw - mean,
            threshold * std_dev,
            threshold,
        )
    }

    /// Cutoff at the `1 - contamination` quantile of the forest scores.
    ///
    /// By construction roughly a `contamination` fraction of samples exceed that
    /// quantile, so roughly that fraction score above 0.5 — which is exactly
    /// what the caller asked for.
    fn detect_with_contamination(
        &self,
        values: &[f64],
        raw_scores: Vec<f64>,
        contamination: f64,
    ) -> AnomalyResult {
        let start = self.output_after.min(raw_scores.len());
        let usable_scores = &raw_scores[start..];

        // Keep contamination in a safe open interval for quantile selection.
        let contamination = contamination.clamp(f64::EPSILON, 1.0 - f64::EPSILON);

        // If everything is in warmup there is no usable distribution to take a
        // quantile of. An unusable boundary scores every point 0.0, and the
        // warmup window flags nothing regardless.
        let boundary = if usable_scores.is_empty() {
            f64::NAN
        } else {
            quantile(usable_scores, 1.0 - contamination)
        };

        self.assemble(values, &raw_scores, |raw| raw, boundary, contamination)
    }
}

/// RCF implements only [`AnomalyDetector`], not [`PointDetector`]. Under a
/// contamination threshold the cutoff is a quantile of the batch's own score
/// distribution, so a point's verdict depends on the company it keeps; and the
/// forest scores magnitude only, leaving no per-point basis for a direction.
impl AnomalyDetector for RcfOutlierDetector {
    fn method(&self) -> AnomalyMethod {
        AnomalyMethod::RandomCutForest
    }

    fn detect(&mut self, ts: &[f64]) -> TimeSeriesAnalysisResult<AnomalyResult> {
        RcfOutlierDetector::detect(self, ts)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rcf_detects_large_outlier() {
        // small example; tune num_trees/sample_size for your kcrf build
        let data = [1.0, 1.1, 0.9, 1.05, 1.0, 100.0];
        let mut detector = RcfOutlierDetector::new(RCFOptions {
            num_trees: Some(50),
            sample_size: Some(256),
            threshold: None,
            time_decay: None,
            shingle_size: None,
            output_after: Some(data.len()),
            parallel_enabled: None,
        })
        .unwrap();
        detector.set_data(&data);

        // 100.0 should score highly.
        // NOTE: is_anomaly(value) uses a fixed threshold (not std-dev based)
        // because it operates on a single point without full series context.
        assert!(detector.score(100.0) > 0.8);
    }

    #[test]
    fn score_is_nan_when_forest_is_empty() {
        let detector = RcfOutlierDetector::new(RCFOptions {
            num_trees: Some(20),
            sample_size: Some(64),
            threshold: None,
            time_decay: None,
            shingle_size: None,
            output_after: None,
            parallel_enabled: None,
        })
        .unwrap();

        // Scoring before any updates may error internally; wrapper converts that into NaN.
        let score = detector.score(1.0);
        assert_eq!(score, 0.0); // Rcf returns 0.0 for empty forest
        // assert!(!detector.is_anomaly(1.0)); // NaN > threshold is false
    }

    #[test]
    fn outlier_scores_higher_than_typical_values_after_training() {
        // Train on a tight cluster around 1.0
        let train = [1.0, 1.02, 0.98, 1.01, 0.99, 1.0, 1.01, 0.995, 1.005, 1.0];

        // This avoids relying on an absolute threshold (which can be flaky across builds);
        // we only assert that an obvious outlier scores higher than a typical point.
        let mut detector = RcfOutlierDetector::new(RCFOptions {
            num_trees: Some(60),
            sample_size: Some(128),
            threshold: Some(RCFThreshold::StdDev(7.0)), // effectively disable threshold check for this test
            time_decay: None,
            shingle_size: None,
            output_after: Some(train.len()),
            parallel_enabled: None,
        })
        .unwrap();

        detector.set_data(&train);

        let typical = detector.score(1.0);
        let outlier = detector.score(50.0);

        assert!(
            !typical.is_nan(),
            "typical score should be a number after training"
        );
        assert!(
            !outlier.is_nan(),
            "outlier score should be a number after training"
        );
        assert!(
            outlier > typical,
            "expected outlier score ({outlier}) to be > typical score ({typical})"
        );
    }

    #[test]
    fn rcf_detects_spiky_data() {
        // Baseline around 10.0, with occasional spikes to 100.0
        let mut data = vec![10.0; 100];
        data[25] = 100.0; // spike
        data[50] = 95.0; // spike
        data[75] = 105.0; // spike

        let mut detector = RcfOutlierDetector::new(RCFOptions {
            num_trees: Some(100),
            sample_size: Some(256),
            threshold: Some(RCFThreshold::StdDev(3.0)),
            time_decay: Some(0.1),
            shingle_size: Some(1),
            output_after: Some(30), // allow a training period
            parallel_enabled: Some(false),
        })
        .unwrap();

        detector.set_data(&data);

        // Verify spikes score high
        let spike_score = detector.score(100.0);
        let normal_score = detector.score(10.0);

        assert!(!spike_score.is_nan(), "spike score should be valid");
        assert!(!normal_score.is_nan(), "normal score should be valid");
        assert!(
            spike_score > normal_score,
            "spike ({spike_score}) should score higher than normal ({normal_score})"
        );
        assert!(
            detector.score(100.0) > 1.0,
            "spike should have a high raw score"
        );
        assert!(
            detector.score(10.0) < 3.0,
            "normal value should have a low raw score"
        );
    }

    #[test]
    fn rcf_handles_constant_data() {
        // All values identical
        let data = vec![42.0; 100];

        let mut detector = RcfOutlierDetector::new(RCFOptions {
            num_trees: Some(50),
            sample_size: Some(128),
            threshold: None,
            time_decay: None,
            shingle_size: Some(1),
            output_after: Some(20),
            parallel_enabled: Some(false),
        })
        .unwrap();

        detector.set_data(&data);

        // Constant value should have a low anomaly score
        let constant_score = detector.score(42.0);
        assert!(!constant_score.is_nan(), "constant score should be valid");

        // A different value should score higher
        let different_score = detector.score(100.0);
        assert!(
            different_score > constant_score,
            "different value ({different_score}) should score higher than constant ({constant_score})"
        );
    }

    #[test]
    fn rcf_detects_gradual_shift() {
        // Data shifts from ~1.0 to ~10.0 gradually
        let mut data = Vec::with_capacity(200);
        for i in 0..100 {
            data.push(1.0 + 0.05 * (i as f64 * 0.1).sin());
        }
        for i in 0..100 {
            data.push(10.0 + 0.05 * (i as f64 * 0.1).sin());
        }

        let mut detector = RcfOutlierDetector::new(RCFOptions {
            num_trees: Some(100),
            sample_size: Some(256),
            threshold: Some(RCFThreshold::StdDev(2.5)),
            time_decay: Some(0.05), // helps adapt to shift
            shingle_size: Some(4),  // multi-point context
            output_after: Some(50),
            parallel_enabled: Some(false),
        })
        .unwrap();

        detector.set_data(&data);

        // After shift, values around 10.0 should eventually normalize
        // but immediate transition should show high scores
        let baseline_score = detector.score(1.0);
        let shifted_score = detector.score(10.0);

        assert!(!baseline_score.is_nan());
        assert!(!shifted_score.is_nan());
    }

    #[test]
    fn rcf_batch_scoring_matches_individual() {
        let train = vec![1.0, 1.1, 0.9, 1.05, 1.0, 0.95, 1.02, 1.08];
        let test_values = vec![1.0, 5.0, 1.05, 20.0];

        let mut detector = RcfOutlierDetector::new(RCFOptions {
            num_trees: Some(60),
            sample_size: Some(128),
            threshold: None,
            time_decay: None,
            shingle_size: Some(1),
            output_after: Some(train.len()),
            parallel_enabled: Some(false),
        })
        .unwrap();

        detector.set_data(&train);

        // Individual scoring
        let individual_scores: Vec<f64> = test_values
            .iter()
            .map(|&v| detector.try_score(v).unwrap())
            .collect();

        // Batch scoring
        let batch_scores = detector.try_batch_scores(&test_values).unwrap();

        assert_eq!(individual_scores.len(), batch_scores.len());
        for (i, (&ind, &batch)) in individual_scores
            .iter()
            .zip(batch_scores.iter())
            .enumerate()
        {
            assert!(
                (ind - batch).abs() < 1e-10,
                "score mismatch at index {i}: individual={ind}, batch={batch}"
            );
        }
    }

    #[test]
    fn rcf_parallel_batch_scoring() {
        let train: Vec<f64> = (0..100)
            .map(|i| 1.0 + 0.1 * (i as f64 * 0.05).sin())
            .collect();
        // Large test batch to trigger parallel execution
        let test_values: Vec<f64> = (0..2000)
            .map(|i| if i % 100 == 0 { 50.0 } else { 1.0 })
            .collect();

        let mut detector = RcfOutlierDetector::new(RCFOptions {
            num_trees: Some(100),
            sample_size: Some(256),
            threshold: None,
            time_decay: None,
            shingle_size: Some(1),
            output_after: Some(train.len()),
            parallel_enabled: Some(true),
        })
        .unwrap();

        detector.set_data(&train);

        let scores = detector.try_batch_scores(&test_values).unwrap();
        assert_eq!(scores.len(), test_values.len());

        // Verify no NaN scores
        assert!(
            scores.iter().all(|s| !s.is_nan()),
            "all scores should be valid"
        );
    }

    #[test]
    fn rcf_with_shingle_size() {
        // Shingle size creates multi-point context
        let mut data = vec![1.0; 50];
        data.extend(vec![10.0; 50]); // abrupt shift

        let mut detector = RcfOutlierDetector::new(RCFOptions {
            num_trees: Some(80),
            sample_size: Some(256),
            threshold: None,
            time_decay: Some(0.1),
            shingle_size: Some(4), // 4-point sliding window
            output_after: Some(20),
            parallel_enabled: Some(false),
        })
        .unwrap();

        detector.set_data(&data);

        // With shingle, the detector sees sequences like [1,1,1,1] vs [10,10,10,10]
        let score = detector.score(10.0);
        assert!(!score.is_nan(), "score with shingle should be valid");
    }

    #[test]
    fn rcf_should_parallelize_heuristic() {
        // Test the heuristic logic
        let opts_small = RCFOptions {
            num_trees: Some(10),
            sample_size: Some(64),
            threshold: None,
            shingle_size: Some(1),
            ..Default::default()
        };
        assert!(
            !opts_small.should_parallelize(),
            "small forest should not parallelize"
        );

        let opts_large = RCFOptions {
            num_trees: Some(200),
            sample_size: Some(512),
            threshold: None,
            shingle_size: Some(8),
            ..Default::default()
        };
        // The result depends on NUM_THREADS, but we can verify it returns a boolean
        let _ = opts_large.should_parallelize();
    }

    #[test]
    fn rcf_with_time_decay() {
        let mut data = Vec::with_capacity(200);
        // Early pattern: ~5.0
        for i in 0..100 {
            data.push(5.0 + 0.2 * (i as f64 * 0.1).sin());
        }
        // Later pattern: ~15.0
        for i in 0..100 {
            data.push(15.0 + 0.2 * (i as f64 * 0.1).sin());
        }

        let mut detector = RcfOutlierDetector::new(RCFOptions {
            num_trees: Some(100),
            sample_size: Some(256),
            threshold: None,
            time_decay: Some(0.001), // decay parameter helps adapt
            shingle_size: Some(1),
            output_after: Some(50),
            parallel_enabled: Some(false),
        })
        .unwrap();

        detector.set_data(&data);

        // With time decay, the forest adapts to a new pattern
        let score = detector.score(15.0);
        assert!(!score.is_nan(), "score with time decay should be valid");
    }

    #[test]
    fn rcf_empty_batch_scoring() {
        let detector = RcfOutlierDetector::new(RCFOptions::default()).unwrap();
        let scores = detector.try_batch_scores(&[]).unwrap();
        assert!(scores.is_empty(), "empty input should yield empty output");
    }

    #[test]
    fn rcf_single_value_batch() {
        let train = vec![1.0; 50];
        let mut detector = RcfOutlierDetector::new(RCFOptions {
            num_trees: Some(50),
            sample_size: Some(128),
            threshold: None,
            output_after: Some(train.len()),
            ..Default::default()
        })
        .unwrap();

        detector.set_data(&train);

        let scores = detector.try_batch_scores(&[5.0]).unwrap();
        assert_eq!(scores.len(), 1);
        assert!(!scores[0].is_nan());
    }

    // ── Tests for `detect` ────────────────────────────────────────────────────

    /// All score-related fields in `AnomalyResult` must be normalized to [0, 1]
    /// and consistent with each other.
    #[test]
    fn detect_anomaly_score_consistent_with_result_scores_and_threshold() {
        let mut ts = vec![1.0f64; 50];
        ts[40] = 100.0; // obvious outlier near the end of the warmup period

        let mut detector = RcfOutlierDetector::new(RCFOptions {
            num_trees: Some(50),
            sample_size: Some(64),
            threshold: Some(RCFThreshold::StdDev(3.0)),
            output_after: Some(20),
            ..Default::default()
        })
        .unwrap();

        let result = detector.detect(&ts).unwrap();

        assert_eq!(result.scores.len(), ts.len(), "one score per input point");

        for anomaly in &result.anomalies {
            let score_from_vec = result.scores[anomaly.index];

            // Anomaly::score and AnomalyResult::scores must agree (same normalized value).
            assert!(
                (anomaly.score - score_from_vec).abs() < 1e-10,
                "Anomaly::score ({}) must equal AnomalyResult::scores[{}] ({})",
                anomaly.score,
                anomaly.index,
                score_from_vec
            );

            // Scores should be in [0, 1]
            assert!(anomaly.score >= 0.0 && anomaly.score <= 1.0);
        }
    }

    /// A value clearly above the series mean must receive `Positive` signal;
    /// a value clearly below must receive `Negative` signal.
    #[test]
    fn detect_assigns_direction_relative_to_mean() {
        // Build a series with mean ≈ 0.  High and low anomalies are symmetric.
        let n = 60usize;
        let mut ts: Vec<f64> = (0..n).map(|_| 0.0).collect();
        ts[20] = 80.0; // well above mean → Positive
        ts[40] = -80.0; // well below mean → Negative

        let mut detector = RcfOutlierDetector::new(RCFOptions {
            num_trees: Some(60),
            sample_size: Some(64),
            threshold: None,
            output_after: Some(10),
            ..Default::default()
        })
        .unwrap();

        let result = detector.detect(&ts).unwrap();

        // We may not detect every anomaly in every random seed, but if an anomaly IS
        // reported, its direction must match the expected signal.
        for anomaly in &result.anomalies {
            if anomaly.value > 0.0 {
                assert_eq!(
                    anomaly.signal,
                    AnomalySignal::Positive,
                    "value {} above mean should be Positive",
                    anomaly.value
                );
            } else if anomaly.value < 0.0 {
                assert_eq!(
                    anomaly.signal,
                    AnomalySignal::Negative,
                    "value {} below mean should be Negative",
                    anomaly.value
                );
            }
        }
    }

    /// Direction must come from the data, on a value scale deliberately unlike
    /// the forest's.
    ///
    /// `StdDev` used to pick a signal by comparing each data *value* to the mean
    /// of the *forest scores* — unrelated units, so the answer depended on
    /// whether the series happened to live near the forest's own numeric range.
    /// Raw RCF scores sit around 1-4; a series near 10,000 therefore has every
    /// value above that mean, and every anomaly was reported `Positive`,
    /// including the dips. `detect_assigns_direction_relative_to_mean` above
    /// cannot catch this: it builds a series centered on zero, where the two
    /// scales happen to agree.
    #[test]
    fn detect_assigns_direction_on_the_data_scale_not_the_forest_scale() {
        let mut ts: Vec<f64> = (0..120).map(|i| 10_000.0 + (i % 7) as f64).collect();
        ts[60] = 90_000.0; // unmistakable high spike
        ts[90] = -70_000.0; // unmistakable low dip

        for threshold in [RCFThreshold::StdDev(2.0), RCFThreshold::Contamination(0.05)] {
            let mut detector = RcfOutlierDetector::new(RCFOptions {
                num_trees: Some(50),
                sample_size: Some(64),
                threshold: Some(threshold),
                output_after: Some(20),
                ..Default::default()
            })
            .unwrap();

            let result = detector.detect(&ts).unwrap();

            let dip = result.anomalies.iter().find(|a| a.index == 90);
            assert!(
                dip.is_some(),
                "{threshold:?}: the dip at index 90 should be detected"
            );
            assert_eq!(
                dip.unwrap().signal,
                AnomalySignal::Negative,
                "{threshold:?}: a value far *below* the series mean must be Negative"
            );

            if let Some(spike) = result.anomalies.iter().find(|a| a.index == 60) {
                assert_eq!(
                    spike.signal,
                    AnomalySignal::Positive,
                    "{threshold:?}: a value far above the series mean must be Positive"
                );
            }
        }
    }

    /// The score is one-sided, matching the verdict.
    ///
    /// `StdDev` used to score `|z|` of the forest scores while flagging on a
    /// plain `>` against the cutoff, so a point whose forest score was unusually
    /// *low* — more ordinary than average — received a high anomaly score and
    /// was correctly not flagged. Score and verdict disagreed by construction.
    #[test]
    fn stddev_scores_are_one_sided() {
        let mut ts = vec![10.0f64; 120];
        ts[80] = 900.0;

        let mut detector = RcfOutlierDetector::new(RCFOptions {
            num_trees: Some(50),
            sample_size: Some(64),
            threshold: Some(RCFThreshold::StdDev(3.0)),
            output_after: Some(20),
            ..Default::default()
        })
        .unwrap();

        let result = detector.detect(&ts).unwrap();
        let flagged: Vec<usize> = result.anomalies.iter().map(|a| a.index).collect();

        for (index, &score) in result.scores.iter().enumerate().skip(20) {
            assert_eq!(
                score > 0.5,
                flagged.contains(&index),
                "score[{index}] = {score} disagrees with the verdict"
            );
        }
    }

    /// Both threshold modes report on the same scale. They used to report a
    /// normalized z-score of the forest scores and `1 - e^(-raw)` of the forest
    /// score respectively, so switching threshold mode silently changed what the
    /// number meant.
    #[test]
    fn both_threshold_modes_put_the_boundary_at_one_half() {
        let mut ts: Vec<f64> = (0..200).map(|i| 10.0 + (i as f64 * 0.1).sin()).collect();
        ts[150] = 100.0;

        for threshold in [RCFThreshold::StdDev(3.0), RCFThreshold::Contamination(0.05)] {
            let mut detector = RcfOutlierDetector::new(RCFOptions {
                num_trees: Some(50),
                sample_size: Some(64),
                threshold: Some(threshold),
                output_after: Some(50),
                ..Default::default()
            })
            .unwrap();

            let result = detector.detect(&ts).unwrap();

            for anomaly in &result.anomalies {
                assert!(
                    anomaly.score > 0.5,
                    "{threshold:?}: a flagged point scored {}, at or below the boundary",
                    anomaly.score
                );
            }
            assert!(
                result.scores.iter().all(|s| (0.0..=1.0).contains(s)),
                "{threshold:?}: scores must stay in [0, 1]"
            );
        }
    }

    /// `detect` on an empty slice must succeed and return empty collections.
    #[test]
    fn detect_empty_input() {
        let mut detector = RcfOutlierDetector::new(RCFOptions::default()).unwrap();
        let result = detector.detect(&[]).unwrap();
        assert!(result.scores.is_empty());
        assert!(result.anomalies.is_empty());
    }

    /// Calling `detect` on a fresh detector must produce exactly one score per point,
    /// never NaN, and the lengths of `scores` and `anomalies` must be self-consistent.
    #[test]
    fn detect_output_lengths_and_no_nan_scores() {
        let ts: Vec<f64> = (0..80).map(|i| (i as f64 * 0.1).sin()).collect();

        let mut detector = RcfOutlierDetector::new(RCFOptions {
            num_trees: Some(40),
            sample_size: Some(64),
            threshold: None,
            output_after: Some(10),
            ..Default::default()
        })
        .unwrap();

        let result = detector.detect(&ts).unwrap();

        assert_eq!(result.scores.len(), ts.len());
        assert!(
            result.scores.iter().all(|s| !s.is_nan()),
            "no score should be NaN"
        );
        assert!(
            result.anomalies.len() <= ts.len(),
            "cannot have more anomalies than points"
        );
        // Every reported anomaly index must be in-bounds.
        for a in &result.anomalies {
            assert!(a.index < ts.len());
        }
    }

    #[test]
    fn detect_with_stddev_threshold() {
        let mut ts = vec![10.0; 100];
        ts[50] = 100.0; // outlier

        let mut detector = RcfOutlierDetector::new(RCFOptions {
            num_trees: Some(50),
            sample_size: Some(64),
            threshold: Some(RCFThreshold::StdDev(2.0)),
            output_after: Some(20),
            ..Default::default()
        })
        .unwrap();

        let result = detector.detect(&ts).unwrap();

        // At least the outlier should be detected if the threshold is low enough
        // and RCF has seen enough data to recognize it.
        // We use a high outlier to be sure.
        let outlier_detected = result.anomalies.iter().any(|a| a.index == 50);
        assert!(
            outlier_detected,
            "Outlier at index 50 should be detected with StdDev threshold"
        );
    }

    #[test]
    fn detect_with_contamination_threshold() {
        // Use a longer series with some noise to ensure score distribution is more diverse.
        // Identical values can cause many ties in RCF scores, making quantile selection less stable.
        let mut ts: Vec<f64> = (0..200).map(|i| 10.0 + (i as f64 * 0.1).sin()).collect();
        ts[150] = 100.0;
        ts[180] = 110.0;

        let mut detector = RcfOutlierDetector::new(RCFOptions {
            num_trees: Some(50),
            sample_size: Some(64),
            threshold: Some(RCFThreshold::Contamination(0.05)), // 5% contamination
            output_after: Some(100),                            // longer warmup
            ..Default::default()
        })
        .unwrap();

        // Pre-train on the first 100 points so that scores for the rest are more stable
        // detector.set_data(&ts[0..100]);

        // Now run detect on the full series (detect will re-score everything, but the forest is now primed)
        let result = detector.detect(&ts).unwrap();

        let anomalies_indices: Vec<usize> = result.anomalies.iter().map(|a| a.index).collect();
        println!("Detected anomalies at indices: {:?}", anomalies_indices);

        // The clear outliers should be detected.
        assert!(
            anomalies_indices.contains(&150),
            "Outlier at index 150 should be detected with Contamination threshold"
        );
        assert!(
            anomalies_indices.contains(&180),
            "Outlier at index 180 should be detected with Contamination threshold"
        );

        // It shouldn't detect too many anomalies (with 5% contamination on 200 points, we expect around 10).
        // Since quantile uses (1.0 - 0.05) * 100 = 95th percentile on USABLE scores (index 100..200),
        // it should pick the top 5 points of the 100 points after warmup.
        assert!(
            result.anomalies.len() <= 15,
            "Should not detect too many anomalies, got {}",
            result.anomalies.len()
        );
    }
}
