use crate::analysis::math::calculate_median_sorted;
use crate::analysis::outliers::{
    Anomaly, AnomalyDetector, AnomalyMethod, AnomalyResult, AnomalySignal,
};
use crate::analysis::{TimeSeriesAnalysisError, TimeSeriesAnalysisResult};
use statrs::distribution::{ContinuousCDF, StudentsT};

/// Which location/scale pair the studentized statistic is built from.
///
/// The distinction is the one the literature draws between Rosner's generalized
/// ESD and the "hybrid" variant. Rosner's procedure studentizes against the
/// mean and sample standard deviation; both are themselves pulled by the
/// outliers under test, which is the masking effect ESD's step-down loop is
/// trying to overcome. The hybrid form substitutes the median and a
/// normal-consistent MAD, so the reference point barely moves as outliers are
/// removed.
///
/// This was previously a `hybrid: bool` whose `true` arm selected *mean/std* —
/// the opposite of what the name claims in Rosner and in Twitter's S-H-ESD.
/// Naming the two variants removes the polarity that hid the inversion.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub enum EsdEstimator {
    /// Median and normal-consistent MAD. Robust, and the default.
    #[default]
    Hybrid,
    /// Mean and sample standard deviation, as in Rosner (1983).
    Classic,
}

impl EsdEstimator {
    pub fn is_hybrid(&self) -> bool {
        *self == EsdEstimator::Hybrid
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            EsdEstimator::Hybrid => "hybrid",
            EsdEstimator::Classic => "classic",
        }
    }
}

/// ESD (Extreme Studentized Deviate) anomaly detector
/// https://www.itl.nist.gov/div898/handbook/eda/section3/eda35h3.htm
#[derive(Clone, Debug)]
pub struct ESDOutlierOptions {
    /// Significance level for the statistical test (e.g., 0.05)
    pub alpha: f64,
    /// Location/scale pair backing the test statistic.
    pub estimator: EsdEstimator,
    /// Maximum number of outliers to detect. Must be < n/2.
    pub max_outliers: Option<usize>,
}

impl Default for ESDOutlierOptions {
    fn default() -> Self {
        ESDOutlierOptions {
            alpha: 0.05,
            estimator: EsdEstimator::Hybrid,
            max_outliers: None,
        }
    }
}

/// Outlier detector based on the Extreme Studentized Deviate (ESD) test. Used to detect one or more outliers in a
/// univariate data set that follows an approximately normal distribution
///
/// Rosner, Bernard (May 1983), Percentage Points for a Generalized ESD Many-Outlier Procedure, Technometrics, 25(2), pp. 165-172.
/// https://www.itl.nist.gov/div898/handbook/eda/section3/eda35h3.htm
pub struct ESDOutlierDetector {
    /// Significance level for a hypothesis test. Lower alpha means more conservative (fewer outliers).
    alpha: f64,
    /// Location/scale pair backing the test statistic.
    estimator: EsdEstimator,
    /// Maximum number of outliers to detect. Must be < n/2. If None, uses len(data)/2.
    max_outliers: Option<usize>,
}

impl Default for ESDOutlierDetector {
    fn default() -> Self {
        ESDOutlierDetector {
            alpha: 0.05,
            estimator: EsdEstimator::Hybrid,
            max_outliers: None,
        }
    }
}

impl ESDOutlierDetector {
    pub fn new(alpha: f64, estimator: EsdEstimator, max_outliers: Option<usize>) -> Self {
        ESDOutlierDetector {
            alpha,
            estimator,
            max_outliers,
        }
    }
}

/// ESD deliberately implements only [`AnomalyDetector`], not [`PointDetector`]:
/// its verdict comes from iteratively removing the most extreme observation and
/// re-testing, so "is this point an outlier?" has no answer independent of the
/// rest of the sample.
impl AnomalyDetector for ESDOutlierDetector {
    fn method(&self) -> AnomalyMethod {
        AnomalyMethod::Esd
    }

    fn detect(&mut self, ts: &[f64]) -> TimeSeriesAnalysisResult<AnomalyResult> {
        detect_anomalies_esd(ts, self.alpha, self.estimator, self.max_outliers)
    }
}

fn detect_anomalies_esd(
    data: &[f64],
    alpha: f64,
    estimator: EsdEstimator,
    max_outliers: Option<usize>,
) -> TimeSeriesAnalysisResult<AnomalyResult> {
    let n = data.len();

    // Parameter check: max_outliers
    let max_outliers = match max_outliers {
        Some(m) if m >= n / 2 => {
            let message = format!(
                "max_outliers must be less than n/2. Got max_outliers = {} and n = {}",
                m, n
            );
            let err = TimeSeriesAnalysisError::InvalidParameter {
                name: "max_outliers".to_string(),
                message,
            };
            return Err(err);
        }
        Some(m) => m,
        None => n / 2,
    };

    // Perform ESD test
    let (anomalies, scores) = esd_test(data, alpha, estimator, max_outliers)?;

    Ok(AnomalyResult {
        anomalies,
        scores,
        method_info: None,
        threshold: max_outliers as f64, // ESD doesn't have a fixed threshold, so we can return the max outlier count as info
        method: AnomalyMethod::Esd,
    })
}

/// Map a test statistic and the ESD critical value (lambda) to [0.0, 1.0].
/// Uses score = stat / (stat + lambda). Handles edge cases.
fn esd_score_stat_over_stat_plus_lambda(stat: f64, lambda: f64) -> f64 {
    if !stat.is_finite() || stat <= 0.0 {
        return 0.0;
    }
    if !lambda.is_finite() || lambda <= 0.0 {
        // Fallback denom to 1.0 to avoid division by zero and still provide a monotonic mapping
        return (stat / (stat + 1.0)).clamp(0.0, 1.0);
    }
    (stat / (stat + lambda)).clamp(0.0, 1.0)
}

/// Perform the Extreme Studentized Deviate (ESD) test to detect potential outliers in the data.
///
/// - `data`: slice of f64 values (original data)
/// - `alpha`: significance level (default 0.05)
/// - `estimator`: location/scale pair backing the statistic — see [`EsdEstimator`]
/// - `max_outliers`: optional maximum number of outliers to search for. If None, uses len(data)/2.
///
/// Returns a `TimeSeriesAnalysisResult<(Vec<Anomaly>, Vec<f64>)>` pair where `anomalies` contains
/// detailed `Anomaly` structs (signal direction, original value/index, normalized score) and `scores`
/// provides a [0.0, 1.0] normalized anomaly score for every observation.
fn esd_test(
    data: &[f64],
    alpha: f64,
    estimator: EsdEstimator,
    max_out: usize,
) -> TimeSeriesAnalysisResult<(Vec<Anomaly>, Vec<f64>)> {
    let n_total = data.len();

    // masked representation: Some(value) for active, None for masked
    let mut masked: Vec<Option<f64>> = data.iter().cloned().map(Some).collect();
    let mut anomalies: Vec<Anomaly> = Vec::new();

    // per-observation scores (original ordering)
    let mut scores: Vec<f64> = vec![0.0; n_total];

    // track the last computed lambda to score remaining points consistently
    let mut last_lambda: Option<f64> = None;

    for _ in 0..max_out {
        // count non-masked
        let n_non_masked = masked.iter().filter(|v| v.is_some()).count();
        if n_non_masked < 3 {
            // need at least 3 to compute t with df = n - 2 > 0
            break;
        }

        let current_vals = active_points(&masked);
        let Some((loc, _scale)) = loc_and_scale(&current_vals, estimator) else {
            break;
        };

        let (test_stat, test_idx) = calc_test_statistic(&current_vals, estimator);

        // compute critical value using the current non-masked count
        let n = n_non_masked as f64;
        let df = n - 2.0;
        if df <= 0.0 {
            break;
        }

        let prob = 1.0 - alpha / (2.0 * n);
        let student_t = StudentsT::new(0.0, 1.0, df).map_err(|e| {
            TimeSeriesAnalysisError::InvalidParameter {
                name: "df".to_string(),
                message: format!("Failed to create t-distribution: {}", e),
            }
        })?;
        let t_value = student_t.inverse_cdf(prob);

        let n_sq = n * n;
        let t_sq = t_value * t_value;
        let critical_value = ((n - 1.0) * t_value) / (n_sq - 2.0 * n + n * t_sq).sqrt();

        // remember latest lambda
        last_lambda = Some(critical_value);

        // compute normalized score for this candidate
        let score = esd_score_stat_over_stat_plus_lambda(test_stat, critical_value);
        if test_idx < scores.len() {
            scores[test_idx] = score;
        }

        let value = data[test_idx];
        let signal = if value > loc {
            AnomalySignal::Positive
        } else {
            AnomalySignal::Negative
        };

        anomalies.push(Anomaly {
            signal,
            value,
            score,
            index: test_idx,
        });

        // mask that index (in original indexing)
        masked[test_idx] = None;
    }

    // Standard ESD: find the largest k such that R_i > lambda_i for all i <= k
    let mut k = 0;
    for (i, anomaly) in anomalies.iter().enumerate() {
        if anomaly.score > 0.5 {
            // score > 0.5 means test_stat > critical_value
            k = i + 1;
        }
    }
    anomalies.truncate(k);

    // After loop, score any remaining unmasked observations using the last lambda if available.
    score_active_points(
        &active_points(&masked),
        estimator,
        last_lambda.unwrap_or(0.0),
        &mut scores,
    );

    Ok((anomalies, scores))
}

/// Calculate the test statistic and index for the (masked) data.
/// Returns (test_statistic, index_in_original_array)
fn calc_test_statistic(values: &[(usize, f64)], estimator: EsdEstimator) -> (f64, usize) {
    // If empty, return 0,0
    if values.is_empty() {
        return (0.0, 0);
    }

    let Some((loc, scale)) = loc_and_scale(values, estimator) else {
        return (0.0f64, 0);
    };

    // Prevent division by zero; if scale is zero, return 0 statistic at index of maximum deviation
    // find index with maximum absolute deviation
    let (max_idx, max_dev) = values
        .iter()
        .map(|(i, x)| (*i, (x - loc).abs()))
        .max_by(|a, b| a.1.partial_cmp(&b.1).unwrap())
        .unwrap();

    if scale == 0.0 || scale.is_nan() {
        return (0.0f64, max_idx);
    }

    let test_stat = max_dev / scale;
    (test_stat, max_idx)
}

/// Collect the active observations from the masked representation.
fn active_points(masked: &[Option<f64>]) -> Vec<(usize, f64)> {
    masked
        .iter()
        .enumerate()
        .filter_map(|(i, v)| v.map(|x| (i, x)))
        .collect()
}

/// Compute the location and scale used by the ESD detector.
fn loc_and_scale(values: &[(usize, f64)], estimator: EsdEstimator) -> Option<(f64, f64)> {
    if values.is_empty() {
        return None;
    }

    match estimator {
        EsdEstimator::Classic => {
            let loc = mean_values(values);
            let scale = std_sample_values(values, loc);
            Some((loc, scale))
        }
        EsdEstimator::Hybrid => {
            let loc = median_values(values);
            let mut abs_devs: Vec<f64> = values.iter().map(|(_, x)| (x - loc).abs()).collect();
            abs_devs.sort_by(|a, b| a.partial_cmp(b).unwrap());
            let mad = calculate_median_sorted(&abs_devs);
            // MAD * 1.4826 is a consistent estimator for the standard deviation of a normal distribution
            Some((loc, mad * 1.4826))
        }
    }
}

/// Score a set of active observations using the provided lambda.
fn score_active_points(
    values: &[(usize, f64)],
    estimator: EsdEstimator,
    lambda: f64,
    scores: &mut [f64],
) {
    let Some((loc, scale)) = loc_and_scale(values, estimator) else {
        return;
    };

    if scale <= 0.0 || !scale.is_finite() {
        return;
    }

    for (idx, val) in values.iter() {
        let stat = (val - loc).abs() / scale;
        let sc = esd_score_stat_over_stat_plus_lambda(stat, lambda);
        scores[*idx] = sc;
    }
}

/// Helper: compute mean from Vec<(idx, value)>
fn mean_values(values: &[(usize, f64)]) -> f64 {
    let sum: f64 = values.iter().map(|(_, v)| *v).sum();
    sum / (values.len() as f64)
}

/// Helper: sample std dev (ddof=1)
fn std_sample_values(values: &[(usize, f64)], mean: f64) -> f64 {
    let n = values.len();
    if n < 2 {
        return 0.0;
    }
    let sum_sq = values.iter().map(|(_, v)| (v - mean).powi(2)).sum::<f64>();
    (sum_sq / (n as f64 - 1.0)).sqrt()
}

/// Helper: median for unsorted values (Vec<(idx,f64)>)
fn median_values(values: &[(usize, f64)]) -> f64 {
    let mut v: Vec<f64> = values.iter().map(|(_, x)| *x).collect();
    v.sort_by(|a, b| a.partial_cmp(b).unwrap());
    calculate_median_sorted(&v)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_calc_test_statistic_median_mad() {
        // simple data with an outlier at index 4
        let data = [1.0, 1.1, 0.9, 1.05, 10.0];
        let values = active_points(&data.iter().cloned().map(Some).collect::<Vec<_>>());
        let (stat, idx) = calc_test_statistic(&values, EsdEstimator::Hybrid);
        assert_eq!(idx, 4);
        assert!(stat > 0.0);
    }

    /// `Hybrid` must studentize against median/MAD and `Classic` against
    /// mean/std. The two were transposed behind a `hybrid: bool` whose `true`
    /// arm selected mean/std — the opposite of the name. A single left-skewed
    /// sample separates them: the mean is dragged below the median, so the two
    /// locations cannot coincide.
    #[test]
    fn estimator_selects_the_documented_location_and_scale() {
        let data = [-20.0, 1.0, 1.1, 0.9, 1.05, 1.2];
        let values = active_points(&data.iter().cloned().map(Some).collect::<Vec<_>>());

        let (hybrid_loc, _) = loc_and_scale(&values, EsdEstimator::Hybrid).unwrap();
        let (classic_loc, classic_scale) = loc_and_scale(&values, EsdEstimator::Classic).unwrap();

        let expected_median = 1.025; // mean of the two middle values, 1.0 and 1.05
        assert!(
            (hybrid_loc - expected_median).abs() < 1e-9,
            "Hybrid must locate at the median, got {hybrid_loc}"
        );

        let expected_mean = data.iter().sum::<f64>() / data.len() as f64;
        assert!(
            (classic_loc - expected_mean).abs() < 1e-9,
            "Classic must locate at the mean, got {classic_loc}"
        );

        // The mean is dragged toward the outlier; the median is not.
        assert!(classic_loc < hybrid_loc);

        // Classic's scale is the sample standard deviation, which the outlier
        // inflates far beyond a MAD-based scale.
        let (_, hybrid_scale) = loc_and_scale(&values, EsdEstimator::Hybrid).unwrap();
        assert!(
            classic_scale > hybrid_scale,
            "the outlier must inflate Classic's scale ({classic_scale}) past Hybrid's ({hybrid_scale})"
        );
    }

    /// The default must be the robust variant, so `METHOD ESD` with no estimator
    /// token keeps studentizing against median/MAD.
    #[test]
    fn default_estimator_is_hybrid() {
        assert_eq!(EsdEstimator::default(), EsdEstimator::Hybrid);
        assert_eq!(ESDOutlierOptions::default().estimator, EsdEstimator::Hybrid);
        assert!(ESDOutlierOptions::default().estimator.is_hybrid());
    }

    #[test]
    fn test_esd_detects_outlier() {
        // index 4 (value 10.0) is a positive outlier
        let data = vec![1.0, 1.1, 0.9, 1.05, 10.0];
        // For n=5, alpha=0.05, max_outliers=2
        // Mean = 2.81, Std = 4.02, max_dev = (10-2.81) = 7.19, R = 1.78
        // t_crit (df=3, p=0.005) = 4.54, Lambda = (4*4.54)/sqrt(25-10+5*20.6) = 1.71
        // Since R > Lambda, index 4 is an outlier.
        // The arithmetic above is mean/std, so this case is `Classic`.
        let (anomalies, scores) = esd_test(&data, 0.05, EsdEstimator::Classic, 2).unwrap();

        // outlier at index 4 should be detected
        assert!(anomalies.iter().any(|a| a.index == 4));

        // value 10.0 is above the location estimate, so the signal must be Positive
        let outlier = anomalies.iter().find(|a| a.index == 4).unwrap();
        assert!(matches!(outlier.signal, AnomalySignal::Positive));
        assert_eq!(outlier.value, 10.0);
        assert!(outlier.score > 0.5); // stat >> lambda so score > 0.5

        // scores vector length matches data
        assert_eq!(scores.len(), data.len());

        // all scores are in [0, 1]
        for &s in &scores {
            assert!((0.0..=1.0).contains(&s));
        }

        // the outlier index has the highest score
        let max_idx = scores
            .iter()
            .enumerate()
            .max_by(|a, b| a.1.partial_cmp(b.1).unwrap())
            .map(|(i, _)| i)
            .unwrap();
        assert_eq!(max_idx, 4);
    }

    #[test]
    fn test_esd_detects_negative_outlier() {
        // index 0 (value -10.0) is below the median — should be Negative signal
        let data = vec![-10.0, 1.0, 1.1, 0.9, 1.05];
        let (anomalies, scores) =
            esd_test(&data, 0.05, EsdEstimator::Hybrid, data.len() / 2).unwrap();

        assert!(anomalies.iter().any(|a| a.index == 0));
        let outlier = anomalies.iter().find(|a| a.index == 0).unwrap();
        assert!(matches!(outlier.signal, AnomalySignal::Negative));
        assert_eq!(outlier.value, -10.0);
        assert_eq!(scores.len(), data.len());
    }

    #[test]
    fn test_esd_rosner_data() {
        // Data from Rosner (1983)
        let data = [
            -0.25, 0.68, 0.94, 1.15, 1.20, 1.26, 1.26, 1.34, 1.38, 1.43, 1.49, 1.49, 1.55, 1.56,
            1.58, 1.65, 1.69, 1.70, 1.76, 1.77, 1.81, 1.91, 1.94, 1.96, 1.99, 2.06, 2.09, 2.10,
            2.14, 2.15, 2.23, 2.24, 2.26, 2.35, 2.37, 2.40, 2.47, 2.54, 2.62, 2.64, 2.90, 2.92,
            2.92, 2.93, 3.21, 3.26, 3.30, 3.59, 3.68, 4.30, 4.64, 5.34, 5.42, 6.01,
        ];

        // Rosner's published result is the mean/std procedure, i.e. `Classic`.
        let result = detect_anomalies_esd(&data, 0.05, EsdEstimator::Classic, Some(10)).unwrap();
        // Rosner's test on this data (with alpha=0.05) detects 3 outliers: 6.01, 5.42, 5.34
        assert_eq!(result.anomalies.len(), 3);
        let indices: Vec<usize> = result.anomalies.iter().map(|a| a.index).collect();
        assert!(indices.contains(&53)); // 6.01
        assert!(indices.contains(&52)); // 5.42
        assert!(indices.contains(&51)); // 5.34
    }
}
