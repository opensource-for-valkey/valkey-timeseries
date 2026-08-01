/// Smoothed Z-Score Anomaly Detector algorithm described here:
/// https://stackoverflow.com/a/22640362/14797322
///
/// Port of the golang implementation here:
/// https://github.com/MicahParks/peakdetect
/// Original License: Apache-2.0
use super::utils::normalize_evidence;
use super::{Anomaly, AnomalyDetector, AnomalyMethod, AnomalyResult, AnomalySignal};
use crate::analysis::{TimeSeriesAnalysisError, TimeSeriesAnalysisResult};

struct MovingMeanStdDev {
    cache: Vec<f64>,
    index: usize,
    prev_mean: f64,
    prev_variance: f64,
}

impl MovingMeanStdDev {
    fn new() -> Self {
        Self {
            cache: Vec::new(),
            index: 0,
            prev_mean: 0.0,
            prev_variance: 0.0,
        }
    }

    /// Initialize creates the necessary assets for the MovingMeanStdDev. It also computes the
    /// resulting mean and population standard deviation using Welford's method.
    ///
    /// https://www.johndcook.com/blog/standard_deviation/
    fn initialize(&mut self, initial_values: &[f64]) -> (f64, f64) {
        self.cache = initial_values.to_vec();
        self.index = 0;

        let cache_len = self.cache.len();

        let mut mean = initial_values[0];
        let mut prev_mean = mean;
        let mut sum_of_squares = 0.0;

        for i in 2..=cache_len {
            let value = initial_values[i - 1];
            let delta = value - prev_mean;
            mean = prev_mean + delta / (i as f64);
            sum_of_squares += delta * (value - mean);
            prev_mean = mean;
        }

        self.prev_mean = mean;
        // Numerical errors can cause a tiny negative M2; clamp to zero to
        // avoid sqrt of negative which results in NaN.
        self.prev_variance = (sum_of_squares / cache_len as f64).max(0.0);

        (mean, self.prev_variance.sqrt())
    }

    /// `next` computes the next mean and population standard deviation. It uses a sliding window and is based on Welford's
    /// method.
    ///
    /// https://stackoverflow.com/a/14638138/14797322
    fn next(&mut self, value: f64) -> (f64, f64) {
        let out_of_window = self.cache[self.index];
        self.cache[self.index] = value;
        self.index = (self.index + 1) % self.cache.len();

        let cache_len = self.cache.len() as f64;
        let new_mean = self.prev_mean + (value - out_of_window) / cache_len;
        self.prev_variance += (value - new_mean + out_of_window - self.prev_mean)
            * (value - out_of_window)
            / cache_len;
        // Guard against tiny negative variances introduced by floating point
        // rounding errors.
        if !self.prev_variance.is_finite() || self.prev_variance < 0.0 {
            self.prev_variance = 0.0;
        }

        self.prev_mean = new_mean;

        (self.prev_mean, self.prev_variance.sqrt())
    }
}

/// SmoothedZScoreAnomalyDetector detects peaks in realtime timeseries data using z-scores.
///
/// This is a Rust implementation of the algorithm described by this StackOverflow answer:
/// https://stackoverflow.com/a/22640362/14797322
///
/// Brakel, J.P.G. van (2014). "Robust peak detection algorithm using z-scores". Stack Overflow. Available
/// at: https://stackoverflow.com/questions/22583391/peak-signal-detection-in-realtime-timeseries-data/22640362#22640362
/// (version: 2020-11-08).
pub struct SmoothedZScoreAnomalyDetector {
    index: usize,
    /// `influence` is the influence of signals on the algorithm's detection threshold. If put at 0, signals have
    /// no influence on the threshold, such that future signals are detected based on a threshold that is calculated with
    /// a mean and standard deviation that is not influenced by past signals. If put at 0.5, signals have half the
    /// influence of normal data points. If you put the influence at 0, you implicitly assume stationarity
    /// (i.e., with a stable average over the long term).
    influence: f64,
    /// `threshold` is the number of standard deviations from the moving mean above which the algorithm will classify a new
    /// datapoint as being a signal. The threshold therefore directly influences how sensitive the algorithm is and thereby
    /// also determines how often the algorithm signals. Examine your own data and choose a sensible threshold that makes
    /// the algorithm signal when you want it to (some trial-and-error might be needed here to get to a good threshold for your purpose).
    threshold: f64,
    /// `lag` determines how much your data will be smoothed and how adaptive the algorithm is to change in the long-term
    /// average of the data. The more stationary your data is, the more lags you should include to improve the
    /// robustness of the algorithm. If your data contains time-varying trends, you should consider how quickly you want
    /// the algorithm to adapt to these trends. I.e., if you put lag at 10, it takes 10 'periods' before the algorithm's
    /// threshold is adjusted to any systematic changes in the long-term average.
    lag: usize,
    moving_stats: MovingMeanStdDev,
    prev_mean: f64,
    prev_std_dev: f64,
    prev_value: f64,
    prev_score: f64,
    is_trained: bool,
}

impl SmoothedZScoreAnomalyDetector {
    /// Creates a new SmoothedZScoreAnomalyDetector.
    pub fn new(
        influence: f64,
        threshold: f64,
        lag: usize,
    ) -> Result<Self, TimeSeriesAnalysisError> {
        if lag == 0 {
            return Err(TimeSeriesAnalysisError::InvalidInput(
                "the length of the initial values is zero, the length is used as the lag for the algorithm".to_string()
            ));
        }

        let mut res = Self {
            index: 0,
            influence,
            threshold,
            lag,
            ..Default::default()
        };

        res.is_trained = false;

        Ok(res)
    }

    /// Next processes the next value and determines its signal.
    pub fn next(&mut self, value: f64) -> TimeSeriesAnalysisResult<AnomalySignal> {
        if !self.is_trained {
            return Err(TimeSeriesAnalysisError::NotTrained);
        }
        self.index = (self.index + 1) % self.lag;
        let score = (value - self.prev_mean).abs();

        let signal = if score > self.threshold * self.prev_std_dev {
            if value > self.prev_mean {
                AnomalySignal::Positive
            } else {
                AnomalySignal::Negative
            }
        } else {
            AnomalySignal::None
        };

        let adjusted_value = if signal != AnomalySignal::None {
            self.influence * value + (1.0 - self.influence) * self.prev_value
        } else {
            value
        };

        let (mean, std_dev) = self.moving_stats.next(adjusted_value);
        self.prev_mean = mean;
        self.prev_std_dev = std_dev;
        self.prev_value = adjusted_value;
        self.prev_score = score;

        Ok(signal)
    }

    pub fn next_batch(&mut self, values: &[f64]) -> TimeSeriesAnalysisResult<Vec<AnomalySignal>> {
        values
            .iter()
            .map(|&v| self.next(v))
            .collect::<TimeSeriesAnalysisResult<Vec<_>>>()
    }

    /// Calculates a normalized anomaly score in [0, 1] for `value`.
    ///
    /// Evidence is the departure from the moving mean; the boundary is the
    /// `threshold * prev_std_dev` that [`Self::next`] tests it against. So:
    /// - `0.0` when `value == prev_mean`
    /// - `0.5` when the deviation sits exactly on the threshold
    /// - approaches `1.0` as the deviation grows
    ///
    /// This algorithm already satisfied the 0.5 contract — `z / (threshold + z)`
    /// is `normalize_evidence(deviation, threshold * std_dev)` rearranged — so
    /// routing it through the shared helper changes no value. It is written this
    /// way so the arithmetic lives in one place rather than being re-derived,
    /// and so the degenerate cases cannot drift from the rest of the module.
    pub fn get_anomaly_score(&self, value: f64) -> f64 {
        let deviation = (value - self.prev_mean).abs();
        normalize_evidence(deviation, self.threshold * self.prev_std_dev)
    }

    pub fn detect(&mut self, ts: &[f64]) -> TimeSeriesAnalysisResult<AnomalyResult> {
        // Use the smaller of the timeseries length and lag to form the initial window.
        // Previously max() was used which caused the initial window to be the entire
        // timeseries when ts.len() >= lag, leaving nothing to process in `rest`.
        let n = ts.len().min(self.lag);
        let (_initial, rest) = ts.split_at(n);
        if !self.is_trained {
            self.train(ts)?;
        }

        // Keep output lengths equal to the input length (pad the initial window).
        let mut scores: Vec<f64> = vec![0.0; n];
        let mut anomalies: Vec<Anomaly> = Vec::with_capacity(4);

        for (index, &value) in rest.iter().enumerate() {
            let score = self.get_anomaly_score(value);

            let signal = self.next(value)?;
            if signal != AnomalySignal::None {
                anomalies.push(Anomaly {
                    index: index + n,
                    signal,
                    value,
                    score,
                });
            }
            scores.push(score);
        }

        Ok(AnomalyResult {
            scores,
            anomalies,
            threshold: self.threshold,
            method: AnomalyMethod::SmoothedZScore,
            method_info: None,
        })
    }
}

impl Default for SmoothedZScoreAnomalyDetector {
    fn default() -> Self {
        Self {
            index: 0,
            influence: 0.0,
            lag: 0,
            moving_stats: MovingMeanStdDev::new(),
            prev_mean: 0.0,
            prev_std_dev: 0.0,
            prev_value: 0.0,
            threshold: 0.0,
            prev_score: f64::NAN,
            is_trained: false,
        }
    }
}

/// The smoothed z-score algorithm implements only [`AnomalyDetector`], not
/// [`PointDetector`]: each verdict is taken against a moving mean and standard
/// deviation that the preceding points established, and that signals themselves
/// feed back into via `influence`.
impl AnomalyDetector for SmoothedZScoreAnomalyDetector {
    fn method(&self) -> AnomalyMethod {
        AnomalyMethod::SmoothedZScore
    }

    fn train(&mut self, data: &[f64]) -> TimeSeriesAnalysisResult<()> {
        // Use the smaller of the input data length and the configured lag for
        // initializing the moving statistics.
        let mut n = self.lag;
        if n >= data.len() {
            n = if !data.is_empty() { data.len() - 1 } else { 0 };
        }
        if n == 0 && !data.is_empty() {
            // If lag is 0 or data.len() is 1, use the first element
            n = 1;
        }
        if n == 0 {
            return Err(TimeSeriesAnalysisError::InsufficientData {
                message: "insufficient data to train smoothed z-score".to_string(),
                required: 1,
                actual: 0,
            });
        }
        let (initial, _rest) = data.split_at(n);
        let (mean, std_dev) = self.moving_stats.initialize(initial);

        self.prev_mean = mean;
        self.prev_std_dev = std_dev;
        self.prev_value = initial[n - 1];
        // Mark the detector as trained so `next()` can be called safely.
        self.is_trained = true;
        Ok(())
    }

    fn detect(&mut self, ts: &[f64]) -> TimeSeriesAnalysisResult<AnomalyResult> {
        SmoothedZScoreAnomalyDetector::detect(self, ts)
    }
}

/// Options for configuring the Smoothed Z-Score algorithm.
///
/// The Smoothed Z-Score algorithm is useful for detecting signals, such as anomalies or outliers, in time-series
/// data by comparing new datapoints to a continually adjusted moving average and standard deviation. The following
/// parameters influence its sensitivity and adaptability.
///
/// # Fields
///
/// * `threshold` - The number of standard deviations from the moving mean required to classify a new datapoint as
///   a signal. A larger threshold reduces sensitivity to outliers, while a smaller threshold makes the algorithm
///   more sensitive.
///
/// * `influence` - A value between 0 and 1 that determines how much detected signals influence the dataset's
///   moving mean and standard deviation. A lower influence makes the algorithm less affected by signals, while a
///   higher influence allows signals to have a greater impact.
///
/// * `lag` - The number of previous datapoints used to calculate the moving mean and standard deviation. Higher
///   values result in a smoother long-term average, making the algorithm less responsive to short-term fluctuations
///   but more robust to changes in the long-term trend.
///
/// # Examples
///
/// ```ignore
/// let options = SmoothedZScoreOptions {
///     threshold: 3.5,
///     influence: 0.5,
///     lag: 10,
/// };
/// ```
///
/// This example initializes the `SmoothedZScoreOptions` structure with a threshold of 3.5 standard deviations,
/// an influence of 0.5, and a lag of 10, providing a balanced configuration for detecting outliers in moderately
/// stationary data.
///
/// # Notes
///
/// Adjusting these parameters requires an understanding of your dataset's characteristics. For highly
/// non-stationary data, consider decreasing `lag` to improve adaptability. For datasets with frequent
/// noise or minor fluctuations, increasing `threshold` can improve robustness, while tuning `influence`
/// helps control the trade-off between reactivity and noise sensitivity.
#[derive(Clone, Copy, Debug)]
pub struct SmoothedZScoreOptions {
    /// `threshold` is the number of standard deviations from the moving mean above which the algorithm will classify a new
    /// datapoint as being a signal.
    pub threshold: f64,
    /// `influence` is the influence of signals on the algorithm's detection threshold.
    pub influence: f64,
    /// `lag` determines how much your data will be smoothed and how adaptive the algorithm is to change in the long-term
    /// average of the data. The more stationary your data is, the more lags you should include to improve the
    /// robustness of the algorithm.
    pub lag: usize,
}

impl Default for SmoothedZScoreOptions {
    fn default() -> Self {
        Self {
            threshold: 3.5,
            influence: 0.0,
            lag: 10,
        }
    }
}

/// Detects anomalies in a time series using the Smoothed Z-Score algorithm.
pub(super) fn detect_anomalies_smoothed_zscore(
    ts: &[f64],
    options: SmoothedZScoreOptions,
) -> TimeSeriesAnalysisResult<AnomalyResult> {
    let SmoothedZScoreOptions {
        lag,
        influence,
        threshold,
    } = options;

    let n = ts.len();
    if n < lag {
        return Err(TimeSeriesAnalysisError::InsufficientData {
            message: "TSDB: insufficient samples for smoothed z-score lag".to_string(),
            required: lag,
            actual: n,
        });
    }

    let mut detector = SmoothedZScoreAnomalyDetector::new(influence, threshold, lag)?;
    detector.train(ts)?;
    detector.detect(ts)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_peak_detector_initialization() {
        let result = SmoothedZScoreAnomalyDetector::new(0.1, 2.0, 5);
        assert!(result.is_ok());
    }

    #[test]
    fn test_peak_detector_empty_initial_values() {
        let result = SmoothedZScoreAnomalyDetector::new(0.1, 2.0, 0);
        assert!(result.is_err());
    }

    #[test]
    fn test_peak_detection() {
        let initial_values = vec![
            1.0, 1.0, 1.1, 1.0, 0.9, 1.0, 1.0, 1.1, 1.0, 0.9, 1.0, 1.1, 1.0, 1.0, 0.9, 1.0, 1.0,
            1.1, 1.0, 1.0,
        ];

        let lag = initial_values.len();
        let mut detector = SmoothedZScoreAnomalyDetector::new(0.0, 2.0, lag).unwrap();
        // Train the detector using the provided initial values so the moving
        // statistics reflect the sample baseline expected by this test.
        detector.train(&initial_values).unwrap();

        // Test with a clear positive peak
        let signal = detector.next(5.0).unwrap();
        assert_eq!(signal, AnomalySignal::Positive);

        // Test with normal values
        let signal = detector.next(1.0).unwrap();
        assert_eq!(signal, AnomalySignal::None);

        // Test with a clear negative peak
        let signal = detector.next(-3.0).unwrap();
        assert_eq!(signal, AnomalySignal::Negative);
    }

    #[test]
    fn test_batch_processing() {
        let initial_values = vec![1.0; 10];

        let lag = initial_values.len();
        let mut detector = SmoothedZScoreAnomalyDetector::new(0.0, 2.0, lag).unwrap();
        detector.train(&initial_values).unwrap();

        let test_values = vec![1.0, 5.0, 1.0, -3.0, 1.0];
        let signals = detector.next_batch(&test_values).unwrap();

        assert_eq!(signals.len(), 5);
        // The exact signals depend on the algorithm's internal state,
        // but we can verify the length matches
    }

    #[test]
    fn test_get_anomaly_score_is_normalized_and_roughly_monotonic() {
        let initial_values = vec![1.0, 2.0, 3.0, 2.0, 1.0];

        let lag = initial_values.len();
        let mut detector = SmoothedZScoreAnomalyDetector::new(0.0, 2.0, lag).unwrap();
        detector.train(&initial_values).unwrap();

        // At the mean => 0.0
        let s0 = detector.get_anomaly_score(detector.prev_mean);
        assert!(s0.abs() < 1e-12);

        // At z == threshold => 0.5
        // value = mean + threshold * std_dev
        let at_threshold_value = detector.prev_mean + detector.threshold * detector.prev_std_dev;
        let s_half = detector.get_anomaly_score(at_threshold_value);
        assert!((s_half - 0.5).abs() < 1e-12);

        // Larger deviation => higher score, still within [0, 1]
        let far_value = detector.prev_mean + 10.0 * detector.threshold * detector.prev_std_dev;
        let s_far = detector.get_anomaly_score(far_value);
        assert!(s_far > s_half);
        assert!((0.0..=1.0).contains(&s_far));
    }

    #[test]
    fn test_get_anomaly_score_clamps_to_unit_interval() {
        let initial_values = [1.0, 2.0, 3.0, 2.0, 1.0];

        let lag = initial_values.len();
        let mut detector = SmoothedZScoreAnomalyDetector::new(0.0, 2.0, lag).unwrap();
        detector.train(&initial_values).unwrap();

        let s = detector.get_anomaly_score(detector.prev_mean + 1e308);
        assert!((0.0..=1.0).contains(&s));
    }

    #[test]
    fn test_get_anomaly_score_handles_degenerate_std_dev() {
        // All equal => std_dev == 0.0 after initialization.
        let initial_values = vec![1.0; 10];
        let lag = initial_values.len();

        let mut detector = SmoothedZScoreAnomalyDetector::new(0.0, 2.0, lag).unwrap();
        detector.train(&initial_values).unwrap();

        assert!(detector.prev_std_dev <= 0.0);

        // No deviation => 0.0
        let s0 = detector.get_anomaly_score(1.0);
        assert!(s0.abs() < 1e-12);

        // Any deviation => 1.0
        let s1 = detector.get_anomaly_score(2.0);
        assert!((s1 - 1.0).abs() < 1e-12);
    }

    #[test]
    fn test_detect_with_valid_input() {
        let ts = vec![
            1.0, 1.0, 1.1, 1.0, 0.9, 1.0, 1.0, 1.1, 1.0, 0.9, 1.0, 1.1, 1.0, 1.0, 0.9, 1.0, 1.0,
            1.1, 1.0, 1.0, 5.0, // anomaly
            1.0, 1.0, 1.0,
        ];

        let options = SmoothedZScoreOptions {
            threshold: 2.0,
            influence: 0.0,
            lag: 10,
        };

        let anomaly_result = detect_anomalies_smoothed_zscore(&ts, options).unwrap();

        assert_eq!(anomaly_result.scores.len(), ts.len());

        assert_eq!(anomaly_result.anomalies.len(), 1);
        assert_eq!(anomaly_result.anomalies[0].index, 20);
        assert_eq!(anomaly_result.anomalies[0].signal, AnomalySignal::Positive);
        assert_eq!(anomaly_result.anomalies[0].value, 5.0);

        assert_eq!(anomaly_result.threshold, 2.0);
        assert_eq!(anomaly_result.method, AnomalyMethod::SmoothedZScore);
    }

    #[test]
    fn test_detect_with_zero_lag() {
        let ts = vec![1.0, 2.0, 3.0, 4.0, 5.0];

        let options = SmoothedZScoreOptions {
            threshold: 2.0,
            influence: 0.5,
            lag: 0,
        };

        let result = detect_anomalies_smoothed_zscore(&ts, options);
        assert!(result.is_err());

        match result.unwrap_err() {
            TimeSeriesAnalysisError::InvalidInput(msg) => {
                assert!(msg.contains("zero"));
            }
            _ => panic!("Expected InvalidInput error"),
        }
    }

    #[test]
    fn test_detect_with_insufficient_data() {
        let ts = vec![1.0, 2.0, 3.0];

        let options = SmoothedZScoreOptions {
            threshold: 2.0,
            influence: 0.5,
            lag: 10,
        };

        let result = detect_anomalies_smoothed_zscore(&ts, options);
        assert!(result.is_err());

        match result.unwrap_err() {
            TimeSeriesAnalysisError::InsufficientData {
                required, actual, ..
            } => {
                assert_eq!(required, 10);
                assert_eq!(actual, 3);
            }
            _ => panic!("Expected InsufficientData error"),
        }
    }

    #[test]
    fn test_detect_pads_initial_window() {
        let ts = vec![1.0, 1.0, 1.0, 1.0, 1.0, 5.0, 1.0];

        let options = SmoothedZScoreOptions {
            threshold: 2.0,
            influence: 0.0,
            lag: 5,
        };

        let result = detect_anomalies_smoothed_zscore(&ts, options).unwrap();

        assert_eq!(result.anomalies.len(), 1);
        assert_eq!(result.scores.len(), ts.len());
    }

    #[test]
    fn test_detect_identifies_positive_anomaly() {
        let ts = vec![
            1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 10.0, // clear positive anomaly
        ];

        let options = SmoothedZScoreOptions {
            threshold: 2.0,
            influence: 0.0,
            lag: 10,
        };

        let result = detect_anomalies_smoothed_zscore(&ts, options).unwrap();

        // The anomaly should be detected at index 10
        assert_eq!(result.anomalies.len(), 1);
        assert_eq!(result.anomalies[0].signal, AnomalySignal::Positive);
    }

    #[test]
    fn test_detect_identifies_negative_anomaly() {
        let ts = vec![
            1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, -10.0, // clear negative anomaly
        ];

        let options = SmoothedZScoreOptions {
            threshold: 2.0,
            influence: 0.0,
            lag: 10,
        };

        let anomaly_result = detect_anomalies_smoothed_zscore(&ts, options).unwrap();

        // The anomaly should be detected at index 10
        assert_eq!(anomaly_result.anomalies.len(), 1);
        assert_eq!(anomaly_result.anomalies[0].signal, AnomalySignal::Negative);
    }

    #[test]
    fn test_detect_with_high_influence() {
        let ts = vec![1.0, 1.0, 1.0, 1.0, 1.0, 5.0, 5.0, 5.0, 5.0, 5.0, 1.0];

        let options = SmoothedZScoreOptions {
            threshold: 2.0,
            influence: 0.9,
            lag: 5,
        };

        let anomaly_result = detect_anomalies_smoothed_zscore(&ts, options).unwrap();

        // With high influence, the algorithm should adapt to the new level
        assert_eq!(anomaly_result.anomalies.len(), 3);
    }

    #[test]
    fn test_detect_with_low_influence() {
        let ts = vec![1.0, 1.0, 1.0, 1.0, 1.0, 5.0, 5.0, 5.0, 5.0, 5.0, 1.0];

        let options = SmoothedZScoreOptions {
            threshold: 2.0,
            influence: 0.1,
            lag: 5,
        };

        let anomaly_result = detect_anomalies_smoothed_zscore(&ts, options).unwrap();

        // With low influence, anomalies should have minimal effect on the threshold
        assert_eq!(anomaly_result.anomalies.len(), 6);
    }

    #[test]
    fn test_detect_exact_lag_length() {
        let ts = vec![1.0, 1.0, 1.0, 1.0, 1.0];

        let options = SmoothedZScoreOptions {
            threshold: 2.0,
            influence: 0.5,
            lag: 5,
        };

        let anomaly_result = detect_anomalies_smoothed_zscore(&ts, options).unwrap();

        assert_eq!(anomaly_result.anomalies.len(), 0);
        assert_eq!(anomaly_result.scores.len(), 5);
    }

    #[test]
    fn test_detect_default_options() {
        let ts = vec![1.0; 20];

        let options = SmoothedZScoreOptions::default();
        let options_with_lag = SmoothedZScoreOptions { lag: 10, ..options };

        let result = detect_anomalies_smoothed_zscore(&ts, options_with_lag);
        assert!(result.is_ok());
    }

    #[test]
    fn test_detect_scores_are_non_negative() {
        let ts = vec![
            1.0, 1.0, 1.0, 1.0, 1.0, 5.0, -5.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0,
        ];

        let options = SmoothedZScoreOptions {
            threshold: 2.0,
            influence: 0.5,
            lag: 5,
        };

        let result = detect_anomalies_smoothed_zscore(&ts, options).unwrap();

        for score in result.scores {
            assert!(score >= 0.0, "Score should be non-negative: {}", score);
        }
    }

    #[test]
    fn test_detect_with_flat_data() {
        let ts = vec![5.0; 20];

        let options = SmoothedZScoreOptions {
            threshold: 2.0,
            influence: 0.0,
            lag: 10,
        };

        let result = detect_anomalies_smoothed_zscore(&ts, options).unwrap();

        // All should be non-anomalous in flat data
        assert_eq!(result.anomalies.len(), 0);
    }
}
