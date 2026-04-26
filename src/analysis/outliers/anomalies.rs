//! Anomaly detection algorithms for time series
//!
//! This module provides various algorithms for detecting anomalies and outliers
//! in time series data, including statistical process control, isolation forest,
//! Mad, Double Mad, and Random Cut Forest approaches.

use super::{
    AnomalyDetector, AnomalyMethod, AnomalyResult, Detector, MADAnomalyOptions,
    rcf_outlier_detector::RCFOptions, smoothed_zscores::SmoothedZScoreOptions,
    zscore_outlier_detector::ZScoreOutlierDetector,
};
use crate::analysis::outliers::esd_outlier_detector::ESDOutlierOptions;
use crate::analysis::seasonality::{Seasonality, seasonally_adjust};
use crate::analysis::{INSUFFICIENT_DATA_ERROR, TimeSeriesAnalysisError, TimeSeriesAnalysisResult};

#[derive(Debug, Clone)]
pub enum AnomalyDetectionMethodOptions {
    Cusum,
    Ewma(Option<f64>),
    InterQuartileRange(Option<f64>),
    ZScore(Option<f64>),
    SmoothedZScore(SmoothedZScoreOptions),
    ModifiedZScore(Option<f64>),
    Mad(MADAnomalyOptions),
    DoubleMAD(MADAnomalyOptions),
    Rcf(RCFOptions),
    Esd(Option<ESDOutlierOptions>),
}

impl Default for AnomalyDetectionMethodOptions {
    fn default() -> Self {
        AnomalyDetectionMethodOptions::ZScore(Some(ZScoreOutlierDetector::DEFAULT_THRESHOLD))
    }
}

impl AnomalyDetectionMethodOptions {
    pub fn method(&self) -> AnomalyMethod {
        match self {
            Self::Cusum => AnomalyMethod::Cusum,
            Self::Ewma(_) => AnomalyMethod::Ewma,
            Self::InterQuartileRange(_) => AnomalyMethod::InterquartileRange,
            Self::ZScore(_) => AnomalyMethod::ZScore,
            Self::SmoothedZScore(_) => AnomalyMethod::SmoothedZScore,
            Self::ModifiedZScore(_) => AnomalyMethod::ModifiedZScore,
            Self::Mad(_) => AnomalyMethod::Mad,
            Self::DoubleMAD(_) => AnomalyMethod::DoubleMAD,
            Self::Rcf(_) => AnomalyMethod::RandomCutForest,
            Self::Esd(_) => AnomalyMethod::Esd,
        }
    }
}

/// Options for anomaly detection
#[derive(Debug, Clone, Default)]
pub struct AnomalyOptions {
    /// Seasonal adjustment options
    pub seasonality: Option<Seasonality>,
    /// Anomaly detection method options
    pub options: AnomalyDetectionMethodOptions,
}

impl AnomalyOptions {
    pub fn method(&self) -> AnomalyMethod {
        self.options.method()
    }
}

/// Detects anomalies in a time series
///
/// This function applies various anomaly detection algorithms to identify
/// points in the time series that deviate significantly from normal behavior.
///
/// # Arguments
///
/// * `values` - The time series to analyze
/// * `options` - Options controlling the anomaly detection
///
/// # Returns
///
/// * A result containing anomaly scores and binary classifications
///
/// # Example
///
/// ```ignore
/// use valkey_timeseries::analysis::outliers::anomalies::{
///     AnomalyDetectionMethodOptions, AnomalyOptions, detect_anomalies,
/// };
///
/// // Create a time series with some anomalies
/// let mut values = vec![0.0; 100];
/// for i in 0..100 {
///     values[i] = (i as f64 / 10.0).sin();
/// }
/// values[25] = 5.0; // Anomaly
/// values[75] = -5.0; // Anomaly
///
/// let options = AnomalyOptions {
///     options: AnomalyDetectionMethodOptions::ZScore(Some(3.0)),
///     ..Default::default()
/// };
///
/// let result = detect_anomalies(&values, options).unwrap();
/// println!("Anomalies detected: {}", result.anomalies.len());
/// ```
/// Run the configured detector over `values`.
///
/// Non-finite values (NaN, ±inf) are left out of the analysis: they have no place on the
/// scale any detector measures, and letting them in broke most of them — CUSUM and EWMA
/// normalized NaN to 0 and flagged everything after it, ESD picked NaN as the most extreme
/// point, a NaN in the smoothed z-score window disabled detection for the rest of the series.
/// Detection runs on the finite values; the result is mapped back onto the original
/// positions, with a NaN score (not scored) for every excluded one.
pub fn detect_anomalies(
    values: &[f64],
    options: &AnomalyOptions,
) -> TimeSeriesAnalysisResult<AnomalyResult> {
    if values.iter().all(|v| v.is_finite()) {
        return detect_finite(values, options);
    }

    let (positions, finite): (Vec<usize>, Vec<f64>) = values
        .iter()
        .enumerate()
        .filter(|(_, v)| v.is_finite())
        .map(|(i, &v)| (i, v))
        .unzip();
    let mut result = detect_finite(&finite, options)?;

    let mut scores = vec![f64::NAN; values.len()];
    for (&pos, &score) in positions.iter().zip(&result.scores) {
        scores[pos] = score;
    }
    result.scores = scores;
    for anomaly in &mut result.anomalies {
        anomaly.index = positions[anomaly.index];
    }
    Ok(result)
}

fn detect_finite(
    values: &[f64],
    options: &AnomalyOptions,
) -> TimeSeriesAnalysisResult<AnomalyResult> {
    let n = values.len();

    if n < 3 {
        return Err(TimeSeriesAnalysisError::InsufficientData {
            message: INSUFFICIENT_DATA_ERROR.to_string(),
            required: 3,
            actual: n,
        });
    }

    // Apply seasonal adjustment if requested
    if let Some(adjustment) = &options.seasonality {
        let adjusted = seasonally_adjust(values, adjustment)?;
        let mut result = handle_dispatch(&adjusted, options)?;
        // we calculate anomalies on the seasonally adjusted data, but we want to report the original values in the result
        for anomaly in &mut result.anomalies {
            anomaly.value = values[anomaly.index];
        }
        return Ok(result);
    };

    handle_dispatch(values, options)
}

fn handle_dispatch(
    values: &[f64],
    options: &AnomalyOptions,
) -> TimeSeriesAnalysisResult<AnomalyResult> {
    let mut detector = Detector::build(values, &options.options)?;
    detector.train(values)?;
    let res = detector.detect(values)?;
    debug_assert_eq!(
        values.len(),
        res.scores.len(),
        "Mismatch between scores.len() and samples.len() in {:?}",
        options.method()
    );
    Ok(res)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn options(method: AnomalyDetectionMethodOptions) -> AnomalyOptions {
        AnomalyOptions {
            seasonality: None,
            options: method,
        }
    }

    /// 200 points of ~100 ± 2 with no outliers.
    fn steady_series() -> Vec<f64> {
        (0..200)
            .map(|i| 100.0 + ((i * 7919) % 41) as f64 / 10.0 - 2.0)
            .collect()
    }

    #[test]
    fn test_nan_is_excluded_from_detection() {
        // CUSUM used to normalize NaN to 0, flagging every point after it.
        let clean = steady_series();
        let mut with_nan = clean.clone();
        with_nan[150] = f64::NAN;

        for method in [
            AnomalyDetectionMethodOptions::Cusum,
            AnomalyDetectionMethodOptions::Esd(None),
            AnomalyDetectionMethodOptions::ModifiedZScore(None),
        ] {
            let name = format!("{:?}", method.method());
            let baseline = detect_anomalies(&clean, &options(method.clone())).unwrap();
            let result = detect_anomalies(&with_nan, &options(method)).unwrap();

            assert_eq!(result.scores.len(), with_nan.len(), "{name}");
            assert!(
                result.scores[150].is_nan(),
                "{name}: the NaN sample is not scored"
            );
            assert!(
                result.anomalies.iter().all(|a| a.index != 150),
                "{name}: the NaN sample is not flagged"
            );
            assert_eq!(
                result.anomalies.len(),
                baseline.anomalies.len(),
                "{name}: one missing sample must not change what is flagged"
            );
        }
    }

    #[test]
    fn test_anomaly_indices_map_back_past_excluded_values() {
        let mut values = vec![5.0; 30];
        values[3] = f64::NAN;
        values[20] = 100.0;
        let result =
            detect_anomalies(&values, &options(AnomalyDetectionMethodOptions::Esd(None))).unwrap();
        let indices: Vec<usize> = result.anomalies.iter().map(|a| a.index).collect();
        assert_eq!(indices, vec![20]);
        assert_eq!(result.anomalies[0].value, 100.0);
    }

    #[test]
    fn test_zero_mad_series_still_flags_a_spike() {
        // More than half the points on the median makes the MAD zero; ESD and the modified
        // z-score then divided by zero and never flagged anything.
        let mut values = vec![5.0; 29];
        values.push(100.0);
        for method in [
            AnomalyDetectionMethodOptions::Esd(None),
            AnomalyDetectionMethodOptions::ModifiedZScore(None),
        ] {
            let name = format!("{:?}", method.method());
            let result = detect_anomalies(&values, &options(method)).unwrap();
            let indices: Vec<usize> = result.anomalies.iter().map(|a| a.index).collect();
            assert_eq!(indices, vec![29], "{name}");
        }

        // A truly constant series has nothing to flag.
        let constant = vec![5.0; 30];
        for method in [
            AnomalyDetectionMethodOptions::Esd(None),
            AnomalyDetectionMethodOptions::ModifiedZScore(None),
        ] {
            let result = detect_anomalies(&constant, &options(method)).unwrap();
            assert!(result.anomalies.is_empty());
        }
    }
}
