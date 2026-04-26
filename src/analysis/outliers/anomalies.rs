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

    pub fn for_ewma(alpha: f64) -> Self {
        AnomalyDetectionMethodOptions::Ewma(Some(alpha))
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

    pub fn set_seasonal_periods(&mut self, periods: Vec<usize>) {
        self.seasonality = Some(Seasonality::Periods(periods));
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
pub fn detect_anomalies(
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
