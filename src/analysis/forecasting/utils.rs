use crate::common::Sample;
use crate::common::hash::IntMap;
use crate::error::TsdbError;
use anofox_forecast::seasonality::auto_trend::TrendCriterion;
use anofox_forecast::{ForecastError, core::TimeSeries as ForecastTimeSeries};
use chrono::{DateTime, Utc};
use std::time::Duration;
use valkey_module::{ValkeyError, ValkeyResult};

pub fn make_forecast_time_series(
    iter: impl Iterator<Item = Sample>,
) -> Result<ForecastTimeSeries, TsdbError> {
    let initial_capacity = iter.size_hint().0;
    let mut timestamps = Vec::with_capacity(initial_capacity);
    let mut values = Vec::with_capacity(initial_capacity);
    for sample in iter {
        let dt: DateTime<Utc> =
            DateTime::from_timestamp_millis(sample.timestamp).ok_or_else(|| {
                TsdbError::ForecastError(format!(
                    "Invalid timestamp {} in sample. ",
                    sample.timestamp
                ))
            })?;

        timestamps.push(dt);
        values.push(sample.value);
    }
    ForecastTimeSeries::univariate(timestamps, values).map_err(|e| e.into())
}

impl From<ForecastError> for TsdbError {
    fn from(err: ForecastError) -> Self {
        TsdbError::ForecastError(err.to_string())
    }
}

pub fn normalize_model_name(selected_model: &str) -> &str {
    match selected_model {
        "AutoARIMA (SARIMA)" => "SARIMA",
        "AutoARIMA" => "ARIMA",
        "AutoTheta" => "Theta",
        "AutoETS" => "ETS",
        other => other, // fallback to original name if it doesn't match known models
    }
}

pub fn try_parse_trend_criterion(s: &str) -> Result<TrendCriterion, TsdbError> {
    match s.to_lowercase().as_str() {
        "aicc" => Ok(TrendCriterion::AICc),
        "bic" => Ok(TrendCriterion::BIC),
        "holdout" => Ok(TrendCriterion::Holdout),
        other => Err(TsdbError::ForecastError(format!(
            "TSDB: Invalid trend criterion '{}'. Valid options are: AICc, BIC, and Holdout.",
            other
        ))),
    }
}

/// Infer the frequency (in milliseconds) from a set of samples by finding the most
/// common interval between consecutive timestamps.
///
/// Uses a two-step approach:
/// 1. Find the modal (most common) interval and require it to represent at least 50%
///    of all intervals.
/// 2. Compute the GCD of all intervals. If the GCD is smaller than the modal, divides
///    the modal evenly, and appears as an actual interval in the data, prefer the GCD —
///    this recovers the original frequency when samples have been deleted from a
///    uniformly-spaced series (leaving intervals that are multiples of the true frequency).
pub fn infer_frequency_from_samples(samples: &[Sample]) -> ValkeyResult<Duration> {
    if samples.len() < 2 {
        return Err(ValkeyError::String(
            "TSDB: insufficient data to infer frequency; at least 2 samples required".to_string(),
        ));
    }

    // Calculate all differences between consecutive samples
    let diffs: Vec<i64> = samples
        .windows(2)
        .map(|w| w[1].timestamp - w[0].timestamp)
        .filter(|&d| d > 0)
        .collect();

    if diffs.is_empty() {
        return Err(ValkeyError::String(
            "TSDB: cannot infer frequency; no valid intervals found".to_string(),
        ));
    }

    // Find modal (most common) difference
    let mut counts: IntMap<i64, usize> = IntMap::default();
    for &diff in &diffs {
        *counts.entry(diff).or_insert(0) += 1;
    }

    let (modal_diff, modal_count) = counts
        .iter()
        .max_by_key(|(_, count)| **count)
        .map(|(&diff, &count)| (diff, count))
        .unwrap(); // Safe because diffs is non-empty

    let total_count: usize = counts.values().sum();
    let modal_ratio = modal_count as f64 / total_count as f64;

    // Require at least 50% of intervals to match the modal
    if modal_ratio < 0.5 {
        return Err(ValkeyError::String(
            "TSDB: cannot infer frequency; no dominant interval found".to_string(),
        ));
    }

    // Compute GCD of all intervals to detect the underlying base frequency.
    // This handles cases where samples were deleted from a uniformly-spaced series,
    // leaving only intervals that are multiples of the true frequency.
    let gcd_all = diffs.iter().fold(0i64, |a, &b| gcd(a, b));

    // Use GCD if it is a proper divisor of the modal AND appears as an actual
    // interval in the data (prevents picking GCD=1 due to noise).
    if gcd_all > 0
        && gcd_all < modal_diff
        && modal_diff % gcd_all == 0
        && counts.contains_key(&gcd_all)
    {
        return Ok(Duration::from_millis(gcd_all as u64));
    }

    Ok(Duration::from_millis(modal_diff as u64))
}

/// Compute the greatest common divisor of two non-negative integers.
fn gcd(mut a: i64, mut b: i64) -> i64 {
    while b != 0 {
        (a, b) = (b, a % b);
    }
    a
}
