//! Data preparation utilities for time series forecasting.
//! This module includes:
//! - TimeSeries struct for representing time series data with timestamps and values.
//! - Methods for handling missing values (NaN/Inf) with various policies.
//! - Frequency inference from timestamps, including calendar-aware inference.
//! - Gap filling to create regular time series from irregular data.
//! - Seasonal and trend strength estimation via STL decomposition.
//! - Outlier detection and replacement with local median.
use chrono::{DateTime, Datelike, Duration, Utc};
use smallvec::SmallVec;
use std::collections::{HashMap, BTreeMap};
use anofox_forecast::core::{CalendarAnnotations, Frequency, MissingValuePolicy};
use anofox_forecast::detection::{detect_outliers, OutlierConfig};
use anofox_forecast::ForecastError;
use anofox_forecast::seasonality::STL;
use anofox_forecast::utils::stats::{nan_mean, nan_median};
use simd_json::prelude::{ArrayTrait, IndexedMut};
use crate::common::Sample;

/// A time series with timestamps and values.
#[derive(Debug, Clone)]
pub struct TimeSeries {
    timestamps: Vec<DateTime<Utc>>,
    /// Values stored in observation order.
    values: Vec<f64>,
    frequency: Option<Duration>,
    calendar: Option<CalendarAnnotations>,
    delta: BTreeMap<DateTime<Utc>, Sample>,
}

impl TimeSeries {
    /// Create a new TimeSeries with full configuration.
    pub fn new(
        timestamps: Vec<DateTime<Utc>>,
        values: Vec<f64>,
        frequency: Option<Duration>,
        calendar: Option<CalendarAnnotations>,
    ) -> Result<Self, ForecastError> {
        // Validate timestamps are strictly increasing
        for i in 1..timestamps.len() {
            if timestamps[i] <= timestamps[i - 1] {
                return Err(ForecastError::TimestampError(
                    "timestamps must be strictly increasing".to_string(),
                ));
            }
        }

        if timestamps.len() != values.len() {
            return Err(ForecastError::InvalidParameter(
                "timestamps and values must have the same length".to_string(),
            ));
        }

        let mut delta = BTreeMap::new();
        for i in 1..timestamps.len() {
            let ts = timestamps[i];
            let sample = Sample {
                timestamp: ts.timestamp(),
                value: values[i],
            };
            delta.insert(ts, sample);
        }
        Ok(Self {
            timestamps,
            values,
            frequency,
            calendar,
            delta,
        })
    }

    /// Create a simple time series.
    pub fn univariate(timestamps: Vec<DateTime<Utc>>, values: Vec<f64>) -> Result<TimeSeries, ForecastError> {
        Self::new(
            timestamps,
            values,
            None,
            None,
        )
    }

    /// Get the number of observations.
    pub fn len(&self) -> usize {
        self.timestamps.len()
    }

    /// Check if the series is empty.
    pub fn is_empty(&self) -> bool {
        self.timestamps.is_empty()
    }

    /// Get timestamps.
    pub fn timestamps(&self) -> &[DateTime<Utc>] {
        &self.timestamps
    }

    /// Get values.
    pub fn values(&self) -> &[f64] {
        &self.values
    }

    pub fn frequency(&self) -> &Option<Duration> {
        &self.frequency
    }

    pub fn set_frequency(&mut self, frequency: Duration) {
        self.frequency = Some(frequency);
    }

    pub fn calendar(mut self, calendar: CalendarAnnotations) {
        self.calendar = Some(calendar);
    }

    pub(crate) fn clear_frequency(&mut self) {
        self.frequency = None;
    }

    pub fn set_calendar(&mut self, calendar: CalendarAnnotations) {
        self.calendar = Some(calendar);
    }

    pub fn is_holiday(&self, timestamp: &DateTime<Utc>) -> bool {
        if let Some(ref cal) = self.calendar {
            return cal.is_holiday(timestamp);
        }
        false
    }

    pub fn is_business_day(&self, timestamp: &DateTime<Utc>) -> bool {
        let weekday = timestamp.weekday();
        !matches!(weekday, chrono::Weekday::Sat | chrono::Weekday::Sun)
            && !self.is_holiday(timestamp)
    }

    /// Check if series has missing values (NaN or Inf).
    pub fn has_missing_values(&self) -> bool {
        self.values
            .iter()
            .any(|v| !v.is_finite())
    }

    /// Return a sanitized copy with missing values handled.
    pub fn sanitize(&mut self, policy: MissingValuePolicy) -> Result<(), ForecastError> {
        match policy {
            MissingValuePolicy::Error => {
                if self.has_missing_values() {
                    return Err(ForecastError::MissingValues);
                }
            }
            MissingValuePolicy::Drop => {
                // Find indices of valid observations
                let mut dropped_indices: SmallVec<usize, 32> = SmallVec::new();
                for i in 0..self.len() {
                    if self.values[i].is_finite() {
                        continue;
                    }
                    dropped_indices.push(i);
                }
                self.values.retain_mut(|v| v.is_finite());
                for i in dropped_indices.iter().rev() {
                    self.timestamps.remove(*i);
                }
            }
            MissingValuePolicy::Fill(fill_value) => {
                self
                    .values
                    .iter_mut()
                    .zip(self.timestamps.iter())
                    .filter(|(v, _)| !v.is_finite())
                    .for_each(|(v, &timestamp)| {
                        *v = fill_value;
                        self.delta.insert(timestamp, Sample {
                            timestamp: timestamp.timestamp(),
                            value: *v,
                        });
                    });
            }
            MissingValuePolicy::ForwardFill => {
                let mut last_valid = None;
                for (v, &timestamp) in self.values.iter_mut().zip(self.timestamps.iter()) {
                    if !v.is_finite() {
                        let value_to_use = last_valid.unwrap_or(*v);
                        *v = value_to_use;
                        self.delta.insert(timestamp, Sample {
                            timestamp: timestamp.timestamp(),
                            value: *v,
                        });
                    } else {
                        last_valid = Some(*v);
                    }
                }
            }
            MissingValuePolicy::BackwardFill => {
                let mut next_valid = None;
                for i in (0..self.values.len()).rev() {
                    let val = self.values.get_mut(i).unwrap();
                    if !val.is_finite() {
                        if let Some(v) = next_valid {
                            *val = v;
                            let timestamp = self.timestamps[i];
                            self.delta.insert(timestamp, Sample {
                                timestamp: timestamp.timestamp(),
                                value: *val,
                            });
                        }
                    } else {
                        next_valid = Some(*val);
                    }
                }
            }
            MissingValuePolicy::FillMean => {
                let m = nan_mean(&self.values);
                self
                    .timestamps.iter()
                    .zip(self.values.iter_mut())
                    .for_each(|(i, v)|
                        if !v.is_finite() {
                            *v = m;
                            self.delta.insert(*i, Sample {
                                timestamp: i.timestamp(),
                                value: *v,
                            });
                        }
                    )
            }
            MissingValuePolicy::FillMedian => {
                let med = nan_median(&self.values);
                self
                    .timestamps.iter()
                    .zip(self.values.iter_mut())
                    .for_each(|(i, v)|
                        if !v.is_finite() {
                            *v = med;
                            self.delta.insert(*i, Sample {
                                timestamp: i.timestamp(),
                                value: *v,
                            });
                        }
                    )
            }
            MissingValuePolicy::Interpolate => self.interpolate(true),
        }
        Ok(())
    }

    /// Return a copy with linear interpolation for NaN values.
    pub fn interpolate(&mut self, fill_edges: bool) {
        self.values = interpolate_series(&self.values, fill_edges);
    }

    /// Returns a boolean mask: true where value is NaN or Inf.
    pub fn missing_mask(&self) -> Vec<bool> {
        self.values.iter().map(|v| !v.is_finite()).collect()
    }

    /// Count of missing values.
    pub fn missing_count(&self) -> usize {
        self.values.iter().filter(|v| v.is_finite()).count()
    }

    /// Forward-fill then backward-fill — handles both leading and trailing NaNs.
    pub fn impute_forward_backward(&mut self) -> Result<(), ForecastError> {
        // Forward fill
        let mut last_valid = None;
        for (i, v) in self.values.iter_mut().enumerate() {
            let value = *v;
            if !v.is_finite() {
                let timestamp = self.timestamps[i];
                let value_to_use = last_valid.unwrap_or(value);
                *v = value_to_use;
                self.delta.insert(timestamp, Sample {
                    timestamp: timestamp.timestamp(),
                    value: value_to_use,
                });
            } else {
                last_valid = Some(value);
            }
        }
        // Backward fill remaining (leading NaNs)
        let mut next_valid = None;
        for i in (0..self.values.len()).rev() {
            let value = self.values.get_mut(i).unwrap();
            if !value.is_finite() {
                if let Some(v) = next_valid {
                    let ts = self.timestamps[i];
                    let timestamp = ts.timestamp();
                    *value = v;
                    self.delta.insert(ts, Sample {
                        timestamp,
                        value: *value,
                    });
                }
            } else {
                next_valid = Some(*value);
            }
        }
        Ok(())
    }

    /// Impute NaN using mean of valid values in a centered window.
    ///
    /// Window must be odd. Multi-pass (up to 3) for adjacent NaNs.
    /// Remaining NaNs filled with global mean.
    pub fn impute_moving_average(&mut self, window: usize) -> Result<(), ForecastError> {
        if window == 0 || window.is_multiple_of(2) {
            return Err(ForecastError::InvalidParameter(
                "moving average window must be odd and > 0".to_string(),
            ));
        }

        let half = window / 2;
        let mut result = self.values.clone();
        let n = result.len();

        // Multi-pass: up to 3 passes to handle adjacent NaNs
        for _ in 0..3 {
            let mut changed = false;
            let snapshot = result.clone();
            for i in 0..n {
                if snapshot[i].is_finite() {
                    continue;
                }
                let start = i.saturating_sub(half);
                let end = (i + half + 1).min(n);
                let mut sum = 0.0;
                let mut count = 0usize;
                for (j, value) in snapshot.iter().enumerate().take(end).skip(start) {
                    if j != i && value.is_finite() {
                        sum += *value;
                        count += 1;
                    }
                }
                if count > 0 {
                    result[i] = sum / count as f64;
                    changed = true;
                }
            }
            if !changed {
                break;
            }
        }

        // Fill any remaining NaNs with global mean
        let global_mean = nan_mean(&result);
        for v in &mut result {
            if !v.is_finite() {
                *v = global_mean;
            }
        }
        self.values = result;
        Ok(())
    }

    /// Impute NaN using the median of observed values at the same seasonal position.
    ///
    /// Groups values by (index % period), computes median per group, fills NaN.
    /// Returns error if period is 0 or if >50% of values in any seasonal bucket are NaN.
    pub fn impute_seasonal(&mut self, period: usize) -> Result<(), ForecastError> {
        if period == 0 {
            return Err(ForecastError::InvalidParameter(
                "seasonal period must be > 0".to_string(),
            ));
        }
        if self.len() < period {
            return Err(ForecastError::InsufficientData {
                needed: period,
                got: self.len(),
                hint: None,
            });
        }

        let n = self.values.len();

        // Collect finite values per seasonal bucket
        let mut buckets: Vec<Vec<f64>> = vec![Vec::new(); period];
        for (i, &v) in self.values.iter().enumerate() {
            if v.is_finite() {
                buckets[i % period].push(v);
            }
        }

        // Compute median per bucket
        let medians: Vec<f64> = buckets
            .iter()
            .map(|b| {
                if b.is_empty() {
                    f64::NAN
                } else {
                    let mut sorted = b.clone();
                    sorted.sort_by(|a, b| {
                        a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal)
                    });
                    let len = sorted.len();
                    if len % 2 == 0 {
                        (sorted[len / 2 - 1] + sorted[len / 2]) / 2.0
                    } else {
                        sorted[len / 2]
                    }
                }
            })
            .collect();

        // Fill NaN with seasonal median
        let mut result = self.values.clone();
        for i in 0..n {
            if !result[i].is_finite() {
                result[i] = medians[i % period];
            }
        }

        // Validate: check that no seasonal bucket had >50% NaN
        let mut bucket_total: Vec<usize> = vec![0; period];
        let mut bucket_missing: Vec<usize> = vec![0; period];
        for (i, &v) in self.values.iter().enumerate() { 
            bucket_total[i % period] += 1;
            if !v.is_finite() {
                bucket_missing[i % period] += 1;
            }
        }
        for (b, (&total, &missing)) in
            bucket_total.iter().zip(bucket_missing.iter()).enumerate()
        {
            if total > 0 && missing as f64 / total as f64 > 0.5 {
                return Err(ForecastError::InvalidParameter(format!(
                    "seasonal bucket {} has >50% missing values ({}/{})",
                    b, missing, total
                )));
            }
        }
        self.values = result;
        Ok(())
    }

    /// Impute NaN values in all regressors using the given policy.
    ///
    /// Applies the policy to each regressor vector independently.
    /// Only `Fill`, `ForwardFill`, `BackwardFill`, `FillMean`, `FillMedian`,
    /// and `Interpolate` are supported. `Drop` and `Error` return an error.
    pub fn with_imputed_regressors(&self, policy: MissingValuePolicy) -> Result<TimeSeries, ForecastError> {
        match policy {
            MissingValuePolicy::Drop | MissingValuePolicy::Error => {
                return Err(ForecastError::InvalidParameter(
                    "Drop and Error policies are not supported for regressor imputation"
                        .to_string(),
                ));
            }
            _ => {}
        }

        let mut result = self.clone();
        if let Some(ref mut cal) = result.calendar {
            let mut imputed_regressors = HashMap::new();
            for (name, values) in cal.regressors() {
                let imputed = match policy {
                    MissingValuePolicy::Fill(fill_value) => values
                        .iter()
                        .map(|&v| {
                            if !v.is_finite() {
                                fill_value
                            } else {
                                v
                            }
                        })
                        .collect(),
                    MissingValuePolicy::ForwardFill => {
                        let mut res = Vec::with_capacity(values.len());
                        let mut last_valid = None;
                        for &v in values {
                            if !v.is_finite() {
                                res.push(last_valid.unwrap_or(v));
                            } else {
                                last_valid = Some(v);
                                res.push(v);
                            }
                        }
                        res
                    }
                    MissingValuePolicy::BackwardFill => {
                        let mut res = values.to_vec();
                        let mut next_valid = None;
                        for i in (0..res.len()).rev() {
                            if !res[i].is_finite() {
                                if let Some(v) = next_valid {
                                    res[i] = v;
                                }
                            } else {
                                next_valid = Some(res[i]);
                            }
                        }
                        res
                    }
                    MissingValuePolicy::FillMean => {
                        let m = nan_mean(values);
                        values
                            .iter()
                            .map(|&v| if !v.is_finite() { m } else { v })
                            .collect()
                    }
                    MissingValuePolicy::FillMedian => {
                        let med = nan_median(values);
                        values
                            .iter()
                            .map(|&v| {
                                if !v.is_finite() {
                                    med
                                } else {
                                    v
                                }
                            })
                            .collect()
                    }
                    MissingValuePolicy::Interpolate => interpolate_series(values, true),
                    MissingValuePolicy::Drop | MissingValuePolicy::Error => {
                        unreachable!()
                    }
                };
                imputed_regressors.insert(name.clone(), imputed);
            }
            // Replace regressors in calendar
            let mut new_cal = CalendarAnnotations::new().with_holidays(cal.holidays().to_vec());
            for (name, values) in imputed_regressors {
                new_cal = new_cal.with_regressor(name, values);
            }
            result.calendar = Some(new_cal);
        }
        Ok(result)
    }

    /// Infer frequency from timestamps.
    pub fn infer_frequency(&self, tolerance: f64) -> Result<Duration, ForecastError> {
        if self.len() < 2 {
            return Err(ForecastError::InsufficientData {
                needed: 2,
                got: self.len(),
                hint: None,
            });
        }

        // Calculate all differences
        let diffs: Vec<i64> = self
            .timestamps
            .windows(2)
            .map(|w| (w[1] - w[0]).num_seconds())
            .collect();

        // Find modal (most common) difference
        let mut counts: HashMap<i64, usize> = HashMap::new();
        for &diff in &diffs {
            *counts.entry(diff).or_insert(0) += 1;
        }

        let (modal_diff, modal_count) = counts
            .iter()
            .max_by_key(|(_, count)| **count)
            .map(|(&diff, &count)| (diff, count))
            .ok_or(ForecastError::FrequencyInference(
                "empty spacing data".to_string(),
            ))?;

        // Check if modal is unique enough
        let total_count: usize = counts.values().sum();
        let modal_ratio = modal_count as f64 / total_count as f64;

        if modal_ratio < tolerance {
            return Err(ForecastError::FrequencyInference(
                "no unique modal spacing found".to_string(),
            ));
        }

        Ok(Duration::seconds(modal_diff))
    }

    /// Infer frequency respecting business day calendar.
    pub fn infer_frequency_calendar(&self, tolerance: f64) -> Result<Duration, ForecastError> {
        if self.len() < 2 {
            return Err(ForecastError::InsufficientData {
                needed: 2,
                got: self.len(),
                hint: None,
            });
        }

        // Filter to business days only if calendar is present
        let business_timestamps: Vec<&DateTime<Utc>> = if self.calendar.is_some() {
            self.timestamps
                .iter()
                .filter(|t| self.is_business_day(t))
                .collect()
        } else {
            self.timestamps.iter().collect()
        };

        if business_timestamps.len() < 2 {
            return Err(ForecastError::InsufficientData {
                needed: 2,
                got: business_timestamps.len(),
                hint: None,
            });
        }

        // Calculate differences between consecutive business days
        let diffs: Vec<i64> = business_timestamps
            .windows(2)
            .map(|w| (*w[1] - *w[0]).num_seconds())
            .collect();

        let mut counts: HashMap<i64, usize> = HashMap::new();
        for &diff in &diffs {
            *counts.entry(diff).or_insert(0) += 1;
        }

        let (modal_diff, modal_count) = counts
            .iter()
            .max_by_key(|(_, count)| **count)
            .map(|(&diff, &count)| (diff, count))
            .ok_or(ForecastError::FrequencyInference(
                "empty spacing data".to_string(),
            ))?;

        let total_count: usize = counts.values().sum();
        let modal_ratio = modal_count as f64 / total_count as f64;

        if modal_ratio < tolerance {
            return Err(ForecastError::FrequencyInference(
                "no unique modal spacing found".to_string(),
            ));
        }

        Ok(Duration::seconds(modal_diff))
    }

    /// Generate future timestamps for a forecast horizon.
    ///
    /// Infers the frequency from existing timestamps, then extrapolates forward.
    /// Uses calendar-aware arithmetic for monthly/quarterly/yearly data.
    pub fn future_timestamps(&self, horizon: usize) -> Result<Vec<DateTime<Utc>>, ForecastError> {
        if self.timestamps.is_empty() {
            return Err(ForecastError::EmptyData);
        }
        let last = *self.timestamps.last().unwrap();

        // Try to detect monthly/quarterly/yearly from median spacing
        let freq_duration = self
            .infer_frequency_calendar(0.5)
            .or_else(|_| self.infer_frequency(0.5))?;

        let secs = freq_duration.num_seconds();

        // Classify: ~30 days = monthly, ~91 days = quarterly, ~365 days = yearly
        let frequency = if (27 * 86400..=33 * 86400).contains(&secs) {
            Frequency::Months(1)
        } else if (88 * 86400..=95 * 86400).contains(&secs) {
            Frequency::Months(3)
        } else if (360 * 86400..=370 * 86400).contains(&secs) {
            Frequency::Years(1)
        } else {
            Frequency::Duration(freq_duration)
        };

        Ok(generate_future_timestamps(&last, &frequency, horizon))
    }

    /// Set frequency from timestamps (auto-infer).
    pub fn set_frequency_from_timestamps(&mut self) -> Result<(), ForecastError> {
        let freq = self.infer_frequency(0.5)?;
        self.frequency = Some(freq);
        Ok(())
    }

    /// Fill missing timestamps in the time series with NULL (NaN) values.
    ///
    /// This method generates a complete sequence of timestamps based on the specified
    ///  frequency and fills in missing timestamps with NaN values. This is useful for
    /// ensuring a time series has regular intervals before analysis or forecasting.
    ///
    /// # Arguments
    ///
    /// * `frequency` - The frequency to use for gap filling. Can be:
    ///   - Polars-style string: "30m", "1h", "1d", "1w", "1mo", "1q", "1y"
    ///   - Duration: `Frequency::Duration(Duration::hours(1))`
    ///   - Months: `Frequency::Months(1)` for monthly
    ///   - Years: `Frequency::Years(1)` for yearly
    ///
    /// # Returns
    ///
    /// A new `TimeSeries` with all gaps filled with NaN values.
    ///
    /// # Examples
    ///
    /// ```
    /// use anofox_forecast::core::{TimeSeries, Frequency};
    /// use chrono::{TimeZone, Utc};
    ///
    /// // Create a time series with gaps
    /// let timestamps = vec![
    ///     Utc.with_ymd_and_hms(2024, 1, 1, 0, 0, 0).unwrap(),
    ///     Utc.with_ymd_and_hms(2024, 1, 1, 1, 0, 0).unwrap(),
    ///     // Gap at 2:00
    ///     Utc.with_ymd_and_hms(2024, 1, 1, 3, 0, 0).unwrap(),
    /// ];
    /// let values = vec![1.0, 2.0, 4.0];
    ///
    /// let ts = TimeSeries::univariate(timestamps, values).unwrap();
    /// let filled = ts.fill_gaps(Frequency::parse("1h").unwrap()).unwrap();
    ///
    /// assert_eq!(filled.len(), 4); // Now includes 2:00
    /// ```
    pub fn fill_gaps(&mut self, frequency: Frequency) -> Result<(), ForecastError> {
        if self.len() <= 1 {
            return Ok(());
        }

        let start = self.timestamps[0];
        // SAFETY: `self.is_empty()` and `self.len() == 1` are handled above, so timestamps has >= 2 elements.
        let end = *self.timestamps.last().unwrap();

        // Generate the expected timestamps based on frequency
        let expected_timestamps = generate_timestamps(start, end, &frequency)?;

        if expected_timestamps.is_empty() {
            return Ok(());
        }

        // Build a map from existing timestamps to their indices
        let existing: HashMap<DateTime<Utc>, usize> = self
            .timestamps
            .iter()
            .enumerate()
            .map(|(i, t)| (*t, i))
            .collect();

        let mut new_values = Vec::with_capacity(expected_timestamps.len());

        // Create new timestamps and values
        self.timestamps.clear();

        for ts in expected_timestamps {
            self.timestamps.push(ts);
            if let Some(&idx) = existing.get(&ts) {
                new_values.push(self.values[idx]);
            } else {
                new_values.push(f64::NAN);
            }
        }

        self.values = new_values;

        self.frequency = match &frequency {
            Frequency::Duration(d) => Some(*d),
            _ => self.frequency,
        };

        Ok(())
    }

    /// Fill gaps using a Polars-style frequency string.
    ///
    /// This is a convenience method that parses the frequency string and calls `fill_gaps`.
    ///
    /// # Arguments
    ///
    /// * `frequency` - A Polars-style frequency string like "30m", "1h", "1d", etc.
    ///
    /// # Examples
    ///
    /// ```
    /// use anofox_forecast::core::TimeSeries;
    /// use chrono::{TimeZone, Utc};
    ///
    /// let timestamps = vec![
    ///     Utc.with_ymd_and_hms(2024, 1, 1, 0, 0, 0).unwrap(),
    ///     Utc.with_ymd_and_hms(2024, 1, 1, 2, 0, 0).unwrap(), // Gap at 1:00
    /// ];
    /// let values = vec![1.0, 3.0];
    ///
    /// let ts = TimeSeries::univariate(timestamps, values).unwrap();
    /// let filled = ts.fill_gaps_str("1h").unwrap();
    ///
    /// assert_eq!(filled.len(), 3);
    /// ```
    pub fn fill_gaps_str(&mut self, frequency: &str) -> Result<(), ForecastError> {
        let freq = Frequency::parse(frequency)?;
        self.fill_gaps(freq)
    }

    /// Compute the seasonal strength via STL decomposition.
    ///
    /// Returns a value between 0 and 1, where values close to 1 indicate
    /// strong seasonality. Requires `period >= 2` and series length `>= 2 * period`.
    ///
    /// This is a convenience wrapper around STL decomposition — if you need both
    /// seasonal and trend strength, call [`STL::decompose`](crate::seasonality::STL)
    /// once and use `STLResult::seasonal_strength()` / `STLResult::trend_strength()`.
    pub fn seasonal_strength(&self, period: usize) -> Result<f64, ForecastError> {
        if period < 2 {
            return Err(ForecastError::InvalidParameter(
                "period must be at least 2".into(),
            ));
        }
        let vals = self.values();
        if vals.len() < 2 * period {
            return Err(ForecastError::InsufficientData {
                needed: 2 * period,
                got: vals.len(),
                hint: Some("need at least 2 full seasonal cycles".into()),
            });
        }
        let stl = STL::new(period);
        let result = stl.decompose(vals).ok_or(ForecastError::ComputationError(
            "STL decomposition failed".into(),
        ))?;
        Ok(result.seasonal_strength())
    }

    /// Compute the trend strength via STL decomposition.
    ///
    /// Returns a value between 0 and 1, where values close to 1 indicate
    /// a strong trend component. Requires `period >= 2` and series length `>= 2 * period`.
    pub fn trend_strength(&self, period: usize) -> Result<f64, ForecastError> {
        if period < 2 {
            return Err(ForecastError::InvalidParameter(
                "period must be at least 2".into(),
            ));
        }
        let vals = self.values();
        if vals.len() < 2 * period {
            return Err(ForecastError::InsufficientData {
                needed: 2 * period,
                got: vals.len(),
                hint: Some("need at least 2 full seasonal cycles".into()),
            });
        }
        let stl = STL::new(period);
        let result = stl.decompose(vals).ok_or(ForecastError::ComputationError(
            "STL decomposition failed".into(),
        ))?;
        Ok(result.trend_strength())
    }

    /// Detect outliers and return a sanitized copy.
    ///
    /// Outlier values are replaced with the local median within a window of
    /// `window_size` around each outlier. The original series is unchanged.
    ///
    /// Uses the specified [`OutlierConfig`](crate::detection::OutlierConfig)
    /// for detection (IQR, Z-score, or Modified Z-score).
    ///
    /// # Example
    ///
    /// ```
    /// use anofox_forecast::core::TimeSeries;
    /// use anofox_forecast::detection::OutlierConfig;
    /// use chrono::{TimeZone, Utc};
    ///
    /// let timestamps: Vec<_> = (0..50)
    ///     .map(|i| Utc.with_ymd_and_hms(2024, 1, 1, 0, 0, 0).unwrap()
    ///         + chrono::Duration::hours(i))
    ///     .collect();
    /// let mut values: Vec<f64> = (0..50).map(|i| 10.0 + 0.1 * i as f64).collect();
    /// values[25] = 1000.0; // inject outlier
    ///
    /// let ts = TimeSeries::univariate(timestamps, values).unwrap();
    /// let clean = ts.with_outliers_replaced(&OutlierConfig::default(), 5).unwrap();
    ///
    /// // The outlier at index 25 has been replaced
    /// assert!((clean.values()[25] - 1000.0).abs() > 1.0);
    /// ```
    pub fn with_outliers_replaced(
        &mut self,
        config: &OutlierConfig,
        window_size: usize,
    ) -> Result<(), ForecastError> {
        let outlier_result = detect_outliers(&self.values, config);

        if outlier_result.outlier_indices.is_empty() {
            return Ok(());
        }

        let n = self.values.len();
        let half = window_size / 2;

        for &idx in &outlier_result.outlier_indices {
            // Collect non-outlier neighbors within the window
            let start = idx.saturating_sub(half);
            let end = (idx + half + 1).min(n);

            let mut neighbors: Vec<f64> = (start..end)
                .filter(|&i| i != idx && !outlier_result.outlier_indices.contains(&i))
                .map(|i| self.values[i])
                .filter(|v| v.is_finite())
                .collect();

            if neighbors.is_empty() {
                // Fallback: use overall median
                let mut all: Vec<f64> = self.values.iter().copied().filter(|v| v.is_finite()).collect();
                all.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
                if !all.is_empty() {
                    self.values[idx] = all[all.len() / 2];
                }
            } else {
                neighbors.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
                self.values[idx] = neighbors[neighbors.len() / 2];
            }
        }
        Ok(())
    }
}


/// Generate a sequence of timestamps from start to end (inclusive) with the given frequency.
fn generate_timestamps(
    start: DateTime<Utc>,
    end: DateTime<Utc>,
    frequency: &Frequency,
) -> Result<Vec<DateTime<Utc>>, ForecastError> {
    validate_frequency_positive(frequency)?;

    let mut timestamps = Vec::new();
    let mut current = start;
    while current <= end {
        timestamps.push(current);
        current = advance_timestamp(current, frequency);
    }

    Ok(timestamps)
}

/// Validate that a frequency value is positive.
#[inline]
fn validate_frequency_positive(frequency: &Frequency) -> Result<(), ForecastError> {
    let valid = match frequency {
        Frequency::Duration(d) => d.num_seconds() > 0,
        Frequency::Months(m) => *m > 0,
        Frequency::Years(y) => *y > 0,
    };
    if valid {
        Ok(())
    } else {
        Err(ForecastError::InvalidParameter(
            "frequency must be positive".to_string(),
        ))
    }
}

/// Generate future timestamps by advancing from a starting point.
///
/// Uses calendar-aware arithmetic for monthly/quarterly/yearly frequencies
/// (e.g., Jan 31 + 1 month = Feb 28/29, not Mar 02).
///
/// # Arguments
/// * `last` - The last known timestamp
/// * `frequency` - The step frequency
/// * `horizon` - Number of future timestamps to generate
///
/// # Example
/// ```
/// use anofox_forecast::core::time_series::{generate_future_timestamps, Frequency};
/// use chrono::{Datelike, TimeZone, Utc};
///
/// let last = Utc.with_ymd_and_hms(2024, 1, 31, 0, 0, 0).unwrap();
/// let future = generate_future_timestamps(&last, &Frequency::Months(1), 3);
///
/// assert_eq!(future[0].day(), 29); // Feb 29 (2024 is leap year)
/// assert_eq!(future[1].month(), 3); // Mar 31
/// assert_eq!(future[2].month(), 4); // Apr 30
/// ```
pub fn generate_future_timestamps(
    last: &DateTime<Utc>,
    frequency: &Frequency,
    horizon: usize,
) -> Vec<DateTime<Utc>> {
    let mut result = Vec::with_capacity(horizon);
    let original_day = last.day();

    let mut current = *last;
    for _ in 0..horizon {
        current = match frequency {
            // For month/year frequencies, preserve the original day to prevent drift
            // (Jan 31 → Feb 29 → Mar 31, not Jan 31 → Feb 29 → Mar 29)
            Frequency::Months(m) => add_months_with_target_day(current, *m, original_day),
            Frequency::Years(y) => add_months_with_target_day(current, *y * 12, original_day),
            Frequency::Duration(d) => current + *d,
        };
        result.push(current);
    }
    result
}

/// Advance a timestamp by one frequency step.
#[inline]
fn advance_timestamp(current: DateTime<Utc>, frequency: &Frequency) -> DateTime<Utc> {
    match frequency {
        Frequency::Duration(duration) => current + *duration,
        Frequency::Months(months) => add_months(current, *months),
        Frequency::Years(years) => add_months(current, *years * 12),
    }
}

/// Add months to a DateTime, handling month-end edge cases.
///
/// Uses the *original* day (stored in the Frequency step chain via generate_future_timestamps)
/// rather than the current timestamp's day. This prevents month-end drift:
/// Jan 31 → Feb 29 → Mar 31 → Apr 30 (correct)
/// instead of Jan 31 → Feb 29 → Mar 29 → Apr 29 (drift bug).
fn add_months(dt: DateTime<Utc>, months: i32) -> DateTime<Utc> {
    add_months_with_target_day(dt, months, dt.day())
}

/// Add months with a specific target day. If the target day exceeds the
/// destination month's length, clamp to the last day of that month.
fn add_months_with_target_day(dt: DateTime<Utc>, months: i32, target_day: u32) -> DateTime<Utc> {
    use chrono::{NaiveDate, Timelike};

    let year = dt.year();
    let month = dt.month() as i32;

    let total_months = year * 12 + (month - 1) + months;
    let new_year = total_months / 12;
    let new_month = (total_months % 12 + 1) as u32;

    let max_day = days_in_month(new_year, new_month);
    let new_day = target_day.min(max_day);

    // Build the new date directly using NaiveDate to avoid issues with chrono's with_* methods
    if let Some(naive_date) = NaiveDate::from_ymd_opt(new_year, new_month, new_day) {
        naive_date
            .and_hms_opt(dt.hour(), dt.minute(), dt.second())
            .map(|naive_dt| DateTime::from_naive_utc_and_offset(naive_dt, Utc))
            .unwrap_or(dt)
    } else {
        // Fallback: just add 30 days per month as approximation
        dt + Duration::days(30 * months as i64)
    }
}

/// Get the number of days in a given month.
fn days_in_month(year: i32, month: u32) -> u32 {
    match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 => {
            if is_leap_year(year) {
                29
            } else {
                28
            }
        }
        _ => 30, // Should never happen
    }
}

/// Check if a year is a leap year.
fn is_leap_year(year: i32) -> bool {
    (year % 4 == 0 && year % 100 != 0) || (year % 400 == 0)
}

/// Public crate-internal accessor for `days_in_month`.
pub(crate) fn days_in_month_pub(year: i32, month: u32) -> u32 {
    days_in_month(year, month)
}

/// Public crate-internal accessor for `is_leap_year`.
pub(crate) fn is_leap_year_pub(year: i32) -> bool {
    is_leap_year(year)
}

/// Linear interpolation for a series with NaN values.
fn interpolate_series(values: &[f64], fill_edges: bool) -> Vec<f64> {
    if values.is_empty() {
        return vec![];
    }

    let mut result = values.to_vec();
    let n = result.len();

    // Find and fill NaN segments
    let mut i = 0;
    while i < n {
        if result[i].is_nan() {
            let start = i;
            while i < n && result[i].is_nan() {
                i += 1;
            }
            let left = if start > 0 {
                Some(result[start - 1])
            } else {
                None
            };
            let right = if i < n { Some(result[i]) } else { None };
            fill_nan_segment(&mut result[start..i], left, right, fill_edges);
        } else {
            i += 1;
        }
    }

    result
}

/// Fill a NaN segment using linear interpolation or edge values.
fn fill_nan_segment(segment: &mut [f64], left: Option<f64>, right: Option<f64>, fill_edges: bool) {
    match (left, right) {
        (Some(l), Some(r)) => {
            let segments = (segment.len() + 1) as f64;
            for (j, val) in segment.iter_mut().enumerate() {
                let t = (j + 1) as f64 / segments;
                *val = l + t * (r - l);
            }
        }
        (Some(l), None) if fill_edges => segment.fill(l),
        (None, Some(r)) if fill_edges => segment.fill(r),
        _ => {} // Leave as NaN
    }
}


#[cfg(test)]
mod tests {
    use approx::assert_relative_eq;
    use super::*;
    use chrono::TimeZone;

    fn create_timestamp(year: i32, month: u32, day: u32, hour: u32) -> i64 {
        let date = Utc.with_ymd_and_hms(year, month, day, hour, 0, 0).unwrap();
        date.timestamp()
    }

    fn make_timestamps(n: usize) -> Vec<DateTime<Utc>> {
        (0..n)
            .map(|i| Utc.with_ymd_and_hms(2024, 1, 1, i as u32, 0, 0).unwrap())
            .collect()
    }

    fn make_daily_timestamps(n: usize) -> Vec<DateTime<Utc>> {
        (0..n)
            .map(|i| {
                Utc.with_ymd_and_hms(2024, 1, 1 + i as u32, 0, 0, 0)
                    .unwrap()
            })
            .collect()
    }

    #[test]
    fn time_series_sets_frequency() {
        let timestamps = make_timestamps(3);
        let values = vec![1.0, 2.0, 3.0];

        let mut ts = TimeSeries::univariate(timestamps, values).unwrap();

        assert!(ts.frequency().is_none());

        ts.set_frequency(Duration::hours(1));
        assert_eq!(ts.frequency, Some(Duration::hours(1)));

        ts.clear_frequency();
        assert!(ts.frequency().is_none());
    }

    #[test]
    fn time_series_rejects_non_increasing_timestamps() {
        // Non-monotonic timestamps
        let timestamps = vec![
            Utc.with_ymd_and_hms(2024, 1, 1, 0, 0, 0).unwrap(),
            Utc.with_ymd_and_hms(2024, 1, 1, 2, 0, 0).unwrap(),
            Utc.with_ymd_and_hms(2024, 1, 1, 1, 0, 0).unwrap(), // goes backward
        ];
        let values = vec![1.0, 2.0, 3.0];

        let result = TimeSeries::univariate(timestamps, values);
        assert!(matches!(result, Err(ForecastError::TimestampError(_))));

        // Duplicate timestamps
        let timestamps = vec![
            Utc.with_ymd_and_hms(2024, 1, 1, 0, 0, 0).unwrap(),
            Utc.with_ymd_and_hms(2024, 1, 1, 1, 0, 0).unwrap(),
            Utc.with_ymd_and_hms(2024, 1, 1, 1, 0, 0).unwrap(), // duplicate
        ];
        let values = vec![1.0, 2.0, 3.0];

        let result = TimeSeries::univariate(timestamps, values);
        assert!(matches!(result, Err(ForecastError::TimestampError(_))));
    }

    #[test]
    fn time_series_sanitizes_missing_values() {
        let timestamps = make_timestamps(5);

        // Drop policy
        let values = vec![1.0, f64::NAN, 3.0, f64::INFINITY, 5.0];
        let mut ts = TimeSeries::univariate(timestamps.clone(), values).unwrap();
        assert!(ts.has_missing_values());
        ts.sanitize(MissingValuePolicy::Drop).unwrap();
        assert_eq!(ts.len(), 3);
        assert_eq!(ts.values(), &[1.0, 3.0, 5.0]);

        // Fill policy
        let values = vec![1.0, f64::NAN, 3.0, f64::INFINITY, 5.0];
        let mut ts = TimeSeries::univariate(timestamps.clone(), values).unwrap();
        ts.sanitize(MissingValuePolicy::Fill(0.0)).unwrap();
        assert_eq!(ts.len(), 5);
        assert_eq!(ts.values(), &[1.0, 0.0, 3.0, 0.0, 5.0]);

        // ForwardFill policy
        let values = vec![1.0, f64::NAN, 3.0, f64::INFINITY, 5.0];
        let mut ts = TimeSeries::univariate(timestamps.clone(), values).unwrap();
        ts.sanitize(MissingValuePolicy::ForwardFill).unwrap();
        assert_eq!(ts.values(), &[1.0, 1.0, 3.0, 3.0, 5.0]);

        // Error policy
        let values = vec![1.0, f64::NAN, 3.0, f64::INFINITY, 5.0];
        let mut ts = TimeSeries::univariate(timestamps, values).unwrap();
        let result = ts.sanitize(MissingValuePolicy::Error);
        assert!(matches!(result, Err(ForecastError::MissingValues)));
    }

    #[test]
    fn calendar_aware_frequency_inference_skips_weekends() {
        // Create timestamps for a week (Mon-Fri, skipping Sat-Sun)
        let timestamps = vec![
            Utc.with_ymd_and_hms(2024, 1, 1, 0, 0, 0).unwrap(), // Mon
            Utc.with_ymd_and_hms(2024, 1, 2, 0, 0, 0).unwrap(), // Tue
            Utc.with_ymd_and_hms(2024, 1, 3, 0, 0, 0).unwrap(), // Wed
            Utc.with_ymd_and_hms(2024, 1, 4, 0, 0, 0).unwrap(), // Thu
            Utc.with_ymd_and_hms(2024, 1, 5, 0, 0, 0).unwrap(), // Fri
            Utc.with_ymd_and_hms(2024, 1, 8, 0, 0, 0).unwrap(), // Mon (skip weekend)
            Utc.with_ymd_and_hms(2024, 1, 9, 0, 0, 0).unwrap(), // Tue
        ];
        let values: Vec<f64> = (0..7).map(|i| i as f64).collect();

        let mut ts = TimeSeries::univariate(timestamps, values).unwrap();
        ts.set_calendar(CalendarAnnotations::new());

        let freq = ts.infer_frequency_calendar(0.5).unwrap();
        assert_eq!(freq, Duration::days(1));
    }

    #[test]
    fn time_series_linear_interpolation_fills_gaps() {
        let timestamps = make_timestamps(5);
        let values = vec![1.0, f64::NAN, f64::NAN, 4.0, 5.0];

        let mut interpolated = TimeSeries::univariate(timestamps, values).unwrap();
        interpolated.interpolate(true);

        let result = interpolated.values();
        assert_relative_eq!(result[0], 1.0, epsilon = 1e-10);
        assert_relative_eq!(result[1], 2.0, epsilon = 1e-10);
        assert_relative_eq!(result[2], 3.0, epsilon = 1e-10);
        assert_relative_eq!(result[3], 4.0, epsilon = 1e-10);
        assert_relative_eq!(result[4], 5.0, epsilon = 1e-10);
    }

    #[test]
    fn time_series_interpolation_fills_edges() {
        let timestamps = make_timestamps(5);
        let values = vec![f64::NAN, f64::NAN, 3.0, 4.0, f64::NAN];

        // With edge filling
        let mut interpolated = TimeSeries::univariate(timestamps.clone(), values.clone()).unwrap();
        interpolated.interpolate(true);
        let result = interpolated.values();
        assert_relative_eq!(result[0], 3.0, epsilon = 1e-10); // Filled with first valid
        assert_relative_eq!(result[1], 3.0, epsilon = 1e-10);
        assert_relative_eq!(result[4], 4.0, epsilon = 1e-10); // Filled with last valid

        // Without edge filling — use a fresh copy so NaN edges are present
        let mut interpolated = TimeSeries::univariate(timestamps, values).unwrap();
        interpolated.interpolate(false);
        let result = interpolated.values();
        assert!(result[0].is_nan()); // Not filled
        assert!(result[4].is_nan()); // Not filled
    }

    #[test]
    fn time_series_infers_regular_frequency() {
        // Hourly data
        let timestamps = make_timestamps(10);
        let values: Vec<f64> = (0..10).map(|i| i as f64).collect();

        let ts = TimeSeries::univariate(timestamps, values).unwrap();
        let freq = ts.infer_frequency(0.5).unwrap();

        assert_eq!(freq, Duration::hours(1));
    }

    #[test]
    fn time_series_frequency_inference_requires_unique_modal_spacing() {
        // Irregular timestamps with no clear pattern
        let timestamps = vec![
            Utc.with_ymd_and_hms(2024, 1, 1, 0, 0, 0).unwrap(),
            Utc.with_ymd_and_hms(2024, 1, 1, 1, 0, 0).unwrap(), // 1 hour
            Utc.with_ymd_and_hms(2024, 1, 1, 3, 0, 0).unwrap(), // 2 hours
            Utc.with_ymd_and_hms(2024, 1, 1, 6, 0, 0).unwrap(), // 3 hours
            Utc.with_ymd_and_hms(2024, 1, 1, 10, 0, 0).unwrap(), // 4 hours
        ];
        let values = vec![1.0, 2.0, 3.0, 4.0, 5.0];

        let ts = TimeSeries::univariate(timestamps, values).unwrap();
        let result = ts.infer_frequency(0.8); // High tolerance

        assert!(matches!(result, Err(ForecastError::FrequencyInference(_))));
    }

    #[test]
    fn time_series_detects_partial_day_holiday_occurrences() {
        // Create timestamps within a single day
        let base_date = Utc.with_ymd_and_hms(2024, 12, 25, 0, 0, 0).unwrap(); // Christmas
        let timestamps: Vec<DateTime<Utc>> =
            (0..24).map(|h| base_date + Duration::hours(h)).collect();
        let values: Vec<f64> = (0..24).map(|i| i as f64).collect();

        let calendar = CalendarAnnotations::new().with_holidays(vec![base_date]);

        let mut ts = TimeSeries::univariate(timestamps.clone(), values).unwrap();
        ts.set_calendar(calendar);

        // All timestamps on Christmas day should be holidays
        for t in &timestamps {
            assert!(ts.is_holiday(t), "Expected {} to be a holiday", t);
        }

        // Non-Christmas day should not be a holiday
        let non_holiday = Utc.with_ymd_and_hms(2024, 12, 26, 12, 0, 0).unwrap();
        assert!(!ts.is_holiday(&non_holiday));
    }

    // Gap filling tests

    #[test]
    fn fill_gaps_with_hourly_frequency() {
        // Create timestamps with a gap at hour 2
        let timestamps = vec![
            Utc.with_ymd_and_hms(2024, 1, 1, 0, 0, 0).unwrap(),
            Utc.with_ymd_and_hms(2024, 1, 1, 1, 0, 0).unwrap(),
            // Gap at 2:00
            Utc.with_ymd_and_hms(2024, 1, 1, 3, 0, 0).unwrap(),
            Utc.with_ymd_and_hms(2024, 1, 1, 4, 0, 0).unwrap(),
        ];
        let values = vec![0.0, 1.0, 3.0, 4.0];

        let mut filled = TimeSeries::univariate(timestamps, values).unwrap();
        filled.fill_gaps_str("1h").unwrap();

        assert_eq!(filled.len(), 5);
        assert_eq!(
            filled.timestamps()[2],
            Utc.with_ymd_and_hms(2024, 1, 1, 2, 0, 0).unwrap()
        );

        let vals = filled.values();
        assert_relative_eq!(vals[0], 0.0);
        assert_relative_eq!(vals[1], 1.0);
        assert!(vals[2].is_nan()); // Gap filled with NaN
        assert_relative_eq!(vals[3], 3.0);
        assert_relative_eq!(vals[4], 4.0);
    }

    #[test]
    fn fill_gaps_with_daily_frequency() {
        // Create timestamps with gaps
        let timestamps = vec![
            Utc.with_ymd_and_hms(2024, 1, 1, 0, 0, 0).unwrap(),
            // Gap at Jan 2
            Utc.with_ymd_and_hms(2024, 1, 3, 0, 0, 0).unwrap(),
            // Gap at Jan 4
            Utc.with_ymd_and_hms(2024, 1, 5, 0, 0, 0).unwrap(),
        ];
        let values = vec![1.0, 3.0, 5.0];

        let mut filled = TimeSeries::univariate(timestamps, values).unwrap();
        filled.fill_gaps_str("1d").unwrap();

        assert_eq!(filled.len(), 5);

        let vals = filled.values();
        assert_relative_eq!(vals[0], 1.0);
        assert!(vals[1].is_nan()); // Jan 2 - gap
        assert_relative_eq!(vals[2], 3.0);
        assert!(vals[3].is_nan()); // Jan 4 - gap
        assert_relative_eq!(vals[4], 5.0);
    }

    #[test]
    fn fill_gaps_with_weekly_frequency() {
        let timestamps = vec![
            Utc.with_ymd_and_hms(2024, 1, 1, 0, 0, 0).unwrap(), // Week 1
            Utc.with_ymd_and_hms(2024, 1, 8, 0, 0, 0).unwrap(), // Week 2
            // Gap at Week 3 (Jan 15)
            Utc.with_ymd_and_hms(2024, 1, 22, 0, 0, 0).unwrap(), // Week 4
        ];
        let values = vec![1.0, 2.0, 4.0];

        let mut filled = TimeSeries::univariate(timestamps, values).unwrap();
        filled.fill_gaps_str("1w").unwrap();

        assert_eq!(filled.len(), 4);
        assert!(filled.values()[2].is_nan()); // Gap at week 3
    }

    #[test]
    fn fill_gaps_with_monthly_frequency() {
        let timestamps = vec![
            Utc.with_ymd_and_hms(2024, 1, 1, 0, 0, 0).unwrap(),
            // Gap at Feb
            Utc.with_ymd_and_hms(2024, 3, 1, 0, 0, 0).unwrap(),
            Utc.with_ymd_and_hms(2024, 4, 1, 0, 0, 0).unwrap(),
        ];
        let values = vec![1.0, 3.0, 4.0];

        let mut ts = TimeSeries::univariate(timestamps, values).unwrap();
        ts.fill_gaps(Frequency::Months(1)).unwrap();

        assert_eq!(ts.len(), 4);
        assert_eq!(
            ts.timestamps()[1],
            Utc.with_ymd_and_hms(2024, 2, 1, 0, 0, 0).unwrap()
        );
        assert!(ts.values()[1].is_nan()); // Feb is filled with NaN
    }

    #[test]
    fn fill_gaps_with_quarterly_frequency() {
        let timestamps = vec![
            Utc.with_ymd_and_hms(2024, 1, 1, 0, 0, 0).unwrap(), // Q1
            // Gap at Q2 (Apr)
            Utc.with_ymd_and_hms(2024, 7, 1, 0, 0, 0).unwrap(), // Q3
        ];
        let values = vec![1.0, 3.0];

        let mut filled = TimeSeries::univariate(timestamps, values).unwrap();
        filled.fill_gaps_str("1q").unwrap();

        assert_eq!(filled.len(), 3);
        assert_eq!(
            filled.timestamps()[1],
            Utc.with_ymd_and_hms(2024, 4, 1, 0, 0, 0).unwrap()
        );
        assert!(filled.values()[1].is_nan());
    }

    #[test]
    fn fill_gaps_with_yearly_frequency() {
        let timestamps = vec![
            Utc.with_ymd_and_hms(2020, 1, 1, 0, 0, 0).unwrap(),
            // Gap at 2021
            Utc.with_ymd_and_hms(2022, 1, 1, 0, 0, 0).unwrap(),
            Utc.with_ymd_and_hms(2023, 1, 1, 0, 0, 0).unwrap(),
        ];
        let values = vec![1.0, 3.0, 4.0];

        let mut filled = TimeSeries::univariate(timestamps, values).unwrap();
        filled.fill_gaps(Frequency::Years(1)).unwrap();

        assert_eq!(filled.len(), 4);
        assert_eq!(
            filled.timestamps()[1],
            Utc.with_ymd_and_hms(2021, 1, 1, 0, 0, 0).unwrap()
        );
        assert!(filled.values()[1].is_nan());
    }

    #[test]
    fn fill_gaps_with_30_minute_frequency() {
        let timestamps = vec![
            Utc.with_ymd_and_hms(2024, 1, 1, 0, 0, 0).unwrap(),
            Utc.with_ymd_and_hms(2024, 1, 1, 0, 30, 0).unwrap(),
            // Gap at 1:00
            Utc.with_ymd_and_hms(2024, 1, 1, 1, 30, 0).unwrap(),
        ];
        let values = vec![1.0, 2.0, 4.0];

        let mut filled = TimeSeries::univariate(timestamps, values).unwrap();
        filled.fill_gaps_str("30m").unwrap();

        assert_eq!(filled.len(), 4);
        assert_eq!(
            filled.timestamps()[2],
            Utc.with_ymd_and_hms(2024, 1, 1, 1, 0, 0).unwrap()
        );
        assert!(filled.values()[2].is_nan());
    }

    #[test]
    fn fill_gaps_handles_empty_series() {
        let mut filled = TimeSeries::univariate(vec![], vec![]).unwrap();
        filled.fill_gaps_str("1h").unwrap();
        assert!(filled.is_empty());
    }

    #[test]
    fn fill_gaps_handles_single_element() {
        let timestamps = vec![Utc.with_ymd_and_hms(2024, 1, 1, 0, 0, 0).unwrap()];
        let values = vec![1.0];

        let mut filled = TimeSeries::univariate(timestamps.clone(), values.clone()).unwrap();
        filled.fill_gaps_str("1h").unwrap();

        assert_eq!(filled.len(), 1);
        assert_eq!(filled.timestamps(), &timestamps);
        assert_eq!(filled.values(), &values);
    }

    #[test]
    fn fill_gaps_handles_no_gaps() {
        let timestamps = vec![
            Utc.with_ymd_and_hms(2024, 1, 1, 0, 0, 0).unwrap(),
            Utc.with_ymd_and_hms(2024, 1, 1, 1, 0, 0).unwrap(),
            Utc.with_ymd_and_hms(2024, 1, 1, 2, 0, 0).unwrap(),
        ];
        let values = vec![1.0, 2.0, 3.0];

        let mut filled = TimeSeries::univariate(timestamps.clone(), values.clone()).unwrap();
        filled.fill_gaps_str("1h").unwrap();

        assert_eq!(filled.len(), 3);
        assert_eq!(filled.timestamps(), &timestamps);
        assert_eq!(filled.values(), &values);
        assert!(!filled.has_missing_values());
    }

    #[test]
    fn fill_gaps_month_end_handling() {
        // Test that month-end dates are handled correctly
        // Using first-of-month dates to avoid month-end complications
        let timestamps = vec![
            Utc.with_ymd_and_hms(2024, 1, 1, 0, 0, 0).unwrap(),
            // Gap at Feb 1
            Utc.with_ymd_and_hms(2024, 3, 1, 0, 0, 0).unwrap(),
        ];
        let values = vec![1.0, 3.0];

        let mut filled = TimeSeries::univariate(timestamps, values).unwrap();
        filled.fill_gaps(Frequency::Months(1)).unwrap();

        assert_eq!(filled.len(), 3);
        // Feb 1 should be generated
        assert_eq!(
            filled.timestamps()[1],
            Utc.with_ymd_and_hms(2024, 2, 1, 0, 0, 0).unwrap()
        );
        assert!(filled.values()[1].is_nan());
    }

    #[test]
    fn fill_gaps_handles_end_of_month_dates() {
        // When using month-end dates, ensure they still align correctly
        // Jan 31 -> Feb 29 (leap year) -> Mar 29 (not 31!)
        let timestamps = vec![
            Utc.with_ymd_and_hms(2024, 1, 31, 0, 0, 0).unwrap(),
            Utc.with_ymd_and_hms(2024, 2, 29, 0, 0, 0).unwrap(), // Feb 29 exists in original
        ];
        let values = vec![1.0, 2.0];

        let mut filled = TimeSeries::univariate(timestamps, values).unwrap();
        filled.fill_gaps(Frequency::Months(1)).unwrap();

        // Jan 31 + 1mo = Feb 29 (clamped), which matches existing timestamp
        assert_eq!(filled.len(), 2);
        assert!(!filled.has_missing_values());
    }

    // === Missing value imputation tests ===

    #[test]
    fn backward_fill_basic() {
        let timestamps = make_timestamps(5);
        let values = vec![1.0, 2.0, 3.0, f64::NAN, f64::NAN];
        let mut result  = TimeSeries::univariate(timestamps, values).unwrap();
        result.sanitize(MissingValuePolicy::BackwardFill).unwrap();
        // Trailing NaN left as NaN (no next valid value)
        assert_relative_eq!(result.values()[0], 1.0);
        assert_relative_eq!(result.values()[1], 2.0);
        assert_relative_eq!(result.values()[2], 3.0);
        assert!(result.values()[3].is_nan());
        assert!(result.values()[4].is_nan());
    }

    #[test]
    fn backward_fill_interior() {
        let timestamps = make_timestamps(5);
        let values = vec![1.0, f64::NAN, f64::NAN, 4.0, 5.0];
        let mut result = TimeSeries::univariate(timestamps, values).unwrap();
        result.sanitize(MissingValuePolicy::BackwardFill).unwrap();
        assert_relative_eq!(result.values()[0], 1.0);
        assert_relative_eq!(result.values()[1], 4.0);
        assert_relative_eq!(result.values()[2], 4.0);
        assert_relative_eq!(result.values()[3], 4.0);
        assert_relative_eq!(result.values()[4], 5.0);
    }

    #[test]
    fn backward_fill_leading_nan() {
        let timestamps = make_timestamps(4);
        let values = vec![f64::NAN, f64::NAN, 3.0, 4.0];
        let mut result = TimeSeries::univariate(timestamps, values).unwrap();
        result.sanitize(MissingValuePolicy::BackwardFill).unwrap();
        assert_relative_eq!(result.values()[0], 3.0);
        assert_relative_eq!(result.values()[1], 3.0);
        assert_relative_eq!(result.values()[2], 3.0);
        assert_relative_eq!(result.values()[3], 4.0);
    }

    #[test]
    fn fill_mean_basic() {
        let timestamps = make_timestamps(5);
        let values = vec![1.0, f64::NAN, 3.0, f64::NAN, 5.0];
        let mut result = TimeSeries::univariate(timestamps, values).unwrap();
        result.sanitize(MissingValuePolicy::FillMean).unwrap();
        // Mean of [1, 3, 5] = 3.0
        assert_relative_eq!(result.values()[0], 1.0);
        assert_relative_eq!(result.values()[1], 3.0);
        assert_relative_eq!(result.values()[2], 3.0);
        assert_relative_eq!(result.values()[3], 3.0);
        assert_relative_eq!(result.values()[4], 5.0);
    }

    #[test]
    fn fill_median_basic() {
        let timestamps = make_timestamps(5);
        let values = vec![1.0, f64::NAN, 3.0, f64::NAN, 10.0];
        let mut result = TimeSeries::univariate(timestamps, values).unwrap();
        result.sanitize(MissingValuePolicy::FillMedian).unwrap();
        // Median of [1, 3, 10] = 3.0
        assert_relative_eq!(result.values()[0], 1.0);
        assert_relative_eq!(result.values()[1], 3.0);
        assert_relative_eq!(result.values()[2], 3.0);
        assert_relative_eq!(result.values()[3], 3.0);
        assert_relative_eq!(result.values()[4], 10.0);
    }

    #[test]
    fn fill_mean_all_nan() {
        let timestamps = make_timestamps(3);
        let values = vec![f64::NAN, f64::NAN, f64::NAN];
        let mut result = TimeSeries::univariate(timestamps, values).unwrap();
        result.sanitize(MissingValuePolicy::FillMean).unwrap();
        // All-NaN produces NaN fill
        assert!(result.values()[0].is_nan());
        assert!(result.values()[1].is_nan());
        assert!(result.values()[2].is_nan());
    }

    #[test]
    fn fill_mean_with_inf() {
        let timestamps = make_timestamps(4);
        let values = vec![2.0, f64::INFINITY, 4.0, f64::NAN];
        let mut result = TimeSeries::univariate(timestamps, values).unwrap();
        result.sanitize(MissingValuePolicy::FillMean).unwrap();
        // Mean of [2, 4] = 3.0, Inf and NaN both replaced
        assert_relative_eq!(result.values()[0], 2.0);
        assert_relative_eq!(result.values()[1], 3.0);
        assert_relative_eq!(result.values()[2], 4.0);
        assert_relative_eq!(result.values()[3], 3.0);
    }

    #[test]
    fn interpolate_policy_matches_interpolated() {
        let timestamps = make_timestamps(5);
        let values = vec![1.0, f64::NAN, f64::NAN, 4.0, 5.0];
        let mut via_policy = TimeSeries::univariate(timestamps, values).unwrap();
        let mut via_method = via_policy.clone();

        via_policy.sanitize(MissingValuePolicy::Interpolate).unwrap();
        via_method.interpolate(true);

        for (a, b) in via_policy
            .values()
            .iter()
            .zip(via_method.values().iter())
        {
            assert_relative_eq!(a, b, epsilon = 1e-10);
        }
    }

    #[test]
    fn missing_mask_correct() {
        let timestamps = make_timestamps(5);
        let values = vec![1.0, f64::NAN, 3.0, f64::INFINITY, 5.0];
        let ts = TimeSeries::univariate(timestamps, values).unwrap();
        let mask = ts.missing_mask();
        assert_eq!(mask, vec![false, true, false, true, false]);
    }

    #[test]
    fn missing_count_basic() {
        let timestamps = make_timestamps(4);
        let values = vec![1.0, f64::NAN, 3.0, f64::NAN];
        let ts = TimeSeries::univariate(timestamps, values).unwrap();
        assert_eq!(ts.missing_count(), 2);
    }

    #[test]
    fn forward_backward_handles_leading() {
        let timestamps = make_timestamps(5);
        let values = vec![f64::NAN, f64::NAN, 3.0, f64::NAN, 5.0];
        let mut result = TimeSeries::univariate(timestamps, values).unwrap();
        result.impute_forward_backward().unwrap();
        // Leading NaN filled backward from 3.0, interior NaN filled forward from 3.0
        assert_relative_eq!(result.values()[0], 3.0);
        assert_relative_eq!(result.values()[1], 3.0);
        assert_relative_eq!(result.values()[2], 3.0);
        assert_relative_eq!(result.values()[3], 3.0);
        assert_relative_eq!(result.values()[4], 5.0);
    }

    #[test]
    fn forward_backward_handles_trailing() {
        let timestamps = make_timestamps(4);
        let values = vec![1.0, 2.0, f64::NAN, f64::NAN];
        let mut result = TimeSeries::univariate(timestamps, values).unwrap();
        result.impute_forward_backward().unwrap();
        assert_relative_eq!(result.values()[0], 1.0);
        assert_relative_eq!(result.values()[1], 2.0);
        // Trailing NaN forward-filled from 2.0
        assert_relative_eq!(result.values()[2], 2.0);
        assert_relative_eq!(result.values()[3], 2.0);
    }

    #[test]
    fn moving_average_single_gap() {
        let timestamps = make_timestamps(5);
        let values = vec![1.0, 2.0, f64::NAN, 4.0, 5.0];
        let mut result = TimeSeries::univariate(timestamps, values).unwrap();
        result.impute_moving_average(3).unwrap();
        // Window of 3: neighbors are 2.0 and 4.0 → mean = 3.0
        assert_relative_eq!(result.values()[2], 3.0, epsilon = 1e-10);
    }

    #[test]
    fn moving_average_adjacent_gaps() {
        let timestamps = make_timestamps(6);
        let values = vec![1.0, f64::NAN, f64::NAN, 4.0, 5.0, 6.0];
        let mut result = TimeSeries::univariate(timestamps, values).unwrap();
        result.impute_moving_average(3).unwrap();
        // After multi-pass, gaps should be filled
        assert!(result.values[1].is_finite());
        assert!(result.values[2].is_finite());
    }

    #[test]
    fn moving_average_rejects_even_window() {
        let timestamps = make_timestamps(5);
        let values = vec![1.0, 2.0, 3.0, 4.0, 5.0];
        let mut ts = TimeSeries::univariate(timestamps, values).unwrap();
        assert!(ts.impute_moving_average(4).is_err());
    }

    #[test]
    fn moving_average_rejects_zero_window() {
        let timestamps = make_timestamps(3);
        let values = vec![1.0, 2.0, 3.0];
        let mut ts = TimeSeries::univariate(timestamps, values).unwrap();
        assert!(ts.impute_moving_average(0).is_err());
    }

    #[test]
    fn seasonal_imputation_basic() {
        // Period 3: positions 0,1,2,0,1,2,0,1,2
        let timestamps = make_timestamps(9);
        let values = vec![
            10.0,
            20.0,
            30.0, // cycle 1
            11.0,
            21.0,
            31.0, // cycle 2
            f64::NAN,
            22.0,
            32.0, // cycle 3 - NaN at position 0
        ];
        let mut result = TimeSeries::univariate(timestamps, values).unwrap();
        result.impute_seasonal(3).unwrap();
        // Position 0 values: [10.0, 11.0], median = 10.5
        assert_relative_eq!(result.values()[6], 10.5, epsilon = 1e-10);
    }

    #[test]
    fn seasonal_imputation_insufficient_data() {
        let timestamps = make_timestamps(3);
        let values = vec![1.0, f64::NAN, 3.0];
        let mut ts = TimeSeries::univariate(timestamps, values).unwrap();
        // Period 4 but only 3 data points
        assert!(ts.impute_seasonal(4).is_err());
    }

    #[test]
    fn seasonal_imputation_rejects_zero_period() {
        let timestamps = make_timestamps(5);
        let values = vec![1.0, 2.0, 3.0, 4.0, 5.0];
        let mut ts = TimeSeries::univariate(timestamps, values).unwrap();
        assert!(ts.impute_seasonal(0).is_err());
    }

    #[test]
    fn seasonal_imputation_rejects_too_many_missing() {
        // Period 2: bucket 0 has indices [0, 2, 4], bucket 1 has [1, 3, 5]
        let timestamps = make_timestamps(6);
        let values = vec![f64::NAN, 1.0, f64::NAN, 2.0, f64::NAN, 3.0];
        let mut ts = TimeSeries::univariate(timestamps, values).unwrap();
        // Bucket 0 is 100% NaN (3/3)
        assert!(ts.impute_seasonal(2).is_err());
    }

    #[test]
    fn with_imputed_regressors_rejects_drop() {
        let timestamps = make_daily_timestamps(3);
        let values = vec![1.0, 2.0, 3.0];
        let calendar =
            CalendarAnnotations::new().with_regressor("x".to_string(), vec![1.0, f64::NAN, 3.0]);

        let mut ts = TimeSeries::univariate(timestamps, values).unwrap();
        ts.set_calendar(calendar);

        assert!(ts
            .with_imputed_regressors(MissingValuePolicy::Drop)
            .is_err());
    }

    // ---------------------------------------------------------------
    // generate_future_timestamps & TimeSeries::future_timestamps tests
    // ---------------------------------------------------------------

    /// Helper: build a UTC datetime from date components.
    fn ymd(year: i32, month: u32, day: u32) -> DateTime<Utc> {
        Utc.with_ymd_and_hms(year, month, day, 0, 0, 0).unwrap()
    }

    /// Helper: build a UTC datetime from date + time components.
    fn ymdhms(year: i32, month: u32, day: u32, h: u32, m: u32, s: u32) -> DateTime<Utc> {
        Utc.with_ymd_and_hms(year, month, day, h, m, s).unwrap()
    }

    // --- 1. Monthly stepping from month-end dates ---

    #[test]
    fn monthly_step_jan31_clamps_to_feb28_in_non_leap_year() {
        // Jan 31 2023 + 1mo: original_day=31, clamped per target month
        let last = ymd(2023, 1, 31);
        let result = generate_future_timestamps(&last, &Frequency::Months(1), 3);
        assert_eq!(result.len(), 3);
        assert_eq!(result[0], ymd(2023, 2, 28)); // Feb 28 (31 clamped)
        assert_eq!(result[1], ymd(2023, 3, 31)); // Mar 31 (original_day=31 preserved)
        assert_eq!(result[2], ymd(2023, 4, 30)); // Apr 30 (31 clamped)
    }

    #[test]
    fn monthly_step_jan31_clamps_to_feb29_in_leap_year() {
        // Jan 31 2024: original_day=31, preserved across months
        let last = ymd(2024, 1, 31);
        let result = generate_future_timestamps(&last, &Frequency::Months(1), 4);
        assert_eq!(result.len(), 4);
        assert_eq!(result[0], ymd(2024, 2, 29)); // Feb 29 (31 clamped, leap)
        assert_eq!(result[1], ymd(2024, 3, 31)); // Mar 31 (original_day=31)
        assert_eq!(result[2], ymd(2024, 4, 30)); // Apr 30 (31 clamped)
        assert_eq!(result[3], ymd(2024, 5, 31)); // May 31 (original_day=31)
    }

    #[test]
    fn monthly_step_mar31_to_apr30_and_beyond() {
        // Mar 31: original_day=31, preserved
        let last = ymd(2024, 3, 31);
        let result = generate_future_timestamps(&last, &Frequency::Months(1), 3);
        assert_eq!(result[0], ymd(2024, 4, 30)); // Apr 30 (31 clamped)
        assert_eq!(result[1], ymd(2024, 5, 31)); // May 31 (original_day=31)
        assert_eq!(result[2], ymd(2024, 6, 30)); // Jun 30 (31 clamped)
    }

    #[test]
    fn monthly_step_crosses_year_boundary() {
        let last = ymd(2024, 11, 15);
        let result = generate_future_timestamps(&last, &Frequency::Months(1), 3);
        assert_eq!(result[0], ymd(2024, 12, 15));
        assert_eq!(result[1], ymd(2025, 1, 15));
        assert_eq!(result[2], ymd(2025, 2, 15));
    }

    // --- 2. Quarterly stepping (Frequency::Months(3)) ---

    #[test]
    fn quarterly_step_from_jan31() {
        // original_day=31 preserved across quarters
        let last = ymd(2024, 1, 31);
        let result = generate_future_timestamps(&last, &Frequency::Months(3), 4);
        assert_eq!(result[0], ymd(2024, 4, 30)); // Apr 30 (31 clamped)
        assert_eq!(result[1], ymd(2024, 7, 31)); // Jul 31 (original_day=31)
        assert_eq!(result[2], ymd(2024, 10, 31)); // Oct 31 (original_day=31)
        assert_eq!(result[3], ymd(2025, 1, 31)); // Jan 31 (original_day=31)
    }

    #[test]
    fn quarterly_step_from_mid_month() {
        let last = ymd(2024, 3, 15);
        let result = generate_future_timestamps(&last, &Frequency::Months(3), 4);
        assert_eq!(result[0], ymd(2024, 6, 15));
        assert_eq!(result[1], ymd(2024, 9, 15));
        assert_eq!(result[2], ymd(2024, 12, 15));
        assert_eq!(result[3], ymd(2025, 3, 15));
    }

    #[test]
    fn quarterly_step_crosses_year_boundary() {
        let last = ymd(2024, 10, 31);
        let result = generate_future_timestamps(&last, &Frequency::Months(3), 2);
        assert_eq!(result[0], ymd(2025, 1, 31));
        assert_eq!(result[1], ymd(2025, 4, 30)); // Apr has 30 days
    }

    // --- 3. Yearly stepping from Feb 29 (leap -> non-leap) ---

    #[test]
    fn yearly_step_from_feb29_leap_to_non_leap() {
        // original_day=29 preserved: clamped to 28 in non-leap, restored to 29 in leap
        let last = ymd(2024, 2, 29);
        let result = generate_future_timestamps(&last, &Frequency::Years(1), 4);
        assert_eq!(result[0], ymd(2025, 2, 28)); // 2025 not leap (29 clamped)
        assert_eq!(result[1], ymd(2026, 2, 28)); // 2026 not leap (29 clamped)
        assert_eq!(result[2], ymd(2027, 2, 28)); // 2027 not leap (29 clamped)
        assert_eq!(result[3], ymd(2028, 2, 29)); // 2028 IS leap (original_day=29 fits)
    }

    #[test]
    fn yearly_step_from_regular_date() {
        let last = ymd(2024, 7, 4);
        let result = generate_future_timestamps(&last, &Frequency::Years(1), 3);
        assert_eq!(result[0], ymd(2025, 7, 4));
        assert_eq!(result[1], ymd(2026, 7, 4));
        assert_eq!(result[2], ymd(2027, 7, 4));
    }

    #[test]
    fn yearly_step_two_years() {
        // original_day=29, 2-year stride
        let last = ymd(2024, 2, 29);
        let result = generate_future_timestamps(&last, &Frequency::Years(2), 3);
        assert_eq!(result[0], ymd(2026, 2, 28)); // not leap (29 clamped)
        assert_eq!(result[1], ymd(2028, 2, 29)); // leap (original_day=29 fits)
        assert_eq!(result[2], ymd(2030, 2, 28)); // not leap (29 clamped)
    }

    // --- 4. Daily / weekly / hourly Duration-based stepping ---

    #[test]
    fn daily_duration_step() {
        let last = ymd(2024, 12, 30);
        let result = generate_future_timestamps(&last, &Frequency::Duration(Duration::days(1)), 3);
        assert_eq!(result[0], ymd(2024, 12, 31));
        assert_eq!(result[1], ymd(2025, 1, 1));
        assert_eq!(result[2], ymd(2025, 1, 2));
    }

    #[test]
    fn weekly_duration_step() {
        let last = ymd(2024, 1, 1); // Monday
        let result = generate_future_timestamps(&last, &Frequency::Duration(Duration::weeks(1)), 4);
        assert_eq!(result[0], ymd(2024, 1, 8));
        assert_eq!(result[1], ymd(2024, 1, 15));
        assert_eq!(result[2], ymd(2024, 1, 22));
        assert_eq!(result[3], ymd(2024, 1, 29));
    }

    #[test]
    fn hourly_duration_step() {
        let last = ymdhms(2024, 3, 10, 22, 0, 0);
        let result = generate_future_timestamps(&last, &Frequency::Duration(Duration::hours(1)), 4);
        assert_eq!(result[0], ymdhms(2024, 3, 10, 23, 0, 0));
        assert_eq!(result[1], ymdhms(2024, 3, 11, 0, 0, 0)); // crosses midnight
        assert_eq!(result[2], ymdhms(2024, 3, 11, 1, 0, 0));
        assert_eq!(result[3], ymdhms(2024, 3, 11, 2, 0, 0));
    }

    #[test]
    fn sub_hourly_duration_step_30min() {
        let last = ymdhms(2024, 1, 1, 0, 0, 0);
        let result =
            generate_future_timestamps(&last, &Frequency::Duration(Duration::minutes(30)), 3);
        assert_eq!(result[0], ymdhms(2024, 1, 1, 0, 30, 0));
        assert_eq!(result[1], ymdhms(2024, 1, 1, 1, 0, 0));
        assert_eq!(result[2], ymdhms(2024, 1, 1, 1, 30, 0));
    }

    // --- 5. TimeSeries::future_timestamps auto-inference ---

    #[test]
    fn future_timestamps_infers_daily_frequency() {
        // Build a daily series of 30 points so inference is reliable
        let timestamps: Vec<DateTime<Utc>> = (0..30)
            .map(|i| ymd(2024, 1, 1) + Duration::days(i))
            .collect();
        let values: Vec<f64> = (0..30).map(|i| i as f64).collect();
        let ts = TimeSeries::univariate(timestamps, values).unwrap();

        let future = ts.future_timestamps(5).unwrap();
        assert_eq!(future.len(), 5);
        assert_eq!(future[0], ymd(2024, 1, 31));
        assert_eq!(future[1], ymd(2024, 2, 1));
        assert_eq!(future[2], ymd(2024, 2, 2));
        assert_eq!(future[3], ymd(2024, 2, 3));
        assert_eq!(future[4], ymd(2024, 2, 4));
    }

    #[test]
    fn future_timestamps_infers_weekly_frequency() {
        // Build a weekly series (every Monday) for 10 weeks
        let timestamps: Vec<DateTime<Utc>> = (0..10)
            .map(|i| ymd(2024, 1, 1) + Duration::weeks(i))
            .collect();
        let values: Vec<f64> = (0..10).map(|i| i as f64).collect();
        let ts = TimeSeries::univariate(timestamps, values).unwrap();

        let future = ts.future_timestamps(3).unwrap();
        assert_eq!(future.len(), 3);
        // Last data point is 2024-01-01 + 9 weeks = 2024-03-04
        assert_eq!(future[0], ymd(2024, 3, 11));
        assert_eq!(future[1], ymd(2024, 3, 18));
        assert_eq!(future[2], ymd(2024, 3, 25));
    }

    #[test]
    fn future_timestamps_infers_monthly_frequency() {
        // Build a monthly series using first-of-month dates
        let timestamps: Vec<DateTime<Utc>> = (0..12).map(|i| ymd(2024, 1 + i as u32, 1)).collect();
        let values: Vec<f64> = (0..12).map(|i| i as f64).collect();
        let ts = TimeSeries::univariate(timestamps, values).unwrap();

        // future_timestamps detects ~30-day spacing and uses Frequency::Months(1)
        let future = ts.future_timestamps(3).unwrap();
        assert_eq!(future.len(), 3);
        // Calendar-aware monthly stepping from Dec 1
        assert_eq!(future[0], ymd(2025, 1, 1));
        assert_eq!(future[1], ymd(2025, 2, 1));
        assert_eq!(future[2], ymd(2025, 3, 1));
    }

    // --- 6. Empty series edge case ---

    #[test]
    fn future_timestamps_empty_series_returns_error() {
        let ts = TimeSeries::univariate(vec![], vec![]).unwrap();
        let result = ts.future_timestamps(5);
        assert!(result.is_err());
        assert!(matches!(result, Err(ForecastError::EmptyData)));
    }

    // --- 7. Horizon=0 returns empty vec ---

    #[test]
    fn generate_future_timestamps_horizon_zero_returns_empty() {
        let last = ymd(2024, 6, 15);
        let result = generate_future_timestamps(&last, &Frequency::Duration(Duration::days(1)), 0);
        assert!(result.is_empty());
    }

    #[test]
    fn generate_future_timestamps_horizon_zero_monthly() {
        let last = ymd(2024, 6, 15);
        let result = generate_future_timestamps(&last, &Frequency::Months(1), 0);
        assert!(result.is_empty());
    }

    #[test]
    fn generate_future_timestamps_horizon_zero_yearly() {
        let last = ymd(2024, 6, 15);
        let result = generate_future_timestamps(&last, &Frequency::Years(1), 0);
        assert!(result.is_empty());
    }

    // --- 8. Large horizon (100+ steps) ---

    #[test]
    fn large_horizon_daily_produces_correct_count_and_ordering() {
        let last = ymd(2024, 1, 1);
        let horizon = 365;
        let result =
            generate_future_timestamps(&last, &Frequency::Duration(Duration::days(1)), horizon);
        assert_eq!(result.len(), horizon);
        // First and last
        assert_eq!(result[0], ymd(2024, 1, 2));
        assert_eq!(result[364], ymd(2024, 12, 31)); // 2024 is leap => 366 days, so day 366 = Dec 31
                                                    // Strictly increasing
        for w in result.windows(2) {
            assert!(w[1] > w[0]);
        }
    }

    #[test]
    fn large_horizon_monthly_120_steps() {
        let last = ymd(2024, 1, 15);
        let horizon = 120; // 10 years of months
        let result = generate_future_timestamps(&last, &Frequency::Months(1), horizon);
        assert_eq!(result.len(), horizon);
        // First step
        assert_eq!(result[0], ymd(2024, 2, 15));
        // 12th step: Jan 2025
        assert_eq!(result[11], ymd(2025, 1, 15));
        // 120th step: Jan 2034
        assert_eq!(result[119], ymd(2034, 1, 15));
        // Strictly increasing
        for w in result.windows(2) {
            assert!(w[1] > w[0]);
        }
    }

    #[test]
    fn large_horizon_yearly_200_steps() {
        let last = ymd(2024, 6, 15);
        let horizon = 200;
        let result = generate_future_timestamps(&last, &Frequency::Years(1), horizon);
        assert_eq!(result.len(), horizon);
        assert_eq!(result[0], ymd(2025, 6, 15));
        assert_eq!(result[199], ymd(2224, 6, 15));
        for w in result.windows(2) {
            assert!(w[1] > w[0]);
        }
    }

    #[test]
    fn large_horizon_hourly_1000_steps() {
        let last = ymdhms(2024, 1, 1, 0, 0, 0);
        let horizon = 1000;
        let result =
            generate_future_timestamps(&last, &Frequency::Duration(Duration::hours(1)), horizon);
        assert_eq!(result.len(), horizon);
        assert_eq!(result[0], ymdhms(2024, 1, 1, 1, 0, 0));
        // 1000 hours = 41 days + 16 hours
        assert_eq!(result[999], ymdhms(2024, 2, 11, 16, 0, 0));
        for w in result.windows(2) {
            assert!(w[1] > w[0]);
        }
    }

    // --- Additional edge cases ---

    #[test]
    fn monthly_step_preserves_time_of_day() {
        let last = ymdhms(2024, 1, 15, 14, 30, 45);
        let result = generate_future_timestamps(&last, &Frequency::Months(1), 2);
        assert_eq!(result[0], ymdhms(2024, 2, 15, 14, 30, 45));
        assert_eq!(result[1], ymdhms(2024, 3, 15, 14, 30, 45));
    }

    #[test]
    fn yearly_step_preserves_time_of_day() {
        let last = ymdhms(2024, 7, 4, 10, 15, 0);
        let result = generate_future_timestamps(&last, &Frequency::Years(1), 2);
        assert_eq!(result[0], ymdhms(2025, 7, 4, 10, 15, 0));
        assert_eq!(result[1], ymdhms(2026, 7, 4, 10, 15, 0));
    }

    #[test]
    fn monthly_step_single_step() {
        let last = ymd(2024, 6, 30);
        let result = generate_future_timestamps(&last, &Frequency::Months(1), 1);
        assert_eq!(result.len(), 1);
        assert_eq!(result[0], ymd(2024, 7, 30));
    }

    #[test]
    fn quarterly_step_from_quarter_parse() {
        // Frequency::parse("1q") should produce Months(3), original_day=31
        let freq = Frequency::parse("1q").unwrap();
        assert_eq!(freq, Frequency::Months(3));
        let last = ymd(2024, 3, 31);
        let result = generate_future_timestamps(&last, &freq, 4);
        assert_eq!(result[0], ymd(2024, 6, 30)); // Jun 30 (31 clamped)
        assert_eq!(result[1], ymd(2024, 9, 30)); // Sep 30 (31 clamped)
        assert_eq!(result[2], ymd(2024, 12, 31)); // Dec 31 (original_day=31)
        assert_eq!(result[3], ymd(2025, 3, 31)); // Mar 31 (original_day=31)
    }
}