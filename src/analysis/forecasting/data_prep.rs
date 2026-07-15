//! Data preparation utilities for time series forecasting.
//! This module includes:
//! - TimeSeries struct for representing time series data with timestamps and values.
//! - Methods for handling missing values (NaN/Inf) with various policies.
//! - Frequency inference from timestamps, including calendar-aware inference.
//! - Gap filling to create regular time series from irregular data.
//! - Seasonal and trend strength estimation via STL decomposition.
//! - Outlier detection and replacement with local median.
use crate::common::Sample;
use anofox_forecast::ForecastError;
use anofox_forecast::core::{CalendarAnnotations, Frequency, MissingValuePolicy};
use anofox_forecast::detection::{OutlierConfig, detect_outliers};
use anofox_forecast::seasonality::STL;
use anofox_forecast::utils::stats::{nan_mean, nan_median};
use chrono::{DateTime, Datelike, Duration, Utc};
use std::collections::{BTreeMap, HashMap};

/// A time series with timestamps and values.
#[derive(Debug, Clone)]
pub struct TimeSeriesFiller {
    timestamps: Vec<DateTime<Utc>>,
    /// Values stored in observation order.
    values: Vec<f64>,
    frequency: Option<Duration>,
    calendar: Option<CalendarAnnotations>,
    delta: BTreeMap<DateTime<Utc>, Sample>,
}

impl TimeSeriesFiller {
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
    pub fn univariate(
        timestamps: Vec<DateTime<Utc>>,
        values: Vec<f64>,
    ) -> Result<TimeSeriesFiller, ForecastError> {
        Self::new(timestamps, values, None, None)
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
        self.values.iter().any(|v| !v.is_finite())
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

        let mut ts = TimeSeriesFiller::univariate(timestamps, values).unwrap();

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

        let result = TimeSeriesFiller::univariate(timestamps, values);
        assert!(matches!(result, Err(ForecastError::TimestampError(_))));

        // Duplicate timestamps
        let timestamps = vec![
            Utc.with_ymd_and_hms(2024, 1, 1, 0, 0, 0).unwrap(),
            Utc.with_ymd_and_hms(2024, 1, 1, 1, 0, 0).unwrap(),
            Utc.with_ymd_and_hms(2024, 1, 1, 1, 0, 0).unwrap(), // duplicate
        ];
        let values = vec![1.0, 2.0, 3.0];

        let result = TimeSeriesFiller::univariate(timestamps, values);
        assert!(matches!(result, Err(ForecastError::TimestampError(_))));
    }

    #[test]
    fn missing_mask_correct() {
        let timestamps = make_timestamps(5);
        let values = vec![1.0, f64::NAN, 3.0, f64::INFINITY, 5.0];
        let ts = TimeSeriesFiller::univariate(timestamps, values).unwrap();
        let mask = ts.missing_mask();
        assert_eq!(mask, vec![false, true, false, true, false]);
    }

    #[test]
    fn missing_count_basic() {
        let timestamps = make_timestamps(4);
        let values = vec![1.0, f64::NAN, 3.0, f64::NAN];
        let ts = TimeSeriesFiller::univariate(timestamps, values).unwrap();
        assert_eq!(ts.missing_count(), 2);
    }

    #[test]
    fn with_imputed_regressors_rejects_drop() {
        let timestamps = make_daily_timestamps(3);
        let values = vec![1.0, 2.0, 3.0];
        let calendar =
            CalendarAnnotations::new().with_regressor("x".to_string(), vec![1.0, f64::NAN, 3.0]);

        let mut ts = TimeSeriesFiller::univariate(timestamps, values).unwrap();
        ts.set_calendar(calendar);

        assert!(
            ts.with_imputed_regressors(MissingValuePolicy::Drop)
                .is_err()
        );
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
        let ts = TimeSeriesFiller::univariate(timestamps, values).unwrap();

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
        let ts = TimeSeriesFiller::univariate(timestamps, values).unwrap();

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
        let ts = TimeSeriesFiller::univariate(timestamps, values).unwrap();

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
        let ts = TimeSeriesFiller::univariate(vec![], vec![]).unwrap();
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
