use crate::common::Sample;
use anofox_forecast::error::ForecastError;

/// Policy for imputing missing (NaN/infinite) values in a time series.
#[derive(Debug, Clone)]
pub enum ImputationPolicy {
    /// Return an error if any missing values are present.
    Error,
    /// Drop all samples with missing values.
    Drop,
    /// Replace missing values with a constant fill value.
    Fill(f64),
    /// Forward-fill: carry the last valid observation forward.
    ForwardFill,
    /// Backward-fill: carry the next valid observation backward.
    BackwardFill,
    /// Replace missing values with the mean of all valid values.
    FillMean,
    /// Replace missing values with the median of all valid values.
    FillMedian,
    /// Linearly interpolate between valid neighbors.
    Interpolate,
    /// Forward-fill then backward-fill (handles both gaps and edge NaNs).
    ForwardBackwardFill,
    /// Replace missing values with the mean of valid values in a centered
    /// window. The window size must be odd and > 0.
    MovingAverage(usize),
    /// Replace missing values with the median of values at the same seasonal
    /// position (index % period). The period must be > 0.
    Seasonal(usize),
}

// Return a sanitized copy with missing values handled. Modifies the input in-place and
// returns a vector of imputed samples for merging with the original time series.
pub fn sanitize(
    samples: &mut Vec<Sample>,
    policy: ImputationPolicy,
) -> Result<Vec<Sample>, ForecastError> {
    let mut map = Vec::new();
    match policy {
        ImputationPolicy::Error => {
            if samples.iter().any(|s| !s.value.is_finite()) {
                return Err(ForecastError::MissingValues);
            }
        }
        ImputationPolicy::Drop => {
            // Find indices of valid observations
            map.extend(samples.iter().filter(|s| s.value.is_finite()).copied());
            samples.retain(|s| s.value.is_finite());
        }
        ImputationPolicy::Fill(fill_value) => {
            samples
                .iter_mut()
                .filter(|s| !s.value.is_finite())
                .for_each(|s| {
                    s.value = fill_value;
                    map.push(Sample {
                        timestamp: s.timestamp,
                        value: s.value,
                    });
                });
        }
        ImputationPolicy::ForwardFill => {
            let mut last_valid = None;
            for s in samples.iter_mut() {
                if !s.value.is_finite() {
                    let value_to_use = last_valid.unwrap_or(s.value);
                    s.value = value_to_use;
                    map.push(Sample {
                        timestamp: s.timestamp,
                        value: s.value,
                    });
                } else {
                    last_valid = Some(s.value);
                }
            }
        }
        ImputationPolicy::BackwardFill => {
            let mut next_valid = None;
            for s in samples.iter_mut().rev() {
                if !s.value.is_finite() {
                    if let Some(v) = next_valid {
                        s.value = v;
                        map.push(Sample {
                            timestamp: s.timestamp,
                            value: s.value,
                        });
                    }
                } else {
                    next_valid = Some(s.value);
                }
            }
        }
        ImputationPolicy::FillMean => {
            let m = calc_mean(samples);
            for s in samples.iter_mut() {
                if !s.value.is_finite() {
                    s.value = m;
                    map.push(Sample {
                        timestamp: s.timestamp,
                        value: s.value,
                    });
                }
            }
        }
        ImputationPolicy::FillMedian => {
            let med = calc_median(samples);
            for s in samples.iter_mut() {
                if !s.value.is_finite() {
                    s.value = med;
                    map.push(Sample {
                        timestamp: s.timestamp,
                        value: s.value,
                    });
                }
            }
        }
        ImputationPolicy::Interpolate => {
            let interpolated = interpolate_series(samples, true);
            for (orig, interp) in samples.iter().zip(interpolated.iter()) {
                if !orig.value.is_finite() && interp.value.is_finite() {
                    map.push(*interp);
                }
            }
            *samples = interpolated;
        }
        ImputationPolicy::ForwardBackwardFill => {
            map = impute_forward_backward(samples)?;
        }
        ImputationPolicy::MovingAverage(window) => {
            map = impute_moving_average(samples, window)?;
        }
        ImputationPolicy::Seasonal(period) => {
            map = impute_seasonal(samples, period)?;
        }
    }

    Ok(map)
}

pub fn calc_mean(values: &[Sample]) -> f64 {
    let mut sum = 0.0;
    let mut count = 0usize;
    for sample in values {
        if sample.value.is_finite() {
            sum += sample.value;
            count += 1;
        }
    }
    if count == 0 {
        f64::NAN
    } else {
        sum / count as f64
    }
}

fn calc_median(values: &[Sample]) -> f64 {
    let mut finite: Vec<f64> = values
        .iter()
        .filter_map(|s| {
            if s.value.is_finite() {
                Some(s.value)
            } else {
                None
            }
        })
        .collect();
    if finite.is_empty() {
        return f64::NAN;
    }
    finite.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    let n = finite.len();
    if n.is_multiple_of(2) {
        (finite[n / 2 - 1] + finite[n / 2]) / 2.0
    } else {
        finite[n / 2]
    }
}

/// Linear interpolation for a series with NaN values.
fn interpolate_series(samples: &[Sample], fill_edges: bool) -> Vec<Sample> {
    if samples.is_empty() {
        return vec![];
    }

    let mut result = samples.to_vec();
    let n = result.len();

    // Find and fill NaN segments
    let mut i = 0;
    while i < n {
        if result[i].value.is_nan() {
            let start = i;
            while i < n && result[i].value.is_nan() {
                i += 1;
            }
            let left = if start > 0 {
                Some(result[start - 1].value)
            } else {
                None
            };
            let right = if i < n { Some(result[i].value) } else { None };
            fill_nan_segment(&mut result[start..i], left, right, fill_edges);
        } else {
            i += 1;
        }
    }

    result
}

/// Fill a NaN segment using linear interpolation or edge values.
fn fill_nan_segment(
    segment: &mut [Sample],
    left: Option<f64>,
    right: Option<f64>,
    fill_edges: bool,
) {
    match (left, right) {
        (Some(l), Some(r)) => {
            let segments = (segment.len() + 1) as f64;
            for (j, sample) in segment.iter_mut().enumerate() {
                let t = (j + 1) as f64 / segments;
                sample.value = l + t * (r - l);
            }
        }
        (Some(l), None) if fill_edges => {
            for sample in segment.iter_mut() {
                sample.value = l;
            }
        }
        (None, Some(r)) if fill_edges => {
            for sample in segment.iter_mut() {
                sample.value = r;
            }
        }
        _ => {} // Leave as NaN
    }
}

/// Forward-fill then backward-fill — handles both leading and trailing NaNs.
pub fn impute_forward_backward(samples: &mut [Sample]) -> Result<Vec<Sample>, ForecastError> {
    let mut result = Vec::new();

    // Forward fill
    let mut last_valid = None;
    for v in samples.iter_mut() {
        let value = v.value;
        if !value.is_finite() {
            let value_to_use = last_valid.unwrap_or(value);
            *v = Sample {
                timestamp: v.timestamp,
                value: value_to_use,
            };
            result.push(*v);
        } else {
            last_valid = Some(value);
        }
    }

    // Backward fill remaining (leading NaNs)
    let mut next_valid = None;
    for i in (0..samples.len()).rev() {
        let value = samples[i].value;
        if !value.is_finite() {
            if let Some(v) = next_valid {
                samples[i].value = v;
                result.push(samples[i]);
            }
        } else {
            next_valid = Some(value);
        }
    }
    Ok(result)
}

/// Impute NaN using mean of valid values in a centered window.
///
/// Window must be odd. Multi-pass (up to 3) for adjacent NaNs.
/// Remaining NaNs filled with global mean.
pub fn impute_moving_average(
    samples: &mut [Sample],
    window: usize,
) -> Result<Vec<Sample>, ForecastError> {
    if window == 0 || window.is_multiple_of(2) {
        return Err(ForecastError::InvalidParameter(
            "moving average window must be odd and > 0".to_string(),
        ));
    }

    let half = window / 2;
    let mut working_set = samples.to_vec();
    let n = working_set.len();

    // Multi-pass: up to 3 passes to handle adjacent NaNs
    for _ in 0..3 {
        let mut changed = false;
        let snapshot = working_set.clone();
        for i in 0..n {
            let value = snapshot[i].value;
            if value.is_finite() {
                continue;
            }
            let start = i.saturating_sub(half);
            let end = (i + half + 1).min(n);
            let mut sum = 0.0;
            let mut count = 0usize;
            for (j, sample) in snapshot.iter().enumerate().take(end).skip(start) {
                if j != i && sample.value.is_finite() {
                    sum += sample.value;
                    count += 1;
                }
            }
            if count > 0 {
                working_set[i].value = sum / count as f64;
                changed = true;
            }
        }
        if !changed {
            break;
        }
    }

    // Fill any remaining NaNs with global mean
    let global_mean = calc_mean(&working_set);

    let result = working_set
        .into_iter()
        .map(|v| {
            if v.value.is_finite() {
                v
            } else {
                Sample {
                    timestamp: v.timestamp,
                    value: global_mean,
                }
            }
        })
        .collect::<Vec<Sample>>();

    Ok(result)
}

/// Impute NaN using the median of observed values at the same seasonal position.
///
/// Groups values by (index % period), computes median per group, fills NaN.
/// Returns error if period is 0 or if >50% of values in any seasonal bucket are NaN.
pub fn impute_seasonal(samples: &[Sample], period: usize) -> Result<Vec<Sample>, ForecastError> {
    if period == 0 {
        return Err(ForecastError::InvalidParameter(
            "seasonal period must be > 0".to_string(),
        ));
    }
    if samples.len() < period {
        return Err(ForecastError::InsufficientData {
            needed: period,
            got: samples.len(),
            hint: None,
        });
    }

    let n = samples.len();

    // Collect finite values per seasonal bucket
    let mut buckets: Vec<Vec<f64>> = vec![Vec::new(); period];
    for (i, sample) in samples.iter().enumerate() {
        if sample.value.is_finite() {
            buckets[i % period].push(sample.value);
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
                sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
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
    let mut result = samples.to_vec();
    for i in 0..n {
        if !samples[i].value.is_finite() {
            result[i] = Sample {
                timestamp: samples[i].timestamp,
                value: medians[i % period],
            };
        }
    }

    // Validate: check that no seasonal bucket had >50% NaN
    let mut bucket_total: Vec<usize> = vec![0; period];
    let mut bucket_missing: Vec<usize> = vec![0; period];
    for (i, sample) in samples.iter().enumerate() {
        bucket_total[i % period] += 1;
        if !sample.value.is_finite() {
            bucket_missing[i % period] += 1;
        }
    }
    for (b, (&total, &missing)) in bucket_total.iter().zip(bucket_missing.iter()).enumerate() {
        if total > 0 && missing as f64 / total as f64 > 0.5 {
            return Err(ForecastError::InvalidParameter(format!(
                "seasonal bucket {} has >50% missing values ({}/{})",
                b, missing, total
            )));
        }
    }
    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;
    use approx::assert_relative_eq;
    use chrono::{DateTime, TimeZone, Utc};

    // --- Helpers ---

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

    /// Build samples with hourly timestamps starting at 2024-01-01T00:00:00.
    fn make_samples(values: Vec<f64>) -> Vec<Sample> {
        let base = Utc.with_ymd_and_hms(2024, 1, 1, 0, 0, 0).unwrap();
        values
            .into_iter()
            .enumerate()
            .map(|(i, v)| Sample {
                timestamp: (base + chrono::Duration::hours(i as i64)).timestamp(),
                value: v,
            })
            .collect()
    }

    /// Extract values from a slice of Samples.
    fn sample_values(samples: &[Sample]) -> Vec<f64> {
        samples.iter().map(|s| s.value).collect()
    }

    // ============================================================
    // sanitize — Error policy
    // ============================================================

    #[test]
    fn sanitize_error_on_missing() {
        let mut samples = make_samples(vec![1.0, f64::NAN, 3.0]);
        let result = sanitize(&mut samples, ImputationPolicy::Error);
        assert!(matches!(result, Err(ForecastError::MissingValues)));
    }

    #[test]
    fn sanitize_error_on_inf() {
        let mut samples = make_samples(vec![1.0, f64::INFINITY, 3.0]);
        let result = sanitize(&mut samples, ImputationPolicy::Error);
        assert!(matches!(result, Err(ForecastError::MissingValues)));
    }

    #[test]
    fn sanitize_error_no_missing() {
        let mut samples = make_samples(vec![1.0, 2.0, 3.0]);
        let result = sanitize(&mut samples, ImputationPolicy::Error);
        assert!(result.is_ok());
        assert_eq!(sample_values(&samples), vec![1.0, 2.0, 3.0]);
        // Map is empty — no values were imputed
        assert!(result.unwrap().is_empty());
    }

    // ============================================================
    // sanitize — Drop policy
    // ============================================================

    #[test]
    fn sanitize_drop() {
        let mut samples = make_samples(vec![1.0, f64::NAN, 3.0, f64::INFINITY, 5.0]);
        let map = sanitize(&mut samples, ImputationPolicy::Drop).unwrap();
        assert_eq!(samples.len(), 3);
        assert_eq!(sample_values(&samples), vec![1.0, 3.0, 5.0]);
        // Map contains the valid (kept) samples
        assert_eq!(map.len(), 3);
        assert_eq!(sample_values(&map), vec![1.0, 3.0, 5.0]);
    }

    // ============================================================
    // sanitize — Fill constant
    // ============================================================

    #[test]
    fn sanitize_fill_constant() {
        let mut samples = make_samples(vec![1.0, f64::NAN, 3.0, f64::INFINITY, 5.0]);
        let map = sanitize(&mut samples, ImputationPolicy::Fill(0.0)).unwrap();
        assert_eq!(sample_values(&samples), vec![1.0, 0.0, 3.0, 0.0, 5.0]);
        // Map contains the 2 imputed samples
        assert_eq!(map.len(), 2);
        assert_relative_eq!(map[0].value, 0.0);
        assert_relative_eq!(map[1].value, 0.0);
    }

    // ============================================================
    // sanitize — ForwardFill
    // ============================================================

    #[test]
    fn sanitize_forward_fill() {
        let mut samples = make_samples(vec![1.0, f64::NAN, 3.0, f64::INFINITY, 5.0]);
        let map = sanitize(&mut samples, ImputationPolicy::ForwardFill).unwrap();
        assert_eq!(sample_values(&samples), vec![1.0, 1.0, 3.0, 3.0, 5.0]);
        // Map has the 2 imputed samples
        assert_eq!(map.len(), 2);
    }

    #[test]
    fn sanitize_forward_fill_leading_nan() {
        let mut samples = make_samples(vec![f64::NAN, 2.0, 3.0]);
        let map = sanitize(&mut samples, ImputationPolicy::ForwardFill).unwrap();
        // Leading NaN stays NaN (no last_valid to carry forward)
        assert!(samples[0].value.is_nan());
        assert_relative_eq!(samples[1].value, 2.0);
        assert_relative_eq!(samples[2].value, 3.0);
        // Map contains the imputed leading NaN (with its original NaN value)
        assert_eq!(map.len(), 1);
        assert!(map[0].value.is_nan());
    }

    // ============================================================
    // sanitize — BackwardFill
    // ============================================================

    #[test]
    fn sanitize_backward_fill_basic() {
        let mut samples = make_samples(vec![1.0, 2.0, 3.0, f64::NAN, f64::NAN]);
        let map = sanitize(&mut samples, ImputationPolicy::BackwardFill).unwrap();
        // Trailing NaN left as NaN (no next valid value to pull from)
        assert_relative_eq!(samples[0].value, 1.0);
        assert_relative_eq!(samples[1].value, 2.0);
        assert_relative_eq!(samples[2].value, 3.0);
        assert!(samples[3].value.is_nan());
        assert!(samples[4].value.is_nan());
        // Map is empty — trailing NaNs not imputed (no next_valid)
        assert!(map.is_empty());
    }

    #[test]
    fn sanitize_backward_fill_interior() {
        let mut samples = make_samples(vec![1.0, f64::NAN, f64::NAN, 4.0, 5.0]);
        let map = sanitize(&mut samples, ImputationPolicy::BackwardFill).unwrap();
        assert_relative_eq!(samples[0].value, 1.0);
        assert_relative_eq!(samples[1].value, 4.0);
        assert_relative_eq!(samples[2].value, 4.0);
        assert_relative_eq!(samples[3].value, 4.0);
        assert_relative_eq!(samples[4].value, 5.0);
        // Map contains 2 imputed samples (indices 1 and 2)
        assert_eq!(map.len(), 2);
    }

    #[test]
    fn sanitize_backward_fill_leading_nan() {
        let mut samples = make_samples(vec![f64::NAN, f64::NAN, 3.0, 4.0]);
        let map = sanitize(&mut samples, ImputationPolicy::BackwardFill).unwrap();
        assert_relative_eq!(samples[0].value, 3.0);
        assert_relative_eq!(samples[1].value, 3.0);
        assert_relative_eq!(samples[2].value, 3.0);
        assert_relative_eq!(samples[3].value, 4.0);
        // Map contains 2 imputed samples (indices 0 and 1)
        assert_eq!(map.len(), 2);
    }

    // ============================================================
    // sanitize — FillMean
    // ============================================================

    #[test]
    fn sanitize_fill_mean() {
        let mut samples = make_samples(vec![1.0, f64::NAN, 3.0, f64::NAN, 5.0]);
        let map = sanitize(&mut samples, ImputationPolicy::FillMean).unwrap();
        // Mean of [1, 3, 5] = 3.0
        assert_eq!(sample_values(&samples), vec![1.0, 3.0, 3.0, 3.0, 5.0]);
        assert_eq!(map.len(), 2);
    }

    #[test]
    fn sanitize_fill_mean_all_nan() {
        let mut samples = make_samples(vec![f64::NAN, f64::NAN, f64::NAN]);
        let map = sanitize(&mut samples, ImputationPolicy::FillMean).unwrap();
        // All-NaN produces NaN fill (calc_mean returns NaN when count == 0)
        assert!(samples[0].value.is_nan());
        assert!(samples[1].value.is_nan());
        assert!(samples[2].value.is_nan());
        assert_eq!(map.len(), 3);
    }

    #[test]
    fn sanitize_fill_mean_with_inf() {
        let mut samples = make_samples(vec![2.0, f64::INFINITY, 4.0, f64::NAN]);
        let map = sanitize(&mut samples, ImputationPolicy::FillMean).unwrap();
        // Mean of [2, 4] = 3.0; Inf and NaN both replaced
        assert_eq!(sample_values(&samples), vec![2.0, 3.0, 4.0, 3.0]);
        assert_eq!(map.len(), 2);
    }

    // ============================================================
    // sanitize — FillMedian
    // ============================================================

    #[test]
    fn sanitize_fill_median() {
        let mut samples = make_samples(vec![1.0, f64::NAN, 3.0, f64::NAN, 10.0]);
        let map = sanitize(&mut samples, ImputationPolicy::FillMedian).unwrap();
        // Median of [1, 3, 10] = 3.0
        assert_eq!(sample_values(&samples), vec![1.0, 3.0, 3.0, 3.0, 10.0]);
        assert_eq!(map.len(), 2);
    }

    // ============================================================
    // sanitize — Interpolate
    // ============================================================

    #[test]
    fn sanitize_interpolate_fills_gaps() {
        let mut samples = make_samples(vec![1.0, f64::NAN, f64::NAN, 4.0, 5.0]);
        let map = sanitize(&mut samples, ImputationPolicy::Interpolate).unwrap();
        // Linear interpolation: 1→4 over 3 steps → 2.0, 3.0
        assert_relative_eq!(samples[0].value, 1.0, epsilon = 1e-10);
        assert_relative_eq!(samples[1].value, 2.0, epsilon = 1e-10);
        assert_relative_eq!(samples[2].value, 3.0, epsilon = 1e-10);
        assert_relative_eq!(samples[3].value, 4.0, epsilon = 1e-10);
        assert_relative_eq!(samples[4].value, 5.0, epsilon = 1e-10);
        assert_eq!(map.len(), 2);
    }

    #[test]
    fn sanitize_interpolate_fills_edges() {
        let mut samples = make_samples(vec![f64::NAN, f64::NAN, 3.0, 4.0, f64::NAN]);
        let map = sanitize(&mut samples, ImputationPolicy::Interpolate).unwrap();
        // Edge filling enabled: leading NaN → 3.0, trailing NaN → 4.0
        assert_relative_eq!(samples[0].value, 3.0, epsilon = 1e-10);
        assert_relative_eq!(samples[1].value, 3.0, epsilon = 1e-10);
        assert_relative_eq!(samples[2].value, 3.0, epsilon = 1e-10);
        assert_relative_eq!(samples[3].value, 4.0, epsilon = 1e-10);
        assert_relative_eq!(samples[4].value, 4.0, epsilon = 1e-10);
        assert_eq!(map.len(), 3);
    }

    // ============================================================
    // sanitize — ForwardBackwardFill
    // ============================================================

    #[test]
    fn sanitize_forward_backward_handles_leading() {
        let mut samples = make_samples(vec![f64::NAN, f64::NAN, 3.0, f64::NAN, 5.0]);
        let _map = sanitize(&mut samples, ImputationPolicy::ForwardBackwardFill).unwrap();
        // Leading NaN filled backward from 3.0, interior NaN filled forward from 3.0
        assert_relative_eq!(samples[0].value, 3.0);
        assert_relative_eq!(samples[1].value, 3.0);
        assert_relative_eq!(samples[2].value, 3.0);
        assert_relative_eq!(samples[3].value, 3.0);
        assert_relative_eq!(samples[4].value, 5.0);
    }

    #[test]
    fn sanitize_forward_backward_handles_trailing() {
        let mut samples = make_samples(vec![1.0, 2.0, f64::NAN, f64::NAN]);
        let _map = sanitize(&mut samples, ImputationPolicy::ForwardBackwardFill).unwrap();
        assert_relative_eq!(samples[0].value, 1.0);
        assert_relative_eq!(samples[1].value, 2.0);
        // Trailing NaN forward-filled from 2.0
        assert_relative_eq!(samples[2].value, 2.0);
        assert_relative_eq!(samples[3].value, 2.0);
    }

    // ============================================================
    // sanitize — MovingAverage
    // ============================================================

    #[test]
    fn sanitize_moving_average_single_gap() {
        let mut samples = make_samples(vec![1.0, 2.0, f64::NAN, 4.0, 5.0]);
        let map = sanitize(&mut samples, ImputationPolicy::MovingAverage(3)).unwrap();
        // impute_moving_average returns the full imputed result as the map;
        // the original samples are not modified in-place by this policy.
        // Window of 3: neighbors are 2.0 and 4.0 → mean = 3.0
        assert_eq!(map.len(), 5);
        assert_relative_eq!(map[2].value, 3.0, epsilon = 1e-10);
    }

    #[test]
    fn sanitize_moving_average_adjacent_gaps() {
        let mut samples = make_samples(vec![1.0, f64::NAN, f64::NAN, 4.0, 5.0, 6.0]);
        let map = sanitize(&mut samples, ImputationPolicy::MovingAverage(3)).unwrap();
        // After multi-pass, gaps should be filled
        assert_eq!(map.len(), 6);
        assert!(map[1].value.is_finite());
        assert!(map[2].value.is_finite());
    }

    #[test]
    fn sanitize_moving_average_rejects_even_window() {
        let mut samples = make_samples(vec![1.0, 2.0, 3.0, 4.0, 5.0]);
        let result = sanitize(&mut samples, ImputationPolicy::MovingAverage(4));
        assert!(result.is_err());
    }

    #[test]
    fn sanitize_moving_average_rejects_zero_window() {
        let mut samples = make_samples(vec![1.0, 2.0, 3.0]);
        let result = sanitize(&mut samples, ImputationPolicy::MovingAverage(0));
        assert!(result.is_err());
    }

    // ============================================================
    // sanitize — Seasonal
    // ============================================================

    #[test]
    fn sanitize_seasonal_basic() {
        // Period 3: positions 0,1,2,0,1,2,0,1,2
        let mut samples = make_samples(vec![
            10.0,
            20.0,
            30.0, // cycle 1
            11.0,
            21.0,
            31.0, // cycle 2
            f64::NAN,
            22.0,
            32.0, // cycle 3 — NaN at position 0
        ]);
        let map = sanitize(&mut samples, ImputationPolicy::Seasonal(3)).unwrap();
        // impute_seasonal returns the full imputed result as the map;
        // Position 0 values: [10.0, 11.0], median = 10.5
        assert_eq!(map.len(), 9);
        assert_relative_eq!(map[6].value, 10.5, epsilon = 1e-10);
    }

    #[test]
    fn sanitize_seasonal_insufficient_data() {
        let mut samples = make_samples(vec![1.0, f64::NAN, 3.0]);
        // Period 4 but only 3 data points
        let result = sanitize(&mut samples, ImputationPolicy::Seasonal(4));
        assert!(result.is_err());
    }

    #[test]
    fn sanitize_seasonal_rejects_zero_period() {
        let mut samples = make_samples(vec![1.0, 2.0, 3.0, 4.0, 5.0]);
        let result = sanitize(&mut samples, ImputationPolicy::Seasonal(0));
        assert!(result.is_err());
    }

    #[test]
    fn sanitize_seasonal_rejects_too_many_missing() {
        // Period 2: bucket 0 has indices [0, 2, 4], bucket 1 has [1, 3, 5]
        let mut samples = make_samples(vec![f64::NAN, 1.0, f64::NAN, 2.0, f64::NAN, 3.0]);
        // Bucket 0 is 100% NaN (3/3)
        let result = sanitize(&mut samples, ImputationPolicy::Seasonal(2));
        assert!(result.is_err());
    }

    // ============================================================
    // calc_mean
    // ============================================================

    #[test]
    fn calc_mean_basic() {
        let samples = make_samples(vec![1.0, 2.0, 3.0, 4.0, 5.0]);
        assert_relative_eq!(calc_mean(&samples), 3.0, epsilon = 1e-10);
    }

    #[test]
    fn calc_mean_with_nan() {
        let samples = make_samples(vec![1.0, f64::NAN, 3.0, f64::NAN, 5.0]);
        // Mean of [1, 3, 5] = 3.0
        assert_relative_eq!(calc_mean(&samples), 3.0, epsilon = 1e-10);
    }

    #[test]
    fn calc_mean_all_nan() {
        let samples = make_samples(vec![f64::NAN, f64::NAN, f64::NAN]);
        assert!(calc_mean(&samples).is_nan());
    }

    #[test]
    fn calc_mean_with_inf() {
        let samples = make_samples(vec![2.0, f64::INFINITY, 4.0, f64::NAN]);
        // Inf is not finite, so only [2, 4] counted → mean = 3.0
        assert_relative_eq!(calc_mean(&samples), 3.0, epsilon = 1e-10);
    }

    #[test]
    fn calc_mean_empty() {
        assert!(calc_mean(&[]).is_nan());
    }

    // ============================================================
    // calc_median
    // ============================================================

    #[test]
    fn calc_median_basic_odd() {
        let samples = make_samples(vec![1.0, 3.0, 2.0]); // sorted: [1, 2, 3]
        assert_relative_eq!(calc_median(&samples), 2.0, epsilon = 1e-10);
    }

    #[test]
    fn calc_median_basic_even() {
        let samples = make_samples(vec![1.0, 4.0, 2.0, 3.0]); // sorted: [1, 2, 3, 4]
        assert_relative_eq!(calc_median(&samples), 2.5, epsilon = 1e-10);
    }

    #[test]
    fn calc_median_with_nan() {
        let samples = make_samples(vec![1.0, f64::NAN, 3.0, f64::NAN, 10.0]);
        // Median of [1, 3, 10] = 3.0
        assert_relative_eq!(calc_median(&samples), 3.0, epsilon = 1e-10);
    }

    #[test]
    fn calc_median_all_nan() {
        let samples = make_samples(vec![f64::NAN, f64::NAN, f64::NAN]);
        assert!(calc_median(&samples).is_nan());
    }

    #[test]
    fn calc_median_empty() {
        assert!(calc_median(&[]).is_nan());
    }

    // ============================================================
    // interpolate_series
    // ============================================================

    #[test]
    fn interpolate_series_basic() {
        let samples = make_samples(vec![1.0, f64::NAN, f64::NAN, 4.0, 5.0]);
        let result = interpolate_series(&samples, true);
        assert_relative_eq!(result[0].value, 1.0, epsilon = 1e-10);
        assert_relative_eq!(result[1].value, 2.0, epsilon = 1e-10);
        assert_relative_eq!(result[2].value, 3.0, epsilon = 1e-10);
        assert_relative_eq!(result[3].value, 4.0, epsilon = 1e-10);
        assert_relative_eq!(result[4].value, 5.0, epsilon = 1e-10);
    }

    #[test]
    fn interpolate_series_fills_edges() {
        let samples = make_samples(vec![f64::NAN, f64::NAN, 3.0, 4.0, f64::NAN]);
        let result = interpolate_series(&samples, true);
        // Edge filling enabled
        assert_relative_eq!(result[0].value, 3.0, epsilon = 1e-10);
        assert_relative_eq!(result[1].value, 3.0, epsilon = 1e-10);
        assert_relative_eq!(result[4].value, 4.0, epsilon = 1e-10);
    }

    #[test]
    fn interpolate_series_no_edge_fill() {
        let samples = make_samples(vec![f64::NAN, f64::NAN, 3.0, 4.0, f64::NAN]);
        let result = interpolate_series(&samples, false);
        // Edges not filled
        assert!(result[0].value.is_nan());
        assert!(result[1].value.is_nan());
        assert_relative_eq!(result[2].value, 3.0, epsilon = 1e-10);
        assert_relative_eq!(result[3].value, 4.0, epsilon = 1e-10);
        assert!(result[4].value.is_nan());
    }

    #[test]
    fn interpolate_series_empty() {
        let result = interpolate_series(&[], true);
        assert!(result.is_empty());
    }

    // ============================================================
    // impute_forward_backward
    // ============================================================

    #[test]
    fn impute_forward_backward_handles_leading() {
        let mut samples = make_samples(vec![f64::NAN, f64::NAN, 3.0, f64::NAN, 5.0]);
        let _map = impute_forward_backward(&mut samples).unwrap();
        // Leading NaN filled backward from 3.0, interior NaN filled forward from 3.0
        assert_relative_eq!(samples[0].value, 3.0);
        assert_relative_eq!(samples[1].value, 3.0);
        assert_relative_eq!(samples[2].value, 3.0);
        assert_relative_eq!(samples[3].value, 3.0);
        assert_relative_eq!(samples[4].value, 5.0);
        // Map contains imputed samples
        assert!(_map.len() >= 3);
    }

    #[test]
    fn impute_forward_backward_handles_trailing() {
        let mut samples = make_samples(vec![1.0, 2.0, f64::NAN, f64::NAN]);
        let _map = impute_forward_backward(&mut samples).unwrap();
        assert_relative_eq!(samples[0].value, 1.0);
        assert_relative_eq!(samples[1].value, 2.0);
        // Trailing NaN forward-filled from 2.0
        assert_relative_eq!(samples[2].value, 2.0);
        assert_relative_eq!(samples[3].value, 2.0);
    }

    #[test]
    fn impute_forward_backward_all_valid() {
        let mut samples = make_samples(vec![1.0, 2.0, 3.0]);
        let map = impute_forward_backward(&mut samples).unwrap();
        assert_eq!(sample_values(&samples), vec![1.0, 2.0, 3.0]);
        // Map is empty — no values were imputed
        assert!(map.is_empty());
    }

    // ============================================================
    // impute_moving_average
    // ============================================================

    #[test]
    fn impute_moving_average_single_gap() {
        let mut samples = make_samples(vec![1.0, 2.0, f64::NAN, 4.0, 5.0]);
        let result = impute_moving_average(&mut samples, 3).unwrap();
        // Window of 3: neighbors are 2.0 and 4.0 → mean = 3.0
        assert_eq!(result.len(), 5);
        assert_relative_eq!(result[2].value, 3.0, epsilon = 1e-10);
        // Original samples are not modified
        assert!(samples[2].value.is_nan());
    }

    #[test]
    fn impute_moving_average_adjacent_gaps() {
        let mut samples = make_samples(vec![1.0, f64::NAN, f64::NAN, 4.0, 5.0, 6.0]);
        let result = impute_moving_average(&mut samples, 3).unwrap();
        // After multi-pass, gaps should be filled
        assert_eq!(result.len(), 6);
        assert!(result[1].value.is_finite());
        assert!(result[2].value.is_finite());
    }

    #[test]
    fn impute_moving_average_rejects_even_window() {
        let mut samples = make_samples(vec![1.0, 2.0, 3.0, 4.0, 5.0]);
        let result = impute_moving_average(&mut samples, 4);
        assert!(matches!(result, Err(ForecastError::InvalidParameter(_))));
    }

    #[test]
    fn impute_moving_average_rejects_zero_window() {
        let mut samples = make_samples(vec![1.0, 2.0, 3.0]);
        let result = impute_moving_average(&mut samples, 0);
        assert!(matches!(result, Err(ForecastError::InvalidParameter(_))));
    }

    #[test]
    fn impute_moving_average_window_larger_than_data() {
        // Window 5 but only 3 data points — still works, just uses available neighbors
        let mut samples = make_samples(vec![1.0, f64::NAN, 3.0]);
        let result = impute_moving_average(&mut samples, 5).unwrap();
        assert_eq!(result.len(), 3);
        assert!(result[1].value.is_finite());
    }

    // ============================================================
    // impute_seasonal
    // ============================================================

    #[test]
    fn impute_seasonal_basic() {
        // Period 3: positions 0,1,2,0,1,2,0,1,2
        let samples = make_samples(vec![
            10.0,
            20.0,
            30.0, // cycle 1
            11.0,
            21.0,
            31.0, // cycle 2
            f64::NAN,
            22.0,
            32.0, // cycle 3 — NaN at position 0
        ]);
        let result = impute_seasonal(&samples, 3).unwrap();
        // Position 0 values: [10.0, 11.0], median = 10.5
        assert_relative_eq!(result[6].value, 10.5, epsilon = 1e-10);
    }

    #[test]
    fn impute_seasonal_insufficient_data() {
        let samples = make_samples(vec![1.0, f64::NAN, 3.0]);
        // Period 4 but only 3 data points
        let result = impute_seasonal(&samples, 4);
        assert!(matches!(
            result,
            Err(ForecastError::InsufficientData { .. })
        ));
    }

    #[test]
    fn impute_seasonal_rejects_zero_period() {
        let samples = make_samples(vec![1.0, 2.0, 3.0, 4.0, 5.0]);
        let result = impute_seasonal(&samples, 0);
        assert!(matches!(result, Err(ForecastError::InvalidParameter(_))));
    }

    #[test]
    fn impute_seasonal_rejects_too_many_missing() {
        // Period 2: bucket 0 has indices [0, 2, 4], bucket 1 has [1, 3, 5]
        let samples = make_samples(vec![f64::NAN, 1.0, f64::NAN, 2.0, f64::NAN, 3.0]);
        // Bucket 0 is 100% NaN (3/3)
        let result = impute_seasonal(&samples, 2);
        assert!(matches!(result, Err(ForecastError::InvalidParameter(_))));
    }

    #[test]
    fn impute_seasonal_all_valid() {
        let samples = make_samples(vec![10.0, 20.0, 30.0, 11.0, 21.0, 31.0]);
        let result = impute_seasonal(&samples, 3).unwrap();
        // No NaN, result equals input
        for (a, b) in samples.iter().zip(result.iter()) {
            assert_relative_eq!(a.value, b.value, epsilon = 1e-10);
        }
    }

    // ============================================================
    // Interpolation policy matches direct interpolate_series call
    // ============================================================

    #[test]
    fn sanitize_interpolate_matches_direct_call() {
        let values = vec![1.0, f64::NAN, f64::NAN, 4.0, 5.0];

        // Via sanitize
        let mut samples_sanitize = make_samples(values.clone());
        sanitize(&mut samples_sanitize, ImputationPolicy::Interpolate).unwrap();

        // Via direct interpolate_series
        let samples_direct = make_samples(values);
        let interpolated = interpolate_series(&samples_direct, true);

        for (a, b) in samples_sanitize.iter().zip(interpolated.iter()) {
            assert_relative_eq!(a.value, b.value, epsilon = 1e-10);
        }
    }
}
