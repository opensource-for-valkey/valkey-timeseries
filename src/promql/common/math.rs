use crate::common::Sample;

#[inline]
pub(crate) fn min_with_nan(left: f64, right: f64) -> f64 {
    if left.is_nan() || right.is_nan() {
        f64::NAN
    } else if left < right {
        left
    } else {
        right
    }
}

#[inline]
pub(crate) fn max_with_nan(left: f64, right: f64) -> f64 {
    if left.is_nan() || right.is_nan() {
        f64::NAN
    } else if left > right {
        left
    } else {
        right
    }
}

pub(crate) fn linear_regression(
    values: &[f64],
    timestamps: &[i64],
    intercept_time: i64,
) -> (f64, f64) {
    let n = values.len();
    if n == 0 {
        return (f64::NAN, f64::NAN);
    }
    if are_const_values(values) {
        return (values[0], 0.0);
    }

    // See https://en.wikipedia.org/wiki/Simple_linear_regression#Numerical_example
    let mut v_sum: f64 = 0.0;
    let mut t_sum: f64 = 0.0;
    let mut tv_sum: f64 = 0.0;
    let mut tt_sum: f64 = 0.0;

    for (ts, v) in timestamps.iter().zip(values.iter()) {
        let dt = (ts - intercept_time) as f64 / 1e3_f64;
        v_sum += v;
        t_sum += dt;
        tv_sum += dt * v;
        tt_sum += dt * dt
    }

    let mut k: f64 = 0.0;
    let n = n as f64;
    let t_diff = tt_sum - t_sum * t_sum / n;
    if t_diff.abs() >= 1e-6 {
        // Prevent from incorrect division for too small t_diff values.
        k = (tv_sum - t_sum * v_sum / n) / t_diff;
    }
    let v = v_sum / n - k * t_sum / n;
    (v, k)
}

pub(crate) fn are_const_values(values: &[f64]) -> bool {
    if values.len() <= 1 {
        return true;
    }
    let mut v_prev = values[0];
    for v in &values[1..] {
        if *v != v_prev {
            return false;
        }
        v_prev = *v
    }

    true
}

pub(in crate::promql) fn sample_regression(samples: &[Sample]) -> Option<(f64, f64)> {
    if samples.len() < 2 {
        return None;
    }

    let first = samples.first()?;

    let mut all_equal = true;
    let mut v_prev = first.value;

    for sample in samples.iter().skip(1) {
        let v = sample.value;
        if v.is_nan() {
            return Some((f64::NAN, v));
        }
        if v != v_prev {
            all_equal = false;
        }
        v_prev = v;
    }

    if all_equal {
        // A flat line has zero slope and an intercept equal to its value. The
        // return order is `(slope, intercept)` — the same as the general path
        // below and what both callers destructure — so getting it backwards
        // here made `deriv` of a constant return the constant, and
        // `predict_linear` return `value * elapsed` instead of `value`.
        return Some((0.0, first.value));
    }

    let first_ts = first.timestamp;

    let mut count = 0.0;
    let mut sum_x = 0.0;
    let mut sum_y = 0.0;
    let mut sum_xy = 0.0;
    let mut sum_x2 = 0.0;

    for &Sample { timestamp, value } in samples {
        let x = (timestamp - first_ts) as f64 / 1_000f64;
        count += 1.0;
        sum_x += x;
        sum_y += value;
        sum_xy += x * value;
        sum_x2 += x * x;
    }

    let denominator = count * sum_x2 - sum_x * sum_x;
    if denominator == 0.0 {
        return None;
    }

    let slope = (count * sum_xy - sum_x * sum_y) / denominator;
    let intercept = (sum_y - slope * sum_x) / count;
    Some((slope, intercept))
}

pub(crate) fn float_to_int_bounded(f: f64) -> i64 {
    (f as i64).clamp(i64::MIN, i64::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Samples on a 15s grid starting at an arbitrary non-zero instant, so a
    /// regression that leaked absolute timestamps would show up.
    fn grid(values: &[f64]) -> Vec<Sample> {
        values
            .iter()
            .enumerate()
            .map(|(i, v)| Sample {
                timestamp: 1_767_218_400_000 + i as i64 * 15_000,
                value: *v,
            })
            .collect()
    }

    #[test]
    fn a_constant_series_has_zero_slope_and_its_value_as_intercept() {
        // The regression this file exists for. Returning `(value, 0.0)` here
        // made `deriv(up[5m])` answer 1 for a series pinned at 1, and
        // `predict_linear` answer `value * elapsed`.
        let (slope, intercept) = sample_regression(&grid(&[1.0; 20])).unwrap();
        assert_eq!(slope, 0.0);
        assert_eq!(intercept, 1.0);

        let (slope, intercept) = sample_regression(&grid(&[7.5; 4])).unwrap();
        assert_eq!(slope, 0.0);
        assert_eq!(intercept, 7.5);

        // Including zero, where a swap would be invisible.
        let (slope, intercept) = sample_regression(&grid(&[0.0; 4])).unwrap();
        assert_eq!(slope, 0.0);
        assert_eq!(intercept, 0.0);
    }

    #[test]
    fn a_linear_ramp_recovers_its_slope_and_intercept() {
        // 0.5 per second on a 15s grid is 7.5 per sample.
        let values: Vec<f64> = (0..20).map(|i| i as f64 * 7.5).collect();
        let (slope, intercept) = sample_regression(&grid(&values)).unwrap();
        assert!((slope - 0.5).abs() < 1e-12, "slope was {slope}");
        assert!((intercept - 0.0).abs() < 1e-9, "intercept was {intercept}");

        // With an offset, the intercept is the value at the first sample.
        let values: Vec<f64> = (0..20).map(|i| 100.0 + i as f64 * 7.5).collect();
        let (slope, intercept) = sample_regression(&grid(&values)).unwrap();
        assert!((slope - 0.5).abs() < 1e-12);
        assert!(
            (intercept - 100.0).abs() < 1e-9,
            "intercept was {intercept}"
        );
    }

    #[test]
    fn a_descending_series_has_a_negative_slope() {
        let values: Vec<f64> = (0..10).map(|i| 100.0 - i as f64 * 15.0).collect();
        let (slope, _) = sample_regression(&grid(&values)).unwrap();
        assert!((slope + 1.0).abs() < 1e-12, "slope was {slope}");
    }

    #[test]
    fn the_constant_and_general_paths_agree_at_the_boundary() {
        // Two series that differ by one ulp take different code paths but must
        // not produce wildly different answers.
        let flat = sample_regression(&grid(&[5.0; 8])).unwrap();
        let nearly_flat = {
            let mut v = vec![5.0; 8];
            v[7] = 5.0 + f64::EPSILON;
            sample_regression(&grid(&v)).unwrap()
        };
        assert!((flat.0 - nearly_flat.0).abs() < 1e-9, "slopes diverge");
        assert!((flat.1 - nearly_flat.1).abs() < 1e-9, "intercepts diverge");
    }

    #[test]
    fn fewer_than_two_samples_has_no_regression() {
        assert!(sample_regression(&[]).is_none());
        assert!(sample_regression(&grid(&[1.0])).is_none());
    }

    #[test]
    fn a_nan_anywhere_poisons_the_slope() {
        // `rollup_deriv` drops the series on a NaN slope, so this is how a
        // window containing NaN produces no result rather than a wrong one.
        let (slope, _) = sample_regression(&grid(&[1.0, 2.0, f64::NAN, 4.0])).unwrap();
        assert!(slope.is_nan());
    }
}
