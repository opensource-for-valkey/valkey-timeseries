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
        return Some((first.value, 0.0));
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
