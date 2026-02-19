/// Kahan summation increment with Neumaier improvement (1974)
///
/// Performs compensated summation to minimize floating-point rounding errors.
/// The Neumaier variant handles the case where the next term is larger than
/// the running sum, which the original Kahan algorithm (1965) did not address.
///
/// Returns (new_sum, new_compensation)
#[inline(never)]
// Important: do NOT inline.
// Compiler reordering of floating-point operations can cause precision loss.
// This was observed in Prometheus (issue #16714) and we lock the behavior
// to maintain IEEE-754 semantics exactly.
pub(crate) fn kahan_inc(inc: f64, sum: f64, c: f64) -> (f64, f64) {
    let t = sum + inc;

    let new_c = if t.is_infinite() {
        0.0
    } else if sum.abs() >= inc.abs() {
        // Neumaier improvement: swap roles when next term is larger
        c + ((sum - t) + inc)
    } else {
        c + ((inc - t) + sum)
    };

    (t, new_c)
}

pub(crate) fn kahan_sum(values: &[f64]) -> f64 {
    let mut sum = 0.0;
    let mut c = 0.0;
    for &value in values {
        (sum, c) = kahan_inc(value, sum, c);
    }
    if sum.is_infinite() { sum } else { sum + c }
}

/// Average calculation matching Prometheus semantics.
///
/// Strategy:
/// 1. Use Kahan summation for numerical stability.
/// 2. If intermediate sum overflows to ±Inf, switch to incremental mean
///    to avoid poisoning the entire result.
///
/// This mirrors Prometheus' hybrid strategy and prevents overflow-induced
/// divergence while maintaining IEEE-754 parity.
pub(crate) fn kahan_avg(values: &[f64]) -> f64 {
    if values.len() == 1 {
        return values[0];
    }

    let mut sum = values[0];
    let mut c = 0.0;
    let mut mean = 0.0;
    let mut incremental = false;

    for (i, &value) in values.iter().enumerate().skip(1) {
        let count = (i + 1) as f64;

        if !incremental {
            let (new_sum, new_c) = kahan_inc(value, sum, c);
            if !new_sum.is_infinite() {
                sum = new_sum;
                c = new_c;
                continue;
            }

            incremental = true;
            mean = sum / (count - 1.0);
            c /= count - 1.0;
        }

        let q = (count - 1.0) / count;
        (mean, c) = kahan_inc(value / count, q * mean, q * c);
    }

    if incremental {
        mean + c
    } else {
        let count = values.len() as f64;
        sum / count + c / count
    }
}

/// Variance calculation using Welford's online algorithm (1962)
/// with compensated summation for improved numerical stability.
///
/// Algorithm:
///   For each value x:
///     count += 1
///     delta  = x - mean
///     mean  += delta / count
///     delta2 = x - mean
///     M2    += delta * delta2
///   variance = M2 / count   (population variance)
///
/// Enhancement:
///   Kahan compensated summation is applied to the incremental
///   updates of both the running mean and M2 accumulators,
///   reducing floating-point rounding error in long sequences.
///
/// Semantics:
///   - Computes population variance (divides by n)
///   - Matches Prometheus population variance semantics
///
/// NaN handling:
///   - Empty input returns NaN
///   - Single value returns 0.0
///   - NaN values propagate through the calculation
///
/// References:
///   - <https://en.wikipedia.org/wiki/Algorithms_for_calculating_variance#Welford's_online_algorithm>
///   - Prometheus: `promql/functions.go::varianceOverTime`
pub(crate) fn kahan_variance(values: &[f64]) -> f64 {
    if values.is_empty() {
        return f64::NAN;
    }

    let mut count = 0.0;
    let mut mean = 0.0;
    let mut c_mean = 0.0;
    let mut m2 = 0.0;
    let mut c_m2 = 0.0;

    for &value in values {
        count += 1.0;
        let delta = value - (mean + c_mean);
        (mean, c_mean) = kahan_inc(delta / count, mean, c_mean);
        let new_delta = value - (mean + c_mean);
        (m2, c_m2) = kahan_inc(delta * new_delta, m2, c_m2);
    }

    (m2 + c_m2) / count
}

pub(crate) fn kahan_std_dev(values: &[f64]) -> f64 {
    if values.len() == 1 {
        return values[0];
    }
    let variance = kahan_variance(values);
    variance.sqrt()
}

pub fn quantile(values: &mut [f64], phi: f64) -> f64 {
    if values.is_empty() {
        return f64::NAN;
    }
    if phi.is_nan() {
        return f64::NAN;
    }
    if phi < 0.0 {
        return f64::NEG_INFINITY;
    }
    if phi > 1.0 {
        return f64::INFINITY;
    }

    values.sort_by(|a, b| a.total_cmp(b));
    if values.len() == 1 {
        return values[0];
    }

    let rank = phi * (values.len() - 1) as f64;
    let lower = rank.floor() as usize;
    let upper = rank.ceil() as usize;
    if lower == upper {
        return values[lower];
    }

    let weight = rank - lower as f64;
    values[lower] + (values[upper] - values[lower]) * weight
}

/// quantile_sorted calculates the given quantile over a sorted list of values.
///
/// It is expected that values won't contain NaN items.
/// The implementation mimics Prometheus implementation for compatibility's sake.
pub(crate) fn quantile_sorted(phi: f64, values: &[f64]) -> f64 {
    if values.is_empty() || phi.is_nan() {
        return f64::NAN;
    }
    if phi < 0.0 {
        return f64::NEG_INFINITY;
    }
    if phi > 1.0 {
        return f64::INFINITY;
    }
    let n = values.len();
    let rank = phi * (n - 1) as f64;

    let lower_index = std::cmp::max(0, rank.floor() as usize);
    let upper_index = std::cmp::min(n - 1, lower_index + 1);

    let weight = rank - rank.floor();
    values[lower_index] * (1.0 - weight) + values[upper_index] * weight
}

pub(crate) fn median(values: &[f64]) -> f64 {
    quantile_sorted(0.5, values)
}
