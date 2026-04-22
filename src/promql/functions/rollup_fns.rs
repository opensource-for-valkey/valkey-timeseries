use crate::common::math::{kahan_avg, kahan_std_dev, kahan_sum, kahan_variance, quantile};
use crate::promql::functions::rollup_window::RollupWindow;
// https://github.com/VictoriaMetrics/VictoriaMetrics/blob/master/app/vmselect/promql/rollup.go

const NAN: f64 = f64::NAN;

macro_rules! make_factory {
    ( $name: ident, $rf: expr ) => {
        #[inline]
        pub(super) fn $name(_: &[QueryValue]) -> RuntimeResult<RollupHandler> {
            Ok(RollupHandler::wrap($rf))
        }
    };
}

/// Removes resets for rollup functions over counters - see rollupFuncsRemoveCounterResetsAdd comment.
/// It doesn't remove resets between samples with staleNaNs, or samples that exceed maxStalenessInterval
pub(super) fn remove_counter_resets(
    values: &mut [f64],
    timestamps: &[i64],
    max_staleness_interval: i64,
) {
    if values.is_empty() {
        return;
    }

    let mut correction: f64 = 0.0;
    let mut prev_value: f64 = values[0];

    for i in 0..values.len() {
        let v = values[i];

        let d = v - prev_value;

        if d < 0.0 {
            if (-d * 8.0) < prev_value {
                // This is likely a partial counter-reset.
                // See https://github.com/VictoriaMetrics/VictoriaMetrics/issues/2787
                correction += prev_value - v;
            } else {
                correction += prev_value;
            }
        }

        if i > 0 && max_staleness_interval > 0 {
            let gap = timestamps[i] - timestamps[i - 1];
            if gap > max_staleness_interval {
                // Reset correction if gap between samples exceeds staleness interval.
                correction = 0.0;
                prev_value = v;
                continue;
            }
        }

        prev_value = v;
        let new_value = v + correction;
        values[i] = new_value;

        // Check again, there could be a precision error in float operations.
        // SAFETY: the i > 0 check ensures that both indices are valid
        unsafe {
            if i > 0 {
                let prev = *values.get_unchecked(i - 1);
                if new_value < prev {
                    *values.get_unchecked_mut(i) = prev;
                }
            }
        }
    }
}

pub(super) fn rollup_avg(rfa: &RollupWindow) -> f64 {
    kahan_avg(rfa.values)
}

pub(super) fn rollup_min(rfa: &RollupWindow) -> f64 {
    let mut min = rfa.values[0];

    for &cur in rfa.values.iter().skip(1) {
        if cur < min || min.is_nan() {
            min = cur;
        }
    }
    min
}

pub(crate) fn rollup_mad(rfa: &RollupWindow) -> f64 {
    let mut values = rfa.values.to_vec();

    let median = quantile(&mut values, 0.5);

    // reuse values vec for deviations to avoid extra allocation
    for value in values.iter_mut() {
        *value = (*value - median).abs();
    }

    quantile(&mut values, 0.5)
}

/// max with prometheus semantics
pub(super) fn rollup_max(rfa: &RollupWindow) -> f64 {
    let mut max = rfa.values[0];
    for &cur in rfa.values.iter().skip(1) {
        if cur > max || max.is_nan() {
            max = cur;
        }
    }
    max
}

pub(super) fn rollup_tmin(rfa: &RollupWindow) -> f64 {
    let values = rfa.values;
    let mut min_value = values[0];
    let mut min_timestamp = rfa.timestamps[0];
    for (v, ts) in rfa
        .values
        .iter()
        .copied()
        .zip(rfa.timestamps.iter().copied())
    {
        // Get the last timestamp for the minimum value as most users expect.
        if v <= min_value {
            min_value = v;
            min_timestamp = ts;
        }
    }
    min_timestamp as f64 / 1e3_f64
}

pub(super) fn rollup_tmax(rfa: &RollupWindow) -> f64 {
    let mut max_value = rfa.values[0];
    let mut max_timestamp = rfa.timestamps[0];

    for (v, ts) in rfa
        .values
        .iter()
        .copied()
        .zip(rfa.timestamps.iter().copied())
    {
        // Get the last timestamp for the maximum value as most users expect.
        if v >= max_value {
            max_value = v;
            max_timestamp = ts;
        }
    }

    max_timestamp as f64 / 1e3_f64
}

pub(super) fn rollup_tfirst(rfa: &RollupWindow) -> f64 {
    if rfa.timestamps.is_empty() {
        // do not take into account rfa.prev_timestamp, since it may lead
        // to inconsistent results comparing to Prometheus on broken time series
        // with irregular data points.
        return NAN;
    }
    rfa.timestamps[0] as f64 / 1e3_f64
}

pub(super) fn rollup_tlast(rfa: &RollupWindow) -> f64 {
    let timestamps = rfa.timestamps;
    if timestamps.is_empty() {
        // do not take into account rfa.prev_timestamp, since it may lead
        // to inconsistent results comparing to Prometheus on broken time series
        // with irregular data points.
        return NAN;
    }
    timestamps[timestamps.len() - 1] as f64 / 1e3_f64
}

pub(super) fn rollup_sum(rfa: &RollupWindow) -> f64 {
    kahan_sum(rfa.values)
}

pub(super) fn rollup_stddev(rfa: &RollupWindow) -> f64 {
    kahan_std_dev(rfa.values)
}

pub(super) fn rollup_stdvar(rfa: &RollupWindow) -> f64 {
    kahan_variance(rfa.values)
}

#[inline]
fn change_below_tolerance(v: f64, prev_value: f64) -> bool {
    let tolerance = 1e-12 * v.abs();
    (v - prev_value).abs() < tolerance
}

pub(super) fn rollup_changes(rfa: &RollupWindow) -> f64 {
    // Do not take into account rfa.prev_value like Prometheus does.
    let mut prev_value = rfa.values[0];
    let mut n = 0;
    for v in rfa.values.iter().copied().skip(1) {
        if v != prev_value {
            if change_below_tolerance(v, prev_value) {
                // This may be a precision error. See https://github.com/VictoriaMetrics/VictoriaMetrics/issues/767#issuecomment-1650932203
                continue;
            }
            n += 1;
            prev_value = v;
        }
    }

    n as f64
}

pub(super) fn rollup_increases(rfa: &RollupWindow) -> f64 {
    let mut prev_value = rfa.prev_value;
    let mut values = &rfa.values[0..];

    if values.is_empty() {
        if prev_value.is_nan() {
            return NAN;
        }
        return 0.0;
    }

    if prev_value.is_nan() {
        prev_value = values[0];
        values = &values[1..];
    }

    if values.is_empty() {
        return 0.0;
    }

    let mut n = 0;
    for val in values.iter().copied() {
        if val > prev_value {
            if change_below_tolerance(val, prev_value) {
                // This may be a precision error. See https://github.com/VictoriaMetrics/VictoriaMetrics/issues/767#issuecomment-1650932203
                continue;
            }
            n += 1;
        }
        prev_value = val;
    }

    n as f64
}

pub(super) fn rollup_resets(rfa: &RollupWindow) -> f64 {
    let mut values = &rfa.values[0..];
    if values.is_empty() {
        if rfa.prev_value.is_nan() {
            return NAN;
        }
        return 0.0;
    }

    let mut prev_value = rfa.prev_value;
    if prev_value.is_nan() {
        prev_value = values[0];
        values = &values[1..];
    }

    if values.is_empty() {
        return 0.0;
    }

    let mut n = 0;
    for val in values.iter().copied() {
        if val < prev_value {
            if change_below_tolerance(val, prev_value) {
                // This may be a precision error. See https://github.com/VictoriaMetrics/VictoriaMetrics/issues/767#issuecomment-1650932203
                continue;
            }
            n += 1;
        }
        prev_value = val;
    }

    n as f64
}

pub(super) fn rollup_first(rfa: &RollupWindow) -> f64 {
    let values = rfa.values;
    if values.is_empty() {
        // do not take into account rfa.prev_value, since it may lead
        // to inconsistent results comparing to Prometheus on broken time series
        // with irregular data points.
        return NAN;
    }
    values[0]
}
