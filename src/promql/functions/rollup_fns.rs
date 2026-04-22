use crate::common::math::{kahan_avg, kahan_std_dev, kahan_sum, kahan_variance, quantile};
use crate::common::Sample;
use crate::promql::functions::rollup_window::{eval_rollups_basic, exec_rollups, RollupWindow};
use crate::promql::functions::utils::expect_exact_arg_count;
use crate::promql::functions::PromQLArg;
use crate::promql::{EvalContext, EvalResult, EvaluationError, ExprResult};
use crate::promql::functions::PromQLFunction;
// https://github.com/VictoriaMetrics/VictoriaMetrics/blob/master/app/vmselect/promql/rollup.go

const NAN: f64 = f64::NAN;


pub(super) fn exec_rollup_fn(name: &str, mut args: Vec<PromQLArg>, ctx: &EvalContext, f: fn(&RollupWindow) -> f64) -> EvalResult<ExprResult> {
    expect_exact_arg_count(name, 1, args.len())?;
    let range = args.swap_remove(0).into_range_vector()?;
    let rollups = exec_rollups(ctx, range, f)?;
    Ok(ExprResult::RangeVector(rollups))
}

pub(super) fn exec_basic_rollup_fn(name: &str, mut args: Vec<PromQLArg>, ctx: &EvalContext, f: fn(&[Sample]) -> f64) -> EvalResult<ExprResult> {
    expect_exact_arg_count(name, 1, args.len())?;
    let range = args.swap_remove(0).into_range_vector()?;
    let rollups = eval_rollups_basic(ctx, range, f);
    Ok(ExprResult::RangeVector(rollups))
}

macro_rules! make_rollup_function {
    ( $type_name: ident, $name: expr, $rf: expr) => {
        #[derive(Copy, Clone, Default)]
        pub(in crate::promql) struct $type_name;

        impl $type_name {
            pub fn new() -> Self {
                Self
            }
        }

        impl PromQLFunction for $type_name {
            fn apply(&self, _arg: PromQLArg, _eval_timestamp_ms: i64) -> EvalResult<ExprResult> {
                Err(EvaluationError::ArgumentError(format!(
                    "invalid invocation of rollup function '{}'",
                    $name
                )))
            }

            fn apply_call(&self, args: Vec<PromQLArg>, ctx: &EvalContext) -> EvalResult<ExprResult> {
                exec_rollup_fn($name, args, ctx, $rf)
            }
        }
    };
}


make_rollup_function!(RollupAvgFunction, "avg_over_time", rollup_avg);
make_rollup_function!(RollupMinFunction, "min_over_time", rollup_min);
make_rollup_function!(RollupMadFunction, "mad_over_time", rollup_mad);
make_rollup_function!(RollupMaxFunction, "max_over_time", rollup_max);
make_rollup_function!(RollupSumFunction, "sum_over_time", rollup_sum);
make_rollup_function!(RollupTminFunction, "ts_of_min_over_time", rollup_ts_of_min);
make_rollup_function!(RollupTmaxFunction, "ts_of_max_over_time", rollup_ts_of_max);
make_rollup_function!(RollupStddevFunction, "stddev_over_time", rollup_stddev);
make_rollup_function!(RollupStdvarFunction, "stdvar_over_time", rollup_stdvar);


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

fn rollup_avg(rfa: &RollupWindow) -> f64 {
    kahan_avg(rfa.values)
}

fn rollup_min(rfa: &RollupWindow) -> f64 {
    let mut min = rfa.values[0];

    for &cur in rfa.values.iter().skip(1) {
        if cur < min || min.is_nan() {
            min = cur;
        }
    }
    min
}

fn rollup_mad(rfa: &RollupWindow) -> f64 {
    let mut values = rfa.values.to_vec();

    let median = quantile(&mut values, 0.5);

    // reuse values vec for deviations to avoid extra allocation
    for value in values.iter_mut() {
        *value = (*value - median).abs();
    }

    quantile(&mut values, 0.5)
}


/// max with prometheus semantics
fn rollup_max(rfa: &RollupWindow) -> f64 {
    let mut max = rfa.values[0];
    for &cur in rfa.values.iter().skip(1) {
        if cur > max || max.is_nan() {
            max = cur;
        }
    }
    max
}

fn rollup_ts_of_min(rfa: &RollupWindow) -> f64 {
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

fn rollup_ts_of_max(rfa: &RollupWindow) -> f64 {
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


fn rollup_sum(rfa: &RollupWindow) -> f64 {
    kahan_sum(rfa.values)
}

fn rollup_stddev(rfa: &RollupWindow) -> f64 {
    kahan_std_dev(rfa.values)
}

fn rollup_stdvar(rfa: &RollupWindow) -> f64 {
    kahan_variance(rfa.values)
}


fn rollup_present(samples: &[Sample]) -> f64 {
    if !samples.is_empty() {
        1.0
    } else {
        0.0
    }
}

fn rollup_count(samples: &[Sample]) -> f64 {
    samples.len() as f64
}

fn rollup_tfirst(samples: &[Sample]) -> f64 {
    // Safety: the caller ensures !samples.is_empty()
    samples[0].timestamp as f64 / 1e3_f64
}

fn rollup_tlast(samples: &[Sample]) -> f64 {
    // Safety: the caller ensures !samples.is_empty()
    samples[samples.len()].timestamp as f64 / 1e3_f64
}

fn rollup_first(samples: &[Sample]) -> f64 {
    // Safety: the caller ensures !samples.is_empty()
    samples[0].value
}

fn rollup_last(samples: &[Sample]) -> f64 {
    // Safety: the caller ensures !samples.is_empty()
    samples[samples.len() - 1].value
}
