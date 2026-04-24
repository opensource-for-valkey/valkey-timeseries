use crate::common::math::{kahan_avg, kahan_std_dev, kahan_sum, kahan_variance, quantile};
use crate::common::Sample;
use crate::promql::functions::rollup_window::{eval_rollups_basic, exec_rollups, RollupWindow};
use crate::promql::functions::utils::{exact_arity_error, expect_exact_arg_count, expect_range_vector, expect_scalar};
use crate::promql::functions::PromQLArg;
use crate::promql::functions::PromQLFunction;
use crate::promql::{EvalContext, EvalResult, EvalSample, EvaluationError, ExprResult};
// https://github.com/VictoriaMetrics/VictoriaMetrics/blob/master/app/vmselect/promql/rollup.go


pub(super) fn exec_rollup_fn(
    name: &str,
    mut args: Vec<PromQLArg>,
    ctx: &EvalContext,
    optional_param: Option<f64>,
    f: fn(&RollupWindow, Option<f64>) -> f64,
) -> EvalResult<ExprResult> {
    expect_exact_arg_count(name, 1, args.len())?;
    let range = args.swap_remove(0).into_range_vector()?;
    let rollups = exec_rollups(ctx, range, optional_param, f)?;
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
                exec_rollup_fn($name, args, ctx, None, $rf)
            }
        }
    };
}

macro_rules! basic_rollup_function {
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
                exec_basic_rollup_fn($name, args, ctx, $rf)
            }
        }
    };
}

make_rollup_function!(AvgOverTimeFunction, "avg_over_time", rollup_avg);
make_rollup_function!(MinOverTimeFunction, "min_over_time", rollup_min);
make_rollup_function!(MadOverTimeFunction, "mad_over_time", rollup_mad);
make_rollup_function!(MaxOverTimeFunction, "max_over_time", rollup_max);
make_rollup_function!(SumOverTimeFunction, "sum_over_time", rollup_sum);
make_rollup_function!(TsOfMinOverTimeFunction, "ts_of_min_over_time", rollup_ts_of_min);
make_rollup_function!(TsOfMaxOverTimeFunction, "ts_of_max_over_time", rollup_ts_of_max);
make_rollup_function!(StddevOverTimeFunction, "stddev_over_time", rollup_stddev);
make_rollup_function!(StdvarOverTimeFunction, "stdvar_over_time", rollup_stdvar);

basic_rollup_function!(PresentOverTimeFunction, "present_over_time", rollup_present);
basic_rollup_function!(CountOverTimeFunction, "count_over_time", rollup_count);
basic_rollup_function!(TsOfFirstOverTimeFunction, "ts_of_first_over_time", rollup_tfirst);
basic_rollup_function!(TsOfLastOverTimeFunction, "ts_of_last_over_time", rollup_tlast);
basic_rollup_function!(FirstOverTimeFunction, "first_over_time", rollup_first);
basic_rollup_function!(LastOverTimeFunction, "last_over_time", rollup_last);

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

fn rollup_avg(rfa: &RollupWindow, _param: Option<f64>) -> f64 {
    kahan_avg(rfa.values)
}

/// Min over time.
///
/// IMPORTANT:
/// We intentionally do NOT use `f64::min` or a fold with +inf.
///
/// Prometheus semantics:
/// - If the first value is NaN and later values are real numbers,
///   NaN is replaced by the first real number.
/// - If all values are NaN, the result must remain NaN.
///
/// A naive fold starting from +inf would incorrectly return +inf
/// for all-NaN input. This manual loop preserves exact PromQL behavior.
fn rollup_min(rfa: &RollupWindow, _param: Option<f64>) -> f64 {
    let mut min = rfa.values[0];

    for &cur in rfa.values.iter().skip(1) {
        if cur < min || min.is_nan() {
            min = cur;
        }
    }
    min
}

fn rollup_mad(rfa: &RollupWindow, _param: Option<f64>) -> f64 {
    let mut values = rfa.values.to_vec();

    let median = quantile(&mut values, 0.5);

    // reuse values vec for deviations to avoid extra allocation
    for value in values.iter_mut() {
        *value = (*value - median).abs();
    }

    quantile(&mut values, 0.5)
}


/// Max over time.
///
/// IMPORTANT:
/// We intentionally do NOT use `f64::max` or a fold with -inf.
///
/// Prometheus semantics:
/// - NaN is replaced by any subsequent real value.
/// - If all values are NaN, the result must remain NaN.
///
/// A naive fold starting from -inf would incorrectly return -inf
/// for all-NaN input. This manual loop guarantees semantic parity
/// with Prometheus.
fn rollup_max(rfa: &RollupWindow, _param: Option<f64>) -> f64 {
    let mut max = rfa.values[0];
    for &cur in rfa.values.iter().skip(1) {
        if cur > max || max.is_nan() {
            max = cur;
        }
    }
    max
}

fn rollup_ts_of_min(rfa: &RollupWindow, _param: Option<f64>) -> f64 {
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

fn rollup_ts_of_max(rfa: &RollupWindow, _param: Option<f64>) -> f64 {
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


fn rollup_sum(rfa: &RollupWindow, _param: Option<f64>) -> f64 {
    kahan_sum(rfa.values)
}

fn rollup_stddev(rfa: &RollupWindow, _param: Option<f64>) -> f64 {
    kahan_std_dev(rfa.values)
}

fn rollup_stdvar(rfa: &RollupWindow, _param: Option<f64>) -> f64 {
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


/// `absent_over_time(range-vector)`
///
/// Returns an empty vector if the range vector has any elements (i.e., at least
/// one series with at least one sample in the look-back window), or a
/// single-element instant vector with value `1` and no labels otherwise.
///
/// This matches Prometheus semantics: the function is used to detect when a
/// time series is absent from a given range.
#[derive(Copy, Clone)]
pub(in crate::promql) struct AbsentOverTimeFunction;

impl PromQLFunction for AbsentOverTimeFunction {
    fn apply(&self, arg: PromQLArg, eval_timestamp_ms: i64) -> EvalResult<ExprResult> {
        let series = arg.into_range_vector()?;
        // todo: what labels should the output sample have?
        let has_samples = series.iter().any(|s| !s.values.is_empty());
        if has_samples {
            Ok(ExprResult::InstantVector(vec![]))
        } else {
            Ok(ExprResult::InstantVector(vec![EvalSample {
                timestamp_ms: eval_timestamp_ms,
                value: 1.0,
                labels: Default::default(),
                drop_name: false,
            }]))
        }
    }
}


/// `quantile_over_time(scalar, range-vector)`
///
/// the φ-quantile (0 ≤ φ ≤ 1) of all float samples in the specified interval.
#[derive(Copy, Clone)]
pub(in crate::promql) struct QuantileOverTimeFunction;

impl PromQLFunction for QuantileOverTimeFunction {
    fn apply(&self, _arg: PromQLArg, _eval_timestamp_ms: i64) -> EvalResult<ExprResult> {
        Err(exact_arity_error("quantile_over_time", 2, 0))
    }

    fn apply_call(&self, args: Vec<PromQLArg>, ctx: &EvalContext) -> EvalResult<ExprResult> {
        if args.len() != 2 {
            return Err(exact_arity_error("quantile_over_time", 2, args.len()));
        }
        let mut iter = args.into_iter();
        let phi_arg = iter.next().unwrap();
        let range_arg = iter.next().unwrap();

        let phi = expect_scalar(phi_arg, "quantiles_over_time", "phi")?;
        let range = expect_range_vector(range_arg, "quantiles_over_time")?;

        let rollups = exec_rollups(ctx, range, Some(phi), |samples, phi| {
            let mut values = samples.values.to_vec();
            let phi = phi.unwrap_or(0.5);
            quantile(&mut values, phi)
        })?;

        Ok(ExprResult::RangeVector(rollups))
    }
}