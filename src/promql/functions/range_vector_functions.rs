use crate::common::Sample;
use crate::common::math::{kahan_inc, quantile};
use crate::promql::functions::types::{PromQLArg, PromQLFunction};
use crate::promql::functions::utils::{
    avg_kahan, change_below_tolerance, exact_arity_error, expect_range_vector, expect_scalar,
    variance_kahan,
};
use crate::promql::{EvalResult, EvalSample, EvalSamples, ExprResult};
use orx_parallel::IntoParIter;
use orx_parallel::ParIter;

/// Generic aggregator for range vector functions.
///
/// Invariant:
/// - Each input series is reduced to a single output sample at eval_timestamp_ms.
/// - Empty series are skipped (matching Prometheus behavior).
/// - Aggregation function `f` must implement PromQL float semantics exactly.
pub(super) fn aggr_over_time<F>(
    samples: Vec<EvalSamples>,
    eval_timestamp_ms: i64,
    f: F,
) -> ExprResult
where
    F: Fn(&[Sample]) -> f64 + Send + Sync,
{
    let vec = samples
        .into_par()
        .filter_map(|series| {
            if series.values.is_empty() {
                None
            } else {
                let value = f(&series.values);
                Some(EvalSample {
                    timestamp_ms: eval_timestamp_ms,
                    value,
                    labels: series.labels,
                    drop_name: series.drop_name,
                })
            }
        })
        .collect::<Vec<_>>();

    ExprResult::InstantVector(vec)
}

/// Rate function: calculates per-second rate of change for range vectors
#[derive(Copy, Clone)]
pub(in crate::promql) struct RateFunction;

impl PromQLFunction for RateFunction {
    fn apply(&self, arg: PromQLArg, eval_timestamp_ms: i64) -> EvalResult<ExprResult> {
        // TODO(rohan): handle counter-resets
        // TODO(rohan): implement extrapolation
        let samples = arg.into_range_vector()?;
        let mut result = Vec::with_capacity(samples.len());

        for sample_series in samples {
            if sample_series.values.len() < 2 {
                continue;
            }

            let first = &sample_series.values[0];
            let last = &sample_series.values[sample_series.values.len() - 1];

            let time_diff_seconds = (last.timestamp - first.timestamp) as f64 / 1000.0;

            if time_diff_seconds <= 0.0 {
                continue;
            }

            let value_diff = last.value - first.value;

            let rate = value_diff / time_diff_seconds;

            let rate = if rate < 0.0 { 0.0 } else { rate };

            result.push(EvalSample {
                timestamp_ms: eval_timestamp_ms,
                value: rate,
                labels: sample_series.labels,
                drop_name: sample_series.drop_name,
            });
        }

        Ok(ExprResult::InstantVector(result))
    }
}

impl Default for RateFunction {
    fn default() -> Self {
        RateFunction
    }
}

/// Sum over time function: sums all sample values in the range
/// Uses Kahan summation for numerical stability
/// TODO: Add histogram support when histogram types are implemented
#[derive(Copy, Clone)]
pub(in crate::promql) struct SumOverTimeFunction;

impl PromQLFunction for SumOverTimeFunction {
    fn apply(&self, arg: PromQLArg, eval_timestamp_ms: i64) -> EvalResult<ExprResult> {
        let samples = arg.into_range_vector()?;
        Ok(aggr_over_time(samples, eval_timestamp_ms, |values| {
            let mut sum = 0.0;
            let mut c = 0.0;
            for sample in values {
                (sum, c) = kahan_inc(sample.value, sum, c);
            }
            // If the sum is infinite, return it directly without compensation
            if sum.is_infinite() { sum } else { sum + c }
        }))
    }
}

impl Default for SumOverTimeFunction {
    fn default() -> Self {
        SumOverTimeFunction
    }
}

/// Average over time function: averages all sample values in the range
/// Uses hybrid approach: direct mean with Kahan summation, switching to incremental mean on overflow
/// TODO: Add histogram support when histogram types are implemented
#[derive(Copy, Clone)]
pub(in crate::promql) struct AvgOverTimeFunction;

impl PromQLFunction for AvgOverTimeFunction {
    fn apply(&self, arg: PromQLArg, eval_timestamp_ms: i64) -> EvalResult<ExprResult> {
        let samples = arg.into_range_vector()?;
        Ok(aggr_over_time(samples, eval_timestamp_ms, avg_kahan))
    }
}

impl Default for AvgOverTimeFunction {
    fn default() -> Self {
        AvgOverTimeFunction
    }
}

// NOTE ON NaN HANDLING:
//
// Prometheus does NOT use simple f64::min/max semantics.
// It uses explicit comparisons to ensure:
//   - Real numbers replace NaN
//   - All-NaN input returns NaN
//
// We mirror that behavior exactly for semantic parity.

fn get_min_sample(values: &[Sample]) -> Sample {
    // this is called in context of aggr_over_time, which ensures values is non-empty,
    // so we can safely access values[0]
    let mut min = values[0];
    let mut min_val = min.value;

    for sample in values.iter().skip(1) {
        let cur = sample.value;
        if cur < min_val || min_val.is_nan() {
            min_val = cur;
            min = *sample;
        }
    }
    min
}

/// Min over time.
///
/// IMPORTANT:
/// We intentionally do NOT use `f64::min` or a fold with +inf.
///
/// Prometheus semantics:
/// - If the first value is NaN and later values are real numbers,
///   NaN is replaced by the first real number.
/// - If all values are NaN, result must remain NaN.
///
/// A naive fold starting from +inf would incorrectly return +inf
/// for all-NaN input. This manual loop preserves exact PromQL behavior.
#[derive(Copy, Clone)]
pub(in crate::promql) struct MinOverTimeFunction;

impl PromQLFunction for MinOverTimeFunction {
    fn apply(&self, arg: PromQLArg, eval_timestamp_ms: i64) -> EvalResult<ExprResult> {
        let samples = arg.into_range_vector()?;
        Ok(aggr_over_time(samples, eval_timestamp_ms, |values| {
            let min = get_min_sample(values);
            min.value
        }))
    }
}

impl Default for MinOverTimeFunction {
    fn default() -> Self {
        MinOverTimeFunction
    }
}

fn get_max_sample(values: &[Sample]) -> Sample {
    let mut max = values[0];
    let mut max_val = max.value;
    for sample in values.iter().skip(1) {
        let cur = sample.value;
        if cur > max_val || max_val.is_nan() {
            max_val = cur;
            max = *sample;
        }
    }
    max
}

/// Max over time.
///
/// IMPORTANT:
/// We intentionally do NOT use `f64::max` or a fold with -inf.
///
/// Prometheus semantics:
/// - NaN is replaced by any subsequent real value.
/// - If all values are NaN, result must remain NaN.
///
/// A naive fold starting from -inf would incorrectly return -inf
/// for all-NaN input. This manual loop guarantees semantic parity
/// with Prometheus.
#[derive(Copy, Clone)]
pub(in crate::promql) struct MaxOverTimeFunction;

impl PromQLFunction for MaxOverTimeFunction {
    fn apply(&self, arg: PromQLArg, eval_timestamp_ms: i64) -> EvalResult<ExprResult> {
        let samples = arg.into_range_vector()?;
        Ok(aggr_over_time(samples, eval_timestamp_ms, |values| {
            let max = get_max_sample(values);
            max.value
        }))
    }
}

impl Default for MaxOverTimeFunction {
    fn default() -> Self {
        MaxOverTimeFunction
    }
}

/// Count over time function: counts the number of samples in the range
#[derive(Copy, Clone)]
pub(in crate::promql) struct CountOverTimeFunction;

impl PromQLFunction for CountOverTimeFunction {
    fn apply(&self, arg: PromQLArg, eval_timestamp_ms: i64) -> EvalResult<ExprResult> {
        let samples = arg.into_range_vector()?;
        Ok(aggr_over_time(samples, eval_timestamp_ms, |values| {
            values.len() as f64
        }))
    }
}

impl Default for CountOverTimeFunction {
    fn default() -> Self {
        CountOverTimeFunction
    }
}

/// Standard deviation over time function: population stddev of all sample values
/// Only operates on float samples; histogram samples are ignored
#[derive(Copy, Clone)]
pub(in crate::promql) struct StddevOverTimeFunction;

impl PromQLFunction for StddevOverTimeFunction {
    fn apply(&self, args: PromQLArg, eval_timestamp_ms: i64) -> EvalResult<ExprResult> {
        let samples = args.into_range_vector()?;
        Ok(aggr_over_time(samples, eval_timestamp_ms, |values| {
            variance_kahan(values).sqrt()
        }))
    }
}

impl Default for StddevOverTimeFunction {
    fn default() -> Self {
        StddevOverTimeFunction
    }
}

/// Standard variance over time function: population variance of all sample values
#[derive(Copy, Clone)]
pub(in crate::promql) struct StdvarOverTimeFunction;

impl PromQLFunction for StdvarOverTimeFunction {
    fn apply(&self, arg: PromQLArg, eval_timestamp_ms: i64) -> EvalResult<ExprResult> {
        let samples = arg.into_range_vector()?;
        Ok(aggr_over_time(samples, eval_timestamp_ms, variance_kahan))
    }
}

impl Default for StdvarOverTimeFunction {
    fn default() -> Self {
        StdvarOverTimeFunction
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
    fn apply_args(&self, args: Vec<PromQLArg>, eval_timestamp_ms: i64) -> EvalResult<ExprResult> {
        if args.len() != 2 {
            return Err(exact_arity_error("quantile_over_time", 2, args.len()));
        }
        let mut iter = args.into_iter();
        let phi_arg = iter.next().unwrap();
        let range_arg = iter.next().unwrap();

        let phi = expect_scalar(phi_arg, "quantiles_over_time", "phi")?;
        let samples = expect_range_vector(range_arg, "quantiles_over_time")?;

        Ok(aggr_over_time(samples, eval_timestamp_ms, |samples| {
            let mut values = samples.iter().map(|s| s.value).collect::<Vec<_>>();

            quantile(&mut values, phi)
        }))
    }
}

///  MAD (mean absolute deviation) over time function
#[derive(Copy, Clone)]
pub(in crate::promql) struct MadOverTimeFunction;

impl PromQLFunction for MadOverTimeFunction {
    fn apply(&self, arg: PromQLArg, eval_timestamp_ms: i64) -> EvalResult<ExprResult> {
        let samples = arg.into_range_vector()?;
        Ok(aggr_over_time(samples, eval_timestamp_ms, |samples| {
            let mut values = samples
                .iter()
                .map(|sample| sample.value)
                .collect::<Vec<_>>();
            let median = quantile(&mut values, 0.5);

            // reuse values vec for deviations to avoid extra allocation
            for value in values.iter_mut() {
                *value = (*value - median).abs();
            }

            quantile(&mut values, 0.5)
        }))
    }
}

impl Default for MadOverTimeFunction {
    fn default() -> Self {
        MadOverTimeFunction
    }
}

/// Returns the number of counter-resets within the provided time range as an instant vector. Any decrease in the value
/// between two consecutive float samples is interpreted as a counter-reset.
#[derive(Copy, Clone)]
pub(in crate::promql) struct ResetsFunction;

impl PromQLFunction for ResetsFunction {
    fn apply(&self, arg: PromQLArg, eval_timestamp_ms: i64) -> EvalResult<ExprResult> {
        let samples = arg.into_range_vector()?;
        Ok(aggr_over_time(samples, eval_timestamp_ms, |values| {
            if values.is_empty() {
                return 0.0;
            }
            let mut n = 0;
            let mut prev_value = values[0].value;
            for sample in values.iter().skip(1) {
                let val = sample.value;
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
        }))
    }
}

impl Default for ResetsFunction {
    fn default() -> Self {
        Self
    }
}

/// Returns the number of times its value has changed within the provided time range as an instant vector.
#[derive(Copy, Clone)]
pub(in crate::promql) struct ChangesFunction;

impl PromQLFunction for ChangesFunction {
    fn apply(&self, arg: PromQLArg, eval_timestamp_ms: i64) -> EvalResult<ExprResult> {
        let samples = arg.into_range_vector()?;
        Ok(aggr_over_time(samples, eval_timestamp_ms, |values| {
            if values.is_empty() {
                return 0.0;
            }
            let mut n = 0;
            let mut prev_value = values[0].value;
            for sample in values.iter().skip(1) {
                let val = sample.value;
                if val != prev_value {
                    if change_below_tolerance(val, prev_value) {
                        // This may be a precision error. See https://github.com/VictoriaMetrics/VictoriaMetrics/issues/767#issuecomment-1650932203
                        continue;
                    }

                    n += 1;
                }
                prev_value = val;
            }

            n as f64
        }))
    }
}

impl Default for ChangesFunction {
    fn default() -> Self {
        Self
    }
}

#[derive(Copy, Clone)]
pub(in crate::promql) struct TSOfMinOverTimeFunction;

impl PromQLFunction for TSOfMinOverTimeFunction {
    fn apply(&self, arg: PromQLArg, eval_timestamp_ms: i64) -> EvalResult<ExprResult> {
        let samples = arg.into_range_vector()?;
        Ok(aggr_over_time(samples, eval_timestamp_ms, |values| {
            let min_sample = get_min_sample(values);
            min_sample.timestamp as f64
        }))
    }

    fn is_experimental(&self) -> bool {
        true
    }
}

#[derive(Copy, Clone)]
pub(in crate::promql) struct TSOfMaxOverTimeFunction;

impl PromQLFunction for TSOfMaxOverTimeFunction {
    fn apply(&self, arg: PromQLArg, eval_timestamp_ms: i64) -> EvalResult<ExprResult> {
        let samples = arg.into_range_vector()?;
        Ok(aggr_over_time(samples, eval_timestamp_ms, |values| {
            let max_sample = get_max_sample(values);
            max_sample.timestamp as f64
        }))
    }

    fn is_experimental(&self) -> bool {
        true
    }
}

#[derive(Copy, Clone)]
pub(in crate::promql) struct FirstOverTimeFunction;

impl PromQLFunction for FirstOverTimeFunction {
    fn apply(&self, arg: PromQLArg, eval_timestamp_ms: i64) -> EvalResult<ExprResult> {
        let samples = arg.into_range_vector()?;
        Ok(aggr_over_time(samples, eval_timestamp_ms, |values| {
            values[0].value
        }))
    }

    fn is_experimental(&self) -> bool {
        true
    }
}

#[derive(Copy, Clone)]
pub(in crate::promql) struct LastOverTimeFunction;

impl PromQLFunction for LastOverTimeFunction {
    fn apply(&self, arg: PromQLArg, eval_timestamp_ms: i64) -> EvalResult<ExprResult> {
        let samples = arg.into_range_vector()?;
        Ok(aggr_over_time(samples, eval_timestamp_ms, |values| {
            values[values.len() - 1].value
        }))
    }
}

/// the timestamp of last sample in the specified interval.
#[derive(Copy, Clone)]
pub(in crate::promql) struct TSLastOverTimeFunction;

impl PromQLFunction for TSLastOverTimeFunction {
    fn apply(&self, arg: PromQLArg, eval_timestamp_ms: i64) -> EvalResult<ExprResult> {
        let samples = arg.into_range_vector()?;
        Ok(aggr_over_time(samples, eval_timestamp_ms, |values| {
            values[values.len() - 1].timestamp as f64 / 1000_f64
        }))
    }

    fn is_experimental(&self) -> bool {
        true
    }
}

/// the timestamp of the first sample in the specified interval.
#[derive(Copy, Clone)]
pub(in crate::promql) struct TSFirstOverTimeFunction;

impl PromQLFunction for TSFirstOverTimeFunction {
    fn apply(&self, arg: PromQLArg, eval_timestamp_ms: i64) -> EvalResult<ExprResult> {
        let samples = arg.into_range_vector()?;
        Ok(aggr_over_time(samples, eval_timestamp_ms, |values| {
            values[0].timestamp as f64 / 1000_f64
        }))
    }
}

#[derive(Copy, Clone)]
pub(in crate::promql) struct PresentOverTimeFunction;

impl PromQLFunction for PresentOverTimeFunction {
    fn apply(&self, arg: PromQLArg, eval_timestamp_ms: i64) -> EvalResult<ExprResult> {
        let samples = arg.into_range_vector()?;
        Ok(aggr_over_time(samples, eval_timestamp_ms, |values| {
            if values.is_empty() { 0.0 } else { 1.0 }
        }))
    }
}

#[derive(Copy, Clone)]
pub(in crate::promql) struct AbsentOverTimeFunction;

impl PromQLFunction for AbsentOverTimeFunction {
    fn apply(&self, arg: PromQLArg, eval_timestamp_ms: i64) -> EvalResult<ExprResult> {
        let samples = arg.into_range_vector()?;
        Ok(aggr_over_time(samples, eval_timestamp_ms, |values| {
            if values.is_empty() { 1.0 } else { 0.0 }
        }))
    }
}
