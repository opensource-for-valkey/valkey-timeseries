use crate::common::Sample;
use crate::common::threads::IntoParRayon;
use crate::promql::deadline::SERIES_WORK;
use crate::promql::functions::types::{PromQLArg, PromQLFunction};
use crate::promql::{EvalContext, EvalResult, EvalSample, EvalSamples, ExprResult};
use orx_parallel::{Par, ParResult};

/// Generic aggregator for range vector functions. As opposed to `aggr_over_time`, this operates
/// over the entire range rather than bucketed windows.
///
/// Invariant:
/// - Each input series is reduced to a single output sample at eval_timestamp_ms.
/// - Empty series are skipped (matching Prometheus behavior).
/// - Aggregation function `f` must implement PromQL float semantics exactly.
///
/// The deadline is checked per series, weighted by its samples, before `f` runs over them:
/// never inside `f`, whose loop over one series' samples is the one the compiler vectorizes.
pub(super) fn eval_range<F>(
    ctx: &EvalContext,
    series: Vec<EvalSamples>,
    f: F,
) -> EvalResult<ExprResult>
where
    F: Fn(&[Sample]) -> Option<f64> + Send + Sync,
{
    let (deadline, eval_timestamp_ms) = (ctx.deadline, ctx.evaluation_ts);
    let res = series
        .into_par_rayon()
        .map(|series| -> EvalResult<Option<EvalSample>> {
            deadline.checkpoint(series.values.len() + SERIES_WORK)?;
            if series.values.is_empty() {
                return Ok(None);
            }
            let Some(value) = f(&series.values) else {
                return Ok(None);
            };
            Ok(Some(EvalSample {
                timestamp_ms: eval_timestamp_ms,
                value,
                labels: series.labels,
                drop_name: series.drop_name,
            }))
        })
        .into_fallible()
        .filter_map(|sample| sample)
        .collect::<Vec<_>>()?;

    Ok(ExprResult::InstantVector(res))
}

/// Returns the number of counter-resets within the provided time range as an instant vector. Any decrease in the value
/// between two consecutive float samples is interpreted as a counter-reset.
#[derive(Copy, Clone)]
pub(in crate::promql) struct ResetsFunction;

impl PromQLFunction for ResetsFunction {
    fn apply(&self, arg: PromQLArg, ctx: &EvalContext) -> EvalResult<ExprResult> {
        let samples = arg.into_range_vector()?;
        eval_range(ctx, samples, rollup_resets)
    }
}

/// `resets` over one window. Named rather than inline so the pushed-down path
/// reduces a window with the very same function the local path runs.
///
/// Any decrease counts, however small. There is no tolerance here on purpose —
/// see [`rollup_changes`].
///
/// NaN never triggers a reset, because every comparison against NaN is false.
/// That is Prometheus' behaviour too, and it falls out of the same expression
/// rather than needing a case.
pub(in crate::promql) fn rollup_resets(values: &[Sample]) -> Option<f64> {
    if values.is_empty() {
        return Some(0.0);
    }
    let mut n = 0;
    let mut prev_value = values[0].value;
    for sample in values.iter().skip(1) {
        if sample.value < prev_value {
            n += 1;
        }
        prev_value = sample.value;
    }

    Some(n as f64)
}

/// Returns the number of times its value has changed within the provided time range as an instant vector.
#[derive(Copy, Clone)]
pub(in crate::promql) struct ChangesFunction;

impl PromQLFunction for ChangesFunction {
    fn apply(&self, arg: PromQLArg, ctx: &EvalContext) -> EvalResult<ExprResult> {
        let samples = arg.into_range_vector()?;
        eval_range(ctx, samples, rollup_changes)
    }
}

/// `changes` over one window. Named rather than inline so the pushed-down path
/// reduces a window with the very same function the local path runs.
///
/// Two consecutive samples differ if their `f64` values differ — exactly, with
/// no tolerance. This module's Gorilla encoding is lossless for `f64`, so a
/// difference here is always a difference the writer actually put there.
///
/// The one exception is NaN following NaN. `NaN != NaN` is true in IEEE 754, so
/// a run of NaNs would otherwise count as a change at every step; Prometheus
/// special-cases it and so does this.
pub(in crate::promql) fn rollup_changes(values: &[Sample]) -> Option<f64> {
    if values.is_empty() {
        return Some(0.0);
    }
    let mut n = 0;
    let mut prev_value = values[0].value;
    for sample in values.iter().skip(1) {
        let val = sample.value;
        if val != prev_value && !(val.is_nan() && prev_value.is_nan()) {
            n += 1;
        }
        prev_value = val;
    }

    Some(n as f64)
}
