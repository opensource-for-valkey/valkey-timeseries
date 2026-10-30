use crate::common::Sample;
use crate::promql::functions::rollups::eval_series;
use crate::promql::functions::types::{PromQLArg, PromQLFunction};
use crate::promql::{EvalContext, EvalResult, EvalSamples, ExprResult};

/// Generic aggregator for range vector functions. As opposed to `aggr_over_time`, this operates
/// over the entire range rather than bucketed windows: each input series is reduced to a
/// single output sample at the evaluation timestamp, empty series are skipped, and `f` must
/// implement PromQL float semantics exactly. See [`eval_series`].
pub(super) fn eval_range<F>(
    ctx: &EvalContext,
    series: Vec<EvalSamples>,
    f: F,
) -> EvalResult<ExprResult>
where
    F: Fn(&[Sample]) -> Option<f64> + Sync,
{
    Ok(ExprResult::InstantVector(eval_series(ctx, series, |s| {
        f(&s.values)
    })?))
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

#[cfg(test)]
mod deadline_tests {
    use crate::common::Sample;
    use crate::promql::deadline::{CHECK_EVERY, Deadline};
    use crate::promql::exec::types::EvalLabels;
    use crate::promql::functions::deriv::DerivFunction;
    use crate::promql::functions::holt_winters::DoubleExponentialSmoothingFunction;
    use crate::promql::functions::rate::RateFunction;
    use crate::promql::functions::{PromQLArg, PromQLFunction};
    use crate::promql::{EvalContext, EvalSamples, QueryError};

    /// Series long enough that each one alone crosses a checkpoint.
    fn long_series() -> Vec<EvalSamples> {
        let len = CHECK_EVERY as i64 + 10;
        (0..4)
            .map(|i| EvalSamples {
                values: (0..len)
                    .map(|t| Sample::new(t * 10, t as f64))
                    .collect::<Vec<_>>()
                    .into(),
                labels: EvalLabels::from_pairs(&[("i", &i.to_string())]),
                drop_name: false,
                range_ms: len * 10,
                range_end_ms: len * 10,
            })
            .collect()
    }

    fn passed_ctx() -> EvalContext {
        EvalContext {
            query_start: 0,
            query_end: 0,
            evaluation_ts: 0,
            lookback_delta_ms: 300_000,
            step_ms: 0,
            deadline: Deadline::until(Some(std::time::Instant::now())),
        }
    }

    fn is_timeout(result: crate::promql::EvalResult<crate::promql::ExprResult>) -> bool {
        matches!(result.map_err(QueryError::from), Err(QueryError::Timeout))
    }

    /// Every per-series range driver stops once the deadline has passed: a
    /// long range over many series must not run every kernel to the end.
    #[test]
    fn rate_family_stops_at_a_passed_deadline() {
        let ctx = passed_ctx();
        assert!(is_timeout(
            RateFunction.apply(PromQLArg::RangeVector(long_series()), &ctx)
        ));
        assert!(is_timeout(
            DerivFunction.apply(PromQLArg::RangeVector(long_series()), &ctx)
        ));
        assert!(is_timeout(DoubleExponentialSmoothingFunction.apply_args(
            vec![
                PromQLArg::RangeVector(long_series()),
                PromQLArg::Scalar(0.5),
                PromQLArg::Scalar(0.5),
            ],
            &ctx,
        )));
    }
}
