use crate::common::Sample;
use crate::promql::functions::types::{PromQLArg, PromQLFunction};
use crate::promql::functions::utils::change_below_tolerance;
use crate::promql::{EvalResult, EvalSample, EvalSamples, ExprResult};
use orx_parallel::IntoParIter;
use orx_parallel::ParIter;

/// Generic aggregator for range vector functions. As opposed to `aggr_over_time`, this operates
/// over the entire range rather than bucketed windows.
///
/// Invariant:
/// - Each input series is reduced to a single output sample at eval_timestamp_ms.
/// - Empty series are skipped (matching Prometheus behavior).
/// - Aggregation function `f` must implement PromQL float semantics exactly.
pub(super) fn eval_range<F>(
    series: Vec<EvalSamples>,
    eval_timestamp_ms: i64,
    f: F,
) -> ExprResult
where
    F: Fn(&[Sample]) -> Option<f64> + Send + Sync,
{
    let res = series
        .into_par()
        .filter_map(|series| {
            if series.values.is_empty() {
                return None;
            }
            let value = f(&series.values)?;
            Some(EvalSample {
                timestamp_ms: eval_timestamp_ms,
                value,
                labels: series.labels,
                drop_name: series.drop_name,
            })
        })
        .collect::<Vec<_>>();

    ExprResult::InstantVector(res)
}

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


/// Returns the number of counter-resets within the provided time range as an instant vector. Any decrease in the value
/// between two consecutive float samples is interpreted as a counter-reset.
#[derive(Copy, Clone)]
pub(in crate::promql) struct ResetsFunction;

impl PromQLFunction for ResetsFunction {
    fn apply(&self, arg: PromQLArg, eval_timestamp_ms: i64) -> EvalResult<ExprResult> {
        let samples = arg.into_range_vector()?;
        Ok(eval_range(samples, eval_timestamp_ms, |values| {
            if values.is_empty() {
                return Some(0.0);
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

            Some(n as f64)
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
        Ok(eval_range(samples, eval_timestamp_ms, |values| {
            if values.is_empty() {
                return Some(0.0);
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

            Some(n as f64)
        }))
    }
}