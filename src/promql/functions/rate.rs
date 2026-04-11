use crate::common::Sample;
use crate::promql::functions::{PromQLArg, PromQLFunction};
use crate::promql::{EvalResult, EvalSample, EvalSamples, ExprResult};

/// Rate function: calculates per-second rate of change for range vectors
#[derive(Copy, Clone)]
pub(in crate::promql) struct RateFunction;

impl PromQLFunction for RateFunction {
    fn apply(&self, arg: PromQLArg, eval_timestamp_ms: i64) -> EvalResult<ExprResult> {
        let samples = arg.into_range_vector()?;
        let mut result = Vec::with_capacity(samples.len());

        for sample_series in samples {
            if let Some(rate) = extrapolated_rate(&sample_series) {
                result.push(EvalSample {
                    timestamp_ms: eval_timestamp_ms,
                    value: rate,
                    labels: sample_series.labels,
                    drop_name: false,
                });
            }
        }

        Ok(ExprResult::InstantVector(result))
    }
}

impl Default for RateFunction {
    fn default() -> Self {
        RateFunction
    }
}

#[derive(Copy, Clone)]
pub(in crate::promql) struct DeltaFunction;

impl PromQLFunction for DeltaFunction {
    fn apply(&self, _arg: PromQLArg, _eval_timestamp_ms: i64) -> EvalResult<ExprResult> {
        todo!()
    }
}

fn counter_increase_value(samples: &[Sample]) -> Option<f64> {
    let mut total = 0.0;
    let mut prev = samples[0].value;
    for Sample {
        timestamp: _,
        value,
    } in &samples[1..]
    {
        if *value >= prev {
            total += value - prev;
        } else {
            total += value;
        }
        prev = *value;
    }
    Some(total)
}

fn sample_delta_value(samples: &[Sample]) -> Option<f64> {
    if samples.len() < 2 {
        return None;
    }
    Some(samples.last()?.value - samples.first()?.value)
}

fn idelta_value(samples: &[(i64, f64)]) -> Option<f64> {
    if samples.len() < 2 {
        return None;
    }
    Some(samples[samples.len() - 1].1 - samples[samples.len() - 2].1)
}

/// Computes counter-reset-corrected increase for a sample series.
/// Walks through samples and accumulates previous values at each reset point.
fn counter_reset_increase(values: &[Sample]) -> f64 {
    let first = &values[0];
    let last = &values[values.len() - 1];
    let mut result = last.value - first.value;
    let mut prev_value = first.value;
    for sample in &values[1..] {
        if sample.value < prev_value {
            result += prev_value;
        }
        prev_value = sample.value;
    }
    result
}

/// Computes extrapolated rate for a series as per Prometheus logic.
pub(super) fn extrapolated_rate(series: &EvalSamples) -> Option<f64> {
    if series.values.len() < 2 {
        return None;
    }

    let range_seconds = series.range_ms as f64 / 1000.0;
    let range_start = series.range_end_ms - series.range_ms;
    let range_end = series.range_end_ms;

    let first = &series.values[0];
    let last = &series.values[series.values.len() - 1];

    let time_diff_seconds = (last.timestamp - first.timestamp) as f64 / 1000.0;

    if time_diff_seconds <= 0.0 {
        return None;
    }

    let mut result = counter_reset_increase(&series.values);

    let num_samples_minus_one = (series.values.len() - 1) as f64;
    let avg_duration_between_samples = time_diff_seconds / num_samples_minus_one;
    let extrapolation_threshold = avg_duration_between_samples * 1.1;

    let mut duration_to_start = (first.timestamp - range_start) as f64 / 1000.0;
    let mut duration_to_end = (range_end - last.timestamp) as f64 / 1000.0;

    if duration_to_start >= extrapolation_threshold {
        duration_to_start = avg_duration_between_samples / 2.0;
    }

    // Avoid extrapolating to negative values by considering the zero point of the counter.
    let mut duration_to_zero = duration_to_start;
    if result > 0.0 && first.value >= 0.0 {
        duration_to_zero = time_diff_seconds * (first.value / result);
    }
    if duration_to_zero < duration_to_start {
        duration_to_start = duration_to_zero;
    }
    if duration_to_end >= extrapolation_threshold {
        duration_to_end = avg_duration_between_samples / 2.0;
    }

    let factor = (time_diff_seconds + duration_to_start + duration_to_end)
        / time_diff_seconds
        / range_seconds;

    result *= factor;
    Some(result)
}
