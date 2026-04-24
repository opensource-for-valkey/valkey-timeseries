use crate::common::Sample;
use crate::promql::functions::{PromQLArg, PromQLFunction};
use crate::promql::{EvalResult, EvalSample, EvalSamples, ExprResult};
use orx_parallel::{IntoParIter, ParIter};

#[derive(Clone, Copy, Debug)]
enum RateKind {
    Rate,
    Increase,
    Delta,
}

/// Rate function: calculates per-second rate of change for range vectors
#[derive(Copy, Clone)]
pub(in crate::promql) struct RateFunction;

impl PromQLFunction for RateFunction {
    fn apply(&self, arg: PromQLArg, eval_timestamp_ms: i64) -> EvalResult<ExprResult> {
        calculate_rate(arg, eval_timestamp_ms, RateKind::Rate)
    }
}

#[derive(Copy, Clone)]
pub(in crate::promql) struct DeltaFunction;

impl PromQLFunction for DeltaFunction {
    fn apply(&self, arg: PromQLArg, eval_timestamp_ms: i64) -> EvalResult<ExprResult> {
        calculate_rate(arg, eval_timestamp_ms, RateKind::Delta)
    }
}

#[derive(Copy, Clone)]
pub(in crate::promql) struct IncreaseFunction;

impl PromQLFunction for IncreaseFunction {
    fn apply(&self, arg: PromQLArg, eval_timestamp_ms: i64) -> EvalResult<ExprResult> {
        calculate_rate(arg, eval_timestamp_ms, RateKind::Increase)
    }
}

fn calculate_rate(arg: PromQLArg, eval_timestamp_ms: i64, kind: RateKind) -> EvalResult<ExprResult> {
    let samples = arg.into_range_vector()?;
    let result = samples
        .into_par()
        .filter_map(|sample_series| {
            let Some(value) = extrapolated_rate(&sample_series, kind) else {
                return None;
            };
            Some(EvalSample {
                timestamp_ms: eval_timestamp_ms,
                value,
                labels: sample_series.labels,
                drop_name: false,
            })
        }).collect();

    Ok(ExprResult::InstantVector(result))
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
pub(super) fn extrapolated_rate(
    samples: &EvalSamples,
    kind: RateKind,
) -> Option<f64> {
    if samples.values.len() < 2 {
        return None;
    }

    let range_start = samples.range_end_ms - samples.range_ms;
    let range_end = samples.range_end_ms;
    let range_duration_seconds = samples.range_ms as f64 / 1000.0;

    let samples = &samples.values;
    let count = samples.len();
    let first_sample = &samples[0];
    let last_sample = &samples[count - 1];

    let first_t = first_sample.timestamp;
    let last_t = last_sample.timestamp;

    let is_counter = matches!(kind, RateKind::Rate | RateKind::Increase);
    let is_rate = matches!(kind, RateKind::Rate);

    let mut result = if is_counter {
        counter_increase_value(samples)?
    } else {
        sample_delta_value(samples)?
    };


    // Duration between first/last samples and boundary of range
    let duration_to_start = (first_t - range_start) as f64 / 1000.0;
    let duration_to_end = (range_end - last_t) as f64 / 1000.0;

    let sampled_interval = (last_t - first_t) as f64 / 1000.0;
    let average_duration_between_samples = sampled_interval / (count - 1) as f64;

    let extrapolation_threshold = average_duration_between_samples * 1.1;

    let mut duration_to_start = if duration_to_start >= extrapolation_threshold {
        average_duration_between_samples / 2.0
    } else {
        duration_to_start
    };

    if is_counter {
        // Counters cannot be negative. If we have any slope at all
        // (i.e., result went up), we can extrapolate the zero point
        // of the counter. If the duration to the zero point is shorter
        // than the durationToStart, we take the zero point as the start
        // of the series, thereby avoiding extrapolation to negative
        // counter values.
        let duration_to_zero = if result > 0.0
            && first_sample.value >= 0.0 {
            sampled_interval * (first_sample.value / result)
        } else {
            duration_to_start
        };

        if duration_to_zero < duration_to_start {
            duration_to_start = duration_to_zero;
        }
    }

    let duration_to_end = if duration_to_end >= extrapolation_threshold {
        average_duration_between_samples / 2.0
    } else {
        duration_to_end
    };

    let mut factor = (sampled_interval + duration_to_start + duration_to_end) / sampled_interval;

    if is_rate {
        factor /= range_duration_seconds;
    }

    result *= factor;

    Some(result)
}