use crate::common::Sample;
use crate::promql::functions::{PromQLArg, PromQLFunction};
use crate::promql::{EvalResult, ExprResult};

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

fn extrapolated_delta(
    samples: &[Sample],
    window: Option<(i64, i64)>,
    is_counter: bool,
    is_rate: bool,
    units_per_second: i64,
) -> Option<f64> {
    if samples.len() < 2 {
        return None;
    }

    let (range_start, range_end) = window?;
    let last = samples.last()?;
    let first = samples.first()?;

    let first_ts = first.timestamp;
    let first_value = first.value;
    let last_ts = last.timestamp;

    let sampled_interval = last.timestamp.saturating_sub(first.timestamp);
    if sampled_interval <= 0 {
        return None;
    }

    let mut result = if is_counter {
        counter_increase_value(samples)?
    } else {
        sample_delta_value(samples)?
    };

    let average_duration_between_samples = sampled_interval as f64 / (samples.len() - 1) as f64;
    let mut duration_to_start = first_ts.saturating_sub(range_start) as f64;
    let duration_to_end = range_end.saturating_sub(last_ts) as f64;

    if is_counter && result > 0.0 && first_value >= 0.0 {
        let duration_to_zero = sampled_interval as f64 * (first_value / result);
        if duration_to_zero < duration_to_start {
            duration_to_start = duration_to_zero;
        }
    }

    let extrapolation_threshold = average_duration_between_samples * 1.1;
    let mut extrapolated_interval = sampled_interval as f64;

    if duration_to_start < extrapolation_threshold {
        extrapolated_interval += duration_to_start;
    } else {
        extrapolated_interval += average_duration_between_samples / 2.0;
    }

    if duration_to_end < extrapolation_threshold {
        extrapolated_interval += duration_to_end;
    } else {
        extrapolated_interval += average_duration_between_samples / 2.0;
    }

    result *= extrapolated_interval / sampled_interval as f64;

    if is_rate {
        let range_duration = range_end.saturating_sub(range_start);
        if range_duration <= 0 {
            return None;
        }
        result /= range_duration as f64 / units_per_second as f64;
    }

    Some(result)
}
