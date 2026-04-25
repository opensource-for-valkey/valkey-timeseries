use crate::common::{Sample, Timestamp};
use crate::promql::functions::types::RollupWindow;
use crate::promql::time::step_times;
use crate::promql::{EvalContext, EvalResult, EvalSamples};
use num_traits::Zero;
use orx_parallel::ParIter;
use orx_parallel::{IntoParIter, IterIntoParIter};

pub(super) fn eval_rollups(
    ctx: &EvalContext,
    range_vec: Vec<EvalSamples>,
    optional_param: Option<f64>,
    rollup_fn: fn(&RollupWindow, Option<f64>) -> f64,
) -> EvalResult<Vec<EvalSamples>> {
    let results = range_vec
        .into_par()
        .map(|series| exec_series_rollup(ctx, series, optional_param, rollup_fn))
        .collect();

    Ok(results)
}

/// Evaluates a rollup function over a given time range and step interval.
///
/// This function is the primary engine for executing PromQL range vector functions (e.g., `rate`, `sum_over_time`).
/// It processes each time series by iterating through the query's time steps and applying the provided `rollup_fn`
/// to the samples falling within the specified `range` (the "range" in range vector).
///
/// ### Arguments
///
/// *   `ctx` - The evaluation context containing query parameters like start/end times, step interval, and lookback delta.
/// *   `series` - The input time series data. Note that `series.values` is reused as a buffer for the output samples
///     to minimize allocations.
/// *   `range` - The duration of the range vector window (e.g., the `5m` in `rate(http_requests_total[5m])`).
/// *   `optional_param` - An optional parameter that some rollup functions may require (e.g., the `phi` in `quantile_over_time`).
/// *   `rollup_fn` - The specific rollup calculation to perform (e.g., sum, average, rate).
///
/// ### Implementation Details
///
/// 1.  **De-interleaving:** It first extracts timestamps and values from the `Sample` structs into two separate
///     parallel vectors. This improves cache locality and enables the compiler to use SIMD instructions for
///     the later rollup calculations.
/// 2.  **Parallel Mapping:** It iterates over the query's time steps in parallel. For each step, it identifies
///     the subset of samples ("window") that fall within the specified `range` before the step's timestamp.
/// 3.  **Rollup Application:** It constructs a `RollupWindow` providing the `rollup_fn` with the relevant
///     slices of values and timestamps, as well as metadata like preceding/following values for functions
///     that require them (e.g., `deriv`, `rate`).
/// 4.  **In-place Collection:** The resulting samples are collected back into the original `series.values`
///     vector, effectively reusing the memory.
pub(super) fn exec_series_rollup(
    ctx: &EvalContext,
    mut series: EvalSamples,
    optional_param: Option<f64>,
    rollup_fn: fn(&RollupWindow, Option<f64>) -> f64,
) -> EvalSamples {
    let window = series.range_ms;
    let lookback = ctx.lookback_delta_ms;
    let step = ctx.step_ms;
    // Instant queries execute with step=0; evaluate exactly once at the matrix range end
    // (which already includes @/offset adjustments).
    let (start_ms, end_ms, step) = if step > 0 {
        (ctx.query_start, ctx.query_end, step)
    } else {
        (series.range_end_ms, series.range_end_ms, 1)
    };

    let prev_step = if step > 0 { step } else { 1 };

    let sample_len = series.values.len();

    let mut values: Vec<f64> = Vec::with_capacity(sample_len);
    let mut timestamps: Vec<i64> = Vec::with_capacity(sample_len);

    for sample in &series.values {
        values.push(sample.value);
        timestamps.push(sample.timestamp);
    }

    series.values.clear();
    let samples = std::mem::take(&mut series.values);

    series.values = step_times(start_ms, end_ms, step)
        .enumerate()
        .iter_into_par()
        .filter_map(move |(idx, t_end)| {
            let t_start = t_end - window;

            // Compute absolute start/end indexes for this step.
            let i = seek_first_timestamp_idx_after(&timestamps, t_start, 0);
            let j = seek_first_timestamp_idx_after(&timestamps, t_end, i);

            let mut rollup_window = RollupWindow {
                window,
                prev_value: f64::NAN,
                prev_timestamp: t_start - prev_step,
                real_prev_value: f64::NAN,
                ..Default::default()
            };

            if i < sample_len && i > 0 && timestamps[i - 1] > rollup_window.prev_timestamp {
                // SAFETY: range is checked above
                unsafe {
                    let prev_idx = i - 1;
                    rollup_window.prev_value = *values.get_unchecked(prev_idx);
                    rollup_window.prev_timestamp = *timestamps.get_unchecked(prev_idx);
                }
            }

            rollup_window.values = &values[i..j];
            rollup_window.timestamps = &timestamps[i..j];

            if rollup_window.values.is_empty() {
                return None;
            }

            if i > 0 {
                let prev_idx = i - 1;

                // SAFETY: i > 0 is checked above
                unsafe {
                    // set real_prev_value if rc.lookback_delta == 0
                    // or if the distance between datapoint in the prev interval and the beginning of this interval
                    // doesn't exceed lookback_delta.
                    // https://github.com/VictoriaMetrics/VictoriaMetrics/issues/894
                    // https://github.com/VictoriaMetrics/VictoriaMetrics/issues/8045
                    // https://github.com/VictoriaMetrics/VictoriaMetrics/issues/8935

                    let mut curr_timestamp = t_start;
                    if !rollup_window.timestamps.is_empty() {
                        curr_timestamp = *rollup_window.timestamps.get_unchecked(0);
                    }

                    // Use the actual timestamp of the preceding sample for the staleness
                    // check, not a synthetic value of 0 or t_start.
                    let prev_ts = *timestamps.get_unchecked(prev_idx);

                    if lookback.is_zero() || (curr_timestamp - prev_ts) < lookback {
                        let prev_value = values.get_unchecked(prev_idx);
                        rollup_window.real_prev_value = *prev_value;
                    }
                }
            }

            rollup_window.real_next_value = if j < values.len() {
                values[j]
            } else {
                f64::NAN
            };

            rollup_window.curr_timestamp = t_end;
            rollup_window.idx = idx;

            // Call the wrapped function via a reference to the inner Fn.
            let value = rollup_fn(&rollup_window, optional_param);
            Some(Sample {
                value,
                timestamp: rollup_window.curr_timestamp,
            })
        })
        .collect_into(samples);

    series
}

fn seek_first_timestamp_idx_after(
    timestamps: &[Timestamp],
    seek_timestamp: Timestamp,
    n_hint: usize,
) -> usize {
    let count = timestamps.len();
    if count == 0 || timestamps[0] > seek_timestamp {
        return 0;
    }

    let start_idx = n_hint.saturating_sub(2).min(count - 1);
    let end_idx = (n_hint + 2).min(count);

    let slice_start = if timestamps[start_idx] <= seek_timestamp {
        start_idx
    } else {
        0
    };

    let slice_end = if end_idx < count && timestamps[end_idx] > seek_timestamp {
        end_idx
    } else {
        count
    };

    let slice = &timestamps[slice_start..slice_end];

    if slice.len() < 32 {
        slice
            .iter()
            .position(|&t| t > seek_timestamp)
            .map_or(slice.len(), |pos| pos)
    } else {
        match slice.binary_search(&(seek_timestamp + 1)) {
            Ok(pos) | Err(pos) => pos,
        }
    }
        .saturating_add(slice_start)
}

/// An alternate implementation of rollup evaluation that operates directly on `Sample` objects without de-interleaving
/// into separate vectors. It can be used for testing or as a fallback for simpler rollup functions that don't benefit
/// from vectorization like `absent_over_time` and `count_over_time`
pub(super) fn eval_rollups_basic<F>(
    ctx: &EvalContext,
    series_data: Vec<EvalSamples>,
    f: F,
) -> Vec<EvalSamples>
where
    F: Fn(&[Sample]) -> f64 + Sync,
{
    let _lookback_delta = ctx.lookback_delta_ms;
    let step_ms = ctx.step_ms;
    let range_ms = ctx.query_end - ctx.query_start;
    let end_ms = ctx.query_end;
    let eval_timestamps: Vec<i64> = if step_ms > 0 {
        step_times(ctx.query_start, ctx.query_end, step_ms).collect()
    } else {
        // For instant eval, use each series' effective range end (after @/offset).
        Vec::new()
    };

    series_data
        .into_par()
        .filter_map(|sample| {
            if sample.values.is_empty() {
                return None;
            }

            let timestamps = if step_ms > 0 {
                eval_timestamps.clone()
            } else {
                vec![sample.range_end_ms]
            };

            let step_samples: Vec<Sample> = timestamps
                .into_iter()
                .iter_into_par()
                .filter_map(|current_step_ms| {
                    // Use the series' explicit range window, not the lookback delta.
                    // The lookback delta is for instant vector staleness; *_over_time
                    // functions must use the declared range from the query (e.g. [5m]).
                    let lookback_start_ms = current_step_ms - sample.range_ms;
                    let i = sample
                        .values
                        .partition_point(|s| s.timestamp <= lookback_start_ms);
                    let j = sample
                        .values
                        .partition_point(|s| s.timestamp <= current_step_ms);
                    let window_samples = &sample.values[i..j];

                    if window_samples.is_empty() {
                        return None;
                    }

                    let value = f(window_samples);
                    Some(Sample {
                        value,
                        timestamp: current_step_ms,
                    })
                })
                .collect();

            if step_samples.is_empty() {
                None
            } else {
                Some(EvalSamples {
                    values: step_samples,
                    labels: sample.labels,
                    drop_name: false,
                    range_ms,
                    range_end_ms: end_ms,
                })
            }
        })
        .collect()
}
