//! This module provides the core engine for executing PromQL rollup (range vector) functions.
//!
//! The main entry point is `exec_rollups`, which evaluates a rollup function over a given
//! time range and step interval.
//!
//! ### Optimization Highlights
//!
//! *   **Allocation Efficiency:** To minimize overhead during query evaluation, particularly for
//!     queries with many steps, this module avoids per-step allocations. It performs a single
//!     pass to extract raw timestamps and values into contiguous vectors before starting the
//!     parallel evaluation.
//! *   **Performance & Vectorization:** Rollup functions operate on slices of `f64` values and
//!     `i64` timestamps rather than vectors of `Sample` objects. This layout is significantly
//!     more cache-friendly and allows the compiler to better leverage SIMD auto-vectorization
//!     when calculating aggregates over the rollup windows.
//! *   **Parallel Execution:** Rollup evaluation is performed in parallel using `orx-parallel`,
//!     partitioning the work across the query's time steps.

use crate::common::{Sample, Timestamp};
use crate::promql::time::step_times;
use crate::promql::{EvalContext, EvalSamples};
use num_traits::Zero;
use orx_parallel::ParIter;
use orx_parallel::{IntoParIter, IterIntoParIter};

#[derive(Default, Clone, Debug)]
pub(super) struct RollupWindow<'a> {
    /// The value preceding values if it fits the staleness interval.
    pub(super) prev_value: f64,

    /// The timestamp for prev_value.
    pub(super) prev_timestamp: Timestamp,

    /// Values that fit the window ending at curr_timestamp.
    pub(crate) values: &'a [f64],

    /// Timestamps for values.
    pub(crate) timestamps: &'a [Timestamp],

    /// Real value preceding value
    /// Populated if the preceding value is within the staleness interval.
    pub(super) real_prev_value: f64,

    /// Real value that goes after values.
    pub(crate) real_next_value: f64,

    /// Current timestamp for rollup evaluation.
    pub(super) curr_timestamp: Timestamp,

    /// Index for the currently evaluated point relative to the time range for query evaluation.
    pub(super) idx: usize,

    /// Time window for rollup calculations.
    pub(super) window: i64,
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
/// *   `rollup_fn` - The specific rollup calculation to perform (e.g., sum, average, rate).
///
/// ### Implementation Details
///
/// 1.  **De-interleaving:** It first extracts timestamps and values from the `Sample` structs into two separate
///     parallel vectors. This improves cache locality and enables the compiler to use SIMD instructions for
///     the subsequent rollup calculations.
/// 2.  **Parallel Mapping:** It iterates over the query's time steps in parallel. For each step, it identifies
///     the subset of samples ("window") that fall within the specified `range` before the step's timestamp.
/// 3.  **Rollup Application:** It constructs a `RollupWindow` providing the `rollup_fn` with the relevant
///     slices of values and timestamps, as well as metadata like preceding/following values for functions
///     that require them (e.g., `deriv`, `rate`).
/// 4.  **In-place Collection:** The resulting samples are collected back into the original `series.values`
///     vector, effectively reusing the memory.
pub(super) fn exec_rollups<F>(
    ctx: &EvalContext,
    mut series: EvalSamples,
    rollup_fn: F,
) -> EvalSamples
where
    F: Fn(&RollupWindow) -> f64 + Sync + Clone,
{
    let window = series.range_ms;
    let lookback = ctx.lookback_delta_ms;
    let step = ctx.step_ms;

    let sample_len = series.values.len();

    let mut values: Vec<f64> = Vec::with_capacity(sample_len);
    let mut timestamps: Vec<i64> = Vec::with_capacity(sample_len);

    for (sample, (v, t)) in series
        .values
        .iter()
        .zip(values.iter_mut().zip(timestamps.iter_mut()))
    {
        *v = sample.value;
        *t = sample.timestamp;
    }

    series.values.clear();
    let samples = std::mem::take(&mut series.values);

    series.values = step_times(ctx.query_start, ctx.query_end, step)
        .enumerate()
        .iter_into_par()
        .filter_map(move |(idx, t_end)| {
            let t_start = t_end - window;

            // Compute absolute start/end indexes for this timestamp. We don't rely on
            // shared mutable hints here, so the closure is Clone + Sync-friendly for parallel execution.
            let i = seek_first_timestamp_idx_after(&timestamps, t_start, 0);
            let j = seek_first_timestamp_idx_after(&timestamps, t_end, i);

            let mut rollup_window = RollupWindow {
                window,
                prev_value: f64::NAN,
                prev_timestamp: t_start - step,
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

                    let prev_timestamp = if prev_idx == 0 { 0 } else { t_start };

                    if lookback.is_zero() || (curr_timestamp - prev_timestamp) < lookback {
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

            let value = rollup_fn(&rollup_window);
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

pub(super) fn eval_range_basic<F>(
    series_data: Vec<EvalSamples>,
    ctx: &EvalContext,
    f: F,
) -> Vec<EvalSamples>
where
    F: Fn(&[Sample]) -> f64 + Sync,
{
    let lookback_delta = ctx.lookback_delta_ms;
    let step_ms = ctx.step_ms;
    let range_ms = ctx.query_end - ctx.query_start;
    let end_ms = ctx.query_end;
    let start = ctx.query_start;
    let aligned_start = start - start % step_ms;
    let end = ctx.query_end;

    series_data
        .into_par()
        .filter_map(|sample| {
            if sample.values.is_empty() {
                return None;
            }

            let step_samples: Vec<Sample> = step_times(aligned_start, end, step_ms)
                .iter_into_par()
                .filter_map(|current_step_ms| {
                    let lookback_start_ms = current_step_ms - lookback_delta;
                    let i = sample
                        .values
                        .partition_point(|s| s.timestamp < lookback_start_ms);
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
