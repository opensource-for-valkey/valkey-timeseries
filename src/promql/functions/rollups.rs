//! Window construction and evaluation for PromQL range-vector (rollup) functions.
//!
//! A rollup reduces the samples inside a window `(t - range, t]` of one series to
//! a single value. Two properties of that definition drive everything here:
//!
//! * **The window grid is the caller's, not ours.** A range query is driven one
//!   step at a time by [`crate::promql::engine::evaluate_range`], which hands the
//!   evaluator a context whose `evaluation_ts` *is* the step. The matrix selector
//!   under the call then loads exactly one window's worth of samples, so a rollup
//!   must produce exactly one value — for the window the selector actually
//!   fetched. Re-deriving a step grid inside the rollup and picking a survivor
//!   from it reads windows the selector never loaded.
//! * **An empty window yields no output, not NaN.** A `(series, step)` pair with
//!   no samples is absent from the result, which is how the caller distinguishes
//!   "nothing to roll up" from "rolled up to NaN".
//!
//! [`rollup_series_over_grid`] takes a list of window ends so that a caller with
//! the whole grid in hand — the shard side of rollup push-down — evaluates every
//! step in one pass over the series, while the local path passes a single window
//! end. Both go through the same window construction, which is the point.

use crate::common::{Sample, Timestamp};
use crate::promql::functions::types::RollupWindow;
use crate::promql::{EvalContext, EvalResult, EvalSample, EvalSamples};
use num_traits::Zero;
use orx_parallel::IntoParIter;
use orx_parallel::ParIter;

/// Evaluate `rollup_fn` over each series' window, one value per series.
///
/// The window is the one the matrix selector loaded: it ends at
/// [`EvalSamples::range_end_ms`], which already carries any `@`/`offset`
/// adjustment. The output is stamped with the query's evaluation timestamp
/// instead, so a shifted selector still reports at the step the client asked
/// for — the same rule [`crate::promql::functions::rate`] follows.
///
/// Series whose window holds no samples are dropped rather than emitted as NaN.
pub(super) fn eval_rollups(
    ctx: &EvalContext,
    range_vec: Vec<EvalSamples>,
    optional_param: Option<f64>,
    rollup_fn: fn(&RollupWindow, Option<f64>) -> f64,
) -> EvalResult<Vec<EvalSample>> {
    let results = range_vec
        .into_par()
        .filter_map(|series| exec_series_rollup(ctx, series, optional_param, rollup_fn))
        .collect();

    Ok(results)
}

/// Evaluate one series' rollup at the end of the window the selector loaded.
fn exec_series_rollup(
    ctx: &EvalContext,
    series: EvalSamples,
    optional_param: Option<f64>,
    rollup_fn: fn(&RollupWindow, Option<f64>) -> f64,
) -> Option<EvalSample> {
    let value = rollup_series_over_grid(
        &series.values,
        series.range_ms,
        ctx.lookback_delta_ms,
        ctx.step_ms,
        [series.range_end_ms],
        optional_param,
        rollup_fn,
    )
    .pop()?;

    Some(EvalSample {
        // The window ends at `range_end_ms`, but the sample belongs to the step.
        timestamp_ms: ctx.evaluation_ts,
        value: value.value,
        labels: series.labels,
        drop_name: series.drop_name,
    })
}

/// Evaluate `rollup_fn` for one series over the windows ending at each of
/// `window_ends`, in the order given.
///
/// Each output sample is stamped with its own window end, and windows holding no
/// samples produce no output at all — so the returned vector is sparse and its
/// length is *not* the number of window ends.
///
/// `samples` must be sorted ascending by timestamp (the storage invariant).
/// `step_ms` only bounds how far before the window start a preceding sample may
/// sit and still be offered to `rollup_fn` as `prev_value`; pass 0 for a
/// single-instant evaluation.
pub(in crate::promql) fn rollup_series_over_grid(
    samples: &[Sample],
    window_ms: i64,
    lookback_ms: i64,
    step_ms: i64,
    window_ends: impl IntoIterator<Item = Timestamp>,
    optional_param: Option<f64>,
    rollup_fn: fn(&RollupWindow, Option<f64>) -> f64,
) -> Vec<Sample> {
    if samples.is_empty() {
        return Vec::new();
    }

    // De-interleave into parallel arrays: better cache locality, and it lets the
    // compiler use SIMD for the rollup arithmetic.
    let mut values: Vec<f64> = Vec::with_capacity(samples.len());
    let mut timestamps: Vec<Timestamp> = Vec::with_capacity(samples.len());
    for sample in samples {
        values.push(sample.value);
        timestamps.push(sample.timestamp);
    }

    let prev_step = if step_ms > 0 { step_ms } else { 1 };
    let mut cursor = 0usize;

    window_ends
        .into_iter()
        .enumerate()
        .filter_map(|(idx, t_end)| {
            let t_start = t_end - window_ms;

            // Window ends are non-decreasing in every caller, so the search for
            // the window start can resume from the previous window's start.
            let i = seek_first_timestamp_idx_after(&timestamps, t_start, cursor);
            let j = seek_first_timestamp_idx_after(&timestamps, t_end, i);
            cursor = i;

            if i >= j {
                // No samples in this window: the (series, step) pair is absent
                // from the result rather than present with a NaN value.
                return None;
            }

            let mut window = RollupWindow {
                window: window_ms,
                prev_value: f64::NAN,
                prev_timestamp: t_start - prev_step,
                real_prev_value: f64::NAN,
                values: &values[i..j],
                timestamps: &timestamps[i..j],
                curr_timestamp: t_end,
                idx,
                ..Default::default()
            };

            if i > 0 {
                let prev_idx = i - 1;
                let prev_ts = timestamps[prev_idx];

                if prev_ts > window.prev_timestamp {
                    window.prev_value = values[prev_idx];
                    window.prev_timestamp = prev_ts;
                }

                // Set real_prev_value if lookback_delta == 0, or if the distance
                // between the datapoint in the previous interval and the start of
                // this one does not exceed lookback_delta.
                // https://github.com/VictoriaMetrics/VictoriaMetrics/issues/894
                // https://github.com/VictoriaMetrics/VictoriaMetrics/issues/8045
                // https://github.com/VictoriaMetrics/VictoriaMetrics/issues/8935
                //
                // Use the actual timestamp of the preceding sample for the
                // staleness check, not a synthetic value of 0 or t_start.
                let curr_timestamp = window.timestamps[0];
                if lookback_ms.is_zero() || (curr_timestamp - prev_ts) < lookback_ms {
                    window.real_prev_value = values[prev_idx];
                }
            }

            window.real_next_value = if j < values.len() {
                values[j]
            } else {
                f64::NAN
            };

            Some(Sample {
                value: rollup_fn(&window, optional_param),
                timestamp: t_end,
            })
        })
        .collect()
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

/// An alternate implementation of rollup evaluation that operates directly on
/// `Sample` objects without de-interleaving into separate vectors. It is used by
/// the rollups that read whole samples rather than values — `count_over_time`,
/// `first_over_time`, `ts_of_last_over_time` — and that gain nothing from
/// vectorization.
///
/// Window selection and the empty-window rule are the same as
/// [`rollup_series_over_grid`]; see that function for why the grid belongs to the
/// caller.
pub(super) fn eval_rollups_basic<F>(
    ctx: &EvalContext,
    series_data: Vec<EvalSamples>,
    f: F,
) -> Vec<EvalSample>
where
    F: Fn(&[Sample]) -> f64 + Sync,
{
    series_data
        .into_par()
        .filter_map(|series| {
            let window = window_samples(&series.values, series.range_end_ms, series.range_ms)?;

            Some(EvalSample {
                timestamp_ms: ctx.evaluation_ts,
                value: f(window),
                labels: series.labels,
                drop_name: series.drop_name,
            })
        })
        .collect()
}

/// The samples of `series` inside `(window_end - window_ms, window_end]`, or
/// `None` when that window is empty.
pub(in crate::promql) fn window_samples(
    samples: &[Sample],
    window_end: Timestamp,
    window_ms: i64,
) -> Option<&[Sample]> {
    let window_start = window_end - window_ms;
    let i = samples.partition_point(|s| s.timestamp <= window_start);
    let j = samples.partition_point(|s| s.timestamp <= window_end);
    (i < j).then(|| &samples[i..j])
}

#[cfg(test)]
mod tests {
    use super::*;

    fn samples(points: &[(i64, f64)]) -> Vec<Sample> {
        points
            .iter()
            .map(|&(timestamp, value)| Sample { timestamp, value })
            .collect()
    }

    fn count(window: &RollupWindow, _param: Option<f64>) -> f64 {
        window.values.len() as f64
    }

    fn sum(window: &RollupWindow, _param: Option<f64>) -> f64 {
        window.values.iter().sum()
    }

    /// One pass over the series must produce the same windows as evaluating each
    /// window end on its own — this is what lets the shard side evaluate a whole
    /// grid while the coordinator evaluates one step at a time.
    #[test]
    fn test_grid_matches_per_window_evaluation() {
        let series = samples(&[
            (10, 1.0),
            (20, 2.0),
            (30, 3.0),
            (40, 4.0),
            (50, 5.0),
            (60, 6.0),
        ]);
        let ends: Vec<Timestamp> = (0..=80).step_by(5).collect();

        for window_ms in [1, 10, 25, 100] {
            let grid =
                rollup_series_over_grid(&series, window_ms, 0, 5, ends.iter().copied(), None, sum);
            let one_at_a_time: Vec<Sample> = ends
                .iter()
                .flat_map(|&end| {
                    rollup_series_over_grid(&series, window_ms, 0, 5, [end], None, sum)
                })
                .collect();
            assert_eq!(grid, one_at_a_time, "window_ms={window_ms}");
        }
    }

    /// The window is half-open: `(end - range, end]`.
    #[test]
    fn test_window_bounds_are_half_open() {
        let series = samples(&[(10, 1.0), (20, 2.0), (30, 3.0)]);

        // Window (10, 30] excludes the sample at exactly 10.
        let out = rollup_series_over_grid(&series, 20, 0, 0, [30], None, count);
        assert_eq!(
            out,
            vec![Sample {
                timestamp: 30,
                value: 2.0
            }]
        );

        // Window (9, 29] includes 10 and 20 but not 30.
        let out = rollup_series_over_grid(&series, 20, 0, 0, [29], None, count);
        assert_eq!(
            out,
            vec![Sample {
                timestamp: 29,
                value: 2.0
            }]
        );
    }

    /// An empty window emits nothing; it never emits NaN. The output is sparse.
    #[test]
    fn test_empty_windows_are_omitted() {
        let series = samples(&[(10, 1.0), (100, 2.0)]);
        let ends: Vec<Timestamp> = vec![10, 40, 70, 100];

        let out = rollup_series_over_grid(&series, 10, 0, 30, ends.iter().copied(), None, count);
        assert_eq!(
            out,
            vec![
                Sample {
                    timestamp: 10,
                    value: 1.0
                },
                Sample {
                    timestamp: 100,
                    value: 1.0
                },
            ],
            "steps 40 and 70 have no samples and must be absent, not NaN"
        );

        assert!(rollup_series_over_grid(&[], 10, 0, 0, [10], None, count).is_empty());
    }

    /// A NaN *value* is a real output; only an empty window is absent.
    #[test]
    fn test_nan_value_is_distinct_from_missing_window() {
        let series = samples(&[(10, f64::NAN)]);
        let out = rollup_series_over_grid(&series, 10, 0, 0, [10, 100], None, sum);
        assert_eq!(out.len(), 1, "only the window holding the NaN sample emits");
        assert_eq!(out[0].timestamp, 10);
        assert!(out[0].value.is_nan());
    }

    /// The resumable cursor must not skip samples when successive windows
    /// overlap heavily (range >> step), the shape push-down targets.
    #[test]
    fn test_overlapping_windows_keep_full_range() {
        let series: Vec<Sample> = (1..=100)
            .map(|i| Sample {
                timestamp: i * 10,
                value: 1.0,
            })
            .collect();
        let ends: Vec<Timestamp> = (100..=1000).step_by(100).collect();

        let out = rollup_series_over_grid(&series, 500, 0, 100, ends.iter().copied(), None, count);

        // Window (end-500, end] holds one sample per 10ms, capped by the series
        // start at t=10.
        let expected: Vec<f64> = ends
            .iter()
            .map(|&end| ((end.min(1000) - (end - 500).max(0)) / 10) as f64)
            .collect();
        let got: Vec<f64> = out.iter().map(|s| s.value).collect();
        assert_eq!(got, expected);
    }

    /// `window_samples` and the vectorized path must agree on which samples the
    /// window holds, or the two rollup families would disagree step by step.
    #[test]
    fn test_basic_and_vectorized_windows_agree() {
        let series = samples(&[(10, 1.0), (25, 2.0), (26, 3.0), (90, 4.0)]);

        for window_ms in [1, 5, 20, 200] {
            for end in [0, 10, 25, 30, 89, 90, 300] {
                let basic = window_samples(&series, end, window_ms).map(<[Sample]>::len);
                let vectorized =
                    rollup_series_over_grid(&series, window_ms, 0, 0, [end], None, count)
                        .first()
                        .map(|s| s.value as usize);
                assert_eq!(basic, vectorized, "window_ms={window_ms} end={end}");
            }
        }
    }
}
