//! Explicit phase artifacts for the evaluator query pipeline.
//!
//! All evaluator-backed query paths (instant vector selector, matrix selector,
//! subquery vector-selector fast path) share the same logical execution model:
//!
//! 1. **Plan** – compute concrete time bounds, bucket list, path-specific parameters
//! 2. **LoadSamples** – load sample data for explicit (bucket, series) work items
//! 3. **ShapeSamples** – merge/filter/dedup into evaluator-ready per-series structures
//! 4. **Evaluate** – run PromQL expression semantics on prepared in-memory inputs
//!
//! The types in this module represent the intermediate artifacts produced by each phase.

use crate::common::{Sample, Timestamp};
use crate::promql::engine::QueryReader;
use crate::promql::exec::types::EvalLabels;
use crate::promql::{EvalResult, EvalSample, EvalSamples, ExprResult, QueryOptions};
use orx_parallel::{IntoParIter, ParIter};
use promql_parser::parser::VectorSelector;
use std::time::{Duration, Instant};

// ---------------------------------------------------------------------------
// Phase artifact types
// ---------------------------------------------------------------------------

/// Path-specific execution parameters.
pub(crate) enum QueryPathKind {
    /// Instant vector selector. Buckets newest-first, fingerprint dedup.
    InstantVector { lookback_delta_ms: i64 },
    /// Matrix selector. Buckets chronological, merge all samples per label set.
    Matrix,
    /// Subquery vector-selector fast path. Merge, sort/dedup, step-bucket.
    SubqueryVectorSelector {
        range_ms: i64,
        aligned_start_ms: i64,
        step_ms: i64,
        lookback_delta_ms: i64,
        expected_steps: usize,
    },
}

impl QueryPathKind {
    fn name(&self) -> &'static str {
        match self {
            QueryPathKind::InstantVector { .. } => "instant",
            QueryPathKind::Matrix => "matrix",
            QueryPathKind::SubqueryVectorSelector { .. } => "subquery",
        }
    }
}

/// Computed execution plan for a query path.
pub(crate) struct QueryPlan {
    /// Exclusive lower bound for sample filtering (timestamp > start).
    pub sample_start_ms: Timestamp,
    /// Inclusive upper bound for sample filtering (timestamp <= end).
    pub sample_end_ms: Timestamp,
    /// Path-specific behavior and parameters.
    pub path_kind: QueryPathKind,
}

// ---------------------------------------------------------------------------
// Phase 1: Planning
// ---------------------------------------------------------------------------

impl QueryPlan {
    /// Construct a plan for instant vector selector.
    ///
    /// Buckets sorted newest-first so that cross-bucket fingerprint dedup
    /// can short-circuit on the first bucket that has data for a series.
    pub(crate) fn for_instant_vector(
        adjusted_eval_ts_ms: Timestamp,
        lookback_delta_ms: Timestamp,
    ) -> Self {
        let end_ms = adjusted_eval_ts_ms;
        let start_ms = end_ms - lookback_delta_ms;
        QueryPlan {
            sample_start_ms: start_ms,
            sample_end_ms: end_ms,
            path_kind: QueryPathKind::InstantVector { lookback_delta_ms },
        }
    }

    /// Construct a plan for a matrix selector.
    ///
    /// Buckets sorted chronologically and filtered to only those overlapping
    /// with `[start_ms, end_ms]`. All samples are merged per label set.
    pub(crate) fn for_matrix(adjusted_eval_ts_ms: Timestamp, range_ms: Timestamp) -> Self {
        let end_ms = adjusted_eval_ts_ms;
        let start_ms = end_ms - range_ms;
        QueryPlan {
            sample_start_ms: start_ms,
            sample_end_ms: end_ms,
            path_kind: QueryPathKind::Matrix,
        }
    }

    /// Construct a plan for a subquery vector-selector fast path.
    ///
    /// Computes step alignment and extends the sample range backward by
    /// `lookback_delta_ms` so the first step has data. Buckets sorted
    /// newest-first (matching existing `fetch_series_samples` behavior).
    pub(crate) fn for_subquery_vector_selector(
        subquery_start_ms: Timestamp,
        subquery_end_ms: Timestamp,
        step_ms: Timestamp,
        lookback_delta_ms: Timestamp,
    ) -> Self {
        let (aligned_start_ms, range_start_ms, range_end_ms, expected_steps) =
            compute_subquery_alignment(
                subquery_start_ms,
                subquery_end_ms,
                step_ms,
                lookback_delta_ms,
            );

        let range_ms = subquery_end_ms - subquery_start_ms;
        QueryPlan {
            sample_start_ms: range_start_ms,
            sample_end_ms: range_end_ms,
            path_kind: QueryPathKind::SubqueryVectorSelector {
                range_ms,
                aligned_start_ms,
                step_ms,
                lookback_delta_ms,
                expected_steps,
            },
        }
    }
}

/// Compute subquery time alignment and range extension.
///
/// Returns `(aligned_start_ms, range_start_ms, range_end_ms, expected_steps)`.
///
/// Uses `div_euclid` for correct floor division with negative timestamps
/// (e.g. `-41ms / 10ms` → `-50ms`, not `-40ms`).
pub(crate) fn compute_subquery_alignment(
    subquery_start_ms: Timestamp,
    subquery_end_ms: Timestamp,
    step_ms: Timestamp,
    lookback_delta_ms: Timestamp,
) -> (Timestamp, Timestamp, Timestamp, usize) {
    let div = subquery_start_ms.div_euclid(step_ms);
    let mut aligned_start_ms = div * step_ms;
    if aligned_start_ms <= subquery_start_ms {
        aligned_start_ms += step_ms;
    }
    let expected_steps = ((subquery_end_ms - aligned_start_ms) / step_ms) as usize + 1;
    let range_start_ms = aligned_start_ms - lookback_delta_ms;
    let range_end_ms = subquery_end_ms;
    (
        aligned_start_ms,
        range_start_ms,
        range_end_ms,
        expected_steps,
    )
}

/// Walk `steps` (non-decreasing timestamps) over `samples` (ascending by
/// timestamp), invoking `on_step` for each step with the most recent sample at
/// or before the step timestamp, or `None` if that sample falls outside the
/// lookback window (`sample.timestamp <= step_ts - lookback_delta_ms`).
///
/// Shared by the range-query preload path and the subquery vector-selector
/// fast path so the lookback boundary semantics live in one place.
pub(crate) fn for_each_step_sample<I, F>(
    samples: &[Sample],
    steps: I,
    lookback_delta_ms: i64,
    mut on_step: F,
) where
    I: Iterator<Item = i64>,
    F: FnMut(i64, Option<&Sample>),
{
    let mut i = 0usize;
    let mut last_valid: Option<&Sample> = None;

    for step_ts in steps {
        let lookback_start = step_ts - lookback_delta_ms;

        while i < samples.len() && samples[i].timestamp <= step_ts {
            last_valid = Some(&samples[i]);
            i += 1;
        }

        let within_lookback = last_valid.filter(|s| s.timestamp > lookback_start);
        on_step(step_ts, within_lookback);
    }
}

/// Shape loaded data for the subquery vector-selector fast path.
///
/// Merges by fingerprint across series, sorts/deduplicates, then applies
/// the sliding-window step-bucketing algorithm.
fn shape_subquery_results(series_data: Vec<EvalSamples>, plan: &QueryPlan) -> Vec<EvalSamples> {
    let (range_ms, aligned_start_ms, step_ms, lookback_delta_ms, expected_steps) =
        match &plan.path_kind {
            QueryPathKind::SubqueryVectorSelector {
                range_ms,
                aligned_start_ms,
                step_ms,
                lookback_delta_ms,
                expected_steps,
            } => (
                *range_ms,
                *aligned_start_ms,
                *step_ms,
                *lookback_delta_ms,
                *expected_steps,
            ),
            _ => return Vec::new(),
        };

    let subquery_end_ms = plan.sample_end_ms;

    let range_vector: Vec<EvalSamples> = series_data
        .into_par()
        .filter_map(|sample| {
            if sample.values.is_empty() {
                return None;
            }

            let mut step_samples = Vec::with_capacity(expected_steps);
            let steps = (aligned_start_ms..=subquery_end_ms).step_by(step_ms as usize);
            for_each_step_sample(
                &sample.values,
                steps,
                lookback_delta_ms,
                |current_step_ms, latest| {
                    if let Some(latest) = latest {
                        step_samples.push(Sample {
                            timestamp: current_step_ms,
                            value: latest.value,
                        });
                    }
                },
            );

            if step_samples.is_empty() {
                None
            } else {
                Some(EvalSamples {
                    values: step_samples,
                    labels: sample.labels,
                    range_ms,
                    range_end_ms: subquery_end_ms,
                    drop_name: false,
                })
            }
        })
        .collect();

    range_vector
}

// ---------------------------------------------------------------------------
// Unified pipeline orchestrator
// ---------------------------------------------------------------------------

/// Aggregate phase timings for the selector pipeline.
#[derive(Debug, Default, Clone)]
pub(crate) struct PipelineTimings {
    pub sample_load_ms: f64,
    pub shape_samples_ms: f64,
}

/// Execute the shared selector pipeline for all evaluator-backed query paths.
///
/// Orchestrates: resolve metadata -> build work -> load samples -> shape results.
/// Branching on `QueryPathKind` handles the behavioral differences between
/// instant vector selectors, matrix selectors, and subquery fast paths.
pub(crate) fn execute_selector_pipeline<R: QueryReader>(
    reader: &R,
    plan: &QueryPlan,
    selector: &VectorSelector,
    mut options: QueryOptions,
) -> EvalResult<ExprResult> {
    let mut timings = PipelineTimings::default();

    // Phase: LoadSamples
    let t1 = Instant::now();

    // Special-case the handling of instant vector query plans. Since we account for clustering, it makes sense
    // to filter for the last item on the worker nodes instead of shipping the data only to filter
    // out on the requester. The `query` method on the reader should handle this
    if let QueryPathKind::InstantVector { lookback_delta_ms } = plan.path_kind {
        options.lookback_delta = Duration::from_millis(lookback_delta_ms as u64);
        let series_data = reader
            .query(selector, plan.sample_end_ms, options)?
            .into_iter()
            .map(|is| EvalSample {
                timestamp_ms: is.timestamp_ms,
                value: is.value,
                labels: EvalLabels::from(is.labels),
                drop_name: false,
            })
            .collect::<Vec<_>>();

        timings.sample_load_ms += t1.elapsed().as_secs_f64() * 1000.0;

        tracing::debug!(
            path = plan.path_kind.name(),
            load_ms = format!("{:.2}", timings.sample_load_ms),
            "pipeline phase timings"
        );

        let result = ExprResult::InstantVector(series_data);
        return Ok(result);
    };

    // The QueryPlan.sample_start_ms is defined as an exclusive lower bound
    // (timestamp > start). Underlying storage `query_range` / `TimeSeries::get_range`
    // treats the start parameter as inclusive. Increment the start timestamp by
    // 1ms to enforce the exclusive lower-bound semantics expected by the
    // pipeline. Use saturating_add to avoid overflow on extreme values.
    let query_start_inclusive = plan.sample_start_ms.saturating_add(1);

    let raw_range_samples =
        reader.query_range(selector, query_start_inclusive, plan.sample_end_ms, options)?;

    timings.sample_load_ms += t1.elapsed().as_secs_f64() * 1000.0;

    // Phase: ShapeSamples
    let t2 = Instant::now();

    let result = match &plan.path_kind {
        QueryPathKind::SubqueryVectorSelector { .. } => {
            // For the subquery fast path the fetch range includes a backward
            // extension by `lookback_delta_ms` so the first step has data.
            // The raw samples therefore span more than the declared subquery
            // range; we must apply the per-step sliding-window bucketing
            // (`shape_subquery_results`) so that:
            //   1. Only the sample most-recently-seen at each step is kept.
            //   2. `range_ms` on the returned `EvalSamples` reflects the
            //      actual subquery range (not the extended fetch range).
            let series: Vec<EvalSamples> = raw_range_samples
                .into_iter()
                .map(|s| EvalSamples {
                    labels: EvalLabels::from(s.labels),
                    drop_name: false,
                    range_ms: 0, // overwritten by shape_subquery_results
                    values: s.samples,
                    range_end_ms: plan.sample_end_ms,
                })
                .collect();
            let shaped = shape_subquery_results(series, plan);
            ExprResult::RangeVector(shaped)
        }
        _ => {
            // Matrix selector: the fetch range equals the declared query range,
            // so `range_ms` can be computed directly from the plan bounds.
            let range = plan.sample_end_ms - plan.sample_start_ms;
            let series: Vec<EvalSamples> = raw_range_samples
                .into_iter()
                .map(|s| EvalSamples {
                    labels: EvalLabels::from(s.labels),
                    drop_name: false,
                    range_ms: range,
                    values: s.samples,
                    range_end_ms: plan.sample_end_ms,
                })
                .collect();
            ExprResult::RangeVector(series)
        }
    };

    timings.shape_samples_ms = t2.elapsed().as_secs_f64() * 1000.0;

    tracing::debug!(
        path = plan.path_kind.name(),
        load_ms = format!("{:.2}", timings.sample_load_ms),
        shape_ms = format!("{:.2}", timings.shape_samples_ms),
        "pipeline phase timings"
    );

    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::labels::Label;

    fn label(name: &str, value: &str) -> Label {
        Label::new(name, value)
    }

    fn sample(ts: i64, val: f64) -> Sample {
        Sample::new(ts, val)
    }

    fn make_loaded(labels: Vec<Label>, samples: Vec<Sample>) -> EvalSamples {
        EvalSamples {
            labels: EvalLabels::from(crate::labels::Labels::new(labels)),
            drop_name: false,
            range_ms: 0,
            values: samples,
            range_end_ms: 0,
        }
    }

    // -----------------------------------------------------------------------
    // Planning tests
    // -----------------------------------------------------------------------

    #[test]
    fn instant_plan_sorts_buckets_newest_first() {
        let plan = QueryPlan::for_instant_vector(500_000, 60_000);
        assert_eq!(plan.sample_start_ms, 500_000 - 60_000);
        assert_eq!(plan.sample_end_ms, 500_000);
    }

    #[test]
    fn subquery_plan_alignment() {
        // subquery_start=15, end=55, step=10, lookback=20
        // aligned_start = ceil_to_next_step(15, 10) = 20
        // expected_steps = (55 - 20) / 10 + 1 = 4 (steps at 20, 30, 40, 50)
        // range_start = 20 - 20 = 0
        let plan = QueryPlan::for_subquery_vector_selector(15, 55, 10, 20);
        match &plan.path_kind {
            QueryPathKind::SubqueryVectorSelector {
                range_ms,
                aligned_start_ms,
                step_ms,
                lookback_delta_ms,
                expected_steps,
                ..
            } => {
                assert_eq!(*range_ms, 40);
                assert_eq!(*aligned_start_ms, 20);
                assert_eq!(*step_ms, 10);
                assert_eq!(*lookback_delta_ms, 20);
                assert_eq!(*expected_steps, 4);
            }
            _ => panic!("wrong path kind"),
        }
        assert_eq!(plan.sample_start_ms, 0);
        assert_eq!(plan.sample_end_ms, 55);
    }

    #[test]
    fn subquery_alignment_with_negative_timestamps() {
        // subquery_start=-41, end=0, step=10, lookback=5
        // div_euclid(-41, 10) = -5, aligned = -50, since -50 <= -41, aligned = -40
        // expected_steps = (0 - (-40)) / 10 + 1 = 5
        // range_start = -40 - 5 = -45
        let plan = QueryPlan::for_subquery_vector_selector(-41, 0, 10, 5);
        match &plan.path_kind {
            QueryPathKind::SubqueryVectorSelector {
                aligned_start_ms,
                expected_steps,
                ..
            } => {
                assert_eq!(*aligned_start_ms, -40);
                assert_eq!(*expected_steps, 5);
            }
            _ => panic!("wrong path kind"),
        }
        assert_eq!(plan.sample_start_ms, -45);
    }

    // -----------------------------------------------------------------------
    // Shaping tests
    // -----------------------------------------------------------------------

    #[test]
    fn shape_subquery_step_bucketing() {
        let labels_a = vec![label("__name__", "m")];

        // Samples at t=5, t=15, t=25
        let bucket_data = vec![make_loaded(
            labels_a,
            vec![sample(5, 1.0), sample(15, 2.0), sample(25, 3.0)],
        )];

        // Steps at 10, 20, 30 with lookback=10
        let plan = QueryPlan {
            sample_start_ms: -5,
            sample_end_ms: 30,
            path_kind: QueryPathKind::SubqueryVectorSelector {
                range_ms: 30,
                aligned_start_ms: 10,
                step_ms: 10,
                lookback_delta_ms: 10,
                expected_steps: 3,
            },
        };

        let results = shape_subquery_results(bucket_data, &plan);
        assert_eq!(results.len(), 1);
        let values = &results[0].values;
        assert_eq!(values.len(), 3);
        // Step 10: latest sample <= 10 with ts > 0 -> sample at t=5, value=1.0
        assert_eq!(values[0].timestamp, 10);
        assert_eq!(values[0].value, 1.0);
        // Step 20: latest sample <= 20 with ts > 10 -> sample at t=15, value=2.0
        assert_eq!(values[1].timestamp, 20);
        assert_eq!(values[1].value, 2.0);
        // Step 30: latest sample <= 30 with ts > 20 -> sample at t=25, value=3.0
        assert_eq!(values[2].timestamp, 30);
        assert_eq!(values[2].value, 3.0);
    }
}
