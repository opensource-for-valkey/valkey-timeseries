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
use crate::labels::Label;
use crate::promql::engine::{CachedQueryReader, SeriesQuerier};
use crate::promql::hashers::{FingerprintHashMap, HasFingerprint, SeriesFingerprint};
use crate::promql::{
    EvalResult, EvalSample, EvalSamples, EvaluationError, ExprResult, Labels, QueryOptions,
};
use ahash::{AHashMap, AHashSet};
use promql_parser::parser::VectorSelector;
use std::time::Instant;
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

impl QueryPlan {
    /// Shape all loaded bucket data into the final ExprResult for this path.
    fn shape(&self, all_bucket_data: Vec<LoadedSeriesSamples>) -> ExprResult {
        match &self.path_kind {
            QueryPathKind::InstantVector { .. } => {
                ExprResult::InstantVector(shape_instant_results(all_bucket_data))
            }
            QueryPathKind::Matrix => ExprResult::RangeVector(shape_matrix_results(
                all_bucket_data,
                self.sample_end_ms - self.sample_start_ms,
                self.sample_end_ms,
            )),
            QueryPathKind::SubqueryVectorSelector { .. } => {
                ExprResult::RangeVector(shape_subquery_results(all_bucket_data, self))
            }
        }
    }
}

/// Loaded samples for one series.
pub(crate) struct LoadedSeriesSamples {
    pub fingerprint: SeriesFingerprint,
    pub labels: Labels,
    pub samples: Vec<Sample>,
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
fn compute_subquery_alignment(
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

// ---------------------------------------------------------------------------
// Phase 5: Shaping
// ---------------------------------------------------------------------------

/// Shape loaded data for instant vector selector.
///
/// Takes the latest sample per fingerprint. Bucket data should already be in
/// newest-first order (from `QueryPlan::for_instant_vector`). A fingerprint
/// dedup pass ensures within-bucket duplicates are also handled.
pub(crate) fn shape_instant_results(series_data: Vec<LoadedSeriesSamples>) -> Vec<EvalSample> {
    let mut seen: AHashSet<SeriesFingerprint> = AHashSet::new();
    let mut results = Vec::new();

    for series in series_data {
        if seen.contains(&series.fingerprint) {
            // todo: append values
            continue;
        }
        if let Some(best) = series.samples.last() {
            results.push(EvalSample {
                timestamp_ms: best.timestamp,
                value: best.value,
                labels: series.labels,
                drop_name: false,
            });
            seen.insert(series.fingerprint);
        }
    }

    results
}

/// Shape loaded data for matrix selector.
///
/// Merges all samples per series (keyed by sorted label vector) across all
/// buckets. Bucket data should be in chronological order.
pub(crate) fn shape_matrix_results(
    series_data: Vec<LoadedSeriesSamples>,
    range_ms: i64,
    range_end_ms: i64,
) -> Vec<EvalSamples> {
    let mut series_map: AHashMap<Labels, Vec<Sample>> = AHashMap::new();

    for series in series_data {
        if let Some(samples) = series_map.get_mut(&series.labels) {
            samples.extend(series.samples);
        } else {
            series_map.insert(series.labels, series.samples);
        }
    }

    series_map
        .into_iter()
        .map(|(labels, values)| EvalSamples {
            values,
            labels,
            range_ms,
            range_end_ms,
            drop_name: false,
        })
        .collect()
}

/// Shape loaded data for the subquery vector-selector fast path.
///
/// Merges by fingerprint across buckets, sorts/deduplicates, then applies
/// the sliding-window step-bucketing algorithm.
pub(crate) fn shape_subquery_results(
    series_data: Vec<LoadedSeriesSamples>,
    plan: &QueryPlan,
) -> Vec<EvalSamples> {
    let (range_ms, aligned_start_ms, step_ms, lookback_delta_ms, expected_steps) = match &plan.path_kind {
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

    // Merge by fingerprint
    let mut merged: FingerprintHashMap<(Labels, Vec<Sample>)> = FingerprintHashMap::default();

    for series in &series_data {
        let entry = merged
            .entry(series.fingerprint)
            .or_insert_with(|| (series.labels.clone(), Vec::new()));
        entry.1.extend(series.samples.iter().cloned());
    }

    // Sort and dedup
    for (_, samples) in merged.values_mut() {
        samples.sort_by_key(|s| s.timestamp);
        samples.dedup_by_key(|s| s.timestamp);
    }

    // Step-bucketing with sliding window
    let subquery_end_ms = plan.sample_end_ms;
    let mut range_vector = Vec::with_capacity(merged.len());

    for (_, (labels, samples)) in merged {
        let mut step_samples = Vec::with_capacity(expected_steps);
        let mut i = 0usize;
        let mut last_valid: Option<&Sample> = None;

        for current_step_ms in (aligned_start_ms..=subquery_end_ms).step_by(step_ms as usize) {
            let lookback_start_ms = current_step_ms - lookback_delta_ms;

            while i < samples.len() && samples[i].timestamp <= current_step_ms {
                last_valid = Some(&samples[i]);
                i += 1;
            }

            if let Some(sample) = last_valid
                && sample.timestamp > lookback_start_ms
            {
                step_samples.push(Sample {
                    timestamp: current_step_ms,
                    value: sample.value,
                });
            }
        }

        if !step_samples.is_empty() {
            range_vector.push(EvalSamples {
                values: step_samples,
                labels,
                range_ms,
                range_end_ms: subquery_end_ms,
                drop_name: false,
            });
        }
    }

    range_vector
}

// ---------------------------------------------------------------------------
// Unified pipeline orchestrator
// ---------------------------------------------------------------------------

/// Aggregate phase timings for the selector pipeline.
#[derive(Debug, Default, Clone)]
pub(crate) struct PipelineTimings {
    pub metadata_resolve_ms: f64,
    pub sample_load_ms: f64,
    pub shape_samples_ms: f64,
}

/// Execute the shared selector pipeline for all evaluator-backed query paths.
///
/// Orchestrates: resolve metadata -> build work -> load samples -> shape results.
/// Branching on `QueryPathKind` handles the behavioral differences between
/// instant vector selectors, matrix selectors, and subquery fast paths.
pub(crate) fn execute_selector_pipeline<'reader, R: SeriesQuerier>(
    reader: &CachedQueryReader<'reader, R>,
    plan: &QueryPlan,
    selector: &VectorSelector,
    options: QueryOptions,
) -> EvalResult<ExprResult> {
    let mut timings = PipelineTimings::default();

    // Phase: LoadSamples
    let t1 = Instant::now();

    // The QueryPlan.sample_start_ms is defined as an exclusive lower bound
    // (timestamp > start). Underlying storage `query_range` / `TimeSeries::get_range`
    // treats the start parameter as inclusive. Increment the start timestamp by
    // 1ms to enforce the exclusive lower-bound semantics expected by the
    // pipeline. Use saturating_add to avoid overflow on extreme values.
    let query_start_inclusive = plan.sample_start_ms.saturating_add(1);

    let series_data = reader
        .query_range(selector, query_start_inclusive, plan.sample_end_ms, options)
        .map_err(|e| EvaluationError::InternalError(e.to_string()))? // todo: audit error
        .into_iter()
        .map(|s| {
            let fingerprint = s.labels.signature();
            LoadedSeriesSamples {
                fingerprint,
                samples: s.samples,
                labels: s.labels,
            }
        })
        .collect::<Vec<_>>();

    timings.sample_load_ms += t1.elapsed().as_secs_f64() * 1000.0;

    // Phase: ShapeSamples
    let t2 = Instant::now();
    let result = plan.shape(series_data);
    timings.shape_samples_ms = t2.elapsed().as_secs_f64() * 1000.0;

    tracing::debug!(
        path = plan.path_kind.name(),
        load_ms = format!("{:.2}", timings.sample_load_ms),
        shape_ms = format!("{:.2}", timings.shape_samples_ms),
        "pipeline phase timings"
    );

    Ok(result)
}

// ---------------------------------------------------------------------------
// Utility functions
// ---------------------------------------------------------------------------

/// Compute a fingerprint from a label slice for series deduplication.
///
/// Sorts labels by name (consistent with the existing evaluator implementation)
/// and hashes them. Two label sets that are logically identical will produce the
/// same fingerprint regardless of the order they are stored in.
pub(crate) fn compute_fingerprint(labels: &[Label]) -> SeriesFingerprint {
    let mut to_hash = labels.to_vec();
    to_hash.sort();
    to_hash.fingerprint()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn label(name: &str, value: &str) -> Label {
        Label::new(name, value)
    }

    fn sample(ts: i64, val: f64) -> Sample {
        Sample::new(ts, val)
    }

    fn make_loaded(
        fingerprint: SeriesFingerprint,
        labels: Vec<Label>,
        samples: Vec<Sample>,
    ) -> LoadedSeriesSamples {
        LoadedSeriesSamples {
            fingerprint,
            labels: Labels::new(labels),
            samples,
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
    fn shape_instant_newest_bucket_wins() {
        let labels_a = vec![label("__name__", "m"), label("env", "prod")];
        let fp = compute_fingerprint(&labels_a);

        let bucket_data = vec![
            // Newest bucket first (bucket 200)
            make_loaded(fp, labels_a.clone(), vec![sample(100, 1.0)]),
            // Older bucket (bucket 100)
            make_loaded(fp, labels_a, vec![sample(50, 2.0)]),
        ];

        let results = shape_instant_results(bucket_data);
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].value, 1.0); // newest bucket wins
        assert_eq!(results[0].timestamp_ms, 100);
    }

    #[test]
    fn shape_instant_takes_latest_sample() {
        let labels_a = vec![label("__name__", "m")];
        let fp = compute_fingerprint(&labels_a);

        let bucket_data = vec![make_loaded(
            fp,
            labels_a,
            vec![sample(10, 1.0), sample(20, 2.0), sample(30, 3.0)],
        )];

        let results = shape_instant_results(bucket_data);
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].value, 3.0); // latest sample
    }

    #[test]
    fn shape_instant_skips_empty_samples() {
        let labels_a = vec![label("__name__", "m")];
        let fp = compute_fingerprint(&labels_a);

        let bucket_data = vec![
            make_loaded(fp, labels_a, vec![]), // no samples
        ];

        let results = shape_instant_results(bucket_data);
        assert!(results.is_empty());
    }

    #[test]
    fn shape_matrix_merges_across_buckets() {
        let labels_a = vec![label("__name__", "m"), label("env", "prod")];

        let fp = compute_fingerprint(&labels_a);

        let bucket_data = vec![
            make_loaded(fp, labels_a.clone(), vec![sample(10, 1.0), sample(20, 2.0)]),
            make_loaded(fp, labels_a, vec![sample(30, 3.0)]),
        ];

        let results = shape_matrix_results(bucket_data, 100_000_000, 100_000_000);
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].values.len(), 3);
    }

    #[test]
    fn shape_subquery_step_bucketing() {
        let labels_a = vec![label("__name__", "m")];
        let fp = compute_fingerprint(&labels_a);

        // Samples at t=5, t=15, t=25
        let bucket_data = vec![make_loaded(
            fp,
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

    #[test]
    fn shape_subquery_deduplicates_across_buckets() {
        let labels_a = vec![label("__name__", "m")];
        let fp = compute_fingerprint(&labels_a);

        // Same timestamp in two buckets
        let bucket_data = vec![
            make_loaded(fp, labels_a.clone(), vec![sample(10, 1.0), sample(20, 2.0)]),
            make_loaded(
                fp,
                labels_a,
                vec![sample(10, 1.0), sample(15, 1.5)], // t=10 is duplicate
            ),
        ];

        let plan = QueryPlan {
            sample_start_ms: 0,
            sample_end_ms: 20,
            path_kind: QueryPathKind::SubqueryVectorSelector {
                range_ms: 20,
                aligned_start_ms: 10,
                step_ms: 10,
                lookback_delta_ms: 10,
                expected_steps: 2,
            },
        };

        let results = shape_subquery_results(bucket_data, &plan);
        assert_eq!(results.len(), 1);
        // After merge+dedup: samples at t=10, t=15, t=20
        // Step 10: sample at t=10, value=1.0
        // Step 20: sample at t=20, value=2.0
        let values = &results[0].values;
        assert_eq!(values.len(), 2);
        assert_eq!(values[0].value, 1.0);
        assert_eq!(values[1].value, 2.0);
    }

    // -----------------------------------------------------------------------
    // Utility tests
    // -----------------------------------------------------------------------

    #[test]
    fn fingerprint_order_independent() {
        let labels_a = vec![label("b", "2"), label("a", "1")];
        let labels_b = vec![label("a", "1"), label("b", "2")];
        assert_eq!(
            compute_fingerprint(&labels_a),
            compute_fingerprint(&labels_b)
        );
    }
}
