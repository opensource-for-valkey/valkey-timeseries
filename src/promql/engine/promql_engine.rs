use crate::common::threads::IntoParRayon;
use crate::common::time::{current_time_millis, system_time_to_millis};
use crate::common::{Sample, Timestamp};
use crate::promql::engine::derived_filters::derive_filters_in_place;
use crate::promql::engine::{QueryOptions, QueryReader};
use crate::promql::error::QueryError;
use crate::promql::exec::planner::PlannedQuery;
use crate::promql::exec::preloader::Preloader;
use crate::promql::exec::types::{EvalLabels, SeriesMap};
use crate::promql::exec::utils::merge_step_into_series_map;
use crate::promql::model::{InstantSample, QueryValue, RangeSample};
use crate::promql::optimizer::optimize_expr;
use crate::promql::time::duration_ms;
use crate::promql::time::{grid_step_count, step_times};
use crate::promql::utils::{check_subquery_cost, validate_max_points_per_timeseries};
use crate::promql::{Evaluator, ExprResult, QueryResult};
use orx_parallel::{Par, ParResult};
use promql_parser::parser::EvalStmt;
use std::sync::Arc;
use std::time::SystemTime;

#[cfg(test)]
use crate::promql::utils::range_bounds_to_system_time;
#[cfg(test)]
use promql_parser::parser::Expr;
#[cfg(test)]
use std::ops::RangeBounds;
#[cfg(test)]
use std::time::Duration;

// `parse_query`, `PromqlEngine` and `PromqlQuerier` are a convenience API over
// `evaluate_instant` / `evaluate_range` that only tests use.
#[cfg(test)]
fn parse_query(query: &str) -> QueryResult<Expr> {
    promql_parser::parser::parse(query).map_err(QueryError::InvalidQuery)
}

fn optimize_statement(stmt: &mut EvalStmt, options: &QueryOptions) -> QueryResult<()> {
    if options.optimize_queries {
        // Keep this at the shared evaluation boundary so command handlers and
        // the convenience API apply precisely the same optional rewrites.
        stmt.expr = optimize_expr(stmt.expr.clone())
            .map_err(|e| QueryError::InvalidQuery(format!("optimization error: {e}")))?;
    }
    Ok(())
}

/// Number of range-query steps evaluated in parallel and folded into the series
/// map per batch. Bounds peak intermediate memory to this many step results
/// rather than materializing every step at once (see `evaluate_range`).
const STEP_MERGE_CHUNK_SIZE: usize = 64;

fn resolve_deadline_ms(opts: QueryOptions) -> i64 {
    if let Some(deadline) = opts.deadline {
        return deadline;
    }
    if let Some(timeout) = opts.timeout {
        return current_time_millis().saturating_add(duration_ms(timeout));
    }
    0
}

#[cfg(test)]
pub(crate) trait PromqlEngine: Send + Sync {
    /// Build a query reader
    fn make_query_reader(&self) -> QueryResult<Arc<dyn QueryReader>>;

    /// Evaluate an instant PromQL query, returning typed `InstantSample`s.
    fn eval_query(
        &self,
        query: &str,
        time: Option<SystemTime>,
        opts: QueryOptions,
    ) -> QueryResult<QueryValue> {
        let expr = parse_query(query)?;

        let query_time = time.unwrap_or_else(SystemTime::now);
        let lookback_delta = opts.lookback_delta;
        let stmt = EvalStmt {
            expr,
            start: query_time,
            end: query_time,
            interval: Duration::from_secs(0),
            lookback_delta,
        };

        let reader = self.make_query_reader()?;

        evaluate_instant(reader, stmt, query_time, opts)
    }

    /// Evaluate a range PromQL query, returning typed `RangeSample`s.
    fn eval_query_range(
        &self,
        query: &str,
        start: SystemTime,
        end: SystemTime,
        step: Duration,
        opts: QueryOptions,
    ) -> QueryResult<Vec<RangeSample>> {
        let expr = parse_query(query)?;

        let lookback_delta = opts.lookback_delta;
        let stmt = EvalStmt {
            expr,
            start,
            end,
            interval: step,
            lookback_delta,
        };

        let reader = self.make_query_reader()?;

        evaluate_range(reader, stmt, opts)
    }
}

// ── Shared evaluation free functions ────────────────────────────────

/// Evaluate an instant PromQL query against the given reader.
pub fn evaluate_instant(
    reader: Arc<dyn QueryReader>,
    mut stmt: EvalStmt,
    query_time: SystemTime,
    opts: QueryOptions,
) -> Result<QueryValue, QueryError> {
    optimize_statement(&mut stmt, &opts)?;
    check_subquery_cost(&stmt.expr, 1).map_err(QueryError::from)?;
    let deadline = resolve_deadline_ms(opts);
    let evaluator = Evaluator::new(&reader, opts);

    // Best-effort timeout: compute a deadline if set and check before/after heavy ops
    if deadline > 0 && current_time_millis() > deadline {
        return Err(QueryError::Timeout);
    }

    let result = evaluator.evaluate(stmt)?;

    if deadline > 0 && current_time_millis() > deadline {
        return Err(QueryError::Timeout);
    }

    match result {
        ExprResult::Scalar(value) => {
            let timestamp_ms = system_time_to_millis(query_time);
            Ok(QueryValue::Scalar {
                timestamp_ms,
                value,
            })
        }
        ExprResult::InstantVector(samples) => Ok(QueryValue::Vector(
            samples
                .into_iter()
                .map(|s| InstantSample {
                    labels: s.labels.into_labels(),
                    timestamp_ms: s.timestamp_ms,
                    value: s.value,
                })
                .collect(),
        )),
        // __name__ drops were already materialized by cleanup_metric_labels
        // inside evaluate_with_context.
        ExprResult::RangeVector(samples) => Ok(QueryValue::Matrix(
            samples
                .into_iter()
                .map(|s| RangeSample {
                    labels: s.labels.into_labels(),
                    samples: s.values.into_vec(),
                })
                .collect(),
        )),
        ExprResult::String(s) => Ok(QueryValue::String(s)),
    }
}

/// Evaluate a range PromQL query against the given reader.
/// Returns the result and the EvalStats for metrics publishing.
pub fn evaluate_range(
    reader: Arc<dyn QueryReader>,
    mut stmt: EvalStmt,
    opts: QueryOptions,
) -> QueryResult<Vec<RangeSample>> {
    optimize_statement(&mut stmt, &opts)?;
    let start = stmt.start;
    let end = stmt.end;
    let step = stmt.interval;
    let lookback_delta = stmt.lookback_delta;

    if step.is_zero() {
        return Err(QueryError::InvalidQuery(
            "step must be greater than zero".to_string(),
        ));
    }

    let start_ms = system_time_to_millis(start);
    let end_ms = system_time_to_millis(end);

    // Reject an over-wide step grid before anything is sized from it. The preload
    // phase reserves one slot per step — `(end - start) / step` of them — so an
    // unbounded `END` (the `+` sentinel resolves to i64::MAX) would otherwise drive a
    // multi-terabyte allocation and abort the server. `max_points_per_series` carries
    // the configured `ts-promql-max-points-per-timeseries`; 0 (its default) leaves only
    // the `MAX_GRID_STEPS` ceiling, which applies whatever the setting.
    validate_max_points_per_timeseries(
        start_ms,
        end_ms,
        step,
        opts.max_points_per_series.unwrap_or(0),
    )
    .map_err(QueryError::from)?;

    let deadline = resolve_deadline_ms(opts);

    let step_ms = duration_ms(step);
    // Every outer step evaluates each subquery in full.
    check_subquery_cost(&stmt.expr, grid_step_count(start_ms, end_ms, step_ms))
        .map_err(QueryError::from)?;
    let lookback_delta_ms = duration_ms(lookback_delta);
    let range_ctx = crate::promql::EvalContext {
        query_start: start_ms,
        query_end: end_ms,
        evaluation_ts: end_ms,
        lookback_delta_ms,
        step_ms,
    };
    // Before planning, so the tree that is preloaded and the tree that is
    // stepped over are the same narrowed one: see `derived_filters`.
    if opts.derived_filter_pushdown {
        derive_filters_in_place(&mut stmt.expr, reader.as_ref(), opts)?;
    }

    let plan = PlannedQuery::for_range(&stmt.expr, &range_ctx);
    let prepared = Preloader::new(reader.as_ref(), opts)
        .prepare(plan)
        .map_err(QueryError::from)?;
    let evaluator = Evaluator::with_prepared(reader.as_ref(), opts, prepared);

    if deadline > 0 && current_time_millis() > deadline {
        return Err(QueryError::Timeout);
    }

    let eval_step = |t: Timestamp| -> QueryResult<(Timestamp, ExprResult)> {
        // Best-effort per-step timeout check.
        if deadline > 0 && current_time_millis() > deadline {
            return Err(QueryError::Timeout);
        }

        let ctx = crate::promql::EvalContext {
            query_start: start_ms,
            query_end: end_ms,
            evaluation_ts: t,
            lookback_delta_ms,
            step_ms,
        };

        let result = evaluator
            .evaluate_with_context(&stmt.expr, ctx)
            .map_err(QueryError::from)?;
        Ok((t, result))
    };

    // Process steps in bounded chunks rather than materializing every step's
    // result at once. Each chunk is evaluated in parallel and folded into the
    // series map before the next chunk starts, so peak intermediate memory is
    // bounded to `STEP_MERGE_CHUNK_SIZE` step results instead of O(steps).
    //
    // Ordering is preserved: `collect` keeps input order within a
    // chunk, and chunks are drained in ascending step order, so the per-series
    // sample vectors stay chronologically sorted without an extra sort.
    let mut step_ts = step_times(start_ms, end_ms, step_ms);
    let mut series_map = SeriesMap::default();

    loop {
        let chunk: Vec<Timestamp> = step_ts.by_ref().take(STEP_MERGE_CHUNK_SIZE).collect();
        if chunk.is_empty() {
            break;
        }

        let chunk_results: Vec<(Timestamp, ExprResult)> = chunk
            .into_par_rayon()
            .map(eval_step)
            .into_fallible()
            .collect()?;

        for (current_time, result) in chunk_results {
            match result {
                ExprResult::InstantVector(samples) => {
                    merge_step_into_series_map(&mut series_map, current_time, samples);
                }
                ExprResult::Scalar(value) => {
                    series_map
                        .entry(EvalLabels::empty())
                        .or_default()
                        .push(Sample::new(current_time, value));
                }
                ExprResult::RangeVector(_) => {
                    return Err(QueryError::Execution(
                        "range vectors not supported in range query evaluation".to_string(),
                    ));
                }
                ExprResult::String(_) => {
                    return Err(QueryError::Execution(
                        "string expressions not supported in range query evaluation".to_string(),
                    ));
                }
            }
        }
    }

    let mut result: Vec<RangeSample> = series_map
        .into_iter()
        .map(|(labels, samples)| RangeSample {
            samples,
            labels: labels.into_labels(),
        })
        .collect();
    // The map iterates in a per-process random order, so without this the same query
    // returned its series in a different order after a restart or on another node.
    // Prometheus sorts a range result by labels: pair by pair, name then value, the
    // shorter set first, which is `Labels`' own ordering.
    par_sort_unstable_by(&mut result, &|a: &RangeSample, b: &RangeSample| {
        a.labels.cmp(&b.labels)
    });

    Ok(result)
}

/// Below this many series a range result is sorted on the calling thread: at a few
/// nanoseconds a comparison, that is well under a millisecond.
const PAR_SORT_MIN_LEN: usize = 8192;

/// `sort_unstable_by`, split across the pool for a large slice: partition around the
/// median in place, then sort the two halves concurrently. A result with one series per
/// distinct value (`count_values` over many series and steps) can hold 100 000+ series,
/// and sorting that many label sets on one thread cost 12 ms of a 55 ms query.
fn par_sort_unstable_by<T, F>(v: &mut [T], cmp: &F)
where
    T: Send,
    F: Fn(&T, &T) -> std::cmp::Ordering + Sync,
{
    if v.len() <= PAR_SORT_MIN_LEN {
        v.sort_unstable_by(cmp);
        return;
    }
    let mid = v.len() / 2;
    v.select_nth_unstable_by(mid, cmp);
    let (lo, hi) = v.split_at_mut(mid);
    crate::common::threads::join(
        || par_sort_unstable_by(lo, cmp),
        || par_sort_unstable_by(hi, cmp),
    );
}

/// Tsdb manages a unified Promql QueryReader interface
#[cfg(test)]
pub(crate) struct PromqlQuerier {
    pub(crate) querier: Arc<dyn QueryReader>,
}

#[cfg(test)]
impl PromqlQuerier {
    pub(crate) fn with_query_reader(querier: Arc<dyn QueryReader>) -> Self {
        Self { querier }
    }

    /// Evaluate an instant PromQL query, returning typed `InstantSample`s.
    /// Convenience wrapper over [`PromqlEngine::eval_query`].
    pub fn eval_query(
        &self,
        query: &str,
        time: Option<SystemTime>,
        opts: &QueryOptions,
    ) -> QueryResult<QueryValue> {
        PromqlEngine::eval_query(self, query, time, *opts)
    }

    /// Evaluate a range PromQL query, returning typed `RangeSample`s.
    /// Convenience wrapper over [`PromqlEngine::eval_query_range`] that accepts
    /// Rust range bounds.
    pub fn eval_query_range(
        &self,
        query: &str,
        range: impl RangeBounds<SystemTime>,
        step: Duration,
        opts: &QueryOptions,
    ) -> QueryResult<Vec<RangeSample>> {
        let (start, end) = range_bounds_to_system_time(range);
        PromqlEngine::eval_query_range(self, query, start, end, step, *opts)
    }
}

#[cfg(test)]
impl PromqlEngine for PromqlQuerier {
    fn make_query_reader(&self) -> QueryResult<Arc<dyn QueryReader>> {
        Ok(self.querier.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::labels::Label;
    use crate::promql::engine::memory_series_querier::MemorySeriesQuerier;
    use std::time::UNIX_EPOCH;

    fn create_tsdb() -> PromqlQuerier {
        let querier = Arc::new(MemorySeriesQuerier::new());
        PromqlQuerier::with_query_reader(querier)
    }

    fn create_sample(
        metric_name: &str,
        label_pairs: Vec<(&str, &str)>,
        timestamp: i64,
        value: f64,
    ) -> RangeSample {
        let mut labels = vec![Label {
            name: "__name__".to_string(),
            value: metric_name.to_string(),
        }];
        for (key, val) in label_pairs {
            labels.push(Label {
                name: key.to_string(),
                value: val.to_string(),
            });
        }
        labels.sort();
        RangeSample {
            labels: crate::labels::Labels::new(labels),
            samples: vec![Sample { timestamp, value }],
        }
    }

    // ── Native read method tests ─────────────────────────────────────

    fn create_tsdb_with_data() -> PromqlQuerier {
        let querier = MemorySeriesQuerier::new();

        // Ingest two series into a bucket at minute 60 (covers 3,600,000–7,199,999 ms)
        let series = vec![
            create_sample("http_requests", vec![("env", "prod")], 4_000_000, 42.0),
            create_sample("http_requests", vec![("env", "staging")], 4_000_000, 10.0),
        ];

        for sample in series {
            for val in sample.samples {
                querier.add_sample(&sample.labels, val);
            }
        }

        PromqlQuerier::with_query_reader(Arc::new(querier))
    }

    #[test]
    fn optimized_identity_arithmetic_matches_unoptimized_results() {
        let querier = MemorySeriesQuerier::new();
        let labels = crate::labels::Labels::new(vec![Label {
            name: "__name__".to_string(),
            value: "signed_zero".to_string(),
        }]);
        querier.add_sample(&labels, Sample::new(4_000_000, -0.0));
        let tsdb = PromqlQuerier::with_query_reader(Arc::new(querier));
        let query_time = UNIX_EPOCH + Duration::from_secs(4_000);

        for query in [
            "signed_zero + 0",
            "0 + signed_zero",
            "signed_zero * 1",
            "1 * signed_zero",
            "signed_zero / 1",
        ] {
            let unoptimized = tsdb
                .eval_query(
                    query,
                    Some(query_time),
                    &QueryOptions {
                        optimize_queries: false,
                        ..QueryOptions::default()
                    },
                )
                .expect("unoptimized query must evaluate");
            let optimized = tsdb
                .eval_query(
                    query,
                    Some(query_time),
                    &QueryOptions {
                        optimize_queries: true,
                        ..QueryOptions::default()
                    },
                )
                .expect("optimized query must evaluate");

            let (QueryValue::Vector(unoptimized), QueryValue::Vector(optimized)) =
                (unoptimized, optimized)
            else {
                panic!("{query} must return an instant vector");
            };
            assert_eq!(unoptimized.len(), 1, "{query}");
            assert_eq!(optimized.len(), 1, "{query}");
            assert_eq!(unoptimized[0].labels, optimized[0].labels, "{query}");
            assert!(
                unoptimized[0].labels.get("__name__").is_none(),
                "{query} must drop the metric name"
            );
            assert_eq!(
                unoptimized[0].value.to_bits(),
                optimized[0].value.to_bits(),
                "{query} must preserve the exact floating-point result"
            );
        }
    }

    #[test]
    fn optimized_range_evaluation_matches_unoptimized_results() {
        let querier = MemorySeriesQuerier::new();
        let labels = crate::labels::Labels::new(vec![Label {
            name: "__name__".to_string(),
            value: "range_metric".to_string(),
        }]);
        for timestamp in [4_000_000, 4_001_000] {
            querier.add_sample(&labels, Sample::new(timestamp, -0.0));
        }
        let tsdb = PromqlQuerier::with_query_reader(Arc::new(querier));
        let start = UNIX_EPOCH + Duration::from_secs(4_000);
        let end = UNIX_EPOCH + Duration::from_secs(4_001);
        let step = Duration::from_secs(1);

        let unoptimized = tsdb
            .eval_query_range(
                "range_metric + (1 - 1)",
                start..=end,
                step,
                &QueryOptions {
                    optimize_queries: false,
                    ..QueryOptions::default()
                },
            )
            .expect("unoptimized range query must evaluate");
        let optimized = tsdb
            .eval_query_range(
                "range_metric + (1 - 1)",
                start..=end,
                step,
                &QueryOptions {
                    optimize_queries: true,
                    ..QueryOptions::default()
                },
            )
            .expect("optimized range query must evaluate");

        assert_eq!(unoptimized.len(), 1);
        assert_eq!(optimized.len(), 1);
        assert_eq!(unoptimized[0].labels, optimized[0].labels);
        assert!(unoptimized[0].labels.get("__name__").is_none());
        assert_eq!(unoptimized[0].samples.len(), optimized[0].samples.len());
        for (unoptimized, optimized) in unoptimized[0].samples.iter().zip(&optimized[0].samples) {
            assert_eq!(unoptimized.timestamp, optimized.timestamp);
            assert_eq!(unoptimized.value.to_bits(), optimized.value.to_bits());
        }
    }

    #[test]
    fn eval_query_should_return_instant_vector() {
        let tsdb = create_tsdb_with_data();
        let query_time = UNIX_EPOCH + Duration::from_secs(4100);

        let opts = QueryOptions::default();
        let result = tsdb
            .eval_query("http_requests", Some(query_time), &opts)
            .unwrap();
        let mut samples = match result {
            QueryValue::Vector(samples) => samples,
            other => panic!("expected Vector, got {:?}", other),
        };
        samples.sort_by(|a, b| {
            a.labels
                .metric_name()
                .cmp(b.labels.metric_name())
                .then_with(|| a.labels.get("env").cmp(&b.labels.get("env")))
        });

        assert_eq!(samples.len(), 2);
        assert_eq!(samples[0].labels.get("env"), Some("prod"));
        assert_eq!(samples[0].value, 42.0);
        assert_eq!(samples[1].labels.get("env"), Some("staging"));
        assert_eq!(samples[1].value, 10.0);
    }

    #[test]
    fn eval_query_should_respect_lookback_delta() {
        // Sample at t=4000s, query at t=4100s (100s later).
        // Default 5m lookback finds it; 10s lookback should not.
        let tsdb = create_tsdb_with_data();
        let query_time = UNIX_EPOCH + Duration::from_secs(4100);

        let wide = QueryOptions::default(); // 5m
        let results = tsdb
            .eval_query("http_requests", Some(query_time), &wide)
            .unwrap()
            .into_matrix()
            .unwrap();
        assert_eq!(results.len(), 2);

        let narrow = QueryOptions {
            lookback_delta: Duration::from_secs(10),
            ..QueryOptions::default()
        };
        let results = tsdb
            .eval_query("http_requests", Some(query_time), &narrow)
            .unwrap()
            .into_matrix()
            .unwrap();
        assert_eq!(
            results.len(),
            0,
            "10s lookback should miss samples 100s ago"
        );
    }

    #[test]
    fn eval_query_range_should_respect_lookback_delta() {
        // Same idea but for range queries: narrow lookback → no results.
        let tsdb = create_tsdb_with_data();
        let start = UNIX_EPOCH + Duration::from_secs(4100);
        let end = start;
        let step = Duration::from_secs(60);

        let wide = QueryOptions::default();
        let results = tsdb
            .eval_query_range("http_requests", start..=end, step, &wide)
            .unwrap();
        assert_eq!(results.len(), 2);

        let narrow = QueryOptions {
            lookback_delta: Duration::from_secs(10),
            ..QueryOptions::default()
        };
        let results = tsdb
            .eval_query_range("http_requests", start..=end, step, &narrow)
            .unwrap();
        assert!(
            results.is_empty(),
            "10s lookback should miss samples 100s ago"
        );
    }

    #[test]
    fn eval_query_should_return_scalar() {
        let tsdb = create_tsdb();
        let query_time = UNIX_EPOCH + Duration::from_secs(100);

        let opts = QueryOptions::default();
        let result = tsdb.eval_query("1+1", Some(query_time), &opts).unwrap();

        match result {
            QueryValue::Scalar {
                timestamp_ms,
                value,
            } => {
                assert_eq!(value, 2.0);
                assert_eq!(
                    timestamp_ms,
                    duration_ms(query_time.duration_since(UNIX_EPOCH).unwrap())
                );
            }
            other => panic!("expected Scalar, got {:?}", other),
        }
    }

    #[test]
    fn eval_query_should_return_error_for_invalid_query() {
        let tsdb = create_tsdb();

        let opts = QueryOptions::default();
        let result = tsdb.eval_query("invalid{", None, &opts);

        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), QueryError::InvalidQuery(_)));
    }

    #[test]
    fn eval_query_range_should_return_range_samples() {
        let tsdb = create_tsdb_with_data();
        let start = UNIX_EPOCH + Duration::from_secs(4000);
        let end = UNIX_EPOCH + Duration::from_secs(4000);
        let step = Duration::from_secs(60);

        let opts = QueryOptions::default();
        let mut results = tsdb
            .eval_query_range("http_requests", start..=end, step, &opts)
            .unwrap();
        results.sort_by(|a, b| a.labels.get("env").cmp(&b.labels.get("env")));

        assert_eq!(results.len(), 2);
        assert_eq!(results[0].labels.get("env"), Some("prod"));
        assert!(!results[0].samples.is_empty());
        assert_eq!(results[1].labels.get("env"), Some("staging"));
    }

    /// The parallel sort orders a slice past the sequential cutoff exactly as the
    /// sequential sort does, and leaves a short one to it.
    #[test]
    fn par_sort_matches_sequential_sort() {
        for len in [0, 1, PAR_SORT_MIN_LEN, PAR_SORT_MIN_LEN * 5 + 3] {
            // A scrambled permutation with repeats: a large stride modulo a prime.
            let mut v: Vec<u64> = (0..len as u64).map(|i| (i * 7_919) % 10_007).collect();
            let mut expected = v.clone();
            expected.sort_unstable();
            par_sort_unstable_by(&mut v, &|a: &u64, b: &u64| a.cmp(b));
            assert_eq!(v, expected, "len {len}");
        }
    }

    /// Range results come back sorted by labels, as Prometheus returns them, not in the
    /// series map's random iteration order.
    #[test]
    fn eval_query_range_sorts_series_by_labels() {
        let querier = MemorySeriesQuerier::new();
        let mut label_sets: Vec<Vec<(String, String)>> = (0..30)
            .map(|i| {
                let mut pairs = vec![
                    ("__name__".to_string(), format!("m{}", i % 3)),
                    ("host".to_string(), format!("h{:02}", (i * 7) % 30)),
                ];
                if i % 4 == 0 {
                    pairs.push(("zone".to_string(), "a".to_string()));
                }
                pairs
            })
            .collect();
        // A set that is a prefix of another sorts first.
        label_sets.push(vec![("__name__".to_string(), "m0".to_string())]);
        for pairs in &label_sets {
            let pairs: Vec<(&str, &str)> = pairs
                .iter()
                .map(|(n, v)| (n.as_str(), v.as_str()))
                .collect();
            querier.add_sample(
                &crate::labels::Labels::from_pairs(&pairs),
                Sample::new(1000, 1.0),
            );
        }
        let tsdb = PromqlQuerier::with_query_reader(Arc::new(querier));

        let at = UNIX_EPOCH + Duration::from_secs(1);
        let results = tsdb
            .eval_query_range(
                r#"{__name__=~"m.*"}"#,
                at..=at,
                Duration::from_secs(60),
                &QueryOptions::default(),
            )
            .unwrap();
        assert_eq!(results.len(), label_sets.len());
        for pair in results.windows(2) {
            assert!(
                pair[0].labels < pair[1].labels,
                "{} came before {}",
                pair[0].labels,
                pair[1].labels
            );
        }
        assert_eq!(
            results[0].labels.iter().count(),
            1,
            "the shortest m0 set sorts first"
        );
    }

    #[test]
    fn eval_query_range_should_return_scalar() {
        let tsdb = create_tsdb();
        let start = UNIX_EPOCH + Duration::from_secs(100);
        let end = UNIX_EPOCH + Duration::from_secs(160);
        let step = Duration::from_secs(60);

        let opts = QueryOptions::default();
        let results = tsdb
            .eval_query_range("1+1", start..=end, step, &opts)
            .unwrap();

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].labels.metric_name(), "");
        assert_eq!(results[0].samples.len(), 2); // two steps: 100s and 160s
        assert_eq!(results[0].samples[0].value, 2.0);
        assert_eq!(results[0].samples[1].value, 2.0);
    }

    #[test]
    fn eval_query_range_vector_samples_use_step_timestamps() {
        let tsdb = create_tsdb_with_data();
        let start = UNIX_EPOCH + Duration::from_secs(4100);
        let end = UNIX_EPOCH + Duration::from_secs(4220);
        let step = Duration::from_secs(60);

        let opts = QueryOptions::default();
        let mut results = tsdb
            .eval_query_range("http_requests", start..=end, step, &opts)
            .unwrap();
        results.sort_by(|a, b| a.labels.get("env").cmp(&b.labels.get("env")));

        assert_eq!(results.len(), 2);
        for rs in results {
            let mut ts: Vec<_> = rs.samples.into_iter().map(|s| s.timestamp).collect();
            ts.sort_unstable();
            assert_eq!(ts, vec![4_100_000, 4_160_000, 4_220_000]);
        }
    }

    #[test]
    fn eval_query_range_should_return_error_for_invalid_query() {
        let tsdb = create_tsdb();
        let start = UNIX_EPOCH + Duration::from_secs(100);
        let end = UNIX_EPOCH + Duration::from_secs(200);

        let opts = QueryOptions::default();
        let result = tsdb.eval_query_range("invalid{", start..=end, Duration::from_secs(60), &opts);

        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), QueryError::InvalidQuery(_)));
    }

    // -----------------------------------------------------------------------
    // Offset / @ modifier tests (preloading)
    // -----------------------------------------------------------------------

    #[test]
    fn eval_query_with_offset_should_load_correct_bucket() {
        // Data at 4000s (bucket hour-60: 3600–7199s).
        // Query at 7600s with offset 1h → effective time = 4000s.
        let tsdb = create_tsdb_with_data();
        let query_time = UNIX_EPOCH + Duration::from_secs(7600);
        let opts = QueryOptions::default();

        let result = tsdb
            .eval_query("http_requests offset 1h", Some(query_time), &opts)
            .unwrap();
        let samples = result.into_matrix().unwrap();
        assert_eq!(samples.len(), 2, "offset 1h should find data at 4000s");
    }

    #[test]
    fn eval_query_range_with_offset_crossing_bucket() {
        // Data at 4000s. Range [7600,7660] with offset 1h → effective [4000,4060].
        let tsdb = create_tsdb_with_data();
        let start = UNIX_EPOCH + Duration::from_secs(7600);
        let end = UNIX_EPOCH + Duration::from_secs(7660);
        let step = Duration::from_secs(60);
        let opts = QueryOptions::default();

        let results = tsdb
            .eval_query_range("http_requests offset 1h", start..=end, step, &opts)
            .unwrap();
        assert!(!results.is_empty(), "offset range query should find data");
        for rs in &results {
            assert!(!rs.samples.is_empty());
        }
    }

    #[test]
    fn eval_query_with_offset_before_epoch_should_not_error() {
        // Query at 100s with offset 1h → effective time = -3500s (before epoch).
        let querier = Arc::new(MemorySeriesQuerier::new());
        let tsdb = PromqlQuerier::with_query_reader(querier);
        let query_time = UNIX_EPOCH + Duration::from_secs(100);
        let opts = QueryOptions::default();

        let result = tsdb
            .eval_query("up offset 1h", Some(query_time), &opts)
            .unwrap();
        let samples = result.into_matrix().unwrap();
        assert!(
            samples.is_empty(),
            "before-epoch offset should return empty"
        );
    }

    #[test]
    fn eval_query_range_with_at_before_epoch_should_not_error() {
        // `@ 0 offset 1h` pins evaluation to t=0, then offset pushes to -3600.
        let tsdb = create_tsdb();
        let start = UNIX_EPOCH + Duration::from_secs(1000);
        let end = UNIX_EPOCH + Duration::from_secs(2000);
        let step = Duration::from_secs(60);
        let opts = QueryOptions::default();

        let results = tsdb
            .eval_query_range("up @ 0 offset 1h", start..=end, step, &opts)
            .unwrap();
        assert!(
            results.is_empty() || results.iter().all(|rs| rs.samples.is_empty()),
            "@ 0 offset 1h should return empty matrix"
        );
    }

    #[test]
    fn eval_query_range_with_at_end_should_load_correct_bucket() {
        // Data at 4000s. Range [4000,8000]. `@ end()` pins to 8000s.
        // With the default 5m lookback, the sample at 4000s is within range from 8000.
        // Actually, @ end() pins evaluation to t=8000 for each step, but
        // lookback only covers 5min=300s. Data at 4000s is 4000s before 8000s,
        // so it won't be found by lookback. Let's use a range where @ end()
        // helps: range [3900,4100], data at 4000s, `@ end()` pins to 4100s,
        // lookback 5min covers it.
        let tsdb = create_tsdb_with_data();
        let start = UNIX_EPOCH + Duration::from_secs(3900);
        let end = UNIX_EPOCH + Duration::from_secs(4100);
        let step = Duration::from_secs(60);
        let opts = QueryOptions::default();

        let results = tsdb
            .eval_query_range("http_requests @ end()", start..=end, step, &opts)
            .unwrap();
        assert!(!results.is_empty(), "@ end() should find data");
        // All steps should see the same sample (pinned to end)
        for rs in &results {
            assert!(!rs.samples.is_empty());
        }
    }
}
