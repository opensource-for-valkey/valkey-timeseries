use crate::common::time::{current_time_millis, system_time_to_millis};
use crate::common::{Sample, Timestamp};
use crate::promql::engine::{QueryOptions, QueryReader};
use crate::promql::error::QueryError;
use crate::promql::model::{InstantSample, Labels, QueryValue, RangeSample};
use crate::promql::time::step_times;
use crate::promql::utils::range_bounds_to_system_time;
use crate::promql::{Evaluator, ExprResult, QueryResult};
use orx_parallel::{IterIntoParIter, ParIter, ParIterResult};
use promql_parser::label::METRIC_NAME;
use promql_parser::parser::{EvalStmt, Expr, VectorSelector};
use std::hash::BuildHasherDefault;
use std::ops::RangeBounds;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use twox_hash::XxHash64;

/// Parse a match[] selector string into a VectorSelector
fn parse_selector(selector: &str) -> Result<VectorSelector, String> {
    let expr = promql_parser::parser::parse(selector).map_err(|e| e.to_string())?;
    match expr {
        Expr::VectorSelector(vs) => Ok(vs),
        _ => Err("Expected a vector selector".to_string()),
    }
}

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
        let expr = promql_parser::parser::parse(query)
            .map_err(|e| QueryError::InvalidQuery(e.to_string()))?;

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
        let expr = promql_parser::parser::parse(query)
            .map_err(|e| QueryError::InvalidQuery(e.to_string()))?;

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

/// Evaluate a range query using Rust range bounds, converting to
/// `(start, end)` exactly once before dispatching to `TsdbReadEngine`.
pub(crate) fn eval_query_range_bounds<E: PromqlEngine + ?Sized>(
    engine: &E,
    query: &str,
    range: impl RangeBounds<SystemTime>,
    step: Duration,
    opts: QueryOptions,
) -> QueryResult<Vec<RangeSample>> {
    let (start, end) = range_bounds_to_system_time(range);
    E::eval_query_range(engine, query, start, end, step, opts)
}

// ── Shared evaluation free functions ────────────────────────────────

/// Evaluate an instant PromQL query against the given reader.
pub fn evaluate_instant(
    reader: Arc<dyn QueryReader>,
    stmt: EvalStmt,
    query_time: SystemTime,
    opts: QueryOptions,
) -> Result<QueryValue, QueryError> {
    let deadline = opts.deadline.map_or(0, |d| d);
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
                    labels: s.labels,
                    timestamp_ms: s.timestamp_ms,
                    value: s.value,
                })
                .collect(),
        )),
        ExprResult::RangeVector(samples) => Ok(QueryValue::Matrix(
            samples
                .into_iter()
                .map(|mut s| {
                    if s.drop_name {
                        s.labels.remove(METRIC_NAME);
                    }
                    RangeSample {
                        labels: s.labels,
                        samples: s.values,
                    }
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
    stmt: EvalStmt,
    opts: QueryOptions,
) -> QueryResult<Vec<RangeSample>> {
    let start = stmt.start;
    let end = stmt.end;
    let step = stmt.interval;
    let lookback_delta = stmt.lookback_delta;

    if step.is_zero() {
        return Err(QueryError::InvalidQuery(
            "step must be greater than zero".to_string(),
        ));
    }

    let evaluator = Evaluator::new(&reader, opts);

    evaluator
        .preload_for_range_from_stmt(&stmt)
        .map_err(|e| QueryError::InvalidQuery(e.to_string()))?;

    if let Some(dl) = opts.deadline
        && current_time_millis() > dl
    {
        return Err(QueryError::Timeout);
    }

    let start_ms = system_time_to_millis(start);
    let end_ms = system_time_to_millis(end);

    // Evaluate every step in parallel, each returning its timestamp + result.
    let step_results: Vec<(Timestamp, ExprResult)> =
        step_times(start_ms, end_ms, step.as_millis() as i64)
            .iter_into_par()
            .map(|t| -> QueryResult<(Timestamp, ExprResult)> {
                // Best-effort per-step timeout check.
                if let Some(dl) = opts.deadline
                    && current_time_millis() > dl
                {
                    return Err(QueryError::Timeout);
                }

                let ts = UNIX_EPOCH + Duration::from_millis(t as u64);

                let instant_stmt = EvalStmt {
                    expr: stmt.expr.clone(),
                    start: ts,
                    end: ts,
                    interval: Duration::from_secs(0),
                    lookback_delta,
                };

                let result = evaluator.evaluate(instant_stmt).map_err(QueryError::from)?;
                Ok((t, result))
            })
            .into_fallible_result()
            .collect()?;

    // Merge per-step results into the series map.
    let mut series_map =
        halfbrown::HashMap::<Labels, Vec<Sample>, BuildHasherDefault<XxHash64>>::default();

    for (current_time, result) in step_results {
        match result {
            ExprResult::InstantVector(samples) => {
                for sample in samples {
                    let labels = sample.labels;
                    series_map
                        .entry(labels)
                        .or_default()
                        .push(Sample::new(sample.timestamp_ms, sample.value));
                }
            }
            ExprResult::Scalar(value) => {
                let labels = Labels::empty();
                series_map
                    .entry(labels)
                    .or_default()
                    .push(Sample::new(current_time, value));
            }
            ExprResult::RangeVector(_) => {
                return Err(QueryError::Execution(
                    "range vectors not supported in range query evaluation".to_string(),
                ));
            }
            ExprResult::String(_s) => {
                return Err(QueryError::Execution(
                    "string expressions not supported in range query evaluation".to_string(),
                ));
            }
        }
    }

    let result = series_map
        .into_iter()
        .map(|(labels, samples)| RangeSample { samples, labels })
        .collect();

    Ok(result)
}

/// Tsdb manages a unified Promql QueryReader interface
pub(crate) struct Tsdb {
    pub(crate) querier: Arc<dyn QueryReader>,
}

impl Tsdb {
    pub(crate) fn new(querier: Arc<dyn QueryReader>) -> Self {
        Self { querier }
    }

    pub fn eval(&self, stmt: EvalStmt) -> QueryResult<ExprResult> {
        let opts = QueryOptions {
            timeout: None,
            lookback_delta: stmt.lookback_delta,
            ..QueryOptions::default()
        };
        let evaluator = Evaluator::new(
            &self.querier,
            opts,
        );
        evaluator
            .evaluate(stmt)
            .map_err(|e| QueryError::Execution(e.to_string()))
    }

    /// Evaluate an instant PromQL query, returning typed `InstantSample`s.
    pub fn eval_query(
        &self,
        query: &str,
        time: Option<SystemTime>,
        opts: &QueryOptions,
    ) -> QueryResult<QueryValue> {
        let expr = promql_parser::parser::parse(query)
            .map_err(|e| QueryError::InvalidQuery(e.to_string()))?;

        let query_time = time.unwrap_or_else(SystemTime::now);
        let lookback_delta = opts.lookback_delta;
        let stmt = EvalStmt {
            expr,
            start: query_time,
            end: query_time,
            interval: Duration::ZERO,
            lookback_delta,
        };

        // Use the caller-supplied `opts` so caller-provided lookback_delta is respected,
        // but disable the evaluator timeout here.
        let evaluator = Evaluator::new(&self.querier, QueryOptions { timeout: None, ..*opts });
        let result = evaluator.evaluate(stmt)?;

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
                        labels: s.labels,
                        timestamp_ms: s.timestamp_ms,
                        value: s.value,
                    })
                    .collect(),
            )),
            ExprResult::RangeVector(samples) => Ok(QueryValue::Matrix(
                samples
                    .into_iter()
                    .map(|s| RangeSample {
                        labels: s.labels,
                        samples: s.values,
                    })
                    .collect(),
            )),
            ExprResult::String(s) => Ok(QueryValue::String(s)),
        }
    }

    /// Evaluate a range PromQL query, returning typed `RangeSample`s.
    pub fn eval_query_range(
        &self,
        query: &str,
        range: impl RangeBounds<SystemTime>,
        step: Duration,
        opts: &QueryOptions,
    ) -> QueryResult<Vec<RangeSample>> {
        let (start, end) = range_bounds_to_system_time(range);
        let expr = promql_parser::parser::parse(query)
            .map_err(|e| QueryError::InvalidQuery(e.to_string()))?;

        let lookback_delta = opts.lookback_delta;
        let stmt = EvalStmt {
            expr,
            start,
            end,
            interval: step,
            lookback_delta,
        };

        evaluate_range(self.querier.clone(), stmt, *opts)
    }
}

impl PromqlEngine for Tsdb {
    fn make_query_reader(&self) -> QueryResult<Arc<dyn QueryReader>> {
        Ok(self.querier.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::labels::Label;
    use crate::promql::engine::mock_series_querier::MockSeriesQuerier;

    fn create_tsdb() -> Tsdb {
        let querier = Arc::new(MockSeriesQuerier::new());
        Tsdb::new(querier)
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
            labels: Labels::new(labels),
            samples: vec![Sample { timestamp, value }],
        }
    }

    // ── Native read method tests ─────────────────────────────────────

    fn create_tsdb_with_data() -> Tsdb {
        let querier = MockSeriesQuerier::new();

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

        Tsdb::new(Arc::new(querier))
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
                    query_time.duration_since(UNIX_EPOCH).unwrap().as_millis() as i64
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
        let querier = Arc::new(MockSeriesQuerier::new());
        let tsdb = Tsdb::new(querier);
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
