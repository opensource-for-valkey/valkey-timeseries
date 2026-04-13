use crate::promql::engine::Tsdb;
use crate::promql::{QueryOptions, RangeSample};
use std::time::SystemTime;

/// Execute instant query and return structured results
pub(super) fn eval_instant(
    tsdb: &Tsdb,
    time: SystemTime,
    query: &str,
) -> Result<Vec<RangeSample>, String> {
    tsdb.eval_query(query, Some(time), &QueryOptions::default())
        .map_err(|e| e.to_string())
        .and_then(|qv| qv.into_matrix().map_err(|e| e.to_string()))
}

pub(super) fn eval_range(
    tsdb: &Tsdb,
    start: SystemTime,
    end: SystemTime,
    step: std::time::Duration,
    query: &str,
) -> Result<Vec<RangeSample>, String> {
    let options = QueryOptions::default();
    let range = start..=end;
    tsdb.eval_query_range(query, range, step, &options)
        .map_err(|e| e.to_string())
}
