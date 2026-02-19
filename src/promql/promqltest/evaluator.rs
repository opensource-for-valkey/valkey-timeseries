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
