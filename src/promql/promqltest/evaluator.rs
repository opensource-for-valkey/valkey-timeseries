use crate::promql::{QueryOptions, QueryValue, RangeSample};
use std::time::SystemTime;
use crate::promql::engine::promql_engine::Tsdb;

/// Execute instant query and return structured results
pub(super) fn eval_instant(
    tsdb: &Tsdb,
    time: SystemTime,
    query: &str,
) -> Result<QueryValue, String> {
    // Quick debug: attempt to parse the query directly to get a more detailed parser error
    match promql_parser::parser::parse(query) {
        Ok(_) => {
            // parsed OK, proceed
            tsdb.eval_query(query, Some(time), &QueryOptions::default())
                .map_err(|e| e.to_string())
        }
        Err(e) => {
            eprintln!(
                "[promqltest] promql parse error for query '{}': {:?}",
                query, e
            );
            // Try to normalize whitespace and retry (tests may contain odd spacing)
            let normalized = regex::Regex::new(r"\s+")
                .unwrap()
                .replace_all(query, " ")
                .to_string();
            if promql_parser::parser::parse(&normalized).is_ok() {
                eprintln!(
                    "[promqltest] retrying with normalized query: '{}'",
                    normalized
                );
                return tsdb
                    .eval_query(&normalized, Some(time), &QueryOptions::default())
                    .map_err(|e| e.to_string());
            }
            // fall through to attempt evaluation which will return error
            tsdb.eval_query(query, Some(time), &QueryOptions::default())
                .map_err(|err| err.to_string())
        }
    }
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
