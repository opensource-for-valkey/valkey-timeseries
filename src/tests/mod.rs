//! Test and benchmark support code.
//!
//! Compiled for unit tests, and for benchmarks / tools via the `test-utils`
//! feature (see the `[dev-dependencies]` self-dependency in `Cargo.toml`).

use crate::aggregators::AggregationType;
use crate::series::request_types::{AggregationOptions, AggregatorConfig};

pub mod chunk_utils;
pub mod generators;
pub mod parsers;
pub mod selectors;

/// Options for an aggregation over `columns`, each with its default parameters.
pub fn aggregation(
    columns: &[AggregationType],
    bucket_duration: u64,
    report_empty: bool,
) -> AggregationOptions {
    AggregationOptions {
        aggregations: columns
            .iter()
            .map(|&ty| AggregatorConfig::new(ty, None).unwrap())
            .collect(),
        bucket_duration,
        report_empty,
        ..Default::default()
    }
}

/// Whether two values are equal to 1e-9 relative to `expected`, with a floor of 1 on the scale.
/// NaN equals NaN.
pub fn values_close(actual: f64, expected: f64) -> bool {
    actual == expected
        || (actual.is_nan() && expected.is_nan())
        || (actual - expected).abs() <= 1e-9 * expected.abs().max(1.0)
}

#[track_caller]
pub fn assert_value_eq(actual: f64, expected: f64, context: &str) {
    assert!(
        values_close(actual, expected),
        "{context}: expected {expected}, got {actual}"
    );
}

/// Asserts that two lists of rows have the same timestamps and close values.
#[track_caller]
pub fn assert_rows_close(actual: &[(i64, f64)], expected: &[(i64, f64)]) {
    assert!(
        actual.len() == expected.len()
            && actual
                .iter()
                .zip(expected)
                .all(|(a, e)| a.0 == e.0 && values_close(a.1, e.1)),
        "{actual:?} != {expected:?}"
    );
}
