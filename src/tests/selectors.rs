//! Selector parsing for benchmarks, which cannot reach the crate-private parser.

use crate::labels::filters::SeriesSelector;
use crate::parser::series_selector::parse_series_selector;

/// Parses a RedisTimeSeries or Prometheus selector; panics on invalid input.
pub fn parse_selector(s: &str) -> SeriesSelector {
    parse_series_selector(s).unwrap_or_else(|e| panic!("invalid selector {s:?}: {e:?}"))
}
