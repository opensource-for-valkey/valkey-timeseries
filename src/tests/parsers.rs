//! Parser entry points for benchmarks, which cannot reach the crate-private parser. Errors are
//! reduced to `None`: `ParseError` is not part of the public surface.

use crate::labels::Label;
use crate::labels::filters::SeriesSelector;
use crate::parser::series_selector::parse_series_selector;

pub fn try_parse_selector(s: &str) -> Option<SeriesSelector> {
    parse_series_selector(s).ok()
}

pub fn try_parse_metric_name(s: &str) -> Option<Vec<Label>> {
    crate::parser::metric_name::parse_metric_name(s).ok()
}

pub fn try_parse_duration(s: &str) -> Option<i64> {
    crate::parser::parse_duration_value(s).ok()
}

pub fn try_parse_timestamp(s: &str) -> Option<i64> {
    crate::parser::timestamp::parse_timestamp(s).ok()
}

pub fn try_parse_number(s: &str) -> Option<f64> {
    crate::parser::number::parse_number(s).ok()
}
