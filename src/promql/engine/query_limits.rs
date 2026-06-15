use crate::common::Timestamp;

pub(crate) const MAX_SERIES_ERROR_MSG: &str =
    "the query returns more than the configured max series limit";
pub(crate) const MAX_POINTS_PER_SERIES_ERROR_MSG: &str =
    "the query returns a series with more points than the configured max points per series limit";

pub(in crate::promql) fn instant_lookback_start_ms(
    timestamp: Timestamp,
    lookback_delta_ms: Timestamp,
) -> Timestamp {
    timestamp
        .saturating_sub(lookback_delta_ms)
        .saturating_add(1)
}

pub(in crate::promql) fn validate_max_series(
    series_count: usize,
    max_series: usize,
) -> Result<(), String> {
    if max_series > 0 && series_count > max_series {
        Err(format!(
            "{}: {} > {}",
            MAX_SERIES_ERROR_MSG, series_count, max_series
        ))
    } else {
        Ok(())
    }
}

pub(in crate::promql) fn validate_max_points(
    points_count: usize,
    max_points: Option<usize>,
) -> Result<(), String> {
    if let Some(max) = max_points
        && max > 0
        && points_count > max
    {
        Err(format!(
            "{}: {} > {}",
            MAX_POINTS_PER_SERIES_ERROR_MSG, points_count, max
        ))
    } else {
        Ok(())
    }
}
