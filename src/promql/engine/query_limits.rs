use crate::common::{Sample, Timestamp};
use crate::series::{TimeSeries, chunks::ChunkOps};

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

pub(in crate::promql) fn get_series_range(
    series: &TimeSeries,
    start_time: Timestamp,
    end_time: Timestamp,
    max_points_per_series: Option<usize>,
) -> Result<Vec<Sample>, String> {
    let Some(points_count) = max_points_per_series else {
        let samples = series.get_range(start_time, end_time);
        return Ok(samples);
    };

    if points_count == 0 {
        let samples = series.get_range(start_time, end_time);
        return Ok(samples);
    }

    if !series.overlaps(start_time, end_time) {
        return Ok(Vec::new());
    }
    let Some(range) = series.get_chunk_index_bounds(start_time, end_time) else {
        return Ok(Vec::new());
    };

    // do a cheap check to see if the sample count exceed the max, without decoding
    let (start_index, end_index) = range;
    let chunks = &series.chunks[start_index..=end_index];
    // only count points in the chunks that overlap with the range, as some chunks may be partially outside the range
    // this allows us to avoid decoding if it's not necessary
    let count: usize = chunks
        .iter()
        .filter_map(|chunk| {
            if chunk.overlaps(start_time, end_time) {
                Some(chunk.len())
            } else {
                None
            }
        })
        .sum();
    if count > points_count {
        Err(format!(
            "{MAX_POINTS_PER_SERIES_ERROR_MSG}: {count} > {points_count}",
        ))
    } else {
        let samples = series.get_range(start_time, end_time);
        validate_max_points(samples.len(), max_points_per_series)?;
        Ok(samples)
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
