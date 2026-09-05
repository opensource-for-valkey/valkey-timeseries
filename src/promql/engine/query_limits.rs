use crate::common::{Sample, Timestamp};
use crate::series::TimeSeries;

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
    // Chunk headers only describe the entire chunk. A chunk that overlaps the
    // query may contain many samples outside the requested interval, so its
    // length is an upper bound rather than the number of returned points.
    // Validate the decoded, range-filtered result to enforce the documented
    // per-series limit exactly.
    let samples = series.get_range(start_time, end_time);
    validate_max_points(samples.len(), max_points_per_series)?;
    Ok(samples)
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::series::SampleAddResult;

    #[test]
    fn range_limit_counts_only_samples_inside_the_requested_interval() {
        let mut series = TimeSeries::new();
        for timestamp in 0..=10 {
            assert!(matches!(
                series.add(timestamp, timestamp as f64, None),
                SampleAddResult::Ok(_)
            ));
        }

        let samples = get_series_range(&series, 5, 5, Some(1))
            .expect("one in-range sample must not be rejected by its larger chunk");

        assert_eq!(samples, vec![Sample::new(5, 5.0)]);
    }

    #[test]
    fn range_limit_rejects_when_the_filtered_result_exceeds_the_limit() {
        let mut series = TimeSeries::new();
        for timestamp in 0..=10 {
            assert!(matches!(
                series.add(timestamp, timestamp as f64, None),
                SampleAddResult::Ok(_)
            ));
        }

        let error = get_series_range(&series, 5, 7, Some(2))
            .expect_err("three in-range samples exceed the two-point limit");

        assert!(error.contains("3 > 2"));
    }
}
