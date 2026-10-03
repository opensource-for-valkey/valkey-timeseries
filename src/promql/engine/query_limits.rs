use crate::common::{Sample, Timestamp};
use crate::series::RangeSnapshot;

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

/// The samples of a [`RangeSnapshot`] under the per-series point limit,
/// decoding chunks the caller copied out under the module lock so that this
/// runs without it.
///
/// Chunk headers only describe the entire chunk. A chunk that overlaps the
/// query may contain many samples outside the requested interval, so its
/// length is an upper bound rather than the number of returned points. Stream
/// the range instead: a rejected query keeps at most the permitted samples and
/// the sample that proves the limit was exceeded.
pub(in crate::promql) fn get_snapshot_range(
    snapshot: &RangeSnapshot,
    max_points_per_series: Option<usize>,
) -> Result<Vec<Sample>, String> {
    let points_count = match max_points_per_series {
        Some(count) if count > 0 => count,
        _ => return Ok(snapshot.get_range()),
    };
    let mut samples = Vec::new();
    for sample in snapshot.range_iter() {
        if samples.len() >= points_count {
            validate_max_points(points_count.saturating_add(1), max_points_per_series)?;
        }
        samples.push(sample);
    }
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
    use crate::series::{SampleAddResult, TimeSeries};

    #[test]
    fn range_limit_counts_only_samples_inside_the_requested_interval() {
        let mut series = TimeSeries::new();
        for timestamp in 0..=10 {
            assert!(matches!(
                series.add(timestamp, timestamp as f64, None),
                SampleAddResult::Ok(_)
            ));
        }

        let samples = get_snapshot_range(&series.snapshot_range(5, 5), Some(1))
            .expect("one in-range sample must not be rejected by its larger chunk");

        assert_eq!(samples, vec![Sample::new(5, 5.0)]);
    }

    #[test]
    fn range_limit_allows_the_exact_in_range_boundary() {
        let mut series = TimeSeries::new();
        for timestamp in 0..=10 {
            assert!(matches!(
                series.add(timestamp, timestamp as f64, None),
                SampleAddResult::Ok(_)
            ));
        }

        let samples = get_snapshot_range(&series.snapshot_range(5, 7), Some(3))
            .expect("the configured boundary is inclusive");

        assert_eq!(
            samples,
            vec![
                Sample::new(5, 5.0),
                Sample::new(6, 6.0),
                Sample::new(7, 7.0)
            ]
        );
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

        let error = get_snapshot_range(&series.snapshot_range(5, 7), Some(2))
            .expect_err("three in-range samples exceed the two-point limit");

        assert!(error.contains("3 > 2"));
    }
}
