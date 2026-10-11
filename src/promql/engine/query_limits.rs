use crate::common::{Sample, Timestamp};
use crate::error_consts;
use crate::promql::QueryResult;
use crate::promql::engine::sample_budget::{SampleBudget, too_many_samples};
use crate::series::RangeSnapshot;

pub(crate) const MAX_SERIES_ERROR_MSG: &str = error_consts::PROMQL_MAX_SERIES_ERROR;
pub(crate) const MAX_POINTS_PER_SERIES_ERROR_MSG: &str =
    error_consts::PROMQL_MAX_POINTS_PER_SERIES_ERROR;

pub(in crate::promql) fn instant_lookback_start_ms(
    timestamp: Timestamp,
    lookback_delta_ms: Timestamp,
) -> Timestamp {
    timestamp
        .saturating_sub(lookback_delta_ms)
        .saturating_add(1)
}

/// Whether `series_count` returned series already exceed `max_series` (0: no limit).
///
/// The count a read returns only grows, so once this holds [`validate_max_series`] will refuse
/// the read however many more series it finds: a read stops walking here rather than open and
/// collect every series the selector matched first. The refusal then reports the count at
/// which the read stopped, not the total it would have reached.
pub(in crate::promql) fn exceeds_max_series(series_count: usize, max_series: usize) -> bool {
    max_series > 0 && series_count > max_series
}

pub(in crate::promql) fn validate_max_series(
    series_count: usize,
    max_series: usize,
) -> Result<(), String> {
    if exceeds_max_series(series_count, max_series) {
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

/// Whether a range read should decode `snapshot`, given what `budget` has left: the early
/// stop that refuses a read past its budget without decoding the rest of its series.
///
/// A budget spent exactly is not a refusal by itself: a snapshot's chunks only overlap the
/// span, so a series can hold nothing inside it, and charging nothing is within the limit.
/// At exactly the limit this decodes up to the first in-span sample to tell which; past it
/// the query is already refused and nothing is decoded. `Ok(false)`: skip, nothing to charge.
pub(in crate::promql) fn decode_within_budget(
    budget: &SampleBudget,
    snapshot: &RangeSnapshot,
) -> QueryResult<bool> {
    let (loaded, limit) = (budget.loaded(), budget.limit());
    if limit == 0 || loaded < limit {
        return Ok(true);
    }
    if loaded == limit && snapshot.range_iter().next().is_none() {
        return Ok(false);
    }
    // At least one sample past the limit, so never the contradictory `limit > limit`.
    Err(too_many_samples(loaded.max(limit + 1), limit))
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

    /// One chunk holding samples at 0 and 10, so `[3, 7]` overlaps it with nothing inside.
    fn series_with_a_gap() -> TimeSeries {
        let mut series = TimeSeries::new();
        for timestamp in [0, 10] {
            assert!(matches!(
                series.add(timestamp, timestamp as f64, None),
                SampleAddResult::Ok(_)
            ));
        }
        series
    }

    #[test]
    fn a_spent_budget_skips_a_series_empty_in_the_span() {
        let series = series_with_a_gap();
        let gap = series.snapshot_range(3, 7);
        assert!(!gap.is_empty(), "the chunk overlaps the span");
        let budget = SampleBudget::new(1);
        budget.charge(1).unwrap();

        assert!(!decode_within_budget(&budget, &gap).expect("charging nothing fits"));
        assert_eq!(budget.loaded(), 1, "the check charges nothing");
    }

    #[test]
    fn a_spent_budget_refuses_a_series_with_samples_in_the_span() {
        let series = series_with_a_gap();
        let budget = SampleBudget::new(1);
        budget.charge(1).unwrap();

        let err = decode_within_budget(&budget, &series.snapshot_range(0, 10))
            .expect_err("one more sample is past the limit")
            .to_string();
        assert!(err.contains("2 > 1"), "{err}");

        // Already past the limit: refused as it stands, empty span or not.
        let _ = budget.charge(5);
        let err = decode_within_budget(&budget, &series.snapshot_range(3, 7))
            .expect_err("the query is already refused")
            .to_string();
        assert!(err.contains("6 > 1"), "{err}");
    }

    #[test]
    fn a_budget_with_room_decodes() {
        let series = series_with_a_gap();
        let snapshot = series.snapshot_range(0, 10);
        assert!(decode_within_budget(&SampleBudget::new(0), &snapshot).unwrap());
        let budget = SampleBudget::new(2);
        budget.charge(1).unwrap();
        assert!(decode_within_budget(&budget, &snapshot).unwrap());
    }
}
