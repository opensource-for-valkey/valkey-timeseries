use crate::common::{Sample, Timestamp};
use crate::labels::Labels;
use crate::labels::filters::SeriesSelector;
use crate::promql::EvalSample;
use crate::promql::engine::query_reader::rollup_fetch_bounds;
use crate::promql::engine::{
    instant_lookback_start_ms, metric_name_to_proto_labels, validate_max_points,
    validate_max_series,
};
use crate::promql::generated::Sample as PromSample;
use crate::promql::generated::{
    InstantQueryResponse, InstantSample, RangeQueryResponse, RangeSample,
};
use crate::series::index::series_by_selectors;
use orx_parallel::IterIntoParIter;
use orx_parallel::ParIter;
use std::ops::Deref;
use valkey_module::{Context, ValkeyResult};

pub(super) fn handle_instant_query(
    ctx: &Context,
    selector: SeriesSelector,
    timestamp: Timestamp,
    lookback_delta: u64,
    max_series: u64,
    _max_points_per_series: u64,
) -> ValkeyResult<InstantQueryResponse> {
    let series = series_by_selectors(ctx, &[selector], None)?;
    let samples = series
        .iter()
        .map(|(s, k)| {
            let series = s.deref();
            let key = k.to_string();
            (series, key)
        })
        .iter_into_par()
        .filter_map(|(s, key)| {
            // in prometheus, given a timestamp and delta, we select the latest sample in the range
            // (ts - delta, ts], so we need to adjust the timestamp accordingly
            let start_time = instant_lookback_start_ms(timestamp, lookback_delta as i64);
            let end_time = timestamp;

            let sample = *s.get_range(start_time, end_time).last()?;
            let labels = metric_name_to_proto_labels(&s.labels);
            Some(InstantSample {
                labels,
                value: sample.value,
                timestamp: sample.timestamp,
                key,
            })
        })
        .collect::<Vec<_>>();

    validate_max_series(samples.len(), max_series as usize)
        .map_err(valkey_module::ValkeyError::String)?;

    Ok(InstantQueryResponse { samples })
}

/// Local instant-vector evaluation for the aggregation push-down.
///
/// Identical lookback semantics to [`handle_instant_query`], but yielding
/// evaluator-native samples so the aggregation operators can be applied to them
/// directly instead of round-tripping through the wire types. Only the
/// aggregated result crosses the wire, which is the point of the push-down.
pub(super) fn local_instant_eval_samples(
    ctx: &Context,
    selector: SeriesSelector,
    timestamp: Timestamp,
    lookback_delta: u64,
    max_series: u64,
) -> ValkeyResult<Vec<EvalSample>> {
    let series = series_by_selectors(ctx, &[selector], None)?;
    // Prometheus selects the latest sample in (ts - delta, ts].
    let start_time = instant_lookback_start_ms(timestamp, lookback_delta as i64);

    let samples = series
        .iter()
        .map(|(s, _)| s.deref())
        .iter_into_par()
        .filter_map(|s| {
            let sample = *s.get_range(start_time, timestamp).last()?;
            let labels: Labels = (&s.labels).into();
            Some(EvalSample {
                labels: labels.into(),
                value: sample.value,
                timestamp_ms: sample.timestamp,
                drop_name: false,
            })
        })
        .collect::<Vec<_>>();

    // Bound the shard's own working set, exactly as the unaggregated instant
    // query does. The coordinator additionally bounds the aggregated result.
    validate_max_series(samples.len(), max_series as usize)
        .map_err(valkey_module::ValkeyError::String)?;

    Ok(samples)
}

/// Read the raw windows a pushed-down rollup needs, one entry per series.
///
/// The samples returned are exactly those inside the union of the requested
/// windows — `(first_end - range_ms, last_end]` — so the shard reduces the same
/// data the coordinator's own matrix selector would have loaded. Series with no
/// samples in that span are dropped: an empty window contributes nothing.
///
/// `max_points_per_series` bounds the *raw* points examined per series, which is
/// the resource this push-down is trading away; the coordinator separately
/// bounds the rolled-up points it accepts back.
pub(super) fn local_rollup_windows(
    ctx: &Context,
    selector: SeriesSelector,
    window_ends: &[Timestamp],
    range_ms: i64,
    max_series: u64,
    max_points_per_series: u64,
) -> ValkeyResult<Vec<crate::promql::model::RangeSample>> {
    let Some((start_time, end_time)) = rollup_fetch_bounds(window_ends, range_ms) else {
        return Ok(Vec::new());
    };

    let series = series_by_selectors(ctx, &[selector], None)?;

    let candidates = series
        .iter()
        .map(|(s, _)| s.deref())
        .iter_into_par()
        .map(|s| {
            let samples = s.get_range(start_time, end_time);
            // An empty window contributes nothing, so skip the label conversion
            // for it as well — matched-but-empty series are the common case for
            // a wide selector over a narrow time range.
            (!samples.is_empty()).then(|| {
                let labels: Labels = (&s.labels).into();
                crate::promql::model::RangeSample { labels, samples }
            })
        })
        .collect::<Vec<_>>();

    bound_windows(candidates, max_series, max_points_per_series)
        .map_err(valkey_module::ValkeyError::String)
}

/// Drop the matched-but-empty series, then apply the query limits to what is
/// left.
///
/// The order matters: `max_series` bounds the series the query actually
/// *returns*, not the ones the selector happened to match. Validating the match
/// count instead would make the push-down reject queries that the unaggregated
/// range path — which filters first, see [`handle_range_query`] — accepts, so
/// whether a query succeeded would depend on an internal optimization decision.
fn bound_windows(
    candidates: Vec<Option<crate::promql::model::RangeSample>>,
    max_series: u64,
    max_points_per_series: u64,
) -> Result<Vec<crate::promql::model::RangeSample>, String> {
    let windows: Vec<_> = candidates.into_iter().flatten().collect();

    validate_max_series(windows.len(), max_series as usize)?;

    if max_points_per_series > 0 && max_points_per_series != u64::MAX {
        let limit = max_points_per_series as usize;
        for window in &windows {
            validate_max_points(window.samples.len(), Some(limit))?;
        }
    }

    Ok(windows)
}

pub(super) fn handle_range_query(
    ctx: &Context,
    selector: SeriesSelector,
    start_time: i64,
    end_time: i64,
    max_series: u64,
    max_points_per_series: u64,
) -> ValkeyResult<RangeQueryResponse> {
    let series = series_by_selectors(ctx, &[selector], None)?;
    let ranges = series
        .iter()
        .map(|(s, _)| s.deref())
        .iter_into_par()
        .filter_map(|s| {
            let series_samples = s.get_range(start_time, end_time);
            if series_samples.is_empty() {
                return None;
            }
            let samples: Vec<PromSample> = series_samples.into_iter().map(Sample::into).collect();
            let labels = metric_name_to_proto_labels(&s.labels);
            let range = RangeSample {
                labels,
                samples,
                key: "".to_string(),
            };
            Some(range)
        })
        .collect::<Vec<_>>();

    validate_max_series(ranges.len(), max_series as usize)
        .map_err(valkey_module::ValkeyError::String)?;

    if max_points_per_series > 0 && max_points_per_series != u64::MAX {
        let limit = max_points_per_series as usize;
        for range in &ranges {
            validate_max_points(range.samples.len(), Some(limit))
                .map_err(valkey_module::ValkeyError::String)?;
        }
    }

    Ok(RangeQueryResponse { series: ranges })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::promql::model::RangeSample;

    fn sample(timestamp: Timestamp, value: f64) -> Sample {
        Sample { timestamp, value }
    }

    /// A series the selector matched that holds samples in the queried span.
    fn filled(name: &str, count: usize) -> Option<RangeSample> {
        Some(RangeSample {
            labels: Labels::from_pairs(&[("__name__", name)]),
            samples: (0..count as i64)
                .map(|i| sample(1000 + i, i as f64))
                .collect(),
        })
    }

    /// A series the selector matched whose windows are all empty.
    fn empty() -> Option<RangeSample> {
        None
    }

    /// A wide selector over a narrow span: matched series that contribute
    /// nothing must not count against `max_series`, otherwise the push-down
    /// rejects a query the unaggregated range path answers.
    #[test]
    fn matched_but_empty_series_do_not_count_against_max_series() {
        let mut candidates = vec![empty(); 100];
        candidates.push(filled("only_one_with_data", 3));

        let windows = bound_windows(candidates, 100, 0).expect("101 matched, 1 returned");

        assert_eq!(windows.len(), 1);
        assert_eq!(windows[0].samples.len(), 3);
    }

    #[test]
    fn returned_series_over_the_limit_are_rejected() {
        let candidates: Vec<_> = (0..101).map(|i| filled(&format!("s{i}"), 1)).collect();

        let err = bound_windows(candidates, 100, 0).expect_err("101 returned > 100");

        assert!(err.contains("101 > 100"), "unexpected message: {err}");
    }

    #[test]
    fn per_series_point_limit_applies_to_the_surviving_windows() {
        let candidates = vec![empty(), filled("wide", 5), empty()];

        let err = bound_windows(candidates, 0, 4).expect_err("5 points > 4");

        assert!(err.contains("5 > 4"), "unexpected message: {err}");
    }

    #[test]
    fn zero_means_unlimited_for_both_limits() {
        let candidates: Vec<_> = (0..10).map(|i| filled(&format!("s{i}"), 10)).collect();

        let windows = bound_windows(candidates, 0, 0).expect("no limits configured");

        assert_eq!(windows.len(), 10);
    }
}
