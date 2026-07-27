use crate::common::{Sample, Timestamp};
use crate::labels::Labels;
use crate::labels::filters::SeriesSelector;
use crate::promql::EvalSample;
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
    let (Some(first_end), Some(last_end)) = (window_ends.first(), window_ends.last()) else {
        return Ok(Vec::new());
    };
    // Windows are half-open — `(end - range, end]` — and storage's `get_range`
    // takes an inclusive lower bound, so start one millisecond later.
    let start_time = (first_end - range_ms).saturating_add(1);
    let end_time = *last_end;

    let series = series_by_selectors(ctx, &[selector], None)?;
    validate_max_series(series.len(), max_series as usize)
        .map_err(valkey_module::ValkeyError::String)?;

    let windows = series
        .iter()
        .map(|(s, _)| s.deref())
        .iter_into_par()
        .filter_map(|s| {
            let samples = s.get_range(start_time, end_time);
            if samples.is_empty() {
                return None;
            }
            let labels: Labels = (&s.labels).into();
            Some(crate::promql::model::RangeSample { labels, samples })
        })
        .collect::<Vec<_>>();

    if max_points_per_series > 0 && max_points_per_series != u64::MAX {
        let limit = max_points_per_series as usize;
        for window in &windows {
            validate_max_points(window.samples.len(), Some(limit))
                .map_err(valkey_module::ValkeyError::String)?;
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
