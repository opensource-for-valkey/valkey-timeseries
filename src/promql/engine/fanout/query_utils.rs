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
