use crate::commands::fanout_codec::chunks::serialize_chunk;
use crate::commands::fanout_codec::symbol_table;
use crate::common::Timestamp;
use crate::common::threads::IntoParRayon;
use crate::labels::filters::SeriesSelector;
use crate::promql::EvalLabels;
use crate::promql::EvalSample;
use crate::promql::engine::label_profile::{LabelProfile, LabelProfileBuilder};
use crate::promql::engine::promql_config;
use crate::promql::engine::query_reader::grid_fetch_bounds;
use crate::promql::engine::sample_budget::{SampleBudget, too_many_samples};
use crate::promql::engine::{
    get_snapshot_range, instant_lookback_start_ms, validate_max_points, validate_max_series,
};
use crate::promql::generated::{
    InstantQueryResponse, InstantSample, RangeQueryResponse, RangeSample,
};
use crate::series::chunks::samples_to_chunk_lossless;
use crate::series::index::{
    DEFAULT_SERIES_BATCH_SIZE, GilSource, for_each_series, for_each_series_batch_then,
};
use crate::series::{RangeSnapshot, TimeSeries};
use orx_parallel::Par;
use orx_parallel::ParResult;
use std::ops::ControlFlow;
use valkey_module::ValkeyResult;

/// This shard's share of an instant-vector selector: the newest sample in the
/// lookback window of every matched series, read one batch per lock hold. The
/// head chunk's cached last sample usually answers without a decode, so all of
/// it runs under the lock.
pub(super) fn handle_instant_query<S: GilSource + ?Sized>(
    src: &S,
    selector: SeriesSelector,
    timestamp: Timestamp,
    lookback_delta: u64,
    max_series: u64,
    _max_points_per_series: u64,
) -> ValkeyResult<InstantQueryResponse> {
    // in prometheus, given a timestamp and delta, we select the latest sample in the range
    // (ts - delta, ts], so we need to adjust the timestamp accordingly
    let start_time = instant_lookback_start_ms(timestamp, lookback_delta as i64);
    let end_time = timestamp;

    // Labels go straight from storage into the response's symbol table by
    // identity; no owned label strings are built per series. Interning under
    // each batch's lock is safe across them: the builder keeps every entry it
    // has keyed alive, so an address cannot be reused by another label.
    let mut symbol_table = symbol_table::SymbolTableBuilder::default();
    let mut samples = Vec::new();
    for_each_series(
        src,
        &[selector],
        None,
        DEFAULT_SERIES_BATCH_SIZE,
        |_, series, _| {
            if let Some(sample) = series.last_sample_in_range(start_time, end_time) {
                let (label_name_refs, label_value_refs) = symbol_table.intern(&series.labels);
                samples.push(InstantSample {
                    labels: Vec::new(),
                    value: sample.value,
                    timestamp: sample.timestamp,
                    label_name_refs,
                    label_value_refs,
                });
            }
            Ok(ControlFlow::Continue(()))
        },
    )?;
    let symbol_table = symbol_table.finish();

    validate_max_series(samples.len(), max_series as usize)
        .map_err(valkey_module::ValkeyError::String)?;

    Ok(InstantQueryResponse {
        samples,
        labels: Some(symbol_table),
    })
}

/// Local instant-vector evaluation for the aggregation push-down.
///
/// Identical lookback semantics to [`handle_instant_query`], but yielding
/// evaluator-native samples so the aggregation operators can be applied to them
/// directly instead of round-tripping through the wire types. Only the
/// aggregated result crosses the wire, which is the point of the push-down.
pub(super) fn local_instant_eval_samples<S: GilSource + ?Sized>(
    src: &S,
    selector: SeriesSelector,
    timestamp: Timestamp,
    lookback_delta: u64,
    max_series: u64,
) -> ValkeyResult<Vec<EvalSample>> {
    // Prometheus selects the latest sample in (ts - delta, ts].
    let start_time = instant_lookback_start_ms(timestamp, lookback_delta as i64);

    let mut samples = Vec::new();
    for_each_series(
        src,
        &[selector],
        None,
        DEFAULT_SERIES_BATCH_SIZE,
        |_, series, _| {
            if let Some(sample) = series.last_sample_in_range(start_time, timestamp) {
                samples.push(EvalSample {
                    // Shares the series' label set; nothing borrows the series.
                    labels: EvalLabels::interned(&series.labels),
                    value: sample.value,
                    timestamp_ms: sample.timestamp,
                    drop_name: false,
                });
            }
            Ok(ControlFlow::Continue(()))
        },
    )?;

    // Bound the shard's own working set, exactly as the unaggregated instant
    // query does. The coordinator additionally bounds the aggregated result.
    validate_max_series(samples.len(), max_series as usize)
        .map_err(valkey_module::ValkeyError::String)?;

    Ok(samples)
}

/// The labels of the series `selector` matches on this node: the local half
/// of [`super::LabelProfileFanoutCommand`], and what a single node answers
/// `QueryReader::label_profile` with. `None` past `max_series` matches, and
/// for the same reason the coordinator has: a selector that large is not
/// worth walking to narrow another.
///
/// No sample is read. Every matched key is opened, as a read of the same
/// selector would, until the cap is passed; the walk stops there.
pub(in crate::promql) fn local_label_profile<S: GilSource + ?Sized>(
    src: &S,
    selector: SeriesSelector,
    max_series: usize,
) -> ValkeyResult<Option<LabelProfile>> {
    let mut builder = LabelProfileBuilder::new();
    let mut matched = 0usize;
    let mut overflow = false;
    for_each_series(
        src,
        &[selector],
        None,
        DEFAULT_SERIES_BATCH_SIZE,
        |_, series, _| {
            matched += 1;
            if max_series > 0 && matched > max_series {
                overflow = true;
                return Ok(ControlFlow::Break(()));
            }
            // The builder copies what it keeps, so nothing outlives the lock.
            builder.add_series(series.labels.iter().map(|l| (l.name, l.value)));
            Ok(ControlFlow::Continue(()))
        },
    )?;
    Ok((!overflow).then(|| builder.finish()))
}

/// Read the chunks `[start_time, end_time]` touches in every series `selector`
/// matches, a batch at a time: copy a batch's chunks under its lock, then hand
/// them to `decode` once the lock is released, before the next batch takes it.
/// A shard so never holds more than one batch of decoded samples, and the
/// decoding is the gap in which the main thread gets the lock back.
///
/// Only series with a chunk in the span are kept, with `labels` of each: the
/// evaluator's shared set, or the storage set itself for a response whose
/// symbol table interns by identity. Either shares the series' `Arc`, so no
/// string is copied and nothing borrows the series.
///
/// The copy is sequential on purpose. A batch is a few hundred KB, and fanning
/// it out across the shared pool made the lock holder wait for workers busy
/// decoding other queries' snapshots: with 4 concurrent rollups the main
/// thread's PING p99 stayed at 85–100 ms, against 6–15 ms sequential.
fn for_each_snapshot_batch<S, L, T>(
    src: &S,
    selector: SeriesSelector,
    start_time: Timestamp,
    end_time: Timestamp,
    labels: L,
    mut decode: impl FnMut(Vec<(T, RangeSnapshot)>) -> ValkeyResult<()>,
) -> ValkeyResult<()>
where
    S: GilSource + ?Sized,
    L: Fn(&TimeSeries) -> T,
{
    for_each_series_batch_then(
        src,
        &[selector],
        None,
        DEFAULT_SERIES_BATCH_SIZE,
        |_, batch| {
            Ok(batch
                .iter()
                .filter_map(|(s, _)| {
                    let snapshot = s.snapshot_range(start_time, end_time);
                    (!snapshot.is_empty()).then(|| (labels(s), snapshot))
                })
                .collect::<Vec<_>>())
        },
        |snapshots| {
            if !snapshots.is_empty() {
                decode(snapshots)?;
            }
            Ok(ControlFlow::Continue(()))
        },
    )
}

/// Read the raw windows a pushed-down grid query needs, one entry per series,
/// handing them to `sink` a batch at a time.
///
/// The samples are exactly those inside the union of the requested windows —
/// `(first_end - backward_ms, last_end]`, where `backward_ms` is the window
/// width for a rollup and the lookback for a stepped selection — so the shard
/// evaluates the same data the coordinator's own selector would have loaded.
/// Series with no samples in that span are dropped: an empty window
/// contributes nothing.
///
/// `max_points_per_series` bounds the *raw* points examined per series, which is
/// the resource this push-down is trading away; the coordinator separately
/// bounds the points it accepts back. Both limits are judged over the whole
/// read, so a violation is reported only once every batch is in (see
/// [`WindowBounds`]), after `sink` has seen them.
pub(super) fn local_grid_windows<S: GilSource + ?Sized>(
    src: &S,
    selector: SeriesSelector,
    window_ends: &[Timestamp],
    backward_ms: i64,
    max_series: u64,
    max_points_per_series: u64,
    mut sink: impl FnMut(Vec<crate::promql::model::RangeSample<EvalLabels>>),
) -> ValkeyResult<()> {
    let Some((start_time, end_time)) = grid_fetch_bounds(window_ends, backward_ms) else {
        return Ok(());
    };

    let budget = SampleBudget::new(local_max_samples());
    let mut bounds = WindowBounds::new(max_series, max_points_per_series);
    for_each_snapshot_batch(
        src,
        selector,
        start_time,
        end_time,
        |s| EvalLabels::interned(&s.labels),
        |snapshots| {
            let candidates = snapshots
                .into_par_rayon()
                .map(|(labels, snapshot)| {
                    if budget.exhausted() {
                        return Err(too_many_samples(budget.loaded(), budget.limit()).to_string());
                    }
                    let samples = snapshot.get_range();
                    budget
                        .charge(samples.len())
                        .map_err(|err| err.to_string())?;
                    // A snapshot with chunks can still hold nothing inside the
                    // span: the chunks only overlap it.
                    Ok((!samples.is_empty())
                        .then_some(crate::promql::model::RangeSample { labels, samples }))
                })
                .into_fallible()
                .collect::<Vec<_>>()
                .map_err(valkey_module::ValkeyError::String)?;
            let windows = bounds.admit(candidates);
            if !windows.is_empty() {
                sink(windows);
            }
            Ok(())
        },
    )?;

    bounds.finish().map_err(valkey_module::ValkeyError::String)
}

/// This node's `ts-promql-max-samples-per-query`, applied to the reads it
/// performs on another node's behalf.
fn local_max_samples() -> usize {
    // Not `unwrap_or(0)` on a poisoned lock: 0 means unlimited, so a poisoned
    // lock would have lifted this node's sample budget.
    promql_config().max_samples_per_query
}

/// The query limits on a shard's grid windows, applied as batches of them are
/// decoded: drop the matched-but-empty series, then bound what is left.
///
/// `max_series` bounds the series the query actually *returns*, not the ones
/// the selector happened to match. Validating the match count instead would
/// make the push-down reject queries that the unaggregated range path — which
/// filters first, see [`handle_range_query`] — accepts, so whether a query
/// succeeded would depend on an internal optimization decision.
///
/// That count is only known once every batch is in. A window over the point
/// limit is remembered rather than reported, so a read over both limits still
/// fails on `max_series`, as it did when the windows were bounded all at once.
struct WindowBounds {
    max_series: u64,
    max_points: Option<usize>,
    returned: usize,
    over_points: Option<String>,
}

impl WindowBounds {
    fn new(max_series: u64, max_points_per_series: u64) -> Self {
        Self {
            max_series,
            max_points: points_limit(max_points_per_series),
            returned: 0,
            over_points: None,
        }
    }

    /// The non-empty windows of one batch, counted against the limits.
    fn admit(
        &mut self,
        candidates: Vec<Option<crate::promql::model::RangeSample<EvalLabels>>>,
    ) -> Vec<crate::promql::model::RangeSample<EvalLabels>> {
        let windows: Vec<_> = candidates.into_iter().flatten().collect();
        self.returned += windows.len();
        if self.over_points.is_none()
            && let Some(limit) = self.max_points
        {
            self.over_points = windows
                .iter()
                .find_map(|w| validate_max_points(w.samples.len(), Some(limit)).err());
        }
        windows
    }

    /// The verdict over every batch admitted.
    fn finish(self) -> Result<(), String> {
        validate_max_series(self.returned, self.max_series as usize)?;
        self.over_points.map_or(Ok(()), Err)
    }
}

/// Translate the wire form of the per-series point limit for [`get_snapshot_range`].
///
/// On the wire both `0` and `u64::MAX` mean "unlimited". `get_snapshot_range`
/// already reads `Some(0)` that way, but `Some(u64::MAX as usize)` would be a
/// real limit, so both sentinels become `None` before the limit reaches it.
fn points_limit(max_points_per_series: u64) -> Option<usize> {
    (max_points_per_series > 0 && max_points_per_series != u64::MAX)
        .then_some(max_points_per_series as usize)
}

/// This shard's share of a range-vector selector: every matched series' raw
/// span, encoded for the wire. Each batch is decoded and encoded as soon as
/// its lock is released, so what accumulates is compressed chunks, never the
/// whole share's decoded samples.
pub(super) fn handle_range_query<S: GilSource + ?Sized>(
    src: &S,
    selector: SeriesSelector,
    start_time: i64,
    end_time: i64,
    max_series: u64,
    max_points_per_series: u64,
) -> ValkeyResult<RangeQueryResponse> {
    let max_points = points_limit(max_points_per_series);
    // This shard's own `ts-promql-max-samples-per-query`: the request does not
    // carry the coordinator's budget, and one node's share of a query should
    // not exceed what that node would allow a query of its own.
    let budget = SampleBudget::new(local_max_samples());
    let mut ranges = Vec::new();
    // The storage label set itself, not an evaluator copy: the symbol table
    // below interns by identity.
    for_each_snapshot_batch(
        src,
        selector,
        start_time,
        end_time,
        |s| s.labels.clone(),
        |snapshots| {
            let batch = snapshots
                .into_par_rayon()
                .map(|(labels, snapshot)| {
                    if budget.exhausted() {
                        return Err(too_many_samples(budget.loaded(), budget.limit()).to_string());
                    }
                    // Streams against the per-series point limit, so an
                    // over-wide span is rejected having kept at most the
                    // permitted samples.
                    let series_samples = get_snapshot_range(&snapshot, max_points)?;
                    budget
                        .charge(series_samples.len())
                        .map_err(|err| err.to_string())?;
                    if series_samples.is_empty() {
                        return Ok(None);
                    }
                    let data = serialize_chunk(samples_to_chunk_lossless(series_samples))
                        .map_err(|e| e.to_string())?;
                    Ok(Some((labels, data)))
                })
                .into_fallible()
                .filter_map(|range| range)
                .collect::<Vec<_>>()
                .map_err(valkey_module::ValkeyError::String)?;
            ranges.extend(batch);
            Ok(())
        },
    )?;

    validate_max_series(ranges.len(), max_series as usize)
        .map_err(valkey_module::ValkeyError::String)?;

    // Labels go into the response's symbol table by identity, as for an
    // instant query: serially, since the table is one per response, and after
    // the parallel read so only the series that ship are interned.
    let mut symbol_table = symbol_table::SymbolTableBuilder::default();
    let series = ranges
        .into_iter()
        .map(|(labels, data)| {
            let (label_name_refs, label_value_refs) = symbol_table.intern(&labels);
            RangeSample {
                labels: Vec::new(),
                data: Some(data),
                label_name_refs,
                label_value_refs,
            }
        })
        .collect();

    Ok(RangeQueryResponse {
        series,
        labels: Some(symbol_table.finish()),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::Sample;
    use crate::promql::model::RangeSample;

    /// [`WindowBounds`] over a read that arrives in one batch.
    fn bound_windows(
        candidates: Vec<Option<RangeSample<EvalLabels>>>,
        max_series: u64,
        max_points_per_series: u64,
    ) -> Result<Vec<RangeSample<EvalLabels>>, String> {
        let mut bounds = WindowBounds::new(max_series, max_points_per_series);
        let windows = bounds.admit(candidates);
        bounds.finish().map(|()| windows)
    }

    fn sample(timestamp: Timestamp, value: f64) -> Sample {
        Sample { timestamp, value }
    }

    /// A series the selector matched that holds samples in the queried span.
    fn filled(name: &str, count: usize) -> Option<RangeSample<EvalLabels>> {
        Some(RangeSample {
            labels: EvalLabels::from_pairs(&[("__name__", name)]),
            samples: (0..count as i64)
                .map(|i| sample(1000 + i, i as f64))
                .collect(),
        })
    }

    /// A series the selector matched whose windows are all empty.
    fn empty() -> Option<RangeSample<EvalLabels>> {
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

    /// The limits are judged over the whole read, not per batch: three batches
    /// of 40 returned series exceed a limit of 100 that none exceeds alone.
    #[test]
    fn max_series_counts_every_batch() {
        let mut bounds = WindowBounds::new(100, 0);
        for b in 0..3 {
            let batch: Vec<_> = (0..40).map(|i| filled(&format!("s{b}_{i}"), 1)).collect();
            assert_eq!(bounds.admit(batch).len(), 40);
        }

        let err = bounds.finish().expect_err("120 returned > 100");

        assert!(err.contains("120 > 100"), "unexpected message: {err}");
    }

    /// A batch over the point limit early in the read does not pre-empt the
    /// series-count verdict, which needs every batch: over both, the read fails
    /// on `max_series`, as it did bounded all at once.
    #[test]
    fn max_series_outranks_an_earlier_point_violation() {
        let mut bounds = WindowBounds::new(2, 4);
        bounds.admit(vec![filled("wide", 5)]);
        bounds.admit(vec![filled("a", 1), filled("b", 1)]);

        let err = bounds.finish().expect_err("3 returned > 2");

        assert!(err.contains("3 > 2"), "unexpected message: {err}");
    }

    #[test]
    fn a_point_violation_in_any_batch_fails_the_read() {
        let mut bounds = WindowBounds::new(0, 4);
        bounds.admit(vec![filled("a", 1)]);
        bounds.admit(vec![empty(), filled("wide", 5)]);
        bounds.admit(vec![filled("b", 1)]);

        let err = bounds.finish().expect_err("5 points > 4");

        assert!(err.contains("5 > 4"), "unexpected message: {err}");
    }
}
