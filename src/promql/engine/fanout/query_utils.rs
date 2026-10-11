use crate::commands::fanout_codec::chunks::serialize_chunk;
use crate::commands::fanout_codec::symbol_table;
use crate::common::threads::IntoParRayon;
use crate::common::{Sample, Timestamp};
use crate::fanout::FanoutContext;
use crate::labels::filters::SeriesSelector;
use crate::promql::EvalLabels;
use crate::promql::EvalSample;
use crate::promql::deadline::{Deadline, SERIES_WORK};
use crate::promql::engine::label_profile::{LabelProfile, LabelProfileBuilder};
use crate::promql::engine::promql_config;
use crate::promql::engine::query_reader::grid_fetch_bounds;
use crate::promql::engine::sample_budget::SampleBudget;
use crate::promql::engine::{
    decode_within_budget, exceeds_max_series, get_snapshot_range, instant_lookback_start_ms,
    validate_max_points, validate_max_series,
};
use crate::promql::generated::{
    InstantQueryResponse, InstantSample, RangeQueryResponse, RangeSample,
};
use crate::series::chunks::samples_to_chunk_lossless;
use crate::series::index::{
    BatchLimit, DEFAULT_BATCH_COPY_BYTES, DEFAULT_SERIES_BATCH_SIZE, GilSource, for_each_series,
    for_each_series_batch_then,
};
use crate::series::{RangeSnapshot, TimeSeries};
use orx_parallel::Par;
use orx_parallel::ParResult;
use std::ops::ControlFlow;
use valkey_module::ValkeyResult;

/// How many samples an instant read keeps between charges to its budget. Charging per
/// sample would cost three atomic updates per series under the module lock, one of them on
/// the process-wide count every query shares; between charges, [`ChargedSamples::push`]
/// still refuses a sample the read's own limit has no room for, from a count it keeps
/// itself rather than one more atomic load per sample.
const INSTANT_CHARGE_BATCH: usize = 512;

/// The samples an instant read keeps, charged to `budget` as they are kept: one per matched
/// series, so the series count is what an instant read holds — and what a pushed-down
/// aggregation holds on the shard however few groups it ships back.
struct ChargedSamples<'b, T> {
    budget: &'b SampleBudget,
    samples: Vec<T>,
    uncharged: usize,
    /// What the budget admitted at the last charge ([`SampleBudget::room`]): kept here so
    /// a push compares against it instead of loading the budget's count again. The read
    /// owns its budget, so nothing else shrinks it meanwhile.
    room: usize,
}

impl<'b, T> ChargedSamples<'b, T> {
    fn new(budget: &'b SampleBudget) -> Self {
        Self {
            budget,
            samples: Vec::new(),
            uncharged: 0,
            room: budget.room(),
        }
    }

    fn len(&self) -> usize {
        self.samples.len()
    }

    /// Keep `sample`, failing first if the read's limit has no room for it.
    fn push(&mut self, sample: T) -> ValkeyResult<()> {
        if self.uncharged >= self.room {
            // Out of room as of the last charge: the budget's own check gives the refusal
            // its words, and lets the sample through if the count was stale after all.
            self.budget
                .check_ahead(self.uncharged + 1)
                .map_err(budget_error)?;
            self.room = self.uncharged + self.budget.room();
        }
        self.samples.push(sample);
        self.uncharged += 1;
        if self.uncharged >= INSTANT_CHARGE_BATCH {
            self.charge()?;
        }
        Ok(())
    }

    fn charge(&mut self) -> ValkeyResult<()> {
        let samples = std::mem::take(&mut self.uncharged);
        self.budget.charge(samples).map_err(budget_error)?;
        self.room = self.budget.room();
        Ok(())
    }

    /// Every sample kept, all of them charged. The charge outlives this: it stays on `budget`.
    fn finish(mut self) -> ValkeyResult<Vec<T>> {
        self.charge()?;
        Ok(self.samples)
    }
}

/// A sample-budget refusal as a shard read reports it, in the words the coordinator relays.
fn budget_error(err: crate::promql::QueryError) -> valkey_module::ValkeyError {
    valkey_module::ValkeyError::String(err.to_string())
}

/// The newest sample in the lookback window `(timestamp - lookback_delta, timestamp]` of
/// every series `selector` matches on this node, each made into a `T` by `make`, read one
/// batch per lock hold. The head chunk's cached last sample usually answers without a decode,
/// so all of it runs under the lock.
///
/// The samples are charged to `budget` as they are kept (see [`shard_read`]); the caller
/// holds it for as long as it holds them. The read stops once it has one series past
/// `max_series`, which it then refuses: the shard's own working set is bounded whatever the
/// coordinator does with the samples. It also stops, between series, once `deadline` has
/// passed or this node's memory is past its limit.
#[allow(clippy::too_many_arguments)]
fn read_instant_samples<S, T>(
    src: &S,
    selector: SeriesSelector,
    timestamp: Timestamp,
    lookback_delta: u64,
    max_series: u64,
    budget: &SampleBudget,
    deadline: &Deadline,
    mut make: impl FnMut(&TimeSeries, Sample) -> T,
) -> ValkeyResult<Vec<T>>
where
    S: GilSource + ?Sized,
{
    let start_time = instant_lookback_start_ms(timestamp, lookback_delta as i64);

    let mut samples = ChargedSamples::new(budget);
    for_each_series(
        src,
        &[selector],
        None,
        DEFAULT_SERIES_BATCH_SIZE,
        |_, series, _| {
            deadline
                .checkpoint(SERIES_WORK)
                .map_err(super::shard_error)?;
            if let Some(sample) = series.last_sample_in_range(start_time, timestamp) {
                samples.push(make(&series, sample))?;
                // Refused below whatever the rest holds: stop opening series.
                if exceeds_max_series(samples.len(), max_series as usize) {
                    return Ok(ControlFlow::Break(()));
                }
            }
            Ok(ControlFlow::Continue(()))
        },
    )?;
    let samples = samples.finish()?;
    validate_max_series(samples.len(), max_series as usize)
        .map_err(valkey_module::ValkeyError::String)?;
    Ok(samples)
}

/// This shard's share of an instant-vector selector, in wire form: see
/// [`read_instant_samples`].
///
/// Labels go straight from storage into the response's symbol table by identity; no owned
/// label strings are built per series. Interning under each batch's lock is safe across them:
/// the builder keeps every entry it has keyed alive, so an address cannot be reused by
/// another label.
pub(super) fn handle_instant_query<S: GilSource + ?Sized>(
    src: &S,
    selector: SeriesSelector,
    timestamp: Timestamp,
    lookback_delta: u64,
    max_series: u64,
    _max_points_per_series: u64,
    read: &ShardRead,
) -> ValkeyResult<InstantQueryResponse> {
    let mut symbol_table = symbol_table::SymbolTableBuilder::default();
    let samples = read_instant_samples(
        src,
        selector,
        timestamp,
        lookback_delta,
        max_series,
        &read.budget,
        &read.deadline,
        |series, sample| {
            let (label_name_refs, label_value_refs) = symbol_table.intern(&series.labels);
            InstantSample {
                labels: Vec::new(),
                value: sample.value,
                timestamp: sample.timestamp,
                label_name_refs,
                label_value_refs,
            }
        },
    )?;
    Ok(InstantQueryResponse {
        samples,
        labels: Some(symbol_table.finish()),
    })
}

/// Local instant-vector evaluation for the aggregation push-down: the same read as
/// [`handle_instant_query`] ([`read_instant_samples`]), yielding evaluator-native samples so
/// the aggregation operators can be applied to them directly instead of round-tripping
/// through the wire types. Only the aggregated result crosses the wire, which is the point
/// of the push-down.
///
/// The coordinator only ever counts the aggregated result, so `budget` is the one place the
/// aggregation's input is counted: the caller holds it until it has aggregated them.
pub(super) fn local_instant_eval_samples<S: GilSource + ?Sized>(
    src: &S,
    selector: SeriesSelector,
    timestamp: Timestamp,
    lookback_delta: u64,
    max_series: u64,
    read: &ShardRead,
) -> ValkeyResult<Vec<EvalSample>> {
    read_instant_samples(
        src,
        selector,
        timestamp,
        lookback_delta,
        max_series,
        &read.budget,
        &read.deadline,
        |series, sample| EvalSample {
            // Shares the series' label set; nothing borrows the series.
            labels: EvalLabels::interned(&series.labels),
            value: sample.value,
            timestamp_ms: sample.timestamp,
            drop_name: false,
        },
    )
}

/// The labels of the series `selector` matches on this node: the local half
/// of [`super::LabelProfileFanoutCommand`], and what a single node answers
/// `QueryReader::label_profile` with. `None` past `max_series` matches, and
/// for the same reason the coordinator has: a selector that large is not
/// worth walking to narrow another.
///
/// No sample is read. Every matched key is opened, as a read of the same
/// selector would, until the cap is passed; the walk stops there. Refused
/// before it starts, and stopped between series, once `deadline` has passed or
/// this node's memory is past its limit ([`super::shard_deadline`]).
pub(in crate::promql) fn local_label_profile<S: GilSource + ?Sized>(
    src: &S,
    selector: SeriesSelector,
    max_series: usize,
    deadline: &Deadline,
) -> ValkeyResult<Option<LabelProfile>> {
    deadline.check().map_err(super::shard_error)?;
    let mut builder = LabelProfileBuilder::new();
    let mut matched = 0usize;
    let mut overflow = false;
    for_each_series(
        src,
        &[selector],
        None,
        DEFAULT_SERIES_BATCH_SIZE,
        |_, series, _| {
            deadline
                .checkpoint(SERIES_WORK)
                .map_err(super::shard_error)?;
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
/// A batch is at most 512 series and [`DEFAULT_BATCH_COPY_BYTES`] of chunks,
/// so a long window over dense series is copied in more, shorter holds rather
/// than one hold per 512 series however many chunks each has.
///
/// The copy also refuses the read early against `read`'s budget: the samples of the
/// chunks wholly inside the span ([`RangeSnapshot::min_samples`]) are known
/// without decoding, so a read whose batch alone would overrun what `decode`
/// has left fails before decoding any of it. `decode` still charges the exact
/// count; this only gets to the same answer sooner.
///
/// Only series with a chunk in the span are kept, with `labels` of each: the
/// evaluator's shared set, or the storage set itself for a response whose
/// symbol table interns by identity. Either shares the series' `Arc`, so no
/// string is copied and nothing borrows the series.
///
/// `decode` says whether to read on: a read that already knows its verdict (over the series
/// limit) breaks, and the batches after it are never copied. So does one whose deadline
/// (`read`'s) has passed, or whose node's memory is past its limit, checked before each
/// batch is copied.
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
    read: &ShardRead,
    labels: L,
    mut decode: impl FnMut(Vec<(T, RangeSnapshot)>) -> ValkeyResult<ControlFlow<()>>,
) -> ValkeyResult<()>
where
    S: GilSource + ?Sized,
    L: Fn(&TimeSeries) -> T,
{
    let limit = BatchLimit::weighed(
        DEFAULT_SERIES_BATCH_SIZE.get(),
        DEFAULT_BATCH_COPY_BYTES,
        |s: &TimeSeries| s.range_copy_bytes(start_time, end_time),
    );
    for_each_series_batch_then(
        src,
        &[selector],
        None,
        limit,
        |_, batch| {
            read.deadline.check().map_err(super::shard_error)?;
            let mut copied = Vec::with_capacity(batch.len());
            let mut pending = 0usize;
            for (s, _) in batch {
                let snapshot = s.snapshot_range(start_time, end_time);
                if snapshot.is_empty() {
                    continue;
                }
                pending = pending.saturating_add(snapshot.min_samples());
                read.budget
                    .check_ahead(pending)
                    .map_err(|err| valkey_module::ValkeyError::String(err.to_string()))?;
                copied.push((labels(s), snapshot));
            }
            Ok(copied)
        },
        |snapshots| {
            if snapshots.is_empty() {
                return Ok(ControlFlow::Continue(()));
            }
            decode(snapshots)
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
/// bounds the points it accepts back. A violation is reported at the end of
/// the read, after `sink` has seen the batches before it: once every batch is
/// in, or, for `max_series`, at the batch that takes the count over it (see
/// [`WindowBounds`]).
///
/// The samples are charged to `read`'s budget as they are decoded, and its deadline is
/// checked between batches. `sink` keeps what it makes of them past this call, so the
/// caller holds the budget until it has built its response.
#[allow(clippy::too_many_arguments)]
pub(super) fn local_grid_windows<S: GilSource + ?Sized>(
    src: &S,
    selector: SeriesSelector,
    window_ends: &[Timestamp],
    backward_ms: i64,
    max_series: u64,
    max_points_per_series: u64,
    read: &ShardRead,
    mut sink: impl FnMut(Vec<crate::promql::model::RangeSample<EvalLabels>>) -> ValkeyResult<()>,
) -> ValkeyResult<()> {
    let Some((start_time, end_time)) = grid_fetch_bounds(window_ends, backward_ms) else {
        return Ok(());
    };

    let budget = &read.budget;
    let mut bounds = WindowBounds::new(max_series, max_points_per_series);
    for_each_snapshot_batch(
        src,
        selector,
        start_time,
        end_time,
        read,
        |s| EvalLabels::interned(&s.labels),
        |snapshots| {
            let candidates = snapshots
                .into_par_rayon()
                .map(|(labels, snapshot)| {
                    if !decode_within_budget(budget, &snapshot).map_err(|err| err.to_string())? {
                        return Ok(None);
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
            if bounds.over_series() {
                // `finish` refuses the read whatever the rest holds.
                return Ok(ControlFlow::Break(()));
            }
            if !windows.is_empty() {
                sink(windows)?;
            }
            Ok(ControlFlow::Continue(()))
        },
    )?;

    bounds.finish().map_err(valkey_module::ValkeyError::String)
}

/// What a read this node performs on another node's behalf runs under, from one read of
/// this node's config. Every shard read takes one — range, grid, instant, and the
/// aggregation push-down's input, which the coordinator never sees — and holds it until
/// what the read produced has been consumed into the response.
pub(super) struct ShardRead {
    /// This node's `ts-promql-max-samples-per-query`, counted toward its
    /// `ts-promql-max-samples-in-flight`, since the read runs here. The charge counts every
    /// sample the read decodes until the budget drops, though a grid read
    /// ([`ShardGrid`](super::grid_fanout_command::ShardGrid)) holds only a batch at a time.
    pub(super) budget: SampleBudget,
    /// When the requester stops waiting, and this node's `ts-promql-max-memory-percent`
    /// ([`super::shard_deadline`]): checked between the read's batches and in the kernels
    /// that run over what it read, so a read stops when memory pressure rises mid-way, not
    /// only when it is already high as the read starts.
    pub(super) deadline: Deadline,
}

/// Start a read on another node's behalf ([`ShardRead`]). Refused while this node's memory
/// use is at or past its `ts-promql-max-memory-percent`, or once the requester has stopped
/// waiting: this is the read's admission check, done under the same config read.
pub(super) fn shard_read(ctx: &FanoutContext) -> ValkeyResult<ShardRead> {
    let config = promql_config();
    let deadline = super::shard_deadline(ctx, config.max_memory_percent);
    deadline.check().map_err(super::shard_error)?;
    Ok(ShardRead {
        budget: SampleBudget::counted(config.max_samples_per_query, config.max_samples_in_flight),
        deadline,
    })
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
/// That count only grows, so the read stops at the batch that takes it over
/// the limit ([`Self::over_series`]). A window over the point limit is
/// remembered rather than reported, so a read over both limits still fails on
/// `max_series`, as it did when the windows were bounded all at once.
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

    /// Whether the windows admitted so far already exceed `max_series`: the
    /// verdict [`Self::finish`] reaches however many more batches come.
    fn over_series(&self) -> bool {
        exceeds_max_series(self.returned, self.max_series as usize)
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
    read: &ShardRead,
) -> ValkeyResult<RangeQueryResponse> {
    let max_points = points_limit(max_points_per_series);
    // This shard's own `ts-promql-max-samples-per-query`: the request does not
    // carry the coordinator's budget, and one node's share of a query should
    // not exceed what that node would allow a query of its own.
    let budget = &read.budget;
    let mut ranges = Vec::new();
    // The storage label set itself, not an evaluator copy: the symbol table
    // below interns by identity.
    for_each_snapshot_batch(
        src,
        selector,
        start_time,
        end_time,
        read,
        |s| s.labels.clone(),
        |snapshots| {
            let batch = snapshots
                .into_par_rayon()
                .map(|(labels, snapshot)| {
                    if !decode_within_budget(budget, &snapshot).map_err(|err| err.to_string())? {
                        return Ok(None);
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
            // Refused below whatever the rest holds: copy no more batches.
            Ok(if exceeds_max_series(ranges.len(), max_series as usize) {
                ControlFlow::Break(())
            } else {
                ControlFlow::Continue(())
            })
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

    /// An instant read stops at the sample its limit has no room for, before keeping it,
    /// though it charges only every `INSTANT_CHARGE_BATCH` samples.
    #[test]
    fn an_instant_read_is_refused_at_its_sample_limit() {
        let budget = SampleBudget::new(3);
        let mut samples = ChargedSamples::new(&budget);
        for i in 0..3 {
            samples.push(i).unwrap();
        }
        let err = samples.push(3).unwrap_err().to_string();
        assert!(err.contains("ts-promql-max-samples-per-query"), "{err}");
        assert_eq!(samples.len(), 3, "the refused sample is not kept");
        assert_eq!(samples.finish().unwrap(), vec![0, 1, 2]);
        assert_eq!(budget.loaded(), 3);
    }

    /// The room a push compares against is refreshed at every batch charge, so the refusal
    /// lands on the first sample past the limit however many batches came before it.
    #[test]
    fn an_instant_read_is_refused_past_its_limit_after_a_batch_charge() {
        let limit = INSTANT_CHARGE_BATCH + 2;
        let budget = SampleBudget::new(limit);
        let mut samples = ChargedSamples::new(&budget);
        for i in 0..limit {
            samples.push(i).unwrap();
        }
        assert_eq!(
            budget.loaded(),
            INSTANT_CHARGE_BATCH,
            "one batch charged so far"
        );
        let err = samples.push(limit).unwrap_err().to_string();
        assert!(err.contains("ts-promql-max-samples-per-query"), "{err}");
        assert_eq!(samples.finish().unwrap().len(), limit);
        assert_eq!(budget.loaded(), limit);
    }

    /// The finding this pins: an instant read on another node's behalf — a plain selector,
    /// or a pushed-down aggregation's input — counts toward this node's
    /// `ts-promql-max-samples-in-flight` for as long as its budget is held, however few
    /// samples it goes on to ship, and gives them back when it is dropped.
    #[test]
    fn an_instant_read_counts_toward_samples_in_flight_until_its_budget_drops() {
        use crate::promql::engine::admission::Admission;
        // Its own count, so the totals are exact and no other test's queries see them.
        static COUNTS: Admission = Admission::new();
        let read = INSTANT_CHARGE_BATCH * 3 + 7;

        let budget = SampleBudget::counted_in(&COUNTS, 0, 1_000_000_000);
        let mut samples = ChargedSamples::new(&budget);
        for i in 0..read {
            samples.push(i).unwrap();
        }
        // An aggregation that reduces the read to one value still holds the read.
        let reduced: usize = samples.finish().unwrap().into_iter().sum();
        assert!(reduced > 0);
        assert_eq!(COUNTS.samples_in_flight(), read, "charged while held");
        drop(budget);
        assert_eq!(COUNTS.samples_in_flight(), 0, "given back when dropped");

        // A node whose in-flight limit is already taken refuses the read.
        let full = SampleBudget::counted_in(&COUNTS, 0, 1);
        let mut samples = ChargedSamples::new(&full);
        let refused = (0..INSTANT_CHARGE_BATCH).try_for_each(|i| samples.push(i));
        let err = refused
            .and_then(|()| samples.finish().map(drop))
            .unwrap_err();
        assert!(
            err.to_string().contains("ts-promql-max-samples-in-flight"),
            "{err}"
        );
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
    /// The grid read stops at the batch that takes it over the limit: from there
    /// `finish` refuses it whatever later batches would hold.
    #[test]
    fn over_series_holds_once_the_returned_count_passes_the_limit() {
        let mut bounds = WindowBounds::new(3, 0);
        bounds.admit(vec![filled("a", 1), empty(), filled("b", 1)]);
        assert!(!bounds.over_series(), "two returned, one matched but empty");
        bounds.admit(vec![filled("c", 1)]);
        assert!(!bounds.over_series(), "exactly the limit is allowed");
        bounds.admit(vec![filled("d", 1)]);
        assert!(bounds.over_series());
        let err = bounds.finish().unwrap_err();
        assert!(err.ends_with(": 4 > 3"), "{err}");

        let mut unlimited = WindowBounds::new(0, 0);
        unlimited.admit((0..10).map(|i| filled(&i.to_string(), 1)).collect());
        assert!(!unlimited.over_series());
    }

    #[test]
    fn exceeds_max_series_agrees_with_validate_max_series() {
        for (count, max) in [(0, 0), (5, 0), (3, 3), (4, 3), (1, 1), (2, 1)] {
            assert_eq!(
                exceeds_max_series(count, max),
                validate_max_series(count, max).is_err(),
                "{count} vs {max}"
            );
        }
    }

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
