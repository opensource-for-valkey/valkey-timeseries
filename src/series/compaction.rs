use crate::aggregators::{AggregationHandler, Aggregator, calc_bucket_start};
use crate::common::logging::log_warning;
use crate::common::rdb::{
    RdbSerializable, rdb_load_bool, rdb_load_timestamp, rdb_save_bool, rdb_save_timestamp,
};
use crate::common::{Sample, Timestamp};
use crate::error::{TsdbError, TsdbResult};
use crate::error_consts;
use crate::series::index::{get_series_by_id, with_timeseries_postings};
use crate::series::{DuplicatePolicy, SampleAddResult, SeriesGuardMut, SeriesRef, TimeSeries};
use get_size2::GetSize;
use orx_parallel::{ParIter, ParallelizableCollectionMut};
use smallvec::SmallVec;
use std::cmp::Ordering;
use topo_sort::{SortResults, TopoSort};
use valkey_module::{Context, NotifyEvent, ValkeyError, ValkeyResult, raw};

const PARALLEL_THRESHOLD: usize = 2;
const TEMP_VEC_LEN: usize = 6;

/// (dest_id, written samples, previous last sample) queued for cascading compaction.
type PendingCompactionWrite = (SeriesRef, Vec<Sample>, Option<Timestamp>);

#[derive(Debug, Clone, Hash, PartialEq)]
pub struct CompactionRule {
    pub dest_id: SeriesRef,
    pub aggregator: Aggregator,
    pub bucket_duration: u64,
    pub align_timestamp: Timestamp,
    pub bucket_start: Option<Timestamp>,
    pub has_samples: bool,
}

impl GetSize for CompactionRule {
    fn get_size(&self) -> usize {
        size_of::<SeriesRef>() // dest_id
            + self.aggregator.get_size()
            + size_of::<u64>() // bucket_duration
            + size_of::<Timestamp>() // align_timestamp
            + size_of::<Option<Timestamp>>() // bucket_start
            + size_of::<bool>() // has_samples
    }
}

impl CompactionRule {
    pub(crate) fn calc_bucket_start(&self, ts: Timestamp) -> Timestamp {
        calc_bucket_start(ts, self.align_timestamp, self.bucket_duration)
    }

    pub(super) fn get_bucket_range(&self, ts: Timestamp) -> (Timestamp, Timestamp) {
        let start = self.calc_bucket_start(ts);
        let end = start.saturating_add_unsigned(self.bucket_duration);
        (start, end)
    }

    pub(super) fn reset(&mut self) {
        AggregationHandler::reset(&mut self.aggregator);
        self.bucket_start = None;
        self.has_samples = false;
    }

    pub(super) fn update(&mut self, ts: Timestamp, value: f64) {
        if AggregationHandler::update(&mut self.aggregator, ts, value) {
            self.has_samples = true;
        }
    }
}

impl RdbSerializable for CompactionRule {
    fn rdb_save(&self, rdb: *mut raw::RedisModuleIO) {
        raw::save_unsigned(rdb, self.dest_id);
        self.aggregator.rdb_save(rdb);
        raw::save_unsigned(rdb, self.bucket_duration);
        rdb_save_timestamp(rdb, self.align_timestamp);
        rdb_save_timestamp(rdb, self.bucket_start.unwrap_or(-1));
        rdb_save_bool(rdb, self.has_samples);
    }

    fn rdb_load(rdb: *mut raw::RedisModuleIO) -> ValkeyResult<Self> {
        let dest_id = raw::load_unsigned(rdb)? as SeriesRef;
        let aggregator = Aggregator::rdb_load(rdb)?;
        let bucket_duration = raw::load_unsigned(rdb)?;
        let align_timestamp = rdb_load_timestamp(rdb)?;
        let start_ts = rdb_load_timestamp(rdb)?;
        let has_samples = rdb_load_bool(rdb)?;
        let bucket_start = if start_ts == -1 { None } else { Some(start_ts) };

        Ok(CompactionRule {
            dest_id,
            aggregator,
            bucket_duration,
            align_timestamp,
            bucket_start,
            has_samples,
        })
    }
}

struct CompactionContext<'a> {
    parent: &'a TimeSeries,
    dest: &'a mut TimeSeries,
    rule: &'a mut CompactionRule,
    /// Samples actually committed to `dest` during this operation (the stored, rounded
    /// values), in the order they were written. Used both to decide whether to notify on
    /// `dest`, and to cascade into `dest`'s own compaction rules — see
    /// [`process_series_with_compaction`].
    written: Vec<Sample>,
}

impl<'a> CompactionContext<'a> {
    fn new(parent: &'a TimeSeries, dest: &'a mut TimeSeries, rule: &'a mut CompactionRule) -> Self {
        Self {
            parent,
            dest,
            rule,
            written: Vec::new(),
        }
    }

    fn update(&mut self, ts: Timestamp, value: f64) {
        self.rule.update(ts, value);
    }

    fn start_bucket(&mut self, bucket_start: Timestamp, sample: Sample) {
        self.rule.bucket_start = Some(bucket_start);
        // Start a new bucket with the new sample
        self.update(sample.timestamp, sample.value);
    }

    fn has_samples(&self) -> bool {
        self.rule.has_samples
    }

    /// Remove a range from the destination, keeping the DIV-0023 marker live.
    ///
    /// Every destination removal goes through here, the counterpart to `add_dest_bucket`
    /// for writes. If the removal took the bucket the marker names, the marker falls back
    /// to whatever the destination still holds — its current last sample, or nothing when
    /// the removal emptied it.
    ///
    /// Both halves matter, and the difference only shows when a *later* write re-creates
    /// the removed timestamp. Leaving a dead marker set lets that write resurrect it and
    /// drag the reported last-sample backwards. Clearing it unconditionally is equally
    /// wrong: the next write would claim it, even though a surviving newer bucket is what
    /// RedisTimeSeries goes on reporting.
    fn remove_dest_range(&mut self, start: Timestamp, end: Timestamp) -> TsdbResult<usize> {
        let removed = self.dest.remove_range(start, end)?;
        if let Some(marker) = self.dest.last_forward_close
            && matches!(self.dest.get_sample(marker.timestamp), Ok(None))
        {
            self.dest.last_forward_close = self.dest.last_sample;
        }
        Ok(removed)
    }
}

/// Single entry point for all compaction-related mutations.
#[derive(Debug, Clone, Copy)]
pub enum CompactionOp<'a> {
    /// Handle compaction for a genuinely new sample (timestamp > last sample timestamp)
    AddNew(Sample),
    /// Handle compaction for an upsert (timestamp <= last sample timestamp)
    Upsert(Sample),
    /// Propagate a batch of samples that were already merged into the source series.
    ///
    /// `samples` must be sorted by timestamp ascending with unique timestamps and hold the
    /// stored (rounded) values. `prev_last` is the source series' last timestamp from before
    /// the batch was merged: samples above it are guaranteed fresh appends, samples at or
    /// below it may have replaced existing values and are treated as upserts.
    ///
    /// `input_order` holds the accepted timestamps in the order the caller supplied them
    /// (MADD argument order), which sorting `samples` discards. Only the DIV-0023 forward-close
    /// marker needs it — see [`last_forward_close_in_input_order`].
    AddBatch {
        samples: &'a [Sample],
        prev_last: Option<Timestamp>,
        input_order: &'a [Timestamp],
    },
    /// Remove a range from source and reflect it into destinations (and ongoing aggregation state)
    RemoveRange { start: Timestamp, end: Timestamp },
}

pub fn apply_compaction(
    ctx: &Context,
    series: &mut TimeSeries,
    op: CompactionOp,
) -> TsdbResult<()> {
    if series.rules.is_empty() {
        return Ok(());
    }
    process_series_with_compaction(ctx, series, op)
}

/// The "no filtering" filter for bucket recalculation: every stored sample counts.
///
/// Timestamp 0 is a valid sample timestamp and must be included — a previous
/// version excluded it (`ts != 0`), which made any recalculation of a bucket
/// containing a ts=0 sample drop that sample: an out-of-order write into a
/// finalized first bucket produced the wrong downstream value, and overwriting a
/// lone ts=0 sample deleted the downstream bucket entirely.
fn null_ts_filter(_ts: Timestamp) -> bool {
    true
}

fn apply_op(ctx: &mut CompactionContext<'_>, op: CompactionOp) -> TsdbResult<()> {
    match op {
        CompactionOp::AddNew(sample) => handle_sample_compaction(ctx, sample),
        CompactionOp::Upsert(sample) => handle_compaction_upsert(ctx, sample),
        CompactionOp::AddBatch {
            samples,
            prev_last,
            input_order,
        } => handle_batch_compaction(ctx, samples, prev_last, input_order),
        CompactionOp::RemoveRange { start, end } => {
            handle_compaction_range_removal(ctx, start, end)
        }
    }
}

/// Propagate a batch of samples (already merged into the source) through one rule.
///
/// Appends stream through the open-bucket aggregator in O(1) each, exactly like sequential
/// single-sample adds. Back-fills may have replaced existing values, so their buckets are
/// deduplicated and each affected bucket is recalculated from the source once.
///
/// The two are told apart by replaying the batch in the caller's order against a running
/// maximum seeded with `prev_last`, because that is how RedisTimeSeries applies MADD items:
/// one at a time, each item above the running maximum appending and each item at or below it
/// back-filling. Comparing against `prev_last` alone is not equivalent — on a series that the
/// batch itself populates (`prev_last == None`) every item outranks it, so
/// `TS.MADD k 0 v k 2000 v k 1000 v` would stream as three appends and never recalculate the
/// bucket `1000` back-fills. That distinction is load-bearing under retention: only the
/// recalculation reads the source back through the retention-clamped iterator, which is what
/// drops a sample the pending trim is about to evict.
///
/// Appends are processed first: a bucket recalculation reads the source, which already
/// contains this batch's appends, so recalculating first and then streaming the appends would
/// count them twice. If an append closes a bucket that a pending upsert also touched, the
/// destination value is first written from the (stale) aggregator and then overwritten by the
/// recalculation (destination adds use KeepLast), keeping the batch self-correcting.
fn handle_batch_compaction(
    ctx: &mut CompactionContext,
    samples: &[Sample],
    prev_last: Option<Timestamp>,
    input_order: &[Timestamp],
) -> TsdbResult<()> {
    debug_assert!(
        samples.is_sorted_by_key(|s| s.timestamp),
        "batch compaction requires samples sorted by timestamp"
    );

    // Captured before the streaming below mutates them: the DIV-0023 marker has to be derived
    // from the batch's *input* order, which the sorted `samples` no longer carries.
    let marker_before = ctx.dest.last_forward_close;
    let cache_trace =
        trace_dest_cache_in_input_order(ctx.rule, ctx.rule.bucket_start, prev_last, input_order);

    // Replay the caller's order to find the back-fills — items at or below the running maximum
    // by the time they are applied — and, with each, the series' last timestamp at that moment.
    // That timestamp fixes the retention floor the recalculation must read the source through:
    // applied sequentially, the trim would have run with exactly that window in force.
    let retention = ctx.parent.retention;
    let mut advanced: SmallVec<[Timestamp; TEMP_VEC_LEN]> = SmallVec::new();
    let mut backfilled: SmallVec<[(Timestamp, Timestamp); TEMP_VEC_LEN]> = SmallVec::new();
    let mut high_water = prev_last;
    for &ts in input_order {
        match high_water {
            // A repeat of a timestamp this same batch already appended is a duplicate-policy
            // fold, not a back-fill. The append is what advances the rule through its buckets,
            // and `samples` carries one entry per timestamp holding the folded value, so
            // classifying the repeat as a back-fill would take that entry out of the append
            // stream entirely: the bucket it should have closed stays open and unpublished,
            // while the bucket it opened gets written to the destination as though it were
            // historical. `TS.MADD k 0 0 k 500 0 k 500 0` published bucket 500 and lost
            // bucket 0 that way.
            Some(max) if ts <= max => {
                if !advanced.contains(&ts) {
                    backfilled.push((ts, max));
                }
            }
            _ => {
                high_water = Some(ts);
                advanced.push(ts);
            }
        }
    }
    // Later entries win: a bucket back-filled more than once settles at the floor of the last
    // recalculation, and the replay above is already in input order.
    backfilled.sort_by_key(|(ts, _)| *ts);
    backfilled.dedup_by_key(|(ts, _)| *ts);

    let floor_at = |last_ts: Timestamp| {
        if retention.is_zero() {
            0
        } else {
            last_ts.saturating_sub(retention.as_millis() as i64).max(0)
        }
    };
    let backfill_floor = |ts: Timestamp| match backfilled.binary_search_by_key(&ts, |(bts, _)| *bts)
    {
        Ok(idx) => Some(floor_at(backfilled[idx].1)),
        Err(_) => None,
    };

    // Both halves keep `samples`' ascending order: appends stream into the open bucket, and the
    // recalculation loop below relies on same-bucket entries being adjacent. Each upsert carries
    // the retention floor its recalculation must use.
    let mut appends: SmallVec<[Sample; TEMP_VEC_LEN]> = SmallVec::new();
    let mut upserts: SmallVec<[(Sample, Timestamp); TEMP_VEC_LEN]> = SmallVec::new();
    for sample in samples {
        match backfill_floor(sample.timestamp) {
            Some(min_ts) => upserts.push((*sample, min_ts)),
            None => appends.push(*sample),
        }
    }

    for sample in &appends {
        handle_sample_compaction(ctx, *sample)?;
    }

    // One recalculation per affected bucket (samples are sorted, so same-bucket entries are
    // adjacent).
    //
    // A bucket only reaches the destination when it closes, so `recalculate_bucket` (which
    // writes) is for buckets the rule has already moved past. When the rule has no open bucket
    // at all this upsert is opening it, exactly as the single-sample path does for its own
    // "no current bucket" case — publishing here would expose a bucket still accepting samples.
    // A batch reaches that state whenever a timestamp appears twice in the caller's order: the
    // repeat reads as a back-fill, `samples` carries the timestamp once, so it is classified as
    // an upsert and never streams through the append path that would have opened the bucket.
    let mut prev_bucket: Option<Timestamp> = None;
    for (sample, min_ts) in &upserts {
        let bucket_start = ctx.rule.calc_bucket_start(sample.timestamp);
        if prev_bucket == Some(bucket_start) {
            continue;
        }
        prev_bucket = Some(bucket_start);

        if ctx.rule.bucket_start.is_none() {
            let bucket_end = bucket_start.saturating_add_unsigned(ctx.rule.bucket_duration);
            recalculate_current_bucket(ctx, bucket_start, bucket_end, *min_ts)?;
            continue;
        }

        let bucket_end = bucket_start.saturating_add_unsigned(ctx.rule.bucket_duration);
        if ctx.rule.bucket_start == Some(bucket_start) {
            recalculate_current_bucket(ctx, bucket_start, bucket_end, *min_ts)?;
        } else {
            recalculate_bucket(ctx, bucket_start, bucket_end, *min_ts, null_ts_filter)?;
        }
    }

    // Overwrite whatever the sorted append stream recorded: sorting turns an out-of-order
    // MADD into a run of forward closes, which would advance the marker past a bucket that
    // RedisTimeSeries only ever back-filled. A forward close always wins; failing that an
    // already-set marker stands, and only a still-unset one takes the batch's first write.
    let marker_bucket = match (cache_trace.last_forward_close, marker_before) {
        (Some(ts), _) => Some(ts),
        (None, Some(prev)) => Some(prev.timestamp),
        (None, None) => cache_trace.first_write,
    };
    ctx.dest.last_forward_close =
        marker_bucket.and_then(|ts| ctx.dest.get_sample(ts).ok().flatten());

    Ok(())
}

/// What replaying a batch in the caller's input order does to the destination's cached
/// last-sample. Both fields are bucket starts.
struct DestCacheTrace {
    /// Bucket closed by the last forward advance, if the batch made one.
    last_forward_close: Option<Timestamp>,
    /// Bucket of the first downstream write in that order, forward or back-fill.
    first_write: Option<Timestamp>,
}

/// Replay a batch in input order to work out the destination's cached last-sample (DIV-0023).
///
/// RedisTimeSeries applies MADD items one at a time in argument order. An item above the
/// series' running maximum moves forward and closes the open bucket; an item at or below it
/// back-fills, materializing a downstream bucket *without* refreshing the cache. We merge the
/// batch as one sorted run, which is right for the stored data but erases that distinction:
/// `TS.MADD k 0 v k 1000 v k 500 v` streams as 0,500,1000 and closes two buckets forward,
/// where RTS closes only `[0,500)` and back-fills `[500,1000)`.
///
/// The cache also has to be *initialized*: with the max arriving first
/// (`TS.MADD k 1000 v k 0 v k 500 v`) nothing closes forward at all, yet RTS still reports a
/// value — the first bucket it materialized. So an empty cache takes the first write, and from
/// then on only forward closes move it. Both halves were derived black-box and checked against
/// the reference over every permutation of three and four timestamps.
///
/// Replaying against the rule's bucket boundaries recovers this without disturbing the
/// aggregation path, which stays sorted and keeps producing identical stored data.
fn trace_dest_cache_in_input_order(
    rule: &CompactionRule,
    pre_batch_bucket_start: Option<Timestamp>,
    prev_last: Option<Timestamp>,
    input_order: &[Timestamp],
) -> DestCacheTrace {
    let mut current = pre_batch_bucket_start;
    let mut high_water = prev_last;
    let mut trace = DestCacheTrace {
        last_forward_close: None,
        first_write: None,
    };

    for &ts in input_order {
        let bucket = rule.calc_bucket_start(ts);

        if high_water.is_some_and(|hw| ts <= hw) {
            // Back-fill. It publishes unless it lands in the still-open bucket, which is
            // never written downstream.
            if current.is_some_and(|open| bucket < open) {
                trace.first_write.get_or_insert(bucket);
            }
            continue;
        }
        high_water = Some(ts);

        match current {
            Some(open) if bucket > open => {
                trace.first_write.get_or_insert(open);
                trace.last_forward_close = Some(open);
                current = Some(bucket);
            }
            Some(_) => {}
            None => current = Some(bucket),
        }
    }

    trace
}

/// Handle compaction for a genuinely new sample (timestamp > last sample timestamp)
fn handle_sample_compaction(ctx: &mut CompactionContext, sample: Sample) -> TsdbResult<()> {
    let ts = sample.timestamp;
    let sample_bucket_start = ctx.rule.calc_bucket_start(ts);

    let Some(current_bucket_start) = ctx.rule.bucket_start else {
        // First sample for this rule - initialize the aggregation
        ctx.start_bucket(sample_bucket_start, sample);
        return Ok(());
    };

    match sample_bucket_start.cmp(&current_bucket_start) {
        Ordering::Equal => {
            // Sample belongs to the current aggregation bucket
            ctx.update(sample.timestamp, sample.value);
        }
        Ordering::Greater => {
            // Sample starts a new bucket - finalize the current bucket first
            finalize_current_bucket(ctx, sample, sample_bucket_start)?;
        }
        Ordering::Less => {
            let bucket_end = sample_bucket_start.saturating_add_unsigned(ctx.rule.bucket_duration);
            // Sample is in an older bucket (shouldn't happen for new samples, but handle gracefully)
            recalculate_bucket(
                ctx,
                sample_bucket_start,
                bucket_end,
                ctx.parent.get_min_timestamp(),
                null_ts_filter,
            )?;
        }
    }

    Ok(())
}

/// Finalize the current aggregation bucket and start a new one
fn finalize_current_bucket(
    ctx: &mut CompactionContext<'_>,
    new_sample: Sample,
    new_bucket_start: Timestamp,
) -> TsdbResult<()> {
    if ctx.has_samples() {
        // Finalize the current bucket
        let aggregated_value = AggregationHandler::finalize(&mut ctx.rule.aggregator);
        let current_bucket_start = ctx.rule.bucket_start.expect(
            "finalize_current_bucket should be called when current bucket start is already set",
        );

        if let Some(published) = add_dest_bucket(ctx, current_bucket_start, aggregated_value)? {
            // DIV-0023: this is the only write that advances the destination
            // last-sample `ts-compatibility-mode strict` reports from TS.GET/TS.MGET.
            // Back-filling an older bucket (`recalculate_bucket`) materializes a
            // downstream sample but deliberately leaves this untouched, mirroring
            // RedisTimeSeries's cached last-sample.
            ctx.dest.last_forward_close = Some(published);
        }
    }
    ctx.rule.reset();

    // Start a new bucket with the new sample
    ctx.start_bucket(new_bucket_start, new_sample);

    Ok(())
}

/// Handle upsert compaction for a destination series
/// This is called when a sample is being inserted/updated with a timestamp that's <= the last sample timestamp
fn handle_compaction_upsert(ctx: &mut CompactionContext, sample: Sample) -> TsdbResult<()> {
    let ts = sample.timestamp;
    let bucket_start = ctx.rule.calc_bucket_start(ts);

    // Check if this affects the current ongoing aggregation bucket
    let Some(current_bucket_start) = ctx.rule.bucket_start else {
        // No current bucket, this is the first sample for this rule
        ctx.start_bucket(bucket_start, sample);
        return Ok(());
    };

    let duration = ctx.rule.bucket_duration;
    let bucket_end = current_bucket_start.saturating_add_unsigned(duration);

    if bucket_start == current_bucket_start {
        // This sample belongs to the current aggregation bucket
        // We need to recalculate the entire bucket since we don't know what changed
        recalculate_current_bucket(
            ctx,
            current_bucket_start,
            bucket_end,
            ctx.parent.get_min_timestamp(),
        )?;
        return Ok(());
    }

    // This is a historical upsert — recalculate the affected (closed) bucket.
    // The recompute range must be that bucket's own span; `bucket_end` above
    // belongs to the *current* open bucket, and using it here would fold every
    // sample between the historical bucket and the open one into the recompute.
    let historical_bucket_end = bucket_start.saturating_add_unsigned(duration);
    let min_ts = ctx.parent.get_min_timestamp();
    recalculate_bucket(
        ctx,
        bucket_start,
        historical_bucket_end,
        min_ts,
        null_ts_filter,
    )
}

/// Recalculate the current ongoing aggregation bucket
fn recalculate_current_bucket(
    ctx: &mut CompactionContext,
    bucket_start: Timestamp,
    bucket_end: Timestamp,
    min_ts: Timestamp,
) -> TsdbResult<()> {
    // Reset the aggregator and recalculate from all samples in the bucket
    let has_samples = calculate_range(
        ctx.parent,
        &mut ctx.rule.aggregator,
        bucket_start,
        bucket_end - 1,
        min_ts,
        null_ts_filter,
    );

    ctx.rule.has_samples = has_samples;

    // reset would have cleared the bucket_start, so we need to set it again
    ctx.rule.bucket_start = Some(bucket_start);

    Ok(())
}

/// Recalculate a historical bucket and update the destination series
fn recalculate_bucket<F>(
    ctx: &mut CompactionContext,
    bucket_start: Timestamp,
    bucket_end: Timestamp,
    min_ts: Timestamp,
    filter: F,
) -> TsdbResult<()>
where
    F: Fn(Timestamp) -> bool,
{
    // Create a new aggregator for this bucket
    let mut bucket_aggregator = ctx.rule.aggregator.clone();
    AggregationHandler::reset(&mut bucket_aggregator);

    // Aggregate all samples in this bucket
    let has_samples = calculate_range(
        ctx.parent,
        &mut bucket_aggregator,
        bucket_start,
        bucket_end - 1,
        min_ts,
        &filter,
    );

    if has_samples {
        let aggregated_value = AggregationHandler::finalize(&mut bucket_aggregator);
        add_dest_bucket(ctx, bucket_start, aggregated_value)?;
    } else {
        // No samples in this bucket anymore, remove it from destination
        ctx.remove_dest_range(bucket_start, bucket_end - 1)?;
    }

    Ok(())
}

/// When a range of samples is removed, we need to remove samples in the corresponding
/// rule destination series that overlap with the range.
///
/// We need to handle the following scenarios:
/// - `Single Bucket Partial Removal`: When the removal range affects only part of a single aggregation bucket,
///   it recalculates the aggregation for the remaining samples.
/// - `Multiple Bucket Removal`: When the removal spans multiple buckets, we need to handle each bucket appropriately:
///   completely removing middle buckets and recalculating partial buckets at the boundaries.
/// - `Complete Bucket Removal`: When entire buckets are removed, remove the corresponding aggregated
///   samples from the destination series.
/// - `Current Aggregation State`: If there's an ongoing aggregation (indicated by `hi_ts`), adjust the
///   current aggregation state to account for the removed samples.
/// - `Error Handling`: Properly handle cases where destination series are missing or inaccessible.
///
fn handle_compaction_range_removal(
    ctx: &mut CompactionContext,
    start: Timestamp,
    end: Timestamp,
) -> TsdbResult<()> {
    // Update destination series buckets that overlap with [start, end].
    //
    // Only the first and last bucket can be partially covered; every bucket strictly between
    // them is fully covered by definition (the removal spans them end to end), so they are
    // dropped from the destination with a single range removal. Walking bucket by bucket
    // instead made the cost scale with the *number of buckets in the range* rather than with
    // the data actually stored: `TS.DEL key 0 <huge>` on a rule with a small bucketDuration
    // spins for billions of iterations and blocks the server (a 1e10-bucket range took ~13s
    // against RTS's 0.02s for the same two samples).
    let first_bucket_start = ctx.rule.calc_bucket_start(start);
    let last_bucket_start = ctx.rule.calc_bucket_start(end);

    remove_or_recalculate_bucket(ctx, first_bucket_start, start, end)?;

    if last_bucket_start != first_bucket_start {
        let middle_start = first_bucket_start.saturating_add_unsigned(ctx.rule.bucket_duration);
        if middle_start < last_bucket_start && !ctx.dest.is_empty() {
            // Buckets tile the range, so the fully covered middle is exactly
            // [middle_start, last_bucket_start).
            ctx.remove_dest_range(middle_start, last_bucket_start - 1)?;
        }
        remove_or_recalculate_bucket(ctx, last_bucket_start, start, end)?;
    }

    // Re-establish the open bucket from what the source still holds, retracting any
    // destination bucket the removal re-opened.
    resync_open_bucket_after_removal(ctx)?;

    Ok(())
}

/// Apply a removal of `[start, end]` to the single destination bucket starting at `bucket_start`:
/// drop it outright when the removal covers it entirely, otherwise recalculate it from the
/// surviving source samples.
fn remove_or_recalculate_bucket(
    ctx: &mut CompactionContext,
    bucket_start: Timestamp,
    start: Timestamp,
    end: Timestamp,
) -> TsdbResult<()> {
    // The rule's still-open bucket has no destination entry yet and must not gain one: a bucket
    // only reaches the destination when it closes. Recalculating it here would publish a bucket
    // that is still accepting samples (RTS reports nothing for it). Its aggregator is rebuilt by
    // `resync_open_bucket_after_removal`, which runs after this, so leave it untouched.
    if ctx.rule.bucket_start == Some(bucket_start) {
        return Ok(());
    }

    let bucket_end = bucket_start.saturating_add_unsigned(ctx.rule.bucket_duration);

    if start <= bucket_start && end >= bucket_end {
        if !ctx.dest.is_empty() {
            ctx.remove_dest_range(bucket_start, bucket_end - 1)?;
        }
    } else {
        // Recalculate this bucket excluding removed timestamps.
        // If destination has no flushed buckets yet, this still correctly maintains the aggregator state.
        let min_ts = ctx.parent.get_min_timestamp();
        recalculate_bucket(ctx, bucket_start, bucket_end, min_ts, |ts| {
            ts < start || ts > end
        })?;
    }

    Ok(())
}

/// Re-establish the rule's open bucket from the source's surviving samples, retracting any
/// destination bucket the removal re-opened.
///
/// The invariant this restores (confirmed by black-box probing of the reference): the
/// destination holds exactly the buckets *strictly older* than the bucket containing the
/// source's current last timestamp. That bucket is by definition still open, and an open
/// bucket has no destination entry.
///
/// A removal that deletes the source's trailing samples therefore moves the open bucket
/// *backwards*, and every destination sample at or after the new open bucket belongs to a
/// bucket that is open again — it must be retracted, not left stranded. The differential
/// fuzzer found this via `TS.DEL` deleting the very sample whose arrival had closed the
/// preceding bucket: we kept publishing the closed value where the reference reports nothing.
///
/// Recomputing the aggregation state from the source (rather than patching it against the
/// removed range) is also what makes the re-opened bucket publish the right value when a later
/// write closes it again: its surviving samples are simply re-aggregated.
fn resync_open_bucket_after_removal(ctx: &mut CompactionContext<'_>) -> TsdbResult<()> {
    let Some(last_sample) = ctx.parent.last_sample else {
        // Source is empty, so no bucket can ever have closed: the destination holds nothing
        // valid and there is no aggregation in progress.
        ctx.rule.reset();
        if !ctx.dest.is_empty() {
            ctx.remove_dest_range(Timestamp::MIN, Timestamp::MAX)?;
        }
        return Ok(());
    };

    let (open_start, open_end) = ctx.rule.get_bucket_range(last_sample.timestamp);

    // The re-opened bucket and anything after it are no longer closed.
    if !ctx.dest.is_empty() {
        ctx.remove_dest_range(open_start, Timestamp::MAX)?;
    }

    let mut aggregator = ctx.rule.aggregator.clone();
    AggregationHandler::reset(&mut aggregator);

    // `open_end - 1`: buckets are [start, end) but `range_iter` is inclusive on both bounds.
    let mut has_samples = false;
    for sample in ctx.parent.range_iter(open_start, open_end - 1) {
        if AggregationHandler::update(&mut aggregator, sample.timestamp, sample.value) {
            has_samples = true;
        }
    }

    ctx.rule.aggregator = aggregator;
    ctx.rule.has_samples = has_samples;
    ctx.rule.bucket_start = Some(open_start);

    Ok(())
}

/// Outcome of applying one compaction rule to its destination series.
struct RuleOutcome {
    dest_id: SeriesRef,
    /// `dest`'s last timestamp *before* this operation ran — the `prev_last` a cascaded
    /// `AddBatch` into `dest`'s own rules must use.
    dest_prev_last: Option<Timestamp>,
    /// Samples committed to `dest` by this operation, in commit order (not necessarily
    /// sorted or unique — see [`dedupe_written_samples`]).
    written: Vec<Sample>,
}

/// Sorts and deduplicates a rule's written samples for use as the next cascade level's batch.
///
/// A single [`CompactionOp::AddBatch`] application can write the same bucket twice: once from
/// the streaming append path, then again when a same-batch historical upsert recalculates that
/// bucket (see `handle_batch_compaction`). The recalculation is authoritative and always runs
/// after the streaming write, so a stable sort followed by keeping the last of each duplicate
/// timestamp preserves the correct (final) value while producing the sorted, unique-timestamp
/// sequence `AddBatch` requires downstream.
fn dedupe_written_samples(mut samples: Vec<Sample>) -> Vec<Sample> {
    if samples.len() < 2 {
        return samples;
    }
    samples.sort_by_key(|s| s.timestamp);
    let mut out: Vec<Sample> = Vec::with_capacity(samples.len());
    for sample in samples {
        if out
            .last()
            .is_some_and(|last: &Sample| last.timestamp == sample.timestamp)
        {
            *out.last_mut().unwrap() = sample;
        } else {
            out.push(sample);
        }
    }
    out
}

/// Iterates through compaction rules (possibly in parallel) and applies the specified operation,
/// cascading through chained compaction series (a destination that itself has rules).
///
/// Rule creation rejects circular chains (`check_new_rule_circular_dependency`), but the
/// traversal keeps a `visited` guard anyway so a corrupted topology cannot loop forever.
///
/// Cascading strategy differs by operation:
/// - `RemoveRange` reapplies the *same* absolute `[start, end]` range at every level. This is
///   correct regardless of chain depth because each level's recalculation reads directly from
///   its own immediate parent (already updated by the level above), never from a value carried
///   across levels.
/// - `AddNew`/`Upsert`/`AddBatch` instead feed each destination's own *written* samples — the
///   values actually committed to it — as an `AddBatch` into that destination's rules. This
///   makes every rule aggregate over its declared source series, matching a rule's own
///   definition (e.g. `mid -> dest AVG` averages `mid`'s stored samples, not the top-level raw
///   stream). Reusing the original op across levels would instead feed the *raw* top-level
///   samples straight into every descendant's aggregator, which only happens to agree with the
///   declared semantics for associative aggregators (SUM/MIN/MAX) and silently diverges for
///   AVG/COUNT/STD/VAR/RANGE.
fn process_series_with_compaction(
    ctx: &Context,
    series: &mut TimeSeries,
    op: CompactionOp,
) -> TsdbResult<()> {
    // `ts.add:dest` fires only when compaction appends new bucket data to a
    // downstream series (a bucket close). Reference-observed (§7.3): an
    // upsert into an already-closed bucket recomputes the destination
    // silently, and a TS.DEL propagated into destinations emits only the
    // source's `ts.del`.
    let notify_destinations = matches!(op, CompactionOp::AddNew(_) | CompactionOp::AddBatch { .. });
    let mut notified: SmallVec<[SeriesRef; TEMP_VEC_LEN]> = SmallVec::new();
    let mut visited: SmallVec<[SeriesRef; TEMP_VEC_LEN]> = SmallVec::new();
    visited.push(series.id);

    let destinations = get_compaction_series(ctx, series);
    if destinations.is_empty() {
        return Ok(());
    }

    let outcomes = apply_rules_on_destinations(series, destinations, op)?;

    match op {
        CompactionOp::RemoveRange { .. } => {
            let mut pending: SmallVec<[SeriesRef; TEMP_VEC_LEN]> =
                outcomes.iter().map(|o| o.dest_id).collect();
            for outcome in &outcomes {
                if !outcome.written.is_empty() {
                    notified.push(outcome.dest_id);
                }
            }

            while let Some(id) = pending.pop() {
                if visited.contains(&id) {
                    continue;
                }
                visited.push(id);

                let Some(mut child) = get_destination_series(ctx, id) else {
                    continue;
                };
                let child_destinations = get_compaction_series(ctx, &mut child);
                if child_destinations.is_empty() {
                    continue;
                }
                pending.extend(child_destinations.iter().map(|d| d.id));

                let child_outcomes =
                    apply_rules_on_destinations(&mut child, child_destinations, op)?;
                for outcome in child_outcomes {
                    if !outcome.written.is_empty() {
                        notified.push(outcome.dest_id);
                    }
                }
            }
        }
        _ => {
            let mut pending: SmallVec<[PendingCompactionWrite; TEMP_VEC_LEN]> = SmallVec::new();
            for outcome in outcomes {
                if outcome.written.is_empty() {
                    continue;
                }
                notified.push(outcome.dest_id);
                pending.push((
                    outcome.dest_id,
                    dedupe_written_samples(outcome.written),
                    outcome.dest_prev_last,
                ));
            }

            while let Some((id, samples, prev_last)) = pending.pop() {
                if visited.contains(&id) {
                    continue;
                }
                visited.push(id);

                let Some(mut child) = get_destination_series(ctx, id) else {
                    continue;
                };
                let child_destinations = get_compaction_series(ctx, &mut child);
                if child_destinations.is_empty() {
                    continue;
                }

                // A cascaded batch is this level's own committed buckets, which are published
                // in bucket order — so input order and sorted order coincide.
                let cascade_order: SmallVec<[Timestamp; TEMP_VEC_LEN]> =
                    samples.iter().map(|s| s.timestamp).collect();
                let batch_op = CompactionOp::AddBatch {
                    samples: &samples,
                    prev_last,
                    input_order: &cascade_order,
                };
                let child_outcomes =
                    apply_rules_on_destinations(&mut child, child_destinations, batch_op)?;

                for outcome in child_outcomes {
                    if outcome.written.is_empty() {
                        continue;
                    }
                    notified.push(outcome.dest_id);
                    pending.push((
                        outcome.dest_id,
                        dedupe_written_samples(outcome.written),
                        outcome.dest_prev_last,
                    ));
                }
            }
        }
    }

    if notify_destinations && !notified.is_empty() {
        notify_compaction(ctx, &notified);
    }

    Ok(())
}

fn apply_rules_on_destinations(
    series: &mut TimeSeries,
    destinations: SmallVec<[SeriesGuardMut; TEMP_VEC_LEN]>,
    op: CompactionOp,
) -> TsdbResult<Vec<RuleOutcome>> {
    let mut rules = std::mem::take(&mut series.rules);
    let result = apply_rules_internal(series, &mut rules, destinations, op);
    series.rules = rules;
    result
}

/// Internal function that handles execution of compaction rules.
fn apply_rules_internal(
    series: &TimeSeries,
    rules: &mut [CompactionRule],
    child_series: SmallVec<[SeriesGuardMut; TEMP_VEC_LEN]>,
    op: CompactionOp,
) -> TsdbResult<Vec<RuleOutcome>> {
    if rules.is_empty() {
        return Ok(Vec::new());
    }

    let len = rules.len();
    let mut destinations = rules.iter_mut().zip(child_series).collect::<Vec<_>>();
    let results: Vec<Result<RuleOutcome, TsdbError>> = destinations
        .par_mut()
        .num_threads(if len < PARALLEL_THRESHOLD { 1 } else { 0 }) // 0 is shorthand for Auto
        .map(|(rule, dest_guard)| {
            let dest_id = dest_guard.id;
            let dest_prev_last = dest_guard.last_sample.map(|s| s.timestamp);
            let mut cctx = CompactionContext::new(series, dest_guard, rule);
            apply_op(&mut cctx, op).map(|_| RuleOutcome {
                dest_id,
                dest_prev_last,
                written: cctx.written,
            })
        })
        .collect();

    let mut outcomes = Vec::with_capacity(results.len());
    let mut first_error: Option<TsdbError> = None;

    for r in results {
        match r {
            Ok(outcome) => outcomes.push(outcome),
            Err(error) => {
                if first_error.is_none() {
                    first_error = Some(error.clone());
                }
                let msg = format!("Failed to handle compaction rule for series: {error}");
                log_warning(msg);
            }
        }
    }

    if let Some(err) = first_error {
        Err(err)
    } else {
        Ok(outcomes)
    }
}

pub(super) fn get_destination_series(
    ctx: &'_ Context,
    dest_id: SeriesRef,
) -> Option<SeriesGuardMut<'_>> {
    if let Ok(Some(res)) = get_series_by_id(ctx, dest_id, false, None)
        && res.is_compaction()
    {
        return Some(res);
    };
    ctx.log_verbose("Destination series for compaction not found or not a compaction series");
    None
}

fn get_compaction_series<'a>(
    ctx: &'a Context,
    series: &mut TimeSeries,
) -> SmallVec<[SeriesGuardMut<'a>; TEMP_VEC_LEN]> {
    if series.rules.is_empty() {
        return SmallVec::new();
    }

    let mut missing: SmallVec<[_; TEMP_VEC_LEN]> = SmallVec::new();
    let mut destinations: SmallVec<[_; TEMP_VEC_LEN]> = SmallVec::new();

    for rule in series.rules.iter() {
        if let Some(dest_series) = get_destination_series(ctx, rule.dest_id) {
            // Destination series exists, add it to the list
            destinations.push(dest_series);
        } else {
            // Destination series doesn't exist, mark rule for removal
            missing.push(rule.dest_id);
        }
    }

    if !missing.is_empty() {
        series.rules.retain(|r| !missing.contains(&r.dest_id));
    }
    destinations
}

fn notify_compaction(ctx: &Context, ids: &[SeriesRef]) {
    with_timeseries_postings(ctx, |postings| {
        for &id in ids {
            let Some(key) = postings.get_key_by_id(id) else {
                ctx.log_warning("Compaction notification failed: series key not found");
                continue;
            };
            let key = ctx.create_string(key.as_ref());
            ctx.notify_keyspace_event(NotifyEvent::MODULE, "ts.add:dest", &key);
        }
    });
}

/// Write one aggregated bucket to the destination.
///
/// Returns the stored sample when the write actually landed, so a caller can tell a
/// real publish from one the destination ignored (see DIV-0023 in
/// `finalize_current_bucket`).
fn add_dest_bucket(
    ctx: &mut CompactionContext,
    ts: Timestamp,
    value: f64,
) -> TsdbResult<Option<Sample>> {
    let bucket_start = ctx.rule.calc_bucket_start(ts);
    // Add the sample to the destination series
    // todo: specify to ignore whatever adjustments
    match ctx
        .dest
        .add(bucket_start, value, Some(DuplicatePolicy::KeepLast))
    {
        SampleAddResult::Ok(sample) => {
            ctx.written.push(sample);
            // DIV-0023: an unset destination cache takes the *first* downstream write,
            // whether it came from a forward close or a back-fill — RedisTimeSeries reports
            // a value even for a destination only ever populated out of order. Later
            // back-fills leave it alone; only a forward close moves it (see
            // `finalize_current_bucket`). This is the single point every write passes
            // through, so it covers the sequential and batch paths alike; the batch path
            // then recomputes the final marker from input order.
            //
            // Removals keep the marker live themselves (`remove_dest_range`), so an unset
            // marker here really means "nothing published yet", not "the marker's bucket
            // went away".
            if ctx.dest.last_forward_close.is_none() {
                ctx.dest.last_forward_close = Some(sample);
            }
            Ok(Some(sample))
        }
        SampleAddResult::Ignored(_) => Ok(None), // duplicate sample, (ignored)
        SampleAddResult::TooOld => {
            // bucket start is too old, we cannot add it
            Ok(None)
        }
        x => {
            let base_msg = format!("TSDB: failed to add sample @{ts} to destination bucket: {x}",);
            log_warning(base_msg.as_str());
            Err(TsdbError::General(base_msg))
        }
    }
}

/// Aggregate the source samples in `[start, end]` that pass `filter`, ignoring anything below
/// `min_ts`.
///
/// The floor is a parameter rather than `series.get_min_timestamp()` because a batch is applied
/// as one sorted run while the retention trim runs after it: the window in force when a given
/// item would have been applied sequentially is not the series' current one. Callers outside the
/// batch path pass the current window, which is the same thing for them.
fn calculate_range<F>(
    series: &TimeSeries,
    aggregator: &mut Aggregator,
    start: Timestamp,
    end: Timestamp,
    min_ts: Timestamp,
    filter: F,
) -> bool
where
    F: Fn(Timestamp) -> bool,
{
    let mut has_samples = false;
    aggregator.reset();
    for sample in series
        .stored_range_iter(start.max(min_ts), end)
        .filter(|sample| filter(sample.timestamp))
    {
        if aggregator.update(sample.timestamp, sample.value) {
            has_samples = true;
        }
    }
    has_samples
}

impl TimeSeries {
    pub fn add_compaction_rule(&mut self, rule: CompactionRule) {
        let mut rule = rule;
        if let Aggregator::Rate(r) = &mut rule.aggregator {
            r.set_window_ms(rule.bucket_duration);
        }
        self.rules.push(rule);
    }

    pub fn remove_compaction_rule(&mut self, dest_id: SeriesRef) -> Option<CompactionRule> {
        let Some(index) = self.rules.iter().position(|rule| rule.dest_id == dest_id) else {
            // No rule found for this destination ID
            return None;
        };
        Some(self.rules.remove(index))
    }

    pub fn get_rule_by_dest_id(&self, dest_id: SeriesRef) -> Option<&CompactionRule> {
        self.rules.iter().find(|rule| rule.dest_id == dest_id)
    }

    pub fn remove_range_with_compaction(
        &mut self,
        ctx: &Context,
        start_ts: Timestamp,
        end_ts: Timestamp,
    ) -> TsdbResult<usize> {
        // Then remove the actual data from the source series
        let deleted_count = self.remove_range(start_ts, end_ts)?;

        if deleted_count > 0 && !self.rules.is_empty() {
            apply_compaction(
                ctx,
                self,
                CompactionOp::RemoveRange {
                    start: start_ts,
                    end: end_ts,
                },
            )?;
        }

        Ok(deleted_count)
    }

    pub fn run_compaction(&mut self, ctx: &Context, value: Sample) -> TsdbResult<()> {
        if self.rules.is_empty() {
            return Ok(());
        }
        apply_compaction(ctx, self, CompactionOp::AddNew(value))
    }

    pub fn upsert_compaction(&mut self, ctx: &Context, value: Sample) -> TsdbResult<()> {
        if self.rules.is_empty() {
            return Ok(());
        }
        apply_compaction(ctx, self, CompactionOp::Upsert(value))
    }

    /// Propagate a batch of samples that were already merged into this series.
    ///
    /// `samples` must be sorted by timestamp ascending (unique timestamps) and `prev_last`
    /// must be this series' last timestamp from before the batch was merged. See
    /// [`CompactionOp::AddBatch`].
    pub fn batch_compaction(
        &mut self,
        ctx: &Context,
        samples: &[Sample],
        prev_last: Option<Timestamp>,
        input_order: &[Timestamp],
    ) -> TsdbResult<()> {
        if self.rules.is_empty() || samples.is_empty() {
            return Ok(());
        }
        apply_compaction(
            ctx,
            self,
            CompactionOp::AddBatch {
                samples,
                prev_last,
                input_order,
            },
        )
    }
}

pub(crate) fn get_latest_compaction_sample(ctx: &Context, series: &TimeSeries) -> Option<Sample> {
    let src_id = series.src_series?;
    let Ok(Some(parent)) = get_series_by_id(ctx, src_id, false, None) else {
        // No source series or it doesn't exist
        return None;
    };

    let rule = parent.get_rule_by_dest_id(series.id)?;
    let start = rule.bucket_start?;

    let mut agg = rule.aggregator.clone();
    let value = AggregationHandler::finalize(&mut agg);

    let sample = Sample::new(start, value);
    Some(sample)
}

pub fn check_circular_dependencies(ctx: &Context, series: &mut TimeSeries) -> ValkeyResult<()> {
    let graph = build_dependency_graph(ctx, series)?;
    if graph.is_empty() {
        return Ok(());
    }
    Ok(())
}

/// Check if adding a new compaction rule would create a circular dependency
pub fn check_new_rule_circular_dependency(
    ctx: &Context,
    series: &mut TimeSeries,
    dest: &mut TimeSeries,
) -> ValkeyResult<()> {
    if series.rules.is_empty() {
        return Ok(());
    }

    let mut graph = build_dependency_graph(ctx, series)?;
    if graph.is_empty() {
        return Ok(());
    }

    // Check if the new rule would create a circular dependency
    // log_info(format!("candidate rule {} -> {}", series.id, dest.id));
    graph.insert(series.id, vec![dest.id]);
    build_dependency_graph_internal(ctx, dest, &mut graph)?;

    let SortResults::Full(_nodes) = graph.into_vec_nodes() else {
        return Err(ValkeyError::Str(
            error_consts::COMPACTION_CIRCULAR_DEPENDENCY,
        ));
    };

    Ok(())
}

pub fn build_dependency_graph(
    ctx: &Context,
    series: &mut TimeSeries,
) -> ValkeyResult<TopoSort<SeriesRef>> {
    let mut graph = TopoSort::with_capacity(10);

    if !series.rules.is_empty() {
        build_dependency_graph_internal(ctx, series, &mut graph)?;
    }

    Ok(graph)
}

fn build_dependency_graph_internal(
    ctx: &Context,
    source_series: &mut TimeSeries,
    graph: &mut TopoSort<SeriesRef>,
) -> ValkeyResult<()> {
    let mut destinations = get_compaction_series(ctx, source_series);
    if destinations.is_empty() {
        return Ok(());
    }
    let dest_ids = destinations.iter().map(|x| x.id).collect::<Vec<_>>();
    graph.insert(source_series.id, dest_ids);
    if graph.cycle_detected() {
        return Err(ValkeyError::Str(
            error_consts::COMPACTION_CIRCULAR_DEPENDENCY,
        ));
    }
    for dest in destinations.iter_mut() {
        build_dependency_graph_internal(ctx, dest, graph)?;
    }
    Ok(())
}
