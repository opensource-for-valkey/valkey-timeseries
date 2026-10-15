use super::{CompactionRule, TEMP_VEC_LEN};
use crate::aggregators::{AggregationHandler, Aggregator};
use crate::common::logging::log_warning;
use crate::common::{Sample, Timestamp};
use crate::error::{TsdbError, TsdbResult};
use crate::series::{DuplicatePolicy, SampleAddResult, TimeSeries};
use smallvec::SmallVec;
use std::cmp::Ordering;

pub(super) struct CompactionContext<'a> {
    pub(super) parent: &'a TimeSeries,
    pub(super) dest: &'a mut TimeSeries,
    pub(super) rule: &'a mut CompactionRule,
    /// Samples actually committed to `dest` during this operation (the stored, rounded
    /// values), in the order they were written. Used both to decide whether to notify on
    /// `dest`, and to cascade into `dest`'s own compaction rules — see
    /// [`process_series_with_compaction`].
    pub(super) written: Vec<Sample>,
}

impl<'a> CompactionContext<'a> {
    pub(super) fn new(
        parent: &'a TimeSeries,
        dest: &'a mut TimeSeries,
        rule: &'a mut CompactionRule,
    ) -> Self {
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
    /// (MADD argument order), which sorting `samples` discards. Back-fill classification replays
    /// it: an item at or below the running maximum *when it was applied* is a back-fill, which a
    /// comparison against `prev_last` alone cannot tell.
    AddBatch {
        samples: &'a [Sample],
        prev_last: Option<Timestamp>,
        input_order: &'a [Timestamp],
    },
    /// Remove a range from source and reflect it into destinations (and ongoing aggregation state)
    RemoveRange { start: Timestamp, end: Timestamp },
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

pub(super) fn apply_op(ctx: &mut CompactionContext<'_>, op: CompactionOp) -> TsdbResult<()> {
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
/// bucket `1000` back-fills. That distinction is consequential under retention: only the
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

    // Replay the caller's order to find the back-fills — items at or below the running maximum
    // by the time they are applied — and, with each, the series' last timestamp at that moment.
    // That timestamp fixes the retention floor the recalculation must read the source through:
    // applied sequentially, the trim would have run with exactly that window in force.
    let retention = ctx.parent.retention;
    let mut advanced: SmallVec<[Timestamp; TEMP_VEC_LEN]> = SmallVec::new();
    let mut backfilled: SmallVec<[(Timestamp, Timestamp); TEMP_VEC_LEN]> = SmallVec::new();
    let mut high_water = prev_last;
    // The bucket the rule is still accumulating into, tracked through the replay — not just
    // `ctx.rule.bucket_start`, which is the *pre-batch* value and does not see this batch's own
    // forward advances as they happen one by one.
    let mut open_bucket = ctx.rule.bucket_start;
    for &ts in input_order {
        match high_water {
            // A repeat of a timestamp this same batch already appended is a duplicate-policy
            // fold, not a back-fill, but only while it still lands in the bucket the rule is
            // currently accumulating into. The append is what advances the rule through its
            // buckets, and `samples` carries one entry per timestamp holding the folded value,
            // so classifying an open-bucket repeat as a back-fill would take that entry out of
            // the append stream entirely: the bucket it should have closed stays open and
            // unpublished, while the bucket it opened gets written to the destination as though
            // it were historical. `TS.MADD k 0 0 k 500 0 k 500 0` published bucket 500 and lost
            // bucket 0 that way.
            //
            // A repeat whose bucket a *later* forward advance in this same batch has since
            // closed is a genuine back-fill into already-published history, exactly like a
            // repeat of a distinct earlier timestamp: `TS.MADD k 0 0 k 2000 0 k 1000 0` closes
            // bucket 0 on the middle item, and the trailing `1000` back-fills it.
            //
            // A timestamp this call has *not* itself advanced through — even one that lands in
            // the currently open bucket — is a genuine out-of-order write, not a fold: it must
            // go through recalculation, which replays the bucket's stored samples in timestamp
            // order. Streaming it straight into the open aggregator via the append path assumes
            // call order is timestamp order, which order-sensitive aggregators like `last`/
            // `first` depend on — two separate `TS.MADD` calls, `1999` then `1500`, into the
            // same still-open 500ms bucket must report `last`=1 (`1999`'s value); streaming
            // `1500` straight in overwrites it with `1500`'s value instead.
            Some(max) if ts <= max => {
                if !advanced.contains(&ts) || Some(ctx.rule.calc_bucket_start(ts)) != open_bucket {
                    backfilled.push((ts, max));
                }
            }
            _ => {
                high_water = Some(ts);
                open_bucket = Some(ctx.rule.calc_bucket_start(ts));
                advanced.push(ts);
            }
        }
    }
    // Later entries win: a timestamp back-filled more than once settles at the floor of its
    // last recalculation. The high-water mark only rises in input order, so that is the
    // largest one — keep the max rather than whichever entry the sort left first (which was
    // the earliest, lowest floor).
    backfilled.sort_by_key(|(ts, _)| *ts);
    backfilled.dedup_by(|dup, kept| {
        if dup.0 == kept.0 {
            kept.1 = kept.1.max(dup.1);
            true
        } else {
            false
        }
    });

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

    // One recalculation per affected bucket, using the *largest* retention floor across
    // all back-fills to that bucket. When an append between two back-fills advances the
    // retention window past earlier samples (e.g. MADD 0 v 1001 v 2 v with a retention that
    // evicts timestamp 0 after 1001 arrives), the first back-fill's floor sees the earlier
    // samples while the second back-fill's floor does not; the last one the sequential path
    // would run is the one with the highest floor, which matches the source state that
    // survives retention after the batch is complete. That floor is not tied to the bucket's
    // highest timestamp — a lower timestamp can be back-filled later in the batch, after the
    // high-water mark has risen — so it is taken as the max over the whole bucket.
    //
    // A bucket only reaches the destination when it closes, so `recalculate_bucket` (which
    // writes) is for buckets the rule has already moved past. When the rule has no open bucket
    // at all this upsert is opening it, exactly as the single-sample path does for its own
    // "no current bucket" case — publishing here would expose a bucket still accepting samples.
    // A batch reaches that state whenever a timestamp appears twice in the caller's order: the
    // repeat reads as a back-fill, `samples` carries the timestamp once, so it is classified as
    // an upsert and never streams through the append path that would have opened the bucket.
    //
    // Buckets are processed newest first, so when this batch opens the rule's bucket it opens
    // the latest one.
    let mut buckets: SmallVec<[(Timestamp, Timestamp); TEMP_VEC_LEN]> = SmallVec::new();
    for (sample, min_ts) in &upserts {
        let bucket_start = ctx.rule.calc_bucket_start(sample.timestamp);
        match buckets.last_mut() {
            Some((start, floor)) if *start == bucket_start => *floor = (*floor).max(*min_ts),
            _ => buckets.push((bucket_start, *min_ts)),
        }
    }

    for &(bucket_start, min_ts) in buckets.iter().rev() {
        let bucket_end = ctx.rule.bucket_end(bucket_start);
        if ctx.rule.bucket_start.is_none() || ctx.rule.bucket_start == Some(bucket_start) {
            recalculate_current_bucket(ctx, bucket_start, bucket_end, min_ts)?;
        } else {
            recalculate_bucket(ctx, bucket_start, bucket_end, min_ts, null_ts_filter)?;
        }
    }

    Ok(())
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
            let bucket_end = ctx.rule.bucket_end(sample_bucket_start);
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

        add_dest_bucket(ctx, current_bucket_start, aggregated_value)?;
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

    let bucket_end = ctx.rule.bucket_end(current_bucket_start);

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
    let historical_bucket_end = ctx.rule.bucket_end(bucket_start);
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
        ctx.dest.remove_range(bucket_start, bucket_end - 1)?;
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
        let middle_start = ctx.rule.bucket_end(first_bucket_start);
        if middle_start < last_bucket_start && !ctx.dest.is_empty() {
            // Buckets tile the range, so the fully covered middle is exactly
            // [middle_start, last_bucket_start).
            ctx.dest.remove_range(middle_start, last_bucket_start - 1)?;
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

    let bucket_end = ctx.rule.bucket_end(bucket_start);

    if start <= bucket_start && end >= bucket_end {
        if !ctx.dest.is_empty() {
            ctx.dest.remove_range(bucket_start, bucket_end - 1)?;
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
            ctx.dest.remove_range(Timestamp::MIN, Timestamp::MAX)?;
        }
        return Ok(());
    };

    let (open_start, open_end) = ctx.rule.get_bucket_range(last_sample.timestamp);

    // The re-opened bucket and anything after it are no longer closed.
    if !ctx.dest.is_empty() {
        ctx.dest.remove_range(open_start, Timestamp::MAX)?;
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

/// Write one aggregated bucket to the destination.
fn add_dest_bucket(ctx: &mut CompactionContext, ts: Timestamp, value: f64) -> TsdbResult<()> {
    let bucket_start = ctx.rule.calc_bucket_start(ts);
    match ctx
        .dest
        .add(bucket_start, value, Some(DuplicatePolicy::KeepLast))
    {
        SampleAddResult::Ok(sample) => {
            ctx.written.push(sample);
            Ok(())
        }
        // A duplicate the destination ignored, or a bucket below its retention window.
        SampleAddResult::Ignored(_) | SampleAddResult::TooOld => Ok(()),
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
