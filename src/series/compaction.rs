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
    AddBatch {
        samples: &'a [Sample],
        prev_last: Option<Timestamp>,
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
        CompactionOp::AddBatch { samples, prev_last } => {
            handle_batch_compaction(ctx, samples, prev_last)
        }
        CompactionOp::RemoveRange { start, end } => {
            handle_compaction_range_removal(ctx, start, end)
        }
    }
}

/// Propagate a batch of samples (already merged into the source) through one rule.
///
/// Samples newer than `prev_last` are guaranteed fresh appends and stream through the
/// open-bucket aggregator in O(1) each, exactly like sequential single-sample adds. Samples at
/// or below `prev_last` may have replaced existing values, so their buckets are deduplicated
/// and each affected bucket is recalculated from the source once.
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
) -> TsdbResult<()> {
    debug_assert!(
        samples.is_sorted_by_key(|s| s.timestamp),
        "batch compaction requires samples sorted by timestamp"
    );

    let split = prev_last.map_or(0, |last| samples.partition_point(|s| s.timestamp <= last));
    let (upserts, appends) = samples.split_at(split);

    for sample in appends {
        handle_sample_compaction(ctx, *sample)?;
    }

    // One recalculation per affected bucket (samples are sorted, so same-bucket entries are
    // adjacent).
    let mut prev_bucket: Option<Timestamp> = None;
    for sample in upserts {
        let bucket_start = ctx.rule.calc_bucket_start(sample.timestamp);
        if prev_bucket == Some(bucket_start) {
            continue;
        }
        prev_bucket = Some(bucket_start);

        let bucket_end = bucket_start.saturating_add_unsigned(ctx.rule.bucket_duration);
        if ctx.rule.bucket_start == Some(bucket_start) {
            recalculate_current_bucket(ctx, bucket_start, bucket_end)?;
        } else {
            recalculate_bucket(ctx, bucket_start, bucket_end, null_ts_filter)?;
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
            let bucket_end = sample_bucket_start.saturating_add_unsigned(ctx.rule.bucket_duration);
            // Sample is in an older bucket (shouldn't happen for new samples, but handle gracefully)
            recalculate_bucket(ctx, sample_bucket_start, bucket_end, null_ts_filter)?;
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

    let duration = ctx.rule.bucket_duration;
    let bucket_end = current_bucket_start.saturating_add_unsigned(duration);

    if bucket_start == current_bucket_start {
        // This sample belongs to the current aggregation bucket
        // We need to recalculate the entire bucket since we don't know what changed
        recalculate_current_bucket(ctx, current_bucket_start, bucket_end)?;
        return Ok(());
    }

    // This is a historical upsert — recalculate the affected (closed) bucket.
    // The recompute range must be that bucket's own span; `bucket_end` above
    // belongs to the *current* open bucket, and using it here would fold every
    // sample between the historical bucket and the open one into the recompute.
    let historical_bucket_end = bucket_start.saturating_add_unsigned(duration);
    recalculate_bucket(ctx, bucket_start, historical_bucket_end, null_ts_filter)
}

/// Recalculate the current ongoing aggregation bucket
fn recalculate_current_bucket(
    ctx: &mut CompactionContext,
    bucket_start: Timestamp,
    bucket_end: Timestamp,
) -> TsdbResult<()> {
    // Reset the aggregator and recalculate from all samples in the bucket
    let has_samples = calculate_range(
        ctx.parent,
        &mut ctx.rule.aggregator,
        bucket_start,
        bucket_end - 1,
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
        let middle_start = first_bucket_start.saturating_add_unsigned(ctx.rule.bucket_duration);
        if middle_start < last_bucket_start && !ctx.dest.is_empty() {
            // Buckets tile the range, so the fully covered middle is exactly
            // [middle_start, last_bucket_start).
            ctx.dest.remove_range(middle_start, last_bucket_start - 1)?;
        }
        remove_or_recalculate_bucket(ctx, last_bucket_start, start, end)?;
    }

    // Adjust current in-memory aggregation (ongoing bucket), if affected
    adjust_current_bucket_after_removal(ctx, start, end);

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
    // that is still accepting samples (RTS reports nothing for it). Its aggregator is repaired
    // by `adjust_current_bucket_after_removal`, which runs after this, so leave it untouched.
    if ctx.rule.bucket_start == Some(bucket_start) {
        return Ok(());
    }

    let bucket_end = bucket_start.saturating_add_unsigned(ctx.rule.bucket_duration);

    if start <= bucket_start && end >= bucket_end {
        if !ctx.dest.is_empty() {
            ctx.dest.remove_range(bucket_start, bucket_end - 1)?;
        }
    } else {
        // Recalculate this bucket excluding removed timestamps.
        // If destination has no flushed buckets yet, this still correctly maintains the aggregator state.
        recalculate_bucket(ctx, bucket_start, bucket_end, |ts| ts < start || ts > end)?;
    }

    Ok(())
}

fn adjust_current_bucket_after_removal(
    ctx: &mut CompactionContext<'_>,
    removal_start: Timestamp,
    removal_end: Timestamp,
) {
    let Some(current_bucket_start) = ctx.rule.bucket_start else {
        // No active aggregation, nothing to adjust
        return;
    };

    let current_bucket_end = current_bucket_start.saturating_add_unsigned(ctx.rule.bucket_duration);

    // Check if the removal affects the current aggregation bucket. The removal range is
    // inclusive on both ends while the bucket is [start, end), so a removal ending exactly at
    // `current_bucket_start` still deletes that bucket's first sample: the bound is `>=`.
    if removal_start < current_bucket_end && removal_end >= current_bucket_start {
        let mut new_aggregator = ctx.rule.aggregator.clone();
        AggregationHandler::reset(&mut new_aggregator);

        // Re-aggregate samples that are not in the removal range
        let bucket_samples = ctx
            .parent
            .range_iter(current_bucket_start, current_bucket_end)
            .filter(|sample| sample.timestamp < removal_start || sample.timestamp > removal_end);

        let mut has_samples = false;
        for sample in bucket_samples {
            if AggregationHandler::update(&mut new_aggregator, sample.timestamp, sample.value) {
                has_samples = true;
            }
        }

        ctx.rule.aggregator = new_aggregator;
        ctx.rule.has_samples = has_samples;

        if !has_samples {
            // The open bucket lost every sample, so there is no aggregation in progress any
            // more. Drop `bucket_start` as well: leaving it set makes the next write compare
            // against a bucket that no longer holds anything, so a sample landing *earlier*
            // than it (possible once the deletion removed the series' last samples) takes the
            // back-fill branch in `handle_sample_compaction` and materializes a bucket that is
            // still open. Clearing it lets that sample simply open a fresh bucket.
            ctx.rule.bucket_start = None;
        }
    }
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
    let notify_destinations = matches!(
        op,
        CompactionOp::AddNew(_) | CompactionOp::AddBatch { .. }
    );
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

                let batch_op = CompactionOp::AddBatch {
                    samples: &samples,
                    prev_last,
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

fn add_dest_bucket(ctx: &mut CompactionContext, ts: Timestamp, value: f64) -> TsdbResult<()> {
    let bucket_start = ctx.rule.calc_bucket_start(ts);
    // Add the sample to the destination series
    // todo: specify to ignore whatever adjustments
    match ctx
        .dest
        .add(bucket_start, value, Some(DuplicatePolicy::KeepLast))
    {
        SampleAddResult::Ok(sample) => {
            ctx.written.push(sample);
            Ok(())
        }
        SampleAddResult::Ignored(_) => Ok(()), // duplicate sample, (ignored)
        SampleAddResult::TooOld => {
            // bucket start is too old, we cannot add it
            Ok(())
        }
        x => {
            let base_msg = format!("TSDB: failed to add sample @{ts} to destination bucket: {x}",);
            log_warning(base_msg.as_str());
            Err(TsdbError::General(base_msg))
        }
    }
}

fn calculate_range<F>(
    series: &TimeSeries,
    aggregator: &mut Aggregator,
    start: Timestamp,
    end: Timestamp,
    filter: F,
) -> bool
where
    F: Fn(Timestamp) -> bool,
{
    let mut has_samples = false;
    aggregator.reset();
    for sample in series
        .range_iter(start, end)
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
    ) -> TsdbResult<()> {
        if self.rules.is_empty() || samples.is_empty() {
            return Ok(());
        }
        apply_compaction(ctx, self, CompactionOp::AddBatch { samples, prev_last })
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
