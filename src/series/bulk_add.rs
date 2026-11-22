//! Bulk ingest: merges a batch of samples into a series' chunks.
//!
//! The shared core of `TS.ADDBULK` ([`bulk_insert_samples`]) and `TS.MADD`
//! (`sample_merge::merge_samples`): normalize the batch, group it by destination chunk, merge
//! each group, and place each outcome back at its sample's input index.
//!
//! Groups are merged in parallel only when the batch is big enough to pay for a pool round
//! trip ([`request_par_threads`] counts samples). [`MAX_SAMPLES_PER_INSERT`] keeps every
//! `TS.ADDBULK` batch below that line, so the parallel branches run for large `TS.MADD`
//! batches only; tune `TS.ADDBULK` on the sequential path.
#[cfg(not(test))]
use crate::common::block_on_keys::signal_timeseries_ready;
use crate::common::context::key_for_display;
#[cfg(not(test))]
use crate::common::context::{create_key_string, notify_keyspace_event};
use crate::common::logging::log_warning;
use crate::common::threads::{IterIntoParRayon, request_par_threads};
use crate::common::{Sample, Timestamp};
use crate::error_consts;
use crate::series::chunks::{ChunkEncoding, ChunkOps, TimeSeriesChunk};
use crate::series::ingest_normalize::{NormalizedBatch, normalize_batch};
use crate::series::{DuplicatePolicy, SampleAddResult, TimeSeries, seal_chunk};
use orx_parallel::Par;
use simd_json::base::{ValueAsArray, ValueAsScalar};
use simd_json::borrowed::Value;
use simd_json::prelude::ValueObjectAccess;
use valkey_module::{Context, ValkeyError, ValkeyResult};

/// Most samples one `TS.ADDBULK` payload may carry.
pub(crate) const MAX_SAMPLES_PER_INSERT: usize = 1_000;

// Chunk sizing for bulk ingest. Samples newer than the series and samples older than it are
// both runs that no existing chunk holds, and both are written exactly as single adds would:
// each chunk is filled up to `is_full` and then sealed (see `fill_chunks`), so none ends past
// `max_size`. Both used to be planned from an estimate of encoded bytes per sample, which ended
// chunks a few bytes past `max_size` about half the time, and backfill chunks overfilled by up
// to an eighth on purpose; either put a sealed 4 KiB chunk in jemalloc's 5,120-byte size class.
//
// Backfill chunks are sealed for good (chunks after them own every later timestamp), and the
// last one keeps whatever remainder it got (see `backfill_run`).

/// Parses a `TS.ADDBULK` payload: a single JSON object with equal-length `values` and
/// `timestamps` arrays of at most [`MAX_SAMPLES_PER_INSERT`] entries. Timestamps must be
/// non-negative integers. Returns the samples sorted by timestamp.
pub(crate) fn parse_bulk_payload(input: &mut [u8]) -> ValkeyResult<Vec<Sample>> {
    let v: Value = simd_json::to_borrowed_value(input)?;

    let values_arr = v
        .get("values")
        .and_then(|v| v.as_array())
        .ok_or(ValkeyError::Str(error_consts::BULK_MISSING_VALUES))?;

    let timestamps_arr = v
        .get("timestamps")
        .and_then(|t| t.as_array())
        .ok_or(ValkeyError::Str(error_consts::BULK_MISSING_TIMESTAMPS))?;

    if timestamps_arr.is_empty() || values_arr.is_empty() {
        return Err(ValkeyError::Str(error_consts::BULK_EMPTY_PAYLOAD));
    }

    if timestamps_arr.len() != values_arr.len() {
        return Err(ValkeyError::Str(error_consts::BULK_LENGTH_MISMATCH));
    }

    if values_arr.len() > MAX_SAMPLES_PER_INSERT {
        return Err(ValkeyError::Str(error_consts::TOO_MANY_SAMPLES));
    }

    let mut samples = Vec::with_capacity(values_arr.len());
    for (val, ts) in values_arr.iter().zip(timestamps_arr.iter()) {
        let value = val
            .cast_f64()
            .ok_or(ValkeyError::Str(error_consts::BULK_INVALID_VALUE))?;

        let timestamp_u64 = ts
            .as_u64()
            .ok_or(ValkeyError::Str(error_consts::BULK_INVALID_TIMESTAMP))?;
        // `TS.ADD`'s domain, `0..=i64::MAX`. An `as` cast stored 2^63 and up as negative
        // timestamps, which reads skip and the RDB loader refuses.
        let timestamp = Timestamp::try_from(timestamp_u64)
            .map_err(|_| ValkeyError::Str(error_consts::INVALID_TIMESTAMP))?;

        samples.push(Sample { timestamp, value });
    }

    // Stable on purpose: of two samples sharing a timestamp, the first in the payload must
    // stay first, so that it is the one stored and the later one is reported `Duplicate`
    // (see `normalize_batch`).
    samples.sort_by_key(|s| s.timestamp);

    Ok(samples)
}

/// Where a [`ChunkSampleGroup`] is merged.
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
enum GroupTarget {
    /// The existing chunk at this index.
    Existing(usize),
    /// Samples older than every existing chunk: written in order into new chunks placed
    /// before them.
    NewBefore,
    /// Samples newer than every existing chunk (all of them, for a series without chunks):
    /// appended in order to the last chunk until it is full, then to new chunks.
    Append,
}

/// A contiguous sub-slice of the batch that all goes to the same chunk.
#[derive(Debug)]
struct ChunkSampleGroup<'a> {
    target: GroupTarget,
    samples: &'a [Sample],
}

/// A group into an existing chunk, with its offset into the normalized batch.
struct ExistingGroup<'a> {
    offset: usize,
    chunk_idx: usize,
    samples: &'a [Sample],
}

/// A group not merged into an existing chunk (the backfill or the appends), with its offset
/// into the normalized batch.
struct NewGroup<'a> {
    offset: usize,
    samples: &'a [Sample],
}

/// The merge outcome of one group: one result per sample of the group, which starts at
/// `offset` in the normalized batch and holds `len` samples.
struct GroupOutcome {
    offset: usize,
    len: usize,
    results: Vec<SampleAddResult>,
}

/// Merges one group into `chunk`.
///
/// A merge that fails as a whole reports every sample of the group as `Error`. That is
/// accurate because chunk merges either fail before changing the chunk or report failures
/// per sample: the out-of-order path encodes into a fresh buffer and swaps it in only on
/// success, and the append path (`merge::append_samples`) fails as a whole only on
/// `CapacityFull`, which no encoding that uses it returns.
fn merge_group(
    chunk: &mut TimeSeriesChunk,
    samples: &[Sample],
    policy: Option<DuplicatePolicy>,
    key: &[u8],
) -> Vec<SampleAddResult> {
    match chunk.merge_samples(samples, policy) {
        Ok(results) => results,
        Err(e) => {
            log_warning(format!(
                "TSDB: failed to merge {} samples into a chunk of '{}': {e}",
                samples.len(),
                key_for_display(key)
            ));
            vec![SampleAddResult::Error(error_consts::CANNOT_ADD_SAMPLE); samples.len()]
        }
    }
}

/// Groups a sorted `samples` slice into the chunk they would belong to if inserted.
///
/// Rules:
/// - `samples` must be sorted by `timestamp` ascending, already normalized (retention
///   filtering happens in [`normalize_batch`], not here), and `series` must hold no empty
///   chunk (see [`drop_empty_chunks_of_empty_series`]).
/// - Samples older than the first existing chunk form one `NewBefore` group, consumed
///   strictly below the first chunk's start so the new chunks cannot overlap existing ones;
///   `backfill_run` writes it into new chunks.
/// - Each existing chunk consumes every remaining sample up to its last timestamp. This
///   also routes samples falling in the gap *before* a chunk into that chunk (as upserts),
///   so a new chunk is never created overlapping an existing one.
/// - Samples newer than the last chunk (every sample, for a series without chunks) form one
///   `Append` group, which `append_run` writes exactly as single adds would.
/// - Returns groups in ascending timestamp order: the `NewBefore` group, then the existing
///   chunks in index order, then the `Append` group. Together they cover `samples` exactly,
///   each group borrowing a contiguous sub-slice of it.
///
/// # Arguments
/// * `series` - The time series to group samples into
/// * `samples` - The samples to group
fn group_samples_by_chunk<'a>(
    series: &TimeSeries,
    samples: &'a [Sample],
) -> Vec<ChunkSampleGroup<'a>> {
    if samples.is_empty() {
        return Vec::new();
    }
    debug_assert!(
        series.chunks.iter().all(|c| !c.is_empty()),
        "grouping needs a series without empty chunks"
    );

    // At most one group per sample, so a small batch into a long series reserves little; the
    // two extra slots are the backfill and the appends.
    let estimated_groups = series.chunks.len().min(samples.len()).saturating_add(2);
    let mut out: Vec<ChunkSampleGroup<'a>> = Vec::with_capacity(estimated_groups);

    // No chunks: every sample is an append.
    if series.chunks.is_empty() {
        out.push(ChunkSampleGroup {
            target: GroupTarget::Append,
            samples,
        });
        return out;
    }

    let mut i = 0usize;

    // Samples older than the first existing chunk go to new chunk(s).
    let first_chunk_start = series.chunks[0].first_timestamp();
    if samples[i].timestamp < first_chunk_start {
        i = samples.partition_point(|s| s.timestamp < first_chunk_start);
        out.push(ChunkSampleGroup {
            target: GroupTarget::NewBefore,
            samples: &samples[..i],
        });

        if i >= samples.len() {
            return out;
        }
    }

    let last_index = series.chunks.len() - 1;

    // Visit only the chunks the batch reaches: chunks are sorted and disjoint, so the next one
    // to take samples is the first ending at or after the next sample, found by binary search.
    // Samples past every chunk are the `Append` group.
    while i < samples.len() {
        let chunk_idx = series
            .chunks
            .partition_point(|c| c.last_timestamp() < samples[i].timestamp)
            .min(last_index);
        let chunk = &series.chunks[chunk_idx];
        let start = i;

        let chunk_last = chunk.last_timestamp();
        while i < samples.len() && samples[i].timestamp <= chunk_last {
            i += 1;
        }

        if start < i {
            out.push(ChunkSampleGroup {
                target: GroupTarget::Existing(chunk_idx),
                samples: &samples[start..i],
            });
        }
        if chunk_idx == last_index {
            break;
        }
    }

    if i < samples.len() {
        out.push(ChunkSampleGroup {
            target: GroupTarget::Append,
            samples: &samples[i..],
        });
    }

    out
}

/// Disjoint `&mut` references to `slice[indices[0]]`, `slice[indices[1]]`, ...
///
/// `indices` must be strictly increasing and in bounds. This is safe code: a violation
/// panics (in release builds too), it cannot alias.
fn get_many_mut_ascending<'a, T>(slice: &'a mut [T], indices: &[usize]) -> Vec<&'a mut T> {
    let mut out: Vec<&'a mut T> = Vec::with_capacity(indices.len());
    let mut base = 0usize;
    let mut tail: &'a mut [T] = slice;

    for &idx in indices {
        debug_assert!(idx >= base, "indices must be strictly increasing");
        let (_, rest) = tail.split_at_mut(idx - base);
        let (item, rest) = rest.split_at_mut(1);
        out.push(&mut item[0]);
        tail = rest;
        base = idx + 1;
    }

    out
}

/// A series with no samples may still hold one empty chunk: deletes, defrag and the trim drop
/// the chunks they empty, but a load keeps a lone one (see `serialization::drop_empty_chunks`).
/// An empty chunk reports `first_timestamp() == 0`, so left ahead of the new chunks it would
/// pin the series' `first_timestamp` at 0. Cheap: a series without samples has at most a
/// chunk or two.
fn drop_empty_chunks_of_empty_series(series: &mut TimeSeries) {
    if series.is_empty() && !series.chunks.is_empty() {
        series.chunks.retain(|c| !c.is_empty());
    }
}

/// Writes one group's merge results at the input indices of its samples.
///
/// A chunk merge returns exactly one result per sample of the group, in order. Should one
/// ever return fewer, the samples without a result are reported `Error` rather than letting
/// later results shift onto the wrong samples.
fn place_results(results: &mut [SampleAddResult], insert_index: &[usize], outcome: GroupOutcome) {
    debug_assert_eq!(
        outcome.results.len(),
        outcome.len,
        "a chunk merge must return one result per sample"
    );
    let mut group_results = outcome.results.into_iter();
    for &index in &insert_index[outcome.offset..outcome.offset + outcome.len] {
        results[index] = group_results
            .next()
            .unwrap_or(SampleAddResult::Error(error_consts::CANNOT_ADD_SAMPLE));
    }
}

/// Merges every group into its existing chunk, in parallel when the batch is big enough.
/// Returns each group's outcome and how many samples the chunks grew by in all.
fn merge_into_existing_chunks(
    series: &mut TimeSeries,
    groups: &[ExistingGroup<'_>],
    policy: Option<DuplicatePolicy>,
) -> (Vec<GroupOutcome>, usize) {
    if groups.is_empty() {
        return (Vec::new(), 0);
    }

    let key: &[u8] = &series.key;
    let indices: Vec<usize> = groups.iter().map(|g| g.chunk_idx).collect();
    let chunks = get_many_mut_ascending(series.chunks.as_mut_slice(), &indices);

    let merge = |(chunk, group): (&mut TimeSeriesChunk, &ExistingGroup<'_>)| {
        let len_before = chunk.len();
        let results = merge_group(chunk, group.samples, policy, key);
        let outcome = GroupOutcome {
            offset: group.offset,
            len: group.samples.len(),
            results,
        };
        (outcome, chunk.len().saturating_sub(len_before))
    };

    // A MADD group is a handful of samples into one chunk; the parallel path is for bulk
    // loads spanning many chunks.
    let work: usize = groups.iter().map(|g| g.samples.len()).sum();
    let threads = request_par_threads(groups.len(), work);
    let merged: Vec<(GroupOutcome, usize)> = if threads == 1 {
        chunks.into_iter().zip(groups.iter()).map(merge).collect()
    } else {
        chunks
            .into_iter()
            .zip(groups.iter())
            .iter_into_par_rayon()
            .num_threads(threads)
            .map(merge)
            .collect()
    };

    let grown = merged.iter().map(|(_, grown)| grown).sum();
    (
        merged.into_iter().map(|(outcome, _)| outcome).collect(),
        grown,
    )
}

/// Writes `samples`, sorted and newer than anything in `chunks`, the way single adds do: into
/// the last of `chunks` until it is full, then into new chunks of `encoding` and `chunk_size`,
/// sealing each chunk it leaves. Returns one result per sample and how many were stored.
///
/// The fill is exact, so no chunk ends past `max_size` (see `XorChunk::is_full`). A chunk
/// opened for a sample that then failed is left empty at the end, for the caller to drop.
/// Each chunk is filled by one `append_until_full` call, a loop compiled per encoding.
/// Sequential on purpose: a batch reaches past one chunk only when it carries more than a
/// chunk of samples for one series, and encoding a chunk takes a few microseconds.
fn fill_chunks(
    chunks: &mut Vec<TimeSeriesChunk>,
    samples: &[Sample],
    encoding: ChunkEncoding,
    chunk_size: usize,
    key: &[u8],
) -> (Vec<SampleAddResult>, usize) {
    let mut results = Vec::with_capacity(samples.len());
    let mut stored = 0usize;
    let mut failed = None;
    let mut rest = samples;
    while !rest.is_empty() {
        if chunks.last().is_none_or(|c| c.is_full()) {
            if let Some(full) = chunks.last_mut() {
                seal_chunk(full);
            }
            chunks.push(TimeSeriesChunk::new(encoding, chunk_size));
        }
        let Some(chunk) = chunks.last_mut() else {
            unreachable!("a chunk was just pushed");
        };
        // The chunk is not full, so this stores a sample or fails on one: `rest` shrinks.
        let (appended, error) = chunk.append_until_full(rest);
        results.extend(rest[..appended].iter().map(|s| SampleAddResult::Ok(*s)));
        stored += appended;
        rest = &rest[appended..];
        if let Some(e) = error {
            failed.get_or_insert(e);
            results.push(SampleAddResult::Error(error_consts::CANNOT_ADD_SAMPLE));
            rest = &rest[1..];
        }
    }
    if let Some(e) = failed {
        log_warning(format!(
            "TSDB: failed to add samples to '{}': {e}",
            key_for_display(key)
        ));
    }
    (results, stored)
}

/// Appends `group` (samples newer than every chunk) to the last chunk until it is full, then
/// to new chunks (see [`fill_chunks`]). The last chunk stays open: it is the append target.
/// Returns the group's outcome and how many samples were stored.
fn append_run(series: &mut TimeSeries, group: &NewGroup<'_>) -> (GroupOutcome, usize) {
    let (encoding, chunk_size) = (series.chunk_encoding, series.chunk_size_bytes);
    let (results, appended) = fill_chunks(
        &mut series.chunks,
        group.samples,
        encoding,
        chunk_size,
        &series.key,
    );
    // A series must not hold an empty chunk next to others.
    if series.chunks.len() > 1 && series.chunks.last().is_some_and(|c| c.is_empty()) {
        series.chunks.pop();
    }
    let outcome = GroupOutcome {
        offset: group.offset,
        len: group.samples.len(),
        results,
    };
    (outcome, appended)
}

/// Writes `group` (samples older than every chunk) into new chunks filled as appends fill
/// them (see [`fill_chunks`]) and seals them all. Returns the chunks in ascending order, the
/// group's outcome and how many samples were stored.
///
/// The chunks are sealed for good: they go before chunks that already own every later
/// timestamp, so nothing tops them up later. The last one keeps whatever remainder it got:
/// evening it out with the chunk before it re-encoded a chunk's worth of samples (up to 15 %
/// more ingest CPU on batched backfills) and saved nothing, since a full chunk plus a small
/// remainder never takes more allocator size classes than two half-full chunks.
fn backfill_run(
    series: &TimeSeries,
    group: &NewGroup<'_>,
) -> (Vec<TimeSeriesChunk>, GroupOutcome, usize) {
    let (encoding, chunk_size) = (series.chunk_encoding, series.chunk_size_bytes);
    let mut chunks = Vec::new();
    let (results, stored) = fill_chunks(
        &mut chunks,
        group.samples,
        encoding,
        chunk_size,
        &series.key,
    );
    if chunks.last().is_some_and(|c| c.is_empty()) {
        chunks.pop();
    }
    // `fill_chunks` sealed every chunk it left but the last.
    if let Some(last) = chunks.last_mut() {
        seal_chunk(last);
    }
    let outcome = GroupOutcome {
        offset: group.offset,
        len: group.samples.len(),
        results,
    };
    (chunks, outcome, stored)
}

/// Shared bulk-merge core used by both `TS.ADDBULK` (`bulk_insert_samples`) and `TS.MADD`
/// (`sample_merge::merge_samples`): normalize, group by destination chunk, merge each group,
/// place results back in input order, and refresh series metadata.
///
/// Existing-chunk groups are merged through disjoint `&mut` borrows of their chunks, in
/// parallel when the batch is big enough. Samples older than the series are built into new
/// chunks spliced in before the existing ones, and samples newer than it are appended after;
/// both fill each chunk exactly (see [`fill_chunks`]).
///
/// Callers are responsible for post-merge maintenance (`split_chunks_if_needed`),
/// keyspace notification and compaction propagation.
///
/// Returns one result per input sample. Of samples sharing a timestamp, only the first in
/// input order is merged; the others are reported `Duplicate` without consulting the
/// duplicate policy (see `normalize_batch`).
pub(super) fn merge_samples_into_series(
    series: &mut TimeSeries,
    samples: &[Sample],
    policy: Option<DuplicatePolicy>,
) -> Vec<SampleAddResult> {
    if samples.is_empty() {
        return Vec::new();
    }
    let prepared = prepare_merge(series, samples, policy);
    merge_prepared(series, prepared)
}

/// A batch normalized for [`merge_prepared`], with the duplicate policy it was normalized under.
pub(super) struct PreparedMerge {
    pub(super) batch: NormalizedBatch,
    policy: Option<DuplicatePolicy>,
}

/// The first half of [`merge_samples_into_series`]: normalizes `samples` against `series`
/// without changing it, so a caller can look at the batch (`TS.MADD` checks
/// [`NormalizedBatch::has_repeats`]) before committing to the merge.
pub(super) fn prepare_merge(
    series: &TimeSeries,
    samples: &[Sample],
    policy: Option<DuplicatePolicy>,
) -> PreparedMerge {
    // The one place the duplicate policy is resolved, so that a `None` override honors the
    // series' configured policy rather than the chunk merge's KeepLast default.
    let policy = Some(series.sample_duplicates.resolve_policy(policy));
    // Apply the same retention/rounding/IGNORE normalization as single-sample add.
    let batch = normalize_batch(series, samples, policy);
    PreparedMerge { batch, policy }
}

/// The second half of [`merge_samples_into_series`]: merges a batch [`prepare_merge`]
/// normalized against this same, unchanged `series`.
pub(super) fn merge_prepared(
    series: &mut TimeSeries,
    prepared: PreparedMerge,
) -> Vec<SampleAddResult> {
    let PreparedMerge {
        batch:
            NormalizedBatch {
                to_insert,
                insert_index,
                mut results,
                has_repeats: _,
            },
        policy,
    } = prepared;

    if to_insert.is_empty() {
        return results;
    }

    drop_empty_chunks_of_empty_series(series);

    // Split the groups by kind. They are contiguous, ascending and cover `to_insert`, so
    // each one's offset into it is the running total of the lengths before it.
    let groups = group_samples_by_chunk(series, &to_insert);
    let mut existing: Vec<ExistingGroup<'_>> = Vec::with_capacity(groups.len());
    let mut backfill: Option<NewGroup<'_>> = None;
    let mut append: Option<NewGroup<'_>> = None;
    let mut offset = 0usize;
    for group in &groups {
        let new_group = NewGroup {
            offset,
            samples: group.samples,
        };
        match group.target {
            GroupTarget::Existing(chunk_idx) => existing.push(ExistingGroup {
                offset,
                chunk_idx,
                samples: group.samples,
            }),
            GroupTarget::NewBefore => {
                debug_assert!(backfill.is_none(), "one backfill group at most");
                backfill = Some(new_group);
            }
            GroupTarget::Append => {
                debug_assert!(append.is_none(), "one append group at most");
                append = Some(new_group);
            }
        }
        offset += group.samples.len();
    }
    debug_assert_eq!(offset, to_insert.len(), "groups must cover the batch");

    let (outcomes, mut grown) = merge_into_existing_chunks(series, &existing, policy);
    for outcome in outcomes {
        place_results(&mut results, &insert_index, outcome);
    }

    // Backfill chunks lie strictly before the first existing chunk (see
    // `group_samples_by_chunk`) and come in ascending order, so splicing them in at the front
    // keeps the chunks sorted without a sort.
    if let Some(group) = backfill {
        let (before, outcome, stored) = backfill_run(series, &group);
        place_results(&mut results, &insert_index, outcome);
        grown += stored;
        series.chunks.splice(0..0, before);
    }

    // Appends go last: they extend whatever the existing merges left in the last chunk.
    if let Some(group) = append {
        let (outcome, appended) = append_run(series, &group);
        place_results(&mut results, &insert_index, outcome);
        grown += appended;
    }
    debug_assert!(
        series
            .chunks
            .windows(2)
            .all(|w| w[0].last_timestamp() < w[1].first_timestamp()),
        "chunks must stay sorted and disjoint"
    );

    // Merges only add samples (an upsert of an existing timestamp replaces in place), so the
    // count moves by the growth of the chunks this batch touched; no need to walk them all.
    series.total_samples += grown;
    debug_assert_eq!(
        series.total_samples,
        series.chunks.iter().map(|c| c.len()).sum::<usize>(),
        "total_samples must match the chunks"
    );
    // Even without new chunks, a merge can append to the last chunk or update the value at
    // the current last timestamp.
    series.update_first_last_timestamps();

    results
}

/// Bulk insert for `TS.ADDBULK`: runs the shared merge core, then performs post-merge
/// maintenance (chunk splitting), keyspace notification, compaction propagation and the
/// retention trim.
pub fn bulk_insert_samples(
    ctx: &Context,
    series: &mut TimeSeries,
    samples: &[Sample],
    policy: Option<DuplicatePolicy>,
) -> Vec<SampleAddResult> {
    if samples.is_empty() {
        return Vec::new();
    }

    #[cfg_attr(test, allow(unused_variables))]
    let count_before = series.total_samples;
    // Last timestamp before the merge: batch compaction uses it to tell guaranteed-fresh
    // appends apart from samples that may have replaced existing values.
    let prev_last = series.last_sample.map(|s| s.timestamp);

    let results = merge_samples_into_series(series, samples, policy);

    series.split_chunks_if_needed().unwrap_or_else(|e| {
        ctx.log_warning(&format!(
            "Failed to split chunks after bulk insert samples: {}",
            e
        ))
    });

    #[cfg(not(test))]
    notify_added(
        ctx,
        &series.key,
        results.iter().any(SampleAddResult::is_ok),
        series.total_samples > count_before,
    );

    // Propagate the accepted samples to compaction destinations in one batch.
    if !series.rules.is_empty() {
        let mut added: Vec<Sample> = results
            .iter()
            .filter_map(|res| {
                if let SampleAddResult::Ok(s) = res {
                    Some(*s)
                } else {
                    None
                }
            })
            .collect();

        // `results` follow the caller's input order; batch compaction needs ascending
        // timestamps. TS.ADDBULK pre-sorts its input, so this is normally a no-op check.
        // The pre-sort order is kept separately: back-fill classification replays it (see
        // `CompactionOp::Ingest`).
        let input_order: Vec<Timestamp> = added.iter().map(|s| s.timestamp).collect();
        if !added.is_sorted_by_key(|s| s.timestamp) {
            added.sort_unstable_by_key(|s| s.timestamp);
        }

        if let Err(e) = series.batch_compaction(ctx, &added, prev_last, &input_order) {
            ctx.log_warning(&format!(
                "Failed to run compactions after bulk insert samples: {e:?}"
            ))
        }
    }

    // Last, as in TS.ADD / TS.MADD: after the sample-count check above, which a trim could
    // defeat by dropping more than the batch added, and after compaction, which must see the
    // pre-trim series (see `TimeSeries::apply_retention`).
    series.apply_retention();

    results
}

/// Emits the write's events. `ts.add` fires whenever a sample was accepted, upserts of
/// existing timestamps included, as for TS.ADD and TS.MADD. It is `ts.add` even when the
/// target is a compaction destination: `ts.add:dest` marks rule output only (emitted by the
/// compaction path), matching TS.ADD/TS.MADD on RTS. A blocked `TS.READ` waits for new
/// samples, so it is signalled only when the series grew.
#[cfg(not(test))]
fn notify_added(ctx: &Context, key: &[u8], accepted: bool, grew: bool) {
    if !accepted {
        return;
    }
    let key = create_key_string(ctx, key);
    notify_keyspace_event(ctx, c"ts.add", &key);
    if grew {
        signal_timeseries_ready(ctx, &key);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::series::SPLIT_SLACK_DIVISOR;
    use crate::tests::generators::{DataGenerator, ValueWorkload};
    use std::time::Duration;

    /// The oldest timestamp the series will accept, derived from its retention window.
    fn get_min_allowed_timestamp(series: &TimeSeries) -> Timestamp {
        if series.retention.is_zero() {
            0
        } else {
            series.get_min_timestamp()
        }
    }

    fn generate_random_samples(count: usize) -> Vec<Sample> {
        DataGenerator::builder()
            .samples(count)
            .start(1000)
            .interval(Duration::from_millis(1000))
            .decimal_digits(4)
            .build()
            .generate()
    }

    #[test]
    fn test_parse_json_line() {
        let mut data = br#"{
            "values": [1, 1, 1],
            "timestamps": [1549891472010, 1549891487724, 1549891503438]
        }"#
        .to_vec();

        let parsed = parse_bulk_payload(&mut data).unwrap();
        assert_eq!(parsed.len(), 3);
        assert_eq!(parsed[0].value, 1.0);
        assert_eq!(parsed[0].timestamp, 1549891472010);
        assert_eq!(parsed[2].timestamp, 1549891503438);
    }

    /// Timestamps share `TS.ADD`'s domain, `0..=i64::MAX`. A JSON integer from 2^63 up used to
    /// be cast to a negative timestamp, stored, and then refused by the RDB loader.
    #[test]
    fn test_parse_timestamps_past_i64_max_are_refused() {
        for ts in [i64::MAX as u64 + 1, u64::MAX] {
            let mut data =
                format!(r#"{{"values": [1, 2], "timestamps": [1000, {ts}]}}"#).into_bytes();
            match parse_bulk_payload(&mut data) {
                Err(ValkeyError::Str(msg)) => {
                    assert_eq!(msg, error_consts::INVALID_TIMESTAMP, "{ts}")
                }
                other => panic!("{ts}: expected the timestamp to be refused, got {other:?}"),
            }
        }

        let mut data = format!(r#"{{"values": [1], "timestamps": [{}]}}"#, i64::MAX).into_bytes();
        let parsed = parse_bulk_payload(&mut data).unwrap();
        assert_eq!(parsed[0].timestamp, i64::MAX);
    }

    #[test]
    fn test_parse_mismatched_lengths() {
        let mut data = br#"{
            "metric": {"__name__": "test"},
            "values": [1, 2],
            "timestamps": [1000]
        }"#
        .to_vec();

        assert!(parse_bulk_payload(&mut data).is_err());
    }

    fn s(ts: i64, v: f64) -> Sample {
        Sample {
            timestamp: ts,
            value: v,
        }
    }

    #[test]
    fn group_empty_samples_returns_empty() {
        let series = TimeSeries::default();
        let groups = group_samples_by_chunk(&series, &[]);
        assert!(groups.is_empty());
    }

    #[test]
    fn normalize_skips_samples_older_than_retention_min_timestamp() {
        let mut series = TimeSeries::default();
        // Create initial data so the series isn't empty and has retention-derived min timestamp.
        // Insert a sample to establish a baseline.
        bulk_insert_samples(&Context::dummy(), &mut series, &[s(100_000, 1.0)], None);
        series.retention = Duration::from_millis(1_000);

        let min_allowed = get_min_allowed_timestamp(&series);

        // One sample below the retention window, one inside.
        let samples = vec![s(min_allowed - 1, 1.0), s(min_allowed, 2.0)];
        let batch = normalize_batch(&series, &samples, None);

        // Retention filtering happens in normalization, before grouping.
        assert!(matches!(batch.results[0], SampleAddResult::TooOld));
        let kept: Vec<i64> = batch.to_insert.iter().map(|x| x.timestamp).collect();
        assert_eq!(kept, vec![min_allowed]);
    }

    #[test]
    fn normalize_retention_gate_is_input_order_sensitive() {
        // On an empty series with retention, an item is TooOld only if it is older than the floor
        // induced by items at or before it in INPUT order — matching sequential TS.MADD. A later
        // item raising the floor does not retroactively reject an earlier accepted one.
        let mk = || TimeSeries {
            retention: Duration::from_millis(1_000),
            ..Default::default()
        };

        // Newer-first: the older item is below the floor the newer item established -> TooOld.
        let series = mk();
        let batch = normalize_batch(&series, &[s(1001, 0.0), s(0, 0.0)], None);
        assert!(matches!(batch.results[1], SampleAddResult::TooOld));
        let kept: Vec<i64> = batch.to_insert.iter().map(|x| x.timestamp).collect();
        assert_eq!(kept, vec![1001]);

        // Older-first: both accepted (the older one is later removed by the post-merge trim, but is
        // still reported as accepted, not TooOld).
        let series = mk();
        let batch = normalize_batch(&series, &[s(0, 0.0), s(1001, 0.0)], None);
        assert!(!matches!(batch.results[0], SampleAddResult::TooOld));
        let mut kept: Vec<i64> = batch.to_insert.iter().map(|x| x.timestamp).collect();
        kept.sort();
        assert_eq!(kept, vec![0, 1001]);
    }

    /// The single sort of an unsorted batch gives both the repeats (every occurrence after the
    /// first in input order) and the merge order, with each survivor still at its input index.
    #[test]
    fn normalize_unsorted_batch_sorts_once_for_repeats_and_order() {
        let series = TimeSeries::default();
        let samples = [
            s(30, 1.0),
            s(10, 2.0),
            s(30, 3.0),
            s(20, 4.0),
            s(10, 5.0),
            s(5, 6.0),
        ];
        let batch = normalize_batch(&series, &samples, None);
        assert!(batch.has_repeats);
        let kept: Vec<(i64, f64)> = batch
            .to_insert
            .iter()
            .map(|x| (x.timestamp, x.value))
            .collect();
        assert_eq!(kept, vec![(5, 6.0), (10, 2.0), (20, 4.0), (30, 1.0)]);
        assert_eq!(batch.insert_index, vec![5, 1, 3, 0]);
        for repeat in [2, 4] {
            assert!(matches!(batch.results[repeat], SampleAddResult::Duplicate));
        }

        let batch = normalize_batch(&series, &[s(3, 0.0), s(1, 0.0), s(2, 0.0)], None);
        assert!(!batch.has_repeats);
        assert_eq!(batch.insert_index, vec![1, 2, 0]);

        // Sorted input takes the no-allocation path and reports repeats the same way.
        let batch = normalize_batch(&series, &[s(1, 0.0), s(2, 0.0), s(2, 1.0)], None);
        assert!(batch.has_repeats);
        assert_eq!(batch.insert_index, vec![0, 1]);
        let batch = normalize_batch(&series, &[s(1, 0.0), s(2, 0.0)], None);
        assert!(!batch.has_repeats);
    }

    #[test]
    fn group_splits_samples_across_chunks_when_ranges_differ() {
        let ctx = Context::dummy();
        let mut series = TimeSeries::default();

        // Seed series with enough samples to create multiple chunks.
        let seed: Vec<Sample> = (0..10_000).map(|i| s(i, i as f64)).collect();
        bulk_insert_samples(&ctx, &mut series, &seed, None);

        assert!(series.chunks.len() >= 2);

        // Disable retention to ensure samples are not filtered.
        series.retention = Duration::ZERO;

        // Find two adjacent chunks where we can pick timestamps that are exclusive to each chunk.
        // This avoids relying on boundary timestamps and avoids assumptions about overlapping ranges.
        let mut chosen: Option<(i64, i64)> = None;

        for pair_start in (0..series.chunks.len() - 1).rev() {
            let c0 = &series.chunks[pair_start];
            let c1 = &series.chunks[pair_start + 1];

            let c0_first = c0.first_timestamp();
            let c0_last = c0.last_timestamp();
            let c1_first = c1.first_timestamp();
            let c1_last = c1.last_timestamp();

            let c0_mid = c0_first.saturating_add((c0_last.saturating_sub(c0_first)) / 2);
            let c1_mid = c1_first.saturating_add((c1_last.saturating_sub(c1_first)) / 2);

            let c0_candidates = [c0_first, c0_mid, c0_last];
            let c1_candidates = [c1_first, c1_mid, c1_last];

            let ts0_opt = c0_candidates
                .into_iter()
                .find(|&ts| c0.is_timestamp_in_range(ts) && !c1.is_timestamp_in_range(ts));

            let ts1_opt = c1_candidates
                .into_iter()
                .find(|&ts| c1.is_timestamp_in_range(ts) && !c0.is_timestamp_in_range(ts));

            if let (Some(ts0), Some(ts1)) = (ts0_opt, ts1_opt) {
                // Ensure sorted input for the grouping function contract.
                let (a, b) = if ts0 <= ts1 { (ts0, ts1) } else { (ts1, ts0) };
                chosen = Some((a, b));
                break;
            }
        }

        let (ts0, ts1) = chosen.expect(
            "Could not find adjacent chunks with non-overlapping (exclusive) timestamp membership; \
                 chunk range semantics may overlap. Adjust the test to assert weaker properties.",
        );

        let samples = vec![s(ts0, 123.0), s(ts1, 456.0)];
        let groups = group_samples_by_chunk(&series, &samples);

        // Now it's valid to expect two groups: we've ensured the timestamps are exclusive.
        assert_eq!(groups.len(), 2);

        // Verify each group borrows a contiguous sub-slice of the input.
        let total_grouped: usize = groups.iter().map(|g| g.samples.len()).sum();
        assert_eq!(total_grouped, samples.len());
        assert_eq!(groups[0].samples, &samples[0..1]);
        assert_eq!(groups[1].samples, &samples[1..2]);
    }

    #[test]
    fn group_pins_samples_newer_than_last_chunk_to_last_chunk() {
        let ctx = Context::dummy();
        let mut series = TimeSeries::default();

        // Seed series so it has at least one chunk.
        let seed: Vec<Sample> = (0..1_000).map(|i| s(i * 10, i as f64)).collect();
        bulk_insert_samples(&ctx, &mut series, &seed, None);

        // Pick timestamps that are beyond the last chunk's max timestamp.
        let last_max = series.chunks.last().unwrap().last_timestamp();
        let samples = vec![s(last_max + 1, 1.0), s(last_max + 2, 2.0)];
        let groups = group_samples_by_chunk(&series, &samples);

        assert_eq!(groups.len(), 1);

        assert_eq!(groups[0].samples.len(), 2);
        assert_eq!(groups[0].samples[0].timestamp, last_max + 1);
        assert_eq!(groups[0].samples[1].timestamp, last_max + 2);
    }

    fn collect_grouped_timestamps(groups: &[ChunkSampleGroup<'_>]) -> Vec<i64> {
        groups
            .iter()
            .flat_map(|g| g.samples.iter().map(|x| x.timestamp))
            .collect()
    }

    #[test]
    fn group_requires_sorted_input_preserves_contiguity_and_order() {
        let ctx = Context::dummy();
        let mut series = TimeSeries::default();

        // Seed to avoid an empty-series special case.
        let seed: Vec<Sample> = (0..2_000).map(|i| s(i * 10, i as f64)).collect();
        bulk_insert_samples(&ctx, &mut series, &seed, None);

        // Carefully sorted input.
        let samples = vec![s(5, 1.0), s(15, 2.0), s(25, 3.0), s(35, 4.0)];
        let groups = group_samples_by_chunk(&series, &samples);

        let grouped = collect_grouped_timestamps(&groups);
        assert_eq!(grouped, vec![5, 15, 25, 35]);

        // Each group must borrow a contiguous sub-slice from `samples` (no reordering).
        let mut cursor = 0usize;
        for g in groups.iter() {
            assert!(!g.samples.is_empty());
            let len = g.samples.len();
            assert_eq!(&samples[cursor..cursor + len], g.samples);
            cursor += len;
        }
        assert_eq!(cursor, samples.len());
    }

    #[test]
    fn normalize_filters_all_samples_when_all_are_older_than_retention() {
        let ctx = Context::dummy();
        let mut series = TimeSeries::default();

        // Create baseline data to establish a retention window.
        bulk_insert_samples(&ctx, &mut series, &[s(100_000, 1.0)], None);
        series.retention = Duration::from_millis(1_000);
        let min_allowed = get_min_allowed_timestamp(&series);

        let samples = vec![s(min_allowed - 100, 1.0), s(min_allowed - 1, 2.0)];
        let batch = normalize_batch(&series, &samples, None);

        assert!(batch.to_insert.is_empty());
        assert!(
            batch
                .results
                .iter()
                .all(|r| matches!(r, SampleAddResult::TooOld))
        );
    }

    #[test]
    fn bulk_insert_appends_into_last_chunk_when_it_has_capacity() {
        let ctx = Context::dummy();
        let mut series = TimeSeries::default();

        // Small seed: the last chunk has plenty of remaining capacity.
        let seed: Vec<Sample> = (0..100).map(|i| s(i * 10, i as f64)).collect();
        bulk_insert_samples(&ctx, &mut series, &seed, None);
        let chunks_before = series.chunks.len();
        let last_max = series.last_timestamp();

        // A small append batch must reuse the last chunk instead of creating a new one.
        let samples = vec![s(last_max + 10, 1.0), s(last_max + 20, 2.0)];
        let results = bulk_insert_samples(&ctx, &mut series, &samples, None);

        assert!(results.iter().all(|r| r.is_ok()));
        assert_eq!(series.chunks.len(), chunks_before);
        assert_eq!(series.last_timestamp(), last_max + 20);
        assert_eq!(series.total_samples, seed.len() + samples.len());
    }

    #[test]
    fn bulk_insert_gap_samples_do_not_create_overlapping_chunks() {
        let ctx = Context::dummy();
        let mut series = TimeSeries::default();

        // Seed enough data to create multiple chunks.
        let seed: Vec<Sample> = (0..10_000).map(|i| s(i * 10, i as f64)).collect();
        bulk_insert_samples(&ctx, &mut series, &seed, None);
        assert!(series.chunks.len() >= 2);
        series.retention = Duration::ZERO;

        // One sample in the "gap" just before a chunk's first timestamp, one inside
        // that same chunk. Both must be routed into the existing chunk rather than
        // spawning a new chunk whose range overlaps it.
        let c1_first = series.chunks[1].first_timestamp();
        let samples = vec![s(c1_first - 5, 123.0), s(c1_first + 5, 456.0)];
        let results = bulk_insert_samples(&ctx, &mut series, &samples, None);
        assert!(results.iter().all(|r| r.is_ok()));

        // Chunks must remain sorted and strictly non-overlapping.
        for pair in series.chunks.windows(2) {
            assert!(
                pair[0].last_timestamp() < pair[1].first_timestamp(),
                "chunks overlap: {}..{} vs {}..{}",
                pair[0].first_timestamp(),
                pair[0].last_timestamp(),
                pair[1].first_timestamp(),
                pair[1].last_timestamp()
            );
        }

        let stored: Vec<i64> = series.iter().map(|smpl| smpl.timestamp).collect();
        assert!(stored.contains(&(c1_first - 5)));
        assert!(stored.contains(&(c1_first + 5)));
    }

    #[test]
    fn bulk_insert_stores_samples_correctly_in_series() {
        let ctx = Context::dummy();
        let mut series = TimeSeries::default();

        let samples = generate_random_samples(120);

        let results = bulk_insert_samples(&ctx, &mut series, &samples, None);

        // All samples should have been accepted.
        assert_eq!(results.len(), samples.len());
        assert!(results.iter().all(|r| matches!(r, SampleAddResult::Ok(_))));

        // Series metadata should reflect stored samples.
        assert_eq!(series.total_samples, samples.len());
        assert_eq!(series.first_timestamp, samples[0].timestamp);
        assert_eq!(
            series.last_timestamp(),
            samples[samples.len() - 1].timestamp
        );

        // Stored samples should match exactly (timestamps and values) in order.
        let stored: Vec<Sample> = series.iter().collect();
        assert_eq!(stored.len(), samples.len());
        assert_eq!(stored, samples);
    }

    #[test]
    fn bulk_insert_large_batch_creates_multiple_chunks() {
        let ctx = Context::dummy();
        let mut series = TimeSeries::default();

        let samples: Vec<_> = generate_random_samples(10_000);
        let results = bulk_insert_samples(&ctx, &mut series, &samples, None);

        assert_eq!(results.len(), 10_000);
        let errors: Vec<&SampleAddResult> = results.iter().filter(|r| !r.is_ok()).collect();

        assert!(
            errors.is_empty(),
            "Some samples failed to insert: {:?}",
            errors
        );
        assert!(results.iter().all(|r| matches!(r, SampleAddResult::Ok(_))));

        // make sure the series contains all samples
        assert_eq!(series.total_samples, samples.len());
        assert!(series.chunks.len() > 1);

        results.iter().zip(samples.iter()).for_each(|(r, s)| {
            if let SampleAddResult::Ok(sample) = r {
                assert_eq!(sample.timestamp, s.timestamp);
                assert_eq!(sample.value, s.value);
            } else {
                panic!("Expected all samples to be inserted successfully");
            }
        });

        for (sample, sample2) in series.iter().zip(samples.iter()) {
            assert_eq!(sample.timestamp, sample2.timestamp);
            assert_eq!(sample.value, sample2.value);
        }
    }

    #[test]
    fn bulk_insert_inserts_into_existing_chunks_and_creates_new_chunks_in_one_call() {
        let ctx = Context::dummy();
        let mut series = TimeSeries::default();

        // Seed enough data to ensure we have at least one existing chunk with a well-defined range.
        let seed: Vec<Sample> = (0..5_000).map(|i| s(i * 10, i as f64)).collect();
        bulk_insert_samples(&ctx, &mut series, &seed, None);

        // Make sure retention doesn't filter anything in this test.
        series.retention = Duration::ZERO;

        let chunks_before = series.chunks.len();
        assert!(chunks_before >= 1);

        // Pick a timestamp that lies inside an *existing* chunk, off the seed's 10 ms grid so
        // it is an insert rather than a duplicate (the default policy is BLOCK).
        let c0 = &series.chunks[0];
        let mid = c0
            .first_timestamp()
            .saturating_add((c0.last_timestamp().saturating_sub(c0.first_timestamp())) / 2);
        let existing_ts = mid / 10 * 10 + 5;
        assert!(c0.is_timestamp_in_range(existing_ts));

        // Append far more samples than the last chunk can absorb so the overflow is
        // forced into newly created chunk(s).
        let last_max = series.chunks.last().unwrap().last_timestamp();
        let mut samples = vec![s(existing_ts, 123.0)];
        samples.extend((1..=20_000).map(|i| s(last_max + i * 10, i as f64)));

        let results = bulk_insert_samples(&ctx, &mut series, &samples, None);

        // All inserts should succeed.
        assert_eq!(results.len(), samples.len());
        assert!(results.iter().all(|r| matches!(r, SampleAddResult::Ok(_))));

        // We should have inserted into an existing chunk AND created at least one new chunk.
        assert!(series.chunks.len() > chunks_before);

        // Validate the timestamps are present in the series after insertion.
        let stored_ts: Vec<i64> = series.iter().map(|smpl| smpl.timestamp).collect();
        assert!(stored_ts.contains(&existing_ts));
        assert!(stored_ts.contains(&(last_max + 10)));
        assert!(stored_ts.contains(&(last_max + 20_000 * 10)));

        // Metadata sanity: last timestamp must advance to the newest inserted sample.
        assert_eq!(series.last_timestamp(), last_max + 20_000 * 10);
    }

    #[test]
    fn bulk_insert_updates_first_last_timestamps() {
        let ctx = Context::dummy();
        let mut series = TimeSeries::default();

        let samples = vec![s(1000, 1.0), s(2000, 2.0), s(3000, 3.0)];
        bulk_insert_samples(&ctx, &mut series, &samples, None);

        assert_eq!(series.first_timestamp, 1000);
        assert_eq!(series.last_timestamp(), 3000);
    }

    // Consistency with single-sample `TimeSeries::add`:

    #[test]
    fn bulk_insert_applies_value_rounding() {
        use crate::common::rounding::RoundingStrategy;

        let ctx = Context::dummy();
        let mut series = TimeSeries {
            rounding: Some(RoundingStrategy::DecimalDigits(2)),
            ..Default::default()
        };

        let samples = vec![s(1000, 1.23456), s(2000, 2.98765)];
        let results = bulk_insert_samples(&ctx, &mut series, &samples, None);

        // Results should carry the rounded values.
        match results[0] {
            SampleAddResult::Ok(sample) => assert_eq!(sample.value, 1.23),
            other => panic!("expected Ok, got {other:?}"),
        }
        match results[1] {
            SampleAddResult::Ok(sample) => assert_eq!(sample.value, 2.99),
            other => panic!("expected Ok, got {other:?}"),
        }

        // Stored values must be rounded too.
        let stored: Vec<Sample> = series.iter().collect();
        assert_eq!(stored, vec![s(1000, 1.23), s(2000, 2.99)]);
    }

    #[test]
    fn bulk_insert_reports_too_old_samples_in_results() {
        let ctx = Context::dummy();
        let mut series = TimeSeries::default();

        // Establish a retention window.
        bulk_insert_samples(&ctx, &mut series, &[s(100_000, 1.0)], None);
        series.retention = Duration::from_millis(1000);

        let min_allowed = get_min_allowed_timestamp(&series);
        let samples = vec![
            s(min_allowed - 10, 1.0), // too old
            s(min_allowed - 1, 2.0),  // too old
            s(min_allowed + 50, 3.0), // accepted
        ];

        let results = bulk_insert_samples(&ctx, &mut series, &samples, None);

        // One result per input sample, with TooOld reported (not silently dropped).
        assert_eq!(results.len(), samples.len());
        assert!(matches!(results[0], SampleAddResult::TooOld));
        assert!(matches!(results[1], SampleAddResult::TooOld));
        assert!(results[2].is_ok());
    }

    #[test]
    fn bulk_insert_honors_ignore_filter() {
        let ctx = Context::dummy();

        // Build two identical series; add the same batch via bulk and via single-add.
        let make_series = || {
            let mut series = TimeSeries::default();
            series.sample_duplicates.policy = Some(DuplicatePolicy::KeepLast);
            series.sample_duplicates.max_time_delta = 5;
            series.sample_duplicates.max_value_delta = 0.5;
            series
        };

        let samples = vec![
            s(1000, 10.0),
            s(1003, 10.2), // within both deltas of the last -> ignored
            s(1010, 20.0), // outside value delta -> accepted
        ];

        let mut bulk_series = make_series();
        let bulk_results = bulk_insert_samples(&ctx, &mut bulk_series, &samples, None);

        let mut single_series = make_series();
        let single_results: Vec<SampleAddResult> = samples
            .iter()
            .map(|smp| single_series.add(smp.timestamp, smp.value, None))
            .collect();

        // Bulk must match single-add: the near-duplicate is ignored in both.
        assert!(matches!(bulk_results[1], SampleAddResult::Ignored(1000)));
        assert!(matches!(single_results[1], SampleAddResult::Ignored(1000)));

        let bulk_stored: Vec<Sample> = bulk_series.iter().collect();
        let single_stored: Vec<Sample> = single_series.iter().collect();
        assert_eq!(bulk_stored, single_stored);
        assert_eq!(bulk_stored, vec![s(1000, 10.0), s(1010, 20.0)]);
    }

    #[test]
    fn bulk_insert_resolves_series_duplicate_policy_when_no_override() {
        let ctx = Context::dummy();
        let mut series = TimeSeries::default();
        // Configure KeepFirst so an exact-timestamp duplicate keeps the original value.
        series.sample_duplicates.policy = Some(DuplicatePolicy::KeepFirst);

        // Store the original sample, then re-add the same timestamp with a different value in a
        // second batch. With no override, the series policy (KeepFirst) — not the chunk merge
        // default of KeepLast — must decide the winner.
        bulk_insert_samples(&ctx, &mut series, &[s(1000, 1.0)], None);
        bulk_insert_samples(&ctx, &mut series, &[s(1000, 2.0)], None);

        let stored: Vec<Sample> = series.iter().collect();
        assert_eq!(stored, vec![s(1000, 1.0)]);
    }

    #[test]
    fn bulk_insert_disallows_duplicate_timestamps_within_batch() {
        let ctx = Context::dummy();
        let mut series = TimeSeries::default();

        // Three samples share timestamp 2000; only the first is kept, the rest are rejected.
        let samples = vec![
            s(1000, 1.0),
            s(2000, 2.0),
            s(2000, 3.0),
            s(2000, 4.0),
            s(3000, 5.0),
        ];

        let results = bulk_insert_samples(&ctx, &mut series, &samples, None);

        assert_eq!(results.len(), samples.len());
        assert!(results[0].is_ok());
        assert!(results[1].is_ok());
        assert!(matches!(results[2], SampleAddResult::Duplicate));
        assert!(matches!(results[3], SampleAddResult::Duplicate));
        assert!(results[4].is_ok());

        // Only the first sample for each timestamp is stored.
        let stored: Vec<Sample> = series.iter().collect();
        assert_eq!(stored, vec![s(1000, 1.0), s(2000, 2.0), s(3000, 5.0)]);
        assert_eq!(series.total_samples, 3);
    }

    // --- chunk sizing -------------------------------------------------------------------------
    //
    // Uncompressed series make the arithmetic exact: 16 bytes per sample, so a 4096-byte chunk
    // holds 256 samples.

    fn uncompressed_series() -> TimeSeries {
        TimeSeries {
            chunk_encoding: ChunkEncoding::Uncompressed,
            chunk_size_bytes: 4096,
            ..Default::default()
        }
    }

    fn run(start: i64, count: usize) -> Vec<Sample> {
        (0..count as i64).map(|i| s(start + i, i as f64)).collect()
    }

    fn group_shape(groups: &[ChunkSampleGroup<'_>]) -> Vec<(Option<usize>, usize)> {
        groups
            .iter()
            .map(|g| match g.target {
                GroupTarget::Existing(idx) => (Some(idx), g.samples.len()),
                GroupTarget::NewBefore | GroupTarget::Append => (None, g.samples.len()),
            })
            .collect()
    }

    fn chunk_lens(series: &TimeSeries) -> Vec<usize> {
        series.chunks.iter().map(|c| c.len()).collect()
    }

    #[test]
    fn appends_fill_new_chunks_and_leave_the_remainder_to_the_last() {
        let ctx = Context::dummy();
        let mut series = uncompressed_series();
        let samples = run(0, 600);
        // One run, filled exactly at merge time rather than planned.
        let groups = group_samples_by_chunk(&series, &samples);
        assert_eq!(group_shape(&groups), vec![(None, 600)]);
        assert!(groups[0].target == GroupTarget::Append);

        bulk_insert_samples(&ctx, &mut series, &samples, None);
        assert_eq!(chunk_lens(&series), vec![256, 256, 88]);
        assert_eq!(series.iter().collect::<Vec<_>>(), samples);
    }

    #[test]
    fn tail_is_filled_to_chunk_size_before_opening_a_chunk() {
        let ctx = Context::dummy();
        let mut series = uncompressed_series();
        bulk_insert_samples(&ctx, &mut series, &run(0, 250), None);
        assert_eq!(series.chunks.len(), 1);

        // 96 free bytes = 6 samples; the other 94 open a new chunk.
        bulk_insert_samples(&ctx, &mut series, &run(1_000, 100), None);
        assert_eq!(chunk_lens(&series), vec![256, 94]);
        assert_eq!(series.total_samples, 350);
    }

    #[test]
    fn small_append_to_a_roomy_tail_stays_in_the_tail() {
        let ctx = Context::dummy();
        let mut series = uncompressed_series();
        bulk_insert_samples(&ctx, &mut series, &run(0, 10), None);

        bulk_insert_samples(&ctx, &mut series, &run(1_000, 3), None);
        assert_eq!(chunk_lens(&series), vec![13]);
    }

    /// Batched appends fill compressed chunks exactly as single adds do: every chunk but the
    /// last ends full and at or under `max_size`, so its sealed allocation stays in the
    /// allocator size class of `max_size` (an estimate-planned fill ended past it about half
    /// the time, in jemalloc's 5,120-byte class for a 4,096-byte chunk).
    #[test]
    fn batched_appends_seal_compressed_chunks_within_max_size() {
        let ctx = Context::dummy();
        for encoding in [ChunkEncoding::Chimp, ChunkEncoding::Gorilla] {
            let samples = DataGenerator::builder()
                .start(1_700_000_000_000)
                .interval(Duration::from_millis(1000))
                .algorithm(ValueWorkload::Drift)
                .samples(20_000)
                .seed(5)
                .build()
                .generate();
            let new_series = || TimeSeries {
                chunk_encoding: encoding,
                chunk_size_bytes: 4096,
                ..Default::default()
            };
            let mut batched = new_series();
            for batch in samples.chunks(37) {
                let results = bulk_insert_samples(&ctx, &mut batched, batch, None);
                assert!(results.iter().all(|r| r.is_ok()), "{encoding:?}");
            }
            let mut whole = new_series();
            bulk_insert_samples(&ctx, &mut whole, &samples, None);
            let mut single = new_series();
            for sample in &samples {
                assert!(single.add(sample.timestamp, sample.value, None).is_ok());
            }

            for (what, series) in [
                ("batched", &batched),
                ("whole", &whole),
                ("single", &single),
            ] {
                assert!(series.chunks.len() > 4, "{encoding:?} {what}");
                let (last, sealed) = series.chunks.split_last().unwrap();
                for chunk in sealed {
                    assert!(chunk.is_full(), "{encoding:?} {what}: sealed short of full");
                    assert!(
                        chunk.size() <= 4096,
                        "{encoding:?} {what}: sealed at {} bytes",
                        chunk.size()
                    );
                    assert!(
                        chunk.memory_usage() - size_of::<TimeSeriesChunk>() <= 4096,
                        "{encoding:?} {what}: sealed allocation past 4096"
                    );
                }
                assert!(
                    last.size() <= 4096,
                    "{encoding:?} {what}: tail at {}",
                    last.size()
                );
                assert_eq!(series.total_samples, samples.len(), "{encoding:?} {what}");
            }
            // The same rule everywhere: the three paths cut chunks at the same samples.
            assert_eq!(chunk_lens(&batched), chunk_lens(&single), "{encoding:?}");
            assert_eq!(chunk_lens(&whole), chunk_lens(&single), "{encoding:?}");
        }
    }

    /// Backfill lengths for `n` samples older than a 10-sample uncompressed series (256
    /// samples per chunk): the series' own chunk is left out.
    fn backfill_lens(n: usize) -> Vec<usize> {
        let ctx = Context::dummy();
        let mut series = uncompressed_series();
        bulk_insert_samples(&ctx, &mut series, &run(1_000_000, 10), None);
        let samples = run(0, n);
        let results = bulk_insert_samples(&ctx, &mut series, &samples, None);
        assert!(results.iter().all(|r| r.is_ok()), "{n}");
        assert_eq!(series.total_samples, n + 10, "{n}");
        let mut expected = samples;
        expected.extend(run(1_000_000, 10));
        assert_eq!(series.iter().collect::<Vec<_>>(), expected, "{n}");
        let mut lens = chunk_lens(&series);
        assert_eq!(lens.pop(), Some(10), "{n}");
        lens
    }

    /// `append_until_full` stops exactly where `is_full` would have stopped single adds, and
    /// appends nothing to a chunk that is already full: under `HEADROOM_MIN_CHUNK_SIZE`, where
    /// a chunk is full at `max_size`, and at or above it, where it keeps a sample of headroom.
    #[test]
    fn append_until_full_stops_at_is_full() {
        let samples = run(0, 10_000);
        for max_size in [96, 4096] {
            for encoding in [
                ChunkEncoding::Uncompressed,
                ChunkEncoding::Chimp,
                ChunkEncoding::Gorilla,
            ] {
                let what = format!("{encoding:?} {max_size}");
                let mut chunk = TimeSeriesChunk::new(encoding, max_size);
                let (appended, error) = chunk.append_until_full(&samples);
                assert!(error.is_none(), "{what}");
                assert!(appended > 0 && appended < samples.len(), "{what}");
                assert_eq!(chunk.len(), appended, "{what}");
                assert!(chunk.is_full(), "{what}");

                let mut single = TimeSeriesChunk::new(encoding, max_size);
                for sample in &samples {
                    if single.is_full() {
                        break;
                    }
                    single.add_sample(sample).unwrap();
                }
                assert_eq!(single.len(), appended, "{what}");

                let (more, error) = chunk.append_until_full(&samples[appended..]);
                assert_eq!(more, 0, "{what}");
                assert!(error.is_none(), "{what}");
            }
        }
    }

    #[test]
    fn backfill_fills_chunks_exactly() {
        // One group, filled at merge time rather than planned.
        let mut series = uncompressed_series();
        bulk_insert_samples(&Context::dummy(), &mut series, &run(1_000_000, 10), None);
        let samples = run(0, 600);
        let groups = group_samples_by_chunk(&series, &samples);
        assert_eq!(group_shape(&groups), vec![(None, 600)]);
        assert!(groups[0].target == GroupTarget::NewBefore);

        // Every chunk but the last is full; the last keeps the remainder, however small
        // (the estimate-planned fill made 257 one chunk over budget, 1,030 four of 257-258).
        assert_eq!(backfill_lens(100), vec![100]);
        assert_eq!(backfill_lens(512), vec![256, 256]);
        assert_eq!(backfill_lens(257), vec![256, 1]);
        assert_eq!(backfill_lens(600), vec![256, 256, 88]);
        assert_eq!(backfill_lens(1_030), vec![256, 256, 256, 256, 6]);
    }

    /// Backfilled compressed chunks seal within `max_size`, so their allocation stays in the
    /// allocator size class of `max_size` (an estimate-planned fill with an eighth of overfill
    /// put them in jemalloc's 5,120-byte class), and every chunk of a run but its last is full.
    #[test]
    fn backfill_seals_compressed_chunks_within_max_size() {
        let ctx = Context::dummy();
        let max = 4096;
        for encoding in [ChunkEncoding::Chimp, ChunkEncoding::Gorilla] {
            for workload in [ValueWorkload::Drift, ValueWorkload::Constant] {
                let samples = DataGenerator::builder()
                    .start(1_700_000_000_000)
                    .interval(Duration::from_millis(1000))
                    .algorithm(workload)
                    .samples(20_000)
                    .seed(11)
                    .build()
                    .generate();
                let (older, newest) = samples.split_at(samples.len() - 1);
                // Older history loaded newest batch first, and all at once.
                for batch in [1_000, older.len()] {
                    let what = format!("{encoding:?} {workload:?} batch {batch}");
                    let mut series = TimeSeries {
                        chunk_encoding: encoding,
                        chunk_size_bytes: max,
                        ..Default::default()
                    };
                    bulk_insert_samples(&ctx, &mut series, newest, None);
                    for part in older.rchunks(batch) {
                        let results = bulk_insert_samples(&ctx, &mut series, part, None);
                        assert!(results.iter().all(|r| r.is_ok()), "{what}");
                    }
                    assert_eq!(series.total_samples, samples.len(), "{what}");
                    assert_eq!(series.iter().collect::<Vec<_>>(), samples, "{what}");

                    let (_, backfilled) = series.chunks.split_last().unwrap();
                    assert!(backfilled.len() > 1, "{what}");
                    let last = backfilled.len() - 1;
                    for (i, chunk) in backfilled.iter().enumerate() {
                        let size = chunk.size();
                        assert!(size <= max, "{what}: chunk {i} sealed at {size} bytes");
                        assert!(
                            chunk.memory_usage() - size_of::<TimeSeriesChunk>() <= max,
                            "{what}: chunk {i} allocated past {max}"
                        );
                        // In one shot, one run: full up to its last chunk.
                        if batch == older.len() && i < last {
                            assert!(chunk.is_full(), "{what}: chunk {i} holds {size} bytes");
                        }
                    }
                }
            }
        }
    }

    /// A series without samples can hold one empty chunk (a load keeps a lone one). It reports
    /// `first_timestamp() == 0`, so kept ahead of the new chunks it pinned the series'
    /// `first_timestamp` at 0, and left an empty chunk among non-empty ones.
    #[test]
    fn bulk_insert_drops_the_lone_empty_chunk_of_an_empty_series() {
        let ctx = Context::dummy();
        for encoding in [ChunkEncoding::Chimp, ChunkEncoding::Uncompressed] {
            let mut series = TimeSeries {
                chunk_encoding: encoding,
                chunk_size_bytes: 4096,
                ..Default::default()
            };
            series.chunks.push(TimeSeriesChunk::new(encoding, 4096));

            let samples = run(1_000, 600);
            let results = bulk_insert_samples(&ctx, &mut series, &samples, None);

            assert!(results.iter().all(|r| r.is_ok()), "{encoding:?}");
            assert!(series.chunks.iter().all(|c| !c.is_empty()), "{encoding:?}");
            assert_eq!(series.first_timestamp, 1_000, "{encoding:?}");
            assert_eq!(series.total_samples, samples.len(), "{encoding:?}");
            assert_eq!(series.iter().collect::<Vec<_>>(), samples, "{encoding:?}");
        }
    }

    /// One batch that opens chunks before the series, upserts into an existing chunk and
    /// overflows the tail into new chunks, given in descending order: every result must land
    /// at its own input index.
    #[test]
    fn results_follow_input_order_across_backfill_existing_and_append_groups() {
        let ctx = Context::dummy();
        let mut series = uncompressed_series();
        // One chunk of 250 samples at 10_000..10_250: room for 6 more.
        let seed = run(10_000, 250);
        bulk_insert_samples(&ctx, &mut series, &seed, None);
        assert_eq!(series.chunks.len(), 1);

        let backfill = (0..300).map(|i| s(i, 1_000.0 + i as f64));
        let upserts = (0..10).map(|i| s(10_000 + i * 20, -(i as f64)));
        let appends = (0..300).map(|i| s(20_000 + i, 5_000.0 + i as f64));
        let mut samples: Vec<Sample> = backfill.chain(upserts).chain(appends).collect();
        samples.reverse();

        let results =
            bulk_insert_samples(&ctx, &mut series, &samples, Some(DuplicatePolicy::KeepLast));

        assert_eq!(results.len(), samples.len());
        for (i, (result, sample)) in results.iter().zip(&samples).enumerate() {
            match result {
                SampleAddResult::Ok(stored) => assert_eq!(stored, sample, "input {i}"),
                other => panic!("input {i} ({sample:?}): expected Ok, got {other:?}"),
            }
        }

        // Backfill: 300 samples as 256 + 44. Tail: 6 appends, then 256 + 38 new.
        assert_eq!(series.chunks.len(), 5);
        assert_eq!(series.total_samples, seed.len() + 300 + 300);
        assert_eq!(series.first_timestamp, 0);
        assert_eq!(series.last_timestamp(), 20_299);

        let mut expected: Vec<Sample> = seed
            .iter()
            .map(|smp| {
                let upserted = smp.timestamp < 10_200 && (smp.timestamp - 10_000) % 20 == 0;
                if upserted {
                    s(smp.timestamp, -(((smp.timestamp - 10_000) / 20) as f64))
                } else {
                    *smp
                }
            })
            .collect();
        expected.extend(
            samples
                .iter()
                .filter(|smp| !(10_000..20_000).contains(&smp.timestamp)),
        );
        expected.sort_by_key(|smp| smp.timestamp);
        assert_eq!(series.iter().collect::<Vec<_>>(), expected);
    }

    /// Upserts spread over many existing chunks, enough work for the parallel merge path.
    #[test]
    fn parallel_upserts_into_existing_chunks_keep_results_and_counts() {
        let ctx = Context::dummy();
        let mut series = uncompressed_series();
        let seed = run(0, 20_000);
        bulk_insert_samples(&ctx, &mut series, &seed, None);
        let chunks_before = series.chunks.len();
        assert!(chunks_before > 2);

        let upserts: Vec<Sample> = (0..5_000).map(|i| s(i * 4, -(i as f64))).collect();
        let work = upserts.len();
        assert!(
            request_par_threads(2, work) != 1,
            "batch must take the parallel path"
        );

        let results =
            bulk_insert_samples(&ctx, &mut series, &upserts, Some(DuplicatePolicy::KeepLast));

        for (i, (result, sample)) in results.iter().zip(&upserts).enumerate() {
            assert!(
                matches!(result, SampleAddResult::Ok(stored) if stored == sample),
                "input {i}: {result:?}"
            );
        }
        assert_eq!(series.chunks.len(), chunks_before);
        assert_eq!(series.total_samples, seed.len());
        let stored: Vec<Sample> = series.iter().collect();
        for upsert in &upserts {
            assert_eq!(stored[upsert.timestamp as usize], *upsert);
        }
    }

    #[test]
    fn bulk_loads_keep_compressed_chunks_near_chunk_size() {
        use crate::tests::generators::{
            DatasetKey, TimestampModel, ValueWorkload, dataset_seed, generate_dataset,
        };

        let ctx = Context::dummy();
        let count = 20_000;
        for workload in [
            ValueWorkload::Constant,
            ValueWorkload::Counter,
            ValueWorkload::Drift,
            ValueWorkload::Noisy,
        ] {
            let key = DatasetKey::new(workload, TimestampModel::Regular);
            let data = generate_dataset(key, count, dataset_seed(key));
            for encoding in [ChunkEncoding::Chimp, ChunkEncoding::Gorilla] {
                for batch in [count, 1_000, 100] {
                    let mut series = TimeSeries {
                        chunk_encoding: encoding,
                        chunk_size_bytes: 4096,
                        ..Default::default()
                    };
                    for part in data.chunks(batch) {
                        bulk_insert_samples(&ctx, &mut series, part, None);
                    }
                    assert_eq!(series.total_samples, count);

                    // The last chunk is the append target and may be partly filled.
                    let max = series.chunk_size_bytes;
                    let sealed = &series.chunks[..series.chunks.len() - 1];
                    for (i, chunk) in sealed.iter().enumerate() {
                        let size = chunk.size();
                        assert!(
                            size >= max / 2 && size <= max + max / SPLIT_SLACK_DIVISOR,
                            "{} {encoding:?} batch {batch}: chunk {i} holds {size} of {max} bytes",
                            key.id(),
                        );
                    }
                }
            }
        }
    }
}
