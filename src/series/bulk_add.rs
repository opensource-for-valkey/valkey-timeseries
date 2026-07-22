//! Ingest samples at ludicrous speed.
//!
//! This module provides bulk insertion of samples into a time series, with support for duplicate
//! policies and automatic compaction handling. It is optimized for high-throughput data ingestion
//! scenarios by leveraging parallel processing and efficient sample merging.
use crate::common::{Sample, Timestamp};
use crate::error_consts;
use crate::series::chunks::{ChunkOps, TimeSeriesChunk};
use crate::series::index::with_timeseries_postings;
use crate::series::ingest_normalize::{NormalizedBatch, normalize_batch};
use crate::series::{DuplicatePolicy, SampleAddResult, SeriesRef, TimeSeries};
use orx_parallel::{IterIntoParIter, ParIter, ParallelizableCollection};
use simd_json::base::{ValueAsArray, ValueAsScalar};
use simd_json::borrowed::Value;
use simd_json::prelude::ValueObjectAccess;
use valkey_module::{Context, NotifyEvent, ValkeyError, ValkeyResult};

pub const MAX_SAMPLES_PER_INSERT: usize = 1_000;
const COMPRESSION_RATIO_CONSERVATIVE: f64 = 2.0;
const EARLY_CHUNK_CAPACITY_FACTOR: f64 = 0.7;
const EARLY_CHUNK_SAMPLE_THRESHOLD: usize = 10;

#[derive(Debug)]
pub struct IngestedSamples {
    pub key: String,
    pub samples: Vec<Sample>,
}

impl IngestedSamples {
    pub fn from_json_lines(input: &mut [u8]) -> ValkeyResult<Self> {
        let v: Value = simd_json::to_borrowed_value(input)?;

        let values_arr = v
            .get("values")
            .and_then(|v| v.as_array())
            .ok_or(ValkeyError::Str("TSDB: missing values"))?;

        let timestamps_arr = v
            .get("timestamps")
            .and_then(|t| t.as_array())
            .ok_or(ValkeyError::Str("TSDB: missing timestamps"))?;

        if timestamps_arr.is_empty() || values_arr.is_empty() {
            return Err(ValkeyError::Str("TSDB: no timestamps or values"));
        }

        if timestamps_arr.len() != values_arr.len() {
            return Err(ValkeyError::Str(
                "TSDB: timestamps and values length mismatch",
            ));
        }

        if values_arr.len() > MAX_SAMPLES_PER_INSERT {
            return Err(ValkeyError::Str(error_consts::TOO_MANY_SAMPLES));
        }

        let mut samples = Vec::with_capacity(values_arr.len());
        for (val, ts) in values_arr.iter().zip(timestamps_arr.iter()) {
            let value = val
                .cast_f64()
                .ok_or(ValkeyError::Str("TSDB: invalid value (expected number)"))?;

            let timestamp_u64 = ts
                .as_u64()
                .ok_or(ValkeyError::Str("TSDB: invalid timestamp (expected u64)"))?;

            samples.push(Sample {
                timestamp: timestamp_u64 as i64,
                value,
            });
        }

        samples.sort_by_key(|s| s.timestamp);

        Ok(IngestedSamples {
            key: String::new(),
            samples,
        })
    }
}

/// A holder for either an existing chunk reference or new chunk info.
#[derive(Debug, Copy, Clone)]
enum ChunkHolder {
    ExistingIdx(usize),
    New,
}

/// A grouped view of a contiguous sub-slice of `samples` that all map to the same destination chunk.
#[derive(Debug)]
struct ChunkSampleGroup<'a> {
    /// The chunk to insert samples into. None indicates a new chunk needs to be created.
    pub chunk: ChunkHolder,
    pub samples: &'a [Sample],
}

#[inline]
fn exec_merge(
    chunk: &mut TimeSeriesChunk,
    samples: &[Sample],
    policy: Option<DuplicatePolicy>,
) -> Vec<SampleAddResult> {
    chunk.merge_samples(samples, policy).unwrap_or_else(|_| {
        let err = SampleAddResult::Error(error_consts::CANNOT_ADD_SAMPLE);
        std::iter::repeat_n(err, samples.len()).collect()
    })
}

#[inline]
fn calculate_capacity(chunk: &TimeSeriesChunk) -> usize {
    let mut capacity = chunk.estimate_remaining_sample_capacity();

    if capacity > 0 && chunk.len() < EARLY_CHUNK_SAMPLE_THRESHOLD && chunk.is_compressed() {
        capacity = (capacity as f64 * EARLY_CHUNK_CAPACITY_FACTOR).floor() as usize;
        capacity = capacity.max(1);
    }

    capacity
}

fn add_chunks_for_remaining_samples<'a>(
    dest: &mut Vec<ChunkSampleGroup<'a>>,
    series: &TimeSeries,
    samples: &'a [Sample],
) {
    if samples.is_empty() {
        return;
    }

    let is_compressed = series.is_compressed();
    // based on max chunk size and encoding, estimate capacity
    let chunk_size = series.chunk_size_bytes;
    // if we're compressed, estimate how many samples per chunk based on a conservative compression ratio of 2:1
    let sample_size = size_of::<Sample>();
    let sample_capacity = if is_compressed {
        chunk_size / (sample_size / 2)
    } else {
        // uncompressed, so each sample is 16 bytes (8 bytes timestamp + 8 bytes value)
        chunk_size / sample_size
    };

    // chunk the samples into new chunks
    for slice in samples.chunks(sample_capacity) {
        dest.push(ChunkSampleGroup {
            chunk: ChunkHolder::New,
            samples: slice,
        });
    }
}

/// Groups a sorted `samples` slice into the chunk they would belong to if inserted.
///
/// Rules:
/// - `samples` must be sorted by `timestamp` ascending, already normalized (retention
///   filtering happens in [`normalize_batch`], not here).
/// - Samples older than the first existing chunk go to new chunk(s), consumed strictly
///   below the first chunk's start so the new chunks cannot overlap existing ones.
/// - Each existing chunk consumes every remaining sample up to its last timestamp. This
///   also routes samples falling in the gap *before* a chunk into that chunk (as upserts),
///   so a new chunk is never created overlapping an existing one.
/// - Samples newer than the last chunk are appended to the last chunk while it has
///   (estimated) remaining capacity; overflow goes to new chunk(s).
/// - Returns groups in ascending chunk order, each group borrowing from the input slice.
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

    // Pre-allocate based on chunk count + potential new chunks
    let estimated_groups = series.chunks.len().saturating_add(2);
    let mut out: Vec<ChunkSampleGroup<'a>> = Vec::with_capacity(estimated_groups);

    // Empty series: create new chunks from all samples.
    if series.chunks.is_empty() || series.is_empty() {
        add_chunks_for_remaining_samples(&mut out, series, samples);
        return out;
    }

    let mut i = 0usize;

    // Samples older than the first existing chunk go to new chunk(s).
    let first_chunk_start = series.chunks[0].first_timestamp();
    if samples[i].timestamp < first_chunk_start {
        let start = i;
        while i < samples.len() && samples[i].timestamp < first_chunk_start {
            i += 1;
        }
        add_chunks_for_remaining_samples(&mut out, series, &samples[start..i]);

        if i >= samples.len() {
            return out;
        }
    }

    let last_index = series.chunks.len() - 1;

    for (chunk_idx, chunk) in series.chunks.iter().enumerate() {
        let start = i;

        let chunk_last = chunk.last_timestamp();
        while i < samples.len() && samples[i].timestamp <= chunk_last {
            i += 1;
        }

        // Beyond the last chunk: keep appending into it while it has estimated capacity.
        if chunk_idx == last_index && i < samples.len() {
            let take = calculate_capacity(chunk).min(samples.len() - i);
            i += take;
        }

        if start < i {
            out.push(ChunkSampleGroup {
                chunk: ChunkHolder::ExistingIdx(chunk_idx),
                samples: &samples[start..i],
            });
        }
    }

    // Whatever the last chunk couldn't absorb goes to new chunk(s).
    if i < samples.len() {
        add_chunks_for_remaining_samples(&mut out, series, &samples[i..]);
    }

    out
}

/// Produces disjoint `&mut` references for unique indices.
///
/// # Safety Requirements
/// Callers MUST ensure that `indices`:
/// - Are all within bounds of `slice`
/// - Are strictly unique (no duplicates)
/// - Are sorted in non-decreasing order
///
/// Violating these invariants leads to undefined behavior.
///
/// # Panics
/// Panics in debug builds if indices are not non-decreasing.
fn disjoint_get_many_mut_with_pos<'a, T>(slice: &'a mut [T], indices: &[usize]) -> Vec<&'a mut T> {
    let mut out: Vec<&'a mut T> = Vec::with_capacity(indices.len());
    let mut base = 0usize;
    let mut tail: &'a mut [T] = slice;

    for &idx in indices {
        debug_assert!(idx >= base, "indices must be non-decreasing after sort");
        let rel = idx - base;
        let (_left, rest) = tail.split_at_mut(rel);
        let (item, rest) = rest.split_at_mut(1);
        out.push(&mut item[0]);
        tail = rest;
        base = idx + 1;
    }

    out
}

/// Shared bulk-merge core used by both `TS.ADDBULK` (`bulk_insert_samples`) and `TS.MADD`
/// (`sample_merge::merge_samples`): normalize, group by destination chunk, merge in parallel,
/// splice results back to input order, and refresh series metadata.
///
/// Parallel merge implementation:
/// - existing-chunk groups: merge in parallel by taking disjoint `&mut` borrows
/// - new-chunk groups: create and merge in parallel, then append sequentially
///
/// Callers are responsible for post-merge maintenance (`split_chunks_if_needed`),
/// keyspace notification and compaction propagation.
///
/// `samples` **must** be sorted by timestamp ascending. Returns one result per input sample.
pub(super) fn merge_samples_into_series(
    series: &mut TimeSeries,
    samples: &[Sample],
    policy: Option<DuplicatePolicy>,
) -> Vec<SampleAddResult> {
    if samples.is_empty() {
        return Vec::new();
    }

    // Resolve the duplicate policy against the series/global defaults so that a `None` override
    // honors the series' configured policy rather than silently defaulting to KeepLast.
    let resolved_policy = Some(series.sample_duplicates.resolve_policy(policy));

    // Apply the same retention/rounding/IGNORE normalization as single-sample add.
    let NormalizedBatch {
        to_insert,
        insert_index,
        mut results,
    } = normalize_batch(series, samples, policy);

    if to_insert.is_empty() {
        return results;
    }

    let groups = group_samples_by_chunk(series, &to_insert);

    if groups.is_empty() {
        return results;
    }

    // separate groups into existing-chunk and new-chunk categories.
    let (new_groups, existing_groups): (Vec<_>, Vec<_>) = groups
        .iter()
        .enumerate()
        .partition(|(_, g)| matches!(g.chunk, ChunkHolder::New));

    let existing_groups: Vec<_> = existing_groups
        .into_iter()
        .filter_map(|(pos, g)| {
            if let ChunkHolder::ExistingIdx(idx) = g.chunk {
                Some((pos, idx, g.samples))
            } else {
                None
            }
        })
        .collect();

    let new_groups: Vec<_> = new_groups
        .into_iter()
        .map(|(pos, g)| (pos, g.samples))
        .collect();

    // Prepare result slots.
    let mut group_results: Vec<Option<Vec<SampleAddResult>>> = vec![None; groups.len()];

    // Merge into existing chunks in parallel.
    if !existing_groups.is_empty() {
        let chunk_indices: Vec<usize> = existing_groups.iter().map(|(_, idx, _)| *idx).collect();
        let chunk_refs =
            disjoint_get_many_mut_with_pos(series.chunks.as_mut_slice(), &chunk_indices);

        let existing_results: Vec<(usize, Vec<SampleAddResult>)> = chunk_refs
            .into_iter()
            .zip(existing_groups.iter())
            .iter_into_par()
            .map(|(chunk, &(group_pos, _, samples))| {
                let res = exec_merge(chunk, samples, resolved_policy);
                (group_pos, res)
            })
            .collect();

        for (group_pos, res) in existing_results {
            group_results[group_pos] = Some(res);
        }
    }

    // Process new-chunk groups in parallel (each creates its own chunk).
    if !new_groups.is_empty() {
        let encoding = series.chunk_encoding;
        let chunk_size = series.chunk_size_bytes;
        let new_results: Vec<(usize, TimeSeriesChunk, Vec<SampleAddResult>)> = new_groups
            .par()
            .map(|&(group_pos, samples)| {
                let mut chunk = TimeSeriesChunk::new(encoding, chunk_size);
                let res = exec_merge(&mut chunk, samples, resolved_policy);
                (group_pos, chunk, res)
            })
            .collect::<Vec<_>>();

        let chunk_count_before = series.chunks.len();

        for (group_pos, chunk, res) in new_results {
            group_results[group_pos] = Some(res);
            series.chunks.push(chunk);
        }

        // append new chunks to the series
        if series.chunks.len() != chunk_count_before {
            // Sort chunks by the first timestamp to maintain order.
            series.chunks.sort_by_key(|c| c.first_timestamp());
        }
    }

    // Refresh metadata unconditionally: even without new chunks, a merge can append to the
    // last chunk or update the value at the current last timestamp, and `total_samples` can
    // only be derived from chunk lengths (upserts of existing timestamps report `Ok` too).
    series.update_first_last_timestamps();
    series.recalculate_total_samples();

    // Merge results arrive in group order, which matches `to_insert` order (groups borrow
    // contiguous, ascending sub-slices of `to_insert`). Splice each outcome back into `results`
    // at its original input index so the returned vec has one entry per input sample.
    let mut cursor = 0usize;
    for opt in group_results.into_iter().flatten() {
        for res in opt {
            results[insert_index[cursor]] = res;
            cursor += 1;
        }
    }
    debug_assert_eq!(
        cursor,
        to_insert.len(),
        "merge produced a different number of results than samples inserted"
    );

    results
}

/// Bulk insert for `TS.ADDBULK`: runs the shared merge core, then performs post-merge
/// maintenance (chunk splitting), keyspace notification and compaction propagation.
pub fn bulk_insert_samples(
    ctx: &Context,
    series: &mut TimeSeries,
    samples: &[Sample],
    policy: Option<DuplicatePolicy>,
) -> Vec<SampleAddResult> {
    if samples.is_empty() {
        return Vec::new();
    }

    let _saved_sample_count = series.total_samples;
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
    if series.total_samples > _saved_sample_count {
        let event = if series.is_compaction() {
            "ts.add:dest"
        } else {
            "ts.add"
        };
        notify_added(ctx, event, &[series.id]);
    }

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
        // The pre-sort order is kept separately: the DIV-0023 forward-close marker is
        // input-order sensitive (see `last_forward_close_in_input_order`).
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

    results
}

fn notify_added(ctx: &Context, event: &str, ids: &[SeriesRef]) {
    with_timeseries_postings(ctx, |postings| {
        for &id in ids {
            let Some(key) = postings.get_key_by_id(id) else {
                ctx.log_warning("Compaction notification failed: series key not found");
                continue;
            };
            let key = ctx.create_string(key.as_ref());
            ctx.notify_keyspace_event(NotifyEvent::MODULE, event, &key);
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::series::ingest_normalize::get_min_allowed_timestamp;
    use crate::tests::generators::DataGenerator;
    use std::time::Duration;

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

        let parsed = IngestedSamples::from_json_lines(&mut data).unwrap();
        assert_eq!(parsed.samples.len(), 3);
        assert_eq!(parsed.samples[0].value, 1.0);
        assert_eq!(parsed.samples[0].timestamp, 1549891472010);
        assert_eq!(parsed.samples[2].timestamp, 1549891503438);
    }

    #[test]
    fn test_parse_mismatched_lengths() {
        let mut data = br#"{
            "metric": {"__name__": "test"},
            "values": [1, 2],
            "timestamps": [1000]
        }"#
        .to_vec();

        assert!(IngestedSamples::from_json_lines(&mut data).is_err());
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

        // Pick a timestamp that we know lies inside an *existing* chunk.
        let c0 = &series.chunks[0];
        let existing_ts = c0
            .first_timestamp()
            .saturating_add((c0.last_timestamp().saturating_sub(c0.first_timestamp())) / 2);
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
}
