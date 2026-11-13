use crate::common::context::key_for_display;
use crate::common::logging::log_warning;
use crate::common::threads::{ParMutRayon, request_par_threads};
use crate::common::{Sample, Timestamp};
use crate::series::bulk_add::merge_samples_into_series;
use crate::series::{DuplicatePolicy, SampleAddResult, TimeSeries};
use orx_parallel::Par;
use smallvec::{SmallVec, smallvec};
use valkey_module::Context;

pub struct IndexedSample {
    pub index: usize,
    pub timestamp: Timestamp,
    pub value: f64,
}

/// A collection of samples for a single timeseries, along with their original indices.
pub struct PerSeriesSamples<'a> {
    series: &'a mut TimeSeries,
    samples: SmallVec<[IndexedSample; 6]>,
    /// Samples accepted by the merge, ascending by timestamp; consumed by the post-merge
    /// compaction pass.
    added: SmallVec<[Sample; 8]>,
    /// The accepted timestamps in the order the caller supplied them (MADD argument order),
    /// which `added` discards by sorting. Batch compaction replays it to classify back-fills
    /// (see `CompactionOp::Ingest`).
    added_order: SmallVec<[Timestamp; 8]>,
    /// The series' last timestamp before the merge: batch compaction uses it to tell
    /// guaranteed-fresh appends apart from samples that may have replaced existing values.
    prev_last: Option<Timestamp>,
}

impl<'a> PerSeriesSamples<'a> {
    /// Creates a new `PerSeriesSamples` instance.
    pub fn new(series: &'a mut TimeSeries) -> Self {
        Self {
            series,
            samples: SmallVec::new(),
            added: SmallVec::new(),
            added_order: SmallVec::new(),
            prev_last: None,
        }
    }

    /// Adds a sample to the collection, along with its original index.
    pub fn add_sample(&mut self, sample: Sample, index: usize) {
        self.samples.push(IndexedSample {
            index,
            timestamp: sample.timestamp,
            value: sample.value,
        });
    }

    pub fn is_empty(&self) -> bool {
        self.samples.is_empty()
    }

    pub fn sort_by_timestamp(&mut self) {
        // Sort indexed samples by timestamp
        self.samples.sort_unstable_by_key(|s| s.timestamp);
    }

    pub fn sort_by_index(&mut self) {
        // Sort indexed samples by the original index
        self.samples.sort_unstable_by_key(|s| s.index);
    }
}

/// Merges a collection of samples into a time series.
///
/// Delegates to the shared bulk-merge core ([`merge_samples_into_series`]) used by
/// `TS.ADDBULK`, so normalization (retention/rounding/IGNORE), chunk grouping and
/// parallel merging behave identically on both ingest paths.
///
/// ## Note
///
/// `samples` is in argument order, which carries meaning: retention and IGNORE are decided
/// item by item in that order, as sequential adds would (see `normalize_batch`). Only the
/// samples handed on to the chunk merge are sorted.
///
/// ### Arguments
///
/// * `samples` - A slice of samples to merge into the time series
/// * `policy_override` - Optional override for the duplicate policy to use when merging
///
/// ### Returns
///
/// A vector of `SampleAddResult` with the outcome for each sample.
///
pub(super) fn merge_samples(
    series: &mut TimeSeries,
    samples: &[Sample],
    policy_override: Option<DuplicatePolicy>,
) -> Vec<SampleAddResult> {
    if samples.is_empty() {
        return Vec::new();
    }

    let results = merge_samples_into_series(series, samples, policy_override);
    // The samples are stored by now and a failed split only leaves a chunk oversized, so it is
    // logged rather than returned (as in `bulk_insert_samples`): an error would make TS.MADD
    // skip compaction, the retention trim and replication for samples the series keeps.
    if let Err(e) = series.split_chunks_if_needed() {
        let key = key_for_display(&series.key);
        log_warning(format!("TSDB: failed to split chunks for key '{key}': {e}"));
    }
    results
}

/// Sample-equivalents one appended sample is worth in the dispatch decision
/// (see `common::threads::PARALLEL_MIN_WORK`): with this weight a batch goes
/// parallel from about 256 samples across at least two series. Settled by a
/// paired A/B of TS.MADD at 16, 128 and 1,024 samples per command.
const MADD_SAMPLE_WORK: usize = 16;

/// Merges samples across multiple series, supporting parallel processing when applicable.
///
/// The merge phase may run on worker threads, but compaction propagation runs afterwards,
/// sequentially on the calling (command) thread: it acquires destination-series guards and
/// must not lock the global context from inside the parallel section (the command thread
/// already holds the GIL, so a `ThreadSafeContext` lock there deadlocks the server).
///
/// ### Parameters
/// - `groups`: A slice of series with their related samples.
/// - `ctx`: Command context; when present, compaction rules are propagated after the merge.
///
/// The retention trim is left to the caller, which must run `TimeSeries::apply_retention` on
/// every series afterwards (see the note at the end of the body).
///
/// ### Returns
/// A `SmallVec` of tuples (input index, SampleAddResult), one per sample.
///
///
pub fn multi_series_merge_samples(
    groups: Vec<PerSeriesSamples>,
    ctx: Option<&Context>,
) -> SmallVec<[(usize, SampleAddResult); 8]> {
    if groups.is_empty() {
        return smallvec![];
    }
    let mut groups = groups;

    // Dispatch to the pool only when the batch carries real work. An append costs
    // more than decoding a sample (normalisation, duplicate handling, chunk
    // encode), hence the weight; below the threshold a plain loop on the calling
    // thread avoids the pool round trip *and* orx's per-call runner construction.
    let total_samples: usize = groups.iter().map(|g| g.samples.len()).sum();
    let threads = request_par_threads(groups.len(), total_samples * MADD_SAMPLE_WORK);
    let res = if threads == 1 {
        let mut acc: SmallVec<[(usize, SampleAddResult); 8]> = SmallVec::new();
        for group in groups.iter_mut() {
            acc.extend(add_samples_internal(group));
        }
        acc
    } else {
        groups
            .par_mut_rayon()
            .num_threads(threads)
            .map(add_samples_internal)
            .reduce(|mut acc, item| {
                acc.extend(item);
                acc
            })
            .unwrap_or_default()
    };

    if let Some(ctx) = ctx {
        run_group_compactions(ctx, &mut groups);
    }

    // Retention is *not* applied here; the caller runs `TimeSeries::apply_retention` on each
    // series afterwards. The merge deliberately skips the eager trim so that
    // `add_group_sequentially`'s read-back still sees a sample which a later item in the same
    // batch pushed outside the window — that is what lets retention be decided ahead of the
    // duplicate policy (corpus: madd_retention_beats_duplicate_policy). The caller also reads
    // each series' pre-trim sample count first: the trim can drop more samples than the batch
    // added, which would hide the insert from the `TS.READ` wake-up.
    //
    // Deferring it does *not* leak expired samples into the destination buckets: compaction
    // aggregates through the retention-clamped `range_iter`, so a sample the trim is about to
    // evict is already invisible to the bucket recalculation.
    res
}

fn add_samples_internal(input: &mut PerSeriesSamples) -> SmallVec<[(usize, SampleAddResult); 8]> {
    input.prev_last = input.series.last_sample.map(|s| s.timestamp);

    if input.samples.len() == 1 {
        let sample = input.samples.pop().unwrap();
        let index = sample.index;
        let result = input
            .series
            .add_deferring_retention(sample.timestamp, sample.value, None);
        if let SampleAddResult::Ok(added) = result {
            input.added.push(added);
            input.added_order.push(added.timestamp);
        }

        return smallvec![(index, result)];
    }

    // In-batch duplicate timestamps: RTS applies each MADD item as an independent, sequential
    // upsert under the series' duplicate policy (SUM accumulates, LAST wins, FIRST/MIN/MAX fold,
    // BLOCK errors on the duplicate), so they must NOT be collapsed. The parallel bulk merge
    // assumes unique timestamps per batch, so fall back to sequential single-sample add() in
    // input order, which reproduces RTS exactly.
    if has_in_batch_duplicate(&input.samples) {
        return add_group_sequentially(input);
    }

    // Keep the samples in INPUT order: `normalize_batch`'s retention gate is input-order
    // sensitive (it mirrors sequential TS.MADD — an item is TooOld only if it is older than the
    // floor established by items at or before it). `merge_samples` returns results in the same
    // order as the samples slice, and normalizes/sorts internally for grouping, so unsorted input
    // is fully supported here.
    let samples: SmallVec<[Sample; 8]> = input
        .samples
        .iter()
        .map(|s| Sample::new(s.timestamp, s.value))
        .collect();

    let add_results = input
        .series
        .merge_samples_deferring_retention(&samples, None);

    // Accepted samples feed batch compaction, which requires them ascending by timestamp.
    // `add_results` is parallel to `samples`, i.e. still in input order, so the order is
    // captured before sorting (batch compaction replays it to classify back-fills).
    let mut added: SmallVec<[Sample; 8]> = add_results
        .iter()
        .filter_map(|res| {
            if let SampleAddResult::Ok(s) = res {
                Some(*s)
            } else {
                None
            }
        })
        .collect();
    input.added_order = added.iter().map(|s| s.timestamp).collect();
    added.sort_unstable_by_key(|s| s.timestamp);
    input.added = added;

    let mut result: SmallVec<[(usize, SampleAddResult); 8]> = SmallVec::new();
    for item in add_results
        .iter()
        .zip(input.samples.iter())
        .map(|(res, original)| (original.index, *res))
    {
        result.push(item);
    }

    result
}

/// True if two samples in the group share a timestamp (checked over a sorted copy of the
/// timestamps; groups are small, so the O(n log n) copy is cheap and only paid for MADD).
fn has_in_batch_duplicate(samples: &[IndexedSample]) -> bool {
    let mut timestamps: SmallVec<[Timestamp; 8]> = samples.iter().map(|s| s.timestamp).collect();
    timestamps.sort_unstable();
    timestamps.windows(2).any(|w| w[0] == w[1])
}

/// Apply a group with in-batch duplicate timestamps as sequential single-sample adds, in input
/// order, so each item folds under the duplicate policy exactly as a standalone TS.ADD would.
///
/// Compaction is fed the *distinct* final stored samples (read back per affected timestamp): the
/// append path streams samples into the open bucket one-by-one, so a duplicate timestamp appearing
/// twice would otherwise be counted twice. Reading the committed value back also yields the
/// policy-folded result (e.g. the summed value for `SUM`) rather than the raw inputs.
///
/// The adds here defer the retention trim (the caller applies it once, after compaction), so the
/// read-back still sees a sample that a later item in the same batch pushed outside the window.
fn add_group_sequentially(input: &mut PerSeriesSamples) -> SmallVec<[(usize, SampleAddResult); 8]> {
    let mut result: SmallVec<[(usize, SampleAddResult); 8]> = SmallVec::new();
    let mut touched: SmallVec<[Timestamp; 8]> = SmallVec::new();

    // `input.samples` is in original MADD argument order (never sorted on the merge path).
    let items: SmallVec<[IndexedSample; 6]> = std::mem::take(&mut input.samples);
    for item in &items {
        let res = input
            .series
            .add_deferring_retention(item.timestamp, item.value, None);
        // Accepted items only. A rejected one — a BLOCK duplicate included — stores nothing, and
        // RedisTimeSeries runs no compaction for it. Replayed, a rejected repeat whose bucket a
        // later item had closed read as a back-fill and recalculated that bucket under the
        // retention floor in force, dropping its older samples (`RETENTION 1000`, sum/1000,
        // `TS.MADD k 1100 1 k 1800 2 k 2500 4 k 1800 8` published 2, not 3). It also made the
        // primary disagree with its replicas, which never see the rejected item: the
        // propagated TS.MADD carries accepted items only.
        if res.is_ok() {
            touched.push(item.timestamp);
            input.added_order.push(item.timestamp);
        }
        result.push((item.index, res));
    }

    touched.sort_unstable();
    touched.dedup();
    let mut added: SmallVec<[Sample; 8]> = SmallVec::new();
    for ts in touched {
        if let Ok(Some(sample)) = input.series.get_sample(ts) {
            added.push(sample);
        }
    }
    input.added = added;

    result
}

/// Propagate each group's merged batch to its compaction destinations, one batch per series
/// (the old per-sample path locked the context once per sample and rebuilt the open bucket
/// for every in-order sample).
fn run_group_compactions(ctx: &Context, groups: &mut [PerSeriesSamples]) {
    for group in groups.iter_mut() {
        if group.series.rules.is_empty() || group.added.is_empty() {
            continue;
        }

        if let Err(e) =
            group
                .series
                .batch_compaction(ctx, &group.added, group.prev_last, &group.added_order)
        {
            let key = key_for_display(&group.series.key);
            let msg = format!("TSDB: error running compaction for key '{key}': {e}");
            ctx.log_warning(&msg);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    fn s(timestamp: i64, value: f64) -> Sample {
        Sample { timestamp, value }
    }

    type Outcome = (Vec<SampleAddResult>, Vec<Sample>);

    /// `batch` applied to clones of `series` through the TS.MADD merge path and through
    /// sequential single-sample adds (each without the retention trim): per-input results and
    /// stored samples, in that order.
    fn madd_vs_sequential(series: &TimeSeries, batch: &[Sample]) -> (Outcome, Outcome) {
        let mut merged = series.clone();
        let mut group = PerSeriesSamples::new(&mut merged);
        for (index, sample) in batch.iter().enumerate() {
            group.add_sample(*sample, index);
        }
        let mut pairs = multi_series_merge_samples(vec![group], None);
        pairs.sort_by_key(|(index, _)| *index);
        let merge_results = pairs.into_iter().map(|(_, result)| result).collect();

        let mut sequential = series.clone();
        let sequential_results = batch
            .iter()
            .map(|smp| sequential.add_deferring_retention(smp.timestamp, smp.value, None))
            .collect();

        (
            (merge_results, merged.iter().collect()),
            (sequential_results, sequential.iter().collect()),
        )
    }

    fn ignoring_series(retention_ms: u64) -> TimeSeries {
        let mut series = TimeSeries {
            retention: Duration::from_millis(retention_ms),
            ..Default::default()
        };
        series.sample_duplicates.policy = Some(DuplicatePolicy::KeepLast);
        series.sample_duplicates.max_time_delta = 100;
        series.sample_duplicates.max_value_delta = 1.0;
        series
    }

    /// An item is in order (and so subject to IGNORE) by the items before it in argument order,
    /// not by timestamp order: here 1000 arrives after 1002 and is an upsert, which IGNORE never
    /// filters. Deciding IGNORE in sorted order dropped 1002 instead.
    #[test]
    fn madd_decides_ignore_in_argument_order() {
        let series = ignoring_series(0);
        let batch = [s(1002, 1.0), s(1000, 1.0)];

        let (merged, sequential) = madd_vs_sequential(&series, &batch);

        assert_eq!(
            sequential.0,
            vec![SampleAddResult::Ok(batch[0]), SampleAddResult::Ok(batch[1])]
        );
        assert_eq!(sequential.1, vec![s(1000, 1.0), s(1002, 1.0)]);
        assert_eq!(merged, sequential);
    }

    /// An ignored item stores nothing, so it must not raise the retention floor: 960 is within
    /// 50 ms of the stored 1000, whatever the ignored 1040 would have implied.
    #[test]
    fn madd_ignored_items_do_not_raise_the_retention_floor() {
        let mut series = ignoring_series(50);
        series.add(1000, 1.0, None);
        let batch = [s(1040, 1.0), s(960, 5.0)];

        let (merged, sequential) = madd_vs_sequential(&series, &batch);

        assert_eq!(
            sequential.0,
            vec![
                SampleAddResult::Ignored(1000),
                SampleAddResult::Ok(batch[1])
            ]
        );
        assert_eq!(sequential.1, vec![s(960, 5.0), s(1000, 1.0)]);
        assert_eq!(merged, sequential);
    }

    /// An upsert onto the last sample leaves the policy-resolved value as the IGNORE baseline:
    /// a NaN under LAST keeps the stored 1, so 1001 is within the IGNORE bounds of it.
    /// Tracking the NaN input instead accepted 1001.
    #[test]
    fn madd_nan_upsert_keeps_the_stored_value_as_the_ignore_baseline() {
        let mut series = ignoring_series(0);
        series.add(1000, 1.0, None);
        let batch = [s(1000, f64::NAN), s(1001, 1.0)];

        let (merged, sequential) = madd_vs_sequential(&series, &batch);

        assert_eq!(sequential.0[1], SampleAddResult::Ignored(1000));
        assert_eq!(sequential.1, vec![s(1000, 1.0)]);
        assert_eq!(merged.0[1], sequential.0[1]);
        assert_eq!(merged.1, sequential.1);
    }

    /// Seeded batches over every duplicate policy, with and without IGNORE and retention, in
    /// arbitrary argument order: the merge path must answer and store exactly what sequential
    /// adds do.
    #[test]
    fn madd_merge_matches_sequential_adds() {
        let mut state = 0x2545_F491_4F6C_DD1Du64;
        let mut next = |bound: u64| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state % bound
        };
        let policies = [
            DuplicatePolicy::Block,
            DuplicatePolicy::KeepFirst,
            DuplicatePolicy::KeepLast,
            DuplicatePolicy::Min,
            DuplicatePolicy::Max,
            DuplicatePolicy::Sum,
        ];

        for case in 0..5_000 {
            let mut series = TimeSeries {
                retention: Duration::from_millis([0, 50, 500][next(3) as usize]),
                ..Default::default()
            };
            series.sample_duplicates.policy = Some(policies[next(6) as usize]);
            if next(2) == 0 {
                series.sample_duplicates.max_time_delta = 100;
                series.sample_duplicates.max_value_delta = 1.0;
            }
            for _ in 0..next(5) {
                series.add(900 + 20 * next(10) as i64, next(4) as f64, None);
            }

            // Distinct timestamps: a batch with repeats takes the sequential path anyway.
            let mut timestamps: Vec<i64> = Vec::new();
            // Often upsert onto the last sample first: it moves the IGNORE baseline to whatever
            // the policy stores there, which later in-order items are compared against.
            let lead_upsert = series.last_sample.is_some() && next(3) == 0;
            if let Some(last) = series.last_sample.filter(|_| lead_upsert) {
                timestamps.push(last.timestamp);
            }
            for _ in 0..2 + next(10) {
                let ts = 800 + 10 * next(40) as i64;
                if !timestamps.contains(&ts) {
                    timestamps.push(ts);
                }
            }
            if timestamps.len() < 2 {
                continue;
            }
            let mut batch: Vec<Sample> = timestamps
                .iter()
                .map(|&ts| {
                    let value = match next(8) {
                        0 => f64::NAN,
                        v => v as f64 * 0.375,
                    };
                    s(ts, value)
                })
                .collect();
            // The generator's consecutive draws are correlated; pick the leading upsert's value
            // on its own so it is a NaN often enough (NaN under LAST keeps the stored value).
            if lead_upsert && next(2) == 0 {
                batch[0].value = f64::NAN;
            }

            let (merged, sequential) = madd_vs_sequential(&series, &batch);
            // `Ok` carries the stored (merged) value on the merge path and the input value on
            // the sequential one; compare what was decided, not that payload.
            let decided = |results: &[SampleAddResult]| -> Vec<SampleAddResult> {
                results
                    .iter()
                    .map(|r| match r {
                        SampleAddResult::Ok(smp) => SampleAddResult::Ok(s(smp.timestamp, 0.0)),
                        other => *other,
                    })
                    .collect()
            };
            // NaN is stored as-is under some policies; compare it by bit pattern.
            let stored = |samples: &[Sample]| -> Vec<(i64, u64)> {
                samples
                    .iter()
                    .map(|smp| (smp.timestamp, smp.value.to_bits()))
                    .collect()
            };
            assert_eq!(
                (decided(&merged.0), stored(&merged.1)),
                (decided(&sequential.0), stored(&sequential.1)),
                "case {case}: policy {:?}, ignore ({}, {}), retention {:?}, stored {:?}, batch {batch:?}",
                series.sample_duplicates.policy,
                series.sample_duplicates.max_time_delta,
                series.sample_duplicates.max_value_delta,
                series.retention,
                series.iter().collect::<Vec<_>>(),
            );
        }
    }
}
