use crate::common::{Sample, Timestamp};
use crate::error::TsdbResult;
use crate::series::bulk_add::merge_samples_into_series;
use crate::series::index::get_series_key_by_id;
use crate::series::{DuplicatePolicy, SampleAddResult, TimeSeries};
use orx_parallel::ParIterResult;
use orx_parallel::{ParIter, ParallelizableCollectionMut};
use smallvec::{SmallVec, smallvec};
use valkey_module::{Context, ValkeyError, ValkeyResult};

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
    /// which `added` discards by sorting. Only the DIV-0023 forward-close marker needs it —
    /// see `last_forward_close_in_input_order`.
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
/// `samples` **must** be sorted by timestamp.
///
/// ### Arguments
///
/// * `samples` - A slice of samples to merge into the time series
/// * `policy_override` - Optional override for the duplicate policy to use when merging
///
/// ### Returns
///
/// A result containing a vector of `SampleAddResult` with the outcome for each sample.
///
pub(super) fn merge_samples(
    series: &mut TimeSeries,
    samples: &[Sample],
    policy_override: Option<DuplicatePolicy>,
) -> TsdbResult<Vec<SampleAddResult>> {
    if samples.is_empty() {
        return Ok(Vec::new());
    }

    let results = merge_samples_into_series(series, samples, policy_override);
    series.split_chunks_if_needed()?;
    Ok(results)
}

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
/// ### Returns
/// Returns a `ValkeyResult` containing a `SmallVec` of tuples (group index, SampleAddResult) on success.:
/// - The second element is the result of processing the samples for that series.
///
///
pub fn multi_series_merge_samples(
    groups: Vec<PerSeriesSamples>,
    ctx: Option<&Context>,
) -> ValkeyResult<SmallVec<[(usize, SampleAddResult); 8]>> {
    if groups.is_empty() {
        return Ok(smallvec![]);
    }
    let mut groups = groups;

    let res = if groups.len() == 1 {
        add_samples_internal(&mut groups[0])?
    } else {
        groups
            .par_mut()
            .map(add_samples_internal)
            .into_fallible_result()
            .reduce(|mut acc, item| {
                acc.extend(item);
                acc
            })?
            .unwrap()
    };

    if let Some(ctx) = ctx {
        run_group_compactions(ctx, &mut groups);
    }

    // Retention is applied only now. The merge above deliberately skipped the eager trim so
    // that `add_group_sequentially`'s read-back still sees a sample which a later item in the
    // same batch pushed outside the window — that is what lets retention be decided ahead of
    // the duplicate policy (corpus: madd_retention_beats_duplicate_policy).
    //
    // Deferring it does *not* leak expired samples into the destination buckets: compaction
    // aggregates through the retention-clamped `range_iter`, so a sample this trim is about to
    // evict is already invisible to the bucket recalculation.
    for group in groups.iter_mut() {
        group.series.apply_retention();
    }

    Ok(res)
}

fn add_samples_internal(
    input: &mut PerSeriesSamples,
) -> ValkeyResult<SmallVec<[(usize, SampleAddResult); 8]>> {
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

        return Ok(smallvec![(index, result)]);
    }

    // In-batch duplicate timestamps: RTS applies each MADD item as an independent, sequential
    // upsert under the series' duplicate policy (SUM accumulates, LAST wins, FIRST/MIN/MAX fold,
    // BLOCK errors on the duplicate), so they must NOT be collapsed. The parallel bulk merge
    // assumes unique timestamps per batch, so fall back to sequential single-sample add() in
    // input order, which reproduces RTS exactly.
    if has_in_batch_duplicate(&input.samples) {
        return Ok(add_group_sequentially(input));
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
        .merge_samples_deferring_retention(&samples, None)
        .map_err(|e| ValkeyError::String(format!("{e}")))?;

    // Accepted samples feed batch compaction, which requires them ascending by timestamp.
    // `add_results` is parallel to `samples`, i.e. still in input order, so the order is
    // captured before sorting (the DIV-0023 forward-close marker depends on it).
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

    Ok(result)
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
        if res.is_ok() {
            touched.push(item.timestamp);
            // Input order, duplicates included: a repeated timestamp does not advance the
            // running max, so it reads as a back-fill exactly as RTS applies it.
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
            let key = get_series_key_by_id(ctx, group.series.id)
                .unwrap_or_else(|| ctx.create_string("Unknown"));
            let msg = format!("TSDB: error running compaction for key '{key}': {e}");
            ctx.log_warning(&msg);
        }
    }
}
