//! Pre-merge batch normalization shared by all bulk ingest paths
//! (`TS.ADDBULK` via `bulk_add` and `TS.MADD` via `sample_merge`).
//!
//! Normalization is the single place where retention filtering, value rounding,
//! in-batch duplicate rejection and the IGNORE filter are applied, so the bulk
//! paths stay behaviorally consistent with single-sample [`TimeSeries::add`].
use crate::common::{Sample, Timestamp};
use crate::series::{DuplicatePolicy, SampleAddResult, TimeSeries};

/// Result of pre-merge normalization shared by all bulk ingest paths.
pub(super) struct NormalizedBatch {
    /// Samples that should actually be merged into chunks (values already rounded), in ascending
    /// timestamp order.
    pub(super) to_insert: Vec<Sample>,
    /// Original index (into the caller's `samples`) for each entry in `to_insert`.
    pub(super) insert_index: Vec<usize>,
    /// Per-original-index results, sized to the caller's `samples`. Retention-filtered samples are
    /// pre-set to `TooOld` and IGNORE-filtered samples to `Ignored`; survivors hold a placeholder
    /// that the caller overwrites with the merge outcome.
    pub(super) results: Vec<SampleAddResult>,
    /// Whether the batch repeats a timestamp. Every repeat is reported `Duplicate` here, which
    /// is `TS.ADDBULK`'s rule; `TS.MADD` folds repeats under the duplicate policy instead, so it
    /// applies such a batch item by item rather than merging this one.
    pub(super) has_repeats: bool,
}

/// An *unsorted* batch's `(timestamp, input index)` pairs, sorted. The index breaks ties, so
/// equal timestamps sit together in input order: the in-batch repeats (every occurrence after
/// the first) and the order the survivors are merged in both come from this one sort. A sorted
/// batch needs neither (a repeat equals its predecessor), so [`normalize_batch`] skips it.
fn unsorted_order(samples: &[Sample]) -> Vec<(Timestamp, usize)> {
    let mut order: Vec<(Timestamp, usize)> = samples
        .iter()
        .enumerate()
        .map(|(index, sample)| (sample.timestamp, index))
        .collect();
    order.sort_unstable();
    order
}

/// Per-input-index mask of the repeats in `order` (see [`unsorted_order`]).
fn repeat_mask(order: &[(Timestamp, usize)]) -> Vec<bool> {
    let mut repeat = vec![false; order.len()];
    for pair in order.windows(2) {
        repeat[pair[1].1] = pair[1].0 == pair[0].0;
    }
    repeat
}

/// Normalizes a batch of samples the same way single-sample [`TimeSeries::add`] does, so the
/// bulk path stays behaviorally consistent with it:
/// - refuses every repeat of a timestamp within the batch as `Duplicate` (the first in input
///   order is kept; `TS.MADD` routes batches with repeats to sequential adds instead);
/// - reports samples older than the retention window as `TooOld`;
/// - applies the series' value rounding (`SIGNIFICANT_DIGITS`/`DECIMAL_DIGITS`);
/// - applies the IGNORE filter (`max_time_delta`/`max_value_delta`) to in-order samples, reporting
///   ignored samples as `Ignored`.
///
/// Retention and IGNORE are decided in **input order**, against the series' last sample as
/// sequential adds would leave it: it advances only on an accepted sample at or past it (to the
/// policy-resolved value for one at it), never on one that is `TooOld`, `Ignored` or a repeat.
/// That is what RedisTimeSeries does for `TS.MADD`, item by item: whether an item is in order
/// (and so subject to IGNORE) and where the retention floor sits both depend on the items before
/// it, not on timestamp order. A *later*
/// item that raises the floor does not retroactively reject an earlier accepted item — that
/// earlier sample is inserted and then removed by the post-merge retention trim, so it is still
/// reported as accepted.
///
/// Results are reported at each sample's input index; the accepted samples are returned sorted
/// by timestamp for chunk merging.
pub(super) fn normalize_batch(
    series: &TimeSeries,
    samples: &[Sample],
    policy_override: Option<DuplicatePolicy>,
) -> NormalizedBatch {
    // Sorted input is the hot case (TS.ADDBULK sorts its payload; in-order TS.MADD), and costs
    // no allocations beyond the three returned vectors.
    let sorted = samples.is_sorted_by_key(|s| s.timestamp);
    let order = (!sorted).then(|| unsorted_order(samples));
    let repeats = order.as_deref().map(repeat_mask);
    let is_repeat = |i: usize| match &repeats {
        Some(mask) => mask[i],
        None => i > 0 && samples[i].timestamp == samples[i - 1].timestamp,
    };
    let mut has_repeats = false;
    let dup_policy = series.sample_duplicates;
    let retention_ms = (!series.retention.is_zero()).then_some(series.retention.as_millis() as i64);

    let mut results = vec![SampleAddResult::Error("Unknown error"); samples.len()];
    let mut to_insert: Vec<Sample> = Vec::with_capacity(samples.len());
    let mut insert_index: Vec<usize> = Vec::with_capacity(samples.len());
    // For unsorted input: each input index's position in `to_insert`, or `usize::MAX` for one
    // that is not inserted, so the survivors can be put in timestamp order without a sort.
    let mut position = if sorted {
        Vec::new()
    } else {
        vec![usize::MAX; samples.len()]
    };

    // The last stored sample as sequential adds would leave it: the retention floor and the
    // IGNORE comparison both read it.
    let mut running_last = series.last_sample;

    for (index, sample) in samples.iter().enumerate() {
        if is_repeat(index) {
            results[index] = SampleAddResult::Duplicate;
            has_repeats = true;
            continue;
        }

        if let (Some(retention_ms), Some(last)) = (retention_ms, running_last)
            && sample.timestamp < last.timestamp.saturating_sub(retention_ms)
        {
            results[index] = SampleAddResult::TooOld;
            continue;
        }

        let adjusted = Sample {
            timestamp: sample.timestamp,
            value: series.adjust_value(sample.value),
        };

        // IGNORE only applies to in-order samples (ts >= last); out-of-order samples are upserts
        // and are never filtered here, matching `TimeSeries::add`.
        if let Some(last) = running_last
            && adjusted.timestamp >= last.timestamp
            && dup_policy.is_duplicate(&adjusted, &last, policy_override)
        {
            results[index] = SampleAddResult::Ignored(last.timestamp);
            continue;
        }

        match running_last {
            None => running_last = Some(adjusted),
            Some(last) if adjusted.timestamp > last.timestamp => running_last = Some(adjusted),
            // An upsert onto the last sample leaves what the duplicate policy stores there,
            // which need not be the input (a NaN under LAST keeps the old value; SUM adds).
            // Repeats were refused above, so `last` is the stored sample. A refused upsert
            // (BLOCK, NaN under MIN/MAX/SUM) leaves it unchanged.
            Some(last) if adjusted.timestamp == last.timestamp => {
                if let Ok(value) = dup_policy.resolve_policy(policy_override).duplicate_value(
                    last.timestamp,
                    last.value,
                    adjusted.value,
                ) {
                    running_last = Some(Sample { value, ..last });
                }
            }
            Some(_) => {}
        }
        if let Some(slot) = position.get_mut(index) {
            *slot = to_insert.len();
        }
        to_insert.push(adjusted);
        insert_index.push(index);
    }

    // Chunk merging needs ascending timestamps. A subsequence of a sorted batch is sorted, so
    // only unsorted input can need this, and its sorted order is already at hand: walk it and
    // keep the inserted entries.
    if let Some(order) = order
        && !to_insert.is_sorted_by_key(|s| s.timestamp)
    {
        let (mut sorted_samples, mut sorted_index) = (
            Vec::with_capacity(to_insert.len()),
            Vec::with_capacity(to_insert.len()),
        );
        for (_, index) in order {
            let at = position[index];
            if at != usize::MAX {
                sorted_samples.push(to_insert[at]);
                sorted_index.push(index);
            }
        }
        to_insert = sorted_samples;
        insert_index = sorted_index;
    }

    NormalizedBatch {
        to_insert,
        insert_index,
        results,
        has_repeats,
    }
}
