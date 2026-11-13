//! Pre-merge batch normalization shared by all bulk ingest paths
//! (`TS.ADDBULK` via `bulk_add` and `TS.MADD` via `sample_merge`).
//!
//! Normalization is the single place where retention filtering, value rounding,
//! in-batch duplicate rejection and the IGNORE filter are applied, so the bulk
//! paths stay behaviorally consistent with single-sample [`TimeSeries::add`].
use crate::common::Sample;
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
}

/// Per-input-index mask of in-batch duplicates: every occurrence of a timestamp after the first
/// in input order. A stable index sort makes equal timestamps adjacent in input order.
fn in_batch_duplicates(samples: &[Sample]) -> Vec<bool> {
    let mut duplicate = vec![false; samples.len()];
    if samples.is_sorted_by_key(|s| s.timestamp) {
        for i in 1..samples.len() {
            duplicate[i] = samples[i].timestamp == samples[i - 1].timestamp;
        }
    } else {
        let mut order: Vec<usize> = (0..samples.len()).collect();
        order.sort_by_key(|&i| samples[i].timestamp);
        for pair in order.windows(2) {
            duplicate[pair[1]] = samples[pair[1]].timestamp == samples[pair[0]].timestamp;
        }
    }
    duplicate
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
/// sequential adds would leave it: it advances only on an accepted sample at or past it, never
/// on one that is `TooOld`, `Ignored` or a repeat. That is what RedisTimeSeries does for
/// `TS.MADD`, item by item: whether an item is in order (and so subject to IGNORE) and where the
/// retention floor sits both depend on the items before it, not on timestamp order. A *later*
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
    let duplicate = in_batch_duplicates(samples);
    let dup_policy = series.sample_duplicates;
    let retention_ms = (!series.retention.is_zero()).then_some(series.retention.as_millis() as i64);

    let mut results = vec![SampleAddResult::Error("Unknown error"); samples.len()];
    let mut accepted: Vec<(usize, Sample)> = Vec::with_capacity(samples.len());

    // The last stored sample as sequential adds would leave it: the retention floor and the
    // IGNORE comparison both read it.
    let mut running_last = series.last_sample;

    for (index, sample) in samples.iter().enumerate() {
        if duplicate[index] {
            results[index] = SampleAddResult::Duplicate;
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

        if running_last.is_none_or(|last| adjusted.timestamp >= last.timestamp) {
            running_last = Some(adjusted);
        }
        accepted.push((index, adjusted));
    }

    // Chunk merging needs ascending timestamps. They are unique by now, so the sort need not
    // be stable.
    if !accepted.is_sorted_by_key(|(_, s)| s.timestamp) {
        accepted.sort_unstable_by_key(|(_, s)| s.timestamp);
    }
    let (insert_index, to_insert) = accepted.into_iter().unzip();

    NormalizedBatch {
        to_insert,
        insert_index,
        results,
    }
}
