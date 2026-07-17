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
}

/// The oldest timestamp the series will accept, derived from its retention window.
pub(super) fn get_min_allowed_timestamp(series: &TimeSeries) -> Timestamp {
    if series.retention.is_zero() {
        0
    } else {
        series.get_min_timestamp()
    }
}

/// Per-input-index mask of samples to reject as `TooOld`, evaluated in input order.
///
/// Mirrors sequential per-item `TS.ADD`: a running max timestamp (seeded from the series' current
/// max, or the first batch item if the series is empty) advances as accepted items are seen, and
/// each item is rejected only when it is older than `running_max - retention` *at that point*.
/// Retention `0` never rejects.
fn retention_gate(series: &TimeSeries, samples: &[Sample]) -> Vec<bool> {
    let mut too_old = vec![false; samples.len()];
    if series.retention.is_zero() {
        return too_old;
    }
    let retention_ms = series.retention.as_millis() as i64;
    let mut running_max: Option<Timestamp> = if series.is_empty() {
        None
    } else {
        Some(series.last_timestamp())
    };
    for (i, sample) in samples.iter().enumerate() {
        match running_max {
            Some(max) if sample.timestamp < max.saturating_sub(retention_ms) => too_old[i] = true,
            Some(max) => running_max = Some(max.max(sample.timestamp)),
            None => running_max = Some(sample.timestamp),
        }
    }
    too_old
}

/// Normalizes a batch of samples the same way single-sample [`TimeSeries::add`] does, so the
/// bulk path stays behaviorally consistent with it:
/// - reports samples older than the retention window as `TooOld` (see below);
/// - applies the series' value rounding (`SIGNIFICANT_DIGITS`/`DECIMAL_DIGITS`);
/// - applies the IGNORE filter (`max_time_delta`/`max_value_delta`) to in-order samples, reporting
///   ignored samples as `Ignored`.
///
/// The batch is processed in ascending timestamp order; unsorted input pays for one extra
/// index sort (stable, so the *first* of two equal timestamps in input order wins). Results
/// are always reported at the sample's original input index.
///
/// The retention gate is the one decision evaluated in **input order** rather than sorted order,
/// because RedisTimeSeries processes `TS.MADD` items sequentially: an item is `TooOld` only if it
/// falls below the retention floor induced by the max timestamp of the items at or before it (and
/// any pre-existing samples). A *later* item that raises the floor does not retroactively reject an
/// earlier accepted item — that earlier sample is inserted and then removed by the post-merge
/// retention trim, so it is still reported as accepted. Only the per-item result differs between
/// the two orderings; the stored end-state is identical either way.
pub(super) fn normalize_batch(
    series: &TimeSeries,
    samples: &[Sample],
    policy_override: Option<DuplicatePolicy>,
) -> NormalizedBatch {
    let too_old = retention_gate(series, samples);
    let dup_policy = series.sample_duplicates;

    // Process in ascending timestamp order even if the caller's batch isn't sorted.
    let order: Option<Vec<usize>> = if samples.is_sorted_by_key(|s| s.timestamp) {
        None
    } else {
        let mut order: Vec<usize> = (0..samples.len()).collect();
        order.sort_by_key(|&i| samples[i].timestamp);
        Some(order)
    };

    let mut results = vec![SampleAddResult::Error("Unknown error"); samples.len()];
    let mut to_insert = Vec::with_capacity(samples.len());
    let mut insert_index = Vec::with_capacity(samples.len());

    // Running last stored sample, so the IGNORE filter compares against the value that would
    // actually be present, mirroring single-sample add semantics as the batch is applied in order.
    let mut running_last = series.last_sample;
    // Timestamp of the previous sample in processing order; duplicate timestamps within a single
    // batch are disallowed (processing order is sorted, so duplicates are adjacent).
    let mut prev_ts: Option<Timestamp> = None;

    for pos in 0..samples.len() {
        let index = order.as_ref().map_or(pos, |o| o[pos]);
        let sample = &samples[index];

        if prev_ts == Some(sample.timestamp) {
            results[index] = SampleAddResult::Duplicate;
            continue;
        }
        prev_ts = Some(sample.timestamp);

        if too_old[index] {
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

        to_insert.push(adjusted);
        insert_index.push(index);
    }

    NormalizedBatch {
        to_insert,
        insert_index,
        results,
    }
}
