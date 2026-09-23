use crate::common::hash::IntSet;
use crate::common::logging::log_warning;
use crate::common::{Sample, Timestamp};
use crate::error::{TsdbError, TsdbResult};
use crate::error_consts;
use crate::iterators::{SampleIter, SampleMergeIterator};
use crate::series::chunks::{ChunkOps, TimeSeriesChunk};
use crate::series::{DuplicatePolicy, SampleAddResult};

pub fn merge_samples<'a, F, STATE>(
    left: SampleIter<'a>,
    right: SampleIter<'a>,
    dp_policy: Option<DuplicatePolicy>,
    state: &mut STATE,
    mut f: F,
) -> TsdbResult<()>
where
    F: FnMut(&mut STATE, Sample, bool) -> TsdbResult<()>,
{
    let dp_policy = dp_policy.unwrap_or(DuplicatePolicy::KeepLast);

    let mut merge_iterator = SampleMergeIterator::new(left, right, dp_policy);

    while let Some((sample, blocked)) = merge_iterator.next_internal() {
        f(state, sample, blocked)?;
    }

    Ok(())
}

/// Shared bookkeeping for a chunk rebuild-merge: merges the chunk's `existing` samples with
/// the incoming `samples` under `dp_policy` (default `KeepLast`), feeding every winning
/// sample to `append` in timestamp order.
///
/// Returns one result per unique input timestamp — `Ok` for stored samples, `Duplicate` for
/// samples rejected by the duplicate policy — in ascending timestamp order, matching the
/// order of `samples` (which must be sorted).
///
/// Encodings only supply `append` (write one sample into the rebuilt chunk state) and commit
/// that state after this returns.
pub(crate) fn merge_chunk_samples<'a, F>(
    existing: SampleIter<'a>,
    samples: &'a [Sample],
    dp_policy: Option<DuplicatePolicy>,
    mut append: F,
) -> TsdbResult<Vec<SampleAddResult>>
where
    F: FnMut(Sample) -> TsdbResult<()>,
{
    let dp_policy = dp_policy.unwrap_or(DuplicatePolicy::KeepLast);

    // Track the input timestamps so we produce exactly one result per unique input
    // timestamp, and none for pre-existing samples that are only being rewritten.
    let mut sample_set: IntSet<Timestamp> =
        IntSet::with_capacity_and_hasher(samples.len(), Default::default());
    for sample in samples {
        sample_set.insert(sample.timestamp);
    }

    let mut results = Vec::with_capacity(samples.len());
    let right = SampleIter::Slice(samples.iter());
    let mut merge_iterator = SampleMergeIterator::new(existing, right, dp_policy);

    while let Some((sample, blocked)) = merge_iterator.next_internal() {
        append(sample)?;
        if sample_set.remove(&sample.timestamp) {
            if blocked {
                results.push(SampleAddResult::Duplicate);
            } else {
                results.push(SampleAddResult::Ok(sample));
            }
        }
    }

    Ok(results)
}

/// Shared fast path for merging samples that all lie past the chunk's last timestamp (or
/// into an empty chunk): append them one by one.
///
/// `CapacityFull` aborts the whole merge (the series layer sizes batches to chunk capacity
/// and treats this as a group-level failure); any other per-sample error is reported in that
/// sample's result slot without stopping the batch.
pub(crate) fn append_samples<C: ChunkOps>(
    chunk: &mut C,
    samples: &[Sample],
) -> TsdbResult<Vec<SampleAddResult>> {
    let mut results = Vec::with_capacity(samples.len());
    for sample in samples {
        match chunk.add_sample(sample) {
            Ok(_) => results.push(SampleAddResult::Ok(*sample)),
            Err(TsdbError::CapacityFull(cap)) => return Err(TsdbError::CapacityFull(cap)),
            Err(e) => {
                log_warning(format!("error appending sample in chunk merge: {e:?}"));
                results.push(SampleAddResult::Error(error_consts::CANNOT_ADD_SAMPLE));
            }
        }
    }
    Ok(results)
}

pub(crate) fn merge_by_capacity(
    dest: &mut TimeSeriesChunk,
    src: &mut TimeSeriesChunk,
    min_timestamp: Timestamp,
    duplicate_policy: Option<DuplicatePolicy>,
) -> TsdbResult<Option<usize>> {
    if src.is_empty() {
        return Ok(None);
    }

    // check if the previous block has capacity, and if so, merge into it
    let count = src.len();
    let remaining_capacity = dest.estimate_remaining_sample_capacity();
    let first_ts = src.first_timestamp().max(min_timestamp);
    // if there is enough capacity in the previous block, merge the last block into it
    if remaining_capacity >= count {
        // merge_samples needs a materialized, multi-pass slice and rebuilds the
        // destination per call, so one transient buffer is unavoidable — but its
        // size is known (src.len()), so pre-size it to avoid reallocation churn.
        let mut samples = Vec::with_capacity(count);
        samples.extend(src.iter());
        let res = dest.merge_samples(&samples, duplicate_policy)?;
        let merged = res.iter().filter(|s| s.is_ok()).count();
        // reuse last block
        src.clear();
        return Ok(Some(merged));
    } else if remaining_capacity > count / 4 {
        // do a partial merge
        let samples = src.get_range(first_ts, src.last_timestamp())?;
        // `samples` can be shorter than `count` when `min_timestamp` clips the front.
        let (left, right) = samples.split_at(remaining_capacity.min(samples.len()));
        let res = dest.merge_samples(left, duplicate_policy)?;
        src.set_data(right)?;
        let count = res.iter().filter(|s| s.is_ok()).count();
        return Ok(Some(count));
    }
    Ok(None)
}
