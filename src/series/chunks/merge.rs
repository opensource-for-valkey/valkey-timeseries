use crate::common::logging::log_warning;
use crate::common::{Sample, Timestamp};
use crate::error::{TsdbError, TsdbResult};
use crate::error_consts;
use crate::iterators::SampleMergeIterator;
use crate::series::chunks::{ChunkOps, TimeSeriesChunk};
use crate::series::{DuplicatePolicy, SampleAddResult};

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
pub(crate) fn merge_chunk_samples<F>(
    existing: impl Iterator<Item = Sample>,
    samples: &[Sample],
    dp_policy: Option<DuplicatePolicy>,
    mut append: F,
) -> TsdbResult<Vec<SampleAddResult>>
where
    F: FnMut(Sample) -> TsdbResult<()>,
{
    let dp_policy = dp_policy.unwrap_or(DuplicatePolicy::KeepLast);

    let mut results = Vec::with_capacity(samples.len());
    let mut merge_iterator = SampleMergeIterator::new(existing, samples.iter().copied(), dp_policy);
    // A result is owed for each step that consumed an incoming sample, once per timestamp: a
    // pre-existing sample only being rewritten gets none, and a repeated input timestamp gets one.
    let mut last_reported: Option<Timestamp> = None;

    while let Some((sample, blocked, from_right)) = merge_iterator.next_internal() {
        append(sample)?;
        if from_right && last_reported != Some(sample.timestamp) {
            last_reported = Some(sample.timestamp);
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

    // Room in `dest`, in samples of `src`. `estimate_remaining_sample_capacity` divides the
    // free bytes by `dest`'s own bytes per sample, which overstates the room for a sparser
    // source by the ratio of their densities: a dense constant chunk (about a byte a sample)
    // took a chunk of 17-byte samples whole and outgrew its budget, at 1 MiB budgets past the
    // RDB loader's chunk-size limit. Merged sizes are close to additive, so `src`'s own
    // density bounds what fits. `defrag_series` splits whatever still overshoots.
    let count = src.len();
    let room = dest.max_size().saturating_sub(dest.size());
    let src_bytes_per_sample = src.size().div_ceil(count).max(1);
    let remaining_capacity = dest
        .estimate_remaining_sample_capacity()
        .min(room / src_bytes_per_sample);
    // Decide before decoding where the floor cannot shorten the source: then every sample
    // counts, and room for a quarter of them or less merges nothing. That is the usual case
    // in defrag (a chunk completed by appends is full), which runs on the main thread, so it
    // must not decode a whole chunk only to learn nothing moves.
    if min_timestamp <= src.first_timestamp() && remaining_capacity <= count / 4 {
        return Ok(None);
    }
    // Neither path moves a sample below `min_timestamp` (both used to differ: the whole-source
    // merge carried expired samples along). The source is decoded strictly: `iter()` and
    // `get_range` end early at a decode error, and emptying or rewriting the source after such
    // a read lost every sample past it while reporting a successful merge.
    let floor = src.first_timestamp().max(min_timestamp);
    let mut samples = src.decode_all()?;
    let below = samples.partition_point(|s| s.timestamp < floor);
    samples.drain(..below);
    if remaining_capacity >= samples.len() {
        let res = dest.merge_samples(&samples, duplicate_policy)?;
        let merged = res.iter().filter(|s| s.is_ok()).count();
        // reuse last block
        src.clear();
        return Ok(Some(merged));
    } else if remaining_capacity > count / 4 {
        // do a partial merge
        let (left, right) = samples.split_at(remaining_capacity);
        let res = dest.merge_samples(left, duplicate_policy)?;
        src.set_data(right)?;
        let count = res.iter().filter(|s| s.is_ok()).count();
        return Ok(Some(count));
    }
    Ok(None)
}
