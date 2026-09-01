use super::chunks::{ChunkOps, merge_by_capacity};
use super::time_series::TimeSeries;
use crate::error::TsdbResult;

/// Compact a series in place: drop expired data, then pull each chunk's samples forward into
/// the preceding chunk wherever that chunk still has room, and discard whatever is left empty.
///
/// Runs from the module type's `defrag` callback, so it must leave the series' bookkeeping
/// (`total_samples`, `first_timestamp`, `last_sample`) exactly consistent with the chunks it
/// produced -- nothing downstream re-derives them.
pub fn defrag_series(series: &mut TimeSeries) -> TsdbResult {
    series.trim()?;

    if series.chunks.len() < 2 {
        return Ok(());
    }

    let min_timestamp = series.get_min_timestamp();
    let duplicate_policy = series.sample_duplicates.policy;

    let mut iter = series.chunks.iter_mut();
    // we ensure above that we have at least 2 chunks
    let mut prev_chunk = iter.next().unwrap();
    while let Some(mut chunk) = iter.next() {
        if chunk.is_empty() {
            continue;
        }

        // while the previous block has capacity merge into it
        while merge_by_capacity(prev_chunk, chunk, min_timestamp, duplicate_policy)?.is_some() {
            let Some(next_chunk) = iter.next() else {
                break;
            };
            chunk = next_chunk;
        }

        prev_chunk = chunk;
    }

    // Discard whatever the merges emptied. This used to collect indices during the walk and
    // remove them afterwards in ascending order, which shifted every later index down by one, so
    // the wrong chunks were dropped -- and once enough had been, `Vec::remove` ran off the end
    // and panicked (observed: "removal index (is 2) should be < len (is 2)" on a four-chunk
    // series). The collected indices were themselves off by one: the counter was seeded before
    // the first chunk the loop actually visits. Retaining by emptiness needs no index at all.
    let saved_len = series.chunks.len();
    series.chunks.retain(|chunk| !chunk.is_empty());
    if series.chunks.len() < saved_len {
        series.chunks.shrink_to_fit();
    }

    // Recount instead of adjusting by a delta. `merge_by_capacity` reports how many samples it
    // *wrote into the destination*, which is not a count of samples the series lost: the old code
    // subtracted it from a counter that no branch could ever raise above zero, underflowing
    // `usize` on the first merge. In debug that panicked; in release, where this crate leaves
    // `overflow-checks` off, it wrapped twice and left `total_samples` inflated by exactly the
    // number of merged samples (observed: 15 reported for a 9-sample series). A merge can also
    // genuinely drop samples -- one the destination rejects, or one below the retention floor
    // that the partial merge path filters out -- so summing the chunks is the only exact answer.
    series.recalculate_total_samples();
    series.update_first_last_timestamps();

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::Sample;
    use crate::series::chunks::{ChunkOps, GorillaChunk, TimeSeriesChunk};
    use std::time::Duration;

    /// A chunk holding `count` samples starting at `start`, with room to spare so the
    /// defragmenter's capacity check lets it be merged into.
    fn chunk_with(start: i64, count: i64) -> TimeSeriesChunk {
        let mut chunk = TimeSeriesChunk::Gorilla(GorillaChunk::with_max_size(1024));
        for i in 0..count {
            let timestamp = start + i;
            chunk
                .add_sample(&Sample {
                    timestamp,
                    value: timestamp as f64,
                })
                .unwrap();
        }
        chunk
    }

    fn series_from(chunks: Vec<TimeSeriesChunk>) -> TimeSeries {
        let mut series = TimeSeries::from_chunks(chunks).unwrap();
        // Keep `trim()` a no-op so these exercise the merge path alone.
        series.retention = Duration::ZERO;
        series
    }

    fn all_samples(series: &TimeSeries) -> Vec<Sample> {
        series
            .chunks
            .iter()
            .flat_map(|chunk| chunk.iter())
            .collect()
    }

    #[test]
    fn test_defrag_merges_chunks_without_losing_samples() {
        let expected: Vec<Sample> = (0..4)
            .flat_map(|c| (0..3).map(move |i| 1000 + c * 100 + i))
            .map(|timestamp| Sample {
                timestamp,
                value: timestamp as f64,
            })
            .collect();

        let mut series = series_from(vec![
            chunk_with(1000, 3),
            chunk_with(1100, 3),
            chunk_with(1200, 3),
            chunk_with(1300, 3),
        ]);
        assert_eq!(series.total_samples, 12);

        defrag_series(&mut series).unwrap();

        // Every emptied chunk is dropped, and the data lands in the one that absorbed it.
        // Removing the collected indices in ascending order used to shift every later index
        // down by one, so the wrong chunks were dropped -- including the merge destination.
        assert_eq!(series.chunks.len(), 1);
        assert_eq!(all_samples(&series), expected);
    }

    #[test]
    fn test_defrag_keeps_total_samples_in_step_with_the_chunks() {
        let mut series = series_from(vec![
            chunk_with(1000, 3),
            chunk_with(1100, 3),
            chunk_with(1200, 3),
        ]);

        // `merge_by_capacity` reports samples *written to the destination*, and the old code
        // subtracted that from a counter that was always zero: a debug build panicked here on
        // the very first merge, and a release build wrapped `total_samples` around.
        defrag_series(&mut series).unwrap();

        assert_eq!(series.total_samples, 9);
        assert_eq!(
            series.total_samples,
            series.chunks.iter().map(|c| c.len()).sum::<usize>()
        );
        assert_eq!(series.first_timestamp, 1000);
        assert_eq!(series.last_sample.map(|s| s.timestamp), Some(1202));
    }

    #[test]
    fn test_defrag_drops_empty_chunks() {
        let mut series = series_from(vec![
            chunk_with(1000, 3),
            TimeSeriesChunk::Gorilla(GorillaChunk::with_max_size(1024)),
            chunk_with(1200, 3),
        ]);
        // `from_chunks` counts the samples, so the empty chunk contributes nothing.
        assert_eq!(series.total_samples, 6);

        defrag_series(&mut series).unwrap();

        assert!(series.chunks.iter().all(|chunk| !chunk.is_empty()));
        assert_eq!(series.total_samples, 6);
        assert_eq!(all_samples(&series).len(), 6);
    }

    #[test]
    fn test_defrag_is_a_noop_below_two_chunks() {
        let mut series = series_from(vec![chunk_with(1000, 3)]);
        defrag_series(&mut series).unwrap();
        assert_eq!(series.chunks.len(), 1);
        assert_eq!(series.total_samples, 3);
    }
}
