use super::chunks::{ChunkOps, merge_by_capacity};
use super::time_series::{TimeSeries, seal_chunk};
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

        // Pull chunks into `prev_chunk` while each one drains into it completely. A partial
        // merge fills `prev_chunk` and leaves `chunk` holding the upper part of its range, so
        // `chunk` becomes the next destination: pulling a later chunk into `prev_chunk` past
        // it (as this loop used to) put newer samples ahead of older ones and left the chunks
        // overlapping, breaking the binary search, upsert routing and range reads.
        while merge_by_capacity(prev_chunk, chunk, min_timestamp, duplicate_policy)?.is_some()
            && chunk.is_empty()
        {
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

    // `merge_by_capacity` budgets each merge from estimates, so a merged chunk can still end
    // past its budget. Split it as the merge path does: the RDB loader refuses a chunk past
    // `MAX_LOADED_CHUNK_BYTES` from a foreign payload, and every writer keeping chunks within
    // 1.25x their budget is what keeps the server's own snapshots under it. Almost always a
    // scan that finds nothing. Defrag runs on the main thread holding the GIL, and waiting on
    // the shared pool there is allowed (rule R2, `common::threads`). A failed split is
    // reported only after the recount below: the merges above have already changed the
    // chunks, and the counts must describe them either way.
    let split = series.split_chunks_if_needed();

    // Every survivor just absorbed samples from its successors, growing its bit stream by
    // doubling; and defrag is exactly the point at which the caller is asking for memory back.
    // The last chunk is still the append target, so it is left alone to avoid a regrow on the
    // next write.
    let last = series.chunks.len().saturating_sub(1);
    for chunk in series.chunks[..last].iter_mut() {
        seal_chunk(chunk);
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

    split
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::Sample;
    use crate::series::chunks::{ChimpChunk, ChunkOps, GorillaChunk, TimeSeriesChunk};
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

    /// A dense chunk's room was estimated at its own bytes per sample, so it took a sparse
    /// chunk whole at many times the bytes it had left. The merged chunk outgrew its budget,
    /// and at 1 MiB budgets the RDB loader's chunk-size limit: the server could not reload
    /// what it had saved. Defrag must keep every chunk within the merge path's split threshold.
    #[test]
    fn test_defrag_keeps_chunks_within_their_budget() {
        use crate::series::DuplicatePolicy;
        use crate::series::SPLIT_SLACK_DIVISOR;
        use crate::series::chunks::ChunkEncoding;
        use crate::series::serialization::check_series_invariants;

        const BUDGET: usize = 4096;
        for encoding in [ChunkEncoding::Gorilla, ChunkEncoding::Chimp] {
            let mut series = TimeSeries::new();
            series.chunk_encoding = encoding;
            series.chunk_size_bytes = BUDGET;
            series.retention = Duration::ZERO;

            // A constant value at step 1 until the first chunk is full: a few bits a sample.
            let mut timestamp = 0;
            while series.chunks.len() < 2 {
                series.add(timestamp, 1.0, None);
                timestamp += 1;
            }
            let dense_end = series.chunks[0].last_timestamp();

            // Then expensive samples: timestamp deltas alternating 2^42 and 1, random values.
            let mut seed = 42u64;
            let sparse: Vec<Sample> = (0..600)
                .map(|i| {
                    timestamp += if i % 2 == 0 { 1 << 42 } else { 1 };
                    seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1);
                    Sample {
                        timestamp,
                        value: f64::from_bits(seed >> 2),
                    }
                })
                .collect();
            series
                .merge_samples(&sparse, Some(DuplicatePolicy::KeepLast))
                .unwrap();

            // Room in the dense chunk, at the same density.
            series.remove_range(0, dense_end * 9 / 10).unwrap();
            let before = all_samples(&series);

            defrag_series(&mut series).unwrap();

            assert_eq!(all_samples(&series), before, "{encoding}: samples changed");
            for (i, chunk) in series.chunks.iter().enumerate() {
                let max = chunk.max_size();
                assert!(
                    chunk.size() <= max + max / SPLIT_SLACK_DIVISOR,
                    "{encoding}: chunk {i} holds {} bytes on a {max}-byte budget",
                    chunk.size()
                );
            }
            if let Err(e) = check_series_invariants(&series) {
                panic!("{encoding}: {e}");
            }
        }
    }

    /// A `max_size`-byte chunk holding up to `count` samples from `start` (fewer if it fills).
    fn sized_chunk(chimp: bool, max_size: usize, start: i64, count: i64) -> TimeSeriesChunk {
        let mut chunk = if chimp {
            TimeSeriesChunk::Chimp(ChimpChunk::with_max_size(max_size))
        } else {
            TimeSeriesChunk::Gorilla(GorillaChunk::with_max_size(max_size))
        };
        for i in 0..count {
            let timestamp = start + i * 10;
            // Varying values so the chunks compress to realistic, uneven sizes.
            let value = ((timestamp * 7919) % 1000) as f64 / 7.0;
            if chunk.add_sample(&Sample { timestamp, value }).is_err() {
                break;
            }
        }
        chunk
    }

    #[test]
    fn test_defrag_keeps_chunks_ordered_through_partial_merges() {
        // A partial merge leaves the source chunk holding the upper part of its range; the
        // loop used to keep pulling later chunks into the destination past it, leaving the
        // chunks out of order.
        let counts = [3i64, 12, 40, 90, 200];
        for chimp in [false, true] {
            for max_size in [64usize, 128, 256] {
                for &a in &counts {
                    for &b in &counts {
                        for &c in &counts {
                            let chunks = vec![
                                sized_chunk(chimp, max_size, 0, a),
                                sized_chunk(chimp, max_size, 100_000, b),
                                sized_chunk(chimp, max_size, 200_000, c),
                                sized_chunk(chimp, max_size, 300_000, 5),
                            ];
                            let mut series = series_from(chunks);
                            let before = all_samples(&series);

                            defrag_series(&mut series).unwrap();

                            let after = all_samples(&series);
                            assert_eq!(
                                after, before,
                                "samples changed (chimp={chimp}, max_size={max_size}, counts={a},{b},{c})"
                            );
                            for pair in series.chunks.windows(2) {
                                assert!(
                                    pair[0].last_timestamp() < pair[1].first_timestamp(),
                                    "chunks overlap (chimp={chimp}, max_size={max_size}, counts={a},{b},{c})"
                                );
                            }
                        }
                    }
                }
            }
        }
    }
}
