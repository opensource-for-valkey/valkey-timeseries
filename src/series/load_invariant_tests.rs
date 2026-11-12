//! The RDB loader's series checks (`check_series_invariants`, the chunks' `check_write_limits`,
//! and the compressed chunks' `check_header` / `verify_stream`) must accept everything the write
//! path can store: a load
//! error fails a whole RDB load, so a check tighter than the commands makes a server that saved
//! fine unable to restart. Random command sequences pin that, and the forged cases pin what the
//! checks are for.

use crate::common::Sample;
use crate::series::chunks::{
    ChunkEncoding, ChunkOps, MAX_CHUNK_SIZE, MAX_LOADED_CHUNK_BYTES, TimeSeriesChunk,
};
use crate::series::serialization::{check_series_invariants, drop_empty_chunks};
use crate::series::{DuplicatePolicy, TimeSeries};
use proptest::prelude::*;
use std::time::Duration;

const ENCODINGS: [ChunkEncoding; 3] = [
    ChunkEncoding::Uncompressed,
    ChunkEncoding::Gorilla,
    ChunkEncoding::Chimp,
];

fn new_series(encoding: ChunkEncoding, chunk_size: usize) -> TimeSeries {
    let mut ts = TimeSeries::new();
    ts.chunk_encoding = encoding;
    ts.chunk_size_bytes = chunk_size;
    ts
}

/// Everything the loader checks, as it would see this series after a save.
fn assert_loadable(ts: &TimeSeries) {
    if let Err(e) = check_series_invariants(ts) {
        panic!("series rejected: {e}");
    }
    let mut total = 0usize;
    for (i, chunk) in ts.chunks.iter().enumerate() {
        total += chunk.len();
        if let Err(e) = chunk.check_write_limits() {
            panic!(
                "chunk {i} of {} outside the write limits: {e}",
                ts.chunks.len()
            );
        }
        let checked = match chunk {
            TimeSeriesChunk::Uncompressed(c) => {
                assert!(
                    c.samples
                        .windows(2)
                        .all(|w| w[0].timestamp < w[1].timestamp)
                );
                Ok(())
            }
            TimeSeriesChunk::Gorilla(c) => c.check_header().and_then(|_| c.verify_stream()),
            TimeSeriesChunk::Chimp(c) => c.check_header().and_then(|_| c.verify_stream()),
        };
        if let Err(e) = checked {
            panic!("chunk {i} of {} rejected: {e}", ts.chunks.len());
        }
    }
    assert_eq!(total, ts.total_samples);
}

#[derive(Debug, Clone)]
enum Op {
    /// `TS.ADD` at `last + gap` (or at `gap` on an empty series).
    Append { gap: i64, value: f64 },
    /// `TS.ADD` into the past, `KEEP_LAST` on a collision.
    Backfill { back: i64, value: f64 },
    /// `TS.MADD` of a sorted batch spread over `[start, start + span]`.
    Merge { start: i64, span: i64, count: usize },
    /// `TS.DEL` over `[start, start + span]`.
    Delete { start: i64, span: i64 },
    /// `TS.ALTER CHUNK_SIZE`.
    Alter { chunk_size: usize },
    /// A retention trim, as the cron or a delete runs it.
    Trim { retention: u64 },
}

fn op() -> impl Strategy<Value = Op> {
    let value = prop_oneof![
        Just(1.5),
        -1e6..1e6f64,
        (0u32..200).prop_map(|n| n as f64),
        Just(f64::NAN),
    ];
    prop_oneof![
        6 => (1i64..2_000, value.clone()).prop_map(|(gap, value)| Op::Append { gap, value }),
        2 => (0i64..50_000, value).prop_map(|(back, value)| Op::Backfill { back, value }),
        2 => (0i64..60_000, 0i64..60_000, 1usize..1_500)
            .prop_map(|(start, span, count)| Op::Merge { start, span, count }),
        1 => (0i64..60_000, 0i64..10_000).prop_map(|(start, span)| Op::Delete { start, span }),
        1 => (6usize..=64).prop_map(|n| Op::Alter { chunk_size: n * 8 }),
        1 => (1_000u64..50_000).prop_map(|retention| Op::Trim { retention }),
    ]
}

fn apply(ts: &mut TimeSeries, op: &Op) {
    match *op {
        Op::Append { gap, value } => {
            let at = if ts.is_empty() {
                gap
            } else {
                ts.last_timestamp() + gap
            };
            ts.add(at, value, None);
        }
        Op::Backfill { back, value } => {
            let at = (ts.last_timestamp() - back).max(0);
            if !ts.is_older_than_retention(at) {
                ts.add(at, value, Some(DuplicatePolicy::KeepLast));
            }
        }
        Op::Merge { start, span, count } => {
            let step = (span / count as i64).max(1);
            let samples: Vec<Sample> = (0..count as i64)
                .map(|i| Sample::new(start + i * step, i as f64 * 0.25))
                .collect();
            let _ = ts.merge_samples(&samples, Some(DuplicatePolicy::KeepLast));
        }
        Op::Delete { start, span } => {
            let _ = ts.remove_range(start, start + span);
        }
        Op::Alter { chunk_size } => ts.chunk_size_bytes = chunk_size,
        Op::Trim { retention } => {
            ts.retention = Duration::from_millis(retention);
            let _ = ts.trim();
        }
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(96))]

    #[test]
    fn command_sequences_stay_loadable(
        encoding in 0usize..3,
        chunk_size in (6usize..=64).prop_map(|n| n * 8),
        ops in prop::collection::vec(op(), 1..120),
    ) {
        let mut ts = new_series(ENCODINGS[encoding], chunk_size);
        for op in &ops {
            apply(&mut ts, op);
        }
        assert_loadable(&ts);
    }
}

/// A `TS.MADD` that backfills many samples into one chunk's span grew that chunk to many times
/// its budget, because the merge path halved it only once. The loader's chunk-size limit needs
/// the write path to keep every chunk near its budget.
#[test]
fn one_merge_cannot_grow_a_chunk_past_its_budget() {
    for encoding in ENCODINGS {
        let mut ts = new_series(encoding, 256);
        ts.add(0, 1.0, None);
        ts.add(10_000_000, 2.0, None);
        let samples: Vec<Sample> = (1..100_000)
            .map(|i| Sample::new(i * 10, i as f64))
            .collect();
        ts.merge_samples(&samples, None).unwrap();
        for chunk in &ts.chunks {
            let max = chunk.max_size();
            assert!(
                chunk.size() <= max + max / 4 + 64,
                "{encoding}: {} bytes in a {max}-byte chunk",
                chunk.size()
            );
        }
        assert_loadable(&ts);
    }
}

fn loaded_base(encoding: ChunkEncoding) -> TimeSeries {
    let mut ts = new_series(encoding, 128);
    for i in 1..=200 {
        ts.add(i * 1000, i as f64 * 1.5, None);
    }
    assert!(ts.chunks.len() > 2, "{encoding}: needs several chunks");
    assert_loadable(&ts);
    ts
}

#[test]
fn forged_series_are_rejected() {
    for encoding in ENCODINGS {
        let base = loaded_base(encoding);
        let rejected = |what: &str, ts: TimeSeries| {
            assert!(
                check_series_invariants(&ts).is_err(),
                "{encoding}: accepted {what}"
            );
        };

        let mut ts = base.clone();
        ts.chunks.reverse();
        rejected("descending chunks", ts);

        let mut ts = base.clone();
        let copy = ts.chunks.clone();
        ts.chunks.extend(copy);
        rejected("every chunk twice", ts);

        let mut ts = base.clone();
        let dup = ts.chunks[1].clone();
        ts.chunks.insert(1, dup);
        rejected("one chunk twice", ts);

        for size in [0, 7, 40, MAX_CHUNK_SIZE + 8, usize::MAX] {
            let mut ts = base.clone();
            ts.chunk_size_bytes = size;
            rejected("chunk size out of range", ts);
        }

        let mut ts = base.clone();
        ts.retention = Duration::from_millis(u64::MAX);
        rejected("retention past i64", ts);

        let other = if encoding == ChunkEncoding::Uncompressed {
            ChunkEncoding::Chimp
        } else {
            ChunkEncoding::Uncompressed
        };
        let mut ts = base.clone();
        let mut stray = TimeSeriesChunk::new(other, 128);
        stray.add_sample(&Sample::new(1_000_000, 1.0)).unwrap();
        ts.chunks.push(stray);
        rejected("a chunk of another encoding", ts);

        let mut ts = base.clone();
        let mut negative = TimeSeriesChunk::new(encoding, 128);
        negative.add_sample(&Sample::new(-101, 1.0)).unwrap();
        ts.chunks.insert(0, negative);
        rejected("a negative timestamp", ts);
    }
}

/// An empty chunk reports bounds of zero, which breaks the ordering the chunk searches assume
/// once a series has more than 16 chunks: deletes and bounded reads of existing samples found
/// nothing. Commands only ever leave a sole empty chunk, so the loader drops every other one.
#[test]
fn empty_chunks_are_dropped_on_load() {
    for encoding in ENCODINGS {
        assert_loadable(&new_series(encoding, 128));

        let mut base = new_series(encoding, 128);
        let mut t = 0;
        // More than `LINEAR_SCAN_MAX` chunks, so the lookups below binary-search.
        while base.chunks.len() <= 24 {
            t += 1000;
            base.add(t, t as f64 * 0.37, None);
        }
        let mut ts = base.clone();
        let middle = ts.chunks.len() / 2;
        ts.chunks
            .insert(middle, TimeSeriesChunk::new(encoding, 128));
        ts.chunks.insert(0, TimeSeriesChunk::new(encoding, 128));
        ts.chunks.push(TimeSeriesChunk::new(encoding, 128));
        assert!(
            check_series_invariants(&ts).is_err(),
            "{encoding}: not the loaded form"
        );

        assert!(drop_empty_chunks(&mut ts), "{encoding}");
        assert_eq!(ts.chunks.len(), base.chunks.len(), "{encoding}");
        assert_loadable(&ts);
        for sample in base.iter() {
            let t = sample.timestamp;
            assert_eq!(ts.get_range(t, t), vec![sample], "{encoding}: read at {t}");
            assert_eq!(
                ts.clone().remove_range(t, t).unwrap(),
                1,
                "{encoding}: delete at {t}"
            );
        }

        // A sole empty chunk is what a series that never kept a sample looks like: kept.
        let mut sole = new_series(encoding, 128);
        sole.chunks.push(TimeSeriesChunk::new(encoding, 128));
        assert!(!drop_empty_chunks(&mut sole));
        assert_eq!(sole.chunks.len(), 1);
        assert_loadable(&sole);

        // Several, and nothing else: none, which `get_last_chunk` already handles.
        let mut several = new_series(encoding, 128);
        several.chunks.push(TimeSeriesChunk::new(encoding, 128));
        several.chunks.push(TimeSeriesChunk::new(encoding, 128));
        assert!(drop_empty_chunks(&mut several));
        assert!(several.chunks.is_empty());
        assert_loadable(&several);
    }
}

#[test]
fn oversized_chunks_are_rejected() {
    // A valid constant Gorilla chunk at step 1 costs ~2 bits a sample.
    let mut chunk = TimeSeriesChunk::new(ChunkEncoding::Gorilla, MAX_CHUNK_SIZE);
    let mut ts = 0;
    while chunk.size() <= MAX_LOADED_CHUNK_BYTES {
        for _ in 0..4096 {
            chunk.add_sample(&Sample::new(ts, 1.0)).unwrap();
            ts += 1;
        }
    }
    let TimeSeriesChunk::Gorilla(gorilla) = &chunk else {
        unreachable!()
    };
    assert!(
        gorilla.verify_stream().is_ok(),
        "the stream itself is honest"
    );
    // The densest honest stream still fits the count bound: `MIN_SAMPLE_BITS` is not too high.
    assert!(gorilla.check_header().is_ok());
    assert!(gorilla.check_write_limits().is_err());

    let mut series = new_series(ChunkEncoding::Gorilla, MAX_CHUNK_SIZE);
    series.chunks.push(chunk);
    series.recalculate_total_samples();
    assert!(check_series_invariants(&series).is_err());
}
