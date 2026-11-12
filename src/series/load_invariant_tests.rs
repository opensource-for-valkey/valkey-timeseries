//! The RDB loader's series checks (`check_series_invariants`, the chunks' `check_write_limits`,
//! and the compressed chunks' `check_header` / `verify_stream`) must accept everything the write
//! path can store. A foreign payload is refused on any of them, and an own one repaired, so a
//! check tighter than the commands turns a series the server saved into one it cannot hand to
//! another node, or rewrites it on every restart. Random command sequences pin that; the forged
//! cases pin what the checks are for.

use crate::common::{Sample, Timestamp};
use crate::series::chunks::{
    Chunk, ChunkEncoding, ChunkOps, MAX_CHUNK_SIZE, MAX_LOADED_CHUNK_BYTES, SampleCodec,
    TimeSeriesChunk, XorChunk,
};
use crate::series::serialization::{
    BUDGET_CLAMPED, CHUNK_SIZE_CLAMPED, CHUNKS_OUT_OF_ORDER, ENCODING_ADOPTED, LoadTrust,
    NEGATIVE_TIMESTAMPS, OVERSIZED_SPLIT, RETENTION_CLAMPED, STRAYS_REENCODED,
    check_series_invariants, drop_empty_chunks, normalize_series,
};
use crate::series::{
    DuplicatePolicy, SPLIT_SLACK_DIVISOR, TimeSeries, bulk_insert_samples, defrag_series,
};
use proptest::prelude::*;
use std::time::Duration;
use valkey_module::Context;

const ENCODINGS: [ChunkEncoding; 3] = [
    ChunkEncoding::Uncompressed,
    ChunkEncoding::Gorilla,
    ChunkEncoding::Chimp,
];

/// What a chunk may hold past `max + max / SPLIT_SLACK_DIVISOR`: the sample whose append
/// crossed its budget. The loader's `MAX_LOADED_CHUNK_BYTES` is twice the largest budget on the
/// strength of this bound, so every writer is held to it, not just to the load limit.
const ONE_SAMPLE_SLACK: usize = 64;

fn new_series(encoding: ChunkEncoding, chunk_size: usize) -> TimeSeries {
    let mut ts = TimeSeries::new();
    ts.chunk_encoding = encoding;
    ts.chunk_size_bytes = chunk_size;
    ts
}

/// Everything the loader checks, as it would see this series after a save; that every chunk
/// stays within its budget; and that each chunk still verifies after a save, a load and more
/// appends.
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
        let max = chunk.max_size();
        assert!(
            chunk.size() <= max + max / SPLIT_SLACK_DIVISOR + ONE_SAMPLE_SLACK,
            "chunk {i} of {} holds {} bytes on a {max}-byte budget",
            ts.chunks.len(),
            chunk.size()
        );
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
        assert_reload_then_append_verifies(i, chunk);
    }
    assert_eq!(total, ts.total_samples);
}

/// `verify_stream` compares a chunk with a from-scratch re-encode of its samples, so a chunk
/// that was saved, loaded and then appended to must encode exactly as one that never stopped.
/// Chimp carries `stored_val`, `last_alpha` and a hint-recheck counter across the hop; if the
/// hop changed any of them, `RESTORE` of a live series would start failing after its next
/// append. Both persisted formats hold the same codec state, so the wire format stands in for
/// the RDB one.
fn assert_reload_then_append_verifies(i: usize, chunk: &TimeSeriesChunk) {
    let mut buf = Vec::new();
    chunk.serialize(&mut buf);
    let mut loaded = TimeSeriesChunk::deserialize(&buf)
        .unwrap_or_else(|e| panic!("chunk {i}: does not load back: {e}"));
    assert!(loaded == *chunk, "chunk {i}: changed by a save and load");
    match &mut loaded {
        TimeSeriesChunk::Uncompressed(_) => {}
        TimeSeriesChunk::Gorilla(c) => append_and_verify(i, c),
        TimeSeriesChunk::Chimp(c) => append_and_verify(i, c),
    }
}

fn append_and_verify<C: SampleCodec>(i: usize, chunk: &mut XorChunk<C>) {
    let (start, last_value) = if chunk.is_empty() {
        (0, 0.0)
    } else {
        (chunk.last_timestamp(), chunk.encoder.last_value())
    };
    for k in 1..=50i64 {
        let Some(timestamp) = start.checked_add(k * 7) else {
            break;
        };
        // A repeat, a short decimal, a long one and an integer: each takes a different path
        // through the value encoders' state.
        let value = match k % 4 {
            0 => last_value,
            1 => k as f64 * 0.25,
            2 => (k as f64).sqrt(),
            _ => (k * 1000) as f64,
        };
        // Past the budget is fine: the encoder state is what is under test.
        chunk.encoder.push(&Sample::new(timestamp, value)).unwrap();
    }
    if let Err(e) = chunk.check_header().and_then(|_| chunk.verify_stream()) {
        panic!("chunk {i}: rejected after a reload and 50 appends: {e}");
    }
}

#[derive(Debug, Clone)]
enum Op {
    /// `TS.ADD` at `last + gap` (or at `gap` on an empty series).
    Append { gap: i64, value: f64 },
    /// `count` appends at a fixed step, constant or noisy: a run of one density.
    Run {
        count: usize,
        step: i64,
        noisy: bool,
    },
    /// `TS.ADD` into the past, `KEEP_LAST` on a collision.
    Backfill { back: i64, value: f64 },
    /// `TS.MADD` of a sorted batch spread over `[start, start + span]`.
    Merge {
        start: i64,
        span: i64,
        count: usize,
        noisy: bool,
    },
    /// `TS.ADDBULK` of the same: the bulk path groups by chunk and plans new chunks itself.
    Bulk {
        start: i64,
        span: i64,
        count: usize,
        noisy: bool,
    },
    /// `TS.DEL` from `at` per mille of the way through the series' span, `width` per mille of
    /// it wide: always where the data is, which absolute offsets often missed.
    Delete { at: u16, width: u16 },
    /// `TS.ALTER CHUNK_SIZE`.
    Alter { chunk_size: usize },
    /// A retention trim, as the cron or a delete runs it.
    Trim { retention: u64 },
    /// Defragmentation, as the module type's `defrag` callback runs it.
    Defrag,
    /// Every chunk saved and loaded; later operations continue on the loaded state.
    Reload,
}

/// Operations whose timestamps are offsets below `max_offset`, with batches of up to
/// `max_count` samples, appended at steps of up to `max_step`, onto chunk budgets drawn from
/// `budgets`.
fn op_with(
    max_offset: i64,
    max_count: usize,
    max_step: i64,
    budgets: Vec<usize>,
) -> impl Strategy<Value = Op> {
    let value = prop_oneof![
        Just(1.5),
        -1e6..1e6f64,
        (0u32..200).prop_map(|n| n as f64),
        Just(f64::NAN),
    ];
    let span = 0..max_offset;
    prop_oneof![
        5 => (1..max_step.min(2_000), value.clone()).prop_map(|(gap, value)| Op::Append { gap, value }),
        2 => (1..3_000usize, prop_oneof![Just(1i64), 1..max_step], any::<bool>())
            .prop_map(|(count, step, noisy)| Op::Run { count, step, noisy }),
        2 => (0i64..50_000, value).prop_map(|(back, value)| Op::Backfill { back, value }),
        2 => (span.clone(), span.clone(), 1..max_count, any::<bool>())
            .prop_map(|(start, span, count, noisy)| Op::Merge { start, span, count, noisy }),
        2 => (span.clone(), span.clone(), 1..max_count, any::<bool>())
            .prop_map(|(start, span, count, noisy)| Op::Bulk { start, span, count, noisy }),
        2 => (0..1000u16, 0..500u16).prop_map(|(at, width)| Op::Delete { at, width }),
        1 => prop::sample::select(budgets).prop_map(|chunk_size| Op::Alter { chunk_size }),
        // Up to the offsets' scale, so a trim keeps part of the data rather than all but the
        // last few samples.
        1 => (1_000u64..(max_offset as u64).max(50_000)).prop_map(|retention| Op::Trim { retention }),
        2 => Just(Op::Defrag),
        1 => Just(Op::Reload),
    ]
}

/// A value for sample `i` of a batch: a cheap ramp, or incompressible bits (finite, so samples
/// compare equal).
fn batch_value(noisy: bool, i: i64) -> f64 {
    if noisy {
        let bits = (i as u64 ^ 0x9e37_79b9_7f4a_7c15).wrapping_mul(6364136223846793005);
        f64::from_bits(bits >> 2)
    } else {
        i as f64 * 0.25
    }
}

/// `count` sorted samples over `[base + start, base + start + span]`, or none if that range
/// leaves `i64`.
fn batch(base: Timestamp, start: i64, span: i64, count: usize, noisy: bool) -> Vec<Sample> {
    let step = (span / count as i64).max(1);
    (0..count as i64)
        .map_while(|i| {
            base.checked_add(start)?
                .checked_add(i * step)
                .map(|t| Sample::new(t, batch_value(noisy, i)))
        })
        .collect()
}

/// Applies `op` with every timestamp offset by `base`.
fn apply(ts: &mut TimeSeries, base: Timestamp, op: &Op) {
    let last = if ts.is_empty() {
        base
    } else {
        ts.last_timestamp()
    };
    match *op {
        Op::Append { gap, value } => {
            if let Some(at) = last.checked_add(gap) {
                ts.add(at, value, None);
            }
        }
        Op::Run { count, step, noisy } => {
            for i in 1..=count as i64 {
                let Some(at) = i.checked_mul(step).and_then(|d| last.checked_add(d)) else {
                    break;
                };
                ts.add(at, if noisy { batch_value(true, i) } else { 1.0 }, None);
            }
        }
        Op::Backfill { back, value } => {
            let at = (last - back).max(base);
            if !ts.is_older_than_retention(at) {
                ts.add(at, value, Some(DuplicatePolicy::KeepLast));
            }
        }
        Op::Merge {
            start,
            span,
            count,
            noisy,
        } => {
            let samples = batch(base, start, span, count, noisy);
            let _ = ts.merge_samples(&samples, Some(DuplicatePolicy::KeepLast));
        }
        Op::Bulk {
            start,
            span,
            count,
            noisy,
        } => {
            let samples = batch(base, start, span, count, noisy);
            if !samples.is_empty() {
                bulk_insert_samples(
                    &Context::dummy(),
                    ts,
                    &samples,
                    Some(DuplicatePolicy::KeepLast),
                );
            }
        }
        Op::Delete { at, width } => {
            if !ts.is_empty() {
                // In `i128`: a span near `i64::MAX` times a per-mille factor overflows `i64`.
                let first = ts.first_timestamp as i128;
                let span = last as i128 - first;
                let from = first + span * at as i128 / 1000;
                let to = from + span * width as i128 / 1000;
                let _ = ts.remove_range(from as Timestamp, to as Timestamp);
            }
        }
        Op::Alter { chunk_size } => ts.chunk_size_bytes = chunk_size,
        Op::Trim { retention } => {
            ts.retention = Duration::from_millis(retention);
            let _ = ts.trim();
        }
        Op::Defrag => {
            let _ = defrag_series(ts);
        }
        Op::Reload => {
            for chunk in ts.chunks.iter_mut() {
                let mut buf = Vec::new();
                chunk.serialize(&mut buf);
                *chunk = TimeSeriesChunk::deserialize(&buf).unwrap();
            }
        }
    }
}

/// Small budgets, so a short sequence fills, splits, merges and defragments many chunks.
fn small_budgets() -> Vec<usize> {
    (6usize..=64).map(|n| n * 8).collect()
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(192))]

    #[test]
    fn command_sequences_stay_loadable(
        encoding in 0usize..3,
        chunk_size in prop::sample::select(small_budgets()),
        ops in prop::collection::vec(op_with(60_000, 1_500, 100_000, small_budgets()), 1..120),
    ) {
        let mut ts = new_series(ENCODINGS[encoding], chunk_size);
        for op in &ops {
            apply(&mut ts, 0, op);
        }
        assert_loadable(&ts);
    }
}

/// Budgets up to `MAX_CHUNK_SIZE`, the chunk sizes the load limit is twice of.
fn large_budgets() -> Vec<usize> {
    vec![48, 4096, 65536, MAX_CHUNK_SIZE]
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(24))]

    /// The same property at the scale where the load limit matters: budgets up to 1 MiB,
    /// batches of up to 100k samples, density switching between operations, and timestamps
    /// either near zero or within 2^40 of `i64::MAX`, where every delta and offset must stay
    /// in range.
    #[test]
    fn large_budgets_stay_loadable(
        encoding in 0usize..3,
        chunk_size in prop::sample::select(large_budgets()),
        near_max in any::<bool>(),
        ops in prop::collection::vec(op_with(1 << 39, 100_000, 1 << 20, large_budgets()), 1..24),
    ) {
        let base = if near_max { i64::MAX - (1 << 40) } else { 0 };
        let mut ts = new_series(ENCODINGS[encoding], chunk_size);
        for op in &ops {
            apply(&mut ts, base, op);
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

fn other_encoding(encoding: ChunkEncoding) -> ChunkEncoding {
    if encoding == ChunkEncoding::Uncompressed {
        ChunkEncoding::Chimp
    } else {
        ChunkEncoding::Uncompressed
    }
}

fn set_budget(chunk: &mut TimeSeriesChunk, max_size: usize) {
    match chunk {
        TimeSeriesChunk::Uncompressed(c) => c.max_size = max_size,
        TimeSeriesChunk::Gorilla(c) => c.max_size = max_size,
        TimeSeriesChunk::Chimp(c) => c.max_size = max_size,
    }
}

/// A chunk past `MAX_LOADED_CHUNK_BYTES`, after `after`: incompressible values at step 1, so
/// a few hundred thousand samples are enough in every encoding.
fn oversized_chunk(encoding: ChunkEncoding, after: i64) -> TimeSeriesChunk {
    let mut seed = 42u64;
    let samples: Vec<Sample> = (1..=300_000)
        .map(|i| {
            seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1);
            // The top two bits clear keep the value finite, so samples compare equal.
            Sample::new(after + i, f64::from_bits(seed >> 2))
        })
        .collect();
    let mut chunk = TimeSeriesChunk::new(encoding, MAX_CHUNK_SIZE);
    chunk.set_data(&samples).unwrap();
    assert!(
        chunk.size() > MAX_LOADED_CHUNK_BYTES,
        "{encoding}: only {} bytes",
        chunk.size()
    );
    chunk
}

fn stored_samples(ts: &TimeSeries) -> Vec<Sample> {
    ts.chunks.iter().flat_map(|chunk| chunk.iter()).collect()
}

/// What an own load does with a series a foreign load refuses.
enum OwnLoad {
    /// Brought back within these rules, keeping every sample.
    Repaired(&'static [&'static str]),
    /// Loaded unchanged although it breaks this rule.
    AsIs(&'static str),
}

/// Every write-path rule refuses a foreign payload. A payload the server wrote itself is
/// repaired where a local repair exists, and otherwise loaded as it is: a refusal there fails
/// the whole RDB load, which is what a `TS.ADDBULK` timestamp past `i64::MAX` once did.
#[test]
fn forged_series_are_refused_when_foreign_and_kept_when_own() {
    use OwnLoad::*;
    for encoding in ENCODINGS {
        let base = loaded_base(encoding);
        let last = base.last_timestamp();
        let mut cases: Vec<(&str, TimeSeries, OwnLoad)> = Vec::new();

        let mut ts = base.clone();
        ts.chunks.reverse();
        cases.push(("descending chunks", ts, AsIs(CHUNKS_OUT_OF_ORDER)));

        let mut ts = base.clone();
        let copy = ts.chunks.clone();
        ts.chunks.extend(copy);
        cases.push(("every chunk twice", ts, AsIs(CHUNKS_OUT_OF_ORDER)));

        let mut ts = base.clone();
        let dup = ts.chunks[1].clone();
        ts.chunks.insert(1, dup);
        cases.push(("one chunk twice", ts, AsIs(CHUNKS_OUT_OF_ORDER)));

        let mut ts = base.clone();
        let mut negative = TimeSeriesChunk::new(encoding, 128);
        negative.add_sample(&Sample::new(-101, 1.0)).unwrap();
        ts.chunks.insert(0, negative);
        cases.push(("a negative timestamp", ts, AsIs(NEGATIVE_TIMESTAMPS)));

        for size in [0, 7, 40, MAX_CHUNK_SIZE + 8, usize::MAX] {
            let mut ts = base.clone();
            ts.chunk_size_bytes = size;
            cases.push((
                "chunk size out of range",
                ts,
                Repaired(&[CHUNK_SIZE_CLAMPED]),
            ));
        }

        let mut ts = base.clone();
        ts.retention = Duration::from_millis(u64::MAX);
        cases.push(("retention past i64", ts, Repaired(&[RETENTION_CLAMPED])));

        let mut ts = base.clone();
        let mut stray = TimeSeriesChunk::new(other_encoding(encoding), 128);
        stray.add_sample(&Sample::new(last + 1000, 1.0)).unwrap();
        ts.chunks.push(stray);
        cases.push((
            "a chunk of another encoding",
            ts,
            Repaired(&[STRAYS_REENCODED]),
        ));

        let mut ts = base.clone();
        set_budget(&mut ts.chunks[0], MAX_CHUNK_SIZE + 8);
        cases.push((
            "a chunk budget past the limit",
            ts,
            Repaired(&[BUDGET_CLAMPED]),
        ));

        // The budget is clamped before the split: `needs_split` adds a quarter of it to itself.
        let mut ts = base.clone();
        let mut big = oversized_chunk(encoding, last);
        set_budget(&mut big, usize::MAX);
        ts.chunks.push(big);
        ts.recalculate_total_samples();
        ts.update_first_last_timestamps();
        cases.push((
            "an oversized chunk with a usize::MAX budget",
            ts,
            Repaired(&[BUDGET_CLAMPED, OVERSIZED_SPLIT]),
        ));

        for (what, ts, own) in cases {
            assert!(
                normalize_series(&mut ts.clone(), LoadTrust::Foreign).is_err(),
                "{encoding}: a foreign load accepted {what}"
            );
            let mut loaded = ts.clone();
            let repairs = normalize_series(&mut loaded, LoadTrust::Own)
                .unwrap_or_else(|e| panic!("{encoding}: an own load refused {what}: {e}"));
            assert_eq!(
                stored_samples(&loaded),
                stored_samples(&ts),
                "{encoding}: {what} lost samples"
            );
            match own {
                Repaired(rules) => {
                    assert_eq!(repairs.repaired, rules, "{encoding}: {what}");
                    assert!(repairs.tolerated.is_empty(), "{encoding}: {what}");
                    assert_loadable(&loaded);
                    // Visible, too: a clamp that hid samples would pass the checks above.
                    assert_eq!(
                        loaded.get_range(0, i64::MAX),
                        stored_samples(&ts),
                        "{encoding}: {what}"
                    );
                }
                AsIs(rule) => {
                    assert_eq!(repairs.tolerated, [rule], "{encoding}: {what}");
                    assert!(repairs.repaired.is_empty(), "{encoding}: {what}");
                    assert!(loaded == ts, "{encoding}: {what} was changed");
                }
            }
        }
    }
}

/// A series' encoding is only the hint new chunks are created from, so one that disagrees with
/// chunks that all agree is taken from them, on any load.
#[test]
fn a_series_encoding_its_chunks_disagree_with_is_taken_from_them() {
    for encoding in ENCODINGS {
        for trust in [LoadTrust::Foreign, LoadTrust::Own] {
            let mut ts = loaded_base(encoding);
            ts.chunk_encoding = other_encoding(encoding);
            let repairs = normalize_series(&mut ts, trust).unwrap();
            assert_eq!(repairs.repaired, [ENCODING_ADOPTED], "{encoding} {trust:?}");
            assert_eq!(ts.chunk_encoding, encoding);
            assert_loadable(&ts);
        }
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
