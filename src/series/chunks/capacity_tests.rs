//! Chunk capacity: `SampleCodec::MAX_SAMPLE_BYTES` bounds every push, and a chunk of at least
//! `HEADROOM_MIN_CHUNK_SIZE` bytes filled up to `is_full` ends at or under `max_size`, so its
//! sealed allocation stays in the allocator size class of `max_size`. Ending a few bytes over
//! put every sealed 4,096-byte chunk in jemalloc's 5,120-byte class. Smaller chunks fill to
//! `max_size` and end less than one sample past it.

use crate::common::Sample;
use crate::series::chunks::{
    ChimpCompressor, ChunkOps, GorillaEncoder, HEADROOM_MIN_CHUNK_SIZE, MIN_CHUNK_SIZE,
    SampleCodec, XorChunk,
};
use crate::tests::generators::{DataGenerator, ValueWorkload};
use get_size2::GetSize;
use rand::rngs::StdRng;
use rand::{RngExt, SeedableRng};
use std::time::Duration;

/// Gaps that drive every delta-of-delta bucket, the widest included: alternating 1 and 2^56
/// is a delta-of-delta past 2^55, the 72-bit Gorilla case.
const GAPS: [i64; 7] = [1, 2, 1_000, 1 << 20, 1 << 33, 1 << 52, 1 << 56];

/// Values whose XOR against the last one is as wide as it gets: random bit patterns (NaN
/// payloads and subnormals among them), their complements and short decimals.
fn hostile_value(rng: &mut StdRng, last: f64) -> f64 {
    match rng.random_range(0..6) {
        0 => f64::from_bits(rng.random::<u64>()),
        1 => f64::from_bits(!last.to_bits()),
        2 => f64::from_bits(last.to_bits() ^ 0x8000_0000_0000_0001),
        3 => (rng.random_range(-1_000_000..1_000_000) as f64) / 100.0,
        4 => f64::from_bits(rng.random::<u64>() | 1),
        _ => last,
    }
}

/// Largest growth of `bytes()` over any single push of a hostile stream, restarting the
/// codec whenever the next timestamp would overflow.
fn max_push_growth<C: SampleCodec>(seed: u64, pushes: usize) -> usize {
    let mut rng = StdRng::seed_from_u64(seed);
    let mut codec = C::with_soft_cap(usize::MAX);
    let mut ts: i64 = rng.random_range(0..1 << 40);
    let mut value = 0.0;
    let mut worst = 0;
    for _ in 0..pushes {
        let gap = GAPS[rng.random_range(0..GAPS.len())];
        match ts.checked_add(gap) {
            Some(next) if next > 0 => ts = next,
            _ => {
                codec = C::with_soft_cap(usize::MAX);
                ts = rng.random_range(0..1 << 40);
            }
        }
        value = hostile_value(&mut rng, value);
        let before = codec.bytes().len();
        codec.push(&Sample::new(ts, value)).unwrap();
        worst = worst.max(codec.bytes().len() - before);
    }
    worst
}

/// The widest single samples, built directly: a first sample at a timestamp with a 10-byte
/// varint, then a second sample a near-`i64::MAX` delta later, then alternating 1 and 2^56
/// gaps with full-width XORs.
fn max_growth_crafted<C: SampleCodec>() -> usize {
    let mut worst = 0;
    for first_ts in [0i64, 1 << 62, i64::MAX / 2] {
        let mut codec = C::with_soft_cap(usize::MAX);
        let mut ts = first_ts;
        let mut value = f64::from_bits(0x7ff8_dead_beef_0001);
        let mut push = |codec: &mut C, ts: i64, value: f64| {
            let before = codec.bytes().len();
            codec.push(&Sample::new(ts, value)).unwrap();
            worst = worst.max(codec.bytes().len() - before);
        };
        push(&mut codec, ts, value);
        let second = ts.saturating_add(i64::MAX - ts - (64 << 56));
        if second > ts {
            ts = second;
            value = f64::from_bits(!value.to_bits());
            push(&mut codec, ts, value);
        }
        for i in 0..64 {
            let gap = if i % 2 == 0 { 1 } else { 1 << 56 };
            let Some(next) = ts.checked_add(gap) else {
                break;
            };
            ts = next;
            value = f64::from_bits(!value.to_bits() ^ (i as u64));
            push(&mut codec, ts, value);
        }
    }
    worst
}

fn assert_push_bound<C: SampleCodec>() {
    let mut worst = max_growth_crafted::<C>();
    for seed in 0..16 {
        worst = worst.max(max_push_growth::<C>(seed, 4_000));
    }
    assert!(
        worst <= C::MAX_SAMPLE_BYTES,
        "{}: a push grew the stream by {worst} bytes, MAX_SAMPLE_BYTES is {}",
        C::ENCODING.name(),
        C::MAX_SAMPLE_BYTES
    );
    // A loose bound would leave every chunk short for nothing.
    assert!(
        worst + 2 >= C::MAX_SAMPLE_BYTES,
        "{}: the widest push seen is {worst} bytes, MAX_SAMPLE_BYTES {} looks loose",
        C::ENCODING.name(),
        C::MAX_SAMPLE_BYTES
    );
}

#[test]
fn max_sample_bytes_bounds_every_chimp_push() {
    assert_push_bound::<ChimpCompressor>();
}

#[test]
fn max_sample_bytes_bounds_every_gorilla_push() {
    assert_push_bound::<GorillaEncoder>();
}

/// Fills a chunk the way the series does (append until `is_full`) and seals it.
fn fill_and_seal<C: SampleCodec>(max_size: usize, samples: &[Sample]) -> XorChunk<C> {
    let mut chunk = XorChunk::<C>::with_max_size(max_size);
    for sample in samples {
        if chunk.is_full() {
            break;
        }
        chunk.add_sample(sample).unwrap();
    }
    assert!(
        chunk.is_full(),
        "{max_size}: ran out of samples before the chunk filled"
    );
    chunk.optimize().unwrap();
    chunk
}

fn workload_samples(workload: ValueWorkload, max_size: usize) -> Vec<Sample> {
    DataGenerator::builder()
        .start(1_700_000_000_000)
        .interval(Duration::from_millis(1000))
        .algorithm(workload)
        .samples(max_size * 8 + 64)
        .seed(11)
        .build()
        .generate()
}

fn assert_fill_stays_in_budget<C: SampleCodec>() {
    for workload in ValueWorkload::workloads() {
        for max_size in [HEADROOM_MIN_CHUNK_SIZE, 1024, 4000, 4096, 8192] {
            let samples = workload_samples(*workload, max_size);
            let chunk = fill_and_seal::<C>(max_size, &samples);
            let what = format!("{} {} {max_size}", C::ENCODING.name(), workload.id());
            assert!(
                chunk.data_size() <= max_size,
                "{what}: filled to {} bytes",
                chunk.data_size()
            );
            assert!(
                chunk.data_size() + C::MAX_SAMPLE_BYTES > max_size,
                "{what}: sealed at {} bytes, short of full",
                chunk.data_size()
            );
            let heap = chunk.encoder.get_heap_size();
            assert!(heap <= max_size, "{what}: sealed allocation {heap} bytes");
        }
    }
}

#[test]
fn chimp_chunks_fill_within_max_size() {
    assert_fill_stays_in_budget::<ChimpCompressor>();
}

#[test]
fn gorilla_chunks_fill_within_max_size() {
    assert_fill_stays_in_budget::<GorillaEncoder>();
}

/// Under `HEADROOM_MIN_CHUNK_SIZE` the headroom would cost more samples than the size class it
/// saves (1.7x the bytes per sample at 48), so a chunk fills until the payload reaches
/// `max_size` and ends less than one sample past it, its allocation within the bit stream's
/// slack.
fn assert_small_chunks_fill_to_max_size<C: SampleCodec>() {
    for workload in ValueWorkload::workloads() {
        for max_size in [MIN_CHUNK_SIZE, 64, 128, HEADROOM_MIN_CHUNK_SIZE - 8] {
            let samples = workload_samples(*workload, max_size);
            let mut chunk = XorChunk::<C>::with_max_size(max_size);
            for sample in &samples {
                if chunk.is_full() {
                    break;
                }
                chunk.add_sample(sample).unwrap();
                let heap = chunk.encoder.get_heap_size();
                // 32 is `BitStream`'s `SOFT_CAP_SLACK`: no doubling past the budget.
                assert!(heap <= max_size + 32, "{max_size}: allocation {heap} bytes");
            }
            let what = format!("{} {} {max_size}", C::ENCODING.name(), workload.id());
            let size = chunk.data_size();
            assert!(
                size >= max_size,
                "{what}: sealed at {size} bytes, short of full"
            );
            assert!(
                size < max_size + C::MAX_SAMPLE_BYTES,
                "{what}: filled to {size} bytes"
            );
        }
    }
}

#[test]
fn chimp_small_chunks_fill_to_max_size() {
    assert_small_chunks_fill_to_max_size::<ChimpCompressor>();
}

#[test]
fn gorilla_small_chunks_fill_to_max_size() {
    assert_small_chunks_fill_to_max_size::<GorillaEncoder>();
}

/// The active chunk too: while filling, its allocation never goes past `max_size`.
#[test]
fn filling_chunk_allocation_stays_within_max_size() {
    let samples = DataGenerator::builder()
        .start(1_700_000_000_000)
        .interval(Duration::from_millis(1000))
        .algorithm(ValueWorkload::Noisy)
        .samples(4_000)
        .seed(3)
        .build()
        .generate();
    let mut chunk = XorChunk::<ChimpCompressor>::with_max_size(4096);
    for sample in &samples {
        if chunk.is_full() {
            break;
        }
        chunk.add_sample(sample).unwrap();
        let heap = chunk.encoder.get_heap_size();
        assert!(
            heap <= 4096,
            "allocation {heap} at {} bytes",
            chunk.data_size()
        );
    }
    assert!(chunk.is_full());
}
