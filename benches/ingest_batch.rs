//! `TS.MADD`'s per-series merge (`multi_series_merge_samples` on one group): normalization,
//! in-batch repeat detection and the chunk merge, for in-order appends and for shuffled
//! back-fills into a populated series. No compaction rule, so no server context is needed.
use criterion::{BatchSize, BenchmarkId, Criterion, criterion_group, criterion_main};
use std::hint::black_box;
use valkey_timeseries::common::{Sample, Timestamp};
use valkey_timeseries::series::{PerSeriesSamples, TimeSeries, multi_series_merge_samples};

const SEED_SAMPLES: i64 = 20_000;
const STEP: Timestamp = 10;

fn seeded() -> TimeSeries {
    let mut series = TimeSeries::new();
    for i in 0..SEED_SAMPLES {
        // Odd slots only, so a back-fill onto the even slots inserts rather than upserts.
        series.add(i * 2 * STEP + STEP, i as f64, None);
    }
    series
}

/// A deterministic shuffle (an LCG-driven Fisher-Yates), so runs compare.
fn shuffled(mut timestamps: Vec<Timestamp>) -> Vec<Timestamp> {
    let mut state = 0x9E37_79B9_7F4A_7C15u64;
    for i in (1..timestamps.len()).rev() {
        state = state
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        let j = (state >> 33) as usize % (i + 1);
        timestamps.swap(i, j);
    }
    timestamps
}

fn bench_madd_merge(c: &mut Criterion) {
    let mut group = c.benchmark_group("madd_merge");
    let base = seeded();
    let last = base.last_sample.map_or(0, |s| s.timestamp);

    for batch in [16usize, 256, 4096] {
        let append: Vec<Timestamp> = (1..=batch as i64).map(|i| last + i * STEP).collect();
        let backfill = shuffled((0..batch as i64).map(|i| i * 2 * STEP).collect());
        // The back-fill with every timestamp twice, which TS.MADD applies item by item.
        let mut repeats = backfill.clone();
        repeats.truncate(batch / 2);
        repeats.extend_from_within(..);
        let repeats = shuffled(repeats);

        for (case, timestamps) in [
            ("append_sorted", &append),
            ("backfill_shuffled", &backfill),
            ("backfill_repeats", &repeats),
        ] {
            group.bench_with_input(
                BenchmarkId::new(case, batch),
                timestamps,
                |b, timestamps| {
                    b.iter_batched(
                        || base.clone(),
                        |mut series| {
                            let mut group = PerSeriesSamples::new(&mut series);
                            for (index, &ts) in timestamps.iter().enumerate() {
                                group.add_sample(Sample::new(ts, index as f64), index);
                            }
                            black_box(multi_series_merge_samples(vec![group], None));
                            series
                        },
                        BatchSize::LargeInput,
                    );
                },
            );
        }
    }
    group.finish();
}

criterion_group!(benches, bench_madd_merge);
criterion_main!(benches);
