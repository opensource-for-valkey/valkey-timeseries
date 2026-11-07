//! Per-sample cost of the bucket aggregators and of the GROUPBY partial reducers, isolated from
//! decoding and reply building. Gate for numerical changes to the accumulators (overflow handling,
//! dispersion precision): save a baseline before, compare after.
use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use std::hint::black_box;
use valkey_timeseries::aggregators::{
    AggregationHandler, AggregationType, Aggregator, PartialReducer, PartialSampleReducer,
};
use valkey_timeseries::common::Sample;
use valkey_timeseries::series::request_types::AggregatorConfig;

const SAMPLES: usize = 100_000;
const BUCKET: usize = 1000;

/// A gauge wandering around 500 with sub-unit noise: realistic magnitudes, no overflow.
fn values() -> Vec<f64> {
    let mut state = 0x9E37_79B9_7F4A_7C15_u64;
    let mut level = 500.0;
    (0..SAMPLES)
        .map(|_| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            level += ((state >> 11) as f64 / (1u64 << 53) as f64) - 0.5;
            level
        })
        .collect()
}

const TYPES: &[(&str, AggregationType)] = &[
    ("avg", AggregationType::Avg),
    ("sum", AggregationType::Sum),
    ("var.p", AggregationType::VarP),
    ("std.s", AggregationType::StdS),
];

fn bucket_aggregators(c: &mut Criterion) {
    let values = values();
    let mut group = c.benchmark_group("aggregators/bucket");
    group.throughput(Throughput::Elements(SAMPLES as u64));
    for &(name, ty) in TYPES {
        group.bench_function(BenchmarkId::from_parameter(name), |b| {
            let mut agg = Aggregator::try_from(ty).unwrap();
            b.iter(|| {
                let mut acc = 0.0;
                for (bucket, chunk) in values.chunks(BUCKET).enumerate() {
                    for (i, &value) in chunk.iter().enumerate() {
                        agg.update((bucket * BUCKET + i) as i64, black_box(value));
                    }
                    acc += agg.finalize();
                }
                black_box(acc)
            });
        });
    }
    group.finish();
}

fn partial_reducers(c: &mut Criterion) {
    // The shard side groups an ascending stream by timestamp, as GROUPBY's per-series rows
    // arrive: here BUCKET rows share each timestamp. Two halves per timestamp stand in for two
    // shards and are merged afterwards.
    let values = values();
    let rows = |half: usize| -> Vec<Sample> {
        values
            .chunks(BUCKET)
            .enumerate()
            .flat_map(|(bucket, chunk)| {
                let (left, right) = chunk.split_at(BUCKET / 2);
                let part = if half == 0 { left } else { right };
                part.iter()
                    .map(move |&value| Sample::new(bucket as i64, value))
            })
            .collect()
    };
    let shards = [rows(0), rows(1)];
    let mut group = c.benchmark_group("aggregators/partial");
    group.throughput(Throughput::Elements(SAMPLES as u64));
    for &(name, ty) in TYPES {
        let config = AggregatorConfig::new(ty, None).unwrap();
        let template = PartialReducer::for_config(&config).unwrap();
        let kind = template.kind();
        group.bench_function(BenchmarkId::from_parameter(name), |b| {
            b.iter(|| {
                let [left, right] = shards
                    .each_ref()
                    .map(|rows| PartialSampleReducer::new(rows.iter().copied(), template.clone()));
                let mut acc = 0.0;
                for ((_, mut merged), (_, other)) in left.zip(right) {
                    PartialReducer::merge(kind, &mut merged, &other);
                    acc += PartialReducer::finalize(kind, &merged);
                }
                black_box(acc)
            });
        });
    }
    group.finish();
}

criterion_group!(benches, bucket_aggregators, partial_reducers);
criterion_main!(benches);
