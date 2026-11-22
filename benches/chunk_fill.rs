//! Filling one fresh chunk from a run of in-order samples until it is full: the per-chunk loop
//! of bulk ingest (`bulk_add::fill_chunks`, via `ChunkOps::append_until_full`). The input is
//! longer than a chunk holds, so every fill stops on capacity, as it does mid-batch.
use criterion::{BatchSize, BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use std::hint::black_box;
use valkey_timeseries::series::chunks::{ChunkEncoding, ChunkOps, TimeSeriesChunk};

mod support;

use support::{DatasetKey, TimestampModel, ValueWorkload};

fn keys() -> [DatasetKey; 5] {
    [
        DatasetKey::new(ValueWorkload::Constant, TimestampModel::Regular),
        DatasetKey::new(ValueWorkload::Counter, TimestampModel::Regular),
        DatasetKey::new(ValueWorkload::Drift, TimestampModel::Regular),
        DatasetKey::new(ValueWorkload::Noisy, TimestampModel::Regular),
        DatasetKey::new(ValueWorkload::Noisy, TimestampModel::Irregular),
    ]
}

fn bench_chunk_fill(c: &mut Criterion) {
    let mut group = c.benchmark_group("chunk_fill");
    let registry = support::DatasetRegistry::new();

    for key in keys() {
        let dataset = registry.dataset(key);
        for encoding in [
            ChunkEncoding::Uncompressed,
            ChunkEncoding::Gorilla,
            ChunkEncoding::Chimp,
        ] {
            for chunk_size in [support::CHUNK_SIZE_4K, support::CHUNK_SIZE_64K] {
                let fits = support::filled_prefix(dataset, encoding, chunk_size).len();
                let input = &dataset[..(fits + 64).min(dataset.len())];
                let id = format!(
                    "{}/{}/{}",
                    encoding.name(),
                    key.id(),
                    support::chunk_size_id(chunk_size)
                );
                group.throughput(Throughput::Elements(fits as u64));
                group.bench_with_input(BenchmarkId::from_parameter(&id), &input, |b, input| {
                    b.iter_batched(
                        || TimeSeriesChunk::new(encoding, chunk_size),
                        |mut chunk| {
                            let (n, err) = chunk.append_until_full(input);
                            assert!(err.is_none() && n == fits);
                            black_box(chunk)
                        },
                        BatchSize::SmallInput,
                    )
                });
            }
        }
    }
    group.finish();
}

criterion_group!(benches, bench_chunk_fill);
criterion_main!(benches);
