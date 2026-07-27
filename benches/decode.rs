use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use std::time::Duration;
use valkey_timeseries::series::chunks::{ChunkEncoding, ChunkOps};

mod support;

use support::{
    CHUNK_SIZE_1K, CHUNK_SIZE_4K, CHUNK_SIZE_64K, DatasetKey, DatasetRegistry, ValueWorkload,
    build_chunk, chunk_size_id,
};

fn encodings() -> [ChunkEncoding; 2] {
    [
        ChunkEncoding::Uncompressed,
        ChunkEncoding::Gorilla,
    ]
}

/// Mirror encode.rs: drift and noisy get all three chunk sizes; everything
/// else gets only the 4 KiB default.
fn bench_chunk_sizes(key: DatasetKey) -> Vec<usize> {
    match key.workload {
        ValueWorkload::Drift | ValueWorkload::Noisy => {
            vec![CHUNK_SIZE_1K, CHUNK_SIZE_4K, CHUNK_SIZE_64K]
        }
        _ => vec![CHUNK_SIZE_4K],
    }
}

/// `decode_full`: drain `chunk.iter()` with `black_box` over each
/// `(timestamp, value)`.  Avoids `Vec` allocation; measures raw iteration cost.
fn bench_decode_full(c: &mut Criterion) {
    let mut group = c.benchmark_group("decode_full");
    group.measurement_time(Duration::from_secs(5));
    let registry = DatasetRegistry::new();

    for key in support::benchmark_dataset_keys() {
        let dataset = registry.dataset(key);

        for encoding in encodings() {
            for chunk_size in bench_chunk_sizes(key) {
                let chunk = build_chunk(encoding, chunk_size, dataset);
                let n = chunk.len();

                let bench_id = format!(
                    "{}/{}/{}/{}",
                    encoding.name(),
                    key.workload.id(),
                    key.timestamp_model.id(),
                    chunk_size_id(chunk_size),
                );

                group.throughput(Throughput::Elements(n as u64));
                group.bench_function(BenchmarkId::from_parameter(&bench_id), |b| {
                    b.iter(|| {
                        chunk.iter().for_each(|s| {
                            std::hint::black_box((s.timestamp, s.value));
                        });
                    });
                });
            }
        }
    }

    group.finish();
}

/// `decode_materialize`: `get_range(first_ts, last_ts)` returning a
/// `Vec<Sample>`.  This is the shape the command layer actually uses; the Vec
/// allocation cost is intentionally included.
fn bench_decode_materialize(c: &mut Criterion) {
    let mut group = c.benchmark_group("decode_materialize");
    group.measurement_time(Duration::from_secs(5));
    let registry = DatasetRegistry::new();

    for key in support::benchmark_dataset_keys() {
        let dataset = registry.dataset(key);

        for encoding in encodings() {
            for chunk_size in bench_chunk_sizes(key) {
                let chunk = build_chunk(encoding, chunk_size, dataset);
                let first_ts = chunk.first_timestamp();
                let last_ts = chunk.last_timestamp();
                let n = chunk.len();

                let bench_id = format!(
                    "{}/{}/{}/{}",
                    encoding.name(),
                    key.workload.id(),
                    key.timestamp_model.id(),
                    chunk_size_id(chunk_size),
                );

                group.throughput(Throughput::Elements(n as u64));
                group.bench_function(BenchmarkId::from_parameter(&bench_id), |b| {
                    b.iter(|| {
                        std::hint::black_box(
                            chunk
                                .get_range(first_ts, last_ts)
                                .expect("get_range should succeed"),
                        );
                    });
                });
            }
        }
    }

    group.finish();
}

criterion_group!(benches, bench_decode_full, bench_decode_materialize);
criterion_main!(benches);
