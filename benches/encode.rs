use criterion::{BatchSize, BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use valkey_timeseries::series::chunks::{ChunkEncoding, ChunkOps, TimeSeriesChunk};

mod support;

use support::ValueWorkload;

fn bench_chunk_sizes(key: support::DatasetKey) -> Vec<usize> {
    match key.workload {
        ValueWorkload::Drift | ValueWorkload::Noisy => {
            vec![
                support::CHUNK_SIZE_1K,
                support::CHUNK_SIZE_4K,
                support::CHUNK_SIZE_64K,
            ]
        }
        _ => vec![support::CHUNK_SIZE_4K],
    }
}

fn encodings() -> [ChunkEncoding; 2] {
    [ChunkEncoding::Uncompressed, ChunkEncoding::Gorilla]
}

fn bench_encode_bulk(c: &mut Criterion) {
    let mut bulk = c.benchmark_group("encode_bulk");
    let registry = support::DatasetRegistry::new();

    for key in support::benchmark_dataset_keys() {
        let dataset = registry.dataset(key);
        for encoding in encodings() {
            for chunk_size in bench_chunk_sizes(key) {
                let samples = support::filled_prefix(dataset, encoding, chunk_size);
                let bench_id = format!(
                    "{}/{}/{}/{}",
                    encoding.name(),
                    key.workload.id(),
                    key.timestamp_model.id(),
                    support::chunk_size_id(chunk_size)
                );

                bulk.throughput(Throughput::Elements(samples.len() as u64));
                bulk.bench_with_input(
                    BenchmarkId::from_parameter(&bench_id),
                    &samples,
                    |b, &samples| {
                        b.iter_batched(
                            || TimeSeriesChunk::new(encoding, chunk_size),
                            |mut chunk| chunk.set_data(samples).expect("set_data should succeed"),
                            BatchSize::SmallInput,
                        )
                    },
                );
            }
        }
    }

    bulk.finish();
}

fn bench_encode_append(c: &mut Criterion) {
    let mut append = c.benchmark_group("encode_append");
    let registry = support::DatasetRegistry::new();

    for key in support::benchmark_dataset_keys() {
        let dataset = registry.dataset(key);
        for encoding in encodings() {
            for chunk_size in bench_chunk_sizes(key) {
                let samples = support::filled_prefix(dataset, encoding, chunk_size);
                let bench_id = format!(
                    "{}/{}/{}/{}",
                    encoding.name(),
                    key.workload.id(),
                    key.timestamp_model.id(),
                    support::chunk_size_id(chunk_size)
                );

                append.throughput(Throughput::Elements(samples.len() as u64));
                append.bench_with_input(
                    BenchmarkId::from_parameter(&bench_id),
                    &samples,
                    |b, &samples| {
                        b.iter_batched(
                            || TimeSeriesChunk::new(encoding, chunk_size),
                            |mut chunk| {
                                for sample in samples {
                                    if chunk.is_full() {
                                        break;
                                    }
                                    chunk.add_sample(sample).expect("add_sample should succeed");
                                }
                            },
                            BatchSize::SmallInput,
                        )
                    },
                );
            }
        }
    }

    append.finish();
}

criterion_group!(benches, bench_encode_bulk, bench_encode_append);
criterion_main!(benches);
