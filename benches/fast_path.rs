use criterion::{BenchmarkId, Criterion};
use criterion::{criterion_group, criterion_main};

// These functions are provided by the crate under the `bench` feature.
use valkey_timeseries::promql::binops::{
    bench_eval_aligned, bench_eval_unaligned, bench_eval_with_fill,
};

fn bench_paths(c: &mut Criterion) {
    let mut group = c.benchmark_group("vector_vector_ops");

    for &size in &[100usize, 1_000usize, 10_000usize] {
        group.bench_with_input(BenchmarkId::new("aligned", size), &size, |b, &s| {
            b.iter(|| {
                let r = bench_eval_aligned(std::hint::black_box(s));
                std::hint::black_box(r);
            })
        });

        group.bench_with_input(BenchmarkId::new("unaligned", size), &size, |b, &s| {
            b.iter(|| {
                let r = bench_eval_unaligned(std::hint::black_box(s));
                std::hint::black_box(r);
            })
        });

        group.bench_with_input(BenchmarkId::new("with_fill", size), &size, |b, &s| {
            b.iter(|| {
                let r = bench_eval_with_fill(std::hint::black_box(s));
                std::hint::black_box(r);
            })
        });
    }

    group.finish();
}

criterion_group!(benches, bench_paths);
criterion_main!(benches);
