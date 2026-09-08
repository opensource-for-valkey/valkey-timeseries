#[cfg(feature = "bench")]
use criterion::{BenchmarkId, Criterion};
#[cfg(feature = "bench")]
use criterion::{criterion_group, criterion_main};

#[cfg(feature = "bench")]
use criterion::BatchSize;
#[cfg(feature = "bench")]
// These items are provided by the crate under the `bench` feature.
use valkey_timeseries::promql::binops::{
    BenchOp, LabelMode, VectorScalarCase, bench_eval_aligned, bench_eval_unaligned,
    bench_eval_with_fill,
};

#[cfg(feature = "bench")]
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
/// A/B the vector-scalar loop: the current implementation, which hoists the
/// comparison branch out of the loop and skips `retain_mut` when nothing can be
/// filtered, against the pre-split `retain_mut`-for-everything version.
///
/// Input construction is kept out of the timed region via `iter_batched`, and
/// the result is returned so Criterion frees it untimed as well: at ~3 heap
/// allocations per sample, setup and teardown otherwise dwarf the loop.
#[cfg(feature = "bench")]
fn bench_vector_scalar(c: &mut Criterion) {
    // (label, op, pass ratio) — pass ratio only bites on the filtering op.
    let cases = [
        ("add", BenchOp::Add, 1.0),
        ("gtr_bool", BenchOp::GreaterBool, 1.0),
        ("gtr_keep90", BenchOp::Greater, 0.9),
        ("gtr_keep50", BenchOp::Greater, 0.5),
        ("gtr_keep10", BenchOp::Greater, 0.1),
    ];

    let mut group = c.benchmark_group("vector_scalar_ops");

    for &size in &[100usize, 1_000usize, 10_000usize] {
        for (name, op, pass_ratio) in cases {
            let case = VectorScalarCase::new(op, size, pass_ratio, false);

            group.bench_with_input(
                BenchmarkId::new(format!("{name}/new"), size),
                &size,
                |b, _| {
                    b.iter_batched(
                        || case.input(),
                        |input| case.run(input),
                        BatchSize::SmallInput,
                    )
                },
            );

            group.bench_with_input(
                BenchmarkId::new(format!("{name}/legacy"), size),
                &size,
                |b, _| {
                    b.iter_batched(
                        || case.input(),
                        |input| case.run_legacy(input),
                        BatchSize::SmallInput,
                    )
                },
            );
        }

        // The scalar-on-the-left function got the same treatment; spot-check it
        // at one shape rather than duplicating the whole matrix.
        let swapped = VectorScalarCase::new(BenchOp::Add, size, 1.0, true);
        group.bench_with_input(
            BenchmarkId::new("add_scalar_left/new", size),
            &size,
            |b, _| {
                b.iter_batched(
                    || swapped.input(),
                    |input| swapped.run(input),
                    BatchSize::SmallInput,
                )
            },
        );
    }

    group.finish();
}

/// Isolate what a *dropped* sample costs under each way its labels can be
/// held. `add` is the no-drop reference; `gtr_keep10` drops 90% of samples.
#[cfg(feature = "bench")]
fn bench_vector_scalar_labels(c: &mut Criterion) {
    let modes = [
        ("owned", LabelMode::Owned),
        ("shared_sole", LabelMode::SharedSole),
        ("shared_retained", LabelMode::SharedRetained),
    ];
    let size = 10_000usize;

    let mut group = c.benchmark_group("vector_scalar_labels");
    for (mode_name, mode) in modes {
        for (name, op, pass_ratio) in [
            ("add", BenchOp::Add, 1.0),
            ("gtr_keep10", BenchOp::Greater, 0.1),
        ] {
            let case = VectorScalarCase::with_labels(op, size, pass_ratio, false, mode);
            group.bench_with_input(
                BenchmarkId::new(format!("{name}/{mode_name}"), size),
                &size,
                |b, _| {
                    b.iter_batched(
                        || case.input(),
                        |input| case.run(input),
                        BatchSize::SmallInput,
                    )
                },
            );
        }
    }
    group.finish();
}

#[cfg(feature = "bench")]
criterion_group!(
    benches,
    bench_paths,
    bench_vector_scalar,
    bench_vector_scalar_labels
);
#[cfg(feature = "bench")]
criterion_main!(benches);
