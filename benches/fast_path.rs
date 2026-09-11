#[cfg(feature = "bench")]
use criterion::{BenchmarkId, Criterion};
#[cfg(feature = "bench")]
use criterion::{criterion_group, criterion_main};

#[cfg(feature = "bench")]
use criterion::BatchSize;
#[cfg(feature = "bench")]
// These items are provided by the crate under the `bench` feature.
use valkey_timeseries::promql::binops::{
    BenchOp, LabelMode, VectorScalarCase, VectorVectorCase, VectorVectorShape,
};

/// Vector-vector `a + b` by input shape. Operand construction sits in
/// `iter_batched`'s untimed setup and the result is returned to be freed
/// untimed: at several heap allocations per sample, both would otherwise
/// dominate the join being measured. (The old version timed them.)
#[cfg(feature = "bench")]
fn bench_paths(c: &mut Criterion) {
    let shapes = [
        ("aligned", VectorVectorShape::Aligned),
        ("half_overlap", VectorVectorShape::HalfOverlap),
        (
            "half_overlap_with_fill",
            VectorVectorShape::HalfOverlapWithFill,
        ),
    ];

    let mut group = c.benchmark_group("vector_vector_ops");

    for &size in &[100usize, 1_000usize, 10_000usize] {
        for (name, shape) in shapes {
            let case = VectorVectorCase::new(shape, size);
            group.bench_with_input(BenchmarkId::new(name, size), &size, |b, _| {
                b.iter_batched(
                    || case.input(),
                    |input| case.run(input),
                    BatchSize::LargeInput,
                )
            });
        }
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

/// What a series' labels cost to hand from storage to the evaluator and drop.
///
/// `to_labels` is today's `Labels::from(&MetricName)`: a fresh `String` per
/// name and per value. `clone_interned` is the proposed alternative: cloning
/// the `MetricName` itself, one `Vec` plus an `Arc` bump per label. Drops are
/// deliberately inside the timed region — alloc+free is the pair being
/// compared. The threaded variants hit the same pool-shared strings
/// (`__name__`, `job`) from 8 threads at once, mirroring the parallel
/// instant-selector path.
#[cfg(feature = "bench")]
fn bench_labels_conversion(c: &mut Criterion) {
    use valkey_timeseries::labels::{Label, Labels, MetricName};

    fn series(n: usize, extra_labels: usize) -> Vec<MetricName> {
        (0..n)
            .map(|i| {
                let mut labels = vec![
                    Label::new("__name__".to_string(), "bench_metric".to_string()),
                    Label::new("id".to_string(), i.to_string()),
                    Label::new("job".to_string(), "app-server".to_string()),
                ];
                for k in 0..extra_labels {
                    labels.push(Label::new(format!("l{k}"), format!("v{}", i % 10)));
                }
                labels.sort_by(|a, b| a.name.cmp(&b.name));
                MetricName::from(labels.as_slice())
            })
            .collect()
    }

    let mut group = c.benchmark_group("labels_conversion");
    let n = 10_000usize;

    for (label_count, extra) in [(3usize, 0usize), (8, 5)] {
        let names = series(n, extra);

        group.bench_with_input(
            BenchmarkId::new(format!("to_labels/{label_count}_labels"), n),
            &n,
            |b, _| {
                b.iter(|| {
                    let out: Vec<Labels> = names.iter().map(Labels::from).collect();
                    std::hint::black_box(out);
                })
            },
        );

        group.bench_with_input(
            BenchmarkId::new(format!("clone_interned/{label_count}_labels"), n),
            &n,
            |b, _| {
                b.iter(|| {
                    let out: Vec<MetricName> = names.to_vec();
                    std::hint::black_box(out);
                })
            },
        );

        let threads = 8;
        group.bench_with_input(
            BenchmarkId::new(format!("to_labels_8thr/{label_count}_labels"), n),
            &n,
            |b, _| {
                b.iter(|| {
                    std::thread::scope(|sc| {
                        for _ in 0..threads {
                            sc.spawn(|| {
                                let out: Vec<Labels> = names.iter().map(Labels::from).collect();
                                std::hint::black_box(out);
                            });
                        }
                    })
                })
            },
        );

        group.bench_with_input(
            BenchmarkId::new(format!("clone_interned_8thr/{label_count}_labels"), n),
            &n,
            |b, _| {
                b.iter(|| {
                    std::thread::scope(|sc| {
                        for _ in 0..threads {
                            sc.spawn(|| {
                                let out: Vec<MetricName> = names.to_vec();
                                std::hint::black_box(out);
                            });
                        }
                    })
                })
            },
        );
    }

    group.finish();
}

/// Per-sample label primitives the range driver runs 100k+ times per query,
/// on identical content held as `Interned` (from storage) vs `Shared`
/// (`Arc<[Label]>`, the pre-interning representation). Isolates which
/// primitive, if any, got slower.
#[cfg(feature = "bench")]
fn bench_evallabels_ops(c: &mut Criterion) {
    use ahash::RandomState;
    use valkey_timeseries::labels::{Label, MetricName};
    use valkey_timeseries::promql::EvalLabels;

    const SERIES: usize = 100;
    const STEPS: usize = 1000;

    fn labels_for(i: usize) -> Vec<Label> {
        let mut v = vec![
            Label::new("__name__".to_string(), "a_hundred".to_string()),
            Label::new("instance".to_string(), format!("host-{i}")),
            Label::new("job".to_string(), "app".to_string()),
            Label::new("l".to_string(), i.to_string()),
        ];
        v.sort();
        v
    }
    let interned: Vec<EvalLabels> = (0..SERIES)
        .map(|i| EvalLabels::interned(&MetricName::from(labels_for(i).as_slice())))
        .collect();
    let shared: Vec<EvalLabels> = (0..SERIES)
        .map(|i| EvalLabels::shared(labels_for(i)))
        .collect();
    // The per-step stream of samples: every series once per step, as the
    // preloaded range path produces them (a clone of the series labels).
    let stream = |src: &Vec<EvalLabels>| -> Vec<EvalLabels> {
        (0..STEPS).flat_map(|_| src.iter().cloned()).collect()
    };
    let rs = RandomState::new();

    let mut group = c.benchmark_group("evallabels_ops");
    for (name, src) in [("interned", &interned), ("shared", &shared)] {
        let samples = stream(src);

        group.bench_function(BenchmarkId::new("clone_drop", name), |b| {
            b.iter(|| {
                for l in src {
                    std::hint::black_box(l.clone());
                }
            })
        });
        group.bench_function(BenchmarkId::new("hash", name), |b| {
            b.iter(|| {
                let mut acc = 0u64;
                for l in &samples {
                    acc ^= rs.hash_one(l);
                }
                std::hint::black_box(acc)
            })
        });
        group.bench_function(BenchmarkId::new("fingerprint", name), |b| {
            b.iter(|| {
                let mut acc = 0u128;
                for l in &samples {
                    acc ^= l.fingerprint_u128();
                }
                std::hint::black_box(acc)
            })
        });
        group.bench_function(BenchmarkId::new("get_name", name), |b| {
            b.iter(|| {
                let mut n = 0usize;
                for l in &samples {
                    n += l.metric_name().len();
                }
                std::hint::black_box(n)
            })
        });
        // SeriesMap-style grouping: hash + eq per sample, 100 distinct keys.
        group.bench_function(BenchmarkId::new("group_by_labels", name), |b| {
            b.iter(|| {
                let mut map: halfbrown::HashMap<EvalLabels, usize, RandomState> =
                    halfbrown::HashMap::with_hasher(RandomState::new());
                for l in &samples {
                    *map.entry(l.clone()).or_insert(0) += 1;
                }
                std::hint::black_box(map.len())
            })
        });
        group.bench_function(BenchmarkId::new("drop_name_promote", name), |b| {
            b.iter(|| {
                for l in &samples {
                    let mut l = l.clone();
                    l.drop_name();
                    std::hint::black_box(&l);
                }
            })
        });
    }
    group.finish();
}

#[cfg(feature = "bench")]
criterion_group!(
    benches,
    bench_paths,
    bench_vector_scalar,
    bench_vector_scalar_labels,
    bench_labels_conversion,
    bench_evallabels_ops
);
#[cfg(feature = "bench")]
criterion_main!(benches);
