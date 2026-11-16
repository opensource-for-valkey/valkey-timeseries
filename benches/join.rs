//! `TS.JOIN` costs, outside a server.
//!
//! - `merge/{create_join_iter,baseline}`: the exact-join merge as `create_join_iter` runs it
//!   against a bare `merge_join_by` + filter, over decoded inputs. When `create_join_iter`
//!   returned a boxed iterator with a function-pointer filter it ran 3.7–5.3× slower than the
//!   baseline; the two should now match.
//! - `decode/{sequential,parallel}`: decoding both inputs on one thread against
//!   `threads::join`, around `PARALLEL_MIN_WORK`. Decides where the parallel decode starts.
//! - `end_to_end/{all,count10}`: `JoinInputs::read` + `join` on real series, decode included,
//!   without `COUNT` and with `COUNT 10`.
use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use itertools::{EitherOrBoth, Itertools};
use std::hint::black_box;
use std::time::Duration;
use valkey_timeseries::common::Sample;
use valkey_timeseries::common::threads;
use valkey_timeseries::join::{JoinInputs, JoinOptions, JoinType, create_join_iter};
use valkey_timeseries::series::TimeSeries;

/// Left samples every 2 ms, right every 3 ms: a third of the left timestamps match.
fn inputs(n: usize) -> (Vec<Sample>, Vec<Sample>) {
    let left = (0..n as i64)
        .map(|i| Sample::new(i * 2, i as f64))
        .collect();
    let right = (0..n as i64)
        .map(|i| Sample::new(i * 3, -(i as f64)))
        .collect();
    (left, right)
}

fn series(samples: &[Sample]) -> TimeSeries {
    let mut series = TimeSeries::new();
    for s in samples {
        assert!(series.add(s.timestamp, s.value, None).is_ok());
    }
    series
}

fn bench_merge(c: &mut Criterion) {
    let mut group = c.benchmark_group("join/merge");
    group.measurement_time(Duration::from_secs(2));
    for n in [1_000, 100_000, 1_000_000] {
        let (left, right) = inputs(n);
        group.throughput(Throughput::Elements(2 * n as u64));
        for (name, join_type) in [("inner", JoinType::Inner), ("full", JoinType::Full)] {
            group.bench_with_input(
                BenchmarkId::new(format!("create_join_iter/{name}"), n),
                &n,
                |b, _| {
                    b.iter(|| {
                        create_join_iter(left.iter().copied(), right.iter().copied(), join_type)
                            .map(black_box)
                            .count()
                    })
                },
            );
            let keep_both = matches!(join_type, JoinType::Inner);
            group.bench_with_input(
                BenchmarkId::new(format!("baseline/{name}"), n),
                &n,
                |b, _| {
                    b.iter(|| {
                        left.iter()
                            .copied()
                            .merge_join_by(right.iter().copied(), |l, r| {
                                l.timestamp.cmp(&r.timestamp)
                            })
                            .filter(|row: &EitherOrBoth<Sample, Sample>| {
                                !keep_both || row.is_both()
                            })
                            .map(black_box)
                            .count()
                    })
                },
            );
        }
    }
    group.finish();
}

fn bench_decode(c: &mut Criterion) {
    let mut group = c.benchmark_group("join/decode");
    group.measurement_time(Duration::from_secs(2));
    // Total samples across both inputs: PARALLEL_MIN_WORK is 4096.
    for total in [512, 2_048, 4_096, 16_384, 65_536, 262_144] {
        let (left, right) = inputs(total / 2);
        let (left, right) = (series(&left), series(&right));
        let decode = |s: &TimeSeries| s.range_iter(i64::MIN, i64::MAX).collect::<Vec<_>>();
        group.throughput(Throughput::Elements(total as u64));
        group.bench_with_input(BenchmarkId::new("sequential", total), &total, |b, _| {
            b.iter(|| black_box((decode(&left), decode(&right))))
        });
        group.bench_with_input(BenchmarkId::new("parallel", total), &total, |b, _| {
            b.iter(|| black_box(threads::join(|| decode(&left), || decode(&right))))
        });
    }
    group.finish();
}

fn bench_end_to_end(c: &mut Criterion) {
    let mut group = c.benchmark_group("join/end_to_end");
    group.measurement_time(Duration::from_secs(2));
    for n in [1_000, 100_000, 1_000_000] {
        let (left, right) = inputs(n);
        let (left, right) = (series(&left), series(&right));
        group.throughput(Throughput::Elements(2 * n as u64));
        for (name, count) in [("all", None), ("count10", Some(10))] {
            let options = JoinOptions {
                join_type: JoinType::Inner,
                count,
                ..Default::default()
            };
            group.bench_with_input(BenchmarkId::new(name, n), &n, |b, _| {
                b.iter(|| {
                    let inputs = JoinInputs::read(&left, &right, &options).unwrap();
                    black_box(inputs.join(&options))
                })
            });
        }
    }
    group.finish();
}

criterion_group!(benches, bench_merge, bench_decode, bench_end_to_end);
criterion_main!(benches);
