//! Per-append cost of driving one compaction rule, the path every `TS.ADD` into a ruled series
//! pays. Gate for the batch-of-one switch (docs/plans/2026-10-14-compaction-refactor.md, Task 9):
//! save a baseline before the switch, compare after.
use criterion::{Criterion, criterion_group, criterion_main};
use std::hint::black_box;
use valkey_timeseries::aggregators::AggregationType;
use valkey_timeseries::common::Sample;
use valkey_timeseries::series::{TimeSeries, bench_ingest_one, bench_rule};

fn append_one_rule(c: &mut Criterion) {
    let mut group = c.benchmark_group("compaction_ingest");
    for (name, agg) in [
        ("sum", AggregationType::Sum),
        ("avg", AggregationType::Avg),
        ("std.s", AggregationType::StdS),
        ("rate", AggregationType::Rate),
    ] {
        group.bench_function(format!("append_one_rule/{name}"), |b| {
            // The append path never reads the source, so an empty one suffices; the
            // destination gains one bucket per 1000 iterations.
            let parent = TimeSeries::new();
            let mut dest = TimeSeries::new();
            let mut rule = bench_rule(agg, 1000);
            let mut ts: i64 = 0;
            b.iter(|| {
                let prev_last = (ts > 0).then(|| ts - 1);
                bench_ingest_one(
                    &parent,
                    &mut dest,
                    &mut rule,
                    black_box(Sample::new(ts, 1.0)),
                    prev_last,
                );
                ts += 1;
            });
        });
    }
    group.finish();
}

criterion_group!(benches, append_one_rule);
criterion_main!(benches);
