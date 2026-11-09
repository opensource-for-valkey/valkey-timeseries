//! Per-sample cost of the bucket aggregators and of the GROUPBY partial reducers, isolated from
//! decoding and reply building. Gate for numerical changes to the accumulators (overflow handling,
//! dispersion precision): save a baseline before, compare after. `range_scan` adds the read
//! pipeline around them (decode, filtering, bucketing), for changes to that path.
use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use std::hint::black_box;
use valkey_timeseries::aggregators::{
    AggregationHandler, AggregationType, Aggregator, BucketAlignment, BucketTimestamp,
    PartialReducer, PartialSampleReducer,
};
use valkey_timeseries::common::Sample;
use valkey_timeseries::common::binop::ComparisonOperator;
use valkey_timeseries::iterators::{create_range_iterator, create_row_iterator};
use valkey_timeseries::series::request_types::{
    AggregationOptions, AggregatorConfig, RangeOptions, ValueComparisonFilter,
};
use valkey_timeseries::series::{TimeSeries, ValueFilter};

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

/// Bucket-only aggregators, with the condition the filtered ones need.
fn bucket_configs() -> Vec<(&'static str, AggregatorConfig)> {
    let above = |value| {
        Some(ValueComparisonFilter {
            operator: ComparisonOperator::GreaterThan,
            value,
        })
    };
    let mut configs: Vec<_> = TYPES
        .iter()
        .map(|&(name, ty)| (name, AggregatorConfig::new(ty, None).unwrap()))
        .collect();
    configs.extend([
        (
            "range",
            AggregatorConfig::new(AggregationType::Range, None).unwrap(),
        ),
        (
            "increase",
            AggregatorConfig::new(AggregationType::Increase, None).unwrap(),
        ),
        (
            "countif",
            AggregatorConfig::new(AggregationType::CountIf, above(500.0)).unwrap(),
        ),
        (
            "sumif",
            AggregatorConfig::new(AggregationType::SumIf, above(500.0)).unwrap(),
        ),
        (
            "share",
            AggregatorConfig::new(AggregationType::Share, above(500.0)).unwrap(),
        ),
        (
            "all",
            AggregatorConfig::new(AggregationType::All, above(0.0)).unwrap(),
        ),
    ]);
    configs
}

fn bucket_aggregators(c: &mut Criterion) {
    let values = values();
    let mut group = c.benchmark_group("aggregators/bucket");
    group.throughput(Throughput::Elements(SAMPLES as u64));
    for (name, config) in bucket_configs() {
        group.bench_function(BenchmarkId::from_parameter(name), |b| {
            let mut agg: Aggregator = config.create_aggregator();
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

/// `TS.RANGE`-shaped reads over a 100k-sample series (default encoding) in 60 s buckets: one
/// aggregation, one with `FILTER_BY_VALUE`, and a raw read through `create_range_iterator`, and
/// two aggregations through `create_row_iterator`, the row pipeline a multi-aggregation
/// `TS.RANGE` takes. The read pipeline sits on every sample here, which the isolated groups above
/// leave out.
fn range_scan(c: &mut Criterion) {
    let mut series = TimeSeries::new();
    for (i, v) in values().into_iter().enumerate() {
        series.add(1_700_000_000_000 + i as i64 * 1000, v, None);
    }
    let agg = |types: &[AggregationType]| AggregationOptions {
        aggregations: types
            .iter()
            .map(|&t| AggregatorConfig::new(t, None).unwrap())
            .collect(),
        bucket_duration: 60_000,
        timestamp_output: BucketTimestamp::default(),
        alignment: BucketAlignment::default(),
        report_empty: false,
    };
    let cases: Vec<(&str, RangeOptions)> = vec![
        (
            "avg",
            RangeOptions {
                aggregation: Some(agg(&[AggregationType::Avg])),
                ..Default::default()
            },
        ),
        (
            "sum",
            RangeOptions {
                aggregation: Some(agg(&[AggregationType::Sum])),
                ..Default::default()
            },
        ),
        (
            "max",
            RangeOptions {
                aggregation: Some(agg(&[AggregationType::Max])),
                ..Default::default()
            },
        ),
        (
            "avg_filter_by_value",
            RangeOptions {
                aggregation: Some(agg(&[AggregationType::Avg])),
                // Every sample passes: the same work as `avg`, plus the filter itself.
                value_filter: Some(ValueFilter { min: 0.0, max: 1e9 }),
                ..Default::default()
            },
        ),
        ("raw", RangeOptions::default()),
    ];
    // `create_range_iterator` yields one value per bucket, so a multi-aggregation read takes the
    // row pipeline, as `TS.RANGE` does.
    let two_columns = RangeOptions {
        aggregation: Some(agg(&[AggregationType::Avg, AggregationType::Max])),
        ..Default::default()
    };

    let mut group = c.benchmark_group("aggregators/range_scan");
    group.throughput(Throughput::Elements(SAMPLES as u64));
    for (name, options) in &cases {
        group.bench_function(BenchmarkId::from_parameter(name), |b| {
            b.iter(|| {
                create_range_iterator(&series, options, None, false).for_each(|s| {
                    black_box(s);
                });
            })
        });
    }
    group.bench_function(BenchmarkId::from_parameter("avg_max_two_columns"), |b| {
        b.iter(|| {
            create_row_iterator(&series, &two_columns, None, false).for_each(|row| {
                black_box(row.timestamp);
                for value in row.values.iter() {
                    black_box(*value);
                }
            });
        })
    });
    group.finish();
}

criterion_group!(benches, bucket_aggregators, partial_reducers, range_scan);
criterion_main!(benches);
