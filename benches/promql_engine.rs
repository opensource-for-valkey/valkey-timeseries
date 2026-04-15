use criterion::{
    BenchmarkId, Criterion, SamplingMode, black_box, criterion_group, criterion_main,
};
use promql_parser::parser::{EvalStmt, parse};
use std::env;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use valkey_timeseries::common::Sample;
use valkey_timeseries::promql::engine::mock_series_querier::MockSeriesQuerier;
use valkey_timeseries::promql::engine::{QueryOptions, QueryReader, evaluate_instant, evaluate_range};
use valkey_timeseries::promql::Labels;

fn benchmark_config() -> Criterion {
    // BENCH_PROFILE=ci keeps runs quick; default local profile favors stable statistics.
    match env::var("BENCH_PROFILE").as_deref() {
        Ok("ci") => Criterion::default()
            .warm_up_time(Duration::from_millis(250))
            .measurement_time(Duration::from_secs(2))
            .sample_size(20),
        _ => Criterion::default()
            .warm_up_time(Duration::from_secs(2))
            .measurement_time(Duration::from_secs(8))
            .sample_size(40),
    }
}

fn use_flat_sampling() -> bool {
    matches!(env::var("BENCH_PROFILE").as_deref(), Ok("ci"))
}

fn ms_to_system_time(ms: i64) -> SystemTime {
    UNIX_EPOCH + Duration::from_millis(ms as u64)
}

fn series_labels(metric: &str, instance: usize) -> Labels {
    let mut labels = Labels::empty();
    labels.set("__name__", metric.to_string());
    labels.set("job", "api".to_string());
    labels.set("instance", format!("i-{instance:03}"));
    labels
}

fn build_reader(series_count: usize, points_per_series: usize, start_ms: i64, step_ms: i64) -> Arc<dyn QueryReader> {
    let querier = Arc::new(MockSeriesQuerier::new());

    for series_idx in 0..series_count {
        let labels = series_labels("http_requests_total", series_idx);
        for point in 0..points_per_series {
            let ts = start_ms + (point as i64 * step_ms);
            let value = (series_idx as f64 * 10.0) + (point as f64 * 0.1);
            querier.add_sample(&labels, Sample::new(ts, value));
        }
    }

    querier
}

fn bench_evaluate_instant(c: &mut Criterion) {
    let reader = build_reader(64, 300, 4_000_000, 1_000);
    let opts = QueryOptions {
        timeout: None,
        deadline: None,
        ..QueryOptions::default()
    };
    let query_time = ms_to_system_time(4_299_000);

    let cases = [
        ("selector", "http_requests_total"),
        ("aggregate", "sum(http_requests_total)"),
    ];

    let mut group = c.benchmark_group("promql_evaluate_instant");
    if use_flat_sampling() {
        group.sampling_mode(SamplingMode::Flat);
    }
    for (name, query) in cases {
        let expr = parse(query).expect("valid instant benchmark query");
        group.bench_with_input(BenchmarkId::new("query", name), &query, |b, _| {
            b.iter(|| {
                let stmt = EvalStmt {
                    expr: expr.clone(),
                    start: query_time,
                    end: query_time,
                    interval: Duration::ZERO,
                    lookback_delta: opts.lookback_delta,
                };
                let result = evaluate_instant(reader.clone(), stmt, query_time, opts)
                    .expect("instant benchmark should evaluate");
                black_box(result);
            });
        });
    }
    group.finish();
}

fn bench_evaluate_range(c: &mut Criterion) {
    let reader = build_reader(64, 300, 4_000_000, 1_000);
    let opts = QueryOptions {
        timeout: None,
        deadline: None,
        ..QueryOptions::default()
    };

    let start = ms_to_system_time(4_240_000);
    let end = ms_to_system_time(4_299_000);
    let step = Duration::from_secs(15);

    let cases = [
        ("selector", "http_requests_total"),
        ("aggregate", "sum(http_requests_total)"),
    ];

    let mut group = c.benchmark_group("promql_evaluate_range");
    if use_flat_sampling() {
        group.sampling_mode(SamplingMode::Flat);
    }
    for (name, query) in cases {
        let expr = parse(query).expect("valid range benchmark query");
        group.bench_with_input(BenchmarkId::new("query", name), &query, |b, _| {
            b.iter(|| {
                let stmt = EvalStmt {
                    expr: expr.clone(),
                    start,
                    end,
                    interval: step,
                    lookback_delta: opts.lookback_delta,
                };
                let result = evaluate_range(reader.clone(), stmt, opts)
                    .expect("range benchmark should evaluate");
                black_box(result);
            });
        });
    }
    group.finish();
}

criterion_group!(
    name = promql_engine_benches;
    config = benchmark_config();
    targets = bench_evaluate_instant, bench_evaluate_range
);
criterion_main!(promql_engine_benches);

