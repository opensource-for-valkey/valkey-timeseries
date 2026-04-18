// Code derived from the Prometheus Project
// Copyright The Prometheus Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use criterion::{BenchmarkId, Criterion, SamplingMode, black_box, criterion_group, criterion_main};
use promql_parser::parser::{EvalStmt, parse};
use rand::distr::{Alphanumeric, SampleString};
use std::env;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use valkey_timeseries::common::Sample;
use valkey_timeseries::promql::Labels;
use valkey_timeseries::promql::engine::mock_series_querier::MockSeriesQuerier;
use valkey_timeseries::promql::engine::{
    QueryOptions, QueryReader, evaluate_instant, evaluate_range,
};

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

fn build_reader(
    series_count: usize,
    points_per_series: usize,
    start_ms: i64,
    step_ms: i64,
) -> Arc<dyn QueryReader> {
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

// -----------------------------------------------------------------------------
// Data generation helpers (idiomatic Rust)
// -----------------------------------------------------------------------------

/// Setup data for range query benchmarks.
fn setup_range_query_test_data(interval_ms: i64, num_intervals: usize) -> Arc<dyn QueryReader> {
    let mut metrics = Vec::new();

    // a_one, b_one, h_one (with le buckets)
    metrics.push(Labels::from_pairs(&[("__name__", "a_one")]));
    metrics.push(Labels::from_pairs(&[("__name__", "b_one")]));
    for j in 0..10 {
        metrics.push(Labels::from_pairs(&[
            ("__name__", "h_one"),
            ("le", &j.to_string()),
        ]));
    }
    metrics.push(Labels::from_pairs(&[("__name__", "h_one"), ("le", "+Inf")]));

    // a_ten, b_ten, h_ten (10 variants)
    for i in 0..10 {
        metrics.push(Labels::from_pairs(&[
            ("__name__", "a_ten"),
            ("l", &i.to_string()),
        ]));
        metrics.push(Labels::from_pairs(&[
            ("__name__", "b_ten"),
            ("l", &i.to_string()),
        ]));
        for j in 0..10 {
            metrics.push(Labels::from_pairs(&[
                ("__name__", "h_ten"),
                ("l", &i.to_string()),
                ("le", &j.to_string()),
            ]));
        }
        metrics.push(Labels::from_pairs(&[
            ("__name__", "h_ten"),
            ("l", &i.to_string()),
            ("le", "+Inf"),
        ]));
    }

    // a_hundred, b_hundred, h_hundred (100 variants)
    for i in 0..100 {
        metrics.push(Labels::from_pairs(&[
            ("__name__", "a_hundred"),
            ("l", &i.to_string()),
        ]));
        metrics.push(Labels::from_pairs(&[
            ("__name__", "b_hundred"),
            ("l", &i.to_string()),
        ]));
        for j in 0..10 {
            metrics.push(Labels::from_pairs(&[
                ("__name__", "h_hundred"),
                ("l", &i.to_string()),
                ("le", &j.to_string()),
            ]));
        }
        metrics.push(Labels::from_pairs(&[
            ("__name__", "h_hundred"),
            ("l", &i.to_string()),
            ("le", "+Inf"),
        ]));
    }

    let points_per_sparse_series = num_intervals / 50;

    let querier = MockSeriesQuerier::new();

    for s in 0..num_intervals {
        let timestamp = (s as i64) * interval_ms;
        for (i, metric) in metrics.iter().enumerate() {
            let value = s as f64 + (i as f64) / (metrics.len() as f64);
            let sample = Sample::new(timestamp, value);
            querier.add_sample(metric, sample);
        }
        // sparse series
        let val = (s / points_per_sparse_series).to_string();
        let sparse_labels = Labels::from_pairs(&[("__name__", "sparse"), ("l", &val)]);

        let value = s as f64 / metrics.len() as f64;
        let sample = Sample { timestamp, value };

        querier.add_sample(&sparse_labels, sample);
    }

    Arc::new(querier)
}

/// All range query cases.
fn range_query_cases() -> Vec<(String, usize)> {
    let raw_cases = vec![
        ("a_X", 0),
        ("rate(a_X[1m])", 0),
        ("rate(a_X[1m])", 10000),
        // ("rate(a_X[1m] smoothed)", 0),
        // ("rate(a_X[1m] smoothed)", 10000),
        ("rate(sparse[1m])", 10000),
        // ("rate(sparse[1m] smoothed)", 10000),
        ("double_exponential_smoothing(a_X[1d], 0.3, 0.3)", 0),
        ("changes(a_X[1d])", 0),
        ("rate(a_X[1d])", 0),
        ("absent_over_time(a_X[1d])", 0),
        ("-a_X", 0),
        ("a_X - b_X", 0),
        ("a_X - b_X", 10000),
        ("a_X and b_X{l=~'.*[0-4]$'}", 0),
        ("a_X or b_X{l=~'.*[0-4]$'}", 0),
        ("a_X unless b_X{l=~'.*[0-4]$'}", 0),
        ("a_X and b_X{l='notfound'}", 0),
        ("abs(a_X)", 0),
        ("label_replace(a_X, 'l2', '$1', 'l', '(.*)')", 0),
        ("label_join(a_X, 'l2', '-', 'l', 'l')", 0),
        ("sum(a_X)", 0),
        ("avg(a_X)", 0),
        ("sum without (l)(h_X)", 0),
        ("sum without (le)(h_X)", 0),
        ("sum by (l)(h_X)", 0),
        ("sum by (le)(h_X)", 0),
        ("count_values('value', h_X)", 100),
        ("topk(1, a_X)", 0),
        ("topk(5, a_X)", 0),
        ("limitk(1, a_X)", 0),
        ("limitk(5, a_X)", 0),
        ("limit_ratio(0.1, a_X)", 0),
        ("limit_ratio(0.5, a_X)", 0),
        ("limit_ratio(-0.5, a_X)", 0),
        ("rate(a_X[1m]) + rate(b_X[1m])", 0),
        ("sum without (l)(rate(a_X[1m]))", 0),
        (
            "sum without (l)(rate(a_X[1m])) / sum without (l)(rate(b_X[1m]))",
            0,
        ),
        ("histogram_quantile(0.9, rate(h_X[5m]))", 0),
        ("a_X + on(l) group_right a_one", 0),
        ("count({__name__!=\"\"})", 1),
        ("count({__name__!=\"\",l=\"\"})", 1),
        ("timestamp(a_X)", 0),
    ];

    let mut expanded = Vec::new();
    for (expr, steps) in raw_cases {
        if expr.contains("X") {
            for size in &["one", "hundred"] {
                let replaced = expr.replace("X", size);
                let final_steps = if steps == 0 { 1 } else { steps };
                expanded.push((replaced.clone(), final_steps));
                if steps == 0 {
                    expanded.push((replaced, 1000));
                }
            }
        } else {
            let final_steps = if steps == 0 { 1 } else { steps };
            expanded.push((expr.to_string(), final_steps));
            if steps == 0 {
                expanded.push((expr.to_string(), 1000));
            }
        }
    }
    expanded
}

/// Benchmark: Range Query
fn bench_range_query(c: &mut Criterion) {
    const INTERVAL_MS: i64 = 10000; // 10s
    let num_intervals = 8640 + 10000; // day + 10k steps

    let reader = setup_range_query_test_data(INTERVAL_MS, num_intervals);

    let cases = range_query_cases();

    let start = SystemTime::UNIX_EPOCH + Duration::from_secs((num_intervals - 10000) as u64 * 10);
    // let end = SystemTime::UNIX_EPOCH + Duration::from_secs((num_intervals) as u64 * 10);
    let step = Duration::from_secs(10);

    let mut group = c.benchmark_group("range_query");
    if use_flat_sampling() {
        group.sampling_mode(SamplingMode::Flat);
    }

    let opts = QueryOptions {
        timeout: None,
        deadline: None,
        ..QueryOptions::default()
    };

    for (expr, steps) in cases {
        // Adjust end time based on steps
        let actual_end =
            SystemTime::UNIX_EPOCH + Duration::from_secs((num_intervals - steps) as u64 * 10);
        let id = format!("expr={},steps={}", expr, steps);
        let ast = parse(&expr).expect("valid range benchmark query");

        group.bench_with_input(
            BenchmarkId::new("range_query", id),
            &(ast, actual_end),
            |b, (ast, end)| {
                b.iter(|| {
                    let stmt = EvalStmt {
                        expr: ast.clone(),
                        start,
                        end: *end,
                        interval: step,
                        lookback_delta: opts.lookback_delta,
                    };
                    let result = evaluate_range(reader.clone(), stmt, opts)
                        .expect("range benchmark should evaluate");
                    black_box(result);
                });
            },
        );
    }
    group.finish();
}

/// Setup join query test data.
fn setup_join_query_test_data(
    interval_ms: i64,
    num_intervals: usize,
    num_instances: usize,
) -> Arc<dyn QueryReader> {
    let common = Labels::from_pairs(&[
        ("environment", "staging"),
        ("cluster", "test-kubernetes-cluster"),
        ("namespace", "test-kubernetes-namespace"),
        ("job", "worker"),
        ("rpc_method", "fetch-my-data-from-this-service"),
        ("domain", "test-domain"),
    ]);

    let mut rng = rand::rng();
    let mut metrics: Vec<Labels> = Vec::new();

    for _ in 0..num_instances {
        let instance = Alphanumeric.sample_string(&mut rng, 16);
        let mut lbls = Labels::default();
        // copy common labels
        for label in common.iter() {
            lbls.push(label.clone());
        }

        lbls.set("instance", instance.clone());

        lbls.set("__name__", "rpc_request_success_total".to_string());
        metrics.push(lbls.clone());

        lbls.set("__name__", "rpc_request_error_total".to_string());
        metrics.push(lbls);
    }

    let querier = MockSeriesQuerier::new();

    for s in 0..num_intervals {
        let timestamp = (s as i64) * interval_ms;
        for (i, metric) in metrics.iter().enumerate() {
            let value = s as f64 + (i as f64) / (metrics.len() as f64);
            let sample = Sample::new(timestamp, value);
            querier.add_sample(metric, sample);
        }
    }

    Arc::new(querier)
}

/// Benchmark: Join Query
fn bench_join_query(c: &mut Criterion) {
    const INTERVAL_MS: i64 = 10000;
    const STEPS: usize = 5000;
    const NUM_INSTANCES: usize = 1000;
    const LOOKBACK_DELTA: Duration = Duration::from_mins(1);

    let num_intervals = 8640 + STEPS;

    let reader = setup_join_query_test_data(INTERVAL_MS, num_intervals, NUM_INSTANCES);

    let start = SystemTime::UNIX_EPOCH + Duration::from_secs((num_intervals - STEPS) as u64 * 10);
    let end = SystemTime::UNIX_EPOCH + Duration::from_secs(num_intervals as u64 * 10);
    let step = Duration::from_secs(10);

    let queries = vec![
        "rpc_request_success_total + rpc_request_error_total",
        "rpc_request_success_total + ON (job, instance) GROUP_LEFT rpc_request_error_total",
        "rpc_request_success_total AND rpc_request_error_total{instance=~\"0.*\"}",
        "rpc_request_success_total OR rpc_request_error_total{instance=~\"0.*\"}",
        "rpc_request_success_total UNLESS rpc_request_error_total{instance=~\"0.*\"}",
    ];

    let opts = QueryOptions {
        timeout: None,
        deadline: None,
        ..QueryOptions::default()
    };

    let mut group = c.benchmark_group("join_query");
    if use_flat_sampling() {
        group.sampling_mode(SamplingMode::Flat);
    }

    for query in queries {
        group.bench_with_input(BenchmarkId::new("join_query", query), query, |b, query| {
            b.iter(|| {
                let expr = parse(query).expect("valid range benchmark query");
                let stmt = EvalStmt {
                    expr,
                    start,
                    end,
                    interval: step,
                    lookback_delta: LOOKBACK_DELTA,
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
    targets = bench_evaluate_instant, bench_range_query, bench_join_query
);
criterion_main!(promql_engine_benches);
