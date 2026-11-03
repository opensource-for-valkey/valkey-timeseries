//! Postings-index lookups: selector planning and the series-create uniqueness check.
//!
//! The cases are chosen around whether the result can borrow a stored bitmap or must
//! materialise one, so a regression in either direction shows up as a size-dependent cost.
//! Results are read through `get_postings()` rather than `TimeSeriesIndex::postings_for_selector`,
//! which always copies.

use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use std::hint::black_box;
use std::time::Duration;
use valkey_timeseries::Label;
use valkey_timeseries::MetricName;
use valkey_timeseries::series::TimeSeries;
use valkey_timeseries::series::index::TimeSeriesIndex;
use valkey_timeseries::tests::selectors::parse_selector;

const FLEET_SIZES: [u64; 3] = [10_000, 100_000, 1_000_000];

/// Labels of series `id`: `__name__` covers 90 % of the fleet, `host` is unique, `job` has a
/// single value, `env` two and `region` four.
fn fleet_labels(id: u64) -> [(String, String); 5] {
    let name = if id.is_multiple_of(10) { "mem" } else { "cpu" };
    let env = if id.is_multiple_of(2) { "dev" } else { "prod" };
    [
        ("__name__".into(), name.into()),
        ("host".into(), format!("h{id}")),
        ("job".into(), "api".into()),
        ("env".into(), env.into()),
        ("region".into(), format!("r{}", id % 4)),
    ]
}

fn metric_name(pairs: &[(String, String)]) -> MetricName {
    MetricName::from_pairs(pairs.iter().map(|(n, v)| (n.as_str(), v.as_str())))
}

/// Labels as the series-create path builds them for the uniqueness check.
fn query_labels(pairs: &[(String, String)]) -> Vec<Label> {
    metric_name(pairs).to_label_vec()
}

fn build_index(size: u64) -> TimeSeriesIndex {
    let index = TimeSeriesIndex::new();
    for id in 1..=size {
        let labels = fleet_labels(id);
        let mut ts = TimeSeries::new();
        ts.id = id;
        ts.labels = metric_name(&labels);
        index.index_timeseries(&ts, format!("ts:{id}").as_bytes());
    }
    index
}

/// (case id, selector, expected share of the fleet matched, as a numerator over 20)
const SELECTOR_CASES: &[(&str, &str, u64)] = &[
    // single-valued label: one key's bitmap
    ("label_present_one_value", r#"{job!=""}"#, 20),
    ("regex_one_value", r#"{job=~"a.*"}"#, 20),
    // literal list where only one alternative exists on the shard
    ("list_one_present", r#"{env=~"prod|staging"}"#, 10),
    // intersecting set minus a value that does not exist
    ("eq_minus_absent", r#"{__name__="cpu", env!="staging"}"#, 18),
    // intersecting set minus a value that does exist: must materialise (odd ids are all `cpu`)
    ("eq_minus_present", r#"{__name__="cpu", env!="dev"}"#, 10),
    // OR where only one branch matches anything
    (
        "or_one_branch",
        r#"{__name__="cpu" or __name__="nope"}"#,
        18,
    ),
    // control: several keys unioned, owned before and after
    ("list_two_present", r#"{region=~"r0|r1"}"#, 10),
    // control: two intersecting sets
    ("eq_and_eq", r#"{__name__="cpu", env="prod"}"#, 10),
    // owned union as the smallest set of an intersection: narrowed in place
    ("list_and_eq", r#"{region=~"r0|r1", job="api"}"#, 10),
];

fn bench_selectors(c: &mut Criterion) {
    let mut group = c.benchmark_group("postings_selector");
    group
        .warm_up_time(Duration::from_secs(1))
        .measurement_time(Duration::from_secs(3));

    for size in FLEET_SIZES {
        let index = build_index(size);
        for &(case, text, twentieths) in SELECTOR_CASES {
            let selector = parse_selector(text);
            let postings = index.get_postings();
            let matched = postings
                .postings_for_selector(&selector)
                .unwrap()
                .cardinality();
            let expected = size * twentieths / 20;
            assert!(
                matched.abs_diff(expected) <= size / 100,
                "{case} at {size}: matched {matched}, expected about {expected}"
            );
            group.bench_with_input(BenchmarkId::new(case, size), &selector, |b, selector| {
                b.iter(|| {
                    let result = postings.postings_for_selector(black_box(selector)).unwrap();
                    black_box(result.cardinality())
                })
            });
        }
    }
    group.finish();
}

fn bench_series_id_by_labels(c: &mut Criterion) {
    let mut group = c.benchmark_group("postings_series_id_by_labels");
    group
        .warm_up_time(Duration::from_secs(1))
        .measurement_time(Duration::from_secs(3));

    for size in FLEET_SIZES {
        let index = build_index(size);
        let id = size / 2 + 1;
        // TS.CREATE passes `MetricName::to_label_vec`, which is in label-name order.
        let mut pairs = fleet_labels(id);
        let labels = query_labels(&pairs);
        assert_eq!(index.series_id_by_labels(&labels), Some(id));
        // a new series: same labels except an unseen host
        pairs[1].1 = "h-new".into();
        let fresh = query_labels(&pairs);
        assert_eq!(index.series_id_by_labels(&fresh), None);

        group.bench_with_input(BenchmarkId::new("existing", size), &labels, |b, labels| {
            b.iter(|| black_box(index.series_id_by_labels(black_box(labels))))
        });
        group.bench_with_input(BenchmarkId::new("new_series", size), &fresh, |b, labels| {
            b.iter(|| black_box(index.series_id_by_labels(black_box(labels))))
        });
    }
    group.finish();
}

criterion_group!(benches, bench_selectors, bench_series_id_by_labels);
criterion_main!(benches);
