//! A/B of how a PromQL literal alternation (`instance=~"a|b|…"`, the shape of
//! every derived push-down filter) reaches the index: the old conversion's
//! `RegexEqual`, which scans every value of the label, versus the new
//! `Equal(List)`, one postings lookup per value. Both arms run in one binary.

use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use promql_parser::label::{MatchOp as PromMatchOp, Matcher, Matchers};
use promql_parser::parser::VectorSelector;
use std::hint::black_box;
use std::time::Duration;
use valkey_timeseries::labels::Labels;
use valkey_timeseries::labels::filters::{
    FilterList, LabelFilter, PredicateMatch, RegexMatcher, SeriesSelector,
};
use valkey_timeseries::promql::engine::label_profile::join_regexp_values;
use valkey_timeseries::series::TimeSeries;
use valkey_timeseries::series::index::TimeSeriesIndex;

const SERIES: [usize; 3] = [1_000, 10_000, 100_000];
const ALTERNATIVES: [usize; 4] = [2, 8, 16, 60];

fn instance(i: usize) -> String {
    format!("host-{i:06}.example.net:9100")
}

fn build_index(series: usize) -> TimeSeriesIndex {
    let index = TimeSeriesIndex::new();
    for i in 0..series {
        let mut labels = Labels::empty();
        labels.set("__name__", "cpu".to_string());
        labels.set("job", "api".to_string());
        labels.set("instance", instance(i));
        let mut ts = TimeSeries::new();
        ts.id = i as u64 + 1;
        ts.labels = labels.as_ref().into();
        index.index_timeseries(&ts, format!("ts:{i}").as_bytes());
    }
    index
}

fn alternation(series: usize, k: usize) -> String {
    let values: Vec<String> = (0..k).map(|j| instance(j * series / k)).collect();
    join_regexp_values(values.iter().map(String::as_str))
}

fn matcher(alt: &str) -> Matcher {
    let re = regex::Regex::new(&format!("^(?:{alt})$")).unwrap();
    Matcher::new(PromMatchOp::Re(re), "instance", alt)
}

fn vector_selector(alt: &str) -> VectorSelector {
    VectorSelector {
        name: Some("cpu".to_string()),
        matchers: Matchers {
            matchers: vec![matcher(alt)],
            or_matchers: vec![],
        },
        offset: None,
        at: None,
    }
}

/// What the conversion produced before the change.
/// `None` where the bounded compiler rejects it: the old conversion panicked
/// and `try_regex_matcher` drops such a derived filter.
fn old_filter(alt: &str) -> Option<LabelFilter> {
    Some(LabelFilter {
        label: "instance".to_string(),
        matcher: PredicateMatch::RegexEqual(RegexMatcher::create(alt).ok()?),
    })
}

fn old_selector(alt: &str) -> Option<SeriesSelector> {
    let mut filters = FilterList::default();
    filters.push(LabelFilter::equals("__name__".to_string(), "cpu"));
    filters.push(old_filter(alt)?);
    Some(SeriesSelector::And(filters))
}

/// The most values of each shape whose alternation the bounded compiler accepts.
fn probe_compile_limit() {
    let shapes: [(&str, fn(usize) -> String); 5] = [
        ("region (us-east-1)", |i| format!("region-{i}")),
        ("pod (api-7f9c-xk2p)", |i| {
            format!("api-7f9c{i:02x}-xk{i:03}")
        }),
        ("instance (host-000123.example.net:9100)", instance),
        ("ip:port (10.0.3.17:9100)", |i| {
            format!("10.0.{}.{}:9100", i / 250, i % 250)
        }),
        ("uuid", |i| {
            format!("{:08x}-4b1e-9c2d-8e7f-{:012x}", i * 7919, i * 104729)
        }),
    ];
    for (name, f) in shapes {
        let max = (1..=60)
            .take_while(|&k| {
                let values: Vec<String> = (0..k).map(|j| f(j * 997)).collect();
                RegexMatcher::create(&join_regexp_values(values.iter().map(String::as_str))).is_ok()
            })
            .last()
            .unwrap_or(0);
        eprintln!("compile-limit {name}: {max} of 60");
    }
}

fn config() -> Criterion {
    Criterion::default()
        .warm_up_time(Duration::from_millis(500))
        .measurement_time(Duration::from_secs(3))
        .sample_size(30)
}

fn bench_postings(c: &mut Criterion) {
    for &series in &SERIES {
        let index = build_index(series);
        let mut group = c.benchmark_group(format!("postings/{series}"));
        for &k in &ALTERNATIVES {
            let alt = alternation(series, k);
            let new = SeriesSelector::from(&vector_selector(&alt));
            match &new {
                SeriesSelector::And(f) => {
                    assert!(matches!(f[1].matcher, PredicateMatch::Equal(_)), "{f:?}")
                }
                other => panic!("{other:?}"),
            }
            let b = index.postings_for_selector(&new).unwrap();
            assert_eq!(b.cardinality(), k as u64);
            if let Some(old) = old_selector(&alt) {
                let a = index.postings_for_selector(&old).unwrap();
                assert_eq!(a, b, "arms disagree at series={series} k={k}");
                group.bench_with_input(BenchmarkId::new("regex", k), &old, |bch, sel| {
                    bch.iter(|| black_box(index.postings_for_selector(sel).unwrap()))
                });
            } else {
                eprintln!("postings/{series}/regex/{k}: does not compile");
            }
            group.bench_with_input(BenchmarkId::new("list", k), &new, |bch, sel| {
                bch.iter(|| black_box(index.postings_for_selector(sel).unwrap()))
            });
        }
        group.finish();
    }
}

fn bench_conversion(c: &mut Criterion) {
    probe_compile_limit();
    let mut group = c.benchmark_group("conversion");
    for &k in &ALTERNATIVES {
        let alt = alternation(100_000, k);
        let m = matcher(&alt);
        if old_filter(&alt).is_some() {
            group.bench_with_input(BenchmarkId::new("regex", k), &alt, |bch, alt| {
                bch.iter(|| black_box(old_filter(alt)))
            });
        }
        group.bench_with_input(BenchmarkId::new("list", k), &m, |bch, m| {
            bch.iter(|| black_box(LabelFilter::from(m)))
        });
    }
    group.finish();
}

/// Compiling the Matcher's regex for a derived filter: today's bounded
/// `RegexMatcher::create` (which also walks the HIR for prefix/suffix hints)
/// versus a plain builder with a larger size limit.
fn bench_derived_compile(c: &mut Criterion) {
    let mut group = c.benchmark_group("derived_compile");
    for (len, k) in [(28usize, 8usize), (28, 60), (64, 60)] {
        let values: Vec<String> = (0..k)
            .map(|j| {
                let tail = format!("-{j:04}.example.net:9100");
                format!("{}{tail}", "h".repeat(len.saturating_sub(tail.len())))
            })
            .collect();
        let alt = join_regexp_values(values.iter().map(String::as_str));
        let id = format!("{k}x{len}B={}B", alt.len());
        if RegexMatcher::create(&alt).is_ok() {
            group.bench_with_input(
                BenchmarkId::new("regex_matcher_create", &id),
                &alt,
                |b, alt| b.iter(|| black_box(RegexMatcher::create(alt).unwrap())),
            );
        }
        group.bench_with_input(
            BenchmarkId::new("plain_builder_1MiB", &id),
            &alt,
            |b, alt| {
                b.iter(|| {
                    black_box(
                        regex::RegexBuilder::new(&format!("^(?:{alt})$"))
                            .size_limit(1 << 20)
                            .dfa_size_limit(16 << 10)
                            .dot_matches_new_line(true)
                            .build()
                            .unwrap(),
                    )
                })
            },
        );
    }
    group.finish();
}

criterion_group! {
    name = benches;
    config = config();
    targets = bench_derived_compile, bench_conversion, bench_postings
}
criterion_main!(benches);
