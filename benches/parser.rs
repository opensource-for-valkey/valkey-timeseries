//! Parser throughput: series selectors (both dialects), METRIC names, durations, timestamps and
//! numeric operands, including error paths. Each case parses one command argument, which is how
//! commands call the parser.
//!
//! A selector parse that contains a regex includes compiling it (`LabelFilter::create`), which is
//! usually most of its cost; the `regex_*` cases show that share.

use criterion::{Criterion, criterion_group, criterion_main};
use std::hint::black_box;
use std::time::Duration;
use valkey_timeseries::tests::parsers::{
    try_parse_duration, try_parse_metric_name, try_parse_number, try_parse_selector,
    try_parse_timestamp,
};

/// (case id, input, whether it parses)
const SELECTOR_CASES: &[(&str, &str, bool)] = &[
    ("rts_equal", "region=us-east-1", true),
    ("rts_dotted_value", "host=10.0.0.1", true),
    (
        "rts_list",
        "region=(us-east-1,us-west-2,eu-central-1)",
        true,
    ),
    ("rts_absent", "region=", true),
    ("rts_regex", r#"instance=~"host-0[0-9]+""#, true),
    ("bare_metric", "http_requests_total", true),
    ("braces_one", r#"{job="api"}"#, true),
    (
        "braces_three",
        r#"http_requests_total{method="GET", status="200", job="api"}"#,
        true,
    ),
    ("braces_list", r#"{status=("500","502","503","504")}"#, true),
    (
        "braces_escaped",
        r#"{path="C:\\temp\\x", msg="say \"hi\"\n"}"#,
        true,
    ),
    (
        "braces_or_groups",
        r#"{env="prod" or env="staging" or region="eu"}"#,
        true,
    ),
    (
        "selectors_or",
        r#"http_status{status="500"} or api_host{service="auth", env=~"prod|staging"}"#,
        true,
    ),
    ("regex_literal", r#"{job=~"api"}"#, true),
    ("regex_prefix", r#"{job=~"api.*"}"#, true),
    ("regex_alternation", r#"{env=~"prod|staging|dev"}"#, true),
    (
        "regex_complex",
        r#"{instance=~"^host-(a|b)[0-9]{2,4}\\.example\\.com$"}"#,
        true,
    ),
    ("err_unterminated", r#"{job="api}"#, false),
    ("err_bad_list", "{job=(a,,b)}", false),
    ("err_trailing", r#"cpu{job="a"} junk"#, false),
];

const METRIC_CASES: &[(&str, &str, bool)] = &[
    ("bare", "node_memory_used_bytes", true),
    (
        "labels",
        r#"node_memory_used_bytes{hostname="host1.domain.com", job="node", env="prod"}"#,
        true,
    ),
    ("err_duplicate", r#"cpu{a="1",a="2"}"#, false),
];

const DURATION_CASES: &[(&str, &str, bool)] = &[
    ("integer", "86400000", true),
    ("unit", "1h", true),
    ("compound", "1d2h3m4s5ms", true),
    ("fractional", "1.5h", true),
    ("err_order", "5s10m", false),
];

const TIMESTAMP_CASES: &[(&str, &str, bool)] = &[
    ("integer", "1700000000000", true),
    ("fractional_seconds", "1700000000.123", true),
    ("rfc3339", "2023-11-14T22:13:20.123Z", true),
    ("err_garbage", "yesterday", false),
];

const NUMBER_CASES: &[(&str, &str, bool)] = &[
    ("integer", "12345", true),
    ("float", "-1.25e3", true),
    ("suffix", "4KiB", true),
    ("hex", "0xff_ff", true),
    ("err_double_sign", "--12", false),
];

fn run<T>(
    c: &mut Criterion,
    group: &str,
    cases: &[(&str, &str, bool)],
    parse: impl Fn(&str) -> Option<T>,
) {
    let mut g = c.benchmark_group(group);
    g.warm_up_time(Duration::from_millis(500))
        .measurement_time(Duration::from_secs(2));
    for &(id, input, ok) in cases {
        assert_eq!(parse(input).is_some(), ok, "{group}/{id}: {input}");
        g.bench_function(id, |b| b.iter(|| parse(black_box(input))));
    }
    g.finish();
}

fn bench_parsers(c: &mut Criterion) {
    run(c, "selector", SELECTOR_CASES, try_parse_selector);
    run(c, "metric_name", METRIC_CASES, try_parse_metric_name);
    run(c, "duration", DURATION_CASES, try_parse_duration);
    run(c, "timestamp", TIMESTAMP_CASES, try_parse_timestamp);
    run(c, "number", NUMBER_CASES, try_parse_number);
}

criterion_group!(benches, bench_parsers);
criterion_main!(benches);
