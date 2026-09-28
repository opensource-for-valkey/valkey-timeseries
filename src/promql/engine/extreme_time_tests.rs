//! Queries at the ends of the timestamp range must not overflow.
//!
//! Timestamps are `i64` milliseconds, and `END +` is `i64::MAX`. Offsets, windows, lookbacks and
//! subquery ranges are subtracted from or added to them all over the evaluator. Release builds
//! wrap silently on overflow (a window that wraps lands on unrelated data); debug builds, like
//! these tests, panic. Every such sum saturates instead: a time past either end clamps to it,
//! where there is no data.
use crate::common::Sample;
use crate::labels::Labels;
use crate::promql::QueryOptions;
use crate::promql::engine::memory_series_querier::MemorySeriesQuerier;
use crate::promql::engine::{QueryReader, evaluate_instant, evaluate_range};
use promql_parser::parser::{EvalStmt, parse};
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

const HOUR_MS: i64 = 3_600_000;

fn reader() -> Arc<dyn QueryReader> {
    let querier = MemorySeriesQuerier::new();
    let labels = Labels::from_pairs(&[("__name__", "m"), ("job", "a")]);
    for t in 0..10 {
        querier.add_sample(&labels, Sample::new(t * 10_000, t as f64));
    }
    // And data at the top of the range, so windows there reach the rollup and rate code.
    let top = Labels::from_pairs(&[("__name__", "m"), ("job", "top")]);
    for t in (0..10).rev() {
        querier.add_sample(&top, Sample::new(i64::MAX - t * 10_000, (10 - t) as f64));
    }
    // And at the bottom, which only an offset reaches: query times below the epoch clamp to 0.
    let bottom = Labels::from_pairs(&[("__name__", "m"), ("job", "bottom")]);
    for t in 0..10 {
        querier.add_sample(&bottom, Sample::new(i64::MIN + t * 10_000, t as f64));
    }
    Arc::new(querier)
}

/// `ms` as a `SystemTime`; negative values lie before the epoch.
fn at(ms: i64) -> SystemTime {
    if ms >= 0 {
        UNIX_EPOCH + Duration::from_millis(ms as u64)
    } else {
        UNIX_EPOCH - Duration::from_millis(ms.unsigned_abs())
    }
}

fn options() -> QueryOptions {
    QueryOptions {
        timeout: None,
        deadline: None,
        ..QueryOptions::default()
    }
}

const QUERIES: &[&str] = &[
    "m",
    "m offset 1h",
    "m offset -1h",
    "m[5m]",
    "m[5m] offset -1h",
    "rate(m[5m])",
    "rate(m[5m] offset 1h)",
    "sum_over_time(m[5m])",
    "max_over_time(m[10m:1m])",
    "max_over_time(m[10m:1m] offset -1h)",
    "max_over_time((m offset -1h)[10m:1m])",
    "sum(m offset 1h)",
    "m @ end() offset -1h",
    "m @ start() offset 1h",
    "timestamp(m)",
    "m + m offset 1h",
    "increase(m[1m])",
    "deriv(m[1m])",
    "predict_linear(m[1m], 3600)",
    "changes(m[1m])",
    "quantile_over_time(0.5, m[1m])",
    "last_over_time(m[1m])",
    "irate(m[1m])",
    "rate(m[1m:10s])",
    // Durations near the largest promql-parser accepts (~292 million years).
    "m offset 250000000y",
    "m offset -250000000y",
    "m[250000000y]",
    "rate(m[250000000y] offset 250000000y)",
    "max_over_time(m[250000000y:100000000y] offset -250000000y)",
    "max_over_time((m offset 250000000y)[250000000y:100000000y] offset 250000000y)",
    // From time 0 these land on the bottom series with windows reaching past i64::MIN.
    "rate(m[292000000y] offset 292000000y)",
    "increase(m[292000000y] offset 292000000y)",
    "max_over_time(m[292000000y:100000000y] offset 292000000y)",
    "last_over_time(m[292000000y] offset 292000000y)",
    "m offset 292000000y",
];

/// Runs `query` and reports a panic as `Err`, so one test lists every failing shape.
fn check(label: &str, run: impl FnOnce() -> Result<(), String>) -> Option<String> {
    match catch_unwind(AssertUnwindSafe(run)) {
        Ok(Ok(())) => None,
        // A grid ceiling, and a range vector at the top level of a range query, are
        // ordinary rejections, not overflows.
        Ok(Err(err)) if err.contains("too many") || err.contains("range vectors not supported") => {
            None
        }
        Ok(Err(err)) => Some(format!("{label}: error {err}")),
        Err(panic) => Some(format!(
            "{label}: panicked: {}",
            panic
                .downcast_ref::<String>()
                .cloned()
                .or_else(|| panic.downcast_ref::<&str>().map(|s| s.to_string()))
                .unwrap_or_default()
        )),
    }
}

#[test]
fn queries_at_the_ends_of_time_do_not_overflow() {
    let mut failures = Vec::new();
    for &query in QUERIES {
        // Times before the epoch clamp to 0 on the way in, so 0 is the bottom of the range.
        for (name, ts) in [
            ("max", i64::MAX),
            ("max-1h", i64::MAX - HOUR_MS),
            ("zero", 0),
        ] {
            let label = format!("instant {query:?} at {name}");
            failures.extend(check(&label, || {
                let expr = parse(query).map_err(|e| e.to_string())?;
                let stmt = EvalStmt {
                    expr,
                    start: at(ts),
                    end: at(ts),
                    interval: Duration::ZERO,
                    lookback_delta: options().lookback_delta,
                };
                evaluate_instant(reader(), stmt, at(ts), options())
                    .map(|_| ())
                    .map_err(|e| e.to_string())
            }));
        }
        for (name, start, end) in [
            ("top", i64::MAX - 10 * 60_000, i64::MAX),
            ("bottom", 0, 10 * 60_000),
        ] {
            let label = format!("range {query:?} at {name}");
            failures.extend(check(&label, || {
                let expr = parse(query).map_err(|e| e.to_string())?;
                let stmt = EvalStmt {
                    expr,
                    start: at(start),
                    end: at(end),
                    interval: Duration::from_secs(60),
                    lookback_delta: options().lookback_delta,
                };
                evaluate_range(reader(), stmt, options())
                    .map(|_| ())
                    .map_err(|e| e.to_string())
            }));
        }
    }
    assert!(
        failures.is_empty(),
        "{} failures:\n{}",
        failures.len(),
        failures.join("\n")
    );
}
