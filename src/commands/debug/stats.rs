use crate::commands::CommandArgIterator;
use crate::common::metrics::{self, MetricReading, MetricValue, Section};
use crate::common::replies::*;
use metered::{HistogramSnapshot, Scalar};
use valkey_module::{Context, ValkeyError, ValkeyResult};

fn saturating_i64(value: u64) -> i64 {
    i64::try_from(value).unwrap_or(i64::MAX)
}

/// Emits a histogram as a flat key/value list: `count`, `sum`, then `buckets`, an array of
/// `[le, count]` pairs with cumulative counts. Only the finite buckets are listed: the `+Inf`
/// bucket's count is `count`.
fn reply_histogram(ctx: &Context, snapshot: &HistogramSnapshot) {
    let finite: Vec<_> = snapshot
        .buckets
        .iter()
        .filter(|bucket| bucket.le.is_finite())
        .collect();
    reply_with_array(ctx, 6);
    reply_with_str(ctx, "count");
    reply_with_integer(ctx, saturating_i64(snapshot.count));
    reply_with_str(ctx, "sum");
    reply_with_double(ctx, snapshot.sum);
    reply_with_str(ctx, "buckets");
    reply_with_array(ctx, finite.len());
    for bucket in finite {
        reply_with_array(ctx, 2);
        reply_with_double(ctx, bucket.le);
        reply_with_integer(ctx, saturating_i64(bucket.cumulative_count));
    }
}

fn reply_metric_value(ctx: &Context, value: &MetricValue) {
    match value {
        MetricValue::Counter(v) => {
            reply_with_integer(ctx, saturating_i64(*v));
        }
        MetricValue::Gauge(Scalar::Int(v)) => {
            reply_with_integer(ctx, *v);
        }
        MetricValue::Gauge(Scalar::UInt(v)) => {
            reply_with_integer(ctx, saturating_i64(*v));
        }
        MetricValue::Gauge(Scalar::Float(v)) => {
            reply_with_double(ctx, *v);
        }
        MetricValue::Histogram(snapshot) => reply_histogram(ctx, snapshot),
    }
}

/// Emits one metric in verbose format as a flat key/value list, like `LIST_CONFIGS VERBOSE`.
fn reply_metric_verbose(ctx: &Context, reading: &MetricReading) {
    reply_with_array(ctx, 10);
    reply_with_str(ctx, "name");
    reply_with_bulk_string(ctx, &reading.name);
    reply_with_str(ctx, "section");
    reply_with_str(ctx, reading.section.as_str());
    reply_with_str(ctx, "kind");
    reply_with_str(ctx, reading.kind.as_str());
    reply_with_str(ctx, "value");
    reply_metric_value(ctx, &reading.value);
    reply_with_str(ctx, "description");
    reply_with_bulk_string(ctx, &reading.help);
}

fn unknown_section(name: &str) -> ValkeyError {
    let valid: Vec<_> = Section::ALL.iter().map(|s| s.as_str()).collect();
    ValkeyError::String(format!(
        "TSDB: unknown STATS section '{name}' (valid sections: {})",
        valid.join(", ")
    ))
}

/// Reports module metrics, or starts them over.
///
/// Syntax:
/// - `TS._DEBUG STATS [section ...] [VERBOSE]`
/// - `TS._DEBUG STATS RESET`
///
/// Without `VERBOSE`, replies with a flat `name value ...` list covering the named sections
/// (every section when none is given), section by section and by name within each. Counters
/// are integers, gauges integers or doubles; a histogram's value is a nested list (see
/// [`reply_histogram`]). With `VERBOSE`, replies with one flat key/value list per metric: name,
/// section, kind, value, description.
///
/// `RESET` starts counters and histograms over from zero, as `STATS` reports them, and leaves
/// gauges alone. It is node-local and not replicated.
pub(super) fn stats_cmd(ctx: &Context, args: &mut CommandArgIterator) -> ValkeyResult<()> {
    if args
        .peek()
        .is_some_and(|arg| arg.as_slice().eq_ignore_ascii_case(b"RESET"))
    {
        args.next();
        if args.peek().is_some() {
            return Err(ValkeyError::Str(
                "TSDB: STATS RESET takes no further arguments",
            ));
        }
        metrics::reset();
        reply_with_str(ctx, "OK");
        return Ok(());
    }

    let mut verbose = false;
    let mut sections = Vec::new();
    for arg in args.by_ref() {
        let arg = arg.as_slice();
        if arg.eq_ignore_ascii_case(b"VERBOSE") {
            verbose = true;
        } else {
            let section = Section::parse(arg)
                .ok_or_else(|| unknown_section(&String::from_utf8_lossy(arg)))?;
            if !sections.contains(&section) {
                sections.push(section);
            }
        }
    }

    let readings = metrics::snapshot(&sections);
    if verbose {
        reply_with_array(ctx, readings.len());
        for reading in &readings {
            reply_metric_verbose(ctx, reading);
        }
    } else {
        reply_with_array(ctx, readings.len() * 2);
        for reading in &readings {
            reply_with_bulk_string(ctx, &reading.name);
            reply_metric_value(ctx, &reading.value);
        }
    }
    Ok(())
}
