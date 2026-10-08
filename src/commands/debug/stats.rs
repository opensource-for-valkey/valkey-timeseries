use super::stats_fanout_command::{ClusterReading, ClusterValue, StatsFanoutCommand};
use crate::commands::CommandArgIterator;
use crate::common::metrics::{self, MetricReading, MetricValue, Section};
use crate::common::replies::*;
use crate::fanout::{FanoutClientCommand, is_clustered};
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

fn reply_scalar(ctx: &Context, value: &Scalar) {
    match value {
        Scalar::Int(v) => {
            reply_with_integer(ctx, *v);
        }
        Scalar::UInt(v) => {
            reply_with_integer(ctx, saturating_i64(*v));
        }
        Scalar::Float(v) => {
            reply_with_double(ctx, *v);
        }
    }
}

fn reply_metric_value(ctx: &Context, value: &MetricValue) {
    match value {
        MetricValue::Counter(v) => {
            reply_with_integer(ctx, saturating_i64(*v));
        }
        MetricValue::Gauge(v) => reply_scalar(ctx, v),
        MetricValue::Histogram(snapshot) => reply_histogram(ctx, snapshot),
    }
}

/// As [`reply_metric_value`], except that a gauge is a flat `address value ...` list, one pair
/// per node, sorted by address.
fn reply_cluster_value(ctx: &Context, value: &ClusterValue) {
    match value {
        ClusterValue::Counter(v) => {
            reply_with_integer(ctx, saturating_i64(*v));
        }
        ClusterValue::Gauge(per_node) => {
            reply_with_array(ctx, per_node.len() * 2);
            for (node, v) in per_node {
                reply_with_bulk_string(ctx, node);
                reply_scalar(ctx, v);
            }
        }
        ClusterValue::Histogram(snapshot) => reply_histogram(ctx, snapshot),
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

/// Writes the cluster view in the same layout as a single node's reply (see [`stats_cmd`]).
pub(super) fn reply_cluster_stats(ctx: &Context, readings: &[ClusterReading], verbose: bool) {
    if verbose {
        reply_with_array(ctx, readings.len());
        for reading in readings {
            // A metric only a newer peer knows has no section here; its name's prefix stands in.
            let section = match reading.section {
                Some(section) => section.as_str(),
                None => reading.name.split('_').next().unwrap_or_default(),
            };
            reply_with_array(ctx, 10);
            reply_with_str(ctx, "name");
            reply_with_bulk_string(ctx, &reading.name);
            reply_with_str(ctx, "section");
            reply_with_bulk_string(ctx, section);
            reply_with_str(ctx, "kind");
            reply_with_str(ctx, reading.kind.as_str());
            reply_with_str(ctx, "value");
            reply_cluster_value(ctx, &reading.value);
            reply_with_str(ctx, "description");
            reply_with_bulk_string(ctx, &reading.help);
        }
    } else {
        reply_with_array(ctx, readings.len() * 2);
        for reading in readings {
            reply_with_bulk_string(ctx, &reading.name);
            reply_cluster_value(ctx, &reading.value);
        }
    }
}

fn is_keyword(arg: &[u8], keyword: &[u8]) -> bool {
    arg.eq_ignore_ascii_case(keyword)
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
/// - `TS._DEBUG STATS [section ...] [VERBOSE] [LOCAL]`
/// - `TS._DEBUG STATS RESET [LOCAL]`
///
/// Without `VERBOSE`, replies with a flat `name value ...` list covering the named sections
/// (every section when none is given), section by section and by name within each. Counters
/// are integers, gauges integers or doubles; a histogram's value is a nested list (see
/// [`reply_histogram`]). With `VERBOSE`, replies with one flat key/value list per metric: name,
/// section, kind, value, description.
///
/// `RESET` starts counters and histograms over from zero, as `STATS` reports them, and leaves
/// gauges alone. It is not replicated.
///
/// In cluster mode both fan out to every node, replicas included (see [`StatsFanoutCommand`]):
/// counters and histograms are summed, and each gauge's value becomes a flat `address value ...`
/// list, one pair per node. `LOCAL` reports, or resets, the connected node alone, in the
/// single-node layout.
pub(super) fn stats_cmd(ctx: &Context, args: &mut CommandArgIterator) -> ValkeyResult<()> {
    if args
        .peek()
        .is_some_and(|arg| is_keyword(arg.as_slice(), b"RESET"))
    {
        args.next();
        let local = match args.next() {
            None => false,
            Some(arg) if is_keyword(arg.as_slice(), b"LOCAL") && args.peek().is_none() => true,
            Some(_) => {
                return Err(ValkeyError::Str(
                    "TSDB: STATS RESET takes no further arguments but LOCAL",
                ));
            }
        };
        if !local && is_clustered(ctx) {
            StatsFanoutCommand::reset().exec(ctx)?;
            return Ok(());
        }
        metrics::reset();
        reply_with_str(ctx, "OK");
        return Ok(());
    }

    let mut verbose = false;
    let mut local = false;
    let mut sections = Vec::new();
    for arg in args.by_ref() {
        let arg = arg.as_slice();
        if is_keyword(arg, b"VERBOSE") {
            verbose = true;
        } else if is_keyword(arg, b"LOCAL") {
            local = true;
        } else {
            let section = Section::parse(arg)
                .ok_or_else(|| unknown_section(&String::from_utf8_lossy(arg)))?;
            if !sections.contains(&section) {
                sections.push(section);
            }
        }
    }

    if !local && is_clustered(ctx) {
        StatsFanoutCommand::report(sections, verbose).exec(ctx)?;
        return Ok(());
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
