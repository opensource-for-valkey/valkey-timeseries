use super::stats_fanout_command::{ClusterReading, ClusterValue, StatsFanoutCommand};
use super::target::{parse_node_target, resolve_node_target};
use crate::commands::CommandArgIterator;
use crate::common::metrics::{self, MetricReading, MetricValue, Section};
use crate::common::replies::*;
use crate::fanout::{FanoutClientCommand, FanoutTarget};
use metered::{HistogramSnapshot, Scalar};
use valkey_module::{Context, NextArg, ValkeyError, ValkeyResult};

fn saturating_i64(value: u64) -> i64 {
    i64::try_from(value).unwrap_or(i64::MAX)
}

/// Emits a histogram as a map: `count`, `sum`, then `buckets`, an array of
/// `[le, count]` pairs with cumulative counts. Only the finite buckets are listed: the `+Inf`
/// bucket's count is `count`.
fn reply_histogram(ctx: &Context, snapshot: &HistogramSnapshot) {
    let finite: Vec<_> = snapshot
        .buckets
        .iter()
        .filter(|bucket| bucket.le.is_finite())
        .collect();
    reply_with_map(ctx, 3);
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

/// As [`reply_metric_value`], except that a gauge is a map from node address to value, one entry
/// per node, sorted by address.
fn reply_cluster_value(ctx: &Context, value: &ClusterValue) {
    match value {
        ClusterValue::Counter(v) => {
            reply_with_integer(ctx, saturating_i64(*v));
        }
        ClusterValue::Gauge(per_node) => {
            reply_with_map(ctx, per_node.len());
            for (node, v) in per_node {
                reply_with_bulk_string(ctx, node);
                reply_scalar(ctx, v);
            }
        }
        ClusterValue::Histogram(snapshot) => reply_histogram(ctx, snapshot),
    }
}

/// Emits one metric in verbose format as a map, like `LIST_CONFIGS VERBOSE`.
fn reply_metric_verbose(ctx: &Context, reading: &MetricReading) {
    reply_with_map(ctx, 5);
    reply_with_str(ctx, "name");
    reply_with_bulk_string(ctx, &reading.name);
    reply_with_str(ctx, "section");
    reply_with_bulk_string(ctx, reading.section.as_str());
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
            reply_with_map(ctx, 5);
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
        reply_with_map(ctx, readings.len());
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
/// - `TS._DEBUG STATS [section ...] [VERBOSE] [<target>]`
/// - `TS._DEBUG STATS RESET [<target>]`
///
/// where `<target>` is `LOCAL`, `PRIMARY`, `CLUSTER` or `HASHTAG <tag>[,<tag>...]`, at most once
/// and, for the report, in any position. Both read, or reset, the connected node alone without
/// a target or with `LOCAL`.
///
/// Without `VERBOSE`, replies with a `name => value` map covering the named sections
/// (every section when none is given), section by section and by name within each. Counters
/// are integers, gauges integers or doubles; a histogram's value is a nested map (see
/// [`reply_histogram`]). With `VERBOSE`, replies with one map per metric: name,
/// section, kind, value, description.
///
/// `RESET` starts counters and histograms over from zero, as `STATS` reports them, and leaves
/// gauges alone. It is not replicated.
///
/// With any other target both fan out to those nodes (see [`StatsFanoutCommand`]); `CLUSTER`
/// asks every node, replicas included, `PRIMARY` the shard primaries, and `HASHTAG` the primaries
/// of the shards owning those tags. Counters and histograms are summed, and each gauge's value becomes an
/// `address => value` map, one entry per node. Any target but `LOCAL` on a standalone server is
/// an error.
pub(super) fn stats_cmd(ctx: &Context, args: &mut CommandArgIterator) -> ValkeyResult<()> {
    if args
        .peek()
        .is_some_and(|arg| is_keyword(arg.as_slice(), b"RESET"))
    {
        args.next();
        let mut target = None;
        parse_node_target(args, &mut target)?;
        if args.peek().is_some() {
            return Err(ValkeyError::Str(
                "TSDB: STATS RESET takes no further arguments but a node target",
            ));
        }
        if let Some(target) = resolve_node_target(ctx, target, FanoutTarget::All)? {
            StatsFanoutCommand::reset(target).exec(ctx)?;
            return Ok(());
        }
        metrics::reset();
        reply_with_str(ctx, "OK");
        return Ok(());
    }

    let mut verbose = false;
    let mut target = None;
    let mut sections = Vec::new();
    while let Some(arg) = args.peek() {
        if is_keyword(arg.as_slice(), b"VERBOSE") {
            args.next();
            verbose = true;
        } else if !parse_node_target(args, &mut target)? {
            let arg = args.next_arg()?;
            let arg = arg.as_slice();
            let section = Section::parse(arg)
                .ok_or_else(|| unknown_section(&String::from_utf8_lossy(arg)))?;
            if !sections.contains(&section) {
                sections.push(section);
            }
        }
    }

    if let Some(target) = resolve_node_target(ctx, target, FanoutTarget::All)? {
        StatsFanoutCommand::report(sections, verbose, target).exec(ctx)?;
        return Ok(());
    }

    let readings = metrics::snapshot(&sections);
    if verbose {
        reply_with_array(ctx, readings.len());
        for reading in &readings {
            reply_metric_verbose(ctx, reading);
        }
    } else {
        reply_with_map(ctx, readings.len());
        for reading in &readings {
            reply_with_bulk_string(ctx, &reading.name);
            reply_metric_value(ctx, &reading.value);
        }
    }
    Ok(())
}
