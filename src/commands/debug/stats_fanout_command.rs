use super::stats::reply_cluster_stats;
use crate::commands::fanout_codec::generated::debug_stats_metric::Value;
use crate::commands::fanout_codec::generated::{
    DebugStatsHistogram, DebugStatsMetric, DebugStatsRequest, DebugStatsResponse,
};
use crate::common::logging::log_warning;
use crate::common::metrics::{self, MetricKind, MetricReading, MetricValue, Section};
use crate::common::replies::{ReplyContext, reply_with_str};
use crate::config::is_debug_mode_enabled;
use crate::error_consts;
use crate::fanout::{
    FanoutClientCommand, FanoutCommandResult, FanoutContext, FanoutTarget, NodeInfo,
};
use ahash::AHashMap;
use metered::{Bucket, HistogramSnapshot, Scalar};
use valkey_module::{Context, Status, ValkeyError, ValkeyResult};

impl From<&MetricReading> for DebugStatsMetric {
    fn from(reading: &MetricReading) -> Self {
        let value = match &reading.value {
            MetricValue::Counter(v) => Value::Counter(*v),
            MetricValue::Gauge(Scalar::Int(v)) => Value::GaugeInt(*v),
            MetricValue::Gauge(Scalar::UInt(v)) => Value::GaugeUint(*v),
            MetricValue::Gauge(Scalar::Float(v)) => Value::GaugeFloat(*v),
            MetricValue::Histogram(snapshot) => Value::Histogram(DebugStatsHistogram {
                count: snapshot.count,
                sum: snapshot.sum,
                bounds: snapshot.buckets.iter().map(|b| b.le).collect(),
                cumulative_counts: snapshot
                    .buckets
                    .iter()
                    .map(|b| b.cumulative_count)
                    .collect(),
            }),
        };
        Self {
            name: reading.name.clone(),
            value: Some(value),
        }
    }
}

fn histogram_snapshot(histogram: DebugStatsHistogram) -> HistogramSnapshot {
    let buckets = histogram
        .bounds
        .into_iter()
        .zip(histogram.cumulative_counts)
        .map(|(le, count)| Bucket::new(le, count, None))
        .collect();
    HistogramSnapshot::new(buckets, histogram.sum, histogram.count)
}

/// One metric's value across the nodes that answered.
#[derive(Debug, Clone, PartialEq)]
pub(super) enum ClusterValue {
    /// Summed over nodes.
    Counter(u64),
    /// One value per node, by `host:port`, sorted by address in the reply.
    Gauge(Vec<(String, Scalar)>),
    /// Summed bucket by bucket over nodes.
    Histogram(HistogramSnapshot),
}

impl ClusterValue {
    fn kind(&self) -> MetricKind {
        match self {
            ClusterValue::Counter(_) => MetricKind::Counter,
            ClusterValue::Gauge(_) => MetricKind::Gauge,
            ClusterValue::Histogram(_) => MetricKind::Histogram,
        }
    }
}

/// One metric of the cluster view, as `TS._DEBUG STATS` reports it.
#[derive(Debug, Clone, PartialEq)]
pub(super) struct ClusterReading {
    pub name: String,
    /// `None` for a metric this node does not know (a newer peer's).
    pub section: Option<Section>,
    pub kind: MetricKind,
    /// From this node's registry; empty for a metric it does not know.
    pub help: String,
    pub value: ClusterValue,
}

/// Adds `other`'s observations to `into`. Both must come from histograms with the same bounds;
/// a peer whose bounds differ (a different version) cannot be added bucket by bucket, so it is
/// left out and `false` returned.
fn add_histogram(into: &mut HistogramSnapshot, other: &HistogramSnapshot) -> bool {
    let same_bounds = into.buckets.len() == other.buckets.len()
        && into
            .buckets
            .iter()
            .zip(&other.buckets)
            .all(|(a, b)| a.le.total_cmp(&b.le).is_eq());
    if !same_bounds {
        return false;
    }
    for (bucket, theirs) in into.buckets.iter_mut().zip(&other.buckets) {
        bucket.cumulative_count = bucket
            .cumulative_count
            .saturating_add(theirs.cumulative_count);
    }
    into.count = into.count.saturating_add(other.count);
    into.sum += other.sum;
    true
}

/// The metrics of every node that answered, merged by name.
#[derive(Default)]
struct ClusterStats {
    values: AHashMap<String, ClusterValue>,
}

impl ClusterStats {
    fn add_node(&mut self, node: &str, response: DebugStatsResponse) {
        for metric in response.metrics {
            let Some(value) = metric.value else {
                continue;
            };
            self.add(node, metric.name, value);
        }
    }

    fn add(&mut self, node: &str, name: String, value: Value) {
        let gauge = |scalar| ClusterValue::Gauge(vec![(node.to_owned(), scalar)]);
        let incoming = match value {
            Value::Counter(v) => ClusterValue::Counter(v),
            Value::GaugeInt(v) => gauge(Scalar::Int(v)),
            Value::GaugeUint(v) => gauge(Scalar::UInt(v)),
            Value::GaugeFloat(v) => gauge(Scalar::Float(v)),
            Value::Histogram(h) => ClusterValue::Histogram(histogram_snapshot(h)),
        };
        let Some(existing) = self.values.get_mut(&name) else {
            self.values.insert(name, incoming);
            return;
        };
        let merged = match (existing, incoming) {
            (ClusterValue::Counter(total), ClusterValue::Counter(v)) => {
                *total = total.saturating_add(v);
                true
            }
            (ClusterValue::Gauge(per_node), ClusterValue::Gauge(mut theirs)) => {
                per_node.append(&mut theirs);
                true
            }
            (ClusterValue::Histogram(total), ClusterValue::Histogram(theirs)) => {
                add_histogram(total, &theirs)
            }
            _ => false,
        };
        if !merged {
            log_warning(format!(
                "TS._DEBUG STATS: left node {node}'s {name} out of the cluster view: its kind or buckets differ from another node's"
            ));
        }
    }

    /// The merged metrics in reporting order — by section, then by family name, as a single
    /// node reports them — with this node's kind and help. Metrics this node does not know go
    /// last, by name.
    fn into_readings(self, local: &[MetricReading]) -> Vec<ClusterReading> {
        let help: AHashMap<&str, &str> = local
            .iter()
            .map(|r| (r.name.as_str(), r.help.as_str()))
            .collect();
        let mut readings: Vec<_> = self
            .values
            .into_iter()
            .map(|(name, mut value)| {
                if let ClusterValue::Gauge(per_node) = &mut value {
                    per_node.sort_by(|a, b| a.0.cmp(&b.0));
                }
                let section = name
                    .split_once('_')
                    .and_then(|(prefix, _)| Section::parse(prefix.as_bytes()));
                ClusterReading {
                    help: help.get(name.as_str()).copied().unwrap_or("").to_owned(),
                    section,
                    kind: value.kind(),
                    name,
                    value,
                }
            })
            .collect();
        readings.sort_by(|a, b| {
            let rank = |r: &ClusterReading| {
                let section = r
                    .section
                    .and_then(|s| Section::ALL.iter().position(|&x| x == s))
                    .unwrap_or(usize::MAX);
                let family = match r.kind {
                    MetricKind::Counter => r.name.strip_suffix("_total").unwrap_or(&r.name),
                    _ => r.name.as_str(),
                };
                (section, family.to_owned())
            };
            rank(a).cmp(&rank(b))
        });
        readings
    }
}

/// `TS._DEBUG STATS` (and `STATS RESET`) across the cluster: every node, replicas included,
/// reports its own metrics, since each counts only what happened on it. The coordinator sums
/// counters and histograms, and lists each gauge per node: a sum of refresh intervals or ages
/// means nothing, and a per-node queue depth says where the queue is.
///
/// Each node gates its own internals on `debug-mode`, as for a direct `TS._DEBUG` call, so one
/// node with it off fails the command.
#[derive(Default)]
pub struct StatsFanoutCommand {
    sections: Vec<Section>,
    verbose: bool,
    reset: bool,
    stats: ClusterStats,
}

impl StatsFanoutCommand {
    pub fn report(sections: Vec<Section>, verbose: bool) -> Self {
        Self {
            sections,
            verbose,
            ..Default::default()
        }
    }

    pub fn reset() -> Self {
        Self {
            reset: true,
            ..Default::default()
        }
    }
}

impl FanoutClientCommand for StatsFanoutCommand {
    type Request = DebugStatsRequest;
    type Response = DebugStatsResponse;

    fn name() -> &'static str {
        "cmd::debug_stats"
    }

    /// Reads atomics and the registries; takes no GIL and holds no keys.
    fn get_local_response(
        _ctx: &FanoutContext,
        req: DebugStatsRequest,
    ) -> ValkeyResult<DebugStatsResponse> {
        if !is_debug_mode_enabled() {
            return Err(ValkeyError::Str(error_consts::DEBUG_MODE_DISABLED));
        }
        if req.reset {
            metrics::reset();
            return Ok(DebugStatsResponse::default());
        }
        let sections: Vec<Section> = req
            .sections
            .iter()
            .filter_map(|name| Section::parse(name.as_bytes()))
            .collect();
        // Every name unknown here means none of the requested sections exist on this node: report
        // nothing rather than everything.
        if sections.is_empty() && !req.sections.is_empty() {
            return Ok(DebugStatsResponse::default());
        }
        Ok(DebugStatsResponse {
            metrics: metrics::snapshot(&sections)
                .iter()
                .map(DebugStatsMetric::from)
                .collect(),
        })
    }

    fn generate_request(&self) -> DebugStatsRequest {
        DebugStatsRequest {
            sections: self
                .sections
                .iter()
                .map(|s| s.as_str().to_owned())
                .collect(),
            reset: self.reset,
        }
    }

    fn get_targets(&self, _ctx: &Context) -> FanoutTarget {
        FanoutTarget::All
    }

    fn on_response(&mut self, resp: Self::Response, target: &NodeInfo) -> FanoutCommandResult {
        self.stats
            .add_node(&target.socket_address.to_string(), resp);
        Ok(())
    }

    fn reply(&mut self, ctx: &ReplyContext) -> Status {
        let ctx = ctx.context();
        if self.reset {
            reply_with_str(ctx, "OK");
            return Status::Ok;
        }
        let local = metrics::snapshot(&self.sections);
        let readings = std::mem::take(&mut self.stats).into_readings(&local);
        reply_cluster_stats(ctx, &readings, self.verbose);
        Status::Ok
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use metered::{BucketHistogram, Buckets};

    fn reading(name: &str, value: MetricValue) -> MetricReading {
        let kind = match value {
            MetricValue::Counter(_) => MetricKind::Counter,
            MetricValue::Gauge(_) => MetricKind::Gauge,
            MetricValue::Histogram(_) => MetricKind::Histogram,
        };
        MetricReading {
            name: name.to_owned(),
            section: Section::Fanout,
            kind,
            help: format!("help for {name}"),
            value,
        }
    }

    fn histogram(observations: &[f64]) -> HistogramSnapshot {
        let h = BucketHistogram::new(Buckets::custom([1.0, 2.0, 4.0]));
        for &v in observations {
            h.observe(v);
        }
        h.snapshot()
    }

    fn node_response(readings: &[MetricReading]) -> DebugStatsResponse {
        DebugStatsResponse {
            metrics: readings.iter().map(DebugStatsMetric::from).collect(),
        }
    }

    #[test]
    fn readings_round_trip_through_the_wire_format() {
        for value in [
            MetricValue::Counter(7),
            MetricValue::Gauge(Scalar::Int(-3)),
            MetricValue::Gauge(Scalar::UInt(9)),
            MetricValue::Gauge(Scalar::Float(0.25)),
        ] {
            let metric = DebugStatsMetric::from(&reading("fanout_x", value.clone()));
            let mut stats = ClusterStats::default();
            stats.add("n1:1", metric.name, metric.value.unwrap());
            let expected = match value {
                MetricValue::Counter(v) => ClusterValue::Counter(v),
                MetricValue::Gauge(s) => ClusterValue::Gauge(vec![("n1:1".into(), s)]),
                MetricValue::Histogram(_) => unreachable!(),
            };
            assert_eq!(stats.values["fanout_x"], expected);
        }

        let snapshot = histogram(&[0.5, 3.0, 9.0]);
        let DebugStatsMetric { value, .. } = DebugStatsMetric::from(&reading(
            "fanout_h",
            MetricValue::Histogram(snapshot.clone()),
        ));
        let Some(Value::Histogram(wire)) = value else {
            panic!("not a histogram");
        };
        assert_eq!(histogram_snapshot(wire), snapshot);
    }

    #[test]
    fn counters_and_histograms_sum_and_gauges_stay_per_node() {
        let mut stats = ClusterStats::default();
        for (node, n, gauge, observations) in [
            ("10.0.0.2:7001", 5, 1, &[0.5, 3.0][..]),
            ("10.0.0.1:7000", 2, 4, &[9.0][..]),
        ] {
            stats.add_node(
                node,
                node_response(&[
                    reading("fanout_operations_total", MetricValue::Counter(n)),
                    reading("fanout_inflight", MetricValue::Gauge(Scalar::Int(gauge))),
                    reading(
                        "fanout_duration_seconds",
                        MetricValue::Histogram(histogram(observations)),
                    ),
                ]),
            );
        }
        let readings = stats.into_readings(&[]);
        let by_name: AHashMap<_, _> = readings.iter().map(|r| (r.name.as_str(), r)).collect();
        assert_eq!(
            by_name["fanout_operations_total"].value,
            ClusterValue::Counter(7)
        );
        assert_eq!(
            by_name["fanout_inflight"].value,
            ClusterValue::Gauge(vec![
                ("10.0.0.1:7000".into(), Scalar::Int(4)),
                ("10.0.0.2:7001".into(), Scalar::Int(1)),
            ])
        );
        assert_eq!(
            by_name["fanout_duration_seconds"].value,
            ClusterValue::Histogram(histogram(&[0.5, 3.0, 9.0]))
        );
    }

    #[test]
    fn a_histogram_with_other_bounds_is_left_out() {
        let mut stats = ClusterStats::default();
        let ours = histogram(&[0.5]);
        stats.add_node(
            "a:1",
            node_response(&[reading("fanout_h", MetricValue::Histogram(ours.clone()))]),
        );
        let other = BucketHistogram::new(Buckets::custom([10.0]));
        other.observe(1.0);
        stats.add_node(
            "b:1",
            node_response(&[reading(
                "fanout_h",
                MetricValue::Histogram(other.snapshot()),
            )]),
        );
        assert_eq!(stats.values["fanout_h"], ClusterValue::Histogram(ours));
    }

    #[test]
    fn readings_come_in_section_then_family_order_with_local_help() {
        let mut stats = ClusterStats::default();
        stats.add_node(
            "a:1",
            node_response(&[
                reading("fanout_ticks_total", MetricValue::Counter(1)),
                reading("zzz_from_a_newer_peer", MetricValue::Counter(1)),
                reading("fanout_tick_seconds", MetricValue::Gauge(Scalar::Int(1))),
                reading("cron_ticks_total", MetricValue::Counter(1)),
            ]),
        );
        let local = [reading("cron_ticks_total", MetricValue::Counter(0))];
        let readings = stats.into_readings(&local);
        let names: Vec<_> = readings.iter().map(|r| r.name.as_str()).collect();
        assert_eq!(
            names,
            [
                "cron_ticks_total",
                "fanout_tick_seconds",
                "fanout_ticks_total",
                "zzz_from_a_newer_peer"
            ]
        );
        assert_eq!(readings[0].help, "help for cron_ticks_total");
        assert_eq!(readings[0].section, Some(Section::Cron));
        assert_eq!(readings[3].section, None);
        assert_eq!(readings[3].help, "");
    }
}
