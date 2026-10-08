//! Module metrics, built on [`metered`]: one registry per [`Section`], read by `TS._DEBUG STATS`.
//!
//! Each section is a `metered::Registry` that prefixes its metric names with the section's
//! name. A metric is registered once, with its help text, in [`build_registry`]. Where the code
//! already keeps the value, the registry reads that state directly: a std atomic is a metered
//! counter or gauge as it stands, and an executor's own counts are read through a closure.
//! `TS._DEBUG STATS` reports from the registries, and so will the `INFO ts_stats` mirror and any
//! OpenMetrics export. The design and the roster of planned metrics are in
//! `docs/plans/observability-metrics-plan.md`.
//!
//! Collection is always on: a counter is one relaxed atomic add, a histogram observation a
//! bucket search, an atomic add and a compare-and-swap on the sum. Only the read surface is
//! gated (by `debug-mode`, like the rest of `TS._DEBUG`), so the history is there when an
//! operator turns it on.
//!
//! Naming follows OpenMetrics, as metered renders it: a counter is registered without its
//! `_total` suffix, which its sample name gains, and a duration is a histogram in seconds.
//!
//! Metered values are monotonic, as a scraper expects. `TS._DEBUG STATS RESET` is a view over
//! them: it records the current counters and histograms as a baseline, and [`snapshot`] reports
//! the difference. Nothing a scraper would read ever goes backwards.
//!
//! Reads are relaxed loads, and a snapshot is not atomic across metrics: two values read in one
//! snapshot may straddle an update. That is fine for diagnostics.
//!
//! Metrics are node-local. Two rules keep them honest:
//!
//! - Reading a metric must not walk the keyspace or the index. A gauge is a load, or a call
//!   that is itself O(1).
//! - Nothing may be counted in a forked child (`rdb_save`, the index aux save, `aof_rewrite`):
//!   the parent never sees it. Count on the receiving side instead.

use crate::commands::analysis_lane_stats;
use crate::common::sync::lock;
use crate::fanout::request_lane_stats;
use crate::series::background_tasks::{CRON_TICKS, cron_interval_ms};
use metered::entry::{counter, counter_value, gauge_value, metric};
use metered::{
    Bucket, BucketHistogram, Buckets, HistogramData, HistogramSnapshot, MetricSampleValue,
    MetricType, MetricValues, Registry, Scalar,
};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{LazyLock, Mutex};
use std::time::Duration;

/// Finite buckets of a duration histogram: powers of two from 1 µs to 2^23 µs (about 8.4 s).
/// Slower observations land in the implicit `+Inf` bucket.
pub const DURATION_BUCKETS: usize = 24;

/// The bounds of a duration histogram, in seconds. See [`DURATION_BUCKETS`].
pub fn duration_buckets() -> Buckets {
    Buckets::exponential_duration(Duration::from_micros(1), 2.0, DURATION_BUCKETS)
}

// --- cron ---------------------------------------------------------------------------------

/// Cron ticks skipped because the server was loading or shutting down.
pub static CRON_TICKS_SKIPPED: AtomicU64 = AtomicU64::new(0);

/// Main-thread time per cron tick spent dispatching background tasks.
pub static CRON_TICK_DURATION: LazyLock<BucketHistogram> =
    LazyLock::new(|| BucketHistogram::new(duration_buckets()));

// --- fanout -------------------------------------------------------------------------------

/// Fanout messages of one type this node handed to the cluster bus, and their payload bytes.
///
/// The payload is what the module passes to `ValkeyModule_SendClusterMessage`; the bus adds its
/// own framing, which the module cannot see. A request to several peers is sent, and counted,
/// once per peer. A fanout's local share never touches the bus and is not counted.
pub struct WireCounters {
    pub messages: AtomicU64,
    pub bytes: AtomicU64,
}

impl WireCounters {
    pub const fn new() -> Self {
        Self {
            messages: AtomicU64::new(0),
            bytes: AtomicU64::new(0),
        }
    }

    #[inline]
    pub fn record(&self, payload_len: usize) {
        self.messages.fetch_add(1, Ordering::Relaxed);
        self.bytes.fetch_add(payload_len as u64, Ordering::Relaxed);
    }
}

impl Default for WireCounters {
    fn default() -> Self {
        Self::new()
    }
}

/// Requests sent to peers, as coordinator.
pub static FANOUT_REQUESTS_SENT: WireCounters = WireCounters::new();
/// Successful responses sent back to coordinators, as a peer.
pub static FANOUT_RESPONSES_SENT: WireCounters = WireCounters::new();
/// Error responses sent back to coordinators, as a peer.
pub static FANOUT_ERROR_RESPONSES_SENT: WireCounters = WireCounters::new();

/// Groups of metrics, selectable in `TS._DEBUG STATS`. Each is one registry, and each metric's
/// name starts with its section's name.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Section {
    /// The module's cron handler, which schedules the periodic background tasks.
    Cron,
    /// Bounded executors: the lanes that run blocking, GIL-taking work off the main thread.
    Exec,
    /// Cluster fanout: messages exchanged with peers over the cluster bus.
    Fanout,
}

impl Section {
    /// Every section, in reporting order.
    pub const ALL: &'static [Section] = &[Section::Cron, Section::Exec, Section::Fanout];

    pub const fn as_str(self) -> &'static str {
        match self {
            Section::Cron => "cron",
            Section::Exec => "exec",
            Section::Fanout => "fanout",
        }
    }

    pub fn parse(name: &[u8]) -> Option<Section> {
        Self::ALL
            .iter()
            .copied()
            .find(|section| name.eq_ignore_ascii_case(section.as_str().as_bytes()))
    }
}

/// Builds a section's registry: the one place its metrics are declared. Order doesn't matter:
/// metered reports a registry's metrics sorted by name.
fn build_registry(section: Section) -> Registry<'static> {
    let mut registry = Registry::with_prefix(section.as_str());
    match section {
        Section::Cron => {
            registry
                .register(
                    counter("ticks")
                        .source(&CRON_TICKS)
                        .help("Cron ticks that ran the background-task scheduler"),
                )
                .register(
                    counter("ticks_skipped")
                        .source(&CRON_TICKS_SKIPPED)
                        .help("Cron ticks skipped because the server was loading or shutting down"),
                )
                .register(
                    gauge_value("interval_seconds")
                        .read(|_: &()| cron_interval_ms() as f64 / 1000.0)
                        .help("Time between cron ticks, derived from the server's hz")
                        .unit("seconds"),
                )
                .register(
                    metric("tick_duration_seconds")
                        .source(LazyLock::force(&CRON_TICK_DURATION))
                        .help("Main-thread time per cron tick spent dispatching background tasks")
                        .unit("seconds"),
                );
        }
        Section::Exec => {
            registry
                .register(
                    gauge_value("fanout_queued")
                        .read(|_: &()| request_lane_stats().queued)
                        .help("Jobs waiting for a worker on the ts-fanout-request lane (peer requests and local fanout shares)"),
                )
                .register(
                    gauge_value("fanout_running")
                        .read(|_: &()| request_lane_stats().running)
                        .help("Jobs a worker is running on the ts-fanout-request lane"),
                )
                .register(
                    counter_value("fanout_rejected")
                        .read(|_: &()| request_lane_stats().rejected)
                        .help("Jobs refused because the ts-fanout-request queue was full (answered as busy)"),
                )
                .register(
                    gauge_value("analysis_queued")
                        .read(|_: &()| analysis_lane_stats().queued)
                        .help("Jobs waiting for a worker on the ts-analysis lane"),
                )
                .register(
                    gauge_value("analysis_running")
                        .read(|_: &()| analysis_lane_stats().running)
                        .help("Jobs a worker is running on the ts-analysis lane"),
                )
                .register(
                    counter_value("analysis_rejected")
                        .read(|_: &()| analysis_lane_stats().rejected)
                        .help("Jobs refused because the ts-analysis queue was full"),
                );
        }
        Section::Fanout => {
            registry
                .register(
                    counter("requests_sent")
                        .source(&FANOUT_REQUESTS_SENT.messages)
                        .help("Fanout requests sent to peers over the cluster bus, one per peer"),
                )
                .register(
                    counter("request_sent_bytes")
                        .source(&FANOUT_REQUESTS_SENT.bytes)
                        .help("Payload bytes of the fanout requests sent to peers")
                        .unit("bytes"),
                )
                .register(
                    counter("responses_sent")
                        .source(&FANOUT_RESPONSES_SENT.messages)
                        .help("Fanout responses sent back to coordinators over the cluster bus"),
                )
                .register(
                    counter("response_sent_bytes")
                        .source(&FANOUT_RESPONSES_SENT.bytes)
                        .help("Payload bytes of the fanout responses sent back to coordinators")
                        .unit("bytes"),
                )
                .register(
                    counter("error_responses_sent")
                        .source(&FANOUT_ERROR_RESPONSES_SENT.messages)
                        .help(
                            "Fanout error responses sent back to coordinators over the cluster bus",
                        ),
                )
                .register(
                    counter("error_response_sent_bytes")
                        .source(&FANOUT_ERROR_RESPONSES_SENT.bytes)
                        .help(
                            "Payload bytes of the fanout error responses sent back to coordinators",
                        )
                        .unit("bytes"),
                );
        }
    }
    registry
}

/// Every section's registry, in [`Section::ALL`] order.
static REGISTRIES: LazyLock<Vec<(Section, Registry<'static>)>> = LazyLock::new(|| {
    Section::ALL
        .iter()
        .map(|&section| (section, build_registry(section)))
        .collect()
});

/// The kinds of metric the registries hold.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MetricKind {
    Counter,
    Gauge,
    Histogram,
}

impl MetricKind {
    fn of(metric_type: MetricType) -> Option<MetricKind> {
        match metric_type {
            MetricType::Counter => Some(MetricKind::Counter),
            MetricType::Gauge => Some(MetricKind::Gauge),
            MetricType::Histogram => Some(MetricKind::Histogram),
            _ => None,
        }
    }

    pub const fn as_str(self) -> &'static str {
        match self {
            MetricKind::Counter => "counter",
            MetricKind::Gauge => "gauge",
            MetricKind::Histogram => "histogram",
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum MetricValue {
    Counter(u64),
    Gauge(Scalar),
    Histogram(HistogramSnapshot),
}

/// One metric's value, as `TS._DEBUG STATS` reports it.
#[derive(Debug, Clone, PartialEq)]
pub struct MetricReading {
    /// The sample name: the family name, plus `_total` for a counter.
    pub name: String,
    pub section: Section,
    pub kind: MetricKind,
    pub help: String,
    pub value: MetricValue,
}

fn sample_value(values: &MetricValues, name: &str) -> Option<MetricSampleValue> {
    values
        .samples()
        .iter()
        .find(|sample| sample.name == name)
        .map(|sample| sample.value)
}

fn to_scalar(value: MetricSampleValue) -> Scalar {
    match value {
        MetricSampleValue::Int(v) => Scalar::Int(v),
        MetricSampleValue::UInt(v) => Scalar::UInt(v),
        MetricSampleValue::Float(v) => Scalar::Float(v),
    }
}

fn to_u64(value: MetricSampleValue) -> u64 {
    match value {
        MetricSampleValue::UInt(v) => v,
        MetricSampleValue::Int(v) => u64::try_from(v).unwrap_or(0),
        MetricSampleValue::Float(v) => v as u64,
    }
}

/// Reads one registry's metrics as metered reports them, before any baseline, sorted by family
/// name (the order of metered's schema, and of its OpenMetrics output).
fn read_registry(section: Section, registry: &Registry<'static>) -> Vec<MetricReading> {
    let schema = registry.schema();
    let values = registry.values();
    schema
        .families()
        .iter()
        .filter_map(|family| {
            let kind = MetricKind::of(family.metric_type)?;
            let (name, value) = match kind {
                MetricKind::Counter => {
                    let name = format!("{}_total", family.name);
                    let value = to_u64(sample_value(&values, &name)?);
                    (name, MetricValue::Counter(value))
                }
                MetricKind::Gauge => {
                    let value = to_scalar(sample_value(&values, &family.name)?);
                    (family.name.clone(), MetricValue::Gauge(value))
                }
                MetricKind::Histogram => {
                    let histogram = values
                        .histograms()
                        .iter()
                        .find(|histogram| histogram.name == family.name)?;
                    let HistogramData::Classic(snapshot) = &histogram.data else {
                        return None;
                    };
                    (
                        family.name.clone(),
                        MetricValue::Histogram(snapshot.clone()),
                    )
                }
            };
            Some(MetricReading {
                name,
                section,
                kind,
                help: family
                    .help
                    .as_ref()
                    .map(|help| help.as_str().to_owned())
                    .unwrap_or_default(),
                value,
            })
        })
        .collect()
}

/// What the last `TS._DEBUG STATS RESET` read, keyed by sample name.
#[derive(Default)]
struct Baseline {
    counters: HashMap<String, u64>,
    histograms: HashMap<String, HistogramSnapshot>,
}

impl Baseline {
    fn record(&mut self, reading: MetricReading) {
        match reading.value {
            MetricValue::Counter(value) => {
                self.counters.insert(reading.name, value);
            }
            MetricValue::Histogram(snapshot) => {
                self.histograms.insert(reading.name, snapshot);
            }
            MetricValue::Gauge(_) => {}
        }
    }

    fn apply(&self, reading: &mut MetricReading) {
        match &mut reading.value {
            MetricValue::Counter(value) => {
                if let Some(base) = self.counters.get(&reading.name) {
                    *value = value.saturating_sub(*base);
                }
            }
            MetricValue::Histogram(snapshot) => {
                if let Some(base) = self.histograms.get(&reading.name) {
                    *snapshot = subtract_histogram(snapshot, base);
                }
            }
            MetricValue::Gauge(_) => {}
        }
    }
}

static BASELINE: LazyLock<Mutex<Baseline>> = LazyLock::new(Mutex::default);

/// The observations in `current` that arrived after `base` was taken. Both must come from the
/// same histogram; if their buckets differ, `current` is returned as it is.
fn subtract_histogram(current: &HistogramSnapshot, base: &HistogramSnapshot) -> HistogramSnapshot {
    if current.buckets.len() != base.buckets.len() {
        return current.clone();
    }
    let buckets = current
        .buckets
        .iter()
        .zip(&base.buckets)
        .map(|(now, then)| {
            Bucket::new(
                now.le,
                now.cumulative_count.saturating_sub(then.cumulative_count),
                now.exemplar.clone(),
            )
        })
        .collect();
    HistogramSnapshot::new(
        buckets,
        (current.sum - base.sum).max(0.0),
        current.count.saturating_sub(base.count),
    )
}

/// Reads the metrics of the given sections, section by section in [`Section::ALL`] order and by
/// family name within each; every section when `sections` is empty. Counters and histograms
/// count from the last [`reset`].
pub fn snapshot(sections: &[Section]) -> Vec<MetricReading> {
    let mut readings: Vec<_> = REGISTRIES
        .iter()
        .filter(|(section, _)| sections.is_empty() || sections.contains(section))
        .flat_map(|(section, registry)| read_registry(*section, registry))
        .collect();
    let baseline = lock(&BASELINE);
    for reading in &mut readings {
        baseline.apply(reading);
    }
    readings
}

/// Starts counters and histograms over from zero, as [`snapshot`] reports them. The metered
/// values underneath are left alone, and so are gauges.
pub fn reset() {
    let mut baseline = Baseline::default();
    for (section, registry) in REGISTRIES.iter() {
        for reading in read_registry(*section, registry) {
            baseline.record(reading);
        }
    }
    *lock(&BASELINE) = baseline;
}

#[cfg(test)]
mod tests {
    use super::*;
    use metered::MetricSchema;

    fn schemas() -> impl Iterator<Item = (Section, MetricSchema)> {
        REGISTRIES
            .iter()
            .map(|(section, registry)| (*section, registry.schema()))
    }

    fn reading(sections: &[Section], name: &str) -> MetricReading {
        snapshot(sections)
            .into_iter()
            .find(|reading| reading.name == name)
            .unwrap_or_else(|| panic!("{name} is not registered"))
    }

    #[test]
    fn duration_buckets_are_powers_of_two_microseconds() {
        let buckets = duration_buckets();
        assert_eq!(buckets.bounds().len(), DURATION_BUCKETS);
        for (i, &bound) in buckets.bounds().iter().enumerate() {
            let expected = (1u64 << i) as f64 / 1e6;
            assert!(
                (bound - expected).abs() <= expected * 1e-9,
                "bucket {i}: {bound} != {expected}"
            );
        }
    }

    #[test]
    fn subtracting_a_baseline_leaves_the_later_observations() {
        let all = BucketHistogram::new(duration_buckets());
        let later = BucketHistogram::new(duration_buckets());
        for micros in [1, 3, 100] {
            all.observe_duration(Duration::from_micros(micros));
        }
        let base = all.snapshot();
        for micros in [2, 100, 20_000_000] {
            all.observe_duration(Duration::from_micros(micros));
            later.observe_duration(Duration::from_micros(micros));
        }

        let since = subtract_histogram(&all.snapshot(), &base);
        let expected = later.snapshot();
        assert_eq!(since.count, expected.count);
        assert!((since.sum - expected.sum).abs() < 1e-9);
        let counts = |s: &HistogramSnapshot| -> Vec<u64> {
            s.buckets.iter().map(|b| b.cumulative_count).collect()
        };
        assert_eq!(counts(&since), counts(&expected));
    }

    #[test]
    fn mismatched_histograms_are_not_subtracted() {
        let small = BucketHistogram::new(Buckets::custom([1.0]));
        small.observe(0.5);
        let other = BucketHistogram::new(duration_buckets()).snapshot();
        assert_eq!(
            subtract_histogram(&small.snapshot(), &other),
            small.snapshot()
        );
    }

    /// The only test that resets the shared registries, so the counts it asserts are its own.
    #[test]
    fn reset_rebases_counters_and_histograms_but_not_gauges() {
        CRON_TICKS_SKIPPED.fetch_add(5, Ordering::Relaxed);
        CRON_TICK_DURATION.observe_duration(Duration::from_micros(10));
        reset();

        assert_eq!(
            reading(&[Section::Cron], "cron_ticks_skipped_total").value,
            MetricValue::Counter(0)
        );
        let MetricValue::Histogram(snapshot) =
            reading(&[Section::Cron], "cron_tick_duration_seconds").value
        else {
            panic!("not a histogram");
        };
        assert_eq!(snapshot.count, 0);

        CRON_TICKS_SKIPPED.fetch_add(3, Ordering::Relaxed);
        assert_eq!(
            reading(&[Section::Cron], "cron_ticks_skipped_total").value,
            MetricValue::Counter(3)
        );
        // The value underneath keeps counting from where it was.
        assert!(CRON_TICKS_SKIPPED.load(Ordering::Relaxed) >= 8);
        assert_eq!(
            reading(&[Section::Cron], "cron_interval_seconds").value,
            MetricValue::Gauge(Scalar::Float(cron_interval_ms() as f64 / 1000.0))
        );
    }

    #[test]
    fn wire_counters_count_messages_and_bytes() {
        let wire = WireCounters::new();
        wire.record(100);
        wire.record(28);
        assert_eq!(wire.messages.load(Ordering::Relaxed), 2);
        assert_eq!(wire.bytes.load(Ordering::Relaxed), 128);
    }

    #[test]
    fn section_names_parse_case_insensitively() {
        for &section in Section::ALL {
            assert_eq!(Section::parse(section.as_str().as_bytes()), Some(section));
            let upper = section.as_str().to_ascii_uppercase();
            assert_eq!(Section::parse(upper.as_bytes()), Some(section));
        }
        assert_eq!(Section::parse(b"nope"), None);
    }

    /// Sections come in `Section::ALL` order, each one contiguous, and metrics within a section
    /// by family name — a counter's name before metered adds `_total`.
    #[test]
    fn snapshot_orders_by_section_then_family_name() {
        let readings = snapshot(&[]);
        let sections: Vec<_> = readings.iter().map(|r| r.section).collect();
        let mut expected = sections.clone();
        expected.dedup();
        assert_eq!(expected, Section::ALL, "sections out of order or split");

        for &section in Section::ALL {
            let families: Vec<_> = readings
                .iter()
                .filter(|r| r.section == section)
                .map(|r| match r.kind {
                    MetricKind::Counter => r.name.strip_suffix("_total").unwrap().to_owned(),
                    _ => r.name.clone(),
                })
                .collect();
            let mut sorted = families.clone();
            sorted.sort();
            assert_eq!(families, sorted, "{} not sorted by name", section.as_str());
        }
    }

    #[test]
    fn snapshot_filters_by_section() {
        let cron_then_exec: Vec<_> = snapshot(&[Section::Cron, Section::Exec])
            .into_iter()
            .map(|r| r.name)
            .collect();
        let cron = snapshot(&[Section::Cron]);
        assert!(!cron.is_empty());
        assert!(cron.iter().all(|r| r.section == Section::Cron));

        // Section order in the request doesn't reorder the reply.
        let both: Vec<_> = snapshot(&[Section::Exec, Section::Cron])
            .into_iter()
            .map(|r| r.name)
            .collect();
        assert_eq!(both, cron_then_exec);
    }

    #[test]
    fn duration_histograms_use_the_shared_buckets() {
        let MetricValue::Histogram(snapshot) =
            reading(&[Section::Cron], "cron_tick_duration_seconds").value
        else {
            panic!("not a histogram");
        };
        // The finite bounds plus `+Inf`.
        assert_eq!(snapshot.buckets.len(), DURATION_BUCKETS + 1);
        assert!(snapshot.buckets.last().unwrap().le.is_infinite());
    }

    // --- roster ---------------------------------------------------------------------------

    #[test]
    fn registries_follow_section_order() {
        let order: Vec<_> = REGISTRIES.iter().map(|(section, _)| *section).collect();
        assert_eq!(order, Section::ALL);
    }

    /// Metered's own check: a declared unit must suffix the name, label names must be legal,
    /// and no family may be declared twice with different types.
    #[test]
    fn schemas_validate() {
        for (section, schema) in schemas() {
            if let Err(errors) = schema.validate() {
                panic!("{}: {errors:?}", section.as_str());
            }
        }
    }

    #[test]
    fn every_metric_is_a_supported_kind() {
        for (_, schema) in schemas() {
            for family in schema.families() {
                assert!(
                    MetricKind::of(family.metric_type).is_some(),
                    "{}: {:?} is not reported by TS._DEBUG STATS",
                    family.name,
                    family.metric_type
                );
            }
        }
    }

    #[test]
    fn every_registered_metric_is_reported() {
        let reported = snapshot(&[]).len();
        let registered: usize = schemas().map(|(_, schema)| schema.families().len()).sum();
        assert_eq!(reported, registered);
    }

    #[test]
    fn names_are_unique() {
        let names: Vec<_> = snapshot(&[]).into_iter().map(|r| r.name).collect();
        for (i, name) in names.iter().enumerate() {
            assert!(!names[..i].contains(name), "duplicate metric name: {name}");
        }
    }

    #[test]
    fn names_are_snake_case_and_start_with_their_section() {
        for reading in snapshot(&[]) {
            let name = &reading.name;
            assert!(
                name.starts_with(|c: char| c.is_ascii_lowercase())
                    && name
                        .chars()
                        .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_')
                    && !name.contains("__")
                    && !name.ends_with('_'),
                "{name}: not snake_case"
            );
            let prefix = format!("{}_", reading.section.as_str());
            assert!(
                name.starts_with(&prefix),
                "{name}: expected prefix {prefix}"
            );
        }
    }

    /// Metered adds `_total` to a counter's sample name, so the registered name must not carry
    /// it already, and nothing else may end in it.
    #[test]
    fn only_counters_end_in_total() {
        for (_, schema) in schemas() {
            for family in schema.families() {
                assert!(
                    !family.name.ends_with("_total"),
                    "{}: register a counter without its _total suffix",
                    family.name
                );
            }
        }
        for reading in snapshot(&[]) {
            assert_eq!(
                reading.name.ends_with("_total"),
                reading.kind == MetricKind::Counter,
                "{}",
                reading.name
            );
        }
    }

    /// `TS._DEBUG STATS VERBOSE` reports the help text as the description.
    #[test]
    fn every_metric_has_help() {
        for reading in snapshot(&[]) {
            assert!(
                !reading.help.trim().is_empty(),
                "{}: missing help",
                reading.name
            );
        }
    }

    /// A section with nothing in it would be accepted by `STATS` and report nothing.
    #[test]
    fn every_section_has_a_metric() {
        for (section, schema) in schemas() {
            assert!(
                !schema.families().is_empty(),
                "section {} has no metrics",
                section.as_str()
            );
        }
    }
}
