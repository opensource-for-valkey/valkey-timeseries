//! Versioned workload profiles (`scenarios/*.json`).
//!
//! A scenario is explicit about everything that shapes a measurement — fixture
//! shape, series settings on *each* engine, protocol, connection/pipeline
//! depth, trial counts — so a report can be reproduced from the scenario file
//! and the run manifest alone. Unknown fields and unknown case kinds are
//! rejected rather than ignored: a typo must not silently turn into a default.

use std::collections::HashSet;
use std::fs;
use std::path::Path;

use anyhow::{Context, Result, bail, ensure};
use serde::{Deserialize, Serialize};

/// Bumped whenever a field changes meaning. Older files are refused, never
/// reinterpreted.
pub const SCHEMA_VERSION: u32 = 1;

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Scenario {
    pub schema_version: u32,
    pub name: String,
    pub description: String,
    pub fixture: FixtureSpec,
    pub series: SeriesSpec,
    #[serde(default)]
    pub protocol: Protocol,
    /// Paired trials per case; AB/BA order alternates between trials.
    pub trials: u32,
    /// Untimed warm-up before each timed read trial.
    pub warmup_seconds: u64,
    /// Length of each timed read trial.
    pub read_duration_seconds: u64,
    /// Number of requests in a read case's replayed cycle. The trial loops over
    /// the cycle for `read_duration_seconds`; calibration (step 2) may raise it.
    #[serde(default = "default_read_cycle")]
    pub read_cycle_requests: usize,
    /// A write trial whose faster engine finished sooner than this is reported
    /// as under-calibrated: enlarge the fixture rather than trust the ratio.
    #[serde(default = "default_min_write_seconds")]
    pub min_write_seconds: u64,
    /// Pause before each memory snapshot.
    #[serde(default = "default_settle_seconds")]
    pub settle_seconds: u64,
    pub cases: Vec<Case>,
}

fn default_read_cycle() -> usize {
    100_000
}
fn default_min_write_seconds() -> u64 {
    30
}
fn default_settle_seconds() -> u64 {
    2
}

/// What `tools/benchmark_dataset.rs` must be asked for. Workload and timestamp
/// model ids are passed through verbatim; the exporter owns the list of valid
/// ids, so the driver never duplicates it.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct FixtureSpec {
    pub series: usize,
    pub samples_per_series: usize,
    pub workload: String,
    pub timestamp_model: String,
    #[serde(default = "default_label_cardinality")]
    pub label_cardinality: Vec<usize>,
    #[serde(default = "default_label_value_len")]
    pub label_value_len: usize,
    #[serde(default = "default_key_prefix")]
    pub key_prefix: String,
    #[serde(default = "default_interval_ms")]
    pub interval_ms: u64,
}

fn default_label_cardinality() -> Vec<usize> {
    vec![1, 10, 100]
}
fn default_label_value_len() -> usize {
    8
}
fn default_key_prefix() -> String {
    "bench".to_string()
}
fn default_interval_ms() -> u64 {
    1000
}

impl FixtureSpec {
    pub fn total_samples(&self) -> usize {
        self.series * self.samples_per_series
    }
}

/// Series settings sent with every `TS.CREATE`. Encodings are named per engine
/// because the two products do not share an encoding vocabulary; the report
/// prints exactly what was sent (plan: "Name actual settings").
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct SeriesSpec {
    pub chunk_size: u32,
    pub duplicate_policy: DuplicatePolicy,
    pub encoding: EncodingPair,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum DuplicatePolicy {
    Block,
    First,
    Last,
    Min,
    Max,
    Sum,
}

impl DuplicatePolicy {
    pub fn as_arg(self) -> &'static str {
        match self {
            Self::Block => "BLOCK",
            Self::First => "FIRST",
            Self::Last => "LAST",
            Self::Min => "MIN",
            Self::Max => "MAX",
            Self::Sum => "SUM",
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct EncodingPair {
    pub subject: SubjectEncoding,
    pub reference: ReferenceEncoding,
}

/// `ENCODING` values accepted by this module's `TS.CREATE`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum SubjectEncoding {
    Chimp,
    Gorilla,
    Uncompressed,
}

impl SubjectEncoding {
    pub fn as_arg(self) -> &'static str {
        match self {
            Self::Chimp => "CHIMP",
            Self::Gorilla => "GORILLA",
            Self::Uncompressed => "UNCOMPRESSED",
        }
    }
}

/// `ENCODING` values documented for the reference's `TS.CREATE`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum ReferenceEncoding {
    Compressed,
    Uncompressed,
}

impl ReferenceEncoding {
    pub fn as_arg(self) -> &'static str {
        match self {
            Self::Compressed => "COMPRESSED",
            Self::Uncompressed => "UNCOMPRESSED",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum Protocol {
    #[default]
    Resp2,
    Resp3,
}

// No `deny_unknown_fields` here: serde does not support it next to `flatten`.
// Unknown keys still fail, because they fall through to the flattened
// `CaseKind`, whose variants do deny them (covered by a test).
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Case {
    pub id: String,
    #[serde(flatten)]
    pub kind: CaseKind,
    #[serde(default = "one")]
    pub connections: u32,
    #[serde(default = "one")]
    pub pipeline: u32,
    /// Overrides the scenario's protocol for this case's workload connections.
    /// Lets one scenario carry RESP2/RESP3 twins of a case, so the wire-format
    /// share of a gap can be read off the same run.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub protocol: Option<Protocol>,
}

impl Case {
    pub fn protocol(&self, scenario: Protocol) -> Protocol {
        self.protocol.unwrap_or(scenario)
    }
}

fn one() -> u32 {
    1
}
fn one_usize() -> usize {
    1
}

/// Case families. Only the first-comparison set (plan, sequence step 2) exists
/// so far; aggregation, label and grouped queries are step 3 and will be added
/// here as new variants, never as loosely typed parameters.
// Every variant is a struct variant, empty ones included: serde only enforces
// `deny_unknown_fields` on struct variants, so `Add {}` refuses a stray key
// where a unit `Add` would silently accept it.
#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum CaseKind {
    /// Ordered `TS.ADD` into precreated series. Each connection owns a disjoint
    /// set of series and writes them as monotonic timestamp streams.
    Add {},
    /// `TS.MADD` with `batch` samples per command, same ownership rules.
    /// `samples_per_series` is how many consecutive samples of one series a
    /// batch carries: 1 is per-tick fan-in (one sample for each of `batch`
    /// series), `batch` is per-series buffering. The two shapes take different
    /// paths in the module, so both are worth measuring.
    Madd {
        batch: usize,
        #[serde(default = "one_usize")]
        samples_per_series: usize,
    },
    /// `TS.GET` over loaded series.
    Get { distribution: KeyDistribution },
    /// `TS.RANGE` / `TS.REVRANGE` over loaded series.
    Range {
        window: RangeWindow,
        #[serde(default)]
        reverse: bool,
    },
    /// Memory accounting of empty and loaded series; no timed workload.
    Memory {},
    /// `TS.RANGE ... ALIGN start AGGREGATION <aggregator> <bucket>` over a
    /// window, with the bucket sized to yield about `buckets` points.
    Aggregate {
        window: RangeWindow,
        aggregator: Aggregator,
        buckets: usize,
        #[serde(default)]
        reverse: bool,
    },
    /// `TS.QUERYINDEX l<label>=<value>`; the value cycles deterministically, so
    /// the expected key set is exact for every request.
    #[serde(rename = "queryindex")]
    QueryIndex { label: usize },
    /// `TS.MGET FILTER l<label>=<value>`: last sample of every matched series.
    Mget { label: usize },
    /// `TS.MRANGE <window> FILTER l<label>=<value>` — raw samples of every
    /// matched series over the window.
    Mrange { label: usize, window: RangeWindow },
    /// `TS.MRANGE <window> AGGREGATION ... FILTER l<label>=<value> GROUPBY
    /// l<group_label> REDUCE <reducer>`; output cardinality is the number of
    /// distinct `l<group_label>` values among the matched series.
    #[serde(rename = "groupby")]
    GroupBy {
        label: usize,
        group_label: usize,
        window: RangeWindow,
        aggregator: Aggregator,
        buckets: usize,
        reducer: Reducer,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum Aggregator {
    Min,
    Max,
    Count,
    Sum,
    Avg,
}

impl Aggregator {
    pub fn as_arg(self) -> &'static str {
        match self {
            Self::Min => "min",
            Self::Max => "max",
            Self::Count => "count",
            Self::Sum => "sum",
            Self::Avg => "avg",
        }
    }

    /// `count`, `min` and `max` are order-independent and exact on both
    /// engines; `sum` and `avg` depend on summation order and are compared
    /// with a tolerance while every deviation is counted and reported.
    pub fn is_exact(self) -> bool {
        matches!(self, Self::Min | Self::Max | Self::Count)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum Reducer {
    Sum,
    Min,
    Max,
    Avg,
    Count,
}

impl Reducer {
    pub fn as_arg(self) -> &'static str {
        match self {
            Self::Sum => "sum",
            Self::Min => "min",
            Self::Max => "max",
            Self::Avg => "avg",
            Self::Count => "count",
        }
    }
}

impl CaseKind {
    /// True for cases whose timed phase replays a fixed-length write trace.
    pub fn is_write(&self) -> bool {
        matches!(self, Self::Add {} | Self::Madd { .. })
    }

    /// True for cases whose timed phase is a duration-bounded read loop.
    pub fn is_read(&self) -> bool {
        !self.is_write() && !matches!(self, Self::Memory {})
    }

    /// Every server command the case sends, for `COMMAND INFO` support checks.
    pub fn commands(&self) -> Vec<&'static str> {
        let mut cmds = vec!["TS.CREATE", "TS.INFO"];
        match self {
            Self::Add {} => cmds.push("TS.ADD"),
            Self::Madd { .. } => cmds.push("TS.MADD"),
            Self::Get { .. } => cmds.extend(["TS.MADD", "TS.GET"]),
            Self::Range { reverse, .. } | Self::Aggregate { reverse, .. } => {
                cmds.push("TS.MADD");
                cmds.push(if *reverse { "TS.REVRANGE" } else { "TS.RANGE" });
            }
            Self::Memory {} => cmds.push("TS.MADD"),
            Self::QueryIndex { .. } => cmds.extend(["TS.MADD", "TS.QUERYINDEX"]),
            Self::Mget { .. } => cmds.extend(["TS.MADD", "TS.MGET"]),
            Self::Mrange { .. } | Self::GroupBy { .. } => cmds.extend(["TS.MADD", "TS.MRANGE"]),
        }
        cmds
    }
}

/// Written as an object: `{"type": "uniform"}` or
/// `{"type": "hot", "keys": 10, "share_percent": 90}`.
#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum KeyDistribution {
    Uniform {},
    /// `share_percent` of requests go to the first `keys` series; the rest are
    /// uniform over all series.
    Hot {
        keys: usize,
        share_percent: u8,
    },
}

/// Written as an object: `{"type": "recent", "points": 100}`,
/// `{"type": "middle", "percent": 10}` or `{"type": "full"}`.
#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum RangeWindow {
    /// The last `points` samples of a series.
    Recent { points: usize },
    /// The first `points` samples of a series. The dual of `recent`: the same
    /// reply size, but a read that starts at the first chunk's first sample, so
    /// `recent − head` on one engine is what it spends decoding samples the
    /// window discards.
    Head { points: usize },
    /// `percent` of the series, centred on its midpoint.
    Middle { percent: u8 },
    /// `- +`.
    Full {},
}

fn check_window(case_id: &str, window: &RangeWindow, samples: usize) -> Result<()> {
    match window {
        RangeWindow::Recent { points } => ensure!(
            *points >= 1 && *points <= samples,
            "case {case_id}: recent points must be within 1..={samples}"
        ),
        RangeWindow::Head { points } => ensure!(
            *points >= 1 && *points <= samples,
            "case {case_id}: head points must be within 1..={samples}"
        ),
        RangeWindow::Middle { percent } => ensure!(
            (1..=100).contains(percent),
            "case {case_id}: middle percent must be within 1..=100"
        ),
        RangeWindow::Full {} => {}
    }
    Ok(())
}

fn check_label(case_id: &str, label: usize, labels: usize) -> Result<()> {
    ensure!(
        label < labels,
        "case {case_id}: label index {label} is out of range (fixture has {labels} label(s))"
    );
    Ok(())
}

impl Scenario {
    pub fn load(path: &Path) -> Result<Self> {
        let text = fs::read_to_string(path)
            .with_context(|| format!("reading scenario {}", path.display()))?;
        let scenario: Scenario = serde_json::from_str(&text)
            .with_context(|| format!("parsing scenario {}", path.display()))?;
        scenario
            .validate()
            .with_context(|| format!("invalid scenario {}", path.display()))?;
        Ok(scenario)
    }

    pub fn validate(&self) -> Result<()> {
        ensure!(
            self.schema_version == SCHEMA_VERSION,
            "schema_version {} is not supported (this driver understands {SCHEMA_VERSION})",
            self.schema_version
        );
        ensure!(!self.name.is_empty(), "name must not be empty");
        ensure!(self.trials >= 1, "trials must be at least 1");
        ensure!(
            self.read_duration_seconds >= 1,
            "read_duration_seconds must be at least 1"
        );
        ensure!(
            self.read_cycle_requests >= 1,
            "read_cycle_requests must be at least 1"
        );
        ensure!(!self.cases.is_empty(), "at least one case is required");

        let f = &self.fixture;
        ensure!(f.series >= 1, "fixture.series must be at least 1");
        ensure!(
            f.samples_per_series >= 1,
            "fixture.samples_per_series must be at least 1"
        );
        ensure!(f.interval_ms >= 1, "fixture.interval_ms must be at least 1");
        ensure!(!f.workload.is_empty(), "fixture.workload must not be empty");
        ensure!(
            !f.timestamp_model.is_empty(),
            "fixture.timestamp_model must not be empty"
        );
        ensure!(
            !f.key_prefix.is_empty() && !f.key_prefix.contains([' ', ':', '\t', '\n']),
            "fixture.key_prefix must be non-empty and free of whitespace and ':'"
        );
        ensure!(
            f.label_cardinality.iter().all(|&c| c >= 1),
            "fixture.label_cardinality entries must be at least 1"
        );
        ensure!(
            self.series.chunk_size >= 64,
            "series.chunk_size must be at least 64 bytes"
        );

        let mut ids = HashSet::new();
        for case in &self.cases {
            ensure!(!case.id.is_empty(), "case id must not be empty");
            ensure!(
                case.id
                    .chars()
                    .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-'),
                "case id {:?} must be [A-Za-z0-9_-]",
                case.id
            );
            ensure!(
                ids.insert(case.id.as_str()),
                "duplicate case id {:?}",
                case.id
            );
            ensure!(
                case.connections >= 1,
                "case {}: connections must be at least 1",
                case.id
            );
            ensure!(
                case.pipeline >= 1,
                "case {}: pipeline must be at least 1",
                case.id
            );
            match &case.kind {
                CaseKind::Madd {
                    batch,
                    samples_per_series,
                } => {
                    ensure!(
                        *batch >= 1,
                        "case {}: madd batch must be at least 1",
                        case.id
                    );
                    ensure!(
                        *samples_per_series >= 1 && *samples_per_series <= *batch,
                        "case {}: samples_per_series must be within 1..=batch",
                        case.id
                    );
                    ensure!(
                        *samples_per_series <= f.samples_per_series,
                        "case {}: samples_per_series exceeds the fixture's samples per series",
                        case.id
                    );
                }
                CaseKind::Get {
                    distribution:
                        KeyDistribution::Hot {
                            keys,
                            share_percent,
                        },
                } => {
                    ensure!(
                        *keys >= 1 && *keys <= f.series,
                        "case {}: hot keys must be within 1..={}",
                        case.id,
                        f.series
                    );
                    ensure!(
                        *share_percent <= 100,
                        "case {}: share_percent must be at most 100",
                        case.id
                    );
                }
                CaseKind::Range { window, .. } => {
                    check_window(&case.id, window, f.samples_per_series)?;
                }
                CaseKind::Aggregate {
                    window, buckets, ..
                } => {
                    check_window(&case.id, window, f.samples_per_series)?;
                    ensure!(
                        *buckets >= 1,
                        "case {}: buckets must be at least 1",
                        case.id
                    );
                }
                CaseKind::QueryIndex { label } | CaseKind::Mget { label } => {
                    check_label(&case.id, *label, f.label_cardinality.len())?;
                }
                CaseKind::Mrange { label, window } => {
                    check_label(&case.id, *label, f.label_cardinality.len())?;
                    check_window(&case.id, window, f.samples_per_series)?;
                }
                CaseKind::GroupBy {
                    label,
                    group_label,
                    window,
                    buckets,
                    ..
                } => {
                    check_label(&case.id, *label, f.label_cardinality.len())?;
                    check_label(&case.id, *group_label, f.label_cardinality.len())?;
                    ensure!(
                        label != group_label,
                        "case {}: group_label must differ from the filter label",
                        case.id
                    );
                    check_window(&case.id, window, f.samples_per_series)?;
                    ensure!(
                        *buckets >= 1,
                        "case {}: buckets must be at least 1",
                        case.id
                    );
                }
                CaseKind::Memory {} if case.connections != 1 || case.pipeline != 1 => {
                    bail!(
                        "case {}: memory cases take no connections/pipeline settings",
                        case.id
                    );
                }
                _ => {}
            }
        }
        Ok(())
    }

    /// Union of commands over all cases, sorted, for support checks.
    pub fn commands(&self) -> Vec<&'static str> {
        let mut set: Vec<&'static str> =
            self.cases.iter().flat_map(|c| c.kind.commands()).collect();
        set.sort_unstable();
        set.dedup();
        set
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn minimal() -> serde_json::Value {
        serde_json::json!({
            "schema_version": 1,
            "name": "t",
            "description": "",
            "fixture": {"series": 4, "samples_per_series": 10, "workload": "drift", "timestamp_model": "regular"},
            "series": {"chunk_size": 4096, "duplicate_policy": "BLOCK",
                       "encoding": {"subject": "chimp", "reference": "compressed"}},
            "trials": 1, "warmup_seconds": 0, "read_duration_seconds": 1,
            "cases": [{"id": "add", "kind": "add"}]
        })
    }

    fn parse(v: serde_json::Value) -> Result<Scenario> {
        let s: Scenario = serde_json::from_value(v)?;
        s.validate()?;
        Ok(s)
    }

    #[test]
    fn minimal_scenario_parses_with_defaults() {
        let s = parse(minimal()).unwrap();
        assert_eq!(s.protocol, Protocol::Resp2);
        assert_eq!(s.fixture.label_cardinality, vec![1, 10, 100]);
        assert_eq!(s.cases[0].connections, 1);
        assert_eq!(s.commands(), vec!["TS.ADD", "TS.CREATE", "TS.INFO"]);
    }

    #[test]
    fn wrong_schema_version_is_refused() {
        let mut v = minimal();
        v["schema_version"] = 2.into();
        let err = parse(v).unwrap_err().to_string();
        assert!(err.contains("schema_version 2"), "{err}");
    }

    #[test]
    fn unknown_field_is_refused() {
        let mut v = minimal();
        v["cases"][0]["pipelien"] = 16.into();
        assert!(parse(v).is_err());
    }

    #[test]
    fn query_breadth_kinds_parse_and_validate() {
        let mut v = minimal();
        v["cases"] = serde_json::json!([
            {"id": "agg", "kind": "aggregate", "window": {"type": "full"}, "aggregator": "avg", "buckets": 10},
            {"id": "qi", "kind": "queryindex", "label": 1},
            {"id": "mg", "kind": "mget", "label": 2},
            {"id": "mr", "kind": "mrange", "label": 1, "window": {"type": "recent", "points": 5}},
            {"id": "gb", "kind": "groupby", "label": 0, "group_label": 1, "window": {"type": "full"},
             "aggregator": "sum", "buckets": 4, "reducer": "sum"}
        ]);
        let s = parse(v).unwrap();
        assert!(s.cases.iter().all(|c| c.kind.is_read()));
        assert_eq!(
            s.commands(),
            vec![
                "TS.CREATE",
                "TS.INFO",
                "TS.MADD",
                "TS.MGET",
                "TS.MRANGE",
                "TS.QUERYINDEX",
                "TS.RANGE"
            ]
        );

        let mut v = minimal();
        v["cases"] = serde_json::json!([{"id": "qi", "kind": "queryindex", "label": 3}]);
        assert!(parse(v).unwrap_err().to_string().contains("out of range"));

        let mut v = minimal();
        v["cases"] = serde_json::json!([
            {"id": "gb", "kind": "groupby", "label": 1, "group_label": 1, "window": {"type": "full"},
             "aggregator": "sum", "buckets": 4, "reducer": "sum"}
        ]);
        assert!(parse(v).unwrap_err().to_string().contains("must differ"));

        let mut v = minimal();
        v["cases"] = serde_json::json!([
            {"id": "agg", "kind": "aggregate", "window": {"type": "full"}, "aggregator": "median", "buckets": 10}
        ]);
        assert!(parse(v).is_err());
    }

    #[test]
    fn unknown_nested_field_is_refused() {
        let mut v = minimal();
        v["cases"] = serde_json::json!([
            {"id": "g", "kind": "get", "distribution": {"type": "uniform", "keys": 1}}
        ]);
        assert!(parse(v).is_err());
        let mut v = minimal();
        v["cases"] = serde_json::json!([{"id": "m", "kind": "memory", "batch": 1}]);
        assert!(parse(v).is_err());
    }

    #[test]
    fn unknown_case_kind_is_refused() {
        let mut v = minimal();
        v["cases"][0]["kind"] = "outliers".into();
        assert!(parse(v).is_err());
    }

    #[test]
    fn duplicate_case_ids_are_refused() {
        let mut v = minimal();
        v["cases"] = serde_json::json!([{"id": "a", "kind": "add"}, {"id": "a", "kind": "memory"}]);
        assert!(
            parse(v)
                .unwrap_err()
                .to_string()
                .contains("duplicate case id")
        );
    }

    #[test]
    fn tagged_parameters_are_checked() {
        let mut v = minimal();
        v["cases"] = serde_json::json!([
            {"id": "g", "kind": "get", "distribution": {"type": "hot", "keys": 99, "share_percent": 90}}
        ]);
        assert!(parse(v).unwrap_err().to_string().contains("hot keys"));

        let mut v = minimal();
        v["cases"] = serde_json::json!([
            {"id": "r", "kind": "range", "window": {"type": "recent", "points": 11}}
        ]);
        assert!(parse(v).unwrap_err().to_string().contains("recent points"));

        let mut v = minimal();
        v["cases"] = serde_json::json!([
            {"id": "r", "kind": "range", "window": {"type": "middle", "percent": 10}, "reverse": true}
        ]);
        let s = parse(v).unwrap();
        assert_eq!(
            s.commands(),
            vec!["TS.CREATE", "TS.INFO", "TS.MADD", "TS.REVRANGE"]
        );
    }
}
