//! The module's `INFO` sections: `ts_index`, `ts_compaction`, `ts_rdb`, `ts_blocked_read`,
//! `ts_string_interning`, `ts_task`, `ts_asm`, `ts_index_reconciliation`, `ts_fanout` and
//! `ts_chunk`.
//!
//! Module sections are emitted only for an explicit `INFO modules`, `INFO everything`, or
//! `INFO <section>`; bare `INFO` and `INFO all` omit them. Section selection is checked before
//! collecting data, so an index walk is needed only when the index section is requested. A crash
//! report carries only the sections that are safe to read on a faulting thread (see
//! [`InfoSection::crash_safe`]).

use crate::common::humanize::bytes_to_human;
use crate::common::metrics::{self, MetricValue, Section};
use crate::series::index::index_memory_usage;
use crate::series::{COMPACTION_FAILURES, COMPACTION_POLICY_SKIPS, COMPACTION_STATE_REBUILDS};
use metered::Scalar;
use std::sync::atomic::Ordering;
use valkey_module::{
    InfoContext, InfoContextBuilderFieldBottomLevelValue as InfoValue, Status, ValkeyResult, raw,
};
use valkey_module_macros::info_command_handler;

/// `INFO ts_index`, `INFO ts_compaction`, `INFO ts_rdb`, `INFO ts_blocked_read`,
/// `INFO ts_string_interning`, `INFO ts_task`, `INFO ts_asm`, `INFO ts_index_reconciliation`,
/// `INFO ts_fanout` and `INFO ts_chunk`.
///
/// The sections are declared without the module name; the server prefixes each with it, so they
/// land as `# ts_index`, `# ts_compaction`, `# ts_rdb` and so on, with every field prefixed
/// `ts_`.
///
/// Each `*_bytes` field, mirrored ones included, is followed by a `*_bytes_human` rendering of it
/// (`1.21M`), formatted as the server formats `used_memory_human`.
///
/// On a crash the server calls this from its crash report, on the faulting thread, with every
/// section selected. That thread may hold any lock the module has (an index's write lock, say),
/// and a `std` lock taken again by its holder deadlocks: the report never finishes and the
/// process hangs instead of exiting. So a crash report carries only the sections
/// [`InfoSection::crash_safe`] admits, and of those only the ones it can read without
/// initializing anything ([`InfoSection::readable_in_crash_report`]). A new section must be
/// classified in both.
#[info_command_handler]
fn module_info(ctx: &InfoContext, for_crash_report: bool) -> ValkeyResult<()> {
    for &section in SECTIONS {
        if !section.emitted(for_crash_report) {
            continue;
        }
        if for_crash_report && !section.readable_in_crash_report() {
            continue;
        }
        write_section(ctx, section.name(), || section.fields(for_crash_report))?;
    }
    Ok(())
}

/// Select the section before reading its fields. The builder defers this check until emission,
/// after its field values have already been collected. Emit directly to avoid a second header.
fn write_section(
    ctx: &InfoContext,
    name: &str,
    collect: impl FnOnce() -> Vec<(String, InfoValue)>,
) -> ValkeyResult<()> {
    if raw::add_info_section(ctx.ctx, Some(name)) != Status::Ok {
        return Ok(());
    }
    collect().into_iter().try_for_each(|(name, value)| {
        match value {
            InfoValue::String(value) => raw::add_info_field_str(ctx.ctx, &name, &value),
            InfoValue::U64(value) => raw::add_info_field_unsigned_long_long(ctx.ctx, &name, value),
            InfoValue::I64(value) => raw::add_info_field_long_long(ctx.ctx, &name, value),
            InfoValue::F64(value) => raw::add_info_field_double(ctx.ctx, &name, value),
        }
        .into()
    })
}

/// One of the module's `INFO` sections.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum InfoSection {
    /// `ts_index`: the postings index's memory, from a walk of every database's index.
    Index,
    /// `ts_compaction`: compaction failures and state rebuilds.
    Compaction,
    /// A `TS._DEBUG METRICS` section, mirrored under its own name.
    Metrics(Section),
}

/// Every section, in emission order.
const SECTIONS: &[InfoSection] = &[
    InfoSection::Index,
    InfoSection::Compaction,
    InfoSection::Metrics(Section::Rdb),
    InfoSection::Metrics(Section::Read),
    InfoSection::Metrics(Section::StringInterning),
    InfoSection::Metrics(Section::Task),
    InfoSection::Metrics(Section::Asm),
    InfoSection::Metrics(Section::IndexReconciliation),
    InfoSection::Metrics(Section::Fanout),
    InfoSection::Metrics(Section::Chunk),
];

impl InfoSection {
    const fn name(self) -> &'static str {
        match self {
            InfoSection::Index => "index",
            InfoSection::Compaction => "compaction",
            InfoSection::Metrics(section) => section.as_str(),
        }
    }

    /// Whether the section may go into a crash report: every value it reads is an atomic load,
    /// or a sum over a fixed set of them, so it takes no lock, walks no structure, and
    /// initializes nothing lazily — a `LazyLock` is a lock too, and the faulting thread may be
    /// the one running its initializer. Allocating is allowed: the server's own crash report
    /// allocates too.
    ///
    /// - `index` walks every database's postings under each index's read lock: not safe.
    /// - `compaction`, `rdb`, `blocked_read`, `chunk`: plain counters and gauges.
    /// - `task`, `asm`, `index_reconciliation`: counters and gauges, and duration histograms
    ///   that metered snapshots with atomic loads (INFO leaves them out, but reads them). The
    ///   histograms are forced when the registries are built, so reading them initializes nothing.
    /// - `string_interning`: the pool's entry count (a sum of papaya's striped counters) and an
    ///   atomic byte count, both read without creating the pool (zero before its first string).
    /// - `fanout`: counters, and two map sizes read the same way, without a guard and without
    ///   creating the maps.
    /// - `clustermap`, `exec`: not mirrored, so not audited; not safe until they are.
    ///
    /// The mirrored metrics are read through metered's registry, which takes no lock either, and
    /// only once it exists ([`Self::readable_in_crash_report`]).
    const fn crash_safe(self) -> bool {
        match self {
            InfoSection::Index => false,
            InfoSection::Compaction => true,
            InfoSection::Metrics(section) => match section {
                Section::Rdb
                | Section::Read
                | Section::Chunk
                | Section::Task
                | Section::Asm
                | Section::IndexReconciliation
                | Section::StringInterning
                | Section::Fanout => true,
                Section::Clustermap | Section::Exec => false,
            },
        }
    }

    /// Whether the section is emitted: always, except a crash report skips the unsafe ones.
    const fn emitted(self, for_crash_report: bool) -> bool {
        !for_crash_report || self.crash_safe()
    }

    /// Whether a crash report can read the section right now without initializing anything.
    /// The metrics sections need the registries, which a crash report must not build. Module
    /// init builds them ([`metrics::init`]), so this is `false` only for a crash during or before
    /// that build, and the section is then left out. Once built they stay built, so a `true`
    /// holds for [`Self::fields`].
    fn readable_in_crash_report(self) -> bool {
        match self {
            InfoSection::Index | InfoSection::Compaction => true,
            InfoSection::Metrics(_) => metrics::registries_built(),
        }
    }

    fn fields(self, for_crash_report: bool) -> Vec<(String, InfoValue)> {
        match self {
            InfoSection::Index => index_fields(),
            InfoSection::Compaction => compaction_fields(),
            InfoSection::Metrics(section) if for_crash_report => {
                // Never builds the registries; checked built before the section was opened.
                info_fields_from(metrics::current_if_built(&[section]).unwrap_or_default())
            }
            InfoSection::Metrics(section) => info_fields(section),
        }
    }
}

fn index_fields() -> Vec<(String, InfoValue)> {
    let index = index_memory_usage();
    let mut fields = Vec::new();
    // `index_bytes` is the sum of the four parts. It is a gauge, so `total` stays out of its
    // name: OpenMetrics reserves that for counters.
    for (name, bytes) in [
        ("index_bytes", index.total_bytes()),
        ("index_terms_bytes", index.terms_bytes),
        ("index_postings_bytes", index.postings_bytes),
        ("index_id_to_key_bytes", index.id_to_key_bytes),
        ("index_bookkeeping_bytes", index.bookkeeping_bytes),
    ] {
        fields.push((name.to_owned(), (bytes as u64).into()));
        fields.push((format!("{name}_human"), bytes_to_human(bytes as u64).into()));
    }
    for (name, count) in [
        ("index_terms", index.term_count),
        ("index_series", index.series_count),
        ("index_databases", index.db_count),
    ] {
        fields.push((name.to_owned(), (count as u64).into()));
    }
    fields
}

fn compaction_fields() -> Vec<(String, InfoValue)> {
    vec![
        (
            "compaction_failures_total".to_owned(),
            COMPACTION_FAILURES.load(Ordering::Relaxed).into(),
        ),
        (
            "compaction_state_rebuilds_total".to_owned(),
            COMPACTION_STATE_REBUILDS.load(Ordering::Relaxed).into(),
        ),
        (
            "compaction_policy_skipped_total".to_owned(),
            COMPACTION_POLICY_SKIPS.load(Ordering::Relaxed).into(),
        ),
    ]
}

/// A metrics section's `INFO` fields: each counter and gauge under its
/// `TS._DEBUG METRICS` name, a byte metric followed by its `_human` rendering. Histograms are left
/// out (see [`info_value`]). Values are read without the `METRICS RESET` baseline, so counters only
/// ever increase, as a scraper expects.
///
/// Returning plain data keeps the conversion testable without a server.
fn info_fields(section: Section) -> Vec<(String, InfoValue)> {
    info_fields_from(metrics::current(&[section]))
}

/// [`info_fields`] for readings already taken.
fn info_fields_from(readings: Vec<metrics::MetricReading>) -> Vec<(String, InfoValue)> {
    let mut fields = Vec::new();
    for reading in readings {
        if let Some(value) = info_value(&reading.value) {
            fields.push((reading.name.clone(), value));
        }
        if let Some(bytes) = reading.bytes() {
            fields.push((
                format!("{}_human", reading.name),
                bytes_to_human(bytes).into(),
            ));
        }
    }
    fields
}

/// A metric as an INFO field. INFO has no shape for a histogram, so those are left to
/// `TS._DEBUG METRICS`.
fn info_value(value: &MetricValue) -> Option<InfoValue> {
    Some(match *value {
        MetricValue::Counter(v) => v.into(),
        MetricValue::Gauge(Scalar::Int(v)) => v.into(),
        MetricValue::Gauge(Scalar::UInt(v)) => v.into(),
        MetricValue::Gauge(Scalar::Float(v)) => InfoValue::F64(v),
        MetricValue::Histogram(_) => return None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::metrics::MetricKind;

    fn mirrored() -> impl Iterator<Item = Section> {
        SECTIONS.iter().filter_map(|section| match *section {
            InfoSection::Metrics(section) => Some(section),
            _ => None,
        })
    }

    fn field_names(section: Section) -> Vec<String> {
        info_fields(section)
            .into_iter()
            .map(|(name, _)| name)
            .collect()
    }

    /// Every counter and gauge of the section, in `METRICS` order, with a `_human` twin after each
    /// byte metric; no histogram.
    #[test]
    fn info_section_mirrors_the_metrics_section() {
        for section in mirrored() {
            let mut expected = Vec::new();
            for reading in metrics::current(&[section]) {
                if reading.kind == MetricKind::Histogram {
                    continue;
                }
                if reading.bytes().is_some() {
                    expected.push(reading.name.clone());
                    expected.push(format!("{}_human", reading.name));
                } else {
                    expected.push(reading.name);
                }
            }
            assert_eq!(field_names(section), expected, "{}", section.as_str());
        }
    }

    #[test]
    fn task_section_leaves_out_duration_histograms() {
        let names = field_names(Section::Task);
        assert!(names.contains(&"task_retention_trim_started_total".to_owned()));
        assert!(names.contains(&"task_retention_trim_last_completed_timestamp_seconds".to_owned()));
        assert!(!names.iter().any(|n| n.ends_with("_duration_seconds")));
    }

    #[test]
    fn byte_gauges_get_a_human_twin() {
        assert_eq!(
            field_names(Section::StringInterning),
            [
                "string_interning_bytes",
                "string_interning_bytes_human",
                "string_interning_strings"
            ]
        );
    }

    /// A plain `INFO` emits every section; a crash report leaves out the index walk, which takes
    /// locks the faulting thread may hold.
    #[test]
    fn crash_report_skips_only_the_index_section() {
        let emitted = |for_crash_report| -> Vec<_> {
            SECTIONS
                .iter()
                .filter(|section| section.emitted(for_crash_report))
                .map(|section| section.name())
                .collect()
        };
        let all: Vec<_> = SECTIONS.iter().map(|section| section.name()).collect();
        assert_eq!(emitted(false), all);
        let crash = emitted(true);
        assert_eq!(crash, all[1..]);
        assert!(!crash.contains(&"index"));
    }

    #[test]
    fn section_names_are_unique() {
        let names: Vec<_> = SECTIONS.iter().map(|section| section.name()).collect();
        for (i, name) in names.iter().enumerate() {
            assert!(!names[..i].contains(name), "{name} listed twice");
        }
    }
}
