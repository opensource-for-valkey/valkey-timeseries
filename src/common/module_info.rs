//! The module's `INFO` sections: `ts_index`, `ts_compaction`, `ts_rdb`, `ts_blocked_read`,
//! `ts_string_interning`, `ts_task`, `ts_asm`, `ts_index_reconciliation` and `ts_fanout`.
//!
//! Module sections are emitted only for an explicit `INFO modules`, `INFO everything`/`INFO all`,
//! or `INFO <section>` — never for a bare `INFO` — which is what makes it acceptable for the
//! index section to walk the term dictionary.

use crate::common::humanize::bytes_to_human;
use crate::common::metrics::{self, MetricValue, Section};
use crate::series::index::index_memory_usage;
use crate::series::{COMPACTION_FAILURES, COMPACTION_STATE_REBUILDS};
use metered::Scalar;
use valkey_module::{
    InfoContext, InfoContextBuilderFieldBottomLevelValue, InfoContextBuilderFieldTopLevelValue,
    OneInfoSectionData, ValkeyResult,
};
use valkey_module_macros::info_command_handler;

/// `INFO ts_index`, `INFO ts_compaction`, `INFO ts_rdb`, `INFO ts_blocked_read`,
/// `INFO ts_string_interning`, `INFO ts_task`, `INFO ts_asm`, `INFO ts_index_reconciliation` and
/// `INFO ts_fanout`.
///
/// The sections are declared without the module name; the server prefixes each with it, so they
/// land as `# ts_index`, `# ts_compaction`, `# ts_rdb` and so on, with every field prefixed
/// `ts_`.
///
/// Each `*_bytes` field, mirrored ones included, is followed by a `*_bytes_human` rendering of it
/// (`1.21M`), formatted as the server formats `used_memory_human`.
#[info_command_handler]
fn module_info(ctx: &InfoContext, _for_crash_report: bool) -> ValkeyResult<()> {
    let index = index_memory_usage();

    ctx.builder()
        .add_section("index")
        .field("index_total_bytes", index.total_bytes() as u64)?
        .field("index_total_bytes_human", human(index.total_bytes()))?
        .field("index_terms_bytes", index.terms_bytes as u64)?
        .field("index_terms_bytes_human", human(index.terms_bytes))?
        .field("index_postings_bytes", index.postings_bytes as u64)?
        .field("index_postings_bytes_human", human(index.postings_bytes))?
        .field("index_id_to_key_bytes", index.id_to_key_bytes as u64)?
        .field("index_id_to_key_bytes_human", human(index.id_to_key_bytes))?
        .field("index_bookkeeping_bytes", index.bookkeeping_bytes as u64)?
        .field(
            "index_bookkeeping_bytes_human",
            human(index.bookkeeping_bytes),
        )?
        .field("index_terms", index.term_count as u64)?
        .field("index_series", index.series_count as u64)?
        .field("index_databases", index.db_count as u64)?
        .build_section()?
        .add_section("compaction")
        .field(
            "compaction_failures",
            COMPACTION_FAILURES.load(std::sync::atomic::Ordering::Relaxed),
        )?
        .field(
            "compaction_state_rebuilds",
            COMPACTION_STATE_REBUILDS.load(std::sync::atomic::Ordering::Relaxed),
        )?
        .build_section()?
        .build_info()?;

    for &section in MIRRORED {
        ctx.build_one_section(info_section(section))?;
    }

    Ok(())
}

/// The `TS._DEBUG STATS` sections `INFO` mirrors, each as a section of its own.
const MIRRORED: &[Section] = &[
    Section::Rdb,
    Section::Read,
    Section::StringInterning,
    Section::Task,
    Section::Asm,
    Section::IndexReconciliation,
    Section::Fanout,
];

/// A metrics section as an `INFO` section of the same name: each counter and gauge under its
/// `TS._DEBUG STATS` name, a byte metric followed by its `_human` rendering. Histograms are left
/// out (see [`info_value`]). Values are read without the `STATS RESET` baseline, so counters only
/// ever increase, as a scraper expects.
///
/// Plain data rather than a builder step: valkey-module doesn't export its builder type, and this
/// way the conversion is testable without a server.
fn info_section(section: Section) -> OneInfoSectionData {
    let mut fields = Vec::new();
    for reading in metrics::current(&[section]) {
        if let Some(value) = info_value(&reading.value) {
            fields.push((
                reading.name.clone(),
                InfoContextBuilderFieldTopLevelValue::Value(value),
            ));
        }
        if let Some(bytes) = reading.bytes() {
            fields.push((
                format!("{}_human", reading.name),
                InfoContextBuilderFieldTopLevelValue::Value(bytes_to_human(bytes).into()),
            ));
        }
    }
    (section.as_str().to_owned(), fields)
}

/// A metric as an INFO field. INFO has no shape for a histogram, so those are left to
/// `TS._DEBUG STATS`.
fn info_value(value: &MetricValue) -> Option<InfoContextBuilderFieldBottomLevelValue> {
    Some(match *value {
        MetricValue::Counter(v) => v.into(),
        MetricValue::Gauge(Scalar::Int(v)) => v.into(),
        MetricValue::Gauge(Scalar::UInt(v)) => v.into(),
        MetricValue::Gauge(Scalar::Float(v)) => InfoContextBuilderFieldBottomLevelValue::F64(v),
        MetricValue::Histogram(_) => return None,
    })
}

fn human(bytes: usize) -> String {
    bytes_to_human(bytes as u64)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::metrics::MetricKind;

    fn field_names(section: Section) -> Vec<String> {
        info_section(section)
            .1
            .into_iter()
            .map(|(name, _)| name)
            .collect()
    }

    /// Every counter and gauge of the section, in `STATS` order, with a `_human` twin after each
    /// byte metric; no histogram.
    #[test]
    fn info_section_mirrors_the_metrics_section() {
        for &section in MIRRORED {
            let (name, _) = info_section(section);
            assert_eq!(name, section.as_str());
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
        assert!(names.contains(&"task_trim_started_total".to_owned()));
        assert!(names.contains(&"task_trim_last_completed_timestamp_seconds".to_owned()));
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
}
