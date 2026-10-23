//! The module's `INFO` sections: `ts_index`, `ts_compaction`, `ts_rdb`, `ts_blocked_read`,
//! `ts_string_interning`, `ts_task`, `ts_asm`, `ts_index_reconciliation` and `ts_fanout`.
//!
//! Module sections are emitted only for an explicit `INFO modules`, `INFO everything`, or
//! `INFO <section>`; bare `INFO` and `INFO all` omit them. Section selection is checked before
//! collecting data, so an index walk is needed only when the index section is requested.

use crate::common::humanize::bytes_to_human;
use crate::common::metrics::{self, MetricValue, Section};
use crate::series::index::index_memory_usage;
use crate::series::{COMPACTION_FAILURES, COMPACTION_STATE_REBUILDS};
use metered::Scalar;
use valkey_module::{
    InfoContext, InfoContextBuilderFieldBottomLevelValue as InfoValue, Status, ValkeyResult, raw,
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
    write_section(ctx, "index", || {
        let index = index_memory_usage();
        let mut fields = Vec::new();
        for (name, bytes) in [
            ("index_total_bytes", index.total_bytes()),
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
    })?;
    write_section(ctx, "compaction", || {
        vec![
            (
                "compaction_failures_total".to_owned(),
                COMPACTION_FAILURES
                    .load(std::sync::atomic::Ordering::Relaxed)
                    .into(),
            ),
            (
                "compaction_state_rebuilds_total".to_owned(),
                COMPACTION_STATE_REBUILDS
                    .load(std::sync::atomic::Ordering::Relaxed)
                    .into(),
            ),
        ]
    })?;

    for &section in MIRRORED {
        write_section(ctx, section.as_str(), || info_fields(section))?;
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

/// A metrics section's `INFO` fields: each counter and gauge under its
/// `TS._DEBUG STATS` name, a byte metric followed by its `_human` rendering. Histograms are left
/// out (see [`info_value`]). Values are read without the `STATS RESET` baseline, so counters only
/// ever increase, as a scraper expects.
///
/// Returning plain data keeps the conversion testable without a server.
fn info_fields(section: Section) -> Vec<(String, InfoValue)> {
    let mut fields = Vec::new();
    for reading in metrics::current(&[section]) {
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
/// `TS._DEBUG STATS`.
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

    fn field_names(section: Section) -> Vec<String> {
        info_fields(section)
            .into_iter()
            .map(|(name, _)| name)
            .collect()
    }

    /// Every counter and gauge of the section, in `STATS` order, with a `_human` twin after each
    /// byte metric; no histogram.
    #[test]
    fn info_section_mirrors_the_metrics_section() {
        for &section in MIRRORED {
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
}
