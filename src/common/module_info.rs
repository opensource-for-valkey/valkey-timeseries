//! The module's `INFO` sections: `ts_memory` and `ts_compaction`.
//!
//! Module sections are emitted only for an explicit `INFO modules`, `INFO everything`/`INFO all`,
//! or `INFO <section>` — never for a bare `INFO` — which is what makes it acceptable for the
//! memory section to walk the term dictionary.

use crate::common::humanize::bytes_to_human;
use crate::common::string_interner::InternedString;
use crate::series::index::index_memory_usage;
use crate::series::{COMPACTION_FAILURES, COMPACTION_STATE_REBUILDS};
use valkey_module::{InfoContext, ValkeyResult};
use valkey_module_macros::info_command_handler;

/// `INFO ts_memory` and `INFO ts_compaction`.
///
/// `ts_memory` is the module's global heap, which no per-key figure can show.
/// `ts_compaction` counts the compaction rule applications that failed and the dirty rules that
/// were rebuilt afterwards, since module start.
///
/// The sections are declared as `memory` and `compaction`; the server prefixes each with the
/// module name, so they land as `# ts_memory` and `# ts_compaction` with every field prefixed
/// `ts_`.
///
/// `MEMORY USAGE <key>` and `TS.INFO memoryUsage` report one series: its chunks, its labels'
/// amortized share of the interner, and its own struct. The label index is not attached to any
/// key, so neither of them can see it at all, and on a high-cardinality keyspace it is routinely
/// the larger half of what the module holds. Everything reported here is *in addition to* the
/// per-key numbers, with one exception noted on `interned_strings_bytes`.
///
/// `terms_bytes` and `id_to_key_bytes` count the entries an adaptive radix tree and a `BTreeMap`
/// hold without their internal node overhead, so the totals are a floor.
///
/// Each `*_bytes` field is followed by a `*_bytes_human` rendering of it (`1.21M`), formatted
/// as the server formats `used_memory_human`.
#[info_command_handler]
fn module_info(ctx: &InfoContext, _for_crash_report: bool) -> ValkeyResult<()> {
    let index = index_memory_usage();
    let interned_bytes = InternedString::memory_used();

    ctx.builder()
        .add_section("memory")
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
        // The pool total, not a per-series share: this is the whole interner, which the
        // `memoryUsage` of the series holding those labels already divides amongst themselves.
        // Adding it to the sum of every key's `MEMORY USAGE` therefore double-counts it.
        //
        // Both of these are O(1) reads. `TS._DEBUG STRINGPOOLSTATS` has the distribution and the
        // savings figure, which cost a walk of the pool.
        .field("interned_strings", InternedString::interned_count() as u64)?
        .field("interned_strings_bytes", interned_bytes as u64)?
        .field("interned_strings_bytes_human", human(interned_bytes))?
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

    Ok(())
}

fn human(bytes: usize) -> String {
    bytes_to_human(bytes as u64)
}
