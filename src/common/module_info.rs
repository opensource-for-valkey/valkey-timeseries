//! The module's `INFO` sections.
//!
//! Module sections are emitted only for an explicit `INFO modules`, `INFO everything`/`INFO all`,
//! or `INFO <section>` — never for a bare `INFO` — which is what makes it acceptable for the
//! memory section to walk the term dictionary.

use crate::common::string_interner::InternedString;
use crate::series::index::index_memory_usage;
use valkey_module::{InfoContext, ValkeyResult};
use valkey_module_macros::info_command_handler;

/// `INFO ts_memory`: the module's global heap, which no per-key figure can show.
///
/// The section is declared as `memory`; the server prefixes it with the module name, so it lands
/// as `# ts_memory` with every field prefixed `ts_`.
///
/// `MEMORY USAGE <key>` and `TS.INFO memoryUsage` report one series: its chunks, its labels'
/// amortized share of the interner, and its own struct. The label index is not attached to any
/// key, so neither of them can see it at all, and on a high-cardinality keyspace it is routinely
/// the larger half of what the module holds. Everything reported here is *in addition to* the
/// per-key numbers, with one exception noted on `interned_strings_bytes`.
///
/// `terms_bytes` and `id_to_key_bytes` count the entries an adaptive radix tree and a `BTreeMap`
/// hold without their internal node overhead, so the totals are a floor.
#[info_command_handler]
fn memory_info(ctx: &InfoContext, _for_crash_report: bool) -> ValkeyResult<()> {
    let index = index_memory_usage();

    ctx.builder()
        .add_section("memory")
        .field("index_total_bytes", index.total_bytes() as u64)?
        .field("index_terms_bytes", index.terms_bytes as u64)?
        .field("index_postings_bytes", index.postings_bytes as u64)?
        .field("index_id_to_key_bytes", index.id_to_key_bytes as u64)?
        .field("index_bookkeeping_bytes", index.bookkeeping_bytes as u64)?
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
        .field(
            "interned_strings_bytes",
            InternedString::memory_used() as u64,
        )?
        .build_section()?
        .build_info()?;

    Ok(())
}
