use super::ts_debug_configs::list_configs_cmd;
use crate::commands::CommandArgIterator;
use crate::commands::command_parser::parse_query_index_command_args;
use crate::common::context::replies::*;
use crate::common::string_interner::{BucketStats, InternedString, TopKEntry};
use crate::config::is_debug_mode_enabled;
use crate::error_consts;
use crate::series::index::series_keys_by_selectors;
use valkey_module::{Context, NextArg, ValkeyError, ValkeyResult, ValkeyString};

/// Dumps a bucket's statistics to the reply.
fn dump_bucket(ctx: &Context, bucket: &BucketStats) {
    reply_with_array(ctx, 12);

    reply_with_str(ctx, "count");
    reply_with_usize(ctx, bucket.count);

    reply_with_str(ctx, "bytes");
    reply_with_usize(ctx, bucket.bytes);

    reply_with_str(ctx, "avgSize");
    reply_with_double(ctx.ctx, bucket.get_avg_size());

    reply_with_str(ctx, "allocated");
    reply_with_usize(ctx, bucket.allocated);

    reply_with_str(ctx, "avgAllocated");
    reply_with_double(ctx, bucket.get_avg_allocated());

    let utilization = bucket.get_utilization() * 100.0;
    reply_with_str(ctx, "utilization");
    reply_with_usize(ctx, utilization as usize);
}

/// Dumps a TopKEntry to the reply.
fn dump_top_k_entry(ctx: &Context, entry: &TopKEntry) {
    reply_with_array(ctx, 8);

    reply_with_str(ctx, "value");
    reply_with_bulk_string(ctx, &entry.value);

    reply_with_str(ctx, "refCount");
    reply_with_usize(ctx, entry.ref_count);

    reply_with_str(ctx, "bytes");
    reply_with_usize(ctx, entry.bytes);

    reply_with_str(ctx, "allocated");
    reply_with_usize(ctx, entry.allocated);
}

/// Returns statistics about the string pool.
///
/// TS._DEBUG STRINGPOOLSTATS
fn string_pool_stats(ctx: &Context, args: &mut CommandArgIterator) -> ValkeyResult<()> {
    // Parse optional k parameter (default: 0 for backward compatibility)
    let k = if args.peek().is_some() {
        args.next_u64()? as usize
    } else {
        0
    };

    args.done()?;

    // todo: currently we're local only. Support cluster mode
    let stats = InternedString::get_stats_with_top_k(k);

    let arr_len = if k > 0 { 6 } else { 4 };
    reply_with_array(ctx, arr_len);

    // Reply[0] -> GlobalStats
    dump_bucket(ctx, &stats.total_stats);

    // Reply[1] -> ByRefcount
    reply_with_array(ctx, stats.by_ref_stats.len());
    for (&ref_count, bucket) in &stats.by_ref_stats {
        reply_with_array(ctx, 2);
        reply_with_usize(ctx, ref_count);
        dump_bucket(ctx, bucket);
    }

    // Reply[2] -> BySize
    reply_with_array(ctx, stats.by_size_stats.len());
    for (&size, bucket) in &stats.by_size_stats {
        reply_with_array(ctx, 2);
        reply_with_usize(ctx, size);
        dump_bucket(ctx, bucket);
    }

    // Reply[3] -> MemorySavings
    reply_with_array(ctx, 4);
    reply_with_str(ctx, "memorySavedBytes");
    reply_with_usize(ctx, stats.memory_saved_bytes);
    reply_with_str(ctx, "memorySavedPct");
    reply_with_double(ctx.ctx, stats.memory_saved_pct);

    if k > 0 {
        // Reply[4] -> TopK by RefCount
        reply_with_array(ctx, stats.top_k_by_ref.len());
        for entry in &stats.top_k_by_ref {
            dump_top_k_entry(ctx, entry);
        }

        // Reply[5] -> TopK by Size
        reply_with_array(ctx, stats.top_k_by_size.len());
        for entry in &stats.top_k_by_size {
            dump_top_k_entry(ctx, entry);
        }
    }

    Ok(())
}

/// Runs a query against this node's *local* index only, bypassing the cluster fanout that
/// `TS.QUERYINDEX` performs. This is primarily used by tests to assert per-node index state (for
/// example, that a source node's index was cleared after an atomic slot migration, which a
/// fanned-out `TS.QUERYINDEX` cannot observe because peers may still hold the keys).
///
/// TS._DEBUG QUERYINDEX <filter> [<filter> ...]
fn local_query_index(ctx: &Context, args: &mut CommandArgIterator) -> ValkeyResult<()> {
    let options = parse_query_index_command_args(args)?;
    let mut keys = series_keys_by_selectors(ctx, &options.matchers, options.date_range)?;
    keys.sort_unstable();

    reply_with_array(ctx, keys.len());
    for key in keys.iter() {
        reply_with_valkey_string(ctx, key);
    }
    Ok(())
}

/// Displays help text for the TS._DEBUG command.
fn help_cmd(ctx: &Context, args: &mut CommandArgIterator) -> ValkeyResult<()> {
    args.done()?;

    const HELP_TEXT: &[(&str, &str)] = &[
        ("TS._DEBUG SHOW_INFO", "Show Info Variable Information"),
        (
            "TS._DEBUG STRINGPOOLSTATS [TOPK]",
            "Show String Interner Stats",
        ),
        (
            "TS._DEBUG QUERYINDEX <filter> [<filter> ...]",
            "Query this node's local index only (no cluster fanout)",
        ),
        (
            "TS._DEBUG LIST_CONFIGS [VERBOSE] [APP|DEV|HIDDEN]",
            "List config names (default) or VERBOSE details, optionally filtered by visibility",
        ),
    ];

    reply_with_array(ctx, HELP_TEXT.len() * 2);
    for &(command, description) in HELP_TEXT {
        reply_with_bulk_string(ctx, command);
        reply_with_bulk_string(ctx, description);
    }

    Ok(())
}

/// Main entry point for TS._DEBUG command.
///
/// The whole command surface is gated on `debug-mode`, which is off by default: these
/// subcommands expose module internals that are not part of the supported API. The gate is
/// checked before the subcommand is parsed, so a disabled server reports that it is disabled
/// rather than complaining about the arguments.
pub fn ts_debug_cmd(ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult<()> {
    if !is_debug_mode_enabled() {
        return Err(ValkeyError::Str(error_consts::DEBUG_MODE_DISABLED));
    }

    // skip the command name and parse the subcommand keyword
    let mut itr = args.into_iter().skip(1).peekable();

    let keyword = itr.next_str()?.to_ascii_uppercase();

    match keyword.as_str() {
        "STRINGPOOLSTATS" => string_pool_stats(ctx, &mut itr),
        "QUERYINDEX" => local_query_index(ctx, &mut itr),
        "HELP" => help_cmd(ctx, &mut itr),
        "LIST_CONFIGS" => list_configs_cmd(ctx, &mut itr),
        _ => Err(ValkeyError::String(format!(
            "Unknown subcommand: {} try HELP subcommand",
            keyword
        ))),
    }
}
