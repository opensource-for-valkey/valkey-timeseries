use super::fanout_codec::generated::StringPoolTopKEntry;
use super::ts_debug_configs::list_configs_cmd;
use super::ts_string_pool_stats_fanout_command::{StringPoolStatsFanoutCommand, StringPoolSummary};
use crate::commands::CommandArgIterator;
use crate::commands::analysis_runner::panic_next_analysis_job;
use crate::commands::command_parser::parse_query_index_command_args;
use crate::common::replies::*;
use crate::common::string_interner::{BucketStats, saved_pct};
use crate::config::is_debug_mode_enabled;
use crate::error_consts;
use crate::fanout::{FanoutClientCommand, is_clustered};
use crate::series::index::series_keys_by_selectors;
use valkey_module::{Context, NextArg, ValkeyError, ValkeyResult, ValkeyString, ValkeyValue};

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

/// Dumps a top-K entry to the reply.
fn dump_top_k_entry(ctx: &Context, entry: &StringPoolTopKEntry) {
    reply_with_array(ctx, 8);

    reply_with_str(ctx, "value");
    reply_with_bulk_string(ctx, &entry.value);

    reply_with_str(ctx, "refCount");
    reply_with_usize(ctx, entry.ref_count as usize);

    reply_with_str(ctx, "bytes");
    reply_with_usize(ctx, entry.bytes as usize);

    reply_with_str(ctx, "allocated");
    reply_with_usize(ctx, entry.allocated as usize);
}

/// Returns statistics about the string pool.
///
/// TS._DEBUG STRINGPOOLSTATS [k] [LOCAL]
///
/// In cluster mode the statistics are summed over one primary per shard (see
/// [`StringPoolSummary`] for what the sums mean); `LOCAL` reports this node's pool alone.
fn string_pool_stats(ctx: &Context, args: &mut CommandArgIterator) -> ValkeyResult<()> {
    // Parse optional k parameter (default: 0 for backward compatibility)
    let k = match args.peek() {
        Some(arg) if !arg.as_slice().eq_ignore_ascii_case(b"LOCAL") => args.next_u64()? as usize,
        _ => 0,
    };
    let local = match args.peek() {
        Some(arg) if arg.as_slice().eq_ignore_ascii_case(b"LOCAL") => {
            args.next();
            true
        }
        _ => false,
    };

    args.done()?;

    if !local && is_clustered(ctx) {
        StringPoolStatsFanoutCommand::new(k).exec(ctx)?;
        return Ok(());
    }

    reply_with_string_pool_stats(ctx, &StringPoolSummary::local(k), k > 0);
    Ok(())
}

/// Writes a `TS._DEBUG STRINGPOOLSTATS` reply. Shared by the local path and the cluster fan-out,
/// so both reply with the same shape.
pub(super) fn reply_with_string_pool_stats(
    ctx: &Context,
    stats: &StringPoolSummary,
    with_top_k: bool,
) {
    let arr_len = if with_top_k { 6 } else { 4 };
    reply_with_array(ctx, arr_len);

    // Reply[0] -> GlobalStats
    dump_bucket(ctx, &stats.total);

    // Reply[1] -> ByRefcount
    reply_with_array(ctx, stats.by_ref_count.len());
    for (&ref_count, bucket) in &stats.by_ref_count {
        reply_with_array(ctx, 2);
        reply_with_usize(ctx, ref_count);
        dump_bucket(ctx, bucket);
    }

    // Reply[2] -> BySize
    reply_with_array(ctx, stats.by_size.len());
    for (&size, bucket) in &stats.by_size {
        reply_with_array(ctx, 2);
        reply_with_usize(ctx, size);
        dump_bucket(ctx, bucket);
    }

    // Reply[3] -> MemorySavings
    //
    // `memorySavedPct` compares the pool against one allocation per reference and nothing else,
    // so it reads near 100% on any label set worth interning. The holder slot is what a
    // reference costs whether or not the bytes behind it are shared, so the four fields after
    // it restate the same saving against total string storage; see `Stats` for the split.
    let total_storage_bytes = stats.total_storage_bytes();
    reply_with_array(ctx, 12);
    reply_with_str(ctx, "memorySavedBytes");
    reply_with_usize(ctx, stats.memory_saved_bytes);
    reply_with_str(ctx, "memorySavedPct");
    reply_with_double(
        ctx.ctx,
        saved_pct(stats.memory_saved_bytes, stats.total.allocated),
    );
    reply_with_str(ctx, "holders");
    reply_with_usize(ctx, stats.holder_count);
    reply_with_str(ctx, "holderSlotBytes");
    reply_with_usize(ctx, stats.holder_slot_bytes);
    reply_with_str(ctx, "totalStorageBytes");
    reply_with_usize(ctx, total_storage_bytes);
    reply_with_str(ctx, "storageSavedPct");
    reply_with_double(
        ctx.ctx,
        saved_pct(stats.memory_saved_bytes, total_storage_bytes),
    );

    if with_top_k {
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
}

/// Runs a query against this node's *local* index only, bypassing the cluster fanout that
/// `TS.QUERYINDEX` performs. This is primarily used by tests to assert per-node index state (for
/// example, that a source node's index was cleared after an atomic slot migration, which a
/// fanned-out `TS.QUERYINDEX` cannot observe because peers may still hold the keys).
///
/// TS._DEBUG QUERYINDEX <filter> [<filter> ...]
fn local_query_index(ctx: &Context, args: &mut CommandArgIterator) -> ValkeyResult<()> {
    // HASHTAG is accepted by the shared parser but meaningless here: this path never
    // fans out, so the tags are discarded.
    let (options, _tags) = parse_query_index_command_args(args)?;
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
            "TS._DEBUG STRINGPOOLSTATS [TOPK] [LOCAL]",
            "Show String Interner Stats (summed over shard primaries in cluster mode unless LOCAL)",
        ),
        (
            "TS._DEBUG QUERYINDEX <filter> [<filter> ...]",
            "Query this node's local index only (no cluster fanout)",
        ),
        (
            "TS._DEBUG LIST_CONFIGS [VERBOSE] [APP|DEV|HIDDEN]",
            "List config names (default) or VERBOSE details, optionally filtered by visibility",
        ),
        (
            "TS._DEBUG PANIC_NEXT_ANALYSIS_JOB",
            "Make the next job on the analysis lane panic (tests the error reply)",
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
///
/// Every subcommand writes its own reply, so success maps to `NoReply`: `Ok(())` would convert
/// to a Null reply and send it after the real one, desynchronizing the client.
pub fn ts_debug_cmd(ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    if !is_debug_mode_enabled() {
        return Err(ValkeyError::Str(error_consts::DEBUG_MODE_DISABLED));
    }

    // skip the command name and parse the subcommand keyword
    let mut itr = args.into_iter().skip(1).peekable();

    let keyword = itr.next_str()?.to_ascii_uppercase();

    let result = match keyword.as_str() {
        "STRINGPOOLSTATS" => string_pool_stats(ctx, &mut itr),
        "QUERYINDEX" => local_query_index(ctx, &mut itr),
        "HELP" => help_cmd(ctx, &mut itr),
        "LIST_CONFIGS" => list_configs_cmd(ctx, &mut itr),
        "PANIC_NEXT_ANALYSIS_JOB" => {
            itr.done()?;
            panic_next_analysis_job();
            reply_with_str(ctx, "OK");
            Ok(())
        }
        _ => Err(ValkeyError::String(format!(
            "Unknown subcommand: {} try HELP subcommand",
            keyword
        ))),
    };
    result.map(|()| ValkeyValue::NoReply)
}
