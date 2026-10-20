use super::configs::list_configs_cmd;
use super::ensure_debug_enabled;
use super::index_memory::index_memory;
use super::stats::stats_cmd;
use super::string_pool_stats::string_pool_stats;
use crate::commands::CommandArgIterator;
use crate::commands::analysis_runner::panic_next_analysis_job;
use crate::commands::command_parser::parse_query_index_command_args;
use crate::common::replies::*;
use crate::fanout::inflight_requests;
use crate::series::index::series_keys_by_selectors;
use valkey_module::{Context, NextArg, ValkeyError, ValkeyResult, ValkeyString, ValkeyValue};

/// Lists this node's fanout RPCs that still have remote shares outstanding, oldest first: one
/// map per request with `id` (a string: ids span the full `u64` range),
/// `command`, `ageMs`, `remoteTargets` and `outstanding`.
///
/// TS._DEBUG INFLIGHT
///
/// Node-local: a request is in flight only on the node coordinating it. A fanout whose only
/// target is this node has no RPC and never appears, and a request leaves the list as soon as
/// its timer fires.
fn inflight(ctx: &Context, args: &mut CommandArgIterator) -> ValkeyResult<()> {
    args.done()?;
    let requests = inflight_requests();
    reply_with_array(ctx, requests.len());
    for request in &requests {
        reply_with_map(ctx, 5);
        reply_with_str(ctx, "id");
        reply_with_bulk_string(ctx, &request.id.to_string());
        reply_with_str(ctx, "command");
        reply_with_str(ctx, request.command);
        reply_with_str(ctx, "ageMs");
        reply_with_integer(ctx, request.age.as_millis().min(i64::MAX as u128) as i64);
        reply_with_str(ctx, "remoteTargets");
        reply_with_usize(ctx, request.remote_targets);
        reply_with_str(ctx, "outstanding");
        reply_with_integer(ctx, request.outstanding.min(i64::MAX as u64) as i64);
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
    // HASHTAG is accepted by the shared parser but meaningless here: this path never
    // fans out, so the tags are discarded.
    let (options, _tags) = parse_query_index_command_args(args)?;
    let mut keys = series_keys_by_selectors(ctx, &options.matchers, options.date_range)?;
    keys.sort_unstable();

    reply_with_array(ctx, keys.len());
    for key in keys.iter() {
        reply_with_slice(ctx, key);
    }
    Ok(())
}

/// Displays help text for the TS._DEBUG command.
fn help_cmd(ctx: &Context, args: &mut CommandArgIterator) -> ValkeyResult<()> {
    args.done()?;

    const HELP_TEXT: &[(&str, &str)] = &[
        (
            "TS._DEBUG STATS [section ...] [VERBOSE] [LOCAL|PRIMARY|CLUSTER|HASHTAG <tags>]",
            "Show this node's module metrics, optionally for the named sections only (VERBOSE adds kind and description); CLUSTER sums counters and histograms over every node and lists gauges per node (PRIMARY over shard primaries; HASHTAG over the primaries of the shards owning those tags)",
        ),
        (
            "TS._DEBUG STATS RESET [LOCAL|PRIMARY|CLUSTER|HASHTAG <tags>]",
            "Start this node's metric counters and histograms over (gauges are left alone), or every node's with CLUSTER (PRIMARY: shard primaries; HASHTAG: the primaries of the shards owning those tags)",
        ),
        (
            "TS._DEBUG INFLIGHT",
            "List this node's fanout requests with remote shares outstanding, oldest first",
        ),
        (
            "TS._DEBUG STRINGPOOLSTATS [TOPK] [LOCAL|PRIMARY|CLUSTER|HASHTAG <tags>]",
            "Show this node's String Interner Stats (CLUSTER or PRIMARY sums them over shard primaries; HASHTAG over the primaries of the shards owning those tags)",
        ),
        (
            "TS._DEBUG INDEXMEMORY [ALLDBS] [LOCAL|PRIMARY|CLUSTER|HASHTAG <tags>]",
            "Show this node's label index heap usage for the selected db, or every db with ALLDBS (CLUSTER sums it over one node per shard, replicas preferred; PRIMARY over shard primaries; HASHTAG over the primaries of the shards owning those tags)",
        ),
        (
            "TS._DEBUG QUERYINDEX <filter> [<filter> ...]",
            "Query this node's local index only (no cluster fanout)",
        ),
        (
            "TS._DEBUG LIST_CONFIGS [VERBOSE]",
            "List config names (default) or VERBOSE details",
        ),
        (
            "TS._DEBUG PANIC_NEXT_ANALYSIS_JOB",
            "Make the next job on the analysis lane panic (tests the error reply)",
        ),
        (
            "TS._DEBUG FAIL_NEXT_COMPACTION",
            "Make the next compaction rule application fail (tests dirty-rule recovery)",
        ),
    ];

    reply_with_map(ctx, HELP_TEXT.len());
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
    ensure_debug_enabled()?;
    // skip the command name and parse the subcommand keyword
    let mut itr = args.into_iter().skip(1).peekable();

    let keyword = itr.next_str()?;

    let result = hashify::fnc_map_ignore_case!(keyword.as_bytes(),
        "STATS" => stats_cmd(ctx, &mut itr),
        "STRINGPOOLSTATS" => string_pool_stats(ctx, &mut itr),
        "INDEXMEMORY" => index_memory(ctx, &mut itr),
        "INFLIGHT" => inflight(ctx, &mut itr),
        "QUERYINDEX" => local_query_index(ctx, &mut itr),
        "HELP" => help_cmd(ctx, &mut itr),
        "LIST_CONFIGS" => list_configs_cmd(ctx, &mut itr),
        "PANIC_NEXT_ANALYSIS_JOB" => {
            itr.done()?;
            panic_next_analysis_job();
            reply_with_str(ctx, "OK");
            Ok(())
        },
        "FAIL_NEXT_COMPACTION" => {
            itr.done()?;
            crate::series::fail_next_compaction();
            reply_with_str(ctx, "OK");
            Ok(())
        },
        _ => Err(ValkeyError::String(format!(
            "Unknown subcommand: {} try HELP subcommand",
            keyword
        ))),
    );

    result.map(|()| ValkeyValue::NoReply)
}
