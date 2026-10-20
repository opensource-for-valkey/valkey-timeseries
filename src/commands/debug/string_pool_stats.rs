use super::string_pool_stats_fanout_command::{StringPoolStatsFanoutCommand, StringPoolSummary};
use super::target::{is_target_keyword, parse_node_target, resolve_node_target};
use crate::commands::CommandArgIterator;
use crate::commands::fanout_codec::generated::StringPoolTopKEntry;
use crate::common::replies::*;
use crate::common::string_interner::{BucketStats, saved_pct};
use crate::fanout::{FanoutClientCommand, FanoutTarget};
use valkey_module::{Context, NextArg, ValkeyResult};

/// Dumps a bucket's statistics to the reply.
fn dump_bucket(ctx: &Context, bucket: &BucketStats) {
    reply_with_map(ctx, 6);

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
    reply_with_str(ctx, "utilizationPct");
    reply_with_usize(ctx, utilization as usize);
}

/// Dumps a top-K entry to the reply.
fn dump_top_k_entry(ctx: &Context, entry: &StringPoolTopKEntry) {
    reply_with_map(ctx, 4);

    reply_with_str(ctx, "value");
    reply_with_bulk_string(ctx, &entry.value);

    reply_with_str(ctx, "refCount");
    reply_with_usize(ctx, entry.ref_count as usize);

    reply_with_str(ctx, "bytes");
    reply_with_usize(ctx, entry.bytes as usize);

    reply_with_str(ctx, "allocated");
    reply_with_usize(ctx, entry.allocated as usize);
}

/// Returns statistics about this node's string pool, or the cluster's with `CLUSTER`.
///
/// TS._DEBUG STRINGPOOLSTATS [k] [LOCAL|PRIMARY|CLUSTER|HASHTAG <tag>[,<tag>...]]
///
/// `CLUSTER` (like `PRIMARY`) sums the statistics over one primary per shard (see
/// [`StringPoolSummary`] for what the sums mean); `HASHTAG` over the primaries of the shards
/// owning those tags. Any target but `LOCAL` needs cluster mode.
pub(super) fn string_pool_stats(ctx: &Context, args: &mut CommandArgIterator) -> ValkeyResult<()> {
    // Parse optional k parameter (default: 0 for backward compatibility). Anything that is not a
    // node target must be it, so a malformed k reports the integer parse error.
    let mut k: usize = 0;
    if let Some(arg) = args.peek()
        && !is_target_keyword(arg.as_slice())
    {
        k = args.next_u64()? as usize;
    }

    // A second target is refused by the parser, so this loop reads at most one.
    let mut target = None;
    while parse_node_target(args, &mut target)? {}
    args.done()?;

    // Replicas hold a copy of their primary's labels, so including them would count each
    // shard's pool twice.
    if let Some(target) = resolve_node_target(ctx, target, FanoutTarget::Primary)? {
        StringPoolStatsFanoutCommand::new(k, target).exec(ctx)?;
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

    // Reply[1] -> ByRefcount, Reply[2] -> BySize
    for buckets in [&stats.by_ref_count, &stats.by_size] {
        reply_with_array(ctx, buckets.len());
        for (&key, bucket) in buckets {
            reply_with_array(ctx, 2);
            reply_with_usize(ctx, key);
            dump_bucket(ctx, bucket);
        }
    }

    // Reply[3] -> MemorySavings
    //
    // `memorySavedPct` compares the pool against one allocation per reference and nothing else,
    // so it reads near 100% on any label set worth interning. The holder slot is what a
    // reference costs whether or not the bytes behind it are shared, so the four fields after
    // it restate the same saving against total string storage; see `Stats` for the split.
    let total_storage_bytes = stats.total_storage_bytes();
    reply_with_map(ctx, 6);
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
        // Reply[4] -> TopK by RefCount, Reply[5] -> TopK by Size
        for entries in [&stats.top_k_by_ref, &stats.top_k_by_size] {
            reply_with_array(ctx, entries.len());
            for entry in entries {
                dump_top_k_entry(ctx, entry);
            }
        }
    }
}
