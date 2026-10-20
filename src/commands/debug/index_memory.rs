use super::index_memory_fanout_command::{IndexMemoryFanoutCommand, local_index_memory};
use super::target::{parse_node_target, resolve_node_target};
use crate::commands::CommandArgIterator;
use crate::common::context::get_current_db;
use crate::common::replies::*;
use crate::error_consts;
use crate::fanout::{FanoutClientCommand, FanoutTarget};
use crate::series::index::IndexMemory;
use valkey_module::{Context, ValkeyError, ValkeyResult};

/// Returns the heap footprint of this node's label index for the selected database, or summed
/// over every database with `ALLDBS`.
///
/// TS._DEBUG INDEXMEMORY [ALLDBS] [LOCAL|PRIMARY|CLUSTER|HASHTAG <tag>[,<tag>...]]
///
/// The options may come in either order, each at most once. With no target, or `LOCAL`, this
/// node answers alone. `CLUSTER` sums the footprint over one node per shard, a replica where the
/// shard has one (see [`IndexMemoryFanoutCommand`]); `PRIMARY` sums it over the shard primaries,
/// and `HASHTAG` over the primaries of the shards owning those tags. Any target but `LOCAL`
/// needs cluster mode.
pub(super) fn index_memory(ctx: &Context, args: &mut CommandArgIterator) -> ValkeyResult<()> {
    let mut all_dbs = false;
    let mut target = None;
    while let Some(arg) = args.peek() {
        if !all_dbs && arg.as_slice().eq_ignore_ascii_case(b"ALLDBS") {
            args.next();
            all_dbs = true;
        } else if !parse_node_target(args, &mut target)? {
            return Err(ValkeyError::Str(error_consts::INVALID_ARGUMENT));
        }
    }

    if let Some(target) = resolve_node_target(ctx, target, FanoutTarget::ReplicaPerShard)? {
        IndexMemoryFanoutCommand::new(all_dbs, target).exec(ctx)?;
        return Ok(());
    }

    let memory = local_index_memory(get_current_db(ctx), all_dbs);
    reply_with_index_memory(ctx, &memory, 1);
    Ok(())
}

/// Writes a `TS._DEBUG INDEXMEMORY` reply. Shared by the local path and the cluster fan-out, so
/// both reply with the same shape. The fields are those of `INFO ts_memory`'s `index_*` group;
/// `nodes` is how many nodes the figures were summed over.
pub(super) fn reply_with_index_memory(ctx: &Context, memory: &IndexMemory, nodes: usize) {
    let fields = [
        ("totalBytes", memory.total_bytes()),
        ("termsBytes", memory.terms_bytes),
        ("postingsBytes", memory.postings_bytes),
        ("idToKeyBytes", memory.id_to_key_bytes),
        ("bookkeepingBytes", memory.bookkeeping_bytes),
        ("terms", memory.term_count),
        ("series", memory.series_count),
        ("databases", memory.db_count),
        ("nodes", nodes),
    ];

    reply_with_map(ctx, fields.len());
    for (name, value) in fields {
        reply_with_str(ctx, name);
        reply_with_usize(ctx, value);
    }
}
