use super::query_cache_fanout_command::ClearQueryCacheFanoutCommand;
use super::target::{parse_node_target, resolve_node_target};
use crate::commands::CommandArgIterator;
use crate::common::replies::reply_with_usize;
use crate::fanout::{FanoutClientCommand, FanoutTarget};
use crate::promql::engine::query_cache;
use valkey_module::{Context, ValkeyError, ValkeyResult};

/// Empties the parsed PromQL query cache, first sightings included, and replies with how many
/// entries it held. The `INFO ts_promql` counters are left alone.
///
/// TS._DEBUG CLEAR_QUERY_CACHE [LOCAL|PRIMARY|CLUSTER|HASHTAG <tag>[,<tag>...]]
///
/// With no target, or `LOCAL`, this node alone is cleared. The cache is per process, so
/// `CLUSTER` clears every node, replicas included (see [`ClearQueryCacheFanoutCommand`]);
/// `PRIMARY` the shard primaries, and `HASHTAG` the primaries of the shards owning those tags.
/// The reply is then the sum over those nodes. Any target but `LOCAL` needs cluster mode.
pub(super) fn clear_query_cache(ctx: &Context, args: &mut CommandArgIterator) -> ValkeyResult<()> {
    let mut target = None;
    parse_node_target(args, &mut target)?;
    if args.peek().is_some() {
        return Err(ValkeyError::Str(
            "TSDB: CLEAR_QUERY_CACHE takes no arguments but a node target",
        ));
    }

    if let Some(target) = resolve_node_target(ctx, target, FanoutTarget::All)? {
        ClearQueryCacheFanoutCommand::new(target).exec(ctx)?;
        return Ok(());
    }

    reply_with_usize(ctx, query_cache::clear());
    Ok(())
}
