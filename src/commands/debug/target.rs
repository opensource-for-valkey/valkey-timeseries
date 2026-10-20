//! Node targets for `TS._DEBUG`: which nodes a subcommand reads.
//!
//! `LOCAL`, `PRIMARY`, `CLUSTER` and `HASHTAG <tag>[,<tag>...]` are parsed here rather than by
//! the shared RTS parser. They mean something only to `TS._DEBUG`, and registering them as
//! `CommandArgToken`s would change how every other command words its "invalid argument" errors.

use crate::commands::CommandArgIterator;
use crate::commands::command_parser::parse_hash_tags;
use crate::error_consts;
use crate::fanout::{FanoutTarget, is_clustered};
use valkey_module::{Context, ValkeyError, ValkeyResult};

/// Keywords that cannot stand for a hash tag: `HASHTAG ALLDBS` is a missing tag, not a tag named
/// `ALLDBS`, as `TS.MGET HASHTAG FILTER` is. These are the options of every subcommand that takes
/// a target, so a hash tag may not be named after any of them.
const OPTION_KEYWORDS: [&[u8]; 7] = [
    b"LOCAL", b"PRIMARY", b"CLUSTER", b"HASHTAG", b"ALLDBS", b"VERBOSE", b"RESET",
];

/// Which nodes a `TS._DEBUG` subcommand asks.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum NodeTarget {
    /// This node alone, as with no target.
    Local,
    /// The primary of every shard.
    Primary,
    /// The whole cluster, as each subcommand defines it (see [`resolve_node_target`]).
    Cluster,
    /// The primary of each shard owning one of these hash tags.
    HashTag(Vec<String>),
}

impl NodeTarget {
    fn keyword(&self) -> &'static str {
        match self {
            NodeTarget::Local => "LOCAL",
            NodeTarget::Primary => "PRIMARY",
            NodeTarget::Cluster => "CLUSTER",
            NodeTarget::HashTag(_) => "HASHTAG",
        }
    }

    fn from_bare_keyword(arg: &[u8]) -> Option<Self> {
        if arg.eq_ignore_ascii_case(b"LOCAL") {
            Some(NodeTarget::Local)
        } else if arg.eq_ignore_ascii_case(b"PRIMARY") {
            Some(NodeTarget::Primary)
        } else if arg.eq_ignore_ascii_case(b"CLUSTER") {
            Some(NodeTarget::Cluster)
        } else {
            None
        }
    }
}

/// Whether `arg` is a node target keyword.
pub(super) fn is_target_keyword(arg: &[u8]) -> bool {
    NodeTarget::from_bare_keyword(arg).is_some() || arg.eq_ignore_ascii_case(b"HASHTAG")
}

/// Consumes a node target into `slot` if the next argument is one, and says whether it was.
/// Nothing is consumed otherwise, so callers can mix a target with their own options in any
/// order. A target when `slot` already holds one is an error, not an unrelated option.
pub(super) fn parse_node_target(
    args: &mut CommandArgIterator,
    slot: &mut Option<NodeTarget>,
) -> ValkeyResult<bool> {
    let Some(arg) = args.peek() else {
        return Ok(false);
    };
    let bare = NodeTarget::from_bare_keyword(arg.as_slice());
    if bare.is_none() && !arg.as_slice().eq_ignore_ascii_case(b"HASHTAG") {
        return Ok(false);
    }
    if slot.is_some() {
        return Err(ValkeyError::Str(error_consts::DEBUG_DUPLICATE_TARGET));
    }
    args.next();
    *slot = Some(match bare {
        Some(target) => target,
        None => NodeTarget::HashTag(parse_tag_list(args)?),
    });
    Ok(true)
}

/// Reads the value of `HASHTAG`, refusing an option keyword in its place.
fn parse_tag_list(args: &mut CommandArgIterator) -> ValkeyResult<Vec<String>> {
    if args.peek().is_some_and(|next| {
        OPTION_KEYWORDS
            .iter()
            .any(|k| next.as_slice().eq_ignore_ascii_case(k))
    }) {
        return Err(ValkeyError::Str(error_consts::MISSING_HASHTAG));
    }
    parse_hash_tags(args)
}

/// Turns a parsed target into the nodes to fan out to, or `None` to answer from this node alone
/// (no target, or `LOCAL`). Any other target needs cluster mode.
///
/// `cluster_target` is what `CLUSTER` means to the calling subcommand: every node for per-node
/// figures, or one node per shard for data each node of a shard holds a copy of. `PRIMARY` and
/// `HASHTAG` are the same for every subcommand and reach primaries only, so a request always
/// reaches the same nodes.
pub(super) fn resolve_node_target(
    ctx: &Context,
    target: Option<NodeTarget>,
    cluster_target: FanoutTarget,
) -> ValkeyResult<Option<FanoutTarget>> {
    match target {
        None | Some(NodeTarget::Local) => Ok(None),
        Some(target) if !is_clustered(ctx) => Err(ValkeyError::String(format!(
            "TSDB: {} requires cluster mode",
            target.keyword()
        ))),
        Some(NodeTarget::Primary) => Ok(Some(FanoutTarget::Primary)),
        Some(NodeTarget::Cluster) => Ok(Some(cluster_target)),
        Some(NodeTarget::HashTag(tags)) => Ok(Some(FanoutTarget::HashTagsPrimary(tags))),
    }
}
