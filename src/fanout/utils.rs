use crate::fanout::FanoutTarget;
use valkey_module::{Context, ContextFlags, ValkeyResult};
pub(crate) const SLOT_SIZE: u16 = 16384;

const VALKEYMODULE_CLIENT_INFO_FLAG_READONLY: u64 = 1 << 6; /* Valkey 9 */

pub fn is_client_read_only(ctx: &Context) -> ValkeyResult<bool> {
    let info = ctx.get_client_info()?;
    Ok(info.flags & VALKEYMODULE_CLIENT_INFO_FLAG_READONLY != 0)
}

pub fn is_clustered(ctx: &Context) -> bool {
    let flags = ctx.get_flags();
    flags.contains(ContextFlags::CLUSTER)
}

pub fn is_multi_or_lua(ctx: &Context) -> bool {
    let flags = ctx.get_flags();
    flags.contains(ContextFlags::MULTI) || flags.contains(ContextFlags::LUA)
}

/// Helper function to check if the Valkey server version is considered "legacy" (e.g., < 9).
/// In legacy versions, client read-only status might not be reliably determinable.
fn is_valkey_version_legacy(context: &Context) -> bool {
    context
        .get_server_version()
        .is_ok_and(|version| version.major < 9)
}

/// Determines whether a query may fan out to replicas based on Valkey version and client
/// read-only status. The following logic is based on the issue
/// https://github.com/valkey-io/valkey-search/issues/139
///
/// Returns `true` if replicas may be targeted (client is READONLY, or its status can't be
/// determined), `false` if only primaries should be targeted.
pub fn client_allows_replica_fanout(context: &Context) -> bool {
    if is_valkey_version_legacy(context) {
        // Valkey 8 doesn't provide a way to determine if a client is READONLY,
        // So we choose random distribution.
        return true;
    }
    match is_client_read_only(context) {
        Ok(allowed) => allowed,
        Err(_) => {
            // If we can't determine client read-only status, default to Random
            crate::common::logging::log_warning(
                "Could not determine client read-only status, defaulting to Random fanout mode.",
            );
            true
        }
    }
}

pub fn compute_query_fanout_target(context: &Context) -> FanoutTarget {
    if client_allows_replica_fanout(context) {
        FanoutTarget::Random
    } else {
        FanoutTarget::Primary
    }
}

/// Target selection for a command scoped by an explicit `HASHTAG` clause.
///
/// This is the single implementation of the rule shared by the multi-series
/// commands (`TS.MRANGE`, `TS.MGET`, the metadata commands) and the PromQL
/// commands: no tags means the command's ordinary target selection, and tags
/// restrict the fanout to the shards owning them while preserving the
/// replica/primary read policy the client is entitled to.
pub fn compute_hash_tag_fanout_target(context: &Context, tags: &[String]) -> FanoutTarget {
    if tags.is_empty() {
        return compute_query_fanout_target(context);
    }
    hash_tag_fanout_target(tags, client_allows_replica_fanout(context))
}

/// The tag-scoped half of [`compute_hash_tag_fanout_target`], split out so the
/// replica/primary decision can be exercised without a live client context.
fn hash_tag_fanout_target(tags: &[String], allows_replicas: bool) -> FanoutTarget {
    if allows_replicas {
        FanoutTarget::HashTags(tags.to_vec())
    } else {
        FanoutTarget::HashTagsPrimary(tags.to_vec())
    }
}

/// Calculates the hash slot for a given key according to Valkey cluster specification.
///
/// The hash slot is computed as CRC16(key) % 16384, but only considering the substring
/// between curly braces {} if present (hash tags).
///
/// # Arguments
/// * `key` - The key string to compute the hash slot for
///
/// # Returns
/// * The hash slot number between 0 and 16383
pub fn calculate_hash_slot(key: &[u8]) -> u16 {
    let key = get_hash_tag(key).unwrap_or(key);
    slot(key)
}

pub(crate) fn slot(key: &[u8]) -> u16 {
    crc16::State::<crc16::XMODEM>::calculate(key) % SLOT_SIZE
}

/// Finds the hash tag boundaries in a key.
/// Returns (start_index_of_brace, end_index_of_brace)
/// If no valid hash tag is found, returns (key.len(), 0)
fn get_hash_tag(key: &[u8]) -> Option<&[u8]> {
    let open = key.iter().position(|v| *v == b'{')?;
    let close = key[open..].iter().position(|v| *v == b'}')?;

    let rv = &key[open + 1..open + close];
    (!rv.is_empty()).then_some(rv)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hash_tag_target_reads_replicas_when_the_client_allows_it() {
        let tags = vec!["tenant-a".to_string(), "tenant-b".to_string()];
        assert_eq!(
            hash_tag_fanout_target(&tags, true),
            FanoutTarget::HashTags(tags.clone())
        );
    }

    #[test]
    fn hash_tag_target_stays_on_primaries_when_replica_reads_are_not_allowed() {
        let tags = vec!["tenant-a".to_string()];
        assert_eq!(
            hash_tag_fanout_target(&tags, false),
            FanoutTarget::HashTagsPrimary(tags.clone())
        );
    }

    /// Braced and bare forms name the same slot, so a scoped command routes to
    /// the same shard either way.
    #[test]
    fn braced_and_bare_tags_select_the_same_slot() {
        assert_eq!(calculate_hash_slot(b"{tenant-a}"), slot(b"tenant-a"));
    }

    #[test]
    fn test_simple_key() {
        let slot = calculate_hash_slot(b"hello");
        // Expected slot can be verified against Redis/Valkey
        assert!(slot < 16384);
    }

    #[test]
    fn test_hash_tag() {
        // With hash tag, only "world" is used for hashing
        let slot1 = calculate_hash_slot(b"hello{world}foo");
        let slot2 = calculate_hash_slot(b"world");
        assert_eq!(slot1, slot2);
    }

    #[test]
    fn test_empty_hash_tag() {
        // Empty braces should be ignored
        let slot1 = calculate_hash_slot(b"hello{}world");
        let slot2 = calculate_hash_slot(b"hello{world");
        let slot3 = calculate_hash_slot(b"hello}world");

        // With invalid/empty hash tag, the whole key should be used.
        assert_eq!(slot1, slot(b"hello{}world"));
        assert_eq!(slot2, slot(b"hello{world"));
        assert_eq!(slot3, slot(b"hello}world"));
    }

    #[test]
    fn test_get_hashtag() {
        assert_eq!(get_hash_tag(&b"foo{bar}baz"[..]), Some(&b"bar"[..]));
        assert_eq!(get_hash_tag(&b"foo{}{baz}"[..]), None);
        assert_eq!(get_hash_tag(&b"foo{{bar}}zap"[..]), Some(&b"{bar"[..]));
    }
}
