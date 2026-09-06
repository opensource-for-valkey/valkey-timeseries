use crate::fanout::FanoutTarget;
use std::sync::atomic::AtomicBool;
use valkey_module::{Context, ContextFlags, ValkeyResult};
pub(crate) const SLOT_SIZE: u16 = 16384;

const VALKEYMODULE_CLIENT_INFO_FLAG_READONLY: u64 = 1 << 6; /* Valkey 9 */

pub static FORCE_REPLICAS_READONLY: AtomicBool = AtomicBool::new(false);

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

pub fn compute_query_fanout_mode(context: &Context) -> FanoutTarget {
    #[cfg(test)]
    if FORCE_REPLICAS_READONLY.load(std::sync::atomic::Ordering::Relaxed) {
        // Testing only
        return FanoutTarget::ReplicasOnly;
    }

    if client_allows_replica_fanout(context) {
        FanoutTarget::Random
    } else {
        FanoutTarget::Primary
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
