use crate::fanout::FanoutTargetMode;
use std::sync::atomic::AtomicBool;
use valkey_module::{Context, ContextFlags, ValkeyResult, logging::log_warning};
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

/// Returns `true` when the current node owns the hash slot for `key` in cluster mode.
/// In non-cluster mode, always returns `true`.
pub fn key_belongs_to_local_node(ctx: &Context, key: &[u8]) -> bool {
    if !is_clustered(ctx) {
        return true;
    }
    let cluster_map = super::get_cluster_map();
    let current_shard = cluster_map.get_local_shard();
    let Some(current_shard) = current_shard else {
        // If we don't have a local shard, we can't own any slots, though this should not happen
        // in a healthy cluster setup.
        log_warning(
            "Cluster mode is enabled but no local shard found in cluster map. Assuming key does not belong to local node.",
        );
        return false;
    };
    current_shard.i_own_key(key)
}

/// Compute the Valkey/Redis hash slot (0..=16383) for a key byte slice.
///
/// Respects hash tags: if the key contains `{tag}` where the tag is non-empty,
/// only the bytes inside the braces are hashed.
pub fn key_hash_slot(key: &[u8]) -> u16 {
    let effective = if let Some(open) = key.iter().position(|&b| b == b'{') {
        let after_open = &key[open + 1..];
        if let Some(close) = after_open.iter().position(|&b| b == b'}') {
            let tag = &after_open[..close];
            if !tag.is_empty() { tag } else { key }
        } else {
            key
        }
    } else {
        key
    };
    crc16(effective) % 16384
}

/// CRC-16/CCITT-FALSE as used by Valkey/Redis for hash slot computation.
fn crc16(data: &[u8]) -> u16 {
    #[rustfmt::skip]
    const TABLE: [u16; 256] = [
        0x0000, 0x1021, 0x2042, 0x3063, 0x4084, 0x50a5, 0x60c6, 0x70e7,
        0x8108, 0x9129, 0xa14a, 0xb16b, 0xc18c, 0xd1ad, 0xe1ce, 0xf1ef,
        0x1231, 0x0210, 0x3273, 0x2252, 0x52b5, 0x4294, 0x72f7, 0x62d6,
        0x9339, 0x8318, 0xb37b, 0xa35a, 0xd3bd, 0xc39c, 0xf3ff, 0xe3de,
        0x2462, 0x3443, 0x0420, 0x1401, 0x64e6, 0x74c7, 0x44a4, 0x5485,
        0xa56a, 0xb54b, 0x8528, 0x9509, 0xe5ee, 0xf5cf, 0xc5ac, 0xd58d,
        0x3653, 0x2672, 0x1611, 0x0630, 0x76d7, 0x66f6, 0x5695, 0x46b4,
        0xb75b, 0xa77a, 0x9719, 0x8738, 0xf7df, 0xe7fe, 0xd79d, 0xc7bc,
        0x4864, 0x5845, 0x6826, 0x7807, 0x08e0, 0x18c1, 0x28a2, 0x3883,
        0xc96c, 0xd94d, 0xe92e, 0xf90f, 0x89e8, 0x99c9, 0xa9aa, 0xb98b,
        0x5a55, 0x4a74, 0x7a17, 0x6a36, 0x1ad1, 0x0af0, 0x3a93, 0x2ab2,
        0xdb5d, 0xcb7c, 0xfb1f, 0xeb3e, 0x9bd9, 0x8bf8, 0xbb9b, 0xabba,
        0x6c66, 0x7c47, 0x4c24, 0x5c05, 0x2ce2, 0x3cc3, 0x0ca0, 0x1c81,
        0xed6e, 0xfd4f, 0xcd2c, 0xdd0d, 0xadea, 0xbdcb, 0x8da8, 0x9d89,
        0x7e57, 0x6e76, 0x5e15, 0x4e34, 0x3ed3, 0x2ef2, 0x1e91, 0x0eb0,
        0xff5f, 0xef7e, 0xdf1d, 0xcf3c, 0xbffb, 0xafda, 0x9fb9, 0x8f98,
        0x9188, 0x81a9, 0xb1ca, 0xa1eb, 0xd10c, 0xc12d, 0xf14e, 0xe16f,
        0x1080, 0x00a1, 0x30c2, 0x20e3, 0x5004, 0x4025, 0x7046, 0x6067,
        0x83b9, 0x9398, 0xa3fb, 0xb3da, 0xc33d, 0xd31c, 0xe37f, 0xf35e,
        0x02b1, 0x1290, 0x22f3, 0x32d2, 0x4235, 0x5214, 0x6277, 0x7256,
        0xb5ea, 0xa5cb, 0x95a8, 0x8589, 0xf56e, 0xe54f, 0xd52c, 0xc50d,
        0x34e2, 0x24c3, 0x14a0, 0x0481, 0x7466, 0x6447, 0x5424, 0x4405,
        0xa7db, 0xb7fa, 0x8799, 0x97b8, 0xe75f, 0xf77e, 0xc71d, 0xd73c,
        0x26d3, 0x36f2, 0x0691, 0x16b0, 0x6657, 0x7676, 0x4615, 0x5634,
        0xd94c, 0xc96d, 0xf90e, 0xe92f, 0x99c8, 0x89e9, 0xb98a, 0xa9ab,
        0x5844, 0x4865, 0x7806, 0x6827, 0x18c0, 0x08e1, 0x3882, 0x28a3,
        0xcb7d, 0xdb5c, 0xeb3f, 0xfb1e, 0x8bf9, 0x9bd8, 0xabbb, 0xbb9a,
        0x4a75, 0x5a54, 0x6a37, 0x7a16, 0x0af1, 0x1ad0, 0x2ab3, 0x3a92,
        0xfd2e, 0xed0f, 0xdd6c, 0xcd4d, 0xbdaa, 0xad8b, 0x9de8, 0x8dc9,
        0x7c26, 0x6c07, 0x5c64, 0x4c45, 0x3ca2, 0x2c83, 0x1ce0, 0x0cc1,
        0xef1f, 0xff3e, 0xcf5d, 0xdf7c, 0xaf9b, 0xbfba, 0x8fd9, 0x9ff8,
        0x6e17, 0x7e36, 0x4e55, 0x5e74, 0x2e93, 0x3eb2, 0x0ed1, 0x1ef0,
    ];
    let mut crc: u16 = 0;
    for &byte in data {
        crc = (crc << 8) ^ TABLE[((crc >> 8) as u8 ^ byte) as usize];
    }
    crc
}

/// Helper function to check if the Valkey server version is considered "legacy" (e.g., < 9).
/// In legacy versions, client read-only status might not be reliably determinable.
fn is_valkey_version_legacy(context: &Context) -> bool {
    context
        .get_server_version()
        .is_ok_and(|version| version.major < 9)
}

pub fn compute_query_fanout_mode(context: &Context) -> FanoutTargetMode {
    #[cfg(test)]
    if FORCE_REPLICAS_READONLY.load(std::sync::atomic::Ordering::Relaxed) {
        // Testing only
        return FanoutTargetMode::ReplicasOnly;
    }

    // Determine fanout mode based on Valkey version and client read-only status.
    // The following logic is based on the issue https://github.com/valkey-io/valkey-search/issues/139
    if is_valkey_version_legacy(context) {
        // Valkey 8 doesn't provide a way to determine if a client is READONLY,
        // So we choose random distribution.
        FanoutTargetMode::Random
    } else {
        match is_client_read_only(context) {
            Ok(true) => FanoutTargetMode::Random,
            Ok(false) => FanoutTargetMode::Primary,
            Err(_) => {
                // If we can't determine client read-only status, default to Random
                crate::common::logging::log_warning(
                    "Could not determine client read-only status, defaulting to Random fanout mode.",
                );
                FanoutTargetMode::Random
            }
        }
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
