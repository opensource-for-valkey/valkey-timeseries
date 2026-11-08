//! The cluster topology model: node ids, nodes, shards and their slot sets.

use rand::{Rng, RngExt};
use range_set_blaze::{RangeSetBlaze, RangesIter};
use std::borrow::Borrow;
use std::cmp::Ordering;
use std::fmt;
use std::fmt::{Display, Formatter};
use std::hash::{Hash, Hasher};
use std::net::IpAddr;
use std::ops::Deref;
use std::sync::LazyLock;
use valkey_module::{VALKEYMODULE_NODE_ID_LEN, ValkeyModule_GetMyClusterID};

/// Node role enumeration
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NodeRole {
    Primary,
    Replica,
}

impl Display for NodeRole {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            NodeRole::Primary => write!(f, "Primary"),
            NodeRole::Replica => write!(f, "Replica"),
        }
    }
}

/// Node location enumeration
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub enum NodeLocation {
    #[default]
    Local,
    Remote,
}

impl Display for NodeLocation {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            NodeLocation::Local => write!(f, "Local"),
            NodeLocation::Remote => write!(f, "Remote"),
        }
    }
}

/// Socket address for a node. Derived Eq/Hash so it can be used as a map key.
#[derive(Copy, Clone, Debug, Eq)]
pub struct SocketAddress {
    pub primary_endpoint: IpAddr,
    pub port: u16,
}

impl PartialEq for SocketAddress {
    fn eq(&self, other: &Self) -> bool {
        self.primary_endpoint == other.primary_endpoint && self.port == other.port
    }
}

impl Display for SocketAddress {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{}:{}", self.primary_endpoint, self.port)
    }
}

impl Hash for SocketAddress {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.primary_endpoint.hash(state);
        self.port.hash(state);
    }
}

#[derive(Debug, Default, Clone, Hash, PartialEq, Eq)]
pub struct SlotRangeSet {
    ranges: RangeSetBlaze<u16>,
}

impl SlotRangeSet {
    #[cfg(test)]
    pub fn len(&self) -> usize {
        self.ranges.len() as usize
    }

    #[cfg(test)]
    pub fn contains(&self, slot: u16) -> bool {
        self.ranges.contains(slot)
    }

    pub fn insert_range(&mut self, start: u16, end: u16) {
        debug_assert!(start <= end, "Invalid range: start ({start}) > end ({end})");
        self.ranges.ranges_insert(start..=end);
    }

    pub fn range_iter(&self) -> RangesIter<'_, u16> {
        self.ranges.ranges()
    }

    pub fn append(&mut self, other: &SlotRangeSet) {
        for range in other.range_iter() {
            self.ranges.ranges_insert(range.clone());
        }
    }
}

impl Display for SlotRangeSet {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        self.ranges.fmt(f)
    }
}

impl Deref for SlotRangeSet {
    type Target = RangeSetBlaze<u16>;

    fn deref(&self) -> &Self::Target {
        &self.ranges
    }
}

pub type NodeIdBuf = [u8; (VALKEYMODULE_NODE_ID_LEN as usize) + 1]; // +1 for null terminator

#[derive(Debug, Copy, Clone, Hash, PartialEq, Eq)]
pub struct NodeId(NodeIdBuf);

impl NodeId {
    /// Construct a [`NodeId`] from a raw `*const c_char` pointer returned by the
    /// Valkey C API (e.g., [`ValkeyModule_GetMyClusterID`], cluster-message
    /// sender IDs, or `CLUSTER SLOTS` reply fields).
    ///
    /// This is the **only** place in the codebase that converts a raw pointer
    /// into a [`NodeId`].  All other code should obtain [`NodeId`] values
    /// through this constructor or via [`NodeId::from_bytes`].
    ///
    /// # Null pointers
    ///
    /// If `node_id_ptr` is null, an empty (all-zero) [`NodeId`] is returned
    /// silently.  This is intentional: several call-sites receive possibly-null
    /// pointers from Valkey and rely on the resulting empty [`NodeId`] to
    /// represent "no node" without an additional null-check at every call-site.
    ///
    /// # Safety
    ///
    /// When `node_id_ptr` is **non-null**, the caller must ensure it points to
    /// a buffer of at least [`VALKEYMODULE_NODE_ID_LEN`] bytes (40) holding a
    /// hex-encoded, null-terminated node ID string, e.g.
    /// `"07c37dfeb235213a872192d90877d0cd55635b91"`.  The pointer is only read;
    /// the bytes are copied into an owned buffer and the pointer is not retained.
    ///
    /// This function is safe to call from any thread — it performs no
    /// synchronisation and only reads from the caller-provided pointer.
    pub unsafe fn from_raw(node_id_ptr: *const std::os::raw::c_char) -> Self {
        let mut buf: NodeIdBuf = [0; (VALKEYMODULE_NODE_ID_LEN as usize) + 1];

        if node_id_ptr.is_null() {
            return NodeId(buf);
        }

        // SAFETY: caller guarantees node_id_ptr is non-null and points to at
        // least VALKEYMODULE_NODE_ID_LEN bytes of initialized memory.
        let bytes = unsafe {
            std::slice::from_raw_parts(node_id_ptr as *const u8, VALKEYMODULE_NODE_ID_LEN as usize)
        };

        // In debug builds, verify the bytes look like a hex node ID.
        debug_assert!(
            bytes.iter().all(|b| b.is_ascii_hexdigit() || *b == 0),
            "NodeId::from_raw: non-null pointer contains non-hex bytes"
        );

        let len = bytes.len().min(VALKEYMODULE_NODE_ID_LEN as usize);
        buf[..len].copy_from_slice(&bytes[..len]);
        NodeId(buf)
    }

    pub(crate) fn from_bytes(bytes: &[u8]) -> Self {
        let mut buf: NodeIdBuf = [0; (VALKEYMODULE_NODE_ID_LEN as usize) + 1];
        let len = bytes.len().min(VALKEYMODULE_NODE_ID_LEN as usize);
        buf[..len].copy_from_slice(&bytes[..len]);
        NodeId(buf)
    }

    pub fn raw_ptr(&self) -> *const std::os::raw::c_char {
        if self.is_empty() {
            return std::ptr::null();
        }
        // SAFETY: self.0 is a null-terminated byte array
        self.0.as_ptr() as *const std::os::raw::c_char
    }

    /// The id as text; `""` for an empty id. Ids are hex, so the checked
    /// conversion only fails on bytes no well-behaved server sends, and those
    /// read as empty rather than as undefined behaviour.
    pub fn as_str(&self) -> &str {
        std::str::from_utf8(self.as_bytes()).unwrap_or("")
    }

    pub fn as_bytes(&self) -> &[u8] {
        if self.is_empty() {
            return &[];
        }
        &self.0[..VALKEYMODULE_NODE_ID_LEN as usize]
    }

    #[inline(always)]
    pub fn is_empty(&self) -> bool {
        self.0[0] == 0
    }
}

impl AsRef<str> for NodeId {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

impl AsRef<[u8]> for NodeId {
    fn as_ref(&self) -> &[u8] {
        self.as_bytes()
    }
}

impl PartialOrd for NodeId {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for NodeId {
    fn cmp(&self, other: &Self) -> Ordering {
        self.as_str().cmp(other.as_str())
    }
}

impl Display for NodeId {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

impl Borrow<str> for NodeId {
    fn borrow(&self) -> &str {
        self.as_ref()
    }
}

impl Default for NodeId {
    fn default() -> Self {
        NodeId([0; VALKEYMODULE_NODE_ID_LEN as usize + 1])
    }
}

/// Static buffer holding the current node's ID
pub static CURRENT_NODE_ID: LazyLock<NodeId> = LazyLock::new(|| {
    // SAFETY: the server returns null or its own 40-byte hex node id.
    unsafe {
        let node_id = ValkeyModule_GetMyClusterID
            .expect("ValkeyModule_GetMyClusterID function is unavailable")();
        NodeId::from_raw(node_id)
    }
});

/// Information about a cluster node
#[derive(Copy, Clone, PartialEq, Eq)]
pub struct NodeInfo {
    pub id: NodeId,
    pub shard_id: NodeId,
    pub socket_address: SocketAddress,
    pub role: NodeRole,
    pub location: NodeLocation,
    /// False when `CLUSTER NODES` flags the node `fail`/`fail?` or reports its
    /// bus link down. Target selection prefers healthy nodes within a shard but
    /// never drops a shard because none of its nodes is healthy.
    pub healthy: bool,
}

impl NodeInfo {
    pub fn address(&self) -> String {
        if self.location == NodeLocation::Local {
            String::new()
        } else {
            self.socket_address.to_string()
        }
    }

    pub fn is_local(&self) -> bool {
        self.location == NodeLocation::Local
    }
}

impl Hash for NodeInfo {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.id.hash(state);
    }
}

impl fmt::Debug for NodeInfo {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("NodeInfo")
            .field("id", &format_args!("{}", self.id))
            .field("shard_id", &format_args!("{}", self.shard_id))
            .field("socket_address", &self.socket_address)
            .field("role", &self.role)
            .field("location", &self.location)
            .field("healthy", &self.healthy)
            .finish()
    }
}

impl Borrow<NodeId> for NodeInfo {
    fn borrow(&self) -> &NodeId {
        &self.id
    }
}

/// Information about a shard in the cluster.
///
/// Equality and ordering are by `id` alone, so a `BTreeSet<ShardInfo>` can be
/// searched by id; [`ShardInfo::same_content`] compares everything else.
#[derive(Debug, Default, Clone)]
pub struct ShardInfo {
    /// id is the primary node id
    pub id: NodeId,
    /// the primary node can be None
    pub primary: Option<NodeInfo>,
    /// Ordered by node id, so equal membership compares equal.
    pub replicas: Vec<NodeInfo>,
    pub owned_slots: SlotRangeSet,
    /// Whether this shard is local
    pub is_local: bool,
}

impl ShardInfo {
    /// Same shard, nodes (with addresses, roles and health) and slots.
    pub(super) fn same_content(&self, other: &ShardInfo) -> bool {
        self.id == other.id
            && self.primary == other.primary
            && self.replicas == other.replicas
            && self.owned_slots == other.owned_slots
            && self.is_local == other.is_local
    }

    /// Pick one node to serve the shard, uniformly among its healthy nodes.
    ///
    /// With `prefer_replica`, a healthy replica wins over the primary when one
    /// exists. When no node is healthy the primary (or, lacking one, a replica)
    /// is still returned, so the fanout fails on the dead node rather than
    /// silently omitting the shard's data. `None` only for a shard with no nodes.
    pub(super) fn pick_target<R: Rng + ?Sized>(
        &self,
        rng: &mut R,
        prefer_replica: bool,
    ) -> Option<NodeInfo> {
        fn pick_healthy<'a, R: Rng + ?Sized>(
            rng: &mut R,
            nodes: impl Iterator<Item = &'a NodeInfo> + Clone,
        ) -> Option<NodeInfo> {
            let healthy = nodes.filter(|node| node.healthy);
            let count = healthy.clone().count();
            if count == 0 {
                return None;
            }
            healthy.clone().nth(rng.random_range(0..count)).copied()
        }

        if prefer_replica && let Some(replica) = pick_healthy(rng, self.replicas.iter()) {
            return Some(replica);
        }
        pick_healthy(rng, self.primary.iter().chain(self.replicas.iter()))
            .or(self.primary)
            .or_else(|| self.replicas.first().copied())
    }
}

impl PartialEq for ShardInfo {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}

impl Eq for ShardInfo {}

impl PartialOrd<ShardInfo> for ShardInfo {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for ShardInfo {
    fn cmp(&self, other: &Self) -> Ordering {
        self.id.cmp(&other.id)
    }
}

impl Borrow<NodeId> for ShardInfo {
    fn borrow(&self) -> &NodeId {
        &self.id
    }
}

impl Borrow<str> for ShardInfo {
    fn borrow(&self) -> &str {
        self.id.as_ref()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const M1: &str = "1111111111111111111111111111111111111111";

    fn id(s: &str) -> NodeId {
        NodeId::from_bytes(s.as_bytes())
    }

    #[test]
    fn test_empty_node_id_accessors_agree() {
        let empty = NodeId::default();
        assert_eq!(empty.as_str(), "");
        assert_eq!(AsRef::<str>::as_ref(&empty), "");
        assert!(empty.as_bytes().is_empty());
        assert!(AsRef::<[u8]>::as_ref(&empty).is_empty());
        assert_eq!(id(M1).as_str(), M1);
        assert_eq!(AsRef::<str>::as_ref(&id(M1)), M1);
    }
}
