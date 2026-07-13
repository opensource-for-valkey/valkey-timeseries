use crate::common::time::current_time_millis;
use crate::config::CLUSTER_MAP_EXPIRATION_MS;
use crate::fanout::calculate_hash_slot;
use ahash::{AHashMap, HashSet, HashSetExt};
use rand::{Rng, RngExt, rng};
use range_set_blaze::{RangeMapBlaze, RangeSetBlaze, RangesIter};
use smallvec::SmallVec;
use std::borrow::Borrow;
use std::cmp::Ordering;
use std::collections::BTreeSet;
use std::fmt;
use std::fmt::{Display, Formatter};
use std::hash::{DefaultHasher, Hash, Hasher};
use std::net::{IpAddr, Ipv4Addr};
use std::ops::Deref;
use std::sync::atomic::{AtomicI64, Ordering as AtomicOrdering};
use std::sync::{Arc, LazyLock, OnceLock};
use valkey_module::logging::{log_notice, log_warning};
use valkey_module::{
    CallOptionsBuilder, CallReply, CallResult, Context, VALKEYMODULE_NODE_ID_LEN,
    ValkeyModule_GetMyClusterID,
};

// Constants
pub const NUM_SLOTS: u16 = 16384;

/// Enumeration for fanout target modes
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub enum FanoutTargetMode {
    /// Default: randomly select one node per shard
    #[default]
    Random,
    /// Select only replicas, one per shard
    ReplicasOnly,
    /// Select one replica per shard (if available), otherwise primary
    OneReplicaPerShard,
    /// Select all primary (master) nodes
    Primary,
    /// Select all nodes (both primary and replica)
    All,
}

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
    pub fn new() -> Self {
        Self {
            ranges: RangeSetBlaze::new(),
        }
    }

    pub fn len(&self) -> usize {
        self.ranges.len() as usize
    }

    pub fn is_empty(&self) -> bool {
        self.ranges.is_empty()
    }

    pub fn contains(&self, slot: u16) -> bool {
        self.ranges.contains(slot)
    }

    pub fn insert(&mut self, slot: u16) {
        self.ranges.insert(slot);
    }

    pub fn insert_range(&mut self, start: u16, end: u16) {
        debug_assert!(start <= end, "Invalid range: start ({start}) > end ({end})");
        self.ranges.ranges_insert(start..=end);
    }

    pub fn iter(&self) -> impl Iterator<Item = u16> + '_ {
        self.ranges.iter()
    }

    pub fn range_iter(&self) -> RangesIter<'_, u16> {
        self.ranges.ranges()
    }

    pub fn append(&mut self, other: &SlotRangeSet) {
        for range in other.range_iter() {
            self.ranges.ranges_insert(range.clone());
        }
    }

    pub fn first(&self) -> Option<u16> {
        self.ranges.first()
    }

    pub fn last(&self) -> Option<u16> {
        self.ranges.last()
    }

    /// Helper method to calculate slot fingerprint
    fn calculate_fingerprint(&self) -> u64 {
        let mut hasher = DefaultHasher::new();
        self.hash(&mut hasher);
        hasher.finish()
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
    pub fn from_raw(node_id_ptr: *const std::os::raw::c_char) -> Self {
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

    pub(super) fn from_bytes(bytes: &[u8]) -> Self {
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

    pub fn as_str(&self) -> &str {
        if self.is_empty() {
            return "";
        }
        // SAFETY: self.0 is always valid UTF-8 as it is copied from valid node ID strings
        let buf = &self.0[..VALKEYMODULE_NODE_ID_LEN as usize];
        debug_assert!(
            std::str::from_utf8(buf).is_ok(),
            "NodeId::as_str: node id bytes are not valid UTF-8"
        );
        unsafe { std::str::from_utf8_unchecked(buf) }
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

    pub fn len(&self) -> usize {
        if self.is_empty() {
            0
        } else {
            VALKEYMODULE_NODE_ID_LEN as usize
        }
    }
}

impl AsRef<str> for NodeId {
    fn as_ref(&self) -> &str {
        // SAFETY: self.0 is always valid UTF-8 as it is copied from valid node ID strings
        let buf = &self.0[..VALKEYMODULE_NODE_ID_LEN as usize];
        debug_assert!(
            std::str::from_utf8(buf).is_ok(),
            "NodeId::as_ref: node id bytes are not valid UTF-8"
        );
        unsafe { std::str::from_utf8_unchecked(buf) }
    }
}

impl AsRef<[u8]> for NodeId {
    fn as_ref(&self) -> &[u8] {
        &self.0[..VALKEYMODULE_NODE_ID_LEN as usize]
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
    let node_id = unsafe {
        ValkeyModule_GetMyClusterID.expect("ValkeyModule_GetMyClusterID function is unavailable")()
    };
    NodeId::from_raw(node_id)
});

/// Information about a cluster node
#[derive(Copy, Clone, PartialEq, Eq)]
pub struct NodeInfo {
    pub id: NodeId,
    pub shard_id: NodeId,
    pub socket_address: SocketAddress,
    pub role: NodeRole,
    pub location: NodeLocation,
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

    pub fn is_primary(&self) -> bool {
        self.role == NodeRole::Primary
    }
}

impl PartialOrd for NodeInfo {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for NodeInfo {
    fn cmp(&self, other: &Self) -> Ordering {
        self.id.cmp(&other.id)
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
            .finish()
    }
}

impl Borrow<NodeId> for NodeInfo {
    fn borrow(&self) -> &NodeId {
        &self.id
    }
}

/// Information about a shard in the cluster
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct ShardInfo {
    /// id is the primary node id
    pub id: NodeId,
    /// the primary node can be None
    pub primary: Option<NodeInfo>,
    pub replicas: Vec<NodeInfo>,
    pub owned_slots: SlotRangeSet,
    /// Whether this shard is local
    pub is_local: bool,
    /// Hash of an owned_slots set
    pub slots_fingerprint: u64,
    pub is_consistent: bool,
}

impl ShardInfo {
    /// Pick a node from the shard.
    /// - replica_only: pick only from replicas (must exist)
    /// - prefer_replica: pick a replica if available, else primary
    fn pick_target<R: Rng + ?Sized>(
        &self,
        rng: &mut R,
        replica_only: bool,
        prefer_replica: bool,
    ) -> NodeInfo {
        if replica_only {
            debug_assert!(
                !self.replicas.is_empty(),
                "Shard has no replicas to select from in replica-only mode"
            );
            let idx = rng.random_range(0..self.replicas.len());
            return self.replicas[idx];
        }

        if prefer_replica && !self.replicas.is_empty() {
            let idx = rng.random_range(0..self.replicas.len());
            return self.replicas[idx];
        }

        if let Some(primary) = self.primary {
            if self.replicas.is_empty() {
                return primary;
            }
            // random between primary + replicas
            let idx = rng.random_range(0..(self.replicas.len() + 1));
            if idx == 0 {
                primary
            } else {
                self.replicas[idx - 1]
            }
        } else {
            assert!(
                !self.replicas.is_empty(),
                "Shard has no nodes to select from"
            );
            let idx = rng.random_range(0..self.replicas.len());
            self.replicas[idx]
        }
    }

    pub fn get_random_replica(&self) -> NodeInfo {
        let mut rng_ = rng();
        self.pick_target(&mut rng_, true, false)
    }

    pub fn is_empty(&self) -> bool {
        self.primary.is_none() && self.replicas.is_empty()
    }

    /// Slot ownership checks
    pub fn i_own_slot(&self, slot: u16) -> bool {
        self.owned_slots.contains(slot)
    }

    pub fn i_own_key(&self, key: &[u8]) -> bool {
        let slot = calculate_hash_slot(key);
        self.i_own_slot(slot)
    }
}

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

/// Helper describing a parsed slot range before building the slot -> shard map.
#[derive(Copy, Clone)]
struct SlotRangeInfo {
    pub start_slot: u16,
    pub end_slot: u16,
    pub shard_id: NodeId,
}

type LazyTargets = OnceLock<Arc<HashSet<NodeInfo>>>;

/// Main cluster map structure
#[derive(Debug, Default)]
pub struct ClusterMap {
    /// The id of the local shard (for easy access without dereferencing CURRENT_NODE_ID all the time)
    local_node_id: NodeId,
    /// Slot set for slots owned by the cluster
    owned_slots: SlotRangeSet,
    shards: BTreeSet<ShardInfo>,
    /// A range map, mapping(start slot, end slot) => shard id
    slot_to_shard_map: RangeMapBlaze<u16, NodeId>,
    /// Cluster-level fingerprint (hash of all shard fingerprints)
    cluster_slots_fingerprint: u64,
    /// (Lazily) pre-computed target lists
    primary_targets: LazyTargets,
    replica_targets: LazyTargets,
    all_targets: LazyTargets,
    /// Expiration timestamp in ms (since epoch). Atomic so a refresh that
    /// finds the topology unchanged can extend the published map's lifetime
    /// in place instead of swapping in an identical copy.
    expiration_ts: AtomicI64,
    /// is the current map consistent (no collisions/inconsistencies found while building)
    pub is_consistent: bool,
    /// Whether the cluster map covers all slots consecutively
    pub is_cluster_map_full: bool,
}

impl ClusterMap {
    /// Slot ownership checks
    pub fn i_own_slot(&self, slot: u16) -> bool {
        self.owned_slots.contains(slot)
    }

    pub fn is_owned_key(&self, key: &[u8]) -> bool {
        let slot = calculate_hash_slot(key);
        self.i_own_slot(slot)
    }

    /// Get the count of owned slots
    pub fn owned_slot_count(&self) -> usize {
        self.owned_slots.len()
    }

    /// Look up a shard by id. Will return None if shard does not exist
    pub fn get_shard_by_id(&self, shard_id: &str) -> Option<&ShardInfo> {
        self.shards.get(shard_id)
    }

    pub fn get_shard_by_slot(&self, slot: u16) -> Option<&ShardInfo> {
        let shard_id = self.slot_to_shard_map.get(slot)?;
        self.shards.get(shard_id)
    }

    /// Get all shards
    pub fn all_shards(&self) -> &BTreeSet<ShardInfo> {
        &self.shards
    }

    /// Get the local shard — the shard whose slots are owned by the current node.
    /// Returns `None` if no local shard is registered (e.g. the cluster map
    /// hasn't been built yet or the node doesn't own any slots).
    pub fn get_local_shard(&self) -> Option<&ShardInfo> {
        self.shards.iter().find(|&shard| shard.is_local)
    }

    /// Get cluster level slot fingerprint
    pub fn cluster_slots_fingerprint(&self) -> u64 {
        self.cluster_slots_fingerprint
    }

    /// Helper function to refresh targets in CreateNewClusterMap
    pub fn get_targets(&self, target_mode: FanoutTargetMode) -> Arc<HashSet<NodeInfo>> {
        match target_mode {
            FanoutTargetMode::Primary => self.primary_targets(),
            FanoutTargetMode::ReplicasOnly => self.replica_targets(),
            FanoutTargetMode::All => self.all_targets(),
            FanoutTargetMode::OneReplicaPerShard => self.random_one_replica_per_shard(),
            FanoutTargetMode::Random => self.random_one_per_shard(),
        }
    }

    fn random_one_per_shard(&self) -> Arc<HashSet<NodeInfo>> {
        let mut rng_ = rng();
        let mut targets = HashSet::new();
        for shard in self.shards.iter().filter(|shard| !shard.is_empty()) {
            targets.insert(shard.pick_target(&mut rng_, false, false));
        }
        Arc::new(targets)
    }

    fn random_one_replica_per_shard(&self) -> Arc<HashSet<NodeInfo>> {
        let mut rng_ = rng();
        let mut targets = HashSet::new();
        for shard in self.shards.iter().filter(|shard| !shard.is_empty()) {
            // prefer a replica, fall back to primary if no replicas exist
            targets.insert(shard.pick_target(&mut rng_, false, true));
        }
        Arc::new(targets)
    }

    #[inline]
    fn all_targets(&self) -> Arc<HashSet<NodeInfo>> {
        self.all_targets
            .get_or_init(|| {
                let mut targets = HashSet::new();
                for shard in self.shards.iter() {
                    if let Some(primary) = shard.primary {
                        targets.insert(primary);
                    }
                    for replica in shard.replicas.iter() {
                        targets.insert(*replica);
                    }
                }
                Arc::new(targets)
            })
            .clone()
    }

    #[inline]
    fn primary_targets(&self) -> Arc<HashSet<NodeInfo>> {
        self.primary_targets
            .get_or_init(|| {
                let mut targets = HashSet::new();
                for shard in self.shards.iter() {
                    if let Some(primary) = shard.primary {
                        targets.insert(primary);
                    }
                }
                Arc::new(targets)
            })
            .clone()
    }

    #[inline]
    fn replica_targets(&self) -> Arc<HashSet<NodeInfo>> {
        self.replica_targets
            .get_or_init(|| {
                let mut targets = HashSet::new();
                for shard in self.shards.iter() {
                    for replica in shard.replicas.iter() {
                        targets.insert(*replica);
                    }
                }
                Arc::new(targets)
            })
            .clone()
    }

    /// Build a new ClusterMap from `CLUSTER NODES`.
    ///
    /// `CLUSTER NODES` is used in preference to `CLUSTER SLOTS` because its reply
    /// is generated from the node's own gossip view and does not consult the
    /// calling client's connection. `CLUSTER SLOTS` resolves each node's
    /// preferred endpoint against the current client, which crashes the server
    /// when invoked without a real client context (e.g. from a background
    /// thread or the cluster-message callback). Parsing `CLUSTER NODES` lets us
    /// refresh the map safely from any context that holds the module lock.
    /// Returns `None` when the `CLUSTER NODES` call fails to produce a usable
    /// reply, so callers can distinguish a build failure from a successfully
    /// built (possibly inconsistent) map and avoid clobbering a good map.
    pub fn create(ctx: &Context) -> Option<Self> {
        let call_options = CallOptionsBuilder::new().errors_as_replies().build();

        log_notice("Calling CLUSTER NODES...");

        let res: CallResult = ctx.call_ext::<_, CallResult>("CLUSTER", &call_options, &["NODES"]);

        let Some(text) = reply_as_string(res) else {
            log_warning("CLUSTER NODES did not return a usable string reply");
            return None;
        };

        let my_node_id = *CURRENT_NODE_ID;
        debug_assert!(!my_node_id.is_empty(), "Current node id is empty");

        Some(Self::from_cluster_nodes(&text, &my_node_id))
    }

    /// Build a ClusterMap from the raw text of a `CLUSTER NODES` reply.
    ///
    /// Pure and self-contained so it can be unit-tested without a running
    /// server. Each line has the form:
    /// `<id> <ip:port@cport[,aux...]> <flags> <master> <ping> <pong> <epoch> <link> <slot...>`
    pub(super) fn from_cluster_nodes(text: &str, my_node_id: &NodeId) -> Self {
        let mut new_map = ClusterMap {
            is_consistent: true,
            ..Default::default()
        };

        // Parse every line first so masters and replicas can be assembled in
        // two passes regardless of the order they appear. One record per line,
        // so sizing from the line count avoids growth reallocations.
        let mut records: Vec<ParsedNodeLine> = Vec::with_capacity(text.lines().count());
        for line in text.lines() {
            let line = line.trim();
            if line.is_empty() {
                continue;
            }
            match parse_node_line(line, my_node_id) {
                Some(rec) => records.push(rec),
                None => {
                    log_warning("cluster-map: dropping unparseable CLUSTER NODES line");
                    new_map.is_consistent = false;
                }
            }
        }

        // Detect socket-address collisions across distinct node ids.
        let mut socket_addr_to_node: AHashMap<SocketAddress, NodeId> =
            AHashMap::with_capacity(records.len());
        for rec in records.iter() {
            let Some(addr) = rec.socket_address else {
                continue;
            };
            match socket_addr_to_node.get(&addr) {
                Some(existing) if *existing != rec.id => {
                    let msg = format!("Socket address collision at {addr} for node {}", rec.id);
                    log_warning(&msg);
                    new_map.is_consistent = false;
                }
                Some(_) => {}
                None => {
                    socket_addr_to_node.insert(addr, rec.id);
                }
            }
        }

        let mut shard_map: AHashMap<NodeId, ShardInfo> = AHashMap::with_capacity(records.len());
        let mut slot_ranges_parsed: Vec<SlotRangeInfo> = Vec::new();
        // The shard id (master node id) whose primary or a replica is the local node.
        let mut local_shard_id: Option<NodeId> = None;

        // Pass 1: masters become shards.
        for rec in records.iter() {
            if !rec.is_master {
                continue;
            }
            let Some(socket_address) = rec.socket_address else {
                // A master with no usable address cannot serve; if it owns slots
                // the map is not trustworthy.
                if !rec.slot_ranges.is_empty() {
                    new_map.is_consistent = false;
                }
                continue;
            };

            let primary = NodeInfo {
                id: rec.id,
                shard_id: rec.id,
                socket_address,
                role: NodeRole::Primary,
                location: rec.location(),
            };

            let shard = shard_map.entry(rec.id).or_insert_with(|| ShardInfo {
                id: rec.id,
                ..Default::default()
            });
            shard.primary = Some(primary);
            if rec.is_local {
                shard.is_local = true;
                local_shard_id = Some(rec.id);
            }

            for &(start, end) in rec.slot_ranges.iter() {
                if start > end || end >= NUM_SLOTS {
                    new_map.is_consistent = false;
                    continue;
                }
                shard.owned_slots.insert_range(start, end);
                slot_ranges_parsed.push(SlotRangeInfo {
                    start_slot: start,
                    end_slot: end,
                    shard_id: rec.id,
                });
            }
        }

        // Pass 2: attach replicas to their master's shard.
        for rec in records.iter() {
            if rec.is_master || !rec.has_master {
                continue;
            }
            let Some(socket_address) = rec.socket_address else {
                continue;
            };
            let Some(shard) = shard_map.get_mut(&rec.master_id) else {
                // Replica whose master is not (yet) known; view is inconsistent.
                new_map.is_consistent = false;
                continue;
            };
            shard.replicas.push(NodeInfo {
                id: rec.id,
                shard_id: shard.id,
                socket_address,
                role: NodeRole::Replica,
                location: rec.location(),
            });
            if rec.is_local {
                shard.is_local = true;
                local_shard_id = Some(rec.master_id);
            }
        }

        // The local node owns (serves) the slots of its shard.
        if let Some(shard_id) = local_shard_id
            && let Some(shard) = shard_map.get(&shard_id)
        {
            new_map.owned_slots.append(&shard.owned_slots);
        }

        // Finalize shards: back-pointers and per-shard fingerprints.
        let mut shard_set: BTreeSet<ShardInfo> = BTreeSet::new();
        for (_, mut shard) in shard_map.into_iter() {
            if let Some(primary) = shard.primary.as_mut() {
                primary.shard_id = shard.id;
            }
            for replica in shard.replicas.iter_mut() {
                replica.shard_id = shard.id;
            }
            shard.slots_fingerprint = shard.owned_slots.calculate_fingerprint();
            shard_set.insert(shard);
        }
        new_map.shards = shard_set;

        // Build slot-to-shard map
        new_map.build_slot_to_shard_map(&slot_ranges_parsed);
        new_map.local_node_id = *my_node_id;

        // Check if a cluster map is full
        new_map.is_consistent &= new_map.check_cluster_map_full();

        // Compute cluster-level fingerprint
        new_map.cluster_slots_fingerprint = new_map.compute_cluster_fingerprint();

        // Set expiration time
        new_map.extend_expiration(CLUSTER_MAP_EXPIRATION_MS.load(AtomicOrdering::Relaxed));

        new_map
    }

    /// Convenience: is the cluster map expired?
    pub fn is_expired(&self) -> bool {
        current_time_millis() >= self.expiration_ts.load(AtomicOrdering::Relaxed)
    }

    /// Push the expiration `ttl_ms` forward from now. Called when a rebuild
    /// confirmed the topology is unchanged, so the published map can be kept
    /// (along with its lazily-computed target caches) instead of being
    /// replaced. The caller chooses the TTL: the configured expiration for a
    /// freshly built map, or the adaptive backoff interval when extending.
    pub fn extend_expiration(&self, ttl_ms: u64) {
        self.expiration_ts.store(
            current_time_millis() + ttl_ms as i64,
            AtomicOrdering::Relaxed,
        );
    }

    /// True when `other` describes the same topology: same shards with the
    /// same primaries, replicas, addresses, roles, and slot assignments.
    /// `slot_to_shard_map` and `owned_slots` are derived from the shard set,
    /// so comparing the shards covers them.
    pub fn same_topology(&self, other: &ClusterMap) -> bool {
        self.shards == other.shards
    }

    fn check_cluster_map_full(&self) -> bool {
        // Ensure the first start is 0
        if self.slot_to_shard_map.is_empty() {
            return false;
        }

        let mut expected_next: u16 = 0;
        for range in self.slot_to_shard_map.ranges() {
            let start_slot = *range.start();
            let end_slot = *range.end();
            if expected_next > 0 {
                debug_assert!(
                    start_slot >= expected_next,
                    "Slot {start_slot} overlaps with previous range ending at {}",
                    expected_next - 1
                );
            }
            if start_slot != expected_next {
                let msg = format!(
                    "Slot gap found: slots {expected_next} to {} are not covered",
                    start_slot - 1
                );
                log_warning(&msg);
                return false;
            }
            expected_next = end_slot.wrapping_add(1);
        }
        expected_next == NUM_SLOTS
    }

    fn build_slot_to_shard_map(&mut self, slot_ranges: &[SlotRangeInfo]) {
        for r in slot_ranges {
            // sanity: shard must exist
            assert!(
                self.shards.contains(&r.shard_id),
                "Shard not found when building slot map"
            );
            self.slot_to_shard_map
                .ranges_insert(r.start_slot..=r.end_slot, r.shard_id);
        }
    }

    fn compute_cluster_fingerprint(&self) -> u64 {
        let mut hasher = DefaultHasher::new();
        for shard in self.shards.iter() {
            hasher.write(shard.id.as_bytes());
            shard.slots_fingerprint.hash(&mut hasher);
        }
        hasher.finish()
    }
}

fn reply_as_string(call_reply: CallResult) -> Option<String> {
    match call_reply {
        Ok(CallReply::String(v)) => v.to_string(),
        Ok(CallReply::VerbatimString(v)) => v
            .to_parts()
            .map(|(_, bytes)| String::from_utf8_lossy(&bytes).into_owned()),
        _ => None,
    }
}

/// Inline-capacity buffer for a master's slot ranges: a settled cluster has
/// one contiguous range per shard, so four covers all but heavily fragmented
/// topologies without a heap allocation per line.
type SlotRangeBuf = SmallVec<[(u16, u16); 4]>;

/// A single parsed line of `CLUSTER NODES` output.
struct ParsedNodeLine {
    id: NodeId,
    /// `None` when the node has no usable address (dropped for remote nodes).
    socket_address: Option<SocketAddress>,
    is_master: bool,
    is_local: bool,
    /// True when this replica has a resolvable master id.
    has_master: bool,
    master_id: NodeId,
    /// Owned slot ranges (masters only); import/export markers are excluded.
    slot_ranges: SlotRangeBuf,
}

impl ParsedNodeLine {
    fn location(&self) -> NodeLocation {
        if self.is_local {
            NodeLocation::Local
        } else {
            NodeLocation::Remote
        }
    }
}

/// Parse one `CLUSTER NODES` line:
/// `<id> <ip:port@cport[,aux]> <flags> <master> <ping> <pong> <epoch> <link> [slots...]`
///
/// Returns `None` when the line is too malformed to trust (too few fields,
/// unparseable node id, or no single clear role).
fn parse_node_line(line: &str, my_node_id: &NodeId) -> Option<ParsedNodeLine> {
    // Walk the fields without collecting them: only the first four and the
    // trailing slot specs carry information.
    let mut fields = line.split_ascii_whitespace();
    let id_field = fields.next()?;
    let addr_field = fields.next()?;
    let flags_field = fields.next()?;
    let master_field = fields.next()?;
    // Skip ping-sent/pong-recv/config-epoch and require link-state, so a line
    // with fewer than 8 fields is rejected just as the indexed form did.
    let mut fields = fields.skip(3);
    fields.next()?;

    if id_field.len() != VALKEYMODULE_NODE_ID_LEN as usize {
        return None;
    }
    let id = NodeId::from_bytes(id_field.as_bytes());

    let mut is_master = false;
    let mut is_replica = false;
    let mut is_myself = false;
    let mut noaddr = false;
    for flag in flags_field.split(',') {
        match flag {
            "master" => is_master = true,
            "slave" | "replica" => is_replica = true,
            "myself" => is_myself = true,
            "noaddr" => noaddr = true,
            _ => {}
        }
    }
    // Exactly one role must be declared (rejects handshake/unknown lines).
    if is_master == is_replica {
        return None;
    }
    let is_local = is_myself || &id == my_node_id;

    let socket_address = parse_node_address(addr_field, noaddr, is_local);

    let has_master = is_replica && master_field.len() == VALKEYMODULE_NODE_ID_LEN as usize;
    let master_id = if has_master {
        NodeId::from_bytes(master_field.as_bytes())
    } else {
        NodeId::default()
    };

    let mut slot_ranges = SlotRangeBuf::new();
    if is_master {
        for spec in fields {
            // Skip importing/migrating markers like "[12000-<-<id>]".
            if spec.starts_with('[') {
                continue;
            }
            if let Some((a, b)) = spec.split_once('-') {
                if let (Ok(a), Ok(b)) = (a.parse::<u16>(), b.parse::<u16>()) {
                    slot_ranges.push((a, b));
                }
            } else if let Ok(n) = spec.parse::<u16>() {
                slot_ranges.push((n, n));
            }
        }
    }

    Some(ParsedNodeLine {
        id,
        socket_address,
        is_master,
        is_local,
        has_master,
        master_id,
        slot_ranges,
    })
}

/// Parse the `ip:port@cport[,aux...]` address field.
///
/// Returns `None` for a remote node with no usable address (it can't be
/// targeted). The local node keeps a placeholder address instead, since a
/// node never dials itself but must still appear in its own map.
fn parse_node_address(field: &str, noaddr: bool, is_local: bool) -> Option<SocketAddress> {
    // Drop the optional ",hostname"/aux fields, then the "@cport" bus suffix.
    let addr = field.split(',').next().unwrap_or(field);
    let host_port = addr.split('@').next().unwrap_or(addr);
    // Split at the last ':' so IPv6 addresses (which contain ':') parse.
    let (ip_str, port) = match host_port.rsplit_once(':') {
        Some((ip, port)) => (ip, port.parse::<u16>().unwrap_or(0)),
        None => (host_port, 0),
    };

    let local_placeholder = || {
        is_local.then_some(SocketAddress {
            primary_endpoint: IpAddr::V4(Ipv4Addr::UNSPECIFIED),
            port,
        })
    };

    if noaddr || ip_str.is_empty() {
        return local_placeholder();
    }
    match ip_str.parse::<IpAddr>() {
        Ok(primary_endpoint) => Some(SocketAddress {
            primary_endpoint,
            port,
        }),
        Err(_) => local_placeholder(),
    }
}

// Unit tests
#[cfg(test)]
mod tests {
    use super::*;

    // 40-char hex node ids.
    const M1: &str = "1111111111111111111111111111111111111111";
    const M2: &str = "2222222222222222222222222222222222222222";
    const M3: &str = "3333333333333333333333333333333333333333";
    const R1: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    const R2: &str = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
    const R3: &str = "cccccccccccccccccccccccccccccccccccccccc";

    fn id(s: &str) -> NodeId {
        NodeId::from_bytes(s.as_bytes())
    }

    /// A 3-master / 3-replica cluster, `myself` on master M1.
    fn three_shard_nodes() -> String {
        format!(
            "\
{M1} 127.0.0.1:7001@17001 myself,master - 0 0 1 connected 0-5460\n\
{R1} 127.0.0.1:7101@17101 slave {M1} 0 0 1 connected\n\
{M2} 127.0.0.1:7002@17002 master - 0 0 2 connected 5461-10922\n\
{R2} 127.0.0.1:7102@17102 slave {M2} 0 0 2 connected\n\
{M3} 127.0.0.1:7003@17003 master - 0 0 3 connected 10923-16383\n\
{R3} 127.0.0.1:7103@17103 slave {M3} 0 0 3 connected\n"
        )
    }

    #[test]
    fn test_parse_three_shards() {
        let map = ClusterMap::from_cluster_nodes(&three_shard_nodes(), &id(M1));

        assert!(map.is_consistent);
        assert_eq!(map.all_shards().len(), 3);

        // M1 is local and owns 0-5460.
        assert!(map.i_own_slot(0));
        assert!(map.i_own_slot(5460));
        assert!(!map.i_own_slot(5461));
        assert_eq!(map.owned_slot_count(), 5461);

        let shard1 = map.get_shard_by_id(M1).unwrap();
        assert!(shard1.is_local);
        assert_eq!(shard1.replicas.len(), 1);
        assert_eq!(shard1.replicas[0].id, id(R1));
        assert_eq!(shard1.primary.unwrap().location, NodeLocation::Local);

        // Slot -> shard routing across shards.
        assert_eq!(map.get_shard_by_slot(0).unwrap().id, id(M1));
        assert_eq!(map.get_shard_by_slot(6000).unwrap().id, id(M2));
        assert_eq!(map.get_shard_by_slot(16383).unwrap().id, id(M3));

        // Replicas belong to their master and are marked remote.
        let shard2 = map.get_shard_by_id(M2).unwrap();
        assert_eq!(shard2.replicas[0].id, id(R2));
        assert_eq!(shard2.replicas[0].location, NodeLocation::Remote);
        assert_eq!(shard2.replicas[0].shard_id, id(M2));
    }

    #[test]
    fn test_fingerprint_independent_of_line_order() {
        let a = ClusterMap::from_cluster_nodes(&three_shard_nodes(), &id(M1));

        // Same topology, lines shuffled and replicas listed before masters.
        let shuffled = format!(
            "\
{R3} 127.0.0.1:7103@17103 slave {M3} 0 0 3 connected\n\
{M2} 127.0.0.1:7002@17002 master - 0 0 2 connected 5461-10922\n\
{R1} 127.0.0.1:7101@17101 slave {M1} 0 0 1 connected\n\
{M3} 127.0.0.1:7003@17003 master - 0 0 3 connected 10923-16383\n\
{R2} 127.0.0.1:7102@17102 slave {M2} 0 0 2 connected\n\
{M1} 127.0.0.1:7001@17001 myself,master - 0 0 1 connected 0-5460\n"
        );
        let b = ClusterMap::from_cluster_nodes(&shuffled, &id(M1));

        assert_eq!(a.cluster_slots_fingerprint(), b.cluster_slots_fingerprint());
        assert!(a.cluster_slots_fingerprint() != 0);
    }

    #[test]
    fn test_fingerprint_changes_when_slot_moves() {
        let before = ClusterMap::from_cluster_nodes(&three_shard_nodes(), &id(M1));

        // Move slot 12000 from M3 to M2 (M2 gains a single-slot range).
        let after_text = format!(
            "\
{M1} 127.0.0.1:7001@17001 myself,master - 0 0 1 connected 0-5460\n\
{M2} 127.0.0.1:7002@17002 master - 0 0 2 connected 5461-10922 12000\n\
{M3} 127.0.0.1:7003@17003 master - 0 0 3 connected 10923-11999 12001-16383\n"
        );
        let after = ClusterMap::from_cluster_nodes(&after_text, &id(M1));

        assert!(after.is_consistent);
        assert_ne!(
            before.cluster_slots_fingerprint(),
            after.cluster_slots_fingerprint()
        );
        assert_eq!(after.get_shard_by_slot(12000).unwrap().id, id(M2));
    }

    #[test]
    fn test_import_export_markers_are_ignored() {
        // M3 is exporting 12000, M2 is importing it: neither has settled
        // ownership yet, so the slot is uncovered and the map is not full.
        let text = format!(
            "\
{M1} 127.0.0.1:7001@17001 myself,master - 0 0 1 connected 0-5460\n\
{M2} 127.0.0.1:7002@17002 master - 0 0 2 connected 5461-10922 [12000-<-{M3}]\n\
{M3} 127.0.0.1:7003@17003 master - 0 0 3 connected 10923-16383 [12000->-{M2}]\n"
        );
        let map = ClusterMap::from_cluster_nodes(&text, &id(M1));

        // The bracketed markers contributed no slots to either shard.
        assert_eq!(map.get_shard_by_id(M2).unwrap().owned_slots.len(), 5462);
        assert_eq!(map.get_shard_by_id(M3).unwrap().owned_slots.len(), 5461);
    }

    #[test]
    fn test_empty_ip_on_local_myself_line() {
        // Early in cluster life the local node's own line can have an empty ip.
        let text = format!(
            "\
{M1} :7001@17001 myself,master - 0 0 1 connected 0-16383\n"
        );
        let map = ClusterMap::from_cluster_nodes(&text, &id(M1));

        let shard = map.get_shard_by_id(M1).unwrap();
        let primary = shard.primary.unwrap();
        assert!(primary.is_local());
        // A local node's address is never dialed; a placeholder is fine.
        assert_eq!(
            primary.socket_address.primary_endpoint,
            IpAddr::V4(Ipv4Addr::UNSPECIFIED)
        );
        assert!(map.is_consistent);
    }

    #[test]
    fn test_ipv6_and_hostname_address() {
        let line = format!("{M1} ::1:7001@17001,host.example.com master - 0 0 1 connected 0-16383");
        let rec = parse_node_line(&line, &id(M2)).unwrap();
        let addr = rec.socket_address.unwrap();
        assert_eq!(addr.primary_endpoint, "::1".parse::<IpAddr>().unwrap());
        assert_eq!(addr.port, 7001);
        assert!(rec.is_master);
        assert!(!rec.is_local);
    }

    #[test]
    fn test_replica_before_master_line_ordering() {
        // Replica line appears before its master; two-pass assembly must still
        // attach it.
        let text = format!(
            "\
{R1} 127.0.0.1:7101@17101 slave {M1} 0 0 1 connected\n\
{M1} 127.0.0.1:7001@17001 myself,master - 0 0 1 connected 0-16383\n"
        );
        let map = ClusterMap::from_cluster_nodes(&text, &id(M1));
        let shard = map.get_shard_by_id(M1).unwrap();
        assert_eq!(shard.replicas.len(), 1);
        assert_eq!(shard.replicas[0].id, id(R1));
    }

    #[test]
    fn test_parse_node_line_rejects_malformed() {
        // Too few fields.
        assert!(parse_node_line("too short line here", &id(M1)).is_none());
        // Node id of the wrong length.
        assert!(
            parse_node_line(
                "short 127.0.0.1:7001@17001 master - 0 0 1 connected",
                &id(M1)
            )
            .is_none()
        );
        // No clear role flag.
        let line = format!("{M1} 127.0.0.1:7001@17001 handshake - 0 0 1 connected");
        assert!(parse_node_line(&line, &id(M2)).is_none());
    }

    #[test]
    fn test_same_topology_independent_of_line_order() {
        let a = ClusterMap::from_cluster_nodes(&three_shard_nodes(), &id(M1));
        let shuffled = format!(
            "\
{R3} 127.0.0.1:7103@17103 slave {M3} 0 0 3 connected\n\
{M2} 127.0.0.1:7002@17002 master - 0 0 2 connected 5461-10922\n\
{R1} 127.0.0.1:7101@17101 slave {M1} 0 0 1 connected\n\
{M3} 127.0.0.1:7003@17003 master - 0 0 3 connected 10923-16383\n\
{R2} 127.0.0.1:7102@17102 slave {M2} 0 0 2 connected\n\
{M1} 127.0.0.1:7001@17001 myself,master - 0 0 1 connected 0-5460\n"
        );
        let b = ClusterMap::from_cluster_nodes(&shuffled, &id(M1));
        assert!(a.same_topology(&b));
        assert!(b.same_topology(&a));
    }

    #[test]
    fn test_same_topology_detects_replica_change() {
        // Removing a replica keeps the slot fingerprint identical but must
        // still be seen as a topology change.
        let a = ClusterMap::from_cluster_nodes(&three_shard_nodes(), &id(M1));
        let without_r3 = format!(
            "\
{M1} 127.0.0.1:7001@17001 myself,master - 0 0 1 connected 0-5460\n\
{R1} 127.0.0.1:7101@17101 slave {M1} 0 0 1 connected\n\
{M2} 127.0.0.1:7002@17002 master - 0 0 2 connected 5461-10922\n\
{R2} 127.0.0.1:7102@17102 slave {M2} 0 0 2 connected\n\
{M3} 127.0.0.1:7003@17003 master - 0 0 3 connected 10923-16383\n"
        );
        let b = ClusterMap::from_cluster_nodes(&without_r3, &id(M1));
        assert_eq!(a.cluster_slots_fingerprint(), b.cluster_slots_fingerprint());
        assert!(!a.same_topology(&b));
    }

    #[test]
    fn test_same_topology_detects_slot_move() {
        let a = ClusterMap::from_cluster_nodes(&three_shard_nodes(), &id(M1));
        let moved = format!(
            "\
{M1} 127.0.0.1:7001@17001 myself,master - 0 0 1 connected 0-5460\n\
{R1} 127.0.0.1:7101@17101 slave {M1} 0 0 1 connected\n\
{M2} 127.0.0.1:7002@17002 master - 0 0 2 connected 5461-10922 12000\n\
{R2} 127.0.0.1:7102@17102 slave {M2} 0 0 2 connected\n\
{M3} 127.0.0.1:7003@17003 master - 0 0 3 connected 10923-11999 12001-16383\n\
{R3} 127.0.0.1:7103@17103 slave {M3} 0 0 3 connected\n"
        );
        let b = ClusterMap::from_cluster_nodes(&moved, &id(M1));
        assert!(!a.same_topology(&b));
    }

    #[test]
    fn test_extend_expiration_unexpires_map() {
        let map = ClusterMap::from_cluster_nodes(&three_shard_nodes(), &id(M1));
        map.expiration_ts.store(0, AtomicOrdering::Relaxed);
        assert!(map.is_expired());
        map.extend_expiration(750);
        assert!(!map.is_expired());
    }

    #[test]
    fn test_replica_with_missing_master_marks_inconsistent() {
        // M1 covers all slots (full, no gap → no logging), but R2 references a
        // master that is absent from the reply.
        let text = format!(
            "\
{M1} 127.0.0.1:7001@17001 myself,master - 0 0 1 connected 0-16383\n\
{R2} 127.0.0.1:7102@17102 slave {M2} 0 0 2 connected\n"
        );
        let map = ClusterMap::from_cluster_nodes(&text, &id(M1));
        assert!(!map.is_consistent);
    }

    #[test]
    fn test_local_replica_owns_its_shard_slots() {
        // Local node is a replica of M1; it "owns" (serves) M1's slots.
        let text = format!(
            "\
{M1} 127.0.0.1:7001@17001 master - 0 0 1 connected 0-5460\n\
{R1} 127.0.0.1:7101@17101 myself,slave {M1} 0 0 1 connected\n\
{M2} 127.0.0.1:7002@17002 master - 0 0 2 connected 5461-16383\n"
        );
        let map = ClusterMap::from_cluster_nodes(&text, &id(R1));
        assert!(map.i_own_slot(0));
        assert!(map.i_own_slot(5460));
        assert!(!map.i_own_slot(5461));
        assert!(map.get_shard_by_id(M1).unwrap().is_local);
    }
}
