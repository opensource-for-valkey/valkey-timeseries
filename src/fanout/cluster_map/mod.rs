//! The cluster map: which shard owns each slot and which nodes serve it, built
//! from `CLUSTER NODES` ([`parse`]) over the node model in [`node`], and the
//! fanout target selection that reads it.

use crate::common::hash::DeterministicHasher;
use crate::common::logging::{log_debug, log_warning};
use crate::common::threads::LockGil;
use crate::common::time::current_time_millis;
use crate::config::CLUSTER_MAP_EXPIRATION_MS;
use crate::fanout::calculate_hash_slot;
use ahash::{AHashMap, HashSet, HashSetExt};
use rand::rng;
use range_set_blaze::{RangeMapBlaze, RangeSetBlaze};
use smallvec::SmallVec;
use std::collections::BTreeSet;
use std::hash::Hasher;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering as AtomicOrdering};
use std::sync::{Arc, OnceLock};
use valkey_module::{CallOptionsBuilder, CallReply, CallResult, Context, DetachedContext};

mod node;
mod parse;

#[cfg(test)]
pub use node::NodeLocation;
pub use node::{
    CURRENT_NODE_ID, NodeId, NodeInfo, NodeRole, ShardInfo, SlotRangeSet, SocketAddress,
};
use parse::{ParsedNodeLine, SlotRangeBuf, parse_node_line};

/// Where [`ClusterMap::create`] gets its `CLUSTER NODES` reply from.
///
/// The call needs the GIL; parsing the reply does not. A [`DetachedContext`]
/// takes the lock for the call alone, so a worker thread rebuilding the map
/// holds the GIL only for as long as the server needs to render the reply. A
/// [`Context`] is already locked (a command handler or callback on the main
/// thread) and issues the call directly.
pub trait ClusterNodesSource {
    /// The raw `CLUSTER NODES` reply, or `None` when the call did not produce a
    /// usable string, together with its observation sequence: a number that
    /// increases strictly in the order the replies were produced, so a refresh
    /// can tell whether a concurrently published map saw a newer topology.
    fn cluster_nodes(&self) -> Option<(u64, String)>;
}

/// Observation sequence for `CLUSTER NODES` replies. Only advanced under the
/// GIL, right after the call, so its order is the order of the observations.
static CLUSTER_NODES_OBSERVATIONS: AtomicU64 = AtomicU64::new(0);

impl ClusterNodesSource for Context {
    fn cluster_nodes(&self) -> Option<(u64, String)> {
        let call_options = CallOptionsBuilder::new().errors_as_replies().build();
        let res: CallResult = self.call_ext::<_, CallResult>("CLUSTER", &call_options, &["NODES"]);
        let observed = CLUSTER_NODES_OBSERVATIONS.fetch_add(1, AtomicOrdering::Relaxed) + 1;
        // The reply is freed here, while the lock is still held.
        reply_as_string(res).map(|text| (observed, text))
    }
}

impl ClusterNodesSource for DetachedContext {
    fn cluster_nodes(&self) -> Option<(u64, String)> {
        let ctx = self.lock_gil();
        ctx.cluster_nodes()
    }
}

// Constants
pub const NUM_SLOTS: u16 = 16384;

/// Enumeration for fanout target modes
// `Slots` has no command caller yet; the hash-tag modes are built on it.
#[allow(dead_code)]
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub enum FanoutTarget {
    /// Default: randomly select one node per shard
    #[default]
    Random,
    /// Select one replica per shard (if available), otherwise primary
    ReplicaPerShard,
    /// Select all primary (master) nodes
    Primary,
    /// Select all nodes (both primary and replica)
    All,
    /// Randomly select one node per shard owning any of the slots
    Slots(SmallVec<[u16; 4]>),
    /// Select a random node from each of the slots corresponding to the hash tags provided
    HashTags(Vec<String>),
    /// Select the primary node for each slot corresponding to the hash tags provided
    HashTagsPrimary(Vec<String>),
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
    /// Slot set for slots owned by the cluster
    owned_slots: SlotRangeSet,
    shards: BTreeSet<ShardInfo>,
    /// A range map, mapping(start slot, end slot) => shard id
    slot_to_shard_map: RangeMapBlaze<u16, NodeId>,
    /// Cluster-level fingerprint (hash of all shard fingerprints)
    cluster_slots_fingerprint: u64,
    /// (Lazily) pre-computed target lists
    primary_targets: LazyTargets,
    all_targets: LazyTargets,
    /// Expiration timestamp in ms (since epoch). Atomic so a refresh that
    /// finds the topology unchanged can extend the published map's lifetime
    /// in place instead of swapping in an identical copy.
    expiration_ts: AtomicI64,
    /// Observation sequence of the latest `CLUSTER NODES` reply this map
    /// stands for (see [`ClusterNodesSource`]): the one it was built from, or a
    /// later one that confirmed its topology unchanged. 0 for the initial empty
    /// map. Atomic so a confirmation can raise it on the published map in place.
    observed_seq: AtomicU64,
    /// is the current map consistent (no collisions/inconsistencies found while building)
    pub is_consistent: bool,
}

impl ClusterMap {
    /// Slot ownership checks
    #[cfg(test)]
    pub fn i_own_slot(&self, slot: u16) -> bool {
        self.owned_slots.contains(slot)
    }

    /// Get the count of owned slots
    #[cfg(test)]
    pub fn owned_slot_count(&self) -> usize {
        self.owned_slots.len()
    }

    /// Look up a shard by id. Will return None if shard does not exist
    #[cfg(test)]
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

    /// Get cluster level slot fingerprint
    pub fn cluster_slots_fingerprint(&self) -> u64 {
        self.cluster_slots_fingerprint
    }

    /// Helper function to refresh targets in CreateNewClusterMap
    pub fn get_targets(&self, target_mode: FanoutTarget) -> Arc<HashSet<NodeInfo>> {
        match target_mode {
            FanoutTarget::Primary => self.primary_targets(),
            FanoutTarget::All => self.all_targets(),
            FanoutTarget::ReplicaPerShard => self.random_one_replica_per_shard(),
            FanoutTarget::Random => self.random_one_per_shard(),
            FanoutTarget::Slots(slots) => self.targets_for_slots(slots, false),
            FanoutTarget::HashTags(hash_tags) => {
                let slots = hash_tags
                    .iter()
                    .map(|tag| calculate_hash_slot(tag.as_ref()));
                self.targets_for_slots(slots, false)
            }
            FanoutTarget::HashTagsPrimary(hash_tags) => {
                let slots = hash_tags
                    .iter()
                    .map(|tag| calculate_hash_slot(tag.as_ref()));
                self.targets_for_slots(slots, true)
            }
        }
    }

    /// One node for each distinct shard owning `slots`.
    ///
    /// Slots are resolved to shards *before* a node is chosen, so two slots of
    /// one shard cannot pick its primary and a replica and count the shard
    /// twice. A slot with no owner yields an empty set, which fails the fanout
    /// (`no cluster nodes available`) instead of returning the owned shards'
    /// answer as if it were complete.
    fn targets_for_slots(
        &self,
        slots: impl IntoIterator<Item = u16>,
        primary_only: bool,
    ) -> Arc<HashSet<NodeInfo>> {
        let mut shards: SmallVec<[&ShardInfo; 4]> = SmallVec::new();
        for slot in slots {
            let Some(shard) = self.get_shard_by_slot(slot) else {
                return Arc::default();
            };
            if !shards.iter().any(|known| known.id == shard.id) {
                shards.push(shard);
            }
        }

        let mut rng_ = rng();
        let mut targets = HashSet::with_capacity(shards.len());
        for shard in shards {
            let node = if primary_only {
                shard.primary
            } else {
                shard.pick_target(&mut rng_, false)
            };
            let Some(node) = node else {
                return Arc::default();
            };
            targets.insert(node);
        }
        Arc::new(targets)
    }

    fn random_one_per_shard(&self) -> Arc<HashSet<NodeInfo>> {
        let mut rng_ = rng();
        let mut targets = HashSet::with_capacity(self.shards.len());
        for shard in self.shards.iter() {
            targets.extend(shard.pick_target(&mut rng_, false));
        }
        Arc::new(targets)
    }

    fn random_one_replica_per_shard(&self) -> Arc<HashSet<NodeInfo>> {
        let mut rng_ = rng();
        let mut targets = HashSet::with_capacity(self.shards.len());
        for shard in self.shards.iter() {
            // prefer a healthy replica, fall back to the primary otherwise
            targets.extend(shard.pick_target(&mut rng_, true));
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

    /// Build a new ClusterMap from `CLUSTER NODES`.
    ///
    /// `CLUSTER NODES` is used in preference to `CLUSTER SLOTS` because its reply
    /// is generated from the node's own gossip view and does not consult the
    /// calling client's connection. `CLUSTER SLOTS` resolves each node's
    /// preferred endpoint against the current client, which crashes the server
    /// when invoked without a real client context (e.g. from a background
    /// thread or the cluster-message callback). Parsing `CLUSTER NODES` lets us
    /// refresh the map safely from any context that can take the module lock.
    ///
    /// Only the call itself runs under the GIL (see [`ClusterNodesSource`]);
    /// the reply is parsed after it is released, so a [`DetachedContext`]
    /// caller holds the lock for the call alone.
    /// Returns `None` when the `CLUSTER NODES` call fails to produce a usable
    /// reply, so callers can distinguish a build failure from a successfully
    /// built (possibly inconsistent) map and avoid clobbering a good map.
    pub fn create(ctx: &impl ClusterNodesSource) -> Option<Self> {
        log_debug("Calling CLUSTER NODES...");

        let Some((observed_seq, text)) = ctx.cluster_nodes() else {
            log_warning("CLUSTER NODES did not return a usable string reply");
            return None;
        };

        let my_node_id = *CURRENT_NODE_ID;
        debug_assert!(!my_node_id.is_empty(), "Current node id is empty");

        let mut map = Self::from_cluster_nodes(&text, &my_node_id);
        *map.observed_seq.get_mut() = observed_seq;
        Some(map)
    }

    #[cfg(test)]
    pub(super) fn with_observed_seq(mut self, observed_seq: u64) -> Self {
        *self.observed_seq.get_mut() = observed_seq;
        self
    }

    /// See [`ClusterNodesSource`]: a map with a higher sequence saw a newer topology.
    pub fn observed_seq(&self) -> u64 {
        self.observed_seq.load(AtomicOrdering::Relaxed)
    }

    /// Record that the later observation `observed_seq` found this map's
    /// topology unchanged, so the map now stands for it. Only the publication
    /// step in `fanout::publish_observation` calls this, under its lock.
    pub(in crate::fanout) fn confirm_observation(&self, observed_seq: u64) {
        self.observed_seq
            .fetch_max(observed_seq, AtomicOrdering::Relaxed);
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
        // Masters that hold no data: no settled slots and none being imported.
        let mut slotless_masters: HashSet<NodeId> = HashSet::new();

        // Pass 1: masters that own (or are importing) slots become shards.
        for rec in records.iter() {
            if !rec.is_master {
                continue;
            }
            let mut slot_ranges = SlotRangeBuf::new();
            for &(start, end) in rec.slot_ranges.iter() {
                if start > end || end >= NUM_SLOTS {
                    new_map.is_consistent = false;
                    continue;
                }
                slot_ranges.push((start, end));
            }

            // A master with no slots holds no data, so it is not a shard: it
            // must neither be a fanout target nor enter the fingerprint. The
            // common case is a failed former primary after a failover, which
            // stays listed as `master,fail` until it rejoins or is forgotten.
            // A master importing a slot keeps its shard, as keys already moved
            // to it are only visible there until the migration settles.
            if slot_ranges.is_empty() && !rec.imports_slots {
                slotless_masters.insert(rec.id);
                continue;
            }

            let Some(socket_address) = rec.socket_address else {
                // A master that owns slots but has no usable address cannot
                // serve them, so the map is not trustworthy.
                new_map.is_consistent = false;
                continue;
            };

            let primary = NodeInfo {
                id: rec.id,
                shard_id: rec.id,
                socket_address,
                role: NodeRole::Primary,
                location: rec.location(),
                healthy: rec.is_healthy,
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

            for (start, end) in slot_ranges {
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
                // A replica of a slot-less master serves nothing; one whose
                // master is not (yet) known makes the view inconsistent.
                if !slotless_masters.contains(&rec.master_id) {
                    new_map.is_consistent = false;
                }
                continue;
            };
            shard.replicas.push(NodeInfo {
                id: rec.id,
                shard_id: shard.id,
                socket_address,
                role: NodeRole::Replica,
                location: rec.location(),
                healthy: rec.is_healthy,
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

        // Finalize shards: back-pointers.
        let mut shard_set: BTreeSet<ShardInfo> = BTreeSet::new();
        for (_, mut shard) in shard_map.into_iter() {
            if let Some(primary) = shard.primary.as_mut() {
                primary.shard_id = shard.id;
            }
            for replica in shard.replicas.iter_mut() {
                replica.shard_id = shard.id;
            }
            // Line order is not topology: two replies listing the same replicas
            // in a different order must compare equal.
            shard.replicas.sort_unstable_by_key(|replica| replica.id);
            shard_set.insert(shard);
        }
        new_map.shards = shard_set;

        // Build slot-to-shard map
        new_map.is_consistent &= new_map.build_slot_to_shard_map(&slot_ranges_parsed);

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
    /// same primaries, replicas, addresses, roles, health and slot assignments.
    /// `slot_to_shard_map` and `owned_slots` are derived from the shard set,
    /// so comparing the shards covers them.
    pub fn same_topology(&self, other: &ClusterMap) -> bool {
        self.shards.len() == other.shards.len()
            && self
                .shards
                .iter()
                .zip(other.shards.iter())
                .all(|(a, b)| a.same_content(b))
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

    /// Route each claimed slot range to its shard. Returns false when the
    /// claims conflict: slots claimed by more than one master are left
    /// unrouted rather than given to whichever claim came last, so slot-based
    /// routing fails for them instead of silently picking one owner.
    fn build_slot_to_shard_map(&mut self, slot_ranges: &[SlotRangeInfo]) -> bool {
        let mut claimed = RangeSetBlaze::new();
        let mut contested = RangeSetBlaze::new();
        let mut consistent = true;
        for r in slot_ranges {
            if !self.shards.contains(&r.shard_id) {
                consistent = false;
                continue;
            }
            let range = RangeSetBlaze::from_iter([r.start_slot..=r.end_slot]);
            contested |= &claimed & &range;
            claimed |= range;
            self.slot_to_shard_map
                .ranges_insert(r.start_slot..=r.end_slot, r.shard_id);
        }
        if !contested.is_empty() {
            log_warning(format!(
                "cluster-map: slots {contested} are claimed by more than one master"
            ));
            self.slot_to_shard_map = &self.slot_to_shard_map - &contested;
            consistent = false;
        }
        consistent
    }

    /// Hash of the slot -> owner assignment alone. Two nodes agree on it exactly
    /// when they would route every slot to the same shard; membership that owns
    /// no slots (replicas, empty or forgotten masters) leaves it unchanged.
    fn compute_cluster_fingerprint(&self) -> u64 {
        let mut hasher = DeterministicHasher::default();
        for (range, shard_id) in self.slot_to_shard_map.range_values() {
            hasher.write_u16(*range.start());
            hasher.write_u16(*range.end());
            hasher.write(shard_id.as_bytes());
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

// Unit tests
#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{IpAddr, Ipv4Addr};

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

    /// Ids of every node `mode` selected across `draws` independent selections.
    fn selected_ids(map: &ClusterMap, mode: FanoutTarget, draws: usize) -> BTreeSet<NodeId> {
        (0..draws)
            .flat_map(|_| {
                map.get_targets(mode.clone())
                    .iter()
                    .map(|n| n.id)
                    .collect::<Vec<_>>()
            })
            .collect()
    }

    fn ids(nodes: &[&str]) -> BTreeSet<NodeId> {
        nodes.iter().map(|n| id(n)).collect()
    }

    #[test]
    fn test_failed_slotless_former_primary_is_not_a_shard() {
        // R1 took over M1's slots; M1 stays listed as a failed, slot-less master.
        let after_failover = format!(
            "\
{R1} 127.0.0.1:7101@17101 myself,master - 0 0 4 connected 0-8191\n\
{M1} 127.0.0.1:7001@17001 master,fail - 0 0 1 disconnected\n\
{M2} 127.0.0.1:7002@17002 master - 0 0 2 connected 8192-16383\n"
        );
        let map = ClusterMap::from_cluster_nodes(&after_failover, &id(R1));

        assert!(map.is_consistent);
        assert_eq!(map.all_shards().len(), 2);
        assert!(map.get_shard_by_id(M1).is_none());
        assert_eq!(selected_ids(&map, FanoutTarget::Primary, 1), ids(&[R1, M2]));
        assert_eq!(
            selected_ids(&map, FanoutTarget::Random, 100),
            ids(&[R1, M2])
        );
        assert_eq!(selected_ids(&map, FanoutTarget::All, 1), ids(&[R1, M2]));

        // Nodes that have and have not forgotten M1 agree on the fingerprint.
        let forgotten = format!(
            "\
{R1} 127.0.0.1:7101@17101 myself,master - 0 0 4 connected 0-8191\n\
{M2} 127.0.0.1:7002@17002 master - 0 0 2 connected 8192-16383\n"
        );
        let forgotten = ClusterMap::from_cluster_nodes(&forgotten, &id(R1));
        assert_eq!(
            map.cluster_slots_fingerprint(),
            forgotten.cluster_slots_fingerprint()
        );
    }

    #[test]
    fn test_slotless_master_and_its_replica_are_ignored() {
        // A freshly met empty master with a replica holds no data.
        let text = format!(
            "\
{M1} 127.0.0.1:7001@17001 myself,master - 0 0 1 connected 0-16383\n\
{M3} 127.0.0.1:7003@17003 master - 0 0 3 connected\n\
{R3} 127.0.0.1:7103@17103 slave {M3} 0 0 3 connected\n"
        );
        let map = ClusterMap::from_cluster_nodes(&text, &id(M1));
        assert!(map.is_consistent);
        assert_eq!(map.all_shards().len(), 1);
        assert_eq!(selected_ids(&map, FanoutTarget::All, 1), ids(&[M1]));

        let alone = format!("{M1} 127.0.0.1:7001@17001 myself,master - 0 0 1 connected 0-16383\n");
        let alone = ClusterMap::from_cluster_nodes(&alone, &id(M1));
        assert_eq!(
            map.cluster_slots_fingerprint(),
            alone.cluster_slots_fingerprint()
        );
    }

    #[test]
    fn test_importing_master_without_slots_stays_a_shard() {
        // Keys already migrated to M3 live only there until the slot settles.
        let text = format!(
            "\
{M1} 127.0.0.1:7001@17001 myself,master - 0 0 1 connected 0-16383 [100->-{M3}]\n\
{M3} 127.0.0.1:7003@17003 master - 0 0 3 connected [100-<-{M1}]\n"
        );
        let map = ClusterMap::from_cluster_nodes(&text, &id(M1));
        assert!(map.is_consistent);
        assert_eq!(selected_ids(&map, FanoutTarget::Primary, 1), ids(&[M1, M3]));
        // It owns nothing yet, so slot routing still goes to M1.
        assert_eq!(map.get_shard_by_slot(100).unwrap().id, id(M1));
    }

    #[test]
    fn test_unhealthy_replicas_are_not_selected() {
        let text = format!(
            "\
{M1} 127.0.0.1:7001@17001 myself,master - 0 0 1 connected 0-16383\n\
{R1} 127.0.0.1:7101@17101 slave,fail {M1} 0 0 1 disconnected\n\
{R2} 127.0.0.1:7102@17102 slave {M1} 0 0 1 disconnected\n\
{R3} 127.0.0.1:7103@17103 slave,fail? {M1} 0 0 1 connected\n\
{R4} 127.0.0.1:7104@17104 slave {M1} 0 0 1 connected\n",
            R4 = "dddddddddddddddddddddddddddddddddddddddd"
        );
        let map = ClusterMap::from_cluster_nodes(&text, &id(M1));
        assert!(map.is_consistent);
        let r4 = "dddddddddddddddddddddddddddddddddddddddd";
        assert_eq!(
            selected_ids(&map, FanoutTarget::Random, 200),
            ids(&[M1, r4])
        );
        assert_eq!(
            selected_ids(&map, FanoutTarget::HashTags(vec!["a".into()]), 200),
            ids(&[M1, r4])
        );
        assert_eq!(
            selected_ids(&map, FanoutTarget::ReplicaPerShard, 50),
            ids(&[r4])
        );
    }

    #[test]
    fn test_shard_with_no_healthy_node_still_targets_its_primary() {
        // Omitting the shard would silently drop its data; failing is correct.
        let text = format!(
            "\
{M1} 127.0.0.1:7001@17001 myself,master - 0 0 1 connected 0-8191\n\
{M2} 127.0.0.1:7002@17002 master,fail? - 0 0 2 connected 8192-16383\n\
{R2} 127.0.0.1:7102@17102 slave,fail {M2} 0 0 2 disconnected\n"
        );
        let map = ClusterMap::from_cluster_nodes(&text, &id(M1));
        assert_eq!(
            selected_ids(&map, FanoutTarget::Random, 100),
            ids(&[M1, M2])
        );
        assert_eq!(
            selected_ids(&map, FanoutTarget::ReplicaPerShard, 50),
            ids(&[M1, M2])
        );
    }

    #[test]
    fn test_failed_primary_with_healthy_replica() {
        let text = format!(
            "\
{M1} 127.0.0.1:7001@17001 master,fail? - 0 0 1 connected 0-16383\n\
{R1} 127.0.0.1:7101@17101 myself,slave {M1} 0 0 1 connected\n"
        );
        let map = ClusterMap::from_cluster_nodes(&text, &id(R1));
        // Primary-only routing must not substitute a replica.
        assert_eq!(selected_ids(&map, FanoutTarget::Primary, 1), ids(&[M1]));
        assert_eq!(selected_ids(&map, FanoutTarget::Random, 100), ids(&[R1]));
    }

    /// `count` distinct hash tags whose slots are distinct and lie in `range`.
    fn tags_in(range: std::ops::RangeInclusive<u16>, count: usize) -> Vec<String> {
        let mut slots = Vec::new();
        let mut tags = Vec::new();
        for i in 0.. {
            let tag = format!("t{i}");
            let slot = calculate_hash_slot(tag.as_bytes());
            if range.contains(&slot) && !slots.contains(&slot) {
                slots.push(slot);
                tags.push(tag);
                if tags.len() == count {
                    break;
                }
            }
        }
        tags
    }

    #[test]
    fn test_hash_tags_on_one_shard_select_one_node() {
        let text = format!(
            "\
{M1} 127.0.0.1:7001@17001 myself,master - 0 0 1 connected 0-16383\n\
{R1} 127.0.0.1:7101@17101 slave {M1} 0 0 1 connected\n\
{R2} 127.0.0.1:7102@17102 slave {M1} 0 0 1 connected\n"
        );
        let map = ClusterMap::from_cluster_nodes(&text, &id(M1));
        let mut tags = tags_in(0..=16383, 4);
        tags.push(tags[0].clone());
        let mut seen = BTreeSet::new();
        for _ in 0..200 {
            let targets = map.get_targets(FanoutTarget::HashTags(tags.clone()));
            assert_eq!(targets.len(), 1);
            seen.extend(targets.iter().map(|n| n.id));
        }
        // Still a random choice among the shard's nodes.
        assert_eq!(seen, ids(&[M1, R1, R2]));
    }

    #[test]
    fn test_hash_tags_across_shards_select_one_node_per_shard() {
        let map = ClusterMap::from_cluster_nodes(&three_shard_nodes(), &id(M1));
        let mut tags = tags_in(0..=5460, 3);
        tags.extend(tags_in(10923..=16383, 2));
        for _ in 0..100 {
            let targets = map.get_targets(FanoutTarget::HashTags(tags.clone()));
            let shards: BTreeSet<_> = targets.iter().map(|n| n.shard_id).collect();
            assert_eq!(targets.len(), 2);
            assert_eq!(shards, ids(&[M1, M3]));
        }
        let primaries = map.get_targets(FanoutTarget::HashTagsPrimary(tags));
        let primaries: BTreeSet<_> = primaries.iter().map(|n| n.id).collect();
        assert_eq!(primaries, ids(&[M1, M3]));
    }

    #[test]
    fn test_hash_tag_on_unowned_slot_selects_nothing() {
        // Slots 8192-16383 have no owner.
        let text = format!("{M1} 127.0.0.1:7001@17001 myself,master - 0 0 1 connected 0-8191\n");
        let map = ClusterMap::from_cluster_nodes(&text, &id(M1));
        assert!(!map.is_consistent);
        let mut tags = tags_in(0..=8191, 1);
        assert_eq!(
            map.get_targets(FanoutTarget::HashTags(tags.clone())).len(),
            1
        );
        tags.extend(tags_in(8192..=16383, 1));
        assert!(
            map.get_targets(FanoutTarget::HashTags(tags.clone()))
                .is_empty()
        );
        assert!(
            map.get_targets(FanoutTarget::HashTagsPrimary(tags))
                .is_empty()
        );
    }

    #[test]
    fn test_conflicting_slot_claims_are_not_routed() {
        let text = format!(
            "\
{M1} 127.0.0.1:7001@17001 myself,master - 0 0 1 connected 0-16383\n\
{M2} 127.0.0.1:7002@17002 master - 0 0 2 connected 1 100-200\n"
        );
        let map = ClusterMap::from_cluster_nodes(&text, &id(M1));
        assert!(!map.is_consistent);
        assert!(map.get_shard_by_slot(1).is_none());
        assert!(map.get_shard_by_slot(150).is_none());
        assert_eq!(map.get_shard_by_slot(0).unwrap().id, id(M1));
        assert_eq!(map.get_shard_by_slot(2).unwrap().id, id(M1));
        assert_eq!(map.get_shard_by_slot(201).unwrap().id, id(M1));
    }

    #[test]
    fn test_same_topology_independent_of_replica_order() {
        let a = format!(
            "\
{M1} 127.0.0.1:7001@17001 myself,master - 0 0 1 connected 0-16383\n\
{R1} 127.0.0.1:7101@17101 slave {M1} 0 0 1 connected\n\
{R2} 127.0.0.1:7102@17102 slave {M1} 0 0 1 connected\n"
        );
        let b = format!(
            "\
{R2} 127.0.0.1:7102@17102 slave {M1} 0 0 1 connected\n\
{M1} 127.0.0.1:7001@17001 myself,master - 0 0 1 connected 0-16383\n\
{R1} 127.0.0.1:7101@17101 slave {M1} 0 0 1 connected\n"
        );
        let a = ClusterMap::from_cluster_nodes(&a, &id(M1));
        let b = ClusterMap::from_cluster_nodes(&b, &id(M1));
        assert!(a.same_topology(&b));
    }

    #[test]
    fn test_same_topology_detects_health_and_address_changes() {
        let base = ClusterMap::from_cluster_nodes(&three_shard_nodes(), &id(M1));
        let failed = three_shard_nodes().replace(
            &format!("{R2} 127.0.0.1:7102@17102 slave {M2} 0 0 2 connected"),
            &format!("{R2} 127.0.0.1:7102@17102 slave,fail {M2} 0 0 2 disconnected"),
        );
        let moved = three_shard_nodes().replace("127.0.0.1:7103@17103", "127.0.0.1:7203@17203");
        assert!(!base.same_topology(&ClusterMap::from_cluster_nodes(&failed, &id(M1))));
        assert!(!base.same_topology(&ClusterMap::from_cluster_nodes(&moved, &id(M1))));
    }

    #[test]
    fn test_remote_port_zero_is_unusable() {
        let text = format!(
            "\
{M1} 127.0.0.1:7001@17001 myself,master - 0 0 1 connected 0-8191\n\
{M2} 127.0.0.1:0@0 master - 0 0 2 connected 8192-16383\n\
{R1} 127.0.0.1:x@17101 slave {M1} 0 0 1 connected\n"
        );
        let map = ClusterMap::from_cluster_nodes(&text, &id(M1));
        // M2 owns slots it cannot be asked about.
        assert!(!map.is_consistent);
        assert!(map.get_shard_by_id(M1).unwrap().replicas.is_empty());
    }
}
