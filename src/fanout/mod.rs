mod blocked_client;
mod cluster_map;
mod cluster_rpc;
mod fanout_client_command;
mod fanout_command;
mod fanout_context;
mod fanout_error;
mod fanout_message;
mod registry;
pub mod serialization;
mod utils;
mod workers;

use crate::common::logging::{log_debug, log_notice, log_warning};
use crate::common::metrics::clustermap as metrics;
use crate::common::time::current_time_millis;
use crate::config::CLUSTER_MAP_EXPIRATION_MS;
use ahash::HashSet;
use arc_swap::{ArcSwap, Guard};
use metered::Counter;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering};
use std::sync::{Arc, LazyLock};
use std::time::Duration;
use valkey_module::Context;

use super::fanout::cluster_rpc::register_cluster_message_handlers;
pub(crate) use cluster_rpc::{inflight_request_count, inflight_requests};
pub use fanout_client_command::*;
pub use fanout_command::*;
pub use fanout_context::FanoutContext;
pub use fanout_error::*;
pub use utils::*;
pub(crate) use workers::request_lane_stats;

pub(crate) use cluster_map::NUM_SLOTS;
pub use cluster_map::{ClusterMap, ClusterNodesSource, FanoutTarget, NodeInfo};
#[cfg(test)]
pub(crate) use cluster_map::{NodeRole, SocketAddress};
pub use registry::register_fanout_operation;

pub(crate) fn init_fanout(ctx: &Context) {
    if !is_clustered(ctx) {
        return;
    }
    register_cluster_message_handlers(ctx);
}

static CLUSTER_MAP: LazyLock<ArcSwap<ClusterMap>> =
    LazyLock::new(|| ArcSwap::from_pointee(ClusterMap::default()));

/// Set when a peer rejects one of our requests with a cluster-map mismatch.
/// The next target-selection or map access treats the local map as needing a
/// refresh, so the following fanout is built from up-to-date topology.
static CLUSTER_MAP_STALE: AtomicBool = AtomicBool::new(false);

/// Upper bound on the adaptive refresh interval. Replica membership and node
/// health changes alter neither the slot fingerprint (so no peer ever rejects
/// with a mismatch) nor, on remote nodes, any local event. The only thing that
/// notices them is the lazy refresh a fanout triggers once the map expires, so
/// the backoff must stay bounded.
const CLUSTER_MAP_BACKOFF_CAP_MS: u64 = 5_000;

/// Current adaptive refresh interval in ms. Starts at (and resets to) the
/// configured expiration, and doubles up to [`CLUSTER_MAP_BACKOFF_CAP_MS`]
/// each time a refresh confirms the topology is unchanged, so a stable
/// cluster converges to one `CLUSTER NODES` call per cap interval while
/// topology churn is re-checked at the configured floor.
static CLUSTER_MAP_REFRESH_INTERVAL_MS: AtomicU64 = AtomicU64::new(0);

/// When a refresh last built the cluster map or confirmed it unchanged, in ms since the epoch;
/// 0 before the first. Reported as `clustermap_age_seconds`: the map's expiry can't stand in for
/// it, because the expiry's TTL is adaptive.
static CLUSTER_MAP_VERIFIED_AT_MS: AtomicI64 = AtomicI64::new(0);

/// The current adaptive refresh interval, in ms (0 until the first refresh).
pub(crate) fn cluster_map_refresh_interval_ms() -> u64 {
    CLUSTER_MAP_REFRESH_INTERVAL_MS.load(Ordering::Relaxed)
}

/// How long since a refresh last built the cluster map or confirmed it unchanged; `None` before
/// the first.
pub(crate) fn cluster_map_age() -> Option<Duration> {
    let verified_at = CLUSTER_MAP_VERIFIED_AT_MS.load(Ordering::Relaxed);
    (verified_at > 0).then(|| {
        Duration::from_millis(current_time_millis().saturating_sub(verified_at).max(0) as u64)
    })
}

fn mark_cluster_map_verified() {
    CLUSTER_MAP_VERIFIED_AT_MS.store(current_time_millis(), Ordering::Relaxed);
}

/// Double the adaptive refresh interval and return the new value, clamped to
/// [configured floor, cap]. Called when a refresh confirmed the topology
/// unchanged. A floor configured above the cap wins: the operator asked for a
/// longer expiration than the backoff would ever produce.
fn grow_refresh_interval() -> u64 {
    let floor = CLUSTER_MAP_EXPIRATION_MS.load(Ordering::Relaxed);
    let cap = CLUSTER_MAP_BACKOFF_CAP_MS.max(floor);
    let current = CLUSTER_MAP_REFRESH_INTERVAL_MS.load(Ordering::Relaxed);
    let next = current.saturating_mul(2).clamp(floor, cap);
    CLUSTER_MAP_REFRESH_INTERVAL_MS.store(next, Ordering::Relaxed);
    next
}

/// Reset the adaptive refresh interval to the configured floor. Called when
/// the topology changed or a rebuild failed, so the next quiet period starts
/// backing off from scratch.
fn reset_refresh_interval() {
    let floor = CLUSTER_MAP_EXPIRATION_MS.load(Ordering::Relaxed);
    CLUSTER_MAP_REFRESH_INTERVAL_MS.store(floor, Ordering::Relaxed);
}

pub fn get_cluster_map() -> Guard<Arc<ClusterMap>> {
    CLUSTER_MAP.load()
}

/// Mark the local cluster map as stale so it is rebuilt on next use.
pub fn mark_cluster_map_stale() {
    metrics::STALE_MARKS.incr();
    CLUSTER_MAP_STALE.store(true, Ordering::Release);
}

fn take_cluster_map_stale() -> bool {
    CLUSTER_MAP_STALE.swap(false, Ordering::AcqRel)
}

/// Get the set of target nodes for a fanout, together with the cluster-map
/// fingerprint of the *same* snapshot the targets were chosen from. If the
/// local cluster map is stale, rebuild it first. The returned fingerprint is
/// used to detect topology changes on remote nodes, which may reject requests
/// with a mismatch error. The caller should mark the local map as stale when
/// that happens, so the next fanout is built from up-to-date topology.
pub fn get_fanout_targets(ctx: &Context, mode: FanoutTarget) -> (Arc<HashSet<NodeInfo>>, u64) {
    let current_map = CLUSTER_MAP.load();
    // Check if we need to refresh. A stale flag is set when a peer rejected one
    // of our requests due to a topology mismatch.
    let stale = take_cluster_map_stale();
    if !needs_refresh(&current_map, stale) {
        return (
            current_map.get_targets(mode),
            current_map.cluster_slots_fingerprint(),
        );
    }
    drop(current_map);
    // Take targets and fingerprint from the snapshot the refresh produced, not a reload.
    let map = refresh_after_trigger(ctx, stale);
    (map.get_targets(mode), map.cluster_slots_fingerprint())
}

/// Whether the published map must be rebuilt before use: it expired, or a peer
/// rejected one of our requests with a fingerprint mismatch (`stale`).
///
/// An inconsistent map is deliberately *not* a trigger. It is rebuilt when it
/// expires, like any other, and its expiry never backs off; rebuilding it on
/// every use made each fanout pay a `CLUSTER NODES` call and parse for as long
/// as the view stayed inconsistent (a node in handshake, a transient slot gap).
fn needs_refresh(map: &ClusterMap, stale: bool) -> bool {
    stale || map.is_expired()
}

/// Refresh because [`needs_refresh`] said so, and return the map to use. A
/// stale mark this caller consumed is put back when the refresh fails, so the
/// invalidation is not lost with it.
fn refresh_after_trigger(ctx: &impl ClusterNodesSource, stale: bool) -> Arc<ClusterMap> {
    refresh_cluster_map(ctx).into_map().unwrap_or_else(|| {
        if stale {
            CLUSTER_MAP_STALE.store(true, Ordering::Release);
        }
        CLUSTER_MAP.load_full()
    })
}

/// What [`refresh_cluster_map`] did with the map it built, carrying the exact snapshot the
/// refresh published or kept, so callers never pair the outcome with a later, possibly
/// concurrently swapped, map.
#[derive(Debug, Clone)]
pub enum ClusterMapRefresh {
    /// The topology changed (or a map was inconsistent): this new map was published.
    Changed(Arc<ClusterMap>),
    /// The topology was unchanged: this published map was kept, its expiry extended.
    Unchanged(Arc<ClusterMap>),
    /// The topology differed, but a concurrent refresh had already published a map built from
    /// a newer `CLUSTER NODES` observation; that map was kept and is carried here.
    Superseded(Arc<ClusterMap>),
    /// `CLUSTER NODES` gave no usable reply: the previous map stays in place.
    Failed,
}

impl ClusterMapRefresh {
    /// The snapshot this refresh published or kept; `None` when it failed.
    pub fn into_map(self) -> Option<Arc<ClusterMap>> {
        match self {
            Self::Changed(map) | Self::Unchanged(map) | Self::Superseded(map) => Some(map),
            Self::Failed => None,
        }
    }
}

// Refresh the cluster map by creating a new one from the current cluster state.
// When the rebuild shows the topology is unchanged, the published map is kept
// and its expiration extended instead of swapping in an identical copy: this
// preserves the lazily-computed target caches and the allocations shared by
// in-flight readers. If building the new map fails, the previous map is kept
// in place and a warning is logged so subsequent calls will retry the refresh.
//
// Refreshes may run concurrently (the main thread and the peer-request
// workers), and a slow one can finish after a faster, newer one. A new map is
// therefore published only if no map built from a later `CLUSTER NODES`
// observation is already in place (see `publish_if_newer`).
//
// `ctx` is a `Context` on the main thread or a `DetachedContext` on a worker;
// the latter holds the GIL only for the `CLUSTER NODES` call (see
// `ClusterNodesSource`).
pub fn refresh_cluster_map(ctx: &impl ClusterNodesSource) -> ClusterMapRefresh {
    log_debug("Refreshing cluster map...");
    metrics::REFRESHES.incr();
    let Some(new_map) = ClusterMap::create(ctx) else {
        metrics::REFRESH_FAILURES.incr();
        reset_refresh_interval();
        log_warning("Failed to build cluster map; keeping the previous map and retrying later");
        return ClusterMapRefresh::Failed;
    };
    mark_cluster_map_verified();

    let current_map = CLUSTER_MAP.load_full();
    if new_map.is_consistent == current_map.is_consistent && new_map.same_topology(&current_map) {
        // Only a consistent view earns a longer interval; an inconsistent one
        // is re-checked at the configured floor.
        let ttl = if new_map.is_consistent {
            grow_refresh_interval()
        } else {
            reset_refresh_interval();
            CLUSTER_MAP_EXPIRATION_MS.load(Ordering::Relaxed)
        };
        current_map.extend_expiration(ttl);
        metrics::REFRESH_UNCHANGED.incr();
        log_debug("Cluster map unchanged; extended expiration");
        return ClusterMapRefresh::Unchanged(current_map);
    }
    drop(current_map);

    reset_refresh_interval();
    match publish_if_newer(Arc::new(new_map)) {
        Ok(published) => {
            metrics::REFRESH_CHANGED.incr();
            log_notice("Cluster map refreshed: topology changed");
            ClusterMapRefresh::Changed(published)
        }
        Err(newer) => ClusterMapRefresh::Superseded(newer),
    }
}

/// Publish `map` unless the published map was built from a later observation.
/// `Ok` carries `map` once published; `Err` the newer map that was kept.
fn publish_if_newer(map: Arc<ClusterMap>) -> Result<Arc<ClusterMap>, Arc<ClusterMap>> {
    let previous = CLUSTER_MAP.rcu(|current| {
        if current.observed_seq() < map.observed_seq() {
            Arc::clone(&map)
        } else {
            Arc::clone(current)
        }
    });
    if previous.observed_seq() < map.observed_seq() {
        Ok(map)
    } else {
        Err(previous)
    }
}

/// The map to use now, and whether this call rebuilt it (so it reflects a
/// `CLUSTER NODES` observation taken just now).
pub fn current_or_refreshed_cluster_map(ctx: &impl ClusterNodesSource) -> (Arc<ClusterMap>, bool) {
    let current_map = get_cluster_map();
    let stale = take_cluster_map_stale();
    if !needs_refresh(&current_map, stale) {
        return (current_map.clone(), false);
    }
    drop(current_map);
    (refresh_after_trigger(ctx, stale), true)
}

#[cfg(test)]
mod tests {
    use super::*;

    const ONE_SHARD: &str = "1111111111111111111111111111111111111111 127.0.0.1:7001@17001 myself,master - 0 0 1 connected 0-16383\n";
    const SLOT_GAP: &str = "1111111111111111111111111111111111111111 127.0.0.1:7001@17001 myself,master - 0 0 1 connected 0-100\n";

    fn map(text: &str, observed_seq: u64) -> Arc<ClusterMap> {
        let me = cluster_map::NodeId::from_bytes(&text.as_bytes()[..40]);
        Arc::new(ClusterMap::from_cluster_nodes(text, &me).with_observed_seq(observed_seq))
    }

    /// A refresh that observed the topology earlier must not replace a map
    /// published from a later observation, whatever order they finish in.
    /// The only test that touches the published map.
    #[test]
    fn test_publication_follows_observation_order() {
        let newer = map(ONE_SHARD, u64::MAX - 1);
        assert!(Arc::ptr_eq(
            &publish_if_newer(newer.clone()).unwrap(),
            &newer
        ));

        let older = map(SLOT_GAP, u64::MAX - 2);
        let kept = publish_if_newer(older).unwrap_err();
        assert!(Arc::ptr_eq(&kept, &newer));
        assert!(Arc::ptr_eq(&CLUSTER_MAP.load_full(), &newer));

        let newest = map(SLOT_GAP, u64::MAX);
        publish_if_newer(newest.clone()).unwrap();
        assert!(Arc::ptr_eq(&CLUSTER_MAP.load_full(), &newest));
    }

    #[test]
    fn test_inconsistent_map_is_not_rebuilt_before_it_expires() {
        let inconsistent = map(SLOT_GAP, 1);
        assert!(!inconsistent.is_consistent);
        assert!(!needs_refresh(&inconsistent, false));
        assert!(needs_refresh(&inconsistent, true));
        inconsistent.extend_expiration(0);
        assert!(needs_refresh(&inconsistent, false));
    }

    /// The whole grow/reset lifecycle in one test: the interval is a
    /// process-wide static, so splitting this across tests would race under
    /// the parallel test runner.
    #[test]
    fn test_adaptive_refresh_interval_backoff() {
        let floor = CLUSTER_MAP_EXPIRATION_MS.load(Ordering::Relaxed);
        assert!(floor > 0 && floor * 2 < CLUSTER_MAP_BACKOFF_CAP_MS);

        // Cold start: the static begins at 0, so the first grow lands on the floor.
        CLUSTER_MAP_REFRESH_INTERVAL_MS.store(0, Ordering::Relaxed);
        assert_eq!(grow_refresh_interval(), floor);

        // Doubles per unchanged refresh, then saturates at the cap.
        assert_eq!(grow_refresh_interval(), floor * 2);
        assert_eq!(grow_refresh_interval(), floor * 4);
        let mut prev = floor * 4;
        loop {
            let next = grow_refresh_interval();
            assert!(next <= CLUSTER_MAP_BACKOFF_CAP_MS);
            if next == prev {
                break;
            }
            assert!(next > prev);
            prev = next;
        }
        assert_eq!(prev, CLUSTER_MAP_BACKOFF_CAP_MS);

        // A topology change or build failure resets to the floor.
        reset_refresh_interval();
        assert_eq!(
            CLUSTER_MAP_REFRESH_INTERVAL_MS.load(Ordering::Relaxed),
            floor
        );
        assert_eq!(grow_refresh_interval(), floor * 2);
    }
}
