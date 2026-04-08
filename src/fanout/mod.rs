pub mod acl;
mod blocked_client;
mod cluster_map;
pub(crate) mod cluster_migrations;
mod cluster_rpc;
mod fanout_client_command;
mod fanout_command;
mod fanout_error;
mod fanout_message;
mod registry;
pub mod serialization;
mod utils;

use crate::config::CLUSTER_MAP_EXPIRATION_MS;
use ahash::HashSet;
use arc_swap::{ArcSwap, Guard};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, LazyLock};
use valkey_module::Context;

use super::fanout::cluster_rpc::register_cluster_message_handlers;
pub use acl::*;
pub use cluster_rpc::get_cluster_command_timeout;
pub use fanout_client_command::*;
pub use fanout_command::*;
pub use fanout_error::*;
pub use utils::*;

pub use cluster_map::{ClusterMap, FanoutTargetMode, NodeInfo};
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

/// Upper bound on the adaptive refresh interval. Replica membership changes
/// alter neither the slot fingerprint (so no peer ever rejects with a
/// mismatch) nor, on remote nodes, any local event: the periodic refresh is
/// the only mechanism that detects them, so the backoff must stay bounded.
const CLUSTER_MAP_BACKOFF_CAP_MS: u64 = 5_000;

/// Current adaptive refresh interval in ms. Starts at (and resets to) the
/// configured expiration, and doubles up to [`CLUSTER_MAP_BACKOFF_CAP_MS`]
/// each time a refresh confirms the topology is unchanged, so a stable
/// cluster converges to one `CLUSTER NODES` call per cap interval while
/// topology churn is re-checked at the configured floor.
static CLUSTER_MAP_REFRESH_INTERVAL_MS: AtomicU64 = AtomicU64::new(0);

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

/// The set of target nodes for a fanout, together with the cluster-map
/// fingerprint of the *same* snapshot the targets were chosen from. Keeping them
/// together avoids a race where the map is swapped between target selection and
/// sending, which would pair a fresh target list with a stale fingerprint.
#[derive(Clone)]
pub struct FanoutTargets {
    pub nodes: Arc<HashSet<NodeInfo>>,
    pub cluster_fingerprint: u64,
}

pub fn get_cluster_map() -> Guard<Arc<ClusterMap>> {
    CLUSTER_MAP.load()
}

/// Mark the local cluster map as stale so it is rebuilt on next use.
pub fn mark_cluster_map_stale() {
    CLUSTER_MAP_STALE.store(true, Ordering::Release);
}

fn take_cluster_map_stale() -> bool {
    CLUSTER_MAP_STALE.swap(false, Ordering::AcqRel)
}

fn update_cluster_map(map: ClusterMap) {
    CLUSTER_MAP.swap(Arc::new(map));
}

pub fn get_fanout_targets(ctx: &Context, mode: FanoutTargetMode) -> FanoutTargets {
    let current_map = CLUSTER_MAP.load();
    // Check if we need to refresh. A stale flag is set when a peer rejected one
    // of our requests due to a topology mismatch.
    let stale = take_cluster_map_stale();
    let needs_refresh = !current_map.is_consistent || current_map.is_expired() || stale;
    if !needs_refresh {
        return FanoutTargets {
            nodes: current_map.get_targets(mode),
            cluster_fingerprint: current_map.cluster_slots_fingerprint(),
        };
    }
    // Possibly race condition, but only if called concurrently, which is possible but very unlikely.
    // In any case, the worst that can happen is that we refresh more than once.
    refresh_cluster_map(ctx);
    // Load the refreshed map once so targets and fingerprint come from the same snapshot.
    let map = CLUSTER_MAP.load();
    FanoutTargets {
        nodes: map.get_targets(mode),
        cluster_fingerprint: map.cluster_slots_fingerprint(),
    }
}

// Refresh the cluster map by creating a new one from the current cluster state.
// When the rebuild shows the topology is unchanged, the published map is kept
// and its expiration extended instead of swapping in an identical copy: this
// preserves the lazily-computed target caches and the allocations shared by
// in-flight readers. If building the new map fails, the previous map is kept
// in place and a warning is logged so subsequent calls will retry the refresh.
pub fn refresh_cluster_map(ctx: &Context) {
    ctx.log_notice("Refreshing cluster map...");
    match ClusterMap::create(ctx) {
        Some(new_map) => {
            let current_map = CLUSTER_MAP.load();
            if new_map.is_consistent
                && current_map.is_consistent
                && new_map.same_topology(&current_map)
            {
                current_map.extend_expiration(grow_refresh_interval());
                ctx.log_notice("Cluster map unchanged; extended expiration");
            } else {
                drop(current_map);
                reset_refresh_interval();
                update_cluster_map(new_map);
                ctx.log_notice("Cluster map refreshed");
            }
        }
        None => {
            reset_refresh_interval();
            ctx.log_warning(
                "Failed to build cluster map; keeping the previous map and retrying later",
            );
        }
    }
}

pub fn get_or_refresh_cluster_map(ctx: &Context) -> Arc<ClusterMap> {
    let current_map = get_cluster_map();
    let stale = take_cluster_map_stale();

    // Check if we need to refresh
    let needs_refresh = !current_map.is_consistent || current_map.is_expired() || stale;

    if needs_refresh {
        drop(current_map);
        refresh_cluster_map(ctx);
        return get_cluster_map().clone();
    }

    current_map.clone()
}

#[cfg(test)]
mod tests {
    use super::*;

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
