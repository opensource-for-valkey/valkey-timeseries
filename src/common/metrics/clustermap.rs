//! `clustermap`: the cluster map fanouts pick their targets from. Counted in `fanout` (`mod.rs`
//! for refreshes and staleness, `cluster_rpc` for the refresh a peer request can force).

use crate::fanout::{cluster_map_age, cluster_map_refresh_interval_ms};
use metered::Registry;
use metered::entry::{counter, gauge_value};
use std::sync::atomic::AtomicU64;

/// Rebuilds of the cluster map from `CLUSTER NODES`, whatever their outcome.
pub static REFRESHES: AtomicU64 = AtomicU64::new(0);
/// Rebuilds that found the topology unchanged: the published map was kept, its expiry extended.
pub static REFRESH_UNCHANGED: AtomicU64 = AtomicU64::new(0);
/// Rebuilds that published a new map (the topology changed, or a map was inconsistent).
pub static REFRESH_CHANGED: AtomicU64 = AtomicU64::new(0);
/// Rebuilds that failed; the previous map stays in place.
pub static REFRESH_FAILURES: AtomicU64 = AtomicU64::new(0);
/// Rebuilds forced on a serving worker because a peer's request carried a different fingerprint.
pub static FORCED_REFRESHES: AtomicU64 = AtomicU64::new(0);
/// Times a peer's cluster-map mismatch error marked this node's map stale.
pub static STALE_MARKS: AtomicU64 = AtomicU64::new(0);

static COUNTERS: &[(&str, &AtomicU64, &str)] = &[
    (
        "refreshes",
        &REFRESHES,
        "Cluster map rebuilds from CLUSTER NODES, whatever their outcome",
    ),
    (
        "refresh_unchanged",
        &REFRESH_UNCHANGED,
        "Cluster map rebuilds that found the topology unchanged (expiry extended)",
    ),
    (
        "refresh_changed",
        &REFRESH_CHANGED,
        "Cluster map rebuilds that published a new map",
    ),
    (
        "refresh_failures",
        &REFRESH_FAILURES,
        "Cluster map rebuilds that failed, leaving the previous map in place",
    ),
    (
        "forced_refreshes",
        &FORCED_REFRESHES,
        "Cluster map rebuilds forced by a peer request carrying a different fingerprint",
    ),
    (
        "stale_marks",
        &STALE_MARKS,
        "Times a peer's cluster-map mismatch error marked this node's map stale",
    ),
];

pub(super) fn register(registry: &mut Registry<'static>) {
    for &(name, source, help) in COUNTERS {
        registry.register(counter(name).source(source).help(help));
    }
    registry
        .register(
            gauge_value("refresh_interval_seconds")
                .read(|_: &()| cluster_map_refresh_interval_ms() as f64 / 1000.0)
                .help("Current adaptive cluster map refresh interval (0 until the first refresh)")
                .unit("seconds"),
        )
        .register(
            gauge_value("age_seconds")
                .read(|_: &()| cluster_map_age().map_or(-1.0, |age| age.as_secs_f64()))
                .help("Time since the cluster map was last built or confirmed unchanged (-1 before the first build)")
                .unit("seconds"),
        );
}
