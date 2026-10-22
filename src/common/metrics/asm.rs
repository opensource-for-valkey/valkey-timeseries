//! `asm`: atomic slot migration (Valkey 9.0+). Two tasks with the lifecycle metrics every task
//! has (see [`super::task`]), plus how much work each did: on the importing node, `drain` indexes
//! the keys deferred while the import ran; on the exporting node, `post_migration_cleanup` marks
//! the exported series stale.
//!
//! The full stale-ID sweep the cleanup requests afterwards stays in the `task` section, as
//! `task_tombstone_full_sweep_*`: it shares the periodic stale-ID cleanup's single-flight guard
//! and its `tombstones_retired` counter.

use super::task::{TaskMetrics, register_task};
use metered::Registry;
use metered::entry::counter;
use std::sync::atomic::AtomicU64;

/// Indexes the keys deferred during a slot import, once it completes.
pub static DRAIN: TaskMetrics = TaskMetrics::new("drain", "ts-delayed-indexing");
/// Marks series stale on the source node once their slots have been exported.
pub static POST_MIGRATION_CLEANUP: TaskMetrics =
    TaskMetrics::new("post_migration_cleanup", "ts-asm-cleanup");

/// Deferred import keys indexed by `drain`, including batches finished before a shutdown abort.
pub static KEYS_INDEXED: AtomicU64 = AtomicU64::new(0);
/// Exported ids marked stale by `post_migration_cleanup`.
pub static POST_MIGRATION_CLEANUP_IDS_MARKED: AtomicU64 = AtomicU64::new(0);

pub(super) fn register(registry: &mut Registry<'static>) {
    register_task(registry, &DRAIN);
    register_task(registry, &POST_MIGRATION_CLEANUP);
    registry
        .register(
            counter("keys_indexed").source(&KEYS_INDEXED).help(
                "Queued import keys successfully indexed, including progress before shutdown",
            ),
        )
        .register(
            counter("post_migration_cleanup_ids_marked")
                .source(&POST_MIGRATION_CLEANUP_IDS_MARKED)
                .help("Exported ids marked stale by post-migration cleanup"),
        );
}
