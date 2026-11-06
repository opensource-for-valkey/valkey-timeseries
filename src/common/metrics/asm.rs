//! `asm`: atomic slot migration (Valkey 9.0+). One task with the lifecycle metrics every task has
//! (see [`super::task`]), plus how much work a completed import did. On the importing node, a
//! completion publishes the import's hidden series on the main thread, then `optimize` compacts
//! the posting lists the burst grew. The exporting node needs no task of its own: the server
//! deletes the exported keys through `unlink2`, which retires them from the index.

use super::task::{TaskMetrics, register_task};
use metered::Registry;
use metered::entry::counter;
use std::sync::atomic::AtomicU64;

/// Optimizes the posting lists of the dbs a completed slot import published series into.
pub static OPTIMIZE: TaskMetrics = TaskMetrics::new("optimize", "ts-asm-optimize");

/// Series made visible by the completion of the slot import that hid them.
pub static KEYS_PUBLISHED: AtomicU64 = AtomicU64::new(0);

pub(super) fn register(registry: &mut Registry<'static>) {
    register_task(registry, &OPTIMIZE);
    registry.register(
        counter("keys_published")
            .source(&KEYS_PUBLISHED)
            .help("Series hidden during a slot import and published when it completed"),
    );
}
