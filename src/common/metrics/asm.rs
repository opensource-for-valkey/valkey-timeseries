//! `asm`: atomic slot migration (Valkey 9.0+). One task with the lifecycle metrics every task
//! has (see [`super::task`]), plus how much work it did: on the importing node, `drain` indexes
//! the keys deferred while the import ran. The exporting node needs no task of its own — the
//! server deletes the exported keys through `unlink2`, which retires them from the index.

use super::task::{TaskMetrics, register_task};
use metered::Registry;
use metered::entry::counter;
use std::sync::atomic::AtomicU64;

/// Indexes the keys deferred during a slot import, once it completes.
pub static DRAIN: TaskMetrics = TaskMetrics::new("drain", "ts-delayed-indexing");

/// Deferred import keys indexed by `drain`, including batches finished before a shutdown abort.
pub static DRAIN_KEYS_INDEXED: AtomicU64 = AtomicU64::new(0);

pub(super) fn register(registry: &mut Registry<'static>) {
    register_task(registry, &DRAIN);
    registry.register(
        counter("drain_keys_indexed")
            .source(&DRAIN_KEYS_INDEXED)
            .help("Queued import keys successfully indexed, including progress before shutdown"),
    );
}
