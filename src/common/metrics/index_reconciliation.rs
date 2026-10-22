//! `index_reconciliation`: the background pass that checks a postings index preloaded from the
//! RDB aux field against the keys that actually loaded, sweeping out dangling ids and re-indexing
//! anything missed (see `series::index::persistence`). It runs once per load that preloaded an
//! index, on its own thread (`ts-index-sweep`).
//!
//! The section is this one task: its lifecycle metrics (see [`super::task`]) carry no task name,
//! so they read `index_reconciliation_started_total`, `index_reconciliation_running`, and so on.

use super::task::{TaskMetrics, register_task};
use metered::Registry;

/// The reconciliation pass after a load that preloaded an index.
pub static RECONCILE: TaskMetrics = TaskMetrics::new("", "ts-index-sweep");

pub(super) fn register(registry: &mut Registry<'static>) {
    register_task(registry, &RECONCILE);
}
