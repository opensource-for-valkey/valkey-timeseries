//! Sorted bulk-build fallback for index rebuild during load
//!
//! When a load window opens and a db's index was NOT preloaded from the RDB aux payload
//! (payload absent, version-mismatched, corrupt, or `ts-index-persist` off at save time), the
//! per-key rebuild pays O(labels) ART lookups and bitmap adds for every `loaded` key. This
//! module defers that work: each loaded series is buffered as an `(id, key, label-keys)` tuple,
//! and at `LoadingSubevent::Ended` the buffer is drained with one sorted bulk build per db
//! ([`Postings::bulk_index`]): sorted-order ART inserts and one `add_many` per posting list.
//!
//! Scope: only RDB (`RdbStarted`) and replication full-sync (`ReplStarted`) windows. An AOF
//! load is excluded because its command tail replays into a keyspace whose buffered keys are
//! not indexed yet — a `DEL` or `RENAME` of a buffered key followed by re-creation would leave
//! a dangling id at drain time. Pure RDB/repl windows execute no commands, so nothing can
//! mutate a buffered key before the drain. For the same reason the drain at `Ended` runs
//! synchronously on the main thread: the index must be complete before serving resumes,
//! matching the per-key path's guarantee.
//!
//! Memory bounding: the buffer is transient overhead on top of a keyspace that is itself still
//! filling, so it is capped by `ts-index-build-max-memory` (bytes, 0 = unlimited). On
//! crossing the cap the buffer is drained immediately (the sorted work already collected is not
//! wasted) and indexing degrades to the per-key path for the remainder of the load window.

use super::postings::BulkIndexEntry;
use super::{IndexKey, get_db_index};
use crate::common::context::create_key_string;
use crate::common::logging::{log_debug, log_notice};
use crate::common::sync::lock;
use crate::config::index_build_max_memory;
use crate::labels::InternedLabel;
use crate::series::get_timeseries_mut;
use blart::AsBytes;
use std::collections::BTreeMap;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use valkey_module::Context;

/// Per-db buffered entries for the current load window. All access is from the main thread
/// (`loaded` notifications and loading events); the mutex only makes the static `Sync`.
static BULK_BUFFERS: Mutex<BTreeMap<i32, Vec<BulkIndexEntry>>> = Mutex::new(BTreeMap::new());

/// Estimated heap footprint of everything in `BULK_BUFFERS` (see [`entry_footprint`]).
static BULK_BUFFER_BYTES: AtomicUsize = AtomicUsize::new(0);

/// Buffering is engaged for the current load window (RDB/repl load in progress).
static BULK_ACTIVE: AtomicBool = AtomicBool::new(false);

/// The memory cap was crossed: the buffer has been drained and the remainder of the load
/// window uses the per-key path.
static BULK_DEGRADED: AtomicBool = AtomicBool::new(false);

/// Loading `*Started`: engage buffering for RDB/repl windows; AOF keeps the per-key path
/// (see the module docs for why).
pub(crate) fn on_load_started(is_aof: bool) {
    BULK_ACTIVE.store(!is_aof, Ordering::Relaxed);
    BULK_DEGRADED.store(false, Ordering::Relaxed);
    // Defensive: leftover state from an earlier load cycle is stale by now.
    lock(&BULK_BUFFERS).clear();
    BULK_BUFFER_BYTES.store(0, Ordering::Relaxed);
}

/// Loading `Ended`: drain the buffer into the per-db indexes. Runs synchronously on the main
/// thread so the index is complete before the server resumes serving.
pub(crate) fn on_load_ended() {
    if !BULK_ACTIVE.swap(false, Ordering::Relaxed) {
        return;
    }
    drain_buffers("load end");
    BULK_DEGRADED.store(false, Ordering::Relaxed);
}

/// Loading `Failed`: the keyspace state is engine-dependent (startup aborts; a replica may
/// restore its pre-sync dataset), so drop the buffer — nothing from it was indexed, which is
/// strictly safer than the per-key path (which would already have indexed those keys).
pub(crate) fn on_load_failed() {
    BULK_ACTIVE.store(false, Ordering::Relaxed);
    BULK_DEGRADED.store(false, Ordering::Relaxed);
    let discarded = {
        let mut buffers = lock(&BULK_BUFFERS);
        let n: usize = buffers.values().map(Vec::len).sum();
        buffers.clear();
        n
    };
    BULK_BUFFER_BYTES.store(0, Ordering::Relaxed);
    if discarded > 0 {
        log_notice(format!(
            "Load failed; discarded {discarded} series buffered for bulk index build"
        ));
    }
}

/// Buffers one `loaded` key for the end-of-load bulk build. Returns `true` when the key was
/// handled here (buffered, or not a live timeseries key — the per-key path would also index
/// nothing for those); `false` when buffering is inactive for this window (AOF load, memory cap
/// crossed) and the caller should fall through to `index_series_by_key`.
pub(crate) fn try_buffer_loaded_key(ctx: &Context, db: i32, key: &[u8]) -> bool {
    if !BULK_ACTIVE.load(Ordering::Relaxed) || BULK_DEGRADED.load(Ordering::Relaxed) {
        return false;
    }

    let valkey_key = create_key_string(ctx, key);
    let Ok(Some(mut series)) = get_timeseries_mut(ctx, &valkey_key, false, None) else {
        return true;
    };
    series._db = Some(db);

    let label_keys: Vec<IndexKey> = series
        .labels
        .iter()
        .map(|InternedLabel { name, value }| IndexKey::for_label_value(name, value))
        .collect();
    let entry = BulkIndexEntry {
        id: series.id,
        key: key.to_vec().into_boxed_slice(),
        label_keys,
    };

    buffer_entry(db, entry);
    true
}

/// Appends `entry` to `db`'s buffer, tracking the memory estimate; on crossing
/// `ts-index-build-max-memory`, drains everything collected so far and degrades to the
/// per-key path for the remainder of the load window.
fn buffer_entry(db: i32, entry: BulkIndexEntry) {
    let footprint = entry_footprint(&entry);
    let total = BULK_BUFFER_BYTES.fetch_add(footprint, Ordering::Relaxed) + footprint;
    lock(&BULK_BUFFERS).entry(db).or_default().push(entry);

    let max_bytes = index_build_max_memory();
    if max_bytes > 0 && total > max_bytes as usize {
        log_notice(format!(
            "Bulk index build buffer reached {total} bytes (ts-index-build-max-memory = \
             {max_bytes}); draining now and switching to per-key indexing for the rest of the load"
        ));
        BULK_DEGRADED.store(true, Ordering::Relaxed);
        drain_buffers("memory cap");
    }
}

/// Estimated heap footprint of one buffered entry: key bytes + label-key bytes (each IndexKey
/// carries a NUL sentinel, included via `as_bytes`) + fixed per-allocation bookkeeping.
fn entry_footprint(entry: &BulkIndexEntry) -> usize {
    size_of::<BulkIndexEntry>()
        + entry.key.len()
        + entry
            .label_keys
            .iter()
            .map(|k| size_of::<IndexKey>() + k.as_bytes().len())
            .sum::<usize>()
}

/// Drains every db's buffer with one sorted bulk build each. Main-thread only (loaded
/// notification or loading-event handler), so no shutdown coordination is needed.
fn drain_buffers(reason: &str) {
    let buffers = std::mem::take(&mut *lock(&BULK_BUFFERS));
    BULK_BUFFER_BYTES.store(0, Ordering::Relaxed);

    for (db, entries) in buffers {
        if entries.is_empty() {
            continue;
        }
        let count = entries.len();
        let start = std::time::Instant::now();
        let index = get_db_index(db);
        index.get_postings_mut().bulk_index(entries);
        log_debug(format!(
            "Bulk index build for db {db} ({reason}): {count} series indexed in {:?}",
            start.elapsed()
        ));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serial_test::serial;

    fn entry(id: u64, key: &str, labels: &[(&str, &str)]) -> BulkIndexEntry {
        BulkIndexEntry {
            id,
            key: key.as_bytes().to_vec().into_boxed_slice(),
            label_keys: labels
                .iter()
                .map(|(name, value)| IndexKey::for_label_value(name, value))
                .collect(),
        }
    }

    /// Drives the buffer/drain lifecycle directly (the ctx-dependent keyspace lookup in
    /// `try_buffer_loaded_key` is exercised by the Python integration suite). Uses a dedicated
    /// db number so the shared `TIMESERIES_INDEX` static doesn't collide with other tests.
    #[test]
    #[serial(bulk_build)]
    fn test_buffer_and_drain_on_load_end() {
        const DB: i32 = 240;
        on_load_started(false);
        assert!(BULK_ACTIVE.load(Ordering::Relaxed));

        buffer_entry(DB, entry(1, "key1", &[("host", "a"), ("job", "web")]));
        buffer_entry(DB, entry(2, "key2", &[("host", "b"), ("job", "web")]));
        assert!(BULK_BUFFER_BYTES.load(Ordering::Relaxed) > 0);

        on_load_ended();
        assert_eq!(BULK_BUFFER_BYTES.load(Ordering::Relaxed), 0);

        let index = get_db_index(DB);
        let postings = index.get_postings();
        assert_eq!(postings.count(), 2);
        let web = postings.postings_for_label_value("job", "web");
        assert!(web.contains(1) && web.contains(2));
        assert!(postings.postings_for_label_value("host", "a").contains(1));
        assert_eq!(
            postings.get_key_by_id(2).map(|k| k.as_ref()),
            Some(&b"key2"[..])
        );
    }

    #[test]
    #[serial(bulk_build)]
    fn test_aof_window_disables_buffering() {
        on_load_started(true);
        assert!(!BULK_ACTIVE.load(Ordering::Relaxed));
        on_load_ended();
    }

    #[test]
    #[serial(bulk_build)]
    fn test_load_failed_discards_buffer() {
        const DB: i32 = 241;
        on_load_started(false);
        buffer_entry(DB, entry(10, "doomed", &[("host", "x")]));
        on_load_failed();

        assert_eq!(BULK_BUFFER_BYTES.load(Ordering::Relaxed), 0);
        let index = get_db_index(DB);
        assert_eq!(index.get_postings().count(), 0);
    }

    #[test]
    #[serial(bulk_build)]
    fn test_memory_cap_drains_and_degrades() {
        use crate::config::INDEX_BUILD_MAX_MEMORY;
        const DB: i32 = 242;

        let saved = INDEX_BUILD_MAX_MEMORY.load(Ordering::Relaxed);
        // Cap below one entry's footprint: the first buffered entry crosses it.
        INDEX_BUILD_MAX_MEMORY.store(1, Ordering::Relaxed);

        on_load_started(false);
        buffer_entry(DB, entry(20, "key20", &[("host", "cap")]));

        // The cap crossing drained immediately and degraded the window.
        assert!(BULK_DEGRADED.load(Ordering::Relaxed));
        assert_eq!(BULK_BUFFER_BYTES.load(Ordering::Relaxed), 0);
        assert!(
            get_db_index(DB)
                .get_postings()
                .postings_for_label_value("host", "cap")
                .contains(20)
        );

        INDEX_BUILD_MAX_MEMORY.store(saved, Ordering::Relaxed);
        on_load_ended();
    }
}
