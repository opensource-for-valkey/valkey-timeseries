//! Atomic slot migration (ASM, Valkey 9.0+) support.
//!
//! Subscribes to the server's ASM events and keeps the secondary index consistent with them. On
//! the destination, indexing of imported keys is deferred until their import completes (so reads
//! never see phantom series of an import that may still abort); on the source, the index entries
//! of exported slots are retired once the export completes.
use crate::common::context::{create_key_string, get_current_db, set_current_db};
use crate::common::hash::BuildNoHashHasher;
use crate::common::logging::{log_debug, log_notice};
use crate::common::module_options::{HANDLE_ATOMIC_SLOT_MIGRATION, declare_module_options};
use crate::common::sync::{lock, read_lock, write_lock};
#[cfg(test)]
use crate::fanout::NUM_SLOTS;
use crate::fanout::{is_clustered, mark_cluster_map_stale};
use crate::series::index::{TIMESERIES_INDEX, get_db_index, index_imported_series};
use crate::series::series_data_type::VK_TIME_SERIES_TYPE;
use crate::series::tasks::remove_all_stale_series_internal;
use crate::series::{SeriesRef, TimeSeries};
use range_set_blaze::RangeSetBlaze;
use std::ffi::{c_char, c_int, c_void};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{LazyLock, Mutex, RwLock};
use valkey_module::{Context, MODULE_CONTEXT, Version, raw};

const ASM_MINIMUM_VERSION: Version = Version {
    major: 9,
    minor: 0,
    patch: 0,
};

fn supports_atomic_slot_migration(ctx: &Context) -> bool {
    if !is_clustered(ctx) {
        return false;
    }
    match ctx.get_server_version() {
        Err(e) => {
            ctx.log_warning(&format!("Error getting server version: {e}"));
            false
        }
        Ok(ver) => {
            // Compare lexicographically; comparing each component independently would, for
            // example, reject 10.0.0 against a 9.1.0 minimum.
            (ver.major, ver.minor, ver.patch)
                >= (
                    ASM_MINIMUM_VERSION.major,
                    ASM_MINIMUM_VERSION.minor,
                    ASM_MINIMUM_VERSION.patch,
                )
        }
    }
}

/// Parse a slot ranges string into a vector of inclusive ranges.
///
/// Examples accepted:
/// - "0-100"
/// - "0-100 200-300"
/// - "0-100,200-300"
/// - "5" (single slot)
#[cfg(test)]
fn parse_slot_ranges(s: &str) -> Result<RangeSetBlaze<u16>, String> {
    let mut out = RangeSetBlaze::new();
    let trimmed = s.trim();
    if trimmed.is_empty() {
        return Ok(out);
    }

    // Accept spaces or commas as separators
    for token in trimmed
        .split(|c: char| c.is_whitespace() || c == ',')
        .filter(|t| !t.is_empty())
    {
        let token = token.trim();
        // Expect either "start-end" or a single number
        if let Some(pos) = token.find('-') {
            let (a, b) = token.split_at(pos);
            let start_str = a.trim();
            let end_str = b[1..].trim(); // skip '-'
            let start: u16 = start_str
                .parse()
                .map_err(|e| format!("Invalid start slot '{start_str}': {e}"))?;
            let end: u16 = end_str
                .parse()
                .map_err(|e| format!("Invalid end slot '{end_str}': {e}"))?;
            if start > end {
                return Err(format!(
                    "Start slot {} greater than end slot {}",
                    start, end
                ));
            }
            if end >= NUM_SLOTS {
                return Err(format!(
                    "End slot {} out of range (must be < {})",
                    end, NUM_SLOTS
                ));
            }
            out.ranges_insert(start..=end);
        } else {
            // single slot
            let v: u16 = token
                .parse()
                .map_err(|e| format!("Invalid slot '{}': {e}", token))?;
            if v >= NUM_SLOTS {
                return Err(format!("Slot {} out of range (must be < {})", v, NUM_SLOTS));
            }
            out.ranges_insert(v..=v);
        }
    }

    Ok(out)
}

// symbolic constants for atomic slot migration subevents (from valkeymodule.h).
// Prefer to read the event id from the `raw` bindings when available; otherwise
// fall back to the numeric constant below.
const VALKEYMODULE_EVENT_ATOMIC_SLOT_MIGRATION: u64 = 19u64;
const VALKEYMODULE_SUBEVENT_ATOMIC_SLOT_MIGRATION_IMPORT_STARTED: u64 = 0;
#[allow(dead_code)] // mirrors valkeymodule.h; export start is not acted on
const VALKEYMODULE_SUBEVENT_ATOMIC_SLOT_MIGRATION_EXPORT_STARTED: u64 = 1;
const VALKEYMODULE_SUBEVENT_ATOMIC_SLOT_MIGRATION_IMPORT_ABORTED: u64 = 2;
#[allow(dead_code)] // mirrors valkeymodule.h; export abort is not acted on
const VALKEYMODULE_SUBEVENT_ATOMIC_SLOT_MIGRATION_EXPORT_ABORTED: u64 = 3;
const VALKEYMODULE_SUBEVENT_ATOMIC_SLOT_MIGRATION_IMPORT_COMPLETED: u64 = 4;
const VALKEYMODULE_SUBEVENT_ATOMIC_SLOT_MIGRATION_EXPORT_COMPLETED: u64 = 5;

const VALKEYMODULE_NODE_ID_LEN: usize = 40;

#[derive(Debug)]
enum AtomicSlotMigrationEvent {
    ImportStarted,
    #[allow(dead_code)] // mirrors the server subevent; not raised yet
    ExportStarted,
    ImportAborted,
    #[allow(dead_code)] // mirrors the server subevent; not raised yet
    ExportAborted,
    ImportCompleted,
    ExportCompleted,
}

#[repr(C)]
#[derive(Debug, Copy, Clone)]
struct ValkeyModuleSlotRange {
    start: c_int, // Start slot, inclusive.
    end: c_int,   // End slot, inclusive.
}

#[repr(C)]
#[derive(Debug, Copy, Clone)]
struct ValkeyModuleAtomicSlotMigrationInfoV1 {
    version: u64, // Version of this structure for ABI compat.
    job_name: [c_char; VALKEYMODULE_NODE_ID_LEN + 1], // Unique ID for the migration operation.
    slot_ranges: *mut ValkeyModuleSlotRange, // Array of slot ranges involved in the migration.
    num_slot_ranges: u32, // Number of slot ranges in the array.
}

impl ValkeyModuleAtomicSlotMigrationInfoV1 {
    fn convert_slot_ranges(&self) -> RangeSetBlaze<u16> {
        let mut ranges = RangeSetBlaze::new();
        self.extend_slot_ranges(&mut ranges);
        ranges
    }

    fn extend_slot_ranges(&self, dest: &mut RangeSetBlaze<u16>) {
        for i in 0..self.num_slot_ranges {
            unsafe {
                let range = *self.slot_ranges.add(i as usize);
                let start = range.start as u16;
                let end = range.end as u16;
                dest.extend(start..=end);
            }
        }
    }
}

type ValkeyModuleAtomicSlotMigrationInfo = ValkeyModuleAtomicSlotMigrationInfoV1;

unsafe extern "C" fn on_atomic_slot_migration_event(
    _ctx: *mut raw::RedisModuleCtx,
    _eid: raw::RedisModuleEvent,
    sub_event: u64,
    data: *mut c_void,
) {
    fn raise_event(event: AtomicSlotMigrationEvent, data: *mut c_void) {
        let info = unsafe { &*(data as *const ValkeyModuleAtomicSlotMigrationInfo) };
        slot_migration_event_handler(event, info.convert_slot_ranges());
    }

    match sub_event {
        VALKEYMODULE_SUBEVENT_ATOMIC_SLOT_MIGRATION_IMPORT_STARTED => {
            mark_cluster_map_stale();
            raise_event(AtomicSlotMigrationEvent::ImportStarted, data);
        }
        VALKEYMODULE_SUBEVENT_ATOMIC_SLOT_MIGRATION_IMPORT_COMPLETED => {
            mark_cluster_map_stale();
            raise_event(AtomicSlotMigrationEvent::ImportCompleted, data);
        }
        VALKEYMODULE_SUBEVENT_ATOMIC_SLOT_MIGRATION_IMPORT_ABORTED => {
            mark_cluster_map_stale();
            raise_event(AtomicSlotMigrationEvent::ImportAborted, data);
        }
        VALKEYMODULE_SUBEVENT_ATOMIC_SLOT_MIGRATION_EXPORT_COMPLETED => {
            mark_cluster_map_stale();
            raise_event(AtomicSlotMigrationEvent::ExportCompleted, data);
        }
        _ => {}
    }
}

fn subscribe_to_atomic_slot_migration_events(ctx: &Context) {
    let res = unsafe {
        raw::RedisModule_SubscribeToServerEvent.unwrap()(
            ctx.ctx,
            raw::RedisModuleEvent {
                id: VALKEYMODULE_EVENT_ATOMIC_SLOT_MIGRATION,
                dataver: 1,
            },
            Some(on_atomic_slot_migration_event),
        )
    };
    if res != raw::REDISMODULE_OK as i32 {
        ctx.log_warning("Failed to subscribe to atomic slot migration events");
    }
    // Declare that this module handles atomic slot migration events, so the
    // server will permit ASM operations involving this module. Declared through
    // `declare_module_options` because `SetModuleOptions` takes the complete mask:
    // passing this flag alone would drop the options declared at init.
    declare_module_options(ctx, HANDLE_ATOMIC_SLOT_MIGRATION);
}

const BATCH_SIZE: usize = 256;

/// Collects indexed keys that are pending indexing for each database. This is used during slot migrations
/// to track keys that have been imported but not yet indexed, so we can ensure they are indexed after the migration completes.
/// Doing this at the end of the migration process
/// - to ensure that we have a complete view of all
/// - avoids the complication of filtering out "phantom keys" in the read path during migration
/// - indexes them efficiently in batches after the migration completes.
type DelayedKeysMap = papaya::HashMap<i32, RwLock<Vec<Box<[u8]>>>, BuildNoHashHasher<i32>>;

static DELAYED_KEYS_MAP: LazyLock<DelayedKeysMap> = LazyLock::new(DelayedKeysMap::default);
static DELAYED_KEYS_COUNTER: AtomicUsize = AtomicUsize::new(0);

pub(super) fn clear_delayed_keys_map() {
    DELAYED_KEYS_MAP.pin().clear();
}

pub(super) fn clear_delayed_keys_in_db(db: i32) {
    let pending_keys = DELAYED_KEYS_MAP.pin();
    pending_keys.remove(&db);
}

pub(crate) fn add_delayed_indexing_key(db: i32, key: &[u8]) {
    let pending_keys = DELAYED_KEYS_MAP.pin();

    let converted_key = key.to_vec().into_boxed_slice();
    write_lock(pending_keys.get_or_insert_with(db, || RwLock::new(Vec::with_capacity(64))))
        .push(converted_key);

    let count = DELAYED_KEYS_COUNTER.fetch_add(1, Ordering::Relaxed) + 1;
    // log every 100 queued keys to avoid excessive noise
    if count.is_multiple_of(100) {
        log_debug(format!(
            "ASM queued delayed indexing keys: total_queued={count}, db={db}"
        ));
    }
}

/// Indexes a batch of imported keys. Returns the number of keys that had no timeseries value at
/// index time (already deleted, wrong type, or import aborted).
fn index_timeseries_in_batch(db: i32, batch: &[Box<[u8]>]) -> usize {
    let mut skipped = 0usize;

    log_debug(format!(
        "ASM indexing batch: db={}, batch_size={}",
        db,
        batch.len()
    ));

    let ctx = MODULE_CONTEXT.lock();
    let save_db = get_current_db(&ctx);
    set_current_db(&ctx, db);

    // Open every key *before* taking the postings write lock. Opening a key runs lazy
    // expiry, which reaps an expired series through the `unlink` callback — and that takes
    // the same write lock on this thread, so opening under the lock would hang the server
    // with the GIL held (see `get_series_by_id`).
    let opened: Vec<_> = batch
        .iter()
        .map(|key_name| {
            let valkey_key = create_key_string(&ctx, key_name.as_ref());
            let writeable_key = ctx.open_key_writable(&valkey_key);
            (key_name, writeable_key)
        })
        .collect();

    let index = get_db_index(db);
    let mut postings = write_lock(&index.inner);

    for (key_name, writeable_key) in opened.iter() {
        let Ok(Some(series)) = writeable_key.get_value::<TimeSeries>(&VK_TIME_SERIES_TYPE) else {
            skipped += 1;
            continue;
        };
        series._db = Some(db);
        // Imported ids come from another node's id space: remap a collision rather than
        // merging two series' postings. Compaction links name keys, so they survive it.
        index_imported_series(&mut postings, series, key_name.as_ref());
    }
    drop(postings);
    drop(opened);

    set_current_db(&ctx, save_db);
    skipped
}

/// Removes and returns the keys queued for `db` whose hash slot is in `slots`, leaving every
/// other queued key — another import's — where it is.
fn take_delayed_keys_in_slots(db: i32, slots: &RangeSetBlaze<u16>) -> Vec<Box<[u8]>> {
    let pending_keys = DELAYED_KEYS_MAP.pin();
    let Some(lock) = pending_keys.get(&db) else {
        return Vec::new();
    };
    // The entry itself stays (possibly empty) so a concurrent append cannot be lost.
    let mut guard = write_lock(lock);
    let (taken, kept): (Vec<_>, Vec<_>) = std::mem::take(&mut *guard)
        .into_iter()
        .partition(|key| slots.contains(crate::fanout::calculate_hash_slot(key)));
    *guard = kept;
    taken
}

fn process_delayed_keys_for_db(db: i32, slots: &RangeSetBlaze<u16>) {
    // Take ownership of this import's queued keys so we can index without holding the write
    // lock. Keys of an import still in progress stay queued for its own completion.
    let keys_vec = take_delayed_keys_in_slots(db, slots);

    if keys_vec.is_empty() {
        return;
    }

    let total = keys_vec.len();
    let mut indexed = 0usize;
    let mut skipped = 0usize;
    for batch in keys_vec.chunks(BATCH_SIZE) {
        // Once dequeued above, a key is only indexed here — bailing out mid-drain leaves the
        // remaining keys in this db un-indexed. That's acceptable on shutdown (the process is
        // about to exit; a restart re-derives the index from the RDB/AOF), but not otherwise, so
        // this only checks the shutdown flag, not e.g. a generic cancellation.
        if crate::is_shutting_down() {
            log_debug(format!(
                "ASM delayed indexing for db={db} aborted by shutdown; {} of {total} key(s) left un-indexed",
                total - indexed
            ));
            return;
        }
        indexed += batch.len();
        skipped += index_timeseries_in_batch(db, batch);
    }

    if skipped > 0 {
        // A skipped key had no timeseries value at drain time; this is terminal (retrying would
        // never succeed), so we drop it rather than re-queueing it forever.
        log_debug(format!(
            "ASM delayed indexing dropped {skipped} key(s) with no series value in db={db}"
        ));
    }
}

/// Indexes the keys queued during the import of `slots`, which has just completed.
///
/// There is no "already running" guard: a completion that found one drain in flight used to
/// skip its own, and with each drain now taking only its own import's keys, that would strand
/// them. Concurrent drains touch disjoint keys.
fn process_delayed_indexing(slots: RangeSetBlaze<u16>) {
    let pending_keys = DELAYED_KEYS_MAP.pin();
    let mut dbs: Vec<i32> = pending_keys.keys().copied().collect();
    dbs.sort_unstable();

    log_debug(format!(
        "ASM delayed indexing drain scheduled: dbs={}, slot_ranges={}",
        dbs.len(),
        slots.ranges_len()
    ));

    // On its own thread rather than the pool because it takes the module lock (see
    // `spawn_background`), and checks `is_shutting_down()` between dbs — an aborted drain
    // leaves the remaining dbs' keys un-indexed, which is fine on shutdown (see
    // `process_delayed_keys_for_db`) but must not happen otherwise.
    crate::common::threads::spawn_background("ts-delayed-indexing", move || {
        for db in dbs {
            if crate::is_shutting_down() {
                log_debug("ASM delayed indexing drain aborted by shutdown");
                return;
            }
            process_delayed_keys_for_db(db, &slots);
        }
        log_debug("ASM delayed indexing drain finished");
    });
}

/// Removes keys from the index that are not owned by the given shard. This is used during shard migrations on
/// the source to clean up keys that have moved to a different shard.
fn remove_non_owned_keys(db: i32, source_slots: &RangeSetBlaze<u16>) -> usize {
    let mut batch: Vec<SeriesRef> = Vec::with_capacity(BATCH_SIZE);

    fn flush(db: i32, batch: &mut Vec<SeriesRef>) -> bool {
        if batch.is_empty() {
            return false;
        }
        let index = get_db_index(db);
        let mut postings = write_lock(&index.inner);
        postings.mark_ids_as_stale(batch);
        batch.clear();
        true
    }

    let mut deleted_count = 0usize;
    let mut cursor: u64 = 0;

    log_notice(format!(
        "ASM remove_non_owned_keys starting db={}, source_slots_count={}",
        db,
        source_slots.iter().count()
    ));

    loop {
        let index = get_db_index(db);
        // Acquire a read lock to iterate a window of keys starting at `cursor`.
        // We must drop this read lock before acquiring the write lock inside `flush` to avoid deadlocks.
        let postings_read = read_lock(&index.inner);

        let mut processed = 0usize;
        for (id, key) in postings_read.id_to_key.range(cursor..).take(BATCH_SIZE) {
            let slot = crate::fanout::calculate_hash_slot(key.as_ref());

            // Advance cursor for every visited entry so the iterator makes progress
            // even when entries are skipped.
            cursor = *id + 1;

            if !source_slots.contains(slot) {
                // This key is outside the migration range; skip it.
                processed += 1;
                continue;
            }

            batch.push(*id);
            deleted_count += 1;
            processed += 1;

            // If we've reached the batch threshold, break so we can drop the read guard
            // and flush the batch without moving the guard.
            if batch.len() >= BATCH_SIZE {
                break;
            }
        }

        // Drop the read lock before any potential write lock acquisition
        drop(postings_read);

        if batch.len() >= BATCH_SIZE {
            log_debug(format!(
                "ASM flush (batch full) db={} batch_len={} cursor={} deleted_so_far={}",
                db,
                batch.len(),
                cursor,
                deleted_count
            ));
            flush(db, &mut batch);
            // Continue outer loop to reacquire fresh read lock and resume
            continue;
        }

        if !batch.is_empty() {
            log_debug(format!(
                "ASM flush (final) db={} batch_len={} cursor={} deleted_so_far={}",
                db,
                batch.len(),
                cursor,
                deleted_count
            ));
            flush(db, &mut batch);
        }

        if processed == 0 {
            break; // No more entries to process
        }
    }

    deleted_count
}

/// Handles post-migration cleanup for the source shard by removing keys that are no longer owned after a successful migration.
/// Once a migration succeeds and the cluster topology updates, the slots are no longer owned by the source shard.
///
/// ## Primary Source Shard Handling
/// Valkey primaries automatically clean up unowned keys in the background, which calls standard engine deletion routines.
/// We need to ensure that the timeseries index is also cleaned up accordingly by marking the relevant series as stale in the index,
/// which will prevent them from being returned in queries and allow them to be cleaned up lazily over time as they are accessed,
/// or when the index performs maintenance.
/// The event triggering this is only received on the source primary shard.
///
/// ## Source Replica Handling
/// Because the source replicas are completely blind to the export event state machine, we cannot rely on module events there.
///
/// These deletions are natively propagated down the replication stream as standard DEL or UNLINK commands to the source replicas. We handle these events already
/// by subscribing to the Keyspace Notification hooks in `server_events` to handle the standard data eviction/deletion hooks.
///
/// In other words, we only need special handling for the source primary node.
/// ## Possible Future Optimization
/// Check if the source_slots covers all slots the current node is responsible for (or a full reshard) and if so, we can just clear
/// the entire index instead.
fn handle_post_migration_cleanup(source_slots: RangeSetBlaze<u16>) {
    let slots_count = source_slots.iter().count();
    log_notice(format!(
        "ASM post-migration cleanup scheduled: source_slots_count={slots_count}"
    ));

    // Spawn a background task so we don't block the main thread; this can take a while if there are
    // a lot of keys to clean up. Off the pool: it takes the module lock (see `spawn_background`).
    crate::common::threads::spawn_background("ts-asm-cleanup", move || {
        let index = TIMESERIES_INDEX.pin();
        let mut dbs: Vec<i32> = index.keys().copied().collect();
        dbs.sort_unstable();
        let db_count = dbs.len();
        drop(index);

        log_notice(format!(
            "ASM post-migration cleanup starting: db_count={db_count} slots_count={slots_count}"
        ));

        let mut deleted_count = 0usize;
        for db in dbs {
            deleted_count += remove_non_owned_keys(db, &source_slots);
        }

        if deleted_count > 0 {
            remove_all_stale_series_internal();
        }

        log_notice(format!(
            "ASM post-migration cleanup finished: deleted_count={deleted_count} db_count={db_count} slots_count={slots_count}"
        ));
    });
}

/// Slots this node is importing right now, across every in-flight atomic slot migration.
///
/// This used to be one flag for "an import is running". With imports overlapping, the first to
/// complete cleared it and let the others' keys be indexed mid-import (phantom reads), and an
/// abort dropped every import's queued keys. Keyed by slot, each import is handled on its own.
static IMPORTING_SLOTS: LazyLock<Mutex<RangeSetBlaze<u16>>> =
    LazyLock::new(|| Mutex::new(RangeSetBlaze::new()));

/// Whether `key` belongs to a slot being imported, in which case indexing it waits for that
/// import to complete.
pub(crate) fn is_key_in_slot_import(key: &[u8]) -> bool {
    let importing = lock(&IMPORTING_SLOTS);
    !importing.is_empty() && importing.contains(crate::fanout::calculate_hash_slot(key))
}

/// Drops the keys queued during an aborted import of `slots` (the series themselves are
/// removed by the server).
fn discard_delayed_keys_in_slots(slots: &RangeSetBlaze<u16>) {
    let dbs: Vec<i32> = DELAYED_KEYS_MAP.pin().keys().copied().collect();
    for db in dbs {
        let _ = take_delayed_keys_in_slots(db, slots);
    }
}

fn remove_importing_slots(slots: &RangeSetBlaze<u16>) {
    let mut importing = lock(&IMPORTING_SLOTS);
    *importing = &*importing - slots;
}

fn slot_migration_event_handler(event: AtomicSlotMigrationEvent, slots: RangeSetBlaze<u16>) {
    match event {
        AtomicSlotMigrationEvent::ExportCompleted => {
            handle_post_migration_cleanup(slots);
        }
        AtomicSlotMigrationEvent::ImportStarted => {
            *lock(&IMPORTING_SLOTS) |= &slots;
        }
        AtomicSlotMigrationEvent::ImportCompleted => {
            remove_importing_slots(&slots);
            log_debug("ASM ImportCompleted received; triggering delayed indexing");
            process_delayed_indexing(slots);
        }
        AtomicSlotMigrationEvent::ImportAborted => {
            remove_importing_slots(&slots);
            discard_delayed_keys_in_slots(&slots);
        }
        _ => {
            // no action needed for other events
        }
    }
}

pub(super) fn register_asm_event_handler(ctx: &Context) {
    if supports_atomic_slot_migration(ctx) {
        ctx.log_notice("Registering atomic slot migration event handler");
        subscribe_to_atomic_slot_migration_events(ctx);
    } else {
        ctx.log_notice(
            "Atomic slot migration not supported, skipping registration of related event handler",
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn slot_of(key: &[u8]) -> u16 {
        crate::fanout::calculate_hash_slot(key)
    }

    fn slots(keys: &[&[u8]]) -> RangeSetBlaze<u16> {
        keys.iter().map(|k| slot_of(k)).collect()
    }

    #[test]
    fn test_overlapping_imports_are_tracked_per_slot() {
        // Two imports in flight: `{a}` keys belong to the first, `{b}` keys to the second.
        let first = slots(&[b"{a}x"]);
        let second = slots(&[b"{b}x"]);
        assert_ne!(first, second);
        const DB: i32 = 91;

        slot_migration_event_handler(AtomicSlotMigrationEvent::ImportStarted, first.clone());
        slot_migration_event_handler(AtomicSlotMigrationEvent::ImportStarted, second.clone());
        assert!(is_key_in_slot_import(b"{a}1"));
        assert!(is_key_in_slot_import(b"{b}1"));
        assert!(
            !is_key_in_slot_import(b"{c}1"),
            "a key outside both imports is indexed now"
        );

        add_delayed_indexing_key(DB, b"{a}1");
        add_delayed_indexing_key(DB, b"{b}1");
        add_delayed_indexing_key(DB, b"{b}2");

        // Aborting the first drops its queued key only, and leaves the second import running.
        slot_migration_event_handler(AtomicSlotMigrationEvent::ImportAborted, first.clone());
        assert!(!is_key_in_slot_import(b"{a}1"));
        assert!(
            is_key_in_slot_import(b"{b}1"),
            "the second import is still in progress"
        );
        let remaining = take_delayed_keys_in_slots(DB, &second);
        let mut remaining: Vec<&[u8]> = remaining.iter().map(|k| k.as_ref()).collect();
        remaining.sort();
        assert_eq!(remaining, vec![&b"{b}1"[..], &b"{b}2"[..]]);
        assert!(take_delayed_keys_in_slots(DB, &first).is_empty());

        remove_importing_slots(&second);
        assert!(!is_key_in_slot_import(b"{b}1"));
    }

    #[test]
    fn test_parse_single_range() {
        let s = "0-100";
        let r = parse_slot_ranges(s).unwrap();
        let ranges: Vec<_> = r.ranges().collect();
        assert_eq!(ranges.len(), 1);
        assert_eq!(*ranges[0].start(), 0);
        assert_eq!(*ranges[0].end(), 100);
    }

    #[test]
    fn test_parse_multiple_ranges_space() {
        let s = "0-10 20-30";
        let r = parse_slot_ranges(s).unwrap();
        let ranges: Vec<_> = r.ranges().collect();
        assert_eq!(ranges.len(), 2);
        assert_eq!(*ranges[0].start(), 0);
        assert_eq!(*ranges[0].end(), 10);
        assert_eq!(*ranges[1].start(), 20);
        assert_eq!(*ranges[1].end(), 30);
    }

    #[test]
    fn test_parse_multiple_ranges_comma() {
        let s = "0-10,20-30";
        let r = parse_slot_ranges(s).unwrap();
        let ranges: Vec<_> = r.ranges().collect();
        assert_eq!(ranges.len(), 2);
        assert_eq!(*ranges[0].start(), 0);
        assert_eq!(*ranges[0].end(), 10);
        assert_eq!(*ranges[1].start(), 20);
        assert_eq!(*ranges[1].end(), 30);
    }

    #[test]
    fn test_parse_single_slot() {
        let s = "5";
        let r = parse_slot_ranges(s).unwrap();
        let ranges: Vec<_> = r.ranges().collect();
        assert_eq!(ranges.len(), 1);
        assert_eq!(*ranges[0].start(), 5);
        assert_eq!(*ranges[0].end(), 5);
    }
}
