//! Atomic slot migration (ASM, Valkey 9.0+) support.
//!
//! Subscribes to the server's ASM events and keeps the secondary index consistent with them. On
//! the destination, indexing of imported keys is deferred until their import completes (so reads
//! never see phantom series of an import that may still abort); on the source, the index entries
//! of exported slots are retired once the export completes.
use crate::common::context::{create_key_string, get_current_db, set_current_db};
use crate::common::hash::BuildNoHashHasher;
use crate::common::logging::{log_debug, log_notice, log_warning};
use crate::common::metrics::asm as metrics;
use crate::common::metrics::task::TaskRun;
use crate::common::module_options::{HANDLE_ATOMIC_SLOT_MIGRATION, declare_module_options};
use crate::common::sync::{lock, read_lock, write_lock};
use crate::common::threads::LockGil;
use crate::fanout::NUM_SLOTS;
use crate::fanout::{is_clustered, mark_cluster_map_stale};
use crate::series::index::{TIMESERIES_INDEX, get_db_index, index_loaded_series};
use crate::series::series_data_type::VK_TIME_SERIES_TYPE;
use crate::series::tasks::{optimize_index_fully, remove_all_stale_series_internal};
use crate::series::{SeriesRef, TimeSeries};
use range_set_blaze::RangeSetBlaze;
use std::collections::HashMap;
use std::ffi::{c_char, c_int, c_void};
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
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

/// The payload layout this module understands (`VALKEYMODULE_ATOMICSLOTMIGRATION_INFO_VERSION`).
const ASM_INFO_VERSION: u64 = 1;

/// Interprets the raw payload of an ASM server event as the set of slots it names.
///
/// The bindings do not describe this struct, so the layout above is transcribed from
/// `valkeymodule.h` by hand and nothing checks it at compile time. Every field is therefore
/// validated before it is trusted: a null payload, a version this module was not written
/// against, a null or implausibly long range array, and any range outside `0..NUM_SLOTS` are
/// rejected with a warning rather than reinterpreted (a `c_int` cast straight to `u16` would
/// silently wrap). A bad range is skipped; an event that names no usable slot is dropped, so an
/// `ImportStarted` the module cannot interpret defers nothing rather than registering a slot
/// set a later event might not be able to remove.
fn parse_event_payload(data: *mut c_void) -> Option<SlotMigrationEvent> {
    if data.is_null() {
        log_warning("ASM event carried no payload; ignoring it");
        return None;
    }
    // SAFETY: the server hands every ASM event a `ValkeyModuleAtomicSlotMigrationInfo` that
    // outlives the callback; the null check above is the only precondition we can verify.
    let info = unsafe { &*(data as *const ValkeyModuleAtomicSlotMigrationInfoV1) };
    if info.version != ASM_INFO_VERSION {
        log_warning(format!(
            "ASM event payload version {} is not the supported version {ASM_INFO_VERSION}; ignoring the event",
            info.version
        ));
        return None;
    }
    let count = info.num_slot_ranges as usize;
    if count == 0 || count > NUM_SLOTS as usize {
        log_warning(format!(
            "ASM event names {count} slot ranges, which is not a plausible count; ignoring the event"
        ));
        return None;
    }
    if info.slot_ranges.is_null() {
        log_warning("ASM event has a null slot range array; ignoring the event");
        return None;
    }
    // SAFETY: non-null and `count` entries long by the checks above and the server's contract.
    let ranges = unsafe { std::slice::from_raw_parts(info.slot_ranges, count) };
    let mut slots = RangeSetBlaze::new();
    for range in ranges {
        match (u16::try_from(range.start), u16::try_from(range.end)) {
            (Ok(start), Ok(end)) if start <= end && end < NUM_SLOTS => {
                slots.ranges_insert(start..=end);
            }
            _ => log_warning(format!(
                "ASM event names slot range {}-{}, outside 0..{NUM_SLOTS}; skipping it",
                range.start, range.end
            )),
        }
    }
    if slots.is_empty() {
        log_warning("ASM event named no usable slot; ignoring the event");
        return None;
    }
    let mut job = [0u8; VALKEYMODULE_NODE_ID_LEN];
    for (dst, src) in job.iter_mut().zip(info.job_name.iter()) {
        *dst = *src as u8;
    }
    Some(SlotMigrationEvent { job, slots })
}

unsafe extern "C" fn on_atomic_slot_migration_event(
    _ctx: *mut raw::RedisModuleCtx,
    _eid: raw::RedisModuleEvent,
    sub_event: u64,
    data: *mut c_void,
) {
    fn raise_event(event: AtomicSlotMigrationEvent, data: *mut c_void) {
        if let Some(reported) = parse_event_payload(data) {
            slot_migration_event_handler(event, reported);
        }
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

/// The server's identity for a migration job: `CLUSTER_NAMELEN` bytes, NUL-terminated in the
/// event payload. Every event of one job carries the same name, on the target primary and on
/// its replicas alike.
type JobName = [u8; VALKEYMODULE_NODE_ID_LEN];

fn job_display(job: &JobName) -> String {
    String::from_utf8_lossy(job).into_owned()
}

/// A slot migration event as the server reports it: which job, and which slots it covers.
struct SlotMigrationEvent {
    job: JobName,
    slots: RangeSetBlaze<u16>,
}

/// An import in flight on this node.
///
/// Keys that arrive for it — restored by `TS._RESTORE`, loaded, or created, altered, renamed or
/// copied by the replayed stream — are queued here instead of being indexed, so reads never see
/// phantom series of an import that may still abort. Completion detaches the whole job, its
/// registered slots and its queue, in one step on the main thread: a drain only ever indexes the
/// keys of the import it was spawned for, even when the same slot is imported again before that
/// drain runs, and an abort or completion that reports fewer slots than the start still clears
/// the registration.
struct ImportJob {
    name: JobName,
    slots: RangeSetBlaze<u16>,
    /// Queued keys per db.
    pending: HashMap<i32, Vec<Box<[u8]>>, BuildNoHashHasher<i32>>,
}

/// Every import in flight, in start order. Mutated only on the main thread (event handlers,
/// command paths, flush handlers); a drain works on a detached job and never touches this.
static IMPORTS: Mutex<Vec<ImportJob>> = Mutex::new(Vec::new());

/// Whether `IMPORTS` is non-empty. Maintained under its mutex, so it is only ever stale in the
/// direction of "take the lock and look": [`is_key_in_slot_import`] runs on every series
/// creation, restore and load, almost always with no import in flight.
static IMPORT_ACTIVE: AtomicBool = AtomicBool::new(false);

static DELAYED_KEYS_COUNTER: AtomicUsize = AtomicUsize::new(0);

fn with_imports<R>(f: impl FnOnce(&mut Vec<ImportJob>) -> R) -> R {
    let mut imports = lock(&IMPORTS);
    let result = f(&mut imports);
    IMPORT_ACTIVE.store(!imports.is_empty(), Ordering::Release);
    result
}

/// FLUSHALL: every queued key is gone from the keyspace. The imports themselves stay
/// registered; the server fails them and reports that as its own event.
pub(super) fn clear_delayed_keys_map() {
    with_imports(|imports| imports.iter_mut().for_each(|job| job.pending.clear()));
}

/// FLUSHDB: as [`clear_delayed_keys_map`], for one db.
pub(super) fn clear_delayed_keys_in_db(db: i32) {
    with_imports(|imports| {
        for job in imports.iter_mut() {
            job.pending.remove(&db);
        }
    });
}

/// Queues `key` for indexing when the import of its slot completes.
///
/// The caller has just checked [`is_key_in_slot_import`]; both run on the main thread, as do
/// the event handlers that change the set of imports, so the owning import cannot have gone
/// away in between.
pub(crate) fn add_delayed_indexing_key(db: i32, key: &[u8]) {
    let slot = crate::fanout::calculate_hash_slot(key);
    with_imports(
        |imports| match imports.iter_mut().find(|job| job.slots.contains(slot)) {
            Some(job) => job.pending.entry(db).or_default().push(key.into()),
            None => log_warning(format!(
                "ASM: no import in flight owns slot {slot}; key {} in db {db} is left unindexed",
                String::from_utf8_lossy(key)
            )),
        },
    );

    let count = DELAYED_KEYS_COUNTER.fetch_add(1, Ordering::Relaxed) + 1;
    // log every 100 queued keys to avoid excessive noise
    if count.is_multiple_of(100) {
        log_debug(format!(
            "ASM queued delayed indexing keys: total_queued={count}, db={db}"
        ));
    }
}

/// Whether `key` belongs to a slot being imported, in which case indexing it waits for that
/// import to complete.
pub(crate) fn is_key_in_slot_import(key: &[u8]) -> bool {
    if !IMPORT_ACTIVE.load(Ordering::Acquire) {
        return false;
    }
    let slot = crate::fanout::calculate_hash_slot(key);
    lock(&IMPORTS).iter().any(|job| job.slots.contains(slot))
}

fn start_import(job: JobName, slots: RangeSetBlaze<u16>) {
    with_imports(|imports| {
        if let Some(existing) = imports.iter_mut().find(|j| j.name == job) {
            log_warning(format!(
                "ASM import {} reported as started twice; merging its slot ranges",
                job_display(&job)
            ));
            existing.slots |= &slots;
            return;
        }
        imports.push(ImportJob {
            name: job,
            slots,
            pending: HashMap::default(),
        });
    });
}

/// Detaches the import `job`: its registered slots stop deferring, and its queue comes back to
/// the caller. `reported` is what the completion or abort event named; a difference from the
/// start is logged, but it is the registration that gets removed. `None` when the job was never
/// registered here — an import restored from a snapshot, which fires no start event.
fn finish_import(job: &JobName, reported: &RangeSetBlaze<u16>) -> Option<ImportJob> {
    with_imports(|imports| {
        let position = imports.iter().position(|j| &j.name == job)?;
        let done = imports.remove(position);
        if done.slots != *reported {
            log_warning(format!(
                "ASM import {} finished with {} slot range(s) but started with {}; releasing the slots it started with",
                job_display(job),
                reported.ranges_len(),
                done.slots.ranges_len()
            ));
        }
        Some(done)
    })
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

    let ctx = MODULE_CONTEXT.lock_gil();
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
        // Imported ids come from another node's id space, so every imported series gets a
        // fresh local id; a key queued twice has its first id retired, not duplicated.
        index_loaded_series(&mut postings, series, key_name.as_ref());
    }
    drop(postings);
    drop(opened);

    set_current_db(&ctx, save_db);
    skipped
}

/// Indexes `keys`, queued for `db` during an import that has just completed. Returns how many
/// were indexed.
fn process_delayed_keys_for_db(db: i32, keys: Vec<Box<[u8]>>, run: &TaskRun<'_>) -> usize {
    let total = keys.len();
    let mut indexed = 0usize;
    let mut skipped = 0usize;
    for batch in keys.chunks(BATCH_SIZE) {
        // Once detached from its import, a key is only indexed here — bailing out mid-drain
        // leaves the remaining keys in this db un-indexed. That's acceptable on shutdown (the
        // process is about to exit; a restart re-derives the index from the RDB/AOF), but not
        // otherwise, so this only checks the shutdown flag, not e.g. a generic cancellation.
        if run.check_shutdown() {
            log_debug(format!(
                "ASM delayed indexing for db={db} aborted by shutdown; {} of {total} key(s) left un-indexed",
                total - indexed
            ));
            return 0;
        }
        indexed += batch.len();
        let batch_skipped = index_timeseries_in_batch(db, batch);
        skipped += batch_skipped;
        metrics::DRAIN_KEYS_INDEXED
            .fetch_add((batch.len() - batch_skipped) as u64, Ordering::Relaxed);
    }

    if skipped > 0 {
        // A skipped key had no timeseries value at drain time; this is terminal (retrying would
        // never succeed), so we drop it rather than re-queueing it forever.
        log_debug(format!(
            "ASM delayed indexing dropped {skipped} key(s) with no series value in db={db}"
        ));
    }
    indexed - skipped
}

/// Indexes the keys queued during `job`, an import that has just completed.
///
/// There is no "already running" guard: each drain owns exactly its import's keys, so
/// concurrent drains touch disjoint keys and skipping one would strand them.
fn process_delayed_indexing(job: ImportJob) {
    let mut pending: Vec<(i32, Vec<Box<[u8]>>)> = job
        .pending
        .into_iter()
        .filter(|(_, keys)| !keys.is_empty())
        .collect();
    pending.sort_unstable_by_key(|(db, _)| *db);

    log_debug(format!(
        "ASM delayed indexing drain scheduled: job={} dbs={} slot_ranges={}",
        job_display(&job.name),
        pending.len(),
        job.slots.ranges_len()
    ));
    if pending.is_empty() {
        return;
    }

    // On its own thread rather than the pool because it takes the module lock (see
    // `spawn_background`), and checks `is_shutting_down()` between dbs — an aborted drain
    // leaves the remaining dbs' keys un-indexed, which is fine on shutdown (see
    // `process_delayed_keys_for_db`) but must not happen otherwise.
    crate::common::threads::spawn_task(&metrics::DRAIN, move |run| {
        for (db, keys) in pending {
            if run.check_shutdown() {
                log_debug("ASM delayed indexing drain aborted by shutdown");
                return;
            }
            // The import added a slot's worth of series to the postings in one burst.
            if process_delayed_keys_for_db(db, keys, run) > 0 {
                optimize_index_fully(db);
                if run.check_shutdown() {
                    return;
                }
            }
        }
        log_debug("ASM delayed indexing drain finished");
    });
}

/// Retires from `db`'s index every series whose key hashes into `exported_slots`, and returns
/// how many ids that marked stale.
///
/// By the time this runs the server has already deleted every exported key through the type's
/// `unlink2` callback (see [`handle_post_migration_cleanup`]), so normally it finds nothing: it is
/// a net for an id whose index entry drifted from its key and so escaped the key-checked
/// retirement in `unlink2`. The scan walks `id_to_key` one window at a time under the read lock
/// and marks each window's matches under the write lock, never holding both.
fn remove_non_owned_keys(db: i32, exported_slots: &RangeSetBlaze<u16>) -> usize {
    if exported_slots.is_empty() {
        return 0;
    }
    let mut marked = 0usize;
    let mut cursor: u64 = 0;
    let mut batch: Vec<SeriesRef> = Vec::with_capacity(BATCH_SIZE);
    loop {
        // `batch` is empty at the top of every iteration: each window is flushed below.
        let index = get_db_index(db);
        let visited = {
            let postings = read_lock(&index.inner);
            let mut visited = 0usize;
            for (id, key) in postings.id_to_key.range(cursor..).take(BATCH_SIZE) {
                visited += 1;
                cursor = *id + 1;
                if exported_slots.contains(crate::fanout::calculate_hash_slot(key.as_ref())) {
                    batch.push(*id);
                }
            }
            visited
        };
        marked += index.mark_ids_as_stale(&batch);
        batch.clear();
        if visited < BATCH_SIZE {
            break;
        }
    }
    marked
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
    let slots_count = source_slots.len();
    log_notice(format!(
        "ASM post-migration cleanup scheduled: source_slots_count={slots_count}"
    ));

    // Spawn a background task so we don't block the main thread; this can take a while if there are
    // a lot of keys to clean up. Off the pool: it takes the module lock (see `spawn_background`).
    crate::common::threads::spawn_task(&metrics::POST_MIGRATION_CLEANUP, move |run| {
        let index = TIMESERIES_INDEX.pin();
        let mut dbs: Vec<i32> = index.keys().copied().collect();
        dbs.sort_unstable();
        let db_count = dbs.len();
        drop(index);

        log_notice(format!(
            "ASM post-migration cleanup starting: db_count={db_count} slots_count={slots_count}"
        ));

        let mut deleted_count = 0usize;
        let mut touched_dbs = Vec::new();
        for db in dbs {
            let deleted = remove_non_owned_keys(db, &source_slots);
            metrics::POST_MIGRATION_CLEANUP_IDS_MARKED.fetch_add(deleted as u64, Ordering::Relaxed);
            if deleted > 0 {
                touched_dbs.push(db);
                deleted_count += deleted;
            }
        }

        if deleted_count > 0 {
            remove_all_stale_series_internal();
            // Masking the exported ids out leaves the surviving posting lists sparse.
            for db in touched_dbs {
                if run.check_shutdown() {
                    break;
                }
                optimize_index_fully(db);
                // `optimize_index_fully` stops early on shutdown without aborting this run; check
                // again so a pass cut short on the last db isn't recorded as completed.
                if run.check_shutdown() {
                    break;
                }
            }
        }

        log_notice(format!(
            "ASM post-migration cleanup finished: deleted_count={deleted_count} db_count={db_count} slots_count={slots_count}"
        ));
    });
}

fn slot_migration_event_handler(event: AtomicSlotMigrationEvent, reported: SlotMigrationEvent) {
    let SlotMigrationEvent { job, slots } = reported;
    match event {
        AtomicSlotMigrationEvent::ExportCompleted => {
            handle_post_migration_cleanup(slots);
        }
        AtomicSlotMigrationEvent::ImportStarted => {
            start_import(job, slots);
        }
        AtomicSlotMigrationEvent::ImportCompleted => match finish_import(&job, &slots) {
            Some(done) => {
                log_debug("ASM ImportCompleted received; triggering delayed indexing");
                process_delayed_indexing(done);
            }
            None => log_debug(format!(
                "ASM import {} completed but was not registered here (restored from a snapshot?); nothing to drain",
                job_display(&job)
            )),
        },
        AtomicSlotMigrationEvent::ImportAborted => {
            // The queued keys go with the job; the server has already removed the series.
            let registered = finish_import(&job, &slots).is_some();
            if !registered {
                log_debug(format!(
                    "ASM import {} aborted but was not registered here",
                    job_display(&job)
                ));
            }
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

    fn job(tag: u8) -> JobName {
        [tag; VALKEYMODULE_NODE_ID_LEN]
    }

    fn start(job: JobName, slots: &RangeSetBlaze<u16>) {
        slot_migration_event_handler(
            AtomicSlotMigrationEvent::ImportStarted,
            SlotMigrationEvent {
                job,
                slots: slots.clone(),
            },
        );
    }

    fn abort(job: JobName, slots: &RangeSetBlaze<u16>) {
        slot_migration_event_handler(
            AtomicSlotMigrationEvent::ImportAborted,
            SlotMigrationEvent {
                job,
                slots: slots.clone(),
            },
        );
    }

    fn pending_keys(job: &ImportJob, db: i32) -> Vec<&[u8]> {
        let mut keys: Vec<&[u8]> = job
            .pending
            .get(&db)
            .map(|keys| keys.iter().map(|k| k.as_ref()).collect())
            .unwrap_or_default();
        keys.sort();
        keys
    }

    #[test]
    #[serial]
    fn test_overlapping_imports_are_tracked_per_job() {
        // Two imports in flight: `{a}` keys belong to the first, `{b}` keys to the second.
        let first = slots(&[b"{a}x"]);
        let second = slots(&[b"{b}x"]);
        assert_ne!(first, second);
        const DB: i32 = 91;

        start(job(b'1'), &first);
        start(job(b'2'), &second);
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
        abort(job(b'1'), &first);
        assert!(!is_key_in_slot_import(b"{a}1"));
        assert!(
            is_key_in_slot_import(b"{b}1"),
            "the second import is still in progress"
        );
        let done = finish_import(&job(b'2'), &second).expect("the second import is registered");
        assert_eq!(pending_keys(&done, DB), vec![&b"{b}1"[..], &b"{b}2"[..]]);
        assert!(!is_key_in_slot_import(b"{b}1"));
        assert!(
            finish_import(&job(b'1'), &first).is_none(),
            "already aborted"
        );
    }

    #[test]
    #[serial]
    fn test_a_completed_import_does_not_own_a_later_imports_keys() {
        // ASM-03: the same slot is imported twice in a row. The first import's completion
        // detaches exactly its own queue; a key queued for the second import afterwards belongs
        // to the second import, even though both used the same slot.
        let shared = slots(&[b"{d}x"]);
        const DB: i32 = 92;

        start(job(b'3'), &shared);
        add_delayed_indexing_key(DB, b"{d}first");
        let first = finish_import(&job(b'3'), &shared).expect("first import is registered");
        assert!(
            !is_key_in_slot_import(b"{d}x"),
            "nothing is importing between the two"
        );

        start(job(b'4'), &shared);
        add_delayed_indexing_key(DB, b"{d}second");
        assert_eq!(pending_keys(&first, DB), vec![&b"{d}first"[..]]);

        let second = finish_import(&job(b'4'), &shared).expect("second import is registered");
        assert_eq!(pending_keys(&second, DB), vec![&b"{d}second"[..]]);
        assert!(!is_key_in_slot_import(b"{d}x"));
    }

    #[test]
    #[serial]
    fn test_finishing_removes_the_registered_slots_not_the_reported_ones() {
        // ASM-07: a completion or abort that names fewer slots than its start must not leave
        // the rest importing forever; the job's registration is what gets removed.
        let registered = slots(&[b"{e}x", b"{f}x"]);
        let reported = slots(&[b"{e}x"]);
        assert_ne!(registered, reported);

        start(job(b'5'), &registered);
        assert!(is_key_in_slot_import(b"{f}1"));
        abort(job(b'5'), &reported);
        assert!(!is_key_in_slot_import(b"{e}1"));
        assert!(
            !is_key_in_slot_import(b"{f}1"),
            "slot of the start, absent from the abort"
        );
    }

    #[test]
    #[serial]
    fn test_unregistered_completion_leaves_other_imports_alone() {
        // An import this node never saw start (restored from RDB, say) completes: nothing to
        // drain, and an unrelated import in flight keeps its slots and keys.
        let running = slots(&[b"{g}x"]);
        let unknown = slots(&[b"{h}x"]);
        const DB: i32 = 95;

        start(job(b'6'), &running);
        add_delayed_indexing_key(DB, b"{g}1");
        assert!(finish_import(&job(b'7'), &unknown).is_none());
        assert!(is_key_in_slot_import(b"{g}1"));
        let done = finish_import(&job(b'6'), &running).expect("registered");
        assert_eq!(pending_keys(&done, DB), vec![&b"{g}1"[..]]);
    }

    #[test]
    #[serial]
    fn test_flushing_a_db_drops_only_that_dbs_queued_keys() {
        let importing = slots(&[b"{i}x"]);
        start(job(b'8'), &importing);
        add_delayed_indexing_key(96, b"{i}1");
        add_delayed_indexing_key(97, b"{i}2");

        clear_delayed_keys_in_db(96);
        let done = finish_import(&job(b'8'), &importing).expect("registered");
        assert!(pending_keys(&done, 96).is_empty());
        assert_eq!(pending_keys(&done, 97), vec![&b"{i}2"[..]]);
    }

    #[test]
    #[serial]
    fn test_flushing_everything_drops_every_queued_key_but_keeps_the_import() {
        let importing = slots(&[b"{j}x"]);
        start(job(b'9'), &importing);
        add_delayed_indexing_key(98, b"{j}1");

        clear_delayed_keys_map();
        assert!(
            is_key_in_slot_import(b"{j}1"),
            "the import itself is still running"
        );
        let done = finish_import(&job(b'9'), &importing).expect("registered");
        assert!(pending_keys(&done, 98).is_empty());
    }

    /// A synthetic event payload, laid out exactly as the server fills it in.
    struct Payload {
        ranges: Vec<ValkeyModuleSlotRange>,
        info: ValkeyModuleAtomicSlotMigrationInfoV1,
    }

    impl Payload {
        fn new(version: u64, ranges: &[(c_int, c_int)]) -> Self {
            let mut ranges: Vec<ValkeyModuleSlotRange> = ranges
                .iter()
                .map(|&(start, end)| ValkeyModuleSlotRange { start, end })
                .collect();
            let mut job_name = [0 as c_char; VALKEYMODULE_NODE_ID_LEN + 1];
            for (dst, src) in job_name
                .iter_mut()
                .zip(b"0123456789abcdef0123456789abcdef01234567")
            {
                *dst = *src as c_char;
            }
            let info = ValkeyModuleAtomicSlotMigrationInfoV1 {
                version,
                job_name,
                slot_ranges: ranges.as_mut_ptr(),
                num_slot_ranges: ranges.len() as u32,
            };
            Payload { ranges, info }
        }

        fn ptr(&self) -> *mut c_void {
            &self.info as *const _ as *mut c_void
        }
    }

    #[test]
    fn test_payload_valid_ranges_are_parsed() {
        let p = Payload::new(1, &[(0, 100), (200, 300), (16383, 16383)]);
        let event = parse_event_payload(p.ptr()).expect("valid payload");
        assert_eq!(&event.job[..], b"0123456789abcdef0123456789abcdef01234567");
        let slots = event.slots;
        assert_eq!(slots.ranges_len(), 3);
        assert!(slots.contains(0) && slots.contains(100) && slots.contains(16383));
        assert!(!slots.contains(101) && !slots.contains(199));
        assert_eq!(slots.len(), 101 + 101 + 1);
    }

    #[test]
    fn test_payload_overlapping_and_unsorted_ranges_merge() {
        let p = Payload::new(1, &[(50, 60), (10, 55), (7, 7)]);
        let slots = parse_event_payload(p.ptr()).expect("valid payload").slots;
        assert_eq!(slots.ranges_len(), 2);
        assert_eq!(slots.len(), 51 + 1);
        let _ = &p.ranges;
    }

    #[test]
    fn test_payload_null_is_rejected() {
        assert!(parse_event_payload(std::ptr::null_mut()).is_none());
    }

    #[test]
    fn test_payload_unknown_version_is_rejected() {
        let p = Payload::new(2, &[(0, 10)]);
        assert!(parse_event_payload(p.ptr()).is_none());
    }

    #[test]
    fn test_payload_null_ranges_with_nonzero_count_is_rejected() {
        let mut p = Payload::new(1, &[(0, 10)]);
        p.info.slot_ranges = std::ptr::null_mut();
        assert!(parse_event_payload(p.ptr()).is_none());
    }

    #[test]
    fn test_payload_implausible_range_count_is_rejected() {
        let mut p = Payload::new(1, &[(0, 10)]);
        p.info.num_slot_ranges = u32::MAX;
        assert!(parse_event_payload(p.ptr()).is_none());
    }

    #[test]
    fn test_payload_empty_range_list_is_rejected() {
        // Every event names at least one slot; an empty list has no meaning for any handler.
        let p = Payload::new(1, &[]);
        assert!(parse_event_payload(p.ptr()).is_none());
    }

    #[test]
    fn test_payload_bad_ranges_are_skipped_not_wrapped() {
        // Inverted, negative and out-of-range bounds are each dropped; the good range survives.
        let p = Payload::new(
            1,
            &[(20, 10), (-1, 5), (16380, 16384), (-70000, 3), (100, 110)],
        );
        let slots = parse_event_payload(p.ptr()).expect("one good range").slots;
        assert_eq!(slots.ranges_len(), 1);
        assert_eq!(slots.len(), 11);
        assert!(slots.contains(100) && slots.contains(110));
    }

    #[test]
    fn test_payload_with_only_bad_ranges_is_rejected() {
        let p = Payload::new(1, &[(20, 10), (16384, 16385)]);
        assert!(parse_event_payload(p.ptr()).is_none());
    }

    use crate::series::index::{clear_timeseries_index, next_timeseries_id};
    use serial_test::serial;

    fn index_series(db: i32, key: &str) -> SeriesRef {
        let mut ts = TimeSeries::new();
        ts.id = next_timeseries_id();
        ts.labels = r#"metric{l="v"}"#.parse().unwrap();
        get_db_index(db).index_timeseries(&ts, key.as_bytes());
        ts.id
    }

    /// `count` owned and `count` exported series, interleaved in id order so that exported
    /// matches are sparser than `BATCH_SIZE` within a scan window.
    fn populate_interleaved(db: i32, count: usize) -> (Vec<SeriesRef>, Vec<SeriesRef>) {
        clear_timeseries_index(db);
        let mut owned = Vec::with_capacity(count);
        let mut exported = Vec::with_capacity(count);
        for i in 0..count {
            owned.push(index_series(db, &format!("{{own}}:{i}")));
            exported.push(index_series(db, &format!("{{exp}}:{i}")));
        }
        (owned, exported)
    }

    fn indexed_ids(db: i32) -> Vec<SeriesRef> {
        let index = get_db_index(db);
        let postings = read_lock(&index.inner);
        postings.id_to_key.keys().copied().collect()
    }

    #[test]
    #[serial]
    fn test_cleanup_retires_exactly_the_exported_slots_across_windows() {
        const DB: i32 = 93;
        // More than one BATCH_SIZE of exported ids, so the scan both fills a batch mid-window
        // and advances past windows that hold fewer matches than a batch.
        let (owned, exported) = populate_interleaved(DB, 600);
        let exported_slots = slots(&[b"{exp}:0"]);
        assert!(!exported_slots.contains(slot_of(b"{own}:0")));

        let marked = remove_non_owned_keys(DB, &exported_slots);

        assert_eq!(marked, exported.len());
        assert_eq!(
            indexed_ids(DB),
            owned,
            "only the owned ids keep their key mapping"
        );
        let index = get_db_index(DB);
        let postings = read_lock(&index.inner);
        for id in &exported {
            assert!(!postings.has_id(*id), "exported id {id} still has postings");
        }
        for id in &owned {
            assert!(postings.has_id(*id), "owned id {id} lost its postings");
        }
        drop(postings);
        clear_timeseries_index(DB);
    }

    #[test]
    #[serial]
    fn test_cleanup_with_no_exported_slot_touches_nothing() {
        const DB: i32 = 94;
        let (mut owned, exported) = populate_interleaved(DB, 10);
        owned.extend(exported);
        owned.sort_unstable();

        let marked = remove_non_owned_keys(DB, &RangeSetBlaze::new());

        assert_eq!(marked, 0);
        assert_eq!(indexed_ids(DB), owned);
        clear_timeseries_index(DB);
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
