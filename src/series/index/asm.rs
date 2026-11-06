//! Atomic slot migration (ASM, Valkey 9.0+) support.
//!
//! Subscribes to the server's ASM events and keeps the secondary index consistent with them. On
//! the destination, this module is the registry of imports in flight: imported series are indexed
//! as they arrive, on whichever path brings them, but [`Postings::index_timeseries`] consults
//! [`importing_slot`] and hides them from reads (see `postings::hidden`), so reads never see
//! phantom series of an import that may still abort. Completion publishes them on the main thread,
//! together with the ownership change. On the source nothing extra is needed: the server deletes
//! the exported keys through `unlink2`, which retires them from the index like any other deletion
//! (see the `ExportCompleted` arm of the event handler).
//!
//! The import events fire on the target primary *and* on its replicas (the server propagates the
//! migration's control commands down the replication stream), so a replica hides and publishes
//! exactly like its primary.
//!
//! [`Postings::index_timeseries`]: crate::series::index::Postings::index_timeseries
use crate::common::logging::{log_debug, log_notice, log_warning};
use crate::common::metrics::asm as metrics;
use crate::common::module_options::{HANDLE_ATOMIC_SLOT_MIGRATION, declare_module_options};
use crate::common::sync::{lock, write_lock};
use crate::fanout::NUM_SLOTS;
use crate::fanout::{is_clustered, mark_cluster_map_stale};
use crate::series::index::TIMESERIES_INDEX;
use crate::series::index::postings::Postings;
use crate::series::tasks::optimize_index_fully;
use range_set_blaze::RangeSetBlaze;
use std::collections::BTreeSet;
use std::ffi::{c_char, c_int, c_void};
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, Ordering};
use valkey_module::redisvalue::ValkeyValueKey;
use valkey_module::{Context, ValkeyValue, Version, raw};

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

/// Parse a slot ranges string into a vector of inclusive ranges (the `slot_ranges` field of
/// `CLUSTER GETSLOTMIGRATIONS` is the space-separated form).
///
/// Examples accepted:
/// - "0-100"
/// - "0-100 200-300"
/// - "0-100,200-300"
/// - "5" (single slot)
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

/// An import in flight on this node: which slots hide their series until it finishes. Completion
/// and abort detach the whole job in one step on the main thread, so an abort or completion that
/// reports fewer slots than the start still clears the registration.
struct ImportJob {
    name: JobName,
    slots: RangeSetBlaze<u16>,
}

/// Every import in flight, in start order. Mutated only on the main thread, by the event handlers
/// and the load-end reconciliation.
static IMPORTS: Mutex<Vec<ImportJob>> = Mutex::new(Vec::new());

/// Whether `IMPORTS` is non-empty. Maintained under its mutex, so it is only ever stale in the
/// direction of "take the lock and look": [`importing_slot`] runs every time a series is indexed,
/// almost always with no import in flight.
static IMPORT_ACTIVE: AtomicBool = AtomicBool::new(false);

// Unit tests index series on many threads at once, and only those of the thread that started an
// import may see it: the rest would otherwise have a key hidden whenever its slot happened to
// match.
#[cfg(test)]
thread_local! {
    static IMPORTS_IN_SCOPE: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

fn with_imports<R>(f: impl FnOnce(&mut Vec<ImportJob>) -> R) -> R {
    let mut imports = lock(&IMPORTS);
    let result = f(&mut imports);
    IMPORT_ACTIVE.store(!imports.is_empty(), Ordering::Release);
    result
}

/// The slot of `key` if an import of it is in flight, in which case its series stays hidden from
/// reads until that import completes.
pub(in crate::series::index) fn importing_slot(key: &[u8]) -> Option<u16> {
    #[cfg(test)]
    if !IMPORTS_IN_SCOPE.get() {
        return None;
    }
    if !IMPORT_ACTIVE.load(Ordering::Acquire) {
        return None;
    }
    let slot = crate::fanout::calculate_hash_slot(key);
    lock(&IMPORTS)
        .iter()
        .any(|job| job.slots.contains(slot))
        .then_some(slot)
}

fn start_import(job: JobName, slots: RangeSetBlaze<u16>) {
    #[cfg(test)]
    IMPORTS_IN_SCOPE.set(true);
    with_imports(|imports| {
        if let Some(existing) = imports.iter_mut().find(|j| j.name == job) {
            log_warning(format!(
                "ASM import {} reported as started twice; merging its slot ranges",
                job_display(&job)
            ));
            existing.slots |= &slots;
            return;
        }
        imports.push(ImportJob { name: job, slots });
    });
}

/// Detaches the import `job`: its registered slots stop hiding what is indexed from now on, and
/// the job comes back to the caller. `reported` is what the completion or abort event named; a difference from the
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

/// Runs `f` on the postings of every db, with the write lock held. Lock order: never with
/// `IMPORTS` locked, since indexing takes that one under the postings lock.
fn for_each_db_postings(mut f: impl FnMut(&mut Postings) -> u64) -> u64 {
    let indexes = TIMESERIES_INDEX.pin();
    indexes
        .values()
        .map(|index| f(&mut write_lock(&index.inner)))
        .sum()
}

/// The import of `slots` completed: their series become visible, before the server resumes.
fn publish_import(slots: &RangeSetBlaze<u16>) {
    let mut dbs = Vec::new();
    let mut published = 0;
    for (db, index) in TIMESERIES_INDEX.pin().iter() {
        let in_db = write_lock(&index.inner).publish_hidden_in_slots(slots);
        if in_db > 0 {
            dbs.push(*db);
            published += in_db;
        }
    }
    metrics::KEYS_PUBLISHED.fetch_add(published, Ordering::Relaxed);
    log_debug(format!(
        "ASM import completed: {published} series published"
    ));
    optimize_after_import(dbs);
}

/// The dbs waiting for their post-import optimize, and whether a worker is draining them.
///
/// A run of small imports would otherwise repeat a pass over every posting list per import, and
/// several passes over one db at a time would only contend for its write lock. Publishes during a
/// run fold into one more pass per db. Both fields sit behind one mutex so that a worker's decision
/// to exit and a publish's decision not to start one cannot miss each other, which a separate
/// single-flight guard released after the exit check would allow.
struct OptimizeQueue {
    dbs: BTreeSet<i32>,
    running: bool,
}

impl OptimizeQueue {
    const fn new() -> Self {
        Self {
            dbs: BTreeSet::new(),
            running: false,
        }
    }

    /// Queues `dbs`. Returns whether the caller must start a worker.
    fn add(&mut self, dbs: impl IntoIterator<Item = i32>) -> bool {
        self.dbs.extend(dbs);
        !std::mem::replace(&mut self.running, true)
    }

    /// What a worker should optimize next. `None` ends its run.
    fn next_batch(&mut self) -> Option<BTreeSet<i32>> {
        if self.dbs.is_empty() {
            self.running = false;
            return None;
        }
        Some(std::mem::take(&mut self.dbs))
    }

    /// The worker the last [`OptimizeQueue::add`] asked for could not start. Its dbs wait for the
    /// next publish, and the periodic optimizer reaches them regardless.
    fn worker_not_started(&mut self) {
        self.running = false;
    }
}

/// Runs `optimize` on queued dbs until none are left. Stopping is for shutdown, so it does not
/// bother handing the queue back.
fn drain_optimize_queue(
    queue: &Mutex<OptimizeQueue>,
    should_stop: impl Fn() -> bool,
    mut optimize: impl FnMut(i32),
) {
    /// A worker that panics must not leave the queue believing it still runs.
    struct ReleaseOnPanic<'a>(&'a Mutex<OptimizeQueue>);
    impl Drop for ReleaseOnPanic<'_> {
        fn drop(&mut self) {
            if std::thread::panicking() {
                lock(self.0).running = false;
            }
        }
    }
    let _release = ReleaseOnPanic(queue);

    loop {
        let Some(dbs) = lock(queue).next_batch() else {
            return;
        };
        for db in dbs {
            if should_stop() {
                return;
            }
            optimize(db);
        }
    }
}

static OPTIMIZE_QUEUE: Mutex<OptimizeQueue> = Mutex::new(OptimizeQueue::new());

/// The import added a slot's worth of series to the postings in one burst, which the periodic
/// optimizer would take a long time to reach. On its own thread because it takes the postings
/// write lock batch by batch.
fn optimize_after_import(dbs: Vec<i32>) {
    if dbs.is_empty() {
        return;
    }
    if !lock(&OPTIMIZE_QUEUE).add(dbs) {
        metrics::OPTIMIZE
            .skipped_inflight
            .fetch_add(1, Ordering::Relaxed);
        return;
    }
    let started = crate::common::threads::spawn_task(&metrics::OPTIMIZE, |run| {
        drain_optimize_queue(
            &OPTIMIZE_QUEUE,
            || run.check_shutdown(),
            optimize_index_fully,
        );
    });
    if !started {
        lock(&OPTIMIZE_QUEUE).worker_not_started();
    }
}

/// The import of `slots` aborted, and the server has already deleted its keys.
fn retire_import(slots: &RangeSetBlaze<u16>) {
    let retired = for_each_db_postings(|postings| postings.retire_hidden_in_slots(slots));
    if retired > 0 {
        log_warning(format!(
            "ASM import aborted with {retired} series still hidden; the server's deletion did not reach them"
        ));
    }
}

fn slot_migration_event_handler(event: AtomicSlotMigrationEvent, reported: SlotMigrationEvent) {
    let SlotMigrationEvent { job, slots } = reported;
    match event {
        AtomicSlotMigrationEvent::ExportCompleted => {
            // Nothing to do for the index. The server deletes every exported key on the source
            // (`delKeysInSlot`, run on the main thread as part of the same ownership change that
            // completes the export) through the type's `unlink2` callback, which retires each
            // series from the index key-checked, and the same deletions reach the source's
            // replicas as replicated DELs. A scan that marked every id in the exported slots
            // stale used to run here as well; it never found anything `unlink2` had not already
            // retired, and it could retire a series the slot brought *back* before the scan ran.
        }
        AtomicSlotMigrationEvent::ImportStarted => {
            start_import(job, slots);
        }
        AtomicSlotMigrationEvent::ImportCompleted => match finish_import(&job, &slots) {
            Some(done) => publish_import(&done.slots),
            None => log_debug(format!(
                "ASM import {} completed but was not registered here; nothing to publish",
                job_display(&job)
            )),
        },
        AtomicSlotMigrationEvent::ImportAborted => match finish_import(&job, &slots) {
            Some(done) => retire_import(&done.slots),
            None => log_debug(format!(
                "ASM import {} aborted but was not registered here",
                job_display(&job)
            )),
        },
        _ => {
            // no action needed for other events
        }
    }
}

/// Job states `CLUSTER GETSLOTMIGRATIONS` reports for a finished job.
const TERMINAL_JOB_STATES: [&str; 3] = ["success", "failed", "cancelled"];

/// Reads one `CLUSTER GETSLOTMIGRATIONS` entry, as (field, value) pairs, as an import still in
/// progress: its job name and slots. `None` for exports, finished jobs and malformed entries.
fn restored_import_from_fields(
    fields: &[(String, String)],
) -> Option<(JobName, RangeSetBlaze<u16>)> {
    let field = |name: &str| {
        fields
            .iter()
            .find(|(k, _)| k.eq_ignore_ascii_case(name))
            .map(|(_, v)| v.as_str())
    };
    if !field("operation")?.eq_ignore_ascii_case("IMPORT") {
        return None;
    }
    let state = field("state")?;
    if TERMINAL_JOB_STATES
        .iter()
        .any(|terminal| terminal.eq_ignore_ascii_case(state))
    {
        return None;
    }
    let name: JobName = field("name")?.as_bytes().try_into().ok()?;
    let slots = parse_slot_ranges(field("slot_ranges")?).ok()?;
    if slots.is_empty() {
        return None;
    }
    Some((name, slots))
}

fn value_text(value: &ValkeyValue) -> Option<String> {
    match value {
        ValkeyValue::SimpleString(s) | ValkeyValue::BulkString(s) => Some(s.clone()),
        ValkeyValue::SimpleStringStatic(s) => Some((*s).to_string()),
        ValkeyValue::StringBuffer(b) => Some(String::from_utf8_lossy(b).into_owned()),
        ValkeyValue::BulkValkeyString(s) => Some(s.to_string_lossy()),
        ValkeyValue::Integer(i) => Some(i.to_string()),
        _ => None,
    }
}

fn key_text(key: &ValkeyValueKey) -> Option<String> {
    match key {
        ValkeyValueKey::Integer(i) => Some(i.to_string()),
        ValkeyValueKey::String(s) => Some(s.clone()),
        ValkeyValueKey::BulkValkeyString(s) => Some(s.to_string_lossy()),
        ValkeyValueKey::BulkString(b) => Some(String::from_utf8_lossy(b).into_owned()),
        ValkeyValueKey::Bool(_) => None,
    }
}

/// The (field, value) pairs of one `CLUSTER GETSLOTMIGRATIONS` entry, whichever protocol shape
/// the reply took.
fn reply_fields(entry: &ValkeyValue) -> Vec<(String, String)> {
    match entry {
        ValkeyValue::Array(items) => items
            .chunks(2)
            .filter_map(|pair| match pair {
                [k, v] => Some((value_text(k)?, value_text(v)?)),
                _ => None,
            })
            .collect(),
        ValkeyValue::Map(map) => map
            .iter()
            .filter_map(|(k, v)| Some((key_text(k)?, value_text(v)?)))
            .collect(),
        ValkeyValue::OrderedMap(map) => map
            .iter()
            .filter_map(|(k, v)| Some((key_text(k)?, value_text(v)?)))
            .collect(),
        _ => Vec::new(),
    }
}

/// Registers the imports the server restored from a snapshot, and hides their keys.
///
/// A node that loads an in-flight import from an RDB — a target primary restarting, or a
/// target replica full-syncing from its primary mid-import — recreates the import job from the
/// RDB's import record and keeps hiding the slots, but fires no `ImportStarted`, and whichever
/// index the load built holds every key of those slots as visible. Ask the server which imports
/// are in progress, register each one this module does not know, and hide its series; the
/// `SYNCSLOTS FINISH` that ends the import then publishes them like any other. A known import is
/// hidden again too: a preloaded index replaced the postings that were hiding its series.
pub(super) fn reconcile_imports_after_load(ctx: &Context) {
    if !ASM_ENABLED.load(Ordering::Acquire) {
        return;
    }
    let entries = match ctx.call("CLUSTER", &["GETSLOTMIGRATIONS"]) {
        Ok(ValkeyValue::Array(entries)) => entries,
        Ok(_) => return,
        Err(e) => {
            log_warning(format!(
                "ASM: CLUSTER GETSLOTMIGRATIONS failed after load; imports restored from the snapshot (if any) are not hidden: {e}"
            ));
            return;
        }
    };
    for (job, slots) in entries
        .iter()
        .filter_map(|entry| restored_import_from_fields(&reply_fields(entry)))
    {
        if !with_imports(|imports| imports.iter().any(|j| j.name == job)) {
            start_import(job, slots.clone());
        }
        let hidden = for_each_db_postings(|postings| postings.hide_indexed_keys_in_slots(&slots));
        log_notice(format!(
            "ASM import {} restored from snapshot: {} slot range(s), {hidden} loaded series hidden",
            job_display(&job),
            slots.ranges_len()
        ));
    }
}

/// Set once the event handler is registered: this server can migrate slots atomically.
static ASM_ENABLED: AtomicBool = AtomicBool::new(false);

pub(super) fn register_asm_event_handler(ctx: &Context) {
    if supports_atomic_slot_migration(ctx) {
        ctx.log_notice("Registering atomic slot migration event handler");
        subscribe_to_atomic_slot_migration_events(ctx);
        ASM_ENABLED.store(true, Ordering::Release);
    } else {
        ctx.log_notice(
            "Atomic slot migration not supported, skipping registration of related event handler",
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::labels::{Label, MetricName};
    use crate::series::index::postings::BulkIndexEntry;
    use crate::series::index::{IndexKey, get_db_index};
    use crate::series::{SeriesRef, TimeSeries};
    use serial_test::serial;
    use std::sync::atomic::AtomicI32;

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

    fn complete(job: JobName, slots: &RangeSetBlaze<u16>) {
        slot_migration_event_handler(
            AtomicSlotMigrationEvent::ImportCompleted,
            SlotMigrationEvent {
                job,
                slots: slots.clone(),
            },
        );
    }

    fn importing(key: &[u8]) -> bool {
        importing_slot(key).is_some()
    }

    fn series(id: SeriesRef, label_value: &str) -> TimeSeries {
        let mut series = TimeSeries::new();
        series.id = id;
        series.labels = MetricName::new(&[Label::new("asm_test", label_value)]);
        series
    }

    /// The label values a read sees in `db`.
    fn visible_values(db: i32) -> Vec<String> {
        let index = get_db_index(db);
        let mut values = index.get_label_values("asm_test");
        values.sort();
        values
    }

    #[test]
    fn test_publishes_during_a_pass_fold_into_one_more_pass() {
        let mut queue = OptimizeQueue::new();
        assert!(queue.add([1]), "nothing is running: start a worker");
        assert!(
            !queue.add([2]),
            "the worker is not known to have looked yet"
        );
        assert_eq!(queue.next_batch(), Some(BTreeSet::from([1, 2])));

        assert!(!queue.add([2]), "a pass is in progress");
        assert!(!queue.add([3]));
        assert!(!queue.add([2]));
        assert_eq!(queue.next_batch(), Some(BTreeSet::from([2, 3])));

        assert_eq!(queue.next_batch(), None, "nothing is left: the run ends");
        assert!(queue.add([4]), "and the next publish starts a new one");
    }

    #[test]
    fn test_a_worker_that_cannot_start_keeps_the_dbs_for_the_next_one() {
        let mut queue = OptimizeQueue::new();
        assert!(queue.add([1]));
        queue.worker_not_started();
        assert!(queue.add([2]), "no worker is running");
        assert_eq!(queue.next_batch(), Some(BTreeSet::from([1, 2])));
    }

    #[test]
    fn test_a_publish_racing_a_worker_exit_is_never_stranded() {
        // The worker's "nothing left" and a publish's "a worker is running" must be one decision.
        // Each publish lands just after the previous db was optimized, as its worker heads for the
        // exit, and must be drained before the next: a later worker would hide a stranded db.
        static QUEUE: Mutex<OptimizeQueue> = Mutex::new(OptimizeQueue::new());
        const PUBLISHES: i32 = 2_000;
        let last_seen = std::sync::Arc::new(AtomicI32::new(-1));
        let mut handles = Vec::new();
        for db in 0..PUBLISHES {
            if lock(&QUEUE).add([db]) {
                let last_seen = last_seen.clone();
                handles.push(std::thread::spawn(move || {
                    drain_optimize_queue(
                        &QUEUE,
                        || false,
                        |db| last_seen.store(db, Ordering::SeqCst),
                    );
                }));
            }
            let deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
            while last_seen.load(Ordering::SeqCst) != db {
                assert!(
                    std::time::Instant::now() < deadline,
                    "db {db} was queued but no worker came for it"
                );
                std::hint::spin_loop();
            }
        }
        for handle in handles {
            handle.join().unwrap();
        }
    }

    #[test]
    fn test_a_panicking_pass_does_not_block_later_ones() {
        static QUEUE: Mutex<OptimizeQueue> = Mutex::new(OptimizeQueue::new());
        assert!(lock(&QUEUE).add([1]));
        let outcome = std::panic::catch_unwind(|| {
            drain_optimize_queue(&QUEUE, || false, |_| panic!("optimize failed"));
        });
        assert!(outcome.is_err());
        assert!(lock(&QUEUE).add([2]), "the dead worker no longer counts");
    }

    #[test]
    fn test_a_stopped_pass_runs_nothing_more() {
        static QUEUE: Mutex<OptimizeQueue> = Mutex::new(OptimizeQueue::new());
        assert!(lock(&QUEUE).add([1, 2]));
        let mut ran = Vec::new();
        drain_optimize_queue(&QUEUE, || true, |db| ran.push(db));
        assert!(ran.is_empty());
    }

    #[test]
    #[serial]
    fn test_a_series_indexed_during_its_import_appears_when_it_completes() {
        const DB: i32 = 80;
        let shared = slots(&[b"{p}x"]);
        start(job(b'a'), &shared);

        get_db_index(DB).index_timeseries(&series(1, "imported"), b"{p}1");
        get_db_index(DB).index_timeseries(&series(2, "local"), b"{q}1");
        assert_eq!(visible_values(DB), ["local"], "reads do not see the import");
        assert_eq!(get_db_index(DB).count(), 1);

        complete(job(b'a'), &shared);
        assert_eq!(visible_values(DB), ["imported", "local"]);
        assert_eq!(get_db_index(DB).count(), 2);
    }

    #[test]
    #[serial]
    fn test_a_series_indexed_after_its_import_completed_is_visible_at_once() {
        const DB: i32 = 81;
        let shared = slots(&[b"{r}x"]);
        start(job(b'b'), &shared);
        complete(job(b'b'), &shared);

        get_db_index(DB).index_timeseries(&series(1, "after"), b"{r}1");
        assert_eq!(visible_values(DB), ["after"]);
    }

    #[test]
    #[serial]
    fn test_an_abort_leaves_nothing_behind() {
        const DB: i32 = 82;
        let shared = slots(&[b"{s}x"]);
        start(job(b'c'), &shared);
        let imported = series(1, "aborted");
        get_db_index(DB).index_timeseries(&imported, b"{s}1");

        abort(job(b'c'), &shared);
        assert!(visible_values(DB).is_empty());
        assert_eq!(get_db_index(DB).count(), 0);
        assert!(!get_db_index(DB).has_id(1), "nothing can publish it later");
    }

    #[test]
    #[serial]
    fn test_overlapping_imports_publish_only_their_own_series() {
        const DB: i32 = 83;
        let first = slots(&[b"{a}x"]);
        let second = slots(&[b"{b}x"]);
        assert_ne!(first, second);

        start(job(b'1'), &first);
        start(job(b'2'), &second);
        assert!(importing(b"{a}1"));
        assert!(importing(b"{b}1"));
        assert!(!importing(b"{c}1"), "a key outside both imports is visible");
        get_db_index(DB).index_timeseries(&series(1, "first"), b"{a}1");
        get_db_index(DB).index_timeseries(&series(2, "second"), b"{b}1");

        complete(job(b'1'), &first);
        assert_eq!(visible_values(DB), ["first"]);
        assert!(importing(b"{b}1"), "the second import is still in progress");

        abort(job(b'2'), &second);
        assert_eq!(visible_values(DB), ["first"]);
        assert!(!importing(b"{b}1"));
    }

    #[test]
    #[serial]
    fn test_the_same_slot_imported_twice_keeps_the_imports_apart() {
        // ASM-03: publishing is synchronous, so nothing of the first import is left to mistake
        // for the second one's.
        const DB: i32 = 84;
        let shared = slots(&[b"{d}x"]);
        start(job(b'3'), &shared);
        get_db_index(DB).index_timeseries(&series(1, "first"), b"{d}first");
        complete(job(b'3'), &shared);
        assert!(!importing(b"{d}x"), "nothing is importing between the two");

        start(job(b'4'), &shared);
        get_db_index(DB).index_timeseries(&series(2, "second"), b"{d}second");
        assert_eq!(visible_values(DB), ["first"]);

        complete(job(b'4'), &shared);
        assert_eq!(visible_values(DB), ["first", "second"]);
    }

    #[test]
    #[serial]
    fn test_finishing_uses_the_registered_slots_not_the_reported_ones() {
        // ASM-07: a completion that names fewer slots than its start must neither leave the rest
        // importing forever nor strand their series.
        const DB: i32 = 85;
        let registered = slots(&[b"{e}x", b"{f}x"]);
        let reported = slots(&[b"{e}x"]);
        assert_ne!(registered, reported);

        start(job(b'5'), &registered);
        get_db_index(DB).index_timeseries(&series(1, "absent_from_report"), b"{f}1");
        complete(job(b'5'), &reported);
        assert!(
            !importing(b"{f}1"),
            "slot of the start, absent from the report"
        );
        assert_eq!(visible_values(DB), ["absent_from_report"]);
    }

    #[test]
    #[serial]
    fn test_unregistered_completion_leaves_other_imports_alone() {
        // An import this node never saw start completes: an unrelated one keeps hiding.
        const DB: i32 = 86;
        let running = slots(&[b"{g}x"]);
        let unknown = slots(&[b"{h}x"]);
        start(job(b'6'), &running);
        get_db_index(DB).index_timeseries(&series(1, "running"), b"{g}1");

        complete(job(b'7'), &unknown);
        assert!(importing(b"{g}1"));
        assert!(visible_values(DB).is_empty());

        complete(job(b'6'), &running);
        assert_eq!(visible_values(DB), ["running"]);
    }

    #[test]
    #[serial]
    fn test_flushing_a_db_mid_import_forgets_only_that_dbs_series() {
        let shared = slots(&[b"{i}x"]);
        start(job(b'8'), &shared);
        get_db_index(87).index_timeseries(&series(1, "flushed"), b"{i}1");
        get_db_index(88).index_timeseries(&series(2, "kept"), b"{i}2");

        crate::series::index::clear_timeseries_index(87);
        complete(job(b'8'), &shared);
        assert!(visible_values(87).is_empty());
        assert_eq!(visible_values(88), ["kept"]);
    }

    #[test]
    #[serial]
    fn test_a_replayed_label_change_stays_hidden() {
        // `TS.ALTER … LABELS` re-indexes the series: one more path that must not reveal it.
        const DB: i32 = 89;
        let shared = slots(&[b"{j}x"]);
        start(job(b'9'), &shared);
        let before = series(1, "before");
        get_db_index(DB).index_timeseries(&before, b"{j}1");

        get_db_index(DB).remove_timeseries(&before);
        get_db_index(DB).index_timeseries(&series(1, "after"), b"{j}1");
        assert!(visible_values(DB).is_empty());

        complete(job(b'9'), &shared);
        assert_eq!(visible_values(DB), ["after"]);
    }

    #[test]
    #[serial]
    fn test_a_bulk_build_hides_keys_of_an_import_in_flight() {
        const DB: i32 = 90;
        let shared = slots(&[b"{k}x"]);
        start(job(b'0'), &shared);
        let entry = |id: SeriesRef, key: &str, value: &str| BulkIndexEntry {
            id,
            key: key.as_bytes().into(),
            label_keys: vec![IndexKey::for_label_value("asm_test", value)],
        };
        get_db_index(DB).get_postings_mut().bulk_index(vec![
            entry(1, "{k}1", "imported"),
            entry(2, "{l}1", "local"),
        ]);
        assert_eq!(visible_values(DB), ["local"]);

        complete(job(b'0'), &shared);
        assert_eq!(visible_values(DB), ["imported", "local"]);
    }

    #[test]
    #[serial]
    fn test_a_restored_import_hides_the_series_the_load_indexed() {
        // No start event: the load indexed these before the import was known.
        const DB: i32 = 79;
        get_db_index(DB).index_timeseries(&series(1, "loaded"), b"{m}1");
        assert_eq!(visible_values(DB), ["loaded"]);

        let shared = slots(&[b"{m}x"]);
        start(job(b'x'), &shared);
        assert_eq!(
            for_each_db_postings(|postings| postings.hide_indexed_keys_in_slots(&shared)),
            1
        );
        assert!(visible_values(DB).is_empty());

        complete(job(b'x'), &shared);
        assert_eq!(visible_values(DB), ["loaded"]);
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

    fn fields(pairs: &[(&str, &str)]) -> Vec<(String, String)> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    #[test]
    fn test_in_progress_import_is_read_from_getslotmigrations_fields() {
        let name = "0123456789abcdef0123456789abcdef01234567";
        let parsed = restored_import_from_fields(&fields(&[
            ("name", name),
            ("operation", "IMPORT"),
            ("slot_ranges", "10-20 30-30"),
            ("state", "occurring-on-primary"),
        ]))
        .expect("an in-flight import");
        assert_eq!(&parsed.0[..], name.as_bytes());
        assert_eq!(parsed.1, RangeSetBlaze::from_iter([10..=20, 30..=30]));
    }

    #[test]
    fn test_exports_finished_jobs_and_malformed_entries_are_not_restored() {
        let name = "0123456789abcdef0123456789abcdef01234567";
        let base = |op: &str, state: &str, ranges: &str, n: &str| {
            fields(&[
                ("name", n),
                ("operation", op),
                ("slot_ranges", ranges),
                ("state", state),
            ])
        };
        assert!(
            restored_import_from_fields(&base("EXPORT", "snapshotting", "1-2", name)).is_none()
        );
        for state in ["success", "failed", "cancelled"] {
            assert!(restored_import_from_fields(&base("IMPORT", state, "1-2", name)).is_none());
        }
        assert!(
            restored_import_from_fields(&base("IMPORT", "occurring-on-primary", "20-10", name))
                .is_none()
        );
        assert!(
            restored_import_from_fields(&base("IMPORT", "occurring-on-primary", "1-2", "short"))
                .is_none()
        );
        assert!(
            restored_import_from_fields(&fields(&[("name", name), ("operation", "IMPORT")]))
                .is_none()
        );
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
