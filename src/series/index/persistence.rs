//! RDB aux-field persistence for the postings index.
//!
//! Design: The whole index snapshot for all dbs is written as a single length-prefixed string via
//! the module type's `aux_save2` callback with `AUX_BEFORE_RDB`, so `aux_load` runs before any key loads
//! and consumes exactly one string from the RDB stream. Any parse failure after that read is soft:
//! the payload is discarded and the index is rebuilt by the existing per-key `loaded` notification
//! path (`index_series_by_key`).
//!
//! Fork safety: `aux_save` runs in the BGSAVE fork child, where a background task holding the
//! postings write lock at fork time would deadlock the child. All db locks are acquired with
//! `try_read`; if any is contended we write nothing at all (`aux_save2` omits the aux field
//! entirely) and the loader falls back to the rebuild path.

use super::postings::{Postings, PostingsBitmap, PostingsIndex, StaleSet};
use super::{TIMESERIES_INDEX, get_db_index, index_series_by_key};
use crate::common::context::{get_current_db, set_current_db};
use crate::common::encoding::{
    try_read_byte_slice, try_read_u8, try_read_uvarint, write_byte_slice, write_u8, write_uvarint,
};
use crate::common::hash::{BuildNoHashHasher, DeterministicHasher};
use crate::common::logging::{log_debug, log_notice, log_warning};
use crate::common::sync::{read_lock, write_lock};
use crate::config::is_index_persist_enabled;
use crate::series::index::IndexKey;
use crate::series::series_data_type::VK_TIME_SERIES_TYPE;
use crate::series::{SeriesRef, TimeSeries};
use croaring::{Bitmap64, Portable};
use std::borrow::Cow;
use std::collections::BTreeMap;
use std::os::raw::c_int;
use std::sync::LazyLock;
use std::sync::atomic::{AtomicBool, Ordering};
use valkey_module::key::ValkeyKey;
use valkey_module::{Context, KeysCursor, MODULE_CONTEXT, RedisModuleIO, ValkeyString, raw};

/// Identifies the aux payload; guards against reading garbage from a foreign/corrupt field.
const INDEX_AUX_MAGIC: &[u8; 4] = b"TSIX";

/// Internal payload format version, independent of `TIMESERIES_TYPE_ENCODING_VERSION`.
/// Any mismatch discards the payload and falls back to the per-key rebuild.
const INDEX_AUX_VERSION: u8 = 1;

/// Databases whose index was preloaded from the aux payload during the current load.
/// Consumed by the post-load reconciliation pass (`LoadingSubevent::Ended`) or discarded
/// on load failure. A lock-free set: `should_skip_load_indexing` probes it once per loaded
/// key, so reads must not contend with each other or with the (rare) inserts from `aux_load`.
static PRELOADED_DBS: LazyLock<papaya::HashSet<i32, BuildNoHashHasher<i32>>> =
    LazyLock::new(papaya::HashSet::default);

/// True between a loading `*Started` subevent and the matching `Ended`/`Failed`. Gates the
/// loaded-series counter and the `loaded`-event short-circuit so neither engages for runtime
/// `RESTORE`/`TS._RESTORE` traffic.
static LOADING_ACTIVE: AtomicBool = AtomicBool::new(false);

/// Per-db summary of the series that came through `rdb_load` during the current load window.
/// Compared against the same summary computed over the preloaded index, both to gate the
/// reconciliation sweep and to detect drift after it (see [`reconcile_preloaded_indexes`]).
/// Lock-free: `note_series_loaded` is called once per series deserialized from the RDB stream.
static LOADED_SERIES_STATS: LazyLock<papaya::HashMap<i32, LoadStats, BuildNoHashHasher<i32>>> =
    LazyLock::new(papaya::HashMap::default);

/// What the loader observed for one db, in a form comparable against `id_to_key` without
/// touching the keyspace.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct LoadStats {
    /// Number of series deserialized for this db.
    count: u64,
    /// Order-independent sum of per-entry `(id, key_name)` hashes. Detects the drift that
    /// `count` alone cannot: an id whose key loaded under a *different name* leaves both
    /// counts equal but changes the digest.
    digest: u64,
    /// Set when any series' key name could not be read, which makes `digest` meaningless.
    /// Forces the sweep rather than trusting a partial digest.
    incomplete: bool,
}

impl LoadStats {
    fn observe(&self, entry_hash: Option<u64>) -> Self {
        Self {
            count: self.count + 1,
            // Wrapping add rather than XOR: ids are unique within a db, but if a stream ever
            // did repeat one, XOR would silently cancel the pair out of the digest.
            digest: self.digest.wrapping_add(entry_hash.unwrap_or(0)),
            incomplete: self.incomplete || entry_hash.is_none(),
        }
    }
}

/// Shared so the per-series load path does not rebuild the seed state on every call.
static DIGEST_HASHER: LazyLock<DeterministicHasher> = LazyLock::new(DeterministicHasher::new);

/// Per-entry digest contribution. Fixed-seed so both sides of the comparison agree; the digest
/// never leaves the process (it is not persisted), so only intra-run consistency is required.
///
/// Not collision-resistant against a chosen-input adversary, and deliberately so: the threat
/// model here is corruption and bugs, not forgery. Engineering a collision would require
/// controlling key names *and* corrupting the RDB, and would buy only a skipped sweep — an
/// index entry that stays stale until the query path's self-heal reaches it.
fn entry_hash(id: SeriesRef, key_name: &[u8]) -> u64 {
    use std::hash::{BuildHasher, Hasher};
    let mut hasher = DIGEST_HASHER.build_hasher();
    hasher.write_u64(id);
    hasher.write(key_name);
    hasher.finish()
}

const RECONCILE_BATCH_SIZE: usize = 256;

type PayloadResult<T> = Result<T, String>;

// ---------------------------------------------------------------------------
// Serialization
// ---------------------------------------------------------------------------

fn write_bitmap(buf: &mut Vec<u8>, bitmap: &PostingsBitmap) {
    let size = bitmap.get_serialized_size_in_bytes::<Portable>();
    write_uvarint(buf, size as u64);
    buf.reserve(size);
    let before = buf.len();
    let _ = bitmap.serialize_into_vec::<Portable>(buf);
    debug_assert_eq!(buf.len() - before, size);
}

fn read_bitmap(buf: &mut &[u8]) -> PayloadResult<PostingsBitmap> {
    let blob = try_read_byte_slice(buf).map_err(|e| e.to_string())?;
    Bitmap64::try_deserialize::<Portable>(blob)
        .ok_or_else(|| "corrupt roaring bitmap blob".to_string())
}

/// Appends one per-db section to `buf`.
fn serialize_postings_section(buf: &mut Vec<u8>, db: i32, postings: &Postings) {
    write_uvarint(buf, db as u64);

    // label_index in tree iteration order (sorted): cache-friendly ART rebuild on load.
    //
    // Keys are written grouped by their shared `name=` prefix, which is emitted once per group
    // rather than once per entry — the on-disk analogue of the prefix sharing the ART gives us
    // in memory. Label *value* cardinality is what grows (hosts, instances); the set of label
    // *names* is small and schema-bound, so a name with k values costs one copy of the name
    // instead of k. Sorted iteration order is what makes this a single pass: a group's entries
    // are necessarily contiguous, because every key in it shares the prefix up to and including
    // the first `=`, and no other group's prefix can fall between them (a prefix never contains
    // `=` except as its last byte, so no prefix is a prefix of another).
    let stale = &postings.stale_ids;
    // Group directory: (prefix, entry count) pairs, one per distinct label name.
    let mut groups: Vec<u8> = Vec::new();
    let mut group_count: u64 = 0;
    let mut entries: Vec<u8> = Vec::new();
    let mut open_group: Option<(&[u8], u64)> = None;
    for (key, bitmap) in postings.label_index.iter() {
        if bitmap.is_empty() {
            continue;
        }

        // Stale ids are subtracted here rather than persisted: `mark_ids_as_stale` already cleans
        // `id_to_key` and `all_postings` at mark time, so the label bitmaps are the only structures
        // still carrying stale ids — persisting them would persist a cleanup obligation. Subtracting
        // yields exactly the state a completed GC drain would produce. A stale id whose key is still
        // in the RDB (a fork can land mid-ASM-export, before the engine's lazy delete) is
        // resurrected by the post-load count-verification scan, matching the keyspace either way.
        // The subtraction only costs anything when `stale_ids` is non-empty (rare: the cron GC
        // drains continuously); entries that become empty are dropped, so each group's count is
        // written only once its entries have been serialized.
        let cleaned: Cow<PostingsBitmap> = stale.mask_cow(Cow::Borrowed(bitmap));
        if cleaned.is_empty() {
            continue;
        }
        // `as_str` strips the NUL sentinel; `IndexKey::from(&[u8])` re-appends it on load.
        let full = key.as_str().as_bytes();
        // Split *after* the first `=` so that `prefix + suffix` reproduces the key byte for
        // byte, whatever it contains — a value with an `=` in it, or a key with none at all.
        // Nothing here has to agree with `IndexKey::for_label_value` about where the name ends.
        let split_at = full
            .iter()
            .position(|b| *b == b'=')
            .map_or(full.len(), |i| i + 1);
        let (prefix, suffix) = full.split_at(split_at);

        match open_group {
            Some((open, ref mut count)) if open == prefix => *count += 1,
            _ => {
                if let Some((open, count)) = open_group {
                    write_byte_slice(&mut groups, open);
                    write_uvarint(&mut groups, count);
                    group_count += 1;
                }
                open_group = Some((prefix, 1));
            }
        }
        write_byte_slice(&mut entries, suffix);
        write_bitmap(&mut entries, &cleaned);
    }
    if let Some((open, count)) = open_group {
        write_byte_slice(&mut groups, open);
        write_uvarint(&mut groups, count);
        group_count += 1;
    }

    write_uvarint(buf, group_count);
    buf.extend_from_slice(&groups);
    buf.extend_from_slice(&entries);

    // id_to_key in ascending id order: ids share their high epoch bits and increment densely,
    // so varint deltas stay small.
    write_uvarint(buf, postings.id_to_key.len() as u64);
    let mut prev_id: SeriesRef = 0;
    for (id, key) in postings.id_to_key.iter() {
        write_uvarint(buf, id - prev_id);
        prev_id = *id;
        write_byte_slice(buf, key.as_ref());
    }

    write_bitmap(buf, &postings.all_postings);
}

fn deserialize_postings_section(buf: &mut &[u8]) -> PayloadResult<(i32, Postings)> {
    let db = try_read_uvarint(buf).map_err(|e| e.to_string())?;
    let db = i32::try_from(db).map_err(|_| format!("invalid db number {db}"))?;

    // Group directory first (see `serialize_postings_section`), then the entries for each group
    // in the same order. `with_capacity` is clamped: the count comes from the payload, and a
    // corrupt one must not turn into an unbounded allocation before the read that rejects it.
    let group_count = try_read_uvarint(buf).map_err(|e| e.to_string())? as usize;
    let mut groups: Vec<(&[u8], usize)> = Vec::with_capacity(group_count.min(64));
    for _ in 0..group_count {
        let prefix = try_read_byte_slice(buf).map_err(|e| e.to_string())?;
        let count = try_read_uvarint(buf).map_err(|e| e.to_string())? as usize;
        groups.push((prefix, count));
    }

    let mut label_index = PostingsIndex::new();
    let mut key_bytes: Vec<u8> = Vec::new();
    for (prefix, count) in groups {
        for _ in 0..count {
            let suffix = try_read_byte_slice(buf).map_err(|e| e.to_string())?;
            let bitmap = read_bitmap(buf)?;
            key_bytes.clear();
            key_bytes.reserve(prefix.len() + suffix.len());
            key_bytes.extend_from_slice(prefix);
            key_bytes.extend_from_slice(suffix);
            let key = IndexKey::from(key_bytes.as_slice());
            label_index
                .try_insert(key, bitmap)
                .map_err(|e| format!("label index insert failed: {e}"))?;
        }
    }

    let id_count = try_read_uvarint(buf).map_err(|e| e.to_string())? as usize;
    let mut id_to_key = BTreeMap::new();
    let mut prev_id: SeriesRef = 0;
    for _ in 0..id_count {
        let delta = try_read_uvarint(buf).map_err(|e| e.to_string())?;
        let id = prev_id
            .checked_add(delta)
            .ok_or_else(|| "series id overflow".to_string())?;
        prev_id = id;
        let key = try_read_byte_slice(buf).map_err(|e| e.to_string())?;
        id_to_key.insert(id, key.to_vec().into_boxed_slice());
    }

    let all_postings = read_bitmap(buf)?;

    Ok((
        db,
        Postings {
            label_index,
            id_to_key,
            // Stale ids were subtracted at save time; the loaded index starts clean.
            stale_ids: StaleSet::default(),
            all_postings,
        },
    ))
}

/// Serializes every non-empty db index into a single payload buffer.
/// Returns `None` if any db's postings lock is contended (possible fork-time writer:
/// skip persisting entirely rather than risk deadlock or write a torn snapshot).
fn build_aux_payload() -> Option<Vec<u8>> {
    let map = TIMESERIES_INDEX.pin();
    let mut dbs: Vec<i32> = map.keys().copied().collect();
    dbs.sort_unstable();

    let mut sections: Vec<u8> = Vec::new();
    let mut section_count: u64 = 0;

    for db in dbs {
        let Some(index) = map.get(&db) else {
            continue;
        };
        let Ok(postings) = index.inner.try_read() else {
            log_warning(format!(
                "Postings lock for db {db} contended at aux save time; skipping index persistence"
            ));
            return None;
        };
        // Nothing worth persisting: with no live series, the payload would only carry
        // stale leftovers that are dropped at save time anyway.
        if postings.id_to_key.is_empty() {
            continue;
        }
        serialize_postings_section(&mut sections, db, &postings);
        section_count += 1;
    }

    if section_count == 0 {
        return None;
    }

    let mut buf = Vec::with_capacity(sections.len() + 16);
    buf.extend_from_slice(INDEX_AUX_MAGIC);
    write_u8(&mut buf, INDEX_AUX_VERSION);
    write_uvarint(&mut buf, section_count);
    buf.extend_from_slice(&sections);
    Some(buf)
}

fn parse_aux_payload(mut buf: &[u8]) -> PayloadResult<Vec<(i32, Postings)>> {
    let buf = &mut buf;

    let magic = try_read_byte_slice_exact(buf, INDEX_AUX_MAGIC.len())?;
    if magic != INDEX_AUX_MAGIC {
        return Err("bad magic".to_string());
    }
    let version = try_read_u8(buf).map_err(|e| e.to_string())?;
    if version != INDEX_AUX_VERSION {
        return Err(format!(
            "unsupported index payload version {version} (expected {INDEX_AUX_VERSION})"
        ));
    }

    let section_count = try_read_uvarint(buf).map_err(|e| e.to_string())? as usize;
    let mut sections = Vec::with_capacity(section_count.min(16));
    for _ in 0..section_count {
        sections.push(deserialize_postings_section(buf)?);
    }
    if !buf.is_empty() {
        return Err(format!("{} trailing bytes after payload", buf.len()));
    }
    Ok(sections)
}

fn try_read_byte_slice_exact<'a>(buf: &mut &'a [u8], len: usize) -> PayloadResult<&'a [u8]> {
    if buf.len() < len {
        return Err(format!(
            "not enough bytes: {} available, {len} requested",
            buf.len()
        ));
    }
    let (head, tail) = buf.split_at(len);
    *buf = tail;
    Ok(head)
}

// ---------------------------------------------------------------------------
// RDB aux save / load
// ---------------------------------------------------------------------------

/// `aux_save2` body. Runs in the BGSAVE fork child; must not block on locks.
/// Writing nothing at all makes `aux_save2` omit the aux field from the RDB.
pub(crate) fn save_index_to_rdb(rdb: *mut RedisModuleIO) {
    if !is_index_persist_enabled() {
        return;
    }
    let Some(payload) = build_aux_payload() else {
        return;
    };
    log_debug(format!(
        "Persisting postings index aux payload ({} bytes)",
        payload.len()
    ));
    raw::save_slice(rdb, &payload);
}

/// `aux_load` body. Must consume exactly the string written by `save_index_to_rdb`; only a
/// failure to read it at all is a hard error (the RDB stream would be desynced). Every error
/// after that point discards the payload and falls back to the per-key rebuild.
pub(crate) fn load_index_from_rdb(rdb: *mut RedisModuleIO) -> c_int {
    let Ok(buffer) = raw::load_string_buffer(rdb) else {
        log_warning("Failed to read postings index aux payload from RDB");
        return raw::Status::Err as c_int;
    };

    if !is_index_persist_enabled() {
        log_notice(
            "ts-index-persist is disabled; discarding persisted postings index and rebuilding",
        );
        return raw::Status::Ok as c_int;
    }

    match parse_aux_payload(buffer.as_ref()) {
        Ok(sections) => {
            let preloaded = PRELOADED_DBS.pin();
            for (db, postings) in sections {
                let series_count = postings.id_to_key.len();
                let index = get_db_index(db);
                *write_lock(&index.inner) = postings;
                preloaded.insert(db);
                log_notice(format!(
                    "Preloaded postings index for db {db} ({series_count} series)"
                ));
            }
        }
        Err(e) => {
            log_warning(format!(
                "Discarding persisted postings index ({e}); falling back to per-key rebuild"
            ));
        }
    }
    raw::Status::Ok as c_int
}

// ---------------------------------------------------------------------------
// Load lifecycle & fast path
// ---------------------------------------------------------------------------

/// Loading `RdbStarted`/`AofStarted`/`ReplStarted`: open the load window and reset per-load state.
pub(crate) fn on_loading_started() {
    LOADING_ACTIVE.store(true, Ordering::SeqCst);
    LOADED_SERIES_STATS.pin().clear();
    // Defensive: any leftover preload state from an earlier load cycle is stale by now.
    PRELOADED_DBS.pin().clear();
}

/// Loading `Ended`: close the load window and kick off reconciliation of preloaded dbs.
pub(crate) fn on_loading_ended() {
    LOADING_ACTIVE.store(false, Ordering::SeqCst);
    reconcile_preloaded_indexes();
}

/// Loading `Failed`: close the load window and drop preloaded state (see
/// [`discard_preloaded_indexes`]).
pub(crate) fn on_loading_failed() {
    LOADING_ACTIVE.store(false, Ordering::SeqCst);
    LOADED_SERIES_STATS.pin().clear();
    discard_preloaded_indexes();
}

#[inline]
pub(crate) fn is_loading_active() -> bool {
    LOADING_ACTIVE.load(Ordering::Relaxed)
}

/// True when the `loaded` keyspace notification for a key in `db` can be skipped outright:
/// we are inside a load window and this db's index came from the aux payload. `rdb_load` has
/// already assigned `_db` and counted the series; post-load verification covers any drift.
/// This is the fast path that avoids the per-key `RedisModuleString` allocation and keyspace
/// lookup entirely (for every key type — `loaded` fires for non-timeseries keys too).
pub(crate) fn should_skip_load_indexing(db: i32) -> bool {
    is_loading_active() && PRELOADED_DBS.pin().contains(&db)
}

/// Verifies the module APIs this file depends on are present, at load time rather than at
/// first use. `GetDbIdFromIO` is available on every server version the module supports (see
/// `TIMESERIES_MIN_SUPPORTED_VERSION`), and the load-reconciliation logic treats its output as
/// authoritative: `LOADED_SERIES_STATS` gates whether the post-load sweep runs at all, so a
/// silently absent symbol would mean permanently under-counted loads and a keyspace scan after
/// every RDB load. Refusing to load beats degrading quietly.
pub(crate) fn check_required_module_apis() -> Result<(), &'static str> {
    if unsafe { raw::RedisModule_GetDbIdFromIO }.is_none() {
        return Err("RedisModule_GetDbIdFromIO");
    }
    if unsafe { raw::RedisModule_GetKeyNameFromIO }.is_none() {
        return Err("RedisModule_GetKeyNameFromIO");
    }
    Ok(())
}

/// Digest contribution for the series currently being deserialized, from the key name the
/// engine is loading it under. `None` when the name is unavailable, which degrades that db's
/// digest to `incomplete` and forces the sweep rather than producing a wrong match.
fn loaded_entry_hash(rdb: *mut RedisModuleIO, id: SeriesRef) -> Option<u64> {
    // Presence is a load-time invariant (`check_required_module_apis`).
    let get_key_name = unsafe { raw::RedisModule_GetKeyNameFromIO }
        .expect("RedisModule_GetKeyNameFromIO unavailable");
    let key = unsafe { get_key_name(rdb) };
    if key.is_null() {
        return None;
    }
    let mut len: usize = 0;
    let ptr = raw::string_ptr_len(key, &mut len);
    if ptr.is_null() {
        return None;
    }
    // Borrowed for the duration of this call only: the engine owns the string, and we neither
    // retain nor free it.
    let name = unsafe { std::slice::from_raw_parts(ptr as *const u8, len) };
    Some(entry_hash(id, name))
}

/// Called from `rdb_load_series` for every series deserialized from an RDB stream: assigns
/// `series._db` from the IO context (previously done by the `loaded` notification handler,
/// which required opening the key), and counts the series toward the current load window's
/// per-db total. Payloads with no db context (`RESTORE` / `TS._RESTORE` strings) are skipped;
/// their callers assign `_db` themselves.
pub(crate) fn observe_series_rdb_load(rdb: *mut RedisModuleIO, series: &mut TimeSeries) {
    // Presence is a load-time invariant (`check_required_module_apis`).
    let get_db_id =
        unsafe { raw::RedisModule_GetDbIdFromIO }.expect("RedisModule_GetDbIdFromIO unavailable");
    // A negative db means there is no IO context to read it from: a `RESTORE`/`TS._RESTORE`
    // payload rather than an RDB stream. Not an error — those callers assign `_db` themselves.
    let db = unsafe { get_db_id(rdb) };
    if db < 0 {
        return;
    }
    series._db = Some(db);
    if LOADING_ACTIVE.load(Ordering::Relaxed) {
        note_series_loaded(db, loaded_entry_hash(rdb, series.id));
    }
}

fn note_series_loaded(db: i32, entry_hash: Option<u64>) {
    let stats = LOADED_SERIES_STATS.pin();
    stats.update_or_insert(
        db,
        |current| current.observe(entry_hash),
        LoadStats::default().observe(entry_hash),
    );
}

/// Drains `LOADED_SERIES_STATS`: snapshot the per-db stats, then clear. Not atomic with
/// respect to a concurrent `note_series_loaded`, but there is none — this is only called from
/// the loading-event handler, after the load window (and thus all `rdb_load` traffic) has ended.
fn take_loaded_stats() -> Vec<(i32, LoadStats)> {
    let guard = LOADED_SERIES_STATS.pin();
    let stats: Vec<(i32, LoadStats)> = guard.iter().map(|(db, st)| (*db, *st)).collect();
    guard.clear();
    stats
}

/// Drains `PRELOADED_DBS`: snapshot the members, then clear. Not atomic with respect to a
/// concurrent `aux_load` insert, but that race cannot happen in practice — this is only called
/// from the loading-event handler, after `aux_load` for the current load window has finished.
fn take_preloaded_dbs() -> Vec<i32> {
    let guard = PRELOADED_DBS.pin();
    let dbs: Vec<i32> = guard.iter().copied().collect();
    guard.clear();
    dbs
}

// ---------------------------------------------------------------------------
// Post-load reconciliation
// ---------------------------------------------------------------------------

/// Runs after a successful load (`LoadingSubevent::Ended`). A preloaded index can contain
/// "dangling" ids whose key never made it into the keyspace (e.g. its `rdb_load` was skipped
/// because its TTL had already elapsed); nothing in the query path stale-marks those, because
/// a dangling id's `id_to_key` entry is intact — that entry is exactly what was serialized —
/// so `get_key_by_id` still answers `Some`. The index-only commands (`TS.CARD` and
/// `TS.QUERYINDEX` without a date filter, the label commands) never open a key and so never
/// self-heal: they would over-count and emit phantom key names indefinitely. Hence the sweep.
///
/// The sweep is gated on a digest comparison rather than run unconditionally. A cardinality
/// check alone would not be sound: it cannot see *identity* drift, where the payload and the
/// keyspace disagree about a key's *name* (RDB body corruption, a fork landing mid-write),
/// leaving an id dangling with both counts equal — the case
/// `test_dangling_id_triggers_reconciliation_and_repair_scan` constructs. So the loader also
/// accumulates a per-db digest over each series' `(id, key_name)` as it deserializes them
/// ([`LoadStats`]), and the same digest is recomputed here over `id_to_key`. Agreement means
/// the index describes exactly the set of series that loaded, under exactly the names they
/// loaded under, so there is nothing for the sweep to find.
///
/// The digest costs one hash per series on each side and touches no keys, versus one keyspace
/// probe per series (each under the GIL) for the sweep it replaces on a clean load.
///
/// Runtime traffic between load end and the digest can skew either side (a `DEL` unindexes a
/// key, a `TS.CREATE` adds an id the loader never saw). That costs a spurious sweep, which is
/// safe and bounded — never a spurious skip, since any such change alters the digest.
fn reconcile_preloaded_indexes() {
    let dbs: Vec<i32> = take_preloaded_dbs();
    if dbs.is_empty() {
        return;
    }
    let stats = take_loaded_stats();

    // Off the main thread: the sweep opens every indexed key once. Runs on the module's
    // thread pool (not a detached `std::thread`) so it participates in the same thread
    // lifecycle as every other background job, and checks `is_shutting_down()` between
    // batches — an aborted sweep is safe, since ids it never reached are either valid or
    // will be stale-marked by the query path's self-heal.
    crate::common::threads::spawn(move || {
        for db in dbs {
            if crate::is_shutting_down() {
                return;
            }
            // Absent from the map means zero series arrived for this db, not "unknown":
            // `observe_series_rdb_load` is called for every series in the stream, and neither
            // its db nor its key-name lookup can be missing (both checked at load time by
            // `check_required_module_apis`).
            let loaded = stats
                .iter()
                .find(|(d, _)| *d == db)
                .map(|(_, st)| *st)
                .unwrap_or_default();

            let indexed = read_indexed_stats(db);
            if !loaded.incomplete && indexed == loaded {
                // Notice rather than debug: this is the once-per-db outcome that says the
                // sweep was skipped, and it is the only externally visible evidence that the
                // digest fast path is working.
                log_notice(format!(
                    "Postings index for db {db}: {} indexed series match the loaded set by digest; skipping reconciliation sweep",
                    indexed.count
                ));
                continue;
            }

            log_notice(format!(
                "Postings index for db {db}: index ({} series) does not match the loaded set ({} series{}); reconciling",
                indexed.count,
                loaded.count,
                if loaded.incomplete {
                    ", digest unavailable"
                } else {
                    ""
                }
            ));
            reconcile_db(db);
            verify_and_repair_db(db, loaded.count);
        }
    });
}

/// The index side of the [`LoadStats`] comparison, computed over `id_to_key`. Hashing only —
/// holds the read lock without touching the keyspace or the GIL. Never `incomplete`: every
/// entry here has both an id and a name by construction.
fn read_indexed_stats(db: i32) -> LoadStats {
    let index = get_db_index(db);
    let postings = read_lock(&index.inner);
    let mut stats = LoadStats::default();
    for (id, key) in postings.id_to_key.iter() {
        stats = stats.observe(Some(entry_hash(*id, key.as_ref())));
    }
    stats
}

fn read_indexed_count(db: i32) -> u64 {
    let index = get_db_index(db);
    let postings = read_lock(&index.inner);
    postings.count() as u64
}

/// Post-sweep verification for a preloaded db. `reconcile_db` has just established that every
/// id remaining in `id_to_key` maps to a live key with a matching series id — i.e. the index is
/// a *subset* of the loaded keyspace. Equal cardinality therefore proves set equality, so a
/// count match means no key was missed by the preload. On mismatch, scan the db's keyspace and
/// index anything with `has_id == false` (the same per-key path the fast path skipped).
///
/// Runtime traffic between load end and this check can legitimately skew the counts (a DEL
/// removes a key and unindexes it; a TS.CREATE adds an id the loader never counted). That only
/// makes the scan fire spuriously — `index_series_by_key` is guarded, so the scan is always
/// safe, just not free.
fn verify_and_repair_db(db: i32, loaded_count: u64) {
    let indexed_count = read_indexed_count(db);

    if indexed_count == loaded_count {
        log_debug(format!(
            "Postings index verification for db {db}: {indexed_count} indexed series match the loaded count"
        ));
        return;
    }

    log_warning(format!(
        "Postings index verification for db {db}: {indexed_count} indexed vs {loaded_count} loaded; scanning keyspace to repair"
    ));

    let scan_callback = |ctx: &Context, key_name: ValkeyString, _key: Option<&ValkeyKey>| {
        index_series_by_key(ctx, key_name.as_slice());
    };

    let cursor = KeysCursor::new();
    let mut more = true;
    while more {
        if crate::is_shutting_down() {
            log_notice(format!(
                "Postings index repair scan for db {db} aborted by shutdown"
            ));
            return;
        }
        // Lock per scan bucket so the main thread is not starved for the whole scan.
        let ctx = MODULE_CONTEXT.lock();
        let save_db = get_current_db(&ctx);
        set_current_db(&ctx, db);
        more = cursor.scan(&ctx, &scan_callback);
        set_current_db(&ctx, save_db);
    }

    let repaired_count = read_indexed_count(db);
    log_notice(format!(
        "Postings index repair scan for db {db} finished: {indexed_count} -> {repaired_count} indexed series"
    ));
}

fn reconcile_db(db: i32) {
    let mut cursor: SeriesRef = 0;
    let mut checked = 0usize;
    let mut dangling = 0usize;

    loop {
        if crate::is_shutting_down() {
            log_notice(format!(
                "Postings index reconciliation for db {db} aborted by shutdown after {checked} ids"
            ));
            return;
        }
        // Snapshot a window of (id, key) pairs under the read lock, then drop it before
        // touching the keyspace or taking the write lock.
        let window: Vec<(SeriesRef, Box<[u8]>)> = {
            let index = get_db_index(db);
            let postings = read_lock(&index.inner);
            postings
                .id_to_key
                .range(cursor..)
                .take(RECONCILE_BATCH_SIZE)
                .map(|(id, key)| (*id, key.clone()))
                .collect()
        };
        let Some(&(last_id, _)) = window.last() else {
            break;
        };
        cursor = last_id + 1;

        let mut missing: Vec<SeriesRef> = Vec::new();
        {
            let ctx = MODULE_CONTEXT.lock();
            let save_db = get_current_db(&ctx);
            set_current_db(&ctx, db);
            for (id, key) in &window {
                let valkey_key = ctx.create_string(key.as_ref());
                let key_handle = ctx.open_key(&valkey_key);
                match key_handle.get_value::<TimeSeries>(&VK_TIME_SERIES_TYPE) {
                    Ok(Some(series)) if series.id == *id => {}
                    _ => missing.push(*id),
                }
            }
            set_current_db(&ctx, save_db);
        }

        checked += window.len();
        if !missing.is_empty() {
            dangling += missing.len();
            let index = get_db_index(db);
            let mut postings = write_lock(&index.inner);
            postings.mark_ids_as_stale(&missing);
        }
    }

    if dangling > 0 {
        log_notice(format!(
            "Postings index reconciliation for db {db}: {checked} ids checked, {dangling} dangling ids marked stale"
        ));
    } else {
        log_debug(format!(
            "Postings index reconciliation for db {db}: {checked} ids checked, none dangling"
        ));
    }
}

/// Runs when a load fails (`LoadingSubevent::Failed`). The keyspace state after a failed load is
/// engine-dependent (startup aborts; a replica may restore its pre-sync dataset), so a preloaded
/// index cannot be trusted — drop it and let the natural indexing paths rebuild.
fn discard_preloaded_indexes() {
    let dbs: Vec<i32> = take_preloaded_dbs();
    for db in dbs {
        let index = get_db_index(db);
        write_lock(&index.inner).clear();
        log_warning(format!(
            "Load failed; discarded preloaded postings index for db {db}"
        ));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_postings() -> Postings {
        let mut postings = Postings::default();
        let mut bmp_a = PostingsBitmap::new();
        bmp_a.add_many(&[1, 2, 3]);
        let mut bmp_b = PostingsBitmap::new();
        bmp_b.add_many(&[2, 3]);
        postings
            .label_index
            .try_insert(IndexKey::for_label_value("region", "us-east-1"), bmp_a)
            .unwrap();
        postings
            .label_index
            .try_insert(IndexKey::for_label_value("service", "api"), bmp_b)
            .unwrap();
        for (id, key) in [(1u64, "ts:one"), (2, "ts:two"), (3, "ts:three")] {
            postings
                .id_to_key
                .insert(id, key.as_bytes().to_vec().into_boxed_slice());
            postings.all_postings.add(id);
        }
        postings
    }

    fn assert_postings_eq(a: &Postings, b: &Postings) {
        assert_eq!(a.id_to_key, b.id_to_key);
        assert_eq!(a.all_postings, b.all_postings);
        assert_eq!(a.stale_ids, b.stale_ids);
        assert_eq!(a.label_index.len(), b.label_index.len());
        for ((ka, va), (kb, vb)) in a.label_index.iter().zip(b.label_index.iter()) {
            assert_eq!(ka, kb);
            assert_eq!(va, vb);
        }
    }

    fn build_payload(sections: &[(i32, &Postings)]) -> Vec<u8> {
        let mut body = Vec::new();
        for (db, postings) in sections {
            serialize_postings_section(&mut body, *db, postings);
        }
        let mut buf = Vec::new();
        buf.extend_from_slice(INDEX_AUX_MAGIC);
        write_u8(&mut buf, INDEX_AUX_VERSION);
        write_uvarint(&mut buf, sections.len() as u64);
        buf.extend_from_slice(&body);
        buf
    }

    #[test]
    fn payload_roundtrip_single_db() {
        let postings = sample_postings();
        let payload = build_payload(&[(0, &postings)]);
        let sections = parse_aux_payload(&payload).unwrap();
        assert_eq!(sections.len(), 1);
        assert_eq!(sections[0].0, 0);
        assert_postings_eq(&sections[0].1, &postings);
    }

    /// Stale ids are subtracted at save time: the loaded index must look like the state a
    /// completed GC drain would produce — clean bitmaps, no stale set, empty entries dropped.
    #[test]
    fn save_subtracts_stale_ids() {
        let mut postings = sample_postings();
        // A label whose bitmap holds only the id about to go stale: its entry must vanish.
        let mut only_two = PostingsBitmap::new();
        only_two.add(2);
        postings
            .label_index
            .try_insert(IndexKey::for_label_value("host", "h2"), only_two)
            .unwrap();
        // Removes id 2 from id_to_key/all_postings, leaves it in the label bitmaps.
        postings.mark_ids_as_stale(&[2]);
        assert!(postings.stale_ids.contains(2));

        let payload = build_payload(&[(0, &postings)]);
        let sections = parse_aux_payload(&payload).unwrap();
        let loaded = &sections[0].1;

        assert!(loaded.stale_ids.is_empty(), "stale set is not persisted");
        assert!(loaded.get_key_by_id(2).is_none());
        assert!(!loaded.all_postings.contains(2));
        for (key, bitmap) in loaded.label_index.iter() {
            assert!(!bitmap.contains(2), "stale id survived in {key}");
            assert!(!bitmap.is_empty());
        }
        assert!(
            loaded
                .label_index
                .get(&IndexKey::for_label_value("host", "h2"))
                .is_none(),
            "entry that became empty must be dropped"
        );
        // The live ids are untouched.
        assert_eq!(loaded.count(), 2);
        assert!(loaded.all_postings.contains(1) && loaded.all_postings.contains(3));
    }

    fn count_occurrences(haystack: &[u8], needle: &[u8]) -> usize {
        haystack
            .windows(needle.len())
            .filter(|w| *w == needle)
            .count()
    }

    /// The point of the grouped encoding: a label name shared by many values is written once,
    /// not once per value.
    #[test]
    fn label_name_is_written_once_per_group() {
        let mut postings = Postings::default();
        for i in 0..200u64 {
            let mut bitmap = PostingsBitmap::new();
            bitmap.add(i);
            postings
                .label_index
                .try_insert(
                    IndexKey::for_label_value("region", &format!("us-east-{i}")),
                    bitmap,
                )
                .unwrap();
        }

        let payload = build_payload(&[(0, &postings)]);
        assert_eq!(
            count_occurrences(&payload, b"region="),
            1,
            "the shared label name must appear exactly once in the payload"
        );
        assert_postings_eq(&parse_aux_payload(&payload).unwrap()[0].1, &postings);
    }

    /// `prefix ++ suffix` must reproduce every key byte for byte, including shapes that
    /// `IndexKey::for_label_value` never produces: a value containing `=`, and a key with no
    /// `=` at all. Also covers the grouping edge cases — a name that extends another name, and
    /// a group whose single entry has an empty suffix.
    #[test]
    fn roundtrip_preserves_unusual_key_shapes() {
        let keys = ["bare", "a=1", "a=2", "ab=1", "eq=x=y", "eq=x=y=z"];
        let mut postings = Postings::default();
        for (i, key) in keys.iter().enumerate() {
            let mut bitmap = PostingsBitmap::new();
            bitmap.add(i as u64 + 1);
            postings
                .label_index
                .try_insert(IndexKey::from(*key), bitmap)
                .unwrap();
        }

        let payload = build_payload(&[(0, &postings)]);
        let sections = parse_aux_payload(&payload).unwrap();
        assert_postings_eq(&sections[0].1, &postings);

        let loaded: Vec<String> = sections[0]
            .1
            .label_index
            .iter()
            .map(|(k, _)| k.to_string())
            .collect();
        let mut expected: Vec<String> = keys.iter().map(|k| k.to_string()).collect();
        expected.sort();
        assert_eq!(loaded, expected);

        // The two `eq=` keys were grouped despite the `=` inside their values, and the
        // `=`-less key formed its own group rather than being merged into a neighbour.
        assert_eq!(count_occurrences(&payload, b"eq="), 1);
        assert_eq!(count_occurrences(&payload, b"bare"), 1);
    }

    #[test]
    fn payload_roundtrip_multiple_dbs() {
        let postings_a = sample_postings();
        let postings_b = Postings::default();
        let payload = build_payload(&[(0, &postings_a), (5, &postings_b)]);
        let sections = parse_aux_payload(&payload).unwrap();
        assert_eq!(sections.len(), 2);
        assert_eq!(sections[0].0, 0);
        assert_eq!(sections[1].0, 5);
        assert_postings_eq(&sections[0].1, &postings_a);
        assert_postings_eq(&sections[1].1, &postings_b);
    }

    #[test]
    fn payload_roundtrip_empty_postings() {
        let postings = Postings::default();
        let payload = build_payload(&[(2, &postings)]);
        let sections = parse_aux_payload(&payload).unwrap();
        assert_eq!(sections.len(), 1);
        assert_eq!(sections[0].0, 2);
        assert_postings_eq(&sections[0].1, &postings);
    }

    #[test]
    fn rejects_bad_magic() {
        let postings = sample_postings();
        let mut payload = build_payload(&[(0, &postings)]);
        payload[0] ^= 0xFF;
        assert!(parse_aux_payload(&payload).is_err());
    }

    #[test]
    fn rejects_unknown_version() {
        let postings = sample_postings();
        let mut payload = build_payload(&[(0, &postings)]);
        payload[INDEX_AUX_MAGIC.len()] = INDEX_AUX_VERSION + 1;
        assert!(parse_aux_payload(&payload).is_err());
    }

    #[test]
    fn rejects_truncated_payload() {
        let postings = sample_postings();
        let payload = build_payload(&[(0, &postings)]);
        for len in 0..payload.len() {
            assert!(
                parse_aux_payload(&payload[..len]).is_err(),
                "truncation at {len} bytes should fail"
            );
        }
    }

    #[test]
    fn rejects_trailing_garbage() {
        let postings = sample_postings();
        let mut payload = build_payload(&[(0, &postings)]);
        payload.push(0xAB);
        assert!(parse_aux_payload(&payload).is_err());
    }

    #[test]
    fn rejects_corrupt_bitmap_blob() {
        // A blob whose declared length is intact but whose contents are not a valid portable
        // bitmap: the leading u64 bucket count is absurd relative to the remaining bytes.
        // (Bit flips that still decode as *some* valid bitmap are the RDB CRC's job to catch.)
        let mut buf = Vec::new();
        write_byte_slice(&mut buf, &[0xFF; 9]);
        let mut slice = buf.as_slice();
        assert!(read_bitmap(&mut slice).is_err());
    }

    /// The repair-scan scenario: the reconciliation sweep marks an id stale, then the keyspace
    /// scan re-indexes the same id under its real key. Re-indexing must revoke the stale marking,
    /// or the next GC drain would strip the id from the label bitmaps while it stays in
    /// `id_to_key` — and the outcome would depend on GC timing.
    #[test]
    fn reindex_revokes_stale_marking() {
        let mut postings = Postings::default();
        let mut ts = TimeSeries::new();
        ts.id = 424242;
        ts.labels = r#"metric{region="us-east-4"}"#.parse().unwrap();

        postings.index_timeseries(&ts, b"ts:4");
        postings.mark_ids_as_stale(&[ts.id]);
        assert!(postings.stale_ids.contains(ts.id));

        postings.index_timeseries(&ts, b"tz:4");
        assert!(
            !postings.stale_ids.contains(ts.id),
            "re-indexing must revoke the stale marking"
        );

        // A GC drain must now be a no-op for this id.
        let mut cursor = None;
        while let Some(next) = postings.remove_stale_ids(cursor.take(), 16) {
            cursor = Some(next);
        }
        let key = IndexKey::for_label_value("region", "us-east-4");
        let bitmap = postings
            .label_index
            .get(&key)
            .expect("label bitmap must survive the GC drain");
        assert!(bitmap.contains(ts.id));
        assert_eq!(
            postings.get_key_by_id(ts.id).map(|k| k.as_ref().to_vec()),
            Some(b"tz:4".to_vec())
        );
        assert_eq!(postings.count(), 1);
    }

    /// Single test for the whole load-window lifecycle: the statics involved are process-global,
    /// so exercising them from one test avoids intra-suite races under parallel test threads.
    #[test]
    fn load_window_lifecycle() {
        // Db numbers chosen to not collide with other tests sharing TIMESERIES_INDEX.
        const DB_A: i32 = 91;
        const DB_B: i32 = 92;

        on_loading_started();
        assert!(
            !should_skip_load_indexing(DB_A),
            "no skip before this db's index is preloaded"
        );

        // Simulate aux preload of DB_A and some rdb_load traffic.
        PRELOADED_DBS.pin().insert(DB_A);
        note_series_loaded(DB_A, Some(entry_hash(1, b"ts:one")));
        note_series_loaded(DB_A, Some(entry_hash(2, b"ts:two")));
        note_series_loaded(DB_B, Some(entry_hash(3, b"ts:three")));

        assert!(should_skip_load_indexing(DB_A));
        assert!(
            !should_skip_load_indexing(DB_B),
            "dbs without a preloaded index keep the per-key path"
        );

        let mut stats = take_loaded_stats();
        stats.sort_unstable_by_key(|(db, _)| *db);
        let counts: Vec<(i32, u64)> = stats.iter().map(|(db, st)| (*db, st.count)).collect();
        assert_eq!(counts, vec![(DB_A, 2), (DB_B, 1)]);
        // Digest accumulation is order-independent and matches the index-side computation.
        assert_eq!(
            stats[0].1.digest,
            entry_hash(2, b"ts:two").wrapping_add(entry_hash(1, b"ts:one"))
        );
        assert!(stats.iter().all(|(_, st)| !st.incomplete));
        assert!(take_loaded_stats().is_empty(), "stats are consumed on take");

        // Failure path: window closes, preloaded state is dropped.
        on_loading_failed();
        assert!(
            !should_skip_load_indexing(DB_A),
            "no skip outside a load window"
        );
        assert!(PRELOADED_DBS.pin().is_empty());

        // A new load window starts clean.
        note_series_loaded(DB_A, Some(entry_hash(1, b"ts:one")));
        on_loading_started();
        assert!(take_loaded_stats().is_empty());
        LOADING_ACTIVE.store(false, Ordering::SeqCst);
    }

    /// The property the digest exists for: a key that loads under a different name than the
    /// index believes leaves the counts equal, so only the digest can catch it. This is the
    /// unit-level analogue of `test_dangling_id_triggers_reconciliation_and_repair_scan`.
    #[test]
    fn digest_detects_rename_that_count_alone_misses() {
        let indexed = [(1u64, &b"ts:one"[..]), (2, b"ts:two"), (3, b"ts:three")];
        // Same ids, same cardinality, one key loaded under a different name.
        let loaded = [(1u64, &b"ts:one"[..]), (2, b"ts:two"), (3, b"tz:three")];

        let summarize = |entries: &[(u64, &[u8])]| {
            entries.iter().fold(LoadStats::default(), |st, (id, key)| {
                st.observe(Some(entry_hash(*id, key)))
            })
        };

        let a = summarize(&indexed);
        let b = summarize(&loaded);
        assert_eq!(a.count, b.count, "cardinality is blind to the rename");
        assert_ne!(a.digest, b.digest, "digest must catch the rename");
        assert_ne!(a, b);
    }

    /// Digest accumulation must not depend on the order entries are observed in: the loader
    /// walks the RDB stream, the index side walks `id_to_key` in id order.
    #[test]
    fn digest_is_order_independent() {
        let forward = [(1u64, &b"ts:one"[..]), (2, b"ts:two"), (3, b"ts:three")];
        let mut reversed = forward;
        reversed.reverse();

        let summarize = |entries: &[(u64, &[u8])]| {
            entries.iter().fold(LoadStats::default(), |st, (id, key)| {
                st.observe(Some(entry_hash(*id, key)))
            })
        };
        assert_eq!(summarize(&forward), summarize(&reversed));
    }

    /// An unreadable key name must poison the digest rather than silently contributing zero,
    /// so the gate falls back to the sweep instead of comparing an incomplete digest.
    #[test]
    fn missing_key_name_marks_stats_incomplete() {
        let stats = LoadStats::default()
            .observe(Some(entry_hash(1, b"ts:one")))
            .observe(None);
        assert_eq!(stats.count, 2, "the series still counts");
        assert!(stats.incomplete);
    }

    #[test]
    fn id_delta_encoding_handles_epoch_style_ids() {
        // Ids with large shared high bits (epoch) and dense low bits.
        let epoch = 0x0000_ABCD_u64 << 40;
        let mut postings = Postings::default();
        for i in 1..=100u64 {
            let id = epoch | i;
            postings
                .id_to_key
                .insert(id, format!("key:{i}").into_bytes().into_boxed_slice());
            postings.all_postings.add(id);
        }
        let payload = build_payload(&[(0, &postings)]);
        let sections = parse_aux_payload(&payload).unwrap();
        assert_postings_eq(&sections[0].1, &postings);
    }
}
