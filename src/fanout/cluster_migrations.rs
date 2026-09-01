use crate::common::module_options::{HANDLE_ATOMIC_SLOT_MIGRATION, declare_module_options};
use crate::common::sync::{read_lock, write_lock};
use crate::fanout::cluster_map::NUM_SLOTS;
use crate::fanout::is_clustered;
use crate::fanout::mark_cluster_map_stale;
use range_set_blaze::RangeSetBlaze;
use std::ffi::{c_char, c_int, c_void};
use std::sync::{LazyLock, RwLock};
use valkey_module::{
    CallOptionResp, CallOptionsBuilder, CallReply, CallResult, Context, Version, raw,
};

const ASM_MINIMUM_VERSION: Version = Version {
    major: 9,
    minor: 0,
    patch: 0,
};

/// Representation of a single slot migration entry returned by
/// `CLUSTER GETSLOTMIGRATIONS`.
#[derive(Debug, Clone, Default)]
pub struct SlotMigration {
    pub name: String,
    pub operation: String,
    pub slot_ranges: RangeSetBlaze<u16>,
    pub target_node: Option<String>,
    pub source_node: Option<String>,
    pub create_time: i64,
    pub last_update_time: i64,
    pub last_ack_time: i64,
    pub state: String,
    pub message: String,
    pub cow_size: i64,
    pub remaining_repl_size: i64,
}

pub fn supports_atomic_slot_migration(ctx: &Context) -> bool {
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

/// Call CLUSTER GETSLOTMIGRATIONS and parse the result into a Vec<SlotMigration>.
pub fn get_slot_migrations(ctx: &Context) -> Result<Vec<SlotMigration>, String> {
    let call_options = CallOptionsBuilder::new()
        .resp(CallOptionResp::Resp3)
        .errors_as_replies()
        .build();

    let res: CallResult =
        ctx.call_ext::<_, CallResult>("CLUSTER", &call_options, &["GETSLOTMIGRATIONS"]);

    let top = match res {
        Err(e) => return Err(format!("Error calling CLUSTER GETSLOTMIGRATIONS: {e}")),
        Ok(CallReply::Array(arr)) => {
            debug_assert!(
                arr.len() % 2 == 0,
                "Expected even number of entries in CLUSTER GETSLOTMIGRATIONS result"
            );
            arr
        }
        _ => return Err("CLUSTER GETSLOTMIGRATIONS did not return an array".to_string()),
    };

    let mut out = Vec::with_capacity(top.len());

    fn apply_field(ctx: &Context, mig: &mut SlotMigration, key: &str, value: CallResult) {
        match key {
            "name" => mig.name = as_str(value).unwrap_or_default(),
            "operation" => mig.operation = as_str(value).unwrap_or_default(),
            "slot_ranges" => {
                let s = as_str(value).unwrap_or_default();
                match parse_slot_ranges(&s) {
                    Ok(ranges) => mig.slot_ranges = ranges,
                    Err(e) => ctx.log_warning(&format!("Error parsing slot ranges '{s}': {e}")),
                }
            }
            "target_node" => {
                let s = as_str(value).unwrap_or_default();
                if !s.is_empty() {
                    mig.target_node = Some(s);
                }
            }
            "source_node" => {
                let s = as_str(value).unwrap_or_default();
                if !s.is_empty() {
                    mig.source_node = Some(s);
                }
            }
            "create_time" => mig.create_time = as_i64(value).unwrap_or(0),
            "last_update_time" => mig.last_update_time = as_i64(value).unwrap_or(0),
            "last_ack_time" => mig.last_ack_time = as_i64(value).unwrap_or(0),
            "state" => mig.state = as_str(value).unwrap_or_default(),
            "message" => mig.message = as_str(value).unwrap_or_default(),
            "cow_size" => mig.cow_size = as_i64(value).unwrap_or(0),
            "remaining_repl_size" => mig.remaining_repl_size = as_i64(value).unwrap_or(0),
            _ => {}
        }
    }

    for job_entry in top.iter().flatten() {
        let mut mig = SlotMigration::default();
        match job_entry {
            CallReply::Array(arr) => {
                let mut i = 0usize;
                while i + 1 < arr.len() {
                    let key = arr.get(i).and_then(as_str).unwrap_or_default();
                    if !key.is_empty()
                        && let Some(value) = arr.get(i + 1)
                    {
                        apply_field(ctx, &mut mig, &key.to_ascii_lowercase(), value);
                    }
                    i += 2;
                }
            }
            CallReply::Map(map) => {
                for (key, value) in map.iter() {
                    let Some(key) = as_str(key) else {
                        continue;
                    };
                    apply_field(ctx, &mut mig, &key.to_ascii_lowercase(), value);
                }
            }
            other => {
                ctx.log_warning(&format!("Unexpected slot migration entry type: {other:?}"));
                continue;
            }
        }

        out.push(mig);
    }

    Ok(out)
}

fn as_str(r: CallResult) -> Option<String> {
    match r {
        Ok(CallReply::String(v)) => v.to_string(),
        _ => None,
    }
}

fn as_i64(r: CallResult) -> Option<i64> {
    match r {
        Ok(CallReply::I64(v)) => Some(v.to_i64()),
        _ => None,
    }
}

/// Parse a slot ranges string into a vector of inclusive ranges.
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
const VALKEYMODULE_SUBEVENT_ATOMIC_SLOT_MIGRATION_EXPORT_STARTED: u64 = 1;
const VALKEYMODULE_SUBEVENT_ATOMIC_SLOT_MIGRATION_IMPORT_ABORTED: u64 = 2;
const VALKEYMODULE_SUBEVENT_ATOMIC_SLOT_MIGRATION_EXPORT_ABORTED: u64 = 3;
const VALKEYMODULE_SUBEVENT_ATOMIC_SLOT_MIGRATION_IMPORT_COMPLETED: u64 = 4;
const VALKEYMODULE_SUBEVENT_ATOMIC_SLOT_MIGRATION_EXPORT_COMPLETED: u64 = 5;

const VALKEYMODULE_NODE_ID_LEN: usize = 40;

#[derive(Debug)]
pub enum AtomicSlotMigrationEvent {
    ImportStarted,
    ExportStarted,
    ImportAborted,
    ExportAborted,
    ImportCompleted,
    ExportCompleted,
}

pub type AtomicSlotMigrationEventHandler =
    fn(event: AtomicSlotMigrationEvent, slots: RangeSetBlaze<u16>);
pub type PostMigrationCleanupFn = fn(slots: RangeSetBlaze<u16>);

static EVENT_HANDLER_FN: LazyLock<RwLock<Option<AtomicSlotMigrationEventHandler>>> =
    LazyLock::new(|| RwLock::new(None));

#[repr(C)]
#[derive(Debug, Copy, Clone)]
pub struct ValkeyModuleSlotRange {
    pub start: c_int, // Start slot, inclusive.
    pub end: c_int,   // End slot, inclusive.
}

#[repr(C)]
#[derive(Debug, Copy, Clone)]
pub struct ValkeyModuleAtomicSlotMigrationInfoV1 {
    pub version: u64, // Version of this structure for ABI compat.
    pub job_name: [c_char; VALKEYMODULE_NODE_ID_LEN + 1], // Unique ID for the migration operation.
    pub slot_ranges: *mut ValkeyModuleSlotRange, // Array of slot ranges involved in the migration.
    pub num_slot_ranges: u32, // Number of slot ranges in the array.
}

impl ValkeyModuleAtomicSlotMigrationInfoV1 {
    pub fn job_name_str(&self) -> String {
        let c_str = unsafe { std::ffi::CStr::from_ptr(self.job_name.as_ptr()) };
        c_str.to_string_lossy().into_owned()
    }

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

    fn remove_slots_from(&self, dest: &mut RangeSetBlaze<u16>) {
        for i in 0..self.num_slot_ranges {
            unsafe {
                let range = *self.slot_ranges.add(i as usize);
                let start = range.start as u16;
                let end = range.end as u16;
                dest.retain(|x| !(start..=end).contains(x));
            }
        }
    }
}

pub type ValkeyModuleAtomicSlotMigrationInfo = ValkeyModuleAtomicSlotMigrationInfoV1;

unsafe extern "C" fn on_atomic_slot_migration_event(
    _ctx: *mut raw::RedisModuleCtx,
    _eid: raw::RedisModuleEvent,
    sub_event: u64,
    data: *mut c_void,
) {
    fn raise_event(event: AtomicSlotMigrationEvent, data: *mut c_void) {
        if let Some(handler) = *read_lock(&EVENT_HANDLER_FN) {
            let info = unsafe { &*(data as *const ValkeyModuleAtomicSlotMigrationInfo) };
            let slots = info.convert_slot_ranges();
            handler(event, slots);
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

pub fn register_atomic_slot_migration_event_handler(
    ctx: &Context,
    on_event: Option<AtomicSlotMigrationEventHandler>,
) {
    {
        let mut guard = write_lock(&EVENT_HANDLER_FN);
        *guard = on_event;
    }
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

#[cfg(test)]
mod tests {
    use super::*;

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
