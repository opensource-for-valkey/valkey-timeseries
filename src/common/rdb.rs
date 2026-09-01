use crate::common::Timestamp;
use crate::common::rounding::RoundingStrategy;
use std::collections::HashMap;
use std::sync::atomic::AtomicU64;
use std::time::Duration;
use valkey_module::{RedisModuleIO, ValkeyError, ValkeyResult, raw};

// STALE_NAN is a sentinel NaN used to encode `None` for `Option<f64>` in the RDB format.
// The bit pattern 0x7ff0000000000002 is:
//   - An IEEE-754 double with exponent = 0x7ff (all 1s) and a non-zero mantissa,
//     which makes it a NaN rather than ±∞ or a normal finite value.
//   - Deliberately not the canonical quiet-NaN used by `f64::NAN` (often 0x7ff8000000000000),
//     and instead a rarely used NaN payload that is unlikely to be produced by normal
//     floating-point operations.
// We construct and compare it via `from_bits` / `to_bits` so that we can reliably distinguish
// this exact sentinel from any "real" NaN that may appear in user data.
const STALE_NAN: f64 = f64::from_bits(0x7ff0000000000002);

const OPTIONAL_MARKER_PRESENT: u64 = 0xfe;
const OPTIONAL_MARKER_ABSENT: u64 = 0xff;

pub trait RdbSerializable {
    fn rdb_save(&self, rdb: *mut RedisModuleIO);
    fn rdb_load(rdb: *mut RedisModuleIO) -> ValkeyResult<Self>
    where
        Self: Sized;
}

pub(crate) fn load_optional_marker(rdb: *mut RedisModuleIO) -> ValkeyResult<bool> {
    let marker = raw::load_unsigned(rdb)?;
    match marker {
        OPTIONAL_MARKER_PRESENT => Ok(true),
        OPTIONAL_MARKER_ABSENT => Ok(false),
        _ => Err(ValkeyError::String(format!("Invalid marker: {marker}"))),
    }
}

pub(crate) fn rdb_save_optional_marker(rdb: *mut RedisModuleIO, is_some: bool) {
    if is_some {
        raw::save_unsigned(rdb, OPTIONAL_MARKER_PRESENT);
    } else {
        raw::save_unsigned(rdb, OPTIONAL_MARKER_ABSENT);
    }
}

/// WARNING!: internal and *ONLY* for ints < 64 bits! We used a signed integer with value.abs() if a value
/// is present, and -1 otherwise
pub(crate) fn save_optional_unsigned(rdb: *mut RedisModuleIO, value: Option<u64>) {
    if let Some(value) = value {
        raw::save_signed(rdb, value as i64);
    } else {
        raw::save_signed(rdb, -1);
    }
}

#[inline]
pub(crate) fn rdb_save_f64(rdb: *mut RedisModuleIO, value: f64) {
    raw::save_double(rdb, value);
}

#[inline]
pub(crate) fn rdb_load_f64(rdb: *mut RedisModuleIO) -> ValkeyResult<f64> {
    let value = raw::load_double(rdb)?;
    Ok(value)
}

pub(crate) fn rdb_save_optional_f64(rdb: *mut RedisModuleIO, value: Option<f64>) {
    let val = value.unwrap_or(STALE_NAN);
    raw::save_double(rdb, val);
}

pub(crate) fn rdb_load_optional_f64(rdb: *mut RedisModuleIO) -> ValkeyResult<Option<f64>> {
    let val = raw::load_double(rdb)?;
    if val.to_bits() == STALE_NAN.to_bits() {
        return Ok(None);
    }
    Ok(Some(val))
}

pub fn rdb_save_duration(rdb: *mut RedisModuleIO, duration: &Duration) {
    let millis = duration.as_millis() as i64;
    raw::save_signed(rdb, millis);
}

/// NOTE: represents optional duration as i64, meaning that durations above i64::MAX milliseconds
/// will be truncated
pub fn rdb_load_optional_duration(rdb: *mut RedisModuleIO) -> ValkeyResult<Option<Duration>> {
    let val = raw::load_signed(rdb)?;
    if val == -1 {
        return Ok(None);
    }
    Ok(Some(Duration::from_millis(val as u64)))
}

pub fn rdb_save_optional_duration(rdb: *mut RedisModuleIO, duration: &Option<Duration>) {
    if let Some(duration) = duration {
        rdb_save_optional_marker(rdb, true);
        raw::save_signed(rdb, duration.as_millis() as i64);
    } else {
        raw::save_signed(rdb, -1);
    }
}

pub fn rdb_load_duration(rdb: *mut RedisModuleIO) -> ValkeyResult<Duration> {
    let millis = raw::load_unsigned(rdb)?;
    Ok(Duration::from_millis(millis))
}

#[inline]
pub fn rdb_save_usize(rdb: *mut RedisModuleIO, value: usize) {
    raw::save_unsigned(rdb, value as u64)
}

pub fn rdb_load_usize(rdb: *mut RedisModuleIO) -> ValkeyResult<usize> {
    let value = raw::load_unsigned(rdb)?;
    Ok(value as usize)
}

/// Default ceiling for [`rdb_load_len`], used wherever no tighter domain-specific bound applies.
/// Generous enough that no legitimate payload ever approaches it, but small enough that a
/// corrupt/hostile length can't drive `Vec::with_capacity`/`HashMap::with_capacity` anywhere near
/// an allocator abort.
pub const MAX_RDB_COLLECTION_LEN: usize = 16 * 1024 * 1024;

/// Reads a length prefix for a collection that the caller is about to pre-allocate (e.g. via
/// `Vec::with_capacity`/`HashMap::with_capacity`) and rejects it if it exceeds `max`.
///
/// RDB/AOF/`TS._RESTORE` payloads are untrusted: a corrupt or adversarial length field is
/// otherwise trusted as-is and handed straight to `with_capacity`, where an oversized value
/// aborts the process on allocation failure instead of failing the load with a clean error.
/// An allocation abort is not a panic, so it cannot be caught or reported after the fact --
/// the length has to be rejected before it reaches the allocator.
pub fn rdb_load_len(rdb: *mut RedisModuleIO, max: usize) -> ValkeyResult<usize> {
    let len = rdb_load_usize(rdb)?;
    if len > max {
        return Err(ValkeyError::String(format!(
            "RDB: corrupt collection length {len} exceeds maximum {max}"
        )));
    }
    Ok(len)
}

pub fn rdb_save_optional_usize(rdb: *mut RedisModuleIO, value: Option<usize>) {
    save_optional_unsigned(rdb, value.map(|x| x as u64));
}

#[inline]
pub(crate) fn rdb_save_timestamp(rdb: *mut RedisModuleIO, value: Timestamp) {
    raw::save_signed(rdb, value)
}

pub(crate) fn rdb_load_timestamp(rdb: *mut RedisModuleIO) -> ValkeyResult<Timestamp> {
    let value = raw::load_signed(rdb)?;
    Ok(value as Timestamp)
}

pub fn rdb_save_u8(rdb: *mut RedisModuleIO, value: u8) {
    raw::save_unsigned(rdb, value as u64)
}

pub fn rdb_load_u8(rdb: *mut RedisModuleIO) -> ValkeyResult<u8> {
    let value = raw::load_unsigned(rdb)?;
    Ok(value as u8)
}

pub fn rdb_save_i8(rdb: *mut RedisModuleIO, value: i8) {
    raw::save_signed(rdb, value as i64)
}

pub fn rdb_load_i8(rdb: *mut RedisModuleIO) -> ValkeyResult<i8> {
    let value = raw::load_signed(rdb)?;
    Ok(value as i8)
}

#[inline]
pub fn rdb_save_i32(rdb: *mut RedisModuleIO, value: i32) {
    raw::save_signed(rdb, value as i64)
}

#[inline]
pub fn rdb_load_i32(rdb: *mut RedisModuleIO) -> ValkeyResult<i32> {
    let value = raw::load_signed(rdb)?;
    Ok(value as i32)
}

#[inline]
pub fn rdb_save_string(rdb: *mut RedisModuleIO, value: &str) {
    raw::save_string(rdb, value);
}

#[inline]
pub fn rdb_load_string(rdb: *mut RedisModuleIO) -> ValkeyResult<String> {
    Ok(String::from(raw::load_string(rdb)?))
}

pub fn rdb_save_bool(rdb: *mut RedisModuleIO, val: bool) {
    let bool_val = if val { 1 } else { 0 };
    rdb_save_u8(rdb, bool_val)
}

pub fn rdb_load_bool(rdb: *mut RedisModuleIO) -> ValkeyResult<bool> {
    let bool_val = rdb_load_u8(rdb)?;
    Ok(bool_val != 0)
}

pub fn rdb_save_optional_bool(rdb: *mut RedisModuleIO, value: Option<bool>) {
    if let Some(value) = value {
        rdb_save_bool(rdb, value);
    } else {
        rdb_save_u8(rdb, 2)
    }
}

pub fn rdb_load_optional_bool(rdb: *mut RedisModuleIO) -> ValkeyResult<Option<bool>> {
    let marker = rdb_load_u8(rdb)?;
    match marker {
        0 => Ok(Some(false)),
        1 => Ok(Some(true)),
        2 => Ok(None),
        _ => Err(ValkeyError::String(format!(
            "Invalid bool marker: {marker}"
        ))),
    }
}

pub fn rdb_save_string_hashmap(rdb: *mut RedisModuleIO, map: &HashMap<String, String>) {
    rdb_save_usize(rdb, map.len());
    for (key, val) in map.iter() {
        rdb_save_string(rdb, key);
        rdb_save_string(rdb, val);
    }
}

pub fn rdb_load_string_hashmap(rdb: *mut RedisModuleIO) -> ValkeyResult<HashMap<String, String>> {
    let len = rdb_load_len(rdb, MAX_RDB_COLLECTION_LEN)?;
    let mut map = HashMap::with_capacity(len);
    for _ in 0..len {
        let key = rdb_load_string(rdb)?;
        let val = rdb_load_string(rdb)?;
        map.insert(key, val);
    }
    Ok(map)
}

pub fn save_atomic_u64(rdb: *mut RedisModuleIO, value: &AtomicU64) {
    raw::save_unsigned(rdb, value.load(std::sync::atomic::Ordering::Relaxed))
}

pub fn load_atomic_u64(rdb: *mut RedisModuleIO) -> ValkeyResult<AtomicU64> {
    let value = raw::load_unsigned(rdb)?;
    Ok(AtomicU64::new(value))
}

pub(crate) fn rdb_save_rounding(rdb: *mut RedisModuleIO, rounding: &RoundingStrategy) {
    match rounding {
        RoundingStrategy::SignificantDigits(sig_figs) => {
            rdb_save_u8(rdb, 1);
            rdb_save_u8(rdb, *sig_figs)
        }
        RoundingStrategy::DecimalDigits(digits) => {
            rdb_save_u8(rdb, 2);
            rdb_save_u8(rdb, *digits)
        }
    }
}

pub(crate) fn rdb_load_rounding(rdb: *mut RedisModuleIO) -> ValkeyResult<RoundingStrategy> {
    let marker = rdb_load_u8(rdb)?;
    match marker {
        1 => {
            let sig_figs = rdb_load_u8(rdb)?;
            Ok(RoundingStrategy::SignificantDigits(sig_figs))
        }
        2 => {
            let digits = rdb_load_u8(rdb)?;
            Ok(RoundingStrategy::DecimalDigits(digits))
        }
        _ => Err(ValkeyError::String(format!(
            "Invalid rounding marker: {marker}"
        ))),
    }
}

pub(crate) fn rdb_save_optional_rounding(
    rdb: *mut RedisModuleIO,
    rounding: &Option<RoundingStrategy>,
) {
    if let Some(rounding) = rounding {
        rdb_save_optional_marker(rdb, true);
        rdb_save_rounding(rdb, rounding)
    } else {
        rdb_save_optional_marker(rdb, false);
    }
}

pub(crate) fn rdb_load_optional_rounding(
    rdb: *mut RedisModuleIO,
) -> ValkeyResult<Option<RoundingStrategy>> {
    if load_optional_marker(rdb)? {
        let rounding = rdb_load_rounding(rdb)?;
        Ok(Some(rounding))
    } else {
        Ok(None)
    }
}
