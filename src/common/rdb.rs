use crate::common::Timestamp;
use crate::common::rounding::RoundingStrategy;
use std::collections::HashMap;
use std::sync::atomic::AtomicU64;
use std::time::Duration;
use valkey_module::{RedisModuleIO, ValkeyError, ValkeyResult, raw};

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

/// Writes a presence marker, then the double when there is one. Presence is never encoded
/// in-band as a NaN payload: every bit pattern, Prometheus's StaleNaN included, is a value.
pub(crate) fn rdb_save_optional_f64(rdb: *mut RedisModuleIO, value: Option<f64>) {
    rdb_save_optional_marker(rdb, value.is_some());
    if let Some(value) = value {
        raw::save_double(rdb, value);
    }
}

/// Reads what [`rdb_save_optional_f64`] wrote; a marker other than present/absent fails the load.
pub(crate) fn rdb_load_optional_f64(rdb: *mut RedisModuleIO) -> ValkeyResult<Option<f64>> {
    if load_optional_marker(rdb)? {
        Ok(Some(raw::load_double(rdb)?))
    } else {
        Ok(None)
    }
}

pub fn rdb_save_duration(rdb: *mut RedisModuleIO, duration: &Duration) {
    let millis = duration.as_millis() as i64;
    raw::save_signed(rdb, millis);
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
    narrow(raw::load_unsigned(rdb)?, "usize")
}

/// Narrows a loaded integer to the type it was saved from, failing the load on a value that
/// type cannot hold. An `as` cast would wrap instead: a chunk type of 258 loaded as Gorilla.
///
/// For `usize` this depends on the build: a value a 64-bit node saved past `u32::MAX` (a
/// chunk budget, a count) fails to load on a 32-bit build rather than wrapping there.
fn narrow<T: TryFrom<V>, V: Copy + std::fmt::Display>(value: V, ty: &str) -> ValkeyResult<T> {
    T::try_from(value)
        .map_err(|_| ValkeyError::String(format!("RDB: {value} out of range for {ty}")))
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
    narrow(raw::load_unsigned(rdb)?, "u8")
}

pub fn rdb_save_i8(rdb: *mut RedisModuleIO, value: i8) {
    raw::save_signed(rdb, value as i64)
}

pub fn rdb_load_i8(rdb: *mut RedisModuleIO) -> ValkeyResult<i8> {
    narrow(raw::load_signed(rdb)?, "i8")
}

#[inline]
pub fn rdb_save_i32(rdb: *mut RedisModuleIO, value: i32) {
    raw::save_signed(rdb, value as i64)
}

#[inline]
pub fn rdb_load_i32(rdb: *mut RedisModuleIO) -> ValkeyResult<i32> {
    narrow(raw::load_signed(rdb)?, "i32")
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

/// Reads what [`rdb_save_bool`] wrote: `0` or `1`, anything else failing the load. Never pair it
/// with [`rdb_save_optional_bool`], which writes `2` for `None`; that one reads back with
/// [`rdb_load_optional_bool`].
pub fn rdb_load_bool(rdb: *mut RedisModuleIO) -> ValkeyResult<bool> {
    match rdb_load_u8(rdb)? {
        0 => Ok(false),
        1 => Ok(true),
        other => Err(ValkeyError::String(format!("Invalid bool: {other}"))),
    }
}

/// Writes `0`, `1`, or `2` for `None`; read it back with [`rdb_load_optional_bool`] only, since
/// [`rdb_load_bool`] refuses `2`.
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

/// An in-memory stand-in for the server's RDB stream, so unit tests can round-trip
/// `rdb_save`/`rdb_load` without a server.
///
/// The `RedisModuleIO` pointer handed to the codecs is a [`MemIo`], and the server's save/load
/// entry points are replaced (once, process-wide) by functions that push to and pop from it.
/// Each item keeps its kind, so a load reading a different kind than was saved there (a
/// desynchronized stream) reports an IO error, as a short or garbled read would on the server.
/// Only the integer and double entry points are installed.
#[cfg(test)]
pub(crate) mod mem_io {
    use std::os::raw::c_int;
    use std::sync::Once;
    use valkey_module::{RedisModuleIO, raw};

    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    pub(crate) enum Item {
        Unsigned(u64),
        Signed(i64),
        /// The double's bits, so NaN payloads compare exactly.
        Double(u64),
    }

    #[derive(Default)]
    pub(crate) struct MemIo {
        pub(crate) items: Vec<Item>,
        pos: usize,
        error: bool,
    }

    impl MemIo {
        pub(crate) fn new() -> Self {
            install();
            Self::default()
        }

        pub(crate) fn io(&mut self) -> *mut RedisModuleIO {
            (self as *mut Self).cast()
        }

        /// Starts reading from the beginning again.
        pub(crate) fn rewind(&mut self) {
            self.pos = 0;
            self.error = false;
        }

        pub(crate) fn is_drained(&self) -> bool {
            self.pos == self.items.len()
        }
    }

    /// # Safety
    /// `io` must come from [`MemIo::io`] on a live `MemIo`.
    unsafe fn mem<'a>(io: *mut RedisModuleIO) -> &'a mut MemIo {
        unsafe { &mut *io.cast::<MemIo>() }
    }

    fn next(io: &mut MemIo) -> Option<Item> {
        let item = io.items.get(io.pos).copied();
        io.pos += 1;
        if item.is_none() {
            io.error = true;
        }
        item
    }

    unsafe extern "C" fn save_unsigned(io: *mut RedisModuleIO, value: u64) {
        unsafe { mem(io) }.items.push(Item::Unsigned(value));
    }

    unsafe extern "C" fn save_signed(io: *mut RedisModuleIO, value: i64) {
        unsafe { mem(io) }.items.push(Item::Signed(value));
    }

    unsafe extern "C" fn save_double(io: *mut RedisModuleIO, value: f64) {
        unsafe { mem(io) }.items.push(Item::Double(value.to_bits()));
    }

    unsafe extern "C" fn load_unsigned(io: *mut RedisModuleIO) -> u64 {
        let io = unsafe { mem(io) };
        match next(io) {
            Some(Item::Unsigned(value)) => value,
            _ => {
                io.error = true;
                0
            }
        }
    }

    unsafe extern "C" fn load_signed(io: *mut RedisModuleIO) -> i64 {
        let io = unsafe { mem(io) };
        match next(io) {
            Some(Item::Signed(value)) => value,
            _ => {
                io.error = true;
                0
            }
        }
    }

    unsafe extern "C" fn load_double(io: *mut RedisModuleIO) -> f64 {
        let io = unsafe { mem(io) };
        match next(io) {
            Some(Item::Double(bits)) => f64::from_bits(bits),
            _ => {
                io.error = true;
                0.0
            }
        }
    }

    unsafe extern "C" fn is_io_error(io: *mut RedisModuleIO) -> c_int {
        c_int::from(unsafe { mem(io) }.error)
    }

    fn install() {
        static INSTALL: Once = Once::new();
        INSTALL.call_once(|| unsafe {
            raw::RedisModule_SaveUnsigned = Some(save_unsigned);
            raw::RedisModule_SaveSigned = Some(save_signed);
            raw::RedisModule_SaveDouble = Some(save_double);
            raw::RedisModule_LoadUnsigned = Some(load_unsigned);
            raw::RedisModule_LoadSigned = Some(load_signed);
            raw::RedisModule_LoadDouble = Some(load_double);
            raw::RedisModule_IsIOError = Some(is_io_error);
        });
    }
}

#[cfg(test)]
mod tests {
    use super::mem_io::{Item, MemIo};
    use super::{narrow, rdb_load_optional_f64, rdb_save_optional_f64};

    /// Prometheus's stale marker, once the in-band encoding of `None`.
    const STALE_NAN_BITS: u64 = 0x7ff0000000000002;

    #[test]
    fn optional_f64_round_trips_every_bit_pattern() {
        let values = [
            None,
            Some(1.5),
            Some(-0.0),
            Some(f64::INFINITY),
            Some(f64::NAN),
            Some(f64::from_bits(STALE_NAN_BITS)),
        ];
        let mut mem = MemIo::new();
        for value in values {
            rdb_save_optional_f64(mem.io(), value);
        }
        mem.rewind();
        for value in values {
            let loaded = rdb_load_optional_f64(mem.io()).expect("load");
            assert_eq!(
                loaded.map(f64::to_bits),
                value.map(f64::to_bits),
                "{value:?}"
            );
        }
        assert!(mem.is_drained());
    }

    #[test]
    fn optional_f64_refuses_an_unknown_marker() {
        let mut mem = MemIo::new();
        mem.items.push(Item::Unsigned(0));
        mem.items.push(Item::Double(1.0f64.to_bits()));
        assert!(rdb_load_optional_f64(mem.io()).is_err());
    }

    #[test]
    fn narrowing_refuses_what_the_type_cannot_hold() {
        assert_eq!(narrow::<u8, u64>(255, "u8").unwrap(), 255);
        // `as u8` read 258 as 2 (a chunk type of Gorilla) and 511 as 255.
        assert!(narrow::<u8, u64>(258, "u8").is_err());
        assert!(narrow::<u8, u64>(511, "u8").is_err());
        assert!(narrow::<i8, i64>(-129, "i8").is_err());
        assert_eq!(narrow::<i32, i64>(-1, "i32").unwrap(), -1);
        assert!(narrow::<i32, i64>(i64::from(i32::MAX) + 1, "i32").is_err());
    }
}
