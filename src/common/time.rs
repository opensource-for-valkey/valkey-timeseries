use crate::common::Timestamp;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use valkey_module::{RedisModule_CachedMicroseconds, RedisModule_Milliseconds};

/// Returns the time duration since UNIX_EPOCH in milliseconds.
pub fn system_time_millis() -> i64 {
    let now = std::time::SystemTime::now();
    now.duration_since(std::time::UNIX_EPOCH)
        .expect("time went backwards")
        .as_millis() as i64
}

pub fn valkey_current_time_millis() -> i64 {
    unsafe {
        if let Some(func) = RedisModule_Milliseconds {
            func()
        } else {
            // Fallback to system time if the Valkey function pointer is not available.
            system_time_millis()
        }
    }
}

pub fn valkey_cached_time_micros() -> i64 {
    unsafe {
        if let Some(func) = RedisModule_CachedMicroseconds {
            func()
        } else {
            // Fallback: use system time in milliseconds and convert to microseconds.
            system_time_millis() * 1000
        }
    }
}

pub fn valkey_cached_time_millis() -> i64 {
    valkey_cached_time_micros() / 1000
}

pub fn current_time_millis() -> i64 {
    cfg_if::cfg_if! {
        if #[cfg(test)] {
            system_time_millis()
        } else {
            valkey_current_time_millis()
        }
    }
}

pub fn system_time_to_millis(t: SystemTime) -> i64 {
    t.duration_since(SystemTime::UNIX_EPOCH)
        .unwrap_or(Duration::ZERO)
        .as_millis() as i64
}

pub fn system_time_to_secs(t: SystemTime) -> i64 {
    t.duration_since(SystemTime::UNIX_EPOCH)
        .unwrap_or(Duration::ZERO)
        .as_secs() as i64
}

pub fn timestamp_so_system_time(timestamp: Timestamp) -> SystemTime {
    UNIX_EPOCH + Duration::from_millis(timestamp as u64)
}
