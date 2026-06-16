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

/// Compute the median step between consecutive timestamps (in milliseconds).
///
/// Returns `None` if there are fewer than 2 timestamps or no positive intervals exist.
///
/// # Examples
///
/// ```
/// # use valkey_timeseries::common::time::compute_median_step_ms;
/// let ts = vec![1000, 2000, 3000, 5000, 8000];
/// assert_eq!(compute_median_step_ms(&ts), Some(2000)); // steps: 1000, 1000, 2000, 3000 → upper median is 2000
/// ```
pub fn compute_median_step_ms(timestamps: &[i64]) -> Option<i64> {
    if timestamps.len() < 2 {
        return None;
    }
    let mut diffs: Vec<i64> = timestamps
        .windows(2)
        .map(|w| w[1] - w[0])
        .filter(|&d| d > 0)
        .collect();
    if diffs.is_empty() {
        return None;
    }
    diffs.sort_unstable();
    Some(diffs[diffs.len() / 2])
}
