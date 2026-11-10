//! `chunk`: stored chunks that could not be read back.
//!
//! A compressed chunk is decoded on every read, and a decode error ends the read early with the
//! samples decoded so far: the reply is still OK, so without this counter a corrupt chunk (bit
//! rot in memory, a codec bug) truncated results without a trace. Loading rejects a chunk whose
//! stream disagrees with its header, so on a healthy node this stays at zero.

use crate::common::logging::log_warning;
use metered::Registry;
use metered::entry::counter;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

/// Reads that stopped at an undecodable sample.
pub static DECODE_ERRORS: AtomicU64 = AtomicU64::new(0);

/// Unix second of the last decode-error warning, for [`note_decode_error`]'s rate limit.
static LAST_WARNING_SECS: AtomicU64 = AtomicU64::new(0);

/// Seconds between two decode-error warnings; the counter has the rest.
const WARNING_INTERVAL_SECS: u64 = 60;

/// Counts a read that stopped at an undecodable sample, and logs at most one warning per
/// [`WARNING_INTERVAL_SECS`]: a corrupt chunk on a hot key would otherwise log on every read.
#[cold]
#[inline(never)]
pub fn note_decode_error(err: &dyn std::fmt::Debug) {
    let total = DECODE_ERRORS.fetch_add(1, Ordering::Relaxed) + 1;
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_secs());
    let last = LAST_WARNING_SECS.load(Ordering::Relaxed);
    if (last == 0 || now >= last + WARNING_INTERVAL_SECS)
        && LAST_WARNING_SECS
            .compare_exchange(last, now.max(1), Ordering::Relaxed, Ordering::Relaxed)
            .is_ok()
    {
        log_warning(format!(
            "TSDB: a chunk read stopped at an undecodable sample ({err:?}); the reply was \
             truncated. {total} such reads since start (chunk_decode_errors_total)"
        ));
    }
}

pub(super) fn register(registry: &mut Registry<'static>) {
    registry.register(counter("decode_errors").source(&DECODE_ERRORS).help(
        "Reads that stopped at an undecodable sample in a stored chunk; the reply was truncated",
    ));
}
