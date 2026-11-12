//! `chunk`: stored chunks that could not be read back.
//!
//! A compressed chunk is decoded on every read, and a decode error ends the read early with the
//! samples decoded so far: the reply is still OK, so without this counter a corrupt chunk (bit
//! rot in memory, a codec bug) truncated results without a trace. Loading rejects a chunk whose
//! stream disagrees with its header, so on a healthy node this stays at zero.

use super::WarningLimit;
use crate::common::logging::log_warning;
use metered::Registry;
use metered::entry::counter;
use std::sync::atomic::{AtomicU64, Ordering};

/// Reads that stopped at an undecodable sample.
pub static DECODE_ERRORS: AtomicU64 = AtomicU64::new(0);

/// Rate limit for [`note_decode_error`]'s warning.
static WARNING: WarningLimit = WarningLimit::new();

/// Counts a read that stopped at an undecodable sample, and logs at most one warning per
/// [`WarningLimit`] interval: a corrupt chunk on a hot key would otherwise log on every read.
#[cold]
#[inline(never)]
pub fn note_decode_error(err: &dyn std::fmt::Debug) {
    let total = DECODE_ERRORS.fetch_add(1, Ordering::Relaxed) + 1;
    if WARNING.due() {
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
