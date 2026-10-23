//! `rdb`: the module's RDB aux field, which carries the postings index (see
//! `series::index::persistence`). One load or save per RDB that carries the field, not one per
//! series.
//!
//! The load counters are plain atomics: an RDB loads in the server process. The save counters
//! cannot be: `aux_save` runs in the fork child for a `BGSAVE`, a replica's full sync and an AOF
//! rewrite's RDB preamble, and the child's copy of an ordinary static is its own. So they live in
//! an anonymous `MAP_SHARED` mapping, which a fork shares rather than copies, and the server reads
//! what the child wrote. [`init`] maps it at module load, before the server can fork.

use crate::common::logging::log_warning;
use metered::Registry;
use metered::entry::{counter, counter_value};
use std::sync::LazyLock;
use std::sync::atomic::{AtomicU64, Ordering};

/// Aux payloads read and installed as the index.
pub static LOAD_SUCCESS: AtomicU64 = AtomicU64::new(0);
/// Aux payloads that could not be used: unreadable, of a foreign encoding version, or rejected
/// by the parser. The index is rebuilt from the keys instead.
pub static LOAD_FAILURE: AtomicU64 = AtomicU64::new(0);

/// The save outcomes, in memory shared with the fork child. Zeroed memory is a valid value.
#[repr(C)]
pub struct SaveCounters {
    /// Saves that wrote the index aux payload.
    pub success: AtomicU64,
    /// Saves that left the payload out because the index was locked, or whose write failed.
    pub failure: AtomicU64,
}

static SAVE: LazyLock<&'static SaveCounters> = LazyLock::new(map_save_counters);

/// Maps the save counters. If the mapping fails they fall back to process memory, where only a
/// save that runs in the server itself (`SAVE`, `DEBUG RELOAD`, a shutdown save) is counted.
fn map_save_counters() -> &'static SaveCounters {
    let len = size_of::<SaveCounters>();
    // SAFETY: a fresh anonymous mapping, never unmapped, so the reference lives as long as the
    // process. The kernel zero-fills it, and page alignment covers the atomics' alignment.
    let ptr = unsafe {
        libc::mmap(
            std::ptr::null_mut(),
            len,
            libc::PROT_READ | libc::PROT_WRITE,
            libc::MAP_SHARED | libc::MAP_ANON,
            -1,
            0,
        )
    };
    if ptr == libc::MAP_FAILED {
        log_warning(format!(
            "Could not map shared memory for the RDB save counters ({}); \
             saves in a fork child will not be counted",
            std::io::Error::last_os_error()
        ));
        return Box::leak(Box::new(SaveCounters {
            success: AtomicU64::new(0),
            failure: AtomicU64::new(0),
        }));
    }
    unsafe { &*ptr.cast::<SaveCounters>() }
}

/// The save counters. Safe to bump from the fork child.
pub fn save_counters() -> &'static SaveCounters {
    *SAVE
}

/// Maps the save counters now. Called from module init: mapped lazily after a fork, the
/// child's increments would land in a mapping the server never sees.
pub fn init() {
    LazyLock::force(&SAVE);
}

/// Loads and saves are exported as a total and a failure count, as Prometheus recommends; the
/// total is derived, so the counters bumped at each call site stay one per outcome.
pub(super) fn register(registry: &mut Registry<'static>) {
    let save = save_counters();
    registry
        .register(
            counter_value("loads")
                .read(|_: &()| {
                    LOAD_SUCCESS.load(Ordering::Relaxed) + LOAD_FAILURE.load(Ordering::Relaxed)
                })
                .help("RDB loads that carried the postings index aux field, failed ones included"),
        )
        .register(
            counter("load_failures")
                .source(&LOAD_FAILURE)
                .help("RDB loads whose index aux field was unreadable, of a foreign encoding, or rejected (the index is rebuilt from the keys)"),
        )
        .register(
            counter_value("saves")
                .read(move |_: &()| {
                    save.success.load(Ordering::Relaxed) + save.failure.load(Ordering::Relaxed)
                })
                .help("RDB saves with a postings index to write, fork-child saves and failed ones included"),
        )
        .register(
            counter("save_failures")
                .source(&save.failure)
                .help("RDB saves that left the index aux field out because the index was locked, or whose write failed"),
        );
}
