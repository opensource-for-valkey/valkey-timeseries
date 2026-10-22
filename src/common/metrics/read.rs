//! `blocked_read`: blocking reads (`TS.READ ... BLOCK`). Counted in `common::block_on_keys` for what the
//! server does to a blocked client (blocking, timing out, releasing its state) and in
//! `commands::ts_read` for what the command decides.

use metered::Registry;
use metered::entry::{counter, gauge};
use std::sync::atomic::{AtomicI64, AtomicU64};

/// Clients the server accepted as blocked on a key.
pub static BLOCKED: AtomicU64 = AtomicU64::new(0);
/// `BLOCK` reads that found enough qualifying samples at once and never blocked.
pub static SERVED_WITHOUT_BLOCK: AtomicU64 = AtomicU64::new(0);
/// `BLOCK` reads that had to wait but could not: a deny-blocking context (`MULTI`, a script), or
/// the server declined to block the client.
pub static BLOCK_REFUSED: AtomicU64 = AtomicU64::new(0);
/// Blocked reads released by the timeout callback: a deadline, or `CLIENT UNBLOCK ... TIMEOUT`.
pub static BLOCK_TIMEOUTS: AtomicU64 = AtomicU64::new(0);
/// Times a blocked read was woken, re-checked its key, and stayed blocked.
pub static SPURIOUS_WAKEUPS: AtomicU64 = AtomicU64::new(0);
/// Blocked reads released with an empty reply because their key was deleted.
pub static UNBLOCKED_BY_DELETE: AtomicU64 = AtomicU64::new(0);
/// Blocked reads whose server-held state has not been reclaimed yet. Up on a successful block,
/// down in the free callback: the one place that runs exactly once per block, however it ends.
pub static BLOCKED_CLIENTS: AtomicI64 = AtomicI64::new(0);

static COUNTERS: &[(&str, &AtomicU64, &str)] = &[
    ("blocked", &BLOCKED, "Reads the server blocked on a key"),
    (
        "served_without_block",
        &SERVED_WITHOUT_BLOCK,
        "BLOCK reads served at once because enough qualifying samples already existed",
    ),
    (
        "block_refused",
        &BLOCK_REFUSED,
        "BLOCK reads that had to wait but could not (MULTI, a script, or the server declining)",
    ),
    (
        "block_timeouts",
        &BLOCK_TIMEOUTS,
        "Blocked reads released by the timeout callback, including CLIENT UNBLOCK TIMEOUT",
    ),
    (
        "spurious_wakeups",
        &SPURIOUS_WAKEUPS,
        "Wakeups after which a blocked read re-checked its key and stayed blocked",
    ),
    (
        "unblocked_by_delete",
        &UNBLOCKED_BY_DELETE,
        "Blocked reads released with an empty reply because their key was deleted",
    ),
];

pub(super) fn register(registry: &mut Registry<'static>) {
    for &(name, source, help) in COUNTERS {
        registry.register(counter(name).source(source).help(help));
    }
    registry.register(
        gauge("blocked_clients")
            .source(&BLOCKED_CLIENTS)
            .help("Blocked reads the server still holds state for"),
    );
}
