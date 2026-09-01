//! Cumulative declaration of `ValkeyModule_SetModuleOptions` flags.
//!
//! The server API takes the *complete* option bitmask on every call, so two independent
//! call sites each passing their own flag leaves only whichever ran last. Declarations here
//! are OR-ed into a process-wide mask and the full mask is re-applied, so the order in which
//! subsystems declare their options does not matter.

use std::os::raw::c_int;
use std::sync::atomic::{AtomicI32, Ordering};
use valkey_module::{Context, raw};

/// `VALKEYMODULE_OPTIONS_HANDLE_IO_ERRORS`.
///
/// Without this flag the server's `moduleRDBLoadError` reaches `serverPanic` the moment an
/// RDB/AOF/`TS._RESTORE` stream underruns — inside our own `rdb_load`, before any of the
/// module's bounds checks or error handling can run. With it, the failed read is recorded on
/// the `RedisModuleIO` instead; `valkey_module::raw`'s load helpers check `is_io_error` after
/// every read and surface a short-read `Err`, which the loaders propagate.
pub const HANDLE_IO_ERRORS: c_int = 1 << 0;

/// `VALKEYMODULE_OPTIONS_HANDLE_ATOMIC_SLOT_MIGRATION`.
pub const HANDLE_ATOMIC_SLOT_MIGRATION: c_int = 1 << 5;

static DECLARED_OPTIONS: AtomicI32 = AtomicI32::new(0);

/// Adds `flags` to the module's declared options and re-applies the accumulated mask.
pub fn declare_module_options(ctx: &Context, flags: c_int) {
    let previous = DECLARED_OPTIONS.fetch_or(flags, Ordering::SeqCst);
    let combined = previous | flags;

    // SAFETY: `ctx` is a live module context; the API pointer is checked before use because
    // older servers may not export it.
    unsafe {
        let Some(set_opts) = raw::ValkeyModule_SetModuleOptions else {
            ctx.log_notice("ValkeyModule_SetModuleOptions not available in raw bindings");
            return;
        };
        set_opts(ctx.ctx as *mut raw::ValkeyModuleCtx, combined);
    }
    ctx.log_notice(&format!("Declared module options: 0x{combined:x}"));
}
