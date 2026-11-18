//! Cumulative declaration of `ValkeyModule_SetModuleOptions` flags.
//!
//! The server API takes the *complete* option bitmask on every call, so two independent
//! call sites each passing their own flag leaves only whichever ran last. Declarations here
//! are OR-ed into a process-wide mask and the full mask is re-applied, so a later declaration
//! keeps the earlier ones. That holds for declarations made one after another, which is what
//! module load is; see [`declare_module_options`].

use std::os::raw::c_int;
use std::sync::atomic::{AtomicI32, Ordering};
use valkey_module::{Context, raw, raw::ModuleOptions};

/// `VALKEYMODULE_OPTIONS_HANDLE_IO_ERRORS`.
///
/// Without this flag the server's `moduleRDBLoadError` reaches `serverPanic` the moment an
/// RDB/AOF/`TS._RESTORE` stream underruns — inside our own `rdb_load`, before any of the
/// module's bounds checks or error handling can run. With it, the failed read is recorded on
/// the `RedisModuleIO` instead; `valkey_module::raw`'s load helpers check `is_io_error` after
/// every read and surface a short-read `Err`, which the loaders propagate.
pub const HANDLE_IO_ERRORS: ModuleOptions = ModuleOptions::HANDLE_IO_ERRORS;

/// `VALKEYMODULE_OPTIONS_HANDLE_ATOMIC_SLOT_MIGRATION`.
///
/// `valkey-module` names this flag only under its `min-valkey-compatibility-version-9-0`
/// feature, which would raise the module's minimum server to 9.0; an 8.x server ignores the
/// bit, so it is built from the header constant instead.
pub const HANDLE_ATOMIC_SLOT_MIGRATION: ModuleOptions = ModuleOptions::from_bits_retain(
    raw::VALKEYMODULE_OPTIONS_HANDLE_ATOMIC_SLOT_MIGRATION as c_int,
);

static DECLARED_OPTIONS: AtomicI32 = AtomicI32::new(0);

/// Adds `flags` to the module's declared options and re-applies the accumulated mask.
///
/// Module load only: the server runs the module's init on its main thread, so declarations
/// never overlap. Accumulating and applying are two steps, not one atomic update; two
/// concurrent callers could apply their masks in either order, and the server would keep
/// whichever landed last.
///
/// `HANDLE_REPL_ASYNC_LOAD` is deliberately never declared. Without it, a replica configured
/// for `repl-diskless-load swapdb` loads a full sync synchronously instead ("Skipping
/// diskless-load because there are modules that are not aware of async replication"), and a
/// synchronous load refuses every client command with `-LOADING`. So no client `RESTORE` can
/// reach the series loader while the server loads its own data, which is what lets
/// `LoadTrust::current` (`series::serialization`) tell an own payload from a foreign one by the
/// load window alone. Declaring it would let a forged `RESTORE` on a writable replica load as
/// the server's own data; the loader would first need to tell the two apart per call.
///
/// # Panics
///
/// On `HANDLE_REPL_ASYNC_LOAD`. Every module load runs this, so any test catches it.
/// A debug build also panics if the module has finished loading.
pub fn declare_module_options(ctx: &Context, flags: ModuleOptions) {
    assert!(
        !flags.contains(ModuleOptions::HANDLE_REPL_ASYNC_LOAD),
        "HANDLE_REPL_ASYNC_LOAD would let client commands reach the series loader during a load"
    );
    debug_assert!(
        !crate::is_module_initialized(),
        "module options are declared during module load only"
    );
    // Relaxed: every declaration runs on the one thread loading the module.
    let previous = DECLARED_OPTIONS.fetch_or(flags.bits(), Ordering::Relaxed);
    let combined = previous | flags.bits();
    ctx.set_module_options(ModuleOptions::from_bits_retain(combined));
    ctx.log_notice(&format!("Declared module options: 0x{combined:x}"));
}
