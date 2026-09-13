#![deny(unsafe_op_in_unsafe_fn)]
#![allow(dead_code)]
extern crate enum_dispatch;
extern crate get_size2;
#[cfg(test)]
extern crate serial_test;
extern crate strum;
extern crate strum_macros;
extern crate valkey_module_macros;

use crate::commands::register_fanout_operations;
use crate::common::module_options::{HANDLE_IO_ERRORS, declare_module_options};
use crate::common::threads::init_thread_pool;
use crate::config::register_config;
use crate::fanout::{init_fanout, is_clustered};
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread::ThreadId;
use valkey_module::{Context, Status, ValkeyString, Version, valkey_module};
use valkey_module_macros::shutdown_event_handler;

pub mod aggregators;
mod analysis;
mod commands;
pub mod common;
pub mod config;
mod error;
pub mod error_consts;
mod fanout;
pub mod iterators;
mod join;
mod labels;
mod parser;
pub mod series;

pub use labels::Label;

/// Data generators and chunk helpers shared by unit tests, benchmarks and the
/// `compression_report` tool. Not part of the module's runtime surface.
#[cfg(any(test, feature = "test-utils"))]
pub mod tests;

use crate::common::block_on_keys::check_blocking_module_apis;
use crate::series::background_tasks::init_background_tasks;
use crate::series::index::init_croaring_allocator;
use crate::series::index::persistence::check_required_module_apis;
use crate::series::index::server_events::{
    generic_key_events_handler, register_server_event_handlers,
};
use crate::series::series_data_type::VK_TIME_SERIES_TYPE;

pub const VK_TIMESERIES_VERSION: i32 = 1;
pub const MODULE_NAME: &str = "ts";

static IS_MODULE_INITIALIZED: AtomicBool = AtomicBool::new(false);
static IS_SHUTTING_DOWN: AtomicBool = AtomicBool::new(false);

static MAIN_THREAD_ID: OnceLock<ThreadId> = OnceLock::new();

pub fn is_module_initialized() -> bool {
    IS_MODULE_INITIALIZED.load(Ordering::Relaxed)
}

pub fn is_shutting_down() -> bool {
    IS_SHUTTING_DOWN.load(Ordering::Relaxed)
}

pub fn is_main_thread() -> bool {
    MAIN_THREAD_ID
        .get()
        .is_some_and(|id| *id == std::thread::current().id())
}

pub fn valid_server_version(version: Version) -> bool {
    let server_version = &[
        version.major.into(),
        version.minor.into(),
        version.patch.into(),
    ];
    server_version >= config::TIMESERIES_MIN_SUPPORTED_VERSION
}

fn preload(ctx: &Context, args: &[ValkeyString]) -> Status {
    // perform preload validations here, useful for MODULE LOAD
    // unlike init which is called at the end of the valkey_module! macro this is called at the beginning
    let version = ctx.get_server_version().unwrap();
    ctx.log_notice(&format!(
        "preload for server version {version:?} with args: {args:?}"
    ));

    let ver = ctx
        .get_server_version()
        .expect("Unable to get server version!");

    if !valid_server_version(ver) {
        ctx.log_warning(
            format!(
                "The minimum supported Valkey server version for the valkey-timeseries module is {:?}",
                config::TIMESERIES_MIN_SUPPORTED_VERSION
            )
                .as_str(),
        );
        return Status::Err;
    }

    if let Err(symbol) = check_required_module_apis() {
        ctx.log_warning(&format!(
            "Required module API {symbol} is unavailable on this server; refusing to load"
        ));
        return Status::Err;
    }

    // TS.READ's block-on-keys path dereferences these directly; fail the load rather than the
    // first blocking read.
    if let Err(symbol) = check_blocking_module_apis() {
        ctx.log_warning(&format!(
            "Required module API {symbol} is unavailable on this server; refusing to load"
        ));
        return Status::Err;
    }

    Status::Ok
}

/// Apply the ACL categories each `#[command]`-annotated handler declares with
/// `acl_categories!` (see [`commands::COMMAND_ACL_CATEGORIES`]). The command-info
/// registration path sets none, and the server derives none from the command flags, so this
/// is the only place the built-in `read`/`write`/`fast` categories and the module's own
/// `@timeseries` category get attached to those commands.
///
/// Any failure aborts the load rather than logging: a command left without categories is
/// invisible to `@timeseries` ACL rules and, if it writes, runnable by a `-@write` user. The
/// server rejects both an unknown command name and an unknown category, so a typo in either
/// half of a declaration surfaces here.
#[cfg(feature = "min-valkey-compatibility-version-8-0")]
fn assign_command_acl_categories(ctx: &Context) -> Result<(), String> {
    use std::ffi::CString;
    for (name, categories) in commands::COMMAND_ACL_CATEGORIES {
        let command = CString::new(*name).map_err(|e| format!("command name {name:?}: {e}"))?;
        let acl = CString::new(*categories)
            .map_err(|e| format!("categories {categories:?} for {name}: {e}"))?;
        if ctx.set_acl_category(command.as_ptr(), acl.as_ptr()) == Status::Err {
            return Err(format!(
                "could not set ACL categories {categories:?} on command {name:?} \
                 (unknown command or category?)"
            ));
        }
    }
    Ok(())
}

#[cfg(not(feature = "min-valkey-compatibility-version-8-0"))]
fn assign_command_acl_categories(_ctx: &Context) -> Result<(), String> {
    Ok(())
}

fn initialize(ctx: &Context, args: &[ValkeyString]) -> Status {
    init_croaring_allocator();

    // Declare this first: until the server knows the module handles IO errors, any short
    // read while loading a TSDB-TYPE payload panics the server from inside `rdb_load`
    // rather than returning an error we can report.
    declare_module_options(ctx, HANDLE_IO_ERRORS);

    if let Err(e) = register_config(ctx, args) {
        let msg = format!("Failed to register config: {e}");
        ctx.log_warning(&msg);
        return Status::Err;
    }

    if let Err(e) = assign_command_acl_categories(ctx) {
        ctx.log_warning(&format!("Failed to assign command ACL categories: {e}"));
        return Status::Err;
    }

    if let Err(e) = register_server_event_handlers(ctx) {
        let msg = format!("Failed to register server event handlers: {e}");
        ctx.log_warning(&msg);
        return Status::Err;
    }

    if is_clustered(ctx) {
        init_fanout(ctx);
        if let Err(e) = register_fanout_operations() {
            let msg = format!("Failed to register fanout operations: {e}");
            ctx.log_warning(&msg);
            return Status::Err;
        };
    }

    MAIN_THREAD_ID.get_or_init(|| std::thread::current().id());

    init_thread_pool();
    init_background_tasks(ctx);

    ctx.log_notice("valkey-timeseries module initialized");
    IS_MODULE_INITIALIZED.store(true, Ordering::Relaxed);
    Status::Ok
}

fn deinitialize(ctx: &Context) -> Status {
    ctx.log_notice("deinitialize");
    IS_MODULE_INITIALIZED.store(false, Ordering::Relaxed);
    Status::Ok
}

#[shutdown_event_handler]
fn __shutdown_event_handler(ctx: &Context, _event: u64) {
    ctx.log_notice("Server shutdown callback event ...");
    IS_SHUTTING_DOWN.store(true, Ordering::Relaxed);
}

#[cfg(not(all(test, doctest)))]
macro_rules! get_allocator {
    () => {
        // Not `ValkeyAlloc` directly: it ignores `Layout::align()`, returning
        // misaligned memory for align > 16 (see `common::alloc`).
        $crate::common::alloc::AlignedValkeyAlloc
    };
}

#[cfg(all(test, doctest))]
macro_rules! get_allocator {
    () => {
        std::alloc::System
    };
}

valkey_module! {
    name: MODULE_NAME,
    version: VK_TIMESERIES_VERSION,
    allocator: (get_allocator!(), get_allocator!()),
    data_types: [VK_TIME_SERIES_TYPE],
    preload: preload,
    init: initialize,
    deinit: deinitialize,
    acl_categories: [
        "timeseries",
    ]
    // Command names are registered in lowercase to match RedisTimeSeries, both here and in
    // the `#[command]` annotations in `src/commands/*`. The registered name is what Valkey
    // echoes back in COMMAND INFO/DOCS and in the "wrong number of arguments for '<name>'
    // command" arity error; RedisTimeSeries uses lowercase there. Command dispatch is
    // case-insensitive, so clients may still invoke `TS.CREATE`, `ts.create`, etc.
    commands: [
        // User-facing commands are registered with full command info (summary, complexity,
        // arity, and key specs) through the `#[valkey_module_macros::command]` attribute on
        // each handler in `src/commands/*`; the `valkey_module!` macro registers them via
        // `register_commands`. Only internal/admin commands remain in this positional table.
        // ACL categories for the annotated commands come from the `acl_categories!`
        // declaration beside each handler, applied by `assign_command_acl_categories`.
        ["ts._debug", commands::ts_debug_cmd, "readonly", 0, 0, 0, "read timeseries admin"],
        ["ts._restore", commands::ts_restore_cmd, "write deny-oom", 1, 1, 1, "write timeseries admin"],
    ]
    event_handlers: [
        [@GENERIC @LOADED @TRIMMED: generic_key_events_handler]
    ]
}
