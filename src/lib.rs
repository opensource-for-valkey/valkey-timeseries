#![allow(dead_code)]
#![deny(unsafe_op_in_unsafe_fn)]
extern crate enum_dispatch;
extern crate get_size2;
#[cfg(test)]
extern crate serial_test;
extern crate strum;
extern crate strum_macros;
extern crate valkey_module_macros;

use crate::commands::register_fanout_operations;
use crate::common::threads::init_thread_pool;
use crate::config::register_config;
use crate::fanout::{init_fanout, is_clustered};
use logger_rust::{LogLevel, set_log_level};
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread::ThreadId;
use valkey_module::{Context, Status, ValkeyString, Version, valkey_module};
use valkey_module_macros::shutdown_event_handler;

pub mod aggregators;
mod analysis;
pub(crate) mod commands;
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

    Status::Ok
}

/// ACL categories for the commands registered through the `#[valkey_module_macros::command]`
/// command-info path, keyed by command name. That path (via `register_commands`) does not
/// assign ACL categories the way the positional command table does, so we (re-)apply them at
/// load time in [`assign_command_acl_categories`]. Keep this in sync with the `#[command]`
/// annotations in `src/commands/*`.
#[cfg(feature = "min-valkey-compatibility-version-8-0")]
const COMMAND_ACL_CATEGORIES: &[(&str, &str)] = &[
    ("TS.CREATE", "write fast timeseries"),
    ("TS.ALTER", "write timeseries"),
    ("TS.ADD", "write timeseries"),
    ("TS.ADDBULK", "write timeseries"),
    ("TS.GET", "fast read timeseries"),
    ("TS.MGET", "fast read timeseries"),
    ("TS.MADD", "fast write timeseries"),
    ("TS.DEL", "write timeseries"),
    ("TS.DECRBY", "write timeseries"),
    ("TS.INCRBY", "write timeseries"),
    ("TS.JOIN", "read timeseries"),
    ("TS.MDEL", "write timeseries"),
    ("TS.MRANGE", "read timeseries"),
    ("TS.MREVRANGE", "read timeseries"),
    ("TS.RANGE", "read timeseries"),
    ("TS.REVRANGE", "read timeseries"),
    ("TS.INFO", "read fast timeseries"),
    ("TS.QUERYINDEX", "read timeseries"),
    ("TS.CARD", "read timeseries"),
    ("TS.LABELNAMES", "read timeseries"),
    ("TS.LABELVALUES", "read timeseries"),
    ("TS.METRICNAMES", "read timeseries"),
    ("TS.LABELSTATS", "read timeseries"),
    ("TS.CREATERULE", "write timeseries"),
    ("TS.DELETERULE", "write timeseries"),
    ("TS.OUTLIERS", "fast read timeseries"),
    ("TS.XCORR", "read timeseries"),
    ("TS.FORECAST", "write timeseries"),
    ("TS.AUTOFORECAST", "read write timeseries"),
    ("TS.BACKTEST", "read timeseries"),
    ("TS.DECOMPOSE", "read timeseries"),
    ("TS.PERIODS", "read timeseries"),
    ("TS.AUTOCORRELATION", "read timeseries"),
    ("TS.TREND", "read write timeseries"),
    ("TS.FILLGAPS", "fast write timeseries"),
    ("TS.SANITIZE", "write timeseries"),
    ("TS.STATS", "fast read timeseries"),
    ("TS.FEATURES", "read timeseries"),
    ("TS.STATIONARITY", "fast read timeseries"),
];

/// Assign ACL categories to the commands registered via the command-info path. The
/// `#[command]`-based registration performed by `register_commands` does not set ACL
/// categories, so we apply them here to keep the custom `@timeseries` category (and the
/// built-in read/write/fast categories) associated with each command.
#[cfg(feature = "min-valkey-compatibility-version-8-0")]
fn assign_command_acl_categories(ctx: &Context) {
    use std::ffi::CString;
    for (name, categories) in COMMAND_ACL_CATEGORIES {
        if let (Ok(command), Ok(acl)) = (CString::new(*name), CString::new(*categories))
            && Status::Err == ctx.set_acl_category(command.as_ptr(), acl.as_ptr())
        {
            // Handle the result if necessary, e.g., log errors or assert success
            ctx.log_warning(&format!("Failed to set ACL category for command {name}"));
        }
    }
}

#[cfg(not(feature = "min-valkey-compatibility-version-8-0"))]
fn assign_command_acl_categories(_ctx: &Context) {}

fn initialize(ctx: &Context, args: &[ValkeyString]) -> Status {
    init_croaring_allocator();

    set_log_level(LogLevel::Console);

    if let Err(e) = register_config(ctx, args) {
        let msg = format!("Failed to register config: {e}");
        ctx.log_warning(&msg);
        return Status::Err;
    }

    assign_command_acl_categories(ctx);

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
fn shutdown_event_handler(ctx: &Context, _event: u64) {
    ctx.log_notice("Server shutdown callback event ...");
    IS_SHUTTING_DOWN.store(true, Ordering::Relaxed);
}

#[cfg(not(any(test, doctest, use_system_alloc)))]
macro_rules! get_allocator {
    () => {
        // Not `ValkeyAlloc` directly: it ignores `Layout::align()`, returning
        // misaligned memory for align > 16 (see `common::alloc`).
        $crate::common::alloc::AlignedValkeyAlloc
    };
}

#[cfg(any(test, doctest, use_system_alloc))]
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
    commands: [
        // User-facing commands are registered with full command info (summary, complexity,
        // arity, and key specs) through the `#[valkey_module_macros::command]` attribute on
        // each handler in `src/commands/*`; the `valkey_module!` macro registers them via
        // `register_commands`. Only internal/admin commands remain in this positional table.
        // ACL categories for the annotated commands are (re-)applied by
        // `assign_command_acl_categories`, since the command-info path does not set them.
        ["TS._DEBUG", commands::ts_debug_cmd, "readonly", 0, 0, 0, "read timeseries admin"],
        ["TS._RESTORE", commands::ts_asm_restore_cmd, "write deny-oom", 1, 1, 1, "write timeseries admin"],
    ]
    event_handlers: [
        [@GENERIC @LOADED @TRIMMED: generic_key_events_handler]
    ]
}
