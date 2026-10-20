//! `TS._DEBUG`: internal, undocumented-API introspection, gated on `debug-mode`.
//!
//! [`ts_debug`] holds the entry point and dispatcher; each larger subcommand has its own module.
//! `STATS`, `STRINGPOOLSTATS` and `INDEXMEMORY` report this node unless given `CLUSTER`, which
//! fans them out across the cluster; their fanout commands live here too and are registered with
//! the rest in `commands::register_fanout_operations`.

mod configs;
mod index_memory;
mod index_memory_fanout_command;
mod stats;
mod stats_fanout_command;
mod string_pool_stats;
mod string_pool_stats_fanout_command;
mod target;
mod ts_debug;

use crate::config::is_debug_mode_enabled;
use crate::error_consts;
use valkey_module::{ValkeyError, ValkeyResult};

pub(super) use index_memory_fanout_command::IndexMemoryFanoutCommand;
pub(super) use stats_fanout_command::StatsFanoutCommand;
pub(super) use string_pool_stats_fanout_command::StringPoolStatsFanoutCommand;
pub use ts_debug::ts_debug_cmd;

/// Gates debug commands and fanout responders on this node's debug-mode setting.
fn ensure_debug_enabled() -> ValkeyResult<()> {
    if !is_debug_mode_enabled() {
        return Err(ValkeyError::Str(error_consts::DEBUG_MODE_DISABLED));
    }
    Ok(())
}
