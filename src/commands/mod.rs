/// ACL categories for every command registered through `#[valkey_module_macros::command]`.
///
/// That attribute carries no ACL information, and the server derives none from the command
/// flags: `VM_SetCommandACLCategories` *replaces* a module command's category set, and a
/// command with no categories is invisible to `@timeseries` rules and — for a write command —
/// runnable by a `-@write` user. So each handler declares its own entry with [`acl_categories!`]
/// immediately above its `#[command]` annotation, and `assign_command_acl_categories` in
/// `src/lib.rs` applies them at load time, failing the load if any cannot be set. The
/// `every_annotated_command_declares_acl_categories` test below pins the two lists to the same
/// length, so a handler without a declaration (or a stray declaration) fails `cargo test`.
///
/// `TS._DEBUG` / `TS._RESTORE` are not here: they are registered through the positional
/// `commands:` table in `valkey_module!`, which sets categories itself.
#[linkme::distributed_slice]
pub static COMMAND_ACL_CATEGORIES: [(&'static str, &'static str)] = [..];

/// Declares the ACL categories of a `#[command]`-annotated handler; see
/// [`COMMAND_ACL_CATEGORIES`]. Place it directly above the annotation:
///
/// ```ignore
/// acl_categories!(TS_CREATE, "ts.create", "write fast timeseries");
/// #[valkey_module_macros::command({ name: "ts.create", ... })]
/// fn ts_create_cmd(...) { ... }
/// ```
///
/// The first argument names the generated `static`; the second is the command name exactly as
/// registered (lowercase); the third is the space-separated category list, which must include
/// `timeseries` and exactly one of `read` / `write`.
macro_rules! acl_categories {
    ($ident:ident, $name:literal, $categories:literal) => {
        #[linkme::distributed_slice(crate::commands::COMMAND_ACL_CATEGORIES)]
        static $ident: (&'static str, &'static str) = ($name, $categories);
    };
}

pub mod command_parser;
mod fanout_codec;
mod label_search_utils;
mod ts_add;
mod ts_addbulk;
mod ts_alter;
mod ts_card;
mod ts_card_fanout_command;
mod ts_create;
mod ts_createrule;
mod ts_debug;
mod ts_debug_configs;
mod ts_del;
mod ts_deleterule;
mod ts_get;
mod ts_incr_decr_by;
mod ts_info;
mod ts_join;
mod ts_label_search_fanout_command;
mod ts_labelnames;
mod ts_labelstats;
mod ts_labelstats_fanout_command;
mod ts_labelvalues;
mod ts_madd;
mod ts_mdel;
mod ts_mdel_fanout_command;
mod ts_metricnames;
mod ts_mget;
mod ts_mget_fanout_command;
mod ts_mrange;
mod ts_mrange_fanout_command;
mod ts_nrange;
mod ts_outliers;
mod ts_queryindex;
mod ts_queryindex_fanout_command;
mod ts_querylabels;
mod ts_querylabels_fanout_command;
mod ts_range;
mod ts_read;
mod ts_restore;
mod utils;

// Command handlers are registered through the `#[valkey_module_macros::command]` attribute on
// each `ts_*_cmd` function (see the individual `ts_*` modules), so they no longer need to be
// re-exported here for the positional command table. Cross-module parser helpers are imported
// via their defining module path (e.g. `crate::commands::ts_create::parse_series_options`).
// Only modules whose items are consumed through `crate::commands::*` are re-exported below.
pub use command_parser::*;
pub use ts_debug::*;
pub use ts_mget::*;
pub use ts_restore::*;
use valkey_module::ValkeyResult;

use crate::fanout::register_fanout_operation;
use ts_card_fanout_command::CardFanoutCommand;
use ts_label_search_fanout_command::LabelSearchFanoutCommand;
use ts_labelstats_fanout_command::LabelStatsFanoutCommand;
use ts_mdel_fanout_command::MDelFanoutCommand;
use ts_mget_fanout_command::MGetFanoutCommand;
use ts_mrange_fanout_command::MRangeFanoutCommand;
use ts_queryindex_fanout_command::QueryIndexFanoutCommand;
use ts_querylabels_fanout_command::QueryLabelsFanoutCommand;

pub(crate) fn register_fanout_operations() -> ValkeyResult<()> {
    register_fanout_operation::<LabelStatsFanoutCommand>()?;
    register_fanout_operation::<CardFanoutCommand>()?;
    register_fanout_operation::<LabelSearchFanoutCommand>()?;
    register_fanout_operation::<MDelFanoutCommand>()?;
    register_fanout_operation::<MGetFanoutCommand>()?;
    register_fanout_operation::<MRangeFanoutCommand>()?;
    register_fanout_operation::<QueryIndexFanoutCommand>()?;
    register_fanout_operation::<QueryLabelsFanoutCommand>()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use crate::commands::{COMMAND_ACL_CATEGORIES, register_fanout_operations};

    #[test]
    fn test_register_fanout_operations() {
        let result = register_fanout_operations();
        assert!(result.is_ok());
    }

    /// Every `#[command]` handler must carry an `acl_categories!` declaration and vice versa.
    /// `CommandInfo` exposes no accessors, so the two registries are pinned by count plus
    /// uniqueness; a misspelled command name is caught at load time instead, where
    /// `assign_command_acl_categories` refuses to load the module.
    #[test]
    fn every_annotated_command_declares_acl_categories() {
        let mut names: Vec<&str> = COMMAND_ACL_CATEGORIES.iter().map(|(n, _)| *n).collect();
        names.sort_unstable();
        let declared = names.len();
        names.dedup();
        assert_eq!(
            declared,
            names.len(),
            "duplicate acl_categories! declaration: {names:?}"
        );

        assert_eq!(
            valkey_module::commands::COMMANDS_LIST.len(),
            declared,
            "every #[command] handler needs an acl_categories! declaration (declared: {names:?})"
        );

        for (name, categories) in COMMAND_ACL_CATEGORIES {
            assert!(
                name.starts_with("ts.") && *name == name.to_lowercase(),
                "{name}: command names are registered lowercase with a `ts.` prefix"
            );
            let categories: Vec<&str> = categories.split_whitespace().collect();
            assert!(
                categories.contains(&"timeseries"),
                "{name}: missing `timeseries` category"
            );
            let rw = categories
                .iter()
                .filter(|c| matches!(**c, "read" | "write"))
                .count();
            assert_eq!(
                rw, 1,
                "{name}: expected exactly one of `read` / `write`, got {categories:?}"
            );
        }
    }
}
