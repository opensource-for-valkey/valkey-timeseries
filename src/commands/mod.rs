pub mod command_parser;
mod fanout_codec;
mod forecast_utils;
mod label_search_utils;
mod ts_add;
mod ts_addbulk;
mod ts_alter;
mod ts_asm_restore;
mod ts_autocorrelation;
mod ts_autoforecast;
mod ts_card;
mod ts_card_fanout_command;
mod ts_create;
mod ts_createrule;
mod ts_debug;
mod ts_debug_configs;
mod ts_decompose;
mod ts_del;
mod ts_deleterule;
mod ts_features;
mod ts_fillgaps_cmd;
mod ts_forecast_cmd;
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
mod ts_outliers;
mod ts_periods;
mod ts_queryindex;
mod ts_queryindex_fanout_command;
mod ts_range;
mod ts_sanitize;
mod ts_stationarity;
mod ts_stats;
mod ts_trend;
mod utils;

// Command handlers are registered through the `#[valkey_module_macros::command]` attribute on
// each `ts_*_cmd` function (see the individual `ts_*` modules), so they no longer need to be
// re-exported here for the positional command table. Cross-module parser helpers are imported
// via their defining module path (e.g. `crate::commands::ts_create::parse_series_options`).
// Only modules whose items are consumed through `crate::commands::*` are re-exported below.
pub use command_parser::*;
pub use ts_asm_restore::*;
pub use ts_autocorrelation::*;
pub(crate) use ts_autoforecast::*;
pub use ts_autotrend::*;
pub use ts_debug::*;
pub use ts_decompose::*;
pub use ts_features::*;
pub use ts_fillgaps_cmd::*;
pub use ts_mget::*;
pub use ts_periods::*;
pub use ts_sanitize::*;
pub use ts_stationarity::*;
pub use ts_stats::*;
pub use ts_trend::*;
use valkey_module::ValkeyResult;

use crate::fanout::register_fanout_operation;
use ts_card_fanout_command::CardFanoutCommand;
use ts_label_search_fanout_command::LabelSearchFanoutCommand;
use ts_labelstats_fanout_command::LabelStatsFanoutCommand;
use ts_mdel_fanout_command::MDelFanoutCommand;
use ts_mget_fanout_command::MGetFanoutCommand;
use ts_mrange_fanout_command::MRangeFanoutCommand;
use ts_queryindex_fanout_command::QueryIndexFanoutCommand;

pub(crate) fn register_fanout_operations() -> ValkeyResult<()> {
    register_fanout_operation::<LabelStatsFanoutCommand>()?;
    register_fanout_operation::<CardFanoutCommand>()?;
    register_fanout_operation::<LabelSearchFanoutCommand>()?;
    register_fanout_operation::<MDelFanoutCommand>()?;
    register_fanout_operation::<MGetFanoutCommand>()?;
    register_fanout_operation::<MRangeFanoutCommand>()?;
    register_fanout_operation::<QueryIndexFanoutCommand>()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use crate::commands::register_fanout_operations;

    #[test]
    fn test_register_fanout_operations() {
        let result = register_fanout_operations();
        assert!(result.is_ok());
    }
}
