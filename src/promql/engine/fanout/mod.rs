mod query_fanout_command;
mod query_range_fanout_command;
mod query_utils;
mod type_conversions;
mod vector_selector_fanout_command;

pub(in crate::promql) use query_fanout_command::QueryFanoutCommand;
pub(in crate::promql) use query_range_fanout_command::QueryRangeFanoutCommand;
use valkey_module::ValkeyResult;
pub(in crate::promql) use vector_selector_fanout_command::VectorSelectorFanoutCommand;

use crate::fanout::register_fanout_operation;

pub(crate) fn register_fanout_commands() -> ValkeyResult<()> {
    register_fanout_operation::<VectorSelectorFanoutCommand>()?;
    register_fanout_operation::<QueryFanoutCommand>()?;
    register_fanout_operation::<QueryRangeFanoutCommand>()?;
    Ok(())
}
