mod query_fanout_command;
mod query_range_fanout_command;
mod query_utils;
mod type_conversions;

pub(in crate::promql) use query_fanout_command::QueryFanoutCommand;
pub(in crate::promql) use query_range_fanout_command::QueryRangeFanoutCommand;
pub(in crate::promql) use type_conversions::{metric_name_to_proto_labels, proto_labels_to_labels};
use valkey_module::ValkeyResult;

use crate::fanout::register_fanout_operation;

pub(crate) fn register_fanout_commands() -> ValkeyResult<()> {
    register_fanout_operation::<QueryFanoutCommand>()?;
    register_fanout_operation::<QueryRangeFanoutCommand>()?;
    Ok(())
}
