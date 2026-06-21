mod instant_vector_selector_fanout_command;
mod range_vector_selector_fanout_command;
mod query_utils;
mod type_conversions;

pub(in crate::promql) use instant_vector_selector_fanout_command::InstantVectorSelectorFanoutCommand;
pub(in crate::promql) use range_vector_selector_fanout_command::RangeVectorSelectorFanoutCommand;
pub(in crate::promql) use type_conversions::{metric_name_to_proto_labels, proto_labels_to_labels};
use valkey_module::ValkeyResult;

use crate::fanout::{ErrorKind, FanoutError, register_fanout_operation};
use crate::promql::QueryError;

impl From<FanoutError> for QueryError {
    fn from(value: FanoutError) -> Self {
        if value.kind == ErrorKind::Timeout {
            return QueryError::Timeout;
        }
        QueryError::Execution(value.to_string())
    }
}

pub(crate) fn register_fanout_commands() -> ValkeyResult<()> {
    register_fanout_operation::<InstantVectorSelectorFanoutCommand>()?;
    register_fanout_operation::<RangeVectorSelectorFanoutCommand>()?;
    Ok(())
}
