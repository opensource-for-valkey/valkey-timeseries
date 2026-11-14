//! `TS.JOIN`: join two series on sample timestamps.
use crate::common::{Sample, Timestamp};
use crate::error_consts;
use crate::series::TimestampRange;
use crate::series::request_types::AggregationOptions;
use crate::series::types::ValueFilter;
use itertools::EitherOrBoth;
use std::time::Duration;
use valkey_module::{ValkeyError, ValkeyResult};

mod asof;
mod join_handler;
mod join_iter;
mod join_reducer;

#[cfg(test)]
mod join_handler_tests;

pub use asof::AsOfJoinStrategy;
use asof::JoinAsOfIter;
pub use join_handler::{JoinInputs, JoinResultType};
use join_iter::create_join_iter;
pub use join_reducer::JoinReducer;

/// One joined row: a sample from either side, or both. Exact joins pair equal timestamps;
/// an ASOF row pairs a left sample with the right sample it matched.
pub type JoinValue = EitherOrBoth<Sample, Sample>;

#[derive(Clone, Debug, Copy, PartialEq)]
pub struct AsOfJoinOptions {
    pub strategy: AsOfJoinStrategy,
    /// Maximum distance to a match; `None` for no limit.
    pub tolerance: Option<Duration>,
    pub allow_exact_match: bool,
}

#[derive(Debug, Default, Copy, Clone, PartialEq)]
pub enum JoinType {
    Left,
    Right,
    #[default]
    Inner,
    Full,
    Semi,
    Anti,
    AsOf(AsOfJoinOptions),
}

impl JoinType {
    /// SEMI and ANTI return left samples only, so they take no reducer.
    pub fn is_filtering(&self) -> bool {
        matches!(self, JoinType::Semi | JoinType::Anti)
    }
}

#[derive(Debug, Default)]
pub struct JoinOptions {
    pub join_type: JoinType,
    pub date_range: TimestampRange,
    pub count: Option<usize>,
    pub timestamp_filter: Option<Vec<Timestamp>>,
    pub value_filter: Option<ValueFilter>,
    pub reducer: Option<JoinReducer>,
    pub aggregation: Option<AggregationOptions>,
}

impl JoinOptions {
    /// The option combinations a join accepts. The parser calls this once every argument is
    /// read; `process_join` calls it again before reading either series.
    pub fn validate(&self) -> ValkeyResult<()> {
        if self.join_type.is_filtering() {
            if self.reducer.is_some() {
                return Err(ValkeyError::Str(
                    error_consts::JOIN_REDUCER_WITH_SEMI_OR_ANTI,
                ));
            }
        } else if self.aggregation.is_some() && self.reducer.is_none() {
            // Aggregation needs one value per row: a reducer's, or SEMI/ANTI's left sample.
            return Err(ValkeyError::Str(error_consts::MISSING_JOIN_REDUCER));
        }
        Ok(())
    }
}
