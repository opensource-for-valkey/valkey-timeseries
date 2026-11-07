use crate::aggregators::AggregationHandler;
use crate::aggregators::kahan::KahanSum;
use crate::common::binop::{ComparisonOperator, op_eq};
use crate::common::rdb::{
    RdbSerializable, rdb_load_bool, rdb_load_optional_bool, rdb_load_usize, rdb_save_bool,
    rdb_save_optional_bool, rdb_save_usize,
};
use crate::series::request_types::ValueComparisonFilter;
use get_size2::GetSize;
use std::hash::Hash;
use valkey_module::ValkeyResult;
use valkey_module::{RedisModuleIO, raw};

/// COUNTIF's state: an integer tally of matches. A compensated float sum of ones is exact anyway,
/// and costs the hot loop a Neumaier step and the state a heap allocation.
#[derive(Debug, Default, Clone, Copy, Hash, PartialEq, GetSize)]
pub struct ConditionalTally {
    pub count: u64,
    pub filter: ValueComparisonFilter,
}

impl ConditionalTally {
    pub fn new(op: ComparisonOperator, comparand: f64) -> Self {
        Self {
            count: 0,
            filter: ValueComparisonFilter {
                operator: op,
                value: comparand,
            },
        }
    }

    /// NaN is skipped, as by the rest of the filtered family.
    #[inline]
    fn update(&mut self, value: f64) -> bool {
        if value.is_nan() {
            return false;
        }
        self.count += u64::from(self.filter.compare(value));
        true
    }

    pub fn save_to_rdb(&self, rdb: *mut RedisModuleIO) {
        self.filter.save_to_rdb(rdb);
        raw::save_unsigned(rdb, self.count);
    }

    pub fn load_from_rdb(rdb: *mut RedisModuleIO) -> ValkeyResult<Self> {
        let filter = ValueComparisonFilter::load_from_rdb(rdb)?;
        let count = raw::load_unsigned(rdb)?;
        Ok(Self { count, filter })
    }
}

#[derive(Debug, Clone, Copy, GetSize)]
pub struct ConditionalCountState {
    pub has_samples: bool, // used a bool since sizeof::<Option<f64>>() is 16 bytes instead of 8
    pub sum: KahanSum,
    pub filter: ValueComparisonFilter,
}

impl Hash for ConditionalCountState {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.filter.hash(state);
        self.sum.hash(state);
        self.has_samples.hash(state);
    }
}

impl PartialEq for ConditionalCountState {
    fn eq(&self, other: &Self) -> bool {
        self.filter == other.filter
            // op_eq for its NaN == NaN semantics; compare both parts so
            // Eq stays consistent with Hash (which covers the full state).
            && op_eq(self.sum.value(), other.sum.value())
            && op_eq(self.sum.err(), other.sum.err())
            && self.has_samples == other.has_samples
    }
}

impl Default for ConditionalCountState {
    fn default() -> Self {
        Self {
            sum: KahanSum::new(),
            has_samples: false,
            filter: ValueComparisonFilter::default(),
        }
    }
}

impl ConditionalCountState {
    pub fn new(op: ComparisonOperator, comparand: f64) -> Self {
        Self {
            sum: KahanSum::new(),
            has_samples: false,
            filter: ValueComparisonFilter {
                operator: op,
                value: comparand,
            },
        }
    }

    pub fn reset(&mut self) {
        self.sum.reset();
        self.has_samples = false;
    }

    // NaN is skipped, as by every filtered aggregator (and by the cluster push-down's partial
    // reducer). Passing it to the filter let `!=` match it, so `sumif` over [1, NaN] returned
    // NaN and `countif` counted it — and a push-down GROUPBY of the same query answered 1.
    fn update_sum(&mut self, value: f64) -> bool {
        if value.is_nan() {
            return false;
        }
        if self.filter.compare(value) {
            self.has_samples = true;
            self.sum += value;
        }
        true
    }

    pub fn get_value(&self) -> f64 {
        if !self.has_samples {
            return f64::NAN;
        }

        self.sum.value()
    }

    pub fn save_to_rdb(&self, rdb: *mut RedisModuleIO) {
        self.filter.save_to_rdb(rdb);
        self.sum.rdb_save(rdb);
        rdb_save_bool(rdb, self.has_samples);
    }

    pub fn load_from_rdb(rdb: *mut RedisModuleIO) -> ValkeyResult<Self> {
        let filter = ValueComparisonFilter::load_from_rdb(rdb)?;
        let sum = KahanSum::rdb_load(rdb)?;
        let has_samples = rdb_load_bool(rdb)?;

        Ok(Self {
            filter,
            sum,
            has_samples,
        })
    }
}

#[derive(Debug, Default, Clone, Hash, PartialEq, GetSize)]
pub struct CountIfAggregator(ConditionalTally);

impl AggregationHandler for CountIfAggregator {
    fn update(&mut self, _timestamp: i64, value: f64) -> bool {
        self.0.update(value)
    }

    fn reset(&mut self) {
        self.0.count = 0;
    }

    fn current(&self) -> Option<f64> {
        (self.0.count > 0).then_some(self.0.count as f64)
    }

    fn empty_value(&self) -> f64 {
        0.0
    }
}

impl RdbSerializable for CountIfAggregator {
    fn rdb_save(&self, rdb: *mut RedisModuleIO) {
        self.0.save_to_rdb(rdb);
    }

    fn rdb_load(rdb: *mut RedisModuleIO) -> ValkeyResult<Self> {
        ConditionalTally::load_from_rdb(rdb).map(Self)
    }
}

#[derive(Debug, Clone, Hash, PartialEq, GetSize)]
pub struct SumIfAggregator(Box<ConditionalCountState>);

impl RdbSerializable for SumIfAggregator {
    fn rdb_save(&self, rdb: *mut RedisModuleIO) {
        self.0.save_to_rdb(rdb);
    }

    fn rdb_load(rdb: *mut RedisModuleIO) -> ValkeyResult<Self> {
        let state = ConditionalCountState::load_from_rdb(rdb)?;
        Ok(Self(Box::new(state)))
    }
}

impl AggregationHandler for SumIfAggregator {
    fn update(&mut self, _timestamp: i64, value: f64) -> bool {
        self.0.update_sum(value)
    }

    fn reset(&mut self) {
        self.0.reset();
    }

    fn current(&self) -> Option<f64> {
        if self.0.has_samples {
            Some(self.0.get_value())
        } else {
            None
        }
    }

    fn empty_value(&self) -> f64 {
        0.0
    }
}

impl Default for SumIfAggregator {
    fn default() -> Self {
        Self::new(ComparisonOperator::NotEqual, f64::NAN)
    }
}

#[derive(Debug, Default, Clone, Copy, Hash, PartialEq, GetSize)]
pub struct ShareAggregatorState {
    pub count: usize,
    pub match_count: usize,
    pub filter: ValueComparisonFilter,
}

impl ShareAggregatorState {
    pub fn reset(&mut self) {
        self.count = 0;
        self.match_count = 0;
    }
    pub fn share(&self) -> f64 {
        if self.count == 0 {
            return f64::NAN;
        }

        (self.match_count as f64) / (self.count as f64)
    }

    /// NaN is skipped like everywhere else in the filtered family: under `!=` it would match,
    /// and under any other operator it would still count against the share.
    pub fn update(&mut self, value: f64) -> bool {
        if value.is_nan() {
            return false;
        }
        self.count = self.count.saturating_add(1);
        if self.filter.compare(value) {
            self.match_count = self.match_count.saturating_add(1);
        }
        true
    }
}

impl RdbSerializable for ShareAggregatorState {
    fn rdb_save(&self, rdb: *mut RedisModuleIO) {
        self.filter.save_to_rdb(rdb);
        rdb_save_usize(rdb, self.count);
        rdb_save_usize(rdb, self.match_count);
    }

    fn rdb_load(rdb: *mut RedisModuleIO) -> ValkeyResult<Self> {
        let filter = ValueComparisonFilter::load_from_rdb(rdb)?;
        let count = rdb_load_usize(rdb)?;
        let match_count = rdb_load_usize(rdb)?;
        Ok(Self {
            filter,
            count,
            match_count,
        })
    }
}

/// An Aggregator that computes the share of values matching a condition.
/// The share is defined as the percentage (in the range [0..1)) of raw values matching the condition
#[derive(Debug, Default, Clone, GetSize, Hash, PartialEq)]
pub struct ShareAggregator(ShareAggregatorState);

impl ShareAggregator {
    pub fn new(op: ComparisonOperator, comparand: f64) -> Self {
        Self(ShareAggregatorState {
            count: 0,
            match_count: 0,
            filter: ValueComparisonFilter {
                operator: op,
                value: comparand,
            },
        })
    }

    pub fn share(&self) -> f64 {
        self.0.share()
    }

    pub fn condition(&self) -> ValueComparisonFilter {
        self.0.filter
    }
}

impl RdbSerializable for ShareAggregator {
    fn rdb_save(&self, rdb: *mut RedisModuleIO) {
        self.0.rdb_save(rdb);
    }

    fn rdb_load(rdb: *mut RedisModuleIO) -> ValkeyResult<Self> {
        ShareAggregatorState::rdb_load(rdb).map(Self)
    }
}

impl AggregationHandler for ShareAggregator {
    fn update(&mut self, _timestamp: i64, value: f64) -> bool {
        self.0.update(value)
    }

    fn reset(&mut self) {
        self.0.reset();
    }

    fn current(&self) -> Option<f64> {
        if self.0.count > 0 {
            Some(self.share())
        } else {
            None
        }
    }
}

// Support for ALL, ANY and NONE. Each update skips NaN, like the rest of the filtered family.
#[derive(Debug, Default, Clone, Copy, Hash, PartialEq, GetSize)]
struct AllNoneAggregatorState {
    matches: Option<bool>,
    filter: ValueComparisonFilter,
}

impl AllNoneAggregatorState {
    fn update_all(&mut self, value: f64) -> bool {
        if value.is_nan() {
            return false;
        }
        match self.matches {
            Some(false) => { // already failed
            }
            _ => {
                let matched = self.compare(value);
                self.matches = Some(matched);
            }
        }
        true
    }

    fn update_none(&mut self, value: f64) -> bool {
        if value.is_nan() {
            return false;
        }
        match self.matches {
            Some(false) => (), // already false
            _ => {
                self.matches = Some(!self.compare(value));
            }
        }
        true
    }

    fn update_any(&mut self, value: f64) -> bool {
        if value.is_nan() {
            return false;
        }
        match self.matches {
            Some(true) => (), // already matched
            _ => {
                self.matches = Some(self.compare(value));
            }
        }
        true
    }

    pub fn new(operator: ComparisonOperator, value: f64) -> Self {
        Self {
            matches: None,
            filter: ValueComparisonFilter { operator, value },
        }
    }

    pub fn reset(&mut self) {
        self.matches = None;
    }

    fn current(&self) -> Option<f64> {
        match self.matches {
            Some(true) => Some(1.0),
            Some(false) => Some(0.0),
            None => None,
        }
    }

    fn compare(&self, value: f64) -> bool {
        self.filter.compare(value)
    }

    fn save_to_rdb(&self, rdb: *mut RedisModuleIO) {
        self.filter.save_to_rdb(rdb);
        rdb_save_optional_bool(rdb, self.matches);
    }

    fn load_from_rdb(rdb: *mut RedisModuleIO) -> ValkeyResult<Self> {
        let filter = ValueComparisonFilter::load_from_rdb(rdb)?;
        let matches = rdb_load_optional_bool(rdb)?;

        Ok(Self { filter, matches })
    }
}

/// Aggregator that returns a 1.0 if all values match a condition
#[derive(Debug, Default, Clone, Hash, PartialEq, GetSize)]
pub struct AllAggregator(AllNoneAggregatorState);

impl RdbSerializable for AllAggregator {
    fn rdb_save(&self, rdb: *mut RedisModuleIO) {
        self.0.save_to_rdb(rdb);
    }

    fn rdb_load(rdb: *mut RedisModuleIO) -> ValkeyResult<Self> {
        AllNoneAggregatorState::load_from_rdb(rdb).map(Self)
    }
}

impl AggregationHandler for AllAggregator {
    fn update(&mut self, _timestamp: i64, value: f64) -> bool {
        self.0.update_all(value)
    }

    fn reset(&mut self) {
        self.0.reset();
    }

    fn current(&self) -> Option<f64> {
        self.0.current()
    }
}

/// Aggregator that checks returns a 1.0 if none of the values match a condition
#[derive(Debug, Default, Clone, Hash, PartialEq, GetSize)]
pub struct NoneAggregator(AllNoneAggregatorState);

impl AggregationHandler for NoneAggregator {
    fn update(&mut self, _timestamp: i64, value: f64) -> bool {
        self.0.update_none(value)
    }

    fn reset(&mut self) {
        self.0.reset();
    }

    fn current(&self) -> Option<f64> {
        self.0.current()
    }
}

impl RdbSerializable for NoneAggregator {
    fn rdb_save(&self, rdb: *mut RedisModuleIO) {
        self.0.save_to_rdb(rdb);
    }

    fn rdb_load(rdb: *mut RedisModuleIO) -> ValkeyResult<Self> {
        AllNoneAggregatorState::load_from_rdb(rdb).map(Self)
    }
}

/// Any aggregator that checks if any of the values match a condition
#[derive(Debug, Default, Clone, Hash, PartialEq, GetSize)]
pub struct AnyAggregator(AllNoneAggregatorState);
impl AggregationHandler for AnyAggregator {
    fn update(&mut self, _timestamp: i64, value: f64) -> bool {
        self.0.update_any(value)
    }

    fn reset(&mut self) {
        self.0.reset();
    }

    fn current(&self) -> Option<f64> {
        self.0.current()
    }
}

impl RdbSerializable for AnyAggregator {
    fn rdb_save(&self, rdb: *mut RedisModuleIO) {
        self.0.save_to_rdb(rdb);
    }

    fn rdb_load(rdb: *mut RedisModuleIO) -> ValkeyResult<Self> {
        AllNoneAggregatorState::load_from_rdb(rdb).map(Self)
    }
}

macro_rules! impl_conditional_aggregator {
    ($name:ident, $state:ty) => {
        impl_conditional_aggregator!($name, $state, |state| state);
    };
    ($name:ident, $state:ty, boxed) => {
        impl_conditional_aggregator!($name, $state, Box::new);
    };
    ($name:ident, $state:ty, $wrap:expr) => {
        impl $name {
            pub fn new(op: ComparisonOperator, comparand: f64) -> Self {
                Self($wrap(<$state>::new(op, comparand)))
            }

            pub fn condition(&self) -> ValueComparisonFilter {
                self.0.filter
            }

            pub fn save_to_rdb(&self, rdb: *mut RedisModuleIO) {
                self.0.save_to_rdb(rdb);
            }

            pub fn load_from_rdb(rdb: *mut RedisModuleIO) -> ValkeyResult<Self> {
                let state = <$state>::load_from_rdb(rdb)?;
                Ok(Self($wrap(state)))
            }
        }
    };
}

impl_conditional_aggregator!(CountIfAggregator, ConditionalTally);
// 40 bytes, past the `Aggregator` payload: inlining it would grow every aggregator.
impl_conditional_aggregator!(SumIfAggregator, ConditionalCountState, boxed);
impl_conditional_aggregator!(AllAggregator, AllNoneAggregatorState);
impl_conditional_aggregator!(NoneAggregator, AllNoneAggregatorState);
impl_conditional_aggregator!(AnyAggregator, AllNoneAggregatorState);

#[cfg(test)]
mod tests {
    use super::*;

    fn not_equal_5() -> Box<ConditionalCountState> {
        Box::new(ConditionalCountState::new(
            ComparisonOperator::NotEqual,
            5.0,
        ))
    }

    #[test]
    fn test_sumif_and_countif_skip_nan() {
        // `NaN != 5` is true, so NaN used to pass the filter: `sumif` returned NaN and
        // `countif` counted it.
        let mut sum_if = SumIfAggregator(not_equal_5());
        let mut count_if = CountIfAggregator::new(ComparisonOperator::NotEqual, 5.0);
        for value in [1.0, f64::NAN, 5.0, 2.0] {
            sum_if.update(0, value);
            count_if.update(0, value);
        }
        assert_eq!(sum_if.current(), Some(3.0));
        assert_eq!(count_if.current(), Some(2.0));
    }

    #[test]
    fn test_sumif_nan_is_not_a_sample() {
        let mut sum_if = SumIfAggregator(not_equal_5());
        assert!(!sum_if.update(0, f64::NAN));
        assert_eq!(sum_if.current(), None);
    }
}
