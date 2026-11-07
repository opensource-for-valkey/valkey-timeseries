// Initial code based on:
// https://github.com/cryptorelay/redis-aggregation/tree/master
// License: Apache License 2.0

use super::{AggregationType, AllAggregator, AnyAggregator, NoneAggregator};
use crate::aggregators::filtered::{CountIfAggregator, ShareAggregator, SumIfAggregator};
use crate::aggregators::kahan::{KahanAvg, KahanSum};
use crate::common::Sample;
use crate::common::hash::hash_f64;
use crate::common::rdb::{
    RdbSerializable, rdb_load_bool, rdb_load_optional_f64, rdb_load_u8, rdb_load_usize,
    rdb_save_bool, rdb_save_optional_f64, rdb_save_u8, rdb_save_usize,
};
use crate::series::request_types::ValueComparisonFilter;
use enum_dispatch::enum_dispatch;
use get_size2::GetSize;
use std::fmt::Display;
use std::hash::Hash;
use std::time::Duration;
use valkey_module::{RedisModuleIO, ValkeyResult, raw};

type Value = f64;
type Timestamp = i64;

#[enum_dispatch]
pub trait AggregationHandler: RdbSerializable + Default + GetSize {
    fn update(&mut self, timestamp: Timestamp, value: Value) -> bool;
    /// Ends the bucket: what the next bucket's value depends on stays. Only the counter
    /// aggregators keep anything, their last sample, so a delta crossing the boundary is counted.
    fn reset(&mut self);
    /// Starts over, as a fresh aggregator: unlike [`Self::reset`] it also drops the counters'
    /// carried sample.
    fn clear(&mut self) {
        self.reset();
    }
    fn current(&self) -> Option<Value>;
    fn empty_value(&self) -> Value {
        f64::NAN
    }
    fn finalize(&mut self) -> f64 {
        let result = if let Some(v) = self.current() {
            v
        } else {
            self.empty_value()
        };
        self.reset();
        result
    }
}

// -- First ----------------------------------------------------------------

#[derive(Copy, Clone, Default, Debug, PartialEq, GetSize)]
pub struct FirstAggregator(Option<Value>);
impl AggregationHandler for FirstAggregator {
    fn update(&mut self, _timestamp: Timestamp, value: Value) -> bool {
        if value.is_nan() {
            return false;
        }
        if self.0.is_none() {
            self.0 = Some(value);
        }
        true
    }
    fn reset(&mut self) {
        self.0 = None;
    }
    fn current(&self) -> Option<Value> {
        self.0
    }
}

impl RdbSerializable for FirstAggregator {
    fn rdb_save(&self, rdb: *mut RedisModuleIO) {
        rdb_save_optional_f64(rdb, self.0)
    }
    fn rdb_load(rdb: *mut RedisModuleIO) -> ValkeyResult<Self>
    where
        Self: Sized,
    {
        rdb_load_optional_f64(rdb).map(Self)
    }
}

impl Hash for FirstAggregator {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        hash_f64(self.0.unwrap_or(f64::NAN), state);
    }
}

// -- Last ----------------------------------------------------------------

#[derive(Copy, Clone, Default, Debug, PartialEq, GetSize)]
pub struct LastAggregator {
    current: Option<f64>,
}

impl AggregationHandler for LastAggregator {
    fn update(&mut self, _timestamp: i64, value: f64) -> bool {
        if value.is_nan() {
            return false;
        }
        self.current = Some(value);
        true
    }

    fn reset(&mut self) {
        self.current = None;
    }

    fn current(&self) -> Option<Value> {
        self.current
    }

    /// NaN, like every other non-counting aggregator.
    ///
    /// `last` is the one aggregator whose EMPTY fill is not a constant — RTS reports the
    /// chronologically previous non-empty bucket's value for a gap, a gap meaning "no new
    /// reading, so the last reading still stands" (RedisTimeSeries 8.10; carry is timeline-
    /// based, independent of query direction). That carry cannot be done here: this
    /// aggregator is reset per bucket and has no visibility across buckets. `LastCarry`
    /// (src/aggregators/aggregate_iterator.rs) applies it across the chronological stream
    /// instead, before any reversal, so a reverse query still carries forward in time rather
    /// than from whichever bucket happens to be emitted first. Stored NaN is the one place the
    /// two directions differ; `LastCarry` records how.
    fn empty_value(&self) -> Value {
        f64::NAN
    }
}

impl RdbSerializable for LastAggregator {
    fn rdb_save(&self, rdb: *mut RedisModuleIO) {
        rdb_save_optional_f64(rdb, self.current);
    }
    fn rdb_load(rdb: *mut RedisModuleIO) -> ValkeyResult<Self>
    where
        Self: Sized,
    {
        rdb_load_optional_f64(rdb).map(|current| Self { current })
    }
}

impl Hash for LastAggregator {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        hash_f64(self.current.unwrap_or(f64::NAN), state);
    }
}

// -- Min -----------------------------------------------------------------
#[derive(Copy, Clone, Default, Debug, PartialEq, GetSize)]
pub struct MinAggregator(Option<Value>);
impl AggregationHandler for MinAggregator {
    fn update(&mut self, _timestamp: Timestamp, value: Value) -> bool {
        if !value.is_nan() {
            self.0 = Some(match self.0 {
                None => value,
                Some(v) => v.min(value),
            });
            true
        } else {
            false
        }
    }
    fn reset(&mut self) {
        self.0 = None;
    }
    fn current(&self) -> Option<Value> {
        self.0
    }
}

impl RdbSerializable for MinAggregator {
    fn rdb_save(&self, rdb: *mut RedisModuleIO) {
        rdb_save_optional_f64(rdb, self.0)
    }
    fn rdb_load(rdb: *mut RedisModuleIO) -> ValkeyResult<Self>
    where
        Self: Sized,
    {
        rdb_load_optional_f64(rdb).map(Self)
    }
}

impl Hash for MinAggregator {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        hash_f64(self.0.unwrap_or(f64::NAN), state);
    }
}

// -- Max ----------------------------------------------------------------

#[derive(Copy, Clone, Default, Debug, PartialEq, GetSize)]
pub struct MaxAggregator(Option<Value>);
impl AggregationHandler for MaxAggregator {
    fn update(&mut self, _timestamp: Timestamp, value: Value) -> bool {
        if value.is_nan() {
            return false;
        }
        self.0 = Some(match self.0 {
            None => value,
            Some(v) => v.max(value),
        });
        true
    }
    fn reset(&mut self) {
        self.0 = None;
    }
    fn current(&self) -> Option<Value> {
        self.0
    }
}

impl RdbSerializable for MaxAggregator {
    fn rdb_save(&self, rdb: *mut RedisModuleIO) {
        rdb_save_optional_f64(rdb, self.0);
    }

    fn rdb_load(rdb: *mut RedisModuleIO) -> ValkeyResult<Self>
    where
        Self: Sized,
    {
        let value = rdb_load_optional_f64(rdb)?;
        Ok(Self(value))
    }
}

impl Hash for MaxAggregator {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        hash_f64(self.0.unwrap_or(f64::NAN), state);
    }
}

// -- First ----------------------------------------------------------------

#[derive(Copy, Clone, Default, Debug, PartialEq, GetSize)]
pub struct RangeAggregatorState {
    min: Value,
    max: Value,
    _init: bool,
}

impl Hash for RangeAggregatorState {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        hash_f64(self.min, state);
        hash_f64(self.max, state);
        self._init.hash(state);
    }
}

impl RdbSerializable for RangeAggregatorState {
    fn rdb_save(&self, rdb: *mut RedisModuleIO) {
        rdb_save_bool(rdb, self._init);
        if self._init {
            raw::save_double(rdb, self.min);
            raw::save_double(rdb, self.max);
        }
    }

    fn rdb_load(rdb: *mut RedisModuleIO) -> ValkeyResult<Self>
    where
        Self: Sized,
    {
        let init = rdb_load_bool(rdb)?;
        if init {
            let min = raw::load_double(rdb)?;
            let max = raw::load_double(rdb)?;
            Ok(Self {
                min,
                max,
                _init: init,
            })
        } else {
            Ok(Self::default())
        }
    }
}

impl RangeAggregatorState {
    fn update(&mut self, _timestamp: Timestamp, value: Value) -> bool {
        if value.is_nan() {
            return false;
        }
        if !self._init {
            self._init = true;
            self.min = value;
            self.max = value;
        } else {
            self.max = self.max.max(value);
            self.min = self.min.min(value);
        }
        true
    }

    fn reset(&mut self) {
        self.min = 0.;
        self.max = 0.;
        self._init = false;
    }

    fn current(&self) -> Option<Value> {
        if !self._init {
            None
        } else {
            Some(self.max - self.min)
        }
    }
}

#[derive(Clone, Default, Debug, PartialEq, GetSize, Hash)]
pub struct RangeAggregator(Box<RangeAggregatorState>);
impl AggregationHandler for RangeAggregator {
    fn update(&mut self, _timestamp: Timestamp, value: Value) -> bool {
        self.0.update(_timestamp, value)
    }

    fn reset(&mut self) {
        self.0.reset();
    }

    fn current(&self) -> Option<Value> {
        self.0.current()
    }
}

impl RdbSerializable for RangeAggregator {
    fn rdb_save(&self, rdb: *mut RedisModuleIO) {
        self.0.rdb_save(rdb);
    }

    fn rdb_load(rdb: *mut RedisModuleIO) -> ValkeyResult<Self> {
        RangeAggregatorState::rdb_load(rdb).map(|state| Self(Box::new(state)))
    }
}

// -- Avg -----------------------------------------------------------------

#[derive(Copy, Clone, Default, Debug, PartialEq, GetSize, Hash)]
pub struct AvgAggregator(KahanAvg);

impl AggregationHandler for AvgAggregator {
    fn update(&mut self, _timestamp: Timestamp, value: Value) -> bool {
        if !value.is_nan() {
            self.0 += value;
            true
        } else {
            false
        }
    }
    fn reset(&mut self) {
        self.0.reset();
    }
    fn current(&self) -> Option<Value> {
        self.0.value()
    }
}

impl RdbSerializable for AvgAggregator {
    fn rdb_save(&self, rdb: *mut RedisModuleIO) {
        self.0.rdb_save(rdb);
    }

    fn rdb_load(rdb: *mut RedisModuleIO) -> ValkeyResult<Self>
    where
        Self: Sized,
    {
        let inner = KahanAvg::rdb_load(rdb)?;
        Ok(Self(inner))
    }
}

// -- Sum -----------------------------------------------------------------

/// Occupancy is tracked apart from the value: a sum that comes to zero is still a sum.
#[derive(Copy, Clone, Default, Debug, PartialEq, GetSize, Hash)]
pub struct SumAggregator {
    sum: KahanSum,
    has_value: bool,
}
impl AggregationHandler for SumAggregator {
    fn update(&mut self, _timestamp: Timestamp, value: Value) -> bool {
        if value.is_nan() {
            return false;
        }
        self.sum += value;
        self.has_value = true;
        true
    }
    fn reset(&mut self) {
        *self = Self::default();
    }
    fn current(&self) -> Option<Value> {
        self.has_value.then(|| self.sum.value())
    }
    fn empty_value(&self) -> Value {
        0.
    }
}

impl RdbSerializable for SumAggregator {
    fn rdb_save(&self, rdb: *mut RedisModuleIO) {
        self.sum.rdb_save(rdb);
        rdb_save_bool(rdb, self.has_value);
    }

    fn rdb_load(rdb: *mut RedisModuleIO) -> ValkeyResult<Self>
    where
        Self: Sized,
    {
        let sum = KahanSum::rdb_load(rdb)?;
        let has_value = rdb_load_bool(rdb)?;
        Ok(Self { sum, has_value })
    }
}

// -- Count -----------------------------------------------------------------

#[derive(Copy, Clone, Default, Debug, PartialEq, GetSize)]
pub struct CountAggregator(usize);
impl AggregationHandler for CountAggregator {
    fn update(&mut self, _timestamp: Timestamp, value: Value) -> bool {
        if !value.is_nan() {
            self.0 += 1;
            true
        } else {
            false
        }
    }
    fn reset(&mut self) {
        self.0 = 0;
    }
    fn current(&self) -> Option<Value> {
        if self.0 == 0 {
            return None;
        }
        Some(self.0 as Value)
    }
    fn empty_value(&self) -> Value {
        0.
    }
}

impl RdbSerializable for CountAggregator {
    fn rdb_save(&self, rdb: *mut RedisModuleIO) {
        rdb_save_usize(rdb, self.0);
    }

    fn rdb_load(rdb: *mut RedisModuleIO) -> ValkeyResult<Self>
    where
        Self: Sized,
    {
        rdb_load_usize(rdb).map(Self)
    }
}

impl Hash for CountAggregator {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.0.hash(state);
    }
}

// -- CountNAN ----------------------------------------------------------------

#[derive(Copy, Clone, Default, Debug, PartialEq, GetSize)]
pub struct CountNanAggregator(usize);

impl AggregationHandler for CountNanAggregator {
    fn update(&mut self, _timestamp: Timestamp, value: Value) -> bool {
        if value.is_nan() {
            self.0 += 1;
            true
        } else {
            false
        }
    }
    fn reset(&mut self) {
        self.0 = 0;
    }
    fn current(&self) -> Option<Value> {
        if self.0 == 0 {
            return None;
        }
        Some(self.0 as Value)
    }
    fn empty_value(&self) -> Value {
        0.
    }
}

impl RdbSerializable for CountNanAggregator {
    fn rdb_save(&self, rdb: *mut RedisModuleIO) {
        rdb_save_usize(rdb, self.0);
    }

    fn rdb_load(rdb: *mut RedisModuleIO) -> ValkeyResult<Self>
    where
        Self: Sized,
    {
        rdb_load_usize(rdb).map(Self)
    }
}

impl Hash for CountNanAggregator {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.0.hash(state);
    }
}

// -- CountAll ----------------------------------------------------------------

#[derive(Copy, Clone, Default, Debug, PartialEq, GetSize)]
pub struct CountAllAggregator(usize);

impl AggregationHandler for CountAllAggregator {
    fn update(&mut self, _timestamp: Timestamp, _value: Value) -> bool {
        self.0 += 1;
        true
    }
    fn reset(&mut self) {
        self.0 = 0;
    }
    fn current(&self) -> Option<Value> {
        if self.0 == 0 {
            return None;
        }
        Some(self.0 as Value)
    }
    fn empty_value(&self) -> Value {
        0.
    }
}

impl RdbSerializable for CountAllAggregator {
    fn rdb_save(&self, rdb: *mut RedisModuleIO) {
        rdb_save_usize(rdb, self.0);
    }

    fn rdb_load(rdb: *mut RedisModuleIO) -> ValkeyResult<Self>
    where
        Self: Sized,
    {
        rdb_load_usize(rdb).map(Self)
    }
}

impl Hash for CountAllAggregator {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.0.hash(state);
    }
}

/// Welford online accumulator for the std/var family: `mean` is the running
/// mean, `m2` the sum of squared deviations from it. Unlike the textbook
/// sum-of-squares form this cannot cancel catastrophically and `m2` is
/// nonnegative by construction. Mirrors the partial-reducer Std/Var state
/// (partial_reducer.rs), so push-down matches single-node results.
///
/// Values are accumulated relative to `shift`, the bucket's first value, so
/// `mean` is small next to the data and keeps the precision the data has: at
/// 1e16, where the ulp is 2, an absolute running mean of `[1e16, 1e16 + 2]`
/// cannot hold 1e16 + 1, and the rounding doubled M2.
#[derive(Copy, Clone, Default, Debug, PartialEq, GetSize)]
pub struct AggStd {
    /// Running mean of `value - shift`.
    mean: Value,
    m2: Value,
    count: usize,
    shift: Value,
}

impl AggStd {
    fn add(&mut self, value: Value) -> bool {
        if value.is_nan() {
            return false;
        }
        if self.count == 0 {
            self.shift = value;
        }
        self.count += 1;
        let x = value - self.shift;
        let delta = x - self.mean;
        self.mean += delta / self.count as Value;
        self.m2 += delta * (x - self.mean);
        true
    }
    fn reset(&mut self) {
        *self = Self::default();
    }
    /// The variance numerator (M2); callers divide by n or n - 1.
    fn variance(&self) -> Value {
        if self.count <= 1 { 0. } else { self.m2 }
    }
}

impl Display for AggStd {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "mean: {}, m2: {}, count: {}, shift: {}",
            self.mean, self.m2, self.count, self.shift
        )
    }
}

impl RdbSerializable for AggStd {
    fn rdb_save(&self, rdb: *mut RedisModuleIO) {
        raw::save_double(rdb, self.mean);
        raw::save_double(rdb, self.m2);
        rdb_save_usize(rdb, self.count);
        raw::save_double(rdb, self.shift);
    }

    fn rdb_load(rdb: *mut RedisModuleIO) -> ValkeyResult<Self>
    where
        Self: Sized,
    {
        let mean = raw::load_double(rdb)?;
        let m2 = raw::load_double(rdb)?;
        let count = rdb_load_usize(rdb)?;
        let shift = raw::load_double(rdb)?;
        Ok(Self {
            mean,
            m2,
            count,
            shift,
        })
    }
}

impl Hash for AggStd {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        hash_f64(self.mean, state);
        hash_f64(self.m2, state);
        self.count.hash(state);
        hash_f64(self.shift, state);
    }
}

// boxed to minimize size of stack Aggregator enum
pub(crate) type OnlineAggregator = Box<AggStd>;
impl RdbSerializable for OnlineAggregator {
    fn rdb_save(&self, rdb: *mut RedisModuleIO) {
        self.as_ref().rdb_save(rdb);
    }

    fn rdb_load(rdb: *mut RedisModuleIO) -> ValkeyResult<Self>
    where
        Self: Sized,
    {
        let inner = AggStd::rdb_load(rdb)?;
        Ok(Box::new(inner))
    }
}

// -- VarP -----------------------------------------------------------------

#[derive(Clone, Default, Debug, Hash, PartialEq, GetSize)]
pub struct VarPAggregator(OnlineAggregator);
impl AggregationHandler for VarPAggregator {
    fn update(&mut self, _timestamp: Timestamp, value: Value) -> bool {
        self.0.add(value)
    }
    fn reset(&mut self) {
        self.0.reset()
    }
    fn current(&self) -> Option<Value> {
        if self.0.count == 0 {
            None
        } else {
            Some(self.0.variance() / self.0.count as Value)
        }
    }
}

impl RdbSerializable for VarPAggregator {
    fn rdb_save(&self, rdb: *mut RedisModuleIO) {
        self.0.rdb_save(rdb);
    }

    fn rdb_load(rdb: *mut RedisModuleIO) -> ValkeyResult<Self>
    where
        Self: Sized,
    {
        OnlineAggregator::rdb_load(rdb).map(Self)
    }
}

// -- VarS -----------------------------------------------------------------

#[derive(Clone, Default, Debug, Hash, PartialEq, GetSize)]
pub struct VarSAggregator(OnlineAggregator);
impl AggregationHandler for VarSAggregator {
    fn update(&mut self, _timestamp: Timestamp, value: Value) -> bool {
        self.0.add(value)
    }
    fn reset(&mut self) {
        self.0.reset()
    }
    fn current(&self) -> Option<Value> {
        if self.0.count == 0 {
            None
        } else if self.0.count == 1 {
            Some(0.)
        } else {
            Some(self.0.variance() / (self.0.count - 1) as Value)
        }
    }
}

impl RdbSerializable for VarSAggregator {
    fn rdb_save(&self, rdb: *mut RedisModuleIO) {
        self.0.rdb_save(rdb);
    }

    fn rdb_load(rdb: *mut RedisModuleIO) -> ValkeyResult<Self>
    where
        Self: Sized,
    {
        OnlineAggregator::rdb_load(rdb).map(Self)
    }
}

// -- StdP -----------------------------------------------------------------

#[derive(Clone, Default, Debug, Hash, PartialEq, GetSize)]
pub struct StdPAggregator(OnlineAggregator);
impl AggregationHandler for StdPAggregator {
    fn update(&mut self, _timestamp: Timestamp, value: Value) -> bool {
        self.0.add(value)
    }
    fn reset(&mut self) {
        self.0.reset()
    }
    fn current(&self) -> Option<Value> {
        if self.0.count == 0 {
            None
        } else {
            Some((self.0.variance() / self.0.count as Value).sqrt())
        }
    }
}

impl RdbSerializable for StdPAggregator {
    fn rdb_save(&self, rdb: *mut RedisModuleIO) {
        self.0.rdb_save(rdb);
    }

    fn rdb_load(rdb: *mut RedisModuleIO) -> ValkeyResult<Self>
    where
        Self: Sized,
    {
        OnlineAggregator::rdb_load(rdb).map(Self)
    }
}

#[derive(Clone, Default, Debug, Hash, PartialEq, GetSize)]
pub struct StdSAggregator(OnlineAggregator);
impl AggregationHandler for StdSAggregator {
    fn update(&mut self, _timestamp: Timestamp, value: Value) -> bool {
        self.0.add(value)
    }
    fn reset(&mut self) {
        self.0.reset()
    }
    fn current(&self) -> Option<Value> {
        if self.0.count == 0 {
            None
        } else if self.0.count == 1 {
            Some(0.)
        } else {
            Some((self.0.variance() / (self.0.count - 1) as Value).sqrt())
        }
    }
}

impl RdbSerializable for StdSAggregator {
    fn rdb_save(&self, rdb: *mut RedisModuleIO) {
        self.0.rdb_save(rdb);
    }

    fn rdb_load(rdb: *mut RedisModuleIO) -> ValkeyResult<Self>
    where
        Self: Sized,
    {
        OnlineAggregator::rdb_load(rdb).map(Self)
    }
}

/// State for rate/increase calculations of monotonically increasing counters, with resets.
///
/// The last value is carried across bucket boundaries ([`AggregationHandler::reset`] keeps it),
/// so the delta between one bucket's last sample and the next bucket's first belongs to the
/// later bucket and the per-bucket increases add up to the whole increase. A bucket observes a
/// delta only once a sample arrives with a value before it; until then it has nothing to report.
#[derive(Debug, Default, Clone, PartialEq, GetSize)]
pub struct CounterAggregatorState {
    /// Accumulated deltas, compensated like every other sum in the module: a counter that
    /// resets often adds small deltas to a large total.
    sum_deltas: KahanSum,
    /// Last raw value seen, in this bucket or an earlier one.
    last_value: Option<f64>,
    /// Whether this bucket observed a delta: a sample arrived with `last_value` set.
    has_delta: bool,
}

impl Hash for CounterAggregatorState {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.sum_deltas.hash(state);
        hash_f64(self.last_value.unwrap_or(f64::NAN), state);
        self.has_delta.hash(state);
    }
}

impl RdbSerializable for CounterAggregatorState {
    fn rdb_save(&self, rdb: *mut RedisModuleIO) {
        rdb_save_optional_f64(rdb, self.last_value);
        self.sum_deltas.rdb_save(rdb);
        rdb_save_bool(rdb, self.has_delta);
    }

    fn rdb_load(rdb: *mut RedisModuleIO) -> ValkeyResult<Self>
    where
        Self: Sized,
    {
        let last_value = rdb_load_optional_f64(rdb)?;
        let sum_deltas = KahanSum::rdb_load(rdb)?;
        let has_delta = rdb_load_bool(rdb)?;
        Ok(CounterAggregatorState {
            sum_deltas,
            last_value,
            has_delta,
        })
    }
}

impl CounterAggregatorState {
    fn update(&mut self, _t: Timestamp, v: f64) -> bool {
        if v.is_nan() {
            return false;
        }
        if let Some(last) = self.last_value {
            self.sum_deltas += (v - last).max(0.0);
            self.has_delta = true;
        }
        self.last_value = Some(v);
        true
    }

    /// The deltas this bucket observed, if any.
    fn increase(&self) -> Option<f64> {
        self.has_delta.then(|| self.sum_deltas.value())
    }

    /// Ends the bucket, keeping the last value as the next bucket's baseline.
    fn end_bucket(&mut self) {
        self.sum_deltas.reset();
        self.has_delta = false;
    }

    fn clear(&mut self) {
        *self = Self::default();
    }
}

// -- Increase ---------------------------------------------------------------

#[derive(Debug, Default, Clone, Hash, PartialEq, GetSize)]
pub struct IncreaseAggregator(CounterAggregatorState);

impl AggregationHandler for IncreaseAggregator {
    fn update(&mut self, timestamp: Timestamp, value: Value) -> bool {
        self.0.update(timestamp, value)
    }
    fn reset(&mut self) {
        self.0.end_bucket();
    }
    fn clear(&mut self) {
        self.0.clear();
    }
    fn current(&self) -> Option<Value> {
        self.0.increase()
    }
}

impl RdbSerializable for IncreaseAggregator {
    fn rdb_save(&self, rdb: *mut RedisModuleIO) {
        self.0.rdb_save(rdb);
    }

    fn rdb_load(rdb: *mut RedisModuleIO) -> ValkeyResult<Self>
    where
        Self: Sized,
    {
        CounterAggregatorState::rdb_load(rdb).map(Self)
    }
}

#[derive(Debug, Default, Clone, Hash, PartialEq, GetSize)]
pub struct RateAggregatorState {
    /// The bucket length in milliseconds. Whole seconds truncated it: a 1500 ms bucket was
    /// divided by 1 s, and anything under a second by 0 (reported as no value).
    window_ms: u64,
    counter: CounterAggregatorState,
}

/// Rate of a monotonically increasing counter with resets, per second of bucket.
///
/// Each sample adds `max(v - last_v, 0)` to the bucket's delta sum (a drop is a reset and
/// contributes nothing), and the rate is that sum divided by the bucket length. The length must
/// be set — [`RateAggregator::new`] or [`RateAggregator::set_window_ms`] — before the first
/// update; `Default` leaves it 0, which can only report no value.
#[derive(Debug, Default, Clone, Hash, PartialEq, GetSize)]
pub struct RateAggregator(Box<RateAggregatorState>);

impl RateAggregator {
    /// Create a new online rate calculator with the given window length.
    pub fn new(window: Duration) -> Self {
        let counter = CounterAggregatorState::default();
        let state = Box::new(RateAggregatorState {
            window_ms: window.as_millis() as u64,
            counter,
        });
        Self(state)
    }

    pub fn set_window_ms(&mut self, window: u64) {
        self.0.window_ms = window;
    }
}

impl RdbSerializable for RateAggregator {
    fn rdb_save(&self, rdb: *mut RedisModuleIO) {
        // The format stores whole seconds. The field is informational only: a compaction rule
        // (the one place this is persisted) re-derives the window from its bucket duration on
        // load, which is the only way sub-second windows survive a reload.
        rdb_save_usize(rdb, (self.0.window_ms / 1000) as usize);
        self.0.counter.rdb_save(rdb);
    }

    fn rdb_load(rdb: *mut RedisModuleIO) -> ValkeyResult<Self>
    where
        Self: Sized,
    {
        let window_secs = rdb_load_usize(rdb)? as u64;
        let counter = CounterAggregatorState::rdb_load(rdb)?;
        let state = Box::new(RateAggregatorState {
            window_ms: window_secs.saturating_mul(1000),
            counter,
        });

        Ok(Self(state))
    }
}

impl AggregationHandler for RateAggregator {
    fn update(&mut self, timestamp: Timestamp, value: Value) -> bool {
        debug_assert!(
            self.0.window_ms > 0,
            "rate window must be set before the first update"
        );
        self.0.counter.update(timestamp, value)
    }
    fn reset(&mut self) {
        self.0.counter.end_bucket();
    }
    fn clear(&mut self) {
        self.0.counter.clear();
    }
    fn current(&self) -> Option<Value> {
        let state = &self.0;
        let increase = state.counter.increase()?;
        (state.window_ms > 0).then(|| increase / (state.window_ms as f64 / 1000.0))
    }
}

#[derive(Clone, Debug, Default, PartialEq, GetSize)]
struct IRateAggregatorState {
    dt: i64,
    dv: f64,
    last_sample: Option<Sample>,
}

impl Hash for IRateAggregatorState {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.dt.hash(state);
        hash_f64(self.dv, state);
        if let Some(sample) = &self.last_sample {
            sample.hash(state);
        } else {
            hash_f64(f64::NAN, state);
        }
    }
}

impl IRateAggregatorState {
    fn update_irate(&mut self, timestamp: Timestamp, value: Value) -> bool {
        if value.is_nan() {
            return false;
        }
        let current = Sample { timestamp, value };

        if let Some(prev) = self.last_sample {
            // Detect reset: counter dropped instead of increased.
            if current.value < prev.value {
                // Treat as reset: keep current as the new baseline, but no rate. The sample
                // still counts as taken, as it does for `increase` and `rate`.
                self.dv = 0.0;
                self.dt = 0;
                self.last_sample = Some(current);
            } else {
                self.dt = current.timestamp - prev.timestamp;
                if self.dt > 0 {
                    self.dv = current.value - prev.value;
                    self.last_sample = Some(current);
                }
            }
        } else {
            // First sample: just store it.
            self.last_sample = Some(current);
        }
        true
    }

    /// Ends the bucket, keeping the last sample as the next bucket's baseline.
    fn end_bucket(&mut self) {
        self.dt = 0;
        self.dv = 0.0;
    }

    fn clear(&mut self) {
        *self = Self::default();
    }
}

impl RdbSerializable for IRateAggregatorState {
    fn rdb_save(&self, rdb: *mut RedisModuleIO) {
        if let Some(x) = self.last_sample {
            rdb_save_optional_f64(rdb, Some(x.value));
            rdb_save_usize(rdb, x.timestamp as usize);
            raw::save_signed(rdb, self.dt);
            raw::save_double(rdb, self.dv);
            return;
        }
        rdb_save_optional_f64(rdb, None);
    }

    fn rdb_load(rdb: *mut RedisModuleIO) -> ValkeyResult<Self>
    where
        Self: Sized,
    {
        let Some(value) = rdb_load_optional_f64(rdb)? else {
            return Ok(Self {
                last_sample: None,
                dt: 0,
                dv: 0.0,
            });
        };

        let timestamp = rdb_load_usize(rdb)? as i64;
        let last_sample = Some(Sample { timestamp, value });
        let dt = raw::load_signed(rdb)?;
        let dv = raw::load_double(rdb)?;

        Ok(Self {
            last_sample,
            dt,
            dv,
        })
    }
}

// -- IRate -----------------------------------------------------------------

#[derive(Clone, Debug, Default, GetSize, Hash, PartialEq)]
pub struct IRateAggregator(Box<IRateAggregatorState>);

impl RdbSerializable for IRateAggregator {
    fn rdb_save(&self, rdb: *mut RedisModuleIO) {
        self.0.rdb_save(rdb);
    }

    fn rdb_load(rdb: *mut RedisModuleIO) -> ValkeyResult<Self>
    where
        Self: Sized,
    {
        let inner = IRateAggregatorState::rdb_load(rdb)?;
        Ok(Self(Box::new(inner)))
    }
}

impl AggregationHandler for IRateAggregator {
    fn update(&mut self, timestamp: Timestamp, value: Value) -> bool {
        self.0.update_irate(timestamp, value)
    }

    fn reset(&mut self) {
        self.0.end_bucket();
    }

    fn clear(&mut self) {
        self.0.clear();
    }

    fn current(&self) -> Option<Value> {
        if self.0.last_sample.is_none() || self.0.dt == 0 {
            return None;
        }
        let rate = self.0.dv / (self.0.dt as f64 / 1e3);
        Some(rate)
    }
}

#[enum_dispatch(AggregationHandler)]
#[derive(Clone, Debug, Hash, PartialEq, GetSize)]
pub enum Aggregator {
    All(AllAggregator),
    Any(AnyAggregator),
    Avg(AvgAggregator),
    Count(CountAggregator),
    CountAll(CountAllAggregator),
    CountIf(CountIfAggregator),
    CountNan(CountNanAggregator),
    First(FirstAggregator),
    Increase(IncreaseAggregator),
    IRate(IRateAggregator),
    Last(LastAggregator),
    Max(MaxAggregator),
    Min(MinAggregator),
    None(NoneAggregator),
    Range(RangeAggregator),
    Rate(RateAggregator),
    Share(ShareAggregator),
    StdP(StdPAggregator),
    StdS(StdSAggregator),
    Sum(SumAggregator),
    SumIf(SumIfAggregator),
    VarP(VarPAggregator),
    VarS(VarSAggregator),
}

impl Default for Aggregator {
    fn default() -> Self {
        AggregationType::Avg.into()
    }
}

impl From<AggregationType> for Aggregator {
    fn from(agg: AggregationType) -> Self {
        match agg {
            AggregationType::All => Aggregator::All(AllAggregator::default()),
            AggregationType::Any => Aggregator::Any(AnyAggregator::default()),
            AggregationType::Avg => Aggregator::Avg(AvgAggregator::default()),
            AggregationType::Count => Aggregator::Count(CountAggregator::default()),
            AggregationType::CountAll => Aggregator::CountAll(CountAllAggregator::default()),
            AggregationType::CountIf => Aggregator::CountIf(CountIfAggregator::default()),
            AggregationType::CountNan => Aggregator::CountNan(CountNanAggregator::default()),
            AggregationType::First => Aggregator::First(FirstAggregator::default()),
            AggregationType::Increase => Aggregator::Increase(IncreaseAggregator::default()),
            AggregationType::IRate => Aggregator::IRate(IRateAggregator::default()),
            AggregationType::Last => Aggregator::Last(LastAggregator::default()),
            AggregationType::Max => Aggregator::Max(MaxAggregator::default()),
            AggregationType::Min => Aggregator::Min(MinAggregator::default()),
            AggregationType::None => Aggregator::None(NoneAggregator::default()),
            AggregationType::Range => Aggregator::Range(RangeAggregator::default()),
            AggregationType::Rate => Aggregator::Rate(RateAggregator::default()),
            AggregationType::Share => Aggregator::Share(ShareAggregator::default()),
            AggregationType::StdP => Aggregator::StdP(StdPAggregator::default()),
            AggregationType::StdS => Aggregator::StdS(StdSAggregator::default()),
            AggregationType::VarP => Aggregator::VarP(VarPAggregator::default()),
            AggregationType::VarS => Aggregator::VarS(VarSAggregator::default()),
            AggregationType::Sum => Aggregator::Sum(SumAggregator::default()),
            AggregationType::SumIf => Aggregator::SumIf(SumIfAggregator::default()),
        }
    }
}

impl RdbSerializable for Aggregator {
    fn rdb_save(&self, rdb: *mut RedisModuleIO) {
        let agg_type = self.aggregation_type() as u8;
        rdb_save_u8(rdb, agg_type);

        match self {
            Aggregator::All(agg) => agg.rdb_save(rdb),
            Aggregator::Any(agg) => agg.rdb_save(rdb),
            Aggregator::Avg(agg) => agg.rdb_save(rdb),
            Aggregator::Count(agg) => agg.rdb_save(rdb),
            Aggregator::CountAll(agg) => agg.rdb_save(rdb),
            Aggregator::CountIf(agg) => agg.rdb_save(rdb),
            Aggregator::CountNan(agg) => agg.rdb_save(rdb),
            Aggregator::First(agg) => agg.rdb_save(rdb),
            Aggregator::Increase(agg) => agg.rdb_save(rdb),
            Aggregator::IRate(agg) => agg.rdb_save(rdb),
            Aggregator::Last(agg) => agg.rdb_save(rdb),
            Aggregator::Max(agg) => agg.rdb_save(rdb),
            Aggregator::Min(agg) => agg.rdb_save(rdb),
            Aggregator::None(agg) => agg.rdb_save(rdb),
            Aggregator::Rate(agg) => agg.rdb_save(rdb),
            Aggregator::Range(agg) => agg.rdb_save(rdb),
            Aggregator::Share(agg) => agg.rdb_save(rdb),
            Aggregator::Sum(agg) => agg.rdb_save(rdb),
            Aggregator::SumIf(agg) => agg.rdb_save(rdb),
            Aggregator::StdP(agg) => agg.rdb_save(rdb),
            Aggregator::StdS(agg) => agg.rdb_save(rdb),
            Aggregator::VarP(agg) => agg.rdb_save(rdb),
            Aggregator::VarS(agg) => agg.rdb_save(rdb),
        }
    }

    fn rdb_load(rdb: *mut RedisModuleIO) -> ValkeyResult<Self>
    where
        Self: Sized,
    {
        let agg_type_u8 = rdb_load_u8(rdb)?;
        let agg_type = AggregationType::try_from(agg_type_u8)?;

        match agg_type {
            AggregationType::All => AllAggregator::rdb_load(rdb).map(Aggregator::All),
            AggregationType::Any => AnyAggregator::rdb_load(rdb).map(Aggregator::Any),
            AggregationType::Avg => AvgAggregator::rdb_load(rdb).map(Aggregator::Avg),
            AggregationType::Count => CountAggregator::rdb_load(rdb).map(Aggregator::Count),
            AggregationType::CountAll => {
                CountAllAggregator::rdb_load(rdb).map(Aggregator::CountAll)
            }
            AggregationType::CountIf => CountIfAggregator::rdb_load(rdb).map(Aggregator::CountIf),
            AggregationType::CountNan => {
                CountNanAggregator::rdb_load(rdb).map(Aggregator::CountNan)
            }
            AggregationType::First => FirstAggregator::rdb_load(rdb).map(Aggregator::First),
            AggregationType::Increase => {
                IncreaseAggregator::rdb_load(rdb).map(Aggregator::Increase)
            }
            AggregationType::IRate => IRateAggregator::rdb_load(rdb).map(Aggregator::IRate),
            AggregationType::Last => LastAggregator::rdb_load(rdb).map(Aggregator::Last),
            AggregationType::Max => MaxAggregator::rdb_load(rdb).map(Aggregator::Max),
            AggregationType::Min => MinAggregator::rdb_load(rdb).map(Aggregator::Min),
            AggregationType::None => NoneAggregator::rdb_load(rdb).map(Aggregator::None),
            AggregationType::Range => RangeAggregator::rdb_load(rdb).map(Aggregator::Range),
            AggregationType::Rate => RateAggregator::rdb_load(rdb).map(Aggregator::Rate),
            AggregationType::Share => ShareAggregator::rdb_load(rdb).map(Aggregator::Share),
            AggregationType::StdP => StdPAggregator::rdb_load(rdb).map(Aggregator::StdP),
            AggregationType::StdS => StdSAggregator::rdb_load(rdb).map(Aggregator::StdS),
            AggregationType::VarP => VarPAggregator::rdb_load(rdb).map(Aggregator::VarP),
            AggregationType::VarS => VarSAggregator::rdb_load(rdb).map(Aggregator::VarS),
            AggregationType::Sum => SumAggregator::rdb_load(rdb).map(Aggregator::Sum),
            AggregationType::SumIf => SumIfAggregator::rdb_load(rdb).map(Aggregator::SumIf),
        }
    }
}

impl Aggregator {
    /// See [`AggregationType::carries_baseline`].
    pub fn carries_baseline(&self) -> bool {
        self.aggregation_type().carries_baseline()
    }

    pub fn aggregation_type(&self) -> AggregationType {
        match self {
            Aggregator::All(_) => AggregationType::All,
            Aggregator::Any(_) => AggregationType::Any,
            Aggregator::Avg(_) => AggregationType::Avg,
            Aggregator::Count(_) => AggregationType::Count,
            Aggregator::CountAll(_) => AggregationType::CountAll,
            Aggregator::CountIf(_) => AggregationType::CountIf,
            Aggregator::CountNan(_) => AggregationType::CountNan,
            Aggregator::First(_) => AggregationType::First,
            Aggregator::Increase(_) => AggregationType::Increase,
            Aggregator::IRate(_) => AggregationType::IRate,
            Aggregator::Last(_) => AggregationType::Last,
            Aggregator::Max(_) => AggregationType::Max,
            Aggregator::Min(_) => AggregationType::Min,
            Aggregator::None(_) => AggregationType::None,
            Aggregator::Range(_) => AggregationType::Range,
            Aggregator::Rate(_) => AggregationType::Rate,
            Aggregator::Share(_) => AggregationType::Share,
            Aggregator::StdP(_) => AggregationType::StdP,
            Aggregator::StdS(_) => AggregationType::StdS,
            Aggregator::VarP(_) => AggregationType::VarP,
            Aggregator::VarS(_) => AggregationType::VarS,
            Aggregator::Sum(_) => AggregationType::Sum,
            Aggregator::SumIf(_) => AggregationType::SumIf,
        }
    }

    /// The condition a filtered aggregator (`countif>5`, …) applies; `None` for the rest.
    pub fn condition(&self) -> Option<ValueComparisonFilter> {
        match self {
            Aggregator::All(a) => Some(a.condition()),
            Aggregator::Any(a) => Some(a.condition()),
            Aggregator::CountIf(a) => Some(a.condition()),
            Aggregator::None(a) => Some(a.condition()),
            Aggregator::Share(a) => Some(a.condition()),
            Aggregator::SumIf(a) => Some(a.condition()),
            _ => None,
        }
    }

    /// Value for a group this aggregator accepted no member of — every value was NaN, or
    /// every value failed the aggregator's condition.
    ///
    /// This is deliberately *not* `empty_value()`, which answers the different question of
    /// what an empty *bucket* holds. The two disagree on `sum`: an empty bucket sums to 0,
    /// but a group of nothing-but-NaN sums to NaN. See
    /// [`AggregationType::reduces_empty_to_zero`].
    pub fn empty_group_value(&self) -> Value {
        if self.aggregation_type().reduces_empty_to_zero() {
            0.0
        } else {
            f64::NAN
        }
    }
}
