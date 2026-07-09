use crate::common::rdb::rdb_load_string;
use crate::common::rounding::RoundingStrategy;
use crate::common::{Sample, Timestamp};
use crate::config::{
    CHUNK_SIZE_DEFAULT, chunk_encoding, chunk_size_bytes, duplicate_policy, ignore_max_time_diff,
    ignore_max_value_diff, retention_period, rounding_strategy,
};
use crate::error::{TsdbError, TsdbResult};
use crate::error_consts;
use crate::labels::Label;
use crate::series::SeriesRef;
use crate::series::chunks::ChunkEncoding;
use get_size2::GetSize;
use std::fmt::Display;
use std::hash::Hash;
use std::str::FromStr;
use std::time::Duration;
use valkey_module::{ValkeyError, ValkeyResult, ValkeyValue, raw};

#[derive(Debug, Default, PartialEq, Clone, Copy, GetSize, Hash)]
/// The policy to use when a duplicate sample is encountered
///
/// The discriminants let the policy be held in an atomic (see `config::DUPLICATE_POLICY`).
/// They are an in-memory detail only: RDB and the fanout protocol both carry the *name*, so
/// the numbering can change without affecting persistence or the wire format.
#[repr(u8)]
pub enum DuplicatePolicy {
    /// Block the sample and return an error
    #[default]
    Block = 0,
    /// Keep the first sample
    KeepFirst = 1,
    /// Keep the last (current) sample
    KeepLast = 2,
    /// Keep the minimum value of the current and old sample
    Min = 3,
    /// Keep the maximum value of the current and old sample
    Max = 4,
    /// Sum the current and old sample
    Sum = 5,
}

impl Display for DuplicatePolicy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

impl DuplicatePolicy {
    pub const fn as_str(&self) -> &'static str {
        match self {
            DuplicatePolicy::Block => "block",
            DuplicatePolicy::KeepFirst => "first",
            DuplicatePolicy::KeepLast => "last",
            DuplicatePolicy::Min => "min",
            DuplicatePolicy::Max => "max",
            DuplicatePolicy::Sum => "sum",
        }
    }

    /// The discriminant, for storing the policy in an atomic.
    pub const fn as_u8(self) -> u8 {
        self as u8
    }

    /// Inverse of [`Self::as_u8`]; `None` for a value that is not a discriminant.
    pub const fn from_u8(value: u8) -> Option<Self> {
        match value {
            0 => Some(DuplicatePolicy::Block),
            1 => Some(DuplicatePolicy::KeepFirst),
            2 => Some(DuplicatePolicy::KeepLast),
            3 => Some(DuplicatePolicy::Min),
            4 => Some(DuplicatePolicy::Max),
            5 => Some(DuplicatePolicy::Sum),
            _ => None,
        }
    }

    /// Handles duplicate values for a given timestamp based on the `DuplicatePolicy`
    /// defined for the current instance.
    ///
    /// # Parameters
    /// - `self`: The current `DuplicatePolicy` instance.
    /// - `ts`: The `Timestamp` of the duplicate sample.
    /// - `old`: The previously stored value.
    /// - `new`: The newly encountered value.
    ///
    /// # Returns
    /// - `TsdbResult<f64>`: A result containing the resolved value as per the duplicate
    ///   policy, or an error if the policy is `Block` and a duplicate value is encountered.
    ///
    /// # Behavior
    /// The behavior of the method is determined by the policy represented by `self`:
    ///
    /// - **`Block`**:
    ///   - Always returns an error of type `TsdbError::DuplicateSample` with information about
    ///     the conflicting value and timestamp.
    ///
    /// - **`KeepFirst`**:
    ///   - Retains and returns the `old` value.
    ///
    /// - **`KeepLast`**:
    ///   - Replaces the `old` value with the `new` value and returns `new`.
    ///
    /// - **`Min`**:
    ///   - Returns the smaller of the `old` and `new` values (`old.min(new)`).
    ///
    /// - **`Max`**:
    ///   - Returns the larger of the `old` and `new` values (`old.max(new)`).
    ///
    /// - **`Sum`**:
    ///   - Returns the sum of `old` and `new` values (`old + new`).
    ///
    /// # Special Cases
    /// - If either `old` or `new` is NaN (Not-a-Number):
    ///   - If the policy is not `Block`, the method returns the non-NaN value
    ///     (`new` if `old` is NaN or `old` if `new` is NaN).
    ///   - If both `old` and `new` are NaN, it returns `old`.
    ///
    /// # Errors
    /// - Returns `TsdbError::DuplicateSample` if the `DuplicatePolicy` is `Block`
    ///   and a duplicate value is encountered.
    ///
    /// # Example
    /// ```ignore
    /// use crate::series::DuplicatePolicy;
    /// let duplicate_policy = DuplicatePolicy::KeepLast;
    /// let ts: i64 = 0;
    /// let result = duplicate_policy.duplicate_value(ts, 42.0, 43.0);
    /// match result {
    ///     Ok(value) => println!("Resolved value: {}", value),
    ///     Err(err) => eprintln!("Error: {}", err),
    /// }
    /// ```
    pub fn duplicate_value(self, _ts: Timestamp, old: f64, new: f64) -> TsdbResult<f64> {
        use DuplicatePolicy::*;
        let old_nan = old.is_nan();
        let new_nan = new.is_nan();

        fn raise_error(err: &str) -> TsdbResult<f64> {
            Err(TsdbError::DuplicateSample(err.to_string()))
        }

        fn raise_block_error() -> TsdbResult<f64> {
            raise_error(
                "TSDB: Error at upsert, update is not supported when DUPLICATE_POLICY is set to BLOCK mode",
            )
        }

        if old_nan || new_nan {
            return if self == Block {
                raise_block_error()
            } else if old_nan != new_nan && matches!(self, Max | Min | Sum) {
                raise_error(
                    "TSDB: Error at upsert, NaN values are not supported for MAX, MIN, and SUM duplicate policies",
                )
            } else if new_nan {
                Ok(old)
            } else {
                Ok(new)
            };
        }

        match self {
            Block => raise_block_error(),
            KeepFirst => Ok(old),
            KeepLast => Ok(new),
            Min => Ok(old.min(new)),
            Max => Ok(old.max(new)),
            Sum => Ok(old + new),
        }
    }
}

fn get_policy_from_bytes(bytes: &[u8]) -> Option<DuplicatePolicy> {
    use DuplicatePolicy::*;
    hashify::tiny_map_ignore_case! {
        bytes,
        "block" => Block,
        "first"  => KeepFirst,
        "last"   => KeepLast,
        "min"    => Min,
        "max"    => Max,
        "sum"    => Sum,
    }
}

impl FromStr for DuplicatePolicy {
    type Err = ValkeyError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if let Some(policy) = get_policy_from_bytes(s.as_bytes()) {
            Ok(policy)
        } else {
            Err(ValkeyError::Str(error_consts::INVALID_DUPLICATE_POLICY))
        }
    }
}

impl TryFrom<&[u8]> for DuplicatePolicy {
    type Error = ValkeyError;
    fn try_from(bytes: &[u8]) -> Result<Self, Self::Error> {
        get_policy_from_bytes(bytes).ok_or(ValkeyError::Str(error_consts::INVALID_DUPLICATE_POLICY))
    }
}

impl TryFrom<&str> for DuplicatePolicy {
    type Error = ValkeyError;
    fn try_from(s: &str) -> Result<Self, Self::Error> {
        DuplicatePolicy::from_str(s)
    }
}

impl TryFrom<String> for DuplicatePolicy {
    type Error = ValkeyError;
    fn try_from(s: String) -> Result<Self, Self::Error> {
        DuplicatePolicy::from_str(&s)
    }
}

/// A struct that defines the policy for determining and handling duplicate samples in a dataset.
#[derive(Copy, Clone, Default, Debug, GetSize, PartialEq)]
pub struct SampleDuplicatePolicy {
    pub policy: Option<DuplicatePolicy>,
    /// The maximum difference between the new and existing timestamp to consider them duplicates
    pub max_time_delta: u64,
    /// The maximum difference between the new and existing value to consider them duplicates
    pub max_value_delta: f64,
}

impl Hash for SampleDuplicatePolicy {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.policy.hash(state);
        self.max_time_delta.hash(state);
        self.max_value_delta.to_bits().hash(state);
    }
}

impl SampleDuplicatePolicy {
    pub fn is_duplicate(
        &self,
        current_sample: &Sample,
        last_sample: &Sample,
        override_policy: Option<DuplicatePolicy>,
    ) -> bool {
        let time_delta = current_sample.timestamp - last_sample.timestamp;
        if time_delta >= 0 && time_delta <= self.max_time_delta as i64 {
            if self.resolve_policy(override_policy) != DuplicatePolicy::KeepLast {
                return false;
            }
            // NaN values are never considered duplicates
            if current_sample.value.is_nan() || last_sample.value.is_nan() {
                return false;
            }
            return (last_sample.value - current_sample.value).abs() <= self.max_value_delta;
        }

        false
    }

    pub fn resolve_policy(&self, override_policy: Option<DuplicatePolicy>) -> DuplicatePolicy {
        override_policy
            .or(self.policy)
            .unwrap_or_else(duplicate_policy)
    }

    pub(crate) fn rdb_save(&self, rdb: *mut raw::RedisModuleIO) {
        if let Some(policy) = self.policy {
            raw::save_string(rdb, policy.as_str());
        } else {
            raw::save_string(rdb, "-");
        }
        raw::save_unsigned(rdb, self.max_time_delta);
        raw::save_double(rdb, self.max_value_delta);
    }

    pub(crate) fn rdb_load(rdb: *mut raw::RedisModuleIO) -> ValkeyResult<SampleDuplicatePolicy> {
        let policy = rdb_load_string(rdb)?;
        let max_time_delta = raw::load_unsigned(rdb)?;
        let max_value_delta = raw::load_double(rdb)?;
        let duplicate_policy = if policy == "-" {
            None
        } else {
            Some(DuplicatePolicy::from_str(&policy)?)
        };
        Ok(SampleDuplicatePolicy {
            policy: duplicate_policy,
            max_time_delta,
            max_value_delta,
        })
    }
}

#[derive(Copy, Clone, Debug, Default, PartialEq, Eq)]
pub enum SampleAddResult {
    Ok(Sample),
    #[default]
    Duplicate,
    Ignored(Timestamp),
    TooOld,
    Error(&'static str),
}

impl SampleAddResult {
    pub fn is_ok(&self) -> bool {
        matches!(self, SampleAddResult::Ok(_))
    }
}

impl Display for SampleAddResult {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SampleAddResult::Ok(sample) => write!(f, "Added {sample}"),
            SampleAddResult::Duplicate => write!(f, "{}", error_consts::DUPLICATE_SAMPLE),
            SampleAddResult::Ignored(ts) => write!(f, "Ignored. Using ts: {ts}"),
            SampleAddResult::TooOld => write!(f, "{}", error_consts::SAMPLE_TOO_OLD),
            SampleAddResult::Error(e) => write!(f, "{e}"),
        }
    }
}

impl From<SampleAddResult> for ValkeyValue {
    fn from(res: SampleAddResult) -> Self {
        match res {
            SampleAddResult::Ok(ts) => ValkeyValue::Integer(ts.timestamp),
            SampleAddResult::Ignored(ts) => ValkeyValue::Integer(ts),
            SampleAddResult::Duplicate => {
                ValkeyValue::SimpleStringStatic(error_consts::DUPLICATE_SAMPLE)
            }
            SampleAddResult::TooOld => ValkeyValue::StaticError(error_consts::SAMPLE_TOO_OLD),
            SampleAddResult::Error(e) => ValkeyValue::StaticError(e),
        }
    }
}

impl From<SampleAddResult> for ValkeyResult {
    fn from(result: SampleAddResult) -> Self {
        match result {
            SampleAddResult::Ok(sample) => Ok(sample.into()),
            SampleAddResult::Ignored(ts) => Ok(ValkeyValue::Integer(ts)),
            SampleAddResult::Duplicate => Err(ValkeyError::Str(error_consts::DUPLICATE_SAMPLE)),
            SampleAddResult::TooOld => Err(ValkeyError::Str(error_consts::SAMPLE_TOO_OLD)),
            SampleAddResult::Error(e) => Err(ValkeyError::Str(e)),
        }
    }
}

/// Options for time series configuration
#[derive(Debug, Clone)]
pub struct TimeSeriesOptions {
    /// The source ID of the series, if this is a derived series
    pub src_id: Option<SeriesRef>,
    pub chunk_encoding: ChunkEncoding,
    pub chunk_size: Option<usize>,
    pub retention: Option<Duration>,
    pub sample_duplicate_policy: Option<SampleDuplicatePolicy>,
    pub labels: Option<Vec<Label>>,
    pub rounding: Option<RoundingStrategy>,
    pub on_duplicate: Option<DuplicatePolicy>,
}

impl TimeSeriesOptions {
    pub fn retention(&mut self, retention: Duration) {
        self.retention = Some(retention);
    }

    /// Builds options from the module-level configuration. Every value is read from a
    /// lock-free store, so this stays cheap on the create path of `TS.ADD`/`TS.MADD`.
    pub fn from_config() -> Self {
        let retention = retention_period();

        TimeSeriesOptions {
            retention: if retention.is_zero() {
                None
            } else {
                Some(retention)
            },
            chunk_encoding: chunk_encoding(),
            chunk_size: Some(chunk_size_bytes()),
            rounding: rounding_strategy(),
            sample_duplicate_policy: Some(SampleDuplicatePolicy {
                policy: Some(duplicate_policy()),
                max_time_delta: ignore_max_time_diff(),
                max_value_delta: ignore_max_value_diff(),
            }),
            ..Default::default()
        }
    }
}

impl Default for TimeSeriesOptions {
    fn default() -> Self {
        Self {
            src_id: None,
            chunk_encoding: ChunkEncoding::default(),
            chunk_size: Some(CHUNK_SIZE_DEFAULT as usize),
            retention: None,
            sample_duplicate_policy: None,
            labels: None,
            rounding: None,
            on_duplicate: None,
        }
    }
}

#[derive(Debug, PartialEq, Clone, Copy, Default)]
pub struct ValueFilter {
    pub min: f64,
    pub max: f64,
}

impl ValueFilter {
    pub(crate) fn new(min: f64, max: f64) -> ValkeyResult<Self> {
        if min > max {
            return Err(ValkeyError::Str("ERR invalid range"));
        }
        Ok(Self { min, max })
    }

    pub fn greater_than(value: f64) -> Self {
        Self {
            min: value,
            max: f64::MAX,
        }
    }

    pub fn less_than(value: f64) -> Self {
        Self {
            min: f64::MIN,
            max: value,
        }
    }

    pub fn is_match(&self, value: f64) -> bool {
        value >= self.min && value <= self.max
    }
}

/// Options for how to write to a destination time series when using the STORE option
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum DestinationWriteMode {
    /// Append the samples to the destination time series
    Merge,
    /// Overwrite the destination time series with the new samples
    #[default]
    Overwrite,
}

impl Display for DestinationWriteMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DestinationWriteMode::Merge => write!(f, "MERGE"),
            DestinationWriteMode::Overwrite => write!(f, "OVERWRITE"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::DuplicatePolicy;
    use crate::common::Sample;
    use crate::error::TsdbError;
    use crate::series::SampleDuplicatePolicy;
    use std::str::FromStr;

    #[test]
    fn test_duplicate_policy_parse() {
        assert!(matches!(
            DuplicatePolicy::from_str("block"),
            Ok(DuplicatePolicy::Block)
        ));
        assert!(matches!(
            DuplicatePolicy::from_str("last"),
            Ok(DuplicatePolicy::KeepLast)
        ));
        assert!(matches!(
            DuplicatePolicy::from_str("first"),
            Ok(DuplicatePolicy::KeepFirst)
        ));
        assert!(matches!(
            DuplicatePolicy::from_str("min"),
            Ok(DuplicatePolicy::Min)
        ));
        assert!(matches!(
            DuplicatePolicy::from_str("max"),
            Ok(DuplicatePolicy::Max)
        ));
        assert!(matches!(
            DuplicatePolicy::from_str("sum"),
            Ok(DuplicatePolicy::Sum)
        ));
    }

    #[test]
    fn test_duplicate_policy_handle_duplicate() {
        let dp = DuplicatePolicy::Block;
        let ts = 0;
        let old = 1.0;
        let new = 2.0;
        assert!(matches!(
            dp.duplicate_value(ts, old, new),
            Err(TsdbError::DuplicateSample(_))
        ));

        let dp = DuplicatePolicy::KeepFirst;
        let ts = 0;
        let old = 1.0;
        let new = 2.0;
        assert_eq!(dp.duplicate_value(ts, old, new).unwrap(), old);

        let dp = DuplicatePolicy::KeepLast;
        let ts = 0;
        let old = 1.0;
        let new = 2.0;
        assert_eq!(dp.duplicate_value(ts, old, new).unwrap(), new);

        let dp = DuplicatePolicy::Min;
        let ts = 0;
        let old = 1.0;
        let new = 2.0;
        assert_eq!(dp.duplicate_value(ts, old, new).unwrap(), old);

        let dp = DuplicatePolicy::Max;
        let ts = 0;
        let old = 1.0;
        let new = 2.0;
        assert_eq!(dp.duplicate_value(ts, old, new).unwrap(), new);

        let dp = DuplicatePolicy::Sum;
        let ts = 0;
        let old = 1.0;
        let new = 2.0;
        assert_eq!(dp.duplicate_value(ts, old, new).unwrap(), old + new);
    }

    #[test]
    fn test_duplicate_policy_handle_nan() {
        use DuplicatePolicy::*;

        let dp = Block;
        let ts = 0;
        let old = 1.0;
        let new = f64::NAN;
        assert!(matches!(
            dp.duplicate_value(ts, old, new),
            Err(TsdbError::DuplicateSample(_))
        ));

        let valid_policies = [KeepFirst, KeepLast];
        for policy in valid_policies {
            assert_eq!(policy.duplicate_value(ts, 10.0, f64::NAN).unwrap(), 10.0);
            assert_eq!(policy.duplicate_value(ts, f64::NAN, 8.0).unwrap(), 8.0);
        }

        // If one value is NaN and the other isn't, it should return an error
        // since NaN values are not supported for Min, Max, and Sum policies
        let invalid_policies = [Min, Max, Sum];
        for policy in invalid_policies {
            assert!(matches!(
                dp.duplicate_value(ts, f64::NAN, 8.0),
                Err(TsdbError::DuplicateSample(_))
            ));
            assert!(matches!(
                dp.duplicate_value(ts, 8.0, f64::NAN),
                Err(TsdbError::DuplicateSample(_))
            ));
            let actual = policy.duplicate_value(ts, f64::NAN, f64::NAN).unwrap();
            assert!(actual.is_nan());
        }
    }

    #[test]
    fn test_sample_duplicate_policy_is_duplicate_keep_last() {
        let policy = SampleDuplicatePolicy {
            policy: Some(DuplicatePolicy::KeepLast),
            max_time_delta: 10,
            max_value_delta: 0.001,
        };

        // Test time delta check - within a threshold
        let last_sample = Sample {
            timestamp: 100,
            value: 10.0,
        };
        let current_sample = Sample {
            timestamp: 105,
            value: 10.0,
        };
        assert!(policy.is_duplicate(&current_sample, &last_sample, None));

        // Test time delta check - outside threshold
        let current_sample = Sample {
            timestamp: 120,
            value: 100.0,
        };
        assert!(!policy.is_duplicate(&current_sample, &last_sample, None));

        // Test value delta check - within a threshold
        let current_sample = Sample {
            timestamp: 105,
            value: 10.0005,
        };
        assert!(policy.is_duplicate(&current_sample, &last_sample, None));

        // Test value delta check - outside threshold
        let current_sample = Sample {
            timestamp: 205,
            value: 10.1,
        };
        assert!(!policy.is_duplicate(&current_sample, &last_sample, None));

        // Test older timestamp - should return false regardless of deltas
        let current_sample = Sample {
            timestamp: 95,
            value: 10.0,
        };
        assert!(!policy.is_duplicate(&current_sample, &last_sample, None));
    }

    #[test]
    fn test_sample_duplicate_policy_is_duplicate_with_override_policy() {
        let policy = SampleDuplicatePolicy {
            policy: Some(DuplicatePolicy::Block),
            max_time_delta: 10,
            max_value_delta: 0.001,
        };

        let last_sample = Sample {
            timestamp: 100,
            value: 10.0,
        };
        let current_sample = Sample {
            timestamp: 105,
            value: 10.0,
        };

        // With the original Block policy - should not be considered duplicate
        assert!(!policy.is_duplicate(&current_sample, &last_sample, None));

        // With override to KeepLast - should be considered duplicate
        assert!(policy.is_duplicate(
            &current_sample,
            &last_sample,
            Some(DuplicatePolicy::KeepLast)
        ));
    }

    #[test]
    fn test_sample_duplicate_policy_is_duplicate_with_zero_deltas() {
        let policy = SampleDuplicatePolicy {
            policy: Some(DuplicatePolicy::KeepLast),
            max_time_delta: 0, // Zero time delta
            max_value_delta: 0.001,
        };

        let last_sample = Sample {
            timestamp: 100,
            value: 10.0,
        };
        let current_sample = Sample {
            timestamp: 105,
            value: 30.0,
        };

        // With a zero-time delta, should not detect as duplicate based on time
        assert!(!policy.is_duplicate(&current_sample, &last_sample, None));

        // But still should detect as duplicate based on value
        let policy = SampleDuplicatePolicy {
            policy: Some(DuplicatePolicy::KeepLast),
            max_time_delta: 10,
            max_value_delta: 0.0, // Zero value delta
        };

        // With the exact same values
        let current_sample = Sample {
            timestamp: 105,
            value: 10.0,
        };
        assert!(policy.is_duplicate(&current_sample, &last_sample, None));

        // With a slight value difference
        let current_sample = Sample {
            timestamp: 125,
            value: 10.00001,
        };
        assert!(!policy.is_duplicate(&current_sample, &last_sample, None));
    }

    #[test]
    fn test_sample_duplicate_policy_is_duplicate_nan_values() {
        let policy = SampleDuplicatePolicy {
            policy: Some(DuplicatePolicy::KeepLast),
            max_time_delta: 100,
            max_value_delta: f64::MAX, // permissive delta so only NaN check matters
        };

        let last_sample = Sample {
            timestamp: 100,
            value: 10.0,
        };

        // current value is NaN — never a duplicate
        let current_nan = Sample {
            timestamp: 105,
            value: f64::NAN,
        };
        assert!(!policy.is_duplicate(&current_nan, &last_sample, None));

        // last value is NaN — never a duplicate
        let last_nan = Sample {
            timestamp: 100,
            value: f64::NAN,
        };
        let current_sample = Sample {
            timestamp: 105,
            value: 10.0,
        };
        assert!(!policy.is_duplicate(&current_sample, &last_nan, None));

        // both NaN — never a duplicate
        assert!(!policy.is_duplicate(&current_nan, &last_nan, None));
    }
}
