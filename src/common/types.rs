use crate::common::hash::hash_f64;
use get_size2::GetSize;
use smallvec::SmallVec;
use std::cmp::Ordering;
use std::fmt::Display;
use std::hash::{Hash, Hasher};
use valkey_module::ValkeyValue;

pub type Timestamp = i64;
pub type SampleValue = f64;

#[derive(Debug, Default, Clone, Copy, GetSize)]
pub struct Sample {
    pub timestamp: Timestamp,
    pub value: SampleValue,
}

impl Sample {
    pub fn new(timestamp: Timestamp, value: SampleValue) -> Self {
        Sample { timestamp, value }
    }
}

pub const SAMPLE_SIZE: usize = size_of::<Sample>();
impl PartialEq for Sample {
    #[inline]
    fn eq(&self, other: &Sample) -> bool {
        // Two data points are equal if their times are equal, and their values are either equal or are NaN.
        if self.timestamp == other.timestamp {
            return if self.value.is_nan() {
                other.value.is_nan()
            } else {
                self.value == other.value
            };
        }
        false
    }
}

impl Eq for Sample {}

impl Ord for Sample {
    fn cmp(&self, other: &Self) -> Ordering {
        let cmp = self.timestamp.cmp(&other.timestamp);
        if cmp == Ordering::Equal {
            if self.value.is_nan() {
                if other.value.is_nan() {
                    Ordering::Equal
                } else {
                    Ordering::Greater
                }
            } else if other.value.is_nan() {
                Ordering::Less
            } else {
                self.value.partial_cmp(&other.value).unwrap()
            }
        } else {
            cmp
        }
    }
}

impl PartialOrd for Sample {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Hash for Sample {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.timestamp.hash(state);
        // Must agree with `eq`: `-0.0 == 0.0`, so both hash as `0.0`. Normalized here rather
        // than in `hash_f64`, whose digest callers keep the two apart. `hash_f64` already maps
        // every NaN payload to one value, matching `eq`'s NaN == NaN.
        let value = if self.value == 0.0 { 0.0 } else { self.value };
        hash_f64(value, state);
    }
}

impl Display for Sample {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} @ {}", self.value, self.timestamp)
    }
}

impl From<Sample> for ValkeyValue {
    fn from(sample: Sample) -> Self {
        (&sample).into()
    }
}

impl From<&Sample> for ValkeyValue {
    fn from(sample: &Sample) -> Self {
        let row = vec![
            ValkeyValue::from(sample.timestamp),
            ValkeyValue::from(sample.value),
        ];
        ValkeyValue::from(row)
    }
}

/// One output bucket of a multi-aggregation query: `values[i]` corresponds to
/// `AggregationOptions.aggregations[i]`.
#[derive(Debug, Clone, Default)]
pub struct MultiSample {
    pub timestamp: Timestamp,
    pub values: SmallVec<[SampleValue; 4]>,
}

impl MultiSample {
    pub fn new(timestamp: Timestamp, values: SmallVec<[SampleValue; 4]>) -> Self {
        Self { timestamp, values }
    }
}

impl From<MultiSample> for Sample {
    /// The row of a single-column aggregation.
    fn from(row: MultiSample) -> Self {
        debug_assert_eq!(row.values.len(), 1);
        Sample::new(row.timestamp, row.values[0])
    }
}

impl PartialEq for MultiSample {
    fn eq(&self, other: &Self) -> bool {
        // Mirrors Sample equality: values are equal if both are NaN or numerically equal.
        self.timestamp == other.timestamp
            && self.values.len() == other.values.len()
            && self
                .values
                .iter()
                .zip(other.values.iter())
                .all(|(a, b)| if a.is_nan() { b.is_nan() } else { a == b })
    }
}

impl Eq for MultiSample {}

impl Display for MultiSample {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?} @ {}", self.values.as_slice(), self.timestamp)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub(crate) enum SortDir {
    #[default]
    Asc,
    Desc,
}

impl Display for SortDir {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Asc => write!(f, "asc"),
            Self::Desc => write!(f, "desc"),
        }
    }
}

impl TryFrom<&[u8]> for SortDir {
    type Error = String;

    fn try_from(value: &[u8]) -> Result<Self, Self::Error> {
        match value.len() {
            3 if value.eq_ignore_ascii_case(b"asc") => Ok(Self::Asc),
            4 if value.eq_ignore_ascii_case(b"desc") => Ok(Self::Desc),
            _ => Err(format!(
                "invalid sort direction: {}",
                String::from_utf8_lossy(value)
            )),
        }
    }
}

impl TryFrom<&str> for SortDir {
    type Error = String;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        Self::try_from(value.as_bytes())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::hash::{BuildHasher, BuildHasherDefault, DefaultHasher};

    fn hash_of(sample: Sample) -> u64 {
        BuildHasherDefault::<DefaultHasher>::default().hash_one(sample)
    }

    #[test]
    fn equal_samples_hash_alike() {
        let pos = Sample::new(10, 0.0);
        let neg = Sample::new(10, -0.0);
        assert_eq!(pos, neg);
        assert_eq!(hash_of(pos), hash_of(neg));

        let quiet = Sample::new(10, f64::NAN);
        let payload = Sample::new(10, f64::from_bits(0x7ff0_0000_0000_0001));
        let negative = Sample::new(10, f64::from_bits(0xfff8_0000_0000_dead));
        assert!(payload.value.is_nan() && negative.value.is_nan());
        assert_eq!(quiet, payload);
        assert_eq!(quiet, negative);
        assert_eq!(hash_of(quiet), hash_of(payload));
        assert_eq!(hash_of(quiet), hash_of(negative));

        assert_ne!(hash_of(pos), hash_of(Sample::new(11, 0.0)));
        assert_ne!(hash_of(pos), hash_of(Sample::new(10, 1.0)));
    }
}
