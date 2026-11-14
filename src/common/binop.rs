use get_size2::GetSize;
use std::fmt::Display;
use valkey_module::ValkeyError;

/// eq returns true if left == right.
#[inline]
pub(crate) const fn op_eq(left: f64, right: f64) -> bool {
    // Special handling for nan == nan.
    if left.is_nan() {
        return right.is_nan();
    }
    left == right
}

/// neq returns true if left != right.
#[inline]
pub(crate) const fn op_neq(left: f64, right: f64) -> bool {
    // Special handling for comparison with nan.
    if left.is_nan() {
        return !right.is_nan();
    }
    if right.is_nan() {
        return true;
    }
    left != right
}

/// Gte returns true if left >= right
#[inline]
pub(crate) const fn op_gte(left: f64, right: f64) -> bool {
    left >= right
}

/// Lte returns true if left <= right
#[inline]
pub(crate) const fn op_lte(left: f64, right: f64) -> bool {
    left <= right
}

#[derive(Debug, PartialEq, Eq, Clone, Copy, Hash, GetSize)]
#[repr(u8)]
pub enum ComparisonOperator {
    Equal = 0,
    NotEqual = 1,
    GreaterThan = 2,
    GreaterThanOrEqual = 3,
    LessThan = 4,
    LessThanOrEqual = 5,
}

impl ComparisonOperator {
    pub fn compare(&self, a: f64, b: f64) -> bool {
        match self {
            ComparisonOperator::Equal => op_eq(a, b),
            ComparisonOperator::NotEqual => op_neq(a, b),
            ComparisonOperator::GreaterThan => a > b,
            ComparisonOperator::GreaterThanOrEqual => op_gte(a, b),
            ComparisonOperator::LessThan => a < b,
            ComparisonOperator::LessThanOrEqual => op_lte(a, b),
        }
    }
}

impl Display for ComparisonOperator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            ComparisonOperator::Equal => "==",
            ComparisonOperator::NotEqual => "!=",
            ComparisonOperator::GreaterThan => ">",
            ComparisonOperator::GreaterThanOrEqual => ">=",
            ComparisonOperator::LessThan => "<",
            ComparisonOperator::LessThanOrEqual => "<=",
        };
        write!(f, "{}", s)
    }
}

impl TryFrom<&str> for ComparisonOperator {
    type Error = ValkeyError;
    fn try_from(value: &str) -> Result<Self, Self::Error> {
        let condition = hashify::map_ignore_case!(
            value.as_bytes(),
            ComparisonOperator,
            "==" => ComparisonOperator::Equal,
            "!=" => ComparisonOperator::NotEqual,
            ">" => ComparisonOperator::GreaterThan,
            ">=" => ComparisonOperator::GreaterThanOrEqual,
            "<" => ComparisonOperator::LessThan,
            "<=" => ComparisonOperator::LessThanOrEqual,
        )
        .copied();
        match condition {
            Some(cond) => Ok(cond),
            None => Err(ValkeyError::Str("TSDB: invalid comparison operator")),
        }
    }
}

impl TryFrom<u8> for ComparisonOperator {
    type Error = ValkeyError;
    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(ComparisonOperator::Equal),
            1 => Ok(ComparisonOperator::NotEqual),
            2 => Ok(ComparisonOperator::GreaterThan),
            3 => Ok(ComparisonOperator::GreaterThanOrEqual),
            4 => Ok(ComparisonOperator::LessThan),
            5 => Ok(ComparisonOperator::LessThanOrEqual),
            _ => Err(ValkeyError::Str("TSDB: invalid comparison operator value")),
        }
    }
}

impl From<ComparisonOperator> for u8 {
    fn from(value: ComparisonOperator) -> Self {
        match value {
            ComparisonOperator::Equal => 0,
            ComparisonOperator::NotEqual => 1,
            ComparisonOperator::GreaterThan => 2,
            ComparisonOperator::GreaterThanOrEqual => 3,
            ComparisonOperator::LessThan => 4,
            ComparisonOperator::LessThanOrEqual => 5,
        }
    }
}
