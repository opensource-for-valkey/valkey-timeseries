use get_size2::GetSize;
use std::cmp::Ordering;
use std::fmt::Display;
use valkey_module::ValkeyError;

pub type BinopFunc = fn(left: f64, right: f64) -> f64;

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

#[inline]
pub(crate) const fn op_coalesce(left: f64, right: f64) -> f64 {
    if !left.is_nan() {
        return left;
    }
    right
}

/// gt returns true if left > right
#[inline]
pub(crate) const fn op_gt(left: f64, right: f64) -> bool {
    left > right
}

/// lt returns true if left < right
#[inline]
pub(crate) const fn op_lt(left: f64, right: f64) -> bool {
    left < right
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

/// Plus returns left + right
#[inline]
pub(crate) const fn op_plus(left: f64, right: f64) -> f64 {
    left + right
}

/// Minus returns left - right
#[inline]
pub(crate) const fn op_minus(left: f64, right: f64) -> f64 {
    left - right
}

/// Mul returns left * right
#[inline]
pub(crate) const fn op_mul(left: f64, right: f64) -> f64 {
    left * right
}

/// Div returns left / right. If right is 0, returns NaN.
#[inline]
pub(crate) const fn op_div(left: f64, right: f64) -> f64 {
    if right == 0.0 {
        return f64::NAN;
    }
    left / right
}

/// returns left % right
#[inline]
pub(crate) const fn op_mod(left: f64, right: f64) -> f64 {
    left % right
}

/// pow returns pow(left, right)
#[inline]
pub(crate) fn op_pow(left: f64, right: f64) -> f64 {
    left.powf(right)
}

/// convert true to x, false to NaN.
#[inline]
pub const fn to_comparison_value(b: bool, x: f64) -> f64 {
    if b { x } else { f64::NAN }
}

pub(crate) fn cmp(x: f64, y: f64) -> f64 {
    if x.is_nan() && y.is_nan() {
        return 1.0;
    }
    if x.is_nan() {
        return -1.0;
    }
    if y.is_nan() {
        return 1.0;
    }
    match x.total_cmp(&y) {
        Ordering::Less => -1.0,
        Ordering::Equal => 0.0,
        Ordering::Greater => 1.0,
    }
}

pub(crate) const fn min(x: f64, y: f64) -> f64 {
    x.min(y)
}

pub(crate) const fn max(x: f64, y: f64) -> f64 {
    x.max(y)
}

pub(crate) const fn avg(x: f64, y: f64) -> f64 {
    (x + y) / 2.0
}

pub(crate) const fn abs_diff(x: f64, y: f64) -> f64 {
    (x - y).abs()
}

pub(crate) const fn sgn_diff(x: f64, y: f64) -> f64 {
    (x - y).signum()
}

pub(crate) const fn percent_change(x: f64, y: f64) -> f64 {
    if x == 0.0 {
        return 0.0;
    }
    (y - x) / x
}

macro_rules! make_comparison_func {
    ($name: ident, $func: expr) => {
        #[inline]
        pub const fn $name(left: f64, right: f64) -> f64 {
            to_comparison_value($func(left, right), left)
        }
    };
}

macro_rules! make_comparison_func_bool {
    ($name: ident, $func: expr) => {
        #[inline]
        pub const fn $name(left: f64, right: f64) -> f64 {
            if left.is_nan() {
                return f64::NAN;
            }
            if $func(left, right) { 1_f64 } else { 0_f64 }
        }
    };
}

make_comparison_func!(compare_eq, op_eq);
make_comparison_func!(compare_neq, op_neq);
make_comparison_func!(compare_gt, op_gt);
make_comparison_func!(compare_lt, op_lt);
make_comparison_func!(compare_gte, op_gte);
make_comparison_func!(compare_lte, op_lte);

make_comparison_func_bool!(compare_eq_bool, op_eq);
make_comparison_func_bool!(compare_neq_bool, op_neq);
make_comparison_func_bool!(compare_gt_bool, op_gt);
make_comparison_func_bool!(compare_lt_bool, op_lt);
make_comparison_func_bool!(compare_gte_bool, op_gte);
make_comparison_func_bool!(compare_lte_bool, op_lte);

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

    pub const fn get_func(&self) -> BinopFunc {
        match self {
            ComparisonOperator::Equal => compare_eq,
            ComparisonOperator::NotEqual => compare_neq,
            ComparisonOperator::GreaterThan => compare_gt,
            ComparisonOperator::GreaterThanOrEqual => compare_gte,
            ComparisonOperator::LessThan => compare_lt,
            ComparisonOperator::LessThanOrEqual => compare_lte,
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
        let condition = hashify::tiny_map_ignore_case! {
            value.as_bytes(),
            "==" => ComparisonOperator::Equal,
            "!=" => ComparisonOperator::NotEqual,
            ">" => ComparisonOperator::GreaterThan,
            ">=" => ComparisonOperator::GreaterThanOrEqual,
            "<" => ComparisonOperator::LessThan,
            "<=" => ComparisonOperator::LessThanOrEqual,
        };
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
