//! `TS.JOIN ... REDUCE op`: the per-row kernels.
//!
//! A reducer sees `(left, right)` for every joined row. An outer-join row with no sample on
//! one side passes `NaN` for that side, and so does a stored `NaN`; the two are not
//! distinguished. The contract, pinned by the tests below and documented in
//! `docs/commands/ts.join.md`:
//!
//! - `coalesce`, `max` and `min` use whichever operand is present, and return `NaN` only
//!   when both are missing.
//! - Every other reducer returns `NaN` when either operand is missing.
//! - Comparisons (`eq`, `ne`, `gt`, `gte`, `lt`, `lte`) return `1` or `0`.
//! - `cmp` and `sgn_diff` return `-1`, `0` or `1`, comparing numerically (`-0 == +0`).
//! - `pct_change` is the fraction `(right - left) / left`, and `NaN` for a zero `left`.
use std::str::FromStr;
use valkey_module::ValkeyError;

/// A reducer kernel: `(left, right) -> value`.
pub type ReducerFn = fn(left: f64, right: f64) -> f64;

macro_rules! join_reducers {
    ($($variant:ident => $name:literal, $kernel:expr;)*) => {
        #[derive(Copy, Clone, Debug, PartialEq, Eq)]
        pub enum JoinReducer {
            $($variant,)*
        }

        impl JoinReducer {
            /// Every reducer, in declaration order.
            pub const ALL: &[JoinReducer] = &[$(JoinReducer::$variant,)*];

            /// The name `REDUCE` accepts (case-insensitively).
            pub const fn name(self) -> &'static str {
                match self {
                    $(JoinReducer::$variant => $name,)*
                }
            }

            pub fn get_handler(self) -> ReducerFn {
                match self {
                    $(JoinReducer::$variant => $kernel,)*
                }
            }
        }
    };
}

join_reducers! {
    AbsDiff   => "abs_diff",   |l, r| (l - r).abs();
    Avg       => "avg",        |l, r| (l + r) / 2.0;
    Cmp       => "cmp",        |l, r| strict(l, r, sign_of_difference);
    Coalesce  => "coalesce",   |l, r| if l.is_nan() { r } else { l };
    Div       => "div",        |l, r| if r == 0.0 { f64::NAN } else { l / r };
    Eq        => "eq",         |l, r| strict(l, r, |l, r| bool_value(l == r));
    Gt        => "gt",         |l, r| strict(l, r, |l, r| bool_value(l > r));
    Gte       => "gte",        |l, r| strict(l, r, |l, r| bool_value(l >= r));
    Lt        => "lt",         |l, r| strict(l, r, |l, r| bool_value(l < r));
    Lte       => "lte",        |l, r| strict(l, r, |l, r| bool_value(l <= r));
    Max       => "max",        f64::max;
    Min       => "min",        f64::min;
    Mod       => "mod",        |l, r| l % r;
    Mul       => "mul",        |l, r| l * r;
    Ne        => "ne",         |l, r| strict(l, r, |l, r| bool_value(l != r));
    PctChange => "pct_change", |l, r| strict(l, r, |l, r| if l == 0.0 { f64::NAN } else { (r - l) / l });
    // `powf` returns 1 for `NaN^0` and `1^NaN`; a missing operand must not produce a value.
    Pow       => "pow",        |l, r| strict(l, r, f64::powf);
    SgnDiff   => "sgn_diff",   |l, r| strict(l, r, sign_of_difference);
    Sub       => "sub",        |l, r| l - r;
    Sum       => "sum",        |l, r| l + r;
}

/// `f(l, r)`, or `NaN` when either operand is missing.
#[inline]
fn strict(l: f64, r: f64, f: impl Fn(f64, f64) -> f64) -> f64 {
    if l.is_nan() || r.is_nan() {
        f64::NAN
    } else {
        f(l, r)
    }
}

#[inline]
fn bool_value(b: bool) -> f64 {
    if b { 1.0 } else { 0.0 }
}

/// `-1`, `0` or `1` for non-NaN operands, comparing numerically so that `-0 == +0`. Two
/// infinities of the same sign compare equal.
#[inline]
fn sign_of_difference(l: f64, r: f64) -> f64 {
    if l < r {
        -1.0
    } else if l > r {
        1.0
    } else {
        0.0
    }
}

impl FromStr for JoinReducer {
    type Err = ValkeyError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        JoinReducer::try_from(s)
    }
}

impl TryFrom<&str> for JoinReducer {
    type Error = ValkeyError;

    fn try_from(op: &str) -> Result<Self, Self::Error> {
        JoinReducer::ALL
            .iter()
            .copied()
            .find(|reducer| reducer.name().eq_ignore_ascii_case(op))
            .ok_or_else(|| ValkeyError::String(format!("TSDB: unknown binary op '{op}'")))
    }
}

#[cfg(test)]
mod tests {
    use super::JoinReducer::{self, *};

    const NAN: f64 = f64::NAN;
    const INF: f64 = f64::INFINITY;

    fn reduce(reducer: JoinReducer, l: f64, r: f64) -> f64 {
        reducer.get_handler()(l, r)
    }

    /// Asserts `reducer(l, r) == expected`, treating two NaNs as equal.
    fn check(reducer: JoinReducer, l: f64, r: f64, expected: f64) {
        let got = reduce(reducer, l, r);
        let same = (got.is_nan() && expected.is_nan()) || got == expected;
        assert!(
            same,
            "{}({l}, {r}) = {got}, expected {expected}",
            reducer.name()
        );
    }

    #[test]
    fn names_parse_back_case_insensitively() {
        assert_eq!(JoinReducer::ALL.len(), 20);
        for &reducer in JoinReducer::ALL {
            assert_eq!(JoinReducer::try_from(reducer.name()).unwrap(), reducer);
            let upper = reducer.name().to_ascii_uppercase();
            assert_eq!(JoinReducer::try_from(upper.as_str()).unwrap(), reducer);
        }
        assert!(JoinReducer::try_from("invalid_op").is_err());
        assert!(JoinReducer::try_from("==").is_err());
    }

    #[test]
    fn arithmetic_values() {
        check(Sum, 7.0, 2.0, 9.0);
        check(Sub, 7.0, 2.0, 5.0);
        check(Mul, 7.0, 2.0, 14.0);
        check(Div, 7.0, 2.0, 3.5);
        check(Div, 7.0, 0.0, NAN);
        check(Mod, 7.0, 2.0, 1.0);
        check(Pow, 2.0, 3.0, 8.0);
        check(Avg, 7.0, 2.0, 4.5);
        check(AbsDiff, 2.0, 7.0, 5.0);
        check(Max, 7.0, 2.0, 7.0);
        check(Min, 7.0, 2.0, 2.0);
        check(Coalesce, 7.0, 2.0, 7.0);
        check(PctChange, 100.0, 150.0, 0.5);
        check(PctChange, 10.0, 5.0, -0.5);
    }

    #[test]
    fn comparisons_return_booleans() {
        // (left, right) = (7, 2), (7, 7), (7, 9)
        let table = [
            (Eq, [0.0, 1.0, 0.0]),
            (Ne, [1.0, 0.0, 1.0]),
            (Gt, [1.0, 0.0, 0.0]),
            (Gte, [1.0, 1.0, 0.0]),
            (Lt, [0.0, 0.0, 1.0]),
            (Lte, [0.0, 1.0, 1.0]),
            (Cmp, [1.0, 0.0, -1.0]),
            (SgnDiff, [1.0, 0.0, -1.0]),
        ];
        for (reducer, expected) in table {
            for (r, e) in [2.0, 7.0, 9.0].into_iter().zip(expected) {
                check(reducer, 7.0, r, e);
            }
        }
    }

    #[test]
    fn signed_zeros_and_infinities() {
        for reducer in [Cmp, SgnDiff] {
            check(reducer, -0.0, 0.0, 0.0);
            check(reducer, INF, INF, 0.0);
            check(reducer, -INF, INF, -1.0);
        }
        check(Eq, -0.0, 0.0, 1.0);
        check(Ne, -0.0, 0.0, 0.0);
        check(Gt, INF, 1.0, 1.0);
        check(PctChange, 0.0, 100.0, NAN);
        check(PctChange, -0.0, 100.0, NAN);
    }

    #[test]
    fn missing_operand_policy() {
        let present_wins = [Coalesce, Max, Min];
        for &reducer in JoinReducer::ALL {
            for (l, r) in [(5.0, NAN), (NAN, 5.0)] {
                let expected = if present_wins.contains(&reducer) {
                    5.0
                } else {
                    NAN
                };
                check(reducer, l, r, expected);
            }
            check(reducer, NAN, NAN, NAN);
        }
    }
}
