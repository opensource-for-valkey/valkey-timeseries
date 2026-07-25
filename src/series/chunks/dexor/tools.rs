//! Numeric helpers shared by the DeXOR encoder and decoder.
//!
//! Port of `algorithms.DeXOR.DeXORTools` from the reference Java implementation
//! (<https://github.com/SuDIS-ZJU/DeXOR>).
//!
//! Every table lookup here is bounds-checked and returns `Option`. The Java
//! original indexes its power-of-ten table directly and throws on out-of-range
//! magnitudes; in this port a miss simply means "not representable on the
//! decimal path", and the caller falls back to the raw exception coder.

/// Smallest exponent held in [`P10`].
pub(super) const P10_MIN_EXP: i32 = -23;
/// Largest exponent held in [`P10`].
pub(super) const P10_MAX_EXP: i32 = 23;

/// Powers of ten from `1e-23` through `1e23`, indexed by `exp - P10_MIN_EXP`.
const P10: [f64; 47] = [
    1e-23, 1e-22, 1e-21, 1e-20, 1e-19, 1e-18, 1e-17, 1e-16, 1e-15, 1e-14, 1e-13, 1e-12, 1e-11,
    1e-10, 1e-9, 1e-8, 1e-7, 1e-6, 1e-5, 1e-4, 1e-3, 1e-2, 1e-1, 1e0, 1e1, 1e2, 1e3, 1e4, 1e5, 1e6,
    1e7, 1e8, 1e9, 1e10, 1e11, 1e12, 1e13, 1e14, 1e15, 1e16, 1e17, 1e18, 1e19, 1e20, 1e21, 1e22,
    1e23,
];

/// `DECIMAL_BITS[d] == ceil(log2(10^d))`: the number of bits needed to hold any
/// `d`-digit decimal residual.
const DECIMAL_BITS: [u32; 24] = [
    0, 4, 7, 10, 14, 17, 20, 24, 27, 30, 34, 37, 40, 44, 47, 50, 54, 57, 60, 64, 67, 70, 74, 77,
];

/// Tolerance for "these two doubles denote the same decimal value".
pub(super) const EQUAL_EPS: f64 = 1e-23;

/// Tolerance for "this scaled double is a whole number".
pub(super) const INTEGER_EPS: f64 = 1e-6;

/// Below this hint the digit scan is replaced by an exact decimal-string probe,
/// mirroring the reference implementation's `getEnd_HP` cutoff.
const HIGH_PRECISION_HINT: i32 = -12;

/// `10^exp`, or `None` when `exp` falls outside the table.
#[inline]
pub(super) fn p10(exp: i32) -> Option<f64> {
    if (P10_MIN_EXP..=P10_MAX_EXP).contains(&exp) {
        Some(P10[(exp - P10_MIN_EXP) as usize])
    } else {
        None
    }
}

/// Bit width of a `delta`-digit residual.
///
/// `delta` is always read back from a 4-bit field, so it can never exceed 15
/// and the width can never exceed 50.
#[inline]
pub(super) fn decimal_bits(delta: u32) -> u32 {
    debug_assert!((delta as usize) < DECIMAL_BITS.len());
    DECIMAL_BITS[delta as usize]
}

/// `|a - b| <= EQUAL_EPS`, matching the reference's two-argument `comp`.
#[inline]
pub(super) fn approx_eq(a: f64, b: f64) -> bool {
    (a - b).abs() <= EQUAL_EPS
}

/// `|value| <= EQUAL_EPS`. Both sides of the codec must agree on this exact
/// predicate: it decides whether an explicit sign bit is present.
#[inline]
pub(super) fn approx_zero(value: f64) -> bool {
    value.abs() <= EQUAL_EPS
}

/// `|a - b| < eps`, matching the reference's three-argument `comp`.
#[inline]
fn approx_eq_within(a: f64, b: f64, eps: f64) -> bool {
    (a - b).abs() < eps
}

/// Whether `value` is a whole number to within `eps`.
#[inline]
fn is_int(value: f64, eps: f64) -> bool {
    approx_eq_within(value, value.round(), eps)
}

/// Round toward zero, with the reference's `INTEGER_EPS` nudge that absorbs the
/// rounding error introduced by the preceding division.
#[inline]
pub(super) fn truncate(value: f64) -> i64 {
    if is_int(value, EQUAL_EPS) {
        // `as` saturates at the i64 bounds and maps NaN to 0.
        return value.round() as i64;
    }
    if value > EQUAL_EPS {
        return (value + INTEGER_EPS).floor() as i64;
    }
    if value < -EQUAL_EPS {
        return (value - INTEGER_EPS).ceil() as i64;
    }
    0
}

/// Whether `exp` is exactly the decimal exponent of `value`'s last significant
/// digit: `value / 10^exp` is whole but `value / 10^(exp-1)` is not.
fn is_last_digit_exponent(value: f64, exp: i32) -> bool {
    let (Some(hi), Some(lo)) = (p10(exp), p10(exp - 1)) else {
        return false;
    };
    is_int(value / hi, EQUAL_EPS) && !is_int(value / lo, EQUAL_EPS)
}

/// Decimal exponent of the last significant digit of `value`, read off its
/// shortest round-tripping decimal representation.
///
/// This replaces the reference's `getEnd_HP`, which derives the exponent from
/// `Double.toString` but does not account for Java switching to scientific
/// notation below `1e-3`, and so reports the wrong exponent for those values.
/// Rust's `Display` never uses scientific notation for `f64`, so reading the
/// fraction length is both simpler and correct.
fn last_digit_exponent_from_repr(value: f64) -> i32 {
    let text = format!("{value}");
    match text.split_once('.') {
        Some((_, fraction)) => -(fraction.len() as i32),
        None => {
            let digits = text.trim_start_matches('-');
            (digits.len() - digits.trim_end_matches('0').len()) as i32
        }
    }
}

/// Decimal exponent `q` of `value`'s last significant digit, i.e. the largest
/// `q` for which `value` is an integer multiple of `10^q`.
///
/// `hint` is the exponent chosen for the previous value; consecutive samples in
/// a series overwhelmingly share it, so the scan starts there and usually
/// terminates immediately.
///
/// Returns `None` when the answer would fall outside the power-of-ten table.
/// Port of `DeXORTools.getEnd`.
pub(super) fn last_digit_exponent(value: f64, hint: i32) -> Option<i32> {
    if !value.is_finite() {
        return None;
    }
    if approx_eq_within(value, 0.0, EQUAL_EPS) {
        return Some(0);
    }

    if hint < HIGH_PRECISION_HINT {
        if is_last_digit_exponent(value, hint) {
            return Some(hint);
        }
        let exp = last_digit_exponent_from_repr(value);
        return (P10_MIN_EXP..=P10_MAX_EXP).contains(&exp).then_some(exp);
    }

    let mut q = hint;
    if is_int(value / p10(q)?, INTEGER_EPS) {
        // Walk up while trailing zeros remain.
        while is_int(value / p10(q + 1)?, INTEGER_EPS) {
            q += 1;
        }
        Some(q)
    } else {
        // Walk down until the scaled value is whole.
        loop {
            q -= 1;
            if is_int(value / p10(q)?, INTEGER_EPS) {
                return Some(q);
            }
        }
    }
}
