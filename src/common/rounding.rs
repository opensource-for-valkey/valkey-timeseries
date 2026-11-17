use get_size2::GetSize;
use std::fmt::Display;

/// The `DECIMAL_DIGITS` operand that means "no rounding", overriding any configured
/// `ts-decimal-digits`/`ts-significant-digits` default. (`DECIMAL_DIGITS 0` is whole-number
/// rounding, not this.) It is also how a replicated `TS.CREATE` spells a series without rounding.
pub const NO_ROUNDING_KEYWORD: &str = "none";

/// Largest accepted `SIGNIFICANT_DIGITS` value.
///
/// Note this is a cap, not the largest *effective* value: [`round_to_sig_figs`] leaves values
/// untouched at exactly this many digits, because it is at the edge of `f64`'s ~15-17
/// significant decimal digits and rounding there would only introduce error. Requesting 16
/// is therefore accepted but rounds nothing; 15 is the largest value that has an effect.
pub const MAX_SIGNIFICANT_DIGITS: u8 = 16;

/// Smallest accepted `SIGNIFICANT_DIGITS` value.
///
/// Zero is rejected rather than accepted-and-ignored: "no significant digits" is not a
/// quantity, so unlike `DECIMAL_DIGITS 0` there is nothing it could sensibly mean. To turn
/// rounding off, omit the argument (or set the config to `none`).
pub const MIN_SIGNIFICANT_DIGITS: u8 = 1;

/// Largest accepted `DECIMAL_DIGITS` value. Unlike [`MAX_SIGNIFICANT_DIGITS`], every value up
/// to and including this one has an effect.
pub const MAX_DECIMAL_DIGITS: u8 = 16;

/// Smallest accepted `DECIMAL_DIGITS` value. Zero is meaningful here — it rounds to whole
/// numbers — so it is accepted.
pub const MIN_DECIMAL_DIGITS: u8 = 0;

#[derive(Clone, Debug, Hash, PartialEq, Copy, GetSize)]
pub enum RoundingStrategy {
    SignificantDigits(u8),
    DecimalDigits(u8),
}

impl RoundingStrategy {
    /// Whether the digit count is one the write path accepts: `SIGNIFICANT_DIGITS`
    /// [`MIN_SIGNIFICANT_DIGITS`]..=[`MAX_SIGNIFICANT_DIGITS`], `DECIMAL_DIGITS`
    /// [`MIN_DECIMAL_DIGITS`]..=[`MAX_DECIMAL_DIGITS`]. A loaded payload is held to the same.
    pub fn is_valid(&self) -> bool {
        match self {
            RoundingStrategy::SignificantDigits(digits) => {
                (MIN_SIGNIFICANT_DIGITS..=MAX_SIGNIFICANT_DIGITS).contains(digits)
            }
            RoundingStrategy::DecimalDigits(digits) => {
                (MIN_DECIMAL_DIGITS..=MAX_DECIMAL_DIGITS).contains(digits)
            }
        }
    }

    pub fn round(&self, value: f64) -> f64 {
        match self {
            RoundingStrategy::SignificantDigits(digits) => round_to_sig_figs(value, *digits),
            RoundingStrategy::DecimalDigits(digits) => round_to_decimal_digits(value, *digits),
        }
    }
}

impl Display for RoundingStrategy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RoundingStrategy::SignificantDigits(digits) => {
                write!(f, "significant_digits({digits})")
            }
            RoundingStrategy::DecimalDigits(digits) => write!(f, "decimal_digits({digits})"),
        }
    }
}

/// Every `f64` at least this large in magnitude (2^52) is a whole number: its 52 fraction bits
/// are all spent above the point. Rounding one to any number of decimal digits leaves it as it
/// is, and scaling it by `10^digits` first could overflow to infinity.
const INTEGRAL_MAGNITUDE: f64 = 4_503_599_627_370_496.0;

/// The largest power of ten [`round_to_sig_figs`] applies as one factor. `10^±300` are normal
/// `f64`s with room to spare: past `10^308` the factor overflows, and below `10^-307` it is
/// subnormal and has lost precision.
const MAX_SCALE_EXPONENT: i32 = 300;

/// Rounds `value` to the given number of decimal digits after the point.
///
/// `digits == 0` rounds to whole numbers — it does *not* mean "no rounding". Both the
/// per-series `DECIMAL_DIGITS 0` argument and `ts-decimal-digits 0` mean this; rounding is
/// switched off by omitting the argument, or by setting the config to `none`.
///
/// Values above [`MAX_DECIMAL_DIGITS`] are clamped rather than rejected; both callers validate
/// the range first, so the clamp only guards direct use.
///
/// Non-finite values, and values too large to have a fractional part, are returned unchanged,
/// so a finite value always stays finite. The sign of zero is kept.
///
/// See also [`round_to_sig_figs`].
pub fn round_to_decimal_digits(value: f64, digits: u8) -> f64 {
    if !value.is_finite() || value.abs() >= INTEGRAL_MAGNITUDE {
        return value;
    }
    if digits == 0 {
        return value.round();
    }
    let digits = digits.clamp(1, MAX_DECIMAL_DIGITS);
    let factor = 10_f64.powi(digits as i32);
    // Below 2^52 times at most 10^16, the product is nowhere near overflowing.
    (value * factor).round() / factor
}

/// Rounds a floating-point value to a specified number of significant figures.
///
/// ### Parameters
///
/// - `value`: The floating-point value to be rounded.
/// - `digits`: The number of significant figures to round to. If `digits` is 0 or greater than
///   or equal to [`MAX_SIGNIFICANT_DIGITS`], the function returns the original `value`
///   unchanged — at that point the request is at or beyond `f64`'s precision, so rounding
///   would only introduce error.
///
/// ### Returns
///
/// The rounded floating-point value with the specified number of significant figures.
/// Non-finite values and zeros (of either sign) are returned unchanged. A finite value stays
/// finite: the scale is split for values whose `10^k` factor would overflow or go subnormal
/// (below ~1e-286 or above ~1e301), and a value whose rounded form is past `f64::MAX` (such as
/// `f64::MAX` to 3 digits, 1.80e308) is returned unchanged.
pub fn round_to_sig_figs(value: f64, digits: u8) -> f64 {
    if digits == 0 || digits >= MAX_SIGNIFICANT_DIGITS || !value.is_finite() || value == 0.0 {
        return value;
    }
    // The power of ten that brings `digits` significant digits above the point.
    let exponent = digits as i32 - 1 - value.abs().log10().floor() as i32;
    let rounded = if exponent > MAX_SCALE_EXPONENT {
        // A tiny value (down to subnormals): `10^exponent` overflows, so scale in two steps.
        // At most 10^338 in all, so `rest` is at most 10^38.
        let first = 10_f64.powi(MAX_SCALE_EXPONENT);
        let rest = 10_f64.powi(exponent - MAX_SCALE_EXPONENT);
        ((value * first) * rest).round() / rest / first
    } else if exponent < -MAX_SCALE_EXPONENT {
        // A huge value: `10^exponent` is subnormal, so divide by its reciprocal instead, which
        // is at most 10^308 and finite.
        let divisor = 10_f64.powi(-exponent);
        (value / divisor).round() * divisor
    } else {
        let magnitude = 10_f64.powi(exponent);
        (value * magnitude).round() / magnitude
    };
    // Only a rounded value past `f64::MAX` gets here non-finite; it has no representation.
    if rounded.is_finite() { rounded } else { value }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_round_to_decimal_digits_small_positive_number() {
        let f = 0.0000000123456789;
        let digits = 10;
        let expected = 0.0000000123;
        let result = round_to_decimal_digits(f, digits);
        assert_eq!(result, expected);
    }

    #[test]
    fn test_round_to_decimal_digits_zero_digits() {
        let f = std::f64::consts::PI;
        let digits = 0;
        let expected = 3.0;
        let result = round_to_decimal_digits(f, digits);
        assert_eq!(result, expected);
    }

    #[test]
    fn test_round_to_sig_figs_negative_value() {
        let value = -123.456;
        let digits = 3;
        let expected = -123.0;
        let result = round_to_sig_figs(value, digits);
        assert_eq!(result, expected);
    }

    #[test]
    fn test_round_to_sig_figs_large_value() {
        let value = 123456789.0;
        let digits = 2;
        let expected = 120000000.0;
        let result = round_to_sig_figs(value, digits);
        assert_eq!(result, expected);
    }

    #[test]
    fn test_round_to_sig_figs_trailing_zeros() {
        let value = 123456789.0;
        let digits = 2;
        let expected = 120000000.0;
        let result = round_to_sig_figs(value, digits);
        assert_eq!(result, expected);
    }

    #[test]
    fn test_round_to_sig_figs_long_string_of_zeros() {
        let value = 0.0000000123456789;
        let digits = 3;
        let expected = 0.0000000123;
        let result = round_to_sig_figs(value, digits);
        assert!((result - expected).abs() <= 1e-18);
    }

    #[test]
    fn test_round_to_sig_figs_halfway_between_floats() {
        let value = 1.5; // Halfway between 1.4999999999999998 and 1.5000000000000002
        let digits = 1;
        let expected = 2.0;
        let result = round_to_sig_figs(value, digits);
        assert_eq!(result, expected);
    }

    #[test]
    fn test_round_to_sig_figs_long_string_of_nines() {
        let value = 0.999999999;
        let digits = 3;
        let expected = 1.0;
        let result = round_to_sig_figs(value, digits);
        assert_eq!(result, expected);
    }

    #[test]
    fn test_round_to_sig_figs_special_cases() {
        assert!(round_to_sig_figs(f64::NAN, 3).is_nan());
        assert_eq!(round_to_sig_figs(f64::INFINITY, 3), f64::INFINITY);
        assert_eq!(round_to_sig_figs(f64::NEG_INFINITY, 3), f64::NEG_INFINITY);
        assert_eq!(round_to_sig_figs(0.0, 3), 0.0);
        assert_eq!(round_to_sig_figs(-123.456, 2), -120.0);
        assert_eq!(round_to_sig_figs(123456789.0, 2), 120000000.0);
    }

    /// The formula both helpers used before extreme magnitudes were handled. Inside the
    /// normal range the results must not move: they are stored sample values.
    fn old_sig_figs(value: f64, digits: u8) -> f64 {
        let magnitude = 10.0_f64.powi(digits as i32 - 1 - value.abs().log10().floor() as i32);
        (value * magnitude).round() / magnitude
    }

    fn old_decimal_digits(value: f64, digits: u8) -> f64 {
        let factor = 10_f64.powi(digits as i32);
        (value * factor).round() / factor
    }

    fn assert_same(actual: f64, expected: f64, what: &str) {
        assert_eq!(
            actual.to_bits(),
            expected.to_bits(),
            "{what}: {actual:e} != {expected:e}"
        );
    }

    /// `10^k` from `powi` is a few ulps off for large `k`, so results at extreme exponents are
    /// held to the nearest `f64` within a few ulps (subnormal ulps, below `MIN_POSITIVE`), with
    /// the sign exact.
    fn assert_close(actual: f64, expected: f64, what: &str) {
        assert!(actual.is_finite(), "{what}: {actual:e}");
        assert_eq!(
            actual.is_sign_negative(),
            expected.is_sign_negative(),
            "{what}: {actual:e}"
        );
        let tolerance = (expected.abs() * 1e-14).max(8.0 * 5e-324);
        assert!(
            (actual - expected).abs() <= tolerance,
            "{what}: {actual:e} != {expected:e}"
        );
    }

    #[test]
    fn test_decimal_digits_large_values_are_kept() {
        for value in [
            1e308,
            f64::MAX,
            -f64::MAX,
            1.8e306,
            2f64.powi(52),
            2f64.powi(53) + 2.0,
        ] {
            for digits in [0, 1, 2, 16] {
                assert_same(
                    round_to_decimal_digits(value, digits),
                    value,
                    &format!("{value:e} to {digits}"),
                );
            }
        }
        // Just below 2^52 a fraction still exists, and is rounded.
        assert_eq!(
            round_to_decimal_digits(4_503_599_627_370_495.5, 0),
            4_503_599_627_370_496.0
        );
        assert_eq!(round_to_decimal_digits(1e15 + 0.25, 1), 1e15 + 0.25);
        assert_eq!(round_to_decimal_digits(1e15 + 0.75, 0), 1e15 + 1.0);
    }

    #[test]
    fn test_decimal_digits_tiny_values_round_to_signed_zero() {
        for value in [5e-324, f64::MIN_POSITIVE, 1e-300, 1e-17, 0.004] {
            for digits in [0, 2, 16] {
                if value * 10f64.powi(digits as i32) >= 0.5 {
                    continue;
                }
                assert_same(round_to_decimal_digits(value, digits), 0.0, "positive");
                assert_same(round_to_decimal_digits(-value, digits), -0.0, "negative");
            }
        }
        assert_same(round_to_decimal_digits(0.0, 2), 0.0, "zero");
        assert_same(round_to_decimal_digits(-0.0, 2), -0.0, "negative zero");
        assert_same(
            round_to_decimal_digits(-0.0, 0),
            -0.0,
            "negative zero, 0 digits",
        );
    }

    #[test]
    fn test_decimal_digits_nonfinite() {
        for digits in [0, 2, 16] {
            assert!(round_to_decimal_digits(f64::NAN, digits).is_nan());
            assert_eq!(
                round_to_decimal_digits(f64::INFINITY, digits),
                f64::INFINITY
            );
            assert_eq!(
                round_to_decimal_digits(f64::NEG_INFINITY, digits),
                f64::NEG_INFINITY
            );
        }
    }

    #[test]
    fn test_sig_figs_tiny_and_subnormal_values() {
        let cases = [
            (1e-310, 3, 1e-310),
            (1.2345e-310, 3, 1.23e-310),
            (-1.2345e-310, 3, -1.23e-310),
            (1e-300, 15, 1e-300),
            (f64::MIN_POSITIVE, 3, 2.23e-308),
            (5e-324, 1, 5e-324),
            (5e-324, 15, 5e-324),
            (1.2345e-305, 3, 1.23e-305),
        ];
        for (value, digits, expected) in cases {
            let result = round_to_sig_figs(value, digits);
            assert_close(result, expected, &format!("{value:e} to {digits}"));
        }
    }

    #[test]
    fn test_sig_figs_huge_values() {
        assert_close(round_to_sig_figs(1.2345e308, 3), 1.23e308, "1.2345e308");
        assert_close(round_to_sig_figs(-1.2345e308, 3), -1.23e308, "-1.2345e308");
        assert_close(round_to_sig_figs(1e308, 1), 1e308, "1e308");
        // Rounded up, these are past f64::MAX: kept rather than turned into infinity.
        for digits in [1, 3] {
            assert_same(round_to_sig_figs(f64::MAX, digits), f64::MAX, "MAX");
            assert_same(round_to_sig_figs(-f64::MAX, digits), -f64::MAX, "-MAX");
        }
        // At 15 digits the result sits within an ulp or so of f64::MAX, on either side.
        assert_close(round_to_sig_figs(f64::MAX, 15), f64::MAX, "MAX to 15");
        assert_close(round_to_sig_figs(-f64::MAX, 15), -f64::MAX, "-MAX to 15");
        assert_same(round_to_sig_figs(1.7e308, 1), 1.7e308, "1.7e308 to 2e308");
    }

    #[test]
    fn test_sig_figs_scale_exponent_boundaries() {
        // Each pair puts the scale exponent on either side of the single-factor limit.
        let cases = [
            (1.23456e-286, 15, 1.23456e-286),
            (1.23456e-287, 15, 1.23456e-287),
            (1.23456e-298, 3, 1.23e-298),
            (1.23456e-299, 3, 1.23e-299),
            (1.23456e302, 3, 1.23e302),
            (1.23456e303, 3, 1.23e303),
            (9.876e300, 1, 1e301),
            (9.876e301, 1, 1e302),
        ];
        for (value, digits, expected) in cases {
            assert_close(
                round_to_sig_figs(value, digits),
                expected,
                &format!("{value:e} to {digits}"),
            );
        }
        // Decimal exponent boundaries in the ordinary range.
        assert_eq!(round_to_sig_figs(1000.0, 3), 1000.0);
        assert_eq!(round_to_sig_figs(999.5, 3), 1000.0);
        assert_eq!(round_to_sig_figs(0.001, 2), 0.001);
        assert_eq!(round_to_sig_figs(0.00999, 2), 0.01);
    }

    #[test]
    fn test_sig_figs_signed_zero_and_nonfinite() {
        for digits in [1, 3, 15] {
            assert_same(round_to_sig_figs(0.0, digits), 0.0, "zero");
            assert_same(round_to_sig_figs(-0.0, digits), -0.0, "negative zero");
            assert!(round_to_sig_figs(f64::NAN, digits).is_nan());
            assert_eq!(round_to_sig_figs(f64::INFINITY, digits), f64::INFINITY);
            assert_eq!(
                round_to_sig_figs(f64::NEG_INFINITY, digits),
                f64::NEG_INFINITY
            );
        }
    }

    #[test]
    fn test_normal_range_results_are_unchanged() {
        let mut seed = 0x9E37_79B9_7F4A_7C15u64;
        for _ in 0..20_000 {
            seed = seed
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            let mantissa = 1.0 + (seed >> 11) as f64 / (1u64 << 53) as f64 * 9.0;
            let exponent = ((seed >> 3) % 561) as i32 - 280;
            let sign = if seed & 1 == 0 { 1.0 } else { -1.0 };
            let value = sign * mantissa * 10f64.powi(exponent);
            for digits in 1..MAX_SIGNIFICANT_DIGITS {
                assert_same(
                    round_to_sig_figs(value, digits),
                    old_sig_figs(value, digits),
                    &format!("{value:e} to {digits} significant"),
                );
            }
            if value.abs() < 1e15 {
                for digits in 1..=MAX_DECIMAL_DIGITS {
                    assert_same(
                        round_to_decimal_digits(value, digits),
                        old_decimal_digits(value, digits),
                        &format!("{value:e} to {digits} decimal"),
                    );
                }
            }
        }
    }

    proptest::proptest! {
        #[test]
        fn finite_values_stay_finite_with_their_sign(
            value in proptest::num::f64::ANY,
            digits in 0u8..=20,
        ) {
            for result in [round_to_sig_figs(value, digits), round_to_decimal_digits(value, digits)] {
                if value.is_nan() {
                    proptest::prop_assert!(result.is_nan());
                    continue;
                }
                proptest::prop_assert_eq!(result.is_finite(), value.is_finite(), "{:e} -> {:e}", value, result);
                proptest::prop_assert_eq!(result.is_sign_negative(), value.is_sign_negative(), "{:e} -> {:e}", value, result);
            }
        }
    }

    #[test]
    fn test_is_valid_matches_write_path_bounds() {
        use RoundingStrategy::*;
        for digits in 0..=u8::MAX {
            assert_eq!(
                SignificantDigits(digits).is_valid(),
                (1..=16).contains(&digits),
                "{digits}"
            );
            assert_eq!(DecimalDigits(digits).is_valid(), digits <= 16, "{digits}");
        }
    }
}
