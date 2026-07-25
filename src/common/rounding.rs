use get_size2::GetSize;
use std::f64;
use std::fmt::Display;

pub const MAX_SIGNIFICANT_DIGITS: u8 = 16;
pub const MAX_DECIMAL_DIGITS: u8 = 16;

#[derive(Clone, Debug, Hash, PartialEq, Copy, GetSize)]
pub enum RoundingStrategy {
    SignificantDigits(u8),
    DecimalDigits(u8),
}

impl RoundingStrategy {
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

/// Rounds f to the given number of decimal digits after the point.
///
/// See also round_to_sig_figs.
pub fn round_to_decimal_digits(value: f64, digits: u8) -> f64 {
    if digits == 0 {
        return value.round();
    }
    let digits = digits.clamp(0, MAX_DECIMAL_DIGITS);
    if digits == 0 {
        return value;
    }
    let factor = 10_f64.powi(digits as i32);
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
pub fn round_to_sig_figs(value: f64, digits: u8) -> f64 {
    if digits == 0
        || digits >= MAX_SIGNIFICANT_DIGITS
        || value.is_nan()
        || value.is_infinite()
        || value == 0.0
    {
        return value;
    }
    let magnitude = 10.0_f64.powi(digits as i32 - 1 - value.abs().log10().floor() as i32);
    (value * magnitude).round() / magnitude
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
        let f = f64::consts::PI;
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
}
