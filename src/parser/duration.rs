use crate::parser::{ParseError, ParseResult};
use std::str;

const MILLIS_PER_SECOND: i64 = 1_000;
const MILLIS_PER_MINUTE: i64 = 60 * MILLIS_PER_SECOND;
const MILLIS_PER_HOUR: i64 = 60 * MILLIS_PER_MINUTE;
const MILLIS_PER_DAY: i64 = 24 * MILLIS_PER_HOUR;
const MILLIS_PER_WEEK: i64 = 7 * MILLIS_PER_DAY;
const MILLIS_PER_YEAR: i64 = 365 * MILLIS_PER_DAY;

/// Parses a nonnegative duration in milliseconds (`RETENTION`, `IGNORE`, bucket and policy
/// durations). Accepts what [`parse_duration_value`] does, but rejects a negative total and
/// any leading `-`, so `-0` and `-0.4ms` are errors rather than zero (RTS rejects
/// `RETENTION -0`). The sign is checked before rounding.
pub fn parse_positive_duration_value(s: &str) -> Result<i64, ParseError> {
    let sum = sum_duration(s)?;
    if s.starts_with('-') || sum.is_negative() {
        return Err(ParseError::InvalidDuration(format!(
            "duration cannot be negative; got {s}"
        )));
    }
    sum.to_millis(s)
}

/// Parses a duration in milliseconds, which may be negative.
///
/// Segments may be combined, e.g. `2h5m`, `-2h5m` or `2h-5m`. Once a segment is negative,
/// every later segment is negated too (`2h-5m5s` is 2h - 5m - 5s). A segment without a unit is
/// milliseconds. Integer segments are summed exactly, so `9007199254740993ms` and any plain
/// integer up to `i64::MAX` come back unchanged; a fraction or exponent goes through `f64`.
/// Values outside the `i64` range, and sums that are not finite, are errors.
pub fn parse_duration_value(s: &str) -> ParseResult<i64> {
    sum_duration(s)?.to_millis(s)
}

/// One segment's contribution in milliseconds: exact for an integer amount, `f64` otherwise.
#[derive(Debug, Clone, Copy, PartialEq)]
enum SegmentMs {
    Exact(i128),
    Approx(f64),
}

impl SegmentMs {
    fn is_negative(self) -> bool {
        match self {
            Self::Exact(v) => v < 0,
            Self::Approx(v) => v < 0.0,
        }
    }

    fn negated(self) -> Self {
        match self {
            Self::Exact(v) => Self::Exact(-v),
            Self::Approx(v) => Self::Approx(-v),
        }
    }

    #[cfg(test)]
    fn as_f64(self) -> f64 {
        match self {
            Self::Exact(v) => v as f64,
            Self::Approx(v) => v,
        }
    }
}

#[derive(Debug, Default)]
struct DurationSum {
    exact: i128,
    approx: f64,
    has_approx: bool,
}

impl DurationSum {
    fn total_f64(&self) -> f64 {
        self.exact as f64 + self.approx
    }

    fn is_negative(&self) -> bool {
        if self.has_approx {
            self.total_f64() < 0.0
        } else {
            self.exact < 0
        }
    }

    fn to_millis(&self, s: &str) -> ParseResult<i64> {
        let total = if self.has_approx {
            let rounded = self.total_f64().round();
            // `i64::MAX as f64` is 2^63, one past the range, so `>` lets 2^63 through to
            // `try_from` below, which rejects it.
            if !rounded.is_finite() || rounded.abs() > i64::MAX as f64 {
                return Err(out_of_range(s));
            }
            rounded as i128
        } else {
            self.exact
        };
        // `i64::MIN` is excluded so a caller can always negate the result.
        i64::try_from(total)
            .ok()
            .filter(|v| *v != i64::MIN)
            .ok_or_else(|| out_of_range(s))
    }
}

fn out_of_range(s: &str) -> ParseError {
    ParseError::InvalidDuration(format!("{s} (out of range)"))
}

fn sum_duration(s: &str) -> ParseResult<DurationSum> {
    // todo: duration segments should go from coarser to finer grain,
    // i.e. we should forbid something like 25s2d
    if s.is_empty() {
        return Err(ParseError::InvalidDuration(r##""""##.to_string()));
    }

    let mut sum = DurationSum::default();
    let mut rest = s;
    let mut is_minus = false;
    while !rest.is_empty() {
        let (mut segment, next) = scan_duration_segment(rest)?;
        if is_minus && !segment.is_negative() {
            segment = segment.negated();
        }
        if segment.is_negative() {
            is_minus = true;
        }
        match segment {
            SegmentMs::Exact(v) => {
                sum.exact = sum.exact.checked_add(v).ok_or_else(|| out_of_range(s))?;
            }
            SegmentMs::Approx(v) => {
                sum.approx += v;
                sum.has_approx = true;
                // inf + -inf is NaN, which every later range check would let through.
                if !sum.approx.is_finite() {
                    return Err(out_of_range(s));
                }
            }
        }
        rest = next;
    }
    Ok(sum)
}

fn scan_duration_segment(s: &str) -> ParseResult<(SegmentMs, &str)> {
    let suffix_start = s.find(['d', 'h', 'm', 's', 'w', 'y']).unwrap_or(s.len());
    let (multiplier, suffix_len): (i64, usize) = match &s.as_bytes()[suffix_start..] {
        [b'm', b's', ..] => (1, 2),
        [b'm', ..] => (MILLIS_PER_MINUTE, 1),
        [b's', ..] => (MILLIS_PER_SECOND, 1),
        [b'h', ..] => (MILLIS_PER_HOUR, 1),
        [b'd', ..] => (MILLIS_PER_DAY, 1),
        [b'w', ..] => (MILLIS_PER_WEEK, 1),
        [b'y', ..] => (MILLIS_PER_YEAR, 1),
        _ => (1, 0),
    };

    let num_str = &s[..suffix_start];
    let segment = if let Ok(n) = num_str.parse::<i64>() {
        // |n| <= 2^63 and the largest multiplier is < 2^35, so the product fits in an i128.
        SegmentMs::Exact(i128::from(n) * i128::from(multiplier))
    } else {
        // `f64::parse` also accepts `nan`/`inf`/`infinity`. NaN in particular slipped past
        // every range check (all its comparisons are false) and came out as a zero duration,
        // so `RETENTION nan` and `RETENTION NaNms` were accepted.
        match num_str.parse::<f64>() {
            Ok(num) if num.is_finite() => {
                let ms = num * multiplier as f64;
                if !ms.is_finite() {
                    return Err(out_of_range(s));
                }
                SegmentMs::Approx(ms)
            }
            _ => return Err(ParseError::InvalidDuration(num_str.to_string())),
        }
    };
    Ok((segment, &s[suffix_start + suffix_len..]))
}

#[cfg(test)]
mod tests {
    use super::*;

    const MILLIS_PER_SECOND: f64 = super::MILLIS_PER_SECOND as f64;
    const MILLIS_PER_MINUTE: f64 = super::MILLIS_PER_MINUTE as f64;
    const MILLIS_PER_HOUR: f64 = super::MILLIS_PER_HOUR as f64;
    const MILLIS_PER_DAY: f64 = super::MILLIS_PER_DAY as f64;
    const MILLIS_PER_WEEK: f64 = super::MILLIS_PER_WEEK as f64;
    const MILLIS_PER_YEAR: f64 = super::MILLIS_PER_YEAR as f64;

    /// The unrounded sum, so fractional cases can be checked exactly.
    fn parse_duration_internal(s: &str) -> ParseResult<f64> {
        sum_duration(s).map(|sum| sum.total_f64())
    }

    #[test]
    fn test_non_finite_numbers_are_not_durations() {
        for s in [
            "nan", "NaN", "NaNms", "nanh", "inf", "infinity", "-inf", "infms",
        ] {
            assert!(parse_duration_value(s).is_err(), "{s}");
        }
    }

    #[test]
    fn test_scan_duration_segment_empty() {
        let result = scan_duration_segment("");
        assert!(result.is_err());
        match result {
            Err(ParseError::InvalidDuration(msg)) => assert_eq!(msg, ""),
            _ => panic!("Expected InvalidDuration error for empty string"),
        }
    }

    #[test]
    fn test_scan_duration_segment_basic() {
        // Test basic time units
        let cases = [
            ("5s", 5.0 * MILLIS_PER_SECOND, ""),
            ("10m", 10.0 * MILLIS_PER_MINUTE, ""),
            ("2h", 2.0 * MILLIS_PER_HOUR, ""),
            ("3d", 3.0 * MILLIS_PER_DAY, ""),
            ("1w", 1.0 * MILLIS_PER_WEEK, ""),
            ("0.5y", 0.5 * MILLIS_PER_YEAR, ""),
            ("100ms", 100.0, ""),
        ];

        for (input, expected_duration, expected_remaining) in cases {
            let result = scan_duration_segment(input).unwrap();
            assert_eq!(
                result.0.as_f64(),
                expected_duration,
                "Duration mismatch for {}: got {}, expected {}",
                input,
                result.0.as_f64(),
                expected_duration
            );
            assert_eq!(
                result.1, expected_remaining,
                "Remaining string mismatch for {}: got '{}', expected '{}'",
                input, result.1, expected_remaining
            );
        }
    }

    #[test]
    fn test_scan_duration_segment_negative() {
        // Test negative durations
        let cases = [
            ("-5s", -5.0 * MILLIS_PER_SECOND, ""),
            ("-10m", -10.0 * MILLIS_PER_MINUTE, ""),
            ("-2.5h", -2.5 * MILLIS_PER_HOUR, ""),
            ("-0.1d", -0.1 * MILLIS_PER_DAY, ""),
        ];

        for (input, expected_duration, expected_remaining) in cases {
            let result = scan_duration_segment(input).unwrap();
            assert_eq!(
                result.0.as_f64(),
                expected_duration,
                "Duration mismatch for {}: got {}, expected {}",
                input,
                result.0.as_f64(),
                expected_duration
            );
            assert_eq!(
                result.1, expected_remaining,
                "Remaining string mismatch for {}: got '{}', expected '{}'",
                input, result.1, expected_remaining
            );
        }
    }

    #[test]
    fn test_scan_duration_segment_float_values() {
        // Test floating point values
        let cases = [
            ("3.27s", 3.27 * MILLIS_PER_SECOND, ""),
            ("0.5m", 0.5 * MILLIS_PER_MINUTE, ""),
            ("1.75h", 1.75 * MILLIS_PER_HOUR, ""),
            ("0.25d", 0.25 * MILLIS_PER_DAY, ""),
            ("1.5ms", 1.5, ""),
        ];

        for (input, expected_duration, expected_remaining) in cases {
            let result = scan_duration_segment(input).unwrap();
            assert_eq!(
                result.0.as_f64(),
                expected_duration,
                "Duration mismatch for {}: got {}, expected {}",
                input,
                result.0.as_f64(),
                expected_duration
            );
            assert_eq!(
                result.1, expected_remaining,
                "Remaining string mismatch for {}: got '{}', expected '{}'",
                input, result.1, expected_remaining
            );
        }
    }

    #[test]
    fn test_scan_duration_segment_with_remainder() {
        // Test cases with remaining text
        let cases = [
            ("5s10m", 5.0 * MILLIS_PER_SECOND, "10m"),
            ("2h45m", 2.0 * MILLIS_PER_HOUR, "45m"),
            ("1.5dextra", 1.5 * MILLIS_PER_DAY, "extra"),
            ("10msrest", 10.0, "rest"),
        ];

        for (input, expected_duration, expected_remaining) in cases {
            let result = scan_duration_segment(input).unwrap();
            assert_eq!(
                result.0.as_f64(),
                expected_duration,
                "Duration mismatch for {}: got {}, expected {}",
                input,
                result.0.as_f64(),
                expected_duration
            );
            assert_eq!(
                result.1, expected_remaining,
                "Remaining string mismatch for {}: got '{}', expected '{}'",
                input, result.1, expected_remaining
            );
        }
    }

    #[test]
    fn test_scan_duration_segment_edge_cases() {
        // Test edge cases
        let cases = [("0s", 0.0, ""), ("0ms", 0.0, ""), ("0.0h", 0.0, "")];

        for (input, expected_duration, expected_remaining) in cases {
            let result = scan_duration_segment(input).unwrap();
            assert_eq!(
                result.0.as_f64(),
                expected_duration,
                "Duration mismatch for {}: got {}, expected {}",
                input,
                result.0.as_f64(),
                expected_duration
            );
            assert_eq!(
                result.1, expected_remaining,
                "Remaining string mismatch for {}: got '{}', expected '{}'",
                input, result.1, expected_remaining
            );
        }
    }

    #[test]
    fn test_duration_success() {
        fn f(s: &str, expected: f64) {
            let d = parse_duration_internal(s).unwrap();
            assert_eq!(
                d, expected,
                "unexpected duration; got {d}; want {expected}; expr {s}"
            )
        }

        f("1.23", 1.23);

        // Integer durations
        f("123ms", 123.0);
        f("123s", 123.0 * 1000f64);
        f("4236579305ms", 4236579305f64);
        f("123m", 123.0 * MILLIS_PER_MINUTE);
        f("1h", MILLIS_PER_HOUR);
        f("2d", 2.0 * MILLIS_PER_DAY);
        f("3w", 3.0 * MILLIS_PER_WEEK);
        f("4y", 4.0 * MILLIS_PER_YEAR);
        f("1m34s24ms", 94024f64);
        f("-1m34s24ms", -94024.0);
        f("1m-34s24ms", 25976.0);

        // Float durations
        f("34.54ms", 34.54);
        f("0.234s", 0.234 * MILLIS_PER_SECOND);
        f("1.5s", 1.5 * MILLIS_PER_SECOND);
        f("1.5m", 1.5 * MILLIS_PER_MINUTE);
        f("1.2h", 1.2 * MILLIS_PER_HOUR);
        f("1.1d", 1.1 * MILLIS_PER_DAY);
        f("1.1w", 1.1 * MILLIS_PER_WEEK);
        f("1.3y", 1.3 * MILLIS_PER_YEAR);
        f("1.5m3.4s2.4ms", 93402.4);

        // Floating-point durations without suffix.
        f("0.56", 0.56);
        f(".523e2", 0.523e2)
    }

    #[test]
    fn test_duration_error() {
        fn f(s: &str) {
            if let Ok(d) = parse_duration_value(s) {
                panic!("Expected error, got {d} for expr {s}")
            }
        }

        f("");
        f("foo");
        f("m");
        f("1.23mm");
        f("123q");

        // With uppercase duration
        f("1M");
        f("1Ms");
        f("1MS");
        f("1Y");
        f("2W");
        f("3D");
        f("3H");
        f("3S")
    }

    #[test]
    fn test_non_finite_arithmetic_is_rejected() {
        // Each prefix is finite, but the product or the sum is not: inf + -inf is NaN, which
        // used to pass the range check and come out as 0.
        for s in ["1e308y-1e308y", "1e308y", "1e308y1e308y", "1.5e308w"] {
            assert!(parse_duration_value(s).is_err(), "{s}");
            assert!(parse_positive_duration_value(s).is_err(), "{s}");
        }
    }

    #[test]
    fn test_negative_is_checked_before_rounding() {
        for s in ["-0.4ms", "-0", "-0s", "-0.0", "1ms-1.4ms"] {
            assert!(parse_positive_duration_value(s).is_err(), "{s}");
        }
        assert_eq!(parse_positive_duration_value("0").unwrap(), 0);
        assert_eq!(parse_positive_duration_value("0.4ms").unwrap(), 0);
        assert_eq!(parse_duration_value("-0.4ms").unwrap(), 0);
    }

    #[test]
    fn test_integer_durations_are_exact_to_i64_max() {
        // RTS accepts RETENTION and IGNORE up to i64::MAX exactly (black-box checked).
        for (s, expected) in [
            ("9007199254740993", 9_007_199_254_740_993),
            ("9007199254740993ms", 9_007_199_254_740_993),
            ("2305843009213693953", (1_i64 << 61) + 1),
            ("9223372036854775807", i64::MAX),
            ("106751991167d", 106_751_991_167 * 86_400_000),
            ("1y1ms", 31_536_000_001),
        ] {
            assert_eq!(parse_positive_duration_value(s).unwrap(), expected, "{s}");
        }
        assert_eq!(
            parse_duration_value("-9223372036854775807").unwrap(),
            -i64::MAX
        );
        for s in [
            "9223372036854775808",
            "-9223372036854775808",
            "9223372036854775807ms1ms",
            "106751991168d",
            "1e19",
        ] {
            assert!(parse_duration_value(s).is_err(), "{s}");
        }
    }

    #[test]
    fn test_sticky_sign_applies_to_later_segments() {
        assert_eq!(parse_duration_value("2h-5m5s").unwrap(), 6_895_000);
        assert_eq!(parse_duration_value("-1m30s").unwrap(), -90_000);
    }

    #[test]
    fn test_duration_unicode_prefix_returns_error() {
        assert!(parse_duration_value("éé1s").is_err());
    }

    #[test]
    fn test_positive_duration_error() {
        fn f(s: &str) {
            if parse_positive_duration_value(s).is_ok() {
                panic!("Expecting an error for duration {s}")
            }
        }
        f("");
        f("foo");
        f("m");
        f("1.23mm");
        f("123q");
        f("-123s");

        // Too big duration
        f("10000000000y")
    }
}
