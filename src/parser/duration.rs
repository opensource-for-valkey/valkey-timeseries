use crate::parser::{ParseError, ParseResult};
use std::str;

const MILLIS_PER_SECOND: f64 = 1e3;
const MILLIS_PER_MINUTE: f64 = 60.0 * MILLIS_PER_SECOND;
const MILLIS_PER_HOUR: f64 = 60.0 * MILLIS_PER_MINUTE;
const MILLIS_PER_DAY: f64 = 24.0 * MILLIS_PER_HOUR;
const MILLIS_PER_WEEK: f64 = 7.0 * MILLIS_PER_DAY;
const MILLIS_PER_YEAR: f64 = 365.0 * MILLIS_PER_DAY;

/// `positive_duration_value` returns positive duration in milliseconds for the given s
/// and the given step.
///
/// Duration in s may be combined, i.e., 2h5m or 2h-5m.
///
/// Error is returned if the duration in s is negative.
pub fn parse_positive_duration_value(s: &str) -> Result<i64, ParseError> {
    let d = parse_duration_value(s)?;
    if d < 0 {
        return Err(ParseError::InvalidDuration(format!(
            "duration cannot be negative; got {s}"
        )));
    }
    Ok(d)
}

fn validate_duration(ms: f64) -> ParseResult<f64> {
    if ms.abs() > (1_i64 << (62 - 1)) as f64 {
        let msg = format!("duration ({ms}) is too large");
        return Err(ParseError::General(msg));
    }
    Ok(ms)
}

/// `parse_duration_value` returns the duration in milliseconds for the given s
///
/// Duration in s may be combined, i.e., 2h5m, -2h5m or 2h-5m.
///
/// The returned duration value can be negative.
pub fn parse_duration_internal(s: &str) -> ParseResult<f64> {
    let mut duration: f64 = 0.0;
    let mut s = s;
    let mut is_minus = false;

    // todo: duration segments should go from courser to finer grain
    // i.e. we should forbid something like 25s2d

    if s.is_empty() {
        return Err(ParseError::InvalidDuration(r##""""##.to_string()));
    }

    while !s.is_empty() {
        let cursor = match scan_duration_segment(s) {
            Ok((mut value, cursor)) => {
                if is_minus && (value > 0.0) {
                    value = -value
                }
                duration += value;
                if value < 0f64 {
                    is_minus = true
                }
                cursor
            }
            Err(e) => return Err(e),
        };
        s = cursor;
    }

    validate_duration(duration)
}

pub fn parse_duration_value(s: &str) -> ParseResult<i64> {
    Ok(parse_duration_internal(s)?.round() as i64)
}

fn scan_duration_segment(s: &str) -> ParseResult<(f64, &str)> {
    fn parse_prefix(str: &str, end: usize) -> ParseResult<f64> {
        let num_str = &str[..end];
        match num_str.parse::<f64>() {
            Ok(num) => Ok(num),
            Err(_) => Err(ParseError::InvalidDuration(num_str.to_string())),
        }
    }

    let mut suffix_start = s.len();
    let mut suffix_char = None;

    for (index, ch) in s.char_indices() {
        if matches!(ch, 'd' | 'h' | 'm' | 's' | 'w' | 'y') {
            suffix_start = index;
            suffix_char = Some(ch);
            break;
        }
    }

    let number_part = parse_prefix(s, suffix_start)?;
    let (multiplier, suffix_len) = match suffix_char {
        Some('m') if s[suffix_start..].starts_with("ms") => (1.0, 2),
        Some('m') => (MILLIS_PER_MINUTE, 1),
        Some('s') => (MILLIS_PER_SECOND, 1),
        Some('h') => (MILLIS_PER_HOUR, 1),
        Some('d') => (MILLIS_PER_DAY, 1),
        Some('w') => (MILLIS_PER_WEEK, 1),
        Some('y') => (MILLIS_PER_YEAR, 1),
        Some(_) => (1.0, 0),
        None => (1.0, 0),
    };

    let duration = number_part * multiplier;
    let remaining = &s[suffix_start + suffix_len..];
    Ok((duration, remaining))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parser::duration::{
        MILLIS_PER_DAY, MILLIS_PER_MINUTE, MILLIS_PER_SECOND, parse_duration_value,
        parse_positive_duration_value, scan_duration_segment,
    };

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
                result.0, expected_duration,
                "Duration mismatch for {}: got {}, expected {}",
                input, result.0, expected_duration
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
                result.0, expected_duration,
                "Duration mismatch for {}: got {}, expected {}",
                input, result.0, expected_duration
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
                result.0, expected_duration,
                "Duration mismatch for {}: got {}, expected {}",
                input, result.0, expected_duration
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
                result.0, expected_duration,
                "Duration mismatch for {}: got {}, expected {}",
                input, result.0, expected_duration
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
                result.0, expected_duration,
                "Duration mismatch for {}: got {}, expected {}",
                input, result.0, expected_duration
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
