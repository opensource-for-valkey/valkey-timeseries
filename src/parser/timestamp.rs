use crate::error_consts;
use crate::parser::parse_error::{ParseError, ParseResult};
use speedate::DateTime;

/// Maps a timestamp parse failure to the client-facing message.
///
/// RedisTimeSeries reports a well-formed but negative timestamp differently from one it
/// could not parse at all, so the two cases must not collapse into a single message.
/// The range family deliberately ignores this and reports a bad bound positionally
/// (`wrong fromTimestamp` / `wrong toTimestamp`) whatever the underlying cause.
pub fn timestamp_error(err: &ParseError) -> &'static str {
    match err {
        ParseError::NegativeTimestamp(_) => error_consts::NEGATIVE_TIMESTAMP,
        _ => error_consts::INVALID_TIMESTAMP,
    }
}

/// Parses a string into a unix timestamp (milliseconds). Accepts a nonnegative integer
/// (milliseconds), a number with a fraction or exponent (Unix seconds below 2^32,
/// milliseconds at or above it; DIV-0041), or an RFC3339 timestamp.
///
/// A negative value is [`ParseError::NegativeTimestamp`]; a value outside `0..=i64::MAX`
/// milliseconds after scaling, and anything unparseable, is [`ParseError::InvalidTimestamp`].
/// Included here only to avoid having to include chrono in the public API
pub fn parse_timestamp(s: &str) -> ParseResult<i64> {
    let value = match parse_numeric_timestamp(s) {
        Ok(ts) => return Ok(ts),
        Err(NumericTimestampError::Negative) => {
            return Err(ParseError::NegativeTimestamp(s.to_string()));
        }
        Err(NumericTimestampError::OutOfRange) => {
            return Err(ParseError::InvalidTimestamp(s.to_string()));
        }
        Err(NumericTimestampError::NotNumeric) => DateTime::parse_str(s)
            .map_err(|_| ParseError::InvalidTimestamp(s.to_string()))?
            .timestamp_ms(),
    };
    if value < 0 {
        return Err(ParseError::NegativeTimestamp(s.to_string()));
    }
    Ok(value)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum NumericTimestampError {
    /// Not a number; the caller tries RFC3339 next.
    NotNumeric,
    Negative,
    /// Not finite, or outside the `i64` millisecond range once scaled.
    OutOfRange,
}

/// Values with a fraction or exponent at or above this are already milliseconds (DIV-0041).
const SECONDS_LIMIT: f64 = 4_294_967_296.0; // 2^32

/// Exclusive upper bound of `i64` as an `f64` (2^63; `i64::MAX` itself is not representable).
const I64_LIMIT: f64 = 9_223_372_036_854_775_808.0;

/// Parses a numeric timestamp, returning milliseconds.
///
/// A plain integer is milliseconds. A number with a fraction or exponent is Unix seconds
/// (`1562529662.678`), unless it is at or above 2^32, when it is taken as milliseconds. The
/// sign is checked before rounding, so `-0.0001` is negative rather than 0, and the bound is
/// checked before the cast, which would otherwise saturate `1e100` to `i64::MAX`.
fn parse_numeric_timestamp(s: &str) -> Result<i64, NumericTimestampError> {
    use NumericTimestampError::*;

    if !s.contains(['.', 'e', 'E']) {
        let ts: i64 = s.parse().map_err(|_| NotNumeric)?;
        return if ts < 0 { Err(Negative) } else { Ok(ts) };
    }

    let ts: f64 = s.parse().map_err(|_| NotNumeric)?;
    // `1e999` parses as infinity.
    if !ts.is_finite() {
        return Err(OutOfRange);
    }
    if ts < 0.0 {
        return Err(Negative);
    }
    let ms = if ts >= SECONDS_LIMIT { ts } else { ts * 1000.0 }.round();
    if ms >= I64_LIMIT {
        return Err(OutOfRange);
    }
    Ok(ms as i64)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_non_finite_numeric_timestamps_are_rejected() {
        for s in ["1e999", "-1e999", "nan", "NaN", "inf", "1.5e400"] {
            assert!(parse_timestamp(s).is_err(), "{s}");
        }
    }

    #[test]
    fn test_fractional_timestamps_are_seconds() {
        // DIV-0041: a fraction or exponent means Unix seconds; a plain integer milliseconds.
        assert_eq!(parse_timestamp("1000").unwrap(), 1000);
        assert_eq!(parse_timestamp("1.5").unwrap(), 1500);
        assert_eq!(parse_timestamp("1000.0").unwrap(), 1_000_000);
    }

    #[test]
    fn test_out_of_range_timestamps_are_rejected_not_saturated() {
        for s in [
            "1e100",
            "9223372036854775808.0",
            "9.3e18",
            "9223372036854775808",
        ] {
            assert!(
                matches!(parse_timestamp(s), Err(ParseError::InvalidTimestamp(_))),
                "{s}: {:?}",
                parse_timestamp(s)
            );
        }
        assert_eq!(parse_timestamp("9223372036854775807").unwrap(), i64::MAX);
        // The largest f64 below 2^63.
        assert_eq!(
            parse_timestamp("9223372036854774784.0").unwrap(),
            9_223_372_036_854_774_784
        );
    }

    #[test]
    fn test_negative_is_checked_before_rounding() {
        for s in ["-0.0001", "-1", "-1.5", "-1e3"] {
            assert!(
                matches!(parse_timestamp(s), Err(ParseError::NegativeTimestamp(_))),
                "{s}"
            );
        }
        assert_eq!(parse_timestamp("-0.0").unwrap(), 0);
        assert_eq!(parse_timestamp("0.0001").unwrap(), 0);
    }

    #[test]
    fn test_seconds_threshold_is_two_to_the_32() {
        // DIV-0041: below 2^32 a fractional value is seconds, at or above it milliseconds.
        assert_eq!(parse_timestamp("4294967295.0").unwrap(), 4_294_967_295_000);
        assert_eq!(parse_timestamp("4294967295.5").unwrap(), 4_294_967_295_500);
        assert_eq!(parse_timestamp("4294967296.0").unwrap(), 4_294_967_296);
        assert_eq!(parse_timestamp("1e10").unwrap(), 10_000_000_000);
        assert_eq!(parse_timestamp("1e9").unwrap(), 1_000_000_000_000);
    }

    #[test]
    fn test_rfc3339_still_parses() {
        assert_eq!(parse_timestamp("1970-01-01T00:00:01Z").unwrap(), 1000);
        assert!(matches!(
            parse_timestamp("1969-12-31T23:59:59Z"),
            Err(ParseError::NegativeTimestamp(_))
        ));
    }
}
