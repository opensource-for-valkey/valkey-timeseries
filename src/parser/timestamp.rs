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

/// Parses a string into a unix timestamp (milliseconds). Accepts a positive integer or an RFC3339 timestamp.
/// Included here only to avoid having to include chrono in the public API
pub fn parse_timestamp(s: &str, auto_scale: bool) -> ParseResult<i64> {
    let value = if let Ok(dt) = parse_numeric_timestamp(s, auto_scale) {
        dt
    } else {
        let value =
            DateTime::parse_str(s).map_err(|_| ParseError::InvalidTimestamp(s.to_string()))?;
        value.timestamp_ms()
    };
    if value < 0 {
        return Err(ParseError::NegativeTimestamp(s.to_string()));
    }
    Ok(value)
}

/// `parse_numeric_timestamp` parses timestamp at s in seconds, milliseconds, microseconds or nanoseconds.
///
/// It returns milliseconds for the parsed timestamp.
pub fn parse_numeric_timestamp(
    s: &str,
    auto_scale: bool,
) -> Result<i64, Box<dyn std::error::Error>> {
    const CHARS_TO_CHECK: &[char] = &['.', 'e', 'E'];

    if s.contains(CHARS_TO_CHECK) {
        // Unix timestamps in seconds with optional milliseconds after the point. For example, 1562529662.678.
        let ts: f64 = s.parse()?;
        // `1e999` parses as infinity, which would saturate to `i64::MAX` below.
        if !ts.is_finite() {
            return Err(format!("timestamp is not finite: {s}").into());
        }
        if ts >= u32::MAX as f64 {
            // The timestamp is in milliseconds
            return Ok(ts.round() as i64);
        }
        let ts = (ts * 1000.0).round();
        return Ok(ts as i64);
    }
    // The timestamp is an integer number
    let ts: i64 = s.parse()?;
    if !auto_scale {
        return Ok(ts);
    }
    match ts {
        ts if ts >= (1 << 32) * 1_000_000 => {
            // The timestamp is in nanoseconds
            Ok(ts / 1_000_000)
        }
        ts if ts >= (1 << 32) * 1_000 => {
            // The timestamp is in microseconds
            Ok(ts / 1_000)
        }
        ts if ts >= (1 << 32) => {
            // The timestamp is in milliseconds
            Ok(ts)
        }
        _ => Ok(ts * 1_000), // seconds
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_non_finite_numeric_timestamps_are_rejected() {
        for s in ["1e999", "-1e999", "nan", "NaN", "inf", "1.5e400"] {
            assert!(parse_timestamp(s, false).is_err(), "{s}");
        }
    }

    #[test]
    fn test_fractional_timestamps_are_seconds() {
        // DIV-0041: a fraction or exponent means Unix seconds; a plain integer milliseconds.
        assert_eq!(parse_timestamp("1000", false).unwrap(), 1000);
        assert_eq!(parse_timestamp("1.5", false).unwrap(), 1500);
        assert_eq!(parse_timestamp("1000.0", false).unwrap(), 1_000_000);
    }
}
