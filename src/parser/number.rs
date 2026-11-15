use crate::parser::ParseError::InvalidNumber;
use crate::parser::ParseResult;
use std::borrow::Cow;

/// Removes `_` digit separators. Each must sit between two digits of `radix` (`1_000`,
/// `0xff_ff`); a leading, trailing or doubled one, or one next to `.` or `e`, is an error.
fn strip_separators(s: &str, radix: u32) -> Option<Cow<'_, str>> {
    if !s.contains('_') {
        return Some(Cow::Borrowed(s));
    }
    let bytes = s.as_bytes();
    let is_digit = |i: usize| bytes.get(i).is_some_and(|b| (*b as char).is_digit(radix));
    for (i, b) in bytes.iter().enumerate() {
        if *b == b'_' && !(i > 0 && is_digit(i - 1) && is_digit(i + 1)) {
            return None;
        }
    }
    Some(Cow::Owned(s.replace('_', "")))
}

fn from_str_radix(digits: &str, radix: u32) -> ParseResult<f64> {
    let invalid = || InvalidNumber(digits.to_string());
    // `u64::from_str_radix` takes a leading `+`, so `0x+5` would be 5.
    if !digits.starts_with(|c: char| c.is_digit(radix)) {
        return Err(invalid());
    }
    let digits = strip_separators(digits, radix).ok_or_else(invalid)?;
    u64::from_str_radix(&digits, radix)
        .map(|value| value as f64)
        .map_err(|_| invalid())
}

fn parse_basic(str: &str) -> ParseResult<f64> {
    let (number, multiplier) = match get_number_suffix(str) {
        Some((ending, mult)) => (&str[..str.len() - ending.len()], mult),
        None => (str, 1),
    };
    let value = strip_separators(number, 10)
        .and_then(|number| number.parse::<f64>().ok())
        .ok_or_else(|| InvalidNumber(str.to_string()))?;
    Ok(if multiplier > 1 {
        value * multiplier as f64
    } else {
        value
    })
}

/// Parses an unsigned number: a decimal with optional fraction, exponent, `_` separators and
/// unit suffix (`2.5k`, `4KiB`); a `0x`, `0o` or `0b` integer; `inf`; or `nan`. A leading
/// zero is decimal. A sign is an error here; [`parse_number`] reads at most one.
pub fn parse_magnitude(str: &str) -> ParseResult<f64> {
    let invalid = || InvalidNumber(str.to_string());
    let Some(ch) = str.chars().next() else {
        return Err(invalid());
    };
    match ch {
        // `f64::from_str` would take a second sign, so `--12` was 12 and `+-12` was -12.
        '+' | '-' => Err(invalid()),
        'i' | 'I' if str.eq_ignore_ascii_case("inf") => Ok(f64::INFINITY),
        'n' | 'N' if str.eq_ignore_ascii_case("nan") => Ok(f64::NAN),
        // Other words, `infinity` included, are not numbers here.
        c if c.is_ascii_alphabetic() => Err(invalid()),
        '0' => match str.as_bytes().get(1) {
            None => Ok(0.0),
            Some(b'b' | b'B') => from_str_radix(&str[2..], 2),
            Some(b'o' | b'O') => from_str_radix(&str[2..], 8),
            Some(b'x' | b'X') => from_str_radix(&str[2..], 16),
            // A leading zero is decimal (`010` is 10, `09` is 9), as `strtod` reads it and
            // so RedisTimeSeries does. Go-style octal turned `FILTER_BY_VALUE 010` into 8.
            _ => parse_basic(str),
        },
        _ => parse_basic(str),
    }
}

/// Parses a number with at most one leading sign; see [`parse_magnitude`] for the rest.
///
/// NaN and both infinities are returned as such: whether an operand may be one is the
/// caller's policy (`FILTER_BY_VALUE` and `IGNORE` reject NaN, numeric configuration values
/// require a finite number, `CHUNK_SIZE` requires an integer in range).
pub fn parse_number(str: &str) -> ParseResult<f64> {
    let (negative, magnitude) = match str.as_bytes().first() {
        Some(b'-') => (true, &str[1..]),
        Some(b'+') => (false, &str[1..]),
        _ => (false, str),
    };
    parse_magnitude(magnitude)
        .map(|value| if negative { -value } else { value })
        .map_err(|_| InvalidNumber(str.to_string()))
}

type SuffixValue = (&'static str, usize);

fn get_suffix_value(s: &str) -> Option<SuffixValue> {
    hashify::map_ignore_case!(
        s.as_bytes(),
        SuffixValue,
        "kib" => ("kib", 1024),
        "ki" => ("ki", 1024),
        "kb" => ("kb", 1000),
        "k" => ("k", 1000),
        "mib" => ("mib", 1024 * 1024),
        "mi" => ("mi", 1024 * 1024),
        "mb" => ("mb", 1000 * 1000),
        "m" => ("m", 1000 * 1000),
        "gib" => ("gib", 1024 * 1024 * 1024),
        "gi" => ("gi", 1024 * 1024 * 1024),
        "gb" => ("gb", 1000 * 1000 * 1000),
        "g" => ("g", 1000 * 1000 * 1000),
        "tib" => ("tib", 1024 * 1024 * 1024 * 1024),
        "ti" => ("ti", 1024 * 1024 * 1024 * 1024),
        "tb" => ("tb", 1000 * 1000 * 1000 * 1000),
        "t" => ("t", 1000 * 1000 * 1000 * 1000)
    )
    .copied()
}

/// The unit suffix at the end of `s`, if any. Suffixes are at most three characters, so only the
/// tail is looked at rather than scanning the mantissa.
pub fn get_number_suffix(s: &str) -> Option<SuffixValue> {
    (1..=3.min(s.len()))
        .rev()
        .filter(|len| s.is_char_boundary(s.len() - len))
        .find_map(|len| get_suffix_value(&s[s.len() - len..]))
}

#[cfg(test)]
mod tests {
    use crate::parser::number::{parse_magnitude, parse_number};

    fn expect_failure(s: &str) {
        match parse_magnitude(s) {
            Err(..) => {}
            Ok(ns) => {
                panic!("expecting error in parse_magnitude({s}); got result {ns}")
            }
        }
    }

    #[test]
    fn test_parse_number_with_unit() {
        fn f(s: &str, expected: f64) {
            let v = parse_magnitude(s).unwrap();
            if v.is_nan() {
                assert!(
                    expected.is_nan(),
                    "unexpected value returned from parse_number_with_unit({s}); got {v}; want {expected}"
                )
            } else {
                assert_eq!(
                    v, expected,
                    "unexpected value returned from parse_number_with_unit({s}); got {v}; want {expected}"
                )
            }
        }

        f("2k", (2 * 1000) as f64);
        f("2.3Kb", 2.3 * 1000_f64);
        f("3ki", (3 * 1024) as f64);
        f("4.5Kib", 4.5 * 1024_f64);
        f("2m", (2 * 1000 * 1000) as f64);
        f("2.3Mb", 2.3 * 1000_f64 * 1000_f64);
        f("3Mi", (3_i64 * 1024 * 1024) as f64);
        f("4.5mib", 4.5 * (1024 * 1024) as f64);
        f("2G", (2_i64 * 1000 * 1000 * 1000) as f64);
        f("2.3gB", 2.3 * (1000 * 1000 * 1000) as f64);
        f("3gI", (3_i64 * 1024 * 1024 * 1024) as f64);
        f("4.5GiB", 4.5 * (1024_i64 * 1024 * 1024) as f64);
        f("2T", (2_i64 * 1000 * 1000 * 1000 * 1000) as f64);
        f("2.3tb", 2.3 * (1000_i64 * 1000 * 1000 * 1000) as f64);
        f("3tI", (3 * 1024_i64 * 1024 * 1024 * 1024) as f64);
        f("4.5TIB", 4.5 * (1024_i64 * 1024 * 1024 * 1024) as f64)
    }

    #[test]
    fn test_parse_magnitude_success() {
        fn f(s: &str, expected: f64) {
            match parse_magnitude(s) {
                Err(err) => {
                    panic!("unexpected error in parse_magnitude({s}): {err:?}")
                }
                Ok(v) => {
                    if v.is_nan() {
                        if !expected.is_nan() {
                            panic!(
                                "unexpected value returned from parse_magnitude({s}); got {v}; want {expected}"
                            )
                        }
                    } else if v != expected {
                        panic!(
                            "unexpected value returned from parse_magnitude({s}); got {v}; want {expected}"
                        )
                    }
                }
            }
        }
        f("123", 123.0);
        f("1.23", 1.23);
        f("12e5", 12e5);
        f("1.3E-3", 1.3e-3);
        f("234.", 234.0);
        f("Inf", f64::INFINITY);
        f("NaN", f64::NAN);
        f("0xfe", 0xfe as f64);
        f("0b0110", 0b0110 as f64);
        f("0O765", 0o765 as f64);
        f("0765", 765.0);
        f("010", 10.0);
        f("0010", 10.0);
        f("09", 9.0);
        f("08", 8.0);
        f("010.5", 10.5);
        f("0.010", 0.010);
        f("2k", (2 * 1000) as f64);
        f("2.3Kb", 2.3 * 1000_f64);
        f("3ki", 3.0 * 1024_f64);
        f("4.5Kib", 4.5 * 1024_f64);
        f("2m", 2.0 * (1000 * 1000) as f64);
        f("2.3Mb", 2.3 * (1000 * 1000) as f64);
        f("3Mi", 3.0 * 1024.0 * 1024.0);
        f("4.5mib", 4.5 * 1024.0 * 1024.0);
        f("2G", 2.0 * 1000.0 * 1000.0 * 1000.0);
        f("2.3gB", 2.3 * 1000.0 * 1000.0 * 1000.0);
        f("3gI", 3.0 * 1024.0 * 1024.0 * 1024.0);
        f("4.5GiB", 4.5 * 1024.0 * 1024.0 * 1024.0);
        f("2T", 2.0 * 1000.0 * 1000.0 * 1000.0 * 1000.0);
        f("2.3tb", 2.3 * 1000.0 * 1000.0 * 1000.0 * 1000.0);
        f("3tI", 3.0 * 1024.0 * 1024.0 * 1024.0 * 1024.0);
        f("4.5TIB", 4.5 * 1024.0 * 1024.0 * 1024.0 * 1024.0)
    }

    #[test]
    fn test_parse_magnitude_failure() {
        fn f(s: &str) {
            expect_failure(s)
        }
        f("");
        f("0xqwert");
        f("foobar");
        f("234.foobar");
        f("123e");
        f("1233Ebc");
        f("12.34E+abc");
        f("12.34e-");
        f("12.weKB")
    }

    #[test]
    fn test_parse_number_empty_is_error_not_panic() {
        // An empty operand reaches `parse_number` from user input (IGNORE operands,
        // `CONFIG SET ts.ts-ignore-max-val-diff ""`); it must not panic.
        assert!(parse_number("").is_err());
        // A bare sign leaves an empty remainder after the sign is stripped.
        assert!(parse_number("-").is_err());
        assert!(parse_number("+").is_err());
        // Signed parsing still works.
        assert_eq!(parse_number("-12").unwrap(), -12.0);
        assert_eq!(parse_number("+12").unwrap(), 12.0);
    }

    #[test]
    fn test_sign_is_read_once() {
        for s in ["--12", "+-12", "-+12", "++12", "- 12", "-", "+", ""] {
            assert!(parse_number(s).is_err(), "{s}: {:?}", parse_number(s));
        }
        for s in ["-12", "+12", "-1e3", "1e-3", "-inf", "+inf", "-0x10"] {
            assert!(parse_number(s).is_ok(), "{s}");
        }
        // The magnitude parser takes no sign at all.
        for s in ["-12", "+12", "-inf"] {
            assert!(parse_magnitude(s).is_err(), "{s}");
        }
        assert!(parse_number("-nan").unwrap().is_nan());
        assert!(parse_number("infinity").is_err());
        // The error names the whole operand.
        assert_eq!(
            parse_number("09x").unwrap_err(),
            super::InvalidNumber("09x".to_string())
        );
    }

    #[test]
    fn test_digit_separators() {
        for (s, expected) in [
            ("1_000", 1000.0),
            ("1_000.5", 1000.5),
            ("1.000_5", 1.0005),
            ("1_000k", 1_000_000.0),
            ("0x1_0", 16.0),
            ("0b1_0", 2.0),
            ("0o1_0", 8.0),
        ] {
            assert_eq!(parse_number(s).unwrap(), expected, "{s}");
        }
        for s in [
            "1__0", "1_", "_1", "1_.5", "1._5", "1_e3", "0x_10", "0x10_", "0x+5", "0x",
        ] {
            assert!(parse_number(s).is_err(), "{s}: {:?}", parse_number(s));
        }
    }

    #[test]
    fn test_leading_zero_is_decimal() {
        // RTS reads these as `strtod` does; checked black-box against the compat reference.
        for (s, expected) in [
            ("010", 10.0),
            ("-010", -10.0),
            ("09", 9.0),
            ("0010", 10.0),
            ("010.5", 10.5),
        ] {
            assert_eq!(parse_number(s).unwrap(), expected, "{s}");
        }
        // The explicit radix prefixes are unchanged.
        assert_eq!(parse_number("0o10").unwrap(), 8.0);
        assert_eq!(parse_number("0x10").unwrap(), 16.0);
        assert_eq!(parse_number("0b10").unwrap(), 2.0);
    }
}
