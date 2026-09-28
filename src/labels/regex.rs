use crate::parser::ParseError;
use regex::{Regex, RegexBuilder};

/// Sets the approximate size limit, in bytes, of the compiled regex.
///
/// 64 KiB, raised from 16 KiB on 2026-09-28: 16 KiB refused everyday patterns
/// such as `\w+`, `\pL+` and `(?i)\w+` (one Unicode word class is most of it).
/// Refusing a heavy pattern still takes well under a millisecond, and the
/// worst query of admitted patterns that fits the default 4 KiB query length
/// costs about 75 ms to check (measurements in
/// `docs/plans/promql-threads-review-2026-09-28.md`, section 2.1).
const REGEX_SIZE_LIMIT: usize = 64 * 1024;
/// Sets the approximate size limit, in bytes, of the cache used by the lazy DFA at match time.
const DFA_SIZE_LIMIT: usize = 16 * 1024;
/// The compiled size limit for [`compile_literal_set`]. Its callers bound the
/// pattern text instead, which is what actually bounds the cost: the parse
/// that runs before any size check takes ~260 bytes of transient heap per
/// byte of pattern, whatever the limit. This only has to admit the largest
/// set they allow (8 KiB of escaped text needs well under 1 MiB).
const LITERAL_SET_SIZE_LIMIT: usize = 2 * 1024 * 1024;

/// remove_start_end_anchors removes '^' at the start of expr and '$' at the end of the expr.
pub fn remove_start_end_anchors(expr: &str) -> &str {
    let mut cursor = expr;
    while let Some(t) = cursor.strip_prefix('^') {
        cursor = t;
    }
    while cursor.ends_with("$") && !cursor.ends_with("\\$") {
        if let Some(t) = cursor.strip_suffix("$") {
            cursor = t;
        } else {
            break;
        }
    }
    cursor
}

/// Go and Rust handle the repeat pattern differently
/// in Go the following is valid: `aaa{bbb}ccc`
/// in Rust {bbb} is seen as an invalid repeat and must be escaped \{bbb}
/// This escapes the opening "{" if it's not followed by valid repeat pattern (e.g. 4,6).
pub fn try_escape_for_repeat_re(re: &str) -> String {
    fn is_repeat(chars: &mut std::str::Chars<'_>) -> (bool, String) {
        let mut buf = String::new();
        let mut comma_seen = false;
        for c in chars.by_ref() {
            buf.push(c);
            match c {
                ',' if comma_seen => {
                    return (false, buf); // ",," is invalid
                }
                ',' if buf == "," => {
                    return (false, buf); // {, is invalid
                }
                ',' if !comma_seen => comma_seen = true,
                '}' if buf == "}" => {
                    return (false, buf); // {} is invalid
                }
                '}' => {
                    return (true, buf);
                }
                _ if c.is_ascii_digit() => continue,
                _ => {
                    return (false, buf); // false if visit non-digit char
                }
            }
        }
        (false, buf) // not ended with "}"
    }

    let mut result = String::with_capacity(re.len() + 1);
    let mut chars = re.chars();

    while let Some(c) = chars.next() {
        match c {
            '\\' => {
                if let Some(cc) = chars.next() {
                    result.push(c);
                    result.push(cc);
                }
            }
            '{' => {
                let (is, s) = is_repeat(&mut chars);
                if !is {
                    result.push('\\');
                }
                result.push(c);
                result.push_str(&s);
            }
            _ => result.push(c),
        }
    }
    result
}

/// Parse and potentially transform the regex.
///
/// Go and Rust handle the repeat pattern differently,
/// in Go the following is valid: `aaa{bbb}ccc` but
/// in Rust {bbb} is seen as an invalid repeat and must be escaped as \{bbb}.
/// This escapes the opening "{" if it's not followed by a valid repeat pattern (e.g., 4,6).
///
/// Regexes used in PromQL are fully anchored.
fn build(re: &str) -> Result<Regex, regex::Error> {
    // flags to match Prometheus' behavior
    RegexBuilder::new(re)
        .size_limit(REGEX_SIZE_LIMIT)
        .dfa_size_limit(DFA_SIZE_LIMIT)
        .dot_matches_new_line(true)
        .build()
}

/// Compiles `re`, retrying with Go-style `{...}` repeats escaped if the first attempt fails to
/// parse. Both attempts go through `build`, so the size limit applies on the retry too.
pub(crate) fn build_with_repeat_fallback(re: &str) -> Result<Regex, regex::Error> {
    build(re).or_else(|_| build(&try_escape_for_repeat_re(re)))
}

/// The anchored regex for `alternation`, a `|`-joined list of escaped literals
/// whose length the caller has bounded — a derived push-down filter. The
/// general `REGEX_SIZE_LIMIT` would reject a dozen host names; the index
/// never runs this regex anyway (it looks the values up), so it only has to
/// exist for the PromQL matcher that carries it.
pub fn compile_literal_set(alternation: &str) -> Result<Regex, regex::Error> {
    RegexBuilder::new(&format!("^(?:{alternation})$"))
        .size_limit(LITERAL_SET_SIZE_LIMIT)
        .dfa_size_limit(DFA_SIZE_LIMIT)
        .dot_matches_new_line(true)
        .build()
}

fn try_parse_re(original_re: &str) -> Result<Regex, ParseError> {
    let re = format!("^(?:{original_re})$",);

    build_with_repeat_fallback(&re).map_err(|_| ParseError::InvalidRegex(original_re.to_string()))
}

pub fn parse_regex_anchored(value: &str) -> Result<(Regex, &str), ParseError> {
    // ensure all regexes are anchored
    let unanchored = remove_start_end_anchors(value);
    let regex = try_parse_re(unanchored)?;
    Ok((regex, unanchored))
}

#[cfg(test)]
mod tests {
    use crate::labels::regex::try_escape_for_repeat_re;

    #[test]
    fn test_convert_re() {
        assert_eq!(try_escape_for_repeat_re("abc{}"), r"abc\{}");
        assert_eq!(try_escape_for_repeat_re("abc{def}"), r"abc\{def}");
        assert_eq!(try_escape_for_repeat_re("abc{def"), r"abc\{def");
        assert_eq!(try_escape_for_repeat_re("abc{1}"), "abc{1}");
        assert_eq!(try_escape_for_repeat_re("abc{1,}"), "abc{1,}");
        assert_eq!(try_escape_for_repeat_re("abc{1,2}"), "abc{1,2}");
        assert_eq!(try_escape_for_repeat_re("abc{,2}"), r"abc\{,2}");
        assert_eq!(try_escape_for_repeat_re("abc{{1,2}}"), r"abc\{{1,2}}");
        assert_eq!(try_escape_for_repeat_re(r"abc\{abc"), r"abc\{abc");
        assert_eq!(try_escape_for_repeat_re("abc{1a}"), r"abc\{1a}");
        assert_eq!(try_escape_for_repeat_re("abc{1,a}"), r"abc\{1,a}");
        assert_eq!(try_escape_for_repeat_re("abc{1,2a}"), r"abc\{1,2a}");
        assert_eq!(try_escape_for_repeat_re("abc{1,2,3}"), r"abc\{1,2,3}");
        assert_eq!(try_escape_for_repeat_re("abc{1,,2}"), r"abc\{1,,2}");
    }
}
