use crate::parser::{ParseError, ParseResult};
use std::borrow::Cow;

/// Whether `name` can be written unquoted where the parsers take a name: it lexes as one
/// identifier (`[_a-zA-Z][_a-zA-Z0-9:.-]*`, the lexer's rule) and is not the keyword `or`, which
/// the lexer matches in any case. Anything else is written as a quoted string.
pub fn is_bare_name(name: &str) -> bool {
    let mut chars = name.chars();
    let starts_well = chars
        .next()
        .is_some_and(|c| c == '_' || c.is_ascii_alphabetic());
    starts_well
        && chars.all(|c| c.is_ascii_alphanumeric() || matches!(c, '_' | ':' | '.' | '-'))
        && !name.eq_ignore_ascii_case("or")
}

/// Writes `name` as the parsers read it back: bare when [`is_bare_name`], quoted otherwise.
pub fn write_name(f: &mut impl std::fmt::Write, name: &str) -> std::fmt::Result {
    if is_bare_name(name) {
        f.write_str(name)
    } else {
        f.write_str(&quote_string(name))
    }
}

/// Decodes a quoted PromQL string literal (the token, quotes included), with the escaping rules
/// Prometheus uses, which are Go's:
///
/// - Backtick strings are raw: no escapes, and no backtick inside.
/// - Single- and double-quoted strings are treated alike, except that each forbids its own
///   quote unescaped. Neither may contain a raw newline. A backslash starts one of `\a \b \f \n
///   \r \t \v \\`, the escaped enclosing quote (`\'` only in `'...'`, `\"` only in `"..."`),
///   `\xHH` or `\NNN` (one byte, hex or three octal digits), or `\uHHHH` / `\UHHHHHHHH` (a
///   Unicode scalar value). Byte escapes may combine into UTF-8 (`\xc3\xa9` is `é`); the
///   result must be valid UTF-8. Anything else is an error.
///
/// See <https://prometheus.io/docs/prometheus/latest/querying/basics/#string-literals>.
pub fn extract_string_value(token: &'_ str) -> ParseResult<Cow<'_, str>> {
    let bytes = token.as_bytes();
    let n = bytes.len();
    if n < 2 || bytes[0] != bytes[n - 1] {
        return Err(ParseError::SyntaxError(format!(
            "invalid quoted string literal: {token}"
        )));
    }
    let body = &token[1..n - 1];
    match bytes[0] {
        b'`' if body.contains('`') => Err(ParseError::SyntaxError(
            "a backtick string may not contain a backtick".to_string(),
        )),
        b'`' => Ok(Cow::Borrowed(body)),
        quote @ (b'"' | b'\'') => unquote_escaped(body, quote as char),
        other => Err(ParseError::SyntaxError(format!(
            "invalid quote character {}",
            other as char
        ))),
    }
}

fn syntax(msg: impl Into<String>) -> ParseError {
    ParseError::SyntaxError(msg.into())
}

/// Decodes the body of a `'...'` or `"..."` literal in one pass; see [`extract_string_value`].
fn unquote_escaped(body: &str, quote: char) -> ParseResult<Cow<'_, str>> {
    if body.contains('\n') {
        return Err(syntax(
            "a quoted string may not contain a newline; write \\n",
        ));
    }
    if !body.contains(['\\', quote]) {
        return Ok(Cow::Borrowed(body));
    }

    let mut out = Vec::with_capacity(body.len());
    let mut utf8 = [0u8; 4];
    let mut chars = body.chars();
    while let Some(c) = chars.next() {
        if c == quote {
            return Err(syntax(format!(
                "unescaped {quote} in a {quote}-quoted string"
            )));
        }
        if c != '\\' {
            out.extend_from_slice(c.encode_utf8(&mut utf8).as_bytes());
            continue;
        }
        let Some(escape) = chars.next() else {
            return Err(syntax("string literal ends with a lone backslash"));
        };
        let byte = match escape {
            'a' => 0x07,
            'b' => 0x08,
            'f' => 0x0c,
            'n' => b'\n',
            'r' => b'\r',
            't' => b'\t',
            'v' => 0x0b,
            '\\' => b'\\',
            '\'' | '"' if escape == quote => escape as u8,
            '0'..='7' => {
                let rest = take_digits(&mut chars, 2, 8)
                    .ok_or_else(|| syntax("an octal escape takes three digits 0-7"))?;
                let value = escape.to_digit(8).unwrap() * 64 + rest;
                u8::try_from(value).map_err(|_| syntax("octal escape above \\377"))?
            }
            'x' => take_digits(&mut chars, 2, 16)
                .ok_or_else(|| syntax("\\x takes two hex digits"))? as u8,
            'u' | 'U' => {
                let len = if escape == 'u' { 4 } else { 8 };
                let ch = take_digits(&mut chars, len, 16)
                    .and_then(char::from_u32)
                    .ok_or_else(|| syntax(format!("invalid \\{escape} escape")))?;
                out.extend_from_slice(ch.encode_utf8(&mut utf8).as_bytes());
                continue;
            }
            other => return Err(syntax(format!("unknown escape \\{other}"))),
        };
        out.push(byte);
    }
    String::from_utf8(out)
        .map(Cow::Owned)
        .map_err(|_| syntax("string literal is not valid UTF-8"))
}

/// Takes exactly `n` digits of `radix` and returns their value.
fn take_digits(chars: &mut std::str::Chars, n: usize, radix: u32) -> Option<u32> {
    (0..n).try_fold(0u32, |acc, _| {
        let digit = chars.next()?.to_digit(radix)?;
        Some(acc * radix + digit)
    })
}

/// Renders `s` as a double-quoted literal that [`extract_string_value`] reads back as `s`:
/// `"` and `\` are escaped, as are `\n`, `\r`, `\t` and every other control character
/// (`\xHH` below 0x20 and for 0x7f, `\uHHHH` for the C1 range). Other text is kept as is.
pub fn quote_string(s: &str) -> String {
    use std::fmt::Write;

    let mut out = String::with_capacity(s.len() + 2);
    out.push('"');
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if (c as u32) < 0x20 || c as u32 == 0x7f => {
                let _ = write!(out, "\\x{:02x}", c as u32);
            }
            c if c.is_control() => {
                let _ = write!(out, "\\u{:04x}", c as u32);
            }
            c => out.push(c),
        }
    }
    out.push('"');
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extract_string_value_empty_string() {
        let result = extract_string_value("\"\"");
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), "");
    }

    #[test]
    fn test_extract_string_value_simple_string() {
        let result = extract_string_value("\"hello\"");
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), "hello");
    }

    #[test]
    fn test_extract_string_value_string_with_escaped_quote() {
        let result = extract_string_value("\"hello\\\"world\"");
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), "hello\"world");
    }

    #[test]
    fn test_extract_string_value_string_with_newline() {
        let result = extract_string_value("\"hello\\nworld\"");
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), "hello\nworld");
    }

    #[test]
    fn test_extract_string_value_string_with_backslash() {
        let result = extract_string_value("\"hello\\\\world\"");
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), "hello\\world");
    }

    #[test]
    fn test_extract_string_value_unterminated_string() {
        let result = extract_string_value("\"hello");
        assert!(result.is_err());
    }

    #[test]
    fn test_extract_string_value_invalid_quote_character() {
        let result = extract_string_value("hello");
        assert!(result.is_err());
    }

    #[test]
    fn test_extract_string_value_backquoted_string() {
        let result = extract_string_value("`hello`");
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), "hello");
    }

    #[test]
    fn test_extract_string_value_single_quoted_string() {
        let result = extract_string_value("'hello'");
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), "hello");
    }

    #[test]
    fn test_extract_string_value_string_with_invalid_escape() {
        let result = extract_string_value("\"hello\\xworld\"");
        assert!(result.is_err());
    }

    #[test]
    fn test_extract_string_value_hex_escape() {
        let result = extract_string_value("\"hello\\x20world\"");
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), "hello world");
    }

    #[test]
    fn test_extract_string_value_unicode_escape() {
        let result = extract_string_value("\"hello\\u0020world\"");
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), "hello world");
    }

    #[test]
    fn test_extract_string_value_unicode_escape_long() {
        let result = extract_string_value("\"hello\\U00000020world\"");
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), "hello world");
    }

    #[test]
    fn test_extract_string_value_invalid_hex_escape() {
        let result = extract_string_value("\"hello\\x2Gworld\"");
        assert!(result.is_err());
    }

    #[test]
    fn test_extract_string_value_invalid_unicode_escape() {
        let result = extract_string_value("\"hello\\u002Gworld\"");
        assert!(result.is_err());
    }

    #[test]
    fn test_quotes_follow_go_rules() {
        // Each quote style escapes only its own quote, and allows the other one raw.
        for (token, expected) in [
            (r#"'a\'b'"#, "a'b"),
            (r#"'a"b'"#, "a\"b"),
            (r#""a\"b""#, "a\"b"),
            (r#""a'b""#, "a'b"),
            (r#"'a\\b'"#, "a\\b"),
            (r#""\101\x41\u0041\U00000041""#, "AAAA"),
            (r#""\xc3\xa9 \303\251""#, "é é"),
            (r#""\a\b\f\v""#, "\x07\x08\x0c\x0b"),
            ("`C:\\`", "C:\\"),
            ("`a\\nb`", "a\\nb"),
            ("`a\nb`", "a\nb"),
        ] {
            assert_eq!(extract_string_value(token).unwrap(), expected, "{token}");
        }
        for token in [
            r#"'a\"b'"#,
            r#""a\'b""#,
            "\"a\nb\"",
            "'a\nb'",
            r#""\q""#,
            r#""\8""#,
            r#""\400""#,
            r#""\12""#,
            r#""\xff""#,
            r#""\ud800""#,
            r#""\U00110000""#,
            r#""a\""#,
            "`a`b`",
        ] {
            assert!(
                extract_string_value(token).is_err(),
                "{token}: {:?}",
                extract_string_value(token)
            );
        }
    }

    #[test]
    fn test_quote_string_round_trips() {
        for s in [
            "",
            "plain",
            "say \"hi\"",
            "a\\b",
            "a\nb",
            "tab\there",
            "\r",
            "\x00\x1f\x7f",
            "\u{85}",
            "café 😀",
            "{x=\"y\"},z",
            "'single'",
            "`tick`",
            "\\\"",
        ] {
            let quoted = quote_string(s);
            assert_eq!(extract_string_value(&quoted).unwrap(), s, "{quoted}");
        }
    }

    #[test]
    fn test_extract_string_value_invalid_unicode_escape_long() {
        let result = extract_string_value("\"hello\\U0000002Gworld\"");
        assert!(result.is_err());
    }
}
