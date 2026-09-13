use crate::parser::{ParseError, ParseResult};
use enquote::unescape;
use std::borrow::Cow;
use std::num::ParseIntError;

/// Interprets `token` as a single-quoted, double-quoted, or backquoted
/// Prometheus query language string literal, returning the string value that `token`
/// quotes.
///
/// Special-casing for single quotes was removed, and single quoted strings are now treated the
/// same as double-quoted ones.
pub fn extract_string_value(token: &'_ str) -> ParseResult<Cow<'_, str>> {
    let n = token.len();

    if n < 2 {
        return Err(ParseError::SyntaxError(format!(
            "invalid quoted string literal. A minimum of 2 chars needed; got {token}"
        )));
    }

    // See https://prometheus.io/docs/prometheus/latest/querying/basics/#string-literals
    let mut quote_ch = token.chars().next().unwrap();
    if !['"', '\'', '`'].contains(&quote_ch) {
        return Err(ParseError::SyntaxError(format!(
            "invalid quote character {quote_ch}"
        )));
    }

    let last = token.chars().last().unwrap();

    if last != quote_ch {
        return Err(ParseError::SyntaxError(format!(
            "string literal contains unexpected trailing char; got {token}"
        )));
    }

    if n == 2 {
        return Ok(Cow::from(""));
    }

    let s = &token[1..n - 1];

    if quote_ch == '`' {
        if s.contains('`') {
            return Err(ParseError::SyntaxError("invalid syntax".to_string()));
        }
        return Ok(Cow::Borrowed(s));
    }

    if quote_ch == '\'' {
        let needs_unquote = s.contains(['\\', '\'', '"']);
        if !needs_unquote {
            return Ok(Cow::Borrowed(s));
        }
        let tok = s.replace("\\'", "'").replace('\"', r#"\""#);
        quote_ch = '"';
        let res = handle_unquote(tok.as_str(), quote_ch)?;
        return Ok(Cow::Owned(res));
    }

    // Is it trivial? Avoid allocation.
    if !s.contains(['\\', quote_ch]) {
        return Ok(Cow::Borrowed(s));
    }

    let res = handle_unquote(s, quote_ch)?;
    Ok(Cow::Owned(res))
}

#[inline]
fn handle_unquote(token: &str, quote: char) -> ParseResult<String> {
    match unescape(token, Some(quote)) {
        Err(err) => {
            let msg = format!("cannot parse string literal {token}: {err:?}");
            Err(ParseError::SyntaxError(msg))
        }
        Ok(s) => Ok(s),
    }
}

pub fn unescape_ident(s: &'_ str) -> ParseResult<Cow<'_, str>> {
    let v = s.find('\\');
    if v.is_none() {
        return Ok(Cow::Borrowed(s));
    }
    let mut unescaped = String::with_capacity(s.len() - 1);
    let v = v.unwrap();
    unescaped.insert_str(0, &s[..v]);
    let s = &s[v..];
    let mut chars = s.chars();

    fn map_err(_e: ParseIntError) -> ParseError {
        ParseError::SyntaxError("invalid escape sequence".to_string())
    }

    loop {
        match chars.next() {
            None => break,
            Some(c) => unescaped.push(match c {
                '\\' => match chars.next() {
                    None => return Err(ParseError::UnexpectedEOF),
                    Some(c) => match c {
                        _ if c == '\\' => c,
                        // octal
                        '0'..='9' => {
                            let octal = c.to_string() + &take(&mut chars, 2);
                            u8::from_str_radix(&octal, 8).map_err(map_err)? as char
                        }
                        // hex
                        'x' => {
                            let hex = take(&mut chars, 2);
                            u8::from_str_radix(&hex, 16).map_err(map_err)? as char
                        }
                        // unicode
                        'u' => decode_unicode(&take(&mut chars, 4))?,
                        'U' => decode_unicode(&take(&mut chars, 8))?,
                        _ => c,
                    },
                },
                _ => c,
            }),
        }
    }

    Ok(Cow::Owned(unescaped))
}

#[inline]
// Iterator#take cannot be used because it consumes the iterator
fn take<I: Iterator<Item = char>>(iterator: &mut I, n: usize) -> String {
    let mut s = String::with_capacity(n);
    for _ in 0..n {
        s.push(iterator.next().unwrap_or_default());
    }
    s
}

fn decode_unicode(code_point: &str) -> Result<char, ParseError> {
    match u32::from_str_radix(code_point, 16) {
        Err(_) => Err(ParseError::General("unrecognized escape".to_string())),
        Ok(n) => std::char::from_u32(n).ok_or(ParseError::General("invalid unicode".to_string())),
    }
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
    fn test_extract_string_value_invalid_unicode_escape_long() {
        let result = extract_string_value("\"hello\\U0000002Gworld\"");
        assert!(result.is_err());
    }
}
