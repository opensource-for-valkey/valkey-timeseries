//! Label-matcher regexes, compiled the way Prometheus compiles them.
//!
//! Prometheus matches a label value against a regex with Go's `regexp` package (RE2 syntax),
//! fully anchored and with `.` matching a newline (`^(?s:pattern)$`). The `regex` crate is close
//! to RE2 but not identical, so a user's pattern is first rewritten into the Rust pattern that
//! means what Go would make of it ([`go_to_rust_syntax`]), then held to Go's repeat-count rules
//! ([`check_repeat_counts`]), then compiled. The differences that change what a pattern
//! *matches*, rather than only whether it parses, are:
//!
//! - `\d`, `\s`, `\w` and `\b` are ASCII in Go and Unicode in Rust. `\d+` must not match `٣٤`.
//!   Being ASCII also keeps them small: Unicode `\w+` alone is over 16 KiB of compiled program.
//! - Inside a bracket class, Go treats `[`, `&&`, `~~` and `--` as ordinary characters, where
//!   Rust reads nested classes and set operations.
//! - A `{` that does not start a valid repeat (`abc{def}`, `x{,2}`, `a{01}`) is a literal in Go
//!   and a syntax error in Rust.
//! - `\Q…\E` quotes a literal in Go; Rust has no such syntax.
//!
//! Syntax Go rejects and Rust accepts (`(?x)`, `\b{start}`, class set operations written in
//! Rust's syntax) is not rejected here; nothing Go accepts is affected by that.
//!
//! Patterns that come from the decomposition in `regex_utils` (a parsed tree printed back out)
//! are already Rust syntax and go through [`compile_rust_syntax`] instead.

use crate::labels::regex_cache::{REGEX_CACHE_CAPACITY, REGEX_CACHE_MAX_PATTERN_LEN, RegexCache};
use regex::{Regex, RegexBuilder};
use regex_syntax::ast::{self, Ast, RepetitionKind, RepetitionRange};
use regex_syntax::hir::Hir;
use regex_syntax::hir::translate::TranslatorBuilder;
use std::fmt::{self, Display, Formatter};
use std::sync::LazyLock;

/// The approximate size limit, in bytes, of a compiled matcher regex. Go has no equivalent
/// limit, only the repeat-count rule below, so this is where the two can still differ: it
/// admits every counted repeat Go allows over `.`, ASCII and bracket classes (`.{1,1000}` needs
/// about 1 MiB), and rejects long repeats of the big Unicode classes (`\pL{1000}` would need
/// 128 MiB and 200 ms to compile).
const REGEX_SIZE_LIMIT: usize = 4 << 20;
/// The approximate size limit, in bytes, of the cache used by the lazy DFA at match time (the
/// `regex` crate's default). The cache only grows as matching visits new states, so a small
/// pattern stays small. It used to be 16 KiB, which costs nothing on small patterns but sends
/// the larger ones [`REGEX_SIZE_LIMIT`] admits to a slower engine: `.{1,1000}` and
/// `(?:/[^/]{1,255}){1,8}` measured 10-20x slower per value.
const DFA_SIZE_LIMIT: usize = 2 << 20;
/// Go's limit on a counted repeat (`{n}`, `{n,}`, `{n,m}`), and on the product of the counts
/// of repeats nested inside one another.
const MAX_REPEAT_COUNT: u64 = 1000;

/// Why a matcher regex was rejected.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum RegexError {
    /// Go would reject it too; the message says why.
    Syntax(String),
    /// Valid, but its compiled form is over [`REGEX_SIZE_LIMIT`].
    TooLarge,
}

impl Display for RegexError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            RegexError::Syntax(reason) => f.write_str(reason),
            RegexError::TooLarge => write!(
                f,
                "too large to compile (the limit is {} MiB of compiled program)",
                REGEX_SIZE_LIMIT >> 20
            ),
        }
    }
}

impl From<regex::Error> for RegexError {
    fn from(err: regex::Error) -> Self {
        match err {
            regex::Error::CompiledTooBig(_) => RegexError::TooLarge,
            other => RegexError::Syntax(other.to_string()),
        }
    }
}

/// Compile a user's matcher pattern: Go syntax, anchored at both ends.
pub(crate) fn compile_matcher(pattern: &str) -> Result<Regex, RegexError> {
    let rust = go_to_rust_syntax(pattern)?;
    parse_ast(&rust)?;
    compile_rust_syntax(&rust)
}

/// Parse a user's matcher pattern (Go syntax) into the tree [`compile_matcher`] compiles, for
/// decomposition. It accepts and rejects exactly the patterns `compile_matcher` does, apart from
/// the size limit, which only compiling can tell.
pub(crate) fn parse_matcher_hir(pattern: &str) -> Result<Hir, RegexError> {
    let rust = go_to_rust_syntax(pattern)?;
    let ast = parse_ast(&rust)?;
    TranslatorBuilder::new()
        .dot_matches_new_line(true)
        .build()
        .translate(&rust, &ast)
        .map_err(|err| RegexError::Syntax(err.kind().to_string()))
}

/// Label-matcher regexes compiled so far, keyed by their anchored Rust-syntax source: see
/// `regex_cache`. Keying on the translated source, not the user's Go pattern, keeps the two
/// syntaxes from sharing a key (`\d` means different things in each).
static COMPILED_MATCHERS: LazyLock<RegexCache> =
    LazyLock::new(|| RegexCache::new(REGEX_CACHE_CAPACITY, REGEX_CACHE_MAX_PATTERN_LEN));

/// Compile a pattern that is already Rust syntax (a printed `Hir`, or a literal escaped with
/// `regex::escape`), anchored at both ends, under the same flags and limits, through the
/// process-wide cache of compiled matchers. Callers must only call `is_match` on the result
/// (see `regex_cache`).
pub(crate) fn compile_rust_syntax(pattern: &str) -> Result<Regex, RegexError> {
    let anchored = format!("^(?:{pattern})$");
    Ok(COMPILED_MATCHERS.get_or_compile(&anchored, |anchored| {
        RegexBuilder::new(anchored)
            .size_limit(REGEX_SIZE_LIMIT)
            .dfa_size_limit(DFA_SIZE_LIMIT)
            .dot_matches_new_line(true)
            .build()
    })?)
}

/// Parse the (already translated) pattern and apply Go's repeat-count rule.
fn parse_ast(rust: &str) -> Result<Ast, RegexError> {
    let ast = ast::parse::Parser::new()
        .parse(rust)
        .map_err(|err| RegexError::Syntax(err.kind().to_string()))?;
    check_repeat_counts(rust, &ast)?;
    Ok(ast)
}

/// Go rejects a counted repeat over 1000, and a repeat whose count, multiplied by the counts of
/// the repeats inside it, exceeds 1000 along any path: `(a{10}){100}` is accepted and
/// `(a{10}){101}` is not, while `*`, `+` and `?` count as 1 (`(a{1000})*` is accepted). The
/// error names the repeat that went over, as Go's does. Returns the largest product inside
/// `ast`.
fn check_repeat_counts(pattern: &str, ast: &Ast) -> Result<u64, RegexError> {
    Ok(match ast {
        Ast::Repetition(rep) => {
            // Go rejects a repeat applied directly to a repeat (`a**`, `a{2}{3}`); Rust
            // accepts it. A lazy repeat (`a*?`) is one operator in both.
            if let Ast::Repetition(inner) = rep.ast.as_ref() {
                let op = &pattern[inner.op.span.start.offset..rep.op.span.end.offset];
                return Err(RegexError::Syntax(format!(
                    "invalid nested repetition operator: `{op}`"
                )));
            }
            let inner = check_repeat_counts(pattern, &rep.ast)?;
            let count = match &rep.op.kind {
                RepetitionKind::ZeroOrOne
                | RepetitionKind::ZeroOrMore
                | RepetitionKind::OneOrMore => 1,
                RepetitionKind::Range(
                    RepetitionRange::Exactly(n) | RepetitionRange::AtLeast(n),
                ) => u64::from(*n),
                RepetitionKind::Range(RepetitionRange::Bounded(_, max)) => u64::from(*max),
            };
            let product = inner.saturating_mul(count);
            if count > MAX_REPEAT_COUNT || product > MAX_REPEAT_COUNT {
                let span = &rep.op.span;
                let op = &pattern[span.start.offset..span.end.offset];
                return Err(RegexError::Syntax(format!("invalid repeat count: `{op}`")));
            }
            product
        }
        Ast::Group(group) => check_repeat_counts(pattern, &group.ast)?,
        Ast::Alternation(alt) => max_repeat_count(pattern, &alt.asts)?,
        Ast::Concat(concat) => max_repeat_count(pattern, &concat.asts)?,
        _ => 1,
    })
}

fn max_repeat_count(pattern: &str, asts: &[Ast]) -> Result<u64, RegexError> {
    asts.iter().try_fold(
        1,
        |max, ast| Ok(max.max(check_repeat_counts(pattern, ast)?)),
    )
}

/// The POSIX class names Go accepts in `[[:name:]]` (Rust accepts the same set).
const POSIX_CLASSES: [&str; 14] = [
    "alnum", "alpha", "ascii", "blank", "cntrl", "digit", "graph", "lower", "print", "punct",
    "space", "upper", "word", "xdigit",
];

const ASCII_DIGIT: &str = "0-9";
// Go's `\s` is `[\t\n\f\r ]`: no vertical tab, unlike Rust's ASCII `\s`.
const ASCII_SPACE: &str = r"\t\n\f\r ";
const ASCII_WORD: &str = "0-9A-Za-z_";

/// The class a Go Perl escape (`\d`, `\D`, `\s`, …) stands for, as a bracket class.
fn perl_class(escape: char) -> Option<String> {
    let (body, negated) = match escape {
        'd' => (ASCII_DIGIT, false),
        'D' => (ASCII_DIGIT, true),
        's' => (ASCII_SPACE, false),
        'S' => (ASCII_SPACE, true),
        'w' => (ASCII_WORD, false),
        'W' => (ASCII_WORD, true),
        _ => return None,
    };
    Some(format!("[{}{body}]", if negated { "^" } else { "" }))
}

/// Rewrite a Go (RE2) pattern into the Rust pattern that matches the same strings.
///
/// This is a lexical pass: it tracks escapes and bracket classes, which is all the rewriting
/// needs, and leaves grouping, alternation and repetition to the parser. A pattern the pass
/// leaves malformed is reported by the parser afterwards.
pub(crate) fn go_to_rust_syntax(pattern: &str) -> Result<String, RegexError> {
    let mut out = String::with_capacity(pattern.len() + 8);
    let mut rest = pattern;
    while let Some(c) = rest.chars().next() {
        rest = &rest[c.len_utf8()..];
        match c {
            '\\' => {
                let Some(e) = rest.chars().next() else {
                    // A dangling `\`: keep it so the parser rejects the pattern.
                    out.push('\\');
                    break;
                };
                rest = &rest[e.len_utf8()..];
                if let Some((ch, after)) = octal_escape(e, rest) {
                    push_hex_escape(&mut out, ch);
                    rest = after;
                } else if let Some(class) = perl_class(e) {
                    out.push_str(&class);
                } else if e == 'b' || e == 'B' {
                    // Go's word boundary is ASCII.
                    out.push_str(if e == 'b' { r"(?-u:\b)" } else { r"(?-u:\B)" });
                } else if e == 'Q' {
                    let Some(end) = rest.find(r"\E") else {
                        // Prometheus wraps the pattern, so an open `\Q` swallows the closing
                        // `)` and Go fails to parse it.
                        return Err(RegexError::Syntax("missing closing `\\E`".to_string()));
                    };
                    out.push_str(&regex_syntax::escape(&rest[..end]));
                    rest = &rest[end + 2..];
                } else {
                    out.push('\\');
                    out.push(e);
                    rest = copy_escape_argument(e, rest, &mut out);
                }
            }
            '[' => {
                out.push('[');
                rest = translate_class(rest, &mut out)?;
            }
            '{' => {
                let len = repeat_len(rest);
                if len == 0 {
                    out.push_str(r"\{");
                } else {
                    let op = &rest[..len];
                    // Checked here as well as on the tree: a count too big for the parser
                    // would otherwise fail with the parser's own message, not Go's.
                    let too_big = op[..len - 1]
                        .split(',')
                        .filter(|n| !n.is_empty())
                        .any(|n| n.parse::<u64>().map_or(true, |n| n > MAX_REPEAT_COUNT));
                    if too_big {
                        return Err(RegexError::Syntax(format!(
                            "invalid repeat count: `{{{op}`"
                        )));
                    }
                    out.push('{');
                    out.push_str(op);
                    rest = &rest[len..];
                }
            }
            _ => out.push(c),
        }
    }
    Ok(out)
}

/// After `\p`, `\P` or `\x`, copy its argument: braced (`\p{Greek}`, `\x{41}`) through the `}`,
/// or the short form, one letter after `\p`/`\P` (`\pL`) and two hex digits after `\x`
/// (`\x41`). Copied whole, a brace is not mistaken for a repeat and, in a class, none of its
/// characters for a range end. Returns what follows it.
fn copy_escape_argument<'a>(escape: char, rest: &'a str, out: &mut String) -> &'a str {
    let short_len = match escape {
        'p' | 'P' => 1,
        'x' => 2,
        _ => return rest,
    };
    let len = if rest.starts_with('{') {
        match rest.find('}') {
            Some(end) => end + 1,
            None => return rest,
        }
    } else {
        rest.chars().take(short_len).map(char::len_utf8).sum()
    };
    out.push_str(&rest[..len]);
    &rest[len..]
}

/// A Go octal escape, `first` being the digit after the `\`: `\0` and up to two more octal
/// digits, or `\1`-`\7` and one or two more (`\141` is `a`). A lone non-zero digit is a
/// backreference, which Go rejects, so it is not one: it stays as written and the parser
/// rejects it too. Returns the character and what follows the escape.
fn octal_escape(first: char, rest: &str) -> Option<(char, &str)> {
    let mut value = first.to_digit(8)?;
    let mut rest = rest;
    let mut digits = 1;
    while digits < 3 {
        let Some(digit) = rest.chars().next().and_then(|c| c.to_digit(8)) else {
            break;
        };
        value = value * 8 + digit;
        rest = &rest[1..];
        digits += 1;
    }
    if first != '0' && digits == 1 {
        return None;
    }
    // At most `\777`, 511: always a valid character.
    Some((char::from_u32(value)?, rest))
}

/// `ch` as a Rust escape, which reads the same inside and outside a class.
fn push_hex_escape(out: &mut String, ch: char) {
    use std::fmt::Write;
    let _ = write!(out, "\\x{{{:x}}}", u32::from(ch));
}

/// If `rest` (what follows a `{`) is the rest of a repeat Go accepts — `n}`, `n,}` or `n,m}`,
/// each count a decimal without leading zeros — its length through the `}`; otherwise 0.
fn repeat_len(rest: &str) -> usize {
    fn count_len(s: &str) -> usize {
        let len = s.bytes().take_while(u8::is_ascii_digit).count();
        // Go reads `01` as no count at all, which makes the braces literal.
        if len > 1 && s.starts_with('0') {
            0
        } else {
            len
        }
    }
    let min = count_len(rest);
    if min == 0 {
        return 0;
    }
    let mut at = min;
    if rest[at..].starts_with(',') {
        at += 1;
        if !rest[at..].starts_with('}') {
            let max = count_len(&rest[at..]);
            if max == 0 {
                return 0;
            }
            at += max;
        }
    }
    if rest[at..].starts_with('}') {
        at + 1
    } else {
        0
    }
}

/// Translate the inside of a bracket class, `rest` being what follows its `[`, through the
/// closing `]`. Returns what follows the class.
///
/// Go reads a class as a sequence of items, each a character, a range `a-z`, a class escape
/// (`\d`, `\pL`), or a POSIX class `[:alpha:]`. A `]` first in the class is a character like any
/// other, so it can start a range (`[]-a]`). Only a character starts a range: a `-` after a class
/// item is literal (`[\d-z]` is digits, `-` and `z`), and a class after `-` is an error. Everything
/// else is literal, so `[`, `&` and `~` are escaped (they open nested classes and set operations in
/// Rust), and so is a `-` that is not a range operator, which keeps `--` from reading as a set
/// difference.
fn translate_class<'a>(mut rest: &'a str, out: &mut String) -> Result<&'a str, RegexError> {
    if let Some(r) = rest.strip_prefix('^') {
        out.push('^');
        rest = r;
    }
    let mut first = true;
    loop {
        let Some(c) = rest.chars().next() else {
            // Unclosed: let the parser report it.
            return Ok(rest);
        };
        if c == ']' && !first {
            out.push(']');
            return Ok(&rest[1..]);
        }
        first = false;
        if c == '['
            && rest.starts_with("[:")
            && let Some(end) = rest.find(":]")
        {
            let class = &rest[..end + 2];
            let name = class[2..end].trim_start_matches('^');
            if !POSIX_CLASSES.contains(&name) {
                return Err(RegexError::Syntax(format!(
                    "invalid character class range: `{class}`"
                )));
            }
            out.push_str(class);
            rest = &rest[end + 2..];
            continue;
        }
        let item = rest;
        let is_class;
        (rest, is_class) = class_item(item, out)?;
        // A range: `-` between this character and a following item that is not the closing `]`.
        if !is_class
            && let Some(after) = rest.strip_prefix('-')
            && !after.is_empty()
            && !after.starts_with(']')
        {
            out.push('-');
            let hi_is_class;
            (rest, hi_is_class) = class_item(after, out)?;
            if hi_is_class {
                let range = &item[..item.len() - rest.len()];
                return Err(RegexError::Syntax(format!(
                    "invalid character class range: `{range}`"
                )));
            }
        }
    }
}

/// Translate one item of a class: a character (a literal or an escape) or a class escape
/// (`\d`, `\pL`). Returns what follows it, and whether it was a class.
fn class_item<'a>(rest: &'a str, out: &mut String) -> Result<(&'a str, bool), RegexError> {
    let Some(c) = rest.chars().next() else {
        return Ok((rest, false));
    };
    let mut rest = &rest[c.len_utf8()..];
    let mut is_class = false;
    match c {
        '\\' => {
            let Some(e) = rest.chars().next() else {
                out.push('\\');
                return Ok((rest, false));
            };
            rest = &rest[e.len_utf8()..];
            if let Some((ch, after)) = octal_escape(e, rest) {
                push_hex_escape(out, ch);
                rest = after;
            } else if let Some(class) = perl_class(e) {
                // A nested class is how Rust spells a union inside a class.
                out.push_str(&class);
                is_class = true;
            } else {
                out.push('\\');
                out.push(e);
                rest = copy_escape_argument(e, rest, out);
                is_class = matches!(e, 'p' | 'P');
            }
        }
        // `]` is only an item when it comes first; the class closes on any later one.
        '[' | ']' | '&' | '~' | '-' => {
            out.push('\\');
            out.push(c);
        }
        _ => out.push(c),
    }
    Ok((rest, is_class))
}

#[cfg(test)]
mod tests {
    use super::{RegexError, compile_matcher, go_to_rust_syntax};

    #[test]
    fn test_translates_go_syntax() {
        for (go, rust) in [
            (r"\d+", r"[0-9]+"),
            (r"\W", r"[^0-9A-Za-z_]"),
            (r"\s", r"[\t\n\f\r ]"),
            (r"\bfoo\B", r"(?-u:\b)foo(?-u:\B)"),
            (r"[\w-]", r"[[0-9A-Za-z_]\-]"),
            (r"[a[b]", r"[a\[b]"),
            (r"[a&&b~~c]", r"[a\&\&b\~\~c]"),
            (r"[--b]", r"[\--b]"),
            (r"[a-]", r"[a\-]"),
            (r"[]a]", r"[\]a]"),
            (r"[^]a]", r"[^\]a]"),
            (r"[]-a]", r"[\]-a]"),
            (r"[\d-z]", r"[[0-9]\-z]"),
            (r"[\pL-z]", r"[\pL\-z]"),
            (r"[\x41-\x5a]", r"[\x41-\x5a]"),
            (r"\141", r"\x{61}"),
            (r"[\0-\x01]", r"[\x{0}-\x01]"),
            (r"[[:^alpha:]x]", r"[[:^alpha:]x]"),
            (r"\Qa.b\E+", r"a\.b+"),
            (r"abc{def}", r"abc\{def}"),
            (r"x{,2}", r"x\{,2}"),
            (r"a{01}", r"a\{01}"),
            (r"a{1,2,}", r"a\{1,2,}"),
            (r"a{2}b{1,}c{3,4}", r"a{2}b{1,}c{3,4}"),
            (r"\p{Greek}x{y}", r"\p{Greek}x\{y}"),
            (r"\x{41}b{c}", r"\x{41}b\{c}"),
            (r"[\p{Greek}x]", r"[\p{Greek}x]"),
            (r"abc\{", r"abc\{"),
            (r"abc\", r"abc\"),
        ] {
            assert_eq!(go_to_rust_syntax(go).unwrap(), rust, "{go:?}");
        }
        assert!(go_to_rust_syntax(r"\Qa").is_err());
    }

    /// Each pattern against `VALUES`, as Go's `regexp` (Prometheus' `^(?s:pattern)$`) answers:
    /// `None` when Go rejects the pattern, else the values it matches. Observed by running Go
    /// 1.22 on these exact inputs.
    #[test]
    fn test_matches_like_go() {
        const VALUES: [&str; 19] = [
            "a", "b", "-", "]", "\x0b", "\t", " ", "5", "x", "A", "K", "\u{212a}", "é", "\n", "&",
            "~", "m", "\\", "Ω",
        ];
        let cases: &[(&str, Option<&[&str]>)] = &[
            (r"[--b]", Some(&["a", "b", "-", "]", "5", "A", "K", "\\"])),
            (r"[a-]", Some(&["a", "-"])),
            (
                r"[[:^alpha:]]",
                Some(&[
                    "-", "]", "\x0b", "\t", " ", "5", "\u{212a}", "é", "\n", "&", "~", "\\", "Ω",
                ]),
            ),
            (r"[]a]", Some(&["a", "]"])),
            (
                r"[^]a]",
                Some(&[
                    "b", "-", "\x0b", "\t", " ", "5", "x", "A", "K", "\u{212a}", "é", "\n", "&",
                    "~", "m", "\\", "Ω",
                ]),
            ),
            (r"\s", Some(&["\t", " ", "\n"])),
            (r"[\s]", Some(&["\t", " ", "\n"])),
            (r"\pN", Some(&["5"])),
            (r"[\p{Greek}x]", Some(&["x", "Ω"])),
            (r"\x{41}", Some(&["A"])),
            (r"(?i)k", Some(&["K", "\u{212a}"])),
            (r"[^\D]", Some(&["5"])),
            (
                r"[\W\d]",
                Some(&[
                    "-", "]", "\x0b", "\t", " ", "5", "\u{212a}", "é", "\n", "&", "~", "\\", "Ω",
                ]),
            ),
            (r"\w", Some(&["a", "b", "5", "x", "A", "K", "m"])),
            (r"\\", Some(&["\\"])),
            (r"[a&b]", Some(&["a", "b", "&"])),
            (r"[a~b]", Some(&["a", "b", "~"])),
            (r"[a-z&&[^m]]", Some(&[])),
            (r"(?s-i:a)", Some(&["a"])),
            (r"(?P<x>a)", Some(&["a"])),
            (r"a{01}", Some(&[])),
            (r"[a--b]", None),
            (r"[[:foo:]]", None),
            (r"\Qa", None),
            (r"a\E", None),
            (r"{2}", None),
            (r"a{2}{3}", None),
            (r"a**", None),
            (r"a\", None),
            (r"\1", None),
            (r"[\b]", None),
            (r"(a{2}){500}*", None),
        ];
        for (pattern, want) in cases {
            let got = compile_matcher(pattern).ok().map(|re| {
                VALUES
                    .iter()
                    .copied()
                    .filter(|v| re.is_match(v))
                    .collect::<Vec<&str>>()
            });
            assert_eq!(got.as_deref(), *want, "{pattern:?}");
        }
    }

    /// Class items and escapes the translator once got wrong, against Go as above (Go 1.22 on
    /// these exact inputs): a `]` first in a class starts a range; a `-` after a class escape
    /// (`\d`, `\pL`) is literal, and a class cannot end a range; octal escapes are characters,
    /// while a lone non-zero digit is a backreference Go rejects.
    #[test]
    fn test_class_ranges_and_octal_escapes_match_like_go() {
        const VALUES: [&str; 17] = [
            "a", "b", "m", "z", "]", "^", "_", "-", "5", "A", "M", "\n", "\x00", "\x01", "x\x00y",
            "Ω", "ǿ",
        ];
        let cases: &[(&str, Option<&[&str]>)] = &[
            (r"[]-a]", Some(&["a", "]", "^", "_"])),
            (
                r"[^]-a]",
                Some(&[
                    "b", "m", "z", "-", "5", "A", "M", "\n", "\x00", "\x01", "Ω", "ǿ",
                ]),
            ),
            (r"[]-]", Some(&["]", "-"])),
            (r"[\d-a-z]", Some(&["a", "b", "m", "z", "-", "5"])),
            (
                r"[\w-z]",
                Some(&["a", "b", "m", "z", "_", "-", "5", "A", "M"]),
            ),
            (
                r"[\pL-z]",
                Some(&["a", "b", "m", "z", "-", "A", "M", "Ω", "ǿ"]),
            ),
            (
                r"[\PL-z]",
                Some(&["z", "]", "^", "_", "-", "5", "\n", "\x00", "\x01"]),
            ),
            (r"[\p{Greek}-z]", Some(&["z", "-", "Ω"])),
            (r"[[:digit:]-z]", Some(&["z", "-", "5"])),
            (r"[a-\d]", None),
            (r"[a-\pL]", None),
            (r"[\x41-\x5a]", Some(&["A", "M"])),
            (r"\141", Some(&["a"])),
            (r"[\141-\143]", Some(&["a", "b"])),
            (r"\0", Some(&["\x00"])),
            (r"\012", Some(&["\n"])),
            (r"x\0y", Some(&["x\x00y"])),
            (r"\777", Some(&["ǿ"])),
            (r"[\0-\x01]", Some(&["\x00", "\x01"])),
            (r"\1", None),
            (r"\18", None),
            (r"[\1]", None),
            (r"\8", None),
        ];
        for (pattern, want) in cases {
            let got = compile_matcher(pattern).ok().map(|re| {
                VALUES
                    .iter()
                    .copied()
                    .filter(|v| re.is_match(v))
                    .collect::<Vec<&str>>()
            });
            assert_eq!(got.as_deref(), *want, "{pattern:?}");
        }
    }

    #[test]
    fn test_perl_classes_are_ascii() {
        let digits = compile_matcher(r"\d+").unwrap();
        assert!(digits.is_match("123"));
        assert!(
            !digits.is_match("٣٤"),
            "Arabic-Indic digits are not \\d in Go"
        );
        let word = compile_matcher(r"\w+").unwrap();
        assert!(word.is_match("region_1"));
        assert!(!word.is_match("région"));
        let boundary = compile_matcher(r".*\bfoo\b.*").unwrap();
        assert!(boundary.is_match("a foo b"));
        // `é` is not an ASCII word character, so there is a boundary between it and `foo`.
        assert!(boundary.is_match("éfoo"));
        assert!(!boundary.is_match("xfoo"));
    }

    #[test]
    fn test_repeat_counts_follow_go() {
        for ok in [
            "a{1000}",
            "a{1000,}",
            "(a{10}){100}",
            "(a{2}){500}",
            "((a{10}){10}){10}",
            "(a{2}b{500}){2}",
            "(a*){1000}",
            "(a{1000})*",
            "(a{1,}){1000}",
            "a{0}",
        ] {
            assert!(compile_matcher(ok).is_ok(), "{ok:?} should compile");
        }
        for (bad, op) in [
            ("a{1001}", "{1001}"),
            ("(a{10}){101}", "{101}"),
            ("(a{2}){501}", "{501}"),
            ("((a{10}){10}){11}", "{11}"),
            ("(a{2}|b{500}){3}", "{3}"),
            ("(.{1,1000}){1,1000}", "{1,1000}"),
            ("a{99999999999}", "{99999999999}"),
        ] {
            assert_eq!(
                compile_matcher(bad).unwrap_err(),
                RegexError::Syntax(format!("invalid repeat count: `{op}`")),
                "{bad:?}"
            );
        }
    }

    /// Patterns Go accepts that the old 16 KiB limit, with Unicode Perl classes, rejected.
    #[test]
    fn test_ordinary_patterns_compile() {
        for pattern in [
            r"\w+",
            r"[\w-]+",
            r"host-\w+",
            r"\W+",
            r"\pL+",
            r"\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}",
            r".{1,64}",
            r".{1,256}",
            r".{1,1000}",
            r".{1000}.{1000}.{1000}",
            r"[^/]{1,1000}/[^/]{1,1000}",
            r"\p{Greek}{1000}",
        ] {
            assert!(compile_matcher(pattern).is_ok(), "{pattern:?}");
        }
    }

    #[test]
    fn test_oversized_pattern_says_so() {
        // Valid for Go, but its compiled form is far past the limit.
        assert_eq!(
            compile_matcher(r"\pL{1000}").unwrap_err(),
            RegexError::TooLarge
        );
        assert!(
            RegexError::TooLarge.to_string().contains("too large"),
            "{}",
            RegexError::TooLarge
        );
    }
}
