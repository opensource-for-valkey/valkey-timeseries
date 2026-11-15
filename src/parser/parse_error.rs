use std::fmt;
use std::fmt::{Display, Formatter};

use crate::parser::lex::{Lexeme, Token};
use logos::Span;
use thiserror::Error;

pub type ParseResult<T> = Result<T, ParseError>;

#[derive(Default, Debug, PartialEq, Clone, Error)]
pub enum ParseError {
    #[error(transparent)]
    Unexpected(ParseErr),
    #[error("Invalid duration: `{0}`")]
    InvalidDuration(String),
    #[error("Expected positive unix millis or rfc3339 timestamp: found `{0}`")]
    InvalidTimestamp(String),
    /// Parsed cleanly as a number but is negative. Kept distinct from
    /// `InvalidTimestamp` so the write path can report RedisTimeSeries'
    /// separate "must be a nonnegative integer" message.
    #[error("Expected a nonnegative timestamp: found `{0}`")]
    NegativeTimestamp(String),
    #[error("Expected number: found `{0}`")]
    InvalidNumber(String),
    #[error("Syntax Error: `{0}`")]
    SyntaxError(String),
    #[error("{0}")]
    General(String),
    #[error("Invalid regex: {0}")]
    InvalidRegex(String),
    #[error("Empty series selector")]
    EmptySeriesSelector,
    /// A selector names its metric before the braces and again with a `__name__` matcher
    /// inside them (`cpu{__name__="mem"}`). Prometheus rejects this with the same text;
    /// the second field is the matcher's value as written, already quoted.
    #[error("metric name must not be set twice: {0:?} or {1}")]
    MetricNameSetTwice(String, String),
    /// A `METRIC` operand with more than `MAX_LABELS_PER_SERIES` labels, `__name__` included.
    /// Reported as soon as the limit is passed, so an oversized operand is not parsed to the end.
    #[error("too many labels")]
    TooManyLabels,
    /// What the lexer reports for input no token matches: logos builds its error value with
    /// `Default`. `Cursor` replaces it with an `Unexpected` naming the text.
    #[default]
    #[error("Parse error")]
    Other,
}

/// ParseErr wraps a parser error with its position in the input.
#[derive(Debug, PartialEq, Clone, Error)]
pub struct ParseErr {
    pub range: Span,
    pub err: String,
    /// 1-based line of `range.start`.
    pub line: usize,
    /// 1-based byte column of `range.start` within its line.
    pub column: usize,
}

impl ParseErr {
    /// `source` is the whole input `range` indexes into; line and column are computed from it
    /// the way Prometheus reports positions (`line:column:`, both 1-based, column in bytes).
    pub fn new(msg: &str, range: Span, source: &str) -> Self {
        let start = range.start.min(source.len());
        let before = &source.as_bytes()[..start];
        let line = 1 + before.iter().filter(|&&b| b == b'\n').count();
        let line_start = before
            .iter()
            .rposition(|&b| b == b'\n')
            .map_or(0, |i| i + 1);
        Self {
            range,
            err: msg.to_string(),
            line,
            column: start - line_start + 1,
        }
    }
}

impl Display for ParseErr {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}:{}: parse error: {}",
            self.line, self.column, self.err
        )
    }
}

/// An error for an unexpected token: `unexpected "x" in <context>, expected <expected>`.
/// End of input is named as such rather than as an empty string.
pub(crate) fn unexpected(
    context: &str,
    found: &Lexeme,
    expected: &str,
    source: &str,
) -> ParseError {
    let found_text = if found.token == Token::Eof {
        "end of input".to_string()
    } else {
        format!("\"{}\"", found.text)
    };
    let mut msg = format!("unexpected {found_text}");
    if !context.is_empty() {
        msg.push_str(" in ");
        msg.push_str(context);
    }
    if !expected.is_empty() {
        msg.push_str(", expected ");
        msg.push_str(expected);
    }
    ParseError::Unexpected(ParseErr::new(&msg, found.span.clone(), source))
}
