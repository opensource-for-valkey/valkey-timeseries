use std::fmt;
use std::fmt::{Display, Formatter};

use logos::Span;
use thiserror::Error;

pub type ParseResult<T> = Result<T, ParseError>;

#[derive(Default, Debug, PartialEq, Clone, Error)]
pub enum ParseError {
    #[error(transparent)]
    Unexpected(ParseErr),
    #[error("Expected positive duration: found `{0}`")]
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
    #[error("Invalid match operator: {0}")]
    InvalidMatchOperator(String),
    #[error("Empty series selector")]
    EmptySeriesSelector,
    /// A selector names its metric before the braces and again with a `__name__` matcher
    /// inside them (`cpu{__name__="mem"}`). Prometheus rejects this with the same text;
    /// the second field is the matcher's value as written, already quoted.
    #[error("metric name must not be set twice: {0:?} or {1}")]
    MetricNameSetTwice(String, String),
    /// What the lexer reports for input no token matches: logos builds its error value with
    /// `Default`. `get_next_token` replaces it with an `Unexpected` naming the text.
    #[default]
    #[error("Parse error")]
    Other,
}

/// ParseErr wraps a parser error with its byte span in the input.
#[derive(Debug, PartialEq, Clone, Error)]
pub struct ParseErr {
    pub range: Span,
    pub err: String,
}

impl ParseErr {
    pub fn new<S: Into<Span>>(msg: &str, range: S) -> Self {
        Self {
            range: range.into(),
            err: msg.to_string(),
        }
    }
}

impl Display for ParseErr {
    /// Renders `1:<byte offset>: parse error: ...`. The line is always 1 and the "column" is
    /// the 0-based byte offset; clients see this text, so changing it is a separate decision.
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "1:{}: parse error: {}", self.range.start, self.err)
    }
}

/// `unexpected` creates a parser error complaining about an unexpected lexer item.
/// The item presented as unexpected is always the last item produced by the lexer.
pub(crate) fn unexpected(
    context: &str,
    actual: &str,
    expected: &str,
    span: Option<&Span>,
) -> ParseError {
    let mut err_msg: String = String::with_capacity(25 + context.len() + expected.len());

    err_msg.push_str("unexpected ");

    let actual = format!("\"{actual}\"");
    err_msg.push_str(&actual);

    if !context.is_empty() {
        err_msg.push_str(" in ");
        err_msg.push_str(context)
    }

    if !expected.is_empty() {
        err_msg.push_str(", expected ");
        err_msg.push_str(expected)
    }

    let span = if let Some(sp) = span {
        sp.clone()
    } else {
        Span::default()
    };
    ParseError::Unexpected(ParseErr::new(&err_msg, span))
}
