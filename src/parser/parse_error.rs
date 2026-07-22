use std::fmt;
use std::fmt::{Display, Formatter};

use logos::Span;
use thiserror::Error;

pub type ParseResult<T> = Result<T, ParseError>;

#[derive(Default, Debug, PartialEq, Clone, Error)]
pub enum ParseError {
    #[error(transparent)]
    Unexpected(ParseErr), // TODO !!!!!!
    #[error("Unexpected end of text")]
    UnexpectedEOF,
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
    #[error("Invalid series selector")]
    InvalidSeriesSelector,
    #[default]
    #[error("Parse error")]
    Other,
}

/// ParseErr wraps a parser error with line and position context.
#[derive(Debug, PartialEq, Clone, Error)]
pub struct ParseErr {
    pub range: Span,
    pub err: String,
    /// line_offset is an additional line offset to be added. Only used inside unit tests.
    pub line_offset: usize,
}

impl ParseErr {
    pub fn new<S: Into<Span>>(msg: &str, range: S) -> Self {
        Self {
            range: range.into(),
            err: msg.to_string(),
            line_offset: 0,
        }
    }
}

impl Display for ParseErr {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        let pos: usize = self.range.start;
        let last_line_break = 0;
        let line = self.line_offset + 1;

        let col = pos - last_line_break;
        let position_str = format!("{line}:{col}:").to_string();
        write!(f, "{} parse error: {}", position_str, self.err)?;
        Ok(())
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
