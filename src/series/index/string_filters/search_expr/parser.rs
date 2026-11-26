// Copyright The Prometheus Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Ported from Prometheus `util/searchexpr/parser.go`.

use super::{
    MAX_EXPRESSION_BYTES, MAX_EXPRESSION_NESTING_DEPTH, MAX_EXPRESSION_TERMS, SearchExprError,
    invalid,
};
use std::fmt;

/// A search expression AST node.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Expr {
    /// Matches one search term.
    Term(String),
    /// Accepts candidates accepted by both children.
    And(Box<Expr>, Box<Expr>),
    /// Accepts candidates accepted by either child.
    Or(Box<Expr>, Box<Expr>),
    /// Rejects candidates accepted by its child.
    Not(Box<Expr>),
}

impl Expr {
    pub fn term(value: impl Into<String>) -> Self {
        Expr::Term(value.into())
    }

    pub fn and(left: Expr, right: Expr) -> Self {
        Expr::And(Box::new(left), Box::new(right))
    }

    pub fn or(left: Expr, right: Expr) -> Self {
        Expr::Or(Box::new(left), Box::new(right))
    }

    #[allow(clippy::should_implement_trait)]
    pub fn not(expr: Expr) -> Self {
        Expr::Not(Box::new(expr))
    }
}

/// Parses terms combined with `NOT`, `AND`, `OR`, and parentheses. `NOT` binds more tightly
/// than `AND`, which binds more tightly than `OR`. Operators are ASCII case-insensitive; quote
/// a term to search for literal operator text. Inside quotes, `\"` and `\\` are the only
/// escapes. Rejects input longer than 4096 bytes, with more than 32 terms, or nested more
/// than 16 levels deep, where each `NOT` and each parenthesised group adds one level. Error
/// positions are 0-based byte offsets into `input`.
pub fn parse(input: &str) -> Result<Expr, SearchExprError> {
    if input.len() > MAX_EXPRESSION_BYTES {
        return Err(invalid(format!(
            "length {} exceeds maximum of {MAX_EXPRESSION_BYTES} bytes",
            input.len()
        )));
    }

    let mut p = Parser::new(input);
    p.advance()?;
    let expr = p.parse_or()?;
    if p.cur.kind != TokenKind::Eof {
        return Err(p.error(format_args!("unexpected {}", p.cur.kind)));
    }
    Ok(expr)
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum TokenKind {
    Eof,
    Term(String),
    Not,
    And,
    Or,
    LeftParen,
    RightParen,
}

impl fmt::Display for TokenKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TokenKind::Eof => f.write_str("end of expression"),
            TokenKind::Term(value) => write!(f, "term {value:?}"),
            TokenKind::Not => f.write_str("NOT"),
            TokenKind::And => f.write_str("AND"),
            TokenKind::Or => f.write_str("OR"),
            TokenKind::LeftParen => f.write_str("("),
            TokenKind::RightParen => f.write_str(")"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct Token {
    pub(super) kind: TokenKind,
    pub(super) pos: usize,
}

/// The parser lexes one token of lookahead on demand, so hostile input fails at the first bad
/// token without the cost of lexing the rest. It consumes a token only after accepting it, so
/// the first error in the input wins.
pub(super) struct Parser<'a> {
    input: &'a str,
    pos: usize,
    pub(super) cur: Token,
    term_count: usize,
    depth: usize,
}

fn is_separator(b: u8) -> bool {
    matches!(b, b' ' | b'\t' | b'\n' | b'\r')
}

impl<'a> Parser<'a> {
    pub(super) fn new(input: &'a str) -> Self {
        Self {
            input,
            pos: 0,
            cur: Token {
                kind: TokenKind::Eof,
                pos: 0,
            },
            term_count: 0,
            depth: 0,
        }
    }

    pub(super) fn advance(&mut self) -> Result<(), SearchExprError> {
        let bytes = self.input.as_bytes();
        while self.pos < bytes.len() && is_separator(bytes[self.pos]) {
            self.pos += 1;
        }
        let start = self.pos;
        let Some(&first) = bytes.get(start) else {
            self.cur = Token {
                kind: TokenKind::Eof,
                pos: start,
            };
            return Ok(());
        };

        match first {
            b'(' | b')' => {
                let kind = if first == b'(' {
                    TokenKind::LeftParen
                } else {
                    TokenKind::RightParen
                };
                self.cur = Token { kind, pos: start };
                self.pos += 1;
                return Ok(());
            }
            b'"' => {
                let (value, next) = quoted_term(self.input, start)?;
                if value.is_empty() {
                    return Err(invalid(format!("empty quoted term at position {start}")));
                }
                self.cur = Token {
                    kind: TokenKind::Term(value),
                    pos: start,
                };
                self.pos = next;
            }
            _ => {
                let (kind, end) = bare_token(self.input, start)?;
                let is_term = matches!(kind, TokenKind::Term(_));
                self.cur = Token { kind, pos: start };
                self.pos = end;
                if !is_term {
                    return Ok(());
                }
            }
        }

        self.term_count += 1;
        if self.term_count > MAX_EXPRESSION_TERMS {
            return Err(invalid(format!(
                "term count exceeds maximum of {MAX_EXPRESSION_TERMS} at position {start}"
            )));
        }
        Ok(())
    }

    fn parse_or(&mut self) -> Result<Expr, SearchExprError> {
        let mut left = self.parse_and()?;
        while self.cur.kind == TokenKind::Or {
            self.advance()?;
            let right = self.parse_and()?;
            left = Expr::or(left, right);
        }
        Ok(left)
    }

    fn parse_and(&mut self) -> Result<Expr, SearchExprError> {
        let mut left = self.parse_unary()?;
        while self.cur.kind == TokenKind::And {
            self.advance()?;
            let right = self.parse_unary()?;
            left = Expr::and(left, right);
        }
        Ok(left)
    }

    fn parse_unary(&mut self) -> Result<Expr, SearchExprError> {
        if self.cur.kind != TokenKind::Not {
            return self.parse_primary();
        }
        let expr = self.parse_nested(Self::parse_unary)?;
        Ok(Expr::not(expr))
    }

    fn parse_primary(&mut self) -> Result<Expr, SearchExprError> {
        match &mut self.cur.kind {
            TokenKind::Term(value) => {
                let term = Expr::Term(std::mem::take(value));
                self.advance()?;
                Ok(term)
            }
            TokenKind::LeftParen => {
                let expr = self.parse_nested(Self::parse_or)?;
                if self.cur.kind != TokenKind::RightParen {
                    return Err(self.error(format_args!("expected ), got {}", self.cur.kind)));
                }
                self.advance()?;
                Ok(expr)
            }
            _ => Err(self.error(format_args!("expected term or (, got {}", self.cur.kind))),
        }
    }

    /// Consumes the current `NOT` or `(` token and parses its operand one nesting level
    /// deeper.
    fn parse_nested(
        &mut self,
        parse: fn(&mut Self) -> Result<Expr, SearchExprError>,
    ) -> Result<Expr, SearchExprError> {
        if self.depth >= MAX_EXPRESSION_NESTING_DEPTH {
            return Err(self.error(format_args!(
                "nesting depth exceeds maximum of {MAX_EXPRESSION_NESTING_DEPTH}"
            )));
        }
        self.depth += 1;
        let result = self.advance().and_then(|()| parse(self));
        self.depth -= 1;
        result
    }

    fn error(&self, message: fmt::Arguments<'_>) -> SearchExprError {
        invalid(format!("{message} at position {}", self.cur.pos))
    }
}

/// Splits on ASCII bytes only, so every slice boundary is a `char` boundary.
fn bare_token(input: &str, start: usize) -> Result<(TokenKind, usize), SearchExprError> {
    let bytes = input.as_bytes();
    let mut pos = start;
    while pos < bytes.len() && !is_separator(bytes[pos]) && bytes[pos] != b'(' && bytes[pos] != b')'
    {
        if bytes[pos] == b'"' {
            return Err(invalid(format!("unexpected quote at position {pos}")));
        }
        pos += 1;
    }
    Ok((keyword_token(&input[start..pos]), pos))
}

fn quoted_term(input: &str, start: usize) -> Result<(String, usize), SearchExprError> {
    let unterminated = || invalid(format!("unterminated quoted term at position {start}"));
    let bytes = input.as_bytes();
    let body = start + 1;
    let Some(i) = bytes[body..].iter().position(|&b| b == b'"' || b == b'\\') else {
        return Err(unterminated());
    };
    let mut pos = body + i;
    if bytes[pos] == b'"' {
        return Ok((input[body..pos].to_owned(), pos + 1));
    }

    let mut value = String::with_capacity(pos - body + 16);
    // The start of the bytes not yet copied into `value`.
    let mut run = body;
    while pos < bytes.len() {
        match bytes[pos] {
            b'"' => {
                value.push_str(&input[run..pos]);
                return Ok((value, pos + 1));
            }
            b'\\' => {
                value.push_str(&input[run..pos]);
                match bytes.get(pos + 1) {
                    None => return Err(unterminated()),
                    Some(b'"' | b'\\') => {}
                    Some(_) => return Err(invalid(format!("invalid escape at position {pos}"))),
                }
                // The escaped byte starts the next run.
                run = pos + 1;
                pos += 2;
            }
            _ => pos += 1,
        }
    }
    Err(unterminated())
}

fn keyword_token(value: &str) -> TokenKind {
    if value.eq_ignore_ascii_case("NOT") {
        TokenKind::Not
    } else if value.eq_ignore_ascii_case("AND") {
        TokenKind::And
    } else if value.eq_ignore_ascii_case("OR") {
        TokenKind::Or
    } else {
        TokenKind::Term(value.to_owned())
    }
}
