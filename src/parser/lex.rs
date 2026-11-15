use crate::parser::parse_error::unexpected;
use crate::parser::{ParseErr, ParseError, ParseResult};
use logos::{Lexer, Logos, Span};
use std::fmt::{Display, Formatter};

fn unterminated_string_literal(_: &mut Lexer<Token>) -> ParseResult<()> {
    Err(ParseError::SyntaxError(
        "unterminated string literal".to_string(),
    ))
}

#[derive(Logos, Debug, PartialEq, Clone, Copy)]
#[logos(error = ParseError)]
#[logos(subpattern decimal = r"[0-9][_0-9]*")]
#[logos(subpattern hex = r"-?0[xX][0-9a-fA-F][_0-9a-fA-F]*")]
#[logos(subpattern octal = r"-?0[oO]?[1-7][_0-7]*")]
#[logos(subpattern binary = r"-?0[bB][0-1][_0-1]*")]
#[logos(subpattern float = r"-?(?:([0-9]*[.])?[0-9][0-9_]*)(?:[eE][+-]?[0-9][0-9_]*)?")]
// No comment rule: a selector is a single command argument, and skipping `#…` silently cut
// unquoted values short (`color=#ff0000` became `color=`, which selects series *without* the label).
#[logos(skip r"[ \t\n\f\r]+")]
pub enum Token {
    #[token(b"or", ignore(case))]
    OpOr,

    #[regex(r"[_a-zA-Z][_a-zA-Z0-9:\.\-]*")]
    Identifier,

    #[regex(r#"'(?s:[^'\\]|\\.)*'"#)]
    // Backtick strings are raw, as in Go and PromQL: `\` is an ordinary character and a
    // backtick always ends the string, so `C:\` is valid and no backtick can be escaped.
    #[regex(r#"`[^`]*`"#)]
    #[regex(r#""(?s:[^"\\]|\\.)*""#)]
    StringLiteral,

    #[regex("(?&decimal)", priority = 3)]
    #[regex("(?&binary)")]
    #[regex("(?&hex)")]
    #[regex("(?&octal)")]
    #[regex("(?&float)")]
    Number,

    #[token("{")]
    LeftBrace,

    #[token("}")]
    RightBrace,

    #[token(",")]
    Comma,

    #[token("(")]
    LeftParen,

    #[token(")")]
    RightParen,

    #[token("=")]
    Equal,

    #[token("!=")]
    OpNotEqual,

    #[token("=~")]
    RegexEqual,

    #[token("!~")]
    RegexNotEqual,

    #[token("^=")]
    StartsWith,

    #[token("^~")]
    NotStartsWith,

    /// Never produced as a token: these patterns match a string literal with no closing
    /// quote, and their callback turns the match into an "unterminated string literal" error.
    /// Logos attaches patterns to a variant, so the variant has to exist.
    #[regex("\"(?s:[^\"\\\\]|\\\\.)*", unterminated_string_literal)]
    #[regex("'(?s:[^'\\\\]|\\\\.)*", unterminated_string_literal)]
    ErrorStringUnterminated,

    /// Marker for end of stream.
    Eof,
}

impl Token {
    pub const fn as_str(&self) -> &'static str {
        match self {
            // keywords
            Self::StringLiteral => "<string literal>",

            Self::Identifier => "<identifier>",
            Self::Number => "<number>",

            // symbols
            Self::LeftBrace => "{",
            Self::RightBrace => "}",
            Self::Comma => ",",
            Self::LeftParen => "(",
            Self::RightParen => ")",
            Self::Equal => "=",

            // operators
            Self::OpOr => "or",
            Self::OpNotEqual => "!=",
            Self::RegexEqual => "=~",
            Self::RegexNotEqual => "!~",
            Self::StartsWith => "^=",
            Self::NotStartsWith => "^~",

            Self::Eof => "end of input",

            // other
            Self::ErrorStringUnterminated => "<unterminated string literal>",
        }
    }
}

impl Display for Token {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// One token and where it came from. `text` borrows the source, not the lexer, so a lexeme
/// outlives the next read.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct Lexeme<'s> {
    pub token: Token,
    pub text: &'s str,
    pub span: Span,
}

/// A token stream with one token of lookahead, the shared front end of the selector and
/// metric-name grammars. End of input is a `Token::Eof` lexeme positioned at the end of the
/// source; reading past it keeps returning it.
pub(crate) struct Cursor<'s> {
    lex: Lexer<'s, Token>,
    peeked: Option<Lexeme<'s>>,
}

impl<'s> Cursor<'s> {
    pub fn new(source: &'s str) -> Self {
        Self::at(source, 0)
    }

    /// A cursor over `source` that starts at byte `offset`, so errors report positions in the
    /// whole input. `offset` must be a char boundary.
    pub fn at(source: &'s str, offset: usize) -> Self {
        let mut lex = Token::lexer(source);
        lex.bump(offset);
        Self { lex, peeked: None }
    }

    pub fn source(&self) -> &'s str {
        self.lex.source()
    }

    fn read(&mut self) -> ParseResult<Lexeme<'s>> {
        match self.lex.next() {
            Some(Ok(token)) => Ok(Lexeme {
                token,
                text: self.lex.slice(),
                span: self.lex.span(),
            }),
            Some(Err(err)) => {
                // A callback's own message ("unterminated string literal") is kept; input no
                // rule matches is reported by its text.
                let msg = match err {
                    ParseError::SyntaxError(msg) => msg,
                    _ => format!("unexpected token \"{}\"", self.lex.slice().trim()),
                };
                Err(ParseError::Unexpected(ParseErr::new(
                    &msg,
                    self.lex.span(),
                    self.source(),
                )))
            }
            None => {
                let end = self.source().len();
                Ok(Lexeme {
                    token: Token::Eof,
                    text: "",
                    span: end..end,
                })
            }
        }
    }

    pub fn peek(&mut self) -> ParseResult<Lexeme<'s>> {
        if let Some(lexeme) = &self.peeked {
            return Ok(lexeme.clone());
        }
        let lexeme = self.read()?;
        self.peeked = Some(lexeme.clone());
        Ok(lexeme)
    }

    pub fn next(&mut self) -> ParseResult<Lexeme<'s>> {
        match self.peeked.take() {
            Some(lexeme) => Ok(lexeme),
            None => self.read(),
        }
    }

    /// Consumes the next token if it is `token`.
    pub fn next_if(&mut self, token: Token) -> ParseResult<Option<Lexeme<'s>>> {
        if self.peek()?.token == token {
            self.next().map(Some)
        } else {
            Ok(None)
        }
    }

    /// Consumes the next token, which must be one of `expected`; otherwise reports it as
    /// unexpected in `context` (a grammar production, e.g. "label matcher").
    pub fn expect(&mut self, expected: &[Token], context: &str) -> ParseResult<Lexeme<'s>> {
        let lexeme = self.next()?;
        if expected.contains(&lexeme.token) {
            Ok(lexeme)
        } else {
            Err(self.unexpected(&lexeme, context, &describe_tokens(expected)))
        }
    }

    pub fn unexpected(&self, found: &Lexeme, context: &str, expected: &str) -> ParseError {
        unexpected(context, found, expected, self.source())
    }
}

/// `a`, `a or b`, `one of: a, b, c`.
pub(crate) fn describe_tokens(tokens: &[Token]) -> String {
    match tokens {
        [] => String::new(),
        [one] => one.as_str().to_string(),
        [a, b] => format!("{} or {}", a.as_str(), b.as_str()),
        _ => {
            let names: Vec<_> = tokens.iter().map(Token::as_str).collect();
            format!("one of: {}", names.join(", "))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::Token;
    use super::Token::*;
    use logos::Logos;
    use test_case::test_case;

    macro_rules! test_tokens {
    ($src:expr, [$(
      $tok:expr
      $(=> $val:expr)?
    ),*$(,)?]) => {#[allow(unused)] {
      let src: &str = $src;
      let mut lex = Token::lexer(src);
      let mut index = 0;
      let mut offset = 0;
      $({
        let actual = lex.next().expect(&format!("Expected token {}", index + 1)).unwrap();
        let expected = $tok;
        let text = lex.slice();
        assert_eq!(actual, expected);
        $(
          assert_eq!(text, $val, "index: {}", index);
        )?

        index += 1;
        offset += text.len();
      })*

      match lex.next() {
        None => (),
        Some(t) => panic!("Expected exactly {} tokens, but got {:#?} when expecting Eof", index, t),
      }
    }};
  }

    #[test]
    fn empty() {
        test_tokens!("", []);
    }

    #[test]
    fn whitespace() {
        // whitespace is skipped
        test_tokens!("  \t\n\r\r\n", []);
    }

    #[test_case("{", LeftBrace)]
    #[test_case("}", RightBrace)]
    #[test_case("(", LeftParen)]
    #[test_case(")", RightParen)]
    #[test_case(",", Comma)]
    #[test_case("=", Equal)]
    fn symbol(src: &str, tok: Token) {
        test_tokens!(src, [tok]);
    }

    #[test_case("!=", OpNotEqual)]
    #[test_case("=~", RegexEqual)]
    #[test_case("!~", RegexNotEqual)]
    #[test_case("^=", StartsWith)]
    #[test_case("^~", NotStartsWith)]
    fn operator(src: &str, tok: Token) {
        test_tokens!(src, [tok]);
    }

    fn expect_error_containing(s: &str, needle: &str) {
        let mut lex = Token::lexer(s);
        let actual = lex.next().unwrap();
        assert!(actual.is_err());
        let msg = format!("{}", actual.unwrap_err());
        assert!(msg.contains(needle));
    }

    #[test_case("\"\"", StringLiteral; "empty_1")]
    #[test_case("\'\'", StringLiteral; "empty_2")]
    #[test_case("\"hi\"", StringLiteral; "double_1")]
    #[test_case("\"hi\n\"", StringLiteral; "double_2")]
    #[test_case("\"hi\\\"\"", StringLiteral; "double_3")]
    #[test_case("'hi'", StringLiteral; "single_1")]
    #[test_case("'hi\n'", StringLiteral; "single_2")]
    #[test_case("'hi\\''", StringLiteral; "single_3")]
    fn string(src: &str, tok: Token) {
        test_tokens!(src, [tok]);
    }

    #[test]
    fn string_unterminated() {
        fn check(s: &str) {
            expect_error_containing(s, "unterminated string literal");
        }

        check("\"hi");
        check("\'hi");
    }

    #[test]
    fn identifier() {
        test_tokens!("foobar123", [Identifier]);
    }

    #[test]
    fn identifiers() {
        // short
        test_tokens!(
          "m y f i h d s",
          [
            Identifier=>"m",
            Identifier=>"y",
            Identifier=>"f",
            Identifier=>"i",
            Identifier=>"h",
            Identifier=>"d",
            Identifier=>"s"
          ]
        );

        test_tokens!(
          "foo bar123",
          [
            Identifier=>"foo",
            Identifier=>"bar123",
          ]
        );

        // accept dashes in label names
        test_tokens!(
          "foo-bar",
          [
            Identifier=>"foo-bar",
          ]
        );
    }

    #[test]
    fn hash_is_not_a_comment() {
        // `#` is not skipped: it starts no token, so the lexer reports it rather than
        // dropping the rest of the input.
        let mut lex = Token::lexer("a #b");
        assert_eq!(lex.next(), Some(Ok(Identifier)));
        assert!(matches!(lex.next(), Some(Err(_))));
        // Inside a string literal it is an ordinary character.
        test_tokens!(r##""#ff0000""##, [StringLiteral => r##""#ff0000""##]);
    }

    #[test]
    fn double_equals_is_two_tokens() {
        // No grammar accepts `==`, so it is not a token of its own.
        test_tokens!("==", [Equal, Equal]);
    }

    #[test]
    fn junk() {
        let src = "💩";
        let mut lex = Token::lexer(src);
        let actual = lex.next().unwrap();
        assert!(actual.is_err());
    }

    #[test]
    fn metric_name() {
        // Just metric name
        test_success("metric", &["metric"]);

        // Metric name with spec chars
        test_success("foo.bar_", &["foo.bar_"]);
    }

    #[test]
    fn metric_name_with_tag_filters() {
        // Metric name with tag filters
        let s = r#"  metric:12.34{a="foo", b != "bar", c=~ "x.+y", d !~ "zzz"}"#;
        let expected = vec![
            "metric:12.34",
            "{",
            "a",
            "=",
            r#""foo""#,
            ",",
            "b",
            "!=",
            r#""bar""#,
            ",",
            "c",
            "=~",
            r#""x.+y""#,
            ",",
            "d",
            "!~",
            r#""zzz""#,
            "}",
        ];

        test_success(s, &expected);
    }

    #[test]
    fn miscellaneous() {
        let s = "   `foo\\\\бар`  `C:\\`";
        let expected = vec!["`foo\\\\бар`", "`C:\\`"];
        test_success(s, &expected);
    }

    fn test_success(s: &str, expected_tokens: &[&str]) {
        let mut lexer = Token::lexer(s);
        let mut tokens = vec![];
        while let Some(tok) = lexer.next() {
            match tok {
                Ok(_) => {
                    tokens.push(lexer.slice());
                }
                Err(e) => {
                    panic!("unexpected error {e:?}\n {}", lexer.slice())
                }
            }
        }

        assert_eq!(
            tokens.len(),
            expected_tokens.len(),
            "expected {:?} tokens, got {:?}",
            expected_tokens.len(),
            tokens.len()
        );

        for i in 0..tokens.len() {
            let actual = tokens[i];
            let expected = expected_tokens[i];
            assert_eq!(
                actual, expected,
                "expected {expected:?} at index {i}, got {actual:?}"
            );
        }
    }
}
