use logos::Logos;
use std::collections::HashSet;
use std::fmt::Display;

#[derive(Clone, PartialEq)]
pub enum ValueType {
    Number,
    Ident,
    String,
    Flag,
    List,
}

impl Display for ValueType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ValueType::Number => write!(f, "number"),
            ValueType::Ident => write!(f, "ident"),
            ValueType::String => write!(f, "string"),
            ValueType::Flag => write!(f, "flag"),
            ValueType::List => write!(f, "list"),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum SpecValue {
    Number(f64),
    Ident(String),
    String(String),
    Flag(bool),
    List(Vec<SpecValue>),
}

impl Display for SpecValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SpecValue::Number(n) => write!(f, "{}", n),
            SpecValue::Ident(s) | SpecValue::String(s) => write!(f, "{}", s),
            SpecValue::Flag(s) => write!(f, "{}", s),
            SpecValue::List(vals) => {
                let vals_str = vals
                    .iter()
                    .map(|v| v.to_string())
                    .collect::<Vec<_>>()
                    .join(", ");
                write!(f, "[{}]", vals_str)
            }
        }
    }
}

impl SpecValue {
    pub fn value_type(&self) -> ValueType {
        match self {
            SpecValue::Number(_) => ValueType::Number,
            SpecValue::Ident(_) => ValueType::Ident,
            SpecValue::String(_) => ValueType::String,
            SpecValue::Flag(_) => ValueType::Flag,
            SpecValue::List(_) => ValueType::List,
        }
    }

    pub fn as_float(&self) -> Result<f64, SpecError> {
        if let SpecValue::Number(n) = self {
            Ok(*n)
        } else {
            Err(SpecError::new("expected a float value"))
        }
    }

    pub fn as_usize(&self) -> Result<usize, SpecError> {
        match self {
            SpecValue::Number(n) if *n >= 0.0 && n.fract() == 0.0 => Ok(*n as usize),
            _ => Err(SpecError::new("expected a non-negative integer value")),
        }
    }

    pub fn as_ident(&self) -> Option<&str> {
        match self {
            SpecValue::Ident(s) | SpecValue::String(s) => Some(s.as_str()),
            _ => None,
        }
    }

    pub fn as_flag(&self) -> Result<bool, SpecError> {
        if let SpecValue::Flag(value) = self {
            Ok(*value)
        } else {
            Err(SpecError::new("expected a flag value"))
        }
    }

    pub fn as_usize_list(&self) -> Result<Vec<usize>, SpecError> {
        if let SpecValue::List(items) = self {
            items.iter().map(|x| x.as_usize()).collect::<Result<Vec<_>, _>>()
        } else {
            Err(SpecError::new("Expected a list value"))
        }
    }

    pub fn as_float_list(&self) -> Result<Vec<f64>, SpecError> {
        if let SpecValue::List(items) = self {
            items.iter().map(|x| x.as_float()).collect::<Result<Vec<_>, _>>()
        } else {
            Err(SpecError::new("Expected a list value"))
        }
    }
}

pub type KeywordArgs = Vec<(String, SpecValue)>;

#[derive(Debug, Clone)]
pub struct ParsedSpec {
    pub name: String,
    pub positional_args: Vec<SpecValue>,
    pub keyword_args: KeywordArgs,
}

#[derive(Debug, Clone, PartialEq)]
pub struct SpecError {
    message: String,
}

impl SpecError {
    pub(crate) fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

impl Display for SpecError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.message)
    }
}

impl std::error::Error for SpecError {}

#[derive(Logos, Debug, Clone, PartialEq)]
#[logos(skip r"[ \t\n\r]+")]
enum Token {
    #[token("(")]
    LParen,
    #[token(")")]
    RParen,
    #[token("[")]
    LBracket,
    #[token("]")]
    RBracket,
    #[token(",")]
    Comma,
    #[token("=")]
    Eq,
    #[regex(r#"\"([^\"\\]|\\.)*\"|'([^'\\]|\\.)*'"#, parse_string)]
    String(String),
    #[regex(r"[+-]?(?:\d+\.\d*|\d*\.\d+|\d+)(?:[eE][+-]?\d+)?", parse_number)]
    Number(f64),
    #[regex(r"[A-Za-z_][A-Za-z0-9_]*", parse_ident)]
    Ident(String),
    #[regex(r"(?i)(true|false)", |lex| {
        let slice = lex.slice().to_ascii_lowercase();
        Some(slice == "true")
    })]
    Boolean(bool),
}

fn parse_string(lex: &mut logos::Lexer<Token>) -> Option<String> {
    let slice = lex.slice();
    let mut chars = slice.chars();
    let quote = chars.next()?;
    let mut out = String::new();
    let mut escaped = false;
    for ch in chars {
        if !escaped && ch == quote {
            break;
        }
        if escaped {
            out.push(ch);
            escaped = false;
        } else if ch == '\\' {
            escaped = true;
        } else {
            out.push(ch);
        }
    }
    Some(out)
}

fn parse_number(lex: &mut logos::Lexer<Token>) -> Option<f64> {
    lex.slice().parse::<f64>().ok()
}

fn parse_ident(lex: &mut logos::Lexer<Token>) -> Option<String> {
    Some(lex.slice().to_string())
}

pub fn parse_specs(input: &str) -> Result<Vec<ParsedSpec>, SpecError> {
    let lexer = Token::lexer(input);
    let mut tokens = Vec::new();
    for token in lexer {
        let token = token.map_err(|_| SpecError::new("Invalid token in model specification"))?;
        tokens.push(token);
    }
    let mut parser = Parser { tokens, pos: 0 };
    parser.parse_input()
}

struct Parser {
    tokens: Vec<Token>,
    pos: usize,
}

impl Parser {
    fn parse_input(&mut self) -> Result<Vec<ParsedSpec>, SpecError> {
        let has_brackets = self.consume(&Token::LBracket);
        let specs = self.parse_spec_sequence(has_brackets)?;

        if has_brackets {
            self.expect(&Token::RBracket, "Expected ']' after model specification list")?;
        }

        if !self.is_eof() {
            return Err(SpecError::new("Unexpected token after model specification"));
        }

        Ok(specs)
    }

    fn parse_spec_sequence(&mut self, bracketed: bool) -> Result<Vec<ParsedSpec>, SpecError> {
        let mut specs = Vec::new();
        if self.is_eof() || (bracketed && self.peek() == Some(&Token::RBracket)) {
            return Ok(specs);
        }

        specs.push(self.parse_spec()?);
        while self.consume(&Token::Comma) {
            let at_sequence_end = if bracketed {
                self.peek() == Some(&Token::RBracket)
            } else {
                self.is_eof()
            };

            if at_sequence_end {
                return Err(SpecError::new("Trailing comma after model specification"));
            }

            specs.push(self.parse_spec()?);
        }

        Ok(specs)
    }

    fn parse_spec(&mut self) -> Result<ParsedSpec, SpecError> {
        let name = match self.next() {
            Some(Token::Ident(name)) => name,
            _ => return Err(SpecError::new("Expected model name")),
        };

        let (positional_args, keyword_args) = if self.consume(&Token::LParen) {
            let (positional_args, keyword_args) = self.parse_arg_list()?;
            self.expect(&Token::RParen, "Expected ')' after argument list")?;
            (positional_args, keyword_args)
        } else {
            (Vec::new(), Vec::new())
        };

        Ok(ParsedSpec {
            name,
            positional_args,
            keyword_args,
        })
    }

    fn parse_arg_list(&mut self) -> Result<(Vec<SpecValue>, KeywordArgs), SpecError> {
        let mut positional = Vec::new();
        let mut keyword = Vec::new();
        let mut seen_keyword = false;
        let mut seen_keys = HashSet::new();

        if self.peek() == Some(&Token::RParen) {
            return Ok((positional, keyword));
        }

        loop {
            if matches!(self.peek(), Some(Token::Ident(_))) && self.peek_n(1) == Some(&Token::Eq) {
                seen_keyword = true;
                let key = match self.next() {
                    Some(Token::Ident(v)) => v,
                    _ => unreachable!(),
                };
                self.expect(&Token::Eq, "Expected '=' after keyword")?;
                if !seen_keys.insert(key.clone()) {
                    return Err(SpecError::new(format!("Duplicate keyword argument {}", key)));
                }
                let value = self.parse_value()?;
                keyword.push((key, value));
            } else {
                if seen_keyword {
                    return Err(SpecError::new(
                        "Positional arguments are not allowed after keyword arguments",
                    ));
                }
                positional.push(self.parse_value()?);
            }

            if !self.consume(&Token::Comma) || self.peek() == Some(&Token::RParen) {
                break;
            }
        }

        Ok((positional, keyword))
    }

    fn parse_value(&mut self) -> Result<SpecValue, SpecError> {
        match self.next() {
            Some(Token::Number(v)) => Ok(SpecValue::Number(v)),
            Some(Token::Ident(v)) => Ok(SpecValue::Ident(v)),
            Some(Token::String(v)) => Ok(SpecValue::String(v)),
            Some(Token::Boolean(v)) => Ok(SpecValue::Flag(v)),
            Some(Token::LBracket) => self.parse_list_value(),
            _ => Err(SpecError::new("Expected value")),
        }
    }

    fn parse_list_value(&mut self) -> Result<SpecValue, SpecError> {
        let mut items = Vec::new();

        if self.peek() != Some(&Token::RBracket) {
            loop {
                items.push(self.parse_value()?);
                if !self.consume(&Token::Comma) || self.peek() == Some(&Token::RBracket) {
                    break;
                }
            }
        }

        self.expect(&Token::RBracket, "Expected ']' to close list value")?;
        Ok(SpecValue::List(items))
    }

    fn expect(&mut self, token: &Token, message: &str) -> Result<(), SpecError> {
        if self.consume(token) {
            Ok(())
        } else {
            Err(SpecError::new(message))
        }
    }

    fn consume(&mut self, token: &Token) -> bool {
        if self.peek() == Some(token) {
            self.pos += 1;
            true
        } else {
            false
        }
    }

    fn peek(&self) -> Option<&Token> {
        self.tokens.get(self.pos)
    }

    fn peek_n(&self, offset: usize) -> Option<&Token> {
        self.tokens.get(self.pos + offset)
    }

    fn next(&mut self) -> Option<Token> {
        let value = self.tokens.get(self.pos).cloned();
        if value.is_some() {
            self.pos += 1;
        }
        value
    }

    fn is_eof(&self) -> bool {
        self.pos >= self.tokens.len()
    }
}
