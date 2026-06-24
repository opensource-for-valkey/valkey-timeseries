use super::ForecastModelKind;
use logos::Logos;
use std::collections::HashSet;
use std::fmt::Display;

#[derive(Clone, PartialEq)]
pub enum ValueType {
    Number,
    Ident,
    String,
    Flag,
    List, // For future use if we want to support list-style kwargs like "seasonal_periods=[12,24]"
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
    Flag(bool),           // For flag-style kwargs like "seasonal=True"
    List(Vec<SpecValue>), // to support list-style kwargs like "seasonal_periods=[12,24]"
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
    fn value_type(&self) -> ValueType {
        match self {
            SpecValue::Number(_) => ValueType::Number,
            SpecValue::Ident(_) => ValueType::Ident,
            SpecValue::String(_) => ValueType::String,
            SpecValue::Flag(_) => ValueType::Flag,
            SpecValue::List(_) => ValueType::List,
        }
    }

    pub fn as_float(&self) -> Result<f64, ModelSpecError> {
        if let SpecValue::Number(n) = self {
            Ok(*n)
        } else {
            Err(ModelSpecError::new("expected a float value"))
        }
    }

    pub fn as_usize(&self) -> Result<usize, ModelSpecError> {
        match self {
            SpecValue::Number(n) if *n >= 0.0 && n.fract() == 0.0 => Ok(*n as usize),
            _ => Err(ModelSpecError::new("expected a non-negative integer value")),
        }
    }

    pub fn as_ident(&self) -> Option<&str> {
        match self {
            SpecValue::Ident(s) | SpecValue::String(s) => Some(s.as_str()),
            _ => None,
        }
    }

    pub fn as_flag(&self) -> Result<bool, ModelSpecError> {
        if let SpecValue::Flag(value) = self {
            Ok(*value)
        } else {
            Err(ModelSpecError::new("expected a flag value"))
        }
    }

    pub fn as_usize_list(&self) -> Result<Vec<usize>, ModelSpecError> {
        if let SpecValue::List(items) = self {
            items
                .iter()
                .map(|x| x.as_usize())
                .collect::<Result<Vec<_>, _>>()
        } else {
            Err(ModelSpecError::new("Expected a list value"))
        }
    }

    pub fn as_float_list(&self) -> Result<Vec<f64>, ModelSpecError> {
        if let SpecValue::List(items) = self {
            items
                .iter()
                .map(|x| x.as_float())
                .collect::<Result<Vec<_>, _>>()
        } else {
            Err(ModelSpecError::new("Expected a list value"))
        }
    }
}

pub type KeywordArgs = Vec<(String, SpecValue)>;

#[derive(Debug, Clone)]
pub struct ModelSpec {
    pub model_name: String,
    pub model_type: ForecastModelKind,
    pub positional_args: Vec<SpecValue>,
    pub keyword_args: KeywordArgs,
}

impl ModelSpec {
    pub fn ensure_arity(&self, expected: usize) -> Result<(), ModelSpecError> {
        let actual = self.positional_args.len();
        if actual == expected {
            Ok(())
        } else {
            Err(ModelSpecError::new(format!(
                "{} expects {expected} positional args, got {actual}",
                self.model_name,
            )))
        }
    }

    pub fn get_kwarg(&self, key: &str) -> Option<&SpecValue> {
        self.keyword_args
            .iter()
            .find(|(k, _)| k == key)
            .map(|(_, v)| v)
    }

    pub(super) fn remove_kwarg(&mut self, key: &str) -> Option<SpecValue> {
        if let Some(pos) = self.keyword_args.iter().position(|(k, _)| k == key) {
            Some(self.keyword_args.remove(pos).1)
        } else {
            None
        }
    }

    pub fn get_kwarg_as_number(&self, key: &str) -> Result<Option<f64>, ModelSpecError> {
        if let Some(value) = self.get_kwarg(key) {
            match value {
                SpecValue::Number(n) => Ok(Some(*n)),
                _ => Err(ModelSpecError::new(format!(
                    "Expected keyword argument '{key}' for model {} to be a number",
                    self.model_name
                ))),
            }
        } else {
            Ok(None)
        }
    }

    pub fn get_kwarg_as_flag(&self, key: &str) -> Result<Option<bool>, ModelSpecError> {
        if let Some(value) = self.get_kwarg(key) {
            match value {
                SpecValue::Flag(n) => Ok(Some(*n)),
                _ => Err(ModelSpecError::new(format!(
                    "Expected keyword argument '{key}' for model {} to be a flag",
                    self.model_name
                ))),
            }
        } else {
            Ok(None)
        }
    }

    pub fn get_kwarg_as_ident(&self, key: &str) -> Option<&str> {
        self.get_kwarg(key).and_then(|v| match v {
            SpecValue::Ident(s) | SpecValue::String(s) => Some(s.as_str()),
            _ => None,
        })
    }

    pub fn get_usize_kwarg(&mut self, key: &str) -> Result<Option<usize>, ModelSpecError> {
        if let Some(arg) = self.remove_kwarg(key) {
            let value = arg.as_usize().map_err(|_| {
                ModelSpecError::new(format!("{} must be a non-negative integer", key))
            })?;
            return Ok(Some(value));
        }
        Ok(None)
    }

    pub fn expect_kwarg_as_float_list(
        &mut self,
        key: &str,
    ) -> Result<Option<Vec<f64>>, ModelSpecError> {
        match self.remove_kwarg(key) {
            Some(value) => {
                let value = value.as_float_list().map_err(|_| {
                    ModelSpecError::new(format!(
                        "Expected argument '{key}' for model {} to be a list of float values",
                        self.model_name
                    ))
                })?;
                Ok(Some(value))
            }
            None => Ok(None),
        }
    }

    pub fn get_positionals_as_numbers(&self) -> Result<Vec<f64>, ModelSpecError> {
        self.positional_args
            .iter()
            .map(|arg| match arg {
                SpecValue::Number(n) => Ok(*n),
                _ => Err(ModelSpecError::new(format!(
                    "Expected all positional arguments for model {} to be numbers",
                    self.model_name
                ))),
            })
            .collect()
    }

    pub fn get_positionals_as_usize(&self) -> Result<Vec<usize>, ModelSpecError> {
        self.positional_args
            .iter()
            .map(as_usize)
            .collect::<Result<Vec<_>, _>>()
    }
}

impl Display for ModelSpec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}(", self.model_name)?;
        for (i, arg) in self.positional_args.iter().enumerate() {
            if i > 0 {
                write!(f, ", ")?;
            }
            write!(f, "{}", arg)?;
        }
        for (i, (k, v)) in self.keyword_args.iter().enumerate() {
            if i == 0 && self.positional_args.is_empty() {
                write!(f, "{}={}", k, v)?;
            } else {
                write!(f, ", {}={}", k, v)?;
            }
        }
        write!(f, ")")?;
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct ModelSpecError {
    message: String,
}

impl ModelSpecError {
    pub(crate) fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

impl Display for ModelSpecError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.message)
    }
}

impl std::error::Error for ModelSpecError {}

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
    #[regex(r#""([^"\\]|\\.)*"|'([^'\\]|\\.)*'"#, parse_string)]
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

pub fn parse_model_specs(input: &str) -> Result<Vec<ModelSpec>, ModelSpecError> {
    let lexer = Token::lexer(input);
    let mut tokens = Vec::new();
    for token in lexer {
        let token =
            token.map_err(|_| ModelSpecError::new("Invalid token in model specification"))?;
        tokens.push(token);
    }
    let mut parser = Parser { tokens, pos: 0 };
    parser.parse_input()
}

pub(super) fn get_usize_kwarg(
    spec: &mut ModelSpec,
    key: &str,
) -> Result<Option<usize>, ModelSpecError> {
    if let Some(arg) = spec.remove_kwarg(key) {
        return match arg {
            SpecValue::Number(n) if n >= 0.0 && n.fract() == 0.0 => Ok(Some(n as usize)),
            _ => Err(ModelSpecError::new(format!(
                "{} must be a non-negative integer",
                key
            ))),
        };
    }
    Ok(None)
}

pub(super) fn get_float_kwarg(
    spec: &mut ModelSpec,
    key: &str,
) -> Result<Option<f64>, ModelSpecError> {
    if let Some(arg) = spec.remove_kwarg(key) {
        match arg {
            SpecValue::Number(n) => Ok(Some(n)),
            _ => Err(ModelSpecError::new(format!(
                "Expected argument '{}' for model {} to be a float value",
                key, spec.model_name
            ))),
        }
    } else {
        Ok(None)
    }
}

pub(super) fn get_kwarg_as_flag(
    spec: &mut ModelSpec,
    key: &str,
) -> Result<Option<bool>, ModelSpecError> {
    if let Some(value) = spec.remove_kwarg(key) {
        match value {
            SpecValue::Flag(n) => Ok(Some(n)),
            _ => Err(ModelSpecError::new(format!(
                "Expected argument '{}' for model {} to be a flag",
                key, spec.model_name
            ))),
        }
    } else {
        Ok(None)
    }
}

pub(super) fn value_as_usize_list(value: &SpecValue) -> Result<Vec<usize>, ModelSpecError> {
    if let SpecValue::List(items) = value {
        let mut numbers = Vec::new();
        for item in items {
            if let SpecValue::Number(n) = item {
                if *n >= 1.0 && n.fract() == 0.0 {
                    numbers.push(*n as usize);
                } else {
                    return Err(ModelSpecError::new(format!(
                        "Expected all items in list to be positive integers, got {n}"
                    )));
                }
            } else {
                return Err(ModelSpecError::new(
                    "Expected all items in list to be numbers",
                ));
            }
        }
        Ok(numbers)
    } else {
        Err(ModelSpecError::new("Expected a list value"))
    }
}

fn as_usize(value: &SpecValue) -> Result<usize, ModelSpecError> {
    match value {
        SpecValue::Number(n) if *n >= 0.0 && n.fract() == 0.0 => Ok(*n as usize),
        _ => Err(ModelSpecError::new(
            "Canonical signatures require integer positional values",
        )),
    }
}

struct Parser {
    tokens: Vec<Token>,
    pos: usize,
}

impl Parser {
    fn parse_input(&mut self) -> Result<Vec<ModelSpec>, ModelSpecError> {
        let has_brackets = self.consume(&Token::LBracket);
        let specs = self.parse_model_spec_sequence(has_brackets)?;

        if has_brackets {
            self.expect(
                &Token::RBracket,
                "Expected ']' after model specification list",
            )?;
        }

        if !self.is_eof() {
            return Err(ModelSpecError::new(
                "Unexpected token after model specification",
            ));
        }

        Ok(specs)
    }

    fn parse_model_spec_sequence(
        &mut self,
        bracketed: bool,
    ) -> Result<Vec<ModelSpec>, ModelSpecError> {
        let mut specs = Vec::new();
        if self.is_eof() || (bracketed && self.peek() == Some(&Token::RBracket)) {
            return Ok(specs);
        }

        specs.push(self.parse_model_spec()?);
        while self.consume(&Token::Comma) {
            let at_sequence_end = if bracketed {
                self.peek() == Some(&Token::RBracket)
            } else {
                self.is_eof()
            };

            if at_sequence_end {
                return Err(ModelSpecError::new(
                    "Trailing comma after model specification",
                ));
            }

            specs.push(self.parse_model_spec()?);
        }

        Ok(specs)
    }

    fn parse_model_spec(&mut self) -> Result<ModelSpec, ModelSpecError> {
        let model_name = match self.next() {
            Some(Token::Ident(name)) => name,
            _ => return Err(ModelSpecError::new("Expected model name")),
        };
        let model_type: ForecastModelKind = model_name
            .parse()
            .map_err(|_| ModelSpecError::new(format!("Unsupported model name {}", model_name)))?;

        // Allow models with no-arg form without parentheses, e.g., "AutoARIMA, ETS"
        // If the next token is a '(', parse the argument list; otherwise assume empty args.
        let (positional_args, keyword_args) = if self.consume(&Token::LParen) {
            let (positional_args, keyword_args) = self.parse_arg_list()?;
            self.expect(&Token::RParen, "Expected ')' after argument list")?;
            (positional_args, keyword_args)
        } else {
            (Vec::new(), Vec::new())
        };
        Ok(ModelSpec {
            model_name,
            model_type,
            positional_args,
            keyword_args,
        })
    }

    fn parse_arg_list(&mut self) -> Result<(Vec<SpecValue>, KeywordArgs), ModelSpecError> {
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
                    return Err(ModelSpecError::new(format!(
                        "Duplicate keyword argument {}",
                        key
                    )));
                }
                let value = self.parse_value()?;
                keyword.push((key, value));
            } else {
                if seen_keyword {
                    return Err(ModelSpecError::new(
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

    fn parse_value(&mut self) -> Result<SpecValue, ModelSpecError> {
        match self.next() {
            Some(Token::Number(v)) => Ok(SpecValue::Number(v)),
            Some(Token::Ident(v)) => Ok(SpecValue::Ident(v)),
            Some(Token::String(v)) => Ok(SpecValue::String(v)),
            Some(Token::Boolean(v)) => Ok(SpecValue::Flag(v)),
            Some(Token::LBracket) => self.parse_list_value(),
            _ => Err(ModelSpecError::new("Expected value")),
        }
    }

    fn parse_list_value(&mut self) -> Result<SpecValue, ModelSpecError> {
        let mut items = Vec::new();

        // Allow empty list
        if self.peek() != Some(&Token::RBracket) {
            loop {
                items.push(self.parse_value()?);

                // Allow trailing comma before closing bracket.
                if !self.consume(&Token::Comma) || self.peek() == Some(&Token::RBracket) {
                    break;
                }
            }
        }

        self.expect(&Token::RBracket, "Expected ']' to close list value")?;
        Ok(SpecValue::List(items))
    }

    fn expect(&mut self, token: &Token, message: &str) -> Result<(), ModelSpecError> {
        if self.consume(token) {
            Ok(())
        } else {
            Err(ModelSpecError::new(message))
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

#[cfg(test)]
mod tests {
    use super::{SpecValue, parse_model_specs};

    #[test]
    fn parses_multiple_models_with_numbers_and_ident_values() {
        let specs = parse_model_specs("ARIMA(1, -2, 3e1), ETS(), AutoETS()").unwrap();
        assert_eq!(specs.len(), 3);
        assert_eq!(specs[0].model_name, "ARIMA");
        assert_eq!(specs[0].positional_args[0], SpecValue::Number(1.0));
        assert_eq!(specs[0].positional_args[1], SpecValue::Number(-2.0));
        assert_eq!(specs[0].positional_args[2], SpecValue::Number(30.0));
    }

    #[test]
    fn allows_trailing_comma_in_model_args() {
        let specs = parse_model_specs("ARIMA(1,2,3,)").unwrap();
        assert_eq!(specs.len(), 1);
        assert_eq!(specs[0].positional_args.len(), 3);
    }

    #[test]
    fn rejects_duplicate_kwargs() {
        let err = parse_model_specs("ARIMA(p=1,p=2)").unwrap_err();
        assert!(err.to_string().contains("Duplicate keyword"));
    }

    #[test]
    fn rejects_positional_after_keyword() {
        let err = parse_model_specs("ARIMA(p=1,2)").unwrap_err();
        assert!(err.to_string().contains("Positional arguments"));
    }

    #[test]
    fn rejects_unknown_model_name() {
        let err = parse_model_specs("Unknown(1)").unwrap_err();
        assert!(err.to_string().contains("Unsupported model"));
    }

    #[test]
    fn parses_list_in_kwargs() {
        let specs = parse_model_specs("TBATS(periods=[12, 24, 7])").unwrap();
        assert_eq!(specs.len(), 1);
        assert_eq!(specs[0].model_name, "TBATS");
        let (_, val) = &specs[0].keyword_args[0];
        match val {
            SpecValue::List(items) => {
                assert_eq!(items.len(), 3);
                assert_eq!(items[0], SpecValue::Number(12.0));
                assert_eq!(items[1], SpecValue::Number(24.0));
                assert_eq!(items[2], SpecValue::Number(7.0));
            }
            other => panic!("Expected list, got {:?}", other),
        }
    }

    #[test]
    fn parses_list_in_positional_args_with_trailing_comma() {
        let specs = parse_model_specs("TBATS([12, 24, 7,])").unwrap();
        assert_eq!(specs.len(), 1);
        assert_eq!(specs[0].model_name, "TBATS");
        let val = &specs[0].positional_args[0];
        match val {
            SpecValue::List(items) => {
                assert_eq!(items.len(), 3);
                assert_eq!(items[0], SpecValue::Number(12.0));
                assert_eq!(items[1], SpecValue::Number(24.0));
                assert_eq!(items[2], SpecValue::Number(7.0));
            }
            other => panic!("Expected list, got {:?}", other),
        }
    }

    #[test]
    fn parses_bare_model_names_without_parentheses() {
        let specs = parse_model_specs("AutoARIMA, AutoETS, ETS").unwrap();
        assert_eq!(specs.len(), 3);
        assert_eq!(specs[0].model_name, "AutoARIMA");
        assert!(specs[0].positional_args.is_empty());
        assert!(specs[0].keyword_args.is_empty());

        assert_eq!(specs[1].model_name, "AutoETS");
        assert!(specs[1].positional_args.is_empty());
        assert!(specs[1].keyword_args.is_empty());

        assert_eq!(specs[2].model_name, "ETS");
        assert!(specs[2].positional_args.is_empty());
        assert!(specs[2].keyword_args.is_empty());
    }

    #[test]
    fn parses_bracketed_model_spec_list() {
        let specs = parse_model_specs("[ARIMA(1,2,3), ETS, AutoETS()]").unwrap();
        assert_eq!(specs.len(), 3);
        assert_eq!(specs[0].model_name, "ARIMA");
        assert_eq!(specs[1].model_name, "ETS");
        assert_eq!(specs[2].model_name, "AutoETS");
    }

    #[test]
    fn parses_empty_bracketed_model_spec_list() {
        let specs = parse_model_specs("[]").unwrap();
        assert!(specs.is_empty());
    }
}
