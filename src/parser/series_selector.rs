use crate::common::constants::METRIC_NAME_LABEL;
use crate::labels::filters::{
    FilterList, LabelFilter, MatchOp, OrFiltersList, PredicateMatch, PredicateValue,
    SeriesSelector, ValueList,
};
use crate::parser::lex::{Token, expect_one_of_tokens, expect_token};
use crate::parser::parse_error::unexpected;
use crate::parser::utils::{extract_string_value, unescape_ident};
use crate::parser::{ParseError, ParseResult};
use logos::{Lexer, Logos};
use smallvec::SmallVec;

const INITIAL_TOKENS: &[Token] = &[
    Token::Identifier,
    Token::OpOr,
    Token::LeftBrace,
    Token::StringLiteral,
];

const SECONDARY_TOKENS: &[Token] = &[
    Token::Equal,
    Token::OpNotEqual,
    Token::RegexEqual,
    Token::RegexNotEqual,
    Token::StartsWith,
    Token::NotStartsWith,
    Token::LeftBrace,
    Token::Eof,
];

/// Parses a series selector, either using RedisTimeSeries or Prometheus syntax.
pub fn parse_series_selector(s: &str) -> ParseResult<SeriesSelector> {
    if s.is_empty() {
        return Err(ParseError::EmptySeriesSelector);
    }
    let mut lex = Token::lexer(s);
    let result = parse_series_selector_internal(&mut lex)?;
    expect_token(&mut lex, Token::Eof)?;
    Ok(result)
}

fn parse_series_selector_internal(p: &mut Lexer<Token>) -> ParseResult<SeriesSelector> {
    use Token::*;

    let mut selectors: SmallVec<[_; 4]> = SmallVec::default();

    loop {
        let (tok, text) = expect_one_of_tokens(p, INITIAL_TOKENS)?;

        let selector = if tok == LeftBrace {
            parse_prometheus_selector_internal(p, String::new())?
        } else {
            let text = parse_identifier_or_string(tok, text)?;
            let (tok, _) = expect_one_of_tokens(p, SECONDARY_TOKENS)?;

            match tok {
                Eof => {
                    let matcher = LabelFilter::equals(METRIC_NAME_LABEL.into(), &text);
                    SeriesSelector::with_filters(vec![matcher])
                }
                LeftBrace => parse_prometheus_selector_internal(p, text)?,
                _ => {
                    let matcher = parse_redis_ts_predicate(text, tok, p)?;
                    SeriesSelector::with_filters(vec![matcher])
                }
            }
        };

        selectors.push(selector);

        let (next_tok, _) = expect_one_of_tokens(p, &[OpOr, Eof])?;
        if next_tok == Eof {
            break;
        }
    }

    if selectors.len() == 1 {
        Ok(selectors.into_iter().next().unwrap())
    } else {
        Ok(selectors
            .into_iter()
            .reduce(|acc, sel| acc.merge_with(sel))
            .unwrap())
    }
}

fn parse_identifier_or_string(tok: Token, text: &str) -> ParseResult<String> {
    match tok {
        Token::Identifier => Ok(unescape_ident(text)?.to_string()),
        Token::StringLiteral => Ok(extract_string_value(text)?.to_string()),
        _ => Ok(text.to_string()),
    }
}

fn parse_prometheus_selector_internal(
    lex: &mut Lexer<Token>,
    name: String,
) -> ParseResult<SeriesSelector> {
    let name = if name.is_empty() { None } else { Some(name) };

    // Check for empty braces pattern
    if is_empty_braces(lex.remainder()) {
        let _ = expect_token(lex, Token::RightBrace)?;
        return Ok(create_selector_with_optional_name(
            name,
            FilterList::default(),
        ));
    }

    parse_label_filters(lex, name)
}

fn is_empty_braces(remainder: &str) -> bool {
    remainder.bytes().find(|&b| !b.is_ascii_whitespace()) == Some(b'}')
}

fn create_selector_with_optional_name(
    name: Option<String>,
    mut filters: FilterList,
) -> SeriesSelector {
    if let Some(name) = name {
        filters.push(LabelFilter::equals(METRIC_NAME_LABEL.into(), &name));
    }
    SeriesSelector::And(filters)
}

/// Support RedisTimeseries style selectors
fn parse_redis_ts_predicate(
    label: String,
    operator_token: Token,
    lex: &mut Lexer<Token>,
) -> ParseResult<LabelFilter> {
    let op: MatchOp = operator_token.try_into()?;

    if op.is_regex() {
        let value = parse_string_literal(lex)?;
        LabelFilter::create(op, label, value)
    } else {
        let value = parse_matcher_value(lex)?;
        create_predicate_filter(label, op, value)
    }
}

fn create_predicate_filter(
    label: String,
    op: MatchOp,
    value: PredicateValue,
) -> ParseResult<LabelFilter> {
    LabelFilter::create_with_value(op, label, value)
}

fn parse_label_filters(p: &mut Lexer<Token>, name: Option<String>) -> ParseResult<SeriesSelector> {
    use Token::*;

    let mut or_matchers: OrFiltersList = OrFiltersList::default();
    let mut current_filters = FilterList::default();
    let mut has_or = false;

    loop {
        if has_or && !current_filters.is_empty() {
            or_matchers.push(std::mem::take(&mut current_filters));
        }

        let (filters, last_token, has_metric_filter) = parse_label_filters_internal(p)?;
        current_filters = add_metric_name_if_needed(filters, &name, has_metric_filter);

        match last_token {
            RightBrace => break,
            OpOr => has_or = true,
            _ => {
                return Err(unexpected(
                    "label filter",
                    last_token.as_str(),
                    "OR or }",
                    None,
                ));
            }
        }
    }

    build_selector_from_filters(current_filters, or_matchers, has_or)
}

fn add_metric_name_if_needed(
    mut filters: FilterList,
    name: &Option<String>,
    has_metric_filter: bool,
) -> FilterList {
    if let Some(name) = name
        && !has_metric_filter
    {
        filters.push(LabelFilter::equals(METRIC_NAME_LABEL.into(), name));
    }
    filters
}

fn build_selector_from_filters(
    filters: FilterList,
    mut or_matchers: OrFiltersList,
    has_or: bool,
) -> ParseResult<SeriesSelector> {
    if filters.is_empty() && or_matchers.is_empty() {
        return Err(ParseError::EmptySeriesSelector);
    }

    if has_or {
        if !filters.is_empty() {
            or_matchers.push(filters);
        }
        Ok(SeriesSelector::Or(or_matchers))
    } else {
        Ok(SeriesSelector::And(filters))
    }
}

fn parse_label_filters_internal(p: &mut Lexer<Token>) -> ParseResult<(FilterList, Token, bool)> {
    use Token::*;

    let mut matchers = FilterList::default();
    let mut metric_name_seen = false;

    loop {
        let (matcher, tok) = parse_label_filter(p, !metric_name_seen)?;

        if matcher.is_metric_name_filter() {
            if metric_name_seen {
                return Err(unexpected(
                    "metric name",
                    matcher.label.as_str(),
                    "only one metric name allowed",
                    None,
                ));
            }
            metric_name_seen = true;
            matchers.insert(0, matcher);
        } else {
            matchers.push(matcher);
        }

        let tok = match tok {
            Some(tok) => tok,
            None => expect_one_of_tokens(p, &[Comma, RightBrace, OpOr])?.0,
        };

        if tok == RightBrace || tok == OpOr {
            return Ok((matchers, tok, metric_name_seen));
        }
    }
}

fn parse_label_filter(
    p: &mut Lexer<Token>,
    accept_single: bool,
) -> ParseResult<(LabelFilter, Option<Token>)> {
    use Token::*;

    let label = expect_label_name(p)?;

    let tokens = if accept_single {
        &[
            Equal,
            OpNotEqual,
            RegexEqual,
            RegexNotEqual,
            StartsWith,
            NotStartsWith,
            Comma,
            RightBrace,
            OpOr,
        ][..]
    } else {
        &[
            Equal,
            OpNotEqual,
            RegexEqual,
            RegexNotEqual,
            StartsWith,
            NotStartsWith,
            Comma,
            RightBrace,
        ][..]
    };

    let (tok, _) = expect_one_of_tokens(p, tokens)?;

    // Handle metric name shorthand
    if matches!(tok, Comma | RightBrace | OpOr) {
        return Ok((
            LabelFilter {
                label: METRIC_NAME_LABEL.into(),
                matcher: PredicateMatch::Equal(PredicateValue::String(label)),
            },
            Some(tok),
        ));
    }

    let op: MatchOp = tok.try_into()?;

    if op.is_regex() {
        let value = parse_string_literal(p)?;
        Ok((LabelFilter::create(op, label, value)?, None))
    } else {
        let value = parse_matcher_value(p)?;
        Ok((create_predicate_filter(label, op, value)?, None))
    }
}

fn expect_label_name(lex: &mut Lexer<Token>) -> ParseResult<String> {
    let (tok, text) = expect_one_of_tokens(lex, &[Token::Identifier, Token::StringLiteral])?;
    match tok {
        Token::Identifier => Ok(unescape_ident(text)?.to_string()),
        Token::StringLiteral => Ok(extract_string_value(text)?.to_string()),
        _ => unreachable!("expect_label_name: unexpected token"),
    }
}

fn parse_string_literal(lexer: &mut Lexer<Token>) -> ParseResult<String> {
    let value = expect_token(lexer, Token::StringLiteral)?;
    Ok(extract_string_value(value)?.to_string())
}

fn parse_matcher_value(lexer: &mut Lexer<Token>) -> ParseResult<PredicateValue> {
    use Token::*;

    let (tok, text) =
        expect_one_of_tokens(lexer, &[StringLiteral, Identifier, Number, LeftParen, Eof])?;

    match tok {
        Eof => Ok(PredicateValue::Empty),
        Identifier | Number => Ok(PredicateValue::String(text.to_string())),
        StringLiteral => {
            let value = extract_string_value(text)?;
            Ok(PredicateValue::String(value.to_string()))
        }
        LeftParen => parse_value_list(lexer),
        _ => unreachable!("parse_matcher_value: unexpected token"),
    }
}

fn parse_value_list(lexer: &mut Lexer<Token>) -> ParseResult<PredicateValue> {
    use Token::*;

    let mut values = ValueList::new();
    let mut expect_value = true;

    loop {
        let tokens = if expect_value {
            &[StringLiteral, Identifier, Number, Comma, RightParen][..]
        } else {
            &[Comma, RightParen][..]
        };

        let (tok, name) = expect_one_of_tokens(lexer, tokens)?;

        match tok {
            Identifier | Number => {
                values.push(name.to_string());
                expect_value = false;
            }
            StringLiteral => {
                let value = extract_string_value(name)?;
                values.push(value.to_string());
                expect_value = false;
            }
            Comma => {
                expect_value = true;
            }
            RightParen => break,
            _ => return Err(unexpected("value", name, ", or )", None)),
        }
    }

    Ok(match values.len() {
        0 => PredicateValue::Empty,
        1 => PredicateValue::String(values.into_iter().next().unwrap()),
        _ => PredicateValue::List(values),
    })
}
