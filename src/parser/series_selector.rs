use crate::common::constants::METRIC_NAME_LABEL;
use crate::labels::filters::{
    FilterList, LabelFilter, MatchOp, OrFiltersList, PredicateMatch, PredicateValue,
    SeriesSelector, ValueList,
};
use crate::parser::lex::{Token, expect_one_of_tokens, expect_token};
use crate::parser::parse_error::unexpected;
use crate::parser::utils::extract_string_value;
use crate::parser::{ParseError, ParseResult};
use logos::{Lexer, Logos};
use smallvec::SmallVec;

const INITIAL_TOKENS: &[Token] = &[
    Token::Identifier,
    Token::OpOr,
    Token::LeftBrace,
    Token::StringLiteral,
];

/// What may follow a bare metric name. An operator is not here: an argument whose first
/// operator comes before any `{` is an RTS-form predicate and never reaches the lexer.
const SECONDARY_TOKENS: &[Token] = &[Token::LeftBrace, Token::Eof];

/// Characters an unquoted RTS-form value may not contain. RTS gives them no consistent meaning
/// (`e=x=y` reads as `e=x`; `e=p,q` and `e=a)b` match nothing), so they are an error here and
/// the braces form, where values are quoted strings, is the way to match them.
const RTS_RESERVED_CHARS: [char; 4] = ['=', '(', ')', ','];

/// Parses a series selector, either using RedisTimeSeries or Prometheus syntax.
///
/// An argument whose first operator (`=`, `!=`, `=~`, `!~`, `^=`, `^~`) comes before any `{` is
/// an RTS-form predicate, `label<op>value`, read the way RedisTimeSeries reads it: the label is
/// the raw text before the operator and the value the raw text after it, untrimmed, with quotes
/// as ordinary characters. Everything else (`{...}`, `name{...}`, a bare metric name, and `or`
/// between those) goes through the Prometheus-style grammar.
pub fn parse_series_selector(s: &str) -> ParseResult<SeriesSelector> {
    if s.is_empty() {
        return Err(ParseError::EmptySeriesSelector);
    }
    if let Some((label, op, value)) = split_rts_predicate(s) {
        let filter = parse_rts_predicate(label, op, value)?;
        return Ok(SeriesSelector::with_filters(vec![filter]));
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
                _ => parse_prometheus_selector_internal(p, text)?,
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
        // The identifier rule admits no `\\`, so there is nothing to unescape.
        Token::Identifier => Ok(text.to_string()),
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

/// Splits `label<op>value` at the first operator, or returns `None` when a `{` comes first (a
/// Prometheus-style selector) or there is no operator (a bare metric name).
///
/// Indexing by byte is safe: every byte matched here is ASCII, and a UTF-8 continuation or
/// lead byte never is.
fn split_rts_predicate(s: &str) -> Option<(&str, MatchOp, &str)> {
    let bytes = s.as_bytes();
    for (i, &b) in bytes.iter().enumerate() {
        let (op, len) = match (b, bytes.get(i + 1)) {
            (b'{', _) => return None,
            (b'=', Some(b'~')) => (MatchOp::RegexEqual, 2),
            (b'=', _) => (MatchOp::Equal, 1),
            (b'!', Some(b'=')) => (MatchOp::NotEqual, 2),
            (b'!', Some(b'~')) => (MatchOp::RegexNotEqual, 2),
            (b'^', Some(b'=')) => (MatchOp::StartsWith, 2),
            (b'^', Some(b'~')) => (MatchOp::NotStartsWith, 2),
            _ => continue,
        };
        return Some((&s[..i], op, &s[i + len..]));
    }
    None
}

/// Builds the filter for an RTS-form predicate. `=` and `!=` are RTS; `=~`, `!~`, `^=` and `^~`
/// are extensions. The regex operators take a quoted string literal (so a pattern can hold any
/// character); the others take a raw value like `=` does.
fn parse_rts_predicate(label: &str, op: MatchOp, value: &str) -> ParseResult<LabelFilter> {
    if label.is_empty() {
        return Err(ParseError::SyntaxError(format!(
            "missing label name before `{op}`"
        )));
    }
    if op.is_regex() {
        let mut lex = Token::lexer(value);
        let pattern = parse_string_literal(&mut lex)?;
        expect_token(&mut lex, Token::Eof)?;
        return LabelFilter::create(op, label, pattern);
    }
    LabelFilter::create_with_value(op, label, parse_rts_value(value)?)
}

/// Reads an RTS-form value: empty (the label is absent), `(v1,v2,...)`, or one raw value.
/// Nothing is trimmed or unquoted, matching RTS: `( a)` lists `" a"`, and `"x"` is three
/// characters long.
fn parse_rts_value(raw: &str) -> ParseResult<PredicateValue> {
    let reserved = |text: &str| {
        text.chars().find(|c| RTS_RESERVED_CHARS.contains(c)).map(|c| {
            ParseError::SyntaxError(format!(
                "unquoted value `{raw}` contains `{c}`; match it with the {{label=\"value\"}} form"
            ))
        })
    };

    if raw.is_empty() {
        return Ok(PredicateValue::Empty);
    }
    let Some(list) = raw.strip_prefix('(') else {
        return match reserved(raw) {
            Some(err) => Err(err),
            None => Ok(PredicateValue::String(raw.to_string())),
        };
    };
    let Some(list) = list.strip_suffix(')') else {
        return Err(ParseError::SyntaxError(format!(
            "value list `{raw}` must end with `)`"
        )));
    };
    if list.is_empty() {
        return Err(ParseError::SyntaxError("empty value list `()`".to_string()));
    }

    let mut values = ValueList::new();
    for item in list.split(',') {
        if item.is_empty() {
            return Err(ParseError::SyntaxError(format!(
                "empty item in value list `{raw}`"
            )));
        }
        if let Some(err) = reserved(item) {
            return Err(err);
        }
        values.push(item.to_string());
    }
    Ok(if values.len() == 1 {
        PredicateValue::String(values.swap_remove(0))
    } else {
        PredicateValue::List(values)
    })
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

        let (filters, last_token) = parse_label_filters_internal(p, name.as_deref())?;
        current_filters = add_outer_metric_name(filters, &name);

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

/// Adds the metric name written before the braces to one OR branch. Every branch gets it:
/// `parse_label_filter` has already rejected a branch that names the metric again.
fn add_outer_metric_name(mut filters: FilterList, name: &Option<String>) -> FilterList {
    if let Some(name) = name {
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

fn parse_label_filters_internal(
    p: &mut Lexer<Token>,
    outer_name: Option<&str>,
) -> ParseResult<(FilterList, Token)> {
    use Token::*;

    let mut matchers = FilterList::default();
    let mut metric_name_seen = false;

    loop {
        let (matcher, tok) = parse_label_filter(p, !metric_name_seen, outer_name)?;

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
            return Ok((matchers, tok));
        }
        // One trailing comma before `}` is allowed, as in PromQL and metric definitions.
        let mut ahead = p.clone();
        if let Some(Ok(RightBrace)) = ahead.next() {
            *p = ahead;
            return Ok((matchers, RightBrace));
        }
    }
}

/// Parses one matcher inside braces.
///
/// `outer_name` is the metric name written before the braces, if any. As in Prometheus, a
/// selector that has one must not name the metric again inside the braces, with any operator
/// or the bare-name shorthand: `cpu{__name__="mem"}`, `cpu{__name__=~"m.*"}` and `cpu{mem}` are
/// all rejected. The check looks at the label name as written and runs before
/// `LabelFilter::create`, so regex decomposition (which turns `=~"mem"` into `="mem"`) can't
/// change the outcome.
fn parse_label_filter(
    p: &mut Lexer<Token>,
    accept_single: bool,
    outer_name: Option<&str>,
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
        if let Some(outer) = outer_name {
            let inner = PredicateValue::String(label).to_string();
            return Err(ParseError::MetricNameSetTwice(outer.to_string(), inner));
        }
        return Ok((
            LabelFilter {
                label: METRIC_NAME_LABEL.into(),
                matcher: PredicateMatch::Equal(PredicateValue::String(label)),
            },
            Some(tok),
        ));
    }

    let op: MatchOp = tok.try_into()?;
    let names_metric_again = outer_name.filter(|_| label == METRIC_NAME_LABEL);

    if op.is_regex() {
        let value = parse_string_literal(p)?;
        if let Some(outer) = names_metric_again {
            let inner = PredicateValue::String(value).to_string();
            return Err(ParseError::MetricNameSetTwice(outer.to_string(), inner));
        }
        Ok((LabelFilter::create(op, label, value)?, None))
    } else {
        let value = parse_matcher_value(p)?;
        if let Some(outer) = names_metric_again {
            return Err(ParseError::MetricNameSetTwice(
                outer.to_string(),
                value.to_string(),
            ));
        }
        Ok((create_predicate_filter(label, op, value)?, None))
    }
}

fn expect_label_name(lex: &mut Lexer<Token>) -> ParseResult<String> {
    let (tok, text) = expect_one_of_tokens(lex, &[Token::Identifier, Token::StringLiteral])?;
    match tok {
        // The identifier rule admits no `\\`, so there is nothing to unescape.
        Token::Identifier => Ok(text.to_string()),
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
