use crate::common::constants::METRIC_NAME_LABEL;
use crate::labels::filters::{
    FilterList, LabelFilter, MatchOp, OrFiltersList, PredicateMatch, PredicateValue,
    SeriesSelector, ValueList,
};
use crate::parser::lex::{Cursor, Lexeme, Token};
use crate::parser::{ParseError, ParseResult};

/// Characters an unquoted RTS-form value may not contain. RTS gives them no consistent meaning
/// (`e=x=y` reads as `e=x`; `e=p,q` and `e=a)b` match nothing), so they are an error here and
/// the braces form, where values are quoted strings, is the way to match them.
const RTS_RESERVED_CHARS: [char; 4] = ['=', '(', ')', ','];

const MATCH_OPERATORS: &[Token] = &[
    Token::Equal,
    Token::OpNotEqual,
    Token::RegexEqual,
    Token::RegexNotEqual,
    Token::StartsWith,
    Token::NotStartsWith,
];

/// Tokens that can be a name: `or` is a keyword only between selectors and between groups of
/// matchers, and a name anywhere a name is expected (`{or="1"}`, a metric called `or`).
const NAME_TOKENS: &[Token] = &[Token::Identifier, Token::StringLiteral, Token::OpOr];

/// Parses a series selector, either using RedisTimeSeries or Prometheus syntax.
///
/// An argument whose first operator (`=`, `!=`, `=~`, `!~`, `^=`, `^~`) comes before any `{` is
/// an RTS-form predicate, `label<op>value`, read the way RedisTimeSeries reads it: the label is
/// the raw text before the operator and the value the raw text after it, untrimmed, with quotes
/// as ordinary characters. Everything else goes through the Prometheus-style grammar:
///
/// ```text
/// selectors := selector ( "or" selector )*
/// selector  := name [ "{" [ group ( "or" group )* ] "}" ] | "{" [ group ( "or" group )* ] "}"
/// group     := matcher ( "," matcher )* [ "," ]
/// matcher   := name [ op value ]          (a bare name is a metric-name matcher)
/// value     := string | identifier | number | "(" value ( "," value )* ")"
/// ```
pub fn parse_series_selector(s: &str) -> ParseResult<SeriesSelector> {
    if s.trim().is_empty() {
        return Err(ParseError::EmptySeriesSelector);
    }
    // DIV-0068: no stored label holds a NUL. A raw one is refused before parsing; one a quoted
    // string decodes (`\x00`) is caught on the parsed matchers below.
    if s.contains('\0') {
        return Err(ParseError::NulInFilter);
    }
    let selector = if let Some((label, op, value_at)) = split_rts_predicate(s) {
        let filter = parse_rts_predicate(s, label, op, value_at)?;
        SeriesSelector::with_filters(vec![filter])
    } else {
        let mut cur = Cursor::new(s);
        let mut selector = parse_one_selector(&mut cur)?;
        while cur.next_if(Token::OpOr)?.is_some() {
            selector = selector.or_with(parse_one_selector(&mut cur)?);
        }
        cur.expect(&[Token::OpOr, Token::Eof], "series selector")?;
        selector
    };
    if selector
        .groups()
        .flat_map(|group| group.iter())
        .any(LabelFilter::contains_nul)
    {
        return Err(ParseError::NulInFilter);
    }
    Ok(selector)
}

/// One selector: a metric name, braces, or both. Leaves what follows (`or`, end of input) to
/// the caller.
fn parse_one_selector(cur: &mut Cursor) -> ParseResult<SeriesSelector> {
    let first = cur.expect(
        &[
            Token::Identifier,
            Token::StringLiteral,
            Token::OpOr,
            Token::LeftBrace,
        ],
        "series selector",
    )?;
    if first.token == Token::LeftBrace {
        return parse_braces(cur, None);
    }
    let name = decoded_name(cur, &first, "series selector")?;
    if cur.next_if(Token::LeftBrace)?.is_some() {
        return parse_braces(cur, Some(name));
    }
    Ok(SeriesSelector::with_filters(vec![LabelFilter::equals(
        METRIC_NAME_LABEL.into(),
        &name,
    )]))
}

/// The text of a name token (an identifier, `or`, or a quoted string), which must not be
/// empty. Only a quoted string can be: `{""="x"}`, `{""}`, `""{…}`. No series has an empty
/// label or metric name (`LABELS` and `METRIC` reject one), and RTS-form predicates and fan-out
/// payloads reject an empty label too, so the planner never sees one.
fn decoded_name(cur: &Cursor, lexeme: &Lexeme, context: &str) -> ParseResult<String> {
    let name = lexeme.decoded()?.into_owned();
    if name.is_empty() {
        return Err(cur.unexpected(lexeme, context, "a non-empty name"));
    }
    Ok(name)
}

/// The matchers after `{`, through the closing `}`. Groups separated by `or` are ORed; the
/// outer metric name, if any, is added to every group.
fn parse_braces(cur: &mut Cursor, outer: Option<String>) -> ParseResult<SeriesSelector> {
    let mut groups = OrFiltersList::new();
    if cur.next_if(Token::RightBrace)?.is_none() {
        loop {
            groups.push(parse_group(cur, outer.as_deref())?);
            let sep = cur.expect(&[Token::OpOr, Token::RightBrace], "label matchers")?;
            if sep.token == Token::RightBrace {
                break;
            }
        }
    } else {
        groups.push(FilterList::default());
    }
    if let Some(name) = &outer {
        for group in groups.iter_mut() {
            group.push(LabelFilter::equals(METRIC_NAME_LABEL.into(), name));
        }
    }
    Ok(if groups.len() == 1 {
        SeriesSelector::And(groups.pop().unwrap())
    } else {
        SeriesSelector::Or(groups)
    })
}

/// Comma-separated matchers, with one trailing comma allowed before `}`. Leaves the `or` or
/// `}` that ends the group to the caller.
fn parse_group(cur: &mut Cursor, outer: Option<&str>) -> ParseResult<FilterList> {
    let mut matchers = FilterList::default();
    let mut metric_name_seen = false;
    loop {
        let (matcher, names_metric) = parse_matcher(cur, outer)?;
        if names_metric {
            // At most one metric-name equality per group, judged from the matcher as written
            // (an equality or the bare-name shorthand), not after regex decomposition.
            if metric_name_seen {
                return Err(ParseError::SyntaxError(
                    "only one metric name allowed in a group of matchers".to_string(),
                ));
            }
            metric_name_seen = true;
            matchers.insert(0, matcher);
        } else {
            matchers.push(matcher);
        }
        if cur.next_if(Token::Comma)?.is_none() || cur.peek()?.token == Token::RightBrace {
            return Ok(matchers);
        }
    }
}

/// One matcher. Returns it and whether it is a metric-name equality as written.
///
/// `outer` is the metric name written before the braces, if any. As in Prometheus, a selector
/// that has one must not name the metric again inside the braces, with any operator or the
/// bare-name shorthand: `cpu{__name__="mem"}`, `cpu{__name__=~"m.*"}` and `cpu{mem}` are all
/// rejected. The check looks at the label name as written and runs before
/// `LabelFilter::create`, so regex decomposition (which turns `=~"mem"` into `="mem"`) can't
/// change the outcome.
fn parse_matcher(cur: &mut Cursor, outer: Option<&str>) -> ParseResult<(LabelFilter, bool)> {
    let lexeme = cur.expect(NAME_TOKENS, "label matcher")?;
    let name = decoded_name(cur, &lexeme, "label matcher")?;

    // A bare name is the metric-name shorthand: `{cpu, job="a"}`.
    if matches!(
        cur.peek()?.token,
        Token::Comma | Token::RightBrace | Token::OpOr
    ) {
        if let Some(outer) = outer {
            let inner = PredicateValue::String(name).to_string();
            return Err(ParseError::MetricNameSetTwice(outer.to_string(), inner));
        }
        let matcher = LabelFilter {
            label: METRIC_NAME_LABEL.into(),
            matcher: PredicateMatch::Equal(PredicateValue::String(name)),
        };
        return Ok((matcher, true));
    }

    let op = match_op(cur.expect(MATCH_OPERATORS, "label matcher")?.token);
    let names_metric_again = outer.filter(|_| name == METRIC_NAME_LABEL);
    let names_metric = name == METRIC_NAME_LABEL && op == MatchOp::Equal;

    if op.is_regex() {
        let pattern = cur.expect_string("regex pattern")?;
        if let Some(outer) = names_metric_again {
            let inner = PredicateValue::String(pattern).to_string();
            return Err(ParseError::MetricNameSetTwice(outer.to_string(), inner));
        }
        return Ok((LabelFilter::create(op, name, pattern)?, false));
    }
    let value = parse_matcher_value(cur)?;
    if let Some(outer) = names_metric_again {
        return Err(ParseError::MetricNameSetTwice(
            outer.to_string(),
            value.to_string(),
        ));
    }
    Ok((
        LabelFilter::create_with_value(op, name, value)?,
        names_metric,
    ))
}

fn match_op(token: Token) -> MatchOp {
    match token {
        Token::Equal => MatchOp::Equal,
        Token::OpNotEqual => MatchOp::NotEqual,
        Token::RegexEqual => MatchOp::RegexEqual,
        Token::RegexNotEqual => MatchOp::RegexNotEqual,
        Token::StartsWith => MatchOp::StartsWith,
        Token::NotStartsWith => MatchOp::NotStartsWith,
        other => unreachable!("not a match operator: {other}"),
    }
}

/// A value in the braces form: a quoted string, an unquoted identifier or number (taken as
/// written; `or` included), or a parenthesized list.
fn parse_matcher_value(cur: &mut Cursor) -> ParseResult<PredicateValue> {
    let lexeme = cur.expect(VALUE_TOKENS_OR_LIST, "label value")?;
    if lexeme.token == Token::LeftParen {
        return parse_value_list(cur);
    }
    Ok(PredicateValue::String(lexeme.decoded()?.into_owned()))
}

const VALUE_TOKENS: &[Token] = &[
    Token::StringLiteral,
    Token::Identifier,
    Token::Number,
    Token::OpOr,
];
const VALUE_TOKENS_OR_LIST: &[Token] = &[
    Token::StringLiteral,
    Token::Identifier,
    Token::Number,
    Token::OpOr,
    Token::LeftParen,
];

/// `(v1, v2, ...)` after the `(`: one or more values separated by single commas. An empty
/// list or an empty item is an error (as in the RTS form), so `(,a,,)` cannot quietly mean `a`
/// and `()` cannot mean "label absent".
fn parse_value_list(cur: &mut Cursor) -> ParseResult<PredicateValue> {
    let mut values = ValueList::new();
    loop {
        values.push(
            cur.expect(VALUE_TOKENS, "value list")?
                .decoded()?
                .into_owned(),
        );
        if cur
            .expect(&[Token::Comma, Token::RightParen], "value list")?
            .token
            == Token::RightParen
        {
            break;
        }
    }
    Ok(if values.len() == 1 {
        PredicateValue::String(values.swap_remove(0))
    } else {
        PredicateValue::List(values)
    })
}

/// Splits `label<op>value` at the first operator, returning the label, the operator and the
/// byte offset where the value starts; `None` when a `{` comes first (a Prometheus-style
/// selector) or there is no operator (a bare metric name).
///
/// Indexing by byte is safe: every byte matched here is ASCII, and a UTF-8 continuation or
/// lead byte never is.
fn split_rts_predicate(s: &str) -> Option<(&str, MatchOp, usize)> {
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
        return Some((&s[..i], op, i + len));
    }
    None
}

/// Builds the filter for an RTS-form predicate. `=` and `!=` are RTS; `=~`, `!~`, `^=` and `^~`
/// are extensions. The regex operators take a quoted string literal (so a pattern can hold any
/// character); the others take a raw value like `=` does.
fn parse_rts_predicate(
    s: &str,
    label: &str,
    op: MatchOp,
    value_at: usize,
) -> ParseResult<LabelFilter> {
    if label.is_empty() {
        return Err(ParseError::SyntaxError(format!(
            "missing label name before `{op}`"
        )));
    }
    if op.is_regex() {
        // Lexed in place, so an error reports its position in the whole argument.
        let mut cur = Cursor::at(s, value_at);
        let pattern = cur.expect_string("regex pattern")?;
        cur.expect(&[Token::Eof], "regex matcher")?;
        return LabelFilter::create(op, label, pattern);
    }
    LabelFilter::create_with_value(op, label, parse_rts_value(&s[value_at..])?)
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
