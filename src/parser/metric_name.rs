use super::lex::{Cursor, Token};
use crate::common::constants::METRIC_NAME_LABEL;
use crate::labels::{Label, MAX_LABELS_PER_SERIES};
use crate::parser::{ParseError, ParseResult};

/// Parses a metric definition, the `TS.CREATE ... METRIC` operand: a metric name and a label
/// set, as Prometheus writes them.
///
/// ```text
/// metric     := name [ "{" [ items ] "}" ] | "{" items "}"
/// items      := item ( "," item )* [ "," ]
/// item       := label_name "=" string | string
/// name       := identifier | "or"
/// label_name := identifier | "or" | string
/// ```
///
/// The whole input must be consumed. A name that is not an identifier is written as a quoted
/// string: a label name before its `=`, and the metric name as a bare string inside the braces,
/// `{"my metric", a="1"}` (Prometheus 3's form for UTF-8 names). The metric name must be given
/// exactly once. One trailing comma is allowed (as in PromQL and in selectors); an empty item
/// (`{,a="1"}`, `{a="1",,}`) is not. Each label is held to what `LABELS` requires: a non-empty
/// name without `=`, a non-empty value. A repeated name, and a `__name__` label (the metric
/// name already is the `__name__`), are errors, as is more than `MAX_LABELS_PER_SERIES` labels
/// counting the `__name__` ([`ParseError::TooManyLabels`]).
/// [`MetricName`](crate::labels::MetricName)'s `Display` writes this syntax back.
pub fn parse_metric_name(s: &str) -> ParseResult<Vec<Label>> {
    let mut cur = Cursor::new(s);

    // `or` lexes as a keyword but is a name here, as it is in selectors.
    let first = cur.expect(
        &[Token::Identifier, Token::OpOr, Token::LeftBrace],
        "metric name",
    )?;
    let mut measurement = None;
    // The labels other than `__name__`, which goes first once it is known.
    let mut labels = Vec::new();
    if first.token == Token::LeftBrace {
        parse_label_set(&mut cur, &mut measurement, &mut labels)?;
    } else {
        measurement = Some(first.text.to_string());
        if cur.next_if(Token::LeftBrace)?.is_some() {
            parse_label_set(&mut cur, &mut measurement, &mut labels)?;
        }
    }
    cur.expect(&[Token::Eof], "metric name")?;

    let Some(measurement) = measurement else {
        return Err(ParseError::General("missing metric name".to_string()));
    };
    labels.insert(0, Label::new(METRIC_NAME_LABEL, &measurement));
    Ok(labels)
}

/// Parses the items of a label set after the opening brace, through the closing `}`, appending
/// to `labels`. A bare quoted string is the metric name and is stored in `measurement`, which
/// must not already be set.
fn parse_label_set(
    cur: &mut Cursor,
    measurement: &mut Option<String>,
    labels: &mut Vec<Label>,
) -> ParseResult<()> {
    use Token::*;

    loop {
        // `}` here closes an empty set or follows a trailing comma. `or` is a name here, as
        // everywhere a name is expected.
        let name = cur.expect(&[Identifier, StringLiteral, OpOr, RightBrace], "label set")?;
        if name.token == RightBrace {
            return Ok(());
        }
        let is_string = name.token == StringLiteral;
        let name = name.decoded()?.into_owned();

        // A quoted string not followed by `=` is the metric name; a bare identifier must be.
        let next = if is_string {
            cur.expect(&[Equal, Comma, RightBrace], "label set")?
        } else {
            cur.expect(&[Equal], "label set")?
        };
        if next.token != Equal {
            if name.is_empty() {
                return Err(ParseError::General("empty metric name".to_string()));
            }
            if let Some(first) = measurement.replace(name) {
                return Err(ParseError::General(format!(
                    "metric name given twice: `{first}`"
                )));
            }
            if next.token == RightBrace {
                return Ok(());
            }
            continue;
        }

        // Checked before the duplicate scan in `check_label`, which is linear in the labels
        // so far: past the limit the operand is rejected anyway. The `__name__` counts.
        if labels.len() + 1 == MAX_LABELS_PER_SERIES {
            return Err(ParseError::TooManyLabels);
        }
        check_label(labels, &name)?;

        let value = cur.expect_string("label value")?;
        if value.is_empty() {
            return Err(ParseError::General(format!(
                "label `{name}` has an empty value"
            )));
        }
        labels.push(Label::new(name, value));

        if cur.expect(&[Comma, RightBrace], "label set")?.token == RightBrace {
            return Ok(());
        }
    }
}

fn check_label(labels: &[Label], name: &str) -> ParseResult<()> {
    if name.is_empty() || name.contains('=') {
        return Err(ParseError::General(format!(
            "invalid label name `{name}`: must be non-empty and must not contain '='"
        )));
    }
    // The metric name is the `__name__`; a second one would override it, with any value —
    // including ones (empty, not an identifier) that no metric name renders back to.
    if name == METRIC_NAME_LABEL {
        return Err(ParseError::General(format!(
            "{METRIC_NAME_LABEL} label conflicts with the metric name"
        )));
    }
    if labels.iter().any(|label| label.name == name) {
        return Err(ParseError::General(format!("duplicate label `{name}`")));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_metric_name() {
        let cases = vec![
            ("foo{}", vec![Label::new("__name__", "foo")]),
            ("foo", vec![Label::new("__name__", "foo")]),
            (
                "foo{bar=\"baz\"}",
                vec![Label::new("__name__", "foo"), Label::new("bar", "baz")],
            ),
            (
                "foo{bar=\"baz\", qux=\"quux\"}",
                vec![
                    Label::new("__name__", "foo"),
                    Label::new("bar", "baz"),
                    Label::new("qux", "quux"),
                ],
            ),
            (
                "metric_name{label1=\"value1\", label2=\"value2\"}",
                vec![
                    Label::new("__name__", "metric_name"),
                    Label::new("label1", "value1"),
                    Label::new("label2", "value2"),
                ],
            ),
            (
                "http_requests_total{method=\"post\", code=\"200\"}",
                vec![
                    Label::new("__name__", "http_requests_total"),
                    Label::new("code", "200"),
                    Label::new("method", "post"),
                ],
            ),
            (
                "up{instance=\"localhost:9090\", job=\"prometheus\"}",
                vec![
                    Label::new("__name__", "up"),
                    Label::new("instance", "localhost:9090"),
                    Label::new("job", "prometheus"),
                ],
            ),
        ];

        for (input, expected) in cases {
            let mut got = parse_metric_name(input).unwrap();
            got.sort();
            assert_eq!(got, expected);
        }
    }

    #[test]
    fn test_parse_metric_name_rejects_name_label() {
        // The measurement is `__name__`; a second one in the label set is an error, empty or not.
        for input in [
            r#"cpu{__name__="other"}"#,
            r#"cpu{__name__="cpu"}"#,
            r#"cpu{__name__=""}"#,
            r#"cpu{a="1",__name__="9cpu",b=""}"#,
        ] {
            assert!(parse_metric_name(input).is_err(), "{input}");
        }
    }

    #[test]
    fn test_parse_metric_name_requires_complete_input() {
        for input in [
            "cpu garbage",
            "cpu=garbage",
            "cpu{} garbage",
            r#"cpu{job="a"}x"#,
            r#"cpu{job="a"}}"#,
            "cpu{",
            r#"cpu{job="a""#,
            "123",
        ] {
            assert!(
                parse_metric_name(input).is_err(),
                "{input}: {:?}",
                parse_metric_name(input)
            );
        }
    }

    #[test]
    fn test_parse_metric_name_comma_policy() {
        // One trailing comma, as in PromQL; no empty items.
        let got = parse_metric_name(r#"cpu{a="b",}"#).unwrap();
        assert_eq!(
            got,
            vec![Label::new("__name__", "cpu"), Label::new("a", "b")]
        );
        for input in [
            r#"cpu{,a="b"}"#,
            r#"cpu{a="b",,}"#,
            r#"cpu{,,job="api",,}"#,
            "cpu{,}",
        ] {
            assert!(parse_metric_name(input).is_err(), "{input}");
        }
    }

    #[test]
    fn test_parse_metric_name_validates_labels_like_labels_option() {
        for input in [
            r#"cpu{a=""}"#,
            r#"cpu{a="1",a="2"}"#,
            r#"cpu{""="1"}"#,
            r#"cpu{"a=b"="1"}"#,
        ] {
            assert!(
                parse_metric_name(input).is_err(),
                "{input}: {:?}",
                parse_metric_name(input)
            );
        }
    }

    #[test]
    fn test_parse_metric_name_or_is_a_name() {
        for (input, expected) in [
            ("or", vec![Label::new("__name__", "or")]),
            ("OR", vec![Label::new("__name__", "OR")]),
            (
                r#"or{host="a"}"#,
                vec![Label::new("__name__", "or"), Label::new("host", "a")],
            ),
            (
                r#"or{or="b"}"#,
                vec![Label::new("__name__", "or"), Label::new("or", "b")],
            ),
        ] {
            assert_eq!(parse_metric_name(input), Ok(expected), "{input}");
        }
    }

    #[test]
    fn test_parse_metric_name_label_limit() {
        // `n` labels besides the `__name__`.
        let metric = |n: usize| {
            let labels: Vec<String> = (0..n).map(|i| format!(r#"l{i}="v""#)).collect();
            format!("cpu{{{}}}", labels.join(","))
        };
        let at_limit = parse_metric_name(&metric(MAX_LABELS_PER_SERIES - 1)).unwrap();
        assert_eq!(at_limit.len(), MAX_LABELS_PER_SERIES);
        assert_eq!(
            parse_metric_name(&metric(MAX_LABELS_PER_SERIES)),
            Err(ParseError::TooManyLabels)
        );
        // Rejected at the first label past the limit: neither a later duplicate nor a later
        // syntax error is reached.
        let mut past = metric(MAX_LABELS_PER_SERIES);
        past.insert_str(past.len() - 1, r#",l0="v",!"#);
        assert_eq!(parse_metric_name(&past), Err(ParseError::TooManyLabels));
    }

    #[test]
    fn test_parse_metric_name_accepts_quoted_label_names() {
        let got = parse_metric_name(r#"cpu{"host.name"="a", 'b-c'="d"}"#).unwrap();
        assert_eq!(
            got,
            vec![
                Label::new("__name__", "cpu"),
                Label::new("host.name", "a"),
                Label::new("b-c", "d")
            ]
        );
    }

    #[test]
    fn test_parse_metric_name_quoted_names() {
        let ok = [
            (r#"m{"a b"="v"}"#, vec![("__name__", "m"), ("a b", "v")]),
            (
                r#"m{"9"="v",ok="w"}"#,
                vec![("__name__", "m"), ("9", "v"), ("ok", "w")],
            ),
            (r#"m{or="1"}"#, vec![("__name__", "m"), ("or", "1")]),
            (r#"{"my metric"}"#, vec![("__name__", "my metric")]),
            (
                r#"{"my metric", "a b"="v"}"#,
                vec![("__name__", "my metric"), ("a b", "v")],
            ),
            (r#"{a="v", "9m"}"#, vec![("__name__", "9m"), ("a", "v")]),
            (r#"or{a="v"}"#, vec![("__name__", "or"), ("a", "v")]),
            ("m{}", vec![("__name__", "m")]),
            (r#"m{a="1",}"#, vec![("__name__", "m"), ("a", "1")]),
        ];
        for (input, want) in ok {
            let got = parse_metric_name(input).unwrap_or_else(|e| panic!("{input}: {e}"));
            let got: Vec<(&str, &str)> = got
                .iter()
                .map(|l| (l.name.as_str(), l.value.as_str()))
                .collect();
            assert_eq!(got, want, "{input}");
        }
        for bad in [
            r#"{a="v"}"#,           // no metric name
            "{}",                   // no metric name
            r#"m{"other"}"#,        // named twice
            r#"{"x", "y"}"#,        // named twice
            r#"{""}"#,              // empty metric name
            r#"m{""="v"}"#,         // empty label name
            r#"m{a}"#,              // a bare name must be quoted to be the metric name
            r#"m{a="1"} junk"#,     // trailing text
            r#"m{"__name__"="x"}"#, // `__name__` again, quoted
        ] {
            assert!(parse_metric_name(bad).is_err(), "{bad} should be rejected");
        }
    }

    #[test]
    fn test_parse_metric_name_label_limit_with_quoted_name() {
        // The limit counts the `__name__` wherever it is written.
        let labels: Vec<String> = (0..MAX_LABELS_PER_SERIES - 1)
            .map(|i| format!(r#"l{i}="v""#))
            .collect();
        let at_limit = format!(r#"{{"m", {}}}"#, labels.join(","));
        assert_eq!(
            parse_metric_name(&at_limit).unwrap().len(),
            MAX_LABELS_PER_SERIES
        );
        let past = format!(r#"{{"m", {}, x="v"}}"#, labels.join(","));
        assert_eq!(parse_metric_name(&past), Err(ParseError::TooManyLabels));
    }
}
