use super::lex::{Token, expect_one_of_tokens, expect_token, get_next_token};
use super::parse_error::unexpected;
use crate::common::constants::METRIC_NAME_LABEL;
use crate::labels::Label;
use crate::parser::utils::extract_string_value;
use crate::parser::{ParseError, ParseResult};
use logos::{Lexer, Logos};

/// specialized parser for a `Prometheus` compatible metric name (as opposed to a metric selector).
///
///    <label_set>
///
///    <metric_identifier> [<label_set>]
///
pub fn parse_metric_name(s: &str) -> ParseResult<Vec<Label>> {
    let mut lex = Token::lexer(s);

    let mut labels: Vec<Label> = Vec::new();

    let measurement = expect_token(&mut lex, Token::Identifier)?;
    labels.push(Label::new(METRIC_NAME_LABEL, measurement));
    let res = get_next_token(&mut lex)?;
    if res.0 == Token::Eof {
        return Ok(labels);
    }
    if res.0 == Token::LeftBrace {
        parse_label_filters(&mut lex, &mut labels)?;
        // The measurement is the `__name__`; a second one would override it, with any value —
        // including ones (empty, not an identifier) that no metric name renders back to.
        if labels[1..]
            .iter()
            .any(|label| label.name == METRIC_NAME_LABEL)
        {
            return Err(ParseError::General(format!(
                "{METRIC_NAME_LABEL} label conflicts with the measurement `{}`",
                labels[0].value
            )));
        }
    }
    // todo: expect eof
    Ok(labels)
}

/// parse a set of label matchers.
///
/// '{' [ <label_name> <match_op> <match_string>, ... ] '}'
///
pub(crate) fn parse_label_filters(
    lex: &mut Lexer<Token>,
    labels: &mut Vec<Label>,
) -> ParseResult<()> {
    use Token::*;

    let mut was_value = false;

    loop {
        let (tok, name) = if was_value {
            expect_one_of_tokens(lex, &[Comma, RightBrace])
        } else {
            expect_one_of_tokens(lex, &[Identifier, Comma, RightBrace])
        }?;
        match tok {
            Identifier => {
                let name = name.to_string();
                let _ = expect_token(lex, Equal)?;
                let value = expect_token(lex, StringLiteral)?;
                let contents = extract_string_value(value)?;
                labels.push(Label::new(name, contents.to_string()));
                was_value = true;
            }
            Comma => {
                was_value = false;
            }
            RightBrace => break,
            _ => return Err(unexpected("metric name label", name, ", or }", None)),
        }
    }
    // make sure we're at eof

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
}
