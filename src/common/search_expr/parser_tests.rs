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

// Ported from Prometheus `util/searchexpr/parser_test.go`. Its aliasing test has no
// counterpart: a parsed `Expr` owns its `String`s by construction.

use super::parser::{Parser, Token, TokenKind};
use super::{
    Expr, MAX_EXPRESSION_BYTES, MAX_EXPRESSION_NESTING_DEPTH, MAX_EXPRESSION_TERMS, parse,
};
use proptest::prelude::*;

fn t(value: &str) -> Expr {
    Expr::term(value)
}

#[track_caller]
fn assert_parse(input: &str, want: Expr) {
    match parse(input) {
        Ok(got) => assert_eq!(got, want, "input {input:?}"),
        Err(err) => panic!("parse({input:?}) failed: {err}"),
    }
}

#[track_caller]
fn assert_parse_error(input: &str, want: &str) {
    match parse(input) {
        Ok(expr) => panic!("parse({input:?}) = {expr:?}, want error {want:?}"),
        Err(err) => assert_eq!(err.to_string(), want, "input {input:?}"),
    }
}

fn repeat_joined(term: &str, count: usize, separator: &str) -> String {
    vec![term; count].join(separator)
}

#[test]
fn parse_precedence() {
    let cases = [
        ("term", "cortex", t("cortex")),
        (
            "AND binds tighter than OR",
            "cortex AND rule_evaluation_failures OR loki",
            Expr::or(
                Expr::and(t("cortex"), t("rule_evaluation_failures")),
                t("loki"),
            ),
        ),
        (
            "AND binds tighter than a preceding OR",
            "cortex OR loki AND envoy",
            Expr::or(t("cortex"), Expr::and(t("loki"), t("envoy"))),
        ),
        (
            "parentheses override precedence",
            "(cortex OR loki) AND rule_evaluation_failures",
            Expr::and(
                Expr::or(t("cortex"), t("loki")),
                t("rule_evaluation_failures"),
            ),
        ),
        (
            "NOT binds tighter than AND",
            "NOT cortex AND loki",
            Expr::and(Expr::not(t("cortex")), t("loki")),
        ),
        (
            "parenthesized NOT applies to the whole group",
            "NOT (cortex OR loki)",
            Expr::not(Expr::or(t("cortex"), t("loki"))),
        ),
    ];
    for (name, input, want) in cases {
        println!("{name}");
        assert_parse(input, want);
    }
}

#[test]
fn parse_operators_and_quoted_terms() {
    let cases = [
        (
            "operators are case insensitive",
            "cortex and loki Or envoy",
            Expr::or(Expr::and(t("cortex"), t("loki")), t("envoy")),
        ),
        (
            "chains AND left to right",
            "cortex AND loki AND envoy",
            Expr::and(Expr::and(t("cortex"), t("loki")), t("envoy")),
        ),
        (
            "chains OR left to right",
            "cortex OR loki OR envoy",
            Expr::or(Expr::or(t("cortex"), t("loki")), t("envoy")),
        ),
        (
            "quoted keywords are terms",
            r#""AND" AND "OR" OR "NOT""#,
            Expr::or(Expr::and(t("AND"), t("OR")), t("NOT")),
        ),
        (
            "quoted term supports escapes",
            r#""cortex \"ruler\"""#,
            t(r#"cortex "ruler""#),
        ),
    ];
    for (name, input, want) in cases {
        println!("{name}");
        assert_parse(input, want);
    }
}

/// Label names and values may hold any UTF-8, so terms must keep every byte that is not ASCII
/// whitespace, a parenthesis, or a quote. Upstream also passes invalid UTF-8 through; a `&str`
/// cannot hold it.
#[test]
fn parse_utf8_terms() {
    let cases = [
        ("accented", "métrique", t("métrique")),
        ("CJK", "指标 AND 名前", Expr::and(t("指标"), t("名前"))),
        ("emoji", "🔥 OR 🚀", Expr::or(t("🔥"), t("🚀"))),
        ("right-to-left", "NOT مقياس", Expr::not(t("مقياس"))),
        ("combining mark", "e\u{301}", t("e\u{301}")),
        (
            "dotted UTF-8 label name",
            "http.server.request.duration",
            t("http.server.request.duration"),
        ),
        (
            "punctuation in bare term",
            r"k8s-pod:name/a=b,c\d",
            t(r"k8s-pod:name/a=b,c\d"),
        ),
        (
            "non-ASCII whitespace does not separate terms",
            "a\u{a0}b\u{3000}c\u{2028}d",
            t("a\u{a0}b\u{3000}c\u{2028}d"),
        ),
        (
            "fullwidth keywords are terms",
            "ＮＯＴ AND ｏｒ",
            Expr::and(t("ＮＯＴ"), t("ｏｒ")),
        ),
        (
            "quoted delimiters and keywords",
            r#""foo (bar) AND \"baz\" OR qux""#,
            t(r#"foo (bar) AND "baz" OR qux"#),
        ),
        (
            "quoted escapes around multi-byte runes",
            r#""\"é\" \\🔥""#,
            t(r#""é" \🔥"#),
        ),
        (
            "quoted control bytes",
            "\"a\x00b\x7fc\td\"",
            t("a\x00b\x7fc\td"),
        ),
    ];
    for (name, input, want) in cases {
        println!("{name}");
        assert_parse(input, want);
    }
}

#[test]
fn parse_utf8_positions_and_limits_are_bytes() {
    assert_parse_error(
        "é AND",
        "search expression: expected term or (, got end of expression at position 6",
    );

    let at_limit = "é".repeat(MAX_EXPRESSION_BYTES / "é".len());
    parse(&at_limit).unwrap();

    assert_parse_error(
        &(at_limit + "a"),
        "search expression: length 4097 exceeds maximum of 4096 bytes",
    );
}

#[test]
fn parse_rejects_invalid_expressions() {
    let cases = [
        (
            "",
            "search expression: expected term or (, got end of expression at position 0",
        ),
        (
            "AND cortex",
            "search expression: expected term or (, got AND at position 0",
        ),
        (
            "OR cortex",
            "search expression: expected term or (, got OR at position 0",
        ),
        (
            "NOT",
            "search expression: expected term or (, got end of expression at position 3",
        ),
        (
            "cortex AND",
            "search expression: expected term or (, got end of expression at position 10",
        ),
        (
            "cortex OR",
            "search expression: expected term or (, got end of expression at position 9",
        ),
        (
            "cortex AND NOT",
            "search expression: expected term or (, got end of expression at position 14",
        ),
        (
            "cortex AND AND loki",
            "search expression: expected term or (, got AND at position 11",
        ),
        (
            "cortex OR OR loki",
            "search expression: expected term or (, got OR at position 10",
        ),
        (
            "NOT )",
            "search expression: expected term or (, got ) at position 4",
        ),
        (
            "cortex rule_evaluation_failures",
            r#"search expression: unexpected term "rule_evaluation_failures" at position 7"#,
        ),
        (
            "cortex (loki OR envoy)",
            "search expression: unexpected ( at position 7",
        ),
        (
            "()",
            "search expression: expected term or (, got ) at position 1",
        ),
        (
            "(  )",
            "search expression: expected term or (, got ) at position 3",
        ),
        (
            "(cortex OR loki",
            "search expression: expected ), got end of expression at position 15",
        ),
        (
            "(",
            "search expression: expected term or (, got end of expression at position 1",
        ),
        (
            "cortex AND )",
            "search expression: expected term or (, got ) at position 11",
        ),
        ("cortex)", "search expression: unexpected ) at position 6"),
        (
            "cortex AND (loki OR)",
            "search expression: expected term or (, got ) at position 19",
        ),
        (
            r#""cortex""loki""#,
            r#"search expression: unexpected term "loki" at position 8"#,
        ),
        (
            r#""""#,
            "search expression: empty quoted term at position 0",
        ),
        (
            r#""unterminated"#,
            "search expression: unterminated quoted term at position 0",
        ),
        (
            "\"trailing escape\\",
            "search expression: unterminated quoted term at position 0",
        ),
        (
            r#""esc\"aped"#,
            "search expression: unterminated quoted term at position 0",
        ),
        (
            r#""a\nb""#,
            "search expression: invalid escape at position 2",
        ),
        (
            "\"\\\t\"",
            "search expression: invalid escape at position 1",
        ),
        (
            r#""\x41""#,
            "search expression: invalid escape at position 1",
        ),
        (r#""\é""#, "search expression: invalid escape at position 1"),
        (
            r#""a\\\"\qb""#,
            "search expression: invalid escape at position 6",
        ),
        (
            r#""ok" AND "a\qb"#,
            "search expression: invalid escape at position 11",
        ),
    ];
    for (input, want) in cases {
        assert_parse_error(input, want);
    }
}

#[test]
fn parse_reports_offending_token_position() {
    for (input, want) in [
        (
            "AND foo",
            "search expression: expected term or (, got AND at position 0",
        ),
        (
            "OR foo",
            "search expression: expected term or (, got OR at position 0",
        ),
        (
            ") foo",
            "search expression: expected term or (, got ) at position 0",
        ),
    ] {
        assert_parse_error(input, want);
    }
}

#[test]
fn parse_rejects_quote_inside_bare_term() {
    for (input, want) in [
        (
            r#"NOT"foo""#,
            "search expression: unexpected quote at position 3",
        ),
        (r#"a""#, "search expression: unexpected quote at position 1"),
        (
            r#"foo"bar baz""#,
            "search expression: unexpected quote at position 3",
        ),
    ] {
        assert_parse_error(input, want);
    }
}

#[test]
fn parse_separators() {
    let want = Expr::and(t("a"), Expr::or(t("b"), Expr::not(t("c"))));
    for separator in [" ", "\t", "\n", "\r", "\r\n"] {
        let input = ["", "a", "AND", "(b", "OR", "NOT", "c)", ""].join(separator);
        assert_parse(&input, want.clone());
    }
}

/// The parser lexes on demand, so a parse error that comes before a lex error in the input
/// wins.
#[test]
fn parse_reports_first_error() {
    let cases = [
        (
            r#"AND "unterminated"#.to_string(),
            "search expression: expected term or (, got AND at position 0",
        ),
        (
            r#"a b "unterminated"#.to_string(),
            r#"search expression: unexpected term "b" at position 2"#,
        ),
        (
            r#"a ) b""#.to_string(),
            "search expression: unexpected ) at position 2",
        ),
        (
            "t ".repeat(MAX_EXPRESSION_TERMS + 1),
            r#"search expression: unexpected term "t" at position 2"#,
        ),
        (
            "(".repeat(MAX_EXPRESSION_NESTING_DEPTH + 1) + r#""x"#,
            "search expression: nesting depth exceeds maximum of 16 at position 16",
        ),
        (
            r#""x" AND ""#.to_string(),
            "search expression: unterminated quoted term at position 8",
        ),
        (
            r#"a "x"#.to_string(),
            "search expression: unterminated quoted term at position 2",
        ),
        (
            r#"NOT "x"#.to_string(),
            "search expression: unterminated quoted term at position 4",
        ),
        (
            r#"("x"#.to_string(),
            "search expression: unterminated quoted term at position 1",
        ),
        (
            r#"(a) "x"#.to_string(),
            "search expression: unterminated quoted term at position 4",
        ),
    ];
    for (input, want) in cases {
        assert_parse_error(&input, want);
    }
}

#[test]
fn parse_resource_limits() {
    assert_parse_error(
        &("a".to_string() + &" ".repeat(MAX_EXPRESSION_BYTES)),
        "search expression: length 4097 exceeds maximum of 4096 bytes",
    );
    assert_parse_error(
        &repeat_joined("term", MAX_EXPRESSION_TERMS + 1, " AND "),
        "search expression: term count exceeds maximum of 32 at position 288",
    );
    assert_parse_error(
        &repeat_joined(r#""term""#, MAX_EXPRESSION_TERMS + 1, " AND "),
        "search expression: term count exceeds maximum of 32 at position 352",
    );

    let depth = MAX_EXPRESSION_NESTING_DEPTH + 1;
    let half = MAX_EXPRESSION_NESTING_DEPTH / 2 + 1;
    for input in [
        "(".repeat(depth) + "foo" + &")".repeat(depth),
        "NOT ".repeat(depth) + "foo",
        "NOT (".repeat(half) + "foo" + &")".repeat(half),
    ] {
        let err = parse(&input).unwrap_err().to_string();
        assert!(
            err.contains("nesting depth exceeds maximum of 16"),
            "{input:?}: {err}"
        );
    }
}

#[test]
fn parse_accepts_resource_limit_boundaries() {
    let terms = repeat_joined("term", MAX_EXPRESSION_TERMS, " AND ");
    let depth = MAX_EXPRESSION_NESTING_DEPTH;
    for input in [
        "foo".to_string() + &" ".repeat(MAX_EXPRESSION_BYTES - "foo".len()),
        terms.clone(),
        "(".repeat(depth) + "foo" + &")".repeat(depth),
        "NOT ".repeat(depth) + "foo",
        "NOT ".repeat(depth) + &terms,
    ] {
        if let Err(err) = parse(&input) {
            panic!("parse({input:?}) failed: {err}");
        }
    }
}

#[test]
fn token_display() {
    for (kind, want) in [
        (TokenKind::Eof, "end of expression"),
        (TokenKind::Term("cortex".into()), r#"term "cortex""#),
        (TokenKind::Not, "NOT"),
        (TokenKind::And, "AND"),
        (TokenKind::Or, "OR"),
        (TokenKind::LeftParen, "("),
        (TokenKind::RightParen, ")"),
    ] {
        assert_eq!(kind.to_string(), want);
    }
}

/// Every `Term` value in `expr`, left to right.
fn collect_term_values(expr: &Expr) -> Vec<&str> {
    match expr {
        Expr::Term(value) => vec![value.as_str()],
        Expr::And(left, right) | Expr::Or(left, right) => {
            let mut terms = collect_term_values(left);
            terms.extend(collect_term_values(right));
            terms
        }
        Expr::Not(expr) => collect_term_values(expr),
    }
}

fn lex_tokens(input: &str) -> Vec<Token> {
    let mut p = Parser::new(input);
    let mut tokens = Vec::new();
    loop {
        p.advance().unwrap();
        tokens.push(p.cur.clone());
        if p.cur.kind == TokenKind::Eof {
            return tokens;
        }
    }
}

/// Measures nesting from tokens, because groups leave no node in the AST. Each `NOT` and each
/// group adds one level. A `NOT` run ends with the primary that follows it: a term, or a group
/// at its closing parenthesis.
fn max_nesting_depth(tokens: &[Token]) -> usize {
    let (mut depth, mut deepest, mut pending_nots) = (0usize, 0usize, 0usize);
    let mut nots_before_group = Vec::new();
    for token in tokens {
        match token.kind {
            TokenKind::Not => {
                pending_nots += 1;
                depth += 1;
            }
            TokenKind::Term(_) => {
                depth -= pending_nots;
                pending_nots = 0;
            }
            TokenKind::LeftParen => {
                nots_before_group.push(pending_nots);
                pending_nots = 0;
                depth += 1;
            }
            TokenKind::RightParen => {
                depth -= 1 + nots_before_group.pop().unwrap();
            }
            TokenKind::And | TokenKind::Or | TokenKind::Eof => {}
        }
        deepest = deepest.max(depth);
    }
    deepest
}

#[test]
fn max_nesting_depth_counts_nots_and_groups() {
    let half = MAX_EXPRESSION_NESTING_DEPTH / 2;
    for (input, want) in [
        ("foo".to_string(), 0),
        ("NOT foo".to_string(), 1),
        ("NOT (foo)".to_string(), 2),
        ("NOT NOT foo AND NOT bar".to_string(), 2),
        ("(((foo))) AND NOT NOT bar".to_string(), 3),
        ("(NOT foo) AND (bar)".to_string(), 2),
        ("NOT (NOT foo OR (bar)) AND NOT baz".to_string(), 3),
        (
            "NOT (".repeat(half) + "foo" + &")".repeat(half),
            MAX_EXPRESSION_NESTING_DEPTH,
        ),
    ] {
        parse(&input).unwrap();
        assert_eq!(max_nesting_depth(&lex_tokens(&input)), want, "{input:?}");
    }
}

fn assert_well_formed(expr: &Expr) {
    match expr {
        Expr::Term(value) => assert!(!value.is_empty()),
        Expr::And(left, right) | Expr::Or(left, right) => {
            assert_well_formed(left);
            assert_well_formed(right);
        }
        Expr::Not(expr) => assert_well_formed(expr),
    }
}

/// Inputs built from the grammar's pieces, which reach far more valid and nearly valid
/// expressions than arbitrary strings do.
fn expression_soup() -> impl Strategy<Value = String> {
    let piece = prop_oneof![
        Just("cortex".to_string()),
        Just("rule_evaluation_failures".to_string()),
        Just("AND".to_string()),
        Just("and".to_string()),
        Just("OR".to_string()),
        Just("NOT".to_string()),
        Just("(".to_string()),
        Just(")".to_string()),
        Just("\"".to_string()),
        Just(r#""AND""#.to_string()),
        Just(r#""指标 \"é\"""#.to_string()),
        Just("\\".to_string()),
        Just("🔥".to_string()),
        Just("ＮＯＴ".to_string()),
        "[a-z]{1,3}",
    ];
    let separator = prop_oneof![Just(""), Just(" "), Just("\t"), Just("\r\n")];
    prop::collection::vec((piece, separator), 0..48).prop_map(|parts| {
        parts
            .into_iter()
            .map(|(piece, separator)| piece + separator)
            .collect()
    })
}

proptest! {
    #[test]
    fn parse_results_respect_the_limits(
        input in prop_oneof![
            expression_soup(),
            any::<String>(),
            Just("NOT ".repeat(MAX_EXPRESSION_NESTING_DEPTH + 1) + "cortex"),
            Just("((((((cortex))))))".to_string()),
        ]
    ) {
        if let Ok(expr) = parse(&input) {
            assert_well_formed(&expr);
            prop_assert!(input.len() <= MAX_EXPRESSION_BYTES);
            prop_assert!(collect_term_values(&expr).len() <= MAX_EXPRESSION_TERMS);
            prop_assert!(max_nesting_depth(&lex_tokens(&input)) <= MAX_EXPRESSION_NESTING_DEPTH);
        }
    }
}
