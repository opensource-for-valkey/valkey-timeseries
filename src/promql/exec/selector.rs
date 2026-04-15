use regex_syntax::hir::{Hir, HirKind};
use regex_syntax::Parser;

fn parse_literal(hir: &Hir, pattern: &str) -> Result<String, String> {
    match hir.kind() {
        HirKind::Empty => Err(format!("empty alternative in pattern: {pattern}")),
        HirKind::Literal(l) => String::from_utf8(l.0.to_vec())
            .map_err(|_| format!("Non-UTF-8 literal in regex: {:?}", l)),
        HirKind::Concat(hirs) => {
            let mut value = String::new();
            for hir in hirs {
                value.push_str(&parse_literal(hir, pattern)?);
            }
            Ok(value)
        }
        _ => Err(format!(
            "Regex pattern '{pattern}' is not supported. Only alternations of literal strings allowed (e.g., 'value1|value2|value3')."
        )),
    }
}

/// Parse a limited regex pattern of the form "value1|value2|...|valueN" into individual values.
/// Returns an error if the regex is not of the expected simple pipe-separated form.
/// Uses regex-syntax to properly parse and validate the regex structure.
fn parse_limited_regex(pattern: &str) -> Result<Vec<String>, String> {
    let hir = Parser::new()
        .parse(pattern)
        .map_err(|e| format!("Invalid regex pattern '{pattern}': {}", e))?;

    match hir.kind() {
        HirKind::Alternation(alternatives) => {
            let mut values = Vec::new();
            // Each alternative must be a literal string or a simple concatenation
            for alt in alternatives {
                values.push(parse_literal(alt, pattern)?);
            }
            Ok(values)
        }
        HirKind::Literal(_) => Ok(vec![parse_literal(&hir, pattern)?]),
        HirKind::Concat(_) => Ok(vec![parse_literal(&hir, pattern)?]),
        _ => Err(format!(
            "Regex pattern '{pattern}' not supported. Only alternations of literal strings allowed (e.g., 'value1|value2|value3')."
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use promql_parser::label::{MatchOp, Matcher, Matchers};
    use rstest::rstest;

    fn empty_matchers() -> Matchers {
        Matchers {
            matchers: vec![],
            or_matchers: vec![],
        }
    }

    #[rstest]
    #[case("host-40", Ok(vec!["host-40".to_string()]))]
    #[case("host-40|host-39|host-38", Ok(vec!["host-40".to_string(), "host-39".to_string(), "host-38".to_string()]
    ))]
    #[case("prod|staging|dev", Ok(vec!["prod".to_string(), "staging".to_string(), "dev".to_string()]
    ))]
    #[case("host-[0-9]+", Err("regex constructs not allowed"))]
    #[case("host.*", Err("regex constructs not allowed"))]
    #[case("(host-40|host-39)", Err("regex constructs not allowed"))]
    #[case("host-40+", Err("regex constructs not allowed"))]
    #[case("^host-40$", Err("regex constructs not allowed"))]
    #[case("host-40?", Err("regex constructs not allowed"))]
    #[case("host-4[0-9]", Err("regex constructs not allowed"))]
    #[case("", Err("empty pattern"))]
    #[case("host-40||host-39", Err("empty alternative"))]
    #[case("|host-40", Err("empty alternative"))]
    #[case("host-40|", Err("empty alternative"))]
    fn should_parse_limited_regex_patterns(
        #[case] pattern: &str,
        #[case] expected: Result<Vec<String>, &str>,
    ) {
        let result = parse_limited_regex(pattern);
        match expected {
            Ok(expected_values) => {
                assert_eq!(result, Ok(expected_values));
            }
            Err(_) => {
                assert!(
                    result.is_err(),
                    "Pattern '{}' should be rejected but was accepted: {:?}",
                    pattern,
                    result
                );
            }
        }
    }

    #[rstest]
    #[case(r"host\d+", "Digit escape")]
    #[case(r"host\w+", "Word character escape")]
    #[case(r"host\s*", "Whitespace escape")]
    #[case(r"host.*\.com", "Dot-star pattern")]
    #[case(r"(prod|staging)", "Grouped alternation")]
    #[case(r"host-\d{2}", "Digit with quantifier")]
    #[case(r"^host-40$", "Start/end anchors")]
    #[case(r"host-40|host-.*", "Mixed literal and pattern")]
    #[case(r"(?i)host-40", "Case-insensitive flag")]
    #[case(r"host-40{1,3}", "Counted repetition")]
    #[case(r"GE[Tt]", "Character class")]
    #[case(r"GET.*", "Dot-star")]
    fn should_fail_complex_regex_patterns(#[case] pattern: &str, #[case] description: &str) {
        let result = parse_limited_regex(pattern);
        assert!(
            result.is_err(),
            "Pattern '{}' ({}) should be rejected but was accepted: {:?}",
            pattern,
            description,
            result
        );
    }

    fn create_regex_matcher(name: &str, pattern: &str) -> Result<Matcher, String> {
        // Create a regex from the pattern to validate it
        use regex::Regex;
        let regex = Regex::new(pattern).map_err(|e| format!("Invalid regex: {}", e))?;

        Ok(Matcher {
            op: MatchOp::Re(regex),
            name: name.to_string(),
            value: pattern.to_string(),
        })
    }

    fn create_not_regex_matcher(name: &str, pattern: &str) -> Result<Matcher, String> {
        // Create a regex from the pattern to validate it
        use regex::Regex;
        let regex = Regex::new(pattern).map_err(|e| format!("Invalid regex: {}", e))?;

        Ok(Matcher {
            op: MatchOp::NotRe(regex),
            name: name.to_string(),
            value: pattern.to_string(),
        })
    }
}
