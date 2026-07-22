#[cfg(test)]
mod tests {
    use crate::commands::command_parser::validate_selector_list;
    use crate::error_consts;
    use crate::labels::filters::{
        FilterList, LabelFilter, MatchOp, PredicateMatch, PredicateValue, SeriesSelector,
    };
    use crate::labels::parse_series_selector;

    /// Asserts that `selector` parses, but is rejected as the sole filter of a command.
    fn assert_unbounded_alone(selector: &str) -> SeriesSelector {
        let parsed = parse_series_selector(selector)
            .unwrap_or_else(|e| panic!("expected {selector} to parse, got {e}"));
        assert!(!parsed.is_bounded(), "{selector} should not be bounded");
        let err = validate_selector_list(std::slice::from_ref(&parsed))
            .expect_err("selector has no positive matcher");
        assert_eq!(
            err.to_string(),
            error_consts::MISSING_FILTER,
            "unexpected error for {selector}"
        );
        parsed
    }

    fn assert_matcher(matcher: &LabelFilter, label: &str, op: MatchOp, value: &str) {
        let expected = LabelFilter::create(op, label, value).unwrap();
        assert_eq!(
            matcher, &expected,
            "expected matcher: {}, found {}",
            &expected, matcher
        );
    }

    fn assert_contains_matcher(matchers: &[LabelFilter], label: &str, op: MatchOp, value: &str) {
        let expected = LabelFilter::create(op, label, value).unwrap();
        assert!(
            matchers.contains(&expected),
            "expected matcher: {}, not found in {:?}",
            &expected,
            matchers
        );
    }

    fn assert_list_matcher(matcher: &LabelFilter, label: &str, op: MatchOp, values: &[&str]) {
        let values = values.iter().map(|s| s.to_string()).collect();
        if op.is_regex() {
            panic!("regex matchers are not supported in list matchers");
        }
        let value = PredicateValue::List(values);
        let expected = match op {
            MatchOp::Equal => LabelFilter {
                label: label.to_string(),
                matcher: PredicateMatch::Equal(value.clone()),
            },
            MatchOp::NotEqual => LabelFilter {
                label: label.to_string(),
                matcher: PredicateMatch::NotEqual(value.clone()),
            },
            MatchOp::StartsWith => LabelFilter {
                label: label.to_string(),
                matcher: PredicateMatch::StartsWith(value.clone()),
            },
            MatchOp::NotStartsWith => LabelFilter {
                label: label.to_string(),
                matcher: PredicateMatch::NotStartsWith(value),
            },
            _ => panic!("unsupported list matcher op: {op:?}"),
        };
        assert_eq!(
            matcher, &expected,
            "expected matcher: {}, found {}",
            &expected, matcher
        );
    }

    fn with_and_matchers<F>(matchers: &SeriesSelector, f: F)
    where
        F: Fn(&[LabelFilter]),
    {
        match matchers {
            SeriesSelector::And(m) => f(m),
            _ => panic!("expected AND matcher"),
        }
    }

    fn with_or_matchers<F>(matchers: &SeriesSelector, f: F)
    where
        F: Fn(&[FilterList]),
    {
        match matchers {
            SeriesSelector::Or(m) => f(m),
            _ => panic!("expected OR matcher"),
        }
    }

    #[test]
    fn test_parse_series_selector_empty_input() {
        let result = parse_series_selector("");
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().to_string(), "Empty series selector");
    }

    #[test]
    fn test_series_selector_number_literal_value() {
        let input = "job=1234";
        let result = parse_series_selector(input).unwrap();

        with_and_matchers(&result, |matchers| {
            assert_eq!(matchers.len(), 1);

            let matcher = &matchers[0];
            assert_eq!(matcher.label, "job");
            assert!(
                matches!(matcher.matcher, PredicateMatch::Equal(PredicateValue::String(ref s)) if s == "1234")
            );
        });
    }

    #[test]
    fn test_parse_series_selector_single_label_matcher_without_metric_name() {
        let input = "{job=\"prometheus\"}";
        let result = parse_series_selector(input).unwrap();

        let metric_name = result.get_metric_name();
        assert_eq!(metric_name, None);

        with_and_matchers(&result, |matchers| {
            assert_eq!(matchers.len(), 1);

            let matcher = &matchers[0];
            assert_eq!(matcher.label, "job");
            assert!(
                matches!(matcher.matcher, PredicateMatch::Equal(PredicateValue::String(ref s)) if s == "prometheus")
            );
        });
    }

    #[test]
    fn test_parse_series_selector_with_or_conditions() {
        let selector = r#"{__name__="metric_name",label1="value1" or label2=~"value.*"}"#;
        let matchers = parse_series_selector(selector).unwrap();

        let metric_name = matchers.get_metric_name();
        assert_eq!(metric_name, Some("metric_name"));

        with_or_matchers(&matchers, |or_matchers| {
            assert_eq!(or_matchers.len(), 2);
            assert_eq!(or_matchers[0].len(), 2);
            assert_eq!(or_matchers[1].len(), 1);
            assert_contains_matcher(&or_matchers[0], "label1", MatchOp::Equal, "value1");
            assert_contains_matcher(&or_matchers[1], "label2", MatchOp::RegexEqual, "value.*");
        });
    }

    #[test]
    fn test_parse_series_selector_multiple_or_conditions() {
        let selector = r#"{job="prometheus",env="prod" or datacenter=~"us-.*" or instance="localhost",role!="standby"}"#;
        let matchers = parse_series_selector(selector).unwrap();

        assert!(matchers.get_metric_name().is_none());
        with_or_matchers(&matchers, |or_matchers| {
            assert_eq!(or_matchers.len(), 3);

            assert_eq!(or_matchers[0].len(), 2);
            assert_eq!(or_matchers[1].len(), 1);
            assert_eq!(or_matchers[2].len(), 2);
            assert_matcher(&or_matchers[0][0], "job", MatchOp::Equal, "prometheus");
            assert_matcher(&or_matchers[0][1], "env", MatchOp::Equal, "prod");
            assert_matcher(
                &or_matchers[1][0],
                "datacenter",
                MatchOp::RegexEqual,
                "us-.*",
            );
            assert_matcher(&or_matchers[2][0], "instance", MatchOp::Equal, "localhost");
            assert_matcher(&or_matchers[2][1], "role", MatchOp::NotEqual, "standby");
        });
    }

    #[test]
    fn test_parse_series_selector_or_branch_without_positive_matcher_is_rejected() {
        // OR branches are unioned, so an unbounded branch drags the whole selector
        // unbounded even though the other branch is bounded.
        assert_unbounded_alone(r#"{job="prometheus",env="prod" or instance!="localhost"}"#);
    }

    #[test]
    fn test_parse_series_selector_with_regex_not_equal_matchers() {
        // A filter set consisting solely of negative matchers has nothing to intersect
        // against but the whole keyspace, so it is rejected (mirrors RedisTimeSeries).
        let matchers = assert_unbounded_alone(r#"{job!~"prom.*",instance!~"local.*"}"#);

        assert!(matchers.get_metric_name().is_none());
        with_and_matchers(&matchers, |and_matchers| {
            assert_eq!(and_matchers.len(), 2);
            assert_matcher(&and_matchers[0], "job", MatchOp::RegexNotEqual, "prom.*");
            assert_matcher(
                &and_matchers[1],
                "instance",
                MatchOp::RegexNotEqual,
                "local.*",
            );
        });
    }

    #[test]
    fn test_parse_series_selector_with_negated_label_matchers() {
        // Same as above: two negative matchers, no positive matcher.
        let matchers = assert_unbounded_alone(r#"{job!="prometheus",instance!="localhost:9090"}"#);

        assert!(matchers.get_metric_name().is_none());
        with_and_matchers(&matchers, |and_matchers| {
            assert_eq!(and_matchers.len(), 2);
            assert_matcher(&and_matchers[0], "job", MatchOp::NotEqual, "prometheus");
            assert_matcher(
                &and_matchers[1],
                "instance",
                MatchOp::NotEqual,
                "localhost:9090",
            );
        });
    }

    #[test]
    fn test_validate_selector_list_accepts_bounded_sibling() {
        // The filter list is conjunctive, so one bounded selector bounds the whole query:
        // `TS.QUERYINDEX n=1 i!=a` must be accepted even though `i!=a` alone is not.
        let bounded = parse_series_selector("n=1").unwrap();
        let unbounded = parse_series_selector("i!=a").unwrap();
        assert!(bounded.is_bounded());
        assert!(!unbounded.is_bounded());

        validate_selector_list(&[bounded.clone(), unbounded.clone()])
            .expect("bounded sibling should bound the list");
        validate_selector_list(&[unbounded.clone(), bounded]).expect("order must not matter");
        validate_selector_list(&[unbounded]).expect_err("no bounded selector in the list");
    }

    #[test]
    fn test_validate_selector_list_rejects_empty_and_empty_matching_filters() {
        // `l=` (label absent) and `l=~".*"` are both satisfied by a missing label, so neither
        // narrows the search. `l=~".+"` requires a non-empty value, so it does.
        for selector in [r#"{i=""}"#, r#"{i=~".*"}"#, r#"{i!=""}"#] {
            assert_unbounded_alone(selector);
        }
        let bounded = parse_series_selector(r#"{i=~".+"}"#).unwrap();
        assert!(bounded.is_bounded());
        validate_selector_list(&[bounded]).expect("`.+` cannot match a missing label");
    }

    #[test]
    fn test_parse_series_selector_with_negated_label_matchers_and_positive_matcher() {
        // Pairing a negative matcher with a positive one is still allowed.
        let input = r#"{job="prometheus",instance!="localhost:9090"}"#;
        let matchers = parse_series_selector(input).unwrap();

        assert!(matchers.get_metric_name().is_none());
        with_and_matchers(&matchers, |and_matchers| {
            assert_eq!(and_matchers.len(), 2);
            assert_matcher(&and_matchers[0], "job", MatchOp::Equal, "prometheus");
            assert_matcher(
                &and_matchers[1],
                "instance",
                MatchOp::NotEqual,
                "localhost:9090",
            );
        });
    }

    #[test]
    fn test_parse_series_selector_with_special_characters() {
        let input = "metric_name:with.special_characters";
        let matchers = parse_series_selector(input).unwrap();

        let metric_name = matchers.get_metric_name();
        assert_eq!(metric_name, Some("metric_name:with.special_characters"));
        assert_eq!(matchers.len(), 1);
    }

    #[test]
    fn test_parse_series_selector_with_metric_name_and_labels() {
        let input = "http_requests_total{method=\"GET\", status=\"200\"}";
        let result = parse_series_selector(input).unwrap();

        assert_eq!(result.get_metric_name(), Some("http_requests_total"));
        assert_eq!(result.len(), 3);

        with_and_matchers(&result, |and_matchers| {
            let method_matcher = &and_matchers[0];
            assert_matcher(method_matcher, "method", MatchOp::Equal, "GET");

            let status_matcher = &and_matchers[1];
            assert_matcher(status_matcher, "status", MatchOp::Equal, "200");
        });
    }

    #[test]
    fn test_prometheus_selector_with_list_matcher() {
        let input = "http_requests_total{method=(GET,SET,\"POST\"), status=~\"2[0-9]{2}\"}";
        let result = parse_series_selector(input).unwrap();

        assert_eq!(result.get_metric_name(), Some("http_requests_total"));
        with_and_matchers(&result, |matchers| {
            assert_eq!(matchers.len(), 3);

            let method_matcher = &matchers[0];
            assert_list_matcher(
                method_matcher,
                "method",
                MatchOp::Equal,
                &["GET", "SET", "POST"],
            );

            let status_matcher = &matchers[1];
            assert_matcher(status_matcher, "status", MatchOp::RegexEqual, "2[0-9]{2}");
        });
    }

    #[test]
    fn test_metric_name_distributed_across_or_conditions() {
        // test that the metric name is distributed across each OR branch; i.e.,
        // "http_requests{status="400" or method="POST"}"
        // gets parsed to
        // {__name__="http_requests", status="400"} or {__name__="http_requests", method="POST"}
        let matchers =
            parse_series_selector("http_requests{status=\"400\" or method=\"POST\"}").unwrap();

        assert_eq!(matchers.get_metric_name(), Some("http_requests"));

        with_or_matchers(&matchers, |or_matchers| {
            assert_eq!(or_matchers.len(), 2);

            // Each OR branch should include the metric name matcher
            for branch in or_matchers {
                let has_metric_name = branch.iter().any(|m| {
                    m.label == "__name__" && matches!(m.matcher, PredicateMatch::Equal(PredicateValue::String(ref s)) if s == "http_requests")
                });
                assert!(
                    has_metric_name,
                    "Each OR branch must include metric name matcher"
                );
            }
        });
    }

    #[test]
    fn test_parse_series_selector_metric_name_only() {
        let input = "metric_name";
        let matchers = parse_series_selector(input).unwrap();

        assert_eq!(matchers.get_metric_name(), Some("metric_name"));
        assert_eq!(matchers.len(), 1);
        assert!(matchers.is_only_metric_name());

        let input = "metric_name{}";
        let matchers = parse_series_selector(input).unwrap();
        assert_eq!(matchers.get_metric_name(), Some("metric_name"));
        assert_eq!(matchers.len(), 1);
        assert!(matchers.is_only_metric_name());
    }

    #[test]
    fn test_parse_series_selector_redis_ts_style() {
        let input = "temperature=hot";
        let result = parse_series_selector(input).unwrap();

        assert!(result.get_metric_name().is_none());
        with_and_matchers(&result, |matchers| {
            assert_eq!(matchers.len(), 1);

            let temperature_matcher = &matchers[0];
            assert_matcher(temperature_matcher, "temperature", MatchOp::Equal, "hot");
        });
    }

    #[test]
    fn test_selector_with_newline() {
        let input = r#"label="\n""#;
        let result = parse_series_selector(input).unwrap();

        with_and_matchers(&result, |matchers| {
            assert_eq!(matchers.len(), 1);

            let temperature_matcher = &matchers[0];
            assert_matcher(temperature_matcher, "label", MatchOp::Equal, "\n");
        });
    }

    // https://redis.io/docs/latest/commands/ts.queryindex/
    #[test]
    fn redis_ts_selector_equal_with_lists() {
        let input = "size=(small,medium,large)";
        let result = parse_series_selector(input).unwrap();

        assert!(result.get_metric_name().is_none());
        with_and_matchers(&result, |matchers| {
            assert_eq!(matchers.len(), 1);

            let matcher = &matchers[0];
            assert_list_matcher(
                matcher,
                "size",
                MatchOp::Equal,
                &["small", "medium", "large"],
            );
        });
    }

    #[test]
    fn redis_ts_selector_not_equal_with_lists() {
        // A bare NotEqual-with-list selector has no positive matcher and is rejected.
        assert_unbounded_alone("flavor!=(original,cajun,\"extra spicy\")");

        let input = "{size=(small,medium,large),flavor!=(original,cajun,\"extra spicy\")}";
        let result = parse_series_selector(input).unwrap();

        with_and_matchers(&result, |matchers| {
            assert_eq!(matchers.len(), 2);

            let matcher = &matchers[1];
            assert_list_matcher(
                matcher,
                "flavor",
                MatchOp::NotEqual,
                &["original", "cajun", "extra spicy"],
            );
        });
    }

    #[test]
    fn redis_ts_selector_starts_with_with_lists() {
        let input = "service^=(api,worker,\"batch-job\")";
        let result = parse_series_selector(input).unwrap();

        with_and_matchers(&result, |matchers| {
            assert_eq!(matchers.len(), 1);

            let matcher = &matchers[0];
            assert_list_matcher(
                matcher,
                "service",
                MatchOp::StartsWith,
                &["api", "worker", "batch-job"],
            );
        });
    }

    #[test]
    fn prometheus_selector_not_starts_with_with_lists() {
        // A bare NotStartsWith-with-list selector has no positive matcher and is rejected.
        assert_unbounded_alone(r#"{service^~(api,worker)}"#);

        let input = r#"{env="prod",service^~(api,worker)}"#;
        let result = parse_series_selector(input).unwrap();

        with_and_matchers(&result, |matchers| {
            assert_eq!(matchers.len(), 2);

            let matcher = &matchers[1];
            assert_list_matcher(
                matcher,
                "service",
                MatchOp::NotStartsWith,
                &["api", "worker"],
            );
        });
    }

    #[test]
    fn starts_with_selector_rejects_empty_values() {
        for input in ["service^=\"\"", "service^=(api,\"\")", "{service^=()}"] {
            let err = parse_series_selector(input).expect_err("empty prefix values should fail");
            assert!(
                err.to_string()
                    .contains("starts with matcher does not allow empty values"),
                "unexpected error for {input:?}: {err}"
            );
        }
    }

    #[test]
    fn regex_match_all_selector_normalizes_to_match_all_but_is_rejected() {
        // `l=~".*"` matches the empty string, so a selector consisting only of such matchers
        // is just as much a full-keyspace scan as `l!="nonexistent"` and is rejected for the
        // same reason.
        assert_unbounded_alone(r#"{service=~".*", instance=~"^.*$"}"#);

        let input = r#"{job="x", service=~".*", instance=~"^.*$"}"#;
        let result = parse_series_selector(input).unwrap();

        with_and_matchers(&result, |matchers| {
            assert_eq!(matchers.len(), 3);
            assert_eq!(matchers[1].label, "service");
            assert!(matches!(matchers[1].matcher, PredicateMatch::MatchAll));
            assert_eq!(matchers[2].label, "instance");
            assert!(matches!(matchers[2].matcher, PredicateMatch::MatchAll));
        });
    }

    #[test]
    fn regex_not_match_all_selector_normalizes_to_match_none() {
        let input = r#"{service!~".*", instance!~"^.*$"}"#;
        let result = parse_series_selector(input).unwrap();

        with_and_matchers(&result, |matchers| {
            assert_eq!(matchers.len(), 2);
            assert_eq!(matchers[0].label, "service");
            assert!(
                matches!(matchers[0].matcher, PredicateMatch::MatchNone),
                "unexpected matcher: {:?}",
                matchers[0].matcher
            );
            assert_eq!(matchers[1].label, "instance");
            assert!(
                matches!(matchers[1].matcher, PredicateMatch::MatchNone),
                "unexpected matcher: {:?}",
                matchers[1].matcher
            );
        });
    }

    #[test]
    fn test_parse_series_selector_with_quoted_labels() {
        let input = r#"{"metric.name"="value", "label-with-dash"="foo", "quoted.label"=~"val.*"}"#;
        let result = parse_series_selector(input).unwrap();

        with_and_matchers(&result, |matchers| {
            assert_eq!(matchers.len(), 3);

            assert_matcher(&matchers[0], "metric.name", MatchOp::Equal, "value");
            assert_matcher(&matchers[1], "label-with-dash", MatchOp::Equal, "foo");
            assert_matcher(&matchers[2], "quoted.label", MatchOp::RegexEqual, "val.*");
        });
    }

    #[test]
    fn test_parse_series_selector_with_escaped_quotes() {
        let input =
            r#"{"metric.name"="value", "label-with-dash"="foo", "quoted.label"=~"val\".*"}"#;
        let result = parse_series_selector(input).unwrap();

        with_and_matchers(&result, |matchers| {
            assert_eq!(matchers.len(), 3);

            assert_matcher(&matchers[0], "metric.name", MatchOp::Equal, "value");
            assert_matcher(&matchers[1], "label-with-dash", MatchOp::Equal, "foo");
            assert_matcher(
                &matchers[2],
                "quoted.label",
                MatchOp::RegexEqual,
                r##"val".*"##,
            );
        });
    }

    #[test]
    fn test_parse_single_identifier_matcher() {
        let input = r#"{"foo"}"#;
        let result = parse_series_selector(input).unwrap();

        assert_eq!(result.get_metric_name(), Some("foo"));
        assert_eq!(result.len(), 1);
    }

    #[test]
    fn test_parse_single_identifier_matcher_with_normal() {
        let input = r#"{"foo", a="bc"}"#;
        let result = parse_series_selector(input).unwrap();
        // the metric name should be set to "foo"
        let metric_name = result.get_metric_name();

        assert_eq!(metric_name, Some("foo"));
        assert_eq!(result.len(), 2);
        with_and_matchers(&result, |matchers| {
            assert_contains_matcher(matchers, "a", MatchOp::Equal, "bc");
        });
    }

    #[test]
    fn test_parse_reserved_symbols_as_labels() {
        let input = r#"foo{NaN="inf"}"#;
        let result = parse_series_selector(input).unwrap();

        // the metric name should be set to "foo"
        let metric_name = result.get_metric_name();
        assert_eq!(metric_name, Some("foo"));
        assert_eq!(result.len(), 2);

        with_and_matchers(&result, |matchers| {
            assert_contains_matcher(matchers, "NaN", MatchOp::Equal, "inf");
        });
    }

    #[test]
    fn test_parse_metric_name_in_the_middle_of_selector_list() {
        let input = r#"{a="b", foo!="bar", "metric_name", test=~"test", bar!~"baz"}"#;
        let result = parse_series_selector(input).unwrap();

        let metric_name = result.get_metric_name();
        assert_eq!(metric_name, Some("metric_name"));
        assert_eq!(result.len(), 5);

        with_and_matchers(&result, |matchers| {
            assert_contains_matcher(matchers, "a", MatchOp::Equal, "b");
            assert_contains_matcher(matchers, "foo", MatchOp::NotEqual, "bar");
            assert_contains_matcher(matchers, "test", MatchOp::RegexEqual, "test");
            assert_contains_matcher(matchers, "bar", MatchOp::RegexNotEqual, "baz");
        });
    }

    // OR tests
    #[test]
    fn test_parse_or_with_list_matchers() {
        let input = r#"{a="b", foo!="bar" or size=(small,medium,large) or color="red",flavor!=(original,cajun,"extra spicy")}"#;
        let result = parse_series_selector(input).unwrap();

        assert!(result.get_metric_name().is_none());
        with_or_matchers(&result, |or_matchers| {
            assert_eq!(or_matchers.len(), 3);

            // First OR branch
            assert_eq!(or_matchers[0].len(), 2);
            assert_contains_matcher(&or_matchers[0], "a", MatchOp::Equal, "b");
            assert_contains_matcher(&or_matchers[0], "foo", MatchOp::NotEqual, "bar");

            // Second OR branch
            assert_eq!(or_matchers[1].len(), 1);
            assert_list_matcher(
                &or_matchers[1][0],
                "size",
                MatchOp::Equal,
                &["small", "medium", "large"],
            );

            // Third OR branch
            assert_eq!(or_matchers[2].len(), 2);
            assert_contains_matcher(&or_matchers[2], "color", MatchOp::Equal, "red");
            assert_list_matcher(
                &or_matchers[2][1],
                "flavor",
                MatchOp::NotEqual,
                &["original", "cajun", "extra spicy"],
            );
        });
    }

    #[test]
    fn test_parse_or_with_list_matchers_branch_without_positive_matcher_is_rejected() {
        assert_unbounded_alone(r#"{a="b", foo!="bar" or flavor!=(original,cajun,"extra spicy")}"#);
    }

    #[test]
    fn test_or_with_prometheus_style_matchers() {
        let selector =
            r#"http_status{status="500"} or api_host{service="auth", env=~"prod|staging"}"#;
        let matchers = parse_series_selector(selector).unwrap();
        assert!(matchers.get_metric_name().is_none());
        with_or_matchers(&matchers, |or_matchers| {
            assert_eq!(or_matchers.len(), 2);

            // First OR branch
            let first = &or_matchers[0];
            assert_eq!(first.len(), 2); // metric name + status matcher
            assert_contains_matcher(first, "__name__", MatchOp::Equal, "http_status");
            assert_contains_matcher(first, "status", MatchOp::Equal, "500");

            // Second OR branch
            let second = &or_matchers[1];
            assert_eq!(second.len(), 3); // metric name + 2 matchers
            assert_contains_matcher(second, "__name__", MatchOp::Equal, "api_host");
            assert_contains_matcher(second, "service", MatchOp::Equal, "auth");
            assert_contains_matcher(second, "env", MatchOp::RegexEqual, "prod|staging");
        });
    }
}
