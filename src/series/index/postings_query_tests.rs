//! Tests for querying postings from the time series index.
//! Specifically, these tests verify that the correct postings are returned for a given set of
//! Prometheus style series selectors. These tests are heavily inspired by Prometheus's own
//! querier tests.
//!
//! https://github.com/prometheus/prometheus/blob/main/tsdb/index/postings_test.go
//! https://github.com/prometheus/prometheus/blob/main/tsdb/querier_test.go (see func TestPostingsForMatchers())
//!
//! Original Code Copyright 2017 The Prometheus Authors
//! Licensed under the Apache License, Version 2.0 (the "License");
#[cfg(test)]
mod tests {
    use crate::labels::filters::{LabelFilter, MatchOp, SeriesSelector};
    use crate::labels::{Label, MetricName};
    use crate::series::index::{TimeSeriesIndex, next_timeseries_id};
    use crate::series::{SeriesRef, TimeSeries};
    use std::collections::{HashMap, HashSet};

    fn labels_from_strings<S: Into<String> + Clone>(ss: &[S]) -> Vec<Label> {
        if ss.is_empty() {
            return vec![];
        }
        if !ss.len().is_multiple_of(2) {
            panic!("labels_from_strings: odd number of strings")
        }
        let mut labels = vec![];
        for i in (0..ss.len()).step_by(2) {
            let name = <S as Clone>::clone(&ss[i]).into();
            let value = <S as Clone>::clone(&ss[i + 1]).into();
            labels.push(Label { name, value })
        }
        labels
    }

    fn add_series(
        ix: &mut TimeSeriesIndex,
        labels_map: &mut HashMap<SeriesRef, Vec<Label>>,
        series_ref: SeriesRef,
        labels: &[Label],
    ) {
        let mut state = ();
        ix.with_postings_mut(&mut state, |p, _| {
            for label in labels.iter() {
                p.add_posting_for_label_value(series_ref, &label.name, &label.value);
            }
        });

        labels_map.insert(series_ref, labels.to_vec());
    }

    fn to_label_vec(labels: &[Vec<Label>]) -> Vec<Vec<Label>> {
        labels
            .iter()
            .map(|items| {
                let mut items = items.clone();
                items.sort();
                items
            })
            .collect::<Vec<_>>()
    }

    fn get_labels_by_filters(
        ix: &TimeSeriesIndex,
        filters: &[LabelFilter],
        series_data: &HashMap<SeriesRef, Vec<Label>>,
    ) -> Vec<Vec<Label>> {
        let copy = filters.to_vec().clone();
        let filter = SeriesSelector::with_filters(copy);
        let p = ix.postings_for_selector(&filter).unwrap();
        let mut actual: Vec<_> = p
            .iter()
            .flat_map(|id| series_data.get(&id))
            .cloned()
            .collect();

        actual.sort();
        actual
    }

    fn label_vec_to_string(labels: &[Label]) -> String {
        let mut items = labels.to_vec();
        items.sort();

        items
            .iter()
            .map(|l| l.to_string())
            .collect::<Vec<_>>()
            .join(", ")
    }

    #[test]
    fn test_postings_for_matchers() {
        use MatchOp::*;

        let mut ix: TimeSeriesIndex = TimeSeriesIndex::default();
        let mut labels_map: HashMap<SeriesRef, Vec<Label>> = HashMap::new();

        let series_data = HashMap::from([
            (1, labels_from_strings(&["n", "1"])),
            (2, labels_from_strings(&["n", "1", "i", "a"])),
            (3, labels_from_strings(&["n", "1", "i", "b"])),
            (4, labels_from_strings(&["n", "1", "i", "\n"])),
            (5, labels_from_strings(&["n", "2"])),
            (6, labels_from_strings(&["n", "2.5"])),
        ]);

        for (series_ref, labels) in series_data.iter() {
            add_series(&mut ix, &mut labels_map, *series_ref, labels);
        }

        struct TestCase {
            matchers: Vec<LabelFilter>,
            exp: Vec<Vec<Label>>,
        }

        let cases = vec![
            TestCase {
                matchers: vec![
                    LabelFilter::create(Equal, "n", "1").unwrap(),
                    LabelFilter::create(RegexEqual, "i", "^.*$").unwrap(),
                ],
                exp: to_label_vec(&[
                    labels_from_strings(&["n", "1"]),
                    labels_from_strings(&["n", "1", "i", "a"]),
                    labels_from_strings(&["n", "1", "i", "b"]),
                    labels_from_strings(&["n", "1", "i", "\n"]),
                ]),
            },
            // ----------------------------------------------------------------
            TestCase {
                matchers: vec![LabelFilter::create(Equal, "n", "1").unwrap()],
                exp: to_label_vec(&[
                    labels_from_strings(&["n", "1"]),
                    labels_from_strings(&["n", "1", "i", "a"]),
                    labels_from_strings(&["n", "1", "i", "b"]),
                    labels_from_strings(&["n", "1", "i", "\n"]),
                ]),
            },
            TestCase {
                matchers: vec![
                    LabelFilter::create(Equal, "n", "1").unwrap(),
                    LabelFilter::create(Equal, "i", "a").unwrap(),
                ],
                exp: to_label_vec(&[labels_from_strings(&["n", "1", "i", "a"])]),
            },
            TestCase {
                matchers: vec![
                    LabelFilter::create(Equal, "n", "1").unwrap(),
                    LabelFilter::create(Equal, "i", "missing").unwrap(),
                ],
                exp: vec![],
            },
            TestCase {
                matchers: vec![LabelFilter::create(Equal, "missing", "").unwrap()],
                exp: to_label_vec(&[
                    labels_from_strings(&["n", "1"]),
                    labels_from_strings(&["n", "1", "i", "a"]),
                    labels_from_strings(&["n", "1", "i", "b"]),
                    labels_from_strings(&["n", "1", "i", "\n"]),
                    labels_from_strings(&["n", "2"]),
                    labels_from_strings(&["n", "2.5"]),
                ]),
            },
            // Not equals
            TestCase {
                matchers: vec![LabelFilter::create(NotEqual, "n", "1").unwrap()],
                exp: to_label_vec(&[
                    labels_from_strings(&["n", "2"]),
                    labels_from_strings(&["n", "2.5"]),
                ]),
            },
            TestCase {
                matchers: vec![LabelFilter::create(NotEqual, "i", "").unwrap()],
                exp: to_label_vec(&[
                    labels_from_strings(&["n", "1", "i", "a"]),
                    labels_from_strings(&["n", "1", "i", "b"]),
                    labels_from_strings(&["n", "1", "i", "\n"]),
                ]),
            },
            TestCase {
                matchers: vec![LabelFilter::create(NotEqual, "missing", "").unwrap()],
                exp: vec![],
            },
            TestCase {
                matchers: vec![
                    LabelFilter::create(Equal, "n", "1").unwrap(),
                    LabelFilter::create(NotEqual, "i", "a").unwrap(),
                ],
                exp: to_label_vec(&[
                    labels_from_strings(&["n", "1"]),
                    labels_from_strings(&["n", "1", "i", "b"]),
                    labels_from_strings(&["n", "1", "i", "\n"]),
                ]),
            },
            //----------------------------------------------------------------------------
            TestCase {
                matchers: vec![
                    LabelFilter::create(Equal, "n", "1").unwrap(),
                    LabelFilter::create(Equal, "i", "a").unwrap(),
                ],
                exp: vec![labels_from_strings(&["n", "1", "i", "a"])],
            },
            TestCase {
                matchers: vec![
                    LabelFilter::create(Equal, "n", "1").unwrap(),
                    LabelFilter::create(Equal, "i", "missing").unwrap(),
                ],
                exp: vec![],
            },
            TestCase {
                matchers: vec![LabelFilter::create(Equal, "missing", "").unwrap()],
                exp: to_label_vec(&[
                    labels_from_strings(&["n", "1"]),
                    labels_from_strings(&["n", "1", "i", "a"]),
                    labels_from_strings(&["n", "1", "i", "b"]),
                    labels_from_strings(&["n", "1", "i", "\n"]),
                    labels_from_strings(&["n", "2"]),
                    labels_from_strings(&["n", "2.5"]),
                ]),
            },
            // Not equals.
            TestCase {
                matchers: vec![LabelFilter::create(NotEqual, "n", "1").unwrap()],
                exp: to_label_vec(&[
                    labels_from_strings(&["n", "2"]),
                    labels_from_strings(&["n", "2.5"]),
                ]),
            },
            TestCase {
                matchers: vec![LabelFilter::create(NotEqual, "i", "").unwrap()],
                exp: to_label_vec(&[
                    labels_from_strings(&["n", "1", "i", "a"]),
                    labels_from_strings(&["n", "1", "i", "b"]),
                    labels_from_strings(&["n", "1", "i", "\n"]),
                ]),
            },
            TestCase {
                matchers: vec![LabelFilter::create(NotEqual, "missing", "").unwrap()],
                exp: vec![],
            },
            TestCase {
                matchers: vec![
                    LabelFilter::create(Equal, "n", "1").unwrap(),
                    LabelFilter::create(NotEqual, "i", "a").unwrap(),
                ],
                exp: to_label_vec(&[
                    labels_from_strings(&["n", "1"]),
                    labels_from_strings(&["n", "1", "i", "b"]),
                    labels_from_strings(&["n", "1", "i", "\n"]),
                ]),
            },
            TestCase {
                matchers: vec![
                    LabelFilter::create(Equal, "n", "1").unwrap(),
                    LabelFilter::create(NotEqual, "i", "").unwrap(),
                ],
                exp: to_label_vec(&[
                    labels_from_strings(&["n", "1", "i", "a"]),
                    labels_from_strings(&["n", "1", "i", "b"]),
                    labels_from_strings(&["n", "1", "i", "\n"]),
                ]),
            },
            // Regex.
            TestCase {
                matchers: vec![LabelFilter::create(RegexEqual, "n", "^1$").unwrap()],
                exp: to_label_vec(&[
                    labels_from_strings(&["n", "1"]),
                    labels_from_strings(&["n", "1", "i", "a"]),
                    labels_from_strings(&["n", "1", "i", "b"]),
                    labels_from_strings(&["n", "1", "i", "\n"]),
                ]),
            },
            TestCase {
                matchers: vec![
                    LabelFilter::create(Equal, "n", "1").unwrap(),
                    LabelFilter::create(RegexEqual, "i", "^a$").unwrap(),
                ],
                exp: vec![labels_from_strings(&["n", "1", "i", "a"])],
            },
            TestCase {
                matchers: vec![
                    LabelFilter::create(Equal, "n", "1").unwrap(),
                    LabelFilter::create(RegexEqual, "i", "^a?$").unwrap(),
                ],
                exp: to_label_vec(&[
                    labels_from_strings(&["n", "1"]),
                    labels_from_strings(&["n", "1", "i", "a"]),
                ]),
            },
            TestCase {
                matchers: vec![LabelFilter::create(RegexEqual, "i", "^$").unwrap()],
                exp: to_label_vec(&[
                    labels_from_strings(&["n", "1"]),
                    labels_from_strings(&["n", "2"]),
                    labels_from_strings(&["n", "2.5"]),
                ]),
            },
            TestCase {
                matchers: vec![
                    LabelFilter::create(Equal, "n", "1").unwrap(),
                    LabelFilter::create(RegexEqual, "i", "^$").unwrap(),
                ],
                exp: vec![labels_from_strings(&["n", "1"])],
            },
            TestCase {
                matchers: vec![
                    LabelFilter::create(Equal, "n", "1").unwrap(),
                    LabelFilter::create(RegexEqual, "i", "^.*$").unwrap(),
                ],
                exp: to_label_vec(&[
                    labels_from_strings(&["n", "1"]),
                    labels_from_strings(&["n", "1", "i", "a"]),
                    labels_from_strings(&["n", "1", "i", "b"]),
                    labels_from_strings(&["n", "1", "i", "\n"]),
                ]),
            },
            TestCase {
                matchers: vec![
                    LabelFilter::create(Equal, "n", "1").unwrap(),
                    LabelFilter::create(RegexEqual, "i", "^.+$").unwrap(),
                ],
                exp: to_label_vec(&[
                    labels_from_strings(&["n", "1", "i", "a"]),
                    labels_from_strings(&["n", "1", "i", "b"]),
                    labels_from_strings(&["n", "1", "i", "\n"]),
                ]),
            },
            // Not regex.
            TestCase {
                matchers: vec![LabelFilter::create(RegexNotEqual, "i", "").unwrap()],
                exp: to_label_vec(&[
                    labels_from_strings(&["n", "1", "i", "a"]),
                    labels_from_strings(&["n", "1", "i", "b"]),
                    labels_from_strings(&["n", "1", "i", "\n"]),
                ]),
            },
            TestCase {
                matchers: vec![LabelFilter::create(RegexNotEqual, "n", "^1$").unwrap()],
                exp: to_label_vec(&[
                    labels_from_strings(&["n", "2"]),
                    labels_from_strings(&["n", "2.5"]),
                ]),
            },
            TestCase {
                matchers: vec![LabelFilter::create(RegexNotEqual, "n", "1").unwrap()],
                exp: to_label_vec(&[
                    labels_from_strings(&["n", "2"]),
                    labels_from_strings(&["n", "2.5"]),
                ]),
            },
            TestCase {
                matchers: vec![LabelFilter::create(RegexNotEqual, "n", "1|2.5").unwrap()],
                exp: vec![labels_from_strings(&["n", "2"])],
            },
            TestCase {
                matchers: vec![LabelFilter::create(RegexNotEqual, "n", "(1|2.5)").unwrap()],
                exp: vec![labels_from_strings(&["n", "2"])],
            },
            TestCase {
                matchers: vec![
                    LabelFilter::create(Equal, "n", "1").unwrap(),
                    LabelFilter::create(RegexNotEqual, "i", "^a$").unwrap(),
                ],
                exp: to_label_vec(&[
                    labels_from_strings(&["n", "1"]),
                    labels_from_strings(&["n", "1", "i", "b"]),
                    labels_from_strings(&["n", "1", "i", "\n"]),
                ]),
            },
            TestCase {
                matchers: vec![
                    LabelFilter::create(Equal, "n", "1").unwrap(),
                    LabelFilter::create(RegexNotEqual, "i", "^a?$").unwrap(),
                ],
                exp: to_label_vec(&[
                    labels_from_strings(&["n", "1", "i", "b"]),
                    labels_from_strings(&["n", "1", "i", "\n"]),
                ]),
            },
            TestCase {
                matchers: vec![
                    LabelFilter::create(Equal, "n", "1").unwrap(),
                    LabelFilter::create(RegexNotEqual, "i", "^$").unwrap(),
                ],
                exp: to_label_vec(&[
                    labels_from_strings(&["n", "1", "i", "a"]),
                    labels_from_strings(&["n", "1", "i", "b"]),
                    labels_from_strings(&["n", "1", "i", "\n"]),
                ]),
            },
            TestCase {
                matchers: vec![
                    LabelFilter::create(Equal, "n", "1").unwrap(),
                    LabelFilter::create(RegexNotEqual, "i", "^.*$").unwrap(),
                ],
                exp: vec![],
            },
            TestCase {
                matchers: vec![
                    LabelFilter::create(Equal, "n", "1").unwrap(),
                    LabelFilter::create(RegexNotEqual, "i", "^.+$").unwrap(),
                ],
                exp: vec![labels_from_strings(&["n", "1"])],
            },
            // Combinations.
            TestCase {
                matchers: vec![
                    LabelFilter::create(Equal, "n", "1").unwrap(),
                    LabelFilter::create(NotEqual, "i", "").unwrap(),
                    LabelFilter::create(Equal, "i", "a").unwrap(),
                ],
                exp: vec![labels_from_strings(&["n", "1", "i", "a"])],
            },
            TestCase {
                matchers: vec![
                    LabelFilter::create(Equal, "n", "1").unwrap(),
                    LabelFilter::create(NotEqual, "i", "b").unwrap(),
                    LabelFilter::create(RegexEqual, "i", "^(b|a).*$").unwrap(),
                ],
                exp: vec![labels_from_strings(&["n", "1", "i", "a"])],
            },
            // Set optimization for Regex.
            // Refer to https://github.com/prometheus/prometheus/issues/2651.
            TestCase {
                matchers: vec![LabelFilter::create(RegexEqual, "n", "1|2").unwrap()],
                exp: to_label_vec(&[
                    labels_from_strings(&["n", "1"]),
                    labels_from_strings(&["n", "1", "i", "a"]),
                    labels_from_strings(&["n", "1", "i", "b"]),
                    labels_from_strings(&["n", "1", "i", "\n"]),
                    labels_from_strings(&["n", "2"]),
                ]),
            },
            TestCase {
                matchers: vec![LabelFilter::create(RegexEqual, "i", "a|b").unwrap()],
                exp: to_label_vec(&[
                    labels_from_strings(&["n", "1", "i", "a"]),
                    labels_from_strings(&["n", "1", "i", "b"]),
                ]),
            },
            TestCase {
                matchers: vec![LabelFilter::create(RegexEqual, "i", "(a|b)").unwrap()],
                exp: to_label_vec(&[
                    labels_from_strings(&["n", "1", "i", "a"]),
                    labels_from_strings(&["n", "1", "i", "b"]),
                ]),
            },
            TestCase {
                matchers: vec![LabelFilter::create(RegexEqual, "n", "x1|2").unwrap()],
                exp: vec![labels_from_strings(&["n", "2"])],
            },
            TestCase {
                matchers: vec![LabelFilter::create(RegexEqual, "n", "2|2.5").unwrap()],
                exp: to_label_vec(&[
                    labels_from_strings(&["n", "2"]),
                    labels_from_strings(&["n", "2.5"]),
                ]),
            },
            // Empty value.
            TestCase {
                matchers: vec![LabelFilter::create(RegexEqual, "i", "c||d").unwrap()],
                exp: to_label_vec(&[
                    labels_from_strings(&["n", "1"]),
                    labels_from_strings(&["n", "2"]),
                    labels_from_strings(&["n", "2.5"]),
                ]),
            },
            TestCase {
                matchers: vec![LabelFilter::create(RegexEqual, "i", "(c||d)").unwrap()],
                exp: to_label_vec(&[
                    labels_from_strings(&["n", "1"]),
                    labels_from_strings(&["n", "2"]),
                    labels_from_strings(&["n", "2.5"]),
                ]),
            },
            // Test shortcut for i=~".*"
            TestCase {
                matchers: vec![LabelFilter::create(RegexEqual, "i", ".*").unwrap()],
                exp: to_label_vec(&[
                    labels_from_strings(&["n", "1"]),
                    labels_from_strings(&["n", "1", "i", "a"]),
                    labels_from_strings(&["n", "1", "i", "b"]),
                    labels_from_strings(&["n", "1", "i", "\n"]),
                    labels_from_strings(&["n", "2"]),
                    labels_from_strings(&["n", "2.5"]),
                ]),
            },
            // Test shortcut for n=~".*" and i=~"^.*$"
            TestCase {
                matchers: vec![
                    LabelFilter::create(RegexEqual, "n", ".*").unwrap(),
                    LabelFilter::create(RegexEqual, "i", "^.*$").unwrap(),
                ],
                exp: to_label_vec(&[
                    labels_from_strings(&["n", "1"]),
                    labels_from_strings(&["n", "1", "i", "a"]),
                    labels_from_strings(&["n", "1", "i", "b"]),
                    labels_from_strings(&["n", "1", "i", "\n"]),
                    labels_from_strings(&["n", "2"]),
                    labels_from_strings(&["n", "2.5"]),
                ]),
            },
            // Test shortcut for n=~"^.*$"
            TestCase {
                matchers: vec![
                    LabelFilter::create(RegexEqual, "n", "^.*$").unwrap(),
                    LabelFilter::create(Equal, "i", "a").unwrap(),
                ],
                exp: vec![labels_from_strings(&["n", "1", "i", "a"])],
            },
            // Test shortcut for i!~".*"
            TestCase {
                matchers: vec![LabelFilter::create(RegexNotEqual, "i", ".*").unwrap()],
                exp: vec![],
            },
            // Test shortcut for n!~"^.*$",  i!~".*". First one triggers empty result.
            TestCase {
                matchers: vec![
                    LabelFilter::create(RegexNotEqual, "n", "^.*$").unwrap(),
                    LabelFilter::create(RegexNotEqual, "i", ".*").unwrap(),
                ],
                exp: vec![],
            },
            // Test shortcut i!~".*"
            TestCase {
                matchers: vec![
                    LabelFilter::create(RegexEqual, "n", ".*").unwrap(),
                    LabelFilter::create(RegexNotEqual, "i", ".*").unwrap(),
                ],
                exp: vec![],
            },
            // Test shortcut i!~".+"
            TestCase {
                matchers: vec![
                    LabelFilter::create(RegexEqual, "n", ".*").unwrap(),
                    LabelFilter::create(RegexNotEqual, "i", ".+").unwrap(),
                ],
                exp: to_label_vec(&[
                    labels_from_strings(&["n", "1"]),
                    labels_from_strings(&["n", "2"]),
                    labels_from_strings(&["n", "2.5"]),
                ]),
            },
            // Test shortcut i!~"^.*$"
            TestCase {
                matchers: vec![
                    LabelFilter::create(Equal, "n", "1").unwrap(),
                    LabelFilter::create(RegexNotEqual, "i", "^.*$").unwrap(),
                ],
                exp: vec![],
            },
        ];

        let mut exp: HashSet<String> = HashSet::new();

        for case in cases {
            let name = case
                .matchers
                .iter()
                .map(|matcher| matcher.to_string())
                .collect::<Vec<_>>()
                .join(", ");

            let name = format!("{{{name}}}");
            exp.clear();

            let mut case = case;
            case.exp.sort();

            for labels in case.exp {
                let val = label_vec_to_string(&labels);
                exp.insert(val);
            }

            let actual = get_labels_by_filters(&ix, &case.matchers, &series_data);

            for labels in actual {
                let current = label_vec_to_string(&labels);
                let found = exp.remove(&current);
                assert!(found, "Evaluating {name}\n unexpected result {current}");
            }

            assert!(
                exp.is_empty(),
                "Evaluating {name}\nextra result(s): {exp:?}"
            );
        }
    }

    fn create_series_from_metric_name(prometheus_name: &str) -> TimeSeries {
        let mut ts = TimeSeries::new();
        ts.id = next_timeseries_id();

        ts.labels = prometheus_name.parse().unwrap();
        ts
    }

    #[test]
    fn test_postings_for_or_selectors() {
        use MatchOp::*;

        let mut ix: TimeSeriesIndex = TimeSeriesIndex::default();
        let mut labels_map: HashMap<SeriesRef, Vec<Label>> = HashMap::new();

        fn parse_metric(metric_name: &str) -> MetricName {
            metric_name.parse().unwrap()
        }

        let series_data = HashMap::from([
            (
                1,
                parse_metric(r#"http_requests{status="200", method="GET"}"#),
            ),
            (
                2,
                parse_metric(r#"http_requests{status="200", method="POST"}"#),
            ),
            (
                3,
                parse_metric(r#"http_requests{status="404", method="GET"}"#),
            ),
            (
                4,
                parse_metric(r#"http_requests{status="500", method="POST"}"#),
            ),
            (5, parse_metric(r#"cpu_usage{host="server1", env="prod"}"#)),
            (6, parse_metric(r#"cpu_usage{host="server2", env="prod"}"#)),
            (
                7,
                parse_metric(r#"memory_usage{host="server1", env="staging"}"#),
            ),
            (
                8,
                parse_metric(r#"memory_usage{host="server2", env="staging"}"#),
            ),
        ]);

        for (&series_ref, metric) in series_data.iter() {
            let labels = metric.to_label_vec();
            add_series(&mut ix, &mut labels_map, series_ref, &labels);
        }

        struct TestCase {
            name: &'static str,
            or_matchers: Vec<Vec<LabelFilter>>,
            exp: &'static [&'static str],
        }

        let cases = vec![
            // Simple OR with two branches - match different status codes
            TestCase {
                name: "OR matching status 200 or 404",
                or_matchers: vec![
                    vec![LabelFilter::create(Equal, "status", "200").unwrap()],
                    vec![LabelFilter::create(Equal, "status", "404").unwrap()],
                ],
                exp: &[
                    r#"http_requests{status="200", method="GET"}"#,
                    r#"http_requests{status="200", method="POST"}"#,
                    r#"http_requests{status="404", method="GET"}"#,
                ],
            },
            // OR with three branches - match different metric names
            TestCase {
                name: "OR matching multiple metric names",
                or_matchers: vec![
                    vec![LabelFilter::create(Equal, "__name__", "http_requests").unwrap()],
                    vec![LabelFilter::create(Equal, "__name__", "cpu_usage").unwrap()],
                    vec![LabelFilter::create(Equal, "__name__", "memory_usage").unwrap()],
                ],
                exp: &[
                    r#"http_requests{status="200", method="GET"}"#,
                    r#"http_requests{status="200", method="POST"}"#,
                    r#"http_requests{status="404", method="GET"}"#,
                    r#"http_requests{status="500", method="POST"}"#,
                    r#"cpu_usage{host="server1", env="prod"}"#,
                    r#"cpu_usage{host="server2", env="prod"}"#,
                    r#"memory_usage{host="server1", env="staging"}"#,
                    r#"memory_usage{host="server2", env="staging"}"#,
                ],
            },
            // OR with multiple matchers per branch (AND within OR)
            TestCase {
                name: "OR with AND conditions - specific host and environment combinations",
                or_matchers: vec![
                    vec![
                        LabelFilter::create(Equal, "host", "server1").unwrap(),
                        LabelFilter::create(Equal, "env", "prod").unwrap(),
                    ],
                    vec![
                        LabelFilter::create(Equal, "host", "server2").unwrap(),
                        LabelFilter::create(Equal, "env", "staging").unwrap(),
                    ],
                ],
                exp: &[
                    r#"cpu_usage{host="server1", env="prod"}"#,
                    r#"memory_usage{host="server2", env="staging"}"#,
                ],
            },
            // OR with regex matchers
            TestCase {
                name: "OR with regex matchers for HTTP methods",
                or_matchers: vec![
                    vec![LabelFilter::create(RegexEqual, "method", "^GET$").unwrap()],
                    vec![LabelFilter::create(RegexEqual, "method", "^POST$").unwrap()],
                ],
                exp: &[
                    r#"http_requests{status="200", method="GET"}"#,
                    r#"http_requests{status="200", method="POST"}"#,
                    r#"http_requests{status="404", method="GET"}"#,
                    r#"http_requests{status="500", method="POST"}"#,
                ],
            },
            // OR with not-equal matchers
            TestCase {
                name: "OR with not-equal matchers for status codes",
                or_matchers: vec![
                    vec![
                        LabelFilter::create(Equal, "__name__", "http_requests").unwrap(),
                        LabelFilter::create(NotEqual, "status", "200").unwrap(),
                    ],
                    vec![LabelFilter::create(Equal, "env", "prod").unwrap()],
                ],
                exp: &[
                    r#"http_requests{status="404",method="GET"}"#,
                    r#"http_requests{status="500",method="POST"}"#,
                    r#"cpu_usage{host="server1",env="prod"}"#,
                    r#"cpu_usage{host="server2",env="prod"}"#,
                ],
            },
            // OR with an empty result from one branch
            TestCase {
                name: "OR with one empty branch and one valid branch",
                or_matchers: vec![
                    vec![LabelFilter::create(Equal, "status", "999").unwrap()],
                    vec![LabelFilter::create(Equal, "env", "prod").unwrap()],
                ],
                exp: &[
                    r#"cpu_usage{host="server1",env="prod"}"#,
                    r#"cpu_usage{host="server2",env="prod"}"#,
                ],
            },
            // OR with all empty branches
            TestCase {
                name: "OR with all empty branches - no matches",
                or_matchers: vec![
                    vec![LabelFilter::create(Equal, "status", "999").unwrap()],
                    vec![LabelFilter::create(Equal, "env", "development").unwrap()],
                ],
                exp: &[],
            },
            // OR with overlapping results
            TestCase {
                name: "OR with overlapping conditions",
                or_matchers: vec![
                    vec![LabelFilter::create(Equal, "__name__", "http_requests").unwrap()],
                    vec![
                        LabelFilter::create(Equal, "__name__", "http_requests").unwrap(),
                        LabelFilter::create(Equal, "status", "200").unwrap(),
                    ],
                ],
                exp: &[
                    r#"http_requests{status="200",method="GET"}"#,
                    r#"http_requests{status="200",method="POST"}"#,
                    r#"http_requests{status="404",method="GET"}"#,
                    r#"http_requests{status="500",method="POST"}"#,
                ],
            },
            // OR with regex not-equal
            TestCase {
                name: "OR with regex not-equal for methods",
                or_matchers: vec![
                    vec![LabelFilter::create(RegexNotEqual, "method", "^GET$").unwrap()],
                    vec![LabelFilter::create(Equal, "env", "staging").unwrap()],
                ],
                exp: &[
                    r#"http_requests{status="200",method="POST"}"#,
                    r#"http_requests{status="500",method="POST"}"#,
                    r#"cpu_usage{host="server1",env="prod"}"#,
                    r#"cpu_usage{host="server2",env="prod"}"#,
                    r#"memory_usage{host="server1",env="staging"}"#,
                    r#"memory_usage{host="server2",env="staging"}"#,
                ],
            },
            // OR with complex regex patterns
            TestCase {
                name: "OR with complex regex patterns",
                or_matchers: vec![
                    vec![LabelFilter::create(RegexEqual, "status", "^[24]\\d{2}$").unwrap()],
                    vec![LabelFilter::create(RegexEqual, "host", "^server[12]$").unwrap()],
                ],
                exp: &[
                    r#"http_requests{status="200",method="GET"}"#,
                    r#"http_requests{status="200",method="POST"}"#,
                    r#"http_requests{status="404",method="GET"}"#,
                    r#"cpu_usage{host="server1",env="prod"}"#,
                    r#"cpu_usage{host="server2",env="prod"}"#,
                    r#"memory_usage{host="server1",env="staging"}"#,
                    r#"memory_usage{host="server2",env="staging"}"#,
                ],
            },
            // OR with missing labels
            TestCase {
                name: "OR with missing label checks",
                or_matchers: vec![
                    vec![LabelFilter::create(Equal, "nonexistent_label", "").unwrap()],
                    vec![LabelFilter::create(Equal, "env", "prod").unwrap()],
                ],
                exp: &[
                    r#"http_requests{status="200",method="GET"}"#,
                    r#"http_requests{status="200",method="POST"}"#,
                    r#"http_requests{status="404",method="GET"}"#,
                    r#"http_requests{status="500",method="POST"}"#,
                    r#"cpu_usage{host="server1",env="prod"}"#,
                    r#"cpu_usage{host="server2",env="prod"}"#,
                    r#"memory_usage{host="server1",env="staging"}"#,
                    r#"memory_usage{host="server2",env="staging"}"#,
                ],
            },
            // OR with a combination of positive and negative matchers
            TestCase {
                name: "OR mixing positive and negative matchers",
                or_matchers: vec![
                    vec![
                        LabelFilter::create(Equal, "__name__", "http_requests").unwrap(),
                        LabelFilter::create(RegexEqual, "method", "^GET|POST$").unwrap(),
                    ],
                    vec![
                        LabelFilter::create(Equal, "env", "staging").unwrap(),
                        LabelFilter::create(NotEqual, "host", "server2").unwrap(),
                    ],
                ],
                exp: &[
                    r#"http_requests{status="200",method="GET"}"#,
                    r#"http_requests{status="200",method="POST"}"#,
                    r#"http_requests{status="404",method="GET"}"#,
                    r#"http_requests{status="500",method="POST"}"#,
                    r#"memory_usage{host="server1",env="staging"}"#,
                ],
            },
        ];

        for case in cases {
            let name = case.name;
            let filter: SeriesSelector = case.or_matchers.into();
            let actual = ix.postings_for_selector(&filter).unwrap();
            let actual_ids: HashSet<SeriesRef> = actual.iter().collect();

            let mut missing: Vec<MetricName> = vec![];
            let mut extra: Vec<MetricName> = vec![];

            let mut exp_ids: HashSet<SeriesRef> = HashSet::new();
            for &metric_name in case.exp {
                let metric = parse_metric(metric_name);

                if let Some(id) = series_data
                    .iter()
                    .find(|&(_, mn)| mn == &metric)
                    .map(|(id, _)| *id)
                {
                    exp_ids.insert(id);
                }
            }

            // find the difference between actual and expected
            for &id in exp_ids.difference(&actual_ids) {
                if let Some(metric) = series_data.get(&id) {
                    missing.push(metric.clone());
                }
            }

            // find items in actual but not in expected (extra items)
            for &id in actual_ids.difference(&exp_ids) {
                if let Some(metric) = series_data.get(&id) {
                    extra.push(metric.clone());
                }
            }

            if !missing.is_empty() {
                let expected = case.exp.join(",\n");
                let missing = missing
                    .iter()
                    .map(|l| l.to_string())
                    .collect::<Vec<_>>()
                    .join(",\n");

                let matcher = filter.to_string();

                panic!(
                    "Case '{name}': \nMatcher: {matcher}\nExpected: {expected}\nMissing: {missing}",
                );
            }

            if !extra.is_empty() {
                let extra = extra
                    .iter()
                    .map(|l| l.to_string())
                    .collect::<Vec<_>>()
                    .join(",\n");

                let matcher = filter.to_string();

                panic!(
                    "Test case '{name}': \nMatcher: {matcher}\nunexpected extra metrics found: {extra}",
                );
            }
        }
    }

    #[test]
    fn test_debug_or_host_regex() {
        use MatchOp::*;

        let mut ix: TimeSeriesIndex = TimeSeriesIndex::default();
        let mut labels_map: HashMap<SeriesRef, Vec<Label>> = HashMap::new();

        fn parse_metric(metric_name: &str) -> MetricName {
            metric_name.parse().unwrap()
        }

        let series_data = HashMap::from([
            (5, parse_metric(r#"cpu_usage{host="server1", env="prod"}"#)),
            (6, parse_metric(r#"cpu_usage{host="server2", env="prod"}"#)),
        ]);

        for (&series_ref, metric) in series_data.iter() {
            let labels = metric.to_label_vec();
            add_series(&mut ix, &mut labels_map, series_ref, &labels);
        }

        let or_matchers = vec![vec![
            LabelFilter::create(RegexEqual, "host", "^server[12]$").unwrap(),
        ]];
        let filter: SeriesSelector = or_matchers.into();
        let actual = ix.postings_for_selector(&filter).unwrap();

        assert!(actual.contains(5));
        assert!(actual.contains(6));
    }

    #[test]
    fn test_querying_after_reindex() {
        let index = TimeSeriesIndex::new();

        // Create a time series with specific labels
        let ts = create_series_from_metric_name(
            r#"cpu_usage{instance="server1",region="us-west-2",env="prod"}"#,
        );
        let old_key = b"ts:cpu_usage:old";

        // Index the series with the old key
        index.index_timeseries(&ts, old_key);

        // Verify we can query by labels before rename
        let labels = ts.labels.to_label_vec();
        let result_before = index.posting_by_labels(&labels).unwrap();
        assert_eq!(result_before, Some(ts.id));

        // Verify we can query by specific label combinations before rename
        let instance_labels = vec![Label {
            name: "instance".to_string(),
            value: "server1".to_string(),
        }];
        let postings_before = index.posting_by_labels(&instance_labels).unwrap();
        assert_eq!(postings_before, Some(ts.id));

        // Test querying with matchers before rename
        let mut state = ();
        index.with_postings(&mut state, |postings, _| {
            let instance_postings = postings.postings_for_label_value("instance", "server1");
            assert!(instance_postings.contains(ts.id));

            let region_postings = postings.postings_for_label_value("region", "us-west-2");
            assert!(region_postings.contains(ts.id));

            let env_postings = postings.postings_for_label_value("env", "prod");
            assert!(env_postings.contains(ts.id));
        });

        // Now rename the series
        let new_key = b"ts:cpu_usage:new";
        index.reindex_timeseries(&ts, old_key, new_key);

        // Verify the same queries still work after rename
        let result_after = index.posting_by_labels(&labels).unwrap();
        assert_eq!(
            result_after,
            Some(ts.id),
            "Should still find series by labels after rename"
        );

        // Verify querying by specific label combinations still works
        let postings_after = index.posting_by_labels(&instance_labels).unwrap();
        assert_eq!(
            postings_after,
            Some(ts.id),
            "Should still find series by instance label after rename"
        );

        // Test querying with matchers after rename
        index.with_postings(&mut state, |postings, _| {
            let instance_postings = postings.postings_for_label_value("instance", "server1");
            assert!(
                instance_postings.contains(ts.id),
                "Should find series by instance after rename"
            );

            let region_postings = postings.postings_for_label_value("region", "us-west-2");
            assert!(
                region_postings.contains(ts.id),
                "Should find series by region after rename"
            );

            let env_postings = postings.postings_for_label_value("env", "prod");
            assert!(
                env_postings.contains(ts.id),
                "Should find series by env after rename"
            );

            // Verify series ID maps to the new key
            let expected_new_key = new_key.to_vec().into_boxed_slice();
            assert_eq!(
                postings.id_to_key.get(&ts.id),
                Some(&expected_new_key),
                "Series ID should map to new key"
            );
        });

        // Test more queries after rename
        let matchers = vec![
            LabelFilter::create(MatchOp::Equal, "instance", "server1").unwrap(),
            LabelFilter::create(MatchOp::Equal, "region", "us-west-2").unwrap(),
        ];
        let filter = SeriesSelector::with_filters(matchers);

        let complex_query_result = index.postings_for_selector(&filter).unwrap();
        assert!(
            complex_query_result.contains(ts.id),
            "Query should still work after rename"
        );

        let regex_matchers = vec![
            LabelFilter::create(MatchOp::RegexEqual, "instance", "server.*").unwrap(),
            LabelFilter::create(MatchOp::Equal, "env", "prod").unwrap(),
        ];
        let regex_filter = SeriesSelector::with_filters(regex_matchers);

        let regex_query_result = index.postings_for_selector(&regex_filter).unwrap();

        assert!(
            regex_query_result.contains(ts.id),
            "Query should still work after rename"
        );

        // Verify that the new key is present in the index
        index.with_postings(&mut state, |postings, _| {
            // Verify that the series ID maps to the new key
            assert_eq!(
                postings.id_to_key.get(&ts.id),
                Some(&new_key.to_vec().into_boxed_slice()),
                "Series ID should map to new key"
            );
        });
    }

    #[test]
    fn test_regex_charclass_node12() {
        use MatchOp::*;

        let mut ix: TimeSeriesIndex = TimeSeriesIndex::default();
        let mut labels_map: HashMap<SeriesRef, Vec<Label>> = HashMap::new();

        let series_data = HashMap::from([
            (1, labels_from_strings(&["node", "node1"])),
            (2, labels_from_strings(&["node", "node2"])),
            (3, labels_from_strings(&["node", "node3"])),
        ]);

        for (series_ref, labels) in series_data.iter() {
            add_series(&mut ix, &mut labels_map, *series_ref, labels);
        }

        // Create a regex matcher using a character class and ensure it matches node1 and node2
        let lf = LabelFilter::create(RegexEqual, "node", "node[12]").unwrap();
        let actual = get_labels_by_filters(&ix, &[lf], &labels_map);

        // Extract the matched node values and assert the right ones matched
        let mut matched: Vec<String> = actual
            .iter()
            .map(|lbls| {
                lbls.iter()
                    .find(|l| l.name == "node")
                    .unwrap()
                    .value
                    .clone()
            })
            .collect();
        matched.sort();
        assert_eq!(matched, vec!["node1".to_string(), "node2".to_string()]);
    }

    #[test]
    fn test_regex_matchers_with_ordered_literal_hints() {
        use MatchOp::*;

        let mut ix: TimeSeriesIndex = TimeSeriesIndex::default();
        let mut labels_map: HashMap<SeriesRef, Vec<Label>> = HashMap::new();

        let series_data = HashMap::from([
            (
                1,
                labels_from_strings(&["instance", "server-east-db-primary-prod"]),
            ),
            (
                2,
                labels_from_strings(&["instance", "server-east-prod-primary-db"]),
            ),
            (3, labels_from_strings(&["instance", "foo-123-bar"])),
            (4, labels_from_strings(&["instance", "bar-123-foo"])),
        ]);

        for (series_ref, labels) in series_data.iter() {
            add_series(&mut ix, &mut labels_map, *series_ref, labels);
        }

        let prefixed = LabelFilter::create(RegexEqual, "instance", "^server.*db.*prod$").unwrap();
        let actual = get_labels_by_filters(&ix, &[prefixed], &labels_map);
        assert_eq!(actual.len(), 1);
        assert_eq!(
            actual[0]
                .iter()
                .find(|label| label.name == "instance")
                .map(|label| label.value.as_str()),
            Some("server-east-db-primary-prod")
        );

        let generic = LabelFilter::create(RegexEqual, "instance", ".*foo.*bar.*").unwrap();
        let actual = get_labels_by_filters(&ix, &[generic], &labels_map);
        assert_eq!(actual.len(), 1);
        assert_eq!(
            actual[0]
                .iter()
                .find(|label| label.name == "instance")
                .map(|label| label.value.as_str()),
            Some("foo-123-bar")
        );

        let prefixed_suffix = LabelFilter::create(RegexEqual, "instance", "^server.+db$").unwrap();
        let actual = get_labels_by_filters(&ix, &[prefixed_suffix], &labels_map);
        assert_eq!(actual.len(), 1);
        assert_eq!(
            actual[0]
                .iter()
                .find(|label| label.name == "instance")
                .map(|label| label.value.as_str()),
            Some("server-east-prod-primary-db")
        );

        let suffix_only = LabelFilter::create(RegexEqual, "instance", "^.*bar$").unwrap();
        let actual = get_labels_by_filters(&ix, &[suffix_only], &labels_map);
        assert_eq!(actual.len(), 1);
        assert_eq!(
            actual[0]
                .iter()
                .find(|label| label.name == "instance")
                .map(|label| label.value.as_str()),
            Some("foo-123-bar")
        );

        let contains_list =
            LabelFilter::create(RegexEqual, "instance", ".*(server|client).*").unwrap();
        let actual = get_labels_by_filters(&ix, &[contains_list], &labels_map);
        assert_eq!(actual.len(), 2);
        let mut matched: Vec<_> = actual
            .iter()
            .filter_map(|labels| {
                labels
                    .iter()
                    .find(|label| label.name == "instance")
                    .map(|label| label.value.clone())
            })
            .collect();
        matched.sort();
        assert_eq!(
            matched,
            vec![
                "server-east-db-primary-prod".to_string(),
                "server-east-prod-primary-db".to_string(),
            ]
        );
    }
}
