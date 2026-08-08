#[cfg(test)]
mod tests {
    use crate::common::constants::METRIC_NAME_LABEL;
    use crate::labels::Label;
    use crate::labels::filters::{LabelFilter, SeriesSelector};
    use crate::series::index::{PostingStat, TimeSeriesIndex, next_timeseries_id};
    use crate::series::time_series::TimeSeries;

    fn create_series_from_metric_name(prometheus_name: &str) -> TimeSeries {
        let mut ts = TimeSeries::new();
        ts.id = next_timeseries_id();
        ts.labels = prometheus_name.parse().unwrap();
        ts
    }

    #[test]
    fn test_index_time_series() {
        let index = TimeSeriesIndex::new();
        let ts = create_series_from_metric_name(r#"latency{region="us-east-1",env="qa"}"#);

        index.index_timeseries(&ts, b"time-series-1");

        assert_eq!(index.count(), 1);
        assert_eq!(index.label_count(), 3); // metric_name + region + env
    }

    #[test]
    fn test_reindex_time_series() {
        let index = TimeSeriesIndex::new();
        let mut ts = create_series_from_metric_name(r#"latency{region="us-east-1",env="qa"}"#);

        index.index_timeseries(&ts, b"time-series-1");

        ts.labels.add_label("service", "web");
        ts.labels.add_label("pod", "pod-1");

        index.reindex_timeseries(&ts, b"time-series-1");

        assert_eq!(index.count(), 1);
        assert_eq!(index.label_count(), 5); // metric_name + region + env
    }

    #[test]
    fn test_remove_time_series() {
        let index = TimeSeriesIndex::new();
        let ts = create_series_from_metric_name(r#"latency{region="us-east-1",env="qa"}"#);

        index.index_timeseries(&ts, b"time-series-1");
        assert_eq!(index.count(), 1);

        index.remove_timeseries(&ts);

        assert_eq!(index.count(), 0);
        assert_eq!(index.label_count(), 0);
    }

    #[test]
    fn test_get_id_by_name_and_labels() {
        let index = TimeSeriesIndex::new();
        let ts = create_series_from_metric_name(r#"latency{region="us-east-1",env="qa"}"#);

        index.index_timeseries(&ts, b"time-series-1");

        let labels = ts
            .labels
            .iter()
            .map(|x| Label::new(x.name, x.value))
            .collect::<Vec<_>>();
        let id = index
            .posting_by_labels(&labels)
            .unwrap()
            .unwrap_or_default();
        assert_eq!(id, ts.id);
    }

    #[test]
    fn test_get_cardinality_by_selectors_empty() {
        let index = TimeSeriesIndex::new();

        // When no selectors are provided, cardinality should be zero.
        let selectors: Vec<SeriesSelector> = Vec::new();
        let cardinality = index
            .get_cardinality_by_selectors(&selectors)
            .expect("call should succeed");

        assert_eq!(cardinality, 0);
    }

    #[test]
    fn test_get_cardinality_by_single_selector() {
        let index = TimeSeriesIndex::new();

        let ts = create_series_from_metric_name(r#"latency{region="us-east-1",env="qa"}"#);
        index.index_timeseries(&ts, b"time-series-1");

        // Build a selector that matches the inserted series.
        let selector = SeriesSelector::parse("region='us-east-1'").unwrap();
        let selectors = vec![selector];

        let cardinality = index
            .get_cardinality_by_selectors(&selectors)
            .expect("call should succeed");

        // We inserted exactly one matching series.
        assert_eq!(cardinality, 1);
    }

    #[test]
    fn test_get_cardinality_by_multiple_selectors_and_intersection() {
        let index = TimeSeriesIndex::new();

        // Two series that share some labels but differ on at least one.
        let ts1 =
            create_series_from_metric_name(r#"latency{region="us-east-1",env="qa",service="api"}"#);
        let ts2 = create_series_from_metric_name(
            r#"latency{region="us-east-1",env="prod",service="api"}"#,
        );

        index.index_timeseries(&ts1, b"time-series-1");
        index.index_timeseries(&ts2, b"time-series-2");

        // You should construct these selectors so that:
        //   selector_env_qa matches only ts1
        //   selector_region matches both ts1 and ts2
        //
        // After AND-ing them, only ts1 should remain, so the
        // resulting cardinality must be 1.
        let selector_env_qa = SeriesSelector::parse("env='qa'").unwrap();
        let selector_region = SeriesSelector::parse(r#"region="us-east-1""#).unwrap();

        let selectors = vec![selector_region, selector_env_qa];

        let cardinality = index
            .get_cardinality_by_selectors(&selectors)
            .expect("call should succeed");

        assert_eq!(
            cardinality, 1,
            "intersection of selectors should yield exactly one series"
        );
    }

    #[test]
    fn test_stats_empty_index() {
        let index = TimeSeriesIndex::new();

        let stats = index.stats("", 10);

        // No series present
        assert_eq!(stats.series_count, 0);
        // No labels present (metric name isn't counted when empty)
        assert_eq!(stats.label_count, 0);
        assert_eq!(stats.total_label_value_pairs, 0);

        // All top-N vectors should be empty
        assert!(stats.series_count_by_metric_name.is_empty());
        assert!(stats.series_count_by_label_name.is_empty());
        assert!(stats.series_count_by_label_value_pairs.is_empty());

        // For empty focus label (metric name) we expect the optional vector to be Some(empty vec)
        match stats.series_count_by_focus_label_value {
            Some(v) => assert!(v.is_empty()),
            None => panic!("expected Some(vec) for focus label when empty"),
        }
    }

    #[test]
    fn test_stats_single_series_basic_counts() {
        let index = TimeSeriesIndex::new();
        let ts = create_series_from_metric_name(r#"requests_total{region="us",host="A"}"#);

        index.index_timeseries(&ts, b"time-series-1");

        let stats = index.stats("region", 10);

        // One series indexed
        assert_eq!(stats.series_count, 1);

        // label_count includes metric name + the two other labels
        assert_eq!(stats.label_count, 3);

        // total label value pairs should include metric + two label values
        assert!(stats.total_label_value_pairs >= 2);

        // metric name counts should include our metric with count 1
        let mut found_metric = false;
        for stat in &stats.series_count_by_metric_name {
            if stat.name == "requests_total" {
                assert_eq!(stat.count, 1);
                found_metric = true;
            }
        }
        assert!(
            found_metric,
            "metric name not found in series_count_by_metric_name"
        );

        // label value pairs must include region=us and host=A
        let mut found_region = false;
        let mut found_host = false;
        for stat in &stats.series_count_by_label_value_pairs {
            if stat.name == "region=us" {
                assert_eq!(stat.count, 1);
                found_region = true;
            }
            if stat.name == "host=A" {
                assert_eq!(stat.count, 1);
                found_host = true;
            }
        }
        assert!(
            found_region && found_host,
            "expected region=us and host=A label value pairs"
        );
    }

    #[test]
    fn test_stats_multi_series_and_limit_behavior() {
        let index = TimeSeriesIndex::new();

        let s1 = create_series_from_metric_name(r#"latency{region="us",host="A"}"#);
        let s2 = create_series_from_metric_name(r#"latency{region="us",host="B"}"#);
        let s3 = create_series_from_metric_name(r#"latency{region="eu",host="A"}"#);

        index.index_timeseries(&s1, b"time-series-1");
        index.index_timeseries(&s2, b"time-series-2");
        index.index_timeseries(&s3, b"time-series-3");

        // With limit 2 we should get the top 2 metric names (by count)
        let stats_lim2 = index.stats("region", 2);
        assert_eq!(stats_lim2.series_count, 3);

        // There should be at most 2 entries in the focus list
        match stats_lim2.series_count_by_focus_label_value {
            Some(v) => {
                assert!(v.len() <= 2);
                // ensure "us" with count 2 is present
                let mut found_us = false;
                let mut found_eu = false;
                for st in v {
                    if st.name == "us" {
                        assert_eq!(st.count, 2);
                        found_us = true;
                    }
                    if st.name == "eu" {
                        assert_eq!(st.count, 1);
                        found_eu = true;
                    }
                }
                assert!(
                    found_us && found_eu,
                    "expected both 'us' and 'eu' in top-2 region results"
                );
            }
            None => panic!("expected Some(vec) for focus label results"),
        }

        // With limit 1 we should only get the top ("us", 2)
        let stats_lim1 = index.stats("region", 1);
        match stats_lim1.series_count_by_focus_label_value {
            Some(v) => {
                assert_eq!(v.len(), 1);
                assert_eq!(v[0].name, "us");
                assert_eq!(v[0].count, 2);
            }
            None => panic!("expected Some(vec) for focus label results"),
        }
    }

    #[test]
    fn test_stats_missing_label_and_zero_limit() {
        let index = TimeSeriesIndex::new();

        let s1 = create_series_from_metric_name(r#"m{a="1",b="x"}"#);
        let s2 = create_series_from_metric_name(r#"m{a="2",b="y"}"#);

        index.index_timeseries(&s1, b"t1");
        index.index_timeseries(&s2, b"t2");

        // Request stats for a label that doesn't exist
        let stats_missing = index.stats("does_not_exist", 10);
        assert_eq!(stats_missing.series_count, 2);

        // The focus vector should exist but be empty since the label is missing
        match stats_missing.series_count_by_focus_label_value {
            Some(v) => assert!(v.is_empty()),
            None => panic!("expected Some(vec) for focus label when label is missing"),
        }

        // Request with zero limit: top lists should be empty but counts preserved
        let stats_zero_limit = index.stats("a", 0);
        assert_eq!(stats_zero_limit.series_count, 2);
        assert!(stats_zero_limit.label_count >= 1);
        assert!(stats_zero_limit.series_count_by_metric_name.is_empty());
        assert!(stats_zero_limit.series_count_by_label_name.is_empty());
        assert!(
            stats_zero_limit
                .series_count_by_label_value_pairs
                .is_empty()
        );
        match stats_zero_limit.series_count_by_focus_label_value {
            Some(v) => assert!(v.is_empty()),
            None => panic!("expected Some(vec) for focus label even when limit is zero"),
        }
    }

    #[test]
    fn test_stats_large_limit() {
        let index = TimeSeriesIndex::new();

        let s1 = create_series_from_metric_name(r#"m{region="us"}"#);
        let s2 = create_series_from_metric_name(r#"m{region="eu"}"#);

        index.index_timeseries(&s1, b"s1");
        index.index_timeseries(&s2, b"s2");

        // Very large limit - should return all items
        let stats = index.stats("region", 10000);

        assert_eq!(stats.series_count, 2);

        match stats.series_count_by_focus_label_value {
            Some(values) => {
                assert_eq!(
                    values.len(),
                    2,
                    "should return both values despite large limit"
                );
            }
            None => panic!("expected focus label values"),
        }
    }

    #[test]
    fn test_stats_label_value_pair_counts() {
        let index = TimeSeriesIndex::new();

        let s1 = create_series_from_metric_name(r#"m{env="prod",dc="us-east"}"#);
        let s2 = create_series_from_metric_name(r#"m{env="prod",dc="us-west"}"#);
        let s3 = create_series_from_metric_name(r#"m{env="dev",dc="us-east"}"#);

        index.index_timeseries(&s1, b"s1");
        index.index_timeseries(&s2, b"s2");
        index.index_timeseries(&s3, b"s3");

        let stats = index.stats("", 10);

        // Check that env=prod appears twice
        let env_prod = stats
            .series_count_by_label_value_pairs
            .iter()
            .find(|s| s.name == "env=prod")
            .expect("env=prod should be present");
        assert_eq!(env_prod.count, 2);

        // Check that dc=us-east appears twice
        let dc_us_east = stats
            .series_count_by_label_value_pairs
            .iter()
            .find(|s| s.name == "dc=us-east")
            .expect("dc=us-east should be present");
        assert_eq!(dc_us_east.count, 2);

        // Check that env=dev appears once
        let env_dev = stats
            .series_count_by_label_value_pairs
            .iter()
            .find(|s| s.name == "env=dev")
            .expect("env=dev should be present");
        assert_eq!(env_dev.count, 1);
    }

    #[test]
    fn test_stats_complex_multi_label_scenario() {
        let index = TimeSeriesIndex::new();

        // Complex scenario with multiple labels and different cardinalities
        let series = [
            r#"http_requests{method="GET",status="200",region="us"}"#,
            r#"http_requests{method="GET",status="404",region="us"}"#,
            r#"http_requests{method="POST",status="200",region="us"}"#,
            r#"http_requests{method="POST",status="200",region="eu"}"#,
            r#"db_queries{method="SELECT",status="200",region="us"}"#,
        ];

        for (i, prom_name) in series.iter().enumerate() {
            let ts = create_series_from_metric_name(prom_name);
            index.index_timeseries(&ts, format!("s{}", i).as_bytes());
        }

        let stats = index.stats("method", 10);

        assert_eq!(stats.series_count, 5);

        // Verify total label value pairs
        assert!(stats.total_label_value_pairs > 0);

        // Check method focus label
        match stats.series_count_by_focus_label_value {
            Some(values) => {
                let get_count = values
                    .iter()
                    .find(|v| v.name == "GET")
                    .map(|v| v.count)
                    .unwrap_or(0);
                assert_eq!(get_count, 2, "GET method appears in 2 series");

                let post_count = values
                    .iter()
                    .find(|v| v.name == "POST")
                    .map(|v| v.count)
                    .unwrap_or(0);
                assert_eq!(post_count, 2, "POST method appears in 2 series");

                let select_count = values
                    .iter()
                    .find(|v| v.name == "SELECT")
                    .map(|v| v.count)
                    .unwrap_or(0);
                assert_eq!(select_count, 1, "SELECT method appears in 1 series");
            }
            None => panic!("expected focus label values for method"),
        }

        // Verify metric name counts
        let http_metric = stats
            .series_count_by_metric_name
            .iter()
            .find(|s| s.name == "http_requests")
            .expect("http_requests metric should be present");
        assert_eq!(http_metric.count, 4);

        let db_metric = stats
            .series_count_by_metric_name
            .iter()
            .find(|s| s.name == "db_queries")
            .expect("db_queries metric should be present");
        assert_eq!(db_metric.count, 1);
    }

    /// The index used by the `stats_filtered` tests: four `region="us"` series (three
    /// `http_requests`, one `db_queries`) plus one `region="eu"` series carrying a `tier` label
    /// that no other series has.
    fn filtered_stats_index() -> TimeSeriesIndex {
        let index = TimeSeriesIndex::new();

        let series = [
            r#"http_requests{method="GET",status="200",region="us"}"#,
            r#"http_requests{method="GET",status="404",region="us"}"#,
            r#"http_requests{method="POST",status="200",region="us"}"#,
            r#"http_requests{method="POST",status="200",region="eu",tier="edge"}"#,
            r#"db_queries{method="SELECT",status="200",region="us"}"#,
        ];

        for (i, prom_name) in series.iter().enumerate() {
            let ts = create_series_from_metric_name(prom_name);
            index.index_timeseries(&ts, format!("s{i}").as_bytes());
        }

        index
    }

    fn count_of(stats: &[PostingStat], name: &str) -> u64 {
        stats
            .iter()
            .find(|s| s.name == name)
            .map(|s| s.count)
            .unwrap_or(0)
    }

    #[test]
    fn test_stats_filtered_no_filters_matches_unfiltered() {
        let index = filtered_stats_index();

        let unfiltered = index.stats("method", 10);
        let filtered = index
            .stats_filtered(&[], "method", 10)
            .expect("call should succeed");

        assert_eq!(filtered.series_count, unfiltered.series_count);
        assert_eq!(filtered.label_count, unfiltered.label_count);
        assert_eq!(
            filtered.total_label_value_pairs,
            unfiltered.total_label_value_pairs
        );
        assert_eq!(
            filtered.series_count_by_label_value_pairs.len(),
            unfiltered.series_count_by_label_value_pairs.len()
        );
        assert_eq!(
            count_of(&filtered.series_count_by_metric_name, "http_requests"),
            4
        );
    }

    #[test]
    fn test_stats_filtered_counts_matching_series_only() {
        let index = filtered_stats_index();

        let filters = [LabelFilter::equals("region".to_string(), "us")];
        let stats = index
            .stats_filtered(&filters, "method", 10)
            .expect("call should succeed");

        // Four of the five series are in us-east; the eu one drops out of every count.
        assert_eq!(stats.series_count, 4);

        assert_eq!(
            count_of(&stats.series_count_by_metric_name, "http_requests"),
            3
        );
        assert_eq!(
            count_of(&stats.series_count_by_metric_name, "db_queries"),
            1
        );

        let focus = stats
            .series_count_by_focus_label_value
            .expect("expected focus label values for method");
        assert_eq!(count_of(&focus, "GET"), 2);
        assert_eq!(
            count_of(&focus, "POST"),
            1,
            "the POST/eu series is excluded"
        );
        assert_eq!(count_of(&focus, "SELECT"), 1);

        let pairs = &stats.series_count_by_label_value_pairs;
        assert_eq!(count_of(pairs, "region=us"), 4);
        assert_eq!(count_of(pairs, "status=200"), 3);
        assert_eq!(count_of(pairs, "status=404"), 1);
        assert_eq!(
            count_of(pairs, "region=eu"),
            0,
            "region=eu has no matching series and must not be reported"
        );
    }

    #[test]
    fn test_stats_filtered_omits_labels_of_non_matching_series() {
        let index = filtered_stats_index();

        let filters = [LabelFilter::equals("region".to_string(), "us")];
        let stats = index
            .stats_filtered(&filters, "", 10)
            .expect("call should succeed");

        // `tier` belongs to the eu series alone, so it contributes neither a label name, a
        // label-value pair, nor a bump to the totals.
        assert_eq!(count_of(&stats.series_count_by_label_name, "tier"), 0);
        assert_eq!(
            count_of(&stats.series_count_by_label_value_pairs, "tier=edge"),
            0
        );

        // __name__, method, status and region — but not tier.
        assert_eq!(stats.label_count, 4);
        // Two metric names, three methods, two statuses and one region.
        assert_eq!(stats.total_label_value_pairs, 8);
    }

    #[test]
    fn test_stats_filtered_no_matching_series() {
        let index = filtered_stats_index();

        let filters = [LabelFilter::equals("region".to_string(), "apac")];
        let stats = index
            .stats_filtered(&filters, "method", 10)
            .expect("call should succeed");

        assert_eq!(stats.series_count, 0);
        assert_eq!(stats.label_count, 0);
        assert_eq!(stats.total_label_value_pairs, 0);
        assert!(stats.series_count_by_metric_name.is_empty());
        assert!(stats.series_count_by_label_name.is_empty());
        assert!(stats.series_count_by_label_value_pairs.is_empty());
        match stats.series_count_by_focus_label_value {
            Some(v) => assert!(v.is_empty()),
            None => panic!("expected Some(vec) for focus label when nothing matches"),
        }
    }

    #[test]
    fn test_stats_filtered_intersects_multiple_filters() {
        let index = filtered_stats_index();

        // AND-ed together: us-east series that are not 404s.
        let filters = [
            LabelFilter::equals("region".to_string(), "us"),
            LabelFilter::not_equals("status".to_string(), "404"),
        ];
        let stats = index
            .stats_filtered(&filters, METRIC_NAME_LABEL, 10)
            .expect("call should succeed");

        assert_eq!(stats.series_count, 3);

        let focus = stats
            .series_count_by_focus_label_value
            .expect("expected focus label values for the metric name");
        assert_eq!(count_of(&focus, "http_requests"), 2);
        assert_eq!(count_of(&focus, "db_queries"), 1);

        assert_eq!(
            count_of(&stats.series_count_by_label_value_pairs, "status=404"),
            0
        );
    }
}
