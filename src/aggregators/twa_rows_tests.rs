#[cfg(test)]
mod tests {
    use crate::aggregators::*;
    use crate::common::{MultiSample, Sample, Timestamp};
    use crate::series::request_types::AggregationOptions;
    use crate::tests::aggregation;
    use crate::tests::assert_rows_close as assert_rows;
    use crate::tests::generators::{DataGenerator, TimestampModel};

    const NAN: f64 = f64::NAN;

    fn timeline(points: &[(Timestamp, f64)]) -> impl Iterator<Item = Sample> + '_ {
        points.iter().map(|&(t, v)| Sample::new(t, v))
    }

    #[test]
    fn split_keeps_two_real_neighbors_a_side_and_the_window() {
        let points = [
            (1, 1.0),
            (2, 2.0),
            (3, NAN),
            (4, 4.0),
            (10, 10.0),
            (11, NAN),
            (12, 12.0),
            (13, 13.0),
            (14, 14.0),
        ];
        let (neighbors, window) = TwaNeighbors::split(timeline(&points), 5, 10);
        let pairs =
            |samples: &[Sample]| -> Vec<_> { samples.iter().map(|s| s.timestamp).collect() };
        assert_eq!(pairs(&neighbors.before), [2, 4]);
        assert_eq!(pairs(&neighbors.after), [12, 13]);
        assert_eq!(pairs(&window), [10]);
    }

    #[test]
    fn split_of_an_inverted_window_reads_a_sample_between_the_edges_on_both_sides() {
        let points = [(1, 1.0), (5, 5.0), (9, 9.0)];
        let (neighbors, window) = TwaNeighbors::split(timeline(&points), 8, 2);
        assert!(window.is_empty());
        assert_eq!(
            neighbors
                .before
                .iter()
                .map(|s| s.timestamp)
                .collect::<Vec<_>>(),
            [1, 5]
        );
        assert_eq!(
            neighbors
                .after
                .iter()
                .map(|s| s.timestamp)
                .collect::<Vec<_>>(),
            [5, 9]
        );
    }

    #[test]
    fn split_stops_reading_once_both_neighbors_are_known() {
        let read = std::cell::Cell::new(0);
        let samples = (0..1_000).map(|t| {
            read.set(read.get() + 1);
            Sample::new(t, t as f64)
        });
        let (neighbors, window) = TwaNeighbors::split(samples, 10, 20);
        assert_eq!(window.len(), 11);
        assert_eq!(neighbors.after.len(), 2);
        assert_eq!(read.get(), 23);
    }

    struct Query {
        start: Timestamp,
        end: Timestamp,
        width: u64,
        align: Timestamp,
        empty: bool,
        explicit_end: bool,
        columns: &'static [AggregationType],
        latest: Option<Sample>,
        empty_fill: EmptyFillBounds,
    }

    impl Query {
        fn new(start: Timestamp, end: Timestamp, width: u64) -> Self {
            Self {
                start,
                end,
                width,
                align: 0,
                empty: false,
                explicit_end: true,
                columns: &[AggregationType::Twa],
                latest: None,
                empty_fill: EmptyFillBounds::default(),
            }
        }

        /// Rows in time order for a series holding `samples`, which may lie outside the window.
        fn rows(&self, samples: &[(Timestamp, f64)], direction: ScanDirection) -> Vec<MultiSample> {
            self.iterator(samples, direction).collect()
        }

        fn iterator(
            &self,
            samples: &[(Timestamp, f64)],
            direction: ScanDirection,
        ) -> TwaRowIterator<std::vec::IntoIter<Sample>> {
            let samples: Vec<Sample> = samples.iter().map(|&(t, v)| Sample::new(t, v)).collect();
            let (neighbors, inside) = TwaNeighbors::split(samples, self.start, self.end);
            let options = AggregationOptions {
                alignment: BucketAlignment::Timestamp(self.align),
                ..aggregation(self.columns, self.width, self.empty)
            };
            let context = TwaContext {
                start: self.start,
                end: self.end,
                explicit_end: self.explicit_end,
                window_has_real: inside.iter().any(is_real),
                neighbors,
                direction,
                latest: self.latest,
                empty_fill: self.empty_fill,
            };
            TwaRowIterator::new(inside.into_iter(), &options, context)
        }

        fn twa(
            &self,
            samples: &[(Timestamp, f64)],
            direction: ScanDirection,
        ) -> Vec<(Timestamp, f64)> {
            let column = self
                .columns
                .iter()
                .position(|ty| *ty == AggregationType::Twa)
                .unwrap();
            self.rows(samples, direction)
                .into_iter()
                .map(|row| (row.timestamp, row.values[column]))
                .collect()
        }
    }

    use ScanDirection::{Forward, Reverse};

    #[test]
    fn a_raw_nan_closing_sample_stops_only_the_forward_segment() {
        // [0, 10) holds 0 and 5; [10, 20) opens with a NaN. Forwards, that NaN closes the
        // first bucket, so its line stops at 5: (0 + 5) / 2. Backwards, the first bucket is
        // entered from the second bucket's first real sample, 15: (12.5 + 37.5) / 10.
        let samples = [(0, 0.0), (5, 5.0), (12, NAN), (15, 15.0)];
        let query = Query::new(0, 20, 10);
        assert_rows(&query.twa(&samples, Forward), &[(0, 2.5), (10, 12.5)]);
        assert_rows(&query.twa(&samples, Reverse), &[(0, 5.0), (10, 12.5)]);
    }

    #[test]
    fn a_nan_only_bucket_stops_the_carry_but_gaps_interpolate_across_it() {
        let samples = [(0, 0.0), (12, NAN), (25, 25.0)];
        let mut query = Query::new(0, 30, 10);
        for direction in [Forward, Reverse] {
            assert_rows(&query.twa(&samples, direction), &[(0, 0.0), (20, 25.0)]);
        }
        // With EMPTY the NaN-only bucket reports the line from 0 to 25 over [10, 20].
        query.empty = true;
        for direction in [Forward, Reverse] {
            assert_rows(
                &query.twa(&samples, direction),
                &[(0, 0.0), (10, 15.0), (20, 25.0)],
            );
        }
    }

    #[test]
    fn neighbors_outside_the_window_bracket_its_edge_buckets() {
        // The window [5, 15] holds only 10; the samples at 0 and 20 bracket it. With EMPTY the
        // real sample before the window also opens a row for [5, 10).
        let samples = [(0, 0.0), (10, 10.0), (20, 20.0)];
        let mut query = Query::new(5, 15, 10);
        for direction in [Forward, Reverse] {
            assert_rows(&query.twa(&samples, direction), &[(10, 12.5)]);
        }
        query.empty = true;
        for direction in [Forward, Reverse] {
            assert_rows(&query.twa(&samples, direction), &[(0, 7.5), (10, 12.5)]);
        }
    }

    #[test]
    fn countnan_emits_nan_only_buckets_with_the_gap_value() {
        let mut query = Query::new(0, 30, 10);
        query.columns = &[AggregationType::CountNan, AggregationType::Twa];
        let rows: Vec<Vec<f64>> = query
            .rows(&[(0, 0.0), (12, NAN), (25, 25.0)], Forward)
            .into_iter()
            .map(|row| row.values.to_vec())
            .collect();
        assert_eq!(rows[0], vec![0.0, 0.0]);
        assert_eq!(rows[1], vec![1.0, 15.0]);
        assert_eq!(rows[2], vec![0.0, 25.0]);
    }

    #[test]
    fn one_sided_gaps_hold_the_edge_value_for_half_the_spacing() {
        // Nothing real follows 10, so the samples at 0 and 10 hold 8 until 10 + 10 / 2 = 15:
        // the gap starting at 12 gets 8, the one starting at 16 does not.
        let samples = [(0, 1.0), (10, 8.0), (100, NAN)];
        let mut query = Query::new(0, 100, 4);
        query.empty = true;
        let rows = query.twa(&samples, Forward);
        assert_rows(
            &rows[..5],
            &[(0, 2.4), (4, 5.2), (8, 7.3), (12, 8.0), (16, NAN)],
        );
    }

    #[test]
    fn empty_reaches_the_window_edges_only_toward_real_neighbors() {
        let mut query = Query::new(5, 45, 10);
        query.empty = true;
        let real_both = [(0, 0.0), (20, 20.0), (50, 50.0)];
        let timestamps: Vec<Timestamp> =
            query.twa(&real_both, Forward).iter().map(|r| r.0).collect();
        assert_eq!(timestamps, vec![0, 10, 20, 30, 40]);
        let nan_both = [(0, NAN), (20, 20.0), (50, NAN)];
        let timestamps: Vec<Timestamp> =
            query.twa(&nan_both, Forward).iter().map(|r| r.0).collect();
        assert_eq!(timestamps, vec![20]);
    }

    #[test]
    fn a_negative_first_bucket_follows_the_reference_extent() {
        // ALIGN 7 puts the first bucket at [-3, 7). Going backwards with an explicit end
        // inside it, the reference reports nothing; forwards it reports the bucket at 0.
        let samples = [(0, 1.0), (2, 4.0), (100, 5.0)];
        let mut query = Query::new(0, 2, 10);
        query.align = 7;
        query.empty = true;
        assert!(query.twa(&samples, Reverse).is_empty());
        assert_eq!(query.twa(&samples, Forward).len(), 1);
        // Leading EMPTY rows stop at the first sample when the window starts below zero.
        let mut query = Query::new(5, 60, 10);
        query.align = 7;
        query.empty = true;
        let samples = [(0, 1.0), (15, 4.0), (100, 5.0)];
        let timestamps: Vec<Timestamp> = query.twa(&samples, Forward).iter().map(|r| r.0).collect();
        assert_eq!(timestamps, vec![7, 17, 27, 37, 47, 57]);
    }

    #[test]
    fn a_straight_line_averages_to_its_midpoint_in_every_bucket() {
        let timestamps: Vec<Timestamp> = DataGenerator::builder()
            .start(1_000)
            .interval(std::time::Duration::from_millis(7))
            .samples(400)
            .timestamp_model(TimestampModel::Irregular)
            .seed(17)
            .build()
            .generate()
            .into_iter()
            .map(|sample| sample.timestamp)
            .collect();
        let (first, last) = (timestamps[0], *timestamps.last().unwrap());
        let line = |t: Timestamp| 0.25 * t as f64 - 40.0;
        let samples: Vec<(Timestamp, f64)> = timestamps.iter().map(|&t| (t, line(t))).collect();
        let query = Query::new(first, last, 50);
        for direction in [Forward, Reverse] {
            for (key, value) in query.twa(&samples, direction) {
                let (lo, hi) = (key.max(first), (key + 50).min(last));
                let midpoint = line(lo) / 2.0 + line(hi) / 2.0;
                assert!(
                    (value - midpoint).abs() <= 1e-9 * midpoint.abs().max(1.0),
                    "{direction:?} bucket {key}: {value} != {midpoint}"
                );
            }
        }
    }
    #[test]
    fn a_bucket_width_that_overflows_the_grid_still_consumes_its_samples() {
        // One bucket, [0, i64::MAX), holds both samples.
        let samples = [(1_731_628_800_000, 1.0), (1_731_628_800_010, 2.0)];
        let query = Query::new(0, Timestamp::MAX, Timestamp::MAX as u64);
        for direction in [Forward, Reverse] {
            assert_rows(&query.twa(&samples, direction), &[(0, 1.5)]);
        }
    }

    #[test]
    fn latest_brackets_buckets_but_neither_extends_rows_nor_supports_gaps() {
        // A compaction destination storing 5 at 0 and 15 at 10, with LATEST adding 25 at 20.
        let stored = [(0, 5.0), (10, 15.0)];
        let mut query = Query::new(11, 25, 5);
        query.empty = true;
        query.latest = Some(Sample::new(20, 25.0));
        assert_rows(&query.twa(&stored, Forward), &[(20, 25.0)]);
        let mut query = Query::new(0, 25, 3);
        query.empty = true;
        query.latest = Some(Sample::new(20, 25.0));
        let rows = query.twa(&stored, Forward);
        // [9, 12] closes on LATEST's 25: (14.5 + 32) / 3. The gaps after it ignore LATEST and
        // hold 15 for 10 / 2 = 5 ms past the sample at 10.
        assert_rows(&rows[3..6], &[(9, 15.5), (12, 15.0), (15, NAN)]);
    }

    #[test]
    fn a_zero_width_interval_reports_the_line_or_the_sample_at_it() {
        let p = Sample::new;
        assert_eq!(twa_at(5, Some(p(0, 0.0)), None, Some(p(10, 10.0))), 5.0);
        assert_eq!(twa_at(5, None, Some(p(5, 9.0)), None), 9.0);
        assert_eq!(
            twa_at(5, Some(p(0, 0.0)), Some(p(5, 9.0)), Some(p(10, 10.0))),
            9.0
        );
        assert!(twa_at(5, Some(p(0, 3.0)), None, None).is_nan());
        assert!(twa_at(5, None, None, Some(p(10, 3.0))).is_nan());
    }

    #[test]
    fn a_window_of_one_timestamp_reports_the_line_or_the_sample_at_it() {
        let line = [(0, 0.0), (10, 10.0)];
        let on_a_sample = [(0, 0.0), (5, 7.0), (10, 10.0)];
        let beside_nan = [(0, 0.0), (5, NAN), (10, 10.0)];
        let one_sided = [(0, 0.0), (5, 7.0)];
        for direction in [Forward, Reverse] {
            let query = Query::new(5, 5, 10);
            assert_rows(&query.twa(&line, direction), &[]);
            assert_rows(&query.twa(&on_a_sample, direction), &[(0, 7.0)]);
            assert_rows(&query.twa(&beside_nan, direction), &[]);
            assert_rows(&query.twa(&one_sided, direction), &[(0, 7.0)]);
            let mut empty = Query::new(5, 5, 10);
            empty.empty = true;
            assert_rows(&empty.twa(&line, direction), &[(0, 5.0)]);
            assert_rows(&empty.twa(&beside_nan, direction), &[(0, 5.0)]);
        }
    }

    #[test]
    fn reverse_scans_add_segments_newest_first() {
        // The reference sums this bucket from 34 back to 0, where the large values cancel to 0.
        let samples = [(0, -7e16), (6, 2.0), (22, 1.5e16), (34, 1.0)];
        let query = Query::new(0, 40, 50);
        assert_rows(&query.twa(&samples, Reverse), &[(0, 0.0)]);
    }
    #[test]
    fn an_inverted_window_averages_its_gap_from_end_to_start() {
        // [15, 12] selects nothing; the line y = 2t averages 27 over [12, 15].
        let mut query = Query::new(15, 12, 10);
        query.empty = true;
        for direction in [Forward, Reverse] {
            assert_rows(
                &query.twa(&[(0, 0.0), (30, 60.0)], direction),
                &[(10, 27.0)],
            );
        }
    }

    #[test]
    fn a_nan_only_bucket_changes_the_carry_even_when_its_row_is_omitted() {
        let samples = [(0, 0.0), (9, 9.0), (15, NAN), (21, 21.0), (29, 29.0)];
        let query = Query::new(0, 30, 10);
        assert_rows(&query.twa(&samples, Forward), &[(0, 4.5), (20, 25.0)]);
    }

    #[test]
    fn count_can_stop_a_billion_bucket_empty_run() {
        let mut query = Query::new(0, 10_000_000_000, 1);
        query.empty = true;
        let samples = [(0, 1.0), (10_000_000_000, 3.0)];
        let rows: Vec<MultiSample> = query.iterator(&samples, Forward).take(2).collect();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[1].timestamp, 1);
        assert!(rows[1].values[0] > 1.0);
    }

    #[test]
    fn a_tail_limit_skips_the_head_of_a_gap_run() {
        let mut query = Query::new(0, 1_000_000_000_000, 1);
        query.empty = true;
        query.empty_fill.tail_limit = Some(3);
        let samples = [(0, 1.0), (1_000_000_000_000, 3.0)];
        let timestamps: Vec<Timestamp> = query
            .rows(&samples, Reverse)
            .iter()
            .map(|row| row.timestamp)
            .collect();
        let end = 1_000_000_000_000;
        assert_eq!(timestamps, vec![0, end - 3, end - 2, end - 1, end]);
    }

    /// `last` beside `twa` must equal `last` in a list without it, for every row both produce.
    fn last_column(
        samples: &[(Timestamp, f64)],
        window: (Timestamp, Timestamp),
        reverse: bool,
        with_twa: bool,
    ) -> Vec<(Timestamp, f64)> {
        let samples = samples.iter().map(|&(t, v)| Sample::new(t, v));
        let (start, end) = window;
        let (neighbors, inside) = TwaNeighbors::split(samples, start, end);
        let (before, after) = (&neighbors.before, &neighbors.after);
        let width = 10;
        let first_bucket = bucket_start_for(start, 0, width);
        let seed = |until: Timestamp| {
            before
                .iter()
                .rev()
                .find(|s| s.timestamp < until)
                .map_or(NAN, |s| s.value)
        };
        let fill = EmptyFillBounds {
            start: (!before.is_empty()).then_some(start),
            end: (!after.is_empty()).then_some(end),
            carry_seed: seed(first_bucket),
            carry_lookback: seed(start),
            carry_reverse: reverse,
            tail_limit: None,
        };
        let options = |columns: &[AggregationType]| aggregation(columns, width, true);
        if !with_twa {
            let options = options(&[AggregationType::Last]);
            return crate::aggregators::MultiAggregateIterator::with_empty_fill(
                inside.into_iter(),
                &options,
                0,
                fill,
            )
            .map(|row| (row.timestamp, row.values[0]))
            .collect();
        }
        let context = TwaContext {
            start,
            end,
            explicit_end: true,
            window_has_real: inside.iter().any(is_real),
            neighbors,
            direction: ScanDirection::from_reverse(reverse),
            empty_fill: fill,
            ..Default::default()
        };
        let options = options(&[AggregationType::Last, AggregationType::Twa]);
        TwaRowIterator::new(inside.into_iter(), &options, context)
            .map(|row| (row.timestamp, row.values[0]))
            .collect()
    }

    fn same_bits(a: &[(Timestamp, f64)], b: &[(Timestamp, f64)]) -> bool {
        a.len() == b.len()
            && a.iter()
                .zip(b)
                .all(|(x, y)| x.0 == y.0 && x.1.to_bits() == y.1.to_bits())
    }

    #[test]
    fn last_beside_twa_matches_last_alone_for_every_nan_pattern() {
        let timestamps = [3, 14, 27, 31, 58, 77];
        let windows = [(0, 100), (25, 60), (29, 80)];
        for pattern in 0..3usize.pow(timestamps.len() as u32) {
            let samples: Vec<(Timestamp, f64)> = timestamps
                .iter()
                .enumerate()
                .filter_map(|(i, &t)| match pattern / 3usize.pow(i as u32) % 3 {
                    0 => None,
                    1 => Some((t, 10.0 + i as f64)),
                    _ => Some((t, NAN)),
                })
                .collect();
            for window in windows {
                for reverse in [false, true] {
                    let alone = last_column(&samples, window, reverse, false);
                    let beside = last_column(&samples, window, reverse, true);
                    let shared: Vec<(Timestamp, f64)> = beside
                        .iter()
                        .copied()
                        .filter(|row| alone.iter().any(|other| other.0 == row.0))
                        .collect();
                    let expected: Vec<(Timestamp, f64)> = alone
                        .iter()
                        .copied()
                        .filter(|row| beside.iter().any(|other| other.0 == row.0))
                        .collect();
                    assert!(
                        same_bits(&shared, &expected),
                        "{samples:?} window={window:?} reverse={reverse}\nalone={alone:?}\nbeside={beside:?}"
                    );
                }
            }
        }
    }

    #[test]
    fn a_leading_run_holds_the_last_reading_before_the_window() {
        let samples = [(3, 4.0), (14, 7.0), (58, 9.0)];
        for reverse in [false, true] {
            for with_twa in [false, true] {
                let rows = last_column(&samples, (25, 60), reverse, with_twa);
                let expected = [(20, 7.0), (30, 7.0), (40, 7.0), (50, 9.0)];
                assert!(
                    same_bits(&rows, &expected),
                    "reverse={reverse} twa={with_twa}: {rows:?}"
                );
            }
        }
    }
}
