"""Time-weighted range aggregation, with values calculated by hand from straight lines."""
import math
import random

import pytest
from valkey import ResponseError
from valkeytestframework.conftest import resource_port_tracker

from valkey_timeseries_test_case import ValkeyTimeSeriesTestCaseBase, assert_rows


class TestTimeSeriesTwaRange(ValkeyTimeSeriesTestCaseBase):
    def series(self, key, points):
        self.client.execute_command("TS.CREATE", key)
        for timestamp, value in points:
            self.client.execute_command("TS.ADD", key, timestamp, value)

    def query(self, command, key, *args):
        return self.client.execute_command(command, key, *args)

    def test_a_raw_nan_closing_sample_stops_only_forward_interpolation(self):
        # [0, 10) holds 0 and 5; [10, 20) opens with NaN. Forwards the NaN closes the first
        # bucket, so it averages its own line from 0 to 5. Backwards the first bucket is
        # entered from 15, the next real sample: (12.5 + 37.5) / 10.
        self.series("twa", [(0, 0), (5, 5), (12, "NaN"), (15, 15)])
        assert_rows(self.query("TS.RANGE", "twa", 0, 20, "AGGREGATION", "twa", 10),
                    [(0, 2.5), (10, 12.5)])
        assert_rows(self.query("TS.REVRANGE", "twa", 0, 20, "AGGREGATION", "twa", 10),
                    [(10, 12.5), (0, 5)])

    def test_neighbors_outside_the_window_pass_the_query_filters(self):
        self.series("twa", [(0, 1), (10, 3), (20, 9)])
        window = ("TS.RANGE", "twa", 5, 15)
        aggregation = ("AGGREGATION", "twa", 10)
        # 0 and 20 bracket [10, 15]: the line from 3 reaches 6 at 15.
        assert_rows(self.query(*window, *aggregation), [(10, 4.5)])
        # A neighbor must be one of the FILTER_BY_TS timestamps, even outside the window.
        assert_rows(self.query(*window, "FILTER_BY_TS", 0, 10, 20, *aggregation), [(10, 4.5)])
        assert_rows(self.query(*window, "FILTER_BY_TS", 10, *aggregation), [(10, 3)])
        # ...and must pass FILTER_BY_VALUE, which drops 9.
        assert_rows(self.query(*window, "FILTER_BY_VALUE", 0, 5, *aggregation), [(10, 3)])

    def test_mixed_columns_read_only_the_window(self):
        self.series("twa", [(0, 1), (10, 3), (20, 9)])
        assert_rows(self.query("TS.RANGE", "twa", 5, 15, "FILTER_BY_TS", 0, 10, 20,
                               "AGGREGATION", "avg,twa,count", 10),
                    [(10, 3, 4.5, 1)])

    def test_count_limits_buckets_in_both_directions(self):
        self.series("twa", [(0, 0), (10, 10), (20, 20), (30, 30)])
        aggregation = ("AGGREGATION", "twa", 10)
        assert_rows(self.query("TS.RANGE", "twa", "-", "+", "COUNT", 2, *aggregation),
                    [(0, 5), (10, 15)])
        assert_rows(self.query("TS.REVRANGE", "twa", "-", "+", "COUNT", 2, *aggregation),
                    [(30, 30), (20, 25)])

    def test_empty_interpolates_gaps_and_reports_bucket_timestamps(self):
        self.series("twa", [(0, 0), (30, 30)])
        assert_rows(self.query("TS.RANGE", "twa", "-", "+", "AGGREGATION", "twa", 10, "EMPTY"),
                    [(0, 5), (10, 15), (20, 25), (30, 30)])
        assert_rows(self.query("TS.REVRANGE", "twa", "-", "+", "AGGREGATION", "twa", 10,
                               "BUCKETTIMESTAMP", "mid", "EMPTY", "COUNT", 2),
                    [(35, 30), (25, 25)])
        assert_rows(self.query("TS.RANGE", "twa", "-", "+", "ALIGN", 5,
                               "AGGREGATION", "twa", 10, "EMPTY"),
                    [(0, 2.5), (5, 10), (15, 20), (25, 27.5)])

    def test_nan_only_bucket_counts_with_countnan_and_reports_the_gap_value(self):
        self.series("twa", [(0, 0), (12, "NaN"), (25, 25)])
        assert_rows(self.query("TS.RANGE", "twa", 0, 30, "AGGREGATION", "countnan,twa", 10),
                    [(0, 0, 0), (10, 1, 15), (20, 0, 25)])
        assert_rows(self.query("TS.RANGE", "twa", 0, 30, "AGGREGATION", "twa", 10),
                    [(0, 0), (20, 25)])

    def test_latest_includes_the_open_compaction_bucket(self):
        self.series("src", [])
        self.client.execute_command("TS.CREATE", "dst")
        self.client.execute_command("TS.CREATERULE", "src", "dst", "AGGREGATION", "avg", 10)
        for timestamp in (2, 8, 12, 18, 22, 28):
            self.client.execute_command("TS.ADD", "src", timestamp, timestamp)
        # dst holds 5 and 15; LATEST adds the open bucket's 25.
        assert_rows(self.query("TS.RANGE", "dst", "-", "+", "AGGREGATION", "twa", 10),
                    [(0, 10), (10, 15)])
        assert_rows(self.query("TS.RANGE", "dst", "-", "+", "LATEST", "AGGREGATION", "twa", 10),
                    [(0, 10), (10, 20), (20, 25)])

    def test_twa_is_rejected_where_it_is_not_supported(self):
        self.series("src", [(0, 1)])
        self.client.execute_command("TS.CREATE", "dst", "LABELS", "a", "b")
        unknown_type = [
            ("TS.MRANGE", "-", "+", "FILTER", "a=b", "AGGREGATION", "twa", 10),
            ("TS.MRANGE", "-", "+", "AGGREGATION", "twa", 0, "FILTER", "a=b"),
            ("TS.MRANGE", "-", "+", "AGGREGATION", "twa>5", 10, "FILTER", "a=b"),
            ("TS.MRANGE", "-", "+", "AGGREGATION", "twa,twa", 10, "FILTER", "a=b"),
            ("TS.MRANGE", "-", "+", "AGGREGATION", "twa", "abc", "FILTER", "a=b"),
            ("TS.MREVRANGE", "-", "+", "FILTER", "a=b", "AGGREGATION", "avg,twa", 10),
            ("TS.MRANGE", "-", "+", "ALIGN", "start", "AGGREGATION", "twa", 10, "FILTER", "a=b"),
            ("TS.JOIN", "src", "dst", "-", "+", "AGGREGATION", "twa", 10),
            ("TS.JOIN", "src", "dst", "-", "+", "AGGREGATION", "twa", 0),
            ("TS.JOIN", "src", "dst", "-", "+", "AGGREGATION", "twa>5", 10),
            ("TS.JOIN", "src", "dst", "-", "+", "AGGREGATION", "twa,twa", 10),
            ("TS.JOIN", "src", "dst", "-", "+", "AGGREGATION", "twa", "abc"),
            ("TS.CREATERULE", "src", "dst", "AGGREGATION", "twa", 10),
            ("CONFIG", "SET", "ts.ts-compaction-policy", "twa:1h:1d"),
        ]
        for command in unknown_type:
            with pytest.raises(ResponseError) as error:
                self.client.execute_command(*command)
            assert "Unknown aggregation type" in str(error.value), command
        with pytest.raises(ResponseError):
            self.client.execute_command(
                "TS.MRANGE", "-", "+", "FILTER", "a=b", "GROUPBY", "a", "REDUCE", "twa")

    # LATEST brackets buckets but does not fill gaps or extend EMPTY rows. The replies are those
    # of RedisTimeSeries 8.10 for a compaction destination with an open bucket.
    LATEST_SCENARIOS = [
        ([(0, 5), (1, 5), (10, 15), (11, 15), (50, 25), (51, 25)], 10, [
            (("-", "+"), False, [(0, 10), (10, 16.25), (50, 25)]),
            (("-", "+"), True, [(0, 10), (10, 16.25), (20, math.nan), (30, math.nan), (40, math.nan),
                                (50, 25)]),
            ((0, 60), True, [(0, 10), (10, 16.25), (20, math.nan), (30, math.nan), (40, math.nan),
                             (50, 25)]),
            ((15, 45), False, []),
            ((15, 45), True, []),
        ]),
        ([(0, 1), (2, 9), (12, 4), (40, 8), (41, 8)], 3, [
            (("-", "+"), False, [(0, 4.875), (12, 4.222222222222222), (39, 8)]),
            (("-", "+"), True, [(0, 4.875), (3, 4.625), (6, 4.375), (9, 4.125),
                                (12, 4.222222222222222), (15, 4), (18, math.nan), (21, math.nan),
                                (24, math.nan), (27, math.nan), (30, math.nan), (33, math.nan),
                                (36, math.nan), (39, 8)]),
            ((15, 45), False, [(39, 8)]),
            ((15, 45), True, [(39, 8)]),
        ]),
    ]

    @pytest.mark.parametrize("scenario", range(2))
    def test_latest_brackets_buckets_without_filling_gaps(self, scenario):
        source, width, cases = self.LATEST_SCENARIOS[scenario]
        self.client.execute_command("TS.CREATE", "src")
        self.client.execute_command("TS.CREATE", "dst")
        self.client.execute_command("TS.CREATERULE", "src", "dst", "AGGREGATION", "avg", width)
        for timestamp, value in source:
            self.client.execute_command("TS.ADD", "src", timestamp, value)
        for bounds, empty, expected in cases:
            tail = ("LATEST", "AGGREGATION", "twa", width, *(("EMPTY",) if empty else ()))
            assert_rows(self.query("TS.RANGE", "dst", *bounds, *tail), expected)
            assert_rows(self.query("TS.REVRANGE", "dst", *bounds, *tail), expected[::-1])

    def test_last_beside_twa_follows_last_alone_with_filter_timestamps_outside(self):
        self.series("k", [(8, 12), (21, "NaN")])
        window = ("TS.RANGE", "k", 20, "+", "FILTER_BY_TS", 4, 5, 7, 8, 16, 19, 21)
        alone = self.query(*window, "AGGREGATION", "last", 5, "EMPTY")
        beside = self.query(*window, "AGGREGATION", "twa,last", 5, "EMPTY")
        shared = [(row[0], row[2]) for row in beside]
        assert shared == [(row[0], row[1]) for row in alone if row[0] in dict(shared)]

    SINGLE = ["avg", "sum", "min", "max", "count", "first", "last", "range", "countnan",
              "countall", "std.p", "std.s", "var.p", "var.s", "increase", "rate", "irate"]

    def random_series(self, rng, key):
        self.client.execute_command("TS.CREATE", key)
        timestamps = sorted(rng.sample(range(0, 160), rng.randint(0, 24)))
        for timestamp in timestamps:
            value = "NaN" if rng.random() < 0.25 else rng.choice([1, 2, 3.5, -4, 10, 7.25])
            self.client.execute_command("TS.ADD", key, timestamp, value)

    @staticmethod
    def by_timestamp(rows, width):
        columns = {}
        for row in rows:
            cells = [row[1]] if width == 1 else row[1:]
            columns[row[0]] = [float(cell).hex() for cell in cells]
        return columns

    def test_adding_twa_never_changes_another_column(self):
        rng = random.Random(20261116)
        checked = 0
        for round in range(120):
            key = f"r{round}"
            self.random_series(rng, key)
            columns = rng.sample(self.SINGLE, rng.randint(1, 3))
            options = []
            if rng.random() < 0.5:
                options += ["EMPTY"]
            window = rng.choice([("-", "+"), (rng.randint(0, 40), rng.randint(60, 200))])
            if rng.random() < 0.3:
                aligns = [0, 3, 7] if window[0] == "-" else [0, 3, 7, "start", "end"]
                options += ["ALIGN", rng.choice(aligns)]
            filters = []
            if rng.random() < 0.3:
                filters = ["FILTER_BY_TS", *rng.sample(range(0, 160), rng.randint(1, 30))]
            if rng.random() < 0.3:
                filters += ["COUNT", rng.randint(1, 8)]
            width = rng.choice([4, 10, 25])
            for command in ("TS.RANGE", "TS.REVRANGE"):
                def run(names):
                    return self.query(command, key, *window, *filters, "AGGREGATION",
                                      ",".join(names), width, *options)
                alone = self.by_timestamp(run(columns), len(columns))
                for beside_names, offset in ((columns + ["twa"], 0), (["twa"] + columns, 1)):
                    beside = self.by_timestamp(run(beside_names), len(beside_names))
                    for timestamp in alone.keys() & beside.keys():
                        got = beside[timestamp][offset:offset + len(columns)]
                        assert got == alone[timestamp], (
                            command, key, window, filters, beside_names, options, timestamp)
                        checked += 1
        assert checked > 500

    def test_count_returns_the_first_rows_of_the_uncounted_reverse_query(self):
        points = [(1, 2), (4, "NaN"), (5, "NaN"), (9, 6), (30, "NaN"), (31, 3), (58, 9),
                  (59, "NaN"), (60, "NaN"), (61, "NaN"), (90, 1), (140, 4), (141, "NaN")]
        self.series("a", points)
        self.series("b", [(3, 5), (50, "NaN"), (80, 2), (81, 8), (120, "NaN")])
        for empty in ((), ("EMPTY",)):
            for aggregation in ("twa", "twa,last"):
                tail = ("AGGREGATION", aggregation, 10, *empty)
                full = self.query("TS.REVRANGE", "a", "-", "+", *tail)
                assert len(full) > 3
                for count in (1, 2, 3, 5, 8, len(full) + 2):
                    assert self.query("TS.REVRANGE", "a", "-", "+", "COUNT", count,
                                      *tail) == full[:count], (aggregation, empty, count)
            tail = ("AGGREGATION", "twa", "twa", 10, *empty)
            full = self.query("TS.NREVRANGE", 2, "a", "b", "-", "+", *tail)
            assert len(full) > 3
            for count in (1, 2, 3, 5, 8, len(full) + 2):
                assert self.query("TS.NREVRANGE", 2, "a", "b", "-", "+", "COUNT", count,
                                  *tail) == full[:count], (empty, count)
