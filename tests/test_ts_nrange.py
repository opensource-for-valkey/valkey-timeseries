import math

import pytest
from valkey import ResponseError
from valkeytestframework.conftest import resource_port_tracker

from valkey_timeseries_test_case import ValkeyTimeSeriesTestCaseBase


def rows(result):
    """Normalize a TS.NRANGE reply to [(timestamp, [value|None, ...])].

    Values arrive as bulk strings under RESP2 and doubles under RESP3; NaN — which the
    command uses for "this key had nothing here" — becomes None so rows compare by value.
    """
    normalized = []
    for entry in result:
        assert len(entry) == 2, f"expected [timestamp, [values]], got {entry}"
        timestamp, values = entry
        parsed = []
        for value in values:
            value = float(value)
            parsed.append(None if math.isnan(value) else value)
        normalized.append((timestamp, parsed))
    return normalized


class TestTimeSeriesNRange(ValkeyTimeSeriesTestCaseBase):

    def setup_data(self):
        """Two partially overlapping series, plus two dense ones for aggregation."""
        for key in ('s1', 's2', 's3', 's4'):
            self.client.execute_command('TS.CREATE', key)

        self.client.execute_command('TS.MADD', 's1', 1000, 10, 's1', 2000, 12)
        self.client.execute_command('TS.MADD', 's2', 1000, 20, 's2', 3000, 25)

        self.client.execute_command('TS.MADD', 's3', 1000, 10, 's3', 1100, 20, 's3', 2000, 30)
        self.client.execute_command('TS.MADD', 's4', 1000, 5, 's4', 1100, 15, 's4', 2000, 25)

    def test_nrange_pivots_raw_samples(self):
        """One row per distinct timestamp, one column per key, NaN where a key has no sample."""

        self.setup_data()

        result = self.client.execute_command('TS.NRANGE', 2, 's1', 's2', '-', '+')

        assert rows(result) == [
            (1000, [10.0, 20.0]),
            (2000, [12.0, None]),
            (3000, [None, 25.0]),
        ]

    def test_nrange_single_key(self):
        """A one-key query is a TS.RANGE with the values nested one level deeper."""

        self.setup_data()

        result = self.client.execute_command('TS.NRANGE', 1, 's1', '-', '+')
        assert rows(result) == [(1000, [10.0]), (2000, [12.0])]

    def test_nrange_column_order_follows_key_order(self):
        """Columns are positional: reversing the keys reverses the values."""

        self.setup_data()

        forward = rows(self.client.execute_command('TS.NRANGE', 2, 's1', 's2', '-', '+'))
        reversed_keys = rows(self.client.execute_command('TS.NRANGE', 2, 's2', 's1', '-', '+'))

        assert forward == [(1000, [10.0, 20.0]), (2000, [12.0, None]), (3000, [None, 25.0])]
        assert reversed_keys == [(1000, [20.0, 10.0]), (2000, [None, 12.0]), (3000, [25.0, None])]

    def test_nrange_duplicate_keys_get_their_own_column(self):
        """A key listed twice contributes two columns, as documented."""

        self.setup_data()

        result = self.client.execute_command('TS.NRANGE', 3, 's1', 's2', 's1', '-', '+')

        assert rows(result) == [
            (1000, [10.0, 20.0, 10.0]),
            (2000, [12.0, None, 12.0]),
            (3000, [None, 25.0, None]),
        ]

    def test_nrange_explicit_range_bounds(self):
        """fromTimestamp/toTimestamp are inclusive and bound the join, not just each key."""

        self.setup_data()

        result = self.client.execute_command('TS.NRANGE', 2, 's1', 's2', 1000, 2000)
        assert rows(result) == [(1000, [10.0, 20.0]), (2000, [12.0, None])]

        result = self.client.execute_command('TS.NRANGE', 2, 's1', 's2', 2001, 5000)
        assert rows(result) == [(3000, [None, 25.0])]

    def test_nrange_empty_reply_when_nothing_matches(self):
        """No samples in range is an empty array, not a row of NaN."""

        self.setup_data()

        result = self.client.execute_command('TS.NRANGE', 2, 's1', 's2', 100000, 200000)
        assert result == []

    def test_nrange_count_applies_to_joined_rows(self):
        """COUNT limits whole rows, keeping the lowest timestamps in forward order."""

        self.setup_data()

        result = self.client.execute_command('TS.NRANGE', 2, 's1', 's2', '-', '+', 'COUNT', 2)
        assert rows(result) == [(1000, [10.0, 20.0]), (2000, [12.0, None])]

        result = self.client.execute_command('TS.NRANGE', 2, 's1', 's2', '-', '+', 'COUNT', 1)
        assert rows(result) == [(1000, [10.0, 20.0])]

    def test_nrange_filter_by_value_leaves_a_gap(self):
        """Filters run per key, before the join: a removed sample is NaN, not a dropped row."""

        self.setup_data()

        result = self.client.execute_command(
            'TS.NRANGE', 2, 's1', 's2', '-', '+', 'FILTER_BY_VALUE', 0, 15)

        assert rows(result) == [
            (1000, [10.0, None]),
            (2000, [12.0, None]),
        ]

    def test_nrange_filter_by_ts(self):
        """FILTER_BY_TS keeps only the listed timestamps, across every key."""

        self.setup_data()

        result = self.client.execute_command(
            'TS.NRANGE', 2, 's1', 's2', '-', '+', 'FILTER_BY_TS', 1000, 3000)

        assert rows(result) == [(1000, [10.0, 20.0]), (3000, [None, 25.0])]

    def test_nrange_aggregation_one_aggregator_per_key(self):
        """AGGREGATION takes one aggregator argument per key, in key order."""

        self.setup_data()

        result = self.client.execute_command(
            'TS.NRANGE', 2, 's3', 's4', '-', '+', 'AGGREGATION', 'avg', 'sum', 1000)

        assert rows(result) == [
            (1000, [15.0, 20.0]),
            (2000, [30.0, 25.0]),
        ]

    def test_nrange_aggregation_multiple_aggregators_for_one_key(self):
        """A comma-separated list gives a key one column per aggregator, kept together."""

        self.setup_data()

        result = self.client.execute_command(
            'TS.NRANGE', 2, 's3', 's4', '-', '+', 'AGGREGATION', 'avg,max', 'sum', 1000)

        assert rows(result) == [
            (1000, [15.0, 20.0, 20.0]),
            (2000, [30.0, 30.0, 25.0]),
        ]

    def test_nrange_aggregation_blanks_whole_key_block(self):
        """A key with no samples in a bucket reports NaN for every column it owns."""

        self.client.execute_command('TS.CREATE', 'dense')
        self.client.execute_command('TS.CREATE', 'sparse')
        self.client.execute_command('TS.MADD', 'dense', 1000, 1, 'dense', 2000, 2, 'dense', 3000, 3)
        self.client.execute_command('TS.MADD', 'sparse', 2000, 9)

        result = self.client.execute_command(
            'TS.NRANGE', 2, 'dense', 'sparse', '-', '+', 'AGGREGATION', 'min,max', 'sum', 1000)

        assert rows(result) == [
            (1000, [1.0, 1.0, None]),
            (2000, [2.0, 2.0, 9.0]),
            (3000, [3.0, 3.0, None]),
        ]

    def test_nrange_aggregation_extended_aggregators(self):
        """The module's extended aggregators are available per key, inline condition included."""

        self.client.execute_command('TS.CREATE', 'a')
        self.client.execute_command('TS.CREATE', 'b')
        self.client.execute_command('TS.MADD', 'a', 1000, 1, 'a', 1100, 7, 'a', 2000, 9)
        self.client.execute_command('TS.MADD', 'b', 1000, 4, 'b', 1100, 6, 'b', 2000, 2)

        result = self.client.execute_command(
            'TS.NRANGE', 2, 'a', 'b', '-', '+', 'AGGREGATION', 'countif(>5)', 'range', 1000)

        assert rows(result) == [
            (1000, [1.0, 2.0]),
            (2000, [1.0, 0.0]),
        ]

    def test_nrange_aggregation_bucket_timestamp_and_align(self):
        """BUCKETTIMESTAMP and ALIGN are shared by every key."""

        self.setup_data()

        result = self.client.execute_command(
            'TS.NRANGE', 2, 's3', 's4', 0, 5000,
            'AGGREGATION', 'avg', 'avg', 1000, 'BUCKETTIMESTAMP', 'end')
        assert [ts for ts, _ in rows(result)] == [2000, 3000]

        result = self.client.execute_command(
            'TS.NRANGE', 2, 's3', 's4', 500, 5000,
            'ALIGN', 'start', 'AGGREGATION', 'avg', 'avg', 1000)
        assert [ts for ts, _ in rows(result)] == [500, 1500]

    def test_nrange_aggregation_empty_buckets(self):
        """EMPTY reports gap buckets; keys with nothing there report NaN."""

        self.client.execute_command('TS.CREATE', 'gap1')
        self.client.execute_command('TS.CREATE', 'gap2')
        self.client.execute_command('TS.MADD', 'gap1', 1000, 1, 'gap1', 3000, 3)
        self.client.execute_command('TS.MADD', 'gap2', 1000, 10, 'gap2', 3000, 30)

        result = self.client.execute_command(
            'TS.NRANGE', 2, 'gap1', 'gap2', '-', '+', 'AGGREGATION', 'avg', 'avg', 1000, 'EMPTY')

        assert rows(result) == [
            (1000, [1.0, 10.0]),
            (2000, [None, None]),
            (3000, [3.0, 30.0]),
        ]

    def test_nrange_reports_stored_nan(self):
        """A stored NaN is reported like a missing sample — the two are indistinguishable."""

        self.client.execute_command('TS.CREATE', 'n1')
        self.client.execute_command('TS.CREATE', 'n2')
        self.client.execute_command('TS.ADD', 'n1', 1000, 'nan')
        self.client.execute_command('TS.ADD', 'n2', 1000, 5)

        result = self.client.execute_command('TS.NRANGE', 2, 'n1', 'n2', '-', '+')
        assert rows(result) == [(1000, [None, 5.0])]

    def test_nrange_latest_reports_open_compaction_bucket(self):
        """LATEST includes a compaction's still-open bucket, as it does for TS.RANGE."""

        self.client.execute_command('TS.CREATE', 'src')
        self.client.execute_command('TS.CREATE', 'compacted')
        self.client.execute_command('TS.CREATE', 'plain')
        self.client.execute_command('TS.CREATERULE', 'src', 'compacted', 'AGGREGATION', 'sum', 1000)

        # Two samples in the 1000-bucket (closed by the 2000 sample) and one in the open one.
        self.client.execute_command('TS.MADD', 'src', 1000, 1, 'src', 1500, 2, 'src', 2000, 4)
        self.client.execute_command('TS.MADD', 'plain', 1000, 7, 'plain', 2000, 8)

        without = rows(self.client.execute_command('TS.NRANGE', 2, 'compacted', 'plain', '-', '+'))
        assert without == [(1000, [3.0, 7.0]), (2000, [None, 8.0])]

        with_latest = rows(
            self.client.execute_command('TS.NRANGE', 2, 'compacted', 'plain', '-', '+', 'LATEST'))
        assert with_latest == [(1000, [3.0, 7.0]), (2000, [4.0, 8.0])]

    def test_nrange_errors(self):
        """Argument validation."""

        self.setup_data()

        # numkeys must be a positive integer
        for bad in ('0', '-1', 'abc'):
            with pytest.raises(ResponseError):
                self.client.execute_command('TS.NRANGE', bad, 's1', '-', '+')

        # numkeys larger than the key list swallows the range bounds
        with pytest.raises(ResponseError):
            self.client.execute_command('TS.NRANGE', 3, 's1', 's2', '-', '+')

        # missing key
        with pytest.raises(ResponseError):
            self.client.execute_command('TS.NRANGE', 2, 's1', 'nosuchkey', '-', '+')

        # wrong type
        self.client.execute_command('SET', 'astring', 'x')
        with pytest.raises(ResponseError):
            self.client.execute_command('TS.NRANGE', 2, 's1', 'astring', '-', '+')

        # arity
        with pytest.raises(ResponseError):
            self.client.execute_command('TS.NRANGE', 1, 's1', '-')

        # unknown option
        with pytest.raises(ResponseError):
            self.client.execute_command('TS.NRANGE', 1, 's1', '-', '+', 'BOGUS')

        # TS.MRANGE-only options are not accepted
        for option in ('WITHLABELS', 'EXCLUDEEMPTY'):
            with pytest.raises(ResponseError):
                self.client.execute_command('TS.NRANGE', 1, 's1', '-', '+', option)

    def test_nrange_aggregation_errors(self):
        """The aggregator list must have exactly one entry per key."""

        self.setup_data()

        # too few aggregators: the bucket duration lands in an aggregator slot
        with pytest.raises(ResponseError):
            self.client.execute_command(
                'TS.NRANGE', 2, 's3', 's4', '-', '+', 'AGGREGATION', 'avg', 1000)

        # too many aggregators: an aggregator lands in the bucket duration slot
        with pytest.raises(ResponseError):
            self.client.execute_command(
                'TS.NRANGE', 2, 's3', 's4', '-', '+', 'AGGREGATION', 'avg', 'sum', 'max', 1000)

        # unknown aggregation type
        with pytest.raises(ResponseError):
            self.client.execute_command(
                'TS.NRANGE', 2, 's3', 's4', '-', '+', 'AGGREGATION', 'avg', 'bogus', 1000)

        # twa is not supported by this module
        with pytest.raises(ResponseError):
            self.client.execute_command(
                'TS.NRANGE', 2, 's3', 's4', '-', '+', 'AGGREGATION', 'twa', 'avg', 1000)

        # a filtered aggregator without its inline condition
        with pytest.raises(ResponseError):
            self.client.execute_command(
                'TS.NRANGE', 2, 's3', 's4', '-', '+', 'AGGREGATION', 'countif', 'avg', 1000)

        # zero bucket duration
        with pytest.raises(ResponseError):
            self.client.execute_command(
                'TS.NRANGE', 2, 's3', 's4', '-', '+', 'AGGREGATION', 'avg', 'avg', 0)

        # ALIGN without AGGREGATION
        with pytest.raises(ResponseError):
            self.client.execute_command('TS.NRANGE', 1, 's3', '-', '+', 'ALIGN', 'start')

        # ALIGN start needs an explicit fromTimestamp
        with pytest.raises(ResponseError):
            self.client.execute_command(
                'TS.NRANGE', 1, 's3', '-', '+', 'ALIGN', 'start', 'AGGREGATION', 'avg', 1000)
