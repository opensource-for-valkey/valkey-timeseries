import pytest
from valkey import ResponseError
from valkeytestframework.conftest import resource_port_tracker

from test_ts_nrange import rows
from valkey_timeseries_test_case import ValkeyTimeSeriesTestCaseBase


class TestTimeSeriesNRevRange(ValkeyTimeSeriesTestCaseBase):
    """TS.NREVRANGE is TS.NRANGE with the rows reversed.

    Only ordering-sensitive behavior is covered here; the shared surface (pivoting, per-key
    aggregation, filters, ACLs, key specs) is exercised in test_ts_nrange.py and runs through
    the same code path.
    """

    def setup_data(self):
        for key in ('s1', 's2', 's3', 's4'):
            self.client.execute_command('TS.CREATE', key)

        self.client.execute_command('TS.MADD', 's1', 1000, 10, 's1', 2000, 12)
        self.client.execute_command('TS.MADD', 's2', 1000, 20, 's2', 3000, 25)

        self.client.execute_command('TS.MADD', 's3', 1000, 10, 's3', 1100, 20, 's3', 2000, 30)
        self.client.execute_command('TS.MADD', 's4', 1000, 5, 's4', 1100, 15, 's4', 2000, 25)

    def test_nrevrange_reverses_the_rows(self):
        """Same rows as TS.NRANGE, highest timestamp first, columns untouched."""

        self.setup_data()

        result = self.client.execute_command('TS.NREVRANGE', 2, 's1', 's2', '-', '+')

        assert rows(result) == [
            (3000, [None, 25.0]),
            (2000, [12.0, None]),
            (1000, [10.0, 20.0]),
        ]

        forward = rows(self.client.execute_command('TS.NRANGE', 2, 's1', 's2', '-', '+'))
        assert rows(result) == list(reversed(forward))

    def test_nrevrange_count_keeps_the_highest_timestamps(self):
        """COUNT limits rows in the returned order, so it takes the newest rows here."""

        self.setup_data()

        result = self.client.execute_command('TS.NREVRANGE', 2, 's1', 's2', '-', '+', 'COUNT', 2)
        assert rows(result) == [(3000, [None, 25.0]), (2000, [12.0, None])]

        # The forward command's COUNT takes the other end of the same range.
        forward = self.client.execute_command('TS.NRANGE', 2, 's1', 's2', '-', '+', 'COUNT', 2)
        assert rows(forward) == [(1000, [10.0, 20.0]), (2000, [12.0, None])]

    def test_nrevrange_aggregation_buckets_are_reversed(self):
        """Under AGGREGATION, reverse applies to whole buckets, values in key order."""

        self.setup_data()

        result = self.client.execute_command(
            'TS.NREVRANGE', 2, 's3', 's4', '-', '+', 'AGGREGATION', 'avg,max', 'sum', 1000)

        assert rows(result) == [
            (2000, [30.0, 30.0, 25.0]),
            (1000, [15.0, 20.0, 20.0]),
        ]

    def test_nrevrange_first_and_last_stay_chronological(self):
        """`first`/`last` mean earliest/latest sample in the bucket regardless of direction."""

        self.client.execute_command('TS.CREATE', 'fl')
        self.client.execute_command('TS.MADD', 'fl', 1000, 1, 'fl', 1500, 2, 'fl', 2000, 3)

        forward = rows(self.client.execute_command(
            'TS.NRANGE', 1, 'fl', '-', '+', 'AGGREGATION', 'first,last', 1000))
        reverse = rows(self.client.execute_command(
            'TS.NREVRANGE', 1, 'fl', '-', '+', 'AGGREGATION', 'first,last', 1000))

        assert forward == [(1000, [1.0, 2.0]), (2000, [3.0, 3.0])]
        assert reverse == list(reversed(forward))

    def test_nrevrange_empty_reply(self):
        """Nothing in range is an empty array, as for the forward command."""

        self.setup_data()

        assert self.client.execute_command(
            'TS.NREVRANGE', 2, 's1', 's2', 100000, 200000) == []

    def test_nrevrange_errors(self):
        """Argument validation is shared with TS.NRANGE."""

        self.setup_data()

        with pytest.raises(ResponseError):
            self.client.execute_command('TS.NREVRANGE', 0, 's1', '-', '+')

        with pytest.raises(ResponseError):
            self.client.execute_command('TS.NREVRANGE', 3, 's1', 's2', '-', '+')

        with pytest.raises(ResponseError):
            self.client.execute_command('TS.NREVRANGE', 2, 's1', 'nosuchkey', '-', '+')

        # one aggregator per key
        with pytest.raises(ResponseError):
            self.client.execute_command(
                'TS.NREVRANGE', 2, 's3', 's4', '-', '+', 'AGGREGATION', 'avg', 1000)
