import pytest
from valkey import ResponseError
from valkeytestframework.conftest import resource_port_tracker
from valkey_timeseries_test_case import ValkeyTimeSeriesTestCaseBase


class TestTimeSeriesMultiAggregation(ValkeyTimeSeriesTestCaseBase):
    """Multi-aggregation support: AGGREGATION accepts a comma-separated list
    of aggregators sharing one set of bucket parameters. Each bucket row is
    [timestamp, value_per_aggregator...] in the specified order."""

    def setup_data(self):
        self.client.execute_command('TS.CREATE', 'ts1')
        self.client.execute_command('TS.ADD', 'ts1', 1000, 10.0)
        self.client.execute_command('TS.ADD', 'ts1', 1500, 30.0)
        self.client.execute_command('TS.ADD', 'ts1', 2000, 20.0)
        self.client.execute_command('TS.ADD', 'ts1', 3500, 40.0)

    def setup_mrange_data(self):
        self.client.execute_command('TS.CREATE', 'm1', 'LABELS', 'region', 'us', 'kind', 'a')
        self.client.execute_command('TS.CREATE', 'm2', 'LABELS', 'region', 'us', 'kind', 'b')
        self.client.execute_command('TS.ADD', 'm1', 1000, 2.0)
        self.client.execute_command('TS.ADD', 'm1', 1500, 4.0)
        self.client.execute_command('TS.ADD', 'm2', 1000, 10.0)
        self.client.execute_command('TS.ADD', 'm2', 2500, 6.0)

    def test_range_multi_aggregation_row_shape(self):
        self.setup_data()

        result = self.client.execute_command(
            'TS.RANGE', 'ts1', '-', '+', 'AGGREGATION', 'avg,max,count', 1000)
        # bucket [1000,2000): 10, 30 -> avg 20, max 30, count 2
        # bucket [2000,3000): 20     -> avg 20, max 20, count 1
        # bucket [3000,4000): 40     -> avg 40, max 40, count 1
        assert result == [
            [1000, b'20', b'30', b'2'],
            [2000, b'20', b'20', b'1'],
            [3000, b'40', b'40', b'1'],
        ]

    def test_single_aggregation_shape_unchanged(self):
        """A one-element list is identical to the legacy single aggregator."""
        self.setup_data()

        legacy = self.client.execute_command(
            'TS.RANGE', 'ts1', '-', '+', 'AGGREGATION', 'sum', 1000)
        assert legacy == [[1000, b'40'], [2000, b'20'], [3000, b'40']]

    def test_revrange_multi_aggregation(self):
        self.setup_data()

        result = self.client.execute_command(
            'TS.REVRANGE', 'ts1', '-', '+', 'AGGREGATION', 'min,max', 1000, 'COUNT', 2)
        assert result == [
            [3000, b'40', b'40'],
            [2000, b'20', b'20'],
        ]

    def test_multi_aggregation_column_matches_single(self):
        """Column i of a multi query equals running that aggregator alone."""
        self.setup_data()
        aggregators = ['avg', 'max', 'min', 'sum', 'count']

        multi = self.client.execute_command(
            'TS.RANGE', 'ts1', '-', '+',
            'AGGREGATION', ','.join(aggregators), 1000)
        for i, agg in enumerate(aggregators):
            single = self.client.execute_command(
                'TS.RANGE', 'ts1', '-', '+', 'AGGREGATION', agg, 1000)
            assert len(single) == len(multi)
            for row, expected in zip(multi, single):
                assert row[0] == expected[0]
                assert row[1 + i] == expected[1]

    def test_multi_aggregation_with_inline_condition(self):
        """Each condition-requiring aggregator carries its own inline
        condition; plain aggregators in the same list take none."""
        self.setup_data()

        result = self.client.execute_command(
            'TS.RANGE', 'ts1', '-', '+',
            'AGGREGATION', 'countif>15,avg', 1000)
        # bucket [1000,2000): countif>15=1 (30), avg=20
        assert result[0] == [1000, b'1', b'20']

    def test_multi_aggregation_errors(self):
        self.setup_data()

        # duplicates (case-insensitive)
        with pytest.raises(ResponseError, match='duplicate aggregation'):
            self.client.execute_command(
                'TS.RANGE', 'ts1', '-', '+', 'AGGREGATION', 'avg,AVG', 1000)
        # empty element
        with pytest.raises(ResponseError, match='invalid aggregation list'):
            self.client.execute_command(
                'TS.RANGE', 'ts1', '-', '+', 'AGGREGATION', 'avg,,max', 1000)
        # unknown type
        with pytest.raises(ResponseError):
            self.client.execute_command(
                'TS.RANGE', 'ts1', '-', '+', 'AGGREGATION', 'avg,bogus', 1000)
        # condition-requiring aggregator without an inline condition
        with pytest.raises(ResponseError, match='missing condition'):
            self.client.execute_command(
                'TS.RANGE', 'ts1', '-', '+', 'AGGREGATION', 'countif,avg', 1000)
        # inline condition on an aggregator that doesn't accept one
        with pytest.raises(ResponseError, match='does not support a filter'):
            self.client.execute_command(
                'TS.RANGE', 'ts1', '-', '+',
                'AGGREGATION', 'avg>5,max', 1000)

    def test_mrange_multi_aggregation(self):
        self.setup_mrange_data()

        result = self.client.execute_command(
            'TS.MRANGE', '-', '+',
            'AGGREGATION', 'avg,count', 1000,
            'FILTER', 'region=us')
        assert len(result) == 2
        by_key = {series[0]: series[2] for series in result}
        # m1 bucket [1000,2000): avg 3, count 2
        assert by_key[b'm1'] == [[1000, b'3', b'2']]
        # m2 buckets: [1000,2000) avg 10 count 1; [2000,3000) avg 6 count 1
        assert by_key[b'm2'] == [[1000, b'10', b'1'], [2000, b'6', b'1']]

    def test_mrange_multi_aggregation_groupby(self):
        """Column-wise reduce: each column reduced independently across the
        series of a group."""
        self.setup_mrange_data()

        result = self.client.execute_command(
            'TS.MRANGE', '-', '+',
            'AGGREGATION', 'avg,max', 1000,
            'FILTER', 'region=us',
            'GROUPBY', 'region', 'REDUCE', 'sum')
        assert len(result) == 1
        key, _labels, rows = result[0]
        assert key == b'region=us'
        # ts 1000: avg 3+10=13, max 4+10=14; ts 2000: m2 only -> 6, 6
        assert rows == [[1000, b'13', b'14'], [2000, b'6', b'6']]

    def test_join_rejects_multi_aggregation(self):
        self.setup_data()
        self.client.execute_command('TS.CREATE', 'ts2')
        self.client.execute_command('TS.ADD', 'ts2', 1000, 1.0)

        with pytest.raises(ResponseError, match='multiple aggregations'):
            self.client.execute_command(
                'TS.JOIN', 'ts1', 'ts2', '-', '+',
                'REDUCE', 'sum', 'AGGREGATION', 'avg,max', 1000)
