import pytest
from valkey import ResponseError
from valkeytestframework.util.waiters import *
from valkeytestframework.conftest import resource_port_tracker
from valkey_timeseries_test_case import ValkeyTimeSeriesTestCaseBase


class TestTimeSeriesRevRange(ValkeyTimeSeriesTestCaseBase):
    def setup_data(self):
        # Setup some time series data
        self.client.execute_command('TS.CREATE', 'ts1')
        self.client.execute_command('TS.ADD', 'ts1', 1000, 10.1)
        self.client.execute_command('TS.ADD', 'ts1', 2000, 20.2)
        self.client.execute_command('TS.ADD', 'ts1', 3000, 30.3)
        self.client.execute_command('TS.ADD', 'ts1', 4000, 40.4)
        self.client.execute_command('TS.ADD', 'ts1', 5000, 50.5)

    def test_basic_revrange(self):
        """Test basic TS.REVRANGE with start and end timestamps"""
        self.setup_data()

        result = self.client.execute_command('TS.REVRANGE', 'ts1', 2000, 4000)
        # Results should be in reverse order
        assert result == [[4000, b'40.4'], [3000, b'30.3'], [2000, b'20.2']]

    def test_full_revrange(self):
        """Test TS.REVRANGE with '-' and '+'"""
        self.setup_data()

        result = self.client.execute_command('TS.REVRANGE', 'ts1', '-', '+')
        assert len(result) == 5
        # Should be in reverse order
        assert result[0] == [5000, b'50.5']
        assert result[-1] == [1000, b'10.1']

    def test_revrange_with_count(self):
        """Test TS.REVRANGE with the COUNT option"""
        self.setup_data()

        # Should return the first 2 samples in reverse order
        result = self.client.execute_command('TS.REVRANGE', 'ts1', '-', '+', 'COUNT', 2)

        assert result == [[5000, b'50.5'], [4000, b'40.4']]

    def test_revrange_filter_by_ts(self):
        """Test TS.REVRANGE with FILTER_BY_TS"""
        self.setup_data()

        result = self.client.execute_command('TS.REVRANGE', 'ts1', '-', '+', 'FILTER_BY_TS', 1000, 3000, 5000)
        # Results should be in reverse order
        assert result == [[5000, b'50.5'], [3000, b'30.3'], [1000, b'10.1']]

    def test_revrange_filter_by_value(self):
        """Test TS.REVRANGE with FILTER_BY_VALUE"""
        self.setup_data()

        result = self.client.execute_command('TS.REVRANGE', 'ts1', '-', '+', 'FILTER_BY_VALUE', 20, 40)
        # Results should be in reverse order
        assert result == [[3000, b'30.3'], [2000, b'20.2']]

        # Test inclusive range
        result = self.client.execute_command('TS.REVRANGE', 'ts1', '-', '+', 'FILTER_BY_VALUE', 20.2, 40.4)
        assert result == [[4000, b'40.4'], [3000, b'30.3'], [2000, b'20.2']]

    def test_revrange_filter_by_ts_and_value(self):
        """Test TS.REVRANGE combining FILTER_BY_TS and FILTER_BY_VALUE"""
        self.setup_data()

        result = self.client.execute_command('TS.REVRANGE', 'ts1', '-', '+',
                                             'FILTER_BY_TS', 2000, 4000, 5000,
                                             'FILTER_BY_VALUE', 35, 60)
        # Should match samples at 4000 and 5000, in reverse order
        assert result == [[5000, b'50.5'], [4000, b'40.4']]

    def test_revrange_aggregation_options(self):
        """Test TS.REVRANGE aggregation with ALIGN, BUCKETTIMESTAMP, EMPTY"""
        self.setup_data()

        result = self.client.execute_command('TS.REVRANGE', 'ts1', 500, 5000,
                                             'ALIGN', 0,
                                             'AGGREGATION', 'SUM', 2000,
                                             'BUCKETTIMESTAMP', 'MID')
        # Results should be in reverse order
        assert len(result) == 3
        # Bucket 2 (4000-5999): sum(40.4, 50.5) = 90.9, mid timestamp = 5000
        assert result[0] == [5000, b'90.9']
        # Bucket 1 (2000-3999): sum(20.2, 30.3) = 50.5, mid timestamp = 3000
        assert result[1] == [3000, b'50.5']
        # Bucket 0 (0-1999): sum(10.1) = 10.1, mid timestamp = 1000
        assert result[2] == [1000, b'10.1']

    def test_revrange_aggregation_empty_buckets(self):
        """Test TS.REVRANGE aggregation with EMPTY option"""
        self.client.execute_command('TS.CREATE', 'ts1')
        self.client.execute_command('TS.ADD', 'ts1', 100, 10)
        self.client.execute_command('TS.ADD', 'ts1', 110, 20)
        self.client.execute_command('TS.ADD', 'ts1', 150, 30)
        self.client.execute_command('TS.ADD', 'ts1', 160, 40)
        self.client.execute_command('TS.ADD', 'ts1', 200, 50)

        # With the EMPTY option
        result = self.client.execute_command('TS.REVRANGE', 'ts1', '-', '+',
                                             'ALIGN', 0,
                                             'AGGREGATION', 'SUM', 25,
                                             'BUCKETTIMESTAMP', 'START',
                                             'EMPTY')
        assert len(result) == 5
        # Results in reverse order
        assert result[0] == [200, b'50']
        assert result[1] == [175, b'0']  # Empty bucket
        assert result[2] == [150, b'70']
        assert result[3] == [125, b'0']  # Empty bucket
        assert result[4] == [100, b'30']

    def test_revrange_aggregation_with_filters(self):
        """Test TS.REVRANGE combining aggregation and filters"""
        self.client.execute_command('TS.CREATE', 'ts1')
        for i in range(0, 1000, 10):
            self.client.execute_command('TS.ADD', 'ts1', (i + 1) * 1000, 10 + (i * 10))

        result = self.client.execute_command('TS.REVRANGE', 'ts1', '-', '+',
                                             'FILTER_BY_VALUE', 500, 1000,
                                             'ALIGN', 0,
                                             'AGGREGATION', 'SUM', 10000)
        # Results in reverse order
        assert result == [[90000, b'910'], [80000, b'810'], [70000, b'710'], [60000, b'610'], [50000, b'510']]

    def test_revrange_empty_series(self):
        """Test TS.REVRANGE on an existing but empty series"""
        self.client.execute_command('TS.CREATE', 'ts_empty')
        result = self.client.execute_command('TS.REVRANGE', 'ts_empty', '-', '+')
        assert result == []

        # With aggregation and EMPTY
        result = self.client.execute_command('TS.REVRANGE', 'ts_empty', 0, 1000, 'AGGREGATION', 'SUM', 500, 'EMPTY')
        assert result == []

    def test_revrange_non_existent_series(self):
        """Test TS.REVRANGE on a non-existent key"""
        with pytest.raises(ResponseError) as excinfo:
            self.client.execute_command('TS.REVRANGE', 'ts_nonexistent', '-', '+')

        assert "key does not exist" in str(excinfo.value).lower()

    def setup_aggregation_data(self):
        """Setup predictable test data for aggregation tests"""
        self.client.execute_command('TS.CREATE', 'agg_test')

        # Add known values: [1, 2, 3, 4, 5, 6] at timestamps 1000, 2000, 3000, 4000, 5000, 6000
        values = [1.0, 2.0, 3.0, 4.0, 5.0, 6.0]
        for i, value in enumerate(values):
            self.client.execute_command('TS.ADD', 'agg_test', (i + 1) * 1000, value)

    def test_revrange_avg_aggregation(self):
        """Test TS.REVRANGE AVG aggregation in reverse order"""
        self.setup_aggregation_data()

        result = self.client.execute_command('TS.REVRANGE', 'agg_test', '-', '+',
                                             'AGGREGATION', 'AVG', 3000, 'ALIGN', 0)
        assert len(result) == 3
        # Results in reverse order
        assert float(result[0][1]) == pytest.approx(6.0)  # bucket [6000-9000)
        assert float(result[1][1]) == pytest.approx(4.0)  # bucket [3000-6000)
        assert float(result[2][1]) == pytest.approx(1.5)  # bucket [0-3000)

    def test_revrange_comparison_with_range(self):
        """Test that TS.REVRANGE returns reverse of TS.RANGE"""
        self.setup_data()

        range_result = self.client.execute_command('TS.RANGE', 'ts1', '-', '+')
        revrange_result = self.client.execute_command('TS.REVRANGE', 'ts1', '-', '+')

        # REVRANGE should be the reverse of RANGE
        assert revrange_result == list(reversed(range_result))

    def test_revrange_comparison_with_aggregation(self):
        """Test that aggregated TS.REVRANGE returns reverse of aggregated TS.RANGE"""
        self.setup_data()

        range_result = self.client.execute_command('TS.RANGE', 'ts1', '-', '+',
                                                   'AGGREGATION', 'SUM', 2000,
                                                   'ALIGN', 0)
        revrange_result = self.client.execute_command('TS.REVRANGE', 'ts1', '-', '+',
                                                      'AGGREGATION', 'SUM', 2000,
                                                      'ALIGN', 0)

        # REVRANGE should be the reverse of RANGE
        assert revrange_result == list(reversed(range_result))

    def test_revrange_edge_cases(self):
        """Test TS.REVRANGE with edge case timestamps"""
        self.setup_data()

        # Exact start/end match
        result = self.client.execute_command('TS.REVRANGE', 'ts1', 2000, 2000)
        assert result == [[2000, b'20.2']]

        # Range before first sample
        result = self.client.execute_command('TS.REVRANGE', 'ts1', 0, 500)
        assert result == []

        # Range after last sample
        result = self.client.execute_command('TS.REVRANGE', 'ts1', 6000, 7000)
        assert result == []

        # Range partially overlapping
        result = self.client.execute_command('TS.REVRANGE', 'ts1', 4500, 5500)
        assert result == [[5000, b'50.5']]

    def test_revrange_error_handling(self):
        """Test error conditions for TS.REVRANGE"""
        # Wrong number of arguments
        with pytest.raises(ResponseError, match="wrong number of arguments"):
            self.client.execute_command('TS.REVRANGE', 'ts1')

        with pytest.raises(ResponseError, match="wrong number of arguments"):
            self.client.execute_command('TS.REVRANGE', 'ts1', '-')

        # Invalid start/end timestamp
        with pytest.raises(ResponseError, match="wrong fromTimestamp"):
            self.client.execute_command('TS.REVRANGE', 'ts1', 'invalid', '+')

        with pytest.raises(ResponseError, match="wrong toTimestamp"):
            self.client.execute_command('TS.REVRANGE', 'ts1', '-', 'invalid')

        # Invalid filter values
        with pytest.raises(ResponseError, match="TSDB: Couldn't parse MIN"):
            self.client.execute_command('TS.REVRANGE', 'ts1', '-', '+', 'FILTER_BY_VALUE', 'a', 'b')

        with pytest.raises(ResponseError, match="TSDB: Couldn't parse MAX"):
            self.client.execute_command('TS.REVRANGE', 'ts1', '-', '+', 'FILTER_BY_VALUE', 1000, 'b')

    @pytest.mark.parametrize("agg_type,expected_single_bucket", [
        ('AVG', 3.5),
        ('SUM', 21.0),
        ('MIN', 1.0),
        ('MAX', 6.0),
        ('COUNT', 6.0),
        # first/last follow the scan, so TS.REVRANGE reports the newest sample
        # as FIRST — the mirror of TS.RANGE, and what RedisTimeSeries does.
        ('FIRST', 6.0),
        ('LAST', 1.0),
        ('RANGE', 5.0),
        ('STD.P', 1.708),
        ('STD.S', 1.871),
        ('VAR.P', 2.917),
        ('VAR.S', 3.5),
    ])
    def test_revrange_all_aggregation_types(self, agg_type, expected_single_bucket):
        """Parametrized test for all aggregation types with TS.REVRANGE"""
        self.setup_aggregation_data()

        result = self.client.execute_command('TS.REVRANGE', 'agg_test', 0, 7000,
                                             'AGGREGATION', agg_type, 7000)

        assert len(result) == 1
        if agg_type in ['STD.P', 'STD.S', 'VAR.P', 'VAR.S']:
            assert float(result[0][1]) == pytest.approx(expected_single_bucket, abs=0.01)
        else:
            assert float(result[0][1]) == pytest.approx(expected_single_bucket)

    def test_revrange_with_count_and_filters(self):
        """Test TS.REVRANGE with COUNT combined with filters"""
        self.setup_data()

        result = self.client.execute_command('TS.REVRANGE', 'ts1', '-', '+',
                                             'FILTER_BY_VALUE', 20, 60,
                                             'COUNT', 2)
        # Should get the first 2 samples in reverse order after filtering
        assert result == [[5000, b'50.5'], [4000, b'40.4']]

    def test_revrange_single_sample(self):
        """Test TS.REVRANGE with series containing single sample"""
        self.client.execute_command('TS.CREATE', 'ts_single')
        self.client.execute_command('TS.ADD', 'ts_single', 1000, 42.0)

        result = self.client.execute_command('TS.REVRANGE', 'ts_single', '-', '+')
        assert result == [[1000, b'42']]

    def test_revrange_bucket_timestamp_options(self):
        """Test TS.REVRANGE with different BUCKETTIMESTAMP options in reverse"""
        self.setup_aggregation_data()

        # Test START, MID, END - all should have the same values but different timestamps
        result_start = self.client.execute_command('TS.REVRANGE', 'agg_test', 0, 7000,
                                                   'AGGREGATION', 'SUM', 3000, 'ALIGN', 0,
                                                   'BUCKETTIMESTAMP', 'START')

        result_mid = self.client.execute_command('TS.REVRANGE', 'agg_test', 0, 7000,
                                                 'AGGREGATION', 'SUM', 3000, 'ALIGN', 0,
                                                 'BUCKETTIMESTAMP', 'MID')

        result_end = self.client.execute_command('TS.REVRANGE', 'agg_test', 0, 7000,
                                                 'AGGREGATION', 'SUM', 3000, 'ALIGN', 0,
                                                 'BUCKETTIMESTAMP', 'END')

        # All should have the same length and values (in reverse order)
        assert len(result_start) == len(result_mid) == len(result_end) == 3

        # Check values are the same across all bucket timestamp types
        for i in range(len(result_start)):
            assert float(result_start[i][1]) == float(result_mid[i][1]) == float(result_end[i][1])
