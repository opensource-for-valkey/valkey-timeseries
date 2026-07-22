import math

import pytest
from valkey import ResponseError
from valkeytestframework.util.waiters import *
from valkeytestframework.conftest import resource_port_tracker
from valkey_timeseries_test_case import ValkeyTimeSeriesTestCaseBase


# TODO: Aggregation and groupby tests are not (yet) implemented in this test case.
class TestTimeSeriesRange(ValkeyTimeSeriesTestCaseBase):
    def setup_data(self):
        # Setup some time series data
        self.client.execute_command('TS.CREATE', 'ts1')
        self.client.execute_command('TS.ADD', 'ts1', 1000, 10.1)
        self.client.execute_command('TS.ADD', 'ts1', 2000, 20.2)
        self.client.execute_command('TS.ADD', 'ts1', 3000, 30.3)
        self.client.execute_command('TS.ADD', 'ts1', 4000, 40.4)
        self.client.execute_command('TS.ADD', 'ts1', 5000, 50.5)

    def test_basic_range(self):
        """Test basic TS.RANGE with start and end timestamps"""

        self.setup_data()

        result = self.client.execute_command('TS.RANGE', 'ts1', 2000, 4000)
        assert result == [[2000, b'20.2'], [3000, b'30.3'], [4000, b'40.4']]

    def test_full_range(self):
        """Test TS.RANGE with '-' and '+'"""

        self.setup_data()

        result = self.client.execute_command('TS.RANGE', 'ts1', '-', '+')
        assert len(result) == 5
        assert result[0] == [1000, b'10.1']
        assert result[-1] == [5000, b'50.5']

    def test_range_with_count(self):
        """Test TS.RANGE with COUNT option"""

        self.setup_data()

        # Forward count
        result = self.client.execute_command('TS.RANGE', 'ts1', '-', '+', 'COUNT', 2)
        assert result == [[1000, b'10.1'], [2000, b'20.2']]

    def test_range_filter_by_ts(self):
        """Test TS.RANGE with FILTER_BY_TS"""

        self.setup_data()

        result = self.client.execute_command('TS.RANGE', 'ts1', '-', '+', 'FILTER_BY_TS', 1000, 3000, 5000)
        assert result == [[1000, b'10.1'], [3000, b'30.3'], [5000, b'50.5']]

    def test_range_filter_single_item(self):
        """Test TS.RANGE with FILTER_BY_TS"""

        self.setup_data()

        result = self.client.execute_command('TS.RANGE', 'ts1', '-', '+', 'FILTER_BY_TS', 3000)
        assert result == [[3000, b'30.3']]

        self.client.execute_command('TS.ADD', 'single', 1000, 10.0)
        result = self.client.execute_command('TS.RANGE', 'single', 1000, 1000)
        assert result == [[1000, b'10']]

        result = self.client.execute_command('TS.RANGE', 'single', 1000, 1000, 'FILTER_BY_TS', 1000)
        assert result == [[1000, b'10']]

    def test_range_filter_by_value(self):
        """Test TS.RANGE with FILTER_BY_VALUE"""

        self.setup_data()

        result = self.client.execute_command('TS.RANGE', 'ts1', '-', '+', 'FILTER_BY_VALUE', 20, 40)
        assert result == [[2000, b'20.2'], [3000, b'30.3']]
        # Note: 40.4 is excluded because the range is min <= value < max

        # Test inclusive range (using slightly adjusted values)
        result = self.client.execute_command('TS.RANGE', 'ts1', '-', '+', 'FILTER_BY_VALUE', 20.2, 40.4)
        assert result == [[2000, b'20.2'], [3000, b'30.3'], [4000, b'40.4']]

    def test_range_filter_by_ts_and_value(self):
        """Test TS.RANGE combining FILTER_BY_TS and FILTER_BY_VALUE"""

        self.setup_data()

        result = self.client.execute_command('TS.RANGE', 'ts1', '-', '+',
                                             'FILTER_BY_TS', 2000, 4000, 5000,
                                             'FILTER_BY_VALUE', 35, 60)
        assert result == [[4000, b'40.4'], [5000, b'50.5']]

    def test_range_aggregation_options(self):
        """Test TS.RANGE aggregation with ALIGN, BUCKETTIMESTAMP, EMPTY"""

        self.setup_data()

        # Align to 0, bucket timestamp mid, report empty
        result = self.client.execute_command('TS.RANGE', 'ts1', 500, 5000,
                                             'ALIGN', 0,
                                             'AGGREGATION', 'SUM', 2000,
                                             'BUCKETTIMESTAMP', 'MID')
        assert len(result) == 3
        # Bucket 0 (0-1999): sum(10.1) = 10.1, mid timestamp = 1000
        assert result[0] == [1000, b'10.1']
        # Bucket 1 (2000-3999): sum(20.2, 30.3) = 50.5, mid timestamp = 3000
        assert result[1] == [3000, b'50.5']
        # Bucket 2 (4000-5999): sum(40.4, 50.5) = 90.9, mid timestamp = 5000
        assert result[2] == [5000, b'90.9']

    def test_aggregation_empty_buckets(self):
        """Test TS.RANGE aggregation with ALIGN, BUCKETTIMESTAMP, EMPTY"""

        self.client.execute_command('TS.CREATE', 'ts1')
        self.client.execute_command('TS.ADD', 'ts1', 100, 10)
        self.client.execute_command('TS.ADD', 'ts1', 110, 20)
        self.client.execute_command('TS.ADD', 'ts1', 150, 30)
        self.client.execute_command('TS.ADD', 'ts1', 160, 40)
        self.client.execute_command('TS.ADD', 'ts1', 200, 50)

        # Align to 0, bucket timestamp mid, dont report empty
        result = self.client.execute_command('TS.RANGE', 'ts1', "-", "+",
                                             'ALIGN', 0,
                                             'AGGREGATION', 'SUM', 25,
                                             'BUCKETTIMESTAMP', 'START')
        assert len(result) == 3
        assert result[0] == [100, b'30']
        assert result[1] == [150, b'70']
        assert result[2] == [200, b'50']  # Empty buckets return None

        # Align to 0, bucket timestamp mid, report empty
        result = self.client.execute_command('TS.RANGE', 'ts1', "-", "+",
                                             'ALIGN', 0,
                                             'AGGREGATION', 'SUM', 25,
                                             'BUCKETTIMESTAMP', 'START',
                                             'EMPTY')
        assert len(result) == 5
        assert result[0] == [100, b'30']
        assert result[1] == [125, b'0']  # Empty bucket
        assert result[2] == [150, b'70']
        assert result[3] == [175, b'0']  # empty bucket# Last bucket with value
        assert result[4] == [200, b'50']  # Empty buckets return None

    def test_range_aggregation_with_filters(self):
        """Test TS.RANGE combining aggregation and filters"""

        self.client.execute_command('TS.CREATE', 'ts1')
        for i in range(0, 1000, 10):
            self.client.execute_command('TS.ADD', 'ts1', (i + 1) * 1000, 10 + (i * 10))

        result = self.client.execute_command('TS.RANGE', 'ts1', '-', '+',
                                             'FILTER_BY_VALUE', 500, 1000,
                                             'ALIGN', 0,
                                             'AGGREGATION', 'SUM', 10000)

        assert result == [[50000, b'510'], [60000, b'610'], [70000, b'710'], [80000, b'810'], [90000, b'910']]
        # Bucket 3 (6000-...) No values

    def test_range_empty_series(self):
        """Test TS.RANGE on an existing but empty series"""

        self.setup_data()

        self.client.execute_command('TS.CREATE', 'ts_empty')
        result = self.client.execute_command('TS.RANGE', 'ts_empty', '-', '+')
        assert result == []

        # With aggregation and EMPTY
        result = self.client.execute_command('TS.RANGE', 'ts_empty', 0, 1000, 'AGGREGATION', 'SUM', 500, 'EMPTY')
        assert result == []

    def test_range_non_existent_series(self):
        """Test TS.RANGE on a non-existent key"""

        with pytest.raises(ResponseError) as excinfo:
            self.client.execute_command('TS.RANGE', 'ts_nonexistent', '-', '+')

        assert "key does not exist" in str(excinfo.value).lower()

    def test_range_returns_nan_values(self):
        """Test TS.RANGE returns NaN samples without dropping them."""

        self.client.execute_command('TS.CREATE', 'ts_nan')
        self.client.execute_command('TS.ADD', 'ts_nan', 1000, 1.0)
        self.client.execute_command('TS.ADD', 'ts_nan', 2000, 'nan')
        self.client.execute_command('TS.ADD', 'ts_nan', 3000, 3.0)
        self.client.execute_command('TS.ADD', 'ts_nan', 4000, 'nan')

        result = self.client.execute_command('TS.RANGE', 'ts_nan', '-', '+')

        assert len(result) == 4
        assert result[0] == [1000, b'1']
        assert math.isnan(float(result[1][1])), f"Expected NaN, got {result[1][1]}"
        assert result[2] == [3000, b'3']
        assert math.isnan(float(result[3][1])), f"Expected NaN, got {result[3][1]}"

    def setup_aggregation_data(self):
        """Setup predictable test data for aggregation tests"""
        self.client.execute_command('TS.CREATE', 'agg_test')

        # Add known values: [1, 2, 3, 4, 5, 6] at timestamps 1000, 2000, 3000, 4000, 5000, 6000
        values = [1.0, 2.0, 3.0, 4.0, 5.0, 6.0]
        for i, value in enumerate(values):
            self.client.execute_command('TS.ADD', 'agg_test', (i + 1) * 1000, value)

    def test_avg_aggregation(self):
        """Test AVG aggregation"""
        self.setup_aggregation_data()

        # Single bucket containing all values [1,2,3,4,5,6] -> avg = 3.5
        result = self.client.execute_command('TS.RANGE', 'agg_test', '-', '+',
                                             'AGGREGATION', 'AVG', 7000)
        assert len(result) == 1
        assert float(result[0][1]) == pytest.approx(3.5)

        # Two buckets: [1,2,3] and [4,5,6] -> avg = 2.0 and 5.0
        result = self.client.execute_command('TS.RANGE', 'agg_test', '-', '+',
                                             'AGGREGATION', 'AVG', 3000, 'ALIGN', 0)
        assert len(result) == 3
        assert float(result[0][1]) == pytest.approx(1.5)  # (1+2)/2
        assert float(result[1][1]) == pytest.approx(4.0)  # (3+4+5)/3
        assert float(result[2][1]) == pytest.approx(6.0)  # (3+4+5)/3

    def test_sum_aggregation(self):
        """Test SUM aggregation"""
        self.setup_aggregation_data()

        # Single bucket: sum of [1,2,3,4,5,6] = 21
        result = self.client.execute_command('TS.RANGE', 'agg_test', 0, 7000,
                                             'AGGREGATION', 'SUM', 7000)
        assert len(result) == 1
        assert float(result[0][1]) == pytest.approx(21.0)

        result = self.client.execute_command('TS.RANGE', 'agg_test', 0, 7000,
                                             'AGGREGATION', 'SUM', 3000, 'ALIGN', 0)

        assert len(result) == 3
        assert float(result[0][1]) == pytest.approx(3.0)
        assert float(result[1][1]) == pytest.approx(12.0)
        assert float(result[2][1]) == pytest.approx(6.0)

    def test_min_aggregation(self):
        """Test MIN aggregation"""
        self.setup_aggregation_data()

        # Single bucket: min of [1,2,3,4,5,6] = 1
        result = self.client.execute_command('TS.RANGE', 'agg_test', 0, 7000,
                                             'AGGREGATION', 'MIN', 7000)
        assert len(result) == 1
        assert float(result[0][1]) == pytest.approx(1.0)

        # Two buckets: [1,2,3] and [4,5,6] -> min = 1 and 4
        result = self.client.execute_command('TS.RANGE', 'agg_test', 1000, 7000,
                                             'AGGREGATION', 'MIN', 3000, 'ALIGN', 'start')
        assert len(result) == 2
        assert float(result[0][1]) == pytest.approx(1.0)
        assert float(result[1][1]) == pytest.approx(4.0)

    def test_max_aggregation(self):
        """Test MAX aggregation"""
        self.setup_aggregation_data()

        # Single bucket: max of [1,2,3,4,5,6] = 6
        result = self.client.execute_command('TS.RANGE', 'agg_test', 0, 7000,
                                             'AGGREGATION', 'MAX', 7000)
        assert len(result) == 1
        assert float(result[0][1]) == pytest.approx(6.0)

        # Two buckets: [1,2,3] and [4,5,6] -> max = 3 and 6
        result = self.client.execute_command('TS.RANGE', 'agg_test', 1000, 7000,
                                             'AGGREGATION', 'MAX', 3000, 'ALIGN', '-')
        assert len(result) == 2
        assert float(result[0][1]) == pytest.approx(3.0)
        assert float(result[1][1]) == pytest.approx(6.0)

    def test_count_aggregation(self):
        """Test COUNT aggregation"""
        self.setup_aggregation_data()

        # Single bucket: count of [1,2,3,4,5,6] = 6
        result = self.client.execute_command('TS.RANGE', 'agg_test', 0, 7000,
                                             'AGGREGATION', 'COUNT', 7000)
        assert len(result) == 1
        assert float(result[0][1]) == pytest.approx(6.0)

        # Two buckets: [1,2,3] and [4,5,6] -> count = 3 and 3
        result = self.client.execute_command('TS.RANGE', 'agg_test', 1000, 7000,
                                             'AGGREGATION', 'COUNT', 3000, 'ALIGN', 'start')
        assert len(result) == 2
        assert float(result[0][1]) == pytest.approx(3.0)
        assert float(result[1][1]) == pytest.approx(3.0)

    def test_first_aggregation(self):
        """Test FIRST aggregation"""
        self.setup_aggregation_data()

        # Single bucket: first of [1,2,3,4,5,6] = 1
        result = self.client.execute_command('TS.RANGE', 'agg_test', 0, 7000,
                                             'AGGREGATION', 'FIRST', 7000)
        assert len(result) == 1
        assert float(result[0][1]) == pytest.approx(1.0)

        # Two buckets: [1,2,3] and [4,5,6] -> first = 1 and 4
        result = self.client.execute_command('TS.RANGE', 'agg_test', 1000, 7000,
                                             'AGGREGATION', 'FIRST', 3000, 'ALIGN', 'start')
        assert len(result) == 2
        assert float(result[0][1]) == pytest.approx(1.0)
        assert float(result[1][1]) == pytest.approx(4.0)

    def test_increase_aggregation_with_reset(self):
        """
        Test INCREASE aggregator.

        Data models a counter that increases, then resets (drops), then increases again.
        We aggregate with a bucket size that ensures each non-empty bucket contains 2 samples,
        so INCREASE can compute a delta within the bucket.
        """
        self.client.execute_command('TS.CREATE', 'counter_inc')

        # Bucket size will be 2000ms with ALIGN 0:
        #   [0,2000): 1000
        #   [2000,4000): 2000, 3000
        #   [4000,6000): 4000, 5000
        self.client.execute_command('TS.ADD', 'counter_inc', 1000, 0)
        self.client.execute_command('TS.ADD', 'counter_inc', 2000, 10)
        self.client.execute_command('TS.ADD', 'counter_inc', 3000, 20)
        self.client.execute_command('TS.ADD', 'counter_inc', 4000, 5)  # reset
        self.client.execute_command('TS.ADD', 'counter_inc', 5000, 15)  # +10 after reset

        result = self.client.execute_command(
            'TS.RANGE', 'counter_inc', '-', '+',
            'ALIGN', 0,
            'AGGREGATION', 'INCREASE', 2000,
            'BUCKETTIMESTAMP', 'START'
        )

        # Expect 3 buckets (start timestamps 0, 2000, 4000), but note:
        # bucket with only 1 sample yields INCREASE=0 (no prior point inside the bucket)
        assert len(result) == 3
        assert result[0][0] == 0
        assert float(result[0][1]) == pytest.approx(0.0)
        assert result[1][0] == 2000
        assert float(result[1][1]) == pytest.approx(10.0)  # 20 - 10
        assert result[2][0] == 4000
        assert float(result[2][1]) == pytest.approx(10.0)  # reset ignored, 15 - 5

    def test_increase_aggregation_monotonic(self):
        """
        INCREASE on a monotonic counter: per-bucket increase should be last - first
        (and 0 for buckets with <2 samples).
        """
        self.client.execute_command('TS.CREATE', 'counter_inc_mono')

        # Buckets of 2000ms with ALIGN 0:
        # [0,2000): 1000 (single sample)
        # [2000,4000): 2000,3000 (10->25 => +15)
        # [4000,6000): 4000,5000 (25->60 => +35)
        self.client.execute_command('TS.ADD', 'counter_inc_mono', 1000, 0)
        self.client.execute_command('TS.ADD', 'counter_inc_mono', 2000, 10)
        self.client.execute_command('TS.ADD', 'counter_inc_mono', 3000, 25)
        self.client.execute_command('TS.ADD', 'counter_inc_mono', 4000, 25)
        self.client.execute_command('TS.ADD', 'counter_inc_mono', 5000, 60)

        result = self.client.execute_command(
            'TS.RANGE', 'counter_inc_mono', '-', '+',
            'ALIGN', 0,
            'AGGREGATION', 'INCREASE', 2000,
            'BUCKETTIMESTAMP', 'START'
        )

        assert len(result) == 3
        assert result[0][0] == 0
        assert float(result[0][1]) == pytest.approx(0.0)
        assert result[1][0] == 2000
        assert float(result[1][1]) == pytest.approx(15.0)
        assert result[2][0] == 4000
        assert float(result[2][1]) == pytest.approx(35.0)

    def test_increase_aggregation_empty_buckets(self):
        """
        INCREASE with EMPTY: buckets with no samples should still be emitted, with value NaN.
        """
        self.client.execute_command('TS.CREATE', 'counter_inc_empty')

        # Bucket=1000ms ALIGN 0 over [0..5000]
        # Data only in buckets starting at 1000 and 4000.
        self.client.execute_command('TS.ADD', 'counter_inc_empty', 1000, 5)
        self.client.execute_command('TS.ADD', 'counter_inc_empty', 1900, 8)  # within [1000,2000): +3
        self.client.execute_command('TS.ADD', 'counter_inc_empty', 4000, 10)
        self.client.execute_command('TS.ADD', 'counter_inc_empty', 4900, 17)  # within [4000,5000): +7

        result = self.client.execute_command(
            'TS.RANGE', 'counter_inc_empty', 0, 5000,
            'ALIGN', 0,
            'AGGREGATION', 'INCREASE', 1000,
            'BUCKETTIMESTAMP', 'START',
            'EMPTY'
        )

        # Expect 4 buckets: 1000,2000,3000,4000
        assert len(result) == 4

        by_ts = {ts: float(val) for ts, val in result}
        assert by_ts[1000] == pytest.approx(3.0)  # 8 - 5
        assert math.isnan(by_ts[2000]), "Expected Nan"  # empty
        assert math.isnan(by_ts[3000]), "Expected Nan"  # empty
        assert by_ts[4000] == pytest.approx(7.0)  # 17 - 10

    def test_increase_aggregation_resets_within_bucket(self):
        """
        Multiple resets inside one bucket: decreases should not reduce the increase.
        This locks in the "ignore drops" behavior within a bucket.
        """
        self.client.execute_command('TS.CREATE', 'counter_inc_multi_reset')

        # Single bucket of 5000ms with ALIGN 0 includes all points:
        # values: 0 -> 10 -> 2 (reset) -> 12 -> 1 (reset) -> 6
        # Expected increase = (10-0) + (12-2) + (6-1) = 10 + 10 + 5 = 25
        self.client.execute_command('TS.ADD', 'counter_inc_multi_reset', 1000, 0)
        self.client.execute_command('TS.ADD', 'counter_inc_multi_reset', 1500, 10)
        self.client.execute_command('TS.ADD', 'counter_inc_multi_reset', 2000, 2)
        self.client.execute_command('TS.ADD', 'counter_inc_multi_reset', 2500, 12)
        self.client.execute_command('TS.ADD', 'counter_inc_multi_reset', 3000, 1)
        self.client.execute_command('TS.ADD', 'counter_inc_multi_reset', 3500, 6)

        result = self.client.execute_command(
            'TS.RANGE', 'counter_inc_multi_reset', 0, 5000,
            'ALIGN', 0,
            'AGGREGATION', 'INCREASE', 5000,
            'BUCKETTIMESTAMP', 'START'
        )

        assert len(result) == 1
        assert result[0][0] == 0
        assert float(result[0][1]) == pytest.approx(25.0)

    def test_last_aggregation(self):
        """Test LAST aggregation"""
        self.setup_aggregation_data()

        # Single bucket: last of [1,2,3,4,5,6] = 6
        result = self.client.execute_command('TS.RANGE', 'agg_test', 0, 7000,
                                             'AGGREGATION', 'LAST', 7000)
        assert len(result) == 1
        assert float(result[0][1]) == pytest.approx(6.0)

        # Two buckets: [1,2,3] and [4,5,6] -> last = 3 and 6
        result = self.client.execute_command('TS.RANGE', 'agg_test', 1000, 7000,
                                             'AGGREGATION', 'LAST', 3000, 'ALIGN', '-')
        assert len(result) == 2
        assert float(result[0][1]) == pytest.approx(3.0)
        assert float(result[1][1]) == pytest.approx(6.0)

    def test_range_aggregation(self):
        """Test RANGE aggregation (max - min)"""
        self.setup_aggregation_data()

        # Single bucket: range of [1,2,3,4,5,6] = 6 - 1 = 5
        result = self.client.execute_command('TS.RANGE', 'agg_test', 1000, 7000,
                                             'AGGREGATION', 'RANGE', 7000)

        assert len(result) == 1
        assert float(result[0][1]) == pytest.approx(5.0)

        # Two buckets: [1,2,3] and [4,5,6] -> range = 2 and 2
        result = self.client.execute_command('TS.RANGE', 'agg_test', 1000, 7000,
                                             'AGGREGATION', 'RANGE', 3000, 'ALIGN', 'start')
        print(result)

        assert len(result) == 2
        assert float(result[0][1]) == pytest.approx(2.0)  # 3-1
        assert float(result[1][1]) == pytest.approx(2.0)  # 6-4

    def test_rate_aggregation(self):
        """
        RATE aggregator integration test.

        Uses the same data as the INCREASE test, but validates that RATE is:
          RATE = INCREASE / bucket_duration_seconds
        With bucket=2000ms => 2 seconds, so expected RATE is 10/2 = 5 in the buckets that have +10 increase.
        """
        self.client.execute_command('TS.CREATE', 'counter_rate')

        self.client.execute_command('TS.ADD', 'counter_rate', 1000, 0)
        self.client.execute_command('TS.ADD', 'counter_rate', 2000, 10)
        self.client.execute_command('TS.ADD', 'counter_rate', 3000, 20)
        self.client.execute_command('TS.ADD', 'counter_rate', 4000, 5)  # reset
        self.client.execute_command('TS.ADD', 'counter_rate', 5000, 15)  # +10 after reset

        result = self.client.execute_command(
            'TS.RANGE', 'counter_rate', '-', '+',
            'ALIGN', 0,
            'AGGREGATION', 'RATE', 2000,
            'BUCKETTIMESTAMP', 'START'
        )

        print("result:", result)
        assert len(result) == 3
        assert result[0][0] == 0
        assert float(result[0][1]) == pytest.approx(0.0)
        assert result[1][0] == 2000
        assert float(result[1][1]) == pytest.approx(5.0)  # 10 increase / 2s
        assert result[2][0] == 4000
        assert float(result[2][1]) == pytest.approx(5.0)  # 10 increase / 2s

    def test_irate_aggregation_basic(self):
        """
        IRATE aggregator integration test.

        IRATE uses only the last two samples within each bucket:
          irate = (v_last - v_prev) / ((t_last - t_prev) / 1000.0)
        """
        self.client.execute_command('TS.CREATE', 'counter_irate_basic')

        self.client.execute_command('TS.ADD', 'counter_irate_basic', 1000, 0)
        self.client.execute_command('TS.ADD', 'counter_irate_basic', 2000, 10)  # +10 over 1s => 10/s

        result = self.client.execute_command(
            'TS.RANGE', 'counter_irate_basic', 0, 5000,
            'ALIGN', 0,
            'AGGREGATION', 'IRATE', 5000,
            'BUCKETTIMESTAMP', 'START'
        )

        assert len(result) == 1
        assert result[0][0] == 0
        assert float(result[0][1]) == pytest.approx(10.0)

    def test_irate_aggregation_uses_last_two_points(self):
        """
        IRATE should reflect only the *last* delta inside the bucket, not the total increase.
        """
        self.client.execute_command('TS.CREATE', 'counter_irate_last_two')

        # Within one big bucket:
        # 1000->2000: +10 over 1s (rate 10)
        # 2000->4000: +30 over 2s (rate 15)  <-- expected
        self.client.execute_command('TS.ADD', 'counter_irate_last_two', 1000, 0)
        self.client.execute_command('TS.ADD', 'counter_irate_last_two', 2000, 10)
        self.client.execute_command('TS.ADD', 'counter_irate_last_two', 4000, 40)

        result = self.client.execute_command(
            'TS.RANGE', 'counter_irate_last_two', 0, 5000,
            'ALIGN', 0,
            'AGGREGATION', 'IRATE', 5000,
            'BUCKETTIMESTAMP', 'START'
        )

        assert len(result) == 1
        assert result[0][0] == 0
        assert float(result[0][1]) == pytest.approx(15.0)

    def test_irate_aggregation_reset_returns_nan(self):
        """
        If the counter resets (value drops) on the last update in the bucket,
        IRATE should have no valid rate for that bucket (emitted as NaN).
        """
        self.client.execute_command('TS.CREATE', 'counter_irate_reset')

        self.client.execute_command('TS.ADD', 'counter_irate_reset', 1000, 100)
        self.client.execute_command('TS.ADD', 'counter_irate_reset', 2000, 110)
        self.client.execute_command('TS.ADD', 'counter_irate_reset', 3000, 5)  # reset/drop

        result = self.client.execute_command(
            'TS.RANGE', 'counter_irate_reset', 0, 5000,
            'ALIGN', 0,
            'AGGREGATION', 'IRATE', 5000,
            'BUCKETTIMESTAMP', 'START'
        )

        assert len(result) == 1
        assert result[0][0] == 0
        assert math.isnan(float(result[0][1])), f"Expected NaN, got {result[0][1]}"

    def test_std_p_aggregation(self):
        """Test STD.P aggregation (population standard deviation)"""
        self.setup_aggregation_data()

        # Single bucket: std.p of [1,2,3,4,5,6]
        # Population std dev = sqrt(sum((x-mean)^2)/N)
        # mean = 3.5, variance = ((1-3.5)^2 + (2-3.5)^2 + ... + (6-3.5)^2) / 6
        # = (6.25 + 2.25 + 0.25 + 0.25 + 2.25 + 6.25) / 6 = 17.5 / 6 ≈ 2.917
        # std.p = sqrt(2.917) ≈ 1.708
        result = self.client.execute_command('TS.RANGE', 'agg_test', 0, 7000,
                                             'AGGREGATION', 'STD.P', 7000)
        assert len(result) == 1
        assert float(result[0][1]) == pytest.approx(1.708, abs=0.01)

    def test_std_s_aggregation(self):
        """Test STD.S aggregation (sample standard deviation)"""
        self.setup_aggregation_data()

        # Single bucket: std.s of [1,2,3,4,5,6]
        # Sample std dev = sqrt(sum((x-mean)^2)/(N-1))
        # Using the same variance calculation as above but divided by (N-1)
        # std.s = sqrt(17.5 / 5) = sqrt(3.5) ≈ 1.871
        result = self.client.execute_command('TS.RANGE', 'agg_test', 0, 7000,
                                             'AGGREGATION', 'STD.S', 7000)
        assert len(result) == 1
        assert float(result[0][1]) == pytest.approx(1.871, abs=0.01)

    def test_var_p_aggregation(self):
        """Test VAR.P aggregation (population variance)"""
        self.setup_aggregation_data()

        # Single bucket: var.p of [1,2,3,4,5,6]
        # Population variance = sum((x-mean)^2)/N = 17.5 / 6 ≈ 2.917
        result = self.client.execute_command('TS.RANGE', 'agg_test', 0, 7000,
                                             'AGGREGATION', 'VAR.P', 7000)
        assert len(result) == 1
        assert float(result[0][1]) == pytest.approx(2.917, abs=0.01)

    def test_var_s_aggregation(self):
        """Test VAR.S aggregation (sample variance)"""
        self.setup_aggregation_data()

        # Single bucket: var.s of [1,2,3,4,5,6]
        # Sample variance = sum((x-mean)^2)/(N-1) = 17.5 / 5 = 3.5
        result = self.client.execute_command('TS.RANGE', 'agg_test', 0, 7000,
                                             'AGGREGATION', 'VAR.S', 7000)
        assert len(result) == 1
        assert float(result[0][1]) == pytest.approx(3.5, abs=0.01)

    def test_all_aggregation_all_true_single_bucket(self):
        """
        ALL aggregator: when all samples in the bucket are "true" (non-zero),
        the aggregated value should be 1.
        """
        self.client.execute_command('TS.CREATE', 'all_true')

        self.client.execute_command('TS.ADD', 'all_true', 1000, 1)
        self.client.execute_command('TS.ADD', 'all_true', 2000, 1)
        self.client.execute_command('TS.ADD', 'all_true', 3000, 2)

        result = self.client.execute_command(
            'TS.RANGE', 'all_true', 0, 5000,
            'ALIGN', 0,
            'AGGREGATION', 'ALL(>=1)', 5000,
            'BUCKETTIMESTAMP', 'START'
        )

        print("All result", result)
        assert len(result) == 1
        assert result[0][0] == 0
        assert float(result[0][1]) == pytest.approx(1.0)

    def test_all_aggregation_false(self):
        """
        ALL aggregator: if any sample in the bucket is zero, aggregated value should be 0.
        """
        self.client.execute_command('TS.CREATE', 'all_has_zero')

        self.client.execute_command('TS.ADD', 'all_has_zero', 1000, 1)
        self.client.execute_command('TS.ADD', 'all_has_zero', 2000, 0)
        self.client.execute_command('TS.ADD', 'all_has_zero', 3000, 1)

        result = self.client.execute_command(
            'TS.RANGE', 'all_has_zero', 0, 5000,
            'ALIGN', 0,
            'AGGREGATION', 'ALL(!=0)', 5000,
            'BUCKETTIMESTAMP', 'START'
        )

        print("All with zero result", result)
        assert len(result) == 1
        assert result[0][0] == 0
        assert float(result[0][1]) == pytest.approx(0.0)

    def test_all_aggregation_multiple_buckets(self):
        """
        ALL aggregator across multiple buckets to ensure per-bucket behavior.
        """
        self.client.execute_command('TS.CREATE', 'all_multi_bucket')

        self.client.execute_command('TS.ADD', 'all_multi_bucket', 1000, 50)
        self.client.execute_command('TS.ADD', 'all_multi_bucket', 2000, 1000)
        self.client.execute_command('TS.ADD', 'all_multi_bucket', 3000, 200)
        self.client.execute_command('TS.ADD', 'all_multi_bucket', 5000, 210)

        result = self.client.execute_command(
            'TS.RANGE', 'all_multi_bucket', 0, 6000,
            'ALIGN', 0,
            'AGGREGATION', 'ALL(<500)', 2000,
            'BUCKETTIMESTAMP', 'START'
        )

        assert len(result) == 3
        by_ts = {ts: float(val) for ts, val in result}
        assert by_ts[0] == pytest.approx(1.0)
        assert by_ts[2000] == pytest.approx(0.0)
        assert by_ts[4000] == pytest.approx(1.0)

    def test_any_aggregation_any_true_single_bucket(self):
        """
        ANY aggregator: when at least one sample in the bucket matches the condition,
        the aggregated value should be 1.
        """
        self.client.execute_command('TS.CREATE', 'any_true')

        self.client.execute_command('TS.ADD', 'any_true', 1000, 0)
        self.client.execute_command('TS.ADD', 'any_true', 2000, 0)
        self.client.execute_command('TS.ADD', 'any_true', 3000, 2)

        result = self.client.execute_command(
            'TS.RANGE', 'any_true', 0, 5000,
            'ALIGN', 0,
            'AGGREGATION', 'ANY(>=1)', 5000,
            'BUCKETTIMESTAMP', 'START'
        )

        assert len(result) == 1
        assert result[0][0] == 0
        assert float(result[0][1]) == 1.0

    def test_any_aggregation_none_true_single_bucket(self):
        """
        ANY aggregator: when no samples in the bucket match the condition,
        the aggregated value should be 0.
        """
        self.client.execute_command('TS.CREATE', 'any_false')

        self.client.execute_command('TS.ADD', 'any_false', 1000, 0)
        self.client.execute_command('TS.ADD', 'any_false', 2000, 0)
        self.client.execute_command('TS.ADD', 'any_false', 3000, 0)

        result = self.client.execute_command(
            'TS.RANGE', 'any_false', 0, 5000,
            'ALIGN', 0,
            'AGGREGATION', 'ANY(!=0)', 5000,
            'BUCKETTIMESTAMP', 'START'
        )

        assert len(result) == 1
        assert result[0][0] == 0
        assert float(result[0][1]) == 0.0

    def test_any_aggregation_multiple_buckets(self):
        """
        ANY aggregator across multiple buckets to ensure per-bucket behavior.
        """
        self.client.execute_command('TS.CREATE', 'any_multi_bucket')

        # bucket=2000ms, ALIGN 0 => buckets start at 0,2000,4000
        # bucket 0: values [0] -> ANY(v>0)=0
        # bucket 2000: values [1,0] -> ANY(v>0)=1
        # bucket 4000: values [0] -> ANY(v>0)=0
        self.client.execute_command('TS.ADD', 'any_multi_bucket', 1000, 0)
        self.client.execute_command('TS.ADD', 'any_multi_bucket', 2000, 1)
        self.client.execute_command('TS.ADD', 'any_multi_bucket', 3000, 0)
        self.client.execute_command('TS.ADD', 'any_multi_bucket', 5000, 0)

        result = self.client.execute_command(
            'TS.RANGE', 'any_multi_bucket', 0, 6000,
            'ALIGN', 0,
            'AGGREGATION', 'ANY(>0)', 2000,
            'BUCKETTIMESTAMP', 'START'
        )

        assert len(result) == 3
        by_ts = {ts: float(val) for ts, val in result}
        assert by_ts[0] == pytest.approx(0.0)
        assert by_ts[2000] == pytest.approx(1.0)
        assert by_ts[4000] == pytest.approx(0.0)

    def test_sumif_aggregation_all_match(self):
        """
        SUMIF aggregator: sum of samples that match the inline condition.
        When all samples match, should equal regular SUM.
        """
        self.client.execute_command('TS.CREATE', 'sumif_all')

        self.client.execute_command('TS.ADD', 'sumif_all', 1000, 10)
        self.client.execute_command('TS.ADD', 'sumif_all', 2000, 20)
        self.client.execute_command('TS.ADD', 'sumif_all', 3000, 30)

        result = self.client.execute_command(
            'TS.RANGE', 'sumif_all', 0, 4000,
            'ALIGN', 0,
            'AGGREGATION', 'SUM(>0)', 4000,
            'BUCKETTIMESTAMP', 'START'
        )

        assert len(result) == 1
        assert result[0][0] == 0
        assert float(result[0][1]) == pytest.approx(60.0)  # 10 + 20 + 30

    def test_sumif_aggregation_none_match(self):
        """
        SUMIF aggregator: when no samples match the condition, sum should be 0.
        """
        self.client.execute_command('TS.CREATE', 'sumif_none')

        self.client.execute_command('TS.ADD', 'sumif_none', 1000, 1)
        self.client.execute_command('TS.ADD', 'sumif_none', 2000, 2)
        self.client.execute_command('TS.ADD', 'sumif_none', 3000, 3)

        result = self.client.execute_command(
            'TS.RANGE', 'sumif_none', 0, 4000,
            'ALIGN', 0,
            'AGGREGATION', 'SUMIF(>10)', 4000,
            'BUCKETTIMESTAMP', 'START'
        )

        assert len(result) == 1
        assert result[0][0] == 0
        assert float(result[0][1]) == pytest.approx(0.0)

    def test_sumif_aggregation_mixed(self):
        """
        SUMIF aggregator: sum only values matching the condition (> 5).
        """
        self.client.execute_command('TS.CREATE', 'sumif_mixed')

        self.client.execute_command('TS.ADD', 'sumif_mixed', 1000, 3)
        self.client.execute_command('TS.ADD', 'sumif_mixed', 2000, 10)
        self.client.execute_command('TS.ADD', 'sumif_mixed', 3000, 2)
        self.client.execute_command('TS.ADD', 'sumif_mixed', 4000, 15)

        result = self.client.execute_command(
            'TS.RANGE', 'sumif_mixed', 0, 5000,
            'ALIGN', 0,
            'AGGREGATION', 'SUM(>5)', 5000,
            'BUCKETTIMESTAMP', 'START'
        )

        assert len(result) == 1
        assert result[0][0] == 0
        assert float(result[0][1]) == pytest.approx(25.0)  # 10 + 15

    def test_sumif_aggregation_multiple_buckets(self):
        """
        SUMIF aggregator across multiple buckets to ensure per-bucket behavior.
        """
        self.client.execute_command('TS.CREATE', 'sumif_multi')

        # bucket=2000ms, ALIGN 0 => buckets start at 0, 2000, 4000
        # bucket 0: [5, 3] => sumif(v>=5) = 5
        # bucket 2000: [10, 2, 8] => sumif(v>=5) = 18
        # bucket 4000: [1] => sumif(v>=5) = 0
        self.client.execute_command('TS.ADD', 'sumif_multi', 1000, 5)
        self.client.execute_command('TS.ADD', 'sumif_multi', 1500, 3)
        self.client.execute_command('TS.ADD', 'sumif_multi', 2000, 10)
        self.client.execute_command('TS.ADD', 'sumif_multi', 3000, 2)
        self.client.execute_command('TS.ADD', 'sumif_multi', 3500, 8)
        self.client.execute_command('TS.ADD', 'sumif_multi', 5000, 1)

        result = self.client.execute_command(
            'TS.RANGE', 'sumif_multi', 0, 6000,
            'ALIGN', 0,
            'AGGREGATION', 'SUM(>=5)', 2000,
            'BUCKETTIMESTAMP', 'START'
        )

        assert len(result) == 3
        by_ts = {ts: float(val) for ts, val in result}
        assert by_ts[0] == pytest.approx(5.0)
        assert by_ts[2000] == pytest.approx(18.0)
        assert by_ts[4000] == pytest.approx(0.0)

    def test_countif_aggregation_all_match(self):
        """
        COUNTIF aggregator: count of samples that match the inline condition.
        When all samples match, should equal regular COUNT.
        """
        self.client.execute_command('TS.CREATE', 'countif_all')

        self.client.execute_command('TS.ADD', 'countif_all', 1000, 10)
        self.client.execute_command('TS.ADD', 'countif_all', 2000, 20)
        self.client.execute_command('TS.ADD', 'countif_all', 3000, 30)

        result = self.client.execute_command(
            'TS.RANGE', 'countif_all', 0, 4000,
            'ALIGN', 0,
            'AGGREGATION', 'COUNTIF(>0)', 4000,
            'BUCKETTIMESTAMP', 'START'
        )

        assert len(result) == 1
        assert result[0][0] == 0
        assert float(result[0][1]) == pytest.approx(3.0)

    def test_countif_aggregation_none_match(self):
        """
        COUNTIF aggregator: when no samples match the condition, count should be 0.
        """
        self.client.execute_command('TS.CREATE', 'countif_none')

        self.client.execute_command('TS.ADD', 'countif_none', 1000, 1)
        self.client.execute_command('TS.ADD', 'countif_none', 2000, 2)
        self.client.execute_command('TS.ADD', 'countif_none', 3000, 3)

        result = self.client.execute_command(
            'TS.RANGE', 'countif_none', 0, 4000,
            'ALIGN', 0,
            'AGGREGATION', 'COUNT(>10)', 4000,
            'BUCKETTIMESTAMP', 'START'
        )

        assert len(result) == 1
        assert result[0][0] == 0
        assert float(result[0][1]) == pytest.approx(0.0)

    def test_countif_aggregation_mixed(self):
        """
        COUNTIF aggregator: count only values matching the condition (<= 5).
        """
        self.client.execute_command('TS.CREATE', 'countif_mixed')

        self.client.execute_command('TS.ADD', 'countif_mixed', 1000, 3)
        self.client.execute_command('TS.ADD', 'countif_mixed', 2000, 10)
        self.client.execute_command('TS.ADD', 'countif_mixed', 3000, 2)
        self.client.execute_command('TS.ADD', 'countif_mixed', 4000, 15)

        result = self.client.execute_command(
            'TS.RANGE', 'countif_mixed', 0, 5000,
            'ALIGN', 0,
            'AGGREGATION', 'COUNT(<=5)', 5000,
            'BUCKETTIMESTAMP', 'START'
        )

        assert len(result) == 1
        assert result[0][0] == 0
        assert float(result[0][1]) == pytest.approx(2.0)  # 3 and 2

    def test_countif_aggregation_multiple_buckets(self):
        """
        COUNTIF aggregator across multiple buckets to ensure per-bucket behavior.
        """
        self.client.execute_command('TS.CREATE', 'countif_multi')

        # bucket=2000ms, ALIGN 0 => buckets start at 0, 2000, 4000
        # bucket 0: [5, 3] => countif(v>=5) = 1
        # bucket 2000: [10, 2, 8] => countif(v>=5) = 2
        # bucket 4000: [1, 6] => countif(v>=5) = 1
        self.client.execute_command('TS.ADD', 'countif_multi', 1000, 5)
        self.client.execute_command('TS.ADD', 'countif_multi', 1500, 3)
        self.client.execute_command('TS.ADD', 'countif_multi', 2000, 10)
        self.client.execute_command('TS.ADD', 'countif_multi', 3000, 2)
        self.client.execute_command('TS.ADD', 'countif_multi', 3500, 8)
        self.client.execute_command('TS.ADD', 'countif_multi', 5000, 1)
        self.client.execute_command('TS.ADD', 'countif_multi', 5500, 6)

        result = self.client.execute_command(
            'TS.RANGE', 'countif_multi', 0, 6000,
            'ALIGN', 0,
            'AGGREGATION', 'COUNTIF(>=5)', 2000,
            'BUCKETTIMESTAMP', 'START'
        )

        assert len(result) == 3
        by_ts = {ts: float(val) for ts, val in result}
        assert by_ts[0] == pytest.approx(1.0)
        assert by_ts[2000] == pytest.approx(2.0)
        assert by_ts[4000] == pytest.approx(1.0)

    def test_countif_aggregation_with_different_operators(self):
        """
        COUNTIF aggregator: test with different comparison operators.
        """
        self.client.execute_command('TS.CREATE', 'countif_ops')

        self.client.execute_command('TS.ADD', 'countif_ops', 1000, 5)
        self.client.execute_command('TS.ADD', 'countif_ops', 2000, 5)
        self.client.execute_command('TS.ADD', 'countif_ops', 3000, 10)

        # Test equality
        result_eq = self.client.execute_command(
            'TS.RANGE', 'countif_ops', 0, 4000,
            'AGGREGATION', 'COUNTIF(==5)', 4000
        )
        assert float(result_eq[0][1]) == pytest.approx(2.0)

        # Test not equal
        result_neq = self.client.execute_command(
            'TS.RANGE', 'countif_ops', 0, 4000,
            'AGGREGATION', 'COUNTIF(!=5)', 4000
        )
        assert float(result_neq[0][1]) == pytest.approx(1.0)

        # Test less than
        result_lt = self.client.execute_command(
            'TS.RANGE', 'countif_ops', 0, 4000,
            'AGGREGATION', 'COUNTIF(<10)', 4000
        )
        assert float(result_lt[0][1]) == pytest.approx(2.0)

    def test_aggregation_single_value_bucket(self):
        """Test aggregation with buckets containing single values"""
        self.setup_aggregation_data()

        # Use a small bucket size so each contains only one value
        result = self.client.execute_command('TS.RANGE', 'agg_test', 0, 7000,
                                             'AGGREGATION', 'AVG', 1000, 'ALIGN', 0)

        # Each bucket should contain one value, so AVG should equal the value
        assert len(result) == 6
        for i, bucket in enumerate(result):
            assert float(bucket[1]) == pytest.approx(float(i + 1))

    def test_aggregation_with_bucket_timestamps(self):
        """Test aggregation with different BUCKETTIMESTAMP options"""
        self.setup_aggregation_data()

        # Test with START bucket timestamp
        result_start = self.client.execute_command('TS.RANGE', 'agg_test', 0, 7000,
                                                   'AGGREGATION', 'SUM', 3000, 'ALIGN', 0,
                                                   'BUCKETTIMESTAMP', 'START')

        # Test with MID bucket timestamp
        result_mid = self.client.execute_command('TS.RANGE', 'agg_test', 0, 7000,
                                                 'AGGREGATION', 'SUM', 3000, 'ALIGN', 0,
                                                 'BUCKETTIMESTAMP', 'MID')

        # Test with END bucket timestamp
        result_end = self.client.execute_command('TS.RANGE', 'agg_test', 0, 7000,
                                                 'AGGREGATION', 'SUM', 3000, 'ALIGN', 0,
                                                 'BUCKETTIMESTAMP', 'END')

        # Values should be the same, timestamps should differ
        assert len(result_start) == len(result_mid) == len(result_end) == 3

        # Check that timestamps are different but values are the same
        for i in range(len(result_start)):
            assert float(result_start[i][1]) == float(result_mid[i][1]) == float(result_end[i][1])
            # Timestamps should be different (exact values depend on bucket alignment)
            assert result_start[i][0] != result_mid[i][0] or result_mid[i][0] != result_end[i][0]

    @pytest.mark.parametrize("agg_type,expected_single_bucket", [
        ('AVG', 3.5),
        ('SUM', 21.0),
        ('MIN', 1.0),
        ('MAX', 6.0),
        ('COUNT', 6.0),
        ('FIRST', 1.0),
        ('LAST', 6.0),
        ('RANGE', 5.0),
        ('STD.P', 1.708),
        ('STD.S', 1.871),
        ('VAR.P', 2.917),
        ('VAR.S', 3.5),
    ])
    def test_all_aggregation_types_parametrized(self, agg_type, expected_single_bucket):
        """Parametrized test for all aggregation types with a single bucket"""
        self.setup_aggregation_data()

        result = self.client.execute_command('TS.RANGE', 'agg_test', 0, 7000,
                                             'AGGREGATION', agg_type, 7000)

        assert len(result) == 1
        if agg_type in ['STD.P', 'STD.S', 'VAR.P', 'VAR.S']:
            assert float(result[0][1]) == pytest.approx(expected_single_bucket, abs=0.01)
        else:
            assert float(result[0][1]) == pytest.approx(expected_single_bucket)

    def test_none_aggregation_none_match_single_bucket(self):
        """
        NONE aggregator: when *no* samples in the bucket match the condition,
        the aggregated value should be 1.
        """
        self.client.execute_command('TS.CREATE', 'none_true')

        self.client.execute_command('TS.ADD', 'none_true', 1000, 0)
        self.client.execute_command('TS.ADD', 'none_true', 2000, 0)
        self.client.execute_command('TS.ADD', 'none_true', 3000, 0)

        result = self.client.execute_command(
            'TS.RANGE', 'none_true', 0, 5000,
            'ALIGN', 0,
            'AGGREGATION', 'NONE(>0)', 5000,
            'BUCKETTIMESTAMP', 'START'
        )

        assert len(result) == 1
        assert result[0][0] == 0
        assert float(result[0][1]) == pytest.approx(1.0)

    def test_none_aggregation_some_match_single_bucket(self):
        """
        NONE aggregator: when at least one sample in the bucket matches the condition,
        the aggregated value should be 0.
        """
        self.client.execute_command('TS.CREATE', 'none_false')

        self.client.execute_command('TS.ADD', 'none_false', 1000, 0)
        self.client.execute_command('TS.ADD', 'none_false', 2000, 2)
        self.client.execute_command('TS.ADD', 'none_false', 3000, 0)

        result = self.client.execute_command(
            'TS.RANGE', 'none_false', 0, 5000,
            'ALIGN', 0,
            'AGGREGATION', 'NONE(>=1)', 5000,
            'BUCKETTIMESTAMP', 'START'
        )

        assert len(result) == 1
        assert result[0][0] == 0
        assert float(result[0][1]) == pytest.approx(0.0)

    def test_none_aggregation_multiple_buckets(self):
        """
        NONE aggregator across multiple buckets to ensure per-bucket behavior.
        """
        self.client.execute_command('TS.CREATE', 'none_multi_bucket')

        # bucket=2000ms, ALIGN 0 => buckets start at 0,2000,4000
        # bucket 0: values [0]      -> NONE(v>0)=1
        # bucket 2000: values [1,0] -> NONE(v>0)=0
        # bucket 4000: values [0]   -> NONE(v>0)=1
        self.client.execute_command('TS.ADD', 'none_multi_bucket', 1000, 0)
        self.client.execute_command('TS.ADD', 'none_multi_bucket', 2000, 1)
        self.client.execute_command('TS.ADD', 'none_multi_bucket', 3000, 0)
        self.client.execute_command('TS.ADD', 'none_multi_bucket', 5000, 0)

        result = self.client.execute_command(
            'TS.RANGE', 'none_multi_bucket', 0, 6000,
            'ALIGN', 0,
            'AGGREGATION', 'NONE(>0)', 2000,
            'BUCKETTIMESTAMP', 'START'
        )

        assert len(result) == 3
        by_ts = {ts: float(val) for ts, val in result}
        assert by_ts[0] == 1.0
        assert by_ts[2000] == 0.0
        assert by_ts[4000] == 1.0

    def test_share_aggregation_all_match_single_bucket(self):
        """
        SHARE aggregator: share of samples that match the inline condition in the bucket.
        If all samples match, share should be 1.
        """
        self.client.execute_command('TS.CREATE', 'share_all')

        self.client.execute_command('TS.ADD', 'share_all', 1000, 1)
        self.client.execute_command('TS.ADD', 'share_all', 2000, 2)
        self.client.execute_command('TS.ADD', 'share_all', 3000, 3)

        result = self.client.execute_command(
            'TS.RANGE', 'share_all', 0, 5000,
            'ALIGN', 0,
            'AGGREGATION', 'SHARE(>0)', 5000,
            'BUCKETTIMESTAMP', 'START'
        )

        assert len(result) == 1
        assert result[0][0] == 0
        assert float(result[0][1]) == pytest.approx(1.0)

    def test_share_aggregation_none_match_single_bucket(self):
        """
        SHARE aggregator: if no samples match, share should be 0.
        """
        self.client.execute_command('TS.CREATE', 'share_none')

        self.client.execute_command('TS.ADD', 'share_none', 1000, 0)
        self.client.execute_command('TS.ADD', 'share_none', 2000, 0)
        self.client.execute_command('TS.ADD', 'share_none', 3000, 0)

        result = self.client.execute_command(
            'TS.RANGE', 'share_none', 0, 5000,
            'ALIGN', 0,
            'AGGREGATION', 'SHARE(!=0)', 5000,
            'BUCKETTIMESTAMP', 'START'
        )

        assert len(result) == 1
        assert result[0][0] == 0
        assert float(result[0][1]) == pytest.approx(0.0)

    def test_share_aggregation_mixed_single_bucket(self):
        """
        SHARE aggregator: validate ratio for a mixed bucket (2 matching out of 4 => 0.5).
        """
        self.client.execute_command('TS.CREATE', 'share_mixed')

        self.client.execute_command('TS.ADD', 'share_mixed', 1000, 1)
        self.client.execute_command('TS.ADD', 'share_mixed', 2000, 0)
        self.client.execute_command('TS.ADD', 'share_mixed', 3000, 2)
        self.client.execute_command('TS.ADD', 'share_mixed', 4000, 0)

        result = self.client.execute_command(
            'TS.RANGE', 'share_mixed', 0, 5000,
            'ALIGN', 0,
            'AGGREGATION', 'SHARE(>0)', 5000,
            'BUCKETTIMESTAMP', 'START'
        )

        assert len(result) == 1
        assert result[0][0] == 0
        assert float(result[0][1]) == pytest.approx(0.5)

    def test_share_aggregation_multiple_buckets(self):
        """
        SHARE aggregator across multiple buckets to ensure per-bucket behavior.
        """
        self.client.execute_command('TS.CREATE', 'share_multi_bucket')

        # bucket=2000ms, ALIGN 0 => buckets start at 0,2000,4000
        # bucket 0: [1]           => share(v>0)=1.0
        # bucket 2000: [0,2]      => share(v>0)=0.5
        # bucket 4000: [0]        => share(v>0)=0.0
        self.client.execute_command('TS.ADD', 'share_multi_bucket', 1000, 1)
        self.client.execute_command('TS.ADD', 'share_multi_bucket', 2000, 0)
        self.client.execute_command('TS.ADD', 'share_multi_bucket', 3000, 2)
        self.client.execute_command('TS.ADD', 'share_multi_bucket', 5000, 0)

        result = self.client.execute_command(
            'TS.RANGE', 'share_multi_bucket', 0, 6000,
            'ALIGN', 0,
            'AGGREGATION', 'SHARE(>0)', 2000,
            'BUCKETTIMESTAMP', 'START'
        )

        assert len(result) == 3
        by_ts = {ts: float(val) for ts, val in result}
        assert by_ts[0] == pytest.approx(1.0)
        assert by_ts[2000] == pytest.approx(0.5)
        assert by_ts[4000] == pytest.approx(0.0)

    def test_countall_aggregation_counts_all_samples_including_nan(self):
        """
        COUNTALL aggregator: counts all samples in the bucket, including NaN values.
        """
        self.client.execute_command('TS.CREATE', 'countall_test')

        self.client.execute_command('TS.ADD', 'countall_test', 1000, 1.0)
        self.client.execute_command('TS.ADD', 'countall_test', 2000, 'nan')
        self.client.execute_command('TS.ADD', 'countall_test', 3000, 2.0)
        self.client.execute_command('TS.ADD', 'countall_test', 4000, 'nan')

        result = self.client.execute_command(
            'TS.RANGE', 'countall_test', 0, 5000,
            'ALIGN', 0,
            'AGGREGATION', 'COUNTALL', 5000,
            'BUCKETTIMESTAMP', 'START'
        )

        assert len(result) == 1
        assert result[0][0] == 0
        assert float(result[0][1]) == pytest.approx(4.0)

    def test_countnan_aggregation_counts_only_nan_samples(self):
        """
        COUNTNAN aggregator: counts only NaN samples in the bucket.
        """
        self.client.execute_command('TS.CREATE', 'countnan_test')

        self.client.execute_command('TS.ADD', 'countnan_test', 1000, 1.0)
        self.client.execute_command('TS.ADD', 'countnan_test', 2000, 'nan')
        self.client.execute_command('TS.ADD', 'countnan_test', 3000, 2.0)
        self.client.execute_command('TS.ADD', 'countnan_test', 4000, 'nan')
        self.client.execute_command('TS.ADD', 'countnan_test', 5000, 3.0)

        result = self.client.execute_command(
            'TS.RANGE', 'countnan_test', 0, 6000,
            'ALIGN', 0,
            'AGGREGATION', 'COUNTNAN', 6000,
            'BUCKETTIMESTAMP', 'START'
        )

        assert len(result) == 1
        assert result[0][0] == 0
        assert float(result[0][1]) == pytest.approx(2.0)

    @pytest.mark.parametrize(
        "agg_type, expected",
        [
            ("COUNTALL", 5.0),
            ("COUNTNAN", 3.0),
            ("AVG", 15.0),
            ("SUM", 30.0),
            ("FIRST", 10.0),
            ("LAST", 20.0),
        ],
    )
    def test_nan_values_with_different_aggregations(self, agg_type, expected):
        """TS.RANGE aggregation should handle NaN samples consistently across aggregators."""

        self.client.execute_command('TS.CREATE', 'ts_nan_aggs')
        self.client.execute_command('TS.ADD', 'ts_nan_aggs', 1000, 'nan')
        self.client.execute_command('TS.ADD', 'ts_nan_aggs', 2000, 10.0)
        self.client.execute_command('TS.ADD', 'ts_nan_aggs', 3000, 'nan')
        self.client.execute_command('TS.ADD', 'ts_nan_aggs', 4000, 20.0)
        self.client.execute_command('TS.ADD', 'ts_nan_aggs', 5000, 'nan')

        result = self.client.execute_command(
            'TS.RANGE', 'ts_nan_aggs', 0, 6000,
            'ALIGN', 0,
            'AGGREGATION', agg_type, 6000,
            'BUCKETTIMESTAMP', 'START'
        )

        print("Result for", agg_type, result)
        assert len(result) == 1
        assert result[0][0] == 0

        actual = float(result[0][1])
        assert actual == pytest.approx(expected), f"Unexpected value for {agg_type}"

    @pytest.mark.parametrize(
        "agg_type, expected",
        [
            # Aggregators that count NaNs take the samples, so the bucket is real and
            # reports the true count.
            ("COUNTALL", 3.0),
            ("COUNTNAN", 3.0),
            # Every NaN-ignoring aggregator took nothing from this bucket, so without
            # EMPTY there is no bucket to report. `None` means "omitted", not "NaN".
            ("COUNT", None),
            ("SUM", None),
            ("AVG", None),
            ("MIN", None),
            ("MAX", None),
            ("FIRST", None),
            ("LAST", None),
            ("STD.P", None),
            ("STD.S", None),
            ("VAR.P", None),
            ("VAR.S", None),
            ("INCREASE", None),
            ("RATE", None),
            ("IRATE", None),
        ],
    )
    def test_all_nan_values(self, agg_type, expected):
        """
        Aggregation over a bucket holding only NaN samples, without EMPTY.

        Bucket emission is per-aggregator: the bucket is returned iff that aggregation
        accepted a sample from it. COUNTALL/COUNTNAN accept NaNs and report the count;
        every other aggregator ignores them and the bucket is omitted entirely.
        Reference-checked against RedisTimeSeries 8.6.
        """
        self.client.execute_command('TS.CREATE', 'ts_all_nan')

        # Add only NaN samples
        self.client.execute_command('TS.ADD', 'ts_all_nan', 1000, 'nan')
        self.client.execute_command('TS.ADD', 'ts_all_nan', 2000, 'nan')
        self.client.execute_command('TS.ADD', 'ts_all_nan', 3000, 'nan')

        result = self.client.execute_command(
            'TS.RANGE', 'ts_all_nan', 0, 4000,
            'ALIGN', 0,
            'AGGREGATION', agg_type, 4000,
            'BUCKETTIMESTAMP', 'START'
        )

        if expected is None:
            assert result == [], f"Expected {agg_type} to omit the all-NaN bucket, got {result}"
            return

        assert len(result) == 1
        assert result[0][0] == 0
        actual = float(result[0][1])
        assert actual == pytest.approx(expected), f"Unexpected value for {agg_type}"

    @pytest.mark.parametrize("agg_type", ["COUNTNAN", "AVG", "COUNTALL"])
    def test_ordinary_bucket_omitted_for_countnan(self, agg_type):
        """
        The converse of test_all_nan_values: a bucket of ordinary readings is omitted for
        COUNTNAN, which accepts only NaNs, while the others report it. Emission tracks the
        aggregator, not the bucket, in both directions. Reference-checked.
        """
        self.client.execute_command('TS.CREATE', 'ts_ordinary')
        self.client.execute_command('TS.ADD', 'ts_ordinary', 1000, 1.0)
        self.client.execute_command('TS.ADD', 'ts_ordinary', 2000, 2.0)

        result = self.client.execute_command(
            'TS.RANGE', 'ts_ordinary', 0, 4000,
            'ALIGN', 0,
            'AGGREGATION', agg_type, 4000,
            'BUCKETTIMESTAMP', 'START'
        )

        if agg_type == "COUNTNAN":
            assert result == [], f"Expected COUNTNAN to omit an all-ordinary bucket, got {result}"
        else:
            assert len(result) == 1, f"Expected {agg_type} to report the bucket, got {result}"

    @pytest.mark.parametrize(
        "agg_type, expect_value",
        [
            ("COUNTALL", 1.0),
            ("COUNTNAN", 1.0),
            ("COUNT", 0.0),
            ("SUM", 0.0),
            ("AVG", math.nan),
            ("MIN", math.nan),
            ("MAX", math.nan),
            ("FIRST", math.nan),
            ("LAST", math.nan),
            ("STD.P", math.nan),
            ("STD.S", math.nan),
            ("VAR.P", math.nan),
            ("VAR.S", math.nan),
            ("INCREASE", math.nan),
            ("RATE", math.nan),
            ("IRATE", math.nan),
        ],
    )
    def test_all_nan_values_with_empty_option(self, agg_type, expect_value):
        """
        Verify aggregations' behavior when every bucket contains only NaN samples and
        the EMPTY option is provided (buckets should be emitted).
        """
        self.client.execute_command('TS.CREATE', 'ts_all_nan_empty')

        # Place one NaN sample in each 2s bucket: timestamps 1000, 3000, 5000
        self.client.execute_command('TS.ADD', 'ts_all_nan_empty', 1000, 'nan')
        self.client.execute_command('TS.ADD', 'ts_all_nan_empty', 3000, 'nan')
        self.client.execute_command('TS.ADD', 'ts_all_nan_empty', 5000, 'nan')

        # Range 0..6000 with bucket=2000 -> buckets start at 0,2000,4000 (three buckets)
        result = self.client.execute_command(
            'TS.RANGE', 'ts_all_nan_empty', 0, 6000,
            'ALIGN', 0,
            'AGGREGATION', agg_type, 2000,
            'BUCKETTIMESTAMP', 'START',
            'EMPTY'
        )

        # Expect three emitted buckets (one per 2s bucket)
        assert len(result) == 3

        # Validate each bucket according to aggregator semantics
        for ts, val in result:
            actual = float(val)
            if math.isnan(expect_value):
                # Expect NaN value
                assert math.isnan(actual), f"Expected NaN for {agg_type}, got {actual}"
            else:
                # each bucket contains one sample (NaN) -> count all = 1, sum=0, count=0, etc.
                assert actual == pytest.approx(expect_value), f"Unexpected value for {agg_type}"

    def test_range_edge_cases(self):
        """Test TS.RANGE with edge case timestamps"""

        self.setup_data()

        # Exact start/end match
        result = self.client.execute_command('TS.RANGE', 'ts1', 2000, 2000)
        assert result == [[2000, b'20.2']]

        # Range before first sample
        result = self.client.execute_command('TS.RANGE', 'ts1', 0, 500)
        assert result == []

        # Range after the last sample
        result = self.client.execute_command('TS.RANGE', 'ts1', 6000, 7000)
        assert result == []

        # Range partially overlapping
        result = self.client.execute_command('TS.RANGE', 'ts1', 4500, 5500)
        assert result == [[5000, b'50.5']]

    def test_range_error_handling(self):
        """Test error conditions for TS.RANGE"""
        # Wrong number of arguments
        with pytest.raises(ResponseError, match="wrong number of arguments"):
            self.client.execute_command('TS.RANGE', 'ts1')

        with pytest.raises(ResponseError, match="wrong number of arguments"):
            self.client.execute_command('TS.RANGE', 'ts1', '-')

        # Invalid start/end timestamp (RTS wording; see tests/compat)
        with pytest.raises(ResponseError, match="wrong fromTimestamp"):
            self.client.execute_command('TS.RANGE', 'ts1', 'invalid', '+')

        with pytest.raises(ResponseError, match="wrong toTimestamp"):
            self.client.execute_command('TS.RANGE', 'ts1', '-', 'invalid')

        # A bare negative integer is an absolute timestamp, not a relative
        # offset, and is rejected as one.
        with pytest.raises(ResponseError, match="wrong fromTimestamp"):
            self.client.execute_command('TS.RANGE', 'ts1', -1000, 1000)

        # Invalid filter values
        with pytest.raises(ResponseError, match="TSDB: Couldn't parse MIN"):
            self.client.execute_command('TS.RANGE', 'ts1', '-', '+', 'FILTER_BY_VALUE', 'a', 'b')

        with pytest.raises(ResponseError, match="TSDB: Couldn't parse MAX"):
            self.client.execute_command('TS.RANGE', 'ts1', '-', '+', 'FILTER_BY_VALUE', 1000, 'b')

    def test_aggregation_condition_errors(self):
        """Filtered aggregators require an inline (op value) condition;
        non-filtered aggregators must not be given one."""
        self.client.execute_command('TS.CREATE', 'ts1')
        self.client.execute_command('TS.ADD', 'ts1', 1000, 10.0)

        # countif/sumif/all/any/none/share require a condition
        for agg in ['countif', 'sumif', 'all', 'any', 'none', 'share']:
            with pytest.raises(ResponseError, match="TSDB: missing condition for aggregator"):
                self.client.execute_command(
                    'TS.RANGE', 'ts1', '-', '+', 'AGGREGATION', agg, 1000)

        # a condition attached to an aggregator that doesn't support one is an error
        for agg in ['avg', 'min', 'max', 'first', 'last', 'range', 'std.p']:
            with pytest.raises(ResponseError, match="TSDB: aggregation type does not support a filter condition"):
                self.client.execute_command(
                    'TS.RANGE', 'ts1', '-', '+', 'AGGREGATION', f'{agg}(>5)', 1000)

        # count/sum accept an inline condition optionally
        for agg in ['count', 'sum']:
            result = self.client.execute_command(
                'TS.RANGE', 'ts1', '-', '+', 'AGGREGATION', f'{agg}(>5)', 1000)
            assert len(result) == 1
