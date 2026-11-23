import pytest
import time
from valkey import ResponseError
from valkeytestframework.util.waiters import *
from valkeytestframework.conftest import resource_port_tracker
from valkey_timeseries_test_case import ValkeyTimeSeriesTestCaseBase


class TestTsCard(ValkeyTimeSeriesTestCaseBase):

    def setup_data(self):
        # Create test time series with different labels
        self.client.execute_command('TS.CREATE', 'ts1', 'LABELS', 'sensor', 'temp', 'area', 'A', 'location', 'room1')
        self.client.execute_command('TS.CREATE', 'ts2', 'LABELS', 'sensor', 'temp', 'area', 'B', 'location', 'room2')
        self.client.execute_command('TS.CREATE', 'ts3', 'LABELS', 'sensor', 'humidity', 'area', 'A', 'location',
                                    'room1')
        self.client.execute_command('TS.CREATE', 'ts4', 'LABELS', 'sensor', 'pressure', 'area', 'C', 'location',
                                    'room3')

        # Create a series with no data points
        self.client.execute_command('TS.CREATE', 'ts_nodata', 'LABELS', 'sensor', 'light', 'area', 'D', 'location',
                                    'room4')

        # Add data points with specific timestamps
        self.client.execute_command('TS.ADD', 'ts1', 1000, 25)
        self.client.execute_command('TS.ADD', 'ts1', 2000, 26)

        self.client.execute_command('TS.ADD', 'ts2', 1500, 30)
        self.client.execute_command('TS.ADD', 'ts2', 2500, 31)

        self.client.execute_command('TS.ADD', 'ts3', 1200, 60)
        self.client.execute_command('TS.ADD', 'ts3', 2200, 65)

        self.client.execute_command('TS.ADD', 'ts4', 1800, 1000)
        self.client.execute_command('TS.ADD', 'ts4', 2800, 1010)

    def test_card_basic(self):
        """Test basic TS.CARD functionality with no filters"""
        # Should count all time series
        self.setup_data()

        result = self.client.execute_command('TS.CARD')
        assert result == 5  # ts1, ts2, ts3, ts4, ts_nodata

        # After adding another time series, count should increase
        self.client.execute_command('TS.CREATE', 'ts_new', 'LABELS', 'sensor', 'voltage', 'area', 'E')
        result = self.client.execute_command('TS.CARD')
        assert result == 6

    def test_card_with_single_filter(self):
        """Test TS.CARD with a single filter"""
        # Count series with sensor=temp

        self.setup_data()

        result = self.client.execute_command('TS.CARD', 'FILTER', 'sensor=temp')
        assert result == 2  # ts1, ts2

        # Count series with area=A
        result = self.client.execute_command('TS.CARD', 'FILTER', 'area=A')
        assert result == 2  # ts1, ts3

        # Count series with location=room1
        result = self.client.execute_command('TS.CARD', 'FILTER', 'location=room1')
        assert result == 2  # ts1, ts3

        # No matches should return 0
        result = self.client.execute_command('TS.CARD', 'FILTER', 'sensor=nonexistent')
        assert result == 0

    def test_card_with_multiple_filters(self):
        """Test TS.CARD with multiple label filters"""
        # Multiple labels match a single series

        self.setup_data()

        result = self.client.execute_command('TS.CARD', 'FILTER', 'sensor=temp', 'area=A')
        assert result == 1  # only ts1

        # Multiple labels match no series
        result = self.client.execute_command('TS.CARD', 'FILTER', 'sensor=temp', 'area=D')
        assert result == 0

        # Multiple labels including location
        result = self.client.execute_command('TS.CARD', 'FILTER', 'sensor=humidity', 'location=room1')
        assert result == 1  # only ts3

        # Filter that includes series with no data
        result = self.client.execute_command('TS.CARD', 'FILTER', 'sensor=light')
        assert result == 1  # ts_nodata (even though it has no data points)

    def test_card_with_complex_filters(self):
        """Test TS.CARD with more complex filter expressions"""

        self.setup_data()

        # A filter list of nothing but negative matchers is unbounded and rejected.
        self.assert_filters_rejected('TS.CARD', 'FILTER', 'sensor!=temp')
        self.assert_filters_rejected('TS.CARD', 'FILTER', 'sensor!=temp', 'area!=D')

        # Every sensor value here is lowercase letters, so 'sensor=~"[a-z]+"' bounds the query
        # without excluding anything -- it just supplies the positive matcher the filter list
        # needs. ('sensor=~"[a-z]+"' would not: it is a presence check, like 'sensor!=', and
        # RedisTimeSeries accepts neither as a positive matcher.)
        self.assert_filters_rejected('TS.CARD', 'FILTER', 'sensor=~".+"', 'sensor!=temp')
        result = self.client.execute_command('TS.CARD', 'FILTER', 'sensor=~"[a-z]+"', 'sensor!=temp')
        assert result == 3  # ts3, ts4, ts_nodata

        # Multiple inequality filters
        result = self.client.execute_command('TS.CARD', 'FILTER', 'sensor=~"[a-z]+"', 'sensor!=temp', 'area!=D')
        assert result == 2  # ts3, ts4

        # Mix equality and inequality
        result = self.client.execute_command('TS.CARD', 'FILTER', 'sensor=temp', 'area!=A')
        assert result == 1  # ts2

    def test_card_with_date_range(self):
        """Test TS.CARD with date range filtering"""

        self.setup_data()

        # Filter all temp sensors with data between timestamps 1000-2000
        result = self.client.execute_command('TS.CARD', 'FILTER_BY_RANGE', 1000, 2000, 'FILTER', 'sensor=temp')
        assert result == 2  # Both ts1 and ts2 have data in this range

        # Filter temp sensors with data in a narrower range
        result = self.client.execute_command('TS.CARD', 'FILTER_BY_RANGE', 1000, 1200, 'FILTER', 'sensor=temp')
        assert result == 1  # Only ts1 has data in this early range

        # Filter for data after a specific time
        result = self.client.execute_command('TS.CARD', 'FILTER_BY_RANGE', 2600, 3000, 'FILTER', 'sensor=~"[a-z]+"', 'sensor!=light')
        assert result == 1  # Only ts4 has data after timestamp 2600

        # Filter that matches series but outside their data range
        result = self.client.execute_command('TS.CARD', 'FILTER_BY_RANGE', 3000, 4000, 'FILTER', 'sensor=temp')
        assert result == 0  # No temp sensors have data after timestamp 3000

        # Filter with very wide range
        result = self.client.execute_command('TS.CARD', 'FILTER_BY_RANGE', 0, 5000, 'FILTER', 'sensor=~"[a-z]+"', 'sensor!=light')
        assert result == 4  # All except ts_nodata because it has no data points

    def test_card_with_negative_date_range(self):
        """Test TS.CARD with negative date range filtering (FILTER_BY_RANGE ... NOT)"""

        self.setup_data()

        # temp series are ts1 (1000,2000) and ts2 (1500,2500).
        # In range [1000,2000], both have samples. With NOT, neither should match.
        result = self.client.execute_command(
            'TS.CARD',
            'FILTER_BY_RANGE', 'NOT', 1000, 2000,
            'FILTER', 'sensor=temp',
        )
        assert result == 0

        # In range [2100,2400], ts1 has no samples, ts2 has no samples (2500 is outside),
        # so both should match with NOT.
        result = self.client.execute_command(
            'TS.CARD',
            'FILTER_BY_RANGE', 'NOT', 2100, 2400,
            'FILTER', 'sensor=temp',
        )
        assert result == 2

        # For non-light sensors (ts1, ts2, ts3, ts4), in range [0,5000] they all have samples.
        # With NOT, none should match.
        result = self.client.execute_command(
            'TS.CARD',
            'FILTER_BY_RANGE', 'NOT', 0, 5000,
            'FILTER', 'sensor=~"[a-z]+"', 'sensor!=light',
        )
        assert result == 0

        # For all series except light, in range [3000,4000] none have samples (ts4 max is 2800),
        # so all 4 should match with NOT.
        result = self.client.execute_command(
            'TS.CARD',
            'FILTER_BY_RANGE', 'NOT', 3000, 4000,
            'FILTER', 'sensor=~"[a-z]+"', 'sensor!=light',
        )
        assert result == 4

        # A series with no samples should always match any NOT range when it matches labels.
        result = self.client.execute_command(
            'TS.CARD',
            'FILTER_BY_RANGE', 'NOT', 0, 5000,
            'FILTER', 'sensor=light',
        )
        assert result == 1

    def test_card_timestamp_parameter_formats(self):
        """Test TS.CARD with different timestamp parameter formats"""

        self.setup_data()

        # Test with string timestamps
        result = self.client.execute_command('TS.CARD', 'FILTER_BY_RANGE', '1000', '2000', 'FILTER', 'sensor=temp')
        assert result == 2

        # Test with - as min timestamp
        result = self.client.execute_command('TS.CARD', 'FILTER_BY_RANGE', '-', 2000, 'FILTER', 'sensor=temp')
        assert result == 2

        # Test with + as max timestamp
        result = self.client.execute_command('TS.CARD', 'FILTER_BY_RANGE', 1000, '+', 'FILTER', 'sensor=temp')
        assert result == 2

        # Test with both - and +
        result = self.client.execute_command('TS.CARD', 'FILTER_BY_RANGE', '-', '+', 'FILTER', 'sensor=humidity')
        assert result == 1

    def test_card_with_recently_added_series(self):
        """Test TS.CARD behavior with recently added time series"""

        self.setup_data()

        # Count before adding
        initial_count = self.client.execute_command('TS.CARD', 'FILTER', 'sensor=motion')
        assert initial_count == 0

        # Add a new time series
        self.client.execute_command('TS.CREATE', 'ts_motion', 'LABELS', 'sensor', 'motion', 'area', 'F')

        # Count should immediately reflect the new series
        new_count = self.client.execute_command('TS.CARD', 'FILTER', 'sensor=motion')
        assert new_count == 1

        # Add a data point and verify it's still counted
        self.client.execute_command('TS.ADD', 'ts_motion', 3000, 1)
        with_data_count = self.client.execute_command('TS.CARD', 'FILTER', 'sensor=motion')
        assert with_data_count == 1

        # Test with date range that includes the new point
        range_count = self.client.execute_command('TS.CARD', 'FILTER_BY_RANGE', 2900, 3100, 'FILTER', 'sensor=motion')
        assert range_count == 1

    def test_card_after_deleting_samples(self):
        """Test TS.CARD behavior after deleting samples"""

        self.setup_data()

        # Add a time series for deletion testing
        self.client.execute_command('TS.CREATE', 'ts_del', 'LABELS', 'sensor', 'deletion', 'area', 'G')
        self.client.execute_command('TS.ADD', 'ts_del', 1000, 100)
        self.client.execute_command('TS.ADD', 'ts_del', 2000, 200)

        # Initial count with data
        result_before = self.client.execute_command('TS.CARD', 'FILTER_BY_RANGE', 1000, 2000, 'FILTER',
                                                    'sensor=deletion')
        assert result_before == 1

        # Delete samples
        self.client.execute_command('TS.DEL', 'ts_del', 1000, 2000)

        # Count after deletion (within range)
        result_after = self.client.execute_command('TS.CARD', 'FILTER_BY_RANGE', 1000, 2000, 'FILTER',
                                                   'sensor=deletion')
        assert result_after == 0  # No data in range

        # Count without range should still show the series
        total_count = self.client.execute_command('TS.CARD', 'FILTER', 'sensor=deletion')
        assert total_count == 1  # Series still exists, just no data in range

    def test_card_after_altering_labels(self):
        """Test TS.CARD behavior after altering labels"""

        self.setup_data()

        # Count before altering
        result_before = self.client.execute_command('TS.CARD', 'FILTER', 'sensor=temp', 'area=B')
        assert result_before == 1  # ts2

        # Alter the labels
        self.client.execute_command('TS.ALTER', 'ts2', 'LABELS', 'sensor', 'temp', 'area', 'Z')

        # Count after altering
        result_after_old = self.client.execute_command('TS.CARD', 'FILTER', 'sensor=temp', 'area=B')
        assert result_after_old == 0  # No longer matches

        # Count with new labels
        result_after_new = self.client.execute_command('TS.CARD', 'FILTER', 'sensor=temp', 'area=Z')
        assert result_after_new == 1  # Now matches ts2 with new label

    def test_card_with_many_series(self):
        """Test TS.CARD with a larger number of time series"""

        self.setup_data()

        # Create a batch of 50 new series with similar labels but different subvalues
        base_ts = int(time.time())
        for i in range(50):
            key = f'batch_ts_{i}'
            self.client.execute_command('TS.CREATE', key, 'LABELS',
                                        'batch', 'yes',
                                        'index', str(i),
                                        'group', f'g{i // 10}')  # Create 5 groups of 10
            self.client.execute_command('TS.ADD', key, base_ts + i, i)

        # Count all batch series
        result = self.client.execute_command('TS.CARD', 'FILTER', 'batch=yes')
        assert result == 50

        # Count a specific group
        result = self.client.execute_command('TS.CARD', 'FILTER', 'batch=yes', 'group=g2')
        assert result == 10  # Indexes 20-29

        # Count with a time range
        result = self.client.execute_command('TS.CARD', 'FILTER_BY_RANGE', base_ts, base_ts + 25, 'FILTER', 'batch=yes')
        assert result == 26  # Indexes 0-25

        # Count with a complex filter
        result = self.client.execute_command('TS.CARD', 'FILTER', 'batch=yes', 'group!=g0', 'group!=g4')
        assert result == 30  # Groups g1, g2, g3 (indexes 10-39)

    def test_card_date_range_no_filter_error(self):
        """Test that TS.CARD with only date range and no filter raises an error"""

        self.setup_data()

        # This should fail as a date range without filters is not allowed
        with pytest.raises(ResponseError) as excinfo:
            self.client.execute_command('TS.CARD', 'FILTER_BY_RANGE', 1000, 2000)

        error_msg = str(excinfo.value).lower()
        assert "requires at least one matcher" in error_msg or "wrong number of arguments" in error_msg

    def test_card_empty_database(self):
        """Test TS.CARD behavior on an empty database"""

        self.setup_data()

        # Flush the database first
        self.client.flushdb()

        # Total count should be 0
        result = self.client.execute_command('TS.CARD')
        assert result == 0

        # With filter should also be 0
        result = self.client.execute_command('TS.CARD', 'FILTER', 'sensor=temp')
        assert result == 0

    def test_card_invalid_syntax(self):
        """Test TS.CARD with invalid syntax"""

        self.setup_data()

        # Invalid timestamp format
        with pytest.raises(ResponseError):
            self.client.execute_command('TS.CARD', 'FILTER_BY_RANGE', 'not-a-time', 2000, 'FILTER', 'sensor=temp')

        # Missing timestamp
        with pytest.raises(ResponseError):
            self.client.execute_command('TS.CARD', 'FILTER_BY_RANGE', 'FILTER', 'sensor=temp')

        # Missing filter value
        with pytest.raises(ResponseError):
            self.client.execute_command('TS.CARD', 'FILTER')

    def test_card_with_queryindex(self):
        """Test interaction between TS.CARD and TS.QUERYINDEX"""

        self.setup_data()

        # Compare TS.CARD and TS.QUERYINDEX for the same filter
        filter_expr = 'sensor=temp'

        # Get results from both commands
        card_result = self.client.execute_command('TS.CARD', 'FILTER', filter_expr)
        query_result = self.client.execute_command('TS.QUERYINDEX', filter_expr)

        # Should return the same count
        assert card_result == len(query_result)

        # Check for a more complex filter
        complex_filter = ['sensor=temp', 'area=A']
        card_result = self.client.execute_command('TS.CARD', 'FILTER', *complex_filter)
        query_result = self.client.execute_command('TS.QUERYINDEX', *complex_filter)

        assert card_result == len(query_result)
