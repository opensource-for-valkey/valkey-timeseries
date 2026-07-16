import pytest
from valkey import ResponseError
from valkeytestframework.util.waiters import *
from valkeytestframework.conftest import resource_port_tracker
from valkey_timeseries_test_case import ValkeyTimeSeriesTestCaseBase
from common import LabelSearchResponse



class TestTimeSeriesLabelValues(ValkeyTimeSeriesTestCaseBase):

    def setup_test_data(self, client):
        """Set up test data with various label values"""
        # Create time series with different label combinations
        client.execute_command('TS.CREATE', 'ts1', 'LABELS', 'name', 'cpu', 'type', 'usage', 'node', 'server1',
                               'datacenter', 'dc1')
        client.execute_command('TS.CREATE', 'ts2', 'LABELS', 'name', 'cpu', 'type', 'temperature', 'node', 'server1',
                               'datacenter', 'dc1')
        client.execute_command('TS.CREATE', 'ts3', 'LABELS', 'name', 'memory', 'type', 'usage', 'node', 'server2',
                               'datacenter', 'dc1')
        client.execute_command('TS.CREATE', 'ts4', 'LABELS', 'name', 'disk', 'type', 'usage', 'node', 'server2',
                               'datacenter', 'dc2')
        client.execute_command('TS.CREATE', 'ts5', 'LABELS', 'name', 'cpu', 'type', 'usage', 'node', 'server3',
                               'datacenter', 'dc2')
        client.execute_command('TS.CREATE', 'ts6', 'LABELS', 'name', 'network', 'type', 'throughput', 'node', 'server3')

        # Add some sample data
        now = 1000
        for i in range(1, 7):
            ts_key = f'ts{i}'
            client.execute_command('TS.ADD', ts_key, now, i * 10)
            client.execute_command('TS.ADD', ts_key, now + 1000, i * 10 + 5)

    def exec_sorted_values(self, name, *args):
        """Helper method to execute a command and return a LabelValuesResponse"""
        result = self.client.execute_command('TS.LABELVALUES', name, *args)
        result = LabelSearchResponse.parse(result)
        values = sorted([lv.value for lv in result.results])
        return values

    def test_label_values_with_filter(self):
        """Test retrieving label values with a filter"""
        self.setup_test_data(self.client)

        # Get values for the 'name' label filtered by type=usage
        values = self.exec_sorted_values('name', 'FILTER', 'type=usage')
        assert values == [b'cpu', b'disk', b'memory']

        # Get values for the 'type' label filtered by name=cpu
        values = self.exec_sorted_values('type', 'FILTER', 'name=cpu')
        assert values == [b'temperature', b'usage']

    def test_label_values_with_multiple_filters(self):
        """Test retrieving label values with multiple filters"""
        self.setup_test_data(self.client)

        # Get values for the 'node' label with multiple filters
        values = self.exec_sorted_values('node', 'FILTER', 'name=cpu', 'type=usage')
        assert values == [b'server1', b'server3']

        # Get values for the 'datacenter' label with multiple filters
        values = self.exec_sorted_values('datacenter', 'FILTER', 'name=cpu', 'type=usage')
        assert values == [b'dc1', b'dc2']

    def test_label_values_with_regex_filters(self):
        """Test retrieving label values with regex filters"""
        self.setup_test_data(self.client)

        # Get values for the 'node' label with regex filter
        values = self.exec_sorted_values('node', 'FILTER', 'name=~"c.*"')
        assert values == [b'server1', b'server3']

        # Get values for the 'type' label with regex filter
        values = self.exec_sorted_values('type', 'FILTER', 'name=~"(memory|disk)"')
        assert values == [b'usage']

    def test_label_values_with_time_range(self):
        """Test retrieving label values with time range filtering"""
        self.setup_test_data(self.client)

        # Add data with specific timestamps for time range testing
        self.client.execute_command('TS.CREATE', 'ts_old', 'LABELS', 'name', 'archive', 'age', 'old', 'common', '1')
        self.client.execute_command('TS.ADD', 'ts_old', 500, 100)

        self.client.execute_command('TS.CREATE', 'ts_new', 'LABELS', 'name', 'recent', 'age', 'new', 'common', '1')
        self.client.execute_command('TS.ADD', 'ts_new', 2500, 200)

        # Get values for the 'age' label with time range
        # First timestamp should exclude the 'old' series
        values = self.exec_sorted_values('age', 'FILTER_BY_RANGE', 1000, "+", "FILTER", 'common=1')
        assert values == [b'new']

        # Get values with end time range
        values = self.exec_sorted_values('age', 'FILTER_BY_RANGE', '-', 1500, "FILTER", 'common=1')
        assert values == [b'old']

        # Get values with both start and end time range
        self.client.execute_command('TS.CREATE', 'ts_1', 'LABELS', 'name', 'alice', 'age', '32', 'common', '1')
        self.client.execute_command('TS.ADD', 'ts_1', 1000, 200)

        self.client.execute_command('TS.CREATE', 'ts_2', 'LABELS', 'name', 'bob', 'age', '45', 'common', '1')
        self.client.execute_command('TS.ADD', 'ts_2', 1700, 200)

        values = self.exec_sorted_values('name', 'FILTER_BY_RANGE', 900, 2000, "FILTER", 'common=1')
        assert values == [b'alice', b'bob']

        # Test negative date range filtering with NOT
        # Exclude ts_old (500) - should only return 'new' and '45' values
        values = self.exec_sorted_values('age', 'FILTER_BY_RANGE', 'NOT', '-', 1000, 'FILTER', 'common=1')
        assert values == [b'45', b'new']

        # Exclude ts_new (2500) - should only return 'old' age value
        values = self.exec_sorted_values('age', 'FILTER_BY_RANGE', 'NOT', 2000, '+', 'FILTER', 'common=1')
        assert values == [b'32', b'45', b'old']
        assert b'new' not in values

        # Exclude middle range (ts_1 at 1000 and ts_2 at 1700) - should exclude both alice and bob
        values = self.exec_sorted_values('name', 'FILTER_BY_RANGE', 'NOT', 900, 2000, 'FILTER', 'common=1')
        assert b'alice' not in values
        assert b'bob' not in values
        # Only archive and recent should remain from the series with common=1
        assert set(values) == {b'archive', b'recent'}

        # Exclude late data (ts_new at 2500) - should return archive, alice, and bob
        values = self.exec_sorted_values('name', 'FILTER_BY_RANGE', 'NOT', 2400, '+', 'FILTER', 'common=1')

        assert b'recent' not in values
        assert b'archive' in values
        assert b'alice' in values
        assert b'bob' in values

    def test_label_values_with_limit(self):
        """Test retrieving label values with LIMIT parameter"""
        self.setup_test_data(self.client)

        # Get values for the 'name' label with limit
        values = self.exec_sorted_values('name', 'LIMIT', 2, 'FILTER', 'type=usage')
        assert len(values) == 2
        assert all(val in [b'cpu', b'disk', b'memory'] for val in values)

        # Get values for the 'node' label with limit
        values = self.exec_sorted_values('node', 'LIMIT', 1, "FILTER", 'datacenter=dc2')
        assert values == [b'server2']

    def test_label_values_with_combined_parameters(self):
        """Test retrieving label values with combined parameters"""
        self.setup_test_data(self.client)

        # Get values with filter, time range, and limit
        raw = self.client.execute_command(
            'TS.LABELVALUES', 'type',
            'FILTER_BY_RANGE', 500, 2500,
            'LIMIT', 1,
            'FILTER', 'name=~"c.*"',
        )
        parsed = LabelSearchResponse.parse(raw)
        # parsed.results is a list of LabelValue; ensure we have a single parsed value
        assert len(parsed.results) == 1
        assert parsed.results[0].value in [b'temperature', b'usage']

        # Different combination of parameters
        raw = self.client.execute_command(
            'TS.LABELVALUES', 'node',
            'FILTER_BY_RANGE', 900, '+',
            'FILTER', 'datacenter=dc1'
        )
        parsed = LabelSearchResponse.parse(raw)
        result = sorted([lv.value for lv in parsed.results])
        assert result == [b'server1', b'server2']

    def test_label_values_empty_result(self):
        """Test retrieving label values with no matching results"""
        self.setup_test_data(self.client)

        # Filter that doesn't match any series
        values = self.exec_sorted_values('name', 'FILTER', 'nonexistent=value')
        assert values == []

        # Filter with a time range that excludes all series
        values = self.exec_sorted_values('name', 'FILTER_BY_RANGE', 5000, '+', "FILTER", 'type=usage')
        assert values == []

    def test_label_values_error_cases(self):
        """Test error cases for TS.LABELVALUES"""
        self.setup_test_data(self.client)

        # Missing label argument
        self.verify_error_response(
            self.client, 'TS.LABELVALUES',
            "wrong number of arguments for 'ts.labelvalues' command"
        )

        # Invalid filter format
        # self.verify_error_response(
        #     self.client, 'TS.LABELVALUES name FILTER invalid-filter',
        #     "TSDB: series selector is invalid"
        # )

        # Invalid time format
        with pytest.raises(ResponseError):
            self.client.execute_command('TS.LABELVALUES', 'name', 'START', 'invalid-time')

        # Invalid limit format
        with pytest.raises(ResponseError):
            self.client.execute_command('TS.LABELVALUES', 'name', 'LIMIT', 'invalid-limit')

    def test_label_values_after_series_deletion(self):
        """Test retrieving label values after series deletion"""
        self.setup_test_data(self.client)

        # Verify initial state
        values = self.exec_sorted_values('name', 'FILTER', 'name=network')
        assert b'network' in values

        # Delete a time series
        self.client.execute_command('DEL', 'ts6')  # ts6 has name=network

        # Verify the deleted label value is no longer returned
        values = self.exec_sorted_values('name', 'FILTER', 'name=network')
        assert b'network' not in values

    def test_label_values_with_nonexistent_label(self):
        """Test retrieving values for a non-existent label"""
        self.setup_test_data(self.client)

        # Query for a label that doesn't exist
        values = self.exec_sorted_values('nonexistent_label', 'FILTER', 'name=cpu')
        assert values == []

    def test_label_values_after_label_update(self):
        """Test retrieving values after updating labels"""
        self.setup_test_data(self.client)

        # Verify initial state
        values = self.exec_sorted_values('name', 'FILTER', 'type=usage')
        assert b'updated' not in values

        # Create a new time series with a new label value
        self.client.execute_command('TS.CREATE', 'ts_new', 'LABELS', 'name', 'updated', 'type', 'usage')

        # Verify the new label value is included
        values = self.exec_sorted_values('name', 'FILTER', 'type=usage')
        assert b'updated' in values

        # todo: add test for label update of existing series

    def test_label_values_with_empty_database(self):
        """Test TS.LABELVALUES with an empty database"""
        # Ensure a database is empty
        self.client.execute_command('FLUSHALL')

        # Verify no values are returned for any label
        values = self.exec_sorted_values('name', 'FILTER', 'type=usage')
        assert values == []

        values = self.exec_sorted_values('type', 'FILTER', 'name=cpu')
        assert values == []
