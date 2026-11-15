from valkeytestframework.conftest import resource_port_tracker
from valkeytestframework.util.waiters import *

from valkey_timeseries_test_case import ValkeyTimeSeriesTestCaseBase


class TestFlushDb(ValkeyTimeSeriesTestCaseBase):
    """Test cases for server event handling in the TimeSeries module."""

    def create_ts(self, key, timestamp=None, value=1.0):
        """Helper to create a time series with a sample data point."""

        if timestamp is None:
            timestamp = 1000

        self.client.execute_command("TS.CREATE", key, "LABELS", "key", key, "sensor", "temp")
        self.client.execute_command("TS.ADD", key, timestamp, value)
        return key

    def test_flushdb_event(self):
        """Test that the index is cleared when the database is flushed."""

        # Create multiple keys
        start_ts = 5000
        keys = ["ts:flush1", "ts:flush2", "ts:flush3"]
        for i, key in enumerate(keys):
            self.create_ts(key, start_ts + (i * 10), float(i))

        # All keys should exist
        for key in keys:
            assert self.client.exists(key)

        keys = self.client.execute_command("TS.QUERYINDEX", 'sensor=temp')
        assert len(keys) == 3

        # Flush the database
        self.client.flushdb()

        # No keys should exist
        for key in keys:
            assert not self.client.exists(key)

        all_keys = self.client.execute_command("KEYS", "ts:*")
        assert len(all_keys) == 0, "All keys should be flushed"

        # the index should be empty
        keys = self.client.execute_command("TS.QUERYINDEX", 'key=~"ts:flush*"')
        assert len(keys) == 0

        keys = self.client.execute_command("TS.QUERYINDEX", 'sensor=temp')
        assert len(keys) == 0

        # Create a new key - should work without index interference
        new_key = "ts:new"
        self.create_ts(new_key)
        result = self.client.execute_command("TS.RANGE", new_key, 0, "+")
        assert len(result) == 1