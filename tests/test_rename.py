from valkeytestframework.conftest import resource_port_tracker
from valkeytestframework.util.waiters import *

from valkey_timeseries_test_case import ValkeyTimeSeriesTestCaseBase


class TestRename(ValkeyTimeSeriesTestCaseBase):
    """Test cases for server event handling in the TimeSeries module."""

    def create_ts(self, key, timestamp=None, value=1.0):
        """Helper to create a time series with a sample data point."""

        if timestamp is None:
            timestamp = 1000

        self.client.execute_command("TS.CREATE", key, "LABELS", "key", key, "sensor", "temp")
        self.client.execute_command("TS.ADD", key, timestamp, value)
        return key


    def test_rename_event(self):
        """Test that series is re-indexed after being renamed."""

        old_key = "ts:oldname"
        new_key = "ts:newname"

        self.client.execute_command("TS.CREATE", old_key, "LABELS", "__name__", "sensor", "sensor", "temp", "key", old_key)
        self.client.execute_command("TS.ADD", old_key, 3333, 1.5)

        keys = self.client.execute_command("TS.QUERYINDEX", f"key={old_key}")
        print(keys)
        assert len(keys) > 0

        # Rename the key
        self.client.rename(old_key, new_key)

        # Old key should be gone, new key should exist
        assert not self.client.exists(old_key)
        assert self.client.exists(new_key)

        # The new key should have the same data
        sample = self.client.execute_command("TS.GET", new_key)
        assert sample[0] == 3333
        assert float(sample[1]) == 1.5

        # The old key should not be queryable
        keys = self.client.execute_command("TS.QUERYINDEX", 'sensor=temp')
        assert keys[0].decode('utf-8') == new_key

        # The new key should be queryable
        result = self.client.execute_command("TS.RANGE", new_key, 0, "+")
        assert len(result) == 1
        assert result[0][0] == 3333

        # Verify the new key is indexed correctly
        keys = self.client.execute_command("TS.QUERYINDEX", f"key={old_key}")
        assert len(keys) == 1
        assert keys[0].decode('utf-8') == new_key


        keys = self.client.execute_command("TS.QUERYINDEX", 'sensor=temp')
        assert len(keys) == 1
        assert keys[0].decode('utf-8') == new_key