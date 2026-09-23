import time

from valkeytestframework.conftest import resource_port_tracker
from valkeytestframework.util.waiters import *

from valkey_timeseries_test_case import ValkeyTimeSeriesTestCaseBase


class TestServerEvents(ValkeyTimeSeriesTestCaseBase):
    """Test cases for server event handling in the TimeSeries module."""

    def setup_data(self):
        self.start_ts = 1577836800  # 2020-01-01 00:00:00

    def create_ts(self, key, timestamp=None, value=1.0):
        """Helper to create a time series with a sample data point."""

        if timestamp is None:
            timestamp = 1000

        self.client.execute_command("TS.CREATE", key, "LABELS", "key", key, "sensor", "temp")
        self.client.execute_command("TS.ADD", key, timestamp, value)
        return key

    def test_loaded_event(self):
        """Test that a series is properly indexed when loaded."""

        key = "ts:loaded"
        self.client.execute_command("TS.CREATE", key, "LABELS", "__name__", "sensor", "sensor", "temp")

        # Save and reload to trigger load events
        self.client.save()
        self.client.execute_command("DEBUG", "RELOAD")

        # Verify the key exists and is queryable
        assert self.client.exists(key)

        # Query the key to ensure it was loaded and indexed correctly
        keys = self.client.execute_command("TS.QUERYINDEX", f"key={key}")
        assert len(keys) == 0

        keys = self.client.execute_command("TS.QUERYINDEX", 'sensor="temp"')
        assert len(keys) == 1
        assert keys[0].decode('utf-8') == key

    def test_restore_event(self):
        """Test that a restored series is properly re-indexed."""

        key = "ts:restore"
        self.create_ts(key, 2500)

        # Dump the key
        dumped = self.client.dump(key)
        self.client.delete(key)

        # Restore the key
        self.client.restore(key, 0, dumped)

        # Query the key to ensure it was restored and indexed correctly
        keys = self.client.execute_command("TS.QUERYINDEX", f"key={key}")
        assert len(keys) == 1
        assert keys[0].decode('utf-8') == key

        keys = self.client.execute_command("TS.QUERYINDEX", 'sensor="temp"')
        assert len(keys) == 1


        # The key should be queryable
        result = self.client.execute_command("TS.RANGE", key, 0, "+")
        assert len(result) == 1
        assert result[0][0] == 2500

    def test_expire_event(self):
        """Test that an expired series is removed from the index."""

        self.setup_data()

        key = "ts:expire"
        self.create_ts(key)

        # Set the key to expire in 1 second
        self.client.expire(key, 1)

        # Wait for the key to expire using the test framework waiter instead of sleep
        wait_for_true(lambda: not self.client.exists(key), timeout=TEST_MAX_WAIT_TIME_SECONDS)

        # The key should be gone
        assert not self.client.exists(key)
        
        # The key should not be queryable
        keys = self.client.execute_command("TS.QUERYINDEX", f"key={key}")
        assert len(keys) == 0
        keys = self.client.execute_command("TS.QUERYINDEX", 'sensor="temp"')
        assert len(keys) == 0

        # Create the key again - should succeed without index interference
        self.create_ts(key, self.start_ts + 1, 2.0)
        result = self.client.execute_command("TS.RANGE", key, 0, "+")
        assert len(result) == 1
        assert result[0][0] == self.start_ts + 1

    def test_concurrent_events(self):
        """Test handling of concurrent server events."""

        self.setup_data()

        # Create multiple keys
        keys = ["ts:concurrent1", "ts:concurrent2", "ts:concurrent3"]
        for i, key in enumerate(keys):
            self.create_ts(key, self.start_ts + i, float(i))

        # Perform multiple operations in quick succession
        # 1. Rename the first key
        self.client.rename(keys[0], "ts:renamed")

        # 2. Move the second key to db 1
        self.client.move(keys[1], 1)
        self.client.select(0)

        # 3. Delete the third key
        self.client.delete(keys[2])

        # Verify the state after all operations

        assert not self.client.exists(keys[0])
        exists = self.client.exists("ts:renamed")
        assert exists

        assert not self.client.exists(keys[1])
        self.client.select(1)
        assert self.client.exists(keys[1])
        self.client.select(0)

        assert not self.client.exists(keys[2])

        # All remaining keys should be queryable
        result = self.client.execute_command("TS.RANGE", "ts:renamed", 0, "+")
        assert len(result) == 1
        assert result[0][0] == self.start_ts

        self.client.select(1)
        key = keys[1]
        result = self.client.execute_command("TS.RANGE", key, 0, "+")

        assert len(result) == 1
        assert result[0][0] == self.start_ts + 1
        self.client.select(0)

    def test_restore_copy_of_a_live_key(self):
        """`RESTORE b <DUMP a>` with `a` still live brings back `a`'s series id. The copy has to
        be indexed under a fresh id — it used to be skipped as "already indexed" — and deleting
        it must not strip `a` from the index."""
        self.client.execute_command("TS.CREATE", "live:a", "LABELS", "name", "dup")
        self.client.execute_command("TS.ADD", "live:a", 1000, 1)
        self.client.restore("live:b", 0, self.client.dump("live:a"))

        assert sorted(self.client.execute_command("TS.QUERYINDEX", "name=dup")) == [b"live:a", b"live:b"]

        self.client.delete("live:b")
        assert self.client.execute_command("TS.QUERYINDEX", "name=dup") == [b"live:a"]
        assert self.client.execute_command("TS.CARD", "FILTER", "name=dup") == 1
