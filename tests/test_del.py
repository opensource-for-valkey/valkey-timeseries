import time

from valkeytestframework.conftest import resource_port_tracker
from valkeytestframework.util.waiters import *

from valkey_timeseries_test_case import ValkeyTimeSeriesTestCaseBase


class TestDel(ValkeyTimeSeriesTestCaseBase):
    """Test DEL command."""

    def create_ts(self, key, timestamp=None, value=1.0):
        """Helper to create a time series with a sample data point."""

        self.start_ts = timestamp or 1577836800  # Default to 2020-01-01 00:00:00
        if timestamp is None:
            timestamp = self.start_ts

        self.client.execute_command("TS.CREATE", key, "LABELS", "key", key, "sensor", "temp")
        self.client.execute_command("TS.ADD", key, timestamp, value)
        return key

    def test_delete(self):
        """Test that a deleted series is properly removed from the index."""

        key = "ts:delete"
        self.create_ts(key)

        # Delete the key
        self.client.delete(key)

        # The key should be removed
        assert not self.client.exists(key)

        keys = self.client.execute_command("TS.QUERYINDEX", f"key={key}")
        assert len(keys) == 0

        keys = self.client.execute_command("TS.QUERYINDEX", 'sensor=temp')
        assert len(keys) == 0

        # Create the key again - should succeed without index interference
        self.create_ts(key, 50000, 2.0)
        result = self.client.execute_command("TS.RANGE", key, 0, "+")
        print(result)
        assert len(result) == 1
        assert result[0][0] == 50000


    def test_compaction_series_persist_after_parent_deletion(self):
        """Test that compaction series are NOT deleted when the parent series is deleted."""

        parent_key = "ts:parent"
        compaction_key1 = "ts:compaction_avg"
        compaction_key2 = "ts:compaction_sum"

        # Create parent time series
        self.client.execute_command("TS.CREATE", parent_key, "LABELS", "type", "parent", "sensor", "temperature")

        # Create compaction destination series
        self.client.execute_command("TS.CREATE", compaction_key1, "LABELS", "type", "compaction", "aggregation", "avg")
        self.client.execute_command("TS.CREATE", compaction_key2, "LABELS", "type", "compaction", "aggregation", "sum")

        # Create compaction rules
        self.client.execute_command("TS.CREATERULE", parent_key, compaction_key1, "AGGREGATION", "avg", 1000)
        self.client.execute_command("TS.CREATERULE", parent_key, compaction_key2, "AGGREGATION", "sum", 1000)

        # Add sample data to parent series to generate compacted data
        base_timestamp = 1577836800000  # 2020-01-01 00:00:00 in milliseconds
        for i in range(15):  # Add enough samples to fill multiple compaction buckets
            timestamp = base_timestamp + (i * 100)  # 100ms intervals
            value = 10.0 + i
            self.client.execute_command("TS.ADD", parent_key, timestamp, value)

        # Add one more sample in a new bucket to trigger compaction of the previous bucket
        self.client.execute_command("TS.ADD", parent_key, base_timestamp + 2000, 50.0)

        # Verify compaction rules exist on parent
        parent_info = self.ts_info(parent_key)
        assert 'rules' in parent_info
        assert len(parent_info['rules']) == 2

        # Verify compacted data exists in destination series
        compaction1_data = self.client.execute_command("TS.RANGE", compaction_key1, 0, "+")
        compaction2_data = self.client.execute_command("TS.RANGE", compaction_key2, 0, "+")

        print(f"Compaction 1 data before deletion: {compaction1_data}")
        print(f"Compaction 2 data before deletion: {compaction2_data}")

        # Both compaction series should have data
        assert len(compaction1_data) > 0, "Compaction series 1 should have data"
        assert len(compaction2_data) > 0, "Compaction series 2 should have data"

        # Store compacted data for comparison
        original_compaction1_data = compaction1_data.copy()
        original_compaction2_data = compaction2_data.copy()

        # Delete the parent series
        self.client.delete(parent_key)

        # Verify parent series is deleted
        assert not self.client.exists(parent_key), "Parent series should be deleted"

        # Verify compaction series still exist
        assert self.client.exists(compaction_key1), "Compaction series 1 should still exist"
        assert self.client.exists(compaction_key2), "Compaction series 2 should still exist"

        # Verify compacted data is preserved
        compaction1_data_after = self.client.execute_command("TS.RANGE", compaction_key1, 0, "+")
        compaction2_data_after = self.client.execute_command("TS.RANGE", compaction_key2, 0, "+")

        print(f"Compaction 1 data after deletion: {compaction1_data_after}")
        print(f"Compaction 2 data after deletion: {compaction2_data_after}")

        # The compacted data should be identical to what was there before
        assert compaction1_data_after == original_compaction1_data, "Compaction series 1 data should be preserved"
        assert compaction2_data_after == original_compaction2_data, "Compaction series 2 data should be preserved"

        # Verify we can still query the compaction series by their labels
        compaction_keys = self.client.execute_command("TS.QUERYINDEX", 'type=compaction')
        assert len(compaction_keys) == 2, "Should find both compaction series"
        assert compaction_key1.encode() in compaction_keys or compaction_key1 in compaction_keys
        assert compaction_key2.encode() in compaction_keys or compaction_key2 in compaction_keys

        # Verify parent series is no longer in the index
        parent_keys = self.client.execute_command("TS.QUERYINDEX", 'type=parent')
        assert len(parent_keys) == 0, "Parent series should not be in index"

        # Verify we can still get info from compaction series
        compaction1_info = self.ts_info(compaction_key1)
        compaction2_info = self.ts_info(compaction_key2)

        assert compaction1_info is not None
        assert compaction2_info is not None
        assert compaction1_info['totalSamples'] > 0
        assert compaction2_info['totalSamples'] > 0

        # Verify we can add new data to the compaction series directly (they're independent now)
        new_timestamp = base_timestamp + 10000
        self.client.execute_command("TS.ADD", compaction_key1, new_timestamp, 100.0)
        self.client.execute_command("TS.ADD", compaction_key2, new_timestamp, 200.0)

        # Verify the new data was added
        final_compaction1_data = self.client.execute_command("TS.RANGE", compaction_key1, 0, "+")
        final_compaction2_data = self.client.execute_command("TS.RANGE", compaction_key2, 0, "+")

        assert len(final_compaction1_data) == len(original_compaction1_data) + 1
        assert len(final_compaction2_data) == len(original_compaction2_data) + 1
        assert final_compaction1_data[-1] == [new_timestamp, b'100']
        assert final_compaction2_data[-1] == [new_timestamp, b'200']

        print("Test passed: Compaction series persist independently after parent deletion")

    def test_multiple_compaction_levels_persist_after_parent_deletion(self):
        """Test that multiple levels of compaction series persist when the parent is deleted."""

        parent_key = "ts:multilevel_parent"
        level1_key = "ts:level1_compaction"
        level2_key = "ts:level2_compaction"

        # Create all series
        self.client.execute_command("TS.CREATE", parent_key, "LABELS", "level", "0")
        self.client.execute_command("TS.CREATE", level1_key, "LABELS", "level", "1")
        self.client.execute_command("TS.CREATE", level2_key, "LABELS", "level", "2")

        # Create a compaction chain: parent -> level1 -> level2
        self.client.execute_command("TS.CREATERULE", parent_key, level1_key, "AGGREGATION", "avg", 1000)
        self.client.execute_command("TS.CREATERULE", level1_key, level2_key, "AGGREGATION", "avg", 5000)

        # Add data to trigger multiple levels of compaction
        base_timestamp = 1577836800000
        for i in range(50):  # Enough data for multi-level compaction
            timestamp = base_timestamp + (i * 100)
            value = 20.0 + (i % 10)
            self.client.execute_command("TS.ADD", parent_key, timestamp, value)

        # Trigger the final compaction by adding data in new buckets
        self.client.execute_command("TS.ADD", parent_key, base_timestamp + 10000, 100.0)
        self.client.execute_command("TS.ADD", parent_key, base_timestamp + 15000, 100.0)

        # Verify all levels have data
        level1_data_before = self.client.execute_command("TS.RANGE", level1_key, 0, "+")
        level2_data_before = self.client.execute_command("TS.RANGE", level2_key, 0, "+")

        assert len(level1_data_before) > 0, "Level 1 compaction should have data"
        assert len(level2_data_before) > 0, "Level 2 compaction should have data"

        # Delete the parent
        self.client.delete(parent_key)

        # Verify parent is gone but all compaction levels remain
        assert not self.client.exists(parent_key)
        assert self.client.exists(level1_key)
        assert self.client.exists(level2_key)

        # Verify data is preserved at all levels
        level1_data_after = self.client.execute_command("TS.RANGE", level1_key, 0, "+")
        level2_data_after = self.client.execute_command("TS.RANGE", level2_key, 0, "+")

        assert level1_data_after == level1_data_before
        assert level2_data_after == level2_data_before

        # Verify the compaction rule still exists between level1 and level2
        level1_info = self.ts_info(level1_key)
        assert 'rules' in level1_info
        assert len(level1_info['rules']) == 1
        assert level1_info['rules'][0].dest_key == level2_key
