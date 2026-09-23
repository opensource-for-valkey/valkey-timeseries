import uuid

from valkeytestframework.conftest import resource_port_tracker
from valkeytestframework.util.waiters import *

from valkey_timeseries_test_case import ValkeyTimeSeriesTestCaseBase


class TestTsSwapDB(ValkeyTimeSeriesTestCaseBase):

    def generate_unique_key(self, prefix='test'):
        """Generate a unique key with prefix."""
        return f"{prefix}:{uuid.uuid4()}"

    def test_swapdb_basic_functionality(self):
        """Test basic SWAPDB functionality with timeseries data."""
        # Create keys in DB 0
        r = self.client
        r.select(0)

        # Create a timeseries in DB 0
        ts_key0 = self.generate_unique_key("ts")
        r.execute_command("TS.CREATE", ts_key0, "LABELS", "env", "prod", "region", "us-east")
        r.execute_command("TS.ADD", ts_key0, 1000, 10.5)
        r.execute_command("TS.ADD", ts_key0, 2000, 20.5)

        # Create different timeseries in DB 1
        r.select(1)
        ts_key1 = self.generate_unique_key("ts")
        r.execute_command("TS.CREATE", ts_key1, "LABELS", "env", "staging", "region", "us-west")
        r.execute_command("TS.ADD", ts_key1, 1000, 100.5)
        r.execute_command("TS.ADD", ts_key1, 2000, 200.5)

        # Verify the initial state - each timeseries in its correct DB
        r.select(0)
        assert r.exists(ts_key0) == 1
        assert r.exists(ts_key1) == 0

        # Get info from DB 0 timeseries
        info_db0 = self.ts_info(ts_key0)
        assert info_db0["labels"]["env"] == "prod"

        r.select(1)
        assert r.exists(ts_key1) == 1
        assert r.exists(ts_key0) == 0

        # Get info from DB 1 timeseries
        info_db1 = self.ts_info(ts_key1)
        assert info_db1["labels"]["env"] == "staging"

        # Swap DB 0 and DB 1
        r.execute_command("SWAPDB", 0, 1)

        # After swap, verify timeseries are in swapped DBs
        r.select(0)
        assert r.exists(ts_key1) == 1
        assert r.exists(ts_key0) == 0

        # Info should be properly swapped - DB 0 should now have the staging timeseries
        info_db0_after = self.ts_info(ts_key1)
        assert info_db0_after["labels"]["env"] == "staging"

        r.select(1)
        assert r.exists(ts_key0) == 1
        assert r.exists(ts_key1) == 0

        # Info should be properly swapped - the DB 1 should now have the prod timeseries
        info_db1_after = self.ts_info(ts_key0)
        assert info_db1_after["labels"]["env"] == "prod"

    def test_swapdb_with_label_queries(self):
        """Test that label-based queries work correctly after SWAPDB."""
        r = self.client

        # Setup DB 0 with production metrics
        r.select(0)
        # Create multiple timeseries with production labels
        for i in range(5):
            ts_key = self.generate_unique_key("prod")
            r.execute_command("TS.CREATE", ts_key, "LABELS",
                              "env", "prod",
                              "region", f"region-{i}",
                              "service", "api")
            r.execute_command("TS.ADD", ts_key, 1000, i * 10)

        # Setup DB 1 with staging metrics
        r.select(1)
        # Create multiple timeseries with staging labels
        for i in range(3):
            ts_key = self.generate_unique_key("stage")
            r.execute_command("TS.CREATE", ts_key, "LABELS",
                              "env", "staging",
                              "region", f"region-{i}",
                              "service", "web")
            r.execute_command("TS.ADD", ts_key, 1000, i * 100)

        # Verify initial query results
        r.select(0)
        prod_keys_before = r.execute_command("TS.QUERYINDEX", "env=prod")
        assert len(prod_keys_before) == 5

        r.select(1)
        stage_keys_before = r.execute_command("TS.QUERYINDEX", "env=staging")
        assert len(stage_keys_before) == 3

        # Execute SWAPDB
        r.execute_command("SWAPDB", 0, 1)

        # Verify queries work properly after swap
        r.select(0)
        stage_keys_after = r.execute_command("TS.QUERYINDEX", "env=staging")
        assert len(stage_keys_after) == 3
        assert set(stage_keys_after) == set(stage_keys_before)

        prod_keys_after_in_db0 = r.execute_command("TS.QUERYINDEX", "env=prod")
        assert len(prod_keys_after_in_db0) == 0  # No prod keys in DB 0 after swap

        r.select(1)
        prod_keys_after = r.execute_command("TS.QUERYINDEX", "env=prod")
        assert len(prod_keys_after) == 5
        assert set(prod_keys_after) == set(prod_keys_before)

        stage_keys_after_in_db1 = r.execute_command("TS.QUERYINDEX", "env=staging")
        assert len(stage_keys_after_in_db1) == 0  # No staging keys in DB 1 after swap

    def test_swapdb_complex_queries(self):
        """Test that complex label queries continue to work after SWAPDB."""
        r = self.client

        # Setup DB 0 with mixed metrics
        r.select(0)
        # Create timeseries with different combinations of labels
        for i in range(10):
            ts_key = self.generate_unique_key("ts")
            region = "us-east" if i % 2 == 0 else "us-west"
            service = "api" if i % 3 == 0 else "web" if i % 3 == 1 else "db"
            r.execute_command("TS.CREATE", ts_key, "LABELS",
                              "env", "prod",
                              "region", region,
                              "service", service)
            r.execute_command("TS.ADD", ts_key, 1000, i * 10)

        # Setup DB 1 with different metrics
        r.select(1)
        for i in range(5):
            ts_key = self.generate_unique_key("ts")
            region = "eu-central" if i % 2 == 0 else "eu-west"
            service = "cache" if i % 2 == 0 else "queue"
            r.execute_command("TS.CREATE", ts_key, "LABELS",
                              "env", "staging",
                              "region", region,
                              "service", service)
            r.execute_command("TS.ADD", ts_key, 1000, i * 100)

        # Verify initial complex queries
        r.select(0)
        api_east_before = r.execute_command("TS.QUERYINDEX", "service=api", "region=us-east")
        web_west_before = r.execute_command("TS.QUERYINDEX", "service=web", "region=us-west")

        r.select(1)
        cache_central_before = r.execute_command("TS.QUERYINDEX", "service=cache", "region=eu-central")

        # Execute SWAPDB
        r.execute_command("SWAPDB", 0, 1)

        # Verify complex queries after swap
        r.select(0)  # Now contains DB 1's original data
        cache_central_after = r.execute_command("TS.QUERYINDEX", "service=cache", "region=eu-central")
        assert set(cache_central_after) == set(cache_central_before)

        # These queries should now return empty lists in DB 0
        api_east_after_db0 = r.execute_command("TS.QUERYINDEX", "service=api", "region=us-east")
        assert len(api_east_after_db0) == 0

        r.select(1)  # Now contains DB 0's original data
        api_east_after = r.execute_command("TS.QUERYINDEX", "service=api", "region=us-east")
        web_west_after = r.execute_command("TS.QUERYINDEX", "service=web", "region=us-west")

        assert set(api_east_after) == set(api_east_before)
        assert set(web_west_after) == set(web_west_before)

        # This query should now return empty list in DB 1
        cache_central_after_db1 = r.execute_command("TS.QUERYINDEX", "service=cache", "region=eu-central")
        assert len(cache_central_after_db1) == 0

    def test_swapdb_with_updates_after_swap(self):
        """Test updates to timeseries work correctly after SWAPDB."""
        r = self.client

        # Create keys in DB 0 and DB 1
        r.select(0)
        ts_key0 = self.generate_unique_key("ts")
        r.execute_command("TS.CREATE", ts_key0, "LABELS", "env", "prod")
        r.execute_command("TS.ADD", ts_key0, 1000, 10.5)

        r.select(1)
        ts_key1 = self.generate_unique_key("ts")
        r.execute_command("TS.CREATE", ts_key1, "LABELS", "env", "staging")
        r.execute_command("TS.ADD", ts_key1, 1000, 100.5)

        # Swap databases
        r.execute_command("SWAPDB", 0, 1)

        # Add new data to timeseries after swap
        r.select(0)  # Now has the staging series
        r.execute_command("TS.ADD", ts_key1, 2000, 200.5)

        r.select(1)  # Now has the prod series
        r.execute_command("TS.ADD", ts_key0, 2000, 20.5)

        # Verify data in both timeseries
        r.select(0)
        range_result0 = r.execute_command("TS.RANGE", ts_key1, 0, 3000)
        assert len(range_result0) == 2
        assert [point[1] for point in range_result0] == [b'100.5', b'200.5']

        r.select(1)
        range_result1 = r.execute_command("TS.RANGE", ts_key0, 0, 3000)
        assert len(range_result1) == 2
        assert [point[1] for point in range_result1] == [b'10.5', b'20.5']

        # Swap back
        r.execute_command("SWAPDB", 0, 1)

        # Verify that data is intact after swapping back
        r.select(0)
        range_result0_after = r.execute_command("TS.RANGE", ts_key0, 0, 3000)
        assert len(range_result0_after) == 2
        assert [point[1] for point in range_result0_after] == [b'10.5', b'20.5']

        r.select(1)
        range_result1_after = r.execute_command("TS.RANGE", ts_key1, 0, 3000)
        assert len(range_result1_after) == 2
        assert [point[1] for point in range_result1_after] == [b'100.5', b'200.5']

    def test_del_after_swapdb_leaves_no_phantom(self):
        """SWAPDB swaps the indexes but not the series, whose cached db went stale: a DEL
        afterwards removed the id from the wrong db's index and left a phantom entry."""
        r = self.client
        r.select(0)
        key = self.generate_unique_key("swapdel")
        r.execute_command("TS.CREATE", key, "LABELS", "swapdel", "yes")
        r.execute_command("SWAPDB", 0, 1)

        r.select(1)
        assert r.execute_command("TS.QUERYINDEX", "swapdel=yes") == [key.encode()]
        r.delete(key)
        assert r.execute_command("TS.QUERYINDEX", "swapdel=yes") == []
        r.select(0)
        assert r.execute_command("TS.QUERYINDEX", "swapdel=yes") == []
