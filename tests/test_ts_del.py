import pytest
from valkey import ResponseError
from valkeytestframework.util.waiters import *
from valkey_timeseries_test_case import ValkeyTimeSeriesTestCaseBase
from valkeytestframework.conftest import resource_port_tracker


class TestTsDel(ValkeyTimeSeriesTestCaseBase):
    def test_del_single_point(self):
        """Test deleting a single data point from a time series"""
        # Create a time series with one sample
        self.client.execute_command('TS.CREATE', 'ts1')
        self.client.execute_command('TS.ADD', 'ts1', 100, 10.5)

        # Delete the point
        deleted = self.client.execute_command('TS.DEL', 'ts1', 0, 200)
        assert deleted == 1

        # Verify the point was deleted
        result = self.client.execute_command('TS.RANGE', 'ts1', 0, 200)
        assert len(result) == 0

    def test_del_range_of_points(self):
        """Test deleting a range of data points"""
        # Create a time series with multiple points
        self.client.execute_command('TS.CREATE', 'ts2')
        for i in range(10):
            self.client.execute_command('TS.ADD', 'ts2', 100 + i * 10, i)

        # Delete points from 130 to 170
        deleted = self.client.execute_command('TS.DEL', 'ts2', 130, 170)
        assert deleted == 5  # Should delete 5 points (130, 140, 150, 160, 170)

        # Verify correct points were deleted
        result = self.client.execute_command('TS.RANGE', 'ts2', 0, 200)
        timestamps = [entry[0] for entry in result]
        assert 130 not in timestamps
        assert 140 not in timestamps
        assert 150 not in timestamps
        assert 160 not in timestamps
        assert 170 not in timestamps
        assert len(result) == 5  # 5 points should remain

    def test_del_empty_range(self):
        """Test deleting an empty range (no points in range)"""
        # Create a time series with points outside the delete range
        self.client.execute_command('TS.CREATE', 'ts3')
        self.client.execute_command('TS.ADD', 'ts3', 100, 1.0)
        self.client.execute_command('TS.ADD', 'ts3', 500, 5.0)

        # Delete range with no points
        deleted = self.client.execute_command('TS.DEL', 'ts3', 200, 400)
        assert deleted == 0

        # Verify all points still exist
        result = self.client.execute_command('TS.RANGE', 'ts3', 0, 1000)
        assert len(result) == 2

    def test_del_nonexistent_key(self):
        """Test attempting to delete from a nonexistent time series"""
        with pytest.raises(Exception) as excinfo:
            self.client.execute_command('TS.DEL', 'nonexistent', 0, 100)
        assert "the key does not exist" in str(excinfo.value)

    def test_del_boundary_conditions(self):
        """Test deleting points at the boundaries of the specified range"""
        # Create a time series with points at specific times
        self.client.execute_command('TS.CREATE', 'ts4')
        self.client.execute_command('TS.ADD', 'ts4', 100, 1.0)
        self.client.execute_command('TS.ADD', 'ts4', 200, 2.0)
        self.client.execute_command('TS.ADD', 'ts4', 300, 3.0)

        # Delete points including the boundaries
        deleted = self.client.execute_command('TS.DEL', 'ts4', 100, 200)
        assert deleted == 2  # Should delete points at 100 and 200

        # Verify only the point at 300 remains
        result = self.client.execute_command('TS.RANGE', 'ts4', 0, 1000)
        assert len(result) == 1
        assert result[0][0] == 300

    def test_del_with_large_dataset(self):
        """Test deleting from a larger dataset"""
        # Create a time series with 100 points
        self.client.execute_command('TS.CREATE', 'ts5')
        for i in range(100):
            self.client.execute_command('TS.ADD', 'ts5', 1000 + i * 10, i)

        # Delete a large range
        deleted = self.client.execute_command('TS.DEL', 'ts5', 1200, 1600)
        assert deleted == 41  # Should delete points from 1200 to 1600 inclusive

        # Verify the correct number of points remain
        result = self.client.execute_command('TS.RANGE', 'ts5', 0, 2000)
        assert len(result) == 59

    def test_del_with_multiple_chunks(self):
        """Test deleting across multiple chunks"""
        # Create a time series with a small chunk size
        self.client.execute_command('TS.CREATE', 'ts6', 'CHUNK_SIZE', '128')

        # Add enough points to span multiple chunks
        for i in range(50):
            self.client.execute_command('TS.ADD', 'ts6', 1000 + i * 10, i)

        # Delete points across chunk boundaries
        deleted = self.client.execute_command('TS.DEL', 'ts6', 1100, 1300)
        assert deleted > 0

        # Verify points were deleted
        result = self.client.execute_command('TS.RANGE', 'ts6', 1100, 1300)
        assert len(result) == 0

    def test_del_specific_timestamp(self):
        """Test deleting a specific timestamp (start == end)"""
        self.client.execute_command('TS.CREATE', 'ts8')
        for i in range(5):
            self.client.execute_command('TS.ADD', 'ts8', 1000 + i * 10, i)

        # Delete only the point at timestamp 1020
        deleted = self.client.execute_command('TS.DEL', 'ts8', 1020, 1020)
        assert deleted == 1

        # Verify only that a specific point was deleted
        result = self.client.execute_command('TS.RANGE', 'ts8', 0, 2000)
        assert len(result) == 4
        timestamps = [entry[0] for entry in result]
        assert 1020 not in timestamps

    def test_del_propagates_resolved_bounds(self):
        """TS.DEL propagates the bounds it resolved, not the ones it was given: `-`, `+`,
        `*` and relative bounds re-resolved on a replica or an AOF replay against a
        different clock or series would delete a different window."""
        import glob
        import os

        c = self.client
        c.config_set("appendonly", "yes")
        c.config_set("appendfsync", "always")
        wait_for_equal(lambda: c.info("persistence")["aof_rewrite_in_progress"], 0, timeout=30)
        for ts in (1000, 2000, 3000):
            c.execute_command("TS.ADD", "del:resolved", ts, 1)
        assert c.execute_command("TS.DEL", "del:resolved", "-", "+") == 3

        aof_dir = os.path.join(c.config_get("dir")["dir"], c.config_get("appenddirname")["appenddirname"])
        expected = b"*4\r\n$6\r\nTS.DEL\r\n$12\r\ndel:resolved\r\n$4\r\n1000\r\n$4\r\n3000\r\n"

        def aof_bytes():
            data = b""
            for path in glob.glob(os.path.join(aof_dir, "*.incr.aof")):
                with open(path, "rb") as f:
                    data += f.read()
            return data

        wait_for_true(lambda: expected in aof_bytes())

    def test_del_requires_exactly_two_bounds(self):
        c = self.client
        c.execute_command("TS.ADD", "del:arity", 1000, 1)
        for args in (("del:arity", 1000), ("del:arity", "-", "+", "garbage")):
            with pytest.raises(ResponseError, match="wrong number of arguments"):
                c.execute_command("TS.DEL", *args)
        assert c.execute_command("TS.RANGE", "del:arity", "-", "+") == [[1000, b"1"]]
