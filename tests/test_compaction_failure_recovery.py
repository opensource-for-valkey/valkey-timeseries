"""A rule application that fails leaves the rule `dirty`; the next write rebuilds the open bucket
from the source before applying. Driven by `TS._DEBUG FAIL_NEXT_COMPACTION` (debug mode only)."""

from valkeytestframework.conftest import resource_port_tracker
from valkey_timeseries_test_case import ValkeyTimeSeriesTestCaseDebugMode


class TestCompactionFailureRecovery(ValkeyTimeSeriesTestCaseDebugMode):
    @staticmethod
    def info_debug(client, key):
        flat = client.execute_command("TS.INFO", key, "DEBUG")
        return {flat[i]: flat[i + 1] for i in range(0, len(flat), 2)}

    def counters(self):
        info = self.client.info("ts_compaction")
        return info["ts_compaction_failures"], info["ts_compaction_state_rebuilds"]

    def make_rule(self, src, dst):
        c = self.client
        c.execute_command("TS.CREATE", src)
        c.execute_command("TS.CREATE", dst)
        c.execute_command("TS.CREATERULE", src, dst, "AGGREGATION", "sum", 1000)

    def test_next_write_rebuilds_a_dirty_rule(self):
        c = self.client
        self.make_rule("f:src", "f:dst")
        c.execute_command("TS.ADD", "f:src", 0, 1)
        failures, rebuilds = self.counters()

        c.execute_command("TS._DEBUG", "FAIL_NEXT_COMPACTION")
        assert c.execute_command("TS.ADD", "f:src", 300, 2) == 300   # stored; the rule misses it
        assert self.counters() == (failures + 1, rebuilds)
        assert self.info_debug(c, "f:src")[b"rulesState"][0][4] == 1

        c.execute_command("TS.ADD", "f:src", 600, 4)   # rebuilds bucket 0 from the source first
        assert self.counters() == (failures + 1, rebuilds + 1)
        assert self.info_debug(c, "f:src")[b"rulesState"][0] == [b"f:dst", 0, 1, b"7", 0]
        c.execute_command("TS.ADD", "f:src", 1000, 8)
        assert c.execute_command("TS.RANGE", "f:dst", "-", "+") == [[0, b"7"]]

    def test_dirty_survives_reload(self):
        """Review focus 5."""
        c = self.client
        self.make_rule("r:src", "r:dst")
        c.execute_command("TS.ADD", "r:src", 0, 1)
        c.execute_command("TS._DEBUG", "FAIL_NEXT_COMPACTION")
        c.execute_command("TS.ADD", "r:src", 300, 2)
        c.execute_command("DEBUG", "RELOAD")
        assert self.info_debug(c, "r:src")[b"rulesState"][0][4] == 1
        c.execute_command("TS.ADD", "r:src", 1000, 8)  # rebuilds (1 + 2), then closes bucket 0
        assert c.execute_command("TS.RANGE", "r:dst", "-", "+") == [[0, b"3"]]
        assert self.info_debug(c, "r:src")[b"rulesState"][0][4] == 0

    def test_a_removal_clears_dirty(self):
        c = self.client
        self.make_rule("d:src", "d:dst")
        for ts in (0, 300):
            c.execute_command("TS.ADD", "d:src", ts, 1)
        c.execute_command("TS._DEBUG", "FAIL_NEXT_COMPACTION")
        c.execute_command("TS.ADD", "d:src", 600, 1)
        c.execute_command("TS.DEL", "d:src", 300, 300)   # its resync rebuilds the open bucket
        assert self.info_debug(c, "d:src")[b"rulesState"][0] == [b"d:dst", 0, 1, b"2", 0]
