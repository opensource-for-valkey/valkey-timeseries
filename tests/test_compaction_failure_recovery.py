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
        return info["ts_compaction_failures_total"], info["ts_compaction_state_rebuilds_total"]

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
        c.execute_command("TS.ADD", "d:src", 0, 1)
        c.execute_command("TS.ADD", "d:src", 300, 2)
        c.execute_command("TS._DEBUG", "FAIL_NEXT_COMPACTION")
        c.execute_command("TS.ADD", "d:src", 600, 4)     # stored; the rule misses it (state 1 + 2)
        assert self.info_debug(c, "d:src")[b"rulesState"][0] == [b"d:dst", 0, 1, b"3", 1]
        c.execute_command("TS.DEL", "d:src", 300, 300)   # its resync rebuilds the open bucket
        # 1 + 4 from the source; a removal that skipped the resync would leave the stale 3.
        assert self.info_debug(c, "d:src")[b"rulesState"][0] == [b"d:dst", 0, 1, b"5", 0]

    def build_chain(self, prefix):
        """src -> A (sum, 5000 ms, first rule) and src -> B (sum, 1000 ms, second rule);
        B's destination has its own rule C (sum, 2000 ms)."""
        c = self.client
        src, a, b, c_out = (f"{prefix}:{n}" for n in ("src", "a", "b", "c"))
        for key in (src, a, b, c_out):
            c.execute_command("TS.CREATE", key)
        c.execute_command("TS.CREATERULE", src, a, "AGGREGATION", "sum", 5000)
        c.execute_command("TS.CREATERULE", src, b, "AGGREGATION", "sum", 1000)
        c.execute_command("TS.CREATERULE", b, c_out, "AGGREGATION", "sum", 2000)
        return src, a, b, c_out

    def test_a_failing_rule_does_not_abandon_its_siblings_cascade(self):
        """Rule A errors on the write on which sibling rule B closes a bucket into B's destination,
        which has a rule of its own (C). B's output must still cascade into C, or C's next ingest
        would treat the following sample as an append on top of state that never saw this one."""
        c = self.client
        src, a, b, c_out = self.build_chain("x")
        twin_src, twin_a, twin_b, twin_c = self.build_chain("t")
        failures, rebuilds = self.counters()

        # A single-sample write runs the rules in order, so the injected failure hits A (created
        # first) and spares B. The twin chain takes the same writes without any failure.
        for key in (src, twin_src):
            c.execute_command("TS.ADD", key, 0, 1)
        c.execute_command("TS._DEBUG", "FAIL_NEXT_COMPACTION")
        c.execute_command("TS.ADD", src, 1000, 2)      # A fails; B closes bucket 0 into b
        c.execute_command("TS.ADD", twin_src, 1000, 2)

        assert self.counters() == (failures + 1, rebuilds)
        rules = self.info_debug(c, src)[b"rulesState"]
        assert [r[0] for r in rules] == [a.encode(), b.encode()]
        assert [r[4] for r in rules] == [1, 0]        # only A is dirty
        assert c.execute_command("TS.RANGE", b, "-", "+") == [[0, b"1"]]
        # B's closed bucket reached C: its open bucket 0 holds that sample.
        assert self.info_debug(c, b)[b"rulesState"] == [[c_out.encode(), 0, 1, b"1", 0]]

        for key in (src, twin_src):
            c.execute_command("TS.ADD", key, 2000, 4)  # A heals; B closes bucket 1000 into b
        assert self.counters() == (failures + 1, rebuilds + 1)
        assert self.info_debug(c, src)[b"rulesState"][0] == [a.encode(), 0, 1, b"7", 0]
        for key in (src, twin_src):
            c.execute_command("TS.ADD", key, 3000, 8)  # B closes 2000; C closes bucket 0 into c

        assert c.execute_command("TS.RANGE", b, "-", "+") == [[0, b"1"], [1000, b"2"], [2000, b"4"]]
        assert c.execute_command("TS.RANGE", c_out, "-", "+") == [[0, b"3"]]
        for injected, twin in ((b, twin_b), (c_out, twin_c)):
            assert c.execute_command("TS.RANGE", injected, "-", "+") == c.execute_command(
                "TS.RANGE", twin, "-", "+"
            )

    def test_a_failed_removal_still_deletes_and_succeeds(self):
        """The source delete is committed before the rules run, so TS.DEL must not turn a rule's
        failure into an error reply: that would skip replication and the `ts.del` event, and a
        replica (or the AOF) would keep samples the primary deleted. The rule is left dirty."""
        c = self.client
        self.make_rule("fd:src", "fd:dst")
        for ts, value in ((0, 1), (300, 2), (600, 4)):
            c.execute_command("TS.ADD", "fd:src", ts, value)
        failures, rebuilds = self.counters()

        c.execute_command("TS._DEBUG", "FAIL_NEXT_COMPACTION")
        assert c.execute_command("TS.DEL", "fd:src", 300, 300) == 1
        assert c.execute_command("TS.RANGE", "fd:src", "-", "+") == [[0, b"1"], [600, b"4"]]
        assert self.counters() == (failures + 1, rebuilds)
        # The failed resync left the streamed state (1 + 2 + 4) and marked the rule dirty.
        assert self.info_debug(c, "fd:src")[b"rulesState"][0] == [b"fd:dst", 0, 1, b"7", 1]

        c.execute_command("TS.ADD", "fd:src", 1000, 8)  # rebuilds (1 + 4), then closes bucket 0
        assert self.counters() == (failures + 1, rebuilds + 1)
        assert c.execute_command("TS.RANGE", "fd:dst", "-", "+") == [[0, b"5"]]

    def test_a_failed_removal_does_not_stop_mdel(self):
        """TS.MDEL keeps going past a series whose rule fails: every matching series is deleted
        from, and the other series' rule still resyncs its open bucket."""
        c = self.client
        keys = ("md:a", "md:b")
        for key in keys:
            c.execute_command("TS.CREATE", key, "LABELS", "group", "fail_mdel")
            c.execute_command("TS.CREATE", f"{key}:dst")
            c.execute_command("TS.CREATERULE", key, f"{key}:dst", "AGGREGATION", "sum", 1000)
            for ts, value in ((0, 1), (300, 2), (600, 4)):
                c.execute_command("TS.ADD", key, ts, value)
        failures, rebuilds = self.counters()

        c.execute_command("TS._DEBUG", "FAIL_NEXT_COMPACTION")
        assert c.execute_command("TS.MDEL", 300, 300, "FILTER", "group=fail_mdel") == 2
        for key in keys:
            assert c.execute_command("TS.RANGE", key, "-", "+") == [[0, b"1"], [600, b"4"]]
        assert self.counters() == (failures + 1, rebuilds)

        states = sorted((self.info_debug(c, key)[b"rulesState"][0] for key in keys), key=lambda r: r[4])
        # One rule failed (stale 7, dirty); the other resynced from the source (1 + 4).
        assert [(s[3], s[4]) for s in states] == [(b"5", 0), (b"7", 1)]

    def test_a_failed_removal_leaves_the_next_level_alone(self):
        """src -> mid fails on TS.DEL src 0 0, so mid keeps its bucket 0. dst must keep counting
        it: a level recomputes from what its parent holds, so an unchanged parent leaves it
        unchanged, and its own rule stays clean."""
        c = self.client
        for key in ("fc:src", "fc:mid", "fc:dst"):
            c.execute_command("TS.CREATE", key)
        c.execute_command("TS.CREATERULE", "fc:src", "fc:mid", "AGGREGATION", "sum", 1000)
        c.execute_command("TS.CREATERULE", "fc:mid", "fc:dst", "AGGREGATION", "sum", 2000)
        for ts, value in ((0, 1), (1000, 2), (2000, 4), (3000, 8), (4000, 16)):
            c.execute_command("TS.ADD", "fc:src", ts, value)
        mid_before = c.execute_command("TS.RANGE", "fc:mid", "-", "+")
        assert c.execute_command("TS.RANGE", "fc:dst", "-", "+") == [[0, b"3"]]

        c.execute_command("TS._DEBUG", "FAIL_NEXT_COMPACTION")
        assert c.execute_command("TS.DEL", "fc:src", 0, 0) == 1
        assert self.info_debug(c, "fc:src")[b"rulesState"][0][4] == 1
        assert c.execute_command("TS.RANGE", "fc:mid", "-", "+") == mid_before
        assert c.execute_command("TS.RANGE", "fc:dst", "-", "+") == [[0, b"3"]]
        assert self.info_debug(c, "fc:mid")[b"rulesState"][0][4] == 0
