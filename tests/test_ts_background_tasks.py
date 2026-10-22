"""Periodic maintenance tasks keep their wall-clock intervals when the cron frequency changes.

`dynamic-hz` (on by default) only raises `hz` past 200 clients per hz unit, which is impractical
to drive here; the scheduler's unit tests cover a period that changes on every tick.
"""
import time

from valkeytestframework.conftest import resource_port_tracker
from valkeytestframework.util.waiters import wait_for_true
from valkey_timeseries_test_case import ValkeyTimeSeriesTestCaseDebugMode


class TestTaskCadence(ValkeyTimeSeriesTestCaseDebugMode):

    def stats(self, *args):
        flat = self.client.execute_command('TS._DEBUG', 'STATS', *args)
        return {flat[i].decode(): flat[i + 1] for i in range(0, len(flat), 2)}

    def started(self):
        """Retention-trim dispatches, counting single-flight skips so they can't hide one."""
        stats = self.stats('task')
        return (stats['task_retention_trim_started_total']
                + stats['task_retention_trim_skipped_inflight_total'])

    def test_fast_hz_does_not_speed_up_tasks(self):
        wait_for_true(lambda: self.started() >= 1)
        self.client.config_set('hz', 500)
        before = self.started()
        time.sleep(5)
        assert self.started() - before <= 1

    def test_slow_hz_does_not_slow_tasks(self):
        wait_for_true(lambda: self.started() >= 1)
        self.client.config_set('hz', 1)
        before = self.started()
        wait_for_true(lambda: self.started() > before, timeout=15)
