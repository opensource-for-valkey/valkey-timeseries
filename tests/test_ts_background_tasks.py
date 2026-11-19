"""Periodic maintenance tasks keep their wall-clock intervals when the cron frequency changes.

`dynamic-hz` (on by default) only raises `hz` past 200 clients per hz unit, which is impractical
to drive here; the scheduler's unit tests cover a period that changes on every tick.
"""
import time

from valkeytestframework.conftest import resource_port_tracker
from valkeytestframework.util.waiters import wait_for_true
from valkey_timeseries_test_case import ValkeyTimeSeriesTestCaseDebugMode


class TestTaskCadence(ValkeyTimeSeriesTestCaseDebugMode):

    def started(self):
        """Retention-trim dispatches, counting single-flight skips so they can't hide one."""
        stats = self.debug_metrics('task')
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

    def test_load_pauses_task_schedule(self):
        """Time spent loading does not count toward an interval: a task that came due during
        the load does not run as soon as it ends, but once its remaining time has passed."""
        wait_for_true(lambda: self.started() >= 1)
        for i in range(100):
            self.client.execute_command('TS.CREATE', f'ts:load:{i}')
        # 100 keys x 120 ms: the reload outlasts the 10 s trim interval.
        self.client.config_set('key-load-delay', 120_000)
        before = self.started()
        self.client.execute_command('DEBUG', 'RELOAD')
        self.client.config_set('key-load-delay', 0)
        time.sleep(3)
        assert self.started() == before
        wait_for_true(lambda: self.started() > before, timeout=15)


class TestRetentionTrimTask(ValkeyTimeSeriesTestCaseDebugMode):

    def completed_trims(self):
        """Trim passes that ran to the end. Dispatches (`started` + `skipped_inflight`) would not
        do: a dispatch skipped because a pass is still in flight scans nothing."""
        return self.debug_metrics('task')['task_retention_trim_completed_total']

    def test_trim_tick_leaves_untouched_series_unmodified(self):
        """The retention trim opens a series for writing only when it has samples to drop.
        It used to open every series it scanned for writing, so each tick signalled every
        series as modified: a WATCH on any of them aborted the transaction (and client-side
        caching was told to invalidate), although nothing had changed."""
        now = int(time.time() * 1000)
        self.client.execute_command('TS.CREATE', 'w:retention', 'RETENTION', 10_000_000)
        self.client.execute_command('TS.ADD', 'w:retention', now, 1)
        self.client.execute_command('TS.CREATE', 'w:plain')
        self.client.execute_command('TS.ADD', 'w:plain', now, 1)

        watchers = {}
        for key in ('w:retention', 'w:plain'):
            client = self.server.get_new_client()
            client.execute_command('WATCH', key)
            watchers[key] = client
        # Two completed passes after the WATCH. The first may be a pass already in flight when it
        # was taken; the second started after that one ended, so it scanned both keys entirely
        # while the WATCH was held.
        before = self.completed_trims()
        wait_for_true(lambda: self.completed_trims() >= before + 2, timeout=30)
        for key, client in watchers.items():
            client.execute_command('MULTI')
            client.execute_command('PING')
            assert client.execute_command('EXEC') is not None, f"WATCH on {key} was aborted"
