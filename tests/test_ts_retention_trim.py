"""The background retention trim reaches every eligible series in a db."""
from valkeytestframework.conftest import resource_port_tracker
from valkeytestframework.util.waiters import wait_for_true
from valkey_timeseries_test_case import ValkeyTimeSeriesTestCaseDebugMode


class TestRetentionTrim(ValkeyTimeSeriesTestCaseDebugMode):

    def samples_deleted(self):
        return self.debug_metrics('task')['task_retention_trim_samples_deleted_total']

    def test_trim_scans_past_ineligible_series(self):
        """More than a trim batch (40) of ineligible series with lower ids must not end the
        scan: the eligible series after them still gets its deferred expired samples dropped."""
        for i in range(60):
            self.client.execute_command('TS.ADD', f'ts:noretention:{i}', 1, 1, 'RETENTION', 0)

        # A partly expired compressed head chunk is left for the background trim (see
        # `TimeSeries::trim_lazily`): 59 of its samples fall below the floor 1060 - 1000.
        self.client.execute_command('TS.CREATE', 'ts:retained', 'RETENTION', 1000)
        for ts in range(1, 101):
            self.client.execute_command('TS.ADD', 'ts:retained', ts, ts)
        self.client.execute_command('TS.ADD', 'ts:retained', 1060, 1060)

        before = self.samples_deleted()
        wait_for_true(lambda: self.samples_deleted() > before, timeout=40)
