"""A PromQL query still ends at its deadline when the coordinator's fan-out lane is busy.

The coordinator runs two kinds of fan-out work on its request lane besides its peers' requests:

* the decode of a large shard response (64 KiB and up), moved off the main thread, and
* the local share of a fan-out.

The lane's workers can all be busy with peers' reads, and work queued behind them waits.
Two paths used to let that wait outlast the query's deadline:

* The RPC counted a deferred response as answered on arrival and stopped its deadline timer
  once every remote shard had answered, though the fan-out was still waiting on the decode.
* A fan-out whose only target is the local node has no RPC timer at all.

Either way the query's selector read waited on an unbounded `recv()`, so the query (and its
worker) waited for the lane to drain. Now the timer stays armed until the fan-out completes,
and the read's wait is bounded by the query's deadline.

`TS._DEBUG HOLD_FANOUT_LANE <ms>` (debug mode only) occupies every lane worker, standing in
for a lane saturated by peers' reads.
"""

import random
import time

import pytest
from valkey import ResponseError, Valkey, ValkeyCluster
from valkeytestframework.conftest import resource_port_tracker
from valkeytestframework.util.waiters import wait_for_true

from valkey_timeseries_test_case import ValkeyTimeSeriesClusterTestCaseDebugMode

T0 = 1_700_000_000_000
SAMPLES = 1000
SERIES = 40
STEP_MS = 1000

TIMEOUT_MS = 1000
# Long enough that answering when the lane frees cannot pass for answering at the deadline.
HOLD_MS = 6000
# The deadline plus the read's grace period plus scheduling slack, well short of HOLD_MS.
ANSWER_BY_S = 3.5


class TestQueryDeadlineUnderBusyLane(ValkeyTimeSeriesClusterTestCaseDebugMode):

    def tag_on_primary(self, cluster: ValkeyCluster, index: int) -> str:
        """A hash tag whose slot primary `index` owns."""
        port = self.get_primary_port(index)
        for i in range(10_000):
            tag = f'lane{i}'
            if cluster.get_node_from_key('{%s}' % tag).port == port:
                return tag
        raise AssertionError(f'no tag found for primary {index}')

    def load(self, cluster: ValkeyCluster, tag: str):
        """SERIES random-valued series on `tag`'s shard: a range read of them answers with a
        response of several hundred KiB, far past the deferred-decode threshold."""
        rng = random.Random(7)
        for s in range(SERIES):
            key = f'busy:{{{tag}}}:{s}'
            cluster.execute_command('TS.CREATE', key, 'METRIC', f'busy_lane{{tag="{tag}",s="{s}"}}')
            args = ['TS.MADD']
            for i in range(SAMPLES):
                args += [key, T0 + i * STEP_MS, rng.random() * 1e6]
            cluster.execute_command(*args)

    def query(self, client: Valkey, tag: str, timeout_ms=None):
        args = ['TS.QUERYRANGE', f'busy_lane{{tag="{tag}"}}',
                'START', T0, 'END', T0 + (SAMPLES - 1) * STEP_MS, 'STEP', '1s',
                'HASHTAG', tag]
        if timeout_ms is not None:
            args += ['TIMEOUT', f'{timeout_ms}ms']
        return client.execute_command(*args)

    def assert_times_out_at_its_deadline(self, coordinator: Valkey, tag: str, while_held=None):
        """The query times out at its deadline though the lane is held past it; then
        `while_held` runs, before the hold ends."""
        assert self.query(coordinator, tag), 'the query answers while the lane is free'

        held = coordinator.execute_command('TS._DEBUG', 'HOLD_FANOUT_LANE', HOLD_MS)
        assert held >= 1
        started = time.monotonic()
        with pytest.raises(ResponseError, match='timed out|timeframe'):
            self.query(coordinator, tag, TIMEOUT_MS)
        elapsed = time.monotonic() - started
        assert elapsed < ANSWER_BY_S, (
            f'answered after {elapsed:.2f}s: the query waited for the lane ({HOLD_MS} ms hold), '
            f'not its {TIMEOUT_MS} ms deadline')
        if while_held is not None:
            while_held()

        # Once the lane frees, the coordinator is unaffected and the same query answers.
        time.sleep(max(0.0, HOLD_MS / 1000 - elapsed) + 0.5)
        assert self.query(coordinator, tag)

    def test_a_deferred_decode_stuck_behind_a_busy_lane_times_out_at_the_deadline(self):
        cluster = self.new_cluster_client()
        tag = self.tag_on_primary(cluster, 1)
        self.load(cluster, tag)

        def fanout_timed_out():
            # The fan-out itself ended at its deadline, with its decode still queued — not
            # just the query's wait for it, and not when the lane got to the decode.
            wait_for_true(lambda: self.get_primary(0).verify_string_in_logfile(
                'promql: cluster command failed'), timeout=1)

        # The coordinator holds none of the data: its only work on the lane is the decode.
        self.assert_times_out_at_its_deadline(
            self.new_client_for_primary(0), tag, while_held=fanout_timed_out)

    def test_a_local_share_stuck_behind_a_busy_lane_times_out_at_the_deadline(self):
        cluster = self.new_cluster_client()
        tag = self.tag_on_primary(cluster, 0)
        self.load(cluster, tag)

        # Routed to the coordinator alone: no RPC, so no timer; the read's own bound ends it.
        self.assert_times_out_at_its_deadline(self.new_client_for_primary(0), tag)
