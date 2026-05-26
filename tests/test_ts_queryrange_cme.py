import pytest
from valkey import ResponseError
from valkeytestframework.util.waiters import *
from valkeytestframework.conftest import resource_port_tracker
from valkey_timeseries_test_case import ValkeyTimeSeriesTestCaseBase
from query_result import QueryResult


class TestTsQueryRangeCluster(ValkeyTimeSeriesTestCaseBase):
    """Cluster-mode regressions for TS.QUERYRANGE fanout behavior."""

    def get_cluster_env(self):
        return {
            'cluster': True,
            'num_shards': 3,
            'num_replicas': 1,
        }

    def _select_db_or_skip(self, db: int):
        try:
            self.client.execute_command("SELECT", db)
        except ResponseError as e:
            pytest.skip(f"cluster does not allow SELECT {db}: {e}")

    def range_query(self, query: str, start: str, end: str, step: str):
        result = self.client.execute_command(
            "TS.QUERYRANGE",
            query,
            "STEP",
            step,
            "START",
            start,
            "END",
            end,
        )
        return QueryResult.from_raw(result)

    def test_queryrange_fanout_respects_selected_db(self):
        """Fanout range queries should use the caller DB across all shards."""
        client = self.client
        t0 = "2026-04-06T20:00:00Z"
        t1 = "2026-04-06T20:01:00Z"

        self._select_db_or_skip(0)
        client.execute_command('TS.CREATE', 'db0-a:{shard0}', 'METRIC', 'db_fanout_range{db="0",series="a"}')
        client.execute_command('TS.CREATE', 'db0-b:{shard1}', 'METRIC', 'db_fanout_range{db="0",series="b"}')
        client.execute_command('TS.ADD', 'db0-a:{shard0}', t0, 10)
        client.execute_command('TS.ADD', 'db0-a:{shard0}', t1, 11)
        client.execute_command('TS.ADD', 'db0-b:{shard1}', t0, 20)
        client.execute_command('TS.ADD', 'db0-b:{shard1}', t1, 21)

        self._select_db_or_skip(1)
        client.execute_command('TS.CREATE', 'db1-a:{shard0}', 'METRIC', 'db_fanout_range{db="1",series="a"}')
        client.execute_command('TS.CREATE', 'db1-b:{shard2}', 'METRIC', 'db_fanout_range{db="1",series="b"}')
        client.execute_command('TS.ADD', 'db1-a:{shard0}', t0, 100)
        client.execute_command('TS.ADD', 'db1-a:{shard0}', t1, 101)
        client.execute_command('TS.ADD', 'db1-b:{shard2}', t0, 200)
        client.execute_command('TS.ADD', 'db1-b:{shard2}', t1, 201)

        self._select_db_or_skip(0)
        db0_result = self.range_query('db_fanout_range', t0, t1, '60s')
        assert db0_result.is_matrix()
        assert len(db0_result.result) == 2
        assert {sample.metric['db'] for sample in db0_result.result} == {'0'}

        self._select_db_or_skip(1)
        db1_result = self.range_query('db_fanout_range', t0, t1, '60s')
        assert db1_result.is_matrix()
        assert len(db1_result.result) == 2
        assert {sample.metric['db'] for sample in db1_result.result} == {'1'}

