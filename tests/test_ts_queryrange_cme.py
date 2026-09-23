from typing import List

import pytest
from valkey import ResponseError
from valkeytestframework.util.waiters import *
from valkeytestframework.conftest import resource_port_tracker
from valkey_timeseries_test_case import ValkeyTimeSeriesClusterTestCase
from query_result import QueryResult

# Hash tags whose slots land on primaries 0, 1 and 2 of the 3-node test cluster,
# so each series below is stored on the primary its tag names.
TAG_BY_NODE = {0: 'h2', 1: 'h1', 2: 'h0'}


class TestTsQueryRangeCluster(ValkeyTimeSeriesClusterTestCase):
    """Cluster-mode regressions for TS.QUERYRANGE fanout behavior: series are
    spread over the three primaries, and every query is sent to primary 0."""

    def get_config_file_lines(self, test_dir, port) -> List[str]:
        # The selected-DB test needs a second database in cluster mode.
        return super().get_config_file_lines(test_dir, port) + ["cluster-databases 16"]

    def node_client(self, node: int, db: int = 0):
        """A connection to primary `node`, with database `db` selected."""
        client = self.new_client_for_primary(node)
        if db:
            client.execute_command("SELECT", db)
        return client

    def create(self, name: str, node: int, metric: str, samples, db: int = 0):
        """Create `metric` on primary `node` and add `samples` (timestamp, value)."""
        key = f"{name}:{{{TAG_BY_NODE[node]}}}"
        client = self.node_client(node, db)
        client.execute_command("TS.CREATE", key, "METRIC", metric)
        for timestamp, value in samples:
            client.execute_command("TS.ADD", key, timestamp, value)

    def range_query(self, query: str, start: str, end: str, step: str, db: int = 0):
        result = self.node_client(0, db).execute_command(
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
        t0 = "2026-04-06T20:00:00Z"
        t1 = "2026-04-06T20:01:00Z"

        self.create('db0-a', 0, 'db_fanout_range{db="0",series="a"}', [(t0, 10), (t1, 11)], db=0)
        self.create('db0-b', 1, 'db_fanout_range{db="0",series="b"}', [(t0, 20), (t1, 21)], db=0)
        self.create('db1-a', 0, 'db_fanout_range{db="1",series="a"}', [(t0, 100), (t1, 101)], db=1)
        self.create('db1-b', 2, 'db_fanout_range{db="1",series="b"}', [(t0, 200), (t1, 201)], db=1)

        db0_result = self.range_query('db_fanout_range', t0, t1, '60s', db=0)
        assert db0_result.is_matrix()
        assert len(db0_result.result) == 2
        assert {sample.metric['db'] for sample in db0_result.result} == {'0'}

        db1_result = self.range_query('db_fanout_range', t0, t1, '60s', db=1)
        assert db1_result.is_matrix()
        assert len(db1_result.result) == 2
        assert {sample.metric['db'] for sample in db1_result.result} == {'1'}

    def test_queryrange_across_shards(self):
        """A range query gathers every step of series stored on all three primaries."""
        t0 = "2026-04-06T20:00:00Z"
        t1 = "2026-04-06T20:01:00Z"
        for node in range(3):
            self.create(f'range-{node}', node, f'spread_range{{node="{node}"}}',
                        [(t0, node), (t1, node + 10)])

        result = self.range_query('spread_range', t0, t1, '60s')
        assert result.is_matrix()
        by_node = {sample.metric['node']: [p.value for p in sample.values] for sample in result.result}
        assert by_node == {'0': [0.0, 10.0], '1': [1.0, 11.0], '2': [2.0, 12.0]}
