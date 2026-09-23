import math
from typing import List

import pytest
from valkey import ResponseError
from valkeytestframework.util.waiters import *
from valkeytestframework.conftest import resource_port_tracker
from valkey_timeseries_test_case import ValkeyTimeSeriesClusterTestCase
from query_result import (
    QueryResult,
)

# Hash tags whose slots land on primaries 0, 1 and 2 of the 3-node test cluster,
# so each series below is stored on the primary its tag names.
TAG_BY_NODE = {0: 'h2', 1: 'h1', 2: 'h0'}


def cluster_key(name: str, node: int) -> str:
    return f"{name}:{{{TAG_BY_NODE[node]}}}"


class TestTsQueryCluster(ValkeyTimeSeriesClusterTestCase):
    """Cluster-mode tests for TS.QUERY (PromQL support): series are spread over
    the three primaries, and every query is sent to primary 0, which fans out."""

    def get_config_file_lines(self, test_dir, port) -> List[str]:
        # The selected-DB test needs a second database in cluster mode.
        return super().get_config_file_lines(test_dir, port) + ["cluster-databases 16"]

    def node_client(self, node: int, db: int = 0):
        """A connection to primary `node`, with database `db` selected."""
        client = self.new_client_for_primary(node)
        if db:
            client.execute_command("SELECT", db)
        return client

    def create(self, name: str, node: int, metric: str, samples, db: int = 0) -> str:
        """Create `metric` on primary `node` and add `samples` (timestamp, value)."""
        key = cluster_key(name, node)
        client = self.node_client(node, db)
        client.execute_command("TS.CREATE", key, "METRIC", metric)
        for timestamp, value in samples:
            client.execute_command("TS.ADD", key, timestamp, value)
        return key

    def instant_query(self, query: str, time: str | int = None, db: int = 0):
        args = ['TS.QUERY', query]
        if time is not None:
            args.extend(['TIME', str(time)])
        result = self.node_client(0, db).execute_command(*args)
        return QueryResult.from_raw(result)

    def setup_http_requests_cluster(self):
        """Create a small fleet of metrics distributed across the primaries."""
        fleet = [
            ("web-prod-1", 'http_requests_total{server="web_prod_1", environment="production"}', 100, 110, 0),
            ("web-prod-2", 'http_requests_total{server="web_prod_2", environment="production"}', 200, 220, 1),
            ("web-prod-3", 'http_requests_total{server="web_prod_3", environment="production"}', 300, 330, 2),
            ("web-stg-1", 'http_requests_total{server="web_stg_1", environment="staging"}', 10, 20, 0),
        ]

        t0 = "2026-04-06T20:00:00Z"
        t1 = "2026-04-06T20:01:00Z"

        for name, metric, v0, v1, node in fleet:
            self.create(name, node, metric, [(t0, v0), (t1, v1)])

        return t1

    def _vector_values_by_label(self, query_result: QueryResult, label: str) -> dict[str, float]:
        assert query_result.is_vector(), f"expected vector result, got {query_result.result_type}"
        return {sample.metric[label]: sample.value.value for sample in query_result.result}

    def _assert_single_value(self, query_result: QueryResult, expected: float):
        if query_result.is_scalar():
            actual = query_result.result.value
        else:
            assert query_result.is_vector(), f"expected scalar/vector, got {query_result.result_type}"
            assert len(query_result.result) == 1, f"expected one sample, got {len(query_result.result)}"
            actual = query_result.result[0].value.value
        assert math.isclose(actual, expected, rel_tol=1e-9), f"expected {expected}, got {actual}"

    def test_query_cross_shard_basic_metric_name(self):
        """TS.QUERY should return results from multiple shards."""
        time = self.setup_http_requests_cluster()

        result = self.instant_query('http_requests_total', time)
        values = self._vector_values_by_label(result, 'server')

        assert len(values) == 4
        assert values['web_prod_1'] == 110
        assert values['web_prod_2'] == 220
        assert values['web_prod_3'] == 330
        assert values['web_stg_1'] == 20

    def test_query_with_label_filter_cluster(self):
        """Label matchers should filter across shards in cluster mode."""
        time = self.setup_http_requests_cluster()

        result = self.instant_query('http_requests_total{environment="production"}', time)
        values = self._vector_values_by_label(result, 'server')

        assert sorted(values.keys()) == ['web_prod_1', 'web_prod_2', 'web_prod_3']

    def test_aggregation_sum_cluster(self):
        """Aggregation functions (sum) should aggregate across shards."""
        time = self.setup_http_requests_cluster()

        result = self.instant_query('sum(http_requests_total{environment="production"})', time)
        self._assert_single_value(result, 660.0)

    def test_operator_with_on_cluster(self):
        """Binary operators with `on` should match series stored on different shards."""
        timestamp = "2026-04-06T20:00:00Z"
        self.create("prod-web-1-requests", 0,
                    'http_requests_total{job="web-server", env="prod", instance="server1"}',
                    [(timestamp, 1500)])
        self.create("prod-web-2-requests", 1,
                    'http_requests_total{job="web-server", env="prod", instance="server2"}',
                    [(timestamp, 2200)])
        self.create("prod-web-cpu-usage-seconds", 2,
                    'node_cpu_usage_seconds_total{job="web-server", env="prod"}',
                    [(timestamp, 44)])

        query = ('http_requests_total{job=~"web.*"} / on(job, env) group_left() '
                 'node_cpu_usage_seconds_total{job=~"web.*"}')
        result = self.instant_query(query, timestamp)

        values = self._vector_values_by_label(result, 'instance')
        assert values == {'server1': 1500 / 44, 'server2': 2200 / 44}

    def test_invalid_promql_cluster(self):
        """Invalid PromQL should return ResponseError even in cluster mode."""
        self.create('some-metric', 0, 'm{a="b"}', [])

        with pytest.raises(ResponseError):
            self.node_client(0).execute_command('TS.QUERY', 'invalid {[}')

    def test_query_fanout_respects_selected_db(self):
        """Fanout instant queries should execute in the caller-selected DB on every shard."""
        t0 = "2026-04-06T20:00:00Z"

        self.create('db0-a', 0, 'db_fanout_metric{db="0",series="a"}', [(t0, 10)], db=0)
        self.create('db0-b', 1, 'db_fanout_metric{db="0",series="b"}', [(t0, 20)], db=0)
        self.create('db1-a', 0, 'db_fanout_metric{db="1",series="a"}', [(t0, 100)], db=1)
        self.create('db1-b', 2, 'db_fanout_metric{db="1",series="b"}', [(t0, 200)], db=1)

        db0_result = self.instant_query('db_fanout_metric', t0, db=0)
        assert db0_result.is_vector()
        assert len(db0_result.result) == 2
        assert {sample.metric['db'] for sample in db0_result.result} == {'0'}
        assert sorted(sample.value.value for sample in db0_result.result) == [10.0, 20.0]

        db1_result = self.instant_query('db_fanout_metric', t0, db=1)
        assert db1_result.is_vector()
        assert len(db1_result.result) == 2
        assert {sample.metric['db'] for sample in db1_result.result} == {'1'}
        assert sorted(sample.value.value for sample in db1_result.result) == [100.0, 200.0]
