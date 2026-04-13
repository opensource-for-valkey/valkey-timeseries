import math
import pytest
from datetime import datetime, timezone
from valkey import ResponseError
from valkeytestframework.util.waiters import *
from valkeytestframework.conftest import resource_port_tracker
from valkey_timeseries_test_case import ValkeyTimeSeriesTestCaseBase
from query_result import (
    QueryResult,
)


class TestTsQueryCluster(ValkeyTimeSeriesTestCaseBase):
    """Cluster-mode tests for TS.QUERY (PromQL support)."""

    def get_cluster_env(self):
        return {
            'cluster': True,
            'num_shards': 3,
            'num_replicas': 1,
        }

    def instant_query(self, query: str, time: str | int = None):
        args = ['TS.QUERY', query]
        if time is not None:
            args.extend(['TIME', str(time)])
        result = self.client.execute_command(*args)
        return QueryResult.from_raw(result)

    def setup_http_requests_cluster(self):
        """Create a small fleet of metrics distributed across shards using hash-tags."""
        client = self.client

        fleet = [
            ("web-prod-1", 'http_requests_total{server="web_prod_1", environment="production"}', 100, 110, 'shard0'),
            ("web-prod-2", 'http_requests_total{server="web_prod_2", environment="production"}', 200, 220, 'shard1'),
            ("web-prod-3", 'http_requests_total{server="web_prod_3", environment="production"}', 300, 330, 'shard2'),
            ("web-stg-1", 'http_requests_total{server="web_stg_1", environment="staging"}', 10, 20, 'shard0'),
        ]

        t0 = "2026-04-06T20:00:00Z"
        t1 = "2026-04-06T20:01:00Z"

        for key_base, metric, v0, v1, shard in fleet:
            # Use hashtags to force distribution across shards
            key = f"{key_base}:{{{shard}}}"
            client.execute_command("TS.CREATE", key, "METRIC", metric)
            client.execute_command("TS.ADD", key, t0, v0)
            client.execute_command("TS.ADD", key, t1, v1)

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
        """Binary operators with `on` should work across cluster shards."""
        client = self.client

        # Create two metric families on different shards using hash tags
        client.execute_command("TS.CREATE", "prod-web-1-requests:{shard0}", 'METRIC',
                               'http_requests_total{job="web-server", env="prod", instance="server1"}')
        client.execute_command("TS.CREATE", "prod-web-2-requests:{shard1}", 'METRIC',
                               'http_requests_total{job="web-server", env="prod", instance="server2"}')
        client.execute_command("TS.CREATE", "prod-web-cpu-usage-seconds:{shard0}", 'METRIC',
                               'node_cpu_usage_seconds_total{job="web-server", env="prod"}')

        timestamp = "2026-04-06T20:00:00Z"
        client.execute_command("TS.ADD", "prod-web-1-requests:{shard0}", timestamp, 1500)
        client.execute_command("TS.ADD", "prod-web-2-requests:{shard1}", timestamp, 2200)
        client.execute_command("TS.ADD", "prod-web-cpu-usage-seconds:{shard0}", timestamp, 45.5)

        query = 'http_requests_total{job=~"web.*"} / on(job, env) group_left() node_cpu_usage_seconds_total{job=~"web.*"}'
        result = self.instant_query(query, timestamp)

        assert result.is_vector()

    def test_invalid_promql_cluster(self):
        """Invalid PromQL should return ResponseError even in cluster mode."""
        # Create a simple metric on one shard so the query gets routed
        self.client.execute_command('TS.CREATE', 'some-metric:{shard0}', 'METRIC', 'm{a="b"}')

        with pytest.raises(ResponseError):
            self.client.execute_command('TS.QUERY', 'invalid {[}')
