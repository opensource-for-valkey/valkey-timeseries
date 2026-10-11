"""A shard counts its instant reads toward its own `ts-promql-max-samples-in-flight`.

A shard holds every sample it reads on a coordinator's behalf for as long as the read takes,
and counts them toward its process-wide in-flight limit. Range and grid reads did; instant
reads did not: neither a plain instant selector's share nor the input to a pushed-down
aggregation, which the coordinator never sees at all (only the groups it reduces to come back).
A shard configured with a small limit served them regardless.

The fixture puts every series on one shard and gives only that shard a limit below the
series count, so a refusal can only come from that shard's own accounting.
"""

import pytest
from valkey import ResponseError, ValkeyCluster
from valkeytestframework.conftest import resource_port_tracker

from query_result import QueryResult
from valkey_timeseries_test_case import ValkeyTimeSeriesClusterTestCase

IN_FLIGHT_CONFIG = 'ts.ts-promql-max-samples-in-flight'
MAX_SERIES_CONFIG = 'ts.ts-promql-max-response-series'

T0 = 1_700_000_000  # seconds: TS.QUERY reads TIME as Unix seconds
SERIES = 50
# Below SERIES: one instant sample per series is more than the shard admits.
SHARD_LIMIT = 10

QUERIES = [
    pytest.param('in_flight_metric', SERIES, id='selector'),
    pytest.param('count(in_flight_metric)', 1, id='aggregation-pushdown'),
]


class TestInstantReadsCountInFlight(ValkeyTimeSeriesClusterTestCase):

    def tag_on_primary(self, cluster: ValkeyCluster, index: int) -> str:
        port = self.get_primary_port(index)
        for i in range(10_000):
            tag = f'inflight{i}'
            if cluster.get_node_from_key('{%s}' % tag).port == port:
                return tag
        raise AssertionError(f'no tag found for primary {index}')

    def load(self):
        cluster = self.new_cluster_client()
        tag = self.tag_on_primary(cluster, 1)
        for s in range(SERIES):
            key = f'm:{{{tag}}}:{s}'
            cluster.execute_command('TS.CREATE', key, 'METRIC', f'in_flight_metric{{s="{s}"}}')
            cluster.execute_command('TS.ADD', key, T0 * 1000, s)

    def instant(self, query: str) -> QueryResult:
        coordinator = self.client_for_primary(0)
        return QueryResult.from_raw(
            coordinator.execute_command('TS.QUERY', query, 'TIME', T0))

    @pytest.mark.parametrize('query, expected', QUERIES)
    def test_a_shard_refuses_an_instant_read_past_its_in_flight_limit(self, query, expected):
        self.load()
        # The coordinator bounds nothing here: every series is admitted on its side.
        self.client_for_primary(0).execute_command('CONFIG', 'SET', MAX_SERIES_CONFIG, 0)

        assert len(self.instant(query).result) == expected, 'answers under the default limit'

        shard = self.client_for_primary(1)
        shard.execute_command('CONFIG', 'SET', IN_FLIGHT_CONFIG, SHARD_LIMIT)
        with pytest.raises(ResponseError, match='ts-promql-max-samples-in-flight'):
            self.instant(query)

        # The refused read gave its samples back: a limit with room for exactly one read
        # admits the next, which a leaked charge would push over it.
        shard.execute_command('CONFIG', 'SET', IN_FLIGHT_CONFIG, SERIES)
        assert len(self.instant(query).result) == expected
