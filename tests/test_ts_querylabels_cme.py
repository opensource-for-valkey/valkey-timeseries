from valkey import Valkey, ValkeyCluster

from valkey_timeseries_test_case import ValkeyTimeSeriesClusterTestCase
from valkeytestframework.conftest import resource_port_tracker

# use hash tags to ensure keys are distributed across cluster nodes
TS1 = b'ts:{1}:cpu'
TS2 = b'ts:{2}:cpu'
TS3 = b'ts:{3}:mem'
TS4 = b'ts:{1}:mem'
TS5 = b'ts:{2}:net'


class TestTsQueryLabels(ValkeyTimeSeriesClusterTestCase):
    """TS.QUERYLABELS is a cross-slot (all-shards) query: the coordinator fans out to
    every shard and deduplicates the per-shard label names/values into one set."""

    def setup_test_data(self, client):
        client.execute_command('TS.CREATE', TS1, 'LABELS', 'metric', 'cpu', 'host', 'h1', 'region', 'us')
        client.execute_command('TS.CREATE', TS2, 'LABELS', 'metric', 'cpu', 'host', 'h2', 'region', 'eu')
        client.execute_command('TS.CREATE', TS3, 'LABELS', 'metric', 'mem', 'host', 'h1')
        client.execute_command('TS.CREATE', TS4, 'LABELS', 'metric', 'mem', 'host', 'h2', 'region', 'us')
        client.execute_command('TS.CREATE', TS5, 'LABELS', 'metric', 'net', 'host', 'h3')

    def test_labels_across_shards(self):
        cluster: ValkeyCluster = self.new_cluster_client()
        client = self.new_client_for_primary(0)
        self.setup_test_data(cluster)

        # Every distinct label name across all shards, deduplicated.
        result = client.execute_command('TS.QUERYLABELS', 'LABELS')
        assert sorted(result) == [b'host', b'metric', b'region']

    def test_values_across_shards(self):
        cluster: ValkeyCluster = self.new_cluster_client()
        client = self.new_client_for_primary(0)
        self.setup_test_data(cluster)

        # host and region values span shards; each appears once.
        assert sorted(client.execute_command('TS.QUERYLABELS', 'VALUES', 'host')) == [b'h1', b'h2', b'h3']
        assert sorted(client.execute_command('TS.QUERYLABELS', 'VALUES', 'region')) == [b'eu', b'us']

    def test_filter_scopes_to_matching_shards(self):
        cluster: ValkeyCluster = self.new_cluster_client()
        client = self.new_client_for_primary(0)
        self.setup_test_data(cluster)

        result = client.execute_command('TS.QUERYLABELS', 'LABELS', 'FILTER', 'metric=cpu')
        assert sorted(result) == [b'host', b'metric', b'region']

        # mem:1 has no region; the absent-label path still works across shards.
        result = client.execute_command('TS.QUERYLABELS', 'VALUES', 'region', 'FILTER', 'metric=mem')
        assert sorted(result) == [b'us']
