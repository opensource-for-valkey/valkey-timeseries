import pytest
from valkey import ResponseError, ValkeyCluster
from valkeytestframework.util.waiters import *
from valkeytestframework.conftest import resource_port_tracker
from valkey_timeseries_test_case import ValkeyTimeSeriesClusterTestCase


class TestTimeSeriesMgetCluster(ValkeyTimeSeriesClusterTestCase):

    def setup_test_data(self):
        """Create time series distributed across different hash slots"""
        # Use hash tags to control slot distribution
        # Series with {tag1} will hash to same slot, {tag2} to different slot, etc.
        cluster_client: ValkeyCluster = self.new_cluster_client()

        cluster_client.execute_command('TS.CREATE', 'ts:{shard1}:cpu1', 'LABELS', 'name', 'cpu', 'type', 'usage', 'node',
                               'node1', 'region', 'us-east')
        cluster_client.execute_command('TS.CREATE', 'ts:{shard1}:cpu2', 'LABELS', 'name', 'cpu', 'type', 'usage', 'node',
                               'node2', 'region', 'us-east')
        cluster_client.execute_command('TS.CREATE', 'ts:{shard2}:cpu3', 'LABELS', 'name', 'cpu', 'type', 'usage', 'node',
                               'node3', 'region', 'us-west')
        cluster_client.execute_command('TS.CREATE', 'ts:{shard2}:cpu4', 'LABELS', 'name', 'cpu', 'type', 'temperature', 'node',
                               'node4', 'region', 'us-west')
        cluster_client.execute_command('TS.CREATE', 'ts:{shard3}:mem1', 'LABELS', 'name', 'memory', 'type', 'usage', 'node',
                               'node1', 'region', 'eu-central')
        cluster_client.execute_command('TS.CREATE', 'ts:{shard3}:mem2', 'LABELS', 'name', 'memory', 'type', 'usage', 'node',
                               'node2', 'region', 'eu-central')
        cluster_client.execute_command('TS.CREATE', 'ts:{shard1}:disk1', 'LABELS', 'name', 'disk', 'type', 'usage', 'node',
                               'node1', 'region', 'us-east')

        # Add samples to each time series
        current_time = 1000
        cluster_client.execute_command('TS.ADD', 'ts:{shard1}:cpu1', current_time, 10)
        cluster_client.execute_command('TS.ADD', 'ts:{shard1}:cpu2', current_time, 20)
        cluster_client.execute_command('TS.ADD', 'ts:{shard2}:cpu3', current_time, 30)
        cluster_client.execute_command('TS.ADD', 'ts:{shard2}:cpu4', current_time, 40)
        cluster_client.execute_command('TS.ADD', 'ts:{shard3}:mem1', current_time, 50)
        cluster_client.execute_command('TS.ADD', 'ts:{shard3}:mem2', current_time, 60)
        cluster_client.execute_command('TS.ADD', 'ts:{shard1}:disk1', current_time, 70)

        # Add additional samples with different timestamps
        cluster_client.execute_command('TS.ADD', 'ts:{shard1}:cpu1', current_time + 1000, 15)
        cluster_client.execute_command('TS.ADD', 'ts:{shard2}:cpu3', current_time + 1000, 35)
        cluster_client.execute_command('TS.ADD', 'ts:{shard3}:mem1', current_time + 1000, 55)

    def test_cluster_mget_cross_shard(self):
        """Test TS.MGET across multiple shards"""
        self.setup_test_data()

        # Get all CPU metrics that should span multiple shards
        client = self.new_client_for_primary(0)
        result = client.execute_command('TS.MGET', 'FILTER', 'name=cpu')
        result.sort(key=lambda x: x[0])

        assert len(result) == 4

        # Verify keys from different shards are included
        keys = [r[0] for r in result]
        assert b'ts:{shard1}:cpu1' in keys
        assert b'ts:{shard1}:cpu2' in keys
        assert b'ts:{shard2}:cpu3' in keys
        assert b'ts:{shard2}:cpu4' in keys

        # Verify latest values
        assert result[0][2][0] == 2000  # ts:{shard1}:cpu1
        assert result[0][2][1] == b'15'
        assert result[2][2][0] == 2000  # ts:{shard2}:cpu3
        assert result[2][2][1] == b'35'

    def test_mget_cme_with_withlabels(self):
        """Test TS.MGET with WITHLABELS across cluster"""
        self.setup_test_data()

        client = self.new_client_for_primary(0)
        result = client.execute_command('TS.MGET', 'WITHLABELS', 'FILTER', 'name=memory')
        result.sort(key=lambda x: x[0])

        assert len(result) == 2

        # Verify labels are returned from all shards
        for item in result:
            labels = item[1]
            assert len(labels) == 4  # 4 label pairs
            label_dict = {l[0]: l[1] for l in labels}
            assert label_dict[b'name'] == b'memory'
            assert label_dict[b'type'] == b'usage'
            assert label_dict[b'region'] == b'eu-central'

    def test_mget_cme_with_selected_labels(self):
        """Test TS.MGET with SELECTED_LABELS across cluster"""
        self.setup_test_data()

        client = self.new_client_for_primary(0)
        result = client.execute_command('TS.MGET', 'SELECTED_LABELS', 'name', 'region', 'FILTER', 'name=cpu')
        result.sort(key=lambda x: x[0])

        assert len(result) == 4

        # Verify only selected labels are returned from all shards
        for item in result:
            labels = item[1]
            # Should only have name and region labels
            label_names = {l[0] for l in labels if l is not None}
            assert label_names.issubset({b'name', b'region'})

    def test_mget_cme_complex_filter(self):
        """Test TS.MGET with complex filters across cluster"""
        self.setup_test_data()

        client = self.new_client_for_primary(0)

        # Filter by multiple conditions
        result = client.execute_command('TS.MGET', 'FILTER', 'name=cpu', 'type=usage')
        result.sort(key=lambda x: x[0])

        assert len(result) == 3
        assert result[0][0] == b'ts:{shard1}:cpu1'
        assert result[1][0] == b'ts:{shard1}:cpu2'
        assert result[2][0] == b'ts:{shard2}:cpu3'

        # Regex filter across shards
        result = client.execute_command('TS.MGET', 'FILTER', 'region=~"us-.*"')
        result.sort(key=lambda x: x[0])

        assert len(result) == 5
        # Verify all us-* region series are included
        regions = set()
        for item in result:
            # Keys should be from us-east or us-west
            if b'shard1' in item[0]:
                regions.add('us-east')
            elif b'shard2' in item[0]:
                regions.add('us-west')

        assert 'us-east' in regions
        assert 'us-west' in regions

    def test_mget_cme_single_shard(self):
        """Test TS.MGET when results are from a single shard"""
        self.setup_test_data()

        # Get all series from eu-central (should be on one shard)
        client = self.new_client_for_primary(0)
        result = client.execute_command('TS.MGET', 'FILTER', 'region=eu-central')
        result.sort(key=lambda x: x[0])

        assert len(result) == 2
        assert result[0][0] == b'ts:{shard3}:mem1'
        assert result[1][0] == b'ts:{shard3}:mem2'

    def test_mget_cme_with_latest_option(self):
        """Test TS.MGET with LATEST option across cluster"""
        self.setup_test_data()

        cluster_client: ValkeyCluster = self.new_cluster_client()

        # Create compaction rules on different shards
        cluster_client.execute_command('TS.CREATE', 'ts:{shard1}:cpu1:avg', 'LABELS', 'aggregation', 'avg', 'name', 'cpu')
        cluster_client.execute_command('TS.CREATERULE', 'ts:{shard1}:cpu1', 'ts:{shard1}:cpu1:avg', 'AGGREGATION', 'avg',
                                    5000)

        # Add more samples to trigger compaction
        current_time = 10000
        for i in range(10):
            value = 100 * (i + 1)
            ts = current_time + i * 1000
            cluster_client.execute_command('TS.ADD', 'ts:{shard1}:cpu1', ts, value)

        # Test LATEST option
        client = self.new_client_for_primary(0)
        result = client.execute_command('TS.MGET', 'LATEST', 'FILTER', 'name=cpu')

        assert len(result) == 5
        result.sort(key=lambda x: x[0])

        # Should include latest values including from compaction rules
        # find the entry for ts:{shard1}:cpu1:avg
        avg_entry = next((item for item in result if item[0] == b'ts:{shard1}:cpu1:avg'), None)
        assert avg_entry is not None
        # The latest value should correspond to the last bucket's average
        latest_samples = avg_entry[2]
        assert latest_samples[0] == 15000  # start of last bucket
        assert float(latest_samples[1]) == 800.0  # average of last bucket

    def test_mget_cme_empty_result(self):
        """Test TS.MGET with no matching series across cluster"""
        self.setup_test_data()

        client = self.new_client_for_primary(0)
        result = client.execute_command('TS.MGET', 'FILTER', 'name=nonexistent')
        assert result == []

    def test_mget_cme_partial_shards(self):
        """Test TS.MGET behavior when some shards have no matches"""
        self.setup_test_data()

        # Create series on specific shard
        cluster_client: ValkeyCluster = self.new_cluster_client()
        cluster_client.execute_command('TS.CREATE', 'ts:{unique}:test', 'LABELS', 'name', 'test', 'unique', 'true')
        cluster_client.execute_command('TS.ADD', 'ts:{unique}:test', 1000, 100)

        # Query for unique label - should only hit one shard
        client = self.new_client_for_primary(0)
        result = client.execute_command('TS.MGET', 'FILTER', 'unique=true')

        assert len(result) == 1
        assert result[0][0] == b'ts:{unique}:test'

    def test_mget_cme_large_result_set(self):
        """Test TS.MGET with large number of series across cluster"""
        # Create many series distributed across shards
        cluster_client: ValkeyCluster = self.new_cluster_client()
        num_series = 100
        for i in range(num_series):
            shard = f'shard{i % 3}'
            key = f'ts:{{{shard}}}:metric{i}'
            cluster_client.execute_command('TS.CREATE', key, 'LABELS', 'name', 'load_test', 'id', str(i))
            cluster_client.execute_command('TS.ADD', key, 1000, i)

        # Get all load_test metrics
        client = self.new_client_for_primary(0)
        result = client.execute_command('TS.MGET', 'FILTER', 'name=load_test')

        assert len(result) == num_series

        # Verify results are from all shards
        shard_counts = {'shard0': 0, 'shard1': 0, 'shard2': 0}
        for item in result:
            key = item[0].decode('utf-8')
            for shard in shard_counts:
                if shard in key:
                    shard_counts[shard] += 1
                    break

        # Each shard should have approximately num_series/3 series
        for count in shard_counts.values():
            assert count > 0

    def test_mget_cme_after_node_addition(self):
        """Test TS.MGET continues to work after cluster topology changes"""
        self.setup_test_data()

        # Initial query
        client = self.new_client_for_primary(0)
        result1 = client.execute_command('TS.MGET', 'FILTER', 'name=cpu')
        initial_count = len(result1)

        # Add more series (simulating data after potential resharding)
        cluster_client: ValkeyCluster = self.new_cluster_client()
        cluster_client.execute_command('TS.CREATE', 'ts:{new}:cpu5', 'LABELS', 'name', 'cpu', 'type', 'usage', 'node',
                                    'node5')
        cluster_client.execute_command('TS.ADD', 'ts:{new}:cpu5', 1000, 90)

        # Query again
        result2 = client.execute_command('TS.MGET', 'FILTER', 'name=cpu')

        assert len(result2) == initial_count + 1

    def test_mget_cme_with_no_samples(self):
        """Test TS.MGET with series that have no samples across cluster"""
        # Create empty series on different shards
        cluster_client: ValkeyCluster = self.new_cluster_client()
        cluster_client.execute_command('TS.CREATE', 'ts:{shard1}:empty1', 'LABELS', 'name', 'empty', 'shard', '1')
        cluster_client.execute_command('TS.CREATE', 'ts:{shard2}:empty2', 'LABELS', 'name', 'empty', 'shard', '2')
        cluster_client.execute_command('TS.CREATE', 'ts:{shard3}:empty3', 'LABELS', 'name', 'empty', 'shard', '3')

        client = self.new_client_for_primary(0)
        result = client.execute_command('TS.MGET', 'FILTER', 'name=empty')
        result.sort(key=lambda x: x[0])

        assert len(result) == 3
        # All should have empty samples
        for item in result:
            assert item[2] == []

    def test_mget_cme_concurrent_updates(self):
        """Test TS.MGET consistency during concurrent updates"""
        self.setup_test_data()

        # Perform MGET while adding new data
        cluster_client: ValkeyCluster = self.new_cluster_client()
        cluster_client.execute_command('TS.ADD', 'ts:{shard1}:cpu1', 5000, 200)

        client = self.new_client_for_primary(0)
        result = client.execute_command('TS.MGET', 'FILTER', 'name=cpu')

        # Should get results, potentially with new data
        assert len(result) >= 4

    def test_mget_cme_label_consistency(self):
        """Test that labels are consistently returned across all shards"""
        self.setup_test_data()

        client = self.new_client_for_primary(0)
        result = client.execute_command('TS.MGET', 'WITHLABELS', 'FILTER', 'type=usage')

        # Verify all results have consistent label structure
        for item in result:
            labels = item[1]
            label_dict = {l[0]: l[1] for l in labels}
            # All should have the 'type' label with value 'usage'
            assert label_dict[b'type'] == b'usage'

    def tag_per_primary(self, cluster_client: ValkeyCluster):
        """Return one hash tag per primary, each owned by a different node.

        The tag -> slot -> node mapping depends on how the harness splits the slot
        range across primaries, so the tags are discovered at runtime rather than
        hard-coded (e.g. {shard1} and {shard2} land on the same primary here).
        """
        by_node = {}
        for i in range(1000):
            tag = f'tag{i}'
            node = cluster_client.get_node_from_key('{%s}' % tag)
            by_node.setdefault((node.host, node.port), tag)
            if len(by_node) == self.CLUSTER_SIZE:
                break

        assert len(by_node) == self.CLUSTER_SIZE, \
            f'found tags for only {len(by_node)} of {self.CLUSTER_SIZE} primaries'
        return [by_node[node] for node in sorted(by_node)]

    def setup_tagged_data(self, cluster_client: ValkeyCluster):
        """One series per primary, all matching FILTER name=tagged."""
        tags = self.tag_per_primary(cluster_client)
        keys = []
        for i, tag in enumerate(tags):
            key = f'ts:{{{tag}}}:cpu'
            cluster_client.execute_command('TS.CREATE', key, 'LABELS', 'name', 'tagged', 'shard', str(i))
            cluster_client.execute_command('TS.ADD', key, 1000, i)
            keys.append(key.encode())
        return tags, keys

    def test_mget_cme_hashtag_scopes_fanout(self):
        """TS.MGET HASHTAG restricts fanout to the shards owning the tags' slots"""
        cluster_client: ValkeyCluster = self.new_cluster_client()
        tags, keys = self.setup_tagged_data(cluster_client)

        client = self.new_client_for_primary(0)

        def mget_keys(*args):
            result = client.execute_command('TS.MGET', *args, 'FILTER', 'name=tagged')
            return sorted(r[0] for r in result)

        # Without HASHTAG every shard answers.
        assert mget_keys() == sorted(keys)

        # A single tag reaches only the shard owning its slot, so each query sees
        # exactly the one series stored under that tag.
        for tag, key in zip(tags, keys):
            assert mget_keys('HASHTAG', tag) == [key], f'HASHTAG {tag}'

        # Tags are comma separated and the reply is their union.
        assert mget_keys('HASHTAG', ','.join(tags[:2])) == sorted(keys[:2])
        assert mget_keys('HASHTAG', ','.join(tags)) == sorted(keys)

        # A repeated tag resolves to the same shard, not a duplicated reply.
        assert mget_keys('HASHTAG', f'{tags[0]},{tags[0]}') == [keys[0]]

        # The braced form hashes to the same slot as the bare tag.
        assert mget_keys('HASHTAG', '{%s}' % tags[0]) == [keys[0]]

    def test_mget_cme_hashtag_with_other_options(self):
        """HASHTAG composes with the other TS.MGET options, in any position"""
        cluster_client: ValkeyCluster = self.new_cluster_client()
        tags, keys = self.setup_tagged_data(cluster_client)

        client = self.new_client_for_primary(0)

        result = client.execute_command('TS.MGET', 'WITHLABELS', 'HASHTAG', tags[1], 'FILTER', 'name=tagged')
        assert len(result) == 1
        assert result[0][0] == keys[1]
        assert {label_pair[0]: label_pair[1] for label_pair in result[0][1]} == {b'name': b'tagged', b'shard': b'1'}

        # HASHTAG before another option parses the same way.
        result = client.execute_command('TS.MGET', 'HASHTAG', tags[1], 'LATEST', 'SELECTED_LABELS', 'shard',
                                        'FILTER', 'name=tagged')
        assert len(result) == 1
        assert result[0][0] == keys[1]
        assert {label_pair[0]: label_pair[1] for label_pair in result[0][1]} == {b'shard': b'1'}

    def test_mget_cme_hashtag_on_shard_without_matches(self):
        """A HASHTAG pointing at a shard with no matching series yields an empty reply"""
        cluster_client: ValkeyCluster = self.new_cluster_client()
        tags = self.tag_per_primary(cluster_client)

        # Only the first primary holds a matching series.
        key = f'ts:{{{tags[0]}}}:only'
        cluster_client.execute_command('TS.CREATE', key, 'LABELS', 'name', 'lonely')
        cluster_client.execute_command('TS.ADD', key, 1000, 1)

        client = self.new_client_for_primary(0)
        assert client.execute_command('TS.MGET', 'HASHTAG', tags[1], 'FILTER', 'name=lonely') == []
        assert client.execute_command('TS.MGET', 'HASHTAG', tags[0], 'FILTER', 'name=lonely')[0][0] == key.encode()
