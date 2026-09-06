import pytest
from valkey import ValkeyCluster
from valkeytestframework.util.waiters import *
from valkeytestframework.conftest import resource_port_tracker
from valkey_timeseries_test_case import ValkeyTimeSeriesClusterTestCase
from common import LabelSearchResponse

TS1 = 'ts1:{1}'
TS2 = 'ts2:{2}'
TS3 = 'ts3:{3}'
TS4 = 'ts4:{1}'
TS5 = 'ts5:{2}'
TS6 = 'ts6:{3}'
TS7 = 'ts7:{1}'
TS8 = 'ts8:{2}'
TS9 = 'ts9:{3}'


class TestTimeSeriesLabelValues(ValkeyTimeSeriesClusterTestCase):

    @staticmethod
    def setup_test_data(client):
        """Set up test data with various label values"""
        # Create time series with different label combinations
        client.execute_command('TS.CREATE', TS1, 'LABELS', 'name', 'cpu', 'type', 'usage', 'node', 'server1',
                               'datacenter', 'dc1', "key", "ts1")
        client.execute_command('TS.CREATE', TS2, 'LABELS', 'name', 'cpu', 'type', 'temperature', 'node', 'server1',
                               'datacenter', 'dc1', "key", "ts2")
        client.execute_command('TS.CREATE', TS3, 'LABELS', 'name', 'memory', 'type', 'usage', 'node', 'server2',
                               'datacenter', 'dc1', "key", "ts3")
        client.execute_command('TS.CREATE', TS4, 'LABELS', 'name', 'disk', 'type', 'usage', 'node', 'server2',
                               'datacenter', 'dc2', "key", "ts4")
        client.execute_command('TS.CREATE', TS5, 'LABELS', 'name', 'cpu', 'type', 'usage', 'node', 'server3',
                               'datacenter', 'dc2', "key", "ts5")
        client.execute_command('TS.CREATE', TS6, 'LABELS', 'name', 'network', 'type', 'throughput', 'node', 'server3',
                               "key", "ts6")

        # Add some sample data
        now = 1000
        KEYS = [TS1, TS2, TS3, TS4, TS5, TS6]
        i = 1
        for ts_key in KEYS:
            client.execute_command('TS.ADD', ts_key, now, i * 10)
            client.execute_command('TS.ADD', ts_key, now + 1000, i * 10 + 5)

    def test_label_values_cardinality_accumulates_across_cluster_nodes(self):
        """Cardinality should sum when the same value exists on multiple cluster nodes."""
        cluster: ValkeyCluster = self.new_cluster_client()
        client = self.new_client_for_primary(0)

        # Create the same label value on different hash slots so the fanout merge
        # has to combine per-node cardinalities for a single matched value.
        cluster.execute_command('TS.CREATE', 'svc_a:{1}', 'LABELS', 'service', 'api', 'region', 'us-east')
        cluster.execute_command('TS.CREATE', 'svc_b:{2}', 'LABELS', 'service', 'api', 'region', 'us-west')

        raw = client.execute_command('TS.LABELVALUES', 'service', 'SEARCH', 'api', 'INCLUDE_METADATA')
        parsed = LabelSearchResponse.parse(raw)

        assert len(parsed.results) == 1
        item = parsed.results[0]
        assert item.value == b'api'
        assert item.score is not None
        assert item.cardinality == 2

    def test_label_values_with_filter(self):
        """Test retrieving label values with a filter"""
        cluster: ValkeyCluster = self.new_cluster_client()
        client = self.new_client_for_primary(0)

        self.setup_test_data(cluster)

        # Get values for the 'name' label filtered by type=usage
        # result = client.execute_command('TS.LABELVALUES', 'name', 'FILTER', 'type=usage')
        # assert result == [b'cpu', b'disk', b'memory']

        # Get values for the 'type' label filtered by name=cpu
        raw = client.execute_command('TS.LABELVALUES', 'type', 'FILTER', 'name=cpu')
        parsed = LabelSearchResponse.parse(raw)
        values = [lv.value for lv in parsed.results]
        assert values == [b'temperature', b'usage']

    def test_label_values_with_multiple_filters(self):
        """Test retrieving label values with multiple filters"""
        cluster: ValkeyCluster = self.new_cluster_client()
        client = self.new_client_for_primary(0)

        self.setup_test_data(cluster)

        # Get values for the 'node' label with multiple filters
        raw = client.execute_command('TS.LABELVALUES', 'node', 'FILTER', 'name=cpu', 'type=usage')
        parsed = LabelSearchResponse.parse(raw)
        values = [lv.value for lv in parsed.results]
        assert values == [b'server1', b'server3']

        # Get values for the 'datacenter' label with multiple filters
        raw = client.execute_command('TS.LABELVALUES', 'datacenter', 'FILTER', 'name=cpu', 'type=usage')
        parsed = LabelSearchResponse.parse(raw)
        values = [lv.value for lv in parsed.results]
        assert values == [b'dc1', b'dc2']

    def test_label_values_with_regex_filters(self):
        """Test retrieving label values with regex filters"""
        cluster: ValkeyCluster = self.new_cluster_client()
        client = self.new_client_for_primary(0)

        self.setup_test_data(cluster)

        # Get values for the 'node' label with regex filter
        raw = client.execute_command('TS.LABELVALUES', 'node', 'FILTER', 'name=~"c.*"')
        parsed = LabelSearchResponse.parse(raw)
        values = [lv.value for lv in parsed.results]
        assert values == [b'server1', b'server3']

        # Get values for the 'type' label with regex filter
        raw = client.execute_command('TS.LABELVALUES', 'type', 'FILTER', 'name=~"(memory|disk)"')
        parsed = LabelSearchResponse.parse(raw)
        values = [lv.value for lv in parsed.results]
        assert values == [b'usage']

    def test_label_values_with_time_range(self):
        """Test retrieving label values with time range filtering"""
        cluster: ValkeyCluster = self.new_cluster_client()
        client = self.new_client_for_primary(0)

        self.setup_test_data(cluster)

        # Add data with specific timestamps for time range testing
        cluster.execute_command('TS.CREATE', 'ts_old', 'LABELS', 'name', 'archive', 'age', 'old', 'common', '1')
        cluster.execute_command('TS.ADD', 'ts_old', 500, 100)

        cluster.execute_command('TS.CREATE', 'ts_new', 'LABELS', 'name', 'recent', 'age', 'new', 'common', '1')
        cluster.execute_command('TS.ADD', 'ts_new', 2500, 200)

        # Get values for the 'age' label with time range
        # First timestamp should exclude the 'old' series
        raw = client.execute_command('TS.LABELVALUES', 'age', 'FILTER_BY_RANGE', 1000, "+", "FILTER", 'common=1')
        parsed = LabelSearchResponse.parse(raw)
        values = [lv.value for lv in parsed.results]
        assert values == [b'new']

        # Get values with end time range
        raw = client.execute_command('TS.LABELVALUES', 'age', 'FILTER_BY_RANGE', '-', 1500, "FILTER", 'common=1')
        parsed = LabelSearchResponse.parse(raw)
        values = [lv.value for lv in parsed.results]
        assert values == [b'old']

        # Get values with both start and end time range
        cluster.execute_command('TS.CREATE', 'ts_1', 'LABELS', 'name', 'alice', 'age', '32', 'common', '1')
        cluster.execute_command('TS.ADD', 'ts_1', 1000, 200)

        cluster.execute_command('TS.CREATE', 'ts_2', 'LABELS', 'name', 'bob', 'age', '45', 'common', '1')
        cluster.execute_command('TS.ADD', 'ts_2', 1700, 200)

        raw = client.execute_command('TS.LABELVALUES', 'name', 'FILTER_BY_RANGE', 900, 2000, "FILTER", 'common=1')
        parsed = LabelSearchResponse.parse(raw)
        values = [lv.value for lv in parsed.results]
        assert values == [b'alice', b'bob']

        # Test negative date range filtering with NOT
        # Exclude ts_old (500) - should only return 'new' age value
        raw = client.execute_command('TS.LABELVALUES', 'age', 'FILTER_BY_RANGE', 'NOT', '-', 1000, 'FILTER',
                                        'common=1')
        parsed = LabelSearchResponse.parse(raw)
        values = [lv.value for lv in parsed.results]
        assert values == [b'45', b'new']
        assert b'old' not in values

        # Exclude ts_new (2500) - should only return 'old' age value
        raw = client.execute_command('TS.LABELVALUES', 'age', 'FILTER_BY_RANGE', 'NOT', 2000, '+', 'FILTER',
                                        'common=1')
        parsed = LabelSearchResponse.parse(raw)
        values = [lv.value for lv in parsed.results]
        assert values == [b'32', b'45', b'old']
        assert b'new' not in values

        # Exclude middle range (ts_1 at 1000 and ts_2 at 1700) - should exclude both alice and bob
        raw = client.execute_command('TS.LABELVALUES', 'name', 'FILTER_BY_RANGE', 'NOT', 900, 2000, 'FILTER',
                                        'common=1')
        parsed = LabelSearchResponse.parse(raw)
        values = [lv.value for lv in parsed.results]
        assert b'alice' not in values
        assert b'bob' not in values
        # Only archive and recent should remain from the series with common=1
        assert set(values) == {b'archive', b'recent'}

        # Exclude late data (ts_new at 2500) - should return archive, alice, and bob
        raw = client.execute_command('TS.LABELVALUES', 'name', 'FILTER_BY_RANGE', 'NOT', 2400, '+', 'FILTER',
                                        'common=1')
        parsed = LabelSearchResponse.parse(raw)
        values = [lv.value for lv in parsed.results]
        assert b'recent' not in values
        assert b'archive' in values
        assert b'alice' in values
        assert b'bob' in values

        # Test NOT with series distributed across cluster nodes
        # Exclude ts_1 (1000) - should return archive, bob, and recent
        raw = client.execute_command('TS.LABELVALUES', 'name', 'FILTER_BY_RANGE', 'NOT', 950, 1050, 'FILTER',
                                        'common=1')
        parsed = LabelSearchResponse.parse(raw)
        values = [lv.value for lv in parsed.results]
        assert b'alice' not in values
        assert b'archive' in values
        assert b'bob' in values
        assert b'recent' in values

    def test_label_values_with_limit(self):
        """Test retrieving label values with LIMIT parameter"""
        cluster: ValkeyCluster = self.new_cluster_client()
        client = self.new_client_for_primary(0)

        self.setup_test_data(cluster)

        # Get values for the 'name' label with limit
        raw = client.execute_command('TS.LABELVALUES', 'name', 'LIMIT', 2, 'FILTER', 'type=usage')
        parsed = LabelSearchResponse.parse(raw)
        values = [lv.value for lv in parsed.results]
        assert len(values) == 2
        assert all(val in [b'cpu', b'disk', b'memory'] for val in values)

        # Get values for the 'node' label with limit
        raw = client.execute_command('TS.LABELVALUES', 'node', 'LIMIT', 1, "FILTER", 'datacenter=dc2')
        parsed = LabelSearchResponse.parse(raw)
        values = [lv.value for lv in parsed.results]
        assert values == [b'server2']

    def test_label_values_with_combined_parameters(self):
        """Test retrieving label values with combined parameters"""
        cluster: ValkeyCluster = self.new_cluster_client()
        client = self.new_client_for_primary(0)

        self.setup_test_data(cluster)

        # Get values with filter, time range, and limit
        raw = client.execute_command(
            'TS.LABELVALUES', 'type',
            'FILTER_BY_RANGE', 500, 2500,
            'LIMIT', 1,
            'FILTER', 'name=~"c.*"',
        )
        parsed = LabelSearchResponse.parse(raw)
        assert len(parsed.results) == 1
        assert parsed.results[0].value in [b'temperature', b'usage']

        # Different combination of parameters
        raw = client.execute_command(
            'TS.LABELVALUES', 'node',
            'FILTER_BY_RANGE', 900, '+',
            'FILTER', 'datacenter=dc1'
        )
        parsed = LabelSearchResponse.parse(raw)
        values = [lv.value for lv in parsed.results]
        assert values == [b'server1', b'server2']

    def test_label_values_empty_result(self):
        """Test retrieving label values with no matching results"""
        cluster: ValkeyCluster = self.new_cluster_client()
        client = self.new_client_for_primary(0)

        # Filter that doesn't match any series
        raw = client.execute_command('TS.LABELVALUES', 'name', 'FILTER', 'nonexistent=value')
        parsed = LabelSearchResponse.parse(raw)
        values = [lv.value for lv in parsed.results]
        assert values == []

        # Filter with a time range that excludes all series
        raw = client.execute_command('TS.LABELVALUES', 'name', 'FILTER_BY_RANGE', 5000, '+', "FILTER", 'type=usage')
        parsed = LabelSearchResponse.parse(raw)
        values = [lv.value for lv in parsed.results]
        assert values == []

    def test_label_values_after_series_deletion(self):
        """Test retrieving label values after series deletion"""
        cluster: ValkeyCluster = self.new_cluster_client()
        client = self.new_client_for_primary(0)

        self.setup_test_data(cluster)

        # Verify initial state
        raw = client.execute_command('TS.LABELVALUES', 'name', 'FILTER', 'name=network')
        parsed = LabelSearchResponse.parse(raw)
        values = [lv.value for lv in parsed.results]
        assert b'network' in values

        # Delete a time series
        client.execute_command('DEL', TS6)  # ts6 has name=network

        # Verify the deleted label value is no longer returned
        raw = client.execute_command('TS.LABELVALUES', 'name', 'FILTER', 'name=network')
        parsed = LabelSearchResponse.parse(raw)
        values = [lv.value for lv in parsed.results]
        assert b'network' not in values

    def test_label_values_with_nonexistent_label(self):
        """Test retrieving values for a non-existent label"""
        cluster: ValkeyCluster = self.new_cluster_client()
        client = self.new_client_for_primary(0)

        self.setup_test_data(cluster)

        # Query for a label that doesn't exist
        raw = client.execute_command('TS.LABELVALUES', 'nonexistent_label', 'FILTER', 'name=cpu')
        parsed = LabelSearchResponse.parse(raw)
        values = [lv.value for lv in parsed.results]
        assert values == []

    def test_label_values_after_label_update(self):
        """Test retrieving values after updating labels"""
        cluster: ValkeyCluster = self.new_cluster_client()
        client = self.new_client_for_primary(0)

        self.setup_test_data(cluster)

        # Verify initial state
        raw = client.execute_command('TS.LABELVALUES', 'name', 'FILTER', 'type=usage')
        parsed = LabelSearchResponse.parse(raw)
        values = [lv.value for lv in parsed.results]
        assert b'updated' not in values

        # Create a new time series with a new label value
        cluster.execute_command('TS.CREATE', 'ts_new', 'LABELS', 'name', 'updated', 'type', 'usage')

        # Verify the new label value is included
        raw = client.execute_command('TS.LABELVALUES', 'name', 'FILTER', 'type=usage')
        parsed = LabelSearchResponse.parse(raw)
        values = [lv.value for lv in parsed.results]
        assert b'updated' in values

    def tag_per_primary(self, cluster_client: ValkeyCluster):
        """Return one hash tag per primary, each owned by a different node.

        The tag -> slot -> node mapping depends on how the harness splits the slot
        range across primaries, so the tags are discovered at runtime rather than
        hard-coded (e.g. {1} and {2} can land on the same primary here).
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

    def test_labelvalues_hashtag_scopes_cluster_fanout(self):
        """HASHTAG queries only the slot(s) selected by the supplied hash tag."""
        cluster: ValkeyCluster = self.new_cluster_client()
        client = self.new_client_for_primary(0)

        tag_a, tag_b, tag_c = self.tag_per_primary(cluster)

        cluster.execute_command('TS.CREATE', f'ts:{{{tag_a}}}:cpu', 'LABELS', 'name', 'cpu')
        cluster.execute_command('TS.CREATE', f'ts:{{{tag_a}}}:disk', 'LABELS', 'name', 'disk')
        cluster.execute_command('TS.CREATE', f'ts:{{{tag_b}}}:mem', 'LABELS', 'name', 'memory')
        cluster.execute_command('TS.CREATE', f'ts:{{{tag_c}}}:net', 'LABELS', 'name', 'network')

        raw = client.execute_command('TS.LABELVALUES', 'name', 'HASHTAG', tag_a, 'FILTER', 'name=~".+"')
        assert [item.value for item in LabelSearchResponse.parse(raw).results] == [b'cpu', b'disk']

        # Combining two tags on different primaries unions their values, while the
        # third primary's tag (network) stays excluded.
        raw = client.execute_command(
            'TS.LABELVALUES', 'name', 'FILTER', 'name=~".+"', 'HASHTAG', f'{tag_a},{tag_b}'
        )
        assert [item.value for item in LabelSearchResponse.parse(raw).results] == [b'cpu', b'disk', b'memory']
