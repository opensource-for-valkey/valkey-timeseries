import pytest
from valkey import ValkeyCluster, Valkey

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


def exec_sorted_values(client, *args):
    """Helper method to execute a command and return a LabelValuesResponse"""
    result = client.execute_command('TS.LABELNAMES', *args)
    result = LabelSearchResponse.parse(result)
    values = sorted([lv.value for lv in result.results])
    return values


class TestTsLabelNamesCME(ValkeyTimeSeriesClusterTestCase):

    def setup_test_data(self, client):
        """Create a set of time series with different label combinations for testing"""

        # Create multi series with various labels
        client.execute_command('TS.CREATE', TS1, 'LABELS', 'name', 'cpu', 'type', 'usage', 'node', 'node1', "ts1", "1")
        client.execute_command('TS.CREATE', TS2, 'LABELS', 'name', 'cpu', 'type', 'usage', 'node', 'node2', "ts2", "1")
        client.execute_command('TS.CREATE', TS3, 'LABELS', 'name', 'memory', 'type', 'usage', 'node', 'node1', "ts3",
                               "1")
        client.execute_command('TS.CREATE', TS4, 'LABELS', 'name', 'memory', 'type', 'usage', 'node', 'node2', "ts4",
                               "1")
        client.execute_command('TS.CREATE', TS5, 'LABELS', 'name', 'cpu', 'type', 'temperature', 'node', 'node1', "ts5",
                               "1")
        client.execute_command('TS.CREATE', TS6, 'LABELS', 'name', 'cpu', 'node', 'node3', "ts6", "1")  # No type label
        client.execute_command('TS.CREATE', TS7, 'LABELS', 'name', 'disk', 'type', 'usage', 'node', 'node3', "ts7", "1")
        client.execute_command('TS.CREATE', TS8, 'LABELS', 'type', 'usage', "ts8", "1")  # No name label
        client.execute_command('TS.CREATE', TS9, 'LABELS', 'location', 'datacenter', 'rack', 'rack1', "ts9",
                               "1")  # Different labels


    def test_labelnames_with_filter(self):
        """Test TS.LABELNAMES with FILTER parameter"""

        cluster_client: ValkeyCluster = self.new_cluster_client()
        client: Valkey = self.new_client_for_primary(0)

        self.setup_test_data(cluster_client)

        # Get label names for CPU series
        result = exec_sorted_values(client, 'FILTER', 'name=cpu')
        assert result == [b'name', b'node', b'ts1', b'ts2', b'ts5', b'ts6', b'type']

        # Get label names for node1 series
        result = exec_sorted_values(client, 'FILTER', 'node=node1')
        assert result == [b'name', b'node', b'ts1', b'ts3', b'ts5', b'type']

    def test_labelnames_with_multiple_filters(self):
        """Test TS.LABELNAMES with multiple filter conditions"""
        cluster_client: ValkeyCluster = self.new_cluster_client()
        client: Valkey = self.new_client_for_primary(0)

        self.setup_test_data(cluster_client)

        # Get label names for CPU usage series
        result = exec_sorted_values(client, 'FILTER', 'name=cpu', 'type=usage')
        assert result == [b'name', b'node', b'ts1', b'ts2', b'type']

        # Get label names for series with the location label
        result = exec_sorted_values(client, 'FILTER', 'location=datacenter')
        assert result == [b'location', b'rack', b'ts9']

    def test_labelnames_with_regex_filters(self):
        """Test TS.LABELNAMES with regex filter expressions"""
        cluster_client: ValkeyCluster = self.new_cluster_client()
        client: Valkey = self.new_client_for_primary(0)

        self.setup_test_data(cluster_client)

        # Get label names for series where name matches regex
        result = exec_sorted_values(client, 'FILTER', 'name=~"c.*"')
        assert result == [b'name', b'node', b'ts1', b'ts2', b'ts5', b'ts6', b'type']

        # Get label names for series where node matches pattern
        result = exec_sorted_values(client, 'FILTER', 'node=~"node[12]"')
        assert result == [b'name', b'node', b'ts1', b'ts2', b'ts3', b'ts4', b'ts5', b'type']

    def test_labelnames_with_time_range(self):
        """Test TS.LABELNAMES with time range filters"""
        cluster_client: ValkeyCluster = self.new_cluster_client()
        client: Valkey = self.new_client_for_primary(0)

        self.setup_test_data(cluster_client)

        now = 1000

        # From setup_test_data, these series have name=cpu:
        # ts1: name=cpu, node=node1, type=user
        # ts2: name=cpu, node=node1, type=system
        # ts5: name=cpu, node=node2, type=user
        # ts6: name=cpu, node=node2, type=system

        # Add samples at different timestamps to series with name=cpu
        cluster_client.execute_command('TS.ADD', TS1, now, 10)
        cluster_client.execute_command('TS.ADD', TS2, now + 100, 20)
        cluster_client.execute_command('TS.ADD', TS5, now + 200, 30)
        cluster_client.execute_command('TS.ADD', TS6, now + 500, 40)

        # Query from timestamp 150 onwards - should only include ts5 and ts6
        # (ts1 at now=1000, ts2 at now+100=1100, ts5 at now+200=1200, ts6 at now+500=1500)
        result = exec_sorted_values(client, 'FILTER_BY_RANGE', now + 150, "+", 'FILTER', 'name=cpu')

        # Should return label names from ts5 (now+200) and ts6 (now+500)
        assert result == [b'name', b'node', b'ts5', b'ts6', b'type']

        # Query with both START and END - should only include ts2 and ts5
        result = exec_sorted_values(client, 'FILTER_BY_RANGE', now + 50, now + 250, 'FILTER', 'name=cpu')

        # Should return labels from ts2 (now+100) and ts5 (now+200)
        assert b'name' in result
        assert b'node' in result
        assert b'type' in result
        assert b'ts2' in result
        assert b'ts5' in result

        assert len(result) == 5

        # Query that matches only one series
        result = exec_sorted_values(client, 'FILTER_BY_RANGE', now + 400, "+", 'FILTER', 'name=cpu')

        # Should only include ts6 (now+500)
        assert b'name' in result
        assert b'node' in result
        assert b'ts6' in result
        assert len(result) == 3

        # Exclude middle series (ts2 and ts5) - should return ts1 and ts6
        result = exec_sorted_values(client, 'FILTER_BY_RANGE', 'NOT', now + 50, now + 250, 'FILTER', 'name=cpu')
        assert b'name' in result
        assert b'node' in result
        assert b'type' in result
        assert b'ts1' in result
        assert b'ts6' in result
        assert b'ts2' not in result
        assert b'ts5' not in result
        assert len(result) == 5

        # Exclude ts6 (now+500) - should return ts1, ts2, ts5
        result = exec_sorted_values(client, 'FILTER_BY_RANGE', 'NOT', now + 400, "+", 'FILTER', 'name=cpu')
        assert b'name' in result
        assert b'node' in result
        assert b'type' in result
        assert b'ts1' in result
        assert b'ts2' in result
        assert b'ts5' in result
        assert b'ts6' not in result
        assert len(result) == 6

        # Exclude early data (ts1, ts2, ts5) - should only include ts6
        result = exec_sorted_values(client, 'FILTER_BY_RANGE', 'NOT', "-", now + 250, 'FILTER', 'name=cpu')
        assert b'name' in result
        assert b'node' in result
        assert b'ts6' in result
        assert b'ts1' not in result
        assert b'ts2' not in result
        assert b'ts5' not in result
        assert len(result) == 3

    def test_labelnames_with_combined_parameters(self):
        """Test TS.LABELNAMES with combined filter and time range parameters"""
        cluster_client: ValkeyCluster = self.new_cluster_client()
        client: Valkey = self.new_client_for_primary(0)

        self.setup_test_data(cluster_client)

        # Add samples with different timestamps
        now = 1000

        # Add samples to different series at different times
        cluster_client.execute_command('TS.ADD', TS1, now, 10)
        cluster_client.execute_command('TS.ADD', TS2, now + 100, 20)
        cluster_client.execute_command('TS.ADD', TS3, now + 200, 30)
        cluster_client.execute_command('TS.ADD', TS9, now + 500, 40)

        # Query with filter and time range
        result = exec_sorted_values(client, 'FILTER_BY_RANGE', now, now + 150, 'FILTER', 'name=~"c.*"',
                                        )
        assert result == [b'name', b'node', b'ts1', b'ts2', b'type']

    def test_labelnames_empty_result(self):
        """Test TS.LABELNAMES when no series match the criteria"""
        cluster_client: ValkeyCluster = self.new_cluster_client()
        client: Valkey = self.new_client_for_primary(0)

        self.setup_test_data(cluster_client)

        # Filter that doesn't match any series
        result = exec_sorted_values(client, 'FILTER', 'nonexistent=value')
        assert result == []

        # Time range that doesn't match any samples
        now = 1000
        cluster_client.execute_command('TS.ADD', TS1, now, 10)
        result = exec_sorted_values(client, 'FILTER_BY_RANGE', now + 2000, "+", 'FILTER', 'name=cpu')
        assert result == []

    def test_labelnames_with_limit(self):
        """Test TS.LABELNAMES with LIMIT parameter"""
        cluster_client: ValkeyCluster = self.new_cluster_client()
        client: Valkey = self.new_client_for_primary(0)

        self.setup_test_data(cluster_client)

        # Get top 2 label names
        result = exec_sorted_values(client, 'LIMIT', 2, 'FILTER', 'name=cpu')
        assert len(result) == 2

        # Verify sorted order (alphabetical)
        all_labels = exec_sorted_values(client, 'FILTER', 'name=cpu')
        expected = all_labels[:5]

        result = exec_sorted_values(client, 'LIMIT', 5, 'FILTER', 'name=cpu')
        assert result == expected

    def test_labelnames_after_series_deletion(self):
        """Test TS.LABELNAMES after deleting time series"""
        cluster_client: ValkeyCluster = self.new_cluster_client()
        client: Valkey = self.new_client_for_primary(0)

        self.setup_test_data(cluster_client)

        # Delete series with unique labels
        cluster_client.execute_command('DEL', TS9)

        # Verify unique labels are no longer returned
        result = exec_sorted_values(client, 'FILTER', 'location=datacenter')
        assert result == []

    def test_labelnames_with_complex_filters(self):
        """Test TS.LABELNAMES with complex filter combinations"""
        cluster_client: ValkeyCluster = self.new_cluster_client()
        client: Valkey = self.new_client_for_primary(0)

        self.setup_test_data(cluster_client)

        # Complex filter: CPU metrics that are not a usage type
        result = exec_sorted_values(client, 'FILTER', 'name=cpu', 'type!=usage')
        assert result == [b'name', b'node', b'ts5', b'ts6', b'type']

        # A lone negative regex matcher is unbounded and rejected.
        self.assert_filters_rejected('TS.LABELNAMES', 'FILTER', 'node!~"node[12]"', client=client)

        # Complex filter with regex: nodes that don't match the pattern. 'name=~".+"' supplies
        # the positive matcher, which also excludes ts8 and ts9 -- neither has a 'name' label.
        result = exec_sorted_values(client, 'FILTER', 'name=~".+"', 'node!~"node[12]"')
        assert result == [b'name', b'node', b'ts6', b'ts7', b'type']

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

    def test_labelnames_hashtag_scopes_cluster_fanout(self):
        """HASHTAG queries only the slot(s) selected by the supplied hash tag."""
        cluster_client: ValkeyCluster = self.new_cluster_client()
        client: Valkey = self.new_client_for_primary(0)

        tag_a, tag_b, tag_c = self.tag_per_primary(cluster_client)

        # Each series carries a marker label unique to itself so membership in the
        # HASHTAG-scoped label-name set can be checked precisely.
        cluster_client.execute_command('TS.CREATE', f'ts:{{{tag_a}}}:1', 'LABELS', 'name', 'cpu', 'markA1', '1')
        cluster_client.execute_command('TS.CREATE', f'ts:{{{tag_a}}}:2', 'LABELS', 'name', 'memory', 'markA2', '1')
        cluster_client.execute_command('TS.CREATE', f'ts:{{{tag_b}}}:1', 'LABELS', 'name', 'disk', 'markB1', '1')
        cluster_client.execute_command('TS.CREATE', f'ts:{{{tag_c}}}:1', 'LABELS', 'name', 'network', 'markC1', '1')

        assert exec_sorted_values(client, 'HASHTAG', tag_a, 'FILTER', 'name=~".+"') == [
            b'markA1', b'markA2', b'name',
        ]
        assert exec_sorted_values(client, 'FILTER', 'name=~".+"', 'HASHTAG', f'{tag_a},{tag_b}') == [
            b'markA1', b'markA2', b'markB1', b'name',
        ]
