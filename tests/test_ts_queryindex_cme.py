import pytest
from valkey import ResponseError, Valkey, ValkeyCluster
from valkeytestframework.util.waiters import *
from valkeytestframework.conftest import resource_port_tracker
from valkey_timeseries_test_case import ValkeyTimeSeriesClusterTestCase

# use hash tags to ensure keys are distributed across cluster nodes
TS1 = b'ts1:{1}'
TS2 = b'ts2:{2}'
TS3 = b'ts3:{3}'
TS4 = b'ts4:{1}'
TS5 = b'ts5:{2}'
TS6 = b'ts6:{3}'
TS7 = b'ts7:{1}'
TS8 = b'ts8:{2}'
TS9 = b'ts9:{3}'


class TestTsQueryIndex(ValkeyTimeSeriesClusterTestCase):

    def setup_test_data(self, client):
        """Create a set of time series with different label combinations for testing"""
        # Create test series with various labels
        client.execute_command('TS.CREATE', TS1, 'LABELS', 'name', 'cpu', 'type', 'usage', 'node', 'node1')
        client.execute_command('TS.CREATE', TS2, 'LABELS', 'name', 'cpu', 'type', 'usage', 'node', 'node2')
        client.execute_command('TS.CREATE', TS3, 'LABELS', 'name', 'memory', 'type', 'usage', 'node', 'node1')
        client.execute_command('TS.CREATE', TS4, 'LABELS', 'name', 'memory', 'type', 'usage', 'node', 'node2')
        client.execute_command('TS.CREATE', TS5, 'LABELS', 'name', 'cpu', 'type', 'temperature', 'node', 'node1')
        client.execute_command('TS.CREATE', TS6, 'LABELS', 'name', 'cpu', 'node', 'node3')
        client.execute_command('TS.CREATE', TS7, 'LABELS', 'name', 'disk', 'type', 'usage', 'node', 'node3')
        client.execute_command('TS.CREATE', TS8, 'LABELS', 'type', 'usage')  # No name label

    def test_basic_query(self):
        """Test basic TS.QUERYINDEX functionality"""
        cluster: ValkeyCluster = self.new_cluster_client()
        client = self.new_client_for_primary(0)
        self.setup_test_data(cluster)

        # Query for all CPU metrics
        result = client.execute_command('TS.QUERYINDEX', 'name=cpu')
        assert result == [TS1, TS2, TS5, TS6]

        # Query for all metrics from node1
        result = client.execute_command('TS.QUERYINDEX', 'node=node1')
        assert result == [TS1, TS3, TS5]

    def test_compound_filters(self):
        """Test querying with multiple label conditions"""

        cluster: ValkeyCluster = self.new_cluster_client()
        client = self.new_client_for_primary(0)
        self.setup_test_data(cluster)

        # Query for CPU usage metrics
        result = client.execute_command('TS.QUERYINDEX', 'name=cpu', 'type=usage')
        assert result == [TS1, TS2]

        # Query for all usage metrics from node2
        result = client.execute_command('TS.QUERYINDEX', 'type=usage', 'node=node2')
        assert result == [TS2, TS4]

    def test_regex_matching(self):
        """Test querying with regex patterns"""
        cluster: ValkeyCluster = self.new_cluster_client()
        client = self.new_client_for_primary(0)
        self.setup_test_data(cluster)

        # Query for all metrics with a name matching 'c.*'
        result = client.execute_command('TS.QUERYINDEX', 'name=~"c.*"')
        assert result == [TS1, TS2, TS5, TS6]

        # Query for all metrics with a node matching 'node [12]'
        result = client.execute_command('TS.QUERYINDEX', 'node=~"node[12]"')
        assert result == [TS1, TS2, TS3, TS4, TS5]

        # Match using alternation
        result = client.execute_command('TS.QUERYINDEX', 'name=~"cpu|disk"')
        assert result == [TS1, TS2, TS5, TS6, TS7]

    def test_negative_matching(self):
        """Test querying with negation patterns"""
        cluster: ValkeyCluster = self.new_cluster_client()
        client = self.new_client_for_primary(0)
        self.setup_test_data(cluster)

        # A bare negative matcher is unbounded and rejected -- it must be paired with a
        # filter that cannot be satisfied by a missing label.
        self.assert_filters_rejected('TS.QUERYINDEX', 'name!=cpu', client=client)
        self.assert_filters_rejected('TS.QUERYINDEX', 'type!=usage', client=client)

        # Query for all usage metrics with a name not equal to cpu
        result = client.execute_command('TS.QUERYINDEX', 'type=usage', 'name!=cpu')
        assert result == [TS3, TS4, TS7, TS8]

        # Query for all named metrics with type not matching 'usage'
        result = client.execute_command('TS.QUERYINDEX', 'name=~".+"', 'type!=usage')
        assert result == [TS5, TS6]

    def test_prometheus_not_regex_matcher(self):
        """Test Prometheus-style regex negation matchers (label!~"regex")"""
        cluster: ValkeyCluster = self.new_cluster_client()
        client = self.new_client_for_primary(0)
        self.setup_test_data(cluster)

        # Negative regex matchers are unbounded on their own, so each is paired with
        # 'type=usage' or 'name=~".+"' to bound the query.
        self.assert_filters_rejected('TS.QUERYINDEX', 'name!~"c.*"', client=client)
        self.assert_filters_rejected('TS.QUERYINDEX', 'name!~"cpu|memory"', client=client)
        self.assert_filters_rejected('TS.QUERYINDEX', 'node!~"node[12]"', client=client)

        # Not matching regex
        result = client.execute_command('TS.QUERYINDEX', 'type=usage', 'name!~"c.*"')
        assert result == [TS3, TS4, TS7, TS8]

        # Not matching regex alternation
        result = client.execute_command('TS.QUERYINDEX', 'type=usage', 'name!~"cpu|memory"')
        assert result == [TS7, TS8]

        # Not matching using character class. TS8 has no 'node' label -- it would satisfy the
        # negative matcher, but it has no 'name' either, so the bounding filter excludes it.
        result = client.execute_command('TS.QUERYINDEX', 'name=~".+"', 'node!~"node[12]"')
        assert result == [TS6, TS7]

    def test_complex_queries(self):
        """Test more complex query combinations"""
        cluster: ValkeyCluster = self.new_cluster_client()
        client = self.new_client_for_primary(0)
        self.setup_test_data(cluster)

        # CPU metrics that are not usage type
        result = client.execute_command('TS.QUERYINDEX', 'name=cpu', 'type!=usage')
        assert result == [TS5, TS6]

        # Non-CPU metrics that are usage type
        result = client.execute_command('TS.QUERYINDEX', 'name!=cpu', 'type=usage')
        assert result == [TS3, TS4, TS7, TS8]

        # Regex not matching, bounded by the 'type' filter
        result = client.execute_command('TS.QUERYINDEX', 'type=usage', 'name!~"c.*"')
        assert result == [TS3, TS4, TS7, TS8]

    def test_missing_labels(self):
        """Test querying for metrics with missing labels"""
        cluster: ValkeyCluster = self.new_cluster_client()
        client = self.new_client_for_primary(0)
        self.setup_test_data(cluster)

        # 'label=' matches every series lacking that label, so it cannot bound a query
        # on its own and must be paired with a bounded filter.
        self.assert_filters_rejected('TS.QUERYINDEX', 'name=', client=client)
        self.assert_filters_rejected('TS.QUERYINDEX', 'node=', client=client)

        # Find usage series without the 'name' label
        result = client.execute_command('TS.QUERYINDEX', 'type=usage', 'name=')
        assert result == [TS8]

        # Find usage series without the 'node' label
        result = client.execute_command('TS.QUERYINDEX', 'type=usage', 'node=')
        assert result == [TS8]

    def test_combined_operations(self):
        """Test combination of different operations"""
        cluster: ValkeyCluster = self.new_cluster_client()
        self.setup_test_data(cluster)
        client = self.new_client_for_primary(0)

        # Both filters here match a missing label -- `.*` matches empty and `!=` is negative --
        # so the list as a whole is unbounded and rejected. `.+` is the bounded form.
        self.assert_filters_rejected('TS.QUERYINDEX', 'name=~".*"', 'type!=usage', client=client)

        # Find series that match regex but don't match another condition
        result = client.execute_command('TS.QUERYINDEX', 'name=~".+"', 'type!=usage')
        assert result == [TS5, TS6]

        # Mix of equals, not equals, and regex
        result = client.execute_command('TS.QUERYINDEX', 'name=cpu', 'node!=node1', 'type=~".*"')
        assert result == [TS2, TS6]

    def test_error_cases(self):
        """Test error conditions"""
        cluster: ValkeyCluster = self.new_cluster_client()
        self.setup_test_data(cluster)
        client = self.new_client_for_primary(0)

        # Empty query should return error
        with pytest.raises(ResponseError) as excinfo:
            client.execute_command('TS.QUERYINDEX')
        assert "wrong number of arguments for 'ts.queryindex' command" in str(excinfo.value).lower()

    def test_no_results(self):
        """Test queries that should return no results"""
        cluster: ValkeyCluster = self.new_cluster_client()
        self.setup_test_data(cluster)
        client = self.new_client_for_primary(0)

        # Query for non-existent label value
        result = client.execute_command('TS.QUERYINDEX', 'name=nonexistent')
        assert result == []

        # Query with impossible combination
        result = client.execute_command('TS.QUERYINDEX', 'name=cpu', 'name=memory')
        assert result == []

    def test_multiple_queries(self):
        """Test executing multiple QUERYINDEX commands sequentially"""
        cluster: ValkeyCluster = self.new_cluster_client()
        client = self.new_client_for_primary(0)
        self.setup_test_data(cluster)

        # First query
        result1 = client.execute_command('TS.QUERYINDEX', 'name=cpu')
        assert result1 == [TS1, TS2, TS5, TS6]

        # Second query with a different filter
        result2 = client.execute_command('TS.QUERYINDEX', 'type=usage')
        assert result2 == [TS1, TS2, TS3, TS4, TS7, TS8]

    def test_list_include_queries(self):
        """Test executing multiple QUERYINDEX commands sequentially"""
        cluster: ValkeyCluster = self.new_cluster_client()
        client = self.new_client_for_primary(0)
        self.setup_test_data(cluster)

        # First query
        result1 = client.execute_command('TS.QUERYINDEX', 'name=(cpu,disk)')
        assert result1 == [TS1, TS2, TS5, TS6, TS7]

    def setup_or_test_data(self, client):
        client.execute_command('TS.CREATE', TS1, 'METRIC', 'http_status{status="200",method="GET"}')
        client.execute_command('TS.CREATE', TS2, 'METRIC', 'http_status{status="200",method="POST"}')
        client.execute_command('TS.CREATE', TS3, 'METRIC', 'http_status{status="404",method="GET"}')
        client.execute_command('TS.CREATE', TS4, 'METRIC', 'http_status{status="500",method="POST"}')
        client.execute_command('TS.CREATE', TS5, 'METRIC', 'api_host{name="server1",env="prod"}')
        client.execute_command('TS.CREATE', TS6, 'METRIC', 'api_host{name="server2",env="prod"}')
        client.execute_command('TS.CREATE', TS7, 'METRIC', 'api_host{name="server1",env="staging"}')
        client.execute_command('TS.CREATE', TS8, 'METRIC', 'api_host{name="server2",env="staging"}')

    def test_or_status_200_or_404(self):
        cluster: ValkeyCluster = self.new_cluster_client()
        client = self.new_client_for_primary(0)
        self.setup_or_test_data(cluster)

        result = client.execute_command('TS.QUERYINDEX', 'http_status{status="200" or status="404"}')
        assert result == [TS1, TS2, TS3]

    def test_or_multiple_metric_names(self):
        cluster: ValkeyCluster = self.new_cluster_client()
        client = self.new_client_for_primary(0)
        self.setup_or_test_data(cluster)

        result = client.execute_command('TS.QUERYINDEX', 'http_status{method="GET"} or api_host{env=~"prod|staging"}')
        assert result == [TS1, TS3, TS5, TS6, TS7, TS8]

    def test_or_and_conditions(self):
        cluster: ValkeyCluster = self.new_cluster_client()
        client = self.new_client_for_primary(0)
        self.setup_or_test_data(cluster)
        result = client.execute_command('TS.QUERYINDEX',
                                        'api_host{name="server1",env="prod"} or api_host{name="server2",env="staging"}')
        assert result == [TS5, TS8]

    def test_or_regex_matchers(self):
        cluster: ValkeyCluster = self.new_cluster_client()
        client = self.new_client_for_primary(0)
        self.setup_or_test_data(cluster)
        result = client.execute_command('TS.QUERYINDEX', 'http_status{method=~"GET$"} or http_status{method=~"POST$"}')
        assert result == [TS1, TS2, TS3, TS4]

    def test_or_not_equal(self):
        cluster: ValkeyCluster = self.new_cluster_client()
        client = self.new_client_for_primary(0)
        self.setup_or_test_data(cluster)
        result = client.execute_command('TS.QUERYINDEX',
                                        'http_status{status!="200",method="GET"} or api_host{env="prod"}')
        assert result == [TS3, TS5, TS6]

    def test_or_empty_branch(self):
        cluster = self.new_cluster_client()
        client = self.new_client_for_primary(0)
        self.setup_or_test_data(cluster)
        result = client.execute_command('TS.QUERYINDEX', 'status=999 or env=prod')
        assert result == [TS5, TS6]

    def test_or_all_empty(self):
        cluster = self.new_cluster_client()
        client = self.new_client_for_primary(0)
        self.setup_or_test_data(cluster)
        result = client.execute_command('TS.QUERYINDEX', 'status=999 or env=development')
        assert result == []

    def test_or_overlapping(self):
        cluster = self.new_cluster_client()
        client = self.new_client_for_primary(0)
        self.setup_or_test_data(cluster)
        # OR: method=GET OR (method=GET AND status=200)
        result = client.execute_command('TS.QUERYINDEX',
                                        'http_status{method="GET"} or http_status{method="GET",status="200"}')
        assert result == [TS1, TS3]

    def test_or_regex_not_equal(self):
        cluster = self.new_cluster_client()
        client = self.new_client_for_primary(0)
        self.setup_or_test_data(cluster)
        result = client.execute_command('TS.QUERYINDEX', 'http_status{method!~"GET"} or api_host{env="staging"}')
        assert result == [TS2, TS4, TS7, TS8]

    def test_filter_by_range(self):
        cluster = self.new_cluster_client()
        client = self.new_client_for_primary(0)
        self.setup_test_data(cluster)

        # add data points to a few series (1 per node)
        cluster.execute_command('TS.ADD', TS1, 1000, 1)
        cluster.execute_command('TS.ADD', TS5, 1000, 1)
        cluster.execute_command('TS.ADD', TS6, 1000, 1)

        # filter by range (inclusive)
        result = client.execute_command('TS.QUERYINDEX', 'FILTER_BY_RANGE', 500, 1500, 'name=cpu')
        assert result == [TS1, TS5, TS6]

        # filter excluding certain range
        result = client.execute_command('TS.QUERYINDEX', 'FILTER_BY_RANGE', 'NOT', 500, 1500, 'name=cpu')
        assert result == [TS2]

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

    def test_queryindex_hashtag_scopes_cluster_fanout(self):
        """HASHTAG queries only the slot(s) selected by the supplied hash tag."""
        cluster: ValkeyCluster = self.new_cluster_client()
        client: Valkey = self.new_client_for_primary(0)

        tag_a, tag_b, tag_c = self.tag_per_primary(cluster)

        key_a = f'qi:tagged:{{{tag_a}}}'.encode()
        key_b = f'qi:tagged:{{{tag_b}}}'.encode()
        key_c = f'qi:tagged:{{{tag_c}}}'.encode()
        for key in (key_a, key_b, key_c):
            cluster.execute_command('TS.CREATE', key, 'LABELS', 'scope', 'tagged')

        # Without HASHTAG every shard is asked.
        assert client.execute_command('TS.QUERYINDEX', 'scope=tagged') == sorted([key_a, key_b, key_c])

        # One tag -> one shard.
        assert client.execute_command('TS.QUERYINDEX', 'HASHTAG', tag_a, 'scope=tagged') == [key_a]

        # Several tags -> the union of their shards, deduplicated by the coordinator.
        result = client.execute_command('TS.QUERYINDEX', 'HASHTAG', f'{tag_a},{tag_c}', 'scope=tagged')
        assert result == sorted([key_a, key_c])

        # Repeating a tag does not duplicate the shard's keys.
        result = client.execute_command('TS.QUERYINDEX', 'HASHTAG', f'{tag_b},{tag_b}', 'scope=tagged')
        assert result == [key_b]

    def test_queryindex_hashtag_with_filter_by_range(self):
        """HASHTAG and FILTER_BY_RANGE compose, in either order, ahead of the selectors."""
        cluster: ValkeyCluster = self.new_cluster_client()
        client: Valkey = self.new_client_for_primary(0)

        tag_a, tag_b, _tag_c = self.tag_per_primary(cluster)

        key_a = f'qi:ranged:{{{tag_a}}}'.encode()
        key_b = f'qi:ranged:{{{tag_b}}}'.encode()
        for key in (key_a, key_b):
            cluster.execute_command('TS.CREATE', key, 'LABELS', 'scope', 'ranged')
            cluster.execute_command('TS.ADD', key, 1000, 1)

        for args in (
            ('HASHTAG', tag_a, 'FILTER_BY_RANGE', 500, 1500),
            ('FILTER_BY_RANGE', 500, 1500, 'HASHTAG', tag_a),
        ):
            assert client.execute_command('TS.QUERYINDEX', *args, 'scope=ranged') == [key_a], args

        # The shard scoping applies to the NOT form too: key_b is out of scope, not merely
        # out of range, so nothing comes back.
        result = client.execute_command('TS.QUERYINDEX', 'HASHTAG', tag_a,
                                        'FILTER_BY_RANGE', 'NOT', 500, 1500, 'scope=ranged')
        assert result == []

    def test_queryindex_hashtag_unknown_tag_returns_nothing(self):
        """A tag whose slot holds no matching series yields an empty reply, not an error."""
        cluster: ValkeyCluster = self.new_cluster_client()
        client: Valkey = self.new_client_for_primary(0)

        tag_a, tag_b, _tag_c = self.tag_per_primary(cluster)
        cluster.execute_command('TS.CREATE', f'qi:only:{{{tag_a}}}', 'LABELS', 'scope', 'only')

        assert client.execute_command('TS.QUERYINDEX', 'HASHTAG', tag_b, 'scope=only') == []

    def test_queryindex_hashtag_error_cases(self):
        """HASHTAG needs a non-empty value, and a selector must remain after it."""
        client: Valkey = self.new_client_for_primary(0)

        with pytest.raises(ResponseError, match="missing HASHTAG argument"):
            client.execute_command('TS.QUERYINDEX', 'HASHTAG', '', 'name=cpu')

        # No FILTER keyword delimits the tag list, so the selector is eaten as the tag.
        with pytest.raises(ResponseError, match="please provide at least one matcher"):
            client.execute_command('TS.QUERYINDEX', 'HASHTAG', 'name=cpu')
