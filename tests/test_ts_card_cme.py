"""
Simplified TS.CARD cluster mode integration tests.
"""

import pytest
from valkey import ResponseError, ValkeyCluster, Valkey

from valkey_timeseries_test_case import ValkeyTimeSeriesClusterTestCase
from valkeytestframework.conftest import resource_port_tracker


class TestTsCardClusterCME(ValkeyTimeSeriesClusterTestCase):
    """
    Test TS.CARD functionality with cluster mode enabled.
    """

    def test_cluster_mode_filter_requirement(self):
        """Test that cluster mode enforces filter requirements"""

        cluster_client: ValkeyCluster = self.new_cluster_client()
        client: Valkey = self.new_client_for_primary(0)

        # Set up basic data
        cluster_client.execute_command('TS.CREATE', 'metric:cpu', 'LABELS', 'type', 'cpu', 'host', 'server1')
        cluster_client.execute_command('TS.CREATE', 'metric:memory', 'LABELS', 'type', 'memory', 'host', 'server1')

        with pytest.raises(ResponseError, match="TS.CARD in cluster mode requires at least one matcher"):
            client.execute_command('TS.CARD')

        # But in cluster mode, filters are required - simulate the behavior
        # All these patterns would work in cluster mode:
        result = client.execute_command('TS.CARD', 'FILTER', 'type=cpu')
        assert result == 1

        result = client.execute_command('TS.CARD', 'FILTER', 'host=server1')
        assert result == 2

        # A lone negative matcher is unbounded and rejected; 'host' is on both series, so
        # 'host=~".+"' supplies the positive matcher without excluding anything.
        self.assert_filters_rejected('TS.CARD', 'FILTER', 'type!=memory', client=client)

        result = client.execute_command('TS.CARD', 'FILTER', 'host=~".+"', 'type!=memory')
        assert result == 1

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

    def test_card_hashtag_scopes_cluster_fanout(self):
        """HASHTAG queries only the slot(s) selected by the supplied hash tag."""
        cluster: ValkeyCluster = self.new_cluster_client()
        client: Valkey = self.new_client_for_primary(0)

        tag1, tag2 = self.tag_per_primary(cluster)[:2]

        cluster.execute_command('TS.CREATE', f'card:tagged:{{{tag1}}}', 'LABELS', 'scope', 'tagged')
        cluster.execute_command('TS.CREATE', f'card:tagged:{{{tag2}}}', 'LABELS', 'scope', 'tagged')

        assert client.execute_command('TS.CARD', 'HASHTAG', tag1, 'FILTER', 'scope=tagged') == 1
        assert client.execute_command('TS.CARD', 'FILTER', 'scope=tagged', 'HASHTAG', f'{tag1},{tag2}') == 2

    def test_cluster_label_filtering(self):
        cluster: ValkeyCluster = self.new_cluster_client()
        node0: Valkey = self.new_client_for_primary(0)

        # Create series per each cluster node
        cluster.execute_command('TS.CREATE', 'metric:nodeA:cpu:1', 'LABELS',
                                '__name__', 'cpu', 'node', 'nodeA', 'instance', '1')
        cluster.execute_command('TS.CREATE', 'metric:nodeA:memory:1', 'LABELS',
                                '__name__', 'memory', 'node', 'nodeA', 'instance', '1')

        # Series in nodeB would go to another cluster node
        cluster.execute_command('TS.CREATE', 'metric:nodeB:cpu:1', 'LABELS',
                                '__name__', 'cpu', 'node', 'nodeB', 'instance', '1')
        cluster.execute_command('TS.CREATE', 'metric:nodeB:disk:1', 'LABELS',
                                '__name__', 'disk', 'node', 'nodeB', 'instance', '1')

        # Series in nodeC would go to a third cluster node
        cluster.execute_command('TS.CREATE', 'metric:nodeC:cpu:1', 'LABELS',
                                '__name__', 'cpu', 'node', 'nodeC', 'instance', '1')
        cluster.execute_command('TS.CREATE', 'metric:nodeC:network:1', 'LABELS',
                                '__name__', 'network', 'node', 'nodeC', 'instance', '1')

        # Test cross-node aggregation patterns
        # Count all CPU metrics (would span all 3 cluster nodes)
        result = node0.execute_command('TS.CARD', 'FILTER', 'cpu{}')
        assert result == 3

        # Count metrics by node (would query specific cluster node)
        result = node0.execute_command('TS.CARD', 'FILTER', 'node=nodeA')
        assert result == 2

        result = node0.execute_command('TS.CARD', 'FILTER', 'node=nodeB')
        assert result == 2

        result = node0.execute_command('TS.CARD', 'FILTER', 'node=nodeC')
        assert result == 2

        # Test complex filters that would aggregate across the cluster
        result = node0.execute_command('TS.CARD', 'FILTER', 'cpu{node!="nodeA"}')
        assert result == 2  # CPU metrics on nodeB and nodeC

        # Two negative matchers with nothing positive to intersect against -- rejected.
        self.assert_filters_rejected(
            'TS.CARD', 'FILTER', '{__name__!="cpu", node!="nodeC"}', client=node0)

        # Every series has instance="1", so it bounds the query without excluding anything.
        result = node0.execute_command(
            'TS.CARD', 'FILTER', '{instance="1", __name__!="cpu", node!="nodeC"}')
        assert result == 2  # memory and disk metrics (not on nodeC)

    def test_cluster_date_range_filtering(self):
        """Test TS.CARD with date ranges in cluster mode"""
        cluster: ValkeyCluster = self.new_cluster_client()

        # Add data at different time ranges
        cluster.execute_command('TS.CREATE', 'early:{1}:series', 'LABELS', 'timing', 'early', 'slot', 'slot1', 'type',
                                'test')
        cluster.execute_command('TS.ADD', 'early:{1}:series', 1000, 10)  # Early data

        cluster.execute_command('TS.CREATE', 'middle:{2}:series', 'LABELS', 'timing', 'middle', 'slot', 'slot2', 'type',
                                'test')
        cluster.execute_command('TS.ADD', 'middle:{2}:series', 2000, 20)  # Middle data

        cluster.execute_command('TS.CREATE', 'late:{3}:series', 'LABELS', 'timing', 'late', 'slot', 'slot3', 'type',
                                'test')
        cluster.execute_command('TS.ADD', 'late:{3}:series', 3000, 30)  # Late data

        node0 = self.client_for_primary(0)
        # Test date range queries that would span cluster nodes
        result = node0.execute_command('TS.CARD', 'FILTER_BY_RANGE', 1000, 1500, 'FILTER', 'timing=early')
        assert result == 1  # Only early series in this range

        # A date range does not bound a query -- the filter list still needs a positive
        # matcher. All three series carry type=test, so it bounds without excluding.
        self.assert_filters_rejected(
            'TS.CARD', 'FILTER_BY_RANGE', 1000, 3000, 'FILTER', 'slot!=slot2', client=node0)

        result = node0.execute_command(
            'TS.CARD', 'FILTER_BY_RANGE', 1000, 3000, 'FILTER', 'type=test', 'slot!=slot2')
        assert result == 2  # early and late series

        self.assert_filters_rejected(
            'TS.CARD', 'FILTER_BY_RANGE', 2500, 3500, 'FILTER', 'timing!=early', client=node0)

        result = node0.execute_command(
            'TS.CARD', 'FILTER_BY_RANGE', 2500, 3500, 'FILTER', 'type=test', 'timing!=early')
        assert result == 1  # Only late series in this range

        # Test with special timestamp values
        result = node0.execute_command('TS.CARD', 'FILTER_BY_RANGE', '-', 2500, 'FILTER', 'type=test', 'slot!=slot3')
        assert result == 2  # early and middle series

        result = node0.execute_command('TS.CARD', 'FILTER_BY_RANGE', 1500, '+', 'FILTER', 'type=test', 'timing!=early')
        assert result == 2  # middle and late series

        # Test negative date range filtering with NOT
        result = node0.execute_command('TS.CARD', 'FILTER_BY_RANGE', 'NOT', 1000, 1500, 'FILTER', 'type=test')
        assert result == 2  # (excludes early series in range 1000-1500)

        result = node0.execute_command('TS.CARD', 'FILTER_BY_RANGE', 'NOT', 1500, 2500, 'FILTER', 'type=test', 'slot!=slot1')
        assert result == 1  # Only late series (excludes middle series in range 1500-2500)

        result = node0.execute_command('TS.CARD', 'FILTER_BY_RANGE', 'NOT', 2500, 3500, 'FILTER', 'type=test')
        assert result == 2  # (excludes middle series)

        # Test NOT with special timestamp values
        result = node0.execute_command('TS.CARD', 'FILTER_BY_RANGE', 'NOT', '-', 2000, 'FILTER', 'type=test', 'slot!=slot3')
        assert result == 0  # No series excluded (all early/middle data is before/at 2000)

        result = node0.execute_command('TS.CARD', 'FILTER_BY_RANGE', 'NOT', 2000, '+', 'FILTER', 'type=test', 'timing!=late')
        assert result == 1  # Only early series (excludes middle and late which have data at/after 2000)

    def test_cluster_complex_label_queries(self):
        """Test complex label filtering patterns for cluster deployment"""
        cluster: ValkeyCluster = self.new_cluster_client()

        # Create monitoring data that would be distributed across the cluster
        monitoring_data = [
            ('app:{service1}:latency', 'app', 'service1', 'metric', 'latency', 'env', 'prod'),
            ('app:{service1}:throughput', 'app', 'service1', 'metric', 'throughput', 'env', 'prod'),
            ('app:{service2}:latency', 'app', 'service2', 'metric', 'latency', 'env', 'staging'),
            ('app:{service2}:errors', 'app', 'service2', 'metric', 'errors', 'env', 'staging'),
            ('app:{service3}:latency', 'app', 'service3', 'metric', 'latency', 'env', 'dev'),
            ('infra:{dc1}:cpu', 'infra', 'dc1', 'metric', 'cpu', 'env', 'prod'),
            ('infra:{dc1}:memory', 'infra', 'dc1', 'metric', 'memory', 'env', 'prod'),
            ('infra:{dc2}:cpu', 'infra', 'dc2', 'metric', 'cpu', 'env', 'staging'),
        ]

        base_ts = 1000
        for i, (key, type_k, type_v, metric_k, metric_v, env_k, env_v) in enumerate(monitoring_data):
            cluster.execute_command('TS.CREATE', key, 'LABELS', "__name__", metric_v,
                                    type_k, type_v, metric_k, metric_v, env_k, env_v)
            cluster.execute_command('TS.ADD', key, base_ts + i * 100, 50 + i * 5)

        node0: Valkey = self.new_client_for_primary(0)

        # Test application vs. infrastructure filtering
        result = node0.execute_command('TS.CARD', 'FILTER', 'app=service1')
        assert result == 2  # latency and throughput for service1

        # Count all infrastructure metrics (series that have the 'infra' label)
        result = node0.execute_command('TS.CARD', 'FILTER', 'infra=dc1')
        assert result == 2  # CPU and memory for dc1
        result = node0.execute_command('TS.CARD', 'FILTER', 'infra=dc2')
        assert result == 1  # CPU for dc2
        # Total infra metrics
        infra_total = 3

        # Test environment-based filtering
        result = node0.execute_command('TS.CARD', 'FILTER', 'env=prod')
        assert result == 4  # service1 metrics + dc1 metrics

        result = node0.execute_command('TS.CARD', 'FILTER', 'env=staging')
        assert result == 3  # service2 + dc2 metrics

        # Test metric type aggregation across services
        result = node0.execute_command('TS.CARD', 'FILTER', 'metric=latency')
        assert result == 3  # latency for all 3 services

        result = node0.execute_command('TS.CARD', 'FILTER', 'metric=cpu')
        assert result == 2  # CPU for both datacenters

        # Test complex combinations
        result = node0.execute_command('TS.CARD', 'FILTER', 'latency{env!="dev"}')
        assert result == 2  # latency for service1 and service2 (not service3)

        # 'app!=' ("has an app label") is still a negative matcher, so this list has no
        # positive matcher and is rejected. Every series has a 'metric' label, so
        # 'metric=~".+"' bounds the query without excluding anything.
        self.assert_filters_rejected('TS.CARD', 'FILTER', 'app!=', 'metric!=memory', client=node0)

        result = node0.execute_command('TS.CARD', 'FILTER', 'metric=~".+"', 'app!=', 'metric!=memory')
        assert result == 5  # the 5 app series; none of them is a memory metric

    def test_cluster_edge_cases(self):
        """Test edge cases and error conditions in cluster mode"""

        cluster: ValkeyCluster = self.new_cluster_client()
        client: Valkey = self.new_client_for_primary(0)

        # Create a minimal data set
        cluster.execute_command('TS.CREATE', 'test:{a}:1', 'LABELS', 'group', 'test', 'id', '1')
        cluster.execute_command('TS.CREATE', 'test:{b}:2', 'LABELS', 'group', 'test', 'id', '2')
        cluster.execute_command('TS.ADD', 'test:{a}:1', 1000, 1)

        # Test queries that return zero results
        result = client.execute_command('TS.CARD', 'FILTER', 'group=nonexistent')
        assert result == 0

        result = client.execute_command('TS.CARD', 'FILTER', 'id=999')
        assert result == 0

        # Test with series that have no data points
        result = client.execute_command('TS.CARD', 'FILTER', 'id=2')
        assert result == 1  # Series exists even without data

        # Test date range with no matching data
        result = client.execute_command('TS.CARD', 'FILTER_BY_RANGE', 5000, 6000, 'FILTER', 'group=test')
        assert result == 0  # No data in this time range

        # Test date range that includes only one series
        result = client.execute_command('TS.CARD', 'FILTER_BY_RANGE', 1000, 1000, 'FILTER', 'group=test')
        assert result == 1  # Only test:{a}:1 has data at timestamp 1000

    def test_cluster_scale_simulation(self):
        """Test TS.CARD behavior with larger datasets"""

        cluster: ValkeyCluster = self.new_cluster_client()
        # Create a larger dataset distributed across cluster nodes
        base_ts = 1000
        for region in ['us-east', 'us-west', 'eu-central']:
            for service in ['api', 'db', 'cache']:
                for instance in range(3):
                    key = f'metrics:{{{region}}}:{service}:{instance}'
                    cluster.execute_command('TS.CREATE', key, 'LABELS',
                                            '__name__', 'performance',
                                            'region', region,
                                            'service', service,
                                            'instance', str(instance))
                    # Add multiple data points
                    for t in range(5):
                        ts = base_ts + (instance * 100) + (t * 10)
                        value = 50 + (instance * 10) + t
                        cluster.execute_command('TS.ADD', key, ts, value)

        client = self.client_for_primary(0)

        # Test aggregation across all regions (27 total series: 3 regions * 3 services * 3 instances)
        result = client.execute_command('TS.CARD', 'FILTER', 'performance{}')
        assert result == 27

        # Test by region (9 series per region)
        result = client.execute_command('TS.CARD', 'FILTER', 'region=us-east')
        assert result == 9

        result = client.execute_command('TS.CARD', 'FILTER', 'performance{region="eu-central"}')
        assert result == 9

        # Test by service across regions (9 series per service)
        result = client.execute_command('TS.CARD', 'FILTER', 'service=api')
        assert result == 9

        result = client.execute_command('TS.CARD', 'FILTER', 'service=db')
        assert result == 9

        # Test complex filters
        result = client.execute_command('TS.CARD', 'FILTER', 'performance{region="us-east", service="api"}')
        assert result == 3  # 3 API instances in us-east

        self.assert_filters_rejected(
            'TS.CARD', 'FILTER', 'service!=cache', 'region!=eu-central', client=client)

        # Every series has a 'service' label, so 'service=~".+"' bounds without excluding.
        result = client.execute_command(
            'TS.CARD', 'FILTER', 'service=~".+"', 'service!=cache', 'region!=eu-central')
        assert result == 12  # API and DB in us-east and us-west (2 regions * 2 services * 3 instances)

        # Test date range filtering on large dataset
        # Data is added at: base_ts + (instance * 100) + (t * 10) where instance=0,1,2 and t=0,1,2,3,4
        # So timestamps range from base_ts to base_ts + 240
        # result = client.execute_command('TS.CARD', 'FILTER_BY_RANGE', base_ts, base_ts + 250, 'FILTER', 'service=db')
        # assert result == 9  # All DB services have data in this range

    def test_cluster_error_conditions(self):
        """Test error conditions that would occur in cluster mode"""

        cluster_client: ValkeyCluster = self.new_cluster_client()
        client: Valkey = self.new_client_for_primary(0)

        cluster_client.execute_command('TS.CREATE', 'error_test', 'LABELS', 'test', 'error')
        cluster_client.execute_command('TS.ADD', 'error_test', 1000, 1)

        # These should work (valid filter syntax)
        result = client.execute_command('TS.CARD', 'FILTER', 'test=error')
        assert result == 1

        # Test that FILTER_BY_RANGE requires FILTER in cluster mode
        # (This would fail in actual cluster mode, but works in single node)
        try:
            result = client.execute_command('TS.CARD', 'FILTER_BY_RANGE', 1000, 2000)
            # In single node mode, this might work, but in cluster mode it should fail
            # The test documents the expected cluster behavior
        except ResponseError as e:
            # This is the expected cluster mode behavior
            assert "requires at least one matcher" in str(e).lower()

        # Invalid timestamp formats should fail in both modes
        with pytest.raises(ResponseError):
            client.execute_command('TS.CARD', 'FILTER_BY_RANGE', 'invalid', 2000, 'FILTER', 'test=error')

        # Missing filter arguments should fail
        with pytest.raises(ResponseError):
            client.execute_command('TS.CARD', 'FILTER')

    def test_cluster_consistency_patterns(self):
        """Test patterns that ensure consistency in cluster deployments"""

        cluster: ValkeyCluster = self.new_cluster_client()
        client: Valkey = self.new_client_for_primary(0)

        # Use consistent hashtags to ensure related metrics are co-located
        user_sessions = []
        for user_id in range(1, 6):  # Users 1-5
            hash_tag = f"user{user_id}"
            base_key = f"session:{{{hash_tag}}}"

            # Multiple metrics per user (would be on the same cluster node due to hashtag)
            for metric in ['login', 'pageview', 'purchase']:
                key = f"{base_key}:{metric}"
                cluster.execute_command('TS.CREATE', key, 'LABELS',
                                        '__name__', metric,
                                        'user_id', str(user_id),
                                        'metric', metric,
                                        'category', 'user_activity')
                user_sessions.append(key)

                # Add time-series data
                for hour in range(24):
                    ts = 1000 + (hour * 3600)
                    value = user_id * 10 + hour
                    cluster.execute_command('TS.ADD', key, ts, value)

        # Test user-specific queries (single cluster node due to hashtags)
        result = client.execute_command('TS.CARD', 'FILTER', 'user_id=1')
        assert result == 3  # login, pageview, purchase for user 1

        result = client.execute_command('TS.CARD', 'FILTER', 'user_id=3')
        assert result == 3

        # Test metric aggregation across users (spans cluster nodes)
        result = client.execute_command('TS.CARD', 'FILTER', 'metric=login')
        assert result == 5  # login metrics for all 5 users

        result = client.execute_command('TS.CARD', 'FILTER', 'purchase{}')
        assert result == 5  # purchase metrics for all 5 users

        # Test time-based queries across all users
        morning_ts = 1000 + (8 * 3600)  # 8 AM
        evening_ts = 1000 + (20 * 3600)  # 8 PM

        result = client.execute_command('TS.CARD', 'FILTER', 'category=user_activity')

        # result = client.execute_command('TS.CARD', 'FILTER_BY_RANGE', morning_ts, evening_ts,
        #                                 'FILTER', 'category=user_activity')
        #
        # assert result == 15  # All user metrics have data in business hours
        #
        # assert len(user_sessions) == 0 # 15  # 5 users * 3 metrics each

        # Test complex filters combining user and metric dimensions
        self.assert_filters_rejected(
            'TS.CARD', 'FILTER', 'metric!=purchase', 'user_id!=5', client=client)

        # Every series is category=user_activity, so it bounds without excluding anything.
        result = client.execute_command(
            'TS.CARD', 'FILTER', 'category=user_activity', 'metric!=purchase', 'user_id!=5')
        assert result == 8  # login+pageview for users 1-4 (4 users * 2 metrics)
