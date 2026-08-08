from valkey import ValkeyCluster
from valkeytestframework.conftest import resource_port_tracker
from valkey_timeseries_test_case import ValkeyTimeSeriesClusterTestCase
from common import parse_stats_response


class TestTsStatsCluster(ValkeyTimeSeriesClusterTestCase):
    """Test suite for TS.LABELSTATS command in cluster mode."""

    def get_stats(self, limit: int | None = None, label: str | None = None, filters=None):
        args = ['TS.LABELSTATS']
        if limit is not None:
            args.extend(['LIMIT', limit])
        if label is not None:
            args.extend(['LABEL', label])
        # FILTER is variadic, so it always goes last.
        if filters is not None:
            args.append('FILTER')
            args.extend(filters)

        client = self.new_client_for_primary(0)

        result = client.execute_command(*args)
        stats = parse_stats_response(result)
        return stats

    def test_stats_cluster_basic(self):
        """Test TS.LABELSTATS aggregates series counts from multiple shards."""
        # Create series on different shards using hash tags to ensure distribution
        cluster: ValkeyCluster = self.new_cluster_client()

        cluster.execute_command('TS.CREATE', 'ts:{1}', 'LABELS', 'metric', 'cpu', 'host', 'h1')
        cluster.execute_command('TS.CREATE', 'ts:{2}', 'LABELS', 'metric', 'cpu', 'host', 'h2')
        cluster.execute_command('TS.CREATE', 'ts:{3}', 'LABELS', 'metric', 'mem', 'host', 'h3')

        stats = self.get_stats()

        # Verify the total series count across the cluster
        assert stats['totalSeries'] == 3

        # Verify total unique label pairs
        # metric=cpu, metric=mem, host=h1, host=h2, host=h3
        # Total: 5 pairs
        assert stats['totalLabelValuePairs'] == 5

    def test_stats_cluster_top_k_aggregation(self):
        """Test TS.LABELSTATS aggregates top-k lists across shards correctly."""
        cluster: ValkeyCluster = self.new_cluster_client()

        # Shard 1: 2 series with type=A
        cluster.execute_command('TS.CREATE', 'ts:{1}a', 'LABELS', 'type', 'A')
        cluster.execute_command('TS.CREATE', 'ts:{1}b', 'LABELS', 'type', 'A')

        # Shard 2: 1 series with type=A, 1 with type=B
        cluster.execute_command('TS.CREATE', 'ts:{2}a', 'LABELS', 'type', 'A')
        cluster.execute_command('TS.CREATE', 'ts:{2}b', 'LABELS', 'type', 'B')

        # Shard 3: 2 series with type=B
        cluster.execute_command('TS.CREATE', 'ts:{3}a', 'LABELS', 'type', 'B')
        cluster.execute_command('TS.CREATE', 'ts:{3}b', 'LABELS', 'type', 'B')

        # Total across cluster: type=A (3), type=B (3)

        stats = self.get_stats()

        pair_counts = {item[0]: item[1] for item in stats['seriesCountByLabelValuePair']}

        assert pair_counts.get('type=A') == 3
        assert pair_counts.get('type=B') == 3

    def test_stats_cluster_limit(self):
        """Test TS.LABELSTATS LIMIT parameter in cluster mode."""
        # Create 15 unique label values across shards
        cluster: ValkeyCluster = self.new_cluster_client()
        for i in range(15):
            # Use hash tags to distribute
            tag = f"{{{i}}}"
            cluster.execute_command('TS.CREATE', f'ts:{tag}', 'LABELS', 'id', f'val{i}')

        # Default limit is usually 10
        stats = self.get_stats()
        assert len(stats['seriesCountByLabelValuePair']) <= 10

        # Explicit limit 5
        stats = self.get_stats(5)
        assert len(stats['seriesCountByLabelValuePair']) <= 5

    def test_stats_cluster_empty(self):
        """Test TS.LABELSTATS on an empty cluster."""
        stats = self.get_stats()
        assert stats['totalSeries'] == 0
        assert stats['totalLabelValuePairs'] == 0
        assert stats['seriesCountByLabelValuePair'] == []

    def test_stats_cluster_multiple_labels_per_series(self):
        """Test TS.LABELSTATS with series having multiple labels across the cluster."""
        cluster: ValkeyCluster = self.new_cluster_client()

        # Create multiple series with multiple labels on different shards
        cluster.execute_command('TS.CREATE', 'ts:{1}', 'LABELS', 'env', 'prod', 'region', 'us-east', 'tier', 'web')
        cluster.execute_command('TS.CREATE', 'ts:{2}', 'LABELS', 'env', 'prod', 'region', 'us-west', 'tier', 'api')
        cluster.execute_command('TS.CREATE', 'ts:{3}', 'LABELS', 'env', 'dev', 'region', 'us-east', 'tier', 'web')

        stats = self.get_stats()

        assert stats['totalSeries'] == 3
        # env=prod (2), env=dev (1), region=us-east (2), region=us-west (1), tier=web (2), tier=api (1)
        assert stats['totalLabelValuePairs'] == 6

        pair_counts = {item[0]: item[1] for item in stats['seriesCountByLabelValuePair']}
        assert pair_counts.get('env=prod') == 2
        assert pair_counts.get('region=us-east') == 2
        assert pair_counts.get('tier=web') == 2

    def test_stats_cluster_after_series_deletion(self):
        """Test TS.LABELSTATS reflects series deletions across cluster."""
        cluster: ValkeyCluster = self.new_cluster_client()

        cluster.execute_command('TS.CREATE', 'ts:{1}', 'LABELS', 'temp', 'sensor1')
        cluster.execute_command('TS.CREATE', 'ts:{2}', 'LABELS', 'temp', 'sensor2')
        cluster.execute_command('TS.CREATE', 'ts:{3}', 'LABELS', 'temp', 'sensor3')

        stats = self.get_stats()
        assert stats['totalSeries'] == 3

        # Delete one series
        cluster.delete('ts:{2}')

        stats = self.get_stats()
        assert stats['totalSeries'] == 2
        assert stats['totalLabelValuePairs'] == 2  # temp=sensor1, temp=sensor3

    def test_stats_cluster_duplicate_label_values(self):
        """Test TS.LABELSTATS counts duplicate label values correctly across shards."""
        cluster: ValkeyCluster = self.new_cluster_client()

        # Multiple series with same label value on different shards
        for i in range(10):
            tag = f"{{{i}}}"
            cluster.execute_command('TS.CREATE', f'ts:{tag}', 'LABELS', 'status', 'active')

        stats = self.get_stats()

        assert stats['totalSeries'] == 10
        pair_counts = {item[0]: item[1] for item in stats['seriesCountByLabelValuePair']}
        assert pair_counts.get('status=active') == 10

    def test_stats_cluster_mixed_labeled_unlabeled(self):
        """Test TS.LABELSTATS with a mix of labeled and unlabeled series."""
        cluster: ValkeyCluster = self.new_cluster_client()

        # Labeled series
        cluster.execute_command('TS.CREATE', 'ts:{1}', 'LABELS', 'type', 'labeled')
        cluster.execute_command('TS.CREATE', 'ts:{2}', 'LABELS', 'type', 'labeled')

        # Unlabeled series
        cluster.execute_command('TS.CREATE', 'ts:{3}')
        cluster.execute_command('TS.CREATE', 'ts:{4}')

        stats = self.get_stats()

        assert stats['totalSeries'] == 4
        # Only labeled series contribute to label pairs
        assert stats['totalLabelValuePairs'] == 1

    def test_stats_cluster_large_limit(self):
        """Test TS.LABELSTATS with limit larger than available label pairs."""
        cluster: ValkeyCluster = self.new_cluster_client()

        # Create only 3 unique label pairs
        cluster.execute_command('TS.CREATE', 'ts:{1}', 'LABELS', 'id', 'a')
        cluster.execute_command('TS.CREATE', 'ts:{2}', 'LABELS', 'id', 'b')
        cluster.execute_command('TS.CREATE', 'ts:{3}', 'LABELS', 'id', 'c')

        # Request limit of 100 (more than available)
        stats = self.get_stats(limit=100)

        assert len(stats['seriesCountByLabelValuePair']) == 3

    def test_stats_cluster_uneven_distribution(self):
        """Test TS.LABELSTATS with uneven series distribution across shards."""
        cluster: ValkeyCluster = self.new_cluster_client()

        # Heavily load shard 1
        for i in range(5):
            cluster.execute_command('TS.CREATE', f'ts:{{1}}_{i}', 'LABELS', 'shard', '1')

        # Light load on shard 2
        cluster.execute_command('TS.CREATE', 'ts:{2}', 'LABELS', 'shard', '2')

        stats = self.get_stats()

        assert stats['totalSeries'] == 6
        pair_counts = {item[0]: item[1] for item in stats['seriesCountByLabelValuePair']}
        assert pair_counts.get('shard=1') == 5
        assert pair_counts.get('shard=2') == 1

    def test_stats_cluster_after_data_addition(self):
        """Test TS.LABELSTATS remains consistent after adding data points."""
        cluster: ValkeyCluster = self.new_cluster_client()

        cluster.execute_command('TS.CREATE', 'ts:{1}', 'LABELS', 'metric', 'temp')
        cluster.execute_command('TS.CREATE', 'ts:{2}', 'LABELS', 'metric', 'pressure')

        stats_before = self.get_stats()

        # Add data points
        cluster.execute_command('TS.ADD', 'ts:{1}', 1000, 25.5)
        cluster.execute_command('TS.ADD', 'ts:{2}', 1000, 101.3)

        stats_after = self.get_stats()

        # Stats should be unchanged by data addition
        assert stats_before['totalSeries'] == stats_after['totalSeries']
        assert stats_before['totalLabelValuePairs'] == stats_after['totalLabelValuePairs']

    def test_stats_cluster_many_unique_labels(self):
        """Test TS.LABELSTATS with many unique label combinations."""
        cluster: ValkeyCluster = self.new_cluster_client()

        # Create 50 series with unique label combinations
        for i in range(50):
            tag = f"{{{i}}}"
            cluster.execute_command('TS.CREATE', f'ts:{tag}', 'LABELS',
                                    'metric', f'metric{i % 5}',
                                    'host', f'host{i % 10}')

        stats = self.get_stats()
        assert stats['totalSeries'] == 50
        # 5 unique metrics + 10 unique hosts = 15 unique label pairs
        assert stats['totalLabelValuePairs'] == 15

    def test_stats_cluster_same_key_different_labels(self):
        """Test TS.LABELSTATS doesn't count the same label name with different values incorrectly."""
        cluster: ValkeyCluster = self.new_cluster_client()

        cluster.execute_command('TS.CREATE', 'ts:{1}', 'LABELS', 'env', 'prod')
        cluster.execute_command('TS.CREATE', 'ts:{2}', 'LABELS', 'env', 'staging')
        cluster.execute_command('TS.CREATE', 'ts:{3}', 'LABELS', 'env', 'dev')

        stats = self.get_stats()

        assert stats['totalSeries'] == 3
        assert stats['totalLabelValuePairs'] == 3

        pair_counts = {item[0]: item[1] for item in stats['seriesCountByLabelValuePair']}
        assert len([k for k in pair_counts.keys() if k.startswith('env=')]) == 3

    def test_stats_cluster_label_parameter_custom_label(self):
        """Test TS.LABELSTATS LABEL parameter."""
        cluster: ValkeyCluster = self.new_cluster_client()

        cluster.execute_command('TS.CREATE', 'ts:{1}', 'LABELS', 'region', 'us-east', 'env', 'prod')
        cluster.execute_command('TS.CREATE', 'ts:{2}', 'LABELS', 'region', 'us-east', 'env', 'dev')
        cluster.execute_command('TS.CREATE', 'ts:{3}', 'LABELS', 'region', 'us-west', 'env', 'prod')
        cluster.execute_command('TS.CREATE', 'ts:{4}', 'LABELS', 'region', 'eu-west', 'env', 'prod')

        stats = self.get_stats(label='region')

        assert stats['totalSeries'] == 4

        focus_label_counts = {item[0]: item[1] for item in stats['seriesCountByFocusLabelValue']}

        assert focus_label_counts.get(b'us-east') == 2
        assert focus_label_counts.get(b'us-west') == 1
        assert focus_label_counts.get(b'eu-west') == 1

    def test_stats_cluster_label_parameter_with_limit(self):
        """Test TS.LABELSTATS LABEL parameter combined with LIMIT."""
        cluster: ValkeyCluster = self.new_cluster_client()

        # Create series with many different status values
        for i in range(10):
            tag = f"{{{i}}}"
            cluster.execute_command('TS.CREATE', f'ts:{tag}', 'LABELS', 'status', f'status_{i}')

        stats = self.get_stats(limit=5, label='status')

        assert stats['totalSeries'] == 10
        # Should only return top 5 status values
        assert len(stats['seriesCountByFocusLabelValue']) <= 5

    def test_stats_cluster_label_parameter_nonexistent_label(self):
        """Test TS.LABELSTATS LABEL parameter with a label name that doesn't exist."""
        cluster: ValkeyCluster = self.new_cluster_client()

        cluster.execute_command('TS.CREATE', 'ts:{1}', 'LABELS', 'env', 'prod')
        cluster.execute_command('TS.CREATE', 'ts:{2}', 'LABELS', 'env', 'dev')

        stats = self.get_stats(label='nonexistent')

        assert stats['totalSeries'] == 2
        # No series have the 'nonexistent' label
        assert 'seriesCountByFocusLabelValue' not in stats or stats['seriesCountByFocusLabelValue'] is None or len(
            stats['seriesCountByFocusLabelValue']) == 0

    def test_stats_cluster_label_parameter_partial_coverage(self):
        """Test TS.LABELSTATS LABEL parameter where only some series have the label."""
        cluster: ValkeyCluster = self.new_cluster_client()

        # Some series with 'tier' label, some without
        cluster.execute_command('TS.CREATE', 'ts:{1}', 'LABELS', 'env', 'prod', 'tier', 'frontend')
        cluster.execute_command('TS.CREATE', 'ts:{2}', 'LABELS', 'env', 'prod', 'tier', 'backend')
        cluster.execute_command('TS.CREATE', 'ts:{3}', 'LABELS', 'env', 'prod')  # No tier
        cluster.execute_command('TS.CREATE', 'ts:{4}', 'LABELS', 'env', 'dev')  # No tier

        stats = self.get_stats(label='tier')

        assert stats['totalSeries'] == 4

        focus_label_counts = {item[0]: item[1] for item in stats['seriesCountByFocusLabelValue']}

        # Only 2 series have tier labels
        assert focus_label_counts.get(b'frontend') == 1
        assert focus_label_counts.get(b'backend') == 1
        assert len(focus_label_counts) == 2

    def test_stats_cluster_label_parameter_duplicate_values(self):
        """Test TS.LABELSTATS LABEL parameter with many series sharing the same label value."""
        cluster: ValkeyCluster = self.new_cluster_client()

        # 15 series all with priority=high
        for i in range(15):
            tag = f"{{{i}}}"
            cluster.execute_command('TS.CREATE', f'ts:{tag}', 'LABELS', 'priority', 'high', 'id', f'{i}')

        # 5 series with priority=low
        for i in range(15, 20):
            tag = f"{{{i}}}"
            cluster.execute_command('TS.CREATE', f'ts:{tag}', 'LABELS', 'priority', 'low', 'id', f'{i}')

        stats = self.get_stats(label='priority')

        assert stats['totalSeries'] == 20

        focus_label_counts = {item[0]: item[1] for item in stats['seriesCountByFocusLabelValue']}

        assert focus_label_counts.get(b'high') == 15
        assert focus_label_counts.get(b'low') == 5

    def test_stats_cluster_label_parameter_empty_string(self):
        """Test TS.LABELSTATS with empty LABEL parameter defaults to __name__."""
        cluster: ValkeyCluster = self.new_cluster_client()

        cluster.execute_command('TS.CREATE', 'ts:{1}', 'LABELS', '__name__', 'metric1', 'env', 'test')
        cluster.execute_command('TS.CREATE', 'ts:{2}', 'LABELS', '__name__', 'metric1', 'env', 'prod')
        cluster.execute_command('TS.CREATE', 'ts:{3}', 'LABELS', '__name__', 'metric2', 'env', 'stage')

        # Empty label should default to __name__
        stats = self.get_stats(label='')

        focus_label_counts = {item[0]: item[1] for item in stats['seriesCountByFocusLabelValue']}
        assert focus_label_counts.get(b'metric1') == 2
        assert focus_label_counts.get(b'metric2') == 1

    def test_stats_cluster_focus_section_only_with_label(self):
        """Test the cluster reply shape matches standalone: focus section iff LABEL was given."""
        cluster: ValkeyCluster = self.new_cluster_client()

        cluster.execute_command('TS.CREATE', 'ts:{1}', 'LABELS', '__name__', 'm1', 'host', 'h1')
        cluster.execute_command('TS.CREATE', 'ts:{2}', 'LABELS', '__name__', 'm2', 'host', 'h2')

        assert 'seriesCountByFocusLabelValue' not in self.get_stats()
        assert 'seriesCountByFocusLabelValue' in self.get_stats(label='host')

        # An explicit empty LABEL still asks for a focus section, defaulting to the metric name.
        focus = {item[0]: item[1] for item in
                 self.get_stats(label='')['seriesCountByFocusLabelValue']}
        assert focus == {b'm1': 1, b'm2': 1}

    def create_filter_fixtures(self):
        """Six series spread across shards: four in us-east, two in eu-west."""
        cluster: ValkeyCluster = self.new_cluster_client()

        for i in range(4):
            cluster.execute_command('TS.CREATE', f'ts:{{{i}}}', 'LABELS',
                                    '__name__', 'http_requests',
                                    'region', 'us-east',
                                    'env', 'prod' if i < 3 else 'dev',
                                    'id', f'{i}')
        for i in range(4, 6):
            cluster.execute_command('TS.CREATE', f'ts:{{{i}}}', 'LABELS',
                                    '__name__', 'db_queries',
                                    'region', 'eu-west',
                                    'env', 'prod',
                                    'tier', 'edge',
                                    'id', f'{i}')

    def test_stats_cluster_filter_restricts_counts(self):
        """Test TS.LABELSTATS FILTER aggregates only matching series across shards."""
        self.create_filter_fixtures()

        stats = self.get_stats(limit=100, filters=['region=us-east'])

        assert stats['totalSeries'] == 4

        metric_counts = {item[0]: item[1] for item in stats['seriesCountByMetricName']}
        assert metric_counts.get('http_requests') == 4
        assert 'db_queries' not in metric_counts

        pair_counts = {item[0]: item[1] for item in stats['seriesCountByLabelValuePair']}
        assert pair_counts.get('region=us-east') == 4
        assert pair_counts.get('env=prod') == 3
        assert pair_counts.get('env=dev') == 1
        assert 'region=eu-west' not in pair_counts

    def test_stats_cluster_filter_distinct_totals(self):
        """Test the cluster-wide distinct totals are computed over the matching series only."""
        self.create_filter_fixtures()

        stats = self.get_stats(limit=100, filters=['region=us-east'])

        # __name__, region, env, id — 'tier' belongs only to the eu-west series.
        assert stats['totalLabels'] == 4
        # __name__=http_requests, region=us-east, env=prod, env=dev, and 4 distinct ids.
        assert stats['totalLabelValuePairs'] == 8

        label_counts = {item[0]: item[1] for item in stats['labelValueCountByLabelName']}
        assert 'tier' not in label_counts

    def test_stats_cluster_filter_with_label(self):
        """Test TS.LABELSTATS FILTER combined with LABEL in cluster mode."""
        self.create_filter_fixtures()

        stats = self.get_stats(label='env', filters=['region=us-east'])

        assert stats['totalSeries'] == 4

        focus_counts = {item[0]: item[1] for item in stats['seriesCountByFocusLabelValue']}
        assert focus_counts.get(b'prod') == 3
        assert focus_counts.get(b'dev') == 1

    def test_stats_cluster_filter_multiple_selectors(self):
        """Test several FILTER selectors are AND-ed across shards."""
        self.create_filter_fixtures()

        stats = self.get_stats(filters=['region=us-east', 'env=prod'])

        assert stats['totalSeries'] == 3

        pair_counts = {item[0]: item[1] for item in stats['seriesCountByLabelValuePair']}
        assert pair_counts.get('env=prod') == 3
        assert 'env=dev' not in pair_counts

    def test_stats_cluster_filter_no_matching_series(self):
        """Test a FILTER matching nothing on any shard reports zero everywhere."""
        self.create_filter_fixtures()

        stats = self.get_stats(filters=['region=ap-south'])

        assert stats['totalSeries'] == 0
        assert stats['totalLabels'] == 0
        assert stats['totalLabelValuePairs'] == 0
        assert stats['seriesCountByLabelValuePair'] == []

    def test_stats_cluster_label_parameter_across_all_shards(self):
        """Test TS.LABELSTATS LABEL parameter aggregates correctly across all shards."""
        cluster: ValkeyCluster = self.new_cluster_client()

        # Distribute series with the same label across different shards
        for i in range(20):
            tag = f"{{{i}}}"
            datacenter = 'dc1' if i < 10 else 'dc2'
            cluster.execute_command('TS.CREATE', f'ts:{tag}', 'LABELS', 'datacenter', datacenter, 'id', f'{i}')

        stats = self.get_stats(label='datacenter')

        assert stats['totalSeries'] == 20

        focus_label_counts = {item[0]: item[1] for item in stats['seriesCountByFocusLabelValue']}

        assert focus_label_counts.get(b'dc1') == 10
        assert focus_label_counts.get(b'dc2') == 10
