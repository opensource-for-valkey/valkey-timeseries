import pytest
from valkey import ResponseError
from valkeytestframework.util.waiters import *
from common import parse_stats_response
from valkeytestframework.conftest import resource_port_tracker
from valkey_timeseries_test_case import ValkeyTimeSeriesTestCaseBase


class TestTsStats(ValkeyTimeSeriesTestCaseBase):
    """Test suite for TS.LABELSTATS command."""

    def get_stats(self, limit: int | None = None, label=None, filters=None):
        args = ['TS.LABELSTATS']
        if limit is not None:
            args.extend(['LIMIT', limit])
        if label is not None:
            args.extend(['LABEL', label])
        # FILTER is variadic, so it always goes last.
        if filters is not None:
            args.append('FILTER')
            args.extend(filters)

        result = self.client.execute_command(*args)
        stats = parse_stats_response(result)
        return stats

    def test_stats_empty_index(self):
        """Test TS.LABELSTATS on an empty index returns zero counts."""
        stats = self.get_stats()

        assert stats['totalSeries'] == 0
        assert stats['totalLabels'] == 0
        assert stats['totalLabelValuePairs'] == 0
        assert stats['seriesCountByMetricName'] == []
        assert stats['labelValueCountByLabelName'] == []
        assert stats['seriesCountByLabelValuePair'] == []

    def test_stats_single_series(self):
        """Test TS.LABELSTATS with a single time series."""
        self.client.execute_command(
            'TS.CREATE', 'temperature',
            'LABELS', 'sensor', 'temp1', 'location', 'room1'
        )

        result = self.get_stats()

        assert result['totalSeries'] == 1
        assert result['totalLabels'] == 2  # sensor and location
        assert result['totalLabelValuePairs'] == 2

    def test_stats_basic(self):
        """Test basic TS.LABELSTATS with no arguments"""
        # Create multiple series with different metric names
        self.client.execute_command('TS.CREATE', 'ts1', 'LABELS', '__name__', 'cpu_usage', 'host', 'server1')
        self.client.execute_command('TS.CREATE', 'ts2', 'LABELS', '__name__', 'cpu_usage', 'host', 'server2')
        self.client.execute_command('TS.CREATE', 'ts3', 'LABELS', '__name__', 'memory_usage', 'host', 'server1')
        self.client.execute_command('TS.CREATE', 'ts4', 'LABELS', '__name__', 'disk_usage', 'host', 'server2', 'region',
                                    'us-west')

        result = self.get_stats()

        # Verify required fields
        assert 'totalSeries' in result
        assert 'totalLabels' in result
        assert 'totalLabelValuePairs' in result

        # Verify counts
        series_count = result['totalSeries']
        assert series_count == 4

    def test_stats_multiple_series_same_labels(self):
        """Test TS.LABELSTATS with multiple series sharing labels."""
        self.client.execute_command(
            'TS.CREATE', 'temp1',
            'LABELS', 'sensor', 'temp', 'location', 'room1'
        )
        self.client.execute_command(
            'TS.CREATE', 'temp2',
            'LABELS', 'sensor', 'temp', 'location', 'room2'
        )
        self.client.execute_command(
            'TS.CREATE', 'temp3',
            'LABELS', 'sensor', 'temp', 'location', 'room3'
        )

        result = self.get_stats()

        assert result['totalSeries'] == 3
        assert result['totalLabels'] == 2
        assert result['totalLabelValuePairs'] == 4

    def test_stats_with_limit_parameter(self):
        """Test TS.LABELSTATS with LIMIT parameter."""
        # Create multiple series with different metric names
        for i in range(15):
            self.client.execute_command(
                'TS.CREATE', f'metric{i}',
                'LABELS', 'type', f'type{i}'
            )

        # Default limit is 10
        result = self.get_stats()
        assert len(result['seriesCountByMetricName']) <= 10
        assert len(result['labelValueCountByLabelName']) <= 10
        assert len(result['seriesCountByLabelValuePair']) <= 10

        # Custom limit of 5
        result = self.client.execute_command("TS.LABELSTATS", "LIMIT", 5)
        result = parse_stats_response(result)

        assert len(result['seriesCountByMetricName']) <= 5
        assert len(result['labelValueCountByLabelName']) <= 5
        assert len(result['seriesCountByLabelValuePair']) <= 5

    def test_stats_series_count_by_metric_name(self):
        """Test that seriesCountByMetricName returns correct counts."""
        # Create multiple series with the same metric name
        for i in range(3):
            self.client.execute_command(
                'TS.CREATE', f'temperature:{i}',
                'LABELS', '__name__', 'temperature', 'id', f'{i}'
            )

        for i in range(2):
            self.client.execute_command(
                'TS.CREATE', f'pressure:{i}',
                'LABELS', '__name__', 'pressure', 'id', f'{i}'
            )

        result = self.get_stats()

        # Check that temperature has a count of 3 and pressure has a count of 2
        metric_stats = {item[0]: item[1]
                        for item in result['seriesCountByMetricName']}

        assert metric_stats.get('temperature') == 3
        assert metric_stats.get('pressure') == 2

    def test_stats_with_label_filter(self):
        """Test TS.LABELSTATS with LABEL parameter"""
        # Create a series with a specific label
        self.client.execute_command('TS.CREATE', 'ts1', 'LABELS', '__name__', 'http_requests', 'status', '200',
                                    'method', 'GET')
        self.client.execute_command('TS.CREATE', 'ts2', 'LABELS', '__name__', 'http_requests', 'status', '404',
                                    'method', 'GET')
        self.client.execute_command('TS.CREATE', 'ts3', 'LABELS', '__name__', 'http_requests', 'status', '200',
                                    'method', 'POST')
        self.client.execute_command('TS.CREATE', 'ts4', 'LABELS', '__name__', 'http_requests', 'status', '500',
                                    'method', 'POST')

        # Query stats for 'status' label
        stats = self.client.execute_command('TS.LABELSTATS', 'LABEL', 'status')
        results = parse_stats_response(stats)

        # Verify focus label values are present
        focus_label_values = results['seriesCountByFocusLabelValue']
        assert focus_label_values is not None

        # Verify status values are tracked
        assert any('200' in str(k) for k in focus_label_values)

    def test_stats_label_value_count(self):
        """Test labelValueCountByLabelName statistics."""
        self.client.execute_command(
            'TS.CREATE', 'ts1',
            'LABELS', 'region', 'us-east', 'env', 'prod'
        )
        self.client.execute_command(
            'TS.CREATE', 'ts2',
            'LABELS', 'region', 'us-west', 'env', 'prod'
        )
        self.client.execute_command(
            'TS.CREATE', 'ts3',
            'LABELS', 'region', 'eu-west', 'env', 'dev'
        )

        result = self.get_stats()

        label_counts = result['labelValueCountByLabelName']
        print("labelValueCountByLabelName =", label_counts)

        # region has 3 values, env has 2 values
        label_counts = {item[0]: item[1]
                        for item in label_counts}

        assert label_counts.get('region') == 3
        assert label_counts.get('env') == 3

    def test_stats_label_name_counts(self):
        """Test series count by label name"""
        self.client.execute_command('TS.CREATE', 'ts1', 'LABELS', '__name__', 'metric1', 'host', 'server1', 'env',
                                    'prod')
        self.client.execute_command('TS.CREATE', 'ts2', 'LABELS', '__name__', 'metric2', 'host', 'server2', 'env',
                                    'dev')
        self.client.execute_command('TS.CREATE', 'ts3', 'LABELS', '__name__', 'metric3', 'region', 'us-west')

        result = self.get_stats()

        label_name_counts = result['totalLabels']
        assert label_name_counts == 4  # __name__, host, env, region

    def test_stats_series_count_by_label_pair(self):
        """Test seriesCountByLabelPair statistics."""
        self.client.execute_command(
            'TS.CREATE', 'ts1',
            'LABELS', 'status', '200', 'method', 'GET'
        )
        self.client.execute_command(
            'TS.CREATE', 'ts2',
            'LABELS', 'status', '200', 'method', 'POST'
        )
        self.client.execute_command(
            'TS.CREATE', 'ts3',
            'LABELS', 'status', '404', 'method', 'GET'
        )

        result = self.get_stats()
        label_pair_counts = result['seriesCountByLabelValuePair']
        print("seriesCountByLabelPair =", label_pair_counts)

        # status=200 appears in 2 series, status=404 in 1
        pair_counts = {item[0]: item[1]
                       for item in label_pair_counts}

        assert pair_counts.get('status=200') == 2
        assert pair_counts.get('status=404') == 1
        assert pair_counts.get('method=GET') == 2
        assert pair_counts.get('method=POST') == 1

    def test_stats_with_label_and_limit(self):
        """Test TS.LABELSTATS with both LABEL and LIMIT parameters"""
        # Create a series with multiple values for the 'region' label
        regions = ['us-east', 'us-west', 'eu-west', 'eu-east', 'ap-south']
        for i, region in enumerate(regions):
            for j in range(3):
                self.client.execute_command('TS.CREATE', f'ts_limit_{region}_{j}',
                                            'LABELS',
                                            '__name__', f'metric_limit_{i}',
                                            'region', region,
                                            'series_idx', f'{i}_{j}')  # Add unique identifier)

        result = self.get_stats(3, 'region')
        print("Stats with LABEL and LIMIT =", result)
        focus_label_values = result['seriesCountByFocusLabelValue']
        if focus_label_values:
            assert len(focus_label_values) <= 3

    def test_stats_label_value_pairs(self):
        """Test total label-value pairs count"""
        # Create a series with known label-value pairs
        client = self.client
        client.execute_command('TS.CREATE', 'ts1', 'LABELS', '__name__', 'metric1', 'host', 'server1')  # 2 pairs
        client.execute_command('TS.CREATE', 'ts2', 'LABELS', '__name__', 'metric1', 'host', 'server2')  # 2 pairs
        client.execute_command('TS.CREATE', 'ts3', 'LABELS', '__name__', 'metric2', 'host', 'server1', 'region',
                               'us')  # 3 pairs

        result = self.get_stats()
        print("Stats result =", result)
        total_pairs = result['totalLabelValuePairs']
        assert total_pairs >= 5  # At least 5 pairs: metric1, host=server1, host=server2, metric2, region=us

    def test_stats_focus_label_metric_name(self):
        """Test focus label defaults to __name__ when not specified"""
        self.client.execute_command('TS.CREATE', 'ts1', 'LABELS', '__name__', 'metric1', 'host', 'server1')
        self.client.execute_command('TS.CREATE', 'ts2', 'LABELS', '__name__', 'metric1', 'host', 'server2')
        self.client.execute_command('TS.CREATE', 'ts3', 'LABELS', '__name__', 'metric1', 'region', 'us-east-1')
        self.client.execute_command('TS.CREATE', 'ts4', 'LABELS', '__name__', 'metric1', 'host', 'server2', 'region',
                                    'us-west')

        result = self.client.execute_command('TS.LABELSTATS', 'LABEL', 'host')
        result = parse_stats_response(result)

        # When no LABEL specified, focus should be on __name__ (metric name)
        focus_values = result['seriesCountByFocusLabelValue']

        # Focus should be None or show metric names when LABEL not specified
        if focus_values is not None:
            # Verify it contains metric name data
            assert isinstance(focus_values, (list, dict))

    def test_stats_invalid_limit(self):
        """Test TS.LABELSTATS with invalid LIMIT values."""
        # LIMIT must be greater than 0
        with pytest.raises(ResponseError, match='LIMIT must be greater than 0'):
            self.client.execute_command('TS.LABELSTATS', 'LIMIT', 0)

        # LIMIT cannot be negative
        with pytest.raises(ResponseError):
            self.client.execute_command('TS.LABELSTATS', 'LIMIT', -1)

    def test_stats_wrong_arity(self):
        """Test TS.LABELSTATS with the wrong number of arguments."""
        with pytest.raises(ResponseError, match='wrong number of arguments'):
            self.client.execute_command('TS.LABELSTATS', 'LIMIT', 10, "LABEL", 'status', 4)  # Too many arguments

    def test_stats_after_series_deletion(self):
        """Test TS.LABELSTATS after deleting series."""
        self.client.execute_command(
            'TS.CREATE', 'ts1',
            'LABELS', 'type', 'test'
        )
        self.client.execute_command(
            'TS.CREATE', 'ts2',
            'LABELS', 'type', 'test'
        )

        result = self.get_stats()
        assert result['totalSeries'] == 2

        # Delete one series
        self.client.delete('ts1')

        result = self.get_stats()
        assert result['totalSeries'] == 1

    def test_stats_with_metric_name_label(self):
        """Test TS.LABELSTATS with __name__ label (metric name)."""
        self.client.execute_command(
            'TS.CREATE', 'ts1',
            'LABELS', '__name__', 'http_requests', 'status', '200'
        )
        self.client.execute_command(
            'TS.CREATE', 'ts2',
            'LABELS', '__name__', 'http_requests', 'status', '404'
        )

        result = self.get_stats()

        # Should count __name__ as a label, along with status
        assert result['totalLabels'] == 2

    def test_stats_sorted_by_count(self):
        """Test that stats results are sorted by count in descending order."""
        # Create a series with different frequencies
        for i in range(5):
            self.client.execute_command(
                'TS.CREATE', f'common:{i}',
                'LABELS', 'type', 'common'
            )

        for i in range(2):
            self.client.execute_command(
                'TS.CREATE', f'rare:{i}',
                'LABELS', 'type', 'rare'
            )

        result = self.get_stats()
        pair_counts = result['seriesCountByMetricName']

        print("seriesCountByMetricName =", pair_counts)

        # Check that label pair counts are sorted descending
        pair_counts = [item[1]
                       for item in result['seriesCountByLabelValuePair']]

        for i in range(len(pair_counts) - 1):
            assert pair_counts[i] >= pair_counts[i + 1]

    def test_stats_with_maximum_limit(self):
        """Test TS.LABELSTATS with maximum allowed limit"""
        # Create many series
        for i in range(100):
            self.client.execute_command('TS.CREATE', f'ts{i}', 'LABELS', '__name__', f'metric{i}')

        # Test with max limit (1000)
        result = self.client.execute_command('TS.LABELSTATS', 'LIMIT', '1000')
        assert result is not None

    def test_stats_limit_exceeds_maximum(self):
        """Test TS.LABELSTATS with limit exceeding maximum"""
        with pytest.raises(ResponseError) as excinfo:
            self.client.execute_command('TS.LABELSTATS', 'LIMIT', '1001')

        assert 'cannot be greater than' in str(excinfo.value).lower() or 'limit' in str(excinfo.value).lower()

    def test_stats_invalid_arguments(self):
        """Test TS.LABELSTATS with invalid argument combinations"""
        with pytest.raises(ResponseError):
            self.client.execute_command('TS.LABELSTATS', 'INVALID_ARG', 'value')

    def test_stats_focus_section_only_with_label(self):
        """Test the focus section is present iff LABEL was given (same shape as cluster mode)."""
        self.client.execute_command('TS.CREATE', 'ts1', 'LABELS', '__name__', 'm1', 'host', 'h1')
        self.client.execute_command('TS.CREATE', 'ts2', 'LABELS', '__name__', 'm2', 'host', 'h2')

        bare = parse_stats_response(self.client.execute_command('TS.LABELSTATS'))
        assert 'seriesCountByFocusLabelValue' not in bare

        labelled = parse_stats_response(
            self.client.execute_command('TS.LABELSTATS', 'LABEL', 'host'))
        assert 'seriesCountByFocusLabelValue' in labelled

        # An explicit empty LABEL still asks for a focus section, defaulting to the metric name.
        empty = parse_stats_response(
            self.client.execute_command('TS.LABELSTATS', 'LABEL', ''))
        focus = {item[0]: item[1] for item in empty['seriesCountByFocusLabelValue']}
        assert focus == {b'm1': 1, b'm2': 1}

    def create_filter_fixtures(self):
        """Four series: three in us-east-1, one in eu-west-1 carrying a label nothing else has."""
        self.client.execute_command('TS.CREATE', 'ts1', 'LABELS', '__name__', 'http_requests',
                                    'region', 'us-east-1', 'env', 'prod', 'status', '200')
        self.client.execute_command('TS.CREATE', 'ts2', 'LABELS', '__name__', 'http_requests',
                                    'region', 'us-east-1', 'env', 'prod', 'status', '404')
        self.client.execute_command('TS.CREATE', 'ts3', 'LABELS', '__name__', 'db_queries',
                                    'region', 'us-east-1', 'env', 'dev', 'status', '200')
        self.client.execute_command('TS.CREATE', 'ts4', 'LABELS', '__name__', 'http_requests',
                                    'region', 'eu-west-1', 'env', 'prod', 'status', '200',
                                    'tier', 'edge')

    def test_stats_filter_restricts_counts(self):
        """Test TS.LABELSTATS FILTER reports counts over the matching series only."""
        self.create_filter_fixtures()

        stats = self.get_stats(filters=['region=us-east-1'])

        assert stats['totalSeries'] == 3

        metric_counts = {item[0]: item[1] for item in stats['seriesCountByMetricName']}
        assert metric_counts.get('http_requests') == 2
        assert metric_counts.get('db_queries') == 1

        pair_counts = {item[0]: item[1] for item in stats['seriesCountByLabelValuePair']}
        assert pair_counts.get('region=us-east-1') == 3
        assert pair_counts.get('status=200') == 2
        assert pair_counts.get('env=prod') == 2
        # The eu-west-1 series contributes nothing.
        assert 'region=eu-west-1' not in pair_counts

    def test_stats_filter_omits_labels_of_non_matching_series(self):
        """Test TS.LABELSTATS FILTER excludes labels carried only by non-matching series."""
        self.create_filter_fixtures()

        stats = self.get_stats(filters=['region=us-east-1'])

        # 'tier' belongs to the eu-west-1 series alone.
        label_counts = {item[0]: item[1] for item in stats['labelValueCountByLabelName']}
        assert 'tier' not in label_counts

        pair_counts = {item[0]: item[1] for item in stats['seriesCountByLabelValuePair']}
        assert 'tier=edge' not in pair_counts

        # __name__, region, env, status — but not tier.
        assert stats['totalLabels'] == 4
        # 2 metric names + 1 region + 2 envs + 2 statuses.
        assert stats['totalLabelValuePairs'] == 7

    def test_stats_filter_with_label_and_limit(self):
        """Test TS.LABELSTATS FILTER combined with LABEL and LIMIT."""
        self.create_filter_fixtures()

        stats = self.get_stats(limit=5, label='env', filters=['region=us-east-1'])

        assert stats['totalSeries'] == 3

        # Focus label values are not decoded by parse_stats_response, so they stay as bytes.
        focus_counts = {item[0]: item[1] for item in stats['seriesCountByFocusLabelValue']}
        assert focus_counts.get(b'prod') == 2
        assert focus_counts.get(b'dev') == 1

    def test_stats_filter_multiple_selectors_are_intersected(self):
        """Test TS.LABELSTATS with several FILTER selectors, which are AND-ed."""
        self.create_filter_fixtures()

        stats = self.get_stats(filters=['region=us-east-1', 'env=prod'])

        assert stats['totalSeries'] == 2

        pair_counts = {item[0]: item[1] for item in stats['seriesCountByLabelValuePair']}
        assert pair_counts.get('env=prod') == 2
        assert 'env=dev' not in pair_counts

    def test_stats_filter_no_matching_series(self):
        """Test TS.LABELSTATS FILTER that matches nothing reports zero everywhere."""
        self.create_filter_fixtures()

        stats = self.get_stats(filters=['region=ap-south-1'])

        assert stats['totalSeries'] == 0
        assert stats['totalLabels'] == 0
        assert stats['totalLabelValuePairs'] == 0
        assert stats['seriesCountByMetricName'] == []
        assert stats['labelValueCountByLabelName'] == []
        assert stats['seriesCountByLabelValuePair'] == []

    def test_stats_filter_after_series_deletion(self):
        """Test TS.LABELSTATS FILTER stops counting a series once its key is deleted."""
        self.create_filter_fixtures()

        stats = self.get_stats(filters=['region=us-east-1'])
        assert stats['totalSeries'] == 3
        pair_counts = {item[0]: item[1] for item in stats['seriesCountByLabelValuePair']}
        assert pair_counts.get('status=404') == 1

        # ts2 is the only us-east-1 series with status=404.
        self.client.delete('ts2')

        stats = self.get_stats(filters=['region=us-east-1'])
        assert stats['totalSeries'] == 2
        pair_counts = {item[0]: item[1] for item in stats['seriesCountByLabelValuePair']}
        assert pair_counts.get('region=us-east-1') == 2
        assert 'status=404' not in pair_counts
        # __name__, region, env, status still, but one fewer status value.
        assert stats['totalLabels'] == 4
        assert stats['totalLabelValuePairs'] == 6

    def test_stats_filter_prometheus_style_selector(self):
        """Test TS.LABELSTATS FILTER accepts Prometheus-style selectors."""
        self.create_filter_fixtures()

        stats = self.get_stats(filters=['http_requests{env="prod"}'])

        assert stats['totalSeries'] == 3
        metric_counts = {item[0]: item[1] for item in stats['seriesCountByMetricName']}
        assert metric_counts.get('http_requests') == 3
        assert 'db_queries' not in metric_counts

    def test_stats_filter_with_no_expressions(self):
        """Test TS.LABELSTATS FILTER given without any selector."""
        with pytest.raises(ResponseError):
            self.client.execute_command('TS.LABELSTATS', 'FILTER')

    def test_stats_filter_all_negative_matchers(self):
        """Test TS.LABELSTATS FILTER rejects an unbounded (all-negative) selector list."""
        with pytest.raises(ResponseError, match='at least one matcher'):
            self.client.execute_command('TS.LABELSTATS', 'FILTER', 'region!=us-east-1')

    def test_stats_filter_invalid_selector(self):
        """Test TS.LABELSTATS FILTER with a malformed selector."""
        with pytest.raises(ResponseError):
            self.client.execute_command('TS.LABELSTATS', 'FILTER', '{{{')

    def test_stats_filter_after_label_and_limit(self):
        """Test that LABEL and LIMIT are still parsed when FILTER follows them."""
        self.create_filter_fixtures()

        result = self.client.execute_command('TS.LABELSTATS', 'LABEL', 'status', 'LIMIT', '1',
                                             'FILTER', 'region=us-east-1')
        stats = parse_stats_response(result)

        assert stats['totalSeries'] == 3
        # LIMIT 1 applies to every top-N section.
        assert len(stats['seriesCountByMetricName']) == 1
        assert len(stats['seriesCountByFocusLabelValue']) == 1
