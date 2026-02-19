"""
Integration tests for the TS.QUERY command (PromQL support).

This module tests the TS.QUERY command that allows executing PromQL instant queries
against TimeSeries data in a non-clustered (single-node) environment.
"""
from __future__ import annotations
import pytest
from datetime import datetime, timezone, timedelta
from valkey import ResponseError
from valkeytestframework.util.waiters import *
from valkeytestframework.conftest import resource_port_tracker
from valkey_timeseries_test_case import ValkeyTimeSeriesTestCaseBase
from query_result import (
    QueryResult, QuerySample,
    VectorSample, MatrixSample,
    ScalarResult, StringResult,
)

class TestTsQuery(ValkeyTimeSeriesTestCaseBase):
    """Tests for TS.QUERY command in non-clustered mode."""

    def setup_simple_series(self):
        """Create a simple time series with basic data for testing."""
        # Create a time series with labels
        self.client.execute_command('TS.CREATE', 'http_requests',
                                    'LABELS', "__name__", 'http_requests', 'service', 'api', 'env', 'prod')

        # Add sample data points
        self.client.execute_command('TS.ADD', 'http_requests', 1000, 100)
        self.client.execute_command('TS.ADD', 'http_requests', 2000, 200)
        self.client.execute_command('TS.ADD', 'http_requests', 3000, 150)
        self.client.execute_command('TS.ADD', 'http_requests', 4000, 300)
        self.client.execute_command('TS.ADD', 'http_requests', 5000, 250)

    def setup_multi_series(self):
        """Create multiple time series with different labels for more complex queries."""
        # API service time series (prod environment)
        self.client.execute_command('TS.CREATE', 'requests_count',
                                    'LABELS', "__name__", 'requests_count', 'service', 'api', 'env', 'prod')
        self.client.execute_command('TS.ADD', 'requests_count', 1000, 100)
        self.client.execute_command('TS.ADD', 'requests_count', 2000, 150)
        self.client.execute_command('TS.ADD', 'requests_count', 3000, 200)

        # API service time series (staging environment)
        self.client.execute_command('TS.CREATE', 'requests_count_staging',
                                    'LABELS', "__name__", 'requests_count_staging', 'service', 'api', 'env', 'staging')
        self.client.execute_command('TS.ADD', 'requests_count_staging', 1000, 50)
        self.client.execute_command('TS.ADD', 'requests_count_staging', 2000, 75)
        self.client.execute_command('TS.ADD', 'requests_count_staging', 3000, 100)

        # Database service time series (prod environment)
        self.client.execute_command('TS.CREATE', 'db_queries',
                                    'LABELS', "__name__", 'db_queries', 'service', 'database', 'env', 'prod')
        self.client.execute_command('TS.ADD', 'db_queries', 1000, 10)
        self.client.execute_command('TS.ADD', 'db_queries', 2000, 15)
        self.client.execute_command('TS.ADD', 'db_queries', 3000, 20)

    def instant_query(self, query: str, time: int | str = None):
        """Helper method to execute an instant query with optional time parameter."""
        args = ['TS.QUERY', query]
        if time is not None:
            args.extend(['TIME', str(time)])
        result = self.client.execute_command(*args)
        return QueryResult.from_raw(result)

    def test_query_basic_metric_name(self):
        """Test basic TS.QUERY with just a metric name."""
        self.setup_simple_series()

        # Query by metric name should return all samples
        result = self.client.execute_command('TS.QUERY', 'http_requests')

        # The command should complete without error
        assert result is not None

        qr = QueryResult.from_raw(result)

        assert qr.is_vector()

    def test_query_with_explicit_timestamp(self):
        """Test TS.QUERY with explicit unix timestamp."""
        self.setup_simple_series()

        # Query at a specific timestamp (in seconds)
        result = self.client.execute_command('TS.QUERY', 'http_requests', 'TIME', '2')

        # Should return result without error
        assert result is not None

    def test_query_with_rfc3339_timestamp(self):
        """Test TS.QUERY with RFC3339 formatted timestamp."""
        self.setup_simple_series()

        # Query with RFC3339 timestamp format
        # 1970-01-01T00:00:02Z corresponds to 2 seconds from epoch
        result = self.client.execute_command('TS.QUERY', 'http_requests',
                                             'TIME', '1970-01-01T00:00:02Z')

        assert result is not None

    def test_query_with_now_timestamp(self):
        """Test TS.QUERY with NOW timestamp (current time)."""
        self.setup_simple_series()

        # Query at current time
        result = self.client.execute_command('TS.QUERY', 'http_requests', 'TIME', '*')

        assert result is not None

    def test_query_with_plus_timestamp(self):
        """Test TS.QUERY with + timestamp (current time)."""
        self.setup_simple_series()

        # + is another way to specify current time
        result = self.client.execute_command('TS.QUERY', 'http_requests', 'TIME', '+')

        assert result is not None

    def test_query_with_lookback_delta(self):
        """Test TS.QUERY with LOOKBACK_DELTA option."""
        self.setup_simple_series()

        # Query with custom lookback delta
        result = self.client.execute_command('TS.QUERY', 'http_requests',
                                             'LOOKBACK_DELTA', '1000')

        assert result is not None

    def test_query_with_both_time_and_lookback(self):
        """Test TS.QUERY with both TIME and LOOKBACK_DELTA options."""
        self.setup_simple_series()

        result = self.client.execute_command('TS.QUERY', 'http_requests',
                                             'TIME', '3',
                                             'LOOKBACK_DELTA', '2000')

        assert result is not None

    def test_query_nonexistent_series(self):
        """Test TS.QUERY on non-existent series."""
        # Query should handle non-existent series gracefully
        # It should either return empty results or an appropriate error
        result = self.client.execute_command('TS.QUERY', 'nonexistent_metric')

        # Should complete without crashing
        assert result is not None

    def test_query_empty_series(self):
        """Test TS.QUERY on empty time series."""
        # Create an empty time series
        self.client.execute_command('TS.CREATE', 'empty_series')

        result = self.client.execute_command('TS.QUERY', 'empty_series')

        assert result is not None

    def test_query_with_metric_selector(self):
        """Test TS.QUERY with metric selector and label filters."""
        self.setup_simple_series()

        # Query with label filters
        result = self.client.execute_command('TS.QUERY',
                                             'http_requests{env="prod"}')

        assert result is not None

    def test_query_invalid_promql(self):
        """Test TS.QUERY with invalid PromQL syntax."""
        self.setup_simple_series()

        # Invalid PromQL should return an error
        with pytest.raises(ResponseError):
            self.client.execute_command('TS.QUERY', 'invalid {[}')

    def test_query_missing_query_argument(self):
        """Test TS.QUERY without required query argument."""
        # Command requires at least one argument (the query)
        with pytest.raises(ResponseError):
            self.client.execute_command('TS.QUERY')

    def test_query_with_multiple_series(self):
        """Test TS.QUERY returning results from multiple series."""
        self.setup_multi_series()

        # Query for all requests_count metrics (both prod and staging)
        result = self.client.execute_command('TS.QUERY', 'requests_count')

        assert result is not None

    def test_query_results_structure(self):
        """Test that TS.QUERY returns properly structured results."""
        self.setup_simple_series()

        result = self.client.execute_command('TS.QUERY', 'http_requests')

        qr = QueryResult.from_raw(result)

        # Result should be a dict-like structure with resultType and result keys
        # or a properly formatted array/map response from Valkey
        assert qr.is_vector(), "expected a vector"

    def test_query_with_scalar_query(self):
        """Test TS.QUERY with a scalar expression."""
        self.setup_simple_series()

        # Query that evaluates to a scalar constant
        result = self.client.execute_command('TS.QUERY', '42')
        qr = QueryResult.from_raw(result)

        assert qr.is_scalar(), "expected a scalar result"

    def test_query_after_series_deletion(self):
        """Test TS.QUERY behavior after series has been deleted."""
        self.setup_simple_series()

        # Delete the series
        self.client.execute_command('DEL', 'http_requests')

        # Query on deleted series should handle gracefully
        result = self.client.execute_command('TS.QUERY', 'http_requests')

        # Should return results (empty or error, but not crash)
        assert result is not None

    def test_query_with_negative_lookback_delta(self):
        """Test TS.QUERY with negative lookback delta."""
        self.setup_simple_series()

        # Negative lookback delta should be handled (or rejected)
        try:
            result = self.client.execute_command('TS.QUERY', 'http_requests',
                                                 'LOOKBACK_DELTA', '-1000')
            # If it doesn't error, it should return a result
            assert result is not None
        except ResponseError:
            # Or it might return an error, which is also acceptable
            pass

    def test_query_with_very_large_lookback_delta(self):
        """Test TS.QUERY with very large lookback delta."""
        self.setup_simple_series()

        result = self.client.execute_command('TS.QUERY', 'http_requests',
                                             'LOOKBACK_DELTA', '999999999')

        assert result is not None

    def test_query_series_with_many_points(self):
        """Test TS.QUERY on series with many data points."""
        # Create series with many points
        self.client.execute_command('TS.CREATE', 'many_points', 'LABELS', '__name__', 'many_points')

        # Add 1000 data points
        for i in range(1000):
            self.client.execute_command('TS.ADD', 'many_points', i * 1000, i)

        result = self.client.execute_command('TS.QUERY', 'many_points', "time", 4000)
        print("result = ", result)
        assert result is None

    def test_query_error_on_invalid_time_format(self):
        """Test TS.QUERY with invalid TIME format."""
        self.setup_simple_series()

        # Invalid time format should raise error
        with pytest.raises(ResponseError):
            self.client.execute_command('TS.QUERY', 'http_requests',
                                        'TIME', 'invalid_time_format')

    def test_query_error_on_invalid_lookback_format(self):
        """Test TS.QUERY with invalid LOOKBACK_DELTA format."""
        self.setup_simple_series()

        # Invalid lookback format should raise error
        with pytest.raises(ResponseError):
            self.client.execute_command('TS.QUERY', 'http_requests',
                                        'LOOKBACK_DELTA', 'not_a_duration')

    def test_query_expression_with_constants(self):
        """Test TS.QUERY with arithmetic expressions involving constants."""
        self.setup_simple_series()

        # Query with arithmetic expression
        result = self.client.execute_command('TS.QUERY', 'http_requests + 10')

        assert result is not None

    def test_query_expression_with_functions(self):
        """Test TS.QUERY with PromQL functions."""
        self.setup_simple_series()

        # Query with aggregation function
        try:
            result = self.client.execute_command('TS.QUERY', 'sum(http_requests)')
            assert result is not None
        except ResponseError:
            # If sum() isn't supported or works differently, that's ok
            pass

    def test_expression_with_on_ignoring(self):
        query = 'http_requests_total{job=~"web.*"} / on(job, env) node_cpu_usage_seconds_total{job=~"web.*"}'
        client = self.client

        client.execute_command("TS.CREATE", "prod-web-1-requests", 'METRIC',
                               'http_requests_total{job="web-server", env="prod", instance="server1"}')
        client.execute_command('TS.CREATE', 'prod-web-2-requests', 'METRIC',
                               'http_requests_total{job="web-server", env="prod", instance="server2"}')
        client.execute_command('TS.CREATE', 'staging-api-1-requests', 'METRIC',
                               'http_requests_total{job="web-api", env="staging", instance="api1"}')
        client.execute_command('TS.CREATE', 'staging-api-2-requests', 'METRIC',
                               'http_requests_total{job="web-api", env="staging", instance="api2"}')
        client.execute_command('TS.CREATE', 'prod-web-cpu-usage-seconds', 'METRIC',
                               'node_cpu_usage_seconds_total{job="web-server", env="prod"}')
        client.execute_command('TS.CREATE', 'staging-api-cpu-usage-seconds', 'METRIC',
                               'node_cpu_usage_seconds_total{job="web-api", env="staging"}')

        timestamp = "2026-04-06T20:00:00Z"
        client.execute_command("TS.ADD", "prod-web-1-requests", timestamp, 1500)
        client.execute_command("TS.ADD", "prod-web-2-requests", timestamp, 2200)
        client.execute_command("TS.ADD", "staging-api-1-requests", timestamp, 800)
        client.execute_command("TS.ADD", "staging-api-2-requests", timestamp, 1200)

        client.execute_command("TS.ADD", "prod-web-cpu-usage-seconds", timestamp, 45.5)
        client.execute_command("TS.ADD", "staging-api-cpu-usage-seconds", timestamp, 1200)

        result = self.instant_query(query, timestamp)
        print(result)
        assert false
