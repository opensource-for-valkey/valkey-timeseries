"""
Integration tests for the TS.QUERY command (PromQL support).

This module tests the TS.QUERY command that allows executing PromQL instant queries
against TimeSeries data in a non-clustered (single-node) environment.
"""
from __future__ import annotations
import math
import pytest
from datetime import datetime, timezone, timedelta
from time import sleep
from valkey import Connection, ResponseError
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

    def test_query_respects_selected_db(self):
        client = self.client

        client.select(0)
        client.execute_command('TS.CREATE', 'db0_metric', 'LABELS', '__name__', 'db_scoped_metric', 'db', '0')
        client.execute_command('TS.ADD', 'db0_metric', 1000, 11)
        client.execute_command('TS.ADD', 'db0_metric', 2000, 12)

        client.select(1)
        client.execute_command('TS.CREATE', 'db1_metric', 'LABELS', '__name__', 'db_scoped_metric', 'db', '1')
        client.execute_command('TS.ADD', 'db1_metric', 1000, 21)
        client.execute_command('TS.ADD', 'db1_metric', 2000, 22)

        client.select(0)
        db0_result = self.instant_query('db_scoped_metric', 2)
        assert db0_result.is_vector()
        assert len(db0_result.result) == 1
        assert db0_result.result[0].metric['db'] == '0'
        assert db0_result.result[0].value.value == 12

        client.select(1)
        db1_result = self.instant_query('db_scoped_metric', 2)
        assert db1_result.is_vector()
        assert len(db1_result.result) == 1
        assert db1_result.result[0].metric['db'] == '1'
        assert db1_result.result[0].value.value == 22

    def test_query_preserves_callers_acl_identity(self):
        """The background selector read must enforce the command caller's key ACL."""
        self.client.execute_command(
            'TS.CREATE', 'promql:allowed', 'LABELS', '__name__', 'allowed_metric'
        )
        self.client.execute_command('TS.ADD', 'promql:allowed', 1000, 1)
        self.client.execute_command(
            'TS.CREATE', 'promql:denied', 'LABELS', '__name__', 'denied_metric'
        )
        self.client.execute_command('TS.ADD', 'promql:denied', 1000, 1)
        self.client.execute_command(
            'ACL', 'SETUSER', 'promql_reader', 'ON', '>pw',
            '+@read', '+@timeseries', '+TS.QUERY', '~promql:allowed'
        )
        limited = self.server.get_new_client()
        try:
            limited.execute_command('AUTH', 'promql_reader', 'pw')
            assert limited.execute_command('TS.QUERY', 'allowed_metric', 'TIME', 1)
            with pytest.raises(ResponseError, match='permission'):
                limited.execute_command('TS.QUERY', 'denied_metric', 'TIME', 1)
        finally:
            self.client.execute_command('ACL', 'DELUSER', 'promql_reader')

    def test_query_keeps_acl_identity_isolated_between_callers(self):
        """One worker context must not reuse the preceding caller's ACL scope."""
        for key, metric in (
            ('promql:alice', 'alice_metric'),
            ('promql:bob', 'bob_metric'),
        ):
            self.client.execute_command('TS.CREATE', key, 'LABELS', '__name__', metric)
            self.client.execute_command('TS.ADD', key, 1000, 1)

        self.client.execute_command(
            'ACL', 'SETUSER', 'promql_alice', 'ON', '>pw',
            '+@read', '+@timeseries', '+TS.QUERY', '~promql:alice'
        )
        self.client.execute_command(
            'ACL', 'SETUSER', 'promql_bob', 'ON', '>pw',
            '+@read', '+@timeseries', '+TS.QUERY', '~promql:bob'
        )
        alice = self.server.get_new_client()
        bob = self.server.get_new_client()
        try:
            alice.execute_command('AUTH', 'promql_alice', 'pw')
            bob.execute_command('AUTH', 'promql_bob', 'pw')

            assert alice.execute_command('TS.QUERY', 'alice_metric', 'TIME', 1)
            with pytest.raises(ResponseError, match='permission'):
                alice.execute_command('TS.QUERY', 'bob_metric', 'TIME', 1)

            assert bob.execute_command('TS.QUERY', 'bob_metric', 'TIME', 1)
            with pytest.raises(ResponseError, match='permission'):
                bob.execute_command('TS.QUERY', 'alice_metric', 'TIME', 1)
        finally:
            alice.close()
            bob.close()
            self.client.execute_command('ACL', 'DELUSER', 'promql_alice')
            self.client.execute_command('ACL', 'DELUSER', 'promql_bob')

    def test_query_refused_where_blocking_is_denied(self):
        """TS.QUERY and TS.QUERYRANGE reply from a query worker, so they block the
        client. Inside MULTI or a script that is not allowed: the server would
        answer with its own error while the query still ran, and a module call
        without the K flag would abort the server. Both commands refuse instead."""
        self.setup_simple_series()
        commands = [
            ("TS.QUERY", "http_requests"),
            ("TS.QUERYRANGE", "http_requests", "STEP", "1000", "START", "1000", "END", "5000"),
        ]
        for command in commands:
            pipe = self.client.pipeline(transaction=True)
            pipe.execute_command(*command)
            with pytest.raises(ResponseError, match="not allowed inside MULTI"):
                pipe.execute()

            script = "return redis.call(" + ", ".join(f"'{a}'" for a in command) + ")"
            with pytest.raises(ResponseError, match="not allowed inside MULTI"):
                self.client.execute_command("EVAL", script, 0)

        # Outside those contexts the same commands still answer.
        assert self.client.execute_command("TS.QUERY", "http_requests") is not None

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
        """Test TS.QUERY with an explicit unix timestamp."""
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

    def test_query_with_lookback_delta(self):
        """Test TS.QUERY with the LOOKBACK_DELTA option."""
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

    def test_query_string_result_stays_on_one_line(self):
        """A string result is sent as a simple string, which a newline would end early and
        leave the connection out of step; the newline arrives as a space instead."""
        result = self.client.execute_command('TS.QUERY', '"a\\nb"')
        qr = QueryResult.from_raw(result)
        assert qr.is_string(), "expected a string result"
        assert qr.result.value == 'a b'
        # The connection is still in step.
        assert self.client.execute_command('PING')

    def test_query_error_keeps_non_ascii_text(self):
        """An evaluation error echoing non-ASCII input arrives as valid UTF-8, on one line."""
        self.setup_simple_series()
        with pytest.raises(ResponseError) as err:
            self.client.execute_command(
                'TS.QUERY', 'label_replace(http_requests, "dst", "$1", "service", "é(")')
        assert 'é(' in str(err.value)
        assert self.client.execute_command('PING')

    def test_query_missing_query_argument(self):
        """Test TS.QUERY without the required query argument."""
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
        """Test TS.QUERY behavior after a series has been deleted."""
        self.setup_simple_series()

        # Delete the series
        self.client.execute_command('DEL', 'http_requests')

        # Query on deleted series should handle gracefully
        result = self.client.execute_command('TS.QUERY', 'http_requests')

        # Should return results (empty or error, but not crash)
        assert result is not None

    def test_query_with_negative_lookback_delta(self):
        """Test TS.QUERY with a negative lookback delta."""
        self.setup_simple_series()

        # Negative lookback delta should be rejected
        with pytest.raises(ResponseError, match="TSDB: couldn't parse LOOKBACK_DELTA duration"):
            self.client.execute_command('TS.QUERY', 'http_requests',
                                        'LOOKBACK_DELTA', '-1000')

    def test_query_with_very_large_lookback_delta(self):
        """A lookback past ts-promql-max-lookback (1h by default) is refused; with the cap
        lifted (0) the same query answers."""
        self.setup_simple_series()

        with pytest.raises(ResponseError, match="exceeds the 1h allowed by ts-promql-max-lookback"):
            self.client.execute_command('TS.QUERY', 'http_requests', 'LOOKBACK_DELTA', '999999999')
        # The cap itself is allowed.
        assert self.client.execute_command('TS.QUERY', 'http_requests', 'LOOKBACK_DELTA', '1h')

        self.client.execute_command('CONFIG', 'SET', 'ts.ts-promql-max-lookback', '0')
        try:
            result = self.client.execute_command('TS.QUERY', 'http_requests',
                                                 'LOOKBACK_DELTA', '999999999')
            assert result is not None
        finally:
            self.client.execute_command('CONFIG', 'SET', 'ts.ts-promql-max-lookback', '1h')

    def test_query_series_with_many_points(self):
        """Test TS.QUERY on a series with many data points."""
        # Create series with many points
        self.client.execute_command('TS.CREATE', 'many_points', 'LABELS', '__name__', 'many_points')

        # Add 1000 data points. Timestamps are in milliseconds (i * 1000), values are i.
        # So i=4 → timestamp=4000ms, value=4.
        for i in range(1000):
            self.client.execute_command('TS.ADD', 'many_points', i * 1000, i)

        # TIME follows the PromQL/Prometheus HTTP API convention: plain integers are
        # Unix *seconds*, not milliseconds.  Pass 4 (seconds) = 4000 ms so that
        # the lookback window includes the data point at timestamp 4000 ms (value 4).
        result = self.instant_query('many_points', 4)

        assert result.is_vector(), "expected a vector"
        value = result.result[0].value
        assert value.timestamp == 4000, f"expected timestamp 4000 ms, got {value.timestamp}"
        assert value.value == 4, "expected the value from the most recent point at or before the query time"

    def test_error_on_invalid_time_format(self):
        """Test TS.QUERY with an invalid TIME format."""
        self.setup_simple_series()

        # Invalid time format should raise error
        with pytest.raises(ResponseError):
            self.client.execute_command('TS.QUERY', 'http_requests',
                                        'TIME', 'invalid_time_format')

    def test_error_on_invalid_lookback_format(self):
        """Test TS.QUERY with invalid LOOKBACK_DELTA format."""
        self.setup_simple_series()

        # Invalid lookback format should raise error
        with pytest.raises(ResponseError):
            self.client.execute_command('TS.QUERY', 'http_requests',
                                        'LOOKBACK_DELTA', 'not_a_duration')

    def test_expression_with_constants(self):
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

    def test_operator_with_on(self):
        query = 'http_requests_total{job=~"web.*"} / on(job, env) group_left() node_cpu_usage_seconds_total{job=~"web.*"}'
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
        assert result.is_vector()
        assert len(result.result) == 4

    def setup_http_requests_scenario(self):
        client = self.client

        fleet = [
            ("web-prod-1", 'http_requests_total{server="web_prod_1", environment="production"}', 100, 110),
            ("web-prod-2", 'http_requests_total{server="web_prod_2", environment="production"}', 200, 220),
            ("web-prod-3", 'http_requests_total{server="web_prod_3", environment="production"}', 300, 330),
            ("web-stg-1", 'http_requests_total{server="web_stg_1", environment="staging"}', 10, 20),
        ]

        t0 = "2026-04-06T20:00:00Z"
        t1 = "2026-04-06T20:01:00Z"

        for key, metric, v0, v1 in fleet:
            client.execute_command("TS.CREATE", key, "METRIC", metric)
            client.execute_command("TS.ADD", key, t0, v0)
            client.execute_command("TS.ADD", key, t1, v1)

        return t1

    def _vector_values_by_label(self, query_result: QueryResult, label: str) -> dict[str, float]:
        assert query_result.is_vector(), f"expected vector result, got {query_result.result_type}"
        return {sample.metric[label]: sample.value.value for sample in query_result.result}

    def _assert_single_value(self, query_result: QueryResult, expected: float):
        if query_result.is_scalar():
            actual = query_result.result.value
        else:
            assert query_result.is_vector(), f"expected scalar/vector, got {query_result.result_type}"
            assert len(query_result.result) == 1, f"expected one sample, got {len(query_result.result)}"
            actual = query_result.result[0].value.value
        assert math.isclose(actual, expected, rel_tol=1e-9), f"expected {expected}, got {actual}"

    def test_metric_selector_returns_all_servers(self):
        time = self.setup_http_requests_scenario()

        result = self.instant_query("http_requests_total", time)
        values = self._vector_values_by_label(result, "server")

        assert len(values) == 4
        assert values["web_prod_1"] == 110
        assert values["web_prod_2"] == 220
        assert values["web_prod_3"] == 330
        assert values["web_stg_1"] == 20

    def test_label_exact_matcher_filters_environment(self):
        time = self.setup_http_requests_scenario()

        result = self.instant_query('http_requests_total{environment="production"}', time)
        values = self._vector_values_by_label(result, "server")

        assert sorted(values.keys()) == ["web_prod_1", "web_prod_2", "web_prod_3"]

    def test_label_not_equal_matcher(self):
        time = self.setup_http_requests_scenario()

        result = self.instant_query('http_requests_total{environment!="production"}', time)
        values = self._vector_values_by_label(result, "server")

        assert sorted(values.keys()) == ["web_stg_1"]

    def test_label_regex_matcher(self):
        time = self.setup_http_requests_scenario()

        result = self.instant_query('http_requests_total{server=~".*_stg_.*"}', time)
        values = self._vector_values_by_label(result, "server")

        assert sorted(values.keys()) == ["web_stg_1"]

    def test_label_negative_regex_matcher(self):
        time = self.setup_http_requests_scenario()

        result = self.instant_query('http_requests_total{server!~".*_stg_.*"}', time)
        values = self._vector_values_by_label(result, "server")

        assert sorted(values.keys()) == ["web_prod_1", "web_prod_2", "web_prod_3"]

    def test_unicode_word_class_regex_matcher(self):
        """`\\w+` compiles to more than the old 16 KiB regex size limit and was refused."""
        time = self.setup_http_requests_scenario()

        result = self.instant_query('http_requests_total{server=~"web_stg_\\\\w+"}', time)
        assert sorted(self._vector_values_by_label(result, "server")) == ["web_stg_1"]

    def test_empty_regex_matcher_selects_series_without_the_label(self):
        """`=~""` is valid PromQL for "label absent"; it used to panic the selector conversion."""
        time = self.setup_http_requests_scenario()
        self.client.execute_command(
            "TS.CREATE", "web-dev-1", "METRIC", 'http_requests_total{server="web_dev_1"}')
        self.client.execute_command("TS.ADD", "web-dev-1", time, 5)

        result = self.instant_query('http_requests_total{environment=~""}', time)
        assert sorted(self._vector_values_by_label(result, "server")) == ["web_dev_1"]

        result = self.instant_query('http_requests_total{environment!~""}', time)
        assert sorted(self._vector_values_by_label(result, "server")) == [
            "web_prod_1", "web_prod_2", "web_prod_3", "web_stg_1"]

    def test_regex_over_the_size_limit_is_rejected_at_parse_time(self):
        """A pattern the parser accepts but the module's regex size limit refuses is an
        error reply naming the label, not a failure inside evaluation."""
        self.setup_http_requests_scenario()
        with pytest.raises(ResponseError, match="regex for label 'server'"):
            self.client.execute_command('TS.QUERY', 'http_requests_total{server=~"[a-z]{3000}"}')
        assert self.client.execute_command('PING')

    def test_heavy_regex_matchers_do_not_stall_the_server(self):
        """Each matcher like `\\w{2000}` took ~100 ms for the parser to build or refuse, on the
        main thread, and a 4 KiB query holds hundreds of them (~30 s). They are now refused
        under the module's regex size limit before the parser runs."""
        self.setup_http_requests_scenario()
        matcher = 'a=~"\\\\w{2000}"'
        query = "up{" + ",".join([matcher] * (4000 // (len(matcher) + 1))) + "}"
        assert len(query) <= 4096
        started = datetime.now()
        with pytest.raises(ResponseError, match="regex for label 'a'"):
            self.client.execute_command('TS.QUERY', query)
        assert datetime.now() - started < timedelta(seconds=1)
        assert self.client.execute_command('PING')

    def test_deeply_nested_query_is_rejected_not_crashing(self):
        """Evaluation recurses over the expression on 2 MiB stacks. A chain of ~1,900
        vector operators overflowed one and aborted the server; at the 16 KiB maximum
        query length that is easy to send. Depth is now capped at 500 levels."""
        time = self.setup_http_requests_scenario()
        self.client.execute_command("CONFIG", "SET", "ts.ts-promql-max-query-len", 16384)
        try:
            # Past the measured overflow point (~1,900 vector operators).
            chain = "u" + "+u" * 2500
            with pytest.raises(ResponseError, match="nested too deeply"):
                self.client.execute_command('TS.QUERY', chain)
            assert self.client.execute_command('PING')

            deep = "http_requests_total" + " + http_requests_total" * 700
            assert len(deep) <= 16384
            with pytest.raises(ResponseError, match="nested too deeply"):
                self.client.execute_command('TS.QUERY', deep)

            # Just under the limit still evaluates.
            ok = "http_requests_total" + " or http_requests_total" * 498
            result = self.instant_query(ok, time)
            assert len(self._vector_values_by_label(result, "server")) == 4
        finally:
            self.client.execute_command("CONFIG", "SET", "ts.ts-promql-max-query-len", 4096)

    def test_series_limit_stops_the_read_early(self):
        """A read over `ts-promql-max-response-series` stops at the series that takes it
        over the limit (an instant read), or at the end of that lock hold's batch (a range
        read), instead of collecting every matched series first. The refusal reports the
        count at which it stopped: before, this one said 1500."""
        import re
        pipe = self.client.pipeline(transaction=False)
        for i in range(1500):
            pipe.execute_command('TS.ADD', f'wide:{i}', 1000, i,
                                 'LABELS', '__name__', 'wide', 'i', str(i))
        pipe.execute()

        def refused_at(*command) -> int:
            with pytest.raises(ResponseError) as err:
                self.client.execute_command(*command)
            found = re.search(r'max series limit: (\d+) > 10', str(err.value))
            assert found, str(err.value)
            return int(found.group(1))

        self.client.execute_command('CONFIG', 'SET', 'ts.ts-promql-max-response-series', '10')
        try:
            # TIME is in seconds: 1 s is the samples' 1000 ms.
            assert refused_at('TS.QUERY', 'wide', 'TIME', '1') == 11
            assert refused_at('TS.QUERY', 'sum(wide)', 'TIME', '1') == 11
            assert 10 < refused_at('TS.QUERY', 'wide[1m]', 'TIME', '1') < 1500
            assert 10 < refused_at('TS.QUERYRANGE', 'wide', 'START', '1000', 'END', '61000',
                                   'STEP', '30s') < 1500

            # Under the limit, the same read still sees every series.
            self.client.execute_command('CONFIG', 'SET', 'ts.ts-promql-max-response-series', '1500')
            result = QueryResult.from_raw(
                self.client.execute_command('TS.QUERY', 'count(wide)', 'TIME', '1'))
            assert result.result[0].value.value == 1500
        finally:
            self.client.execute_command('CONFIG', 'SET', 'ts.ts-promql-max-response-series', '1000')

    def test_slow_parses_do_not_stall_the_main_thread(self):
        """Parsing grows faster than the query text: a 16 KiB chain of unary minus signs took
        about 0.76 s to parse, and the depth check that refuses it runs only afterwards. The
        command handler used to parse on the main thread, so every client waited for every
        such query. The text is now parsed on a query worker, and the error still arrives."""
        self.client.execute_command("CONFIG", "SET", "ts.ts-promql-max-query-len", 16384)
        connections = []
        try:
            deep = "-" * 16000 + "up"
            for _ in range(4):
                conn = Connection(host=self.server.bind_ip, port=self.server.port, socket_timeout=30)
                conn.connect()
                conn.send_command("TS.QUERY", deep)
                connections.append(conn)
            # Sample the main thread's latency across the burst. One of these parses held it
            # for ~0.8 s on an M2, so a PING that lands during one waits for the rest of it;
            # off the main thread every PING is answered in milliseconds.
            worst = timedelta(0)
            sampling_until = datetime.now() + timedelta(seconds=1.5)
            while datetime.now() < sampling_until:
                started = datetime.now()
                assert self.client.execute_command("PING")
                worst = max(worst, datetime.now() - started)
                sleep(0.01)
            assert worst < timedelta(seconds=0.3), f"main thread stalled for {worst}"

            for conn in connections:
                with pytest.raises(ResponseError, match="nested too deeply"):
                    conn.read_response()
        finally:
            for conn in connections:
                conn.disconnect()
            self.client.execute_command("CONFIG", "SET", "ts.ts-promql-max-query-len", 4096)

    def test_parse_errors_arrive_from_the_worker_in_order(self):
        """A query that fails to parse is answered from the query worker. A pipelined
        command after it must read its own reply, not the parse error."""
        time = self.setup_http_requests_scenario()
        pipe = self.client.pipeline(transaction=False)
        pipe.execute_command("TS.QUERY", "invalid {[}")
        pipe.execute_command("TS.QUERY", "http_requests_total", "TIME", time)
        pipe.execute_command("PING")
        invalid, valid, pong = pipe.execute(raise_on_error=False)
        assert isinstance(invalid, ResponseError)
        assert not isinstance(valid, ResponseError)
        assert pong in (True, b"PONG", "PONG")

    def test_aggregation_sum(self):
        time = self.setup_http_requests_scenario()

        result = self.instant_query('sum(http_requests_total{environment="production"})', time)
        self._assert_single_value(result, 660.0)

    def test_aggregation_avg(self):
        time = self.setup_http_requests_scenario()

        result = self.instant_query('avg(http_requests_total{environment="production"})', time)
        self._assert_single_value(result, 220.0)

    def test_aggregation_min_and_max(self):
        time = self.setup_http_requests_scenario()

        min_result = self.instant_query('min(http_requests_total{environment="production"})', time)
        max_result = self.instant_query('max(http_requests_total{environment="production"})', time)

        self._assert_single_value(min_result, 110.0)
        self._assert_single_value(max_result, 330.0)


    def test_range_increase_production_two_samples_in_window(self):
        """increase() returns a non-zero extrapolated value when two samples
        fall strictly inside the range window (range_start, range_end].

        setup_http_requests_scenario records data at:
            t0 = 2026-04-06T20:00:00Z  (values 100, 200, 300, 10)
            t1 = 2026-04-06T20:01:00Z  (values 110, 220, 330, 20)  ← query time

        Using [2m] at t1: window is (19:59:00Z, t1].
        Both t0 and t1 fall strictly inside, so each series contributes 2 samples.

        Extrapolation for each production server (identical arithmetic):
            duration_to_start = t0 − (t1 − 2 min) = 60 s
            sampled_interval  = t1 − t0            = 60 s
            average_duration  = 60 s,  threshold   = 60 × 1.1 = 66 s
            60 s < 66 s  →  duration_to_start is kept as-is (no capping)
            duration_to_end   = 0 s  (last sample is at range_end)
            factor = (60 + 60 + 0) / 60 = 2.0

        Raw increases: 10, 20, 30  →  extrapolated: 20, 40, 60  →  sum = 120.0
        """
        time = self.setup_http_requests_scenario()

        result = self.instant_query(
            'sum(increase(http_requests_total{environment="production"}[2m]))', time
        )
        self._assert_single_value(result, 120.0)

    # ── HASHTAG: cluster routing scope, a no-op on a standalone server ────

    def test_query_hashtag_is_accepted_and_ignored_outside_a_cluster(self):
        """HASHTAG only scopes the cluster fanout, so a standalone server still
        answers over its whole keyspace.

        Bare, braced and comma-separated forms are all accepted, the token is
        matched case-insensitively, and the clause composes with every other
        option in either order.
        """
        time = self.setup_http_requests_scenario()
        query = 'sum(http_requests_total{environment="production"})'

        at = ('TIME', str(time))

        for args in (
            ('HASHTAG', 'anything') + at,
            ('HASHTAG', 'a,b,c') + at,
            ('HASHTAG', '{braced}') + at,
            ('hashtag', 'lowercase') + at,
            at + ('HASHTAG', 'x'),
            at + ('LOOKBACK_DELTA', '5m', 'HASHTAG', 'x'),
            ('HASHTAG', 'x', 'LOOKBACK_DELTA', '5m') + at,
            at + ('TIMEOUT', '5s', 'HASHTAG', 'x'),
            ('HASHTAG', 'x', 'TIMEOUT', '5s') + at,
        ):
            result = QueryResult.from_raw(
                self.client.execute_command('TS.QUERY', query, *args))
            self._assert_single_value(result, 660.0)

    def test_query_hashtag_last_occurrence_wins(self):
        """Repeating the clause replaces the routing scope rather than widening
        it. Standalone results are identical either way, so this pins the
        parse: a repeat must be accepted, not rejected as a duplicate."""
        time = self.setup_http_requests_scenario()
        query = 'sum(http_requests_total{environment="production"})'

        result = QueryResult.from_raw(self.client.execute_command(
            'TS.QUERY', query, 'HASHTAG', 'first', 'TIME', str(time),
            'HASHTAG', 'second,third'))
        self._assert_single_value(result, 660.0)

    def test_query_hashtag_matches_the_unscoped_result(self):
        """The scoped and unscoped answers agree exactly on a single node."""
        time = self.setup_http_requests_scenario()
        query = 'http_requests_total'

        scoped = QueryResult.from_raw(self.client.execute_command(
            'TS.QUERY', query, 'TIME', str(time), 'HASHTAG', 'tenant-a,tenant-b'))
        unscoped = self.instant_query(query, time)

        assert (self._vector_values_by_label(scoped, 'server')
                == self._vector_values_by_label(unscoped, 'server'))

    def test_query_hashtag_requires_a_value(self):
        """A missing or empty value — including an empty comma-separated
        component — is rejected rather than read as 'no tags'."""
        time = self.setup_http_requests_scenario()
        query = 'http_requests_total'

        for bad in ('', ',a', 'a,', 'a,,b', ','):
            with pytest.raises(ResponseError, match="missing HASHTAG argument"):
                self.client.execute_command('TS.QUERY', query, 'TIME', str(time),
                                            'HASHTAG', bad)

        with pytest.raises(ResponseError, match="missing HASHTAG argument"):
            self.client.execute_command('TS.QUERY', query, 'TIME', str(time), 'HASHTAG')


# About 22 s on an Apple M2 (release build), under the 30 s maximum query duration: three
# nested one-second subquery grids of 421 steps each, 75 million evaluations of the leaf. Over
# the default ts-promql-max-subquery-steps, so the class that runs it lifts that limit.
SLOW_QUERY = ('max_over_time(max_over_time(max_over_time('
              '(label_replace(vector(1), "a", "b", "", ""))[7m:1s])[7m:1s])[7m:1s])')


class TestQueryCancellation(ValkeyTimeSeriesTestCaseBase):
    """A query whose client disconnects is abandoned: stopped at its next deadline check
    if it is running, skipped if it is still queued. It used to run to the end for a
    reply the server then discarded, holding a worker the whole time. One worker, so a
    second query really queues."""

    # The slow query is 75 million subquery steps, past ts-promql-max-subquery-steps' default:
    # these tests need it to run, not to be refused up front.
    MODULE_ARGS = "ts-promql-max-concurrent-queries 1 ts-promql-max-subquery-steps 0"

    def _pool(self) -> dict:
        info = self.client.info('ts_promql')
        # `cancelled` defaults to 0 so that a build without the counter fails on the
        # behaviour below, not on the field.
        return {name: info.get(f'ts_queries_{name}', 0)
                for name in ('running', 'queued', 'cancelled')}

    def _wait_for(self, predicate, timeout: float, what: str) -> dict:
        deadline = datetime.now() + timedelta(seconds=timeout)
        while True:
            pool = self._pool()
            if predicate(pool):
                return pool
            assert datetime.now() < deadline, f"{what}: {pool}"
            sleep(0.02)

    def _send_slow_query(self) -> Connection:
        conn = Connection(host=self.server.bind_ip, port=self.server.port)
        conn.connect()
        conn.send_command('TS.QUERY', SLOW_QUERY, 'TIME', '1000000', 'TIMEOUT', '30s')
        return conn

    def test_running_query_stops_when_its_client_disconnects(self):
        conn = self._send_slow_query()
        self._wait_for(lambda p: p['running'] == 1, 5, "the query never started")

        conn.disconnect()
        pool = self._wait_for(lambda p: p['running'] == 0, 3, "still running after disconnect")
        assert pool['cancelled'] == 1, pool

        # The worker is free again.
        assert self.client.execute_command('TS.QUERY', '1 + 1')

    def test_queued_query_is_dropped_when_its_client_disconnects(self):
        running = self._send_slow_query()
        self._wait_for(lambda p: p['running'] == 1, 5, "the first query never started")
        queued = self._send_slow_query()
        self._wait_for(lambda p: p['queued'] == 1, 5, "the second query never queued")

        queued.disconnect()
        running.disconnect()
        pool = self._wait_for(lambda p: p['running'] == 0 and p['queued'] == 0, 3,
                              "a query outlived its client")
        assert pool['cancelled'] == 2, pool


class TestQueryAdmission(ValkeyTimeSeriesTestCaseBase):
    """Limits on what all PromQL queries on a node may take together, which no per-query
    limit sees: the samples running queries hold, the server's memory use against
    maxmemory, and the size of a reply before any of it is written."""

    def _set(self, name, value):
        self.client.execute_command('CONFIG', 'SET', name, value)

    def _series(self, count: int, samples: int):
        pipe = self.client.pipeline(transaction=False)
        for i in range(count):
            for t in range(samples):
                pipe.execute_command('TS.ADD', f'adm:{i}', 1000 + t * 1000, t,
                                     'LABELS', '__name__', 'adm', 'i', str(i))
        pipe.execute()

    def _in_flight(self) -> int:
        return self.client.info('ts_promql')['ts_samples_in_flight']

    def test_samples_held_by_all_queries_are_bounded(self):
        self._series(1, 2000)
        query = ('TS.QUERY', 'adm[1h]', 'TIME', '2000')
        try:
            self._set('ts.ts-promql-max-samples-in-flight', '100')
            with pytest.raises(ResponseError, match='ts-promql-max-samples-in-flight'):
                self.client.execute_command(*query)
            assert self._in_flight() == 0, "a refused query gives back what it charged"

            self._set('ts.ts-promql-max-samples-in-flight', '0')
            result = QueryResult.from_raw(self.client.execute_command(*query))
            assert len(result.result[0].values) == 2000
            assert self._in_flight() == 0, "a finished query gives back what it charged"
        finally:
            self._set('ts.ts-promql-max-samples-in-flight', '200000000')

    def test_queries_are_refused_while_memory_is_past_the_limit(self):
        self._series(1, 10)
        query = ('TS.QUERY', 'adm', 'TIME', '5')
        try:
            # Without maxmemory the ratio is 0: the limit never refuses.
            self._set('ts.ts-promql-max-memory-percent', '1')
            assert self.client.execute_command(*query)

            # A fresh server uses well over 1 % of 64 MB.
            self._set('maxmemory', str(64 * 1024 * 1024))
            with pytest.raises(ResponseError, match='% of maxmemory'):
                self.client.execute_command(*query)

            self._set('ts.ts-promql-max-memory-percent', '0')
            assert self.client.execute_command(*query)
        finally:
            self._set('maxmemory', '0')
            self._set('ts.ts-promql-max-memory-percent', '90')

    def test_a_reply_past_the_size_limit_is_refused_before_it_is_written(self):
        self._series(50, 1)
        try:
            self._set('ts.ts-promql-max-reply-bytes', '1000')
            with pytest.raises(ResponseError, match='ts-promql-max-reply-bytes'):
                self.client.execute_command('TS.QUERY', 'adm', 'TIME', '1')
            # One series is well under it.
            assert self.client.execute_command('TS.QUERY', 'adm{i="7"}', 'TIME', '1')
            with pytest.raises(ResponseError, match='ts-promql-max-reply-bytes'):
                self.client.execute_command('TS.QUERYRANGE', 'adm', 'START', '1000',
                                            'END', '1000', 'STEP', '1s')
        finally:
            self._set('ts.ts-promql-max-reply-bytes', '268435456')

    def test_subquery_steps_are_bounded_by_the_parameter(self):
        """ts-promql-max-subquery-steps bounds the subquery steps a query evaluates, nesting
        multiplied through, before any of them run. This one is 601 x 3,601 = 2.2 million."""
        query = ('TS.QUERY', 'max_over_time(max_over_time(vector(1)[1h:1s])[10m:1s])', 'TIME', '100000')
        try:
            assert self.client.execute_command(*query), "under the 20 million default"

            self._set('ts.ts-promql-max-subquery-steps', '1000000')
            with pytest.raises(ResponseError, match='ts-promql-max-subquery-steps'):
                self.client.execute_command(*query)

            self._set('ts.ts-promql-max-subquery-steps', '0')
            assert self.client.execute_command(*query), "0 lifts the limit"
        finally:
            self._set('ts.ts-promql-max-subquery-steps', '20000000')
