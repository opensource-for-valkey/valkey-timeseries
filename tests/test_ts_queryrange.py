"""Integration tests for the TS.QUERYRANGE command (PromQL range queries)."""

from __future__ import annotations

from datetime import datetime, timezone

import pytest
from valkey import ResponseError
from valkeytestframework.util.waiters import *
from valkeytestframework.conftest import resource_port_tracker

from query_result import QueryResult
from valkey_timeseries_test_case import ValkeyTimeSeriesTestCaseBase


class TestTsQueryRange(ValkeyTimeSeriesTestCaseBase):
    """Tests for TS.QUERYRANGE command in non-clustered mode."""

    def setup_simple_series(self):
        self.client.execute_command(
            "TS.CREATE",
            "http_requests",
            "LABELS",
            "__name__",
            "http_requests",
            "service",
            "api",
            "env",
            "prod",
        )
        self.client.execute_command("TS.ADD", "http_requests", 1000, 100)
        self.client.execute_command("TS.ADD", "http_requests", 2000, 200)
        self.client.execute_command("TS.ADD", "http_requests", 3000, 150)
        self.client.execute_command("TS.ADD", "http_requests", 4000, 300)
        self.client.execute_command("TS.ADD", "http_requests", 5000, 250)

    def setup_realtime_series(self):
        self.client.execute_command("TS.CREATE", "live_requests", "LABELS", "__name__", "live_requests")
        now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
        self.client.execute_command("TS.ADD", "live_requests", now_ms - 9000, 10)
        self.client.execute_command("TS.ADD", "live_requests", now_ms - 6000, 20)
        self.client.execute_command("TS.ADD", "live_requests", now_ms - 3000, 30)
        self.client.execute_command("TS.ADD", "live_requests", now_ms, 40)

    def setup_observability_metrics(self):
        """Setup realistic observability metrics for multi-dimensional queries."""
        # Setup node memory active vs total for arithmetic
        self.client.execute_command("TS.CREATE", "mem_active_1", "LABELS", "__name__", "node_memory_Active_bytes",
                                    "instance", "node1", "env", "prod")
        self.client.execute_command("TS.CREATE", "mem_total_1", "LABELS", "__name__", "node_memory_MemTotal_bytes",
                                    "instance", "node1", "env", "prod")

        self.client.execute_command("TS.CREATE", "mem_active_2", "LABELS", "__name__", "node_memory_Active_bytes",
                                    "instance", "node2", "env", "staging")
        self.client.execute_command("TS.CREATE", "mem_total_2", "LABELS", "__name__", "node_memory_MemTotal_bytes",
                                    "instance", "node2", "env", "staging")

        # Setup http requests by status code for aggregation
        self.client.execute_command("TS.CREATE", "http_200_1", "LABELS", "__name__", "http_requests_total", "instance",
                                    "app1", "status", "200")
        self.client.execute_command("TS.CREATE", "http_500_1", "LABELS", "__name__", "http_requests_total", "instance",
                                    "app1", "status", "500")
        self.client.execute_command("TS.CREATE", "http_200_2", "LABELS", "__name__", "http_requests_total", "instance",
                                    "app2", "status", "200")

        # Load some data (timestamp 1000, 2000, 3000, 4000)
        for t in [1000, 2000, 3000, 4000]:
            # Memory data
            self.client.execute_command("TS.ADD", "mem_active_1", t, 500 + t / 100)
            self.client.execute_command("TS.ADD", "mem_total_1", t, 1000)

            self.client.execute_command("TS.ADD", "mem_active_2", t, 400 + t / 100)
            self.client.execute_command("TS.ADD", "mem_total_2", t, 2000)

            # HTTP data (cumulative counter)
            # App 1
            self.client.execute_command("TS.ADD", "http_200_1", t, t / 10)
            self.client.execute_command("TS.ADD", "http_500_1", t, t / 100)

            # App 2
            self.client.execute_command("TS.ADD", "http_200_2", t, t / 20)

    def range_query(self, query: str, step: str, start: int | str | None = None, end: int | str | None = None):
        args = ["TS.QUERYRANGE", query, "STEP", str(step)]
        if start is not None:
            args.extend(["START", str(start)])
        if end is not None:
            args.extend(["END", str(end)])
        return self.client.execute_command(*args)

    def test_queryrange_returns_matrix_for_metric(self):
        self.setup_simple_series()

        raw = self.range_query("http_requests", "1000", start=1000, end=5000)
        qr = QueryResult.from_raw(raw)

        assert qr.is_matrix(), "expected matrix result type"
        assert len(qr.result) == 1
        assert qr.result[0].name == "http_requests"
        assert len(qr.result[0].values) >= 1

    def test_queryrange_with_unix_timestamps(self):
        self.setup_simple_series()

        raw = self.range_query("http_requests", "1000", start=2, end=5)
        qr = QueryResult.from_raw(raw)

        assert qr.is_matrix(), "expected matrix result type"

    def test_queryrange_with_rfc3339_timestamps(self):
        self.setup_simple_series()

        raw = self.range_query(
            "http_requests",
            "1000",
            start="1970-01-01T00:00:02Z",
            end="1970-01-01T00:00:05Z",
        )
        qr = QueryResult.from_raw(raw)

        assert qr.is_matrix(), "expected matrix result type"

    def test_queryrange_with_relative_start_and_now_end(self):
        self.setup_realtime_series()

        raw = self.range_query("live_requests", "1000", start="-10s", end="*")
        qr = QueryResult.from_raw(raw)

        assert qr.is_matrix(), "expected matrix result type"
        assert len(qr.result) == 1

    def test_queryrange_missing_step_argument(self):
        self.setup_simple_series()

        with pytest.raises(ResponseError):
            self.client.execute_command("TS.QUERYRANGE", "http_requests")

    def test_queryrange_invalid_argument(self):
        self.setup_simple_series()

        with pytest.raises(ResponseError):
            self.client.execute_command("TS.QUERYRANGE", "http_requests", "STEP", 1000, "TIME", 3)

    def test_queryrange_invalid_start_timestamp(self):
        self.setup_simple_series()

        with pytest.raises(ResponseError):
            self.range_query("http_requests", "1000", start="not-a-time", end=5)

    def test_queryrange_rejects_start_greater_or_equal_end(self):
        self.setup_simple_series()

        with pytest.raises(ResponseError):
            self.range_query("http_requests", "1000", start=4000, end=2000)

    def test_queryrange_scalar_expression_returns_matrix(self):
        self.setup_simple_series()

        raw = self.range_query("42", "1000", start=1, end=5)
        qr = QueryResult.from_raw(raw)

        assert qr.is_matrix(), "range query should return matrix result type"
        assert len(qr.result) == 1
        assert len(qr.result[0].values) >= 1

    def test_queryrange_arithmetic(self):
        """Test arithmetic operations on metrics (e.g. memory usage percentage)"""
        self.setup_observability_metrics()

        # Query memory active percentage
        # Will match by instance and env implicitly if labels match exactly,
        # or we explicitly do "ignoring" or just let it map 1:1 since the label sets are distinct other than metric name.
        # Let's test a simple sum by instance
        raw = self.range_query(
            "sum by (instance) (node_memory_Active_bytes) / sum by (instance) (node_memory_MemTotal_bytes)",
            "1000",
            start=1000,
            end=4000
        )
        qr = QueryResult.from_raw(raw)

        assert qr.is_matrix()
        assert len(qr.result) == 2  # node1, node2

        # For node1, the ratio should be (500+t/100)/1000 => 0.51, 0.52 etc
        for series in qr.result:
            if series.metric.get("instance") == "node1":
                assert series.values[0].value == 0.51
            if series.metric.get("instance") == "node2":
                assert series.values[0].value == 0.205  # (400+10)/2000

    def test_queryrange_rate(self):
        """Test rate calculation on counter metrics"""
        self.setup_observability_metrics()

        # Rate over 2 seconds (using 2000ms duration since timestamps are in ms)
        # PromQL query delta should compute correctly
        raw = self.range_query(
            "rate(http_requests_total[2000s])",
            "1000",
            start=2000,
            end=4000
        )
        qr = QueryResult.from_raw(raw)
        assert qr.is_matrix()
        assert len(qr.result) == 3  # http_200_1, http_500_1, http_200_2

    def test_queryrange_aggregation(self):
        """Test sum by aggregation grouping"""
        self.setup_observability_metrics()

        raw = self.range_query(
            "sum by (status) (http_requests_total)",
            "1000",
            start=1000,
            end=4000
        )
        qr = QueryResult.from_raw(raw)

        assert qr.is_matrix()
        assert len(qr.result) == 2

        # Check that proper grouping occurred
        status_200 = next(r for r in qr.result if r.metric.get("status") == "200")
        status_500 = next(r for r in qr.result if r.metric.get("status") == "500")

        # values[0] is at timestamp 1000
        # 200 is app1 (1000/10=100) + app2 (1000/20=50) = 150
        assert status_200.values[0].value == 150.0

        # 500 is app1 (1000/100=10)
        assert status_500.values[0].value == 10.0

    def test_queryrange_filtering(self):
        """Test label filtering and matching"""
        self.setup_observability_metrics()

        raw = self.range_query(
            'http_requests_total{status="500", instance="app1"}',
            "1000",
            start=1000,
            end=4000
        )
        qr = QueryResult.from_raw(raw)

        assert qr.is_matrix()
        assert len(qr.result) == 1
        assert qr.result[0].metric.get("instance") == "app1"
        assert qr.result[0].metric.get("status") == "500"

        assert len(qr.result[0].values) == 4
