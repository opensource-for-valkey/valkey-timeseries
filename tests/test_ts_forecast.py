"""
Integration tests for TS.FORECAST command.

Covers:
- Basic forecasting with a single model
- Forecasting with multiple models
- HORIZON option
- WITH_METRICS option
- LEVEL option for prediction intervals
- STORE option (persisting forecast to a destination key)
  - Creates new key, returns count not array
  - Overwrites existing key
  - MERGE with existing key
  - With RETENTION, CHUNK_SIZE, DUPLICATE_POLICY, ENCODING, METRIC
  - Multiple STORE options combined
  - Timestamps are sequential
- Time-range filtering
- Error handling: nonexistent key, missing HORIZON, missing MODELS,
  invalid model spec, wrong arity, unknown argument, LEVEL out of range,
  insufficient data
- Response format validation (array of flat key-value maps, one per model)
- Edge cases: different series types, all options combined
"""

import math
from typing import Any, Dict, List

import pytest
from valkey import ResponseError

from valkey_timeseries_test_case import ValkeyTimeSeriesTestCaseBase
from valkeytestframework.conftest import resource_port_tracker
from valkeytestframework.util.waiters import *
from data_helpers import (
    _add,
    create_daily_seasonal_series,
    create_linear_series,
    create_sine_series,
    create_constant_series,
)


def parse_forecast_response(response: List[Any]) -> Dict[str, Any]:
    """Parse the flat key-value list response from a single forecast entry into a dict.

    Each entry in the TS.FORECAST response array is a flat list of alternating
    key-value pairs:
    [model, "ARIMA(2,1,0)", horizon, "5", forecast, [...], ...]

    All values are decoded to their native Python types:
    - forecast, lower_interval, upper_interval → list of float
    - horizon → int
    - level → float
    - metrics → dict of {str: float|None}
    - model → str
    """
    if not response:
        return {}

    result: Dict[str, Any] = {}
    it = iter(response)
    for key in it:
        key_str = key.decode("utf-8") if isinstance(key, bytes) else str(key)
        value = next(it)

        if key_str in ("forecast", "lower_interval", "upper_interval"):
            result[key_str] = [float(v) for v in value]
        elif key_str == "horizon":
            result[key_str] = int(value)
        elif key_str == "level":
            result[key_str] = float(value)
        elif key_str == "metrics":
            metrics_dict = {}
            m_it = iter(value)
            for m_key in m_it:
                m_key_str = m_key.decode("utf-8") if isinstance(m_key, bytes) else str(m_key)
                m_val = next(m_it)
                metrics_dict[m_key_str] = None if m_val is None else float(m_val)
            result[key_str] = metrics_dict
        else:
            result[key_str] = value.decode("utf-8") if isinstance(value, bytes) else str(value)

    assert "model" in result, f"Missing model in {result}"
    assert "horizon" in result, f"Missing horizon in {result}"
    assert "forecast" in result, f"Missing forecast in {result}"

    return result


def parse_forecast_array_response(response: List[Any]) -> List[Dict[str, Any]]:
    """Parse the TS.FORECAST response (array of flat key-value maps).

    TS.FORECAST returns an array where each element is a flat map for one model.
    """
    if not response:
        return []
    return [parse_forecast_response(entry) for entry in response]


def get_forecast_values(result: Dict[str, Any]) -> List[float]:
    """Extract forecast values from the parsed response."""
    return result.get("forecast", [])


# ── test class ───────────────────────────────────────────────────────────────

class TestForecast(ValkeyTimeSeriesTestCaseBase):
    """Test TS.FORECAST command with various options and edge cases."""

    # ══════════════════════════════════════════════════════════════════════
    # Basic functionality
    # ══════════════════════════════════════════════════════════════════════

    def test_basic_forecast_single_model(self):
        """Test basic forecast with a single ARIMA model."""
        key = "test:forecast:basic"
        create_linear_series(self.client, key, count=150)

        result = self.client.execute_command(
            "TS.FORECAST", key, "-", "+",
            "MODELS", "ARIMA(2,1,0)", "HORIZON", "5"
        )

        parsed_list = parse_forecast_array_response(result)
        assert len(parsed_list) == 1, f"Expected 1 model result, got {len(parsed_list)}"

        parsed = parsed_list[0]
        assert "model" in parsed
        assert int(parsed["horizon"]) == 5
        forecasts = get_forecast_values(parsed)
        assert len(forecasts) == 5
        for v in forecasts:
            assert not math.isnan(v)
            assert not math.isinf(v)

    def test_forecast_horizon_one(self):
        """Test forecast with HORIZON 1 (single point prediction)."""
        key = "test:forecast:horizon1"
        create_linear_series(self.client, key, count=100)

        result = self.client.execute_command(
            "TS.FORECAST", key, "-", "+",
            "MODELS", "ARIMA(2,1,0)", "HORIZON", "1"
        )

        parsed_list = parse_forecast_array_response(result)
        assert len(parsed_list) == 1
        forecasts = get_forecast_values(parsed_list[0])
        assert len(forecasts) == 1

    def test_forecast_larger_horizon(self):
        """Test forecast with a larger HORIZON value."""
        key = "test:forecast:horizon_large"
        create_linear_series(self.client, key, count=200)

        result = self.client.execute_command(
            "TS.FORECAST", key, "-", "+",
            "MODELS", "ARIMA(2,1,0)", "HORIZON", "20"
        )

        parsed_list = parse_forecast_array_response(result)
        forecasts = get_forecast_values(parsed_list[0])
        assert len(forecasts) == 20

    # ══════════════════════════════════════════════════════════════════════
    # Multiple models
    # ══════════════════════════════════════════════════════════════════════

    def test_multiple_models(self):
        """Test forecast with multiple model specifications."""
        key = "test:forecast:multi"
        create_linear_series(self.client, key, count=150)

        result = self.client.execute_command(
            "TS.FORECAST", key, "-", "+",
            "MODELS",
            "ARIMA(2,1,0), SES(alpha=0.3), Theta(), Naive()",
            "HORIZON", "5"
        )

        parsed_list = parse_forecast_array_response(result)
        assert len(parsed_list) == 4, \
            f"Expected 4 model results, got {len(parsed_list)}"

        model_names = {p["model"] for p in parsed_list}
        assert "ARIMA(2,1,0)" in model_names
        assert "SES(alpha=0.3)" in model_names
        assert "Theta()" in model_names
        assert "Naive()" in model_names

        for parsed in parsed_list:
            forecasts = get_forecast_values(parsed)
            assert len(forecasts) == 5

    def test_multiple_models_different_forecasts(self):
        """Test that different models produce different forecast values."""
        key = "test:forecast:multi:diff"
        create_linear_series(self.client, key, count=150)

        result = self.client.execute_command(
            "TS.FORECAST", key, "-", "+",
            "MODELS", "Naive(), SES(alpha=0.9), ARIMA(2,1,0)",
            "HORIZON", "5"
        )

        parsed_list = parse_forecast_array_response(result)
        assert len(parsed_list) == 3

        # Gather forecast values from each model
        all_forecasts = []
        for parsed in parsed_list:
            fv = get_forecast_values(parsed)
            all_forecasts.append(tuple(fv))

        # At least one pair should differ (different models = different forecasts)
        unique = set(all_forecasts)
        assert len(unique) >= 2, \
            f"Expected at least 2 distinct forecast sets, got {len(unique)}"

    # ══════════════════════════════════════════════════════════════════════
    # LEVEL option (prediction intervals)
    # ══════════════════════════════════════════════════════════════════════

    def test_forecast_with_level(self):
        """Test forecast with LEVEL for prediction intervals."""
        key = "test:forecast:level"
        create_linear_series(self.client, key, count=150)

        result = self.client.execute_command(
            "TS.FORECAST", key, "-", "+",
            "MODELS", "ARIMA(2,1,0)", "HORIZON", "3", "LEVEL", "95"
        )

        parsed_list = parse_forecast_array_response(result)
        assert len(parsed_list) == 1
        parsed = parsed_list[0]

        assert "level" in parsed
        assert float(parsed["level"]) == 95.0
        assert "lower_interval" in parsed
        assert "upper_interval" in parsed

        forecasts = get_forecast_values(parsed)
        assert len(forecasts) == 3

        lower = parsed["lower_interval"]
        upper = parsed["upper_interval"]
        assert len(lower) == 3
        assert len(upper) == 3

        # lower <= forecast <= upper for each point
        for i, (lo, f, hi) in enumerate(zip(lower, forecasts, upper)):
            assert lo <= f <= hi, \
                f"At index {i}: lower={lo} forecast={f} upper={hi} violates lo <= f <= hi"

    def test_level_multiple_models(self):
        """Test LEVEL with multiple models — each gets intervals."""
        key = "test:forecast:level:multi"
        create_linear_series(self.client, key, count=150)

        result = self.client.execute_command(
            "TS.FORECAST", key, "-", "+",
            "MODELS", "ARIMA(2,1,0), SES(alpha=0.3)",
            "HORIZON", "3", "LEVEL", "90"
        )

        parsed_list = parse_forecast_array_response(result)
        assert len(parsed_list) == 2
        for parsed in parsed_list:
            assert "level" in parsed
            assert float(parsed["level"]) == 90.0
            assert "lower_interval" in parsed
            assert "upper_interval" in parsed

    def test_level_boundary_95(self):
        """Test LEVEL with value 95 (high confidence)."""
        key = "test:forecast:level:95"
        create_linear_series(self.client, key, count=150)

        result = self.client.execute_command(
            "TS.FORECAST", key, "-", "+",
            "MODELS", "ARIMA(2,1,0)", "HORIZON", "3", "LEVEL", "95"
        )

        parsed_list = parse_forecast_array_response(result)
        parsed = parsed_list[0]
        assert "level" in parsed
        assert "lower_interval" in parsed
        assert "upper_interval" in parsed

    # ══════════════════════════════════════════════════════════════════════
    # WITH_METRICS option
    # ══════════════════════════════════════════════════════════════════════

    def test_forecast_with_metrics(self):
        """Test forecast with WITH_METRICS for in-sample accuracy metrics."""
        key = "test:forecast:metrics"
        create_linear_series(self.client, key, count=150)

        result = self.client.execute_command(
            "TS.FORECAST", key, "-", "+",
            "MODELS", "ARIMA(2,1,0)", "HORIZON", "5", "WITH_METRICS"
        )

        parsed_list = parse_forecast_array_response(result)
        assert len(parsed_list) == 1
        parsed = parsed_list[0]

        assert "metrics" in parsed, \
            f"Expected metrics in response, got keys: {list(parsed.keys())}"

        metrics = parsed["metrics"]
        required = ["mae", "mse", "rmse", "mape", "smape", "mase", "r_squared"]
        for field in required:
            assert field in metrics, \
                f"Missing metrics field '{field}', got: {list(metrics.keys())}"

        assert metrics["mae"] >= 0.0
        assert metrics["mse"] >= 0.0
        assert metrics["rmse"] >= 0.0
        assert metrics["smape"] >= 0.0

    def test_without_metrics_omits_field(self):
        """Test that metrics field is omitted when WITH_METRICS is not specified."""
        key = "test:forecast:no_metrics"
        create_linear_series(self.client, key, count=120)

        result = self.client.execute_command(
            "TS.FORECAST", key, "-", "+",
            "MODELS", "ARIMA(2,1,0)", "HORIZON", "3"
        )

        parsed_list = parse_forecast_array_response(result)
        assert "metrics" not in parsed_list[0]

    # ══════════════════════════════════════════════════════════════════════
    # STORE option
    # ══════════════════════════════════════════════════════════════════════

    def test_store_creates_new_key(self):
        """Test STORE persists forecast into a new timeseries key and returns count."""
        key = "test:forecast:store:src"
        store_key = "test:forecast:store:dst"
        create_linear_series(self.client, key, count=150)

        count = self.client.execute_command(
            "TS.FORECAST", key, "-", "+",
            "MODELS", "ARIMA(2,1,0)", "HORIZON", "5",
            "STORE", store_key
        )

        # STORE returns the number of samples written
        assert isinstance(count, int), f"Expected int count, got {type(count)}"
        assert count == 5, f"Expected 5 stored samples, got {count}"

        # Verify the destination key exists and has the forecast values
        assert self.client.execute_command("EXISTS", store_key) == 1, \
            f"STORE key '{store_key}' was not created"

        stored = self.client.execute_command("TS.RANGE", store_key, "-", "+")
        assert len(stored) == 5, \
            f"Expected 5 stored samples, got {len(stored)}"

    def test_store_error_with_multiple_models(self):
        """Test STORE with multiple models returns an error."""
        key = "test:forecast:store:multi:src"
        store_key = "test:forecast:store:multi:dst"
        create_linear_series(self.client, key, count=150)

        with pytest.raises(ResponseError, match="STORE is only supported with a single model"):
            self.client.execute_command(
                "TS.FORECAST", key, "-", "+",
                "MODELS", "ARIMA(2,1,0), SES(alpha=0.3), Naive()",
                "HORIZON", "3",
                "STORE", store_key
            )

    def test_store_overwrites_existing_key(self):
        """Test STORE (without MERGE) overwrites an existing destination key."""
        key = "test:forecast:store:overwrite:src"
        store_key = "test:forecast:store:overwrite:dst"
        create_linear_series(self.client, key, count=150)

        # Pre-create dest key with existing data
        self.client.execute_command("TS.CREATE", store_key)
        self.client.execute_command("TS.ADD", store_key, 500, 99.0)
        self.client.execute_command("TS.ADD", store_key, 1500, 88.0)

        count = self.client.execute_command(
            "TS.FORECAST", key, "-", "+",
            "MODELS", "ARIMA(2,1,0)", "HORIZON", "3",
            "STORE", store_key
        )

        assert count == 3

        # Destination is overwritten (only new forecast samples remain)
        stored = self.client.execute_command("TS.RANGE", store_key, "-", "+")
        assert len(stored) == 3

    def test_store_merge_with_existing_key(self):
        """Test STORE with MERGE combines forecast into existing destination key."""
        key = "test:forecast:store:merge:src"
        store_key = "test:forecast:store:merge:dst"
        create_linear_series(self.client, key, count=150)

        # Pre-create dest key with samples
        self.client.execute_command("TS.CREATE", store_key)
        self.client.execute_command("TS.ADD", store_key, 500, 99.0)
        self.client.execute_command("TS.ADD", store_key, 2000, 77.0)

        count = self.client.execute_command(
            "TS.FORECAST", key, "-", "+",
            "MODELS", "ARIMA(2,1,0)", "HORIZON", "3",
            "STORE", store_key, "MERGE"
        )

        assert count == 3

        # Existing samples + new forecast samples should all be present
        stored = self.client.execute_command("TS.RANGE", store_key, "-", "+")
        assert len(stored) == 5, \
            f"Expected 5 samples (2 existing + 3 forecast), got {len(stored)}"

    def test_store_timestamps_are_sequential(self):
        """Test that stored forecast timestamps are monotonically increasing."""
        key = "test:forecast:store:timestamps:src"
        store_key = "test:forecast:store:timestamps:dst"
        create_linear_series(self.client, key, count=100, step_ms=60000)

        count = self.client.execute_command(
            "TS.FORECAST", key, "-", "+",
            "MODELS", "ARIMA(2,1,0)", "HORIZON", "5",
            "STORE", store_key
        )

        assert count == 5

        stored = self.client.execute_command("TS.RANGE", store_key, "-", "+")
        timestamps = [s[0] for s in stored]
        for i in range(1, len(timestamps)):
            assert timestamps[i] > timestamps[i - 1], \
                f"Timestamps not monotonic: {timestamps}"

    def test_store_with_retention(self):
        """Test STORE with RETENTION sets retention on destination key."""
        key = "test:forecast:store:retention:src"
        store_key = "test:forecast:store:retention:dst"
        create_linear_series(self.client, key, count=150)

        self.client.execute_command(
            "TS.FORECAST", key, "-", "+",
            "MODELS", "ARIMA(2,1,0)", "HORIZON", "3",
            "STORE", store_key, "RETENTION", "5000"
        )

        info = self.client.execute_command("TS.INFO", store_key)
        info_dict = {info[i]: info[i + 1] for i in range(0, len(info), 2)}
        assert info_dict[b'retentionTime'] == 5000

    def test_store_with_chunk_size(self):
        """Test STORE with CHUNK_SIZE sets chunk size on destination key."""
        key = "test:forecast:store:chunk:src"
        store_key = "test:forecast:store:chunk:dst"
        create_linear_series(self.client, key, count=150)

        self.client.execute_command(
            "TS.FORECAST", key, "-", "+",
            "MODELS", "ARIMA(2,1,0)", "HORIZON", "3",
            "STORE", store_key, "CHUNK_SIZE", "128"
        )

        info = self.client.execute_command("TS.INFO", store_key)
        info_dict = {info[i]: info[i + 1] for i in range(0, len(info), 2)}
        assert info_dict[b'chunkSize'] == 128

    def test_store_with_duplicate_policy(self):
        """Test STORE with DUPLICATE_POLICY sets policy on destination key."""
        key = "test:forecast:store:dup:src"
        store_key = "test:forecast:store:dup:dst"
        create_linear_series(self.client, key, count=150)

        self.client.execute_command(
            "TS.FORECAST", key, "-", "+",
            "MODELS", "ARIMA(2,1,0)", "HORIZON", "3",
            "STORE", store_key, "DUPLICATE_POLICY", "SUM"
        )

        info = self.client.execute_command("TS.INFO", store_key)
        info_dict = {info[i]: info[i + 1] for i in range(0, len(info), 2)}
        assert info_dict[b'duplicatePolicy'] == b'sum'

    def test_store_with_encoding(self):
        """Test STORE with ENCODING sets compression on destination key."""
        key = "test:forecast:store:encoding:src"
        store_key = "test:forecast:store:encoding:dst"
        create_linear_series(self.client, key, count=150)

        self.client.execute_command(
            "TS.FORECAST", key, "-", "+",
            "MODELS", "ARIMA(2,1,0)", "HORIZON", "3",
            "STORE", store_key, "ENCODING", "UNCOMPRESSED"
        )

        info = self.client.execute_command("TS.INFO", store_key)
        info_dict = {info[i]: info[i + 1] for i in range(0, len(info), 2)}
        assert info_dict[b'chunkType'] == b'uncompressed'

    def test_store_with_metric(self):
        """Test STORE with METRIC sets metric name on destination key."""
        key = "test:forecast:store:metric:src"
        store_key = "test:forecast:store:metric:dst"
        create_linear_series(self.client, key, count=150)

        self.client.execute_command(
            "TS.FORECAST", key, "-", "+",
            "MODELS", "ARIMA(2,1,0)", "HORIZON", "3",
            "STORE", store_key, "METRIC", "forecast_temp"
        )

        stored = self.client.execute_command("TS.RANGE", store_key, "-", "+")
        assert len(stored) == 3

    def test_store_returns_count_not_array(self):
        """Test STORE returns integer count, not an array of forecast results."""
        key = "test:forecast:store:count:src"
        store_key = "test:forecast:store:count:dst"
        create_linear_series(self.client, key, count=150)

        result = self.client.execute_command(
            "TS.FORECAST", key, "-", "+",
            "MODELS", "ARIMA(2,1,0)", "HORIZON", "5",
            "STORE", store_key
        )

        assert isinstance(result, int), \
            f"Expected int count with STORE, got {type(result)}"
        assert result == 5

    def test_store_with_multiple_options_combined(self):
        """Test STORE with multiple creation options applied together."""
        key = "test:forecast:store:combined:src"
        store_key = "test:forecast:store:combined:dst"
        create_linear_series(self.client, key, count=150)

        self.client.execute_command(
            "TS.FORECAST", key, "-", "+",
            "MODELS", "ARIMA(2,1,0)", "HORIZON", "3",
            "STORE", store_key,
            "RETENTION", "10000",
            "CHUNK_SIZE", "256",
            "DUPLICATE_POLICY", "MIN",
            "ENCODING", "UNCOMPRESSED",
        )

        info = self.client.execute_command("TS.INFO", store_key)
        info_dict = {info[i]: info[i + 1] for i in range(0, len(info), 2)}
        assert info_dict[b'retentionTime'] == 10000
        assert info_dict[b'chunkSize'] == 256
        assert info_dict[b'duplicatePolicy'] == b'min'
        assert info_dict[b'chunkType'] == b'uncompressed'

    def test_store_on_nonexistent_source(self):
        """Test STORE on a nonexistent source key returns error."""
        with pytest.raises(ResponseError, match="key does not exist"):
            self.client.execute_command(
                "TS.FORECAST", "nonexistent", "-", "+",
                "MODELS", "ARIMA(2,1,0)", "HORIZON", "5",
                "STORE", "dest1"
            )

    # ══════════════════════════════════════════════════════════════════════
    # Time-range filtering
    # ══════════════════════════════════════════════════════════════════════

    def test_time_range_filter(self):
        """Test forecast using only a subset of the time range."""
        key = "test:forecast:range"
        values = [10.0 + 0.5 * i for i in range(200)]
        _add(self.client, key, 1000, values, 60000)

        # Use the middle portion of the data
        from_ts = 1000 + 50 * 60000
        to_ts = 1000 + 149 * 60000

        result = self.client.execute_command(
            "TS.FORECAST", key,
            str(from_ts), str(to_ts),
            "MODELS", "ARIMA(2,1,0)", "HORIZON", "5"
        )

        parsed_list = parse_forecast_array_response(result)
        forecasts = get_forecast_values(parsed_list[0])
        assert len(forecasts) == 5
        for v in forecasts:
            assert not math.isnan(v)

    def test_time_range_small_window(self):
        """Test forecast with a narrow time window."""
        key = "test:forecast:range:narrow"
        values = [10.0 + 0.5 * i for i in range(200)]
        _add(self.client, key, 1000, values, 60000)

        from_ts = 1000 + 100 * 60000
        to_ts = 1000 + 109 * 60000

        result = self.client.execute_command(
            "TS.FORECAST", key,
            str(from_ts), str(to_ts),
            "MODELS", "ARIMA(2,1,0)", "HORIZON", "3"
        )

        parsed_list = parse_forecast_array_response(result)
        forecasts = get_forecast_values(parsed_list[0])
        assert len(forecasts) == 3

    # ══════════════════════════════════════════════════════════════════════
    # Response format validation
    # ══════════════════════════════════════════════════════════════════════

    def test_response_is_array_of_maps(self):
        """Test that the response is an array of flat key-value maps."""
        key = "test:forecast:format:array"
        create_linear_series(self.client, key, count=100)

        result = self.client.execute_command(
            "TS.FORECAST", key, "-", "+",
            "MODELS", "ARIMA(2,1,0)", "HORIZON", "3"
        )

        # Response should be a list (array) of lists (maps)
        assert isinstance(result, list), \
            f"Expected list response, got {type(result)}"
        assert len(result) >= 1
        for entry in result:
            assert isinstance(entry, list), \
                f"Expected each entry to be a list (map), got {type(entry)}"
            assert len(entry) % 2 == 0, \
                f"Expected even-length flat map, got {len(entry)} elements"

    def test_response_required_fields(self):
        """Test that each model result has required fields."""
        key = "test:forecast:format:fields"
        create_linear_series(self.client, key, count=100)

        result = self.client.execute_command(
            "TS.FORECAST", key, "-", "+",
            "MODELS", "ARIMA(2,1,0), SES(alpha=0.3)",
            "HORIZON", "3"
        )

        parsed_list = parse_forecast_array_response(result)
        for parsed in parsed_list:
            assert "model" in parsed
            assert "horizon" in parsed
            assert "forecast" in parsed
            assert isinstance(parsed["model"], str)
            horizon = parsed["horizon"]
            assert int(horizon) > 0
            forecast = get_forecast_values(parsed)
            assert isinstance(forecast, list)
            assert len(forecast) > 0

    def test_response_with_level_has_intervals(self):
        """Test that LEVEL adds interval fields but omits them without LEVEL."""
        key = "test:forecast:format:level"
        create_linear_series(self.client, key, count=120)

        # Without LEVEL
        result_no = self.client.execute_command(
            "TS.FORECAST", key, "-", "+",
            "MODELS", "ARIMA(2,1,0)", "HORIZON", "3"
        )
        parsed_no = parse_forecast_array_response(result_no)[0]
        assert "level" not in parsed_no
        assert "lower_interval" not in parsed_no
        assert "upper_interval" not in parsed_no

        # With LEVEL
        result_with = self.client.execute_command(
            "TS.FORECAST", key, "-", "+",
            "MODELS", "ARIMA(2,1,0)", "HORIZON", "3", "LEVEL", "95"
        )
        parsed_with = parse_forecast_array_response(result_with)[0]
        assert "level" in parsed_with
        assert "lower_interval" in parsed_with
        assert "upper_interval" in parsed_with

    # ══════════════════════════════════════════════════════════════════════
    # Error handling
    # ══════════════════════════════════════════════════════════════════════

    def test_error_nonexistent_key(self):
        """Test error on non-existent key."""
        with pytest.raises(ResponseError, match="key does not exist"):
            self.client.execute_command(
                "TS.FORECAST", "nonexistent_key", "-", "+",
                "MODELS", "ARIMA(2,1,0)", "HORIZON", "5"
            )

    def test_error_missing_horizon(self):
        """Test that HORIZON is required."""
        key = "test:forecast:err:no_horizon"
        create_linear_series(self.client, key, count=50)

        with pytest.raises(ResponseError):
            self.client.execute_command(
                "TS.FORECAST", key, "-", "+",
                "MODELS", "ARIMA(2,1,0)"
            )

    def test_error_missing_models(self):
        """Test that MODELS is required."""
        key = "test:forecast:err:no_models"
        create_linear_series(self.client, key, count=50)

        with pytest.raises(ResponseError):
            self.client.execute_command(
                "TS.FORECAST", key, "-", "+",
                "HORIZON", "5"
            )

    def test_error_invalid_model_spec(self):
        """Test error with an invalid model specification."""
        key = "test:forecast:err:bad_model"
        create_linear_series(self.client, key, count=100)

        with pytest.raises(ResponseError, match="MODELS"):
            self.client.execute_command(
                "TS.FORECAST", key, "-", "+",
                "MODELS", "INVALID_MODEL_NAME", "HORIZON", "5"
            )

    def test_error_insufficient_data(self):
        """Test error with very few data points."""
        key = "test:forecast:err:few"
        self.client.execute_command("TS.CREATE", key)
        self.client.execute_command("TS.ADD", key, 1000, 1.0)
        self.client.execute_command("TS.ADD", key, 2000, 2.0)

        with pytest.raises(ResponseError):
            self.client.execute_command(
                "TS.FORECAST", key, "-", "+",
                "MODELS", "ARIMA(2,1,0)", "HORIZON", "5"
            )

    def test_error_wrong_arity(self):
        """Test error with too few arguments."""
        with pytest.raises(ResponseError):
            self.client.execute_command("TS.FORECAST", "key")

    def test_error_unknown_argument(self):
        """Test error with an unknown argument."""
        key = "test:forecast:err:unknown"
        create_linear_series(self.client, key, count=100)

        with pytest.raises(ResponseError, match="Unknown argument"):
            self.client.execute_command(
                "TS.FORECAST", key, "-", "+",
                "MODELS", "ARIMA(2,1,0)", "HORIZON", "5",
                "INVALID_OPTION", "value"
            )

    def test_error_level_zero(self):
        """Test error when LEVEL is 0 (must be > 0)."""
        key = "test:forecast:err:level0"
        create_linear_series(self.client, key, count=100)

        with pytest.raises(ResponseError, match="LEVEL must be between 0 and 100"):
            self.client.execute_command(
                "TS.FORECAST", key, "-", "+",
                "MODELS", "ARIMA(2,1,0)", "HORIZON", "5",
                "LEVEL", "0"
            )

    def test_error_level_100(self):
        """Test error when LEVEL is 100 (must be < 100)."""
        key = "test:forecast:err:level100"
        create_linear_series(self.client, key, count=100)

        with pytest.raises(ResponseError, match="LEVEL must be between 0 and 100"):
            self.client.execute_command(
                "TS.FORECAST", key, "-", "+",
                "MODELS", "ARIMA(2,1,0)", "HORIZON", "5",
                "LEVEL", "100"
            )

    def test_error_horizon_zero(self):
        """Test error when HORIZON is 0 (must be > 0)."""
        key = "test:forecast:err:horizon0"
        create_linear_series(self.client, key, count=100)

        with pytest.raises(ResponseError, match="horizon must be greater than 0"):
            self.client.execute_command(
                "TS.FORECAST", key, "-", "+",
                "MODELS", "ARIMA(2,1,0)", "HORIZON", "0"
            )

    def test_error_missing_store_value(self):
        """Test error when STORE has no key value."""
        key = "test:forecast:err:store_missing"
        create_linear_series(self.client, key, count=100)

        with pytest.raises(ResponseError):
            self.client.execute_command(
                "TS.FORECAST", key, "-", "+",
                "MODELS", "ARIMA(2,1,0)", "HORIZON", "5",
                "STORE"
            )

    # ══════════════════════════════════════════════════════════════════════
    # Edge cases — different series types
    # ══════════════════════════════════════════════════════════════════════

    def test_forecast_sine_series(self):
        """Test forecast on a sine wave series."""
        key = "test:forecast:sine"
        create_sine_series(self.client, key, count=200)

        result = self.client.execute_command(
            "TS.FORECAST", key, "-", "+",
            "MODELS", "ARIMA(2,1,0)", "HORIZON", "5"
        )

        parsed_list = parse_forecast_array_response(result)
        forecasts = get_forecast_values(parsed_list[0])
        assert len(forecasts) == 5
        for v in forecasts:
            assert not math.isnan(v)
            assert not math.isinf(v)

    def test_forecast_constant_series(self):
        """Test forecast on a constant series."""
        key = "test:forecast:constant"
        create_constant_series(self.client, key, count=100, value=10.0)

        result = self.client.execute_command(
            "TS.FORECAST", key, "-", "+",
            "MODELS", "ARIMA(2,1,0)", "HORIZON", "5"
        )

        parsed_list = parse_forecast_array_response(result)
        forecasts = get_forecast_values(parsed_list[0])
        assert len(forecasts) == 5
        for v in forecasts:
            assert not math.isnan(v)
            assert not math.isinf(v)

    def test_forecast_daily_seasonal_series(self):
        """Test forecast on a daily seasonal series."""
        key = "test:forecast:daily"
        create_daily_seasonal_series(self.client, key, days=21)

        result = self.client.execute_command(
            "TS.FORECAST", key, "-", "+",
            "MODELS", "ARIMA(2,1,0)", "HORIZON", "5"
        )

        parsed_list = parse_forecast_array_response(result)
        forecasts = get_forecast_values(parsed_list[0])
        assert len(forecasts) == 5
        for v in forecasts:
            assert not math.isnan(v)
            assert not math.isinf(v)

    def test_forecast_random_walk(self):
        """Test forecast on a random-walk series."""
        key = "test:forecast:random_walk"
        import random
        random.seed(42)
        values = [100.0]
        for i in range(199):
            values.append(values[-1] + random.gauss(0, 2))
        _add(self.client, key, 1000, values, 60000)

        result = self.client.execute_command(
            "TS.FORECAST", key, "-", "+",
            "MODELS", "ARIMA(2,1,0)", "HORIZON", "5"
        )

        parsed_list = parse_forecast_array_response(result)
        forecasts = get_forecast_values(parsed_list[0])
        assert len(forecasts) == 5
        for v in forecasts:
            assert not math.isnan(v)
            assert not math.isinf(v)

    # ══════════════════════════════════════════════════════════════════════
    # Edge cases — combined options
    # ══════════════════════════════════════════════════════════════════════

    def test_all_options_combined(self):
        """Test with MODELS, HORIZON, LEVEL, WITH_METRICS, and STORE together."""
        key = "test:forecast:combined:src"
        store_key = "test:forecast:combined:dst"
        create_linear_series(self.client, key, count=150)

        count = self.client.execute_command(
            "TS.FORECAST", key, "-", "+",
            "MODELS", "ARIMA(2,1,0)",
            "HORIZON", "5",
            "LEVEL", "90",
            "WITH_METRICS",
            "STORE", store_key,
            "RETENTION", "10000",
            "CHUNK_SIZE", "128",
        )

        assert isinstance(count, int)
        assert count == 5

        # Verify stored key
        assert self.client.execute_command("EXISTS", store_key) == 1
        stored = self.client.execute_command("TS.RANGE", store_key, "-", "+")
        assert len(stored) == 5

    def test_all_options_without_store(self):
        """Test with MODELS, HORIZON, LEVEL, and WITH_METRICS (no STORE)."""
        key = "test:forecast:combined:no_store"
        create_linear_series(self.client, key, count=150)

        result = self.client.execute_command(
            "TS.FORECAST", key, "-", "+",
            "MODELS", "ARIMA(2,1,0), SES(alpha=0.3)",
            "HORIZON", "5",
            "LEVEL", "90",
            "WITH_METRICS",
        )

        parsed_list = parse_forecast_array_response(result)
        assert len(parsed_list) == 2

        for parsed in parsed_list:
            assert "model" in parsed
            assert int(parsed["horizon"]) == 5
            assert "level" in parsed
            assert "lower_interval" in parsed
            assert "upper_interval" in parsed
            assert "metrics" in parsed
            forecasts = get_forecast_values(parsed)
            assert len(forecasts) == 5

    # ══════════════════════════════════════════════════════════════════════
    # Variety of model specs
    # ══════════════════════════════════════════════════════════════════════

    def test_model_ses(self):
        """Test Simple Exponential Smoothing model."""
        key = "test:forecast:model:ses"
        create_linear_series(self.client, key, count=100)

        result = self.client.execute_command(
            "TS.FORECAST", key, "-", "+",
            "MODELS", "SES(alpha=0.5)", "HORIZON", "5"
        )

        parsed_list = parse_forecast_array_response(result)
        parsed = parsed_list[0]
        assert "SES" in parsed["model"]
        forecasts = get_forecast_values(parsed)
        assert len(forecasts) == 5

    def test_model_holt(self):
        """Test Holt linear trend model."""
        key = "test:forecast:model:holt"
        create_linear_series(self.client, key, count=100)

        result = self.client.execute_command(
            "TS.FORECAST", key, "-", "+",
            "MODELS", "Holt()", "HORIZON", "5"
        )

        parsed_list = parse_forecast_array_response(result)
        parsed = parsed_list[0]
        assert "Holt" in parsed["model"]
        forecasts = get_forecast_values(parsed)
        assert len(forecasts) == 5

    def test_model_naive(self):
        """Test Naive (random walk drift) model."""
        key = "test:forecast:model:naive"
        create_linear_series(self.client, key, count=100)

        result = self.client.execute_command(
            "TS.FORECAST", key, "-", "+",
            "MODELS", "Naive()", "HORIZON", "5"
        )

        parsed_list = parse_forecast_array_response(result)
        parsed = parsed_list[0]
        assert parsed["model"] == "Naive()"
        forecasts = get_forecast_values(parsed)
        assert len(forecasts) == 5

    def test_model_theta(self):
        """Test Theta model."""
        key = "test:forecast:model:theta"
        create_linear_series(self.client, key, count=100)

        result = self.client.execute_command(
            "TS.FORECAST", key, "-", "+",
            "MODELS", "Theta()", "HORIZON", "5"
        )

        parsed_list = parse_forecast_array_response(result)
        parsed = parsed_list[0]
        assert "Theta" in parsed["model"]
        forecasts = get_forecast_values(parsed)
        assert len(forecasts) == 5

    def test_model_sma(self):
        """Test Simple Moving Average model."""
        key = "test:forecast:model:sma"
        create_linear_series(self.client, key, count=100)

        result = self.client.execute_command(
            "TS.FORECAST", key, "-", "+",
            "MODELS", "SMA(5)", "HORIZON", "5"
        )

        parsed_list = parse_forecast_array_response(result)
        parsed = parsed_list[0]
        assert "SMA" in parsed["model"]
        forecasts = get_forecast_values(parsed)
        assert len(forecasts) == 5

    def test_model_case_insensitive(self):
        """Test that model spec names are case-insensitive."""
        key = "test:forecast:model:case"
        create_linear_series(self.client, key, count=100)

        result = self.client.execute_command(
            "TS.FORECAST", key, "-", "+",
            "MODELS", "arima(2,1,0), naive()", "HORIZON", "5"
        )

        parsed_list = parse_forecast_array_response(result)
        assert len(parsed_list) == 2
        model_names = {p["model"] for p in parsed_list}
        assert "ARIMA(2,1,0)" in model_names
        assert "Naive()" in model_names

    # ══════════════════════════════════════════════════════════════════════
    # STORE + LEVEL together
    # ══════════════════════════════════════════════════════════════════════

    def test_store_with_level(self):
        """Test STORE with LEVEL — count reflects forecast values only."""
        key = "test:forecast:store:level:src"
        store_key = "test:forecast:store:level:dst"
        create_linear_series(self.client, key, count=150)

        count = self.client.execute_command(
            "TS.FORECAST", key, "-", "+",
            "MODELS", "ARIMA(2,1,0)", "HORIZON", "4",
            "LEVEL", "80", "STORE", store_key
        )

        assert isinstance(count, int)
        assert count == 4

        assert self.client.execute_command("EXISTS", store_key) == 1
        stored = self.client.execute_command("TS.RANGE", store_key, "-", "+")
        assert len(stored) == 4
