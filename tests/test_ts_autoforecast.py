"""
Integration tests for TS.AUTOFORECAST command.

Covers:
- Basic forecasting with minimal arguments
- LEVEL option for prediction intervals
- METRICS option for in-sample metrics
- SEASONALITY option
- MODELS option (individual model families and combinations)
- STORE option (persisting forecast to a key)
- Time-range filtering
- Error handling: nonexistent key, missing HORIZON, invalid model,
  invalid LEVEL, insufficient data, unknown argument
- Response format validation (map structure with model,
  horizon, forecast, and optional level/lower_interval/upper_interval)
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
)


def parse_forecast_response(response: List[Any]) -> Dict[str, Any]:
    """Parse the flat key-value list response from TS.AUTOFORECAST into a dict.

    The response is a flat list of alternating key-value pairs:
    [model, "ARIMA", horizon, "5", forecast, [...], ...]

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
            # Preserved as list from the backend array, each element converted to float
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


def get_forecast_values(result: Dict[str, Any]) -> List[float]:
    """Extract forecast values from the parsed response."""
    return result.get("forecast", [])


# ── test class ───────────────────────────────────────────────────────────────

class TestAutoForecast(ValkeyTimeSeriesTestCaseBase):
    """Test TS.AUTOFORECAST command with various options and edge cases."""

    # ── basic functionality ──────────────────────────────────────────────

    def test_basic_forecast(self):
        """Test basic autoforecast with minimal arguments (key, range, HORIZON)."""
        key = "test:autoforecast:basic"
        create_linear_series(self.client, key, count=150)

        result = self.client.execute_command(
            "TS.AUTOFORECAST", key, "-", "+", "HORIZON", "5"
        )

        parsed = parse_forecast_response(result)
        horizon = int(parsed["horizon"])
        assert horizon == 5, f"Expected horizon=5, got {horizon}"
        assert "forecast" in parsed, f"Missing forecast in {parsed}"
        forecasts = get_forecast_values(parsed)
        assert len(forecasts) == 5, f"Expected 5 forecast values, got {len(forecasts)}"
        # The selected model should be one of the known families
        model = parsed["model"]
        assert model in ("ARIMA", "SARIMA", "ETS", "Theta", "unknown"), \
            f"Unexpected model name: {model}"

    def test_forecast_with_level(self):
        """Test forecast with LEVEL for prediction intervals."""
        key = "test:autoforecast:level"
        create_linear_series(self.client, key, count=150)

        result = self.client.execute_command(
            "TS.AUTOFORECAST", key, "-", "+", "HORIZON", "3", "LEVEL", "95"
        )

        parsed = parse_forecast_response(result)
        assert "model" in parsed
        assert "horizon" in parsed
        assert "forecast" in parsed
        assert "level" in parsed
        assert float(parsed["level"]) == 95.0
        assert "lower_interval" in parsed, \
            f"Expected lower_interval when LEVEL specified, got {list(parsed.keys())}"
        assert "upper_interval" in parsed, \
            f"Expected upper_interval when LEVEL specified, got {list(parsed.keys())}"

        forecasts = get_forecast_values(parsed)
        assert len(forecasts) == 3

        # Intervals should have the same length as forecast
        lower = parsed["lower_interval"]
        upper = parsed["upper_interval"]
        assert len(lower) == 3, f"Expected 3 lower values, got {len(lower)}"
        assert len(upper) == 3, f"Expected 3 upper values, got {len(upper)}"

        # lower <= forecast <= upper for each point
        for i, (lo, f, hi) in enumerate(
            zip(lower, forecasts, upper)
        ):
            assert lo <= f <= hi, \
                f"At index {i}: lower={lo} forecast={f} upper={hi} violates lo <= f <= hi"

    def test_forecast_with_metrics(self):
        """Test forecast with METRICS option for in-sample accuracy metrics."""
        key = "test:autoforecast:metrics"
        create_linear_series(self.client, key, count=150)

        result = self.client.execute_command(
            "TS.AUTOFORECAST", key, "-", "+", "HORIZON", "5", "METRICS"
        )

        parsed = parse_forecast_response(result)
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


    def test_forecast_with_horizon_one(self):
        """Test forecast with HORIZON 1 (single point prediction)."""
        key = "test:autoforecast:horizon1"
        create_linear_series(self.client, key, count=100)

        result = self.client.execute_command(
            "TS.AUTOFORECAST", key, "-", "+", "HORIZON", "1"
        )

        parsed = parse_forecast_response(result)
        assert int(parsed["horizon"]) == 1
        forecasts = get_forecast_values(parsed)
        assert len(forecasts) == 1

    def test_forecast_with_larger_horizon(self):
        """Test forecast with a larger HORIZON value."""
        key = "test:autoforecast:horizon_large"
        create_linear_series(self.client, key, count=200)

        result = self.client.execute_command(
            "TS.AUTOFORECAST", key, "-", "+", "HORIZON", "20"
        )

        parsed = parse_forecast_response(result)
        assert int(parsed["horizon"]) == 20
        forecasts = get_forecast_values(parsed)
        assert len(forecasts) == 20

    # ── MODELS option ────────────────────────────────────────────────────

    def test_models_single_arima(self):
        """Test with only ARIMA model selected."""
        key = "test:autoforecast:models:arima"
        create_linear_series(self.client, key, count=150)

        result = self.client.execute_command(
            "TS.AUTOFORECAST", key, "-", "+",
            "HORIZON", "5", "MODELS", "ARIMA"
        )

        parsed = parse_forecast_response(result)
        model = parsed["model"]
        assert model == "ARIMA", f"Expected AutoARIMA, got {model}"
        forecasts = get_forecast_values(parsed)
        assert len(forecasts) == 5

    def test_models_single_ets(self):
        """Test with only ETS model selected."""
        key = "test:autoforecast:models:ets"
        create_linear_series(self.client, key, count=150)

        result = self.client.execute_command(
            "TS.AUTOFORECAST", key, "-", "+",
            "HORIZON", "5", "MODELS", "ETS"
        )

        parsed = parse_forecast_response(result)
        model = parsed["model"]
        assert model == "ETS", f"Expected ETS, got {model}"

    def test_models_single_theta(self):
        """Test with only Theta model selected."""
        key = "test:autoforecast:models:theta"
        create_linear_series(self.client, key, count=150)

        result = self.client.execute_command(
            "TS.AUTOFORECAST", key, "-", "+",
            "HORIZON", "5", "MODELS", "THETA"
        )

        parsed = parse_forecast_response(result)
        model = parsed["model"]
        assert model == "Theta", f"Expected Theta, got {model}"

    def test_models_multiple(self):
        """Test with multiple model families specified."""
        key = "test:autoforecast:models:multi"
        create_linear_series(self.client, key, count=150)

        result = self.client.execute_command(
            "TS.AUTOFORECAST", key, "-", "+",
            "HORIZON", "5", "MODELS", "ARIMA,ETS"
        )

        parsed = parse_forecast_response(result)
        model = parsed["model"]
        assert model in ("ARIMA", "SARIMA", "ETS"), \
            f"Expected ARIMA, SARIMA or ETS, got {model}"

    def test_models_all_three(self):
        """Test with all three model families."""
        key = "test:autoforecast:models:all"
        create_linear_series(self.client, key, count=150)

        result = self.client.execute_command(
            "TS.AUTOFORECAST", key, "-", "+",
            "HORIZON", "5", "MODELS", "ARIMA,ETS,THETA"
        )

        parsed = parse_forecast_response(result)
        model = parsed["model"]
        assert model in ("ARIMA", "SARIMA", "ETS", "Theta"), \
            f"Unexpected model: {model}"

    def test_models_case_insensitive(self):
        """Test that model names are case-insensitive."""
        key = "test:autoforecast:models:case"
        create_linear_series(self.client, key, count=150)

        result = self.client.execute_command(
            "TS.AUTOFORECAST", key, "-", "+",
            "HORIZON", "5", "MODELS", "arima,ets,theta"
        )

        parsed = parse_forecast_response(result)
        model = parsed["model"]
        assert model in ("ARIMA", "SARIMA", "ETS", "Theta"), \
            f"Unexpected model: {model}"

    def test_models_auto_prefix(self):
        """Test that AUTOARIMA/AUTOETS/AUTOTHETA aliases work."""
        key = "test:autoforecast:models:auto"
        create_linear_series(self.client, key, count=150)

        result = self.client.execute_command(
            "TS.AUTOFORECAST", key, "-", "+",
            "HORIZON", "5", "MODELS", "AutoARIMA"
        )

        parsed = parse_forecast_response(result)
        model = parsed["model"]
        assert model == "ARIMA", f"Expected ARIMA, got {model}"

    # ── SEASONALITY option ───────────────────────────────────────────────

    def test_seasonality_daily(self):
        """Test with daily seasonality on seasonal data."""
        key = "test:autoforecast:seasonality:daily"
        create_daily_seasonal_series(self.client, key, days=21)

        result = self.client.execute_command(
            "TS.AUTOFORECAST", key, "-", "+",
            "HORIZON", "10", "SEASONALITY", "24",
            "MODELS", "ARIMA,ETS"
        )

        parsed = parse_forecast_response(result)
        forecasts = get_forecast_values(parsed)
        assert len(forecasts) == 10
        model = parsed["model"]
        assert model in ("ARIMA", "SARIMA", "ETS"), \
            f"Expected ARIMA, SARIMA or ETS, got {model}"

    def test_seasonality_sine(self):
        """Test with seasonality on a pure sine wave."""
        key = "test:autoforecast:seasonality:sine"
        create_sine_series(self.client, key, count=200, period=24)

        result = self.client.execute_command(
            "TS.AUTOFORECAST", key, "-", "+",
            "HORIZON", "24", "SEASONALITY", "24",
            "MODELS", "ARIMA,ETS"
        )

        parsed = parse_forecast_response(result)
        forecasts = get_forecast_values(parsed)
        assert len(forecasts) == 24
        # Forecast values should be reasonable (not NaN or extreme)
        for v in forecasts:
            assert not math.isnan(v), f"Forecast contains NaN: {forecasts}"
            assert not math.isinf(v), f"Forecast contains Inf: {forecasts}"

    # ── STORE option ─────────────────────────────────────────────────────

    def test_store_creates_key(self):
        """Test STORE persists forecast into a new timeseries key."""
        key = "test:autoforecast:store:src"
        store_key = "test:autoforecast:store:dst"
        create_linear_series(self.client, key, count=150)

        result = self.client.execute_command(
            "TS.AUTOFORECAST", key, "-", "+",
            "HORIZON", "5", "STORE", store_key
        )

        parsed = parse_forecast_response(result)
        forecasts = get_forecast_values(parsed)
        assert len(forecasts) == 5

        # Verify the stored key exists and has the forecast values
        assert self.client.execute_command("EXISTS", store_key) == 1, \
            f"STORE key '{store_key}' was not created"

        # Retrieve stored values via TS.RANGE and verify count
        stored = self.client.execute_command("TS.RANGE", store_key, "-", "+")
        assert len(stored) == 5, \
            f"Expected 5 stored samples, got {len(stored)}"

        # Verify stored values match the forecast
        for (ts, val), expected in zip(stored, forecasts):
            assert abs(float(val) - expected) < 0.01, \
                f"Stored value {val} differs from forecast {expected}"

    def test_store_and_level_together(self):
        """Test STORE with LEVEL (both together)."""
        key = "test:autoforecast:store:level:src"
        store_key = "test:autoforecast:store:level:dst"
        create_linear_series(self.client, key, count=150)

        result = self.client.execute_command(
            "TS.AUTOFORECAST", key, "-", "+",
            "HORIZON", "4", "LEVEL", "80", "STORE", store_key
        )

        parsed = parse_forecast_response(result)
        assert "level" in parsed
        assert "lower_interval" in parsed
        assert "upper_interval" in parsed

        # Verify stored key
        assert self.client.execute_command("EXISTS", store_key) == 1
        stored = self.client.execute_command("TS.RANGE", store_key, "-", "+")
        assert len(stored) == 4

    def test_store_on_existing_key(self):
        """Test STORE with MERGE on an existing key combines existing and forecast samples."""
        key = "test:autoforecast:store:exist:src"
        store_key = "test:autoforecast:store:exist:dst"
        create_linear_series(self.client, key, count=150)

        # Pre-create the destination key
        self.client.execute_command("TS.CREATE", store_key)
        self.client.execute_command("TS.ADD", store_key, 1000, 42.0)

        result = self.client.execute_command(
            "TS.AUTOFORECAST", key, "-", "+",
            "HORIZON", "3", "STORE", store_key, "MERGE"
        )

        parsed = parse_forecast_response(result)
        forecasts = get_forecast_values(parsed)
        assert len(forecasts) == 3

        # Stored key should now have the forecast values
        stored = self.client.execute_command("TS.RANGE", store_key, "-", "+")
        assert len(stored) == 4, \
            f"Expected 4 stored samples after merge (1 existing + 3 forecast), got {len(stored)}"

    def test_store_timestamps_are_sequential(self):
        """Test that stored forecast timestamps are in proper sequence."""
        key = "test:autoforecast:store:timestamps:src"
        store_key = "test:autoforecast:store:timestamps:dst"
        create_linear_series(self.client, key, count=100, step_ms=60000)

        result = self.client.execute_command(
            "TS.AUTOFORECAST", key, "-", "+",
            "HORIZON", "5", "STORE", store_key
        )

        parsed = parse_forecast_response(result)
        assert "forecast" in parsed

        stored = self.client.execute_command("TS.RANGE", store_key, "-", "+")
        assert len(stored) >= 1, "Expected at least one stored sample"
        # Timestamps should be monotonically increasing
        timestamps = [s[0] for s in stored]
        for i in range(1, len(timestamps)):
            assert timestamps[i] > timestamps[i - 1], \
                f"Timestamps not monotonic: {timestamps}"

    # ── time-range filtering ─────────────────────────────────────────────

    def test_time_range_filter(self):
        """Test forecast using only a subset of the time range."""
        key = "test:autoforecast:range"
        values = [10.0 + 0.5 * i for i in range(200)]
        _add(self.client, key, 1000, values, 60000)

        # Use the middle portion of the data
        from_ts = 1000 + 50 * 60000   # skip first 50 points
        to_ts = 1000 + 149 * 60000     # use up to point 149

        result = self.client.execute_command(
            "TS.AUTOFORECAST", key,
            str(from_ts), str(to_ts),
            "HORIZON", "5"
        )

        parsed = parse_forecast_response(result)
        forecasts = get_forecast_values(parsed)
        assert len(forecasts) == 5
        for v in forecasts:
            assert not math.isnan(v)

    def test_time_range_small_window(self):
        """Test forecast with a narrow time window."""
        key = "test:autoforecast:range:narrow"
        values = [10.0 + 0.5 * i for i in range(200)]
        _add(self.client, key, 1000, values, 60000)

        # Use only 10 data points
        from_ts = 1000 + 100 * 60000
        to_ts = 1000 + 109 * 60000

        result = self.client.execute_command(
            "TS.AUTOFORECAST", key,
            str(from_ts), str(to_ts),
            "HORIZON", "3"
        )

        parsed = parse_forecast_response(result)
        forecasts = get_forecast_values(parsed)
        assert len(forecasts) == 3

    # ── response format validation ───────────────────────────────────────

    def test_response_flat_map_format(self):
        """Test that the response is a flat key-value map."""
        key = "test:autoforecast:format"
        create_linear_series(self.client, key, count=100)

        result = self.client.execute_command(
            "TS.AUTOFORECAST", key, "-", "+", "HORIZON", "3"
        )

        # Response should be a list with even number of elements
        assert isinstance(result, list), \
            f"Expected list response, got {type(result)}"
        assert len(result) % 2 == 0, \
            f"Expected even-length flat map, got {len(result)} elements"

        parsed = parse_forecast_response(result)

        # Required fields
        assert "model" in parsed
        assert "horizon" in parsed
        assert "forecast" in parsed

        # model should be a string/bytes
        model = parsed["model"]
        assert isinstance(model, (str, bytes)), \
            f"model should be string/bytes, got {type(model)}"

        # horizon should be a string/bytes representing an integer
        horizon = parsed["horizon"]
        assert int(horizon) > 0

        # forecast should be a list of numbers
        forecast = parsed["forecast"]
        assert isinstance(forecast, list)
        assert len(forecast) > 0

    def test_response_with_level_has_intervals(self):
        """Test that LEVEL adds interval fields but omits them without LEVEL."""
        key = "test:autoforecast:format:level"
        create_linear_series(self.client, key, count=120)

        # Without LEVEL
        result_no_level = self.client.execute_command(
            "TS.AUTOFORECAST", key, "-", "+", "HORIZON", "3"
        )
        parsed_no = parse_forecast_response(result_no_level)
        assert "level" not in parsed_no
        assert "lower_interval" not in parsed_no
        assert "upper_interval" not in parsed_no

        # With LEVEL
        result_with = self.client.execute_command(
            "TS.AUTOFORECAST", key, "-", "+", "HORIZON", "3", "LEVEL", "95"
        )
        parsed_with = parse_forecast_response(result_with)
        assert "level" in parsed_with
        assert "lower_interval" in parsed_with
        assert "upper_interval" in parsed_with

    def test_response_without_metrics_omits_field(self):
        """Test that metrics field is omitted when METRICS is not specified."""
        key = "test:autoforecast:format:no_metrics"
        create_linear_series(self.client, key, count=120)

        result = self.client.execute_command(
            "TS.AUTOFORECAST", key, "-", "+", "HORIZON", "3"
        )
        parsed = parse_forecast_response(result)
        assert "metrics" not in parsed

    # ── error handling ───────────────────────────────────────────────────

    def test_error_nonexistent_key(self):
        """Test error on non-existent key."""
        with pytest.raises(ResponseError, match="the key does not exist"):
            self.client.execute_command(
                "TS.AUTOFORECAST", "nonexistent_key", "-", "+",
                "HORIZON", "5"
            )

    def test_error_missing_horizon(self):
        """Test that HORIZON is required."""
        key = "test:autoforecast:err:no_horizon"
        create_linear_series(self.client, key, count=50)

        with pytest.raises(ResponseError):
            self.client.execute_command(
                "TS.AUTOFORECAST", key, "-", "+"
            )

    def test_error_insufficient_data(self):
        """Test error with very few data points."""
        key = "test:autoforecast:err:few_points"
        self.client.execute_command("TS.CREATE", key)
        self.client.execute_command("TS.ADD", key, 1000, 1.0)
        self.client.execute_command("TS.ADD", key, 2000, 2.0)

        with pytest.raises(ResponseError):
            self.client.execute_command(
                "TS.AUTOFORECAST", key, "-", "+", "HORIZON", "5"
            )

    def test_error_invalid_model(self):
        """Test error with an unknown model name."""
        key = "test:autoforecast:err:bad_model"
        create_linear_series(self.client, key, count=100)

        with pytest.raises(ResponseError, match="unknown auto-forecast model"):
            self.client.execute_command(
                "TS.AUTOFORECAST", key, "-", "+",
                "HORIZON", "5", "MODELS", "INVALID_MODEL"
            )

    def test_error_invalid_model_among_valid(self):
        """Test error with a mix of valid and invalid models."""
        key = "test:autoforecast:err:mixed_models"
        create_linear_series(self.client, key, count=100)

        with pytest.raises(ResponseError, match="unknown auto-forecast model"):
            self.client.execute_command(
                "TS.AUTOFORECAST", key, "-", "+",
                "HORIZON", "5", "MODELS", "ARIMA,INVALID,ETS"
            )

    def test_error_level_zero(self):
        """Test error when LEVEL is 0 (must be > 0)."""
        key = "test:autoforecast:err:level_zero"
        create_linear_series(self.client, key, count=100)

        with pytest.raises(ResponseError, match="TSDB: LEVEL must be between 0 and 100"):
            self.client.execute_command(
                "TS.AUTOFORECAST", key, "-", "+",
                "HORIZON", "5", "LEVEL", "0"
            )

    def test_error_level_100(self):
        """Test error when LEVEL is 100 (must be < 100)."""
        key = "test:autoforecast:err:level_100"
        create_linear_series(self.client, key, count=100)

        with pytest.raises(ResponseError, match="LEVEL must be between 0 and 100"):
            self.client.execute_command(
                "TS.AUTOFORECAST", key, "-", "+",
                "HORIZON", "5", "LEVEL", "100"
            )

    def test_error_level_negative(self):
        """Test error when LEVEL is negative."""
        key = "test:autoforecast:err:level_neg"
        create_linear_series(self.client, key, count=100)

        with pytest.raises(ResponseError, match="LEVEL must be between 0 and 100"):
            self.client.execute_command(
                "TS.AUTOFORECAST", key, "-", "+",
                "HORIZON", "5", "LEVEL", "-1"
            )

    def test_error_unknown_argument(self):
        """Test error with an unknown argument."""
        key = "test:autoforecast:err:unknown_arg"
        create_linear_series(self.client, key, count=100)

        with pytest.raises(ResponseError, match="Unknown argument"):
            self.client.execute_command(
                "TS.AUTOFORECAST", key, "-", "+",
                "HORIZON", "5", "INVALID_OPTION", "value"
            )

    def test_error_missing_store_value(self):
        """Test error when STORE has no key value."""
        key = "test:autoforecast:err:store_missing"
        create_linear_series(self.client, key, count=100)

        with pytest.raises(ResponseError):
            self.client.execute_command(
                "TS.AUTOFORECAST", key, "-", "+",
                "HORIZON", "5", "STORE"
            )

    def test_error_wrong_arity(self):
        """Test error with too few arguments."""
        with pytest.raises(ResponseError):
            self.client.execute_command("TS.AUTOFORECAST", "key")

    # ── edge cases ───────────────────────────────────────────────────────

    def test_forecast_constant_series(self):
        """Test forecast on a constant series."""
        key = "test:autoforecast:constant"
        _add(self.client, key, 1000, [10.0] * 100, 60000)

        result = self.client.execute_command(
            "TS.AUTOFORECAST", key, "-", "+", "HORIZON", "5"
        )

        parsed = parse_forecast_response(result)
        forecasts = get_forecast_values(parsed)
        assert len(forecasts) == 5
        for v in forecasts:
            assert not math.isnan(v)
            assert not math.isinf(v)

    def test_forecast_random_walk(self):
        """Test forecast on a random-walk series."""
        key = "test:autoforecast:random_walk"
        import random
        random.seed(42)
        values = [100.0]
        for i in range(199):
            values.append(values[-1] + random.gauss(0, 2))
        _add(self.client, key, 1000, values, 60000)

        result = self.client.execute_command(
            "TS.AUTOFORECAST", key, "-", "+", "HORIZON", "5"
        )

        parsed = parse_forecast_response(result)
        forecasts = get_forecast_values(parsed)
        assert len(forecasts) == 5
        for v in forecasts:
            assert not math.isnan(v)
            assert not math.isinf(v)

    def test_forecast_all_args_combined(self):
        """Test with all options combined together."""
        key = "test:autoforecast:combined"
        store_key = "test:autoforecast:combined:dst"
        create_daily_seasonal_series(self.client, key, days=28)

        result = self.client.execute_command(
            "TS.AUTOFORECAST", key, "-", "+",
            "HORIZON", "7",
            "SEASONALITY", "24",
            "MODELS", "ARIMA,ETS",
            "LEVEL", "95",
            "METRICS",
            "STORE", store_key,
        )

        parsed = parse_forecast_response(result)
        assert "model" in parsed
        assert int(parsed["horizon"]) == 7
        assert "level" in parsed
        assert "lower_interval" in parsed
        assert "upper_interval" in parsed
        assert "metrics" in parsed
        forecasts = get_forecast_values(parsed)
        assert len(forecasts) == 7

        # Validate stored key
        assert self.client.execute_command("EXISTS", store_key) == 1
        stored = self.client.execute_command("TS.RANGE", store_key, "-", "+")
        assert len(stored) == 7

    def test_forecast_level_boundary_90(self):
        """Test LEVEL with boundary value 90 (high confidence)."""
        key = "test:autoforecast:boundary:level90"
        create_linear_series(self.client, key, count=150)

        result = self.client.execute_command(
            "TS.AUTOFORECAST", key, "-", "+",
            "HORIZON", "3", "LEVEL", "90"
        )

        parsed = parse_forecast_response(result)
        assert float(parsed["level"]) == 90.0
        lower = parsed["lower_interval"]
        upper = parsed["upper_interval"]
        forecasts = get_forecast_values(parsed)
        # At 90% confidence, intervals should be wider but still valid
        for i, (lo, f, hi) in enumerate(zip(lower, forecasts, upper)):
            assert lo <= f <= hi, \
                f"Level 90: lower={lo} forecast={f} upper={hi}"

    def test_forecast_level_boundary_20(self):
        """Test LEVEL with boundary value 20 (low confidence, intervals still produced)."""
        key = "test:autoforecast:boundary:level20"
        create_linear_series(self.client, key, count=150)

        result = self.client.execute_command(
            "TS.AUTOFORECAST", key, "-", "+",
            "HORIZON", "3", "LEVEL", "20"
        )

        parsed = parse_forecast_response(result)
        assert float(parsed["level"]) == 20.0
        lower = parsed["lower_interval"]
        upper = parsed["upper_interval"]
        forecasts = get_forecast_values(parsed)
        # At 20% confidence, intervals should be narrow but still valid
        for i, (lo, f, hi) in enumerate(zip(lower, forecasts, upper)):
            assert lo <= f <= hi, \
                f"Level 20: lower={lo} forecast={f} upper={hi}"
