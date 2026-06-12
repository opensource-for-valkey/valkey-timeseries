"""
Integration tests for TS.TREND command.

Converted from the Rust unit tests in anofox-forecast:
  https://github.com/sipemu/anofox-forecast/blob/main/src/seasonality/auto_trend.rs

Covers:
- Trend selection: linear, quadratic, exponential data
- Fitted trend accuracy
- Information criteria: AICc (default), BIC, HOLDOUT
- Prediction (PREDICT option)
- Store (STORE option) — fitted values, combined with PREDICT
- Features (FEATURES option)
- Metrics (METRICS option)
- Scores format, sorting, and candidate completeness
- Recency options: FULL, WINDOW, FRACTION
- Negative data excludes Exponential
- Error handling: nonexistent key, insufficient data, invalid arguments
- Response format validation (map structure)
"""

import math
from typing import Any, Dict, List

import pytest
from valkey import ResponseError

from valkey_timeseries_test_case import ValkeyTimeSeriesTestCaseBase
from data_helpers import (
    _add,
    create_exponential_series,
    create_linear_series,
    create_negative_linear_series,
    create_quadratic_series,
)

def parse_trend_response(response: List[Any]) -> Dict[str, Any]:
    """Parse the flat key-value list response from TS.TREND into a dict.

    The response is a flat list of alternating key-value pairs:
    [key, "t", selected_series, "Linear", criterion, "AICc",
     fitted_trend, [...], scores, [[name, score], ...], n_params, 2, ...]

    All values are decoded to their native Python types:
    - fitted_trend, predicted_trend → list of float
    - scores → list of (str, float) tuples
    - n_params → int
    - features → dict of {str: float}
    - metrics → dict of {str: float|None}
    - key, selected_series, criterion, model → str
    """
    if not response:
        return {}

    result: Dict[str, Any] = {}
    it = iter(response)
    for key in it:
        key_str = key.decode("utf-8") if isinstance(key, bytes) else str(key)
        value = next(it)

        if key_str in ("fitted_trend", "predicted_trend"):
            result[key_str] = [float(v) for v in value]
        elif key_str == "scores":
            # scores is an array of [name, score] pairs
            parsed_scores = []
            for pair in value:
                if isinstance(pair, (list, tuple)):
                    name = pair[0].decode("utf-8") if isinstance(pair[0], bytes) else str(pair[0])
                    score = float(pair[1])
                    parsed_scores.append((name, score))
            result[key_str] = parsed_scores
        elif key_str == "n_params":
            result[key_str] = int(value)
        elif key_str == "features":
            # features is a flat list of key-value pairs: [name1, val1, name2, val2, ...]
            features_dict = {}
            f_it = iter(value)
            for f_key in f_it:
                f_key_str = f_key.decode("utf-8") if isinstance(f_key, bytes) else str(f_key)
                f_val = next(f_it)
                features_dict[f_key_str] = float(f_val)
            result[key_str] = features_dict
        elif key_str == "metrics":
            # metrics is a flat list of key-value pairs: [name1, val1, name2, val2, ...]
            metrics_dict = {}
            m_it = iter(value)
            for m_key in m_it:
                m_key_str = m_key.decode("utf-8") if isinstance(m_key, bytes) else str(m_key)
                m_val = next(m_it)
                metrics_dict[m_key_str] = None if m_val is None else float(m_val)
            result[key_str] = metrics_dict
        else:
            result[key_str] = value.decode("utf-8") if isinstance(value, bytes) else str(value)

    return result


# ── test class ───────────────────────────────────────────────────────────────

class TestTrend(ValkeyTimeSeriesTestCaseBase):
    """Test TS.TREND command — converted from anofox-forecast Rust tests."""

    # ═══════════════════════════════════════════════════════════════════════
    # Trend selection: model picks the right component for the data shape
    # ═══════════════════════════════════════════════════════════════════════

    def test_selects_linear_for_linear_data(self):
        """Linear data → should select Linear or TheilSen.

        Corresponds to Rust: selects_linear_for_linear_data
        """
        key = "test:trend:select:linear"
        create_linear_series(self.client, key, count=100,
                              slope=2.0, intercept=1.0)

        result = self.client.execute_command(
            "TS.TREND", key, "-", "+", "RECENCY", "FULL"
        )

        parsed = parse_trend_response(result)
        selected = parsed["selected_series"]
        # Linear or TheilSen should be top (both fit perfectly)
        assert selected in ("Linear", "TheilSen"), \
            f"expected Linear or TheilSen, got {selected}"

    def test_fitted_matches_data_for_linear(self):
        """Fitted trend values should closely match the input linear data.

        Corresponds to Rust: fitted_matches_data_for_linear
        """
        key = "test:trend:fitted:linear"
        values = [3.0 * i - 5.0 for i in range(100)]
        _add(self.client, key, 1000, values)

        result = self.client.execute_command(
            "TS.TREND", key, "-", "+", "RECENCY", "FULL"
        )

        parsed = parse_trend_response(result)
        fitted = parsed["fitted_trend"]
        assert len(fitted) == 100, \
            f"Expected 100 fitted values, got {len(fitted)}"

        # Fitted values should be close to the original data
        for i, (f, v) in enumerate(zip(fitted, values)):
            assert abs(f - v) < 0.2, \
                f"At index {i}: fitted={f} original={v} diff={abs(f - v)} exceeds 0.2"

    def test_selects_exponential_for_exponential_data(self):
        """Exponential data → should select Exponential.

        Corresponds to Rust: selects_exponential_for_exponential_data
        """
        key = "test:trend:select:exponential"
        create_exponential_series(self.client, key, count=100)

        result = self.client.execute_command(
            "TS.TREND", key, "-", "+", "RECENCY", "FULL"
        )

        parsed = parse_trend_response(result)
        selected = parsed["selected_series"]
        scores = parsed["scores"]
        assert selected == "Exponential", \
            f"expected Exponential, got {selected}, scores: {scores}"

    def test_selects_quadratic_for_quadratic_data(self):
        """Quadratic data → should select Quadratic.

        Corresponds to Rust: selects_quadratic_for_quadratic_data
        """
        key = "test:trend:select:quadratic"
        create_quadratic_series(self.client, key, count=100)

        result = self.client.execute_command(
            "TS.TREND", key, "-", "+", "RECENCY", "FULL"
        )

        parsed = parse_trend_response(result)
        selected = parsed["selected_series"]
        scores = parsed["scores"]
        assert selected == "Quadratic", \
            f"expected Quadratic, got {selected}, scores: {scores}"

    # ═══════════════════════════════════════════════════════════════════════
    # Scores: completeness, sorting
    # ═══════════════════════════════════════════════════════════════════════

    def test_selection_result_has_all_candidates(self):
        """All positive data → should have Linear, Quadratic, Exponential,
        TheilSen, PiecewiseLinear in the scores.

        Corresponds to Rust: selection_result_has_all_candidates
        """
        key = "test:trend:scores:all_candidates"
        create_exponential_series(self.client, key, count=100)

        result = self.client.execute_command(
            "TS.TREND", key, "-", "+", "RECENCY", "FULL"
        )

        parsed = parse_trend_response(result)
        scores = parsed["scores"]
        # All positive data → at least 4 candidates
        assert len(scores) >= 4, \
            f"expected at least 4 candidates in scores, got {len(scores)}: {scores}"

    def test_scores_are_sorted_ascending(self):
        """Scores should be sorted from lowest (best) to highest (worst).

        Corresponds to Rust: scores_are_sorted_ascending
        """
        key = "test:trend:scores:sorted"
        create_linear_series(self.client, key, count=100,
                              slope=2.0, intercept=1.0)

        result = self.client.execute_command(
            "TS.TREND", key, "-", "+", "RECENCY", "FULL"
        )

        parsed = parse_trend_response(result)
        scores = parsed["scores"]
        assert len(scores) >= 2, f"Need at least 2 scores to check sorting, got {len(scores)}"

        for i in range(len(scores) - 1):
            assert scores[i][1] <= scores[i + 1][1], \
                f"scores not sorted: {scores[i]} > {scores[i + 1]}"

    # ═══════════════════════════════════════════════════════════════════════
    # Information criteria: BIC, HOLDOUT
    # ═══════════════════════════════════════════════════════════════════════

    def test_bic_criterion_works(self):
        """BIC criterion returns correct criterion label and non-empty scores.

        Corresponds to Rust: bic_criterion_works
        """
        key = "test:trend:criterion:bic"
        create_linear_series(self.client, key, count=100,
                              slope=2.0, intercept=1.0)

        result = self.client.execute_command(
            "TS.TREND", key, "-", "+",
            "MODEL", "Auto", "BIC", "RECENCY", "FULL"
        )

        parsed = parse_trend_response(result)
        assert parsed["criterion"] == "BIC", \
            f"Expected criterion BIC, got {parsed['criterion']}"
        assert len(parsed["scores"]) > 0, "Expected non-empty scores"

    def test_holdout_criterion_works(self):
        """Holdout criterion returns correct criterion label.

        Corresponds to Rust: holdout_criterion_works
        """
        key = "test:trend:criterion:holdout"
        create_linear_series(self.client, key, count=100,
                              slope=2.0, intercept=1.0)

        result = self.client.execute_command(
            "TS.TREND", key, "-", "+",
            "MODEL", "Auto", "HOLDOUT", "RECENCY", "FULL"
        )

        parsed = parse_trend_response(result)
        assert parsed["criterion"] == "HOLDOUT", \
            f"Expected criterion HOLDOUT, got {parsed['criterion']}"

    def test_default_criterion_is_aicc(self):
        """Default criterion should be AICc when not specified."""
        key = "test:trend:criterion:default"
        create_linear_series(self.client, key, count=100,
                              slope=2.0, intercept=1.0)

        result = self.client.execute_command(
            "TS.TREND", key, "-", "+", "RECENCY", "FULL"
        )

        parsed = parse_trend_response(result)
        assert parsed["criterion"] == "AICc", \
            f"Expected default criterion AICc, got {parsed['criterion']}"

    # ═══════════════════════════════════════════════════════════════════════
    # Prediction (PREDICT option)
    # ═══════════════════════════════════════════════════════════════════════

    def test_predict_delegates_to_winner(self):
        """PREDICT returns forecast values that extrapolate the trend.

        Corresponds to Rust: predict_delegates_to_winner
        """
        key = "test:trend:predict:basic"
        values = [2.0 * i + 1.0 for i in range(100)]
        _add(self.client, key, 1000, values)

        result = self.client.execute_command(
            "TS.TREND", key, "-", "+",
            "RECENCY", "FULL", "PREDICT", "10"
        )

        parsed = parse_trend_response(result)
        assert "predicted_trend" in parsed, \
            f"Expected predicted_trend in response, got keys: {list(parsed.keys())}"

        predicted = parsed["predicted_trend"]
        assert len(predicted) == 10, \
            f"Expected 10 predicted values, got {len(predicted)}"

        # Predictions should extrapolate beyond the last data point
        last_value = values[-1]
        assert predicted[0] > last_value - 1.0, \
            f"First prediction {predicted[0]} should be near or above last value {last_value}"

    def test_predict_without_predict_omits_field(self):
        """Without PREDICT, predicted_trend should be absent from response."""
        key = "test:trend:predict:absent"
        create_linear_series(self.client, key, count=100)

        result = self.client.execute_command(
            "TS.TREND", key, "-", "+", "RECENCY", "FULL"
        )

        parsed = parse_trend_response(result)
        assert "predicted_trend" not in parsed, \
            f"predicted_trend should be absent without PREDICT option"

    # ═══════════════════════════════════════════════════════════════════════
    # STORE option
    # ═══════════════════════════════════════════════════════════════════════

    def test_store_creates_key_with_fitted_values(self):
        """STORE creates a new time series key with fitted trend values."""
        key = "test:trend:store:src"
        store_key = "test:trend:store:fitted:dst"
        values = [2.0 * i + 1.0 for i in range(100)]
        _add(self.client, key, 1000, values)

        result = self.client.execute_command(
            "TS.TREND", key, "-", "+",
            "RECENCY", "FULL", "STORE", store_key
        )

        parsed = parse_trend_response(result)
        fitted = parsed["fitted_trend"]
        assert len(fitted) == 100

        # Verify the stored key exists
        assert self.client.execute_command("EXISTS", store_key) == 1, \
            f"STORE key '{store_key}' was not created"

        # Retrieve stored values via TS.RANGE and verify count
        stored = self.client.execute_command("TS.RANGE", store_key, "-", "+")
        assert len(stored) == 100, \
            f"Expected 100 stored samples, got {len(stored)}"

        # Verify stored values match the fitted trend
        for (ts, val), expected in zip(stored, fitted):
            assert abs(float(val) - expected) < 0.2, \
                f"Stored fitted value {val} differs from fitted {expected}"

    def test_store_fitted_timestamps_match_input(self):
        """STORE preserves the original timestamps from the input series."""
        key = "test:trend:store:timestamps:src"
        store_key = "test:trend:store:timestamps:dst"
        values = [3.0 * i - 5.0 for i in range(50)]
        _add(self.client, key, 5000, values, step_ms=2000)

        self.client.execute_command(
            "TS.TREND", key, "-", "+",
            "RECENCY", "FULL", "STORE", store_key
        )

        # Retrieve stored samples
        stored = self.client.execute_command("TS.RANGE", store_key, "-", "+")
        assert len(stored) == 50

        # Each stored timestamp should match the original input timestamp
        for i, (ts, val) in enumerate(stored):
            expected_ts = 5000 + i * 2000
            assert int(ts) == expected_ts, \
                f"Stored timestamp {ts} != expected {expected_ts} at index {i}"

        # Timestamps should be monotonically increasing
        timestamps = [s[0] for s in stored]
        for i in range(1, len(timestamps)):
            assert timestamps[i] > timestamps[i - 1], \
                f"Timestamps not monotonic: {timestamps}"

    def test_store_with_predict_stores_both(self):
        """STORE + PREDICT stores fitted values then appends predicted values."""
        key = "test:trend:store:predict:src"
        store_key = "test:trend:store:predict:dst"
        values = [2.0 * i + 1.0 for i in range(100)]
        _add(self.client, key, 1000, values)

        result = self.client.execute_command(
            "TS.TREND", key, "-", "+",
            "RECENCY", "FULL", "PREDICT", "5", "STORE", store_key
        )

        parsed = parse_trend_response(result)
        fitted = parsed["fitted_trend"]
        predicted = parsed["predicted_trend"]
        assert len(fitted) == 100
        assert len(predicted) == 5

        # Verify stored key exists
        assert self.client.execute_command("EXISTS", store_key) == 1

        # Retrieve all stored values — should be fitted + predicted
        stored = self.client.execute_command("TS.RANGE", store_key, "-", "+")
        assert len(stored) == 105, \
            f"Expected 105 stored samples (100 fitted + 5 predicted), got {len(stored)}"

        # First 100 stored values should match fitted trend
        for (ts, val), expected in zip(stored[:100], fitted):
            assert abs(float(val) - expected) < 0.2, \
                f"Stored fitted value {val} differs from fitted {expected}"

        # Last 5 stored values should match predicted trend
        for (ts, val), expected in zip(stored[100:], predicted):
            assert abs(float(val) - expected) < 0.2, \
                f"Stored predicted value {val} differs from predicted {expected}"

    def test_store_predicted_timestamps_are_sequential(self):
        """STORE predicted timestamps extend beyond the last fitted timestamp."""
        key = "test:trend:store:pred_ts:src"
        store_key = "test:trend:store:pred_ts:dst"
        values = [1.0 * i for i in range(50)]
        _add(self.client, key, 10000, values, step_ms=60000)

        self.client.execute_command(
            "TS.TREND", key, "-", "+",
            "RECENCY", "FULL", "PREDICT", "3", "STORE", store_key
        )

        stored = self.client.execute_command("TS.RANGE", store_key, "-", "+")
        assert len(stored) == 53  # 50 fitted + 3 predicted

        # The last fitted timestamp should be < first predicted timestamp
        last_fitted_ts = stored[49][0]
        first_pred_ts = stored[50][0]
        assert first_pred_ts > last_fitted_ts, \
            f"Predicted timestamp {first_pred_ts} should be after last fitted {last_fitted_ts}"

        # Predicted timestamps should be at the expected step interval
        step = int(stored[50][0]) - int(stored[49][0])
        assert step >= 60000, \
            f"Expected predicted step >= 60000ms, got {step}"

        # All timestamps should be monotonically increasing
        timestamps = [s[0] for s in stored]
        for i in range(1, len(timestamps)):
            assert timestamps[i] > timestamps[i - 1], \
                f"Timestamps not monotonic at index {i}: {timestamps[i-1]} >= {timestamps[i]}"

    def test_store_on_existing_key_merges(self):
        """STORE on an existing key merges fitted samples into it."""
        key = "test:trend:store:exist:src"
        store_key = "test:trend:store:exist:dst"
        values = [2.0 * i + 1.0 for i in range(10)]
        _add(self.client, key, 1000, values)

        # Pre-create the destination key with one existing sample
        self.client.execute_command("TS.CREATE", store_key)
        self.client.execute_command("TS.ADD", store_key, 500, 99.0)

        self.client.execute_command(
            "TS.TREND", key, "-", "+",
            "RECENCY", "FULL", "STORE", store_key
        )

        # Stored key should now have existing + fitted samples
        stored = self.client.execute_command("TS.RANGE", store_key, "-", "+")
        assert len(stored) == 11, \
            f"Expected 11 stored samples (1 existing + 10 fitted), got {len(stored)}"

        # The pre-existing sample should still be present
        assert int(stored[0][0]) == 500, \
            f"Expected first timestamp 500, got {stored[0][0]}"

    # ═══════════════════════════════════════════════════════════════════════
    # Features (FEATURES option)
    # ═══════════════════════════════════════════════════════════════════════

    def test_features_delegate_to_winner(self):
        """FEATURES returns a non-empty features map.

        Corresponds to Rust: features_delegate_to_winner
        """
        key = "test:trend:features:basic"
        create_linear_series(self.client, key, count=100,
                              slope=2.0, intercept=1.0)

        result = self.client.execute_command(
            "TS.TREND", key, "-", "+",
            "RECENCY", "FULL", "FEATURES"
        )

        parsed = parse_trend_response(result)
        assert "features" in parsed, \
            f"Expected features in response, got keys: {list(parsed.keys())}"
        features = parsed["features"]
        assert len(features) > 0, "Expected non-empty features map"

    def test_features_without_features_omits_field(self):
        """Without FEATURES, features should be absent from response."""
        key = "test:trend:features:absent"
        create_linear_series(self.client, key, count=100)

        result = self.client.execute_command(
            "TS.TREND", key, "-", "+", "RECENCY", "FULL"
        )

        parsed = parse_trend_response(result)
        assert "features" not in parsed, \
            f"features should be absent without FEATURES option"

    # ═══════════════════════════════════════════════════════════════════════
    # Metrics (METRICS option)
    # ═══════════════════════════════════════════════════════════════════════

    def test_metrics_returns_metrics_map(self):
        """METRICS returns a metrics map with expected keys."""
        key = "test:trend:metrics:basic"
        create_linear_series(self.client, key, count=100,
                              slope=2.0, intercept=1.0)

        result = self.client.execute_command(
            "TS.TREND", key, "-", "+",
            "RECENCY", "FULL", "METRICS"
        )

        parsed = parse_trend_response(result)
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

    def test_metrics_without_metrics_omits_field(self):
        """Without METRICS, metrics should be absent from response."""
        key = "test:trend:metrics:absent"
        create_linear_series(self.client, key, count=100)

        result = self.client.execute_command(
            "TS.TREND", key, "-", "+", "RECENCY", "FULL"
        )

        parsed = parse_trend_response(result)
        assert "metrics" not in parsed, \
            f"metrics should be absent without METRICS option"

    def test_metrics_with_specific_model(self):
        """METRICS also works for specific-model mode."""
        key = "test:trend:metrics:specific_model"
        create_linear_series(self.client, key, count=100,
                              slope=2.0, intercept=1.0)

        result = self.client.execute_command(
            "TS.TREND", key, "-", "+",
            "MODEL", "TheilSen", "RECENCY", "FULL", "METRICS"
        )

        parsed = parse_trend_response(result)
        assert parsed["model"] == "TheilSen"
        assert "metrics" in parsed
        assert "r_squared" in parsed["metrics"]

    # ═══════════════════════════════════════════════════════════════════════
    # Recency options
    # ═══════════════════════════════════════════════════════════════════════

    def test_recency_full(self):
        """RECENCY FULL uses all data and returns valid fitted trend."""
        key = "test:trend:recency:full"
        create_linear_series(self.client, key, count=100)

        result = self.client.execute_command(
            "TS.TREND", key, "-", "+", "RECENCY", "FULL"
        )

        parsed = parse_trend_response(result)
        assert len(parsed["fitted_trend"]) == 100, \
            f"Expected 100 fitted values, got {len(parsed['fitted_trend'])}"

    def test_recency_window(self):
        """RECENCY WINDOW n uses last n observations for fitting."""
        key = "test:trend:recency:window"
        create_linear_series(self.client, key, count=100)

        result = self.client.execute_command(
            "TS.TREND", key, "-", "+", "RECENCY", "WINDOW", "50"
        )

        parsed = parse_trend_response(result)
        # Fitted values still cover full series (back-extrapolation)
        assert len(parsed["fitted_trend"]) == 100, \
            f"Expected 100 fitted values (full series), got {len(parsed['fitted_trend'])}"

    def test_recency_fraction(self):
        """RECENCY FRACTION f uses the last fraction of data."""
        key = "test:trend:recency:fraction"
        create_linear_series(self.client, key, count=100)

        result = self.client.execute_command(
            "TS.TREND", key, "-", "+",
            "RECENCY", "FRACTION", "0.5"
        )

        parsed = parse_trend_response(result)
        assert len(parsed["fitted_trend"]) == 100, \
            f"Expected 100 fitted values, got {len(parsed['fitted_trend'])}"

    def test_recency_default_is_fraction_0_3(self):
        """Default recency is FRACTION 0.3 (when RECENCY not specified)."""
        key = "test:trend:recency:default"
        create_linear_series(self.client, key, count=100)

        result = self.client.execute_command(
            "TS.TREND", key, "-", "+"
        )

        parsed = parse_trend_response(result)
        assert len(parsed["fitted_trend"]) == 100, \
            f"Expected 100 fitted values, got {len(parsed['fitted_trend'])}"

    # ═══════════════════════════════════════════════════════════════════════
    # Negative data excludes exponential
    # ═══════════════════════════════════════════════════════════════════════

    def test_negative_data_excludes_exponential(self):
        """Negative data → Exponential should not appear in scores.

        Corresponds to Rust: negative_data_excludes_exponential
        """
        key = "test:trend:negative:exponential"
        create_negative_linear_series(self.client, key, count=100)

        result = self.client.execute_command(
            "TS.TREND", key, "-", "+", "RECENCY", "FULL"
        )

        parsed = parse_trend_response(result)
        scores = parsed["scores"]
        names = [name for name, _ in scores]
        assert "Exponential" not in names, \
            f"Exponential should not be a candidate for negative data, got: {names}"

    # ═══════════════════════════════════════════════════════════════════════
    # Response format validation
    # ═══════════════════════════════════════════════════════════════════════

    def test_response_is_flat_map(self):
        """Response should be a flat key-value list with even length."""
        key = "test:trend:format:flat"
        create_linear_series(self.client, key, count=100)

        result = self.client.execute_command(
            "TS.TREND", key, "-", "+", "RECENCY", "FULL"
        )

        assert isinstance(result, list), \
            f"Expected list response, got {type(result)}"
        assert len(result) % 2 == 0, \
            f"Expected even-length flat map, got {len(result)} elements"

    def test_response_required_fields(self):
        """Response must contain selected_series, criterion, fitted_trend,
        scores, and n_params."""
        key = "test:trend:format:required"
        create_linear_series(self.client, key, count=100)

        result = self.client.execute_command(
            "TS.TREND", key, "-", "+", "RECENCY", "FULL"
        )

        parsed = parse_trend_response(result)
        required = ["selected_series", "criterion", "fitted_trend",
                     "scores", "n_params"]
        for field in required:
            assert field in parsed, \
                f"Missing required field '{field}' in response: {list(parsed.keys())}"

    def test_response_selected_series_is_string(self):
        """selected_series should be a non-empty string."""
        key = "test:trend:format:selected_str"
        create_linear_series(self.client, key, count=100)

        result = self.client.execute_command(
            "TS.TREND", key, "-", "+", "RECENCY", "FULL"
        )

        parsed = parse_trend_response(result)
        selected = parsed["selected_series"]
        assert isinstance(selected, str) and len(selected) > 0, \
            f"selected_series should be non-empty string, got {selected!r}"

    def test_response_with_predict_features_and_metrics(self):
        """When PREDICT, FEATURES, and METRICS are specified, all optional fields appear."""
        key = "test:trend:format:all_fields"
        create_linear_series(self.client, key, count=100,
                              slope=2.0, intercept=1.0)

        result = self.client.execute_command(
            "TS.TREND", key, "-", "+",
            "RECENCY", "FULL", "PREDICT", "5", "FEATURES", "METRICS"
        )

        parsed = parse_trend_response(result)
        assert "predicted_trend" in parsed, \
            f"Missing predicted_trend when PREDICT specified"
        assert len(parsed["predicted_trend"]) == 5
        assert "features" in parsed, \
            f"Missing features when FEATURES specified"
        assert len(parsed["features"]) > 0
        assert "metrics" in parsed, \
            f"Missing metrics when METRICS specified"
        assert "mae" in parsed["metrics"]

    # ═══════════════════════════════════════════════════════════════════════
    # Error handling
    # ═══════════════════════════════════════════════════════════════════════

    def test_error_nonexistent_key(self):
        """Error on non-existent key."""
        with pytest.raises(ResponseError, match="the key does not exist"):
            self.client.execute_command(
                "TS.TREND", "nonexistent_key", "-", "+"
            )

    def test_error_insufficient_data(self):
        """Error with fewer than 4 data points."""
        key = "test:trend:err:few_points"
        self.client.execute_command("TS.CREATE", key)
        self.client.execute_command("TS.ADD", key, 1000, 1.0)
        self.client.execute_command("TS.ADD", key, 2000, 2.0)
        self.client.execute_command("TS.ADD", key, 3000, 3.0)

        with pytest.raises(ResponseError, match="insufficient data"):
            self.client.execute_command(
                "TS.TREND", key, "-", "+"
            )

    def test_error_wrong_arity(self):
        """Error with too few arguments."""
        with pytest.raises(ResponseError):
            self.client.execute_command("TS.TREND", "key")

    def test_error_invalid_model_value(self):
        """Error with an invalid criterion value after MODEL Auto."""
        key = "test:trend:err:bad_model_value"
        create_linear_series(self.client, key, count=100)

        with pytest.raises(ResponseError, match="Invalid MODEL"):
            self.client.execute_command(
                "TS.TREND", key, "-", "+",
                "MODEL", "Auto", "INVALID"
            )

    def test_error_invalid_recency(self):
        """Error with an unknown recency value."""
        key = "test:trend:err:bad_recency"
        create_linear_series(self.client, key, count=100)

        with pytest.raises(ResponseError, match="invalid RECENCY"):
            self.client.execute_command(
                "TS.TREND", key, "-", "+",
                "RECENCY", "INVALID"
            )

    def test_error_window_too_small(self):
        """Error when RECENCY WINDOW is less than 4."""
        key = "test:trend:err:small_window"
        create_linear_series(self.client, key, count=100)

        with pytest.raises(ResponseError, match="WINDOW must be at least 4"):
            self.client.execute_command(
                "TS.TREND", key, "-", "+",
                "RECENCY", "WINDOW", "3"
            )

    def test_error_fraction_invalid(self):
        """Error when RECENCY FRACTION is <= 0 or > 1."""
        key = "test:trend:err:bad_fraction"
        create_linear_series(self.client, key, count=100)

        with pytest.raises(ResponseError, match="FRACTION must be between 0 and 1"):
            self.client.execute_command(
                "TS.TREND", key, "-", "+",
                "RECENCY", "FRACTION", "0"
            )

    def test_error_predict_zero_or_negative(self):
        """Error when PREDICT is 0 or negative."""
        key = "test:trend:err:predict_zero"
        create_linear_series(self.client, key, count=100)

        with pytest.raises(ResponseError, match="PREDICT must be greater than 0"):
            self.client.execute_command(
                "TS.TREND", key, "-", "+",
                "PREDICT", "0"
            )

    def test_error_missing_predict_value(self):
        """Error when PREDICT has no value."""
        key = "test:trend:err:predict_missing"
        create_linear_series(self.client, key, count=100)

        with pytest.raises(ResponseError, match="Missing value for PREDICT"):
            self.client.execute_command(
                "TS.TREND", key, "-", "+",
                "PREDICT"
            )

    def test_error_missing_window_value(self):
        """Error when RECENCY WINDOW has no window size."""
        key = "test:trend:err:window_missing"
        create_linear_series(self.client, key, count=100)

        with pytest.raises(ResponseError, match="Missing window size for RECENCY WINDOW"):
            self.client.execute_command(
                "TS.TREND", key, "-", "+",
                "RECENCY", "WINDOW"
            )

    def test_error_missing_store_value(self):
        """Error when STORE has no value."""
        key = "test:trend:err:store_missing"
        create_linear_series(self.client, key, count=100)

        with pytest.raises(ResponseError, match="Missing value for STORE"):
            self.client.execute_command(
                "TS.TREND", key, "-", "+",
                "RECENCY", "FULL", "STORE"
            )

    def test_error_unknown_argument(self):
        """Error with an unknown argument."""
        key = "test:trend:err:unknown_arg"
        create_linear_series(self.client, key, count=100)

        with pytest.raises(ResponseError, match="Unknown argument"):
            self.client.execute_command(
                "TS.TREND", key, "-", "+",
                "INVALID_OPTION", "value"
            )

    # ═══════════════════════════════════════════════════════════════════════
    # Edge cases
    # ═══════════════════════════════════════════════════════════════════════

    def test_minimum_data_points(self):
        """Test with exactly 4 data points (minimum required)."""
        key = "test:trend:edge:min_points"
        values = [10.0, 12.0, 14.0, 16.0]
        _add(self.client, key, 1000, values)

        result = self.client.execute_command(
            "TS.TREND", key, "-", "+", "RECENCY", "FULL"
        )

        parsed = parse_trend_response(result)
        assert len(parsed["fitted_trend"]) == 4, \
            f"Expected 4 fitted values, got {len(parsed['fitted_trend'])}"

    def test_large_dataset(self):
        """Test with a larger dataset to ensure no performance issues."""
        key = "test:trend:edge:large"
        create_linear_series(self.client, key, count=500,
                              slope=1.0, intercept=0.0)

        result = self.client.execute_command(
            "TS.TREND", key, "-", "+", "RECENCY", "FULL"
        )

        parsed = parse_trend_response(result)
        assert len(parsed["fitted_trend"]) == 500
        assert len(parsed["scores"]) >= 4

    def test_constant_series(self):
        """Test on a constant series (all values equal)."""
        key = "test:trend:edge:constant"
        _add(self.client, key, 1000, [5.0] * 50)

        result = self.client.execute_command(
            "TS.TREND", key, "-", "+", "RECENCY", "FULL"
        )

        parsed = parse_trend_response(result)
        assert len(parsed["fitted_trend"]) == 50
        # Fitted values should be approximately constant
        for v in parsed["fitted_trend"]:
            assert not math.isnan(v), f"Fitted value is NaN"
            assert not math.isinf(v), f"Fitted value is Inf"

    def test_time_range_filter(self):
        """Test trend analysis using only a subset of the time range."""
        key = "test:trend:edge:range"
        values = [2.0 * i + 1.0 for i in range(200)]
        _add(self.client, key, 1000, values)

        # Use the middle portion of the data (indices 50..149)
        from_ts = 1000 + 50 * 1000
        to_ts = 1000 + 149 * 1000

        result = self.client.execute_command(
            "TS.TREND", key, str(from_ts), str(to_ts),
            "RECENCY", "FULL"
        )

        parsed = parse_trend_response(result)
        # Should have 100 fitted values (indices 50..149 inclusive)
        assert len(parsed["fitted_trend"]) == 100, \
            f"Expected 100 fitted values, got {len(parsed['fitted_trend'])}"

    def test_aicc_and_bic_produce_different_scores(self):
        """AICc and BIC should produce different score values for the same data."""
        key = "test:trend:edge:aicc_vs_bic"
        create_linear_series(self.client, key, count=100,
                              slope=2.0, intercept=1.0)

        result_aicc = self.client.execute_command(
            "TS.TREND", key, "-", "+",
            "MODEL", "Auto", "AICc", "RECENCY", "FULL"
        )
        result_bic = self.client.execute_command(
            "TS.TREND", key, "-", "+",
            "MODEL", "Auto", "BIC", "RECENCY", "FULL"
        )

        parsed_aicc = parse_trend_response(result_aicc)
        parsed_bic = parse_trend_response(result_bic)

        # BIC penalizes complexity more — scores should differ
        aicc_scores = {name: score for name, score in parsed_aicc["scores"]}
        bic_scores = {name: score for name, score in parsed_bic["scores"]}

        # At least one score should differ between AICc and BIC
        any_diff = False
        for name in aicc_scores:
            if name in bic_scores:
                if abs(aicc_scores[name] - bic_scores[name]) > 0.001:
                    any_diff = True
                    break
        assert any_diff, \
            f"AICc and BIC scores should differ, aicc: {aicc_scores}, bic: {bic_scores}"

    # ═══════════════════════════════════════════════════════════════════════
    # MODEL argument
    # ═══════════════════════════════════════════════════════════════════════

    def test_model_auto_explicit(self):
        """MODEL Auto (explicit) behaves like the default and includes selected_series."""
        key = "test:trend:model:auto_explicit"
        create_linear_series(self.client, key, count=100,
                              slope=2.0, intercept=1.0)

        result = self.client.execute_command(
            "TS.TREND", key, "-", "+",
            "MODEL", "Auto", "RECENCY", "FULL"
        )

        parsed = parse_trend_response(result)
        assert "selected_series" in parsed, \
            f"Expected selected_series in Auto mode, got keys: {list(parsed.keys())}"
        assert "criterion" in parsed
        assert "scores" in parsed
        assert len(parsed["scores"]) >= 4

    def test_model_auto_with_inline_criterion(self):
        """MODEL Auto BIC sets criterion via the inline syntax."""
        key = "test:trend:model:auto_inline_criterion"
        create_linear_series(self.client, key, count=100,
                              slope=2.0, intercept=1.0)

        result = self.client.execute_command(
            "TS.TREND", key, "-", "+",
            "MODEL", "Auto", "BIC", "RECENCY", "FULL"
        )

        parsed = parse_trend_response(result)
        assert parsed["criterion"] == "BIC", \
            f"Expected criterion BIC, got {parsed['criterion']}"

    def test_model_auto_with_inline_holdout(self):
        """MODEL Auto HOLDOUT sets criterion via the inline syntax."""
        key = "test:trend:model:auto_inline_holdout"
        create_linear_series(self.client, key, count=100,
                              slope=2.0, intercept=1.0)

        result = self.client.execute_command(
            "TS.TREND", key, "-", "+",
            "MODEL", "Auto", "HOLDOUT", "RECENCY", "FULL"
        )

        parsed = parse_trend_response(result)
        assert parsed["criterion"] == "HOLDOUT", \
            f"Expected criterion HOLDOUT, got {parsed['criterion']}"

    def test_model_exponential(self):
        """MODEL Exponential fits only an exponential trend."""
        key = "test:trend:model:exponential"
        create_exponential_series(self.client, key, count=100)

        result = self.client.execute_command(
            "TS.TREND", key, "-", "+",
            "MODEL", "Exponential", "RECENCY", "FULL"
        )

        parsed = parse_trend_response(result)
        assert parsed["model"] == "Exponential", \
            f"Expected model Exponential, got {parsed['model']}"
        assert len(parsed["fitted_trend"]) == 100
        assert "n_params" in parsed
        # Specific model mode should NOT have selected_series, criterion, or scores
        assert "selected_series" not in parsed, \
            "selected_series should not be present in specific model mode"
        assert "criterion" not in parsed, \
            "criterion should not be present in specific model mode"
        assert "scores" not in parsed, \
            "scores should not be present in specific model mode"

    def test_model_logistic(self):
        """MODEL Logistic fits a logistic (S-curve) trend."""
        key = "test:trend:model:logistic"
        # Generate logistic-like data
        values = [100.0 / (1.0 + math.exp(-0.1 * (i - 50.0))) for i in range(100)]
        _add(self.client, key, 1000, values)

        result = self.client.execute_command(
            "TS.TREND", key, "-", "+",
            "MODEL", "Logistic", "RECENCY", "FULL"
        )

        parsed = parse_trend_response(result)
        assert parsed["model"] == "Logistic", \
            f"Expected model Logistic, got {parsed['model']}"
        assert len(parsed["fitted_trend"]) == 100
        assert "n_params" in parsed
        # Check fitted values are reasonable (no NaN/Inf)
        for v in parsed["fitted_trend"]:
            assert not math.isnan(v), f"Fitted value is NaN"
            assert not math.isinf(v), f"Fitted value is Inf"

    def test_model_polynomial(self):
        """MODEL Polynomial fits a quadratic trend."""
        key = "test:trend:model:polynomial"
        create_quadratic_series(self.client, key, count=100)

        result = self.client.execute_command(
            "TS.TREND", key, "-", "+",
            "MODEL", "Polynomial", "RECENCY", "FULL"
        )

        parsed = parse_trend_response(result)
        assert parsed["model"] == "Polynomial", \
            f"Expected model Polynomial, got {parsed['model']}"
        assert len(parsed["fitted_trend"]) == 100
        assert "n_params" in parsed

        # Fitted values should closely match quadratic data
        values = [0.01 * (i * i) + 0.5 * i + 10.0 for i in range(100)]
        fitted = parsed["fitted_trend"]
        for i, (f, v) in enumerate(zip(fitted, values)):
            assert abs(f - v) < 2.0, \
                f"At index {i}: fitted={f} original={v} diff={abs(f - v)} exceeds 2.0"

    def test_model_theilsen(self):
        """MODEL TheilSen fits a robust Theil-Sen linear trend."""
        key = "test:trend:model:theilsen"
        create_linear_series(self.client, key, count=100,
                              slope=2.0, intercept=1.0)

        result = self.client.execute_command(
            "TS.TREND", key, "-", "+",
            "MODEL", "TheilSen", "RECENCY", "FULL"
        )

        parsed = parse_trend_response(result)
        assert parsed["model"] == "TheilSen", \
            f"Expected model TheilSen, got {parsed['model']}"
        assert len(parsed["fitted_trend"]) == 100
        assert "n_params" in parsed

    def test_model_with_predict(self):
        """Specific MODEL with PREDICT returns predicted_trend."""
        key = "test:trend:model:with_predict"
        create_linear_series(self.client, key, count=100,
                              slope=2.0, intercept=1.0)

        result = self.client.execute_command(
            "TS.TREND", key, "-", "+",
            "MODEL", "TheilSen", "RECENCY", "FULL", "PREDICT", "5"
        )

        parsed = parse_trend_response(result)
        assert "predicted_trend" in parsed, \
            f"Expected predicted_trend with PREDICT, got keys: {list(parsed.keys())}"
        assert len(parsed["predicted_trend"]) == 5

    def test_model_with_features(self):
        """Specific MODEL with FEATURES returns features map."""
        key = "test:trend:model:with_features"
        create_linear_series(self.client, key, count=100,
                              slope=2.0, intercept=1.0)

        result = self.client.execute_command(
            "TS.TREND", key, "-", "+",
            "MODEL", "Exponential", "RECENCY", "FULL", "FEATURES"
        )

        parsed = parse_trend_response(result)
        assert "features" in parsed, \
            f"Expected features with FEATURES, got keys: {list(parsed.keys())}"
        assert len(parsed["features"]) > 0

    def test_model_with_store(self):
        """Specific MODEL with STORE persists fitted values."""
        key = "test:trend:model:store_src"
        store_key = "test:trend:model:store_dst"
        create_linear_series(self.client, key, count=50,
                              slope=2.0, intercept=1.0)

        result = self.client.execute_command(
            "TS.TREND", key, "-", "+",
            "MODEL", "Polynomial", "RECENCY", "FULL", "STORE", store_key
        )

        parsed = parse_trend_response(result)
        assert len(parsed["fitted_trend"]) == 50

        # Verify stored key
        assert self.client.execute_command("EXISTS", store_key) == 1
        stored = self.client.execute_command("TS.RANGE", store_key, "-", "+")
        assert len(stored) == 50

    def test_model_default_is_auto(self):
        """When no MODEL is specified, Auto is assumed (selected_series present)."""
        key = "test:trend:model:default_auto"
        create_linear_series(self.client, key, count=100,
                              slope=2.0, intercept=1.0)

        result = self.client.execute_command(
            "TS.TREND", key, "-", "+", "RECENCY", "FULL"
        )

        parsed = parse_trend_response(result)
        assert "selected_series" in parsed, \
            f"Default mode should have selected_series, got keys: {list(parsed.keys())}"
        assert "criterion" in parsed

    # ═══════════════════════════════════════════════════════════════════════
    # MODEL error handling
    # ═══════════════════════════════════════════════════════════════════════

    def test_error_invalid_model(self):
        """Error with an unknown MODEL value."""
        key = "test:trend:err:bad_model"
        create_linear_series(self.client, key, count=100)

        with pytest.raises(ResponseError, match="Invalid MODEL"):
            self.client.execute_command(
                "TS.TREND", key, "-", "+",
                "MODEL", "InvalidModel"
            )

    def test_error_criterion_without_auto_model(self):
        """Error when CRITERION is used as a standalone argument (no longer supported)."""
        key = "test:trend:err:criterion_standalone"
        create_linear_series(self.client, key, count=100)

        with pytest.raises(ResponseError, match="Unknown argument"):
            self.client.execute_command(
                "TS.TREND", key, "-", "+",
                "CRITERION", "AICc"
            )

    def test_error_missing_model_value(self):
        """Error when MODEL has no value."""
        key = "test:trend:err:model_missing"
        create_linear_series(self.client, key, count=100)

        with pytest.raises(ResponseError, match="Missing value for MODEL"):
            self.client.execute_command(
                "TS.TREND", key, "-", "+",
                "MODEL"
            )
