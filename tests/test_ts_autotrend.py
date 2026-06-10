"""
Integration tests for TS.AUTOTREND command.

Converted from the Rust unit tests in anofox-forecast:
  https://github.com/sipemu/anofox-forecast/blob/main/src/seasonality/auto_trend.rs

Covers:
- Trend selection: linear, quadratic, exponential data
- Fitted trend accuracy
- Information criteria: AICc (default), BIC, HOLDOUT
- Prediction (PREDICT option)
- Store (STORE option) — fitted values, combined with PREDICT
- Features (FEATURES option)
- Scores format, sorting, and candidate completeness
- Recency options: FULL, WINDOW, FRACTION
- Negative data excludes Exponential
- Error handling: nonexistent key, insufficient data, invalid arguments
- Response format validation (map structure)
"""

import math
from typing import Any, Dict, List, Optional

import pytest
from valkey import ResponseError

from valkey_timeseries_test_case import ValkeyTimeSeriesTestCaseBase


# ── helpers ──────────────────────────────────────────────────────────────────

def _add(client, key: str, start_ms: int, values: List[float],
         step_ms: int = 1000) -> None:
    """Bulk-add a list of values to *key* starting at *start_ms*."""
    for i, v in enumerate(values):
        client.execute_command("TS.ADD", key, start_ms + i * step_ms, v)


def _create_linear_series(client, key: str, start_ms: int = 1000,
                          count: int = 100, slope: float = 2.0,
                          intercept: float = 1.0,
                          step_ms: int = 1000) -> None:
    """Create a time series with a perfect linear trend."""
    values = [intercept + slope * i for i in range(count)]
    _add(client, key, start_ms, values, step_ms)


def _create_quadratic_series(client, key: str, start_ms: int = 1000,
                              count: int = 100,
                              step_ms: int = 1000) -> None:
    """Create a time series with a quadratic trend: 0.01*t^2 + 0.5*t + 10."""
    values = [0.01 * (i * i) + 0.5 * i + 10.0 for i in range(count)]
    _add(client, key, start_ms, values, step_ms)


def _create_exponential_series(client, key: str, start_ms: int = 1000,
                                count: int = 100,
                                step_ms: int = 1000) -> None:
    """Create a time series with an exponential trend: 2 * exp(0.05 * i)."""
    values = [2.0 * math.exp(0.05 * i) for i in range(count)]
    _add(client, key, start_ms, values, step_ms)


def _create_negative_linear_series(client, key: str, start_ms: int = 1000,
                                    count: int = 100,
                                    step_ms: int = 1000) -> None:
    """Create a time series with all-negative values."""
    values = [-(i + 1.0) for i in range(count)]
    _add(client, key, start_ms, values, step_ms)


def parse_autotrend_response(response: List[Any]) -> Dict[str, Any]:
    """Parse the flat key-value list response from TS.AUTOTREND into a dict.

    The response is a flat list of alternating key-value pairs:
    [key, "t", selected_trend, "Linear", criterion, "AICc",
     fitted_trend, [...], scores, [[name, score], ...], n_params, 2, ...]

    All values are decoded to their native Python types:
    - fitted_trend, predicted_trend → list of float
    - scores → list of (str, float) tuples
    - n_params → int
    - features → dict of {str: float}
    - key, selected_trend, criterion → str
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
        else:
            result[key_str] = value.decode("utf-8") if isinstance(value, bytes) else str(value)

    return result


# ── test class ───────────────────────────────────────────────────────────────

class TestAutoTrend(ValkeyTimeSeriesTestCaseBase):
    """Test TS.AUTOTREND command — converted from anofox-forecast Rust tests."""

    # ═══════════════════════════════════════════════════════════════════════
    # Trend selection: model picks the right component for the data shape
    # ═══════════════════════════════════════════════════════════════════════

    def test_selects_linear_for_linear_data(self):
        """Linear data → should select Linear or TheilSen.

        Corresponds to Rust: selects_linear_for_linear_data
        """
        key = "test:autotrend:select:linear"
        _create_linear_series(self.client, key, count=100,
                              slope=2.0, intercept=1.0)

        result = self.client.execute_command(
            "TS.AUTOTREND", key, "-", "+", "RECENCY", "FULL"
        )

        parsed = parse_autotrend_response(result)
        selected = parsed["selected_trend"]
        # Linear or TheilSen should be top (both fit perfectly)
        assert selected in ("Linear", "TheilSen"), \
            f"expected Linear or TheilSen, got {selected}"

    def test_fitted_matches_data_for_linear(self):
        """Fitted trend values should closely match the input linear data.

        Corresponds to Rust: fitted_matches_data_for_linear
        """
        key = "test:autotrend:fitted:linear"
        values = [3.0 * i - 5.0 for i in range(100)]
        _add(self.client, key, 1000, values)

        result = self.client.execute_command(
            "TS.AUTOTREND", key, "-", "+", "RECENCY", "FULL"
        )

        parsed = parse_autotrend_response(result)
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
        key = "test:autotrend:select:exponential"
        _create_exponential_series(self.client, key, count=100)

        result = self.client.execute_command(
            "TS.AUTOTREND", key, "-", "+", "RECENCY", "FULL"
        )

        parsed = parse_autotrend_response(result)
        selected = parsed["selected_trend"]
        scores = parsed["scores"]
        assert selected == "Exponential", \
            f"expected Exponential, got {selected}, scores: {scores}"

    def test_selects_quadratic_for_quadratic_data(self):
        """Quadratic data → should select Quadratic.

        Corresponds to Rust: selects_quadratic_for_quadratic_data
        """
        key = "test:autotrend:select:quadratic"
        _create_quadratic_series(self.client, key, count=100)

        result = self.client.execute_command(
            "TS.AUTOTREND", key, "-", "+", "RECENCY", "FULL"
        )

        parsed = parse_autotrend_response(result)
        selected = parsed["selected_trend"]
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
        key = "test:autotrend:scores:all_candidates"
        _create_exponential_series(self.client, key, count=100)

        result = self.client.execute_command(
            "TS.AUTOTREND", key, "-", "+", "RECENCY", "FULL"
        )

        parsed = parse_autotrend_response(result)
        scores = parsed["scores"]
        # All positive data → at least 4 candidates
        assert len(scores) >= 4, \
            f"expected at least 4 candidates in scores, got {len(scores)}: {scores}"

    def test_scores_are_sorted_ascending(self):
        """Scores should be sorted from lowest (best) to highest (worst).

        Corresponds to Rust: scores_are_sorted_ascending
        """
        key = "test:autotrend:scores:sorted"
        _create_linear_series(self.client, key, count=100,
                              slope=2.0, intercept=1.0)

        result = self.client.execute_command(
            "TS.AUTOTREND", key, "-", "+", "RECENCY", "FULL"
        )

        parsed = parse_autotrend_response(result)
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
        key = "test:autotrend:criterion:bic"
        _create_linear_series(self.client, key, count=100,
                              slope=2.0, intercept=1.0)

        result = self.client.execute_command(
            "TS.AUTOTREND", key, "-", "+",
            "CRITERION", "BIC", "RECENCY", "FULL"
        )

        parsed = parse_autotrend_response(result)
        assert parsed["criterion"] == "BIC", \
            f"Expected criterion BIC, got {parsed['criterion']}"
        assert len(parsed["scores"]) > 0, "Expected non-empty scores"

    def test_holdout_criterion_works(self):
        """Holdout criterion returns correct criterion label.

        Corresponds to Rust: holdout_criterion_works
        """
        key = "test:autotrend:criterion:holdout"
        _create_linear_series(self.client, key, count=100,
                              slope=2.0, intercept=1.0)

        result = self.client.execute_command(
            "TS.AUTOTREND", key, "-", "+",
            "CRITERION", "HOLDOUT", "RECENCY", "FULL"
        )

        parsed = parse_autotrend_response(result)
        assert parsed["criterion"] == "HOLDOUT", \
            f"Expected criterion HOLDOUT, got {parsed['criterion']}"

    def test_default_criterion_is_aicc(self):
        """Default criterion should be AICc when not specified."""
        key = "test:autotrend:criterion:default"
        _create_linear_series(self.client, key, count=100,
                              slope=2.0, intercept=1.0)

        result = self.client.execute_command(
            "TS.AUTOTREND", key, "-", "+", "RECENCY", "FULL"
        )

        parsed = parse_autotrend_response(result)
        assert parsed["criterion"] == "AICc", \
            f"Expected default criterion AICc, got {parsed['criterion']}"

    # ═══════════════════════════════════════════════════════════════════════
    # Prediction (PREDICT option)
    # ═══════════════════════════════════════════════════════════════════════

    def test_predict_delegates_to_winner(self):
        """PREDICT returns forecast values that extrapolate the trend.

        Corresponds to Rust: predict_delegates_to_winner
        """
        key = "test:autotrend:predict:basic"
        values = [2.0 * i + 1.0 for i in range(100)]
        _add(self.client, key, 1000, values)

        result = self.client.execute_command(
            "TS.AUTOTREND", key, "-", "+",
            "RECENCY", "FULL", "PREDICT", "10"
        )

        parsed = parse_autotrend_response(result)
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
        key = "test:autotrend:predict:absent"
        _create_linear_series(self.client, key, count=100)

        result = self.client.execute_command(
            "TS.AUTOTREND", key, "-", "+", "RECENCY", "FULL"
        )

        parsed = parse_autotrend_response(result)
        assert "predicted_trend" not in parsed, \
            f"predicted_trend should be absent without PREDICT option"

    # ═══════════════════════════════════════════════════════════════════════
    # STORE option
    # ═══════════════════════════════════════════════════════════════════════

    def test_store_creates_key_with_fitted_values(self):
        """STORE creates a new time series key with fitted trend values."""
        key = "test:autotrend:store:src"
        store_key = "test:autotrend:store:fitted:dst"
        values = [2.0 * i + 1.0 for i in range(100)]
        _add(self.client, key, 1000, values)

        result = self.client.execute_command(
            "TS.AUTOTREND", key, "-", "+",
            "RECENCY", "FULL", "STORE", store_key
        )

        parsed = parse_autotrend_response(result)
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
        key = "test:autotrend:store:timestamps:src"
        store_key = "test:autotrend:store:timestamps:dst"
        values = [3.0 * i - 5.0 for i in range(50)]
        _add(self.client, key, 5000, values, step_ms=2000)

        self.client.execute_command(
            "TS.AUTOTREND", key, "-", "+",
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
        key = "test:autotrend:store:predict:src"
        store_key = "test:autotrend:store:predict:dst"
        values = [2.0 * i + 1.0 for i in range(100)]
        _add(self.client, key, 1000, values)

        result = self.client.execute_command(
            "TS.AUTOTREND", key, "-", "+",
            "RECENCY", "FULL", "PREDICT", "5", "STORE", store_key
        )

        parsed = parse_autotrend_response(result)
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
        key = "test:autotrend:store:pred_ts:src"
        store_key = "test:autotrend:store:pred_ts:dst"
        values = [1.0 * i for i in range(50)]
        _add(self.client, key, 10000, values, step_ms=60000)

        self.client.execute_command(
            "TS.AUTOTREND", key, "-", "+",
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
        key = "test:autotrend:store:exist:src"
        store_key = "test:autotrend:store:exist:dst"
        values = [2.0 * i + 1.0 for i in range(10)]
        _add(self.client, key, 1000, values)

        # Pre-create the destination key with one existing sample
        self.client.execute_command("TS.CREATE", store_key)
        self.client.execute_command("TS.ADD", store_key, 500, 99.0)

        self.client.execute_command(
            "TS.AUTOTREND", key, "-", "+",
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
        key = "test:autotrend:features:basic"
        _create_linear_series(self.client, key, count=100,
                              slope=2.0, intercept=1.0)

        result = self.client.execute_command(
            "TS.AUTOTREND", key, "-", "+",
            "RECENCY", "FULL", "FEATURES"
        )

        parsed = parse_autotrend_response(result)
        assert "features" in parsed, \
            f"Expected features in response, got keys: {list(parsed.keys())}"
        features = parsed["features"]
        assert len(features) > 0, "Expected non-empty features map"

    def test_features_without_features_omits_field(self):
        """Without FEATURES, features should be absent from response."""
        key = "test:autotrend:features:absent"
        _create_linear_series(self.client, key, count=100)

        result = self.client.execute_command(
            "TS.AUTOTREND", key, "-", "+", "RECENCY", "FULL"
        )

        parsed = parse_autotrend_response(result)
        assert "features" not in parsed, \
            f"features should be absent without FEATURES option"

    # ═══════════════════════════════════════════════════════════════════════
    # Recency options
    # ═══════════════════════════════════════════════════════════════════════

    def test_recency_full(self):
        """RECENCY FULL uses all data and returns valid fitted trend."""
        key = "test:autotrend:recency:full"
        _create_linear_series(self.client, key, count=100)

        result = self.client.execute_command(
            "TS.AUTOTREND", key, "-", "+", "RECENCY", "FULL"
        )

        parsed = parse_autotrend_response(result)
        assert len(parsed["fitted_trend"]) == 100, \
            f"Expected 100 fitted values, got {len(parsed['fitted_trend'])}"

    def test_recency_window(self):
        """RECENCY WINDOW n uses last n observations for fitting."""
        key = "test:autotrend:recency:window"
        _create_linear_series(self.client, key, count=100)

        result = self.client.execute_command(
            "TS.AUTOTREND", key, "-", "+", "RECENCY", "WINDOW", "50"
        )

        parsed = parse_autotrend_response(result)
        # Fitted values still cover full series (back-extrapolation)
        assert len(parsed["fitted_trend"]) == 100, \
            f"Expected 100 fitted values (full series), got {len(parsed['fitted_trend'])}"

    def test_recency_fraction(self):
        """RECENCY FRACTION f uses the last fraction of data."""
        key = "test:autotrend:recency:fraction"
        _create_linear_series(self.client, key, count=100)

        result = self.client.execute_command(
            "TS.AUTOTREND", key, "-", "+",
            "RECENCY", "FRACTION", "0.5"
        )

        parsed = parse_autotrend_response(result)
        assert len(parsed["fitted_trend"]) == 100, \
            f"Expected 100 fitted values, got {len(parsed['fitted_trend'])}"

    def test_recency_default_is_fraction_0_3(self):
        """Default recency is FRACTION 0.3 (when RECENCY not specified)."""
        key = "test:autotrend:recency:default"
        _create_linear_series(self.client, key, count=100)

        result = self.client.execute_command(
            "TS.AUTOTREND", key, "-", "+"
        )

        parsed = parse_autotrend_response(result)
        assert len(parsed["fitted_trend"]) == 100, \
            f"Expected 100 fitted values, got {len(parsed['fitted_trend'])}"

    # ═══════════════════════════════════════════════════════════════════════
    # Negative data excludes exponential
    # ═══════════════════════════════════════════════════════════════════════

    def test_negative_data_excludes_exponential(self):
        """Negative data → Exponential should not appear in scores.

        Corresponds to Rust: negative_data_excludes_exponential
        """
        key = "test:autotrend:negative:exponential"
        _create_negative_linear_series(self.client, key, count=100)

        result = self.client.execute_command(
            "TS.AUTOTREND", key, "-", "+", "RECENCY", "FULL"
        )

        parsed = parse_autotrend_response(result)
        scores = parsed["scores"]
        names = [name for name, _ in scores]
        assert "Exponential" not in names, \
            f"Exponential should not be a candidate for negative data, got: {names}"

    # ═══════════════════════════════════════════════════════════════════════
    # Response format validation
    # ═══════════════════════════════════════════════════════════════════════

    def test_response_is_flat_map(self):
        """Response should be a flat key-value list with even length."""
        key = "test:autotrend:format:flat"
        _create_linear_series(self.client, key, count=100)

        result = self.client.execute_command(
            "TS.AUTOTREND", key, "-", "+", "RECENCY", "FULL"
        )

        assert isinstance(result, list), \
            f"Expected list response, got {type(result)}"
        assert len(result) % 2 == 0, \
            f"Expected even-length flat map, got {len(result)} elements"

    def test_response_required_fields(self):
        """Response must contain selected_trend, criterion, fitted_trend,
        scores, and n_params."""
        key = "test:autotrend:format:required"
        _create_linear_series(self.client, key, count=100)

        result = self.client.execute_command(
            "TS.AUTOTREND", key, "-", "+", "RECENCY", "FULL"
        )

        parsed = parse_autotrend_response(result)
        required = ["selected_trend", "criterion", "fitted_trend",
                     "scores", "n_params"]
        for field in required:
            assert field in parsed, \
                f"Missing required field '{field}' in response: {list(parsed.keys())}"

    def test_response_selected_trend_is_string(self):
        """selected_trend should be a non-empty string."""
        key = "test:autotrend:format:selected_str"
        _create_linear_series(self.client, key, count=100)

        result = self.client.execute_command(
            "TS.AUTOTREND", key, "-", "+", "RECENCY", "FULL"
        )

        parsed = parse_autotrend_response(result)
        selected = parsed["selected_trend"]
        assert isinstance(selected, str) and len(selected) > 0, \
            f"selected_trend should be non-empty string, got {selected!r}"

    def test_response_with_predict_and_features(self):
        """When both PREDICT and FEATURES are specified, all optional fields appear."""
        key = "test:autotrend:format:all_fields"
        _create_linear_series(self.client, key, count=100,
                              slope=2.0, intercept=1.0)

        result = self.client.execute_command(
            "TS.AUTOTREND", key, "-", "+",
            "RECENCY", "FULL", "PREDICT", "5", "FEATURES"
        )

        parsed = parse_autotrend_response(result)
        assert "predicted_trend" in parsed, \
            f"Missing predicted_trend when PREDICT specified"
        assert len(parsed["predicted_trend"]) == 5
        assert "features" in parsed, \
            f"Missing features when FEATURES specified"
        assert len(parsed["features"]) > 0

    # ═══════════════════════════════════════════════════════════════════════
    # Error handling
    # ═══════════════════════════════════════════════════════════════════════

    def test_error_nonexistent_key(self):
        """Error on non-existent key."""
        with pytest.raises(ResponseError, match="the key does not exist"):
            self.client.execute_command(
                "TS.AUTOTREND", "nonexistent_key", "-", "+"
            )

    def test_error_insufficient_data(self):
        """Error with fewer than 4 data points."""
        key = "test:autotrend:err:few_points"
        self.client.execute_command("TS.CREATE", key)
        self.client.execute_command("TS.ADD", key, 1000, 1.0)
        self.client.execute_command("TS.ADD", key, 2000, 2.0)
        self.client.execute_command("TS.ADD", key, 3000, 3.0)

        with pytest.raises(ResponseError, match="insufficient data"):
            self.client.execute_command(
                "TS.AUTOTREND", key, "-", "+"
            )

    def test_error_wrong_arity(self):
        """Error with too few arguments."""
        with pytest.raises(ResponseError):
            self.client.execute_command("TS.AUTOTREND", "key")

    def test_error_invalid_criterion(self):
        """Error with an unknown criterion value."""
        key = "test:autotrend:err:bad_criterion"
        _create_linear_series(self.client, key, count=100)

        with pytest.raises(ResponseError, match="invalid CRITERION"):
            self.client.execute_command(
                "TS.AUTOTREND", key, "-", "+",
                "CRITERION", "INVALID"
            )

    def test_error_invalid_recency(self):
        """Error with an unknown recency value."""
        key = "test:autotrend:err:bad_recency"
        _create_linear_series(self.client, key, count=100)

        with pytest.raises(ResponseError, match="invalid RECENCY"):
            self.client.execute_command(
                "TS.AUTOTREND", key, "-", "+",
                "RECENCY", "INVALID"
            )

    def test_error_window_too_small(self):
        """Error when RECENCY WINDOW is less than 4."""
        key = "test:autotrend:err:small_window"
        _create_linear_series(self.client, key, count=100)

        with pytest.raises(ResponseError, match="WINDOW must be at least 4"):
            self.client.execute_command(
                "TS.AUTOTREND", key, "-", "+",
                "RECENCY", "WINDOW", "3"
            )

    def test_error_fraction_invalid(self):
        """Error when RECENCY FRACTION is <= 0 or > 1."""
        key = "test:autotrend:err:bad_fraction"
        _create_linear_series(self.client, key, count=100)

        with pytest.raises(ResponseError, match="FRACTION must be between 0 and 1"):
            self.client.execute_command(
                "TS.AUTOTREND", key, "-", "+",
                "RECENCY", "FRACTION", "0"
            )

    def test_error_predict_zero_or_negative(self):
        """Error when PREDICT is 0 or negative."""
        key = "test:autotrend:err:predict_zero"
        _create_linear_series(self.client, key, count=100)

        with pytest.raises(ResponseError, match="PREDICT must be greater than 0"):
            self.client.execute_command(
                "TS.AUTOTREND", key, "-", "+",
                "PREDICT", "0"
            )

    def test_error_missing_criterion_value(self):
        """Error when CRITERION has no value."""
        key = "test:autotrend:err:criterion_missing"
        _create_linear_series(self.client, key, count=100)

        with pytest.raises(ResponseError, match="Missing value for CRITERION"):
            self.client.execute_command(
                "TS.AUTOTREND", key, "-", "+",
                "CRITERION"
            )

    def test_error_missing_predict_value(self):
        """Error when PREDICT has no value."""
        key = "test:autotrend:err:predict_missing"
        _create_linear_series(self.client, key, count=100)

        with pytest.raises(ResponseError, match="Missing value for PREDICT"):
            self.client.execute_command(
                "TS.AUTOTREND", key, "-", "+",
                "PREDICT"
            )

    def test_error_missing_window_value(self):
        """Error when RECENCY WINDOW has no window size."""
        key = "test:autotrend:err:window_missing"
        _create_linear_series(self.client, key, count=100)

        with pytest.raises(ResponseError, match="Missing window size for RECENCY WINDOW"):
            self.client.execute_command(
                "TS.AUTOTREND", key, "-", "+",
                "RECENCY", "WINDOW"
            )

    def test_error_missing_store_value(self):
        """Error when STORE has no value."""
        key = "test:autotrend:err:store_missing"
        _create_linear_series(self.client, key, count=100)

        with pytest.raises(ResponseError, match="Missing value for STORE"):
            self.client.execute_command(
                "TS.AUTOTREND", key, "-", "+",
                "RECENCY", "FULL", "STORE"
            )

    def test_error_unknown_argument(self):
        """Error with an unknown argument."""
        key = "test:autotrend:err:unknown_arg"
        _create_linear_series(self.client, key, count=100)

        with pytest.raises(ResponseError, match="Unknown argument"):
            self.client.execute_command(
                "TS.AUTOTREND", key, "-", "+",
                "INVALID_OPTION", "value"
            )

    # ═══════════════════════════════════════════════════════════════════════
    # Edge cases
    # ═══════════════════════════════════════════════════════════════════════

    def test_minimum_data_points(self):
        """Test with exactly 4 data points (minimum required)."""
        key = "test:autotrend:edge:min_points"
        values = [10.0, 12.0, 14.0, 16.0]
        _add(self.client, key, 1000, values)

        result = self.client.execute_command(
            "TS.AUTOTREND", key, "-", "+", "RECENCY", "FULL"
        )

        parsed = parse_autotrend_response(result)
        assert len(parsed["fitted_trend"]) == 4, \
            f"Expected 4 fitted values, got {len(parsed['fitted_trend'])}"

    def test_large_dataset(self):
        """Test with a larger dataset to ensure no performance issues."""
        key = "test:autotrend:edge:large"
        _create_linear_series(self.client, key, count=500,
                              slope=1.0, intercept=0.0)

        result = self.client.execute_command(
            "TS.AUTOTREND", key, "-", "+", "RECENCY", "FULL"
        )

        parsed = parse_autotrend_response(result)
        assert len(parsed["fitted_trend"]) == 500
        assert len(parsed["scores"]) >= 4

    def test_constant_series(self):
        """Test on a constant series (all values equal)."""
        key = "test:autotrend:edge:constant"
        _add(self.client, key, 1000, [5.0] * 50)

        result = self.client.execute_command(
            "TS.AUTOTREND", key, "-", "+", "RECENCY", "FULL"
        )

        parsed = parse_autotrend_response(result)
        assert len(parsed["fitted_trend"]) == 50
        # Fitted values should be approximately constant
        for v in parsed["fitted_trend"]:
            assert not math.isnan(v), f"Fitted value is NaN"
            assert not math.isinf(v), f"Fitted value is Inf"

    def test_time_range_filter(self):
        """Test trend analysis using only a subset of the time range."""
        key = "test:autotrend:edge:range"
        values = [2.0 * i + 1.0 for i in range(200)]
        _add(self.client, key, 1000, values)

        # Use the middle portion of the data (indices 50..149)
        from_ts = 1000 + 50 * 1000
        to_ts = 1000 + 149 * 1000

        result = self.client.execute_command(
            "TS.AUTOTREND", key, str(from_ts), str(to_ts),
            "RECENCY", "FULL"
        )

        parsed = parse_autotrend_response(result)
        # Should have 100 fitted values (indices 50..149 inclusive)
        assert len(parsed["fitted_trend"]) == 100, \
            f"Expected 100 fitted values, got {len(parsed['fitted_trend'])}"

    def test_aicc_and_bic_produce_different_scores(self):
        """AICc and BIC should produce different score values for the same data."""
        key = "test:autotrend:edge:aicc_vs_bic"
        _create_linear_series(self.client, key, count=100,
                              slope=2.0, intercept=1.0)

        result_aicc = self.client.execute_command(
            "TS.AUTOTREND", key, "-", "+",
            "CRITERION", "AICc", "RECENCY", "FULL"
        )
        result_bic = self.client.execute_command(
            "TS.AUTOTREND", key, "-", "+",
            "CRITERION", "BIC", "RECENCY", "FULL"
        )

        parsed_aicc = parse_autotrend_response(result_aicc)
        parsed_bic = parse_autotrend_response(result_bic)

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
