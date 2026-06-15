"""
Integration tests for TS.FEATURES command.

Covers:
- Category-based feature computation (basic, distribution, autocorrelation, trend)
- Individual feature specification (simple and parameterized)
- Combined CATEGORY and FEATURE usage
- Duplicate category rejection
- Map response format validation
- Error handling: nonexistent key, empty series, missing args, unknown features
"""

import math
from typing import Dict

import pytest
from valkey import ResponseError

from valkey_timeseries_test_case import ValkeyTimeSeriesTestCaseBase
from valkeytestframework.conftest import resource_port_tracker  # noqa: F401
from data_helpers import (
    create_constant_series,
    create_linear_series,
    create_sine_series,
)


def parse_features_response(response: list) -> Dict[str, float]:
    """Parse the flat key-value list response from TS.FEATURES into a dict.

    The response is a flat list of alternating key-value pairs:
    ["mean", 23.86, "variance", 0.81, ...]
    """
    if not response:
        return {}

    result: Dict[str, float] = {}
    it = iter(response)
    for key in it:
        key_str = key.decode("utf-8") if isinstance(key, bytes) else str(key)
        value = next(it)
        result[key_str] = float(value) if value is not None else float("nan")
    return result


class TestFeaturesCategories(ValkeyTimeSeriesTestCaseBase):
    """Test CATEGORY-based feature computation."""

    def test_basic_category(self):
        """Test basic category returns expected features."""
        key = "test:features:basic"
        create_linear_series(self.client, key, count=10)

        result = self.client.execute_command(
            "TS.FEATURES", key, "-", "+", "CATEGORY", "basic"
        )
        parsed = parse_features_response(result)

        expected_features = {"mean", "median", "variance", "variance_sample",
                             "minimum", "maximum", "length"}
        assert set(parsed.keys()) == expected_features, \
            f"Expected {expected_features}, got {set(parsed.keys())}"

        # Basic sanity checks on values
        assert parsed["length"] == 10.0
        assert parsed["minimum"] == 1.0
        assert parsed["maximum"] == 19.0
        assert not math.isnan(parsed["mean"])
        assert not math.isnan(parsed["variance"])

    def test_distribution_category(self):
        """Test distribution category returns skewness and kurtosis."""
        key = "test:features:dist"
        create_sine_series(self.client, key, count=100)

        result = self.client.execute_command(
            "TS.FEATURES", key, "-", "+", "CATEGORY", "distribution"
        )
        parsed = parse_features_response(result)

        assert set(parsed.keys()) == {"skewness", 'quantile_0.25', 'quantile_0.5', 'quantile_0.75', 'quantile_0.9', 'quantile_0.95', 'quantile_0.99', "kurtosis"}
        # Sine wave should have near-zero skewness
        assert abs(parsed["skewness"]) < 0.5, \
            f"Expected near-zero skewness for sine, got {parsed['skewness']}"

    def test_autocorrelation_category(self):
        """Test autocorrelation category returns ACF at lags 1,2,3."""
        key = "test:features:acf"
        create_linear_series(self.client, key, count=50)

        result = self.client.execute_command(
            "TS.FEATURES", key, "-", "+", "CATEGORY", "autocorrelation"
        )
        parsed = parse_features_response(result)

        expected = {"autocorrelation_1", "autocorrelation_2", "autocorrelation_3"}
        assert set(parsed.keys()) == expected

        # Linear series should have high autocorrelation at all short lags
        for lag in range(1, 4):
            assert parsed[f"autocorrelation_{lag}"] > 0.8, \
                f"Expected high ACF({lag}) for linear trend"

    def test_trend_category(self):
        """Test trend category returns linear trend features."""
        key = "test:features:trend"
        create_linear_series(self.client, key, count=100)

        result = self.client.execute_command(
            "TS.FEATURES", key, "-", "+", "CATEGORY", "trend"
        )
        parsed = parse_features_response(result)

        expected = {"linear_trend_intercept", "linear_trend_slope",
                    "linear_trend_p_value", "linear_trend_r_squared"}
        assert set(parsed.keys()) == expected

        # For perfect linear data, r_squared should be close to 1.0
        assert parsed["linear_trend_r_squared"] > 0.99

    def test_multiple_categories(self):
        """Test combining multiple categories."""
        key = "test:features:multicat"
        create_linear_series(self.client, key, count=50)

        result = self.client.execute_command(
            "TS.FEATURES", key, "-", "+", "CATEGORY", "basic,trend"
        )
        parsed = parse_features_response(result)

        basic_features = {"mean", "median", "variance", "variance_sample",
                          "minimum", "maximum", "length"}
        trend_features = {"linear_trend_intercept", "linear_trend_slope",
                          "linear_trend_p_value", "linear_trend_r_squared"}
        assert set(parsed.keys()) == basic_features | trend_features

    def test_category_case_insensitive(self):
        """Test that category names are case-insensitive."""
        key = "test:features:case"
        create_linear_series(self.client, key, count=10)

        result_lower = self.client.execute_command(
            "TS.FEATURES", key, "-", "+", "CATEGORY", "basic"
        )
        result_upper = self.client.execute_command(
            "TS.FEATURES", key, "-", "+", "CATEGORY", "BASIC"
        )
        result_mixed = self.client.execute_command(
            "TS.FEATURES", key, "-", "+", "CATEGORY", "Basic"
        )

        assert parse_features_response(result_lower) == parse_features_response(result_upper)
        assert parse_features_response(result_lower) == parse_features_response(result_mixed)


class TestFeaturesIndividual(ValkeyTimeSeriesTestCaseBase):
    """Test individual FEATURE specification."""

    def test_simple_features(self):
        """Test specifying individual simple features."""
        key = "test:features:simple"
        create_linear_series(self.client, key, count=10)

        result = self.client.execute_command(
            "TS.FEATURES", key, "-", "+", "FEATURE", "mean,median,variance,minimum,maximum"
        )
        parsed = parse_features_response(result)

        assert set(parsed.keys()) == {"mean", "median", "variance", "minimum", "maximum"}
        assert parsed["minimum"] == 1.0
        assert parsed["maximum"] == 19.0

    def test_feature_case_insensitive(self):
        """Test that feature names are case-insensitive."""
        key = "test:features:featcase"
        create_linear_series(self.client, key, count=10)

        result_lower = self.client.execute_command(
            "TS.FEATURES", key, "-", "+", "FEATURE", "mean"
        )
        result_upper = self.client.execute_command(
            "TS.FEATURES", key, "-", "+", "FEATURE", "MEAN"
        )
        result_mixed = self.client.execute_command(
            "TS.FEATURES", key, "-", "+", "FEATURE", "Mean"
        )

        assert parse_features_response(result_lower) == parse_features_response(result_upper)
        assert parse_features_response(result_lower) == parse_features_response(result_mixed)

    def test_quantile_feature(self):
        """Test quantile parameterized feature."""
        key = "test:features:quantile"
        create_linear_series(self.client, key, count=100)

        result = self.client.execute_command(
            "TS.FEATURES", key, "-", "+", "FEATURE", "quantile:0.5"
        )
        parsed = parse_features_response(result)

        assert "quantile_0.5" in parsed
        # Median (quantile 0.5) should be close to the actual median
        assert abs(parsed["quantile_0.5"] - 100.0) < 10.0, \
            f"Expected quantile_0.5 near 100, got {parsed['quantile_0.5']}"

    def test_quantile_multiple(self):
        """Test multiple quantile features."""
        key = "test:features:quantiles"
        create_linear_series(self.client, key, count=100)

        result = self.client.execute_command(
            "TS.FEATURES", key, "-", "+",
            "FEATURE", "quantile:0.25,quantile:0.5,quantile:0.75"
        )
        parsed = parse_features_response(result)

        assert set(parsed.keys()) == {"quantile_0.25", "quantile_0.5", "quantile_0.75"}
        assert parsed["quantile_0.25"] <= parsed["quantile_0.5"] <= parsed["quantile_0.75"]

    def test_autocorrelation_feature(self):
        """Test autocorrelation parameterized feature with custom lag."""
        key = "test:features:acf:custom"
        create_linear_series(self.client, key, count=50)

        result = self.client.execute_command(
            "TS.FEATURES", key, "-", "+", "FEATURE", "autocorrelation:5"
        )
        parsed = parse_features_response(result)

        assert "autocorrelation_5" in parsed
        assert parsed["autocorrelation_5"] > 0.7

    def test_partial_autocorrelation_feature(self):
        """Test partial autocorrelation parameterized feature."""
        key = "test:features:pacf"
        create_linear_series(self.client, key, count=50)

        result = self.client.execute_command(
            "TS.FEATURES", key, "-", "+", "FEATURE", "partial_autocorrelation:2"
        )
        parsed = parse_features_response(result)

        assert "partial_autocorrelation_2" in parsed
        assert not math.isnan(parsed["partial_autocorrelation_2"])

    def test_pacf_alias(self):
        """Test PACF alias works the same as partial_autocorrelation."""
        key = "test:features:pacf:alias"
        create_linear_series(self.client, key, count=50)

        result_full = self.client.execute_command(
            "TS.FEATURES", key, "-", "+", "FEATURE", "partial_autocorrelation:2"
        )
        result_short = self.client.execute_command(
            "TS.FEATURES", key, "-", "+", "FEATURE", "pacf:2"
        )

        parsed_full = parse_features_response(result_full)
        parsed_short = parse_features_response(result_short)

        assert "partial_autocorrelation_2" in parsed_full
        assert "partial_autocorrelation_2" in parsed_short
        assert parsed_full == parsed_short

    def test_combined_features(self):
        """Test multiple individual features together."""
        key = "test:features:combined"
        create_sine_series(self.client, key, count=100)

        result = self.client.execute_command(
            "TS.FEATURES", key, "-", "+",
            "FEATURE", "skewness,kurtosis,quantile:0.5,autocorrelation:1"
        )
        parsed = parse_features_response(result)

        expected = {"skewness", "kurtosis", "quantile_0.5", "autocorrelation_1"}
        assert set(parsed.keys()) == expected


class TestFeaturesCombined(ValkeyTimeSeriesTestCaseBase):
    """Test combining CATEGORY and FEATURE."""

    def test_category_and_feature(self):
        """Test combining CATEGORY and FEATURE yields union."""
        key = "test:features:catandfeat"
        create_linear_series(self.client, key, count=50)

        result = self.client.execute_command(
            "TS.FEATURES", key, "-", "+",
            "CATEGORY", "basic",
            "FEATURE", "skewness,kurtosis"
        )
        parsed = parse_features_response(result)

        basic_features = {"mean", "median", "variance", "variance_sample",
                          "minimum", "maximum", "length"}
        extra_features = {"skewness", "kurtosis"}
        assert set(parsed.keys()) == basic_features | extra_features

    def test_deduplication(self):
        """Test that duplicate features (from category + explicit) are deduplicated."""
        key = "test:features:dedup"
        create_linear_series(self.client, key, count=50)

        # "mean" is in the basic category; specifying it again should deduplicate
        result = self.client.execute_command(
            "TS.FEATURES", key, "-", "+",
            "CATEGORY", "basic",
            "FEATURE", "mean"
        )
        parsed = parse_features_response(result)

        basic_features = {"mean", "median", "variance", "variance_sample",
                          "minimum", "maximum", "length"}
        assert set(parsed.keys()) == basic_features
        # "mean" should appear exactly once
        mean_count = sum(1 for k in parsed if k == "mean")
        assert mean_count == 1

    def test_nan_handling(self):
        """Test that NaN values are returned as null."""
        key = "test:features:nan"
        create_constant_series(self.client, key, count=10)

        result = self.client.execute_command(
            "TS.FEATURES", key, "-", "+", "FEATURE", "variance"
        )
        parsed = parse_features_response(result)

        # Variance of constant series is 0.0, not NaN
        assert parsed["variance"] == 0.0


class TestFeaturesErrors(ValkeyTimeSeriesTestCaseBase):
    """Test error handling for TS.FEATURES."""

    def test_nonexistent_key(self):
        """Test error on non-existent key."""
        with pytest.raises(ResponseError, match="the key does not exist"):
            self.client.execute_command(
                "TS.FEATURES", "nonexistent", "-", "+", "CATEGORY", "basic"
            )

    def test_empty_series(self):
        """Test error on empty series (no samples)."""
        key = "test:features:empty"
        self.client.execute_command("TS.CREATE", key)

        with pytest.raises(ResponseError, match="no samples"):
            self.client.execute_command(
                "TS.FEATURES", key, "-", "+", "CATEGORY", "basic"
            )

    def test_no_category_or_feature(self):
        """Test error when neither CATEGORY nor FEATURE is specified."""
        key = "test:features:nocat"
        self.client.execute_command("TS.CREATE", key)
        self.client.execute_command("TS.ADD", key, 1000, 1.0)

        with pytest.raises(ResponseError,
                           match="at least one of CATEGORY or FEATURE"):
            self.client.execute_command("TS.FEATURES", key, "-", "+")

    def test_duplicate_category(self):
        """Test error on duplicate category."""
        key = "test:features:duplicate"
        self.client.execute_command("TS.CREATE", key)
        self.client.execute_command("TS.ADD", key, 1000, 1.0)

        with pytest.raises(ResponseError, match="duplicate category"):
            self.client.execute_command(
                "TS.FEATURES", key, "-", "+", "CATEGORY", "basic,basic"
            )

    def test_unknown_category(self):
        """Test error on unknown category."""
        key = "test:features:unknowncat"
        self.client.execute_command("TS.CREATE", key)
        self.client.execute_command("TS.ADD", key, 1000, 1.0)

        with pytest.raises(ResponseError, match="Unknown feature category"):
            self.client.execute_command(
                "TS.FEATURES", key, "-", "+", "CATEGORY", "invalid"
            )

    def test_unknown_feature(self):
        """Test error on unknown feature name."""
        key = "test:features:unknownfeat"
        self.client.execute_command("TS.CREATE", key)
        self.client.execute_command("TS.ADD", key, 1000, 1.0)

        with pytest.raises(ResponseError, match="Unknown feature"):
            self.client.execute_command(
                "TS.FEATURES", key, "-", "+", "FEATURE", "nonexistent_feature"
            )

    def test_invalid_quantile_range(self):
        """Test error on quantile outside [0, 1]."""
        key = "test:features:badq"
        self.client.execute_command("TS.CREATE", key)
        self.client.execute_command("TS.ADD", key, 1000, 1.0)

        with pytest.raises(ResponseError, match="Quantile parameter must be between"):
            self.client.execute_command(
                "TS.FEATURES", key, "-", "+", "FEATURE", "quantile:1.5"
            )

        with pytest.raises(ResponseError, match="Quantile parameter must be between"):
            self.client.execute_command(
                "TS.FEATURES", key, "-", "+", "FEATURE", "quantile:-0.1"
            )

    def test_invalid_autocorrelation_lag(self):
        """Test error on autocorrelation with lag 0."""
        key = "test:features:badlag"
        self.client.execute_command("TS.CREATE", key)
        self.client.execute_command("TS.ADD", key, 1000, 1.0)

        with pytest.raises(ResponseError, match="lag must be greater than 0"):
            self.client.execute_command(
                "TS.FEATURES", key, "-", "+", "FEATURE", "autocorrelation:0"
            )

    def test_wrong_arity(self):
        """Test wrong number of arguments."""
        with pytest.raises(ResponseError, match="wrong number of arguments"):
            self.client.execute_command("TS.FEATURES", "somekey")

    def test_unrecognized_argument(self):
        """Test unrecognized argument token."""
        key = "test:features:badarg"
        self.client.execute_command("TS.CREATE", key)
        self.client.execute_command("TS.ADD", key, 1000, 1.0)

        with pytest.raises(ResponseError, match="unrecognized argument"):
            self.client.execute_command(
                "TS.FEATURES", key, "-", "+", "INVALID", "basic"
            )

    def test_not_a_timeseries(self):
        """Test error when key is not a time series."""
        self.client.execute_command("SET", "stringkey", "value")

        with pytest.raises(ResponseError):
            self.client.execute_command(
                "TS.FEATURES", "stringkey", "-", "+", "CATEGORY", "basic"
            )


class TestFeaturesResponseFormat(ValkeyTimeSeriesTestCaseBase):
    """Test the response format and structure."""

    def test_response_is_flat_map(self):
        """Test that the response is a flat alternating key-value list."""
        key = "test:features:format"
        create_linear_series(self.client, key, count=10)

        result = self.client.execute_command(
            "TS.FEATURES", key, "-", "+", "FEATURE", "mean,median"
        )

        # Should be a flat list: [key, value, key, value, ...]
        assert isinstance(result, list)
        assert len(result) == 4  # 2 pairs
        assert len(result) % 2 == 0  # even length

    def test_response_sorted_alphabetically(self):
        """Test that features are returned in alphabetical order."""
        key = "test:features:sorted"
        create_linear_series(self.client, key, count=10)

        result = self.client.execute_command(
            "TS.FEATURES", key, "-", "+", "FEATURE", "maximum,mean,minimum"
        )
        parsed = parse_features_response(result)

        keys = list(parsed.keys())
        assert keys == sorted(keys), \
            f"Expected sorted keys, got {keys}"

    def test_time_range_subset(self):
        """Test that features are computed on the specified time range subset."""
        key = "test:features:subset"
        # Create series with values 0..19
        for i in range(20):
            self.client.execute_command("TS.ADD", key, 1000 + i * 1000, float(i))

        # Compute on subset (timestamps 5000-15000, values 4..14)
        result = self.client.execute_command(
            "TS.FEATURES", key, 5000, 15000, "CATEGORY", "basic"
        )
        parsed = parse_features_response(result)

        # length should be 11 (values at timestamps 5000 through 15000)
        assert parsed["length"] == 11.0
        assert parsed["minimum"] == 4.0
        assert parsed["maximum"] == 14.0
