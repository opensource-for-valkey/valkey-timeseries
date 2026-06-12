import math
import random
import pytest
from valkey import ResponseError
from valkeytestframework.conftest import resource_port_tracker  # noqa: F401
from valkey_timeseries_test_case import ValkeyTimeSeriesTestCaseBase
from data_helpers import create_constant_series, create_linear_series, create_sine_series, create_white_noise_series, create_random_walk_series


# ---------------------------------------------------------------------------
# Test suite
# ---------------------------------------------------------------------------


class TestStationarity(ValkeyTimeSeriesTestCaseBase):
    """Test suite for the TS.STATIONARITY command."""

    # --- Error cases ---

    def test_nonexistent_key(self):
        """Test error on non-existent key."""
        with pytest.raises(ResponseError, match="the key does not exist"):
            self.client.execute_command("TS.STATIONARITY", "nonexistent", "-", "+")

    def test_empty_series(self):
        """Test error on empty series (no samples)."""
        key = "test:stationarity:empty"
        self.client.execute_command("TS.CREATE", key)

        with pytest.raises(ResponseError, match="insufficient data"):
            self.client.execute_command("TS.STATIONARITY", key, "-", "+")

    def test_insufficient_data(self):
        """Test error when fewer than 10 samples are available."""
        key = "test:stationarity:few"
        for i in range(5):
            self.client.execute_command("TS.ADD", key, 1000 + i * 1000, float(i))

        with pytest.raises(ResponseError, match="insufficient data"):
            self.client.execute_command("TS.STATIONARITY", key, "-", "+")

    def test_invalid_test_type(self):
        """Test error on unknown TEST value."""
        key = "test:stationarity:invalid_test"
        create_constant_series(self.client, key, count=15)

        with pytest.raises(ResponseError, match="invalid TEST value"):
            self.client.execute_command(
                "TS.STATIONARITY", key, "-", "+", "TEST", "bogus"
            )

    def test_lags_with_combined_error(self):
        """Test error when LAGS is used with combined test."""
        key = "test:stationarity:lags_combined"
        create_constant_series(self.client, key, count=15)

        with pytest.raises(ResponseError, match="LAGS option is not supported"):
            self.client.execute_command(
                "TS.STATIONARITY", key, "-", "+", "TEST", "combined", "LAGS", "5"
            )

    def test_negative_lags_error(self):
        """Test error on negative LAGS value."""
        key = "test:stationarity:neg_lags"
        create_constant_series(self.client, key, count=15)

        with pytest.raises(ResponseError, match="non-negative"):
            self.client.execute_command(
                "TS.STATIONARITY", key, "-", "+", "TEST", "adf", "LAGS", "-1"
            )

    # --- Response shape tests ---

    def _assert_common_single_test_fields(self, result):
        """Verify the fields common to all single-test responses."""
        assert isinstance(result, list), "Expected list response"
        # Convert flat list of [k1, v1, k2, v2, ...] to dict
        d = dict(zip(result[::2], result[1::2]))
        assert b"test" in d
        assert b"conclusion" in d
        assert b"statistic" in d
        assert b"pValue" in d
        assert b"lags" in d
        assert b"isStationary" in d
        assert b"cv1pct" in d
        assert b"cv5pct" in d
        assert b"cv10pct" in d
        assert len(d) == 9, f"Expected 9 fields, got {len(d)}: {list(d.keys())}"
        return d

    def test_default_is_combined(self):
        """Test that the default test type returns combined fields with nested maps."""
        key = "test:stationarity:default"
        create_sine_series(self.client, key, count=100)

        result = self.client.execute_command("TS.STATIONARITY", key, "-", "+")
        d = dict(zip(result[::2], result[1::2]))
        assert d[b"test"] == b"combined"
        assert b"conclusion" in d
        # adf and kpss are nested maps (lists of [k1, v1, k2, v2, ...])
        assert b"adf" in d
        assert b"kpss" in d
        assert len(d) == 4, f"Expected 4 top-level keys, got {len(d)}: {list(d.keys())}"

    def test_combined_response_keys(self):
        """Test combined response returns nested maps with all expected keys."""
        key = "test:stationarity:combined_keys"
        create_sine_series(self.client, key, count=100)

        result = self.client.execute_command(
            "TS.STATIONARITY", key, "-", "+", "TEST", "combined"
        )
        d = dict(zip(result[::2], result[1::2]))
        assert d[b"test"] == b"combined"
        assert b"conclusion" in d
        assert len(d) == 4, f"Expected 4 top-level keys, got {len(d)}: {list(d.keys())}"

        # ADF nested map — 7 fields
        adf_d = self._assert_nested_result_fields(d, b"adf")
        assert set(adf_d.keys()) == {
            b"statistic",
            b"pValue",
            b"lags",
            b"isStationary",
            b"cv1pct",
            b"cv5pct",
            b"cv10pct",
        }

        # KPSS nested map — 7 fields
        kpss_d = self._assert_nested_result_fields(d, b"kpss")
        assert set(kpss_d.keys()) == {
            b"statistic",
            b"pValue",
            b"lags",
            b"isStationary",
            b"cv1pct",
            b"cv5pct",
            b"cv10pct",
        }

    def _assert_nested_result_fields(self, top_level, key):
        """Verify a nested map exists in the combined response and has 7 fields."""
        nested = top_level[key]
        assert isinstance(nested, list), f"Expected nested list for '{key}', got {type(nested)}"
        d = dict(zip(nested[::2], nested[1::2]))
        assert len(d) == 7, f"Expected 7 fields in '{key}' map, got {len(d)}: {list(d.keys())}"
        return d

    def test_adf_response_keys(self):
        """Test ADF response returns all expected keys."""
        key = "test:stationarity:adf_keys"
        create_sine_series(self.client, key, count=100)

        result = self.client.execute_command(
            "TS.STATIONARITY", key, "-", "+", "TEST", "adf"
        )
        d = self._assert_common_single_test_fields(result)
        assert d[b"test"] == b"adf"
        assert d[b"conclusion"] in (b"stationary", b"non_stationary")

    def test_kpss_response_keys(self):
        """Test KPSS response returns all expected keys."""
        key = "test:stationarity:kpss_keys"
        create_sine_series(self.client, key, count=100)

        result = self.client.execute_command(
            "TS.STATIONARITY", key, "-", "+", "TEST", "kpss"
        )
        d = self._assert_common_single_test_fields(result)
        assert d[b"test"] == b"kpss"
        assert d[b"conclusion"] in (b"stationary", b"non_stationary")

    def test_adf_with_lags(self):
        """Test ADF with explicit LAGS parameter."""
        key = "test:stationarity:adf_lags"
        create_sine_series(self.client, key, count=100)

        result = self.client.execute_command(
            "TS.STATIONARITY", key, "-", "+", "TEST", "adf", "LAGS", "8"
        )
        d = self._assert_common_single_test_fields(result)
        assert d[b"lags"] == b"8"

    def test_kpss_with_lags(self):
        """Test KPSS with explicit LAGS parameter."""
        key = "test:stationarity:kpss_lags"
        create_sine_series(self.client, key, count=100)

        result = self.client.execute_command(
            "TS.STATIONARITY", key, "-", "+", "TEST", "kpss", "LAGS", "6"
        )
        d = self._assert_common_single_test_fields(result)
        assert d[b"lags"] == b"6"

    # --- Statistical behaviour tests ---

    def test_white_noise_is_stationary(self):
        """White noise should be classified as stationary by ADF."""
        key = "test:stationarity:white_noise"
        random.seed(42)  # reproducible
        create_white_noise_series(self.client, key, count=100)

        result = self.client.execute_command(
            "TS.STATIONARITY", key, "-", "+", "TEST", "adf"
        )
        d = dict(zip(result[::2], result[1::2]))
        # White noise should be stationary (ADF rejects null of unit root)
        assert d[b"isStationary"] == b"1", (
            f"Expected white noise to be stationary, got isStationary={d[b'isStationary']}, "
            f"statistic={d[b'statistic']}, pValue={d[b'pValue']}"
        )

    def test_random_walk_is_nonstationary(self):
        """Random walk should be classified as non-stationary by ADF."""
        key = "test:stationarity:random_walk"
        random.seed(123)  # reproducible
        create_random_walk_series(self.client, key, count=100)

        result = self.client.execute_command(
            "TS.STATIONARITY", key, "-", "+", "TEST", "adf"
        )
        d = dict(zip(result[::2], result[1::2]))
        # Random walk should be non-stationary
        # Note: with 100 samples this is very likely but not guaranteed;
        # we assert the statistic is less extreme (closer to zero) than for white noise
        stat = float(d[b"statistic"])
        # A random walk typically has ADF statistic closer to zero (fail to reject null)
        assert stat > -3.0, (
            f"Expected random walk ADF stat > -3.0, got {stat}"
        )

    def test_constant_series_is_stationary(self):
        """A constant series should be stationary."""
        key = "test:stationarity:constant"
        create_constant_series(self.client, key, count=15)

        result = self.client.execute_command(
            "TS.STATIONARITY", key, "-", "+", "TEST", "adf"
        )
        d = dict(zip(result[::2], result[1::2]))
        assert d[b"isStationary"] == b"1", (
            f"Expected constant series to be stationary, got isStationary={d[b'isStationary']}"
        )

    def test_sine_series_is_stationary(self):
        """A sinusoidal series with small noise should be stationary."""
        key = "test:stationarity:sine"
        create_sine_series(self.client, key, count=100)

        result = self.client.execute_command(
            "TS.STATIONARITY", key, "-", "+", "TEST", "combined"
        )
        d = dict(zip(result[::2], result[1::2]))
        # Combined test should conclude stationary for a sine wave
        assert d[b"conclusion"] == b"stationary", (
            f"Expected sine series to be stationary, got {d[b'conclusion']}"
        )

    def test_linear_trend_is_nonstationary(self):
        """A linear trend should be classified as non-stationary."""
        key = "test:stationarity:linear"
        create_linear_series(self.client, key, count=50)

        result = self.client.execute_command(
            "TS.STATIONARITY", key, "-", "+", "TEST", "adf"
        )
        d = dict(zip(result[::2], result[1::2]))
        # Linear trend typically fails stationarity tests
        assert d[b"isStationary"] == b"0", (
            f"Expected linear trend to be non-stationary, got isStationary={d[b'isStationary']}"
        )

    # --- Time range filtering ---

    def test_time_range_filtering(self):
        """Test that stationarity respects the time range."""
        key = "test:stationarity:range"
        # Create a stationary series early, then a trend later
        random.seed(77)
        for i in range(30):
            val = random.gauss(0.0, 1.0)
            self.client.execute_command("TS.ADD", key, 1000 + i * 1000, val)
        # Add a trend afterwards
        for i in range(30):
            self.client.execute_command("TS.ADD", key, 31000 + i * 1000, float(i * 10))

        # Test only the stationary (early) portion
        result = self.client.execute_command(
            "TS.STATIONARITY", key, 1000, 30000, "TEST", "adf"
        )
        d = dict(zip(result[::2], result[1::2]))
        assert d[b"isStationary"] == b"1", (
            f"Expected early range to be stationary, got isStationary={d[b'isStationary']}"
        )

    def test_time_range_insufficient_data(self):
        """Test error when time range yields too few samples."""
        key = "test:stationarity:range_few"
        create_sine_series(self.client, key, count=100)

        # Query a tiny time window that contains < 10 samples
        with pytest.raises(ResponseError, match="insufficient data"):
            self.client.execute_command(
                "TS.STATIONARITY", key, 1000, 5000, "TEST", "adf"
            )
