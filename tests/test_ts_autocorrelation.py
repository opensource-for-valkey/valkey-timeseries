import math
import pytest
from valkey import ResponseError
from valkeytestframework.conftest import resource_port_tracker  # noqa: F401
from valkey_timeseries_test_case import ValkeyTimeSeriesTestCaseBase


def create_linear_series(client, key, start_time=1000, count=20):
    """Create a time series with linear trend."""
    for i in range(count):
        client.execute_command('TS.ADD', key, start_time + i * 1000, float(i))
    return start_time, count


def create_alternating_series(client, key, start_time=1000, count=20):
    """Create a time series with alternating values."""
    for i in range(count):
        val = 1.0 if i % 2 == 0 else -1.0
        client.execute_command('TS.ADD', key, start_time + i * 1000, val)
    return start_time, count


def create_constant_series(client, key, start_time=1000, count=10):
    """Create a time series with constant values."""
    for i in range(count):
        client.execute_command('TS.ADD', key, start_time + i * 1000, 5.0)
    return start_time, count


def create_sine_series(client, key, start_time=1000, count=100):
    """Create a time series with sinusoidal values."""
    for i in range(count):
        val = math.sin(i * 0.1)
        client.execute_command('TS.ADD', key, start_time + i * 1000, val)
    return start_time, count


class TestAutocorrelation(ValkeyTimeSeriesTestCaseBase):
    """Test suite for TS.AUTOCORRELATION command."""

    def test_acf_nonexistent_key(self):
        """Test ACF on non-existent key."""
        with pytest.raises(ResponseError, match="the key does not exist"):
            self.client.execute_command('TS.AUTOCORRELATION', 'nonexistent', '-', '+', 1)

    def test_acf_empty_series(self):
        """Test ACF on empty series."""
        key = 'test:acf:empty'
        self.client.execute_command('TS.CREATE', key)

        with pytest.raises(ResponseError, match="insufficient data"):
            self.client.execute_command('TS.AUTOCORRELATION', key, '-', '+', 1)

    def test_acf_linear_trend(self):
        """Test ACF with linear trend (should be high)."""
        key = 'test:acf:linear'
        create_linear_series(self.client, key, count=20)

        result = self.client.execute_command('TS.AUTOCORRELATION', key, '-', '+', 1)
        result = float(result)
        assert result > 0.8, f"Expected high ACF(1) for linear trend, got {result}"

    def test_acf_alternating(self):
        """Test ACF with alternating values (should be negative at lag 1)."""
        key = 'test:acf:alternating'
        create_alternating_series(self.client, key, count=20)

        result = self.client.execute_command('TS.AUTOCORRELATION', key, '-', '+', 1)
        result = float(result)
        assert result < -0.5, f"Expected negative ACF(1) for alternating, got {result}"

    def test_acf_constant(self):
        """Test ACF with constant values (should be zero)."""
        key = 'test:acf:constant'
        create_constant_series(self.client, key, count=10)

        result = self.client.execute_command('TS.AUTOCORRELATION', key, '-', '+', 1)
        result = float(result)
        assert abs(result) < 1e-10, f"Expected zero ACF for constant series, got {result}"

    def test_acf_lag_zero(self):
        """Test ACF at lag 0 (should be 1.0)."""
        key = 'test:acf:lag0'
        create_linear_series(self.client, key, count=5)

        result = self.client.execute_command('TS.AUTOCORRELATION', key, '-', '+', 0)
        result = float(result)
        assert abs(result - 1.0) < 1e-10, f"Expected ACF(0)=1.0, got {result}"

    def test_acf_insufficient_data(self):
        """Test ACF with insufficient data for the lag."""
        key = 'test:acf:insufficient'
        self.client.execute_command('TS.CREATE', key)
        self.client.execute_command('TS.ADD', key, 1000, 1.0)
        self.client.execute_command('TS.ADD', key, 2000, 2.0)

        with pytest.raises(ResponseError, match="insufficient data"):
            self.client.execute_command('TS.AUTOCORRELATION', key, '-', '+', 5)

    def test_pacf_nonexistent_key(self):
        """Test PACF on non-existent key."""
        with pytest.raises(ResponseError, match="the key does not exist"):
            self.client.execute_command('TS.AUTOCORRELATION', 'nonexistent', '-', '+', 1, 'PARTIAL')

    def test_pacf_lag_zero(self):
        """Test PACF at lag 0 (should be 1.0)."""
        key = 'test:pacf:lag0'
        create_linear_series(self.client, key, count=5)

        result = self.client.execute_command('TS.AUTOCORRELATION', key, '-', '+', 0, 'PARTIAL')
        result = float(result)
        assert abs(result - 1.0) < 1e-10, f"Expected PACF(0)=1.0, got {result}"

    def test_pacf_ar1_process(self):
        """Test PACF on AR(1) process. PACF(1) should be high, PACF(2) lower."""
        key = 'test:pacf:ar1'
        start_time = 1000
        series = [0.0] * 50
        series[0] = 1.0
        for i in range(1, 50):
            series[i] = 0.8 * series[i - 1]
        for i, val in enumerate(series):
            self.client.execute_command('TS.ADD', key, start_time + i * 1000, val)

        pacf1 = float(self.client.execute_command('TS.AUTOCORRELATION', key, '-', '+', 1, 'PARTIAL'))
        pacf2 = float(self.client.execute_command('TS.AUTOCORRELATION', key, '-', '+', 2, 'PARTIAL'))

        assert isinstance(pacf1, float)
        assert isinstance(pacf2, float)
        assert pacf1 > 0.5, f"Expected high PACF(1) for AR(1), got {pacf1}"
        assert abs(pacf2) < abs(pacf1), f"PACF(2) should be smaller than PACF(1)"

    def test_pacf_insufficient_data(self):
        """Test PACF with insufficient data."""
        key = 'test:pacf:insufficient'
        self.client.execute_command('TS.CREATE', key)
        self.client.execute_command('TS.ADD', key, 1000, 1.0)
        self.client.execute_command('TS.ADD', key, 2000, 2.0)

        with pytest.raises(ResponseError, match="insufficient data"):
            self.client.execute_command('TS.AUTOCORRELATION', key, '-', '+', 5, 'PARTIAL')

    def test_tra_nonexistent_key(self):
        """Test TRA on non-existent key."""
        with pytest.raises(ResponseError, match="the key does not exist"):
            self.client.execute_command('TS.AUTOCORRELATION', 'nonexistent', '-', '+', 1, 'TRA')

    def test_tra_sine_wave(self):
        """Test TRA on sine wave (should be close to zero)."""
        key = 'test:tra:sine'
        create_sine_series(self.client, key, count=100)

        result = self.client.execute_command('TS.AUTOCORRELATION', key, '-', '+', 1, 'TRA')
        result = float(result)
        assert abs(result) < 0.1, f"Sine wave TRA should be near zero, got {result}"

    def test_tra_constant(self):
        """Test TRA on constant series (should be zero)."""
        key = 'test:tra:constant'
        create_constant_series(self.client, key, count=20)

        result = self.client.execute_command('TS.AUTOCORRELATION', key, '-', '+', 1, 'TRA')
        result = float(result)
        assert abs(result) < 1e-10, f"Constant series TRA should be zero, got {result}"

    def test_tra_insufficient_data(self):
        """Test TRA with insufficient data."""
        key = 'test:tra:insufficient'
        self.client.execute_command('TS.CREATE', key)
        self.client.execute_command('TS.ADD', key, 1000, 1.0)
        self.client.execute_command('TS.ADD', key, 2000, 2.0)

        with pytest.raises(ResponseError, match="insufficient data"):
            self.client.execute_command('TS.AUTOCORRELATION', key, '-', '+', 2, 'TRA')

    def test_aggregated_mean(self):
        """Test aggregated autocorrelation with mean."""
        key = 'test:agg:mean'
        create_linear_series(self.client, key, count=50)

        result = self.client.execute_command('TS.AUTOCORRELATION', key, '-', '+', 5, 'AGGREGATED', 'mean')
        result = float(result)
        assert not math.isnan(result)
        assert result > 0.0

    def test_aggregated_var(self):
        """Test aggregated autocorrelation with variance."""
        key = 'test:agg:var'
        create_linear_series(self.client, key, count=50)

        result = self.client.execute_command('TS.AUTOCORRELATION', key, '-', '+', 5, 'AGGREGATED', 'var')
        result = float(result)
        assert not math.isnan(result)
        assert result >= 0.0

    def test_aggregated_median(self):
        """Test aggregated autocorrelation with median."""
        key = 'test:agg:median'
        create_linear_series(self.client, key, count=50)

        result = self.client.execute_command('TS.AUTOCORRELATION', key, '-', '+', 5, 'AGGREGATED', 'median')
        result = float(result)
        assert not math.isnan(result)

    def test_aggregated_invalid_func(self):
        """Test aggregated autocorrelation with invalid function."""
        key = 'test:agg:invalid'
        create_linear_series(self.client, key, count=50)

        with pytest.raises(ResponseError, match="invalid AGGREGATED function"):
            self.client.execute_command('TS.AUTOCORRELATION', key, '-', '+', 5, 'AGGREGATED', 'invalid')

    def test_aggregated_insufficient_data(self):
        """Test aggregated autocorrelation with insufficient data."""
        key = 'test:agg:insufficient'
        create_linear_series(self.client, key, count=5)

        with pytest.raises(ResponseError, match="insufficient data"):
            self.client.execute_command('TS.AUTOCORRELATION', key, '-', '+', 10, 'AGGREGATED', 'mean')

    def test_aggregated_lag_zero(self):
        """Test aggregated autocorrelation with lag=0 should fail."""
        key = 'test:agg:zero'
        create_linear_series(self.client, key, count=10)

        with pytest.raises(ResponseError, match="returned NaN"):
            self.client.execute_command('TS.AUTOCORRELATION', key, '-', '+', 0, 'AGGREGATED', 'mean')

    def test_wrong_arity(self):
        """Test wrong number of arguments."""
        with pytest.raises(ResponseError, match="wrong number of arguments"):
            self.client.execute_command('TS.AUTOCORRELATION', 'somekey')

    def test_aggregated_std(self):
        """Test aggregated autocorrelation with standard deviation."""
        key = 'test:agg:std'
        create_linear_series(self.client, key, count=50)

        result = self.client.execute_command('TS.AUTOCORRELATION', key, '-', '+', 5, 'AGGREGATED', 'std')
        result = float(result)
        assert not math.isnan(result)
        assert result >= 0.0

    def test_unknown_option(self):
        """Test unknown option."""
        key = 'test:unknown'
        create_linear_series(self.client, key, count=10)

        with pytest.raises(ResponseError, match="unrecognized option"):
            self.client.execute_command('TS.AUTOCORRELATION', key, '-', '+', 1, 'INVALID')
