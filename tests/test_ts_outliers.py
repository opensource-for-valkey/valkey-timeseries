import math
from typing import List, Any

import pytest
from valkey import ResponseError

from outlier_result import AnomalyEntry, TSOutliersFullResult, TSOutliersCleanedResult
from valkey_timeseries_test_case import ValkeyTimeSeriesTestCaseBase
from valkeytestframework.conftest import resource_port_tracker
from valkeytestframework.util.waiters import *


def create_normal_series(client, key, start_time=1000, count=100):
    """Create a time series with normal data points."""
    for i in range(count):
        client.execute_command(
            'TS.ADD', key, start_time + i * 1000,
            math.sin(i * 0.1)
        )
    return start_time, count


def create_series_with_outliers(client, key, start_time=1000):
    """Create a time series with clear outliers."""
    values = [1.0, 2.0, 1.5, 2.2, 1.8, 10.0, 2.1, 1.9, 1.7, 50.0]
    for i, val in enumerate(values):
        client.execute_command(
            'TS.ADD', key, start_time + i * 1000, val
        )
    return start_time, len(values)


def convert_anomaly_entries(result: List[Any]) -> list[AnomalyEntry]:
    """Convert raw TS.OUTLIERS result to a list of AnomalyEntry."""
    entries = []
    result = result or []
    for entry in result:
        entry = AnomalyEntry.parse(entry)
        entries.append(entry)
    return entries


class TestOutliersMethods(ValkeyTimeSeriesTestCaseBase):
    """Test different outlier detection methods."""

    def test_outliers_empty_series(self):
        """Test outlier detection on empty series."""
        key = 'test:outliers:empty'
        self.client.execute_command('TS.CREATE', key)

        with pytest.raises(Exception) as exc_info:
            self.client.execute_command('TS.OUTLIERS', key, '-', '+', 'METHOD', 'zscore')
        assert 'insufficient data' in str(exc_info.value).lower()

    def test_outliers_nonexistent_key(self):
        """Test outlier detection on non-existent key."""
        with pytest.raises(ResponseError, match="the key does not exist"):
            self.client.execute_command('TS.OUTLIERS', 'nonexistent', "-", "+", "method", "zscore")

    def test_method_spc_cusum_negative_spike(self):
        """Test SPC CUSUM control chart method negative spike."""
        key = 'test:outliers:method:cusum'

        ts = [10.0, 9.0, 11.0, 10.5, 9.5, 10.0, 9.0, 11.0, 10.5, 9.5, 9.0]
        ts.extend([-30.0] * 10)  # Negative spike - much larger deviation
        ts.extend([10.0, 9.0, 11.0, 10.5, 9.5, 8.0])

        for i, val in enumerate(ts):
            self.client.execute_command('TS.ADD', key, 1000 + i * 1000, val)

        result = self.client.execute_command(
            'TS.OUTLIERS', key, '-', '+', 'METHOD', 'cusum'
        )

        assert len(result) >= 4

    def test_method_spc_cusum_gradual_shift(self):
        """Test SPC CUSUM control chart method gradual shift."""

        key = 'test:outliers:method:cusum:shift'
        ts = [float(i) for i in range(0, 50)]

        # Gradual upward shift
        for i in range(50):
            ts.append(float(i + 50))

        for i, val in enumerate(ts):
            self.client.execute_command('TS.ADD', key, 1000 + i * 1000, val)

        result = self.client.execute_command(
            'TS.OUTLIERS', key, '-', '+', 'METHOD', 'cusum'
        )
        outliers = convert_anomaly_entries(result)

        print("cusum gradual shift result", outliers)
        positive_count = sum(1 for entry in outliers if entry.signal == 1)
        assert positive_count >= 4


    def test_invalid_method(self):
        """Test with an invalid method name."""
        key = 'test:outliers:invalid'
        create_series_with_outliers(self.client, key)

        with pytest.raises(ResponseError, match='TSDB: unknown anomaly detection method'):
            self.client.execute_command('TS.OUTLIERS', key, '-', '+', 'METHOD', 'invalid')

    def test_method_zscore(self):
        """Test outlier detection with default z-score method."""
        key = 'test:outliers:zscore'
        client = self.client

        data = [
            0.10, 0.05, 0.12, 0.08, 0.11, 0.09, 0.07, 0.10, 0.06, 0.08, 0.09, 0.11, 0.10, 0.07,
            0.08, 0.12, 0.09, 0.10, 0.08, 0.07, 6.00,  # strong positive anomaly
            0.09, 0.11, 0.08, 0.10, 0.07, 0.09, 0.10, -6.00,  # strong negative anomaly
            0.08, 0.09, 0.10,
        ]

        for i, val in enumerate(data):
            self.client.execute_command('TS.ADD', key, 1000 + i * 1000, val)

        result = client.execute_command('TS.OUTLIERS', key, "-", "+", "method", "zscore")
        anomalies = convert_anomaly_entries(result)

        assert len(anomalies) == 2
        assert anomalies[0].signal == 1
        assert anomalies[0].value == 6.00

        assert anomalies[1].signal == -1
        assert anomalies[1].value == -6.00

    def test_method_modified_zscore(self):
        """Test Modified Z-score using median absolute deviation"""
        key = "modified_zscore_test"

        self.client.execute_command("TS.CREATE", key)

        # Create a series with outliers
        values = [1.0, 2.0, 1.5, 2.2, 1.8, 10.0, 2.1, 1.9]
        for i, value in enumerate(values):
            self.client.execute_command("TS.ADD", key, i, value)

        result = self.client.execute_command(
            "TS.OUTLIERS", key, "-", "+",
            "method", "modified-zscore", "threshold", 3.5
        )

        outliers = convert_anomaly_entries(result)

        assert len(outliers) == 1
        assert outliers[0].signal == 1
        assert outliers[0].value == 10.0

    def test_method_iqr(self):
        """Test Interquartile Range (IQR) anomaly detection"""
        key = "iqr_test"

        self.client.execute_command("TS.CREATE", key)

        # Create a series with outliers
        for i in range(100):
            value = 1.0 if i != 50 else 10.0
            self.client.execute_command("TS.ADD", key, i, value)

        result = self.client.execute_command(
            "TS.OUTLIERS", key, "-", "+",
            "method", "iqr",
            "threshold", 1.5
        )

        result = convert_anomaly_entries(result)
        assert len(result) == 1
        assert result[0].signal == 1
        assert result[0].value == 10.00

        VALUES = [
            50.0, 51.0, 49.0, 52.0, 48.0,  # Normal range
            100.0,  # High outlier
            50.0, 49.0, 51.0,  # Normal range
            5.0,  # Low outlier
            50.0, 52.0,  # Normal range
        ]

        key2 = "iqr_test_2"
        self.client.execute_command("TS.CREATE", key2)
        for i, value in enumerate(VALUES):
            self.client.execute_command("TS.ADD", key2, 1000 + i * 1000, value)

        result2 = self.client.execute_command(
            "TS.OUTLIERS", key2, "-", "+",
            "method", "iqr",
            "threshold", 1.5
        )

        outliers = convert_anomaly_entries(result2)
        assert len(outliers) == 2

        assert outliers[0].signal == 1
        assert outliers[0].value == 100.0
        assert outliers[1].signal == -1
        assert outliers[1].value == 5.0

    def test_method_spc_ewma(self):
        """Test SPC EWMA control chart method."""
        key = 'test:outliers:method:ewma'
        self.client.execute_command('TS.CREATE', key)

        # Test with multiple scattered anomalies
        ts = [2.0 + 0.1 * math.sin(i * 0.1) for i in range(100)]
        # Add multiple anomalies
        ts[10] = 6.0
        ts[30] = -2.0
        ts[50] = 7.0
        ts[80] = -3.0

        for i, val in enumerate(ts):
            self.client.execute_command('TS.ADD', key, 1000 + i * 1000, val)

        result = self.client.execute_command(
            'TS.OUTLIERS', key, '-', '+', 'METHOD', 'ewma',
            'ALPHA', 0.3
        )
        anomalies = convert_anomaly_entries(result)

        assert len(anomalies) >= 3
        assert anomalies[0].signal == 1
        assert anomalies[0].value == 6.0
        assert anomalies[1].signal == -1
        assert anomalies[1].value == -2.0
        assert anomalies[2].signal == 1
        assert anomalies[2].value == 7.0
        assert anomalies[3].signal == -1
        assert anomalies[3].value == -3.0

    def test_method_smoothed_zscore(self):
        """Test smoothed z-score method."""
        key = 'test:outliers:method:smoothed'

        ts = [
            1.0, 1.0, 1.1, 1.0, 0.9, 1.0, 1.0, 1.1, 1.0, 0.9,
            1.0, 1.1, 1.0, 1.0, 0.9, 1.0, 1.0, 1.1, 1.0, 1.0,
            5.0,  # anomaly
            1.0, 1.0, 1.0,
        ]
        for i, val in enumerate(ts):
            self.client.execute_command('TS.ADD', key, 1000 + i * 1000, val)

        result = self.client.execute_command(
            'TS.OUTLIERS', key, '-', '+',
            'METHOD', 'smoothed-zscore', 'LAG', 10, 'THRESHOLD', 2.0, 'INFLUENCE', 0.0
        )
        anomalies = convert_anomaly_entries(result)

        assert len(anomalies) == 1
        assert anomalies[0].signal == 1  # Positive anomaly
        assert anomalies[0].value == 5.0

    def test_method_mad(self):
        """Test Median Absolute Deviation (Mad) detection"""
        key = "mad_test"

        self.client.execute_command("TS.CREATE", key)

        values = [
            1067.0, 1085.0, 1133.0, 1643.0, 3328.0, 3351.0, 3369.0, 3385.0, 3412.0, 3438.0, 3441.0,
            3451.0, 3462.0, 3465.0, 3497.0, 3505.0, 3514.0, 3519.0, 3521.0, 3525.0, 3529.0, 3531.0,
            3555.0, 3575.0, 3587.0, 3600.0, 3624.0, 3634.0, 3635.0, 3639.0, 3652.0, 3652.0, 3660.0,
            3662.0, 3665.0, 3667.0, 3673.0, 3677.0, 3687.0, 3688.0, 3700.0, 3717.0, 3736.0, 3739.0,
            3743.0, 3761.0, 3773.0, 3780.0, 3783.0, 3791.0, 3823.0, 3833.0, 3834.0, 3848.0, 3859.0,
            3860.0, 3861.0, 3866.0, 3870.0, 3881.0, 3882.0, 3884.0, 3892.0, 3897.0, 3903.0, 3909.0,
            3920.0, 3921.0, 3928.0, 3942.0, 3946.0, 3959.0, 3994.0, 3998.0, 4023.0, 4065.0, 4115.0,
            4161.0, 4175.0, 4183.0, 4228.0, 4247.0, 4253.0, 4256.0, 4275.0, 4277.0, 4327.0, 4398.0,
            4416.0, 4642.0,
        ]

        expected = [1067.0, 1085.0, 1133.0, 1643.0, 4642.0]

        for i, value in enumerate(values):
            self.client.execute_command("TS.ADD", key, 1000 + i * 1000, value)

        result = self.client.execute_command("TS.OUTLIERS",
                                             key, "-", "+",
                                             "method", "mad",
                                             "threshold", 3.0,
                                             "estimator", "simple"
                                             )

        anomalies = convert_anomaly_entries(result)

        assert len(anomalies) == len(expected)
        for anomaly, exp in zip(anomalies, expected):
            assert anomaly.value == exp

    def test_method_double_mad(self):
        """Test Double Median Absolute Deviation (Double Mad) outlier detection"""
        key = "double_mad_test"

        self.client.execute_command("TS.CREATE", key)

        VALUES = [
            -2002.0, -2001.0, -2000.0, 9.0, 47.0, 50.0, 71.0, 78.0, 79.0, 97.0, 98.0, 117.0, 123.0,
            136.0, 138.0, 143.0, 145.0, 167.0, 185.0, 202.0, 216.0, 217.0, 229.0, 235.0, 242.0, 257.0,
            297.0, 300.0, 315.0, 344.0, 347.0, 347.0, 360.0, 362.0, 368.0, 387.0, 400.0, 428.0, 455.0,
            468.0, 484.0, 493.0, 523.0, 557.0, 574.0, 586.0, 605.0, 617.0, 618.0, 634.0, 641.0, 646.0,
            649.0, 674.0, 678.0, 689.0, 699.0, 703.0, 709.0, 714.0, 740.0, 795.0, 798.0, 839.0, 880.0,
            938.0, 941.0, 983.0, 1014.0, 1021.0, 1022.0, 1165.0, 1183.0, 1195.0, 1250.0, 1254.0,
            1288.0, 1292.0, 1326.0, 1362.0, 1363.0, 1421.0, 1549.0, 1585.0, 1605.0, 1629.0, 1694.0,
            1695.0, 1719.0, 1799.0, 1827.0, 1828.0, 1862.0, 1991.0, 2140.0, 2186.0, 2255.0, 2266.0,
            2295.0, 2321.0, 2419.0, 2919.0, 3612.0, 6000.0, 6001.0, 6002.0,
        ]

        EXPECTED = [-2002.0, -2001.0, -2000.0, 6000.0, 6001.0, 6002.0]

        for i, value in enumerate(VALUES):
            self.client.execute_command("TS.ADD", key, i, value)

        result = self.client.execute_command("TS.OUTLIERS",
                                             key, "-", "+",
                                             "method", "double-mad",
                                             "threshold", 3.0,
                                             "estimator", "harrell-davis"
                                             )

        outliers = convert_anomaly_entries(result)

        assert len(outliers) == len(EXPECTED)
        for outlier, exp in zip(outliers, EXPECTED):
            assert outlier.value == exp

    def test_anomaly_direction_filtering(self):
        """Test filtering anomalies by direction"""
        key = "direction_test"

        self.client.execute_command("TS.CREATE", key)

        # Create a sufficient baseline with variance
        values = []
        for i in range(30):
            # Add slight variance around 10.0 for valid std dev
            values.append(10.0 + 0.2 * math.sin(i * 0.5))

        # Add positive anomaly
        values.append(20.0)  # Strong positive spike

        # More baseline
        for i in range(10):
            values.append(10.0 + 0.2 * math.sin(i * 0.5))

        # Add negative anomaly
        values.append(-5.0)  # Strong negative spike

        # Final baseline
        for i in range(9):
            values.append(10.0 + 0.2 * math.sin(i * 0.5))

        for i, value in enumerate(values):
            self.client.execute_command("TS.ADD", key, i, value)

        # Test direction="positive"
        result = self.client.execute_command("TS.OUTLIERS",
                                             key, "-", "+",
                                             "method", "zscore",
                                             "threshold", 3.0,
                                             "direction", "positive")

        outliers = convert_anomaly_entries(result)
        assert len(outliers) == 1
        assert outliers[0].signal == 1  # Positive signal
        assert outliers[0].value == 20.0

        # Test direction="negative"
        result = self.client.execute_command("TS.OUTLIERS",
                                             key, "-", "+",
                                             "method", "zscore",
                                             "threshold", 3.0,
                                             "direction", "negative")

        outliers = convert_anomaly_entries(result)
        assert len(outliers) == 1
        assert outliers[0].signal == -1  # Negative signal
        assert outliers[0].value == -5.0

        # Test direction="both"
        result = self.client.execute_command("TS.OUTLIERS",
                                             key, "-", "+",
                                             "method", "zscore",
                                             "threshold", 3.0,
                                             "direction", "both")

        outliers = convert_anomaly_entries(result)

        assert len(outliers) == 2
        assert outliers[0].signal == 1  # Positive signal
        assert outliers[0].value == 20.0
        assert outliers[1].signal == -1  # Negative signal
        assert outliers[1].value == -5.0

    def test_insufficient_data_error(self):
        """Test error handling with insufficient data"""
        key = "insufficient_test"

        self.client.execute_command("TS.CREATE", key)

        # Add only 2 points
        self.client.execute_command("TS.ADD", key, 0, 1.0)
        self.client.execute_command("TS.ADD", key, 1, 2.0)

        with pytest.raises(Exception):
            self.client.execute_command("TS.OUTLIERS", key, method="zscore")

    def test_zscore_constant_series(self):
        """Test anomaly detection on constant series (no anomalies expected)"""
        key = "constant_test"

        self.client.execute_command("TS.CREATE", key)

        # Add constant values
        for i in range(50):
            self.client.execute_command("TS.ADD", key, i, 1.0)

        result = self.client.execute_command("TS.OUTLIERS",
                                             key, "-", "+",
                                             "method", "zscore",
                                             "threshold", 3.0,
        )

        assert len(result) == 0

    def test_format_cleaned(self):
        """Test OUTPUT cleaned returns only samples with directed anomalies removed."""
        key = 'test:outliers:format:cleaned'

        # Create data with clear anomalies
        data = [
            10.0, 10.5, 9.8, 10.2, 9.9,  # Normal values
            50.0,  # Positive anomaly
            10.1, 10.3, 9.7, 10.0,  # Normal values
            -30.0,  # Negative anomaly
            10.2, 9.9, 10.1,  # Normal values
        ]

        for i, val in enumerate(data):
            self.client.execute_command('TS.ADD', key, 1000 + i * 1000, val)

        # Test OUTPUT cleaned with DIRECTION both (default)
        result = self.client.execute_command(
            'TS.OUTLIERS', key, '-', '+',
            'OUTPUT', 'cleaned',
            'METHOD', 'zscore', 'THRESHOLD', '2.5'
        )

        # CLEANED is an array of [timestamp, value] samples
        result = TSOutliersCleanedResult.parse(result)
        anomalies_both = convert_anomaly_entries(
            self.client.execute_command(
                'TS.OUTLIERS', key, '-', '+',
                'OUTPUT', 'simple',
                'METHOD', 'zscore', 'THRESHOLD', '2.5'
            )
        )

        assert len(anomalies_both) == 2

        # Cleaned samples should exclude anomalies
        sample_values = [s.value for s in result.samples]
        assert 50.0 not in sample_values
        assert -30.0 not in sample_values
        assert len(sample_values) == len(data) - len(anomalies_both)

        # Test OUTPUT cleaned with DIRECTION positive (only removes positive anomalies)
        result_positive = self.client.execute_command(
            'TS.OUTLIERS', key, '-', '+',
            'OUTPUT', 'cleaned',
            'DIRECTION', 'positive',
            'METHOD', 'zscore', 'THRESHOLD', '2.5'
        )

        res = TSOutliersCleanedResult.parse(result_positive)
        samples_pos = res.samples
        anomalies_pos = convert_anomaly_entries(
            self.client.execute_command(
                'TS.OUTLIERS', key, '-', '+',
                'OUTPUT', 'simple',
                'DIRECTION', 'positive',
                'METHOD', 'zscore', 'THRESHOLD', '2.5'
            )
        )

        assert len(anomalies_pos) == 1
        assert anomalies_pos[0].value == 50.0
        assert anomalies_pos[0].signal == 1

        # Cleaned samples should only exclude positive anomaly
        sample_values_pos = [s.value for s in samples_pos]
        assert 50.0 not in sample_values_pos
        assert -30.0 in sample_values_pos  # Negative anomaly should remain
        assert len(samples_pos) == len(data) - 1

        # Test OUTPUT cleaned with DIRECTION negative (only removes negative anomalies)
        result_negative = self.client.execute_command(
            'TS.OUTLIERS', key, '-', '+',
            'OUTPUT', 'cleaned',
            'DIRECTION', 'negative',
            'METHOD', 'zscore', 'THRESHOLD', '2.5'
        )

        res = TSOutliersCleanedResult.parse(result_negative)
        samples_neg = res.samples
        anomalies_neg = convert_anomaly_entries(
            self.client.execute_command(
                'TS.OUTLIERS', key, '-', '+',
                'OUTPUT', 'simple',
                'DIRECTION', 'negative',
                'METHOD', 'zscore', 'THRESHOLD', '2.5'
            )
        )

        assert len(anomalies_neg) == 1
        assert anomalies_neg[0].value == -30.0
        assert anomalies_neg[0].signal == -1

        # Cleaned samples should only exclude negative anomaly
        sample_values_neg = [s.value for s in samples_neg]
        assert -30.0 not in sample_values_neg
        assert 50.0 in sample_values_neg  # Positive anomaly should remain
        assert len(samples_neg) == len(data) - 1

    def test_daily_seasonality_with_spike(self):
        """Test detection on a daily seasonal pattern with an anomalous spike."""
        key = "test:seasonality:daily:spike"

        # Use more history so STL has enough repeated cycles to model the pattern reliably.
        days = 28  # 4 weeks is more stable than 7 days
        spike_day = 14
        spike_hour = 14
        spike_ts = None

        for day in range(days):
            for hour in range(24):
                if 9 <= hour <= 17:
                    value = 100.0 + 20.0 * math.sin((hour - 9) * math.pi / 8)
                else:
                    value = 30.0 + 10.0 * math.sin(hour * math.pi / 12)

                ts = 1000 + (day * 24 + hour) * 3600000

                if day == spike_day and hour == spike_hour:
                    value = 500.0
                    spike_ts = ts

                self.client.execute_command("TS.ADD", key, ts, value)

        items = self.client.execute_command("TS.RANGE", key, "-", "+")
        print(f"items = {len(items)}")

        raw = self.client.execute_command(
            "TS.OUTLIERS", key, "-", "+",
            "OUTPUT", "full",
            "SEASONALITY", 24,
            "METHOD", "zscore", "THRESHOLD", "3.0",
        )

        result = TSOutliersFullResult.parse(raw)

        # Basic structure checks
        assert len(result.samples) > 500
        assert result.method.value == "ZScore"
        assert result.parameters["threshold"] == 3.0
        anomalies = result.anomalies()
        assert anomalies, "Expected at least one anomaly"
        assert anomalies[0].timestamp == spike_ts
        assert anomalies[0].value == 500.0

        # The anomaly should be returned in the outliers structure
        assert len(result.anomalies()) == 1, f"Expected one anomaly, got {result.anomalies()}"
        anomaly = result.anomalies()[0]
        assert anomaly.timestamp == spike_ts
        assert anomaly.value == 500.0
        assert anomaly.signal == 1
        assert anomaly.is_positive_anomaly()



    def test_seasonality_with_ewma_method(self):
        """Test EWMA method with seasonal decomposition."""
        key = 'test:seasonality:ewma'

        # Create pattern with daily seasonality
        data = []
        for day in range(14):
            for hour in range(24):
                base = 100.0
                seasonal = 40.0 * math.sin(hour * math.pi / 12)
                noise = 2.0 * math.sin((day * 24 + hour) * 0.5)

                value = base + seasonal + noise
                data.append(value)

        # Add multiple anomalies
        data[100] = 250.0
        data[200] = 10.0
        data[280] = 270.0

        for i, val in enumerate(data):
            self.client.execute_command('TS.ADD', key, 1000 + i * 3600000, val)

        result = self.client.execute_command(
            'TS.OUTLIERS', key, '-', '+',
            'SEASONALITY', 24,
            'METHOD', 'ewma', 'ALPHA', '0.3'
        )

        anomalies = convert_anomaly_entries(result)
        assert len(anomalies) >= 2
        assert any(abs(a.value - 250.0) < 0.1 for a in anomalies)

    def test_multiple_seasonalities_daily_and_weekly(self):
        """Test detection with both daily and weekly seasonality patterns (OUTPUT full)."""
        key = 'test:seasonality:multiple:daily:weekly'

        # Build data with daily + weekly cycles. Weekly (period=168) MSTL
        # decomposition needs enough cycles to reliably isolate point anomalies
        # from the seasonal/trend fit rather than absorbing them, so use 30
        # weeks rather than the bare minimum.
        data = []
        for week in range(30):
            for day in range(7):
                weekly_factor = 1.3 if day < 5 else 0.6
                for hour in range(24):
                    if 9 <= hour <= 17:
                        daily_component = 80.0 + 30.0 * math.sin((hour - 13) * math.pi / 8)
                    else:
                        daily_component = 40.0 + 15.0 * math.sin(hour * math.pi / 12)
                    value = weekly_factor * daily_component
                    data.append(value)

        # Inject anomalies (compute indices carefully)
        idx_pos1 = 2 * 7 * 24 + 2 * 24 + 14  # week 2, wed 14:00
        idx_neg = 3 * 7 * 24 + 6 * 24 + 3  # week 3, sun 03:00
        idx_pos2 = 4 * 7 * 24 + 4 * 24 + 11  # week 4, fri 11:00

        data[idx_pos1] = 400.0
        data[idx_neg] = -50.0
        data[idx_pos2] = 380.0

        # Add to TS
        for i, val in enumerate(data):
            self.client.execute_command('TS.ADD', key, 1000 + i * 3600000, val)

        # Check stored count
        items = self.client.execute_command("TS.RANGE", key, "-", "+")
        stored_count = len(items)
        assert stored_count == len(data), f"TS.RANGE returned {stored_count}, expected {len(data)}"

        raw = self.client.execute_command(
            'TS.OUTLIERS', key, '-', '+',
            'OUTPUT', 'full',
            'SEASONALITY', 24, 168,  # daily, weekly
            'METHOD', 'zscore', 'THRESHOLD', '3.0'
        )

        result = TSOutliersFullResult.parse(raw)

        # basic checks
        assert result.method.value == "ZScore", f"unexpected method {result.method}"
        assert abs(result.parameters["threshold"] - 3.0) < 1e-12, f"unexpected threshold {result.parameters['threshold']}"
        # Seasonal decomposition can surface extra edge anomalies; require at least the injected spikes.
        assert len(result.anomalies()) >= 3, f"expected at least 3 anomalies, got {len(result.anomalies())}"

        # Verify anomaly details
        anomaly_values = [a.value for a in result.anomalies()]
        assert any(abs(v - 400.0) < 1e-8 for v in anomaly_values)
        assert any(abs(v + 50.0) < 1e-8 for v in anomaly_values)
        assert any(abs(v - 380.0) < 1e-8 for v in anomaly_values)

        # verify positive/negative signal distribution
        positive_anomalies = [a for a in result.anomalies() if a.signal == 1]
        negative_anomalies = [a for a in result.anomalies() if a.signal == -1]
        assert len(positive_anomalies) >= 1, f"expected at least one positive anomaly, got {positive_anomalies}"
        assert len(negative_anomalies) >= 1, f"expected at least one negative anomaly, got {negative_anomalies}"


    def test_multiple_seasonalities_with_trend(self):
        """Test detection with multiple seasonalities and an underlying trend."""
        key = 'test:seasonality:multiple:trend'

        # Create a pattern with daily, weekly cycles, and upward trend.
        # Weekly (period=168) MSTL decomposition needs enough cycles to
        # reliably isolate point anomalies rather than absorbing them into
        # the trend/seasonal fit, so use 30 weeks rather than the bare
        # minimum.
        data = []
        for week in range(30):
            for day in range(7):
                # Weekly component
                weekly_component = 20.0 * math.sin(day * math.pi / 3.5)

                for hour in range(24):
                    # Daily component
                    daily_component = 30.0 * math.sin(hour * math.pi / 12)

                    # Upward trend
                    trend = (week * 7 + day) * 0.5

                    value = 100.0 + weekly_component + daily_component + trend
                    data.append(value)

        # Add anomalies
        data[300] = 500.0  # Strong positive spike
        data[600] = 10.0  # Strong negative spike

        for i, val in enumerate(data):
            self.client.execute_command('TS.ADD', key, 1000 + i * 3600000, val)

        result = self.client.execute_command(
            'TS.OUTLIERS', key, '-', '+',
            'SEASONALITY', 24, 168,
            'METHOD', 'modified-zscore', 'THRESHOLD', '3.5'
        )

        anomalies = convert_anomaly_entries(result)
        assert len(anomalies) >= 2

        anomaly_values = [a.value for a in anomalies]
        assert 500.0 in anomaly_values
        assert 10.0 in anomaly_values

    def test_seasonality_with_moderate_trend(self):
        """Test detection with daily seasonality and a moderate upward trend."""
        key = 'test:seasonality:daily:moderate_trend'

        # Create a pattern with daily and weekly cycles, plus a moderate upward trend
        data = []
        for week in range(8):
            for day in range(7):
                # Weekly component - weekday vs weekend pattern
                weekly_factor = 1.2 if day < 5 else 0.8

                for hour in range(24):
                    # Daily component - business hours pattern
                    if 9 <= hour <= 17:
                        hourly_factor = 1.5
                    elif 6 <= hour <= 21:
                        hourly_factor = 1.0
                    else:
                        hourly_factor = 0.5

                    # Moderate upward trend
                    trend = (week * 7 + day) * 0.5

                    # Base value with seasonal components
                    base = 50.0
                    value = base + trend + (weekly_factor * 20.0) + (hourly_factor * 15.0)

                    # Add small noise for realistic variance
                    noise = math.sin(hour * 0.5) * 2.0
                    data.append(value + noise)

        # Add clear anomalies that stand out from the pattern
        data[300] = 500.0  # Strong positive spike
        data[600] = -50.0  # Strong negative spike
        data[900] = 450.0  # Another positive spike

        for i, val in enumerate(data):
            self.client.execute_command('TS.ADD', key, 1000 + i * 3600000, val)

        # Use a single seasonality period (daily) for more stable decomposition
        result = self.client.execute_command(
            'TS.OUTLIERS', key, '-', '+',
            'SEASONALITY', 24,  # daily seasonality
            'METHOD', 'modified-zscore',  # More robust to outliers
            'THRESHOLD', 3.5
        )

        anomalies = convert_anomaly_entries(result)

        # Should detect at least 2 of the 3 anomalies
        assert len(anomalies) >= 2

        # Check for specific anomaly values
        anomaly_values = [a.value for a in anomalies]
        assert any(abs(v - 500.0) < 1.0 for v in anomaly_values)

        # Verify we have both positive and negative detections
        signals = [a.signal for a in anomalies]
        assert 1 in signals  # At least one positive anomaly
