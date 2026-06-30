import math
import pytest
from valkey import ResponseError
from valkeytestframework.util.waiters import *
from valkeytestframework.conftest import resource_port_tracker
from valkey_timeseries_test_case import ValkeyTimeSeriesTestCaseBase


class TestTimeSeriesSanitize(ValkeyTimeSeriesTestCaseBase):
    """Integration tests for TS.SANITIZE command.

    TS.SANITIZE key fromTimestamp toTimestamp
        [POLICY <policy> [options]]
        [STORE destinationKey
            [MERGE]
            [RETENTION retentionPeriod]
            [ENCODING encoding]
            [CHUNK_SIZE chunkSize]
            [DUPLICATE_POLICY duplicatePolicy]
            [SIGNIFICANT_DIGITS significantDigits | DECIMAL_DIGITS decimalDigits]
            [METRIC metric]
            [IGNORE ignoreMaxTimediff ignoreMaxValDiff]
        ]

    Sanitizes missing (NaN/infinite) values in a time series within the
    given timestamp range. Without STORE, returns the sanitized samples as
    an array. With STORE, returns the number of samples written.

    POLICY is optional and defaults to DROP.
    """

    # ------------------------------------------------------------------
    # Helper methods
    # ------------------------------------------------------------------

    def _create_series_with_data(self, key, timestamps_values):
        """Create a series and add samples. timestamps_values is a list of
        (timestamp, value) tuples."""
        self.client.execute_command('TS.CREATE', key)
        for ts, val in timestamps_values:
            self.client.execute_command('TS.ADD', key, ts, val)

    def _get_all_samples(self, key):
        """Return list of (timestamp, float_value) from TS.RANGE."""
        result = self.client.execute_command('TS.RANGE', key, '-', '+')
        return [(entry[0], float(entry[1])) for entry in result]

    def _parse_sanitize_result(self, result):
        """Parse TS.SANITIZE result (array of [timestamp, value] pairs)
        into list of (timestamp, float_value)."""
        return [(entry[0], float(entry[1])) for entry in result]

    # ==================================================================
    # ERROR policy
    # ==================================================================

    def test_error_no_missing(self):
        """ERROR policy succeeds when no missing values are present.
        Returns all samples unchanged."""
        self._create_series_with_data('ts1', [
            (1000, 1.0),
            (2000, 2.0),
            (3000, 3.0),
        ])
        result = self.client.execute_command(
            'TS.SANITIZE', 'ts1', 1000, 3000, 'POLICY', 'ERROR'
        )
        samples = self._parse_sanitize_result(result)
        assert samples == [(1000, 1.0), (2000, 2.0), (3000, 3.0)]
        # Data unchanged in the series
        stored = self._get_all_samples('ts1')
        assert stored == [(1000, 1.0), (2000, 2.0), (3000, 3.0)]

    def test_error_on_nan(self):
        """ERROR policy returns error when NaN is present."""
        self._create_series_with_data('ts1', [
            (1000, 1.0),
            (2000, float('nan')),
            (3000, 3.0),
        ])
        with pytest.raises(ResponseError, match='missing values'):
            self.client.execute_command(
                'TS.SANITIZE', 'ts1', 1000, 3000, 'POLICY', 'ERROR'
            )

    def test_error_on_inf(self):
        """ERROR policy returns error when +Inf is present."""
        self._create_series_with_data('ts1', [
            (1000, 1.0),
            (2000, float('inf')),
            (3000, 3.0),
        ])
        with pytest.raises(ResponseError, match='missing values'):
            self.client.execute_command(
                'TS.SANITIZE', 'ts1', 1000, 3000, 'POLICY', 'ERROR'
            )

    def test_error_on_neg_inf(self):
        """ERROR policy returns error when -Inf is present."""
        self._create_series_with_data('ts1', [
            (1000, 1.0),
            (2000, float('-inf')),
            (3000, 3.0),
        ])
        with pytest.raises(ResponseError, match='missing values'):
            self.client.execute_command(
                'TS.SANITIZE', 'ts1', 1000, 3000, 'POLICY', 'ERROR'
            )

    # ==================================================================
    # DROP policy
    # ==================================================================

    def test_drop_removes_nan(self):
        """DROP policy removes samples with NaN values."""
        self._create_series_with_data('ts1', [
            (1000, 1.0),
            (2000, float('nan')),
            (3000, 3.0),
            (4000, float('nan')),
            (5000, 5.0),
        ])
        result = self.client.execute_command(
            'TS.SANITIZE', 'ts1', 1000, 5000, 'POLICY', 'DROP'
        )
        samples = self._parse_sanitize_result(result)
        assert samples == [(1000, 1.0), (3000, 3.0), (5000, 5.0)]
        stored = self._get_all_samples('ts1')
        assert stored == [(1000, 1.0), (3000, 3.0), (5000, 5.0)]

    def test_drop_removes_inf(self):
        """DROP policy removes samples with +Inf and -Inf values."""
        self._create_series_with_data('ts1', [
            (1000, 1.0),
            (2000, float('inf')),
            (3000, 3.0),
            (4000, float('-inf')),
            (5000, 5.0),
        ])
        result = self.client.execute_command(
            'TS.SANITIZE', 'ts1', 1000, 5000, 'POLICY', 'DROP'
        )
        samples = self._parse_sanitize_result(result)
        assert samples == [(1000, 1.0), (3000, 3.0), (5000, 5.0)]
        stored = self._get_all_samples('ts1')
        assert stored == [(1000, 1.0), (3000, 3.0), (5000, 5.0)]

    def test_drop_all_missing(self):
        """DROP policy with all missing values returns empty result."""
        self._create_series_with_data('ts1', [
            (1000, float('nan')),
            (2000, float('nan')),
            (3000, float('nan')),
        ])
        result = self.client.execute_command(
            'TS.SANITIZE', 'ts1', 1000, 3000, 'POLICY', 'DROP'
        )
        samples = self._parse_sanitize_result(result)
        assert samples == []
        stored = self._get_all_samples('ts1')
        assert stored == []

    def test_drop_all_valid(self):
        """DROP policy with all valid samples returns all samples unchanged."""
        self._create_series_with_data('ts1', [
            (1000, 1.0),
            (2000, 2.0),
            (3000, 3.0),
        ])
        result = self.client.execute_command(
            'TS.SANITIZE', 'ts1', 1000, 3000, 'POLICY', 'DROP'
        )
        samples = self._parse_sanitize_result(result)
        assert samples == [(1000, 1.0), (2000, 2.0), (3000, 3.0)]
        stored = self._get_all_samples('ts1')
        assert len(stored) == 3

    # ==================================================================
    # Default policy (POLICY omitted → DROP)
    # ==================================================================

    def test_default_policy_is_drop(self):
        """When POLICY is omitted, DROP is applied by default."""
        self._create_series_with_data('ts1', [
            (1000, 1.0),
            (2000, float('nan')),
            (3000, 3.0),
            (4000, float('nan')),
            (5000, 5.0),
        ])
        result = self.client.execute_command(
            'TS.SANITIZE', 'ts1', 1000, 5000
        )
        samples = self._parse_sanitize_result(result)
        assert samples == [(1000, 1.0), (3000, 3.0), (5000, 5.0)]
        stored = self._get_all_samples('ts1')
        assert stored == [(1000, 1.0), (3000, 3.0), (5000, 5.0)]

    def test_default_policy_all_valid(self):
        """Default DROP policy with all valid values returns all samples."""
        self._create_series_with_data('ts1', [
            (1000, 1.0),
            (2000, 2.0),
        ])
        result = self.client.execute_command(
            'TS.SANITIZE', 'ts1', 1000, 2000
        )
        samples = self._parse_sanitize_result(result)
        assert samples == [(1000, 1.0), (2000, 2.0)]

    # ==================================================================
    # FILL policy
    # ==================================================================

    def test_fill_constant(self):
        """FILL policy replaces NaN/Inf with a constant value."""
        self._create_series_with_data('ts1', [
            (1000, 1.0),
            (2000, float('nan')),
            (3000, 3.0),
            (4000, float('inf')),
            (5000, 5.0),
        ])
        result = self.client.execute_command(
            'TS.SANITIZE', 'ts1', 1000, 5000, 'POLICY', 'FILL', 0.0
        )
        samples = self._parse_sanitize_result(result)
        assert samples == [
            (1000, 1.0),
            (2000, 0.0),
            (3000, 3.0),
            (4000, 0.0),
            (5000, 5.0),
        ]
        stored = self._get_all_samples('ts1')
        assert stored == [
            (1000, 1.0),
            (2000, 0.0),
            (3000, 3.0),
            (4000, 0.0),
            (5000, 5.0),
        ]

    def test_fill_negative(self):
        """FILL policy with a negative value."""
        self._create_series_with_data('ts1', [
            (1000, 1.0),
            (2000, float('nan')),
        ])
        result = self.client.execute_command(
            'TS.SANITIZE', 'ts1', 1000, 2000, 'POLICY', 'FILL', -999.0
        )
        samples = self._parse_sanitize_result(result)
        assert samples == [(1000, 1.0), (2000, -999.0)]
        stored = self._get_all_samples('ts1')
        assert stored == [(1000, 1.0), (2000, -999.0)]

    def test_fill_neg_inf(self):
        """FILL policy replaces -Inf with constant value."""
        self._create_series_with_data('ts1', [
            (1000, 1.0),
            (2000, float('-inf')),
        ])
        result = self.client.execute_command(
            'TS.SANITIZE', 'ts1', 1000, 2000, 'POLICY', 'FILL', 42.0
        )
        samples = self._parse_sanitize_result(result)
        assert samples == [(1000, 1.0), (2000, 42.0)]

    # ==================================================================
    # FORWARDFILL policy
    # ==================================================================

    def test_forwardfill_basic(self):
        """FORWARDFILL carries last valid observation forward."""
        self._create_series_with_data('ts1', [
            (1000, 1.0),
            (2000, float('nan')),
            (3000, 3.0),
            (4000, float('nan')),
            (5000, 5.0),
        ])
        result = self.client.execute_command(
            'TS.SANITIZE', 'ts1', 1000, 5000, 'POLICY', 'FORWARDFILL'
        )
        samples = self._parse_sanitize_result(result)
        assert samples == [
            (1000, 1.0),
            (2000, 1.0),
            (3000, 3.0),
            (4000, 3.0),
            (5000, 5.0),
        ]
        stored = self._get_all_samples('ts1')
        assert stored == [
            (1000, 1.0),
            (2000, 1.0),
            (3000, 3.0),
            (4000, 3.0),
            (5000, 5.0),
        ]

    def test_forwardfill_leading_nan(self):
        """FORWARDFILL leaves leading NaN unchanged (no last valid)."""
        self._create_series_with_data('ts1', [
            (1000, float('nan')),
            (2000, 2.0),
            (3000, 3.0),
        ])
        result = self.client.execute_command(
            'TS.SANITIZE', 'ts1', 1000, 3000, 'POLICY', 'FORWARDFILL'
        )
        samples = self._parse_sanitize_result(result)
        assert len(samples) == 3
        # Leading NaN stays NaN (no prior valid value to carry forward)
        assert math.isnan(samples[0][1])
        assert samples[1][1] == 2.0
        assert samples[2][1] == 3.0

    def test_forwardfill_trailing_nan(self):
        """FORWARDFILL carries forward into trailing NaN."""
        self._create_series_with_data('ts1', [
            (1000, 1.0),
            (2000, 2.0),
            (3000, float('nan')),
            (4000, float('nan')),
        ])
        result = self.client.execute_command(
            'TS.SANITIZE', 'ts1', 1000, 4000, 'POLICY', 'FORWARDFILL'
        )
        samples = self._parse_sanitize_result(result)
        assert samples == [
            (1000, 1.0),
            (2000, 2.0),
            (3000, 2.0),
            (4000, 2.0),
        ]

    # ==================================================================
    # BACKWARDFILL policy
    # ==================================================================

    def test_backwardfill_basic(self):
        """BACKWARDFILL carries next valid observation backward."""
        self._create_series_with_data('ts1', [
            (1000, 1.0),
            (2000, float('nan')),
            (3000, float('nan')),
            (4000, 4.0),
            (5000, 5.0),
        ])
        result = self.client.execute_command(
            'TS.SANITIZE', 'ts1', 1000, 5000, 'POLICY', 'BACKWARDFILL'
        )
        samples = self._parse_sanitize_result(result)
        assert samples == [
            (1000, 1.0),
            (2000, 4.0),
            (3000, 4.0),
            (4000, 4.0),
            (5000, 5.0),
        ]
        stored = self._get_all_samples('ts1')
        assert stored == [
            (1000, 1.0),
            (2000, 4.0),
            (3000, 4.0),
            (4000, 4.0),
            (5000, 5.0),
        ]

    def test_backwardfill_trailing_nan(self):
        """BACKWARDFILL leaves trailing NaN unchanged (no next valid)."""
        self._create_series_with_data('ts1', [
            (1000, 1.0),
            (2000, 2.0),
            (3000, float('nan')),
        ])
        result = self.client.execute_command(
            'TS.SANITIZE', 'ts1', 1000, 3000, 'POLICY', 'BACKWARDFILL'
        )
        samples = self._parse_sanitize_result(result)
        # Trailing NaN stays NaN
        assert len(samples) == 3
        assert samples[0][1] == 1.0
        assert samples[1][1] == 2.0
        assert math.isnan(samples[2][1])

    def test_backwardfill_leading_nan(self):
        """BACKWARDFILL carries backward into leading NaN."""
        self._create_series_with_data('ts1', [
            (1000, float('nan')),
            (2000, float('nan')),
            (3000, 3.0),
            (4000, 4.0),
        ])
        result = self.client.execute_command(
            'TS.SANITIZE', 'ts1', 1000, 4000, 'POLICY', 'BACKWARDFILL'
        )
        samples = self._parse_sanitize_result(result)
        assert samples == [
            (1000, 3.0),
            (2000, 3.0),
            (3000, 3.0),
            (4000, 4.0),
        ]

    # ==================================================================
    # FILLMEAN policy
    # ==================================================================

    def test_fillmean(self):
        """FILLMEAN replaces NaN with mean of valid values."""
        self._create_series_with_data('ts1', [
            (1000, 1.0),
            (2000, float('nan')),
            (3000, 3.0),
            (4000, float('nan')),
            (5000, 5.0),
        ])
        result = self.client.execute_command(
            'TS.SANITIZE', 'ts1', 1000, 5000, 'POLICY', 'FILLMEAN'
        )
        samples = self._parse_sanitize_result(result)
        # Mean of [1, 3, 5] = 3.0
        assert samples == [
            (1000, 1.0),
            (2000, 3.0),
            (3000, 3.0),
            (4000, 3.0),
            (5000, 5.0),
        ]
        stored = self._get_all_samples('ts1')
        assert stored == [
            (1000, 1.0),
            (2000, 3.0),
            (3000, 3.0),
            (4000, 3.0),
            (5000, 5.0),
        ]

    # ==================================================================
    # FILLMEDIAN policy
    # ==================================================================

    def test_fillmedian(self):
        """FILLMEDIAN replaces NaN with median of valid values."""
        self._create_series_with_data('ts1', [
            (1000, 1.0),
            (2000, float('nan')),
            (3000, 3.0),
            (4000, float('nan')),
            (5000, 10.0),
        ])
        result = self.client.execute_command(
            'TS.SANITIZE', 'ts1', 1000, 5000, 'POLICY', 'FILLMEDIAN'
        )
        samples = self._parse_sanitize_result(result)
        # Median of [1, 3, 10] = 3.0
        assert samples == [
            (1000, 1.0),
            (2000, 3.0),
            (3000, 3.0),
            (4000, 3.0),
            (5000, 10.0),
        ]
        stored = self._get_all_samples('ts1')
        assert stored == [
            (1000, 1.0),
            (2000, 3.0),
            (3000, 3.0),
            (4000, 3.0),
            (5000, 10.0),
        ]

    # ==================================================================
    # INTERPOLATE policy
    # ==================================================================

    def test_interpolate_fills_gaps(self):
        """INTERPOLATE linearly interpolates between valid neighbors."""
        self._create_series_with_data('ts1', [
            (1000, 1.0),
            (2000, float('nan')),
            (3000, float('nan')),
            (4000, 4.0),
            (5000, 5.0),
        ])
        result = self.client.execute_command(
            'TS.SANITIZE', 'ts1', 1000, 5000, 'POLICY', 'INTERPOLATE'
        )
        samples = self._parse_sanitize_result(result)
        assert samples == [
            (1000, 1.0),
            (2000, 2.0),
            (3000, 3.0),
            (4000, 4.0),
            (5000, 5.0),
        ]
        stored = self._get_all_samples('ts1')
        assert stored == [
            (1000, 1.0),
            (2000, 2.0),
            (3000, 3.0),
            (4000, 4.0),
            (5000, 5.0),
        ]

    def test_interpolate_fills_edges(self):
        """INTERPOLATE fills edge NaN with nearest valid value."""
        self._create_series_with_data('ts1', [
            (1000, float('nan')),
            (2000, float('nan')),
            (3000, 3.0),
            (4000, 4.0),
            (5000, float('nan')),
        ])
        result = self.client.execute_command(
            'TS.SANITIZE', 'ts1', 1000, 5000, 'POLICY', 'INTERPOLATE'
        )
        samples = self._parse_sanitize_result(result)
        assert samples == [
            (1000, 3.0),
            (2000, 3.0),
            (3000, 3.0),
            (4000, 4.0),
            (5000, 4.0),
        ]
        stored = self._get_all_samples('ts1')
        assert stored == [
            (1000, 3.0),
            (2000, 3.0),
            (3000, 3.0),
            (4000, 4.0),
            (5000, 4.0),
        ]

    # ==================================================================
    # FORWARDBACKWARDFILL policy
    # ==================================================================

    def test_forwardbackwardfill_handles_leading(self):
        """FORWARDBACKWARDFILL handles leading NaN."""
        self._create_series_with_data('ts1', [
            (1000, float('nan')),
            (2000, float('nan')),
            (3000, 3.0),
            (4000, float('nan')),
            (5000, 5.0),
        ])
        result = self.client.execute_command(
            'TS.SANITIZE', 'ts1', 1000, 5000, 'POLICY', 'FORWARDBACKWARDFILL'
        )
        samples = self._parse_sanitize_result(result)
        # Leading NaN filled backward from 3.0, interior NaN filled forward
        assert samples[0][1] == 3.0  # Leading filled backward
        assert samples[1][1] == 3.0  # Leading filled backward
        assert samples[2][1] == 3.0
        assert samples[3][1] == 3.0  # Interior filled forward
        assert samples[4][1] == 5.0

    def test_forwardbackwardfill_handles_trailing(self):
        """FORWARDBACKWARDFILL handles trailing NaN."""
        self._create_series_with_data('ts1', [
            (1000, 1.0),
            (2000, 2.0),
            (3000, float('nan')),
            (4000, float('nan')),
        ])
        result = self.client.execute_command(
            'TS.SANITIZE', 'ts1', 1000, 4000, 'POLICY', 'FORWARDBACKWARDFILL'
        )
        samples = self._parse_sanitize_result(result)
        assert samples[0][1] == 1.0
        assert samples[1][1] == 2.0
        assert samples[2][1] == 2.0  # Forward-filled
        assert samples[3][1] == 2.0  # Forward-filled

    def test_forwardbackwardfill_all_missing(self):
        """FORWARDBACKWARDFILL with all NaN returns all NaN (nothing to fill from)."""
        self._create_series_with_data('ts1', [
            (1000, float('nan')),
            (2000, float('nan')),
            (3000, float('nan')),
        ])
        result = self.client.execute_command(
            'TS.SANITIZE', 'ts1', 1000, 3000, 'POLICY', 'FORWARDBACKWARDFILL'
        )
        samples = self._parse_sanitize_result(result)
        assert len(samples) == 3
        for _, v in samples:
            assert math.isnan(v)

    # ==================================================================
    # MOVINGAVERAGE policy
    # ==================================================================

    def test_movingaverage_single_gap(self):
        """MOVINGAVERAGE with window=3 fills a single gap."""
        self._create_series_with_data('ts1', [
            (1000, 1.0),
            (2000, 2.0),
            (3000, float('nan')),
            (4000, 4.0),
            (5000, 5.0),
        ])
        result = self.client.execute_command(
            'TS.SANITIZE', 'ts1', 1000, 5000,
            'POLICY', 'MOVINGAVERAGE', 3
        )
        samples = self._parse_sanitize_result(result)
        # Returns all 5 imputed samples
        assert len(samples) == 5
        # NaN at index 2 should be ~3.0 (mean of 2.0 and 4.0)
        assert abs(samples[2][1] - 3.0) < 1e-10

    def test_movingaverage_consecutive_gaps(self):
        """MOVINGAVERAGE handles consecutive NaN with multiple passes."""
        self._create_series_with_data('ts1', [
            (1000, 1.0),
            (2000, float('nan')),
            (3000, float('nan')),
            (4000, 4.0),
            (5000, 5.0),
        ])
        result = self.client.execute_command(
            'TS.SANITIZE', 'ts1', 1000, 5000,
            'POLICY', 'MOVINGAVERAGE', 3
        )
        samples = self._parse_sanitize_result(result)
        assert len(samples) == 5
        # All values should be finite after imputation
        for _, v in samples:
            assert not math.isnan(v)
            assert not math.isinf(v)

    def test_movingaverage_window_5(self):
        """MOVINGAVERAGE with larger window=5."""
        self._create_series_with_data('ts1', [
            (1000, 1.0),
            (2000, 2.0),
            (3000, float('nan')),
            (4000, 4.0),
            (5000, 5.0),
            (6000, 6.0),
            (7000, 7.0),
        ])
        result = self.client.execute_command(
            'TS.SANITIZE', 'ts1', 1000, 7000,
            'POLICY', 'MOVINGAVERAGE', 5
        )
        samples = self._parse_sanitize_result(result)
        assert len(samples) == 7
        # All values finite
        for _, v in samples:
            assert not math.isnan(v)

    def test_movingaverage_rejects_even_window(self):
        """MOVINGAVERAGE rejects even window size."""
        self._create_series_with_data('ts1', [
            (1000, 1.0),
            (2000, 2.0),
            (3000, 3.0),
        ])
        with pytest.raises(ResponseError, match='odd positive integer'):
            self.client.execute_command(
                'TS.SANITIZE', 'ts1', 1000, 3000,
                'POLICY', 'MOVINGAVERAGE', 4
            )

    def test_movingaverage_rejects_zero_window(self):
        """MOVINGAVERAGE rejects zero window size."""
        self._create_series_with_data('ts1', [
            (1000, 1.0),
            (2000, 2.0),
            (3000, 3.0),
        ])
        with pytest.raises(ResponseError, match='odd positive integer'):
            self.client.execute_command(
                'TS.SANITIZE', 'ts1', 1000, 3000,
                'POLICY', 'MOVINGAVERAGE', 0
            )

    # ==================================================================
    # SEASONAL policy
    # ==================================================================

    def test_seasonal_basic(self):
        """SEASONAL fills NaN using seasonal median."""
        # Period 3: positions 0,1,2,0,1,2,0,1,2
        self._create_series_with_data('ts1', [
            (1000, 10.0),   # pos 0
            (2000, 20.0),   # pos 1
            (3000, 30.0),   # pos 2
            (4000, 11.0),   # pos 0
            (5000, 21.0),   # pos 1
            (6000, 31.0),   # pos 2
            (7000, float('nan')),  # pos 0 — median of [10, 11] = 10.5
            (8000, 22.0),   # pos 1
            (9000, 32.0),   # pos 2
        ])
        result = self.client.execute_command(
            'TS.SANITIZE', 'ts1', 1000, 9000,
            'POLICY', 'SEASONAL', 3
        )
        samples = self._parse_sanitize_result(result)
        assert len(samples) == 9
        assert abs(samples[6][1] - 10.5) < 1e-10
        stored = self._get_all_samples('ts1')
        assert abs(stored[6][1] - 10.5) < 1e-10

    def test_seasonal_insufficient_data(self):
        """SEASONAL errors when period exceeds data length."""
        self._create_series_with_data('ts1', [
            (1000, 1.0),
            (2000, float('nan')),
            (3000, 3.0),
        ])
        with pytest.raises(ResponseError, match='insufficient data'):
            self.client.execute_command(
                'TS.SANITIZE', 'ts1', 1000, 3000,
                'POLICY', 'SEASONAL', 4
            )

    def test_seasonal_rejects_zero_period(self):
        """SEASONAL rejects zero period."""
        self._create_series_with_data('ts1', [
            (1000, 1.0),
            (2000, 2.0),
            (3000, 3.0),
        ])
        with pytest.raises(ResponseError, match='positive integer'):
            self.client.execute_command(
                'TS.SANITIZE', 'ts1', 1000, 3000,
                'POLICY', 'SEASONAL', 0
            )

    def test_seasonal_rejects_too_many_missing(self):
        """SEASONAL errors when >50% of a seasonal bucket is missing."""
        # Period 2: bucket 0 has indices [0, 2, 4], bucket 1 has [1, 3, 5]
        self._create_series_with_data('ts1', [
            (1000, float('nan')),
            (2000, 1.0),
            (3000, float('nan')),
            (4000, 2.0),
            (5000, float('nan')),
            (6000, 3.0),
        ])
        with pytest.raises(ResponseError, match='invalid parameter'):
            self.client.execute_command(
                'TS.SANITIZE', 'ts1', 1000, 6000,
                'POLICY', 'SEASONAL', 2
            )

    # ==================================================================
    # STORE clause
    # ==================================================================

    def test_store_creates_new_key(self):
        """STORE writes sanitized samples to a new destination key."""
        self._create_series_with_data('ts1', [
            (1000, 1.0),
            (2000, float('nan')),
            (3000, 3.0),
            (4000, float('nan')),
            (5000, 5.0),
        ])
        count = self.client.execute_command(
            'TS.SANITIZE', 'ts1', 1000, 5000,
            'POLICY', 'DROP', 'STORE', 'dest1'
        )
        # STORE returns the number of samples written
        assert count == 3

        # Destination key exists with sanitized samples
        assert self.client.execute_command('EXISTS', 'dest1') == 1
        stored = self._get_all_samples('dest1')
        assert stored == [(1000, 1.0), (3000, 3.0), (5000, 5.0)]

        # Source key is also sanitized
        source = self._get_all_samples('ts1')
        assert source == [(1000, 1.0), (3000, 3.0), (5000, 5.0)]

    def test_store_overwrites_existing_key(self):
        """STORE (without MERGE) overwrites an existing destination key."""
        self._create_series_with_data('ts1', [
            (1000, 1.0),
            (2000, float('nan')),
            (3000, 3.0),
        ])

        # Pre-create dest key with existing data
        self.client.execute_command('TS.CREATE', 'dest1')
        self.client.execute_command('TS.ADD', 'dest1', 500, 99.0)
        self.client.execute_command('TS.ADD', 'dest1', 1500, 88.0)

        count = self.client.execute_command(
            'TS.SANITIZE', 'ts1', 1000, 3000,
            'POLICY', 'DROP', 'STORE', 'dest1'
        )
        assert count == 2

        # Destination is overwritten (only new samples remain)
        stored = self._get_all_samples('dest1')
        assert stored == [(1000, 1.0), (3000, 3.0)]

    def test_store_merge_with_existing_key(self):
        """STORE with MERGE combines sanitized samples into existing key."""
        self._create_series_with_data('ts1', [
            (1000, 1.0),
            (2000, float('nan')),
            (3000, 3.0),
            (4000, float('nan')),
            (5000, 5.0),
        ])

        # Pre-create dest key with one sample that overlaps
        self.client.execute_command('TS.CREATE', 'dest1')
        self.client.execute_command('TS.ADD', 'dest1', 500, 99.0)
        self.client.execute_command('TS.ADD', 'dest1', 1000, 999.0)  # overlaps ts=1000

        count = self.client.execute_command(
            'TS.SANITIZE', 'ts1', 1000, 5000,
            'POLICY', 'DROP', 'STORE', 'dest1', 'MERGE'
        )
        assert count == 3  # 3 new samples merged into existing key

        stored = self._get_all_samples('dest1')
        assert len(stored) == 4  # 500, 1000, 3000, 5000
        assert stored == [(500, 99.0), (1000, 1.0), (3000, 3.0), (5000, 5.0)]

    def test_store_with_retention(self):
        """STORE with RETENTION sets retention on the destination key."""
        self._create_series_with_data('ts1', [
            (1000, 1.0),
            (2000, float('nan')),
            (3000, 3.0),
        ])
        self.client.execute_command(
            'TS.SANITIZE', 'ts1', 1000, 3000,
            'POLICY', 'DROP', 'STORE', 'dest1', 'RETENTION', 5000
        )
        info = self.client.execute_command('TS.INFO', 'dest1')
        info_dict = {info[i]: info[i + 1] for i in range(0, len(info), 2)}
        assert info_dict[b'retentionTime'] == 5000

    def test_store_with_chunk_size(self):
        """STORE with CHUNK_SIZE sets chunk size on the destination key."""
        self._create_series_with_data('ts1', [
            (1000, 1.0),
            (2000, 2.0),
            (3000, 3.0),
        ])
        self.client.execute_command(
            'TS.SANITIZE', 'ts1', 1000, 3000,
            'POLICY', 'FILLMEAN', 'STORE', 'dest1', 'CHUNK_SIZE', 128
        )
        info = self.client.execute_command('TS.INFO', 'dest1')
        info_dict = {info[i]: info[i + 1] for i in range(0, len(info), 2)}
        assert info_dict[b'chunkSize'] == 128

    def test_store_with_duplicate_policy(self):
        """STORE with DUPLICATE_POLICY sets policy on destination key."""
        self._create_series_with_data('ts1', [
            (1000, 1.0),
            (2000, 2.0),
            (3000, 3.0),
        ])
        self.client.execute_command(
            'TS.SANITIZE', 'ts1', 1000, 3000,
            'POLICY', 'FILLMEAN', 'STORE', 'dest1',
            'DUPLICATE_POLICY', 'SUM'
        )
        info = self.client.execute_command('TS.INFO', 'dest1')
        info_dict = {info[i]: info[i + 1] for i in range(0, len(info), 2)}
        assert info_dict[b'duplicatePolicy'] == b'sum'

    def test_store_with_encoding(self):
        """STORE with ENCODING sets compression on the destination key."""
        self._create_series_with_data('ts1', [
            (1000, 1.0),
            (2000, 2.0),
            (3000, 3.0),
        ])
        self.client.execute_command(
            'TS.SANITIZE', 'ts1', 1000, 3000,
            'POLICY', 'FILLMEAN', 'STORE', 'dest1',
            'ENCODING', 'UNCOMPRESSED'
        )
        info = self.client.execute_command('TS.INFO', 'dest1')
        info_dict = {info[i]: info[i + 1] for i in range(0, len(info), 2)}
        assert info_dict[b'chunkType'] == b'uncompressed'

    def test_store_with_metric(self):
        """STORE with METRIC sets metric name on the destination key."""
        self._create_series_with_data('ts1', [
            (1000, 1.0),
            (2000, 2.0),
            (3000, 3.0),
        ])
        self.client.execute_command(
            'TS.SANITIZE', 'ts1', 1000, 3000,
            'POLICY', 'FILLMEAN', 'STORE', 'dest1',
            'METRIC', 'sanitized_metric'
        )
        # Verify the destination key exists with sanitized data
        stored = self._get_all_samples('dest1')
        assert len(stored) == 3

    def test_store_without_policy_defaults_drop(self):
        """STORE without explicit POLICY defaults to DROP."""
        self._create_series_with_data('ts1', [
            (1000, 1.0),
            (2000, float('nan')),
            (3000, 3.0),
            (4000, float('nan')),
            (5000, 5.0),
        ])
        count = self.client.execute_command(
            'TS.SANITIZE', 'ts1', 1000, 5000, 'STORE', 'dest1'
        )
        assert count == 3
        stored = self._get_all_samples('dest1')
        assert stored == [(1000, 1.0), (3000, 3.0), (5000, 5.0)]

    def test_store_policy_before_store(self):
        """POLICY specified before STORE is respected."""
        self._create_series_with_data('ts1', [
            (1000, 1.0),
            (2000, float('nan')),
            (3000, 3.0),
        ])
        count = self.client.execute_command(
            'TS.SANITIZE', 'ts1', 1000, 3000,
            'POLICY', 'INTERPOLATE', 'STORE', 'dest1'
        )
        assert count == 3
        stored = self._get_all_samples('dest1')
        # NaN at 2000 should be interpolated: (1+3)/2 = 2.0
        assert stored == [(1000, 1.0), (2000, 2.0), (3000, 3.0)]

    def test_store_returns_count_not_samples(self):
        """STORE returns integer count, not an array of samples."""
        self._create_series_with_data('ts1', [
            (1000, 1.0),
            (2000, 2.0),
            (3000, 3.0),
        ])
        count = self.client.execute_command(
            'TS.SANITIZE', 'ts1', 1000, 3000,
            'POLICY', 'DROP', 'STORE', 'dest1'
        )
        assert isinstance(count, int)
        assert count == 3

    def test_store_on_nonexistent_source(self):
        """STORE on a nonexistent source key returns error."""
        with pytest.raises(ResponseError, match='key does not exist'):
            self.client.execute_command(
                'TS.SANITIZE', 'nonexistent', 1000, 2000,
                'POLICY', 'DROP', 'STORE', 'dest1'
            )

    def test_store_with_multiple_options(self):
        """STORE with multiple creation options applied together."""
        self._create_series_with_data('ts1', [
            (1000, 1.0),
            (2000, float('nan')),
            (3000, 3.0),
        ])
        self.client.execute_command(
            'TS.SANITIZE', 'ts1', 1000, 3000,
            'POLICY', 'INTERPOLATE', 'STORE', 'dest1',
            'RETENTION', 10000,
            'CHUNK_SIZE', 256,
            'DUPLICATE_POLICY', 'MIN',
            'ENCODING', 'UNCOMPRESSED',
        )
        info = self.client.execute_command('TS.INFO', 'dest1')
        info_dict = {info[i]: info[i + 1] for i in range(0, len(info), 2)}
        assert info_dict[b'retentionTime'] == 10000
        assert info_dict[b'chunkSize'] == 256
        assert info_dict[b'duplicatePolicy'] == b'min'
        assert info_dict[b'chunkType'] == b'uncompressed'

    # ==================================================================
    # Edge cases
    # ==================================================================

    def test_key_not_found(self):
        """Returns error when key does not exist."""
        with pytest.raises(ResponseError, match='key does not exist'):
            self.client.execute_command(
                'TS.SANITIZE', 'nonexistent', 1000, 2000,
                'POLICY', 'ERROR'
            )

    def test_empty_range(self):
        """Returns empty array when no samples exist in the range."""
        self._create_series_with_data('ts1', [
            (1000, 1.0),
            (2000, 2.0),
        ])
        result = self.client.execute_command(
            'TS.SANITIZE', 'ts1', 5000, 6000,
            'POLICY', 'ERROR'
        )
        samples = self._parse_sanitize_result(result)
        assert samples == []

    def test_range_partial(self):
        """Only samples within the specified range are sanitized;
        samples outside the range are unaffected."""
        self._create_series_with_data('ts1', [
            (1000, float('nan')),
            (2000, 2.0),
            (3000, float('nan')),
            (4000, 4.0),
        ])
        # Only sanitize the middle range [2000, 3000]
        result = self.client.execute_command(
            'TS.SANITIZE', 'ts1', 2000, 3000,
            'POLICY', 'INTERPOLATE'
        )
        samples = self._parse_sanitize_result(result)
        # Returns 2 samples in range [2000, 3000]
        assert len(samples) == 2
        assert samples[0] == (2000, 2.0)
        # Sample at 3000 is NaN; with only the neighbor at 2000=2.0
        # in range, interpolation fills with the nearest valid value (2.0)
        assert abs(samples[1][1] - 2.0) < 1e-10

        # Verify in-series: sample at 1000 (outside range) still NaN
        stored = self._get_all_samples('ts1')
        assert math.isnan(stored[0][1])
        assert stored[1][1] == 2.0
        assert abs(stored[2][1] - 2.0) < 1e-10
        assert stored[3][1] == 4.0

    def test_no_missing_values(self):
        """Returns all samples unchanged when no missing values present."""
        self._create_series_with_data('ts1', [
            (1000, 1.0),
            (2000, 2.0),
            (3000, 3.0),
        ])
        result = self.client.execute_command(
            'TS.SANITIZE', 'ts1', 1000, 3000,
            'POLICY', 'FILL', 0.0
        )
        samples = self._parse_sanitize_result(result)
        assert samples == [(1000, 1.0), (2000, 2.0), (3000, 3.0)]
        stored = self._get_all_samples('ts1')
        assert stored == [(1000, 1.0), (2000, 2.0), (3000, 3.0)]

    def test_single_sample(self):
        """Sanitize a range with only one sample (NaN)."""
        self._create_series_with_data('ts1', [
            (1000, float('nan')),
        ])
        result = self.client.execute_command(
            'TS.SANITIZE', 'ts1', 1000, 1000,
            'POLICY', 'DROP'
        )
        samples = self._parse_sanitize_result(result)
        assert samples == []

    def test_single_sample_valid(self):
        """Sanitize a range with a single valid sample."""
        self._create_series_with_data('ts1', [
            (1000, 42.0),
        ])
        result = self.client.execute_command(
            'TS.SANITIZE', 'ts1', 1000, 1000,
            'POLICY', 'FILLMEAN'
        )
        samples = self._parse_sanitize_result(result)
        assert samples == [(1000, 42.0)]

    def test_mixed_nan_and_inf(self):
        """Sanitize a range with both NaN and ±Inf values."""
        self._create_series_with_data('ts1', [
            (1000, 1.0),
            (2000, float('nan')),
            (3000, float('inf')),
            (4000, 4.0),
            (5000, float('-inf')),
        ])
        result = self.client.execute_command(
            'TS.SANITIZE', 'ts1', 1000, 5000,
            'POLICY', 'DROP'
        )
        samples = self._parse_sanitize_result(result)
        assert samples == [(1000, 1.0), (4000, 4.0)]

    # ==================================================================
    # Invalid arguments
    # ==================================================================

    def test_invalid_policy(self):
        """Returns error for unknown policy name."""
        self._create_series_with_data('ts1', [
            (1000, 1.0),
        ])
        with pytest.raises(ResponseError, match='invalid argument'):
            self.client.execute_command(
                'TS.SANITIZE', 'ts1', 1000, 2000,
                'POLICY', 'INVALIDPOLICY'
            )

    def test_wrong_arity(self):
        """Returns error when not enough arguments."""
        self._create_series_with_data('ts1', [
            (1000, 1.0),
        ])
        with pytest.raises(ResponseError):
            self.client.execute_command(
                'TS.SANITIZE', 'ts1', 1000
            )

    def test_fill_missing_value(self):
        """FILL policy requires a fill value argument."""
        self._create_series_with_data('ts1', [
            (1000, 1.0),
            (2000, float('nan')),
        ])
        with pytest.raises(ResponseError):
            self.client.execute_command(
                'TS.SANITIZE', 'ts1', 1000, 2000,
                'POLICY', 'FILL'
            )

    def test_movingaverage_missing_window(self):
        """MOVINGAVERAGE requires a window argument."""
        self._create_series_with_data('ts1', [
            (1000, 1.0),
            (2000, float('nan')),
        ])
        with pytest.raises(ResponseError):
            self.client.execute_command(
                'TS.SANITIZE', 'ts1', 1000, 2000,
                'POLICY', 'MOVINGAVERAGE'
            )

    def test_seasonal_missing_period(self):
        """SEASONAL requires a period argument."""
        self._create_series_with_data('ts1', [
            (1000, 1.0),
            (2000, float('nan')),
        ])
        with pytest.raises(ResponseError):
            self.client.execute_command(
                'TS.SANITIZE', 'ts1', 1000, 2000,
                'POLICY', 'SEASONAL'
            )
