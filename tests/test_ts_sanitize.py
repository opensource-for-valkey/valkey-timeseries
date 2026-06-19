import math
import pytest
from valkey import ResponseError
from valkeytestframework.util.waiters import *
from valkeytestframework.conftest import resource_port_tracker
from valkey_timeseries_test_case import ValkeyTimeSeriesTestCaseBase


class TestTimeSeriesSanitize(ValkeyTimeSeriesTestCaseBase):
    """Integration tests for TS.SANITIZE command.

    TS.SANITIZE key fromTimestamp toTimestamp POLICY <policy> [options]

    Sanitizes missing (NaN/infinite) values in a time series within the
    given timestamp range. Returns the number of samples sanitized.
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

    # ------------------------------------------------------------------
    # ERROR policy
    # ------------------------------------------------------------------

    def test_error_no_missing(self):
        """ERROR policy succeeds when no missing values are present."""
        self._create_series_with_data('ts1', [
            (1000, 1.0),
            (2000, 2.0),
            (3000, 3.0),
        ])
        count = self.client.execute_command(
            'TS.SANITIZE', 'ts1', 1000, 3000, 'POLICY', 'ERROR'
        )
        assert count == 0
        # Data unchanged
        samples = self._get_all_samples('ts1')
        assert samples == [(1000, 1.0), (2000, 2.0), (3000, 3.0)]

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
        """ERROR policy returns error when Inf is present."""
        self._create_series_with_data('ts1', [
            (1000, 1.0),
            (2000, float('inf')),
            (3000, 3.0),
        ])
        with pytest.raises(ResponseError, match='missing values'):
            self.client.execute_command(
                'TS.SANITIZE', 'ts1', 1000, 3000, 'POLICY', 'ERROR'
            )

    # ------------------------------------------------------------------
    # DROP policy
    # ------------------------------------------------------------------

    def test_drop_removes_nan(self):
        """DROP policy removes samples with NaN values."""
        self._create_series_with_data('ts1', [
            (1000, 1.0),
            (2000, float('nan')),
            (3000, 3.0),
            (4000, float('nan')),
            (5000, 5.0),
        ])
        count = self.client.execute_command(
            'TS.SANITIZE', 'ts1', 1000, 5000, 'POLICY', 'DROP'
        )
        # Map returns the 3 valid samples that were kept
        assert count == 3
        samples = self._get_all_samples('ts1')
        assert samples == [(1000, 1.0), (3000, 3.0), (5000, 5.0)]

    def test_drop_all_valid(self):
        """DROP policy with all valid samples changes nothing."""
        self._create_series_with_data('ts1', [
            (1000, 1.0),
            (2000, 2.0),
            (3000, 3.0),
        ])
        count = self.client.execute_command(
            'TS.SANITIZE', 'ts1', 1000, 3000, 'POLICY', 'DROP'
        )
        assert count == 3
        samples = self._get_all_samples('ts1')
        assert len(samples) == 3

    # ------------------------------------------------------------------
    # FILL policy
    # ------------------------------------------------------------------

    def test_fill_constant(self):
        """FILL policy replaces NaN/Inf with a constant value."""
        self._create_series_with_data('ts1', [
            (1000, 1.0),
            (2000, float('nan')),
            (3000, 3.0),
            (4000, float('inf')),
            (5000, 5.0),
        ])
        count = self.client.execute_command(
            'TS.SANITIZE', 'ts1', 1000, 5000, 'POLICY', 'FILL', 0.0
        )
        assert count == 2
        samples = self._get_all_samples('ts1')
        assert samples == [
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
        count = self.client.execute_command(
            'TS.SANITIZE', 'ts1', 1000, 2000, 'POLICY', 'FILL', -999.0
        )
        assert count == 1
        samples = self._get_all_samples('ts1')
        assert samples == [(1000, 1.0), (2000, -999.0)]

    # ------------------------------------------------------------------
    # FORWARDFILL policy
    # ------------------------------------------------------------------

    def test_forwardfill_basic(self):
        """FORWARDFILL carries last valid observation forward."""
        self._create_series_with_data('ts1', [
            (1000, 1.0),
            (2000, float('nan')),
            (3000, 3.0),
            (4000, float('nan')),
            (5000, 5.0),
        ])
        count = self.client.execute_command(
            'TS.SANITIZE', 'ts1', 1000, 5000, 'POLICY', 'FORWARDFILL'
        )
        assert count == 2
        samples = self._get_all_samples('ts1')
        assert samples == [
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
        count = self.client.execute_command(
            'TS.SANITIZE', 'ts1', 1000, 3000, 'POLICY', 'FORWARDFILL'
        )
        # Leading NaN is in map but stays NaN
        assert count == 1
        # The leading NaN is "imputed" with NaN (no last valid)
        samples = self._get_all_samples('ts1')
        assert len(samples) == 3
        assert math.isnan(samples[0][1])
        assert samples[1][1] == 2.0
        assert samples[2][1] == 3.0

    # ------------------------------------------------------------------
    # BACKWARDFILL policy
    # ------------------------------------------------------------------

    def test_backwardfill_basic(self):
        """BACKWARDFILL carries next valid observation backward."""
        self._create_series_with_data('ts1', [
            (1000, 1.0),
            (2000, float('nan')),
            (3000, float('nan')),
            (4000, 4.0),
            (5000, 5.0),
        ])
        count = self.client.execute_command(
            'TS.SANITIZE', 'ts1', 1000, 5000, 'POLICY', 'BACKWARDFILL'
        )
        assert count == 2
        samples = self._get_all_samples('ts1')
        assert samples == [
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
        count = self.client.execute_command(
            'TS.SANITIZE', 'ts1', 1000, 3000, 'POLICY', 'BACKWARDFILL'
        )
        # Trailing NaN not imputed (no next_valid)
        assert count == 0

    # ------------------------------------------------------------------
    # FILLMEAN policy
    # ------------------------------------------------------------------

    def test_fillmean(self):
        """FILLMEAN replaces NaN with mean of valid values."""
        self._create_series_with_data('ts1', [
            (1000, 1.0),
            (2000, float('nan')),
            (3000, 3.0),
            (4000, float('nan')),
            (5000, 5.0),
        ])
        count = self.client.execute_command(
            'TS.SANITIZE', 'ts1', 1000, 5000, 'POLICY', 'FILLMEAN'
        )
        # Mean of [1, 3, 5] = 3.0
        assert count == 2
        samples = self._get_all_samples('ts1')
        assert samples == [
            (1000, 1.0),
            (2000, 3.0),
            (3000, 3.0),
            (4000, 3.0),
            (5000, 5.0),
        ]

    # ------------------------------------------------------------------
    # FILLMEDIAN policy
    # ------------------------------------------------------------------

    def test_fillmedian(self):
        """FILLMEDIAN replaces NaN with median of valid values."""
        self._create_series_with_data('ts1', [
            (1000, 1.0),
            (2000, float('nan')),
            (3000, 3.0),
            (4000, float('nan')),
            (5000, 10.0),
        ])
        count = self.client.execute_command(
            'TS.SANITIZE', 'ts1', 1000, 5000, 'POLICY', 'FILLMEDIAN'
        )
        # Median of [1, 3, 10] = 3.0
        assert count == 2
        samples = self._get_all_samples('ts1')
        assert samples == [
            (1000, 1.0),
            (2000, 3.0),
            (3000, 3.0),
            (4000, 3.0),
            (5000, 10.0),
        ]

    # ------------------------------------------------------------------
    # INTERPOLATE policy
    # ------------------------------------------------------------------

    def test_interpolate_fills_gaps(self):
        """INTERPOLATE linearly interpolates between valid neighbors."""
        self._create_series_with_data('ts1', [
            (1000, 1.0),
            (2000, float('nan')),
            (3000, float('nan')),
            (4000, 4.0),
            (5000, 5.0),
        ])
        count = self.client.execute_command(
            'TS.SANITIZE', 'ts1', 1000, 5000, 'POLICY', 'INTERPOLATE'
        )
        assert count == 2
        samples = self._get_all_samples('ts1')
        assert samples == [
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
        count = self.client.execute_command(
            'TS.SANITIZE', 'ts1', 1000, 5000, 'POLICY', 'INTERPOLATE'
        )
        assert count == 3
        samples = self._get_all_samples('ts1')
        assert samples == [
            (1000, 3.0),
            (2000, 3.0),
            (3000, 3.0),
            (4000, 4.0),
            (5000, 4.0),
        ]

    # ------------------------------------------------------------------
    # FORWARDBACKWARDFILL policy
    # ------------------------------------------------------------------

    def test_forwardbackwardfill_handles_leading(self):
        """FORWARDBACKWARDFILL handles leading NaN."""
        self._create_series_with_data('ts1', [
            (1000, float('nan')),
            (2000, float('nan')),
            (3000, 3.0),
            (4000, float('nan')),
            (5000, 5.0),
        ])
        count = self.client.execute_command(
            'TS.SANITIZE', 'ts1', 1000, 5000, 'POLICY', 'FORWARDBACKWARDFILL'
        )
        # Leading NaN filled backward from 3.0, interior NaN filled forward
        assert count >= 3
        samples = self._get_all_samples('ts1')
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
        count = self.client.execute_command(
            'TS.SANITIZE', 'ts1', 1000, 4000, 'POLICY', 'FORWARDBACKWARDFILL'
        )
        assert count >= 2
        samples = self._get_all_samples('ts1')
        assert samples[0][1] == 1.0
        assert samples[1][1] == 2.0
        assert samples[2][1] == 2.0  # Forward-filled
        assert samples[3][1] == 2.0  # Forward-filled

    # ------------------------------------------------------------------
    # MOVINGAVERAGE policy
    # ------------------------------------------------------------------

    def test_movingaverage_single_gap(self):
        """MOVINGAVERAGE with window=3 fills a single gap."""
        self._create_series_with_data('ts1', [
            (1000, 1.0),
            (2000, 2.0),
            (3000, float('nan')),
            (4000, 4.0),
            (5000, 5.0),
        ])
        count = self.client.execute_command(
            'TS.SANITIZE', 'ts1', 1000, 5000,
            'POLICY', 'MOVINGAVERAGE', 3
        )
        # map returns all 5 imputed samples
        assert count == 5
        samples = self._get_all_samples('ts1')
        # NaN at index 2 should be ~3.0 (mean of 2.0 and 4.0)
        assert abs(samples[2][1] - 3.0) < 1e-10

    def test_movingaverage_rejects_even_window(self):
        """MOVINGAVERAGE rejects even window size."""
        self._create_series_with_data('ts1', [
            (1000, 1.0),
            (2000, 2.0),
            (3000, 3.0),
        ])
        with pytest.raises(ResponseError, match='invalid parameter'):
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
        with pytest.raises(ResponseError, match='invalid parameter'):
            self.client.execute_command(
                'TS.SANITIZE', 'ts1', 1000, 3000,
                'POLICY', 'MOVINGAVERAGE', 0
            )

    # ------------------------------------------------------------------
    # SEASONAL policy
    # ------------------------------------------------------------------

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
        count = self.client.execute_command(
            'TS.SANITIZE', 'ts1', 1000, 9000,
            'POLICY', 'SEASONAL', 3
        )
        assert count == 9
        samples = self._get_all_samples('ts1')
        assert abs(samples[6][1] - 10.5) < 1e-10

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
        with pytest.raises(ResponseError, match='invalid parameter'):
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

    # ------------------------------------------------------------------
    # Edge cases
    # ------------------------------------------------------------------

    def test_key_not_found(self):
        """Returns error when key does not exist."""
        with pytest.raises(ResponseError, match='key does not exist'):
            self.client.execute_command(
                'TS.SANITIZE', 'nonexistent', 1000, 2000,
                'POLICY', 'ERROR'
            )

    def test_empty_range(self):
        """Returns 0 when no samples exist in the range."""
        self._create_series_with_data('ts1', [
            (1000, 1.0),
            (2000, 2.0),
        ])
        count = self.client.execute_command(
            'TS.SANITIZE', 'ts1', 5000, 6000,
            'POLICY', 'ERROR'
        )
        assert count == 0

    def test_range_partial(self):
        """Only samples within the specified range are sanitized."""
        self._create_series_with_data('ts1', [
            (1000, float('nan')),
            (2000, 2.0),
            (3000, float('nan')),
            (4000, 4.0),
        ])
        # Only sanitize the middle range [2000, 3000]
        count = self.client.execute_command(
            'TS.SANITIZE', 'ts1', 2000, 3000,
            'POLICY', 'INTERPOLATE'
        )
        assert count == 1
        samples = self._get_all_samples('ts1')
        # Sample at 1000 (outside range) should still be NaN
        assert math.isnan(samples[0][1])
        # Sample at 2000 unchanged (was already valid)
        assert samples[1][1] == 2.0
        # Sample at 3000 now interpolated from 2.0 to 4.0 → 3.0
        assert abs(samples[2][1] - 3.0) < 1e-10
        # Sample at 4000 unchanged
        assert samples[3][1] == 4.0

    def test_no_missing_values(self):
        """Returns 0 when all values are valid (non-NaN, non-Inf)."""
        self._create_series_with_data('ts1', [
            (1000, 1.0),
            (2000, 2.0),
            (3000, 3.0),
        ])
        count = self.client.execute_command(
            'TS.SANITIZE', 'ts1', 1000, 3000,
            'POLICY', 'FILL', 0.0
        )
        assert count == 0
        samples = self._get_all_samples('ts1')
        assert samples == [(1000, 1.0), (2000, 2.0), (3000, 3.0)]

    # ------------------------------------------------------------------
    # Invalid arguments
    # ------------------------------------------------------------------

    def test_missing_policy(self):
        """Returns error when POLICY keyword is missing."""
        self._create_series_with_data('ts1', [
            (1000, 1.0),
        ])
        with pytest.raises(ResponseError):
            self.client.execute_command(
                'TS.SANITIZE', 'ts1', 1000, 2000, 'ERROR'
            )

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
