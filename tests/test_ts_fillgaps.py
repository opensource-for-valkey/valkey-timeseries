import math
import pytest
from valkey import ResponseError
from valkeytestframework.util.waiters import *
from valkeytestframework.conftest import resource_port_tracker
from valkey_timeseries_test_case import ValkeyTimeSeriesTestCaseBase


class TestTimeSeriesFillgaps(ValkeyTimeSeriesTestCaseBase):
    """Integration tests for TS.FILLGAPS command.

    TS.FILLGAPS key startTimestamp endTimestamp
      [VALUE value]
      [FREQUENCY duration]
      [ALIGN alignTimestamp|-]

    Fills missing timestamps between startTimestamp and endTimestamp with
    a fill value (default NaN). Returns the number of gaps filled.
    """

    # ------------------------------------------------------------------
    # Helper methods
    # ------------------------------------------------------------------

    def _create_series_with_uniform_data(self, key, start_ts, step, count, value_base=0):
        """Create a series and add uniformly spaced samples."""
        self.client.execute_command('TS.CREATE', key)
        for i in range(count):
            ts = start_ts + i * step
            val = value_base + i
            self.client.execute_command('TS.ADD', key, ts, val)
        return [(start_ts + i * step, value_base + i) for i in range(count)]

    def _get_timestamps(self, key, start='-', end='+'):
        """Return the list of timestamps from TS.RANGE."""
        result = self.client.execute_command('TS.RANGE', key, start, end)
        return [entry[0] for entry in result]

    def _get_values(self, key, start='-', end='+'):
        """Return the list of (timestamp, float_value) from TS.RANGE."""
        result = self.client.execute_command('TS.RANGE', key, start, end)
        return [(entry[0], float(entry[1])) for entry in result]

    # ------------------------------------------------------------------
    # Basic / happy-path tests
    # ------------------------------------------------------------------

    def test_fillgaps_basic_no_gaps(self):
        """Fill gaps in a regularly spaced series where no gaps exist."""
        self._create_series_with_uniform_data('ts1', 1000, 100, 5)
        # Series has timestamps: 1000, 1100, 1200, 1300, 1400
        # No gaps in range 1000-1400 with frequency 100ms

        gaps_filled = self.client.execute_command(
            'TS.FILLGAPS', 'ts1', 1000, 1400, 'FREQUENCY', 100
        )
        assert gaps_filled == 0

        # Verify no new samples were added
        info = self.ts_info('ts1')
        assert info['totalSamples'] == 5

    def test_fillgaps_basic_with_gaps(self):
        """Fill gaps in a series with missing timestamps."""
        self.client.execute_command('TS.CREATE', 'ts2')
        # Add samples at 1000, 3000, 5000 (missing 2000 and 4000)
        self.client.execute_command('TS.ADD', 'ts2', 1000, 10)
        self.client.execute_command('TS.ADD', 'ts2', 3000, 30)
        self.client.execute_command('TS.ADD', 'ts2', 5000, 50)

        gaps_filled = self.client.execute_command(
            'TS.FILLGAPS', 'ts2', 1000, 5000, 'FREQUENCY', 1000
        )
        assert gaps_filled == 2  # timestamps 2000 and 4000

        # Verify all 5 samples exist
        result = self._get_values('ts2')
        assert len(result) == 5
        assert result[0] == (1000, 10.0)
        assert result[2] == (3000, 30.0)
        assert result[4] == (5000, 50.0)

    def test_fillgaps_default_nan_value(self):
        """Fill gaps with default NaN fill value."""
        self.client.execute_command('TS.CREATE', 'ts3')
        self.client.execute_command('TS.ADD', 'ts3', 1000, 10)
        self.client.execute_command('TS.ADD', 'ts3', 3000, 30)

        gaps_filled = self.client.execute_command(
            'TS.FILLGAPS', 'ts3', 1000, 3000, 'FREQUENCY', 1000
        )
        assert gaps_filled == 1  # timestamp 2000 filled with NaN

        result = self.client.execute_command('TS.RANGE', 'ts3', '-', '+')
        assert len(result) == 3
        # Check the gap value is NaN
        assert math.isnan(float(result[1][1])), f"Expected NaN, got {result[1][1]}"

    def test_fillgaps_custom_value(self):
        """Fill gaps with a custom VALUE."""
        self.client.execute_command('TS.CREATE', 'ts4')
        self.client.execute_command('TS.ADD', 'ts4', 1000, 10)
        self.client.execute_command('TS.ADD', 'ts4', 3000, 30)

        gaps_filled = self.client.execute_command(
            'TS.FILLGAPS', 'ts4', 1000, 3000,
            'FREQUENCY', 1000, 'VALUE', 0.0
        )
        assert gaps_filled == 1

        result = self._get_values('ts4')
        assert len(result) == 3
        assert result[1] == (2000, 0.0)  # filled with custom value

    def test_fillgaps_custom_negative_value(self):
        """Fill gaps with a negative VALUE."""
        self.client.execute_command('TS.CREATE', 'ts_neg')
        self.client.execute_command('TS.ADD', 'ts_neg', 1000, 10)
        self.client.execute_command('TS.ADD', 'ts_neg', 3000, 30)

        gaps_filled = self.client.execute_command(
            'TS.FILLGAPS', 'ts_neg', 1000, 3000,
            'FREQUENCY', 1000, 'VALUE', -1.5
        )
        assert gaps_filled == 1

        result = self._get_values('ts_neg')
        assert result[1] == (2000, -1.5)

    # ------------------------------------------------------------------
    # Frequency inference tests
    # ------------------------------------------------------------------

    def test_fillgaps_inferred_frequency(self):
        """Fill gaps using inferred frequency from uniformly spaced data.

        After deletion, the remaining intervals are [1000, 1000, 500].
        The GCD-based inference recovers the original 500ms frequency
        (GCD=500, which divides the modal 1000ms and appears in the intervals).
        """
        self._create_series_with_uniform_data('ts_infer', 1000, 500, 6)
        # Series: 1000, 1500, 2000, 2500, 3000, 3500
        # Delete some samples to create gaps
        self.client.execute_command('TS.DEL', 'ts_infer', 1500, 1500)
        self.client.execute_command('TS.DEL', 'ts_infer', 2500, 2500)

        # Now we have: 1000, 2000, 3000, 3500
        # GCD of intervals [1000, 1000, 500] is 500ms
        gaps_filled = self.client.execute_command(
            'TS.FILLGAPS', 'ts_infer', 1000, 3500
        )
        assert gaps_filled == 2  # 1500 and 2500

        timestamps = self._get_timestamps('ts_infer')
        assert timestamps == [1000, 1500, 2000, 2500, 3000, 3500]

    def test_fillgaps_inferred_frequency_large_step(self):
        """GCD-based inference recovers the original frequency after deletions.

        Remaining intervals [20000, 20000, 10000] have GCD=10000ms which is
        smaller than the modal 20000ms and appears as an actual interval,
        so the algorithm correctly infers 10000ms.
        """
        self._create_series_with_uniform_data('ts_gcd', 0, 10000, 6)
        # Series: 0, 10000, 20000, 30000, 40000, 50000
        # Delete every other sample to create gaps
        self.client.execute_command('TS.DEL', 'ts_gcd', 10000, 10000)
        self.client.execute_command('TS.DEL', 'ts_gcd', 30000, 30000)
        # Remaining: 0, 20000, 40000, 50000
        # Intervals: [20000, 20000, 10000] → GCD=10000ms

        gaps_filled = self.client.execute_command(
            'TS.FILLGAPS', 'ts_gcd', 0, 50000
        )
        # Grid with 10000ms: 0, 10000, 20000, 30000, 40000, 50000
        # Gaps at 10000 and 30000
        assert gaps_filled == 2

        timestamps = self._get_timestamps('ts_gcd')
        assert timestamps == [0, 10000, 20000, 30000, 40000, 50000]

    # ------------------------------------------------------------------
    # ALIGN tests
    # ------------------------------------------------------------------

    def test_fillgaps_align_false_default(self):
        """Without ALIGN, gaps are filled starting from startTimestamp."""
        self.client.execute_command('TS.CREATE', 'ts_align')
        self.client.execute_command('TS.ADD', 'ts_align', 1100, 11)
        self.client.execute_command('TS.ADD', 'ts_align', 2100, 21)

        # start=1100, frequency=1000
        # Expected timestamps: 1100, 2100 (already exist) -> no gaps
        gaps_filled = self.client.execute_command(
            'TS.FILLGAPS', 'ts_align', 1100, 2100, 'FREQUENCY', 1000
        )
        assert gaps_filled == 0

    def test_fillgaps_align_true(self):
        """With ALIGN 0 (epoch), timestamps snap to the epoch-aligned grid.

        Gaps are only filled within [startTimestamp, endTimestamp].
        """
        self.client.execute_command('TS.CREATE', 'ts_align2')
        self.client.execute_command('TS.ADD', 'ts_align2', 1100, 11)
        self.client.execute_command('TS.ADD', 'ts_align2', 2100, 21)

        # start=1100, frequency=1000, ALIGN=0 (epoch)
        # Aligned grid: 1000, 2000, 3000, ...
        # In [1100, 2100]: only 2000 is within range (1000 < 1100)
        # Existing: 1100, 2100 (neither on grid) -> gap at 2000
        gaps_filled = self.client.execute_command(
            'TS.FILLGAPS', 'ts_align2', 1100, 2100,
            'FREQUENCY', 1000, 'ALIGN', 0
        )
        assert gaps_filled == 1  # 2000 only; 1000 is before start_ts

        timestamps = self._get_timestamps('ts_align2')
        assert 1100 in timestamps
        assert 2000 in timestamps
        assert 2100 in timestamps

    def test_fillgaps_align_already_on_grid(self):
        """ALIGN when startTimestamp is already on the frequency grid relative to align timestamp."""
        self._create_series_with_uniform_data('ts_grid', 1000, 1000, 3)
        # Series: 1000, 2000, 3000
        # Remove 2000 to create a gap
        self.client.execute_command('TS.DEL', 'ts_grid', 2000, 2000)

        gaps_filled = self.client.execute_command(
            'TS.FILLGAPS', 'ts_grid', 1000, 3000,
            'FREQUENCY', 1000, 'ALIGN', 0
        )
        assert gaps_filled == 1  # 2000 is the gap

        timestamps = self._get_timestamps('ts_grid')
        assert timestamps == [1000, 2000, 3000]

    def test_fillgaps_align_with_frequency(self):
        """ALIGN combined with explicit frequency and align timestamp.

        Gaps are only filled within [startTimestamp, endTimestamp]; grid points
        before start_ts are skipped.
        """
        self._create_series_with_uniform_data('ts_align_freq', 1100, 1000, 4)
        # Series: 1100, 2100, 3100, 4100
        # Remove one to create a gap
        self.client.execute_command('TS.DEL', 'ts_align_freq', 2100, 2100)
        # Existing timestamps: 1100, 3100, 4100

        # Explicit frequency 1000ms, ALIGN=0 (epoch)
        # Aligned grid: 1000, 2000, 3000, 4000, 5000, ...
        # In [1100, 4100]: 2000, 3000, 4000 are within range (1000 < 1100)
        # None match existing timestamps → 3 gaps
        gaps_filled = self.client.execute_command(
            'TS.FILLGAPS', 'ts_align_freq', 1100, 4100,
            'FREQUENCY', 1000, 'ALIGN', 0
        )
        assert gaps_filled == 3

        timestamps = self._get_timestamps('ts_align_freq')
        assert 2000 in timestamps
        assert 3000 in timestamps
        assert 4000 in timestamps

    def test_fillgaps_align_with_non_zero_reference(self):
        """ALIGN with a non-zero align timestamp shifts the grid accordingly."""
        self.client.execute_command('TS.CREATE', 'ts_align_nonzero')
        self.client.execute_command('TS.ADD', 'ts_align_nonzero', 1500, 15)
        self.client.execute_command('TS.ADD', 'ts_align_nonzero', 2500, 25)

        # frequency=1000, align timestamp=500
        # Grid aligned to 500: 500, 1500, 2500, 3500, ...
        # Existing: 1500, 2500 — both already on the 500-aligned grid
        # start=1500, end=2500 — no gaps on this grid
        gaps_filled = self.client.execute_command(
            'TS.FILLGAPS', 'ts_align_nonzero', 1500, 2500,
            'FREQUENCY', 1000, 'ALIGN', 500
        )
        assert gaps_filled == 0

    def test_fillgaps_align_non_zero_creates_different_gaps(self):
        """ALIGN with a non-zero reference produces a different gap set than epoch alignment."""
        self.client.execute_command('TS.CREATE', 'ts_align_diff')
        # Samples at 1000 and 3000 (both on epoch grid: 0, 1000, 2000, 3000)
        self.client.execute_command('TS.ADD', 'ts_align_diff', 1000, 10)
        self.client.execute_command('TS.ADD', 'ts_align_diff', 3000, 30)

        # With align timestamp=500, grid is: 500, 1500, 2500, 3500
        # Existing 1000 and 3000 are NOT on this grid
        # So all grid points in [1000, 3000] are gaps: 1500, 2500
        gaps_filled = self.client.execute_command(
            'TS.FILLGAPS', 'ts_align_diff', 1000, 3000,
            'FREQUENCY', 1000, 'ALIGN', 500
        )
        assert gaps_filled == 2  # 1500 and 2500

        timestamps = self._get_timestamps('ts_align_diff')
        # Original samples preserved: 1000, 3000
        # Filled gaps: 1500, 2500
        assert 1000 in timestamps
        assert 1500 in timestamps
        assert 2500 in timestamps
        assert 3000 in timestamps

    def test_fillgaps_align_reference_before_range(self):
        """ALIGN with reference timestamp before the data range."""
        self.client.execute_command('TS.CREATE', 'ts_align_before')
        self.client.execute_command('TS.ADD', 'ts_align_before', 5000, 50)
        self.client.execute_command('TS.ADD', 'ts_align_before', 7000, 70)

        # frequency=1000, align timestamp=100 (well before the range)
        # Grid: 100, 1100, 2100, 3100, 4100, 5100, 6100, 7100, ...
        # In [5000, 7000]: grid points are 5100, 6100, 7100
        # Existing: 5000, 7000 — neither on grid except 7100 ≈ 7000? No.
        # 5000 is not on grid (5000-100=4900, 4900%1000=900, not 0)
        # 7000 is not on grid (7000-100=6900, 6900%1000=900, not 0)
        # So gaps: 5100, 6100 — but 7100 > 7000 so excluded
        gaps_filled = self.client.execute_command(
            'TS.FILLGAPS', 'ts_align_before', 5000, 7000,
            'FREQUENCY', 1000, 'ALIGN', 100
        )
        # aligned_start = calc_range_start(5000, 100, 1000)
        # diff = 4900, result = 5000 - ((4900 % 1000 + 1000) % 1000) = 5000 - 900 = 4100
        # Grid from 4100: 4100, 5100, 6100, 7100
        # In [5000, 7000]: 5100, 6100 (7100 > 7000)
        # Existing: 5000, 7000 — neither on grid
        assert gaps_filled == 2

    def test_fillgaps_align_reference_after_range(self):
        """ALIGN with reference timestamp after the data range."""
        self.client.execute_command('TS.CREATE', 'ts_align_after')
        self.client.execute_command('TS.ADD', 'ts_align_after', 1000, 10)
        self.client.execute_command('TS.ADD', 'ts_align_after', 3000, 30)

        # frequency=1000, align timestamp=9500 (after the range)
        # Grid aligned to 9500: ..., 500, 1500, 2500, 3500, ..., 9500
        # In [1000, 3000]: grid points are 1500, 2500
        # Existing: 1000, 3000 — neither on grid (1000-9500=-8500, -8500%1000=500≠0)
        # Wait: calc_range_start(1000, 9500, 1000)
        # diff = 1000 - 9500 = -8500
        # result = 1000 - ((-8500 % 1000 + 1000) % 1000) = 1000 - (500 % 1000) = 1000 - 500 = 500
        # Grid from 500: 500, 1500, 2500, 3500
        # In [1000, 3000]: 1500, 2500
        gaps_filled = self.client.execute_command(
            'TS.FILLGAPS', 'ts_align_after', 1000, 3000,
            'FREQUENCY', 1000, 'ALIGN', 9500
        )
        assert gaps_filled == 2

    def test_fillgaps_align_reference_matches_existing_timestamp(self):
        """ALIGN with reference timestamp that matches an existing sample."""
        self._create_series_with_uniform_data('ts_align_match', 0, 100, 5)
        # Series: 0, 100, 200, 300, 400
        self.client.execute_command('TS.DEL', 'ts_align_match', 200, 200)
        # Existing: 0, 100, 300, 400

        # frequency=100, align timestamp=100
        # Grid aligned to 100: 0, 100, 200, 300, 400, ...
        # In [0, 400]: grid points are 0, 100, 200, 300, 400
        # Existing: 0, 100, 300, 400 — gap at 200
        gaps_filled = self.client.execute_command(
            'TS.FILLGAPS', 'ts_align_match', 0, 400,
            'FREQUENCY', 100, 'ALIGN', 100
        )
        assert gaps_filled == 1

        timestamps = self._get_timestamps('ts_align_match')
        assert timestamps == [0, 100, 200, 300, 400]

    # ------------------------------------------------------------------
    # Range boundary and partial overlap tests
    # ------------------------------------------------------------------

    def test_fillgaps_range_before_data(self):
        """Fill gaps in a range entirely before existing data."""
        self._create_series_with_uniform_data('ts_before', 5000, 1000, 3)
        # Series: 5000, 6000, 7000

        gaps_filled = self.client.execute_command(
            'TS.FILLGAPS', 'ts_before', 1000, 4000, 'FREQUENCY', 1000
        )
        assert gaps_filled == 4  # 1000, 2000, 3000, 4000

        result = self._get_values('ts_before')
        assert len(result) == 7  # 4 filled + 3 existing

    def test_fillgaps_range_after_data(self):
        """Fill gaps in a range entirely after existing data."""
        self._create_series_with_uniform_data('ts_after', 1000, 1000, 3)
        # Series: 1000, 2000, 3000

        gaps_filled = self.client.execute_command(
            'TS.FILLGAPS', 'ts_after', 4000, 7000, 'FREQUENCY', 1000
        )
        assert gaps_filled == 4  # 4000, 5000, 6000, 7000

        result = self._get_values('ts_after')
        assert len(result) == 7  # 3 existing + 4 filled

    def test_fillgaps_range_sparse_data(self):
        """Fill gaps where existing data is sparse relative to the range."""
        self.client.execute_command('TS.CREATE', 'ts_sparse')
        self.client.execute_command('TS.ADD', 'ts_sparse', 1000, 10)
        self.client.execute_command('TS.ADD', 'ts_sparse', 10000, 100)

        gaps_filled = self.client.execute_command(
            'TS.FILLGAPS', 'ts_sparse', 1000, 10000, 'FREQUENCY', 1000
        )
        assert gaps_filled == 8  # 2000..9000 and already have 1000 & 10000

        result = self._get_values('ts_sparse')
        assert len(result) == 10
        # original values should be at their positions
        assert result[0] == (1000, 10.0)
        assert result[9] == (10000, 100.0)

    def test_fillgaps_exact_boundaries(self):
        """Fill gaps with start and end exactly matching existing timestamps."""
        self.client.execute_command('TS.CREATE', 'ts_exact')
        self.client.execute_command('TS.ADD', 'ts_exact', 1000, 10)
        self.client.execute_command('TS.ADD', 'ts_exact', 5000, 50)

        gaps_filled = self.client.execute_command(
            'TS.FILLGAPS', 'ts_exact', 1000, 5000, 'FREQUENCY', 1000
        )
        assert gaps_filled == 3  # 2000, 3000, 4000; 1000&5000 already exist

        timestamps = self._get_timestamps('ts_exact')
        assert timestamps == [1000, 2000, 3000, 4000, 5000]

    # ------------------------------------------------------------------
    # Existing samples are preserved
    # ------------------------------------------------------------------

    def test_fillgaps_does_not_overwrite_existing(self):
        """Verify that existing sample values are never changed."""
        self.client.execute_command('TS.CREATE', 'ts_preserve')
        self.client.execute_command('TS.ADD', 'ts_preserve', 1000, 42.0)
        self.client.execute_command('TS.ADD', 'ts_preserve', 2000, 99.0)
        self.client.execute_command('TS.ADD', 'ts_preserve', 3000, 17.0)

        gaps_filled = self.client.execute_command(
            'TS.FILLGAPS', 'ts_preserve', 1000, 3000,
            'FREQUENCY', 1000, 'VALUE', 0.0
        )
        assert gaps_filled == 0  # No gaps

        result = self._get_values('ts_preserve')
        assert result[0] == (1000, 42.0)
        assert result[1] == (2000, 99.0)
        assert result[2] == (3000, 17.0)

    def test_fillgaps_only_fills_gaps_not_overwrites(self):
        """Fill gaps and confirm existing values remain untouched."""
        self.client.execute_command('TS.CREATE', 'ts_mixed')
        self.client.execute_command('TS.ADD', 'ts_mixed', 1000, 10)
        self.client.execute_command('TS.ADD', 'ts_mixed', 3000, 30)
        self.client.execute_command('TS.ADD', 'ts_mixed', 5000, 50)

        gaps_filled = self.client.execute_command(
            'TS.FILLGAPS', 'ts_mixed', 1000, 5000,
            'FREQUENCY', 1000, 'VALUE', -999.0
        )
        assert gaps_filled == 2  # 2000, 4000

        result = self._get_values('ts_mixed')
        assert len(result) == 5
        assert result[0] == (1000, 10.0)    # original
        assert result[1] == (2000, -999.0)  # filled
        assert result[2] == (3000, 30.0)    # original
        assert result[3] == (4000, -999.0)  # filled
        assert result[4] == (5000, 50.0)    # original

    # ------------------------------------------------------------------
    # totalSamples / INFO verification
    # ------------------------------------------------------------------

    def test_fillgaps_updates_total_samples(self):
        """Verify totalSamples increases by the number of gaps filled."""
        self._create_series_with_uniform_data('ts_info', 1000, 1000, 3)
        # 3 samples

        info_before = self.ts_info('ts_info')
        assert info_before['totalSamples'] == 3

        self.client.execute_command('TS.DEL', 'ts_info', 2000, 2000)
        # Now 2 samples

        gaps_filled = self.client.execute_command(
            'TS.FILLGAPS', 'ts_info', 1000, 3000, 'FREQUENCY', 1000
        )
        assert gaps_filled == 1

        info_after = self.ts_info('ts_info')
        assert info_after['totalSamples'] == 3

    # ------------------------------------------------------------------
    # Large dataset tests
    # ------------------------------------------------------------------

    def test_fillgaps_large_gap_count(self):
        """Fill a large number of gaps."""
        self.client.execute_command('TS.CREATE', 'ts_large_gaps')
        self.client.execute_command('TS.ADD', 'ts_large_gaps', 0, 0)
        self.client.execute_command('TS.ADD', 'ts_large_gaps', 100000, 100)

        gaps_filled = self.client.execute_command(
            'TS.FILLGAPS', 'ts_large_gaps', 0, 100000, 'FREQUENCY', 1000
        )
        assert gaps_filled == 99  # 1000, 2000, ..., 99000 (99 gaps, 0 and 100000 already exist)

        info = self.ts_info('ts_large_gaps')
        assert info['totalSamples'] == 101  # 2 original + 99 filled

    def test_fillgaps_with_many_existing_samples(self):
        """Fill gaps in a series that already has many regular samples."""
        self._create_series_with_uniform_data('ts_dense', 0, 10, 100)
        # Delete every other sample
        for i in range(1, 100, 2):
            ts = i * 10
            self.client.execute_command('TS.DEL', 'ts_dense', ts, ts)

        # Now 50 samples remain, 50 gaps exist
        gaps_filled = self.client.execute_command(
            'TS.FILLGAPS', 'ts_dense', 0, 990, 'FREQUENCY', 10
        )
        assert gaps_filled == 50

        info = self.ts_info('ts_dense')
        assert info['totalSamples'] == 100

    # ------------------------------------------------------------------
    # Error cases
    # ------------------------------------------------------------------

    def test_fillgaps_nonexistent_key(self):
        """Error when the key does not exist."""
        with pytest.raises(ResponseError) as excinfo:
            self.client.execute_command(
                'TS.FILLGAPS', 'nonexistent', 1000, 5000, 'FREQUENCY', 1000
            )
        assert "key does not exist" in str(excinfo.value).lower()

    def test_fillgaps_wrong_arity_no_args(self):
        """Error when called with no arguments."""
        self.verify_error_response(
            self.client,
            'TS.FILLGAPS',
            "wrong number of arguments for 'TS.FILLGAPS' command"
        )

    def test_fillgaps_wrong_arity_missing_end(self):
        """Error when missing the end timestamp."""
        self.client.execute_command('TS.CREATE', 'ts_arity')
        self.verify_error_response(
            self.client,
            'TS.FILLGAPS ts_arity 1000',
            "wrong number of arguments for 'TS.FILLGAPS' command"
        )

    def test_fillgaps_zero_frequency(self):
        """Error when FREQUENCY is zero."""
        self.client.execute_command('TS.CREATE', 'ts_zero_freq')
        self.client.execute_command('TS.ADD', 'ts_zero_freq', 1000, 10)
        self.client.execute_command('TS.ADD', 'ts_zero_freq', 2000, 20)

        with pytest.raises(ResponseError) as excinfo:
            self.client.execute_command(
                'TS.FILLGAPS', 'ts_zero_freq', 1000, 2000, 'FREQUENCY', 0
            )
        assert "frequency must be positive" in str(excinfo.value).lower()

    def test_fillgaps_negative_frequency(self):
        """Error when FREQUENCY is negative."""
        self.client.execute_command('TS.CREATE', 'ts_neg_freq')
        self.client.execute_command('TS.ADD', 'ts_neg_freq', 1000, 10)
        self.client.execute_command('TS.ADD', 'ts_neg_freq', 2000, 20)

        with pytest.raises(ResponseError) as excinfo:
            self.client.execute_command(
                'TS.FILLGAPS', 'ts_neg_freq', 1000, 2000, 'FREQUENCY', -1000
            )
        assert "TSDB:" in str(excinfo.value)

    def test_fillgaps_insufficient_data_for_inference_empty_series(self):
        """Error when series exists but has no samples for frequency inference."""
        self.client.execute_command('TS.CREATE', 'ts_empty_infer')

        with pytest.raises(ResponseError) as excinfo:
            self.client.execute_command(
                'TS.FILLGAPS', 'ts_empty_infer', 1000, 5000
            )
        assert "insufficient data to infer frequency" in str(excinfo.value).lower()

    def test_fillgaps_insufficient_data_for_inference_single_sample(self):
        """Error when series has only 1 sample (need at least 2 for inference)."""
        self.client.execute_command('TS.CREATE', 'ts_single')
        self.client.execute_command('TS.ADD', 'ts_single', 1000, 10)

        with pytest.raises(ResponseError) as excinfo:
            self.client.execute_command(
                'TS.FILLGAPS', 'ts_single', 1000, 5000
            )
        assert "insufficient data to infer frequency" in str(excinfo.value).lower()

    def test_fillgaps_no_dominant_interval(self):
        """Error when intervals are too irregular to infer a dominant frequency."""
        self.client.execute_command('TS.CREATE', 'ts_irregular')
        self.client.execute_command('TS.ADD', 'ts_irregular', 1000, 10)
        self.client.execute_command('TS.ADD', 'ts_irregular', 1500, 15)   # diff=500
        self.client.execute_command('TS.ADD', 'ts_irregular', 1700, 17)   # diff=200
        self.client.execute_command('TS.ADD', 'ts_irregular', 3000, 30)   # diff=1300

        # Intervals: 500, 200, 1300 — each is unique (33% each, <50% threshold)
        with pytest.raises(ResponseError) as excinfo:
            self.client.execute_command(
                'TS.FILLGAPS', 'ts_irregular', 1000, 3000
            )
        assert "no dominant interval found" in str(excinfo.value).lower()

    def test_fillgaps_invalid_argument(self):
        """Error when an unrecognized argument is passed."""
        self.client.execute_command('TS.CREATE', 'ts_invalid')
        self.client.execute_command('TS.ADD', 'ts_invalid', 1000, 10)
        self.client.execute_command('TS.ADD', 'ts_invalid', 2000, 20)

        with pytest.raises(ResponseError) as excinfo:
            self.client.execute_command(
                'TS.FILLGAPS', 'ts_invalid', 1000, 2000,
                'FREQUENCY', 1000, 'INVALID_ARG', 'value'
            )
        assert "invalid argument" in str(excinfo.value).lower()

    def test_fillgaps_align_missing_timestamp(self):
        """Error when ALIGN is specified but no align timestamp follows."""
        self.client.execute_command('TS.CREATE', 'ts_align_missing_ts')
        self.client.execute_command('TS.ADD', 'ts_align_missing_ts', 1000, 10)
        self.client.execute_command('TS.ADD', 'ts_align_missing_ts', 2000, 20)

        with pytest.raises(ResponseError) as excinfo:
            self.client.execute_command(
                'TS.FILLGAPS', 'ts_align_missing_ts', 1000, 2000,
                'FREQUENCY', 1000, 'ALIGN', 
            )
        assert "wrong number of arguments" in str(excinfo.value).lower()

    def test_fillgaps_align_invalid_timestamp(self):
        """Error when ALIGN is given an invalid timestamp value."""
        self.client.execute_command('TS.CREATE', 'ts_align_bad_ts')
        self.client.execute_command('TS.ADD', 'ts_align_bad_ts', 1000, 10)
        self.client.execute_command('TS.ADD', 'ts_align_bad_ts', 2000, 20)

        with pytest.raises(ResponseError) as excinfo:
            self.client.execute_command(
                'TS.FILLGAPS', 'ts_align_bad_ts', 1000, 2000,
                'FREQUENCY', 1000, 'ALIGN', 'not_a_number'
            )
        assert "timestamp" in str(excinfo.value).lower()

    # ------------------------------------------------------------------
    # Edge case tests
    # ------------------------------------------------------------------

    def test_fillgaps_single_timestamp_range(self):
        """Fill gaps with start == end."""
        self.client.execute_command('TS.CREATE', 'ts_single_ts')
        self.client.execute_command('TS.ADD', 'ts_single_ts', 1000, 10)

        # start == end == 1000, sample exists -> no gaps
        gaps_filled = self.client.execute_command(
            'TS.FILLGAPS', 'ts_single_ts', 1000, 1000, 'FREQUENCY', 1000
        )
        assert gaps_filled == 0

        # start == end == 2000, no sample there -> 1 gap
        gaps_filled = self.client.execute_command(
            'TS.FILLGAPS', 'ts_single_ts', 2000, 2000, 'FREQUENCY', 1000
        )
        assert gaps_filled == 1

        result = self._get_values('ts_single_ts')
        # The filled gap at 2000 should be NaN (default)
        nan_filled = any(t == 2000 and math.isnan(v) for t, v in result)
        assert nan_filled, f"Expected NaN at timestamp 2000, got: {result}"

    def test_fillgaps_with_nan_as_custom_value(self):
        """Explicitly set VALUE to NaN (same as default but explicit)."""
        self.client.execute_command('TS.CREATE', 'ts_nan_val')
        self.client.execute_command('TS.ADD', 'ts_nan_val', 1000, 10)
        self.client.execute_command('TS.ADD', 'ts_nan_val', 3000, 30)

        gaps_filled = self.client.execute_command(
            'TS.FILLGAPS', 'ts_nan_val', 1000, 3000,
            'FREQUENCY', 1000, 'VALUE', 'nan'
        )
        assert gaps_filled == 1

        result = self.client.execute_command('TS.RANGE', 'ts_nan_val', '-', '+')
        assert math.isnan(float(result[1][1]))

    def test_fillgaps_with_infinity_value(self):
        """Fill gaps with positive infinity as the fill value."""
        self.client.execute_command('TS.CREATE', 'ts_inf')
        self.client.execute_command('TS.ADD', 'ts_inf', 1000, 10)
        self.client.execute_command('TS.ADD', 'ts_inf', 3000, 30)

        gaps_filled = self.client.execute_command(
            'TS.FILLGAPS', 'ts_inf', 1000, 3000,
            'FREQUENCY', 1000, 'VALUE', 'inf'
        )
        assert gaps_filled == 1

        result = self.client.execute_command('TS.RANGE', 'ts_inf', '-', '+')
        assert float(result[1][1]) == float('inf')

    def test_fillgaps_with_negative_infinity_value(self):
        """Fill gaps with negative infinity as the fill value."""
        self.client.execute_command('TS.CREATE', 'ts_neginf')
        self.client.execute_command('TS.ADD', 'ts_neginf', 1000, 10)
        self.client.execute_command('TS.ADD', 'ts_neginf', 3000, 30)

        gaps_filled = self.client.execute_command(
            'TS.FILLGAPS', 'ts_neginf', 1000, 3000,
            'FREQUENCY', 1000, 'VALUE', '-inf'
        )
        assert gaps_filled == 1

        result = self.client.execute_command('TS.RANGE', 'ts_neginf', '-', '+')
        assert float(result[1][1]) == float('-inf')

    def test_fillgaps_submillisecond_frequency(self):
        """Fill gaps with a sub-millisecond frequency (should be truncated to 0 or error)."""
        self.client.execute_command('TS.CREATE', 'ts_subms')
        self.client.execute_command('TS.ADD', 'ts_subms', 1000, 10)
        self.client.execute_command('TS.ADD', 'ts_subms', 2000, 20)

        # Frequency of 0ms results in "frequency must be positive"
        # parse_duration_arg with "0" may yield 0ms
        with pytest.raises(ResponseError) as excinfo:
            self.client.execute_command(
                'TS.FILLGAPS', 'ts_subms', 1000, 2000, 'FREQUENCY', 0
            )
        assert "frequency must be positive" in str(excinfo.value).lower()

    def test_fillgaps_frequency_as_duration_string(self):
        """Use duration strings (e.g., '1s', '500ms') for FREQUENCY."""
        self.client.execute_command('TS.CREATE', 'ts_duration')
        self.client.execute_command('TS.ADD', 'ts_duration', 1000, 10)
        self.client.execute_command('TS.ADD', 'ts_duration', 2000, 20)
        # gap at 1500 (if frequency allows it)

        # 500ms = 500 millis
        gaps_filled = self.client.execute_command(
            'TS.FILLGAPS', 'ts_duration', 1000, 2000, 'FREQUENCY', 500
        )
        assert gaps_filled == 1  # 1500

        timestamps = self._get_timestamps('ts_duration')
        assert timestamps == [1000, 1500, 2000]

    def test_fillgaps_align_with_large_offset(self):
        """ALIGN with off-grid existing samples snaps them to the aligned grid.

        Existing samples at 1500 and 3500 are not on the epoch-aligned grid
        (1000, 2000, 3000, 4000), so all four grid points in range are filled.
        """
        self.client.execute_command('TS.CREATE', 'ts_align_offset')
        self.client.execute_command('TS.ADD', 'ts_align_offset', 1500, 15)
        self.client.execute_command('TS.ADD', 'ts_align_offset', 3500, 35)

        # ALIGN=0, frequency=1000: grid is 1000, 2000, 3000, 4000
        # Existing {1500, 3500} are not on the grid
        # All 4 grid points in [1000, 4000] are gaps
        gaps_filled = self.client.execute_command(
            'TS.FILLGAPS', 'ts_align_offset', 1000, 4000,
            'FREQUENCY', 1000, 'ALIGN', 0
        )
        assert gaps_filled == 4  # 1000, 2000, 3000, 4000

        timestamps = self._get_timestamps('ts_align_offset')
        assert 1000 in timestamps
        assert 2000 in timestamps
        assert 3000 in timestamps
        assert 4000 in timestamps

    def test_fillgaps_end_before_start(self):
        """Error when endTimestamp is before startTimestamp."""
        self.client.execute_command('TS.CREATE', 'ts_reverse')
        self.client.execute_command('TS.ADD', 'ts_reverse', 1000, 10)

        # end < start: should return an error since TimestampRange::new rejects start > end
        with pytest.raises(ResponseError) as excinfo:
            self.client.execute_command(
                'TS.FILLGAPS', 'ts_reverse', 5000, 1000, 'FREQUENCY', 1000
            )
        assert "invalid timestamp range" in str(excinfo.value).lower()

    # ------------------------------------------------------------------
    # Chunk boundary tests
    # ------------------------------------------------------------------

    def test_fillgaps_across_chunk_boundaries(self):
        """Fill gaps across multiple chunk boundaries (small chunk size)."""
        self.client.execute_command('TS.CREATE', 'ts_chunks', 'CHUNK_SIZE', '128')
        # Add samples spanning multiple chunks
        for i in range(50):
            self.client.execute_command('TS.ADD', 'ts_chunks', 1000 + i * 100, i)

        # Delete some samples from the middle to create gaps
        self.client.execute_command('TS.DEL', 'ts_chunks', 2000, 2500)

        gaps_filled = self.client.execute_command(
            'TS.FILLGAPS', 'ts_chunks', 1000, 5900, 'FREQUENCY', 100
        )
        # 2000, 2100, 2200, 2300, 2400, 2500 = 6 gaps
        assert gaps_filled > 0

        # Verify all timestamps from 1000 to 5900 step 100 exist
        timestamps = self._get_timestamps('ts_chunks')
        for ts in range(1000, 6000, 100):
            assert ts in timestamps, f"Timestamp {ts} missing after fill"

    # ------------------------------------------------------------------
    # Return value verification
    # ------------------------------------------------------------------

    def test_fillgaps_returns_zero_when_no_gaps(self):
        """Return 0 when the range has no missing timestamps."""
        self._create_series_with_uniform_data('ts_nogaps', 0, 100, 10)

        gaps_filled = self.client.execute_command(
            'TS.FILLGAPS', 'ts_nogaps', 0, 900, 'FREQUENCY', 100
        )
        assert gaps_filled == 0

    def test_fillgaps_returns_exact_gap_count(self):
        """Return the exact number of timestamps filled."""
        self.client.execute_command('TS.CREATE', 'ts_exact_count')
        # Create 10 equally spaced samples
        for i in range(10):
            self.client.execute_command('TS.ADD', 'ts_exact_count', i * 100, i)

        # Delete 3 specific samples
        self.client.execute_command('TS.DEL', 'ts_exact_count', 200, 200)
        self.client.execute_command('TS.DEL', 'ts_exact_count', 500, 500)
        self.client.execute_command('TS.DEL', 'ts_exact_count', 800, 800)

        gaps_filled = self.client.execute_command(
            'TS.FILLGAPS', 'ts_exact_count', 0, 900, 'FREQUENCY', 100
        )
        assert gaps_filled == 3

    # ------------------------------------------------------------------
    # Multiple FILLGAPS calls
    # ------------------------------------------------------------------

    def test_fillgaps_idempotent(self):
        """Running FILLGAPS twice on the same range produces 0 gaps on second call."""
        self.client.execute_command('TS.CREATE', 'ts_idempotent')
        self.client.execute_command('TS.ADD', 'ts_idempotent', 1000, 10)
        self.client.execute_command('TS.ADD', 'ts_idempotent', 3000, 30)

        # First fill
        gaps1 = self.client.execute_command(
            'TS.FILLGAPS', 'ts_idempotent', 1000, 3000, 'FREQUENCY', 1000
        )
        assert gaps1 == 1

        # Second fill on same range
        gaps2 = self.client.execute_command(
            'TS.FILLGAPS', 'ts_idempotent', 1000, 3000, 'FREQUENCY', 1000
        )
        assert gaps2 == 0

        # Verify totalSamples didn't double-count
        info = self.ts_info('ts_idempotent')
        assert info['totalSamples'] == 3

    def test_fillgaps_multiple_ranges(self):
        """Fill gaps in multiple non-overlapping ranges."""
        self.client.execute_command('TS.CREATE', 'ts_multi')
        self.client.execute_command('TS.ADD', 'ts_multi', 0, 0)
        self.client.execute_command('TS.ADD', 'ts_multi', 5000, 50)
        self.client.execute_command('TS.ADD', 'ts_multi', 10000, 100)

        # Fill first half
        gaps1 = self.client.execute_command(
            'TS.FILLGAPS', 'ts_multi', 0, 4000, 'FREQUENCY', 1000
        )
        assert gaps1 == 4  # 1000, 2000, 3000, 4000

        # Fill second half
        gaps2 = self.client.execute_command(
            'TS.FILLGAPS', 'ts_multi', 5000, 10000, 'FREQUENCY', 1000
        )
        assert gaps2 == 4  # 6000, 7000, 8000, 9000; 5000&10000 already exist

        timestamps = self._get_timestamps('ts_multi')
        assert timestamps == [0, 1000, 2000, 3000, 4000, 5000, 6000, 7000, 8000, 9000, 10000]
