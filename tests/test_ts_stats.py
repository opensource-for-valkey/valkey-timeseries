"""
Integration tests for TS.STATS command.

TS.STATS computes per-series statistical metrics for exploratory data analysis.
It returns a map (flat key-value list in RESP2) with fields like length, mean,
std, min, max, median, n_nans, n_zeros, etc.
"""

import math

import pytest
from valkey import ResponseError
from valkey_timeseries_test_case import ValkeyTimeSeriesTestCaseBase
from valkeytestframework.conftest import resource_port_tracker  # noqa: F401

# Byte strings that represent special float values in Valkey RESP.
_SPECIAL_FLOAT_BYTES = {b'nan', b'inf', b'-inf', b'infinity', b'-infinity'}


def _stats_to_dict(stats_result):
    """Convert a flat TS.STATS response list into a Python dict."""
    d = {}
    it = iter(stats_result)
    for k in it:
        v = next(it)
        if isinstance(k, bytes):
            k = k.decode()
        if isinstance(v, bytes):
            if v in _SPECIAL_FLOAT_BYTES:
                v = float(v)
            elif b'.' in v or b'e' in v or b'E' in v:
                v = float(v)
            else:
                v = int(v)
        d[k] = v
    return d


class TestTsStats(ValkeyTimeSeriesTestCaseBase):
    """Tests for TS.STATS command."""

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _create_and_populate(self, key, samples):
        """Create a key and add (timestamp, value) samples to it."""
        self.client.execute_command('TS.CREATE', key)
        for ts, val in samples:
            self.client.execute_command('TS.ADD', key, ts, val)

    def _stats(self, key, *args):
        """Run TS.STATS and return result as a dict."""
        cmd = ['TS.STATS', key] + list(args)
        result = self.client.execute_command(*cmd)
        return _stats_to_dict(result)

    # ------------------------------------------------------------------
    # Basic functionality
    # ------------------------------------------------------------------

    def test_basic_stats(self):
        """TS.STATS on a series with mixed values returns all expected fields."""
        self._create_and_populate('ts1', [
            (1000, 22.5),
            (2000, 23.1),
            (3000, 22.8),
            (4000, 0.0),
            (5000, 22.9),
        ])

        s = self._stats('ts1')

        assert s['length'] == 5
        assert s['start_timestamp'] == 1000
        assert s['end_timestamp'] == 5000
        assert s['mean'] == pytest.approx(18.26, rel=1e-4)
        assert s['std'] > 0
        assert s['min'] == 0.0
        assert s['max'] == 23.1
        assert s['range'] == pytest.approx(23.1, rel=1e-6)
        assert s['median'] == 22.8
        assert s['n_nans'] == 0
        assert s['n_zeros'] == 1
        assert s['n_positive'] == 4
        assert s['n_negative'] == 0
        assert s['n_unique_values'] == 5
        assert s['is_constant'] == 0
        assert s['plateau_size'] == 1
        assert s['plateau_size_non_zero'] == 1
        assert s['n_zeros_start'] == 0
        assert s['n_zeros_end'] == 0
        # skewness and kurtosis are present
        assert 'skewness' in s
        assert 'kurtosis' in s

    def test_with_timestamp_range(self):
        """TS.STATS with a timestamp range filters to that window."""
        self._create_and_populate('ts2', [
            (1000, 10.0),
            (2000, 20.0),
            (3000, 30.0),
            (4000, 40.0),
            (5000, 50.0),
        ])

        s = self._stats('ts2', 2000, 4000)

        assert s['length'] == 3
        assert s['start_timestamp'] == 2000
        assert s['end_timestamp'] == 4000
        assert s['min'] == 20.0
        assert s['max'] == 40.0
        assert s['mean'] == 30.0

    # ------------------------------------------------------------------
    # Edge cases
    # ------------------------------------------------------------------

    def test_single_value(self):
        """TS.STATS on a single-value series: std=0, constant, no plateau."""
        self._create_and_populate('ts_single', [(1000, 42.0)])

        s = self._stats('ts_single')

        assert s['length'] == 1
        assert s['mean'] == 42.0
        assert s['std'] == 0.0
        assert s['min'] == 42.0
        assert s['max'] == 42.0
        assert s['range'] == 0.0
        assert s['median'] == 42.0
        assert s['n_unique_values'] == 1
        assert s['is_constant'] == 1
        assert s['plateau_size'] == 1
        assert s['plateau_size_non_zero'] == 1
        # skewness/kurtosis may be NaN for a single value (division by zero)
        assert math.isnan(s['skewness'])
        assert math.isnan(s['kurtosis'])

    def test_all_identical_nonzero(self):
        """TS.STATS when all values are identical and non-zero."""
        self._create_and_populate('ts_const', [
            (1000, 7.0),
            (2000, 7.0),
            (3000, 7.0),
            (4000, 7.0),
        ])

        s = self._stats('ts_const')

        assert s['length'] == 4
        assert s['mean'] == 7.0
        assert s['std'] == 0.0
        assert s['min'] == 7.0
        assert s['max'] == 7.0
        assert s['range'] == 0.0
        assert s['is_constant'] == 1
        assert s['n_unique_values'] == 1
        assert s['n_zeros'] == 0
        assert s['plateau_size'] == 4
        assert s['plateau_size_non_zero'] == 4

    def test_all_zeros(self):
        """TS.STATS on all-zero series: mean=0, n_zeros=length."""
        self._create_and_populate('ts_zeros', [
            (1000, 0.0),
            (2000, 0.0),
            (3000, 0.0),
        ])

        s = self._stats('ts_zeros')

        assert s['length'] == 3
        assert s['mean'] == 0.0
        assert s['std'] == 0.0
        assert s['min'] == 0.0
        assert s['max'] == 0.0
        assert s['n_zeros'] == 3
        assert s['n_positive'] == 0
        assert s['n_negative'] == 0
        assert s['is_constant'] == 1
        assert s['plateau_size'] == 3
        assert s['plateau_size_non_zero'] == 0
        assert s['n_zeros_start'] == 3
        assert s['n_zeros_end'] == 3

    def test_leading_and_trailing_zeros(self):
        """TS.STATS correctly counts leading and trailing zeros."""
        self._create_and_populate('ts_lead_trail', [
            (1000, 0.0),
            (2000, 0.0),
            (3000, 5.0),
            (4000, 10.0),
            (5000, 0.0),
        ])

        s = self._stats('ts_lead_trail')

        assert s['n_zeros_start'] == 2
        assert s['n_zeros_end'] == 1
        assert s['n_zeros'] == 3
        assert s['n_zeros_start'] == 2
        assert s['n_zeros_end'] == 1

    def test_positive_and_negative_counts(self):
        """TS.STATS correctly counts positive and negative values."""
        self._create_and_populate('ts_signs', [
            (1000, -5.0),
            (2000, -2.0),
            (3000, 0.0),
            (4000, 3.0),
            (5000, 8.0),
        ])

        s = self._stats('ts_signs')

        assert s['n_positive'] == 2
        assert s['n_negative'] == 2
        assert s['n_zeros'] == 1

    def test_with_nan_values(self):
        """TS.STATS excludes NaN/Inf from value stats but counts them as n_nans."""
        # Use TS.ADD with 'nan'/'inf' string to insert non-finite values
        self.client.execute_command('TS.CREATE', 'ts_nan')
        self.client.execute_command('TS.ADD', 'ts_nan', 1000, 10.0)
        self.client.execute_command('TS.ADD', 'ts_nan', 2000, 'nan')
        self.client.execute_command('TS.ADD', 'ts_nan', 3000, 30.0)
        self.client.execute_command('TS.ADD', 'ts_nan', 4000, 'inf')
        self.client.execute_command('TS.ADD', 'ts_nan', 5000, '-inf')

        s = self._stats('ts_nan')

        assert s['length'] == 5
        assert s['n_nans'] == 3
        # Only the two finite values affect the stats
        assert s['mean'] == 20.0
        assert s['min'] == 10.0
        assert s['max'] == 30.0
        assert s['n_unique_values'] == 2
        # skewness/kurtosis may be NaN when only 2 finite values remain
        assert math.isnan(s['skewness'])
        assert math.isnan(s['kurtosis'])

    def test_plateau_detection(self):
        """TS.STATS detects the longest plateau correctly."""
        self._create_and_populate('ts_plateau', [
            (1000, 1.0),
            (2000, 2.0),
            (3000, 2.0),
            (4000, 2.0),
            (5000, 3.0),
            (6000, 0.0),
            (7000, 0.0),
        ])

        s = self._stats('ts_plateau')

        assert s['plateau_size'] == 3          # three 2.0's
        assert s['plateau_size_non_zero'] == 3  # same plateau, non-zero

    def test_zero_plateau_separate(self):
        """TS.STATS plateaus: zero plateau tracked separately from non-zero."""
        self._create_and_populate('ts_plateau2', [
            (1000, 0.0),
            (2000, 0.0),
            (3000, 0.0),
            (4000, 5.0),
            (5000, 5.0),
        ])

        s = self._stats('ts_plateau2')

        assert s['plateau_size'] == 3           # three zeros
        assert s['plateau_size_non_zero'] == 2  # two 5.0's

    # ------------------------------------------------------------------
    # Error cases
    # ------------------------------------------------------------------

    def test_missing_key(self):
        """TS.STATS on a non-existent key returns KEY_NOT_FOUND error."""
        with pytest.raises(ResponseError) as excinfo:
            self.client.execute_command('TS.STATS', 'nonexistent')
        assert 'key does not exist' in str(excinfo.value).lower()

    def test_wrong_arity(self):
        """TS.STATS with no key returns wrong arity error."""
        with pytest.raises(ResponseError) as excinfo:
            self.client.execute_command('TS.STATS')
        assert 'wrong number of arguments' in str(excinfo.value).lower()

    def test_too_many_arguments(self):
        """TS.STATS with extra arguments returns syntax error."""
        self._create_and_populate('ts_extra', [(1000, 1.0)])
        with pytest.raises(ResponseError):
            self.client.execute_command('TS.STATS', 'ts_extra', '1000', '2000', 'extra_arg')

    def test_invalid_timestamp(self):
        """TS.STATS with non-numeric timestamp returns parse error."""
        self._create_and_populate('ts_badts', [(1000, 1.0)])
        with pytest.raises(ResponseError):
            self.client.execute_command('TS.STATS', 'ts_badts', 'abc', '2000')

    # ------------------------------------------------------------------
    # Response completeness
    # ------------------------------------------------------------------

    def test_all_expected_fields_present(self):
        """TS.STATS returns exactly the expected set of fields."""
        self._create_and_populate('ts_fields', [
            (1000, 1.0),
            (2000, 2.0),
        ])

        s = self._stats('ts_fields')

        expected_fields = {
            'length', 'start_timestamp', 'end_timestamp',
            'mean', 'std', 'min', 'max', 'range', 'median',
            'n_nans', 'n_zeros', 'n_positive', 'n_negative',
            'n_unique_values', 'is_constant',
            'plateau_size', 'plateau_size_non_zero',
            'n_zeros_start', 'n_zeros_end',
            'skewness', 'kurtosis',
        }
        assert set(s.keys()) == expected_fields
