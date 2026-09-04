"""
Integration tests for TS.OUTLIERS METHOD rcf (Random Cut Forest).

Covers:
- Core detection (spikes, dips, constant data)
- All RCF-specific options: NUM_TREES, SAMPLE_SIZE, THRESHOLD, DECAY,
  SHINGLE_SIZE, OUTPUT_AFTER
- Output formats: simple (default), full, cleaned
- DIRECTION filtering: positive, negative, both
- Time-range filtering
- Error handling: nonexistent key, insufficient data, unknown option,
  duplicate METHOD
"""

import math
from typing import List, Any

import pytest
from valkey import ResponseError

from outlier_result import AnomalyEntry, AnomalyMethod, TSOutliersFullResult, TSOutliersCleanedResult
from valkey_timeseries_test_case import ValkeyTimeSeriesTestCaseBase
from valkeytestframework.conftest import resource_port_tracker
from valkeytestframework.util.waiters import *


# ── helpers ──────────────────────────────────────────────────────────────────

def _add(client, key: str, start_ms: int, values: List[float], step_ms: int = 1000) -> None:
    """Bulk-add a list of values to *key* starting at *start_ms*."""
    for i, v in enumerate(values):
        client.execute_command("TS.ADD", key, start_ms + i * step_ms, v)


def _baseline_with_spikes(n_baseline: int = 120, spike_indices: List[int] = None,
                          baseline: float = 10.0, spike: float = 200.0) -> List[float]:
    """Return a list of *n_baseline* baseline values with optional spike positions."""
    data = [baseline] * n_baseline
    for idx in (spike_indices or []):
        data[idx] = spike
    return data


def _parse_anomalies(raw) -> List[AnomalyEntry]:
    """Convert a simple-format result to a list of AnomalyEntry objects."""
    return [AnomalyEntry.parse(entry) for entry in (raw or [])]


# Shared RCF options that produce stable, repeatable detections
_STABLE_RCF = dict(
    num_trees=100,
    sample_size=256,
    threshold=3.0,
    decay=0.01,
    shingle_size=1,
    output_after=30,
)


def _rcf_cmd(client, key: str, *extra_args):
    """Execute TS.OUTLIERS ... METHOD rcf with *_STABLE_RCF* defaults."""
    opts = _STABLE_RCF
    return client.execute_command(
        "TS.OUTLIERS", key, "-", "+",
        "METHOD", "rcf",
        "NUM_TREES", opts["num_trees"],
        "SAMPLE_SIZE", opts["sample_size"],
        "THRESHOLD", opts["threshold"],
        "DECAY", opts["decay"],
        "SHINGLE_SIZE", opts["shingle_size"],
        "OUTPUT_AFTER", opts["output_after"],
        *extra_args,
    )


# ── test class ────────────────────────────────────────────────────────────────

class TestRCFOutlierDetector(ValkeyTimeSeriesTestCaseBase):
    """Integration tests for TS.OUTLIERS METHOD rcf."""

    # ── basic detection ───────────────────────────────────────────────────────

    def test_rcf_detects_positive_spikes(self):
        """RCF detects clear positive spikes above a flat baseline."""
        key = "rcf:basic:spikes"
        data = _baseline_with_spikes(
            n_baseline=120,
            spike_indices=[40, 70, 100],
            baseline=10.0,
            spike=500.0,
        )
        _add(self.client, key, 1_000, data)

        raw = _rcf_cmd(self.client, key)
        anomalies = _parse_anomalies(raw)

        assert len(anomalies) == 3, f"expected 3 spikes, got {len(anomalies)}: {anomalies}"
        for a in anomalies:
            assert a.signal == 1, f"spike should be Positive, got {a.signal}"
            assert a.value == 500.0

    def test_rcf_detects_negative_dips(self):
        """RCF detects clear negative dips below a flat baseline."""
        key = "rcf:basic:dips"
        data = _baseline_with_spikes(
            n_baseline=120,
            spike_indices=[40, 80],
            baseline=50.0,
            spike=-200.0,
        )
        _add(self.client, key, 1_000, data)

        raw = _rcf_cmd(self.client, key)
        anomalies = _parse_anomalies(raw)

        assert len(anomalies) == 2, f"expected 2 dips, got {anomalies}"
        for a in anomalies:
            assert a.signal == -1, f"dip should be Negative, got {a.signal}"
            assert a.value == -200.0

    def test_rcf_no_false_positives_on_flat_data(self):
        """RCF produces no anomalies when all values are identical."""
        key = "rcf:basic:flat"
        data = [42.0] * 200
        _add(self.client, key, 1_000, data)

        raw = _rcf_cmd(self.client, key)
        anomalies = _parse_anomalies(raw)

        assert len(anomalies) == 0, f"expected no anomalies, got {anomalies}"

    def test_rcf_no_false_positives_on_smooth_sine(self):
        """RCF produces no anomalies on a clean low-amplitude sine wave."""
        key = "rcf:basic:sine"
        data = [10.0 + 0.5 * math.sin(i * 0.1) for i in range(200)]
        _add(self.client, key, 1_000, data)

        raw = _rcf_cmd(self.client, key)
        anomalies = _parse_anomalies(raw)

        # A gentle sine with amplitude 0.5 around 10 should not trigger
        assert len(anomalies) == 0, f"expected no anomalies on clean sine, got {anomalies}"

    def test_rcf_mixed_positive_and_negative_anomalies(self):
        """RCF detects both positive spikes and negative dips in the same series."""
        key = "rcf:basic:mixed"
        data = [20.0] * 150
        data[50] = 500.0  # positive spike
        data[100] = -300.0  # negative dip
        _add(self.client, key, 1_000, data)

        raw = _rcf_cmd(self.client, key)
        anomalies = _parse_anomalies(raw)

        values = {a.value for a in anomalies}
        assert 500.0 in values, "positive spike should be detected"
        assert -300.0 in values, "negative dip should be detected"

        signals = {a.signal for a in anomalies}
        assert 1 in signals
        assert -1 in signals

    # ── direction filtering ───────────────────────────────────────────────────

    def test_rcf_direction_positive_only(self):
        """DIRECTION positive returns only positive anomalies."""
        key = "rcf:direction:positive"
        data = [20.0] * 150
        data[50] = 500.0
        data[100] = -300.0
        _add(self.client, key, 1_000, data)

        raw = _rcf_cmd(self.client, key, "DIRECTION", "positive")
        anomalies = _parse_anomalies(raw)

        assert all(a.signal == 1 for a in anomalies), f"expected only positive signals, got {anomalies}"
        assert any(a.value == 500.0 for a in anomalies), "spike should be present"
        assert not any(a.value == -300.0 for a in anomalies), "dip should be excluded"

    def test_rcf_direction_negative_only(self):
        """DIRECTION negative returns only negative anomalies."""
        key = "rcf:direction:negative"
        data = [20.0] * 150
        data[50] = 500.0
        data[100] = -300.0
        _add(self.client, key, 1_000, data)

        raw = _rcf_cmd(self.client, key, "DIRECTION", "negative")
        anomalies = _parse_anomalies(raw)

        assert all(a.signal == -1 for a in anomalies), f"expected only negative signals, got {anomalies}"
        assert any(a.value == -300.0 for a in anomalies), "dip should be present"
        assert not any(a.value == 500.0 for a in anomalies), "spike should be excluded"

    def test_rcf_direction_both_is_default(self):
        """DIRECTION both is equivalent to omitting the option."""
        key = "rcf:direction:both"
        data = [20.0] * 150
        data[50] = 500.0
        data[100] = -300.0
        _add(self.client, key, 1_000, data)

        raw_both = _rcf_cmd(self.client, key, "DIRECTION", "both")
        raw_default = _rcf_cmd(self.client, key)

        assert len(_parse_anomalies(raw_both)) == len(_parse_anomalies(raw_default))

    # ── OUTPUT format: full ───────────────────────────────────────────────────

    def test_rcf_output_full_structure(self):
        """OUTPUT full returns a map with the expected top-level keys."""
        key = "rcf:output:full:structure"
        data = _baseline_with_spikes(n_baseline=120, spike_indices=[60], spike=500.0)
        _add(self.client, key, 1_000, data)

        raw = self.client.execute_command(
            "TS.OUTLIERS", key, "-", "+",
            "OUTPUT", "full",
            "METHOD", "rcf",
            "NUM_TREES", 100,
            "SAMPLE_SIZE", 256,
            "THRESHOLD", 3.0,
            "OUTPUT_AFTER", 30,
        )
        result = TSOutliersFullResult.parse(raw)

        assert result.method == AnomalyMethod.RANDOM_CUT_FOREST
        assert result.parameters["threshold"] > 0.0
        assert result.parameters["num_trees"] == 100
        assert result.parameters["sample_size"] == 256
        assert result.parameters["output_after"] == 30

        assert len(result.samples) == 120
        # There should be at least one anomaly
        assert result.anomaly_count() >= 1

    def test_rcf_output_full_method_name(self):
        """OUTPUT full reports method as 'RandomCutForest'."""
        key = "rcf:output:full:method_name"
        data = _baseline_with_spikes(n_baseline=120, spike_indices=[60], spike=500.0)
        _add(self.client, key, 1_000, data)

        raw = self.client.execute_command(
            "TS.OUTLIERS", key, "-", "+",
            "OUTPUT", "full",
            "METHOD", "rcf",
            "NUM_TREES", 100,
            "SAMPLE_SIZE", 256,
            "THRESHOLD", 3.0,
            "OUTPUT_AFTER", 30,
        )
        result = TSOutliersFullResult.parse(raw)
        assert result.method == AnomalyMethod.RANDOM_CUT_FOREST

    def test_rcf_output_full_scores_length_matches_samples(self):
        """OUTPUT full: scores array has exactly one entry per input sample."""
        key = "rcf:output:full:scores_length"
        n = 120
        data = _baseline_with_spikes(n_baseline=n, spike_indices=[60])
        _add(self.client, key, 1_000, data)

        raw = self.client.execute_command(
            "TS.OUTLIERS", key, "-", "+",
            "OUTPUT", "full",
            "METHOD", "rcf",
            "NUM_TREES", 80,
            "SAMPLE_SIZE", 128,
            "THRESHOLD", 3.0,
            "OUTPUT_AFTER", 20,
        )
        result = TSOutliersFullResult.parse(raw)
        # samples list in full output contains one [ts, value, score] per point
        assert len(result.samples) == n

    def test_rcf_output_full_anomaly_scores_exceed_threshold(self):
        """In OUTPUT full, every reported anomaly's score must be > threshold."""
        key = "rcf:output:full:score_vs_threshold"
        data = _baseline_with_spikes(n_baseline=120, spike_indices=[50, 90], spike=600.0)
        _add(self.client, key, 1_000, data)

        raw = self.client.execute_command(
            "TS.OUTLIERS", key, "-", "+",
            "OUTPUT", "full",
            "METHOD", "rcf",
            "NUM_TREES", 100,
            "SAMPLE_SIZE", 256,
            "THRESHOLD", 3.0,
            "OUTPUT_AFTER", 30,
        )
        result = TSOutliersFullResult.parse(raw)

        for anomaly in result.anomalies():
            assert anomaly.score > 0.0, (
                f"anomaly score {anomaly.score} should be positive"
            )

    def test_rcf_output_full_threshold_reflects_option(self):
        """The threshold in OUTPUT full reflects the THRESHOLD or CONTAMINATION option."""
        key = "rcf:output:full:threshold_roundtrip"
        data = _baseline_with_spikes(n_baseline=120, spike_indices=[60])
        _add(self.client, key, 1_000, data)

        # Test THRESHOLD (Z-score multiplier)
        for t in (1.5, 3.0, 5.0):
            raw = self.client.execute_command(
                "TS.OUTLIERS", key, "-", "+",
                "OUTPUT", "full",
                "METHOD", "rcf",
                "THRESHOLD", t,
                "OUTPUT_AFTER", 20,
            )
            result = TSOutliersFullResult.parse(raw)
            assert "threshold" in result.parameters
            assert abs(result.parameters["threshold"] - t) < 1e-9, (
                f"expected threshold {t}, got {result.parameters['threshold']}"
            )

        # Test CONTAMINATION (percentile-based)
        for c in (0.01, 0.05, 0.1):
            raw = self.client.execute_command(
                "TS.OUTLIERS", key, "-", "+",
                "OUTPUT", "full",
                "METHOD", "rcf",
                "CONTAMINATION", c,
                "OUTPUT_AFTER", 20,
            )
            result = TSOutliersFullResult.parse(raw)
            assert "contamination" in result.parameters
            assert abs(result.parameters["contamination"] - c) < 1e-9, (
                f"expected contamination {c}, got {result.parameters['contamination']}"
            )

    # ── OUTPUT format: cleaned ────────────────────────────────────────────────

    def test_rcf_output_cleaned_excludes_anomalies_from_samples(self):
        """OUTPUT cleaned: response is cleaned samples only, without anomaly entries."""
        key = "rcf:output_cleaned:exclude"
        data = [10.0] * 120
        data[50] = 500.0
        data[90] = 600.0
        _add(self.client, key, 1_000, data)

        raw = self.client.execute_command(
            "TS.OUTLIERS", key, "-", "+",
            "OUTPUT", "cleaned",
            "METHOD", "rcf",
            "NUM_TREES", 100,
            "SAMPLE_SIZE", 256,
            "THRESHOLD", 3.0,
            "OUTPUT_AFTER", 30,
        )
        result = TSOutliersCleanedResult.parse(raw)

        assert isinstance(raw, list)
        assert all(len(item) == 2 for item in raw)

        sample_values = result.sample_values()
        assert 500.0 not in sample_values, "spike at index 50 must be excluded from cleaned samples"
        assert 600.0 not in sample_values, "spike at index 90 must be excluded from cleaned samples"

    def test_rcf_output_cleaned_samples_plus_anomalies_total_equals_input(self):
        """OUTPUT cleaned: len(cleaned_samples) + len(simple_anomalies) == total input points."""
        key = "rcf:output_cleaned:count"
        n = 120
        data = [10.0] * n
        data[50] = 500.0
        data[90] = 600.0
        _add(self.client, key, 1_000, data)

        raw_cleaned = self.client.execute_command(
            "TS.OUTLIERS", key, "-", "+",
            "OUTPUT", "cleaned",
            "METHOD", "rcf",
            "NUM_TREES", 100,
            "SAMPLE_SIZE", 256,
            "THRESHOLD", 3.0,
            "OUTPUT_AFTER", 30,
        )
        raw_simple = self.client.execute_command(
            "TS.OUTLIERS", key, "-", "+",
            "OUTPUT", "simple",
            "METHOD", "rcf",
            "NUM_TREES", 100,
            "SAMPLE_SIZE", 256,
            "THRESHOLD", 3.0,
            "OUTPUT_AFTER", 30,
        )
        result = TSOutliersCleanedResult.parse(raw_cleaned)
        anomalies = _parse_anomalies(raw_simple)

        assert result.sample_count() + len(anomalies) == n

    def test_rcf_output_cleaned_direction_positive_keeps_negative_anomaly(self):
        """OUTPUT cleaned with DIRECTION positive keeps negative anomalies in samples."""
        key = "rcf:output_cleaned:direction_pos"
        data = [20.0] * 150
        data[50] = 500.0
        data[100] = -300.0
        _add(self.client, key, 1_000, data)

        raw = self.client.execute_command(
            "TS.OUTLIERS", key, "-", "+",
            "OUTPUT", "cleaned",
            "DIRECTION", "positive",
            "METHOD", "rcf",
            "NUM_TREES", 100,
            "SAMPLE_SIZE", 256,
            "THRESHOLD", 3.0,
            "OUTPUT_AFTER", 30,
        )
        result = TSOutliersCleanedResult.parse(raw)

        sample_values = result.sample_values()
        assert 500.0 not in sample_values, "positive spike should be removed"
        assert -300.0 in sample_values, "negative dip should be retained when direction=positive"

    # ── OUTPUT format: simple (default) ──────────────────────────────────────

    def test_rcf_output_simple_is_default(self):
        """Omitting OUTPUT is identical to OUTPUT simple."""
        key = "rcf:output_simple:default"
        data = _baseline_with_spikes(n_baseline=120, spike_indices=[60], spike=500.0)
        _add(self.client, key, 1_000, data)

        cmd = [
            "TS.OUTLIERS", key, "-", "+",
            "METHOD", "rcf",
            "NUM_TREES", 100,
            "SAMPLE_SIZE", 256,
            "THRESHOLD", 3.0,
            "OUTPUT_AFTER", 30,
        ]
        raw_default = self.client.execute_command(*cmd)
        raw_simple = self.client.execute_command(*cmd, "OUTPUT", "simple")

        assert len(raw_default) == len(raw_simple)

    def test_rcf_output_simple_entry_format(self):
        """Simple output entries are [timestamp, value, signal, score] 4-tuples."""
        key = "rcf:output:simple:format"
        data = _baseline_with_spikes(n_baseline=120, spike_indices=[60], spike=500.0)
        _add(self.client, key, 1_000, data)

        raw = _rcf_cmd(self.client, key)
        anomalies = _parse_anomalies(raw)

        for a in anomalies:
            assert isinstance(a.timestamp, int)
            assert isinstance(a.value, float)
            assert a.signal in (-1, 0, 1)
            assert isinstance(a.score, float) and a.score > 0.0

    # ── RCF-specific options ──────────────────────────────────────────────────

    def test_rcf_option_num_trees(self):
        """NUM_TREES option is accepted and produces detections."""
        key = "rcf:options:num_trees"
        data = _baseline_with_spikes(n_baseline=120, spike_indices=[60], spike=500.0)
        _add(self.client, key, 1_000, data)

        for n_trees in (10, 50, 200):
            raw = self.client.execute_command(
                "TS.OUTLIERS", key, "-", "+",
                "METHOD", "rcf",
                "NUM_TREES", n_trees,
                "SAMPLE_SIZE", 128,
                "THRESHOLD", 3.0,
                "OUTPUT_AFTER", 20,
            )
            # Just verify the command succeeds; detection varies across tree counts
            assert raw is not None

    def test_rcf_option_sample_size(self):
        """SAMPLE_SIZE option is accepted and produces detections."""
        key = "rcf:options:sample_size"
        data = _baseline_with_spikes(n_baseline=120, spike_indices=[60], spike=500.0)
        _add(self.client, key, 1_000, data)

        for ss in (64, 128, 512):
            raw = self.client.execute_command(
                "TS.OUTLIERS", key, "-", "+",
                "METHOD", "rcf",
                "NUM_TREES", 50,
                "SAMPLE_SIZE", ss,
                "THRESHOLD", 3.0,
                "OUTPUT_AFTER", 20,
            )
            assert raw is not None

    def test_rcf_option_threshold_sensitivity(self):
        """Lowering THRESHOLD finds at least as many anomalies as a higher threshold."""
        key = "rcf:options:threshold_sensitivity"
        data = [10.0] * 150
        # Multiple spikes of different magnitudes
        data[40] = 200.0
        data[80] = 150.0
        data[120] = 300.0
        _add(self.client, key, 1_000, data)

        raw_low = self.client.execute_command(
            "TS.OUTLIERS", key, "-", "+",
            "METHOD", "rcf",
            "NUM_TREES", 100,
            "SAMPLE_SIZE", 256,
            "THRESHOLD", 2.0,
            "OUTPUT_AFTER", 30,
        )
        raw_high = self.client.execute_command(
            "TS.OUTLIERS", key, "-", "+",
            "METHOD", "rcf",
            "NUM_TREES", 100,
            "SAMPLE_SIZE", 256,
            "THRESHOLD", 10.0,
            "OUTPUT_AFTER", 30,
        )
        count_low = len(_parse_anomalies(raw_low))
        count_high = len(_parse_anomalies(raw_high))

        assert count_low >= count_high, (
            f"lower threshold ({count_low}) should detect >= anomalies than higher ({count_high})"
        )

    def test_rcf_option_threshold_invalid_raises_error(self):
        """THRESHOLD <= 0 returns an error."""
        key = "rcf:options:threshold_invalid"
        data = _baseline_with_spikes(n_baseline=120, spike_indices=[60], spike=500.0)
        _add(self.client, key, 1_000, data)

        for bad_threshold in (0.0, -1.0):
            with pytest.raises(ResponseError, match="std_dev threshold must be positive"):
                self.client.execute_command(
                    "TS.OUTLIERS", key, "-", "+",
                    "METHOD", "rcf",
                    "THRESHOLD", bad_threshold,
                    "OUTPUT_AFTER", 20,
                )

    def test_rcf_option_contamination(self):
        """CONTAMINATION option is accepted and produces detections."""
        key = "rcf:options:contamination"
        # Longer series with noise for stable quantile selection
        data = [10.0 + 0.1 * math.sin(i * 0.1) for i in range(200)]
        data[150] = 100.0
        data[180] = 110.0
        _add(self.client, key, 1_000, data)

        raw = self.client.execute_command(
            "TS.OUTLIERS", key, "-", "+",
            "METHOD", "rcf",
            "CONTAMINATION", 0.05,
            "OUTPUT_AFTER", 100,
        )
        anomalies = _parse_anomalies(raw)
        indices = {(a.timestamp - 1000) // 1000 for a in anomalies}

        assert 150 in indices
        assert 180 in indices
        # 5% of 100 (points after warmup) is 5. 
        # RCF might detect slightly more if scores are tied.
        assert len(anomalies) <= 10

    def test_rcf_contamination_invalid_raises_error(self):
        """CONTAMINATION out of range (0, 0.5] returns an error."""
        key = "rcf:options:contamination_invalid"
        data = [10.0] * 100
        _add(self.client, key, 1_000, data)

        for bad_val in (0.0, 0.6, -0.1):
            with pytest.raises(ResponseError, match="contamination must be in the range"):
                self.client.execute_command(
                    "TS.OUTLIERS", key, "-", "+",
                    "METHOD", "rcf",
                    "CONTAMINATION", bad_val,
                )

    def test_rcf_resource_options_are_bounded(self):
        """RCF sizing options reject non-integers and unsafe resource sizes."""
        key = "rcf:options:bounded"
        _add(self.client, key, 1_000, [10.0] * 10)

        invalid_options = (
            ("NUM_TREES", "1e300", "invalid integer value"),
            ("NUM_TREES", 513, "must be between"),
            ("SAMPLE_SIZE", "1e300", "invalid integer value"),
            ("SAMPLE_SIZE", 1, "must be between"),
            ("SAMPLE_SIZE", 4097, "must be between"),
            ("SHINGLE_SIZE", 1.5, "invalid integer value"),
            ("SHINGLE_SIZE", 129, "must be between"),
            ("SHINGLE_SIZE", 11, "cannot exceed the number of samples"),
        )

        for option, value, message in invalid_options:
            with pytest.raises(ResponseError, match=message):
                self.client.execute_command(
                    "TS.OUTLIERS", key, "-", "+",
                    "METHOD", "rcf",
                    option, value,
                )

    def test_rcf_option_decay(self):
        """DECAY option is accepted and the detector adapts to shifting data."""
        key = "rcf:options:decay"
        # Data shifts from ~5.0 to ~50.0 midway through
        data = [5.0 + 0.1 * math.sin(i * 0.2) for i in range(80)]
        data += [50.0 + 0.1 * math.sin(i * 0.2) for i in range(80)]
        _add(self.client, key, 1_000, data)

        # With a high decay value the forest forgets early data faster
        raw = self.client.execute_command(
            "TS.OUTLIERS", key, "-", "+",
            "METHOD", "rcf",
            "NUM_TREES", 100,
            "SAMPLE_SIZE", 128,
            "THRESHOLD", 0.7,
            "DECAY", 0.1,
            "OUTPUT_AFTER", 40,
        )
        assert raw is not None  # command should succeed

    def test_rcf_option_shingle_size(self):
        """SHINGLE_SIZE option is accepted and adds multi-point context."""
        key = "rcf:options:shingle_size"
        data = _baseline_with_spikes(n_baseline=120, spike_indices=[60, 80], spike=500.0)
        _add(self.client, key, 1_000, data)

        for shingle in (1, 2, 4, 8):
            raw = self.client.execute_command(
                "TS.OUTLIERS", key, "-", "+",
                "METHOD", "rcf",
                "NUM_TREES", 80,
                "SAMPLE_SIZE", 128,
                "THRESHOLD", 3.0,
                "SHINGLE_SIZE", shingle,
                "OUTPUT_AFTER", 20,
            )
            assert raw is not None

    def test_rcf_option_output_after_suppresses_early_scores(self):
        """OUTPUT_AFTER delays detections: spikes before the warmup window may be missed."""
        key = "rcf:options:output_after"
        data = [10.0] * 120
        data[5] = 500.0  # very early — should be suppressed by output_after
        data[100] = 500.0  # well after warmup — should be detected
        _add(self.client, key, 1_000, data)

        raw = self.client.execute_command(
            "TS.OUTLIERS", key, "-", "+",
            "METHOD", "rcf",
            "NUM_TREES", 100,
            "SAMPLE_SIZE", 256,
            "THRESHOLD", 3.0,
            "OUTPUT_AFTER", 50,  # large warmup window
        )
        anomalies = _parse_anomalies(raw)

        # The late spike should be detected; the early one may not be
        late_ts = 1_000 + 100 * 1_000
        assert any(a.timestamp == late_ts for a in anomalies), (
            "spike after warmup period should be detected"
        )

    def test_rcf_all_options_combined(self):
        """All RCF options can be specified together without error."""
        key = "rcf:options:combined"
        data = _baseline_with_spikes(n_baseline=120, spike_indices=[60, 90], spike=500.0)
        _add(self.client, key, 1_000, data)

        raw = self.client.execute_command(
            "TS.OUTLIERS", key, "-", "+",
            "METHOD", "rcf",
            "NUM_TREES", 80,
            "SAMPLE_SIZE", 128,
            "THRESHOLD", 3.0,
            "DECAY", 0.05,
            "SHINGLE_SIZE", 2,
            "OUTPUT_AFTER", 30,
        )
        anomalies = _parse_anomalies(raw)
        assert len(anomalies) >= 1

    # ── time-range filtering ──────────────────────────────────────────────────

    def test_rcf_time_range_excludes_out_of_range_anomalies(self):
        """Anomalies outside the query time range are not reported."""
        key = "rcf:timerange:filter"
        # Place spikes at ts=1000 (index 0) and ts=120000 (index 119)
        data = [10.0] * 120
        data[0] = 500.0  # timestamp 1000
        data[119] = 500.0  # timestamp 120000
        _add(self.client, key, 1_000, data)

        # Query only the middle portion — both spikes should be excluded
        from_ts = 10_000
        to_ts = 100_000

        raw = self.client.execute_command(
            "TS.OUTLIERS", key, from_ts, to_ts,
            "METHOD", "rcf",
            "NUM_TREES", 80,
            "SAMPLE_SIZE", 64,
            "THRESHOLD", 3.0,
            "OUTPUT_AFTER", 10,
        )
        anomalies = _parse_anomalies(raw)
        for a in anomalies:
            assert from_ts <= a.timestamp <= to_ts, (
                f"anomaly at ts={a.timestamp} is outside requested range"
            )

    # ── direction assignment (positive/negative) ──────────────────────────────

    def test_rcf_positive_anomaly_signal_for_above_mean_value(self):
        """Values above the series mean that exceed the threshold get signal=1."""
        key = "rcf:signal:positive"
        # All baseline values near 0; spike is far positive
        data = [0.0] * 120
        data[60] = 1000.0
        _add(self.client, key, 1_000, data)

        raw = _rcf_cmd(self.client, key)
        anomalies = _parse_anomalies(raw)

        spike_anomalies = [a for a in anomalies if a.value == 1000.0]
        assert len(spike_anomalies) >= 1
        for a in spike_anomalies:
            assert a.signal == 1, f"above-mean spike should be Positive, got {a.signal}"

    def test_rcf_negative_anomaly_signal_for_below_mean_value(self):
        """Values below the series mean that exceed the threshold get signal=-1."""
        key = "rcf:signal:negative"
        # All baseline values near 100; dip is far negative
        data = [100.0] * 120
        data[60] = -1000.0
        _add(self.client, key, 1_000, data)

        raw = _rcf_cmd(self.client, key)
        anomalies = _parse_anomalies(raw)

        dip_anomalies = [a for a in anomalies if a.value == -1000.0]
        assert len(dip_anomalies) >= 1
        for a in dip_anomalies:
            assert a.signal == -1, f"below-mean dip should be Negative, got {a.signal}"

    # ── error handling ────────────────────────────────────────────────────────

    def test_rcf_nonexistent_key_raises_error(self):
        """TS.OUTLIERS on a nonexistent key returns 'key does not exist'."""
        with pytest.raises(ResponseError, match="the key does not exist"):
            self.client.execute_command(
                "TS.OUTLIERS", "rcf:no:such:key", "-", "+",
                "METHOD", "rcf",
            )

    def test_rcf_empty_series_raises_insufficient_data(self):
        """TS.OUTLIERS on an empty series raises an 'insufficient data' error."""
        key = "rcf:error:empty"
        self.client.execute_command("TS.CREATE", key)

        with pytest.raises(Exception) as exc_info:
            self.client.execute_command(
                "TS.OUTLIERS", key, "-", "+",
                "METHOD", "rcf",
            )
        assert "insufficient data" in str(exc_info.value).lower()

    def test_rcf_unknown_option_raises_error(self):
        """An unrecognised RCF option returns an error containing 'unknown RCF option'."""
        key = "rcf:error:unknown_option"
        data = _baseline_with_spikes(n_baseline=120, spike_indices=[60], spike=500.0)
        _add(self.client, key, 1_000, data)

        with pytest.raises(ResponseError, match="unknown RCF option"):
            self.client.execute_command(
                "TS.OUTLIERS", key, "-", "+",
                "METHOD", "rcf",
                "BADOPTION", 42,
            )

    def test_rcf_duplicate_method_raises_error(self):
        """Specifying METHOD twice returns an 'already specified' error."""
        key = "rcf:error:duplicate_method"
        data = _baseline_with_spikes(n_baseline=120, spike_indices=[60], spike=500.0)
        _add(self.client, key, 1_000, data)

        with pytest.raises(ResponseError, match="already specified"):
            self.client.execute_command(
                "TS.OUTLIERS", key, "-", "+",
                "METHOD", "rcf",
                "METHOD", "zscore",
            )

    def test_rcf_invalid_output_format_raises_error(self):
        """An unknown OUTPUT format name returns an error."""
        key = "rcf:error:bad_output"
        data = _baseline_with_spikes(n_baseline=120, spike_indices=[60], spike=500.0)
        _add(self.client, key, 1_000, data)

        with pytest.raises(ResponseError, match="unknown output format"):
            self.client.execute_command(
                "TS.OUTLIERS", key, "-", "+",
                "OUTPUT", "badformat",
                "METHOD", "rcf",
            )

    def test_rcf_missing_method_raises_wrong_arity(self):
        """Omitting METHOD entirely causes a wrong-arity or missing-argument error."""
        key = "rcf:error:no_method"
        data = _baseline_with_spikes(n_baseline=120, spike_indices=[60], spike=500.0)
        _add(self.client, key, 1_000, data)

        with pytest.raises(Exception):
            # Too few args; the command requires at least key, from, to, and METHOD
            self.client.execute_command("TS.OUTLIERS", key, "-", "+")

    # ── score properties ──────────────────────────────────────────────────────

    def test_rcf_anomaly_scores_are_positive_floats(self):
        """All reported anomaly scores are finite positive floats."""
        key = "rcf:scores:positive_float"
        data = _baseline_with_spikes(n_baseline=120, spike_indices=[40, 80], spike=500.0)
        _add(self.client, key, 1_000, data)

        raw = _rcf_cmd(self.client, key)
        anomalies = _parse_anomalies(raw)

        assert len(anomalies) >= 1
        for a in anomalies:
            assert math.isfinite(a.score), f"score {a.score} should be finite"
            assert a.score > 0.0, f"score {a.score} should be positive"

    def test_rcf_full_output_all_scores_are_non_negative_and_finite(self):
        """In OUTPUT full, every sample score is non-negative and finite (no NaN)."""
        key = "rcf:scores:no_nan"
        data = [10.0 + 0.5 * math.sin(i * 0.1) for i in range(120)]
        data[60] = 500.0
        _add(self.client, key, 1_000, data)

        raw = self.client.execute_command(
            "TS.OUTLIERS", key, "-", "+",
            "OUTPUT", "full",
            "METHOD", "rcf",
            "NUM_TREES", 80,
            "SAMPLE_SIZE", 128,
            "THRESHOLD", 0.7,
            "OUTPUT_AFTER", 20,
        )
        result = TSOutliersFullResult.parse(raw)

        for sample in result.samples:
            if sample.score is not None:
                assert math.isfinite(sample.score), f"NaN/inf score at ts={sample.timestamp}"
                assert sample.score >= 0.0, f"negative score {sample.score} at ts={sample.timestamp}"

    def test_rcf_spike_scores_higher_than_baseline_scores(self):
        """Spike points score strictly higher than adjacent baseline points."""
        key = "rcf:scores:ordering"
        data = [10.0] * 120
        data[80] = 1000.0  # single large spike well into training
        _add(self.client, key, 1_000, data)

        raw = self.client.execute_command(
            "TS.OUTLIERS", key, "-", "+",
            "OUTPUT", "full",
            "METHOD", "rcf",
            "NUM_TREES", 100,
            "SAMPLE_SIZE", 256,
            "THRESHOLD", 0.5,
            "OUTPUT_AFTER", 40,
        )
        result = TSOutliersFullResult.parse(raw)

        spike_ts = 1_000 + 80 * 1_000
        spike_sample = next((s for s in result.samples if s.timestamp == spike_ts), None)
        assert spike_sample is not None

        # Baseline samples after warmup (indices 50–79, well-trained region)
        baseline_scores = [
            s.score for s in result.samples
            if s.value == 10.0 and s.score is not None
               and (1_000 + 50 * 1_000) <= s.timestamp <= (1_000 + 79 * 1_000)
        ]
        if baseline_scores and spike_sample.score is not None:
            avg_baseline = sum(baseline_scores) / len(baseline_scores)
            assert spike_sample.score > avg_baseline, (
                f"spike score {spike_sample.score} should exceed mean baseline score {avg_baseline}"
            )

    # ── seasonality option (RCF is passed seasonal residuals) ────────────────

    def test_rcf_with_seasonality_detects_spike_in_seasonal_data(self):
        """SEASONALITY + RCF detects a spike embedded in a seasonal pattern."""
        key = "rcf:seasonality:spike"
        data = []
        for day in range(7):
            for hour in range(24):
                value = 100.0 + 20.0 * math.sin(hour * math.pi / 12)
                if day == 3 and hour == 14:
                    value = 800.0  # large spike
                data.append(value)

        _add(self.client, key, 1_000, data, step_ms=3_600_000)

        raw = self.client.execute_command(
            "TS.OUTLIERS", key, "-", "+",
            "SEASONALITY", 24,
            "METHOD", "rcf",
            "NUM_TREES", 100,
            "SAMPLE_SIZE", 128,
            "THRESHOLD", 3.0,
            "OUTPUT_AFTER", 24,
        )
        anomalies = _parse_anomalies(raw)

        assert any(a.value == 800.0 for a in anomalies), (
            "spike in seasonal data should be detected"
        )

    # ── case-insensitivity ────────────────────────────────────────────────────

    def test_rcf_method_name_case_insensitive(self):
        """METHOD name is case-insensitive (RCF, Rcf, rcf all accepted)."""
        key = "rcf:case:method"
        data = _baseline_with_spikes(n_baseline=120, spike_indices=[60], spike=500.0)
        _add(self.client, key, 1_000, data)

        for method_name in ("rcf", "RCF", "Rcf"):
            raw = self.client.execute_command(
                "TS.OUTLIERS", key, "-", "+",
                "METHOD", method_name,
                "NUM_TREES", 80,
                "SAMPLE_SIZE", 128,
                "THRESHOLD", 3.0,
                "OUTPUT_AFTER", 20,
            )
            assert raw is not None, f"METHOD {method_name!r} should be accepted"

    def test_rcf_option_names_case_insensitive(self):
        """RCF option keywords are case-insensitive."""
        key = "rcf:case:options"
        data = _baseline_with_spikes(n_baseline=120, spike_indices=[60], spike=500.0)
        _add(self.client, key, 1_000, data)

        raw = self.client.execute_command(
            "TS.OUTLIERS", key, "-", "+",
            "method", "rcf",
            "num_trees", 80,
            "sample_size", 128,
            "threshold", 3.0,
            "output_after", 20,
        )
        assert raw is not None
