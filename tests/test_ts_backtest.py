"""
Integration tests for TS.BACKTEST command.

TS.BACKTEST evaluates one or more forecasting models against historical data
using walk-forward validation, returning out-of-sample accuracy metrics per
model.

Covers:
- Basic single- and multi-model backtests
- Response shape (array of flat maps: model/horizon/strategy/n_folds/metrics/folds)
- Aggregated metrics (mae, rmse, smape, mape, mae_std, rmse_std)
- Per-fold structure (timestamp boundaries + full per-fold metric set)
- STRATEGY EXPANDING vs ROLLING window behavior
- INITIAL_WINDOW, STEP, N_FOLDS, GAP, PURGE, EMBARGO options
- SEASONAL_PERIOD (MASE scaling) behavior
- WITH_PREDICTIONS (raw predictions/actuals per fold)
- Per-model failure isolation (a failing model reports `error`, others still return)
- Case-insensitive keywords and model names
- Time-range filtering
- Error handling: nonexistent key, missing HORIZON, missing MODELS, invalid
  model spec, bad STRATEGY, unknown argument, non-positive N_FOLDS/STEP/
  INITIAL_WINDOW, HORIZON 0, negative PURGE/EMBARGO, insufficient data, wrong arity
"""

import math
from typing import Any, Dict, List

import pytest
from valkey import ResponseError

from valkey_timeseries_test_case import ValkeyTimeSeriesTestCaseBase
from valkeytestframework.conftest import resource_port_tracker
from valkeytestframework.util.waiters import *
from data_helpers import (
    _add,
    create_daily_seasonal_series,
    create_linear_series,
    create_sine_series,
)


# ── response parsing ───────────────────────────────────────────────────────────

AGG_METRIC_KEYS = ["mae", "rmse", "smape", "mape", "mae_std", "rmse_std"]
FOLD_METRIC_KEYS = ["mae", "mse", "rmse", "mape", "smape", "mase", "r_squared"]


def _decode(x: Any) -> Any:
    return x.decode("utf-8") if isinstance(x, bytes) else x


def _parse_metric_map(value: List[Any]) -> Dict[str, Any]:
    """Parse a flat [k, v, k, v, ...] metric map into {str: float|None}."""
    out: Dict[str, Any] = {}
    it = iter(value)
    for k in it:
        key = _decode(k)
        v = next(it)
        out[key] = None if v is None else float(v)
    return out


def _parse_fold(entry: List[Any]) -> Dict[str, Any]:
    """Parse a single fold's flat key-value map."""
    out: Dict[str, Any] = {}
    it = iter(entry)
    for k in it:
        key = _decode(k)
        v = next(it)
        if key in ("train_start", "train_end", "test_start", "test_end"):
            out[key] = int(v)
        elif key == "metrics":
            out[key] = _parse_metric_map(v)
        elif key in ("predictions", "actuals"):
            out[key] = [float(x) for x in v]
        else:
            out[key] = _decode(v)
    return out


def parse_backtest_entry(entry: List[Any]) -> Dict[str, Any]:
    """Parse one model's flat key-value map from the TS.BACKTEST response."""
    out: Dict[str, Any] = {}
    it = iter(entry)
    for k in it:
        key = _decode(k)
        v = next(it)
        if key in ("horizon", "n_folds"):
            out[key] = int(v)
        elif key == "metrics":
            out[key] = _parse_metric_map(v)
        elif key == "folds":
            out[key] = [_parse_fold(f) for f in v]
        else:  # model, strategy, error
            out[key] = _decode(v)

    assert "model" in out, f"Missing model in {out}"
    return out


def parse_backtest_response(response: List[Any]) -> List[Dict[str, Any]]:
    """Parse the TS.BACKTEST response (array of flat per-model maps)."""
    if not response:
        return []
    return [parse_backtest_entry(entry) for entry in response]


def assert_finite(values: List[float]) -> None:
    for v in values:
        assert not math.isnan(v), f"unexpected NaN in {values}"
        assert not math.isinf(v), f"unexpected inf in {values}"


# ── test class ─────────────────────────────────────────────────────────────────

class TestBacktest(ValkeyTimeSeriesTestCaseBase):
    """Test TS.BACKTEST command with various options and edge cases."""

    # ══════════════════════════════════════════════════════════════════════
    # Basic functionality
    # ══════════════════════════════════════════════════════════════════════

    def test_basic_single_model(self):
        """Basic backtest with a single model produces per-fold + aggregate metrics."""
        key = "test:backtest:basic"
        create_linear_series(self.client, key, count=150)

        result = self.client.execute_command(
            "TS.BACKTEST", key, "-", "+",
            "MODELS", "Naive()", "HORIZON", "12"
        )

        parsed = parse_backtest_response(result)
        assert len(parsed) == 1
        entry = parsed[0]
        assert entry["model"] == "Naive()"
        assert entry["horizon"] == 12
        assert entry["strategy"] == "expanding"
        assert entry["n_folds"] >= 1
        assert "metrics" in entry
        assert "folds" in entry
        assert len(entry["folds"]) == entry["n_folds"]

    def test_multi_model(self):
        """Backtest across multiple models returns one entry per model."""
        key = "test:backtest:multi"
        create_linear_series(self.client, key, count=150)

        result = self.client.execute_command(
            "TS.BACKTEST", key, "-", "+",
            "MODELS", "Naive(), SES(alpha=0.3), SMA(5)", "HORIZON", "10"
        )

        parsed = parse_backtest_response(result)
        assert len(parsed) == 3
        names = {p["model"] for p in parsed}
        assert "Naive()" in names
        assert "SES(alpha=0.3)" in names
        assert "SMA(5)" in names
        for entry in parsed:
            assert "metrics" in entry
            assert entry["horizon"] == 10

    def test_model_order_preserved(self):
        """Response entries appear in the same order as the MODELS spec."""
        key = "test:backtest:order"
        create_linear_series(self.client, key, count=150)

        result = self.client.execute_command(
            "TS.BACKTEST", key, "-", "+",
            "MODELS", "SMA(3), Naive(), SES(alpha=0.5)", "HORIZON", "6"
        )
        parsed = parse_backtest_response(result)
        assert [p["model"] for p in parsed] == ["SMA(3)", "Naive()", "SES(alpha=0.5)"]

    def test_arima_model(self):
        """A heavier statistical model (ARIMA) backtests successfully."""
        key = "test:backtest:arima"
        create_linear_series(self.client, key, count=150)

        result = self.client.execute_command(
            "TS.BACKTEST", key, "-", "+",
            "MODELS", "ARIMA(2,1,0)", "HORIZON", "6", "N_FOLDS", "3"
        )
        parsed = parse_backtest_response(result)
        assert len(parsed) == 1
        entry = parsed[0]
        assert entry["model"] == "ARIMA(2,1,0)"
        assert entry["n_folds"] == 3
        assert entry["metrics"]["mae"] >= 0.0

    # ══════════════════════════════════════════════════════════════════════
    # Aggregated metrics
    # ══════════════════════════════════════════════════════════════════════

    def test_aggregate_metrics_fields(self):
        """Aggregated metrics map has exactly the expected keys, all finite."""
        key = "test:backtest:aggmetrics"
        create_linear_series(self.client, key, count=150)

        result = self.client.execute_command(
            "TS.BACKTEST", key, "-", "+",
            "MODELS", "Naive()", "HORIZON", "12", "N_FOLDS", "4"
        )
        entry = parse_backtest_response(result)[0]
        metrics = entry["metrics"]
        for field in AGG_METRIC_KEYS:
            assert field in metrics, f"missing aggregate metric '{field}'"

        assert metrics["mae"] >= 0.0
        assert metrics["rmse"] >= 0.0
        assert metrics["smape"] >= 0.0
        assert metrics["mae_std"] >= 0.0
        assert metrics["rmse_std"] >= 0.0
        # mape may be None (if any fold had zeros) or a non-negative float
        assert metrics["mape"] is None or metrics["mape"] >= 0.0

    def test_single_fold_std_is_zero(self):
        """With a single fold, the cross-fold std deviations are 0."""
        key = "test:backtest:onefold"
        create_linear_series(self.client, key, count=150)

        result = self.client.execute_command(
            "TS.BACKTEST", key, "-", "+",
            "MODELS", "Naive()", "HORIZON", "10", "N_FOLDS", "1"
        )
        entry = parse_backtest_response(result)[0]
        assert entry["n_folds"] == 1
        assert entry["metrics"]["mae_std"] == 0.0
        assert entry["metrics"]["rmse_std"] == 0.0

    # ══════════════════════════════════════════════════════════════════════
    # Per-fold structure
    # ══════════════════════════════════════════════════════════════════════

    def test_fold_metric_fields(self):
        """Each fold carries the full per-fold metric set."""
        key = "test:backtest:foldmetrics"
        create_linear_series(self.client, key, count=150)

        result = self.client.execute_command(
            "TS.BACKTEST", key, "-", "+",
            "MODELS", "Naive()", "HORIZON", "8", "N_FOLDS", "3"
        )
        entry = parse_backtest_response(result)[0]
        assert len(entry["folds"]) == 3
        for fold in entry["folds"]:
            for field in FOLD_METRIC_KEYS:
                assert field in fold["metrics"], f"fold missing metric '{field}'"

    def test_fold_boundaries_ordered(self):
        """Fold timestamp boundaries satisfy train_start <= train_end < test_start <= test_end."""
        key = "test:backtest:bounds"
        create_linear_series(self.client, key, count=150, step_ms=1000)

        result = self.client.execute_command(
            "TS.BACKTEST", key, "-", "+",
            "MODELS", "Naive()", "HORIZON", "10", "N_FOLDS", "4"
        )
        entry = parse_backtest_response(result)[0]
        for fold in entry["folds"]:
            assert fold["train_start"] <= fold["train_end"], fold
            assert fold["train_end"] < fold["test_start"], fold
            assert fold["test_start"] <= fold["test_end"], fold

    def test_folds_reported_as_timestamps(self):
        """Fold boundaries are real series timestamps, not raw indices."""
        # step_ms=60000 → timestamps are 1000, 61000, 121000, ... (much larger than any index)
        key = "test:backtest:tsbounds"
        create_linear_series(self.client, key, count=150, start_ms=1000, step_ms=60000)

        result = self.client.execute_command(
            "TS.BACKTEST", key, "-", "+",
            "MODELS", "Naive()", "HORIZON", "10", "N_FOLDS", "2"
        )
        entry = parse_backtest_response(result)[0]
        last_fold = entry["folds"][-1]
        # The last test window must end at the series' final timestamp.
        final_ts = 1000 + 149 * 60000
        assert last_fold["test_end"] == final_ts, \
            f"expected last test_end at series end {final_ts}, got {last_fold['test_end']}"

    # ══════════════════════════════════════════════════════════════════════
    # STRATEGY: expanding vs rolling
    # ══════════════════════════════════════════════════════════════════════

    def test_strategy_expanding_fixed_train_start(self):
        """EXPANDING keeps train_start fixed at the series start across folds."""
        key = "test:backtest:expanding"
        create_linear_series(self.client, key, count=150)

        result = self.client.execute_command(
            "TS.BACKTEST", key, "-", "+",
            "MODELS", "Naive()", "HORIZON", "10",
            "STRATEGY", "EXPANDING", "N_FOLDS", "4"
        )
        entry = parse_backtest_response(result)[0]
        assert entry["strategy"] == "expanding"
        train_starts = [f["train_start"] for f in entry["folds"]]
        assert len(set(train_starts)) == 1, \
            f"expanding folds should share one train_start, got {train_starts}"
        # And the training window should grow (train_end increases fold to fold).
        train_ends = [f["train_end"] for f in entry["folds"]]
        assert train_ends == sorted(train_ends)
        assert train_ends[0] < train_ends[-1]

    def test_strategy_rolling_slides_train_start(self):
        """ROLLING slides the training window forward (train_start advances)."""
        key = "test:backtest:rolling"
        create_linear_series(self.client, key, count=200)

        result = self.client.execute_command(
            "TS.BACKTEST", key, "-", "+",
            "MODELS", "Naive()", "HORIZON", "6",
            "STRATEGY", "ROLLING", "INITIAL_WINDOW", "48",
            "STEP", "12", "N_FOLDS", "4"
        )
        entry = parse_backtest_response(result)[0]
        assert entry["strategy"] == "rolling"
        train_starts = [f["train_start"] for f in entry["folds"]]
        # Rolling windows advance, so train_start is not constant.
        assert len(set(train_starts)) > 1, \
            f"rolling folds should have advancing train_start, got {train_starts}"
        assert train_starts == sorted(train_starts)

    # ══════════════════════════════════════════════════════════════════════
    # N_FOLDS / INITIAL_WINDOW / STEP
    # ══════════════════════════════════════════════════════════════════════

    def test_n_folds_respected(self):
        """With plentiful data, n_folds equals the requested count."""
        key = "test:backtest:nfolds"
        create_linear_series(self.client, key, count=200)

        result = self.client.execute_command(
            "TS.BACKTEST", key, "-", "+",
            "MODELS", "Naive()", "HORIZON", "10", "N_FOLDS", "5"
        )
        entry = parse_backtest_response(result)[0]
        assert entry["n_folds"] == 5
        assert len(entry["folds"]) == 5

    def test_n_folds_reduced_when_data_short(self):
        """N_FOLDS is silently reduced (not an error) when data can't fill all folds."""
        key = "test:backtest:reduce"
        # Just enough for a couple of folds with a large initial window.
        create_linear_series(self.client, key, count=80)

        result = self.client.execute_command(
            "TS.BACKTEST", key, "-", "+",
            "MODELS", "Naive()", "HORIZON", "10",
            "INITIAL_WINDOW", "50", "N_FOLDS", "20"
        )
        entry = parse_backtest_response(result)[0]
        assert 1 <= entry["n_folds"] < 20
        assert len(entry["folds"]) == entry["n_folds"]

    def test_step_overlapping_windows(self):
        """STEP smaller than HORIZON yields more, overlapping folds."""
        key = "test:backtest:step"
        create_linear_series(self.client, key, count=150)

        big_step = self.client.execute_command(
            "TS.BACKTEST", key, "-", "+",
            "MODELS", "Naive()", "HORIZON", "12", "STEP", "12", "N_FOLDS", "8"
        )
        small_step = self.client.execute_command(
            "TS.BACKTEST", key, "-", "+",
            "MODELS", "Naive()", "HORIZON", "12", "STEP", "3", "N_FOLDS", "8"
        )
        big = parse_backtest_response(big_step)[0]
        small = parse_backtest_response(small_step)[0]
        # A smaller step fits at least as many folds into the same data.
        assert small["n_folds"] >= big["n_folds"]

    def test_initial_window_affects_first_fold(self):
        """A larger INITIAL_WINDOW pushes the earliest fold's training window later."""
        key = "test:backtest:initwin"
        create_linear_series(self.client, key, count=200)

        small = parse_backtest_response(self.client.execute_command(
            "TS.BACKTEST", key, "-", "+",
            "MODELS", "Naive()", "HORIZON", "10",
            "INITIAL_WINDOW", "30", "N_FOLDS", "3"
        ))[0]
        large = parse_backtest_response(self.client.execute_command(
            "TS.BACKTEST", key, "-", "+",
            "MODELS", "Naive()", "HORIZON", "10",
            "INITIAL_WINDOW", "120", "N_FOLDS", "3"
        ))[0]
        # Both expanding from the same start, so the first fold's train_end reflects
        # the minimum window: a larger INITIAL_WINDOW => later (larger) first train_end.
        assert large["folds"][0]["train_end"] >= small["folds"][0]["train_end"]

    # ══════════════════════════════════════════════════════════════════════
    # GAP
    # ══════════════════════════════════════════════════════════════════════

    def test_gap_increases_train_test_separation(self):
        """A larger GAP widens the separation between train_end and test_start."""
        key = "test:backtest:gap"
        create_linear_series(self.client, key, count=200, step_ms=1000)

        no_gap = parse_backtest_response(self.client.execute_command(
            "TS.BACKTEST", key, "-", "+",
            "MODELS", "Naive()", "HORIZON", "10", "GAP", "0", "N_FOLDS", "2"
        ))[0]
        with_gap = parse_backtest_response(self.client.execute_command(
            "TS.BACKTEST", key, "-", "+",
            "MODELS", "Naive()", "HORIZON", "10", "GAP", "5", "N_FOLDS", "2"
        ))[0]

        sep_no_gap = no_gap["folds"][-1]["test_start"] - no_gap["folds"][-1]["train_end"]
        sep_gap = with_gap["folds"][-1]["test_start"] - with_gap["folds"][-1]["train_end"]
        assert sep_gap > sep_no_gap, \
            f"GAP should widen train->test separation: {sep_gap} vs {sep_no_gap}"

    # ══════════════════════════════════════════════════════════════════════
    # PURGE and EMBARGO
    # ══════════════════════════════════════════════════════════════════════

    def _last_fold_separation(self, key, **opts):
        """Run a 1-fold backtest and return (test_start - train_end) for the last fold."""
        args = ["TS.BACKTEST", key, "-", "+", "MODELS", "Naive()",
                "HORIZON", "10", "N_FOLDS", "1"]
        for k, v in opts.items():
            args.extend([k, str(v)])
        entry = parse_backtest_response(self.client.execute_command(*args))[0]
        fold = entry["folds"][-1]
        return fold["test_start"] - fold["train_end"]

    def test_purge_widens_separation_like_gap(self):
        """PURGE p, GAP p, and any split summing to p all widen train->test equally.

        In the current forecasting engine PURGE and GAP have the same effect on fold
        boundaries; the effective separation is (GAP + PURGE + 1) * step.
        """
        key = "test:backtest:purge_eq_gap"
        create_linear_series(self.client, key, count=200, step_ms=1000)

        gap_only = self._last_fold_separation(key, GAP=5, PURGE=0)
        purge_only = self._last_fold_separation(key, GAP=0, PURGE=5)
        both = self._last_fold_separation(key, GAP=3, PURGE=2)

        assert gap_only == purge_only == both, \
            f"GAP/PURGE should be interchangeable: {gap_only}, {purge_only}, {both}"
        # And it must exceed the zero-separation baseline.
        baseline = self._last_fold_separation(key, GAP=0, PURGE=0)
        assert gap_only > baseline

    def test_purge_shrinks_training_tail(self):
        """PURGE moves train_end earlier while leaving the test window anchored to the end."""
        key = "test:backtest:purge_tail"
        create_linear_series(self.client, key, count=200, step_ms=1000)

        no_purge = parse_backtest_response(self.client.execute_command(
            "TS.BACKTEST", key, "-", "+",
            "MODELS", "Naive()", "HORIZON", "10", "PURGE", "0", "N_FOLDS", "1"
        ))[0]
        purged = parse_backtest_response(self.client.execute_command(
            "TS.BACKTEST", key, "-", "+",
            "MODELS", "Naive()", "HORIZON", "10", "PURGE", "8", "N_FOLDS", "1"
        ))[0]

        # Same test window (both anchored to the series end)...
        assert purged["folds"][-1]["test_end"] == no_purge["folds"][-1]["test_end"]
        assert purged["folds"][-1]["test_start"] == no_purge["folds"][-1]["test_start"]
        # ...but a training tail trimmed earlier.
        assert purged["folds"][-1]["train_end"] < no_purge["folds"][-1]["train_end"]

    def test_embargo_can_reduce_folds(self):
        """A large EMBARGO drops folds whose training window it consumes (expanding)."""
        key = "test:backtest:embargo"
        create_linear_series(self.client, key, count=120)

        base = parse_backtest_response(self.client.execute_command(
            "TS.BACKTEST", key, "-", "+",
            "MODELS", "Naive()", "HORIZON", "10", "N_FOLDS", "6"
        ))[0]
        embargoed = parse_backtest_response(self.client.execute_command(
            "TS.BACKTEST", key, "-", "+",
            "MODELS", "Naive()", "HORIZON", "10", "N_FOLDS", "6", "EMBARGO", "30"
        ))[0]

        assert base["n_folds"] == 6
        assert 1 <= embargoed["n_folds"] < base["n_folds"], \
            f"EMBARGO should reduce fold count: {embargoed['n_folds']} vs {base['n_folds']}"
        assert len(embargoed["folds"]) == embargoed["n_folds"]

    def test_purge_embargo_default_off(self):
        """With PURGE/EMBARGO omitted, results match explicit zeros (defaults are 0)."""
        key = "test:backtest:pe_default"
        create_linear_series(self.client, key, count=150)

        default = parse_backtest_response(self.client.execute_command(
            "TS.BACKTEST", key, "-", "+",
            "MODELS", "Naive()", "HORIZON", "10", "N_FOLDS", "3"
        ))[0]
        explicit_zero = parse_backtest_response(self.client.execute_command(
            "TS.BACKTEST", key, "-", "+",
            "MODELS", "Naive()", "HORIZON", "10", "N_FOLDS", "3",
            "PURGE", "0", "EMBARGO", "0"
        ))[0]
        assert default["n_folds"] == explicit_zero["n_folds"]
        assert [f["train_end"] for f in default["folds"]] == \
               [f["train_end"] for f in explicit_zero["folds"]]

    # ══════════════════════════════════════════════════════════════════════
    # WITH_PREDICTIONS
    # ══════════════════════════════════════════════════════════════════════

    def test_with_predictions_present(self):
        """WITH_PREDICTIONS adds predictions/actuals arrays of length HORIZON per fold."""
        key = "test:backtest:preds"
        create_linear_series(self.client, key, count=150)

        result = self.client.execute_command(
            "TS.BACKTEST", key, "-", "+",
            "MODELS", "Naive()", "HORIZON", "7", "N_FOLDS", "3", "WITH_PREDICTIONS"
        )
        entry = parse_backtest_response(result)[0]
        for fold in entry["folds"]:
            assert "predictions" in fold
            assert "actuals" in fold
            assert len(fold["predictions"]) == 7
            assert len(fold["actuals"]) == 7
            assert_finite(fold["predictions"])
            assert_finite(fold["actuals"])

    def test_without_predictions_omitted(self):
        """predictions/actuals are omitted by default."""
        key = "test:backtest:nopreds"
        create_linear_series(self.client, key, count=150)

        result = self.client.execute_command(
            "TS.BACKTEST", key, "-", "+",
            "MODELS", "Naive()", "HORIZON", "7", "N_FOLDS", "3"
        )
        entry = parse_backtest_response(result)[0]
        for fold in entry["folds"]:
            assert "predictions" not in fold
            assert "actuals" not in fold

    def test_naive_predictions_are_constant(self):
        """Naive forecasts a flat line, so each fold's predictions are all equal."""
        key = "test:backtest:naiveconst"
        create_linear_series(self.client, key, count=150)

        result = self.client.execute_command(
            "TS.BACKTEST", key, "-", "+",
            "MODELS", "Naive()", "HORIZON", "5", "N_FOLDS", "2", "WITH_PREDICTIONS"
        )
        entry = parse_backtest_response(result)[0]
        for fold in entry["folds"]:
            preds = fold["predictions"]
            assert len(set(preds)) == 1, f"Naive predictions should be constant, got {preds}"

    # ══════════════════════════════════════════════════════════════════════
    # SEASONAL_PERIOD (MASE)
    # ══════════════════════════════════════════════════════════════════════

    def test_seasonal_period_null_mase_when_ge_horizon(self):
        """When SEASONAL_PERIOD >= HORIZON, per-fold MASE cannot be scaled → null."""
        key = "test:backtest:season_null"
        create_daily_seasonal_series(self.client, key, days=21)  # hourly, period 24

        result = self.client.execute_command(
            "TS.BACKTEST", key, "-", "+",
            "MODELS", "SeasonalNaive(24)", "HORIZON", "12",
            "SEASONAL_PERIOD", "24", "N_FOLDS", "3"
        )
        entry = parse_backtest_response(result)[0]
        for fold in entry["folds"]:
            assert fold["metrics"]["mase"] is None, \
                "MASE should be null when SEASONAL_PERIOD >= HORIZON"

    def test_omitted_seasonal_period_yields_mase(self):
        """Without SEASONAL_PERIOD, MASE falls back to lag-1 scaling and is defined."""
        key = "test:backtest:season_default"
        create_daily_seasonal_series(self.client, key, days=21)

        result = self.client.execute_command(
            "TS.BACKTEST", key, "-", "+",
            "MODELS", "SeasonalNaive(24)", "HORIZON", "12", "N_FOLDS", "3"
        )
        entry = parse_backtest_response(result)[0]
        for fold in entry["folds"]:
            assert fold["metrics"]["mase"] is not None, \
                "MASE should be defined (lag-1) when SEASONAL_PERIOD is omitted"

    # ══════════════════════════════════════════════════════════════════════
    # Per-model failure isolation
    # ══════════════════════════════════════════════════════════════════════

    def test_failure_isolation(self):
        """A model that fails reports `error`; other models still return metrics."""
        key = "test:backtest:isolation"
        create_linear_series(self.client, key, count=150)

        result = self.client.execute_command(
            "TS.BACKTEST", key, "-", "+",
            # SARIMA with seasonal period 200 needs far more data than we have here.
            "MODELS", "Naive(), SARIMA(1,1,1,1,1,1,200)", "HORIZON", "12", "N_FOLDS", "2"
        )
        parsed = parse_backtest_response(result)
        assert len(parsed) == 2
        by_name = {p["model"]: p for p in parsed}

        good = by_name["Naive()"]
        assert "metrics" in good
        assert "error" not in good

        bad = by_name["SARIMA(1,1,1,1,1,1,200)"]
        assert "error" in bad
        assert "metrics" not in bad
        assert "folds" not in bad
        assert isinstance(bad["error"], str) and len(bad["error"]) > 0

    def test_all_models_fail_still_returns_array(self):
        """If every model fails, the response is still an array of error entries."""
        key = "test:backtest:allfail"
        create_linear_series(self.client, key, count=120)

        result = self.client.execute_command(
            "TS.BACKTEST", key, "-", "+",
            "MODELS", "SARIMA(1,1,1,1,1,1,300), SARIMA(1,1,1,1,1,1,250)",
            "HORIZON", "10", "N_FOLDS", "2"
        )
        parsed = parse_backtest_response(result)
        assert len(parsed) == 2
        for entry in parsed:
            assert "error" in entry
            assert "metrics" not in entry

    # ══════════════════════════════════════════════════════════════════════
    # Case-insensitivity
    # ══════════════════════════════════════════════════════════════════════

    def test_keywords_case_insensitive(self):
        """Option keywords are case-insensitive."""
        key = "test:backtest:kwcase"
        create_linear_series(self.client, key, count=150)

        result = self.client.execute_command(
            "TS.BACKTEST", key, "-", "+",
            "models", "Naive()", "horizon", "8",
            "strategy", "expanding", "n_folds", "3"
        )
        parsed = parse_backtest_response(result)
        assert len(parsed) == 1
        assert parsed[0]["n_folds"] == 3

    def test_model_names_case_insensitive(self):
        """Model spec names are case-insensitive, echoed back canonicalized."""
        key = "test:backtest:modelcase"
        create_linear_series(self.client, key, count=150)

        result = self.client.execute_command(
            "TS.BACKTEST", key, "-", "+",
            "MODELS", "naive(), ses(alpha=0.3)", "HORIZON", "6", "N_FOLDS", "2"
        )
        names = {p["model"] for p in parse_backtest_response(result)}
        assert "Naive()" in names
        assert "SES(alpha=0.3)" in names

    def test_strategy_value_case_insensitive(self):
        """STRATEGY value is case-insensitive."""
        key = "test:backtest:stratcase"
        create_linear_series(self.client, key, count=150)

        result = self.client.execute_command(
            "TS.BACKTEST", key, "-", "+",
            "MODELS", "Naive()", "HORIZON", "8", "STRATEGY", "RoLLiNg",
            "INITIAL_WINDOW", "40", "N_FOLDS", "2"
        )
        entry = parse_backtest_response(result)[0]
        assert entry["strategy"] == "rolling"

    # ══════════════════════════════════════════════════════════════════════
    # Time-range filtering
    # ══════════════════════════════════════════════════════════════════════

    def test_time_range_filter(self):
        """Backtest respects an explicit from/to sub-range."""
        key = "test:backtest:range"
        values = [10.0 + 0.5 * i for i in range(300)]
        _add(self.client, key, 1000, values, 60000)

        from_ts = 1000 + 50 * 60000
        to_ts = 1000 + 199 * 60000

        result = self.client.execute_command(
            "TS.BACKTEST", key, str(from_ts), str(to_ts),
            "MODELS", "Naive()", "HORIZON", "10", "N_FOLDS", "3"
        )
        entry = parse_backtest_response(result)[0]
        # No fold may reach outside the requested window.
        for fold in entry["folds"]:
            assert fold["train_start"] >= from_ts
            assert fold["test_end"] <= to_ts

    # ══════════════════════════════════════════════════════════════════════
    # Response format validation
    # ══════════════════════════════════════════════════════════════════════

    def test_response_is_array_of_maps(self):
        """Top-level response is an array of even-length flat maps."""
        key = "test:backtest:format"
        create_linear_series(self.client, key, count=150)

        result = self.client.execute_command(
            "TS.BACKTEST", key, "-", "+",
            "MODELS", "Naive(), SES(alpha=0.3)", "HORIZON", "8", "N_FOLDS", "2"
        )
        assert isinstance(result, list)
        assert len(result) == 2
        for entry in result:
            assert isinstance(entry, list)
            assert len(entry) % 2 == 0, f"expected even-length flat map, got {len(entry)}"

    # ══════════════════════════════════════════════════════════════════════
    # Error handling
    # ══════════════════════════════════════════════════════════════════════

    def test_error_nonexistent_key(self):
        with pytest.raises(ResponseError, match="key does not exist"):
            self.client.execute_command(
                "TS.BACKTEST", "nope", "-", "+",
                "MODELS", "Naive()", "HORIZON", "10"
            )

    def test_error_missing_horizon(self):
        key = "test:backtest:err:no_horizon"
        create_linear_series(self.client, key, count=150)
        # Enough args to clear the arity gate, but no HORIZON keyword.
        with pytest.raises(ResponseError, match="HORIZON is required"):
            self.client.execute_command(
                "TS.BACKTEST", key, "-", "+", "MODELS", "Naive()", "N_FOLDS", "3"
            )

    def test_error_missing_models(self):
        key = "test:backtest:err:no_models"
        create_linear_series(self.client, key, count=150)
        # Enough args to clear the arity gate, but no MODELS keyword.
        with pytest.raises(ResponseError, match="MODELS must contain at least one"):
            self.client.execute_command(
                "TS.BACKTEST", key, "-", "+", "HORIZON", "10", "N_FOLDS", "3"
            )

    def test_error_invalid_model_spec(self):
        key = "test:backtest:err:bad_model"
        create_linear_series(self.client, key, count=150)
        with pytest.raises(ResponseError, match="MODELS"):
            self.client.execute_command(
                "TS.BACKTEST", key, "-", "+",
                "MODELS", "NotAModel()", "HORIZON", "10"
            )

    def test_error_bad_strategy(self):
        key = "test:backtest:err:strategy"
        create_linear_series(self.client, key, count=150)
        with pytest.raises(ResponseError, match="STRATEGY must be EXPANDING or ROLLING"):
            self.client.execute_command(
                "TS.BACKTEST", key, "-", "+",
                "MODELS", "Naive()", "HORIZON", "10", "STRATEGY", "sideways"
            )

    def test_error_unknown_argument(self):
        key = "test:backtest:err:unknown"
        create_linear_series(self.client, key, count=150)
        with pytest.raises(ResponseError, match="Unknown argument"):
            self.client.execute_command(
                "TS.BACKTEST", key, "-", "+",
                "MODELS", "Naive()", "HORIZON", "10", "BOGUS", "5"
            )

    def test_error_n_folds_zero(self):
        key = "test:backtest:err:nfolds0"
        create_linear_series(self.client, key, count=150)
        with pytest.raises(ResponseError, match="N_FOLDS must be greater than 0"):
            self.client.execute_command(
                "TS.BACKTEST", key, "-", "+",
                "MODELS", "Naive()", "HORIZON", "10", "N_FOLDS", "0"
            )

    def test_error_step_zero(self):
        key = "test:backtest:err:step0"
        create_linear_series(self.client, key, count=150)
        with pytest.raises(ResponseError, match="STEP must be greater than 0"):
            self.client.execute_command(
                "TS.BACKTEST", key, "-", "+",
                "MODELS", "Naive()", "HORIZON", "10", "STEP", "0"
            )

    def test_error_initial_window_zero(self):
        key = "test:backtest:err:initwin0"
        create_linear_series(self.client, key, count=150)
        with pytest.raises(ResponseError, match="INITIAL_WINDOW must be greater than 0"):
            self.client.execute_command(
                "TS.BACKTEST", key, "-", "+",
                "MODELS", "Naive()", "HORIZON", "10", "INITIAL_WINDOW", "0"
            )

    def test_error_purge_negative(self):
        key = "test:backtest:err:purge_neg"
        create_linear_series(self.client, key, count=150)
        with pytest.raises(ResponseError, match="PURGE must be non-negative"):
            self.client.execute_command(
                "TS.BACKTEST", key, "-", "+",
                "MODELS", "Naive()", "HORIZON", "10", "PURGE", "-1"
            )

    def test_error_embargo_negative(self):
        key = "test:backtest:err:embargo_neg"
        create_linear_series(self.client, key, count=150)
        with pytest.raises(ResponseError, match="EMBARGO must be non-negative"):
            self.client.execute_command(
                "TS.BACKTEST", key, "-", "+",
                "MODELS", "Naive()", "HORIZON", "10", "EMBARGO", "-5"
            )

    def test_error_horizon_zero(self):
        key = "test:backtest:err:horizon0"
        create_linear_series(self.client, key, count=150)
        with pytest.raises(ResponseError, match="horizon must be greater than 0"):
            self.client.execute_command(
                "TS.BACKTEST", key, "-", "+",
                "MODELS", "Naive()", "HORIZON", "0"
            )

    def test_error_insufficient_data(self):
        """A horizon larger than the whole series can't form a single fold."""
        key = "test:backtest:err:insufficient"
        create_linear_series(self.client, key, count=30)
        with pytest.raises(ResponseError, match="not enough data"):
            self.client.execute_command(
                "TS.BACKTEST", key, "-", "+",
                "MODELS", "Naive()", "HORIZON", "5000"
            )

    def test_error_wrong_arity(self):
        with pytest.raises(ResponseError):
            self.client.execute_command("TS.BACKTEST", "key", "-", "+")
