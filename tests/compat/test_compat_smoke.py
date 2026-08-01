"""Smoke subset of the Tier A differential harness (test plan §8, PR gate).

Fixed scenarios over TS.CREATE / TS.ADD / TS.RANGE / TS.MRANGE / TS.INFO plus
error-condition parity. Every command issued through the `diff` fixture is sent
to both engines and its reply diffed automatically; a test body is just the
command sequence.

Written clean-room from public RedisTimeSeries documentation and black-box
observation of the reference server. Do NOT consult RedisTimeSeries source or
test code (see tests/compat/README.md).
"""

import os

import pytest
import yaml
from valkey.exceptions import ResponseError


def _mk(diff, key, *extra):
    """Create a series with deterministic explicit options.

    CHUNK_SIZE/ENCODING/DUPLICATE_POLICY are pinned so that engine-default
    differences (a §7.1 config-parity concern) don't leak into every smoke test.
    """
    return diff(
        "TS.CREATE", key,
        "RETENTION", 0,
        "CHUNK_SIZE", 4096,
        "ENCODING", "COMPRESSED",
        "DUPLICATE_POLICY", "BLOCK",
        *extra,
    )


class TestCreateInfo:
    def test_create_reports_configured_properties(self, diff):
        _mk(diff, "smoke:create", "LABELS", "sensor", "s1", "area", "us-east")
        diff("TS.INFO", "smoke:create")

    def test_create_duplicate_key_errors(self, diff):
        _mk(diff, "smoke:dup")
        with pytest.raises(ResponseError):
            diff("TS.CREATE", "smoke:dup")

    def test_alter_retention_reflected_in_info(self, diff):
        _mk(diff, "smoke:alter")
        diff("TS.ALTER", "smoke:alter", "RETENTION", 60000)
        diff("TS.INFO", "smoke:alter")


class TestAddRange:
    def test_add_and_range_roundtrip(self, diff):
        _mk(diff, "smoke:ar")
        diff("TS.ADD", "smoke:ar", 0, 1.5)
        diff("TS.ADD", "smoke:ar", 1000, -2.25)
        diff("TS.ADD", "smoke:ar", 2000, 0)
        diff("TS.ADD", "smoke:ar", 5000, 123456789.000001)
        diff("TS.RANGE", "smoke:ar", "-", "+")
        diff("TS.RANGE", "smoke:ar", 0, 2000)
        diff("TS.RANGE", "smoke:ar", "-", "+", "COUNT", 2)
        diff("TS.REVRANGE", "smoke:ar", "-", "+")

    def test_out_of_order_insert(self, diff):
        _mk(diff, "smoke:ooo")
        diff("TS.ADD", "smoke:ooo", 5000, 5.0)
        diff("TS.ADD", "smoke:ooo", 1000, 1.0)
        diff("TS.ADD", "smoke:ooo", 3000, 3.0)
        diff("TS.RANGE", "smoke:ooo", "-", "+")

    def test_auto_create_on_add(self, diff):
        diff("TS.ADD", "smoke:auto", 100, 42.0)
        diff("TS.RANGE", "smoke:auto", "-", "+")
        diff("TS.GET", "smoke:auto")

    def test_range_aggregation(self, diff):
        _mk(diff, "smoke:agg")
        for ts, value in [(0, 1.0), (500, 2.0), (1000, 3.0), (1500, 4.0), (2500, 10.5)]:
            diff("TS.ADD", "smoke:agg", ts, value)
        for agg in ("avg", "sum", "min", "max", "count", "first", "last", "range"):
            diff("TS.RANGE", "smoke:agg", "-", "+", "AGGREGATION", agg, 1000)
        diff("TS.RANGE", "smoke:agg", "-", "+", "ALIGN", 250, "AGGREGATION", "sum", 1000)

    def test_incrby_decrby_accumulation(self, diff):
        diff("TS.INCRBY", "smoke:ctr", 5, "TIMESTAMP", 100)
        diff("TS.INCRBY", "smoke:ctr", 5, "TIMESTAMP", 100)
        diff("TS.INCRBY", "smoke:ctr", 2.5, "TIMESTAMP", 200)
        diff("TS.DECRBY", "smoke:ctr", 3, "TIMESTAMP", 300)
        diff("TS.RANGE", "smoke:ctr", "-", "+")

    @pytest.mark.parametrize("command", ["TS.INCRBY", "TS.DECRBY"])
    def test_negative_timestamp_is_rejected(self, diff, command):
        """DIV-0032/DIV-0033: we validate where RTS does not.

        The reference accepts this and stores a sample at the negative timestamp, so the
        key is used once and never read back — the two engines hold different state
        afterwards by design. Going through `diff` (rather than asserting on the subject
        alone) is what records the divergence and keeps the registry entries live.
        """
        key = f"smoke:negts:{command}"
        with pytest.raises(ResponseError, match="must be a nonnegative integer"):
            diff(command, key, 1, "TIMESTAMP", -1000)

    def test_del_boundary_inclusivity(self, diff):
        _mk(diff, "smoke:del")
        for ts in (100, 200, 300, 400, 500):
            diff("TS.ADD", "smoke:del", ts, ts / 10.0)
        diff("TS.DEL", "smoke:del", 200, 400)
        diff("TS.RANGE", "smoke:del", "-", "+")


class TestMadd:
    def test_madd_multi_key(self, diff):
        _mk(diff, "smoke:m1")
        _mk(diff, "smoke:m2")
        diff("TS.MADD", "smoke:m1", 100, 1.0, "smoke:m2", 100, 2.0, "smoke:m1", 200, 3.0)
        diff("TS.RANGE", "smoke:m1", "-", "+")
        diff("TS.RANGE", "smoke:m2", "-", "+")

    def test_madd_partial_failure(self, diff):
        # BLOCK policy: the duplicate-timestamp item must fail per-item while the
        # others succeed; per-item error placement in the reply array must match.
        _mk(diff, "smoke:mp")
        diff("TS.ADD", "smoke:mp", 100, 1.0)
        diff("TS.MADD", "smoke:mp", 50, 0.5, "smoke:mp", 100, 9.9, "smoke:mp", 200, 2.0)
        diff("TS.RANGE", "smoke:mp", "-", "+")


class TestMultiSeries:
    def _fixture_series(self, diff):
        _mk(diff, "smoke:cpu1", "LABELS", "metric", "cpu", "area", "us", "host", "h1")
        _mk(diff, "smoke:cpu2", "LABELS", "metric", "cpu", "area", "eu", "host", "h2")
        _mk(diff, "smoke:mem1", "LABELS", "metric", "mem", "area", "us", "host", "h1")
        diff("TS.ADD", "smoke:cpu1", 100, 10.0)
        diff("TS.ADD", "smoke:cpu1", 200, 20.0)
        diff("TS.ADD", "smoke:cpu2", 100, 30.0)
        diff("TS.ADD", "smoke:mem1", 100, 512.0)

    def test_mrange_filter_and_withlabels(self, diff):
        self._fixture_series(diff)
        diff("TS.MRANGE", "-", "+", "FILTER", "metric=cpu")
        diff("TS.MRANGE", "-", "+", "WITHLABELS", "FILTER", "metric=cpu")
        diff("TS.MRANGE", "-", "+", "SELECTED_LABELS", "area", "missing", "FILTER", "metric=cpu")
        diff("TS.MRANGE", "-", "+", "FILTER", "metric=cpu", "area!=eu")
        diff("TS.MRANGE", "-", "+", "FILTER", "metric=(cpu,mem)", "host=h1")
        diff("TS.MREVRANGE", "-", "+", "FILTER", "metric=cpu")

    def test_mrange_groupby_reduce(self, diff):
        self._fixture_series(diff)
        diff("TS.MRANGE", "-", "+", "FILTER", "metric=cpu", "GROUPBY", "metric", "REDUCE", "sum")

    def test_get_and_mget(self, diff):
        self._fixture_series(diff)
        diff("TS.GET", "smoke:cpu1")
        diff("TS.MGET", "FILTER", "metric=cpu")
        diff("TS.MGET", "WITHLABELS", "FILTER", "area=us")

    def test_get_empty_series(self, diff):
        _mk(diff, "smoke:empty")
        diff("TS.GET", "smoke:empty")

    def test_queryindex(self, diff):
        self._fixture_series(diff)
        diff("TS.QUERYINDEX", "metric=cpu")
        diff("TS.QUERYINDEX", "metric=cpu", "area=us")
        diff("TS.QUERYINDEX", "metric=nope")


class TestCompactionBasics:
    def test_createrule_reflected_in_info(self, diff):
        _mk(diff, "smoke:src")
        _mk(diff, "smoke:dst")
        diff("TS.CREATERULE", "smoke:src", "smoke:dst", "AGGREGATION", "avg", 1000)
        diff("TS.INFO", "smoke:src")
        diff("TS.INFO", "smoke:dst")
        diff("TS.DELETERULE", "smoke:src", "smoke:dst")
        diff("TS.INFO", "smoke:src")


class TestErrorConditions:
    def test_range_missing_key(self, diff):
        with pytest.raises(ResponseError):
            diff("TS.RANGE", "smoke:nope", "-", "+")

    def test_wrongtype(self, diff):
        diff("SET", "smoke:str", "hello")
        with pytest.raises(ResponseError):
            diff("TS.ADD", "smoke:str", 100, 1.0)
        with pytest.raises(ResponseError):
            diff("TS.RANGE", "smoke:str", "-", "+")

    def test_add_invalid_value(self, diff):
        _mk(diff, "smoke:bad")
        with pytest.raises(ResponseError):
            diff("TS.ADD", "smoke:bad", 100, "not-a-number")

    def test_duplicate_block_policy(self, diff):
        _mk(diff, "smoke:blk")
        diff("TS.ADD", "smoke:blk", 100, 1.0)
        with pytest.raises(ResponseError):
            diff("TS.ADD", "smoke:blk", 100, 2.0)

    def test_queryindex_requires_matcher(self, diff):
        with pytest.raises(ResponseError):
            diff("TS.QUERYINDEX")


class TestInfoBaseline:
    """Validates the frozen RTS 8.10 TS.INFO baseline (plan §5.1 rule 3).

    Runs against the REFERENCE server only: if this fails, either the frozen
    file is wrong or the reference image drifted on a digest bump.
    """

    BASELINE_PATH = os.path.join(os.path.dirname(__file__), "info-fields-8.10.yml")

    def _info_field_names(self, client, key):
        reply = client.execute_command("TS.INFO", key)
        if isinstance(reply, dict):  # RESP3
            keys = reply.keys()
        else:  # RESP2 flattened name/value array
            keys = reply[::2]
        return {k.decode() if isinstance(k, bytes) else str(k) for k in keys}

    def test_reference_matches_frozen_baseline(self, diff):
        with open(self.BASELINE_PATH) as f:
            baseline = yaml.safe_load(f)
        expected = set(baseline["fields"])

        diff.reference.execute_command(
            "TS.CREATE", "smoke:baseline", "LABELS", "a", "b"
        )
        diff.reference.execute_command("TS.ADD", "smoke:baseline", 100, 1.0)
        actual = self._info_field_names(diff.reference, "smoke:baseline")

        assert actual == expected, (
            "RTS 8.10 TS.INFO field set drifted from tests/compat/info-fields-8.10.yml.\n"
            f"  missing from reference: {sorted(expected - actual)}\n"
            f"  new in reference:       {sorted(actual - expected)}\n"
            "Update the frozen baseline in the same reviewed change as the image bump."
        )

    def test_subject_covers_baseline(self, diff):
        """Every frozen RTS field must be present in the subject's TS.INFO."""
        with open(self.BASELINE_PATH) as f:
            baseline = yaml.safe_load(f)
        expected = set(baseline["fields"])

        diff.subject.execute_command(
            "TS.CREATE", "smoke:baseline", "LABELS", "a", "b"
        )
        diff.subject.execute_command("TS.ADD", "smoke:baseline", 100, 1.0)
        actual = self._info_field_names(diff.subject, "smoke:baseline")

        missing = sorted(expected - actual)
        assert not missing, (
            f"subject TS.INFO is missing frozen RTS 8.10 fields: {missing}"
        )
