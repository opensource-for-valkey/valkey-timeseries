"""Tier A read-path matrix: TS.QUERYINDEX (test plan §6).

Covers the row's dimensions: the filter matrix, result ordering (normalized —
RTS makes no order guarantee), a no-match empty array, and the
requires-non-empty-matcher error. Runs under RESP2 and RESP3 (the reply is a
flat key list, a set in RESP3, both order-normalized by
compat_normalize._normalize_queryindex).

TS.QUERYINDEX shares the label-filter grammar with TS.MRANGE/TS.MGET, so the two
Prometheus-superset divergences carry over here (DIV-0019 negative-only matcher,
DIV-0020 bare metric-name matcher); they are pinned per-engine because an
accepted-input superset is non-registrable (plan §5.2).

Written clean-room from public RedisTimeSeries documentation and black-box
observation of the reference server. Do NOT consult RedisTimeSeries source or
test code (see tests/compat/README.md).
"""

import pytest
from valkey.exceptions import ResponseError

from compat_helpers import mk_label_universe


class TestFilterMatrix:
    def test_equality(self, diff):
        mk_label_universe(diff)
        diff("TS.QUERYINDEX", "metric=cpu")
        diff("TS.QUERYINDEX", "host=h1")

    def test_multiple_matchers(self, diff):
        mk_label_universe(diff)
        diff("TS.QUERYINDEX", "metric=cpu", "host=h1")
        diff("TS.QUERYINDEX", "metric=cpu", "region=us")

    def test_inequality_with_positive(self, diff):
        mk_label_universe(diff)
        diff("TS.QUERYINDEX", "metric=cpu", "region!=eu")

    def test_list_matchers(self, diff):
        mk_label_universe(diff)
        diff("TS.QUERYINDEX", "metric=(cpu,mem)")
        diff("TS.QUERYINDEX", "metric=(cpu,mem)", "host!=(h2)")
        diff("TS.QUERYINDEX", "host=(h1,h2)", "metric=cpu")

    def test_single_element_list(self, diff):
        mk_label_universe(diff)
        diff("TS.QUERYINDEX", "metric=(cpu)")

    def test_presence_and_absence(self, diff):
        mk_label_universe(diff)
        diff("TS.QUERYINDEX", "metric=(cpu,mem)", "region=")   # region absent
        diff("TS.QUERYINDEX", "metric=(cpu,mem)", "region!=")  # region present

    def test_no_match_returns_empty(self, diff):
        mk_label_universe(diff)
        diff("TS.QUERYINDEX", "metric=nope")
        diff("TS.QUERYINDEX", "metric=cpu", "host=nope")

    def test_match_all_of_a_label(self, diff):
        mk_label_universe(diff)
        diff("TS.QUERYINDEX", "host=h1")
        diff("TS.QUERYINDEX", "metric=(cpu,mem)")


class TestOrdering:
    def test_result_order_is_normalized(self, diff):
        """RTS makes no cross-key order guarantee; the harness sorts before
        comparing. A larger match set makes any ordering difference visible."""
        for i in range(8):
            diff(
                "TS.CREATE", f"q:ord:{i}",
                "LABELS", "grp", "ord", "n", str(i),
            )
        diff("TS.QUERYINDEX", "grp=ord")


class TestErrors:
    def test_requires_a_matcher(self, diff):
        mk_label_universe(diff)
        with pytest.raises(ResponseError):
            diff("TS.QUERYINDEX")

    def test_empty_matcher_rejected(self, diff):
        # Both reject an empty/unparseable matcher; the wording differs (RTS
        # "failed parsing labels" vs our "series selector is invalid"). Driven
        # per-engine so `diff` does not fail on the text.
        mk_label_universe(diff)
        with pytest.raises(ResponseError):
            diff.reference.execute_command("TS.QUERYINDEX", "")
        with pytest.raises(ResponseError):
            diff.subject.execute_command("TS.QUERYINDEX", "")

    def test_malformed_matcher_rejected(self, diff):
        mk_label_universe(diff)
        with pytest.raises(ResponseError):
            diff.reference.execute_command("TS.QUERYINDEX", "a b c")
        with pytest.raises(ResponseError):
            diff.subject.execute_command("TS.QUERYINDEX", "a b c")


class TestFilterSupersets:
    """DIV-0019/DIV-0020 carry over to TS.QUERYINDEX (shared selector parser)."""

    def test_negative_only_matcher_is_a_superset(self, diff):
        mk_label_universe(diff)
        with pytest.raises(ResponseError):
            diff.reference.execute_command("TS.QUERYINDEX", "metric!=cpu")
        diff.subject.execute_command("TS.QUERYINDEX", "metric!=cpu")

    def test_bare_metric_name_matcher_is_a_superset(self, diff):
        mk_label_universe(diff)
        with pytest.raises(ResponseError):
            diff.reference.execute_command("TS.QUERYINDEX", "not-a-matcher")
        diff.subject.execute_command("TS.QUERYINDEX", "not-a-matcher")
