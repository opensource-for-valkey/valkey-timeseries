"""Tier A read-path matrix: TS.QUERYLABELS (new in RTS 8.10).

Covers the row's dimensions: the LABELS/VALUES subtype switch, the FILTER matrix
(with and without FILTER — no FILTER means every indexed series), a label that no
matching series carries (empty, not an error), a no-match empty result, and the
argument/arity errors. Runs under RESP2 and RESP3 (the reply is an array in RESP2
and a set in RESP3; both order-normalized by compat_normalize).

TS.QUERYLABELS shares the label-filter grammar with TS.MRANGE/TS.MGET/TS.QUERYINDEX,
so the Prometheus-superset divergence carries over here (DIV-0020, bare metric-name
matcher) and the malformed-matcher wording differs per engine ("failed parsing
labels" vs the parser's detailed diagnostic); both are pinned per-engine below.

Written clean-room from public RedisTimeSeries documentation and black-box
observation of the reference server. Do NOT consult RedisTimeSeries source or
test code (see tests/compat/README.md).
"""

import pytest
from valkey.exceptions import ResponseError

from compat_helpers import mk_label_universe


class TestSubtype:
    def test_labels_no_filter(self, diff):
        mk_label_universe(diff)
        diff("TS.QUERYLABELS", "LABELS")

    def test_values_no_filter(self, diff):
        mk_label_universe(diff)
        diff("TS.QUERYLABELS", "VALUES", "host")
        diff("TS.QUERYLABELS", "VALUES", "metric")

    def test_labels_with_filter(self, diff):
        mk_label_universe(diff)
        diff("TS.QUERYLABELS", "LABELS", "FILTER", "metric=cpu")
        diff("TS.QUERYLABELS", "LABELS", "FILTER", "metric=mem")

    def test_values_with_filter(self, diff):
        mk_label_universe(diff)
        # mem:1 has no region label, exercising the absent-label path.
        diff("TS.QUERYLABELS", "VALUES", "region", "FILTER", "metric=mem")
        diff("TS.QUERYLABELS", "VALUES", "host", "FILTER", "metric=cpu")
        diff("TS.QUERYLABELS", "VALUES", "region", "FILTER", "metric=cpu")

    def test_labels_with_compound_filter(self, diff):
        mk_label_universe(diff)
        diff("TS.QUERYLABELS", "LABELS", "FILTER", "metric=cpu", "host=h1")


class TestEmpty:
    def test_values_label_absent_returns_empty(self, diff):
        mk_label_universe(diff)
        diff("TS.QUERYLABELS", "VALUES", "nope")

    def test_no_match_returns_empty(self, diff):
        mk_label_universe(diff)
        diff("TS.QUERYLABELS", "LABELS", "FILTER", "metric=nope")
        diff("TS.QUERYLABELS", "VALUES", "host", "FILTER", "metric=nope")


class TestErrors:
    def test_unknown_subtype(self, diff):
        mk_label_universe(diff)
        with pytest.raises(ResponseError, match="unknown subtype"):
            diff("TS.QUERYLABELS", "FOO")

    def test_missing_label_after_values(self, diff):
        mk_label_universe(diff)
        with pytest.raises(ResponseError, match="wrong number of arguments"):
            diff("TS.QUERYLABELS", "VALUES")

    def test_unknown_argument_after_subtype(self, diff):
        mk_label_universe(diff)
        with pytest.raises(ResponseError, match="expected FILTER"):
            diff("TS.QUERYLABELS", "LABELS", "FOO")

    def test_filter_with_no_expressions(self, diff):
        mk_label_universe(diff)
        with pytest.raises(ResponseError, match="no filter expressions"):
            diff("TS.QUERYLABELS", "LABELS", "FILTER")

    def test_filter_without_bounded_matcher(self, diff):
        mk_label_universe(diff)
        with pytest.raises(ResponseError, match="please provide at least one matcher"):
            diff("TS.QUERYLABELS", "LABELS", "FILTER", "metric!=cpu")


class TestFilterSupersets:
    """DIV-0020 carries over to TS.QUERYLABELS (shared selector parser), as does the
    boundedness rule that retired DIV-0019."""

    def test_malformed_matcher_rejected(self, diff):
        # Wording differs per engine (RTS "failed parsing labels" vs our parser's
        # detailed diagnostic); pinned per-engine so `diff` does not run at all.
        mk_label_universe(diff)
        with pytest.raises(ResponseError):
            diff.reference.execute_command("TS.QUERYLABELS", "LABELS", "FILTER", "a b c")
        with pytest.raises(ResponseError):
            diff.subject.execute_command("TS.QUERYLABELS", "LABELS", "FILTER", "a b c")

    def test_bare_metric_name_matcher_is_a_superset(self, diff):
        mk_label_universe(diff)
        with pytest.raises(ResponseError):
            diff.reference.execute_command("TS.QUERYLABELS", "LABELS", "FILTER", "not-a-matcher")
        diff.subject.execute_command("TS.QUERYLABELS", "LABELS", "FILTER", "not-a-matcher")
