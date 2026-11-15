"""Tier A read-path matrix: TS.QUERYINDEX (test plan §6).

Covers the row's dimensions: the filter matrix, result ordering (normalized —
RTS makes no order guarantee), a no-match empty array, and the
requires-non-empty-matcher error. Runs under RESP2 and RESP3 (the reply is a
flat key list, a set in RESP3, both order-normalized by
compat_normalize._normalize_queryindex).

TS.QUERYINDEX shares the label-filter grammar with TS.MRANGE/TS.MGET, so the
Prometheus-superset divergence carries over here (DIV-0020, bare metric-name
matcher); it is pinned per-engine because an accepted-input superset is
non-registrable (plan §5.2). The negative-only matcher that was DIV-0019 is
rejected by both engines and is diffed normally.

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
    def test_binary_key_names(self, diff):
        for key in (b"q:binary:\x00", b"q:binary:\xff", b"q:binary:\xfe",
                    b"q:binary:\xef\xbf\xbd", b"q:binary:ascii"):
            diff("TS.CREATE", key, "LABELS", "grp", "binary")
        diff("TS.QUERYINDEX", "grp=binary")

    def test_more_matches_than_one_key_batch(self, diff):
        for i in range(1100):
            diff("TS.CREATE", f"q:batch:{i}", "LABELS", "grp", "batch")
        diff("TS.QUERYINDEX", "grp=batch")

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
    """DIV-0020 carries over to TS.QUERYINDEX (shared selector parser), as does the
    boundedness rule that retired DIV-0019."""

    def test_negative_only_matcher_rejected(self, diff):
        mk_label_universe(diff)
        with pytest.raises(ResponseError, match="please provide at least one matcher"):
            diff("TS.QUERYINDEX", "metric!=cpu")

    def test_bare_metric_name_matcher_is_a_superset(self, diff):
        mk_label_universe(diff)
        with pytest.raises(ResponseError):
            diff.reference.execute_command("TS.QUERYINDEX", "not-a-matcher")
        diff.subject.execute_command("TS.QUERYINDEX", "not-a-matcher")


def _mk_raw_value_universe(diff):
    """Series whose label values are not identifier- or number-shaped. Every one carries
    `m=raw`, the positive matcher the queries below anchor on."""
    for key, *labels in [
        ("r:hash", "color", "#ff0000", "tag", "foo#bar"),
        ("r:text", "host", "10.0.0.1", "ver", "1.2.3", "id", "3f2a-9c", "city", "Zürich",
         "path", "/var/log", "email", "a@b.com", "state", "OR", "sp", "a b"),
        ("r:quoted", "host", '"10.0.0.1"'),
        ("r:space", "t", " humidity"),
        ("r:plain", "t", "humidity"),
        ("r:names", "host ", "v", '"q"', "1", "über", "1"),
    ]:
        diff("TS.CREATE", key, "LABELS", "m", "raw", *labels)


class TestRawFilterValues:
    """Basic FILTER matchers are read as raw text on both engines: label and value are the
    text around the first operator, untrimmed, with quotes, `#`, spaces and non-ASCII text
    as ordinary characters. Reference behavior observed black-box (2026-11-14)."""

    @pytest.mark.parametrize("matcher", [
        "color=#ff0000", "tag=foo#bar", "color=#",
        "host=10.0.0.1", "ver=1.2.3", "id=3f2a-9c", "city=Zürich", "path=/var/log",
        "email=a@b.com", "state=OR", "state=or", "sp=a b",
        'host="10.0.0.1"', "host='10.0.0.1'",
        "t=(humidity, humidity)", "t=( humidity)", "t=(humidity,x)", "host=(10.0.0.1,x)",
        "host!=10.0.0.1", "t!=(humidity)",
        "host =v", "host = v", '"q"=1', "q=1", "über=1",
    ])
    def test_value_is_raw_text(self, diff, matcher):
        _mk_raw_value_universe(diff)
        diff("TS.QUERYINDEX", "m=raw", matcher)

    @pytest.mark.parametrize("matcher", ["e=(p", "e=(p,q)z", "e=(p),", "t=(,a)", "t=(a,)"])
    def test_malformed_list_is_rejected_by_both(self, diff, matcher):
        # Both reject; the wording differs, so per engine (see test_empty_matcher_rejected).
        _mk_raw_value_universe(diff)
        with pytest.raises(ResponseError):
            diff.reference.execute_command("TS.QUERYINDEX", "m=raw", matcher)
        with pytest.raises(ResponseError):
            diff.subject.execute_command("TS.QUERYINDEX", "m=raw", matcher)


class TestRawFilterValueDivergences:
    """DIV-0065: inputs RTS accepts with no consistent meaning, which we reject."""

    @pytest.mark.parametrize("matcher", [
        "e=x=y", "e==b", "e=p,q", "e=p)", "e=a(b", "t=()", "=1", "status=999 or env=prod",
    ])
    def test_ambiguous_value_is_rejected_only_here(self, diff, matcher):
        _mk_raw_value_universe(diff)
        assert isinstance(diff.reference.execute_command("TS.QUERYINDEX", "m=raw", matcher), (list, set))
        with pytest.raises(ResponseError):
            diff.subject.execute_command("TS.QUERYINDEX", "m=raw", matcher)
