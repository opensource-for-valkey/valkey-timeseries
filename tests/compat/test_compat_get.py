"""Tier A read-path matrix: TS.GET / TS.MGET (test plan §6).

TS.GET covers the empty-series reply shape and LATEST semantics on a compaction
target with an open bucket. TS.MGET covers the filter matrix, WITHLABELS vs
SELECTED_LABELS (missing label → nil), and empty-series entries. Both add the
common dimensions: missing key, WRONGTYPE, arg parsing, RESP2/RESP3.

TS.MGET shares the filter grammar with TS.MRANGE, so the deep filter-language
coverage lives in test_compat_mrange.py; here it is exercised enough to confirm
MGET wires the same parser (including the Prometheus-superset divergence
DIV-0020 and the boundedness rule that retired DIV-0019).

Cross-series replies are order-normalized (sort by key); within a series the
single sample must match exactly (compat_normalize._normalize_multi_series).

Written clean-room from public RedisTimeSeries documentation and black-box
observation of the reference server. Do NOT consult RedisTimeSeries source or
test code (see tests/compat/README.md).
"""

import pytest
from valkey.exceptions import ResponseError

from compat_helpers import mk_label_universe, mk_populated, mk_series


class TestGet:
    def test_get_last_sample(self, diff):
        mk_populated(diff, "get:v", [(100, 10.0), (200, 20.5), (300, -3.0)])
        diff("TS.GET", "get:v")

    def test_get_single_sample(self, diff):
        mk_populated(diff, "get:one", [(1000, 42.0)])
        diff("TS.GET", "get:one")

    def test_get_empty_series(self, diff):
        mk_series(diff, "get:empty")
        diff("TS.GET", "get:empty")

    def test_get_special_values(self, diff):
        mk_populated(diff, "get:f", [(100, 0.0), (200, -0.0), (300, 1e308)])
        diff("TS.GET", "get:f")

    def test_get_missing_key(self, diff):
        with pytest.raises(ResponseError):
            diff("TS.GET", "get:nonexistent")

    def test_get_wrongtype(self, diff):
        diff("SET", "get:string", "hello")
        with pytest.raises(ResponseError):
            diff("TS.GET", "get:string")

    def test_get_arity(self, diff):
        mk_populated(diff, "get:arity", [(100, 1.0)])
        with pytest.raises(ResponseError):
            diff("TS.GET")
        with pytest.raises(ResponseError):
            diff("TS.GET", "get:arity", "LATEST", "extra")

    def test_get_invalid_third_arg(self, diff):
        mk_populated(diff, "get:bad3", [(100, 1.0)])
        with pytest.raises(ResponseError):
            diff("TS.GET", "get:bad3", "BOGUS")

    def test_get_latest_is_case_insensitive(self, diff):
        mk_populated(diff, "get:ci", [(100, 1.0), (200, 2.0)])
        diff("TS.GET", "get:ci", "latest")
        diff("TS.GET", "get:ci", "LATEST")


class TestGetLatest:
    """LATEST on a compaction target with an open (unfinalized) bucket."""

    def _rule(self, diff):
        mk_series(diff, "get:src")
        mk_series(diff, "get:dst")
        diff("TS.CREATERULE", "get:src", "get:dst", "AGGREGATION", "sum", 1000)
        for ts, value in [(0, 1.0), (500, 2.0), (1000, 4.0), (1500, 8.0), (2500, 16.0)]:
            diff("TS.ADD", "get:src", ts, value)

    def test_get_on_compaction_target(self, diff):
        self._rule(diff)
        # Without LATEST: the last finalized downstream bucket.
        diff("TS.GET", "get:dst")

    def test_get_latest_exposes_open_bucket(self, diff):
        self._rule(diff)
        diff("TS.GET", "get:dst", "LATEST")

    def test_get_latest_on_plain_series(self, diff):
        mk_populated(diff, "get:plain", [(100, 1.0), (200, 2.0)])
        diff("TS.GET", "get:plain", "LATEST")

    def test_get_latest_empty_target(self, diff):
        mk_series(diff, "get:esrc")
        mk_series(diff, "get:edst")
        diff("TS.CREATERULE", "get:esrc", "get:edst", "AGGREGATION", "avg", 1000)
        diff("TS.GET", "get:edst")
        diff("TS.GET", "get:edst", "LATEST")


class TestMget:
    def test_mget_basic(self, diff):
        mk_label_universe(diff)
        diff("TS.MGET", "FILTER", "metric=cpu")

    def test_mget_empty_series_entry(self, diff):
        mk_populated(diff, "mg:1", [(100, 1.0)], "LABELS", "k", "v")
        mk_series(diff, "mg:2", "LABELS", "k", "v")
        diff("TS.MGET", "FILTER", "k=v")

    def test_mget_no_match(self, diff):
        mk_label_universe(diff)
        diff("TS.MGET", "FILTER", "metric=nope")

    def test_mget_filter_operators(self, diff):
        mk_label_universe(diff)
        diff("TS.MGET", "FILTER", "metric=(cpu,mem)")
        diff("TS.MGET", "FILTER", "metric=cpu", "region!=eu")
        diff("TS.MGET", "FILTER", "metric=(cpu,mem)", "host=h1")

    def test_mget_withlabels(self, diff):
        mk_label_universe(diff)
        diff("TS.MGET", "WITHLABELS", "FILTER", "metric=cpu")

    def test_mget_selected_labels_present(self, diff):
        mk_label_universe(diff)
        diff("TS.MGET", "SELECTED_LABELS", "host", "region", "FILTER", "metric=cpu")

    def test_mget_selected_labels_missing_becomes_nil(self, diff):
        # u:mem:1 lacks `region`; the slot must come back nil.
        mk_label_universe(diff)
        diff("TS.MGET", "SELECTED_LABELS", "host", "region", "FILTER", "metric=mem")

    def test_mget_selected_labels_all_missing(self, diff):
        mk_label_universe(diff)
        diff("TS.MGET", "SELECTED_LABELS", "nope", "FILTER", "metric=cpu")

    def test_mget_latest_on_compaction_targets(self, diff):
        mk_series(diff, "mg:src", "LABELS", "role", "src")
        mk_series(diff, "mg:dst", "LABELS", "role", "dst")
        diff("TS.CREATERULE", "mg:src", "mg:dst", "AGGREGATION", "sum", 1000)
        for ts, value in [(0, 1.0), (500, 2.0), (1000, 4.0), (2500, 8.0)]:
            diff("TS.ADD", "mg:src", ts, value)
        diff("TS.MGET", "FILTER", "role=dst")
        diff("TS.MGET", "LATEST", "FILTER", "role=dst")


class TestMgetArgParsing:
    def test_mget_missing_filter_rejected(self, diff):
        mk_label_universe(diff)
        with pytest.raises(ResponseError):
            diff("TS.MGET")

    def test_mget_empty_filter_rejected(self, diff):
        mk_label_universe(diff)
        with pytest.raises(ResponseError):
            diff("TS.MGET", "FILTER")

    def test_mget_withlabels_and_selected_labels_together_rejected(self, diff):
        mk_label_universe(diff)
        with pytest.raises(ResponseError):
            diff("TS.MGET", "WITHLABELS", "SELECTED_LABELS", "host", "FILTER", "metric=cpu")

    def test_mget_selected_labels_requires_a_label(self, diff):
        mk_label_universe(diff)
        with pytest.raises(ResponseError):
            diff("TS.MGET", "SELECTED_LABELS", "FILTER", "metric=cpu")

    def test_mget_malformed_matcher_rejected(self, diff):
        # Both reject an unparseable matcher; the wording differs (RTS "failed
        # parsing labels" vs our "series selector is invalid"). Same condition,
        # different words — driven per-engine so `diff` doesn't fail on the text.
        mk_label_universe(diff)
        with pytest.raises(ResponseError):
            diff.reference.execute_command("TS.MGET", "FILTER", "not a matcher with spaces")
        with pytest.raises(ResponseError):
            diff.subject.execute_command("TS.MGET", "FILTER", "not a matcher with spaces")


class TestMgetFilterSupersets:
    """The Prometheus-superset FILTER divergence (DIV-0020) applies to TS.MGET too,
    since it shares the selector parser, and is pinned per-engine because an
    accepted-input superset is non-registrable (plan §5.2). The negative-only case
    below is no longer a superset — both engines reject it — so it goes through `diff`."""

    def test_negative_only_matcher_rejected(self, diff):
        mk_label_universe(diff)
        with pytest.raises(ResponseError, match="please provide at least one matcher"):
            diff("TS.MGET", "FILTER", "metric!=cpu")

    def test_bare_metric_name_matcher_is_a_superset(self, diff):
        mk_label_universe(diff)
        with pytest.raises(ResponseError):
            diff.reference.execute_command("TS.MGET", "FILTER", "not-a-matcher")
        diff.subject.execute_command("TS.MGET", "FILTER", "not-a-matcher")
