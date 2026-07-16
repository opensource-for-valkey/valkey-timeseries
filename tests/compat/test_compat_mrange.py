"""Tier A read-path matrix: TS.MRANGE / TS.MREVRANGE (test plan §6).

Covers the row's specific dimensions: the full filter-language matrix
(`=`, `!=`, `=(a,b)`, `!=(a,b)`, presence/absence), WITHLABELS vs
SELECTED_LABELS (missing label → nil), GROUPBY/REDUCE across every valid
reducer (incl. empty groups and label-absent series), the RANGE per-series
options applied through the multi-series entry point, and — the reason this
command gets its own module — the reply-nesting differences between RESP2 and
RESP3. Each test runs under both protocols via the `protocol` fixture.

The cross-series reply is order-normalized (sort by key); within a series the
sample order must match exactly (compat_normalize._normalize_multi_series).

Written clean-room from public RedisTimeSeries documentation and black-box
observation of the reference server. Do NOT consult RedisTimeSeries source or
test code (see tests/compat/README.md).
"""

import pytest
from valkey.exceptions import ResponseError

from compat_helpers import AGGREGATORS, mk_label_universe, mk_populated, mk_series

MRANGE_COMMANDS = ("TS.MRANGE", "TS.MREVRANGE")

# Reducers valid for GROUPBY ... REDUCE. A strict subset of the aggregator set:
# first/last (scan-order dependent) and twa are rejected with "Invalid reducer
# type". Probed black-box against RTS 8.6.
GROUP_REDUCERS = (
    "avg",
    "sum",
    "min",
    "max",
    "range",
    "count",
    "std.p",
    "std.s",
    "var.p",
    "var.s",
)

NON_REDUCERS = ("first", "last", "twa")


@pytest.fixture(params=MRANGE_COMMANDS)
def mrange_cmd(request):
    """Direction-agnostic scenarios run for both MRANGE and MREVRANGE."""
    return request.param


def _both_raise(diff, *args):
    """Assert both engines reject `args`, without diffing the error wording.

    Used where reject/accept parity holds but the message *shape* differs — RTS
    emits a generic "wrong number of arguments" arity error for structurally
    missing tokens (no FILTER, GROUPBY without REDUCE), where we emit a specific
    TSDB message. Same condition, different words; routing through `diff` would
    fail on the text, so the engines are driven directly.
    """
    with pytest.raises(ResponseError):
        diff.reference.execute_command(*args)
    with pytest.raises(ResponseError):
        diff.subject.execute_command(*args)


def _ref_rejects_subject_accepts(diff, *args):
    """Assert the reference rejects `args` while the subject accepts them.

    An accepted-input superset (plan §5.2) that is non-registrable and can not
    be routed through `diff`. Used for the Prometheus-style FILTER extensions
    (DIV-0019, DIV-0020) our engine supports beyond RTS.
    """
    with pytest.raises(ResponseError):
        diff.reference.execute_command(*args)
    diff.subject.execute_command(*args)


class TestFilterLanguage:
    def test_equality_matcher(self, diff, mrange_cmd):
        mk_label_universe(diff)
        diff(mrange_cmd, "-", "+", "FILTER", "metric=cpu")
        diff(mrange_cmd, "-", "+", "FILTER", "host=h1")

    def test_inequality_matcher(self, diff, mrange_cmd):
        mk_label_universe(diff)
        # A negative matcher needs a positive one alongside it (see
        # test_negative_only_matcher_rejected); pair it with metric.
        diff(mrange_cmd, "-", "+", "FILTER", "metric=cpu", "region!=eu")

    def test_list_matchers(self, diff, mrange_cmd):
        mk_label_universe(diff)
        diff(mrange_cmd, "-", "+", "FILTER", "metric=(cpu,mem)")
        diff(mrange_cmd, "-", "+", "FILTER", "metric=(cpu,mem)", "host!=(h2)")
        diff(mrange_cmd, "-", "+", "FILTER", "host=(h1,h2)", "metric=cpu")

    def test_single_element_list(self, diff, mrange_cmd):
        mk_label_universe(diff)
        diff(mrange_cmd, "-", "+", "FILTER", "metric=(cpu)")

    def test_presence_and_absence_matchers(self, diff, mrange_cmd):
        mk_label_universe(diff)
        # region= : label absent; region!= : label present. Both need a positive
        # matcher, so anchor on metric.
        diff(mrange_cmd, "-", "+", "FILTER", "metric=(cpu,mem)", "region=")
        diff(mrange_cmd, "-", "+", "FILTER", "metric=(cpu,mem)", "region!=")

    def test_combined_matchers(self, diff, mrange_cmd):
        mk_label_universe(diff)
        diff(
            mrange_cmd, "-", "+",
            "FILTER", "metric=(cpu,mem)", "host=h1", "region!=eu",
        )

    def test_no_match_returns_empty(self, diff, mrange_cmd):
        mk_label_universe(diff)
        diff(mrange_cmd, "-", "+", "FILTER", "metric=nope")
        diff(mrange_cmd, "-", "+", "FILTER", "metric=cpu", "host=nope")

    def test_negative_only_matcher_is_a_superset(self, diff, mrange_cmd):
        """DIV-0019: a filter with no positive matcher.

        RTS requires at least one intersecting (equality) matcher and rejects a
        purely negative/absence filter with "please provide at least one
        matcher". Our FILTER language is Prometheus-style, so `metric!=cpu` or
        `region=` (absence) alone select the complement — an accepted-input
        superset. The reference's rejection text already matches our
        MISSING_FILTER, so this is enforceable if the owner chooses parity.
        """
        mk_label_universe(diff)
        _ref_rejects_subject_accepts(diff, mrange_cmd, "-", "+", "FILTER", "metric!=cpu")
        _ref_rejects_subject_accepts(diff, mrange_cmd, "-", "+", "FILTER", "region=")

    def test_bare_metric_name_matcher_is_a_superset(self, diff, mrange_cmd):
        """DIV-0020: a bare token with no operator.

        RTS rejects `not-a-matcher` with "failed parsing labels". We accept a
        bare token as a Prometheus metric-name selector (matching nothing here),
        an accepted-input superset from the same extended FILTER grammar.
        """
        mk_label_universe(diff)
        _ref_rejects_subject_accepts(diff, mrange_cmd, "-", "+", "FILTER", "not-a-matcher")

    def test_empty_filter_rejected(self, diff, mrange_cmd):
        # Both reject an empty FILTER; the wording differs (RTS "missing labels
        # for filter argument" vs our "please provide at least one matcher").
        mk_label_universe(diff)
        _both_raise(diff, mrange_cmd, "-", "+", "FILTER")


class TestWithLabels:
    def test_withlabels(self, diff, mrange_cmd):
        mk_label_universe(diff)
        diff(mrange_cmd, "-", "+", "WITHLABELS", "FILTER", "metric=cpu")

    def test_selected_labels_present(self, diff, mrange_cmd):
        mk_label_universe(diff)
        diff(
            mrange_cmd, "-", "+",
            "SELECTED_LABELS", "host", "region", "FILTER", "metric=cpu",
        )

    def test_selected_labels_missing_becomes_nil(self, diff, mrange_cmd):
        mk_label_universe(diff)
        # u:mem:1 lacks `region`; the selected slot must come back nil, not
        # absent — a shape the normalizer keeps distinct from empty.
        diff(
            mrange_cmd, "-", "+",
            "SELECTED_LABELS", "host", "region", "FILTER", "metric=mem",
        )

    def test_selected_labels_single(self, diff, mrange_cmd):
        mk_label_universe(diff)
        diff(mrange_cmd, "-", "+", "SELECTED_LABELS", "host", "FILTER", "metric=cpu")

    def test_selected_labels_all_missing(self, diff, mrange_cmd):
        mk_label_universe(diff)
        diff(mrange_cmd, "-", "+", "SELECTED_LABELS", "nope", "FILTER", "metric=cpu")

    def test_withlabels_and_selected_labels_together_rejected(self, diff, mrange_cmd):
        mk_label_universe(diff)
        with pytest.raises(ResponseError):
            diff(
                mrange_cmd, "-", "+",
                "WITHLABELS", "SELECTED_LABELS", "host", "FILTER", "metric=cpu",
            )

    def test_no_labels_by_default(self, diff, mrange_cmd):
        """Without WITHLABELS/SELECTED_LABELS the label slot is an empty set."""
        mk_label_universe(diff)
        diff(mrange_cmd, "-", "+", "FILTER", "metric=cpu")


class TestPerSeriesOptions:
    def test_count(self, diff, mrange_cmd):
        mk_label_universe(diff)
        diff(mrange_cmd, "-", "+", "COUNT", 1, "FILTER", "metric=cpu")
        diff(mrange_cmd, "-", "+", "COUNT", 2, "FILTER", "metric=cpu")

    def test_count_zero_rejected(self, diff, mrange_cmd):
        mk_label_universe(diff)
        with pytest.raises(ResponseError):
            diff(mrange_cmd, "-", "+", "COUNT", 0, "FILTER", "metric=cpu")

    @pytest.mark.parametrize("agg", AGGREGATORS)
    def test_aggregation_per_series(self, diff, mrange_cmd, agg):
        mk_label_universe(diff)
        diff(
            mrange_cmd, "-", "+",
            "AGGREGATION", agg, 100, "FILTER", "metric=cpu",
        )

    def test_aggregation_with_align_and_buckettimestamp(self, diff, mrange_cmd):
        # ALIGN start needs an explicit start bound (both engines enforce it).
        mk_label_universe(diff)
        diff(
            mrange_cmd, 100, 300,
            "ALIGN", "start", "AGGREGATION", "avg", 100,
            "BUCKETTIMESTAMP", "~", "FILTER", "metric=cpu",
        )

    def test_empty_fills_gaps_per_series(self, diff, mrange_cmd):
        mk_populated(diff, "g:1", [(0, 1.0), (2000, 4.0)], "LABELS", "k", "v")
        mk_populated(diff, "g:2", [(0, 2.0), (1000, 3.0)], "LABELS", "k", "v")
        diff(
            mrange_cmd, "-", "+",
            "AGGREGATION", "sum", 1000, "EMPTY", "FILTER", "k=v",
        )

    def test_filter_by_ts(self, diff, mrange_cmd):
        mk_label_universe(diff)
        diff(mrange_cmd, "-", "+", "FILTER_BY_TS", 100, 300, "FILTER", "metric=cpu")

    def test_filter_by_value(self, diff, mrange_cmd):
        mk_label_universe(diff)
        diff(
            mrange_cmd, "-", "+",
            "FILTER_BY_VALUE", 10, 40, "FILTER", "metric=cpu",
        )

    def test_bounds_and_window(self, diff, mrange_cmd):
        mk_label_universe(diff)
        diff(mrange_cmd, 100, 200, "FILTER", "metric=cpu")
        diff(mrange_cmd, 150, 250, "FILTER", "metric=cpu")

    def test_latest_on_compaction_targets(self, diff, mrange_cmd):
        mk_series(diff, "lat:src:1", "LABELS", "role", "src")
        mk_series(diff, "lat:dst:1", "LABELS", "role", "dst")
        mk_series(diff, "lat:src:2", "LABELS", "role", "src")
        mk_series(diff, "lat:dst:2", "LABELS", "role", "dst")
        diff("TS.CREATERULE", "lat:src:1", "lat:dst:1", "AGGREGATION", "sum", 1000)
        diff("TS.CREATERULE", "lat:src:2", "lat:dst:2", "AGGREGATION", "sum", 1000)
        for ts, value in [(0, 1.0), (500, 2.0), (1000, 4.0), (2500, 8.0)]:
            diff("TS.ADD", "lat:src:1", ts, value)
            diff("TS.ADD", "lat:src:2", ts, value * 10)
        diff(mrange_cmd, "-", "+", "FILTER", "role=dst")
        diff(mrange_cmd, "-", "+", "LATEST", "FILTER", "role=dst")


class TestGroupByReduce:
    @pytest.mark.parametrize("reducer", GROUP_REDUCERS)
    def test_all_reducers(self, diff, mrange_cmd, reducer):
        mk_label_universe(diff)
        diff(
            mrange_cmd, "-", "+",
            "FILTER", "metric=cpu", "GROUPBY", "metric", "REDUCE", reducer,
        )

    def test_groupby_multiple_groups(self, diff, mrange_cmd):
        mk_label_universe(diff)
        diff(
            mrange_cmd, "-", "+",
            "FILTER", "metric=(cpu,mem)", "GROUPBY", "metric", "REDUCE", "sum",
        )
        diff(
            mrange_cmd, "-", "+",
            "FILTER", "metric=(cpu,mem)", "GROUPBY", "host", "REDUCE", "max",
        )

    def test_groupby_label_absent_from_some_series(self, diff, mrange_cmd):
        """Series lacking the GROUPBY label are dropped from the result."""
        mk_label_universe(diff)
        diff(
            mrange_cmd, "-", "+",
            "FILTER", "metric=(cpu,mem)", "GROUPBY", "region", "REDUCE", "sum",
        )

    def test_groupby_single_series_group(self, diff, mrange_cmd):
        mk_label_universe(diff)
        diff(
            mrange_cmd, "-", "+",
            "FILTER", "metric=mem", "GROUPBY", "host", "REDUCE", "avg",
        )

    def test_groupby_with_aggregation(self, diff, mrange_cmd):
        mk_label_universe(diff)
        diff(
            mrange_cmd, "-", "+",
            "AGGREGATION", "avg", 100, "FILTER", "metric=cpu",
            "GROUPBY", "metric", "REDUCE", "sum",
        )

    def test_groupby_with_withlabels(self, diff, mrange_cmd):
        mk_label_universe(diff)
        diff(
            mrange_cmd, "-", "+",
            "WITHLABELS", "FILTER", "metric=cpu", "GROUPBY", "metric", "REDUCE", "sum",
        )

    @pytest.mark.parametrize("reducer", NON_REDUCERS)
    def test_non_reducer_rejected(self, diff, mrange_cmd, reducer):
        mk_label_universe(diff)
        with pytest.raises(ResponseError):
            diff(
                mrange_cmd, "-", "+",
                "FILTER", "metric=cpu", "GROUPBY", "metric", "REDUCE", reducer,
            )

    def test_unknown_reducer_rejected(self, diff, mrange_cmd):
        mk_label_universe(diff)
        with pytest.raises(ResponseError):
            diff(
                mrange_cmd, "-", "+",
                "FILTER", "metric=cpu", "GROUPBY", "metric", "REDUCE", "median",
            )

    def test_groupby_without_reduce_rejected(self, diff, mrange_cmd):
        # Both reject; RTS as an arity error, we as "missing REDUCE".
        mk_label_universe(diff)
        _both_raise(diff, mrange_cmd, "-", "+", "FILTER", "metric=cpu", "GROUPBY", "metric")

    def test_reduce_without_groupby_rejected(self, diff, mrange_cmd):
        # RTS parses REDUCE as a filter token ("failed parsing labels"); we
        # report the missing GROUPBY. Both reject.
        mk_label_universe(diff)
        _both_raise(diff, mrange_cmd, "-", "+", "FILTER", "metric=cpu", "REDUCE", "sum")

    def test_keyword_case_insensitivity(self, diff, mrange_cmd):
        mk_label_universe(diff)
        diff(
            mrange_cmd, "-", "+",
            "FILTER", "metric=cpu", "groupby", "metric", "reduce", "SUM",
        )


class TestKeyStates:
    def test_wrongtype_key_matching_filter_is_ignored(self, diff, mrange_cmd):
        """A non-TSDB key can not carry ts-* labels, so it never matches — but a
        matching string key must not corrupt the reply."""
        mk_label_universe(diff)
        diff("SET", "u:notaseries", "hello")
        diff(mrange_cmd, "-", "+", "FILTER", "metric=cpu")

    def test_empty_series_in_result(self, diff, mrange_cmd):
        mk_series(diff, "e:1", "LABELS", "k", "v")
        mk_populated(diff, "e:2", [(100, 1.0)], "LABELS", "k", "v")
        diff(mrange_cmd, "-", "+", "FILTER", "k=v")

    def test_all_series_empty(self, diff, mrange_cmd):
        mk_series(diff, "e:a", "LABELS", "k", "v")
        mk_series(diff, "e:b", "LABELS", "k", "v")
        diff(mrange_cmd, "-", "+", "FILTER", "k=v")


class TestArgParsing:
    def test_missing_filter_rejected(self, diff, mrange_cmd):
        # No FILTER at all: RTS arity error, we "no FILTER given". Both reject.
        mk_label_universe(diff)
        _both_raise(diff, mrange_cmd, "-", "+")

    def test_missing_bounds_rejected(self, diff, mrange_cmd):
        mk_label_universe(diff)
        with pytest.raises(ResponseError):
            diff(mrange_cmd, "-", "FILTER", "metric=cpu")

    def test_invalid_bounds_rejected(self, diff, mrange_cmd):
        mk_label_universe(diff)
        with pytest.raises(ResponseError):
            diff(mrange_cmd, "not-a-ts", "+", "FILTER", "metric=cpu")

    def test_selected_labels_requires_a_label(self, diff, mrange_cmd):
        mk_label_universe(diff)
        with pytest.raises(ResponseError):
            diff(mrange_cmd, "-", "+", "SELECTED_LABELS", "FILTER", "metric=cpu")

    def test_full_option_combination(self, diff, mrange_cmd):
        mk_label_universe(diff)
        diff(
            mrange_cmd, 100, 300,
            "FILTER_BY_TS", 100, 200, 300,
            "FILTER_BY_VALUE", 0, 1000,
            "WITHLABELS",
            "COUNT", 5,
            "ALIGN", "start",
            "AGGREGATION", "avg", 100,
            "FILTER", "metric=cpu",
            "GROUPBY", "metric", "REDUCE", "sum",
        )
