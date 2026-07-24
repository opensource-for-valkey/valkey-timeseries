"""Tier A read-path matrix: the AGGREGATION aggregator *list* (test plan §6).

RedisTimeSeries 8.8 widened the range family's AGGREGATION clause. The
`aggregators` operand is now one or more aggregator names separated by commas
(`AGGREGATION min,avg,max 1000`), and the arity of that list switches the reply
shape: without AGGREGATION, or with a single aggregator, each bucket is the
familiar `[timestamp, value]` pair; with two or more, each bucket widens to
`[timestamp, value...]` — one value per aggregator, in the order requested.

That switch is why this dimension gets its own module. A client that parses a
bucket as a two-element pair breaks on a three-element row, so the boundary
between "one aggregator" and "a list of one" has to fall in exactly the same
place on both engines, and the columns have to carry the same values in the
same order. Covered here: the pair/row switch across TS.RANGE, TS.REVRANGE,
TS.MRANGE and TS.MREVRANGE; column order and per-column equivalence with the
same aggregator queried alone; the list grammar (separators, whitespace, case,
unknown and empty elements); and the list's interaction with the bucketing and
filtering options (ALIGN, BUCKETTIMESTAMP, EMPTY, COUNT, FILTER_BY_TS,
FILTER_BY_VALUE, LATEST) plus the RESP3 `aggregators` metadata map. Each test
runs under RESP2 and RESP3 via the `protocol` parametrization in conftest.

Written clean-room from public RedisTimeSeries documentation and black-box
observation of the reference server. Do NOT consult RedisTimeSeries source or
test code (see tests/compat/README.md).
"""

import pytest
from valkey.exceptions import ResponseError

from compat_helpers import AGGREGATORS, mk_populated, mk_series

RANGE_COMMANDS = ("TS.RANGE", "TS.REVRANGE")
MRANGE_COMMANDS = ("TS.MRANGE", "TS.MREVRANGE")

# Every aggregator name RTS 8.8 accepts as a list element, minus `twa`
# (DIV-0012: unimplemented here, so a list containing it errors on one side
# only — pinned by test_twa_inside_a_list_is_rejected). The 8.6 NaN counters
# are ordinary list elements and are included.
LIST_AGGREGATORS = AGGREGATORS + ("countnan", "countall")

# Four samples inside one bucket, valued so that all eight plain aggregators
# yield a *distinct* number — avg 26, sum 104, min 10, max 44, range 34,
# count 4, first 30, last 20. Two columns swapped therefore surface as a value
# mismatch instead of passing silently. Bucket [2000,3000) holds a single
# sample, [3000,4000) is deliberately empty (the EMPTY hole), and [4000,5000)
# closes the series.
MULTI_SAMPLES = [
    (1000, 30.0),
    (1200, 44.0),
    (1500, 10.0),
    (1700, 20.0),
    (2000, 7.0),
    (4000, 5.0),
    (4500, 15.0),
]

# Enough columns to catch an off-by-one in row width without depending on the
# whole aggregator vocabulary.
TRIPLE = "avg,max,count"


@pytest.fixture(params=RANGE_COMMANDS)
def range_cmd(request):
    """Direction-agnostic scenarios run for both directions."""
    return request.param


@pytest.fixture(params=MRANGE_COMMANDS)
def mrange_cmd(request):
    return request.param


def mk_multi_universe(diff):
    """Two labelled series with overlapping buckets, for the MRANGE cases."""
    mk_populated(
        diff, "ma:1", [(1000, 2.0), (1500, 4.0), (2500, 9.0)],
        "LABELS", "region", "us", "kind", "a",
    )
    mk_populated(
        diff, "ma:2", [(1000, 10.0), (2500, 6.0)],
        "LABELS", "region", "us", "kind", "b",
    )


def _both_reject(diff, *args):
    """Assert both engines reject `args`, without diffing the error wording.

    Used where the reject/accept decision matches but the message does not —
    RTS names the specific list defect ("Empty aggregation type in list") where
    we report the clause as a whole ("invalid aggregation list"). Same
    condition, different words; routing through `diff` would fail on an
    error-text delta, and error-message text parity is an explicit non-goal
    (COMPATIBILITY.md, "Error message text parity").
    """
    with pytest.raises(ResponseError):
        diff.reference.execute_command(*args)
    with pytest.raises(ResponseError):
        diff.subject.execute_command(*args)


def _series_rows(reply, key, protocol):
    """Pull one series' sample rows out of an MRANGE reply.

    RESP2 replies are a list of `[key, labels, samples]`; RESP3 replies are a
    map from key to `[labels, metadata..., samples]`. The sample block is last
    in both, so indexing from the end covers both shapes and the extra grouped
    metadata entries.
    """
    key = key.encode()
    if protocol == 3:
        return reply[key][-1]
    for entry in reply:
        if entry[0] == key:
            return entry[-1]
    raise AssertionError(f"series {key!r} missing from reply: {reply!r}")


class TestReplyShapeSwitch:
    """The arity of the list decides pair-vs-row. This is the contract."""

    def test_single_aggregator_keeps_the_pair_shape(self, diff, range_cmd):
        """One aggregator is a one-element list, and must stay a 2-tuple."""
        mk_populated(diff, "ma:pair", MULTI_SAMPLES)
        reply = diff(range_cmd, "ma:pair", "-", "+", "AGGREGATION", "avg", 1000)
        assert all(len(row) == 2 for row in reply), reply

    def test_two_aggregators_widen_the_row(self, diff, range_cmd):
        mk_populated(diff, "ma:two", MULTI_SAMPLES)
        reply = diff(range_cmd, "ma:two", "-", "+", "AGGREGATION", "min,max", 1000)
        assert all(len(row) == 3 for row in reply), reply

    def test_row_width_tracks_the_list_length(self, diff, range_cmd):
        """Width is 1 + len(list) for every arity, not just the two-column case."""
        mk_populated(diff, "ma:width", MULTI_SAMPLES)
        for n in range(1, len(LIST_AGGREGATORS) + 1):
            aggregators = ",".join(LIST_AGGREGATORS[:n])
            reply = diff(range_cmd, "ma:width", "-", "+", "AGGREGATION", aggregators, 1000)
            assert all(len(row) == 1 + n for row in reply), (aggregators, reply)

    def test_no_aggregation_keeps_the_pair_shape(self, diff, range_cmd):
        """The baseline the switch is measured against."""
        mk_populated(diff, "ma:none", MULTI_SAMPLES)
        reply = diff(range_cmd, "ma:none", "-", "+")
        assert all(len(row) == 2 for row in reply), reply


class TestColumns:
    def test_column_order_follows_the_request(self, diff, range_cmd):
        """Not a canonical order: the same set requested differently transposes."""
        mk_populated(diff, "ma:order", MULTI_SAMPLES)
        forward = diff(range_cmd, "ma:order", "-", "+", "AGGREGATION", "avg,max,count", 1000)
        reverse = diff(range_cmd, "ma:order", "-", "+", "AGGREGATION", "count,max,avg", 1000)
        assert len(forward) == len(reverse)
        for fwd, rev in zip(forward, reverse):
            assert fwd[0] == rev[0]
            assert [fwd[1], fwd[2], fwd[3]] == [rev[3], rev[2], rev[1]]

    # Parametrized over AGGREGATORS rather than LIST_AGGREGATORS: the NaN
    # counters are excluded because their *solo* baseline is not well-defined
    # on a NaN-free fixture (countnan accepts no sample, so RTS emits no bucket
    # at all), which makes the comparison a statement about bucket emission
    # rather than about column plumbing. TestNanColumns owns that dimension.
    @pytest.mark.parametrize("agg", AGGREGATORS)
    def test_column_matches_the_same_aggregator_alone(self, diff, range_cmd, agg):
        """Column i is exactly what that aggregator returns on its own.

        Both queries go through `diff`, so each is already checked against the
        reference; this pins the *relationship* between them, which is what a
        column-plumbing bug would break without changing either reply's shape.
        """
        mk_populated(diff, "ma:col", MULTI_SAMPLES)
        aggregators = list(AGGREGATORS)
        multi = diff(
            range_cmd, "ma:col", "-", "+", "AGGREGATION", ",".join(aggregators), 1000
        )
        index = aggregators.index(agg)
        solo = diff(range_cmd, "ma:col", "-", "+", "AGGREGATION", agg, 1000)

        assert len(multi) == len(solo)
        for row, expected in zip(multi, solo):
            assert row[0] == expected[0]
            assert row[1 + index] == expected[1], (agg, row, expected)

    def test_repeating_the_whole_list_is_stable(self, diff, range_cmd):
        """Same query twice: column values must not depend on accumulator reuse."""
        mk_populated(diff, "ma:stable", MULTI_SAMPLES)
        first = diff(range_cmd, "ma:stable", "-", "+", "AGGREGATION", TRIPLE, 1000)
        second = diff(range_cmd, "ma:stable", "-", "+", "AGGREGATION", TRIPLE, 1000)
        assert first == second


class TestListGrammar:
    def test_all_supported_aggregators_in_one_list(self, diff, range_cmd):
        mk_populated(diff, "ma:all", MULTI_SAMPLES)
        diff(range_cmd, "ma:all", "-", "+", "AGGREGATION", ",".join(LIST_AGGREGATORS), 1000)

    def test_element_names_are_case_insensitive(self, diff, range_cmd):
        mk_populated(diff, "ma:case", MULTI_SAMPLES)
        diff(range_cmd, "ma:case", "-", "+", "AGGREGATION", "AVG,Max,cOuNt", 1000)
        diff(range_cmd, "ma:case", "-", "+", "AGGREGATION", "STD.P,var.S", 1000)

    @pytest.mark.parametrize(
        "aggregators",
        [
            "avg,,max",   # empty interior element
            "avg,max,",   # trailing separator
            ",avg,max",   # leading separator
            ",",          # separators only
            "",           # empty operand
        ],
    )
    def test_empty_list_element_rejected(self, diff, range_cmd, aggregators):
        """Both engines reject; only the wording differs (see _both_reject)."""
        mk_populated(diff, "ma:empty", MULTI_SAMPLES)
        _both_reject(diff, range_cmd, "ma:empty", "-", "+", "AGGREGATION", aggregators, 1000)

    @pytest.mark.parametrize("aggregators", ["avg, max", "avg ,max", "avg , max"])
    def test_whitespace_between_elements_rejected(self, diff, range_cmd, aggregators):
        """The docs are explicit: no whitespace is allowed between aggregators."""
        mk_populated(diff, "ma:ws", MULTI_SAMPLES)
        with pytest.raises(ResponseError):
            diff(range_cmd, "ma:ws", "-", "+", "AGGREGATION", aggregators, 1000)

    @pytest.mark.parametrize("aggregators", ["avg,bogus", "bogus,avg", "avg,median,max"])
    def test_unknown_element_rejects_the_whole_list(self, diff, range_cmd, aggregators):
        mk_populated(diff, "ma:unknown", MULTI_SAMPLES)
        with pytest.raises(ResponseError):
            diff(range_cmd, "ma:unknown", "-", "+", "AGGREGATION", aggregators, 1000)

    def test_list_still_requires_a_bucket_duration(self, diff, range_cmd):
        mk_populated(diff, "ma:dur", MULTI_SAMPLES)
        with pytest.raises(ResponseError):
            diff(range_cmd, "ma:dur", "-", "+", "AGGREGATION", "min,max")

    @pytest.mark.parametrize("duration", [0, -1000])
    def test_list_bucket_duration_validated(self, diff, range_cmd, duration):
        mk_populated(diff, "ma:dur2", MULTI_SAMPLES)
        with pytest.raises(ResponseError):
            diff(range_cmd, "ma:dur2", "-", "+", "AGGREGATION", "min,max", duration)


class TestOptionInteraction:
    """A list must not change how the surrounding options behave."""

    def test_count_limits_buckets_not_columns(self, diff, range_cmd):
        mk_populated(diff, "ma:count", MULTI_SAMPLES)
        reply = diff(range_cmd, "ma:count", "-", "+", "AGGREGATION", TRIPLE, 1000, "COUNT", 2)
        assert len(reply) == 2, reply
        assert all(len(row) == 4 for row in reply), reply

    @pytest.mark.parametrize("align", ["-", "start", 0, 500])
    def test_align(self, diff, range_cmd, align):
        mk_populated(diff, "ma:align", MULTI_SAMPLES)
        diff(range_cmd, "ma:align", 1000, 4500, "ALIGN", align, "AGGREGATION", TRIPLE, 1000)

    @pytest.mark.parametrize("bt", ["-", "+", "~"])
    def test_buckettimestamp(self, diff, range_cmd, bt):
        mk_populated(diff, "ma:bt", MULTI_SAMPLES)
        diff(
            range_cmd, "ma:bt", "-", "+",
            "AGGREGATION", TRIPLE, 1000, "BUCKETTIMESTAMP", bt,
        )

    def test_empty_fills_each_column_with_its_own_value(self, diff):
        """The [3000,4000) hole materializes, and the per-aggregator fill is
        applied column-wise: sum/count fill 0, avg/min fill NaN, last carries
        the previous bucket forward.

        Forward only — `last`'s carry direction under REVRANGE is DIV-0016.
        """
        mk_populated(diff, "ma:empty", MULTI_SAMPLES)
        reply = diff(
            "TS.RANGE", "ma:empty", "-", "+",
            "AGGREGATION", "sum,count,avg,min,last", 1000, "EMPTY",
        )
        assert any(row[0] == 3000 for row in reply), reply

    def test_empty_without_carry_aggregators_in_both_directions(self, diff, range_cmd):
        mk_populated(diff, "ma:empty2", MULTI_SAMPLES)
        diff(range_cmd, "ma:empty2", "-", "+", "AGGREGATION", "sum,count,avg", 1000, "EMPTY")

    def test_filter_by_ts(self, diff, range_cmd):
        mk_populated(diff, "ma:fbts", MULTI_SAMPLES)
        diff(
            range_cmd, "ma:fbts", "-", "+",
            "FILTER_BY_TS", 1000, 1500, 2000, "AGGREGATION", TRIPLE, 1000,
        )

    def test_filter_by_value_applies_before_the_columns(self, diff, range_cmd):
        mk_populated(diff, "ma:fbv", MULTI_SAMPLES)
        diff(
            range_cmd, "ma:fbv", "-", "+",
            "FILTER_BY_VALUE", 10.0, 30.0, "AGGREGATION", TRIPLE, 1000,
        )

    def test_latest_on_a_compaction_target(self, diff, range_cmd):
        mk_series(diff, "ma:lat:src")
        mk_series(diff, "ma:lat:dst")
        diff("TS.CREATERULE", "ma:lat:src", "ma:lat:dst", "AGGREGATION", "sum", 1000)
        for ts, value in [(0, 1.0), (500, 2.0), (1000, 4.0), (1500, 8.0), (2500, 16.0)]:
            diff("TS.ADD", "ma:lat:src", ts, value)
        diff(range_cmd, "ma:lat:dst", "-", "+", "AGGREGATION", "min,max", 2000)
        diff(range_cmd, "ma:lat:dst", "-", "+", "LATEST", "AGGREGATION", "min,max", 2000)

    def test_full_option_combination(self, diff, range_cmd):
        mk_populated(diff, "ma:combo", MULTI_SAMPLES)
        diff(
            range_cmd, "ma:combo", 1000, 4500,
            "FILTER_BY_TS", 1000, 1200, 1500, 1700, 2000, 4000, 4500,
            "FILTER_BY_VALUE", 5.0, 44.0,
            "COUNT", 3,
            "ALIGN", "start",
            "AGGREGATION", "avg,count,sum", 1000,
            "BUCKETTIMESTAMP", "~",
            "EMPTY",
        )


class TestKeyStates:
    def test_empty_series(self, diff, range_cmd):
        mk_series(diff, "ma:emptyseries")
        diff(range_cmd, "ma:emptyseries", "-", "+", "AGGREGATION", TRIPLE, 1000)
        diff(range_cmd, "ma:emptyseries", "-", "+", "AGGREGATION", TRIPLE, 1000, "EMPTY")

    def test_single_sample_series(self, diff, range_cmd):
        """One sample: the sample-variance columns are degenerate alongside
        well-defined ones in the same row."""
        mk_populated(diff, "ma:one", [(1000, 42.0)])
        diff(range_cmd, "ma:one", "-", "+", "AGGREGATION", "avg,std.s,var.s,count", 1000)

    def test_missing_key(self, diff, range_cmd):
        with pytest.raises(ResponseError):
            diff(range_cmd, "ma:nonexistent", "-", "+", "AGGREGATION", TRIPLE, 1000)

    def test_wrongtype(self, diff, range_cmd):
        diff("SET", "ma:string", "hello")
        with pytest.raises(ResponseError):
            diff(range_cmd, "ma:string", "-", "+", "AGGREGATION", TRIPLE, 1000)


class TestMultiSeries:
    """TS.MRANGE / TS.MREVRANGE — the same switch, one nesting level deeper.

    RESP3 additionally reports the list in an `aggregators` metadata map per
    series; `diff` compares it automatically as part of the reply.
    """

    def test_row_shape_per_series(self, diff, mrange_cmd, protocol):
        mk_multi_universe(diff)
        reply = diff(
            mrange_cmd, "-", "+", "AGGREGATION", TRIPLE, 1000, "FILTER", "region=us"
        )
        for key in ("ma:1", "ma:2"):
            rows = _series_rows(reply, key, protocol)
            assert rows and all(len(row) == 4 for row in rows), (key, rows)

    def test_single_aggregator_keeps_the_pair_shape(self, diff, mrange_cmd, protocol):
        mk_multi_universe(diff)
        reply = diff(
            mrange_cmd, "-", "+", "AGGREGATION", "avg", 1000, "FILTER", "region=us"
        )
        for key in ("ma:1", "ma:2"):
            rows = _series_rows(reply, key, protocol)
            assert rows and all(len(row) == 2 for row in rows), (key, rows)

    def test_with_labels(self, diff, mrange_cmd):
        mk_multi_universe(diff)
        diff(
            mrange_cmd, "-", "+", "WITHLABELS",
            "AGGREGATION", TRIPLE, 1000, "FILTER", "region=us",
        )

    def test_selected_labels(self, diff, mrange_cmd):
        mk_multi_universe(diff)
        diff(
            mrange_cmd, "-", "+", "SELECTED_LABELS", "kind",
            "AGGREGATION", TRIPLE, 1000, "FILTER", "region=us",
        )

    def test_options_through_the_multi_series_entry_point(self, diff, mrange_cmd):
        mk_multi_universe(diff)
        diff(
            mrange_cmd, "-", "+", "COUNT", 2,
            "ALIGN", 0, "AGGREGATION", "avg,count", 1000, "BUCKETTIMESTAMP", "+",
            "FILTER", "region=us",
        )

    def test_series_with_no_samples_in_window(self, diff, mrange_cmd):
        mk_multi_universe(diff)
        mk_series(diff, "ma:3", "LABELS", "region", "us", "kind", "c")
        diff(mrange_cmd, "-", "+", "AGGREGATION", TRIPLE, 1000, "FILTER", "region=us")

    def test_list_grammar_errors_surface_the_same_way(self, diff, mrange_cmd):
        """The list is parsed before the filter, so the same defects reject
        here as on the single-series path."""
        mk_multi_universe(diff)
        # Identical wording on both engines: diffable.
        for aggregators in ("avg,bogus", "avg, max"):
            with pytest.raises(ResponseError):
                diff(
                    mrange_cmd, "-", "+",
                    "AGGREGATION", aggregators, 1000, "FILTER", "region=us",
                )
        # Same rejection, different message (see _both_reject).
        _both_reject(
            diff, mrange_cmd, "-", "+",
            "AGGREGATION", "avg,,max", 1000, "FILTER", "region=us",
        )


class TestNanColumns:
    """NaN samples across a row: emission is decided per bucket, values per column.

    `countnan`/`countall` accept NaN samples that every other aggregator skips,
    so a bucket holding only NaNs is real for those columns and empty for the
    rest. The reference emits such a bucket when *any* column accepted a sample
    and fills the others (NaN for avg, 0 for count).

    These go through `diff`, not per-engine assertions, on purpose: unlike the
    entries in TestDivergences this is not an intentional divergence but a
    genuine subject bug, so it should keep failing until fixed. The all-NaN
    case below currently diverges for the same reason the single-aggregator
    tests in test_compat_range.py (TestAggregation::test_aggregator_over_all_nan_bucket)
    already fail unregistered: the subject omits an all-NaN bucket that RTS
    emits once a NaN-counting column keeps it alive. The row surface adds the
    specific question those tests can't ask — whether the *whole row* is
    dropped when one column (avg/count) rejected the NaNs but another
    (countnan) accepted them — and the answer today is that it is.
    """

    def test_all_nan_bucket_with_a_counting_column(self, diff, range_cmd):
        mk_populated(diff, "ma:nan", [(1000, "nan"), (2000, 5.0)])
        diff(range_cmd, "ma:nan", "-", "+", "AGGREGATION", "avg,count,countnan", 1000)

    def test_mixed_nan_bucket(self, diff, range_cmd):
        mk_populated(diff, "ma:nanmix", [(1000, 1.0), (1200, "nan"), (1500, 3.0)])
        diff(range_cmd, "ma:nanmix", "-", "+", "AGGREGATION", "avg,count,countnan,countall", 1000)

    def test_ordinary_bucket_with_a_nan_column(self, diff, range_cmd):
        """The converse: countnan accepts nothing here, but the other columns
        keep the bucket alive."""
        mk_populated(diff, "ma:nanord", [(1000, 1.0), (2000, 2.0)])
        diff(range_cmd, "ma:nanord", "-", "+", "AGGREGATION", "avg,countnan", 1000)


# --------------------------------------------------------------------------
# Divergences. Each is pinned per-engine rather than through `diff`, because
# the mismatch class is one the registry can not express (README, "Divergences
# the registry can not express"): an accepted-input superset is hard-failed by
# DiffClient by design, and an over-strict rejection could only be scoped by a
# regex broad enough to mask real regressions in the same delta class.
# --------------------------------------------------------------------------


class TestDivergences:
    def test_duplicate_aggregator_in_a_list_is_rejected(self, diff, range_cmd):
        """DIV-0036: we reject a repeated aggregator; RTS 8.8 accepts it.

        RTS treats the list positionally — `AGGREGATION avg,avg` returns two
        identical columns — while we reject the whole clause with "duplicate
        aggregation". Over-strict rejection, so it is asserted per-engine.

        This also puts the list-length cap out of joint reach. Both engines cap
        at 16 elements, but only 15 distinct names exist, so a 17-element list
        needs a repeat and dies on this divergence here before it reaches the
        cap. Delete this test if the duplicate restriction is lifted.
        """
        mk_populated(diff, "ma:dup", MULTI_SAMPLES)
        for aggregators in ("avg,avg", "avg,AVG", "min,max,min"):
            reference = diff.reference.execute_command(
                range_cmd, "ma:dup", "-", "+", "AGGREGATION", aggregators, 1000
            )
            assert reference, f"expected RTS to accept {aggregators!r}"
            with pytest.raises(ResponseError):
                diff.subject.execute_command(
                    range_cmd, "ma:dup", "-", "+", "AGGREGATION", aggregators, 1000
                )

    def test_duplicate_columns_are_identical_on_the_reference(self, diff, range_cmd):
        """DIV-0036, value side: RTS's repeated column is a real second copy,
        not a placeholder — which is what makes the restriction a behavior
        change rather than a no-op for a client that sends one."""
        mk_populated(diff, "ma:dup2", MULTI_SAMPLES)
        rows = diff.reference.execute_command(
            range_cmd, "ma:dup2", "-", "+", "AGGREGATION", "avg,avg", 1000
        )
        for row in rows:
            assert len(row) == 3 and row[1] == row[2], row

    def test_groupby_with_a_multi_aggregator_list_is_a_superset(self, diff, mrange_cmd):
        """DIV-0037: RTS rejects GROUPBY alongside a multi-aggregator list; we
        reduce column-wise instead.

        The documented rule is "GROUPBY/REDUCE is not permitted when multiple
        aggregators are specified", and RTS enforces it on list length — even
        `avg,avg`, one distinct aggregator, is refused. We apply the reducer to
        each column independently and return a grouped row of the same width.
        An accepted-input superset, which plan §5.2 makes non-registrable, so it
        is stated here explicitly rather than routed through `diff`.
        """
        mk_multi_universe(diff)
        args = (
            mrange_cmd, "-", "+", "AGGREGATION", "avg,max", 1000,
            "FILTER", "region=us", "GROUPBY", "region", "REDUCE", "sum",
        )
        with pytest.raises(ResponseError):
            diff.reference.execute_command(*args)
        subject = diff.subject.execute_command(*args)
        assert subject, "expected a grouped reply from the subject"

    def test_groupby_with_a_single_aggregator_still_agrees(self, diff, mrange_cmd):
        """The boundary DIV-0037 must not be allowed to swallow: with one
        aggregator, GROUPBY is legal on both engines and must still match."""
        mk_multi_universe(diff)
        diff(
            mrange_cmd, "-", "+", "AGGREGATION", "avg", 1000,
            "FILTER", "region=us", "GROUPBY", "region", "REDUCE", "sum",
        )

    def test_reduce_does_not_accept_a_list(self, diff, mrange_cmd):
        """The list grammar is scoped to AGGREGATION: REDUCE stays single, and
        both engines reject a comma-separated reducer."""
        mk_multi_universe(diff)
        with pytest.raises(ResponseError):
            diff(
                mrange_cmd, "-", "+", "FILTER", "region=us",
                "GROUPBY", "region", "REDUCE", "sum,max",
            )

    def test_createrule_does_not_accept_a_list(self, diff):
        """Also scoped out of the compaction path: a rule takes exactly one
        aggregator. Both engines reject — RTS with "CREATERULE requires exactly
        one aggregation type", we with "Unknown aggregation type" — so this is
        a _both_reject case rather than a diffable one."""
        mk_series(diff, "ma:rule:src")
        mk_series(diff, "ma:rule:dst")
        _both_reject(
            diff, "TS.CREATERULE", "ma:rule:src", "ma:rule:dst",
            "AGGREGATION", "avg,max", 1000,
        )

    def test_twa_inside_a_list_is_rejected(self, diff, range_cmd):
        """DIV-0012 on the list surface: `twa` is unimplemented here, so a list
        containing it fails even though every other element is supported.
        Delete this test when twa lands."""
        mk_populated(diff, "ma:twa", MULTI_SAMPLES)
        args = (range_cmd, "ma:twa", "-", "+", "AGGREGATION", "avg,twa", 1000)
        diff.reference.execute_command(*args)
        with pytest.raises(ResponseError):
            diff.subject.execute_command(*args)

    @pytest.mark.parametrize("aggregators", ["countif(>15),avg", "avg,rate"])
    def test_extension_list_elements_are_a_superset(self, diff, range_cmd, aggregators):
        """DIV-0038: our list-element grammar accepts names and an inline
        `name(condition)` form that RTS has no vocabulary for.

        `rate` is not an RTS aggregator, and the parenthesized per-element
        condition (`countif(>15)`) has no RTS equivalent — both answer "Unknown
        aggregation type". An accepted-input superset, so per-engine.
        """
        mk_populated(diff, "ma:ext", MULTI_SAMPLES)
        args = (range_cmd, "ma:ext", "-", "+", "AGGREGATION", aggregators, 1000)
        with pytest.raises(ResponseError):
            diff.reference.execute_command(*args)
        assert diff.subject.execute_command(*args)
