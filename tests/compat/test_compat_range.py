"""Tier A read-path matrix: TS.RANGE / TS.REVRANGE (test plan §6).

Covers the row's specific dimensions: `-`/`+` bounds, inclusive boundaries,
COUNT, AGGREGATION × all 13 aggregators × bucket boundary cases, ALIGN
(`-`/`+`/`start`/`end`/explicit ts), BUCKETTIMESTAMP (`-`/`+`/`~`), EMPTY,
FILTER_BY_TS, FILTER_BY_VALUE, LATEST on a compaction target, and option
ordering — plus the dimensions applied to every command: arg parsing (valid /
invalid / missing value / duplicated / case-insensitivity), key states (missing
key, WRONGTYPE, empty series), and error paths. Each test runs under RESP2 and
RESP3 via the `protocol` parametrization in conftest.

Every command issued through the `diff` fixture is sent to both engines and its
reply diffed automatically; a test body is just the command sequence.

Written clean-room from public RedisTimeSeries documentation and black-box
observation of the reference server. Do NOT consult RedisTimeSeries source or
test code (see tests/compat/README.md).
"""

import pytest
from valkey.exceptions import ResponseError

from compat_helpers import (
    AGGREGATORS,
    UNSUPPORTED_AGGREGATORS,
    mk_populated,
    mk_series,
)

RANGE_COMMANDS = ("TS.RANGE", "TS.REVRANGE")

# A series with an intra-bucket gap (nothing in [2000,3000)) so EMPTY and
# bucket-alignment cases have a hole to expose, and unevenly spaced samples so
# twa and the variance family have something non-trivial to chew on.
BASE_SAMPLES = [
    (0, 1.0),
    (250, 2.0),
    (750, 4.0),
    (1000, 8.0),
    (1500, 16.0),
    (3000, 32.0),
    (3999, 64.0),
    (4000, 128.0),
]


@pytest.fixture(params=RANGE_COMMANDS)
def range_cmd(request):
    """Every scenario that is direction-agnostic runs for both directions."""
    return request.param


class TestBounds:
    def test_full_range_and_explicit_bounds(self, diff, range_cmd):
        mk_populated(diff, "r:bounds", BASE_SAMPLES)
        diff(range_cmd, "r:bounds", "-", "+")
        diff(range_cmd, "r:bounds", 0, 4000)
        diff(range_cmd, "r:bounds", "-", 1000)
        diff(range_cmd, "r:bounds", 1000, "+")

    def test_bounds_are_inclusive(self, diff, range_cmd):
        mk_populated(diff, "r:incl", BASE_SAMPLES)
        # Both endpoints land exactly on samples: both must be returned.
        diff(range_cmd, "r:incl", 250, 1000)
        # ±1 around a sample isolates the inclusivity decision.
        diff(range_cmd, "r:incl", 251, 999)
        diff(range_cmd, "r:incl", 249, 1001)

    def test_single_point_range(self, diff, range_cmd):
        mk_populated(diff, "r:point", BASE_SAMPLES)
        diff(range_cmd, "r:point", 1000, 1000)  # on a sample
        diff(range_cmd, "r:point", 1001, 1001)  # between samples

    def test_range_outside_data(self, diff, range_cmd):
        mk_populated(diff, "r:outside", BASE_SAMPLES)
        diff(range_cmd, "r:outside", 10000, 20000)
        diff(range_cmd, "r:outside", 0, 0)

    def test_start_after_end(self, diff, range_cmd):
        """An inverted window is an empty result, not an error."""
        mk_populated(diff, "r:reversed", BASE_SAMPLES)
        diff(range_cmd, "r:reversed", 4000, 0)
        diff(range_cmd, "r:reversed", 4000, 0, "AGGREGATION", "sum", 1000)

    def test_negative_timestamp_bounds(self, diff, range_cmd):
        mk_populated(diff, "r:neg", BASE_SAMPLES)
        with pytest.raises(ResponseError):
            diff(range_cmd, "r:neg", -1000, 1000)


class TestCount:
    def test_count_truncates_from_the_scan_direction(self, diff, range_cmd):
        mk_populated(diff, "r:count", BASE_SAMPLES)
        for count in (1, 3, len(BASE_SAMPLES), len(BASE_SAMPLES) + 10):
            diff(range_cmd, "r:count", "-", "+", "COUNT", count)

    def test_count_zero_rejected(self, diff, range_cmd):
        """COUNT must be >= 1: zero is rejected, not treated as "no samples"."""
        mk_populated(diff, "r:count0", BASE_SAMPLES)
        with pytest.raises(ResponseError):
            diff(range_cmd, "r:count0", "-", "+", "COUNT", 0)

    def test_count_negative_rejected(self, diff, range_cmd):
        mk_populated(diff, "r:countneg", BASE_SAMPLES)
        with pytest.raises(ResponseError):
            diff(range_cmd, "r:countneg", "-", "+", "COUNT", -1)

    def test_count_applies_after_aggregation(self, diff, range_cmd):
        mk_populated(diff, "r:countagg", BASE_SAMPLES)
        diff(range_cmd, "r:countagg", "-", "+", "AGGREGATION", "sum", 1000, "COUNT", 2)


class TestAggregation:
    @pytest.mark.parametrize("agg", AGGREGATORS)
    def test_aggregator_over_multi_sample_buckets(self, diff, range_cmd, agg):
        mk_populated(diff, "r:agg", BASE_SAMPLES)
        diff(range_cmd, "r:agg", "-", "+", "AGGREGATION", agg, 1000)

    @pytest.mark.parametrize("agg", AGGREGATORS)
    def test_aggregator_single_sample_bucket(self, diff, range_cmd, agg):
        """One sample per bucket: the sample-variance family's degenerate case."""
        mk_populated(diff, "r:agg1", [(0, 5.0), (1000, 7.0), (2000, 11.0)])
        diff(range_cmd, "r:agg1", "-", "+", "AGGREGATION", agg, 1000)

    @pytest.mark.parametrize("agg", AGGREGATORS)
    def test_aggregator_whole_series_in_one_bucket(self, diff, range_cmd, agg):
        mk_populated(diff, "r:agg_one", BASE_SAMPLES)
        diff(range_cmd, "r:agg_one", "-", "+", "AGGREGATION", agg, 100000)

    # NaN-only buckets. Bucket emission is per-aggregator — the bucket is returned iff that
    # aggregation accepted a sample from it — so an all-NaN bucket is real for the counters
    # that accept NaNs and absent for everything else. Nothing exercised NaN samples here
    # before, which is why a per-bucket emission rule diverged unnoticed.
    NAN_AGGREGATORS = AGGREGATORS + ("countall", "countnan")

    @pytest.mark.parametrize("agg", NAN_AGGREGATORS)
    def test_aggregator_over_all_nan_bucket(self, diff, range_cmd, agg):
        mk_populated(diff, "r:nan", [(1000, "nan"), (2000, "nan"), (3000, "nan")])
        diff(range_cmd, "r:nan", "-", "+", "AGGREGATION", agg, 4000)

    @pytest.mark.parametrize("agg", NAN_AGGREGATORS)
    def test_aggregator_over_all_nan_bucket_with_empty(self, diff, range_cmd, agg):
        """With EMPTY the bucket is emitted either way — but an aggregator that did accept
        the NaNs must report its real count, not the fill."""
        mk_populated(diff, "r:nanE", [(1000, "nan"), (3000, "nan"), (5000, "nan")])
        diff(range_cmd, "r:nanE", 0, 6000, "ALIGN", 0, "AGGREGATION", agg, 2000, "EMPTY")

    @pytest.mark.parametrize("agg", NAN_AGGREGATORS)
    def test_aggregator_over_mixed_nan_bucket(self, diff, range_cmd, agg):
        mk_populated(diff, "r:nanMix", [(1000, 1.0), (2000, "nan"), (3000, 3.0)])
        diff(range_cmd, "r:nanMix", "-", "+", "AGGREGATION", agg, 4000)

    @pytest.mark.parametrize("agg", NAN_AGGREGATORS)
    def test_aggregator_over_ordinary_bucket(self, diff, range_cmd, agg):
        """The converse direction: countnan accepts only NaNs, so a bucket of ordinary
        readings is absent for it while every other aggregator reports it."""
        mk_populated(diff, "r:nanOrd", [(1000, 1.0), (2000, 2.0)])
        diff(range_cmd, "r:nanOrd", "-", "+", "AGGREGATION", agg, 4000)

    def test_bucket_boundary_assignment(self, diff, range_cmd):
        """Samples exactly on, ±1 around, a bucket boundary."""
        mk_populated(
            diff, "r:boundary",
            [(999, 1.0), (1000, 2.0), (1001, 3.0), (1999, 4.0), (2000, 5.0)],
        )
        diff(range_cmd, "r:boundary", "-", "+", "AGGREGATION", "count", 1000)
        diff(range_cmd, "r:boundary", "-", "+", "AGGREGATION", "first", 1000)
        diff(range_cmd, "r:boundary", "-", "+", "AGGREGATION", "last", 1000)

    def test_bucket_duration_one(self, diff, range_cmd):
        mk_populated(diff, "r:bucket1", [(0, 1.0), (1, 2.0), (2, 3.0)])
        diff(range_cmd, "r:bucket1", "-", "+", "AGGREGATION", "sum", 1)

    def test_bucket_duration_zero_rejected(self, diff, range_cmd):
        mk_populated(diff, "r:bucket0", BASE_SAMPLES)
        with pytest.raises(ResponseError):
            diff(range_cmd, "r:bucket0", "-", "+", "AGGREGATION", "avg", 0)

    def test_bucket_duration_negative_rejected(self, diff, range_cmd):
        mk_populated(diff, "r:bucketneg", BASE_SAMPLES)
        with pytest.raises(ResponseError):
            diff(range_cmd, "r:bucketneg", "-", "+", "AGGREGATION", "avg", -1000)

    @pytest.mark.parametrize("agg", UNSUPPORTED_AGGREGATORS)
    def test_unsupported_aggregator_is_rejected_not_wrong(self, diff, range_cmd, agg):
        """DIV-0012: twa is unimplemented. Pin the *shape* of the gap.

        Asserted against each engine directly rather than through `diff`: this
        is a known one-sided mismatch, and the point is that our side fails
        cleanly (a rejection a client can detect) instead of silently returning
        a differently-computed average. Delete this test when twa lands.
        """
        mk_populated(diff, "r:twa", BASE_SAMPLES)
        diff.reference.execute_command(range_cmd, "r:twa", "-", "+", "AGGREGATION", agg, 1000)
        with pytest.raises(ResponseError):
            diff.subject.execute_command(range_cmd, "r:twa", "-", "+", "AGGREGATION", agg, 1000)

    def test_unknown_aggregator_rejected(self, diff, range_cmd):
        mk_populated(diff, "r:aggbad", BASE_SAMPLES)
        with pytest.raises(ResponseError):
            diff(range_cmd, "r:aggbad", "-", "+", "AGGREGATION", "median", 1000)

    def test_aggregator_name_is_case_insensitive(self, diff, range_cmd):
        mk_populated(diff, "r:aggcase", BASE_SAMPLES)
        diff(range_cmd, "r:aggcase", "-", "+", "AGGREGATION", "AVG", 1000)
        diff(range_cmd, "r:aggcase", "-", "+", "AGGREGATION", "Std.P", 1000)
        diff(range_cmd, "r:aggcase", "-", "+", "aggregation", "avg", 1000)


class TestAlign:
    @pytest.mark.parametrize("align", ["-", "+", "start", "end", "START", "End", 250, 0])
    def test_align_variants(self, diff, range_cmd, align):
        mk_populated(diff, "r:align", BASE_SAMPLES)
        diff(range_cmd, "r:align", 250, 3999, "ALIGN", align, "AGGREGATION", "sum", 1000)

    def test_align_shifts_bucket_boundaries(self, diff, range_cmd):
        mk_populated(diff, "r:alignshift", BASE_SAMPLES)
        for align in (0, 100, 500, 999):
            diff(
                range_cmd, "r:alignshift", "-", "+",
                "ALIGN", align, "AGGREGATION", "count", 1000,
            )

    def test_align_without_aggregation_rejected(self, diff, range_cmd):
        mk_populated(diff, "r:alignbare", BASE_SAMPLES)
        with pytest.raises(ResponseError):
            diff(range_cmd, "r:alignbare", "-", "+", "ALIGN", 0)

    def test_align_invalid_value_rejected(self, diff, range_cmd):
        mk_populated(diff, "r:alignbad", BASE_SAMPLES)
        with pytest.raises(ResponseError):
            diff(
                range_cmd, "r:alignbad", "-", "+",
                "ALIGN", "middle", "AGGREGATION", "avg", 1000,
            )


class TestBucketTimestamp:
    # The accepted vocabulary is three aliases pairs: -/start, +/end, ~/mid
    # (case-insensitive). `low`/`high` are NOT accepted — see
    # test_buckettimestamp_invalid_value_rejected.
    @pytest.mark.parametrize("bt", ["-", "+", "~", "mid", "start", "end", "MID", "Start"])
    def test_buckettimestamp_variants(self, diff, range_cmd, bt):
        mk_populated(diff, "r:bt", BASE_SAMPLES)
        diff(
            range_cmd, "r:bt", "-", "+",
            "AGGREGATION", "avg", 1000, "BUCKETTIMESTAMP", bt,
        )

    # BUCKETTIMESTAMP without AGGREGATION is ignored by RTS rather than
    # rejected — covered by TestOptionParsing::test_unknown_or_inapplicable_*.

    @pytest.mark.parametrize("bt", ["center", "low", "high"])
    def test_buckettimestamp_invalid_value_rejected(self, diff, range_cmd, bt):
        mk_populated(diff, "r:btbad", BASE_SAMPLES)
        with pytest.raises(ResponseError):
            diff(
                range_cmd, "r:btbad", "-", "+",
                "AGGREGATION", "avg", 1000, "BUCKETTIMESTAMP", bt,
            )

    def test_buckettimestamp_mid_rounding(self, diff, range_cmd):
        """`~` on an odd bucket duration: the midpoint is not an integer."""
        mk_populated(diff, "r:btmid", [(0, 1.0), (1, 2.0), (2, 3.0), (3, 4.0)])
        diff(
            range_cmd, "r:btmid", "-", "+",
            "AGGREGATION", "avg", 3, "BUCKETTIMESTAMP", "~",
        )


class TestEmpty:
    # first/last are excluded for the reverse direction only: see
    # test_empty_carry_forward_direction and DIV-0016.
    @pytest.mark.parametrize("agg", AGGREGATORS)
    def test_empty_gap_bucket_value_per_aggregator(self, diff, range_cmd, agg):
        """The gap in BASE_SAMPLES ([2000,3000)) must materialize under EMPTY.

        The per-aggregator fill value is exactly the cell the plan calls out,
        and it is not uniform: sum/count fill 0, most fill NaN, and `last`
        carries the previous bucket's value forward.
        """
        if range_cmd == "TS.REVRANGE" and agg in ("first", "last"):
            pytest.skip("DIV-0016: see test_empty_carry_forward_direction")
        mk_populated(diff, "r:empty", BASE_SAMPLES)
        diff(range_cmd, "r:empty", "-", "+", "AGGREGATION", agg, 1000, "EMPTY")

    def test_empty_carry_forward_is_chronological_not_scan_order(self, diff):
        """DIV-0016: `last`'s EMPTY carry-forward follows the wrong direction
        under TS.REVRANGE.

        RTS fills a gap bucket with the previously *emitted* bucket's value, so
        reversing the scan reverses which neighbour is carried: its REVRANGE
        `last` carries the chronologically newer value, and its REVRANGE `first`
        fills NaN. We aggregate forward and reverse the finished buckets, so our
        carry stays chronological and the two come out swapped.

        Forward is byte-identical (test_empty_gap_bucket_value_per_aggregator);
        only the reverse direction diverges.
        """
        mk_populated(diff, "r:emptyrev", BASE_SAMPLES)
        for agg in ("first", "last"):
            reference = diff.reference.execute_command(
                "TS.REVRANGE", "r:emptyrev", "-", "+", "AGGREGATION", agg, 1000, "EMPTY"
            )
            subject = diff.subject.execute_command(
                "TS.REVRANGE", "r:emptyrev", "-", "+", "AGGREGATION", agg, 1000, "EMPTY"
            )
            assert len(reference) == len(subject), (
                "bucket count must still match; only the gap fill diverges"
            )

    def test_empty_does_not_extend_past_data(self, diff, range_cmd):
        """EMPTY fills interior gaps; the queried window is wider than the data."""
        mk_populated(diff, "r:emptyedge", [(1000, 1.0), (5000, 2.0)])
        diff(range_cmd, "r:emptyedge", 0, 9000, "AGGREGATION", "sum", 1000, "EMPTY")

    def test_empty_with_align_and_buckettimestamp(self, diff, range_cmd):
        mk_populated(diff, "r:emptycombo", BASE_SAMPLES)
        diff(
            range_cmd, "r:emptycombo", "-", "+",
            "ALIGN", 500, "AGGREGATION", "avg", 1000,
            "BUCKETTIMESTAMP", "~", "EMPTY",
        )

    # EMPTY without AGGREGATION is ignored by RTS rather than rejected —
    # covered by TestOptionParsing::test_unknown_or_inapplicable_option_rejected.


class TestFilterByTs:
    def test_filter_by_ts_basic(self, diff, range_cmd):
        mk_populated(diff, "r:fbts", BASE_SAMPLES)
        diff(range_cmd, "r:fbts", "-", "+", "FILTER_BY_TS", 250, 1000, 4000)

    def test_filter_by_ts_nonexistent_timestamps(self, diff, range_cmd):
        mk_populated(diff, "r:fbtsmiss", BASE_SAMPLES)
        diff(range_cmd, "r:fbtsmiss", "-", "+", "FILTER_BY_TS", 1, 2, 3)
        diff(range_cmd, "r:fbtsmiss", "-", "+", "FILTER_BY_TS", 250, 999, 1000)

    def test_filter_by_ts_unsorted_and_duplicated(self, diff, range_cmd):
        mk_populated(diff, "r:fbtsdup", BASE_SAMPLES)
        diff(range_cmd, "r:fbtsdup", "-", "+", "FILTER_BY_TS", 4000, 250, 1000, 250)

    def test_filter_by_ts_intersects_the_range_window(self, diff, range_cmd):
        mk_populated(diff, "r:fbtswin", BASE_SAMPLES)
        # 4000 is listed but outside [0,1500]: the window still applies.
        diff(range_cmd, "r:fbtswin", 0, 1500, "FILTER_BY_TS", 250, 4000)

    def test_filter_by_ts_with_aggregation(self, diff, range_cmd):
        mk_populated(diff, "r:fbtsagg", BASE_SAMPLES)
        diff(
            range_cmd, "r:fbtsagg", "-", "+",
            "FILTER_BY_TS", 0, 250, 3000, "AGGREGATION", "sum", 1000,
        )

    def test_filter_by_ts_requires_a_timestamp(self, diff, range_cmd):
        mk_populated(diff, "r:fbtsempty", BASE_SAMPLES)
        with pytest.raises(ResponseError):
            diff(range_cmd, "r:fbtsempty", "-", "+", "FILTER_BY_TS")

    def test_filter_by_ts_invalid_timestamp_rejected(self, diff, range_cmd):
        mk_populated(diff, "r:fbtsbad", BASE_SAMPLES)
        with pytest.raises(ResponseError):
            diff(range_cmd, "r:fbtsbad", "-", "+", "FILTER_BY_TS", "abc")


class TestFilterByValue:
    def test_filter_by_value_basic(self, diff, range_cmd):
        mk_populated(diff, "r:fbv", BASE_SAMPLES)
        diff(range_cmd, "r:fbv", "-", "+", "FILTER_BY_VALUE", 2.0, 16.0)

    def test_filter_by_value_bounds_are_inclusive(self, diff, range_cmd):
        mk_populated(diff, "r:fbvincl", BASE_SAMPLES)
        diff(range_cmd, "r:fbvincl", "-", "+", "FILTER_BY_VALUE", 8.0, 8.0)
        diff(range_cmd, "r:fbvincl", "-", "+", "FILTER_BY_VALUE", 8.001, 15.999)

    def test_filter_by_value_min_greater_than_max(self, diff, range_cmd):
        mk_populated(diff, "r:fbvinv", BASE_SAMPLES)
        diff(range_cmd, "r:fbvinv", "-", "+", "FILTER_BY_VALUE", 100.0, 1.0)

    def test_filter_by_value_negative_and_zero(self, diff, range_cmd):
        mk_populated(diff, "r:fbvneg", [(0, -5.0), (100, 0.0), (200, 5.0)])
        diff(range_cmd, "r:fbvneg", "-", "+", "FILTER_BY_VALUE", -5.0, 0.0)
        diff(range_cmd, "r:fbvneg", "-", "+", "FILTER_BY_VALUE", 0.0, 0.0)

    def test_filter_by_value_with_aggregation(self, diff, range_cmd):
        mk_populated(diff, "r:fbvagg", BASE_SAMPLES)
        diff(
            range_cmd, "r:fbvagg", "-", "+",
            "FILTER_BY_VALUE", 1.0, 16.0, "AGGREGATION", "count", 1000,
        )

    def test_filter_by_value_missing_operand_rejected(self, diff, range_cmd):
        mk_populated(diff, "r:fbvmiss", BASE_SAMPLES)
        with pytest.raises(ResponseError):
            diff(range_cmd, "r:fbvmiss", "-", "+", "FILTER_BY_VALUE", 1.0)

    def test_filter_by_value_invalid_operand_rejected(self, diff, range_cmd):
        mk_populated(diff, "r:fbvbad", BASE_SAMPLES)
        with pytest.raises(ResponseError):
            diff(range_cmd, "r:fbvbad", "-", "+", "FILTER_BY_VALUE", "low", "high")

    def test_filter_by_ts_and_value_combined(self, diff, range_cmd):
        mk_populated(diff, "r:fbboth", BASE_SAMPLES)
        diff(
            range_cmd, "r:fbboth", "-", "+",
            "FILTER_BY_TS", 250, 1000, 1500, "FILTER_BY_VALUE", 2.0, 8.0,
        )


class TestLatest:
    """LATEST on a compaction target with an open (unfinalized) bucket."""

    def _rule(self, diff):
        mk_series(diff, "r:latest:src")
        mk_series(diff, "r:latest:dst")
        diff("TS.CREATERULE", "r:latest:src", "r:latest:dst", "AGGREGATION", "sum", 1000)
        # Buckets [0,1000) and [1000,2000) are closed by the 2500 write; the
        # bucket containing 2500 stays open.
        for ts, value in [(0, 1.0), (500, 2.0), (1000, 4.0), (1500, 8.0), (2500, 16.0)]:
            diff("TS.ADD", "r:latest:src", ts, value)

    def test_latest_on_compaction_target(self, diff, range_cmd):
        self._rule(diff)
        diff(range_cmd, "r:latest:dst", "-", "+")
        diff(range_cmd, "r:latest:dst", "-", "+", "LATEST")

    def test_latest_is_ignored_on_a_plain_series(self, diff, range_cmd):
        mk_populated(diff, "r:latestplain", BASE_SAMPLES)
        diff(range_cmd, "r:latestplain", "-", "+", "LATEST")

    def test_latest_respects_the_range_window(self, diff, range_cmd):
        self._rule(diff)
        # The open bucket's timestamp (2000) is outside this window.
        diff(range_cmd, "r:latest:dst", 0, 1500, "LATEST")

    def test_latest_with_aggregation_on_target(self, diff, range_cmd):
        self._rule(diff)
        diff(
            range_cmd, "r:latest:dst", "-", "+",
            "LATEST", "AGGREGATION", "max", 2000,
        )

    def test_latest_on_empty_source_bucket(self, diff, range_cmd):
        mk_series(diff, "r:latest2:src")
        mk_series(diff, "r:latest2:dst")
        diff("TS.CREATERULE", "r:latest2:src", "r:latest2:dst", "AGGREGATION", "avg", 1000)
        diff(range_cmd, "r:latest2:dst", "-", "+", "LATEST")
        diff("TS.ADD", "r:latest2:src", 100, 1.0)
        diff(range_cmd, "r:latest2:dst", "-", "+", "LATEST")


class TestOptionParsing:
    def test_option_names_are_case_insensitive(self, diff, range_cmd):
        mk_populated(diff, "r:case", BASE_SAMPLES)
        diff(range_cmd, "r:case", "-", "+", "count", 3)
        diff(range_cmd, "r:case", "-", "+", "Filter_By_Value", 1.0, 8.0)
        diff(
            range_cmd, "r:case", 0, 4000,
            "align", "-", "aggregation", "avg", 1000, "buckettimestamp", "+", "empty",
        )

    def test_duplicated_option(self, diff, range_cmd):
        """DIV-0014: neither engine errors on a repeated option; they disagree
        on which copy wins (RTS keeps the first, we keep the last)."""
        mk_populated(diff, "r:dupopt", BASE_SAMPLES)
        for args in (
            ("COUNT", 5, "COUNT", 2),
            ("AGGREGATION", "sum", 1000, "AGGREGATION", "avg", 2000),
        ):
            reference = diff.reference.execute_command(range_cmd, "r:dupopt", "-", "+", *args)
            subject = diff.subject.execute_command(range_cmd, "r:dupopt", "-", "+", *args)
            assert reference and subject, "both engines accept a repeated option"

    def test_missing_option_value(self, diff, range_cmd):
        mk_populated(diff, "r:missingval", BASE_SAMPLES)
        for tail in (["COUNT"], ["ALIGN"], ["AGGREGATION"], ["AGGREGATION", "avg"]):
            with pytest.raises(ResponseError):
                diff(range_cmd, "r:missingval", "-", "+", *tail)

    @pytest.mark.parametrize(
        "tail",
        [
            ["GARBAGE"],                    # not a token at all
            ["WITHLABELS"],                 # a real token, but not for TS.RANGE
            ["EMPTY"],                      # valid only alongside AGGREGATION
            ["BUCKETTIMESTAMP", "-"],       # ditto
        ],
    )
    def test_unknown_or_inapplicable_option_rejected(self, diff, range_cmd, tail):
        """DIV-0015: we reject what RTS 8.8 silently ignores.

        RTS's range parser drops trailing tokens it does not recognize or can
        not apply — `TS.RANGE k - + GARBAGE` returns samples. We reject them.
        Per-engine assertions because this is an over-strict rejection, and the
        registry can only scope it by a regex broad enough to mask real
        regressions in the same delta class.
        """
        mk_populated(diff, "r:unknownopt", BASE_SAMPLES)
        diff.reference.execute_command(range_cmd, "r:unknownopt", "-", "+", *tail)
        with pytest.raises(ResponseError):
            diff.subject.execute_command(range_cmd, "r:unknownopt", "-", "+", *tail)

    def test_missing_required_args(self, diff, range_cmd):
        mk_populated(diff, "r:arity", BASE_SAMPLES)
        with pytest.raises(ResponseError):
            diff(range_cmd)
        with pytest.raises(ResponseError):
            diff(range_cmd, "r:arity")
        with pytest.raises(ResponseError):
            diff(range_cmd, "r:arity", "-")

    def test_invalid_timestamp_bounds(self, diff, range_cmd):
        mk_populated(diff, "r:badts", BASE_SAMPLES)
        with pytest.raises(ResponseError):
            diff(range_cmd, "r:badts", "not-a-ts", "+")
        with pytest.raises(ResponseError):
            diff(range_cmd, "r:badts", "-", "not-a-ts")
        # A bare negative integer is an absolute timestamp, and rejected as one
        # — not silently reinterpreted as a relative "1000ms ago" offset.
        with pytest.raises(ResponseError):
            diff(range_cmd, "r:badts", -1000, 1000)
        with pytest.raises(ResponseError):
            diff(range_cmd, "r:badts", "-", -5)

    def test_relative_and_now_bounds_are_an_accepted_input_superset(self, diff, range_cmd):
        """DIV-0013: we accept range bounds RTS rejects.

        `*` (now) and unit-suffixed offsets (`-1h`) are deliberate extensions to
        the bound grammar; RTS 8.8 answers both with "wrong fromTimestamp".
        Asserted per-engine because plan §5.2 makes an accepted-input superset
        non-registrable — the harness hard-fails it through `diff` by design, so
        this test states the gap explicitly instead of hiding it.
        """
        mk_populated(diff, "r:super", BASE_SAMPLES)
        for bound in ("*", "-1h"):
            with pytest.raises(ResponseError):
                diff.reference.execute_command(range_cmd, "r:super", bound, "+")
            diff.subject.execute_command(range_cmd, "r:super", bound, "+")

    def test_full_option_combination(self, diff, range_cmd):
        mk_populated(diff, "r:combo", BASE_SAMPLES)
        # ALIGN start needs an explicit start bound (both engines enforce that).
        diff(
            range_cmd, "r:combo", 0, 4000,
            "FILTER_BY_TS", 0, 250, 750, 1000, 1500, 3000, 3999, 4000,
            "FILTER_BY_VALUE", 1.0, 64.0,
            "COUNT", 4,
            "ALIGN", "start",
            "AGGREGATION", "avg", 1000,
            "BUCKETTIMESTAMP", "~",
            "EMPTY",
        )


class TestKeyStates:
    def test_missing_key(self, diff, range_cmd):
        with pytest.raises(ResponseError):
            diff(range_cmd, "r:nonexistent", "-", "+")

    def test_wrongtype(self, diff, range_cmd):
        diff("SET", "r:string", "hello")
        with pytest.raises(ResponseError):
            diff(range_cmd, "r:string", "-", "+")

    def test_empty_series(self, diff, range_cmd):
        mk_series(diff, "r:emptyseries")
        diff(range_cmd, "r:emptyseries", "-", "+")
        diff(range_cmd, "r:emptyseries", "-", "+", "COUNT", 5)
        diff(range_cmd, "r:emptyseries", "-", "+", "AGGREGATION", "avg", 1000)
        diff(range_cmd, "r:emptyseries", "-", "+", "AGGREGATION", "avg", 1000, "EMPTY")

    def test_single_sample_series(self, diff, range_cmd):
        mk_populated(diff, "r:single", [(1000, 42.0)])
        diff(range_cmd, "r:single", "-", "+")
        for agg in ("avg", "count", "std.s", "var.s"):
            diff(range_cmd, "r:single", "-", "+", "AGGREGATION", agg, 1000)


class TestValueFormatting:
    """Float formatting is the classic divergence source (plan §5.1, §11)."""

    def test_special_values(self, diff, range_cmd):
        mk_populated(
            diff, "r:floats",
            [
                (0, 0.0),
                (100, -0.0),
                (200, 1e-300),
                (300, 1e300),
                (400, 0.1),
                (500, 1.0 / 3.0),
                (600, 123456789.000001),
                (700, -1.5e-8),
            ],
        )
        diff(range_cmd, "r:floats", "-", "+")

    def test_aggregation_produces_repeating_decimals(self, diff, range_cmd):
        mk_populated(diff, "r:repeating", [(0, 1.0), (1, 1.0), (2, 2.0)])
        diff(range_cmd, "r:repeating", "-", "+", "AGGREGATION", "avg", 1000)


class TestAggregationOverflow:
    """DIV-0022: the variance/stddev family overflows to NaN on RTS for huge magnitudes.

    Pinned per-engine instead of via `diff`, because the only registry regex that could
    cover it ("reference='NaN' subject=<n>") would also absorb a real aggregation bug
    returning a number where RTS returns NaN (plan §5.3; see tests/compat/README.md
    "Divergences the registry can not express").
    """

    # sqrt(DBL_MAX) ~= 1.34e154: above this, RTS's naive sum-of-squares overflows to
    # +Inf and the Inf - Inf term yields NaN.
    VARIANCE_AGGREGATORS = ("std.p", "std.s", "var.p", "var.s")

    @pytest.mark.parametrize("agg", ("std.p", "var.p"))
    def test_variance_family_overflows_on_rts(self, diff, agg):
        """A single sample has zero variance; RTS reports NaN once x**2 overflows."""
        for client in (diff.reference, diff.subject):
            client.execute_command("TS.CREATE", "r:ovf")
            client.execute_command("TS.ADD", "r:ovf", 0, "1.7976931348623157e308")

        ref = diff.reference.execute_command(
            "TS.RANGE", "r:ovf", "-", "+", "AGGREGATION", agg, 500
        )
        sub = diff.subject.execute_command(
            "TS.RANGE", "r:ovf", "-", "+", "AGGREGATION", agg, 500
        )
        assert float(ref[0][1]) != float(ref[0][1]), f"expected RTS NaN, got {ref!r}"
        assert float(sub[0][1]) == 0.0, f"expected a correct 0.0, got {sub!r}"

    @pytest.mark.parametrize("agg", ("std.p", "var.p"))
    def test_below_the_overflow_boundary_agrees(self, diff, agg):
        """1e150 squares to 1e300 (finite), so both engines agree — this is the delta
        class DIV-0022 must not be allowed to mask."""
        mk_populated(diff, "r:safe", [(0, 1e150)])
        diff("TS.RANGE", "r:safe", "-", "+", "AGGREGATION", agg, 500)

    def test_non_variance_aggregators_agree_at_dbl_max(self, diff):
        """avg/sum/min/max do not accumulate squares, so DBL_MAX is fine on both."""
        mk_populated(diff, "r:big", [(0, 1.7976931348623157e308)])
        for agg in ("avg", "sum", "min", "max", "count", "first", "last"):
            diff("TS.RANGE", "r:big", "-", "+", "AGGREGATION", agg, 500)
