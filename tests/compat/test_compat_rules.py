"""TS.CREATERULE / TS.DELETERULE argument and aggregator surface
(test plan §6, TS.CREATERULE and TS.DELETERULE rows).

Rule *semantics* — bucket finalization, out-of-order writes, DEL and retention
interaction, reload — live in test_compat_compaction.py. This module covers the
command surface those tests take for granted: every aggregator accepted as a
rule, bucket-duration and alignTimestamp validation, duplicate and conflicting
rule creation, what TS.INFO reports on both ends of a rule, and the error paths.

`twa` is excluded from the aggregator parametrization for the reason given in
compat_helpers.UNSUPPORTED_AGGREGATORS (DIV-0012) and pinned separately.

Written clean-room from public RedisTimeSeries documentation and black-box
observation of the reference server. Do NOT consult RedisTimeSeries source or
test code (see tests/compat/README.md).
"""

from __future__ import annotations

import pytest
from valkey.exceptions import ResponseError

from compat_helpers import AGGREGATORS, UNSUPPORTED_AGGREGATORS, mk_populated, mk_series
from test_compat_create import _errors

SOURCE_SAMPLES = [(100, 1.0), (150, 2.0), (250, 4.0), (275, 5.0), (350, 8.0)]


def _pair(diff, name):
    mk_series(diff, f"g:{name}:src")
    mk_series(diff, f"g:{name}:dst")
    return f"g:{name}:src", f"g:{name}:dst"


class TestAggregators:
    @pytest.mark.parametrize("agg", AGGREGATORS)
    def test_rule_for_each_aggregator(self, diff, agg):
        src, dst = _pair(diff, "agg")
        diff("TS.CREATERULE", src, dst, "AGGREGATION", agg, 100)
        diff("TS.INFO", src)
        diff("TS.INFO", dst)
        for ts, value in SOURCE_SAMPLES:
            diff("TS.ADD", src, ts, value)
        diff("TS.RANGE", dst, "-", "+")

    @pytest.mark.parametrize("agg", AGGREGATORS)
    def test_aggregator_name_is_case_insensitive(self, diff, agg):
        src, dst = _pair(diff, "case")
        diff("TS.CREATERULE", src, dst, "AGGREGATION", agg.upper(), 100)
        diff("TS.INFO", src)

    @pytest.mark.parametrize("agg", UNSUPPORTED_AGGREGATORS)
    def test_unsupported_aggregator_is_rejected_not_wrong(self, diff, agg):
        """DIV-0012: `twa` is not implemented. It must be an error, never a rule
        that silently computes something else."""
        src, dst = _pair(diff, "unsup")
        assert diff.reference.execute_command(
            "TS.CREATERULE", src, dst, "AGGREGATION", agg, 100
        ) == b"OK"
        assert _errors(diff.subject, "TS.CREATERULE", src, dst, "AGGREGATION", agg, 100)

    @pytest.mark.parametrize("agg", ["median", "", "average", "p99"])
    def test_unknown_aggregator_rejected(self, diff, agg):
        src, dst = _pair(diff, "unknown")
        with pytest.raises(ResponseError):
            diff("TS.CREATERULE", src, dst, "AGGREGATION", agg, 100)
        diff("TS.INFO", src)


class TestBucketDuration:
    @pytest.mark.parametrize("duration", [1, 100, 86400000])
    def test_accepted_durations(self, diff, duration):
        src, dst = _pair(diff, "dur")
        diff("TS.CREATERULE", src, dst, "AGGREGATION", "avg", duration)
        diff("TS.INFO", src)

    @pytest.mark.parametrize("duration", [0, -1, -100])
    def test_rejected_durations(self, diff, duration):
        src, dst = _pair(diff, "dur:bad")
        with pytest.raises(ResponseError):
            diff("TS.CREATERULE", src, dst, "AGGREGATION", "avg", duration)
        diff("TS.INFO", src)

    @pytest.mark.parametrize("duration", ["abc", ""])
    def test_unparseable_duration_rejected(self, diff, duration):
        src, dst = _pair(diff, "dur:parse")
        with pytest.raises(ResponseError):
            diff("TS.CREATERULE", src, dst, "AGGREGATION", "avg", duration)

    def test_missing_duration(self, diff):
        src, dst = _pair(diff, "dur:missing")
        with pytest.raises(ResponseError):
            diff("TS.CREATERULE", src, dst, "AGGREGATION", "avg")


class TestAlignTimestamp:
    @pytest.mark.parametrize("align", [0, 1, 50, 99])
    def test_accepted_alignments(self, diff, align):
        src, dst = _pair(diff, "align")
        diff("TS.CREATERULE", src, dst, "AGGREGATION", "sum", 100, align)
        diff("TS.INFO", src)
        for ts, value in SOURCE_SAMPLES:
            diff("TS.ADD", src, ts, value)
        diff("TS.RANGE", dst, "-", "+")

    def test_alignment_larger_than_the_bucket(self, diff):
        src, dst = _pair(diff, "align:big")
        diff("TS.CREATERULE", src, dst, "AGGREGATION", "sum", 100, 250)
        for ts, value in SOURCE_SAMPLES:
            diff("TS.ADD", src, ts, value)
        diff("TS.RANGE", dst, "-", "+")

    @pytest.mark.parametrize("align", ["abc", ""])
    def test_unparseable_alignment_rejected(self, diff, align):
        src, dst = _pair(diff, "align:bad")
        with pytest.raises(ResponseError):
            diff("TS.CREATERULE", src, dst, "AGGREGATION", "sum", 100, align)

    def test_negative_alignment(self, diff):
        src, dst = _pair(diff, "align:neg")
        try:
            diff("TS.CREATERULE", src, dst, "AGGREGATION", "sum", 100, -1)
        except ResponseError:
            pass
        diff("TS.INFO", src)


class TestArgumentParsing:
    def test_aggregation_keyword_is_case_insensitive(self, diff):
        src, dst = _pair(diff, "kw")
        diff("TS.CREATERULE", src, dst, "aggregation", "avg", 100)
        diff("TS.INFO", src)

    def test_missing_aggregation_keyword(self, diff):
        src, dst = _pair(diff, "kw:missing")
        with pytest.raises(ResponseError):
            diff("TS.CREATERULE", src, dst, "avg", 100)

    @pytest.mark.parametrize("args", [(), ("g:only:src",)])
    def test_missing_keys(self, diff, args):
        with pytest.raises(ResponseError):
            diff("TS.CREATERULE", *args)

    def test_deleterule_missing_arguments(self, diff):
        with pytest.raises(ResponseError):
            diff("TS.DELETERULE", "g:x:src")


class TestRuleCreation:
    def test_info_reports_the_rule_on_both_ends(self, diff):
        src, dst = _pair(diff, "info")
        diff("TS.CREATERULE", src, dst, "AGGREGATION", "avg", 100, 25)
        diff("TS.INFO", src)
        diff("TS.INFO", dst)

    def test_duplicate_rule_rejected(self, diff):
        src, dst = _pair(diff, "dup")
        diff("TS.CREATERULE", src, dst, "AGGREGATION", "avg", 100)
        with pytest.raises(ResponseError):
            diff("TS.CREATERULE", src, dst, "AGGREGATION", "avg", 100)
        diff("TS.INFO", src)

    def test_second_rule_to_the_same_destination_rejected(self, diff):
        """A destination may only be fed by one rule."""
        mk_series(diff, "g:two:src1")
        mk_series(diff, "g:two:src2")
        mk_series(diff, "g:two:dst")
        diff("TS.CREATERULE", "g:two:src1", "g:two:dst", "AGGREGATION", "avg", 100)
        with pytest.raises(ResponseError):
            diff("TS.CREATERULE", "g:two:src2", "g:two:dst", "AGGREGATION", "avg", 100)
        diff("TS.INFO", "g:two:dst")

    def test_differing_rules_to_distinct_destinations(self, diff):
        mk_series(diff, "g:multi:src")
        mk_series(diff, "g:multi:d1")
        mk_series(diff, "g:multi:d2")
        diff("TS.CREATERULE", "g:multi:src", "g:multi:d1", "AGGREGATION", "min", 100)
        diff("TS.CREATERULE", "g:multi:src", "g:multi:d2", "AGGREGATION", "max", 200)
        diff("TS.INFO", "g:multi:src")
        for ts, value in SOURCE_SAMPLES:
            diff("TS.ADD", "g:multi:src", ts, value)
        diff("TS.RANGE", "g:multi:d1", "-", "+")
        diff("TS.RANGE", "g:multi:d2", "-", "+")

    def test_destination_with_existing_data(self, diff):
        mk_populated(diff, "g:dstdata:src", SOURCE_SAMPLES)
        mk_populated(diff, "g:dstdata:dst", [(50, 99.0)])
        diff("TS.CREATERULE", "g:dstdata:src", "g:dstdata:dst", "AGGREGATION", "sum", 100)
        diff("TS.RANGE", "g:dstdata:dst", "-", "+")
        diff("TS.ADD", "g:dstdata:src", 450, 1.0)
        diff("TS.RANGE", "g:dstdata:dst", "-", "+")

    def test_wrongtype_source_and_destination(self, diff):
        diff("SET", "g:wt:string", "hello")
        mk_series(diff, "g:wt:series")
        with pytest.raises(ResponseError):
            diff("TS.CREATERULE", "g:wt:string", "g:wt:series", "AGGREGATION", "avg", 100)
        with pytest.raises(ResponseError):
            diff("TS.CREATERULE", "g:wt:series", "g:wt:string", "AGGREGATION", "avg", 100)


class TestRuleDeletion:
    def test_deleterule_removes_it_from_info(self, diff):
        src, dst = _pair(diff, "del")
        diff("TS.CREATERULE", src, dst, "AGGREGATION", "avg", 100)
        diff("TS.DELETERULE", src, dst)
        diff("TS.INFO", src)
        diff("TS.INFO", dst)

    def test_deleterule_leaves_the_destination_data(self, diff):
        src, dst = _pair(diff, "del:data")
        diff("TS.CREATERULE", src, dst, "AGGREGATION", "sum", 100)
        for ts, value in SOURCE_SAMPLES:
            diff("TS.ADD", src, ts, value)
        diff("TS.DELETERULE", src, dst)
        diff("TS.RANGE", dst, "-", "+")

    def test_deleterule_for_a_pair_with_no_rule(self, diff):
        src, dst = _pair(diff, "del:norule")
        with pytest.raises(ResponseError):
            diff("TS.DELETERULE", src, dst)

    def test_deleterule_with_missing_keys(self, diff):
        mk_series(diff, "g:del:exists")
        with pytest.raises(ResponseError):
            diff("TS.DELETERULE", "g:del:absent", "g:del:exists")
        with pytest.raises(ResponseError):
            diff("TS.DELETERULE", "g:del:exists", "g:del:absent")

    def test_deleterule_then_recreate(self, diff):
        src, dst = _pair(diff, "del:recreate")
        diff("TS.CREATERULE", src, dst, "AGGREGATION", "avg", 100)
        diff("TS.DELETERULE", src, dst)
        diff("TS.CREATERULE", src, dst, "AGGREGATION", "max", 200)
        diff("TS.INFO", src)
        for ts, value in SOURCE_SAMPLES:
            diff("TS.ADD", src, ts, value)
        diff("TS.RANGE", dst, "-", "+")

    def test_deleterule_only_removes_the_named_pair(self, diff):
        mk_series(diff, "g:del:multi:src")
        mk_series(diff, "g:del:multi:d1")
        mk_series(diff, "g:del:multi:d2")
        diff("TS.CREATERULE", "g:del:multi:src", "g:del:multi:d1", "AGGREGATION", "min", 100)
        diff("TS.CREATERULE", "g:del:multi:src", "g:del:multi:d2", "AGGREGATION", "max", 100)
        diff("TS.DELETERULE", "g:del:multi:src", "g:del:multi:d1")
        diff("TS.INFO", "g:del:multi:src")
        for ts, value in SOURCE_SAMPLES:
            diff("TS.ADD", "g:del:multi:src", ts, value)
        diff("TS.RANGE", "g:del:multi:d1", "-", "+")
        diff("TS.RANGE", "g:del:multi:d2", "-", "+")
