"""TS.MADD parity matrix (test plan §6, TS.MADD row).

Covers partial-failure semantics (per-item errors embedded in the reply array and
their ordering), the same key repeated within one call, interaction with each
duplicate policy, and the auto-create / key-state dimensions.

TS.MADD's reply is an array whose entries are either the stored timestamp or an
error, so the reply itself carries most of the contract; the normalizer compares
embedded errors as `ErrorReply` values, so a per-item error on one side and a
timestamp on the other is a reply-level delta rather than a raised exception.

The fuzzer already pinned several TS.MADD orderings as corpus entries
(`corpus/madd_*.json`); this module is the systematic matrix around them.

Written clean-room from public RedisTimeSeries documentation and black-box
observation of the reference server. Do NOT consult RedisTimeSeries source or
test code (see tests/compat/README.md).
"""

from __future__ import annotations

import pytest
from valkey.exceptions import ResponseError

from compat_helpers import mk_populated, mk_series
from test_compat_create import DUPLICATE_POLICIES, _errors


class TestBasicMadd:
    def test_single_item(self, diff):
        mk_series(diff, "e:one")
        diff("TS.MADD", "e:one", 100, 1.0)
        diff("TS.RANGE", "e:one", "-", "+")

    def test_multiple_keys(self, diff):
        mk_series(diff, "e:a")
        mk_series(diff, "e:b")
        diff("TS.MADD", "e:a", 100, 1.0, "e:b", 100, 2.0, "e:a", 200, 3.0)
        diff("TS.RANGE", "e:a", "-", "+")
        diff("TS.RANGE", "e:b", "-", "+")

    def test_reply_order_follows_argument_order(self, diff):
        mk_series(diff, "e:order:1")
        mk_series(diff, "e:order:2")
        diff(
            "TS.MADD",
            "e:order:2", 300, 3.0,
            "e:order:1", 100, 1.0,
            "e:order:2", 100, 2.0,
        )

    def test_out_of_order_within_one_call(self, diff):
        mk_series(diff, "e:ooo")
        diff("TS.MADD", "e:ooo", 300, 3.0, "e:ooo", 100, 1.0, "e:ooo", 200, 2.0)
        diff("TS.RANGE", "e:ooo", "-", "+")
        diff("TS.INFO", "e:ooo")

    def test_star_timestamp(self, diff):
        """Server clocks differ, so only the effect is comparable."""
        mk_series(diff, "e:star")
        ref, sub = diff.both("TS.MADD", "e:star", "*", 1.0)
        assert isinstance(ref[0], int) and isinstance(sub[0], int)
        assert (
            diff.reference.execute_command("TS.RANGE", "e:star", "-", "+")[0][1]
            == diff.subject.execute_command("TS.RANGE", "e:star", "-", "+")[0][1]
        )

    def test_timestamp_zero(self, diff):
        mk_series(diff, "e:zero")
        diff("TS.MADD", "e:zero", 0, 1.0)
        diff("TS.RANGE", "e:zero", "-", "+")


class TestArity:
    @pytest.mark.parametrize("args", [(), ("e:x",), ("e:x", 100)])
    def test_incomplete_triples_rejected(self, diff, args):
        mk_series(diff, "e:x")
        with pytest.raises(ResponseError):
            diff("TS.MADD", *args)

    def test_trailing_partial_triple_rejected(self, diff):
        """A trailing incomplete triple must reject the whole command, not apply
        the complete prefix."""
        mk_series(diff, "e:partial")
        with pytest.raises(ResponseError):
            diff("TS.MADD", "e:partial", 100, 1.0, "e:partial", 200)
        diff("TS.RANGE", "e:partial", "-", "+")


class TestPartialFailure:
    def test_missing_key_is_a_per_item_error(self, diff):
        """TS.MADD does not create the series (unlike TS.ADD): a missing key is a
        per-item error and the keyspace is left alone."""
        mk_series(diff, "e:pf:ok")
        diff("TS.MADD", "e:pf:ok", 100, 1.0, "e:pf:missing", 100, 2.0)
        diff("TS.RANGE", "e:pf:ok", "-", "+")
        diff("EXISTS", "e:pf:missing")

    def test_wrongtype_is_a_per_item_error(self, diff):
        mk_series(diff, "e:pf:ok2")
        diff("SET", "e:pf:string", "hello")
        diff("TS.MADD", "e:pf:ok2", 100, 1.0, "e:pf:string", 100, 2.0)
        diff("TS.RANGE", "e:pf:ok2", "-", "+")

    def test_bad_value_is_a_per_item_error(self, diff):
        mk_series(diff, "e:pf:val")
        diff("TS.MADD", "e:pf:val", 100, 1.0, "e:pf:val", 200, "abc")
        diff("TS.RANGE", "e:pf:val", "-", "+")

    def test_negative_timestamp_is_a_per_item_error(self, diff):
        mk_series(diff, "e:pf:ts")
        diff("TS.MADD", "e:pf:ts", 100, 1.0, "e:pf:ts", -5, 2.0)
        diff("TS.RANGE", "e:pf:ts", "-", "+")

    def test_error_position_matches_item_position(self, diff):
        """Errors must appear at the failing item's index, not be collected."""
        mk_series(diff, "e:pf:pos")
        diff(
            "TS.MADD",
            "e:pf:pos", 100, 1.0,
            "e:pf:nokey", 100, 2.0,
            "e:pf:pos", 200, 3.0,
            "e:pf:nokey2", 100, 4.0,
        )
        diff("TS.RANGE", "e:pf:pos", "-", "+")

    def test_every_item_failing(self, diff):
        diff("TS.MADD", "e:pf:none1", 100, 1.0, "e:pf:none2", 100, 2.0)
        diff("EXISTS", "e:pf:none1")
        diff("EXISTS", "e:pf:none2")

    def test_retention_rejection_is_a_per_item_error(self, diff):
        diff("TS.CREATE", "e:pf:ret", "RETENTION", 1000)
        diff("TS.MADD", "e:pf:ret", 5000, 1.0)
        diff("TS.MADD", "e:pf:ret", 100, 2.0, "e:pf:ret", 5100, 3.0)
        diff("TS.RANGE", "e:pf:ret", "-", "+")


class TestDuplicatePolicy:
    @pytest.mark.parametrize("policy", DUPLICATE_POLICIES)
    def test_duplicate_against_stored_sample(self, diff, policy):
        diff("TS.CREATE", "e:dp", "DUPLICATE_POLICY", policy)
        diff("TS.MADD", "e:dp", 100, 10.0)
        diff("TS.MADD", "e:dp", 100, 3.0)
        diff("TS.RANGE", "e:dp", "-", "+")

    @pytest.mark.parametrize("policy", DUPLICATE_POLICIES)
    def test_duplicate_timestamps_within_one_call(self, diff, policy):
        """Two items for the same key and timestamp in a single TS.MADD: the second
        is folded by the series policy exactly as a separate call would be."""
        diff("TS.CREATE", "e:dp:batch", "DUPLICATE_POLICY", policy)
        diff("TS.MADD", "e:dp:batch", 100, 10.0, "e:dp:batch", 100, 3.0)
        diff("TS.RANGE", "e:dp:batch", "-", "+")
        diff("TS.INFO", "e:dp:batch")

    @pytest.mark.parametrize("policy", DUPLICATE_POLICIES)
    def test_three_way_duplicate_within_one_call(self, diff, policy):
        diff("TS.CREATE", "e:dp:batch3", "DUPLICATE_POLICY", policy)
        diff(
            "TS.MADD",
            "e:dp:batch3", 100, 1.0,
            "e:dp:batch3", 100, 2.0,
            "e:dp:batch3", 100, 4.0,
        )
        diff("TS.RANGE", "e:dp:batch3", "-", "+")

    def test_same_key_repeated_with_distinct_timestamps(self, diff):
        mk_series(diff, "e:repeat")
        diff(
            "TS.MADD",
            "e:repeat", 100, 1.0,
            "e:repeat", 200, 2.0,
            "e:repeat", 300, 3.0,
        )
        diff("TS.RANGE", "e:repeat", "-", "+")
        diff("TS.INFO", "e:repeat")

    def test_ignore_filter_applies_within_a_batch(self, diff):
        diff("TS.CREATE", "e:ign", "DUPLICATE_POLICY", "LAST", "IGNORE", 10, 1.0)
        diff("TS.MADD", "e:ign", 1000, 5.0, "e:ign", 1005, 5.5, "e:ign", 1100, 6.0)
        diff("TS.RANGE", "e:ign", "-", "+")




class TestCompactionInteraction:
    def test_batch_feeds_the_compaction_rule(self, diff):
        mk_series(diff, "e:comp:src")
        mk_series(diff, "e:comp:dst")
        diff("TS.CREATERULE", "e:comp:src", "e:comp:dst", "AGGREGATION", "sum", 100)
        diff(
            "TS.MADD",
            "e:comp:src", 100, 1.0,
            "e:comp:src", 150, 2.0,
            "e:comp:src", 250, 4.0,
            "e:comp:src", 350, 8.0,
        )
        diff("TS.RANGE", "e:comp:dst", "-", "+")
        diff("TS.INFO", "e:comp:dst")

    def test_in_batch_duplicates_do_not_double_count_downstream(self, diff):
        diff("TS.CREATE", "e:comp2:src", "DUPLICATE_POLICY", "SUM")
        mk_series(diff, "e:comp2:dst")
        diff("TS.CREATERULE", "e:comp2:src", "e:comp2:dst", "AGGREGATION", "sum", 100)
        diff(
            "TS.MADD",
            "e:comp2:src", 100, 1.0,
            "e:comp2:src", 100, 2.0,
            "e:comp2:src", 250, 4.0,
        )
        diff("TS.RANGE", "e:comp2:dst", "-", "+")

    def test_backfill_within_a_batch_recalculates_the_bucket(self, diff):
        mk_populated(diff, "e:comp3:src", [(100, 1.0), (250, 4.0)])
        mk_series(diff, "e:comp3:dst")
        diff("TS.CREATERULE", "e:comp3:src", "e:comp3:dst", "AGGREGATION", "sum", 100)
        diff("TS.MADD", "e:comp3:src", 350, 8.0, "e:comp3:src", 150, 2.0)
        diff("TS.RANGE", "e:comp3:dst", "-", "+")


class TestMaddDivergences:
    """Pinned per-engine; see TestParserDivergences in test_compat_create.py."""

    def test_infinite_values_are_a_superset(self, diff):
        """DIV-0044 on the TS.MADD surface: RTS reports a per-item "invalid value"
        error where we store ±inf."""
        for client in (diff.reference, diff.subject):
            client.execute_command("TS.CREATE", "e:div:inf")
        ref = diff.reference.execute_command("TS.MADD", "e:div:inf", 100, "inf")
        assert isinstance(ref[0], ResponseError)
        assert diff.subject.execute_command("TS.MADD", "e:div:inf", 100, "inf") == [100]
