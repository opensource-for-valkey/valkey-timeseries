"""TS.ALTER parity matrix (test plan §6, TS.ALTER row).

Covers each mutable property changed and cleared, label replacement semantics
(full replace, not merge), altering a series that participates in a compaction
rule, and the key-state and argument-parsing dimensions.

Every accepted TS.ALTER is followed by a TS.INFO diff — the reply is a bare +OK,
so the mutation itself is the observable — and, where data is involved, by a
TS.RANGE diff to pin that altering metadata does not disturb stored samples.

Written clean-room from public RedisTimeSeries documentation and black-box
observation of the reference server. Do NOT consult RedisTimeSeries source or
test code (see tests/compat/README.md).
"""

from __future__ import annotations

import pytest
from valkey.exceptions import ResponseError

from compat_helpers import mk_populated, mk_series
from test_compat_create import DUPLICATE_POLICIES, _errors

SAMPLES = [(100, 1.0), (200, 2.0), (300, 3.0)]


class TestMutableProperties:
    @pytest.mark.parametrize("retention", [0, 1, 5000])
    def test_retention_changed(self, diff, retention):
        mk_populated(diff, "a:ret", SAMPLES)
        diff("TS.ALTER", "a:ret", "RETENTION", retention)
        diff("TS.INFO", "a:ret")
        diff("TS.RANGE", "a:ret", "-", "+")

    def test_retention_cleared_back_to_zero(self, diff):
        diff("TS.CREATE", "a:ret:clear", "RETENTION", 5000)
        diff("TS.ALTER", "a:ret:clear", "RETENTION", 0)
        diff("TS.INFO", "a:ret:clear")

    def test_shrinking_retention_trims_existing_samples(self, diff):
        """The retention window is a read-path property, so tightening it must
        take effect for data already stored."""
        mk_populated(diff, "a:ret:trim", [(1000, 1.0), (5000, 2.0), (9000, 3.0)])
        diff("TS.ALTER", "a:ret:trim", "RETENTION", 1000)
        diff("TS.RANGE", "a:ret:trim", "-", "+")
        diff("TS.INFO", "a:ret:trim")

    @pytest.mark.parametrize("size", [48, 256, 1048576])
    def test_chunk_size_changed(self, diff, size):
        mk_populated(diff, "a:cs", SAMPLES)
        diff("TS.ALTER", "a:cs", "CHUNK_SIZE", size)
        diff("TS.INFO", "a:cs")
        diff("TS.RANGE", "a:cs", "-", "+")

    @pytest.mark.parametrize("size", [0, 47, 49, 1048584])
    def test_invalid_chunk_size_rejected(self, diff, size):
        mk_series(diff, "a:cs:bad")
        with pytest.raises(ResponseError):
            diff("TS.ALTER", "a:cs:bad", "CHUNK_SIZE", size)
        diff("TS.INFO", "a:cs:bad")

    @pytest.mark.parametrize("policy", DUPLICATE_POLICIES)
    def test_duplicate_policy_changed(self, diff, policy):
        mk_populated(diff, "a:dp", SAMPLES)
        diff("TS.ALTER", "a:dp", "DUPLICATE_POLICY", policy)
        diff("TS.INFO", "a:dp")

    def test_duplicate_policy_takes_effect_on_next_write(self, diff):
        mk_populated(diff, "a:dp:effect", [(100, 1.0)])
        diff("TS.ALTER", "a:dp:effect", "DUPLICATE_POLICY", "LAST")
        diff("TS.ADD", "a:dp:effect", 100, 9.0)
        diff("TS.RANGE", "a:dp:effect", "-", "+")

    def test_unknown_duplicate_policy_rejected(self, diff):
        mk_series(diff, "a:dp:bad")
        with pytest.raises(ResponseError):
            diff("TS.ALTER", "a:dp:bad", "DUPLICATE_POLICY", "AVERAGE")
        diff("TS.INFO", "a:dp:bad")

    def test_ignore_changed_and_cleared(self, diff):
        diff("TS.CREATE", "a:ign", "DUPLICATE_POLICY", "LAST", "IGNORE", 5, 0.5)
        diff("TS.ALTER", "a:ign", "IGNORE", 10, 1.5)
        diff("TS.INFO", "a:ign")
        diff("TS.ALTER", "a:ign", "IGNORE", 0, 0)
        diff("TS.INFO", "a:ign")

    def test_negative_ignore_rejected(self, diff):
        mk_series(diff, "a:ign:neg")
        with pytest.raises(ResponseError):
            diff("TS.ALTER", "a:ign:neg", "IGNORE", 5, -1)
        diff("TS.INFO", "a:ign:neg")

    def test_alter_with_no_options(self, diff):
        """A bare TS.ALTER must not change anything."""
        diff("TS.CREATE", "a:noop", "RETENTION", 100, "LABELS", "a", "1")
        diff("TS.ALTER", "a:noop")
        diff("TS.INFO", "a:noop")

    def test_multiple_properties_in_one_call(self, diff):
        mk_populated(diff, "a:multi", SAMPLES)
        diff(
            "TS.ALTER", "a:multi",
            "RETENTION", 9000,
            "CHUNK_SIZE", 256,
            "DUPLICATE_POLICY", "SUM",
            "LABELS", "kind", "multi",
        )
        diff("TS.INFO", "a:multi")
        diff("TS.RANGE", "a:multi", "-", "+")

    def test_duplicated_option_first_wins(self, diff):
        mk_series(diff, "a:dup")
        diff("TS.ALTER", "a:dup", "RETENTION", 100, "RETENTION", 200)
        diff("TS.INFO", "a:dup")


class TestLabelReplacement:
    def test_labels_are_replaced_not_merged(self, diff):
        diff("TS.CREATE", "a:lbl", "LABELS", "host", "h1", "region", "us")
        diff("TS.ALTER", "a:lbl", "LABELS", "host", "h2")
        diff("TS.INFO", "a:lbl")

    def test_empty_labels_clears_the_set(self, diff):
        diff("TS.CREATE", "a:lbl:clear", "LABELS", "host", "h1")
        diff("TS.ALTER", "a:lbl:clear", "LABELS")
        diff("TS.INFO", "a:lbl:clear")

    def test_labels_omitted_leaves_the_set_alone(self, diff):
        diff("TS.CREATE", "a:lbl:keep", "LABELS", "host", "h1")
        diff("TS.ALTER", "a:lbl:keep", "RETENTION", 100)
        diff("TS.INFO", "a:lbl:keep")

    def test_replacement_is_visible_to_the_index(self, diff):
        """Label changes must be reflected in filter-based lookups, not just INFO."""
        diff("TS.CREATE", "a:lbl:idx", "LABELS", "metric", "old")
        diff("TS.QUERYINDEX", "metric=old")
        diff("TS.ALTER", "a:lbl:idx", "LABELS", "metric", "new")
        diff("TS.QUERYINDEX", "metric=old")
        diff("TS.QUERYINDEX", "metric=new")
        diff("TS.MGET", "FILTER", "metric=new")

    def test_label_empty_value_rejected(self, diff):
        mk_series(diff, "a:lbl:emptyval")
        with pytest.raises(ResponseError):
            diff("TS.ALTER", "a:lbl:emptyval", "LABELS", "host", "")
        diff("TS.INFO", "a:lbl:emptyval")

    def test_duplicate_label_name(self, diff):
        mk_series(diff, "a:lbl:dupname")
        diff("TS.ALTER", "a:lbl:dupname", "LABELS", "host", "h1", "host", "h2")
        diff("TS.INFO", "a:lbl:dupname")


class TestAlterWithRules:
    def test_alter_source_of_a_rule(self, diff):
        mk_series(diff, "a:rule:src")
        mk_series(diff, "a:rule:dst")
        diff("TS.CREATERULE", "a:rule:src", "a:rule:dst", "AGGREGATION", "avg", 100)
        diff("TS.ALTER", "a:rule:src", "RETENTION", 100000, "LABELS", "role", "src")
        diff("TS.INFO", "a:rule:src")
        diff("TS.INFO", "a:rule:dst")

    def test_alter_destination_of_a_rule(self, diff):
        mk_series(diff, "a:rule2:src")
        mk_series(diff, "a:rule2:dst")
        diff("TS.CREATERULE", "a:rule2:src", "a:rule2:dst", "AGGREGATION", "sum", 100)
        diff("TS.ALTER", "a:rule2:dst", "RETENTION", 100000)
        diff("TS.INFO", "a:rule2:dst")

    def test_rule_still_compacts_after_alter(self, diff):
        mk_series(diff, "a:rule3:src")
        mk_series(diff, "a:rule3:dst")
        diff("TS.CREATERULE", "a:rule3:src", "a:rule3:dst", "AGGREGATION", "sum", 100)
        diff("TS.ALTER", "a:rule3:src", "DUPLICATE_POLICY", "LAST")
        for ts, value in [(100, 1.0), (150, 2.0), (250, 4.0), (350, 8.0)]:
            diff("TS.ADD", "a:rule3:src", ts, value)
        diff("TS.RANGE", "a:rule3:dst", "-", "+")


class TestKeyStates:
    def test_missing_key(self, diff):
        with pytest.raises(ResponseError):
            diff("TS.ALTER", "a:nonexistent", "RETENTION", 100)

    def test_wrongtype(self, diff):
        diff("SET", "a:string", "hello")
        with pytest.raises(ResponseError):
            diff("TS.ALTER", "a:string", "RETENTION", 100)

    def test_missing_key_argument(self, diff):
        with pytest.raises(ResponseError):
            diff("TS.ALTER")

    def test_empty_series(self, diff):
        mk_series(diff, "a:empty")
        diff("TS.ALTER", "a:empty", "RETENTION", 100, "LABELS", "x", "1")
        diff("TS.INFO", "a:empty")
        diff("TS.RANGE", "a:empty", "-", "+")


class TestOptionParsing:
    @pytest.mark.parametrize(
        "option,value",
        [("retention", 100), ("chunk_size", 128), ("duplicate_policy", "LAST")],
    )
    def test_option_names_are_case_insensitive(self, diff, option, value):
        mk_series(diff, "a:case")
        diff("TS.ALTER", "a:case", option, value)
        diff("TS.INFO", "a:case")

    @pytest.mark.parametrize("option", ["RETENTION", "CHUNK_SIZE", "DUPLICATE_POLICY"])
    def test_option_missing_value(self, diff, option):
        mk_series(diff, "a:novalue")
        with pytest.raises(ResponseError):
            diff("TS.ALTER", "a:novalue", option)
        diff("TS.INFO", "a:novalue")


class TestAlterDivergences:
    """Pinned per-engine; see TestParserDivergences in test_compat_create.py for why
    this class of delta can not be routed through `diff`."""

    def test_unrecognized_arguments_are_rejected_not_ignored(self, diff):
        """DIV-0042 on the TS.ALTER surface."""
        for client in (diff.reference, diff.subject):
            client.execute_command("TS.CREATE", "a:div:unk")
        assert diff.reference.execute_command(
            "TS.ALTER", "a:div:unk", "BOGUS", "1"
        ) == b"OK"
        assert _errors(diff.subject, "TS.ALTER", "a:div:unk", "BOGUS", "1")
