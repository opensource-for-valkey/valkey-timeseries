"""TS.INFO parity matrix (test plan §6, TS.INFO row).

The frozen RTS 8.8 field baseline (`info-fields-8.8.yml`, plan §5.1 rule 3) is
checked in test_compat_smoke.py. This module covers what that check does not: the
*values* of those fields field-by-field as the series is mutated (ALTER,
CREATERULE, DEL, retention trimming, chunk splits), the DEBUG variant, and the
key-state and parsing dimensions.

Every `diff("TS.INFO", ...)` compares the whole reply, so each scenario here is a
field-by-field assertion; extra fields we emit are absorbed by DIV-0001 and a
missing or value-mismatched RTS 8.8 field fails.

Written clean-room from public RedisTimeSeries documentation and black-box
observation of the reference server. Do NOT consult RedisTimeSeries source or
test code (see tests/compat/README.md).
"""

from __future__ import annotations

import pytest
from valkey.exceptions import ResponseError

from compat_helpers import AGGREGATORS, mk_populated, mk_series
from test_compat_create import DUPLICATE_POLICIES, _errors, _info


class TestFieldValues:
    def test_empty_series(self, diff):
        mk_series(diff, "h:empty")
        diff("TS.INFO", "h:empty")

    def test_after_a_single_sample(self, diff):
        mk_populated(diff, "h:one", [(1000, 5.0)])
        diff("TS.INFO", "h:one")

    def test_timestamps_track_the_data(self, diff):
        mk_populated(diff, "h:ts", [(100, 1.0), (500, 2.0), (900, 3.0)])
        diff("TS.INFO", "h:ts")
        diff("TS.ADD", "h:ts", 50, 0.5)
        diff("TS.INFO", "h:ts")
        diff("TS.ADD", "h:ts", 1500, 4.0)
        diff("TS.INFO", "h:ts")

    def test_total_samples_after_duplicates(self, diff):
        diff("TS.CREATE", "h:dupcount", "DUPLICATE_POLICY", "LAST")
        diff("TS.ADD", "h:dupcount", 100, 1.0)
        diff("TS.ADD", "h:dupcount", 100, 2.0)
        diff("TS.INFO", "h:dupcount")

    @pytest.mark.parametrize("policy", DUPLICATE_POLICIES)
    def test_duplicate_policy_field(self, diff, policy):
        diff("TS.CREATE", "h:dp", "DUPLICATE_POLICY", policy)
        diff("TS.INFO", "h:dp")

    def test_duplicate_policy_field_when_unset(self, diff):
        diff("TS.CREATE", "h:dp:unset")
        diff("TS.INFO", "h:dp:unset")

    @pytest.mark.parametrize("encoding", ["COMPRESSED", "UNCOMPRESSED"])
    def test_chunk_type_field(self, diff, encoding):
        diff("TS.CREATE", "h:enc", "ENCODING", encoding)
        diff("TS.INFO", "h:enc")

    def test_ignore_fields(self, diff):
        diff("TS.CREATE", "h:ign", "DUPLICATE_POLICY", "LAST", "IGNORE", 7, 1.25)
        diff("TS.INFO", "h:ign")

    def test_labels_field(self, diff):
        diff("TS.CREATE", "h:lbl", "LABELS", "b", "2", "a", "1", "zz", "26")
        diff("TS.INFO", "h:lbl")

    def test_labels_field_when_empty(self, diff):
        mk_series(diff, "h:lbl:none")
        diff("TS.INFO", "h:lbl:none")

    def test_retention_field(self, diff):
        diff("TS.CREATE", "h:ret", "RETENTION", 12345)
        diff("TS.INFO", "h:ret")


class TestAfterMutation:
    def test_after_alter(self, diff):
        mk_populated(diff, "h:mut:alter", [(100, 1.0), (200, 2.0)])
        diff("TS.ALTER", "h:mut:alter", "RETENTION", 50000, "LABELS", "k", "v")
        diff("TS.INFO", "h:mut:alter")

    def test_after_del(self, diff):
        mk_populated(diff, "h:mut:del", [(100, 1.0), (200, 2.0), (300, 3.0)])
        diff("TS.DEL", "h:mut:del", 150, 250)
        diff("TS.INFO", "h:mut:del")

    def test_after_deleting_the_first_sample(self, diff):
        mk_populated(diff, "h:mut:delfirst", [(100, 1.0), (200, 2.0), (300, 3.0)])
        diff("TS.DEL", "h:mut:delfirst", 100, 100)
        diff("TS.INFO", "h:mut:delfirst")

    def test_after_deleting_everything(self, diff):
        mk_populated(diff, "h:mut:delall", [(100, 1.0), (200, 2.0)])
        diff("TS.DEL", "h:mut:delall", 0, 1000)
        diff("TS.INFO", "h:mut:delall")

    def test_after_retention_trimming(self, diff):
        diff("TS.CREATE", "h:mut:ret", "RETENTION", 1000)
        for ts in (1000, 1500, 2500, 3000):
            diff("TS.ADD", "h:mut:ret", ts, 1.0)
        diff("TS.INFO", "h:mut:ret")

    def test_after_a_chunk_split(self, diff):
        mk_series(diff, "h:mut:split", "CHUNK_SIZE", 48)
        for ts in range(100, 2000, 50):
            diff("TS.ADD", "h:mut:split", ts, float(ts))
        diff("TS.INFO", "h:mut:split")

    def test_source_key_and_rules_fields(self, diff):
        mk_series(diff, "h:mut:src")
        mk_series(diff, "h:mut:dst")
        diff("TS.CREATERULE", "h:mut:src", "h:mut:dst", "AGGREGATION", "avg", 100, 10)
        diff("TS.INFO", "h:mut:src")
        diff("TS.INFO", "h:mut:dst")
        diff("TS.DELETERULE", "h:mut:src", "h:mut:dst")
        diff("TS.INFO", "h:mut:src")
        diff("TS.INFO", "h:mut:dst")

    @pytest.mark.parametrize("agg", AGGREGATORS)
    def test_rules_field_per_aggregator(self, diff, agg):
        mk_series(diff, "h:rule:src")
        mk_series(diff, "h:rule:dst")
        diff("TS.CREATERULE", "h:rule:src", "h:rule:dst", "AGGREGATION", agg, 100)
        diff("TS.INFO", "h:rule:src")

    def test_rules_field_with_several_rules(self, diff):
        mk_series(diff, "h:rules:src")
        for name in ("a", "b", "c"):
            mk_series(diff, f"h:rules:{name}")
            diff(
                "TS.CREATERULE", "h:rules:src", f"h:rules:{name}",
                "AGGREGATION", "sum", 100,
            )
        diff("TS.INFO", "h:rules:src")

    def test_on_a_compaction_destination_after_writes(self, diff):
        mk_series(diff, "h:dstinfo:src")
        mk_series(diff, "h:dstinfo:dst")
        diff("TS.CREATERULE", "h:dstinfo:src", "h:dstinfo:dst", "AGGREGATION", "sum", 100)
        for ts, value in [(100, 1.0), (150, 2.0), (250, 4.0), (350, 8.0)]:
            diff("TS.ADD", "h:dstinfo:src", ts, value)
        diff("TS.INFO", "h:dstinfo:dst")


class TestDebugVariant:
    def test_debug_on_an_empty_series(self, diff):
        mk_series(diff, "h:dbg:empty")
        diff("TS.INFO", "h:dbg:empty", "DEBUG")

    def test_debug_on_a_populated_series(self, diff):
        mk_populated(diff, "h:dbg", [(100, 1.0), (200, 2.0), (300, 3.0)])
        diff("TS.INFO", "h:dbg", "DEBUG")

    def test_debug_keyword_is_case_insensitive(self, diff):
        mk_populated(diff, "h:dbg:case", [(100, 1.0)])
        diff("TS.INFO", "h:dbg:case", "debug")

    def test_debug_is_a_superset_of_the_plain_reply(self, diff):
        """Every field of the plain reply must appear, with the same value, in the
        DEBUG reply — on both engines."""
        mk_populated(diff, "h:dbg:super", [(100, 1.0), (200, 2.0)])
        for client in (diff.reference, diff.subject):
            plain = _info(client, "h:dbg:super")
            reply = client.execute_command("TS.INFO", "h:dbg:super", "DEBUG")
            debug = reply if isinstance(reply, dict) else dict(
                zip(reply[::2], reply[1::2])
            )
            for field, value in plain.items():
                if field == b"memoryUsage":
                    continue
                assert field in debug, field
                assert debug[field] == value, field

    def test_unknown_trailing_argument(self, diff):
        """Neither engine rejects a trailing token that is not DEBUG; what matters
        is that they agree on whether it turns the debug fields on."""
        mk_populated(diff, "h:dbg:bad", [(100, 1.0)])
        diff("TS.INFO", "h:dbg:bad", "BOGUS")

    def test_chunk_boundaries_after_a_split(self, diff):
        """Subject-only: chunk *boundaries* follow each engine's own splitting
        policy, so they are self-consistency assertions rather than a diff. The
        normalizer already exempts per-chunk byte counts (plan §6)."""
        mk_series(diff, "h:dbg:split", "CHUNK_SIZE", 48)
        for ts in range(100, 1000, 50):
            diff("TS.ADD", "h:dbg:split", ts, float(ts))

        for client in (diff.reference, diff.subject):
            reply = client.execute_command("TS.INFO", "h:dbg:split", "DEBUG")
            info = reply if isinstance(reply, dict) else dict(
                zip(reply[::2], reply[1::2])
            )
            chunks = [
                c if isinstance(c, dict) else dict(zip(c[::2], c[1::2]))
                for c in info[b"Chunks"]
            ]
            assert sum(c[b"samples"] for c in chunks) == info[b"totalSamples"]
            assert min(c[b"startTimestamp"] for c in chunks) == info[b"firstTimestamp"]
            assert max(c[b"endTimestamp"] for c in chunks) == info[b"lastTimestamp"]


class TestKeyStates:
    def test_missing_key(self, diff):
        with pytest.raises(ResponseError):
            diff("TS.INFO", "h:nonexistent")

    def test_missing_key_with_debug(self, diff):
        with pytest.raises(ResponseError):
            diff("TS.INFO", "h:nonexistent", "DEBUG")

    def test_wrongtype(self, diff):
        diff("SET", "h:string", "hello")
        with pytest.raises(ResponseError):
            diff("TS.INFO", "h:string")

    def test_missing_key_argument(self, diff):
        with pytest.raises(ResponseError):
            diff("TS.INFO")
