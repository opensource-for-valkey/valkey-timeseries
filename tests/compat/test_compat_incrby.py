"""TS.INCRBY / TS.DECRBY parity matrix (test plan §6, counter row).

Covers auto-create (with and without creation options), the TIMESTAMP option
including `*` and the stale-timestamp rule, accumulation onto the existing last
sample, and the interaction with compaction rules.

Both commands take the same option surface, so the matrix is parametrized over
the pair; where the sign matters the test says so.

The negative-TIMESTAMP gap is DIV-0032 / DIV-0033 and is pinned in
TestCounterDivergences rather than diffed.

Written clean-room from public RedisTimeSeries documentation and black-box
observation of the reference server. Do NOT consult RedisTimeSeries source or
test code (see tests/compat/README.md).
"""

from __future__ import annotations

import pytest
from valkey.exceptions import ResponseError

from compat_helpers import mk_series
from test_compat_create import _errors, _info

COUNTERS = ("TS.INCRBY", "TS.DECRBY")


@pytest.fixture(params=COUNTERS)
def counter(request):
    return request.param


class TestBasics:
    def test_auto_create_from_nothing(self, diff, counter):
        """Without TIMESTAMP the sample lands on each engine's own clock, so the
        reply and the stored timestamp can not be diffed — the series shape can."""
        diff.both(counter, "f:auto", 5)
        for client in (diff.reference, diff.subject):
            info = _info(client, "f:auto")
            assert info[b"totalSamples"] == 1
        assert (
            diff.reference.execute_command("TS.GET", "f:auto")[1]
            == diff.subject.execute_command("TS.GET", "f:auto")[1]
        )

    def test_accumulates_onto_the_last_sample(self, diff, counter):
        diff(counter, "f:acc", 5, "TIMESTAMP", 1000)
        diff(counter, "f:acc", 3, "TIMESTAMP", 1000)
        diff("TS.RANGE", "f:acc", "-", "+")
        diff("TS.GET", "f:acc")

    def test_accumulation_advances_the_timestamp(self, diff, counter):
        diff(counter, "f:adv", 5, "TIMESTAMP", 1000)
        diff(counter, "f:adv", 3, "TIMESTAMP", 2000)
        diff("TS.RANGE", "f:adv", "-", "+")
        diff("TS.INFO", "f:adv")

    def test_reply_is_the_stored_timestamp(self, diff, counter):
        mk_series(diff, "f:reply")
        diff(counter, "f:reply", 1, "TIMESTAMP", 4321)

    def test_on_an_existing_populated_series(self, diff, counter):
        mk_series(diff, "f:existing")
        diff("TS.ADD", "f:existing", 1000, 10.0)
        diff(counter, "f:existing", 2.5, "TIMESTAMP", 2000)
        diff("TS.RANGE", "f:existing", "-", "+")

    @pytest.mark.parametrize("value", [0, 1, -1, 2.5, 1e10])
    def test_value_forms(self, diff, counter, value):
        diff(counter, "f:val", value, "TIMESTAMP", 1000)
        diff("TS.RANGE", "f:val", "-", "+")

    @pytest.mark.parametrize("value", ["abc", "", "nan"])
    def test_rejected_values(self, diff, counter, value):
        mk_series(diff, "f:val:bad")
        with pytest.raises(ResponseError):
            diff(counter, "f:val:bad", value, "TIMESTAMP", 1000)
        diff("TS.RANGE", "f:val:bad", "-", "+")

    def test_infinite_delta_is_accepted(self, diff, counter):
        """Unlike TS.ADD, the counter commands take ±inf on both engines."""
        diff(counter, "f:val:inf", "inf", "TIMESTAMP", 1000)
        diff("TS.RANGE", "f:val:inf", "-", "+")

    def test_increment_of_a_nan_sample_rejected(self, diff, counter):
        mk_series(diff, "f:val:nansample")
        diff("TS.ADD", "f:val:nansample", 1000, "nan")
        with pytest.raises(ResponseError):
            diff(counter, "f:val:nansample", 1, "TIMESTAMP", 2000)
        diff("TS.RANGE", "f:val:nansample", "-", "+")

    def test_missing_value_argument(self, diff, counter):
        mk_series(diff, "f:noval")
        with pytest.raises(ResponseError):
            diff(counter, "f:noval")

    def test_wrongtype(self, diff, counter):
        diff("SET", "f:string", "hello")
        with pytest.raises(ResponseError):
            diff(counter, "f:string", 1)

    def test_incrby_and_decrby_are_symmetric(self, diff):
        diff("TS.INCRBY", "f:sym", 10, "TIMESTAMP", 1000)
        diff("TS.DECRBY", "f:sym", 4, "TIMESTAMP", 1000)
        diff("TS.RANGE", "f:sym", "-", "+")


class TestTimestampOption:
    def test_explicit_timestamp(self, diff, counter):
        diff(counter, "f:ts", 1, "TIMESTAMP", 5000)
        diff("TS.RANGE", "f:ts", "-", "+")

    def test_timestamp_zero(self, diff, counter):
        diff(counter, "f:ts:zero", 1, "TIMESTAMP", 0)
        diff("TS.RANGE", "f:ts:zero", "-", "+")
        diff("TS.INFO", "f:ts:zero")

    def test_star_timestamp(self, diff, counter):
        """Clock-dependent reply; only the effect is comparable."""
        mk_series(diff, "f:ts:star")
        ref, sub = diff.both(counter, "f:ts:star", 3, "TIMESTAMP", "*")
        assert isinstance(ref, int) and isinstance(sub, int)
        assert _info(diff.reference, "f:ts:star")[b"totalSamples"] == 1
        assert _info(diff.subject, "f:ts:star")[b"totalSamples"] == 1

    def test_omitted_timestamp_uses_the_clock(self, diff, counter):
        mk_series(diff, "f:ts:implicit")
        diff.both(counter, "f:ts:implicit", 3)
        assert _info(diff.reference, "f:ts:implicit")[b"totalSamples"] == 1
        assert _info(diff.subject, "f:ts:implicit")[b"totalSamples"] == 1

    def test_stale_timestamp_rejected(self, diff, counter):
        """A timestamp older than the series' last sample is an error for the
        counter commands (unlike TS.ADD, which back-fills)."""
        diff(counter, "f:ts:stale", 1, "TIMESTAMP", 5000)
        with pytest.raises(ResponseError):
            diff(counter, "f:ts:stale", 1, "TIMESTAMP", 4000)
        diff("TS.RANGE", "f:ts:stale", "-", "+")

    def test_equal_timestamp_is_not_stale(self, diff, counter):
        diff(counter, "f:ts:equal", 1, "TIMESTAMP", 5000)
        diff(counter, "f:ts:equal", 1, "TIMESTAMP", 5000)
        diff("TS.RANGE", "f:ts:equal", "-", "+")

    @pytest.mark.parametrize("ts", ["abc", ""])
    def test_unparseable_timestamp_rejected(self, diff, counter, ts):
        mk_series(diff, "f:ts:bad")
        with pytest.raises(ResponseError):
            diff(counter, "f:ts:bad", 1, "TIMESTAMP", ts)

    def test_timestamp_missing_value(self, diff, counter):
        """Subject-only, deliberately not diffed.

        `TS.INCRBY key <n> TIMESTAMP` with no operand makes RedisTimeSeries read
        past the end of its argument vector: it usually replies "TSDB: invalid
        timestamp" but sometimes dereferences garbage and takes the server down
        (SIGSEGV in RM_StringPtrLen, reproduced 2026-07-29 on the redis:8.8 pinned
        image, `TS.INCRBY <new-key> 1 TIMESTAMP`). Not reproduced in 15 attempts
        against the redis:8.10 pin on 2026-08-01, but a memory-layout-dependent
        out-of-bounds read does not become safe just because it didn't crash this
        time — treat it as still present. Sending it to the reference would
        intermittently kill the shared container, so only the subject — which must
        report a clean error and stay up — is exercised here."""
        diff.subject.execute_command("TS.CREATE", "f:ts:novalue")
        assert _errors(diff.subject, counter, "f:ts:novalue", 1, "TIMESTAMP")
        assert diff.subject.ping()

    def test_timestamp_option_is_case_insensitive(self, diff, counter):
        diff(counter, "f:ts:case", 1, "timestamp", 1000)
        diff("TS.RANGE", "f:ts:case", "-", "+")


class TestCreationOptions:
    def test_auto_create_with_options(self, diff, counter):
        diff(
            counter, "f:opts", 1,
            "TIMESTAMP", 1000,
            "RETENTION", 60000,
            "CHUNK_SIZE", 128,
            "UNCOMPRESSED",
        )
        diff("TS.INFO", "f:opts")

    def test_auto_create_with_labels(self, diff, counter):
        diff(counter, "f:lbl", 1, "TIMESTAMP", 1000, "LABELS", "metric", "hits")
        diff("TS.INFO", "f:lbl")
        diff("TS.QUERYINDEX", "metric=hits")

    def test_options_do_not_alter_an_existing_series(self, diff, counter):
        diff("TS.CREATE", "f:opts:existing", "RETENTION", 100)
        diff(counter, "f:opts:existing", 1, "TIMESTAMP", 1000, "RETENTION", 99999)
        diff("TS.INFO", "f:opts:existing")

    def test_retention_applies_to_the_counter(self, diff, counter):
        diff(counter, "f:opts:ret", 1, "TIMESTAMP", 1000, "RETENTION", 1000)
        diff(counter, "f:opts:ret", 1, "TIMESTAMP", 5000)
        diff("TS.RANGE", "f:opts:ret", "-", "+")
        diff("TS.INFO", "f:opts:ret")


class TestCompactionInteraction:
    def test_counter_feeds_the_compaction_rule(self, diff, counter):
        mk_series(diff, "f:comp:src")
        mk_series(diff, "f:comp:dst")
        diff("TS.CREATERULE", "f:comp:src", "f:comp:dst", "AGGREGATION", "sum", 100)
        for ts in (100, 150, 250, 350):
            diff(counter, "f:comp:src", 1, "TIMESTAMP", ts)
        diff("TS.RANGE", "f:comp:dst", "-", "+")
        diff("TS.INFO", "f:comp:dst")

    def test_accumulation_within_a_bucket_reaches_the_destination(self, diff, counter):
        mk_series(diff, "f:comp2:src")
        mk_series(diff, "f:comp2:dst")
        diff("TS.CREATERULE", "f:comp2:src", "f:comp2:dst", "AGGREGATION", "max", 100)
        diff(counter, "f:comp2:src", 5, "TIMESTAMP", 100)
        diff(counter, "f:comp2:src", 5, "TIMESTAMP", 100)
        diff(counter, "f:comp2:src", 1, "TIMESTAMP", 250)
        diff("TS.RANGE", "f:comp2:dst", "-", "+")


class TestCounterDivergences:
    """Pinned per-engine; see TestParserDivergences in test_compat_create.py."""

    def test_negative_timestamp_rejected_here(self, diff, counter):
        """DIV-0032 / DIV-0033: RTS stores a sample at a negative timestamp that its
        own read path can not return; we reject the write."""
        key = f"f:div:neg:{counter[-1]}"
        assert diff.reference.execute_command(counter, key, 1, "TIMESTAMP", -5) == -5
        assert _errors(diff.subject, counter, key, 1, "TIMESTAMP", -5)

    def test_nonnegative_timestamps_still_agree(self, diff, counter):
        """The delta class above must not mask an ordinary timestamp regression."""
        diff(counter, "f:div:pos", 1, "TIMESTAMP", 0)
        diff(counter, "f:div:pos", 1, "TIMESTAMP", 1)
        diff("TS.RANGE", "f:div:pos", "-", "+")
