"""TS.ADD parity matrix (test plan §6, TS.ADD row).

Covers auto-create (with options and with per-config defaults), the `*`
timestamp, out-of-order inserts, the retention trimming trigger, the
ON_DUPLICATE / series-policy / config precedence chain, IGNORE filtering and its
only-applies-to-LAST rule, value rejects (NaN, inf, unparseable), and timestamp
edge cases including 0 and negatives.

Written clean-room from public RedisTimeSeries documentation and black-box
observation of the reference server. Do NOT consult RedisTimeSeries source or
test code (see tests/compat/README.md).
"""

from __future__ import annotations

import pytest
from valkey.exceptions import ResponseError

from compat_helpers import mk_populated, mk_series
from test_compat_create import DUPLICATE_POLICIES, _errors, _info


class TestBasicAdd:
    def test_add_to_existing_series(self, diff):
        mk_series(diff, "d:basic")
        diff("TS.ADD", "d:basic", 100, 1.5)
        diff("TS.ADD", "d:basic", 200, 2.5)
        diff("TS.RANGE", "d:basic", "-", "+")
        diff("TS.INFO", "d:basic")

    def test_reply_is_the_stored_timestamp(self, diff):
        mk_series(diff, "d:reply")
        diff("TS.ADD", "d:reply", 12345, 1.0)

    def test_timestamp_zero(self, diff):
        mk_series(diff, "d:zero")
        diff("TS.ADD", "d:zero", 0, 1.0)
        diff("TS.RANGE", "d:zero", "-", "+")
        diff("TS.INFO", "d:zero")

    @pytest.mark.parametrize("ts", [-1, -1000])
    def test_negative_timestamp_rejected(self, diff, ts):
        mk_series(diff, "d:neg")
        with pytest.raises(ResponseError):
            diff("TS.ADD", "d:neg", ts, 1.0)

    @pytest.mark.parametrize("ts", ["abc", "", "100abc"])
    def test_unparseable_timestamp_rejected(self, diff, ts):
        mk_series(diff, "d:badts")
        with pytest.raises(ResponseError):
            diff("TS.ADD", "d:badts", ts, 1.0)

    def test_star_timestamp_uses_server_clock(self, diff):
        """The reply is each engine's own clock, so it can not be diffed; what must
        match is that the sample lands and the series reports one sample."""
        mk_series(diff, "d:star")
        ref_ts, sub_ts = diff.both("TS.ADD", "d:star", "*", 7.0)
        assert isinstance(ref_ts, int) and isinstance(sub_ts, int)
        assert _info(diff.reference, "d:star")[b"totalSamples"] == 1
        assert _info(diff.subject, "d:star")[b"totalSamples"] == 1
        assert (
            diff.reference.execute_command("TS.GET", "d:star")[1]
            == diff.subject.execute_command("TS.GET", "d:star")[1]
        )

    def test_out_of_order_insert(self, diff):
        mk_series(diff, "d:ooo")
        for ts in (300, 100, 200, 50):
            diff("TS.ADD", "d:ooo", ts, float(ts))
        diff("TS.RANGE", "d:ooo", "-", "+")
        diff("TS.INFO", "d:ooo")
        diff("TS.GET", "d:ooo")

    def test_out_of_order_across_chunks(self, diff):
        """Backfill into an earlier chunk after a split."""
        mk_series(diff, "d:ooo:chunks", "CHUNK_SIZE", 48)
        for ts in range(1000, 3000, 100):
            diff("TS.ADD", "d:ooo:chunks", ts, float(ts))
        diff("TS.ADD", "d:ooo:chunks", 1050, -1.0)
        diff("TS.RANGE", "d:ooo:chunks", "-", "+")
        diff("TS.INFO", "d:ooo:chunks")


class TestValues:
    @pytest.mark.parametrize("value", [0, -0.0, 1e-300, 1e300, 3.14159265358979, -42.5])
    def test_value_round_trip(self, diff, value):
        mk_series(diff, "d:val")
        diff("TS.ADD", "d:val", 100, value)
        diff("TS.RANGE", "d:val", "-", "+")
        diff("TS.GET", "d:val")

    @pytest.mark.parametrize("value", ["abc", "", "1.2.3"])
    def test_rejected_values(self, diff, value):
        mk_series(diff, "d:val:bad")
        with pytest.raises(ResponseError):
            diff("TS.ADD", "d:val:bad", 100, value)
        diff("TS.INFO", "d:val:bad")

    def test_nan_is_stored(self, diff):
        """NaN is an accepted sample value on both engines — it is only the
        MIN/MAX/SUM duplicate policies that refuse to fold it."""
        mk_series(diff, "d:val:nan")
        diff("TS.ADD", "d:val:nan", 100, "nan")
        diff("TS.RANGE", "d:val:nan", "-", "+")
        diff("TS.GET", "d:val:nan")
        diff("TS.INFO", "d:val:nan")

    def test_nan_blocked_as_a_duplicate(self, diff):
        diff("TS.CREATE", "d:val:nan2", "DUPLICATE_POLICY", "SUM")
        diff("TS.ADD", "d:val:nan2", 100, "nan")
        with pytest.raises(ResponseError):
            diff("TS.ADD", "d:val:nan2", 100, 5.0)

    def test_missing_value_argument(self, diff):
        mk_series(diff, "d:noval")
        with pytest.raises(ResponseError):
            diff("TS.ADD", "d:noval", 100)

    def test_missing_all_arguments(self, diff):
        with pytest.raises(ResponseError):
            diff("TS.ADD")


class TestAutoCreate:
    def test_auto_create_with_defaults(self, diff):
        diff("TS.ADD", "d:auto", 100, 1.0)
        diff("TS.INFO", "d:auto")
        diff("TS.RANGE", "d:auto", "-", "+")

    def test_auto_create_with_labels(self, diff):
        diff("TS.ADD", "d:auto:lbl", 100, 1.0, "LABELS", "host", "h1", "metric", "cpu")
        diff("TS.INFO", "d:auto:lbl")
        diff("TS.QUERYINDEX", "metric=cpu")

    def test_auto_create_with_options(self, diff):
        diff(
            "TS.ADD", "d:auto:opts", 100, 1.0,
            "RETENTION", 60000,
            "ENCODING", "UNCOMPRESSED",
            "CHUNK_SIZE", 128,
            "DUPLICATE_POLICY", "LAST",
        )
        diff("TS.INFO", "d:auto:opts")

    def test_options_ignored_on_an_existing_series(self, diff):
        """Creation options on a TS.ADD against an existing series must not
        retroactively alter it."""
        diff("TS.CREATE", "d:auto:existing", "RETENTION", 100)
        diff("TS.ADD", "d:auto:existing", 100, 1.0, "RETENTION", 99999)
        diff("TS.INFO", "d:auto:existing")

    @pytest.mark.parametrize(
        "option", [("CHUNK_SIZE", 7), ("RETENTION", -5), ("ENCODING", "BOGUS"),
                   ("DUPLICATE_POLICY", "AVERAGE"), ("IGNORE", -1, 1)],
    )
    def test_inert_options_are_not_validated_on_an_existing_series(self, diff, option):
        """Because they are inert here, an unusable value in one is not an error —
        the option is never interpreted at all. ON_DUPLICATE is the exception; it is
        a per-call override, covered by TestDuplicatePolicy."""
        diff("TS.CREATE", "d:auto:inert")
        diff("TS.ADD", "d:auto:inert", 100, 1.0, *option)
        diff("TS.INFO", "d:auto:inert")

    def test_wrongtype(self, diff):
        diff("SET", "d:string", "hello")
        with pytest.raises(ResponseError):
            diff("TS.ADD", "d:string", 100, 1.0)

    def test_auto_create_rejected_options_do_not_create_the_key(self, diff):
        with pytest.raises(ResponseError):
            diff("TS.ADD", "d:auto:bad", 100, 1.0, "CHUNK_SIZE", 7)
        diff("EXISTS", "d:auto:bad")


class TestDuplicatePolicy:
    @pytest.mark.parametrize("policy", DUPLICATE_POLICIES)
    def test_series_policy_resolves_the_duplicate(self, diff, policy):
        diff("TS.CREATE", "d:dp", "DUPLICATE_POLICY", policy)
        diff("TS.ADD", "d:dp", 100, 10.0)
        if policy == "BLOCK":
            with pytest.raises(ResponseError):
                diff("TS.ADD", "d:dp", 100, 3.0)
        else:
            diff("TS.ADD", "d:dp", 100, 3.0)
        diff("TS.RANGE", "d:dp", "-", "+")

    @pytest.mark.parametrize("policy", DUPLICATE_POLICIES)
    def test_on_duplicate_overrides_the_series_policy(self, diff, policy):
        diff("TS.CREATE", "d:od", "DUPLICATE_POLICY", "BLOCK")
        diff("TS.ADD", "d:od", 100, 10.0)
        if policy == "BLOCK":
            with pytest.raises(ResponseError):
                diff("TS.ADD", "d:od", 100, 3.0, "ON_DUPLICATE", policy)
        else:
            diff("TS.ADD", "d:od", 100, 3.0, "ON_DUPLICATE", policy)
        diff("TS.RANGE", "d:od", "-", "+")

    def test_on_duplicate_does_not_persist_to_the_series(self, diff):
        diff("TS.CREATE", "d:od:persist", "DUPLICATE_POLICY", "BLOCK")
        diff("TS.ADD", "d:od:persist", 100, 10.0)
        diff("TS.ADD", "d:od:persist", 100, 3.0, "ON_DUPLICATE", "LAST")
        diff("TS.INFO", "d:od:persist")
        with pytest.raises(ResponseError):
            diff("TS.ADD", "d:od:persist", 100, 4.0)

    def test_config_default_applies_when_series_has_no_policy(self, diff):
        """A series created without DUPLICATE_POLICY falls back to the module
        configuration, which is BLOCK by default on both engines."""
        diff("TS.ADD", "d:dp:cfg", 100, 1.0)
        with pytest.raises(ResponseError):
            diff("TS.ADD", "d:dp:cfg", 100, 2.0)
        diff("TS.RANGE", "d:dp:cfg", "-", "+")

    def test_unknown_on_duplicate_rejected(self, diff):
        mk_series(diff, "d:od:bad")
        with pytest.raises(ResponseError):
            diff("TS.ADD", "d:od:bad", 100, 1.0, "ON_DUPLICATE", "AVERAGE")

    def test_on_duplicate_missing_value(self, diff):
        mk_series(diff, "d:od:novalue")
        with pytest.raises(ResponseError):
            diff("TS.ADD", "d:od:novalue", 100, 1.0, "ON_DUPLICATE")

    def test_duplicate_of_a_non_last_sample(self, diff):
        """Upsert into the middle of the series, not just onto the last sample."""
        diff("TS.CREATE", "d:dp:mid", "DUPLICATE_POLICY", "SUM")
        for ts in (100, 200, 300):
            diff("TS.ADD", "d:dp:mid", ts, 1.0)
        diff("TS.ADD", "d:dp:mid", 200, 5.0)
        diff("TS.RANGE", "d:dp:mid", "-", "+")
        diff("TS.INFO", "d:dp:mid")


class TestIgnore:
    def test_time_diff_filter(self, diff):
        diff("TS.CREATE", "d:ign:time", "DUPLICATE_POLICY", "LAST", "IGNORE", 10, 0)
        diff("TS.ADD", "d:ign:time", 1000, 5.0)
        diff("TS.ADD", "d:ign:time", 1005, 5.0)   # within both thresholds -> ignored
        diff("TS.ADD", "d:ign:time", 1020, 5.0)   # outside the time threshold
        diff("TS.RANGE", "d:ign:time", "-", "+")
        diff("TS.INFO", "d:ign:time")

    def test_value_diff_filter(self, diff):
        diff("TS.CREATE", "d:ign:val", "DUPLICATE_POLICY", "LAST", "IGNORE", 100, 2.0)
        diff("TS.ADD", "d:ign:val", 1000, 5.0)
        diff("TS.ADD", "d:ign:val", 1050, 6.0)    # within both -> ignored
        diff("TS.ADD", "d:ign:val", 1060, 9.0)    # value moved too far
        diff("TS.RANGE", "d:ign:val", "-", "+")

    def test_both_thresholds_must_hold(self, diff):
        diff("TS.CREATE", "d:ign:both", "DUPLICATE_POLICY", "LAST", "IGNORE", 10, 2.0)
        diff("TS.ADD", "d:ign:both", 1000, 5.0)
        diff("TS.ADD", "d:ign:both", 1005, 100.0)  # time ok, value not -> stored
        diff("TS.RANGE", "d:ign:both", "-", "+")

    def test_ignore_only_applies_under_last_policy(self, diff):
        """IGNORE is documented as taking effect only when the duplicate policy is
        LAST; under another policy the sample must be stored normally."""
        diff("TS.CREATE", "d:ign:block", "DUPLICATE_POLICY", "BLOCK", "IGNORE", 100, 100.0)
        diff("TS.ADD", "d:ign:block", 1000, 5.0)
        diff("TS.ADD", "d:ign:block", 1010, 5.5)
        diff("TS.RANGE", "d:ign:block", "-", "+")
        diff("TS.INFO", "d:ign:block")

    def test_ignored_sample_reply_is_the_timestamp(self, diff):
        """An ignored TS.ADD is not an error: it replies with the timestamp."""
        diff("TS.CREATE", "d:ign:reply", "DUPLICATE_POLICY", "LAST", "IGNORE", 100, 100.0)
        diff("TS.ADD", "d:ign:reply", 1000, 5.0)
        diff("TS.ADD", "d:ign:reply", 1050, 5.5)
        diff("TS.GET", "d:ign:reply")

    def test_ignore_does_not_apply_to_backfill(self, diff):
        """The thresholds are measured against the last sample, so a sample older
        than it takes the ordinary out-of-order path."""
        diff("TS.CREATE", "d:ign:back", "DUPLICATE_POLICY", "LAST", "IGNORE", 100, 100.0)
        diff("TS.ADD", "d:ign:back", 1000, 5.0)
        diff("TS.ADD", "d:ign:back", 2000, 6.0)
        diff("TS.ADD", "d:ign:back", 1500, 7.0)
        diff("TS.RANGE", "d:ign:back", "-", "+")


class TestRetention:
    def test_write_triggers_trimming(self, diff):
        diff("TS.CREATE", "d:ret", "RETENTION", 1000)
        diff("TS.ADD", "d:ret", 1000, 1.0)
        diff("TS.ADD", "d:ret", 1500, 2.0)
        diff("TS.ADD", "d:ret", 2500, 3.0)
        diff("TS.RANGE", "d:ret", "-", "+")
        diff("TS.INFO", "d:ret")

    def test_boundary_sample_is_retained(self, diff):
        """The sample at exactly lastTimestamp - retention stays in the window."""
        diff("TS.CREATE", "d:ret:edge", "RETENTION", 1000)
        diff("TS.ADD", "d:ret:edge", 1000, 1.0)
        diff("TS.ADD", "d:ret:edge", 2000, 2.0)
        diff("TS.RANGE", "d:ret:edge", "-", "+")
        diff("TS.INFO", "d:ret:edge")

    def test_sample_older_than_the_window_is_rejected(self, diff):
        diff("TS.CREATE", "d:ret:old", "RETENTION", 1000)
        diff("TS.ADD", "d:ret:old", 5000, 1.0)
        with pytest.raises(ResponseError):
            diff("TS.ADD", "d:ret:old", 100, 2.0)
        diff("TS.RANGE", "d:ret:old", "-", "+")

    def test_retention_zero_keeps_everything(self, diff):
        diff("TS.CREATE", "d:ret:zero", "RETENTION", 0)
        for ts in (1, 1_000_000, 2_000_000_000):
            diff("TS.ADD", "d:ret:zero", ts, 1.0)
        diff("TS.RANGE", "d:ret:zero", "-", "+")
        diff("TS.INFO", "d:ret:zero")


class TestCompactionInteraction:
    def test_add_feeds_the_compaction_rule(self, diff):
        mk_series(diff, "d:comp:src")
        mk_series(diff, "d:comp:dst")
        diff("TS.CREATERULE", "d:comp:src", "d:comp:dst", "AGGREGATION", "sum", 100)
        for ts, value in [(100, 1.0), (150, 2.0), (250, 4.0), (350, 8.0)]:
            diff("TS.ADD", "d:comp:src", ts, value)
        diff("TS.RANGE", "d:comp:dst", "-", "+")
        diff("TS.INFO", "d:comp:dst")

    def test_add_to_a_compaction_destination(self, diff):
        """Writing directly into a destination series is the reference's call."""
        mk_series(diff, "d:comp2:src")
        mk_series(diff, "d:comp2:dst")
        diff("TS.CREATERULE", "d:comp2:src", "d:comp2:dst", "AGGREGATION", "sum", 100)
        try:
            diff("TS.ADD", "d:comp2:dst", 100, 1.0)
        except ResponseError:
            pass
        diff("TS.RANGE", "d:comp2:dst", "-", "+")


class TestOptionParsing:
    def test_labels_after_other_options(self, diff):
        diff(
            "TS.ADD", "d:parse:lbl", 100, 1.0,
            "RETENTION", 5000, "LABELS", "a", "1", "b", "2",
        )
        diff("TS.INFO", "d:parse:lbl")

    @pytest.mark.parametrize("option", ["retention", "chunk_size", "on_duplicate"])
    def test_option_names_are_case_insensitive(self, diff, option):
        value = {"retention": 100, "chunk_size": 128, "on_duplicate": "LAST"}[option]
        diff("TS.ADD", "d:parse:case", 100, 1.0, option, value)
        diff("TS.INFO", "d:parse:case")

    def test_duplicated_option_first_wins(self, diff):
        diff("TS.ADD", "d:parse:dup", 100, 1.0, "RETENTION", 100, "RETENTION", 200)
        diff("TS.INFO", "d:parse:dup")


class TestAddDivergences:
    """Pinned per-engine; see TestParserDivergences in test_compat_create.py."""

    def test_unrecognized_arguments_are_rejected_not_ignored(self, diff):
        """DIV-0042 on the TS.ADD surface."""
        assert diff.reference.execute_command(
            "TS.ADD", "d:div:unk", 100, 1.0, "BOGUS", "1"
        ) == 100
        assert _errors(diff.subject, "TS.ADD", "d:div:unk", 100, 1.0, "BOGUS", "1")

    @pytest.mark.parametrize("value", ["inf", "-inf"])
    def test_infinite_values_are_a_superset(self, diff, value):
        """DIV-0044: ±inf is stored here and rejected by RTS ("invalid value")."""
        for client in (diff.reference, diff.subject):
            client.execute_command("TS.CREATE", "d:div:inf")
        assert _errors(diff.reference, "TS.ADD", "d:div:inf", 100, value)
        assert diff.subject.execute_command("TS.ADD", "d:div:inf", 100, value) == 100

    def test_duration_string_timestamp_is_a_superset(self, diff):
        """DIV-0041 on the TS.ADD surface: `1.5` parses as a duration here (1500 ms)
        and is rejected by RTS as an invalid timestamp."""
        for client in (diff.reference, diff.subject):
            client.execute_command("TS.CREATE", "d:div:ts")
        assert _errors(diff.reference, "TS.ADD", "d:div:ts", "1.5", 1.0)
        assert diff.subject.execute_command("TS.ADD", "d:div:ts", "1.5", 1.0) == 1500

