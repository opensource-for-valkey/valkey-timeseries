"""TS.CREATE parity matrix (test plan §6, TS.CREATE row).

Covers the universal argument-parsing dimensions (each option valid / invalid /
missing value / duplicated / case-insensitive), the option-specific value
boundaries (RETENTION, ENCODING, CHUNK_SIZE, DUPLICATE_POLICY, IGNORE, LABELS),
and the key-state dimension (key exists, WRONGTYPE).

Every accepted TS.CREATE is followed by a TS.INFO diff: the reply is a bare +OK,
so the observable effect of the option is the created series' properties.

Written clean-room from public RedisTimeSeries documentation and black-box
observation of the reference server. Do NOT consult RedisTimeSeries source or
test code (see tests/compat/README.md).
"""

from __future__ import annotations

import pytest
from valkey.exceptions import ResponseError

DUPLICATE_POLICIES = ("BLOCK", "FIRST", "LAST", "MIN", "MAX", "SUM")

# CHUNK_SIZE is documented as a multiple of 8 in the range [48, 1048576].
CHUNK_SIZE_ACCEPTED = (48, 128, 4096, 1048576)
CHUNK_SIZE_REJECTED = (0, -8, 47, 49, 1048584)


class TestRetention:
    @pytest.mark.parametrize("retention", [0, 1, 1000, 86400000])
    def test_retention_accepted(self, diff, retention):
        diff("TS.CREATE", "c:ret", "RETENTION", retention)
        diff("TS.INFO", "c:ret")

    @pytest.mark.parametrize("retention", [-1, -1000])
    def test_negative_retention_rejected(self, diff, retention):
        with pytest.raises(ResponseError):
            diff("TS.CREATE", "c:ret:neg", "RETENTION", retention)
        diff("EXISTS", "c:ret:neg")

    @pytest.mark.parametrize("retention", ["abc", "", " 100", "100abc"])
    def test_non_integer_retention_rejected(self, diff, retention):
        with pytest.raises(ResponseError):
            diff("TS.CREATE", "c:ret:bad", "RETENTION", retention)

    def test_retention_missing_value(self, diff):
        with pytest.raises(ResponseError):
            diff("TS.CREATE", "c:ret:novalue", "RETENTION")

    def test_retention_duplicated_first_wins(self, diff):
        """RTS resolves each option from its first occurrence; a later repeat is
        consumed and ignored, even when its operand would not parse."""
        diff("TS.CREATE", "c:ret:dup", "RETENTION", 100, "RETENTION", 200)
        diff("TS.INFO", "c:ret:dup")
        diff("TS.CREATE", "c:ret:dup2", "RETENTION", 100, "RETENTION", "bogus")
        diff("TS.INFO", "c:ret:dup2")


class TestEncoding:
    @pytest.mark.parametrize("encoding", ["COMPRESSED", "UNCOMPRESSED"])
    def test_encoding_accepted(self, diff, encoding):
        diff("TS.CREATE", "c:enc", "ENCODING", encoding)
        diff("TS.INFO", "c:enc")

    @pytest.mark.parametrize("encoding", ["compressed", "UnCompressed"])
    def test_encoding_value_is_case_insensitive(self, diff, encoding):
        diff("TS.CREATE", "c:enc:case", "ENCODING", encoding)
        diff("TS.INFO", "c:enc:case")

    @pytest.mark.parametrize("encoding", ["", "COMPRESSED2", "PCO"])
    def test_unknown_encoding_rejected(self, diff, encoding):
        with pytest.raises(ResponseError):
            diff("TS.CREATE", "c:enc:bad", "ENCODING", encoding)

    def test_encoding_missing_value(self, diff):
        with pytest.raises(ResponseError):
            diff("TS.CREATE", "c:enc:novalue", "ENCODING")

    def test_encoding_duplicated_first_wins(self, diff):
        diff("TS.CREATE", "c:enc:dup", "ENCODING", "UNCOMPRESSED", "ENCODING", "COMPRESSED")
        diff("TS.INFO", "c:enc:dup")

    @pytest.mark.parametrize("keyword", ["COMPRESSED", "UNCOMPRESSED", "uncompressed"])
    def test_bare_encoding_keyword(self, diff, keyword):
        """The pre-ENCODING spelling RTS still accepts."""
        diff("TS.CREATE", "c:enc:bare", keyword)
        diff("TS.INFO", "c:enc:bare")

    @pytest.mark.parametrize(
        "args",
        [
            ("UNCOMPRESSED", "ENCODING", "COMPRESSED"),   # ENCODING wins from the right
            ("ENCODING", "COMPRESSED", "UNCOMPRESSED"),   # ...and from the left
            ("UNCOMPRESSED", "COMPRESSED"),               # bare vs bare: first wins
        ],
    )
    def test_bare_keyword_against_explicit_encoding(self, diff, args):
        """An explicit ENCODING beats a bare keyword wherever the two appear
        relative to each other; two bare keywords resolve first-occurrence-wins
        like every other repeated option."""
        diff("TS.CREATE", "c:enc:mixed", *args)
        diff("TS.INFO", "c:enc:mixed")


class TestChunkSize:
    @pytest.mark.parametrize("size", CHUNK_SIZE_ACCEPTED)
    def test_chunk_size_accepted(self, diff, size):
        diff("TS.CREATE", "c:cs", "CHUNK_SIZE", size)
        diff("TS.INFO", "c:cs")

    @pytest.mark.parametrize("size", CHUNK_SIZE_REJECTED)
    def test_chunk_size_rejected(self, diff, size):
        """Bounds and the multiple-of-8 rule, including both off-by-one sides of 48."""
        with pytest.raises(ResponseError):
            diff("TS.CREATE", "c:cs:bad", "CHUNK_SIZE", size)
        diff("EXISTS", "c:cs:bad")

    def test_chunk_size_missing_value(self, diff):
        with pytest.raises(ResponseError):
            diff("TS.CREATE", "c:cs:novalue", "CHUNK_SIZE")

    def test_chunk_size_non_integer_rejected(self, diff):
        with pytest.raises(ResponseError):
            diff("TS.CREATE", "c:cs:bad2", "CHUNK_SIZE", "many")


class TestDuplicatePolicy:
    @pytest.mark.parametrize("policy", DUPLICATE_POLICIES)
    def test_policy_accepted(self, diff, policy):
        diff("TS.CREATE", "c:dp", "DUPLICATE_POLICY", policy)
        diff("TS.INFO", "c:dp")

    @pytest.mark.parametrize("policy", ["block", "Last", "sUm"])
    def test_policy_value_is_case_insensitive(self, diff, policy):
        diff("TS.CREATE", "c:dp:case", "DUPLICATE_POLICY", policy)
        diff("TS.INFO", "c:dp:case")

    @pytest.mark.parametrize("policy", ["AVERAGE", "", "NONE"])
    def test_unknown_policy_rejected(self, diff, policy):
        with pytest.raises(ResponseError):
            diff("TS.CREATE", "c:dp:bad", "DUPLICATE_POLICY", policy)

    def test_policy_missing_value(self, diff):
        with pytest.raises(ResponseError):
            diff("TS.CREATE", "c:dp:novalue", "DUPLICATE_POLICY")

    def test_policy_duplicated_first_wins(self, diff):
        diff("TS.CREATE", "c:dp:dup", "DUPLICATE_POLICY", "MIN", "DUPLICATE_POLICY", "MAX")
        diff("TS.INFO", "c:dp:dup")


class TestIgnore:
    def test_ignore_accepted(self, diff):
        diff("TS.CREATE", "c:ign", "DUPLICATE_POLICY", "LAST", "IGNORE", 5, 0.5)
        diff("TS.INFO", "c:ign")

    def test_ignore_zero_thresholds(self, diff):
        diff("TS.CREATE", "c:ign:zero", "DUPLICATE_POLICY", "LAST", "IGNORE", 0, 0)
        diff("TS.INFO", "c:ign:zero")

    def test_ignore_without_last_policy(self, diff):
        """IGNORE is documented as taking effect only under DUPLICATE_POLICY LAST;
        whether it is *accepted* under another policy is the parity question."""
        diff("TS.CREATE", "c:ign:block", "DUPLICATE_POLICY", "BLOCK", "IGNORE", 5, 0.5)
        diff("TS.INFO", "c:ign:block")

    def test_ignore_without_explicit_policy(self, diff):
        diff("TS.CREATE", "c:ign:nopolicy", "IGNORE", 5, 0.5)
        diff("TS.INFO", "c:ign:nopolicy")

    @pytest.mark.parametrize("args", [(-1, 0.5), (5, -0.5), (-1, -1)])
    def test_negative_ignore_thresholds_rejected(self, diff, args):
        with pytest.raises(ResponseError):
            diff("TS.CREATE", "c:ign:neg", "IGNORE", *args)

    @pytest.mark.parametrize("args", [(), (5,)])
    def test_ignore_missing_values(self, diff, args):
        with pytest.raises(ResponseError):
            diff("TS.CREATE", "c:ign:novalue", "IGNORE", *args)

    def test_ignore_non_numeric_rejected(self, diff):
        with pytest.raises(ResponseError):
            diff("TS.CREATE", "c:ign:bad", "IGNORE", "x", "y")


class TestLabels:
    def test_labels_accepted_and_ordered_in_info(self, diff):
        """Label order in TS.INFO is part of the reply; the normalizer dict-ifies
        the label set, and this pins that both engines report the same pairs."""
        diff("TS.CREATE", "c:lbl", "LABELS", "b", "2", "a", "1", "c", "3")
        diff("TS.INFO", "c:lbl")

    def test_labels_empty_set(self, diff):
        diff("TS.CREATE", "c:lbl:none", "LABELS")
        diff("TS.INFO", "c:lbl:none")

    def test_label_empty_value(self, diff):
        with pytest.raises(ResponseError):
            diff("TS.CREATE", "c:lbl:emptyval", "LABELS", "host", "")

    def test_label_empty_name(self, diff):
        with pytest.raises(ResponseError):
            diff("TS.CREATE", "c:lbl:emptyname", "LABELS", "", "v")

    def test_duplicate_label_name(self, diff):
        diff("TS.CREATE", "c:lbl:dup", "LABELS", "host", "h1", "host", "h2")
        diff("TS.INFO", "c:lbl:dup")



class TestOptionParsing:
    @pytest.mark.parametrize(
        "option,value",
        [("retention", 100), ("chunk_size", 128), ("encoding", "COMPRESSED"),
         ("duplicate_policy", "LAST"), ("labels", None)],
    )
    def test_option_names_are_case_insensitive(self, diff, option, value):
        args = ("TS.CREATE", "c:case", option) + ((value,) if value is not None else ())
        diff(*args)
        diff("TS.INFO", "c:case")

    def test_no_options(self, diff):
        diff("TS.CREATE", "c:bare")
        diff("TS.INFO", "c:bare")

    def test_missing_key_argument(self, diff):
        with pytest.raises(ResponseError):
            diff("TS.CREATE")

    def test_all_options_together(self, diff):
        diff(
            "TS.CREATE", "c:all",
            "RETENTION", 60000,
            "ENCODING", "UNCOMPRESSED",
            "CHUNK_SIZE", 128,
            "DUPLICATE_POLICY", "LAST",
            "IGNORE", 10, 1.5,
            "LABELS", "sensor", "s1", "area", "north",
        )
        diff("TS.INFO", "c:all")

    def test_options_are_order_independent(self, diff):
        diff(
            "TS.CREATE", "c:order",
            "DUPLICATE_POLICY", "SUM",
            "CHUNK_SIZE", 256,
            "RETENTION", 5000,
            "ENCODING", "COMPRESSED",
        )
        diff("TS.INFO", "c:order")


class TestKeyStates:
    def test_key_exists_rejected(self, diff):
        diff("TS.CREATE", "c:exists")
        with pytest.raises(ResponseError):
            diff("TS.CREATE", "c:exists")

    def test_key_exists_rejected_with_different_options(self, diff):
        """The second create must not silently mutate the existing series."""
        diff("TS.CREATE", "c:exists2", "RETENTION", 100)
        with pytest.raises(ResponseError):
            diff("TS.CREATE", "c:exists2", "RETENTION", 999)
        diff("TS.INFO", "c:exists2")

    def test_wrongtype(self, diff):
        diff("SET", "c:string", "hello")
        with pytest.raises(ResponseError):
            diff("TS.CREATE", "c:string")

    def test_created_series_is_empty(self, diff):
        diff("TS.CREATE", "c:empty")
        diff("TS.RANGE", "c:empty", "-", "+")
        diff("TS.GET", "c:empty")


def _info(client, key):
    """TS.INFO as a dict, for both reply shapes (RESP2 flat array, RESP3 map)."""
    reply = client.execute_command("TS.INFO", key)
    if isinstance(reply, dict):
        return reply
    return dict(zip(reply[::2], reply[1::2]))


def _errors(client, *args):
    try:
        client.execute_command(*args)
        return False
    except ResponseError:
        return True


class TestParserDivergences:
    """The four TS.CREATE parser deltas the registry can not express.

    Each is pinned per-engine (`diff.reference` / `diff.subject`) rather than routed
    through `diff`, because they are either accepted-input supersets — which §5.2
    hard-fails and forbids registering — or over-strict rejections whose only
    matching registry regex would be broad enough to mask real regressions in the
    same delta class. See tests/compat/README.md.
    """

    @pytest.mark.parametrize("encoding", ["gorilla", "chimp"])
    def test_extra_encodings_are_a_superset(self, diff, encoding):
        """DIV-0040: gorilla/chimp are additional chunk encodings we expose."""
        assert _errors(diff.reference, "TS.CREATE", "c:div:enc", "ENCODING", encoding)
        assert diff.subject.execute_command(
            "TS.CREATE", "c:div:enc", "ENCODING", encoding
        ) == b"OK"

    @pytest.mark.parametrize("retention", ["1h", "1.5", "30m"])
    def test_duration_string_retention_is_a_superset(self, diff, retention):
        """DIV-0041: RETENTION accepts a duration string / fractional millisecond
        count; RTS accepts a plain integer only."""
        assert _errors(diff.reference, "TS.CREATE", "c:div:ret", "RETENTION", retention)
        assert diff.subject.execute_command(
            "TS.CREATE", "c:div:ret", "RETENTION", retention
        ) == b"OK"

    @pytest.mark.parametrize(
        "args",
        [
            ("BOGUS", "1"),
            ("BOGUS",),
            ("RETENTION", "100", "BOGUS", "7"),
            ("LABELS", "host", "h1", "region"),   # odd trailing label argument
        ],
    )
    def test_unrecognized_arguments_are_rejected_not_ignored(self, diff, args):
        """DIV-0042: RTS resolves the options it recognizes and silently drops
        everything else; we reject the command. Deliberately stricter — a typo in
        an option name is a bug the caller wants to hear about."""
        assert diff.reference.execute_command("TS.CREATE", "c:div:unk", *args) == b"OK"
        assert _errors(diff.subject, "TS.CREATE", "c:div:unk2", *args)

    def test_option_keywords_are_not_scanned_inside_labels(self, diff):
        """DIV-0043: RTS looks for option keywords across the whole argument list,
        so `LABELS a 1 RETENTION 100` both sets the retention and stores a label
        named RETENTION. We treat everything after LABELS as label pairs only."""
        args = ("TS.CREATE", "c:div:lbl", "LABELS", "a", "1", "RETENTION", "100")
        assert diff.reference.execute_command(*args) == b"OK"
        assert diff.subject.execute_command(*args) == b"OK"

        assert _info(diff.reference, "c:div:lbl")[b"retentionTime"] == 100
        assert _info(diff.subject, "c:div:lbl")[b"retentionTime"] == 0

        # The label set itself is identical on both engines — this pin must not
        # be allowed to mask a divergence in how LABELS pairs are stored.
        def labels(client):
            raw = _info(client, "c:div:lbl")[b"labels"]
            return dict(raw) if isinstance(raw, dict) else {k: v for k, v in raw}

        assert labels(diff.reference) == labels(diff.subject)

    def test_label_named_like_an_option_is_only_a_label_here(self, diff):
        """DIV-0043, the rejecting half: because RTS scans for keywords inside the
        label section too, a label *named* CHUNK_SIZE makes it try to parse the
        label's value as a chunk size and fail. Here it is only ever a label."""
        args = ("TS.CREATE", "c:div:lbl2", "LABELS", "CHUNK_SIZE", "x")
        assert _errors(diff.reference, *args)
        assert diff.subject.execute_command(*args) == b"OK"
