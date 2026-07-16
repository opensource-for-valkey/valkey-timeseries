"""Configuration parity with RedisTimeSeries 8.6 (test plan §7.1).

Diffs the `ts-*` config surface between the two engines: names, defaults,
mutability (CONFIG SET accept/reject), and value validation — including the
COMPACTION_POLICY policy-string grammar. Extra subject-only configs are an
allowed superset; a missing or mismatched RTS name fails.

Naming: valkey 8.0's module-config API always namespaces a config under the
module name, so every subject config is reachable only as `ts.<name>`
(module name "ts"): RTS `ts-chunk-size-bytes` == subject
`ts.ts-chunk-size-bytes`. The comparison here is therefore modulo that `ts.`
prefix; the prefix itself is registered as DIV-0008 (`config-name`) — valkey
8.0 offers no unprefixed registration, so the suffix is the name we control,
and it must match RTS exactly.

CONFIG SET outcomes are compared as ok/error only — the error *text* is server
wrapping (`CONFIG SET failed (possibly related to argument ...)`) and is not
part of the module's compatibility surface.

Written clean-room from public RedisTimeSeries documentation and black-box
observation of the reference server. Do NOT consult RedisTimeSeries source or
test code (see tests/compat/README.md).
"""

import pytest
import valkey
from valkey.exceptions import ResponseError

# The complete RTS 8.6 ts-* surface with its defaults, observed black-box
# against the pinned reference image (values as CONFIG GET reports them).
RTS_CONFIG_DEFAULTS = {
    "ts-retention-policy": "0",
    "ts-ignore-max-time-diff": "0",
    "ts-ignore-max-val-diff": "0.000000",
    "ts-encoding": "compressed",
    "ts-compaction-policy": "",
    "ts-num-threads": "3",
    "ts-duplicate-policy": "block",
    "ts-chunk-size-bytes": "4096",
}

IMMUTABLE_CONFIGS = {"ts-num-threads"}

# Registered default-value divergences: subject default != reference default,
# by reviewed decision. Keyed by canonical name -> (expected subject value,
# registry entry). A subject default matching neither side fails.
DEFAULT_DIVERGENCES = {
    "ts-num-threads": ("4", "DIV-0009"),
}

# valkey 8.0 module configs are always `<module>.<name>` (DIV-0008).
SUBJECT_PREFIX = "ts."


class ConfigClient:
    """A client plus the engine's module-config name prefix; all names passed
    in and returned are the canonical (RTS) unprefixed names."""

    def __init__(self, client, prefix: str):
        self.client = client
        self.prefix = prefix

    def get_all(self) -> dict:
        raw = self.client.config_get(f"{self.prefix}ts-*")
        return {name[len(self.prefix):]: value for name, value in raw.items()}

    def set_outcome(self, name: str, value) -> str:
        try:
            self.client.config_set(f"{self.prefix}{name}", value)
            return "ok"
        except ResponseError:
            return "error"

    def close(self):
        self.client.close()


def _normalize(value: str):
    """Compare config values numerically when both parse ("0" == "0.000000"),
    case-insensitively otherwise ("block" == "BLOCK")."""
    try:
        return float(value)
    except ValueError:
        return value.lower()


@pytest.fixture(scope="module")
def clients(subject_url, reference_url):
    # Config replies are plain strings on both protocols; RESP2 suffices.
    subject = ConfigClient(valkey.Valkey.from_url(subject_url), SUBJECT_PREFIX)
    reference = ConfigClient(valkey.Valkey.from_url(reference_url), "")
    yield subject, reference
    subject.close()
    reference.close()


@pytest.fixture(autouse=True)
def restore_configs(clients):
    """Configs are server-global (not cleared by FLUSHALL); snapshot both
    engines and restore every mutable ts-* value after each test.

    The reference is additionally reset to the frozen RTS defaults *before* the
    snapshot: the container outlives pytest sessions (COMPAT_KEEP_REFERENCE, or
    an externally managed COMPAT_REFERENCE_URL), so an interrupted earlier
    session can leave a mutated value behind — and a snapshot of that dirty
    state would faithfully restore the dirt. This repairs every *reversible*
    config; `ts-encoding` is a one-way latch on RTS and can not be reset here
    (the defaults test tolerates it explicitly). The subject is launched fresh
    per session and may legitimately differ from RTS defaults (DIV-0009).
    """
    subject, reference = clients
    current = reference.get_all()
    for name, value in RTS_CONFIG_DEFAULTS.items():
        if name in IMMUTABLE_CONFIGS or name == "ts-encoding":
            continue
        if current.get(name) != value:
            reference.client.config_set(f"{reference.prefix}{name}", value)

    snapshots = [(c, c.get_all()) for c in clients]
    yield
    for client, snapshot in snapshots:
        current = client.get_all()
        for name, value in snapshot.items():
            if name not in IMMUTABLE_CONFIGS and current.get(name) != value:
                client.client.config_set(f"{client.prefix}{name}", value)


class TestConfigSurface:
    def test_rts_names_present_with_matching_defaults(self, clients):
        subject, reference = clients
        subject_cfg = subject.get_all()
        reference_cfg = reference.get_all()

        # The frozen baseline above must still describe the live reference — a
        # mismatch means the pinned image changed under us. `ts-encoding` is
        # excluded from the strict equality because it is a one-way latch on RTS
        # (DIV-0018): once test_encoding_is_a_one_way_latch_on_reference runs, a
        # kept container reports `uncompressed` forever, which is expected, not
        # drift. It is still checked below as a name-present + divergence case.
        reference_baseline = {
            k: v for k, v in reference_cfg.items() if k != "ts-encoding"
        }
        expected_baseline = {
            k: v for k, v in RTS_CONFIG_DEFAULTS.items() if k != "ts-encoding"
        }
        assert reference_baseline == expected_baseline

        missing = sorted(set(reference_cfg) - set(subject_cfg))
        assert not missing, f"subject is missing RTS config name(s): {missing}"

        # The subject's ts-encoding default must match RTS's *default*
        # (compressed), compared against the frozen baseline rather than the
        # live reference value, which may be latched (see above).
        assert _normalize(subject_cfg["ts-encoding"]) == _normalize(
            RTS_CONFIG_DEFAULTS["ts-encoding"]
        )

        mismatched = {}
        for name, ref_value in reference_cfg.items():
            if name == "ts-encoding":
                continue  # handled above; live reference value may be latched
            if name in DEFAULT_DIVERGENCES:
                expected, entry_id = DEFAULT_DIVERGENCES[name]
                assert _normalize(subject_cfg[name]) == _normalize(expected), (
                    f"{name}: subject default {subject_cfg[name]!r} matches neither "
                    f"the reference ({ref_value!r}) nor its registered divergence "
                    f"{entry_id} ({expected!r})"
                )
            elif _normalize(subject_cfg[name]) != _normalize(ref_value):
                mismatched[name] = (subject_cfg[name], ref_value)
        assert not mismatched, f"default mismatches (subject, reference): {mismatched}"

        # Subject-only ts-* configs are an allowed superset (extensions).
        extras = sorted(set(subject_cfg) - set(reference_cfg))
        for name in extras:
            assert name.startswith("ts-")

    def test_mutability_parity(self, clients):
        subject, reference = clients
        for name, default in RTS_CONFIG_DEFAULTS.items():
            expected = "error" if name in IMMUTABLE_CONFIGS else "ok"
            # A self-assignment probes writability without changing state.
            for side in (reference, subject):
                outcome = side.set_outcome(name, default)
                assert outcome == expected, (
                    f"prefix={side.prefix!r}: CONFIG SET {name} => {outcome}, expected {expected}"
                )


class TestValueValidation:
    CHUNK_SIZE_CASES = [
        ("48", "ok"),        # RTS range minimum
        ("8192", "ok"),
        ("1048576", "ok"),   # RTS range maximum (1 MiB)
        ("40", "error"),     # below minimum
        ("47", "error"),
        ("100", "error"),    # in range but not a multiple of 8
        ("2097152", "error"),  # above maximum
    ]

    @pytest.mark.parametrize("value,expected", CHUNK_SIZE_CASES)
    def test_chunk_size_bytes(self, clients, value, expected):
        subject, reference = clients
        assert reference.set_outcome("ts-chunk-size-bytes", value) == expected
        assert subject.set_outcome("ts-chunk-size-bytes", value) == expected

    def test_duplicate_policy_values(self, clients):
        subject, reference = clients
        for policy in ("block", "first", "last", "min", "max", "sum"):
            assert reference.set_outcome("ts-duplicate-policy", policy) == "ok"
            assert subject.set_outcome("ts-duplicate-policy", policy) == "ok"
        assert reference.set_outcome("ts-duplicate-policy", "bogus") == "error"
        assert subject.set_outcome("ts-duplicate-policy", "bogus") == "error"

    def test_encoding_values(self, clients):
        """`ts-encoding` value grammar, exercised without dirtying the reference.

        RTS's `ts-encoding` is a one-way latch (DIV-0018): CONFIG SET away from
        `compressed` succeeds, but it can never be set back — no fixture can
        restore it, so a single `set ... uncompressed` on the shared reference
        container permanently breaks the defaults check for the rest of the
        session. We therefore exercise the reversible value grammar on the
        subject only, and on the reference verify just the two things that leave
        it pristine: a self-set of its current value is accepted, and a bogus
        value is rejected.
        """
        subject, reference = clients
        for encoding in ("compressed", "uncompressed", "COMPRESSED"):
            assert subject.set_outcome("ts-encoding", encoding) == "ok"
        assert subject.set_outcome("ts-encoding", "bogus") == "error"

        current = reference.get_all()["ts-encoding"]
        assert reference.set_outcome("ts-encoding", current) == "ok"
        assert reference.set_outcome("ts-encoding", "bogus") == "error"

    def test_encoding_is_a_one_way_latch_on_reference(self, clients):
        """DIV-0018: RTS `ts-encoding` can leave `compressed` but never return;
        ours is fully reversible. Runs last (dirties the reference irreversibly),
        and is the reason `restore_configs` reference-resets what it can.

        Pinned per-engine: this is a behavior divergence the registry can only
        match with an over-broad rule.
        """
        subject, reference = clients

        # Demonstrating the latch needs a reference that starts at compressed;
        # a kept container already latched by a prior run can not show it again.
        if reference.get_all()["ts-encoding"] != "compressed":
            pytest.skip("reference ts-encoding already latched (kept container)")

        # Subject: reversible.
        assert subject.set_outcome("ts-encoding", "uncompressed") == "ok"
        assert subject.get_all()["ts-encoding"] == "uncompressed"
        assert subject.set_outcome("ts-encoding", "compressed") == "ok"
        assert subject.get_all()["ts-encoding"] == "compressed"

        # Reference: latches. The set returns ok but the value never returns to
        # compressed. This irreversibly dirties the container — no assertion may
        # follow that depends on ts-encoding being at its default.
        assert reference.set_outcome("ts-encoding", "uncompressed") == "ok"
        assert reference.get_all()["ts-encoding"] == "uncompressed"
        assert reference.set_outcome("ts-encoding", "compressed") == "ok"
        assert reference.get_all()["ts-encoding"] == "uncompressed", (
            "expected RTS ts-encoding to latch; if this now reverts, the image "
            "changed and DIV-0018 should be removed"
        )

    def test_retention_policy_values(self, clients):
        subject, reference = clients
        for value, expected in (("0", "ok"), ("60000", "ok"), ("-1", "error")):
            assert reference.set_outcome("ts-retention-policy", value) == expected
            assert subject.set_outcome("ts-retention-policy", value) == expected

    COMPACTION_POLICY_CASES = [
        # aggregator:bucketDuration:retention[:alignTimestamp], ';'-separated
        ("max:1M:1h", "ok"),
        ("avg:2h:10d", "ok"),
        ("avg:2h:10d:2d", "ok"),
        ("std.p:1m:1h", "ok"),
        ("avg:1h:0", "ok"),  # zero retention
        ("max:1M:1h;min:10s:5d", "ok"),
        ("max:1M:1h;min:10s:5d:5m", "ok"),
        ("", "ok"),            # clears the policy
        ("max:1M", "error"),   # too few fields
        ("max", "error"),
        ("bogus:x:y", "error"),
    ]

    @pytest.mark.parametrize("value,expected", COMPACTION_POLICY_CASES)
    def test_compaction_policy_grammar(self, clients, value, expected):
        subject, reference = clients
        assert reference.set_outcome("ts-compaction-policy", value) == expected
        assert subject.set_outcome("ts-compaction-policy", value) == expected

    @pytest.mark.xfail(
        reason="TWA aggregator (RTS's 13th) is not implemented yet — findings doc #14",
        strict=True,
    )
    def test_compaction_policy_twa(self, clients):
        """RTS accepts twa policies; flips to a hard failure once TWA lands so
        this xfail must be removed together with the finding."""
        subject, reference = clients
        assert reference.set_outcome("ts-compaction-policy", "twa:1h:1d") == "ok"
        assert subject.set_outcome("ts-compaction-policy", "twa:1h:1d") == "ok"
