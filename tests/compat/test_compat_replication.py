"""Replication & persistence self-consistency (test plan §7.5).

Three RTS-defined-observable semantics:

  1. `TS.ADD key * value` replicates as an *effect*: the replica stores the
     primary's concrete timestamp, not its own clock.
  2. `TS.INCRBY`/`TS.DECRBY` replicate deterministically: the replica
     converges to the primary's exact timestamps and accumulated values.
  3. Post-`DEBUG RELOAD` equivalence: each engine must round-trip its own
     state; the cross-engine diff is on the post-reload replies.

Item 3 uses the ordinary differential `diff` fixture (both servers run with
`enable-debug-command yes`). Items 1–2 need a primary→replica pair *per
engine*: the reference replica is a second pinned-image container pointed at
the compose-managed reference, and the subject replica is a local
valkey-server + module process pointed at the session subject. Assertions are
per-engine (replica state == primary state) because the semantic is defined
in terms of each pair; wall-clock timestamps make cross-engine value diffs
meaningless for `*`.

Written clean-room from public RedisTimeSeries documentation and black-box
observation of the reference server (see tests/compat/README.md).
"""

import os
import socket
import subprocess
import tempfile
import time
import uuid
from urllib.parse import urlparse

import pytest
import valkey
import yaml

from common import VALKEY_SERVER_PATH, get_module_path

_COMPAT_DIR = os.path.dirname(os.path.abspath(__file__))
_ROOT_DIR = os.path.dirname(os.path.dirname(_COMPAT_DIR))
COMPOSE_FILE = os.path.join(_ROOT_DIR, "docker-compose.compat.yml")

SYNC_TIMEOUT_S = 20


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def _reference_image() -> str:
    with open(COMPOSE_FILE) as f:
        compose = yaml.safe_load(f)
    return compose["services"]["reference"]["image"]


def _wait_replica_synced(replica: valkey.Valkey, timeout=SYNC_TIMEOUT_S):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            info = replica.info("replication")
            if info.get("master_link_status") == "up":
                return
        except Exception:
            pass
        time.sleep(0.3)
    raise TimeoutError("replica never reached master_link_status:up")


def _wait_propagated(primary: valkey.Valkey):
    """Block until at least one replica acked the primary's current offset."""
    assert primary.execute_command("WAIT", 1, 5000) >= 1


@pytest.fixture(scope="module")
def reference_pair(reference_url):
    """(primary, replica) clients for the reference engine. The replica is a
    second pinned-image container replicating the harness reference via the
    host-published port."""
    primary = valkey.Valkey.from_url(reference_url)
    primary_port = urlparse(reference_url).port or 6379

    port = _free_port()
    name = f"compat-ref-replica-{uuid.uuid4().hex[:8]}"
    run = subprocess.run(
        [
            "docker", "run", "-d", "--rm",
            "--name", name,
            "--add-host", "host.docker.internal:host-gateway",
            "-p", f"{port}:6379",
            _reference_image(),
            "redis-server",
            "--replicaof", "host.docker.internal", str(primary_port),
            "--enable-debug-command", "yes",
            "--save", "",
            "--appendonly", "no",
        ],
        capture_output=True,
        text=True,
    )
    if run.returncode != 0:
        pytest.skip(f"could not start reference replica container:\n{run.stderr[-500:]}")

    replica = valkey.Valkey(port=port)
    try:
        _wait_replica_synced(replica)
    except TimeoutError:
        subprocess.run(["docker", "rm", "-f", name], capture_output=True)
        pytest.skip("reference replica never synced (host.docker.internal reachability?)")

    yield primary, replica

    replica.close()
    primary.close()
    subprocess.run(["docker", "rm", "-f", name], capture_output=True)


@pytest.fixture(scope="module")
def subject_pair(subject_url):
    """(primary, replica) clients for the subject engine. The replica is a
    local valkey-server + module process replicating the session subject."""
    primary = valkey.Valkey.from_url(subject_url)
    primary_port = urlparse(subject_url).port or 6379

    port = _free_port()
    workdir = tempfile.mkdtemp(prefix="compat-subject-replica-")
    proc = subprocess.Popen(
        [
            VALKEY_SERVER_PATH,
            "--port", str(port),
            "--dir", workdir,
            "--logfile", os.path.join(workdir, "replica.log"),
            "--loadmodule", get_module_path(),
            "--replicaof", "127.0.0.1", str(primary_port),
            "--enable-debug-command", "yes",
            "--save", "",
            "--appendonly", "no",
        ],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    replica = valkey.Valkey(port=port)
    try:
        _wait_replica_synced(replica)
    except TimeoutError:
        proc.kill()
        pytest.fail("subject replica never synced")

    yield primary, replica

    replica.close()
    primary.close()
    proc.terminate()
    try:
        proc.wait(timeout=5)
    except subprocess.TimeoutExpired:
        proc.kill()


@pytest.fixture(params=["reference", "subject"])
def engine_pair(request):
    """Run each replication-semantics test against both engines' pairs."""
    pair = request.getfixturevalue(f"{request.param}_pair")
    primary, _replica = pair
    primary.flushall()
    _wait_propagated(primary)
    return pair


class TestEffectReplication:
    def test_star_timestamp_replicates_primary_timestamp(self, engine_pair):
        primary, replica = engine_pair
        primary.execute_command("TS.CREATE", "star")
        primary.execute_command("TS.ADD", "star", "*", 1.5)
        ts, value = primary.execute_command("TS.GET", "star")
        _wait_propagated(primary)

        assert replica.execute_command("TS.GET", "star") == [ts, value], (
            "replica must store the primary's concrete timestamp for '*'"
        )

    def test_incrby_with_explicit_timestamp_replicates_deterministically(self, engine_pair):
        primary, replica = engine_pair
        primary.execute_command("TS.INCRBY", "ctr", 5, "TIMESTAMP", 5000)
        primary.execute_command("TS.INCRBY", "ctr", 2.5, "TIMESTAMP", 5000)
        primary.execute_command("TS.DECRBY", "ctr", 3, "TIMESTAMP", 5000)
        primary.execute_command("TS.INCRBY", "ctr", 7, "TIMESTAMP", 6000)
        _wait_propagated(primary)

        p = primary.execute_command("TS.RANGE", "ctr", "-", "+")
        r = replica.execute_command("TS.RANGE", "ctr", "-", "+")
        assert p == r, "replica must converge to the primary's exact samples"
        # INCRBY at a fresh timestamp accumulates from the last value:
        # 5 + 2.5 - 3 = 4.5 at 5000, then 4.5 + 7 = 11.5 at 6000.
        assert p[0] == [5000, b"4.5"] and p[1] == [6000, b"11.5"]

    def test_incrby_auto_timestamp_replicates_value_not_clock(self, engine_pair):
        """Plan §7.5 assumed auto-timestamp TS.INCRBY replicates as a
        deterministic effect; the reference observably does NOT: it
        replicates the command verbatim, so the replica stamps its own clock
        (30/30 divergent timestamps when probed). Both engines share this
        behavior, so the pinned semantic is: the *value* converges, the
        timestamp may differ. Anyone needing timestamp determinism must pass
        TIMESTAMP explicitly."""
        primary, replica = engine_pair
        primary.execute_command("TS.INCRBY", "auto", 5)
        _wait_propagated(primary)

        p_ts, p_val = primary.execute_command("TS.GET", "auto")
        r_ts, r_val = replica.execute_command("TS.GET", "auto")
        assert r_val == p_val
        assert abs(r_ts - p_ts) < 5000  # same wall-clock neighborhood, not equal

    def test_compaction_and_upsert_effects_reach_replica(self, engine_pair):
        """Rules, bucket closes, upserts into closed buckets, and range
        deletes must all leave the replica identical to the primary (this is
        the surface where an unreplicated code path silently drifts)."""
        primary, replica = engine_pair
        primary.execute_command("TS.CREATE", "src")
        primary.execute_command("TS.CREATE", "dst")
        primary.execute_command("TS.CREATERULE", "src", "dst", "AGGREGATION", "avg", 1000)
        for ts, v in ((100, 1.0), (500, 2.0), (1100, 4.0), (2100, 8.0)):
            primary.execute_command("TS.ADD", "src", ts, v)
        primary.execute_command("TS.ADD", "src", 1500, 4.5)     # upsert into closed bucket
        primary.execute_command("TS.DEL", "src", 400, 600)
        _wait_propagated(primary)

        for key in ("src", "dst"):
            p = primary.execute_command("TS.RANGE", key, "-", "+")
            r = replica.execute_command("TS.RANGE", key, "-", "+")
            assert p == r, f"{key}: replica diverged from primary"


class TestDebugReloadEquivalence:
    def test_post_reload_state_diffs_clean(self, diff):
        """Each engine round-trips itself through DEBUG RELOAD; the
        cross-engine diff is on the post-reload replies (plan §7.5)."""
        diff("TS.CREATE", "rel:src",
             "RETENTION", 60000,
             "CHUNK_SIZE", 4096,
             "ENCODING", "COMPRESSED",
             "DUPLICATE_POLICY", "LAST",
             "LABELS", "sensor", "s1", "area", "us-east")
        diff("TS.CREATE", "rel:dst",
             "CHUNK_SIZE", 4096, "ENCODING", "UNCOMPRESSED", "DUPLICATE_POLICY", "BLOCK")
        diff("TS.CREATERULE", "rel:src", "rel:dst", "AGGREGATION", "sum", 1000)
        for ts, v in ((100, 1.5), (500, 2.5), (1100, 4.0), (1900, 6.0), (2100, 8.0)):
            diff("TS.ADD", "rel:src", ts, v)
        diff("TS.ADD", "rel:src", 1500, 4.5)   # upsert into a closed bucket
        diff("TS.DEL", "rel:src", 400, 600)

        diff("DEBUG", "RELOAD")

        diff("TS.INFO", "rel:src")
        diff("TS.INFO", "rel:dst")
        diff("TS.RANGE", "rel:src", "-", "+")
        diff("TS.RANGE", "rel:dst", "-", "+")
        diff("TS.RANGE", "rel:src", "-", "+", "AGGREGATION", "avg", 1000)
