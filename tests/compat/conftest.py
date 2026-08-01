"""Fixtures for the RedisTimeSeries differential compatibility harness.

See docs/rts-compatibility-test-plan.md §4.1 and tests/compat/README.md.

Server resolution:

  reference (RedisTimeSeries 8.10)
    1. COMPAT_REFERENCE_URL, if set (e.g. redis://127.0.0.1:16379) — any
       already-running reference server, including one started manually with
       `docker compose -f docker-compose.compat.yml up -d reference`.
    2. If RTS_COMPAT=1, the harness starts the pinned reference container itself
       via docker compose (and stops it at session end unless COMPAT_KEEP_REFERENCE=1).
    3. Otherwise every rts_compat test is skipped.

  subject (valkey-server + this module)
    1. COMPAT_SUBJECT_URL, if set.
    2. Otherwise a local valkey-server is launched with the module, using the same
       binary/module discovery as the rest of the integration suite (tests/common.py).
"""

from __future__ import annotations

import json
import os
import socket
import subprocess
import sys
import tempfile
import time

import pytest

_COMPAT_DIR = os.path.dirname(os.path.abspath(__file__))
_TESTS_DIR = os.path.dirname(_COMPAT_DIR)
_ROOT_DIR = os.path.dirname(_TESTS_DIR)
for _p in (_COMPAT_DIR, _TESTS_DIR):
    if _p not in sys.path:
        sys.path.insert(0, _p)

import valkey  # noqa: E402

from common import TEST_DIR, VALKEY_SERVER_PATH, get_module_path  # noqa: E402
from compat_diff import DiffClient  # noqa: E402

# PyYAML is a compat-suite-only dependency; without it, skip collection of this
# directory instead of breaking collection of the whole test tree.
try:
    from compat_registry import load_registry  # noqa: E402
except ImportError:  # pragma: no cover - environment-dependent
    load_registry = None
    collect_ignore_glob = ["test_*"]

COMPOSE_FILE = os.path.join(_ROOT_DIR, "docker-compose.compat.yml")
DEFAULT_REFERENCE_PORT = int(os.environ.get("COMPAT_REFERENCE_PORT", "16379"))
REPORT_PATH = os.environ.get(
    "COMPAT_REPORT_PATH", os.path.join(TEST_DIR, "compat-report.json")
)


def _truthy(name: str) -> bool:
    return os.environ.get(name, "").lower() in ("1", "true", "yes")


# --------------------------------------------------------------------------
# collection: everything in this directory carries the rts_compat marker
# --------------------------------------------------------------------------


def pytest_collection_modifyitems(items):
    for item in items:
        if item.fspath and _COMPAT_DIR in str(item.fspath):
            item.add_marker(pytest.mark.rts_compat)


def pytest_configure(config):
    if not hasattr(config, "_compat_divergences"):
        config._compat_divergences = []


# --------------------------------------------------------------------------
# reference server
# --------------------------------------------------------------------------


def _wait_for_ping(url: str, timeout: float = 30.0) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            client = valkey.Valkey.from_url(url, socket_connect_timeout=1)
            client.ping()
            client.close()
            return True
        except Exception:
            time.sleep(0.2)
    return False


@pytest.fixture(scope="session")
def reference_url():
    url = os.environ.get("COMPAT_REFERENCE_URL")
    if url:
        if not _wait_for_ping(url, timeout=5):
            pytest.fail(f"COMPAT_REFERENCE_URL={url} is set but not reachable")
        yield url
        return

    if not _truthy("RTS_COMPAT"):
        pytest.skip(
            "RTS compatibility tests need a reference server: set "
            "COMPAT_REFERENCE_URL to a running RedisTimeSeries 8.10 instance, or "
            "set RTS_COMPAT=1 to let the harness start the pinned Docker image "
            "(docker-compose.compat.yml)."
        )

    up = subprocess.run(
        ["docker", "compose", "-f", COMPOSE_FILE, "up", "-d", "--wait", "reference"],
        capture_output=True,
        text=True,
    )
    if up.returncode != 0:
        pytest.skip(
            "RTS_COMPAT=1 but the reference container could not be started "
            f"(is Docker running?):\n{up.stderr.strip()[-800:]}"
        )
    url = f"redis://127.0.0.1:{DEFAULT_REFERENCE_PORT}"
    if not _wait_for_ping(url):
        pytest.skip(f"reference container started but {url} never became reachable")
    yield url

    if not _truthy("COMPAT_KEEP_REFERENCE"):
        subprocess.run(
            ["docker", "compose", "-f", COMPOSE_FILE, "stop", "reference"],
            capture_output=True,
            text=True,
        )


# --------------------------------------------------------------------------
# subject server
# --------------------------------------------------------------------------


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


@pytest.fixture(scope="session")
def subject_url():
    url = os.environ.get("COMPAT_SUBJECT_URL")
    if url:
        if not _wait_for_ping(url, timeout=5):
            pytest.fail(f"COMPAT_SUBJECT_URL={url} is set but not reachable")
        yield url
        return

    server_path = VALKEY_SERVER_PATH
    if not os.path.exists(server_path):
        pytest.skip(
            f"valkey-server binary not found at {server_path}; run tests/run.sh once "
            "to build it, or set VALKEY_SERVER_PATH / COMPAT_SUBJECT_URL"
        )
    module_path = get_module_path()
    if not module_path or not os.path.exists(module_path):
        pytest.skip(
            f"module not found at {module_path!r}; build it (cargo build --release) "
            "or set MODULE_PATH"
        )

    port = _free_port()
    workdir = tempfile.mkdtemp(prefix="compat-subject-")
    logfile = os.path.join(workdir, "subject.log")
    proc = subprocess.Popen(
        [
            server_path,
            "--port", str(port),
            "--dir", workdir,
            "--logfile", logfile,
            "--loadmodule", module_path,
            "--enable-debug-command", "yes",
            "--notify-keyspace-events", "KEA",
            "--maxmemory-policy", "noeviction",
            "--save", "",
            "--appendonly", "no",
        ],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    url = f"redis://127.0.0.1:{port}"
    if not _wait_for_ping(url):
        proc.kill()
        log_tail = ""
        try:
            with open(logfile) as f:
                log_tail = "".join(f.readlines()[-25:])
        except OSError:
            pass
        pytest.fail(
            f"subject valkey-server failed to start on port {port}\n{log_tail}"
        )
    yield url

    proc.terminate()
    try:
        proc.wait(timeout=5)
    except subprocess.TimeoutExpired:
        proc.kill()


# --------------------------------------------------------------------------
# per-test differential client
# --------------------------------------------------------------------------


@pytest.fixture(params=[2, 3], ids=["resp2", "resp3"])
def protocol(request):
    return request.param


@pytest.fixture(scope="session")
def registry():
    return load_registry()


@pytest.fixture
def diff(request, subject_url, reference_url, protocol, registry):
    subject = valkey.Valkey.from_url(subject_url, protocol=protocol)
    reference = valkey.Valkey.from_url(reference_url, protocol=protocol)
    subject.flushall()
    reference.flushall()
    client = DiffClient(
        subject=subject,
        reference=reference,
        protocol=protocol,
        registry=registry,
        divergence_log=request.config._compat_divergences,
        test_id=request.node.nodeid,
    )
    yield client
    subject.close()
    reference.close()


# --------------------------------------------------------------------------
# conformance report (plan §8): divergence table + JSON artifact
# --------------------------------------------------------------------------


def pytest_terminal_summary(terminalreporter, exitstatus, config):
    events = getattr(config, "_compat_divergences", [])
    if not events:
        return

    terminalreporter.section("RTS compatibility: registered divergences (XFAIL-DIVERGENT)")
    by_entry = {}
    for event in events:
        by_entry.setdefault(event.entry_id, []).append(event)
    for entry_id, entry_events in sorted(by_entry.items()):
        commands = sorted({e.command for e in entry_events})
        terminalreporter.write_line(
            f"  {entry_id}: {len(entry_events)} occurrence(s) across {', '.join(commands)}"
        )

    try:
        os.makedirs(os.path.dirname(REPORT_PATH), exist_ok=True)
        with open(REPORT_PATH, "w") as f:
            json.dump(
                {
                    "divergences": [
                        {
                            "entry_id": e.entry_id,
                            "command": e.command,
                            "delta_kind": e.delta_kind,
                            "detail": e.detail,
                            "test_id": e.test_id,
                            "protocol": e.protocol,
                        }
                        for e in events
                    ],
                },
                f,
                indent=2,
            )
        terminalreporter.write_line(f"  report written to {REPORT_PATH}")
    except OSError as exc:
        terminalreporter.write_line(f"  (could not write {REPORT_PATH}: {exc})")
