"""Integration-suite conftest.

Two jobs:

1. Put the vendored valkey-test-framework on ``sys.path`` (it lives under
   ``tests/build/`` and is cloned by ``build.sh``).
2. Make the suite safe to run under ``pytest-xdist``. See
   ``docs/plans/parallel-integration-tests-plan.md``.

The parallel-safety work is entirely inert when xdist is not in use: with no
``PYTEST_XDIST_WORKER`` in the environment everything below resolves to a single
"master" worker that owns the whole port band and the unsuffixed test directory,
so a serial run behaves as it always has.
"""

import os
import sys

import pytest

# Set the path to find and use the valkey-test-framework
sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), 'build')))
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), 'build/valkeytestframework')))


# ---------------------------------------------------------------------------
# per-worker filesystem isolation
# ---------------------------------------------------------------------------
#
# Server artifacts are already named by port (logfile_<port>, testrdb-<port>.rdb)
# or by test name, so they do not collide across workers once ports are disjoint.
# Giving each worker its own directory anyway keeps one worker's logs together,
# which matters a great deal when triaging a failure out of interleaved output.
#
# This must happen before `common` is imported by any test module, which is why it
# lives at conftest import time rather than in a fixture.

_WORKER_ID = os.environ.get("PYTEST_XDIST_WORKER")
_TESTS_DIR = os.path.dirname(os.path.abspath(__file__))
_ROOT_DIR = os.path.dirname(_TESTS_DIR)

if _WORKER_ID:
    _base_test_dir = os.environ.get("TEST_DIR") or os.path.join(_ROOT_DIR, "test-data")
    if os.path.basename(os.path.normpath(_base_test_dir)) != _WORKER_ID:
        _worker_test_dir = os.path.join(_base_test_dir, _WORKER_ID)
        os.makedirs(_worker_test_dir, exist_ok=True)
        os.environ["TEST_DIR"] = _worker_test_dir
        # common.py derives LOGS_DIR from TEST_DIR unless it is set explicitly;
        # keep an explicit LOGS_DIR consistent with the worker directory too.
        _base_logs_dir = os.environ.get("LOGS_DIR")
        if _base_logs_dir and os.path.basename(os.path.normpath(_base_logs_dir)) != _WORKER_ID:
            _worker_logs_dir = os.path.join(_base_logs_dir, _WORKER_ID)
            os.makedirs(_worker_logs_dir, exist_ok=True)
            os.environ["LOGS_DIR"] = _worker_logs_dir


from parallel_ports import SafePortTracker  # noqa: E402  (needs sys.path above)


# ---------------------------------------------------------------------------
# collision-free ports
# ---------------------------------------------------------------------------
#
# The tracker itself lives in tests/parallel_ports.py; see the note there on why it
# is not defined in this file.


@pytest.fixture(scope="function", autouse=True)
def resource_port_tracker(request):
    """Per-test port tracker.

    Test modules import this name from ``valkeytestframework.conftest`` directly,
    so the definition there is replaced as well (below) rather than relying on
    this one shadowing it.
    """
    with SafePortTracker(request.node.nodeid) as tracker:
        yield tracker


# Replace the framework's tracker in place. `resource_port_tracker` there looks
# `PortTracker` up in its own module globals when the fixture runs, so rebinding the
# attribute is enough to redirect every existing caller.
#
# The framework is vendored, so this is patched rather than fixed upstream; when the
# banded tracker lands in valkey-test-framework this block goes away.
try:
    import valkeytestframework.conftest as _fw_conftest

    _fw_conftest.PortTracker = SafePortTracker
    _fw_conftest.resource_port_tracker = resource_port_tracker
except (ImportError, AttributeError) as exc:  # pragma: no cover - broken checkout
    raise RuntimeError(
        "could not patch valkeytestframework.conftest.PortTracker; tests would "
        "allocate ports from the unsafe framework tracker"
    ) from exc
