"""Guards for the parallel-execution harness (docs/plans/parallel-integration-tests-plan.md).

None of this starts a server; it checks the invariants that make it safe for several
pytest-xdist workers to share a machine. They are worth asserting because every one of
them fails *silently* — as a flaky test somewhere else, hours later — rather than at the
point of breakage.
"""

import os

import pytest

# Imported from its own module, not from conftest: several importable modules
# are named "conftest" during a run.
from parallel_ports import SafePortTracker


def test_framework_port_tracker_is_patched():
    """The framework's tracker must be replaced, not merely shadowed.

    Test modules do `from valkeytestframework.conftest import resource_port_tracker`,
    so a fixture defined only in our conftest would be bypassed. The patch in
    tests/conftest.py rebinds the framework's own module attributes; if a future
    refactor drops it, every worker goes back to allocating from one shared range.
    """
    import valkeytestframework.conftest as fw

    assert fw.PortTracker is SafePortTracker


def test_worker_bands_do_not_overlap(monkeypatch):
    """Two workers must never be offered the same port, bus port included."""
    bands = []
    for idx in range(8):
        monkeypatch.setenv("PYTEST_XDIST_WORKER", f"gw{idx}")
        tracker = SafePortTracker(f"node-{idx}")
        start = tracker.band_start
        bands.append(range(start, start + tracker.band_size))

    for i, first in enumerate(bands):
        for second in bands[i + 1:]:
            assert not (
                first.start < second.stop and second.start < first.stop
            ), f"client bands overlap: {first} and {second}"

            bus_first = range(
                first.start + SafePortTracker.CLUSTER_BUS_PORT_OFFSET,
                first.stop + SafePortTracker.CLUSTER_BUS_PORT_OFFSET,
            )
            bus_second = range(
                second.start + SafePortTracker.CLUSTER_BUS_PORT_OFFSET,
                second.stop + SafePortTracker.CLUSTER_BUS_PORT_OFFSET,
            )
            assert not (bus_first.start < bus_second.stop and bus_second.start < bus_first.stop)


def test_client_and_bus_bands_do_not_collide():
    """The bus band sits above every client port, and both stay out of the
    ephemeral range (Linux starts at 32768)."""
    highest_client = SafePortTracker.BASE_PORT + (
        SafePortTracker.PORTS_PER_WORKER * SafePortTracker.MAX_WORKERS
    )
    assert SafePortTracker.CLUSTER_BUS_PORT_OFFSET >= (
        highest_client - SafePortTracker.BASE_PORT
    )
    assert highest_client + SafePortTracker.CLUSTER_BUS_PORT_OFFSET <= 32768


def test_allocated_port_is_in_band_and_reserves_bus_port(monkeypatch):
    monkeypatch.setenv("PYTEST_XDIST_WORKER", "gw3")
    with SafePortTracker("node") as tracker:
        port = tracker.get_unused_port()
        assert tracker.band_start <= port < tracker.band_start + tracker.band_size
        # Both the client port and its cluster-bus partner must be held, or a
        # cluster test will lose its bus port to another test.
        assert port in tracker.locked_fds
        assert port + SafePortTracker.CLUSTER_BUS_PORT_OFFSET in tracker.locked_fds


def test_ports_are_not_immediately_reused(monkeypatch):
    """Consecutive allocations must advance, so a socket still in TIME_WAIT is not
    handed straight back out."""
    monkeypatch.setenv("PYTEST_XDIST_WORKER", "gw4")
    with SafePortTracker("node") as tracker:
        first = tracker.get_unused_port()
        second = tracker.get_unused_port()
    assert first != second


def test_lock_files_survive_release(monkeypatch):
    """Lock files must not be unlinked on release.

    Removing them lets a second process create a new inode at the same path and lock
    that instead, so the lock excludes nobody. This is the bug in the framework's
    tracker that the parallel work exists to avoid.
    """
    monkeypatch.setenv("PYTEST_XDIST_WORKER", "gw5")
    with SafePortTracker("node") as tracker:
        port = tracker.get_unused_port()
    lock_path = os.path.join(SafePortTracker.LOCKS_DIR, "port_%d.lock" % port)
    assert os.path.exists(lock_path)


def test_serial_run_owns_the_whole_band(monkeypatch):
    """Without xdist there is nobody to partition against, so a serial run should
    have at least as much room as it had before this change."""
    monkeypatch.delenv("PYTEST_XDIST_WORKER", raising=False)
    tracker = SafePortTracker("node")
    assert tracker.band_start == SafePortTracker.BASE_PORT
    assert tracker.band_size == SafePortTracker.PORTS_PER_WORKER * SafePortTracker.MAX_WORKERS


def test_worker_index_beyond_the_bands_is_rejected(monkeypatch):
    """Better a clear error than two workers quietly sharing a band."""
    monkeypatch.setenv("PYTEST_XDIST_WORKER", f"gw{SafePortTracker.MAX_WORKERS}")
    with pytest.raises(RuntimeError, match="exceeds"):
        SafePortTracker("node")


def test_test_dir_is_worker_scoped():
    """Under xdist, TEST_DIR must name the worker; serially it must not."""
    from common import TEST_DIR

    worker = os.environ.get("PYTEST_XDIST_WORKER")
    if worker:
        assert os.path.basename(os.path.normpath(TEST_DIR)) == worker
    else:
        assert os.path.basename(os.path.normpath(TEST_DIR)) == "test-data"
