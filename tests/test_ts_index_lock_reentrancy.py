# tests/test_ts_index_lock_reentrancy.py
"""The postings `RwLock` must never be re-entered on the same thread (INDEX-1).

`std::sync::RwLock` is not reentrant, so a thread that asks for the write lock while it still
holds the read guard blocks forever. The index has two ways to reach that:

  * a query holds the read guard and then records the dangling ids it found, which needs the
    write lock; and
  * a query holds the read guard and *opens a series key*. `RM_OpenKey` runs the server's
    lazy-expiry check, and reaping an expired key calls back into this module's `unlink`
    callback, which takes the write lock:

        series_by_selectors                     [postings read lock held]
          -> get_timeseries -> RM_OpenKey -> lookupKey -> expireIfNeeded
            -> deleteExpiredKeyAndPropagate -> dbGenericDelete
              -> unlink -> remove_series_from_index -> write_lock   [blocks forever]

A deadlocked server does not answer, so every check here runs on a connection with a socket
timeout: a regression shows up as a timeout, not as a hung test run.
"""
import os
import signal
import time

import pytest
from valkey import Valkey
from valkey.exceptions import TimeoutError as ValkeyTimeoutError
from valkeytestframework.conftest import resource_port_tracker
from valkey_timeseries_test_case import ValkeyTimeSeriesTestCaseBase

# Generous next to the microseconds these commands need, short enough that a regression is
# reported quickly.
DEADLOCK_TIMEOUT_SECS = 10

KEY = "lockprobe:k1"

# Every index reader reachable from the command surface that opens the series keys it matched.
INDEX_READERS = [
    ("TS.MRANGE", ["TS.MRANGE", "-", "+", "FILTER", "name=lockprobe"]),
    ("TS.MREVRANGE", ["TS.MREVRANGE", "-", "+", "FILTER", "name=lockprobe"]),
    ("TS.MGET", ["TS.MGET", "FILTER", "name=lockprobe"]),
    ("TS.LABELNAMES", ["TS.LABELNAMES", "FILTER", "name=lockprobe"]),
    ("TS.LABELVALUES", ["TS.LABELVALUES", "name", "FILTER", "name=lockprobe"]),
    ("TS.QUERYINDEX", ["TS.QUERYINDEX", "name=lockprobe"]),
    ("TS.MDEL", ["TS.MDEL", "FILTER", "name=lockprobe"]),
    ("TS.CARD", ["TS.CARD", "FILTER", "name=lockprobe"]),
]


class TestIndexLockReentrancy(ValkeyTimeSeriesTestCaseBase):
    def _timeout_client(self) -> Valkey:
        return Valkey(
            host=self.server.bind_ip,
            port=self.server.port,
            socket_timeout=DEADLOCK_TIMEOUT_SECS,
        )

    @staticmethod
    def _kill_server(pid: int):
        """Reap a deadlocked server so the fixture teardown does not block on its shutdown.

        Only reached on failure. The pid is captured up front, before the command under test:
        a wedged server cannot answer `INFO` either, so asking it for its own pid afterwards
        would block just as hard as the command that hung.
        """
        try:
            os.kill(pid, signal.SIGKILL)
        except OSError:
            pass

    def _series_pending_expiry(self, client: Valkey):
        """A series that is logically expired but still physically present.

        Turning off active expiry pins the state a live server only passes through briefly --
        and that a replica holds indefinitely, since it waits for the primary's DEL.
        """
        client.execute_command("DEBUG", "SET-ACTIVE-EXPIRE", 0)
        client.execute_command("TS.CREATE", KEY, "LABELS", "name", "lockprobe")
        client.execute_command("TS.ADD", KEY, 1000, 1.0)
        client.execute_command("PEXPIRE", KEY, 1)
        time.sleep(0.05)

    def _run(self, client: Valkey, pid: int, label: str, command):
        """Run `command`, turning a deadlock into a reported failure rather than a hung run."""
        started = time.time()
        try:
            return client.execute_command(*command)
        except ValkeyTimeoutError:
            # `valkey.exceptions.TimeoutError` does not derive from the builtin one -- catching
            # the builtin here would let the timeout escape, skipping the kill below and wedging
            # the fixture teardown, which shuts the server down over a connection that has no
            # timeout of its own.
            self._kill_server(pid)
            pytest.fail(
                f"{label} did not return within {DEADLOCK_TIMEOUT_SECS}s over a series pending "
                f"expiry: the postings read guard was still held when the lazy expire took the "
                f"index write lock (waited {time.time() - started:.1f}s)"
            )

    @pytest.mark.parametrize("name,command", INDEX_READERS, ids=[c[0] for c in INDEX_READERS])
    def test_expired_series_does_not_deadlock(self, name, command):
        client = self._timeout_client()
        pid = int(client.info("server")["process_id"])
        self._series_pending_expiry(client)

        self._run(client, pid, name, command)

    def test_expired_series_still_reads_correctly(self):
        """The deadlock fix must not resurrect an expired series."""
        client = self._timeout_client()
        pid = int(client.info("server")["process_id"])
        self._series_pending_expiry(client)

        assert (
            self._run(
                client, pid, "TS.MRANGE",
                ["TS.MRANGE", "-", "+", "FILTER", "name=lockprobe"],
            )
            == []
        )
        assert client.execute_command("EXISTS", KEY) == 0

    def test_live_series_reads_normally(self):
        """Guard against the checks above passing because nothing matched in the first place."""
        client = self._timeout_client()
        pid = int(client.info("server")["process_id"])
        client.execute_command("TS.CREATE", KEY, "LABELS", "name", "lockprobe")
        client.execute_command("TS.ADD", KEY, 1000, 1.0)

        assert self._run(
            client, pid, "TS.MRANGE", ["TS.MRANGE", "-", "+", "FILTER", "name=lockprobe"]
        ) == [[KEY.encode(), [], [[1000, b"1"]]]]
        assert client.execute_command("TS.QUERYINDEX", "name=lockprobe") == [KEY.encode()]
