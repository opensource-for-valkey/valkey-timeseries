# tests/test_ts_flush_index.py
"""The secondary index must not outlive the keyspace it describes.

The module subscribes to FLUSHDB directly (it needs `RedisModuleFlushInfo::dbnum`, which the
`#[flush_event_handler]` surface does not carry). Two things went wrong with that:

  * the `dbnum == -1` case -- FLUSHALL, and the implicit flush a replica performs on a full
    resync -- dropped only the delayed-keys map, never the indexes themselves; and
  * that raw subscription silently displaced valkey-module's own flush dispatcher, so
    `IS_FLUSHING` was never set and the per-key `free`/`unlink` callbacks retired each series
    from the index one at a time.

The second bug masked the first: the per-key path happened to leave the index correct after a
FLUSHALL, at O(series) cost. Restoring the dispatcher without closing the `dbnum == -1` gap
leaves the entire index dangling, every id naming a key that no longer exists -- which is what
these tests pin.
"""
import pytest
from valkeytestframework.conftest import resource_port_tracker
from valkey_timeseries_test_case import ValkeyTimeSeriesTestCaseBase

FILTER = "name=flushprobe"


class TestFlushIndexConsistency(ValkeyTimeSeriesTestCaseBase):
    def _seed(self, client, key: str):
        client.execute_command("TS.CREATE", key, "LABELS", "name", "flushprobe")
        client.execute_command("TS.ADD", key, 1000, 1.0)

    def _indexed(self, client) -> list:
        return client.execute_command("TS.QUERYINDEX", FILTER)

    def test_flushdb_clears_the_index(self):
        client = self.server.get_new_client()
        self._seed(client, "flush:a")
        assert self._indexed(client) == [b"flush:a"]

        client.execute_command("FLUSHDB")

        assert self._indexed(client) == []
        assert client.execute_command("DBSIZE") == 0

    def test_flushall_clears_every_databases_index(self):
        """The `dbnum == -1` case. With the per-key fast path live, this handler is the only
        thing that retires the index, so a gap here strands every id in every database."""
        client = self.server.get_new_client()
        self._seed(client, "flush:db0")
        client.execute_command("SELECT", 1)
        self._seed(client, "flush:db1")

        client.execute_command("SELECT", 0)
        client.execute_command("FLUSHALL")

        assert self._indexed(client) == [], "db 0 index survived FLUSHALL"
        client.execute_command("SELECT", 1)
        assert self._indexed(client) == [], "db 1 index survived FLUSHALL"

    def test_flushdb_leaves_other_databases_alone(self):
        """The converse guard: a single-database flush must not take the other indexes with it."""
        client = self.server.get_new_client()
        self._seed(client, "flush:db0")
        client.execute_command("SELECT", 1)
        self._seed(client, "flush:db1")

        client.execute_command("FLUSHDB")  # db 1 only
        assert self._indexed(client) == []

        client.execute_command("SELECT", 0)
        assert self._indexed(client) == [b"flush:db0"]

    def test_index_is_usable_again_after_flushall(self):
        """A stranded index is not just stale, it shadows the rebuilt one."""
        client = self.server.get_new_client()
        self._seed(client, "flush:before")
        client.execute_command("FLUSHALL")

        self._seed(client, "flush:after")
        assert self._indexed(client) == [b"flush:after"]
        assert client.execute_command("TS.MRANGE", "-", "+", "FILTER", FILTER) == [
            [b"flush:after", [], [[1000, b"1"]]]
        ]

    def test_swapdb_swaps_the_indexes(self):
        """SWAPDB is handled by the same raw-subscription mechanism; keep it honest."""
        client = self.server.get_new_client()
        self._seed(client, "flush:db0")
        client.execute_command("SELECT", 1)
        assert self._indexed(client) == []

        client.execute_command("SELECT", 0)
        client.execute_command("SWAPDB", 0, 1)

        assert self._indexed(client) == []
        client.execute_command("SELECT", 1)
        assert self._indexed(client) == [b"flush:db0"]
