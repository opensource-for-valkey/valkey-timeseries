# tests/test_ts_lazyfree.py
"""A large series must be freed off the main thread (FREE-1).

The module type registered no `free_effort` callback. The server reads that as an effort of 1,
which is below its `LAZYFREE_THRESHOLD` of 64, so *every* series -- a multi-million-sample one
included -- was freed synchronously on the main thread: `UNLINK` and the whole `lazyfree-lazy-*`
family did nothing at all for this type.

Registering the callback moves the `free` callback onto a bio thread for anything sizeable, which
in turn matters for what `free` is allowed to do. It used to redo the index removal that `unlink`
had already performed moments earlier on the main thread, logging a warning about the missing id
every single time; that redundant write-lock acquisition would now happen from a background
thread. `unlink` therefore records the retirement on the series and `free` only acts as a
backstop -- so these tests also pin that every removal path still leaves the index clean.
"""
import time

import pytest
from valkeytestframework.conftest import resource_port_tracker
from valkey_timeseries_test_case import ValkeyTimeSeriesTestCaseBase

# `LAZYFREE_THRESHOLD` in the server's lazyfree.c. Effort must exceed it, not merely reach it.
LAZYFREE_THRESHOLD = 64

# The smallest legal chunk size, so a modest sample count still produces enough chunks to carry
# the series over the threshold without writing millions of samples in a test.
TINY_CHUNK = 48

SPURIOUS_REMOVAL_WARNING = "Tried to remove non-existing series id"


class TestLazyFree(ValkeyTimeSeriesTestCaseBase):
    def _lazyfreed(self, client) -> int:
        return int(client.info("memory")["lazyfreed_objects"])

    def _drain(self, client, timeout=5.0):
        """Wait for the bio thread to catch up, so counters are stable before we read them."""
        deadline = time.time() + timeout
        while time.time() < deadline:
            if int(client.info("memory")["lazyfree_pending_objects"]) == 0:
                return
            time.sleep(0.02)
        raise AssertionError("lazyfree queue never drained")

    def _many_chunk_series(self, client, key: str, samples: int = 400, over_threshold=True):
        """A series in many small chunks, so its free effort clears the threshold cheaply.

        `over_threshold=False` for the index tests, which only need a series to delete and would
        otherwise pay for chunks they do not use.
        """
        client.execute_command(
            "TS.CREATE", key, "CHUNK_SIZE", TINY_CHUNK, "ENCODING", "UNCOMPRESSED",
            "LABELS", "name", "lazyprobe", "key", key,
        )
        pipe = client.pipeline(transaction=False)
        for i in range(samples):
            pipe.execute_command("TS.ADD", key, 1600000000000 + i * 1000, float(i))
        pipe.execute()
        if over_threshold:
            chunks = self._chunk_count(client, key)
            assert chunks > LAZYFREE_THRESHOLD, (
                f"{key} only has {chunks} chunks; the threshold is not being exercised"
            )

    @staticmethod
    def _chunk_count(client, key: str) -> int:
        flat = client.execute_command("TS.INFO", key)
        info = {flat[i]: flat[i + 1] for i in range(0, len(flat), 2)}
        return info[b"chunkCount"]

    def _indexed(self, client) -> list:
        return sorted(client.execute_command("TS.QUERYINDEX", "name=lazyprobe"))

    # ---- the headline behaviour ---------------------------------------------------

    def test_unlinking_a_large_series_is_handed_to_the_lazyfree_thread(self):
        client = self.server.get_new_client()
        self._many_chunk_series(client, "lazy:big")
        self._drain(client)
        before = self._lazyfreed(client)

        client.execute_command("UNLINK", "lazy:big")
        self._drain(client)

        assert self._lazyfreed(client) == before + 1, (
            "the series was freed on the main thread: with no free_effort the server assumes an "
            "effort of 1 and never defers"
        )

    def test_a_small_series_is_still_freed_inline(self):
        """The converse guard. Deferring everything is slower than freeing two allocations
        here, which is why the server has a threshold rather than always deferring."""
        client = self.server.get_new_client()
        client.execute_command("TS.CREATE", "lazy:small", "LABELS", "name", "lazyprobe")
        client.execute_command("TS.ADD", "lazy:small", 1000, 1.0)
        self._drain(client)
        before = self._lazyfreed(client)

        client.execute_command("UNLINK", "lazy:small")
        self._drain(client)

        assert self._lazyfreed(client) == before

    def test_lazy_expiry_of_a_large_series_is_deferred(self):
        """`lazyfree-lazy-expire` routes an expired key through the same effort check."""
        client = self.server.get_new_client()
        client.execute_command("CONFIG", "SET", "lazyfree-lazy-expire", "yes")
        self._many_chunk_series(client, "lazy:expiring")
        self._drain(client)
        before = self._lazyfreed(client)

        client.execute_command("PEXPIRE", "lazy:expiring", 1)
        deadline = time.time() + 5.0
        while time.time() < deadline and client.execute_command("EXISTS", "lazy:expiring"):
            time.sleep(0.02)
        assert client.execute_command("EXISTS", "lazy:expiring") == 0
        self._drain(client)

        assert self._lazyfreed(client) == before + 1

    # ---- the index must survive the rearrangement ---------------------------------

    def test_delete_does_not_log_a_spurious_index_warning(self):
        """`free` used to redo `unlink`'s work, warning about the id it had just removed."""
        client = self.server.get_new_client()
        self._many_chunk_series(client, "lazy:quiet", samples=120, over_threshold=False)
        client.execute_command("UNLINK", "lazy:quiet")
        self._drain(client)
        # The warning is emitted from `free`, which may now run on a bio thread.
        time.sleep(0.2)

        assert not self.server.verify_string_in_logfile(SPURIOUS_REMOVAL_WARNING)

    @pytest.mark.parametrize("command", ["DEL", "UNLINK"])
    def test_index_is_clean_after_an_explicit_delete(self, command):
        client = self.server.get_new_client()
        self._many_chunk_series(client, "lazy:idx", samples=120, over_threshold=False)
        assert self._indexed(client) == [b"lazy:idx"]

        client.execute_command(command, "lazy:idx")
        self._drain(client)

        assert self._indexed(client) == []

    def test_index_is_clean_after_an_overwrite(self):
        """`dbOverwrite` fires `unlink` before freeing the old value."""
        client = self.server.get_new_client()
        self._many_chunk_series(client, "lazy:over", samples=120, over_threshold=False)
        assert self._indexed(client) == [b"lazy:over"]

        client.execute_command("SET", "lazy:over", "now a string")
        self._drain(client)

        assert self._indexed(client) == []

    def test_index_is_clean_after_expiry(self):
        client = self.server.get_new_client()
        self._many_chunk_series(client, "lazy:exp", samples=120, over_threshold=False)
        client.execute_command("PEXPIRE", "lazy:exp", 1)

        deadline = time.time() + 5.0
        while time.time() < deadline and client.execute_command("EXISTS", "lazy:exp"):
            time.sleep(0.02)
        assert client.execute_command("EXISTS", "lazy:exp") == 0
        self._drain(client)

        assert self._indexed(client) == []

    def test_index_follows_a_rename(self):
        """RENAME overwrites the destination, so the destination's series is retired while the
        source's must survive under its new name."""
        client = self.server.get_new_client()
        self._many_chunk_series(client, "lazy:src", samples=120, over_threshold=False)
        self._many_chunk_series(client, "lazy:dst", samples=120, over_threshold=False)
        assert self._indexed(client) == [b"lazy:dst", b"lazy:src"]

        client.execute_command("RENAME", "lazy:src", "lazy:dst")
        self._drain(client)

        assert self._indexed(client) == [b"lazy:dst"]
        assert client.execute_command("TS.INFO", "lazy:dst") is not None

    def test_index_is_clean_after_an_async_flush(self):
        """An async flush frees every value on the bio thread and retires the index wholesale
        rather than key by key."""
        client = self.server.get_new_client()
        for i in range(5):
            self._many_chunk_series(client, f"lazy:flush{i}", samples=120, over_threshold=False)

        client.execute_command("FLUSHALL", "ASYNC")
        self._drain(client)

        assert self._indexed(client) == []
        assert client.execute_command("DBSIZE") == 0
        assert not self.server.verify_string_in_logfile(SPURIOUS_REMOVAL_WARNING)
