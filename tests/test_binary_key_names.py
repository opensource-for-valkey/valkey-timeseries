"""Regression tests for key names that are not valid C strings.

Valkey key names are binary-safe: `TS.CREATE "a\\x00b"` is a legal key, and those same bytes come
back out of the postings index, an RDB reload, a `RENAME`, a `COPY`, and a `MOVE`. The module used
to funnel every one of those conversions through `Context::create_string`, which is
`CString::new(..).unwrap()` underneath and panics on an interior NUL. Most of the affected call
sites run inside a keyspace-notification or data-type callback -- `extern "C"`, so the panic could
not unwind and became an `abort` that took the whole server down. `common::context::create_key_string`
is the binary-safe replacement; these tests keep every path that handles keyspace-derived bytes on it.

The same defect is what made a corrupt RDB crash the server rather than fail the load, since a
mutated byte can land in a key name -- see test_rdb_corrupt_load.py.
"""

import pytest
from valkeytestframework.conftest import resource_port_tracker
from valkeytestframework.util.waiters import wait_for_equal

from valkey_timeseries_test_case import ValkeyTimeSeriesTestCaseBase

# An interior NUL is the byte that used to abort the server. The names stay otherwise-valid UTF-8
# on purpose: `TS.MGET`/`TS.MRANGE`/`TS.MREVRANGE` echo the key name back through a lossy UTF-8
# conversion, so a byte like \xff comes back as U+FFFD. That is a separate reply-encoding defect,
# not this one, and mixing the two here would make these tests fail for the wrong reason.
NUL_KEY = b"bin\x00key"
NUL_KEY_2 = b"bin\x00key2"
NUL_COPY = b"bin\x00copy"

CRASH_MARKERS = ("Guru Meditation", "panicked at", "STACK TRACE", "VALKEY BUG REPORT")


class TestBinaryKeyNames(ValkeyTimeSeriesTestCaseBase):

    def _assert_no_crash(self):
        assert self.server.is_alive(), "server died handling a binary key name"
        for marker in CRASH_MARKERS:
            assert not self.server.verify_string_in_logfile(marker), \
                f'binary key name produced "{marker}" in the log'

    def _seed(self, client, key=NUL_KEY):
        client.execute_command("TS.CREATE", key, "LABELS", "host", "bin")
        for i in range(1, 6):
            client.execute_command("TS.ADD", key, 1000 * i, float(i))

    def test_commands_accept_a_nul_in_the_key_name(self):
        """The read paths resolve ids back to key names through the postings index."""
        client = self.server.get_new_client()
        self._seed(client)

        assert client.execute_command("TS.GET", NUL_KEY) == [5000, b"5"]
        assert client.execute_command("TS.QUERYINDEX", "host=bin") == [NUL_KEY]
        assert client.execute_command("TS.CARD", "FILTER", "host=bin") == 1
        assert client.execute_command("TS.MGET", "FILTER", "host=bin")[0][0] == NUL_KEY
        assert client.execute_command(
            "TS.MRANGE", "-", "+", "FILTER", "host=bin")[0][0] == NUL_KEY
        self._assert_no_crash()

    def test_keyspace_events_accept_a_nul_in_the_key_name(self):
        """RENAME/COPY/MOVE maintain the index from keyspace notifications."""
        client = self.server.get_new_client()
        self._seed(client)

        assert client.rename(NUL_KEY, NUL_KEY_2)
        assert client.execute_command("TS.QUERYINDEX", "host=bin") == [NUL_KEY_2]
        assert client.rename(NUL_KEY_2, NUL_KEY)

        assert client.execute_command("COPY", NUL_KEY, NUL_COPY)
        assert sorted(client.execute_command("TS.QUERYINDEX", "host=bin")) == \
            sorted([NUL_KEY, NUL_COPY])
        assert client.execute_command("TS.GET", NUL_COPY) == [5000, b"5"]

        assert client.execute_command("MOVE", NUL_COPY, 1)
        assert client.execute_command("TS.QUERYINDEX", "host=bin") == [NUL_KEY]
        self._assert_no_crash()

    def test_mdel_accepts_a_nul_in_the_key_name(self):
        client = self.server.get_new_client()
        self._seed(client)

        assert client.execute_command("TS.MDEL", "FILTER", "host=bin") == 1
        assert client.execute_command("TS.QUERYINDEX", "host=bin") == []
        self._assert_no_crash()

    def test_reload_accepts_a_nul_in_the_key_name(self):
        """The `loaded` notification -- the path a corrupt RDB used to abort the server on."""
        client = self.server.get_new_client()
        self._seed(client)

        client.bgsave()
        self.server.wait_for_save_done()
        self.server.restart(remove_rdb=False, remove_nodes_conf=False, connect_client=True)
        assert self.server.is_alive()
        wait_for_equal(lambda: self.server.is_rdb_done_loading(), True)

        client = self.server.get_new_client()
        assert client.execute_command("TS.QUERYINDEX", "host=bin") == [NUL_KEY]
        assert client.execute_command("TS.GET", NUL_KEY) == [5000, b"5"]
        self._assert_no_crash()
