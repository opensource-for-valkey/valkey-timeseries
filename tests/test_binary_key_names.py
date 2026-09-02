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

`TestNonUtf8KeyNames` covers the other half of binary safety: not crashing is not enough, the
name in the reply has to be the name the client used. `TS.MGET`/`TS.MRANGE`/`TS.MREVRANGE` used
to carry the key through a proto3 `string` field and a `to_string_lossy()`, so any non-UTF-8
byte came back as U+FFFD and the client could not use the returned name to address the series.
"""

import pytest
import valkey
from valkeytestframework.conftest import resource_port_tracker
from valkeytestframework.util.waiters import wait_for_equal

from valkey_timeseries_test_case import ValkeyTimeSeriesTestCaseBase

# An interior NUL is the byte that used to abort the server. These names stay otherwise-valid
# UTF-8 so a failure here points at the crash defect alone; `TestNonUtf8KeyNames` below adds the
# bytes that no UTF-8 conversion survives.
NUL_KEY = b"bin\x00key"
NUL_KEY_2 = b"bin\x00key2"
NUL_COPY = b"bin\x00copy"

# Not valid UTF-8 in any position, so a name that survives round-trip proves the bytes were never
# routed through a `String`.
RAW_KEY = b"raw\x00k\xff\xfe"
RAW_KEY_2 = b"raw\x00k2\xfd"

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


class TestNonUtf8KeyNames(ValkeyTimeSeriesTestCaseBase):
    """The multi-series replies must echo the key name byte for byte."""

    def _resp3_client(self):
        """A RESP3 (HELLO 3) client against the same server as self.client."""
        return valkey.Valkey(host=self.server.bind_ip, port=self.server.port, protocol=3)

    def _seed(self, client):
        for key, value in ((RAW_KEY, 1.0), (RAW_KEY_2, 2.0)):
            client.execute_command("TS.CREATE", key, "LABELS", "host", "raw", "grp", "g1")
            client.execute_command("TS.ADD", key, 1000, value)

    def test_mget_and_mrange_echo_the_key_verbatim(self):
        client = self.server.get_new_client()
        self._seed(client)

        # KEYS is the reference: whatever the server itself reports is what these must match.
        expected = sorted([RAW_KEY, RAW_KEY_2])
        assert sorted(client.execute_command("KEYS", "*")) == expected
        assert sorted(client.execute_command("TS.QUERYINDEX", "host=raw")) == expected

        for cmd in (
            ("TS.MGET", "FILTER", "host=raw"),
            ("TS.MRANGE", "-", "+", "FILTER", "host=raw"),
            ("TS.MREVRANGE", "-", "+", "FILTER", "host=raw"),
        ):
            got = sorted(row[0] for row in client.execute_command(*cmd))
            assert got == expected, f"{cmd[0]} mangled the key name: {got}"

    def test_resp3_replies_echo_the_key_verbatim(self):
        """RESP3 keys the map by series name, so a mangled name also collides across series."""
        client = self._resp3_client()
        self._seed(client)
        expected = sorted([RAW_KEY, RAW_KEY_2])

        for cmd in (
            ("TS.MGET", "FILTER", "host=raw"),
            ("TS.MRANGE", "-", "+", "FILTER", "host=raw"),
            ("TS.MREVRANGE", "-", "+", "FILTER", "host=raw"),
        ):
            got = sorted(client.execute_command(*cmd).keys())
            assert got == expected, f"{cmd[0]} mangled the key name: {got}"

    def test_groupby_sources_echo_the_key_verbatim(self):
        """The RESP3 `sources` metadata lists the group's member keys."""
        client = self._resp3_client()
        self._seed(client)

        reply = client.execute_command(
            "TS.MRANGE", "-", "+", "FILTER", "host=raw", "GROUPBY", "grp", "REDUCE", "sum"
        )
        group = reply[b"grp=g1"]
        sources = next(v for entry in group if isinstance(entry, dict)
                       for k, v in entry.items() if k == b"sources")
        assert sorted(sources) == sorted([RAW_KEY, RAW_KEY_2])
