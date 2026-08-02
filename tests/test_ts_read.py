"""Integration tests for TS.READ.

    TS.READ key timestamp [BLOCK milliseconds min_count] [MAX_COUNT max_count]

Every assertion here is anchored to an observation of the pinned RedisTimeSeries 8.10 reference
recorded in docs/ts-read-implementation-plan.md §6, or to a server mechanic established in §7.
Probe numbers in the docstrings refer to that table.

Synchronization note: these tests never sleep to wait for a reader to block. A blocked reader is
detected through `blocked_clients` in `INFO clients`, so the write that is supposed to wake it is
only issued once the server confirms the client is actually waiting. The server is single
threaded and drains ready keys before it accepts the next command, so a wakeup that was going to
happen has always happened by the time a subsequent command on another connection returns.
"""

import json
import threading

import pytest
import valkey
from valkey import ResponseError
from valkeytestframework.conftest import resource_port_tracker
from valkeytestframework.util.waiters import wait_for_equal
from valkeytestframework.valkey_test_case import ReplicationTestCase

from common import SERVER_PATH, get_module_path
from valkey_timeseries_test_case import ValkeyTimeSeriesTestCaseBase

# Bound on how long a test will wait for a blocking reply before declaring the client stuck.
REPLY_TIMEOUT = 15

# Long enough that a wakeup always wins the race against the timeout, short enough that a test
# which fails to wake still finishes quickly.
LONG_BLOCK_MS = 10_000

# Used where the *timeout* is the behavior under test.
SHORT_BLOCK_MS = 500


class _BlockedReader:
    """A `TS.READ ... BLOCK` issued from a worker thread on its own connection.

    The command monopolizes its connection until it resolves, so it cannot share the test's
    client. The reply (or the error) is captured for the test to assert on.
    """

    def __init__(self, server, args):
        self.server = server
        self.args = args
        self.client = server.get_new_client()
        self.reply = None
        self.error = None
        self._thread = threading.Thread(target=self._run, daemon=True)

    def _run(self):
        try:
            self.reply = self.client.execute_command(*self.args)
        except Exception as e:  # noqa: BLE001 - re-raised on the test thread by result()
            self.error = e

    def start(self):
        self._thread.start()
        return self

    def client_id(self):
        """The id of the connection issuing the blocking read, for CLIENT UNBLOCK."""
        for entry in self.server.get_new_client().client_list():
            if entry.get("cmd", "").lower() == "ts.read":
                return int(entry["id"])
        return None

    def result(self, timeout=REPLY_TIMEOUT):
        """Wait for the command to resolve and return its reply, re-raising any error."""
        self._thread.join(timeout)
        assert not self._thread.is_alive(), (
            f"TS.READ {self.args} never returned within {timeout}s — the client is stuck"
        )
        if self.error is not None:
            raise self.error
        return self.reply

    def is_pending(self):
        return self._thread.is_alive()

    def close(self, control_client=None):
        """Release the reader however it is currently stuck, then drop its connection.

        Called from a `finally` so that a failing assertion cannot leave a blocked client behind
        for the next test — particularly important for the `BLOCK 0` cases, which have no timeout
        of their own.
        """
        if self._thread.is_alive() and control_client is not None:
            try:
                for entry in control_client.client_list():
                    if entry.get("cmd", "").lower() == "ts.read":
                        control_client.execute_command(
                            "CLIENT", "UNBLOCK", entry["id"], "TIMEOUT"
                        )
            except Exception:  # noqa: BLE001 - best-effort cleanup
                pass
        self._thread.join(REPLY_TIMEOUT)
        try:
            self.client.close()
        except Exception:  # noqa: BLE001 - best-effort cleanup
            pass


class TsReadTestBase(ValkeyTimeSeriesTestCaseBase):
    """Shared helpers for driving and observing blocked readers."""

    def blocked_clients(self):
        return self.client.info("clients")["blocked_clients"]

    def reader(self, *args):
        """Start a blocking TS.READ and wait until the server reports it as blocked.

        Only valid for reads that will wait long enough to be observed — pass a long BLOCK, or
        use `timeout_reader` instead. A short BLOCK can expire before the poll sees it, which
        would make the wait itself flaky.
        """
        before = self.blocked_clients()
        reader = _BlockedReader(self.server, args).start()
        wait_for_equal(self.blocked_clients, before + 1, timeout=REPLY_TIMEOUT)
        return reader

    def timeout_reader(self, *args):
        """Start a blocking TS.READ without waiting for it to be observably blocked.

        For tests where the timeout *is* the behavior under test: the read may already have
        expired by the time we could look, and the assertion is on the reply, not on the client
        ever being seen in the blocked state.
        """
        return _BlockedReader(self.server, args).start()

    def seed(self, key, timestamps):
        """Create `key` with one sample per timestamp, value == timestamp / 10."""
        self.client.execute_command("TS.CREATE", key)
        for ts in timestamps:
            self.client.execute_command("TS.ADD", key, ts, ts / 10)

    @staticmethod
    def timestamps(reply):
        return [row[0] for row in reply]


class TestTsReadImmediate(TsReadTestBase):
    """TS.READ without BLOCK: it reads what exists and returns."""

    def test_reads_from_an_inclusive_cursor(self):
        """Probe #4: the sample sitting exactly on the cursor is included."""
        self.seed("k", [100, 200, 300])
        assert self.client.execute_command("TS.READ", "k", 0) == [
            [100, b"10"],
            [200, b"20"],
            [300, b"30"],
        ]
        assert self.timestamps(self.client.execute_command("TS.READ", "k", 200)) == [
            200,
            300,
        ]
        assert self.timestamps(self.client.execute_command("TS.READ", "k", 301)) == []

    def test_sentinels(self):
        """Probes #5, #6: `-` is everything, `+` is the newest sample, `$` is empty."""
        self.seed("k", [100, 200, 300])
        assert self.timestamps(self.client.execute_command("TS.READ", "k", "-")) == [
            100,
            200,
            300,
        ]
        assert self.timestamps(self.client.execute_command("TS.READ", "k", "+")) == [300]
        assert self.client.execute_command("TS.READ", "k", "$") == []

    def test_dollar_at_max_timestamp_is_empty_not_an_overflow(self):
        """Probe #7: `$` past i64::MAX reads empty; it must not wrap and return everything."""
        max_ts = 9223372036854775807
        self.client.execute_command("TS.ADD", "k", max_ts, 1.0)
        assert self.client.execute_command("TS.READ", "k", "$") == []
        assert self.timestamps(self.client.execute_command("TS.READ", "k", "+")) == [
            max_ts
        ]

    def test_missing_and_empty_series_read_empty(self):
        """Probes #1, #2: every cursor form yields an empty array, not an error."""
        self.client.execute_command("TS.CREATE", "empty")
        for cursor in [0, "-", "+", "$"]:
            assert self.client.execute_command("TS.READ", "nosuch", cursor) == []
            assert self.client.execute_command("TS.READ", "empty", cursor) == []

    def test_wrong_type_is_the_standard_error(self):
        """Probe #3: WRONGTYPE, not the `not a TSDB key` variant TS.ADD uses."""
        self.client.execute_command("SET", "str", "v")
        with pytest.raises(ResponseError, match="WRONGTYPE"):
            self.client.execute_command("TS.READ", "str", 0)

    def test_max_count_caps_the_reply(self):
        """Probe #11, plus paging by advancing to last_returned_timestamp + 1."""
        self.seed("k", [100, 200, 300, 400])
        page = self.client.execute_command("TS.READ", "k", "-", "MAX_COUNT", 2)
        assert self.timestamps(page) == [100, 200]

        page = self.client.execute_command(
            "TS.READ", "k", page[-1][0] + 1, "MAX_COUNT", 2
        )
        assert self.timestamps(page) == [300, 400]

        assert self.client.execute_command("TS.READ", "k", page[-1][0] + 1) == []

    def test_out_of_order_samples_come_back_ascending(self):
        self.client.execute_command("TS.CREATE", "k")
        for ts in [300, 100, 400, 200]:
            self.client.execute_command("TS.ADD", "k", ts, ts / 10)
        assert self.timestamps(self.client.execute_command("TS.READ", "k", "-")) == [
            100,
            200,
            300,
            400,
        ]

    def test_resp3_returns_doubles(self):
        """Probe #10: RESP2 renders values as bulk strings, RESP3 as doubles."""
        self.seed("k", [100, 200])
        assert self.client.execute_command("TS.READ", "k", "-") == [
            [100, b"10"],
            [200, b"20"],
        ]

        c3 = valkey.Valkey(host=self.server.bind_ip, port=self.server.port, protocol=3)
        try:
            assert c3.execute_command("TS.READ", "k", "-") == [[100, 10.0], [200, 20.0]]
            assert c3.execute_command("TS.READ", "nosuch", "-") == []
        finally:
            c3.close()


class TestTsReadValidation(TsReadTestBase):
    """Argument validation, including which failure *class* each input produces.

    The reference splits these two ways (probes #13–#17): duplicated or malformed options are
    plain wrong-arity, while out-of-range values get a specific TSDB: message. The split is
    observable through the error prefix, so it is part of the contract.
    """

    ARITY_ERROR = "wrong number of arguments"

    @pytest.mark.parametrize(
        "extra",
        [
            ["BLOCK", "10", "1", "BLOCK", "10", "1"],
            ["MAX_COUNT", "1", "MAX_COUNT", "2"],
            ["BLOCK", "50"],
            ["MAX_COUNT"],
            ["BOGUS"],
            ["BLOCK", "50", "1", "EXTRA"],
        ],
        ids=["dup-block", "dup-max-count", "block-no-min", "max-count-no-value",
             "stray-token", "trailing-token"],
    )
    def test_malformed_options_are_arity_errors(self, extra):
        self.seed("k", [100])
        with pytest.raises(ResponseError, match=self.ARITY_ERROR):
            self.client.execute_command("TS.READ", "k", "-", *extra)

    @pytest.mark.parametrize(
        "extra,message",
        [
            (["MAX_COUNT", "0"], "MAX_COUNT must be a positive integer"),
            (["MAX_COUNT", "-1"], "MAX_COUNT must be a positive integer"),
            (["MAX_COUNT", "abc"], "MAX_COUNT must be a positive integer"),
            (["BLOCK", "50", "0"], "BLOCK min_count must be a positive integer"),
            (["BLOCK", "50", "-1"], "BLOCK min_count must be a positive integer"),
            (["BLOCK", "-1", "1"], "BLOCK milliseconds must be a non-negative integer"),
            (["BLOCK", "abc", "1"], "BLOCK milliseconds must be a non-negative integer"),
        ],
        ids=["max-0", "max-neg", "max-nan", "min-0", "min-neg", "ms-neg", "ms-nan"],
    )
    def test_out_of_range_values_get_specific_messages(self, extra, message):
        self.seed("k", [100])
        with pytest.raises(ResponseError, match=message):
            self.client.execute_command("TS.READ", "k", "-", *extra)

    def test_negative_timestamp_is_rejected(self):
        """Probe #9."""
        self.seed("k", [100])
        with pytest.raises(ResponseError, match="invalid timestamp"):
            self.client.execute_command("TS.READ", "k", -5)

    def test_min_count_may_not_exceed_max_count(self):
        """Probe #17: rejected before the key is even looked at."""
        self.seed("k", [100])
        for args in (
            ["BLOCK", "500", "5", "MAX_COUNT", "1"],
            ["MAX_COUNT", "1", "BLOCK", "500", "5"],
        ):
            with pytest.raises(ResponseError, match="min_count must be <= MAX_COUNT"):
                self.client.execute_command("TS.READ", "k", "-", *args)

        # Same error for a key that does not exist: validation precedes key access.
        with pytest.raises(ResponseError, match="min_count must be <= MAX_COUNT"):
            self.client.execute_command(
                "TS.READ", "nosuch", "-", "BLOCK", "500", "5", "MAX_COUNT", "1"
            )

        # Equal counts are fine.
        assert self.client.execute_command(
            "TS.READ", "k", "-", "BLOCK", "500", "1", "MAX_COUNT", "1"
        ) == [[100, b"10"]]

    def test_options_in_either_order_and_any_case(self):
        """Probe #12."""
        self.seed("k", [100, 200, 300])
        expected = [[100, b"10"], [200, b"20"]]
        assert (
            self.client.execute_command(
                "TS.READ", "k", "-", "BLOCK", "50", "1", "MAX_COUNT", "2"
            )
            == expected
        )
        assert (
            self.client.execute_command(
                "TS.READ", "k", "-", "MAX_COUNT", "2", "BLOCK", "50", "1"
            )
            == expected
        )
        assert (
            self.client.execute_command(
                "TS.READ", "k", "-", "block", "50", "1", "max_count", "2"
            )
            == expected
        )


class TestTsReadBlocking(TsReadTestBase):
    """BLOCK semantics: waiting, waking, timing out, and cleaning up."""

    def test_returns_immediately_when_the_threshold_is_already_met(self):
        """No blocking at all when enough samples already qualify."""
        self.seed("k", [100, 200, 300])
        assert self.blocked_clients() == 0
        reply = self.client.execute_command(
            "TS.READ", "k", "-", "BLOCK", LONG_BLOCK_MS, 3
        )
        assert self.timestamps(reply) == [100, 200, 300]
        assert self.blocked_clients() == 0

    def test_wakes_when_the_threshold_is_reached(self):
        """Probe #19."""
        self.seed("k", [100])
        reader = self.reader("TS.READ", "k", "-", "BLOCK", LONG_BLOCK_MS, 2)
        try:
            self.client.execute_command("TS.ADD", "k", 200, 20)
            assert self.timestamps(reader.result()) == [100, 200]
        finally:
            reader.close(self.client)

    def test_timeout_returns_the_partial_result(self):
        """Probe #18: a timeout is a success reply carrying whatever exists."""
        self.seed("k", [100])
        reader = self.timeout_reader("TS.READ", "k", "-", "BLOCK", SHORT_BLOCK_MS, 3)
        try:
            assert reader.result() == [[100, b"10"]]
        finally:
            reader.close(self.client)

    def test_timeout_on_an_empty_series_returns_an_empty_array(self):
        self.client.execute_command("TS.CREATE", "k")
        reader = self.timeout_reader("TS.READ", "k", "-", "BLOCK", SHORT_BLOCK_MS, 1)
        try:
            assert reader.result() == []
        finally:
            reader.close(self.client)

    def test_max_count_caps_a_wakeup_reply(self):
        """MAX_COUNT bounds what a woken reader receives, even when more now qualifies.

        Note this is the only path on which MAX_COUNT can bite while blocking. A *timeout* reply
        can never be truncated by it: timing out means fewer than `min_count` samples exist, and
        `min_count <= max_count` is enforced up front (probe #17), so the partial result is
        always under the cap already.
        """
        self.seed("k", [100])
        reader = self.reader(
            "TS.READ", "k", "-", "BLOCK", LONG_BLOCK_MS, 2, "MAX_COUNT", 2
        )
        try:
            self.client.execute_command("TS.MADD", "k", 200, 20, "k", 300, 30)
            assert self.timestamps(reader.result()) == [100, 200]
        finally:
            reader.close(self.client)

    def test_blocks_on_a_missing_key_and_wakes_when_it_is_created(self):
        """Probe #21: the key need not exist for a client to wait on it."""
        reader = self.reader("TS.READ", "later", "-", "BLOCK", LONG_BLOCK_MS, 1)
        try:
            self.client.execute_command("TS.ADD", "later", 50, 5.0)
            assert self.timestamps(reader.result()) == [50]
        finally:
            reader.close(self.client)

    def test_block_zero_waits_indefinitely(self):
        """BLOCK 0 has no timeout; only data, deletion, or an unblock releases it."""
        self.client.execute_command("TS.CREATE", "k")
        reader = self.reader("TS.READ", "k", "-", "BLOCK", 0, 2)
        try:
            # One sample is not enough — the reader must stay put.
            self.client.execute_command("TS.ADD", "k", 100, 10)
            assert self.blocked_clients() == 1
            assert reader.is_pending()

            self.client.execute_command("TS.ADD", "k", 200, 20)
            assert self.timestamps(reader.result()) == [100, 200]
        finally:
            reader.close(self.client)

    def test_samples_are_not_consumed(self):
        """Probe #22: readers are independent; one reader's reply does not remove data."""
        self.client.execute_command("TS.CREATE", "k")
        first = self.reader("TS.READ", "k", "-", "BLOCK", LONG_BLOCK_MS, 1)
        second = self.reader("TS.READ", "k", "-", "BLOCK", LONG_BLOCK_MS, 1)
        try:
            self.client.execute_command("TS.ADD", "k", 10, 1.0)
            assert first.result() == [[10, b"1"]]
            assert second.result() == [[10, b"1"]]
            # And the data is still there for a later reader.
            assert self.client.execute_command("TS.READ", "k", "-") == [[10, b"1"]]
        finally:
            first.close(self.client)
            second.close(self.client)

    def test_independent_readers_with_different_cursors_and_caps(self):
        """Each blocked reader evaluates its own cursor, threshold, and cap."""
        self.seed("k", [100])
        everything = self.reader("TS.READ", "k", "-", "BLOCK", LONG_BLOCK_MS, 3)
        capped = self.reader(
            "TS.READ", "k", 200, "BLOCK", LONG_BLOCK_MS, 1, "MAX_COUNT", 1
        )
        try:
            # Satisfies `capped` (one sample at/after 200) but not `everything` (needs 3).
            self.client.execute_command("TS.ADD", "k", 200, 20)
            assert self.timestamps(capped.result()) == [200]
            assert everything.is_pending()

            self.client.execute_command("TS.ADD", "k", 300, 30)
            assert self.timestamps(everything.result()) == [100, 200, 300]
        finally:
            everything.close(self.client)
            capped.close(self.client)

    def test_cursor_stays_fixed_while_blocked(self):
        """A resolved cursor does not drift, and an out-of-order write below it still counts.

        `$` is resolved once, against the data present when the command started. A later sample
        *below* that cursor must not satisfy it, while one at or after it must.
        """
        self.seed("k", [100, 200])
        reader = self.reader("TS.READ", "k", "$", "BLOCK", LONG_BLOCK_MS, 1)
        try:
            # Backfill below the resolved cursor (201): not what this reader asked for.
            self.client.execute_command("TS.ADD", "k", 150, 15)
            assert self.blocked_clients() == 1
            assert reader.is_pending()

            self.client.execute_command("TS.ADD", "k", 300, 30)
            assert self.timestamps(reader.result()) == [300]
        finally:
            reader.close(self.client)

    def test_out_of_order_write_above_the_cursor_wakes_a_reader(self):
        """An out-of-order insert is not a tail append, but it does add a qualifying sample."""
        self.seed("k", [100, 500])
        reader = self.reader("TS.READ", "k", 200, "BLOCK", LONG_BLOCK_MS, 2)
        try:
            self.client.execute_command("TS.ADD", "k", 300, 30)
            assert self.timestamps(reader.result()) == [300, 500]
        finally:
            reader.close(self.client)

    def test_duplicate_write_does_not_wake_a_reader(self):
        """A write that does not increase the sample count cannot satisfy a threshold.

        Released by a real append rather than a timeout, so the test asserts on content and
        never races the clock.
        """
        self.seed("k", [100])
        reader = self.reader("TS.READ", "k", "-", "BLOCK", LONG_BLOCK_MS, 2)
        try:
            # Same timestamp: an in-place update, so the count is unchanged.
            self.client.execute_command("TS.ADD", "k", 100, 99.0, "ON_DUPLICATE", "LAST")
            assert self.blocked_clients() == 1
            assert reader.is_pending()

            # A genuine append does release it, and the updated value is what it sees.
            self.client.execute_command("TS.ADD", "k", 200, 20)
            assert reader.result() == [[100, b"99"], [200, b"20"]]
        finally:
            reader.close(self.client)


class TestTsReadDeletion(TsReadTestBase):
    """Removing the key releases blocked readers with an empty array."""

    @pytest.mark.parametrize("command", ["DEL", "UNLINK"])
    def test_delete_releases_the_reader(self, command):
        """Probe #20: deletion is a successful empty reply, not an error."""
        self.seed("k", [100])
        reader = self.reader("TS.READ", "k", "-", "BLOCK", LONG_BLOCK_MS, 9)
        try:
            self.client.execute_command(command, "k")
            assert reader.result() == []
        finally:
            reader.close(self.client)

    @pytest.mark.parametrize("command", ["FLUSHDB", "FLUSHALL"])
    def test_flush_releases_the_reader(self, command):
        self.seed("k", [100])
        reader = self.reader("TS.READ", "k", "-", "BLOCK", LONG_BLOCK_MS, 9)
        try:
            self.client.execute_command(command)
            assert reader.result() == []
        finally:
            reader.close(self.client)

    def test_expiry_releases_the_reader(self):
        self.seed("k", [100])
        reader = self.reader("TS.READ", "k", "-", "BLOCK", LONG_BLOCK_MS, 9)
        try:
            self.client.execute_command("PEXPIRE", "k", 100)
            assert reader.result() == []
        finally:
            reader.close(self.client)

    def test_wrong_type_replacing_the_key_errors_the_reader(self):
        """Waiting longer cannot help once the name holds a non-series value."""
        reader = self.reader("TS.READ", "k", "-", "BLOCK", LONG_BLOCK_MS, 1)
        try:
            self.client.execute_command("SET", "k", "not-a-series")
            with pytest.raises(ResponseError, match="WRONGTYPE"):
                reader.result()
        finally:
            reader.close(self.client)

    def test_eviction_releases_the_reader(self):
        """Eviction reaches the reader through the same deletion path as DEL.

        Nothing here is TS.READ-specific by design: REDISMODULE_BLOCK_UNBLOCK_DELETED is what
        covers every way a key can disappear, so this asserts the flag actually spans eviction
        rather than only explicit deletion. The setup is made deterministic by evicting under
        `allkeys-random` with maxmemory driven below current usage — which key is chosen does
        not matter, because the only key in the database is the watched one.
        """
        self.seed("k", [100])
        reader = self.reader("TS.READ", "k", "-", "BLOCK", LONG_BLOCK_MS, 9)
        try:
            self.client.execute_command("CONFIG", "SET", "maxmemory-policy", "allkeys-random")
            used = int(self.client.info("memory")["used_memory"])
            # Below current usage, so the next cron/command cycle must evict to comply.
            self.client.execute_command("CONFIG", "SET", "maxmemory", max(used // 2, 1))
            wait_for_equal(lambda: self.client.execute_command("EXISTS", "k"), 0,
                           timeout=REPLY_TIMEOUT)
            assert reader.result() == []
        finally:
            # Restore before anything else: a lingering maxmemory would make every later test
            # in the module fail with OOM.
            self.client.execute_command("CONFIG", "SET", "maxmemory", 0)
            self.client.execute_command("CONFIG", "SET", "maxmemory-policy", "noeviction")
            reader.close(self.client)


class TestTsReadWakeupPaths(TsReadTestBase):
    """Every path that can add a sample must be able to wake a reader."""

    def test_ts_madd(self):
        self.client.execute_command("TS.CREATE", "k")
        reader = self.reader("TS.READ", "k", "-", "BLOCK", LONG_BLOCK_MS, 1)
        try:
            self.client.execute_command("TS.MADD", "k", 100, 1.0)
            assert self.timestamps(reader.result()) == [100]
        finally:
            reader.close(self.client)

    def test_ts_madd_wakes_each_affected_key(self):
        self.client.execute_command("TS.CREATE", "a")
        self.client.execute_command("TS.CREATE", "b")
        first = self.reader("TS.READ", "a", "-", "BLOCK", LONG_BLOCK_MS, 1)
        second = self.reader("TS.READ", "b", "-", "BLOCK", LONG_BLOCK_MS, 1)
        try:
            self.client.execute_command("TS.MADD", "a", 100, 1.0, "b", 200, 2.0)
            assert self.timestamps(first.result()) == [100]
            assert self.timestamps(second.result()) == [200]
        finally:
            first.close(self.client)
            second.close(self.client)

    @pytest.mark.parametrize("command", ["TS.INCRBY", "TS.DECRBY"])
    def test_incrby_and_decrby(self, command):
        self.client.execute_command("TS.CREATE", "k")
        reader = self.reader("TS.READ", "k", "-", "BLOCK", LONG_BLOCK_MS, 1)
        try:
            self.client.execute_command(command, "k", 5)
            assert len(reader.result()) == 1
        finally:
            reader.close(self.client)

    def test_addbulk(self):
        self.client.execute_command("TS.CREATE", "k")
        reader = self.reader("TS.READ", "k", "-", "BLOCK", LONG_BLOCK_MS, 2)
        try:
            payload = json.dumps({"timestamps": [100, 200], "values": [1.0, 2.0]})
            self.client.execute_command("TS.ADDBULK", "k", payload)
            assert self.timestamps(reader.result()) == [100, 200]
        finally:
            reader.close(self.client)

    def test_compaction_destination(self):
        """A reader on a rollup key wakes when a bucket is materialized into it."""
        self.client.execute_command("TS.CREATE", "src")
        self.client.execute_command("TS.CREATE", "dest")
        self.client.execute_command(
            "TS.CREATERULE", "src", "dest", "AGGREGATION", "sum", 100
        )
        self.client.execute_command("TS.ADD", "src", 100, 1.0)

        reader = self.reader("TS.READ", "dest", "-", "BLOCK", LONG_BLOCK_MS, 1)
        try:
            # Crossing a bucket boundary closes the first bucket and writes it downstream.
            self.client.execute_command("TS.ADD", "src", 150, 2.0)
            self.client.execute_command("TS.ADD", "src", 250, 3.0)
            assert self.timestamps(reader.result()) == [100]
        finally:
            reader.close(self.client)

    def test_cascaded_compaction_destination(self):
        """A rollup of a rollup wakes its own readers, not just the first level."""
        self.client.execute_command("TS.CREATE", "src")
        self.client.execute_command("TS.CREATE", "mid")
        self.client.execute_command("TS.CREATE", "leaf")
        self.client.execute_command(
            "TS.CREATERULE", "src", "mid", "AGGREGATION", "sum", 100
        )
        self.client.execute_command(
            "TS.CREATERULE", "mid", "leaf", "AGGREGATION", "sum", 200
        )

        reader = self.reader("TS.READ", "leaf", "-", "BLOCK", LONG_BLOCK_MS, 1)
        try:
            # Enough forward progress to close a src bucket into mid, and then a mid bucket
            # into leaf.
            for ts in [100, 150, 250, 350, 450, 550]:
                self.client.execute_command("TS.ADD", "src", ts, 1.0)
            assert len(reader.result()) >= 1
        finally:
            reader.close(self.client)

    def test_restore_into_the_watched_key(self):
        """Key creation wakes readers through the server's own dbAdd signal (§7)."""
        self.seed("src", [1])
        payload = self.client.execute_command("DUMP", "src")

        reader = self.reader("TS.READ", "dst", "-", "BLOCK", LONG_BLOCK_MS, 1)
        try:
            self.client.execute_command("RESTORE", "dst", 0, payload)
            assert self.timestamps(reader.result()) == [1]
        finally:
            reader.close(self.client)

    def test_restore_replace_over_an_existing_series(self):
        """The overwrite form still lands on the creation path, because RESTORE deletes first."""
        self.seed("src", [1])
        payload = self.client.execute_command("DUMP", "src")
        self.client.execute_command("TS.CREATE", "dst")

        reader = self.reader("TS.READ", "dst", "-", "BLOCK", LONG_BLOCK_MS, 1)
        try:
            self.client.execute_command("RESTORE", "dst", 0, payload, "REPLACE")
            assert self.timestamps(reader.result()) == [1]
        finally:
            reader.close(self.client)

    def test_copy_into_the_watched_key(self):
        self.seed("src", [1])
        reader = self.reader("TS.READ", "dst", "-", "BLOCK", LONG_BLOCK_MS, 1)
        try:
            self.client.execute_command("COPY", "src", "dst")
            assert self.timestamps(reader.result()) == [1]
        finally:
            reader.close(self.client)

    def test_rename_onto_the_watched_key(self):
        self.seed("src", [1])
        reader = self.reader("TS.READ", "dst", "-", "BLOCK", LONG_BLOCK_MS, 1)
        try:
            self.client.execute_command("RENAME", "src", "dst")
            assert self.timestamps(reader.result()) == [1]
        finally:
            reader.close(self.client)

    def test_swapdb_brings_the_watched_key_into_view(self):
        """The server rescans blocked keys after a swap, so module readers are re-evaluated."""
        self.client.execute_command("TS.CREATE", "k")
        other = self.server.get_new_client()
        try:
            other.execute_command("SELECT", 1)
            other.execute_command("TS.ADD", "k", 500, 50.0)

            reader = self.reader("TS.READ", "k", "-", "BLOCK", LONG_BLOCK_MS, 1)
            try:
                self.client.execute_command("SWAPDB", 0, 1)
                assert self.timestamps(reader.result()) == [500]
            finally:
                reader.close(self.client)
        finally:
            other.close()

    def test_block_survives_debug_reload(self):
        """A reload rewrites every key; a waiting reader must neither be dropped nor spuriously
        served, and must still wake on the next write."""
        self.seed("k", [100])
        reader = self.reader("TS.READ", "k", "-", "BLOCK", LONG_BLOCK_MS, 2)
        try:
            self.client.execute_command("DEBUG", "RELOAD")
            assert self.blocked_clients() == 1
            assert reader.is_pending()

            self.client.execute_command("TS.ADD", "k", 200, 20)
            assert self.timestamps(reader.result()) == [100, 200]
        finally:
            reader.close(self.client)


class TestTsReadDenyBlocking(TsReadTestBase):
    """Contexts that forbid blocking.

    Probes #23, #24 pin an ordering that differs from BLPOP's: a *satisfiable* read succeeds
    inside MULTI or EVAL, and only an unsatisfiable one errors.
    """

    ERROR = "not allowed inside MULTI, EVAL, or a deny-blocking context"

    def test_multi_returns_data_when_the_threshold_is_met(self):
        self.seed("k", [100, 200, 300])
        pipe = self.client.pipeline(transaction=True)
        pipe.execute_command("TS.READ", "k", "-", "BLOCK", LONG_BLOCK_MS, 3)
        assert self.timestamps(pipe.execute()[0]) == [100, 200, 300]

    def test_multi_block_zero_returns_data_when_the_threshold_is_met(self):
        self.seed("k", [100])
        pipe = self.client.pipeline(transaction=True)
        pipe.execute_command("TS.READ", "k", "-", "BLOCK", 0, 1)
        assert self.timestamps(pipe.execute()[0]) == [100]

    def test_multi_errors_when_it_would_have_to_block(self):
        self.seed("k", [100, 200, 300])
        pipe = self.client.pipeline(transaction=True)
        pipe.execute_command("TS.READ", "k", "-", "BLOCK", LONG_BLOCK_MS, 4)
        with pytest.raises(ResponseError, match=self.ERROR):
            pipe.execute()
        assert self.blocked_clients() == 0

    def test_lua_returns_data_when_the_threshold_is_met(self):
        self.seed("k", [100])
        script = "return redis.call('TS.READ', KEYS[1], '-', 'BLOCK', '1000', '1')"
        assert self.client.eval(script, 1, "k") == [[100, b"10"]]

    def test_lua_errors_when_it_would_have_to_block(self):
        self.seed("k", [100])
        script = "return redis.call('TS.READ', KEYS[1], '$', 'BLOCK', '1000', '1')"
        with pytest.raises(ResponseError, match=self.ERROR):
            self.client.eval(script, 1, "k")
        assert self.blocked_clients() == 0

    def test_a_non_blocking_read_is_fine_in_multi_and_lua(self):
        """Without BLOCK there is nothing to deny."""
        self.seed("k", [100])
        pipe = self.client.pipeline(transaction=True)
        pipe.execute_command("TS.READ", "k", "-")
        assert pipe.execute() == [[[100, b"10"]]]

        script = "return redis.call('TS.READ', KEYS[1], '-')"
        assert self.client.eval(script, 1, "k") == [[100, b"10"]]


class TestTsReadClientLifecycle(TsReadTestBase):
    """Blocked clients must always be released, and never leak."""

    def test_client_unblock_delivers_the_current_snapshot(self):
        """Probe #25: CLIENT UNBLOCK TIMEOUT runs the timeout callback."""
        self.seed("k", [100])
        reader = self.reader("TS.READ", "k", "-", "BLOCK", 0, 5)
        try:
            client_id = reader.client_id()
            assert client_id is not None
            assert self.client.execute_command("CLIENT", "UNBLOCK", client_id, "TIMEOUT") == 1
            assert reader.result() == [[100, b"10"]]
            wait_for_equal(self.blocked_clients, 0, timeout=REPLY_TIMEOUT)
        finally:
            reader.close(self.client)

    def test_client_unblock_error_reports_an_error(self):
        self.seed("k", [100])
        reader = self.reader("TS.READ", "k", "-", "BLOCK", 0, 5)
        try:
            client_id = reader.client_id()
            assert self.client.execute_command("CLIENT", "UNBLOCK", client_id, "ERROR") == 1
            with pytest.raises(ResponseError, match="UNBLOCKED"):
                reader.result()
            wait_for_equal(self.blocked_clients, 0, timeout=REPLY_TIMEOUT)
        finally:
            reader.close(self.client)

    def test_disconnect_while_blocked_leaves_nothing_behind(self):
        """The private state is reclaimed on disconnect, not only on reply or timeout."""
        self.client.execute_command("TS.CREATE", "k")
        reader = self.reader("TS.READ", "k", "-", "BLOCK", 0, 99)
        try:
            reader.client.connection_pool.disconnect()
            wait_for_equal(self.blocked_clients, 0, timeout=REPLY_TIMEOUT)
            # The server is healthy and still serving the key afterwards.
            assert self.client.execute_command("PING")
            self.client.execute_command("TS.ADD", "k", 100, 1.0)
            assert self.client.execute_command("TS.READ", "k", "-") == [[100, b"1"]]
        finally:
            reader.close(self.client)

    def test_many_readers_all_resolve(self):
        """A signal that wakes several clients must serve every one of them."""
        self.client.execute_command("TS.CREATE", "k")
        readers = [
            self.reader("TS.READ", "k", "-", "BLOCK", LONG_BLOCK_MS, 1) for _ in range(8)
        ]
        try:
            wait_for_equal(self.blocked_clients, 8, timeout=REPLY_TIMEOUT)
            self.client.execute_command("TS.ADD", "k", 100, 1.0)
            for reader in readers:
                assert reader.result() == [[100, b"1"]]
            wait_for_equal(self.blocked_clients, 0, timeout=REPLY_TIMEOUT)
        finally:
            for reader in readers:
                reader.close(self.client)


class TestTsReadAcl(TsReadTestBase):
    """Command-category and key-pattern enforcement."""

    def create_user(self, username, rules):
        self.client.execute_command(
            "ACL", "SETUSER", username, "ON", ">pw", *rules
        )
        client = self.server.get_new_client()
        client.execute_command("AUTH", username, "pw")
        return client

    def test_requires_the_read_category(self):
        self.seed("k", [100])
        denied = self.create_user("no_read", ["+@all", "-@read", "~*"])
        try:
            with pytest.raises(ResponseError, match="no permissions"):
                denied.execute_command("TS.READ", "k", "-")
        finally:
            denied.close()

    def test_is_reachable_through_the_timeseries_category(self):
        self.seed("k", [100])
        allowed = self.create_user("ts_reader", ["+@read", "+@timeseries", "~*"])
        try:
            assert allowed.execute_command("TS.READ", "k", "-") == [[100, b"10"]]
        finally:
            allowed.close()

    def test_key_patterns_are_enforced(self):
        self.seed("allowed:k", [100])
        self.seed("denied:k", [100])
        user = self.create_user(
            "patterned", ["+@read", "+@timeseries", "~allowed:*"]
        )
        try:
            assert user.execute_command("TS.READ", "allowed:k", "-") == [[100, b"10"]]
            with pytest.raises(ResponseError, match="key|permission|NOPERM"):
                user.execute_command("TS.READ", "denied:k", "-")
        finally:
            user.close()

    def test_key_patterns_are_enforced_on_the_blocking_form(self):
        """The permission check happens before the client is allowed to wait."""
        self.seed("denied:k", [100])
        user = self.create_user(
            "patterned_block", ["+@read", "+@timeseries", "~allowed:*"]
        )
        try:
            with pytest.raises(ResponseError, match="key|permission|NOPERM"):
                user.execute_command(
                    "TS.READ", "denied:k", "-", "BLOCK", SHORT_BLOCK_MS, 5
                )
            assert self.blocked_clients() == 0
        finally:
            user.close()

    def test_revoking_access_while_blocked_terminates_the_reader(self):
        """Permissions are re-checked on wakeup, inside the blocked-client callback.

        The reference was not probed for this case, so the terminal reply is this module's
        defined behavior rather than an observed parity point. What matters either way: the
        re-check runs against the *real* blocked client, so it neither hangs the reader nor
        faults on a missing user (see plan §7).
        """
        self.seed("k", [100])
        self.create_user("revocable", ["+@read", "+@timeseries", "~k"]).close()

        blocked = _BlockedReader(
            self.server, ("TS.READ", "k", "-", "BLOCK", LONG_BLOCK_MS, 2)
        )
        blocked.client.execute_command("AUTH", "revocable", "pw")
        blocked.start()
        try:
            wait_for_equal(self.blocked_clients, 1, timeout=REPLY_TIMEOUT)

            # Drop the key pattern out from under the waiting client, then wake it.
            self.client.execute_command(
                "ACL", "SETUSER", "revocable", "ON", ">pw", "+@read", "+@timeseries",
                "resetkeys", "~other:*",
            )
            self.client.execute_command("TS.ADD", "k", 200, 20)

            with pytest.raises(ResponseError):
                blocked.result()
            wait_for_equal(self.blocked_clients, 0, timeout=REPLY_TIMEOUT)
            assert self.client.execute_command("PING")
        finally:
            blocked.close(self.client)


class TestTsReadOnReplica(ReplicationTestCase):
    """A reader blocked on a replica, woken by a write arriving over the replication link.

    This is the one place the "signal on committed mutation" rule runs on the apply path, where
    the command context carries the REPLICATED flag. It is a distinct risk from the standalone
    wakeup tests: this module gates several behaviors on `is_real_user_client` /
    `is_acl_enforced` precisely because a replicated command has no real user, and a readiness
    signal placed behind such a gate would leave every replica reader hanging until its timeout
    while working perfectly on the primary.

    TS.READ is `readonly`, so serving it from a replica is the expected deployment shape for
    fanning reads out — which makes this path load-bearing rather than incidental.
    """

    REPLICAS_COUNT = 1

    @pytest.fixture(autouse=True)
    def setup_test(self, setup):
        self.args = {"enable-debug-command": "yes", "loadmodule": get_module_path()}
        self.server, self.client = self.create_server(
            testdir=self.testdir, server_path=SERVER_PATH, args=self.args
        )
        self.setup_replication(num_replicas=1)
        self.replica = self.replicas[0]
        self.replica_client = self.replica.get_new_client()

    def replica_blocked_clients(self):
        return self.replica_client.info("clients")["blocked_clients"]

    def test_replicated_write_wakes_a_reader_blocked_on_the_replica(self):
        self.client.execute_command("TS.CREATE", "k")
        self.client.execute_command("TS.ADD", "k", 100, 10)
        self.waitForReplicaToSyncUp(self.replica)

        reader = _BlockedReader(
            self.replica, ("TS.READ", "k", "-", "BLOCK", LONG_BLOCK_MS, 2)
        ).start()
        try:
            wait_for_equal(self.replica_blocked_clients, 1, timeout=REPLY_TIMEOUT)

            # Written on the primary; the replica only ever sees it as a replicated TS.ADD.
            self.client.execute_command("TS.ADD", "k", 200, 20)

            assert reader.result() == [[100, b"10"], [200, b"20"]]
            wait_for_equal(self.replica_blocked_clients, 0, timeout=REPLY_TIMEOUT)
        finally:
            reader.close(self.replica_client)

    def test_replicated_deletion_releases_a_reader_blocked_on_the_replica(self):
        """The deletion path must also survive the replication link."""
        self.client.execute_command("TS.CREATE", "k")
        self.client.execute_command("TS.ADD", "k", 100, 10)
        self.waitForReplicaToSyncUp(self.replica)

        reader = _BlockedReader(
            self.replica, ("TS.READ", "k", "-", "BLOCK", LONG_BLOCK_MS, 9)
        ).start()
        try:
            wait_for_equal(self.replica_blocked_clients, 1, timeout=REPLY_TIMEOUT)
            self.client.execute_command("DEL", "k")
            assert reader.result() == []
        finally:
            reader.close(self.replica_client)
