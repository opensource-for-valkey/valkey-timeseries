"""Differential compatibility tests for TS.READ (plan §6).

Authored from the public command documentation and black-box observation of the pinned
reference only; no RedisTimeSeries source or test code was consulted. The observations these
scenarios were derived from are recorded in docs/plans/ts-read-implementation-plan.md §6.

TS.READ is the first *blocking* command in the compared surface, which the harness was not
built for: `DiffClient.execute_command` runs the reference and then the subject sequentially,
so a blocking read would leave the first engine waiting forever. Blocking scenarios therefore
drive both engines concurrently on dedicated connections and hand the outcomes back through
`DiffClient.compare_outcomes`, which applies the same comparison policy and registry handling.

Timing is compared loosely on purpose: both engines must resolve within a generous bound, and
a wakeup must be visibly faster than the timeout it beat. Exact elapsed-time equality is not a
compatibility property.
"""

from __future__ import annotations

import threading
import time

import pytest
import valkey
from valkey.exceptions import ResponseError

pytestmark = pytest.mark.rts_compat

# Upper bound on how long either engine may take to resolve a blocking read.
REPLY_TIMEOUT = 20

# Long enough that a wakeup always beats it.
LONG_BLOCK_MS = 10_000

# Used where the timeout is the behavior under test.
SHORT_BLOCK_MS = 600


class _Reader:
    """One engine's blocking TS.READ, on its own connection, in its own thread."""

    def __init__(self, url, protocol, args):
        self.url = url
        self.args = args
        self.client = valkey.Valkey.from_url(url, protocol=protocol)
        self.reply = None
        self.error = None
        self.elapsed = None
        self._thread = threading.Thread(target=self._run, daemon=True)

    def _run(self):
        started = time.monotonic()
        try:
            self.reply = self.client.execute_command(*self.args)
        except ResponseError as exc:
            self.error = exc
        except Exception as exc:  # noqa: BLE001 - surfaced by outcome()
            self.error = exc
        finally:
            self.elapsed = time.monotonic() - started

    def start(self):
        self._thread.start()
        return self

    def blocked_clients(self):
        """Read this engine's blocked-client count over a throwaway connection."""
        probe = valkey.Valkey.from_url(self.url)
        try:
            return probe.info("clients")["blocked_clients"]
        finally:
            probe.close()

    def join(self, timeout=REPLY_TIMEOUT):
        self._thread.join(timeout)
        return not self._thread.is_alive()

    def outcome(self):
        """The `(reply, error)` pair DiffClient.compare_outcomes expects."""
        return (self.reply, self.error)

    def release_and_close(self):
        """Unblock the reader if it is still waiting, then drop its connection."""
        if self._thread.is_alive():
            try:
                admin = valkey.Valkey.from_url(self.url)
                for entry in admin.client_list():
                    if entry.get("cmd", "").lower() == "ts.read":
                        admin.execute_command("CLIENT", "UNBLOCK", entry["id"], "TIMEOUT")
                admin.close()
            except Exception:  # noqa: BLE001 - best-effort cleanup
                pass
        self._thread.join(REPLY_TIMEOUT)
        try:
            self.client.close()
        except Exception:  # noqa: BLE001 - best-effort cleanup
            pass


class _BlockingPair:
    """The same blocking TS.READ running against both engines at once."""

    def __init__(self, diff, subject_url, reference_url, protocol, args):
        self.diff = diff
        self.args = args
        self.reference = _Reader(reference_url, protocol, args)
        self.subject = _Reader(subject_url, protocol, args)

    def start(self, wait_until_blocked=True):
        self.reference.start()
        self.subject.start()
        if wait_until_blocked:
            self._await_blocked()
        return self

    def _await_blocked(self):
        """Block until both engines report a waiting client.

        This is the synchronization point that makes the scenarios deterministic: the write
        that is meant to wake the readers is only issued once both engines are known to be
        waiting for it.
        """
        deadline = time.monotonic() + REPLY_TIMEOUT
        while time.monotonic() < deadline:
            if (
                self.reference.blocked_clients() >= 1
                and self.subject.blocked_clients() >= 1
            ):
                return
            time.sleep(0.02)
        pytest.fail(
            f"{self.args}: engines did not both report a blocked client within "
            f"{REPLY_TIMEOUT}s (reference={self.reference.blocked_clients()}, "
            f"subject={self.subject.blocked_clients()})"
        )

    def compare(self):
        """Join both readers and diff their replies through the harness.

        Returns the subject's reply, or raises as `execute_command` would.
        """
        assert self.reference.join(), f"reference never returned from {self.args}"
        assert self.subject.join(), f"subject never returned from {self.args}"
        return self.diff.compare_outcomes(
            self.args, self.reference.outcome(), self.subject.outcome()
        )

    def close(self):
        self.reference.release_and_close()
        self.subject.release_and_close()

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()
        return False


@pytest.fixture
def blocking(diff, subject_url, reference_url, protocol):
    """Factory for concurrent blocking reads, with guaranteed cleanup."""
    created = []

    def make(*args, wait_until_blocked=True):
        pair = _BlockingPair(diff, subject_url, reference_url, protocol, args)
        created.append(pair)
        return pair.start(wait_until_blocked=wait_until_blocked)

    yield make

    for pair in created:
        pair.close()


def seed(diff, key, timestamps):
    """Create `key` on both engines with one sample per timestamp."""
    diff.execute_command("TS.CREATE", key)
    for ts in timestamps:
        diff.execute_command("TS.ADD", key, ts, ts / 10)


class TestReadImmediate:
    """The non-blocking form: every reply is diffed by the harness itself."""

    def test_cursor_forms(self, diff):
        seed(diff, "k", [100, 200, 300])
        diff.execute_command("TS.READ", "k", 0)
        diff.execute_command("TS.READ", "k", 200)
        diff.execute_command("TS.READ", "k", 301)
        diff.execute_command("TS.READ", "k", "-")
        diff.execute_command("TS.READ", "k", "+")
        diff.execute_command("TS.READ", "k", "$")

    def test_key_states(self, diff):
        diff.execute_command("TS.CREATE", "empty")
        diff.execute_command("SET", "str", "v")
        for cursor in (0, "-", "+", "$"):
            diff.execute_command("TS.READ", "missing", cursor)
            diff.execute_command("TS.READ", "empty", cursor)
        with pytest.raises(ResponseError):
            diff.execute_command("TS.READ", "str", 0)

    def test_max_count_and_paging(self, diff):
        seed(diff, "k", [100, 200, 300, 400])
        diff.execute_command("TS.READ", "k", "-", "MAX_COUNT", 2)
        diff.execute_command("TS.READ", "k", 301, "MAX_COUNT", 2)
        diff.execute_command("TS.READ", "k", "-", "MAX_COUNT", 99)

    def test_out_of_order_storage(self, diff):
        diff.execute_command("TS.CREATE", "k")
        for ts in (300, 100, 400, 200):
            diff.execute_command("TS.ADD", "k", ts, ts / 10)
        diff.execute_command("TS.READ", "k", "-")
        diff.execute_command("TS.READ", "k", 150)

    def test_dollar_at_max_timestamp(self, diff):
        """`$` past the end of the timestamp domain must not overflow on either engine."""
        max_ts = 9223372036854775807
        diff.execute_command("TS.ADD", "k", max_ts, 1.0)
        diff.execute_command("TS.READ", "k", "$")
        diff.execute_command("TS.READ", "k", "+")

    def test_retention_hides_trimmed_samples(self, diff):
        """A cursor below the retention floor sees the same window on both engines."""
        diff.execute_command("TS.CREATE", "k", "RETENTION", 200)
        for ts in (100, 200, 300, 400, 500):
            diff.execute_command("TS.ADD", "k", ts, ts / 10)
        diff.execute_command("TS.READ", "k", "-")
        diff.execute_command("TS.READ", "k", 0)

    def test_compaction_destination_is_readable(self, diff):
        diff.execute_command("TS.CREATE", "src")
        diff.execute_command("TS.CREATE", "dest")
        diff.execute_command("TS.CREATERULE", "src", "dest", "AGGREGATION", "sum", 100)
        for ts in (100, 150, 250, 350):
            diff.execute_command("TS.ADD", "src", ts, 1.0)
        diff.execute_command("TS.READ", "dest", "-")


class TestReadValidation:
    """Argument validation. Both engines must accept and reject the same inputs."""

    @pytest.mark.parametrize(
        "extra",
        [
            ["BLOCK", "10", "1", "BLOCK", "10", "1"],
            ["MAX_COUNT", "1", "MAX_COUNT", "2"],
            ["BLOCK", "50"],
            ["MAX_COUNT"],
            ["BOGUS"],
            ["BLOCK", "50", "1", "EXTRA"],
            ["MAX_COUNT", "0"],
            ["MAX_COUNT", "-1"],
            ["MAX_COUNT", "abc"],
            ["BLOCK", "50", "0"],
            ["BLOCK", "50", "-1"],
            ["BLOCK", "-1", "1"],
            ["BLOCK", "abc", "1"],
            ["BLOCK", "500", "5", "MAX_COUNT", "1"],
            ["MAX_COUNT", "1", "BLOCK", "500", "5"],
        ],
        ids=[
            "dup-block", "dup-max-count", "block-no-min", "max-count-no-value",
            "stray-token", "trailing-token", "max-0", "max-neg", "max-nan",
            "min-0", "min-neg", "ms-neg", "ms-nan", "min-gt-max", "min-gt-max-reordered",
        ],
    )
    def test_rejected_inputs(self, diff, extra):
        seed(diff, "k", [100])
        with pytest.raises(ResponseError):
            diff.execute_command("TS.READ", "k", "-", *extra)

    def test_rejected_before_key_access(self, diff):
        """min_count > max_count fails even for a key that does not exist."""
        with pytest.raises(ResponseError):
            diff.execute_command(
                "TS.READ", "missing", "-", "BLOCK", "500", "5", "MAX_COUNT", "1"
            )

    def test_negative_timestamp(self, diff):
        seed(diff, "k", [100])
        with pytest.raises(ResponseError):
            diff.execute_command("TS.READ", "k", -5)

    @pytest.mark.parametrize(
        "extra",
        [
            ["BLOCK", "50", "1", "MAX_COUNT", "2"],
            ["MAX_COUNT", "2", "BLOCK", "50", "1"],
            ["block", "50", "1", "max_count", "2"],
            ["BlOcK", "50", "1", "Max_Count", "2"],
            ["BLOCK", "500", "2", "MAX_COUNT", "2"],
        ],
        ids=["block-first", "max-first", "lowercase", "mixed-case", "equal-counts"],
    )
    def test_accepted_option_forms(self, diff, extra):
        """Satisfiable, so these return immediately and are diffed like any other reply."""
        seed(diff, "k", [100, 200, 300])
        diff.execute_command("TS.READ", "k", "-", *extra)


class TestReadBlockingSatisfiedImmediately:
    """BLOCK forms that never actually block: still ordinary sequential commands."""

    def test_threshold_already_met(self, diff):
        seed(diff, "k", [100, 200, 300])
        diff.execute_command("TS.READ", "k", "-", "BLOCK", LONG_BLOCK_MS, 3)
        diff.execute_command("TS.READ", "k", "-", "BLOCK", 0, 1)
        diff.execute_command("TS.READ", "k", "+", "BLOCK", LONG_BLOCK_MS, 1)

    def test_multi_returns_data_when_satisfiable(self, diff):
        """Both engines answer a satisfiable blocking read inside MULTI rather than erroring."""
        seed(diff, "k", [100, 200])
        for client in (diff.reference, diff.subject):
            pipe = client.pipeline(transaction=True)
            pipe.execute_command("TS.READ", "k", "-", "BLOCK", LONG_BLOCK_MS, 2)
            assert len(pipe.execute()[0]) == 2

    def test_multi_errors_when_it_would_block(self, diff):
        seed(diff, "k", [100])
        for client in (diff.reference, diff.subject):
            pipe = client.pipeline(transaction=True)
            pipe.execute_command("TS.READ", "k", "-", "BLOCK", LONG_BLOCK_MS, 5)
            with pytest.raises(ResponseError):
                pipe.execute()

    def test_lua_matches_on_both_engines(self, diff):
        seed(diff, "k", [100])
        satisfiable = "return redis.call('TS.READ', KEYS[1], '-', 'BLOCK', '1000', '1')"
        unsatisfiable = "return redis.call('TS.READ', KEYS[1], '$', 'BLOCK', '1000', '1')"
        for client in (diff.reference, diff.subject):
            assert len(client.eval(satisfiable, 1, "k")) == 1
            with pytest.raises(ResponseError):
                client.eval(unsatisfiable, 1, "k")


class TestReadBlockingConcurrent:
    """Scenarios where both engines genuinely wait."""

    def test_wakes_on_threshold(self, diff, blocking):
        seed(diff, "k", [100])
        with blocking("TS.READ", "k", "-", "BLOCK", LONG_BLOCK_MS, 2) as pair:
            diff.execute_command("TS.ADD", "k", 200, 20)
            reply = pair.compare()
            assert len(reply) == 2
            # A wakeup, not a timeout.
            for reader in (pair.reference, pair.subject):
                assert reader.elapsed < LONG_BLOCK_MS / 1000.0

    def test_timeout_returns_partial_result(self, diff, blocking):
        seed(diff, "k", [100])
        pair = blocking(
            "TS.READ", "k", "-", "BLOCK", SHORT_BLOCK_MS, 3, wait_until_blocked=False
        )
        with pair:
            reply = pair.compare()
            assert len(reply) == 1
            for reader in (pair.reference, pair.subject):
                assert reader.elapsed >= SHORT_BLOCK_MS / 1000.0 * 0.5

    def test_timeout_on_empty_series(self, diff, blocking):
        diff.execute_command("TS.CREATE", "k")
        pair = blocking(
            "TS.READ", "k", "-", "BLOCK", SHORT_BLOCK_MS, 1, wait_until_blocked=False
        )
        with pair:
            assert pair.compare() == []

    def test_blocks_on_missing_key_then_wakes(self, diff, blocking):
        with blocking("TS.READ", "later", "-", "BLOCK", LONG_BLOCK_MS, 1) as pair:
            diff.execute_command("TS.ADD", "later", 50, 5.0)
            assert len(pair.compare()) == 1

    def test_deletion_releases_with_empty_array(self, diff, blocking):
        seed(diff, "k", [100])
        with blocking("TS.READ", "k", "-", "BLOCK", LONG_BLOCK_MS, 9) as pair:
            diff.execute_command("DEL", "k")
            assert pair.compare() == []

    def test_flush_releases_with_empty_array(self, diff, blocking):
        seed(diff, "k", [100])
        with blocking("TS.READ", "k", "-", "BLOCK", LONG_BLOCK_MS, 9) as pair:
            diff.execute_command("FLUSHALL")
            assert pair.compare() == []

    def test_dollar_excludes_pre_existing_data(self, diff, blocking):
        """`$` resolves once and does not drift as data arrives."""
        seed(diff, "k", [100, 200])
        with blocking("TS.READ", "k", "$", "BLOCK", LONG_BLOCK_MS, 1) as pair:
            diff.execute_command("TS.ADD", "k", 300, 30)
            reply = pair.compare()
            assert len(reply) == 1

    def test_max_count_caps_a_wakeup(self, diff, blocking):
        seed(diff, "k", [100])
        with blocking(
            "TS.READ", "k", "-", "BLOCK", LONG_BLOCK_MS, 2, "MAX_COUNT", 2
        ) as pair:
            diff.execute_command("TS.MADD", "k", 200, 20, "k", 300, 30)
            assert len(pair.compare()) == 2

    def test_wakes_on_madd(self, diff, blocking):
        diff.execute_command("TS.CREATE", "k")
        with blocking("TS.READ", "k", "-", "BLOCK", LONG_BLOCK_MS, 1) as pair:
            diff.execute_command("TS.MADD", "k", 100, 1.0)
            assert len(pair.compare()) == 1

    @pytest.mark.parametrize("command", ["TS.INCRBY", "TS.DECRBY"])
    def test_wakes_on_incr_decr(self, diff, blocking, command):
        diff.execute_command("TS.CREATE", "k")
        with blocking("TS.READ", "k", "-", "BLOCK", LONG_BLOCK_MS, 1) as pair:
            # Explicit TIMESTAMP so the two engines' clocks cannot diverge the reply.
            diff.execute_command(command, "k", 5, "TIMESTAMP", 1000)
            assert len(pair.compare()) == 1

    def test_wakes_on_compaction_destination(self, diff, blocking):
        diff.execute_command("TS.CREATE", "src")
        diff.execute_command("TS.CREATE", "dest")
        diff.execute_command("TS.CREATERULE", "src", "dest", "AGGREGATION", "sum", 100)
        diff.execute_command("TS.ADD", "src", 100, 1.0)
        with blocking("TS.READ", "dest", "-", "BLOCK", LONG_BLOCK_MS, 1) as pair:
            diff.execute_command("TS.ADD", "src", 150, 2.0)
            diff.execute_command("TS.ADD", "src", 250, 3.0)
            assert len(pair.compare()) >= 1

    def test_wakes_on_restore_into_the_key(self, diff, blocking):
        """Key creation through a core command wakes readers on both engines."""
        seed(diff, "src", [1])
        payload_script = (
            "local d = redis.call('DUMP', KEYS[1]); "
            "return redis.call('RESTORE', KEYS[2], 0, d)"
        )
        with blocking("TS.READ", "dst", "-", "BLOCK", LONG_BLOCK_MS, 1) as pair:
            for client in (diff.reference, diff.subject):
                client.eval(payload_script, 2, "src", "dst")
            assert len(pair.compare()) == 1

    def test_wakes_on_rename_onto_the_key(self, diff, blocking):
        seed(diff, "src", [1])
        with blocking("TS.READ", "dst", "-", "BLOCK", LONG_BLOCK_MS, 1) as pair:
            diff.execute_command("RENAME", "src", "dst")
            assert len(pair.compare()) == 1

    def test_readers_are_independent(self, diff, subject_url, reference_url, protocol):
        """Two readers on one key both receive the sample that woke them."""
        diff.execute_command("TS.CREATE", "k")
        first = _BlockingPair(
            diff, subject_url, reference_url, protocol,
            ("TS.READ", "k", "-", "BLOCK", LONG_BLOCK_MS, 1),
        )
        second = _BlockingPair(
            diff, subject_url, reference_url, protocol,
            ("TS.READ", "k", "-", "BLOCK", LONG_BLOCK_MS, 1),
        )
        try:
            first.start()
            second.start()
            diff.execute_command("TS.ADD", "k", 10, 1.0)
            assert len(first.compare()) == 1
            assert len(second.compare()) == 1
            # And the data survives both reads.
            diff.execute_command("TS.READ", "k", "-")
        finally:
            first.close()
            second.close()

    def test_backfill_below_the_cursor_does_not_wake(self, diff, blocking):
        """A sample written below a resolved `$` cursor must not satisfy either engine."""
        seed(diff, "k", [100, 200])
        with blocking("TS.READ", "k", "$", "BLOCK", LONG_BLOCK_MS, 1) as pair:
            diff.execute_command("TS.ADD", "k", 150, 15)
            # Both engines must still be waiting.
            assert pair.reference.blocked_clients() >= 1
            assert pair.subject.blocked_clients() >= 1

            diff.execute_command("TS.ADD", "k", 300, 30)
            assert len(pair.compare()) == 1
