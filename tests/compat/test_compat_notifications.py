"""Keyspace-notification parity with RedisTimeSeries 8.8 (test plan §7.3).

Subscribes `__keyevent@0__:*` on both engines (both run with
`notify-keyspace-events KEA`, §3), executes a canonical mutation script, and
compares the (event, key) sequence per command. Covers the module events
(`ts.create`, `ts.add`, `ts.add:dest`, `ts.incrby`/`ts.decrby`, `ts.alter`,
`ts.createrule:src`/`:dest`, `ts.del`, `ts.deleterule:src`/`:dest`), the
generic `del`, and expiry.

Reference-observed subtleties this suite pins:
  - auto-creating writes (TS.ADD/TS.INCRBY on a missing key) emit only their own
    write event — no `ts.create`;
  - TS.MADD emits one `ts.add` per item — on the reference, per item *attempted*,
    which is DIV-0047 (see TestMaddFailureEvents);
  - a compaction bucket close emits `ts.add:dest` *before* the source's
    `ts.add`;
  - TS.DEL on a range covered by a rule emits only `ts.del` — the propagation
    into the destination emits no dest event.

Written clean-room from public RedisTimeSeries documentation and black-box
observation of the reference server. Do NOT consult RedisTimeSeries source or
test code (see tests/compat/README.md).
"""

import time

import pytest
import valkey

# Quiet-window drain: a step's events are considered complete after this much
# silence; the cap bounds a runaway stream.
QUIET_S = 0.25
MAX_WAIT_S = 3.0


class EventWatcher:
    def __init__(self, url):
        self.client = valkey.Valkey.from_url(url)
        self.client.flushall()
        self.pubsub = self.client.pubsub()
        self.pubsub.psubscribe("__keyevent@0__:*")
        time.sleep(0.2)
        self.drain(quiet_s=0.1)  # subscribe confirmations / flush residue

    def drain(self, quiet_s=QUIET_S, max_wait_s=MAX_WAIT_S):
        events = []
        deadline = time.monotonic() + max_wait_s
        last = time.monotonic()
        while time.monotonic() < deadline:
            msg = self.pubsub.get_message(timeout=0.05)
            if msg is None:
                if time.monotonic() - last >= quiet_s:
                    break
                continue
            if msg["type"] == "pmessage":
                event = msg["channel"].decode().split(":", 1)[1]
                events.append((event, msg["data"].decode()))
                last = time.monotonic()
        return events

    def run(self, *cmd):
        try:
            self.client.execute_command(*cmd)
        except valkey.exceptions.ResponseError:
            pass  # error parity is covered elsewhere; events are the subject here
        return self.drain()

    def close(self):
        self.pubsub.close()
        self.client.close()


@pytest.fixture
def watchers(subject_url, reference_url):
    subject = EventWatcher(subject_url)
    reference = EventWatcher(reference_url)
    yield subject, reference
    subject.close()
    reference.close()


# One canonical mutation pass over the shared write surface. Comments call
# out the steps that exist to pin a specific reference behavior.
CANONICAL_SCRIPT = [
    ("TS.CREATE", "src"),
    ("TS.CREATE", "dst"),
    ("TS.ADD", "src", 100, 1.0),
    ("TS.ADD", "auto", 100, 1.0),                       # auto-create: no ts.create
    ("TS.MADD", "src", 200, 2.0, "src", 300, 3.0),      # one ts.add per item
    # A TS.MADD whose items *fail* is deliberately not in this script: RTS emits
    # ts.add per item regardless of outcome, so the failing-item case is a
    # divergence (DIV-0047), pinned by TestMaddFailureEvents below.
    ("TS.INCRBY", "ctr", 5),                            # auto-create: no ts.create
    ("TS.DECRBY", "ctr", 2),
    ("TS.ALTER", "src", "RETENTION", 60000),
    ("TS.CREATERULE", "src", "dst", "AGGREGATION", "avg", 1000),
    ("TS.ADD", "src", 1100, 4.0),                       # lands in open bucket
    ("TS.ADD", "src", 2100, 5.0),                       # closes bucket: ts.add:dest then ts.add
    ("TS.ADD", "src", 1500, 4.5),                       # upsert into closed bucket
    ("TS.DEL", "src", 0, 100),                          # only ts.del (no dest event)
    ("TS.DELETERULE", "src", "dst"),
    ("DEL", "auto"),                                    # generic del
]


class TestNotificationParity:
    def test_canonical_script_event_sequences(self, watchers):
        subject, reference = watchers
        mismatches = []
        for cmd in CANONICAL_SCRIPT:
            ref_events = reference.run(*cmd)
            sub_events = subject.run(*cmd)
            if sub_events != ref_events:
                pretty = " ".join(str(a) for a in cmd)
                mismatches.append(
                    f"{pretty}\n    reference: {ref_events}\n    subject:   {sub_events}"
                )
        assert not mismatches, "event sequence mismatches:\n" + "\n".join(mismatches)

    def test_madd_events_agree_when_every_item_succeeds(self, watchers):
        """The success path is plain parity and must stay that way — DIV-0047 is
        confined to items that fail."""
        subject, reference = watchers
        for side in (reference, subject):
            side.run("TS.CREATE", "ok1")
            side.run("TS.CREATE", "ok2")
        assert reference.run("TS.MADD", "ok1", 100, 1.0, "ok2", 100, 2.0) == subject.run(
            "TS.MADD", "ok1", 100, 1.0, "ok2", 100, 2.0
        )

    def test_expired_event(self, watchers):
        subject, reference = watchers
        for name, side in (("reference", reference), ("subject", subject)):
            side.run("TS.ADD", "exp", 100, 1.0)
            side.client.execute_command("PEXPIRE", "exp", 50)

            # Everything from PEXPIRE onward is kept — the `expired` event
            # fires on the engine's own expiry cycle, so poll until it shows.
            deadline = time.monotonic() + 5.0
            seen = []
            while time.monotonic() < deadline:
                seen.extend(side.drain(quiet_s=0.1, max_wait_s=0.3))
                if ("expired", "exp") in seen:
                    break
            assert ("expired", "exp") in seen, f"{name}: no expired event; saw {seen}"


class TestMaddFailureEvents:
    """DIV-0047, pinned per-engine rather than diffed.

    RedisTimeSeries emits a `ts.add` keyspace event for every TS.MADD *item
    attempted*, whatever its outcome. We emit one for every item that got as far as
    a usable series. So the engines agree on stored samples and on items rejected
    by a write-time rule (a duplicate blocked by policy notifies on both), and
    differ on items that never reached a series at all — a missing key, a key
    holding another type, or an unparseable timestamp or value.

    The registry can not express this: the only entry that would match is broad
    enough to absorb a genuine "TS.MADD stopped notifying" regression, which
    test_madd_events_agree_when_every_item_succeeds guards.
    """

    @staticmethod
    def _events(side, setup, cmd):
        for step in setup:
            side.run(*step)
        return side.run(*cmd)

    @pytest.mark.parametrize(
        "name,setup,cmd",
        [
            ("missing key", [], ("TS.MADD", "f1", 100, 1.0)),
            ("wrongtype", [("SET", "f2", "x")], ("TS.MADD", "f2", 100, 1.0)),
            ("unparseable value", [("TS.CREATE", "f3")], ("TS.MADD", "f3", 100, "abc")),
        ],
    )
    def test_reference_notifies_for_rejected_items(self, watchers, name, setup, cmd):
        subject, reference = watchers
        key = cmd[1]
        assert self._events(reference, setup, cmd) == [("ts.add", key)], name
        assert self._events(subject, setup, cmd) == [], name

    def test_write_time_rejections_notify_on_both(self, watchers):
        """The agreeing half: an item the series itself rejected still notifies on
        both engines, so this divergence is about unreachable series only."""
        subject, reference = watchers
        setup = [("TS.CREATE", "d1"), ("TS.ADD", "d1", 100, 1.0)]
        cmd = ("TS.MADD", "d1", 100, 2.0)
        assert self._events(reference, setup, cmd) == [("ts.add", "d1")]
        assert self._events(subject, setup, cmd) == [("ts.add", "d1")]

    def test_only_the_written_key_is_announced(self, watchers):
        """A batch mixing a good and a bad item: both engines announce the key
        that was written; only the reference also announces the one that wasn't."""
        subject, reference = watchers
        setup = [("TS.CREATE", "g1")]
        cmd = ("TS.MADD", "g1", 100, 1.0, "gmiss", 100, 2.0)
        assert self._events(reference, setup, cmd) == [
            ("ts.add", "g1"),
            ("ts.add", "gmiss"),
        ]
        assert self._events(subject, setup, cmd) == [("ts.add", "g1")]
