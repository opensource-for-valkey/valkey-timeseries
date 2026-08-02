"""DiffClient: sends every command to both engines and asserts reply parity.

Test plan §4.1. Scenario authors just write command sequences; comparison happens
automatically on every call. On mismatch the raised `CompatMismatch` carries the
full command history (a reproducer), both raw replies, and the normalized deltas.

Mismatches fully covered by the known-divergence registry (§5.3) are recorded as
XFAIL-DIVERGENT events in the conformance report instead of failing.

Error policy (§5.2):
  - error condition must match on both sides;
  - "reference errors, subject succeeds" is ALWAYS a failure (accepted-input
    superset) and can not be registered away;
  - matching errors are compared by client exception class and full text; a text
    delta with the same class/prefix is an 'error-text' delta (registrable).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, List, Optional

from valkey.exceptions import ResponseError

from compat_normalize import Delta, diff_replies, normalize_reply
from compat_registry import Registry


class CompatMismatch(AssertionError):
    pass


@dataclass
class DivergenceEvent:
    """One registered divergence observed during a test run."""

    entry_id: str
    command: str
    delta_kind: str
    detail: str
    test_id: str
    protocol: int


@dataclass
class CommandRecord:
    args: tuple
    reference_reply: Any = None
    subject_reply: Any = None
    reference_error: Optional[str] = None
    subject_error: Optional[str] = None

    def render(self) -> str:
        return " ".join(_printable(a) for a in self.args)


def _printable(arg) -> str:
    if isinstance(arg, bytes):
        arg = arg.decode("utf-8", errors="backslashreplace")
    text = str(arg)
    if text == "" or any(c.isspace() for c in text):
        return f'"{text}"'
    return text


class DiffClient:
    """Wraps a subject and a reference client; diffs every reply."""

    def __init__(
        self,
        subject,
        reference,
        protocol: int,
        registry: Registry,
        divergence_log: List[DivergenceEvent],
        test_id: str = "",
    ):
        self.subject = subject
        self.reference = reference
        self.protocol = protocol
        self.registry = registry
        self.divergence_log = divergence_log
        self.test_id = test_id
        self.history: List[CommandRecord] = []

    # -- public API ---------------------------------------------------------

    def execute_command(self, *args):
        """Send to both engines, compare, return the subject's reply.

        If both engines error consistently, the subject's ResponseError is
        re-raised so scenarios can use pytest.raises() naturally.
        """
        record = CommandRecord(args=args)
        self.history.append(record)

        ref_reply, ref_err = self._run(self.reference, args)
        sub_reply, sub_err = self._run(self.subject, args)
        return self._settle_outcomes(record, args, (ref_reply, ref_err), (sub_reply, sub_err))

    def compare_outcomes(self, args, reference, subject):
        """Diff one command pair that the caller executed itself.

        `execute_command` runs the reference and then the subject sequentially, so a *blocking*
        command would deadlock there — the first engine would still be waiting when the second
        is asked to run. Callers that must drive both engines concurrently (see
        test_compat_read.py) collect the outcomes on their own connections and hand them back
        here, so the comparison policy, known-divergence registry, divergence log, and
        reproducer history all behave exactly as they do for an ordinary command.

        `reference` and `subject` are each a `(reply, error)` pair, where exactly one element
        is None.
        """
        record = CommandRecord(args=tuple(args))
        self.history.append(record)
        return self._settle_outcomes(record, args, reference, subject)

    def _settle_outcomes(self, record, args, reference, subject):
        """Apply the §5.2 error policy and reply diff to one already-executed command pair."""
        ref_reply, ref_err = reference
        sub_reply, sub_err = subject
        record.reference_reply, record.subject_reply = ref_reply, sub_reply
        record.reference_error = str(ref_err) if ref_err is not None else None
        record.subject_error = str(sub_err) if sub_err is not None else None

        command_name = _command_name(args)

        if ref_err is not None and sub_err is None:
            # Never registrable: accepted-input superset (plan §5.2).
            raise self._mismatch(
                command_name,
                [
                    Delta(
                        "error-condition",
                        "$",
                        f"<error: {ref_err}>",
                        sub_reply,
                        "reference errors, subject succeeds — accepted-input superset",
                    )
                ],
                registrable=False,
            )

        if ref_err is None and sub_err is not None:
            delta = Delta(
                "error-condition",
                "$",
                ref_reply,
                f"<error: {sub_err}>",
                "subject errors, reference succeeds",
            )
            self._settle(command_name, [delta])
            raise sub_err

        if ref_err is not None and sub_err is not None:
            deltas = _compare_errors(ref_err, sub_err)
            if deltas:
                self._settle(command_name, deltas)
            raise sub_err

        ref_norm = normalize_reply(command_name, ref_reply)
        sub_norm = normalize_reply(command_name, sub_reply)
        deltas = diff_replies(ref_norm, sub_norm)
        if deltas:
            self._settle(command_name, deltas)
        return sub_reply

    # Allow dc("TS.ADD", key, ts, val) as shorthand.
    __call__ = execute_command

    def both(self, *args):
        """Run a command on both engines WITHOUT diffing the replies.

        For setup steps whose replies are inherently divergent (e.g. `TS.ADD k *`,
        server clocks differ) but whose downstream effects should still be diffed.
        Returns (reference_reply, subject_reply); errors propagate from either side.
        """
        record = CommandRecord(args=args)
        self.history.append(record)
        record.reference_reply = self.reference.execute_command(*args)
        record.subject_reply = self.subject.execute_command(*args)
        return record.reference_reply, record.subject_reply

    # -- internals ----------------------------------------------------------

    @staticmethod
    def _run(client, args):
        try:
            return client.execute_command(*args), None
        except ResponseError as exc:
            return None, exc

    def _settle(self, command_name: str, deltas: List[Delta]):
        """Fail on unregistered deltas; log registered ones as divergences."""
        unregistered = []
        events = []
        for delta in deltas:
            entry = self.registry.find(command_name, delta.kind, delta.detail())
            if entry is None:
                unregistered.append(delta)
            else:
                events.append(
                    DivergenceEvent(
                        entry_id=entry.id,
                        command=command_name,
                        delta_kind=delta.kind,
                        detail=delta.detail(),
                        test_id=self.test_id,
                        protocol=self.protocol,
                    )
                )
        if unregistered:
            raise self._mismatch(command_name, unregistered, registrable=True)
        self.divergence_log.extend(events)

    def _mismatch(self, command_name: str, deltas: List[Delta], registrable: bool):
        last = self.history[-1]
        lines = [
            f"RTS compatibility mismatch: {command_name} (RESP{self.protocol})",
            "",
            "Deltas:",
        ]
        for delta in deltas:
            lines.append(f"  - [{delta.kind}] {delta.detail()}")
        if not registrable:
            lines.append("  (this class of mismatch can not be registered as a divergence)")
        lines += [
            "",
            f"Raw reference reply: {last.reference_reply!r}"
            + (f" (error: {last.reference_error})" if last.reference_error else ""),
            f"Raw subject reply:   {last.subject_reply!r}"
            + (f" (error: {last.subject_error})" if last.subject_error else ""),
            "",
            "Command history (reproducer):",
        ]
        for i, record in enumerate(self.history, 1):
            lines.append(f"  {i}. {record.render()}")
        return CompatMismatch("\n".join(lines))


def _command_name(args) -> str:
    name = args[0]
    if isinstance(name, bytes):
        name = name.decode("utf-8")
    return str(name).upper()


def _compare_errors(ref_err: ResponseError, sub_err: ResponseError) -> List[Delta]:
    """Compare two error replies (plan §5.2).

    Both sides are parsed by the same client library, so exception class + text
    is a faithful, symmetric proxy for the wire error. Class or prefix mismatch
    is an error-condition delta; same class/prefix with different wording is an
    error-text delta (registrable).
    """
    ref_cls, sub_cls = type(ref_err).__name__, type(sub_err).__name__
    ref_text, sub_text = str(ref_err), str(sub_err)
    if ref_cls != sub_cls:
        return [
            Delta(
                "error-condition",
                "$",
                f"{ref_cls}: {ref_text}",
                f"{sub_cls}: {sub_text}",
                "different error class",
            )
        ]
    if ref_text == sub_text:
        return []
    ref_prefix = ref_text.split(" ", 1)[0]
    sub_prefix = sub_text.split(" ", 1)[0]
    prefix_like = ref_prefix.endswith(":") or sub_prefix.endswith(":")
    if prefix_like and ref_prefix != sub_prefix:
        return [
            Delta(
                "error-condition",
                "$",
                ref_text,
                sub_text,
                "different error prefix",
            )
        ]
    return [Delta("error-text", "$", ref_text, sub_text)]
