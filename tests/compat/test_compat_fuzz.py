"""Tier C: property-based differential fuzzing (test plan §4.3).

Hypothesis generates valid-by-construction command sequences (see
fuzz_strategies.py) and replays each through a `DiffClient`, so every reply is
diffed against RedisTimeSeries 8.6. A generated program that produces any
unregistered divergence fails the example; Hypothesis then shrinks it to a
minimal reproducer, which a human promotes into the regression corpus
(tests/compat/corpus/, replayed by test_compat_corpus.py) as a golden Tier A
test.

This module is **opt-in**: it only runs when COMPAT_FUZZ=1, because it is a
time-budgeted nightly job rather than part of the PR gate (plan §4.3, §8). Knobs:

  COMPAT_FUZZ=1                  enable the fuzzer at all
  COMPAT_FUZZ_MAX_EXAMPLES=N     examples per protocol (default 150)
  COMPAT_FUZZ_DERANDOMIZE=1      fixed seed (reproducible; for debugging)

Writing new fuzz strategies is clean-room work: derive them from public
RedisTimeSeries documentation and black-box observation only (see
tests/compat/README.md). Do NOT consult RedisTimeSeries source or test code.
"""

from __future__ import annotations

import os

import pytest

hypothesis = pytest.importorskip("hypothesis")

from hypothesis import HealthCheck, given, settings  # noqa: E402
from valkey.exceptions import ResponseError  # noqa: E402

from compat_diff import DiffClient  # noqa: E402
from fuzz_strategies import programs  # noqa: E402

pytestmark = pytest.mark.skipif(
    os.environ.get("COMPAT_FUZZ", "").lower() not in ("1", "true", "yes"),
    reason="differential fuzzer is opt-in; set COMPAT_FUZZ=1 to run it",
)

_MAX_EXAMPLES = int(os.environ.get("COMPAT_FUZZ_MAX_EXAMPLES", "150"))
_DERANDOMIZE = os.environ.get("COMPAT_FUZZ_DERANDOMIZE", "").lower() in ("1", "true", "yes")


def replay(client: DiffClient, commands):
    """Replay a command sequence through the diff client.

    Matching errors on both engines are expected and swallowed (the DiffClient
    re-raises the subject's ResponseError once it has confirmed the reference
    errored too). A genuine divergence — including "reference errors, subject
    succeeds" and "subject errors, reference succeeds" — surfaces as a
    CompatMismatch (an AssertionError), which propagates so Hypothesis shrinks it.
    """
    for args in commands:
        try:
            client.execute_command(*args)
        except ResponseError:
            pass


@pytest.fixture
def fuzz_runner(subject_url, reference_url, protocol, registry, request):
    """A callable that replays one generated program against both engines.

    Reused across all Hypothesis examples in a test invocation (hence the
    suppressed function_scoped_fixture health check): a single pair of
    connections is kept open, and each call FLUSHALLs both engines so examples
    do not leak state into one another.
    """
    import valkey

    subject = valkey.Valkey.from_url(subject_url, protocol=protocol)
    reference = valkey.Valkey.from_url(reference_url, protocol=protocol)

    def run(commands):
        subject.flushall()
        reference.flushall()
        client = DiffClient(
            subject=subject,
            reference=reference,
            protocol=protocol,
            registry=registry,
            divergence_log=request.config._compat_divergences,
            test_id=request.node.nodeid,
        )
        replay(client, commands)

    yield run
    subject.close()
    reference.close()


@settings(
    max_examples=_MAX_EXAMPLES,
    deadline=None,  # each example issues many network round-trips; latency varies
    derandomize=_DERANDOMIZE,
    suppress_health_check=[HealthCheck.function_scoped_fixture],
)
@given(commands=programs())
def test_fuzz_read_path_parity(fuzz_runner, commands):
    fuzz_runner(commands)
