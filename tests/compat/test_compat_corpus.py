"""Tier A regression corpus: replay checked-in reproducers (test plan §4.3).

Each `corpus/*.json` file is a minimal command sequence — either a fuzzer shrink
or a hand-written reproducer of a fixed bug — that must stay byte-identical to
RedisTimeSeries 8.10. Every file is replayed through the `diff` fixture under both
RESP versions, so a regression that reopens the bug fails here as a plain
mismatch. A file whose only remaining delta is an intentional divergence is
tolerated via the registry (its `expect_divergence` id documents which one).

Promoting a fuzzer find (plan §4.3): take the shrunk reproducer from a
COMPAT_FUZZ=1 failure, drop its command list into a new corpus/<name>.json with a
description, and confirm it passes here once the underlying divergence is fixed
or registered.

Corpus file schema:
  {
    "description": "one line: what this pins",
    "origin":      "manual reproducer" | "fuzzer shrink YYYY-MM-DD",
    "expect_divergence": "DIV-00xx" | null,   # optional; documents a registered delta
    "commands": [ ["TS.CREATE", "fz:k0", ...], ["TS.ADD", ...], ... ]
  }

Written clean-room from public RedisTimeSeries documentation and black-box
observation (see tests/compat/README.md).
"""

from __future__ import annotations

import glob
import json
import os

import pytest
from valkey.exceptions import ResponseError

_CORPUS_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "corpus")


def _load_cases():
    cases = []
    for path in sorted(glob.glob(os.path.join(_CORPUS_DIR, "*.json"))):
        with open(path) as f:
            spec = json.load(f)
        cases.append(pytest.param(spec, id=os.path.splitext(os.path.basename(path))[0]))
    return cases


_CASES = _load_cases()


@pytest.mark.skipif(not _CASES, reason="no corpus files present")
@pytest.mark.parametrize("spec", _CASES)
def test_corpus_reproducer(diff, spec):
    commands = spec["commands"]
    assert commands, f"empty corpus case: {spec.get('description')!r}"
    for args in commands:
        # A command that errors identically on both engines is a matching error,
        # not a regression; the DiffClient re-raises the subject's ResponseError
        # only after confirming the reference errored too. A genuine divergence
        # raises CompatMismatch (AssertionError), which is not caught here.
        try:
            diff.execute_command(*args)
        except ResponseError:
            pass
