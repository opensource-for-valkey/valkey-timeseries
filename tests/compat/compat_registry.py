"""Known-divergence registry (test plan §5.3).

`divergences.yml` is the single source of truth for intentional divergences from
RedisTimeSeries 8.6. The differential harness consults it before failing a test:
a mismatch fully covered by registry entries is recorded as XFAIL-DIVERGENT in the
conformance report instead of failing.

Registry entry kinds and the comparator delta kinds they may cover:

  reply-superset  -> 'superset' deltas   (extra fields/keys emitted by the subject)
  error-text      -> 'error-text' deltas (same error condition, different wording)
  float-format    -> 'float-format' deltas (numerically equal, textually different)
  behavior        -> 'value' / 'shape' / 'error-condition' deltas
  config-name     -> config parameter naming differences (§7.1 tests)
  unsupported     -> whole surfaces intentionally not supported

A "reference errors, subject succeeds" error-condition mismatch is NEVER registrable:
that is an accepted-input superset, which silently breaks scripts ported back to
Redis (plan §5.2). The harness hard-fails those regardless of registry contents.
"""

from __future__ import annotations

import os
import re
from dataclasses import dataclass, field
from typing import List, Optional

import yaml

REGISTRY_PATH = os.path.join(os.path.dirname(__file__), "divergences.yml")

VALID_KINDS = {
    "reply-superset",
    "error-text",
    "float-format",
    "behavior",
    "config-name",
    "unsupported",
}

# Comparator delta kind -> registry entry kind that may cover it.
DELTA_TO_REGISTRY_KIND = {
    "superset": "reply-superset",
    "error-text": "error-text",
    "float-format": "float-format",
    "value": "behavior",
    "shape": "behavior",
    "error-condition": "behavior",
}


@dataclass(frozen=True)
class DivergenceEntry:
    id: str
    command: str
    kind: str
    description: str
    rationale: str = ""
    since: str = ""
    # Optional regex applied to the delta detail string, to scope an entry to a
    # specific field/path instead of the whole command.
    details_regex: str = ""

    def covers(self, command: str, delta_kind: str, detail: str) -> bool:
        if self.kind != DELTA_TO_REGISTRY_KIND.get(delta_kind):
            return False
        if self.command.upper() != command.upper():
            return False
        if self.details_regex and not re.search(self.details_regex, detail):
            return False
        return True


@dataclass
class Registry:
    entries: List[DivergenceEntry] = field(default_factory=list)

    def find(self, command: str, delta_kind: str, detail: str) -> Optional[DivergenceEntry]:
        for entry in self.entries:
            if entry.covers(command, delta_kind, detail):
                return entry
        return None


def load_registry(path: str = REGISTRY_PATH) -> Registry:
    if not os.path.exists(path):
        return Registry()
    with open(path, "r") as f:
        raw = yaml.safe_load(f) or []

    entries = []
    seen_ids = set()
    for item in raw:
        entry = DivergenceEntry(
            id=str(item["id"]),
            command=str(item["command"]),
            kind=str(item["kind"]),
            description=str(item.get("description", "")).strip(),
            rationale=str(item.get("rationale", "")).strip(),
            since=str(item.get("since", "")),
            details_regex=str(item.get("details_regex", "")),
        )
        if entry.kind not in VALID_KINDS:
            raise ValueError(f"{path}: entry {entry.id} has invalid kind {entry.kind!r}")
        if entry.id in seen_ids:
            raise ValueError(f"{path}: duplicate divergence id {entry.id}")
        if not entry.rationale:
            raise ValueError(f"{path}: entry {entry.id} is missing a rationale")
        seen_ids.add(entry.id)
        entries.append(entry)
    return Registry(entries=entries)
