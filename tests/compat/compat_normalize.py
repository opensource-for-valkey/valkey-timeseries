"""Reply normalization and structural diffing (test plan §5.1).

Both sides' raw client replies are normalized with the same rules, then diffed
recursively. The diff yields `Delta` records classified by kind:

  'value'           genuinely different values
  'shape'           different structure (length, type, missing dict keys)
  'superset'        dict keys present only on the subject side
  'float-format'    numerically equal within rel. tolerance 1e-12 but textually
                    (or bit-wise) different — failing by default, registrable
  'error-condition' one side errored inside a reply element, the other did not
  'error-text'      both errored, wording differs

Normalization rules implemented here:
  1. Floats: bulk-string doubles are compared by parsed value with relative
     tolerance 1e-12 while the raw strings are preserved for the report.
  2. Maps: RESP3 maps and RESP2 flattened name/value arrays compare as dicts
     (TS.INFO, label sets).
  3. TS.INFO nondeterministic fields (memoryUsage, chunkCount) compare by
     type/presence only — enumerated explicitly, never pattern-based.
  4. MRANGE/MGET/QUERYINDEX cross-series ordering is normalized (sort by key);
     within-series sample order is compared exactly.
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field
from typing import Any, List

from valkey.exceptions import ResponseError

FLOAT_REL_TOL = 1e-12

# TS.INFO fields whose values are implementation-dependent; compared by presence
# and type only (plan §5.1 rule 5). Every exemption is enumerated here.
INFO_TYPE_ONLY_FIELDS = {"memoryUsage", "chunkCount"}


@dataclass
class Delta:
    kind: str
    path: str
    reference: Any
    subject: Any
    note: str = ""

    def detail(self) -> str:
        return (
            f"{self.path}: reference={self.reference!r} subject={self.subject!r}"
            + (f" ({self.note})" if self.note else "")
        )


class TypeOnly:
    """Sentinel wrapper: compare by python type only (nondeterministic values)."""

    __slots__ = ("type_name", "raw")

    def __init__(self, value):
        self.type_name = type(value).__name__
        self.raw = value

    def __repr__(self):
        return f"TypeOnly({self.type_name}: {self.raw!r})"


class ErrorReply:
    """An error embedded inside a reply (e.g. per-item TS.MADD errors)."""

    __slots__ = ("text",)

    def __init__(self, exc: Exception):
        self.text = str(exc)

    def __repr__(self):
        return f"ErrorReply({self.text!r})"


class SetReply:
    """A RESP3 set reply. Order-insensitive, but type-distinct from an array."""

    __slots__ = ("items",)

    def __init__(self, items):
        self.items = sorted(items, key=repr)

    def __repr__(self):
        return f"SetReply({self.items!r})"


def _decode(value):
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="backslashreplace")
    return value


def _try_float(value):
    if isinstance(value, float):
        return value
    if isinstance(value, str):
        try:
            return float(value)
        except ValueError:
            return None
    return None


def _floats_equal(a: float, b: float) -> bool:
    if math.isnan(a) and math.isnan(b):
        return True
    return a == b


def _floats_close(a: float, b: float) -> bool:
    if math.isnan(a) and math.isnan(b):
        return True
    if math.isinf(a) or math.isinf(b):
        return a == b
    return math.isclose(a, b, rel_tol=FLOAT_REL_TOL, abs_tol=0.0)


def _generic(value):
    """Generic normalization: decode bytes, recurse containers, wrap errors/sets."""
    if isinstance(value, Exception):
        return ErrorReply(value)
    if isinstance(value, (set, frozenset)):
        return SetReply([_generic(v) for v in value])
    if isinstance(value, dict):
        return {_decode(k): _generic(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_generic(v) for v in value]
    return _decode(value)


def _pairs_to_dict(value):
    """Convert label/field lists to a dict.

    Handles both shapes seen across RESP2 replies:
      [[name, value], [name, value], ...]   (list of pairs)
      [name, value, name, value, ...]       (flattened alternating list)
    Dicts (RESP3) pass through. None passes through (kept distinct from {}
    so a nil-vs-empty divergence stays observable).
    """
    if value is None or isinstance(value, dict):
        return _generic(value)
    if isinstance(value, (list, tuple)):
        if all(isinstance(v, (list, tuple)) and len(v) == 2 for v in value):
            return {_decode(k): _generic(v) for k, v in value}
        if len(value) % 2 == 0 and all(
            isinstance(v, (bytes, str)) for v in value[::2]
        ):
            it = iter(value)
            return {_decode(k): _generic(v) for k, v in zip(it, it)}
    return _generic(value)


def _normalize_info(reply):
    info = _pairs_to_dict(reply)
    if not isinstance(info, dict):
        return info
    out = {}
    for key, value in info.items():
        if key in INFO_TYPE_ONLY_FIELDS:
            out[key] = TypeOnly(value)
        elif key == "labels":
            out[key] = _pairs_to_dict(value)
        elif key == "rules" and isinstance(value, list):
            out[key] = sorted(
                value, key=lambda r: repr(r[0] if isinstance(r, list) and r else r)
            )
        else:
            out[key] = value
    return out


def _series_key(entry):
    if isinstance(entry, (list, tuple)) and entry:
        return repr(entry[0])
    return repr(entry)


def _normalize_multi_series(reply):
    """TS.MRANGE / TS.MREVRANGE / TS.MGET replies.

    RESP2: array of [key, labels, samples-or-sample] entries -> sort by key,
    labels dict-ified. RESP3: map keyed by series name -> generic dict compare.
    """
    if isinstance(reply, dict):
        return {
            _decode(k): _generic(v) for k, v in reply.items()
        }
    if isinstance(reply, (list, tuple)):
        entries = []
        for entry in reply:
            if isinstance(entry, (list, tuple)) and len(entry) >= 2:
                entry = list(entry)
                normalized = [_decode(entry[0]), _pairs_to_dict(entry[1])]
                normalized.extend(_generic(v) for v in entry[2:])
                entries.append(normalized)
            else:
                entries.append(_generic(entry))
        return sorted(entries, key=_series_key)
    return _generic(reply)


def _normalize_queryindex(reply):
    if isinstance(reply, (set, frozenset)):
        return SetReply([_generic(v) for v in reply])
    if isinstance(reply, (list, tuple)):
        return sorted((_generic(v) for v in reply), key=repr)
    return _generic(reply)


_NORMALIZERS = {
    "TS.INFO": _normalize_info,
    "TS.MRANGE": _normalize_multi_series,
    "TS.MREVRANGE": _normalize_multi_series,
    "TS.MGET": _normalize_multi_series,
    "TS.QUERYINDEX": _normalize_queryindex,
}


def normalize_reply(command_name: str, reply):
    handler = _NORMALIZERS.get(command_name.upper())
    if handler is not None:
        return handler(reply)
    return _generic(reply)


def diff_replies(reference, subject, path: str = "$") -> List[Delta]:
    deltas: List[Delta] = []
    _diff(reference, subject, path, deltas)
    return deltas


def _diff(ref, sub, path, deltas: List[Delta]):
    # Nondeterministic values: type/presence only.
    if isinstance(ref, TypeOnly) or isinstance(sub, TypeOnly):
        if not (isinstance(ref, TypeOnly) and isinstance(sub, TypeOnly)):
            deltas.append(Delta("shape", path, ref, sub, "type-only sentinel on one side"))
        elif ref.type_name != sub.type_name:
            deltas.append(Delta("value", path, ref, sub, "type-only field, type differs"))
        return

    # Embedded errors (e.g. TS.MADD per-item errors).
    if isinstance(ref, ErrorReply) or isinstance(sub, ErrorReply):
        if not (isinstance(ref, ErrorReply) and isinstance(sub, ErrorReply)):
            deltas.append(Delta("error-condition", path, ref, sub))
        elif ref.text != sub.text:
            kind = "error-text" if _same_error_prefix(ref.text, sub.text) else "error-condition"
            deltas.append(Delta(kind, path, ref.text, sub.text))
        return

    # RESP3 sets.
    if isinstance(ref, SetReply) or isinstance(sub, SetReply):
        if not (isinstance(ref, SetReply) and isinstance(sub, SetReply)):
            deltas.append(Delta("shape", path, ref, sub, "set vs non-set reply"))
            return
        _diff(ref.items, sub.items, path + "{set}", deltas)
        return

    if isinstance(ref, dict) or isinstance(sub, dict):
        if not (isinstance(ref, dict) and isinstance(sub, dict)):
            deltas.append(Delta("shape", path, ref, sub, "map vs non-map reply"))
            return
        for key in ref:
            if key not in sub:
                deltas.append(
                    Delta("shape", f"{path}.{key}", ref[key], None, "missing on subject side")
                )
        for key in sub:
            if key not in ref:
                deltas.append(
                    Delta("superset", f"{path}.{key}", None, sub[key], "extra subject field")
                )
        for key in ref:
            if key in sub:
                _diff(ref[key], sub[key], f"{path}.{key}", deltas)
        return

    if isinstance(ref, list) or isinstance(sub, list):
        if not (isinstance(ref, list) and isinstance(sub, list)):
            deltas.append(Delta("shape", path, ref, sub, "array vs non-array reply"))
            return
        if len(ref) != len(sub):
            deltas.append(
                Delta("shape", path, ref, sub, f"length {len(ref)} vs {len(sub)}")
            )
            return
        for i, (r, s) in enumerate(zip(ref, sub)):
            _diff(r, s, f"{path}[{i}]", deltas)
        return

    if ref == sub and type(ref) is type(sub):
        return

    # Scalar mismatch. Classify float representation deltas.
    ref_f, sub_f = _try_float(ref), _try_float(sub)
    if ref_f is not None and sub_f is not None:
        if _floats_equal(ref_f, sub_f):
            if type(ref) is not type(sub):
                deltas.append(
                    Delta("shape", path, ref, sub, "same numeric value, different reply type")
                )
            elif isinstance(ref, str) and ref != sub:
                deltas.append(
                    Delta("float-format", path, ref, sub, "identical parsed value, different text")
                )
            return
        if _floats_close(ref_f, sub_f):
            deltas.append(
                Delta(
                    "float-format",
                    path,
                    ref,
                    sub,
                    "numerically equal within 1e-12 relative tolerance",
                )
            )
        else:
            deltas.append(Delta("value", path, ref, sub))
        return

    # bool vs int (RESP3 booleans) and other type-level mismatches.
    if type(ref) is not type(sub) and ref == sub:
        deltas.append(Delta("shape", path, ref, sub, "same value, different reply type"))
        return

    deltas.append(Delta("value", path, ref, sub))


def _same_error_prefix(a: str, b: str) -> bool:
    return _error_prefix(a) == _error_prefix(b)


def _error_prefix(text: str) -> str:
    head = text.split(" ", 1)[0] if text else ""
    if head.endswith(":"):
        return head
    return head if head.isupper() else ""
