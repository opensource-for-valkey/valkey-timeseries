"""
Data classes for parsing TS.QUERY command results.

TS.QUERY executes a PromQL expression and returns a typed result map:

    { "resultType": <type>, "result": <data> }

where ``<type>`` is one of ``"vector"``, ``"matrix"``, ``"scalar"``, or
``"string"`` and ``<data>`` depends on the type:

+-------------+----------------------------------------------------------------+
| resultType  | result shape                                                   |
+=============+================================================================+
| vector      | list of { metric: {label→value}, value: [timestamp, float] }|
+-------------+----------------------------------------------------------------+
| matrix      | list of { metric: {label→value}, value: [[ts_ms, float], ...] }|
+-------------+----------------------------------------------------------------+
| scalar      | [timestamp, float]                                          |
+-------------+----------------------------------------------------------------+
| string      | [timestamp, str]                                            |
+-------------+----------------------------------------------------------------+

The ``from_raw`` constructors on every class accept the value returned directly
by ``client.execute_command("TS.QUERY", ...)``.  Both RESP2 (flat interleaved
lists for maps) and RESP3 (real Python dicts for maps) are handled
transparently.

Example::

    raw = client.execute_command("TS.QUERY", "http_requests", "TIME", "3")
    result = QueryResult.from_raw(raw)

    if result.is_vector():
        for series in result.result:           # List[VectorSample]
            print(series.name, series.value)   # metric labels + QuerySample
    elif result.is_scalar():
        print(result.result.value)             # ScalarResult.value
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Union


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _decode(v) -> str:
    """Decode *bytes* to *str*; pass everything else through unchanged."""
    return v.decode() if isinstance(v, bytes) else v


def _raw_to_dict(raw) -> dict:
    """
    Normalise a raw Valkey map response into a plain Python ``dict``.

    * RESP3 – already a ``dict``, returned as-is.
    * RESP2 – a flat list ``[k1, v1, k2, v2, ...]``, converted to a dict.
    """
    if isinstance(raw, dict):
        return raw
    it = iter(raw)
    return {k: next(it) for k in it}


def _normalize_labels(raw) -> Dict[str, str]:
    """Convert a label map (RESP3 dict or RESP2 flat list) to ``{str: str}``."""
    return {_decode(k): _decode(v) for k, v in _raw_to_dict(raw).items()}


def _get(d: dict, *keys):
    """Return the first value whose key is present in *d*."""
    for k in keys:
        if k in d:
            return d[k]
    return None


# ---------------------------------------------------------------------------
# Public data classes
# ---------------------------------------------------------------------------

@dataclass
class QuerySample:
    """
    A single ``(timestamp, value)`` data point.

    ``timestamp`` is the Unix timestamp in **milliseconds** as returned by
    the server.  ``value`` is a ``float``.
    """

    timestamp: int
    value: float

    @classmethod
    def from_raw(cls, raw) -> "QuerySample":
        """Parse from a 2-element sequence ``[timestamp, value]``."""
        return cls(timestamp=int(raw[0]), value=float(raw[1]))

    def __repr__(self) -> str:
        return f"QuerySample(timestamp={self.timestamp}, value={self.value!r})"


@dataclass
class VectorSample:
    """
    One series entry in a **vector** (instant-query) result.

    Corresponds to the server reply shape::

        {
            "metric": { "__name__": "up", "job": "api", ... },
            "value":  [ timestamp, float ]
        }
    """

    #: Label set identifying this series (e.g. ``{"__name__": "up", ...}``).
    metric: Dict[str, str]
    #: The single sample at the evaluation timestamp.
    value: QuerySample

    @classmethod
    def from_raw(cls, raw) -> "VectorSample":
        """Parse one vector entry from a raw Valkey response."""
        d = _raw_to_dict(raw)
        metric_raw = _get(d, b"metric", "metric") or {}
        value_raw = _get(d, b"value", "value") or []
        return cls(
            metric=_normalize_labels(metric_raw),
            value=QuerySample.from_raw(value_raw),
        )

    @property
    def name(self) -> str:
        """The ``__name__`` label value, or an empty string if absent."""
        return self.metric.get("__name__", "")

    def label(self, name: str) -> str:
        """Return the value of label *name*, or an empty string if absent."""
        return self.metric.get(name, "")


@dataclass
class MatrixSample:
    """
    One series entry in a **matrix** (range-query) result.

    Corresponds to the server reply shape::

        {
            "metric": { "__name__": "up", "job": "api", ... },
            "value":  [ [timestamp, float], ... ]
        }
    """

    #: Label set identifying this series.
    metric: Dict[str, str]
    #: Ordered list of samples across the query range.
    values: List[QuerySample] = field(default_factory=list)

    @classmethod
    def from_raw(cls, raw) -> "MatrixSample":
        """Parse one matrix entry from a raw Valkey response."""
        d = _raw_to_dict(raw)
        metric_raw = _get(d, b"metric", "metric") or {}
        values_raw = _get(d, b"value", "value") or []
        return cls(
            metric=_normalize_labels(metric_raw),
            values=[QuerySample.from_raw(s) for s in values_raw],
        )

    @property
    def name(self) -> str:
        """The ``__name__`` label value, or an empty string if absent."""
        return self.metric.get("__name__", "")

    def label(self, name: str) -> str:
        """Return the value of label *name*, or an empty string if absent."""
        return self.metric.get(name, "")


@dataclass
class ScalarResult:
    """
    Result of a scalar PromQL expression.

    Corresponds to::

        [ timestamp, float ]
    """

    timestamp: int
    value: float

    @classmethod
    def from_raw(cls, raw) -> "ScalarResult":
        """Parse from a 2-element sequence ``[timestamp, value]``."""
        return cls(timestamp=int(raw[0]), value=float(raw[1]))

    def __repr__(self) -> str:
        return f"ScalarResult(timestamp={self.timestamp}, value={self.value!r})"


@dataclass
class StringResult:
    """
    Result of a string PromQL expression.

    Corresponds to::

        [ timestamp, str ]
    """

    timestamp: int
    value: str

    @classmethod
    def from_raw(cls, raw) -> "StringResult":
        """Parse from a 2-element sequence ``[timestamp, string_value]``."""
        return cls(timestamp=int(raw[0]), value=_decode(raw[1]))

    def __repr__(self) -> str:
        return f"StringResult(timestamp={self.timestamp}, value={self.value!r})"


# Union alias used as the type of ``QueryResult.result``.
QueryResultData = Union[
    List[VectorSample],  # resultType == "vector"
    List[MatrixSample],  # resultType == "matrix"
    ScalarResult,  # resultType == "scalar"
    StringResult,  # resultType == "string"
]


@dataclass
class QueryResult:
    """
    Fully parsed result of a ``TS.QUERY`` command.

    Attributes:
        result_type:
            One of ``"vector"``, ``"matrix"``, ``"scalar"``, ``"string"``.
        result:
            Parsed result data whose concrete type matches ``result_type``:

            * ``"vector"``  → ``List[VectorSample]``
            * ``"matrix"``  → ``List[MatrixSample]``
            * ``"scalar"``  → ``ScalarResult``
            * ``"string"``  → ``StringResult``

    Typical usage::

        raw = client.execute_command("TS.QUERY", "http_requests")
        qr = QueryResult.from_raw(raw)

        if qr.is_vector():
            for series in qr.result:
                print(series.name, series.value.timestamp, series.value.value)
        elif qr.is_matrix():
            for series in qr.result:
                for pt in series.values:
                    print(series.name, pt.timestamp, pt.value)
        elif qr.is_scalar():
            print(qr.result.value)
        elif qr.is_string():
            print(qr.result.value)
    """

    result_type: str
    result: QueryResultData

    # ------------------------------------------------------------------
    # Construction
    # ------------------------------------------------------------------

    @classmethod
    def from_raw(cls, raw) -> "QueryResult":
        """
        Parse the raw value returned by
        ``client.execute_command("TS.QUERY", ...)`` into a ``QueryResult``.

        Supports both RESP2 (flat-list maps) and RESP3 (dict maps).

        Raises:
            ValueError: if ``resultType`` is not one of the four known types.
        """
        d = _raw_to_dict(raw)
        result_type = _decode(_get(d, b"resultType", "resultType") or b"")
        result_raw = _get(d, b"result", "result") or []

        if result_type == "vector":
            result: QueryResultData = [VectorSample.from_raw(s) for s in result_raw]
        elif result_type == "matrix":
            result = [MatrixSample.from_raw(s) for s in result_raw]
        elif result_type == "scalar":
            result = ScalarResult.from_raw(result_raw)
        elif result_type == "string":
            result = StringResult.from_raw(result_raw)
        else:
            raise ValueError(
                f"Unexpected TS.QUERY resultType {result_type!r}. "
                "Expected one of: 'vector', 'matrix', 'scalar', 'string'."
            )

        return cls(result_type=result_type, result=result)

    # ------------------------------------------------------------------
    # Type-guard helpers
    # ------------------------------------------------------------------

    def is_vector(self) -> bool:
        """Return ``True`` when ``result_type == "vector"``."""
        return self.result_type == "vector"

    def is_matrix(self) -> bool:
        """Return ``True`` when ``result_type == "matrix"``."""
        return self.result_type == "matrix"

    def is_scalar(self) -> bool:
        """Return ``True`` when ``result_type == "scalar"``."""
        return self.result_type == "scalar"

    def is_string(self) -> bool:
        """Return ``True`` when ``result_type == "string"``."""
        return self.result_type == "string"

    def __repr__(self) -> str:
        return f"QueryResult(result_type={self.result_type!r}, result={self.result!r})"
