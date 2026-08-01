"""Shared scenario helpers for the read-path matrix modules (test plan §6).

Written clean-room from public RedisTimeSeries documentation and black-box
observation of the reference server. Do NOT consult RedisTimeSeries source or
test code (see tests/compat/README.md).
"""

from __future__ import annotations

# The 13 aggregators RedisTimeSeries 8.10 exposes (plan §6, TS.CREATERULE row).
ALL_RTS_AGGREGATORS = (
    "avg",
    "sum",
    "min",
    "max",
    "range",
    "count",
    "first",
    "last",
    "std.p",
    "std.s",
    "var.p",
    "var.s",
    "twa",
)

# `twa` is not implemented (DIV-0012, kind `unsupported`): we reply "unknown
# aggregation type" where RTS computes a time-weighted average. That is a
# reference-errors-nowhere/subject-errors mismatch on every twa cell, which the
# registry can not express per-delta — so the matrix parametrization excludes it
# and test_aggregation_twa_unsupported pins the divergence in one place instead.
# Delete UNSUPPORTED_AGGREGATORS (and that test) when twa lands.
UNSUPPORTED_AGGREGATORS = ("twa",)

AGGREGATORS = tuple(a for a in ALL_RTS_AGGREGATORS if a not in UNSUPPORTED_AGGREGATORS)

# Aggregators whose result is defined for a single-sample bucket without
# variance/stddev degeneracy. std.s/var.s need >= 2 samples; the sample-variance
# family's single-sample reply is itself a matrix cell (see test_range_aggregation_*).
SIMPLE_AGGREGATORS = (
    "avg",
    "sum",
    "min",
    "max",
    "range",
    "count",
    "first",
    "last",
)


def mk_series(diff, key, *extra):
    """Create a series with every engine-default-sensitive option pinned.

    CHUNK_SIZE/ENCODING/DUPLICATE_POLICY are explicit so that default drift
    between the engines (a §7.1 config-parity concern, covered by
    test_compat_config.py) doesn't leak into every read-path test.
    """
    return diff(
        "TS.CREATE", key,
        "RETENTION", 0,
        "CHUNK_SIZE", 4096,
        "ENCODING", "COMPRESSED",
        "DUPLICATE_POLICY", "BLOCK",
        *extra,
    )


def add_samples(diff, key, samples):
    """Add (timestamp, value) pairs to an existing series."""
    for ts, value in samples:
        diff("TS.ADD", key, ts, value)


def mk_populated(diff, key, samples, *extra):
    """Create a series and fill it in one step."""
    mk_series(diff, key, *extra)
    add_samples(diff, key, samples)


def mk_label_universe(diff):
    """The canonical multi-series fixture for filter/GROUPBY scenarios.

    Deliberately includes an asymmetry: `region` is absent from mem:1, so
    label-absent series behavior (SELECTED_LABELS nils, GROUPBY on a missing
    label) is exercised by the same universe.
    """
    mk_populated(
        diff, "u:cpu:1",
        [(100, 10.0), (200, 20.0), (300, 30.0)],
        "LABELS", "metric", "cpu", "host", "h1", "region", "us",
    )
    mk_populated(
        diff, "u:cpu:2",
        [(100, 40.0), (200, 50.0)],
        "LABELS", "metric", "cpu", "host", "h2", "region", "eu",
    )
    mk_populated(
        diff, "u:mem:1",
        [(100, 512.0), (250, 640.0)],
        "LABELS", "metric", "mem", "host", "h1",
    )
    mk_populated(
        diff, "u:mem:2",
        [(150, 128.0)],
        "LABELS", "metric", "mem", "host", "h2", "region", "us",
    )
