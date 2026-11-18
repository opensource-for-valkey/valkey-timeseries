"""Clean-room TWA NaN boundary coverage against the pinned running reference.

Enumerate absent/NaN/real samples at both edges of three adjacent buckets, then
vary query clipping, direction, EMPTY, alignment, filters, mixed columns, and
COUNT. Only numeric-equivalent RESP formatting differences are ignored.
"""

import itertools
import json

import pytest

from compat_helpers import mk_series_command
from compat_normalize import diff_replies, normalize_reply


TIMESTAMPS = (0, 9, 10, 19, 20, 29)
VALUES = (1, 2, 4, 8, 3, 6)


def _assert_replies(command, reference, subject, samples):
    deltas = diff_replies(
        normalize_reply(command[0], reference),
        normalize_reply(command[0], subject),
    )
    deltas = [delta for delta in deltas if delta.kind != "float-format"]
    assert not deltas, (
        f"samples={samples!r}\ncommand={command!r}\n"
        f"reference={reference!r}\nsubject={subject!r}\n"
        + "\n".join(delta.detail() for delta in deltas)
    )


def _compare_batch(diff, commands, samples, failures=None):
    with diff.reference.pipeline(transaction=False) as reference:
        with diff.subject.pipeline(transaction=False) as subject:
            for command in commands:
                reference.execute_command(*command)
                subject.execute_command(*command)
            reference_replies = reference.execute(raise_on_error=False)
            subject_replies = subject.execute(raise_on_error=False)
    assert len(reference_replies) == len(subject_replies) == len(commands)
    for command, expected, actual in zip(commands, reference_replies, subject_replies):
        if failures is None:
            _assert_replies(command, expected, actual, samples)
        else:
            try:
                _assert_replies(command, expected, actual, samples)
            except AssertionError as error:
                failures.append({
                    "samples": samples,
                    "command": command,
                    "reference": expected,
                    "subject": actual,
                    "detail": str(error),
                })


def _populate(diff, key, samples):
    commands = [
        mk_series_command(key),
        *(("TS.ADD", key, timestamp, value) for timestamp, value in samples),
    ]
    _compare_batch(diff, commands, samples)


def _queries(key):
    # Core placement matrix: three buckets, clipped first/last buckets, a single
    # bucket, a zero-width gap query, and a query beyond all stored samples.
    variants = [
        (start, end, (), "twa", 10, ())
        for start, end in [("-", "+"), (5, 24), (10, 19), (15, 15), (30, 40)]
    ]
    variants.extend([
        ("-", "+", (), "twa", 1, ()),
        ("-", "+", (), "twa", 25, ()),
        ("-", "+", ("ALIGN", 3), "twa", 10, ()),
        ("-", "+", (), "twa", 10, ("BUCKETTIMESTAMP", "~")),
        ("-", "+", (), "twa", 10, ("BUCKETTIMESTAMP", "+")),
        ("-", "+", ("COUNT", 1), "twa", 10, ()),
        ("-", "+", ("COUNT", 2), "twa", 10, ()),
        ("-", "+", (), "avg,twa", 10, ()),
        ("-", "+", (), "countnan,twa", 10, ()),
    ])
    for filters in [
        ("FILTER_BY_TS", *TIMESTAMPS),
        ("FILTER_BY_TS", 0, 10, 20),
        ("FILTER_BY_TS", 9, 19, 29),
        ("FILTER_BY_TS", 0, 29),
        ("FILTER_BY_VALUE", 0, 10),
        ("FILTER_BY_VALUE", 2, 4),
        ("FILTER_BY_TS", *TIMESTAMPS, "FILTER_BY_VALUE", 2, 4),
    ]:
        variants.append((5, 24, filters, "twa", 10, ()))
    for command, empty in itertools.product(["TS.RANGE", "TS.REVRANGE"], [False, True]):
        for start, end, before, aggregators, duration, after in variants:
            args = (
                command, key, start, end, *before,
                "AGGREGATION", aggregators, duration, *after,
            )
            yield args + (("EMPTY",) if empty else ())


def test_twa_nan_three_bucket_matrix(diff, tmp_path):
    # 3**6 = 729 patterns, including empty series, all-NaN series, one-sided
    # endpoints, mixed buckets in either order, and consecutive NaN-only buckets.
    failures = []
    query_count = 0
    for index, pattern in enumerate(itertools.product(("absent", "nan", "real"), repeat=6)):
        samples = [
            (timestamp, "NaN" if state == "nan" else value)
            for timestamp, value, state in zip(TIMESTAMPS, VALUES, pattern)
            if state != "absent"
        ]
        key = f"twa:nan:matrix:{index}"
        _populate(diff, key, samples)
        queries = list(_queries(key))
        _compare_batch(diff, queries, samples, failures)
        query_count += len(queries)
    report = tmp_path / f"twa-nan-matrix-resp{diff.protocol}.json"
    report.write_text(json.dumps(
        {"patterns": 729, "queries": query_count, "failures": failures},
        indent=2,
        default=lambda value: value.decode() if isinstance(value, bytes) else str(value),
    ))
    assert not failures, (
        f"{len(failures)} mismatches across {query_count} queries; report: {report}\n"
        + failures[0]["detail"]
    )


@pytest.mark.parametrize("samples,bounds,duration", [
    ([], (0, 100), 10),
    ([(10, "NaN")], (0, 100), 10),
    ([(10, 7)], (0, 100), 10),
    ([(0, 1), (30, "NaN")], (0, 30), 10),
    ([(0, "NaN"), (30, 7)], (0, 30), 10),
    # One-sided empty buckets: distance equal to width does not hold a value.
    ([(0, 7), (100, "NaN")], (9, 31), 10),
    ([(0, "NaN"), (100, 7)], (69, 91), 10),
    # Both window endpoints must look past NaNs outside the selected interval.
    ([(0, 1), (20, "NaN"), (30, "NaN"), (100, 7)], (25, 75), 10),
    ([(0, 1), (20, "NaN"), (30, 5), (80, "NaN"), (100, 7)], (25, 75), 10),
    # Multiple NaNs in one bucket and a long NaN run spanning many buckets.
    ([(10, 2), (11, "NaN"), (12, "NaN"), (20, 6)], (0, 25), 25),
    ([(0, 1), *((t, "NaN") for t in range(1, 101)), (101, 7)], (0, 101), 10),
    # Sparse timestamps and a final bucket near the signed timestamp limit.
    ([(0, 1), (100_000, "NaN"), (1_000_000, 7)], (0, 1_000_000), 100_000),
    ([(2**63 - 101, 1), (2**63 - 51, "NaN"), (2**63 - 2, 7)],
     (2**63 - 101, 2**63 - 2), 25),
], ids=[
    "no-samples", "only-nan", "only-real", "trailing-nan", "leading-nan",
    "left-only-gap", "right-only-gap", "outside-nan-neighbors",
    "outside-nan-with-inside-real", "repeated-inside-nan", "long-nan-run",
    "sparse-timestamps", "timestamp-limit",
])
def test_twa_nan_extent_corners(diff, samples, bounds, duration, tmp_path):
    key = "twa:nan:extent"
    _populate(diff, key, samples)
    commands = [
        (
            command, key, *bounds, *count,
            "AGGREGATION", aggregators, duration,
            *(("EMPTY",) if empty else ()),
        )
        for command, empty, aggregators, count in itertools.product(
            ["TS.RANGE", "TS.REVRANGE"], [False, True],
            ["twa", "avg,twa", "countnan,twa"], [(), ("COUNT", 1), ("COUNT", 2)],
        )
    ]
    failures = []
    _compare_batch(diff, commands, samples, failures)
    report = tmp_path / f"twa-nan-extent-resp{diff.protocol}.json"
    report.write_text(json.dumps(
        {"samples": samples, "queries": len(commands), "failures": failures},
        indent=2,
        default=lambda value: value.decode() if isinstance(value, bytes) else str(value),
    ))
    assert not failures, (
        f"{len(failures)} mismatches across {len(commands)} queries; report: {report}\n"
        + failures[0]["detail"]
    )


def test_twa_latest_uses_the_open_compaction_sample(diff):
    _populate(diff, "twa:latest:src", [])
    _populate(diff, "twa:latest:dst", [])
    diff("TS.CREATERULE", "twa:latest:src", "twa:latest:dst", "AGGREGATION", "avg", 10)
    for timestamp in (2, 8, 12, 18, 22, 28):
        diff("TS.ADD", "twa:latest:src", timestamp, timestamp)
    for command, bounds, width, empty in itertools.product(
        ("TS.RANGE", "TS.REVRANGE"), (("-", "+"), (5, 30), (11, 25), (0, 25)),
        (10, 5, 4, 3), ((), ("EMPTY",)),
    ):
        diff(command, "twa:latest:dst", *bounds, "LATEST", "AGGREGATION", "twa", width, *empty)


@pytest.mark.parametrize("source,width", [
    ([(0, 5), (1, 5), (10, 15), (11, 15), (50, 25), (51, 25)], 10),
    ([(0, 1), (2, 9), (12, 4), (40, 8), (41, 8)], 3),
], ids=["wide-gap", "narrow-gap"])
def test_twa_latest_brackets_buckets_without_filling_gaps(diff, source, width):
    """The open compaction bucket closes the bucket before it, but it neither supports the gap
    rows between it and the stored samples nor extends EMPTY rows."""
    _populate(diff, "twa:gap:src", [])
    _populate(diff, "twa:gap:dst", [])
    diff("TS.CREATERULE", "twa:gap:src", "twa:gap:dst", "AGGREGATION", "avg", width)
    for timestamp, value in source:
        diff("TS.ADD", "twa:gap:src", timestamp, value)
    for command, bounds, empty in itertools.product(
        ("TS.RANGE", "TS.REVRANGE"), (("-", "+"), (0, 60), (15, 45)), ((), ("EMPTY",)),
    ):
        diff(command, "twa:gap:dst", *bounds, "LATEST", "AGGREGATION", "twa", width, *empty)


@pytest.mark.parametrize("command", ["TS.RANGE", "TS.REVRANGE"])
def test_twa_inverted_window_reports_its_empty_bucket(diff, command):
    _populate(diff, "twa:inverted", [(0, 0), (30, 60)])
    for empty in ((), ("EMPTY",)):
        diff(command, "twa:inverted", 15, 12, "AGGREGATION", "twa", 10, *empty)


@pytest.mark.parametrize("command", ["TS.RANGE", "TS.REVRANGE"])
def test_last_beside_twa_carries_filter_timestamps_outside_the_window(diff, command):
    _populate(diff, "twa:last", [(0, 7), (30, 3), (50, 9)])
    for aggregators in ("last,twa", "twa,last"):
        diff(command, "twa:last", 15, 35, "FILTER_BY_TS", 0, 30, 50,
             "AGGREGATION", aggregators, 10, "EMPTY")
