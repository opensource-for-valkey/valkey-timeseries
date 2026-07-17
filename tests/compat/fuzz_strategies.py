"""Hypothesis strategies for the differential fuzzer (test plan §4.3).

Produces random but *valid-by-construction* command sequences over a small
key/label universe: a create phase (series with random options + labels and at
most one compaction rule), an interleaved write phase (ADD/MADD/INCRBY/DECRBY/
DEL with adversarial timestamps and values), and a read phase (RANGE/REVRANGE/
GET/MRANGE/MREVRANGE/MGET/QUERYINDEX with random option combinations).

The generator deliberately stays inside the input space both engines accept, so
that a failure is a *reply* divergence (values, float formatting, aggregation,
reply shape) rather than an input-rejection-boundary difference — that boundary
is already covered exhaustively by the §6 read-path matrix, and several of its
edges are intentional divergences (DIV-0013/0019/0020) that would otherwise
drown the fuzz signal. Concretely the generator never emits:

  - `*` / relative timestamps on writes (server clocks differ — nondeterministic);
  - negative-only or bare-name label matchers (accepted-input supersets, §5.2);
  - `twa` aggregation (DIV-0012, unsupported → subject errors);
  - direct writes to a compaction target;
  - `TS.RANGE` bounds with from > to (inverted-range superset, DIV-0013);
  - `TS.DEL` against a series with a retention window (DIV-0021, an owner-pending
    retention-model divergence pinned by test_compat_del.py);
  - `EMPTY` over an unbounded span (see MAX_EMPTY_BUCKETS — it OOM-kills either engine).

Everything a command emits is a `str`, so a shrunk failing example round-trips
losslessly into a JSON corpus file (see corpus/ and test_compat_corpus.py).

Written clean-room from public RedisTimeSeries documentation and black-box
observation of the reference server. Do NOT consult RedisTimeSeries source or
test code (see tests/compat/README.md).
"""

from __future__ import annotations

from typing import List, Tuple

import hypothesis.strategies as st

from compat_helpers import AGGREGATORS

Command = Tuple[str, ...]

# -- universe ---------------------------------------------------------------

BASE_KEYS = [f"fz:k{i}" for i in range(4)]
ROLLUP_KEY = "fz:rollup"  # dedicated compaction target; never written directly

# Adversarial non-negative timestamps: 0, bucket boundaries and ±1 around 1000,
# clustered small values, and far-future magnitudes. Negatives are omitted
# because both engines reject them (a matching error, no signal).
TIMESTAMPS = [
    0, 1, 2, 499, 500, 501, 999, 1000, 1001, 1500, 1999, 2000, 2001,
    3000, 5000, 10_000, 2**31, 2**40,
]

# Values sent verbatim to BOTH engines as strings, so storage/read-back float
# formatting divergence (the classic source) is observable. Includes signed
# zero, subnormals, huge magnitudes and high-precision decimals.
#
# Magnitudes are capped at ~1e150. RTS's variance/stddev aggregators accumulate a naive
# *sum* of squares, so they overflow to +Inf (and return NaN where we return the correct
# value) once that sum exceeds DBL_MAX — DIV-0022, pinned by
# test_compat_range.py::TestAggregationOverflow. The cap must therefore leave headroom for
# accumulation, not just for a single square: 1e150 squares to 1e300, so even ~20 samples
# in one bucket stay finite, whereas sqrt(DBL_MAX) (~1.34e154) would overflow at the second
# sample. The generator picks aggregators independently of the values written, so it can
# not correlate the two — the cap is what keeps std.p/var.p/std.s/var.s in the fuzzable
# set. DBL_MAX itself is still covered by that test and by corpus/float_formatting_*.
VALUES = [
    "0", "0.0", "-0.0", "1", "-1", "42", "-42", "3.14",
    "3.141592653589793", "2.718281828459045", "0.1", "0.2", "0.3",
    "123456789.123456789", "-9.999999999999999e149", "1e150", "1e-308",
    "5e-324", "-1e150", "100", "0.00001",
]

# INCRBY/DECRBY deltas: finite, no subnormals/signed-zero edge tokens (those
# belong on the value axis, not the accumulation axis).
DELTAS = ["1", "0.5", "2.5", "100", "0.1", "1000", "42"]

ENCODINGS = ["COMPRESSED", "UNCOMPRESSED"]
DUPLICATE_POLICIES = ["BLOCK", "FIRST", "LAST", "MIN", "MAX", "SUM"]
RETENTIONS = ["0", "1000", "5000"]
CHUNK_SIZES = ["128", "4096", "1024"]
BUCKETS = ["500", "1000", "2000"]
BUCKET_TIMESTAMPS = ["-", "+", "~"]

# An `EMPTY` aggregation query materializes one bucket for every bucket-duration across the
# *whole queried span*, not just the ones holding samples. Combined with the far-future
# timestamps above that explodes: a 2**40 ms span at a 500 ms bucket is ~2.2e9 buckets, which
# OOM-kills either engine (observed: the reference container exiting 137/OOMKilled mid-run).
# EMPTY is therefore only emitted with explicit numeric bounds whose span stays under this many
# buckets — symbolic `-`/`+` bounds are excluded because the span is then the series' full
# extent, which the generator does not know here.
MAX_EMPTY_BUCKETS = 5_000

# Positive-anchored matchers over the labels the create phase emits. Every entry
# has at least one `label=value` term, so none is a negative-only / bare-name
# superset (DIV-0019/DIV-0020).
MATCHERS = [
    "metric=cpu", "metric=mem", "host=h1", "host=h2",
    "metric=(cpu,mem)", "host=(h1,h2)", "region=us",
]
LABEL_NAMES = ["metric", "host", "region"]

# Valid GROUPBY reducers (RTS 8.6). `first`/`last`/`twa` are not reducers.
GROUP_REDUCERS = ["sum", "min", "max", "avg", "range", "count", "std.p", "std.s", "var.p", "var.s"]


# -- sub-strategies ---------------------------------------------------------


@st.composite
def _create_options(draw) -> List[str]:
    opts: List[str] = [
        "RETENTION", draw(st.sampled_from(RETENTIONS)),
        "ENCODING", draw(st.sampled_from(ENCODINGS)),
        "CHUNK_SIZE", draw(st.sampled_from(CHUNK_SIZES)),
        "DUPLICATE_POLICY", draw(st.sampled_from(DUPLICATE_POLICIES)),
    ]
    return opts


@st.composite
def _labels(draw) -> List[str]:
    labels = ["metric", draw(st.sampled_from(["cpu", "mem"])),
              "host", draw(st.sampled_from(["h1", "h2"]))]
    if draw(st.booleans()):
        labels += ["region", draw(st.sampled_from(["us", "eu"]))]
    return labels


def _empty_is_safe(bounds: Tuple[str, str], bucket: str) -> bool:
    """True if `EMPTY` over `bounds` at `bucket` yields a bounded number of buckets.

    Requires explicit numeric bounds: with symbolic `-`/`+` the span is the series' full
    extent, which can reach the far-future timestamps and explode (see MAX_EMPTY_BUCKETS).
    """
    frm, to = bounds
    if not (frm.lstrip("-").isdigit() and to.lstrip("-").isdigit()):
        return False
    span = int(to) - int(frm)
    return 0 <= span // int(bucket) <= MAX_EMPTY_BUCKETS


@st.composite
def _range_options(draw, bounds: Tuple[str, str], aggregation_allowed: bool = True) -> List[str]:
    """Options common to TS.RANGE/REVRANGE (emitted in RTS syntax order).

    `bounds` is the already-drawn (from, to) pair; it gates `EMPTY` (see `_empty_is_safe`).
    """
    opts: List[str] = []
    if draw(st.booleans()):
        opts.append("LATEST")
    if draw(st.booleans()):
        lo, hi = sorted(draw(st.sampled_from(VALUES)) for _ in range(2))
        opts += ["FILTER_BY_VALUE", lo, hi]
    if draw(st.booleans()):
        opts += ["COUNT", str(draw(st.integers(min_value=1, max_value=10)))]
    if aggregation_allowed and draw(st.booleans()):
        if draw(st.booleans()):
            opts += ["ALIGN", draw(st.sampled_from(["start", "end", "0", "1000"]))]
        bucket = draw(st.sampled_from(BUCKETS))
        opts += ["AGGREGATION", draw(st.sampled_from(AGGREGATORS)), bucket]
        if draw(st.booleans()):
            opts += ["BUCKETTIMESTAMP", draw(st.sampled_from(BUCKET_TIMESTAMPS))]
        if _empty_is_safe(bounds, bucket) and draw(st.booleans()):
            opts.append("EMPTY")
    return opts


@st.composite
def _bounds(draw) -> Tuple[str, str]:
    """A (from, to) pair with from <= to. Symbolic '-'/'+' or ordered numbers."""
    if draw(st.booleans()):
        return "-", "+"
    lo, hi = sorted(draw(st.sampled_from(TIMESTAMPS)) for _ in range(2))
    frm = "-" if draw(st.booleans()) else str(lo)
    to = "+" if draw(st.booleans()) else str(hi)
    return frm, to


@st.composite
def _filter(draw) -> List[str]:
    matchers = draw(st.lists(st.sampled_from(MATCHERS), min_size=1, max_size=2, unique=True))
    return matchers


# -- write / read op strategies ---------------------------------------------


@st.composite
def _write_op(draw, writable: List[str], deletable: List[str]) -> Command:
    key = draw(st.sampled_from(writable))
    ts = str(draw(st.sampled_from(TIMESTAMPS)))
    val = draw(st.sampled_from(VALUES))
    kinds = ["ADD", "MADD", "INCRBY", "DECRBY"]
    if deletable:
        kinds.append("DEL")
    kind = draw(st.sampled_from(kinds))
    if kind == "ADD":
        return ("TS.ADD", key, ts, val)
    if kind == "MADD":
        triples: List[str] = []
        for _ in range(draw(st.integers(min_value=1, max_value=3))):
            triples += [draw(st.sampled_from(writable)),
                        str(draw(st.sampled_from(TIMESTAMPS))),
                        draw(st.sampled_from(VALUES))]
        return ("TS.MADD", *triples)
    if kind in ("INCRBY", "DECRBY"):
        # Explicit TIMESTAMP: never the server clock (which differs per engine).
        return (f"TS.{kind}", key, draw(st.sampled_from(DELTAS)), "TIMESTAMP", ts)
    # DEL over an ordered range, on a retention-0 series only (see `deletable`).
    lo, hi = sorted(draw(st.sampled_from(TIMESTAMPS)) for _ in range(2))
    return ("TS.DEL", draw(st.sampled_from(deletable)), str(lo), str(hi))


@st.composite
def _read_op(draw, keys: List[str]) -> Command:
    kind = draw(st.sampled_from(
        ["RANGE", "REVRANGE", "GET", "MRANGE", "MREVRANGE", "MGET", "QUERYINDEX"]
    ))
    if kind in ("RANGE", "REVRANGE"):
        key = draw(st.sampled_from(keys))
        frm, to = draw(_bounds())
        return (f"TS.{kind}", key, frm, to, *draw(_range_options((frm, to))))
    if kind == "GET":
        key = draw(st.sampled_from(keys))
        opt = ["LATEST"] if draw(st.booleans()) else []
        return ("TS.GET", key, *opt)
    if kind in ("MRANGE", "MREVRANGE"):
        frm, to = draw(_bounds())
        opts: List[str] = []
        if draw(st.booleans()):
            opts.append("WITHLABELS")
        opts += draw(_range_options((frm, to)))
        # GROUPBY...REDUCE is the trailing clause, AFTER FILTER (RTS syntax order).
        groupby: List[str] = []
        if draw(st.booleans()):
            groupby = ["GROUPBY", draw(st.sampled_from(LABEL_NAMES)),
                       "REDUCE", draw(st.sampled_from(GROUP_REDUCERS))]
        return (f"TS.{kind}", frm, to, *opts, "FILTER", *draw(_filter()), *groupby)
    if kind == "MGET":
        opts = ["WITHLABELS"] if draw(st.booleans()) else []
        return ("TS.MGET", *opts, "FILTER", *draw(_filter()))
    return ("TS.QUERYINDEX", *draw(_filter()))


# -- top-level program strategy ---------------------------------------------


@st.composite
def programs(draw) -> List[Command]:
    """A full create/write/read command sequence over the fuzz universe."""
    n_series = draw(st.integers(min_value=1, max_value=len(BASE_KEYS)))
    keys = BASE_KEYS[:n_series]

    commands: List[Command] = []
    # TS.DEL is restricted to retention-0 series: deleting a range that covers only
    # already-expired samples is DIV-0021 (RTS trims lazily and still counts the sample;
    # we trim eagerly, so it is already gone), an owner-pending divergence pinned by
    # tests/compat/test_compat_del.py. Generating it here would trip the fuzzer on a
    # known divergence forever and drown the signal — the same reason twa (DIV-0012) and
    # the FILTER supersets (DIV-0019/0020) are excluded above.
    deletable: List[str] = []
    for key in keys:
        opts = draw(_create_options())
        if opts[opts.index("RETENTION") + 1] == "0":
            deletable.append(key)
        commands.append(("TS.CREATE", key, *opts, "LABELS", *draw(_labels())))

    read_keys = list(keys)
    if draw(st.booleans()):
        src = draw(st.sampled_from(keys))
        commands.append(("TS.CREATE", ROLLUP_KEY, "LABELS", "metric", "cpu", "host", "h1"))
        commands.append(("TS.CREATERULE", src, ROLLUP_KEY, "AGGREGATION",
                         draw(st.sampled_from(AGGREGATORS)), draw(st.sampled_from(BUCKETS))))
        read_keys = list(keys) + [ROLLUP_KEY]  # rollup is readable, not writable

    for _ in range(draw(st.integers(min_value=0, max_value=14))):
        commands.append(draw(_write_op(keys, deletable)))

    for _ in range(draw(st.integers(min_value=1, max_value=8))):
        commands.append(draw(_read_op(read_keys)))

    return commands
