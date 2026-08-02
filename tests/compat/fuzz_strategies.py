"""Hypothesis strategies for the differential fuzzer (test plan §4.3).

Produces random but *valid-by-construction* command sequences over a small
key/label universe: a create phase (series with random options + labels and at
most one compaction rule), an interleaved write phase (ADD/MADD/INCRBY/DECRBY/
DEL with adversarial timestamps and values), and a read phase (RANGE/REVRANGE/
GET/MRANGE/MREVRANGE/NRANGE/NREVRANGE/MGET/QUERYINDEX with random option
combinations).

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
  - `FILTER_BY_VALUE` on a read that can reach a std/var compaction target
    (DIV-0024..0029 — see VARIANCE_AGGREGATORS below);
  - an ill-conditioned aggregator (the variance family, or sum/avg) together with the
    adversarial value set — see ILL_CONDITIONED_AGGREGATORS;
  - `EMPTY` over an unbounded span (see MAX_EMPTY_BUCKETS — it OOM-kills either engine);
  - a repeated aggregator inside one `AGGREGATION` list (`avg,avg`), which RTS accepts and
    we reject — the stricter direction, registrable but not registered;
  - a compaction destination in a `TS.NRANGE`/`TS.NREVRANGE` key list, except forward over
    a non-variance rule (see `programs`);
  - `EMPTY` on a *reverse* read that can reach a compaction destination (the second symptom
    of DIV-0030/0031 — the reference repeats the whole bucket run).

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
# sample. DBL_MAX itself is still covered by that test and by corpus/float_formatting_*.
#
# The cap alone does NOT make an aggregator fuzzable against this set — it only rules out
# the DIV-0022 overflow. Both remaining failure modes (variance cancellation, and the
# sum/avg cross-magnitude cancellation of DIV-0039) strike well inside the capped range,
# and this set is deliberately adversarial about magnitude mixing: 1e150 and 1e-308 are
# both in it. Correlating values with aggregators, which `programs` does, is what keeps
# those families fuzzable at all.
VALUES = [
    "0", "0.0", "-0.0", "1", "-1", "42", "-42", "3.14",
    "3.141592653589793", "2.718281828459045", "0.1", "0.2", "0.3",
    "123456789.123456789", "-9.999999999999999e149", "1e150", "1e-308",
    "5e-324", "-1e150", "100", "0.00001",
]

# INCRBY/DECRBY deltas: finite, no subnormals/signed-zero edge tokens (those
# belong on the value axis, not the accumulation axis).
DELTAS = ["1", "0.5", "2.5", "100", "0.1", "1000", "42"]

# Values/deltas used when a program may reach an ill-conditioned aggregator (see
# ILL_CONDITIONED_AGGREGATORS — the variance family *and* sum/avg).
#
# RTS's naive E[x^2]-E[x]^2 loses roughly `(mean/sd)^2 * DBL_EPSILON` of relative accuracy,
# so it disagrees with a stable algorithm whenever a bucket's samples are close *relative to
# their magnitude* — long before the DIV-0022 overflow boundary. Observed on the reference:
# {100, 100.1} is wrong in the 10th digit, {100, 100.0001} by 2.2e-4, and {12345.6789,
# 12345.679} comes back NaN. None of that is a subject bug (we are exact or near-exact),
# but it swamps the fuzz signal, and it cannot be registered — the registry sees only
# "path: reference=X subject=Y" and cannot tell which aggregator produced a value.
#
# sum/avg fail on the *opposite* input shape — a bucket that mixes magnitudes, where the
# large terms cancel and the small ones decide the answer (DIV-0039). Keeping magnitudes
# within a few orders of each other is what rules that out; the "spread >= 1" property
# below is what rules out the variance regime. This value set satisfies both at once.
#
# Keeping magnitudes small and spreads >= 1 holds (mean/sd)^2 * eps well under the harness's
# 1e-12 relative tolerance. Deltas are constrained too, not just values: the case that found
# this built 100 and 100.1 by `TS.INCRBY`, so a small delta on a large value reintroduces
# exactly the ill-conditioning the value set avoids.
WELL_CONDITIONED_VALUES = ["0", "1", "-1", "2", "-2", "5", "-5", "10", "-10"]
WELL_CONDITIONED_DELTAS = ["1", "2", "5", "10"]

ENCODINGS = ["COMPRESSED", "UNCOMPRESSED"]
DUPLICATE_POLICIES = ["BLOCK", "FIRST", "LAST", "MIN", "MAX", "SUM"]
RETENTIONS = ["0", "1000", "5000"]
CHUNK_SIZES = ["128", "4096", "1024"]
BUCKETS = ["500", "1000", "2000"]
BUCKET_TIMESTAMPS = ["-", "+", "~"]
ALIGNMENTS = ["start", "end", "0", "1000"]

# An ALIGN offset that is not a multiple of the bucket duration puts a bucket boundary below
# timestamp 0 once data sits within one bucket of it, and there the reference suppresses the
# leading/trailing EMPTY fill while we report it (DIV-0054, pinned per engine in
# tests/compat/test_compat_range.py). These two keep the grid non-negative over the
# non-negative timestamps this universe generates: `0` aligns to the epoch, and `start` to the
# window start, at or after which every sample the query reads lies.
EMPTY_SAFE_ALIGNMENTS = ["start", "0"]

# An `EMPTY` aggregation query materializes one bucket for every bucket-duration across the
# *whole queried span*, not just the ones holding samples. Combined with the far-future
# timestamps above that explodes: a 2**40 ms span at a 500 ms bucket is ~2.2e9 buckets, which
# OOM-kills either engine (observed: the reference container exiting 137/OOMKilled mid-run).
# EMPTY is therefore only emitted with explicit numeric bounds whose span stays under this many
# buckets — symbolic `-`/`+` bounds are excluded because the span is then the series' full
# extent, which the generator does not know here.
MAX_EMPTY_BUCKETS = 5_000

# Positive-anchored matchers over the labels the create phase emits. Every entry has at
# least one `label=value` term, so no generated filter list is a bare-name superset
# (DIV-0020) or unbounded. Unbounded lists are no longer a divergence — both engines now
# reject them identically — but generating one would only ever exercise that rejection,
# so the soak stays on filters that actually select series.
MATCHERS = [
    "metric=cpu", "metric=mem", "host=h1", "host=h2",
    "metric=(cpu,mem)", "host=(h1,h2)", "region=us",
]
LABEL_NAMES = ["metric", "host", "region"]

# Valid GROUPBY reducers (RTS 8.10). `first`/`last`/`twa` are not reducers.
GROUP_REDUCERS = ["sum", "min", "max", "avg", "range", "count", "std.p", "std.s", "var.p", "var.s"]

# The variance family. RTS evaluates it with the naive E[x^2]-E[x]^2 identity, which
# cancels to exactly 0 for large-magnitude, small-spread buckets where the true deviation
# is not 0 (DIV-0024..0029; the same naive formula also overflows — DIV-0022).
#
# As a *value* delta that is registered and rides through the fuzzer as XFAIL-DIVERGENT.
# But when such a value is compared against a predicate, the divergence changes shape: RTS's
# 0 passes `FILTER_BY_VALUE 0 0` and our correct 0.5 is filtered out, so the reply differs by
# a whole row (`length 1 vs 0`) rather than by a value. No registry entry can cover that
# safely — the only matching regex would also absorb "subject returned nothing where the
# reference returned data", the regression class most worth keeping.
#
# The filter is therefore withheld from reads that can reach a std/var *compaction target*,
# whose stored values are aggregator output. In-query `AGGREGATION std.p` / `GROUPBY REDUCE
# std.p` need no such guard: FILTER_BY_VALUE is applied to raw samples *before* aggregation
# (verified black-box on both engines), so it never sees the cancelled value.
VARIANCE_AGGREGATORS = frozenset({"std.p", "std.s", "var.p", "var.s"})

# sum/avg (DIV-0039, Tier C 2026-07-25). We accumulate with compensated (Neumaier)
# summation; RTS adds naively left to right. The two agree until a bucket mixes
# magnitudes — then the naive running sum absorbs the small terms into the rounding of
# the large ones, and once the large terms cancel, what remains is the absorption error
# rather than the small terms themselves. Measured against the reference:
#
#   {1e150, 1, -1e150}  -> we 1,     RTS 0
#   {1e16, 1, 1, -1e16} -> we 2,     RTS 0
#   {1e6, 0.001, -1e6}  -> we 0.001, RTS 1.0000000474974513e-3
#
# The last one matters for the exclusion below: total cancellation (RTS exactly 0) is
# what DIV-0024..0029's `reference='0'` regexes happen to absorb, but *partial*
# cancellation leaves a non-zero wrong value that no entry matches, so the soak fails on
# it. Neither shape is a subject bug, and neither is registrable for the same reason as
# the variance family: the registry cannot see which aggregator produced a value.
#
# Note this needs no extreme magnitudes — 1e6 with a millisecond added is enough. The
# VALUES cap does not help; only correlating values with aggregators does.
COMPENSATED_AGGREGATORS = frozenset({"sum", "avg"})

# Aggregators whose RTS implementation is numerically fragile, in either regime above.
# A program may use these only if its writes are well-conditioned (see `programs`).
ILL_CONDITIONED_AGGREGATORS = VARIANCE_AGGREGATORS | COMPENSATED_AGGREGATORS

# Aggregator/reducer pools for programs that opt out of the ill-conditioned families, so
# those programs can use the full adversarial value set (see WELL_CONDITIONED_VALUES).
# What is left — min/max/range/count/first/last — passes values through without
# accumulating, so it agrees with RTS on the whole adversarial set (verified per
# aggregator against the reference, 2026-07-25).
ROBUST_AGGREGATORS = [a for a in AGGREGATORS if a not in ILL_CONDITIONED_AGGREGATORS]
ROBUST_REDUCERS = [r for r in GROUP_REDUCERS if r not in ILL_CONDITIONED_AGGREGATORS]


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


def _align_choices(empty: bool) -> List[str]:
    """Alignments safe to draw alongside (or without) `EMPTY` — see EMPTY_SAFE_ALIGNMENTS."""
    return EMPTY_SAFE_ALIGNMENTS if empty else ALIGNMENTS


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
def _range_options(
    draw,
    bounds: Tuple[str, str],
    aggregation_allowed: bool = True,
    value_filter_allowed: bool = True,
    aggregators: List[str] = list(AGGREGATORS),
    empty_allowed: bool = True,
) -> List[str]:
    """Options common to TS.RANGE/REVRANGE (emitted in RTS syntax order).

    `bounds` is the already-drawn (from, to) pair; it gates `EMPTY` (see `_empty_is_safe`).
    `value_filter_allowed` is False when the read can reach a std/var compaction target,
    where a value predicate turns DIV-0024..0029 into an unregistrable shape delta (see
    VARIANCE_AGGREGATORS). `empty_allowed` is False for a *reverse* read that can reach a
    compaction destination: `EMPTY` there hits the second symptom of DIV-0030/0031, where the
    reference repeats the whole bucket run, and the row-count delta that produces is likewise
    unregistrable. Dropping `EMPTY` keeps the registered shape generated.
    """
    opts: List[str] = []
    if draw(st.booleans()):
        opts.append("LATEST")
    if value_filter_allowed and draw(st.booleans()):
        lo, hi = sorted(draw(st.sampled_from(VALUES)) for _ in range(2))
        opts += ["FILTER_BY_VALUE", lo, hi]
    if draw(st.booleans()):
        opts += ["COUNT", str(draw(st.integers(min_value=1, max_value=10)))]
    if aggregation_allowed and draw(st.booleans()):
        # EMPTY is decided before ALIGN because it narrows which alignments are safe to draw.
        bucket = draw(st.sampled_from(BUCKETS))
        empty = empty_allowed and _empty_is_safe(bounds, bucket) and draw(st.booleans())
        if draw(st.booleans()):
            opts += ["ALIGN", draw(st.sampled_from(_align_choices(empty)))]
        opts += ["AGGREGATION", draw(st.sampled_from(aggregators)), bucket]
        if draw(st.booleans()):
            opts += ["BUCKETTIMESTAMP", draw(st.sampled_from(BUCKET_TIMESTAMPS))]
        if empty:
            opts.append("EMPTY")
    return opts


@st.composite
def _nrange_options(
    draw,
    bounds: Tuple[str, str],
    key_count: int,
    aggregators: List[str] = list(AGGREGATORS),
) -> List[str]:
    """Options for TS.NRANGE/TS.NREVRANGE (emitted in RTS syntax order).

    Identical to `_range_options` apart from the AGGREGATION clause, which takes one
    aggregator argument per key — in key order, ahead of the single shared bucket duration.
    Each argument is a comma-separated list, drawn `unique` because RTS accepts a repeated
    aggregator inside one list (`avg,avg`) and we reject it.

    `value_filter_allowed` has no counterpart here: the key list never names a std/var
    compaction destination, which is the only case the guard exists for (see `programs`).
    """
    opts: List[str] = []
    if draw(st.booleans()):
        opts.append("LATEST")
    if draw(st.booleans()):
        lo, hi = sorted(draw(st.sampled_from(VALUES)) for _ in range(2))
        opts += ["FILTER_BY_VALUE", lo, hi]
    if draw(st.booleans()):
        opts += ["COUNT", str(draw(st.integers(min_value=1, max_value=10)))]
    if draw(st.booleans()):
        # See `_range_options`: EMPTY first, because it narrows the safe alignments.
        bucket = draw(st.sampled_from(BUCKETS))
        empty = _empty_is_safe(bounds, bucket) and draw(st.booleans())
        if draw(st.booleans()):
            opts += ["ALIGN", draw(st.sampled_from(_align_choices(empty)))]
        per_key = [
            ",".join(
                draw(
                    st.lists(
                        st.sampled_from(aggregators), min_size=1, max_size=2, unique=True
                    )
                )
            )
            for _ in range(key_count)
        ]
        opts += ["AGGREGATION", *per_key, bucket]
        if draw(st.booleans()):
            opts += ["BUCKETTIMESTAMP", draw(st.sampled_from(BUCKET_TIMESTAMPS))]
        if empty:
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


# The read commands `_read_op` draws from. A module constant so a focused run (or a
# bisect) can narrow it to one command without touching the strategy.
READ_KINDS = [
    "RANGE", "REVRANGE", "GET", "MRANGE", "MREVRANGE", "NRANGE", "NREVRANGE",
    "MGET", "QUERYINDEX", "QUERYLABELS",
]


# -- write / read op strategies ---------------------------------------------


@st.composite
def _write_op(
    draw,
    writable: List[str],
    deletable: List[str],
    values: List[str] = VALUES,
    deltas: List[str] = DELTAS,
) -> Command:
    key = draw(st.sampled_from(writable))
    ts = str(draw(st.sampled_from(TIMESTAMPS)))
    val = draw(st.sampled_from(values))
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
                        draw(st.sampled_from(values))]
        return ("TS.MADD", *triples)
    if kind in ("INCRBY", "DECRBY"):
        # Explicit TIMESTAMP: never the server clock (which differs per engine).
        return (f"TS.{kind}", key, draw(st.sampled_from(deltas)), "TIMESTAMP", ts)
    # DEL over an ordered range, on a retention-0 series only (see `deletable`).
    lo, hi = sorted(draw(st.sampled_from(TIMESTAMPS)) for _ in range(2))
    return ("TS.DEL", draw(st.sampled_from(deletable)), str(lo), str(hi))


@st.composite
def _read_op(
    draw,
    keys: List[str],
    variance_rollup: bool = False,
    aggregators: List[str] = list(AGGREGATORS),
    reducers: List[str] = GROUP_REDUCERS,
    pivot_keys: List[str] = list(BASE_KEYS),
) -> Command:
    """One read command. `variance_rollup` is True when the program created a std/var
    compaction rule, which withholds FILTER_BY_VALUE from reads that can reach the
    rollup — directly by key, or via a matcher (its labels are in the MATCHERS set).
    `aggregators`/`reducers` are narrowed to exclude the ill-conditioned families (the
    variance family and sum/avg) for programs written with the full adversarial value
    set — see ILL_CONDITIONED_AGGREGATORS. `pivot_keys` is the pool TS.NRANGE/TS.NREVRANGE
    draw their explicit key list from; it is narrower than `keys` (see `programs`)."""
    kind = draw(st.sampled_from(READ_KINDS))
    if kind in ("RANGE", "REVRANGE"):
        key = draw(st.sampled_from(keys))
        frm, to = draw(_bounds())
        value_filter_allowed = not (variance_rollup and key == ROLLUP_KEY)
        # See `_range_options`: EMPTY is withheld from a reverse read of the destination.
        empty_allowed = not (kind == "REVRANGE" and key == ROLLUP_KEY)
        return (f"TS.{kind}", key, frm, to,
                *draw(_range_options((frm, to), value_filter_allowed=value_filter_allowed,
                                    aggregators=aggregators, empty_allowed=empty_allowed)))
    if kind == "GET":
        key = draw(st.sampled_from(keys))
        opt = ["LATEST"] if draw(st.booleans()) else []
        return ("TS.GET", key, *opt)
    if kind in ("MRANGE", "MREVRANGE"):
        frm, to = draw(_bounds())
        opts: List[str] = []
        if draw(st.booleans()):
            opts.append("WITHLABELS")
        # A matcher can select the rollup, so the guard applies to the whole command.
        opts += draw(_range_options((frm, to), value_filter_allowed=not variance_rollup,
                                    aggregators=aggregators,
                                    empty_allowed=not (kind == "MREVRANGE"
                                                       and ROLLUP_KEY in keys)))
        # GROUPBY...REDUCE is the trailing clause, AFTER FILTER (RTS syntax order).
        groupby: List[str] = []
        if draw(st.booleans()):
            groupby = ["GROUPBY", draw(st.sampled_from(LABEL_NAMES)),
                       "REDUCE", draw(st.sampled_from(reducers))]
        # EXCLUDEEMPTY: both engines reject it alongside GROUPBY, so it is only
        # drawn for ungrouped reads. Emitted before FILTER — the trailing
        # position is an accepted-input superset (DIV-0050) that would diverge
        # by construction rather than finding anything.
        if not groupby and draw(st.booleans()):
            opts.append("EXCLUDEEMPTY")
        return (f"TS.{kind}", frm, to, *opts, "FILTER", *draw(_filter()), *groupby)
    if kind in ("NRANGE", "NREVRANGE"):
        # An explicit key list, not a filter: order matters and repeats are legal (each
        # occurrence gets its own reply column), so the draw is neither sorted nor unique.
        # The reverse command draws from the same pool minus the rollup — see `programs`.
        pool = pivot_keys if kind == "NRANGE" else [k for k in pivot_keys if k != ROLLUP_KEY]
        key_list = [
            draw(st.sampled_from(pool))
            for _ in range(draw(st.integers(min_value=1, max_value=3)))
        ]
        frm, to = draw(_bounds())
        return (f"TS.{kind}", str(len(key_list)), *key_list, frm, to,
                *draw(_nrange_options((frm, to), len(key_list), aggregators=aggregators)))
    if kind == "MGET":
        opts = ["WITHLABELS"] if draw(st.booleans()) else []
        return ("TS.MGET", *opts, "FILTER", *draw(_filter()))
    if kind == "QUERYLABELS":
        # FILTER is optional; the fuzzer never emits a bare metric-name matcher, so
        # the generator stays inside the shared accepted input space (DIV-0020).
        opts: List[str] = []
        if draw(st.booleans()):
            opts = ["FILTER", *draw(_filter())]
        if draw(st.booleans()):
            return ("TS.QUERYLABELS", "LABELS", *opts)
        return ("TS.QUERYLABELS", "VALUES", draw(st.sampled_from(LABEL_NAMES)), *opts)
    return ("TS.QUERYINDEX", *draw(_filter()))


# -- top-level program strategy ---------------------------------------------


@st.composite
def programs(draw) -> List[Command]:
    """A full create/write/read command sequence over the fuzz universe."""
    n_series = draw(st.integers(min_value=1, max_value=len(BASE_KEYS)))
    keys = BASE_KEYS[:n_series]

    # Decided up front, because the write phase is generated before the reads that would
    # choose an aggregator: either this program may use the ill-conditioned aggregators
    # (the variance family and sum/avg) and writes well-conditioned values, or it uses the
    # full adversarial value set with those aggregators withheld. Splitting it this way
    # keeps both axes covered instead of trading one away.
    well_conditioned = draw(st.booleans())
    values = WELL_CONDITIONED_VALUES if well_conditioned else VALUES
    deltas = WELL_CONDITIONED_DELTAS if well_conditioned else DELTAS
    aggregators = list(AGGREGATORS) if well_conditioned else ROBUST_AGGREGATORS
    reducers = GROUP_REDUCERS if well_conditioned else ROBUST_REDUCERS

    commands: List[Command] = []
    # TS.DEL is restricted to retention-0 series: deleting a range that covers only
    # already-expired samples is DIV-0021 (RTS trims lazily and still counts the sample;
    # we trim eagerly, so it is already gone), an owner-pending divergence pinned by
    # tests/compat/test_compat_del.py. Generating it here would trip the fuzzer on a
    # known divergence forever and drown the signal — the same reason twa (DIV-0012) and
    # the bare-name FILTER superset (DIV-0020) are excluded above.
    deletable: List[str] = []
    for key in keys:
        opts = draw(_create_options())
        if opts[opts.index("RETENTION") + 1] == "0":
            deletable.append(key)
        commands.append(("TS.CREATE", key, *opts, "LABELS", *draw(_labels())))

    read_keys = list(keys)
    # The FILTER_BY_VALUE guard below stays keyed on the variance family alone. A sum/avg
    # rollup can cancel to the same unregistrable shape delta in principle, but not here:
    # a program that may pick sum/avg at all is one that writes well-conditioned values,
    # and one that writes adversarial values cannot draw sum/avg as `rollup_agg`.
    variance_rollup = False
    if draw(st.booleans()):
        src = draw(st.sampled_from(keys))
        rollup_agg = draw(st.sampled_from(aggregators))
        variance_rollup = rollup_agg in VARIANCE_AGGREGATORS
        commands.append(("TS.CREATE", ROLLUP_KEY, "LABELS", "metric", "cpu", "host", "h1"))
        commands.append(("TS.CREATERULE", src, ROLLUP_KEY, "AGGREGATION",
                         rollup_agg, draw(st.sampled_from(BUCKETS))))
        read_keys = list(keys) + [ROLLUP_KEY]  # rollup is readable, not writable

    # The pool TS.NRANGE/TS.NREVRANGE name their keys from. Both divergence classes a read
    # of a compaction destination can reproduce are registered per command, and neither has
    # an entry for the pivot commands, so the rollup is admitted only where it cannot
    # reproduce either:
    #
    #   - the variance cancellation of DIV-0024..0029 needs a std|var rule, hence the
    #     `variance_rollup` gate;
    #   - the empty reply of DIV-0030/0031 needs reverse + LATEST + AGGREGATION over a
    #     destination, which is reverse-only — hence TS.NREVRANGE drops the rollup from
    #     the pool in `_read_op` while TS.NRANGE keeps it.
    #
    # Keeping the forward case is what preserves LATEST coverage for the pivot commands:
    # LATEST is a no-op on a series that is not a compaction.
    pivot_keys = list(keys)
    if ROLLUP_KEY in read_keys and not variance_rollup:
        pivot_keys.append(ROLLUP_KEY)

    for _ in range(draw(st.integers(min_value=0, max_value=14))):
        commands.append(draw(_write_op(keys, deletable, values, deltas)))

    for _ in range(draw(st.integers(min_value=1, max_value=8))):
        commands.append(
            draw(_read_op(read_keys, variance_rollup, aggregators, reducers, pivot_keys))
        )

    return commands
