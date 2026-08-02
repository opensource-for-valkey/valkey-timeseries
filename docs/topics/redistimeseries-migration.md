# Migrating from RedisTimeSeries

A practical guide for application teams moving an existing RedisTimeSeries workload to
valkey-timeseries. It condenses [COMPATIBILITY.md](../../COMPATIBILITY.md), which remains
the authoritative contract — consult it when you need the full rationale for a difference
described here.

---

## The short version

Your **commands, query syntax, and data model** carry over unchanged. What does *not*
carry over is everything around the data: configuration, metrics, persisted files, log
output, and error message text. Plan for a rebuild-and-re-ingest migration, not a file
copy.

| Area | Status |
|---|---|
| Command names, arguments, options, reply shapes | Compatible — a difference is a bug |
| Query semantics (`FILTER`, `AGGREGATION`, `GROUPBY`/`REDUCE`, `FILTER_BY_*`, `WITHLABELS`) | Compatible — a difference is a bug |
| Data model (samples, labels, retention, chunk settings, duplicate policy, compaction rules) | Compatible — a difference is a bug |
| Configuration parameters | Different surface — `ts-` prefixed, via `CONFIG SET` |
| `INFO` and `TS.INFO` fields | Different metric set |
| RDB / AOF / `DUMP` payloads, replication stream | Incompatible by design — no conversion tooling |
| `std.p` / `std.s` / `var.p` / `var.s` | Numerically different — valkey-timeseries is the correct one |
| `sum` / `avg` on mixed-magnitude buckets (including `GROUPBY … REDUCE`) | Numerically different — valkey-timeseries is the stable, reproducible one |
| Error message *wording*, log messages | Not part of the contract |
| Ordering of series in multi-series replies, ordering of labels | Undefined in both — do not depend on it |
| ACL enforcement on multi-series queries | Stricter here |

---

## What just works

Issue the same commands with the same client libraries. `TS.CREATE`, `TS.ALTER`,
`TS.DEL`, `TS.CREATERULE`, `TS.DELETERULE`, `TS.ADD`, `TS.MADD`, `TS.INCRBY`,
`TS.DECRBY`, `TS.GET`, `TS.MGET`, `TS.RANGE`, `TS.REVRANGE`, `TS.MRANGE`, `TS.MREVRANGE`,
`TS.INFO`, `TS.QUERYINDEX`, and `TS.QUERYLABELS` all keep their argument order, option
names, and reply shapes.

Query semantics are equally in-contract: label matchers (`label=value`, `label!=value`,
`label=(v1,v2)`, presence/absence forms), `AGGREGATION` with `ALIGN` / `BUCKETTIMESTAMP` /
`EMPTY`, `FILTER_BY_TS`, `FILTER_BY_VALUE`, `COUNT`, `WITHLABELS`, `SELECTED_LABELS`,
`GROUPBY … REDUCE`, and `EXCLUDEEMPTY`. The same query over equivalently configured
series returns the same series and the same samples.

If any of the above behaves differently, that is a **defect** — report it.

---

## What you must change before cutover

### 1. Configuration moves to `CONFIG SET`

RedisTimeSeries takes configurables as `MODULE LOAD` / `MODULE LOADEX` arguments.
valkey-timeseries exposes them through Valkey's native configuration machinery under
`ts-` names, so most become runtime-adjustable:

| RedisTimeSeries | valkey-timeseries |
|---|---|
| `RETENTION_POLICY` | `ts-retention-policy` |
| `CHUNK_SIZE_BYTES` | `ts-chunk-size` |
| `DUPLICATE_POLICY` | `ts-duplicate-policy` |
| `ENCODING` | `ts-encoding` |
| `NUM_THREADS` | `ts-num-threads` (startup-only) |
| `COMPACTION_POLICY` | `ts-compaction-policy` |

Rewrite config files and any automation that set these at load time. Applications that
never touched configuration are unaffected.

### 2. `TS.INFO` parsing and `INFO` field references

The per-series `TS.INFO` response shape and the module's global `INFO` metrics differ.
Some RedisTimeSeries fields have no equivalent here; valkey-timeseries reports metrics of
its own (indexing, cluster fan-out) with no counterpart there.

Update anything that parses `TS.INFO` and retest those paths — a silent parse mismatch
produces misleading values rather than an error. Re-point dashboards, alerts, and scrapers
at the valkey-timeseries metric set.

### 3. Cluster mode: review workarounds, not fan-out

**You almost certainly have no application-side fan-out to remove.** Current
RedisTimeSeries already fans label-based multi-series commands out across the cluster
itself: Redis documents `TS.MGET`, `TS.MRANGE`, `TS.MREVRANGE`, `TS.QUERYINDEX`, and
`TS.QUERYLABELS` as "cross-slot (all shards)" in *every* clustered configuration, and exposes
`NUM_THREADS` / `ts-num-threads` as "the maximum number of per-shard threads for cross-key
queries when using cluster mode." valkey-timeseries does the same over Valkey's native
cluster bus. This step exists to clean up *workarounds*, not to replace missing function.

Look for these, and note that none of them break — they just stop being necessary:

- **Per-shard query-and-merge logic** written against an older RedisTimeSeries. Before
  v1.6 you had to load RedisGears alongside the module to make `TS.MGET`, `TS.MRANGE`, and
  `TS.QUERYINDEX` cluster-aware; applications from that era often merged shard results
  themselves.
- **Hash-tag co-location adopted purely to keep a query on one shard.** Still fine to
  keep, and still *required* for the cases below — but no longer needed just to make a
  multi-series query see all its data.

What actually changes operationally:

- **No global module password to distribute.** RedisTimeSeries before 8.0 required
  `OSS_GLOBAL_PASSWORD` on every cluster node for time series in a cluster (8.0 replaced it
  with an internal shared-secret mechanism). valkey-timeseries has no equivalent: it uses
  Valkey's cluster bus, with no coordinator component and no additional ports, so there is
  no new firewall or security-group configuration.
- **Handle the fan-out timeout.** A cross-shard operation returns an error when a
  consistent cluster-wide view cannot be established within `ts-fanout-command-timeout`.
  Make sure callers handle it rather than treating it as an empty result.

What stays the same in both engines, and still needs hash tags:

- Compaction rules are a two-key operation — source and destination must live in the same
  slot. This applies to `TS.CREATERULE` / `TS.DELETERULE` and to any default compaction
  policy in a clustered deployment.
- `TS.MADD` operates within a single slot.
- On RedisTimeSeries, an `MRANGE` cannot be part of a `MULTI`/`EXEC` transaction on a
  cluster.

Deployments with cluster mode disabled skip this step entirely.

### 4. Stop depending on error text and log messages

Error *wording* is not part of the contract — the semantic error conditions align, the
strings may not. Branch on the operation and structured signals, not on substrings or
regexes over error replies.

Log output is valkey-timeseries-specific in content, format, severity, and frequency. A
given RedisTimeSeries log event may be absent here, emitted at a different level, or
emitted under different conditions. Anything that waits for a log line, counts a phrase,
or treats an absent message as a signal needs replacing with command-, metric-, or
application-level observability.

### 5. Stop depending on reply ordering

Samples within a series are always in timestamp order — ascending for
`TS.RANGE`/`TS.MRANGE`, descending for `TS.REVRANGE`/`TS.MREVRANGE`. That much is
contractual.

The order of *distinct series* in a `TS.MRANGE`, `TS.MREVRANGE`, `TS.MGET`, or
`TS.QUERYINDEX` reply is not. Neither is the order of label name/value pairs within a
series under `WITHLABELS` / `SELECTED_LABELS`. Key off the series' Valkey key or its
labels, and look labels up by name — never by position. Standard client libraries already
parse labels into name-keyed maps and are unaffected; hand-rolled parsers that read "index
0 is `__name__`" will break.

### 6. Review ACL keyspace restrictions

valkey-timeseries enforces the calling user's ACL keyspace restrictions on label-based
multi-series operations (`TS.MRANGE`, `TS.MREVRANGE`, `TS.MGET`, `TS.QUERYINDEX`, and the
label-discovery commands), so a client only sees series it is permitted to access.
RedisTimeSeries applies these less consistently. A query that looked complete there may
return a filtered subset here when the user's key permissions are narrower than the set of
matching series.

### 7. Clean up sloppy syntax

Where RedisTimeSeries silently accepted extraneous or malformed input — unrecognized
trailing arguments, filter fragments outside the documented grammar — valkey-timeseries
may reject it with an error. This is deliberate, and it is the safe direction: you get a
visible, fixable error instead of wrong data. Fix the offending call sites.

---

## Moving the data

**There is no file-level migration path, and none is planned.** RDB files and `DUMP`
payloads do not cross between the modules, and a valkey-timeseries primary cannot
replicate to a RedisTimeSeries replica (or the reverse). Rebuild series from the
underlying data using one of:

- **Export / re-ingest** — for each series read samples with `TS.RANGE key - +` and
  metadata with `TS.INFO`, recreate it on the target with `TS.CREATE` (labels, retention,
  duplicate policy, chunk settings) plus its `TS.CREATERULE` rules, then bulk-load with
  `TS.MADD`.
- **Live dual-write** — during a cutover window write new samples to both deployments
  while backfilling history with the recipe above, then switch reads.

The failure modes are defined and tested rather than left to chance: `RESTORE` of a
RedisTimeSeries `DUMP` payload fails cleanly and creates no key, and starting
valkey-timeseries on a RedisTimeSeries RDB file is refused with a clear log message. In
the other direction a valkey-timeseries payload is likewise rejected cleanly. What is
guaranteed is that the rejection is clean and creates nothing — not the specific message.

---

## Numbers that will legitimately change

Two aggregation surfaces return **different values** from RedisTimeSeries because
RedisTimeSeries computes them with a numerically unstable method. The two are not
equivalent claims, and the distinction matters when you decide what to trust:

- **Variance family** — valkey-timeseries returns the *mathematically correct* result
  where RedisTimeSeries returns `NaN`, exactly `0`, or a value wrong in its leading
  digits. Its value is the one to reconcile against.
- **`sum` / `avg` on mixed-magnitude buckets** — valkey-timeseries's result is the *more
  stable and reproducible* one, not necessarily the exact sum. On inputs this
  ill-conditioned neither engine lands close to the true total; the difference is that
  compensated summation gives the same answer every time, while RedisTimeSeries's depends
  on the accumulation order neither module specifies.

Neither is gated by `strict` mode: there is no second *intended* behavior to select, only
an unstable computation to reproduce.

### `std.p` / `std.s` / `var.p` / `var.s`

RedisTimeSeries uses the textbook `E[x²] − E[x]²` identity, which fails two ways:

- **Overflow** — a sample above ~1.34e154 squares to `+Inf`, and the bucket returns `NaN`.
  `std.p` of a single `1.8e308` sample: `NaN` there, `0` here (one sample has zero
  variance).
- **Cancellation** — samples clustered tightly relative to their magnitude lose accuracy
  at *any* magnitude. `{100, 100.0001}` disagrees at 2.2e-4 relative;
  `{12345.6789, 12345.679}` returns `NaN` there. Where cancellation is total,
  RedisTimeSeries returns exactly `0` for a non-zero deviation — observed from ~1.2e8
  upward.

Reachable two ways: an `AGGREGATION std.*|var.*` clause on a read, or reading a
destination fed by a `std.*|var.*` compaction rule (which also exposes it through `TS.GET`
and `TS.MGET`).

### `sum` / `avg` over buckets that mix magnitudes

valkey-timeseries uses compensated (Neumaier) summation; RedisTimeSeries adds naively,
absorbing small terms into the rounding of large ones. No extreme values needed — only a
mix within one bucket:

| Bucket contents | valkey-timeseries | RedisTimeSeries |
|---|---|---|
| `1e150`, `1`, `-1e150` | `1` | `0` |
| `1e16`, `1`, `1`, `-1e16` | `2` | `0` |
| `1e6`, `0.001`, `-1e6` | `0.001` | `1.0000000474974513e-3` |

The third row is the ordinary case — a millisecond added to a counter near one million.
Note that the RedisTimeSeries result is not always `0`: total cancellation gives `0`,
partial cancellation gives a non-zero value wrong in its leading digits.

Reachable three ways — this is a wider surface than the variance family:

- an `AGGREGATION sum|avg` clause on a read,
- a **`GROUPBY … REDUCE sum|avg`** reducer on `TS.MRANGE` / `TS.MREVRANGE`, where the
  reducer combines values across series,
- the aggregation of a `sum`/`avg` compaction rule (also surfacing through `TS.GET` and
  `TS.MGET` on the destination).

Buckets whose samples sit within a few orders of magnitude of each other agree exactly.
Under `GROUPBY`, note that the result also depends on the order in which the reducer
visits series — which neither module specifies, so RedisTimeSeries has no stable value to
match even against itself.

### What to do about both

1. Remove handling that treats `NaN` from a variance aggregator as "no data" — you will
   get a real number now.
2. Re-tune thresholds, alerts, and anomaly detectors calibrated against RedisTimeSeries
   output; a deviation previously reported as `0` may now be non-zero, and firing behavior
   changes with it.
3. Expect a **step change at the migration boundary** in any compaction destination whose
   history was materialized under RedisTimeSeries — stored bucket values are not
   recomputed.
4. Inventory your `GROUPBY … REDUCE sum|avg` call sites alongside the plain aggregation
   ones; a reducer that combines series at different magnitudes hits the same difference
   and is easy to miss when auditing only `AGGREGATION` clauses.

Applications not using the variance family, and those whose sampled values stay within a
few orders of magnitude of each other, can skip this entirely.

### Ordinary floating-point noise

Separately from the above, small differences in the last digits are expected anywhere
floating-point arithmetic is involved (aggregation, `FILTER_BY_VALUE`, `INCRBY`
accumulation, text parsing/formatting) and are **not** bugs. Do not compare results for
exact equality. A numeric difference *larger* than rounding, outside the two surfaces
above, **is** a bug.

---

## Strict compatibility mode

A few divergences have a uniquely dangerous shape: both engines accept the command, both
return a result, and the results differ — no error to alert you. `ts-compatibility-mode`
closes that class.

```
CONFIG SET ts-compatibility-mode strict
CONFIG GET ts-compatibility-mode
```

| Value | Behavior |
|---|---|
| `extended` (default) | valkey-timeseries semantics |
| `strict` | Resolve the gated cases the way RedisTimeSeries 8.10 resolves them |

Settable at runtime and at startup, fully reversible, and applies only to subsequent
commands — it rewrites no stored state, so switching either direction is safe.

**What it gates:**

| Case | `extended` | `strict` |
|---|---|---|
| A repeated option on the range family (e.g. `COUNT 5 COUNT 2`, duplicated `AGGREGATION` / `FILTER` / `GROUPBY` / …) | **Last** occurrence wins | **First** occurrence wins |
| `TS.GET` / `TS.MGET` without `LATEST` on a compaction destination whose *older* bucket was back-filled | The last sample the destination actually holds | RedisTimeSeries's cached last-sample, which back-filling does not refresh |

The back-fill gate has one limitation: its marker is runtime-only, so after a reload or
restart the destination reports its true last sample again until the next forward bucket
close.

**What it does not gate:** the numeric divergences above; anything where
valkey-timeseries accepts input RedisTimeSeries rejects (relative range bounds like `-1h`
and `*`, cascading compaction rules, complement filters, bare metric-name selectors, and
the valkey-timeseries-only commands all stay available in both modes); anything where
valkey-timeseries is *stricter*; and the intentional incompatibilities — configuration,
metrics, persistence, logs, error text. RESP2 float *formatting* is also out of scope: the
parsed values are bit-identical.

---

## Extensions

valkey-timeseries adds surfaces RedisTimeSeries does not have: forecasting
(`TS.FORECAST`, `TS.AUTOFORECAST`), anomaly and outlier detection (`TS.OUTLIERS`),
decomposition and trend analysis (`TS.DECOMPOSE`, `TS.TREND`), correlation
(`TS.AUTOCORRELATION`, `TS.XCORR`), stationarity and periodicity (`TS.STATIONARITY`,
`TS.PERIODS`), feature extraction (`TS.FEATURES`), gap filling (`TS.FILLGAPS`), joins
(`TS.JOIN`), and richer discovery (`TS.LABELNAMES`, `TS.LABELVALUES`, `TS.METRICNAMES`,
`TS.LABELSTATS`). It also accepts some combinations of existing options that
RedisTimeSeries rejects.

Neither form can affect a migrating application — it cannot have been using a surface that
returned an error. Adopting them does, however, cost you portability back to
RedisTimeSeries.

---

## Migration checklist

| # | Step | Skippable when |
|---|---|---|
| 1 | Confirm every feature you use is supported in your target release | never — do this first |
| 2 | Move `MODULE LOAD` config to `ts-` parameters via `CONFIG SET` | you never set module config |
| 3 | Update `TS.INFO` response parsing, and retest those paths | you don't parse `TS.INFO` |
| 4 | Remap or drop unsupported `INFO` / `TS.INFO` field references | you don't scrape them |
| 5 | Review cluster workarounds; handle the fan-out timeout | cluster mode disabled |
| 6 | Remove dependencies on error text and log messages | you don't inspect either |
| 7 | Audit assumptions about series order in multi-series replies | you key off keys/labels already |
| 8 | Review custom parsers for label ordering | you use a standard client library |
| 9 | Review ACL keyspace restrictions | you don't use ACL key patterns |
| 10 | Re-baseline consumers of `std.*` / `var.*` | you don't use the variance family |
| 11 | Re-baseline `sum` / `avg` over mixed-magnitude buckets — reads, `GROUPBY … REDUCE` reducers, and compaction rules | your values stay within a few orders of magnitude |

---

## Reporting problems

A behavior difference on a **compatible** surface — command syntax, query semantics, data
model — is a defect worth reporting: an application that produces different results, fails
where it succeeded, or succeeds where it failed. Not defects: anything in the
"Intentional Incompatibilities" list above, stricter input validation, reply/label
ordering, error wording, and last-digit floating-point noise.

Where a compatibility bug is fixed in a minor or patch release, the old behavior stays
available through `ts-emulate-release` (set it to a release identifier below the fix to
keep the old behavior). `INFO` fields count uses of emulated incompatible behavior so you
can find call sites that still depend on it.

Full contract and rationale: [COMPATIBILITY.md](../../COMPATIBILITY.md).
