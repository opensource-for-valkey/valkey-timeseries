# ValkeyTimeSeries Module RFC (Revised RLuna 2026/08/09)

> **Revision note.** This revision reconciles the original RFC #4 with the current
> implementation (`github.com/opensource-for-valkey/valkey-timeseries`, Apache-2.0).
> Changes from the original are flagged with **[REVISED]**. The most consequential
> corrections: the default chunk encoding is **Chimp (ELF-on-Chimp)**, not Gorilla; the
> label index is an **Adaptive Radix Tree (the `blart` crate) keyed to 64-bit Roaring
> bitmaps (the `croaring` crate)**; cluster fan-out is a full **protobuf RPC layer over the
> Valkey cluster bus** with aggregation/GROUPBY/COUNT push-down; several commands
> (`TS.READ`, `TS.NRANGE`/`TS.NREVRANGE`, `TS.QUERYLABELS`, `TS.METRICNAMES`,
> `TS.LABELSTATS`) exist beyond the original surface; and variance/sum/avg use
> **numerically stable arithmetic**, a documented value divergence from RedisTimeSeries
> gated by a new `ts-compatibility-mode`.

## Abstract

`ValkeyTimeSeries` is a [Rust](https://www.rust-lang.org/) module that brings a native time
series data type to Valkey. It provides a purpose-built type optimized for high write
throughput, compression, and real-time analytics. Its command surface is a **superset of
RedisTimeSeries (RTS)**, enabling straightforward adoption for teams already running time
series workloads on compatible systems.

## Motivation

Support for a time series data type in Valkey is essential for developers in observability,
monitoring, IoT, and analytics. Valkey TimeSeries enables the unique characteristics of
time-series data:

* **Efficient Storage:** optimize for high write throughput and data compression to absorb
  a continuous influx of timestamped data.
* **Real-Time Analytics:** support real-time querying and computation to detect trends,
  anomalies, or patterns as they occur.
* **Time-centric Queries:** allow time-based aggregations and querying over specific
  intervals with high efficiency.

While time series can be modeled natively with `stream`s and `zset`s, a dedicated type
provides better performance, memory efficiency, and ease of use.

## Use Cases

### Infrastructure and Application Monitoring
Collect metrics from distributed services — CPU, memory, latency, error rates — and query
them in real time. Compaction rules automatically downsample high-resolution data into
coarser rollups while retaining full-resolution data for recent windows.

```shell
TS.CREATE cpu:host42 RETENTION 604800000 LABELS host host42 metric cpu
TS.ADD cpu:host42 * 73.2
TS.RANGE cpu:host42 -1h * AGGREGATION avg 60000
```

### IoT Sensor Ingestion
Handle continuous streams of sensor readings at high write throughput. Deduplication
intervals and retention manage lifecycle automatically; bulk ingestion via `TS.ADDBULK`
reduces round trips for batch uploads from edge devices.

```shell
TS.CREATE sensor:temp:floor3 RETENTION 2592000000 DEDUPE_INTERVAL 1000 LABELS type temperature location floor3
TS.ADDBULK sensor:temp:floor3 '{"values":[22.1,22.3,22.2],"timestamps":[1700000000000,1700000001000,1700000002000]}'
```

### Real-Time Anomaly Detection
Detect outliers as data arrives. `TS.OUTLIERS` applies statistical methods (Z-score, IQR,
MAD, EWMA, CUSUM, Random Cut Forest, etc.) directly on stored samples, with optional
seasonality adjustment (STL/MSTL) for periodic workloads.

```shell
TS.OUTLIERS request_latency:api -1h * SEASONALITY 24 METHOD MODIFIED-ZSCORE THRESHOLD 3.5
```

### Financial and Trading Analytics
Join price feeds with `TS.JOIN` ASOF semantics to compute spreads/correlations even when
timestamps across sources are not aligned.

```shell
TS.JOIN trades:buy trades:sell -1h * ASOF NEAREST 5ms ALLOW_EXACT_MATCH REDUCE sub
```

### SLA and Threshold Reporting
Conditional aggregators (`countif`, `sumif`, `share`, `all`, `any`, `none`) answer
"what fraction of requests exceeded target in each window?" in one query.

```shell
TS.RANGE request_latency:db -24h * AGGREGATION share 300000 CONDITION > 100
```

### Multi-Tenant Observability
Label-based indexing with Prometheus-style selectors queries across thousands of series by
tenant/region/service. Cluster fan-out distributes multi-range queries across shards
transparently.

```shell
TS.MRANGE -6h * FILTER http_requests{service="billing",region=~"us-.*"} AGGREGATION rate 60000
```

## Design Considerations

ValkeyTimeSeries provides commands to create time series and operate on them (add, query,
aggregate, compact, downsample, join). Series properties (encoding, retention, compaction,
rounding, etc.) are customizable per series and via configuration. Efficiency in memory and
performance is achieved through:

* parallelism and background tasks (multi-chunk fetches, compactions, index maintenance);
* efficient memory management (string interning for label pairs, ART prefix compression,
  Roaring bitmaps for series ids);
* efficient filtering and indexing;
* lock-free concurrent reads across series and chunks.

### Write Amplification

Disk-based TSDBs suffer 10–50× write amplification from index updates, WAL writes,
compactions, and LSM/SSTable merges. ValkeyTimeSeries sidesteps this class of problem by
operating in-memory:

* **No WAL/journal per write.** Samples append directly to the active chunk; persistence
  rides Valkey's RDB snapshots and AOF, amortizing cost across all data types.
* **No LSM compaction storms.** No SSTables, no background merge threads, no compaction
  tail-latency spikes.
* **Append-only chunk model.** Each chunk is a contiguous compressed buffer that grows until
  full, then seals. No in-place mutation, no page splits, no B-tree rebalancing.
* **Index writes are metadata-only.** The ART + Roaring index updates a few pointers when a
  *new series* is created — not on every sample.
* **Compaction is logical, not physical.** Downsampling rules aggregate into destination
  series; source data is never rewritten in place.

The trade-off is capacity: the working set must fit in memory. Retention and compaction
manage capacity. This is the kdb+ thesis — memory for the write path, compression and
retention for capacity — applied inside the Valkey ecosystem via standard clients.

Terminology:
* **TimeSeries Object:** top-level structure holding metadata and an ordered list of chunks.
* **Sample:** a `(64-bit timestamp, 64-bit float value)` tuple.
* **Chunk:** a container for samples with configurable size and encoding.

### Key Capabilities

* **Multi-db Support** — create series in different databases.
* **Joins** — INNER, OUTER (LEFT/RIGHT/FULL), SEMI, ANTI, and ASOF joins (`TS.JOIN`).
* **Filtering** — Prometheus-style selectors and RTS-style basic filters.
* **Compaction** — cascading downsampling rules; default policies with per-key filters:

  ```
  CONFIG SET ts-compaction-policy "avg:2h:10d|^metrics:memory:*;sum:60s:1h:5s|^metrics:cpu:*"
  ```

* **Aggregation** — extended set including `increase`, `rate`, `irate`, and conditional
  aggregators (`all`, `any`, `none`, `countif`, `sumif`, `share`).
* **Metadata** — label names/values, cardinality, and index statistics.
* **Rounding** — round sample values to a configured precision (enforced series-wide).
* **Active Expiration** — **[REVISED]** retention is applied *eagerly* on write (so
  `TS.INFO` agrees with `TS.RANGE`) and swept by a background task.
* **Developer Ergonomics** — relative timestamps (`-6h`), unit suffixes (`1s`, `3mb`,
  `20K`), and an expressive selector language.

### [REVISED] Module OnLoad

On load, the module registers the data type, the `TS.*` commands, `ts-`-prefixed configs,
and the `@timeseries` ACL category, then initializes subsystems in this order:
`init_croaring_allocator` → `register_config` → `assign_command_acl_categories` →
`register_server_event_handlers` → (if clustered) `init_fanout` +
`register_fanout_operations` → `init_thread_pool` → `init_background_tasks`.

* Module name: **`ts`** (visible via `HELLO`, `MODULE LIST`, `INFO`; prefixes metrics/configs)
* Data type name: **`TSDB-TYPE`** (encoding version **1**)
* Shared object: `libvalkey_timeseries.[so|dylib|dll]`
* Module framework: the `valkey-module` / `valkey-module-macros` crates (`valkey_module!`)
* Minimum Valkey server version: **8.0.0** (enforced in `preload()`; the load fails hard
  below it or if required blocking-module APIs for `TS.READ` are missing).

### Module Unload
`MODULE UNLOAD` is rejected because the module exports a data type — Valkey does not allow
unloading modules that export module-side data types.

### Persistence

The `TSDB-TYPE` data type implements: `rdb_save`/`rdb_load`, `aof_rewrite`, `free`,
`mem_usage`, `digest`, `aux_save`/`aux_load`, `unlink`, `copy`, and `defrag`.

* **RDB:** id, labels, retention, rounding, duplicate policy, encoding, chunk size, chunk
  count + each chunk (with a leading encoding-type byte), source id, and rules.
  **[REVISED]** Load **refuses any encoding version ≠ 1** — RTS reuses the same type name
  with a different layout, so foreign payloads fail cleanly rather than misparse.
* **AOF rewrite:** serializes via the type's own `rdb_save` (fork-safe) and emits
  `TS._RESTORE key <payload>`.
* **[REVISED] Index aux field:** the postings index is persisted as an RDB aux field
  (`ts-index-persist`, default on), with graceful fallback to rebuild-from-keyspace on load.

### RDB Format
The RDB format is specific to ValkeyTimeSeries and is **not** compatible with RTS or other
implementations.

### Migrating existing workloads
RDB/DUMP migration from other implementations is **not** supported (formats are
incompatible; the loader guards both directions so a cross-loaded payload fails cleanly and
creates no key). Two supported approaches:

1. **Recreate and repopulate** — `TS.CREATE`/`TS.CREATERULE` to recreate series and rules,
   then replay samples via `TS.ADD`/`TS.MADD`/`TS.ADDBULK`.
2. **AOF replay** — replay an AOF of creation/insertion commands against a server with the
   module loaded, then redirect the live workload.

### [REVISED] Memory Management

The module installs a **custom global allocator** (`AlignedValkeyAlloc`) that delegates to
Valkey's allocator (`ValkeyModule_Alloc`/`_Free`). It additionally fixes over-alignment:
Valkey's allocator rounds allocation size but not pointer alignment, which breaks
over-aligned types (e.g. 128-byte `CachePadded` used by the rayon thread pool on aarch64);
for `align > 16` the allocator over-allocates, rounds the pointer up, and stashes the
original pointer for `dealloc`. CRoaring is configured to use the same allocator.

Memory-management callbacks: `free`, `defrag` (trims then merges adjacent chunks into ones
with spare capacity), `mem_usage` (= `size_of::<Self>() + heap size`), `copy` (deep copy
with a fresh id and cleared linkage — a copy is an unlinked series), and `free_effort`.
Labels are **interned**: each `label=value` pair is a single reference-counted, self-cleaning
allocation shared across every series carrying it.

### Replication
Every write operation (creation, add, remove, alter, rule changes) is replicated to
replicas. **[REVISED]** The label index is **node-local**: each node (primary or replica)
maintains its own index, rebuilt from replicated key mutations, and is therefore subject to
replication lag like the keyspace.

## Specification

### TimeSeries Structure
Each series is an ordered list of chunks of samples; a sample is a `(i64 timestamp, f64
value)` tuple. A series is optionally identified by label-value pairs. Because labels group
semantically similar series, pairs are duplicated — so each unique `label=value` pair is
interned into a single allocation. The series object also holds retention, duplicate policy,
encoding, rounding, chunk size, compaction linkage (`src_series`), and outbound `rules`.

### [REVISED] TimeSeries Value Encoding
Chunks have a configurable encoding, defaulting to **Chimp (ELF-on-Chimp)**. Three encodings
are available:

| Encoding | Discriminant | Technique | Best for |
|---|---|---|---|
| **Uncompressed** | 1 | Raw 16 B `Vec<Sample>`, binary search | Random/high-entropy data, ingest-bound, short series, backfill |
| **Gorilla** | 2 | Delta-of-delta timestamps + XOR value coding | Constants/flags/discrete/counters; fastest **encode** |
| **Chimp (ELF-on-Chimp)** | 3 (**default**) | ELF mantissa erasure + Chimp XOR | Decimal-quantized/unknown data; fastest **decode** |

The `COMPRESSED` alias resolves to the current default (Chimp). Chimp has the higher
geometric-mean ratio (3.35 vs 3.03), decodes/scans faster, and round-trips descending
timestamps and NaN/±inf; Gorilla encodes faster and wins repetition-dominated workloads.
Selection rule 0: if values are reported to a fixed number of decimal places, choose Chimp.
Chunk size defaults to **4096 bytes** (range `[48, 1 MiB]`, multiple of 8) and chunks split
at 1.2× utilization.

### [REVISED] TimeSeries Indexing
A series is uniquely identified by an opaque unsigned 64-bit id. The inverted index maps each
`label=value` pair to a bitmap of series ids. It is implemented as an **Adaptive Radix Tree
(the `blart` crate)** whose values are **64-bit Roaring Bitmaps (the `croaring` crate)**. The
ART gives efficient lookups, insertions, prefix scans, and path compression; Roaring bitmaps
give fast set operations on ids. A separate `id → Valkey key` map (a `BTreeMap`, the
authoritative membership set) resolves query results back to keys. Ids are minted from an
`AtomicU64` packed as `[epoch:24 | counter:40]` — a random per-process epoch gives
probabilistic cluster-wide uniqueness while keeping ids dense so Roaring containers stay full.

### TimeSeries Indexing Scheme
For each unique `label`/`value` combination, the key `"{label}={value}"` (with a NUL
sentinel so no key is a prefix of another) maps to the Roaring bitmap of ids carrying it. To
resolve a filter, look up each term's bitmap and intersect. ART prefix queries find all
values of a label (e.g. prefix `"region="`). Query planning follows Prometheus's
`PostingsForMatchers`: partition into intersecting vs subtracting matchers, order cheap exact
lookups before scans/regex, short-circuit degenerate regexes, sort intersecting bitmaps by
cardinality, AND-fold, then `andnot` the subtractions. Stale ids are masked from reads and
physically drained by a background task.

### TimeSeries Filter Enhancements
Regex operators `=~` and `!~` are supported (fully anchored, Prometheus semantics, with
literal-prefix/suffix prefilters and alternation decomposition). Prometheus-style selectors
are supported:

* `TS.QUERYINDEX latency{region=~"us-west-*",service="inference"}`
* `TS.MRANGE -6h -3h FILTER {service="inference"}`
* OR matching: `latency{region="us-west" or region="us-east"}`

`metric{...}` matches `metric` against the reserved `__name__` label. Basic RTS-style filters
(`label=value`, `label!=value`, `label=(v1,v2)`, `label!=(v1,v2)`, presence/absence) are also
supported.

### TimeSeries Command API

The following commands are supported. **[REVISED]** commands new to this revision are marked.

#### TS.CREATE
```shell
TS.CREATE key [RETENTION retentionPeriod] [ENCODING <COMPRESSED|CHIMP|GORILLA|UNCOMPRESSED>]
  [CHUNK_SIZE chunkSize] [DUPLICATE_POLICY policy] [DEDUPE_INTERVAL duplicateTimediff]
  [LABELS [label value ...] | METRIC metricName]
```
Create a new time series. `ENCODING` default `COMPRESSED` (→ Chimp). `DUPLICATE_POLICY`
default `BLOCK`. `LABELS` and `METRIC` are mutually exclusive; `METRIC` uses Prometheus
semantics and adds a `__name__` label. `chunkSize` default 4096 bytes.

#### TS.ALTER
```shell
TS.ALTER key [RETENTION retentionPeriod] [CHUNK_SIZE chunkSize] [DUPLICATE_POLICY policy]
  [DEDUPE_INTERVAL duplicateTimediff] [LABELS [label value ...] | METRIC metricName]
```
Change series properties.

#### TS.ADD / TS.MADD
```shell
TS.ADD key timestamp value
TS.MADD key timestamp value [key timestamp value ...]
```
Append a sample (creating the key if absent). Returns the added timestamp. `TS.MADD` is
single-slot in cluster mode.

#### TS.INCRBY / TS.DECRBY
```shell
TS.INCRBY key delta [TIMESTAMP timestamp] [RETENTION ...] [ENCODING ...] [CHUNK_SIZE ...]
  [DUPLICATE_POLICY ...] [DEDUPE_INTERVAL ...] [LABELS ... | METRIC ...]
```
Increment/decrement the value of the latest sample (creating the series if absent).

#### TS.ADDBULK — **[extension]**
```shell
TS.ADDBULK key data [RETENTION ...] [DUPLICATE_POLICY ...] [ON_DUPLICATE ...] [ENCODING ...]
  [CHUNK_SIZE ...] [METRIC ... | LABELS ...] [IGNORE maxTimediff maxValDiff]
  [SIGNIFICANT_DIGITS n | DECIMAL_DIGITS n]
```
Ingest up to 1000 samples from a JSON payload `{"values":[...],"timestamps":[...]}` (equal
lengths, ≥1 sample). Input is sorted by timestamp; retention filtering happens before chunk
grouping. Returns `[ingested_count, payload_count]`.

#### TS.DEL / TS.MDEL
```shell
TS.DEL key fromTimestamp toTimestamp
TS.MDEL [fromTimestamp toTimestamp] FILTER selector...
```
`TS.DEL` deletes samples in an inclusive range from one series (returns count deleted).
`TS.MDEL` — **[extension]** — deletes samples in a range across matching series, or whole
series when no range is given. Enforces DELETE ACL permission on matched keys; emits `ts.del`
(range) or `del` (series) events.

#### TS.CREATERULE / TS.DELETERULE
```shell
TS.CREATERULE sourceKey destKey AGGREGATION aggregator bucketDuration [alignTimestamp]
TS.DELETERULE sourceKey destKey
```
Manage compaction (downsampling) rules. In cluster mode, source and destination must share a
hash slot. Cascading rules are supported (topo-sorted with circular-dependency detection).

#### TS.GET / TS.MGET
```shell
TS.GET key [LATEST]
TS.MGET [LATEST] [WITHLABELS | SELECTED_LABELS label...] FILTER filterExpr...
```
Get the last sample of one/many series. `LATEST` returns the value of the still-open bucket
for a compaction destination.

#### TS.RANGE / TS.REVRANGE
```shell
TS.RANGE key fromTimestamp toTimestamp [LATEST] [FILTER_BY_TS ts...] [FILTER_BY_VALUE min max]
  [COUNT count] [AGGREGATION aggregator bucketDuration [CONDITION op value]
  [ALIGN align] [BUCKETTIMESTAMP bt] [EMPTY]]
```
Query a range from one series (`TS.REVRANGE` returns descending). Timestamps accept numeric
ms, `-`/`+`/`*`, or duration specs (`2h`). `ALIGN` = `start`/`end`/timestamp;
`BUCKETTIMESTAMP` = `start`(default)/`mid`/`end`.

#### TS.MRANGE / TS.MREVRANGE
```shell
TS.MRANGE fromTimestamp toTimestamp [LATEST] [FILTER_BY_TS ts...] [FILTER_BY_VALUE min max]
  [COUNT count] [WITHLABELS | SELECTED_LABELS label...]
  [AGGREGATION aggregator bucketDuration [CONDITION op value] [ALIGN align] [BUCKETTIMESTAMP bt] [EMPTY]]
  [GROUPBY label REDUCE reducer] [EXCLUDEEMPTY]
  FILTER filterExpr...
```
Query ranges across many series. In cluster mode this fans out across shards (see
[Cluster Mode](#revised-cluster-mode)). `GROUPBY`/`REDUCE` groups matching series by a label
and reduces per bucket. At least one `FILTER` selector is required.

#### TS.NRANGE / TS.NREVRANGE — **[REVISED / extension]**
```shell
TS.NRANGE key fromTimestamp toTimestamp [ ... same options as TS.RANGE, multi-aggregation ]
```
Range query returning **multiple aggregation columns per bucket** in one pass
(e.g. `AGGREGATION avg,max,count ...`), yielding a row of values per bucket.

#### TS.READ — **[REVISED / extension]**
```shell
TS.READ key fromTimestamp [COUNT n] [BLOCK ms]
```
Read samples at/after a timestamp, optionally **blocking** until enough new samples arrive —
the streaming counterpart to `TS.RANGE` for tailing a series. Declares `RO` + `ACCESS` key
flags (an intentional metadata difference from RTS, semantically correct for a
data-returning read). Uses Valkey's block-on-keys API; a write signals readiness.

#### TS.QUERYINDEX
```shell
TS.QUERYINDEX [FILTER_BY_RANGE [NOT] fromTimestamp toTimestamp] filterExpression...
```
Return series keys matching the filter(s). `FILTER_BY_RANGE` restricts to series with (or,
with `NOT`, without) samples in the range.

#### TS.QUERYLABELS — **[REVISED / extension]**
```shell
TS.QUERYLABELS [label] [SEARCH term...] [FUZZY_THRESHOLD t] [FUZZY_ALGORITHM jarowinkler|subsequence]
  [IGNORE_CASE] [INCLUDE_METADATA] [SORTBY value|score|cardinality [ASC|DESC]]
  [FILTER_BY_RANGE [NOT] fromTimestamp toTimestamp] [LIMIT limit] FILTER selector...
```
Return label names, or values of a given label, for matching series. **Silently omits series
the caller lacks read access to** (so private label names/values never leak) — unlike
`TS.QUERYINDEX`, which reveals matching keys regardless.

#### TS.CARD
```shell
TS.CARD [FILTER_BY_RANGE [NOT] fromTimestamp toTimestamp] FILTER filter...
```
Number of unique series matching the filter(s).

#### TS.LABELNAMES / TS.LABELVALUES / TS.METRICNAMES
```shell
TS.LABELNAMES   [SEARCH ...][FUZZY_*][IGNORE_CASE][INCLUDE_METADATA][SORTBY ...][FILTER_BY_RANGE ...][LIMIT n] FILTER selector...
TS.LABELVALUES label [ ... same options ... ] FILTER selector...
TS.METRICNAMES  [ ... same options ... ]        # searches the __name__ label
```
Server-side label/metric discovery with fuzzy matching (Jaro-Winkler / subsequence),
relevance sorting, cardinality metadata, and cluster-aware fan-out. **[REVISED]**
`TS.METRICNAMES` is new; discovery options (`SEARCH`, `FUZZY_*`, `SORTBY`, `INCLUDE_METADATA`)
are documented in the label-discovery topic.

#### TS.LABELSTATS / TS.STATS — **[REVISED / extension]**
```shell
TS.LABELSTATS [LIMIT limit]
```
Cardinality/memory statistics about the index: `numSeries`, `numLabelPairs`,
`seriesCountByMetricName`, `labelValueCountByLabelName`, `memoryInBytesByLabelName`,
`seriesCountByLabelValuePair`. (Referred to as `TS.STATS` in the original RFC; the
implemented command is `TS.LABELSTATS`.)

#### TS.INFO
```shell
TS.INFO key [DEBUG]
```
Per-series metadata and statistics. **[REVISED]** The field set is ValkeyTimeSeries-specific
and does not match RTS name-for-name.

#### TS.JOIN — **[extension]**
```shell
TS.JOIN leftKey rightKey fromTimestamp toTimestamp
  [INNER | FULL | LEFT | RIGHT | ANTI | SEMI | ASOF [PREVIOUS|NEXT|NEAREST] [tolerance] [ALLOW_EXACT_MATCH]]
  [FILTER_BY_TS ts...] [FILTER_BY_VALUE min max] [COUNT count] [REDUCE reducer]
  [AGGREGATION aggregator bucketDuration [CONDITION op value] [ALIGN align] [BUCKETTIMESTAMP bt] [EMPTY]]
```
Join two series on sample timestamps (INNER by default). ASOF matches each left sample to the
closest right sample by direction (`PREVIOUS`/`NEXT`/`NEAREST`) within an optional tolerance,
with `ALLOW_EXACT_MATCH`. `REDUCE` combines the two sides per row (`sub`, `div`, `mul`, `pow`,
`mod`, `abs_diff`, `coalesce`, `pct_change`, `sgn_diff`, comparisons, `min`/`max`/`avg`/`sum`).
`REDUCE` is rejected for SEMI/ANTI. Implemented over `joinkit` merge-joins; ASOF ported from
Polars.

#### TS.OUTLIERS — **[extension]**
```shell
TS.OUTLIERS key fromTimestamp toTimestamp [OUTPUT SIMPLE|FULL|CLEANED]
  [DIRECTION BOTH|POSITIVE|NEGATIVE] [SEASONALITY AUTO | period...] METHOD method [method-options]
```
Detect outliers. Methods: `CUSUM`, `EWMA [ALPHA]`, `IQR [THRESHOLD]`, `ZSCORE [THRESHOLD]`,
`MODIFIED-ZSCORE [THRESHOLD]`, `SMOOTHED-ZSCORE [THRESHOLD] [LAG] [INFLUENCE]`,
`MAD`/`DOUBLE-MAD [ESTIMATOR] [THRESHOLD]`, and `RCF [NUM_TREES] [SAMPLE_SIZE] [THRESHOLD]
[SHINGLE_SIZE] [OUTPUT_AFTER] [DECAY]` (Random Cut Forest via the `krcf` crate). ESD is also
implemented. **[REVISED]** All methods share a normalization contract where **0.5 is always
the decision boundary** and scores are comparable across methods. `SEASONALITY` runs STL
(single period) or MSTL (multiple periods) and detects on the remainder; `AUTO` uses a Welch
periodogram to find dominant periods (requires `n ≥ 2 × max(period)`).

#### Supported Aggregators

**Simple:** `avg`, `sum`, `count`, `min`, `max`, `range`, `first`, `last`
**Statistical:** `std.p`, `std.s`, `var.p`, `var.s` (Welford's algorithm)
**Counter/rate:** `increase`, `rate`, `irate`, plus `count_all`, `count_nan`
**Filtered/conditional:** `countif`, `sumif`, `share`, `all`, `any`, `none`

`twa` (time-weighted average) is **not** supported.

### [REVISED] Numerical Stability (value divergence from RTS)

Sums use **Neumaier compensated summation** and variance uses **Welford's algorithm**, so
ValkeyTimeSeries returns the mathematically correct result where RTS's `E[x²] − E[x]²`
identity or naive left-to-right summation returns `NaN`, an incorrectly-cancelled `0`, or a
value wrong in its leading digits. This affects `std.*`/`var.*` (large-magnitude or
tightly-clustered samples) and `sum`/`avg` (buckets mixing magnitudes). It is a **documented
value divergence** from RTS and is **not** gated by strict mode (there is no second
*intended* behavior — RTS's result is simply wrong).

### [REVISED] Cluster Mode

Keys are placed by Valkey's normal hash-slot algorithm; the module does not alter placement.
Because `TS.*` multi-series commands operate at the index level, the module performs its own
intra-cluster RPC so the application interface is identical in cluster and standalone mode.

* **Transport:** Valkey's native cluster bus (`ValkeyModule_SendClusterMessage`), with no
  separate coordinator, port, or shared password (RTS's `OSS_GLOBAL_PASSWORD` is obsolete).
* **Codec:** protobuf (`prost`; schemas in `proto/v1/`), with a per-response symbol table
  that dictionary-interns repeated label names/values.
* **Cluster map:** built from `CLUSTER NODES` into a slot→shard map over 16384 slots, held in
  an `ArcSwap` with adaptive refresh backoff; requests carry a topology fingerprint and are
  rejected on mismatch (coordinator marks the map stale and fails fast).
* **Async:** the client is blocked; a reply callback runs on the main thread once all shards
  respond. **Any per-shard permission denial aborts the whole fan-out** (a data-returning
  command must not silently drop keys). ACL keyspace restrictions are enforced on the result.
* **Timeout:** each fan-out is bounded by `ts-fanout-command-timeout`; a consistent
  cluster-wide view that cannot be established in time returns an error rather than a partial
  result.
* **Push-down** (`ts-fanout-aggregation-pushdown`, default on): shards return aggregated
  buckets, not raw samples; decomposable `GROUPBY`/`REDUCE` reducers ship partial states
  merged coordinator-side; `COUNT` is applied both shard-side and coordinator-side. Order-
  sensitive reducers (`increase`, `irate`) fall back to per-series bucket transport. Mixed-
  version clusters self-compensate via a compatibility handshake.

Fan-out operations registered: `mrange`, `mrevrange`, `queryindex`, `querylabels`, `mget`,
`mdel`, `card`, label-stats, label-search.

### [REVISED] Threading Model

A global `rayon` thread pool is sized by `ts-num-threads` (default 4, range 1–16) and is
**immutable** (rayon's global pool can't be resized). Query processing parallelizes over it
via `orx_parallel`. Main-thread work is scheduled through the event loop; a batch worker
serializes keyspace access without deadlock; the cluster map, in-flight requests, and per-db
indexes use lock-free structures (`arc-swap`, `papaya`).

### [REVISED] Configurations

Configurables use Valkey's native `CONFIG GET`/`CONFIG SET` under `ts-` prefixes (a
divergence from RTS's `MODULE LOAD` arguments). A rejected startup value fails module load.

| Config | Default | Notes |
|---|---|---|
| `ts-chunk-size` | 4096 | `[48, 1 MiB]`, multiple of 8 |
| `ts-encoding` | `CHIMP` (alias `COMPRESSED`) | or `GORILLA`, `UNCOMPRESSED` |
| `ts-duplicate-policy` | `BLOCK` | block/first/last/min/max/sum |
| `ts-retention-policy` | 0 (no expiry) | ms; max 10 years |
| `ts-compaction-policy` | "" | default rules with per-key filters |
| `ts-compatibility-mode` | `extended` | `strict` reproduces RTS resolution on gated value divergences |
| `ts-decimal-digits` / `ts-significant-digits` | — | mutually exclusive rounding |
| `ts-ignore-max-time-diff` / `-val-diff` | 0 | IGNORE dedup thresholds |
| `ts-num-threads` | 4 | `[1,16]`, **immutable** |
| `ts-fanout-command-timeout` | 5000 ms | `[500, 10000]` |
| `ts-cluster-map-expiration-ms` | 750 | `[0, 3.6M]` |
| `ts-index-build-max-memory` | 256 MiB | bulk-index cap during load |
| `ts-fanout-aggregation-pushdown` | yes | runtime escape hatch |
| `ts-index-persist` | yes | persist postings index as RDB aux |
| `ts-emulate-release` | current major | SemVer-safe compatibility-bug opt-in |

### [REVISED] Compatibility Mode

`ts-compatibility-mode` closes the narrow, uniquely dangerous class where **both engines
accept the command and both return a value, but the values differ silently** — e.g. repeated-
option resolution (last-wins in `extended`, first-wins in `strict`, matching RTS) and a
back-filled compaction destination's `TS.GET` last-sample. It does **not** restrict the
additive surface (relative range bounds, cascading rules, complement filters, bare metric-name
selectors, the extension commands) nor relax stricter input validation, and it does **not**
gate the numerically-correct arithmetic divergences above.

### ACL
The module introduces the `@timeseries` ACL category and updates `@read`, `@write`, `@fast`,
`@slow` to include the relevant `TS.*` commands.

### Keyspace Event Notification
Every mutating command publishes a keyspace event after mutation (type
`VALKEYMODULE_NOTIFY_GENERIC`). Event names include `ts.add`, `ts.alter`, `ts.add:dest`,
`ts.create`, `ts.createrule:src`, `ts.createrule:dest`, `ts.del`, `ts.madd`. Subscribe via
standard keyspace pub/sub (`notify-keyspace-events KEA`, `psubscribe '__key*__:*'`).

### Notable Behaviors
* The parser is lenient — option order does not matter for `TS.CREATE`/`TS.ALTER` outside
  variadic arguments.
* Prometheus-style selectors work in `TS.QUERYINDEX`, `TS.QUERYLABELS`, `TS.MGET`,
  `TS.MRANGE`, `TS.MREVRANGE`, `TS.MDEL`.
* Index metadata commands: `TS.CARD`, `TS.LABELNAMES`, `TS.LABELVALUES`, `TS.METRICNAMES`,
  `TS.LABELSTATS`.
* Extended aggregators (`all`, `any`, `none`, `countif`, `sumif`, `share`, `increase`,
  `rate`, `irate`) and `TS.JOIN` are supported.
* Retention is trimmed eagerly on write; `TS.INFO` therefore agrees with `TS.RANGE`.

### Unsupported Features
`twa` (time-weighted average) is not supported.

### Possible Future Enhancements
* **PromQL** query language (transform/aggregation/rollup functions), possibly with
  alerting/notifications.
* **Tiered storage** — move cold data to higher-compression, higher-latency chunks after an
  age threshold.
* **Advanced analysis** — forecasting (`TS.FORECAST`/`TS.AUTOFORECAST`), decomposition
  (`TS.DECOMPOSE`/`TS.TREND`), correlation (`TS.XCORR`, `TS.AUTOCORRELATION`),
  stationarity/periodicity, feature extraction, gap filling. (Named in the compatibility
  contract's roadmap; not yet in the registered command table.)

## References
* [valkey-timeseries GitHub repo](https://github.com/opensource-for-valkey/valkey-timeseries)
* [Prometheus](https://prometheus.io/)
* [Adaptive Radix Tree (ART)](https://db.in.tum.de/~leis/papers/ART.pdf)
* [Roaring Bitmaps](https://roaringbitmap.org/about/)
* Pelkonen et al., "Gorilla," PVLDB 2015 · Liakos et al., "Chimp," PVLDB 2022 · Li et al.,
  "ELF," PVLDB 2023
