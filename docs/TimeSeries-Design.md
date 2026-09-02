# ValkeyTimeSeries Module — Design Document

**Status:** Draft · **Type:** Design / Architecture · **Language:** Rust (edition 2024, MSRV 1.92)
**Reference RFC:** ValkeyTimeSeries Module RFC #4 (Proposed)
**Reference implementation:** `github.com/opensource-for-valkey/valkey-timeseries` (Apache-2.0)

> This document is grounded in the current implementation. Where the code has moved
> ahead of the RFC, the code is treated as the source of truth and the divergence is
> called out inline (see [§13](#13-notable-divergences-from-the-rfc)).

---

## 1. Abstract

ValkeyTimeSeries is a Rust module that adds a native, in-memory **time series data type**
to Valkey. It targets high write throughput, aggressive compression, and real-time
analytics for observability, IoT, and financial workloads. Its command surface is a
**superset of RedisTimeSeries (RTS)** — existing RTS client code runs against it with
minimal change — while adding joins, outlier detection, richer label/metric discovery,
bulk ingest, and Prometheus-style selectors.

The module registers a `TSDB-TYPE` data type, the `TS.*` command family, `ts-`-prefixed
Valkey configs, and a `@timeseries` ACL category. It runs in both standalone and cluster
mode, fanning multi-series queries out over Valkey's native cluster bus.

---

## 2. Goals and Non-Goals

### Goals
- A purpose-built time series type that beats native `stream`/`zset` modeling on memory,
  throughput, and ergonomics.
- Sustained high write throughput by staying **in-memory** and avoiding the write
  amplification (WAL, LSM compaction, page splits) that dominates disk-based TSDBs.
- Best-in-class compression via floating-point-aware chunk codecs.
- Real-time, query-time aggregation, downsampling (compaction rules), joins, and anomaly
  detection with no client-side post-processing.
- Label-based secondary indexing with Prometheus-style selectors and cluster-transparent
  fan-out.
- RTS command/query/data-model compatibility as an adoption on-ramp.

### Non-Goals
- **On-disk / wire-format compatibility with RTS.** RDB, AOF, and replication byte formats
  are internal. RTS RDB files cannot be loaded and vice versa; migration is command-level
  (export/re-ingest or AOF replay). Both modules register the same `TSDB-TYPE` name, so the
  loader guards against cross-loading with an explicit encoding-version check.
- **Durability beyond Valkey's own primitives.** Persistence rides Valkey RDB/AOF; there is
  no per-write journal.
- **Working sets that exceed memory.** The hot window must fit in RAM; retention and
  compaction manage capacity. (A tiered-storage model is possible future work.)
- **RTS internal parity** (threading model, chunk layout, index structures, `INFO` metric
  names, error text, log messages) — all explicitly out of scope.

---

## 3. Background: the write-amplification thesis

Disk-based TSDBs (TimescaleDB, InfluxDB, Prometheus, ClickHouse, OpenSearch) multiply each
ingested sample into 10–50× physical I/O through index updates, WAL writes, flushes, and
LSM/SSTable merges. ValkeyTimeSeries sidesteps this class of problem entirely:

- **No per-write WAL/journal** — samples append directly to the active in-memory chunk;
  durability is amortized across all data types via Valkey RDB/AOF.
- **No LSM compaction storms** — no SSTables, no background merge threads, no compaction
  tail-latency spikes.
- **Append-only chunk model** — a chunk is a contiguous compressed buffer that grows until
  full, then seals. No in-place mutation, no page splits, no B-tree rebalancing on the hot
  path.
- **Metadata-only index writes** — the label index updates a few pointers only when a *new
  series* is created, not per sample.
- **Logical, not physical compaction** — downsampling aggregates into destination series;
  it never rewrites source data in place.

The trade-off is capacity: the working set must fit in memory. This is the kdb+ thesis —
keep the hot path in memory for lowest latency and highest throughput — applied inside the
Valkey ecosystem and reachable through standard clients.

---

## 4. High-Level Architecture

```
                          ┌─────────────────────────────────────────────────┐
                          │  Valkey server (single node or cluster shard)   │
                          └─────────────────────────────────────────────────┘
   client ── TS.* ──▶  Command dispatch (src/commands/*, valkey_module! in src/lib.rs)
                              │
              ┌───────────────┼───────────────────────────────┐
              ▼               ▼                                ▼
      Parser (src/parser)   Storage engine (src/series)   Cluster fanout (src/fanout)
      selectors/durations/  ├─ TimeSeries + chunks         ├─ ClusterMap (CLUSTER NODES)
      timestamps/numbers    ├─ encodings (Chimp/Gorilla/   ├─ protobuf RPC over cluster bus
                            │   Uncompressed)              ├─ scatter/gather + push-down
                            ├─ compaction / retention      └─ ACL-scoped, timeout-bounded
                            ├─ background tasks
                            └─ label index (ART + Roaring)
              ▼
      Query pipeline (src/iterators, src/aggregators, src/join, src/analysis)
      lazy iterator stack → filters → aggregation/grouping → joins → outliers/seasonality
```

Subsystem-to-directory map:

| Subsystem | Directory | Responsibility |
|---|---|---|
| Module lifecycle | `src/lib.rs`, `src/config.rs` | Load/init, command + type + config + ACL registration |
| Storage engine | `src/series/` | `TimeSeries`, chunks, encodings, compaction, retention, RDB |
| Chunk codecs | `src/series/chunks/{uncompressed,gorilla,chimp}` | Sample encoding/decoding |
| Label index | `src/series/index/`, `src/labels/` | ART + Roaring inverted index, selectors |
| Query pipeline | `src/iterators/` | Lazy sample/row iterator stack |
| Aggregation | `src/aggregators/` | 23 aggregators, bucketing, alignment, push-down |
| Joins | `src/join/` | INNER/OUTER/SEMI/ANTI/ASOF, reducers |
| Analysis | `src/analysis/` | Outlier detectors, STL/MSTL seasonality, quantiles |
| Cluster | `src/fanout/`, `src/commands/*_fanout_command.rs`, `proto/v1/` | Cross-shard RPC |
| Parsing | `src/parser/` | Prometheus selectors, durations, timestamps, numbers |
| Shared | `src/common/` | Allocator, thread pool, interning, RDB helpers |

Key third-party crates: `valkey-module`/`valkey-module-macros` (module framework),
`blart` (adaptive radix tree), `croaring` (64-bit Roaring bitmaps), `prost`/`protox`
(protobuf), `rayon-core`/`orx-parallel` (parallelism), `papaya`/`arc-swap` (lock-free
maps/pointers), `krcf` (Random Cut Forest), `welch-sde`/`statrs` (analysis), `logos`
(lexer), `regex`, `speedate` (RFC3339), `get-size2` (memory accounting).

---

## 5. Data Model and Storage Engine

### 5.1 The `TimeSeries` record

The core in-memory record is `struct TimeSeries` (`src/series/time_series.rs`), split into
configuration, data, and runtime-only bookkeeping:

```rust
pub struct TimeSeries {
    pub id: SeriesRef,                          // opaque u64, minted by next_timeseries_id()
    pub labels: MetricName,                     // interned label set (Prometheus rendering)
    pub retention: Duration,                    // auto-expiry window (0 = disabled)
    pub sample_duplicates: SampleDuplicatePolicy,// duplicate policy + IGNORE thresholds
    pub chunk_encoding: ChunkEncoding,          // Uncompressed=1 | Gorilla=2 | Chimp=3
    pub rounding: Option<RoundingStrategy>,     // significant/decimal-digit quantization
    pub chunk_size_bytes: usize,                // target chunk size (default 4096)
    pub chunks: Vec<TimeSeriesChunk>,           // ordered, non-overlapping by first timestamp
    pub total_samples: usize,                   // cached metadata
    pub first_timestamp: Timestamp,
    pub last_sample: Option<Sample>,
    pub src_series: Option<TimeseriesId>,       // set => this is a compaction destination
    pub rules: Vec<CompactionRule>,             // outbound compaction rules
    pub(crate) _db: Option<i32>,                // owning db (runtime only, not serialized)
    pub(crate) last_forward_close: Option<Sample>, // strict-compat marker (DIV-0023)
}
```

A `Sample` is a `(i64 timestamp, f64 value)` pair. Labels are stored **interned** (see
[§7.4](#74-string-interning)). Metadata (`total_samples`, `first_timestamp`, `last_sample`)
is kept consistent by helpers (`record_appended_sample`, `update_first_last_timestamps`)
so `TS.INFO` never needs to rescan.

### 5.2 Chunk model

`chunks` is a `Vec` kept **sorted ascending and non-overlapping** by first timestamp.
Lookup is hybrid: a linear scan up to `LINEAR_SCAN_MAX = 16` chunks, else binary search
(`find_start_chunk_index`, `get_chunk_index_bounds`).

`TimeSeriesChunk` is an `enum { Uncompressed, Gorilla, Chimp }` dispatched via
`enum_dispatch` over the `ChunkOps` trait (`src/series/chunks/chunk.rs`), which covers
`add_sample`, `upsert_sample`, `merge_samples`, `get_range`, `remove_range`, `is_full`,
`bytes_per_sample`, `set_data`, etc. The extended `Chunk` trait adds `split`, RDB
save/load, serialize/deserialize, and `debug_digest`.

- **Chunk size** is validated to `[48, 1 MiB]` and a multiple of 8 (bit-packed encoder
  word alignment). Default 4096 bytes.
- **Sealing:** uncompressed chunks are full at `chunk_size / 16` samples
  (`MAX_UNCOMPRESSED_SAMPLES = 256`); compressed chunks are full when the encoder buffer
  reaches `max_size`.
- **Splitting:** `should_split()` fires at `utilization() >= SPLIT_FACTOR (1.2)`. `split()`
  divides at the midpoint; uncompressed uses `Vec::split_at`, compressed re-encodes both
  halves from an iterator (the codecs are sequential). Full-chunk splitting is done in
  parallel across chunks via `par_mut()`.

### 5.3 Encodings

Three encodings (`src/series/chunks/`, documented in `docs/topics/encodings.md`):

| Encoding | Discriminant | Technique | Best for |
|---|---|---|---|
| **Uncompressed** | 1 | Raw 16 B `Vec<Sample>`, binary search | Random/high-entropy data, ingest-bound, short series, backfill |
| **Gorilla** | 2 | Delta-of-delta timestamps + XOR value coding | Constants/flags/discrete/counters; fastest **encode** |
| **Chimp (ELF-on-Chimp)** | 3 (**default**) | ELF mantissa erasure + Chimp XOR | Decimal-quantized/unknown data; fastest **decode** |

Highlights:
- **Gorilla** collapses a regular interval to 1 bit and a repeated value to 1 bit (2 bits
  total → 64× on constants). Refuses descending timestamps.
- **Chimp** runs an ELF erasure layer that strips reversible low-mantissa bits from
  decimally-quantized values (a `12.34` stored as `12.3399999…` erases to ~1.5 B vs
  Gorilla's ~5–6 B), then Chimp XOR-compresses with 2-bit fixed-width flags. Zig-zagged
  signed delta-of-delta timestamps mean **descending timestamps round-trip**; NaN/±inf
  bypass ELF and survive bit-exact (important for Prometheus stale markers).
- Chimp is the default: higher geometric-mean ratio (3.35 vs 3.03), shallower worst case,
  and cheaper on all read paths — at the cost of ~1.67× slower encode. Choose Gorilla when
  ingest is the binding constraint or data is repetition-dominated.

**Selection rule 0:** if values are reported to a fixed small number of decimal places,
choose Chimp — it's worth up to ~3× and dominates every other factor.

### 5.4 Ingestion path

`TimeSeries::add(ts, value, dp_override)` (`src/series/time_series.rs`):

1. `make_sample()` applies rounding up front.
2. Retention gate first (RTS parity): `is_older_than_retention` → `TooOld`.
3. If `ts >= last_ts`, run the IGNORE duplicate filter → `Ignored(last_ts)` if within
   `max_time_delta`/`max_value_delta`.
4. `ts <= last_ts` → `upsert_sample()` (locate chunk, split if needed, re-encode/insert);
   `ts > last_ts` → append to the last chunk, allocating a new chunk when full.

`SampleAddResult` = `Ok(Sample) | Duplicate | Ignored(ts) | TooOld | Error(&str)`.

**Duplicate policy** (`DuplicatePolicy`): `Block(default) | KeepFirst | KeepLast | Min |
Max | Sum`, with NaN-aware handling for Min/Max/Sum. **IGNORE** (deduplication) treats a
sample as duplicate only when `0 <= Δt <= max_time_delta` and `|Δv| <= max_value_delta`.

**Batch ingest** (`TS.ADDBULK`, `TS.MADD`) flows through `ingest_normalize.rs::normalize_batch`:
in-batch duplicate timestamps rejected, retention pre-applied in input order, rounding
applied, IGNORE checked against a running-last sample; then `chunks/merge.rs` merges
existing + incoming under the policy producing one result per unique timestamp.

### 5.5 Retention and active expiration

`get_min_timestamp() = last_sample.ts - retention` (clamped ≥ 0). `apply_retention()` →
`trim()`: drop whole expired chunks, then partially trim the boundary chunk via
`remove_range`. Trimming is **eager** on add/merge so `TS.INFO` agrees with `TS.RANGE`
(a deliberate divergence from RTS's lazy-trim model, DIV-0021). A background task
(`TaskType::TrimSeries`, every 10s) sweeps series independently of writes.

### 5.6 Compaction and downsampling

A `CompactionRule` (`src/series/compaction.rs`) holds `{ dest_id, aggregator,
bucket_duration, align_timestamp, bucket_start, has_samples }`. Bucket alignment:
`calc_bucket_start(ts, align, dur)`. Source→dest flow (`apply_compaction` →
`process_series_with_compaction`) streams appends through the open destination bucket,
closing on forward progress and publishing via `add_dest_bucket` (destination writes use
`KeepLast`). Back-fills are detected by replaying MADD input order against a running max
and recalculated once from the source. Cascading rules (a destination with its own rules)
are topo-sorted with circular-dependency detection.

Default compaction policies can be attached by config with per-key filter expressions:
`CONFIG SET ts-compaction-policy "avg:2h:10d|^metrics:memory:*;sum:60s:1h:5s|^metrics:cpu:*"`.

### 5.7 Persistence, defrag, copy, memory

The `TSDB-TYPE` data type (`src/series/series_data_type.rs`, encoding version 1) wires:
`rdb_save`/`rdb_load`, `aof_rewrite`, `free`, `mem_usage`, `digest`, `aux_load`/`aux_save2`,
`unlink`, `copy`, `defrag`.

- **RDB:** id, labels, retention, rounding, duplicate policy, encoding, chunk_size, chunk
  count + each chunk (with a leading encoding-type byte), src_id, and rules. **Load refuses
  any `enc_ver != 1`** — RTS reuses the same type name with a different layout, so foreign
  payloads fail cleanly rather than misparse.
- **AOF rewrite** serializes via the type's own `rdb_save` (fork-safe) and emits
  `TS._RESTORE key <payload>`.
- **Index aux field:** the postings index is persisted as an RDB aux field
  (`ts-index-persist`, default on), with graceful fallback to rebuild-from-keyspace.
- **`copy`** clones on the main thread, assigns a fresh id, and clears `_db`, `src_series`,
  and `rules` (a copy is a fresh, unlinked series).
- **`defrag`** trims then merges adjacent chunks into earlier ones with spare capacity.
- **`mem_usage`** = `size_of::<Self>() + get_heap_size()` (via `get-size2`).

---

## 6. Ingestion & Write Replication Summary

Every mutating command (`TS.CREATE`, `TS.ADD`, `TS.MADD`, `TS.INCRBY`/`TS.DECRBY`,
`TS.DEL`, `TS.CREATERULE`, `TS.ALTER`, …) is flagged `write deny-oom`, replicated to
replicas, and emits a keyspace/module notification (`ts.add`, `ts.create`,
`ts.createrule:src`/`:dest`, `ts.del`, `ts.madd`, `ts.alter`, …). The label index is
**node-local**: replicas rebuild their own index from the replicated key mutations, so the
index is subject to replication lag just like the keyspace.

---

## 7. Label Indexing Subsystem

### 7.1 ART + Roaring bitmap inverted index

The inverted index (`src/series/index/postings/mod.rs`) is:

```rust
pub struct Postings {
    label_index: TreeMap<IndexKey, Bitmap64>, // blart ART: "label=value" -> series ids
    id_to_key:   BTreeMap<SeriesRef, Box<[u8]>>, // authoritative membership (id -> Valkey key)
    stale_ids:   StaleSet,                     // tombstones
    all_postings: Bitmap64,                    // every live id (for negative queries)
}
```

- **`blart` (Adaptive Radix Tree)** is the term dictionary. It supports prefix scans
  natively — so `region=` finds all values of `region`, and prefix compression saves memory
  given the `label=value` key scheme.
- **`croaring` `Bitmap64`** backs every posting list. CRoaring is configured to use
  Valkey's allocator (`init_croaring_allocator` → `configure_rust_alloc`).
- One `TimeSeriesIndex` (a `RwLock<Postings>`) per Valkey db, held in a
  `papaya::HashMap<i32, TimeSeriesIndex>`.

### 7.2 Indexing scheme

Each unique `label=value` pair becomes an `IndexKey` (`Box<[u8]>` of `label=value` + a NUL
sentinel so no key is a prefix of another, satisfying ART's `NoPrefixesBytes`). The ART maps
the term to the bitmap of series ids that carry it. To resolve a filter, look up each term's
bitmap and intersect. `id_to_key` (a `BTreeMap`) is the authoritative membership set (a
label-less series still has a key). Transient lookup keys use a stack-or-heap `KeyBuffer`
(64-byte stack budget) to avoid allocation.

### 7.3 Series id allocation

`IdGenerator` (`src/series/index/ids.rs`) is a single `AtomicU64` packed as
`[epoch:24 | counter:40]`. `next_id()` is a wait-free `fetch_add(1)`; the epoch is minted
once per process (random, non-zero) for probabilistic cluster-wide uniqueness while keeping
ids dense in the low bits so Roaring containers stay full. Collisions are resolved only at
slot import.

### 7.4 String interning

`InternedString` (`src/common/string_interner.rs`) holds one `Arc<[u8]>` per unique value in
a global pool; equality/hash are pointer-based. It is reference-counted and self-cleaning
(drops from the pool at the last external reference). `MetricName` stores each label as a
single interned `"key=value"` string (8 B/pair vs 16 B for two strings) — and because
semantically similar series share pairs, only one allocation exists per unique pair
regardless of how many series carry it. `TS.LABELSTATS`/`TS.STATS` report the memory saved.

### 7.5 Filter evaluation and query planning

Selectors are `SeriesSelector::And(FilterList)` or `Or(...)`; a `LabelFilter` carries a
`PredicateMatch` (`Equal`, `NotEqual`, `MatchAll/None`, `RegexEqual/NotEqual`,
`StartsWith`, `Contains`, …). The planner (`postings/planner.rs`) follows Prometheus's
`PostingsForMatchers`:

- Partition matchers into **intersecting** and **subtracting** (negative or empty-matching)
  sets; if only subtracting matchers exist, seed from `all_postings`.
- Sort intersecting first, then by `cost()` (cheap exact lookups before scans/regex).
- Short-circuit degenerate regexes (`=~".*"` matches all, etc.) before touching the index.
- Sort intersecting bitmaps by cardinality, AND-fold, then `andnot` the subtractions.
- `Cow<Bitmap64>` borrows the ART's bitmap without cloning when nothing is stale.

Regexes (`src/labels/regex.rs`) compile fully anchored with size/DFA limits (Prometheus
semantics), extract literal prefix/suffix/required-substring prefilters, and decompose
simple alternations (`node[12]` → exact `node1`/`node2` lookups).

### 7.6 Staleness, maintenance, discovery

- **Stale ids** are removed from `id_to_key`/`all_postings` eagerly and masked from every
  read; physical removal from posting lists is a cursor-bounded background drain
  (`remove_stale_ids`, `RemoveStaleSeries` task every 20s).
- **Optimization** (`optimize_postings`, `OptimizeIndices` every 60s) run-length-encodes
  and shrinks bitmaps incrementally.
- **Bulk build** (`bulk_build.rs`): during RDB/full-sync load, series are buffered and
  drained with sorted `add_many` inserts, bounded by `ts-index-build-max-memory` (256 MiB),
  degrading to per-key indexing over the cap.
- **Discovery** (`TS.LABELNAMES`, `TS.LABELVALUES`, `TS.METRICNAMES`, `TS.CARD`,
  `TS.LABELSTATS`) is served from ART prefix scans + bitmap cardinalities, with server-side
  fuzzy matching (Jaro-Winkler / subsequence), relevance sorting, and cardinality metadata
  (`docs/topics/label-discovery.md`).

---

## 8. Query Execution Pipeline

### 8.1 Lazy iterator stack

The read path (`src/iterators/`) is a lazily-composed stack of `Iterator<Item = Sample>`
adaptors, monomorphized and boxed only at the end. The base reader is `SampleIter`, an enum
with zero-copy variants (`Slice`, `Vec`, `Gorilla`, `Chimp`, `Range`, `Empty`) — compressed
chunks decode on demand rather than materializing.

`TimeSeriesRangeIterator` (the `TS.RANGE`/`TS.REVRANGE` entry) layers, in order: base reader
(or `TimestampFilterIterator` for `FILTER_BY_TS`), optional `LATEST` chaining, a value
filter, aggregation/grouping, the `last`-carry EMPTY adaptor, reversal, and `COUNT`
truncation. Direction is handled by **always aggregating forward** and reversing finished
buckets (`ReverseIter` buffers bucket count, not raw samples). `TailIter` (a ring buffer)
pushes `COUNT` down on reverse queries so `TS.MREVRANGE … COUNT n` is `O(n)`.

### 8.2 Aggregation framework

`AggregationType` (`src/aggregators/`) enumerates 23 variants across four categories:

- **Simple:** `min`, `max`, `first`, `last`, `range`, `sum`, `avg`.
- **Statistical:** `std.p`, `std.s`, `var.p`, `var.s` (Welford's algorithm → no catastrophic
  cancellation).
- **Counter/rate:** `count`, `count_all`, `count_nan`, `increase`, `rate`, `irate`
  (reset-aware).
- **Filtered/conditional:** `countif`, `sumif`, `share`, `all`, `any`, `none` (each gated by
  an inline `CONDITION op value`).

Behavior is a trait (`AggregationHandler`: `update`/`reset`/`current`/`finalize`/
`empty_value`/`empty_bucket_value`) dispatched via `enum_dispatch`. `MultiAggregateIterator`
updates N accumulators in one scan. Bucketing supports `ALIGN` (`start`/`end`/timestamp),
`BUCKETTIMESTAMP` (`start`/`mid`/`end`, clamped ≥ 0), and `EMPTY` fill. An empty bucket is
present for tallying aggregators (`count_all`) but absent for NaN-ignoring ones.

**Numerical stability:** sums use Neumaier compensated summation (`KahanSum`, `#[inline(never)]`
to prevent FP reordering); variance uses Welford. This is a deliberate, documented divergence
from RTS's unstable `E[x²] − E[x]²` identity — ValkeyTimeSeries returns the correct value
where RTS returns `NaN` or a cancelled `0` (COMPATIBILITY.md, DIV-0022/0024–0029/0039), and
it is **not** gated by strict mode because there is no second *intended* behavior to select.

### 8.3 Joins (`TS.JOIN`)

`process_join` (`src/join/`) fetches both sides in parallel and merges two timestamp-sorted
streams. `JoinType`: `Inner`, `Left`, `Right`, `Full`, `Semi`, `Anti`, `AsOf`.

- Inner/Left/Full use `joinkit` merge-joins; `Anti`/`Semi` are streaming existence filters;
  `Right` buffers into a min-max heap to keep output ordered.
- **ASOF** (ported from Polars): `Backward`/`Forward`/`Nearest` strategies with a monotonic
  scan offset, a `tolerance` window, and an `ALLOW_EXACT_MATCH` flag.
- **Reducers** (`sub`, `div`, `mul`, `abs_diff`, `coalesce`, `pct_change`, comparisons, …)
  combine the two sides per row; missing sides yield NaN. Reduced samples can flow through
  an aggregation.

### 8.4 Analysis (`TS.OUTLIERS`, seasonality)

The `AnomalyDetector` trait emits one score per point in `[0,1]` via a shared
`normalize_evidence(e,b) = e/(e+b)` contract, so **0.5 is always the decision boundary** and
scores are comparable across methods. Detectors (`src/analysis/outliers/`): `ZScore`, `IQR`,
`MAD`, `DoubleMAD`, `ModifiedZScore`, `SmoothedZScore`, `CUSUM`, `EWMA`, `ESD`, and
`RandomCutForest` (via the `krcf` crate; 100 trees × 256 samples, shingling, time-decay).

Optional `SEASONALITY` runs STL (single period) or MSTL (multiple periods) decomposition
(`src/analysis/seasonality/`) and detects on the remainder; `AUTO` uses a Welch periodogram
to find dominant periods. Both require `n ≥ 2 × max_period`.

---

## 9. Cluster and Fanout Architecture

Keys are placed by Valkey's normal hash-slot algorithm; the module does not change data
placement. Because `TS.*` multi-series commands operate at the **index level**, the module
performs its own intra-cluster RPC so the application interface is identical in cluster and
standalone mode.

- **Registration:** `register_fanout_operations` registers each `*FanoutCommand`
  (`mrange`, `mrevrange`, `queryindex`, `querylabels`, `mget`, `mdel`, `card`, label-stats,
  label-search) in a lock-free `FanoutOperationRegistry`.
- **Wire codec:** protobuf via `prost` (schemas in `proto/v1/`, generated code checked in;
  `src/commands/fanout_codec/` does local↔proto conversion). A per-response symbol table
  dictionary-interns repeated label names/values.
- **Cluster map** (`src/fanout/cluster_map.rs`): built from `CLUSTER NODES` text (not
  `CLUSTER SLOTS`, which is unsafe off the main thread), into a `RangeMapBlaze<u16, NodeId>`
  slot→shard map over 16384 slots, with per-shard fingerprints and adaptive refresh backoff
  (starts at `ts-cluster-map-expiration-ms`=750ms, doubles to 5s while stable). Held in an
  `ArcSwap`.
- **RPC** (`cluster_rpc.rs`): `ValkeyModule_SendClusterMessage` with REQUEST/RESPONSE/ERROR
  types. The coordinator ships `{id, db, handler, ACL user, cluster fingerprint, payload}`,
  tracks in-flight requests in a lock-free map with a per-request timeout timer. Receiving
  shards feature-gate (mixed-version handshake), re-authenticate as the ACL user, run on a
  worker thread, and reject requests whose topology fingerprint differs
  (`ClusterMapMismatch` → coordinator marks map stale and fails fast).
- **Async client:** the client is blocked (`ValkeyModule_BlockClient`); a reply callback
  runs on the main thread once all shards respond. **Any per-shard permission denial aborts
  the whole fanout** (data-returning commands must not silently drop keys). ACL keyspace
  restrictions are enforced on the fanned-out result.
- **Aggregation push-down** (`ts-fanout-aggregation-pushdown`, default on): shards return
  aggregated buckets, not raw samples. Decomposable `GROUPBY`/`REDUCE` reducers ship one
  partial state per (group, shard), merged coordinator-side (Kahan + Chan's parallel combine
  for Welford). Order-sensitive reducers (`increase`, `irate`) fall back to per-series bucket
  transport. `COUNT` is applied both shard-side (head/tail bound) and coordinator-side
  (final authority). Mixed-version clusters self-compensate via a compatibility handshake.

Constraints unchanged from RTS: a compaction rule's source and destination must share a hash
slot, and `TS.MADD` operates within a single slot (hash tags remain the mechanism).

---

## 10. Module Lifecycle, Threading, Memory

### 10.1 Lifecycle (`src/lib.rs`)

`valkey_module! { … }` generates `RedisModule_OnLoad`. Sequence:

1. **`preload()`** — hard-fails unless server version ≥ `[8,0,0]` and required (and
   blocking, for `TS.READ`) module APIs exist.
2. **`initialize()`** — `init_croaring_allocator` → `register_config` →
   `assign_command_acl_categories` → `register_server_event_handlers` → (if clustered)
   `init_fanout` + `register_fanout_operations` → capture main-thread id → `init_thread_pool`
   → `init_background_tasks`.

`MODULE UNLOAD` is rejected because the module exports a data type. User commands are
registered via the `#[command]` attribute macro on each `ts_*_cmd`; `@timeseries` ACL
categories are re-applied at load (the command-info path doesn't attach them).

### 10.2 Threading

- **Global rayon pool** sized by `ts-num-threads` (default 4, range 1–16). Rayon's global
  pool can't be resized, so this config is **immutable**. Query processing uses `orx_parallel`
  over rayon.
- **Main-thread execution** via `EventLoopAddOneShot` for anything needing the GIL.
- **Batch worker** — a dedicated thread drains an mpsc channel into batches, holding the
  module context for the whole batch to serialize keyspace access without deadlock.
- **Lock-free concurrency** — `ArcSwap` (cluster map), `papaya::HashMap` (in-flight requests,
  fanout registry, per-db indexes), atomics for config. Reads across series/chunks are
  concurrent and lock-free.

### 10.3 Memory / allocator

`AlignedValkeyAlloc` (`src/common/alloc.rs`) wraps Valkey's allocator and fixes
over-alignment (Valkey's allocator rounds size but not pointer alignment, breaking 128-byte
`CachePadded` used by rayon on aarch64): for `align > 16` it over-allocates, rounds up, and
stashes the original pointer for `dealloc`. Buffer pooling (`byte_pool`) supplies reusable
`Vec<u8>/i64/f64` on the RPC serialization path.

---

## 11. Configuration

All configurables live under Valkey's native `CONFIG GET`/`CONFIG SET` with `ts-` prefixes
(a deliberate divergence from RTS's `MODULE LOAD` args). Single-source-of-truth registry in
`src/config.rs`:

| Config | Default | Notes |
|---|---|---|
| `ts-chunk-size` | 4096 | `[48, 1 MiB]`, multiple of 8 |
| `ts-encoding` | `CHIMP` (alias `COMPRESSED`) | or `GORILLA`, `UNCOMPRESSED` |
| `ts-duplicate-policy` | `BLOCK` | block/first/last/min/max/sum |
| `ts-retention-policy` | 0 (no expiry) | ms; max 10 years |
| `ts-compaction-policy` | "" | default downsampling rules w/ per-key filters |
| `ts-compatibility-mode` | `extended` | `strict` reproduces RTS value resolution on gated divergences |
| `ts-decimal-digits` / `ts-significant-digits` | — | mutually exclusive rounding |
| `ts-ignore-max-time-diff` / `-val-diff` | 0 | IGNORE dedup thresholds |
| `ts-num-threads` | 4 | `[1,16]`, **immutable** |
| `ts-fanout-command-timeout` | 5000ms | `[500, 10000]` |
| `ts-cluster-map-expiration-ms` | 750 | `[0, 3.6M]` |
| `ts-index-build-max-memory` | 256 MiB | bulk-index cap during load |
| `ts-fanout-aggregation-pushdown` | yes | runtime escape hatch |
| `ts-index-persist` | yes | persist postings index as RDB aux |
| `ts-emulate-release` | current major | SemVer-safe compatibility-bug opt-in |

---

## 12. RedisTimeSeries Compatibility

Compatibility is framed from the application developer's perspective — the commands sent,
the responses parsed, the query semantics relied on (see `COMPATIBILITY.md`, the contract).

**Expected to match (differences are bugs):** command/argument syntax and reply shapes;
`FILTER` label matchers, `AGGREGATION`/`ALIGN`/`BUCKETTIMESTAMP`/`EMPTY`, `FILTER_BY_TS`,
`FILTER_BY_VALUE`, `COUNT`, `WITHLABELS`/`SELECTED_LABELS`, `GROUPBY`/`REDUCE`; the data
model (series + samples + labels + retention/encoding/duplicate-policy/rounding); compaction
rules. Sample order within a series is contractual; series order in a multi-series reply is
not.

**Intentional incompatibilities:** `ts-`-prefixed config surface; `INFO`/`TS.INFO` metric
sets; cluster mechanism (native cluster bus, no `OSS_GLOBAL_PASSWORD`, timeout errors,
ACL-scoped results); persistence/RDB/AOF formats; variance and sum/avg accumulation
(numerically correct vs RTS's unstable arithmetic); log messages. The `twa` aggregator is
not supported.

**Strict mode** (`ts-compatibility-mode strict`) closes the narrow class where *both engines
accept and both return a value but the values differ silently* — e.g. repeated-option
resolution (first-wins vs last-wins) and a back-filled compaction destination's `TS.GET`
last-sample. It does **not** restrict the additive surface or the numerically-correct
arithmetic divergences.

**Migration** is command-level: export/re-ingest (`TS.RANGE key - +` → `TS.CREATE` +
`TS.CREATERULE` + `TS.MADD`) or live dual-write. The loader guards both directions so a
cross-loaded `DUMP`/RDB fails cleanly and creates no key.

**Clean-room constraint:** RTS source and test code are license-incompatible and must never
be consulted; compatibility is derived only from public docs and black-box observation of a
pinned `redis:8.10` reference (see `AGENTS.md`, `tests/compat/`).

---

## 13. Notable Divergences from the RFC

The implementation has moved ahead of the RFC in several places; the code is authoritative:

| Area | RFC says | Implementation |
|---|---|---|
| Default encoding | Gorilla XOR | **Chimp (ELF-on-Chimp)**; `COMPRESSED` aliases it |
| Index bitmaps | (unspecified crate) | `croaring` 64-bit Roaring; ART is the **`blart`** crate |
| Default duplicate policy | `BLOCK` (create) / `LAST` (config) | `BLOCK` (`DEFAULT_DUPLICATE_POLICY`) |
| Command surface | core + JOIN/OUTLIERS/ADDBULK/MDEL/… | also `TS.READ`, `TS.NRANGE`/`TS.NREVRANGE`, `TS.QUERYLABELS`, `TS.METRICNAMES`, `TS.LABELSTATS`, `TS._DEBUG` |
| Cluster fan-out | "distributes across shards" | full protobuf RPC over cluster bus with aggregation/GROUPBY/COUNT push-down + mixed-version handshake |
| Variance/sum | (unspecified) | numerically stable (Welford/Neumaier); documented value divergence from RTS |
| Retention | active pruning | **eager** trim on write (DIV-0021) so `TS.INFO` agrees with `TS.RANGE` |
| RCF | "AWS implementation" | `krcf` crate |

The RFC also lists analysis extensions (`TS.FORECAST`, `TS.DECOMPOSE`, `TS.TREND`,
`TS.AUTOCORRELATION`, etc.) that appear in COMPATIBILITY.md's roadmap but are not yet in the
registered command table — treat those as future work.

---

## 14. Testing and Verification Strategy

- **Unit tests** (`cargo test --features enable-system-alloc`) and doc tests, using the
  shared `DataGenerator` fixtures (`src/tests/generators/`) across 16 workload shapes ×
  timestamp models.
- **Property tests** (`proptest`) for chunk codec and fanout-codec round-trips.
- **Integration tests** (Python `pytest` under `tests/`, `*_cme.py` for cluster mode) against
  a built `valkey-server`.
- **Differential compatibility harness** (`tests/compat/`): every command is sent to both the
  subject and a pinned `redis:8.10` reference, replies normalized and asserted equal
  (RESP2 + RESP3). Intentional mismatches are registered in `divergences.yml` as
  XFAIL-DIVERGENT; "reference errors, subject succeeds" always hard-fails.
- **Hypothesis fuzzer** (`./fuzz.sh`): generates valid command sequences and diffs replies
  against the reference (must run in `strict` mode).
- **Benchmarks & reports:** criterion benches (`encode`/`decode`/`query_scan`) plus
  `compression_report`, `latency_report`, and `wire_report` tools (the last drives the
  `WIRE_COMPRESSION_MIN_SAMPLES` wire-encoding policy decision).

---

## 15. Future Work

- **PromQL** query language (transform/aggregation/rollup functions), possibly with
  alerting/notifications.
- **Tiered storage** — move cold data to higher-compression chunks with higher access
  latency after an age threshold.
- **Advanced analysis** — forecasting (`TS.FORECAST`/`TS.AUTOFORECAST`), decomposition
  (`TS.DECOMPOSE`/`TS.TREND`), correlation (`TS.XCORR`), stationarity/periodicity, feature
  extraction, gap filling.
- **TWA** (time-weighted average) aggregation (currently unsupported).

---

## 16. References

- ValkeyTimeSeries RFC #4 (Proposed).
- Repository: `github.com/opensource-for-valkey/valkey-timeseries` (Apache-2.0);
  `README.md`, `AGENTS.md`, `COMPATIBILITY.md`, `docs/overview.md`,
  `docs/topics/{encodings,filter-syntax,label-discovery}.md`.
- Pelkonen et al., "Gorilla: A Fast, Scalable, In-Memory Time Series Database," PVLDB 2015.
- Liakos et al., "Chimp: Efficient Lossless Floating Point Compression," PVLDB 2022.
- Li et al., "ELF: Erasing-based Lossless Floating-Point Compression," PVLDB 2023.
- Leis et al., "The Adaptive Radix Tree," and Roaring Bitmaps (roaringbitmap.org).
- Prometheus selector / PromQL documentation.