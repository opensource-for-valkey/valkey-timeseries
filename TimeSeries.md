---
RFC: 4
Status: Proposed
---

# ValkeyTimeSeries Module RFC

## Abstract

The proposed feature is ValkeyTimeSeries, which is a [Rust](https://www.rust-lang.org/) based Module that brings a native time series data type to Valkey.

To help users migrate from Redis and RedisTimeSeries, as well as capitalize on existing OSS RedisTimeSeries client libraries, 
the module is designed to be API-compatible with Redis Ltd.’s RedisTimeSeries.

## Motivation

Support for a time series data type in Valkey is essential for developers wanting to use Valkey in domains like observability, 
monitoring, IoT, and analytics.

Valkey TimeSeries aims to enable the unique characteristics of time-series data:

* Efficient Storage: optimize for high write throughput and data compression, enabling us to handle the continuous influx of large volumes of timestamped data.
* Real-Time Analytics: support real-time querying and computation, allowing users to detect trends, anomalies, or patterns as they occur.
* Time-centric Queries: allow for time-based aggregations and querying over specific time intervals with high efficiency.

Although Valkey time series modeling can be achieved natively in ValKey using built-in types like `stream`s and `zset`s,
a dedicated time series data type can provide better performance, memory efficiency, and ease of use for time series workloads.

Redis‘ TimeSeries module is published under a proprietary license, hence cannot be distributed freely with ValKey.

## Design Considerations

The ValkeyTimeSeries module brings a time series module data type to Valkey and provides commands to create 
time series, operate on them (add sample, query, perform aggregations, compactions, downsampling, and joins, etc.).
It allows customization of the properties of each series (compression, retention, compactions, etc.) through commands and 
configurations. 

We aim to be efficient both in memory usage and performance. This is achieved through:
* use of parallelism and background tasks where appropriate (multi-chunk fetches, series compactions, index maintenance, etc.)
* efficient memory management (interning for labels, index prefix compression, Roaring Bitmaps for series ids)
* efficient filtering and indexing

We have the following terminologies:
* TimeSeries Object: The top level structure representing the data type. It contains metadata and a list of lower-level
                chunks containing the actual sample data.
* Sample: A tuple of timestamp and value.
* Chunk: A container for samples. Chunks have configurable size and encoding policies.

### Enhancements over RedisTimeSeries
* `Multi-db Support` - allow users to create time series objects in different databases.
* `Joins` - ValkeyTimeSeries supports joins between time series objects, including INNER, OUTER, and ASOF joins
* `Filtering` - support filtering using Prometheus style selectors
* `Compaction` - support for creating compaction rules based on other compactions.
    ```bash
    redis> TS.CREATE visitor:count:1m
    OK
    redis> TS.CREATE visitors:count:1h
    OK
    redis> TS.CREATE visitors:count:1d
    OK
    
    redis> TS.CREATERULE visitor:count:1m visitors:count:1h AGGREGATION sum 1h
    OK
    redis> TS.CREATERULE visitors:count:1h visitors:count:1d AGGREGATION sum 1d
    OK
    ```
    
    In addition, default compactions can specify a filter expression to select which keys they are applied to.

    ```bash
    redis> CONFIG SET ts-compaction-policy avg:2h:10d|^metrics:memory:*;sum:60s:1h:5s|^metrics:cpu:*
    OK
    ```
* `Aggregation` - extended set of aggregations like `increase`, `rate`, and `irate`, as well as conditional aggregators like `all`, `any`, `countif`, `share` and `sumif`.
* `Metadata` - support for returning metadata on time series objects (label names, label values, cardinality, etc)
* `Rounding` - support for rounding sample values to specified precision. This is enforced for all samples in a time series.
* `Active Expiration` - support for active pruning of time series data based on retention.
* `Developer Ergonomics` - support for relative timestamps in queries, e.g. `TS.RANGE key -6hrs -3hrs`, unit suffixes (e.g. `1s`, `3mb`, `20K`), 
    and a more expressive query language.

### Module OnLoad

Upon loading, the module registers a new time series module-based data type, creates time series (TS.*) commands,
timeseries specific configurations, and the TimeSeries ACL category.

* Module name: ts
* Data type name: `TSDB-TYPE`
* Module shared object file name: `libvalkey_timeseries.[so|dylib|dll]`

With the Module name as "ts", ValkeyTimeSeries is compatible with RedisTimeseries in its Module name, which is accessible by clients
through HELLO, MODULE LIST, and INFO commands. Also, metrics and configs will be prefixed with this name (by design for Modules).

Regarding the Module Data type name, because ValkeyTimeSeries's Module Data type (the current version) is not compatible with
RedisTimeSeries, it is not named the same as RedisTimeSeries's. 

### Module Unload

Once the Module has been loaded, the `MODULE UNLOAD` will be rejected since Module Data type is created on load.
Valkey does not allow unloading of Modules that export a module data type.

```
127.0.0.1:6379> MODULE UNLOAD timeseries
(error) ERR Error unloading module: the module exports one or more module-side data types, can't unload
```

### Persistence

ValkeyTimeSeries implements persistence-related Module data type callbacks for the TimeSeries data type:

* rdb_save: Serializes timeseries objects to RDB.
* rdb_load: Deserializes time series objects from RDB.
* aof_rewrite: Emits commands into the AOF during the AOF rewriting process.

### RDB Compatibility with RedisTimeSeries

ValkeyTimeSeries is _**not**_ RDB Compatible with RedisTimeSeries.

The metadata that gets written to the RDB is specific to the Module data type's structure and struct members.
Additionally, the data within the underlying time series is specific to the implementation of
the time series.

Because of this, it is not possible to be RDB compatible with RedisTimeSeries.

### AOF Rewrite handling

Module data types (including timeseries) can implement a callback function that will be triggered for TimeSeries objects to rewrite
its data as command/s. From the AOF callback, we will handle AOF rewrite by using `DUMP` and `RESTORE`.

### Migrating workloads from RedisTimeSeries:

Customers that currently use RedisTimeSeries can move to ValkeyTimeSeries using two approaches:

1. Create the time series objects to have the same properties using TS.CREATE / TS.ALTER on Valkey (with ValkeyTimeSeries loaded).
Re-populate the time series objects by inserting items by moving the existing samples to the Valkey server
(with ValkeyTimeSeries loaded). The workload can be moved without any errors since we are API Compatible.

Pros
* This is the simplest option in terms of effort—assuming the user is fine with recreating time series objects and adding
    items on the Valkey server (with ValkeyTimeSeries).

Cons
* The user will need to re-create the time series (using TS.CREATE/TS.ADD) and populate these objects **AFTER**
    switching to Valkey (with ValkeyTimeSeries).

Users can generate an AOF file from a server (that has the RedisTimeSeries module loaded) and with an ongoing timeseries workload
that creates time series and inserts items into them. Next, this can be re-played on a Valkey Server (with ValkeyTimeSeries loaded).
Then, the user can move their existing workload to the Valkey server (with ValkeyTimeSeries loaded).

### Memory Management

On Module Load, the Rust Module overrides the memory allocator that delegates the allocation and deallocation tasks
to the Valkey server using the existing Module APIs: ValkeyModule_Alloc and ValkeyModule_Free. This API panics if unable
to allocate enough memory.

The timeseries data type also supports memory management related callbacks:

* free: Frees the time series object when it is deleted, expired, or evicted.
* defrag: Supports active defrag for TimeSeries objects
* mem_usage: Reports bytes used by a TimeSeries object and is used by the MEMORY USAGE command
* copy: Supports a deep copy of time series objects and is used by the COPY command
* free_effort: Determine whether the TimeSeries object's memory needs to be lazily reclaimed or synchronously freed.

### Replication
Every TimeSeries-based write operation (creation, adding, and removing samples) will be replicated to replica nodes.

## Specification

### TimeSeries Structure
Each time series object is represented by a sorted list of chunks, each containing a list of samples. Each sample is a tuple
of 64bit epoch-based timestamp and a 64bit float value. 

A time series is optionally identified by a series of label-value pairs used to retrieve the series in queries. Given that these
labels are used to group semantically similar time series, they are necessarily duplicated. We take advantage of this fact to 
intern label-value pairs, meaning that only a single allocation is made per unique pair, irrespective of the number of series
it occurs in.

The time series object also contains metadata like the retention policy, compaction policy, etc.

### TimeSeries Value Encoding
TimeSeries Chunks have configurable sample encoding strategies, defaulting to Gorilla XOR compression.
Chunks can also be configured to store values as Uncompressed, but this is provided only for compatibility with RedisTimeseries.

### TimeSeries Indexing
In addition to labels, a time series is uniquely identified by an opaque unsigned 64bit int. Each label-value pair
is mapped to the id of each series that contains that attribute. The mapping is implemented as an [Adaptive Radix Tree (ART)](https://db.in.tum.de/~leis/papers/ART.pdf) (pdf), 
where each node is a 64bit [Roaring BitMap](https://roaringbitmap.org/about/).

The ART allows for efficient lookups and insertions, while the Roaring BitMap performs fast set operations on the series ids.
In addition, the ART supports path compression for additional memory savings based on our indexing scheme (see below).

### TimeSeries Indexing Scheme
The ART is used to index time series based on their labels. For each unique combination of label and value, we create a key by concatenating 
the label and value strings. E.g. "region=us-west-2". This key is used to manage a 64bit roaring bitmap that contains the ids of all time series 
that have that label-value pair. To retrieve ids for a given list of label-value pairs, we look up the keys in the ART and perform an intersection.
The ART natively supports range queries, so we can efficiently find keys with a given prefix. For example, we can search on the prefix "region=" to
find all time series with a label "region".

We also maintain a mapping from id to a valkey key to retrieve the time series after querying.

### TimeSeries Filter Enhancements
We support an extension to the RedisTimeseries filter syntax to support regex-based filtering using the operators `=~` and `!~`. 
For example:

* `TS.QUERYINDEX region=~us-west.*` will return all time series that have a label `region` with a value that starts with `us-west`.

* `TS.QUERYINDEX region!~us-west.*` will return all time series that have a label `region` with a value that does not start with `us-west`.

In addition, we add Prometheus style selectors to the filter syntax (essentially an [Instant Vector](https://promlabs.com/blog/2020/07/02/selecting-data-in-promql/#instant-vector-selectors) selector). For example:

* `TS.QUERYINDEX latency{region=~"us-west-*",service="inference"}` will return all series recording latency for the inference service in all us west regions. 

* `TS.MRANGE -6hrs -3hrs {service="inference"}` will return samples recorded between 3 and 6 hours ago for the inference service across all series 

We also support `"OR"` matching for Prometheus style selectors. For example:

* `TS.QUERYINDEX latency{region="us-west" or region="us-east"}` will return all series recording latency samples in either `us-west` or `us-east` regions.

As per Prometheus conventions, all regex matchers are anchored. For example, a matcher of `env=~"foo"` is treated as `env=~"^foo$"`, so any anchors 
used in the query will be redundant. 

Note that for selectors of the form `metric{label="value"}`, `metric` is matched against the reserved `__name__` label. In other words,
the series is expected to have a label `__name__` with the value `metric`.

### RDB Format

During RDB save, the Module data type callback is invoked, and we store series-specific data.


### TimeSeries Command API

The following are supported TimeSeries commands with API syntax compatible with RedisTimeSeries:

### TS.CREATE

#### Syntax
```
TS.CREATE key
  [RETENTION retentionPeriod]
  [ENCODING <COMPRESSED|UNCOMPRESSED>]
  [CHUNK_SIZE chunkSize]
  [DUPLICATE_POLICY policy]
  [DEDUPE_INTERVAL duplicateTimediff]
  [[LABELS [label value ...] | METRIC metricName]
```
Create a new time series. This command creates a new time series with the specified key and optional properties.

#### Options
- **ENCODING**: The encoding to use for the timeseries. Default is `COMPRESSED`.
- **DUPLICATE_POLICY**: The policy to use for duplicate samples. Default is `BLOCK`.

#### Required arguments

<summary><code>key</code>
is key name for the time series.
</summary>

### Optional Arguments
<summary><code>retentionPeriod</code>
The period of time for which to keep series samples. Retention can be specified as an integer indication
the duration as milliseconds, or a duration expression like `3wk`
</summary>

<summary><code>chunkSize</code>
The chunk size for the timeseries, in bytes. Default is `4096`.
</summary>

<summary><code>dedupeInterval</code>
Limits sample ingest to the timeseries. If a sample arrives less than `dedupeInterval` from the most
recent sample it is ignored. Default is `0`
</summary>

<summary><code>LABELS</code>
A list of label-value pairs to associate with the time series. For example, `LABELS region us-west service inference` would create a time series with labels `region=us-west` and `service=inference`.
</summary>

<summary><code>METRIC</code>
A shorthand for creating series labels using `prometheus` semantics. For example,

```aiignore
TS.CREATE request_latency:db:sales:usa METRIC "request_latency{region=\"us\",service=\"sales\",type=\"db\"}"
```

would create a time series with the same labels as the example in LABELS above, but with an additional `__name__=request_latency` label.
</summary>

Note that the `LABELS` and `METRIC` options are mutually exclusive, and at most one of them can be provided when creating a time series.

---
### TS.CREATERULE
#### Syntax

```
TS.CREATERULE sourceKey destKey 
    AGGREGATION aggregator bucketDuration
    [alignTimestamp]
```

Create a compaction rule that aggregates data from `sourceKey` into `destKey` using the specified aggregation
function and bucket duration.

---
### TS.ALTER

#### Syntax
```
TS.ALTER key
  [RETENTION retentionPeriod]
  [CHUNK_SIZE chunkSize]
  [DUPLICATE_POLICY policy]
  [DEDUPE_INTERVAL duplicateTimediff]
  [[LABELS [label value ...] | METRIC metricName]
```

Alter an existing time series. This command allows you to change the properties of a time series, such as its 
retention period, chunk size, and duplicate policy.

#### Options
- **ENCODING**: The encoding to use for the timeseries. Default is `COMPRESSED`.
- **DUPLICATE_POLICY**: The policy to use for duplicate samples. Default is `BLOCK`.

#### Required arguments

<summary><code>key</code>
is key name for the time series.
</summary>

#### Optional Arguments
<summary><code>retentionPeriod</code>
The period of time for which to keep series samples. Retention can be specified as an integer indication
the duration as milliseconds, or a duration expression like `3wk`
</summary>

<summary><code>chunkSize</code>
The chunk size for the timeseries, in bytes. Default is `4096`.
</summary>

---
### TS.ADD

#### Syntax
```
TS.ADD key timestamp value
```

Add a sample to a time series. If the key does not exist, it is created with the specified timestamp and value.


#### Required Arguments

<summary><code>key</code>
is key name for the time series.
</summary>

<summary><code>timestamp</code>
The timestamp of the incoming sample
</summary>

<summary><code>value</code>
the value of the sample (double)
</summary>

#### Return
The timestamp of the added sample.

---
### TS.DEL

#### Syntax

```
TS.DEL key fromTimestamp toTimestamp
```

Deletes data for a selection of series in a time range.

#### Options

- **key**: the key being deleted from.
- **fromTimestamp**: Start timestamp, inclusive. Optional.
- **toTimestamp**: End timestamp, inclusive. Optional.

#### Return

- the number of samples deleted.

#### Error

Return an error reply in the following cases:

TODO

#### Examples

```
TS.DEL requests:status:200 587396550 1587396550
```
---
### TS.DELETERULE
#### Syntax

```
TS.DELETERULE sourceKey destKey
```
Delete a compaction rule that aggregates data from `sourceKey` into `destKey`.

---

### TS.MADD

#### Syntax
```
TS.MADD <key> <item> [<item> ...]
```
Add multiple samples to a time series. If the key does not exist, it is created with the specified timestamp and value.


---
### TS.GET
#### Syntax
```
TS.GET <key> [LATEST]
```

Get the last sample from a series. If `LATEST` is specified, it returns the value of the latest unclosed bucket if the series
is a compaction.

---
### TS.MGET
#### Syntax
```
TS.MGET 
    [LATEST] 
    [WITHLABELS | <SELECTED_LABELS label...>] 
    FILTER filterExpr...
```
Get the last sample from a multiple series specified by a filter.

#### Required Arguments
<summary><code>filterExpr</code>
is a filter expression that selects the series to return. At least one filter argument must be provided.
</summary>
---

### TS.INCRBY

#### Syntax
```
TS.INCRBY key delta
  [TIMESTAMP timestamp]
  [RETENTION retentionPeriod]
  [ENCODING <COMPRESSED|UNCOMPRESSED>]
  [CHUNK_SIZE chunkSize]
  [DUPLICATE_POLICY policy]
  [DEDUPE_INTERVAL duplicateTimediff]
  [[LABELS [label value ...] | METRIC metricName]
```
increment the value of the last sample in a time series

---
### TS.DECRBY

#### Syntax
```
TS.DECRBY key delta
  [TIMESTAMP timestamp]
  [RETENTION retentionPeriod]
  [ENCODING <COMPRESSED|UNCOMPRESSED>]
  [CHUNK_SIZE chunkSize]
  [DUPLICATE_POLICY policy]
  [DEDUPE_INTERVAL duplicateTimediff]
  [[LABELS [label value ...] | METRIC metricName]
```

Decrement the value of the last sample in a time series. If the key does not exist, it is created with the specified 
timestamp and value.

#### Required Arguments
<summary><code>key</code>
is key name for the time series.
</summary>
<summary><code>delta</code>
the value to decrement the last sample by (double)
</summary>

---
### TS.INFO

#### Syntax
```
TS.INFO <key>
```

Get information about a time series.

---
### TS.RANGE

#### Syntax
```
TS.RANGE key fromTimestamp toTimestamp 
    [LATEST]
    [FILTER_BY_TS ts...] 
    [FILTER_BY_VALUE min max]
    [COUNT count] 
    [AGGREGATION aggregator bucketDuration [CONDITION op value] [ALIGN align] [BUCKETTIMESTAMP timestamp] [EMPTY]]
```

Query a range of data, optionally with filtering and aggregation.

### Required Arguments
<details open><summary><code>key</code></summary>
the time series key to query.
</details>
<details open><summary><code>fromTimestamp</code></summary>
the start of the time range to query, inclusive. Accepts:
- Numeric timestamp in milliseconds
- `-` for the earliest timestamp in the series
- Duration spec (e.g., `2h` for 2 hours ago)
</details>
<details open><summary><code>toTimestamp</code></summary>
the end of the time range to query, inclusive. Accepts:
  - Numeric timestamp in milliseconds
  - `+` for the latest timestamp in the series
  - `*` for the current time
  - Duration spec (e.g., `30m` for 30 minutes ago)
</details>

### Optional Arguments
<details open><summary><code>LATEST</code></summary>
When querying a compaction, return the latest bucket value even if the bucket is not yet closed. This is in addition to the regular range results. 
</details>
<details open><summary><code>FILTER_BY_TS timestamp ...</code></summary>
Include only samples at the specified timestamp(s). Multiple timestamps can be provided. Applied before aggregation.
</details>
<details open><summary><code>FILTER_BY_VALUE min max</code></summary>
Include only samples with values in `[min, max]`. Both bounds are inclusive. Applied before aggregation.
</details>
<details open><summary><code>COUNT count</code></summary>
Limit output to the first `count` samples or buckets. When used with aggregation, limits bucket
count (not samples per bucket).
</details>
<details open><summary><code>AGGREGATION aggregator bucketDuration [CONDITION op value]</code></summary>
Aggregate raw samples into fixed-size time buckets. See [Aggregators](#aggregators) for supported aggregation functions.
`aggregator` is the aggregation function to apply, and `bucketDuration` is the bucket size in milliseconds (must be positive).

Optionally, a `CONDITION` can be provided to apply a comparison filter for conditional aggregators (e.g., `countif`, `sumif`, `share`, `all/any/none`).
- `op` is a comparison operator: `>`, `<`, `>=`, `<=`, `==`, or `!=`
- `value` is the value to compare against
Only samples satisfying the condition are included in the aggregation.

For example, to compute the share of latency samples greater than 10ms in 1-minute buckets over the last hour:
```valkey-cli
TS.RANGE request_latency:db -1h * AGGREGATION share 60000 CONDITION > 10
```

</details>
<details open><summary><code>ALIGN align</code></summary> 
Control bucket alignment:

  - `start` — Align buckets to range start
  - `end` — Align buckets to range end
  - Numeric timestamp — Align all buckets to a specific timestamp
</details>
<details open><summary><code>BUCKETTIMESTAMP bt</code></summary>
Which timestamp to return for each bucket:

- `start` (default) — Bucket start time
- `end` — Bucket end time
- `mid` — Bucket midpoint

</details>


### Supported Aggregators

#### Simple Aggregators

| Aggregator | Description                    | Empty Bucket Value |
|------------|--------------------------------|--------------------|
| `avg`      | Arithmetic mean                | `NaN`              |
| `sum`      | Sum of all values              | `0`                |
| `count`    | Number of samples              | `0`                |
| `min`      | Minimum value                  | `NaN`              |
| `max`      | Maximum value                  | `NaN`              |
| `range`    | Difference between max and min | `NaN`              |
| `first`    | Earliest sample value          | —                  |
| `last`     | Latest sample value            | —                  |

#### Statistical Aggregators

| Aggregator | Description                   | Empty Bucket Value     |
|------------|-------------------------------|------------------------|
| `std.p`    | Population standard deviation | `NaN`                  |
| `std.s`    | Sample standard deviation     | `NaN` (if < 2 samples) |
| `var.p`    | Population variance           | `NaN`                  |
| `var.s`    | Sample variance               | `NaN` (if < 2 samples) |

#### Counter/Rate Aggregators

| Aggregator | Description                                      | Notes                                                                 |
|------------|--------------------------------------------------|-----------------------------------------------------------------------|
| `increase` | Total increase for monotonic counters            | Handles resets                                                        |
| `rate`     | Rate of change per second over the bucket window | —                                                                     |
| `irate`    | Instantaneous rate from the last two samples     | Requires ≥ 2 samples and positive time delta; returns `NaN` otherwise |

#### Filtered Aggregators

> These operate only on samples matching a comparison condition.

| Aggregator | Description                                           | Empty Bucket Value |
|------------|-------------------------------------------------------|--------------------|
| `countif`  | Count of samples matching condition                   | `0`                |
| `sumif`    | Sum of samples matching condition                     | `0`                |
| `share`    | Fraction of samples matching condition (`[0.0, 1.0]`) | `NaN`              |
| `all`      | `1.0` if all samples match, `0.0` otherwise           | `NaN`              |
| `any`      | `1.0` if any sample matches, `0.0` otherwise          | `NaN`              |
| `none`     | `1.0` if no samples match, `0.0` otherwise            | `NaN`              |

---

#### Return Value

**Without aggregation:**  
Array of `[timestamp, value]` pairs

**With aggregation:**  
Array of `[bucketTimestamp, aggregatedValue]` pairs

---

#### Examples

<details open><summary>Query all samples between Jan 1, 2021 and Jan 2, 2021:</summary>

```
TS.RANGE temperature 1609459200000 1609545600000
``` 
</details>

<details open><summary>
Get samples where value is between 20 and 30:
</summary>

```
TS.RANGE temperature 1609459200000 1609545600000 FILTER_BY_VALUE 20 30
```
</details>

<details open><summary>
Get samples at exact timestamps:
</summary>

```
TS.RANGE sensor:001 - + FILTER_BY_TS -2h 1609459260000 1609459320000
```
</details>

<details open><summary>
Compute average per hour:
</summary>

```
TS.RANGE requests 1609459200000 1609545600000 AGGREGATION avg 3600000
```

<details open><summary>
5-Minute Sums with Empty Buckets
</summary>

```
TS.RANGE bytes 1609459200000 1609470000000 
  ALIGN start 
  AGGREGATION sum 300000 
  BUCKETTIMESTAMP mid 
  EMPTY
```

</details>

<details open><summary>
Limited Results
</summary>

```
TS.RANGE metrics 1609459200000 1609545600000 
  AGGREGATION avg 60000 
  COUNT 100
```
</details>

<details open><summary>
Compute the share of latency samples greater than 10ms in 1-minute buckets over the last hour:
</summary>

```
TS.RANGE request_latency:db -1h * AGGREGATION share 60000 CONDITION > 10
```
</details>

<details open><summary>
Returns 1.0 per bucket if only NaN values are received in 1 min buckets over the past hour, otherwise returns 0.0. 
</summary>

```
TS.RANGE request_latency:db -1h * AGGREGATION all 60000 CONDITION == NaN
```

</details>

---
### TS.REVRANGE

#### Syntax

```
TS.REVRANGE key fromTimestamp toTimestamp 
    [LATEST]
    [FILTER_BY_TS ts...] 
    [FILTER_BY_VALUE min max]
    [COUNT count] 
    [AGGREGATION aggregator bucketDuration [CONDITION op value] [ALIGN align] [BUCKETTIMESTAMP timestamp] [EMPTY]]
```

Query a range of data in reverse order, optionally with filtering and aggregation.

The syntax and options are the same as `TS.RANGE`, but results are returned in reverse chronological order.

---
### TS.MRANGE

#### Syntax

```
TS.MRANGE fromTimestamp toTimestamp
    [LATEST
    [FILTER_BY_TS ts...] 
    [FILTER_BY_VALUE min max]
    [COUNT count] 
    [REDUCE operator]
    [GROUPBY label REDUCE reducer]
    [AGGREGATION aggregator bucketDuration [CONDITION op value] [ALIGN align] [BUCKETTIMESTAMP timestamp] [EMPTY]]
    [WITHLABELS | <SELECTED_LABELS label...>]
```

Query a range of data across multiple series

---
### TS.MREVRANGE
#### Syntax
```
TS.MREVRANGE fromTimestamp toTimestamp 
    [FILTER_BY_TS ts...] 
    [FILTER_BY_VALUE min max]
    [COUNT count] 
    [REDUCE operator]
    [GROUPBY label REDUCE reducer]
    [AGGREGATION aggregator bucketDuration [CONDITION op value] [ALIGN align] [BUCKETTIMESTAMP timestamp] [EMPTY]]
    [WITHLABELS | <SELECTED_LABELS label...>]
```

Query a range of data across multiple series in the reverse order

---
### TS.QUERYINDEX

#### Syntax

```
TS.QUERYINDEX [FILTER_BY_RANGE [NOT] fromTimestamp toTimestamp] filterExpression ...
```

Returns the list of time series keys that match the filter expression(s).

#### Required Arguments
<summary><code>filterExpression</code>
is a filter expression that selects the series to return based on label filters. At least one filter argument must be provided.
</summary>

#### Optional Arguments
<code>fromTimestamp</code>

Start timestamp, inclusive. Results will only be returned for series which have samples in the range `[fromTimestamp, toTimestamp]`

<code>toTimestamp</code>

End timestamp, inclusive.

If **`NOT`** is specified before `fromTimestamp` and `toTimestamp`, the result will include only the keys of series which 
do *NOT* have samples in the specified range.

---
### New Commands

The following are NEW commands that are not included in RedisTimeSeries:

---

### TS.ADDBULK

Ingest time-series samples from a JSON payload into a series. 

#### Syntax

```
TS.ADDBULK key data 
  [RETENTION duration] 
  [DUPLICATE_POLICY policy] 
  [ON_DUPLICATE policy_ovr] 
  [ENCODING COMPRESSED|UNCOMPRESSED] 
  [CHUNK_SIZE chunkSize] 
  [METRIC metric | LABELS labelName labelValue ...] 
  [IGNORE ignoreMaxTimediff ignoreMaxValDiff] 
  [SIGNIFICANT_DIGITS significantDigits | DECIMAL_DIGITS decimalDigits]
```

#### Required arguments

<summary><code>key</code>

 key name for the time series.
</summary>

<summary><code>data</code>
 
JSON payload containing sample data. Must be a single JSON object with `values` and `timestamps` arrays. Up to 1000 samples 
can be ingested per command.
</summary>

#### Optional arguments

<summary><code>RETENTION duration</code>

 Maximum retention period in milliseconds. Samples older than this are automatically deleted. `0` means infinite retention. Default: module configuration.
</summary>

<summary><code>DUPLICATE_POLICY policy</code>

Policy for handling duplicate timestamps:

- `BLOCK` - Ignore duplicate (default when no policy is set)
- `FIRST` - Keep first occurrence
- `LAST` - Keep last occurrence
- `MIN` - Keep minimum value
- `MAX` - Keep maximum value
- `SUM` - Sum all values

</summary>

<summary><code>ON_DUPLICATE policy_ovr</code>

Override the duplicate policy for this command invocation only. Does not modify the series' configured policy.

</summary>

<summary><code>ENCODING COMPRESSED|UNCOMPRESSED</code>

Storage encoding:
 
- `COMPRESSED` - Gorilla compression (default)
- `UNCOMPRESSED` - Raw storage

</summary>

<summary><code>CHUNK_SIZE chunkSize</code>

Maximum size in bytes for each chunk. Actual memory usage may exceed this slightly. Default: 4096.

</summary>

<summary><code>METRIC metric | LABELS labelName labelValue ...</code>

Series metadata for filtering and queries:

- `METRIC` - A prometheus style metric specification (e.g. http_errors_total{service="auth",region="us-east"})
- `LABELS` - Explicit label name-value pairs

</summary>

<summary><code>IGNORE ignoreMaxTimediff ignoreMaxValDiff</code>

Filtering thresholds for incoming samples:
 
 - `ignoreMaxTimediff` - Maximum time difference (ms) from last sample
 - `ignoreMaxValDiff` - Maximum absolute value difference from last sample

Samples exceeding either threshold are dropped.

</summary>

<summary><code>SIGNIFICANT_DIGITS significantDigits | DECIMAL_DIGITS decimalDigits</code>

 Value precision control (mutually exclusive): 

- `SIGNIFICANT_DIGITS` - Number of significant digits (0-18) 
- `DECIMAL_DIGITS` - Number of decimal places

</summary>

#### JSON payload format

The `data` argument expects a JSON object with the following structure:

```json
{
  "values": [
    1.0,
    2.0,
    3.0
  ],
  "timestamps": [
    1620000000000,
    1620000001000,
    1620000002000
  ]
}
```

**Required fields:**

- `values` - Array of numeric values (parsed as floats)
- `timestamps` - Array of timestamps in milliseconds (parsed as 64-bit integers)

**Constraints:**

- `values` and `timestamps` arrays must have equal length
- At least one sample must be present

#### Return value

[Array reply](https://valkey.io/docs/reference/protocol-spec/#arrays) of two integers:

1. Number of successfully ingested samples
2. Total number of samples in the payload

#### Behavior

- **Sorting:** Input samples are automatically sorted by timestamp before insertion
- **Retention filtering:** Samples older than the retention window are dropped before processing
- **Duplicate handling:** Controlled by `DUPLICATE_POLICY` (or `ON_DUPLICATE` override)
- **Chunk allocation:** New chunks are created automatically as needed
- **Series creation:** If the key doesn't exist, a new series is created with the provided options
- **Ingestion count:** Only successfully inserted samples are counted; dropped or blocked samples are excluded from the
  success count

### Examples

#### Basic ingestion

Ingest two samples into a series:

```valkey-cli
TS.ADDBULK sensor:temp:room1 '{"values":[22.5,23.1],"timestamps":[1620000000000,1620000060000]}'
```

**Result:**

```
1) (integer) 2
2) (integer) 2
```

#### Create series with custom options

```valkey-cli
TS.ADDBULK sensor:pressure:tank2 '{"values":[101.3,101.5],"timestamps":[1620000000000,1620000001000]}' RETENTION 86400000 CHUNK_SIZE 8192 DUPLICATE_POLICY LAST LABELS sensor_type pressure location tank2
```

#### Error conditions

- **WRONGTYPE:** Key exists but is not a time series
- **Wrong arity:** Incorrect number of arguments
- **TSDB: missing values:** JSON payload lacks `values` array
- **TSDB: missing timestamps:** JSON payload lacks `timestamps` array
- **TSDB: values and timestamps length mismatch:** Arrays have different lengths
- **TSDB: no timestamps or values:** Arrays are empty
- **missing key or metric_name:** `metric` provided without `key` or `metric_name`

#### Notes

- If the key does not exist, it will be created with the provided options
- Input samples are sorted by timestamp before insertion
- Retention filtering occurs **before** chunk grouping and insertion
- The ingested count may be less than the payload count if samples are dropped due to retention, duplicates, or filters
- When the series doesn't exist and no options are provided, module-level defaults apply

---
### TS.MDEL

#### Syntax

```
TS.MDEL [fromTimestamp toTimestamp] FILTER selector...
TS.MDEL FILTER selector...
```

Delete samples in a timestamp range, or delete entire time series, for all series matching a label filter, possibly
across a cluster.

#### Summary

Two modes:

- **Range deletion**: `TS.MDEL fromTimestamp toTimestamp FILTER ...`  
  Removes samples in the inclusive range [`fromTimestamp`, `toTimestamp`] from every matching series.

- **Series deletion**: `TS.MDEL FILTER ...`  
  Removes entire time series keys matching the filter.

#### Required arguments

<summary><code>FILTER</code>

One or more series selectors used to match series (for example, <code>env=prod</code>).
</summary>

#### Optional arguments

<summary><code>fromTimestamp</code>
Start of the inclusive timestamp range. Required for range deletion.
</summary>

<summary><code>toTimestamp</code>
End of the inclusive timestamp range. Required for range deletion.
</summary>

#### Return

Integer reply:

- In range deletion mode: the total number of samples deleted across all matched series.
- In series deletion mode: number of series (keys) deleted.

#### Notifications and replication

- Range deletions emit `ts.del` events for affected keys (module notification).
- Series deletions emit `del` events for removed keys.

#### Permissions and ACLs

The module enforces DELETE permission on matching keys. Clients must have permission to delete the 
matching series or global delete permission.

#### Errors

- keyword or selectors missing.
— timestamp parse error.

Per-key delete failures are logged and do not stop processing other keys; the overall command returns the count of successful deletions.

#### Behavior notes

- Range deletion removes samples in the inclusive range [`fromTimestamp`, `toTimestamp`].
- The command enforces retention rules.
- Series deletion removes the whole key/value for each matching series.
- Compaction is run after removals as appropriate.

#### Examples

Delete samples between `1609459200000` and `1609545600000` from series with label `env=prod`:

```
TS.MDEL 1609459200000 1609545600000 FILTER env=prod
```

Delete one week of samples from all series tracking 500 errors for the payment service:

```
TS.MDEL -8d -1d FILTER http_errors{service=payment,status=500}
```

Delete all auth service latency related series in the `us-east` region in staging environment:

```
TS.MDEL FILTER api_latency{service=auth,region~="us-east-?",env=staging}
```

---
### TS.CARD

#### Syntax

```
TS.CARD [FILTER_BY_RANGE [NOT] fromTimestamp toTimestamp] FILTER filter...
```

Returns the number of unique time series that match a given filter set. A time range can optionally be provided to
restrict the results to only series which have samples in the range `[fromTimestamp, toTimestamp]`.

#### Required arguments

**`filter`**

Repeated series selector argument that selects the series to return. At least one filter argument must be provided.

#### Optional Arguments
<code>fromTimestamp</code>

Start timestamp, inclusive. Results will only be returned for series which have samples in the range `[fromTimestamp, toTimestamp]`

<code>toTimestamp</code>

End timestamp, inclusive.

If **`NOT`** is specified before `fromTimestamp` and `toTimestamp`, the result will include only series which do *NOT* have 
samples in the specified range.

#### Return

Integer number of unique time series.

---
### TS.LABELNAMES

#### Syntax

```
TS.LABELNAMES 
    [FILTER_BY_RANGE [NOT] fromTimestamp toTimestamp]
    [LIMIT limit] 
    FILTER selector...
```

Returns a list of label names for select series. If a time range is specified, only labels from series which have data in the date range [`fromTimestamp` .. `toTimestamp`] are returned.

### Required Arguments
<code>fromTimestamp</code>
Repeated series selector argument that selects the series to return. At least one selector argument must be provided.


### Optional Arguments
<code>fromTimestamp</code>

If specified along with `toTimestamp`, this limits the result to only labels from series which
have data in the date range [`fromTimestamp` .. `toTimestamp`]

<code>toTimestamp</code>

If specified along with `fromTimestamp`, this limits the result to only labels from series which
have data in the date range [`fromTimestamp` .. `toTimestamp`]

If **`NOT`** is specified before `fromTimestamp` and `toTimestamp`, the result will include only series which do *NOT* have
samples in the specified range.

<code>limit</code>

The maximum number of label names to return. If not specified, all names are returned.

#### Return

An array of string label names.

```
TS.LABELNAMES LIMIT 10 FILTER up process_start_time_seconds{job="prometheus"}
```
```
1) "__name__",
2) "instance",
3) "job"

```

---
### TS.LABELVALUES

#### Syntax

```
TS.LABELVALUES label 
    [FILTER_BY_RANGE [NOT] fromTimestamp toTimestamp]
    [LIMIT limit] 
    FILTER selector...
```

Returns a list of label values for a provided label name. Optionally a time range can be specified to limit the result to only 
labels from series which have data in the date range [`fromTimestamp` .. `toTimestamp`].

#### Required Arguments

**`filter`**

Repeated series selector argument that selects the series to return. At least one filter argument must be provided.

<code>label</code>

The label name for which to retrieve mut values.


#### Optional Arguments

<code>fromTimestamp</code>

If specified along with `toTimestamp`, this limits the result to only labels from series which
have data in the date range [`fromTimestamp` .. `toTimestamp`]


<code>toTimestamp</code>

If specified along with `fromTimestamp`, this limits the result to only labels from series which
have data in the date range [`fromTimestamp` .. `toTimestamp`]

If **`NOT`** is specified before `fromTimestamp` and `toTimestamp`, the result will include only series which do *NOT* have
samples in the specified range.

<code>limit</code>

The maximum number of label values to return. If not specified, all values are returned.


#### Return

An array of string values.

#### Error

Return an error reply in the following cases:

- Invalid options.
- TODO.

#### Examples

This example queries for the regions collecting p95 latency metrics for the billing service:
```
TS.LABELVALUES region FILTER api_latency_p95{service="billing"}
```
```
1) "us-east-1",
2) "us-east-2"
3) "us-west-1",
4) "us-west-2"
```

Return the device ids of factory sensors that have not reported temperature metrics in the last 5 minutes:

```
TS.LABELVALUES device_id FILTER_BY_RANGE NOT -5m * FILTER iot_sensor{type=temperature,location="factory-floor-1"}
```

```
1) "device_001",
2) "device_042",
3) "device_073"
```
---
### TS.STATS

#### Syntax

```
TS.STATS [LIMIT limit]
```

Returns various cardinality statistics about the time series data.

`limit` - the number of returned items to a given number for each set of statistics. By default, 10 items are returned.

#### Example
```
TS.STATS LIMIT 5
```

```
 1) numSeries
 2) (integer) 508
 3) seriesCountByMetricName
    1) 1) "net_conntrack_dialer_conn_failed_total"
       2) (integer) 20
    2) 1) "prometheus_http_request_duration_seconds_bucket"      
       2) (integer) 20
    3) 1) "http_request_latency_seconds_count"
       2) (integer) 20
 4) labelValueCountByLabelName
    1) 1) "__name__"
       2) (integer) 211
    2) 1) "event"      
       2) (integer) 4
 5) memoryInBytesByLabelName
    1) 1) "__name__"
       2) (integer) 8266
    2) 1) "instance"      
       2) (integer) 28
 6) seriesCountByLabelValuePair
    1) 1) "job=prometheus"
       2) (integer) 430
    2) 1) "instance=localhost:9090"      
       2) (integer) 425 
```
The fields returned are:
  * **numSeries**: total number of time series.
  * **numLabelPairs**: total number of unique label value pairs.
  * **seriesCountByMetricName**: metrics names and the count of the series with that name.
  * **labelValueCountByLabelName**: label names and their value count.
  * **memoryInBytesByLabelName**: the label names and memory used in bytes. Memory usage is calculated by adding the length of all values for a given label name.
  * **seriesCountByLabelPair**: label value pairs and their series count.

---
### TS.JOIN

#### Syntax

```
TS.JOIN leftKey rightKey fromTimestamp toTimestamp
    [[INNER] | [FULL] | [LEFT] | [RIGHT] | [ANTI] | [SEMI] | [ASOF [PREVIOUS | NEXT | NEAREST] [tolerance] ALLOW_EXACT_MATCH]]
    [FILTER_BY_TS ts...]
    [FILTER_BY_VALUE min max]
    [COUNT count]
    [REDUCE reducer]
    [AGGREGATION aggregator bucketDuration [CONDITION op value] [ALIGN align] [BUCKETTIMESTAMP timestamp] [EMPTY]]
```

Join two time series on sample timestamps. Performs an INNER join by default.

#### Required arguments

<summary><code>leftKey</code>
is the key name for the time series being joined.
</summary>

<summary><code>rightKey</code>
is the key name for the right time series.
</summary>

Both keys must have been created before `TS.JOIN` is called.

<summary><code>fromTimestamp</code>
`fromTimestamp` is the first timestamp or relative delta from the current time of the request range.
</summary>

<summary><code>toTimestamp</code>
`toTimestamp` is the last timestamp of the requested range, or a relative delta from `fromTimestamp`
</summary>


#### Optional arguments

<summary><code>LEFT</code>
outputs the matching samples between both tables. In case no samples match from the left series, it returns 
those items with null values.
</summary>

<summary><code>RIGHT</code>
outputs all samples in the right series. In case no samples match from the left series, it returns
those items with null values.

</summary>

<summary><code>INNER</code>
a row is generated for samples with matching timestamps in the selected range.
</summary>

<summary><code>ANTI</code>
returns samples for which no matching timestamp exists in the `right` series.
</summary>

<summary><code>SEMI</code>
returns samples for which no corresponding timestamp exists in the `right` series. It does not return any 
values from the right table.
</summary>

<summary><code>FULL</code>
returns samples from both left and right series. If no matching rows exist for the row in the left series, the value of 
the right series will have nulls. Correspondingly, the value of the left series will have nulls if there are no matching 
rows for the sample in the right series.
</summary>

<summary><code>ASOF</code>
match each sample in the left series with the closest preceding or following sample in the right series based on
timestamps. They are particularly useful for analyzing time-series data where records from different sources may not have
perfectly aligned timestamps. ASOF joins solve the problem of finding the value of a varying property at a specific point in time.
</summary>

#### How ASOF Joins Work

When using `ASOF` joins, the join operation looks for the closest matching timestamp in the right series for each
timestamp in the left series, based on the specified `direction` and `tolerance`.

`direction` specifies how to find the closest match:
- `PREVIOUS` (default) selects the last row in the right series whose timeseries is less than or equal to the left’s timestamp.
- `NEXT` (default) selects the first row in the right series whose timestamp is greater than or equal to the left’s timestamp.
- `NEAREST` selects the last row in the right series whose timestamp is nearest to the left’s timestamp.

`tolerance` sets a limit on how far apart the timestamps can be while still considering them a match.
The tolerance can be specified as:
- An integer representing milliseconds
- A duration specified as a string, e.g., 2m

`ALLOW_EXACT_MATCH` is a boolean flag that determines whether to allow exact matches between the left and right series.

If not specified, there is no tolerance limit (equivalent to an infinite tolerance). When set, JOIN ASOF will only match
keys within the specified tolerance range. Any potential matches outside this range will be treated as no match.


#### Example
Suppose we want to get the spreads between buy and sell trades in a trading application

```
TS.JOIN trades:buy trades:sell -1hr * ASOF NEAREST 2ms ALLOW_EXACT_MATCH REDUCE sub
```

The result has all samples from the `buy` series joined with samples from the `sell` series. For each timestamp from the
`buy` series, the query looks for a timestamp that nearest to it from the `sell` series, within a tolerance of
2 milliseconds. If no matching timestamp is found, NULL is inserted.

The `sub` transform function is then supplied to subtract the `sell` value from the `buy` value for each sample returned.

The tolerance parameter is particularly useful when working with time series data where exact matches are rare, but you
want to find the closest match within a reasonable time frame. It helps prevent incorrect matches that might occur if the 
nearest available data point is too far away in time or value.

<code>`count`</code> the maximum number of samples to return.

If used with aggregation, this specifies the number of returned buckets as opposed to the number of samples

**`reducer`** 

performs an operation on the value in each returned row.

`reducer` takes one of the following types:

| `reducer`    | Description                                                                |
|--------------|----------------------------------------------------------------------------| 
| `abs_diff`   | abs(`left` - `right`)                                                      |
| `avg`        | Arithmetic mean of both values                                             |
| `cmp`        | -1 if `left` < `right`, 0 if `left` == `right`, and 1 if `left` > `right`. |
| `coalesce`   | Return the first non-NaN item. If both are NaN, it returns NaN.            |
| `div`        | `left` / `right`                                                           |
| `eq`         | Returns 1 if `left` == `right`, 0 otherwise                                |
| `gt`         | Returns 1 if `left` > `right`, otherwise returns 0                         |
| `gte`        | Returns 1 if left is greater than or equals right, otherwise returns 0     |
| `lt`         | Returns 1 if `left` > `right`, otherwise returns 0                         |
| `lte`        | Returns 1 if `left` is less than or equals `right`, otherwise returns 0    |
| `min`        | Minimum value                                                              |
| `mod`        | `left` % `right`                                                           |
| `max`        | Maximum value                                                              | 
| `mul`        | `left` * `right`                                                           |
| `ne`         | Returns 1 if `left` != `right`, otherwise returns 0                        |
| `pct_change` | The percentage change of `right` over `left`                               |
| `pow`        | `left` ^ `right`                                                           |
| `sgn_diff`   | sgn(`left` - `right`)                                                      |
| `sub`        | `left` - `right`                                                           |
| `sum`        | `left` + `right`                                                           |

#### Return value

Returns one of these replies:

- @simple-string-reply - `OK` if executed correctly
- @error-reply on error (invalid arguments, wrong key type, etc.), when `sourceKey` does not exist, when `destKey` does not exist, 
when `sourceKey` is already a destination of a compaction rule, when `destKey` is already a source or a destination of a compaction rule, or when `sourceKey` and `destKey` are identical

#### Examples


Create a time series to store the temperatures measured in Mexico City and Toronto.

```
127.0.0.1:6379> TS.CREATE temp:CDMX temp{location="CDMX"}
OK
127.0.0.1:6379> TS.CREATE temp:TOR temp{location="TOR"}
OK
```

... Add data


Next, run the join.

```
127.0.0.1:6379> TS.JOIN temp:CDMX temp:TOR REDUCE min 
```
---
### TS.OUTLIERS

#### Syntax
```
TS.OUTLIERS key fromTimestamp toTimestamp
  [OUTPUT <FULL | SIMPLE | CLEANED>]
  [DIRECTION <POSITIVE | NEGATIVE | BOTH>]
  [SEASONALITY [AUTO | period1 ...]]]
  METHOD method [method-options]
```

Detect outliers in a time series using the specified method.

[Examples](#examples)

## Required arguments

<details open>
<summary><code>key</code></summary>

Key name for the time series.
</details>

<details open>
<summary><code>fromTimestamp</code></summary>

The start timestamp for the range query (inclusive).

Use `-` to denote the earliest timestamp in the series.
</details>

<details open>
<summary><code>toTimestamp</code></summary>

End timestamp for the range query (inclusive).

Use `+` to denote the latest timestamp in the series.
</details>

#### Optional arguments

<details open>
<summary><code>OUTPUT</code></summary>

Controls the output format. One of:

* `SIMPLE` (default) - Returns only the detected outliers
* `FULL` - Returns all samples with scores, plus anomaly metadata
* `CLEANED` - Returns normal samples (excluding outliers) and detected anomalies separately

</details>

<details open>
<summary><code>DIRECTION</code></summary>

Filter anomalies by direction. One of:

* `BOTH` (default) - Detect outliers in both directions
* `POSITIVE` - Detect only positive outliers (spikes above normal)
* `NEGATIVE` - Detect only negative outliers (dips below normal)

</details>

<details open>
<summary><code>SEASONALITY</code></summary>

Adjust for seasonal patterns before outlier detection.

```
SEASONALITY AUTO | period ...
```

* For a single period: Uses STL (Seasonal-Trend decomposition using LOESS)
* For multiple periods: Uses MSTL (Multiple Seasonal-Trend decomposition using LOESS)
* Periods must be unique positive integers
* If `AUTO` is specified, the algorithm will attempt to automatically detect the dominant seasonality period(s) in the data using periodograms.
* Requires at least `2 × max(period)` data points

**Example:** `SEASONALITY 24 168` adjusts for daily (24 hours) and weekly (168 hours) patterns in hourly data.
</details>

<details open>
<summary><code>METHOD</code></summary>

Specifies the outlier detection algorithm. **Required**.

#### CUSUM

Cumulative Sum statistical process control method.

```
METHOD CUSUM
```

Detects shifts in the mean by accumulating deviations from the target. No additional options.

---

#### EWMA

Exponentially Weighted Moving Average.

```
METHOD EWMA [ALPHA alpha]
```

**Options:**

* `ALPHA` - Smoothing factor (0 < alpha ≤ 1). Default: `0.2`. Higher values give more weight to recent observations.

---

#### IQR

Interquartile Range method.

```
METHOD IQR [THRESHOLD k]
```

**Options:**

* `THRESHOLD` - IQR multiplier for fence boundaries. Default: `1.5`. Values beyond `Q1 - k×IQR` or `Q3 + k×IQR` are
  outliers.

---

#### ZSCORE

Standard Z-score method.

```
METHOD ZSCORE [THRESHOLD threshold]
```

**Options:**

* `THRESHOLD` - Number of standard deviations from mean. Default: `3.0`. Values beyond `mean ± threshold × σ` are
  outliers.

---

#### MODIFIED-ZSCORE

Modified Z-score using Median Absolute Deviation (MAD).

```
METHOD MODIFIED-ZSCORE [THRESHOLD threshold]
```

**Options:**

* `THRESHOLD` - Modified Z-score threshold. Default: `3.5`. More robust to outliers than standard Z-score.

Uses `0.6745 × (value - median) / MAD` for scoring.

---

#### SMOOTHED-ZSCORE

Smoothed Z-score with adaptive filtering.

```
METHOD SMOOTHED-ZSCORE [THRESHOLD threshold] [LAG lag] [INFLUENCE influence]
```

**Options:**

* `THRESHOLD` - Z-score threshold. Default: `3.0`
* `LAG` - Window size for moving average. Default: `5`
* `INFLUENCE` - Weight of anomalies in calculations (0-1). Default: `0.5`. Lower values reduce the anomaly's impact on
  subsequent detection.

---

#### MAD

Median Absolute Deviation.

```
METHOD MAD [ESTIMATOR estimator] [THRESHOLD k]
```

**Options:**

* `ESTIMATOR` - MAD calculation method: `SIMPLE`, `HARRELL-DAVIS`, or `QUANTILE`. Default: `SIMPLE`
* `THRESHOLD` - MAD multiplier. Default: `3.0`

---

#### DOUBLE-MAD

Double MAD for asymmetric distributions.

```
METHOD DOUBLE-MAD [ESTIMATOR estimator] [THRESHOLD k]
```

**Options:**

* `ESTIMATOR` - MAD calculation method: `SIMPLE`, `HARRELL-DAVIS`, or `QUANTILE`. Default: `SIMPLE`
* `THRESHOLD` - MAD multiplier. Default: `3.0`

Separately analyzes values above and below the median for better handling of skewed data.

---

#### RCF

Random Cut Forest algorithm (AWS implementation).

```
METHOD RCF [NUM_TREES trees] [SAMPLE_SIZE size] [THRESHOLD threshold]
           [SHINGLE_SIZE shingle] [OUTPUT_AFTER warmup] [DECAY decay]
```

**Options:**

* `NUM_TREES` - Number of trees in forest. Default: `100`
* `SAMPLE_SIZE` - Sample size per tree. Default: `256`
* `THRESHOLD` - Anomaly score threshold. Default: `1.0`
* `SHINGLE_SIZE` - Sliding window size. Default: `1`. Use values > 1 for contextual anomalies.
* `OUTPUT_AFTER` - Warmup period (samples). Default: `32`
* `DECAY` - Time decay factor (0-1). Default: `0.1`. Controls how quickly old data is forgotten.

</details>

## Return value

Returns anomaly information based on the `OUTPUT` format:

#### SIMPLE format

**Array reply:** Each anomaly as `[timestamp, value, signal, score]`

* `timestamp` - Sample timestamp (integer)
* `value` - Sample value (float)
* `signal` - Anomaly direction: `1` (positive) or `-1` (negative)
* `score` - Anomaly score (0.0-1.0, higher = stronger anomaly)

#### FULL format

**Map reply** with keys:

* `method` - Detection method name (string)
* `threshold` - Threshold value used (float)
* `samples` - All samples with scores: `[[timestamp, value, score], ...]`
* `scores` - Array of anomaly scores for all samples
* `outliers` - Array of detected outliers (same format as SIMPLE)
* `method_info` - Algorithm-specific metadata (map, if available):
    * For IQR: `lower_fence`, `upper_fence`
    * For SPC methods (CUSUM, EWMA): `control_limits`, `center_line`

#### CLEANED format

**Map reply** with keys:

* `samples` - Normal samples (excluding outliers): `[[timestamp, value], ...]`
* `outliers` - Detected anomalies: `[[timestamp, value, signal, score], ...]`

## Examples

<details open>
<summary><b>Detect outliers using Modified Z-score</b></summary>

Detect outliers in temperature sensor data with the default threshold:

```valkey-cli
127.0.0.1:6379> TS.OUTLIERS temperature:sensor1 - + METHOD MODIFIED-ZSCORE
1) 1) (integer) 1609459200000
   2) "45.7"
   3) (integer) 1
   4) "0.87"
2) 1) (integer) 1609545600000
   2) "-5.2"
   3) (integer) -1
   4) "0.92"
```

</details>

<details open>
<summary><b>Detect with daily seasonality adjustment</b></summary>

Analyze sales data with 24-hour seasonality:

```valkey-cli
127.0.0.1:6379> TS.OUTLIERS sales:daily 1640000000000 1650000000000 SEASONALITY 24 METHOD ZSCORE THRESHOLD 3.0
1) 1) (integer) 1641234567000
   2) "1547.8"
   3) (integer) 1
   4) "0.93"
```

</details>

<details open>
<summary><b>Detect only positive spikes with full output</b></summary>

Get detailed analysis with metadata for API request spikes:

```valkey-cli
127.0.0.1:6379> TS.OUTLIERS requests:api - + OUTPUT FULL DIRECTION POSITIVE METHOD IQR THRESHOLD 1.5
1) "method"
2) "InterquartileRange"
3) "threshold"
4) "1.5"
5) "samples"
6) 1) 1) (integer) 1609459200000
      2) "125.4"
      3) "0.15"
   2) 1) (integer) 1609462800000
      2) "135.2"
      3) "0.18"
   ...
7) "scores"
8) 1) "0.15"
   2) "0.18"
   ...
9) "outliers"
10) 1) 1) (integer) 1609462800000
       2) "850.2"
       3) (integer) 1
       4) "0.94"
11) "method_info"
12) 1) "lower_fence"
    2) "-50.3"
    3) "upper_fence"
    4) "250.7"
```

</details>

<details open>
<summary><b>Multiple seasonality with trend adjustment</b></summary>

Handle both daily and weekly patterns in hourly traffic data:

```valkey-cli
127.0.0.1:6379> TS.OUTLIERS traffic:hourly - + SEASONALITY 24 168 METHOD MODIFIED-ZSCORE THRESHOLD 3.5
1) 1) (integer) 1641234567000
   2) "8547.2"
   3) (integer) 1
   4) "0.89"
```

</details>

<details open>
<summary><b>Get cleaned data (anomalies removed)</b></summary>

Extract normal operating data and anomalies separately:

```valkey-cli
127.0.0.1:6379> TS.OUTLIERS cpu:usage - + OUTPUT CLEANED DIRECTION BOTH METHOD EWMA ALPHA 0.3
1) "samples"
2) 1) 1) (integer) 1609459200000
      2) "45.2"
   2) 1) (integer) 1609462800000
      2) "47.8"
   ...
3) "outliers"
4) 1) 1) (integer) 1609470000000
      2) "98.5"
      3) (integer) 1
      4) "0.91"
```

</details>

<details open>
<summary><b>Random Cut Forest for contextual anomalies</b></summary>

Detect pattern-based anomalies using a sliding window:

```valkey-cli
127.0.0.1:6379> TS.OUTLIERS metrics:response_time - + METHOD RCF NUM_TREES 150 SHINGLE_SIZE 8 THRESHOLD 1.2
1) 1) (integer) 1641234567000
   2) "2547.3"
   3) (integer) 1
   4) "0.88"
```

</details>

## Notes

* **Minimum data requirements:**
    * At least 3 data points required for analysis
    * For `SEASONALITY`: requires at least `2 × max(period)` samples
* **Seasonality constraints:**
    * Maximum 4 periods allowed
    * Periods must be unique positive integers
    * Automatically sorted in ascending order
* **Score normalization:**
    * All anomaly scores are normalized to [0.0, 1.0] range
    * Higher scores indicate stronger anomalies
* **Timestamp preservation:**
    * Output timestamps match original series timestamps
* **Performance tips:**
    * Use `DIRECTION` to filter results when only interested in one type of anomaly
    * Single seasonality periods are faster than multiple
    * RCF is more computationally expensive but better for complex patterns

---
### Differences from RedisTimeSeries

* Our command parser is more lenient than RedisTimeSeries. For example, the order of optional arguments does not matter for 
  commands like TS.CREATE and TS.ALTER when not specifying variadic arguments like LABELS.
* We support Prometheus style selectors in TS.QUERYINDEX, TS.MGET, TS.MRANGE, and TS.MREVRANGE.
* We support new index metadata commands: TS.CARD, TS.LABELNAMES, TS.LABELVALUES, TS.STATS.
* Additional aggregation functions are supported:
  * `ALL` — Returns 1.0 if every sample in the bucket matches a condition.
  * `ANY` — Returns 1.0 if any sample in the bucket matches a condition.
  * `COUNTIF` - counts the number of samples in a bucket that match a condition.
  * `INCREASE` - calculates the total increase in value of a counter over a specified time duration
  * `RATE` - determines the average per-second growth rate of a counter across a given time frame (handling counter-resets)
  * `IRATE` - calculates the average per-second growth rate of a counter across a given time frame, while handling counter-resets.
  * `SHARE` — Fraction [0..1) of samples matching a condition.
  * `SUMIF` - sums the values of samples in a bucket that match a condition.
* We support a new TS.JOIN command to join two time series on sample timestamps.

### Unsupported Features

We do not currently support the TWA (Time-Weighted Average) aggregation function.


### Possible Future Enhancements

* Work is in progress to support [PromQL](https://prometheus.io/docs/prometheus/latest/querying/basics/) as a query language, 
including transform, aggregation, and rollup function support. A stretch goal is to support alerts and notifications based on the query results.

* We may support a tiered storage model where data is moved to higher compression chunks (with higher access latency) after a certain period of time. 

* Support more complex analysis, like forecasting and anomaly detection.


### Configurations

The default properties for TimeSeries can be controlled using configs. The values of the
configs below are only used on a timeseries if the user does not specify the properties explicitly. Example: Using
TS.CREATE or TS.ADD can override the default properties.

Supported Module configurations:
- **`ts-retention-policy`** : The default retention policy for time series (ms). Default to `0`, which means no retention policy.
- **`ts-duplicate-policy`**: The default duplicate policy for time series. Default to `LAST`, which means the last sample is kept when duplicates are added.
- **`ts-chunk-size-bytes`**: Controls the default chunk memory capacity. When create operations (`TS.CREATE`/`TS.ADD`/`TS.INCRBY`/`TS.DECRBY`) are used, the timeseries created
    will use the capacity specified by this setting.
- **`ts-encoding`**: The default encoding for time series. Default to `COMPRESSED`, which means the samples are compressed using Gorilla XOR compression.
- **`ts-ignore-max-time-diff`**: The distance in time between samples below which they are considered duplicated. Default to 0, which means no deduplication is performed.
- **`ts-ignore-max-val-diff`**: The value delta between samples below which they are considered duplicated.
- **`ts-compaction-policy`**: Compaction rules added by default to series created by `TS.ADD`, `TS.INCRBY` and `TS.DECRBY`. 
- **`ts-cluster-map-expiration-ms`**: The cluster map expiration time.

### ACL

The ValkeyTimeSeries module will introduce a new ACL category - `@timeseries`.

There are four existing ACL categories that are updated to include new TimeSeries commands: `@read`, `@write`, `@fast`, `@slow`.

### Keyspace Event Notification

Every timeseries-based write command (that involves mutation as explained in the section above) will be made to publish
a keyspace event after the data is mutated. Commands include: TS.CREATE, TS.ADD, TS.MADD.

* Event type: VALKEYMODULE_NOTIFY_GENERIC
* Event name: One of the two event names will be published based on the command and scenario:
 * ts.add
 * ts.alter
 * ts.add:dest
 * ts.create
 * ts.createrule:dest
 * ts.createrule:src
 * ts.del
 * ts.madd


Users can subscribe to the time series events via the standard keyspace event pub/sub. For example,

```text
1. enable keyspace event notifications:
    valkey-cli config set notify-keyspace-events KEA
2. subscribe to keyspace & keyevent event channels:
    valkey-cli psubscribe '__key*__:*'
```

## References
* [valkey-timeseries GitHub Repo](https://github.com/ccollie/valkey-timeseries)
* [RedisTimeSeries](https://oss.redislabs.com/redistimeseries/)
* [Prometheus](https://prometheus.io/)
* [Adaptive Radix Tree (ART)](https://db.in.tum.de/~leis/papers/ART.pdf)
* [Roaring Bitmaps](https://roaringbitmap.org/about/)
