# Valkey TimeSeries Overview

Valkey TimeSeries is a module for [Valkey](https://valkey.io) that adds a high-performance, in-memory time series data
type. It is designed to handle high-velocity ingestion and real-time querying of metric data, leveraging the speed of
memory and the
versatility of the Valkey ecosystem. The API is a superset of RedisTimeSeries, offering compatibility with existing
tools while
providing optimizations for Valkey.

## Use Cases

The module is architected for scenarios requiring low-latency storage and retrieval of time-stamped numerical data:

* **Application Monitoring:** Storage of metrics from servers, containers, or distributed applications (CPU, memory,
  latency).
* **IoT Telemetry:** Ingesting sensor data from edge devices with high throughput.
* **Real-time Analytics:** Calculation of moving averages, max/min values, and aggregating data streams for dashboards.
* **Financial Data:** Storing tick data, price history, or trading volume for rapid analysis.

## Supported Commands

The command set generally follows the `TS.<COMMAND>` pattern.

### Management

* `TS.CREATE`: Create a new time series with specific retention, encoding, and chunk size policies.
* `TS.ALTER`: Modify the configuration of an existing series.
* `TS.DEL`: Remove an entire time series from the database.
* `TS.MDEL`: Remove multiple time series matching a filter from the database.

### Ingestion

* `TS.ADD`: Append a new sample (timestamp, value) to a series.
* `TS.MADD`: Append samples to multiple series atomically.
* `TS.ADDBULK`: Append multiple samples to a single series in one command.
* `TS.INCRBY` / `TS.DECRBY`: Increment or decrement the value of the latest sample.
* `TS.DEL`: Delete samples within a specific time range.

### Querying

* `TS.GET`: Retrieve the last sample of a series.
* `TS.MGET`: Retrieve the last sample from multiple series matching a filter.
* `TS.RANGE`: Query a range of samples from a single series.
* `TS.MRANGE`: Query ranges across multiple series based on filters.
* `TS.READ`: Read samples at or after a timestamp, optionally filtered by a value condition and
  optionally blocking until enough matching samples arrive. The streaming counterpart to
  `TS.RANGE`, for tailing a series and for alerting on it.

### Compaction & Rules

* `TS.CREATERULE`: Create a downsampling rule to aggregate data from a source key to a destination key over fixed time
  buckets.
* `TS.DELETERULE`: Remove an existing compaction rule.

### Replication

Series and compaction destinations are shaped by the primary. Every series creation — an
explicit `TS.CREATE` or the auto-create of a write command (`TS.ADD`, `TS.INCRBY`/`TS.DECRBY`,
`TS.ADDBULK`) — is replicated as a `TS.CREATE` carrying the primary's resolved options (retention,
encoding, chunk size, duplicate policy, `IGNORE`, rounding, labels), ahead of the write. The
destinations `ts-compaction-policy` creates for an auto-created series are replicated as explicit
`TS.CREATE` + `TS.CREATERULE` pairs, in the primary's rule order, before the write; a replica or
an AOF replay never applies the policy itself. A replica's own `ts-compaction-policy`,
`ts-retention-policy`, `ts-encoding`, chunk size and rounding defaults therefore never shape
replicated data. A policy destination key that already exists is left untouched and its rule
skipped (with a warning naming both keys), on every node alike. `ts-compatibility-mode` is
consulted by the node answering a read, so it must be set identically on every node of a
replication group.

A series without rounding is replicated with `DECIMAL_DIGITS none`, because a bare `TS.CREATE`
would otherwise take the receiving node's configured rounding default. `none` is a spelling
specific to this module, not part of the RedisTimeSeries command set; clients and `TS.ALTER` may
use it too (see `docs/commands/ts.create.md`).

Compaction is recomputed on every node from the replicated source writes, as in
RedisTimeSeries: destinations are not shipped over the replication link. A replica therefore
produces exactly its primary's destination values only while both run the same aggregation
arithmetic. When a release note says a release changes aggregation results (it will say so
explicitly), upgrade the primary and its replicas together and full-resync the replicas
afterwards (`REPLICAOF NO ONE` then `REPLICAOF <primary>` on each), or accept that rollups
written during the mixed-version window may differ in their last digits between nodes.

**Mixed versions.** The replicated stream and the AOF changed with this release: creations are
now spelled out as a resolved `TS.CREATE` (including `DECIMAL_DIGITS none`), and policy
destinations arrive as explicit `TS.CREATE` + `TS.CREATERULE` pairs. A replica or AOF loader
running an older module build cannot parse that stream. In the other direction, an AOF tail or
replication stream written by a build from before this release replays its auto-creates without
the policy rules, because the new loader no longer applies the policy itself. Upgrade the primary
and its replicas together, and rewrite the AOF (`BGREWRITEAOF`) after upgrading so no old tail is
left to replay.

**Keyspace events.** A replica fires `ts.create` for every replicated creation, and
`ts.createrule:src` / `ts.createrule:dest` for the rules of policy destinations. The primary
emits none of these for an auto-create or a policy destination (only an explicit `TS.CREATE` and
`TS.CREATERULE` notify there), so event subscriptions on a replica see more creation events than
the same subscription on the primary.

**Failure recovery.** A rule application that fails (for example, an error while writing to its
destination) marks the rule `dirty`; the next write to the source rebuilds the rule's open bucket
from the source samples before applying itself. The command that hit the failure still succeeds
and replicates — its write to the source is already committed — and the failure is logged. A
closed bucket lost to the failure is not repaired. `INFO ts_compaction` reports `ts_compaction_failures` (applications that failed) and
`ts_compaction_state_rebuilds` (dirty rules rebuilt), both since module start, and
`TS.INFO key DEBUG` shows each rule's open-bucket state under `rulesState` (the last element of
each entry is the `dirty` flag). A non-zero failure counter is the signal that a destination may
be missing a bucket.

`ts-compaction-policy` (`agg:bucket:retention[:align][|regex];…`) accepts only aggregations that
take no condition: `all`, `any`, `countif`, `sumif`, `share` and `none` are rejected (at
`CONFIG SET` and at module load), since the policy grammar cannot carry their condition. Create
such rules with `TS.CREATERULE` and an inline condition (e.g. `countif>5`) instead.

### Metadata & Indexing

* `TS.INFO`: Retrieve detailed information and statistics about a specific time series.
* `TS.QUERYINDEX`: Retrieve all series keys matching a label filter.
* `TS.QUERYLABELS`: Retrieve all label names, or all values of a given label, for series matching a filter.
* `TS.CARD`: Get the cardinality of the index for a specific label filter.
* `TS.LABELNAMES`: Get all label names used in the index.
* `TS.METRICNAMES`: Search metric names with substring and optional fuzzy matching.
* `TS.LABELVALUES`: Get all values for a specific label name in the index.
* `TS.LABELSTATS`: Get statistics about label usage in the index.

### Anomaly Detection

* `TS.OUTLIERS`: Identify outliers in a series based on a specified algorithm and parameters.

## Indexes

Valkey TimeSeries uses a label-based indexing system separate from the key space.

* **Labels:** Every time series can be associated with a set of `field=value` labels (e.g., `region=us-east`,
  `env=prod`).
* **Secondary Indexing:** These labels form a secondary index that allows for efficient discovery of keys. Unlike
  standard Valkey keys which are accessed by the exact name, time series can be grouped and queried dynamically.
* **Cardinality:** Commands like `TS.CARD` provide insights into the cardinality of the index to help monitor memory
  usage and query efficiency.

`valkey-timeseries` maintains a per-node, per-database inverted index to map labels to series.
The indexes themselves exist separate from the Valkey database itself. Applications don't directly modify an index,
rather
mutation operations on keys within the declared keyspace of an index automatically update the index with the labels of
that key.

# Index Replication

Indexes are node-local. Each node, regardless of whether it's a primary or a replica, maintains its own index
independently. Indexes on replicas are updated by key mutations transmitted on the replication channel and thus are
subject to replication lag just like the Valkey database itself.

## Querying

Data retrieval is highly flexible, supporting filtering, aggregation, and arithmetic operations.

* **Filtering:** Queries use label matchers (e.g., `TS.MRANGE ... FILTER region=us-east metric!=cpu`) to select the
  target series.
* **Aggregation:** Raw samples can be aggregated at query time using functions such as `avg`, `sum`, `min`, `max`,
  `count`, `first`, `last`, `std.p`, `std.s`, `var.p`, and `var.s`.
* **Alignment:** Time buckets can be aligned to specific intervals to ensure consistent reporting across different
  series.

# Cluster Mode

Timeseries fully supports cluster mode and uses Valkey's cluster bus and protobuf for intra-cluster communication.

In cluster mode, Valkey distributes keys according to the hash algorithm of the keyname. This placement of data is not
affected by the presence of the timeseries module or any timeseries indexes. Since timeseries commands operate at the
index level -- not the key level -- valkey-timeseries is responsible for the distribution of data, performing
intra-cluster RPC
to execute commands as needed. Thus, the application interface to valkey-timeseries operates the same in cluster and
non-cluster mode.

Timeseries uses a simple architecture where index definitions are replicated on every node, but the corresponding index
only contains the data which is co-resident on that node. Index update operations remain wholly local to a node and will
scale horizontally (save/restore operations also wholly node local). Vertical scaling is also effective because of the
multithreaded architecture.

Query operations are performed by one node of each shard on its local index, and the results are transparently merged to
form a full command response. Query operations are subject to increasing overhead as the cluster shard count increases,
meaning that query operations may scale sub-linearly with increasing shard count.

## Aggregation push-down (TS.MRANGE / TS.MREVRANGE)

Because each time series lives entirely on one shard, per-series `AGGREGATION` for `TS.MRANGE`/`TS.MREVRANGE` is
computed shard-side: shards return aggregated buckets instead of raw samples, cutting network transfer and coordinator
CPU roughly by the ratio of samples per bucket. Results are exact for every aggregator type.

`GROUPBY`/`REDUCE` queries are additionally pre-reduced shard-side when the reducer is decomposable (`sum`, `count`,
`countall`, `countnan`, `min`, `max`, `range`, `avg`, `std.p`, `std.s`, `var.p`, `var.s`, `first`, `last`, including
their filtered variants such as `countif`/`sumif` with an inline condition): each shard ships one partial-state series
per (group, shard), and the coordinator merges and finalizes them per bucket. Order-sensitive reducers (`increase`,
`irate`) automatically fall back to per-series bucket transport. `COUNT` and reversal (`TS.MREVRANGE`) are always
applied at the coordinator.

Multi-aggregation queries (`AGGREGATION avg,max,count …`) participate in both push-downs: per-series buckets travel as
one compressed chunk per aggregation column, and group partials carry one reducer state per column per bucket, reduced
column-wise with the same `REDUCE` type.

`COUNT` is likewise applied shard-side as a head/tail pre-filter bounding transfer to `O(count)` rows per series (or
per group partial) — most valuable for `TS.MREVRANGE … COUNT n` ("last n points") queries. The coordinator always
re-applies `COUNT` as the final authority.

Mixed-version clusters are handled by a compatibility handshake (`fanout-compatibility-handshake.md`): shards
echo which push-down flags they honored, and the coordinator compensates per response — data from a peer that did not
apply push-down is aggregated and pre-reduced coordinator-side before merging. Rolling upgrades therefore need no
special configuration; a lagging node only costs extra transfer and coordinator CPU for its own slice of each query.

Push-down is controlled by the boolean config `ts-fanout-aggregation-pushdown` (default `yes`, changeable at runtime
via `CONFIG SET`). Only the coordinator consults it; shards obey the request. It is not a mixed-version safety knob —
version skew is handled automatically by the handshake above — but an emergency/diagnostic escape hatch: flipping it
off reverts every query to coordinator-side aggregation without a module rollback.
