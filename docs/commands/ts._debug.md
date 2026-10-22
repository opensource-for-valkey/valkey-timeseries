# TS._DEBUG

Internal diagnostic and introspection tool for the valkey-timeseries module.

> **Note:** `TS._DEBUG` is intended for operators and developers. Its subcommands, output formats, and behavior may
> change between versions without notice. Do not rely on this command in production application logic.

## Enabling

`TS._DEBUG` is disabled by default. Every subcommand is rejected until the `debug-mode`
configuration parameter is enabled:

```
TS._DEBUG HELP
(error) TSDB: TS._DEBUG is disabled. Set the 'ts.debug-mode' configuration parameter to yes to enable it
```

Enable it at runtime:

```
CONFIG SET ts.debug-mode yes
```

or at startup, as a module load argument (unprefixed there — the `ts.` prefix applies only to
`CONFIG GET`/`CONFIG SET`):

```
loadmodule /path/to/libvalkey_timeseries.so debug-mode yes
```

The setting takes effect immediately and can be turned back off with `CONFIG SET ts.debug-mode no`.

## Syntax

````aiignore
TS._DEBUG <subcommand> [arguments]</subcommand>
````

## Subcommands

| Subcommand        | Description                                             |
|-------------------|---------------------------------------------------------|
| `HELP`            | Display available subcommands and brief descriptions    |
| `STATS`           | Report this node's module metrics, or reset them        |
| `STRINGPOOLSTATS` | Report statistics for the global string interning pool  |
| `INDEXMEMORY`     | Report the label index's heap footprint (fields as in `INFO ts_index`; each `*Bytes` field is followed by a `*BytesHuman` string such as `1.21M`) |
| `INFLIGHT`        | List this node's fan-out requests still waiting on peers |
| `QUERYINDEX`      | Query this node's local index only, without cluster fan-out |
| `LIST_CONFIGS`    | List module configuration parameters and current values |
| `PANIC_NEXT_ANALYSIS_JOB` | Make the next background analysis job (`TS.OUTLIERS` on a large range) panic, to test that its client still gets an error reply |

### Reply protocol

Replies made of named fields are RESP3 maps. A RESP2 client gets the same pairs as a flat array
of alternating keys and values, in the same order. Over RESP2, doubles arrive as bulk strings.

Field names are camelCase, like `TS.INFO`'s. Sizes are in bytes, and a percentage (0–100) ends in
`Pct`; any other unit is named at the end of the field (`ageMs`). Metric names in `STATS` are not
field names but [OpenMetrics](https://prometheus.io/docs/practices/naming/) identifiers,
snake_case with a unit suffix (`_seconds`, `_bytes`) and `_total` on counters. They are the same
everywhere a metric appears, including the `name` field of a `VERBOSE` entry.

### Cluster scope

Every subcommand reports, or acts on, the node you are connected to. `STATS`, `STATS RESET`,
`STRINGPOOLSTATS` and `INDEXMEMORY` also take a `CLUSTER` keyword, which fans the request out
and combines the answers as each subcommand describes. Every node asked must have `debug-mode`
enabled, or the command fails. On a standalone server `CLUSTER` is an error
(`TSDB: CLUSTER requires cluster mode`), so a given request always gets the same reply layout.

---

### TS._DEBUG HELP

Displays the available `TS._DEBUG` subcommands and their descriptions.

### Syntax

```bash
TS._DEBUG HELP
```

### Return Value

A map from each command synopsis to its description.

### Example

```
TS._DEBUG HELP
```

```
 1) "TS._DEBUG STATS [section ...] [VERBOSE] [CLUSTER]"
 2) "Show this node's module metrics, optionally for the named sections only (VERBOSE adds kind and description); CLUSTER sums counters and histograms over every node and lists gauges per node"
 3) "TS._DEBUG STATS RESET [CLUSTER]"
 4) "Start this node's metric counters and histograms over (gauges are left alone), or every node's with CLUSTER"
 ...
```

---

### TS._DEBUG STATS

Reports module metrics: events that happen inside a command or in background work, which the
server's own `INFO commandstats` / `latencystats` cannot see. Counters are collected whether or
not `debug-mode` is on; only reading them needs it.

Each node counts what happens on it, and `STATS` reports the connected node's metrics. With
`CLUSTER` the command fans out to every node, replicas included, and combines their metrics:
counters and histograms are summed, and each gauge's value becomes a map with one value per node
(summing refresh intervals or queue depths across nodes would hide which node they describe).
Every node must have `debug-mode` enabled, or a `CLUSTER` request fails.

The cluster view counts itself: the fan-out that collects it is an operation on the coordinator,
like any other. A reply is not an atomic snapshot, on one node or across nodes: two values may
straddle an update.

### Syntax

```bash
TS._DEBUG STATS [section ...] [VERBOSE] [CLUSTER]
TS._DEBUG STATS RESET [CLUSTER]
```

### Arguments

| Argument  | Required | Description                                                                                   |
|-----------|----------|-----------------------------------------------------------------------------------------------|
| `section` | No       | Report only these sections (case-insensitive, repeatable). An unknown name is an error that lists the valid ones |
| `VERBOSE` | No       | Report each metric's section, kind and description alongside its value                         |
| `RESET`   | No       | Start the connected node's counters and histograms over from zero. Gauges are left alone. Takes no other argument but `CLUSTER` |
| `CLUSTER` | No       | Report (or reset) every node, replicas included, instead of the connected one. An error on a standalone server |

### Return Value

**Without `VERBOSE`:** a map from metric name to value, in a fixed order
(by section, then by name within a section). Counters are integers; gauges are integers, or doubles when
the unit is fractional (seconds). A counter or gauge whose unit is bytes is followed by a
`<name>_human` entry, a string rendering it as `INFO` renders `used_memory_human` (`925B`, `1.21K`,
`3.50M`). A histogram's value is a map:

| Field     | Type    | Description                                                                         |
|-----------|---------|-------------------------------------------------------------------------------------|
| `count`   | integer | Number of observations                                                              |
| `sum`     | double  | Sum of the observations, in the metric's unit                                       |
| `buckets` | array   | `[le, count]` pairs for the finite bounds; `le` is a double, `count` is cumulative (observations `<= le`) |

The `+Inf` bucket is not listed: its count is `count`.

**With `CLUSTER`** the layout is the same, except that a gauge's value is a
map from node address (`host:port`) to value, one entry per node, sorted by address (a byte
gauge's `_human` entry is the same map of renderings).
Counters and histogram counts are summed over the nodes. A histogram whose buckets differ from the others' (a node on another version) is left out of the sum, with a warning in
that coordinator's log. Duration histograms are in seconds,
with 24 bounds at powers of two from 1 µs (`0.000001`) to 2²³ µs (about 8.4 s).

**With `VERBOSE`:** an array with one map of 5 fields per metric:
`name`, `section`, `kind` (`counter`, `gauge` or `histogram`), `value`, `description`. A byte
metric's map has a sixth, `valueHuman`, after `value`; there is no separate `_human` entry.

**`RESET`:** `OK`.

### Metrics

Names follow OpenMetrics conventions: counters end in `_total`, and a unit suffix names the
unit (durations are in seconds).

`RESET` does not change the values underneath, which only ever increase (as a Prometheus-style
scraper expects). It records them as a baseline, and `STATS` reports counters and histograms
relative to it.

Metrics are grouped in sections; each name starts with its section.

#### `asm`

Atomic slot migration (Valkey 9.0+). Two tasks, each with the same lifecycle names as the tasks
in [`task`](#task), prefixed `asm_` instead of `task_`:

| Task | Node | Trigger |
|------|------|---------|
| `drain` | Importing | Index the keys deferred while a slot import ran, once it completes |
| `post_migration_cleanup` | Exporting | Mark the exported series stale once their slots have left |

So `asm_drain_started_total`, `asm_drain_completed_total`, `asm_drain_skipped_inflight_total`,
`asm_drain_panics_total`, `asm_drain_spawn_failures_total`, `asm_drain_running`,
`asm_drain_duration_seconds` and `asm_drain_last_completed_timestamp_seconds`, and the same for
`asm_post_migration_cleanup_*`. Concurrent drains take disjoint slots and are not suppressed, so
`asm_drain_running` can exceed one.

| Work counter | Description |
|--------------|-------------|
| `asm_keys_indexed_total` | Deferred import keys successfully indexed, including batches completed before shutdown |
| `asm_post_migration_cleanup_ids_marked_total` | Exported IDs marked stale during source cleanup |

The cleanup then requests a full stale-ID sweep, which reports in `task` as
`task_stale_ids_full_sweep_*`.

The same metrics are in `INFO ts_asm`, each prefixed `ts_`, without the `debug-mode` gate, except
the duration histograms. `INFO` counts since module start: `STATS RESET` doesn't move it.

#### `clustermap`

The cluster map fan-outs choose their targets from. Empty-handed on a standalone server: the
counters stay at zero, `refresh_interval_seconds` at 0 and `age_seconds` at -1.

| Name | Kind | Description |
|------|------|-------------|
| `clustermap_refreshes_total` | counter | Rebuilds from `CLUSTER NODES`, whatever their outcome |
| `clustermap_refresh_changed_total` | counter | Rebuilds that published a new map |
| `clustermap_refresh_unchanged_total` | counter | Rebuilds that found the topology unchanged (expiry extended) |
| `clustermap_refresh_failures_total` | counter | Rebuilds that failed, leaving the previous map in place |
| `clustermap_forced_refreshes_total` | counter | Rebuilds forced by a peer request carrying a different fingerprint |
| `clustermap_stale_marks_total` | counter | Times a peer's cluster-map mismatch error marked this node's map stale |
| `clustermap_refresh_interval_seconds` | gauge | Current adaptive refresh interval (doubles while the topology is stable, up to 5 s) |
| `clustermap_age_seconds` | gauge | Time since the map was last built or confirmed unchanged |

#### `exec`

| Name | Kind | Description |
|------|------|-------------|
| `exec_fanout_queued` | gauge | Jobs waiting for a worker on the `ts-fanout-request` lane (peer requests and local fan-out shares) |
| `exec_fanout_running` | gauge | Jobs a worker is running on the `ts-fanout-request` lane |
| `exec_fanout_rejected_total` | counter | Jobs refused because the `ts-fanout-request` queue was full (answered as busy) |
| `exec_analysis_queued` | gauge | Jobs waiting for a worker on the `ts-analysis` lane |
| `exec_analysis_running` | gauge | Jobs a worker is running on the `ts-analysis` lane |
| `exec_analysis_rejected_total` | counter | Jobs refused because the `ts-analysis` queue was full |

#### `fanout`

Counted on the node where the event happens: operations, timeouts and shard errors on the
coordinator; `served_*` and `rejected_*` on the peer answering it; message and byte counts on
the sender. A fan-out whose only target is the coordinator itself is an operation (and
`local_only`) but sends nothing.

As coordinator:

| Name | Kind | Description |
|------|------|-------------|
| `fanout_operations_total` | counter | Fan-out operations started, one per command that fans out |
| `fanout_targets_total` | counter | Targets of those operations, the local node included |
| `fanout_local_only_total` | counter | Operations whose only target was this node (no RPC) |
| `fanout_duration_seconds` | histogram | Time from the start of an operation to its result |
| `fanout_inflight` | gauge | RPCs with remote shares outstanding (see `TS._DEBUG INFLIGHT`) |
| `fanout_errors_total` | counter | Shard errors of every kind, the local share included: the sum of the `fanout_errors_<kind>_total` counters |
| `fanout_errors_<kind>_total` | counter | Shard errors by kind, the local share included. Kinds: `invalid_message`, `node_unreachable`, `timeout`, `unknown_message_type`, `permissions`, `key_permissions`, `serialization`, `bad_request_id`, `internal`, `cluster_map_mismatch`, `unsupported_features`, `invalid_db`, `busy`, `database_swapped`, `custom` |
| `fanout_aborts_total` | counter | Operations ended early by an error returned as it is: a cluster-map mismatch, a permission denial, a busy shard |
| `fanout_generic_error_replies_total` | counter | Operations answered with the generic "Internal error in fanout operation" reply after a shard failed with another kind |
| `fanout_client_timeouts_total` | counter | Blocked clients answered with the timeout error |
| `fanout_rpc_timeouts_total` | counter | RPCs whose timer fired with remote shares outstanding |
| `fanout_setup_failures_total` | counter | Operations that failed before any remote request was sent |
| `fanout_blocking_denied_total` | counter | Commands refused because the client could not be blocked (MULTI, a script, a module call) |
| `fanout_send_failures_total` | counter | Requests the cluster bus refused to send |
| `fanout_local_share_busy_total` | counter | Local shares refused because the fan-out lane's queue was full |
| `fanout_local_share_expired_total` | counter | Local shares that waited in the queue past the deadline |
| `fanout_error_decode_failures_total` | counter | Error responses from peers that could not be decoded |
| `fanout_ignored_unknown_request_total` | counter | Peer answers dropped because their request is no longer in flight (it timed out, say) |
| `fanout_ignored_unknown_sender_total` | counter | Peer answers dropped because their sender is not a remote target of the request |
| `fanout_ignored_duplicate_total` | counter | Peer answers dropped because their sender had already answered |
| `fanout_ignored_after_completion_total` | counter | Shard answers dropped because the operation had already completed (after an abort, say) |
| `fanout_pushdown_fallback_series_total` | counter | `TS.MRANGE` series aggregated on the coordinator because their shard ignored aggregation push-down (an older peer) |
| `fanout_pushdown_group_fallback_series_total` | counter | `TS.MRANGE` series reduced on the coordinator because their shard ignored group-reduce push-down |

One timed-out fan-out usually counts once in both `client_timeouts` and `rpc_timeouts` (whichever
deadline reaches the client first wins), so don't add them. Local-only fan-outs have no RPC, so
only `client_timeouts` can count them.

As a peer serving a coordinator:

| Name | Kind | Description |
|------|------|-------------|
| `fanout_served_ok_total` | counter | Requests served successfully |
| `fanout_served_errors_total` | counter | Requests whose handler failed (answered with an error response) |
| `fanout_reply_send_failures_total` | counter | Successful answers the cluster bus refused to send back |
| `fanout_rejected_parse_total` | counter | Requests that could not be parsed |
| `fanout_rejected_unsupported_features_total` | counter | Requests demanding envelope features this node lacks |
| `fanout_rejected_no_handler_total` | counter | Requests for an operation this node has no handler for |
| `fanout_rejected_busy_total` | counter | Requests refused because the fan-out lane's queue was full |
| `fanout_rejected_cluster_map_mismatch_total` | counter | Requests rejected because the sender's cluster map disagrees with this node's |

On the wire (counted by the sender):

| Name | Kind | Description |
|------|------|-------------|
| `fanout_requests_sent_total` | counter | Requests sent to peers, one per peer |
| `fanout_request_sent_bytes_total` | counter | Payload bytes of those requests |
| `fanout_responses_sent_total` | counter | Responses sent back to coordinators |
| `fanout_response_sent_bytes_total` | counter | Payload bytes of those responses |
| `fanout_error_responses_sent_total` | counter | Error responses sent back to coordinators |
| `fanout_error_response_sent_bytes_total` | counter | Payload bytes of those error responses |

The byte counts are the payloads this node handed to the cluster bus: the bus's own framing is
not included, and a fan-out's local share never crosses the bus.

#### `index_reconciliation`

The background pass (`ts-index-sweep`) that runs after an RDB load whose index was preloaded from
the aux field: it checks the index against the keys that actually loaded, marks dangling ids
stale and re-indexes anything the preload missed. It is a single task, so its names carry no task
identity; otherwise they mean what the [`task`](#task) lifecycle names mean.

| Name | Kind | Description |
|------|------|-------------|
| `index_reconciliation_started_total` | counter | Passes that entered the task body |
| `index_reconciliation_completed_total` | counter | Passes that returned without a panic or shutdown abort |
| `index_reconciliation_skipped_inflight_total` | counter | Always 0: passes are not single-flight |
| `index_reconciliation_panics_total` | counter | Passes that unwound with a panic |
| `index_reconciliation_spawn_failures_total` | counter | Threads that could not be started; these do not count as started passes |
| `index_reconciliation_running` | gauge | Passes executing now |
| `index_reconciliation_duration_seconds` | histogram | Pass time, including the keyspace sweep when the digest check does not skip it |
| `index_reconciliation_last_completed_timestamp_seconds` | gauge | Unix timestamp of the most recent completed pass, or zero before any |

The same metrics, except the duration histogram, are in `INFO ts_index_reconciliation`, each
prefixed `ts_`, without the `debug-mode` gate.

#### `rdb`

The RDB aux field that carries the postings index, counted once per RDB that has one (not per
series). A save is counted wherever the server writes the RDB: `SAVE`, `BGSAVE`, a replica's full
sync, an AOF rewrite's RDB preamble, `DEBUG RELOAD`. The fork child of a background save shares
these counters with the server, so its saves are counted on the node that forked it.

| Name | Kind | Description |
|------|------|-------------|
| `rdb_load_success_total` | counter | Loads that installed the postings index from the aux field |
| `rdb_load_failure_total` | counter | Loads whose aux field was unreadable, of a foreign encoding version, or rejected by the parser; the index is rebuilt from the keys instead |
| `rdb_save_success_total` | counter | Saves that wrote the aux field |
| `rdb_save_failure_total` | counter | Saves that left the aux field out because the index was locked at fork time, or whose write failed |

Neither load counter moves when `ts-index-persist` is off and the payload is discarded, and neither
save counter moves for a save with nothing to write (persistence off, or no series). A failed load
of a foreign encoding fails the whole RDB load; every other failure falls back to rebuilding the
index from the keys.

The same metrics are in `INFO ts_rdb`, each prefixed `ts_` (`ts_rdb_load_success_total`), without
the `debug-mode` gate. `INFO` counts since module start: `STATS RESET` doesn't move it.

#### `blocked_read`

Blocking reads (`TS.READ ... BLOCK`), counted on the node serving the client. A read without
`BLOCK` counts nothing.

| Name | Kind | Description |
|------|------|-------------|
| `blocked_read_blocked_total` | counter | Reads the server blocked on a key |
| `blocked_read_served_without_block_total` | counter | `BLOCK` reads served at once because enough qualifying samples already existed |
| `blocked_read_block_refused_total` | counter | `BLOCK` reads that had to wait but could not: a context that forbids blocking (`MULTI`, a script), or the server declining to block the client |
| `blocked_read_block_timeouts_total` | counter | Blocked reads released by the timeout callback: a deadline, or `CLIENT UNBLOCK ... TIMEOUT` |
| `blocked_read_spurious_wakeups_total` | counter | Wakeups after which a blocked read re-checked its key and stayed blocked (a write that left it below `min_count`, or a condition no new sample met) |
| `blocked_read_unblocked_by_delete_total` | counter | Blocked reads released with an empty reply because their key was deleted |
| `blocked_read_blocked_clients` | gauge | Blocked reads the server still holds state for |

A blocked read ends in a reply from a wakeup, a timeout, a deletion, an error,
`CLIENT UNBLOCK ... ERROR`, or a disconnect. Timeouts and deletions have counters of their own; the
rest don't, so `blocked_read_blocked_total` is not the sum of the others. `blocked_read_blocked_clients` is moved
by the server's free callback, which runs once per block however it ended, so it returns to 0 in
all of those cases. It can differ from the `blocked_clients` of `INFO clients`, which counts every
blocked client of any command.

The same metrics are in `INFO ts_blocked_read`, each prefixed `ts_` (`ts_blocked_read_blocked_total`), without the
`debug-mode` gate. `INFO` counts since module start: `STATS RESET` doesn't move it.

#### `string_interning`

The pool of label strings every series shares. Both are O(1) reads; `STRINGPOOLSTATS` has the
distribution and the savings figure, which cost a walk of the pool.

| Name | Kind | Description |
|------|------|-------------|
| `string_interning_bytes` | gauge | Heap held by the pool |
| `string_interning_strings` | gauge | Distinct strings in the pool |

`string_interning_bytes` is the pool total, which the `MEMORY USAGE` of the series holding those
labels already divides among themselves: adding it to the sum of every key's `MEMORY USAGE`
counts it twice.

The same metrics are in `INFO ts_string_interning`, each prefixed `ts_`
(`ts_string_interning_bytes`, with its `ts_string_interning_bytes_human` rendering), without the
`debug-mode` gate.

#### `task`

Maintenance work on this node. Select it with `TS._DEBUG STATS task` or
`TS._DEBUG STATS task VERBOSE`. Task identities are fixed:

| Task | Trigger |
|------|---------|
| `trim` | Periodic retention trimming (10 s) |
| `stale_ids` | Periodic stale-ID cleanup (20 s) |
| `stale_ids_full_sweep` | Full stale-ID cleanup requested after an atomic slot migration; shares the periodic cleanup's single-flight guard, so it stays here rather than in `asm` |
| `optimize` | Periodic postings optimization (60 s) |

Each task exposes the following names, with `<task>` replaced by its identity:

| Name | Kind | Description |
|------|------|-------------|
| `task_<task>_started_total` | counter | Runs that entered the task body |
| `task_<task>_completed_total` | counter | Runs that returned without a panic or shutdown abort; recoverable per-item errors can still occur |
| `task_<task>_skipped_inflight_total` | counter | Attempts skipped because another run holds the single-flight guard |
| `task_<task>_panics_total` | counter | Runs that unwound with a panic |
| `task_<task>_spawn_failures_total` | counter | Threads that could not be started; these do not count as started runs |
| `task_<task>_running` | gauge | Task bodies executing, including time waiting for locks |
| `task_<task>_duration_seconds` | histogram | Execution time including lock waits, panics and shutdown aborts |
| `task_<task>_last_completed_timestamp_seconds` | gauge | Unix timestamp of the most recent normal completion, or zero before any completion |

The periodic tasks skip an attempt while their previous run remains in flight; the full stale-ID
sweep, which runs on its caller's thread, shares that guard. The generic fields are present for
every task; fields that cannot apply (such as spawn failures for the full sweep) remain zero.
A normal run can find nothing to do. Completion timestamps therefore show that a task is alive;
work and pass counters show its progress. Timestamps and running gauges survive `STATS RESET`.

| Work counter | Description |
|--------------|-------------|
| `task_trim_series_processed_total` | Series attempted by background retention trimming |
| `task_trim_samples_deleted_total` | Samples physically removed by background retention trimming |
| `task_trim_errors_total` | Series trim failures or failures to select a database |
| `task_stale_ids_retired_total` | Tombstoned IDs retired at the end of a complete postings pass, across all cleanup paths; excludes IDs revoked or marked again during the pass |
| `task_stale_ids_passes_completed_total` | Completed postings passes that retired a stale-ID snapshot for one database, across all cleanup paths |
| `task_optimize_passes_completed_total` | Periodic optimizer passes completed for one existing database; excludes full optimizations requested by migration |
| `task_trim_unused_dbs_removed_total` | Empty database indexes removed by cron |

Empty database index cleanup (every 300 s, inline on the main thread) is not one of the tasks
above. It is a short pass, so it reports only `task_trim_unused_dbs_removed_total` and the gauge
`task_trim_unused_dbs_running` (1 while a pass runs, else 0).

`task_optimize_last_pass_completed_timestamp_seconds` is a gauge holding the Unix timestamp of
the last completed database pass, or zero before any pass. A task invocation can process only
part of a database; its completion does not imply a complete pass. These pass metrics do not
claim that every database has been visited.

The same metrics are in `INFO ts_task`, each prefixed `ts_` (`ts_task_trim_started_total`),
without the `debug-mode` gate, except the `task_<task>_duration_seconds` histograms, which INFO
has no shape for. `INFO` counts since module start: `STATS RESET` doesn't move it.

### Examples

```
TS._DEBUG STATS exec
```

```
1) "exec_analysis_queued"
2) (integer) 0
3) "exec_analysis_rejected_total"
4) (integer) 0
5) "exec_analysis_running"
6) (integer) 0
7) "exec_fanout_queued"
8) (integer) 0
9) "exec_fanout_rejected_total"
10) (integer) 0
11) "exec_fanout_running"
12) (integer) 0
```

Start a clean window, then read one section with descriptions:

```
TS._DEBUG STATS RESET
TS._DEBUG STATS exec VERBOSE
```

---

### TS._DEBUG INFLIGHT

Lists the fan-out requests this node is coordinating that still wait on remote shards, oldest
first. The tool for "a fan-out is hung": which command, how long, how many peers have yet to
answer.

### Syntax

```bash
TS._DEBUG INFLIGHT
```

### Return Value

An array with one map of 5 fields per request:

| Field           | Type    | Description                                                             |
|-----------------|---------|-------------------------------------------------------------------------|
| `id`            | string  | The request id (a string: ids span the full unsigned 64-bit range)      |
| `command`       | string  | The fan-out operation, such as `mrange`                                 |
| `ageMs`         | integer | Milliseconds since the request was sent                                 |
| `remoteTargets` | integer | Peers the request was sent to                                           |
| `outstanding`   | integer | Peers that have not answered yet                                        |

Node-local: a request is in flight only on the node coordinating it. A fan-out whose only target
is this node never appears (it has no remote request), and a request leaves the list as soon as
its timeout fires. On a standalone server the list is always empty.

---

### TS._DEBUG STRINGPOOLSTATS

Returns memory usage and efficiency statistics for the global string interning pool. The pool deduplicates repeated
label names and values across all time series.

The command reports the pool of the node you are connected to. With `CLUSTER` it fans out to one
primary per shard and sums their pools; every primary must have `debug-mode` enabled, or the
command fails. See **With `CLUSTER`** below for how the sums read.

### Syntax

```bash
TS._DEBUG STRINGPOOLSTATS [k] [CLUSTER]
```

### Arguments

| Argument | Type    | Required | Description                                                                             |
|----------|---------|----------|-----------------------------------------------------------------------------------------|
| `k`      | integer | No       | If provided and greater than `0`, include top-K entries ranked by ref count and by size |
| `CLUSTER` | keyword | No      | Sum every shard primary's pool instead of reporting this node's. An error on a standalone server |

### Return Value

Without `k` (or `k = 0`), returns an array of 4 elements:

| Index | Name            | Description                                                               |
|-------|-----------------|---------------------------------------------------------------------------|
| 0     | `GlobalStats`   | Aggregate statistics across all interned strings (see **BucketStats**)    |
| 1     | `ByRefcount`    | Array of `[refCount, BucketStats]` pairs, grouped by external ref count   |
| 2     | `BySize`        | Array of `[size, BucketStats]` pairs, grouped by string byte length       |
| 3     | `MemorySavings` | Estimated bytes and percentage saved by interning (see **MemorySavings**) |

When `k > 0`, two additional elements are appended:

| Index | Name         | Description                                                            |
|-------|--------------|------------------------------------------------------------------------|
| 4     | `TopKByRef`  | Top-K interned strings by external reference count (see **TopKEntry**) |
| 5     | `TopKBySize` | Top-K interned strings by allocated byte size (see **TopKEntry**)      |

#### BucketStats fields

Each `BucketStats` entry is a map of 6 fields:

| Field          | Type    | Description                                              |
|----------------|---------|----------------------------------------------------------|
| `count`        | integer | Number of distinct interned strings in this bucket       |
| `bytes`        | integer | Total logical byte length of strings in this bucket      |
| `avgSize`      | float   | Average logical byte length per string                   |
| `allocated`    | integer | Total allocated memory (including Arc overhead) in bytes |
| `avgAllocated` | float   | Average allocated memory per string                      |
| `utilizationPct` | integer | Ratio of used bytes to allocated bytes, as a percentage |

#### MemorySavings fields

A map of 9 fields:

| Field                    | Type    | Description                                                                    |
|--------------------------|---------|--------------------------------------------------------------------------------|
| `memorySavedBytes`       | integer | Bytes saved by sharing interned strings across multiple references             |
| `memorySavedBytesHuman`  | string  | `memorySavedBytes` in human-readable form, e.g. `1.21M`                         |
| `memorySavedPct`         | float   | `memorySavedBytes` as a share of the pool's cost without interning             |
| `holders`                | integer | Live references to interned strings, one per outstanding slot                  |
| `holderSlotBytes`        | integer | Bytes those references spend on slots (8 bytes each), with or without interning |
| `holderSlotBytesHuman`   | string  | `holderSlotBytes` in human-readable form                                       |
| `totalStorageBytes`      | integer | What interned strings cost in total: pool `allocated` plus `holderSlotBytes`   |
| `totalStorageBytesHuman` | string  | `totalStorageBytes` in human-readable form                                     |
| `storageSavedPct`        | float   | `memorySavedBytes` as a share of `totalStorageBytes` without interning         |

The two percentages answer different questions and the gap between them is often wide.

`memorySavedPct` counts only heap allocations on both sides of the ratio, so it reports how
well the pool is deduplicating. On a label set worth interning it reads near 100% and stays
there no matter what the labels cost the server.

`storageSavedPct` adds the slot that every reference occupies to both sides. That slot exists
whether or not the bytes behind it are shared, so it cancels out of the saving but belongs in
the total — which makes this the figure to quote for how much memory interning saves overall.
It is the lower of the two, by a margin that grows as the pool deduplicates better. Measured on
a 121k-series Kubernetes-shaped label set, `memorySavedPct` reads 99.2% and `storageSavedPct`
82.1%.

For capacity planning, `storageSavedPct` is still a slight over-statement of what a series
saves: the pool cannot see the per-series container holding those slots, and it counts
short-lived references alongside stored ones. `TS.INFO`'s `memoryUsage` already amortizes the
pool across the series that share it.

#### TopKEntry fields

Each `TopKEntry` is a map of 4 fields:

| Field       | Type    | Description                                                        |
|-------------|---------|--------------------------------------------------------------------|
| `value`     | string  | The interned string value                                          |
| `refCount`  | integer | Number of external references (excluding the pool's own reference) |
| `bytes`     | integer | Logical byte length of the string                                  |
| `allocated` | integer | Total allocated memory for this string (Arc overhead + data)       |

#### With `CLUSTER`

Each node has its own pool, and replicas are left out because they mirror their primary's
labels. The reply has the same shape as on a single node, with these meanings:

- Counts, bytes and allocations are summed, so a string held by three primaries counts three
  times. `allocated` and `totalStorageBytes` are what the cluster spends on interned strings.
- `ByRefcount` buckets are keyed by each node's *local* reference count.
- `memorySavedPct` and `storageSavedPct` are recomputed from the summed byte counts.
- Top-K lists merge each primary's own top K by value, adding up `refCount` and `allocated`.
  `TopKBySize` is exact. `TopKByRef` is approximate: a string just below the cut on every node
  can be missing, and a listed string's `refCount` omits the nodes where it fell below the cut.

### Examples

Basic statistics (no top-K):

```
TS._DEBUG STRINGPOOLSTATS
```

Statistics with top 10 strings by ref count and size:

```aiignore
TS._DEBUG STRINGPOOLSTATS 10
```

The same, summed over the cluster's shard primaries:

```aiignore
TS._DEBUG STRINGPOOLSTATS 10 CLUSTER
```

---

### TS._DEBUG LIST_CONFIGS

Lists the module's configuration parameters. In compact mode (default), returns only parameter names. In verbose mode,
returns detailed metadata and the current runtime value for each parameter.

### Syntax

```bash
TS._DEBUG LIST_CONFIGS [VERBOSE]
```

### Arguments

| Argument  | Required | Description                                                                  |
|-----------|----------|------------------------------------------------------------------------------|
| `VERBOSE` | No       | When present, includes metadata and current values for each config parameter |

### Return Value

**Without `VERBOSE`:** A flat array of configuration parameter name strings.

**With `VERBOSE`:** An array with one map of 8 fields per parameter:

| Field     | Type   | Description                                                     |
|-----------|--------|-----------------------------------------------------------------|
| `name`    | string | Configuration parameter name                                    |
| `type`    | string | Value type: `integer`, `float`, `boolean`, `string`, `duration`, or `enum` |
| `default` | varies | Default value for the parameter                                 |
| `min`     | varies | Minimum allowed value, or `"none"` if unbounded                 |
| `max`     | varies | Maximum allowed value, or `"none"` if unbounded                 |
| `value`   | varies | Current runtime value                                           |
| `description` | string | What the parameter controls                                 |
| `mutable` | string | `yes` if `CONFIG SET` can change it at runtime, else `no`          |

### Configuration Parameters

| Parameter                      | Type     | Default         | Description                                                                             |
|--------------------------------|----------|-----------------|-----------------------------------------------------------------------------------------|
| `ts-chunk-size-bytes`          | integer  | 4096            | Maximum memory per time series chunk, in bytes                                          |
| `ts-encoding`                  | enum     | `COMPRESSED`    | Default chunk encoding: `CHIMP`, `GORILLA` or `UNCOMPRESSED`                            |
| `ts-duplicate-policy`          | enum     | `BLOCK`         | Policy for handling duplicate timestamps: `BLOCK`, `FIRST`, `LAST`, `MIN`, `MAX`, `SUM` |
| `ts-retention-policy`          | duration | `0` (no expiry) | Default retention period (milliseconds)                                                 |
| `ts-compaction-policy`         | string   | `None`          | Default compaction rules applied to all new time series. Filtered aggregations (`all`, `any`, `countif`, `sumif`, `share`, `none`) need an inline condition, e.g. `countif>5:1M:1h` |
| `ts-compatibility-mode`        | enum     | `EXTENDED`      | Which side of a value divergence from RedisTimeSeries to take: `EXTENDED` or `STRICT`   |
| `ts-decimal-digits`            | integer  | `none`          | Round sample values to N decimal places; `none` disables rounding                       |
| `ts-significant-digits`        | integer  | `none`          | Round sample values to N significant digits; `none` disables rounding                   |
| `ts-ignore-max-time-diff`      | duration | `0ms`           | Max time delta (ms) for which a duplicate sample is silently ignored                    |
| `ts-ignore-max-val-diff`     | float    | 0.0             | Max value delta for which a duplicate sample is silently ignored                        |
| `ts-num-threads`               | integer  | 8               | Number of worker threads for parallel query processing                                  |
| `ts-fanout-command-timeout`    | duration | —               | Timeout (ms) for fanout (cluster scatter/gather) commands                               |
| `ts-cluster-map-expiration-ms` | duration | —               | How long (ms) cluster slot-map entries are cached; `0` disables caching                 |
| `ts-fanout-aggregation-pushdown` | boolean | `yes`          | Shard-side aggregation/reduce push-down for clustered MRANGE. Not needed for rolling upgrades (version skew is handled automatically); disable only as an emergency/diagnostic revert to the coordinator-side path |
| `ts-info-developer-visible`    | boolean  | `no`            | Include developer-only fields in `TS.INFO` output                                       |

### Examples

List all config parameter names:

```valkey-cli
TS._DEBUG LIST_CONFIGS
```

```valkey-cli
1) "ts-chunk-size-bytes"
2) "ts-encoding"
3) "ts-duplicate-policy" ...
```

List configs with full metadata and current values:

```valkey-cli
TS._DEBUG LIST_CONFIGS VERBOSE
```
