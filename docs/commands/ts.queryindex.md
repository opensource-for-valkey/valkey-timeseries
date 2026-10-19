# TS.QUERYINDEX

Return the keys of time series that match one or more label selectors.

## Syntax

```
TS.QUERYINDEX
  [FILTER_BY_RANGE [NOT] start end]
  selector [selector ...]
```

Unlike `TS.MRANGE` and related commands, `TS.QUERYINDEX` does not use a
`FILTER` keyword. Selectors are passed directly after the command.

## Arguments

### `FILTER_BY_RANGE [NOT] start end`

Restricts the result to indexed series that contain at least one sample in
the inclusive timestamp range from `start` to `end`.

With `NOT`, the condition is inverted: series with no samples in the range
are returned. Timestamps are milliseconds since the Unix epoch. Range values
may use the usual timestamp forms such as `-` and `+` where supported.

### `selector`

One or more label selectors. Multiple selector arguments are combined with
logical AND. See [filter syntax](../topics/filter-syntax.md) for the complete
grammar.

Basic selectors include:

```
label=value
label!=value
label=(value1,value2)
label!=(value1,value2)
```

Prometheus-style selectors are also supported:

```
metric_name{label="value",other!="excluded"}
label=~"regular-expression"
label!~"regular-expression"
```

At least one selector in the complete query must contain a positive, bounded
matcher such as `label=value`, `label=(value1,value2)`, or a non-empty regular
expression. A query made only from negative or unbounded matchers is rejected.

## Return value

Returns an array of matching time-series key names. The array is empty when
no series matches. Reply ordering is not part of the command contract. Key
names are binary-safe in standalone and cluster replies: invalid UTF-8 and
interior NUL bytes are preserved, and distinct byte strings remain distinct
results.

What the query reads, and whose access it checks, depends on `FILTER_BY_RANGE`:

- **Without it**, the answer comes from the label index alone. No key is
  opened and no sample data is read. Matching key names are returned whatever
  the caller's per-key read access.
- **With it**, each matching series is opened, and the chunks that overlap
  the range are checked for a sample. Every matching series must be readable,
  even one the range would exclude. If any is not, the query fails with a
  permission error.

In clustered deployments, the query is sent to all shards and the matching
keys are merged into one reply.

## Notes

- In cluster mode, each shard reads its matches in batches and releases the
  module lock between batches. A shard's answer is therefore not a
  point-in-time snapshot:
  - a series renamed before its batch is read is returned under its new name;
  - a deleted series is skipped;
  - a series created after the query was planned is not included;
  - range checks in later batches can see writes made in between.
- On a standalone server, a query estimated to match more than 512 series runs
  on a bounded pool of background workers, shared with `TS.OUTLIERS` and
  label-discovery reads, and reads in batches in the same way. When every
  selector is an `=` or `!=` match on non-empty values, the estimate is the
  exact match count, so a narrow query on a large database stays on the main
  thread. Other selectors (regular expressions, prefix and substring matches,
  empty values) are sized by how many series the database indexes. Building
  the reply happens on the worker too. When the workers' queue is full, the
  command fails with
  `TSDB: series query: too many queued jobs (limit 256)`.
- Smaller queries, and any query inside `MULTI`, a script or a module call,
  hold the module lock for the whole read. Use `MULTI` when the reply must
  reflect a single instant.

## Examples

Create sample series:

```
TS.CREATE ts:cpu:node1 LABELS name cpu type usage node node1
TS.CREATE ts:cpu:node2 LABELS name cpu type usage node node2
TS.CREATE ts:memory:node1 LABELS name memory type usage node node1
TS.ADD ts:cpu:node1 1609462800000 42.5
TS.ADD ts:cpu:node2 1609372800000 37.0
TS.ADD ts:memory:node1 1609462800000 8192
```

`ts:cpu:node1` and `ts:memory:node1` have a sample inside the range used by
the `FILTER_BY_RANGE` examples below; `ts:cpu:node2` only has a sample from
the day before.

Find all CPU series:

```
TS.QUERYINDEX name=cpu
```

Find CPU usage series on node 1:

```
TS.QUERYINDEX name=cpu type=usage node=node1
```

Match a label with a regular expression:

```
TS.QUERYINDEX name=~"cpu|memory"
```

Exclude one value while keeping the query bounded:

```
TS.QUERYINDEX type=usage node!=node2
```

Find CPU series that contain data in a time range:

```
TS.QUERYINDEX FILTER_BY_RANGE 1609459200000 1609545600000 name=cpu
```

Find CPU-indexed series without data in that range:

```
TS.QUERYINDEX FILTER_BY_RANGE NOT 1609459200000 1609545600000 name=cpu
```

## Errors

- `ERR wrong number of arguments` — No selector was provided.
- `TSDB: please provide at least one matcher` — All selectors are negative or otherwise unbounded.
- `TSDB: invalid timestamp` — A `FILTER_BY_RANGE` timestamp cannot be parsed.
- A malformed selector produces a series-selector parsing error.

## Complexity

O(N), where N is the number of time series matching the selectors.
`FILTER_BY_RANGE` additionally opens each candidate series and decodes the
chunks that overlap the range until an in-range sample is found, so its cost
is O(N + ΣCᵢ), where Cᵢ is the number of chunks of candidate series i that
overlap the range.

## ACL categories

`@read`, `@timeseries`
