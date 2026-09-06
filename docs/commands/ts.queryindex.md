# TS.QUERYINDEX

Return the keys of time series that match one or more label selectors.

## Syntax

```
TS.QUERYINDEX
  [FILTER_BY_RANGE [NOT] start end]
  [HASHTAG hash_tag,...]
  selector [selector ...]
```

Unlike `TS.MRANGE` and related commands, `TS.QUERYINDEX` does not use a
`FILTER` keyword. Selectors are passed directly after the command.

Because the selector list is bare and variadic, every option must come *before*
the first selector — `FILTER_BY_RANGE` and `HASHTAG` may appear in either order,
but anything after the first selector is parsed as another selector.

## Arguments

### `FILTER_BY_RANGE [NOT] start end`

Restricts the result to indexed series that contain at least one sample in
the inclusive timestamp range from `start` to `end`.

With `NOT`, the condition is inverted: series with no samples in the range
are returned. Timestamps are milliseconds since the Unix epoch. Range values
may use the usual timestamp forms such as `-` and `+` where supported.

### `HASHTAG hash_tag,...`

In cluster mode, restricts the fan-out to the nodes that own the comma-separated
hash tags. It selects which nodes are queried; it never filters series keys or
labels, so a series whose key carries a different hash tag but which lives on a
selected node is still returned.

Several tags that map to the same node collapse to a single request to that node.
On a standalone server the option is accepted and ignored: there is no fan-out
to scope, so the reply is the same as without it.

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
no series matches. Reply ordering is not part of the command contract.

Without `FILTER_BY_RANGE`, `TS.QUERYINDEX` answers from the label index alone
and never reads sample data. With `FILTER_BY_RANGE`, each candidate series is
opened and the chunks that overlap the range are checked for a sample.
In clustered deployments, the query is sent to all shards — or, with `HASHTAG`,
only to the shards owning the given tags — and the matching keys are merged
into one reply.

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

Ask only the shards owning `{tenant-a}` and `{tenant-b}`:

```
TS.QUERYINDEX HASHTAG tenant-a,tenant-b name=cpu
```

`HASHTAG` combines with `FILTER_BY_RANGE` in either order, as long as both
precede the first selector:

```
TS.QUERYINDEX FILTER_BY_RANGE 1609459200000 1609545600000 HASHTAG tenant-a name=cpu
TS.QUERYINDEX HASHTAG tenant-a FILTER_BY_RANGE 1609459200000 1609545600000 name=cpu
```

## Errors

- `ERR wrong number of arguments` — No selector was provided.
- `TSDB: please provide at least one matcher` — All selectors are negative or otherwise unbounded.
- `TSDB: invalid timestamp` — A `FILTER_BY_RANGE` timestamp cannot be parsed.
- `TSDB: missing HASHTAG argument` — `HASHTAG` was given with no value, or with
  an empty one. Note that `TS.QUERYINDEX HASHTAG name=cpu` is *not* this error:
  with no `FILTER` keyword to delimit them, `name=cpu` is consumed as the tag
  list, leaving no selector, so the query fails with
  `TSDB: please provide at least one matcher`.
- A malformed selector produces a series-selector parsing error.

## Complexity

O(N), where N is the number of time series matching the selectors.
`FILTER_BY_RANGE` additionally opens each candidate series and decodes the
chunks that overlap the range until an in-range sample is found, so its cost
is O(N + ΣCᵢ), where Cᵢ is the number of chunks of candidate series i that
overlap the range. In cluster mode, `HASHTAG` reduces N to the series held by
the selected shards.

## ACL categories

`@read`, `@timeseries`
