# TS.CREATERULE

## Syntax

```
TS.CREATERULE sourceKey destKey 
AGGREGATION aggregator[(op value)] bucketDuration [alignTimestamp]
```

## Description

Creates a compaction rule that automatically aggregates data from a source time series into a destination time series at
regular intervals. This is useful for downsampling high-frequency data into lower-resolution time series for long-term
storage and analysis.

## Arguments

- **sourceKey**: The key of the source time series to compact from.
- **destKey**: The key of the destination time series to compact into.
- **aggregator**: The aggregation function to apply. Supported values:
    - `all` — 1 if all values satisfy a condition, else 0
    - `any` — 1 if any value satisfies a condition, else 0
    - `avg` — Average value
    - `count` — Count of values
    - `countif` — Count of values satisfying a condition
    - `share` — Ratio of values satisfying a condition to total values
    - `none` — 1 if no values satisfy a condition, else 0
    - `first` — First value in the bucket
    - `increase` — Counter increase over the bucket (handles resets)
    - `irate` — Instantaneous rate from the last two values in the bucket (handles resets)
    - `last` — Last value in the bucket
    - `sum` — Sum of values
    - `min` — Minimum value
    - `max` — Maximum value
    - `count` — Number of samples
    - `first` — First value in the bucket
    - `last` — Last value in the bucket
    - `range` — Difference between max and min
    - `sumif` — Sum of values satisfying a condition
    - `stdp` — Population standard deviation
    - `stds` — Sample standard deviation
    - `var_p` — Population variance
    - `var_s` — Sample variance
    - `rate` — Rate of change per second (handles resets)
- **bucketDuration**: The duration of each aggregation bucket (e.g., `1h`, `30m`, `5000ms`).
- **op value**: Inline comparison condition attached directly to `aggregator` with no separator,
  e.g. `countif>100`. `op` is one of `==`, `!=`, `>=`, `<=`, `<`, `>`; `aggregator` and `op value`
  together form a single argument token. **Required** for `all`, `any`, `countif`, `sumif`,
  `share`, `none` — it is an error to omit it for these. **Optional** for `count` and `sum`.
  **Not accepted** by any other aggregator (`avg`, `min`, `max`, ...) — attaching one is an error.
- **alignTimestamp** (optional): Align bucket boundaries to a specific timestamp (milliseconds since epoch).

## Constraints

- **sourceKey** and **destKey** must be different.
- The destination key must not already have a compaction rule as its source.
- The destination key cannot be a compaction rule itself (no chaining).
- Circular dependencies are not allowed.

## Return Value

Returns `OK` on success.

## Errors

- `ERR wrong number of arguments` — Insufficient arguments provided.
- `TSDB: the source key and destination key should be different` — Source and destination are the same.
- `TSDB: the destination key already has a src rule` — Destination already has an incoming compaction rule or is itself
  a compaction destination.
- `TSDB: invalid aggregator` — Unsupported aggregation function.
- `TSDB: invalid bucket duration` — Invalid duration format.
- `TSDB: circular compaction rule` — Creating this rule would form a cycle.
- `TSDB: missing condition for aggregator` — `all`/`any`/`countif`/`sumif`/`share`/`none` used without an inline `op value` condition.
- `TSDB: aggregation type does not support a filter condition` — An inline condition was attached to an aggregator that doesn't accept one.

## Permissions

Requires `UPDATE` permission on both the source and destination time series keys.

## Examples

Create a rule to downsample by minute data into hourly averages:

```
TS.CREATERULE myts:1m myts:1h AGGREGATION avg 1h
```

Create a rule that counts only samples greater than 100:

```
TS.CREATERULE temperature:raw temperature:hourly AGGREGATION countif>100 1h
```

Create a rule with bucket alignment to midnight UTC:

```
TS.CREATERULE prices:minute prices:daily AGGREGATION sum 1d 0
```

## See Also

- `TS.DELETERULE` — Remove a compaction rule
- `TS.CREATE` — Create a new time series
- `TS.ADD` — Add samples to a time series
- `TS.INFO` — Get time series metadata and rules