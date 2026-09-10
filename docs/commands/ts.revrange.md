# TS.REVRANGE

Return a range of samples ordered **from newest to oldest**.
Supports the same filtering, limiting, and aggregation/downsampling options as `TS.RANGE`.

---

## Syntax

```plain text
TS.REVRANGE key fromTimestamp toTimestamp
  [LATEST]
  [FILTER_BY_TS ts...]
  [FILTER_BY_VALUE min max]
  [COUNT count]
  [
      [ALIGN align] AGGREGATION aggregatoroperatorvalue[,aggregatoroperatorvalue...] bucketDuration [BUCKETTIMESTAMP bt] [EMPTY]
  ]
```

> Ordering: results are returned in reverse chronological order.  
> When using `AGGREGATION`, “reverse” refers to the order of returned buckets.

---

## Required Parameters

| Parameter       |      Type | Description                                                                    |
|-----------------|----------:|--------------------------------------------------------------------------------|
| `key`           |    string | Time-series key to query.                                                      |
| `fromTimestamp` | timestamp | Range start. Supports numeric timestamps and special range tokens (see below). |
| `toTimestamp`   | timestamp | Range end. Supports numeric timestamps and special range tokens (see below).   |

#### Timestamp formats (range endpoints)

`fromTimestamp` / `toTimestamp` accept:

- **Numeric timestamp** (milliseconds)
- `-` meaning **earliest**
- `+` meaning **latest**

---

### Optional Arguments

#### Range shaping & limits

| Option   | Arguments | Description                                                                                      |
|----------|-----------|--------------------------------------------------------------------------------------------------|
| `LATEST` | (none)    | Return the current value of the latest "unclosed" bucket, if it exists.                          |
| `COUNT`  | `count`   | Maximum number of returned samples (or buckets when aggregated). Must be a non-negative integer. |

#### Filtering

| Option            | Arguments | Description                                                                                                                                                                                     |
|-------------------|-----------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `FILTER_BY_TS`    | `ts...`   | Only return samples whose timestamps match one of the provided timestamps. Must provide at least 1 timestamp; capped at **128** timestamps; timestamps outside the requested range are ignored. |
| `FILTER_BY_VALUE` | `min max` | Only return samples with values in `[min, max]`. `max` must be `>= min`.                                                                                                                        |

#### Aggregation / downsampling

| Option            | Arguments                   | Description                                                                                                               |
|-------------------|-----------------------------|---------------------------------------------------------------------------------------------------------------------------|
| `AGGREGATION`     | `aggregatoroperatorvalue[,aggregatoroperatorvalue...] bucketDuration` | Downsample into fixed time buckets of size `bucketDuration` and apply each `aggregator` per bucket. A comma-separated list (up to 16 distinct aggregators) yields one value per aggregator per bucket, in the order specified. |
| `ALIGN`           | `align`                     | Bucket alignment anchor. May appear before `AGGREGATION` (`ALIGN … AGGREGATION …`) or after it (`AGGREGATION … ALIGN …`). |
| `BUCKETTIMESTAMP` | `bt`                        | Controls the timestamp emitted for each bucket. Default: `start`.                                                         |
| `EMPTY`           | (none)                      | Include empty buckets (buckets with no samples).                                                                          |

Each element of the `aggregator` list carries its own inline condition —
`aggregatoroperatorvalue`, e.g. `countif>5` — with no separator since `aggregator` and its
condition form a single argument token. `countif`, `sumif`, `share`, and `all`/`any`/`none`
**require** one; omitting it is an error. `count` and `sum` accept one *optionally*, to count/sum
only matching samples. Any other aggregator (`avg`, `max`, ...) does not accept a condition;
attaching one is an error. Different elements in the same list can use different conditions, e.g.
`AGGREGATION countif>5,sumif<=2 60000`.

##### `bucketDuration` format

`bucketDuration` is a duration:

- Integer milliseconds, e.g. `60000`
- Or a duration string (e.g., `5s`, `1m`, etc.,)

##### Alignment restrictions (important)

When aggregation is used:

- If `fromTimestamp` is `-` (earliest), you **cannot** use `ALIGN start`.
- If `toTimestamp` is `+` (latest), you **cannot** use `ALIGN end`.

---

## Available aggregators

Supported `aggregator` values for `AGGREGATION`:

| Aggregator | Description                                                                                            |
|------------|--------------------------------------------------------------------------------------------------------|
| `avg`      | Average of values in the bucket.                                                                       |
| `sum`      | Sum of values in the bucket. If `EMPTY` is enabled, empty buckets yield `0`.                           |
| `count`    | Number of samples in the bucket. If `EMPTY` is enabled, empty buckets yield `0`.                       |
| `min`      | Minimum value in the bucket.                                                                           |
| `max`      | Maximum value in the bucket.                                                                           |
| `range`    | `max - min` within the bucket.                                                                         |
| `first`    | First value encountered in the bucket.                                                                 |
| `last`     | Last value encountered in the bucket.                                                                  |
| `var.p`    | Population variance.                                                                                   |
| `var.s`    | Sample variance.                                                                                       |
| `std.p`    | Population standard deviation.                                                                         |
| `std.s`    | Sample standard deviation.                                                                             |
| `increase` | Counter increase over the bucket (handles counter resets).                                             |
| `rate`     | Counter rate per second over the bucket window (`increase / window_seconds`).                          |
| `irate`    | Instantaneous per-second rate from the last two samples in the bucket/window (handles counter resets). |
| `countif`  | Count of samples matching the inline `operatorvalue` condition.                                         |
| `sumif`    | Sum of sample values matching the inline `operatorvalue` condition.                                   |
| `share`    | Fraction of samples matching the condition (range `[0..1]`), or empty when no samples.                 |
| `all`      | `1.0` if all samples match the condition, else `0.0`.                                                  |
| `any`      | `1.0` if any sample matches the condition, else `0.0`.                                                  |
| `none`     | `1.0` if no samples match the condition, else `0.0`.                                                    |

---

## Return value

An array of samples (or buckets), ordered from newest to oldest:

```plain text
[
  [timestamp, value],
  [timestamp, value],
  ...
]
```

- Without `AGGREGATION`: raw samples in reverse chronological order.
- With `AGGREGATION`: buckets in reverse order; the bucket timestamp is controlled by `BUCKETTIMESTAMP` (default
  `start`).

---

## Examples

### Reverse range query (raw samples)

```plain text
TS.REVRANGE temperature:office 1700000000000 1700003600000
```

### Full history in reverse (using `-` / `+`)

```plain text
TS.REVRANGE temperature:office - +
```

### Reverse query with limit

```plain text
TS.REVRANGE temperature:office 1700000000000 1700003600000 COUNT 50
```

### Reverse query with value filter

```plain text
TS.REVRANGE temperature:office 1700000000000 1700003600000 FILTER_BY_VALUE 20 25
```

### Reverse downsampling: 1-minute max buckets

```plain text
TS.REVRANGE temperature:office 1700000000000 1700003600000 AGGREGATION max 60000
```

### Conditional aggregation in reverse (`any`)

Return `1.0` for each 5-minute bucket where any sample exceeds 0.9:

```plain text
TS.REVRANGE cpu:utilization 1700000000000 1700003600000 AGGREGATION any>0.9 300000
```

---

## Notes

- `TS.REVRANGE` is identical to `TS.RANGE` except for **output order** (reverse).
- `COUNT` applies to the number of returned items (samples or buckets).
