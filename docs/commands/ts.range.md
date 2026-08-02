# TS.RANGE

Query a time series for raw samples or aggregated data over a specified time range.

### Syntax

```bash
TS.RANGE key fromTimestamp toTimestamp
  [LATEST]
  [FILTER_BY_TS timestamp ...]
  [FILTER_BY_VALUE min max]
  [COUNT count]
  [[ALIGN align] AGGREGATION aggregator[(op value)][,aggregator[(op value)]...] bucketDuration [BUCKETTIMESTAMP bt] [EMPTY]]
```

---

## Required Arguments

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

## Optional Arguments

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
<details open><summary><code>AGGREGATION aggregator[(op value)][,aggregator[(op value)]...] bucketDuration</code></summary>
Aggregate raw samples into fixed-size time buckets. See [Aggregators](#aggregators) for supported aggregation functions.

`aggregator` may be a comma-separated list of up to 16 distinct aggregators (e.g. `avg,max,count`).
All aggregators share the bucket parameters (`bucketDuration`, `ALIGN`, `BUCKETTIMESTAMP`, `EMPTY`).
Each bucket then produces one row containing the bucket timestamp followed by one value per
aggregator, in the order specified. With a single aggregator the output shape is unchanged
(`[timestamp, value]`).

A [filtered aggregator](#filtered-aggregators) (`countif`, `sumif`, `all`, `any`, `none`, `share`)
**requires** an inline condition — `aggregator(op value)`, e.g. `countif(>5)` — with no spaces
inside the parentheses since it is a single argument token; omitting it is an error. `count` and
`sum` optionally accept the same inline form to filter which samples they count/sum. Any other
aggregator (`avg`, `max`, ...) does not accept a condition at all; attaching one is an error.
Different elements in the same list can use different conditions, e.g. `AGGREGATION
countif(>5),sumif(<=2),avg 60000` counts samples over 5, sums samples at or under 2, and averages
everything — three independent conditions in a single clause.
</details>
<details open><summary><code>ALIGN align</code></summary> 
Control bucket alignment:
  - `start` — Align buckets to range start
  - `end` — Align buckets to range end
  - Numeric timestamp — Align all buckets to a specific timestamp
  - If omitted, uses module default alignment.
</details>
<details open><summary><code>BUCKETTIMESTAMP bt</code></summary>
(Optional) Which timestamp to return for each bucket:

- `start` (default) — Bucket start time
- `end` — Bucket end time
- `mid` — Bucket midpoint

</details>
### Aggregation

- **`AGGREGATION aggregator[(op value)][,aggregator[(op value)]...] bucketDuration`** — Aggregate raw samples into fixed-size time buckets
  - **`aggregator`** — Aggregation function(s) to apply (see [Aggregators](#aggregators)); a
    comma-separated list produces one output column per aggregator, in the order specified
  - **`(op value)`** — Inline comparison condition for a filtered aggregator, e.g. `countif(>5)`.
    `op` is one of `>`, `<`, `>=`, `<=`, `==`, `!=`; `value` is the number to compare against.
    Only samples satisfying the condition are included in that aggregator's computation.
  - **`bucketDuration`** — Bucket size in milliseconds (must be positive)
---

## Supported Aggregators

### Simple Aggregators

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

### Statistical Aggregators

| Aggregator | Description                   | Empty Bucket Value     |
|------------|-------------------------------|------------------------|
| `std.p`    | Population standard deviation | `NaN`                  |
| `std.s`    | Sample standard deviation     | `NaN` (if < 2 samples) |
| `var.p`    | Population variance           | `NaN`                  |
| `var.s`    | Sample variance               | `NaN` (if < 2 samples) |

### Counter/Rate Aggregators

| Aggregator | Description                                      | Notes                                                                 |
|------------|--------------------------------------------------|-----------------------------------------------------------------------|
| `increase` | Total increase for monotonic counters            | Handles resets                                                        |
| `rate`     | Rate of change per second over the bucket window | —                                                                     |
| `irate`    | Instantaneous rate from the last two samples     | Requires ≥ 2 samples and positive time delta; returns `NaN` otherwise |

### Filtered Aggregators

> These require an inline `(op value)` condition, e.g. `countif(>5)`; omitting it is an error.

| Aggregator | Description                                           | Empty Bucket Value |
|------------|-------------------------------------------------------|--------------------|
| `countif`  | Count of samples matching condition                   | `0`                |
| `sumif`    | Sum of samples matching condition                     | `0`                |
| `share`    | Fraction of samples matching condition (`[0.0, 1.0]`) | `NaN`              |
| `all`      | `1.0` if all samples match, `0.0` otherwise           | `NaN`              |
| `any`      | `1.0` if any sample matches, `0.0` otherwise          | `NaN`              |
| `none`     | `1.0` if no samples match, `0.0` otherwise            | `NaN`              |

`count` and `sum` also accept an *optional* inline condition (`count(>5)`, `sum(<=2)`) to count or
sum only matching samples; without one they operate over every sample in the bucket as usual.

---

## Return Value

**Without aggregation:**  
Array of `[timestamp, value]` pairs

**With aggregation:**  
Array of `[bucketTimestamp, aggregatedValue]` pairs

---

## Examples

### Basic Query

Get all samples in a time range:

```
TS.RANGE temperature 1609459200000 1609545600000
```

### Latest Sample

Get the most recent sample:

```
TS.RANGE temperature - + LATEST
```

### Value Filtering

Get samples where value is between 20 and 30:

```
TS.RANGE temperature 1609459200000 1609545600000 FILTER_BY_VALUE 20 30
```

### Specific Timestamps

Get samples at exact timestamps:

```
TS.RANGE sensor:001 - + FILTER_BY_TS -2h 1609459260000 1609459320000
```

### Hourly Average

Compute average per hour:

```
TS.RANGE requests 1609459200000 1609545600000 AGGREGATION avg 3600000
```

### Multiple Aggregations in One Pass

Compute the average, maximum, and sample count per minute in a single scan. Each returned row is
`[bucketTimestamp, avg, max, count]`, with values in the order the aggregators were specified:

```
TS.RANGE temp:tlv - + AGGREGATION avg,max,count 60000
1) 1) (integer) 1652419200000
   2) (double) 22.4
   3) (double) 31.0
   4) (double) 12
2) ...
```

### 5-Minute Sums with Empty Buckets

```
TS.RANGE bytes 1609459200000 1609470000000 
  ALIGN start 
  AGGREGATION sum 300000 
  BUCKETTIMESTAMP mid 
  EMPTY
```

### Limited Results

Get first 100 aggregated buckets:

```
TS.RANGE metrics 1609459200000 1609545600000 
  AGGREGATION avg 60000 
  COUNT 100
```

### Conditional Aggregation

Count samples over 90 and sum samples at or under 10, per hour, in one scan:

```
TS.RANGE cpu:utilization 1609459200000 1609545600000 AGGREGATION countif(>90),sumif(<=10) 3600000
```

---

## Behavior Notes

- **Timestamp Inclusivity:** Both `fromTimestamp` and `toTimestamp` are inclusive
- **Empty Buckets:** Omitted by default; use `EMPTY` to include them. The buckets reported are
  those the query window and the series' own data extent have in common, so an empty bucket
  appears wherever data exists on both sides of it — including past the last sample inside the
  window, when the series continues beyond it — and no bucket is reported before the series'
  first sample or after its last, however wide the window is. `FILTER_BY_TS`/`FILTER_BY_VALUE`
  narrow that extent to the samples they keep.
- **Filtered Aggregators:** Condition filters are applied within each bucket after timestamp/value filters
- **Reverse Queries:** `TS.REVRANGE` adjusts semantics of `FIRST`/`LAST` appropriately
- **Bucket Boundaries:** Computed based on alignment and `bucketDuration`
- **Special Values:** Use `-inf`/`+inf` for unbounded value ranges

---

## Common Errors

- **Wrong arity** — Missing required arguments
- **invalid AGGREGATION value** — Unrecognized aggregator name
- **TSDB: missing condition for aggregator** — A filtered aggregator (`countif`, `sumif`, `all`, `any`, `none`, `share`) was given without an inline `(op value)` condition
- **TSDB: aggregation type does not support a filter condition** — An inline condition was attached to an aggregator that doesn't accept one (anything but `countif`, `sumif`, `all`, `any`, `none`, `share`, `count`, `sum`)
- **invalid BUCKETTIMESTAMP** — Invalid bucket timestamp option
- **invalid ALIGN** — Invalid alignment parameter
- **invalid bucketDuration** — Bucket duration must be a positive integer

---

## See Also

- `TS.REVRANGE` — Same query in reverse order
- `TS.MRANGE` — Query multiple time series at once
- `TS.GET` — Get the latest sample only
- `TS.ADD` — Add samples to a time series


