# TS.MRANGE

Query multiple time series over a time range, optionally filtering, aggregating, and grouping results.

## Syntax

```
TS.MRANGE fromTimestamp toTimestamp
    [LATEST]
    [FILTER_BY_TS ts...]
    [FILTER_BY_VALUE min max]
    [WITHLABELS | SELECTED_LABELS label...]
    [COUNT count]
    [[ALIGN align] AGGREGATION aggregator[(op value)][,aggregator[(op value)]...] bucketDuration [BUCKETTIMESTAMP bt] [EMPTY]]
    FILTER selector...
    [GROUPBY label REDUCE reducer[(op value)]]
    [EXCLUDEEMPTY]
```

## Required Arguments

### fromTimestamp toTimestamp

Start and end timestamps for the query range (inclusive).

**Supported formats:**

- Unix timestamp in milliseconds (e.g., `1609459200000`)
- `-` for earliest available timestamp
- `+` for latest available timestamp
- Relative time with units (e.g., `1h`, `30m`, `7d`)

**Example:**

```
TS.MRANGE 1609459200000 1609545600000 FILTER sensor_id=12
TS.MRANGE - + FILTER region=us-west
TS.MRANGE -1h + FILTER metric_type=temperature
```

### FILTER selector...

One or more [series selectors](../topics/series-selectors.md) to match time series. At least one selector is required.

**Example:**

```
FILTER sensor_id=12 region=us-west
FILTER metric_type=temperature
```

## Optional Arguments

### LATEST

When specified and a series has compaction rules, returns samples from the latest compacted series instead of raw
samples.

**Default:** Returns raw samples.

### FILTER_BY_TS ts...

Return only samples at the specified timestamps. Timestamps outside the query range are automatically excluded.

**Example:**

```
FILTER_BY_TS 1609459200000 1609459260000 1609459320000
```

**Note:** Maximum 128 timestamps per query.

### FILTER_BY_VALUE min max

Return only samples where the value falls within the specified range (inclusive).

**Example:**

```
FILTER_BY_VALUE 20.0 25.0
```

### WITHLABELS

Return all label name-value pairs for each matched series.

**Note:** Cannot be used with `SELECTED_LABELS`.

### SELECTED_LABELS label...

Return only the specified label name-value pairs for each matched series.

**Example:**
```
SELECTED_LABELS sensor_id region
```

**Note:** Cannot be used with `WITHLABELS`.

### COUNT count

Maximum number of samples to return per series.

**Example:**

```
COUNT 100
```

### ALIGN align

Specify the alignment strategy for aggregation buckets. Must be specified before `AGGREGATION`.

**Valid values:**

- `start` - Align buckets to start timestamp (default)
- `end` - Align buckets to end timestamp
- `-` - Align to start of time range
- `+` - Align to end of time range
- Unix timestamp in milliseconds
- Time value with units (e.g., `1h`)

**Example:**

```
ALIGN start AGGREGATION avg 1h
ALIGN 1609459200000 AGGREGATION sum 5m
```

**Restrictions:**

- Cannot use `start` align with `-` range start timestamp
- Cannot use `end` align with `+` range end timestamp

### AGGREGATION aggregator[(op value)][,aggregator[(op value)]...] bucketDuration

Aggregate samples into time buckets using the specified aggregator(s) and bucket size. A
comma-separated list of up to 16 distinct aggregators produces one row per bucket containing the
bucket timestamp followed by one value per aggregator, in the order specified. All aggregators
share the bucket parameters. With `GROUPBY`/`REDUCE`, each aggregation column is reduced
independently across the series of a group.

**Supported aggregators:**

- `all` - 1 if all samples satisfy a condition, else 0
- `any` - 1 if any sample satisfies a condition, else 0
- `avg` - Average value
- `count` - Count of samples
- `countif` - Count of samples satisfying a condition
- `first` - First sample value
- `last` - Last sample value
- `max` - Maximum value
- `min` - Minimum value
- `none` - 1 if no samples satisfy a condition, else 0
- `range` - Difference between max and min
- `rate` - Per-second rate of change (only for numeric values)
- `share` - Ratio of samples satisfying a condition to total samples
- `std.p` - Population standard deviation
- `std.s` - Sample standard deviation
- `sum` - Sum of values
- `sumif` - Sum of values satisfying a condition
- `var.p` - Population variance
- `var.s` - Sample variance

**Example:**

```
AGGREGATION avg 1h
AGGREGATION sum 5m
```

#### Inline condition: aggregator(op value)

`all`, `any`, `countif`, `sumif`, `share`, and `none` **require** an inline comparison condition —
`aggregator(op value)`, e.g. `countif(>5)` — with no spaces inside the parentheses since it is a
single argument token; omitting it is an error. `count` and `sum` accept the same form
*optionally*, to count/sum only matching samples. Any other aggregator (`avg`, `max`, ...) does
not accept a condition; attaching one is an error.

**Supported operators:** `<`, `<=`, `>`, `>=`, `==`, `!=`

**Example:**

```
AGGREGATION share(>20.0) 1h
```

Different elements of the `aggregator` list can filter on different conditions:

```
AGGREGATION countif(>5),sumif(<=2),avg 1h
```

#### BUCKETTIMESTAMP bt

Specify which timestamp to use for aggregated buckets.

**Valid values:**

- `start` - Bucket start time (default)
- `end` - Bucket end time
- `mid` - Bucket midpoint time

**Example:**

```
AGGREGATION avg 1h BUCKETTIMESTAMP mid
```

#### EMPTY

Include empty buckets (buckets with no samples) in results with no value.

**Example:**

```
AGGREGATION avg 1h EMPTY
```

### GROUPBY label REDUCE reducer

Group matching series by label value and apply a reducer across each group.

**Example:**

```
GROUPBY region REDUCE sum
```

**Supported reducers:**

Supports all aggregators except `rate` (e.g., `avg`, `sum`, `count`, `max`, `min`, etc.)

#### Inline condition: reducer(op value)

Same inline condition syntax as `AGGREGATION` (see above): required for `countif`/`sumif`/`share`/
`all`/`any`/`none`, optional for `count`/`sum`, and disallowed for other reducers.

**Example:**

```
GROUPBY region REDUCE countif(>20.0)
```

### EXCLUDEEMPTY

Omit matched series that report no samples for the query. By default every series
passing `FILTER` is reported, including those with an empty sample list.

Emptiness is judged on what would be reported, not on the stored series: a series
left with nothing by `FILTER_BY_TS`/`FILTER_BY_VALUE`, or one whose in-range
samples produce no bucket under `AGGREGATION`, is omitted just like one with no
samples in the range. A series reporting a `NaN` sample is *not* empty and is kept.

`EXCLUDEEMPTY` cannot be combined with `GROUPBY ... REDUCE` — grouping collapses
the matched series into per-group results, leaving no per-series emptiness to act
on, so the combination is rejected with
`TSDB: EXCLUDEEMPTY is not allowed with GROUPBY`.

**Example:**

```
TS.MRANGE - 500 EXCLUDEEMPTY FILTER region=us-west
```

## Return Value

Returns an array where each element represents a matched series (or group when using `GROUPBY`):

```
1) 1) "series:key"              # Series key (or group label value)
   2) 1) "label1"               # Labels (if WITHLABELS or SELECTED_LABELS)
      2) "value1"
      3) "label2"
      4) "value2"
   3) 1) 1) (integer) 1609459200000  # Timestamp
         2) "23.5"                     # Value
      2) 1) (integer) 1609459260000
         2) "24.1"
      ...
```

- If no labels are requested, element 2 is empty
- Element 3 contains timestamp-value pairs
- When using `GROUPBY`, element 1 contains the group label value instead of series key
- Series/groups are returned in no guaranteed order

## Complexity

O(n×m×k) where:

- n = number of matched series
- m = number of samples per series in the range
- k = aggregation/grouping cost

## Examples

### Query multiple series with all labels

```bash
127.0.0.1:6379> TS.MRANGE 1609459200000 1609545600000 WITHLABELS FILTER sensor_id=12
1) 1) "temperature:sensor:12"
   2) 1) "sensor_id"
      2) "12"
      3) "metric_type"
      4) "temperature"
      5) "location"
      6) "warehouse"
   3) 1) 1) (integer) 1609459200000
         2) "23.5"
      2) 1) (integer) 1609459260000
         2) "23.8"
      3) 1) (integer) 1609459320000
         2) "24.1"
```

### Query with selected labels and value filter

```bash
127.0.0.1:6379> TS.MRANGE - + SELECTED_LABELS region FILTER_BY_VALUE 20 25 FILTER metric_type=temperature
1) 1) "temperature:sensor:12"
   2) 1) "region"
      2) "us-west"
   3) 1) 1) (integer) 1609459200000
         2) "23.5"
      2) 1) (integer) 1609459260000
         2) "23.8"
```

### Query with aggregation

```bash
127.0.0.1:6379> TS.MRANGE - + AGGREGATION avg 1h FILTER sensor_id=12
1) 1) "temperature:sensor:12"
   2) (empty array)
   3) 1) 1) (integer) 1609459200000
         2) "23.8"
      2) 1) (integer) 1609462800000
         2) "24.2"
```

### Query with grouping

```bash
127.0.0.1:6379> TS.MRANGE - + FILTER metric_type=temperature GROUPBY region REDUCE avg
1) 1) "us-west"
   2) (empty array)
   3) 1) 1) (integer) 1609459200000
         2) "23.5"
      2) 1) (integer) 1609459260000
         2) "23.8"
2) 1) "us-east"
   2) (empty array)
   3) 1) 1) (integer) 1609459200000
         2) "21.2"
      2) 1) (integer) 1609459260000
         2) "21.5"
```

### Query with timestamp filter and count limit

```bash
127.0.0.1:6379> TS.MRANGE - + FILTER_BY_TS 1609459200000 1609459320000 COUNT 10 FILTER sensor_id=12
1) 1) "temperature:sensor:12"
   2) (empty array)
   3) 1) 1) (integer) 1609459200000
         2) "23.5"
      2) 1) (integer) 1609459320000
         2) "24.1"
```

### Query with aggregation condition and empty buckets

```bash
127.0.0.1:6379> TS.MRANGE - + AGGREGATION countif(>23.0) 1h EMPTY FILTER sensor_id=12
1) 1) "temperature:sensor:12"
   2) (empty array)
   3) 1) 1) (integer) 1609459200000
         2) "2"
      2) 1) (integer) 1609462800000
         2) "0"
```

### Query excluding series with no samples in the range

```bash
127.0.0.1:6379> TS.MRANGE - 500 WITHLABELS FILTER s=1
1) 1) "s"
   ...
2) 1) "u"
   2) 1) 1) "s"
         2) "1"
   3) (empty array)

127.0.0.1:6379> TS.MRANGE - 500 WITHLABELS EXCLUDEEMPTY FILTER s=1
1) 1) "s"
   2) 1) 1) "s"
         2) "1"
   3) 1) 1) (integer) 100
         2) "100"
```

## Notes

- All filter selectors must match for a series to be included (logical AND)
- For clustered deployments, the command fans out to all shards automatically
- `EXCLUDEEMPTY` is applied on every shard and again on the coordinator, so a
  series is omitted regardless of which shard owns it
- When using `GROUPBY`, series are grouped by the specified label value
- Aggregation is applied before grouping when both are specified
- `LATEST` is useful when you have compaction rules and want aggregated data without querying compacted series directly

## See Also

- [TS.RANGE](range.md) - Query a single series over a time range
- [TS.MREVRANGE](mrevrange.md) - Query multiple series in reverse order
- [TS.MGET](mget.md) - Get the last sample from multiple series
- [Series Selectors](../topics/series-selectors.md) - Label filter syntax