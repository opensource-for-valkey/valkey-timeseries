# TS.MGET

Get the last sample (or latest compacted sample) matching one or more label filters.

## Syntax

```
TS.MGET
    [LATEST]
    [WITHLABELS | SELECTED_LABELS label...]
    FILTER selector...
```

## Required Arguments

### FILTER selector...

One or more [series selectors](../topics/series-selectors.md) to match time series. At least one selector is required.

**Example:**

```
FILTER sensor_id=12 FILTER region=us-west
```

## Optional Arguments

### LATEST

When specified and a series has compaction rules, returns the latest compacted sample instead of the raw last sample.
Useful for getting aggregated data without needing to query the compacted series directly.

**Default:** Returns the last raw sample from the series.

### WITHLABELS

Return all label name-value pairs for each matched series.

**Note:** Cannot be used with `SELECTED_LABELS`.

### SELECTED_LABELS label...

Return only the specified label name-value pairs for each matched series. Multiple labels can be specified.

**Example:**
```
SELECTED_LABELS sensor_id region
```

**Note:** Cannot be used with `WITHLABELS`.

## Return Value

Returns an array where each element represents a matched series:

```
1) 1) "series:key"              # Series key
   2) 1) "label1"               # Labels (if WITHLABELS or SELECTED_LABELS)
      2) "value1"
      3) "label2"
      4) "value2"
   3) 1) (integer) 1609459200000  # Timestamp
      2) "42.5"                    # Value
```

- If no labels are requested, element 2 is empty
- If the series has no samples, element 3 is empty
- Series are returned in no guaranteed order

## Complexity

O(n) where n is the number of time series that match the filter(s).

## Examples

### Get last samples with all labels

```bash
127.0.0.1:6379> TS.MGET WITHLABELS FILTER sensor_id=12
1) 1) "temperature:sensor:12"
   2) 1) "sensor_id"
      2) "12"
      3) "metric_type"
      4) "temperature"
      5) "location"
      6) "warehouse"
   3) 1) (integer) 1609459200000
      2) "23.5"
```

### Get last samples with selected labels

```bash
127.0.0.1:6379> TS.MGET SELECTED_LABELS sensor_id location FILTER metric_type=temperature
1) 1) "temperature:sensor:12"
   2) 1) "sensor_id"
      2) "12"
      3) "location"
      4) "warehouse"
   3) 1) (integer) 1609459200000
      2) "23.5"
2) 1) "temperature:sensor:13"
   2) 1) "sensor_id"
      2) "13"
      3) "location"
      4) "office"
   3) 1) (integer) 1609459201000
      2) "21.8"
```

### Get latest compacted samples

```bash
127.0.0.1:6379> TS.MGET LATEST FILTER metric_type=temperature region=us-west
1) 1) "temperature:sensor:west:1"
   2) (empty array)
   3) 1) (integer) 1609459200000
      2) "23.5"
```

### Multiple filter selectors

```bash
127.0.0.1:6379> TS.MGET FILTER sensor_id=12 FILTER region=us-west
1) 1) "temperature:sensor:12"
   2) (empty array)
   3) 1) (integer) 1609459200000
      2) "23.5"
```

## Notes

- All filter selectors must match for a series to be included (logical AND)
- Label names and values are **case-sensitive**; command keywords are case-insensitive
- Returns only series that exist and match the filters
- For clustered deployments, the command fans out to all shards automatically
- The `LATEST` option is particularly useful when you have compaction rules and want the most recent aggregated value
  without querying the compacted series directly

## See Also

- [TS.GET](get.md) - Get the last sample from a single series
- [TS.MRANGE](mrange.md) - Query multiple series over a time range
- [Series Selectors](../topics/series-selectors.md) - Label filter syntax