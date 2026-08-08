# TS.LABELSTATS

Returns label cardinality statistics about the timeseries data.

## Syntax

```
TS.LABELSTATS [LABEL <label-name>] [LIMIT <n>] [FILTER selector [selector ...]]
```

## Description

`TS.LABELSTATS` provides insights into the cardinality and label distribution
of the time-series data stored in the database.

It returns the following statistics:

- Total number of indexed series
- Number of distinct labels and label=value pairs in the index
- Top-K cardinalities for:
  - a specific label’s values (optional `LABEL`)
  - labels by usage
  - label=value pairs by usage

With `FILTER`, every one of those statistics is computed over the matching series alone
instead of over the whole index.

## Optional Arguments

- `LABEL <label-name>`  
  When provided, `TS.LABELSTATS` also returns the top-`LIMIT` values for this label, sorted by series count (
  cardinality).  
  Example: `LABEL region` returns the most common `region` values.

- `LIMIT <n>`  
  Limits the number of items returned in each top-N section. Higher values may increase runtime.

- `FILTER <selector> [selector ...]`  
  Restricts the report to the series matching every listed selector. See
  [filter syntax](../topics/filter-syntax.md). Because the list is variadic it consumes the rest
  of the argument list, so `FILTER` must be the last option given; `LABEL` and `LIMIT` have to
  precede it. At least one selector must follow `FILTER`, and the list must contain at least one
  positive matcher — a list of only negative matchers such as `region!=us` is rejected with
  `TSDB: please provide at least one matcher`.

  Labels carried only by non-matching series are excluded entirely: they contribute to neither
  `totalLabels`, `totalLabelValuePairs`, nor any of the top-K sections. When nothing matches,
  every count is zero and every section is empty.

## Return

A map containing the following fields.

### `totalSeries`

Total number of indexed series.

### `totalLabels`

Number of distinct label names in the index.

### `totalLabelValuePairs`

Number of distinct `label=value` pairs in the index.

### `seriesCountByMetricName`

Top-K highest cardinality metrics:

- `name`: the metric name
- `count`: cardinality (series count) having `LABEL=<name>`

- This answers "which metrics have the most series?"

### `labelValueCountByLabelName`

Top-K label names by total usage, each item containing:

- `name`: label name
- `count`: sum of cardinalities across that label’s `label=value` postings

This answers "which labels appear most across series?"

### `seriesCountByLabelValuePair`

Top-N label=value pairs by series count, each item containing:

- `name`: `label=value`
- `count`: number of series having that exact label=value

This answers "which exact label=value pairs are most common?"


## Examples

### Basic usage

```text
TS.LABELSTATS
```

```aiignore
valkey> TS.LABELSTATS LABEL http_requests 10
 1) totalSeries
 2) (integer) 1247
 3) totalLabelValuePairs
 4) (integer) 156
 5) totalLabels
 6) (integer) 8
 7) seriesCountByMetricName
 8)  1) 1) "api_latency"
        2) (integer) 523
     2) 1) "api_errors"
        2) (integer) 312
     3) 1) "cpu_usage"
        2) (integer) 289
     4) 1) "memory_usage"
        2) (integer) 123
 9) labelValueCountByLabelName
    1) 1) "status"
        2) (integer) 1247
     2) 1) "method"
        2) (integer) 1247
     3) 1) "endpoint"
        2) (integer) 1089
     4) 1) "region"
        2) (integer) 892
     5) 1) "service"
        2) (integer) 456
10) seriesCountByLabelValuePair
     1) 1) "status=200"
        2) (integer) 856
     2) 1) "status=404"
        2) (integer) 234
     3) 1) "method=GET"
        2) (integer) 789
     4) 1) "method=POST"
        2) (integer) 458
```

Use this to inspect overall index health and growth trends (series count, label counts).

### Top values for a specific label

```text
TS.LABELSTATS LABEL region LIMIT 10
```

Returns the 10 most common `region` values and their series counts, plus the global sections.

### Small limit for quick inspection

```text
TS.LABELSTATS LIMIT 5
```

Useful in production when you want a quick snapshot with minimal overhead.

### Restricting the report to a subset of series

```text
TS.LABELSTATS LABEL status LIMIT 5 FILTER region=us-east-1
```

Reports the same sections, but counting only the series labelled `region=us-east-1`. Selectors
are AND-ed, so several can be combined:

```text
TS.LABELSTATS FILTER region=us-east-1 env=prod
```

This answers "which labels drive cardinality inside this slice of the index?" — for example,
narrowing to a single tenant or metric family before hunting for a high-cardinality label.

### Complexity

Roughly proportional to the number of postings entries (label=value pairs).  
Using a larger `LIMIT` does not change the full scan cost, but increases output and heap operations for top-N tracking.
