# TS.QUERYRANGE

Evaluate a PromQL expression at every step of a time range (a *range query*), the equivalent of Prometheus'
`/api/v1/query_range` endpoint.

The expression is evaluated at `START`, `START + STEP`, `START + 2×STEP`, … up to and including `END`, exactly as
[TS.QUERY](ts.query.md) would evaluate it at each of those instants, and the results are collected into one series
per label set. See [PromQL](../topics/promql.md) for the data model, the supported language surface and the
configuration that governs query limits.

### Syntax

```bash
TS.QUERYRANGE query
  STEP duration
  [START timestamp]
  [END timestamp]
  [LOOKBACK_DELTA lookback]
  [TIMEOUT duration]
  [HASHTAG hash_tag,...]
```

The options after `query` may appear in any order.

---

## Required Arguments

<details open><summary><code>query</code></summary>

The PromQL expression to evaluate. It can combine metric selectors, range selectors, subqueries, operators,
aggregations and functions. A series' metric name is its `__name__` label, so `http_requests_total{service="api"}`
selects the series whose `__name__` is `http_requests_total` and whose `service` is `api`.

The expression must evaluate to an instant vector or a scalar; a top-level range selector such as `metric[5m]` or a
string literal is an error, as in Prometheus. Quote the expression when it contains spaces, so the client sends it as one argument.

The expression may be at most `ts-promql-max-query-len` bytes long (default 4096).

</details>

<details open><summary><code>STEP duration</code></summary>

The query resolution: the distance between consecutive evaluation timestamps. Accepts a duration string (e.g.,
`15s`, `1m`, `1h30m`, `1.5m`) or an integer number of milliseconds. Must be greater than zero.

The grid may have at most 1,000,000 points per series, or `ts-promql-max-points-per-timeseries` when that is set
lower. A `STEP` too small for the range is rejected before any data is read.

</details>

## Optional Arguments

<details open><summary><code>START timestamp</code></summary>

The first evaluation timestamp. Accepts:

- Numeric timestamp in **milliseconds** — a bare integer is always milliseconds,
  with no magnitude detection
- Numeric timestamp with a decimal point or exponent — **seconds**
  (`1672531200.5`)
- RFC3339 formatted date string (`2023-01-01T00:00:00Z`)
- `*` for the current time
- Duration relative to the current time (e.g., `-1h` for 1 hour ago)
- `-` for the Unix epoch (timestamp `0`) and `+` for the largest representable timestamp. These are fixed
  bounds, **not** the earliest or latest sample in the database; a range that starts at `-` or ends at `+`
  almost always exceeds the 1,000,000-point grid limit.

> **Note:** `1672531200` and `1672531200.0` are 53 years apart here — the first
> is milliseconds (1970), the second seconds (2023). And unlike `TIME` on
> [TS.QUERY](ts.query.md), a bare integer is never interpreted as seconds. Pass
> an RFC3339 string when the unit matters; a range that lands entirely outside
> the data returns an empty matrix rather than an error.

**Default:** `END` minus the lookback delta (see `LOOKBACK_DELTA`).

</details>

<details open><summary><code>END timestamp</code></summary>

The last evaluation timestamp. Accepts the same forms as `START`, with the same millisecond-by-default rule.

`END` must not be before `START`. When they are equal the query has a single step, as in Prometheus. The last
point of the grid is the largest `START + n×STEP` that does not exceed `END`.

**Default:** the current time. A `START` in the future without an `END` is therefore an error.

</details>

<details open><summary><code>LOOKBACK_DELTA lookback</code></summary>

How far back from each evaluation timestamp a selector looks for a series' most recent sample. At a step where a
series' latest sample is older than this, the series is stale and has no point at that step. The window is open
at the start: a sample exactly `lookback` old does not count.

Accepts a duration string or an integer number of milliseconds. `0` selects the default.

**Default:** `ts-promql-lookback-delta` (5m), or `STEP` when the step is larger. With
`ts-promql-set-lookback-to-step` enabled, the lookback is always `STEP` and this option is ignored.

</details>

<details open><summary><code>TIMEOUT duration</code></summary>

The maximum wall-clock time the query may take, including any time it spends queued for a free query worker. A
query that runs out of time is aborted with `query timed out`. The deadline is checked between reads, so a small
query can finish even with a very short timeout.

Accepts a duration string or an integer number of milliseconds.

**Default:** `ts-promql-max-query-duration` (30s), which is also the upper bound: a larger `TIMEOUT` is reduced to it.

</details>

<details open><summary><code>HASHTAG hash_tag,...</code></summary>

In cluster mode, restricts the fan-out to the shards that own the given hash
tags. Tags are comma-separated; supplying several queries the union of their
owning shards. A braced tag such as `{tenant-a}` is equivalent to the bare tag
`tenant-a` for slot selection. If the clause is given more than once, the last
occurrence wins.

> **Warning:** `HASHTAG` scopes *shards*, not series. It is not a label or
> key-name filter, and it does not add a predicate to the PromQL expression.
> Once a shard is selected, **every** series on it that the expression matches
> is in scope — including series whose key names carry a different hash tag.
>
> Scoping a query is an explicit request to evaluate over part of the cluster.
> Series on the shards you did not select are simply absent from the
> expression, so aggregations and binary operators are computed from the
> selected shards only: a scoped `sum(...)` is the sum over those shards, not
> the cluster-wide sum. An unknown tag still names a valid slot; that slot's
> shard is queried and may contribute nothing.

On a standalone server the option is accepted and validated but does not
restrict the query. Expressions with no selectors, such as `1 + 2`, perform no
fan-out and are unaffected.

A missing or empty value — including an empty comma-separated component — is
rejected with `TSDB: missing HASHTAG argument`.

</details>

---

## Return Value

A map with two entries, mirroring the `data` object of the Prometheus HTTP API:

| Key | Value |
| --- | --- |
| `resultType` | Always `matrix`. |
| `result` | An array with one element per series. |

Each element of `result` is a map:

- `metric` — the series' labels as a map of name → value. Aggregations and most functions drop `__name__`; an
  aggregation without `by`, or a scalar expression such as `1 + 2`, produces one series with an empty label set.
- `value` — an array of `[timestamp, value]` pairs, one per grid step at which the series has a value. Timestamps
  are the grid's evaluation timestamps, in **milliseconds**, not the times of the underlying samples. Steps at
  which the series is absent or stale are omitted rather than filled.

> **Note:** the samples key is `value`. The Prometheus HTTP API calls it `values`.

In RESP2 the maps are sent as flat arrays of key/value pairs and sample values as bulk strings; in RESP3 they are
typed maps and doubles. A query that matches nothing returns an empty `result` array.

The series are sorted by their labels, as Prometheus sorts a range query's result, so the same query returns them
in the same order on every node and after a restart.

### Blocking and limits

The query is evaluated on a dedicated query worker, so `TS.QUERYRANGE` blocks the calling client until the result
is ready. It is therefore rejected inside `MULTI`/`EXEC`, inside a script (`EVAL`, `FCALL`), and from a module call
that cannot block.

At most `ts-promql-max-concurrent-queries` (default 8) queries run at once; up to `ts-promql-max-queued-queries`
(default 128) more wait for a worker, and further queries are refused on arrival. A result with more series than
`ts-promql-max-response-series` (default 1000) is an error, as is a query that would load more than
`ts-promql-max-samples-per-query` samples (default 50,000,000). See
[Configuration and safeguards](../topics/promql.md#configuration-and-safeguards).

### Errors

| Condition | Error |
| --- | --- |
| `STEP` is missing | `TSDB: missing query STEP argument` |
| `STEP` is zero | `invalid query: step must be greater than zero` |
| `STEP`, `LOOKBACK_DELTA` or `TIMEOUT` cannot be parsed | `TSDB: couldn't parse <OPTION> duration` |
| `START` or `END` cannot be parsed | `TSDB: invalid <START\|END> timestamp` |
| `END` is before `START` | `TSDB: END must not be before START` |
| `START` is in the future and `END` is omitted | `TSDB: START must not be after the current time (the default END)` |
| The grid has more than 1,000,000 points | `execution error: PromQL argument error: too many points for the given step=..., start=... and end=...: <n>; cannot exceed 1000000` |
| The expression is a range vector or a string | `execution error: range vectors not supported in range query evaluation` (or `string expressions ...`) |
| The expression does not parse | `TSDB: the query string could not be parsed or is otherwise invalid.` |
| The expression is longer than `ts-promql-max-query-len` | `TSDB: query too long` |
| An unknown option | `ERR invalid argument '<arg>'` |
| `HASHTAG` is missing or has an empty tag | `TSDB: missing HASHTAG argument` |
| Called inside `MULTI`, a script, or a context that cannot block | `TSDB: TS.QUERY and TS.QUERYRANGE are not allowed inside MULTI, EVAL, or a deny-blocking context` |
| More series than `ts-promql-max-response-series` | `execution error: the query returns more than the configured max series limit: <n> > <limit>` |
| The query ran past its `TIMEOUT` | `query timed out` |

---

## Examples

The examples below run against this data set: three request counters that grow by 15, 30 and 45 every 15 seconds
(1, 2 and 3 requests per second), and two temperature gauges sampled once a minute. All samples lie between
`2026-01-01T00:00:00Z` (`1767225600000`) and `2026-01-01T00:10:00Z` (`1767226200000`).

```bash
TS.CREATE http_requests_total:api:us-east LABELS __name__ http_requests_total service api region us-east
TS.CREATE http_requests_total:api:us-west LABELS __name__ http_requests_total service api region us-west
TS.CREATE http_requests_total:web:us-east LABELS __name__ http_requests_total service web region us-east
# ...one sample every 15s from 1767225600000 to 1767226200000, e.g.
TS.ADD http_requests_total:api:us-east 1767225600000 0
TS.ADD http_requests_total:api:us-east 1767225615000 15

TS.CREATE temperature:us-east LABELS __name__ temperature_celsius region us-east
TS.CREATE temperature:us-west LABELS __name__ temperature_celsius region us-west
# ...one sample every minute:
#   us-east: 20 21 22 23 22 21 20 19 20 21 22
#   us-west: 15 16 17 18 17 16 15 14 15 16 17
```

### Sample gauges on a grid

Every 5 minutes from `00:00` to `00:10`, inclusive. Each point is the series' latest sample at or before that
step.

```bash
127.0.0.1:6379> TS.QUERYRANGE temperature_celsius STEP 5m START 1767225600000 END 1767226200000
1) "resultType"
2) "matrix"
3) "result"
4) 1) 1) "metric"
      2) 1) "__name__"
         2) "temperature_celsius"
         3) "region"
         4) "us-east"
      3) "value"
      4) 1) 1) (integer) 1767225600000
            2) "20"
         2) 1) (integer) 1767225900000
            2) "21"
         3) 1) (integer) 1767226200000
            2) "22"
   2) 1) "metric"
      2) 1) "__name__"
         2) "temperature_celsius"
         3) "region"
         4) "us-west"
      3) "value"
      4) 1) 1) (integer) 1767225600000
            2) "15"
         2) 1) (integer) 1767225900000
            2) "16"
         3) 1) (integer) 1767226200000
            2) "17"
```

The same kind of query over RESP3, where the maps and doubles are typed:

```bash
127.0.0.1:6379> HELLO 3
...
127.0.0.1:6379> TS.QUERYRANGE 'temperature_celsius{region="us-west"}' STEP 2m START 1767225600000 END 1767226200000
1# "resultType" => "matrix"
2# "result" =>
   1) 1# "metric" =>
         1# "__name__" => "temperature_celsius"
         2# "region" => "us-west"
      2# "value" =>
         1) 1) (integer) 1767225600000
            2) (double) 15
         2) 1) (integer) 1767225720000
            2) (double) 17
         3) 1) (integer) 1767225840000
            2) (double) 17
         4) 1) (integer) 1767225960000
            2) (double) 15
         5) 1) (integer) 1767226080000
            2) (double) 15
         6) 1) (integer) 1767226200000
            2) (double) 17
```

### Per-region request rate over time

`START` and `END` as RFC3339 strings. The aggregation drops `__name__` and every label not named in `by`.

```bash
127.0.0.1:6379> TS.QUERYRANGE 'sum by (region) (irate(http_requests_total[1m]))' STEP 2m START 2026-01-01T00:02:00Z END 2026-01-01T00:08:00Z
1) "resultType"
2) "matrix"
3) "result"
4) 1) 1) "metric"
      2) 1) "region"
         2) "us-east"
      3) "value"
      4) 1) 1) (integer) 1767225720000
            2) "4"
         2) 1) (integer) 1767225840000
            2) "4"
         3) 1) (integer) 1767225960000
            2) "4"
         4) 1) (integer) 1767226080000
            2) "4"
   2) 1) "metric"
      2) 1) "region"
         2) "us-west"
      3) "value"
      4) 1) 1) (integer) 1767225720000
            2) "2"
         2) 1) (integer) 1767225840000
            2) "2"
         3) 1) (integer) 1767225960000
            2) "2"
         4) 1) (integer) 1767226080000
            2) "2"
```

### Aggregate across all series

An aggregation without `by` returns a single series with an empty label set.

```bash
127.0.0.1:6379> TS.QUERYRANGE 'sum(temperature_celsius)' STEP 5m START 1767225600000 END 1767226200000
1) "resultType"
2) "matrix"
3) "result"
4) 1) 1) "metric"
      2) (empty array)
      3) "value"
      4) 1) 1) (integer) 1767225600000
            2) "35"
         2) 1) (integer) 1767225900000
            2) "37"
         3) 1) (integer) 1767226200000
            2) "39"
```

A scalar expression is returned the same way, one point per step:

```bash
127.0.0.1:6379> TS.QUERYRANGE '1 + 2' STEP 5m START 1767225600000 END 1767226200000
1) "resultType"
2) "matrix"
3) "result"
4) 1) 1) "metric"
      2) (empty array)
      3) "value"
      4) 1) 1) (integer) 1767225600000
            2) "3"
         2) 1) (integer) 1767225900000
            2) "3"
         3) 1) (integer) 1767226200000
            2) "3"
```

### Staleness and LOOKBACK_DELTA

The last temperature sample is at `00:10`. Queried every minute from `00:10` to `00:20` with the default 5-minute
lookback, the series keeps that value through `00:14`, then goes stale: from `00:15` on the sample is a full
5 minutes old, and those steps are omitted.

```bash
127.0.0.1:6379> TS.QUERYRANGE 'temperature_celsius{region="us-east"}' STEP 1m START 2026-01-01T00:10:00Z END 2026-01-01T00:20:00Z
1) "resultType"
2) "matrix"
3) "result"
4) 1) 1) "metric"
      2) 1) "__name__"
         2) "temperature_celsius"
         3) "region"
         4) "us-east"
      3) "value"
      4) 1) 1) (integer) 1767226200000
            2) "22"
         2) 1) (integer) 1767226260000
            2) "22"
         3) 1) (integer) 1767226320000
            2) "22"
         4) 1) (integer) 1767226380000
            2) "22"
         5) 1) (integer) 1767226440000
            2) "22"
```

With `LOOKBACK_DELTA 2m` the series goes stale after `00:11`:

```bash
127.0.0.1:6379> TS.QUERYRANGE 'temperature_celsius{region="us-east"}' STEP 1m START 1767226200000 END 1767226800000 LOOKBACK_DELTA 2m
1) "resultType"
2) "matrix"
3) "result"
4) 1) 1) "metric"
      2) 1) "__name__"
         2) "temperature_celsius"
         3) "region"
         4) "us-east"
      3) "value"
      4) 1) 1) (integer) 1767226200000
            2) "22"
         2) 1) (integer) 1767226260000
            2) "22"
```

### Relative ranges

```bash
127.0.0.1:6379> TS.QUERYRANGE 'rate(http_requests_total[5m])' STEP 1m START -1h END *
```

The 5-minute request rate of every series over the last hour, one point per minute. `END *` can be omitted, since
the end defaults to now. With neither `START` nor `END`, the range is the last lookback interval (5 minutes by
default).

### Scope a query to part of a cluster

```bash
127.0.0.1:6379> TS.QUERYRANGE 'rate(http_requests_total[5m])' STEP 1m START -1h HASHTAG tenant-a
```

The same range query restricted to the shard owning `tenant-a`; only that shard's series appear in the matrix.

### Errors

```bash
127.0.0.1:6379> TS.QUERYRANGE temperature_celsius START 1767225600000
(error) TSDB: missing query STEP argument

127.0.0.1:6379> TS.QUERYRANGE temperature_celsius STEP 1m START 1767226200000 END 1767225600000
(error) TSDB: END must not be before START

127.0.0.1:6379> TS.QUERYRANGE temperature_celsius STEP 1m START - END 1767226200000
(error) execution error: PromQL argument error: too many points for the given step=60s, start=0 and end=1767226200000: 29453771; cannot exceed 1000000
```
