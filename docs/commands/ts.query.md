# TS.QUERY

Evaluate a PromQL expression at a single point in time (an *instant query*), the equivalent of Prometheus'
`/api/v1/query` endpoint.

See [PromQL](../topics/promql.md) for the data model, the supported language surface and the configuration that
governs query limits. To evaluate an expression over a range of time, use [TS.QUERYRANGE](ts.queryrange.md).

### Syntax

```bash
TS.QUERY query
  [TIME timestamp]
  [LOOKBACK_DELTA lookback]
  [TIMEOUT duration]
  [HASHTAG hash_tag,...]
```

---

## Required Arguments

<details open><summary><code>query</code></summary>

The PromQL expression to evaluate. It can combine metric selectors, range selectors, subqueries, operators,
aggregations and functions. A series' metric name is its `__name__` label, so `http_requests_total{service="api"}`
selects the series whose `__name__` is `http_requests_total` and whose `service` is `api`.

Quote the expression when it contains spaces, so the client sends it as one argument.

The expression may be at most `ts-promql-max-query-len` bytes long (default 4096).

</details>

## Optional Arguments

<details open><summary><code>TIME timestamp</code></summary>

The evaluation timestamp for the instant query. Accepts:

- Numeric timestamp, unit detected from its magnitude: seconds, milliseconds,
  microseconds or nanoseconds. Values below 2^32 are read as **seconds**, which
  is what makes `TIME 1672531200` (the Prometheus HTTP API convention) and
  `TIME 1672531200000` name the same instant.
- Numeric timestamp with a decimal point or exponent — always **seconds**, with
  the fraction kept (`1672531200.5`).
- RFC3339 formatted date string (`2023-01-01T00:00:00Z`)
- `*` for the current time (default)
- Duration relative to the current time (e.g., `-1h` for 1 hour ago)
- `-` for the Unix epoch (timestamp `0`) and `+` for the largest representable timestamp. These are fixed
  bounds, **not** the earliest or latest sample in the database, so an instant query at either one normally
  returns an empty vector.

> **Note:** this differs from `START`/`END` on [TS.QUERYRANGE](ts.queryrange.md),
> where a bare integer is *always* milliseconds. A value like `1672531200` means
> 2023 here and 1970 there.

**Default:** the current time.

</details>

<details open><summary><code>LOOKBACK_DELTA lookback</code></summary>

How far back from the evaluation time a selector looks for a series' most recent sample. A series whose latest
sample is older than this is treated as stale and left out of the result. The window is open at the start: a
sample exactly `lookback` old does not count.

Accepts a duration string (e.g., `30s`, `5m`, `1h30m`) or an integer number of milliseconds.

**Default:** `ts-promql-lookback-delta` (5m).

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
| `resultType` | `vector`, `matrix`, `scalar` or `string`, depending on the type of the expression. |
| `result` | The result, shaped by `resultType` as described below. |

In RESP2 the map is sent as a flat array of key/value pairs. Timestamps are integers in **milliseconds**. Sample
values are bulk strings in RESP2 and doubles in RESP3.

**`vector`** (an instant-vector expression, such as `http_requests_total` or `sum by (region) (...)`) — an array
with one element per series. Each element is a map:

- `metric` — the series' labels as a map of name → value. Aggregations and most functions drop `__name__`; an
  aggregation without `by` produces an empty label set.
- `value` — a `[timestamp, value]` pair. The timestamp is the evaluation time, not the time of the underlying
  sample.

A selector that matches nothing, or matches only stale series, returns an empty array.

**`matrix`** (a range-vector expression, such as `temperature_celsius[3m]`) — an array with one element per
series. Each element is a map of `metric` (as above) and `value`, an array of `[timestamp, value]` pairs holding
the raw samples inside the window, with their own timestamps.

> **Note:** the samples key is `value` for both `vector` and `matrix` elements. The Prometheus HTTP API calls the
> matrix key `values`.

**`scalar`** (such as `1 + 2` or `scalar(...)`) — a single `[timestamp, value]` pair.

**`string`** (a string literal) — a `[timestamp, string]` pair; the string is sent as a simple string.

### Blocking and limits

The query is evaluated on a dedicated query worker, so `TS.QUERY` blocks the calling client until the result is
ready. It is therefore rejected inside `MULTI`/`EXEC`, inside a script (`EVAL`, `FCALL`), and from a module call
that cannot block.

At most `ts-promql-max-concurrent-queries` (default 8) queries run at once; up to `ts-promql-max-queued-queries`
(default 128) more wait for a worker, and further queries are refused on arrival. A result with more series than
`ts-promql-max-response-series` (default 1000) is an error, as is a query that would load more than
`ts-promql-max-samples-per-query` samples (default 50,000,000). See
[Configuration and safeguards](../topics/promql.md#configuration-and-safeguards).

### Errors

| Condition | Error |
| --- | --- |
| The expression does not parse | `TSDB: the query string could not be parsed or is otherwise invalid.` |
| The expression is longer than `ts-promql-max-query-len` | `TSDB: query too long` |
| An unknown option | `TSDB: invalid query argument '<arg>'` |
| `TIME` cannot be parsed | `TSDB: invalid TIME timestamp` |
| `LOOKBACK_DELTA` or `TIMEOUT` cannot be parsed | `TSDB: couldn't parse <OPTION> duration` |
| `HASHTAG` is missing or has an empty tag | `TSDB: missing HASHTAG argument` |
| Called inside `MULTI`, a script, or a context that cannot block | `TSDB: TS.QUERY and TS.QUERYRANGE are not allowed inside MULTI, EVAL, or a deny-blocking context` |
| More series than `ts-promql-max-response-series` | `execution error: the query returns more than the configured max series limit: <n> > <limit>` |
| A subquery grid with more than 1,000,000 steps | `execution error: PromQL argument error: subquery has too many steps: ...` |
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
# ...one sample every minute, e.g.
TS.ADD temperature:us-east 1767226200000 22
TS.ADD temperature:us-west 1767226200000 17
```

### Select series by label

`TIME` given as an RFC3339 string. Each series' value is its latest sample at or before `00:05:00`, stamped with
the evaluation time.

```bash
127.0.0.1:6379> TS.QUERY 'http_requests_total{service="api"}' TIME 2026-01-01T00:05:00Z
1) "resultType"
2) "vector"
3) "result"
4) 1) 1) "metric"
      2) 1) "__name__"
         2) "http_requests_total"
         3) "region"
         4) "us-east"
         5) "service"
         6) "api"
      3) "value"
      4) 1) (integer) 1767225900000
         2) "300"
   2) 1) "metric"
      2) 1) "__name__"
         2) "http_requests_total"
         3) "region"
         4) "us-west"
         5) "service"
         6) "api"
      3) "value"
      4) 1) (integer) 1767225900000
         2) "600"
```

The same query over RESP3, where the maps and doubles are typed:

```bash
127.0.0.1:6379> HELLO 3
...
127.0.0.1:6379> TS.QUERY 'http_requests_total{service="api"}' TIME 2026-01-01T00:05:00Z
1# "resultType" => "vector"
2# "result" =>
   1) 1# "metric" =>
         1# "__name__" => "http_requests_total"
         2# "region" => "us-east"
         3# "service" => "api"
      2# "value" =>
         1) (integer) 1767225900000
         2) (double) 300
   2) 1# "metric" =>
         1# "__name__" => "http_requests_total"
         2# "region" => "us-west"
         3# "service" => "api"
      2# "value" =>
         1) (integer) 1767225900000
         2) (double) 600
```

### Aggregate a per-second rate

Request rate per region. `TIME` is given in seconds, as in the Prometheus HTTP API. The aggregation drops
`__name__` and every label not named in `by`.

```bash
127.0.0.1:6379> TS.QUERY 'sum by (region) (irate(http_requests_total[1m]))' TIME 1767226200
1) "resultType"
2) "vector"
3) "result"
4) 1) 1) "metric"
      2) 1) "region"
         2) "us-east"
      3) "value"
      4) 1) (integer) 1767226200000
         2) "4"
   2) 1) "metric"
      2) 1) "region"
         2) "us-west"
      3) "value"
      4) 1) (integer) 1767226200000
         2) "2"
```

An aggregation without `by` returns one series with an empty label set:

```bash
127.0.0.1:6379> TS.QUERY 'sum(increase(http_requests_total[5m]))' TIME 1767226200
1) "resultType"
2) "vector"
3) "result"
4) 1) 1) "metric"
      2) (empty array)
      3) "value"
      4) 1) (integer) 1767226200000
         2) "1800"
```

### Filter with a comparison

Comparison operators drop the series for which the condition is false.

```bash
127.0.0.1:6379> TS.QUERY 'max by (region) (temperature_celsius) > 18' TIME 1767226200
1) "resultType"
2) "vector"
3) "result"
4) 1) 1) "metric"
      2) 1) "region"
         2) "us-east"
      3) "value"
      4) 1) (integer) 1767226200000
         2) "22"
```

### Rewrite labels

```bash
127.0.0.1:6379> TS.QUERY 'label_replace(temperature_celsius, "zone", "$1", "region", "us-(.*)")' TIME 1767226200
1) "resultType"
2) "vector"
3) "result"
4) 1) 1) "metric"
      2) 1) "__name__"
         2) "temperature_celsius"
         3) "region"
         4) "us-east"
         5) "zone"
         6) "east"
      3) "value"
      4) 1) (integer) 1767226200000
         2) "22"
   2) 1) "metric"
      2) 1) "__name__"
         2) "temperature_celsius"
         3) "region"
         4) "us-west"
         5) "zone"
         6) "west"
      3) "value"
      4) 1) (integer) 1767226200000
         2) "17"
```

### Return the raw samples in a window

A range selector at the top level returns a `matrix` of the samples in `(TIME - 3m, TIME]`, each with its own
timestamp. The sample at exactly `00:07:00` falls on the open edge and is excluded.

```bash
127.0.0.1:6379> TS.QUERY 'temperature_celsius{region="us-east"}[3m]' TIME 1767226200
1) "resultType"
2) "matrix"
3) "result"
4) 1) 1) "metric"
      2) 1) "__name__"
         2) "temperature_celsius"
         3) "region"
         4) "us-east"
      3) "value"
      4) 1) 1) (integer) 1767226080000
            2) "20"
         2) 1) (integer) 1767226140000
            2) "21"
         3) 1) (integer) 1767226200000
            2) "22"
```

### Scalar and string results

```bash
127.0.0.1:6379> TS.QUERY 'scalar(sum(irate(http_requests_total[1m])))' TIME 1767226200
1) "resultType"
2) "scalar"
3) "result"
4) 1) (integer) 1767226200000
   2) "6"

127.0.0.1:6379> TS.QUERY '"hello"' TIME 1767226200
1) "resultType"
2) "string"
3) "result"
4) 1) (integer) 1767226200000
   2) hello
```

### Staleness and LOOKBACK_DELTA

The last temperature sample is at `00:10:00`. Evaluated at `00:13:00` with a 2-minute lookback, it is stale, so
the result is empty. With the default 5-minute lookback both series would be returned.

```bash
127.0.0.1:6379> TS.QUERY 'temperature_celsius' TIME 2026-01-01T00:13:00Z LOOKBACK_DELTA 2m
1) "resultType"
2) "vector"
3) "result"
4) (empty array)
```

### Scope a query to part of a cluster

```bash
127.0.0.1:6379> TS.QUERY 'sum(rate(http_requests_total[5m]))' HASHTAG tenant-a,tenant-b
```

The query is evaluated over the shards owning `tenant-a` and `tenant-b` only. The result is the sum across those
shards — series held elsewhere in the cluster do not contribute.

### Errors

```bash
127.0.0.1:6379> TS.QUERY 'sum('
(error) TSDB: the query string could not be parsed or is otherwise invalid.

127.0.0.1:6379> MULTI
OK
127.0.0.1:6379(TX)> TS.QUERY '1 + 2'
QUEUED
127.0.0.1:6379(TX)> EXEC
1) (error) TSDB: TS.QUERY and TS.QUERYRANGE are not allowed inside MULTI, EVAL, or a deny-blocking context
```
