# TS.QUERY

Execute a PromQL-style instant query against the time series data.

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

The PromQL query string to evaluate. This can include metric selectors, aggregations, and functions.

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
- RFC3339 formatted date string
- `*` for the current time (default)
- `+` for the latest timestamp across all series
- `-` for the earliest timestamp across all series
- Duration spec (e.g., `-1h` for 1 hour ago)

> **Note:** this differs from `START`/`END` on [TS.QUERYRANGE](ts.queryrange.md),
> where a bare integer is *always* milliseconds. A value like `1672531200` means
> 2023 here and 1970 there.

</details>

<details open><summary><code>LOOKBACK_DELTA lookback</code></summary>

The maximum lookback duration to find samples for each series. If not specified, the module's default lookback delta is
used. Accepts a duration string (e.g., `5m`, `1h`).

</details>

<details open><summary><code>TIMEOUT duration</code></summary>

The maximum execution time for the query. If the query exceeds this duration, it will be aborted.

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

The command returns the result of the PromQL evaluation. The format depends on the query result type (scalar or vector).

### Example

```
TS.QUERY sum(rate(http_requests_total[5m])) TIME 1672531200000
```

This query calculates the total rate of HTTP requests over a 5-minute window, evaluated at the specified timestamp.

```
TS.QUERY sum(rate(http_requests_total[5m])) TIME 1672531200000 HASHTAG tenant-a,tenant-b
```

The same query evaluated over the shards owning `tenant-a` and `tenant-b` only.
The result is the sum across those two shards — series held elsewhere in the
cluster do not contribute.
