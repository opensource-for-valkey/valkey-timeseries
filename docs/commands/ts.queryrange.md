# TS.QUERYRANGE

Execute a PromQL-style range query against the time series data.

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

---

## Required Arguments

<details open><summary><code>query</code></summary>

The PromQL query string to evaluate. This can include metric selectors, aggregations, and functions.

</details>

<details open><summary><code>STEP duration</code></summary>

The query resolution step width. Accepts a duration string (e.g., `15s`, `1m`, `1h`).

</details>

## Optional Arguments

<details open><summary><code>START timestamp</code></summary>

The start time for the range query. Accepts:

- Numeric timestamp in **milliseconds** — a bare integer is always milliseconds,
  with no magnitude detection
- Numeric timestamp with a decimal point or exponent — **seconds**
  (`1672531200.5`)
- RFC3339 formatted date string
- `*` for the current time
- `+` for the latest timestamp across all series
- `-` for the earliest timestamp across all series
- Duration spec (e.g., `-1h` for 1 hour ago)

> **Note:** `1672531200` and `1672531200.0` are 53 years apart here — the first
> is milliseconds (1970), the second seconds (2023). And unlike `TIME` on
> [TS.QUERY](ts.query.md), a bare integer is never interpreted as seconds. Pass
> an RFC3339 string when the unit matters; a range that lands entirely outside
> the data returns an empty matrix rather than an error.

</details>

<details open><summary><code>END timestamp</code></summary>

The end time for the range query. Accepts the same forms as `START`, with the
same millisecond-by-default rule:

- Numeric timestamp in **milliseconds**
- Numeric timestamp with a decimal point or exponent — **seconds**
- RFC3339 formatted date string
- `*` for the current time
- `+` for the latest timestamp across all series
- `-` for the earliest timestamp across all series
- Duration spec (e.g., `-30m` for 30 minutes ago)

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

The command returns the result of the PromQL range evaluation as a matrix (list of series with their samples).

### Example

```
TS.QUERYRANGE "rate(http_requests_total[5m])" STEP 1m START -1h END *
```

This query calculates the 5-minute rate of HTTP requests for the last hour, with a 1-minute resolution.

```
TS.QUERYRANGE "rate(http_requests_total[5m])" STEP 1m START -1h END * HASHTAG tenant-a
```

The same range query restricted to the shard owning `tenant-a`; only that
shard's series appear in the matrix.
