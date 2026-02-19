# TS.QUERY

Execute a PromQL-style instant query against the time series data.

### Syntax

```bash
TS.QUERY query
  [TIME timestamp]
  [LOOKBACK_DELTA lookback]
  [TIMEOUT duration]
```

---

## Required Arguments

<details open><summary><code>query</code></summary>

The PromQL query string to evaluate. This can include metric selectors, aggregations, and functions.

</details>

## Optional Arguments

<details open><summary><code>TIME timestamp</code></summary>

The evaluation timestamp for the instant query. Accepts:

- Numeric timestamp in milliseconds
- RFC3339 formatted date string
- `*` for the current time (default)
- `+` for the latest timestamp across all series
- `-` for the earliest timestamp across all series
- Duration spec (e.g., `-1h` for 1 hour ago)

</details>

<details open><summary><code>LOOKBACK_DELTA lookback</code></summary>

The maximum lookback duration to find samples for each series. If not specified, the module's default lookback delta is
used. Accepts a duration string (e.g., `5m`, `1h`).

</details>

<details open><summary><code>TIMEOUT duration</code></summary>

The maximum execution time for the query. If the query exceeds this duration, it will be aborted.

</details>

---

## Return Value

The command returns the result of the PromQL evaluation. The format depends on the query result type (scalar or vector).

### Example

```
TS.QUERY sum(rate(http_requests_total[5m])) TIME 1672531200000
```

This query calculates the total rate of HTTP requests over a 5-minute window, evaluated at the specified timestamp.
