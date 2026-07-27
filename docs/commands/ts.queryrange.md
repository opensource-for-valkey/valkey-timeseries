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

---

## Return Value

The command returns the result of the PromQL range evaluation as a matrix (list of series with their samples).

### Example

```
TS.QUERYRANGE "rate(http_requests_total[5m])" STEP 1m START -1h END *
```

This query calculates the 5-minute rate of HTTP requests for the last hour, with a 1-minute resolution.
