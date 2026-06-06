# TS.DECOMPOSE

Decompose a time series into its constituent components: trend, seasonality, and residual.

`TS.DECOMPOSE` uses STL (Seasonal-Trend decomposition using LOESS) for single seasonal periods
or MSTL (Multiple Seasonal-Trend decomposition using LOESS) for multiple seasonal periods.
The seasonal period can be explicitly specified or automatically detected from the data.

## Syntax

```
TS.DECOMPOSE key fromTimestamp toTimestamp
  [SEASONALITY <AUTO | period [period ...]>]
```

[Examples](#examples)

## Required arguments

<details open>
<summary><code>key</code></summary>

Key name for the time series to decompose.
</details>

<details open>
<summary><code>fromTimestamp</code></summary>

Start timestamp for the range of data to decompose (inclusive).

Use `-` to denote the earliest timestamp in the series.
</details>

<details open>
<summary><code>toTimestamp</code></summary>

End timestamp for the range of data to decompose (inclusive).

Use `+` to denote the latest timestamp in the series.
</details>

## Optional arguments

<details open>
<summary><code>SEASONALITY</code></summary>

Controls how seasonal periods are determined. One of:

* `AUTO` (default) — Automatically detect seasonal periods from the data using a periodogram.
* `<period> [period ...]` — One or more seasonal periods (positive integers).
  - A single period uses STL decomposition.
  - Multiple periods (up to 4) use MSTL decomposition.

Examples:
- `SEASONALITY 24` — daily seasonality for hourly data
- `SEASONALITY 24 168` — daily and weekly seasonality for hourly data
- `SEASONALITY auto` — automatic detection

</details>

## Return

`TS.DECOMPOSE` returns an array of key-value pairs containing the decomposition results.

### STL response (single period)

```
1) "original"
2) 1) 1) (integer) <timestamp>
      2) (double) <value>
   2) ...
3) "trend"
4) 1) 1) (integer) <timestamp>
      2) (double) <value>
   2) ...
5) "seasonal"
6) 1) 1) (integer) <timestamp>
      2) (double) <value>
   2) ...
7) "residual"
8) 1) 1) (integer) <timestamp>
      2) (double) <value>
   2) ...
```

### MSTL response (multiple periods)

```
1) "original"
2) 1) 1) (integer) <timestamp>
      2) (double) <value>
   2) ...
3) "trend"
4) 1) 1) (integer) <timestamp>
      2) (double) <value>
   2) ...
5) "seasonal_components"
6) 1) 1) (integer) <period>
      2) 1) 1) (integer) <timestamp>
            2) (double) <value>
         2) ...
   2) ...
7) "residual"
8) 1) 1) (integer) <timestamp>
      2) (double) <value>
   2) ...
```

The components satisfy the identity: `original = trend + seasonal (+ seasonal_components) + residual`

## Examples

### Decompose with automatic seasonality detection

```
127.0.0.1:6379> TS.DECOMPOSE ts:sensor1 - + SEASONALITY auto
```

### Decompose with a known daily period

```
127.0.0.1:6379> TS.DECOMPOSE ts:sensor1 - + SEASONALITY 24
```

### Decompose with daily and weekly periods (MSTL)

```
127.0.0.1:6379> TS.DECOMPOSE ts:sensor1 - + SEASONALITY 24 168
```

## See also

- `TS.OUTLIERS` — Detect outliers in a time series
- `TS.AUTOFORECAST` — Automatically select and fit the best forecasting model
- `TS.RANGE` — Query a range of samples from a time series

## Complexity

`TS.DECOMPOSE` is O(n × p × i) where n is the number of samples, p is the number of seasonal periods,
and i is the number of inner/outer LOESS iterations.

The command blocks the client during computation and may be slow for very large time series.
Consider using `FILTER_BY_TS` or limiting the time range for better performance with large datasets.
