# TS.AUTOCORRELATION

Compute autocorrelation-based statistics on a time series.

## Syntax

```
TS.AUTOCORRELATION key startTime endTime lag 
    [PARTIAL | TRA | AGGREGATED mean|var|std|median]
```

[Examples](#examples)

## Required arguments

<details open>
<summary><code>key</code></summary>

Key name for the time series.
</details>

<details open>
<summary><code>startTime</code></summary>

Start timestamp for the range query. Use `-` for the earliest sample.
</details>

<details open>
<summary><code>endTime</code></summary>

End timestamp for the range query. Use `+` for the latest sample.
</details>

<details open>
<summary><code>lag</code></summary>

Lag value (positive integer) for the autocorrelation computation.
</details>

## Optional arguments

<details open>
<summary><code>PARTIAL</code></summary>

Returns the partial autocorrelation function (PACF) at the specified lag,
computed using the Durbin-Levinson algorithm.

PACF measures the correlation between observations at lag `k` after removing
the effects of correlations at smaller lags.
</details>

<details open>
<summary><code>TRA</code></summary>

Returns the time reversal asymmetry statistic at the specified lag.

Measures whether the time series looks the same when reversed in time.
A value close to zero indicates time-reversible dynamics. Positive or
negative values suggest directional asymmetry, which is often a signature
of non-linear processes (e.g., skewness in financial returns).
</details>

<details open>
<summary><code>AGGREGATED</code></summary>

Returns an aggregated autocorrelation statistic across lags 1..=lag.

Requires an aggregation function:

* `mean` - Mean of the autocorrelation values across lags 1..=lag
* `var` - Variance of the autocorrelation values across lags 1..=lag
* `std` - Standard deviation of the autocorrelation values across lags 1..=lag
* `median` - Median of the autocorrelation values across lags 1..=lag
</details>

## Return

`TS.AUTOCORRELATION` returns a double-precision floating point value representing
the computed statistic. Returns an error if:

* The key does not exist
* The key is not a time series
* There is insufficient data for the requested lag
* The computation results in NaN

## Complexity

`TS.AUTOCORRELATION` reads the full time series and computes the statistic.
For large time series, performance is linear in the number of samples.

`PARTIAL` (PACF) uses the Durbin-Levinson algorithm which requires O(lag²) additional
computation after the ACF values are computed.

## Examples

### Basic ACF

```valkey
127.0.0.1:6379> TS.CREATE temp:readings
OK
127.0.0.1:6379> TS.ADD temp:readings 1000 20.0
127.0.0.1:6379> TS.ADD temp:readings 2000 21.0
127.0.0.1:6379> TS.ADD temp:readings 3000 22.0
127.0.0.1:6379> TS.ADD temp:readings 4000 23.0
127.0.0.1:6379> TS.ADD temp:readings 5000 24.0
127.0.0.1:6379> TS.AUTOCORRELATION temp:readings - + 1
1
127.0.0.1:6379> TS.AUTOCORRELATION temp:readings - + 2
1
```

### Partial autocorrelation

```valkey
127.0.0.1:6379> TS.AUTOCORRELATION temp:readings - + 1 PARTIAL
1
127.0.0.1:6379> TS.AUTOCORRELATION temp:readings - + 2 PARTIAL
0
```

### Time reversal asymmetry

```valkey
127.0.0.1:6379> TS.AUTOCORRELATION temp:readings - + 1 TRA
0
```

### Aggregated autocorrelation

```valkey
127.0.0.1:6379> TS.AUTOCORRELATION temp:readings - + 3 AGGREGATED mean
1
127.0.0.1:6379> TS.AUTOCORRELATION temp:readings - + 3 AGGREGATED var
0
127.0.0.1:6379> TS.AUTOCORRELATION temp:readings - + 3 AGGREGATED std
0
127.0.0.1:6379> TS.AUTOCORRELATION temp:readings - + 3 AGGREGATED median
1
```
