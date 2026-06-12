# TS.STATIONARITY

Test whether a time series is stationary using statistical hypothesis tests.

Stationarity is a key property for many forecasting and analysis methods. A stationary
series has constant mean, variance, and autocorrelation over time — its statistical
properties do not depend on when you observe it. Non-stationary series (those with
trends, seasonality, or changing variance) often require differencing or transformation
before they can be modeled reliably.

`TS.STATIONARITY` uses the [anofox-forecast](https://docs.rs/anofox-forecast) crate to
run up to two complementary tests:

- **ADF (Augmented Dickey-Fuller)**: Tests the null hypothesis that the series has a
  unit root (i.e., is non-stationary). Rejection of the null implies stationarity.
- **KPSS (Kwiatkowski-Phillips-Schmidt-Shin)**: Tests the null hypothesis that the
  series is stationary. Rejection of the null implies non-stationarity.

Using both tests together (`TEST combined`, the default) provides a more robust
conclusion than either test alone.

## Syntax

```
TS.STATIONARITY key fromTimestamp toTimestamp
    [TEST adf|kpss|combined]
    [LAGS n]
```

[Examples](#examples)

## Required arguments

<details open>
<summary><code>key</code></summary>

Key name for the time series to test.
</details>

<details open>
<summary><code>fromTimestamp</code></summary>

Start timestamp for the range of data to test (inclusive).

Use `-` to denote the earliest timestamp in the series.
</details>

<details open>
<summary><code>toTimestamp</code></summary>

End timestamp for the range of data to test (inclusive).

Use `+` to denote the latest timestamp in the series.
</details>

## Optional arguments

<details open>
<summary><code>TEST</code></summary>

Which test to run. If omitted, defaults to `combined`.

- `adf` — Augmented Dickey-Fuller test only.
- `kpss` — KPSS test only.
- `combined` — Runs both ADF and KPSS and returns an overall conclusion.

When a single test is specified, the response includes the test statistic, p-value,
number of lags used, whether the series appears stationary according to that test, and
critical values at 1%, 5%, and 10% significance levels.

When `combined` is used, the response includes nested maps for both the ADF and KPSS
results, plus an overall conclusion:

| Conclusion | Meaning |
|---|---|
| `stationary` | ADF rejects the null (non-stationary) AND KPSS fails to reject the null (stationary) |
| `non_stationary` | ADF fails to reject the null (non-stationary) AND KPSS rejects the null (stationary) |
| `inconclusive` | The two tests disagree (both reject or both fail to reject) |

</details>

<details open>
<summary><code>LAGS n</code></summary>

Number of lags to use in the test (non-negative integer). Only valid when `TEST` is
`adf` or `kpss`.

- For `adf`: Controls the `max_lags` parameter of the ADF test. If omitted, a sensible
  default is used automatically — `(n-1)^(1/3)` where `n` is the number of observations.
- For `kpss`: Controls the lags parameter for the HAC (heteroskedasticity and
  autocorrelation consistent) variance estimator. If omitted, a sensible default is
  used — `4*(n/100)^0.25`.

Returns an error if `LAGS` is specified with `TEST combined`, since the combined test
function uses its own internal defaults.
</details>

## Return

`TS.STATIONARITY` returns a map (key-value pairs) in RESP3 format.

### Single test response (`TEST adf` or `TEST kpss`)

A flat map with 9 fields:

| Key            | Type    | Description                          |
|----------------|---------|--------------------------------------|
| `test`         | string  | `"adf"` or `"kpss"`                  |
| `conclusion`   | string  | `"stationary"` or `"non_stationary"` |
| `statistic`    | float   | Test statistic value                 |
| `pValue`       | float   | Approximate p-value                  |
| `lags`         | integer | Number of lags used                  |
| `isStationary` | integer | `1` if stationary, `0` otherwise     |
| `cv1pct`       | float   | Critical value at 1% significance    |
| `cv5pct`       | float   | Critical value at 5% significance    |
| `cv10pct`      | float   | Critical value at 10% significance   |

### Combined test response (`TEST combined`, or default)

A map with 4 top-level fields:

| Key          | Type   | Description                                                                                                     |
|--------------|--------|-----------------------------------------------------------------------------------------------------------------|
| `test`       | string | `"combined"`                                                                                                    |
| `conclusion` | string | `"stationary"`, `"non_stationary"`, or `"inconclusive"`                                                         |
| `adf`        | map    | Nested map with the 7 ADF fields: `statistic`, `pValue`, `lags`, `isStationary`, `cv1pct`, `cv5pct`, `cv10pct`  |
| `kpss`       | map    | Nested map with the 7 KPSS fields: `statistic`, `pValue`, `lags`, `isStationary`, `cv1pct`, `cv5pct`, `cv10pct` |

## Complexity

`TS.STATIONARITY` reads the full time series range (or the specified window) and runs
the selected statistical test(s). Both ADF and KPSS are O(n) in the number of
observations within the range. At least 10 observations are required; an error is
returned for smaller series.

## Examples

### Basic combined test

Test whether a time series is stationary using both ADF and KPSS:

```valkey
127.0.0.1:6379> TS.CREATE sensor:readings
OK
127.0.0.1:6379> TS.ADD sensor:readings 1000 1.2
127.0.0.1:6379> TS.ADD sensor:readings 2000 1.5
127.0.0.1:6379> TS.ADD sensor:readings 3000 1.3
127.0.0.1:6379> TS.ADD sensor:readings 4000 1.6
127.0.0.1:6379> TS.ADD sensor:readings 5000 1.4
127.0.0.1:6379> TS.ADD sensor:readings 6000 1.7
127.0.0.1:6379> TS.ADD sensor:readings 7000 1.5
127.0.0.1:6379> TS.ADD sensor:readings 8000 1.8
127.0.0.1:6379> TS.ADD sensor:readings 9000 1.6
127.0.0.1:6379> TS.ADD sensor:readings 10000 1.9
127.0.0.1:6379> TS.STATIONARITY sensor:readings - +
 1) "test"
 2) "combined"
 3) "conclusion"
 4) "stationary"
 5) "adf"
 6) 1) "statistic"
    2) (double) -3.8124
    3) "pValue"
    4) (double) 0.0028
    5) "lags"
    6) (integer) 2
    7) "isStationary"
    8) (integer) 1
    9) "cv1pct"
   10) (double) -3.7884
   11) "cv5pct"
   12) (double) -3.0137
   13) "cv10pct"
   14) (double) -2.6461
 7) "kpss"
 8) 1) "statistic"
    2) (double) 0.1245
    3) "pValue"
    4) (double) 0.1000
    5) "lags"
    6) (integer) 2
    7) "isStationary"
    8) (integer) 1
    9) "cv1pct"
   10) (double) 0.7392
   11) "cv5pct"
   12) (double) 0.4623
   13) "cv10pct"
   14) (double) 0.3468
```

### ADF test only

Run only the Augmented Dickey-Fuller test and inspect its critical values:

```valkey
127.0.0.1:6379> TS.STATIONARITY sensor:readings - + TEST adf
 1) "test"
 2) "adf"
 3) "conclusion"
 4) "stationary"
 5) "statistic"
 6) (double) -3.8124
 7) "pValue"
 8) (double) 0.0028
 9) "lags"
10) (integer) 2
11) "isStationary"
12) (integer) 1
13) "cv1pct"
14) (double) -3.7884
15) "cv5pct"
16) (double) -3.0137
17) "cv10pct"
18) (double) -2.6461
```

### KPSS test with custom lags

Override the default lag selection for the KPSS test:

```valkey
127.0.0.1:6379> TS.STATIONARITY sensor:readings - + TEST kpss LAGS 5
 1) "test"
 2) "kpss"
 3) "conclusion"
 4) "stationary"
 5) "statistic"
 6) (double) 0.0912
 7) "pValue"
 8) (double) 0.1000
 9) "lags"
10) (integer) 5
11) "isStationary"
12) (integer) 1
13) "cv1pct"
14) (double) 0.7392
15) "cv5pct"
16) (double) 0.4623
17) "cv10pct"
18) (double) 0.3468
```