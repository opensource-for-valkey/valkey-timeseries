# TS.FEATURES

Compute a set of statistical features on a time series.

`TS.FEATURES` extracts named statistical features from a time series using the
`anofox-forecast` feature engineering library (tsfresh-compatible). Features can
be selected by category or specified individually.

## Syntax

```
TS.FEATURES key startTimestamp endTimestamp
    [CATEGORY <basic|distribution|autocorrelation|trend>,..]
    [FEATURE feature1,feature2,feature3..]
```

[Examples](#examples)

## Required arguments

<details open>
<summary><code>key</code></summary>

Key name for the time series to analyze.
</details>

<details open>
<summary><code>startTimestamp</code></summary>

Start timestamp for the range of data to analyze (inclusive).

Use `-` to denote the earliest timestamp in the series.
</details>

<details open>
<summary><code>endTimestamp</code></summary>

End timestamp for the range of data to analyze (inclusive).

Use `+` to denote the latest timestamp in the series.
</details>

## Optional arguments

<details open>
<summary><code>CATEGORY</code></summary>

A comma-separated list of feature categories to compute. Duplicate categories are rejected.

Available categories and the features they include:

| Category          | Included features                                                 |
|-------------------|-------------------------------------------------------------------|
| `basic`           | mean, median, variance, variance_sample, minimum, maximum, length |
| `distribution`    | skewness, kurtosis, quantiles at 0.25, 0.5, 0.75                  | 
| `autocorrelation` | autocorrelation at lags 1, 2, 3                                   |
| `trend`           | linear trend intercept, slope, p-value, r-squared                 |

Example: `CATEGORY basic,trend`
</details>

<details open>
<summary><code>FEATURE</code></summary>

A comma-separated list of individual feature names to compute. Feature names are
case-insensitive.

**Simple features** (no parameters):

*Basic:* `mean`, `median`, `variance`, `variance_sample`, `standard_deviation`,
`minimum`, `maximum`, `abs_energy`, `absolute_maximum`, `absolute_sum_of_changes`,
`length`, `mean_abs_change`, `mean_change`, `mean_second_derivative_central`,
`root_mean_square`, `sum_values`

*Distribution:* `skewness`, `kurtosis`, `variance_larger_than_std`,
`variation_coefficient`

*Counting:* `count_above_mean`, `count_below_mean`, `number_crossing_mean`,
`longest_strike_above_mean`, `longest_strike_below_mean`, `first_location_of_maximum`,
`first_location_of_minimum`, `last_location_of_maximum`, `last_location_of_minimum`,
`has_duplicate`, `has_duplicate_max`, `has_duplicate_min`

*Entropy:* `fourier_entropy`

*Trend:* `linear_trend_slope`, `linear_trend_intercept`, `linear_trend_r_squared`,
`linear_trend_p_value`, `augmented_dickey_fuller`

*Change:* `percentage_reoccurring_datapoints`, `percentage_reoccurring_values`,
`ratio_value_number_to_length`, `sum_of_reoccurring_data_points`,
`sum_of_reoccurring_values`

**Parameterized features** use the format `name:value`:

| Feature                   | Syntax                                          | Parameter            | Constraints               |
|---------------------------|-------------------------------------------------|----------------------|---------------------------|
| `quantile`                | `quantile:<q>`                                  | `q` — quantile value | Float between 0.0 and 1.0 |
| `autocorrelation`         | `autocorrelation:<lag>`                         | `lag` — lag value    | Positive integer          |
| `partial_autocorrelation` | `partial_autocorrelation:<lag>` or `pacf:<lag>` | `lag` — lag value    | Positive integer          |

Example: `FEATURE mean,median,quantile:0.5,autocorrelation:3`
</details>

## Return

`TS.FEATURES` returns a map of `{feature_name: value}` pairs. Each key is the
canonical feature name (e.g., `"mean"`, `"quantile_0.5"`, `"autocorrelation_3"`).
Features that produce `NaN` are returned as null.

The final feature list is the union of features from `CATEGORY` and `FEATURE`,
with duplicates removed (first occurrence wins).

Returns an error if:
* The key does not exist
* The key is not a time series
* No samples exist in the specified time range
* Neither `CATEGORY` nor `FEATURE` is specified
* A category or feature name is unrecognized
* A duplicate category is specified
* A parameterized feature has an invalid parameter

## Complexity

`TS.FEATURES` reads the samples in the specified time range and computes each
requested feature. Computation runs on a background thread. Performance is
linear in the number of samples and features.

## Examples

### Compute basic features

```valkey
127.0.0.1:6379> TS.CREATE temp:readings
OK
127.0.0.1:6379> TS.ADD temp:readings 1000 23.5
(integer) 1000
127.0.0.1:6379> TS.ADD temp:readings 2000 24.1
(integer) 2000
127.0.0.1:6379> TS.ADD temp:readings 3000 22.8
(integer) 3000
127.0.0.1:6379> TS.ADD temp:readings 4000 25.0
(integer) 4000
127.0.0.1:6379> TS.ADD temp:readings 5000 23.9
(integer) 5000
127.0.0.1:6379> TS.FEATURES temp:readings - + CATEGORY basic
1) "length"
2) (double) 5
3) "maximum"
4) (double) 25
5) "mean"
6) (double) 23.86
7) "median"
8) (double) 23.9
9) "minimum"
10) (double) 22.8
11) "variance"
12) (double) 0.8104000000000001
13) "variance_sample"
14) (double) 1.0130000000000001
```

### Compute specific parameterized features

```valkey
127.0.0.1:6379> TS.FEATURES temp:readings - + FEATURE quantile:0.5,autocorrelation:1,skewness
1) "autocorrelation_1"
2) (double) 0.12679363582220628
3) "quantile_0.5"
4) (double) 23.9
5) "skewness"
6) (double) 0.1490895255297672
```

### Combine categories and features

```valkey
127.0.0.1:6379> TS.FEATURES temp:readings 1000 5000 CATEGORY basic,trent FEATURE quantile:0.95
(error) TSDB: Unknown feature category: trent
127.0.0.1:6379> TS.FEATURES temp:readings 1000 5000 CATEGORY basic,trend FEATURE kurtosis,pacf:2
 1) "kurtosis"
 2) (double) -2.723634766793434
 3) "length"
 4) (double) 5
 5) "linear_trend_intercept"
 6) (double) 22.860000000000003
 7) "linear_trend_p_value"
 8) (double) 0.5928919556203283
 9) "linear_trend_r_squared"
10) (double) 0.10114942528735646
11) "linear_trend_slope"
12) (double) 0.40000000000000036
13) "maximum"
14) (double) 25
15) "mean"
16) (double) 23.86
17) "median"
18) (double) 23.9
19) "minimum"
20) (double) 22.8
21) "partial_autocorrelation_2"
22) (double) -0.3841584158415841
23) "variance"
24) (double) 0.8104000000000001
25) "variance_sample"
26) (double) 1.0130000000000001
```

### Error cases

```valkey
# Non-existent key
127.0.0.1:6379> TS.FEATURES nonexistent - + CATEGORY basic
(error) TSDB: the key does not exist

# Duplicate categories
127.0.0.1:6379> TS.FEATURES temp:readings - + CATEGORY basic,basic
(error) TSDB: duplicate category 'basic'

# Missing CATEGORY and FEATURE
127.0.0.1:6379> TS.FEATURES temp:readings - +
(error) TSDB: at least one of CATEGORY or FEATURE must be specified
```
