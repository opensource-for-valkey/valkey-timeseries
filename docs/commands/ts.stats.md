# TS.STATS

Compute per-series statistical metrics for exploratory data analysis.

`TS.STATS` calculates descriptive statistics over a time series to help profile
data before modeling. It returns metrics including length, central tendency (mean,
median), dispersion (standard deviation, min, max, range), value distribution
(zeros, positive/negative counts, nulls, unique values, constancy), plateau
characteristics, and data-quality indicators (skewness, kurtosis,
leading/trailing zeros).

This command is inspired by the `anofox_fcst_ts_stats` function from the
[AnoFox forecast extension](https://anofox.com/docs/forecast/eda).

## Syntax

```
TS.STATS key [fromTimestamp toTimestamp]
```

[Examples](#examples)

## Required arguments

<details open>
<summary><code>key</code></summary>

Key name for the time series to analyze.
</details>

## Optional arguments

<details open>
<summary><code>fromTimestamp toTimestamp</code></summary>

Start and end timestamps for the range of data to analyze (inclusive).

If omitted, stats are computed over the entire series.

Use `-` to denote the earliest timestamp, and `+` to denote the latest.
</details>

## Return

`TS.STATS` returns a map of key-value pairs with the following fields:

| Field                  | Type    | Description                                          |
|------------------------|---------|------------------------------------------------------|
| `length`               | Integer | Number of observations in the range                  |
| `start_timestamp`      | Integer | First timestamp in the range                         |
| `end_timestamp`        | Integer | Last timestamp in the range                          |
| `mean`                 | Float   | Average value (excluding NaN/Inf)                    |
| `std`                  | Float   | Population standard deviation                        |
| `min`                  | Float   | Minimum value                                        |
| `max`                  | Float   | Maximum value                                        |
| `range`                | Float   | Difference between max and min                       |
| `median`               | Float   | Median value                                         |
| `n_nans`               | Integer | Count of NaN/Inf (null) values                       |
| `n_zeros`              | Integer | Count of exactly-zero values                         |
| `n_positive`           | Integer | Count of positive values                             |
| `n_negative`           | Integer | Count of negative values                             |
| `n_unique_values`      | Integer | Count of distinct non-null values                    |
| `is_constant`          | Integer | 1 if all non-null values are identical, else 0       |
| `plateau_size`         | Integer | Longest run of consecutive identical values          |
| `plateau_size_non_zero`| Integer | Longest run of consecutive identical non-zero values |
| `n_zeros_start`        | Integer | Count of leading zeros                               |
| `n_zeros_end`          | Integer | Count of trailing zeros                              |
| `skewness`             | Float   | Sample skewness                                      |
| `kurtosis`             | Float   | Sample excess kurtosis                               |


## Examples

<details open>
<summary><code>TS.STATS</code> on a time series</summary>

Create a time series and compute its statistics:

```
127.0.0.1:6379> TS.CREATE ts:temperature
OK
127.0.0.1:6379> TS.ADD ts:temperature 1000 22.5
(integer) 1000
127.0.0.1:6379> TS.ADD ts:temperature 2000 23.1
(integer) 2000
127.0.0.1:6379> TS.ADD ts:temperature 3000 22.8
(integer) 3000
127.0.0.1:6379> TS.ADD ts:temperature 4000 0
(integer) 4000
127.0.0.1:6379> TS.ADD ts:temperature 5000 22.9
(integer) 5000
127.0.0.1:6379> TS.STATS ts:temperature
 1) "length"
 2) (integer) 5
 3) "start_timestamp"
 4) (integer) 1000
 5) "end_timestamp"
 6) (integer) 5000
 7) "mean"
 8) "18.26"
 9) "std"
10) "9.1741084453075019"
11) "min"
12) "0"
13) "max"
14) "23.1"
15) "range"
16) "23.1"
17) "median"
18) "22.8"
19) "n_nans"
20) (integer) 0
21) "n_zeros"
22) (integer) 1
23) "n_positive"
24) (integer) 4
25) "n_negative"
26) (integer) 0
27) "n_unique_values"
28) (integer) 5
29) "is_constant"
30) (integer) 0
31) "plateau_size"
32) (integer) 1
33) "plateau_size_non_zero"
34) (integer) 1
35) "n_zeros_start"
36) (integer) 0
37) "n_zeros_end"
38) (integer) 0
39) "skewness"
40) "-1.4804125726197922"
41) "kurtosis"
42) "1.4870482402064657"
```

</details>

<details open>
<summary><code>TS.STATS</code> with a timestamp range</summary>

Compute statistics over a specific time window:

```
127.0.0.1:6379> TS.STATS ts:temperature 2000 4000
 1) "length"
 2) (integer) 3
 3) "start_timestamp"
 4) (integer) 2000
 5) "end_timestamp"
 6) (integer) 4000
 7) "mean"
 8) "15.3"
 9) "std"
10) "10.833267815159709"
11) "min"
12) "0"
13) "max"
14) "23.1"
15) "range"
16) "23.1"
17) "median"
18) "22.8"
19) "n_nans"
20) (integer) 0
21) "n_zeros"
22) (integer) 1
23) "n_positive"
24) (integer) 2
25) "n_negative"
26) (integer) 0
27) "n_unique_values"
28) (integer) 3
29) "is_constant"
30) (integer) 0
31) "plateau_size"
32) (integer) 1
33) "plateau_size_non_zero"
34) (integer) 1
35) "n_zeros_start"
36) (integer) 0
37) "n_zeros_end"
38) (integer) 1
39) "skewness"
40) "-0.5685587568459284"
41) "kurtosis"
42) "-1.4285154028284064"
```

</details>

## See also

- `TS.RANGE` – Query raw values from a time series
- `TS.PERIODS` – Detect seasonal periods in a time series
- `TS.TREND` – Fit trend components to a time series
