# TS.XCORR

Compute the cross-correlation function (CCF) between two time series.

`TS.XCORR` time-aligns two series via an inner join on matching timestamps (the
same alignment `TS.JOIN` performs by default), then computes the Pearson
correlation coefficient between the aligned value sequences at every lag in
`-maxLag..=maxLag`.

## Syntax

```
TS.XCORR key1 key2 fromTimestamp toTimestamp maxLag
```

[Examples](#examples)

## Required arguments

<details open>
<summary><code>key1</code></summary>

Key name for the first time series.
</details>

<details open>
<summary><code>key2</code></summary>

Key name for the second time series. Must differ from `key1`.
</details>

<details open>
<summary><code>fromTimestamp</code></summary>

Start timestamp for the range of data to analyze (inclusive).

Use `-` to denote the earliest timestamp.
</details>

<details open>
<summary><code>toTimestamp</code></summary>

End timestamp for the range of data to analyze (inclusive).

Use `+` to denote the latest timestamp.
</details>

<details open>
<summary><code>maxLag</code></summary>

Maximum lag (non-negative integer, in samples) to test in either direction.
The command computes correlation at every integer lag in `-maxLag..=maxLag`.
</details>

## Lag convention

For lag `h`, `key1[i]` is compared against `key2[i + h]` over the timestamp-aligned
sample sequence:

* `h > 0` — `key1` at time `t` is compared to `key2` at time `t + h`. A strong
  positive correlation here means **`key1` leads `key2`** by `h` steps.
* `h < 0` — `key2` at time `t` is compared to `key1` at time `t + |h|`. A strong
  positive correlation here means **`key2` leads `key1`** by `|h|` steps.
* `h == 0` — contemporaneous correlation, no lead/lag.

## Alignment

Only samples with **exactly matching timestamps** in both series contribute to
the computation — the same semantics as `TS.JOIN`'s default `INNER` join. Series
sampled on different or irregular grids will have few or no aligned points; use
`TS.FILLGAPS` / resampling via `TS.RANGE ... AGGREGATION` beforehand to put both
series on a common grid if needed.

## Return

`TS.XCORR` returns a map (key-value pairs) in RESP3 format:

| Key                | Type            | Description                                                        |
|--------------------|-----------------|----------------------------------------------------------------------|
| `lags`             | array of integer | The tested lags, `-maxLag..=maxLag`, in order                       |
| `values`           | array of double  | Correlation coefficient at each corresponding lag, range `[-1, 1]`  |
| `peak_lag`         | integer          | The lag with the largest absolute correlation                       |
| `peak_correlation` | double           | The (signed) correlation value at `peak_lag`                        |
| `n`                | integer          | Number of timestamp-aligned sample pairs used                       |

Returns an error if:
* Either key does not exist
* `key1` and `key2` are identical
* `maxLag` is negative
* Fewer than `maxLag + 2` timestamp-aligned sample pairs exist in the range

## Complexity

`TS.XCORR` is O(n × maxLag), where `n` is the number of timestamp-aligned sample
pairs in the range.

## Examples

### Detect a 2-step lead of one sensor over another

```valkey
127.0.0.1:6379> TS.XCORR sensor:upstream sensor:downstream - + 5
 1) "lags"
 2) 1) (integer) -5
    2) (integer) -4
    3) (integer) -3
    4) (integer) -2
    5) (integer) -1
    6) (integer) 0
    7) (integer) 1
    8) (integer) 2
    9) (integer) 3
   10) (integer) 4
   11) (integer) 5
 3) "values"
 4)  1) (double) 0.05
     2) (double) 0.08
     3) (double) 0.11
     4) (double) 0.20
     5) (double) 0.41
     6) (double) 0.63
     7) (double) 0.90
     8) (double) 0.55
     9) (double) 0.22
    10) (double) 0.10
    11) (double) 0.04
 5) "peak_lag"
 6) (integer) 2
 7) "peak_correlation"
 8) (double) 0.9
 9) "n"
10) (integer) 480
```

Here `peak_lag = 2` with a strong positive correlation means `sensor:upstream`
leads `sensor:downstream` by 2 samples.

### Contemporaneous correlation only

```valkey
127.0.0.1:6379> TS.XCORR temp:room1 temp:room2 - + 0
1) "lags"
2) 1) (integer) 0
3) "values"
4) 1) (double) 0.87
5) "peak_lag"
6) (integer) 0
7) "peak_correlation"
8) (double) 0.87
9) "n"
10) (integer) 240
```

### Error: identical keys

```valkey
127.0.0.1:6379> TS.XCORR sensor:a sensor:a - + 10
(error) TSDB: duplicate join keys
```

### Error: insufficient aligned data

```valkey
127.0.0.1:6379> TS.XCORR sensor:a sensor:b - + 100
(error) TSDB: insufficient aligned samples for MAXLAG 100. Need at least 102 timestamp-aligned samples between the two series, got 12
```

## See also

`TS.JOIN` | `TS.AUTOCORRELATION` | `TS.PERIODS`
