# TS.SANITIZE

Sanitize missing (NaN/infinite) values in a time series within a specified timestamp range.

## Syntax

```
TS.SANITIZE key fromTimestamp toTimestamp
    [POLICY <policy> [options]]
    [STORE destinationKey
        [MERGE]
        [RETENTION retentionPeriod]
        [ENCODING encoding]
        [CHUNK_SIZE chunkSize]
        [DUPLICATE_POLICY duplicatePolicy]
        [SIGNIFICANT_DIGITS significantDigits | DECIMAL_DIGITS decimalDigits]
        [METRIC metric]
        [IGNORE ignoreMaxTimediff ignoreMaxValDiff]
    ]
```

## Required Arguments

| Argument         | Description                                                     |
|------------------|-----------------------------------------------------------------|
| `key`            | Key name of the time series                                     |
| `fromTimestamp`  | Start timestamp for the sanitization range (inclusive)          |
| `toTimestamp`    | End timestamp for the sanitization range (inclusive)            |

## Optional Arguments

| Argument         | Description                                                     |
|------------------|-----------------------------------------------------------------|
| `POLICY`         | The imputation policy to apply (see below). Defaults to `DROP`. |

### STORE

Writes the sanitized samples to a destination key instead of returning them inline.

| Option                    | Description                                                     |
|---------------------------|-----------------------------------------------------------------|
| `destinationKey`          | Key name for the destination time series                        |
| `MERGE`                   | Merge sanitized samples into an existing destination key        |
| `RETENTION retentionPeriod`   | Maximum retention period for the destination                |
| `ENCODING encoding`       | Chunk encoding: `COMPRESSED` or `UNCOMPRESSED`                  |
| `CHUNK_SIZE chunkSize`    | Number of samples per memory chunk in the destination           |
| `DUPLICATE_POLICY policy` | Duplicate sample policy for the destination (`BLOCK`, `FIRST`, `LAST`, `MIN`, `MAX`, `SUM`) |
| `SIGNIFICANT_DIGITS digits` | Round to this many significant digits in the destination     |
| `DECIMAL_DIGITS digits`   | Round to this many decimal digits in the destination            |
| `METRIC metric`           | Metric name label for the destination series                    |
| `IGNORE maxTimeDiff maxValDiff` | Ignore samples that differ by these thresholds            |

Without `MERGE` (overwrite mode), the destination is cleared before writing.
With `MERGE`, sanitized samples are merged into an existing destination series.

## Policies

| Policy                    | Description                                                                 | Extra Args               |
|---------------------------|-----------------------------------------------------------------------------|--------------------------|
| `ERROR`                   | Return an error if any missing values are present                           | None                     |
| `DROP`                    | Drop all samples with missing values **(default)**                          | None                     |
| `FILL`                    | Replace missing values with a constant fill value                           | `value` (float)          |
| `FORWARDFILL`             | Forward-fill: carry the last valid observation forward                      | None                     |
| `BACKWARDFILL`            | Backward-fill: carry the next valid observation backward                    | None                     |
| `FILLMEAN`                | Replace missing values with the mean of all valid values                    | None                     |
| `FILLMEDIAN`              | Replace missing values with the median of all valid values                  | None                     |
| `INTERPOLATE`             | Linearly interpolate between valid neighbors                                | None                     |
| `FORWARDBACKWARDFILL`     | Forward-fill then backward-fill (handles both gaps and edge NaNs)           | None                     |
| `MOVINGAVERAGE`           | Replace with the mean of valid values in a centered window                  | `window` (int, must be odd and > 0) |
| `SEASONAL`                | Replace with the median of values at the same seasonal position             | `period` (int, > 0) or `auto` |

## Return Value

- **Without `STORE`:** Returns the sanitized samples as an array of `[timestamp, value]` pairs
  (same format as `TS.RANGE`).

- **With `STORE`:** Returns the number of samples written to the destination key as an integer.

## Behavior

- **Range:** Only samples within `[fromTimestamp, toTimestamp]` are considered. Samples outside this
  range are unaffected.

- **Missing values:** A value is considered "missing" if it is `NaN` (Not a Number), `+Inf`, or `-Inf`.

- **Default policy:** When `POLICY` is omitted, the `DROP` policy is applied. This removes all
  samples with missing values from the time series.

- **ERROR policy:** If any sample in the range has a missing value, the command returns an error.
  No data is modified.

- **DROP policy:** Samples with missing values are removed from the time series. Valid samples
  are preserved unchanged.

- **FILL policy:** Missing values are replaced with the specified constant.

- **FORWARDFILL policy:** Each missing value is replaced with the last valid (finite) value
  seen before it. Leading missing values (at the start of the range) remain unchanged.

- **BACKWARDFILL policy:** Each missing value is replaced with the next valid value seen after it.
  Trailing missing values (at the end of the range) remain unchanged.

- **FILLMEAN policy:** All missing values are replaced with the arithmetic mean of all valid
  (finite) values in the range.

- **FILLMEDIAN policy:** All missing values are replaced with the median of all valid values
  in the range.

- **INTERPOLATE policy:** Missing values between two valid observations are filled by linear
  interpolation. Edge missing values (at the start or end of the range) are filled with the
  nearest valid value.

- **FORWARDBACKWARDFILL policy:** First applies forward-fill, then backward-fill. This handles
  both interior gaps and edge missing values.

- **MOVINGAVERAGE policy:** Each missing value is replaced with the mean of valid values in a
  centered window around it. The window size must be odd and greater than 0. Multiple passes
  (up to 3) are used to handle adjacent missing values. Any remaining missing values are filled
  with the global mean.

- **SEASONAL policy:** Values are grouped by their position in a seasonal cycle
  (`index % period`). Each missing value is replaced with the median of valid values in the same
  seasonal position. The period must be greater than 0 and not exceed the number of samples.
  When `auto` is specified, the dominant period is detected automatically from the data.
  If more than 50% of values in any seasonal bucket are missing, the command returns an error.

- **Interpolation and range boundaries:** When interpolating within a restricted timestamp range,
  only samples within that range are considered. Samples outside the range are not used as
  interpolation anchors.

- **STORE behavior:** When `STORE` is specified, the sanitized samples are written to the
  destination key in addition to being applied to the source series. Without `MERGE`, the
  destination is overwritten. With `MERGE`, samples are merged into an existing destination
  using `KeepLast` semantics (duplicate timestamps are updated with the sanitized value).

- **Notifications:** Keyspace notifications are sent for the `ts.sanitize` event
  when samples are modified.

- **Replication:** The command is replicated to all replicas.

## Errors

- `TSDB: the key does not exist` — the specified key does not hold a time series.
- `TSDB: sanitize error: missing values` — the ERROR policy was specified and missing values
  were found.
- `TSDB: sanitize error: insufficient data` — not enough data for the SEASONAL policy
  (period exceeds the number of samples).
- `TSDB: sanitize error: invalid parameter` — an invalid policy parameter was provided
  (e.g., even window for MOVINGAVERAGE, or >50% missing in a seasonal bucket).
- `TSDB: MovingAverage window must be an odd positive integer` — the MOVINGAVERAGE window
  is even or zero.
- `TSDB: Seasonal period must be a positive integer` — the SEASONAL period is zero.
- `TSDB: unable to detect dominant period for seasonal imputation` — automatic period
  detection failed for SEASONAL with `auto`.
- `TSDB: invalid fill value` — the FILL policy value could not be parsed as a float.
- `TSDB: invalid argument` — an unknown policy name was specified.

## Examples

### Default policy (DROP)

```
127.0.0.1:6379> TS.ADD ts:metrics 1000 1.0
(integer) 1000
127.0.0.1:6379> TS.ADD ts:metrics 2000 NaN
(integer) 2000
127.0.0.1:6379> TS.ADD ts:metrics 3000 3.0
(integer) 3000
127.0.0.1:6379> TS.SANITIZE ts:metrics 1000 3000
1) 1) (integer) 1000
   2) 1.0
2) 1) (integer) 3000
   2) 3.0
127.0.0.1:6379> TS.RANGE ts:metrics - +
1) 1) (integer) 1000
   2) 1.0
2) 1) (integer) 3000
   2) 3.0
```

### Check for missing values

```
127.0.0.1:6379> TS.ADD temperature:room1 1000 22.5
(integer) 1000
127.0.0.1:6379> TS.ADD temperature:room1 2000 NaN
(integer) 2000
127.0.0.1:6379> TS.SANITIZE temperature:room1 1000 2000 POLICY ERROR
(error) TSDB: sanitize error: missing values
```

### Drop missing values

```
127.0.0.1:6379> TS.ADD ts:metrics 1000 1.0
(integer) 1000
127.0.0.1:6379> TS.ADD ts:metrics 2000 NaN
(integer) 2000
127.0.0.1:6379> TS.ADD ts:metrics 3000 3.0
(integer) 3000
127.0.0.1:6379> TS.SANITIZE ts:metrics 1000 3000 POLICY DROP
1) 1) (integer) 1000
   2) 1.0
2) 1) (integer) 3000
   2) 3.0
127.0.0.1:6379> TS.RANGE ts:metrics - +
1) 1) (integer) 1000
   2) 1.0
2) 1) (integer) 3000
   2) 3.0
```

### Fill missing with interpolation

```
127.0.0.1:6379> TS.ADD ts:temp 1000 20.0
(integer) 1000
127.0.0.1:6379> TS.ADD ts:temp 2000 NaN
(integer) 2000
127.0.0.1:6379> TS.ADD ts:temp 3000 30.0
(integer) 3000
127.0.0.1:6379> TS.SANITIZE ts:temp 1000 3000 POLICY INTERPOLATE
1) 1) (integer) 1000
   2) 20.0
2) 1) (integer) 2000
   2) 25.0
3) 1) (integer) 3000
   2) 30.0
```

### Fill missing with forward-fill

```
127.0.0.1:6379> TS.ADD ts:sensor 1000 10.0
(integer) 1000
127.0.0.1:6379> TS.ADD ts:sensor 2000 NaN
(integer) 2000
127.0.0.1:6379> TS.ADD ts:sensor 3000 NaN
(integer) 3000
127.0.0.1:6379> TS.SANITIZE ts:sensor 1000 3000 POLICY FORWARDFILL
1) 1) (integer) 1000
   2) 10.0
2) 1) (integer) 2000
   2) 10.0
3) 1) (integer) 3000
   2) 10.0
```

### Store sanitized result to a new key

```
127.0.0.1:6379> TS.ADD ts:raw 1000 1.0
(integer) 1000
127.0.0.1:6379> TS.ADD ts:raw 2000 NaN
(integer) 2000
127.0.0.1:6379> TS.ADD ts:raw 3000 3.0
(integer) 3000
127.0.0.1:6379> TS.SANITIZE ts:raw 1000 3000 POLICY DROP STORE ts:clean
(integer) 2
127.0.0.1:6379> TS.RANGE ts:clean - +
1) 1) (integer) 1000
   2) 1.0
2) 1) (integer) 3000
   2) 3.0
```

### Store with MERGE into an existing key

```
127.0.0.1:6379> TS.CREATE ts:merged
OK
127.0.0.1:6379> TS.ADD ts:merged 500 99.0
(integer) 500
127.0.0.1:6379> TS.ADD ts:src 1000 1.0
(integer) 1000
127.0.0.1:6379> TS.ADD ts:src 2000 NaN
(integer) 2000
127.0.0.1:6379> TS.ADD ts:src 3000 3.0
(integer) 3000
127.0.0.1:6379> TS.SANITIZE ts:src 1000 3000 POLICY DROP STORE ts:merged MERGE
(integer) 2
127.0.0.1:6379> TS.RANGE ts:merged - +
1) 1) (integer) 500
   2) 99.0
2) 1) (integer) 1000
   2) 1.0
3) 1) (integer) 3000
   2) 3.0
```
