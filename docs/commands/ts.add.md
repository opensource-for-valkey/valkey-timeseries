# TS.ADD

Add a sample to a time series.

## Syntax

```
TS.ADD key timestamp value
 [RETENTION duration]
 [DUPLICATE_POLICY policy]
 [ON_DUPLICATE policy_ovr]
 [ENCODING <COMPRESSED|UNCOMPRESSED>]
 [CHUNK_SIZE chunkSize]
 [METRIC metric | LABELS labelName labelValue ...]
 [IGNORE ignoreMaxTimediff ignoreMaxValDiff]
 [SIGNIFICANT_DIGITS significantDigits | DECIMAL_DIGITS decimalDigits]
```

## Required Arguments

| Argument    | Description                                                                          |
|-------------|--------------------------------------------------------------------------------------|
| `key`       | Key name for the time series                                                         |
| `timestamp` | Timestamp of the incoming sample (milliseconds since epoch, or `*` for current time) |
| `value`     | Value of the sample (double-precision floating-point number)                         |

## Optional Arguments

| Argument                                    | Description                                                                                                                   | Default                 |
|---------------------------------------------|-------------------------------------------------------------------------------------------------------------------------------|-------------------------|
| `RETENTION duration`                        | Data retention duration in milliseconds (or with time unit: `s`, `ms`, `m`, `h`, `d`)                                         | No limit                |
| `DUPLICATE_POLICY policy`                   | Policy for handling duplicate timestamps: `BLOCK`, `FIRST`, `LAST`, `MIN`, `MAX`, `SUM`                                       | `BLOCK`                 |
| `ON_DUPLICATE policy_ovr`                   | Override duplicate policy for this sample                                                                                     | Uses `DUPLICATE_POLICY` |
| `ENCODING <COMPRESSED\|UNCOMPRESSED>`       | Chunk encoding format                                                                                                         | `COMPRESSED`            |
| `CHUNK_SIZE chunkSize`                      | Size of memory chunks (bytes, must be multiple of 8, range `[4096..134217728]`, supports units: `kb`, `Ki`, `mb`, `Mi`, etc.) | `4096`                  |
| `METRIC metric`                             | Metric name (alternative to `LABELS` for simple metric identification)                                                        | —                       |
| `LABELS labelName labelValue ...`           | Key-value pairs for series labels (must have even number of arguments)                                                        | —                       |
| `IGNORE ignoreMaxTimediff ignoreMaxValDiff` | Ignore samples within `ignoreMaxTimediff` (ms) and `ignoreMaxValDiff` threshold                                               | No filtering            |
| `SIGNIFICANT_DIGITS significantDigits`      | Round values to N significant digits (1-16; see note below)                                                                   | No rounding             |
| `DECIMAL_DIGITS decimalDigits`              | Round values to N decimal places (0-16; `0` rounds to whole numbers)                                                          | No rounding             |

> **Rounding notes:** `SIGNIFICANT_DIGITS` and `DECIMAL_DIGITS` are mutually exclusive.
> `DECIMAL_DIGITS 0` rounds to whole numbers — it does not mean "no rounding"; omit the
> argument for that. `SIGNIFICANT_DIGITS` starts at 1, since "zero significant digits" is not a
> quantity. `SIGNIFICANT_DIGITS 16` is accepted but rounds nothing: 16 significant decimal
> digits is at the edge of a 64-bit float's precision, so rounding there would only introduce
> error; 15 is the largest value with an effect.

## Return Value

Returns the timestamp of the added sample as an integer.

## Behavior

- **Sample Handling:**
  - If the series doesn't exist, it is created with the provided options.
  - If `DUPLICATE_POLICY` is set and a sample at the same timestamp exists, the policy determines how it's handled.
  - If `IGNORE` is configured, samples within the specified time and value thresholds are ignored and not stored.
- **Notifications:** Keyspace notifications are sent for the `ts.add` event.
- **Compaction:** If the series has aggregation rules, automatic compaction may be triggered

## Examples

### Basic Usage

Add a sample with the current timestamp:

```
TS.ADD mykey * 42.5
```

### With Timestamp

Add a sample at a specific timestamp:

```
TS.ADD mykey 1609459200000 15.3
```

### With Series Options

Create or update a series with retention and labels:

```
TS.ADD temperature:room1 * 22.5 RETENTION 86400000 LABELS sensor_id 1 location living_room
```

### With Duplicate Policy

Add a sample using a specific duplicate handling policy:

```
TS.ADD stock:price * 152.30 DUPLICATE_POLICY LAST
```

### With Filtering

Add a sample with ignore thresholds (ignore samples within 5 seconds and 0.1 value difference):

```
TS.ADD sensor:data * 18.2 IGNORE 5000 0.1
```

### With Rounding

Add a sample with decimal digit rounding:

```
TS.ADD measurement * 3.14159265359 DECIMAL_DIGITS 2
```

## Error Responses

- `ERR wrong number of arguments` — Missing required arguments.
- `TSDB: invalid timestamp` — Timestamp cannot be parsed.
- `TSDB: invalid value` — Value is not a valid number, NaN, or infinite.
- `TSDB: invalid duration` — Retention duration cannot be parsed.
- `TSDB: invalid encoding` — Encoding must be `COMPRESSED` or `UNCOMPRESSED`.
- `TSDB: CHUNK_SIZE value must be...` — Invalid chunk size (not in range or not a multiple of 8).
- `TSDB: invalid duplicate policy` — Unknown duplicate policy.
- `TSDB: error running compaction...` — Internal error during automatic compaction.

## Complexity

O(1) amortized time to add a sample.

## ACL Categories

`@write`, `@fast`, `@timeseries`
