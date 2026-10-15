# TS.INCRBY

Increment the value of the latest sample in a time series, or create a new
series when the key does not exist.

## Syntax

```
TS.INCRBY key value
  [TIMESTAMP timestamp]
  [RETENTION duration]
  [DUPLICATE_POLICY policy]
  [ENCODING <COMPRESSED|UNCOMPRESSED>]
  [CHUNK_SIZE chunkSize]
  [METRIC metric | LABELS labelName labelValue ...]
  [IGNORE ignoreMaxTimediff ignoreMaxValDiff]
  [SIGNIFICANT_DIGITS significantDigits | DECIMAL_DIGITS <decimalDigits|none>]
```

## Required arguments

| Argument | Description |
| --- | --- |
| `key` | Key name for the time series. |
| `value` | Numeric increment to apply to the latest sample. Negative values decrement it. |

## Optional arguments

| Argument | Description | Default |
| --- | --- | --- |
| `TIMESTAMP timestamp` | Timestamp in milliseconds, or `*` for the current time. If omitted, an existing latest sample is updated at its timestamp; on a new series, the current time is used. | Existing latest timestamp or current time |
| `RETENTION duration` | Retention period in milliseconds. Duration expressions such as `1d` are also accepted. | Module configuration |
| `DUPLICATE_POLICY policy` | Policy used when creating the series: `BLOCK`, `FIRST`, `LAST`, `MIN`, `MAX`, or `SUM`. | `BLOCK` |
| `ENCODING <COMPRESSED\|UNCOMPRESSED>` | Storage encoding for a newly created series. | `COMPRESSED` |
| `CHUNK_SIZE chunkSize` | Chunk size in bytes for a newly created series. | `4096` |
| `METRIC metric` | Prometheus-style metric name for a newly created series. |—|
| `LABELS labelName labelValue ...` | Label name-value pairs for a newly created series. |—|
| `IGNORE ignoreMaxTimediff ignoreMaxValDiff` | Ignore the update when it is within both the time and value thresholds. | No filtering |
| `SIGNIFICANT_DIGITS significantDigits` | Round values to 1–16 significant digits. | No rounding |
| `DECIMAL_DIGITS decimalDigits` | Round values to 0–16 decimal places. `0` rounds to whole numbers; `none` turns rounding off. | No rounding |

`SIGNIFICANT_DIGITS` and `DECIMAL_DIGITS` are mutually exclusive. Creation
options are used only when `TS.INCRBY` auto-creates the series; they do not
alter an existing series.

## Return value

Returns the timestamp of the sample affected by the increment as an integer.

## Behavior

- If the key does not exist, `TS.INCRBY` creates the series and stores `value` as its first sample.
- If the key exists, the increment is added to its latest sample.
- An explicit timestamp must be equal to or greater than the latest timestamp. A timestamp equal to the latest sample updates that sample in place; a later timestamp appends a new sample with the accumulated value.
- `TIMESTAMP *` uses the current server time. Without `TIMESTAMP`, an existing series updates its latest timestamp, while a new series uses the current server time.
- An increment cannot be applied when the latest sample is `NaN`; `NaN` is also invalid as the increment value.
- Retention and compaction rules are applied after a successful update.
- Keyspace notifications use the `ts.incrby` event.

## Examples

### Basic increment

Create a series and add an increment:

```
TS.INCRBY sensor:requests 10
```

Increment the latest value again:

```
TS.INCRBY sensor:requests 5
```

The latest sample now has a value of `15`.

### Increment at an explicit timestamp

```
TS.INCRBY counter 25 TIMESTAMP 1609459200000
TS.INCRBY counter 5 TIMESTAMP 1609459201000
```

The command returns the timestamp used for the updated or appended sample.

### Create a series with options

```
TS.INCRBY temperature:room1 1.5 RETENTION 86400000 LABELS sensor_id 1 location living_room
```

### Apply rounding

```
TS.INCRBY measurement 1.23456 DECIMAL_DIGITS 2
```

## Errors

- `ERR wrong number of arguments` — A required argument is missing.
- `TSDB: invalid increase/decrease value` — The increment is not a valid number or is `NaN`.
- `TSDB: invalid timestamp` — The timestamp cannot be parsed.
- `TSDB: timestamp must be equal to or higher than the maximum existing timestamp` — The timestamp is older than the latest sample.
- `TSDB: cannot increment/decrement NaN value` — The latest sample is `NaN`.
- `WRONGTYPE` — The key exists but is not a time series.

## Complexity

O(1) amortized time.

## ACL categories

`@write`, `@fast`, `@timeseries`
