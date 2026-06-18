# TS.FILLGAPS

Fill missing timestamps in a time series with a specified value (default `NaN`).

## Syntax

```
TS.FILLGAPS key startTimestamp endTimestamp
  [VALUE value]
  [FREQUENCY duration]
  [ALIGN alignment_timestamp|start|-]
```

## Required Arguments

| Argument         | Description                                                     |
|------------------|-----------------------------------------------------------------|
| `key`            | Key name of the time series                                     |
| `startTimestamp` | Start timestamp for the gap-filling range (inclusive)           |
| `endTimestamp`   | End timestamp for the gap-filling range (inclusive)             |

## Optional Arguments

| Argument             | Description                                                                                    | Default    |
|----------------------|------------------------------------------------------------------------------------------------|------------|
| `VALUE value`        | Value to use when filling gaps (a double-precision float)                                      | `NaN`      |
| `FREQUENCY duration` | Step interval between consecutive timestamps (e.g., `1h`, `30m`, `1d`, `1000` for millis)     | Inferred   |
| `ALIGN alignment_timestamp` | Align timestamps to a frequency grid anchored at the given reference timestamp.<br>Use `start` or `-` to align to `startTimestamp`.<br>Example: `ALIGN 0` aligns to epoch (1970-01-01). | No alignment |

## Return Value

Returns the number of gaps filled as an integer.

## Behavior

- **Frequency Inference:** When `FREQUENCY` is not specified, the command infers the dominant interval
  from the existing samples in the time series by finding the most common difference between
  consecutive timestamps. At least two samples are required for inference, and the modal interval
  must account for at least 50% of all intervals.

- **Gap Filling:** For each expected timestamp in the sequence that does not already have a sample,
  the value specified by `VALUE` is inserted (defaults to `NaN`). Existing samples are never overwritten.

- **Alignment:** When `ALIGN` is specified, the timestamp sequence starts from the nearest
  grid-aligned timestamp at or before `startTimestamp`, using the given alignment reference.
  The grid is defined by the frequency relative to that reference point. For example,
  `ALIGN 0` uses epoch (timestamp 0) as the reference; `ALIGN 500` shifts the grid by 500 ms.
  Only timestamps within `[startTimestamp, endTimestamp]` are filled; grid points before
  `startTimestamp` are skipped.

- **Notifications:** Keyspace notifications are sent for the `ts.fillgaps` event for each key
  where gaps were filled.

- **Replication:** The command is replicated to all replicas.

## Errors

- `TSDB: the key does not exist` — the specified key does not hold a time series.
- `TSDB: frequency must be positive` — the specified or inferred frequency is zero or negative.
- `TSDB: insufficient data to infer frequency; at least 2 samples required` — not enough
  samples to determine the frequency automatically.
- `TSDB: cannot infer frequency; no dominant interval found` — the intervals between samples
  are too irregular to determine a single frequency.
- `TSDB: invalid ALIGN timestamp value` — the value provided for `ALIGN` could not be parsed
  as a timestamp, `start`, or `-`.

## Examples

### Fill gaps in hourly data with explicit frequency

```
127.0.0.1:6379> TS.ADD temperature:room1 1609459200000 22.5
(integer) 1609459200000
127.0.0.1:6379> TS.ADD temperature:room1 1609459207200 23.1
(integer) 1609459207200
127.0.0.1:6379> TS.FILLGAPS temperature:room1 1609459200000 1609459218000 FREQUENCY 1h
(integer) 3
```

### Fill gaps with a custom fill value

```
127.0.0.1:6379> TS.ADD temperature:room1 1609459200000 22.5
(integer) 1609459200000
127.0.0.1:6379> TS.ADD temperature:room1 1609459218000 23.1
(integer) 1609459218000
127.0.0.1:6379> TS.FILLGAPS temperature:room1 1609459200000 1609459272000 FREQUENCY 1h VALUE 0.0
(integer) 3
```

### Fill gaps with inferred frequency and alignment to epoch

```
127.0.0.1:6379> TS.ADD stock:price 1609459200000 100.5
(integer) 1609459200000
127.0.0.1:6379> TS.ADD stock:price 1609459260000 101.2
(integer) 1609459260000
127.0.0.1:6379> TS.ADD stock:price 1609459380000 102.0
(integer) 1609459380000
127.0.0.1:6379> TS.FILLGAPS stock:price 1609459200000 1609459440000 FREQUENCY 1m ALIGN 0
(integer) 3
```
