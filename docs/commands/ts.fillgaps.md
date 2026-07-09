# TS.FILLGAPS

Compute missing timestamps in a time series and return the samples that would
fill them, using a specified fill value (default `NaN`).

**The source series is never modified by this command.** Gap-filled samples
are only persisted when the `STORE` option is given, in which case they are
written to the specified destination key — never back into the source series.

## Syntax

```
TS.FILLGAPS key startTimestamp endTimestamp
  [VALUE value]
  [FREQUENCY duration]
  [ALIGN alignment_timestamp|start|-]
  [STORE destinationKey
    [MERGE]
    [RETENTION retentionPeriod]
    [ENCODING <pco|gorilla|uncompressed|compressed>]
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
| `startTimestamp` | Start timestamp for the gap-filling range (inclusive)           |
| `endTimestamp`   | End timestamp for the gap-filling range (inclusive)             |

## Optional Arguments

| Argument             | Description                                                                                    | Default    |
|----------------------|------------------------------------------------------------------------------------------------|------------|
| `VALUE value`        | Value to use when filling gaps (a double-precision float)                                      | `NaN`      |
| `FREQUENCY duration` | Step interval between consecutive timestamps (e.g., `1h`, `30m`, `1d`, `1000` for millis)     | Inferred   |
| `ALIGN alignment_timestamp` | Align timestamps to a frequency grid anchored at the given reference timestamp.<br>Use `start` or `-` to align to `startTimestamp`.<br>Example: `ALIGN 0` aligns to epoch (1970-01-01). | No alignment |
| `STORE destinationKey` | Write the filled gap samples to `destinationKey` instead of returning them directly. Accepts the same options as other `STORE` clauses (`MERGE`, `RETENTION`, `ENCODING`, `CHUNK_SIZE`, `DUPLICATE_POLICY`, `SIGNIFICANT_DIGITS`/`DECIMAL_DIGITS`, `METRIC`, `IGNORE`). | No STORE |

## Return Value

- **Without `STORE`:** An array of `[timestamp, value]` pairs for the computed
  gap-fill samples. The source series is not modified.
- **With `STORE`:** The number of samples written to the destination key, as
  an integer. If there are no gaps, the destination key is left untouched
  (and is not created if it did not already exist).

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

- **No side effects without STORE:** Without `STORE`, the command only computes and returns
  the gap-fill samples — the source series is not written to, no keyspace notification is
  sent, and nothing is replicated.

- **STORE:** With `STORE`, the filled gap samples are written to `destinationKey` using the
  same creation/write semantics as other `STORE` clauses (e.g. `TS.RANGE ... STORE`). A
  `ts.add`-style keyspace notification is sent and the write is replicated, exactly as if the
  samples had been added to `destinationKey` directly. If there are no gaps, the destination
  key is left untouched.

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
1) 1) (integer) 1609459203600
   2) "nan"
2) 1) (integer) 1609459210800
   2) "nan"
3) 1) (integer) 1609459214400
   2) "nan"
```

The computed samples are returned but not written to `temperature:room1`.

### Fill gaps with a custom fill value

```
127.0.0.1:6379> TS.ADD temperature:room1 1609459200000 22.5
(integer) 1609459200000
127.0.0.1:6379> TS.ADD temperature:room1 1609459218000 23.1
(integer) 1609459218000
127.0.0.1:6379> TS.FILLGAPS temperature:room1 1609459200000 1609459272000 FREQUENCY 1h VALUE 0.0
1) 1) (integer) 1609459203600
   2) "0"
2) 1) (integer) 1609459210800
   2) "0"
3) 1) (integer) 1609459214400
   2) "0"
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
1) 1) (integer) 1609459220000
   2) "nan"
2) 1) (integer) 1609459300000
   2) "nan"
3) 1) (integer) 1609459340000
   2) "nan"
```

### Persist filled gaps to a destination key with STORE

```
127.0.0.1:6379> TS.ADD stock:price 1609459200000 100.5
(integer) 1609459200000
127.0.0.1:6379> TS.ADD stock:price 1609459260000 101.2
(integer) 1609459260000
127.0.0.1:6379> TS.ADD stock:price 1609459380000 102.0
(integer) 1609459380000
127.0.0.1:6379> TS.FILLGAPS stock:price 1609459200000 1609459440000 FREQUENCY 1m ALIGN 0 STORE stock:price:gaps
(integer) 3
127.0.0.1:6379> TS.RANGE stock:price:gaps - +
1) 1) (integer) 1609459220000
   2) "nan"
2) 1) (integer) 1609459300000
   2) "nan"
3) 1) (integer) 1609459340000
   2) "nan"
```

`stock:price` itself is left untouched; only `stock:price:gaps` receives the filled samples.
