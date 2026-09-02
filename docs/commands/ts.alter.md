# TS.ALTER

Update the configuration and labels of an existing time series. `TS.ALTER`
does not create a series and does not change its storage encoding.

## Syntax

```
TS.ALTER key
  [RETENTION duration]
  [CHUNK_SIZE chunkSize]
  [DUPLICATE_POLICY policy]
  [METRIC metric | LABELS labelName labelValue ...]
  [IGNORE ignoreMaxTimediff ignoreMaxValDiff]
  [SIGNIFICANT_DIGITS significantDigits | DECIMAL_DIGITS decimalDigits]
```

## Arguments

- `key` — Existing time-series key to alter.
- `RETENTION duration` — Retention period in milliseconds. Duration expressions such as `1d` are also accepted; `0` disables retention.
- `CHUNK_SIZE chunkSize` — Chunk size in bytes for future storage allocation.
- `DUPLICATE_POLICY policy` — Policy for duplicate timestamps: `BLOCK`, `FIRST`, `LAST`, `MIN`, `MAX`, or `SUM`.
- `METRIC metric` — Replace the series labels with the labels parsed from a Prometheus-style metric name.
- `LABELS labelName labelValue ...` — Replace the complete label set. Provide no pairs to clear all labels.
- `IGNORE ignoreMaxTimediff ignoreMaxValDiff` — Ignore incoming samples within both the time and value thresholds.
- `SIGNIFICANT_DIGITS significantDigits` — Round values to 1–16 significant digits.
- `DECIMAL_DIGITS decimalDigits` — Round values to 0–16 decimal places. `0` rounds to whole numbers.

`SIGNIFICANT_DIGITS` and `DECIMAL_DIGITS` are mutually exclusive. Options
not included in the command remain unchanged. `ENCODING`, `ON_DUPLICATE`, and
`DEDUPE_INTERVAL` are not supported by `TS.ALTER`.

Changing `RETENTION` applies the new retention window immediately to existing
samples. Changing labels updates the label index. Existing samples are not
otherwise modified.

## Return value

Returns `OK` on success.

## Errors

- `ERR wrong number of arguments` — The key is missing.
- A key-does-not-exist error — The key does not exist.
- `WRONGTYPE` — The key exists but is not a time series.
- `TSDB: invalid duration` — The retention duration cannot be parsed.
- A chunk-size or rounding error — An option value is outside its valid range.
- `TSDB: invalid duplicate policy` — The duplicate policy is unknown.
- A label parsing error — Labels or a metric name are malformed.

## Complexity

O(1), excluding label-index maintenance when labels are changed and the
retention trim that runs when `RETENTION` is tightened. That trim drops whole
expired chunks and then rewrites the boundary chunk, so it is O(C + S), where
C is the number of expired chunks and S is the number of samples in the first
surviving chunk.

## ACL categories

`@write`, `@timeseries`

## Examples

Change retention and duplicate policy:

```
TS.ALTER temperature RETENTION 7d DUPLICATE_POLICY LAST
```

Replace the labels:

```
TS.ALTER temperature LABELS sensor room1 location kitchen
```

Clear all labels and configure ignore thresholds:

```
TS.ALTER temperature LABELS IGNORE 5000 0.1
```
