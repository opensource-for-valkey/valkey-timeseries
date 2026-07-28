# TS.ALTER

alter a timeseries.

```
TS.ALTER key
  [RETENTION retentionPeriod]
  [CHUNK_SIZE chunkSize]
  [DUPLICATE_POLICY policy]
  [DEDUPE_INTERVAL duplicateTimediff]
  [[LABELS [label value ...] | METRIC metricName]
```
#### Options
- **ENCODING**: The encoding to use for the timeseries: `CHIMP`, `GORILLA`, `UNCOMPRESSED`, or
  `COMPRESSED` (an alias for the configured default compressed encoding). Default is `COMPRESSED`,
  which resolves to `CHIMP` unless `ts-encoding` says otherwise.
- **DUPLICATE_POLICY**: The policy to use for duplicate samples. Default is `BLOCK`.

## Required arguments

<details open><summary><code>key</code></summary>
is key name for the time series.
</details>

## Optional Arguments
<details open><summary><code>retentionPeriod</code></summary>
The period of time for which to keep series samples. Retention can be specified as an integer indication
the duration as milliseconds, or a duration expression like `3wk`
</details>

<details open><summary><code>chunkSize</code></summary>
The chunk size for the timeseries, in bytes. Default is `4096`.
</details>
