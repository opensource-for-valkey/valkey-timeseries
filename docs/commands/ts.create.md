# TS.CREATE

create a timeseries.

```
TS.CREATE key
  [RETENTION retentionPeriod]
  [ENCODING <COMPRESSED|UNCOMPRESSED>]
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

### Required arguments

<details open><summary><code>key</code></summary>
is key name for the time series.
</details>

<details open><summary><code>metric</code></summary> 
The metric name in Prometheus format, e.g. `node_memory_used_bytes{hostname="host1.domain.com"}`
is key name for time series. See https://prometheus.io/docs/concepts/data_model/#metric-names-and-labels
</details>

### Optional Arguments

<details open><summary><code>retentionPeriod</code></summary>
The period of time for which to keep series samples. Retention can be specified as an integer indication
the duration as milliseconds, or a duration expression like `3wk`
</details>

<details open><summary><code>chunkSize</code></summary>
The chunk size for the timeseries, in bytes. Default is `4096`.
</details>

<details open><summary><code>dedupeInterval</code></summary>
Limits sample ingest to the timeseries. If a sample arrives less than `dedupeInterval` from the most
recent sample it is ignored. Default is `0`
</details>

```sh
127.0.0.1:6379> TS.CREATE req_total:post:handler:{us-east-1} METRIC api_http_requests_total{method="POST",handler="/messages"} CHUNK_SIZE 8192 DUPLICATE_POLICY SUM DEDUPE_INTERVAL 2s
```