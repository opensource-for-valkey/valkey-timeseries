# TS.MDEL

Delete samples from multiple time series matching label filters.

## Syntax

```
TS.MDEL [fromTimestamp toTimestamp] [HASHTAG hash_tag,...] FILTER ...
```

## Required arguments

### FILTER

Label filters to select time series. At least one filter is required.

## Optional arguments

### HASHTAG hash_tag,...

In cluster mode, restricts the fan-out to the primary nodes owning the specified hash tags. `HASHTAG` must appear before
`FILTER`. It has no effect on standalone servers.

### fromTimestamp toTimestamp

Time range for sample deletion. When specified, only samples within the range `[fromTimestamp, toTimestamp]` (inclusive)
are deleted from matching series.

When omitted, entire time series matching the filters are deleted.

**Timestamp formats:**

- Unix timestamp in milliseconds
- `-` for minimum timestamp
- `+` for maximum timestamp

## Return value

Returns an integer representing the total number of samples or series deleted.

## Complexity

O(N*M) where N is the number of matching time series and M is the number of samples in the time range (or 1 for full
series deletion).

## Examples

### Delete samples in a time range

```bash
127.0.0.1:6379> TS.MDEL 1000 2000 FILTER iot_sensors{type="temperature"}
(integer) 150
```

Deletes all temperature sensor samples between timestamps 1000 and 2000.

### Delete entire time series

```bash
127.0.0.1:6379> TS.MDEL FILTER api_latency{region="us-west",status="inactive"}
(integer) 3
```

Deletes all api latency time series that have both `region=us-west` and `status=inactive` labels.

## Notes

- The command automatically replicates to replicas and AOF
- In cluster mode, without `HASHTAG`, the operation fans out to one primary node per shard. With `HASHTAG`, it targets
  only the primary nodes that own the supplied tags.
- Deleted samples trigger compaction on the affected series
- Keyspace events are emitted for each modified series (`ts.del`)
- Use with caution – overly broad filters may delete more data than intended

## Related commands

- [`TS.DEL`](/commands/ts.del/) - Delete samples from a single time series
- [`TS.RANGE`](/commands/ts.range/) - Query time range from a single series
- [`TS.MRANGE`](/commands/ts.mrange/) - Query time range from multiple series
- [`TS.QUERYINDEX`](/commands/ts.queryindex/) - Find series matching label filters
