# TS.CARD

counts the number of distinct series.


```
TS.CARD
  [FILTER_BY_RANGE [NOT] rangeStart rangeEnd]
  [HASHTAG hash_tag,...]
  [FILTER selector...]
```
returns the number of unique time series that match a certain label set.

Without arguments, it returns the number of unique time series in the database.

On standalone servers, `FILTER_BY_RANGE` counts estimated to read more than 512 series run on
bounded background workers, releasing the module lock between batches. Literal equality and
inequality filters on non-empty values use their exact match count for this estimate, including
regexes the parser simplifies to those filters. Selectors requiring regular-expression, prefix,
substring or empty-value scans use the database's series count instead, and are planned on the
worker when that count exceeds the budget. Smaller requests, transactions and scripts run inline.
The workers and their queue are shared with `TS.OUTLIERS` and label-discovery reads; a full queue
returns `TSDB: series count: too many queued jobs (limit 256)`.

In cluster mode, `FILTER_BY_RANGE` counts read each shard in batches. Background and cluster
reads can observe writes between batches, so their results are not point-in-time snapshots.
The set of series a read visits is fixed when it is planned: when every filter is a literal
equality or inequality on non-empty values, that is when the command arrives, otherwise when
the read starts. A
background count that waits in the queue therefore misses series created in the meantime,
while series deleted before their batch is read are not counted.
Every matching series must be readable by the caller, even if the range excludes it.
Filtered counts without `FILTER_BY_RANGE` use the index directly, without per-key read checks,
and stay inline on standalone servers. Bare `TS.CARD` also stays inline and requires read
permission for all keys.

### Required arguments

<details open><summary><code>filter</code></summary>
Repeated series selector argument that selects the series to return. Optional.
</details>

### Optional Arguments

- rangeStart
Start timestamp, inclusive. Results will only be returned for series which have samples in the range `[rangeStart, rangeEnd]`

- rangeEnd
End timestamp, inclusive.

- HASHTAG hash_tag,...
In cluster mode, restricts fan-out to the nodes owning the comma-separated hash tags. It has no effect on standalone
servers and only selects cluster nodes; it does not filter series keys or labels.

#### Return

[Integer number](https://redis.io/docs/reference/protocol-spec#resp-integers) of unique time series.
The data section of the query result consists of a list of objects that contain the label name/value pairs which identify
each series.


#### Error

Return an error reply in the following cases:

TODO

#### Examples

```
TS.CARD HASHTAG tenant-a,tenant-b FILTER service=api
```
