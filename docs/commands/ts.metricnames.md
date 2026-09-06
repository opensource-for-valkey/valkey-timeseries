## TS.METRICNAMES

Searches metric names (`__name__` label values) with substring and optional fuzzy matching.

```text
TS.METRICNAMES
  [SEARCH term [term ...]]
  [FUZZY_THRESHOLD threshold]
  [FUZZY_ALGORITHM jarowinkler|subsequence]
  [IGNORE_CASE]
  [INCLUDE_METADATA]
  [SORTBY <value|score|cardinality> [ASC|DESC]]
  [FILTER_BY_RANGE [NOT] fromTimestamp toTimestamp]
  [LIMIT limit]
  [HASHTAG hash_tag,...]
  [FILTER selector ...]
```

### Optional Arguments

- `SEARCH` terms are ORed together.
- `FUZZY_ALGORITHM` accepts `jarowinkler` and `subsequence`. Defaults to `jarowinkler`.
- `FUZZY_THRESHOLD` accepts `[0.0, 1.0]`.
- `IGNORE_CASE` toggles case sensitivity in string matching. Defaults to `false`.
- `INCLUDE_METADATA` include to return `score` and `cardinality` in addition to label names.
- `SORTBY` specify the sort order of the results.
- `FILTER_BY_RANGE` limits results to series with data in the given range. With `NOT`, this filter is inverted.
- `LIMIT` bounds the number of results returned.
- `HASHTAG` restricts cluster fan-out to the nodes owning the comma-separated hash tags. It has no effect on standalone
  servers and only selects cluster nodes; it does not filter metric names or series keys.
- `FILTER` applies one or more series selectors.
- In cluster mode this command fans out to all shards and merges results.

### Return

Map reply with the following fields:

- `results`: the matching metric names.
- `has_more`: indicates whether additional results are available.
  When `INCLUDE_METADATA` is `false`, `results` contains metric name strings.
  When `INCLUDE_METADATA` is `true`, `results` contains tuples with the metric name and its metadata, including `score`
  and `cardinality`.

### Example

```text
TS.METRICNAMES HASHTAG tenant-a SEARCH cpu FUZZY_THRESHOLD 0.80 FUZZY_ALGORITHM jarowinkler SORTBY score DESC FILTER env=prod
```
