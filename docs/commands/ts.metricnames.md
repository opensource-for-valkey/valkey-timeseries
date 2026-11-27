## TS.METRICNAMES

Searches metric names (`__name__` label values) with substring and optional fuzzy matching.

```text
TS.METRICNAMES
  [SEARCH term [term ...] | SEARCH_EXPR expression]
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
- `SEARCH_EXPR` takes one boolean search expression in place of `SEARCH` terms (the two are mutually exclusive):
  terms combined with `AND`, `OR`, `NOT` and parentheses, for example `"cpu AND NOT (idle OR steal)"`. `NOT` binds
  tighter than `AND`, which binds tighter than `OR`. Operators are case-insensitive; a double-quoted term such as
  `"and"` is searched for literally. Positive terms match as `SEARCH` terms do, honouring `FUZZY_ALGORITHM`,
  `FUZZY_THRESHOLD` and `IGNORE_CASE`; negated terms match as plain substrings. Every accepting path must require a
  positive term, so `NOT cpu` alone is refused. A match scores the best mean of its positive terms' scores. An
  expression is limited to 4096 bytes, 32 terms and 16 levels of nesting.
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

### Notes

- `SORTBY score DESC` and `SORTBY cardinality DESC` select the highest-ranked metric names
  before applying `LIMIT`. Equal scores or cardinalities are ordered by metric name ascending.
- A metric name's `cardinality` counts the series with that name, as `TS.LABELVALUES __name__` does: every series
  in the database without `FILTER`, only the selected ones with it. In cluster mode it is summed over the shards.
- On standalone servers, a search that would read many index entries runs on a bounded pool of
  background workers, shared with `TS.OUTLIERS`, and gives the index back to writers between
  batches. Small searches, and any search inside `MULTI`, a script or a module call, run inline.
  A background search is not a snapshot: it can observe changes made while it runs. When the
  workers' queue is full, the search fails with `TSDB: label search: too many queued jobs (limit 256)`.

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
