## TS.METRICNAMES

Searches metric names (`__name__` label values) with substring and optional fuzzy matching.

For a guide to searching, fuzzy matching and boolean search expressions shared by `TS.LABELNAMES`,
`TS.LABELVALUES` and `TS.METRICNAMES`, see [Label and Metric Discovery](../topics/label-discovery.md).

```text
TS.METRICNAMES
  [SEARCH term [term ...] | SEARCH_EXPR expression]
  [FUZZY_THRESHOLD threshold]
  [FUZZY_ALGORITHM subsequence|jarowinkler]
  [IGNORE_CASE]
  [INCLUDE_METADATA]
  [SORTBY <value|score|cardinality> [ASC|DESC]]
  [FILTER_BY_RANGE [NOT] fromTimestamp toTimestamp]
  [LIMIT limit]
  [HASHTAG hash_tag,...]
  [FILTER selector ...]
```

### Optional Arguments

- `SEARCH` terms are ORed together. A request may carry at most 32 terms, counting every `SEARCH`; more fail with
  `TSDB: too many SEARCH terms (maximum 32)`.
- `SEARCH_EXPR` takes one boolean search expression in place of `SEARCH` terms (the two are mutually exclusive):
  terms combined with `AND`, `OR`, `NOT` and parentheses, for example `"cpu AND NOT (idle OR steal)"`. `NOT` binds
  tighter than `AND`, which binds tighter than `OR`. Operators are case-insensitive; a double-quoted term such as
  `"and"` is searched for literally. Positive terms match as `SEARCH` terms do, honouring `FUZZY_ALGORITHM`,
  `FUZZY_THRESHOLD` and `IGNORE_CASE`; negated terms match as plain substrings. Every accepting path must require a
  positive term, so `NOT cpu` alone is refused. A match scores the best mean of its positive terms' scores. An
  expression is limited to 4096 bytes, 32 terms and 16 levels of nesting. See
  [Boolean Search Expressions](../topics/label-discovery.md#3-boolean-search-expressions).
- `FUZZY_ALGORITHM` accepts `subsequence` and `jarowinkler`. Defaults to `subsequence`.
- `FUZZY_THRESHOLD` is a whole percentage in `[0, 100]` and defaults to `0`; a threshold of `N` requires a fuzzy
  score of at least `N / 100`, and scores stay in `[0.0, 1.0]`. With `jarowinkler`, a value that contains a term
  always matches, and a threshold above `0` also accepts values whose Jaro-Winkler similarity to the term reaches
  it. With `subsequence`, a value that starts with a term always matches, and others must contain the term's
  characters in order and score at least the threshold. A threshold of `100` is not an exact match. See
  [Label and Metric Discovery](../topics/label-discovery.md).
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

- Without `SORTBY`, results are ordered by value ascending, with or without `SEARCH` or `SEARCH_EXPR`, as in
  the Prometheus search API. To rank matches by relevance, pass `SORTBY score`; `score` sorts best first.
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
TS.METRICNAMES HASHTAG tenant-a SEARCH cpu FUZZY_THRESHOLD 80 FUZZY_ALGORITHM jarowinkler SORTBY score DESC FILTER env=prod
```
