## TS.LABELNAMES

Searches label names with substring and optional fuzzy matching.

For a guide to searching, fuzzy matching and boolean search expressions shared by `TS.LABELNAMES`,
`TS.LABELVALUES` and `TS.METRICNAMES`, see [Label and Metric Discovery](../topics/label-discovery.md).

```text
TS.LABELNAMES
  [FILTER_BY_RANGE [NOT] fromTimestamp toTimestamp]
  [SEARCH term [term ...] | SEARCH_EXPR expression]
  [FUZZY_THRESHOLD threshold]
  [FUZZY_ALGORITHM subsequence|jarowinkler]
  [IGNORE_CASE]
  [INCLUDE_METADATA]
  [SORTBY <value|score|cardinality> [ASC|DESC]]
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
  expression is limited to 4096 bytes, 32 terms and 16 levels of nesting. An empty expression is no
  expression, as in Prometheus. See
  [Boolean Search Expressions](../topics/label-discovery.md#3-boolean-search-expressions).
- `FUZZY_ALGORITHM` the fuzzy matching algorithm. Accepts `subsequence` and `jarowinkler`. Defaults to `subsequence`.
- `FUZZY_THRESHOLD` is a whole percentage in `[0, 100]` and defaults to `0`; a threshold of `N` requires a fuzzy
  score of at least `N / 100`, and scores stay in `[0.0, 1.0]`. With `jarowinkler`, a value that contains a term
  always matches, and a threshold above `0` also accepts values whose Jaro-Winkler similarity to the term reaches
  it. With `subsequence`, a value that starts with a term always matches, and others must contain the term's
  characters in order and score at least the threshold. A threshold of `100` is not an exact match. See
  [Label and Metric Discovery](../topics/label-discovery.md).
- `IGNORE_CASE` toggle case sensitivity in string matching. Defaults to `false`.
- `INCLUDE_METADATA` include to return `score` and `cardinality` in addition to label names.
- `SORTBY` determine how matching metrics should be sorted in the response.
- `FILTER_BY_RANGE` limits the result to only labels from series which have data in the date range [`fromTimestamp` ..
  `toTimestamp`]. If `NOT` is specified, this filter is inverted to exclude labels from series with data in the
  specified date range.
- `LIMIT` limits the number of results returned.
- `HASHTAG` restricts cluster fan-out to the nodes owning the comma-separated hash tags. It is ignored on standalone
  servers and only selects cluster nodes; it does not filter labels or series keys.
- `FILTER` is a repeated series selector argument that selects the series to search for matching label names. This
  argument is optional; if omitted, the command performs unscoped discovery across all series. In cluster mode, the
  command fans out to all shards (or, if `HASHTAG` is specified, only the shards owning the given hash tags) and
  merges results.
- In cluster mode this command fans out to all shards and merges results, unless `HASHTAG` is specified, in which case
  it fans out only to the shards owning the given hash tags.

### Notes

- On standalone servers, a search that would read many index entries runs on a bounded pool of
  background workers, shared with `TS.OUTLIERS`, and gives the index back to writers between
  batches. Small searches, and any search inside `MULTI`, a script or a module call, run inline.
  A background search is not a snapshot: it can observe changes made while it runs. When the
  workers' queue is full, the search fails with `TSDB: label search: too many queued jobs (limit 256)`.
- Without `SORTBY`, results are ordered by value ascending, with or without `SEARCH` or `SEARCH_EXPR`, as in
  the Prometheus search API. To rank matches by relevance, pass `SORTBY score`; `score` sorts best first.
- `SORTBY score` currently supports `DESC` only.
- `SORTBY value` orders by the label names themselves: `job` comes before `job2`.
- A label name's `cardinality` (reported with `INCLUDE_METADATA`, and ranked by `SORTBY cardinality`) is the
  number of series carrying it: of every series in the database without `FILTER`, and of the selected series with
  `FILTER` (and `FILTER_BY_RANGE`). In cluster mode it is summed over the shards.
- In cluster mode this command fans out to all shards and merges results, unless `HASHTAG` is specified, in which case
  it fans out only to the shards owning the given hash tags.

### Return

Map reply with the following fields:

- `results`: array of matching label names.
- `has_more`: boolean indicating whether more results are available.

### Example

```text
TS.LABELNAMES HASHTAG tenant-a,tenant-b SEARCH http_ FUZZY_THRESHOLD 80 FUZZY_ALGORITHM jarowinkler SORTBY score DESC FILTER service=api
```
