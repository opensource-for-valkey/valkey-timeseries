## TS.LABELNAMES

Searches label names with substring and optional fuzzy matching.

```text
TS.LABELNAMES
  [FILTER_BY_RANGE [NOT] fromTimestamp toTimestamp]
  [SEARCH term [term ...]]
  [FUZZY_THRESHOLD threshold]
  [FUZZY_ALGORITHM jarowinkler|subsequence]
  [IGNORE_CASE]
  [INCLUDE_METADATA]
  [SORTBY <value|score|cardinality> [ASC|DESC]]
  [LIMIT limit]
  [HASHTAG hash_tag,...]
  [FILTER selector ...]
```

### Optional Arguments

- `SEARCH` terms are ORed together.
- `FUZZY_ALGORITHM` the fuzzy matching algorithm. Accepts `jarowinkler` and `subsequence`. Defaults to `jarowinkler`.
- `FUZZY_THRESHOLD` accepts `[0.0, 1.0]`.
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

- `SEARCH` terms are ORed together.
- `FUZZY_THRESHOLD` accepts `[0.0, 1.0]`.
- `SORTBY score` currently supports `DESC` only.
- In cluster mode this command fans out to all shards and merges results, unless `HASHTAG` is specified, in which case
  it fans out only to the shards owning the given hash tags.

### Return

Map reply with the following fields:

- `results`: array of matching label names.
- `has_more`: boolean indicating whether more results are available.

### Example

```text
TS.LABELNAMES HASHTAG tenant-a,tenant-b SEARCH http_ FUZZY_THRESHOLD 0.80 FUZZY_ALGORITHM jarowinkler SORTBY score DESC FILTER service=api
```
