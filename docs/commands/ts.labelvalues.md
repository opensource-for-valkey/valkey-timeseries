### TS.LABELVALUES

#### Syntax

```
TS.LABELVALUES label
  [FILTER_BY_RANGE [NOT] fromTimestamp toTimestamp]
  [SEARCH term [term ...] | SEARCH_EXPR expression]
  [FUZZY_THRESHOLD threshold]
  [FUZZY_ALGORITHM jarowinkler|subsequence]
  [INCLUDE_METADATA]
  [IGNORE_CASE]
  [SORTBY <value|score|cardinality> [ASC|DESC]]
  [LIMIT limit]
  [HASHTAG hash_tag,...]
  [FILTER selector ...]
```

returns a list of label values for a provided label name.

### Required Arguments

<details open><summary><code>label</code></summary>
The label name for which to retrieve values.
</details>

### Optional Arguments

<details open><summary><code>fromTimestamp</code></summary>
If specified along with `toTimestamp`, this limits the result to only labels from series which
have data in the date range [`fromTimestamp` .. `toTimestamp`]
</details>

<details open><summary><code>toTimestamp</code></summary>
If specified along with `fromTimestamp`, this limits the result to only labels from series which
have data in the date range [`fromTimestamp` .. `toTimestamp`]
</details>

<details open><summary><code>HASHTAG hash_tag,...</code></summary>
In cluster mode, restricts fan-out to the nodes owning the comma-separated hash tags. It has no effect on a
standalone server and does not filter label values or series keys.
</details>

### Notes

- `SEARCH` terms are ORed together.
- `SEARCH_EXPR` takes one boolean search expression in place of `SEARCH` terms (the two are mutually exclusive):
  terms combined with `AND`, `OR`, `NOT` and parentheses, for example `"cpu AND NOT (idle OR steal)"`. `NOT` binds
  tighter than `AND`, which binds tighter than `OR`. Operators are case-insensitive; a double-quoted term such as
  `"and"` is searched for literally. Positive terms match as `SEARCH` terms do, honouring `FUZZY_ALGORITHM`,
  `FUZZY_THRESHOLD` and `IGNORE_CASE`; negated terms match as plain substrings. Every accepting path must require a
  positive term, so `NOT cpu` alone is refused. A match scores the best mean of its positive terms' scores. An
  expression is limited to 4096 bytes, 32 terms and 16 levels of nesting.
- `FUZZY_THRESHOLD` accepts `[0.0, 1.0]` and defaults to `0.0`. With `jarowinkler`, a value that contains a
  term always matches, and a threshold above `0.0` also accepts values whose Jaro-Winkler similarity to the term
  reaches it. With `subsequence`, a value that starts with a term always matches, and others must contain the term's
  characters in order and score at least the threshold. A threshold of `1.0` is not an exact match. See
  [Label and Metric Discovery](../topics/label-discovery.md).
- `SORTBY score DESC` and `SORTBY cardinality DESC` select the highest-ranked values before
  applying `LIMIT`. Equal scores or cardinalities are ordered by value ascending.
- A value's `cardinality` (reported with `INCLUDE_METADATA`, and ranked by `SORTBY cardinality`) is the number of
  series carrying it: of every series in the database without `FILTER`, and of the selected series with `FILTER`
  (and `FILTER_BY_RANGE`). In cluster mode it is summed over the shards.
- On standalone servers, a search that would read many index entries runs on a bounded pool of
  background workers, shared with `TS.OUTLIERS`, and gives the index back to writers between
  batches. Small searches, and any search inside `MULTI`, a script or a module call, run inline.
  A background search is not a snapshot: it can observe changes made while it runs. When the
  workers' queue is full, the search fails with `TSDB: label search: too many queued jobs (limit 256)`.

#### Return

The data section of the JSON response is a list of string label mut values.

#### Error

Return an error reply in the following cases:

- Invalid options.
- TODO.

#### Examples

This example queries for all label mut values for the job label:
```
TS.LABELVALUES job HASHTAG tenant-a FILTER service=api
```
```json
{
   "status" : "success",
   "data" : [
      "node",
      "prometheus"
   ]
}
```
