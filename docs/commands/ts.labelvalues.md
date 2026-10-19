### TS.LABELVALUES

#### Syntax

```
TS.LABELVALUES label
  [FILTER_BY_RANGE [NOT] fromTimestamp toTimestamp]
  [SEARCH term [term ...]]
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

- `SORTBY score DESC` and `SORTBY cardinality DESC` select the highest-ranked values before
  applying `LIMIT`. Equal scores or cardinalities are ordered by value ascending.
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
