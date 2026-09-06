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
