# TS.NRANGE

Query a range across an explicit list of time series, returning the results grouped by timestamp.

Where [TS.RANGE](./ts.range.md) answers "what did this series do?" and [TS.MRANGE](./ts.mrange.md)
answers "what did every series matching this filter do, series by series?", `TS.NRANGE` answers
"what did all of these series do *at the same moment*?" — it runs a compatible `TS.RANGE` over each
key and outer-joins the results on timestamp, so each reply row lines up every key's value for one
timestamp.

### Syntax

```bash
TS.NRANGE numkeys key [key ...] fromTimestamp toTimestamp
  [LATEST]
  [FILTER_BY_TS timestamp ...]
  [FILTER_BY_VALUE min max]
  [COUNT count]
  [[ALIGN align] AGGREGATION aggregators [aggregators ...] bucketDuration [BUCKETTIMESTAMP bt] [EMPTY]]
```

In cluster mode every key must hash to the same slot (use hash tags). `TS.NRANGE` is a single-slot
command: it does not split a request across shards or merge replies from several slots.

---

## Required Arguments

<details open><summary><code>numkeys</code></summary>
the number of time series keys that follow. Must be a positive integer and must equal the number of
<code>key</code> arguments.
</details>
<details open><summary><code>key [key ...]</code></summary>
the time series to query. Order is significant — the reply's values follow the key order — and
duplicates are allowed: a key listed twice contributes two (identical) columns. Every key must
exist; a missing key is an error, as it is for <code>TS.RANGE</code>.
</details>
<details open><summary><code>fromTimestamp</code></summary>
the start of the time range to query, inclusive. Accepts:
- Numeric timestamp in milliseconds
- `-` for the earliest sample among the specified series
- Duration spec (e.g., `2h` for 2 hours ago)
</details>
<details open><summary><code>toTimestamp</code></summary>
the end of the time range to query, inclusive. Accepts:
- Numeric timestamp in milliseconds
- `+` for the latest sample among the specified series
- `*` for the current time
- Duration spec (e.g., `30m` for 30 minutes ago)
</details>

## Optional Arguments

<details open><summary><code>LATEST</code></summary>
When a key is a compaction, also report its latest, possibly partial, bucket. Ignored for keys that
are not compactions.
</details>
<details open><summary><code>FILTER_BY_TS timestamp ...</code></summary>
Include only samples at the specified timestamp(s). Applied per key, before the join and before
aggregation.
</details>
<details open><summary><code>FILTER_BY_VALUE min max</code></summary>
Include only samples with values in `[min, max]`, both bounds inclusive. Applied per key, before the
join and before aggregation — a sample the filter removes leaves a `NaN` in that key's column rather
than dropping the timestamp, which another key may still report.
</details>
<details open><summary><code>COUNT count</code></summary>
Limit the reply to the first `count` rows. The limit is applied <em>after</em> the join, to whole
rows, and in the requested order: forward queries keep the lowest timestamps.
</details>
<details open><summary><code>AGGREGATION aggregators [aggregators ...] bucketDuration</code></summary>
Aggregate raw samples into fixed-size time buckets, with one aggregator argument per key, in key
order. The number of these arguments must equal <code>numkeys</code>, and every key shares the one
<code>bucketDuration</code>.

Each per-key argument is a single aggregator or a comma-separated list of up to 16 distinct
aggregators (e.g. `avg,max`), exactly as in [TS.RANGE](./ts.range.md); no whitespace is allowed
inside the argument. A key contributes one value per aggregator it names, and its values appear
together in the reply in that order.

A [filtered aggregator](./ts.range.md#filtered-aggregators) (`countif`, `sumif`, `all`, `any`,
`none`, `share`) **requires** an inline condition — `aggregator(op value)`, e.g. `countif(>5)` — and
`count`/`sum` accept the same form optionally. Conditions are per aggregator, so different keys (and
different aggregators for one key) can use different ones.
</details>
<details open><summary><code>ALIGN align</code></summary>
Control bucket alignment for every key:
  - `start` (or `-`) — align buckets to the range start, which cannot then be `-`
  - `end` (or `+`) — align buckets to the range end, which cannot then be `+`
  - Numeric timestamp — align all buckets to a specific timestamp
  - If omitted, alignment is `0`.
</details>
<details open><summary><code>BUCKETTIMESTAMP bt</code></summary>
Which timestamp to report for each bucket:

- `start` or `-` (default) — bucket start time
- `end` or `+` — bucket end time
- `mid` or `~` — bucket midpoint (rounded down)

</details>
<details open><summary><code>EMPTY</code></summary>
Also report aggregations for empty buckets. A bucket reported for one key but missing from another
leaves that key's values `NaN`. As in `TS.RANGE`, no data is reported for buckets that end before a
series' earliest sample or begin after its latest one.
</details>

## Supported Aggregators

The same set [TS.RANGE](./ts.range.md#supported-aggregators) supports — simple, statistical,
counter/rate, and filtered — including this module's extensions (`countall`, `countnan`, `increase`,
`irate`, `rate`, `countif`, `sumif`, `share`, `all`, `any`, `none`). `twa` is not supported.

---

## Return Value

An array with one entry per distinct timestamp, ordered from the lowest timestamp to the highest
(use [TS.NREVRANGE](./ts.nrevrange.md) for the opposite order).
Each entry is `[timestamp, [value ...]]`, where the values are concatenated across keys in input
order: one value per key without `AGGREGATION`, one value per aggregator with it.

A value is `NaN` when the key has no sample at that timestamp (or no samples in that bucket). A
stored or aggregated `NaN` is reported the same way, so the two cases are indistinguishable in the
reply.

The reply is an empty array when no samples match.

---

## Examples

### Pivot raw samples from several series

```
127.0.0.1:6379> TS.MADD {sensor}:1 1000 10 {sensor}:1 2000 12
127.0.0.1:6379> TS.MADD {sensor}:2 1000 20 {sensor}:2 3000 25
127.0.0.1:6379> TS.NRANGE 2 {sensor}:1 {sensor}:2 - +
1) 1) (integer) 1000
   2) 1) 10
      2) 20
2) 1) (integer) 2000
   2) 1) 12
      2) NaN
3) 1) (integer) 3000
   2) 1) NaN
      2) 25
```

### One aggregator per key

`{sensor}:3` is averaged and `{sensor}:4` summed, both over 1000 ms buckets:

```
127.0.0.1:6379> TS.NRANGE 2 {sensor}:3 {sensor}:4 - + AGGREGATION avg sum 1000
1) 1) (integer) 1000
   2) 1) 15
      2) 20
2) 1) (integer) 2000
   2) 1) 30
      2) 25
```

### Several aggregators for one key

Each timestamp's values are a single flat list — `{sensor}:3`'s `avg`, then its `max`, then
`{sensor}:4`'s `sum`:

```
127.0.0.1:6379> TS.NRANGE 2 {sensor}:3 {sensor}:4 - + AGGREGATION avg,max sum 1000
1) 1) (integer) 1000
   2) 1) 15
      2) 20
      3) 20
2) 1) (integer) 2000
   2) 1) 30
      2) 30
      3) 25
```

### Filtered aggregators per key

Count each key's samples above its own threshold, per minute:

```
127.0.0.1:6379> TS.NRANGE 2 {app}:errors {app}:latency - + AGGREGATION countif(>0) countif(>250) 60000
```

### Latest rows only

`COUNT` applies to joined rows, so this returns the three most recent timestamps covered by either
key:

```
127.0.0.1:6379> TS.NRANGE 2 {sensor}:1 {sensor}:2 - + COUNT 3
```

---

## Notes

- `TS.NRANGE` is equivalent to running a compatible `TS.RANGE` per key and outer-joining the results
  on timestamp; every option except `COUNT` is applied per key, before the join.
- The reply carries no key names or labels. Columns are identified by position, which is why key
  order and duplicates are preserved exactly as given.

## See Also

[TS.NREVRANGE](./ts.nrevrange.md) | [TS.RANGE](./ts.range.md) | [TS.MRANGE](./ts.mrange.md) | [TS.JOIN](./ts.join.md)
