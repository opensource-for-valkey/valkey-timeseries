# TS.JOIN

```
TS.JOIN leftKey rightKey fromTimestamp toTimestamp
    [INNER | FULL | LEFT | RIGHT | ANTI | SEMI | ASOF [PREVIOUS | NEXT | NEAREST] [tolerance] [ALLOW_EXACT_MATCH [true|false]]]
    [FILTER_BY_TS ts...]
    [FILTER_BY_VALUE min max]
    [COUNT count]
    [REDUCE operator]
    [AGGREGATION aggregator bucketDuration [ALIGN align] [BUCKETTIMESTAMP timestamp] [EMPTY]]
```

Join two time series on sample timestamps. Performs an `INNER` join by default.

> Note: unlike the RANGE command family, `TS.JOIN` accepts a single aggregator only; a
> comma-separated aggregation list is rejected.

[Examples](#examples)

## Required arguments

<details open><summary><code>leftKey</code></summary>

is the key name of the left time series.
</details>

<details open><summary><code>rightKey</code></summary>

is the key name of the right time series. It must differ from `leftKey`.
</details>

Both keys must exist before `TS.JOIN` is called.

<details open><summary><code>fromTimestamp</code></summary>

is the first timestamp of the requested range, `-` for each series' earliest sample, `*` for
the current time, or an offset from the current time such as `-1h`.
</details>

<details open><summary><code>toTimestamp</code></summary>

is the last timestamp of the requested range, `+` for each series' latest sample, `*` for the
current time, or an offset from `fromTimestamp` such as `+30m`.

Both inputs are read over this range, clipped to each series' retention window.
</details>

## Optional arguments

### Join type

A series holds at most one sample per timestamp, so an exact join pairs at most one left
sample with one right sample.

<details open><summary><code>INNER</code></summary>

(default) returns a row for every timestamp present in both series.
</details>

<details open><summary><code>LEFT</code></summary>

returns a row for every left sample. When the right series has no sample at that timestamp,
the right side of the row is null.
</details>

<details open><summary><code>RIGHT</code></summary>

returns a row for every right sample. When the left series has no sample at that timestamp,
the left side of the row is null.
</details>

<details open><summary><code>FULL</code></summary>

returns a row for every timestamp present in either series, with a null side where one
series has no sample.
</details>

<details open><summary><code>SEMI</code></summary>

returns the left samples whose timestamp is also present in the right series. Right values
are not returned: the purpose is to filter the left series by the existence of matching
right samples.
</details>

<details open><summary><code>ANTI</code></summary>

returns the left samples whose timestamp is **not** present in the right series.
</details>

<details open><summary><code>ASOF [PREVIOUS | NEXT | NEAREST] [tolerance] [ALLOW_EXACT_MATCH [true|false]]</code></summary>

matches each left sample with one nearby right sample. This is useful when the two series
are sampled at different times, for example to find the value of a slowly changing
property at the time of each event.

- `PREVIOUS` (default) selects the last right sample at or before the left sample.
- `NEXT` selects the first right sample at or after the left sample.
- `NEAREST` selects the right sample nearest to the left sample. When two are equally near,
  the later one is selected.

A left sample with no match is omitted from the result. Only right samples inside the
requested range are candidates.

`tolerance` is the maximum distance between the two timestamps, as milliseconds (`500`) or a
duration (`2m`). Without it there is no limit; a tolerance of `0` matches exact timestamps
only. The tolerance applies to the sample the strategy selects: with `PREVIOUS`, a left
sample whose previous right sample is further away than the tolerance has no match.

`ALLOW_EXACT_MATCH` (default `true`) controls whether a right sample at exactly the left
sample's timestamp may be selected. With `false`, `PREVIOUS` looks strictly before the left
sample, `NEXT` strictly after it, and `NEAREST` at the nearest sample on either side other
than the exact one.

Example: the spread between buy and sell trades, matching each buy with the sell nearest
to it within 2 milliseconds.

```
TS.JOIN trades:buy trades:sell -1h * ASOF NEAREST 2ms REDUCE sub
```
</details>

### Filters

<details open><summary><code>FILTER_BY_TS ts...</code></summary>

restricts **both** series to samples at the listed timestamps, before joining.
</details>

<details open><summary><code>FILTER_BY_VALUE min max</code></summary>

restricts **both** series to samples with values in `[min, max]`, before joining.
</details>

### Result shaping

<details open><summary><code>COUNT count</code></summary>

is the maximum number of rows to return; with `AGGREGATION`, the maximum number of buckets.
The limit applies to the joined result, not to the samples read from each series.
</details>

<details open><summary><code>REDUCE operator</code></summary>

reduces each row to a single value, replacing the row with a sample. The sample's timestamp
is the left sample's, or the right sample's on a right-only row; an `ASOF` row reports the
left timestamp. `REDUCE` cannot be combined with `SEMI` or `ANTI`.

On a `LEFT`, `RIGHT` or `FULL` row with no sample on one side, that side is *missing*. A
stored `NaN` value counts as missing too. `coalesce`, `max` and `min` use the operand that is
present; every other operator returns `NaN` when an operand is missing.

| `operator`   | Result                                                               |
|--------------|----------------------------------------------------------------------|
| `abs_diff`   | abs(`left` - `right`)                                                |
| `avg`        | (`left` + `right`) / 2                                               |
| `cmp`        | `1` if `left` > `right`, `-1` if `left` < `right`, `0` if equal      |
| `coalesce`   | `left`, or `right` when `left` is missing                            |
| `div`        | `left` / `right`; `NaN` when `right` is 0                            |
| `eq`         | `1` if `left` == `right`, otherwise `0`                              |
| `gt`         | `1` if `left` > `right`, otherwise `0`                               |
| `gte`        | `1` if `left` >= `right`, otherwise `0`                              |
| `lt`         | `1` if `left` < `right`, otherwise `0`                               |
| `lte`        | `1` if `left` <= `right`, otherwise `0`                              |
| `max`        | the larger value                                                     |
| `min`        | the smaller value                                                    |
| `mod`        | `left` % `right`                                                     |
| `mul`        | `left` * `right`                                                     |
| `ne`         | `1` if `left` != `right`, otherwise `0`                              |
| `pct_change` | (`right` - `left`) / `left`, as a fraction (`0.5` is +50%); `NaN` when `left` is 0 |
| `pow`        | `left` ^ `right`                                                     |
| `sgn_diff`   | sign of (`left` - `right`): `1`, `-1` or `0`                         |
| `sub`        | `left` - `right`                                                     |
| `sum`        | `left` + `right`                                                     |

Comparisons are numeric: `-0` equals `+0`.
</details>

<details open><summary><code>AGGREGATION aggregator bucketDuration [ALIGN align] [BUCKETTIMESTAMP timestamp] [EMPTY]</code></summary>

aggregates the joined samples into buckets, as `TS.RANGE` does. It needs one value per row:
a `REDUCE` operator, or `SEMI`/`ANTI`, which aggregate the left samples.

`EMPTY` reports the empty buckets between the first and the last joined sample. Unlike
`TS.RANGE`, it does not extend the fill to the edges of the requested range: the joined
stream has no data outside the window to extend towards.
</details>

## Return value

@array-reply of rows, in timestamp order:

- With `REDUCE` or `AGGREGATION`, each row is a sample, `[timestamp, value]`.
- Otherwise each row is a pair `[leftSample, rightSample]`, where each side is
  `[timestamp, value]` or null when that side has no sample. `SEMI` and `ANTI` rows are
  `[leftSample, null]`.

@error-reply on invalid arguments, when either key does not exist or is not a time series,
when `leftKey` and `rightKey` are the same, or when the user may not read either key.

A join that reads more than 65,536 samples runs in the background: the client is blocked
until the result is ready, and the server keeps serving other clients meanwhile. If too many
background jobs are already queued, the command fails with
`TSDB: TS.JOIN: too many queued jobs`. Inside `MULTI`, a script or a module call, a join
always runs inline.

With `COUNT`, a join over larger inputs is first tried inline, since it usually needs only
the start of each window. It moves to the background only if filling `COUNT` would read more
than 65,536 samples: for example, an `INNER` join whose inputs share few timestamps.

## Examples

<details open>
<summary><b>Compare two temperature series</b></summary>

```
127.0.0.1:6379> TS.ADD temp:CDMX 1000 18
(integer) 1000
127.0.0.1:6379> TS.ADD temp:CDMX 2000 20
(integer) 2000
127.0.0.1:6379> TS.ADD temp:TOR 2000 9
(integer) 2000
127.0.0.1:6379> TS.ADD temp:TOR 3000 7
(integer) 3000
```

An inner join returns the timestamp both series share:

```
127.0.0.1:6379> TS.JOIN temp:CDMX temp:TOR - +
1) 1) 1) (integer) 2000
      2) "20"
   2) 1) (integer) 2000
      2) "9"
```

A full join with `REDUCE min` returns one sample per timestamp; `min` uses the side that is
present:

```
127.0.0.1:6379> TS.JOIN temp:CDMX temp:TOR - + FULL REDUCE min
1) 1) (integer) 1000
   2) "18"
2) 1) (integer) 2000
   2) "9"
3) 1) (integer) 3000
   2) "7"
```

`ANTI` returns the Mexico City readings Toronto has no reading for:

```
127.0.0.1:6379> TS.JOIN temp:CDMX temp:TOR - + ANTI
1) 1) 1) (integer) 1000
      2) "18"
   2) (nil)
```
</details>

## See also

`TS.RANGE`
