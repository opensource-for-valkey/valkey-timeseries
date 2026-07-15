# TS.JOIN

```
TS.JOIN leftKey rightKey fromTimestamp toTimestamp
    [[INNER] | [FULL] | [LEFT] | [RIGHT] | [ANTI] | [SEMI] | [ASOF [PREVIOUS | NEXT | NEAREST] [tolerance] ALLOW_EXACT_MATCH]]
    [FILTER_BY_TS ts...]
    [FILTER_BY_VALUE min max]
    [COUNT count]
    [REDUCE operator]
    [AGGREGATION aggregator bucketDuration [ALIGN align] [BUCKETTIMESTAMP timestamp] [EMPTY]]
```

> Note: unlike the RANGE command family, `TS.JOIN` accepts a single aggregator only; a
> comma-separated aggregation list is rejected.

Join two time series on sample timestamps. Performs an INNER join by default.

[Examples](#examples)

## Required arguments

<details open><summary><code>leftKey</code></summary>

is the key name for the time series being joined.
</details>

<details open><summary><code>rightKey</code></summary> 

is the key name for the right time series.

</details>

Both keys must have been created before `TS.JOIN` is called.

<details open><summary><code>fromTimestamp</code></summary>

`fromTimestamp` is the first timestamp or relative delta from the current time of the request range.

</details>

<details open><summary><code>toTimestamp</code></summary>

`toTimestamp` is the last timestamp of the requested range, or a relative delta from `fromTimestamp`
 
</details>



## Optional arguments

<details open><summary><code>LEFT</code></summary>
A `LEFT` join outputs the matching samples between both tables. In case no samples match from the left series, it returns 
those items with null values.

</details>

<details open><summary><code>RIGHT</code></summary>
A `RIGHT` join outputs all samples in the right series. In case no samples match from the left series, it returns
those items with null values.

</details>

<details open><summary><code>INNER</code></summary>
Specifies an `INNER` join. A row is generated for samples with matching timestamps in the selected range.

</details>

<details open><summary><code>ANTI</code></summary>
An `ANTI` join returns samples for which no matching timestamp exists in the `right` series.

</details>

<details open><summary><code>SEMI</code></summary>
An `SEMI` join returns samples for which no corresponding timestamp exists in the `right` series. It does not return any 
values from the right table.

The main purpose is to filter the left series based on the existence of related records in the right table, not to 
combine data from both tables.

</details>

<details open><summary><code>FULL</code></summary>

Specifies an FULL join. Returns samples from both left and right series. If no matching rows exist for the row in the left 
series, the value of the right series will have nulls. Correspondingly, the value of the left series will have nulls if 
there are no matching rows for the sample in the right series.

</details>

<details open><summary><code>ASOF [PREVIOUS | NEXT | NEAREST] tolerance [ALLOW_EXACT_MATCH [true|false]]</code></summary>

`ASOF` joins match each sample in the left series with the closest preceding or following sample in the right series based on 
timestamps. They are particularly useful for analyzing time-series data where records from different sources may not have 
perfectly aligned timestamps. ASOF joins solve the problem of finding the value of a varying property at a specific point in time.

#### How It Works
For each sample in the left table, the join finds the closest matching value from the right table.
- `PREVIOUS` selects the last row in the right series whose timeseries is less than or equal to the left’s timestamp.
- `NEXT` (default) selects the first row in the right series whose timestamp is greater than or equal to the left’s timestamp.
- `NEAREST` selects the last row in the right series whose timestamp is nearest to the left’s timestamp.

`tolerance` sets a limit on how far apart the timestamps can be while still considering them a match. 
The tolerance can be specified as:
 - An integer representing milliseconds
 - A duration specified as a string, e.g. 2m

`ALLOW_EXACT_MATCH` is a boolean flag that determines whether to allow exact matches between the left and right series.


If not specified, there is no tolerance limit (equivalent to an infinite tolerance). When set, JOIN ASOF will only match 
keys within the specified tolerance range. Any potential matches outside this range will be treated as no match.

The tolerance works in conjunction with the 'direction' parameter. 
 - For example, with strategy = `PREVIOUS` (the default), it looks for the nearest timestamp within the tolerance range that is less 
 than or equal to the timestamp of the left sample.


#### Example
Suppose we want to get the spreads between buy and sell trades in a trading application

```
TS.JOIN trades:buy trades:sell -1hr * ASOF NEAREST 2ms REDUCE sub
```

The result has all samples from the `buy` series joined with samples from the `sell` series. For each timestamp from the 
`buy` series, the query looks for a timestamp that nearest to it from the `sell` series, within a tolerance of
2 milliseconds. If no matching timestamp is found, NULL is inserted.

The `sub` transform function is then supplied to subtract the `sell` value from the `buy` value for each sample returned.

The tolerance parameter is particularly useful when working with time series data where exact matches are rare, but you 
want to find the closest match within a reasonable time frame. 

It helps prevent incorrect matches that might occur if the nearest available data point is too far away in time or value.

</details>

<details open><summary><code>COUNT count</code></summary>

the maximum number of samples to return. 
TODO: if used with aggregation, this specifies the number of returned buckets as opposed to the number of samples

</details>

<details open><summary><code>operator</code></summary> 

performs an operation on the value in each returned row.

 `operator` takes one of the following types:

  | `operator`   | Description                                                             |
  |--------------|-------------------------------------------------------------------------| 
  | `abs_diff`   | abs(`left` - `right`)                                                   |
  | `avg`        | Arithmetic mean of both mut values                                      |
  | `cmp`        | Returns 1 if `left` > `right`, -1 if `left` < `right`, 0 otherwise      |
  | `coalesce`   | return the first non-NaN item. If both are NaN, it returns NaN.         |
  | `div`        | `left` / `right`                                                        |
  | `eq`         | Returns 1 if `left` == `right`, 0 otherwise                             |
  | `gt`         | Returns 1 if `left` > `right`, otherwise returns 0                      |
  | `gte`        | Returns 1 if left is greater than or equals right, otherwise returns 0  |
  | `lt`         | Returns 1 if `left` > `right`, otherwise returns 0                      |
  | `lte`        | Returns 1 if `left` is less than or equals `right`, otherwise returns 0 |
  | `min`        | Minimum value                                                           |
  | `max`        | Maximum value                                                           | 
  | `mul`        | `left` * `right`                                                        |
  | `ne`         | Returns 1 if `left` equals `right`, otherwise returns 0                 |
  | `pct_change` | The percentage change of `right` over `left`                            |
  | `pow`        | `left` ^ `right`                                                        |
  | `sgn_diff`   | sgn(`left` - `right`)                                                   |
  | `sub`        | `left` - `right`                                                        |
  | `sum`        | `left` + `right`                                                        |

</details>

## Return value

Returns one of these replies:

- @simple-string-reply - `OK` if executed correctly
- @error-reply on error (invalid arguments, wrong key type, etc.), when `sourceKey` does not exist, when `destKey` does not exist, when `sourceKey` is already a destination of a compaction rule, when `destKey` is already a source or a destination of a compaction rule, or when `sourceKey` and `destKey` are identical

## Examples

<details open>
<summary><b>Create a compaction rule</b></summary>

Create a time series to store the temperatures measured in Mexico City and Toronto.

```
127.0.0.1:6379> TS.CREATE temp:CDMX temp{location="CDMX"}
OK
127.0.0.1:6379> TS.CREATE temp:TOR temp{location="TOR"}
OK
```

... Add data


Next, run the join.

```
127.0.0.1:6379> TS.JOIN temp:CDMX temp:TOR REDUCE min 
```

</details>

## See also

`TS.RANGE` | `TS.XCORR`
