# TS.NREVRANGE

Reverse range query across an explicit list of time series, grouped by timestamp.

```
TS.NREVRANGE numkeys key [key ...] fromTimestamp toTimestamp
    [LATEST]
    [FILTER_BY_TS timestamp ...]
    [FILTER_BY_VALUE min max]
    [COUNT count]
    [[ALIGN align] AGGREGATION aggregators [aggregators ...] bucketDuration [BUCKETTIMESTAMP bt] [EMPTY]]
```

Identical to [TS.NRANGE](ts.nrange.md) except that rows are returned in descending timestamp
order; see that page for the full argument reference.

`COUNT` limits rows in the order they are returned, so here it keeps the **highest** timestamps —
the last rows `TS.NRANGE` would return, not the first.

```
127.0.0.1:6379> TS.NREVRANGE 2 {sensor}:1 {sensor}:2 - +
1) 1) (integer) 3000
   2) 1) NaN
      2) 25
2) 1) (integer) 2000
   2) 1) 12
      2) NaN
3) 1) (integer) 1000
   2) 1) 10
      2) 20
```
