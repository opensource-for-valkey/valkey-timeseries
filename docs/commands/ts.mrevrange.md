# TS.MREVRANGE

Reverse range query for multiple time series.

```
TS.MREVRANGE fromTimestamp toTimestamp
    [LATEST]
    [FILTER_BY_TS ts...]
    [FILTER_BY_VALUE min max]
    [WITHLABELS | SELECTED_LABELS label...]
    [COUNT count]
    [[ALIGN align] AGGREGATION aggregator[(op value)][,aggregator[(op value)]...] bucketDuration [BUCKETTIMESTAMP bt] [EMPTY]]
    [GROUPBY label REDUCE reducer[(op value)]]
    FILTER selector...
    [EXCLUDEEMPTY]
```

Identical to [TS.MRANGE](ts.mrange.md) except that samples are returned in
descending timestamp order; see that page for the full argument reference.
