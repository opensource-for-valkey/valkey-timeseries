# Command List

`TS.RANGE`, `TS.REVRANGE`, `TS.NRANGE` and `TS.NREVRANGE` support time-weighted average
aggregation with `AGGREGATION twa`. See [TS.RANGE](commands/ts.range.md#time-weighted-average)
for the interpolation, NaN and empty-bucket rules. `TS.MRANGE`, `TS.MREVRANGE`, `TS.JOIN`,
`TS.CREATERULE` and the `ts-compaction-policy` setting do not accept `twa` yet.

Standalone `TS.LABELNAMES`, `TS.LABELVALUES`, `TS.METRICNAMES` and `TS.QUERYLABELS` run on
bounded background workers when estimated work is large. `TS.QUERYINDEX` and
`TS.CARD FILTER_BY_RANGE` share these workers for reads estimated to exceed 512 series. Small requests, transactions and
scripts run inline, as do standalone `TS.CARD` counts without a range filter.

`TS.LABELNAMES`, `TS.LABELVALUES` and `TS.METRICNAMES` take either `SEARCH` terms or one
`SEARCH_EXPR` boolean expression (`AND`, `OR`, `NOT`, parentheses); the two are mutually exclusive.
Without `SORTBY` they order results by value ascending, even with a search; `SORTBY score` ranks matches.

Ranked label-value and metric-name searches apply `LIMIT` after ranking; equal scores or
cardinalities are ordered by value ascending.

`LATEST` on `TS.GET`, `TS.MGET`, `TS.RANGE`, `TS.REVRANGE`, `TS.MRANGE` and `TS.MREVRANGE`
recomputes a dirty compaction rule's open bucket from retained source samples, including before
the first write after load. Reads leave the saved rule state unchanged, so each one repeats the
scan until the next source write.

- [`TS.ADD`](commands/ts.add.md)
- [`TS.ADDBULK`](commands/ts.addbulk.md)
- [`TS.ALTER`](commands/ts.alter.md)
- [`TS.CARD`](commands/ts.card.md)
- [`TS.CREATE`](commands/ts.create.md)
- [`TS.CREATERULE`](commands/ts.createrule.md)
- [`TS.DECRBY`](commands/ts.decrby.md)
- [`TS.DEL`](commands/ts.del.md)
- [`TS.DELETERULE`](commands/ts.deleterule.md)
- [`TS.GET`](commands/ts.get.md)
- [`TS.INCRBY`](commands/ts.incrby.md)
- [`TS.INFO`](commands/ts.info.md)
- [`TS.JOIN`](commands/ts.join.md)
- [`TS.LABELNAMES`](commands/ts.labelnames.md)
- [`TS.LABELSTATS`](commands/ts.labelstats.md)
- [`TS.LABELVALUES`](commands/ts.labelvalues.md)
- [`TS.MADD`](commands/ts.madd.md)
- [`TS.MDEL`](commands/ts.mdel.md)
- [`TS.MGET`](commands/ts.mget.md)
- [`TS.METRICNAMES`](commands/ts.metricnames.md)
- [`TS.MRANGE`](commands/ts.mrange.md)
- [`TS.MREVRANGE`](commands/ts.mrevrange.md)
- [`TS.NRANGE`](commands/ts.nrange.md)
- [`TS.NREVRANGE`](commands/ts.nrevrange.md)
- [`TS.OUTLIERS`](commands/ts.outliers.md)
- [`TS.QUERYINDEX`](commands/ts.queryindex.md)
- [`TS.QUERYLABELS`](commands/ts.querylabels.md)
- [`TS.RANGE`](commands/ts.range.md)
- [`TS.READ`](commands/ts.read.md)
- [`TS.REVRANGE`](commands/ts.revrange.md)
- [`TS._DEBUG`](commands/ts._debug.md)
