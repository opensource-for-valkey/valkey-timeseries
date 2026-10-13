# PromQL

Valkey TimeSeries provides a Prometheus-compatible query language for data stored in
time series, so applications can filter, transform, aggregate, and compare multiple 
labeled series without first copying the data into a separate Prometheus-compatible store.
For PromQL language concepts and selector syntax, see the [Prometheus querying
basics](https://prometheus.io/docs/prometheus/latest/querying/basics/) reference.

The PromQL interface is exposed through two read-only commands:

| Command | Use |
| --- | --- |
| [`TS.QUERY`](../commands/ts.query.md) | Evaluate an expression at one point in time. |
| [`TS.QUERYRANGE`](../commands/ts.queryrange.md) | Evaluate an expression repeatedly over a start/end range at a fixed step. |

## Data model

Each time series is selected as a Prometheus-style metric. Its metric name is represented
by the `__name__` label, and labels attached when the series is created are available for
matching and grouping:

```text
http_requests_total{service="api", region="us-east"}
```

Selectors support exact, negative, regular-expression, and negative regular-expression
matchers (`=`, `!=`, `=~`, and `!~`). A selector may be combined with arithmetic,
comparisons, logical set operators, aggregations, and functions.

Examples:

```text
http_requests_total{service="api"}
rate(http_requests_total{service="api"}[5m])
sum by (region) (rate(http_requests_total[5m]))
cpu_usage{host=~"web-[0-9]+"} > 0.8
```

## Query types

### Instant queries

`TS.QUERY` returns a scalar, vector, matrix, or string result depending on the
expression. The default evaluation time is now; `TIME` can select an explicit timestamp
or a duration relative to now.

```text
TS.QUERY sum(rate(http_requests_total[5m])) TIME 1672531200000
```

The response is a map with `resultType` and `result` fields. Vector and matrix elements
contain a `metric` label map and one or more timestamp/value pairs. Scalar results contain
one timestamp/value pair.

### Range queries

`TS.QUERYRANGE` evaluates the expression at every point on an inclusive `START`/`END`
grid. `STEP` is required and accepts duration strings such as `15s`, `1m`, and `1h`.

```text
TS.QUERYRANGE "rate(http_requests_total[5m])" \
  STEP 1m START -1h END *
```

The result is a matrix: each returned series has its labels and its samples for the
evaluation grid. `START` and `END` use milliseconds for bare integer timestamps; use an
RFC3339 timestamp or a decimal number when the unit should be unambiguous.

Both commands accept `LOOKBACK_DELTA`, `TIMEOUT`, and `HASHTAG` overrides. See the command
pages for the complete timestamp and argument rules.

### Restricting a query to part of a cluster

`HASHTAG hash_tag,...` restricts the fan-out to the shards owning the given hash tags,
exactly as it does for `TS.MRANGE`, `TS.MGET`, and the metadata commands. Tags are
comma-separated, and several tags select the union of their owning shards. A braced tag
such as `{tenant-a}` is equivalent to the bare tag `tenant-a`. Repeating the clause
replaces the scope rather than widening it — the last occurrence wins.

```text
TS.QUERY sum(rate(http_requests_total[5m])) TIME 1672531200000 HASHTAG tenant-a,tenant-b

TS.QUERYRANGE "rate(http_requests_total[5m])" \
  STEP 1m START -1h END * HASHTAG tenant-a
```

The scope applies to every selector in the expression, including the ones inside
subqueries and the ones read by aggregation and rollup push-down, so the whole expression
is evaluated over one shard set.

> **Warning:** `HASHTAG` scopes *shards*, not series. It is neither a label filter nor a
> key-name filter, and it adds no predicate to the PromQL expression. Once a shard is
> selected, every series on it that the expression matches is in scope — including series
> whose key names carry a different hash tag.
>
> Supplying a tag is an explicit request to evaluate over part of the cluster. Series on
> unselected shards are absent from the expression, so aggregations and binary operators
> are computed from the selected shards only: the first example above is the sum over two
> shards, not the cluster-wide sum. An unknown tag still names a valid slot; its shard is
> queried and may contribute nothing.

On a standalone server the option is accepted and validated but does not restrict the
query, so a query's answer never depends on whether a local key happens to contain
braces. Expressions with no selectors, such as `1 + 2`, perform no fan-out and are
unaffected either way.

## Language and function coverage

The evaluator supports scalar, instant-vector, and range-vector expressions, including:

- Arithmetic: `+`, `-`, `*`, `/`, `%`, and `^`; comparisons: `==`, `!=`, `<`, `<=`, `>`,
  and `>=`, including boolean comparisons.
- Vector matching and label modifiers such as `on`, `ignoring`, `group_left`, and
  `group_right` where applicable.
- Set operators: `and`, `or`, and `unless`.
- Aggregations: `sum`, `avg`, `min`, `max`, `count`, `group`, `stddev`, `stdvar`,
  `topk`, `bottomk`, `count_values`, `quantile`, `limitk`, and `limit_ratio`, with
  grouping modifiers such as `by` and `without`.
- Time modifiers: range selectors, subqueries, `offset`, `@`, `start()`, `end()`, and
  `step()`.
- Rollups and range functions: `rate`, `irate`, `increase`, `delta`, `idelta`, `deriv`,
  `changes`, `resets`, `predict_linear`, `avg_over_time`, `count_over_time`,
  `first_over_time`, `last_over_time`, `max_over_time`, `min_over_time`,
  `present_over_time`, `quantile_over_time`, `stddev_over_time`, `stdvar_over_time`,
  `sum_over_time`, and timestamp-of-extrema functions.
- Math and date functions, including trigonometric, logarithmic, rounding, clamping,
  calendar, `time()`, and `timestamp()` functions.
- Label and ordering helpers: `label_join`, `label_replace`, `sort`, `sort_desc`,
  `sort_by_label`, and `sort_by_label_desc`.
- Special and histogram functions: `absent`, `scalar`, `vector`, `histogram_quantile`,
  and `histogram_fraction`.

The supported function registry is maintained in
[`src/promql/functions/function_list.rs`](../../src/promql/functions/function_list.rs).
PromQL behavior should be treated as the module’s implemented subset rather than a
guarantee that every function or sample type supported by an external Prometheus release
is available.

## Support, conformance, and deviations from Prometheus

Valkey TimeSeries follows the PromQL expression model and aims to match Prometheus
semantics for the float-sample query surface. The reference documentation for that
surface is the current Prometheus documentation for [querying
basics](https://prometheus.io/docs/prometheus/latest/querying/basics/),
[operators](https://prometheus.io/docs/prometheus/latest/querying/operators/), and
[functions](https://prometheus.io/docs/prometheus/latest/querying/functions/).
Compatibility applies to expressions evaluated by `TS.QUERY` and `TS.QUERYRANGE`; it does
not imply that Valkey TimeSeries implements the Prometheus server HTTP API, rule engine,
scraping, remote read/write, or every function in the current Prometheus release.

### Supported PromQL surface

The implemented language includes Prometheus-style metric and label selectors, the four
label matcher operators, scalar/instant-vector/range-vector expressions, arithmetic and
comparison operators, vector matching, set operators, aggregations, range selectors,
subqueries, `offset`, `@`, `start()`, `end()`, and `step()`. The function list above is
the authoritative list for this build; it includes math, date, label, sorting, rollup,
rate, and classic-histogram helpers.

`TS.QUERY` may return scalar, vector, matrix, or string results according to the
expression. `TS.QUERYRANGE` evaluates a scalar or instant-vector expression at each step,
as in Prometheus range queries. Lookback behavior defaults to five minutes and can be
overridden with the command option or module configuration.

### Conformance and test status

PromQL behavior is checked with Prometheus-style conformance fixtures under
[`src/promql/promqltest/`](../../src/promql/promqltest/), covering selectors, literals,
operators, vector matching, aggregations, functions, time modifiers, subqueries, and
query limits. These tests are adapted to the module’s float-sample data model and result
protocol; they are not a claim of passing the complete upstream Prometheus test corpus.
Run them with:

```bash
cargo test --features enable-system-alloc -- promql_tests
```

When upgrading Prometheus or adding a function, compare the relevant upstream reference
page with the registry and add focused conformance fixtures. A query that is not in the
registry is unsupported and returns an error; it should not be assumed to become
available merely because it appears in the Prometheus documentation.

### Known deviations and limitations

The following differences are intentional or are limitations of the current implementation:

- **Float samples are the storage boundary.** Valkey TimeSeries stores numeric float
  samples. Prometheus native histogram samples, histogram bucket-layout reconciliation,
  and native-histogram-specific operators and functions are not supported as a native
  sample type. `histogram_quantile` and `histogram_fraction` operate on classic histogram
  data represented by float series and labels such as `le`; they do not add native
  histogram storage.
- **The function set can lag the current Prometheus release.** Functions absent from
  [`function_list.rs`](../../src/promql/functions/function_list.rs) are unsupported.
  Prometheus may add functions or change experimental-function status independently of a
  Valkey TimeSeries release. The module also exposes a small number of functions whose
  availability is controlled by its own `ts-promql-enable-experimental-functions` setting.
- **Warnings and info annotations are not part of the public result contract.** Prometheus
  can attach annotations when it ignores samples, repairs histogram input, or encounters
  other special cases. Valkey TimeSeries may return the applicable value or omit an
  element without exposing the corresponding Prometheus annotation to the caller.
- **Range-query root types follow Prometheus.** Both Prometheus and Valkey TimeSeries
  accept scalar and instant-vector roots for range queries and reject range-vector roots.
  `TS.QUERYRANGE` also rejects string roots; strings remain available as instant-query
  results where the expression produces one.
- **An unmatched duplicate on the "one" side is not an error.** In one-to-one vector
  matching, Prometheus rejects a match key that repeats on the right-hand side even when
  nothing on the left matches it. Valkey TimeSeries reports a repeated key only when it
  is matched, because filter push-down usually never reads the unmatched series; the
  stricter rule would make the same query fail or succeed depending on the data and the
  push-down settings. A comparison over an ambiguous *matched* key errors as in
  Prometheus, even where the comparison would have filtered the pair out.
- **Operational behavior is different.** Query limits, timeouts, cluster fan-out,
  ordering, and the `HASHTAG` scope are Valkey TimeSeries behavior. They can affect which
  series are read or returned without changing the PromQL grammar.

For applications that require exact Prometheus behavior, especially native histograms,
annotation handling, or newly introduced functions, validate queries against the
specific Valkey TimeSeries release and use the module’s conformance tests as the support
contract.

## Execution model

Queries read the module’s label index to discover matching series, then evaluate the
expression against the underlying chunks. Range queries evaluate their step grid in
parallel and enforce response-size limits while results are materialized. The evaluator
also performs optional constant folding and filter push-down when query optimization is
enabled.

In cluster mode, matching data is read on the shards that own the series. Selector,
rollup, and supported aggregation work is sent through the module’s PromQL fan-out
protocol, and the coordinator merges the shard responses. In a range query every vector
selector is read over the whole step grid in one request, and the shard ships one point
per series per step rather than the raw span; decomposable aggregations can be partially
reduced on shards; operations that require the complete value set, such as `quantile`,
remain coordinator-side. Cluster fan-out settings are controlled separately by
`ts-fanout-aggregation-pushdown` and `ts-fanout-rollup-pushdown`.

By default the coordinator contacts every shard, preferring replicas when the client is
allowed to read from them. A `HASHTAG` clause narrows that shard set without changing the
replica/primary policy; see [Restricting a query to part of a
cluster](#restricting-a-query-to-part-of-a-cluster).

## Query optimization

The PromQL optimizer can rewrite an expression before execution to reduce evaluation
work and the amount of data read from the index. It is disabled by default and can be
enabled with:

```text
CONFIG SET ts-promql-optimize-queries yes
```

When enabled, the optimizer applies semantics-preserving rewrites such as:

- Folding constant expressions and simplifying repeated or redundant boolean/set
  expressions.
- Rewriting identical vector expressions, for example `metric + metric` to
  `metric * 2`, so the selector only needs to be evaluated once.
- Propagating common label filters through binary expressions, aggregations, supported
  functions, and subqueries so selectors can reject non-matching series earlier.
- Converting an equality matcher on `__name__` into the selector’s metric-name field,
  allowing the index to use the metric name directly.

The optimizer runs before selector execution and makes several passes until the
expression stops changing. It does not change the query’s result type or the meaning of
`HASHTAG`; `HASHTAG` still limits the shards searched, while optimizer filters limit the
series selected within those shards.

Optimization is a performance choice, not a requirement for PromQL support. It can
improve broad or repeated-selector queries, but optimized expressions can use more
temporary memory and can be slower for small or already selective queries. If a query
shows unexpected performance or behavior, compare it with
`ts-promql-optimize-queries` disabled. Cluster aggregation and grid push-down are
separate controls: `ts-fanout-aggregation-pushdown` and
`ts-fanout-rollup-pushdown` govern where supported work is performed after the query is
planned.

### Filter push-down across binary operations

Independently of the optimizer, a binary operation such as
`kube_pod_created{namespace="prod"} * on(uid) group_left kube_pod_info` narrows the
wider operand by the label values the other operand's series actually carry, so the
`kube_pod_info` read covers the `prod` namespace rather than every pod. Two mechanisms
do this, and they differ in where the label values come from:

- **Instant queries** evaluate one operand first and derive the filters from its result
  before reading the other. This is always on.
- **Range queries** read every selector over the whole step grid before anything is
  evaluated, so the filters are derived before planning from the series index instead:
  for each selector under such an operation, the engine asks which labels every one of
  its series carries (in a cluster, one `label-profile` fan-out per distinct selector,
  no samples read) and adds them to the other operand's selectors only where they
  provably exclude series. Only the series with a sample in the span the selector reads
  — after `offset`, `@`, lookback or range, and any enclosing subquery — count, so a
  series that stopped reporting before the query (a rolled-out pod, a retired region)
  does not widen the filters. Operands whose series carry the same values — `cpu - cpu
  offset 5m` — are left untouched. `or`, fill modifiers and label-less aggregations
  are never narrowed. An operand whose selector matches no series at all makes the
  operation's result empty (for every operator but `or`, and for `unless` only on the
  left), so the other operand is not read either. This is governed by
  `ts-promql-derived-filter-pushdown`
  (default `yes`); the static optimizer's `ts-promql-optimize-queries` pushes only the
  matchers written in the query, and blindly. With both on, a range query gets the
  optimizer's other rewrites but leaves the push-down to the derived pass, which pushes
  the written matchers too, but only where they exclude series.

Both are exact: a filter only ever removes series that could not have matched at any
step. A selector matching more than 50 000 series (or the query's
`ts-promql-max-response-series`, whichever is lower) is left as written, as is one
matching more than four times that many series in total, counting those that stopped
reporting before the query, and a label with more than 60 distinct values.

## Configuration and safeguards

PromQL settings are regular module configuration values and can be inspected with
`CONFIG GET` or changed with `CONFIG SET`. A change applies to queries that start after it;
a query already running keeps the values it started with.
`ts-promql-max-concurrent-queries` is the exception: it sizes the worker pool and is fixed
at startup.

| Setting | Default | Purpose |
| --- | ---: | --- |
| `ts-promql-max-concurrent-queries` | 8 | Evaluations running at once; further queries wait for a free worker. Immutable after startup. |
| `ts-promql-max-queued-queries` | 128 | Queries that may wait for a worker; further ones are refused on arrival with an error. `0` means unbounded. |
| `ts-promql-query-cache-max-entries` | 1024 | Parsed queries kept for reuse by later queries with the same text. `0` disables the cache. |
| `ts-promql-query-cache-max-bytes` | 67108864 (64 MiB) | Estimated memory the parsed-query cache may hold. One query may take at most a sixteenth of it. `0` disables the cache. |
| `ts-promql-max-query-len` | 4096 bytes | Maximum query string length. |
| `ts-promql-max-response-series` | 1000 | Maximum returned series, checked on the query's result as well as on each read; `0` means unlimited. A read stops as soon as it passes the limit, without opening the rest of the matched series. |
| `ts-promql-max-points-per-timeseries` | 0 | Maximum points a range query generates per series, at most 1000000; `0` leaves only that built-in ceiling. |
| `ts-promql-max-samples-per-query` | 50000000 | Maximum samples one query may load across all its reads (Prometheus' `--query.max-samples`); `0` means unlimited. The count is a running total: samples stay counted after the query drops them, so it bounds the query's work, not its memory at any one moment. A range query's per-step grid counts too: a series costs the larger of its raw samples and the steps it fills, so one sample under a long lookback costs a point per step. In a cluster each shard applies its own value to what it reads. |
| `ts-promql-max-samples-in-flight` | 200000000 | Maximum of the running totals counted under `ts-promql-max-samples-per-query` by all running queries, and by the shard reads this node serves for other nodes' queries, added together; each is released when its query or read ends. A query that would pass it is refused. Like those totals, it counts samples already dropped, so it is a proxy for memory rather than a measure of it. `0` means unlimited. |
| `ts-promql-max-memory-percent` | 90 | Refuse new queries, and stop running ones at their next check, while used memory is at or past this percentage of `maxmemory`. `0` disables it; it has no effect without `maxmemory`. |
| `ts-promql-max-reply-bytes` | 268435456 (256 MiB) | Maximum estimated size of a query's reply, checked before any of it is written. `0` means unlimited. |
| `ts-promql-max-subquery-steps` | 20000000 | Maximum subquery steps one query may evaluate in all: each subquery's steps, multiplied by every subquery around it and by the query's own steps, summed. Checked before anything runs. `0` means unlimited. |
| `ts-promql-lookback-delta` | 5m | Default sample lookback interval. |
| `ts-promql-max-lookback` | 1h | Longest `LOOKBACK_DELTA` a query may request; longer ones are refused. The default lookback and, for a range query, its `STEP` are always allowed. `0` means unlimited. |
| `ts-promql-max-query-duration` | 30s | Maximum wall-clock query duration. |
| `ts-promql-set-lookback-to-step` | no | Use the range query step as the lookback interval; `LOOKBACK_DELTA` is then ignored. |
| `ts-promql-optimize-queries` | no | Enable query rewrites and selector push-down. |
| `ts-promql-derived-filter-pushdown` | yes | Narrow a range query's binary-operation operands by the label values the index holds for the other operand. |
| `ts-promql-enable-experimental-functions` | no | Allow functions marked experimental by the parser (as Prometheus' `--enable-feature=promql-experimental-functions`). |

The per-query `LOOKBACK_DELTA` and `TIMEOUT` options override their corresponding
defaults. Limits are particularly important for broad selectors and long range queries,
which can otherwise materialize many series and points.

Whatever the configuration, no step grid may have more than 1,000,000 steps: a range
query's own grid, a subquery's grid (`m[30d:1m]` is 43,200 steps), and a grid another
node asks this one to evaluate. A query that needs more is rejected with an error before
anything is read. Prometheus caps only the outer grid, at 11,000 points.

Nesting multiplies those grids: every step of an outer subquery evaluates the whole inner
one. `ts-promql-max-subquery-steps` bounds the product, summed over every subquery and
multiplied by the query's own steps, before anything runs. Its default of 20 million admits a
1,000-point range query over a 7-day subquery at a 1-minute step (about 10 million). It
counts steps, not what each costs, and a subquery over an expression that reads nothing is
invisible to the sample limits, so the deadline remains the bound on the rest: it is checked
at every step and subquery step, and inside aggregations and range functions every few
thousand series.

The query text is parsed by the query worker, not by the command itself: the command checks
only that the query is present and within `ts-promql-max-query-len`, so a slow-to-parse query
never holds up other clients. Syntax, regex and nesting-depth errors are therefore reported
after admission. A query refused because the queue is full, or one sent inside MULTI or a
script, gets that error even if its text is also malformed.

A query that is admitted to the queue keeps its `TIMEOUT` budget while it waits: one that
expires before a worker takes it is answered with a timeout error without being parsed or
evaluated.
Refusing on arrival once the backlog reaches `ts-promql-max-queued-queries` reports an
overrun immediately instead of after every waiting query has timed out.

A query whose client disconnects is abandoned: skipped if it is still queued, and stopped at
its next deadline check if it is running, so its worker is free again within a step or two.
On a single node that includes a read in progress, which stops at its next lock hold. In a
cluster, a fan-out already sent to the shards runs out its time budget, which is never
longer than what was left of the query's `TIMEOUT` when it was sent. Each shard stops its
share at that budget too: a request that waited it out in the shard's queue is dropped
unrun, and a read past it stops before its next batch.

`INFO ts_promql` shows the pool's state as `ts_queries_running`, `ts_queries_queued`, the
cumulative `ts_queries_rejected`, and `ts_queries_cancelled`, the queries abandoned because
their client disconnected. `ts_samples_in_flight` is what running queries, and the shard reads
this node serves for other nodes, have charged against `ts-promql-max-samples-in-flight`: their
running totals, which include samples already dropped, not the samples resident now.

A worker keeps each query it parses, keyed by the exact query text, from the second time that
text is seen, and reuses it for later queries with the same text from either command, skipping the regex check, the parse and the
regex compiles the parser does. Only the parsed expression is kept, never data or results:
every query still reads current data with its own time range, database, `HASHTAG` scope and
permissions, and still goes through the optional rewrites, so nothing a write, a deletion or a
configuration change does needs to clear it. Queries that fail to parse are not kept, and a
text seen only once is remembered only as a hash, so one-off queries cost no more than without
the cache and cannot push out the queries that repeat. The cache
is bounded by `ts-promql-query-cache-max-entries` and `ts-promql-query-cache-max-bytes`,
evicting the least recently used queries; lowering either trims it at once, and `0` empties
and disables it. Its size is an estimate of the memory the parsed queries hold, compiled regex
programs included, not an exact figure. `INFO ts_promql` reports it as `ts_query_cache_entries`
and `ts_query_cache_bytes`, with the cumulative `ts_query_cache_hits`, `ts_query_cache_misses`
and `ts_query_cache_evictions`. Text that differs only in spacing is a different entry.

The per-query limits bound each query alone. Three limits bound what all of them take
together, and each refuses rather than waits, since a query that waited mid-evaluation would
hold what it had already loaded: the samples in flight, memory use against `maxmemory`, and
the reply's size. A refusal for load names the limit and starts with
`PromQL query refused while the node is overloaded`; retrying later can succeed.

## Testing and compatibility

PromQL unit and conformance-style tests live under
[`src/promql/promqltest/`](../../src/promql/promqltest/). The test fixtures cover selectors,
operators, vector matching, aggregations, functions, time modifiers, subqueries, and
limits. Run the PromQL tests with:

```bash
cargo test --features enable-system-alloc -- promql_tests
```

For command-level usage, see [`TS.QUERY`](../commands/ts.query.md) and
[`TS.QUERYRANGE`](../commands/ts.queryrange.md). For label selector syntax shared with
other time-series commands, see [filter syntax](filter-syntax.md).
