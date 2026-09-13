# Stepped selector push-down for PromQL range queries (plan)

**Status:** Implemented 2026-09-13 (work items 1–7; the measurement of item 8 is outstanding).
Proposed and revised the same day for a pre-release codebase — there is no shipped version to
stay compatible with, so nothing below exists for version skew. Of §4, the toggle was kept and
flipped on rather than folded: `ts-fanout-rollup-pushdown` now gates the whole grid push-down
(off = raw span through the range fanout, evaluated on the coordinator). Of §7, `count_values`/
`topk` stay unfused (the selector is pushed stepped and the selection runs on the coordinator),
and the shard refusal text is unchanged.
**Scope:** cluster mode only. Range queries whose vector selectors are *not* under a
rollup — `avg(cpu)`, `cpu / on(host) group_left cpu offset 5m`, `sum by (region)(cpu)` —
and the aggregations directly over them.
**Evidence base:** the 2026-09-13 3-node benchmark (500 series × 3600 samples at 1 s,
240 steps at 15 s; `scratchpad/bench/run_promql_cluster.sh`) and the single-node runs of
2026-09-11/12.

## 1. The problem

`ts-fanout-rollup-pushdown` collapsed `sum by … (rate(cpu[5m]))` from 64 → 18.6 ms and
`max_over_time` from 75 → 34.5 ms, but left `avg(cpu)` at 56 ms and the binop at 108 ms,
against 24 ms and 76 ms on a single node holding *twice* the data. The difference is
what crosses the cluster bus:

| path today | shard sends | coordinator does |
|---|---|---|
| rollup, pushdown on | one point per step per series (or per group) | scatter onto the grid |
| **vector selector in a range query** | **every raw sample in `[start − lookback, end]`** | decode, then `for_each_step_sample` buckets to one sample per step |

For the benchmark shape that is 1.8M raw samples (~29 MB of protobuf) per query per
selector, materialized on the coordinator and then reduced to 500 × 240 = 120k grid
points — a 15× amplification that grows with `cadence / step`. `preload_vector_selector`
([evaluator.rs](../../src/promql/exec/evaluator.rs)) does the bucketing; the wire carries
`RangeQueryResponse` ([promql.proto](../../proto/v1/promql.proto)).

The rollup push-down already solved the identical problem for `rate(...)`: ship the
grid, not the span. This plan generalizes it so that every range-query read of a
selector ships a grid — and, since the wire format is not frozen, folds the rollup
push-down into the same message rather than adding a sibling.

## 2. Design: one grid query

Today's `RollupQuery` already carries everything a stepped read needs — selector, grid
(`query_start`, `query_end`, `step_ms`, `range_end_ms`), `lookback_delta_ms`, limits,
and an optional fused aggregation. The only thing that makes it a *rollup* query is the
mandatory `kind`. Make it optional:

```
GridQuery {
  selector, query_start, query_end, step_ms, range_end_ms, lookback_delta_ms,
  max_series, max_points_per_series,
  optional rollup: { kind, range_ms, scalar_param }      // absent ⇒ stepped instant selection
  optional aggregation: { kind, grouping }              // fuse: fold per (group, step) on the shard
}
GridQueryResponse {
  series:   [ { labels, points: [ { step_ts, sample_ts, value } ] } ]   // stepped/rolled per series
  partials: [ { labels, step_ts, state } ]                             // fused per (group, step)
  raw:      [ RangeSample ]                                            // §2.3 only
}
```

The three response lists are mutually exclusive per series and mean exactly what the
rollup response's `series` / `partials` / `raw` mean now, minus the `applied` and
`aggregated` flags: a response *is* what its non-empty list says it is, and a shard that
sends two lists for the same request is a corrupt peer (the existing defenses stay).

### 2.1 Stepped instant selection (no `rollup`)

The shard resolves the selector, snapshots `[first_end − lookback, last_end]`, decodes, and
runs the *existing* `for_each_step_sample` over the window ends the coordinator sent —
emitting one sparse point per step that has an eligible sample. `sample_ts` travels
because `EvalSample.timestamp_ms` is the sample's own timestamp (`timestamp()` reads it —
[date_functions.rs](../../src/promql/functions/date_functions.rs)), not the step's. On the
coordinator, `preload_vector_selector` builds `PreloadedInstantSeries { labels, values:
StepGrid<Sample> }` straight from the points; `evaluate_vector_selector`, the step loop
and the binop join are untouched — the map they read has the same type and contents.

### 2.2 Fused stepped aggregation (`aggregation` without `rollup`)

`avg(cpu)`, `sum by (region)(cpu)` and every `PushdownStrategy::Reduce` operator over a
bare selector: the shard folds its stepped points into `partials` per (group, step) —
the transport and `SteppedPartialGroups::merge` already exist for fused rollups — and
the response is groups × steps (4 × 240 points for `sum by (region)`). The evaluator side
mirrors the fused-rollup path: candidate collection (an Aggregate directly over a
VectorSelector, `fusable_aggregation`), a preload keyed by (selector, aggregation), and
the lookup in `evaluate_aggregate` before the inner selector is evaluated.

### 2.3 When raw is the right answer

`raw` remains — not for old peers, but because it is sometimes the smaller or the only
answer: a single node has nothing to push to (it answers exactly as its `query_range`
does and the coordinator buckets, which is the branch the promqltest corpus exercises
through the in-memory querier), and for `step < cadence` the stepped form is *larger*
than the span. The shard knows both counts after the decode and, per series, returns
`raw` when `window_ends.len() > samples in span`. There is no "unsupported" outcome: every
node runs the same build.

### 2.4 What this replaces

- `RollupQuery` / `RollupQueryResponse` / `RollupFanoutCommand` become the grid query; the
  `unsupported_peer` tracking and the `RollupOutcome::Unsupported` arm go with them.
- `QueryReader::query_rollup` becomes `query_grid(selector, GridRequest, options) ->
  GridOutcome::{Stepped(series), Rolled(series), Reduced(groups), Raw(range series)}`;
  `preload_rollup` and the new `preload_vector_selector` branch are two callers of one
  reader method.
- `RangeQuery` / `RangeQueryResponse` stay for what genuinely needs a span: matrix
  selectors under non-rollup functions (`predict_linear` …) and subquery grids.

## 3. Semantics that must not move

- **Staleness.** The per-step sample is *the last sample at or before the step and
  after `step − lookback_delta`* — `for_each_step_sample`'s rule, run on the shard with
  the coordinator's `lookback_delta_ms` (which already reflects
  `ts-promql-set-lookback-to-step` / `max_lookback`). A step with no eligible sample is
  absent, never NaN, matching `StepGrid`'s sparse form.
- **Modifiers.** Resolved on the coordinator into window ends exactly as `preload_rollup`
  does today (`resolved_window_ends` plus the `request.window_ends() == resolved` guard);
  `@` collapses every step onto one end and the coordinator replicates by index.
- **Labels.** `proto_labels_to_eval_labels` interns as for rollups; the binop's
  `on(host) group_left` join sees the same `EvalLabels` it sees today.
- **Limits.** `max_series` on returned series; `max_points_per_series` on points per
  series; `ts-promql-max-samples-per-query` charged with the points on the coordinator
  and with the decoded span on the shard.
- **Instant queries** (`step_ms == 0`) keep the instant push-down; subqueries and matrix
  selectors under non-rollup functions stay on the raw path (non-goals).

## 4. Configuration

No new toggle. Shipping the grid is a wire-format decision, not a semantic one: the
value at each step is the same sample, chosen on the other side of the bus. With no
installed base to protect, `ts-fanout-rollup-pushdown` should also default **on** (its own
doc note says it is not a compatibility mechanism), and the two push-down toggles can
collapse into the existing `ts-fanout-aggregation-pushdown` as the one diagnostic escape
hatch — "route everything back through coordinator-side evaluation" — if an escape hatch
is wanted at all. The handshake document keeps its rule for the future; nothing in this
change needs a `required_features` bit or a payload-layer degradation entry, and the
`InvalidMessage → fall back` plumbing can be removed rather than extended.

## 5. Expected effect

With 15 s steps over 1 s data: wire volume ÷ 15, coordinator materialization from 1.8M
samples to 120k points, bucketing moved to the shards (which do it in parallel).
Predicted from the measured components (fetch ≈ 60% of `avg(cpu)`'s 56 ms; the binop is
two selectors): `avg(cpu)` 240 steps ≈ 56 → 25–30 ms stepped and ≈ 15 ms fused; the binop
≈ 108 → 45–55 ms. Coordinator peak memory scales with the grid, not the span, which also
lowers the pressure that produced the 2026-09-12 reboots.

## 6. Work plan

1. **Proto** — replace `RollupQuery`/`RollupQueryResponse` with `GridQuery`/
   `GridQueryResponse` (optional `rollup`, optional `aggregation`, three exclusive lists).
2. **Shard handler** — one `grid-query` handler: decode the span once, then stepped
   selection, rollup, or fused fold; the per-series size rule of §2.3.
3. **Fanout command** — `GridFanoutCommand` from the current rollup command minus
   `unsupported_peer`/`applied`/`aggregated`; `into_result → GridOutcome`.
4. **Reader + executor** — `QueryReader::query_grid`; `ValkeySeriesQuerier` gate is
   `clustered` only; one executor task kind; the local path answers `Raw` via the existing
   snapshot/decode.
5. **Evaluator** — `preload_rollup` re-pointed at the grid reader; `preload_vector_selector`
   consumes `Stepped`; candidate collection, preload map and `evaluate_aggregate` lookup for
   fused stepped aggregations.
6. **Config** — flip `ts-fanout-rollup-pushdown` on (or fold it); update the two rosters.
7. **Tests** — unit: command merge and corrupt-peer defenses over the three lists,
   window-end equivalence against `for_each_step_sample`, `@`/`offset` ends, the size rule;
   counting-reader: one grid request per selector per range query; cluster:
   `test_ts_query_selector_pushdown_cme.py` — results equal to single-node for every shape
   in the benchmark set, limits, sample budget; the rollup push-down suite adapted to the
   grid command.
8. **Measure** — the cluster harness at 500 series under `memwatch.sh`, before/after; then
   the single-node bench to confirm nothing moved there.

Estimate: ≈ 3 days for the whole of it, most of which is the consolidation and its tests;
the stepped branch itself is small once the grid command exists.

## 7. Open questions

- Whether `count_values`/`topk` (`PushdownStrategy::Select`/`CountValues`) join the fused
  path; follow the instant aggregation push-down's table.
- Whether the shard's refusal text should be fixed in the same change (a shard's sample
  budget refusal currently reaches the client as "Internal error in fanout operation");
  it will be this path's most common failure, so probably yes.
