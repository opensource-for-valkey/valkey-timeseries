# PromQL Rollup Push-Down Plan

**Status:** Plan only — not yet implemented.
**Goal:** Push range-vector functions (`rate`, `sum_over_time`, …) to the shards
that hold the data, so that a clustered `rate(m[5m])` ships one float per series
per step instead of the raw window, and a range query costs one fan-out instead
of one per step.

**Builds on:** the `AggregateExpr` push-down landed in `fb4889d2`
(`src/promql/engine/fanout/aggregation_fanout_command.rs`,
`src/promql/exec/partial_aggregation.rs`), whose protocol conventions —
self-describing responses, the `applied` handshake, coordinator-decides — this
work reuses verbatim.

---

## 1. Finding that reframes the problem

**A PromQL series never spans shards, so a rollup needs no merge algebra at all.**

Three independent confirmations:

- `src/series/index/querier.rs:46` — `series_by_selectors` returns one
  `(SeriesGuard, ValkeyString)` per label set. One series = one key = one slot =
  one primary.
- `src/promql/engine/fanout/range_vector_selector_fanout_command.rs:119` — the
  range fan-out *errors* if two shards return the same label set. The invariant
  is already enforced at runtime, not merely assumed.
- "Buckets" in `MockMultiBucketQueryReaderBuilder` are time-partitioned fixture
  data, not a per-series key split. The stale bucket comments on `QueryPlan` in
  `src/promql/exec/pipeline.rs` describe a shape the current code does not have.

This differs sharply from the cross-series aggregation push-down:

| | `sum by (job)(m)` (shipped) | `sum_over_time(m[5m])` (this work) |
|---|---|---|
| Group spans shards? | Yes | **No** |
| Shard produces | mergeable partial state | **final value** |
| Coordinator does | merge + finalize | **concatenate** |
| Float-parity bar | `assert_close` (partition slack) | **bit-exact** |
| Needs `AggregationPartial`? | Yes | **No** |

### 1.1 Recommendation: gate on window-locality, not decomposability

The eligibility gate is not whether a function is a decomposable aggregation —
it is whether its answer depends only on one series' own samples. Every range
function except `absent_over_time` satisfies that, **including** `rate`,
`quantile_over_time`, `mad_over_time`, and `double_exponential_smoothing`, none
of which are decomposable.

Adopting this framing widens the eligible set from ~8 functions to 24 of 28
while *removing* the hardest component of the previous commit (the partial-state
merge algebra). Decomposability still earns its keep, but only in Phase 2 and
only as a memory optimization — see §5.

This is the one open design decision in the plan; §4 and §5 are written for the
window-locality framing.

## 2. What it is worth

The dominant cost is not window bytes — it is that **range queries re-fan-out
per step**. `src/promql/engine/promql_engine.rs:190` evaluates the whole
expression once per step, and matrix selectors are deliberately excluded from
preloading (`src/promql/exec/utils.rs:61`), so each step drives a fresh
`RangeVectorSelectorFanoutCommand`.

`rate(m[5m])` over 6h at 15s step with a 15s scrape interval:

| | round trips | samples/series on the wire |
|---|---|---|
| Today | 1,441 | ~28,800 (20× overlap redundancy) |
| Phase 1 | **1** | **1,441** |
| Phase 2, `sum by (job)`, 10 jobs | 1 | 10 partials/step, independent of series count |

## 3. Phase 1 — `RollupFanoutCommand`

Push `f(selector[range])`, evaluated over the whole step grid, in one request.

### 3.1 Contract

Request carries the selector plus the fully resolved grid: `query_start`,
`query_end`, `step_ms`, `range_ms`, `lookback_delta_ms`, `range_end_ms`,
`rollup_kind`, `optional_param`. Response carries, per series, the labels and
the surviving `(step_ts, value)` pairs, plus the `applied: bool` handshake
copied from `aggregation_fanout_command.rs:249`.

### 3.2 Files

| File | Change |
|---|---|
| `src/promql/engine/fanout/rollup_fanout_command.rs` | New. Mirrors `aggregation_fanout_command.rs`. |
| `src/promql/engine/fanout/mod.rs:28` | Register the new command. |
| `src/promql/types.proto` | `RollupQuery`, `RollupQueryResponse`, `RollupKind` enum, `RollupSeries`. |
| `src/promql/engine/query_reader.rs:60` | `query_rollup` + `RollupRequest` / `RollupOutcome`, defaulting to `Unsupported` as `query_aggregation` does. |
| `src/promql/engine/selector_batch_executor.rs:53` | `SelectorTaskKind::Rollup`, `execute_cluster_rollup`. |
| `src/promql/exec/evaluator.rs:521` | `evaluate_pushed_down_call` hook in `evaluate_call`, mirroring `evaluate_pushed_down_aggregate`. |
| `src/promql/functions/rollups.rs:47` | Extract the grid/window computation out of `exec_series_rollup` (see §3.3). |
| `src/promql/engine/{querier,memory_series_querier}.rs` | Implement / delegate the new reader method. |

### 3.3 The rule that governs the whole design

**The shard must not re-derive the grid.** Extract the grid and window
computation out of `exec_series_rollup` into a function both sides call, and
ship resolved timestamps only. All `@` / `offset` resolution stays on the
coordinator (`src/promql/exec/evaluator.rs:280`); the modifier itself never
crosses the wire.

## 4. Correctness invariants

The ones that will actually bite, in rough order of how quietly they fail:

1. **The fetch window is wider than the eval window.** `RollupWindow` carries
   `prev_value`, `real_prev_value`, and `real_next_value` — samples *outside*
   `[t_start, t_end]` (`src/promql/functions/rollups.rs:100`). `handle_range_query`
   fetches exactly `[start, end]`. The rollup handler must read
   `[query_start - range - lookback, query_end]`. Getting this wrong produces no
   error, just different `rate` / `deriv` / `increase` numbers at window edges.
2. **Absence is not NaN.** `exec_series_rollup` returns `None` for an empty
   window, dropping the series at that step. Encode that as a missing
   `(series, step)` pair; a NaN placeholder is indistinguishable from a genuine
   NaN value.
3. **`max_points_per_series` silently stops applying.** Today it is checked
   against raw window size at the coordinator
   (`src/promql/engine/selector_batch_executor.rs:487`). Once the shard rolls
   up, only the shard can see the raw count — it must enforce the limit and
   return the error itself.
4. **Keep the experimental-function gate on the coordinator**
   (`src/promql/exec/evaluator.rs:534`). `mad_over_time`, `first_over_time`, and
   the four `ts_of_*` functions are experimental; a shard must never evaluate
   one the coordinator would have rejected.
5. **Gate on shape, not name.** Push down only an `Expr::MatrixSelector` whose
   `.vs` is a plain `VectorSelector`. Subqueries such as `f(<expr>[5m:1m])` have
   their own step grid and must stay local — the same discipline
   `evaluate_pushed_down_aggregate` applies with `Expr::VectorSelector`.
6. **Version skew.** Mirror the existing handshake exactly: an unrecognized
   `RollupKind` returns raw `RangeSample`s with `applied = false`;
   `ErrorKind::{InvalidMessage, UnknownMessageType, UnsupportedFeatures}` latches
   `unsupported_peer` and the coordinator retries without push-down. Reuse
   `decode_kind`'s discipline that a proto3 unknown enum value must not be taken
   for the zero variant.

## 5. Eligibility taxonomy

Groups A–C are all Phase-1 eligible. Decomposability separates A from B and C
but gates nothing in Phase 1.

- **A — decomposable reductions** (also fusable in Phase 2):
  `sum_over_time`, `count_over_time`, `min_over_time`, `max_over_time`,
  `avg_over_time`, `stddev_over_time`, `stdvar_over_time`, `present_over_time`
- **B — position-dependent, single-pass:**
  `first_over_time`, `last_over_time`, `ts_of_first_over_time`,
  `ts_of_last_over_time`, `ts_of_min_over_time`, `ts_of_max_over_time`
- **C — window-holistic:**
  `quantile_over_time`, `mad_over_time`, `rate`, `irate`, `increase`, `delta`,
  `idelta`, `deriv`, `predict_linear`, `double_exponential_smoothing`
  (`holt_winters`), `resets`, `changes`
- **D — excluded:** `absent_over_time` only. Its answer depends on whether *any*
  shard holds the series, so a shard answering `1` is simply wrong.
  Coordinator-only, permanently.

## 6. Phase 2 — fusing with an outer aggregation

For `sum by (job)(rate(m[5m]))`, the shard computes the rollup per series, then
folds the results into the existing `PartialGroups`
(`src/promql/exec/partial_aggregation.rs:224`) keyed by `(group, step)`. It
ships `groups × steps` partials instead of `series × steps` floats — this is
where cardinality collapse happens, and it is the larger win on wide metrics.

This works for **every** rollup in groups A, B, and C. Because the shard holds
each series whole, it produces a *final* per-series value, so the decomposability
requirement falls entirely on the outer operator — already solved by
`AggregationKind::pushdown_strategy`. Group A's only advantage is that the shard
can fold straight into a partial without materializing per-series values first:
a memory optimization, not a correctness one.

## 7. Prerequisite to verify before writing any protocol

`src/promql/functions/rollups.rs:70` selects `(ctx.query_start, ctx.query_end, step)`
whenever `step_ms > 0`. In a range query those are the **outer** query bounds,
but `evaluate_matrix_selector` only fetched `[t - range, t]` for the current
step. So it appears to compute a rollup at every outer step over one step's
worth of data, and then `rollup_vec_to_instant_vector`
(`src/promql/functions/rollup_functions.rs:18`) discards all but the last. For
`t < query_end`, that last window may not overlap the fetched data at all.

**This is not confirmed to be a live bug.** The nearest test,
`eval_query_range_vector_samples_use_step_timestamps`
(`src/promql/engine/promql_engine.rs:541`), covers a plain vector selector, not
a rollup. Settle it first: it determines which grid the shard should evaluate
on, and a wire format built on the wrong answer is expensive to change later.

A second item to confirm in the same pass: whether `__name__` is currently
dropped for `sum_over_time`. Both `exec_series_rollup` and `eval_range`
propagate `drop_name` unchanged from a pipeline that sets it `false`
(`src/promql/exec/pipeline.rs:340`), which would diverge from Prometheus. The
shard must apply whatever the coordinator does, consistently.

## 8. Recommended first PR

Phase 1, restricted to group A plus `last_over_time`, instant queries only
(`step_ms == 0`), behind a config flag alongside the existing
aggregation-push-down toggle in `src/config.rs`.

That exercises the entire protocol — request shape, `applied` handshake,
version-skew fallback, limit enforcement — against the narrowest semantics, and
its tests can assert **bit-exact** equality with the single-node path rather
than the tolerance-based `assert_close` the cross-series work required.

Then widen in this order:

1. The step grid (the real payoff — collapses N round trips to 1).
2. Groups B and C.
3. Phase 2 fusion.

**Test shape.** Unit tests per phase boundary, mirroring the round-trip helpers
in `partial_aggregation.rs` (`single_node` vs. `pushed_down`), plus
`tests/test_ts_query_rollup_pushdown_cme.py` following the existing
`ValkeyTimeSeriesClusterTestCase` pattern used by
`tests/test_ts_query_aggregation_pushdown_cme.py`.
