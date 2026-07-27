# PromQL Rollup Push-Down Plan (Revised)

Status: PR 0 (semantics lock), PR 1 (protocol skeleton), PR 2 (range grid) and
PR 3 (full function set) landed. Push-down is behind
`ts-fanout-rollup-pushdown`, default off. Remaining: PR 4 (fusion with outer
aggregation) and the cluster integration suite (§10.3).

Goal: Push range-vector function evaluation to shards so cluster queries avoid
step-by-step fanout and avoid shipping overlapping raw windows.

Builds on aggregation push-down protocol conventions in
`src/promql/engine/fanout/aggregation_fanout_command.rs` and
`src/promql/exec/partial_aggregation.rs`:
- self-describing responses
- `applied` handshake
- coordinator-decides fallback

---

## 1) Core framing

### 1.1 Locality property

A single PromQL series is fully owned by one shard, so per-series rollups do not
need cross-shard merge algebra.

Operational consequence:
- shard computes final per-series rollup values
- coordinator concatenates per-series outputs (plus optional outer aggregation)

### 1.2 Eligibility rule

Phase-1 eligibility is based on window locality, not decomposability:
- eligible if output depends only on one series' own samples in the window
- excluded if output depends on global series presence/absence across shards

This keeps `absent_over_time` coordinator-only and allows broad support for
other rollups.

---

## 2) Why this matters

Today range queries evaluate per step, and matrix selectors are not preloaded.
That drives repeated fanout and repeated transfer of overlapping samples.

For `rate(m[5m])` over 6h with 15s step:
- today: one fanout per step, overlapping windows repeatedly transferred
- target: one fanout for the whole grid, one value per series per step

Primary win is round-trip collapse. Transfer reduction is secondary but large.

---

## 3) Non-negotiable semantic gate before protocol freeze

This section is a hard blocker, not optional discovery.

### 3.1 Gate G1: range-query rollup step semantics

Before adding wire protocol, confirm and lock expected behavior for rollups in
range queries, specifically the interaction between:
- rollup grid selection in `src/promql/functions/rollups.rs`
- matrix selector fetch window in `src/promql/exec/evaluator.rs` and
  `src/promql/exec/pipeline.rs`
- instant-vector flattening in `src/promql/functions/rollup_functions.rs`

Acceptance criteria:
- golden tests that compare current engine vs Prometheus for multi-step range
  rollups on sparse and dense inputs
- no reliance on only "last step" values when multiple steps are requested

### 3.2 Gate G2: label and metric-name parity

Lock expected label behavior for rollups, including whether metric name is
retained or dropped where Prometheus drops it.

Acceptance criteria:
- explicit tests for `sum_over_time`, `rate`, `changes`, `quantile_over_time`
  label sets
- documented rule reused by local and pushed-down paths

### 3.3 Gate G3: missing-window vs NaN semantics

Empty window output must be represented as missing `(series, step)` samples, not
as NaN placeholders.

Acceptance criteria:
- tests that distinguish genuine NaN input values from no-output windows
- identical sparse series-step shape between local and pushed-down execution

---

## 4) Phase 1 protocol: `RollupFanoutCommand`

### 4.1 Request

Coordinator sends:
- selector (normalized)
- resolved step grid: `query_start`, `query_end`, `step_ms`
- window params: `range_ms`, `lookback_delta_ms`
- evaluation anchor: `range_end_ms` (for instant mode)
- `rollup_kind`
- optional scalar parameter
- limits: `max_series`, `max_points_per_series`

Important: shard must not re-derive `@` or `offset`; modifiers are resolved on
the coordinator before request construction.

### 4.2 Response

Shard returns:
- `applied: bool`
- per-series labels
- sparse `(step_ts, value)` pairs for surviving outputs

If not applied, shard returns raw range samples so coordinator can execute local
semantics.

### 4.3 Mixed-version and unknown-kind behavior

Reuse aggregation fallback discipline:
- unknown `rollup_kind` on shard => `applied = false` + raw response
- unsupported operation envelope errors
  (`InvalidMessage` / `UnknownMessageType` / `UnsupportedFeatures`) latch peer
  incompatibility and trigger coordinator fallback

Coordinator merge rule in mixed clusters:
- applied peers contribute rolled-up outputs directly
- non-applied peers are re-evaluated coordinator-side
- final combined result must match all-local semantics

---

## 5) Limits and resource contracts

`max_points_per_series` splits into two checks:
- input limit (raw points examined per series): enforced shard-side
- output limit (rolled-up points returned per series): validated both shard-side
  and coordinator-side

Additional guardrails required before default-on:
- payload size cap per fanout reply
- chunked/streamed decode path for large `series x steps`
- timeout behavior defined for partial progress vs hard failure

---

## 6) Function eligibility taxonomy

### A. Eligible in Phase 1 (after semantic gate)

- `sum_over_time`, `count_over_time`, `min_over_time`, `max_over_time`,
  `avg_over_time`, `stddev_over_time`, `stdvar_over_time`, `present_over_time`
- `first_over_time`, `last_over_time`, `ts_of_first_over_time`,
  `ts_of_last_over_time`, `ts_of_min_over_time`, `ts_of_max_over_time`
- `quantile_over_time`, `mad_over_time`, `rate`, `irate`, `increase`, `delta`,
  `idelta`, `deriv`, `predict_linear`, `double_exponential_smoothing`
  (`holt_winters`), `resets`, `changes`

### B. Permanently coordinator-only

- `absent_over_time`

Reason: result depends on global absence across shards, not per-shard local
state.

### C. Experimental gate

Coordinator keeps authority for experimental function enablement. Shards execute
only when request is coordinator-approved.

---

## 7) Architecture changes

Planned files:

- `src/promql/engine/fanout/rollup_fanout_command.rs` (new)
- `src/promql/engine/fanout/mod.rs` register command
- `src/promql/types.proto` add `RollupQuery`, `RollupQueryResponse`,
  `RollupKind`, `RollupSeries`
- `src/promql/engine/query_reader.rs` add `RollupRequest` + `RollupOutcome` +
  `query_rollup` (default unsupported)
- `src/promql/engine/selector_batch_executor.rs` add rollup task kind and
  cluster execution
- `src/promql/exec/evaluator.rs` add pushed-down call path for eligible
  matrix-selector calls
- `src/promql/functions/rollups.rs` extract shared grid/window helper used by
  local and shard paths
- `src/promql/engine/querier.rs`,
  `src/promql/engine/memory_series_querier.rs` implement/delegate query method

Design constraints:
- push down only when argument shape is `Expr::MatrixSelector` with plain
  `VectorSelector` input
- subqueries (`f(<expr>[5m:1m])`) stay local in Phase 1

---

## 8) Phase 2 fusion with outer aggregation

For queries like `sum by (job) (rate(m[5m]))`:
- shard computes per-series rollup value per step
- shard then accumulates into aggregation partials keyed by `(group, step)`
- coordinator merges/finalizes existing aggregation partial model

Decomposability applies to outer aggregation strategy, not to inner rollup
correctness.

---

## 9) Revised rollout plan

### PR 0 (must land first) — DONE

Semantics lock. No protocol changes. What the three gates turned up:

**G1 — range-query rollup step behavior: was broken, fixed.**
`exec_series_rollup` re-derived the whole query step grid from
`EvalContext::{query_start, query_end, step_ms}` and evaluated a window at every
step, even though the enclosing range query is driven one step at a time and the
matrix selector had loaded exactly one step's window. The flattener then took the
*last* surviving window. Every step but the last read a window the selector never
fetched. It went unnoticed because it only shows up when `range > step`
(`rate(m[5m])` at a 15s step); with `range <= step` exactly one window survives
and the answer is accidentally right.

Fix: the grid belongs to the caller. `rollup_series_over_grid` in
`src/promql/functions/rollups.rs` evaluates the windows it is *given*; the local
path passes one window end (`EvalSamples::range_end_ms`, already `@`/`offset`
resolved) and the shard path will pass the whole grid. Output is stamped with
`EvalContext::evaluation_ts`, not the window end, so a shifted selector still
reports at the step the client asked for — the rule `rate` already followed.

**G2 — label parity: was wrong, fixed.**
No range-vector function dropped `__name__`. The rule now lives once, in
`Evaluator::drops_metric_name`: a matrix-argument call drops the name unless it
is `first_over_time` or `last_over_time`. The drop is *recorded*, not applied,
and materialized at the end by `cleanup_metric_labels` — Prometheus's delayed
name removal, which `label_replace(…, "__name__", …)` depends on.

That exposed a second divergence: `apply_aggregation` eagerly materialized
pending name drops before grouping, so aggregation grouped on a label set
Prometheus still has. Removed; groups now inherit the pending drop. This also
brings local aggregation into agreement with `PartialGroups::accumulate`, which
never did the eager drop — the two had disagreed on group membership, latent only
because push-down is restricted to bare vector selectors.

**G3 — missing vs NaN: was already correct, now tested.**
An empty window emits nothing and never a NaN placeholder, on both the vectorized
and whole-sample paths.

**The oracle itself was broken.** `assert_results` compared only `samples[0]` of
each series, so every multi-step expectation in `testdata/` — and the sparse `_`
shape — was unverified, which is how G1 survived. It now compares the whole
sequence: which steps exist, and each step's value. Fixing it uncovered
`1 / 0 → NaN` (PromQL arithmetic is IEEE 754, so it is `+Inf`), a `_`
step-indexing bug in the expectation parser that slid every value after a gap one
step earlier, and `predict_linear` requiring two *series* rather than two samples
per series.

Landed in: `functions/rollups.rs`, `functions/rollup_functions.rs`,
`exec/evaluator.rs`, `exec/aggregations.rs`, `binops/mod.rs`,
`functions/predict_linear.rs`, `promqltest/{assert,dsl,runner}.rs`, and
`promqltest/testdata/rollup_range_steps.test`.

### PR 1 — DONE

Protocol skeleton behind `ts-fanout-rollup-pushdown` (default off):
- instant queries only (`step_ms == 0`)
- minimal rollup set: `sum_over_time`, `count_over_time`, `last_over_time`
- full handshake/fallback path

Notes on what the implementation settled:

**The wire carries the whole grid already.** `RollupQuery` has
`query_start`/`query_end`/`step_ms` from the start even though PR 1 only ever
sends `step_ms == 0`. PR 2 is then a behavior change, not a protocol break, and
both sides derive their window ends from the same fields — a shard reduces
exactly the windows the coordinator will ask about, which
`test_window_ends_agree_across_the_wire` pins.

**One kernel, three callers.** `RollupKind::eval_windows` dispatches to the same
`fn` pointers the local `*_over_time` implementations use, over the same
`rollup_series_over_grid` from PR 0. Shard-side reduction, coordinator-side
fallback for a non-applying peer, and single-node evaluation are therefore the
same computation, not three that have to be kept in agreement.

**Subquery inner rollups are pushed down.** `f(m[5m])` inside `g(f(m[5m])[10m:1m])`
is a bare matrix selector evaluated at an instant, once per subquery step, so
each step is eligible on its own. Only the *outer* rollup — whose argument is
the subquery — stays local. This falls out of the `step_ms == 0` gate rather
than being special-cased.

**`MemorySeriesQuerier` answers `Raw`**, so the whole PromQL test suite runs
instant `sum_over_time`/`count_over_time`/`last_over_time` through the push-down
path rather than around it.

Landed in: `promql/types.proto`, `engine/fanout/rollup_fanout_command.rs` (new),
`engine/fanout/{mod,query_utils}.rs`, `engine/query_reader.rs`,
`engine/selector_batch_executor.rs`, `engine/{querier,memory_series_querier}.rs`,
`exec/evaluator.rs`, `functions/{mod,rollup_functions}.rs`, `config.rs`.

### PR 2 — DONE

Range grid support:
- one fanout for full step grid
- sparse `(series, step)` transport
- mixed-version parity tests

Notes on what the implementation settled:

**The grid is resolved in a preload, not in the step loop.** A range query is
driven one step at a time, so "one fanout for the whole grid" needs a phase that
runs before the loop — which already exists for vector selectors
(`Evaluator::preload_for_range`). `preload_rollups` joins it: it walks the AST
for pushable calls, issues one `query_rollup` per distinct call covering every
step, and caches a dense `Vec<Option<f64>>` per series indexed by step. The step
loop then reads its slice. `Option` rather than a NaN-able `f64` is what keeps
the sparse shape: `None` is "this window held nothing", NaN is a value.

**Modifiers collapse or shift the grid, and never reach the shard.** The
coordinator resolves `@`/`offset` per step into window ends. `offset` shifts them
uniformly, so they stay an arithmetic progression the request can describe as
start/end/step. `@` pins every step to *one* window end, so the request degenerates
to a single window whose value the coordinator broadcasts across the grid.
`preload_rollup` verifies the progression the shard will derive equals the ends it
resolved, and stays local if it does not — so an unanticipated modifier shape
degrades instead of answering for the wrong windows.

**A grid request carries one parameter, so the parameter must be a literal.**
`quantile_over_time(scalar(q), m[5m])` can have a different phi at every step and
therefore cannot be one request. The grid path accepts only `NumberLiteral`
parameters; the instant path keeps accepting any scalar expression, where the
question does not arise. This matters for PR 3, which adds the first
parameterized rollup.

**Out-of-window context is the constraint on which functions may join.** The local
path hands the kernel exactly one window's samples, because that is all the matrix
selector loaded; the grid path hands it the union of the grid's windows and lets it
slice. `RollupWindow` exposes `prev_value`/`real_prev_value`/`real_next_value`,
which are therefore populated on one path and not the other. A rollup that read
them would answer differently depending on whether push-down was available, so
`RollupKind` may only contain functions that ignore them —
`every_kind_ignores_samples_outside_the_window` enforces it. **PR 3 must check
each candidate against this before adding it**, `rate`/`deriv`/`holt_winters`
especially.

Coverage worth noting: because `MemorySeriesQuerier` answers `Raw`, the golden
`rollup_range_steps.test` and `range_queries.test` now run through the grid path
end to end — breaking the step mapping fails them.

### PR 3 — DONE

Function set broadened from 3 to 24, behind a conformance suite that runs over
every `RollupKind` rather than a hand-written list.

Pushed down: `sum/count/avg/min/max/stddev/stdvar/mad/present/first/last/
quantile_over_time`, `ts_of_{first,last,min,max}_over_time`, `rate`, `increase`,
`delta`, `irate`, `idelta`, `deriv`, `resets`, `changes`.

**Not pushed down, and why** — each is a property of the function, not a gap:

* `absent_over_time` — permanently. Its answer depends on a series being absent
  across the *whole cluster*, which no single shard can observe.
* `predict_linear` — predicts relative to the query's evaluation timestamp,
  which `@`/`offset` divorce from the window end. A shard is told window ends
  only, so it cannot compute the right origin. Pushing it down needs the
  step↔window offset on the wire.
* `double_exponential_smoothing` / `holt_winters` — takes two scalar parameters
  and `RollupQuery` carries one.

Notes on what the implementation settled:

**Three reduction shapes, not one.** The existing kernel covered
`fn(&RollupWindow, Option<f64>) -> f64` and `fn(&[Sample]) -> f64`. Two more were
needed: `WholeWindowOptional` for the functions that legitimately decline
(`irate` over a one-sample window has no answer, which is not NaN), and
`WindowAware` for `rate`/`increase`/`delta`, which extrapolate to the *window's*
edges — so where those edges sit changes the answer even when the samples do
not, and the bounds have to be passed rather than inferred.

**Every function was extracted to a named `fn` first.** `rate`, `irate`,
`idelta`, `deriv`, `resets`, `changes` and `quantile_over_time` computed their
values in closures or inline. Each is now a named function that both the local
path and `RollupKind::implementation` point at, so the two cannot drift by
implementation — only by wiring, which the conformance suite checks.

**The suite is the gate, and it is exhaustive by construction.**
`RollupKind::all()` is written as a match so a new variant fails to compile until
it is listed, and every test iterates it: name round-trip, resolvability,
out-of-window independence, and — through the evaluator — parity between
pushed-down and local evaluation for each kind × 3 window widths × 2 datasets
(dense monotonic, and gappy with a counter reset) × instant and grid × rolled,
raw and unsupported answers, plus `@`/`offset`/`@ start()`/`@ end()`. Mutation
checks confirm it bites: wiring `Delta` to `rate`, `Resets` to `changes`, or
shifting a window end by 1ms each fail it.

**Experimental functions are now gated in the preload path.** `evaluate_call`
checks `func.experimental`, but `preload_rollups` runs before the step loop and
bypassed it — the query would still have errored, after a wasted fan-out. §6.C's
"shards execute only when coordinator-approved" now holds.

### PR 4

Phase-2 fusion with outer aggregation.

---

## 10) Test strategy (required)

### 10.1 Parity classes

- local single-node vs pushed-down cluster equivalence
- mixed-version cluster (some peers apply, some fallback)
- sparse windows and staleness boundaries
- counter reset edge cases (`rate`, `increase`, `irate`)
- NaN propagation and absent windows
- `@` and `offset` modifier correctness
- experimental toggle enforcement

### 10.2 Exactness policy

For rollup push-down parity:
- exact value equality where finite and deterministic
- exact timestamp equality
- exact output shape equality (which `(series, step)` pairs exist)

Do not use tolerance-only assertions as primary proof for this feature.

### 10.3 Suggested test locations

- unit and round-trip tests near fanout command and evaluator push-down path
- cluster integration in a new
  `tests/test_ts_query_rollup_pushdown_cme.py`
- compatibility and fallback cases mirroring aggregation push-down tests

---

## 11) Operational controls

Add a dedicated config toggle (default off until conformance complete):
- `ts-fanout-rollup-pushdown`

Document:
- runtime behavior when disabled (coordinator-local rollup evaluation)
- interaction with existing aggregation push-down toggle
- expected behavior during rolling upgrades
