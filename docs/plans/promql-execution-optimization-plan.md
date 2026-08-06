# PromQL Execution Optimization Assessment

Status: **Phases 0, 1 and 2 implemented** (2026-08-05); Phases 3+ are planned
only.
Findings from a code review of the `promql` module on branch `promql`,
2026-08-05. Revised 2026-08-05 after review: instrumentation placement
corrected, RollupRequest grid semantics corrected,
query-limit/memory/cancellation guardrails added, CPU-section proposals
narrowed and moved behind profiling.

Phase 0 artifacts:

- `src/promql/engine/counting_query_reader.rs` — `CountingQueryReader`, a
  decorator over `QueryReader` with atomic per-method counters, plus tests
  that **pin current reader-call counts** for every query shape named in §2
  (one preload fetch per deduplicated selector; `query_range == steps` for a
  non-pushable rollup; `query_rollup == inner steps` for a
  subquery-over-expression; one push-down request per instant
  aggregation/rollup). Assertions that Phase 1/2 will change are annotated
  with the phase that changes them.
- `benches/promql_engine.rs` — `phase0_baseline` group with the four
  baseline cases: `non_pushable_rollup` (plus its `pushable_rollup` twin for
  the Phase 1 gap), `subquery_over_expr`, `high_card_sum_by`, `vector_join`.
  Pre-Phase-1 baseline (CI profile, in-memory reader): `non_pushable_rollup`
  3.33 s vs `pushable_rollup` 18 ms (512 steps); `subquery_over_expr` 509 ms;
  `high_card_sum_by` 236 ms; `vector_join` 197 ms.

Phase 1 artifacts (finding 1.1 matrix grid preload + finding 1.3 capped
parallel rollup preload):

- `Evaluator::preload_matrices` / `preload_matrix`
  (`src/promql/exec/evaluator.rs`): a third preload map
  (`MatrixPreloadKey` → `PreloadedMatrixData`) holding the raw span of every
  outer-grid matrix selector not covered by a rollup grid, fetched in one
  `query_range`; `evaluate_matrix_selector` slices the per-step window from it
  via `window_samples`, producing byte-identical output to the per-step fetch.
  Subquery steps (`preload_eligible == false`) never read it. The map is
  included in `has_preloaded_data`.
- §4 decisions as implemented: **limits** — the span read is validated by the
  reader's own `max_series`/`max_points_per_series`; any preload error (limit
  rejections included) leaves the map unpopulated and the step loop falls
  back to the per-step path, so a query that succeeded per-step keeps
  succeeding (test: `range_matrix_preload_over_limit_falls_back_to_per_step`).
  **Memory budget** — bounded by those same reader limits; no new config.
  **Deadline/concurrency** — `check_deadline` runs before each preload
  request; rollup and matrix preloads run in parallel capped at
  `MAX_CONCURRENT_PRELOAD_REQUESTS` (4), and the fallible collect stops
  scheduling on first error.
- Updated call-count pins: non-pushable rollup range query is now
  `query_range == 1` (was `== steps`); fused-rollup coverage and
  mixed pushable/non-pushable cases pinned alongside.

Phase 2 artifacts (finding 1.2 subquery-scoped preloading):

- `PreloadGrid` (`src/promql/exec/evaluator.rs`): splits what `EvalContext`
  conflated — the **step grid** being filled (`start_ms`/`end_ms`/`step_ms`)
  from the bounds `@ start()`/`@ end()` resolve against
  (`at_start_ms`/`at_end_ms`). They coincide for a range query and diverge for
  a subquery, whose `@` modifiers refer to the *enclosing* query. Every
  `preload_*` function now takes a grid instead of a context;
  `preload_for_range` is a thin wrapper over the new `preload_grid`.
- `Evaluator::evaluate_subquery` builds a **sub-evaluator** owning its own
  preload maps, calls `preload_grid` on the subquery's aligned grid, and steps
  it with `preload_eligible = true`. A sub-context rather than a grid identity
  on the evaluator-global maps, per §1.2: entries keyed only by selector would
  answer the wrong grid, and an evaluator that drops with the subquery makes
  that structurally impossible and nests for free. Both
  `collect_vector_selectors` and `collect_rollup_candidates` already stop at
  `Expr::Subquery`, so the walk covers exactly the nodes evaluated at that grid.
- `evaluate_fused_rollup` no longer uses `ctx.step_ms > 0` as a proxy for "a
  grid was preloaded": it consults the map first (the preloaded entry carries
  its own `eval_start_ms`/`step_ms`), and only then falls back to the
  range-step-stays-local rule. Without this a subquery step — which has
  `step_ms == 0` — could not read its own grid.
- **Preload failure is best-effort**, on the Phase 1 rule (§4): a reader-limit
  rejection downgrades the subquery to the per-step path rather than failing a
  query that used to succeed. A deadline propagates, since more work cannot
  help.
- Updated call-count pins: `max_over_time(rate(a[1m])[4m:1m])` is now
  `query_rollup == 1` (was `== inner steps`); new pins cover a subquery over a
  binary expression (one fetch per deduplicated selector, was inner_steps ×
  selectors), nested subqueries, and a range query over a subquery (one fetch
  per *outer* step, was outer × inner).
- Measured (CI profile, in-memory reader): `subquery_over_expr` **509 ms →
  108.7 ms (−78.7%)**. The same run confirms Phase 1 on `non_pushable_rollup`:
  **3.33 s → 48.2 ms (−98.6%)**.

Bug found and fixed while landing Phase 2 (pre-existing, unrelated to the
optimization itself): `evaluate_subquery_vector_selector` — the bare-selector
subquery fast path — derives its whole grid from the subquery's
start/end/step via `QueryPlan::for_subquery_vector_selector` and never reads
the selector's `at`/`offset`, so it **silently ignored time modifiers on a
subquery's inner selector**: `metric offset 30s[2m:30s]`,
`metric @ start()[2m:30s]` and friends all returned the unmodified series.
`evaluate_subquery` now keeps modifier-carrying selectors off that fast path,
sending them down the general per-step path — which resolves modifiers through
`apply_time_modifiers_ms`, and which Phase 2 preloading makes cheap (one span
fetch, not one read per step). Pinned by
`subquery_inner_selector_honours_its_time_modifiers` with absolute values and
by `subquery_preload_matches_the_unpreloaded_path_for_every_modifier_shape`.

Phase 3 artifacts (profile, then only what it justified):

- **How it was profiled.** No sampling profiler was usable on the dev machine
  (`xctrace` needs full Xcode, `dtrace` needs SIP disabled), so attribution
  came from temporary in-process counters wrapped around each Tier 2
  candidate, run in release over the Phase 0 bench shapes. The
  instrumentation was removed before commit; it is reproducible by
  re-applying the same counters. Figures below are **CPU time** summed across
  the parallel step loop, not wall time.
- **2.1 aggregation grouping — confirmed, fixed more simply than proposed.**
  `compute_grouping_labels` cost 826 ms of CPU over 1.1M calls (~750 ns each)
  for `sum by (le)` at 1100 series × 1000 steps: `retain` goes through
  `make_owned`, which clones *every* label of the source set into owned
  `String`s before dropping the ones the modifier excludes — 1100 label sets
  built per step to keep 11. But grouping needs a *key* per sample and
  *labels* only once per group. `EvalLabels::compute_grouping_key` hashes the
  filtered view instead (allocation-free), and the label set is materialized
  only when a group is new. That is simpler than the planned
  operation-scoped memo tables — no memo, no cross-step state, no lock
  traffic, nothing keyed on AST identity. Equivalence holds by construction
  (both paths go through the new `labels::fingerprint_labels`, and filtering
  preserves order) and is pinned by
  `grouping_key_matches_the_materialized_grouping_labels`. Result: 826 ms →
  190 ms of CPU; `group_sample_values` overall 1349 ms → 577 ms.
- **2.1 binary-op match keys — REFUTED; the cost was somewhere else.**
  `compute_binary_match_key` was only 33 ms of CPU (165 ns × 200K) for
  `a_hundred - b_hundred`, so the memo this document proposed would have
  addressed ~4% of that query. What actually dominated was
  `collect_fingerprints` at **840 ms** of CPU: `into_par()` fanning a
  *100-element* map across threads, twice per step, nested inside the
  already-parallel step loop — ~420 µs of fan-out to do ~16 µs of hashing.
  Gating it on `PARALLEL_MATCH_KEY_THRESHOLD` (2048) cut it to 56 ms. This is
  exactly the substitution the phase's "profile first" gate exists to
  prevent, and the §2.1 proposal below is left standing as written so the
  miss stays on the record.
- **Deliberately not landed.** The 2.2 prepare pass, re-keying `SeriesMap` by
  fingerprint, and `ensure_unique_labelsets` — the profile does not justify
  them. `merge_step_into_series_map` measured 3.4 ms (`high_card_sum_by`) and
  23 ms (`vector_join`) of CPU, and the preload-lookup path 57 ms / 17 ms,
  all far below the two paths above. They remain planned, not implemented.
- **Benchmark caveat.** Wall-clock on this dev machine is unreliable for
  these parallel shapes — the *same* binary measured 94 ms and 174 ms on
  consecutive `vector_join` runs, drifting with thermal/frequency state. The
  figures below therefore come from an **interleaved A/B** — revert → measure
  → restore → measure, three rounds — so drift cancels rather than being
  charged to the change. Means over the three rounds:
  `high_card_sum_by` 212.8 ms → 148.0 ms (**−30%**), `vector_join`
  178.1 ms → 94.8 ms (**−47%**). Per-round spread was ±3% on each side, and
  the direction was identical in all three rounds. Single-shot before/after
  runs on this machine should not be trusted; any one round in isolation
  overstates the win (round 1 alone read −35% / −50%).

Goal: identify where PromQL query execution does redundant storage/cluster
round trips or redundant per-step CPU work, and lay out a prioritized,
verifiable plan for eliminating them.

Related: [promql-rollup-pushdown-plan.md](../promql-rollup-pushdown-plan.md)
(rollup push-down, behind `ts-fanout-rollup-pushdown`, default **off** — which
makes finding 1.1 below the *default* behavior for range-vector functions).

---

## 1) Execution model today

Range query flow, as implemented:

1. Parse → `optimize_expr` (`src/promql/optimizer/optimize.rs`): constant
   folding, algebraic rewrites, static filter pushdown.
2. `preload_for_range` (`src/promql/exec/evaluator.rs:70`):
   - Vector selectors are collected, deduplicated by `PreloadKey`, fetched
     **once** for the whole query span, and bucketed into dense per-step arrays
     (`preload_vector_selector`, `evaluator.rs:317`).
   - Pushable rollups are evaluated for the whole step grid in one
     `query_rollup` request (`preload_rollups`, `evaluator.rs:103`); on a
     single node the same call returns `RollupOutcome::Raw` (one fetch of the
     whole span) and the grid is reduced locally.
3. Step loop (`evaluate_range`, `src/promql/engine/promql_engine.rs:218`):
   re-walks the full AST once per step in parallel chunks of 64
   (`STEP_MERGE_CHUNK_SIZE`), merging each step's instant vector into a
   `SeriesMap` keyed by `EvalLabels`.

Selectors and rollups that were preloaded are O(1) per step. Everything that
misses the preload maps falls back to live reads inside the step loop — that
is where nearly all of the cost below comes from.

---

## 2) Findings

### Tier 1 — structural gaps (redundant storage/cluster round trips)

#### 1.1 No local grid fallback when a rollup can't be pushed down

When `query_rollup` returns `Unsupported` (`evaluator.rs:231`), or the call
fails `pushable_rollup` (non-literal scalar param, function not in
`RollupKind`), nothing is cached. `evaluate_pushed_down_rollup` refuses
`step_ms != 0` (`evaluator.rs:1242`), so **every step** re-fetches its whole
window via `evaluate_matrix_selector` → `execute_selector_pipeline` →
`query_range` (`src/promql/exec/pipeline.rs:319`). For `rate(m[5m])` at a 15s
step that is ~20× overlapping re-reads, one per step, each under the module
lock (single node) or a full fanout (cluster).

Hit by:

- `ts-fanout-rollup-pushdown` disabled (`src/promql/engine/querier.rs:80`) —
  the **default**. Because grid preloading piggybacks on `query_rollup`, the
  config being off also kills *single-node* grid preloading, where no fanout
  is involved at all.
- Functions outside `RollupKind`
  (`src/promql/functions/rollup_functions.rs:39`): `predict_linear`,
  `holt_winters`, and anything else not in the enum.
- Rollups with a non-literal scalar parameter, e.g.
  `quantile_over_time(scalar(q), m[5m])`.
- A cluster peer that reports `Unsupported`.

**Proposal:** add a third preload map — a *matrix preload* — beside
`preloaded_instant` and `preloaded_rollups`. For any bare `MatrixSelector` in
the outer grid that wasn't rollup-preloaded, fetch
`[first_window_start + 1, last_window_end]` once via `query_range` (the `+ 1`
follows the pipeline's exclusive-lower-bound convention for half-open windows
`(end - range, end]` — see `rollup_fetch_bounds`,
`src/promql/engine/query_reader.rs:203`, and the same convention in
`pipeline.rs:316`), cache the raw per-series samples, and have
`evaluate_matrix_selector` slice per step. The slicing machinery already
exists: the resumable cursor in `rollup_series_over_grid`
(`src/promql/functions/rollups.rs:90`) and `window_samples`
(`rollups.rs:257`). `collect_vector_selectors` deliberately skips
`MatrixSelector` today (`src/promql/exec/utils.rs:63`); this fills that hole.

Properties of this design:

- Works for **every** range-vector function, not just `RollupKind` members.
- Decouples grid preloading from the fanout config: single-node range queries
  stop degrading to per-step window re-fetches when push-down is off.
- Reuses the same window kernels, so pushed-down, preloaded, and local paths
  cannot disagree about grids.

**Required design decisions before implementation** (see §4 for details):
query-limit semantics over a consolidated span, a preload memory budget with
a safe fallback to the current per-step path, and deadline behavior.

**Impact: O(steps) → O(1) storage reads for the affected shapes. Biggest
single win in this document.**

#### 1.2 Subqueries issue one live request per inner step

Inside `eval_subquery_step` the context has `preload_eligible = false` and
`step_ms = 0` (`evaluator.rs:705`), so:

- An inner rollup issues a full `query_rollup` per inner step
  (`evaluator.rs:1280`).
- An inner vector selector inside a non-trivial expression issues a live
  `reader.query` per inner step (`evaluator.rs:687`).

Only the bare-`VectorSelector` inner expression has a fast path
(`evaluate_subquery_vector_selector`, `evaluator.rs:563`). A range query over
`max_over_time(rate(m[5m])[1h:1m])` performs *outer_steps × 60* rollup
requests — the worst asymptotic behavior in the engine. The subquery step
parallelism (`PARALLEL_SUBQUERY_STEP_THRESHOLD`) hides latency but not the
request count.

**Proposal:** subquery-scoped preloading.

- A rollup directly under a subquery can be answered by **one**
  `RollupRequest` whose step grid is the subquery's own grid. Note the
  request wire format describes a *regular* `start/end/step` progression
  (`RollupRequest::window_ends`, `src/promql/engine/query_reader.rs:107` —
  derived via `step_times`); only the local `reduce_windows` accepts an
  arbitrary window list. Subquery grids are regular (aligned start, fixed
  resolution), so this fits — but the implementation must keep the existing
  window-geometry validation that `preload_rollup` performs
  (`evaluator.rs:221`: derive the expected window ends, compare against
  `request.window_ends()`, and stay local on any mismatch, e.g. an
  unanticipated `@`/`offset` shape).
- For general inner expressions, run the equivalent of `preload_for_range`
  scoped to the subquery's grid before its step loop. Prefer a **separate
  evaluator sub-context** owning its own preload maps over adding a grid
  identity to the evaluator-global maps: grid-keyed global maps have awkward
  lifetime, recursion (nested subqueries), and cache-growth implications,
  while a sub-context scopes naturally and drops with the subquery.

#### 1.3 `preload_rollups` runs serially

Each candidate is a blocking round trip in a plain `for` loop
(`evaluator.rs:109-131`), while selector preloading immediately above is
parallelized with `par()`. `rate(a[5m]) / rate(b[5m])` pays 2× fanout latency;
the rollup phase also doesn't overlap the selector phase.

**Proposal:** collect candidates → dedup keys → parallelize the
`preload_rollup` calls **with a concurrency cap**, and stop scheduling new
requests after the first error or deadline expiry. Unbounded parallelism here
multiplies concurrent cluster fanouts and peak memory (each in-flight rollup
holds its raw span on the single-node `Raw` path). Overlapping the rollup
phase with the selector phase is optional and subject to the same cap.

### Tier 2 — per-step CPU (dominates high-cardinality range queries)

These are plausible but **unproven** hot spots: sequence them behind
profiling (Phase 3), and land each as an independent, benchmark-gated change.

#### 2.1 Label sets are re-hashed and re-cloned every step

The series set is frozen after preload, yet each step redoes identity work per
sample:

- Aggregation grouping: `group_sample_values` →
  `EvalLabels::compute_grouping_labels` clones + `retain`s (an owned
  `Vec<Label>` allocation per sample when a `by`/`without` modifier is
  present) and fingerprints the result, per sample per step
  (`src/promql/exec/aggregations.rs:512`, `src/promql/exec/types.rs:176`).
- Binary-op matching recomputes match keys per sample per step — and these
  are *modifier-specific*: `compute_binary_match_key` hashes different label
  subsets depending on `on`/`ignoring` (`src/promql/binops/labels.rs:33`),
  and `get_metric_signature` varies with `drop_name` (`labels.rs:88`).
- `ensure_unique_labelsets` runs once per step via `cleanup_metric_labels`
  (`evaluator.rs:466`) — another full hash pass over the result vector.
- `merge_step_into_series_map` hashes the full `EvalLabels` per sample per
  step. Note the `SeriesMap` uses the std `Hash` impl with an AHash
  `RandomState` (`src/promql/exec/types.rs:58`), **not** `fingerprint()` —
  so a memoized fingerprint would not help this path unless the map is
  re-keyed by fingerprint (with labels stored alongside).

**Proposal: operation-scoped memo tables, not a fingerprint cache inside
`EvalLabels`.** A single memoized base fingerprint cannot serve these paths
(each needs a different projection of the labels), and `EvalLabels::Owned` is
mutable, so an embedded cache would complicate the `Eq`/`Hash` invariants and
invalidation. Instead, cache per operation, keyed by immutable source-label
identity plus an operation descriptor:

- aggregation node + grouping modifier → `(group_key, group_labels)`;
- binary node + match modifier + `drop_name` → match key;
- final-output identity (series-map key) where applicable — e.g. re-key
  `SeriesMap` by a precomputed `u128` fingerprint with labels stored in the
  value.

Sound because preloaded series identity is stable across the step loop, and
the caches live outside the mutable label values, making each one
independently measurable.

#### 2.2 Per-step key reconstruction and lock traffic

Every step, every selector: `PreloadKey::from_selector` re-hashes all matcher
strings (`evaluator.rs:651`, hashing in `src/promql/hashers.rs:74`). Every
rollup call: `pushable_rollup` + `RollupPreloadKey::new`, including
`AggregationKey` label-vector clones (`evaluator.rs:273`), plus an `RwLock`
read acquisition per node per step. The preload maps are *frozen* once the
step loop starts.

**Proposal:** a one-time **prepare pass** after preload that resolves each
selector / call AST node to its preload slot, and freezes the maps into a
plain `Arc`. Per-step cost becomes a pointer lookup: no hashing, no locks.

Scope constraint: pointer-keyed slot resolution applies **only to the
immutable original AST**. Evaluation can construct rewritten expression trees
at runtime — `eval_binop_with_pushdown` builds a filtered copy of one operand
(`evaluator.rs:924`) — and nodes of such owned trees must simply miss the
side table and take the normal fallback path. (Today that rewrite path is
disabled whenever preloaded data exists, via `has_preloaded_data`,
`evaluator.rs:840` — but the prepare pass must not *depend* on that gating.)

Also folds in:

- The duplicated shape analysis in `evaluate_call`, which runs
  `pushable_rollup` and then `rollup_arguments` again on the fallback path
  (`evaluator.rs:782-788`).
- Per-step re-evaluation of literal aggregation params (`evaluator.rs:1015`).
- Caching the normalized `Matchers` that `normalize_selector` currently
  re-clones on every reader call (`src/promql/engine/querier.rs:98`).

### Tier 3 — memory and allocation

- **Dense preload arrays**: `Vec<Option<Sample>>` is 24 B/step/series
  (`src/promql/exec/types.rs:322`). 10k series × 1440 steps ≈ 345 MB peak.
  Rollup grids use `Vec<Option<f64>>` (16 B/step/series). Options: bitmap +
  packed values, or trim leading/trailing absent runs per series. Note this
  is the *later optimization* of existing structures; the **budget** that
  gates new matrix preloading is a Phase 1 requirement, not a Tier 3 item —
  see §4.
- `filter_samples_binary_search` (`src/promql/exec/utils.rs:27`) is
  currently **dead code** — no callers outside its own definition. Remove it
  (or leave until a caller exists); it is not an observed hot path and does
  not belong in this plan's performance work.

---

## 3) Sequencing

| Phase | Work | Expected payoff |
|---|---|---|
| 0 ✅ | Baseline: extend `benches/promql_engine.rs` with range-query benches for (a) non-pushable rollup, (b) subquery-over-expression, (c) high-cardinality `sum by`, (d) vector-vector join — landed as the `phase0_baseline` group. Add a **counting `QueryReader` wrapper** (test-only decorator with atomic per-method counters around the inner reader) so tests can assert reader-call counts — landed as `src/promql/engine/counting_query_reader.rs`. `query_stats` is *not* the place for this — it tracks completed-query durations keyed by query string (`src/promql/engine/query_stats.rs`), not reader activity. | Makes every later claim falsifiable |
| 1 ✅ | 1.1 **bounded** matrix grid preload — with the query-limit decision, memory budget, and deadline fallback of §4 resolved first — plus 1.3 concurrency-capped parallel rollup preload | O(steps)× fewer reads; preload latency ∝ #rollups → bounded rounds |
| 2 ✅ | 1.2 subquery-scoped preloading (sub-context ownership) — landed as `PreloadGrid` + a per-subquery sub-evaluator | Removes the steps × inner-steps request blow-up; `subquery_over_expr` −78.7% |
| 3 | Profile. Then 2.1 operation-scoped memo tables and 2.2 prepare pass, as **independent, benchmark-gated changes** — land only what the profile and Phase 0 benches justify | CPU-bound range queries at high cardinality |
| 4 | Tier 3 packing of existing preload structures as profiling justifies | Peak-memory headroom |

Phase 1 first: it also fixes the config cliff where the (default-off) fanout
push-down setting silently degrades single-node range queries, and the Phase 0
counters prove the win and pin it against regression.

---

## 4) Correctness and operational guardrails

### Semantics

- Every fallback must reuse the existing kernels
  (`RollupRequest::reduce_and_group`, `rollup_series_over_grid`) so
  pushed-down, preloaded, and local paths can never disagree on windows. The
  step loop currently keeps non-preloaded rollups local *specifically* so two
  paths cannot disagree about the grid (`evaluator.rs:1230` doc comment) —
  the matrix preload must inherit that discipline, including the
  window-geometry validation of `preload_rollup` (`evaluator.rs:221`).
- Preserve the absent-vs-NaN distinction: a window that held no samples is
  *absent*, not NaN (`Option<f64>` grids, sparse transport).
- The runtime binop filter pushdown is gated on `has_preloaded_data`
  (`evaluator.rs:840-843`) because rewritten matchers change `PreloadKey`s.
  Any new preload map must be included in that check.
- Keep the exclusive-lower-bound convention consistent: the pipeline
  increments the fetch start by 1 ms for half-open windows
  (`pipeline.rs:316`, `rollup_fetch_bounds`); the matrix preload's
  `[start + 1, end]` span must follow the same rule everywhere.

### Query-limit semantics (decide before Phase 1)

Consolidating per-step window fetches into one span fetch changes what the
limits measure. Today each 5-minute window is individually subject to
`max_series` and `max_points_per_series`
(`query_range_local`, `src/promql/engine/selector_batch_executor.rs:804`);
one query spanning the whole range is checked against the union of series and
samples over that span. A query that succeeds at every step today could fail
after consolidation. (Precedent: instant-selector preload and the single-node
rollup `Raw` path already fetch and limit-check whole spans — but the matrix
path does not, so this is a real behavior change for it.) Options:

- accept span-level limit semantics for preloaded matrices, documented, with
  tests pinning the behavior; or
- a bounded/segmented preload: fetch the span in segments sized to respect
  existing limits, falling back to the current per-step path when the span
  exceeds them.

Either way the decision must be explicit and tested before Phase 1 lands.

### Resource bounds

- **Preload memory budget** (Phase 1 requirement): raw matrix caching can be
  substantially larger than rollup grids — it holds every sample in the span,
  not one value per step — particularly for long, high-frequency ranges.
  Estimate cost up front (series count × span × expected resolution, or a
  first-segment probe), compare against a configurable threshold, and fall
  back safely to the current per-step path when over budget.
- **Deadline and cancellation**: parallelized preloads (1.3) and matrix
  preloads must check the query deadline between requests, use bounded
  concurrency, and stop scheduling work after the first error or deadline
  expiry — not merely fail at collection time.

### Test matrix

Cover, for both the matrix preload and subquery preload paths:

- `@ start()`, `@ end()`, absolute `@`, positive and negative `offset`, and
  combinations;
- duplicate selector occurrences (dedup by preload key);
- empty windows (absence, not NaN) and windows whose samples sit exactly on
  the range boundary (half-open `(start, end]`);
- nested subqueries;
- limit interactions per the decision above.

Validation per phase: promqltest conformance suite, compat fuzzer in
**strict** mode, `cargo test --features enable-system-alloc`, and Phase 0
reader-call assertions — e.g. "a range query performs exactly one
`query_range` per **deduplicated matrix preload key**, and zero when the call
was already covered by a rollup preload."

## 5) Non-goals

- Changing PromQL semantics or the pushdown protocol itself.
- Replacing the per-step evaluation model with a fully vectorized
  per-series engine — the preload + prepare-pass work above captures most of
  that benefit without a rewrite.
