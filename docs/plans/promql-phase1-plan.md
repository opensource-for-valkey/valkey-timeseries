# PromQL refactoring — Phase 1 implementation plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Land the Phase 1 items of the PromQL refactoring assessment — the measurable allocation and lock-hold fixes, the rollup deadline gap, and the remaining dead scaffolding — one bisectable commit per task.

**Architecture:** Changes inside `src/promql/`. Each task ends with `cargo test --lib --features enable-system-alloc -- promql` green and clippy clean. Behaviour changes (A6 deadline, E1 folding) get a test that fails first; behaviour-neutral refactors (A3, B2, B4, D2, D3, D4) get a characterization or shape test where none exists.

**Tech Stack:** Rust 2024, `cargo test --lib --features enable-system-alloc`, `cargo clippy --all-targets -- -D clippy::all`.

**Spec:** [promql-refactoring-assessment.md](promql-refactoring-assessment.md) — items A3, A6 (driver part), B2, B4, D2, D3, D4, E1, and "Suggested implementation order", Phase 1.

## Global Constraints

- Conventional Commits; subject plus at most a 2-line body; no co-author trailer.
- No change to PromQL results except where a task says so.
- Threading rules R1–R6: no new pool entry points; work moved off the GIL must not take it again.
- Disk is tight (~3 GB): delete this crate's `target/debug/deps/{lib,}valkey_timeseries*` between heavy builds.

## Review Focus

- `rate`/`increase`/`delta`/`deriv`/`holt_winters` over many series stop at a passed deadline (A6).
- `drop_name` propagated by the merged driver must not change any output label set: the evaluator sets the drop for every matrix-argument function except `first/last_over_time` (A6).
- Label profiles built off the lock must equal the ones built under it, including the overflow and truncation caps (B2).
- `label_replace` on an interned set must keep the label set sorted and unique, and setting an existing label to its current value must be a no-op (D2).
- Folding inside aggregations must not fold a vector-typed operand (`x * NaN` stays) (E1).

---

### Task 1: A6 — one per-series range driver, with the deadline everywhere

**Files:** `src/promql/functions/range_vector_functions.rs` (driver), `rate.rs`, `deriv.rs`, `holt_winters.rs`, `rollups.rs` (`eval_rollups`, `eval_rollups_basic`).

**Interfaces:**
- Produces: `pub(super) fn eval_series<F>(ctx: &EvalContext, series: Vec<EvalSamples>, f: F) -> EvalResult<Vec<EvalSample>> where F: Fn(&EvalSamples) -> Option<f64> + Send + Sync` — per series: `deadline.checkpoint(len + SERIES_WORK)`, skip empty, `f`, stamp `ctx.evaluation_ts`, keep `labels` and `drop_name`. `eval_range` becomes `eval_series(ctx, series, |s| f(&s.values))`.

- [ ] Test `rate_family_stops_at_a_passed_deadline` (in `functions_tests.rs` or `rate.rs`): ctx whose deadline has passed, enough series × samples to cross `CHECK_EVERY`; `rate`, `deriv`, `double_exponential_smoothing` each return `Err` that converts to `QueryError::Timeout`. Expected RED: `Ok`.
- [ ] Implement `eval_series`; route `calculate_rate`, `DerivFunction`, `eval_double_exponential_smoothing`, `eval_rollups`, `eval_rollups_basic` through it.
- [ ] `-- promql` PASS; commit `fix(promql): check the deadline in every per-series range driver`.

### Task 2: A3 — group key first in shard partial aggregation; keyed finalize

**Files:** `src/promql/exec/partial_aggregation.rs`.

- [ ] `PartialGroups::accumulate`: `compute_grouping_key` per sample, `compute_grouping_labels` only on insert (`entry_keyed` with an owned-labels-on-miss variant). `SteppedPartialGroups::accumulate`: key once per series, labels only when a step's group is new.
- [ ] Private `PartialGroups::into_keyed() -> impl Iterator<Item = (SeriesFingerprint, EvalLabels, AggregationPartial)>`; `SteppedPartialGroups::finalize` keys `by_group` with it instead of re-fingerprinting.
- [ ] Existing `test_pushdown_matches_single_node`, `test_single_shard_is_exact` cover equivalence; add `stepped_finalize_groups_by_label_set` (two series, same group, two steps → one entry with two points). `-- partial_aggregation` and `-- promql` PASS.
- [ ] Commit `perf(promql): key shard partial groups before building their labels`.

### Task 3: D2 — `EvalLabels::set` keeps interned sets interned

**Files:** `src/promql/exec/types.rs` (`insert`/`set`), `src/promql/functions/labels.rs` (callers unchanged).

- [ ] Test `set_keeps_an_interned_set_interned`: on `interned(&[("a","1"),("c","3")])`, `set("b","2")` → still `EvalLabels::Interned`, labels `a,b,c` sorted; `set("a","9")` replaces; `set("a","9")` again is a no-op; equality with the `Owned` result of the same edits. Expected RED: variant is `Owned`.
- [ ] `insert` on `Interned`: binary-search splice with `InternedString::new_pair`, the `copy_label_from` shape; `set` stops allocating the key `String`.
- [ ] `-- labels` and `-- promql` PASS; commit `perf(promql): keep interned label sets interned through label_replace and label_join`.

### Task 4: E1 — fold constants inside aggregations and under unary minus; infallible optimizer

**Files:** `src/promql/optimizer/const_folding.rs`, `optimize.rs`, callers (`engine/promql_engine.rs`, `engine/derived_filters.rs`, tests).

- [ ] Tests: `sum(x * (1 + 2))` folds to `sum(x * 3)`; `-(1 + 2)` folds to the number `-3`; `x * NaN` inside `sum(...)` is unchanged. Expected RED on the first two.
- [ ] `handle_aggregation_expr` folds `expr` as well as `param`; `handle_unary_expr` folds its operand with `fold_constants` and negates a resulting literal. `optimize_expr`/`simplify_expr` return `Expr`.
- [ ] `-- optimizer` and `-- promql` PASS; commit `fix(promql): fold constants inside aggregations and under unary minus`.

### Task 5: D4 — skip the vmrange conversion when no series carries `vmrange`

**Files:** `src/promql/functions/histogram.rs`.

- [ ] Characterization tests: `vmrange_buckets_become_cumulative_le_buckets` (`0...1`=2, `1...2`=3 → `le` 1:2, 2:5, +Inf:5) and `prometheus_buckets_pass_through` (le series unchanged).
- [ ] `classic_histograms` calls `vmrange_buckets_to_le` only when some series has a `vmrange` label; `pop_timeseries` takes the sample out with `Rc::try_unwrap` instead of cloning.
- [ ] `-- histogram` and `-- promql` PASS; commit `perf(promql): convert vmrange buckets only when a series has them`.

### Task 6: D3 — remove unreachable arity stubs

**Files:** `src/promql/functions/types.rs` (`PromQLFunction::apply` gets a default), `functions/*.rs` stubs, `rollup_functions.rs` macros.

- [ ] Default `apply` returns `InternalError("<fn> takes several arguments")`-shaped error via `apply_args`'s arity message; delete every `fn apply` whose body is only an arity error.
- [ ] `-- promql` PASS (no test called the stubs except through arity checks the parser makes unreachable; any such test is deleted with a ledger line).
- [ ] Commit `refactor(promql): drop the unreachable single-argument apply stubs`.

### Task 7: B4 — fold stepped grid output without re-materializing it

**Files:** `src/promql/engine/query_reader.rs` (`GridSink::take`, `GridSeries::into_step_values`), `src/promql/exec/partial_aggregation.rs`.

**Interfaces:**
- Produces: `SteppedPartialGroups::accumulate_series(&mut self, modifier, labels: EvalLabels, points: impl Iterator<Item = (Timestamp, f64)>) -> usize` and `SteppedSelection::take_series(&mut self, labels, points) -> usize`; the existing `Vec<RangeSample>` entry points become loops over them.
- [ ] `GridSink::take` feeds `Stepped` series' points straight from `SteppedPoint`s; `SteppedSeries::into_step_values` deleted if unused.
- [ ] `-- grid` and `-- promql` PASS; commit `perf(promql): fold stepped grid points without copying them first`.

### Task 8: B2 — build label profiles off the module lock

**Files:** `src/promql/engine/selector_batch_executor.rs` (`ReadState::Profile` copy arm).

- [ ] Under the lock: `walk.step(s)` as now, collecting `EvalLabels::interned(&s.labels)` for `Take`; after the hold, `builder.add_series` over them.
- [ ] `-- label_profile`, `-- derived_filters`, `-- promql` PASS; commit `perf(promql): build label profiles after the lock hold, not under it`.

### Task 9: Final verification

- [ ] `cargo fmt --check`; `cargo clippy --all-targets -- -D clippy::all`; `cargo test --lib --features enable-system-alloc`; per-commit clippy over the branch.
- [ ] Whole-branch review.
