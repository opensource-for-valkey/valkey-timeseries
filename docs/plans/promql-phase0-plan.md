# PromQL refactoring — Phase 0 implementation plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Land the Phase 0 cleanups from the PromQL refactoring assessment — dead code, duplicated scaffolding, inconsistent error handling, the dead `RollupWindow` context — as separate, bisectable commits with no behaviour change except two documented bug fixes.

**Architecture:** Pure refactoring inside `src/promql/` (plus one proto field reservation and one shared `src/fanout` trait bound). Every task ends with the existing unit suite and the PromQL conformance fixtures green; tasks that could shift behaviour (shard error replies, `ts_of_min/max` NaN seed, smoothing-factor bounds) add a characterization test first.

**Tech Stack:** Rust (edition 2024), `cargo test --lib --features enable-system-alloc`, `cargo clippy --all-targets --features enable-system-alloc -- -D clippy::all`, protox regeneration via `VALKEY_TS_PROTO_REGEN=1 cargo build`.

**Spec:** [promql-refactoring-assessment.md](promql-refactoring-assessment.md) — Parts A6 (partial), B3, B5, C4, C5, C6, D5, D6, E3, E5, E7 and the "Suggested implementation order" section. Items named there but not below are Phase 1+ and out of scope here.

## Global Constraints

- Conventional Commits, terse: `type(scope): summary`, at most a 1–2 line body, no co-author trailer.
- No behaviour change on the shared RTS surface; PromQL-only changes still run the conformance fixtures (`cargo test --lib --features enable-system-alloc -- promql_tests`).
- After a `.proto` edit: `VALKEY_TS_PROTO_REGEN=1 cargo build` and commit `proto/v1/generated/`.
- Never consult RedisTimeSeries source. Prometheus semantics may be recalled from memory (Apache-2.0).
- Threading rules R1–R6 untouched: no new pool entry points.

## Review Focus

- A fan-out request arriving with no selector must now produce an error reply, and the coordinator must surface it as a fan-out error rather than treat it as an empty shard (Task 12).
- `ts_of_min_over_time` / `ts_of_max_over_time` over a window whose first sample is NaN must report the timestamp of the real extremum (Task 9).
- Removing the four `RollupWindow` context fields must not change any `Windowed` kernel's answer; `every_kind_ignores_samples_outside_the_window` and the conformance fixtures are the guard (Task 8).
- The `PromqlResult` → `QueryResult` rename must not change any error's kind or text (Task 4).
- Reserving `InstantQuery.max_points_per_series` must leave an older coordinator's request decodable (prost ignores unknown fields; Task 11).

---

### Task 1: Branch and baseline

**Files:** none modified.

- [ ] **Step 1:** `git checkout -b refactor/promql-phase0` from the `promql` tip (cf9fd20ee).
- [ ] **Step 2:** Run `cargo test --lib --features enable-system-alloc`; record the pass count and wall time in the ledger.
      Expected: all tests pass.
- [ ] **Step 3:** Run `cargo clippy --all-targets --features enable-system-alloc -- -D clippy::all`.
      Expected: clean.

No commit.

### Task 2: E3 — dead variants, impls and conversions

**Files:**
- Modify: `src/promql/exec/types.rs:20-55` (remove `StorageError`, `UnsupportedFunction` + Display arms)
- Modify: `src/promql/hashers.rs:52-60` (`impl HasFingerprint for [ProtoLabel]`), `:340-359` (`impl HasFingerprint for PreloadKey`), visibility of `OffsetKey`, `AtKey`, `update_hasher_for_vector_selector` → private
- Modify: `src/promql/model.rs:161-197` (`impl Default for EvalContext` → `#[cfg(test)]`; delete `From<&EvalStmt> for EvalContext`)

- [ ] **Step 1:** Delete the two variants and their `Display` arms; `cargo check --features enable-system-alloc`.
      Expected: compiles (no constructors exist).
- [ ] **Step 2:** Delete the two `HasFingerprint` impls; narrow the three visibilities; `cargo check`.
      Expected: compiles. If a test or bench names them, it is the only caller — delete that test.
- [ ] **Step 3:** `#[cfg(test)]` on `impl Default for EvalContext`; delete `From<&EvalStmt>`; `cargo check --all-targets --features enable-system-alloc`.
      Expected: compiles.
- [ ] **Step 4:** `cargo test --lib --features enable-system-alloc -- promql`
      Expected: PASS.
- [ ] **Step 5:** Commit `refactor(promql): remove dead error variants, fingerprint impls and conversions`.

### Task 3: E3 — remove unreachable string–string binary operators

**Files:**
- Delete: `src/promql/binops/binop_string_string.rs`
- Modify: `src/promql/binops/mod.rs:12` (mod decl), `:57-59` (String/String arm)
- Modify: `src/promql/optimizer/const_folding.rs:88-90` (StringLiteral arm), `:105-150` (`handle_string_string`, `string_compare`)

- [ ] **Step 1:** `grep -rn 'string_compare\|eval_binop_string_string\|binop_string_string' src/ benches/` and list every test that exercises them.
- [ ] **Step 2:** Delete the module, the two functions and the two match arms; delete the tests found in Step 1 (they test input the parser rejects: `promql-parser` 0.11 `ast.rs` "binary expression must contain only scalar and instant vector types").
- [ ] **Step 3:** `cargo test --lib --features enable-system-alloc -- promql`
      Expected: PASS.
- [ ] **Step 4:** Commit `refactor(promql): drop string-string binary operators the parser never produces`.

### Task 4: E3 — one result alias

**Files:**
- Modify: `src/promql/error.rs:82-83` (delete `PromqlResult`; fix module doc), every `PromqlResult` use (64 sites, `grep -rln PromqlResult src/`)

- [ ] **Step 1:** `sed -i '' 's/PromqlResult/QueryResult/g'` over the files from grep; then fix any `use` line that now imports `QueryResult` twice.
- [ ] **Step 2:** `cargo check --all-targets --features enable-system-alloc`
      Expected: compiles.
- [ ] **Step 3:** `cargo test --lib --features enable-system-alloc -- promql`
      Expected: PASS.
- [ ] **Step 4:** Commit `refactor(promql): keep one result alias`.

### Task 5: D6 — error texts and smoothing-factor bounds

**Files:**
- Modify: `src/promql/functions/rollup_functions.rs:773-774` (`"quantiles_over_time"` → `"quantile_over_time"`)
- Modify: `src/promql/functions/holt_winters.rs:60-80` (`FUNCTION_NAME`, range checks, messages)
- Test: `src/promql/functions/functions_tests.rs` (or a `#[cfg(test)]` block in `holt_winters.rs`)

- [ ] **Step 1:** `grep -n 'double_exponential_smoothing\|holt_winters' src/promql/promqltest/testdata/*.test | grep -v '^.*#'` and note any fixture using sf or tf of exactly 0 or 1.
- [ ] **Step 2:** Write the failing test `smoothing_factor_bounds_are_exclusive`: sf = 0.0, sf = 1.0, tf = 0.0, tf = 1.0 each return `ArgumentError` whose text names the right factor (`"smoothing factor"` for sf, `"trend factor"` for tf) and the right function name (`double_exponential_smoothing` or `holt_winters`, from the call), and sf = 0.5, tf = 0.5 succeeds. *Ruling if Step 1 found a fixture that passes 0 or 1:* keep inclusive bounds and only fix the texts to match (`0 <= sf <= 1`); ledger it.
- [ ] **Step 3:** Run it. Expected: FAIL (bounds inclusive; tf message says "smoothing factor"; name is `double_exponential_smoothing_value`).
- [ ] **Step 4:** Fix: pass the called function's name into `eval_double_exponential_smoothing`; `(0.0 < sf && sf < 1.0)` check; message `"invalid trend factor. Expected 0 < tf < 1, got {tf}"`.
- [ ] **Step 5:** Run the test and `-- promql_tests`. Expected: PASS.
- [ ] **Step 6:** Commit `fix(promql): correct function names and factor bounds in rollup error texts`.

### Task 6: B3 — one `Raw` grid fallback

**Files:**
- Modify: `src/promql/engine/query_reader.rs:691-702` (trait default), add free fn
- Modify: `src/promql/engine/querier.rs:98-123` (toggle-off arm)

**Interfaces:**
- Produces: `pub(in crate::promql) fn raw_grid_read<R: QueryReader + ?Sized>(reader: &R, selector: &VectorSelector, request: &GridRequest, options: QueryOptions) -> QueryResult<GridOutcome>` in `query_reader.rs`: `fetch_bounds()` → `None` gives `Ok(GridOutcome::Raw(Vec::new()))`, else `reader.query_range(selector, start, end, options).map(GridOutcome::Raw)`.

- [ ] **Step 1:** Add `raw_grid_read`; make the trait default `raw_grid_read(self, selector, request, options)`; replace the querier's arm with `return raw_grid_read(self, selector, request, options);` (its own `query_range` normalizes the selector and carries `caller_user`/`hash_tags`).
- [ ] **Step 2:** `cargo test --lib --features enable-system-alloc -- promql` and `-- grid_fallback`
      Expected: PASS.
- [ ] **Step 3:** Commit `refactor(promql): share the raw grid fallback between querier and trait default`.

### Task 7: B5 — limit validators return `QueryError`

**Files:**
- Modify: `src/promql/engine/query_limits.rs:30-42, :54-67, :96-109` (return `QueryResult<()>`)
- Modify: `src/promql/engine/selector_batch_executor.rs:598-618` (delete `validate_max_series_`, `validate_max_points_per_series`), callers at `:667, :710, :726, :779, :858, :860, :1380, :1392`
- Modify: `src/promql/engine/promql_engine.rs:159, :345`; `src/promql/engine/fanout/query_utils.rs:154, :513, :526, :604`

**Interfaces:**
- Produces: `validate_max_series(series_count, max_series) -> QueryResult<()>`, `validate_max_points(points_count, max_points) -> QueryResult<()>`, `get_snapshot_range(..) -> QueryResult<Vec<Sample>>`, all erring with `QueryError::Execution(<same text as today>)`.
- Produces (executor): `fn warned(err: QueryError) -> QueryError` that `log_warning`s and returns it — the one logging site, used as `.map_err(warned)` where the two deleted wrappers were.

- [ ] **Step 1:** Change the three signatures; `cargo check` and fix every caller: engine callers drop `.map_err(QueryError::Execution)`; executor callers use `validate_max_series(..).map_err(warned)?` / `validate_max_points(count, max_points).map_err(warned)?`; shard callers in `query_utils.rs` use `.map_err(super::shard_error)` (and `:513` stores the `QueryError`'s text where it stored the `String`).
- [ ] **Step 2:** `cargo test --lib --features enable-system-alloc -- promql`
      Expected: PASS (limit texts unchanged; `exceeds_max_series_agrees_with_validate_max_series` still passes).
- [ ] **Step 3:** Commit `refactor(promql): return QueryError from limit validators`.

### Task 8: A6 (part) — drop dead `RollupWindow` context, single-window kernel

**Files:**
- Modify: `src/promql/functions/types.rs:155-193` (`RollupWindow`: keep `values`, `timestamps` only)
- Modify: `src/promql/functions/rollups.rs:59-82` (`exec_series_rollup`), `:96-176` (`rollup_series_over_grid`: remove `lookback_ms`, `step_ms`, the `prev_*`/`real_*` computation)
- Modify: `src/promql/functions/rollup_functions.rs:342-380` (`RollupKind::eval_windows` loses `lookback_ms`, `step_ms`) and its callers (`grep -rn 'eval_windows(\|rollup_series_over_grid(' src/`)

**Interfaces:**
- Produces: `rollup_series_over_grid(samples: &[Sample], window_ms: i64, window_ends: impl IntoIterator<Item = Timestamp>, param: Option<f64>, f: fn(&RollupWindow, Option<f64>) -> f64) -> Vec<Sample>`; `RollupKind::eval_windows(self, samples, window_ms, window_ends, param, unit_ms) -> Vec<Sample>`; `fn rollup_window_value(samples: &[Sample], window_end: Timestamp, window_ms: i64, param: Option<f64>, f) -> Option<f64>` (uses `window_range`, de-interleaves only the window).

- [ ] **Step 1:** Remove the four fields and the code that fills them; drop the two parameters through every caller (shard side `GridRequest::evaluate` path, tests).
- [ ] **Step 2:** Add `rollup_window_value`; `exec_series_rollup` uses it instead of `rollup_series_over_grid(..).pop()`.
- [ ] **Step 3:** `cargo test --lib --features enable-system-alloc -- rollup` then `-- promql`
      Expected: PASS, including `every_kind_ignores_samples_outside_the_window`, `test_grid_matches_per_window_evaluation`, `test_basic_and_vectorized_windows_agree`.
- [ ] **Step 4:** Commit `refactor(promql): drop the unread RollupWindow context and the single-window Vec`.

### Task 9: D5 — `ts_of_min/max_over_time` NaN seed

**Files:**
- Modify: `src/promql/functions/rollup_functions.rs:617-654`
- Test: same file, `mod tests`

- [ ] **Step 1:** Write failing test `ts_of_extrema_replace_a_nan_seed`: window `[(10, NaN), (20, 3.0), (30, 1.0), (40, 1.0)]` → `ts_of_min` = 40 ms → `0.04` (last timestamp of the minimum, the documented `<=` rule), `ts_of_max` = 20 ms → `0.02`; all-NaN window → timestamp of the last sample (consistent with `<=`).
- [ ] **Step 2:** Run it. Expected: FAIL (both return the NaN's timestamp, 0.01).
- [ ] **Step 3:** Fix: `if v <= min_value || min_value.is_nan()` / `if v >= max_value || max_value.is_nan()`, as `rollup_min`/`rollup_max` do.
- [ ] **Step 4:** Run it and `-- promql_tests`. Expected: PASS.
- [ ] **Step 5:** Commit `fix(promql): ts_of_min/max_over_time replace a NaN seed like min/max`.

### Task 10: D5 — duplicate kernels and predicates

**Files:**
- Modify: `src/promql/binops/labels.rs` (add `match_key_keeps`), `src/promql/binops/binop_vector_vector.rs:257-270, :607-625` (use it; `result_metric` is NOT changed — it keeps `__name__` under `ignoring`, the key sites never do)
- Modify: `src/promql/binops/binop_vector_scalar.rs`, delete `src/promql/binops/binop_scalar_vector.rs`, `src/promql/binops/mod.rs:34-40`
- Modify: `src/promql/functions/math_functions.rs:138-277`, `src/promql/functions/utils.rs` (add arg unpackers)

**Interfaces:**
- Produces: `pub(super) fn match_key_keeps(matching: Option<&LabelModifier>, name: &str) -> bool` in `binops/labels.rs` — `None`: `name != METRIC_NAME`; `Include(on)`: listed; `Exclude(ignoring)`: `name != METRIC_NAME && !listed`.
- Produces: `fn eval_vector_scalar<F: Fn(f64) -> f64>(expr: &BinaryExpr, vector: Vec<EvalSample>, apply: F) -> ExprResult` in `binop_vector_scalar.rs` (shape of `binop_range_scalar::eval_range`), with the two public entry points resolving `binary_op_fn` once and closing over operand order.
- Produces: `pub(super) fn two_args(name: &str, args: Vec<PromQLArg>) -> EvalResult<(PromQLArg, PromQLArg)>` and `three_args(..)` in `functions/utils.rs`, erring with `exact_arity_error`.

- [ ] **Step 1:** Add `match_key_keeps`; rewrite `match_labels` and `compute_binary_match_key` on it. Run `cargo test --lib --features enable-system-alloc -- binop`. Expected: PASS.
- [ ] **Step 2:** Generic `eval_vector_scalar`; move `binop_scalar_vector`'s entry point into `binop_vector_scalar.rs`; delete the file. Run `-- binop`. Expected: PASS.
- [ ] **Step 3:** `two_args`/`three_args`; use them in `ClampMax`, `ClampMin`, `Clamp`, `MaxOf`, `MinOf`. Run `-- math`. Expected: PASS.
- [ ] **Step 4:** `-- promql` Expected: PASS.
- [ ] **Step 5:** Commit `refactor(promql): share the match-key predicate, vector-scalar body and arity unpacking`.

### Task 11: C6 — dead wire code, enum tables, aggregation sink, proto doc

**Files:**
- Modify: `src/promql/engine/fanout/type_conversions.rs:32-60, :262-300, :364-375, :571-590` (delete dead `From` impls; keep `From<&Matchers>`), `:381-434` (`AggregationKind` ↔ proto via macro)
- Modify: `src/promql/engine/selector_batch_executor.rs:1186` (`From<Matchers>` → `From<&Matchers>`)
- Modify: `src/promql/engine/fanout/grid_fanout_command.rs:73-150` (`RollupKind` ↔ proto via the same macro)
- Modify: `src/promql/engine/fanout/aggregation_fanout_command.rs:74-100, :145-160, :170-240, :360-400` (`AggregationSink`)
- Modify: `proto/v1/promql.proto:54` (`reserved 5;`), `:249-257` (doc), regenerate `proto/v1/generated/`
- Modify: `src/promql/engine/fanout/query_utils.rs:172` and every `max_points_per_series:` initializer on `InstantQuery`

**Interfaces:**
- Produces: `macro_rules! wire_enum_map { ($kind:ty, $proto:ty; $($variant:ident),+ $(,)?) }` in `type_conversions.rs` generating `impl From<$kind> for $proto` and `impl From<$proto> for $kind` (both directions 1:1), used for `AggregationKind`/`ProtoAggregationKind` and `RollupKind`/`ProtoRollupKind`; one test per pair asserting `for k in all() { assert_eq!(Kind::from(Proto::from(k)), k) }`.
- Produces: `enum AggregationSink { Partials(PartialGroups), Candidates(Vec<EvalSample>) }` replacing `strategy` + `partials` + `samples`; the strategy is `self.aggregation.kind.pushdown_strategy()`.

- [ ] **Step 1:** Delete the dead `From` impls; `cargo check --all-targets --features enable-system-alloc`; fix the one `From<Matchers>` caller and any test fixture that used `From<EvalSample>` (build the proto sample by hand there). Expected: compiles.
- [ ] **Step 2:** `wire_enum_map!` for both pairs with round-trip tests. Run `-- fanout`. Expected: PASS.
- [ ] **Step 3:** `AggregationSink`; run `-- aggregation_fanout`. Expected: PASS.
- [ ] **Step 4:** Proto: `reserved 5;` on `InstantQuery`, fix the "not fused" doc; `VALKEY_TS_PROTO_REGEN=1 cargo build --features enable-system-alloc`; remove the field from Rust initializers. Expected: builds; `cargo build` without the env var passes the drift check.
- [ ] **Step 5:** `-- promql` Expected: PASS.
- [ ] **Step 6:** Commit in two: `refactor(fanout): remove dead PromQL wire conversions and table the kind mappings` and `refactor(fanout): model the aggregation sink as an enum; reserve InstantQuery.max_points_per_series`.

### Task 12: C5 — shard error handling

**Files:**
- Modify: `src/promql/engine/fanout/mod.rs:37-43` (add `required`, `peer_error`)
- Modify: `instant_vector_selector_fanout_command.rs:88`, `range_vector_selector_fanout_command.rs:79-82`, `grid_fanout_command.rs:299-301, :378-383, :432-436, :470-474`, `aggregation_fanout_command.rs:231, :259-269`, `label_profile_fanout_command.rs:141-145`, `query_utils.rs:103, :349, :411-425, :573-605`
- Test: `src/promql/engine/fanout/mod.rs` `#[cfg(test)]`

**Interfaces:**
- Produces: `pub(super) fn required<T>(field: Option<T>, what: &str) -> ValkeyResult<T>` → `Err(ValkeyError::String(format!("TSDB: fan-out request has no {what}")))`.
- Produces: `pub(super) fn peer_error(target: &NodeInfo, what: &str, why: impl std::fmt::Display) -> FanoutError` → `FanoutError::custom(format!("TSDB: peer {} {what}: {why}", target.socket_address))`.

- [ ] **Step 1:** Write failing test `required_refuses_a_missing_field`: `required(None::<u8>, "selector")` is `Err` whose text contains `"no selector"`; `required(Some(3u8), "x") == Ok(3)`. Expected: FAIL (not defined).
- [ ] **Step 2:** Add `required` and `peer_error`; replace the six warn-and-empty sites with `let selector = required(req.selector, "selector")?;` (aggregation: `required(req.query, "instant-vector parameters")?` first); drop the `req.selector.clone()` in grid.
- [ ] **Step 3:** `query_utils.rs`: `budget_error` → `super::shard_error`; inline `ValkeyError::String(err.to_string())` → `shard_error`; the two `par_rayon` closures carry `QueryError` (`QueryError::Execution(e.to_string())` for the serialize error) and the chain ends in `.map_err(shard_error)`.
- [ ] **Step 4:** Grid's three inline peer formats → `peer_error(target, "returned an undecodable raw series", why)` etc.; aggregation `:231` and instant `:135` wrap their `ValkeyError` with `peer_error(target, "answered with", err)`.
- [ ] **Step 5:** `-- fanout` and `-- promql`. Expected: PASS (the `TooManySamples` re-parse test in `mod.rs` still passes: `shard_error` renders the same text).
- [ ] **Step 6:** Commit `fix(fanout): refuse malformed PromQL requests and report shard errors one way`.

### Task 13: C4 — grid plumbing

**Files:**
- Modify: `src/promql/engine/query_reader.rs:439-540` (`GridSink::take_charged`)
- Modify: `src/promql/engine/fanout/grid_fanout_command.rs:267-287, :680-690, :700-712, :742-800`
- Modify: `src/promql/engine/fanout/grid_fanout_command/transport.rs:164-230`, `columns.rs:17-65, :88-119`

**Interfaces:**
- Produces: `GridSink::take_charged(&mut self, request: &GridRequest, output: GridSeries, read: usize, deadline: &Deadline, budget: &SampleBudget) -> QueryResult<()>` = `take` then `budget.charge(added.saturating_sub(read))`; used by `GridFanoutCommand::into_result` and in place of `ShardGrid::stage` (deleted).
- Produces: `columnar_series_from<I>(window_ends: &[i64], entries: impl IntoIterator<Item = (EvalLabels, I)>, with_lag: bool, symbols: &mut SymbolTableBuilder) -> Vec<ProtoGridSeries> where I: Iterator<Item = (i64, i64, f64)>`; `columnar_series(..)` becomes a wrapper over it; `respond()` is one `match` with the `Stepped` arm calling it with `request.sample_timestamps`.
- Produces: `staged_groups(request, staged: impl Iterator<Item = &RangeSample<EvalLabels>>)`; `promoted_raw(request, window_ends, raw: &[RangeSample<EvalLabels>], staged_groups)`; `transport_plan` (test) adapts by collecting its raw subset.
- Produces: `decode_columns` validates via `count_ones` and the highest set bit, iterating lazily; `encode_columns` reserves `points.size_hint().0`.

- [ ] **Step 1:** `take_charged`; delete `ShardGrid::stage`; `into_result` uses it. Run `-- grid_fanout`. Expected: PASS.
- [ ] **Step 2:** `columnar_series_from`; collapse `respond()`'s two matches. Run `-- grid_fanout`. Expected: PASS (the columnar tests cover both lag and no-lag).
- [ ] **Step 3:** Reshape `staged_groups`/`promoted_raw`; `ShardGrid::finish` passes `candidates.iter()` / `&candidates`. Run `-- transport`. Expected: PASS.
- [ ] **Step 4:** `decode_columns`/`encode_columns`. Run `-- columns`. Expected: PASS (corrupt-bitmap tests still reject).
- [ ] **Step 5:** `-- promql`. Expected: PASS.
- [ ] **Step 6:** Commit `refactor(fanout): one take-then-charge, one columnar encoder, iterator-shaped transport helpers`.

### Task 14: E5 — time arithmetic helpers

**Files:**
- Modify: `src/promql/model.rs` (`EvalContext::modified_eval_ts`), `src/promql/exec/planner.rs` (`PreloadGrid::modified_ts`), `src/promql/engine/mod.rs` (`QueryOptions::with_lookback_ms`)
- Modify: the nine `apply_time_modifiers_ms(` call sites (`grep -rn 'apply_time_modifiers_ms(' src/promql | grep -v 'fn \|_tests'`) and the seven `options.lookback_delta = Duration::from_millis` sites
- Modify: `src/promql/utils.rs:36-48` (`calc_points`) and `src/promql/exec/pipeline.rs:219` only if their semantics equal `grid_step_count`/`step_times` (check the saturating and `step < 1` cases first; ledger the finding either way)

**Interfaces:**
- Produces: `impl EvalContext { pub(crate) fn modified_eval_ts(&self, at: Option<&AtModifier>, offset: Option<&Offset>) -> Timestamp }` = `apply_time_modifiers_ms(at, offset, self.query_start, self.query_end, self.evaluation_ts)`.
- Produces: `impl PreloadGrid { pub(crate) fn modified_ts(&self, at: Option<&AtModifier>, offset: Option<&Offset>, ts: Timestamp) -> Timestamp }` against `at_start_ms`/`at_end_ms`.
- Produces: `impl QueryOptions { pub(crate) fn with_lookback_ms(self, ms: i64) -> Self }`.

- [ ] **Step 1:** Add the three helpers; replace the call sites. `cargo check`. Expected: compiles; `grep -c 'apply_time_modifiers_ms(' src/promql/exec src/promql/engine` shows only the helpers.
- [ ] **Step 2:** `-- promql`. Expected: PASS.
- [ ] **Step 3:** Commit `refactor(promql): name the time-modifier and lookback-override idioms`.

### Task 15: E7 — error types in `error.rs`, explicit re-exports, small cleanups

**Files:**
- Modify: `src/promql/error.rs` (receive `EvaluationError`, `EvalResult`, both `From` impls), `src/promql/exec/types.rs:20-80` (remove them), `src/promql/mod.rs:27-29` (explicit `pub use` lists), `src/promql/exec/mod.rs:12-13`
- Modify (minor, each one line): `const_folding.rs:173/:242` (`Pi` once), `:156` (drop the `StringLiteral` arm), `aggregations.rs:91` (remove `_ => Reduce`), `:538` (delete the stale todo), `:366` (`count_values_bucket_key` via `FingerprintHasher::write_u128`'s fold), `hashers.rs:149` (`FingerprintHashSet` on the same halfbrown map type, if its API suffices), `bitset.rs:24-35` (doc says debug-asserts), `function_list.rs:97` (drop the `allow`), `date_functions.rs:89` (inline `sample_value`), `optimize.rs:201-208` (docstring), `pipeline.rs` module doc and `QueryPathKind` docs (remove "bucket lists"); `ensure_unique_labelsets` moves from `binops/labels.rs` to `exec/utils.rs`
- Rename: `src/promql/optimizer/pushdown.rs` → `filter_pushdown.rs` (and `pushdown_tests.rs` → `filter_pushdown_tests.rs`)

- [ ] **Step 1:** Move `EvaluationError`/`EvalResult` and the two `From`s into `error.rs`; replace the three globs in `promql/mod.rs` with explicit lists (let `cargo check --all-targets` enumerate what is needed). Expected: compiles with the same external paths.
- [ ] **Step 2:** The one-line cleanups and the rename (`git mv`). `cargo clippy --all-targets --features enable-system-alloc -- -D clippy::all`. Expected: clean.
- [ ] **Step 3:** `-- promql`. Expected: PASS.
- [ ] **Step 4:** Commit in two: `refactor(promql): move evaluation errors to error.rs and name the re-exports` and `chore(promql): small cleanups and rename the filter push-down module`.

### Task 16: Final verification

- [ ] **Step 1:** `cargo fmt --check && cargo clippy --profile release --all-targets --features enable-system-alloc -- -D clippy::all`. Expected: clean.
- [ ] **Step 2:** `cargo test --lib --features enable-system-alloc` and `cargo test --doc --features enable-system-alloc`. Expected: PASS, same count as Task 1 minus deleted dead-code tests plus the new ones.
- [ ] **Step 3:** `git log --oneline refactor/promql-phase0 ^promql` lists one commit per task (two for Tasks 11 and 15).
- [ ] **Step 4:** Whole-branch review per superpowers:executing-plans.
