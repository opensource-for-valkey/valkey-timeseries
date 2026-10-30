# PromQL refactoring assessment

Date: 2026-10-30 (first draft and same-day revision)

Status: static code review; implementation and performance measurements remain outstanding.

The first draft covered the evaluation core (items A1–A6 below). The revision re-verified
those six against the code and extended the review to the whole subsystem: the engine
(reader, executor, limits), the cluster push-down layer, the function and binary-operator
kernels, and the optimizer and shared types — 98 files, about 52k lines. Each finding is
marked **VERIFIED** (the code path was read end to end and, where it says "unused", grepped
across `src/` and `benches/`) or **PLAUSIBLE** (mechanism confirmed, magnitude or payoff
not). Performance benefits remain hypotheses until measured.

The strongest opportunities are still in preparation and data reuse, now joined by three
resource-safety gaps (a deadline not checked, a lock held through label hashing, a decode
done before the budget is charged) and a large amount of duplicated scaffolding. The
subsystem already has useful optimizations — grid preloading, shared subquery reads,
bounded step batches, copy-under-lock/decode-outside, one fold for shard and coordinator —
and those are listed under *Worth preserving* so they survive the refactoring.

## Priority

Scored per item: Impact (how much it costs in CPU, latency or developer time, 1–5), Risk
(what happens if left alone, 1–5), Effort (1–5). Priority = (Impact + Risk) × (6 − Effort).
Grouped minor cleanups are not scored individually.

| # | Item | Impact | Risk | Effort | Priority |
|---|------|--------|------|--------|----------|
| B2 | Label profiles built under the module GIL | 4 | 4 | 2 | 32 |
| A6 | Rollup drivers: deadline gap, dead window context, per-step allocations | 3 | 4 | 2 | 28 |
| A3 | Grouping labels per sample; re-fingerprinting on the coordinator | 3 | 2 | 1 | 25 |
| C5 | Shard error handling: malformed request answered empty, three error spellings | 2 | 3 | 1 | 25 |
| C2 | Grid coordinator decodes before charging; no duplicate-shard guard | 3 | 4 | 3 | 21 |
| B3 | Single-node `Raw` fallback written three times | 2 | 2 | 1 | 20 |
| B5 | Limit validators return `String`; colliding names | 2 | 2 | 1 | 20 |
| D4 | `vmrange` conversion runs on every histogram call, untested | 2 | 3 | 2 | 20 |
| A2 | Share raw reads across consumers of one selector | 4 | 2 | 3 | 18 |
| D2 | `label_replace`/`label_join` materialize interned label sets per sample | 3 | 1 | 2 | 16 |
| E1 | Const folding gaps; `simplify_expr` clones and compares per pass | 2 | 2 | 2 | 16 |
| A1 | Make prepared queries immutable | 3 | 2 | 3 | 15 |
| B1 | Five cluster fan-out starters share one skeleton | 2 | 1 | 2 | 12 |
| B4 | Stepped grid re-materialized before every fold | 2 | 1 | 2 | 12 |
| D3 | Unreachable arity stubs; `PromQLArg` duplicates `ExprResult` | 2 | 1 | 2 | 12 |
| C7 | Time budget carried only by PromQL requests | 3 | 3 | 4 | 12 |
| A5 | Turn `PlannedQuery` into an execution plan | 3 | 2 | 4 | 10 |
| C1 | `FanoutCommand: Default` forced by one `mem::take` | 2 | 1 | 3 | 9 |
| C3 | Shard-side `(group, step)` fold through a `BTreeMap` per point | 2 | 1 | 3 | 9 |
| A4 | Cache evaluated subquery grids | 4 | 2 | 5 | 6 |

A4 scores low on this formula only because of its effort; its payoff for overlapping
subquery windows is the largest single gain on the list and it stays on the plan as a
separate, higher-risk track.

## Part A — Evaluation core

### A1. Make prepared queries immutable

[`PreparedQuery`](../../src/promql/exec/evaluator/mod.rs) (`mod.rs:84-110`) holds four
`Arc<RwLock<...>>` maps, an `AtomicBool` and an `Arc<OnceLock<NodeKeys>>`, although
evaluation only reads them. `Evaluator` copies all seven fields rather than holding one
`Arc<PreparedQuery>` (`mod.rs:146-250`): `with_shared` clones seven Arcs, and
`evaluate_subquery` (`subquery.rs:108`) does that for every outer step of a range query.
Every per-step lookup takes a read lock — `evaluate_vector_selector`,
`evaluate_matrix_selector`, `preloaded_grid_by_key`, `is_preloaded`, `prepared_union`.
[`Preloader`](../../src/promql/exec/preloader.rs) constructs an `Evaluator` only to call
`preload_grid` and then `into_prepared`.

Introduce a mutable preparation builder that produces one `Arc<PreparedQuery>` containing
ordinary maps and completed keys. Keep the sample budget separately shared. This removes
locks from per-step lookups, reduces refcount traffic when constructing sub-evaluators,
and makes the preparation/evaluation boundary explicit.

**Effort:** medium. **Benefit:** broad simplification and reduced evaluation overhead.
**VERIFIED.**

### A2. Share raw reads across different consumers of the same selector

Preloading deduplicates by request key, not by what is read
([`preload.rs`](../../src/promql/exec/evaluator/preload.rs)):

- `preload_stepped_aggregations` keys by `GridPreloadKey::stepped(vs, AggregationKey)`
  (`:617-627`), so `sum(a) + avg(a)` issues two requests. On a single node both come back
  `GridOutcome::Raw`; each reads the span, charges it, buckets it in `cache_stepped_span`
  and inserts under the same `PreloadKey::from_selector(vs)` (`:655-668`, `:795-850`) —
  the second overwrites the first.
- `preload_rollups` keys by `GridPreloadKey::rollup(vs, rollup, aggregation)` (`:199-215`),
  so `rate(a[5m]) / increase(a[5m])` reads the same span twice and runs
  `GridRequest::evaluate` twice over separate copies.

Plan shared raw spans for local execution — one read per `(selector, span)` feeding every
rollup and stepped aggregation over it — while retaining distinct fused requests where
shard push-down saves transport. Preserve correct accounting for shared inputs and
separately retained outputs (`GridCover`, `Keeps`).

**Effort:** medium. **Benefit:** potentially substantial savings for compound queries.
**VERIFIED.**

### A3. Avoid rebuilding grouping labels for every shard sample

`PartialGroups::accumulate` in
[`partial_aggregation.rs`](../../src/promql/exec/partial_aggregation.rs) (`:255`)
calls `compute_grouping_labels` for every sample before finding its group. The local path
in `aggregations.rs` already uses the better pattern (`group_samples :703-719`,
`group_sample_values :721-742`): `compute_grouping_key` (allocation-free, `exec/types.rs:548`)
for every sample, `compute_grouping_labels` only when inserting a group.
`SteppedPartialGroups::accumulate` (`:358`) does it once per series, which is better but
still materializes labels before the group is known to be new.

Two adjacent costs on the coordinator: `PartialGroups::into_partials` (`:268-270`) discards
the `FingerprintHashMap` key, and `SteppedPartialGroups::finalize` (`:405-425`),
`SteppedSelection::finalize` (`:522`) and `merge_count_values` (`:592`) then re-hash every
label string per `(group, step)` or per output sample. Yield `(key, labels, state)` from
`into_partials`. And the no-modifier reductions — `sum(x)`, the commonest aggregation —
still fingerprint an empty set and probe the map per sample where `eval_top_bottom_k`
(`aggregations.rs:476`) short-circuits `modifier.is_none()`; the three grouping loops
(`group_samples`, `group_sample_values`, `eval_count_values :289-302`) could be one
generic `group_by`.

**Effort:** small. **Benefit:** fewer allocations and hashes in shard and coordinator
aggregation; a good first change. **VERIFIED** (single-group fast path PLAUSIBLE).

### A4. Cache evaluated subquery grids, beyond their input preloads

Subquery evaluation in [`subquery.rs`](../../src/promql/exec/evaluator/subquery.rs)
shares preloaded inputs but still evaluates every inner step and builds a new series map
for each outer window (`:108-184`).

For `max_over_time((a + b)[1h:1m])`, adjacent outer windows repeatedly evaluate most of the
same inner timestamps. Evaluate the inner expression over its union grid once, then expose
shared sample windows to each outer step. Bound retained memory (charge the matrix to the
sample budget, fall back to per-step evaluation past it) and preserve timestamps, sparse
series, `@ start()/end()` resolution and pending metric-name drops. Build it on the
query-driving thread, never lazily from a pool job. The `subquery_twins` bench shows a
twin costing about twice a single subquery after content keys, so evaluation rather than
reading dominates.

**Effort:** high. **Benefit:** high potential impact for overlapping subquery windows;
higher implementation risk. **VERIFIED** (problem), **PLAUSIBLE** (gain).

### A5. Turn PlannedQuery into an actual execution plan

`preload_grid` ([`preload.rs:46-96`](../../src/promql/exec/evaluator/preload.rs)) walks the
expression seven times: `calls_function(expr, "timestamp")`,
`collect_stepped_aggregation_candidates`, `collect_vector_selectors`,
`collect_rollup_candidates` twice (`preload_rollups`, `preload_matrices`),
`collect_subqueries`, and `fill_node_keys`. `pushable_rollup` and `GridPreloadKey::rollup`
are rebuilt for the same call nodes in three passes (`preload.rs:199, :274`;
`pushdown.rs:76, :87`), and `fill_node_keys` recomputes keys the preload passes just
derived. The five walkers in `exec/utils.rs` (`:56-90, :106-138, :148-174, :215-251,
:254-276`) differ only in what they collect and whether they cross a `Subquery` or a covered
aggregation; `promql/utils.rs:160-182 for_each_child` already abstracts child iteration.

Build one analysis per grid containing candidates, dependencies, keys and fallback choices,
and execute its phases in dependency order. Node keys become plan slots instead of a table
keyed by node address. Structural subquery identity could replace `Debug` formatting in
`SubqueryKey::of` (`subquery.rs:296`), provided it preserves distinctions such as signed
zero. Keep analysis scoped to each evaluation grid, including the separate grids of nested
subqueries.

**Effort:** medium to large. **Benefit:** primarily maintainability, with less repeated
expression analysis. **VERIFIED.**

### A6. Narrow the rollup API and fix its drivers

[`rollups.rs`](../../src/promql/functions/rollups.rs) and
[`functions/types.rs`](../../src/promql/functions/types.rs):

- `exec_series_rollup` (`rollups.rs:59-82`) calls `rollup_series_over_grid` for one window,
  allocates an output `Vec`, then pops it. `rollup_series_over_grid` (`:96-176`) also
  de-interleaves every series into fresh `values`/`timestamps` Vecs per call — amortized
  across all steps on the push-down grid path, paid per series per step on the local path
  (every rollup over a subquery, every non-literal parameter). Only the `Windowed` kernels
  (`avg`, `sum`, `min`, `max`, `range`, `quantile`, `mad`, `stddev`, `stdvar`, `ts_of_min`,
  `ts_of_max`) use that view; `rate` and friends are `WindowAware` over `&[Sample]`. A
  single-window kernel returning `Option<f64>` from the `&[Sample]` window removes two of
  the three allocations; whether the de-interleave earns its keep on the grid path is a
  measurement (Kahan sums are a serial chain, so the SIMD argument is weak).
- `RollupWindow`'s `prev_value`, `prev_timestamp`, `real_prev_value` and `real_next_value`
  (`types.rs:170-193`) are populated for every window (`rollups.rs:132-160`) and read
  nowhere in the crate; `every_kind_ignores_samples_outside_the_window` forbids depending
  on them. Remove the fields and the context computation.
- Six per-series drivers share one shape — `eval_rollups`, `eval_rollups_basic`,
  `range_vector_functions::eval_range`, `rate::calculate_rate`, `deriv`, `holt_winters`
  (`into_par_rayon → per series → EvalSample`). The first three call
  `deadline.checkpoint(...)`; **`rate`, `deriv` and `holt_winters` do not**
  (`rate.rs:49`, `deriv.rs:16`, `holt_winters.rs:84`), so a long range over many series
  runs those kernels past a passed deadline. None has the size threshold the binop path
  measured for itself (`binop_vector_vector.rs:26`, ~30 µs per fan-out), so a 1000-step
  query over ten series pays 1000 pool fan-outs per rollup. One `eval_series` driver fixes
  both and makes `drop_name` propagation uniform.
- `RollupImpl` (`rollup_functions.rs:74-100`) has five variants for two shapes, and
  `eval_windows` (`:342-380`) wraps each into a closure for `over_windows`. Collapse to
  `Windowed` plus `Whole(fn(&[Sample], &WindowCtx) -> Option<f64>)`.
- `RollupKind` keeps four parallel 27-entry lists (`from_function_name :112`,
  `function_name :151`, `all :231`, `implementation :301`) plus a fifth in
  `engine/fanout/grid_fanout_command.rs:73-150`. `strum`'s `EnumIter`/`IntoStaticStr` are
  already a dependency (`function_list.rs`).
- Two window-bounds searches, `seek_first_timestamp_idx_after` (`:185-230`) and
  `window_range` (`:283`), pinned equal by a test; `partition_point` on
  `&timestamps[cursor..]` is the same thing in two lines.

**Effort:** small for the API and deadline fix, medium with the driver merge.
**Benefit:** resource limits honoured in every rollup; simpler APIs; fewer allocations.
**VERIFIED** (deadline gap, dead fields, duplication); **PLAUSIBLE** (allocation gain).

## Part B — Engine: reader, executor, limits

### B1. Five cluster fan-out starters share one skeleton

[`selector_batch_executor.rs:640-913`](../../src/promql/engine/selector_batch_executor.rs):
`execute_cluster_vector_selector`, `_range_selector`, `_aggregation`, `_grid` and
`_label_profile` each do `calculate_timeout` → build command →
`compute_hash_tag_fanout_target` → wrap the responder in an `Arc` (redundant:
`mpsc::SyncSender` is `Clone`) → handler closure → `exec_command` → `deliver_task_result`
on error. Only the command constructor and the `Ok` arm differ. One generic
`fan_out(ctx, cmd, hash_tags, timeout, responder, on_ok)` takes ~270 lines to ~120.
`execute_selector_task` (`:532-572`) also destructures a task and rebuilds it so
`execute_selector_task_cluster` can destructure it again.

**Effort:** medium. **Benefit:** simplicity. **VERIFIED.**

### B2. Label profiles are built under the module GIL

`LocalRead::copy`'s `Profile` arm (`selector_batch_executor.rs:1298-1331`) calls
`builder.add_series(...)` inside the `cursor.next_batch` closure, under
`MODULE_CONTEXT.lock_gil()`. `LabelProfileBuilder::add_series`
([`label_profile.rs:365-381`](../../src/promql/engine/label_profile.rs)) does, per label of
every kept series, an `AHashMap` lookup, a `BTreeSet<String>::contains` and a `String`
allocation for each new value. Per hold that is up to `HOLD_BUDGET` series × their labels
of hashing on the lock the main thread waits for. The range path copies under the lock
and decodes after (`RangeSnapshot`); the instant path only interns
(`EvalLabels::interned`, a refcount bump). Do the same here: collect interned label sets
under the lock, `add_series` off it.

**Effort:** medium. **Benefit:** shorter lock holds, lower server-wide PING p99 for
derived-filter range queries over wide selectors. Code path **VERIFIED**; magnitude
**PLAUSIBLE** (measure with `tools/latency_report.sh` on `a - b` over ~50k series).

### B3. The single-node `Raw` fallback is written three times

`querier.rs:111-125` (toggle off: `fetch_bounds` → `Raw(Vec::new())` or `query_range`),
`query_reader.rs:697-702` (trait default, identical) and
`selector_batch_executor.rs:1155-1171` (`LocalRead::plan` grid arm, same shape). One free
function; the querier arm calls it.

**Effort:** small. **Benefit:** three copies cannot drift. **VERIFIED.**

### B4. Stepped grid output is re-materialized before every fold

`SteppedSeries::into_step_values` ([`query_reader.rs:173-182`](../../src/promql/engine/query_reader.rs),
`:211-219`) builds a fresh `Vec<Sample>` per series from `Vec<SteppedPoint>`, and
`GridSink` calls it for every chunk of every fused stepped request (`:485, :508, :523`) on
shards, coordinator and single node alike. The consumers
(`SteppedPartialGroups::accumulate`, `SteppedSelection::transpose`) read only
`(timestamp, value)` per point. Let them take an iterator or `GridSeries` directly.

**Effort:** medium. **Benefit:** one copy less per fused sample. Copy **VERIFIED**; gain
**PLAUSIBLE** (memcpy-bound).

### B5. Limit validators return `String` and every caller re-wraps

[`query_limits.rs:30-42`](../../src/promql/engine/query_limits.rs) `validate_max_series`
returns `Result<(), String>`; callers `.map_err(QueryError::Execution)` at
`promql_engine.rs:159, :345`, `fanout/query_utils.rs:154, :526, :604`, and
`selector_batch_executor.rs:598-604` wraps it again as `validate_max_series_` (trailing
underscore) to add a log line. `selector_batch_executor.rs:606-618
validate_max_points_per_series` re-checks `max > 0` that `query_limits.rs:97-99` already
does, and its name collides with `promql::utils::validate_max_points_per_timeseries` (a
step-count check). Return `QueryError` from `query_limits`, one error variant per limit,
log in one place.

**Effort:** small. **Benefit:** consistent error handling. **VERIFIED.**

### B6. Smaller engine items

- `ConcreteSeriesQuerier` (`querier.rs:208-318`) is a cfg-enum with a dead variant in
  every build: 110 lines of five-method delegation plus `expect(dead_code)`, one caller.
  Two `#[cfg]` type aliases and one constructor replace it. **VERIFIED.**
- `impl QueryReader for Arc<dyn QueryReader>` (`query_reader.rs:720-769`) exists because
  `evaluate_instant` passes `&reader` (`promql_engine.rs:150`) while `evaluate_range` passes
  `reader.as_ref()` (`:261`). Use `as_ref()` everywhere and delete 50 lines. **VERIFIED.**
- `LocalRead::finish` (`:1360-1369`) swaps in a dummy `ReadState` because `retain_mut`
  hands out `&mut`; partition instead and let `finish(self)` consume. **VERIFIED.**
- `QueryOptions` has three sources of truth (`engine/mod.rs:90-119 Default` reads the
  `PROMQL_CONFIG` lock and the clock; `command_parser.rs` and `ts_query.rs` build it too)
  and carries both `timeout` and `deadline`, reconciled by `with_resolved_deadline`
  (`promql_engine.rs:74-80`), after which `timeout` is read only by `calculate_timeout`.
  One `from_config(&PromqlConfig, now)` constructor, `Default` test-only, `deadline` as the
  single time field. **PLAUSIBLE** (command parser not read end to end).
- Per-read clones in the querier (`querier.rs:50-146`: `caller_user: Option<String>`;
  `:162-203 normalize_selector` clones `Matchers`), paid per step on live-read fallbacks.
  `Option<Arc<str>>` for the user is the part worth doing. **VERIFIED.**
- `query_cache.rs:341-347` clones every key into `oldest` on each eviction;
  `select_nth_unstable` on refs then remove by index. `Charge::string(value, capacity)`
  (`:397-400`) uses `value` only in a `debug_assert`. `query_limits.rs:61-68
  get_snapshot_range` is `take(n + 1).collect()` plus a length check.
  `selector_batch_executor.rs:851-857` collects a `Vec<usize>` only to validate it.
  **VERIFIED.**

## Part C — Cluster push-down layer

### C1. `FanoutCommand: Default` forces placeholder constructors

`src/fanout/fanout_command.rs:492` does `std::mem::take(&mut self.operation)`, which is why
the trait is `Default + Send + 'static` (`:52`). Every PromQL command therefore hand-builds
a dummy: `GridFanoutCommand` a `GridRequest` (`grid_fanout_command.rs:186-208`),
`AggregationFanoutCommand` a `PartialGroups` (`:102-128`), the instant command and
`InstantVectorParams::default` (`:30-48, :58-70`) take the `PROMQL_CONFIG` read lock to
fill a `lookback_delta` nobody reads. `Option<OP>` plus `take()` drops the bound for all 17
operations, TS.* included.

**Effort:** medium (touches `src/fanout`). **Benefit:** ~70 lines of dummy state gone.
**VERIFIED.**

### C2. Grid coordinator decodes raw chunks before charging; no duplicate-shard guard

[`grid_fanout_command.rs:465-487`](../../src/promql/engine/fanout/grid_fanout_command.rs):
`range_sample_from_proto` fully decodes each raw chunk (`type_conversions.rs:162-168`)
*then* `budget.charge(added + raw_samples)`. The range path checks `chunk.len()` before
decoding (`selector_batch_executor.rs:716-728`) and decodes on the pool; `on_response` runs
on the main thread for responses under 64 KiB (`src/fanout/fanout_command.rs:242`).
Separately, the instant (`instant_vector_selector_fanout_command.rs:136-145`) and range
(`check_unique_series`) paths refuse a series answered by two shards, while the grid
(`GridSeries::append`) and aggregation (`samples.extend`, `raw.extend`) paths silently
double-count. Either the guard matters (a slot mid-migration) and all four need it, or it
does not and the two selector paths can drop it. Relatedly, the shard's grid read decodes
an over-wide series fully before refusing it (`query_utils.rs:414` then `WindowBounds`),
where the range path streams against the limit (`get_snapshot_range`, `:579`).

**Effort:** medium. **Benefit:** budget enforced before the work, consistent cluster
semantics. **VERIFIED** (asymmetries), **PLAUSIBLE** (cost).

### C3. Shard-side `(group, step)` fold goes through a `BTreeMap` per point

`SteppedPartialGroups::accumulate` (`partial_aggregation.rs:346-372`) does
`steps.entry(point.timestamp)` (O(log steps)) plus a hash per sample of staged output;
`merge` (`:375-390`) does the same per partial on the coordinator. Both sides hold
`window_ends`, and the wire already addresses points by window index (`columns.rs`). A
`Vec<PartialGroups>` indexed by step is O(1) and removes the ordering argument the
`BTreeMap` exists for.

**Effort:** medium. **Benefit:** cheaper fused `sum by` folds off the GIL. **PLAUSIBLE.**

### C4. Duplicated grid plumbing

- "Take, then charge the excess" appears three times: `GridFanoutCommand::into_result`
  (`:267-287`), `ShardGrid::stage` (`:680-690`), `GridRequest::evaluate`
  (`query_reader.rs:365-383`). Let `evaluate` accept a sink. **VERIFIED.**
- `respond()` (`grid_fanout_command.rs:742-800`) matches `staged` twice and its `Stepped`
  arm rebuilds `ProtoGridSeries` as `columns.rs:93-119` does minus the lag flag. One match,
  one `columnar_series`. **VERIFIED.**
- Transport helpers are shaped for the test harness: `staged_groups`
  (`transport.rs:193-203`) takes `(&RangeSample, &bool)` and ignores the bool; production
  passes `&false`. `promoted_raw` (`:218`) takes `&[&RangeSample]`, so `ShardGrid::finish`
  builds a `Vec<&_>` per response. Only `#[cfg(test)] transport_plan` needs the zipped
  form. **VERIFIED.**
- `decode_columns` (`columns.rs:51-60`) collects a `Vec<usize>` of present indices just to
  validate counts; `count_ones` suffices. `encode_columns` (`:23`) starts from `Vec::new()`
  though the caller knows the length. **VERIFIED.**

**Effort:** small each.

### C5. Shard error handling

- A request with no selector is answered with a warning and an empty response in six
  places (`instant :88`, `range :79` — with a `// todo: return error` —, `grid :301`,
  `aggregation :259/:265`, `label-profile :141`). That contradicts the layer's own rule
  (`decode_request`, `decode_pushdown_kind` refuse corrupt input) and `TS.MRANGE`'s
  `req.try_into()?`. One `required(field, "selector")?`. **VERIFIED.**
- `QueryError → ValkeyError` has three spellings on the shard (`query_utils.rs:103
  budget_error`; `:349` inline; `:411-425` `to_string` then `ValkeyError::String`) beside
  `fanout::shard_error` (`mod.rs:37-43`), which additionally maps `Timeout` to
  `FANOUT_REQUESTER_GAVE_UP`. Budget errors are never `Timeout`, so `shard_error`
  everywhere is behaviour-neutral. **VERIFIED.**
- Peer-error formatting diverges: three inline `FanoutError::custom(format!("TSDB: peer {}
  …"))` in the grid command beside `wrong_payload`/`decode_peer_partial`; the aggregation
  and instant commands propagate the bare `ValkeyError`, losing the peer. One
  `peer_error(target, what, why)`. **VERIFIED.**
- `TooManySamples{loaded, limit}` survives the wire only as text re-parsed in
  `mod.rs:45-77`; a message change breaks it silently into `Execution`. **PLAUSIBLE**; note.

**Effort:** small. **Benefit:** corrupt requests fail loudly; one error path.

### C6. Dead and duplicated wire code

- Dead conversions in [`type_conversions.rs`](../../src/promql/engine/fanout/type_conversions.rs):
  `From<InternedLabel> for ProtoLabel` (`:32`), `From<MetricName>`/`From<&Labels>`/
  `From<&EvalLabels> for Vec<ProtoLabel>` (`:41, :50, :584`), `From<ProtoInstantSample> for
  EvalSample` (`:56`), `From<EvalSample> for ProtoInstantSample` (`:571`) — inline-label
  encoders from before the symbol table; production builds `labels: Vec::new()` plus refs.
  Owned selector variants (`:287, :364, :370`) have no non-test callers; `From<Matchers>`
  (`:262`) duplicates `From<&Matchers>` (`:329`). `InstantQuery.max_points_per_series`
  (`promql.proto:54`) is never read shard-side (`query_utils.rs:172`). About 120 lines;
  reserve the proto field. **VERIFIED** by grep (a `.into()` caller cannot be grep-proven;
  the compiler confirms).
- Hand-maintained enum tables: 27 arms × 2 for `RollupKind`
  (`grid_fanout_command.rs:73-150`), 13 × 2 for `AggregationKind`
  (`type_conversions.rs:381-434`), in different files. A pinned `#[repr(i32)]` mapping or
  macro plus one exhaustive round-trip test each. **VERIFIED.**
- `AggregationFanoutCommand` (`:78-88, :150-154`) holds `strategy` + `partials` +
  `samples` + `raw` and builds a `PartialGroups::new(Sum)` it never uses for
  `Select`/`CountValues`. Mirror `GridSink`: `enum AggregationSink { Partials(..),
  Candidates(..) }`. **VERIFIED.**
- `promql.proto:249-257` says selecting operators "are not fused"; `GridSink::for_request`
  fuses them via `SteppedSelection`. Doc fix. **VERIFIED.**

**Effort:** small each.

### C7. Time budget is carried only by PromQL requests

Each of the five commands stores `matchers/limits/timeout`, overrides `get_timeout`,
implements an identical `time_budget`, and repeats the selector and budget encoding in
`generate_request`. The TS.* operations carry no budget at all (`time_budget` appears only
under `promql/` and in the registry). Moving `time_budget_ms` into `FanoutMessageHeader`
(`fanout_message.rs:51-72`; `required_features` exists for versioning) deletes the five
impls and gives all 17 operations queue-drop under load.

**Effort:** medium to large. **Benefit:** simplicity, and load shedding for TS.* fan-outs.
**PLAUSIBLE.**

## Part D — Functions and binary operators

### D1. Rollup driver consolidation

Covered under A6 (six drivers, deadline gap, no size threshold, `RollupImpl`,
`RollupKind` tables).

### D2. `label_replace` / `label_join` materialize interned label sets per sample

[`functions/labels.rs:132, :261`](../../src/promql/functions/labels.rs) call
`EvalLabels::set` per matched sample per step → `insert` (`exec/types.rs:391`) →
`make_owned` (`:621`) → `to_label_vec`, cloning every label of an `Interned` set into owned
`String`s, plus `key.to_string()` (`:469`) even when only the value changes.
`copy_label_from` (`:480-512`) already shows the interned-preserving splice
(`InternedString::new_pair` + one new `Arc<[_]>`); `insert` should do the same, and
`retain`'s and `remove`'s Interned arms (`:366-373, :411-438`) are that splice too.
`with_anchored_regex` (`labels.rs:78`) also allocates `format!("^(?s:{})$")` per call and
looks the cache key up twice.

**Effort:** small to medium. **Benefit:** fewer allocations on a per-sample-per-step path.
Duplication **VERIFIED**; gain **PLAUSIBLE** (label_replace bench).

### D3. Unreachable guards and a duplicate argument type

The parser enforces arity (`promql-parser` `check_call_arity`) and the evaluator
type-checks every argument against `call.func.arg_types` (`evaluator/mod.rs:610-638`)
before the single production entry `func.apply_call(...)` (`:689`). So every `fn apply(..)
-> Err(arity)` body is unreachable — about 44 across `functions/*.rs`, 16 of them via
`make_rollup_function!`/`basic_rollup_function!` (`rollup_functions.rs:447/471`, two macros
differing only in the exec function) — as are `expect_scalar`'s vector→scalar coercion
(`utils.rs:101-109`) and `RoundFunction::apply_args`' guard (`math_functions.rs:119-125`).
`PromQLArg` ([`functions/types.rs:46`](../../src/promql/functions/types.rs)) duplicates
`ExprResult` (`exec/types.rs:1191`): same four variants, a 1:1 `From`, duplicated
`value_type()` and `into_*` accessors, duplicated again as `expect_*` in `utils.rs`.
Default `PromQLFunction::apply`, delete the stubs, `type PromQLArg = ExprResult` or remove
it.

**Effort:** medium (mechanical). **Benefit:** simplicity. **VERIFIED.**

### D4. VictoriaMetrics `vmrange` conversion runs on every histogram call, untested

`classic_histograms` ([`histogram.rs:300`](../../src/promql/functions/histogram.rs))
unconditionally calls `vmrange_buckets_to_le` (`:95-247`), which re-pushes every
Prometheus-format series, uses `Rc<RefCell<EvalSample>>`, clones two `String`s and an `Rc`
per bucket, clones instead of `Rc::try_unwrap` (`:80`), has a no-op branch (`:196-202`) and
an `is_set` that relies on `||`/`&&` precedence (`:56`). `grep vmrange` hits only this
file: no test, fixture or doc. Early-return when no sample carries `vmrange`; rewrite
without `Rc<RefCell>` or feature-gate/delete.

**Effort:** medium. **Benefit:** `histogram_quantile` stops paying for a format it never
sees; the untested path is tested or gone. **VERIFIED.**

### D5. Duplicates in kernels and binops

- The on/ignoring predicate exists four times: `binop_vector_vector.rs:257 match_labels`,
  `:607 compute_binary_match_key`, `:660 result_metric`, and `exec/types.rs:285
  grouping_keeps` (different `None` semantics, documented). One `matching_keeps` for the
  three binop sites. **VERIFIED.**
- `eval_binop_vector_scalar` (`binop_vector_scalar.rs:6-34`) and
  `eval_binop_scalar_vector` (`binop_scalar_vector.rs:8-36`) are identical modulo operand
  order; `binop_range_scalar.rs:42 eval_range<F>` already solved this shape.
  **VERIFIED.**
- `rollup_quantile`/`rollup_mad` (`rollup_functions.rs:533/:561`) `to_vec()` per series per
  step; a thread-local scratch buffer. `rollup_ts_of_min/_max` (`:617/:636`) seed from
  index 0 then loop from index 0 and never replace a NaN seed, unlike `rollup_min`.
  `Clamp*/MaxOf/MinOf` (`math_functions.rs:138-277`) repeat ten lines of unpacking.
  `ensure_unique_labelsets` is a vector invariant living in `binops::labels` but used by
  the evaluator and `functions/labels.rs`. **VERIFIED.**

**Effort:** small each.

### D6. Error text and error kinds

`rollup_functions.rs:773-774` reports `"quantiles_over_time"`; `holt_winters.rs:61` names
`double_exponential_smoothing_value` for both `holt_winters` and
`double_exponential_smoothing`; `:79` calls the trend factor "smoothing factor", and both
say `0 < sf < 1` while the check is `0.0..=1.0`. Arity errors are `InternalError`
(`utils.rs:9-37`), type mismatches `ArgumentError` (`:71-96`) except `label_replace`'s;
user-facing join errors are `InternalError` (`binop_vector_vector.rs:339, 348, 573`).
Nothing downstream distinguishes the kinds (Display only). Apply one rule (user input →
`ArgumentError`) or drop the split.

**Effort:** trivial to small. **VERIFIED.**

## Part E — Optimizer and shared types

### E1. Const folding gaps; `simplify_expr` clones and compares per pass

[`const_folding.rs`](../../src/promql/optimizer/const_folding.rs): `handle_aggregation_expr`
(`:257-267`) folds only `param`, never the aggregated expression, and `handle_unary_expr`
(`:32-51`) folds only a literal or nested unary under the minus — so `sum(x * (1+2))` and
`-(1+2)` keep their constants, while `handle_rollup_expr` (`:269-276`) does fold a
subquery body. An omission, not a policy, and no test pins either case.
`handle_binop_internal` (`:53-80`) special-cases Binary children that `fold_constants`
already dispatches.

`simplify_expr` ([`optimize.rs:59-71`](../../src/promql/optimizer/optimize.rs)) runs up to
four passes of `expr.clone()` → `fold_constants` → `optimize_internal` → `==`, and both
walks rebuild every `Call`'s argument `Vec` (`optimize.rs:247-255`,
`const_folding.rs:218-223`). `optimize_expr`/`simplify_expr` return `ParseResult` but
cannot fail; callers `.unwrap()` (`derived_filters.rs:788`) or map an impossible error
(`promql_engine.rs:58`). One fused post-order pass with a `changed` flag, arguments mutated
in place, infallible signature.

**Effort:** small to medium. **Benefit:** complete folding; per-query microseconds;
simpler. **VERIFIED.**

### E2. Filter push-down re-walks subtrees at every binary node

[`pushdown.rs:98-105`](../../src/promql/optimizer/pushdown.rs) recurses both sides, then
`get_common_label_filters_with` (`:120`) walks the subtree, then
`push_down_binary_op_filters_in_place_with` (`:471`) walks it again; both call
`carries_labels` (`:64`) → `Expr::value_type()`, itself recursive. A chain `a + b + …` of
depth *d* is about d² walks, each with a d-deep type query. Bounded by
`MAX_QUERY_DEPTH = 500`, so not hot, but a bottom-up pass computing `(value_type,
common_filters)` once per node is one walk and simpler. Small items in the same file:
the same sort closure three times (`:441-457`); `filter_label_filters_on/ignoring` copy
into a `SmallVec` just to `contains` (`:649-663`);
`trim_filters_by_aggr_modifier`/`trim_filters_by_match_modifier` identical bar `None`
(`:284-292, :351-362`); `get_label_filters_set` hashes 2–6 matchers into a 128-bit set for
intersect/union (`:585-638`) where derived `PartialEq` with O(n²) is cheaper and drops
`HasFingerprint for Matcher`; `#[cfg(test)] pub` fns in the production file.
`optimizer/pushdown.rs` (filter push-down) and `exec/evaluator/pushdown.rs` (grid
push-down) are a clean split; only the shared filename misleads — rename one.

**Effort:** medium. **Benefit:** maintainability. Structure **VERIFIED**; cost
**PLAUSIBLE**.

### E3. Dead code

All verified by grep over `src/` and `benches/`:

- `EvaluationError::StorageError` and `::UnsupportedFunction` (`exec/types.rs:22, 26`) are
  never constructed.
- `impl HasFingerprint for PreloadKey` (`hashers.rs:341-359`) and `for [ProtoLabel]`
  (`:52-60`) have no callers.
- `From<&EvalStmt> for EvalContext` (`model.rs:180-197`) is unused; `Evaluator::evaluate`
  builds the struct by hand. `EvalContext::default()` (`:162-179`) reads the wall clock and
  is used only by tests → `#[cfg(test)]`.
- **String–string binary operators are unreachable from `parse_query`**: `promql-parser`
  0.11 rejects any String-typed binary operand (`ast.rs`, "binary expression must contain
  only scalar and instant vector types"). `handle_string_string`/`string_compare`
  (`const_folding.rs:105-150`) and `binops/binop_string_string.rs` are dead — and
  disagree (`string_compare` yields NaN for a non-bool false, `compare` yields 0).
- `PromqlResult` and `QueryResult` are two aliases of one type (`error.rs:82-83`), both
  widely used; the module doc describes `Error`/`Result` that do not exist.
- `STALE_NAN` is defined in `model.rs:23` and `src/common/rdb.rs:17`.
- `update_hasher_for_vector_selector`, `AtKey`, `OffsetKey` are `pub(in crate::promql)` but
  used only inside `hashers.rs`.

**Effort:** small. **VERIFIED.**

### E4. Overlapping sample types and duplicate conversions

`to_eval_samples` (`exec/evaluator/pushdown.rs:545-555`) and `pipeline.rs:272-281` are the
same `InstantSample<EvalLabels> → EvalSample` loop; `model.rs:229` has the reverse `From`
but not this one. `EvalSample` ≡ `InstantSample<EvalLabels>` + `drop_name`;
`EvalSamples` ≡ `RangeSample<EvalLabels>` + `drop_name` + `range_ms` + `range_end_ms`,
where the two range fields are grid-wide but stored per series (every construction site
writes one value for all; `pipeline.rs:313` writes `range_ms: 0 // overwritten`). Add the
missing `From` (small); consider moving range metadata onto `ExprResult::RangeVector`
(medium; readers at `rate.rs:93`, `rollups.rs:67, :250`).

**Effort:** small / medium. **VERIFIED** / **PLAUSIBLE.**

### E5. Repeated time arithmetic

Five ways to count or step a grid: `grid_step_count` (`time.rs:127`), `step_times`
(`:140`), `PreloadGrid::last_step` (`planner.rs:65`), `calc_points`
(`promql/utils.rs:36-48`), and `(a..=b).step_by(step as usize)` (`pipeline.rs:219`).
`compute_subquery_alignment` returns a 4-tuple of which `subquery_union` discards three.
`selector_bounds` (`time.rs:17-74`) re-implements the offset arithmetic of
`apply_time_modifiers_ms` (`:95-100`) because `@ start()/end()` must sweep rather than pin.
`apply_time_modifiers_ms(vs.at.as_ref(), vs.offset.as_ref(), ctx.query_start,
ctx.query_end, ts)` is spelled out at nine call sites and `options.lookback_delta =
Duration::from_millis(ms as u64)` at seven (`preload.rs:411, :659, :700`,
`pushdown.rs:189, :325, :426`, `pipeline.rs:271`). A method on `EvalContext`/`PreloadGrid`
and a `QueryOptions::with_lookback_ms` remove both.

**Effort:** small. **VERIFIED.**

### E6. Stale documentation and a per-series fan-out in `pipeline.rs`

The module doc and `QueryPathKind` docs describe "bucket lists" sorted newest-first that
no longer exist; `instant_plan_sorts_buckets_newest_first` asserts arithmetic only;
`QueryPlan.sample_start_ms` is unused on the instant path (`:260-285`).
`shape_subquery_results` (`:211-246`) fans each series out on rayon for typically few
series, where the pool handoff likely exceeds the work. Similarly `optimize_internal`'s
docstring lists rules that live in `const_folding` (`optimize.rs:201-208`).

**Effort:** small. Docs **VERIFIED**; rayon cost **PLAUSIBLE**.

### E7. Namespace flattening and small items

`promql/mod.rs:27-29` (`pub use exec::*; pub use model::*; pub use error::*`) and
`exec/mod.rs:12-13` globs make `EvaluationError` (defined in `exec/types.rs`) appear as
`crate::promql::EvaluationError`, with `QueryError` in `error.rs` and cross-`From`s in
both files. Move `EvaluationError`/`EvalResult` to `error.rs`; replace globs with explicit
lists. Minor: `dedupe_matchers` ignores `or_matchers` (`optimize.rs:278-308`); `Pi` handled
twice (`const_folding.rs:173, 242`); `get_single_scalar_arg` parses a `StringLiteral` the
parser never admits (`:156`); `pushdown_strategy` has an unreachable `_ => Reduce` arm
(`aggregations.rs:91`); `eval_limit_ratio`'s "todo: parallelize" (`:538`) contradicts the
measurement twenty lines above; `count_values_bucket_key` (`:366`) re-implements
`FingerprintHasher::write_u128`'s fold; `FingerprintHashSet` is std `HashSet` while
`FingerprintHashMap` is halfbrown (`hashers.rs:147-151`); `BitSet::get` says "Panics if idx
>= len" but only debug-asserts; `function_list.rs:97 #[allow(clippy::large_enum_variant)]`
on an enum of ZSTs; `date_functions.rs:89 sample_value` wraps `part.extract`.

**Effort:** small. **VERIFIED.**

## Worth preserving

- **Evaluation:** grid preloading with per-step index lookups; the subquery union preload
  keyed by content; bounded step batches (`STEP_MERGE_CHUNK_SIZE`); `GridCover` and
  `Keeps` budget accounting; `drops_metric_name` as one rule with deferred
  `cleanup_metric_labels`; `StepMerger` successor guessing, `StepGrid` packed grids and
  `SampleWindow` copy-on-write, each carrying its measurement in its doc; the
  no-fold-of-vector-typed-NaN discipline in `const_folding`; `LeafFilters` separating
  structural push-down rules from leaf facts.
- **Engine:** `run_local_batch` (shortest-remaining-first, `HOLD_BUDGET` and copy-bytes
  budget shared per hold, copy under the lock then decode on `MATERIALIZE_POOL`,
  `yield_now` between holds, panic isolation per read) and its comments recording the
  alternatives measured; the processor-thread invariants and `wait_for_result`'s
  `check_may_block`; `remaining_budget` capping fan-outs at the deadline; `GridSink` as the
  one fold for shard, coordinator and single node; `SampleBudget::charge(0)` as a read-only
  check and `decode_within_budget`'s exactness at the limit; the query cache's
  second-sighting admission; derived filters keyed by node address with
  `could_prune_a_leaf` asked before any regex compiles; `with_resolved_deadline` once at
  the top.
- **Wire:** one `SymbolTable` per response with identity interning and `EvalLabelResolver`
  producing `EvalLabels::Interned` in one allocation per series; raw series shipped as
  `samples_to_chunk_lossless` chunks kept packed so limits read `chunk.len()` and decode
  runs on the requester's pool; columnar presence-bitmap transport; the
  `ships_raw`/`promoted_raw` decision; corrupt-peer defenses (`wrong_payload`, zero-count
  partial rejection, `decode_pushdown_kind` refusing raw i32s).
- **Kernels:** `promql_function_list!` as the single source for the enum, hashify dispatch
  and Display; `FunctionCallContext` + `Deref<Target = EvalContext>` so the AST reaches only
  `absent*`; `RollupKind::implementation` sharing the same function pointers between local
  and pushed-down paths, pinned by `every_kind_ignores_samples_outside_the_window`; the
  binop join's `ArithOpContext`, `build_result_sample` moving many-side labels, and
  `copy_label_from`/`retain` keeping interned sets interned; `label_replace`'s per-thread
  bounded regex cache; `holt_winters`' fused loop pinned bit-for-bit against a two-pass
  oracle; `EvalLabels`' three variants sharing one `Eq`/`Hash`/fingerprint byte stream,
  with `compute_grouping_key ≡ compute_grouping_labels().fingerprint()` under test;
  `Deadline::checkpoint`'s thread-local work counter; explicit-stack `check_query_depth`.

## Test debt

- **Resource limits:** nothing catches `rate`/`deriv`/`holt_winters` skipping the deadline
  (A6); `query_workers.rs` tests only pool isolation, and the deadline-passed / cancelled /
  prepare-error replies in `submit_evaluation` rely on pytest (not checked).
- **Executor:** no unit test for `run_local_batch`'s hold sharing, `LocalRead::copy`'s
  `over_budget` path (`:1289-1291`) or `SelectorOutput::into_matrix`'s `WireMatrix` decode;
  `execute_selector_pipeline` has none either.
- **Cluster:** `instant_vector_selector_fanout_command.rs` and
  `range_vector_selector_fanout_command.rs` have no unit tests; the duplicate-series
  rejection is untested anywhere, as is grid/aggregation behaviour for a series answered by
  two shards (C2); no exhaustive `AggregationKind` wire round-trip; the batch/lock cadence
  of `for_each_snapshot_batch` is integration-only.
- **Kernels:** the `vmrange` path has zero tests (D4); `ts_of_min/max_over_time` have one
  fixture line each and no NaN case; three `label_join` tests are commented out
  (`functions_tests.rs:648-779`) while their `label_replace` twins are live; `deriv`,
  `predict_linear`, `irate`, `idelta`, `mad_over_time`, `quantile_over_time` have conformance
  fixtures only; `binop_scalar_vector.rs` has no tests of its own; `Bucket::is_set`
  precedence is untested.
- **Optimizer:** nothing pins that `sum(x * (1+2))` or `-(1+2)` fold (they do not); the
  `A + A → A*2` guard is tested only for `c2 + c2`; `or_matchers` appears in two of 201
  push-down cases; `hashers.rs` has no test that `m offset 5m` ≠ `m offset -5m` or
  `a=~"x"` ≠ `a="x"` as preload keys.
- **Test structure:** ~60 % of `counting_query_reader.rs` (1400 lines) is engine
  integration tests that count nothing (budget, reply size, max-series, cancellation) and
  its doc points at a plan removed in `d9d07430c`; eight hand-rolled delegating
  `QueryReader` wrappers (`NoPushdownReader`, `SpanLimitedReader`, `RefusesWideGrids`,
  `NoGridPushdown`, `TableReader`, `SelectorRecordingReader`, `PushdownReader`,
  `GridPushdownReader`) could be one configurable delegate (~250 lines);
  `MockQueryReaderBuilder` wraps `MockMultiBucketQueryReaderBuilder` wraps
  `MemorySeriesQuerier` — "bucket" is a dead concept, 28 uses; the `PromqlEngine` trait
  has a single impl; `memory_series_querier.rs:114-117` iterates a `Result` with
  `into_iter().flat_map`, so a postings error reads as "no series" in every unit test.

## Suggested implementation order and validation

Phase 0 — cleanups with no behaviour change, each an afternoon: E3 dead code; B3, B5, C4,
C5, C6, D5, D6, E5, E7; the `RollupWindow` field removal and `Option<f64>` kernel from
A6. Land them as separate commits so bisection stays cheap.

Phase 1 — small, measurable: A3 (grouping key in partial aggregation, keyed
`into_partials`, single-group fast path); the A6 driver merge with the deadline checkpoint
and size threshold; B2 (profile building off the lock); D2 (interned `insert`); E1 (fused
fold pass); B4; D3 and D4.

Phase 2 — medium: A1 (immutable prepared query), then A2 (shared raw spans) on top of it;
B1; C1; C2; C3.

Phase 3 — larger or riskier: A5 (one plan per grid, absorbing the walker consolidation),
A4 (evaluated subquery grids) as its own track, C7 (header time budget) only with a wire
versioning plan.

Validation:

- Reader reuse: the existing
  [`CountingQueryReader`](../../src/promql/engine/counting_query_reader.rs), including
  multiple aggregations over one selector and multiple rollups over one span (A2), and
  profile reads for B2.
- Semantics: the PromQL conformance suite (`cargo test --features enable-system-alloc --
  promql_tests`), the preload/fallback and grid-fanout tests, and `pushdown_tests.rs`.
  Preserve time modifiers, sparse results, pending metric-name drops, sample budgets,
  deadlines and fallback behaviour. Add the deadline test for `rate` before merging the
  drivers.
- Performance: [`benches/promql_engine.rs`](../../benches/promql_engine.rs) —
  `range_query` for rollups and label functions, `group_samples` for A3, `subquery_twins`
  for A4, `join_query`/`churn_join` for binops, `phase0_baseline` for the pushable-rollup
  and `sum by` shapes. Small cases are dominated by pool handoff when driven from the
  Criterion thread, so A/B anything under ~100 µs in-worker (see
  `promql-bench-measures-pool-handoff`). B2 and C2 are latency items: measure with
  `tools/latency_report.sh` (PING p99 under a concurrent range query) rather than CPU per
  query, and for cluster items run the `*_cme.py` integration suite.
- Compatibility: C5 changes a shard's reply to a malformed request from empty to an
  error; confirm no coordinator path relies on the empty reply.

This assessment is based on static inspection. No implementation changes or benchmark
runs were performed as part of the review; performance benefits remain hypotheses until
measured.
