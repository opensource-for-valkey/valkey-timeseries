# `time_gt_over_time` — implementation plan

**Status:** proposed 2026-09-30; revised 2026-10-07 to replace window locality with a bounded
lookbehind (D1, D9–D11, §5.4). Not started. Decisions D1–D11 (§4) need sign-off before phase 1.
**Scope:** a new experimental PromQL rollup in the PromQL engine (`src/promql/`), with
cluster push-down. No `TS.*` command surface and no compat-suite entries.
**Background:** the research notes and review in
[promql-extension-functions.md](promql-extension-functions.md) (sections "time_above_over_time",
"Sketch out a possible rust implementation of time_gt_over_time", and "Review: implementing
`time_gt_over_time` in this engine"). This plan supersedes the Rust sketch there: that sketch
assumes a sample before the window without saying how either read path would load it. D1 and §5.4
supply that.
**Precedent:** [state-duration-over-time-requirements.md](state-duration-over-time-requirements.md),
whose integration points this plan follows almost one for one, plus a lookbehind mechanism
(§5.4) that no existing rollup needs.

## 1. What it answers

"For how many seconds of this window was the series above `threshold`?" — measured in wall-clock
time, not in samples. `count_over_time(x > θ)` style sample counting depends on the scrape
schedule; this does not.

```
time_gt_over_time(v range-vector, threshold scalar [, max_gap string])
```

- `time_gt_over_time(node_cpu_utilization_ratio[30m], 0.9)` — seconds of the last 30 minutes with
  CPU above 90 %, each sample held for at most the query's `lookback_delta` (default 5m).
- `time_gt_over_time(queue_depth[1h], 10000, "2m")` — the same, holding each sample for at most 2
  minutes.

Output is seconds, as a float. `__name__` is dropped automatically: `drops_metric_name`
([`evaluator.rs`](../../src/promql/exec/evaluator.rs)) drops it for every function with a matrix
argument that is not in `NAME_PRESERVING_ROLLUPS`, so nothing is needed for that.

## 2. Reduction semantics (the contract)

Let the window be `(W_s, W_e]` — the engine's window bounds (`window_range` in
[`rollups.rs`](../../src/promql/functions/rollups.rs)); `W_e` already includes any `@`/`offset`
adjustment. Let `s_1 … s_n` be the window's samples in ascending timestamp order (the storage
invariant), `θ` the threshold and `G` the resolved `max_gap` (D4).

The **predecessor** `s_0 = (t_0, v_0)` is the last sample in `(W_s − L, W_s]`, if there is one,
where `L` is the lookbehind (D1, D9). A sample exactly at `W_s` is outside the left-open window, so
it is the predecessor.

Each sample holds its value (last observation carried forward) until the next sample, for at most
`G`, and never past the window end. The predecessor's hold is clipped to start at `W_s`:

```
hold_0 = [ W_s , min(t_1, t_0 + G, W_e) )          t_1 := W_e when the window is empty
hold_i = [ t_i , min(t_{i+1}, t_i + G, W_e) )      i = 1 … n,  t_{n+1} := W_e

time_gt_over_time = Σ_{i ∈ 0…n : v_i > θ} |hold_i|   (milliseconds, summed as i64)
                    ────────────────────────────
                                1000
```

An interval whose end is not after its start is empty (`|hold| = 0`). Computed in integer
milliseconds and divided once, like `state_duration_over_time`, so long windows stay exact.

| case | answer | rule |
|---|---|---|
| window empty, no predecessor hold reaching into it | no output sample | D10 |
| window empty, predecessor hold reaches into it (`t_0 + G > W_s`) | `hold_0` if `v_0 > θ`, else `0` | D10 |
| window has samples, none above `θ` (predecessor included) | `0` | a real answer, not "no answer" |
| time before the first in-window sample, `(W_s, t_1)` | covered by the predecessor's hold, up to `t_0 + G` | D1 |
| predecessor older than `W_s − G` | contributes nothing; its hold ends before the window | why reading back `L ≥ G` is exact (D1) |
| predecessor exactly at `W_s` | holds from `W_s` | left-open window |
| gap longer than `G` between samples | counted only up to `t_i + G` | D4 |
| last sample | held to `min(t_n + G, W_e)` | the tail reaches the evaluation time, unlike `state_duration` (measured to the last sample) |
| sample exactly at `W_e` | contributes `0` (zero-length hold) | follows from the formula |
| duplicate timestamps | zero-length hold, no effect | falls out of the formula; storage is last-wins anyway |
| `NaN` sample (in window or predecessor) | never `> θ`, so contributes `0`; still ends the previous sample's hold | IEEE comparison; includes `STALE_NAN` ([`model.rs`](../../src/promql/model.rs)) |
| `θ = NaN` | argument error | D3 |
| `θ = ±Inf` | `+Inf` → always `0`; `-Inf` → every non-`NaN` hold counts | IEEE comparison |
| `max_gap` omitted, `lookback_delta > 0` | `G = lookback_delta` for the query | D4 |
| `max_gap` omitted, `lookback_delta = 0` | `G` = 2 × the window's lower-median sample spacing; 5m with fewer than two distinct timestamps | D4 |
| `max_gap` ≥ range | effectively unbounded within the window | follows from the formula |
| `max_gap` `"0s"`, negative, no suffix, unparsable | argument error | D5 |

Two caveats for the user-facing docs:

- **A series can answer at a step whose window holds no samples** (D10), when a sample just
  before the window is still being held. `count(time_gt_over_time(…))` counts such series.
- **`0` does not distinguish "below θ" from "no coverage".** A series whose only sample sits at the
  very end of the window, with no predecessor within `G`, answers `0`. Alert rules should pair
  this with a coverage check (the follow-up in §8) or with `count_over_time`.

### Worked example (the fixture's first case)

Threshold `0.9`, window `[10m]` evaluated at `10m`, so `(0, 600 s]`. The sample at `0` is the
predecessor.

| t | value | hold with `G = 5m` (default) | above? | counted |
|---|---|---|---|---|
| 0 | 0.20 | predecessor, `[0, 60)` | no | 0 |
| 60 s | 0.95 | [60, 120) | yes | 60 |
| 120 s | 0.92 | [120, 420) (capped by `G`) | yes | 300 |
| 540 s | 0.10 | [540, 600) | no | 0 |

Answer `360`. With `"10m"` the second hold is `[120, 540)` and the answer is `480`; with `"1m"` it
is `[120, 180)` and the answer is `120`.

**Left edge.** A series scraped every minute and always above `θ` answers `600` for `[10m]`
evaluated on a scrape boundary: the sample at `W_s` holds `[W_s, W_s + 60 s)`. Without the
predecessor it would answer `540`. That undercount is what D1 removes.

## 3. Surface

```rust
Function::new(
    "time_gt_over_time",
    vec![ValueType::Matrix, ValueType::Scalar, ValueType::String],
    1,                        // max_gap optional: 2..=3 args
    ValueType::Vector,
    /* experimental */ true,  // D7
)
```

`threshold` may be any scalar expression on the local path (evaluated per step, like
`quantile_over_time`'s φ). Push-down takes it only when it can be shipped: a number literal on the
grid path (`pushable_rollup`), any scalar on the instant path (`rollup_arguments`). Otherwise the
call stays local. That is existing machinery; nothing new is needed.

## 4. Decisions requiring sign-off

- **D1 — one predecessor sample, found within a bounded lookbehind.** The answer depends on the
  window's samples plus the predecessor: the last sample in `(W_s − L, W_s]`, whose hold is clipped
  to start at `W_s` (§2). `L` is the kind's *lookbehind*. A new `RollupKind::lookbehind_ms` (§5.4)
  returns `0` for every existing kind and `G` for this one (D9 covers the estimate case).

  **Reading back `G` is exact, not an approximation.** A predecessor at `t_0 ≤ W_s − G` holds until
  at most `t_0 + G ≤ W_s`, so no older sample can contribute. That removes the left-edge undercount
  of window-local semantics (`540` instead of `600` in §2's example).

  **It keeps the function a `RollupKind`.** A series and its predecessor live on the same shard,
  so the locality rule that defines the enum still holds. What changes is the invariant
  `every_kind_ignores_samples_outside_the_window` pins: from "nothing outside `(W_s, W_e]`" to
  "nothing outside `(W_s − lookbehind, W_e]`" (§6.2). The shard derives `L` from fields the
  request already carries (kind, `duration_ms`, `range_ms`), so there is no new wire field.

  Rejected:
  - *Window locality* (this plan's D1 before 2026-10-07). It is the simplest, but undercounts the
    left edge by up to one sample interval at every step. That is the case users check first.
  - *A boolean `needs_predecessor()`.* The reader has no "previous sample" seek, so the evaluator
    would still have to choose how far back to read. The kind knows the right bound; a constant
    does not.
  - *Seeking the previous sample in storage.* There is no reader API for it, it is unbounded, and
    it fits neither `GridRequest::fetch_bounds` nor the preloaded spans.
  - *Shipping window-local first and adding the lookbehind later.* Answers would change under
    users of a released function. Nothing is released between this plan's phases, so there is no
    reason to stage it. The reducer still takes `Option<Sample>` for the predecessor from phase 1.
- **D2 — strict `>`, one operator in v1.** Ship `time_gt_over_time` only. The reducer takes a
  `Comparison` from the start (§5.3), so `ge`/`lt`/`le`/`eq`/`ne` later each cost a registry entry,
  a `RollupKind` and a proto value. They inherit the lookbehind unchanged. `eq`/`ne` would compare
  exactly (no tolerance, like `changes()`/`resets()`).
- **D3 — `θ = NaN` is an argument error.** Every answer would be `0`. That is the same footgun
  `state_duration_over_time` rejects (its D2), and rejecting keeps the two consistent.
- **D4 — `max_gap` default: `lookback_delta`, else estimated from the window.** Resolution
  order:
  1. an explicit `max_gap` argument;
  2. otherwise the query's `lookback_delta`, when it is `> 0`;
  3. otherwise **2 × the lower median of the positive gaps between consecutive samples in the
     window** (`estimate_max_gap`, §5.3);
  4. with fewer than two distinct timestamps in the window, a constant **5m** (the engine's
     `DEFAULT_LOOKBACK_DELTA`, [`model.rs`](../../src/promql/model.rs)).

  **Why `lookback_delta` first:** it is documented, configurable per query (`LOOKBACK_DELTA`),
  already bounds how long an instant selector trusts a sample, and is available as
  `ctx.lookback_delta_ms`.

  **Why a fallback is needed:** a `lookback_delta` of `0` reaches the evaluator. The config
  minimum is `0` ([`config.rs`](../../src/config.rs)). `TS.QUERYRANGE`'s `normalize_lookback`
  ([`command_parser.rs`](../../src/commands/command_parser.rs)) replaces an explicit
  `LOOKBACK_DELTA 0` with the config value, which can itself be `0`. `TS.QUERY` passes an
  explicit `0` straight through (a separate issue, §8). A `0` usually means the operator turned
  off the fixed bound on purpose, so estimating the cadence from the data is the principled
  substitute; a constant would hide outages entirely for fast series (5m against a 10 s scrape).

  **Why these estimator choices:**
  - *Median, not mean:* the gap being detected (an outage) is exactly what inflates a mean. One
    10-minute hole in a 60 s series moves the mean a lot and the median not at all.
  - *Lower median:* with three samples 60 s and 600 s apart, the lower median is 60 (`G` = 120 s,
    the outage is detected); the upper median is 600 and would bridge it. The smaller `G` is the
    safer error.
  - *Factor 2:* one missed scrape (a 120 s gap at a 60 s cadence) stays covered and ordinary
    jitter is absorbed; two or more missed scrapes count only up to two intervals past the last
    sample. A factor of 1.5 would treat a single dropped scrape as an outage.
  - *Zero deltas excluded:* duplicate timestamps are not cadence.
  - *Window samples only, not the predecessor:* the gap from the predecessor to `t_1` is partly
    outside the window, so it is not a cadence observation of the window. Keeping the estimate a
    function of `s_1 … s_n` also keeps "two samples → the gap is always covered" true.

  **Shard and local path agree:** both compute the estimate with the same function from the same
  window, so it needs no coordination. That makes the wire rule (§5.5): the coordinator resolves
  steps 1–2 and ships the value. For steps 3–4 it ships `duration_ms` **absent**, meaning
  "estimate per window". A shard never consults its own config.

  **Consequences to document:**
  - In the estimated case, `G` varies by series and by step, so results can shift as samples
    enter and leave the window.
  - With exactly two samples in the window, the single gap is always covered: one gap cannot be
    told apart from the cadence.
  - For event-driven series, sample spacing is not a scrape interval and the estimate means
    little; such queries should pass an explicit `max_gap`.
  - The estimate costs a scratch buffer and a quickselect per window (the class of
    `quantile_over_time`), only in the fallback case.

  **Rejected:** always falling back to a constant 5m (simple, but blind to cadence, as above);
  falling back to `step` (a display choice, unrelated to data cadence, and `0` for instant
  queries).
- **D5 — `max_gap` parsing.** A string literal parsed by the module's duration parser, with the
  rules `state_duration_over_time`'s `unit` uses (its D8): suffix required (a bare `"60"` would be
  60 ms), `> 0`, parse failures are `EvaluationError::ArgumentError` naming the function and
  argument. The string-argument precedent is already set, so the research doc's objection to
  string arguments no longer applies.
- **D6 — output unit is seconds, with no `unit` argument.** Rejected: an optional `unit` like
  `state_duration`'s. Two optional string arguments would be ambiguous by position, and
  `time_gt_over_time(x, θ) / 60` does the job.
- **D7 — experimental.** Gated by `ts-promql-enable-experimental-functions`, like the state
  functions. Flipping it later is a one-line registration change.
- **D8 — the wire's duration field (§5.5).** Rename `GridRollup.unit_ms` to `duration_ms`,
  meaning "the kind's duration argument": `state_duration`'s unit, `time_gt`'s max gap. Rejected
  alternative: a new `max_gap_ms = 5` field. That would force `pushable_rollup`/`rollup_arguments`
  to route the string by kind, and the two fields would never both be set.
- **D9 — the lookbehind in the estimate case is `max(2 × range, 5m)`.** When `G` is resolved
  (D4 steps 1–2), `L = G`. When it is estimated (steps 3–4), `G` is known only after the window is
  read, so `L` must bound every `G` the window can produce: each in-window gap is shorter than the
  range, so step 3 gives `G < 2 × range`, and step 4 gives `5m`. With that bound D1 stays exact in
  every mode.

  **Cost:** each window reads up to three times its range, but only when `lookback_delta = 0` and
  no `max_gap` is given, which is an unusual configuration. On the grid and preloaded-span paths
  the span grows by `L` once, not per step.

  Rejected: `L = 5m` (silently drops the predecessor of any series slower than a 2.5m cadence);
  window locality in the estimate case only (the left edge would then depend on a config value).
- **D10 — a step answers when its window has samples or the predecessor's hold reaches into it.**
  Otherwise the series is absent at that step, as today. The engine-wide "empty window → absent"
  rule would make a series covered for 4½ minutes by its predecessor answer nothing, then jump to
  the full 4½ minutes once one sample lands at `W_e`. It also matches how an instant selector
  treats a series as present within its lookback. The reducer makes the call (it returns `None`),
  so the generic empty-window short-circuits must not run ahead of it (§5.4).

  Rejected: keeping the engine-wide rule for this kind.
- **D11 — subquery arguments get the lookbehind too.** `time_gt_over_time(expr[1h:1m], θ)`
  evaluates the subquery from `W_s − L` instead of `W_s`: about `L / step` extra inner steps per
  outer step (five for a 5m gap at `1m`), shared through the union preload. Otherwise the selector
  form and its subquery equivalent would answer differently at the left edge.

  Rejected: no predecessor for subqueries (inconsistent, as above); refusing subquery arguments
  (a common shape, e.g. over `rate(…)[1h:1m]`).

## 5. Integration points

### 5.1 Parser registration

Add `time_threshold::parser_function()` to the `register_extra_functions` list in
`register_parser_functions()` ([`functions/mod.rs`](../../src/promql/functions/mod.rs)), next to
the two state functions. Tests that parse the function already get registration from the promqltest
harness and `parse_and_evaluate`.

### 5.2 Function registry

Add `(TimeGtOverTime, "time_gt_over_time", TimeGtOverTimeFunction)` to `promql_function_list!` in
[`function_list.rs`](../../src/promql/functions/function_list.rs).

### 5.3 Implementation (new file `src/promql/functions/time_threshold.rs`)

Modeled on [`state.rs`](../../src/promql/functions/state.rs).

- `Comparison { Gt }` (more variants per D2 later), with `matches(value, threshold) -> bool`.
- The pure reducer, shared by the local path and shards:

  ```rust
  /// Milliseconds of `(window_start, window_end]` during which the held value
  /// compares true against `threshold`, or `None` when the series is absent at
  /// this step (D10). `window` holds only that window's samples; `predecessor`
  /// is the last sample at or before `window_start` within the lookbehind.
  fn time_compare_ms(
      predecessor: Option<Sample>,
      window: &[Sample],
      window_start: Timestamp,
      window_end: Timestamp,
      op: Comparison,
      threshold: f64,
      max_gap_ms: i64,
  ) -> Option<i64>
  ```

  One forward pass over adjacent pairs, `O(n)` time and `O(1)` state. The predecessor's hold is
  clipped to start at `window_start`, and the tail uses `window_end` as the next timestamp. Use
  `Timestamp`/`i64` throughout. There is no out-of-order branch: windows arrive sorted.
- The D4 resolution, split so the part that needs the query stays on the coordinator and the part
  that needs the window runs wherever the window is reduced:

  ```rust
  /// Steps 1–2 of D4: the gap a query fixes, or `None` for "estimate per
  /// window" (steps 3–4). What goes on the wire as `duration_ms`.
  fn resolve_max_gap(explicit_ms: Option<i64>, lookback_delta_ms: i64) -> Option<i64> {
      explicit_ms.or((lookback_delta_ms > 0).then_some(lookback_delta_ms))
  }

  /// Step 3 of D4: twice the window's typical sample spacing, or `None` with
  /// fewer than two distinct timestamps. Reads only the window (not the
  /// predecessor), so a shard and the local path agree.
  fn estimate_max_gap(window: &[Sample]) -> Option<i64> {
      let mut deltas: Vec<i64> = window
          .windows(2)
          .map(|w| w[1].timestamp - w[0].timestamp)
          .filter(|&d| d > 0) // duplicate timestamps are not cadence
          .collect();
      if deltas.is_empty() {
          return None;
      }
      let lower_mid = (deltas.len() - 1) / 2;
      let (_, &mut median, _) = deltas.select_nth_unstable(lower_mid);
      Some(median.saturating_mul(2))
  }

  /// D1/D9: how far before the window start the predecessor is looked for.
  /// `max_gap_ms` is `resolve_max_gap`'s answer.
  fn lookbehind_ms(max_gap_ms: Option<i64>, range_ms: i64) -> i64 {
      max_gap_ms.unwrap_or_else(|| range_ms.saturating_mul(2).max(DEFAULT_LOOKBACK_DELTA))
  }
  ```

  Step 4 is `estimate_max_gap(window).unwrap_or(DEFAULT_LOOKBACK_DELTA)`. Reuse a thread-local
  buffer for `deltas` if the allocation shows up in profiles.
- `time_gt_window(predecessor, window, window_start, window_end, param: Option<f64>, duration_ms:
  Option<i64>) -> Option<f64>`: the `RollupImpl` entry point. `duration_ms: None` means estimate
  (D4 steps 3–4). Returns `None` when the series is absent at this step (D10) or `param` is
  missing (`check_params` refuses that first), else `Some(ms as f64 / 1000.0)`.
- `TimeGtOverTimeFunction: PromQLFunction`, following `StateDurationOverTimeFunction`:
  2..=3 args, `expect_range_vector`, a threshold via `expect_scalar` plus the `NaN` check (D3), an
  optional `expect_string` → `parse_duration_arg` (below), then
  `resolve_max_gap(explicit, ctx.lookback_delta_ms)`, then `time_gt_window` per series. That is the
  entry point a shard uses, so the estimate cannot differ between the paths.
- **Local evaluation needs the window bounds and the predecessor.** `eval_rollups_basic`
  ([`rollups.rs`](../../src/promql/functions/rollups.rs)) passes only the window's `&[Sample]` and
  drops empty windows. Add a sibling, `eval_rollups_lookbehind(ctx, series, |predecessor, window,
  start, end| …)`. It splits `series.values` (which then reach back `L`, §5.4) at
  `start = series.range_end_ms − series.range_ms`, takes the last sample at or before `start` as
  the predecessor, and leaves the empty-window decision to the closure (D10). Don't use
  `ctx.evaluation_ts`: `range_end_ms` is the one that carries `@`/`offset` (the value `rate.rs`
  already reads).
- Generalize `parse_unit` ([`state.rs`](../../src/promql/functions/state.rs)): its error message
  hard-codes `state_duration_over_time: invalid unit`. Move it to `parse_duration_arg(func, arg,
  s)` in [`utils.rs`](../../src/promql/functions/utils.rs) and have `parse_unit` call it. The
  push-down code calls it only for `.ok()`, so its behavior is unchanged.
- Declare the module in [`functions/mod.rs`](../../src/promql/functions/mod.rs).

### 5.4 Lookbehind (D1, D9–D11)

A general mechanism: one method on the kind, and every read path that serves a rollup widens by
it. No existing kind uses it, so every existing answer must stay unchanged; §6.2 checks that.

1. **The source of truth:**
   `RollupKind::lookbehind_ms(self, duration_ms: Option<i64>, range_ms: i64) -> i64` in
   [`rollup_functions.rs`](../../src/promql/functions/rollup_functions.rs). It returns `0` for every
   kind except `TimeGtOverTime`, which delegates to `time_threshold::lookbehind_ms`. It takes the
   *resolved* `duration_ms` (what the wire carries), so coordinator and shard compute the same `L`
   without `lookback_delta`.
2. **A call-level helper**, `call_lookbehind_ms(call, lookback_delta_ms) -> i64`, next to
   `pushable_rollup` in [`pushdown.rs`](../../src/promql/exec/evaluator/pushdown.rs). It finds
   the kind by name, parses the string argument with `parse_duration_arg(…).ok()`, runs
   `resolve_max_gap`, and takes `range_ms` from the matrix or subquery argument. A `max_gap` that
   does not parse gives `0`, so evaluating the function reports the error. Share the
   argument walk with `pushable_rollup` rather than duplicating it. In phase 2, before the kind
   exists, it matches the function name directly; phase 3 reroutes it through `RollupKind`.
3. **Shards, single-node grids and instant push-down:** [`GridRequest::backward_ms`](../../src/promql/engine/query_reader.rs)
   becomes `rollup.range_ms + rollup.kind.lookbehind_ms(rollup.duration_ms, rollup.range_ms)`.
   Every `query_grid` fetch is sized by `fetch_bounds`, including the step-0 request that
   `rollup_arguments` builds, so this one change covers all of them.
4. **Local per-step read:** `evaluate_function_args`
   ([`evaluator.rs`](../../src/promql/exec/evaluator.rs)) evaluates the matrix argument
   without knowing its consumer. For a call with `L > 0`, evaluate a `MatrixSelector` argument
   (parens stripped) through `evaluate_matrix_selector_with_lookbehind(ms, ctx, preload_eligible,
   L)`, and have `evaluate_matrix_selector` delegate with `0`. The live read becomes
   `QueryPlan::for_matrix(adjusted_eval_ts, range_ms + L)`. The pipeline derives
   `EvalSamples.range_ms` from the plan bounds ([`pipeline.rs`](../../src/promql/exec/pipeline.rs)),
   so it must be set back to the window's `range_ms`: `range_ms` stays "the window", and `values`
   may reach back `L` before it.
5. **Preloaded raw spans:** `preload_matrices`/`preload_matrix`
   ([`preload.rs`](../../src/promql/exec/evaluator/preload.rs)) start the span at
   `first − range − L + 1`, computing `L` with `grid.lookback_delta_ms`. `MatrixPreloadKey`
   ([`hashers.rs`](../../src/promql/hashers.rs)) gains `lookbehind_ms`, and the per-step slice in
   `evaluate_matrix_selector` uses `window_range(…, range_ms + L)`. **The key is a correctness
   requirement, not hygiene:** without it, a span preloaded for `sum_over_time(x[10m])` serves
   `time_gt_over_time(x[10m], …)` too short, and the answer is silently wrong rather than slow.
6. **Subqueries (D11):** `evaluate_subquery` ([`subquery.rs`](../../src/promql/exec/evaluator/subquery.rs))
   takes `L` the same way. Its `subquery_start_ms` moves back by `L` on both the vector-selector
   fast path and the per-step path. `preload_subqueries` widens the union to
   `earliest_end − range − L`, and `subquery_key` includes `L`. The returned `EvalSamples.range_ms`
   stays the subquery's range.
7. **Reducing a window:** a new `RollupImpl` arm (§5.5) slices `(W_s − L, W_e]` per window in a
   sibling of `over_windows`, splits off the predecessor, and does **not** skip windows whose
   in-window part is empty (D10). The existing `RollupWindow::prev_value` is not reused: it is
   bounded by `step` (the VictoriaMetrics rule), not by `L`.
8. **Documentation of the invariant:** update the `RollupKind` doc comment (membership is still
   locality) and the `RollupWindow` doc in [`types.rs`](../../src/promql/functions/types.rs): a
   kind may depend on samples in `(W_s − lookbehind_ms, W_e]` and nothing else.

The extra samples are charged to `ts-promql-max-samples-per-query` like any other read; no budget
special case.

### 5.5 Push-down

The function meets the `RollupKind` membership rule (D1), so it goes through the same three paths as
the other rollups: unfused, fused with an outer reduction, and over a range query's step grid.
Every fusable reduction (`sum`, `avg`, `min`, `max`, `count`, …) of seconds is well defined, with no
unit subtleties.

1. **`RollupKind::TimeGtOverTime`** in
   [`rollup_functions.rs`](../../src/promql/functions/rollup_functions.rs): `from_function_name`,
   `function_name`, `all()` (both lists — the exhaustive `match` enforces it), `implementation`,
   `lookbehind_ms` (§5.4), and `test_params` → `(Some(0.5), Some(60_000))`.
2. **New `RollupImpl` arm**, since no existing arm passes a predecessor, the window bounds and
   parameters:
   `WithPredecessor(fn(Option<Sample>, &[Sample], Timestamp, Timestamp, Option<f64>, Option<i64>) -> Option<f64>)`,
   evaluated per §5.4 item 7.
3. **`check_params(param, duration_ms)`**: `TimeGtOverTime` needs a non-`NaN` param; its
   `duration_ms` is optional but positive when present (absent = estimate per window, D4).
   `StateDurationOverTime` keeps "optional, positive" with absent meaning seconds. Every other
   kind still refuses a duration.
4. **Resolving the gap on the coordinator:** in `pushable_rollup` and `rollup_arguments`
   ([`pushdown.rs`](../../src/promql/exec/evaluator/pushdown.rs)), when the kind is
   `TimeGtOverTime`, set `duration_ms` to `resolve_max_gap(explicit, lookback_delta_ms)`: the
   explicit gap if given, else `lookback_delta` if positive, else absent. **Absent is a real
   value on the wire, not "the shard's default":** the shard estimates from each window with
   `estimate_max_gap`, exactly as the local path does, and never reads its own config. It also
   sizes its fetch from the same `duration_ms` (§5.4 item 3), so the lookbehind needs no field of
   its own. `rollup_arguments` has `ctx`. **Verify in phase 3** that `pushable_rollup` (no `ctx`)
   can reach the same `lookback_delta` the local path sees. The grid request already carries
   `lookback_delta_ms`, so the value exists at preload time. If `pushable_rollup` cannot reach it,
   add it as a parameter rather than reading config. Because the preload key includes
   `duration_ms`, "estimate" and an explicit gap over the same selector are separate grids, as
   they must be.
5. **Rename per D8:** `GridRollup::unit_ms` → `duration_ms`
   ([`query_reader.rs`](../../src/promql/engine/query_reader.rs)), the preload-key hasher field
   ([`hashers.rs`](../../src/promql/hashers.rs)), `eval_windows`' parameter, and the proto field
   (`optional int64 duration_ms = 4;` — same number, so the wire format is unchanged). Update its
   doc comment to list both kinds.
6. **Wire:** `ROLLUP_KIND_TIME_GT_OVER_TIME = 27` in [`promql.proto`](../../proto/v1/promql.proto),
   both conversions in
   [`grid_fanout_command.rs`](../../src/promql/engine/fanout/grid_fanout_command.rs), then
   `VALKEY_TS_PROTO_REGEN=1 cargo build` and commit the regenerated file.

### 5.6 Config

No new parameter. The default gap comes from `lookback_delta` or the window estimate (D4), and the
gate from `ts-promql-enable-experimental-functions`.

### 5.7 Docs

- [`docs/topics/promql.md`](../../docs/topics/promql.md): the function, its signature, seconds,
  the predecessor rule, the `max_gap` default and parsing rules, both caveats from §2, the
  experimental flag.
- [`docs/overview.md`](../../docs/overview.md): the pushed-down function list.
- [promql-extension-functions.md](promql-extension-functions.md): point its review section at this
  plan.

## 6. Tests

1. **Reducer unit tests** (in `time_threshold.rs`): the §2 worked example with `G` = 1m / 5m / 10m;
   a hold crossing `W_e` clipped; a sample at `W_e` contributing 0; a gap longer than `G`; strict
   `>` at equality; a `NaN` sample ending the previous hold and contributing 0; `θ = ±Inf`;
   duplicate timestamps; a week of 1 s samples returning an exact integer millisecond total;
   `parse_duration_arg` rejections, with the message naming `time_gt_over_time` and `max_gap`
   (promqltest never compares `expect fail msg:` text).
   Predecessor cases: one above `θ` held into the window, clipped at `W_s`; one exactly at `W_s`;
   one at `W_s − G` or older contributing nothing; a predecessor hold cut short by `t_1`; an empty
   window with a reaching predecessor → `Some` (D10), and with a non-reaching one → `None`; a
   `NaN` predecessor; the steady-scrape left edge answering `600`.
   `resolve_max_gap`: explicit wins; `lookback_delta > 0` next; `0` → `None`.
   `lookbehind_ms`: resolved `G` → `G`; `None` → `max(2 × range, 5m)` for a short and a long range.
   `estimate_max_gap`: steady 60 s cadence → 120 s; 60 s cadence with one 10-minute hole → 120 s
   (median, not mean); samples 60 s and 600 s apart → 120 s (lower median, not upper); 55–65 s
   jitter → about 2 × 60 s; duplicate timestamps ignored; two samples → 2 × their gap; one sample or
   all-equal timestamps → `None`, so the reducer falls back to 5m. The predecessor never enters the
   estimate.
2. **Window invariance:** generalize `every_kind_ignores_samples_outside_the_window` to
   `every_kind_depends_only_on_its_lookbehind_span`. Comparing the whole series with
   `(W_s − kind.lookbehind_ms(…), W_e]` per window, it reduces to today's check for every kind with
   `0`. That test's series is strictly increasing, so also add
   `time_gt_ignores_samples_before_the_lookbehind`: samples older than `W_s − L`, above `θ`, added
   or removed, never change an answer. It is the analogue of
   `state_duration_ignores_runs_before_the_window`. Plus `lookbehind_is_zero_for_existing_kinds`,
   so a future kind opts in deliberately.
3. **PromQL conformance fixture** `src/promql/promqltest/testdata/time_gt.test`:
   - the worked example: `load 1m` / `cpu 0.20 0.95 0.92 _x6 0.10` (samples at 0, 1m, 2m, 9m),
     evaluated at `10m` with no gap, `"1m"` and `"10m"` (360 / 120 / 480);
   - the steady-scrape left-edge case (`600`) so the predecessor rule is pinned;
   - a predecessor above `θ` whose hold covers the start of an otherwise below-`θ` window;
   - a window with samples but none above → `0`; an empty window whose predecessor still holds →
     present (D10); an empty window with no reaching predecessor → series absent;
   - `sum`/`count`/`avg` over several series;
   - `eval range` proving per-step independence, including the first step (whose predecessor sits
     before the query start);
   - `@` and `offset` (the tail must stop at the shifted window end, and the predecessor is looked
     for before the shifted start);
   - the same query over a selector and over an equivalent subquery (`x[10m:1m]` on a 1m series)
     agreeing (D11);
   - `LOOKBACK_DELTA` changing the default gap, and `LOOKBACK_DELTA 0` switching to the
     estimate (a 60 s series with a 10-minute outage counting 2 minutes past the last sample,
     not 5) — only if the DSL can set the lookback; otherwise Rust evaluator tests;
   - arity (1 and 4 args), a non-scalar threshold, a non-string gap, `NaN` threshold, and bad
     gaps (`"0s"`, `"-1m"`, `"60"`, `"abc"`, `""`), all `expect fail`.
4. **Preload key:** a range query over `sum_over_time(x[10m]) + time_gt_over_time(x[10m], 0.5)`
   equals the same query evaluated per step. Mutation check: dropping `lookbehind_ms` from
   `MatrixPreloadKey` must fail it (§5.4 item 5).
5. **Push-down parity** (automatic via `RollupKind::all()`): local vs pushed for instant, grid and
   modifiers, run both with a resolved gap and with the gap absent (estimate); wire round trip,
   including absent `duration_ms` surviving as absent; `fetch_bounds` reaching back `L` for this
   kind and not for others; shard refusals (a non-positive gap, a `NaN` or missing param, a gap on
   a kind that takes none); fused parity.
6. **Cluster:** add cases to
   [`tests/test_ts_query_rollup_pushdown_cme.py`](../../tests/test_ts_query_rollup_pushdown_cme.py)
   covering exact values and push-down on/off equivalence, including one with an explicit gap,
   one relying on the default, one with `LOOKBACK_DELTA 0` (the estimate) on a series with an
   outage, and one whose first step's answer depends on a sample before the query range. Mutation
   checks: a shard that ignores `duration_ms`, substitutes its own `lookback_delta` for an absent
   one, or fetches without the lookbehind must fail at least one case each.
7. **No compat testing:** this is a PromQL-engine surface.

## 7. Phases

Each phase ends green on `cargo test` and is committed on its own.

1. **Reducer and parsing.** `time_threshold.rs` with `Comparison`, the predecessor-aware
   `time_compare_ms`, `resolve_max_gap`, `estimate_max_gap`, `lookbehind_ms`;
   `parse_duration_arg` extracted; unit tests (§6.1).
2. **Local function with lookbehind.** Parser registration, registry entry,
   `TimeGtOverTimeFunction`, `eval_rollups_lookbehind`, and the local read sites of §5.4 (items 2,
   4, 5, 6, keyed by function name for now). Add the conformance fixture (§6.3) and the
   preload-key test (§6.4). Push-down is still off for this name, so the local path is proven
   alone first.
3. **Push-down.** `RollupKind` with `lookbehind_ms`, `backward_ms`, the `RollupImpl` arm,
   `check_params`, the D8 rename, coordinator default resolution, the call helper rerouted through
   the kind, proto + regen, the invariance and parity tests (§6.2, §6.5), and the doc comments
   (§5.4 item 8).
4. **Cluster tests and docs.** §6.6, §5.7. Run `SERVER_VERSION=unstable ./build.sh` with
   `TEST_PATTERN="rollup_pushdown"`.

## 8. Follow-ups / non-goals

- **Coverage companion (strongly recommended next).** Without it, `0` is ambiguous (§2). First
  check whether MetricsQL's `duration_over_time(m[d], max_interval)` means "seconds the series was
  present with gaps bounded by `max_interval`" (unverified). If it does, implement it under that
  name with the same hold rule and lookbehind; the reducer is `time_compare_ms` with an
  always-true comparison. `share_time_gt_over_time` is then `time_gt / duration`.
- **Operator siblings** `time_ge/lt/le/eq/ne_over_time` (D2).
- **Other users of the lookbehind.** `state_duration_over_time` has the same left-edge shape
  (`state_duration_ignores_runs_before_the_window` pins today's undercount). Opting it in changes
  answers of an existing experimental function, so it needs its own decision. Never `rate`,
  `increase`, `delta`, `changes` or `resets`: their windowed behavior is fixed by Prometheus
  compatibility.
- **Linear-interpolation variant** (`time_gt_linear_over_time`): different edge cases (crossings,
  `±Inf`, equal timestamps); a separate plan.
- Duration-literal (unquoted `2m`) or scalar forms of `max_gap`.
- **`TS.QUERY` does not normalize `LOOKBACK_DELTA 0` (separate fix, independent of this
  function).** `parse_query_range_command_args` runs the lookback through `normalize_lookback`
  ([`command_parser.rs`](../../src/commands/command_parser.rs)), which replaces an explicit `0`
  with the configured `ts-promql-lookback-delta`. `parse_query_command_args` does not: an
  explicit `0` goes straight into the `EvalStmt`. So `TS.QUERY … LOOKBACK_DELTA 0` presumably
  evaluates instant selectors over an empty `(t, t]` lookback (confirm when fixing), while the same
  option on `TS.QUERYRANGE` means "use the default". Prometheus's query API treats a zero lookback as "use the default".
  Fix: apply the same zero-means-config rule on the instant path (sharing a helper with
  `normalize_lookback`), with a test for each command. Check for any caller that relies on the
  current behavior first. Even after the fix, the config value can still be `0`, so D4's estimate
  is still needed.
