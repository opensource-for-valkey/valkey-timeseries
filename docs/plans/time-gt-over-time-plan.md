# `time_gt_over_time` — implementation plan

**Status:** proposed 2026-09-30. Not started. Decisions D1–D8 (§4) need sign-off before phase 1.
**Scope:** a new experimental PromQL rollup in the PromQL engine (`src/promql/`), with
cluster push-down. No `TS.*` command surface and no compat-suite entries.
**Background:** the research notes and review in
[promql-extension-functions.md](promql-extension-functions.md) (sections "time_above_over_time",
"Sketch out a possible rust implementation of time_gt_over_time", and "Review: implementing
`time_gt_over_time` in this engine"). This plan supersedes the Rust sketch there: that sketch
assumes a sample before the window, which this engine's push-down contract rules out (D1).
**Precedent:** [state-duration-over-time-requirements.md](state-duration-over-time-requirements.md),
whose integration points this plan follows almost one for one.

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

Each sample holds its value (last observation carried forward) until the next sample, for at most
`G`, and never past the window end:

```
hold_i = [ t_i , min(t_{i+1}, t_i + G, W_e) )      with t_{n+1} := W_e

time_gt_over_time = Σ_{i : v_i > θ} |hold_i|   (milliseconds, summed as i64)
                    ─────────────────────────
                             1000
```

Computed in integer milliseconds and divided once, like `state_duration_over_time`, so long windows
stay exact.

| case | answer | rule |
|---|---|---|
| window empty | no output sample | engine-wide ([`rollups.rs`](../../src/promql/functions/rollups.rs)) |
| window has samples, none above `θ` | `0` | a real answer, not "no answer" |
| time before the first in-window sample, `(W_s, t_1)` | not counted | D1: window locality |
| gap longer than `G` between samples | counted only up to `t_i + G` | D4 |
| last sample | held to `min(t_n + G, W_e)` | the tail reaches the evaluation time, unlike `state_duration` (measured to the last sample) |
| sample exactly at `W_e` | contributes `0` (zero-length hold) | follows from the formula |
| duplicate timestamps | zero-length hold, no effect | falls out of the formula; storage is last-wins anyway |
| `NaN` sample | never `> θ`, so contributes `0`; still ends the previous sample's hold | IEEE comparison; includes `STALE_NAN` ([`model.rs`](../../src/promql/model.rs)) |
| `θ = NaN` | argument error | D3 |
| `θ = ±Inf` | `+Inf` → always `0`; `-Inf` → every non-`NaN` hold counts | IEEE comparison |
| `max_gap` omitted, `lookback_delta > 0` | `G = lookback_delta` for the query | D4 |
| `max_gap` omitted, `lookback_delta = 0` | `G` = 2 × the window's lower-median sample spacing; 5m with fewer than two distinct timestamps | D4 |
| `max_gap` ≥ range | effectively unbounded within the window | follows from the formula |
| `max_gap` `"0s"`, negative, no suffix, unparsable | argument error | D5 |

Two caveats for the user-facing docs:

- **The left edge is undercounted by up to one sample interval.** Nothing before the first
  in-window sample is known (D1). A series scraped every minute and always above `θ` answers
  `540`, not `600`, for `[10m]` evaluated on a scrape boundary: the sample at `W_s` is outside
  the left-open window, and the sample at `W_e` holds for zero time.
- **`0` does not distinguish "below θ" from "no coverage".** A series with one sample at the very
  end of the window answers `0`. Alert rules should pair this with a coverage check (the follow-up
  in §8) or with `count_over_time`.

### Worked example (the fixture's first case)

Threshold `0.9`, window `[10m]` evaluated at `10m`, so `(0, 600 s]`. The sample at `0` is outside
the window.

| t | value | hold with `G = 5m` (default) | above? | counted |
|---|---|---|---|---|
| 0 | 0.20 | outside window | — | — |
| 60 s | 0.95 | [60, 120) | yes | 60 |
| 120 s | 0.92 | [120, 420) (capped by `G`) | yes | 300 |
| 540 s | 0.10 | [540, 600) | no | 0 |

Answer `360`. With `"10m"` the second hold is `[120, 540)` and the answer is `480`; with `"1m"` it
is `[120, 180)` and the answer is `120`.

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

- **D1 — window locality (no predecessor sample).** The answer depends only on samples inside
  `(W_s, W_e]` and on `W_e`. That keeps the function a `RollupKind` that can be pushed down
  (`every_kind_ignores_samples_outside_the_window` in
  [`rollup_functions.rs`](../../src/promql/functions/rollup_functions.rs)), and matches
  `state_duration_over_time`. Cost: the left-edge undercount (§2). Rejected: reading the sample
  before the window. The local path loads exactly `(t − range, t]`, and the grid path fills
  `RollupWindow::prev_value` only for interior windows, so the two paths would disagree. Doing it
  properly means both paths load one extra sample, plus a second rollup class with its own
  invariant test — a separate plan if wanted.
- **D2 — strict `>`, one operator in v1.** Ship `time_gt_over_time` only. The reducer takes a
  `Comparison` from the start (§5.3), so `ge`/`lt`/`le`/`eq`/`ne` later each cost a registry entry,
  a `RollupKind` and a proto value. `eq`/`ne` would compare exactly (no tolerance, like
  `changes()`/`resets()`).
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

  **It keeps D1:** the estimate reads only the window's own samples, so a shard and the local path
  compute the same `G` with the same function. That makes the wire rule (§5.4): the coordinator
  resolves steps 1–2 and ships the value; for steps 3–4 it ships `duration_ms` **absent**, meaning
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
- **D8 — the wire's duration field (§5.4).** Rename `GridRollup.unit_ms` to `duration_ms`,
  meaning "the kind's duration argument": `state_duration`'s unit, `time_gt`'s max gap. Rejected
  alternative: a new `max_gap_ms = 5` field. That would force `pushable_rollup`/`rollup_arguments`
  to route the string by kind, and the two fields would never both be set.

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
  /// Milliseconds of `(window_end - range, window_end]` during which the
  /// held value compares true against `threshold`; `window` holds only that
  /// window's samples.
  fn time_compare_ms(
      window: &[Sample],
      window_end: Timestamp,
      op: Comparison,
      threshold: f64,
      max_gap_ms: i64,
  ) -> i64
  ```

  One forward pass over adjacent pairs, `O(n)` time and `O(1)` state; the tail uses `window_end`
  as the next timestamp. Use `Timestamp`/`i64` throughout. There is no out-of-order branch:
  windows arrive sorted.
- The D4 resolution, split so the part that needs the query stays on the coordinator and the part
  that needs the window runs wherever the window is reduced:

  ```rust
  /// Steps 1–2 of D4: the gap a query fixes, or `None` for "estimate per
  /// window" (steps 3–4). What goes on the wire as `duration_ms`.
  fn resolve_max_gap(explicit_ms: Option<i64>, lookback_delta_ms: i64) -> Option<i64> {
      explicit_ms.or((lookback_delta_ms > 0).then_some(lookback_delta_ms))
  }

  /// Step 3 of D4: twice the window's typical sample spacing, or `None` with
  /// fewer than two distinct timestamps. Reads only the window, so a shard and
  /// the local path agree.
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
  ```

  Step 4 is `estimate_max_gap(window).unwrap_or(DEFAULT_LOOKBACK_DELTA)`. Reuse a thread-local
  buffer for `deltas` if the allocation shows up in profiles.
- `time_gt_window(window, window_end, param: Option<f64>, duration_ms: Option<i64>) -> Option<f64>`:
  the `RollupImpl` entry point. `duration_ms: None` means estimate (D4 steps 3–4). Returns `None`
  only when `param` is missing (`check_params` refuses that first), else
  `Some(ms as f64 / 1000.0)`.
- `TimeGtOverTimeFunction: PromQLFunction`, following `StateDurationOverTimeFunction`:
  2..=3 args, `expect_range_vector`, a threshold via `expect_scalar` plus the `NaN` check (D3), an
  optional `expect_string` → `parse_duration_arg` (below), then
  `resolve_max_gap(explicit, ctx.lookback_delta_ms)`, then `time_gt_window` per series — the same
  entry point a shard uses, so the estimate cannot differ between the paths.
- **Local evaluation needs the window end.** `eval_rollups_basic`
  ([`rollups.rs`](../../src/promql/functions/rollups.rs)) passes only `&[Sample]`. Add a sibling,
  `eval_rollups_window_end(ctx, series, |window, end| …)`, that also hands the closure
  `series.range_end_ms` (the value `rate.rs` already reads). Don't use `ctx.evaluation_ts`:
  `range_end_ms` is the one that carries `@`/`offset`.
- Generalize `parse_unit` ([`state.rs`](../../src/promql/functions/state.rs)): its error message
  hard-codes `state_duration_over_time: invalid unit`. Move it to `parse_duration_arg(func, arg,
  s)` in [`utils.rs`](../../src/promql/functions/utils.rs) and have `parse_unit` call it. The
  push-down code calls it only for `.ok()`, so its behavior is unchanged.
- Declare the module in [`functions/mod.rs`](../../src/promql/functions/mod.rs).

### 5.4 Push-down

The function meets the `RollupKind` membership rule (D1), so it goes through the same three paths as
the other rollups: unfused, fused with an outer reduction, and over a range query's step grid.
Every fusable reduction (`sum`, `avg`, `min`, `max`, `count`, …) of seconds is well defined, with no
unit subtleties.

1. **`RollupKind::TimeGtOverTime`** in
   [`rollup_functions.rs`](../../src/promql/functions/rollup_functions.rs): `from_function_name`,
   `function_name`, `all()` (both lists — the exhaustive `match` enforces it), `implementation`,
   and `test_params` → `(Some(0.5), Some(60_000))`.
2. **New `RollupImpl` arm**, since no existing arm passes both the window end and parameters:
   `WindowEndWithParams(fn(&[Sample], Timestamp, Option<f64>, Option<i64>) -> Option<f64>)`,
   evaluated through `over_windows` with its `window_end`.
3. **`check_params(param, duration_ms)`**: `TimeGtOverTime` needs a non-`NaN` param; its
   `duration_ms` is optional but positive when present (absent = estimate per window, D4).
   `StateDurationOverTime` keeps "optional, positive" with absent meaning seconds. Every other
   kind still refuses a duration.
4. **Resolving the gap on the coordinator:** in `pushable_rollup` and `rollup_arguments`
   ([`pushdown.rs`](../../src/promql/exec/evaluator/pushdown.rs)), when the kind is
   `TimeGtOverTime`, set `duration_ms` to `resolve_max_gap(explicit, lookback_delta_ms)`: the
   explicit gap if given, else `lookback_delta` if positive, else absent. **Absent is a real
   value on the wire, not "the shard's default":** the shard estimates from each window with
   `estimate_max_gap`, exactly as the local path does, and never reads its own config.
   `rollup_arguments` has `ctx`. **Verify in phase 3** that `pushable_rollup` (no `ctx`) can
   reach the same `lookback_delta` the local path sees — the grid request already carries
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

### 5.5 Config

No new parameter. The default gap comes from `lookback_delta` or the window estimate (D4), and the
gate from
`ts-promql-enable-experimental-functions`.

### 5.6 Docs

- [`docs/topics/promql.md`](../../docs/topics/promql.md): the function, its signature, seconds,
  the `max_gap` default and parsing rules, both caveats from §2, the experimental flag.
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
   `resolve_max_gap`: explicit wins; `lookback_delta > 0` next; `0` → `None`.
   `estimate_max_gap`: steady 60 s cadence → 120 s; 60 s cadence with one 10-minute hole → 120 s
   (median, not mean); samples 60 s and 600 s apart → 120 s (lower median, not upper); 55–65 s
   jitter → about 2 × 60 s; duplicate timestamps ignored; two samples → 2 × their gap; one sample or
   all-equal timestamps → `None`, so the reducer falls back to 5m.
2. **Window invariance:** `RollupKind::all()` picks the kind up in
   `every_kind_ignores_samples_outside_the_window`. That test's series is strictly increasing, so
   also add `time_gt_ignores_holds_before_the_window`: a hold that starts before `W_s` must count
   nothing, with and without the neighbours in view. It is the analogue of
   `state_duration_ignores_runs_before_the_window`.
3. **PromQL conformance fixture** `src/promql/promqltest/testdata/time_gt.test`:
   - the worked example: `load 1m` / `cpu 0.20 0.95 0.92 _x6 0.10` (samples at 0, 1m, 2m, 9m),
     evaluated at `10m` with no gap, `"1m"` and `"10m"` (360 / 120 / 480);
   - the steady-scrape left-edge case (`540`, not `600`) so the caveat is pinned;
   - a window with samples but none above → `0`; an empty window → series absent;
   - `sum`/`count`/`avg` over several series;
   - `eval range` proving per-step independence;
   - `@` and `offset` (the tail must stop at the shifted window end);
   - `LOOKBACK_DELTA` changing the default gap, and `LOOKBACK_DELTA 0` switching to the
     estimate (a 60 s series with a 10-minute outage counting 2 minutes past the last sample,
     not 5) — only if the DSL can set the lookback; otherwise Rust evaluator tests;
   - arity (1 and 4 args), a non-scalar threshold, a non-string gap, `NaN` threshold, and bad
     gaps (`"0s"`, `"-1m"`, `"60"`, `"abc"`, `""`), all `expect fail`.
4. **Push-down parity** (automatic via `RollupKind::all()`): local vs pushed for instant, grid and
   modifiers, run both with a resolved gap and with the gap absent (estimate); wire round trip,
   including absent `duration_ms` surviving as absent; shard refusals (a non-positive gap, a
   `NaN` or missing param, a gap on a kind that takes none); fused parity.
5. **Cluster:** add cases to
   [`tests/test_ts_query_rollup_pushdown_cme.py`](../../tests/test_ts_query_rollup_pushdown_cme.py)
   covering exact values and push-down on/off equivalence, including one with an explicit gap and
   one relying on the default, and one with `LOOKBACK_DELTA 0` (the estimate) on a series with an
   outage. Mutation checks: a shard that ignores `duration_ms`, or substitutes its own
   `lookback_delta` for an absent one, must fail at least one case each.
6. **No compat testing:** this is a PromQL-engine surface.

## 7. Phases

Each phase ends green on `cargo test` and is committed on its own.

1. **Reducer and parsing.** `time_threshold.rs` with `Comparison`, `time_compare_ms`,
   `resolve_max_gap`, `estimate_max_gap`; `parse_duration_arg` extracted; unit tests (§6.1).
2. **Local function.** Parser registration, registry entry, `TimeGtOverTimeFunction`,
   `eval_rollups_window_end`, the conformance fixture (§6.3). Push-down still off for this name,
   so the local path is proven alone first.
3. **Push-down.** `RollupKind`, the `RollupImpl` arm, `check_params`, the D8 rename, coordinator
   default resolution, proto + regen, the invariance and parity tests (§6.2, §6.4).
4. **Cluster tests and docs.** §6.5, §5.6. Run `SERVER_VERSION=unstable ./build.sh` with
   `TEST_PATTERN="rollup_pushdown"`.

## 8. Follow-ups / non-goals

- **Coverage companion (strongly recommended next).** Without it, `0` is ambiguous (§2). First
  check whether MetricsQL's `duration_over_time(m[d], max_interval)` means "seconds the series was
  present with gaps bounded by `max_interval`" (unverified). If it does, implement it under that
  name with the same hold rule; the reducer is `time_compare_ms` with an always-true comparison.
  `share_time_gt_over_time` is then `time_gt / duration`.
- **Operator siblings** `time_ge/lt/le/eq/ne_over_time` (D2).
- **Linear-interpolation variant** (`time_gt_linear_over_time`): different edge cases (crossings,
  `±Inf`, equal timestamps); a separate plan.
- **Predecessor-sample semantics** (D1): only with the two-path extra-sample loading and a second
  rollup class.
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
