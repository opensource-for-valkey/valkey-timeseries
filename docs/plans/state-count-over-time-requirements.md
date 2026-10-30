# `state_count_over_time` — requirements and scoping (plan)

**Status:** Implemented 2026-09-30, including push-down (uncommitted). Decisions D1–D7
taken as written; `state_duration.rs` was renamed to `state.rs` per §5.3.
**Scope:** A new PromQL rollup in `src/promql/`, including cluster push-down. The shared
infrastructure already exists: it was built for `state_duration_over_time` (26eebb933,
b4dd222f4; plan: [state-duration-over-time-requirements.md](state-duration-over-time-requirements.md)).
This plan covers only what differs or has to be added.
**Semantics source:** Flux `stateCount()`, as documented in
[Monitor states](https://docs.influxdata.com/influxdb/v2/query-data/flux/monitor-states/).
That page is the only behavioral reference; the function has no Prometheus counterpart.

## 1. Reference semantics (Flux `stateCount`)

From the linked page, `stateCount(fn: (r) => r._value == "closed", column: ...)`:

- For the first point that evaluates `true`, the state count is set to `1`.
- For each consecutive point that evaluates `true`, the state count increases by 1.
- If the state is `false`, the state count is reset to `-1`.

Worked example from the docs (the same door data as `stateDuration`):

| timestamp | `_value` | count (Flux) | count (this plan) |
|---|---|---|---|
| 17:39:16 | closed | 1 | 1 |
| 17:40:16 | closed | 2 | 2 |
| 17:41:16 | closed | 3 | 3 |
| 17:42:16 | open | -1 | **0** |
| 17:43:16 | closed | 1 | 1 |
| 17:44:27 | closed | 2 | 2 |

Unlike the `stateDuration` table, this one is consistent with its rules: it counts samples,
so the uneven 71 s gap in the last row doesn't matter. Flux takes no `unit` here.

## 2. Proposed PromQL surface

```
state_count_over_time(v range-vector, state scalar)
```

Example: `state_count_over_time(door_closed[1h], 1) >= 5` — doors closed for at least the
last five samples.

- Two arguments, both required: the range vector and a float `state`. There is no `unit`
  argument; a count has none.
- The answer is the number of consecutive samples equal to `state` at the end of the window.
- One window of one series in, one sample out. The output drops `__name__` (automatic for a
  function with a range-vector argument, via `drops_metric_name`).
- Experimental, gated by `ts-promql-enable-experimental-functions` (D6), like its sibling.

## 3. Reduction semantics (the contract)

The window's samples are in ascending timestamp order. The answer is the length of the run
of samples equal to `state` that ends at the window's **last** sample:

- the last sample is in `state`: the number of consecutive samples in `state` ending there
  (`1` when only the last one is);
- the last sample is not in `state`: **`0`** (D3);
- the window is empty: no output sample (the engine-wide rule; `count_over_time` behaves the
  same).

Because only the final run matters, the reducer scans backwards and stops at the first
sample not in `state`, so it costs O(run length), not O(window):
`values.iter().rev().take_while(|&&v| v == state).count()`.

Edge cases:

| case | answer | rule |
|---|---|---|
| window empty | no output sample | engine-wide |
| samples, none in state | `0` | D3 |
| last sample not in state | `0` | D3 |
| only the last sample in state | `1` | Flux rule 1 |
| `NaN` sample | never matches, so it ends the run | IEEE `==`, as in the sibling |
| window ends on a `NaN` sample | `0` | follows from the row above |
| `state` argument is `NaN` | argument error | D2 |
| samples outside the window | ignored | D4 |

As with the sibling, the answer is capped by the range: a window holds a bounded number of
samples, so `state_count_over_time(x[5m], 1)` at a 15 s scrape interval can never exceed 20.

## 4. Decisions requiring sign-off

- **D1 — predicate.** Equality with a scalar, as in `state_duration_over_time` (its D1).
- **D2 — `state = NaN`.** Rejected with `EvaluationError::ArgumentError`: no sample ever
  equals NaN, so every answer would be `0`.
- **D3 — "not in state" is `0`, not an omitted sample.** This is where the two siblings
  differ, and on purpose:
  - For a duration, `0` already means "the state began at the last sample", so `0` couldn't
    also mean "not in state"; omitting the sample was the only unambiguous answer.
  - For a count, the first sample in state counts `1`, so `0` is free. It is also simply
    true: there are zero consecutive samples in state. No sentinel is involved.
  - PromQL's own counting functions answer `0` rather than omitting the series: `changes`,
    `resets` and `count_over_time` do.
  - "No data" (no sample) and "not in state" (`0`) stay distinguishable, which the duration
    function lost.
  - Range queries have no gaps: a streak graph falls to 0 instead of disappearing.
  - Flux users rewrite `== -1` as `== 0`.

  The cost is in aggregations. `count(state_count_over_time(...))` counts every series that
  has data, not the series in state; that needs `count(state_count_over_time(...) > 0)`.
  `avg` includes the zeros (an average streak across the fleet, not among those in state).
  `sum` and `max` are unaffected. Combining the siblings still works:
  `state_duration_over_time(x[w], s) / state_count_over_time(x[w], s)` never divides by the
  `0`, because a series not in state is absent on the duration side and drops out of the
  binary operation.

  Alternatives rejected: `-1` (Flux parity; a sentinel that skews `sum` and `avg`), `NaN`
  (poisons `sum` and `avg`, and spreads through arithmetic), and omitting the sample (what
  the duration function does, but here it gives up a true answer for nothing).
- **D4 — window locality.** Each window is counted on its own; a run that began before the
  window is counted from the window's first sample. The reducer must not read `RollupWindow`'s
  `prev_value`, `real_prev_value` or `real_next_value`, or the local and pushed-down paths
  could disagree. This also makes the function eligible for push-down.
- **D5 — push-down in the first version.** Unlike the sibling, whose push-down came
  afterwards, this one ships with it: the shape needs no new machinery (§5.4), and the
  per-kind test suites cover it for free.
- **D6 — experimental gate.** Registered with the parser as experimental, like the sibling.
- **D7 — name.** `state_count_over_time`, matching the sibling and the `*_over_time` family.

## 5. Integration points

### 5.1 Parser registration

Add a second entry to `register_parser_functions()`
([`src/promql/functions/mod.rs`](../../src/promql/functions/mod.rs)):

```rust
Function::new(
    "state_count_over_time",
    vec![ValueType::Matrix, ValueType::Scalar],
    0,                          // exactly two arguments
    ValueType::Vector,
    true,                       // experimental (D6)
)
```

Module load, the `build.rs`-generated promqltest tests and the evaluator tests'
`parse_registered` all call `register_parser_functions()`, so nothing else changes. Any other
test that parses the function has to call it first, as for the sibling.

### 5.2 Function registry

Add `(StateCountOverTime, "state_count_over_time", StateCountOverTimeFunction)` to
`promql_function_list!` in
[`function_list.rs`](../../src/promql/functions/function_list.rs).

### 5.3 Implementation

The two siblings share the `state` argument checks (scalar, not NaN) and the parser
registration, so they belong in one module. Rename `state_duration.rs` to `state.rs` (or
`state_functions.rs`) and add the count beside the duration, rather than copying the checks
into a second file.

- `StateCountOverTimeFunction`: exactly two arguments; `expect_range_vector`, then
  `expect_scalar` for `state`, rejecting NaN (D2). Evaluate with
  `eval_rollups(ctx, range, Some(state), rollup_state_count)` — the path
  `QuantileOverTimeFunction` takes.
- Reducer `rollup_state_count(window: &RollupWindow, state: Option<f64>) -> f64`: the backward
  scan from §3 over `window.values`. This is the `RollupImpl::Windowed` shape, so the local
  path and a shard reduce with the same function pointer. A missing `state` can't reach it
  (`check_params` refuses it, §5.4); return `NaN` rather than panic if it ever does.

### 5.4 Push-down

No new wire field, `RollupImpl` arm or preload-key change. The existing one-parameter path
carries `state` exactly as it carries `quantile_over_time`'s phi.

1. `RollupKind::StateCountOverTime`: `from_function_name`, `function_name`, `all()` (the
   exhaustive match fails to compile until it is added), and
   `implementation() => RollupImpl::Windowed(rollup_state_count)`.
2. `RollupKind::check_params`: the count needs a state that is present and not NaN, like
   `StateDurationOverTime`, and takes no unit (the existing unit rule already refuses a unit
   on any other kind). The coordinator keeps a refused call local, where it errors; a shard
   refuses the request.
3. `RollupKind::test_params()`: `(Some(1.0), None)`.
4. Wire: `ROLLUP_KIND_STATE_COUNT_OVER_TIME = 26` in
   [`proto/v1/promql.proto`](../../proto/v1/promql.proto), both conversion arms in
   [`grid_fanout_command.rs`](../../src/promql/engine/fanout/grid_fanout_command.rs), and
   `VALKEY_TS_PROTO_REGEN=1 cargo build`.
5. `pushable_rollup` and `rollup_arguments` need no change: a matrix and a number literal is
   already a pushable shape.

### 5.5 Docs

- [`docs/topics/promql.md`](../../docs/topics/promql.md), "Functions beyond Prometheus": the
  function next to `state_duration_over_time`. State the D3 asymmetry right there — the
  duration omits a series not in state, the count answers `0` — and the
  `count(... > 0)` pattern for counting series in state.
- [`docs/overview.md`](../../docs/overview.md): add it to the pushed-down function list.
- The duration plan's D7 names this function as future work "under the same contract"; point
  it here and note the different "not in state" answer.

## 6. Test requirements

1. **Conformance fixture** `src/promql/promqltest/testdata/state_count.test` (the harness
   enables experimental functions):
   - the Flux door example, expecting 1/2/3/**0**/1/2;
   - a second door that stays closed, with `count(...)` counting both doors and
     `count(... > 0)` counting one — pinning the D3 cost in a test;
   - samples present but none in state ⇒ `0`; empty window ⇒ no sample;
   - a NaN sample ending a run, and a window ending on NaN ⇒ `0`;
   - an `eval range` showing `0` at the out-of-state steps (no gaps), and runs that start
     before the window counted from the window's first sample;
   - the range cap (a window can't count more samples than it holds);
   - argument errors: 1 or 3 arguments, non-scalar state, `NaN` state.
2. **Unit tests** beside the reducer: an empty slice, a single sample in and out of state,
   the door example row by row, a NaN in the middle and at the end, and a window-locality
   test over a series with multi-sample runs (the per-kind invariance test's series never
   repeats a value, so it can't show that on its own — the sibling has the same dedicated
   test).
3. **Evaluator tests**: the experimental gate; the argument error text (promqltest never
   compares `expect fail msg:` text); `__name__` dropped (promqltest label expectations can't
   prove it); and a `rollup_query_with` arm so the per-kind parity suites cover the function —
   `state_count_over_time({selector}, 8)` gives the gappy dataset a two-sample run.
4. **Push-down**: the per-kind suites pick it up through `RollupKind::all()` (parity for
   instant, grid and modifiers; window invariance; wire shapes). Add a wire round-trip check
   that `state` survives, and a shard-refusal check for a missing or NaN state and for a unit
   on this kind.
5. **Cluster** (`tests/test_ts_query_rollup_pushdown_cme.py`), where `web-3` reads
   7, 7, 7, 8, 8:
   - `state_count_over_time(mem_usage[2m], 8)` at `EVAL` answers `web-3` = 2 **and** `0` for
     every other gauge series. That is the D3 behavior across shards: unlike the sibling,
     series not in state are present;
   - `sum by (job)` and `count(... > 0)` over it;
   - entries in the on/off equivalence lists (instant, range, fused).

   Mutation-check it the way the sibling was checked: make the shard ignore `state` and
   confirm the equivalence tests fail.

## 7. Out of scope

- A `unit` or any other third argument.
- Predicates other than equality (see the sibling's D1).
- Counting a run across window boundaries (D4).
