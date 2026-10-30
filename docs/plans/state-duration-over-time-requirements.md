# `state_duration_over_time` — requirements and scoping (plan)

**Status:** v1 implemented 2026-09-29; cluster push-down (§5.4) implemented 2026-09-30.
Uncommitted. Decisions D1–D8 taken as written, D3 revised (no sentinel).
**Scope:** A new PromQL rollup function in the PromQL engine (`src/promql/`). Cluster
rollup push-down is explicitly out of scope for v1 (see §5.4).
**Semantics source:** Flux `stateDuration()`, as documented in
[Monitor states](https://docs.influxdata.com/influxdb/v2/query-data/flux/monitor-states/).
That page is the only behavioral reference. The function has no Prometheus counterpart, so
there is no upstream behavior to match — the Flux rules below are the contract, adapted to
what a PromQL rollup *is* (one value per window, not one row per input point).

## 1. Reference semantics (Flux `stateDuration`)

From the linked page, `stateDuration(fn: (r) => r._value == "closed", column: ..., unit: 1s)`:

- For the first point that evaluates `true`, the state duration is set to `0`.
- For each consecutive point that evaluates `true`, the duration increases by the time
  interval between each consecutive point, in the specified unit.
- If the state is `false`, the duration is reset to `-1`.

- `unit` is "the unit of time (`1s` (by default), `1m`, `1h`) used to increment the state
  duration".

Worked example from the docs (points written every minute, `unit: 1s`):

| timestamp | `_value` | duration (docs) | duration (by the rules) |
|---|---|---|---|
| 17:39:16 | closed | 0 | 0 |
| 17:40:16 | closed | 60 | 60 |
| 17:41:16 | closed | 120 | 120 |
| 17:42:16 | open | -1 | -1 |
| 17:43:16 | closed | 0 | 0 |
| 17:44:27 | closed | 60 | **71** |

**The docs' last row is inconsistent with their own rule**: 17:43:16 → 17:44:27 is 71 s, not
60. The rules are the contract, so the conformance fixture expects `71` (§6). The page's
prose also contradicts itself once ("If `_value` is not `closed`, the state duration is reset
to `0`"); the rule list and both tables say `-1`, so that is Flux's behavior (this plan omits the
series instead — D3).

Key properties carried over: duration accumulates **time deltas between adjacent matching
points**, and the first point of a run contributes `0`. Flux's `-1` for a non-matching point
is **not** carried over: a PromQL rollup can leave a series out of a step's result, which a
Flux row can't, so that series has no answer instead of a sentinel (D3). Because the deltas of one run telescope, the accumulated duration of a run
always equals `t_last_match − t_run_start` — the implementation uses that identity (§3).
`unit` carries over as an optional trailing argument (D5).

## 2. Proposed PromQL surface

```
state_duration_over_time(v range-vector, state scalar [, unit string])
```

Examples: `state_duration_over_time(door_closed[1h], 1)` (seconds),
`state_duration_over_time(door_closed[1d], 1, "1m")` (minutes).

- One window of one series in, one instant-vector sample out (the standard rollup shape:
  [`make_rollup_function!`](../../src/promql/functions/rollup_functions.rs) /
  `QuantileOverTimeFunction`).
- `state` is a float scalar. Flux takes an arbitrary lambda (`r._value == "closed"`); PromQL
  function calls cannot carry closures, so equality-to-a-scalar is the transcription and
  covers the numeric use cases (`state_duration_over_time(up[1h], 1)`). See D1.
- `unit` is optional and last. It is a PromQL string literal parsed with the module's duration
  parser, `parse_positive_duration_value` ([`src/parser/duration.rs`](../../src/parser/duration.rs)),
  so it accepts the same units as everywhere else (`ms`, `s`, `m`, `h`, `d`, `w`, `y`) and
  compound forms (`"1h30m"`). Default `"1s"` — Flux's default, and PromQL's seconds
  convention. Validation (D8):
  - the result must be `> 0` ms (`parse_positive_duration_value` accepts `0`; a zero unit
    would divide by zero, so reject it explicitly);
  - the string must carry a unit suffix. The shared parser reads a bare number as
    **milliseconds** (`"60"` = 60 ms), while a PromQL user reading `"60"` expects seconds.
    Rejecting suffix-less input removes the ambiguity (it can be relaxed later; it can't be
    tightened later);
  - parse failures map to `EvaluationError::ArgumentError` naming the function and argument.
  A string argument is always a literal in PromQL, so `unit` is parsed once per call, not per
  series or per step.
- Two required arguments plus one optional, in that order; arity and types are validated
  twice — parse time by the parser's `Function` registration (`variadic = 1` makes the last
  declared argument optional: min 2, max 3, per promql-parser's `check_call_arity`), then
  again by the engine's `evaluate_function_args`.

## 3. Reduction semantics (the contract)

Scan the window's samples in ascending timestamp order (the storage invariant). Only the
**final run** matters, so the reducer tracks one `Option<i64>` — the timestamp at which the
current run started:

1. A matching sample (`state == sample.value`) while no run is open ⇒ `run_start = t_i`.
2. A matching sample while a run is open ⇒ nothing (the run continues).
3. A non-matching sample ⇒ `run_start = None`.
4. After the last sample: if the last sample matched, the value is
   `(t_last − run_start) / unit_ms` as a float; otherwise the series has no answer and is
   left out of the result for that step.

This is exactly Flux's "`0`, then `+Δt` per consecutive match" accumulator — the per-step
deltas telescope — evaluated at the window's last sample, i.e. what Flux would emit at that
timestamp. Computing it as one `i64` subtraction and one division, rather than summing `Δt`s
as floats, keeps the result exact for long runs and makes duplicate timestamps (`Δt = 0`)
fall out for free.

Unit conversion rules:

- The division is float division — no truncation. `state_duration_over_time(x[1h], 1, "1m")`
  over a 90 s run answers `1.5`, not `1`. (The monitor-states page doesn't say whether Flux
  truncates to whole units; PromQL values are floats, so this plan doesn't. See D8.)

"Consecutive" means adjacent in the window's sorted sample list — no staleness threshold or
maximum-gap notion exists in Flux either, so none is introduced here.

Edge cases, by rule:

| case | value / behavior | rule |
|---|---|---|
| window empty | no output sample at all | engine-wide: an empty window yields no output, not NaN ([`rollups.rs`](../../src/promql/functions/rollups.rs)) |
| window has samples, none match | no output sample | D3 |
| first sample matches | `0` | Flux rule 1 |
| match after a match | `prev + Δt`, in `unit` (default seconds) | Flux rule 2 |
| match after a non-match | `0` (new run) | Flux rule 1 |
| last sample non-matching | no output sample | D3 |
| `NaN` sample | never matches ⇒ breaks the run | IEEE `==`; same fall-through as `changes`/`resets` |
| `state` argument is `NaN` | rejected as an argument error | a function that can never answer is a footgun (D2) |
| samples outside the window | must not influence the answer | existing `eval_windows` invariant test ("Seeing samples outside its window…") |
| duplicate timestamps | `Δt = 0`, run stays alive | defensive; storage is last-wins |
| `unit` omitted | seconds | D5 |
| `unit` is `"0s"`, negative, unparsable, or has no suffix | argument error | D8 |

Two user-visible consequences worth stating in the function's docs:

- **The answer is capped by the range.** A run can't be longer than the window it was
  measured in, so `state_duration_over_time(x[5m], 1) > 3600` can never fire. Alerting on
  "in state for more than T" needs a range of at least `T` plus one sample interval.
- **The answer is measured to the last sample, not to the evaluation time.** With a 60 s
  scrape interval the reported duration lags the wall clock by up to one interval. This is
  Flux's behavior too (it only emits values at points); it is not extrapolated.

The window-locality rule has one deliberate consequence that diverges from Flux (D4): Flux
streams are continuous, so a run carries across any boundary you draw later. A PromQL rollup
is defined per window — each step's window restarts from scratch, and the function must not
read `RollupWindow`'s `prev_value`/`real_prev_value`/`real_next_value`, or the local and
pushed-down paths would answer differently for the same query.

## 4. Decisions requiring sign-off

- **D1 — predicate shape.** Equality to a scalar for v1. Alternatives rejected: no-op
  predicates (useless), comparison-operator variants (`state_duration_over_time(v, 1, ">")`
  — string arguments and a third arg, easy follow-up if needed), or a no-arg "any non-zero"
  variant. Stateful single-equality is the direct Flux transcription.
- **D2 — `state = NaN`.** Reject with `EvaluationError::ArgumentError` rather than silently
  returning nothing for every series.
- **D3 — no reset sentinel (revised 2026-09-29).** Flux writes `-1` when the state is false.
  Here a series whose last sample isn't in `state` is left out of the step's result — the
  PromQL convention for "no answer" (`rate` with one sample, `irate`), and the shape
  `RollupImpl::WholeWindowOptional` already exists for. Considered and rejected:
  - `-1` (Flux parity): a sentinel silently skews `sum`/`avg` and inflates `count`.
  - `NaN`: comparisons already drop `-1` and NaN alike, so filtering gets no easier; NaN
    poisons `sum`/`avg` for the whole group, still counts in `count`, and spreads through
    arithmetic.

  `0` still means "state started at the last sample", so it stays distinct from "not in
  state". The one thing lost: "has samples, not in state" looks the same as "no samples",
  recoverable with `present_over_time(x[w]) unless state_duration_over_time(x[w], s)`.
  This differs from Flux on purpose; queries ported from Flux that test `== -1` need
  rewriting.
- **D4 — window locality / no carry-over.** Per §3. This also means the function is eligible
  for `RollupKind` push-down later (its answer depends only on the window's own samples).
- **D5 — units.** Output defaults to **seconds** (float), matching both Flux's default
  `unit: 1s` and PromQL convention (`rate`, `resets`, …). Flux's `unit` carries over as an
  optional third argument: a string literal parsed by the shared duration parser (§2), so
  `"1m"` / `"1h"` / `"1h30m"` / `"500ms"` all work and the accepted grammar matches every
  other duration the module takes. A string (rather than a scalar number of seconds) is used
  because it is what Flux users write and it goes through one parser; PromQL string
  arguments are always literals, so there's no dynamic-unit case to handle.
- **D8 — `unit` validation and rounding.** Reject `unit <= 0` and suffix-less numbers
  (`"60"` would otherwise mean 60 ms). Divide as floats; do not truncate to whole units. Alternatives: accept bare numbers as milliseconds
  (consistent with `TS.*` commands, surprising in PromQL), or as seconds (inconsistent with
  the shared parser — would need a PromQL-local wrapper).
- **D6 — experimental gate.** Register with the parser as `experimental = true`, gated by the
  existing `ts-promql-enable-experimental-functions` config (default off) until conformance
  coverage exists — the same pattern as the parser's experimental `start`/`end`/`range`/
  `step`. Flipping to non-experimental later is a one-line registration change.
- **D7 — name.** `state_duration_over_time` as requested. The Flux sibling `stateCount` is
  planned separately as `state_count_over_time(v range-vector, state scalar)` in
  [state-count-over-time-requirements.md](state-count-over-time-requirements.md). It counts
  from `1` and takes no `unit`, and it answers `0` rather than omitting a series not in state,
  because for a count `0` doesn't already mean something else (its D3).

## 5. Integration points (everything "implementing this" touches)

### 5.1 Parser registration (blocking, no dependency change)

`promql-parser = "0.11.0"` (pinned in `Cargo.toml`) already exposes
`register_extra_functions` for exactly this pattern (custom functions, Thanos-style), and
`get_function` consults registered extras at parse time — so arity/type checking of the new
name works without patching the crate.

**Where it's called, as built:** not in the fanout registration (`register_promql_fanout()`
runs only in cluster mode — [`src/lib.rs`](../../src/lib.rs)). `register_parser_functions()`
([`src/promql/functions/mod.rs`](../../src/promql/functions/mod.rs)) registers once behind a
`OnceLock` and is called unconditionally at module load, before any command can parse a
query; a clash with a future built-in fails the load. `crate::promql::parse_query` does
**not** register, so the parse hot path stays a plain parse.

Tests never load the module, so they register explicitly: `build.rs` emits the call at the
top of every generated promqltest `#[test]`, and the evaluator tests' `parse_and_evaluate`
calls it before parsing. Any other test that parses an extension function must call it
first.

```rust
Function::new(
    "state_duration_over_time",
    vec![ValueType::Matrix, ValueType::Scalar, ValueType::String],
    1,                          // last arg (`unit`) optional: 2..=3 args
    ValueType::Vector,
    /* experimental */ true,   // per D6
)
```

Notes: registration refuses conflicts with built-ins.

### 5.2 Function registry

- Add `(StateDurationOverTime, "state_duration_over_time", StateDurationOverTimeFunction)`
  to the canonical `promql_function_list!` in
  [`src/promql/functions/function_list.rs`](../../src/promql/functions/function_list.rs)
  (the "ADD NEW FUNCTIONS HERE" list).
- `resolve_function` then dispatches the name, and the evaluator's
  `call.func.experimental` gate picks the flag up from the parser-side `Function`
  (checked in `evaluate_call` and the preload/pushdown paths).

### 5.3 Implementation (new file)

`src/promql/functions/state_duration.rs`, modeled on the two-arg
`QuantileOverTimeFunction` ([`rollup_functions.rs`](../../src/promql/functions/rollup_functions.rs))
and the sequential-scan reducers `rollup_changes` / `rollup_resets`
([`range_vector_functions.rs`](../../src/promql/functions/range_vector_functions.rs)):

- `StateDurationOverTimeFunction: PromQLFunction` with `apply_call` doing 2..=3-arg
  handling (`expect_min_arg_count` + `max_arity_error` in
  [`utils.rs`](../../src/promql/functions/utils.rs)): `expect_range_vector` for the window,
  `expect_scalar` for `state`, and — when present — `expect_string` for `unit`, parsed once
  via `crate::parser::duration::parse_positive_duration_value` into `unit_ms: i64` with the
  D8 checks (`> 0`, suffix required); `ParseError` maps to `EvaluationError::ArgumentError`.
  Then the per-series reduction.
- Reducer `rollup_state_duration(values: &[Sample], state: f64) -> Option<f64>` returns the
  final run's length in **milliseconds**, or `None` — pure, window-local, sequential.
  `state_duration_window(window, state, unit_ms)` wraps it with the unit division, and is
  the one function both the local path and a shard answering a pushed-down grid reduce with
  (the `rollup_deriv` precedent; §5.4). `eval_rollups_basic` takes an `Option`-returning
  reducer and drops the series on `None`, as it does for an empty window.
- The output drops `__name__` (the value is a duration, not the input metric), like the
  other value-transforming `*_over_time` rollups — confirm which mechanism the rollup macro
  uses and follow it.
- Declare the new module in [`src/promql/functions/mod.rs`](../../src/promql/functions/mod.rs).

### 5.4 Push-down (implemented 2026-09-30)

The function satisfies the `RollupKind` membership rule ("output depends on nothing but one
series' samples inside the window"), so it is pushed down like the other rollups: unfused,
fused with an outer reduction, and over a range query's whole step grid.

**Design change from the original sketch: `unit` goes on the wire.** The sketch had shards
return milliseconds and the coordinator divide. That breaks under fusion: `count(...)` must
not be divided at all, and `sum`/`avg` divided after a shard-side reduction round differently
from the local `sum(ms / unit)`. So the request carries the unit, and the shard reduces with
the very function the local path runs — `state_duration_window(window, state, unit_ms)`
([`state_duration.rs`](../../src/promql/functions/state_duration.rs)).

As built:

1. `RollupKind::StateDurationOverTime` (+ `from_function_name`, `function_name`, `all()`,
   `implementation`), reduced through a new `RollupImpl::WholeWindowWithUnit` arm.
   `eval_windows` takes `unit_ms` alongside `param`.
2. `GridRollup` gains `unit_ms: Option<i64>` (`None` = the kind's default), and so does the
   preload key — two units over one selector are two grids.
3. `RollupKind::check_params(param, unit_ms)` is the one rule for which parameters are
   valid: a unit only on this kind and only positive; a state present and not `NaN`. The
   coordinator keeps a call it refuses local, so the local path reports the error; a shard
   refuses such a request outright.
4. The coordinator parses the `unit` string with `parse_unit` in `pushable_rollup` (grid and
   fused paths) and `rollup_arguments` (instant path); a unit that does not parse keeps the
   call local. `pushable_rollup` now returns the finished `GridRollup`, which also builds the
   preload key.
5. Wire: `ROLLUP_KIND_STATE_DURATION_OVER_TIME = 25` and `GridRollup.unit_ms = 4` in
   [`proto/v1/promql.proto`](../../proto/v1/promql.proto), both conversions in
   [`grid_fanout_command.rs`](../../src/promql/engine/fanout/grid_fanout_command.rs), and
   the regenerated file. Mixed-version clusters are not a concern: every node runs the same
   build, and a shard refuses a kind it does not know.
6. [`docs/overview.md`](../../docs/overview.md)'s pushed-down function list.

### 5.5 Config

No new `ts-promql-*` parameter: v1 reuses `ts-promql-enable-experimental-functions`
([`src/config.rs`](../../src/config.rs); default false, snapshot read into
`enable_experimental_functions`, promqltest harness sets it true).

### 5.6 Docs

- [`docs/topics/promql.md`](../../docs/topics/promql.md): add the function to the rollup
  list, with a semantic note: not-in-state series are omitted (and the `unless` recipe),
  default unit seconds, optional
  `unit` string with the accepted suffixes, the range-caps-the-answer and
  measured-to-last-sample caveats (§3), and the experimental flag.
- Once un-gated / pushed down: [`docs/overview.md`](../../docs/overview.md) lists.

## 6. Test requirements

1. **PromQL conformance** — new fixture `src/promql/promqltest/testdata/state_duration.test`
   (auto-generated into one `#[test]` per file by `build.rs`; the harness enables
   experimental functions). Cases must include:
   - the docs' door example (inputs verbatim) as `eval instant`s over windows ending at each
     row, expecting 0/60/120/(no sample)/0/**71** — not the docs' `60` for the last row (§1);
   - the same example with `"1m"` and `"500ms"`, proving the unit divides the duration and is
     float (no truncation);
   - `count`/`sum` over several series, proving not-in-state series don't reach the
     aggregation, and the `present_over_time … unless` recipe;
   - explicit `"1s"` answers identically to the two-argument form;
   - consecutive accumulation with uneven deltas (prove the *interval*, not the count, is
     accumulated);
   - reset on non-match, then new run starting at 0;
   - window containing samples but no matches ⇒ no output sample;
   - empty window ⇒ series absent from the result;
   - `NaN` sample mid-run ⇒ breaks the run;
   - `eval range` (stepped windows) proving independence per step and that a run spanning a
     window boundary restarts;
   - wrong arity (1 or 4 args) / non-scalar state / non-string unit (parse-time rejection).
   - bad `unit` values: `"0s"`, `"-1m"`, `"60"` (no suffix), `"abc"`, `""`. The promqltest
     DSL never compares `expect fail msg:` text, so the harness only proves *that* these fail;
     the message (naming the function and the `unit` argument) needs a Rust test.
2. **Unit tests** — reducer-level cases in `functions_tests.rs` (or the new module):
   empty input, single sample, exact-delta accumulation, NaN state rejection, a long run
   (days of 1 s samples) returning an exact integer millisecond count, `unit` parsing and
   the D8 rejections, and `__name__` being dropped (promqltest `{...}` label expectations are
   lenient and can't prove it).
3. **No compat testing** — this is a PromQL-engine surface, not a `TS.*` command; nothing to
   diff against the RTS reference server.
4. **Push-down tests** (done) — the per-kind suites pick the new kind up through
   `RollupKind::all()` (window invariance, local-vs-pushed parity instant/grid/modifiers,
   wire shapes), using `RollupKind::test_params()` for its state and a `"1m"` unit; plus
   dedicated tests for the request's state and unit, invalid arguments staying local,
   fused/multi-unit parity, the wire round trip and the shard's refusals, and cluster tests
   in `tests/test_ts_query_rollup_pushdown_cme.py` (exact values + on/off equivalence;
   mutation-checked: a shard that drops the unit fails four of them).

## 7. Out of scope / non-goals

- `stateCount` / `state_count_over_time` sibling (same plan pattern, separate decision).
- Arbitrary predicates (non-equality comparisons, string states, `and`/`or` predicates).
- Multi-state output; carrying runs across range-query steps.
- Duration-literal (unquoted `1m`) or scalar forms of `unit` — string only in v1.
- `TS.*` command surface, cluster wire changes, compat suite entries.
