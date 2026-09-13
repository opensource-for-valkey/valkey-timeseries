# Data-derived filter push-down for PromQL range queries (plan)

**Status:** Implemented 2026-09-13 (work items 1–8). Measured on the 3-node harness
(500 series × 3600 at 1 s, p50 over three interleaved on/off pairs):
`cpu{region="us"} - cpu offset 5m` at 60 steps 17.7–18.0 → 10.5–10.7 ms (one drifted
pair 14.8); `cpu and on(region) cpu{region=~"us|eu"}` 20.8–21.8 → 15.5–20.5 ms; the
non-narrowable `cpu / on(host) group_left cpu offset 5m` within noise (min 38.4–38.8
both ways); instants and non-binop ranges unchanged; coordinator peak RSS 189–217 MB
either way. Single node, criterion `range_query` at 1000 steps: `a_hundred and
b_hundred{l=~'.*[0-4]$'}` −29 %, `a_hundred + on(l) group_right a_one` −11 %,
`a_hundred and b_hundred{l='notfound'}` −81 % (the written matcher crosses),
`a_hundred - b_hundred` and `rate(a_hundred[1m]) + rate(b_hundred[1m])` (byte-identical
trees) −12 %/−13 % against a wide baseline, i.e. no regression. Deviations from the
text below: the leaf rule is *derived ∪ written* with `retain_pruning` deciding at the
target (§2.2 as written, but the written matchers are always offered, not only for
labels the profile could not vouch for — an inner rewrite's matcher is tighter than the
pre-rewrite profile); and §2.2's guard is applied per binary operation inside the
rewrite (`LeafFilters::narrows`), so a `fill`/`or` operation is never narrowed even
when a profiled leaf sits under it. Written for a pre-release codebase — nothing below
exists for version skew.
**Scope:** range queries (`TS.QUERYRANGE`), single node and cluster, whose expression
contains a binary operation that `can_push_down_common_filters` accepts — `a * b`,
`a - b offset 5m`, `a and b`, `a unless b`, `a * on(k) group_left b`, and nests of these
with aggregations, functions, rollups and subqueries as operands. Instant queries are a
non-goal: they already have the runtime push-down and it is not blocked there.
**Evidence base:** the 2026-09-13 3-node benchmark (500 series × 3600 samples at 1 s;
`scratchpad/bench/run_cluster.sh`), where `cpu{region="us"} - cpu offset 5m` at 60 steps
runs in 18.9 ms with no filter push-down and 10.5 ms when the right-hand selector is
narrowed to `region="us"` — the same narrowing this plan derives from data rather than
from what the user typed.

## 1. The problem

There are two filter push-downs today and a range query gets neither of the useful one.

| mechanism | derives filters from | runs | applies to range queries? |
|---|---|---|---|
| static rewrite (`ts-promql-optimize-queries` → `optimize_expr` → `pushdown_filters_in_place`, [pushdown.rs](../../src/promql/optimizer/pushdown.rs)) | matchers *written in the query* | at parse time, before planning | yes — but only helps when the user already wrote a selective matcher on one side |
| runtime push-down (`eval_binop_with_pushdown`, [evaluator.rs](../../src/promql/exec/evaluator.rs); `get_common_label_filters`, [labels.rs](../../src/promql/binops/labels.rs)) | the *label values the first operand's result actually carries* | per evaluation, one side after the other | **no** — gated off by `has_preloaded_data()` |

The runtime path is the one with real leverage (`kube_pod_created{namespace="prod"} *
on(uid) group_left kube_pod_info`: the right side is every pod in the cluster unless the
left's `namespace="prod"` is pushed into it), and it is switched off for range queries for
a structural reason, not a semantic one: a rewritten selector has a different `PreloadKey`
than the one `preload_grid` loaded, so the rewritten subtree would miss its grid and fall
back to one live read per step. The gate at [evaluator.rs](../../src/promql/exec/evaluator.rs)
(`has_preloaded_data`) is the right call given that mechanism; the mechanism is what has
to change.

The fix has to happen **before planning**: the tree that `PlannedQuery` sees must already
be the rewritten one, so that preload keys and step-time lookups agree by construction.
That rules out deriving the filters from an evaluation (there is none yet) and leaves two
sources of "what label values does this side carry": the preloaded grids themselves, read
back after loading one side (rejected, §2.4), or the series index, asked directly
(chosen).

## 2. Design: a data-informed optimizer pass

The static optimizer already knows every structural rule — which labels survive
`sum by (k)`, `label_replace`, `count_values`, `on()`/`ignoring()`, `group_left`, which
operators may not be pruned (`or`, fill modifiers) — and applies them from one leaf rule:
*a selector's common filters are the matchers written on it*. This plan keeps all of that
and replaces the leaf rule with *the labels every series the selector matches carries*,
obtained from the index in one cheap round before preload. Everything above the leaves is
untouched.

### 2.1 Label profiles

A **label profile** of a selector is, over the set `S` of series it matches in the index:

```
LabelProfile {
    series: u64,                       // |S|
    labels: [ { name, carried_by: u64, values: [String] (≤ MAX_PUSHDOWN_VALUES), overflow: bool } ],
}
```

`__name__` is never included. A label is *common* when `carried_by == series`; it yields
`name="v"` for one value and `name=~"v1|v2|…"` (sorted, escaped — `join_regexp_values`)
for several, unless `overflow` (more than `MAX_PUSHDOWN_VALUES` = 60, the cap
`get_common_label_filters` already applies). A profile is **unavailable** when `|S|`
exceeds `MAX_PROFILED_SERIES` (min of `options.max_series` and 50 000) or the source cannot
answer; unavailable means "derive nothing from this leaf", never an error.

The profile is time-agnostic (the index, not the query range). That is a superset of the
series the reads will return, which only loosens the derived filters — see §3. It is also
ACL-agnostic: unauthorized series can only add values or remove a common label, and the
matchers derived are internal, never surfaced.

Sources:

- **`QueryReader::label_profile(&self, selector, options) -> PromqlResult<Option<LabelProfile>>`**,
  default `Ok(None)`. `MemorySeriesQuerier` walks its series (tests, benches).
  `CountingQueryReader` counts calls.
- **Single node** — a new `SelectorTaskKind::Profile` in
  [selector_batch_executor.rs](../../src/promql/engine/selector_batch_executor.rs); the
  local path resolves the selector through the postings index and reads each series'
  labels under the same snapshot discipline as the other task kinds (the walk is
  `O(|S| × labels)`, bounded by `MAX_PROFILED_SERIES`).
- **Cluster** — a `label-profile` fanout command (`LabelProfileRequest { filters,
  max_series }` / `LabelProfileResponse` in [promql.proto](../../proto/v1/promql.proto),
  same targeting as the selector fanouts). Each shard profiles its own series and caps
  values at 61 (setting `overflow`); the coordinator sums `series` and `carried_by`,
  unions values, ORs `overflow`, and re-applies the caps. Counts add across shards, so
  "carried by every series" is exact after the merge. A shard error other than a timeout
  makes the profile unavailable (the read that follows will report the real error, and
  fail closed on ACL exactly as today); a timeout propagates.

### 2.2 The pass

`derive_filters_in_place(expr: &mut Expr, reader, options)`, run in `evaluate_range`
([promql_engine.rs](../../src/promql/engine/promql_engine.rs)) right after
`optimize_statement` and before `PlannedQuery::for_range` — so the static rewrite, when
enabled, has already propagated written matchers, and the profiles describe the selectors
as they will be read.

1. **Collect** the leaves (vector selectors, and the selector under each matrix selector)
   that sit under some binary expression accepted by `can_push_down_common_filters`
   ([labels.rs](../../src/promql/binops/labels.rs) — the same guard the instant path uses:
   both operands vectors, neither a label-less aggregation, no `or`, no fill). Nothing to
   collect → return without touching the reader. This is the common case and costs nothing.
2. **Profile** them, deduplicated by `SelectorKey`, in one parallel batch.
3. **Rewrite** with the static machinery generalized over a leaf resolver:
   `pushdown_filters_in_place` and `get_common_label_filters` take a `&dyn LeafFilters`
   with two methods —
   - `common_filters(&VectorSelector) -> Vec<Matcher>`: the profile's common labels as
     matchers, unioned with the written matchers (so an unavailable profile degrades to
     exactly today's static rule);
   - `retain_pruning(&VectorSelector, &mut Vec<Matcher>)`: called where a matcher is about
     to be appended to a target selector (`push_filters_to_matchers`); drops any matcher
     the target's own profile already satisfies — a label the target carries on every
     series with values ⊆ the matcher's set. With an unavailable target profile every
     matcher is kept (static behaviour).

   The static optimizer passes the trivial resolver (written matchers; retain all), so
   its output is byte-for-byte what it is today and its tests pin that.

`retain_pruning` is what makes the pass safe to leave on: a matcher is added to a selector
only when the index proves it excludes at least one series that selector would otherwise
read. `cpu - cpu offset 5m` (identical profiles) rewrites to nothing; `a_X - b_X` in the
criterion bench rewrites to nothing; `cpu{region="us"} - cpu offset 5m` rewrites the right
side to `cpu{region="us"} offset 5m` and nothing else (`metric="cpu"` is common to both and
prunes neither; `host` has 500 values and overflows).

Nested binary expressions reuse the one batch of profiles: an inner rewrite narrows a
leaf, and the outer binop then sees that leaf's *pre-rewrite* profile — a superset, so
`common_filters` is looser (sound) and `retain_pruning` may keep a matcher that the
narrowed selector would not have needed (harmless). One round, no iteration.

### 2.3 What does not change

- The evaluator. `preload_grid`, the preload keys, `has_preloaded_data` and the step loop
  are untouched; they see a tree whose selectors already carry the derived matchers, and
  the grid push-down ships those narrower selectors to the shards.
- Instant queries. `evaluate_instant` keeps the runtime path, whose filters come from the
  *evaluated* first operand and are tighter than any index-derived set. Making instant
  queries use the profile pass as well (then keeping the runtime pass on top) is a
  possible follow-up, not part of this.
- The subquery preload keys. They key by node address, computed at preload; the rewrite
  precedes preload and mutates matcher lists in place, so no node moves afterwards.

### 2.4 Rejected: derive from the preloaded grids

The alternative is to make `preload_grid` order-aware — load the first operand's subtree,
read the label sets out of `preloaded_instant`/`preloaded_grids`, rewrite the second
operand in place, then load it — with `PlannedQuery` holding `&mut Expr`. It needs no new
reader method or wire message and its labels are exactly the in-range series. It was
rejected because (a) it serializes the two operands' preloads, which today overlap, so
the round trip it adds carries the first side's full data volume rather than a label
list; (b) it only sees leaves the preload covers, so an operand inside a subquery or under
a non-pushable function contributes nothing; (c) it has no target profile, so it cannot
tell a pruning matcher from a useless one and would regress `a - b`-shaped queries
(measured for the fold in the previous plan: the evaluator is sensitive to exactly this);
and (d) it puts a planner inside the evaluator's most complex function. The index round
is cheap (labels only, no samples), keeps preload parallel, and applies uniformly.

## 3. Semantics that must not move

- **Results.** For every accepted binop, a derived matcher on the target side excludes a
  series only if no series on the source side could match it at any step: the source's
  common label values over the index are a superset of its values at any step, and the
  `on()`/`ignoring()`/`group_*` trimming is the static optimizer's, already relied on. The
  unrewritten and rewritten trees therefore evaluate identically on the same data; this is
  the invariant every test below asserts (push-down on vs. off, cluster and single node).
- **Not atomic, not newly so.** A series created between the profile and the read can be
  read on one side and not the other — exactly as two parallel preloads can today. Range
  queries have no snapshot isolation to lose.
- **Errors.** A read that would have failed on a series the derived matcher excludes
  (`max_series`, an unauthorized key in cluster mode) now succeeds without touching it.
  The static rewrite and the instant runtime path already behave this way for written
  matchers; the cluster ACL suite must be checked to be asserting on shapes where nothing
  is derived (bare selectors), and the fail-closed rule itself is unchanged.
- **`__name__`** is never derived (binop matching ignores it), the 60-value cap holds, and
  `count_values`/`label_replace`/`label_join` synthesized labels are dropped by the
  existing structural rules before they reach a leaf.
- **Budgets.** Profiles are not samples: nothing is charged to
  `ts-promql-max-samples-per-query`; `max_series` bounds the walk via `MAX_PROFILED_SERIES`.

## 4. Configuration

One toggle, `ts-promql-derived-filter-pushdown` (boolean, default **yes**), load-time
like the other `ts-promql-*` parameters; off skips the pass entirely. It exists for the
same reason `ts-fanout-rollup-pushdown` does — a new cross-node request wants a kill
switch, and the cluster suite uses it for on/off parity. It is independent of
`ts-promql-optimize-queries` (which stays off by default and still governs pushing
*written* matchers blindly); with both on, the static pass runs first. Note that the
derived pass's leaf rule is *written ∪ derived*, so with only the derived toggle on a
written `region="us"` is pushed across the binop too — subject to `retain_pruning`, i.e.
only where the index says it prunes. For binops the derived pass therefore subsumes the
static one; the static flag keeps its meaning for the blind (no-profile) rewrite.

## 5. Expected effect

- Cluster, `cpu{region="us"} - cpu offset 5m` at 60 steps: ≈ 18.9 → ≈ 11 ms (the flag's
  10.5 ms plus one label-only fanout). `cpu / on(host) group_left cpu offset 5m`: no
  rewrite (`host` overflows), so it measures the pass's overhead — expected ≈ 1 ms.
- Single node, criterion `range_query`: `a_X and b_X{l=~'.*[0-4]$'}` reads half of
  `a_X`; `a_X - b_X`, `rate(a_X[1m]) + rate(b_X[1m])`, `a_X + on(l) group_right a_one`
  (`a_one` carries no `l`, so nothing is common) and `a_hundred - b_hundred` (100 values →
  overflow) are byte-identical trees and must land within noise, which is the regression
  check.
- The real-world shape (`… * on(uid) group_left info_metric` with a selective
  `namespace`/`cluster`/`job` on one side) is where it pays: the info metric is read for
  one namespace instead of all of them, on every shard, once per query.

## 6. Work plan

1. **Optimizer** — `LeafFilters` trait; thread it through `get_common_label_filters`,
   `pushdown_filters_in_place`, `push_down_binary_op_filters_in_place` and
   `push_filters_to_matchers`; trivial resolver for the static pass; `derived.rs` with
   `LabelProfile`, the profile → matchers conversion (shared `MAX_PUSHDOWN_VALUES` and
   `join_regexp_values` with [labels.rs](../../src/promql/binops/labels.rs)), the
   `retain_pruning` rule, and `derive_filters_in_place`.
2. **Reader** — `QueryReader::label_profile` (default `None`); `MemorySeriesQuerier`;
   `CountingQueryReader`.
3. **Executor + local path** — `SelectorTaskKind::Profile`; index walk with the series cap.
4. **Proto + fanout** — `LabelProfileRequest`/`LabelProfileResponse`
   (`VALKEY_TS_PROTO_REGEN=1 cargo build`); `LabelProfileFanoutCommand` ("label-profile")
   with the shard handler in [query_utils.rs](../../src/promql/engine/fanout/query_utils.rs)
   and the merge in `on_response`; register in `fanout/mod.rs`; `ValkeySeriesQuerier`
   routes to it when clustered.
5. **Engine + config** — call the pass in `evaluate_range` behind the toggle; add
   `ts-promql-derived-filter-pushdown` to `config.rs` and both rosters.
6. **Tests** —
   - unit (`derived.rs`): table tests over hand-built profiles — one value, several,
     overflow, not-common, redundant against the target, `on()`/`ignoring()`/`group_left`
     trimming, `or` and fill untouched, `unless` one-directional, nested binops, operands
     under `sum by`, `label_replace`, `count_values`, a rollup and a subquery; static
     optimizer tests unchanged;
   - fanout: shard merge (counts add, values union, overflow), unavailable on cap;
   - counting reader: one profile per distinct leaf per range query, none for a query
     without an accepted binop, none for instant queries;
   - evaluator/promqltest: the `eval range` corpus with the pass on, against
     `MemorySeriesQuerier` (bit-exact parity with the pass off);
   - cluster: `test_ts_query_derived_filter_pushdown_cme.py` — parity on/off for each shape
     above; and the observable proof that fewer series were read: with
     `ts-promql-max-response-series` = 4, `m{job="api"} + m` (4 + 8 series) errors with the
     pass off and succeeds with it on.
7. **Measure** — cluster harness (`range_binop_60steps`, `range_binop_filtered_60steps`,
   the rest unchanged) and the single-node `range_query` group, interleaved A/B, before/after.
8. **Docs** — `docs/topics/promql.md` (the two push-downs and when each applies),
   `docs/fanout-compatibility-handshake.md` (new command), config reference.

Estimate: ≈ 3 days — one for the fanout/executor/proto plumbing, half for the optimizer
generalization, one for tests, half to measure.

## 7. Open questions

- **In-range profiles.** `series_by_selectors` accepts a `MetaDateRangeFilter`; passing
  each leaf's `selector_bounds` would exclude retired series and tighten the filters at no
  extra cost. Left out of v1 to keep the profile independent of time modifiers; worth
  doing if a real dataset shows label drift diluting the value sets.
- **Empty operand short-circuit.** A profile with `series == 0` on the source side of
  `*`/`and`/`unless` means the result is empty; the pass could replace the other operand
  with a never-matching selector instead of reading it. Cheap, but a separate semantic
  step (it changes what is read, not just how much) — decide after measuring.
- **Instant queries.** Whether to run the profile pass there too, ahead of the runtime
  push-down, so the first operand is also narrowed when the second is selective (the
  runtime path only narrows the second).
