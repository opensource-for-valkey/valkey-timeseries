# Aggregators review

## Status (2026-11-07, branch `fix/aggregator-bug-sweep`, unpushed)

| Items | State |
|---|---|
| AGG-01, 02, 03 | Fixed. `bucket_start_for` is unsigned and shared; bucket ends are inclusive |
| AGG-14 | Closed without the 2^61 cap: `redis:8.10` accepts durations up to `i64::MAX`, and they are now safe. DIV-0062 records the reference's wrap (signed off 2026-11-07) |
| AGG-17 | Fixed. `irate`/`increase` rejected, as in the reference |
| AGG-10 | Fixed. Inlined; the 7-15 % is this review's measurement, not re-run |
| AGG-07, 15, 16, 18 | Fixed. `reducer_support()` is the one definition |
| AGG-19 | Partly fixed. `TryFrom` impls and `DerivAggregator` removed; `From<AggregationType>` still builds filtered aggregators from their match-everything `Default` |
| AGG-12 | Fixed (`b42f7012e`): the whole filtered family skips NaN |
| AGG-13 | Fixed (`79f33f7a7`): counter sum compensated; rate asserts its window |
| AGG-05 | Fixed (`fe3b3e5d4`): AVG reads a 2^-32 shadow sum only on overflow; matches `redis:8.10`. Same commit: partial var.p/std.p of one infinity = 0 |
| AGG-11 | Fixed (`c0955241b`): counters carry the last sample across buckets, compaction seeds and refreshes the successor bucket; lone first sample = NaN for all three |
| AGG-04 | Fixed: Welford relative to the first value (option A), no measurable cost |
| AGG-06 | Measured, declined for now (option B: shadow M2 at 2^-512, -2..+9 %, needs a wire field) |
| AGG-08 | Fixed (`64f7410aa`): SUM tracks occupancy apart from its value |
| AGG-09 | Fixed (`3db0b3f93`): AVG count is u64 |
| reset | Fixed (`9362187d7`): one reset per aggregator per bucket |
| AGG-19 remainder | Fixed (`a6838fd41`): `TryFrom<AggregationType>` refuses filtered types; the config `From` is test-only |
| boxing, COUNTIF | Fixed (`467c19664`): range, std/var, share, all/any/none inline, countif integer; sumif stays boxed (40 B). countif -70 %, all -41 %, share -37 %, var/std -12..-16 % |
| AGG-20 | Partly fixed (`e2a5d4c1a`): `accepted` is a u16 bitmap; the `last` carry mask in iterators/utils.rs left for perf/iterators' LastCarry |
| AGG-21 | Open, owned by the compaction work |
| size | `Aggregator` 32 -> 40 B, from the AVG shadow (32 B) and counter carry (40 B) states |

Reviewed 2026-11-06, starting at `d1adcd17a`. Scope: all seven files under
`src/aggregators`, with call-site checks in range iterators, compaction, and MRANGE fanout.
This is an analysis; no implementation changes were made. Existing concurrent changes to
iterator files and other review documents were left alone.

The implementation has useful foundations: constant-space online accumulators, compensated
summation, shared scalar/multi bucketing, lazy gap generation, and explicit persisted type IDs.
The main weaknesses are boundary arithmetic, disagreement between ordinary and partial
dispersion reducers, and contracts that the tests do not exercise independently.

P2 means a correctness issue worth fixing; P3 marks a narrower API, diagnostic, or extreme-scale
issue. Performance opportunities below are separate from these priorities.

| ID | Priority | Finding | Evidence |
|---|---|---|---|
| AGG-01 | P2 | EMPTY skips the first missing one-millisecond bucket | Release command reproduction |
| AGG-02 | P2 | The final representable bucket is split at `i64::MAX` | Release command reproduction; debug panic with EMPTY |
| AGG-03 | P2 | Modulo normalization overflows for valid large durations | Release command reproduction; debug panic |
| AGG-04 | P2 | Dispersion depends materially on reduction partitioning | Direct ordinary/partial reducer probes |
| AGG-05 | P2 | AVG overflows even when the answer is representable | Release command and direct reducer probes |
| AGG-06 | P2 | Dispersion overflows even when the answer is representable | Release command and direct reducer probes |
| AGG-07 | P3 | SUMIF has two incompatible `finalize` methods | Direct public API probe |
| AGG-08 | P3 | A nonempty zero SUM reports no current value | Release `TS.INFO DEBUG` reproduction |
| AGG-09 | P3 | AVG silently stops counting at `u32::MAX` | Source inspection; threshold not exercised |

AGG-10–AGG-16 were added by the second pass and AGG-17–AGG-21 by a third (2026-11-07), both
documented at the end of this file; each later pass re-verified the earlier citations.

## Correctness

**AGG-01 — EMPTY loses a bucket at duration 1.**
[complete_bucket](../../src/aggregators/aggregate_iterator.rs), lines 171–176, starts gap fill at
`bucket_range_end + 1`, although the end is already exclusive. At duration 1 that advances an
entire extra bucket. Samples `(0, 1)` and `(3, 2)` with `AGGREGATION sum 1 EMPTY` return
`[(0, 1), (2, 0), (3, 2)]`, omitting `(1, 0)`. Durations greater than 1 usually conceal this
because the extra millisecond still lands inside the first gap bucket. Start the run at the
exclusive bucket end. Cover scalar and multi output, including a single missing bucket.

**AGG-02 — A saturated end cannot represent the final bucket's exclusive boundary.**
[should_finalize_bucket/update_bucket_timestamps](../../src/aggregators/aggregate_iterator.rs),
lines 190–199, saturate the exclusive end to `i64::MAX`, then treat a sample equal to that value
as belonging to a new bucket. Samples `(9223372036854775806, 1)` and `(9223372036854775807, 2)`
with duration 10 return two rows at `9223372036854775800`, with values 1 and 2; the correct
bucket contains both and sums to 3. EMPTY also reaches the overflowing `end + 1` from AGG-01:
debug panics, while the release probe returns the same duplicate rows.

Represent the terminal bucket explicitly, use inclusive bounds, or retain an exclusive end in
a wider integer. Fixing the increment alone does not fix membership. Exercise `COUNT`, reverse
queries, and downstream grouping after the bucket-level regression is fixed: duplicate bucket
timestamps violate those consumers' assumptions.

**AGG-03 — Positive modulo normalization overflows inside the accepted timestamp domain.**
Both [bucket_start_for](../../src/aggregators/aggregate_iterator.rs), lines 223–230, and the public
[calc_bucket_start](../../src/aggregators/mod.rs), lines 378–385, use
`(diff % delta + delta) % delta`. The addition can overflow even when timestamps, alignment, and
duration are all valid nonnegative command inputs.

With alignment 0, duration `9223372036854775807`, and samples
`(9223372036854775805, 1)` / `(9223372036854775806, 2)`, the release command returns
`[(0, 1), (0, 2)]` instead of `[(0, 3)]`. Debug panics during bucket calculation. The public
helper also feeds compaction, so the arithmetic repair belongs in a shared primitive rather
than only in the query iterator. Use overflow-safe Euclidean remainder within the validated
domain, or wider arithmetic; preserve the intentional distinction between true negative starts
and normalized reply timestamps. Public Rust constructors also need an explicit nonzero,
representable-duration precondition or validation.

**AGG-04 — Ordinary and partial dispersion reducers disagree beyond small rounding noise.**
[AggStd](../../src/aggregators/handlers.rs), lines 555–573, and
[PartialReducer](../../src/aggregators/partial_reducer.rs), lines 193–203, 251–259, and 305–322,
duplicate the accumulator and finalization logic. Two probes expose different problems:

- `var.p` over `[1e16, 1e16 + 2]` is 2 in the ordinary handler and in one sequential partial,
  but 1 when merging two singleton partials. The exact population variance of these two
  representable inputs is 1. The intermediate mean rounds back to `1e16` in the sequential
  update, doubling M2. This is a 100% error, outside the existing partial-equivalence test's
  tolerance, rather than a harmless final-digit difference. A standalone `TS.RANGE` confirms 2.
- `var.p` and `std.p` over one infinity return 0 in the ordinary handler and NaN in the partial
  handler. The initial update produces NaN M2, but only `AggStd::variance()` replaces it with 0
  at count 1. Sample variants already special-case count 1 on both paths.

The direct reducer probes establish the mismatch; no live cluster comparison was run. Share
the numerical state and finalization policy, explicitly define infinity handling, and extend
the partition-invariance tests to large offsets and special values. Sharing code alone will
not repair the finite-input accuracy problem; that needs a more accurate mean/M2 strategy.

**AGG-05 — AVG's sum can overflow before division.**
[KahanAvg::value](../../src/aggregators/kahan.rs), lines 130–139, computes a compensated sum and
then divides by count. `[f64::MAX, f64::MAX]` returns infinity although the mean is `f64::MAX`.
The partial reducer uses the same sum-then-divide shape at
[finalize](../../src/aggregators/partial_reducer.rs), line 304, and has the same result. Confirmed
through `TS.RANGE ... AGGREGATION avg 10`. Compensated summation handles rounding loss but does
not provide additional exponent range. A scaled or overflow-aware average needs to work in
both update and merge paths while retaining the existing cancellation guarantees.

**AGG-06 — M2 can overflow before a representable variance or deviation is formed.**
[AggStd::add](../../src/aggregators/handlers.rs), lines 560–563, and the partial update/merge
form an unscaled squared-deviation numerator. `[1e154, -1e154]` returns infinity for both `var.p`
and `std.p`, although their answers are approximately `1e308` and `1e154`, both representable.
The population numerator is about `2e308`, which overflows before division by 2. Partial merging
also squares `delta` before applying the weight, adding another overflow opportunity.

Confirmed in release command replies and ordinary/partial probes. Consider scaled dispersion
state and overflow-aware merge ordering. Existing compatibility prose describing Welford as
returning the true value needs a narrower numerical guarantee even after the documented RTS
divergences are accounted for. No reference-server behavior is asserted here.

**AGG-07 — SUMIF's inherent finalizer bypasses the trait contract.**
[SumIfAggregator::finalize](../../src/aggregators/filtered.rs), lines 159–162, takes `&self` and
returns the current value without resetting. `AggregationHandler::finalize` takes `&mut self`,
returns the empty value when necessary, and resets. With one matching value 20, direct
`sum_if.finalize()` leaves `current() == Some(20)`; the trait call leaves `None`. With no matches,
the inherent method returns NaN while the trait method returns 0.

Production enum/UFCS calls use the trait contract, so this is a public API and testability
problem rather than a demonstrated command-path error. The existing
[test_sumif_aggregator_none_match](../../src/aggregators/handlers_tests.rs), lines 549–557,
actually pins the conflicting inherent behavior. Remove or rename the inherent method and
test concrete, generic-trait, and enum finalization for the same value/reset semantics.

**AGG-08 — SUM equates an arithmetic zero with an empty accumulator.**
[SumAggregator::current](../../src/aggregators/handlers.rs), lines 381–385, uses
[KahanSum::is_empty](../../src/aggregators/kahan.rs), lines 57–58, which tests whether both
numeric parts equal zero. One accepted zero, or exact cancellation, therefore returns `None`.
After creating a SUM compaction rule and adding `(0, 0)`, `TS.INFO src DEBUG` returns
`rulesState = [[dst, 0, 1, null, 0]]`: it reports samples present but no current aggregate.
Normal bucket finalization still returns 0 through `empty_value`, and LATEST clones/finalizes,
so those outputs are unaffected. Track occupancy independently of the numeric result; account
for persisted state when choosing the representation.

**AGG-09 — AVG's denominator saturates at 4,294,967,295.**
[KahanAvg](../../src/aggregators/kahan.rs), lines 119–139, keeps adding to the sum after its
`u32` count saturates. Streaming a sufficiently large bucket consequently inflates its mean.
The partial reducer uses a `u64` count, creating another partition-dependent threshold. The RDB
field is already saved as an unsigned 64-bit value, but load narrows it with `as u32`. Widen the
count and test a state immediately around the limit through a focused internal fixture. This
finding is from inspection; billions of updates were not executed.

## Performance and allocation patterns

**The clearest allocation opportunity is boxing states that already fit the enum.**
Measured on this arm64 build: `Aggregator` is 32 bytes; `AvgAggregator`, `IncreaseAggregator`,
`RangeAggregatorState`, and `AggStd` are each 24 bytes. Nevertheless,
[RangeAggregator](../../src/aggregators/handlers.rs), line 305, and the std/var family via
`OnlineAggregator`, line 614, heap-allocate their 24-byte states. Inlining a 24-byte payload has
room within the existing enum layout; verify `size_of` after the change. The allocation happens
per constructed/cloned aggregator, not per ordinary bucket reset. It matters for queries over
many short series, persistent compaction rules, historical recomputations, and LATEST reads
that clone a rule's aggregator. Preserve the serialized field order when changing storage.

**There are two different column-count allocation thresholds.**
[AggregationHelper](../../src/aggregators/aggregate_iterator.rs), lines 45–49, keeps two
aggregators and two acceptance flags inline; construction spills both vectors at three columns,
in addition to any boxed states. Output `MultiSample.values` keeps four values inline, so five
columns introduce a new allocation for every emitted row, including gap-fill rows. The partial
row reducer similarly allocates a state vector per output timestamp above four columns.

Capacity probes confirmed four inline slots at 1–4 columns, eight heap slots at 5 columns, and
sixteen at 16 columns. At five columns that means a 64-byte value buffer per produced bucket
and a 320-byte partial-state buffer per reduced timestamp, plus the containers and allocator
overhead. These are buffer capacities, not a heap-profile measurement. Streaming releases them
as rows are consumed; collecting outputs retains them. Tune inline capacities using actual
column-count distributions, since larger inline buffers increase every iterator/container.

An acceptance bitmap would remove the second construction allocation while making the bounded
column relationship explicit; the command parser caps aggregations at 16. Pair that with a
validated Rust options boundary so public constructors cannot violate the bound.

**The default scalar path does not allocate a value buffer per bucket.**
`AggregateIterator` projects the shared one-column multi iterator; its row fits inline. Numeric
state and gap cursors are bounded independently of sample count. Lazy `EmptyBucketRun` is a
useful design to retain. Reverse/COUNT materialization costs belong to the downstream iterator
pipeline already covered by the separate iterator review.

**There is redundant reset work at bucket boundaries.**
[complete_bucket](../../src/aggregators/aggregate_iterator.rs), lines 149–167, calls the trait
finalizer, which resets each aggregator, and then resets every aggregator again. Centralize
reset ownership while retaining the necessary reset for an unaccepted/EMPTY bucket. Expected
benefit is highest with many small buckets; no latency gain was measured.

**COUNTIF pays for compensated floating-point summation of integer counts.**
[ConditionalCountState::update_count](../../src/aggregators/filtered.rs) calls the out-of-line
Neumaier increment for every match and uses the same 40-byte boxed state as SUMIF. A dedicated
integer count plus the filter would simplify the hot loop and could fit in the current enum
payload. Check serialized-state compatibility before separating the shared state. Benchmark
this together with construction costs; neither allocation counts nor timing gains were measured.

**Partial rows are deliberately more expensive than finalized values.**
Measured `PartialState` is 40 bytes, `PartialReducer` 64, and `MultiSample` 56. Each partial row
reducer also retains four inline reducer states. These fixed costs buy mergeability and can
reduce fanout substantially for groups with many members. Include singleton and high-cardinality
groups in benchmarks to find where the larger partial payload offsets that saving.

## Clarity, maintainability, and testability

- Share dispersion state/update/merge/finalization between ordinary and partial reducers. A
  single count-one policy would have prevented the infinity mismatch; broader numerical tests
  are still required for finite-input accuracy.
- Document `AggregationHandler::update`'s boolean precisely. Acceptance, condition match, and
  availability of a result are different: SUMIF accepts a finite sample even when its condition
  fails; IRATE can return false while mutating reset state. Preserve the distinction between an
  empty bucket and an empty reduction group.
- Consolidate the bucket-grid arithmetic, while making normalization an explicit separate
  operation. State the ascending-input, positive-duration, and nonempty-aggregation invariants
  on public iterators, and enforce structural options invariants once at construction.
- Define whether `AggregationType::is_groupable` means command eligibility: it rejects filtered
  types and permits FIRST/LAST/RATE, while the command parser accepts filtered reducers and
  rejects FIRST/LAST/RATE. The method currently has no call sites, making this maintenance drift
  rather than a demonstrated command failure.
- The partial-equivalence tests use small values in `[-10, 10)` plus NaN, and therefore miss both
  large-offset and infinity failures. Add deterministic numerical cases and partition/permutation
  properties, with an equality helper that explicitly handles signed infinity as well as NaN.
- Scalar/multi equivalence is useful for projection and column ordering, but both paths run the
  same bucket engine. Add an independent small `i128` bucket oracle over durations 1, 2, odd
  durations, large durations, offsets, and timestamp boundaries.
- Check finalize/reset/reuse for every concrete handler through the trait and enum. In
  particular, test zero sums, no conditional matches, counter resets, repeated timestamps, and
  all-NaN buckets alongside ordinary values. Add direct ALL/ANY/NONE unit coverage.
- Keep the existing RDB/reload integration coverage; extend boundary-state fixtures for the
  widened AVG count and any accumulator representation changes. Use repository fixture helpers
  for generated datasets and small explicit fixtures for arithmetic counterexamples.
- Remove stale explanations and disabled tests: RANGE's section is labelled FIRST; RATE's
  comment describes a sliding timestamp window it does not store; SHARE's comment excludes 1
  although 1 is valid; the empty-input test retains unrelated commented assertions. Dispersion
  comments overstate numerical guarantees demonstrated false above.

Suggested order: repair AGG-01–03 and add independent boundary tests; align and improve numerical
reducers (AGG-04–06); fix the API/diagnostic contracts and count width; then remove unnecessary
boxes and measure construction, small buckets, and 1/2/3/4/5/16-column workloads. Keep performance
changes separate enough to identify their effect.

## Validation and limits

- `cargo test --offline --lib --features enable-system-alloc aggregators::`: **120 passed**.
- `cargo build --offline --release --features enable-system-alloc`: passed.
- Temporary probes linked the repository's actual aggregator implementations in debug and
  release. They exercised all reproduced findings above, except the inspection-only count limit,
  and measured type sizes; output-vector capacities were checked in debug.
- Release command probes used the local `tests/build/binaries/unstable/valkey-server`, one
  isolated standalone process, a temporary Unix socket, and no persistence. They confirmed
  AGG-01–03, AGG-05–06, AGG-08, and AGG-04's standalone finite-input result.
- No reference server or RedisTimeSeries source/tests were consulted. No live cluster probe,
  full integration/compatibility suite, ASAN pass, heap profile, allocation counter, or comparative
  latency benchmark was completed. Numerical expected values are derived from the stated inputs,
  not from reference-server behavior. Changes to shared command behavior still need the normal
  compatibility checks when implemented.

## Second pass: AGG-10 – AGG-16

Reviewed later on 2026-11-06 against `8d19c65d4`, independently of the pass above: same scope,
same constraints (no reference server, no RedisTimeSeries source or tests), and no implementation
changes. Method: full re-read of all seven files, call-site search for every public item,
`cargo test --offline --lib --features enable-system-alloc aggregators::` (**120 passed**),
release-module command probes against a standalone server loaded with the freshly built
`target/release` module (1,000,000 deterministic samples), and one controlled two-build A/B for
AGG-10.

The three P2 boundary findings of the first pass were reproduced before this pass started — AGG-01
returns `[(0, 1), (2, 0), (3, 2)]`, AGG-02 returns two rows at `9223372036854775800`, and AGG-03
returns `[(0, 1), (0, 2)]` — so they stand as written. Everything below is additional. Line numbers
are as of `8d19c65d4`, the revision everything here was measured on; `aggregate_iterator.rs` was
being restructured in the working tree while this was written, so citations into that file may have
moved by a reader's checkout.

| ID | Priority | Finding | Evidence |
|---|---|---|---|
| AGG-10 | P2 | `#[inline(never)]` on the accumulator increment costs 7–15% of aggregation throughput; inlining is byte-identical | Two release builds, 1M samples, 3× min-of-30 per build |
| AGG-11 | P2 | The three counter aggregators disagree on a bucket with one sample and no baseline, and per-bucket reset discards every cross-bucket delta | Release command reproduction |
| AGG-12 | P2 | `countif`/`sumif` skip NaN while `share`/`all`/`any`/`none` let NaN satisfy the condition; the comment claiming uniform skipping is false | Release command reproduction |
| AGG-13 | P3 | The counter accumulator is the module's only uncompensated `f64` sum, and `RateAggregator` silently reports nothing when its window was never set | Source inspection |
| AGG-14 | P3 | The `AGGREGATION` bucket duration accepts any positive `i64` although the bucket arithmetic is only valid to 2^61 | Release command reproduction |
| AGG-15 | P3 | Four public items have no production caller, and one comment says the opposite | Call-site search |
| AGG-16 | P3 | A unit test asserts the shadowed `SumIfAggregator::finalize`, i.e. a NaN that production never returns | Direct probe through four call paths |

### Correctness

**AGG-11 — counter aggregators disagree on a single-sample bucket.**
[IncreaseAggregator](../../src/aggregators/handlers.rs), line 819,
[RateAggregator](../../src/aggregators/handlers.rs), line 863, and
[IRateAggregator](../../src/aggregators/handlers.rs), line 1025 all wrap
`CounterAggregatorState`, but they disagree whenever a bucket holds one sample and no earlier
baseline exists. [IRateAggregator::current](../../src/aggregators/handlers.rs), line 1050, returns
`None` when `dt == 0`, which finalizes to NaN; `rate` returns `0.0` because `sum_deltas` is zero and
the window is positive; `increase` returns `0.0` for the same reason. Series `1..9` with
`AGGREGATION irate 3`, `rate 3`, `increase 3`:

```
irate    1000, 1000, 1000, nan
rate     333.333…, 666.667…, 666.667…, 0
increase 2, 2, 2, 0
```

The same reset also drops every cross-bucket delta: the true increase of `1..9` is 8, the per-bucket
`increase` values sum to 6, because each bucket restarts `last_value` at its own first sample. The
module already solved the analogous carry problem for `last` with `CarryLastEmpty`
(`src/iterators/utils.rs`; an in-progress working-tree change relocates it to `LastCarry` in
`src/aggregators/aggregate_iterator.rs`); the counter family has no counterpart, and neither
behavior is documented in `docs/commands/`. These are extension aggregators, so this needs a
decision (carry the boundary delta, or state the per-bucket convention) rather than a reference
check; either way the three should agree on the no-baseline case.

**AGG-12 — NaN has no single policy across the filtered family.**
[ConditionalCountState::update_sum](../../src/aggregators/filtered.rs), line 67, opens with "NaN is
skipped, as every other aggregator skips it (and as the cluster push-down's partial reducer
does)". The first clause is false. [ShareAggregatorState::update](../../src/aggregators/filtered.rs),
line 205, increments `count` unconditionally and passes the value to the filter, and
[AllNoneAggregatorState](../../src/aggregators/filtered.rs), line 313, does the same, so NaN reaches
`ValueComparisonFilter`, where `op_neq(NaN, x)` is `true` and `op_eq(NaN, NaN)` is `true`. Over
`[NaN, 10, NaN, 20]`:

```
countif!=5 → 2      count → 2      countall → 4
share!=5   → 1.0    all!=5 → 1     any!=5 → 1     none!=5 → 0
```

`share!=5` therefore reports 100% where `countif!=5 / 4` reports 50%, and `all`/`any`/`none` are
answering a NaN-aware `!=` that no other aggregator applies. The push-down justification only
covers `countif`/`sumif`: `PartialReducer::for_config` rejects the other four, so extending the skip
to them would not create a single-node/cluster mismatch. The existing `share` NaN test
(`test_share_aggregator_with_nan`) only pins `>`, where NaN happens not to match, so the `!=`/`==`
asymmetry is untested. Pick one policy for all six, then correct the comment.

### Performance

**AGG-10 — the compensated increment is de-optimized on purpose.**
[kahan.rs](../../src/aggregators/kahan.rs), line 171, marks `kahan_inc` as `#[inline(never)]` with
"Important: do NOT inline. Compiler reordering of floating-point operations can cause precision
loss", justified by a Prometheus (Go) issue. That justification does not transfer: Rust has no
`fast-math`, so LLVM may not reassociate these operations whether the function is inlined or not.
Rebuilding the same tree twice and running the same script against each build (1000-sample buckets
over 1,000,000 samples, best of 30 per query, then repeated three times per build):

| Query | `#[inline(never)]` | `#[inline(always)]` |
|---|---|---|
| `AGGREGATION sum 1000` | 16.09 / 16.88 / 17.30 ms | 14.99 / 15.46 / 15.04 ms |
| `AGGREGATION sum,avg,count 1000` | 22.08 / 22.81 / 22.44 ms | 19.31 / 19.36 / 19.34 ms |

Nine probes — `sum`, `avg`, `sumif`, `std.s`, `share`, `all`, `irate`, `increase`, and a
five-column row — produced identical result digests under both builds, so this is not a
compare-and-accept trade. `count` also moved (15.48–16.61 ms to 14.41–15.25 ms) even though it
never calls `kahan_inc`, so part of the delta is code-layout noise; the direction is consistent
across all three runs of each build, and the largest gain lands on the workloads that actually
traverse the increment. Fold this into the existing compensated-summation work (the COUNTIF item
above) rather than treating it separately.

### Numerical consistency and input validation

**AGG-13 — the counter accumulator is the only uncompensated sum.**
[handlers.rs](../../src/aggregators/handlers.rs), line 806, accumulates `sum_deltas += delta` in
plain `f64` while `sum`, `avg`, `sumif`, and the dispersion family all use Neumaier compensation.
`rate` divides that drifted sum by the window, so the longest-window counter queries are the ones
with the most accumulated error. The surrounding comment, line 860, describes "a sliding time
window of these deltas and their timestamps" that the state does not keep; the implementation is a
single per-bucket accumulator divided by `window_ms`. Related trap: `RateAggregator::default()` has
`window_ms == 0` and [current](../../src/aggregators/handlers.rs), line 919, then returns `None`
unconditionally. Only `AggregationHelper::new` and the compaction rule builder patch the window in,
so a future call site that forgets gets a silent NaN instead of a diagnosable error. Consider
making the window a constructor argument with no `Default`, or asserting it at first update.

**AGG-14 — the accepted duration domain is wider than the arithmetic.**
[parse_bucket_duration_str](../../src/commands/command_parser.rs), line 168, takes a bare integer
through [bucket_duration_from_millis](../../src/commands/command_parser.rs), line 180, which rejects
only `<= 0`; the suffixed form (`1000ms`) goes through `validate_duration`
(`src/parser/duration.rs`) and is capped at 2^61. Every consumer of the duration then computes
`(diff % delta + delta) % delta`, which overflows above that cap — silently wrapping in release,
because `Cargo.toml` defines no `[profile]` section and therefore leaves `overflow-checks` off, and
panicking in debug. Both [bucket_start_for](../../src/aggregators/aggregate_iterator.rs), line 223,
and the public [calc_bucket_start](../../src/aggregators/mod.rs), line 378, are affected, and the
latter also feeds compaction. With samples at `9223372036854775805` and `9223372036854775806`:

```
bucket duration 2^61        → [6917529027641081856, 3]   correct
bucket duration 2^61 + 1    → [6917529027641081859, 3]
bucket duration i64::MAX    → [(0, 1), (0, 2)]           two rows in one bucket
```

`TS.CREATERULE src dst AGGREGATION sum 9223372036854775807` is likewise accepted (the destination
then simply never publishes). AGG-03 is the arithmetic symptom; this is the reachable input path,
so the bound belongs at the parse boundary *as well as* in the shared overflow-safe helper that
AGG-03 asks for.

### Clarity, maintainability, and testability

**AGG-15 — public items with no production caller, one of them documented as live.**
`AggregationType::is_decomposable`
([mod.rs](../../src/aggregators/mod.rs), line 253) is called only from
`test_is_decomposable_agrees_with_for_config`, yet the comment at
[partial_reducer.rs](../../src/aggregators/partial_reducer.rs), line 642, describes it as "the
coordinator's cheap eligibility test" — the coordinator calls `PartialReducer::for_config`
(`src/commands/ts_mrange_fanout_command.rs`). Also uncalled in production:
`AggregationType::apply_reverse_adjusted` ([mod.rs](../../src/aggregators/mod.rs), line 230),
`KahanSum::incr` ([kahan.rs](../../src/aggregators/kahan.rs), line 28), and
`AggregationHandler::empty_bucket_value` ([handlers.rs](../../src/aggregators/handlers.rs), line 33)
— a hook declared once, used once, and overridden by no handler. This is the same drift the first
pass records for `is_groupable`: either wire them up or delete them, and fix the comment that
promises a caller.

**AGG-16 — a unit test pins the shadowed finalizer.**
AGG-07 above records that [SumIfAggregator::finalize](../../src/aggregators/filtered.rs), line 160,
shadows the trait method with different semantics. What that pass does not note is that
`test_sumif_aggregator_none_match` ([handlers_tests.rs](../../src/aggregators/handlers_tests.rs),
line 550) asserts through the *inherent* one, so the NaN it pins is a value the engine never
produces. Probed through every path with a filter that matches nothing:

```
inherent finalize = NaN   trait finalize = 0   enum-dispatched = 0
bucket path = [Sample { timestamp: 0, value: 0.0 }]
```

Deleting the inherent method therefore breaks a test that looks like coverage of the real
behavior; it must be rewritten to assert `0.0` through `AggregationHandler::finalize` in the same
change. Also worth adding alongside it: direct first/zero/accepted-but-unmatched/reset cases for
`All`, `Any`, and `None`, which have no unit tests at all today.

### Suggested order (combined)

Repair AGG-01–03 and AGG-14 together as one boundary change (validate the duration at the parse
boundary, then share one overflow-safe grid helper), since the first pass's three arithmetic
findings and this pass's reachable input path are the same defect seen from two ends — that also
collapses the `calc_bucket_start`/`bucket_start_for` duplication the first pass asks to consolidate.
Then AGG-10, which is one line plus a rebuild and is already measured. Then the numerical and
semantic decisions (AGG-04–06, AGG-11, AGG-12, AGG-13), which need agreement on behavior rather than
a reference comparison. Then the API and test cleanup (AGG-07, AGG-15, AGG-16, plus the boxed-state
and column-capacity work). Keep the performance changes separate enough to identify their effect.

### Validation and limits (second pass)

- `cargo test --offline --lib --features enable-system-alloc aggregators::`: **120 passed**.
- `cargo build --offline --release --features enable-system-alloc`: passed, twice (the AGG-10 A/B).
- Command probes used a standalone `valkey-server 9.1.2` with the freshly built release module, a
  TCP port, `--save '' --appendonly no`, and deterministic payloads loaded through `TS.ADDBULK`.
  They confirmed AGG-01, AGG-02, AGG-03, AGG-11, AGG-12, and AGG-14.
- AGG-10 is a comparative latency measurement of the module's own query path, not a benchmark of
  the repository's bench suite; it was not repeated on a quiet machine, and the `count` row shows
  that part of the delta is code-layout rather than arithmetic.
- AGG-13, AGG-15, and AGG-16 are source and call-site findings; AGG-16's four-path probe was run as
  a temporary in-crate test and reverted.
- No reference server, RedisTimeSeries source or tests, cluster, ASAN pass, heap profile, or
  allocation counter was used. Nothing above is a compatibility claim, and the AGG-11/AGG-12
  behavior is this module's own extension surface, not shared RTS surface.

## Third pass: AGG-17 – AGG-21

Reviewed 2026-11-07 against `615f0042c`, the commit that lands the `tail_limit`/`skip_to_last`
change the second pass saw only in the working tree. Same scope and constraints as both passes
above: all seven files re-read in full, a call-site search for every public item,
`cargo test --offline --lib --features enable-system-alloc aggregators::` (**123 passed**; the three
`EmptyBucketRun::skip_to_last` tests are the additions since the second pass), and a temporary
in-crate probe module — removed afterwards, `mod.rs` restored from git — that drove the
repository's own `SampleReducer`, `IRateAggregator`, `Aggregator::try_from`, and the EMPTY/`last`
iterator, and measured type sizes. No reference server, RedisTimeSeries source or tests, live
server, cluster, ASAN pass, heap profile, or benchmark was used. Nothing below is a compatibility
claim.

### What still stands from the first two passes

Every code citation was re-checked at `615f0042c`; none of the sixteen findings has been
addressed:

- AGG-01: `bucket_range_end + 1` is still at
  [aggregate_iterator.rs:300](../../src/aggregators/aggregate_iterator.rs). The trailing fill in
  `finalize_last_bucket_if_any` (line 618) starts at `bucket_range_end` without the `+ 1`, so the
  two fill sites in the same file disagree with each other — a useful cross-check for the fix.
- AGG-02/03/14: `update_bucket_timestamps` (lines 320–325) still saturates;
  `bucket_start_for` (line 355) and `calc_bucket_start` (mod.rs, line 385) still compute
  `(diff % delta + delta) % delta`.
- AGG-10: `#[inline(never)]` is still on `kahan_inc` (kahan.rs, line 171).
- AGG-15/16: `is_groupable` (mod.rs, line 203) still has no caller;
  `test_sumif_aggregator_none_match` (handlers_tests.rs, line 557) still asserts NaN.
- AGG-04–09, AGG-11–13: the cited code in `handlers.rs`, `kahan.rs`, `filtered.rs`, and
  `partial_reducer.rs` is unchanged.

The committed `tail_limit` change itself was reviewed: `skip_to_last` is `i128`-safe over the
whole timestamp range (its test pins `i64::MIN..i64::MAX`), and the premise it rests on — every gap
in one run reports the same `last` value — holds, because `apply_to_gap` is constant within a run
after its first call (seed or held before the first real bucket, `lookback` thereafter, with the
forward-mode `held = lookback` side effect taken by whichever gap happens to be popped first). The
skip is re-evaluated on every pop, an `i128` division per gap row, but only while a reverse COUNT
is in force; not worth measuring.

| ID | Priority | Finding | Evidence |
|---|---|---|---|
| AGG-17 | P2 | `REDUCE irate` is NaN for every bucket and `REDUCE increase` depends on member order; parser and docs admit both | Direct `SampleReducer` probe |
| AGG-18 | P3 | Reducer eligibility is defined in four places with four different sets; two of them describe unreachable code | Source and call-site inspection |
| AGG-19 | P3 | A filtered aggregator built without a condition silently gets `!= NaN` and matches everything; `DerivAggregator` is a dead stub | Direct probe; call-site search |
| AGG-20 | P3 | The bucket iterator is 368 bytes per instance, 48 of them holding at most 32 booleans | `size_of` probe |
| AGG-21 | P3 | `DEBUG DIGEST` hashes the open bucket's accumulator bits, so nodes with identical visible data can disagree | Source inspection only |

### Correctness

**AGG-17 — the counter aggregators are accepted as GROUPBY reducers but cannot work as one.**
[is_valid_reducer](../../src/commands/command_parser.rs), line 658, rejects only `first`, `last`,
and `rate`, and [docs/commands/ts.mrange.md](../commands/ts.mrange.md), line 250, promises
"Supports all aggregators except `rate`". A reducer sees every member's value at *one* timestamp,
and the two counter aggregators cannot do anything sensible with that:

- `irate`: [update_irate](../../src/aggregators/handlers.rs), line 966, computes `dt = 0` for the
  second member and never records a rate, so `current()` (line 1052) returns `None` and
  `SampleReducer` finalizes it to NaN. Probed through the production reducer:
  `[1, 5, 3] → NaN`, `[7] → NaN`. Every bucket of a `REDUCE irate` query is NaN — single-node and
  cluster alike, since `PartialReducer::for_config` returns `None` and the coordinator falls back
  to the same reducer.
- `increase`: [CounterAggregatorState::update](../../src/aggregators/handlers.rs), line 805, sums
  the positive deltas between consecutive members in arrival order. Probed: `[1, 5, 3] → 4`,
  `[5, 3, 1] → 0`, `[1, 3, 5] → 4`. The reply depends on the order in which the matching series
  are iterated, which is a key-order artifact rather than a property of the data.

The same `dt = 0` path also discards a previously valid rate when a non-advancing timestamp
arrives — `(0, 100), (1000, 110)` reports 10, then `(1000, 120)` reports `None` — which a series
never produces but a reducer always does. Reject `irate` and `increase` in `is_valid_reducer` with
the existing "Invalid reducer type" error, correct the MRANGE document, and add a parser test that
pins the rejected set. These are extension aggregators, so this is the module's own decision, not
a reference check; AGG-11 is the per-bucket sibling of this finding.

### Clarity, maintainability, and testability

**AGG-18 — four definitions of "valid reducer".**

| Where | Set |
|---|---|
| parser `is_valid_reducer` ([command_parser.rs](../../src/commands/command_parser.rs), line 658) | everything but `first`/`last`/`rate`: 20 types, including `irate`, `increase`, `all`/`any`/`none`/`share`, `countif`/`sumif` |
| `AggregationType::is_decomposable` ([mod.rs](../../src/aggregators/mod.rs), line 253) and `PartialReducer::for_config` ([partial_reducer.rs](../../src/aggregators/partial_reducer.rs), line 101) | 16 types, including `first`/`last`; excluding the four non-counting filtered types and all three counters |
| `AggregationType::is_groupable` ([mod.rs](../../src/aggregators/mod.rs), line 203) | `!is_filtered()`: 17 types, including `first`/`last`/`rate`/`irate`/`increase`, excluding all six filtered types |
| [docs/commands/ts.mrange.md](../commands/ts.mrange.md), line 250 | "all aggregators except `rate`" |

Consequences. `PartialReducerKind::First` and `Last` — their `update`, `merge`, and `finalize`
arms, and the `decomposable_configs()` entries that test them — are unreachable from any command,
because the parser rejects `first`/`last` as reducers; the partition-invariance tests pin dead
paths. The `is_decomposable` doc comment ("`rate` and the filtered types are not accepted as
reducers at all") is wrong twice: `countif`/`sumif` are accepted and decomposable, and
`all`/`any`/`none`/`share` are accepted and fall back. `is_groupable` has no caller (first pass)
and disagrees with the parser on nine types. One `AggregationType::reducer_kind()` returning
`Invalid | Decomposable | FallbackOnly`, consumed by the parser, by `for_config`, and by a docs
test, would collapse all four; AGG-17 is the behavioral correction that collapse forces.

**AGG-19 — defaults that match everything, and a dead stub.**
`Aggregator::try_from(&str)` / `TryFrom<&ValkeyString>`
([handlers.rs](../../src/aggregators/handlers.rs), lines 1098–1115) and
`From<AggregationType> for Aggregator` (line 1117) build the filtered aggregators from their
`Default`, whose condition is `ValueComparisonFilter::default()` =
`!= NaN` ([request_types.rs](../../src/series/request_types.rs), line 100). Probed:
`Aggregator::try_from("countif")` over `[1, 5, NaN, -3]` reports 3, and
`AggregationType::Share.into()` over `[1, 5, NaN]` reports `0.667`. Neither `TryFrom` has a
production caller; `From` is reached in production only through `create_aggregator`'s
`_ => aggr_type.into()` arm for unfiltered types, and otherwise through `bench_rule` and the
compaction tests' `fresh_rule`. `AggregatorConfig::new` is the only constructor that enforces
"filtered needs a condition". Delete the two `TryFrom` impls (AGG-15's list grows by two) or route
them through `AggregatorConfig::new`, and make the filtered `Default`s unreachable from
non-test code.

Also dead: `pub struct DerivAggregator(IRateAggregatorState)`
([handlers.rs](../../src/aggregators/handlers.rs), line 1062) has no impls, is not an `Aggregator`
variant, and has no caller; it escapes `dead_code` only because `pub use handlers::*` re-exports
it. Delete it.

Same-name shadowing as AGG-07, in its benign form: `impl_conditional_aggregator!`
([filtered.rs](../../src/aggregators/filtered.rs), line 473) and `ShareAggregator` (line 269)
define inherent `reset` methods that shadow `AggregationHandler::reset` with identical bodies.
Harmless today; the trait method is the one that should exist.

### Performance and allocation

**AGG-20 — iterator footprint.**
Measured on this arm64 build: `MultiAggregateIterator<vec::IntoIter<Sample>>` is 368 bytes. Of
that, `EmptyFillBounds` is 72, `SmallVec<[Aggregator; 2]>` 80, and the two `SmallVec<[bool; 2]>`
(`AggregationHelper::accepted`, `LastCarry::columns`) 48 — for what is at most `MAX_AGGREGATIONS`
= 16 booleans each, i.e. a `u16` bitmap apiece. `Aggregator` is 32, `PartialReducer` 64,
`PartialState` 40, `MultiSample` 56, agreeing with the first pass. This is per iterator, not per
sample: one instance per series in an MRANGE, so 10,000 matching series alive at once would be
roughly 3.7 MB of iterator state before any boxed aggregator state. The first pass's bitmap item
covers the 48 bytes; the rest is the `EmptyFillBounds` copy and the inline aggregator slots and is
fine. No latency effect was measured.

A note on the first pass's boxing item: the boxed states also cost an indirection on every
`update` along the std/var/range/rate/irate/filtered paths, not only an allocation per
construction; unboxing removes both, and the per-sample half is the one a benchmark would see.

### Observability

**AGG-21 — accumulator bits in the digest.**
[calc_aggregator_digest](../../src/series/digest.rs), line 9, hashes a rule's `Aggregator`
through `Hash`, i.e. the open bucket's exact accumulator bits — Kahan `sum` and `compensation`,
Welford `mean`/`m2`, `IRate` `dt`/`dv`. `DEBUG DIGEST` therefore agrees across nodes only when
their accumulators were fed the same values in the same order. The ordinary paths do agree: the
engine replays out-of-order writes through recalculation in timestamp order
([engine.rs](../../src/series/compaction/engine.rs), lines 203–215), and
`rebuild_open_bucket_as_of` ([compaction/mod.rs](../../src/series/compaction/mod.rs), line 156)
also streams in timestamp order. The exposure is a node whose rule went `dirty` (node-local by
design) and rebuilt, against one that streamed: "exact for every aggregator whose bucket value
depends only on the bucket's samples" (that function's comment) is true of the value, not of the
compensation bits. Inspection only, not reproduced, and the compaction work owns any fix — hashing
the finalized value of the open bucket, or nothing of it, instead of its state.

### Test gaps (adds to AGG-16 and the first pass's list)

- `test_multi_single_column_equivalence`
  ([aggregate_iterator.rs](../../src/aggregators/aggregate_iterator.rs), line 1294) omits the six
  filtered types, so the `accepted`-flag and EMPTY-fill behavior of
  `all`/`any`/`none`/`share`/`countif`/`sumif` in the bucket engine has no equivalence check —
  exactly where AGG-12's NaN asymmetry lives.
- No parser test pins which reducers are rejected (AGG-17/18); `tests/test_ts_mrange.py`
  exercises `countif` as a reducer and nothing else beyond the reference set.
- Reverse `last` carry and the `pending`/`ready` queue in `next_resolved_row` are covered by one
  test outside the module (`iterators::utils::tests::test_reverse_last_empty_fill_around_stored_nan`);
  nothing in this module runs with `carry_reverse = true`, and `tail_limit` is tested only on
  `EmptyBucketRun` directly, never through the iterator together with the carry.
- `aggregation_type_from_u8_conversion` ([mod.rs](../../src/aggregators/mod.rs), line 664) is a
  23-assertion duplicate of `aggregation_type_u8_round_trips` immediately above it.

### Suggested order (updated)

Add AGG-17 to the first batch: it is a one-line parser change, a documentation line, and a test,
and it should land before anything is built on the counter reducers. AGG-18 and AGG-19 join the
AGG-15/16 cleanup. AGG-20 folds into the first pass's bitmap item. AGG-21 is a note for the
compaction work, not an aggregator change. Everything else in the combined order above stands.

### Validation and limits (third pass)

- `cargo test --offline --lib --features enable-system-alloc aggregators::`: **123 passed**.
- Probes ran as a temporary `zz_probe` module with `--nocapture`, once, and were removed; `mod.rs`
  was restored from git. This pass changes no tracked file.
- A concurrent session committed `615f0042c` while this pass was running; every citation was
  re-checked against that revision afterwards.
- AGG-17 was not run as a live command: the evidence is the production `SampleReducer` driven
  directly with the production `Aggregator`. Parser acceptance is read from `is_valid_reducer`,
  and the cluster fallback from `for_config` returning `None`.
- AGG-21 is inspection only. No digest was computed on two nodes.
