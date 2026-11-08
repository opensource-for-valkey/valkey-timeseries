# Iterator review findings and filtering adapter sketch

**Date:** 2026-11-06  
**Status:** Findings consolidated from three review passes. ITER-01 through ITER-06, ITER-09 through ITER-18, ITER-20, and ITER-21 are fixed on
`perf/iterators`, and
ITER-07's FILTER_BY_TS cost is fixed. No other fix and no adapter has been implemented.  
**Scope:** `src/iterators`, its range-query and cluster-coordinator callers, chunk readers, and
relevant tests.

The review examined correctness, performance, allocation and memory pressure, clarity,
maintainability, and testability. Its three passes are consolidated here:

- **Pass 1 (probe-driven)** timed and reproduced defects inside the readers and the range pipeline:
  ITER-01 through ITER-07.
- **Pass 2 (source and caller tracing)** read every file in `src/iterators` and searched for
  callers: ITER-08 through ITER-13.
- **Pass 3 (independent re-read and execution)** read the module again without relying on the
  earlier conclusions, then ran it: real commands against a standalone server and a three-primary
  cluster using the release module, a compiler dead-code probe, and a release-profile Rust probe. It
  added ITER-14 through ITER-20, replaced the debug timings with release timings, and corrected
  several earlier claims (next section).

Current tally:

| Priority | Findings |
| --- | --- |


| P3 | ITER-07 (value-filter remainder) |

| Withdrawn | ITER-08 (not a behavior defect), ITER-19 (matches the reference) |
| Fixed | ITER-01, ITER-02, ITER-03, ITER-04, ITER-05, ITER-06, ITER-09, ITER-10, ITER-11, ITER-12, ITER-13, ITER-14, ITER-15, ITER-16, ITER-17, ITER-18, ITER-20, ITER-21 |

The review used this repository, a running server, and local Rust probes. No RedisTimeSeries source
or test code was consulted. The review itself did no reference comparison; ITER-17, ITER-19, and
ITER-20 were afterward probed black-box against the pinned `redis:8.10` image, and the compat suite
now covers ITER-17 and ITER-20.

## Corrections to earlier passes

These claims from passes 1 and 2 did not survive pass 3. The sections below already reflect the
corrected versions.

| Earlier claim | What pass 3 found |
| --- | --- |
| ITER-08: `TS.ADD ... ON_DUPLICATE max` with NaN replies differently when it triggers a chunk rebuild | `TS.ADD` never takes the merge path, and the direct path collapses the same errors to `Duplicate`. Replies and stored state matched in every probe. Withdrawn. |
| ITER-08: fix by translating only `TsdbError::DuplicateSample` into `blocked` | That could not work: the NaN-policy error is also `DuplicateSample`. |
| ITER-10: `ReduceIterator` has no production caller and can be deleted outright | It is live on the cluster coordinator for GROUPBY without AGGREGATION. It can be *replaced*, not just deleted. |
| ITER-10: `filtered_iter` is referenced only by its own test | `benches/query_scan.rs:148` calls it, so deletion has to rewrite that bench. |
| `SampleReducer` is "used in eleven places" | Two production call sites plus one test elsewhere; the rest are its own tests. |
| ITER-07: EMPTY setup costs up to four full series scans, recomputed per pipeline | The three `has_passing_sample` windows are disjoint, so the total is about one pass. Only one pipeline runs per series. Measured at about 12–14 ns per stored sample. |
| ITER-13: narrowing the globs exposes `finalize_row_iterator`, `BucketValues`, `last_carry_mask`, `CarryLastEmpty`, `ReverseIter` | All five are `pub(crate)`; the glob cannot publish them. Only `TimestampFilter` of that list is public. |
| ITER-13: going first makes the compiler point at dead items | With `iterators` made `pub(crate)`, rustc flagged only `group_reduce`. `pub mod series` still pins the rest. |
| Mixed-encoding chunks arise from "a partial conversion, a restore, or an ASM import" | No conversion path exists. `chunk_encoding` is set at creation and restored unchanged; no producer of a mixed series was found. |
| Range-iterator tests build nineteen `RangeOptions` literals | Twenty. |
| Pass 3's own ITER-07 suggestion: return early for FILTER_BY_TS entries outside the window | The reference probe for ITER-20 showed those entries matter. Sparse lookups replaced the early return. |
| Pass 3's own ITER-17 suggestion: one predicate skipping NaN in the seed | It fixed the common shapes only. Stored NaN also changes the EMPTY extent for every aggregator, and `last` differs by direction. |
| Code comments: `last` carries the same way in both directions | True without stored NaN only. |

## Findings

| ID | Priority | Finding | Evidence |
| --- | --- | --- | --- |




| ITER-07 | P3 | EMPTY setup eagerly scans outside the window under FILTER_BY_VALUE | Release: 27.5 ms vs ~0.09 ms; FILTER_BY_TS cost fixed |














| ITER-08 | — | Withdrawn: duplicate-policy collapse is not observable | Reproduced on both encodings |
| ITER-19 | — | Withdrawn: LATEST at timestamp 0 on an empty destination matches the reference | Reference probe |
| ITER-14 | Fixed | A `FILTER_BY_TS` clipped to empty returned the whole window on a cluster | Reproduced, then pinned by unit and cluster tests |
| ITER-16 | Fixed | The k-way merges used a slower heap than they needed | Server A/B: raw GROUPBY and TS.NRANGE CPU 0.32–0.65× |
| ITER-17 | Fixed | Stored NaN counted as EMPTY data, and `last` mishandled it in both directions | Model and module match the reference on 10,000 random cases |
| ITER-02 | Fixed | Missing timestamps restarted forward compressed decoding | Server A/B: 128 misses 0.027× |
| ITER-03 | Fixed | Timestamp filtering did not skip empty chunks | Reproduced for all three encodings, then pinned |
| ITER-04 | Fixed | EMPTY aggregation exceeded the advertised size-hint upper bound | Failing test: bound 2, actual 11 |
| ITER-09 | Fixed | `chain_latest`'s direction flag was misnamed at both call sites | Pins and two mutations |
| ITER-11 | Fixed | LATEST resolution panicked on a missing context, blocking its unit tests | Unit test without a context |
| ITER-05 | Fixed | FilteredSampleIterator assumed sorted filters and ascending input | Deleted |
| ITER-10 | Fixed | Dead iterator surfaces, including an `Iterator` impl that lost data | Deleted; live reducer route pinned first |
| ITER-12 | Fixed | Three copies of the heap-entry ordering, two identical k-way merges | Merged; server A/B flat |
| ITER-13 | Fixed | Glob re-exports published iterator internals as crate API | Explicit exports |
| ITER-01 | Fixed | Reverse EMPTY queries enumerated every gap bucket despite a small COUNT | Server A/B: span 10⁸, 1,070 ms to 0.17 ms |
| ITER-15 | Fixed | Reverse aggregated COUNT decoded the entire range | Server A/B: 36.5 ms to 0.15 ms on 2 M samples |
| ITER-18 | Fixed | The raw coordinator path lost EMPTY's edge fill and seeds | Failing cluster tests, with push-down off |
| ITER-21 | Fixed | Cluster shards read every series in full for a reverse COUNT | Cluster A/B: raw and aggregated COUNT 1 ~0.02× |
| ITER-20 | Fixed | Parser clipping stopped FILTER_BY_TS entries outside the window from extending EMPTY | Reference probe, then 104 compat cases |
| ITER-06 | Fixed | Timestamp-filter pipelines relied on the parser to enforce the window | Unit tests through both pipelines |

The sections below stay in ID order so existing references remain stable.

### ITER-01 (fixed for gaps): Reverse EMPTY queries did work proportional to the entire bucket span

[reverse_limited and TailIter](../../src/iterators/utils.rs) bound retained output by COUNT, but
drain the entire ascending stream before returning its tail. With EMPTY, that stream included every
gap bucket, so two stored samples required work proportional to their separation divided by bucket
duration, even for COUNT 1, on the main thread for `TS.REVRANGE`. Extrapolated (not run), samples at
0 and a present-day millisecond timestamp with bucket 1 would have stalled the server for hours.

**Fix:** With a newest-first COUNT, only the last `COUNT` rows of the stream can survive, and every
gap in a run reports the same value. `MultiAggregateIterator` therefore skips a run down to its last
`COUNT` buckets by arithmetic (`EmptyBucketRun::skip_to_last`), driven by
`EmptyFillBounds::tail_limit`. `LastCarry` loses nothing: it only looks at a run's first and last
rows.

Median of interleaved pairs, two samples at timestamps 0 and `span`, bucket duration 1, COUNT 1:

| Span | Query | Before | After |
| --- | --- | --- | --- |
| 10⁶ | Reverse `sum` | 11.0 ms | 0.106 ms |
| 10⁶ | Reverse `last` | 21.2 ms | 0.110 ms |
| 10⁸ | Reverse `sum` | 1,070 ms | 0.172 ms |
| 10⁸ | Reverse `last` | 2,080 ms | 0.208 ms |
| 10⁸ | Reverse `sum,last` | 1,681 ms | 0.205 ms |
| 10⁸ | Control: forward `sum` | 0.136 ms | 0.125 ms |

**Not addressed:**

- **Dense series:** reverse EMPTY COUNT still reads every sample in the range (36.8 ms on 2 M
  samples). ITER-15's tail read does not apply, because EMPTY's buckets depend on the whole series.
- **No COUNT:** nothing caps the output. `ReverseIter` buffers every bucket, at 16 B per scalar
  bucket or 56 B per row before Vec doubling, and the forward path streams into the reply buffer.
  Whether to cap it is a compatibility decision, since a cap changes replies.
- **Grouped queries:** per-series streams run ascending with no COUNT, so a reverse GROUPBY with
  EMPTY still walks its gaps.


**Regression:** `test_reverse_empty_count_does_not_walk_the_gap` runs a 10¹² span, which would take
hours if the skip broke, and checks the rows. `TestReverseCountMatchesAFullRead` diffs EMPTY against
the reference for every aggregator and bucket option. The skip arithmetic has its own tests,
including the full timestamp range.

### ITER-02 (fixed): Compressed timestamp misses discarded decoder progress

In [TimestampFilterIterator::get_sample](../../src/iterators/timestamp_filter_iterator.rs), a
forward lookup that read past its timestamp cleared `self.chunk`, so the next lookup started another
`chunk.range_iter`, and compressed readers decode from the beginning of their chunk to reach that
point. For F missing timestamps inside an N-sample chunk, decoding approached O(F × N).

**Fix:** The iterator keeps the sample it read past as `lookahead` and keeps its decoder after a
miss, resetting both only when it moves to another chunk. Timestamps only increase, so the position
is never wrong.

Median of 15 interleaved pairs, replies compared. An 8,000-sample chunk, with samples every 2 ms,
and 128 lookups near its tail:

| Case | Before | After | Ratio |
| --- | --- | --- | --- |
| Chimp, misses | 9.222 ms | 0.247 ms | 0.027 |
| Chimp, half hits, half misses | 4.688 ms | 0.360 ms | 0.076 |
| Chimp, hits | 0.464 ms | 0.464 ms | 0.999 |
| Gorilla, misses | 9.389 ms | 0.262 ms | 0.027 |
| Gorilla, half hits, half misses | 4.993 ms | 0.379 ms | 0.075 |
| Gorilla, hits | 0.457 ms | 0.463 ms | 1.005 |
| Uncompressed, misses | 0.130 ms | 0.131 ms | 1.000 |

Default chunk size, a 200,000-sample Chimp series:

| Case | Before | After | Ratio |
| --- | --- | --- | --- |
| 128 misses in one chunk | 0.361 ms | 0.185 ms | 0.510 |
| 128 misses across many chunks | 1.712 ms | 1.138 ms | 0.661 |
| Control: raw range, no filter | 1.777 ms | 1.759 ms | 0.990 |

Reverse lookups already decoded once and were unchanged (0.973–1.022). A reverse lookup still
materializes the whole chunk; a shared per-chunk merge join could bound that by matches and remove
the `Cow` buffer, but that is an allocation change with no measured need.

**Regression:** `a_hit_directly_after_a_miss_is_found`,
`misses_at_chunk_edges_do_not_disturb_later_lookups`, and `mixed_lookups_across_a_large_chunk` pass
on all three encodings, and were written before the change. Removing the lookahead makes the first
two fail. They pin replies, not decode counts, so the speedup itself is covered by the
`timestamp_filter` group in [query_scan.rs](../../benches/query_scan.rs).

### ITER-03 (fixed): Timestamp filtering did not skip empty chunks

[TimestampFilterIterator::seek_chunk](../../src/iterators/timestamp_filter_iterator.rs) interpreted
first/last timestamps before checking whether a chunk was empty, and an empty chunk reports zero
bounds. Over stored timestamps `[0, 2, 4]`, on all three encodings:

| Fixture | Query | Result before | Expected |
| --- | --- | --- | --- |
| Trailing empty chunk | reverse filter `[2, 4]` | `[]` | `[4, 2]` |
| Leading empty chunk | forward filter `[0, 2]` | `[2]` | `[0, 2]` |

*Reachability:* no normal producer was found:

- `del_range` and defrag both `retain` non-empty chunks.
- Upsert splits refuse to insert an empty upper half.
- `add_chunk_with_sample` pushes only after a successful add.
- `trim_expired` cannot empty its boundary chunk.
- [`bulk_add`](../../src/series/bulk_add.rs) pushes each new chunk unconditionally. An empty one
  would need every append in its group to fail, and no such path was identified.
- [`rdb_load`](../../src/series/serialization.rs) does not reject empty chunks, so a crafted
  `RESTORE` payload can create this state. `TS._RESTORE` shares the loader. Neither was exercised.

**Fix:** `seek_chunk` skips an empty chunk in the current direction before reading its bounds,
matching SeriesSampleIterator.

**Regression:** `empty_chunks_are_skipped_wherever_they_sit` covers leading, interior, and trailing
empty chunks, on all encodings, in both directions, including timestamp zero. It failed before the
fix.

### ITER-04 (fixed): EMPTY broke the range iterator's size-hint upper bound

`TimeSeriesRangeIterator::calculate_size_hint` capped output by the timestamp-filter length, but
EMPTY can generate more buckets than there are selected timestamps. With range `[0, 100]`, timestamp
filter `[0, 100]`, SUM, bucket duration 10, and EMPTY, the iterator advertised `(0, Some(2))` and
emitted 11 samples. This violated the iterator contract; it was not a memory-safety failure.

Nothing in production read this hint:
[`reply_with_counted_array`](../../src/common/replies/raw_replies.rs) uses a postponed length, and
the lower bound was always 0.

**Fix:** Deleted `calculate_size_hint`, the cached field, and the override, leaving the default `(0,
None)`. No reply or allocation depended on them.

**Regression:** `test_size_hint_upper_bound_holds_under_empty` failed with `Some(2)` before the fix.

### ITER-05 (fixed): FilteredSampleIterator's filter and direction requirements were implicit

`FilteredSampleIterator` read the first and last timestamp-filter entries as minimum and maximum,
and its early exit assumed ascending input. For input timestamps `[0, 2, 4, 6]`, the filter `[6, 2,
4]` returned `[]` rather than `[2, 4, 6]`, and an empty filter also returned `[]`, while the
`filtered_iter` wrapper treated empty as "no filter". Both defects were dormant: only a bench called
it.

**Fix:** Deleted with `TimeSeriesChunk::filtered_iter` (ITER-10). No command ever used it.

### ITER-06 (fixed): Timestamp-filter pipelines did not enforce their own range bounds

Both [range pipeline factories](../../src/iterators/utils.rs) selected TimestampFilterIterator
without intersecting its timestamps with RangeOptions' date range, relying on the parser's clipping.
Constructed directly through the Rust API, range `[2, 4]` with filter `[0, 2, 4, 6]` returned all
four timestamps.

**Fix:** Required by ITER-20, which removed the clipping.
[`TimestampFilterIterator::new`](../../src/iterators/timestamp_filter_iterator.rs) now takes the
window and drops entries outside it, alongside its existing retention check. TS.JOIN reads through
`get_range_filtered` and is unaffected.

**Regression:** `test_timestamp_filter_is_limited_to_the_window` covers both directions. The compat
test `test_filter_by_ts_intersects_the_range_window` covers parsed commands.

### ITER-07 (FILTER_BY_TS fixed): EMPTY setup eagerly scans outside the query window

[empty_fill_bounds](../../src/iterators/utils.rs) runs during pipeline construction. With a filter
set, its helpers find out whether passing samples exist outside the window. That happens before the
first item is requested, regardless of COUNT, and on the main thread for `TS.RANGE`. Unfiltered
queries answer from the series extent and are unaffected.

The three `has_passing_sample` windows are disjoint (before, after, and inside the query window), so
together they cost at most about one pass over the series, stopping early at a hit. The `last` carry
seed scans backward over the prefix, but only when a passing sample exists before the window, so the
forward and backward prefix scans are bounded together by roughly the prefix length.

**Fix for FILTER_BY_TS:** The helpers now look up only the named timestamps with
TimestampFilterIterator, instead of scanning every sample and testing filter membership. Median of
15 interleaved pairs, 10-bucket window on a 2 M-sample Chimp series:

| Query (`AGGREGATION ... EMPTY` unless noted) | Before | After | Ratio |
| --- | --- | --- | --- |
| `FILTER_BY_TS`, one entry in the window | 32.07 ms | 0.156 ms | 0.005 |
| `FILTER_BY_TS`, one entry outside the window | 30.74 ms | 0.136 ms | 0.004 |
| `FILTER_BY_TS`, 128 entries outside, value filter matching nothing | 30.63 ms | 0.217 ms | 0.007 |
| `FILTER_BY_TS`, entries on both sides, `last` (reply changes per ITER-20) | 29.97 ms | 0.216 ms | 0.007 |
| Control: no filter | 0.152 ms | 0.140 ms | 0.988 |
| Control: `FILTER_BY_TS`, no `EMPTY` | 0.100 ms | 0.098 ms | 0.972 |
| **Remaining:** `FILTER_BY_VALUE` matching nothing | 27.51 ms | 27.49 ms | 0.992 |

**What remains:** FILTER_BY_VALUE still has to read samples to know whether any pass, about 14 ns
per stored sample. It could stop being eager, by deferring the scan until a fill actually needs the
answer, but COUNT and reversal interact with that. Hence P3. Each helper also recomputes
`visible_first_timestamp`.

**Regression:** Replies are pinned by the ITER-20 compat cases. Nothing counts sample reads yet, so
the speedup itself is only covered by the A/B above.

### ITER-08: Withdrawn. Duplicate-policy collapse is not observable

[sample_merge_iterator.rs:32](../../src/iterators/sample_merge_iterator.rs) sends every
`duplicate_value` error down the BLOCK path. Pass 2 read this as a reply divergence between the
direct and merge write paths. Pass 3 found no such divergence:

- [`TimeSeriesChunk::upsert`](../../src/series/chunks/timeseries_chunk.rs) (line 182) and the
  [uncompressed merge](../../src/series/chunks/uncompressed/mod.rs) (line 372) also map every
  `TsdbError::DuplicateSample` to `SampleAddResult::Duplicate`.
- [`duplicate_value`](../../src/series/types.rs) raises `DuplicateSample` for both BLOCK and the NaN
  cases, and [`DUPLICATE_SAMPLE`](../../src/error_consts.rs) deliberately describes both.
- `TS.ADD` never reaches `merge_chunk_samples`. `TS.MADD` and `TS.ADDBULK` do, and `TS.ADDBULK`'s
  JSON payload cannot carry NaN at all.
- Probing `TS.ADD`, single-sample `TS.MADD`, and multi-sample `TS.MADD` under `max` and `sum`, on
  Uncompressed and on the default compressed encoding, gave the same error text and the same stored
  values on every path.

What remains is a clarity item: the merge's `else` branch reads as "blocked" while covering the NaN
cases too. A comment recording that this is intentional is enough. Do not change behavior for this.

### ITER-09 (fixed): `chain_latest`'s direction flag was misnamed at both call sites

`chain_latest` declared a `should_reverse_iter` parameter, but both call sites passed the raw
`is_reverse`. Correctness rested on a redundant re-check inside it, `should_reverse_iter &&
opts.aggregation.is_none()`. The natural cleanup, deleting the condition that looks duplicated and
keeping the argument, would have placed a LATEST sample ahead of an ascending aggregated stream and
silently moved its position in every bucketed reply.

**Fix:** The argument is now the real read order, named `read_newest_first`, and the re-check is
gone. The other parameters moved into a small `SampleOutput` struct in
[utils.rs](../../src/iterators/utils.rs), so `chain_latest` takes three arguments and the
`too_many_arguments` allowance is removed. Behavior is unchanged.

**Regression:** `test_latest_sample_position_in_the_reply` covers raw, aggregated, FILTER_BY_TS,
both combined, and the row pipeline, in both directions, and
`test_latest_sample_joins_its_bucket_on_a_reverse_aggregation` covers a LATEST sample inside a
stored bucket. Both were written against the old code and passed. Passing the raw `is_reverse` again
fails both.

### ITER-10 (fixed): Several iterator surfaces had no production caller

| Item | What it was | Outcome |
| --- | --- | --- |
| `group_reduce` | No callers | Deleted |
| `SampleIter::Range` | Never constructed | Deleted, which decouples chunk-level from query-level iteration |
| `impl Iterator for SampleMergeIterator` | Unused, and **lost data** (below) | Deleted; the type is now `pub(crate)` and documented |
| `chunks::merge::merge_samples` (free function) | No callers | Deleted |
| `TimeSeriesChunk::filtered_iter`, `FilteredSampleIterator` | One unit test and the `scan_filtered` bench | Deleted; the bench now runs the real range pipeline |
| `(Some(agg), Some(grp))` arm of the sample adapter | Unreachable | Deleted |
| `grouping` parameter of `create_range_iterator` and the adapter | Always `None`, except one coordinator path | Removed; that path reduces in `finish_series_list` |
| `ReduceIterator` | **Live** on the coordinator, for GROUPBY without AGGREGATION | Replaced by `SampleReducer`, then deleted |

**The merge iterator's `Iterator` impl was wrong, not merely unused.** On a blocked duplicate,
`next_internal` returned the left sample flagged `blocked`, and `next` then `continue`d, discarding
it. Merging `[1, 2, 3]` with `[2]` under BLOCK:

```text
next_internal:   (1, 1.0, false), (2, 2.0, true), (3, 3.0, false)
Iterator::next:  (1, 1.0), (3, 3.0)      <- the stored sample at 2 is gone
```

**Why the compiler did not find these:** `pub mod series` still exposed `filtered_iter` and
`SampleIter`, and rustc does not flag trait impls or variants of a publicly reachable enum. They
were found by caller search, which pass 2 ran incompletely when it listed `ReduceIterator` as dead.

**Note for bench history:** `scan_filtered` measures the real pipeline now, so results are not
comparable with baselines from before this change.

**Regression:** `test_grouped_raw_samples_reduce_per_timestamp` pins the coordinator's GROUPBY
route, including all-NaN timestamps mid-stream and at the end, and a reverse query with COUNT. It
was written against `ReduceIterator` and passed, then passed on `SampleReducer`, and it fails if the
reduce is removed. It compares with literal expectations, not with the reference.

### ITER-11 (fixed): LATEST resolution panicked on a missing context

`get_range_latest_sample` did `ctx.expect("Context is required when LATEST option is used")`. Every
production caller passed `Some(ctx)`, so the panic was unreachable, but it was why LATEST could not
be unit tested: the range iterators accepted `Option<&Context>` to allow context-free tests, and the
one option that needed it turned that seam into a panic.

**Fix:** [ts_range.rs](../../src/commands/ts_range.rs) now resolves the sample up front and passes
`Option<Sample>` in, as [mrange.rs](../../src/series/mrange.rs) and
[nrange.rs](../../src/series/nrange.rs) already did. `get_range_latest_sample` takes a plain
`&Context`, and `TimeSeriesRangeIterator` and `TimeSeriesRangeRowIterator` need no context at all.

**Regression:** `test_latest_sample_is_reported_without_a_context`.

### ITER-12 (fixed): The merge machinery was duplicated three ways

The sample merge, row merge, and `PivotIter` each defined an entry type whose `Ord` was "timestamp,
then source index", and the first two were otherwise the same struct.

**Fix:** [`KWayMerge`](../../src/iterators/k_way_merge.rs) is one generic merge over a small
`Timestamped` trait, and `PivotIter` shares its `MergeEntry` and `seeded_heap`, keeping only its own
drain loop. Their tests moved with them.

**Regression:** Flipping the tie-break in the shared entry fails 7 tests. A server A/B against the
previous build, with 15 interleaved pairs per query over all five ITER-16 fixtures, kept the ITER-16
gain: every median ratio is within 0.98–1.02. A few cells lean slower by 1–2 % and did so in almost
every pair: 1.008 at 1,024 sources with `avg,max` (0 of 15 pairs faster), 1.021 at 256 sources with
`TS.NREVRANGE` (1 of 15), and 1.016 for raw GROUPBY over 1,024 non-overlapping sources (1 of 15).
The no-merge control runs at 0.98–1.01. It is small enough to be a code layout effect, and it was
not investigated further.

### ITER-13 (fixed): `pub use module::*` published iterator internals as crate API

[mod.rs](../../src/iterators/mod.rs) glob-re-exported four modules from a `pub mod iterators`. Glob
re-exports cannot widen visibility, so `pub(crate)` items stayed private, but `TimestampFilter`,
`TailIter`, `create_aggregate_iterator`, `group_reduce`, and others leaked, along with any future
`pub` helper.

**Fix:** Explicit re-exports. Outside the crate only `SampleIter` (returned by the public chunk
API), `TimestampFilterIterator`, and `create_range_iterator` remain, the last two for
`benches/query_scan.rs`. Everything else is `pub(crate)` or private. `chunks/mod.rs` also needed its
`merge` re-export narrowed once its last public item was gone.

### ITER-14 (fixed): A `FILTER_BY_TS` clipped to empty returned the whole window on a cluster

The parser clips `FILTER_BY_TS` to the query window
([command_parser.rs:1314](../../src/commands/command_parser.rs)), so a filter whose entries all fall
outside it becomes `Some([])`, meaning "match nothing". The fan-out codec could not express that:

1. [`From<&RangeOptions> for RangeRequest`](../../src/commands/fanout_codec/conversions.rs) (line
   654) encoded `Some([])` and `None` identically, as an empty repeated field.
2. The decoder (line 616) read an empty list as `None`, so each shard ran with no filter.
3. The coordinator clears its own copy in [`reply`](../../src/commands/ts_mrange_fanout_command.rs)
   (line 205), so nothing filtered afterward.

Before the fix, with three series holding samples at 50, 55, and 60:

| Query | Standalone | Three-primary cluster |
| --- | --- | --- |
| `TS.MRANGE 50 60 FILTER_BY_TS 10 FILTER grp=x` | no samples | every series: 50, 55, 60 |
| `TS.MREVRANGE 50 60 FILTER_BY_TS 10 FILTER grp=x` | no samples | every series: 60, 55, 50 |
| `... AGGREGATION sum 10` | no samples | every series: buckets 50, 60 |
| `... GROUPBY grp REDUCE sum` | empty group | 50→3, 55→6, 60→9 |
| `TS.MRANGE 50 60 FILTER_BY_TS 55 FILTER grp=x` (control) | 55 only | 55 only |

The reply was silently wrong rather than an error, and it inverted the filter's meaning: the
narrowest possible filter returned the most data. `TS.RANGE` and `TS.NRANGE` have no fan-out path
and were unaffected. The reference was not consulted, because the module disagreed with itself
across topologies either way.

**Fix:** `RangeRequest.timestamp_filter` is now a `TimestampFilter` wrapper message
([request.proto](../../proto/v1/request.proto)), not a bare `repeated` field, so its presence
survives encoding and decodes to `Option`. The field changed in place, as fan-out formats may before
the first release. A coordinator short-circuit would not have been enough, because series must still
be listed with empty sample arrays and EXCLUDEEMPTY must still apply.

ITER-20 later removed the parser clipping, so parsed commands no longer produce `Some([])`: a filter
outside the window now travels intact and the reader drops it. The wrapper stays as protection for
any other caller, and the tests below still hold.

**Regression:** `test_timestamp_filter_presence_survives_the_wire` round-trips `None`, `Some([])`,
and a populated filter through real protobuf bytes.
[test_ts_mrange_cme.py](../../tests/test_ts_mrange_cme.py) asserts the cluster replies for raw,
reverse, single and multiple aggregation, EMPTY, GROUPBY, and EXCLUDEEMPTY, plus a partly clipped
control. All but the control failed before the fix.

### ITER-15 (fixed): Reverse aggregated COUNT decoded the entire range

For aggregated reverse queries, [`create_range_iterator`](../../src/iterators/utils.rs) aggregated
the whole range forward and kept the tail. Unlike ITER-01 this needed no gaps or unusual parameters:
`TS.REVRANGE k - + AGGREGATION avg 60000 COUNT 1` is the ordinary "latest bucket" query, and it cost
about 18 ns per stored sample on the main thread, growing with series length rather than with the
answer.

**Fix:** `ReverseTailRead` runs the ordinary pipeline over only the newest `COUNT` possible buckets,
and widens if filters or gaps leave it short. Buckets do not depend on each other, so this reuses
every aggregator unchanged, including the order-sensitive ones, and the read always starts on a
bucket boundary. This replaces the reverse bucket cursor suggested earlier, which would have been a
second read path. It does not apply to EMPTY or FILTER_BY_TS, and a COUNT that spans the window
reads all of it, as before. ITER-21 brings the same read to cluster shards.

A short read widens ×4, until the next window would pass half the query window, when it reads the
whole window. With a value filter, one short read goes straight to the whole window, because a
filter that emptied the newest buckets says nothing good about earlier ones. The first version
always widened, and its worst case measured 1.62× slower.

Median of interleaved pairs, replies compared, 2 M samples, 60 s buckets:

| Query | Before | After | Ratio |
| --- | --- | --- | --- |
| Reverse `avg`, COUNT 1 | 36.5 ms | 0.152 ms | 0.004 |
| Reverse `last`, COUNT 1 | 33.3 ms | 0.141 ms | 0.004 |
| Reverse `avg,max`, COUNT 1 | 38.9 ms | 0.103 ms | 0.003 |
| Reverse `avg`, COUNT 10 | 36.7 ms | 0.167 ms | 0.004 |
| Reverse `avg`, COUNT 1000 | 38.3 ms | 2.94 ms | 0.078 |
| `TS.MREVRANGE` `avg`, COUNT 1 | 37.0 ms | 0.190 ms | 0.005 |
| Value filter passing everything, COUNT 1 | 36.8 ms | 0.125 ms | 0.003 |
| Worst: value filter matching only the oldest | 25.8 ms | 25.4 ms | 0.985 |
| Worst: value filter matching nothing | 25.8 ms | 25.2 ms | 0.977 |
| COUNT above the number of buckets | 97.0 ms | 102.4 ms | 1.002 |
| Control: forward `avg`, COUNT 1 | 0.089 ms | 0.091 ms | 0.958 |
| Control: reverse `avg`, no COUNT | 98.3 ms | 94.5 ms | 0.957 |

**Not addressed:**

- **The remaining worst case:** with no value filter, a tail of samples no aggregator accepts, such
  as a long NaN run, makes the search fail. Failed passes read below 1.7× the window. That bound is
  arithmetic, tested on the window sequence; it was not timed.

- **Grouped queries** run ascending with COUNT applied after the reduce, so they are unchanged.

**Regression:** `test_reverse_count_matches_the_full_reversed_result` compares reverse COUNT with
the full reversed result for 20 aggregators, including the rate family, across fixtures with gaps,
NaN, and counter resets, every alignment, windows, value filters, LATEST, and EMPTY, in both
pipelines. Starting the read one past the bucket boundary fails it.
`TestReverseCountMatchesAFullRead` diffs against the reference, and the read planning has its own
tests.

### ITER-16 (fixed): The k-way merges used a slower heap than they needed

All three merges (sample, row, and [pivot](../../src/iterators/pivot_iter.rs), the first two now
[`KWayMerge`](../../src/iterators/k_way_merge.rs)) used `MinMaxHeap`, paying for max-side ordering
they never read, and popped and pushed where replacing the top in place was enough.

**Fix:** `BinaryHeap<Reverse<Entry>>` with `peek_mut`, which replaces the top with one sift. `Ord`
is unchanged, so ties still break by source. The `min-max-heap` dependency stays for `label_querier`
and `string_interner`, which use `pop_max`. The three copies remain; ITER-12 is still open.

**Server A/B:** two release builds on native macOS, 2 M samples per fixture, one group or one pivot,
15 interleaved pairs per query in alternating order. Cells give baseline → fixed median wall time,
with the median of paired ratios in parentheses:

| Sources | GROUPBY, reverse COUNT 1 | GROUPBY, full reply | TS.NREVRANGE, COUNT 1 | `avg` 60 s + GROUPBY | `avg,max` 60 s + GROUPBY |
| --- | --- | --- | --- | --- | --- |
| 16 aligned | 101 → 55 ms (0.55) | 318 → 269 ms (0.85) | 104 → 68 ms (0.65) | 0.98 | 1.00 |
| 256 aligned | 182 → 76 ms (0.41) | 195 → 88 ms (0.45) | 182 → 85 ms (0.47) | 0.96 | 0.97 |
| 1,024 aligned | 217 → 95 ms (0.43) | 221 → 98 ms (0.44) | — | 0.96 | 0.97 |
| 4,096 aligned | 249 → 115 ms (0.46) | 254 → 118 ms (0.46) | — | 0.96 | 0.96 |
| 1,024 disjoint | 172 → 55 ms (0.32) | 4,070 → 3,922 ms (0.96) | — | 0.94 | 0.97 |

- **Noise floor:** an A/A run of baseline against baseline gave ratios of 0.997–1.002. The no-merge
  control (`TS.MRANGE` without GROUPBY) was flat at 0.987–1.003. The fixed build won all 15 pairs in
  every reverse COUNT 1 raw and TS.NREVRANGE cell.
- **Server CPU** tracks wall time, except on full replies, where wall time includes the Python
  client parsing the reply. CPU ratios there are 0.59 at 16 sources and 0.67 for the 1,024 disjoint
  case (about 2 M rows), so those two wall figures understate the server's gain.
- **Bucketed queries gain little** (0.94–1.00), because 60 s buckets cut the merged elements about
  60-fold and other costs dominate. Finer buckets should fall between these columns.
- **Not measured:** the cluster coordinator routes use the same iterators but were not A/B tested.

An earlier release micro-benchmark over slices predicted 0.34–0.66× for aligned sources. The aligned
server results (0.41–0.65×) fall within that range, so the micro-benchmark was not misleading.

**Regression:** new unit tests pin what the swap could have broken: equal timestamps in source
order, a source repeating a timestamp, NaN values, full-sequence comparison against a stable sort
(with spilled rows for the row merge), and TS.NRANGE column placement over 40 sources. They passed
on `MinMaxHeap` before the swap. Flipping the tie-break made 7 of them fail, so they do constrain
it.

### ITER-17 (fixed): Stored NaN counted as EMPTY data, and `last` mishandled it

A black-box probe of the pinned reference showed this was wider than the `last` seed originally
reported here. Before the fix, the module matched the reference on 76 % of random NaN-heavy forward
queries, and row sets differed for every aggregator, not only `last`.

The rules were worked out by writing them as a small model and comparing it with the reference on
random series, with mixed NaN density, bucket sizes, windows, and both filter types. The model then
matched on every case: 3,000 row sets and 2,232 forward `last` sequences on one seed, and 2,500
reverse sequences on another. Nothing had to be registered as a divergence.

**For every aggregator:** a stored NaN is a sample, but not data for EMPTY. It does not extend the
fill past the window's samples.

| Samples, window `[50, 90]`, `sum 10 EMPTY` | Reference | Module before |
| --- | --- | --- |
| `(0, NaN)`, `(100, 7)` | no rows | 50–90 |
| `(0, 7)`, `(100, NaN)` | no rows | 50–90 |
| `(0, NaN)`, `(60, 1)` | 60 | 50, 60 |
| `(60, 1)`, `(100, NaN)` | 60 | 60–90 |

**For `last`:** a leading run of gaps holds the last reading before its first bucket. After that the
direction matters, because the reference's answer follows its scan order:

- **Forward:** a NaN-only bucket reports the value held so far, or NaN if there is none. A gap does
  the same, except that with nothing held it looks back to the last reading before the window.
- **Reverse:** a gap takes the last reading before it in time, looking past the window if needed,
  and a NaN-only bucket takes the value of the next bucket in time.

With `a` holding `(0, 5)`, `(10, NaN)`, `(100, 7)`, and `b` holding `(0, 5)`, `(10, NaN)`, `(20,
6)`, `(30, NaN)`, `(100, 7)`, `AGGREGATION last 10 EMPTY`, and bucket 100 always `7`:

| Series, window | Forward | Reverse |
| --- | --- | --- |
| `a`, `[50, 200]` | 50–90: `5` | 50–90: `5` |
| `a`, `[10, 200]` | 10: NaN, 20–90: `5` | 10–90: `5` |
| `b`, `[25, 200]` | 20–90: `5` | 20: `5`, 30–90: `6` |

**Fix:** `CarryLastEmpty` could not separate a gap from a NaN-only bucket, because both reached it
as NaN, so `LastCarry` in [aggregate_iterator.rs](../../src/aggregators/aggregate_iterator.rs)
replaces it. `MultiAggregateIterator` holds a reverse NaN-only row back until the next bucket can
answer it. `has_passing_sample` ignores NaN and `EmptyFillBounds` gains a second seed and the
direction. Shards aggregate ascending, so `MRangeOptions::reply_reverse` tells them the reply's
order; grouped queries stay forward, which matched the reference in the four reverse GROUPBY shapes
probed. Removing the adapter also collapsed the two identical finalizers into one.

**Side effect on TS.JOIN:** it shares `aggregate()`, so `AGGREGATION last ... EMPTY` now carries
through gaps as `TS.RANGE` does, where it used to report NaN: with samples at 0 and 100 and bucket
25, `REDUCE sum` gives 25–75 as `2`, not NaN. TS.JOIN has no reference to compare with and no test
pins this.

**Cost:** ordinary series are unchanged, since the first sample read or the stored last sample
usually answers. A series whose samples before the window are all NaN now has to read them: about 8
ns each, 0.12 ms to 16.5 ms with 2 M of them. Medians of 15 interleaved pairs:

| Query | Before | After | Ratio |
| --- | --- | --- | --- |
| `sum`, `EMPTY`, mid-series window | 0.123 ms | 0.118 ms | 0.975 |
| `last`, `EMPTY`, mid-series window | 0.096 ms | 0.095 ms | 0.996 |
| `sum`, `EMPTY`, past the tail | 0.067 ms | 0.066 ms | 1.002 |
| `sum`, `EMPTY`, whole series | 93.2 ms | 96.6 ms | 1.004 |
| 2 M NaN before the window, `sum`, `EMPTY` | 0.118 ms | 16.5 ms | 141 |
| 2 M NaN before the window, `last`, `EMPTY` | 0.143 ms | 16.5 ms | 116 |

**Regression:** `TestEmptyFillAroundStoredNan` in
[test_compat_range.py](../../tests/compat/test_compat_range.py) diffs named shapes, every
aggregator, and 150 seeded random series against the reference, in both directions and RESP
versions, with a `TS.MRANGE` test alongside it. Literal reference replies are pinned by
`test_last_empty_fill_around_stored_nan` and `test_reverse_last_empty_fill_around_stored_nan`, and
`test_mrevrange_cme_last_empty_fill_around_stored_nan` covers a cluster. **Not covered:** an all-NaN
stretch this large in production, reverse queries on the raw coordinator path (ITER-18), and GROUPBY
beyond the four probed shapes.

### ITER-18 (fixed): The raw coordinator path lost EMPTY's edge fill and seeds

With `ts.ts-fanout-aggregation-pushdown no` (default is on), the coordinator aggregated raw samples
itself using `EmptyFillBounds::default()`. EMPTY's answer depends on what a series holds outside the
window, which only its shard can see. Series holding `(0, 1)`, `(100, 2)`, `(300, 3)`:

| Query | Standalone and push-down on | Push-down off, before |
| --- | --- | --- |
| `TS.MRANGE 50 75 AGGREGATION last 25 EMPTY` | 50→1, 75→1 | no rows |
| `TS.MRANGE 150 250 AGGREGATION sum 25 EMPTY` | 150–250 report 0 | no rows |
| `TS.MRANGE 0 100 AGGREGATION last,sum 25 EMPTY` | `last` carries 1 through 25–75 | `last` is NaN for 25–75 (fixed by ITER-17) |

**Fix:** A query with EMPTY now always pushes aggregation down, whatever the config says
([ts_mrange_fanout_command.rs](../../src/commands/ts_mrange_fanout_command.rs)). Shards already
answer it correctly, so no outside-window data needed to travel. Without EMPTY the config keeps its
meaning.

**Trade-off:** The config's purpose is an emergency escape hatch for push-down bugs, and this
narrows it: a push-down bug in an EMPTY query can no longer be mitigated by turning it off. The
alternative, shipping bounds and seeds in every raw response, would keep the hatch intact at the
cost of more code and a wire change. [overview.md](../overview.md) and the config comment record the
exception.

**Regression:** `test_empty_reaches_past_the_window_with_pushdown_off` failed on all four shapes
before the fix. `test_aggregation_without_empty_still_honors_pushdown_off` shows the config still
applies elsewhere, though it checks the reply rather than the transport.

### ITER-19: Withdrawn. LATEST at timestamp 0 on an empty destination matches the reference

The guard at [timeseries_range_iterator.rs:30](../../src/iterators/timeseries_range_iterator.rs) is
`end_ts > series.last_timestamp()`, and `last_timestamp` returns 0 for an empty series, so `TS.RANGE
dst 0 0 LATEST` returns `[]` while `TS.RANGE dst 0 5 LATEST` returns `[[0, 1]]`. This looked like a
sentinel bug. The pinned reference returns identical replies for both queries, and for `TS.REVRANGE`
and `TS.MRANGE` forms and a non-zero bucket control, so there is nothing to fix. Do not change this
guard without a reference reason.

### ITER-20 (fixed): Parser clipping stopped FILTER_BY_TS entries outside the window from extending EMPTY

[`EmptyFillBounds`](../../src/aggregators/aggregate_iterator.rs) documents the fill extent as "the
samples passing the query's filters" over "the *whole* series", and `has_passing_sample` looks
outside the window for `FILTER_BY_TS`. On parsed commands that could never succeed, because the
parser had already removed out-of-window entries. With samples at 10, 55, and 90 and `AGGREGATION
sum 5 EMPTY`:

| Query | Reference | Module before the fix |
| --- | --- | --- |
| `TS.RANGE g 50 70 FILTER_BY_TS 10 55 90` | 50→0, 55→2, 60→0, 65→0, 70→0 | 55→2 |
| `TS.RANGE g 50 70 FILTER_BY_TS 55 90` | 55→2, 60→0, 65→0, 70→0 | 55→2 |
| `TS.RANGE g 50 70 FILTER_BY_TS 10 55` | 50→0, 55→2 | 55→2 |
| `TS.RANGE g 60 70 FILTER_BY_TS 10 90` | 60→0, 65→0, 70→0 | no rows |
| `TS.RANGE g 60 70 AGGREGATION last ... FILTER_BY_TS 10 90` | 60→1, 65→1, 70→1 | no rows |

Entries outside the window return no samples, but they decide how far EMPTY reaches, exactly as
passing samples outside the window do under FILTER_BY_VALUE. An entry that matches no stored sample
does not count. No existing compat test or divergence entry combined these options, so the fuzzer
and the suite had not caught it.

**Fix:** The three `ts_filter.retain(..)` sites in
[command_parser.rs](../../src/commands/command_parser.rs) are gone, and the reader enforces the
window instead (ITER-06). Shards receive the whole list, so cluster replies match standalone.

**Regression:** 104 compat cases diffed against the reference, in both directions and both RESP
versions: `test_empty_fill_sees_entries_outside_the_window` (96) and
`test_empty_fill_applies_the_value_filter_to_outside_entries` (4) in
[test_compat_range.py](../../tests/compat/test_compat_range.py), and a `TS.MRANGE` test (4) in
[test_compat_mrange.py](../../tests/compat/test_compat_mrange.py).
`test_mrange_cme_empty_fill_sees_filter_by_ts_entries_outside_the_window` covers a cluster, and
`test_empty_fill_reaches_window_edges_from_outside_filter_entries` covers both pipelines in unit
tests.

### ITER-21 (fixed): Cluster shards read every series in full for a reverse COUNT

A shard streams ascending for the coordinator to reverse, so for `TS.MREVRANGE ... COUNT` it read
each series in full and then kept the tail with `SampleLimit::Tail`. That missed ITER-15 and
ITER-01, and it also made raw reverse COUNT read everything, where standalone raw reverse is lazy.

**Fix:** With a tail limit, a shard calls `create_range_tail_iterator` or `create_row_tail_iterator`
in [utils.rs](../../src/iterators/utils.rs). Aggregated queries use `ReverseTailRead`, EMPTY uses
the gap skip, and raw queries read newest-first, stop at COUNT, and flip the result. Grouped queries
use the same per-series tail, which still reaches every series holding a reply's bucket: a bucket
among the group's newest COUNT is also among the newest COUNT of any series holding it.

Median of interleaved pairs, two three-primary clusters, one 1 M-sample series per primary, replies
compared:

| `TS.MREVRANGE` query | Before | After | Ratio |
| --- | --- | --- | --- |
| Raw, COUNT 1 | 16.5 ms | 0.370 ms | 0.022 |
| Raw, COUNT 1000 | 21.8 ms | 5.52 ms | 0.251 |
| `avg`, COUNT 1 | 19.7 ms | 0.405 ms | 0.020 |
| `avg`, COUNT 1000 | 25.0 ms | 6.65 ms | 0.265 |
| `avg,max`, COUNT 1 | 20.6 ms | 0.367 ms | 0.018 |
| `sum` `EMPTY`, COUNT 1, gap span 10⁸, bucket 1 | 1,097 ms | 0.481 ms | 0.000 |
| Value filter matching nothing, COUNT 1 | 13.5 ms | 13.4 ms | 1.004 |
| GROUPBY `sum` (partials), COUNT 1 | 24.3 ms | 24.3 ms | 1.001 |
| GROUPBY `avg` + AGGREGATION, COUNT 1 | 19.7 ms | 19.7 ms | 1.004 |

**Noise:** Controls on clusters vary more than on standalone. A first run showed forward COUNT 1 at
1.113 and reverse with no COUNT at 1.066, on paths this does not change. Rerunning in both role
orders put them on either side of 1.0 (reverse no COUNT, new over old: 0.969 and about 1.05), so
treat cluster differences under about 10 % as unresolved.

**Not addressed:** GROUPBY with push-down partials, which is unchanged above, because it reduces
across series on the shard first. EMPTY on a dense series, as in ITER-01.

**Regression:** `test_shard_tail_matches_a_full_read` compares the tail with a full read across raw
and aggregated pipelines, EMPTY, both filters, and LATEST, with the reply order both ways. The
`test_mrevrange_cme_count_*` cluster tests pin raw, filtered, aggregated, EMPTY, multi-aggregation,
LATEST, and GROUPBY replies, and they passed on the old build first.

## Measured pipeline cost

Release-profile Rust probe, 1 M samples, ns per sample, minimum of 9 interleaved rounds. "Direct"
drives chunk iterators with no pipeline; "current" is the real boxed pipeline; "sketch" is the
concrete adapter proposed below.

| Encoding | Direct `fold` | Direct `next` | `SeriesSampleIterator` | Current raw | Sketch raw |
| --- | --- | --- | --- | --- | --- |
| Chimp | 7.78 | 7.91 | 11.81 | 13.58 | 14.11 |
| Gorilla | 10.81 | 11.09 | 12.52 | 14.90 | 15.21 |
| Uncompressed | 0.88 | 2.14 | 2.32 | 4.67 | 4.66 |

| Encoding | Current value filter | Sketch value filter | Current scalar aggregation | Sketch scalar aggregation | Current row aggregation | Sketch row aggregation |
| --- | --- | --- | --- | --- | --- | --- |
| Chimp | 12.99 | 13.30 | 17.74 | 18.08 | 14.88 | 18.05 |
| Gorilla | 14.16 | 13.76 | 17.84 | 18.32 | 15.94 | 18.39 |
| Uncompressed | 3.39 | 3.56 | 9.10 | 9.08 | 9.00 | 9.13 |

What this shows:

- **Removing boxes does not speed up iteration.** The concrete sketch is flat against the boxed
  pipeline within about 5 %, and slower on the Chimp and Gorilla row paths (+21 %, +15 %).
  Differences this small may be inlining or layout effects; they were not decomposed.
- **The pipeline itself costs 4–6 ns per sample** above direct iteration: most of the read path on
  Uncompressed (0.88 → 4.67), and a quarter to a third of aggregation on compressed data. Most of it
  is per-sample dispatch in `SeriesSampleIterator` and `SampleIter::next`, plus the `Filter`
  closure.
- **`fold` matters on slices:** dispatching once per chunk cuts Uncompressed from 2.14 to 0.88 ns,
  but `Box<dyn Iterator>` cannot forward `fold`, and `SeriesSampleIterator` does not implement it.
  Capturing this gain would take chunk-at-a-time consumption in `AggregateIterator`, which is a
  design change with an upper bound of about 4–6 ns per sample. It is worth trying only after
  profiling shows reads, not replies, dominate the target workload.

## Allocation and memory-pressure observations

- **The merge invariant is sound.** `KWayMerge` keeps one pending item per live source. Its merge
  state is O(K), with O(N log K) processing. Timestamp/source ordering avoids comparing values that
  may be NaN.
- **Per-source state is modest.** `size_of`: `SampleIter` 152 B, `SeriesSampleIterator` 224 B,
  `TimestampFilterIterator` 360 B, `MultiAggregateIterator<SeriesSampleIterator>` 432 B at the time
  of measuring, `MultiSample` 56 B, `Aggregator` 32 B. These are held per merged series for the
  whole merge, before chunk buffers and inline filter storage.
- **Reverse compressed timestamp lookups materialize a whole chunk.** Even one requested timestamp
  decodes a buffer, whose capacity is reused until drop and whose size follows decoded sample count
  rather than compressed bytes. Measured at 0.169 ms versus 0.164 ms forward for one hit on an
  8,000-sample chunk, so the cost is memory rather than time. ITER-02's merge join would bound it by
  matches.
- **Bounded reversal retains two buffer capacities.** TailIter holds its deque while ReverseIter
  transfers the tail into another allocation. Items move rather than clone, but both capacities
  remain. Yielding directly from the deque's back would avoid the second buffer. Without COUNT,
  reversal buffers the entire output (ITER-01).
- **The row pipeline accumulates boxes.** The base reader, optional LATEST chaining, optional value
  filter, final adapter, and optional reversal introduce successive boxes, multiplied across matched
  series. This is a construction cost and adds dispatch; the measurements above show it does not
  slow iteration measurably.
- **Wide rows allocate per emitted row.** MultiSample stores four values inline; wider pivoted or
  aggregated rows need heap storage. Reverse EMPTY COUNT queries also construct and discard wide
  rows before the retained tail, which compounds ITER-01. The 10⁸-span `sum,last` row above stays
  inline at 8.9 ns/bucket; a wider row was not measured.
- **Reverse COUNT is boxed twice and limited twice.** `reverse_limited` already bounds output
  through `TailIter`, then `apply_iter_limit!` wraps the result in a `take` inside a second `Box`,
  adding a vtable hop per item and an adapter that can never fire. The macro's `limit as usize` cast
  is also dead, since `count` is already a `usize`.
- **No size hint survives the reverse path.** `AggregateIterator`, `ReverseIter`, and `TailIter`
  implement none, and `TimestampFilterIterator` reports `Some(self.timestamps.len())` for its whole
  life rather than the remaining count. Buffered reversal therefore reserves nothing and grows by
  doubling: up to 2× transient memory on a large reverse range.
- **The per-query `opts` rebuild clones aggregation options.** `create_range_iterator` clones
  `AggregationOptions` per series on the FILTER_BY_TS path only to set `timestamp_filter: None`, and
  `per_key_range_options` in TS.NRANGE clones the whole filter and aggregation per key. A flag or
  borrowed view would avoid both. Not measured.
- **`TimestampFilter` rehashes above sixteen entries.** `IntSet::from_iter` could use
  `with_capacity_and_hasher` instead. It is built only for EMPTY's scans and the coordinator
  adapter, so this is minor. Not measured.
- **The reverse timestamp buffer drops capacity on an uncompressed chunk.**
  [fill_buffer](../../src/iterators/timestamp_filter_iterator.rs) assigns `Cow::Borrowed` over a
  `Cow::Owned(Vec)`. This would matter only for a series with alternating encodings, and no producer
  of one was found, so it is not worth changing on its own.

**Measured, no action needed: per-series filter normalization.** `TimestampFilterIterator::new`
copies, sorts, and dedups the filter per series, and the parser does not sort it first, so a shared
normalized slice looks like an easy win. Across a 20,000-series `TS.MRANGE`, 128 shuffled entries
cost about 3,525 ns/series against 3,306 ns with no filter, comparable to run-to-run noise (17
entries measured 3,662 ns; 128 sorted, 3,384 ns).

No allocation-counting or heap-profiling run was performed. The allocation conclusions come from
source inspection and `size_of`.

## Clarity, maintainability, and testability

**Simplify the surviving reducer.** ITER-10 left `SampleReducer` as the only sample reducer. It
could take `RowReducer`'s shape: it writes every same-timestamp sample back into `buffer` for no
reader, and uses `as_ref().unwrap()` on a value it just tested.

**Consolidate the finalizers.** Done with ITER-17, which removed what separated them. The single
`finalize` is the natural place to drop the redundant `take`.

**Share the pre-aggregation filtering stage** between scalar and row pipelines. It should own range
enforcement (ITER-06), value filtering, timestamp-reader selection, and LATEST placement (ITER-09),
with clear direction and latest-sample preconditions (ITER-11) and a single normalization boundary
for filter input (ITER-05). This is a clarity argument, not a performance one (see *Measured
pipeline cost*).

**Name the remaining normalization layers.** A timestamp filter is sorted and clipped to the window
and retention in `TimestampFilterIterator::new`, and separately handled by `filter_timestamp_slice`
for TS.JOIN. ITER-05, ITER-06, ITER-14, and ITER-20 were all consequences of no one layer owning it.

**Document the merge iterator.** `SampleMergeIterator` is the only type in the module with no doc
comment and has the subtlest contract of the set: `next_internal` emits the *left* sample on a block
with the right one already consumed, treats every policy error the same way by design (ITER-08), and
`Iterator::next` does something different and wrong (ITER-10). Its `left < right` reaches `Ord for
Sample` only once timestamps differ, so a timestamp comparison would state the intent directly.

**Fix stale comments.** `TimestampFilterIterator::new` documents a `chunks` parameter that is now
`series`. `EmptyBucketRun`'s note on `.take(COUNT)` overstates what it bounds (ITER-01). The
`scan_filtered` bench claims to measure what the command layer sees (ITER-05).

**Tighten the tests that exist before adding more.**

- *Vacuous assertions.* `test_iteration_with_timestamp_range`, `test_iteration_with_value_filter`,
  and `test_multiple_filters_combined` assert only inside a loop over results, and pass on empty
  output. `test_iteration_with_count_limit` asserts `len <= 5`, which an empty iterator satisfies.
  `test_iteration_with_aggregation` asserts only `len < 10`.
- *Misnamed tests.* `test_iteration_with_retention` never configures retention.
  `test_iterator_is_lazy` checks the first values without measuring upstream consumption.

- *Untested normalization.* Every timestamp-filter fixture is already sorted and duplicate-free, so
  the constructor's load-bearing sort and dedup are untested.
- Use `DataGenerator` for fixtures, as required by AGENTS.md.

**Cover the untested adapters.** No unit test exercises `TimestampFilter`,
`SampleIter::fold`/`size_hint` dispatch, or the `blocked` branch of `SampleMergeIterator` directly.
`empty_fill_bounds` is now covered through the pipelines, including its overlap and carry-seed
rules.

**Cheap test-hygiene win.** The range-iterator test module builds twenty complete `RangeOptions`
literals although the type derives `Default`; `..Default::default()` removes roughly 150 lines of
noise, which is what makes the gaps above easy to miss.

**Add cross-encoding reference comparisons** for filter combinations, missing timestamps, empty
chunks, and direction. Extend query benchmarks beyond scans to reverse aggregated COUNT, reverse
EMPTY COUNT, many-source merges, and wide row pipelines. Distinguish construction cost from
first-item latency and total consumption.

## Suggested order

The findings touch each other's code, so this is a sequencing suggestion rather than a severity
list:

1. **ITER-14.** Done.
2. **ITER-16.** Done. ITER-12's consolidation is still open and now has one fewer reason to hurry.
3. **ITER-20, ITER-06, and ITER-07's FILTER_BY_TS cost.** Done, after a reference probe that also
   withdrew ITER-19.
4. **ITER-17.** Done, along with ITER-18's carry.
5. **ITER-03, then ITER-02.** Done.
6. **ITER-09, ITER-04, ITER-11.** Done.
7. **ITER-10, ITER-05, ITER-13, ITER-12.** Done.
8. **ITER-15, then ITER-01.** Done, for their main paths.
9. **ITER-18.** Done.
10. **ITER-07's value-filter remainder.** Open, with no cheap replacement. Worth doing only if a
    workload shows it.

## Proposed filtering adapter

Replace the intermediate boxes in `create_filtered_sample_iterator` with an enum for reader
selection, then compose ordinary iterator adapters. Returning `impl Iterator` keeps the result
concrete. This is a design sketch; it has not been applied. A close variant, without LATEST's
timestamp-membership check, was compiled and timed for this review.

```rust
enum SeriesReader<'a> {
    Range(SeriesSampleIterator<'a>),
    Timestamps(TimestampFilterIterator<'a>),
}

impl Iterator for SeriesReader<'_> {
    type Item = Sample;

    fn next(&mut self) -> Option<Sample> {
        match self {
            Self::Range(iter) => iter.next(),
            Self::Timestamps(iter) => iter.next(),
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        match self {
            Self::Range(iter) => iter.size_hint(),
            Self::Timestamps(iter) => iter.size_hint(),
        }
    }
}

/// Read stored samples, insert LATEST in chronological position,
/// then enforce range and value filtering.
///
/// `latest` must follow all stored samples chronologically.
fn filtered_samples<'a>(
    series: &'a TimeSeries,
    options: &RangeOptions,
    latest: Option<Sample>,
    reverse: bool,
) -> impl Iterator<Item = Sample> + 'a {
    let (start, end) = options.get_timestamp_range();
    let start = start.max(series.get_min_timestamp());
    let value_filter = options.value_filter;

    let reader = match options.timestamp_filter.as_deref() {
        Some(timestamps) => SeriesReader::Timestamps(
            TimestampFilterIterator::new(series, timestamps, reverse),
        ),
        None => SeriesReader::Range(
            SeriesSampleIterator::from_range_options(
                series, options, reverse,
            ),
        ),
    };

    // Stored samples already pass timestamp membership through the
    // specialized reader. Check the synthetic sample separately.
    let latest = latest.filter(|sample| {
        options.timestamp_filter.as_deref().is_none_or(|timestamps| {
            timestamps.contains(&sample.timestamp)
        })
    });

    // Both branches produce the same concrete iterator type.
    let (head, tail) = if reverse {
        (latest, None)
    } else {
        (None, latest)
    };

    head.into_iter()
        .chain(reader)
        .chain(tail)
        .filter(move |sample| {
            sample.timestamp >= start
                && sample.timestamp <= end
                && value_filter.is_none_or(|filter| {
                    filter.is_match(sample.value)
                })
        })
}
```

`Option::into_iter`, `chain`, and `filter` introduce inline state without adding heap allocations.
TimestampFilterIterator still owns its timestamp list and any decoding buffer. The returned iterator
does not borrow RangeOptions: needed fields are copied, timestamp membership for LATEST is checked
during construction, and the timestamp reader owns its list.

The row path would consume it directly:

```rust
// Aggregation consumes ascending samples; reversal and COUNT come later.
let filtered = filtered_samples(series, options, latest_sample, false);

let aggregated = MultiAggregateIterator::with_empty_fill(
    filtered,
    aggregation,
    aligned_timestamp,
    empty_fill,
);

finalize(aggregated, is_reverse, options.count)
```

The scalar path would use the same helper, passing `reverse = is_reverse &&
options.aggregation.is_none()`. COUNT remains after aggregation and chronological carry. Its
aggregation adapter could then skip duplicate filtering for series-backed streams, but
`create_sample_iterator_adapter` also serves coordinator streams, so it needs splitting rather than
simplifying: by inspection, those streams were already filtered on the shards, and `reply` clears
the coordinator's filters before most of them run.

This sketch addresses ITER-06 through explicit range enforcement and removes the `chain_latest`
helper that ITER-09 is about. It does not fix the decoder and empty-chunk defects inside
TimestampFilterIterator (ITER-02, ITER-03), LATEST's context dependency (ITER-11), or the merge
machinery (ITER-12, ITER-16).

**Verdict from measurement:** Adopt this for clarity if at all, not for latency. It saves a few
boxes at construction but showed no iteration gain, and the row path got slower on compressed
encodings: Chimp 14.88 → 18.05 ns/sample, Gorilla 15.94 → 18.39 ns/sample (*Measured pipeline
cost*). If adopted, re-measure the row path, and consider dropping the range check for the `Range`
reader, which already enforces those bounds.

## Validation and limits

**Run for pass 3:**

- `valkey_timeseries` release module against `valkey-server` unstable
  (`tests/build/binaries/unstable`): one standalone node and a three-primary cluster, no
  persistence, on one machine. Later commits on this branch touch ASM and index files only, not
  `src/iterators` or its read paths.
- Command-level reproductions: ITER-14, ITER-17, ITER-18, ITER-19, ITER-20, and the ITER-08
  withdrawal.
- Server timings: ITER-01, ITER-02, ITER-07, ITER-15, and filter normalization. Each is a minimum
  over 1–9 repetitions and includes about 0.09 ms of client round trip.
- A temporary worktree, since removed, using the normal release profile (LTO, one codegen unit):
  ITER-03 through ITER-06 and the merge `Iterator` defect re-reproduced, plus `size_of`, pipeline,
  and heap timings. The worktree also made `iterators` `pub(crate)` under `cargo check` for the
  dead-code probe.
- `src/iterators` existing tests: all 65 pass under the release profile. Debug assertions were
  therefore off for this run; passes 1 and 2 ran them in debug.

**Run for the ITER-14 fix:** `SERVER_VERSION=unstable ./build.sh --parallel=auto` passed: 1,745 unit
tests, 12 doc tests (5 ignored), and 1,568 integration tests. `buf lint` (not installed locally, run
by CI) and the compat suite (`RTS_COMPAT=1`, standalone, needs Docker) were not run.

**Run for the ITER-16 fix:** `SERVER_VERSION=unstable ./build.sh --parallel=auto` passed: 1,753 unit
tests, 12 doc tests (5 ignored), and 1,568 integration tests. The compat suite ran later, with the
ITER-20 change (below).

**Run for ITER-20, ITER-06, and ITER-07:** the reference probe used the pinned `redis:8.10` image
(module version 81001) with the subject in strict mode. `RTS_COMPAT=1 SERVER_VERSION=unstable
./build.sh` passed: 1,755 unit tests, 12 doc tests (5 ignored), 1,569 integration tests, and 2,958
compat tests with 2 skipped and 13 expected divergences. An earlier parallel run failed 4
replication compat tests at setup with `failed to bind host port ... address already in use`; they
passed on the serial rerun.

**Run for ITER-17:** `RTS_COMPAT=1 SERVER_VERSION=unstable ./build.sh --parallel=auto` passed: 1,758
unit tests, 12 doc tests (5 ignored), 1,571 integration tests, and 3,298 compat tests with 2 skipped
and 13 expected divergences. On two further seeds the module matched the reference on 5,000 forward
and 5,000 reverse random cases. One earlier forward miss was `var.p` differing in the 15th
significant digit, which is unrelated.

**Run for ITER-02 and ITER-03:** `RTS_COMPAT=1 SERVER_VERSION=unstable ./build.sh --parallel=auto`
passed: 1,762 unit tests, 12 doc tests (5 ignored), 1,572 integration tests, and 3,298 compat tests
with 2 skipped and 13 expected divergences.

**Run for ITER-21:** `RTS_COMPAT=1 SERVER_VERSION=unstable ./build.sh --parallel=auto` passed: 1,775
unit tests, 12 doc tests (5 ignored), 1,589 integration tests, and 3,446 compat tests with 2 skipped
and 13 expected divergences. The compat suite compares standalone only, so cluster replies are
pinned by literals, not the reference.

**Run for ITER-18:** `RTS_COMPAT=1 SERVER_VERSION=unstable ./build.sh --parallel=auto` passed: 1,774
unit tests, 12 doc tests (5 ignored), 1,577 integration tests, and 3,446 compat tests with 2 skipped
and 13 expected divergences.

**Run for ITER-15 and ITER-01:** `RTS_COMPAT=1 SERVER_VERSION=unstable ./build.sh --parallel=auto`
passed: 1,774 unit tests, 12 doc tests (5 ignored), 1,572 integration tests, and 3,446 compat tests
with 2 skipped and 13 expected divergences.

**Run for ITER-10, ITER-05, ITER-12, and ITER-13:** `RTS_COMPAT=1 SERVER_VERSION=unstable ./build.sh
--parallel=auto` passed: 1,764 unit tests, 12 doc tests (5 ignored), 1,572 integration tests, and
3,298 compat tests with 2 skipped and 13 expected divergences.

**Run for ITER-04, ITER-09, and ITER-11:** `RTS_COMPAT=1 SERVER_VERSION=unstable ./build.sh
--parallel=auto` passed: 1,764 unit tests, 12 doc tests (5 ignored), 1,572 integration tests, and
3,298 compat tests with 2 skipped and 13 expected divergences. No A/B was run, because none of these
changes the read path.

**Not run or not established (pass 3):**



- No heap profile, allocation counting, or ASAN run.
- The no-COUNT memory figures in ITER-01 are arithmetic from `size_of`, and the multi-hour stall is
  an extrapolation. Neither was run, deliberately.
- By inspection only:
  - ITER-03's reachability, including `bulk_add` and the restore paths; no crafted payload was
    tested.
  - The statement that coordinator streams were already filtered.
- Timings come from one machine and are comparative. Interleaved minimums reduce noise but do not
  provide confidence intervals; treat single-digit percentage differences as unresolved.

This document changes no Rust behavior. Probe source was removed after the review; the tables above
preserve the cases, which should become permanent regression tests alongside their fixes.
