# Chunk Trait Cleanup Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Status:** Implemented 2026-10-16 on branch `chunk-trait-cleanup` (off `unstable` at `8d19c65d4`), ten commits. Retained as the design record; where the work changed an answer, a dated note says so inline below rather than rewriting the plan.

## Outcome

All eight tasks landed. Unit suite 1607 passing; the compression report is byte-identical before and after (84 rows). `SERVER_VERSION=unstable ./build.sh`: 1386 integration tests passing. `./build.sh compat`: 2850 passing, 2 skipped (opt-in fuzzer), 13 xfailed (registered divergences), including the new empty-uncompressed `TS.INFO DEBUG` test on RESP2 and RESP3. Notes from the work, 2026-10-16:

1. **Task 2 needed a second commit.** Borrowing the uncompressed slice alone made long scans ~55 % *slower*: `SampleIter` had no `fold`, so every sample re-matched the variant, and the slice arm compiled worse than the old owned-`Vec` arm. A dispatch-once `SampleIter::fold` fixed it *for fold-driven consumers* (`for_each`, `count`). Net, against the base, on the `for_each`-shaped `scan` bench: uncompressed 64k full scan 5.27 → 1.55 µs, 4k full 378 → 123 ns; Chimp scans −6 %; Gorilla flat. *Corrected 2026-10-16 after review (F3, F11, F12 in [chunk-trait-cleanup-review.md](./chunk-trait-cleanup-review.md)):* `Vec`'s `collect`/`extend` and every `for` loop drive `next()`, which `fold` does not reach, so the original claim that it covered them was wrong. Measured on the `next()`-driven command path (`series_scan`, a `for` loop over `TimeSeries::range_iter`), uncompressed is 0.5–8 % faster and Chimp 2.5–4 % faster than before Task 2, while Gorilla full scans are 5–7 % slower. Six source-level fixes for the Gorilla cost were tested and none recovered it; see F12.
2. **Task 6's rationale was wrong.** The two split rules are not "upsert splits early, merge splits on real overflow": `should_split` fires at 1.2× `max_size`, `needs_split` just over 1.25×. The capacity doc states the thresholds and flags them as a unification candidate with no recorded reason.
3. **Task 7 overran its ±3 % gate on Chimp `decode_full` (+3..+6 %).** The plan's `done`-flag `RangeFiltered` measured 5–19 % slower on Chimp; the old wrappers' loop shape recovered most of it (`decode_materialize` −5..−1 %). The residual is attributed to code placement — in symbol-bearing builds `ChimpDecompressor::next_sample` is the same size in both binaries and moved from offset 0 to 32 in its 64-byte line — but that is inferred, not tested. The confirming experiment is a two-binary A/B with `-C llvm-args=-align-all-functions=6` on both sides.
4. **Task 8's first A/B put two Chimp encode cases at +5.4/+5.8 %**, just over its ±5 % gate; a Chimp-only rerun of three alternations put all eight at 0..+3.5 %. Read as noise on thin evidence. Measurements throughout used two prebuilt bench binaries run alternately; full criterion baselines were too slow and criterion's paired output misaligned.
5. **One user-visible change on the RTS surface (Task 4).** `TS.INFO key DEBUG` on an empty `ENCODING UNCOMPRESSED` series now reports `endTimestamp 0` where it reported `9223372036854775807`. This converges on the reference: `tests/compat/test_compat_info.py::test_debug_on_an_empty_uncompressed_series` passes against `redis:8.10` on RESP2 and RESP3.
6. **Task 8 extended an over-reservation to Chimp (found after review, fixed 2026-10-16).** Chimp inherited Gorilla's `get_range`, which reserved the window's share of the chunk's *time span* as a sample count. Uneven timestamps break that: a window inside a long gap holds nothing but covers most of the span, and a 64 KiB chunk with two 40,000-sample clusters reserved 1,279,904 bytes for an empty result, where Chimp's old `collect` reserved none. Gorilla had the same flaw on `unstable`. The fix: a window that misses the chunk returns without decoding, a full read still reserves exactly `len`, and a partial read reserves at most `PARTIAL_RANGE_RESERVE_CAP` (256 samples) and grows from there. Pinned by `test_gap_window_range_read_reserves_at_most_the_cap`; the `range_gap` bench group in `query_scan` times it — within ±4 % except a whole-cluster partial read on Chimp, +8 %, which now regrows instead of trusting the estimate.
7. **Merging with `fastalp-encoder`.** `BlockChunk` implements these traits, so it needs: `upsert_sample` returning `bool`, `range_iter` on `ChunkOps` (wrapping its fallible iterator in `SampleIter::Boxed`), and a `Boxed` arm in the new `SampleIter::fold` and `size_hint`. `SampleCodec` is for sequential streaming codecs only; block codecs keep their own `BlockChunk` layer.
**Target:** `src/series/chunks/` — the `ChunkOps` / `Chunk` traits, their three implementations, and the series-level call sites.

**Goal:** Make the chunk traits state one contract that all three encodings honour, remove the surface nobody calls, and take the two cheap wins the review found (an empty-slice debug abort and an avoidable copy on every uncompressed range read), then collapse the duplicated compressed implementations.

**Architecture:** The trait split stays: `ChunkOps` is the `enum_dispatch` forwarding surface, `Chunk` holds the `Sized` persistence methods. Work proceeds from zero-risk fixes (Tasks 1–6, each independently shippable) to a structural change (Tasks 7–8) that replaces `GorillaChunk` and `ChimpChunk` with `XorChunk<C: SampleCodec>` and the two chunk-iterator wrappers with one `RangeFiltered<I>`. No on-disk or wire byte changes anywhere in the plan.

**Tech Stack:** Rust 2024 / MSRV 1.96, `enum_dispatch`, `get_size2`, criterion benches `encode`, `decode`, `query_scan`.

**Spec:** §1 of this document (the review findings). There is no separate spec.

Line numbers under **Files:** refer to `unstable` at `8d19c65d4`; earlier tasks shift them slightly, so treat them as anchors, not addresses.

---

## 1. Findings (the spec)

Review of the traits in [chunk.rs](../../src/series/chunks/chunk.rs), the enum in [timeseries_chunk.rs](../../src/series/chunks/timeseries_chunk.rs), the three impls, and every consumer outside the module (`time_series.rs`, `bulk_add.rs`, `defrag.rs`, `series/serialization.rs`, `series_sample_iterator.rs`, `timestamp_filter_iterator.rs`, `fanout_codec/chunks.rs`, `ts_info.rs`).

| # | Finding | Impact | Risk | Effort | Priority | Task |
|---|---|---|---|---|---|---|
| F1 | `merge_by_capacity` partial path passes an empty slice to `set_data`; Gorilla/Chimp `debug_assert!(!samples.is_empty())` aborts debug builds | 2 | 4 | 1 | 30 | 1 |
| F2 | `UncompressedChunk::range_iter` clones the matched slice into a `Vec` although `SampleIter::Slice` exists; two readers special-case `Uncompressed` to dodge it | 3 | 2 | 1 | 25 | 2 |
| F3 | Per-encoding contract drift: empty-chunk sentinels, capacity enforcement in `add_sample`, `upsert_sample` returning a post-count that callers subtract | 3 | 3 | 2 | 24 | 4, 5, 6 |
| F4 | `gorilla_chunk.rs` and `chimp_chunk.rs` are ~300 duplicated lines; differences are accidental API drift | 4 | 3 | 3 | 21 | 8 |
| F5 | Nine names for fullness across chunk, enum and series; two split thresholds (`SPLIT_FACTOR` 1.2, `SPLIT_SLACK_DIVISOR` 4) with no stated reason | 3 | 2 | 2 | 20 | 6 |
| F6 | Dead or shadowed surface: `merge_range`, `compression_ratio` ×2, `remaining_samples` ×2, `remaining_capacity` ×2, chunk-level `memory_usage` ×2, `PartialOrd`, default `debug_digest` body, redundant `is_empty` overrides and `where Self: Sized`, `CapacityFull(MAX_UNCOMPRESSED_SAMPLES)`, unreachable `UncompressedChunk::samples_by_timestamps` | 2 | 1 | 1 | 15 | 3 |
| F7 | `GorillaChunkIterator` / `ChimpChunkIterator` are the same struct; `range_iter` is a hand-written match outside the trait and `iter()` boxes | 2 | 1 | 2 | 12 | 7 |

Priority = (Impact + Risk) × (6 − Effort).

**F1 was verified** on 2026-10-16 with a throwaway test: a Gorilla destination with 40 samples of estimated headroom, an 80-sample source, and a retention floor keeping the last 20 source samples. `merge_by_capacity` took the partial branch, `split_at(min(40, 20))` left `right` empty, and `set_data(&[])` panicked at `gorilla_chunk.rs:297`. Release builds produce an empty source chunk, which is the intended outcome, so the assert is the bug.

**F2 evidence:** [uncompressed/mod.rs:105-108](../../src/series/chunks/uncompressed/mod.rs#L105-L108) and `get_range_slice` at line 135. The reverse iterator ([series_sample_iterator.rs:123-130](../../src/series/series_sample_iterator.rs#L123-L130)) and the timestamp filter ([timestamp_filter_iterator.rs:123-126](../../src/iterators/timestamp_filter_iterator.rs#L123-L126)) both branch on `Uncompressed` to borrow instead. The forward path at `series_sample_iterator.rs:155`, `has_samples_in_range`, the enum-level `samples_by_timestamps`, and `merge_by_capacity` still copy.

**F3 detail:** uncompressed reports `first_timestamp 0 / last_timestamp i64::MAX / last_value f64::MAX` when empty ([uncompressed/mod.rs:289-311](../../src/series/chunks/uncompressed/mod.rs#L289-L311)); both encoders report zeros. An empty uncompressed chunk therefore answers true to `overlaps` and `is_timestamp_in_range` for every timestamp. Uncompressed `add_sample` returns `CapacityFull`; the compressed check is commented out ([gorilla_chunk.rs:164-169](../../src/series/chunks/gorilla/gorilla_chunk.rs#L164-L169)); `append_samples` in `merge.rs` catches an error only one encoding can raise. `upsert_sample`'s post-count contract ([chunk.rs:118-125](../../src/series/chunks/chunk.rs#L118-L125)) already caused one `total_samples` bug; every caller subtracts ([time_series.rs:437-441](../../src/series/time_series.rs#L437-L441)).

**Deferred, deliberately** (not in this plan):
- Compressed `merge_samples` re-encodes through the error-swallowing `self.iter()` while `remove_range`/`upsert_sample`/`split` propagate decode errors. Fixing it means materialising the existing samples or changing `SampleMergeIterator`; measure MADD out-of-order batches first.
- `filtered_iter` is reached only by the `scan_filtered` bench; production uses `TimeSeries::get_range_filtered`. Decide what that bench should measure before touching either.
- Uncompressed chunks carry a redundant flag byte after the enum's encoding byte on both RDB and wire, and persist `max_elements` alongside `max_size`. Pre-release allows changing formats in place, but it is a separate change with its own round-trip tests.

## Global Constraints

- Every `cargo test` / `cargo bench` / `cargo run --bin` invocation passes `--features enable-system-alloc`.
- Clean-room: no RedisTimeSeries source or tests consulted, ever.
- No RDB encoding version bump and no `enc_ver` branches; nothing in this plan changes any persisted or wire byte.
- Threading rules R1–R5 untouched: no pool, GIL or executor changes.
- Nothing user-visible changes: `TS.INFO DEBUG` chunk `size` / `bytes_per_sample`, sample counts, split points. Split thresholds keep their values.
- Commit messages: Conventional Commits with scope `chunks` (or `series` where the diff is series-level). No trailers.
- Gate before every commit: `cargo fmt --check && cargo clippy --profile release --all-targets -- -D clippy::all && cargo test --features enable-system-alloc`.
- Gate before merging the branch: `SERVER_VERSION=unstable ./build.sh` and `RTS_COMPAT=1 SERVER_VERSION=unstable ./build.sh`.

## Review Focus

1. An empty slice given to `set_data` on a compressed chunk — expect an empty chunk, never an abort. (Task 1.)
2. A retention floor that clips a partially merged source to fewer samples than the destination can take — expect `merge_by_capacity` to drain the source and report what it wrote. (Task 1.)
3. An inverted or non-overlapping window on an uncompressed `range_iter` — expect `SampleIter::Empty`, not a slice panic. (Task 2.)
4. An empty chunk of any encoding asked `overlaps`, `is_timestamp_in_range`, `is_contained_by_range` — expect false. (Task 4.)
5. An out-of-order add into each encoding, and a duplicate-timestamp add under `KeepLast` — expect `total_samples` to grow by exactly one and by zero respectively. (Task 5.)

---

### Task 1: `set_data` accepts an empty slice

**Files:**
- Modify: `src/series/chunks/gorilla/gorilla_chunk.rs:296-303`
- Modify: `src/series/chunks/chimp/chimp_chunk.rs:268-271`
- Modify: `src/series/chunks/chunk.rs:152` (doc comment on `set_data`)
- Test: `src/series/chunks/timeseries_chunk_tests.rs`

**Interfaces:**
- Produces: `ChunkOps::set_data(&mut self, samples: &[Sample]) -> TsdbResult<()>` documented as "replace the chunk's contents; an empty slice empties the chunk".

- [ ] **Step 1: Write the failing tests** (next to `test_merge_by_capacity_partial_merge`, which already has the `saturate_chunk` helper)

```rust
#[test]
fn test_set_data_with_empty_slice_empties_the_chunk() {
    for chunk_type in CHUNK_TYPES {
        let mut chunk = TimeSeriesChunk::new(chunk_type, 4096);
        chunk.set_data(&generate_random_samples(50)).unwrap();
        chunk.set_data(&[]).unwrap();
        assert!(chunk.is_empty(), "{chunk_type:?}");
        assert_eq!(chunk.len(), 0, "{chunk_type:?}");
    }
}

/// `merge_by_capacity`'s partial branch: the retention floor clips the source to fewer
/// samples than `dest` can take, so `split_at` leaves nothing for `src.set_data`.
#[test]
fn test_merge_by_capacity_partial_merge_with_clipped_source() {
    for chunk_type in [ChunkEncoding::Gorilla, ChunkEncoding::Chimp] {
        let mut dest = TimeSeriesChunk::new(chunk_type, 4096);
        let mut ts = 1000i64;
        loop {
            dest.add_sample(&Sample { timestamp: ts, value: (ts % 97) as f64 * 1.37 }).unwrap();
            ts += 1000;
            let r = dest.estimate_remaining_sample_capacity();
            if dest.len() >= 16 && (4..=40).contains(&r) { break; }
            assert!(!dest.is_full());
        }
        let r = dest.estimate_remaining_sample_capacity();
        let n = 2 * r;                       // r < n and r > n / 4: partial branch
        let mut src = TimeSeriesChunk::new(chunk_type, 4096);
        for i in 0..n {
            src.add_sample(&Sample { timestamp: ts + i as i64 * 1000, value: i as f64 }).unwrap();
        }
        let keep = (r / 2).max(1);           // survivors fit entirely in dest
        let min_timestamp = ts + (n - keep) as i64 * 1000;
        let merged = merge_by_capacity(&mut dest, &mut src, min_timestamp, None).unwrap();
        assert_eq!(merged, Some(keep), "{chunk_type:?}");
        assert!(src.is_empty(), "{chunk_type:?}: source should be drained");
    }
}
```

- [ ] **Step 2: Run them and confirm the second fails with the assert**

Run: `cargo test --features enable-system-alloc test_set_data_with_empty_slice test_merge_by_capacity_partial_merge_with_clipped -- --nocapture`
Expected: both FAIL with `assertion failed: !samples.is_empty()` from `gorilla_chunk.rs`.

- [ ] **Step 3: Delete the `debug_assert!` line from both compressed `set_data` impls; remove the `// todo: complain if size > max_size` comments while there**

- [ ] **Step 4: Replace the bare `fn set_data` line in `ChunkOps` with a documented one**

```rust
/// Replace the chunk's contents with `samples` (sorted ascending, may be empty — an empty
/// slice empties the chunk). `max_size` is not enforced here; see `TimeSeries::needs_split`.
fn set_data(&mut self, samples: &[Sample]) -> TsdbResult<()>;
```

- [ ] **Step 5: Run the tests and the gate**

Run: `cargo test --features enable-system-alloc chunks`
Expected: PASS, including the two new tests.

- [ ] **Step 6: Commit**

```bash
git add src/series/chunks
git commit -m "fix(chunks): accept an empty slice in set_data"
```

---

### Task 2: Borrow, don't copy, in `UncompressedChunk::range_iter`

**Files:**
- Modify: `src/series/chunks/uncompressed/mod.rs:101-140` (`iter`, `range_iter`, `get_range_slice`), `:338-341` (`get_range`)
- Modify: `src/series/chunks/timeseries_chunk.rs:109-117` (`iter`)
- Possibly delete: `src/iterators/vec_sample_iterator.rs` and `SampleIter::Vec` in `src/iterators/sample_iter.rs`
- Test: `src/series/chunks/uncompressed/mod.rs` (tests module), `src/series/chunks/timeseries_chunk_tests.rs`

**Interfaces:**
- Produces: `UncompressedChunk::range_iter(&self, start, end) -> SampleIter<'_>` returning `SampleIter::Slice` or `SampleIter::Empty`; `TimeSeriesChunk::iter(&self) -> SampleIter<'_>` (was `Box<dyn Iterator>`).

- [ ] **Step 1: Write the failing test in the uncompressed tests module** (add `use crate::iterators::SampleIter;` to its imports)

```rust
#[test]
fn test_range_iter_borrows_the_sample_slice() {
    let samples: Vec<Sample> = (0..100).map(|i| Sample::new(i * 10, i as f64)).collect();
    let chunk = UncompressedChunk::new(4096, &samples);
    assert!(matches!(chunk.range_iter(samples[10].timestamp, samples[20].timestamp), SampleIter::Slice(_)));
    assert!(matches!(chunk.range_iter(40, 20), SampleIter::Empty));
    assert!(matches!(chunk.range_iter(i64::MAX - 1, i64::MAX), SampleIter::Empty));
    assert_eq!(chunk.range_iter(samples[10].timestamp, samples[20].timestamp).count(), 11);
}
```

- [ ] **Step 2: Run it to verify it fails**

Run: `cargo test --features enable-system-alloc test_range_iter_borrows_the_sample_slice`
Expected: FAIL on the first `matches!` (variant is `Vec`).

- [ ] **Step 3: Implement**

`range_iter`: `self.get_range_as_ref(start_ts, end_ts).map(SampleIter::slice).unwrap_or_default()`.
`get_range`: `Ok(self.get_range_as_ref(start, end).map(<[Sample]>::to_vec).unwrap_or_default())`.
Delete `get_range_slice`. Delete the inherent `UncompressedChunk::iter` if nothing outside tests calls it (check with `grep -rn "\.iter()" src/series/chunks/uncompressed`); otherwise leave it.

- [ ] **Step 4: Make `TimeSeriesChunk::iter` return `SampleIter<'_>`**

Body: `self.range_iter(i64::MIN, i64::MAX)`. Remove the `// todo: make this a trait method` comment (Task 7 does that). Fix the callers the compiler reports; `request_types.rs:461` wraps it in `Box::new` already and keeps working.

- [ ] **Step 5: Remove `SampleIter::Vec` if it is now orphaned**

Run: `grep -rn "SampleIter::Vec\|SampleIter::vec\|VecSampleIterator\|From<Vec<Sample>>" src benches tools`
If the only hits are the definitions in `sample_iter.rs` and `vec_sample_iterator.rs`, delete the variant, its two constructors (`vec`, `From<Vec<Sample>>`, `From<VecSampleIterator>`), the file, and its `mod` line in `src/iterators/mod.rs`. Otherwise leave them.

- [ ] **Step 6: Run the gate**

Run: `cargo fmt --check && cargo clippy --profile release --all-targets -- -D clippy::all && cargo test --features enable-system-alloc`
Expected: PASS.

- [ ] **Step 7: Measure**

Run before (on `unstable`) and after: `cargo bench --features enable-system-alloc --bench query_scan -- 'scan/uncompressed'`
Expected: uncompressed `scan` cases faster or flat; gorilla/chimp unchanged. Put the two numbers for `uncompressed/.../64k/full` in the commit body.

- [ ] **Step 8: Commit**

```bash
git add src/series/chunks src/iterators
git commit -m "perf(chunks): borrow the sample slice in UncompressedChunk::range_iter"
```

---

### Task 3: Remove the surface nobody calls

**Files:**
- Modify: `src/series/chunks/chunk.rs:96-186` (trait docs and the default `debug_digest` body)
- Modify: `src/series/chunks/timeseries_chunk.rs` (`merge_range` 187-198, `memory_usage` stays, `PartialOrd` impl 361-365, `samples_by_timestamps` 127-183)
- Modify: `src/series/chunks/gorilla/gorilla_chunk.rs` (`compression_ratio` 47-54, `remaining_capacity` 72-77, `remaining_samples` 81-86, `memory_usage` 88-90, `is_empty` 131-133, `where Self: Sized` 305-308)
- Modify: `src/series/chunks/chimp/chimp_chunk.rs` (same six items at 54-63, 82-84, 89-94, 96-98, 122-124, 275)
- Modify: `src/series/chunks/uncompressed/mod.rs:330-335` (`add_sample` error payload), `:440-442` (`where Self: Sized`)
- Test: `src/series/chunks/timeseries_chunk_tests.rs:1333-1352` (`test_remaining_capacity_saturates_on_an_over_full_chunk`)

**Interfaces:**
- Produces: `Chunk::debug_digest(&self, dig: &mut Digest);` is a required method with no default and no `Hash` bound. `TimeSeriesChunk::samples_by_timestamps` dispatches the `Uncompressed` variant to `UncompressedChunk::samples_by_timestamps`.

- [ ] **Step 1: Re-point the over-full test at the surviving API**

Rewrite `test_remaining_capacity_saturates_on_an_over_full_chunk` to assert `chunk.estimate_remaining_sample_capacity() == 0` for each over-full Gorilla and Chimp chunk, keeping its setup and doc comment (the comment now explains that `estimate_remaining_sample_capacity` guards `used >= total`). Run it; expected PASS before any deletion.

- [ ] **Step 2: Add the dispatch arm and a test that reaches it**

At the top of `TimeSeriesChunk::samples_by_timestamps`: `if let TimeSeriesChunk::Uncompressed(c) = self { return c.samples_by_timestamps(timestamps); }`. The existing `test_samples_by_timestamps` and `test_samples_by_timestamps_partial_overlap` loop `CHUNK_TYPES` and now exercise it; run them.

- [ ] **Step 3: Delete, in this order, compiling after each file**

1. `TimeSeriesChunk::merge_range`, `impl PartialOrd for TimeSeriesChunk`.
2. Gorilla and Chimp: `compression_ratio`, `remaining_capacity`, `remaining_samples`, `memory_usage`, the `is_empty` override, `where Self: Sized` on `split`.
3. `Chunk::debug_digest` default body (keep the signature, drop `where Self: Hash`) and the now-unused imports `DeterministicHasher`, `BuildHasher`, `Hash`, `Hasher`, `GetSize` in `chunk.rs`.
4. Uncompressed: `CapacityFull(MAX_UNCOMPRESSED_SAMPLES)` becomes `CapacityFull(self.max_elements)`; `where Self: Sized` on `split`.

Run after each: `RUSTFLAGS="-D warnings" cargo build --features enable-system-alloc --all-targets`
Expected: clean. A `dead_code` warning after step 3 means a test still calls something; update the test, do not restore the method.

- [ ] **Step 4: Run the gate**

Run: `cargo fmt --check && cargo clippy --profile release --all-targets -- -D clippy::all && cargo test --features enable-system-alloc`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/series/chunks
git commit -m "refactor(chunks): remove unused chunk helpers and defaults"
```

---

### Task 4: One empty-chunk contract

**Files:**
- Modify: `src/series/chunks/uncompressed/mod.rs:289-311` (`first_timestamp`, `last_timestamp`, `last_value`)
- Modify: `src/series/chunks/timeseries_chunk.rs:83-95` (`is_timestamp_in_range`, `is_contained_by_range`, `overlaps`)
- Modify: `src/series/chunks/chunk.rs:103-109` (doc comments)
- Test: `src/series/chunks/timeseries_chunk_tests.rs`

**Interfaces:**
- Produces: for every encoding an empty chunk reports `first_timestamp() == 0`, `last_timestamp() == 0`, `last_value() == 0.0`; `overlaps`, `is_timestamp_in_range`, `is_contained_by_range` return `false` on an empty chunk.

- [ ] **Step 1: Write the failing test**

```rust
#[test]
fn test_empty_chunk_contract_is_the_same_for_every_encoding() {
    for chunk_type in CHUNK_TYPES {
        let chunk = TimeSeriesChunk::new(chunk_type, 4096);
        assert_eq!(chunk.first_timestamp(), 0, "{chunk_type:?}");
        assert_eq!(chunk.last_timestamp(), 0, "{chunk_type:?}");
        assert_eq!(chunk.last_value(), 0.0, "{chunk_type:?}");
        assert!(chunk.last_sample().is_none(), "{chunk_type:?}");
        assert!(!chunk.overlaps(i64::MIN, i64::MAX), "{chunk_type:?}");
        assert!(!chunk.is_timestamp_in_range(0), "{chunk_type:?}");
        assert!(!chunk.is_contained_by_range(i64::MIN, i64::MAX), "{chunk_type:?}");
    }
}
```

- [ ] **Step 2: Run it to verify it fails**

Run: `cargo test --features enable-system-alloc test_empty_chunk_contract`
Expected: FAIL on `last_timestamp` for `Uncompressed` (returns `i64::MAX`).

- [ ] **Step 3: Implement**

Uncompressed: return `0`, `0`, `0.0` on empty. Enum: prefix each of the three predicates with `!self.is_empty() &&`. Trait docs on `first_timestamp` / `last_timestamp` / `last_value`: "`0` (`0.0`) when the chunk is empty; check `is_empty()` or use `last_sample()` before relying on it."

- [ ] **Step 4: Run the whole suite**

Run: `cargo test --features enable-system-alloc`
Expected: PASS. A failure in `time_series_tests` or `series_sample_iterator` tests means a reader relied on the `i64::MAX` sentinel; fix the reader to check `is_empty()`, not the sentinel.

- [ ] **Step 5: Commit**

```bash
git add src/series/chunks
git commit -m "fix(chunks): one empty-chunk contract across encodings"
```

---

### Task 5: `upsert_sample` reports whether a sample was inserted

**Files:**
- Modify: `src/series/chunks/chunk.rs:113-125` (trait signature + doc)
- Modify: `src/series/chunks/uncompressed/mod.rs:343-367`, `gorilla_chunk.rs:190-243`, `chimp_chunk.rs:170-216`
- Modify: `src/series/chunks/timeseries_chunk.rs:209-220` (`upsert`)
- Modify: `src/series/time_series.rs:436-470`
- Test: `src/series/chunks/timeseries_chunk_tests.rs`, `src/series/time_series_tests.rs`, plus the existing `upsert_sample` assertions in `gorilla_chunk.rs` tests, `chimp_chunk_tests.rs`, `uncompressed/mod.rs` tests

**Interfaces:**
- Produces: `ChunkOps::upsert_sample(&mut self, sample: Sample, dp_policy: DuplicatePolicy) -> TsdbResult<bool>` — `Ok(true)` when the chunk grew by one sample, `Ok(false)` when an existing sample at that timestamp was updated. `TimeSeriesChunk::upsert(&mut self, sample, policy) -> (bool, SampleAddResult)` with the same `bool`, `false` on error.

- [ ] **Step 1: Write the failing tests**

Chunk level, in `timeseries_chunk_tests.rs`:

```rust
#[test]
fn test_upsert_reports_insert_versus_update() {
    for chunk_type in CHUNK_TYPES {
        let mut chunk = TimeSeriesChunk::new(chunk_type, 4096);
        assert_eq!(chunk.upsert_sample(Sample::new(100, 1.0), DuplicatePolicy::KeepLast).unwrap(), true);
        assert_eq!(chunk.upsert_sample(Sample::new(300, 3.0), DuplicatePolicy::KeepLast).unwrap(), true);
        assert_eq!(chunk.upsert_sample(Sample::new(200, 2.0), DuplicatePolicy::KeepLast).unwrap(), true);
        assert_eq!(chunk.upsert_sample(Sample::new(200, 9.0), DuplicatePolicy::KeepLast).unwrap(), false);
        assert_eq!(chunk.len(), 3, "{chunk_type:?}");
        assert_eq!(chunk.get_range(200, 200).unwrap()[0].value, 9.0, "{chunk_type:?}");
    }
}
```

Series level, in `time_series_tests.rs`:

```rust
#[test]
fn test_total_samples_tracks_out_of_order_and_duplicate_adds_per_encoding() {
    for encoding in [ChunkEncoding::Uncompressed, ChunkEncoding::Gorilla, ChunkEncoding::Chimp] {
        let mut ts = TimeSeries::with_options(TimeSeriesOptions {
            chunk_encoding: encoding,
            ..Default::default()
        })
        .unwrap();
        ts.add(100, 1.0, None);
        ts.add(300, 3.0, None);
        ts.add(200, 2.0, None);
        assert_eq!(ts.total_samples, 3, "{encoding:?}: out-of-order add");
        ts.add(200, 9.0, Some(DuplicatePolicy::KeepLast));
        assert_eq!(ts.total_samples, 3, "{encoding:?}: duplicate add");
    }
}
```

- [ ] **Step 2: Run them to verify they fail to compile** (`usize` is not `bool`)

Run: `cargo test --features enable-system-alloc test_upsert_reports_insert_versus_update`
Expected: compile error on the return type.

- [ ] **Step 3: Implement**

Trait: change the return type and replace the doc paragraph with the one-line contract above. Gorilla/Chimp: `Ok(!duplicate_found)`; the empty-chunk early return becomes `Ok(true)`. Uncompressed: `Ok(self.len() > count)`, and delete the long comment about the delta bug (the type now prevents it). `TimeSeriesChunk::upsert`: `Ok(inserted) => (inserted, SampleAddResult::Ok(sample))`, error arms `(false, ..)`.

`time_series.rs` upsert path: replace each `let old_size = chunk.len(); let (size, res) = chunk.upsert(..)` / `size.saturating_sub(old_size)` pair with `let (inserted, res) = chunk.upsert(..)` and `usize::from(inserted)`; the `if res.is_ok()` guards stay.

- [ ] **Step 4: Update the existing per-encoding `upsert_sample` tests** that assert a returned count to assert the `bool` and then `len()`.

- [ ] **Step 5: Run the gate**

Run: `cargo fmt --check && cargo clippy --profile release --all-targets -- -D clippy::all && cargo test --features enable-system-alloc`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add src/series
git commit -m "refactor(chunks): upsert_sample reports whether a sample was inserted"
```

---

### Task 6: Document the capacity contract in one place

**Files:**
- Modify: `src/series/chunks/chunk.rs:96-101` (trait-level doc on `ChunkOps`), `:111-113` (`add_sample` doc)
- Modify: `src/series/chunks/timeseries_chunk.rs:204-206` (`should_split` doc)
- Modify: `src/series/time_series.rs:501-511` (`needs_split` doc cross-reference)

**Interfaces:** none; doc-only, values unchanged.

- [ ] **Step 1: Replace the `ChunkOps` doc comment** with a `# Capacity` section stating, one line each: `size` is encoded payload bytes (what `TS.INFO DEBUG` reports); `max_size` is the budget; `is_full` is `size >= max_size` for compressed encodings and `len >= max_elements` for uncompressed; `bytes_per_sample` is `size / len` floored to 1, or `SAMPLE_SIZE / 2` below `MIN_SAMPLES_FOR_BPS_ESTIMATE`; `TimeSeriesChunk::estimate_remaining_sample_capacity` and `utilization` derive from those; two split rules exist — `should_split` (upsert into a sealed chunk, `utilization >= SPLIT_FACTOR`) and `TimeSeries::needs_split` (after a merge, `is_full && size > max + max / SPLIT_SLACK_DIVISOR`) — and why they differ: an upsert re-encodes the whole chunk anyway so it splits early, a merge splits only on real overflow.

- [ ] **Step 2: Document `add_sample`**: "Uncompressed refuses once `max_elements` is reached (`CapacityFull`); compressed encodings never refuse — the series checks `is_full()` first (`add_sample_internal`) and `merge::append_samples` treats `CapacityFull` as a batch-level failure."

- [ ] **Step 3: Add the two cross-reference lines** on `should_split` and `needs_split` pointing at each other and at the `# Capacity` section.

- [ ] **Step 4: Verify the docs build and the gate passes**

Run: `cargo doc --no-deps --features enable-system-alloc 2>&1 | grep -i "warning\|error"; cargo test --doc --features enable-system-alloc`
Expected: no doc warnings for `chunks`; doc tests PASS.

- [ ] **Step 5: Commit**

```bash
git add src/series/chunks/chunk.rs src/series/chunks/timeseries_chunk.rs src/series/time_series.rs
git commit -m "docs(chunks): state the capacity contract once"
```

---

### Task 7: One range-filtering iterator; `range_iter` on the trait

**Files:**
- Create: `src/series/chunks/range_iter.rs`
- Modify: `src/series/chunks/mod.rs` (add `mod range_iter; pub use range_iter::*;`)
- Modify: `src/series/chunks/gorilla/gorilla_chunk.rs:378-437` and `chimp/chimp_chunk.rs:339-396` (delete the wrapper structs; keep type aliases)
- Modify: `src/iterators/sample_iter.rs` (variant payload types)
- Modify: `src/series/chunks/chunk.rs` (add `range_iter`, default `iter`), `timeseries_chunk.rs:109-125` (delete the hand-written `iter`/`range_iter`), the inherent `iter`/`range_iter` on all three chunk types
- Test: `src/series/chunks/timeseries_chunk_tests.rs`

**Interfaces:**
- Produces:

```rust
/// Yields the samples of `inner` with timestamps in `[start, end]`. A decode error ends the
/// iteration (logged in debug builds).
pub struct RangeFiltered<I> { inner: I, start: Timestamp, end: Timestamp, started: bool }
impl<I> RangeFiltered<I> { pub fn new(inner: I, start: Timestamp, end: Timestamp) -> Self }
impl<I: Iterator<Item = TsdbResult<Sample>> + ExactSizeIterator> Iterator for RangeFiltered<I> {
    type Item = Sample;
    fn size_hint(&self) -> (usize, Option<usize>) { (0, Some(self.inner.len())) }
}
pub type GorillaChunkIterator<'a> = RangeFiltered<GorillaIterator<'a>>;
pub type ChimpChunkIterator<'a> = RangeFiltered<ChimpIterator<'a>>;
```

and on `ChunkOps`:

```rust
fn range_iter(&self, start: Timestamp, end: Timestamp) -> SampleIter<'_>;
fn iter(&self) -> SampleIter<'_> { self.range_iter(i64::MIN, i64::MAX) }
```

If `enum_dispatch` rejects the elided lifetime, write it as `fn range_iter<'a>(&'a self, start: Timestamp, end: Timestamp) -> SampleIter<'a>;`.

- [ ] **Step 1: Write the failing test**

```rust
#[test]
fn test_range_iter_upper_size_hint_is_the_chunk_length() {
    for chunk_type in [ChunkEncoding::Gorilla, ChunkEncoding::Chimp] {
        let mut chunk = TimeSeriesChunk::new(chunk_type, 4096);
        chunk.set_data(&generate_random_samples(64)).unwrap();
        assert_eq!(chunk.range_iter(i64::MIN, i64::MAX).size_hint().1, Some(64), "{chunk_type:?}");
        assert_eq!(chunk.iter().count(), 64, "{chunk_type:?}");
    }
}
```

- [ ] **Step 2: Run it to verify it fails** (`size_hint().1` is `None` today)

Run: `cargo test --features enable-system-alloc test_range_iter_upper_size_hint`
Expected: FAIL.

- [ ] **Step 3: Create `range_iter.rs`** with the struct above. `next` keeps today's two-phase logic from `GorillaChunkIterator`: before the first yield skip samples below `start`; stop permanently at the first sample above `end` or on `Err`. Keep the `log_warning` under `#[cfg(debug_assertions)]` in one place.

- [ ] **Step 4: Replace the two wrapper structs with the type aliases, switch `SampleIter::Gorilla` / `SampleIter::Chimp` payloads to the aliases (Chimp stays boxed), and make the three chunk `range_iter`s build `RangeFiltered::new(self.encoder.iter(), start, end).into()`.**

- [ ] **Step 5: Move `range_iter` and `iter` onto `ChunkOps`**; delete `TimeSeriesChunk::iter`, `TimeSeriesChunk::range_iter`, and the inherent `iter` on Gorilla/Chimp (Uncompressed's inherent `iter`, if it survived Task 2, goes too). Fix call sites the compiler reports; none should need more than an import of `ChunkOps`.

- [ ] **Step 6: Run the gate and the decode bench**

Run: `cargo fmt --check && cargo clippy --profile release --all-targets -- -D clippy::all && cargo test --features enable-system-alloc`
Expected: PASS.
Run: `cargo bench --features enable-system-alloc --bench decode -- --save-baseline task7` (and before Task 7, `--save-baseline pre_task7` on the previous commit); compare with `--baseline pre_task7`.
Expected: `decode_full` and `decode_materialize` within noise (± 3 %) for gorilla and chimp.

- [ ] **Step 7: Commit**

```bash
git add src/series/chunks src/iterators
git commit -m "refactor(chunks): one range-filtering iterator; range_iter on ChunkOps"
```

---

### Task 8: `XorChunk<C: SampleCodec>` replaces the two compressed chunk bodies

**Files:**
- Create: `src/series/chunks/xor_chunk.rs` (trait `SampleCodec`, struct `XorChunk<C>`, its `ChunkOps` + `Chunk` + `GetSize` impls, `estimate_samples_in`, `push_sample`)
- Modify: `src/series/chunks/mod.rs` (add `mod xor_chunk; pub use xor_chunk::*;`)
- Modify: `src/series/chunks/gorilla/gorilla_chunk.rs` → `impl SampleCodec for GorillaEncoder`, `pub type GorillaChunk = XorChunk<GorillaEncoder>;`, existing tests unchanged
- Modify: `src/series/chunks/chimp/chimp_chunk.rs` → `impl SampleCodec for ChimpCompressor`, `pub type ChimpChunk = XorChunk<ChimpCompressor>;`
- Test: existing `gorilla_chunk.rs` tests, `chimp_chunk_tests.rs`, `timeseries_chunk_tests.rs`, `proptest_roundtrip.rs`

**Interfaces:**
- Produces:

```rust
pub(crate) trait SampleCodec: Clone + Debug + PartialEq + Hash + GetSize + Sized {
    type Iter<'a>: Iterator<Item = TsdbResult<Sample>> + ExactSizeIterator where Self: 'a;
    const ENCODING: ChunkEncoding;
    fn with_soft_cap(max_size: usize) -> Self;
    fn set_soft_cap(&mut self, max_size: usize);
    fn push(&mut self, sample: &Sample) -> std::io::Result<()>;
    fn iter(&self) -> Self::Iter<'_>;
    fn bytes(&self) -> &[u8];
    fn len(&self) -> usize;
    fn first_timestamp(&self) -> Timestamp;
    fn last_timestamp(&self) -> Timestamp;
    fn last_value(&self) -> f64;
    fn clear(&mut self);
    fn shrink_to_fit(&mut self);
    fn rdb_save(&self, rdb: *mut RedisModuleIO);
    fn rdb_load(rdb: *mut RedisModuleIO) -> ValkeyResult<Self>;
    fn serialize(&self, dest: &mut Vec<u8>);
    fn deserialize(buf: &[u8]) -> TsdbResult<Self>;
    fn debug_digest(&self, dig: &mut Digest);
    fn into_sample_iter<'a>(iter: RangeFiltered<Self::Iter<'a>>) -> SampleIter<'a>;
}

#[derive(Debug, Clone, PartialEq, Hash)]
pub struct XorChunk<C: SampleCodec> { pub(crate) encoder: C, pub max_size: usize }
impl<C: SampleCodec> XorChunk<C> {
    pub fn with_max_size(max_size: usize) -> Self;
    pub fn data_size(&self) -> usize;          // encoder.bytes().len()
}
impl<C: SampleCodec> Default for XorChunk<C>   // with_max_size(DEFAULT_CHUNK_SIZE_BYTES)
impl<C: SampleCodec> GetSize for XorChunk<C>   // manual: forwards get_heap_size_with_tracker to encoder
```

Field names `encoder` and `max_size` are kept so existing tests that read them compile unchanged.

- [ ] **Step 1: Record the before state**

Run: `cargo bench --features enable-system-alloc --bench encode -- --save-baseline pre_task8 && cargo bench --features enable-system-alloc --bench decode -- --save-baseline pre_task8 && tools/compression_report.sh --save-baseline`
Expected: baselines written; `benches/baselines/compression_baseline.csv` updated (do not commit the csv change unless the ratios moved, which they must not).

- [ ] **Step 2: Write `xor_chunk.rs`** by moving the `GorillaChunk` struct, its inherent impl, and its `ChunkOps` and `Chunk` impls out of `gorilla_chunk.rs` into generic form: every `GorillaEncoder::with_soft_cap` becomes `C::with_soft_cap`, `self.encoder.buf()` becomes `self.encoder.bytes()`, `self.encoder.num_samples` becomes `self.encoder.len()`, `encoder.add_sample(sample)` becomes `encoder.push(sample)`. Keep Gorilla's `get_range` (estimate + `try_reserve_exact`) as the single implementation. `range_iter` builds `C::into_sample_iter(RangeFiltered::new(self.encoder.iter(), start, end))`. `with_max_size`, `compress`, `remove_range`, `upsert_sample`, `merge_samples`, `split`, `save_rdb`, `load_rdb`, `serialize`, `deserialize`, `debug_digest`, `bytes_per_sample`, `is_full`, `optimize`, `clear`, `set_data` move verbatim otherwise.

- [ ] **Step 3: Implement `SampleCodec` for `GorillaEncoder`** in `gorilla_chunk.rs` (thin forwards; `push` calls `add_sample`, `len` returns `num_samples`, `into_sample_iter` returns `SampleIter::Gorilla(iter)`), add the type alias, delete the old struct and impls. Compile.

- [ ] **Step 4: Implement `SampleCodec` for `ChimpCompressor`** in `chimp_chunk.rs` (`push` calls `add(*sample)`, `len` is `count() as usize`, `into_sample_iter` returns `SampleIter::Chimp(Box::new(iter))`), add the alias, delete the old struct and impls. Compile.

- [ ] **Step 5: Run the full gate**

Run: `cargo fmt --check && cargo clippy --profile release --all-targets -- -D clippy::all && cargo test --features enable-system-alloc`
Expected: PASS with no test edits beyond imports.

- [ ] **Step 6: Prove the bytes did not move**

Run: `tools/compression_report.sh --check`
Expected: exit 0, no ratio changed (the encoders are untouched; this guards the wrapper).

- [ ] **Step 7: Prove the speed did not move**

Run: `cargo bench --features enable-system-alloc --bench encode -- --baseline pre_task8 && cargo bench --features enable-system-alloc --bench decode -- --baseline pre_task8`
Expected: every `encode_bulk`, `encode_append`, `decode_full`, `decode_materialize` case within ± 5 %. If any case regresses past that, repeat as an interleaved two-binary A/B before concluding (see the bench-profiling notes in `docs/`); a confirmed regression is fixed by adding `#[inline]` on the `SampleCodec` forwards, not by reverting the generic.

- [ ] **Step 8: Commit**

```bash
git add src/series/chunks
git commit -m "refactor(chunks): XorChunk<C> generic over a SampleCodec"
```

---

## Finishing the branch

- [ ] `SERVER_VERSION=unstable ./build.sh` — integration suite green.
- [ ] `RTS_COMPAT=1 SERVER_VERSION=unstable ./build.sh` — no new divergences (nothing user-visible changed).
- [ ] Update this document's **Status** line and note anything a task got wrong, inline and dated, as the other plans in this directory do.
