# Chimp Encoder Latency and Compression Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Bring Chimp encode latency from 2.3–2.6× Gorilla to ≈1.3× (≤1× on integer and
dyadic data) and close its 20–35 % size deficit on counters, discrete sets and quantized
decimals, without giving up its 2.5–3.2× advantage on quantized data.

**Architecture:** Phase A is byte-neutral encoder work: skip the ELF digit search for values it
can never erase, replace the climb/strip search with a top-down branchless count, skip runs
of repeated samples a word at a time in the decoder, and fix a decoder validation gap. Phase B changes the Chimp bit grammar in place (the library is
unreleased): the 4-bit ELF header carries the decimal-place count α instead of β\*, the
encoder verifies the previous α before searching, Chimp flag `10` reuses the full
leading/trailing window, the leading-zero code gains range, and the timestamp escape gets
a 16-bit tier. Phase C bumps the module encoding version so pre-change payloads are rejected
instead of misdecoded, refreshes baselines, fixtures and docs, and writes the evaluation
report in the style of `docs/benchmarks/gorilla-evaluation.md`.

**Tech Stack:** Rust (edition 2024, MSRV 1.96), `cargo test --features enable-system-alloc`,
`tools/compression_report.sh`, `tools/latency_report.sh`, `tools/gorilla_report.rs` (extended
with `--encoding`), pytest integration suite via `./build.sh`.

**Spec:** this document's *Background* section plus the probe notes it preserves in Task 1
(`docs/benchmarks/data/chimp-review/notes.md`, today at
`target/bench-reports/chimp-review/notes.md`, an ignored directory). Code under evaluation:
`src/series/chunks/chimp/{compressor,elf64,encoder}.rs`, `src/series/chunks/stream/`.

## Global Constraints

- Clean-room rule (AGENTS.md): derive nothing from RedisTimeSeries source or tests.
- Every `cargo test`/`cargo bench`/tool invocation needs `--features enable-system-alloc`
  (tools also need `test-utils`).
- Commit messages: Conventional Commits, terse subject, at most a 1–2 line body, no
  co-author trailer.
- Format changes are made **in place**: no `enc_ver` branches, no legacy Chimp read path.
  `TIMESERIES_TYPE_ENCODING_VERSION` is currently `2` (`src/series/series_data_type.rs:36`),
  `MIN_…` is `1` (`:39`). Phase C sets both to `3` (decision point 1 below). Local
  `dump.rdb`/AOF files from older builds stop loading and must be deleted.
  **Outcome:** the bump to `3` was reverted in `bebd629c3`. The module writes version `2`
  and reads `1`–`2`, the Chimp grammar changed in place, and a Chimp chunk in a version `1`
  payload is refused at load (`MIN_CHIMP_GRAMMAR_ENCODING_VERSION`).
- Wire encoding for fan-out stays decided in `samples_to_chunk[_lossless]`
  (`src/series/chunks/serialization.rs:26-44`); no new encoding-selection tier.
- Any change under `src/series/chunks/{stream,gorilla,chimp}` is re-profiled before it lands:
  `#[inline(always)]` placements are measured, not assumed (see the September codec work).
- Byte-neutral tasks must leave `tools/compression_report.sh --check` identical on all 84
  rows. Format tasks refresh the baseline only in Task 12.
- Latency claims come from a three-round interleaved A/B (Task 4 harness), 1,000 and
  10,000 samples, medians of per-round medians, exactly as the Gorilla evaluation did.
  Single-round numbers are not cited.

## Decision points (need sign-off before the task that uses them)

1. **Encoding version (Task 12).** Recommended: `TIMESERIES_TYPE_ENCODING_VERSION = 3` and
   `MIN_TIMESERIES_TYPE_ENCODING_VERSION = 3`. Chimp chunks carry no grammar marker, so a
   same-version reader would decode a pre-change payload into garbage silently; raising MIN
   makes it fail loudly. Consequence: the Gorilla v1 RDB read path becomes unreachable from
   persistence (its golden fixture tests still exercise the stream reader). Alternative:
   keep `2` and accept silent misdecode of stale local dumps.
   **Outcome:** version `2` was kept, with minimum `1`. Instead of raising the minimum, the
   loader refuses only Chimp chunks from version `1` payloads, so stale Chimp dumps fail
   loudly while version `1` Gorilla and uncompressed chunks still load.
2. **Fan-out wire guard (Task 14, optional).** The probe notes point out that an RDB
   version bump does not protect a mixed-version cluster coordinator. For an unreleased
   product the rule "all nodes run the same build" may suffice; Task 14 adds a cheap
   version check to the fan-out contract if you want it.
   **Outcome:** done in `39215071f`: `SampleData.version` is now `2` and a mismatch fails the
   reply. Coordinators from before that check ignore the field, so `TS.MRANGE` replies also
   carry the `MESSAGE_FEATURE_CHUNK_GRAMMAR_V2` envelope bit, which those coordinators refuse.
   A mixed-build cluster errors in both directions; the same-build rule stays.
3. **Leading-zero code width (Task 10).** Data-driven inside the task: 3-bit re-spread vs
   4-bit code, chosen by total bytes over the 45-case matrix with a no-regression bound on
   the 36 base cases.

## Review Focus

Inputs the evidence implies but the homogeneous benchmark shapes never exercise:

1. A series that alternates quantized and full-precision stretches (agent restarts, a
   sensor switching precision): the α hint must not make the full-precision stretch slower
   than today, and the stream must round-trip. → Task 8 test
   `mixed_precision_stream_round_trips`, plus the mixed latency row in Task 13.
2. Negative erased values under α-recovery (`−100.23`, `−0.01`): recovery must round toward
   zero on the magnitude, not `floor` the signed value. → Task 7 test
   `negative_quantized_values_recover_bit_exactly`.
3. A chunk persisted mid-window (after a `01` header set `stored_tz`, or with `last_alpha`
   set) then reloaded and appended to: encoder and decoder state must agree. → Task 9 test
   `append_after_reload_reuses_persisted_window`.
4. A pre-change payload (`DUMP`/`TS._RESTORE` blob with encver 2) must be rejected with the
   existing version error, never decoded. → Task 12 (`test_restore_rejects_unsupported_encoding_version`
   gains version 2 in its rejected set).
5. Corrupt or adversarial headers: `01` with `lz + sig > 64`, an α field whose recovery
   scale overflows, truncated tiers in the new timestamp escape. → Task 2 test
   `corrupt_trailing_window_header_is_rejected`, Task 7 test `recover_rejects_out_of_range_alpha`,
   Task 11 test `truncated_timestamp_escape_reports_eof`.

---

## Background: evidence this plan argues from

Measurements are local chunk microbenchmarks (M2, 1,000 samples), not server numbers.

**Where Chimp stands (fresh `latency_report`, regular timestamps, µs per 1,000 samples):**

| shape | encode Chimp / Gorilla | decode ns/sample Chimp / Gorilla | B/sample Chimp / Gorilla |
|---|---:|---:|---:|
| drift, noisy, periodic, bursty | 19–21 / 7.8–8.6 | 10.4–11.6 / 9.5–9.8 | ≈ parity |
| counter (integers) | 22.2 / 8.6 | 8.5 / 9.6 | 2.38 / 1.76 |
| discrete {0, .25, .5, 1} | 21.8 / 8.7 | 10.0 / 9.5 | 2.21 / 1.34 |
| drift_q2, bursty_q2 | 10.4–12.1 / 8.7 | 11.3 / 10.8 | 1.49 / 4.26, 1.93 / 4.80 |
| noisy_q2, periodic_q2 | 14.3–17.0 / 8.3–9.0 | 12.1–13.5 / 10.4–10.9 | 3.67 / 7.17, 2.73 / 7.19 |
| constant | 4.8 / 4.1 | 6.0 / 10.6 | 0.52 / 0.27 |

**Why encode is slow.** `write_value` (`compressor.rs:335-420`) runs
`get_alpha_and_beta_star` on every distinct finite nonzero value: a table scan for the
decimal scale, then `get_significant_count` (`elf64.rs:177-224`) multiplies by `10^i`,
divides back (an `fdiv` feeding a data-dependent branch that mispredicts ~50 % on
full-precision data), then strips trailing decimal zeros one dependent `% 10` at a time
(13 iterations for a counter near 5,000, 15 for `0.25`). Only then does `try_recover` run
the real check. Three structural problems: (a) integers and short dyadic fractions can never
be erased yet pay the full search; (b) `last_beta_star` only moves on a successful erasure
(`compressor.rs:397`), so after one erasable value a full-precision stream climbs ~12 search
iterations per value forever; (c) the division check is redundant with `try_recover`.

**Probe results (standalone `rustc -O` probes, byte-exact against production; CSVs in
`target/bench-reports/chimp-review/`, to be preserved by Task 1):**

- Bypassing ELF for integers (`v.trunc() == v`) or for `v*16` integral produced
  **byte-identical serialized encoders including final state** across 42 dataset cases and
  3,150 boundary values. Encode: counter 22.0 → 7.0 µs, discrete 20.4 → 5.5 µs, counter+2⁵⁰
  16.3 → 6.0 µs. The broader grid check cost a little on full-precision rows (periodic was
  slower in isolation), so the classifier must be benchmarked, not assumed.
- Full XOR-window reuse (flag `10` reuses leading *and* trailing counts, cost-compared
  against a fresh header, window retained over repeats, trailing reset on a plain `11`):
  drift_q2 1,491 → 997 B (−33.1 %), periodic_q2 −21.4 %, counter −20.3 %, bursty_q2 −19.3 %,
  noisy_q2 −14.9 %; jitter/irregular rows −10…−18 %; full-precision rows ≈ 0 and slightly
  more encode work; counter+2⁵⁰ **0 %** (leading-zero code caps at 24). Round-tripped through
  a matching experimental decoder on all cases. Persistence of `stored_tz` was not built;
  decode time was not measured.
- Retaining `stored_lz` across repeats and `01` headers (format-compatible): 0–3 B per
  1,000 samples. Not worth a task on its own; the window change subsumes it.
- With leading-count reuse available, the fixed `tz > 6` threshold is wrong by 1–2 bits for
  `tz` 7–8 and a tie at 9; choose by cost, not threshold.
- Timestamp escape: today 13–32-bit deltas cost 37 bits (three writes); a 16-bit tier
  (`11110`+16 = 21 bits) with 38/70-bit escapes saves 3–9 % on irregular rows (size model,
  constant irregular 2,558 → 2,320 B, drift_q2 3,558 → 3,292 B, drift 8,086 → 7,830 B) and
  nothing on regular/jitter. Fusing the current escape into one 37-bit write is
  byte-identical; timing was workload-sensitive.
- Correctness: `ChimpDec::next_value` flag `01` accepts `lz + sig > 64`, computes a negative
  `stored_tz` and shifts by `232u8`; reproduced as a debug-assertion panic.

**Derived in the evaluation:** a value with `k = 52 − tz(mantissa) − E` fractional mantissa
bits (`E` the unbiased exponent) has a shortest decimal of exactly `k` fractional digits, so
`erase_bits = 52 − f(k) − E ≤ tz` because `f(α) ≥ α`; the erased delta is zero and the value
is stored raw. Hence any double with `k ≤ 15` (all integers, all 1/2ⁿ grids up to n = 15)
can be classified raw with integer ops only — the generalization of the two probe variants.
Transmitting α rather than β\* removes `get_sp` and the `β* == 0` special case from the
decoder, and α is invariant across magnitude boundaries for fixed-decimal data (β\* flips
from 4 to 5 when a 2-decimal value crosses 100), so it also halves header churn there.

> **Correction (Task 3, recorded in Task 13):** the `k ≤ 15` claim above is wrong. At large
> magnitudes a double's shortest round-trip decimal can be shorter than its exact `k`-digit
> decimal, so ELF finds `α < k` and erases real bits: `17582400172190.2` (`k = 8`) erases, and
> random sweeps found erasing values for every `k` from 8 to 24. The sound bound is the 1/16
> grid: for `k ≤ 4`, `α ≥ 1 ⇒ f(α) ≥ 4 ≥ k ⇒ erase_bits ≤ tz`, and `α = 0` cannot recover a
> non-integer. The shipped classifier (`is_short_dyadic`) uses `k ≤ 4`. See
> [the Chimp evaluation](../benchmarks/chimp-evaluation.md).

**SIMD.** The crate uses no explicit SIMD; x86_64 builds get `+avx2,+bmi1,+bmi2` from
`.cargo/config.toml`, aarch64 has baseline NEON (2 f64 lanes, no 64-bit lzcnt or
multiply-high). Chimp's serial dependency (each field's position depends on earlier widths;
each XOR on the previous encoded value) caps what vectors can do. Two opportunities make the
plan: a word-at-a-time skip over repeat runs (Task 6a — a flat sample is four zero bits)
and moving `recover` into a vector pass over a decoded block (Task 7a, an experiment with
an explicit keep/revert gate; a batch-decode buffer lost on Gorilla in September). A
vectorized classification pre-pass for batch encodes was judged low priority (decimal-zero
stripping needs 64-bit division by constants, which neither NEON nor AVX2 has; single-sample
`TS.ADD` would not benefit). SIMD-shaped decode needs a block layout (the ALP/Pco work on
the `fastalp` branch), not Chimp. Separate timestamp and value streams within a chunk would
let range reads skip values and decode regular timestamps 64 bits at a time; that is a
larger format change, to be evaluated separately.

---

## Phase A — byte-neutral latency and correctness

### Task 1: Preserve the probe evidence

**Files:**
- Create: `docs/benchmarks/data/chimp-review/notes.md`, `…/production-latency.csv`,
  `…/production-latency.md`, `…/timestamp-model.csv`, `…/extended-r{1,2,3}.csv`,
  `…/probe-r{1,2,3}.csv`, `…/edge-checks.txt`
  (copied from `target/bench-reports/chimp-review/`)

- [ ] **Step 1: Copy the directory**

Run: `mkdir -p docs/benchmarks/data/chimp-review && cp target/bench-reports/chimp-review/* docs/benchmarks/data/chimp-review/`

- [ ] **Step 2: Prepend a provenance line to the copied `notes.md`**

One sentence: probes ran against commit `7d5a3d058` on 2026-10-29 from standalone sources
that were in `/tmp/valkey-chimp-review` (not retained); CSV columns are as the notes'
*Method and limitations* section describes.

- [ ] **Step 3: Commit**

```bash
git add docs/benchmarks/data/chimp-review
git commit -m "docs(chimp): preserve 2026-10-29 encoder probe notes and data"
```

### Task 2: Reject corrupt trailing-window headers; pin the β\* width invariant

**Files:**
- Modify: `src/series/chunks/chimp/encoder.rs:284-296` (`ChimpDec::next_value`, arm `1`)
- Modify: `src/series/chunks/chimp/compressor.rs:396-404` (case `11` write)
- Test: `src/series/chunks/chimp/encoder.rs` (new `#[cfg(test)] mod tests`)

**Interfaces:**
- Produces: `ChimpDec::next_value` returns `Err(io::ErrorKind::InvalidData)` for a `01`
  header whose rounded leading count plus significant width exceeds 64.

- [ ] **Step 1: Write the failing test**

```rust
#[test]
fn corrupt_trailing_window_header_is_rejected() {
    // ELF-free Chimp stream: first value 64 raw bits, then flag 01, lz code 7 (=24),
    // sig 0 (=64): 24 + 64 > 64 must be an error, not a shift by 232.
    let mut w = BitStream::new();
    w.write_bits(64, 1.5f64.to_bits()).unwrap();
    w.write_bits(11, (0b01 << 9) | (7 << 6)).unwrap();
    w.write_bits(64, 0).unwrap();
    let mut r = BitStreamReader::new(w.bytes());
    let mut dec = ChimpDec::new();
    dec.read_value(&mut r).unwrap();
    let err = dec.read_value(&mut r).unwrap_err();
    assert_eq!(err.kind(), io::ErrorKind::InvalidData);
}
```

- [ ] **Step 2: Run it; expect a panic (debug) or a wrong value, not `InvalidData`**

Run: `cargo test --features enable-system-alloc corrupt_trailing_window_header_is_rejected`

- [ ] **Step 3: Validate before the subtraction**

In arm `1`, after computing `sig` and the new `stored_lz`, return
`Err(io::Error::new(InvalidData, "corrupt chimp chunk: leading + width exceed 64"))` when
`sig + self.stored_lz > 64`; mirror the Gorilla reader's message style
(`varbit_xor.rs` "corrupt gorilla chunk: …"). Keep the error constructor in a `#[cold]` fn
so the hot arm stays small.

- [ ] **Step 4: Add the encoder invariant**

In `compressor.rs` case `11`: `debug_assert!(beta_star < 16, "beta_star must fit the 4-bit header")`
with a comment that `erase_bits > 4` is what guarantees it.

- [ ] **Step 5: Run the Chimp tests and the compression check**

Run: `cargo test --features enable-system-alloc series::chunks::chimp` → all pass.
Run: `tools/compression_report.sh --check` → no regression lines.

- [ ] **Step 6: Commit**

```bash
git commit -am "fix(chimp): reject trailing-window headers wider than 64 bits"
```

### Task 3: Skip ELF for short dyadic values

**Files:**
- Modify: `src/series/chunks/chimp/elf64.rs` (new fn + tests)
- Modify: `src/series/chunks/chimp/compressor.rs:342-348` (`write_value` raw branch)
- Test: `src/series/chunks/chimp/elf64.rs` tests, `src/series/chunks/chimp/compressor.rs` tests

**Interfaces:**
- Produces: `pub(crate) fn is_short_dyadic(bits: u64) -> bool` in `elf64.rs`: true when
  `bits` is finite, normal, nonzero and `52 − tz(mantissa) − (e − 1023) ≤ 15`, where `tz` is
  `min(trailing_zeros(mantissa), 52)` (an all-zero mantissa counts as 52). Returns false for
  zero, subnormal, infinite and NaN patterns, which keep their existing paths.
- Produces (test-only): `pub(crate) fn elf_would_erase(v: f64, last_beta_star: i32) -> bool`
  in `elf64.rs` under `#[cfg(test)]`: runs today's `get_alpha_and_beta_star` + mask +
  `delta != 0 && erase_bits > 4` + recovery check, i.e. the exact predicate `write_value`
  uses before choosing case `0`/`11`.

- [ ] **Step 1: Write the byte-neutrality proof test**

```rust
#[test]
fn short_dyadic_values_never_erase() {
    for v in boundary_corpus() {           // see Step 2
        for last in [i32::MAX, 0, 1, 2, 5, 15] {
            if is_short_dyadic(v.to_bits()) {
                assert!(!elf_would_erase(v, last), "{v:e} (last {last}) would erase");
            }
        }
    }
}

#[test]
fn classifier_accepts_integers_and_binary_grids() {
    for v in [1.0, -7.0, 123456789.0, 0.5, 0.25, 0.0625, 12.5, 1e15 + 0.5] {
        assert!(is_short_dyadic(v.to_bits()), "{v}");
    }
    for v in [0.1, 100.23, 2f64.powi(-20), 1e-300, f64::MIN_POSITIVE, 0.0, f64::NAN] {
        assert!(!is_short_dyadic(v.to_bits()), "{v:e}");
    }
}
```

- [ ] **Step 2: Add `boundary_corpus()` as a `#[cfg(test)] pub(crate)` helper in `elf64.rs`**

Reproduce the probe's 3,150-value set: `0.0, -0.0, ±inf, NaN 0x7ff8000000000001,
from_bits(1), MIN_POSITIVE, ±MAX`; for `base in [0, 1, 1e6, 1e12, 1e14, 1e15, 1e18]` and
`i in -64..=64`: `base + i/16` and `-base + i/16`; for `exponent in [0,1,10,512,1000,1023,1030,1074,1075,2046]`,
`mantissa in [0,1,15,16,0x000f_ffff_ffff_ffff]`, both signs: `from_bits(sign | exponent<<52 | mantissa)`;
`±10^p` for `p in -308..=308`; then `1.0`. Later tasks reuse it.

- [ ] **Step 3: Run; the proof test fails to compile (`is_short_dyadic` missing)**

- [ ] **Step 4: Implement `is_short_dyadic` and call it**

In `write_value`, extend the existing raw condition at `compressor.rs:342` to
`v == 0.0 || !v.is_finite() || is_short_dyadic(v_long)`; comment that such values have no
erasable bits (proof in the fn doc). Mark the fn `#[inline(always)]` only if the profile
in Step 6 shows it out of line.

- [ ] **Step 5: Run the proof test, the Chimp tests, proptest and the compression check**

Run: `cargo test --features enable-system-alloc series::chunks::chimp` and
`cargo test --features enable-system-alloc chunk_encoding_serialize_roundtrip` → pass.
Run: `tools/compression_report.sh --check` → identical (dyadic values were always raw).

- [ ] **Step 6: Measure, and compare against the `v*16` alternative**

Run `tools/latency_report.sh --encodings chimp --workloads all` three times; record
counter/discrete (expect ≈0.3×) and drift/noisy/periodic (expect ≤ +2 %). Then swap the
classifier body for `v.trunc() == v || (v * 16.0).trunc() == v * 16.0`, re-run, keep the
faster one, and leave the other as a one-line comment with its numbers.

- [ ] **Step 7: Commit**

```bash
git commit -am "perf(chimp): skip the ELF search for integers and short binary fractions"
```

### Task 4: A/B harness — `--encoding` for `gorilla_report`

**Files:**
- Modify: `tools/gorilla_report.rs:16,37-40,100,114` (use `TimeSeriesChunk::new(encoding, max)`
  and `TimeSeriesChunk::deserialize` in place of `GorillaChunk`)
- Modify: `docs/benchmarks/gorilla-evaluation.md` *Reproduction* (one line: flag exists,
  default unchanged)

**Interfaces:**
- Produces: `--encoding gorilla|chimp` (default `gorilla`); CSV columns unchanged. The tool
  must keep using only APIs present at `106088e57` so it can be dropped into a baseline
  worktree as `examples/gorilla_report.rs`, as the Gorilla report describes.

- [ ] **Step 1: Add the flag and parse it with `ChunkEncoding::from_str`/the same parser
  `latency_report.rs:76` uses; reject anything else with the existing usage error**
- [ ] **Step 2: Build and run both encodings for 1,000 samples**

Run: `cargo run --release --features enable-system-alloc,test-utils --bin gorilla_report -- --encoding chimp --samples 1000 --iterations 50 --out /tmp/chimp.csv`
Expected: 45 data rows; `bytes` for `drift_q2,regular,0` equals the `latency_report` value (1491).

- [ ] **Step 3: Commit**

```bash
git commit -am "feat(tools): gorilla_report --encoding for Chimp A/B runs"
```

### Task 5: Top-down branchless digit count

**Files:**
- Modify: `src/series/chunks/chimp/elf64.rs:177-234` (`get_significant_count`, `get_alpha_and_beta_star`)
- Modify: `src/series/chunks/chimp/compressor.rs:350` (call site)
- Test: `src/series/chunks/chimp/elf64.rs` tests (update the three existing
  `get_alpha_and_beta_star` tests to the new signature)

**Interfaces:**
- Produces: `pub fn get_alpha_and_beta_star(v: f64) -> Result<(i32, i32), ()>` — the
  `last_beta_star` parameter is removed (the result no longer depends on history).
- Produces: `fn strip_decimal_zeros(n: u64) -> (u64, u32)`: `(n / 10^z, z)` for the largest
  `z` with `10^z | n`; `(0, 0)` for `n == 0`. Implemented as four cmov-style steps
  (10⁸, 10⁴, 10², 10), no loop.
- Produces: `fn significant_count(v: f64, sp: i32) -> Result<i32, ()>`: `i = 16 − sp`;
  `Err` if `i < 0` or `i >= MAP_10IP.len()`; `n = (v * MAP_10IP[i]) as u64` (always
  `< 10^17`); `β = 17 − z` where `z` is from `strip_decimal_zeros(n)`. No division, no
  guard loop. The `temp / 10^i != v` exactness test is dropped: `try_recover` in
  `write_value` is the exactness check.
- Keeps (test-only): the current implementation as `#[cfg(test)] mod reference` so the
  equivalence test below can compare.
- Produces (test-only): `fn recovers(v: f64, alpha: i32, beta_star: i32) -> bool` in the
  `elf64.rs` tests: builds `v'` with the Task 3 mask arithmetic for `alpha` and returns
  `recover(v'.to_bits(), beta_star) == Ok(v)` bit-exactly (today's `recover` from
  `compressor.rs`, made `pub(crate)`).
- Updates: Task 3's `elf_would_erase(v, last_beta_star)` becomes `elf_would_erase(v)`;
  drop the `last` loop in `short_dyadic_values_never_erase`.

- [ ] **Step 1: Write the tests**

```rust
#[test]
fn strip_decimal_zeros_exact() {
    assert_eq!(strip_decimal_zeros(0), (0, 0));
    assert_eq!(strip_decimal_zeros(7), (7, 0));
    assert_eq!(strip_decimal_zeros(1_230_000), (123, 4));
    assert_eq!(strip_decimal_zeros(10_000_000_000_000_000), (1, 16));
    assert_eq!(strip_decimal_zeros(12_345_000_000_000_000), (12_345, 12));
}

#[test]
fn top_down_count_never_erases_less_than_the_reference() {
    // On every value the old search could erase, the new one finds the same or fewer
    // digits, and the erased value still recovers.
    for v in boundary_corpus().into_iter().chain(matrix_values()) {
        for last in [i32::MAX, 0, 3, 5, 15] {
            if let Ok((_, beta_old)) = reference::get_alpha_and_beta_star(v, last) && beta_old <= 15 {
                let (alpha, beta_new) = get_alpha_and_beta_star(v).unwrap();
                assert!(beta_new <= beta_old, "{v:e}: new {beta_new} > old {beta_old}");
                assert!(recovers(v, alpha, beta_new), "{v:e} does not recover");
            }
        }
    }
}
```

`matrix_values()` is a `#[cfg(test)]` helper yielding the values of all 12 workloads × 3
timestamp models at 1,000 samples via `generate_dataset` (`crate::tests::generators`), plus
the counter workload offset by 2⁴⁰ and 2⁵⁰. Update `small_magnitudes_still_erase`,
`tiny_powers_of_ten_erase_when_the_scale_is_known` and `overflowing_searches_fail_fast` to
the one-argument signature (the last one keeps its 500 ms budget).

- [ ] **Step 2: Run; expect compile failures on the missing fns**
- [ ] **Step 3: Implement `strip_decimal_zeros`, `significant_count`, the new
  `get_alpha_and_beta_star`; move the old code under `#[cfg(test)] mod reference`**
- [ ] **Step 4: Run the ELF and Chimp tests, proptest, and the compression check**

Run: `cargo test --features enable-system-alloc series::chunks::chimp` → pass.
Run: `tools/compression_report.sh --check` → no regression line. Any row that *changes*
(even smaller) is reported in the commit body with the before/after ratio; a change larger
than 0.5 % on any row is investigated before committing.

- [ ] **Step 5: Measure**

Three `latency_report` runs, all workloads, Chimp + Gorilla. Expect full-precision rows
19–21 µs → ≤ 14 µs and no q2 row slower. Record in the commit body.

- [ ] **Step 6: Commit**

```bash
git commit -am "perf(chimp): top-down decimal digit count without the division check"
```

### Task 6: Decimal scale without `log10`

**Files:**
- Modify: `src/series/chunks/chimp/elf64.rs:115-171` (`get_sp`, `get_sp_and_10in_flag`)
- Test: `src/series/chunks/chimp/elf64.rs` tests

**Interfaces:**
- Keeps: `pub fn get_sp(v: f64) -> i32` semantics (floor of log₁₀|v|) for every finite
  positive `v`, with the table scan unchanged for `[1e-10, 1e10)` (it measured faster than
  exponent arithmetic there) and the `log10()` fallback replaced by an exponent estimate
  `((e − 1023) as f64 * LOG10_2).floor()` corrected by one compare against `MAP_10IP`/`MAP_10IN`.

- [ ] **Step 1: Write the test**

```rust
#[test]
fn get_sp_matches_floor_log10_everywhere() {
    for p in -323i32..=308 {
        let v = 10f64.powi(p);
        for x in [v, f64::from_bits(v.to_bits() - 1), f64::from_bits(v.to_bits() + 1), v * 3.0] {
            if x.is_finite() && x > 0.0 {
                assert_eq!(get_sp(x), x.log10().floor() as i32, "{x:e}");
            }
        }
    }
}
```

- [ ] **Step 2: Run; passes today (pins behaviour), then implement the fallback and re-run**
- [ ] **Step 3: Compression check identical; `latency_report --workloads counter` with the
  2⁴⁰/2⁵⁰ offsets via `gorilla_report --encoding chimp` shows no regression**
- [ ] **Step 4: Commit**

```bash
git commit -am "perf(chimp): exponent-derived decimal scale outside the table range"
```

### Task 6a: Skip repeat runs a word at a time in the decoder

A repeated value at an unchanged interval is exactly four zero bits at a sample boundary:
timestamp `0` (dod 0), ELF case `0`, Chimp flag `00`. Every Phase B task keeps that
spelling (Task 11 keeps dod 0 at 1 bit; Tasks 7, 9 and 10 keep case `0` + flag `00`), so
this survives the grammar changes. Decoder-only; bytes unchanged.

**Files:**
- Modify: `src/series/chunks/stream/bitstream_reader.rs` (new method + test)
- Modify: `src/series/chunks/chimp/compressor.rs:476-589` (`ChimpDecompressor`:
  `pending_repeats` field, `next_sample`)
- Test: `bitstream_reader.rs` tests, `compressor.rs` tests

**Interfaces:**
- Produces: `BitStreamReader::leading_zero_bits(&mut self) -> u32` — refills, then returns
  the number of consecutive zero bits at the read position, counting only bits actually in
  the window (never the zero padding `peek_upto` adds past the end); `0` when the window is
  empty. Consumes nothing. Shift `buffer << (64 − valid)` before `leading_zeros`, cap at
  `valid`, and return early for `valid == 0` (avoids a shift by 64).
- Produces: `ChimpDecompressor.pending_repeats: u64`. `next_sample`, when it is non-zero,
  decrements it and returns `(last_timestamp + last_delta, last_value)` (updating
  `last_timestamp` and `count`) without touching the reader.
- Trigger: only after `next_sample` has decoded a sample whose dod was 0 *and* whose value
  was a repeat. Then `run = min(leading_zero_bits() / 4, sample_count − count)`; skip
  `4 · run` bits and set `pending_repeats = run`. Gating on a just-decoded repeat means
  non-flat data never pays for the check. A window holds at most 16 records, so the next
  repeat after a drained run re-checks naturally; no inner loop is needed.

- [ ] **Step 1: Write the reader test**

```rust
#[test]
fn leading_zero_bits_counts_only_real_bits() {
    let data = [0x00u8, 0x00, 0x0F, 0xFF];
    let mut r = BitStreamReader::new(&data);
    assert_eq!(r.leading_zero_bits(), 20);
    r.skip(20).unwrap();
    assert_eq!(r.leading_zero_bits(), 0);
    let zeros = [0u8; 3];                       // 24 real bits, then padding
    let mut r = BitStreamReader::new(&zeros);
    assert_eq!(r.leading_zero_bits(), 24);
    assert_eq!(BitStreamReader::new(&[]).leading_zero_bits(), 0);
}
```

- [ ] **Step 2: Write the decoder tests**

```rust
#[test]
fn repeat_runs_decode_identically_at_every_length_and_offset() {
    // Runs of every length 1..=40 (crossing the 16-record window and refill
    // boundaries), each ended by a timestamp change, a value change, or the stream end.
    for run in 1..=40usize {
        for ender in 0..3 {
            let mut s = vec![Sample::new(1000, 2.5), Sample::new(1010, 3.75)];
            for i in 0..run { s.push(Sample::new(1020 + i as i64 * 10, 3.75)); }
            let next_ts = 1020 + run as i64 * 10;
            match ender {
                0 => s.push(Sample::new(next_ts + 3, 3.75)),   // dod != 0
                1 => s.push(Sample::new(next_ts, 4.0)),        // value change
                _ => {}                                        // stream ends in the run
            }
            round_trip(&s);
        }
    }
}

#[test]
fn run_skip_stops_at_the_sample_count() {
    // A flat stream whose last byte is mostly zero padding: decoding must yield exactly
    // `count` samples and then `None`.
    let mut c = ChimpCompressor::new();
    for i in 0..37 { c.add_sample(1000 + i * 10, 8.75).unwrap(); }
    let mut d = ChimpDecompressor::new(c.bytes(), c.count());
    let mut n = 0;
    while d.next_sample().unwrap().is_some() { n += 1; }
    assert_eq!(n, 37);
}
```

`repeats_interleaved_with_distinct_values_round_trip`, `a_long_flat_stretch_round_trips`
and the `test_range_iter`/`test_get_range` chunk tests cover range-filtered reads over runs.

- [ ] **Step 3: Run; compile failure on `leading_zero_bits`**
- [ ] **Step 4: Implement the reader method and the decoder fields/trigger; run all Chimp
  and stream tests, proptest; compression check identical**
- [ ] **Step 5: Measure**

Three `latency_report --encodings chimp --workloads all --ts-models regular,jitter` runs.
Expect `constant`/`constant_int` regular decode ~6–7 ns/sample → ≤ 2; `discrete`
improves a little. Gate: no non-flat row's `decode_iter` or `scan_10%` more than 2 % worse
than the Task 6 tip. If a row regresses, take a `sample` profile before touching
`#[inline]` attributes (the September note: a batch-decode buffer lost on Gorilla for
exactly this kind of reason).

- [ ] **Step 6: Commit**

```bash
git commit -am "perf(chimp): skip runs of repeated samples a word at a time"
```

---

## Phase B — in-place grammar changes

Every task here changes bytes. Each one ends with: Chimp unit tests, proptest, the
`boundary_corpus()` round-trip (`special_values`-style, added once in Task 7), `gorilla_report
--encoding chimp` three rounds against the Phase A tip, and a note of the per-row byte
deltas in the commit body. `tools/compression_report.sh --check` is expected to report
Chimp rows as *improved*; a reported regression blocks the commit.

### Task 7: α in the 4-bit header; `get_sp`-free recovery

**Files:**
- Modify: `src/series/chunks/chimp/compressor.rs` (`ChimpCompressor.last_beta_star` →
  `last_alpha: i32`, `write_value`, `ChimpDecompressor.last_beta_star` → `last_alpha`,
  `read_value`, `recover`, `try_recover`, `rdb_save/load`, `serialize/deserialize`,
  `debug_digest`, `Hash`, `PartialEq`, module docs lines 1-30)
- Modify: `src/series/chunks/chimp/elf64.rs` (`round_up` → used by `recover` only;
  `get_10in`, `get_sp_and_10in_flag` flag output become unused → delete)
- Test: `compressor.rs` tests, `chimp_chunk_tests.rs`

**Interfaces:**
- Produces: header case `11` + 4 bits = α ∈ 0..=15 (decimal places erased). Case `0` =
  "same α as last" (or repeat, as today). Case `10` = raw. The encoder stores raw whenever
  `alpha < 0 || alpha > 15`.
- Produces: `fn recover(v_prime_bits: u64, alpha: i32) -> io::Result<f64>` =
  `copysign(ceil(|v'| · MAP_10IP[alpha]) / MAP_10IP[alpha], v')`; `Err(InvalidData)` for
  `alpha` outside 0..=15. No `get_sp`, no power-of-ten special case.
- Produces: the unset sentinel `NO_ALPHA: i32 = -1` for `last_alpha` (persisted where
  `last_beta_star` was, same varint slot; decoder starts at `NO_ALPHA`).
- Consumes: `get_alpha_and_beta_star(v)` from Task 5 (only the α half is used; the β\*
  half and the `flag` can be deleted in this task).

- [ ] **Step 1: Write the tests**

```rust
#[test]
fn negative_quantized_values_recover_bit_exactly() {
    round_trip(&[Sample::new(1, -100.23), Sample::new(2, -0.01), Sample::new(3, -99.9),
                 Sample::new(4, -1e-5), Sample::new(5, 0.01), Sample::new(6, -0.01)]);
}

#[test]
fn boundary_corpus_round_trips() {
    let samples: Vec<Sample> = boundary_corpus().into_iter().enumerate()
        .map(|(i, v)| Sample::new(i as Timestamp * 10, v)).collect();
    round_trip(&samples);
}

#[test]
fn recover_rejects_out_of_range_alpha() {
    assert!(recover(1.5f64.to_bits(), -1).is_err());
    assert!(recover(1.5f64.to_bits(), 16).is_err());
}

#[test]
fn fixed_decimal_data_keeps_alpha_across_a_magnitude_boundary() {
    // 99.98, 99.99, 100.00, 100.01 … all have alpha 2: one `11` header, then case `0`.
    let mut c = ChimpCompressor::new();
    for i in 0..40 { c.add_sample(1000 + i * 10, 99.80 + (i as f64) * 0.01).unwrap(); }
    assert_eq!(c.stats.new_alpha, 1);
}
```

`stats` is a `#[cfg(test)] pub(crate) stats: ElfStats { repeats, raw, same_alpha, new_alpha,
window_reuse: u32 }` field on `ChimpCompressor`, incremented in `write_value`/`write_repeat`;
it is excluded from `Hash`/`PartialEq`. Task 9 uses `window_reuse`.

- [ ] **Step 2: Run; compile failures on `recover(_, alpha)`, `stats`**
- [ ] **Step 3: Implement**

`write_value`: after the raw conditions, `let (alpha, _) = get_alpha_and_beta_star(v)?`;
raw if outside 0..=15; mask from `get_f_alpha(alpha) + e − 1023` as today; keep the
near-miss-repeat guard (`v_prime_long == stored_val` → raw) verbatim; case `0` when
`alpha == self.last_alpha`, else case `11` + `alpha` and set `last_alpha`.
`read_value`: case `11` reads 4 bits into `last_alpha`; both erased cases call
`recover(bits, self.last_alpha)`. Update the test helpers `elf_would_erase(v)` and
`recovers(v, alpha, _)` to the α form (`recovers(v, alpha)`), and the module doc's layout
comment; `docs/topics/encodings.md` follows in Task 12.

- [ ] **Step 4: Run all Chimp tests, proptest, `every_repeat_costs_four_bits`,
  `near_miss_values_do_not_masquerade_as_repeats`, `test_append_after_serialize_roundtrip_mixed_cases`**
- [ ] **Step 5: Measure decode**

`gorilla_report --encoding chimp`, three rounds vs the Phase A binary. Expect decode on
`_q2` rows −10…−20 %, bytes within ±1 % except fewer `11` headers on magnitude-crossing
rows. If `tiny powers of ten` rows (1e-300 in `special_values`) grow, that is expected and
noted (they are now raw).

- [ ] **Step 6: Commit**

```bash
git commit -am "feat(chimp)!: carry decimal places in the ELF header; scale-free recovery"
```

### Task 7a (experiment): Two-pass decode with block recovery

After Task 7, `recover` is a pure per-element function (multiply, ceil, divide, copysign)
that cannot fail: the 4-bit field is always 0..=15. Taking it out of the serial bit loop
puts the divide off the loop-carried path and lets it run 2 (NEON) or 4 (AVX2) lanes wide.
Keep this task **only if it meets the gate in Step 5**; otherwise revert and record the
numbers under *Rejected alternatives* in Task 13. Bytes unchanged.

**Files:**
- Modify: `src/series/chunks/chimp/compressor.rs` (`ChimpDecompressor`: block buffer,
  `fill_block`, `recover_block`; `next_sample` serves from the buffer)
- Test: `compressor.rs` tests

**Interfaces:**
- Produces: `const DECODE_BLOCK: usize = 32;` (Step 5 also tries 16 and 64).
- Produces: buffer fields `ts: [i64; DECODE_BLOCK]`, `bits: [u64; DECODE_BLOCK]`,
  `alpha: [u8; DECODE_BLOCK]` (`ALPHA_NONE = u8::MAX` marks raw/repeat lanes, whose `bits`
  already hold the final value), `head: usize`, `len: usize`.
- Produces: `fn fill_block(&mut self) -> io::Result<()>` — the serial pass: decodes up to
  `min(DECODE_BLOCK, sample_count − count)` records into the buffer (timestamps final,
  values as `v'` bits + α), including Task 6a's repeat runs. A decode error is returned on
  the `next_sample` call that reaches the failing record, not earlier: buffer the
  successfully decoded prefix and stash the error.
- Produces: `fn recover_block(bits: &mut [u64], alpha: &[u8])` — free function, the
  vector pass. Computes the recovered value for every lane with α clamped to 0 for
  `ALPHA_NONE` lanes, then selects the original bits for those lanes, so it is branch-free.
  Must equal Task 7's `recover` bit-for-bit on every erased lane.
- Implementation rule: portable scalar Rust that LLVM auto-vectorizes. `std::simd` is
  nightly-only (MSRV is stable 1.96). Check the release asm for `fdiv.2d`/`frintp.2d` on
  aarch64 and `vdivpd`/`vroundpd` on x86_64 (`cargo rustc --release --features
  enable-system-alloc -- --emit asm`, or `cargo asm` if installed). Write
  `core::arch` intrinsics behind `#[cfg(target_arch)]` only if auto-vectorization fails, and
  keep the scalar loop as the fallback for other targets.

- [ ] **Step 1: Write the tests**

```rust
#[test]
fn block_recovery_matches_scalar_recovery() {
    let values: Vec<f64> = boundary_corpus().into_iter().chain(matrix_values()).collect();
    for chunk in values.chunks(DECODE_BLOCK) {
        let (mut bits, mut alpha) = (Vec::new(), Vec::new());
        for &v in chunk {
            match erase_for_test(v) {               // (v' bits, alpha) or None if raw
                Some((vp, a)) => { bits.push(vp); alpha.push(a as u8); }
                None => { bits.push(v.to_bits()); alpha.push(ALPHA_NONE); }
            }
        }
        let expected: Vec<u64> = bits.iter().zip(&alpha).map(|(&b, &a)|
            if a == ALPHA_NONE { b } else { recover(b, a as i32).unwrap().to_bits() }).collect();
        recover_block(&mut bits, &alpha);
        assert_eq!(bits, expected);
    }
}

#[test]
fn decode_error_surfaces_at_the_failing_sample() {
    // Truncate a 100-sample stream inside record 70: the first 69 samples decode,
    // then the next call errors.
    let mut c = ChimpCompressor::new();
    for i in 0..100 { c.add_sample(1000 + i * 10, 100.0 + i as f64 * 0.01).unwrap(); }
    let mut d0 = ChimpDecompressor::new(c.bytes(), 70);
    while d0.next_sample().unwrap().is_some() {}
    let cut_bits = d0.bit_position_for_test();       // #[cfg(test)] accessor
    let bytes = &c.bytes()[..cut_bits / 8];
    let mut d = ChimpDecompressor::new(bytes, 100);
    let mut ok = 0;
    loop {
        match d.next_sample() {
            Ok(Some(_)) => ok += 1,
            Ok(None) => panic!("ran out without an error"),
            Err(_) => break,
        }
    }
    assert!(ok >= 68 && ok <= 69, "{ok}");
}
```

`erase_for_test(v) -> Option<(u64, i32)>` reuses the Task 7 encoder arithmetic
(`get_alpha_and_beta_star` + mask, `None` when the encoder would store raw);
`bit_position_for_test` wraps `BitStreamReader::bit_position` (already `test-utils`-gated;
widen the gate to `any(test, feature = "test-utils")`).

- [ ] **Step 2: Run; compile failures on the new items**
- [ ] **Step 3: Implement the buffer, `fill_block`, `recover_block`, and serving in
  `next_sample`; check the asm as described above**
- [ ] **Step 4: Run all Chimp tests, proptest, `boundary_corpus_round_trips`, Task 6a's run
  tests, and the chunk range tests**
- [ ] **Step 5: Measure and decide**

`gorilla_report --encoding chimp`, three interleaved rounds against the Task 7 binary, for
`DECODE_BLOCK` 16, 32 and 64. **Gate:** keep the best block size only if `drift_q2`,
`noisy_q2`, `periodic_q2` and `bursty_q2` full decode improve by ≥ 10 % (geomean), and no
row's decode, full range, mid or tail scan regresses by more than 3 %. Partial reads now
decode up to one block past the window's end; that is what the tail and mid columns check.
If the gate fails, `git checkout` the files and record the four numbers per block size for
the report.

- [ ] **Step 6: Commit (only if kept)**

```bash
git commit -am "perf(chimp): decode in blocks and recover erased values vector-wide"
```

### Task 8: Verify the previous α before searching

**Files:**
- Modify: `src/series/chunks/chimp/compressor.rs` (`write_value`)
- Test: `compressor.rs` tests

**Interfaces:**
- Produces: in `write_value`, when `last_alpha != NO_ALPHA`: compute the mask for
  `last_alpha`; if `erase_bits > 4`, `delta != 0`, `v' != stored_val` and
  `recover(v', last_alpha) == v` bit-exactly, emit case `0` and return — no digit search.
  Otherwise fall through to the Task 7 path.
- Produces: `fn erase_mask(alpha: i32, exponent: i32) -> Option<(u64 /*mask*/, i32 /*erase_bits*/)>`
  shared by the hint path and the search path (`None` when `erase_bits <= 4`).

- [ ] **Step 1: Write the tests**

```rust
#[test]
fn hint_reuses_alpha_for_values_with_fewer_digits() {
    // 100.2 (one place) under last_alpha 2 must be case `0`, not a new header.
    let mut c = ChimpCompressor::new();
    c.add_sample(1000, 100.23).unwrap();
    c.add_sample(2000, 100.2).unwrap();
    c.add_sample(3000, 100.25).unwrap();
    assert_eq!(c.stats.new_alpha, 1);
    assert_eq!(c.stats.same_alpha, 2);
}

#[test]
fn mixed_precision_stream_round_trips() {
    let mut samples = Vec::new();
    let mut ts = 0;
    for block in 0..6 {
        for i in 0..200 {
            ts += 1000;
            let v = if block % 2 == 0 { 100.0 + (i as f64) * 0.01 } else { 100.0 + (i as f64) * 1e-9 };
            samples.push(Sample::new(ts, v));
        }
    }
    round_trip(&samples);
}
```

- [ ] **Step 2: Run; `hint_reuses…` fails (today it records 2 new headers)**
- [ ] **Step 3: Implement the hint path and `erase_mask`; the search path uses the same helper**
- [ ] **Step 4: Run Chimp tests + proptest; compression check (expect `_q2` rows smaller)**
- [ ] **Step 5: Measure the mixed case**

Add a `mixed_precision` row to the Task 13 report: encode the `mixed_precision_stream`
samples 300× with the Phase A binary and this one. If the full-precision blocks are more
than 5 % slower than Phase A, add an encoder-local `raw_streak: u8` (not persisted, not
part of the format) that skips the hint after 8 consecutive raw values and re-arms on the
next erasure; re-measure.

- [ ] **Step 6: Commit**

```bash
git commit -am "perf(chimp): try the previous decimal-place count before searching"
```

### Task 9: Full XOR-window reuse

**Files:**
- Modify: `src/series/chunks/chimp/encoder.rs` (`ChimpEnc` gains `stored_tz: i32`;
  `ChimpEncState.stored_tz`; `compress_value`; `add_repeat_prefixed` keeps the window;
  `ChimpDec::next_value` arm `2` reads `64 − stored_lz − stored_tz` and shifts; arm `3`
  sets `stored_tz = 0`)
- Modify: `src/series/chunks/chimp/compressor.rs:224-323` (persist `stored_tz` in
  `rdb_save/load`, `serialize/deserialize`, `debug_digest`; it joins `Hash`/`PartialEq`
  through `ChimpEnc`'s derives)
- Test: `encoder.rs` tests, `compressor.rs` tests, `chimp_chunk_tests.rs`

**Interfaces:**
- Produces, encoder decision per nonzero XOR with `nlz`, `lz = round(nlz)`, `tz`:
  `fresh = if tz > THRESHOLD { 11 + 64 − lz − tz } else { 5 + 64 − lz }`;
  if `stored_lz <= 63 && nlz >= stored_lz && tz >= stored_tz`:
  `reuse = 2 + (64 − stored_lz − stored_tz)`; emit `10` when `reuse <= fresh` (tie keeps
  state). Else emit `01` (sets `stored_tz = tz`) or `11` (sets `stored_tz = 0`), both set
  `stored_lz = lz`. A repeat (`xor == 0`) leaves `stored_lz`/`stored_tz` unchanged
  (today's `stored_lz = 65` reset is removed on both sides — the decoder already keeps them).
- Produces: `ChimpEncState { stored_lz, stored_tz, stored_val, first }`; persisted order:
  after `stored_lz`, before `stored_val`, in both `rdb_save` and `serialize`.

- [ ] **Step 1: Write the tests**

```rust
#[test]
fn append_after_reload_reuses_persisted_window() {
    // Two 2-place values open a trailing window (flag 01); serialize mid-window, reload,
    // append a third that fits, decode everything bit-exactly, and see one reuse.
    let mut c = ChimpCompressor::new();
    c.add_sample(1000, 100.23).unwrap();
    c.add_sample(2000, 100.31).unwrap();
    let mut buf = Vec::new();
    c.serialize(&mut buf);
    let mut r = ChimpCompressor::deserialize(&buf).unwrap();
    assert_eq!(r, c);
    r.add_sample(3000, 100.27).unwrap();
    assert_eq!(r.stats.window_reuse, 1);
    let got = ChimpDecompressor::new(r.bytes(), r.count()).collect().unwrap();
    assert_eq!(got.iter().map(|s| s.value.to_bits()).collect::<Vec<_>>(),
               [100.23f64, 100.31, 100.27].map(f64::to_bits));
}

#[test]
fn quantized_drift_reuses_windows_and_shrinks() {
    let key = DatasetKey::new(ValueWorkload::DriftQuantized, TimestampModel::Regular);
    let samples = generate_dataset(key, 1000, dataset_seed(key));
    let mut c = ChimpCompressor::new();
    for s in &samples { c.add(*s).unwrap(); }
    assert!(c.stats.window_reuse > 500, "{}", c.stats.window_reuse);
    assert!(c.bytes().len() < 1_200, "{} bytes", c.bytes().len()); // probe: 997 B
}
```

Add a `repeats_keep_the_window` case to `encoder.rs` tests: value A (opens `01`), repeat,
value B fitting the window → the third write is flag `10`.

- [ ] **Step 2: Run; compile failures (`stored_tz`, `window_reuse`)**
- [ ] **Step 3: Implement encoder, decoder, persistence; update `rdb_load`/`deserialize`
  corrupt-state tests (`test_deserialize_rejects_corrupt_codec_state`) for the new field**
- [ ] **Step 4: Evaluate the erase threshold**

With reuse in place, run the compression report twice: `erase_bits > 4` (today) and
`erase_bits > THRESHOLD` (6) in `erase_mask`. Keep the smaller total over the 28 Chimp rows;
record both totals in the commit body.

- [ ] **Step 5: Run Chimp tests, proptest, boundary corpus; compression check (expect
  `_q2`/counter/discrete rows −15…−33 %, full-precision rows within ±0.5 %)**
- [ ] **Step 6: Measure encode and decode, three rounds**

The Gorilla greedy-refresh variant cost 40 % decode on quantized noise. If `noisy_q2`
decode regresses more than 5 %, add Gorilla's hysteresis (`keep_fast_window` for
32–57-bit windows, `varbit_xor.rs:44-50`) and re-measure; otherwise do not add it.

- [ ] **Step 7: Commit**

```bash
git commit -am "feat(chimp)!: reuse the full XOR window on flag 10"
```

### Task 10: Leading-zero code range

**Files:**
- Modify: `src/series/chunks/chimp/encoder.rs:12-29` (tables), header widths in
  `compress_value`/`emit`/`next_value` (peek width)
- Test: `encoder.rs` tests, `compressor.rs` tests

**Interfaces:**
- Produces: `const LZ_CODE_BITS: u32` (3 or 4) and the three tables regenerated from one
  `const LZ_BUCKETS: [i32; 1 << LZ_CODE_BITS]`; `01` header = `2 + LZ_CODE_BITS + 6`,
  `11` header = `2 + LZ_CODE_BITS`; decoder peek = `2 + LZ_CODE_BITS + 6` (≤ 12 bits).

- [ ] **Step 1: Histogram**

Add an `#[ignore]` test `print_leading_zero_histogram` that encodes the 45-case matrix
(`matrix_values()` from Task 5 as samples) and prints, per case, the histogram of `nlz` for
nonzero XORs *after* the Task 9 encoder has decided the header. Run it once:
`cargo test --features enable-system-alloc print_leading_zero_histogram -- --ignored --nocapture`.

- [ ] **Step 2: Choose the table**

Candidates: current `{0,8,12,16,18,20,22,24}`; 3-bit re-spread `{0,8,12,16,20,24,32,44}`;
4-bit `{0,4,8,…,60}`. Score each by total Chimp bytes over the 45 cases (compression report
+ shifted counters from `gorilla_report --encoding chimp`). Pick the smallest total subject
to: no 36-base-case row more than 0.5 % larger than with the current table. Record the three
totals in the commit body.

- [ ] **Step 3: Write the pin test**

```rust
#[test]
fn shifted_counter_payload_is_bounded() {
    // Counter + 2^50: XOR deltas are tiny with ~61 leading zeros. Probe baseline 5,636 B.
    let key = DatasetKey::new(ValueWorkload::Counter, TimestampModel::Regular);
    let mut samples = generate_dataset(key, 1000, dataset_seed(key));
    for s in &mut samples { s.value += (1u64 << 50) as f64; }
    let mut c = ChimpCompressor::new();
    for s in &samples { c.add(*s).unwrap(); }
    assert!(c.bytes().len() < 2_500, "{} bytes", c.bytes().len());
}
```

- [ ] **Step 4: Implement the chosen table and widths; run Chimp tests, proptest, boundary
  corpus, compression check; measure three rounds**
- [ ] **Step 5: Commit**

```bash
git commit -am "feat(chimp)!: widen the leading-zero code"
```

### Task 11: Timestamp tiers and single-write escapes

**Files:**
- Modify: `src/series/chunks/chimp/compressor.rs:55-56,422-469,515-555` (`TS_ESCAPE_PREFIX`,
  `write_timestamp`, `read_timestamp`)
- Test: `compressor.rs` tests

**Interfaces:**
- Produces, delta-of-delta `d` zig-zagged to `n` bits: `0` → 1 bit; `n ≤ 7` → `10`+7 (9);
  `n ≤ 9` → `110`+9 (12); `n ≤ 12` → `1110`+12 (16); `n ≤ 16` → `11110`+16 (21);
  `n ≤ 32` → `111110`+32 (38); else `111111`+64 (70, two writes). Every tier but the last
  is one `write_bits`.
- Produces: `read_timestamp` peeks 6 bits and dispatches on the prefix.

- [ ] **Step 1: Write the tests**

```rust
#[test]
fn timestamp_tiers_cost_their_documented_bits() {
    // (delta-of-delta, bits) at each bucket's upper edge and one past it.
    for (dod, bits) in [(0i64, 1), (63, 9), (-64, 9), (64, 12), (255, 12), (256, 16), (2047, 16),
                        (2048, 21), (32767, 21), (32768, 38), (i32::MAX as i64, 38),
                        (i32::MAX as i64 + 1, 70)] {
        let mut c = ChimpCompressor::new();
        c.add_sample(0, 1.0).unwrap(); c.add_sample(1000, 1.0).unwrap();
        let before = c.len_bits();
        c.add_sample(2000 + dod, 1.0).unwrap();          // value repeat costs 3 bits
        assert_eq!(c.len_bits() - before - 3, bits as u64, "dod {dod}");
    }
}

#[test]
fn truncated_timestamp_escape_reports_eof() {
    let mut c = ChimpCompressor::new();
    c.add_sample(0, 1.0).unwrap(); c.add_sample(1000, 1.0).unwrap();
    c.add_sample(2000 + (1i64 << 40), 1.0).unwrap();
    let bytes = c.bytes();
    let cut = &bytes[..bytes.len() - 4];
    assert!(ChimpDecompressor::new(cut, 3).collect().is_err());
}
```

`irregular_and_out_of_order_timestamps` stays as the semantic round-trip.

- [ ] **Step 2: Run; the cost test fails on the 21/38/70 rows**
- [ ] **Step 3: Implement both sides; delete `TS_ESCAPE_PREFIX`**
- [ ] **Step 4: Chimp tests, proptest, boundary corpus; compression check (irregular rows
  −3…−9 %, others identical); measure three rounds (expect timing within noise)**
- [ ] **Step 5: Commit**

```bash
git commit -am "feat(chimp)!: 16-bit timestamp tier and single-write escapes"
```

---

## Phase C — version, baselines, docs, report

### Task 12: Encoding version 3, fixtures, baseline, documentation, integration tests

**Files:**
- Modify: `src/series/series_data_type.rs:36,39` (both constants → `3`, comment: version 3
  changes the Chimp grammar; nothing older loads)
- Modify: `tests/test_ts_restore_malformed.py:32` (`CURRENT_ENCVER = 3`; note it was left at
  `1` by the version-2 change and the assertion at `:96` depends on it)
- Modify: `tests/compat/test_compat_persistence.py` (text: "encver 3 (reads 3)")
- Modify: `tests/test_dump_restore.py` (new test, below)
- Modify: `COMPATIBILITY.md` (the version-2 paragraph becomes version 3)
- Create: `src/series/chunks/chimp/fixtures/README.md`, `v3-drift_q2.txt`, `v3-counter.txt`,
  `v3-noisy.txt` (same format as `src/series/chunks/gorilla/fixtures/`: hex of
  `ChimpCompressor::serialize`, then `timestamp value_bits_hex` lines; 32 samples, jitter
  model, `dataset_seed`)
- Modify: `src/series/chunks/chimp/chimp_chunk_tests.rs` (fixture test: load, append 8
  samples, serialize, decode all bit-exactly)
- Modify: `benches/baselines/compression_baseline.csv` via `tools/compression_report.sh --save-baseline`
- Modify: `docs/topics/encodings.md:158-250` (Chimp section: layer-1 header is α; flag
  table at `:194-201` gains the reuse semantics and the new header widths; repeat note at
  `:209`; bit-cost claims at `:203-205`), and the rubric rows/ratios at `:272-304` and the
  timestamp table at `:306-320` from the new baseline
- Modify: `docs/benchmarks/gorilla-evaluation.md` persistence paragraph (version now 3)

- [ ] **Step 1: Write the integration test**

```python
@pytest.mark.parametrize('values', ['quantized', 'counter_2p50'])
def test_chimp_dump_restore_and_append(self, values):
    """α headers, reused windows and new timestamp tiers survive DUMP/RESTORE and appends."""
    client = self.client
    client.execute_command('TS.CREATE', 'src', 'ENCODING', 'CHIMP', 'CHUNK_SIZE', 512,
                           'DUPLICATE_POLICY', 'LAST')
    def val(i):
        return round(100 + i * 0.01, 2) if values == 'quantized' else (1 << 50) + 7 * i
    with client.pipeline(transaction=False) as p:
        for i in range(1000):
            p.execute_command('TS.ADD', 'src', 1000 * (i + 1) + (i % 3) * 7, val(i))  # jittered
        p.execute()
    payload = client.execute_command('DUMP', 'src')
    client.execute_command('RESTORE', 'dst', 0, payload)
    client.execute_command('TS.ADD', 'dst', 1_001_000, val(1000))
    client.execute_command('TS.ADD', 'dst', 2000 + 7, val(1) + 1)      # rewrites a sealed chunk
    actual = client.execute_command('TS.RANGE', 'dst', '-', '+')
    assert len(actual) == 1001
    for i, (ts, v) in enumerate(actual):
        expected = val(i) + (1 if i == 1 else 0)
        assert float(v) == float(expected), (i, v)
```

Extend `test_restore_rejects_unsupported_encoding_version` so `2` is in the rejected set.

- [ ] **Step 2: Bump the constants; run the unit suite**

Run: `cargo test --features enable-system-alloc` and `cargo test --doc --features enable-system-alloc`.

- [ ] **Step 3: Generate fixtures with an `#[ignore]` test `write_chimp_fixtures`, commit the
  three files and the README, add the loading test; run it**
- [ ] **Step 4: Refresh the baseline**

Run: `tools/compression_report.sh --save-baseline`. Verify with `git diff --stat` that only
`chimp` rows changed (28 rows) and paste the Chimp before/after B/sample table into the
commit body.

- [ ] **Step 5: Update the docs listed above; `rg -n "beta_star|β\*" docs src --glob '!**/generated/**'`
  returns only historical mentions in `docs/benchmarks/`**
- [ ] **Step 6: Build, lint, integration**

Run: `cargo fmt --check && cargo clippy --profile release --all-targets -- -D clippy::all && RUSTFLAGS="-D warnings" cargo build --all --all-targets --release`.
Run: `TEST_PATTERN="encoding or dump_restore or restore_malformed or save_and_restore" SERVER_VERSION=unstable ./build.sh`.
Run: `RTS_COMPAT=1 SERVER_VERSION=unstable ./build.sh` → same expected-divergence count as
before this plan; any new divergence is investigated (none is expected: command replies do
not expose the grammar).

- [ ] **Step 7: Commit**

```bash
git commit -am "feat(series)!: module encoding version 3 for the Chimp grammar; refresh baselines and docs"
```

### Task 13: Evaluation report

**Files:**
- Create: `docs/benchmarks/chimp-evaluation.md` (structure of `gorilla-evaluation.md`:
  Decision, Changes retained, Measurements, Rejected alternatives, Validation, Reproduction)
- Create: `docs/benchmarks/data/chimp-1000.csv`, `chimp-10000.csv`, `chimp-environment.txt`
- Modify: `docs/topics/encodings.md:6-9` (pointer paragraph, as for Gorilla)

- [ ] **Step 1: Build the baseline binary**

Worktree at `7d5a3d058`, copy `tools/gorilla_report.rs` to `examples/gorilla_report.rs`
there, build with `--release --features enable-system-alloc,test-utils --jobs 1`; copy out
as `/tmp/chimp-baseline`. Build the branch tip as `/tmp/chimp-candidate`.

- [ ] **Step 2: Run three alternating rounds at 1,000 (1,000 iterations) and 10,000 (300
  iterations) samples with `--encoding chimp`, under `caffeinate -i -s -m`; nothing else
  compiling. Medians of per-round medians into the CSVs.**
- [ ] **Step 3: Write the report**

Required content: the geomean table over the 36 base cases (bytes, bulk, append, decode,
range, mid, tail) for 1,000 and 10,000 samples; the per-shape table for counter, discrete,
drift_q2, noisy_q2, drift, noisy, the shifted counters and the `mixed_precision` row from
Task 8; the largest regression on any case, named; the rejected alternatives with their
numbers (`v*16` classifier if it lost, erase threshold choice, lz tables not chosen,
hysteresis if not added, Task 7a block decode if it failed its gate, with the per-block-size
numbers); the validation list (unit count, clippy, release build,
compression check, integration and compat counts); the reproduction commands. Negative
numbers mean smaller/faster; state that these are chunk microbenchmarks.

- [ ] **Step 4: Commit**

```bash
git add docs/benchmarks/chimp-evaluation.md docs/benchmarks/data/chimp-*
git commit -m "docs(chimp): evaluation report for the encoder changes"
```

### Task 14 (optional, decision point 2): fan-out wire version guard

**Files:**
- Modify: `proto/v1/common.proto` (the chunk message holding `bytes data = 3`, ~line 131:
  add `uint32 encoding_version = N` with the next free tag)
- Regenerate: `VALKEY_TS_PROTO_REGEN=1 cargo build`; commit `proto/v1/generated/`
- Modify: `src/series/chunks/serialization.rs` (`samples_to_chunk[_lossless]` callers set
  the field to `TIMESERIES_TYPE_ENCODING_VERSION`); the fan-out response decoder rejects a
  chunk whose version differs, with an error naming both versions (own `ErrorKind` so the
  text survives the coordinator — see the fan-out error-text note in `src/fanout/`)
- Test: a cluster integration test in the relevant `*_cme.py` that patches one shard's
  response version via `TS._DEBUG` is out of reach; instead a Rust unit test in
  `src/fanout/` decodes a hand-built response with `encoding_version = 2` and asserts the
  error text contains "encoding version 2" and "3".

- [ ] **Step 1: Write the unit test; run; fails**
- [ ] **Step 2: Add the field, regenerate, set and check it**
- [ ] **Step 3: `cargo test --features enable-system-alloc fanout`; `./build.sh` with
  `TEST_PATTERN=cme` subset; commit `feat(fanout): reject chunks from a different encoding version`**

---

## Self-review notes

- Coverage: every evidence item maps to a task — dyadic bypass (3), hint (8), digit count
  (5), scale (6), repeat-run skip (6a), α header (7), block recovery experiment (7a), window reuse incl. persistence and the tz-threshold
  observation (9), leading-zero cap (10), timestamp tier + fused escape (11), decoder
  validation (2), repeat floor and `keep_lz` deliberately not done (Background), wire
  versioning (12 + optional 14), evidence preservation (1), report (13).
- Names used across tasks: `boundary_corpus()` and `matrix_values()` (Task 3/5 helpers,
  reused by 7, 9, 10); `stats` with fields `repeats, raw, same_alpha, new_alpha,
  window_reuse` (Task 7, used by 8, 9); `erase_mask` (Task 8, used by 9); `NO_ALPHA`
  (Task 7); `get_alpha_and_beta_star(v)` one-argument (Task 5, consumed by 7);
  `leading_zero_bits` and `pending_repeats` (Task 6a, used by 7a's `fill_block`);
  `recover(bits, alpha)` (Task 7, the oracle for 7a's `recover_block`).
- Not planned, with reasons: a 1–2-bit repeat (+1 bit on every non-repeat sample), a
  Chimp128-style reference window (≥ 64 B persisted per chunk), reciprocal-multiply recovery
  (erasure-rate loss; the division is off the loop-carried path), fused timestamp peek in
  the decoder (Gorilla probes found it mixed; revisit only with a measured gap).
