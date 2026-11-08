# Chimp encoder evaluation

2026-10-29. Branch: `perf/chimp-encoder`.
Baseline: `7d5a3d058` (branch start).
Candidate: `f0c7429e6`.

The runs used the same changes on a development branch, at `f6e446a35` and `114f171c0`, which are
not in this repository. The commits named here have identical codec files, report tools and data
generators, so they reproduce the same measurements.

## Decision

Retain every change listed below. Over the 36 unshifted cases, Chimp encodes about
**32 % faster**, stores about **12 % fewer bytes** and decodes **12–14 % faster** than at
the branch start. No case grows in bytes. The largest timing regression is +8.0 %, on one
1,000-sample cell (see [Measurements](#measurements)).

The plan's goals were measured against Gorilla with the same tool on the current branch
(`--encoding gorilla`; the Gorilla sources did not change on this branch):

| Goal | Before (branch start) | Now | Met? |
|---|---:|---:|---|
| Full-precision encode ≈ 1.3× Gorilla, 1,000 samples | 2.02× | **1.27×** | Yes (rows 1.06–1.58×) |
| Full-precision encode ≈ 1.3× Gorilla, 10,000 samples | 2.61× | **1.61×** | **No** (rows 1.21–2.23×) |
| Integer data encode ≤ 1× Gorilla (counter) | 2.30× / 2.07× | **0.99× / 0.97×** | Yes |
| Dyadic data encode ≤ 1× Gorilla (discrete {0, ¼, ½, 1}) | 2.22× / 2.41× | **1.03× / 1.17×** | **No** (close at 1,000) |
| Close the counter size deficit vs Gorilla | 1.26× / 1.25× | **1.04× / 1.03×** | Mostly (regular counter still +8 % / +7 %) |
| Close the discrete size deficit vs Gorilla | 1.42× / 1.43× | **1.19× / 1.19×** | **No** (halved) |
| Keep the quantized-data advantage (`_q2` bytes vs Gorilla) | 0.49× / 0.48× | **0.38× / 0.37×** | Yes, widened |

Pairs are 1,000 / 10,000 samples. Each ratio is the geometric mean of the per-case
Chimp/Gorilla ratios over the three timestamp models. "Full precision" means drift, noisy,
periodic and bursty. Counters shifted by 2^30/2^40/2^50 went from 2.08× to **1.07×**
Gorilla's bytes at 1,000 samples, and from 1.91× to **1.03×** at 10,000.

The 10,000-sample gap comes from per-sample cost growth. Chimp's per-sample encode cost
rises with chunk length, and Gorilla's does not. Over the full-precision rows (geomean),
Chimp takes 14.0 ns per sample at 1,000 samples and 17.5 ns at 10,000. Gorilla takes 11.0
and 10.8 ns. The branch start had the same pattern: 22.4 and 28.5 ns. Why Chimp's cost
grows with length was not investigated here. It is the next lever for the encode goal.

The plan did not target constant series, and their regular-timestamp rows are unchanged.
They are still about 2× Gorilla's bytes (516 vs 266 B per 1,000 regular samples), because
a Chimp repeat costs 4 bits; with jittered or irregular timestamps the gap is 6–10 %.

**Persistence.** The bit grammar changed in place, and the module encoding version was
**not** bumped (it stays at 2, reading 1–2), because nothing has been released. Chimp
chunks carry no grammar marker, so RDB, DUMP and AOF files containing Chimp series
written by builds from before this change will not decode correctly, and no load-time
error protects them. Delete local `dump.rdb`/AOF files written by those builds, or
re-ingest their logical samples. (An earlier revision of this branch bumped the version
to 3 and refused older payloads; that was reverted.)

**Cluster fan-out.** The optional wire-version guard for fan-out chunks (plan Task 14) was
**skipped by ruling**: the module is unreleased and a cluster runs one build. A cluster
that mixes this build with an older one would misdecode Chimp fan-out chunks during a
rolling upgrade. Adding the guard is an independent follow-up.

## Changes retained

- **Decoder validation:** a `01` header whose leading count plus width exceeds 64 is
  rejected as corrupt instead of producing a negative shift.
- **ELF bypass:** integers and values on a 1/16 grid skip the decimal search and are
  stored raw. These values can never be erased (see Rejected alternatives for why the
  bound is 1/16 and not 1/2^15). The classifier is
  `v.trunc() == v || (v·16).trunc() == v·16`, for normal doubles.
- **Digit count:** a top-down count to 15 significant digits replaces the climb/strip
  search. It uses constant-divisor zero stripping and an exactness check only where `10^i`
  is exact (`i ≤ 22`). Outside the table range, the decimal scale comes from the binary
  exponent rather than `log10`.
- **α in the header:** case `11` now carries the number of erased decimal places
  (α, 4 bits) instead of β\*. Recovery is `copysign(ceil(|v'|·10^α)/10^α, v')`, with no
  decimal-scale lookup in the decoder. α stays fixed when a fixed-decimal value crosses a
  power of ten.
- **Previous-α hint:** the encoder first tries the last α (case `0`). It skips the hint
  after a raw value and forces a full count every 16th sample. Both conditions derive from
  persisted state, so a reloaded encoder writes the same bytes as an uninterrupted one.
- **Full XOR-window reuse:** flag `10` reuses the stored leading *and* trailing counts.
  The encoder chooses between it and a fresh header by bit cost, using selects instead of
  branches. A repeat keeps the window. The erase threshold stays at `erase_bits > 4`, and
  `01`/`11` stay split at `tz > 6`.
- **Extended leading counts:** the leading-zero code stays 3 bits, with buckets 0, 8, 12,
  16, 18, 20, 22 and 24. Under bucket 24, a `01` width of 41–63 cannot be valid. Those
  widths now encode an exact leading count of 32–54 and are followed by a second 6-bit
  width, for a 17-bit header. The encoder rounds the count down to 32 + 3k, so the window
  keeps headroom.
- **Timestamp tiers:** a 16-bit tier (`11110` + 16 bits = 21 bits) is added. Every tier
  except the 64-bit escape is written in one call. The 32- and 64-bit escapes cost 38 and 70 bits (previously 37 and
  69). Delta-of-delta zero stays 1 bit, so a repeated sample still costs 4 bits.
- Golden fixtures for the current grammar (no encoding version) and a refreshed compression baseline.

## Measurements

These are local **chunk microbenchmarks**, not server throughput or command p99. The tool
(`tools/gorilla_report.rs`) times the concrete `ChimpChunk`/`GorillaChunk` directly, not
through `TimeSeriesChunk`. Its numbers are not comparable with `latency_report` or the
plan's background table.

Setup:
- Both binaries were built in release with LTO, `enable-system-alloc,test-utils`,
  `--locked` and the same `Cargo.lock`.
- The baseline was built in a worktree at `7d5a3d058`, with the candidate's tool copied in
  as `examples/gorilla_report.rs`.
- Environment: macOS 15.3.1, Apple M2, Rust 1.96.0.
- A browser and system daemons were running (load average about 4). No builds or tests ran
  during the timed rounds.

Cases. Each run covers 12 workloads × 3 timestamp models, plus the counter shifted by
2^30, 2^40 and 2^50 under each model, plus a `mixed_precision` row. That row alternates
200-sample blocks of 2- and 9-place decimals: 1,200 samples at `--samples 1000`, and 10,000
at `--samples 10000`. Every row validates exact round trips before timing, including after
a serialize/reload/append.

Timing protocol:
- Three rounds, with the order of five arms rotated each round:
  - baseline (`--encoding chimp`);
  - a byte-identical copy of the baseline binary (the control);
  - candidate (`--encoding chimp`);
  - Gorilla from the baseline binary and from the candidate binary.
- Each operation runs 50 warmups, then 1,000 timed iterations at 1,000 samples or 300 at
  10,000 samples.
- CSV values are medians of the three per-round medians.

The table is the geometric mean of paired candidate/baseline ratios over the **36
unshifted cases**. Negative values mean fewer bytes or less time.

| Samples | Payload bytes | Bulk time | Append time | Decode time | Full-range time | Middle scan | Tail scan |
|---|---:|---:|---:|---:|---:|---:|---:|
| 1000 | -12.0% | -31.9% | -32.4% | -11.8% | -6.9% | -12.9% | -12.9% |
| 10000 | -11.6% | -32.1% | -32.4% | -14.0% | -10.7% | -16.3% | -14.4% |
| 1000, control | +0.0% | +0.0% | +0.1% | -0.7% | +1.6% | +0.7% | +0.2% |
| 10000, control | +0.0% | +0.1% | -0.1% | -0.1% | -0.0% | +0.1% | -0.1% |

Over the 36 cases, total payload fell from 163,974 to 149,713 B at 1,000 samples, and from
1,623,867 to 1,487,037 B at 10,000. The nine shifted counters fell by 48.6 % / 45.9 % in
bytes and by 49.5 % / 48.8 % in bulk encode time (geomeans).

Selected cases, regular timestamps (1,200 samples for `mixed_precision` at 1,000):

| Samples | Case | Payload bytes, baseline → candidate | Bytes | Bulk time | Decode time | Middle scan |
|---|---|---:|---:|---:|---:|---:|
| 1,000 | counter | 2,378 → 1,895 | -20.3% | -63.0% | -16.5% | -24.5% |
| 1,000 | discrete | 2,207 → 1,787 | -19.0% | -57.5% | -17.7% | -16.2% |
| 1,000 | drift_q2 | 1,491 → 960 | -35.6% | -23.3% | -12.4% | -20.0% |
| 1,000 | noisy_q2 | 3,669 → 2,759 | -24.8% | -29.2% | -27.8% | -30.7% |
| 1,000 | drift | 6,006 → 5,973 | -0.5% | -39.1% | -20.4% | -14.3% |
| 1,000 | noisy | 7,411 → 7,407 | -0.1% | -44.6% | -1.4% | +0.9% |
| 1,000 | counter + 2^30 | 3,899 → 1,964 | -49.6% | -57.9% | -29.7% | -31.6% |
| 1,000 | counter + 2^40 | 5,148 → 2,143 | -58.4% | -52.1% | -14.6% | -14.6% |
| 1,000 | counter + 2^50 | 5,636 → 2,160 | -61.7% | -51.1% | -8.0% | -7.8% |
| 1,000 | mixed_precision | 4,033 → 1,700 | -57.8% | -22.5% | -25.5% | -29.9% |
| 10,000 | counter | 23,537 → 18,598 | -21.0% | -59.6% | -12.6% | -18.1% |
| 10,000 | discrete | 21,541 → 17,203 | -20.1% | -55.2% | -19.8% | -21.1% |
| 10,000 | drift_q2 | 14,654 → 10,644 | -27.4% | -18.5% | -15.2% | -18.0% |
| 10,000 | noisy_q2 | 36,656 → 27,432 | -25.2% | -38.6% | -41.7% | -42.4% |
| 10,000 | drift | 59,894 → 59,605 | -0.5% | -36.9% | -20.4% | -19.9% |
| 10,000 | noisy | 74,074 → 74,023 | -0.1% | -35.8% | +1.1% | +2.5% |
| 10,000 | counter + 2^30 | 38,903 → 19,652 | -49.5% | -58.2% | -27.3% | -30.8% |
| 10,000 | counter + 2^40 | 51,402 → 22,759 | -55.7% | -52.2% | -17.7% | -17.4% |
| 10,000 | counter + 2^50 | 56,261 → 27,180 | -51.7% | -52.3% | -9.4% | -9.0% |
| 10,000 | mixed_precision | 33,555 → 14,089 | -58.0% | -23.4% | -26.5% | -31.7% |

The new timestamp tier shows on irregular timestamps. For example, constant/irregular
shrinks from 2,558 to 2,320 B (−9.3 %) at 1,000 samples, and every irregular
full-precision row shrinks by about 3 %. Regular and jitter rows of full-precision data are
within 0.5 % of the baseline in bytes.

Chimp against Gorilla. The table gives geometric means of per-case Chimp/Gorilla ratios.
"Before" pairs both encodings in the baseline binary, and "after" pairs both in the
candidate binary.

| Samples | Group | Bytes before → after | Bulk before → after | Decode before → after |
|---|---|---:|---:|---:|
| 1,000 | full precision (12) | 0.98 → 0.97 | 2.02 → 1.27 | 0.95 → 0.87 |
| 1,000 | `_q2` (12) | 0.49 → 0.38 | 1.42 → 1.11 | 0.94 → 0.74 |
| 1,000 | counter (3) | 1.26 → 1.04 | 2.30 → 0.99 | 0.91 → 0.80 |
| 1,000 | discrete (3) | 1.42 → 1.19 | 2.22 → 1.03 | 0.96 → 0.79 |
| 1,000 | constant, constant_int (6) | 1.36 → 1.32 | 0.98 → 0.94 | 0.88 → 0.89 |
| 1,000 | shifted counters (9) | 2.08 → 1.07 | 2.06 → 1.05 | 0.92 → 0.75 |
| 1,000 | all 36 unshifted | 0.87 → 0.76 | 1.62 → 1.11 | 0.93 → 0.81 |
| 10,000 | full precision (12) | 0.96 → 0.95 | 2.61 → 1.61 | 1.06 → 0.95 |
| 10,000 | `_q2` (12) | 0.48 → 0.37 | 1.62 → 1.25 | 1.08 → 0.81 |
| 10,000 | counter (3) | 1.25 → 1.03 | 2.07 → 0.97 | 0.80 → 0.74 |
| 10,000 | discrete (3) | 1.43 → 1.19 | 2.41 → 1.17 | 0.95 → 0.78 |
| 10,000 | constant, constant_int (6) | 1.37 → 1.33 | 0.99 → 0.95 | 0.77 → 0.78 |
| 10,000 | shifted counters (9) | 1.91 → 1.03 | 1.99 → 1.03 | 0.82 → 0.71 |
| 10,000 | all 36 unshifted | 0.85 → 0.76 | 1.84 → 1.27 | 0.98 → 0.84 |

The slowest full-precision rows relative to Gorilla are noisy/regular (1.34× at 1,000
samples, 2.23× at 10,000) and bursty/regular (1.58×, 1.76×). Discrete's 10,000-sample
1.17× comes mostly from its regular and irregular rows (1.20× and 1.28×); the jitter row is
1.04×.

**Largest regressions.** No case grows in bytes at either sample count.
- **1,000 samples:** the largest regression on any case is **constant/regular decode,
  +8.0 %** (5,208 → 5,625 ns; the control moved 0.0 % on that cell). The same row's bulk
  encode is +6.4 % (control +2.7 %) and append +5.5 %. Every value in that series is a
  repeat, and at 10,000 samples the same row is within +0.7 % on every column. The next
  largest is periodic/jitter decode at +2.4 %.
- **10,000 samples:** the largest is **noisy/irregular decode, +3.0 %** (140,792 →
  144,958 ns; control −0.2 %). Next are noisy/irregular tail scan +2.7 % and noisy/regular
  middle scan +2.5 %.

**Noise floor.** The control (baseline against a byte-identical copy) gives:
- 1,000 samples: geomeans within −0.7…+1.6 % on every column. Single cells reach +11.1 %
  (bursty_q2/irregular and drift_q2/irregular full-range) and −10.3 %
  (drift_q2/irregular decode).
- 10,000 samples: geomeans within ±0.1 %, and the worst single cell is +6.2 %
  (periodic/irregular middle scan).
- Unchanged Gorilla code timed from the two binaries, a cross-binary layout check, differs
  by −2.0…+1.0 % in geomean. Its worst cells are +14.8 % (bursty_q2/jitter decode) at
  1,000 samples and +8.0 % (discrete/regular tail) at 10,000.

Read single-row changes below about 10 % at 1,000 samples, or about 6 % at 10,000, as
indistinguishable from noise on this machine.

## Rejected alternatives

**Fraction-bit limit 15 for the ELF bypass.** The plan assumed that a double with
`k ≤ 15` fractional mantissa bits has a shortest decimal of exactly `k` digits, and so can
never erase. That is false at large magnitudes. The shortest round-trip decimal can be
shorter, ELF then finds `α < k`, and real bits are erased. For example, 17582400172190.2
(`k = 8`) erases.
- Random sweeps found erasing values for every `k` from 8 to 24 and none for `k ≤ 7`.
- With the 15-bit limit wired in, a 2,017-sample golden stream changed from 13,641 to
  13,588 bytes: the limit is not byte-neutral.
- The sound bound is the 1/16 grid. For `k ≤ 4`, `α ≥ 1` gives `f(α) ≥ 4 ≥ k`, so
  `erase_bits ≤ tz`; and `α = 0` cannot recover a non-integer.

**Bypass classifier form.** The kept form selects exactly the bit test's values (a test
pins this). Encode cost on the eight non-dyadic workloads (latency tool, 1,000 samples,
regular timestamps, median of 3 runs, not interleaved):

| Classifier form | Encode vs baseline |
|---|---:|
| Bit test `52 − tz − E ≤ 4` | +5.4 % |
| Exponent-range mask | +4.8 % |
| `(v·16).trunc()` alone | +4.2 % |
| `v·16` with an integer short-circuit (kept) | +2.9 % |

The later digit-count work more than recovered this cost (see the bulk column above).

**Word-at-a-time repeat-run skip in the decoder.** Rejected at its gate, which required
flat decode ≤ 2 ns/sample and no non-flat row worse than 2 %.
- Best variant: flat regular decode fell from 5.21 to 3.58 ns/sample (−31 %), and the
  middle scan on flat data by 52 %.
- Non-flat rows regressed beyond the control: drift_q2/jitter decode +12.3 %,
  discrete/irregular +10.4 %, constant/jitter +6.4 %, noisy/jitter +5.0 %.
- Inlining the pending path reached 2.50 ns/sample, but cost constant/jitter +19.9 % and
  periodic_q2/irregular +13.3 %.
- The brief's form and five variants were tried. The remaining cost appeared to come from
  the per-sample iterator call chain and code layout rather than the run check itself.

**Two-pass block decode with vectorized recovery.** Rejected at its gate, which required
`_q2` decode at −10 % or better.
- Recovery did vectorize (`frintp.2d`/`fdiv.2d` on aarch64), but every block size made
  decode slower. `_q2` decode geomean, 1,000 / 10,000 samples:

  | Block | 1,000 samples | 10,000 samples |
  |---|---:|---:|
  | 16 | +17.9 % | +23.0 % |
  | 32 | +18.5 % | +21.1 % |
  | 64 | +15.8 % | +13.3 % |

- At N = 64 the worst non-`_q2` cells were +48.5 % and +51.1 %. The 10,000-sample column
  is a single round of 100 iterations.
- The bit-parsing pass alone costs 97 % of the old per-sample decode. The divide was never
  on the critical path. Lane stores, serving from the buffer, recovering raw lanes and
  over-decoding partial reads were pure overhead.

**Previous-α hint without a re-count.** A hint that is never re-checked sticks at the
larger α. `mixed_precision` (1,200 samples) grew from 4,033 to 5,065 bytes (+25.6 %), with
only 2 `11` headers in the stream. Re-counting every N-th sample gave:

| Re-count every | mixed_precision bytes |
|---|---:|
| 8 | 4,008 |
| 16 | 4,046 |
| 32 | 4,083 |
| 64 | 4,169 |
| 16, plus skipping the hint after a raw value (kept) | 3,957 |

Trying the hint on every value cost full-precision encode +8.5…+19 % at 1,000 samples
(+17 % at 10,000), because 99 % of attempts miss there. A non-persisted raw-streak counter
was not used: a reloaded encoder would then write different bytes.

**Branchy window choice.** The first full-window encoder chose `10`/`01`/`11` with
branches. On quantized data the choice is close to random, so it cost `_q2` encode
+14…+25 % and discrete/regular +54 % (1,000-sample encode geomean 1.066 against the
previous commit). The same choice written as selects produced identical bytes, at 0.866×
the branchy encode time.

**Erase threshold `> 6` instead of `> 4`.** The result was a wash.
- With `> 4` (kept): 89,751 samples stored in the compression report's fixed-capacity
  chunks, and 464,963 bytes for the same samples.
- With `> 6`: 89,748 samples stored, and 464,982 bytes (+0.004 %).
- `gorilla_report` totals differed by 4–5 bytes.

**Window hysteresis.** Not added. Unlike Gorilla, reusing the full window *reduces* Chimp
decode work: fewer bits, and one width and one shift on `10`. When the window change
landed, noisy_q2 decode measured 0.88–0.96× of the commit before it.

**Wider leading-zero tables.** Scoring used the 45-case byte total at 1,000 samples, with
a cap that no unshifted row may grow by more than 0.5 %:

| Candidate | Bytes | Worst row |
|---|---:|---|
| 3-bit respread {0,8,12,16,20,24,32,44} | −8.24 % | bursty_q2/regular +5.14 % |
| 4-bit {0,4,…,60} | −8.92 % | bursty_q2/regular +6.14 % |
| Finer 4-bit {0,8,10,…,28,32,40,48,56} | −9.06 % | drift_q2/regular +34.7 % |
| Escape, exact counts | −9.51 % | none grows |
| Escape, 32 + 3k (kept) | −9.26 % | none grows |

- The 4-bit tables add a header bit to every fresh window. That tips the cost comparison
  towards keeping stale wide windows.
- Exact counts in the encoder made windows brittle. counter + 2^30 reopened its window on
  251 of 999 values and decoded 28 % slower, so the encoder rounds to 32 + 3k.
- An out-of-line decoder escape cost 5–9 % decode on series that never take it, so the
  escape is inline.

**Fan-out wire-version guard** (plan Task 14): skipped by ruling; see Decision.

## Validation

These results are from the branch-final validation run at `f0c7429e6`.
- 1,776 unit tests passed, 0 failed, 2 ignored. 12 doctests passed, 5 ignored.
- `cargo fmt --check` and Clippy (`--profile release --all-targets -D clippy::all`)
  passed, as did the production release build with `RUSTFLAGS="-D warnings"`.
- Compression:
  - The byte-neutral steps left `tools/compression_report.sh --check` identical on all 84
    rows. The exception was the new digit count, which changed seven Chimp rows by under
    1 % (noisy_q2/4k improved 0.78 %).
  - The format steps changed only Chimp rows.
  - The final baseline refresh changed 25 Chimp rows; every Gorilla and uncompressed row
    is byte-identical.
  - Chimp's compression geomean is 3.59 (was 3.35), against Gorilla's 3.16. The only Chimp
    row that grew is noisy/regular/1k, by 0.1 %.
- Three golden fixtures (drift_q2, counter, noisy) re-encode byte-exactly. They also
  reload and take an append (a repeat, an extended window, a NaN, large timestamp gaps),
  and the result matches an uninterrupted encode. Byte-level unit tests cover every
  timestamp tier.
- Genuine DUMP payloads relabelled as versions 0 and 3 (with a valid CRC) are refused with
  `Bad data format`; no key is created and the server stays up. `TS._RESTORE` rejects
  versions 0, 3, 99, −1 and `x`. The version gate is unchanged from before the branch
  (versions 1–2 load). The module version was later reverted to 1, so the gate now
  loads only version 1.
- Full integration suite: **1,573 passed, 0 failed** (run before the version gate was reverted to 2).
- Compatibility suite: **2,854 passed, 0 failed, 2 skipped, 13 expected divergences** (same caveat).
  These are the previously registered ids; none is new. The 2 skips are the opt-in fuzzer.
- Every case in this evaluation's A/B validated exact round trips, including after
  reload and append, in all five arms.

## Reproduction and data

Build the candidate and the baseline:

```sh
cargo build --release --locked --features enable-system-alloc,test-utils \
  --bin gorilla_report --jobs 1
cp target/release/gorilla_report /tmp/chimp-candidate

git worktree add --detach /tmp/chimp-base 7d5a3d058
mkdir -p /tmp/chimp-base/examples
cp tools/gorilla_report.rs /tmp/chimp-base/examples/gorilla_report.rs
cp Cargo.lock /tmp/chimp-base/
(cd /tmp/chimp-base && CARGO_TARGET_DIR="$PWD/../chimp-base-target" \
  cargo build --release --locked --features enable-system-alloc,test-utils \
  --example gorilla_report --jobs 1)
cp /tmp/chimp-base-target/release/examples/gorilla_report /tmp/chimp-baseline
cp /tmp/chimp-baseline /tmp/chimp-baseline2
git worktree remove /tmp/chimp-base
```

The tool uses only APIs present at the baseline. `Cargo.lock` is untracked, so copy it into
the worktree. Then run each arm three times per sample count, rotating the order, with
nothing else compiling:

```sh
/tmp/chimp-baseline  --encoding chimp   --samples 1000 --iterations 1000 --out base-1000-r1.csv
/tmp/chimp-candidate --encoding chimp   --samples 1000 --iterations 1000 --out cand-1000-r1.csv
/tmp/chimp-baseline2 --encoding chimp   --samples 1000 --iterations 1000 --out ctl-1000-r1.csv
/tmp/chimp-candidate --encoding gorilla --samples 1000 --iterations 1000 --out cgor-1000-r1.csv
/tmp/chimp-baseline  --encoding gorilla --samples 1000 --iterations 1000 --out bgor-1000-r1.csv
# ... rounds 2 and 3 in rotated order; then the same at --samples 10000 --iterations 300
```

The checked-in files are [1,000-sample medians](data/chimp-1000.csv),
[10,000-sample medians](data/chimp-10000.csv) and the [environment](data/chimp-environment.txt).

In the CSVs, `candidate` and `control` are paired against `baseline`, as the `against`
column says. `gorilla-baseline` and `gorilla` are Gorilla runs from the baseline and
candidate binaries. The Chimp/Gorilla ratios above pair `baseline` with
`gorilla-baseline`, and `candidate` with `gorilla`.

The pre-branch probe notes that motivated the plan are in
[data/chimp-review](data/chimp-review/notes.md).
