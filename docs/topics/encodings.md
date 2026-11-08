# Chunk Encoding Guide

This document describes the three time-series chunk encodings available in
valkey-timeseries, their algorithms, performance characteristics, and a selection
rubric for choosing the right encoding based on your data.

Compression figures (sizes, ratios, winners) come from the committed baseline,
`benches/baselines/compression_baseline.csv`, which reflects the current Chimp
grammar. Latency figures come from `latency_report` runs that include the Chimp
grammar change and the Gorilla changes (see
[Latency summary](#latency-summary) for how they were taken). For the Gorilla
changes and their fixed-sample A/B results, see
[the Gorilla evaluation](../benchmarks/gorilla-evaluation.md); for the Chimp
encoder changes, measured the same way, see
[the Chimp evaluation](../benchmarks/chimp-evaluation.md).

**Persistence.** New RDB, DUMP and rewritten AOF payloads use module encoding
version **2**, and this build reads versions 1 and 2 (the Gorilla extended-window
change, see [Gorilla](#2-gorilla)). The Chimp bit grammar was changed in place
(see [Chimp](#3-chimp-elf-on-chimp-default)) **without a version bump**, because
nothing has been released.

---

## Overview

| Encoding | Discriminant | Origin | Compression | Best For |
|---|---|---|---|---|
| **Uncompressed** | 1 | — | None (16 B/sample) | High-throughput ingest, heavy backfill, very short series |
| **Gorilla** | 2 | Facebook Gorilla (2015) | XOR + delta-of-delta | Repetitive data (constants, flags, discrete sets, integer counters); fastest encode |
| **Chimp** | 3 (default) | Chimp + ELF| ELF erasure + Chimp XOR | Decimal-quantized data, unknown workloads; fastest decode |

The default encoding is **Chimp**, but it can be changed at runtime via the
`ts-encoding` Valkey config parameter (`CHIMP`, `GORILLA`, or `UNCOMPRESSED`).
The alias `COMPRESSED` maps to the current default (Chimp).

The default chunk size is **4,096 bytes** (configurable via `ts-chunk-size`,
range 64 B – 1 MiB, must be a multiple of 8).

---

## Encoding Details

### 1. Uncompressed

**Algorithm:** None. Each sample is stored as a raw 16-byte pair
(`i64` timestamp + `f64` value) in a flat `Vec<Sample>`.

**Key constants:**
- 16 bytes/sample always — constant, not amortized
- Capacity is `chunk_size / 16` samples (`max_elements`), so a 4 KiB chunk holds
  256 and a 64 KiB chunk holds 4,096. 
- Binary search for timestamp lookups

**Strengths:**
- Zero CPU overhead for encode and decode — the only encoding whose cost does
  not scale with value entropy
- O(log n) random access via binary search
- No compression artifacts or precision loss
- Predictable memory footprint, and the only encoding that reports an *exact*
  rather than amortized `bytes per sample`
- Mutations are a `Vec` insert. Both compressed encodings must decode and
  re-encode the entire chunk on an out-of-order upsert (see
  [Mutation cost](#mutation-cost)), so this is the only encoding where backfill
  is cheap

**Weaknesses:**
- 16× larger on disk than the typical compressed representation
- Fits 2–32× fewer samples per chunk than a compressed encoding, so a
  fixed chunk budget covers a much shorter time span

**When to use:**
- Data that is entirely random (cryptographic, high-entropy sensor noise) where
  no compressor finds a pattern
- Workloads where ingest throughput is the only bottleneck and storage is
  abundant
- Short-lived series where compression overhead isn't amortized
- Debugging or benchmarking — uncompressed serves as the baseline for all
  compression comparisons

---

### 2. Gorilla

The Gorilla evaluation and its measured tradeoffs are recorded in
[the benchmark report](../benchmarks/gorilla-evaluation.md).

New persistence payloads use module encoding version **2**. This build reads
versions 1 and 2, including legacy Gorilla windows mixed with new windows after
an append. Older module builds reject version 2 RDB/DUMP/AOF payloads. Upgrade
readers before writers; a downgrade requires rebuilding data from logical samples.

**Algorithm:** Two-stage compression based on the Facebook Gorilla paper
(Pelkonen et al., 2015).

**Timestamp encoding — delta-of-delta:**
1. Sample 0: full 64-bit timestamp as varint
2. Sample 1: delta from previous as uvarint
3. Samples 2+: delta-of-delta through the Prometheus varbit buckets, sized for the deltas seen in
   practice. Regular intervals cost **1 bit** (`0`); everything else costs
   prefix + payload:

   | dod range | total bits |
   |---|---|
   | `0` | 1 |
   | ±15 | 7 |
   | ±255 | 12 |
   | ±4,095 | 17 |
   | ±65,535 | 22 |
   | ±8,388,607 | 30 |
   | ±2,147,483,647 | 39 |
   | ±3.6e16 | 64 |
   | anything else | 72 |

**Value encoding — XOR with leading/trailing zero optimization:**
1. Sample 0: raw 64-bit IEEE 754 value
2. Compute `xor = current ^ previous`
3. If `xor == 0` (unchanged): **1 bit** — the cheapest repeat of any encoding here
4. If the XOR fits a previous 32–57-bit window, keep that window to avoid
   frequent header changes on noisy values, unless an extended window can remove
   leading-zero padding. For compact windows (at most 31 bits) and wide windows
   (58–64 bits), compare the total costs of reuse and a fresh window, preferring
   reuse on a tie. Reuse costs 2 bits plus the old width; an ordinary fresh window
   costs 13 bits plus the new width.
5. A fresh ordinary window uses 2 control bits, a 5-bit leading count and a
   6-bit significant width (zero represents width 64).
6. For more than 36 leading zeros, a fresh extended window can pay off: the
   otherwise-invalid `(leading=31, width>33)` headers encode an extended
   leading count, followed by a 5-bit significant width. A zero width code
   represents leading=63. Its total cost is 18 bits plus the payload; subsequent
   reuses still cost only 2 bits plus the width. Ordinary windows keep their
   original header size. This targets large absolute counters and very small
   mantissa changes without adding a bit to every window.

A repeated value therefore costs **2 bits total** per sample (1 for the
delta-of-delta, 1 for the XOR), which is what produces the 64× ratio on
constant series.

**Key characteristics:**
- Sequential/causal: each point compresses using only the previous point

**Strengths:**
- The faster compressed encoder: Chimp takes 1.22–1.34× Gorilla's bulk-encode
  time and 1.25–1.38× its append time (1,000–10,000 samples per chunk)
- Unbeatable on repetition: constants (0.25 B/sample, 64×), discrete sets
  (12×) and counters (9×) are all Gorilla wins, by 1.06–2.00× over Chimp
- Delta-of-delta timestamps collapse regular intervals to 1 bit
- Mature, well-understood algorithm with predictable behavior

**Weaknesses:**
- Slower to read than Chimp: Chimp decodes in 0.82–0.83× Gorilla's time and
  scans in 0.81–0.83× of it
- No special handling for decimal precision — floating-point noise in the low
  mantissa bits consumes bits. This is the single largest compression gap
  against Chimp (up to 4.24×) and it fires on most agent-reported telemetry
- Lower geometric-mean ratio (3.16 vs 3.59) and a deeper worst case: in its
  weakest scenario Gorilla achieves only 0.24 of the best available ratio,
  against Chimp's 0.50

**When to use:**
- Write-heavy ingest where encode cost dominates
- Series dominated by repetition: constants, flags, discrete/enumerated values,
  integer counters
- Values that are integral or full-entropy `f64` — anything where there is no
  fixed decimal precision for ELF to exploit
- Data with regular or near-regular, strictly ascending timestamps

---

### 3. Chimp (ELF-on-Chimp, Default)

**Algorithm:** A two-layer codec combining ELF (Erasing-based Lossless
Floating-point) erasure with Chimp XOR compression. Both layers started as ports
of the published algorithms; the grammar has since been changed in place in
several places, noted below (no encoding version bump; see **Persistence** at the
top of this page). The source of truth is `src/series/chunks/chimp/`
(`compressor.rs` for the ELF layer and timestamps, `encoder.rs` for Chimp), and
golden streams in `src/series/chunks/chimp/fixtures/` pin the current grammar.

```
Sample (timestamp, f64 value)
          │
          ▼
┌──────────────────────┐
│   ELF Erasing Layer  │  ← Erases recoverable low mantissa bits
└──────────┬───────────┘
           │ (possibly erased u64 + case marker)
           ▼
┌──────────────────────┐
│   Chimp XOR Codec    │  ← Compresses the bit pattern
└──────────┬───────────┘
           │
           ▼
      BitStream
```

Each sample is written as `[timestamp][ELF marker][Chimp value]` into one bit
stream. The stream has no terminator: the chunk stores its sample count, and the
decoder reads exactly that many samples, so no bit pattern (NaN included) is
reserved as a sentinel.

**Layer 1 — ELF erasure:**

ELF clears the low mantissa bits a value does not need for its decimal places,
when the decoder can provably put them back. In practice, values like `3.14`
(stored as `3.140000000000000124...`) lose most of their spurious low bits,
because the original precision is known to be 2 decimal places; those bits were
IEEE 754 representation artifacts, not information.

Every value carries a 1-, 2- or 6-bit case marker ahead of its Chimp value:

| Marker | Meaning | Value is |
|---|---|---|
| `0` | Same decimal places as the last `11` header (or a repeat, see below) | erased to `alpha` places |
| `10` | Raw | the bit pattern verbatim |
| `11` + `alpha` (4 bits) | New decimal-place count, `alpha` in 0..=15 | erased to `alpha` places |

The header carries `alpha`, the number of decimal places, and the decoder
recovers an erased `v'` as

```
v = copysign(ceil(|v'| · 10^alpha) / 10^alpha, v')
```

which needs no scale position or digit count from the value itself. The encoder
erases only when that recovery returns the original value bit for bit, when the
erasure clears more than 4 bits, and when the erased value is not already the
bit pattern Chimp holds (that pairing spells a repeat). Anything else is stored
raw, so the layer is lossless.

How the encoder picks `alpha` (policy, not format; the decoder only reads the
header):

- **Values on a 1/16 grid skip ELF.** A normal double that is a multiple of
  1/16, which includes every integer, has no erasable bits, so it goes straight
  to the raw marker without a digit count. So do zero, the infinities and NaN.
  Counters, enums and other integral series pay the 2-bit raw marker and nothing
  else.
- **The previous `alpha` is tried first.** Quantized data keeps one precision,
  so the encoder first tries to erase to the last header's `alpha` and, if that
  recovers exactly, writes marker `0` without counting digits. This also keeps a
  value with fewer places (`100.2` after `100.23`) under marker `0`. The hint is
  skipped after a raw value, and every 16th sample counts digits regardless, so
  a series whose precision drops is re-measured within 16 samples.
- **Otherwise the decimal places are counted** (at most 15 significant digits
  and 15 decimal places; more is stored raw).

**Layer 2 — Chimp XOR compression:**

Chimp compresses the XOR of each (possibly erased) bit pattern with the previous
one under a 2-bit flag. The first value is 64 raw bits.

| Flag | Fields | Bits | Meaning |
|---|---|---:|---|
| `00` | — | 2 | XOR is zero: the value repeats. The stored window is kept |
| `01` | lz code (3), width (6), payload | 11 + width | New window with trailing zeros: leading zeros rounded down to a bucket, `width = 64 − lz − tz` (the decoder reads 0 as 64; the encoder never writes it) |
| `01` | `111`, ext (6), width (6), payload | 17 + width | Extended window: an `ext` field of 41..=63 names an exact leading count `32 + (ext − 41)`, 32..=54 |
| `10` | payload | 2 + width | Reuse the stored window: the same leading **and** trailing zeros |
| `11` | lz code (3), payload | 5 + (64 − lz) | New window with no trailing zeros |

The leading-zero code names one of eight buckets: 0, 8, 12, 16, 18, 20, 22, 24.
An XOR's leading-zero count rounds down to a bucket, and the difference is
carried as payload padding. The encoder writes `01` when the XOR has more than
6 trailing zeros and `11` otherwise. The extended `01` form is chosen by cost
instead, whatever the trailing zeros: whenever it and its payload are strictly
shorter than that header and its payload.

What the grammar change altered from the published Chimp:

- **`10` reuses the whole window.** The stored window is the last header's
  `(leading, trailing)` pair, and `10` writes the XOR's bits between them. The
  encoder takes it whenever the XOR fits (at least as many leading and trailing
  zeros) and `2 + width` is no more than the fresh header plus its payload would
  cost. A repeat leaves the window as it was.
- **Extended leading zeros.** Under the top bucket (24), width fields of 41..=63
  can never be a real window, so they escape to an exact leading count of
  32..=54 followed by a second 6-bit width. Large, slowly changing values (a
  counter far above zero, for example) then stop paying up to 30 bits of padding
  per window. The encoder rounds the count down to `32 + 3k` (headroom for later
  XORs to reuse the window) and uses the escape only when it is strictly cheaper
  than an ordinary header.

**Repeated values** bypass ELF entirely and are written as ELF marker `0` plus
Chimp's `00` flag — **3 value bits**, or 4 bits per sample with a regular
timestamp. That is double Gorilla's 2, and it is the whole reason Gorilla wins
the constant and discrete workloads.

Reserving that pairing for repeats is not free: a *distinct* value can erase
onto the bit pattern Chimp already holds, and the encoder must push those onto
the raw path so they cannot be read back as a repeat.

**Timestamp encoding:** The first timestamp is a varint. Every later one is the
zig-zagged delta-of-delta `d` behind a unary prefix:

| Prefix | `d` range | Total bits |
|---|---|---:|
| `0` | 0 | 1 |
| `10` | −64..63 | 9 |
| `110` | −256..255 | 12 |
| `1110` | −2,048..2,047 | 16 |
| `11110` | −32,768..32,767 | 21 |
| `111110` | −2^31..2^31−1 | 38 |
| `111111` | anything else | 70 |

The grammar change added the 16-bit tier (21 bits) between the 12- and 32-bit ones, so a
delta-of-delta beyond ±2,048 but within ±32,768 (milliseconds, for millisecond
timestamps) no longer takes the 32-bit escape, which was 37 bits before; the two
escapes each cost one bit more than they did (38 and 70, from 37 and 69). Because the delta-of-delta is zigzagged and signed,
**descending timestamps round-trip**, where Gorilla's encoder refuses them.

**Key characteristics:**
- Two-layer design: ELF erasure runs first, Chimp compresses the result

**Strengths:**
- **Dramatically better on quantized data.** Values recorded to a fixed number
  of decimal places (e.g., `12.34`, `99.9`, `0.001`) compress 2.6–4.2× better
  than Gorilla because ELF erases the spurious low mantissa bits
- The faster reader: decode in 0.82–0.83× Gorilla's time, full-range reads in
  0.84–0.85× and scans in 0.81–0.83× (1,000–10,000 samples per chunk)
- 2-bit fixed-width control flags enable efficient branch prediction
- **The best choice when the workload is unknown.** Highest geometric-mean
  ratio (3.59 vs Gorilla's 3.16), most scenarios within 5% of best (24/28 vs
  18/28), and the shallowest floor (0.50 of best in its worst scenario, against
  Gorilla's 0.24)
- Accepts every `f64` including NaN and ±inf: NaNs bypass ELF erasure and are
  stored as raw bit patterns, so payload bits such as a Prometheus stale marker
  survive the round trip unchanged

**Weaknesses:**
- Slower to encode: 1.22–1.34× Gorilla's bulk-encode time and 1.25–1.38× its
  append time. Before the encoder changes it was 1.6–1.9×; skipping the ELF
  search for integers and short binary fractions, and trying the previous
  decimal-place count first, closed most of that gap
- Repetition costs double: 4 bits per sample against Gorilla's 2, which loses
  the constant workload by 2.00× and discrete by 1.35×
- On unquantized floating-point data with random low mantissa bits, Chimp is a
  near-tie with Gorilla — ELF finds nothing to erase and the rounded
  leading-zero encoding is slightly less precise (Gorilla leads `noisy` by up to
  1.04×)

**When to use:**
- **Data recorded with fixed decimal precision** — currency, percentages,
  sensor readings at known resolution. This is the highest-value rule in this
  document; see [Quantization matters](#quantization-matters)
- Any series whose shape you do not know in advance
- Metrics scraped from systems that round to 2–4 decimal digits

---

## Selection Rubric

### Rule 0: quantization dominates everything else

Before consulting any table below, answer one question: **are the values
reported to a fixed, small number of decimal places?** Percentages,
temperatures, currency, latencies in milliseconds — anything emitted by a
collection agent that rounds — all qualify.

If yes, choose **Chimp** and stop. It is worth 2.60–4.24× over Gorilla, which
is larger than every other effect in this document combined. No other single
property of the data comes close to mattering as much.

If no, continue.

### By Data Type

Winners below are the measured winner in `compression_baseline.csv` at 4 KiB
with regular timestamps, not estimates. "Margin" is over the runner-up.

| Data Pattern | Real-World Examples | Best Encoding | Margin | Why |
|---|---|---|---|---|
| **Quantized decimals** | Agent-reported CPU %, temperatures, latency at 2 decimal places | **Chimp** | **2.60–4.24×** | ELF erases the IEEE 754 artifacts Gorilla must carry |
| **Constant / rarely changing** | Config values, feature flags, idle gauges, thermostat setpoints | **Gorilla** | **2.00×** | 2 bits per repeated sample vs Chimp's 4; 64× compression |
| **Discrete / low-cardinality** | HTTP status codes, enum values, small integer sets | **Gorilla** | **1.35×** | Many XOR repeats; 12× compression |
| **Monotonic integer counters** | `http_requests_total`, `bytes_sent`, queue depths | **Gorilla** | 1.06× | Integral values give ELF nothing to erase; its 2-bit raw marker is pure overhead |
| **Periodic / seasonal** | Daily traffic patterns, hourly cron job metrics | **either** | ~1.02× | Full-precision sine/sawtooth values leave ELF little to erase |
| **Bursty / spikey** | Error rates, alert counts, intermittent metrics | **Chimp** | 1.09× | Quiet stretches erase well; bursts fall back to raw |
| **Smooth floating-point gauges** | Full-precision drift, computed averages | **either** | ~1.00× | Exact tie at 4 KiB (2.68 vs 2.68); decide on encode/decode cost instead |
| **High-entropy / noisy** | Wide-spread sensor noise, ML model outputs | **either** | ~1.04× | Still compresses **2.15×** — do *not* reach for Uncompressed here |
| **Genuinely random bits** | Cryptographic values, hash outputs | **Uncompressed** | — | No pattern exists; avoid paying encode cost for nothing |

Note the shape of this table: outside the quantized, constant and discrete
shapes, the two compressed encodings are far more alike than having two of them
suggests. 24 of 28 measured scenarios put Chimp within 5% of the best available
ratio, and 18 of 28 do the same for Gorilla.

### By Operational Concern

| Concern | Recommendation |
|---|---|
| **Maximize ingest throughput** | **Uncompressed** — roughly 50–70× cheaper bulk encode than either compressed encoding |
| **Minimize storage, shape known** | Per the data-type table above |
| **Minimize storage, shape unknown** | **Chimp** — best geomean (3.59) and shallowest worst case (0.50 of best) |
| **Minimize decode / scan latency** | **Chimp** — decodes in 0.82–0.83× and scans in 0.81–0.83× Gorilla's time |
| **Minimize encode latency** | **Gorilla** — Chimp takes 1.22–1.34× its bulk-encode and 1.25–1.38× its append time |
| **Predictable memory** | **Uncompressed** — exactly 16 B/sample, `chunk_size / 16` samples |
| **Best balance (default)** | **Chimp** — the shipped default; see [On the default](#on-the-default) |

### Timestamp regularity

Irregular arrival costs both compressed encodings roughly the same and flips
the winner only where the two were within 1% at a regular cadence, so it is not
a selection criterion — but it does change what you should expect:

| Timestamp model | Gorilla | Chimp |
|---|---|---|
| Regular (fixed interval) | 2.68 | 2.68 |
| Jitter (±50 ms) | 2.09 | 2.28 |
| Irregular (exponential gaps) | 1.91 | 2.03 |

(`drift` workload, 4 KiB chunks.) Chimp degrades slightly more gracefully on
`drift`; on `noisy` the two lose about the same. Budget for roughly a 20–30%
ratio loss going from regular to irregular arrival.

### Quantization Matters

The single biggest factor in choosing Gorilla vs Chimp is whether your values
have a known decimal precision. IEEE 754 `f64` representation introduces
spurious low bits that no XOR-based compressor can exploit — but ELF can erase
them.

**Example:** The value `12.34` is stored as `12.339999999999999857...` in IEEE
754. Gorilla sees random-looking low bits in the XOR delta and encodes them all
(~5–6 bytes). Chimp recognizes that the original had 2 decimal significant
digits, erases the spurious bits, and encodes only the meaningful ones (~1.5
bytes).

This effect is visible in the benchmark data for `_q2` (2-decimal quantized)
workloads:

| Workload | Gorilla (B/sample) | Chimp (B/sample) | Chimp Advantage |
|---|---|---|---|
| `drift_q2` | 4.29 | 1.01 | **4.2× better** |
| `noisy_q2` | 7.14 | 2.75 | **2.6× better** |
| `bursty_q2` | 4.14 | 1.01 | **4.1× better** |
| `periodic_q2` | 7.19 | 2.02 | **3.6× better** |

If your data source rounds to a known precision, **Chimp** is the clear winner.
If your data is raw `f64` with full mantissa entropy (e.g., ML model outputs,
scientific measurements), the two are a near-tie — Gorilla leads the `noisy`
workload by only 1.04× — so choose on encode/decode cost rather than size.

Note the asymmetry in risk: choosing Chimp for data that turns out to be
unquantized costs about 4%, while choosing Gorilla for data that turns out to be
quantized costs up to 324%.

---

## On the Default

The shipped default is **Chimp** (`DEFAULT_CHUNK_ENCODING` in
`src/config.rs`), and the `COMPRESSED` alias resolves to it.

The measurements support that choice for an unknown workload. The scenario count
is close (Chimp wins 13 of 28, Gorilla 14, one tie; Gorilla's 14 are the
repetitive shapes plus `noisy` and one `drift` row, each of those by under 5%), but Chimp has the higher geometric-mean ratio (3.59 vs 3.16),
lands within 5% of the best available ratio more often (24/28 vs 18/28), and has
a much shallower floor (0.50 vs 0.24) — the number that bounds how badly a blind
default can go wrong. It is also the cheaper reader on all three read paths.

The cost of that default is **encode speed**: Chimp takes 1.22–1.34× Gorilla's
bulk-encode time and 1.25–1.38× its append time. If ingest is your binding
constraint, or your series are dominated by repetition (constants, flags,
discrete sets, integer counters), set `ts-encoding GORILLA` — Gorilla's wins are
real, but narrow in scope.

If you are choosing per-series with `TS.CREATE ... ENCODING`, keep the default
unless the data-type table above points elsewhere.

---

## Configuration

| Parameter | Valkey Config | Default |
|---|---|---|
| Encoding | `ts-encoding` | `CHIMP` |
| Chunk size | `ts-chunk-size` | `4096` (4 KiB) |
| Duplicate policy | `ts-duplicate-policy` | `BLOCK` |

Per-series override: `TS.CREATE` and `TS.ALTER` accept `ENCODING` and
`CHUNK_SIZE` arguments to override the global defaults for individual
time-series keys.

---

## Appendix: Measurements and Methodology

Every figure in this document comes from the report tools in `tools/`,
documented in `AGENTS.md` under Benchmarks. Neither is a criterion bench.

```sh
tools/compression_report.sh --check                                   #  84 rows
tools/latency_report.sh --workloads all --ts-models all --samples 1000 \
  --iterations 500 --warmup 50                                        # 108 rows
```

Outputs land in `target/bench-reports/{compression,latency}.{csv,md}`.
The compression baseline is committed at
`benches/baselines/compression_baseline.csv`; `--check` fails the run on a
regression against it.

**Size and ratio figures are deterministic and reproduce exactly.** Timings are
wall-clock medians, machine- and load-dependent: **compare rows within one run,
never absolute numbers across machines.** The timings here were taken on an
Apple M2 in release profile with `--features enable-system-alloc,test-utils`.

### Compression summary

28 scenarios = workload × timestamp model × chunk size (1k / 4k / 64k).
Ratio is `(len * 16) / data_size`; higher is better.

| Encoding | Geomean ratio | Scenarios won | Within 5% of best | Worst case vs. best |
|---|---:|---:|---:|---:|
| **Chimp** | **3.59** | 13/28 | **24/28** | **0.50** |
| Gorilla | 3.16 | **14/28** | 18/28 | 0.24 |

One scenario (`drift`, regular, 4 KiB) is an exact tie.

"Worst case vs. best" is the floor: the lowest ratio the encoding achieves as a
fraction of the best encoding in that same scenario. It is the number that
matters when the workload is unknown, because it bounds how badly the choice can
go wrong.

### Latency summary

Measured on 2026-11-08, with the Chimp grammar change, the Chimp encoder changes
and the Gorilla changes in place. Every workload and timestamp model was run at
1,000 and 10,000 samples per chunk, 500 iterations after 50 warmups. Five passes
alternated with a build of the code before those changes, and each cell is the
fastest of its five medians.

Median time per operation across the 36 workload × timestamp cells, in
microseconds. Lower is better.

| Encoding | Samples | Encode bulk | Encode append | Decode iter | `get_range` | Scan mid 10% |
|---|---:|---:|---:|---:|---:|---:|
| Uncompressed | 1,000 | 0.21 | 1.83 | 0.88 | 0.21 | 0.04 |
| Gorilla | 1,000 | **10.3** | **10.9** | 11.3 | 11.6 | 4.98 |
| Chimp | 1,000 | 11.8 | 13.0 | **10.0** | **10.8** | **4.23** |
| Uncompressed | 10,000 | 2.02 | 17.7 | 8.79 | 2.04 | 0.04 |
| Gorilla | 10,000 | **120** | **127** | 150 | 156 | 70.9 |
| Chimp | 10,000 | 143 | 153 | **112** | **120** | **53.5** |

Chimp's time divided by Gorilla's, as the median over matched cells:

| Samples | Encode bulk | Encode append | Decode iter | `get_range` | Scan mid 10% |
|---:|---:|---:|---:|---:|---:|
| 1,000 | 1.22× | 1.25× | 0.82× | 0.85× | 0.83× |
| 10,000 | 1.34× | 1.38× | 0.83× | 0.84× | 0.81× |

Read this as: compression costs one to two orders of magnitude on encode against
a raw `Vec`, Gorilla is the cheaper encoder, and Chimp is the cheaper reader on
all three read paths. Uncompressed times under a microsecond are a few ticks of
the 42 ns timer, so ratios against them are not meaningful.

Against the code before these changes, Chimp encode is 28–34 % faster and its
reads 10–15 % faster; Gorilla encode is 6–11 % faster and its reads 2–7 % faster.

### Workload detail (4 KiB chunks, regular timestamps)

Samples fitting in one chunk; higher is better.

| Workload | Uncompressed | Gorilla | Chimp | Winner |
|---|---:|---:|---:|---|
| `constant` / `constant_int` | 256 | **16318** | 8159 | Gorilla 2.00× |
| `discrete` | 256 | **3142** | 2334 | Gorilla 1.35× |
| `counter` | 256 | **2331** | 2198 | Gorilla 1.06× |
| `drift_q2` | 256 | 957 | **4049** | Chimp 4.24× |
| `bursty_q2` | 256 | 989 | **4063** | Chimp 4.11× |
| `periodic_q2` | 256 | 570 | **2025** | Chimp 3.55× |
| `noisy_q2` | 256 | 574 | **1491** | Chimp 2.60× |
| `bursty` | 256 | 629 | **684** | Chimp 1.09× |
| `periodic` | 256 | 566 | **576** | tie (1.02×) |
| `noisy` | 256 | **572** | 552 | tie (1.04×) |
| `drift` | 256 | 686 | 686 | tie (1.00×) |

Workload shapes are defined in `src/tests/generators/workload.rs`. The `_q2`
variants are the same shapes rounded to 2 decimal places — "the precision most
collection agents report at".

---

## References

- **Gorilla:** Pelkonen, T., et al. "Gorilla: A Fast, Scalable, In-Memory Time
  Series Database." *PVLDB*, 2015.
- **Chimp:** Liakos, P., et al. "Chimp: Efficient Lossless Floating Point
  Compression for Time Series Databases." *PVLDB*, 2022.
- **ELF:** Li, R., et al. "ELF: Erasing-based Lossless Floating-Point
  Compression." *PVLDB*, 2023.
- Implementation credits in source: `src/series/chunks/gorilla/` (ported from
  SINTEF/rusty-chunkenc), `src/series/chunks/chimp/` (ported from
  gr.aueb.delorean.chimp + org.urbcomp.startdb.compress.elf).
