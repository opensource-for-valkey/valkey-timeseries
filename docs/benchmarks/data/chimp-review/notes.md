Provenance: probes ran against commit `f6e446a35` on 2026-10-29 from standalone sources that were in `/tmp/valkey-chimp-review` (not retained); CSV columns are as the notes' *Method and limitations* section describes.

# Chimp review experiments

Reviewed repository commit `f6e446a359a30d8748d99a3a7f39dc4a62d66ae2` on 2026-10-29. No tracked files were changed.

## Most promising latency change: bypass ELF for simple binary fractions

`write_value` runs `get_alpha_and_beta_star` on every distinct, finite, nonzero value.
Exact integers ultimately take the raw path: the low fraction bits are already zero,
or recovery of an erased non-integer would not reproduce the original integer.
Values on a 1/16 grid also cannot benefit: with alpha >= 1, get_f_alpha(alpha) >= 4,
so there are already enough zero fraction bits; alpha zero would recover an integer,
which cannot equal a non-integer grid value. Keep existing special-value and repeat
handling, and the exact recovery check on the remaining ELF path.

An exploratory probe bypassed ELF for integers, or for values where `v*16` is integral.
Both variants produced byte-identical serialized encoders, including final state,
through 42 generated dataset cases and 3,150 boundary values (signs, subnormals,
NaNs, infinities, powers of ten, and dyadic fractions over several magnitudes).
An overflow of `v*16` occurs only at magnitudes already integral as f64; non-finite
inputs follow the existing raw path. A bit-based classifier is another implementation
option, to be benchmarked rather than assumed faster.

Three-run median, 1,000 samples, regular timestamps:

| Case | Original probe | Integer bypass | 1/16-grid bypass | Payload bytes |
|---|---:|---:|---:|---:|
| Counter | 22.000 us | 7.041 us | 6.667 us | 2,378 unchanged |
| Discrete | 20.375 us | 16.083 us | 5.500 us | 2,207 unchanged |
| Counter + 2^50 | 16.292 us | 5.958 us | 6.000 us | 5,636 unchanged |

This is NOT a universal speedup: the broader grid check adds work on full-precision
values, and the isolated periodic case was slower. Start with the integer bypass,
measure a cheap grid classifier, and validate actual production paths before landing.
A further candidate is caching a successful decimal precision/mask and trying that
with exact recovery before the digit search; guard against penalties on mixed and
non-erasable data. Do not discard the lossless recovery check.

## Compression: reuse the full XOR window (new format)

The decoder already tracks a trailing count, but flag `10` reads `64-stored_lz`
bits and ignores it. A prototype made flag `10` reuse both leading and trailing
counts, retained the window over repeats, reset trailing count on a plain new-leading
header, and compared reuse vs fresh-header bit costs. No larger dictionary was needed.

| Case, 1,000 samples / regular timestamps | Current payload | Full-window prototype | Reduction |
|---|---:|---:|---:|
| Quantized drift | 1,491 B | 997 B | 33.1% |
| Quantized periodic | 2,727 B | 2,144 B | 21.4% |
| Counter | 2,378 B | 1,895 B | 20.3% |
| Quantized bursty | 1,934 B | 1,561 B | 19.3% |
| Quantized noisy | 3,669 B | 3,123 B | 14.9% |

The new stream round-tripped exactly through a matching experimental decoder for
all 42 generated cases and the 3,150 boundary values. The current decoder cannot
read this format. Persistence/append of the added trailing state was NOT implemented.
Do not call this a drop-in change. Some full-precision cases saved essentially
nothing and incurred more encode work; choose based on the full matrix.

## Smaller format-compatible XOR improvements

The encoder resets `stored_lz=65` after repeats, although the decoder retains its
leading count. After the trailing-zero case the decoder knows the header's new lz,
while the encoder invalidates it again. Retaining these states lets a subsequent
matching flag `10` avoid a three-bit leading code. Existing decoder acceptance,
serialization/reload, and continued encoding were validated in the probe.
Measured gains were tiny on these datasets (typically 0-3 bytes per 1,000 samples),
so this should be lower priority than ELF bypass or full-window reuse.

The fixed `tz > 6` threshold is only optimal relative to a new leading-only header:
`11 + (64-lz-tz)` vs `5 + (64-lz)`. When leading count can be reused, the alternative
is `2 + (64-lz)`, so trailing counts 7 and 8 actually cost two and one extra bits;
nine is a tie. A state-aware choice can preserve the old format. Evaluate future
state and decode cost, rather than assuming the locally smallest encoding wins.

## Timestamp encoding

The current 32-bit escape is three writes (4-bit prefix, selector, 32-bit payload).
It can be one 37-bit write. The 64-bit escape can use two writes (5-bit header and
64-bit payload). The probe's fused variant produced byte-identical streams; timing
was workload-sensitive, so there is no broad production speedup claim.

The jump from a 12-bit payload to 32 bits is also a compression opportunity. A
model kept all small prefixes, then used 16/32/64-bit escape payloads with total
lengths 21/38/70 bits instead of 37/69. Ordinary zero/jitter prefixes are unchanged.

| Irregular, 1,000 samples | Current payload | 16-bit bucket size model |
|---|---:|---:|
| Constant | 2,558 B | 2,320 B (9.3% smaller) |
| Quantized drift | 3,558 B | 3,292 B (7.5% smaller) |
| Drift | 8,086 B | 7,830 B (3.2% smaller) |

This is a bit-length model, not an implemented/validated new timestamp decoder.
A separate candidate is folding the common zero timestamp bit into the existing
ELF/Chimp prefixed write, particularly for repeated values; that was not benchmarked.

## Leading-count limit and repeat floor

The rounded leading-count table tops out at 24. An XOR of 4 has 61 leading zeros,
but falls onto a 40-bit payload path because its two trailing zeros miss the trim
threshold. The current 1,000-sample counter shifted by 2^50 uses 5,636 payload bytes.
Test an extended leading representation or a per-chunk palette for large counters;
uniformly coarsening the usual buckets can harm common floating-point workloads.

Flat regular data has a fixed four-bit/sample floor (one timestamp bit plus three
repeat bits). A run-length or combined-record mode could improve that further,
but requires another format and care with streaming appends and range expansion.

## Format deployment matters

Chimp is the default encoding in samples_to_chunk for fan-out. Serialized chunks
currently carry an encoding discriminator but no separate Chimp bitstream version.
Any new grammar needs explicit wire versioning/capability handling as well as RDB
versioning; an RDB-only bump would not protect a mixed-version cluster coordinator.
Keep the centralized wire policy; no additional encoding-selection tier is proposed.

## Separate correctness finding

In `ChimpDec::next_value`, flag `01` maps significant width 0 to 64, then computes
`stored_tz = 64-sig-stored_lz` without validating the sum. A header specifying
rounded leading=24 and width=64 yields -24 and shifts by 232 after casting to u8.
The exact repository decoder code reproduced a caught debug-assertion panic in a
standalone process. Validate `sig + stored_lz <= 64` before the subtraction/shift.
This was not exercised against a live server.

## Method and limitations

- `summary.csv`: medians of three probe runs. Each timing is a median of 301
  whole 1,000-sample encodes after 50 warmups; allocation is timed, destruction is not.
- The encoder probe copies the repository's ELF and XOR methods and actual writer
  methods into a standalone `rustc -O` binary. The baseline serialized bytes match
  production `ChimpCompressor` exactly for every case.
- The generated matrix is 12 shapes x 3 timestamp models plus counters shifted by
  2^40 and 2^50 under all models: 42 cases. The raw-bypass/state/escape variants were
  read with the current production decoder, then serialized, reloaded, appended to
  using the current production encoder, and decoded again.
- Full-window reuse has only a matching experimental stream decoder, not a persisted
  encoder implementation. Its size reductions are deterministic; its timings are
  exploratory and include different state and code layout.
- These are component probes, not integrated command/server benchmarks or guarantees.
  Variants ran in a fixed order, not a randomized trial. Small timing differences
  can reflect code generation, cache state and CPU scheduling. Production release
  uses LTO; the standalone probe does not. Do not transfer absolute microsecond times
  or single-digit-percent differences directly to production.
- `production-latency.csv` / `.md` were generated with the existing release tool.
- `timestamp-model.csv` is explicitly a size model; `edge-checks.txt` records the
  successful boundary checks and the intentionally caught malformed-header panic.
- Sources and executables remain under `/tmp/valkey-chimp-review`. No tracked
  repository files were modified. Only this ignored report directory was written.

Validation: `cargo test --offline --features enable-system-alloc --lib series::chunks::chimp -q --jobs 1` passed all 45 existing Chimp tests. Worktree remains clean.
