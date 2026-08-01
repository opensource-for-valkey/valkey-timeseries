# Differential fuzzer regression corpus

Each `*.json` file here is a minimal command sequence that must stay
byte-identical to RedisTimeSeries 8.10. `test_compat_corpus.py` replays every
file through the `diff` client under RESP2 and RESP3; a regression that reopens
the underlying bug fails as a plain mismatch.

Two sources feed this directory:

- **Fuzzer shrinks (test plan §4.3).** When `COMPAT_FUZZ=1`
  (`test_compat_fuzz.py`) finds a divergence, Hypothesis shrinks it to a minimal
  reproducer. Copy that command list into a new file here, so the case becomes a
  permanent, deterministic Tier A golden test even after the fuzzer moves on.
- **Hand-written reproducers.** Minimal sequences pinning bugs found by other
  means (matrix work, black-box probing).

## File schema

```json
{
  "description": "one line: what this pins",
  "origin": "manual reproducer" | "fuzzer shrink 2026-07-16",
  "expect_divergence": null,
  "commands": [
    ["TS.CREATE", "fz:k0", "..."],
    ["TS.ADD", "fz:k0", "0", "1.5"]
  ]
}
```

- Every command argument is a **string** (a fuzzer shrink round-trips losslessly
  this way, and JSON has no integer/float ambiguity to trip on).
- `expect_divergence` is optional. Set it to a `DIV-` id when the reproducer's
  only remaining delta is an intentional divergence already in
  `divergences.yml`; the replay tolerates it via the registry, and the field
  documents *which* divergence the case rides on. Leave it `null` for a case
  that must diff perfectly clean.

## Adding a case

1. Get the command list (from a fuzzer shrink or written by hand).
2. Save it as `corpus/<short-slug>.json` using the schema above.
3. Run `COMPAT_REFERENCE_URL=... python3 -m pytest tests/compat/test_compat_corpus.py -k <slug>`
   and confirm it passes under both RESP versions.

Clean-room rule applies (see `../README.md`): corpus cases are derived from
public documentation and black-box observation, never from RedisTimeSeries
source or test code.
