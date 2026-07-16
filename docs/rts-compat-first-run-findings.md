# RTS compatibility harness — first differential run findings

**Date:** 2026-07-15
**Reference:** `redis:8.6@sha256:a051e4f48a5d0ceda6554974f3ad0f5369f4479197f36829332c1325cecad2b7`
**Subject:** valkey-server `unstable` + `libvalkey_timeseries` (branch `compatibility`)
**Result:** 27 passed / 21 failed of 48 smoke cases (24 scenarios × RESP2/RESP3)
**Update 2026-07-15:** findings #1, #4, #5, #6, #9, and #11 fixed (see below); suite now at 40 passed / 8 failed

This is the raw triage input the test plan's Phase 1 calls for
(docs/rts-compatibility-test-plan.md §10): each finding must be resolved as either a
module fix or a reviewed entry in `tests/compat/divergences.yml`. Reproducers are in
the failure output of `tests/compat/test_compat_smoke.py`; every finding below was
observed directly against the pinned reference image.

The frozen `TS.INFO` baseline (`tests/compat/info-fields-8.6.yml`) was validated
against the live reference: the RTS 8.6 field set matches exactly.

## Findings

| # | Layer | Surface | Observed difference | Suggested disposition |
|---|---|---|---|---|
| 1 | L1 | `TS.MRANGE`/`TS.MGET` RESP3 | RTS returns a native **map** keyed by series name, with per-series `{aggregators: []}` metadata (MRANGE); we return the RESP2 array shape on both protocols | **FIXED 2026-07-15** — RESP3 clients now get the map shapes (`[labels-map, {aggregators}, samples]`; grouped: `[labels-map, {reducers}, {sources}, samples]`; MGET: `[labels-map, sample]`), verified against the reference in standalone and cluster (fanout) modes. The fix also made MGET `SELECTED_LABELS` report a missing label as `[name, nil]` instead of a bare `nil` (RESP2), matching the reference |
| 2 | L2 | `TS.RANGE ... ALIGN n AGGREGATION ...` | Bucket assignment differs (e.g. `ALIGN 250`, bucket 1000: RTS buckets `[250,1250)` etc. with first bucket clamped to range start; ours groups differently) | **Fix** — semantic aggregation bug |
| 3 | L1 | `TS.MADD` per-item errors | RTS embeds real RESP **error** replies inside the array; we return the error text as a **bulk string** | **Fix** — clients type-check array elements |
| 4 | L1 | `TS.INFO` `sourceKey` | RTS always emits `sourceKey` (nil when not a compaction); we omit it entirely for non-compaction series | **FIXED 2026-07-15** — `sourceKey` is now always present, nil for non-compactions (and nil as a defensive fallback if the source id cannot be resolved) |
| 5 | L1 | `TS.INFO` `labels` | RTS emits an empty **array** for a label-less series; we emit **nil** | **FIXED 2026-07-15** — a label-less series now emits an empty `labels` array (RESP2), matching the reference. The RESP3 map rendering of `labels` (empty `{}` and non-empty) remains #11 |
| 6 | L1 | `TS.INFO` `rules` aggregator | RTS reports the aggregator uppercase (`AVG`); we report lowercase (`avg`) | **FIXED 2026-07-15** — `TS.INFO` `rules` now reports the aggregator uppercase (`AVG`, `STD.P`, `TWA`, …), matching the reference for all supported aggregators. Scoped to TS.INFO only; the TS.MRANGE reducer/aggregator metadata stays lowercase (also matching the reference). RESP2/RESP3 regression assertions in `test_ts_info.py`; the `CompactionRule` test helper compares aggregators case-insensitively |
| 7 | L3 | WRONGTYPE condition | RTS: `TSDB: the key is not a TSDB key`; we: standard `WRONGTYPE ...` | **Decide** — different error *class*, not just text; our form is arguably more idiomatic Valkey. Needs owner call: fix vs `behavior` entry |
| 8 | L3 | Duplicate-sample error text (`TS.ADD`/`TS.MADD`, BLOCK policy) | RTS: long "Error at upsert…" message; we: `TSDB: duplicate sample` | **Register** (`error-text`) or fix; same `TSDB:` prefix, same condition |
| 9 | L3 | Arity error text | RTS lowercases the command name (`'ts.queryindex'`); we echo uppercase (`'TS.QUERYINDEX'`) | **FIXED 2026-07-15** — commands are now registered in lowercase (`ts.create`, `ts.queryindex`, …) in `lib.rs`, so Valkey echoes the lowercase name in the arity error (and in `COMMAND INFO`/`DOCS`), matching the reference. Command dispatch is case-insensitive, so clients may still call `TS.CREATE`. This also resolves the `COMMAND INFO` name-casing divergence (§7.2 parity). Existing tests updated: arity-error string matches lowercased across the suite, and `COMMAND INFO` lookups made case-insensitive |
| 10 | L1 | `TS.INFO` extra fields | We emit `metric`, `encoding` (+ `rounding` when set) beyond the RTS set | Already registered as **DIV-0001** (`reply-superset`) |
| 11 | L1 | `TS.INFO` RESP3 shapes | RTS renders `labels` and `rules` as native RESP3 **maps** (`labels: {name: value}`, `rules: {}` when empty); we return the RESP2 array shapes on both protocols. Unmasked by the #4 fix — same class of gap as #1 | **FIXED 2026-07-15** — `TS.INFO` now branches on the client protocol: RESP3 emits `labels` as `{name: value}` and `rules` as `{destKey: [bucketDuration, aggregator, alignTimestamp]}` (both empty as `{}`); RESP2 shapes unchanged. Verified against the reference (empty and non-empty), with subject-only RESP2/RESP3 regression tests in `test_ts_info.py`. (The aggregator string is still lowercase — that residual `AVG` vs `avg` delta is #6, unchanged) |

| 12 | L1 | `TS.INFO DEBUG` `bytesPerSample` type | In the per-chunk `Chunks` list, RTS types `bytesPerSample` as a **double** (`4096.0`) in RESP3 and a bulk string (`b'4096'`) in RESP2; we emit a bulk string on both. Observed while probing #11; DEBUG-only, out of #11 scope | **Decide** — low priority (DEBUG surface; §6 compares chunk *shape* not byte values). Fix by typing it as a double, or register a `behavior` divergence |

Not yet exercised by the smoke subset (Phase 1/2 scope): config parity (§7.1 — the
`ts-chunk-size` vs `ts-chunk-size-bytes` naming suspects), keyspace notifications,
COMMAND INFO metadata, persistence interop (§7.4), compaction deep-dive, fuzzing.

## Gate status

The `compat-smoke` CI job runs non-blocking (`continue-on-error`) until the findings
above are triaged and the suite is green with a reviewed registry; flipping it to
blocking is the Phase 1 exit action (plan §8).
