# RTS compatibility harness — first differential run findings

**Date:** 2026-07-15
**Reference:** `redis:8.6@sha256:a051e4f48a5d0ceda6554974f3ad0f5369f4479197f36829332c1325cecad2b7`
**Subject:** valkey-server `unstable` + `libvalkey_timeseries` (branch `compatibility`)
**Result:** 27 passed / 21 failed of 48 smoke cases (24 scenarios × RESP2/RESP3)

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
| 1 | L1 | `TS.MRANGE`/`TS.MGET` RESP3 | RTS returns a native **map** keyed by series name, with per-series `{aggregators: []}` metadata (MRANGE); we return the RESP2 array shape on both protocols | **Fix** — headline RESP3 shape gap; clients using RESP3 parse maps |
| 2 | L2 | `TS.RANGE ... ALIGN n AGGREGATION ...` | Bucket assignment differs (e.g. `ALIGN 250`, bucket 1000: RTS buckets `[250,1250)` etc. with first bucket clamped to range start; ours groups differently) | **Fix** — semantic aggregation bug |
| 3 | L1 | `TS.MADD` per-item errors | RTS embeds real RESP **error** replies inside the array; we return the error text as a **bulk string** | **Fix** — clients type-check array elements |
| 4 | L1 | `TS.INFO` `sourceKey` | RTS always emits `sourceKey` (nil when not a compaction); we omit it entirely for non-compaction series | **Fix** — trivially additive |
| 5 | L1 | `TS.INFO` `labels` | RTS emits an empty **array** for a label-less series; we emit **nil** | **Fix** |
| 6 | L1 | `TS.INFO` `rules` aggregator | RTS reports the aggregator uppercase (`AVG`); we report lowercase (`avg`) | **Fix or register** (`behavior`) |
| 7 | L3 | WRONGTYPE condition | RTS: `TSDB: the key is not a TSDB key`; we: standard `WRONGTYPE ...` | **Decide** — different error *class*, not just text; our form is arguably more idiomatic Valkey. Needs owner call: fix vs `behavior` entry |
| 8 | L3 | Duplicate-sample error text (`TS.ADD`/`TS.MADD`, BLOCK policy) | RTS: long "Error at upsert…" message; we: `TSDB: duplicate sample` | **Register** (`error-text`) or fix; same `TSDB:` prefix, same condition |
| 9 | L3 | Arity error text | RTS lowercases the command name (`'ts.queryindex'`); we echo uppercase (`'TS.QUERYINDEX'`) | **Register** (`error-text`) or normalize command-name casing |
| 10 | L1 | `TS.INFO` extra fields | We emit `metric`, `encoding` (+ `rounding` when set) beyond the RTS set | Already registered as **DIV-0001** (`reply-superset`) |

Not yet exercised by the smoke subset (Phase 1/2 scope): config parity (§7.1 — the
`ts-chunk-size` vs `ts-chunk-size-bytes` naming suspects), keyspace notifications,
COMMAND INFO metadata, persistence interop (§7.4), compaction deep-dive, fuzzing.

## Gate status

The `compat-smoke` CI job runs non-blocking (`continue-on-error`) until the findings
above are triaged and the suite is green with a reviewed registry; flipping it to
blocking is the Phase 1 exit action (plan §8).
