# RedisTimeSeries compatibility harness

Differential tests that verify valkey-timeseries behaves like RedisTimeSeries 8.6
on the shared command surface. Implements Tier A of
[docs/rts-compatibility-test-plan.md](../../docs/rts-compatibility-test-plan.md);
the compatibility contract itself is described in
[COMPATIBILITY.md](../../COMPATIBILITY.md).

## ⚠️ Clean-room rule (read first)

The RedisTimeSeries source repository — including its test suite — is licensed
under RSALv2/SSPLv1/AGPLv3, which is incompatible with this repository's
Apache-2.0 license.

**Contributors must not consult RedisTimeSeries source or test code when writing
compatibility tests.** Everything in this directory must be derived from:

- public command documentation, and
- black-box observation of the reference server's behavior.

Nothing from the RedisTimeSeries repository may be vendored, copied, ported, or
fetched at CI time. Running the official `redis:8.6` binary image as a black-box
test target is fine; its source and tests are off-limits.

## How it works

Each test gets a `diff` fixture: a `DiffClient` that sends every command to
**both** engines — the *subject* (valkey-server + this module) and the
*reference* (pinned `redis:8.6` Docker image) — normalizes both replies
(`compat_normalize.py`, plan §5.1), and asserts equality. Tests are just command
sequences; they are automatically parametrized over RESP2 and RESP3.

On mismatch, the failure report contains the full command history (a
reproducer), both raw replies, and the normalized deltas.

Mismatches covered by the known-divergence registry
([`divergences.yml`](divergences.yml), plan §5.3) are recorded as
**XFAIL-DIVERGENT** in the conformance report (`test-data/compat-report.json`
plus a terminal summary) instead of failing. A "reference errors, subject
succeeds" mismatch always fails and can not be registered away (plan §5.2).

## Running locally

The suite is skipped unless a reference server is available:

```sh
# Option 1: let the harness manage the pinned reference container (needs Docker)
RTS_COMPAT=1 python3 -m pytest tests/compat -v

# Option 2: point at an already-running reference server
docker compose -f docker-compose.compat.yml up -d reference
COMPAT_REFERENCE_URL=redis://127.0.0.1:16379 python3 -m pytest tests/compat -v
```

The subject server is launched automatically from the same binary/module
discovery the rest of the integration suite uses (`tests/common.py`): build the
module (`cargo build --release`) and the valkey-server binary (`tests/run.sh`)
first, or point `COMPAT_SUBJECT_URL` at a running instance.

Useful environment variables:

| Variable | Effect |
|---|---|
| `RTS_COMPAT=1` | allow the harness to start/stop the reference container |
| `COMPAT_REFERENCE_URL` | use an existing reference server (skips Docker) |
| `COMPAT_SUBJECT_URL` | use an existing subject server (skips local launch) |
| `COMPAT_REFERENCE_PORT` | host port for the compose reference service (default 16379) |
| `COMPAT_KEEP_REFERENCE=1` | leave the reference container running after the session |
| `COMPAT_REPORT_PATH` | where the JSON conformance report is written |

Plumbing self-check without Docker: point the "reference" at a second instance
of the subject (`COMPAT_SUBJECT_URL`/`COMPAT_REFERENCE_URL` at two local
valkey-timeseries servers) — every diff should pass trivially.

## Registering a divergence

If a test fails on a mismatch that is an *intentional* divergence (see
COMPATIBILITY.md for what qualifies), add an entry to `divergences.yml` with an
id, the command, a kind, a description, and a reviewed rationale.
`behavior`-kind entries need explicit sign-off in the PR that introduces them.
Entries that stop firing should be removed — a stale entry hides regressions.

## Layout

| File | Purpose |
|---|---|
| `conftest.py` | server fixtures, RESP2/RESP3 parametrization, conformance report |
| `compat_diff.py` | `DiffClient` + error-comparison policy (plan §4.1, §5.2) |
| `compat_normalize.py` | reply normalization and structural diffing (plan §5.1) |
| `compat_registry.py` | known-divergence registry loader (plan §5.3) |
| `divergences.yml` | the registry — single source of truth for intentional divergences |
| `info-fields-8.6.yml` | frozen RTS 8.6 `TS.INFO` field baseline (plan §5.1 rule 3) |
| `test_compat_smoke.py` | fixed smoke subset — the CI PR gate (plan §8) |

Planned (later phases): per-command matrix modules (`test_compat_range.py`, …,
plan §6), operational parity (§7), the Hypothesis differential fuzzer and its
regression corpus under `corpus/` (§4.3), and client-library conformance (§4.2).
