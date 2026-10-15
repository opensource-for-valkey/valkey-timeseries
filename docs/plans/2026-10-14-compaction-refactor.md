# Compaction Refactor Implementation Plan (Approach 1)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make the downsampling (compaction) subsystem simpler and its replication verifiably correct: all compaction state lives on the rule, replicas recompute exactly as the reference does, there is one ingest path through the engine, and every input that could differ between nodes is pinned.

**Architecture:** Part 0 adds observability (`TS.INFO … DEBUG rulesState`) and a primary/replica equivalence suite that is green against today's code and stays the acceptance test for everything after it. Part 1 refactors: it drops the strict-mode DIV-0023 emulation (and with it `last_forward_close`, the only compaction state on a destination), splits `src/series/compaction.rs` into a module directory, and routes `TS.ADD`/`TS.INCRBY` through the batch path so the single-sample ops can be deleted. Part 2 pins the remaining node-local inputs — auto-create defaults and `ts-compaction-policy` destinations are replicated from the primary — and adds a persisted `dirty` flag that makes a rule whose last application failed rebuild its open bucket from the source. Part 3 records the outcome in the docs and proposals.

**Tech Stack:** Rust (valkey-module 0.1.14, edition 2024, MSRV 1.96, `proptest`, `criterion`); Python pytest + the cloned `valkeytestframework` (+ Hypothesis) driven by `./build.sh`; the differential compat harness against the pinned `redis:8.10` image.

**Spec:** the *Design* section below (agreed in conversation on 2026-10-14) and the analysis it rests on, `docs/plans/compaction-replication-plan.md` (§3 divergence classes, §3.6 our probe, §3.7 the reference's behavior). This plan supersedes `docs/plans/2026-10-14-compaction-effects-replication.md` (Task 14 marks it so).

## Design

**Decisions (owner, 2026-10-14):**
- *Replication model:* recompute on every node — the reference's model (analysis §3.7). No effects replication, no `TS._COMPACT`.
- *DIV-0023:* `ts-compatibility-mode strict` stops reproducing RedisTimeSeries's cached destination last-sample. `TS.GET`/`TS.MGET`/`TS.INFO lastTimestamp` report the destination's last stored sample in both modes. DIV-0023 becomes an ungated, documented divergence like DIV-0016/0021. This is a `behavior`-kind divergence change; the owner's sign-off is recorded here and must be repeated in the PR description.
- *Encoding version:* no release has shipped; the one new persisted field (`dirty`) changes the payload in place and `TIMESERIES_TYPE_ENCODING_VERSION` stays `1`.

**State model after the refactor.** A `CompactionRule` on the source owns everything: definition (`dest`, `aggregator` kind, `bucket_duration`, `align_timestamp`), open-bucket state (`bucket_start`, `has_samples`, the accumulator inside `aggregator`), and `dirty`. A destination is an ordinary series plus its `src_series` back-link; it carries no compaction state. All of it is in the RDB payload, so a full sync, a restart, `DUMP`/`RESTORE` and `TS._RESTORE` carry it.

**Why the open bucket stays persisted rather than derived.** It is not a pure function of the source: `rate`/`increase` stream deltas across samples, and with retention shorter than a bucket the streamed accumulator includes samples a clamped re-read excludes. So a rebuild from the source is an approximation, used only on the exceptional `dirty` path.

**Engine after the refactor.** Two operations: `CompactionOp::Ingest { samples, prev_last, input_order }` and `CompactionOp::RemoveRange { start, end }`. Every write command submits `Ingest`; `TS.ADD`/`TS.INCRBY` submit a one-item batch via `TimeSeries::compact_sample`. Rules run in parallel only for multi-sample ingests and removals. The batch-of-one switch is gated by a criterion benchmark (≤ +10 % per in-order append) and by a differential property test against today's single-sample path; one known semantic gap (a rule created on a non-empty source whose first write is a back-fill) is settled by the reference before the switch.

**Module layout.** `src/series/compaction/{mod.rs, engine.rs, cascade.rs, links.rs}`: rule type, persistence and the `TimeSeries` entry points; per-destination bucket arithmetic (Context-free, unit-testable); multi-destination traversal, destination resolution and notifications; `LATEST`, rename relinking and rule-cycle checks.

**Failure recovery.** A rule application that errors sets `dirty`; the next `Ingest` rebuilds the open bucket from the source as of the series' pre-write last timestamp before applying, and a `RemoveRange` already rebuilds it. `INFO ts_compaction` exposes `ts_compaction_failures` and `ts_compaction_state_rebuilds`. A closed bucket lost to the failure is not repaired; the failure counter is how an operator learns of it.

**Version skew.** Recompute means a replica applying the stream with different aggregator arithmetic than its primary computes different destination values. Documented as an operational rule (Task 14), not coded.

## Global Constraints

- Clean-room rule: never consult, fetch, or reproduce RedisTimeSeries source or tests; derive behavior from public docs and black-box observation of the pinned `redis:8.10` image only (`AGENTS.md`, `tests/compat/README.md`).
- Lint/build gate, run before every commit that touches Rust: `cargo fmt --check && cargo clippy --profile release --all-targets -- -D clippy::all && RUSTFLAGS="-D warnings" cargo build --all --all-targets --release`.
- Rust unit tests: `cargo test --features enable-system-alloc` (the feature is mandatory outside a live server, or the binary aborts at startup).
- Benchmarks: `cargo bench --bench <name> --features enable-system-alloc`.
- Integration tests: `TEST_PATTERN="<pytest -k expression>" SERVER_VERSION=unstable ./build.sh`.
- Compat gate for anything that can change a reply on the shared RTS surface: `RTS_COMPAT=1 TEST_PATTERN="<expr>" SERVER_VERSION=unstable ./build.sh` (needs Docker; first check `curl -s --max-time 5 --unix-socket /var/run/docker.sock http://localhost/_ping` prints `OK` — a stalled Docker Desktop resume otherwise hangs the run).
- Encoding version: the payload changes in place; `TIMESERIES_TYPE_ENCODING_VERSION`, `MIN_TIMESERIES_TYPE_ENCODING_VERSION` and `tests/test_ts_restore_malformed.py::CURRENT_ENCVER` stay `1`. Delete local `dump.rdb`/`appendonlydir` files from builds before Task 13 when switching builds.
- Commit messages: Conventional Commits `type(scope): summary`; no `Co-Authored-By` trailer (owner preference).
- Threading rules R1–R5 (`src/common/threads/mod.rs`): compaction runs on the command thread; never call `Context` methods from inside a `par_mut_rayon` closure.
- Every task ends with its tests green and a commit; a task that can change an RTS-visible reply also runs the compat gate.

## Review Focus

Inputs the design implies but no existing test exercises, most likely to bite first. Each is pinned by a test in the task named.

1. **A rule created on a source that already holds samples, whose first write is a back-fill.** Today `TS.ADD` opens the bucket from that one sample while a one-item `TS.MADD` recalculates it from the source, so the two commands already disagree. Expected: whatever the reference does, for both commands. → Task 8 (compat tests decide), Task 9.
2. **`TS.ADD` into a source with two or more rules.** Expected: still applied sequentially — a one-item batch must not take the parallel branch and pay a thread handoff per write. → Task 9 (`runs_rules_in_parallel` unit test).
3. **`TS.INCRBY … TIMESTAMP <last>` (in-place update of the last sample).** Expected: the bucket is recalculated, not streamed twice (`avg` of `0` then `+1` at the same timestamp is `1`, not `0.5`). → Task 9.
4. **A sample suppressed by `IGNORE` thresholds.** Expected: no compaction at all for that write. → Task 9.
5. **A rule marked `dirty`, then the server restarts before the next write.** Expected: `dirty` survives the reload and the first write after it rebuilds. → Task 13.

---

## Part 0 — observe, pin, test (no behavior change)

### Task 1: Expose rule state in `TS.INFO … DEBUG`

**Files:**
- Modify: `src/commands/ts_info.rs` (`INFO_FIELD_ORDER` ~line 72; `get_ts_info` debug branch ~line 215)
- Test: `tests/test_compactions_add.py` (class `TestCompactionAdd`)

**Interfaces:**
- Produces: `TS.INFO key DEBUG` gains `rulesState` — an array with one `[destKey, bucketStart|nil, hasSamples (0/1), current|nil]` entry per rule in rule order. Task 13 appends `dirty (0/1)` as a fifth element. Every replication test from Task 2 on reads it.

- [ ] **Step 1: Write the failing test**

Add to `TestCompactionAdd` in `tests/test_compactions_add.py`:

```python
    @staticmethod
    def info_debug(client, key):
        """`TS.INFO key DEBUG` as a dict (the RESP2 flat list, pairwise)."""
        flat = client.execute_command("TS.INFO", key, "DEBUG")
        return {flat[i]: flat[i + 1] for i in range(0, len(flat), 2)}

    def test_info_debug_reports_rule_state(self):
        """The open-bucket state a rule carries is otherwise invisible: a wrong accumulator only
        shows downstream, one bucket later. DEBUG exposes it so tests can compare nodes."""
        src, dst = "state:src", "state:dst"
        self.create_source_and_dest_series(src, dst)
        self.add_compaction_rule(src, dst, "sum", 1000)

        assert self.info_debug(self.client, src)[b"rulesState"] == [[dst.encode(), None, 0, None]]
        assert self.info_debug(self.client, dst)[b"rulesState"] == []

        self.add_sample(src, 100, 1.5)
        self.add_sample(src, 200, 2.0)
        assert self.info_debug(self.client, src)[b"rulesState"] == [[dst.encode(), 0, 1, b"3.5"]]

        self.add_sample(src, 1000, 7.0)  # closes bucket 0
        assert self.info_debug(self.client, src)[b"rulesState"] == [[dst.encode(), 1000, 1, b"7"]]
```

- [ ] **Step 2: Run it to verify it fails**

Run: `TEST_PATTERN="test_info_debug_reports_rule_state" SERVER_VERSION=unstable ./build.sh`
Expected: FAIL with `KeyError: b'rulesState'`.

- [ ] **Step 3: Add the DEBUG field**

In `src/commands/ts_info.rs`, add the import and extend the order list:

```rust
use crate::aggregators::AggregationHandler;
```

```rust
    "rounding",
    "keySelfName",
    "rulesState",
    "Chunks",
];
```

Add the builder next to `reply_with_chunks_info`:

```rust
/// Per-rule aggregation state for `TS.INFO … DEBUG`: one `[destKey, bucketStart, hasSamples,
/// current]` entry per rule, in rule order. `bucketStart` and `current` are nil while the rule
/// has no open bucket. This is the state a primary and its replica must agree on, and it is
/// otherwise invisible — a wrong open bucket only shows up downstream, one bucket later.
fn get_rules_state(ts: &TimeSeries) -> ValkeyValue {
    let entries = ts
        .rules
        .iter()
        .map(|rule| {
            let current = if rule.has_samples {
                AggregationHandler::current(&rule.aggregator)
                    .map_or(ValkeyValue::Null, ValkeyValue::Float)
            } else {
                ValkeyValue::Null
            };
            ValkeyValue::Array(vec![
                ValkeyValue::BulkString(String::from_utf8_lossy(rule.dest.key()).into_owned()),
                rule.bucket_start
                    .map_or(ValkeyValue::Null, ValkeyValue::Integer),
                ValkeyValue::Integer(i64::from(rule.has_samples)),
                current,
            ])
        })
        .collect();
    ValkeyValue::Array(entries)
}
```

In `get_ts_info`, replace the `if debug { … }` block:

```rust
    if debug {
        map.insert("keySelfName".into(), ValkeyValue::from(key));
        map.insert("rulesState".into(), get_rules_state(ts));
        // yes, I know its title case, but that's what redis does. Written by
        // `reply_with_chunks_info`; only the key is needed here, for its place in the reply.
        map.insert("Chunks".into(), ValkeyValue::Null);
    }
```

- [ ] **Step 4: Run the test and the TS.INFO suites**

Run: `TEST_PATTERN="test_info_debug_reports_rule_state or test_ts_info" SERVER_VERSION=unstable ./build.sh`
Expected: PASS.

- [ ] **Step 5: Compat check for the new DEBUG field**

Run: `RTS_COMPAT=1 TEST_PATTERN="info" SERVER_VERSION=unstable ./build.sh`
Expected: PASS, or a shape delta on `TS.INFO … DEBUG` naming `rulesState`. In the latter case add an entry to `tests/compat/divergences.yml` of the same kind as the one that registers the module-only `TS.INFO` fields `encoding` and `metric` (find it with `grep -n -B3 -A8 "metric" tests/compat/divergences.yml`), with `command: TS.INFO`, a description naming the DEBUG-only field, and `since: 0.1.0`; re-run until green.

- [ ] **Step 6: Lint and commit**

Run the lint gate, then:
```bash
git add src/commands/ts_info.rs tests/test_compactions_add.py tests/compat/divergences.yml
git commit -m "feat(info): expose compaction rule state in TS.INFO DEBUG"
```

### Task 2: Replica-oracle equivalence suite

**Files:**
- Test: `tests/test_ts_replication.py` (class `TestTimeSeriesReplication`)

**Interfaces:**
- Produces helpers used by every later replication test: `info_debug(client, key)`, `set_compat_mode(mode)`, `rule(src, dst, agg, bucket, align=None, src_options=())`, `assert_compaction_equivalent(*keys)`, `assert_rule_state_equivalent(*keys)`, `force_full_resync()`, `wait_link_up(client)`.

- [ ] **Step 1: Add the helpers**

Below `assert_series_matches_replica` in `tests/test_ts_replication.py`:

```python
    # --- compaction equivalence helpers (docs/plans/2026-10-14-compaction-refactor.md)

    @staticmethod
    def info_debug(client, key):
        """`TS.INFO key DEBUG` as a dict (the RESP2 flat list, pairwise)."""
        flat = client.execute_command("TS.INFO", key, "DEBUG")
        return {flat[i]: flat[i + 1] for i in range(0, len(flat), 2)}

    def set_compat_mode(self, mode):
        for client in (self.client, self.replicas[0].client):
            client.execute_command("CONFIG", "SET", "ts.ts-compatibility-mode", mode)

    def rule(self, src, dst, agg="sum", bucket=1000, align=None, src_options=()):
        """Create `src` (with `src_options`), `dst`, and a rule between them on the primary."""
        self.client.execute_command("TS.CREATE", src, *src_options)
        self.client.execute_command("TS.CREATE", dst)
        args = ["TS.CREATERULE", src, dst, "AGGREGATION", agg, bucket]
        if align is not None:
            args.append(align)
        assert self.client.execute_command(*args) == b"OK"

    def assert_compaction_equivalent(self, *keys):
        """The replica holds the same stored data as the primary for every key: digest, samples,
        TS.GET, and the open bucket as TS.GET … LATEST reports it."""
        self.wait_for_replication()
        replica = self.replicas[0].client
        for key in keys:
            for cmd in (
                ("DEBUG", "DIGEST-VALUE", key),
                ("TS.RANGE", key, "-", "+"),
                ("TS.GET", key),
                ("TS.GET", key, "LATEST"),
            ):
                assert replica.execute_command(*cmd) == self.client.execute_command(*cmd), (key, cmd)

    def assert_rule_state_equivalent(self, *keys):
        """The replica's open-bucket state (TS.INFO DEBUG rulesState) matches the primary's.
        Replicas recompute, so this always holds; a mismatch is a determinism bug in the engine."""
        replica = self.replicas[0].client
        for key in keys:
            assert self.info_debug(replica, key)[b"rulesState"] == \
                self.info_debug(self.client, key)[b"rulesState"], key

    @staticmethod
    def wait_link_up(client, timeout=10):
        deadline = time.time() + timeout
        while time.time() < deadline:
            info = client.info("replication")
            if info.get("master_link_status") == "up" and not info.get("master_sync_in_progress", 0):
                return
            time.sleep(0.1)
        raise AssertionError("replica link did not come up")

    def force_full_resync(self):
        """Detach the replica, wipe it, re-attach it: with a new replication id the next link-up
        is a full sync of the primary's current snapshot, never a partial one."""
        replica = self.replicas[0].client
        replica.execute_command("REPLICAOF", "NO", "ONE")
        replica.execute_command("FLUSHALL")
        replica.execute_command("REPLICAOF", self.server.bind_ip, self.server.port)
        self.wait_link_up(replica)
        self.wait_for_replication()
```

- [ ] **Step 2: Add the scenarios**

```python
    def test_replication_compaction_add_paths(self):
        """TS.ADD-fed rule: append, same-timestamp upsert, back-fill into a closed bucket, and a
        back-fill two buckets back leave identical destination data and open-bucket state."""
        src, dst = "ts:comp_add:src", "ts:comp_add:dst"
        self.rule(src, dst, "avg", 1000, src_options=("DUPLICATE_POLICY", "LAST"))
        for ts, value in ((0, 1), (500, 2), (1000, 3), (1500, 4), (2500, 5)):
            self.client.execute_command("TS.ADD", src, ts, value)
        self.assert_compaction_equivalent(src, dst)
        self.assert_rule_state_equivalent(src)
        self.client.execute_command("TS.ADD", src, 2500, 50)   # upsert in the open bucket
        self.client.execute_command("TS.ADD", src, 1200, 60)   # back-fill the closed bucket 1000
        self.client.execute_command("TS.ADD", src, 10, 70)     # back-fill two buckets back
        self.assert_compaction_equivalent(src, dst)
        self.assert_rule_state_equivalent(src)

    def test_replication_compaction_madd(self):
        """TS.MADD: out-of-order items, in-batch duplicates under each duplicate policy, and a
        BLOCK-rejected duplicate (which the primary leaves out of the propagated TS.MADD)."""
        pairs = []
        for policy in ("BLOCK", "SUM", "LAST", "MIN"):
            src, dst = f"ts:comp_madd:{policy}:src", f"ts:comp_madd:{policy}:dst"
            self.rule(src, dst, "sum", 1000, src_options=("DUPLICATE_POLICY", policy))
            pairs.append((src, dst))
        for src, _ in pairs:
            self.client.execute_command("TS.MADD", src, 0, 1, src, 2000, 2, src, 1000, 3)
            self.client.execute_command("TS.MADD", src, 500, 4, src, 500, 5, src, 2500, 6)
            self.client.execute_command("TS.MADD", src, 2500, 7, src, 3100, 8)
        keys = [key for pair in pairs for key in pair]
        self.assert_compaction_equivalent(*keys)
        self.assert_rule_state_equivalent(*[src for src, _ in pairs])

    def test_replication_compaction_del(self):
        """TS.DEL: a partial bucket, fully covered middle buckets, and a trailing removal that
        re-opens an already published bucket."""
        src, dst = "ts:comp_del:src", "ts:comp_del:dst"
        self.rule(src, dst, "sum", 1000)
        for ts in range(0, 6000, 100):
            self.client.execute_command("TS.ADD", src, ts, 1)
        self.assert_compaction_equivalent(src, dst)
        assert self.client.execute_command("TS.DEL", src, 1200, 1400) == 3      # partial bucket
        self.assert_compaction_equivalent(src, dst)
        assert self.client.execute_command("TS.DEL", src, 1900, 4100) == 23     # boundaries + middle
        self.assert_compaction_equivalent(src, dst)
        assert self.client.execute_command("TS.DEL", src, 4500, 6000) == 15     # trailing: re-opens 4000
        self.assert_compaction_equivalent(src, dst)
        self.assert_rule_state_equivalent(src)
        self.client.execute_command("TS.ADD", src, 7000, 9)                    # closes it again
        self.assert_compaction_equivalent(src, dst)

    def test_replication_compaction_chain_and_diamond(self):
        """A three-level chain, and a source with two rules."""
        self.rule("ts:chain:src", "ts:chain:mid", "sum", 1000)
        self.client.execute_command("TS.CREATE", "ts:chain:top")
        self.client.execute_command("TS.CREATERULE", "ts:chain:mid", "ts:chain:top", "AGGREGATION", "avg", 2000)
        self.client.execute_command("TS.CREATE", "ts:chain:max")
        self.client.execute_command("TS.CREATERULE", "ts:chain:src", "ts:chain:max", "AGGREGATION", "max", 500)
        for ts in range(0, 9000, 250):
            self.client.execute_command("TS.ADD", "ts:chain:src", ts, ts % 7)
        self.client.execute_command("TS.ADD", "ts:chain:src", 1250, 100)   # back-fill cascades mid -> top
        self.client.execute_command("TS.DEL", "ts:chain:src", 3000, 4999)  # removal cascades
        keys = ("ts:chain:src", "ts:chain:mid", "ts:chain:top", "ts:chain:max")
        self.assert_compaction_equivalent(*keys)
        self.assert_rule_state_equivalent("ts:chain:src", "ts:chain:mid")

    def test_replication_compaction_createrule_on_existing_data(self):
        src, dst = "ts:late_rule:src", "ts:late_rule:dst"
        self.client.execute_command("TS.CREATE", src)
        for ts in range(0, 3500, 500):
            self.client.execute_command("TS.ADD", src, ts, 1)
        self.client.execute_command("TS.CREATE", dst)
        self.client.execute_command("TS.CREATERULE", src, dst, "AGGREGATION", "sum", 1000)
        self.client.execute_command("TS.ADD", src, 3500, 1)
        self.client.execute_command("TS.ADD", src, 4000, 1)
        self.assert_compaction_equivalent(src, dst)
        self.assert_rule_state_equivalent(src)

    def test_replication_compaction_deleterule_mid_bucket_then_recreate(self):
        src, dst = "ts:rerule:src", "ts:rerule:dst"
        self.rule(src, dst, "sum", 1000)
        for ts in (0, 500, 1000, 1200):
            self.client.execute_command("TS.ADD", src, ts, 1)
        assert self.client.execute_command("TS.DELETERULE", src, dst) == b"OK"
        self.client.execute_command("TS.ADD", src, 1500, 1)
        self.client.execute_command("TS.CREATERULE", src, dst, "AGGREGATION", "sum", 1000)
        self.client.execute_command("TS.ADD", src, 2000, 1)
        self.client.execute_command("TS.ADD", src, 3000, 1)
        self.assert_compaction_equivalent(src, dst)
        self.assert_rule_state_equivalent(src)

    def test_replication_compaction_direct_dest_writes(self):
        """A client may write straight to a destination; a source TS.DEL covering that bucket
        recalculates it from the source and erases the direct write (DIV-0021 model)."""
        src, dst = "ts:direct:src", "ts:direct:dst"
        self.rule(src, dst, "sum", 1000)
        for ts in (0, 500, 1000, 1500, 2000):
            self.client.execute_command("TS.ADD", src, ts, 1)
        self.client.execute_command("TS.ADD", dst, 0, 99)      # direct destination write
        self.client.execute_command("TS.ADD", dst, 5000, 42)   # direct write ahead of the source
        self.assert_compaction_equivalent(src, dst)
        self.client.execute_command("TS.DEL", src, 0, 999)
        self.assert_compaction_equivalent(src, dst)

    def test_replication_compaction_source_retention(self):
        """Retention on the source sets the recalculation floor; both nodes must use the same."""
        src, dst = "ts:ret_rule:src", "ts:ret_rule:dst"
        self.rule(src, dst, "sum", 1000, src_options=("RETENTION", 2500))
        for ts in range(0, 6000, 250):
            self.client.execute_command("TS.ADD", src, ts, 1)
        self.client.execute_command("TS.ADD", src, 3300, 10)    # back-fill at the window's edge
        self.assert_compaction_equivalent(src, dst)
        self.assert_rule_state_equivalent(src)

    def test_replication_compaction_policy_rules(self):
        """Rules created by ts-compaction-policy, with the same policy on both nodes."""
        for client in (self.client, self.replicas[0].client):
            client.execute_command("CONFIG", "SET", "ts.ts-compaction-policy", "max:1m:1h;avg:2m:1h")
        self.client.execute_command("TS.CREATE", "ts:policy:src", "LABELS", "a", "b")
        for ts in range(10_000, 300_000, 10_000):
            self.client.execute_command("TS.ADD", "ts:policy:src", ts, ts)
        rules = parse_info_response(self.client.execute_command("TS.INFO", "ts:policy:src"))["rules"]
        dests = [r.dest_key for r in rules]
        assert len(dests) == 2
        self.assert_compaction_equivalent("ts:policy:src", *dests)
        self.assert_rule_state_equivalent("ts:policy:src")

    def test_replication_compaction_full_resync_mid_bucket(self):
        """A full sync taken mid-bucket ships the open-bucket state in the RDB; the stream after
        it must continue from exactly that state."""
        src, dst = "ts:resync:src", "ts:resync:dst"
        self.rule(src, dst, "avg", 1000)
        for ts, value in ((0, 1), (300, 2.5), (450, 7), (1000, 9), (1200, 3)):
            self.client.execute_command("TS.ADD", src, ts, value)
        self.force_full_resync()
        self.assert_compaction_equivalent(src, dst)
        self.assert_rule_state_equivalent(src)
        self.client.execute_command("TS.ADD", src, 1700, 5)   # same open bucket as the snapshot
        self.client.execute_command("TS.ADD", src, 2000, 5)   # closes it
        self.client.execute_command("TS.ADD", src, 200, 100)  # back-fill
        self.client.execute_command("TS.DEL", src, 1100, 2100)
        self.assert_compaction_equivalent(src, dst)
        self.assert_rule_state_equivalent(src)
```

- [ ] **Step 3: Run the suite**

Run: `TEST_PATTERN="test_replication_compaction_" SERVER_VERSION=unstable ./build.sh`
Expected: all 10 PASS. The stored-data model holds today (§3.6 of the spec), so a failure here is a divergence the analysis did not predict: record it in `docs/plans/compaction-replication-plan.md` §3 with the failing assertion, mark the test `@pytest.mark.xfail(strict=True, reason="<spec §3 item>")`, and do not weaken the assertion.

- [ ] **Step 4: Commit**

```bash
git add tests/test_ts_replication.py
git commit -m "test(replication): compaction equivalence suite against a live replica"
```

### Task 3: Reload and promotion variants

**Files:**
- Test: `tests/test_ts_replication.py`

**Interfaces:**
- Produces: `promote_replica()` → `(new_primary_client, new_replica_client)`, `wait_caught_up(primary, replica)`, `assert_equivalent_between(a, b, *keys)`.

- [ ] **Step 1: Add the helpers and tests**

```python
    @staticmethod
    def wait_caught_up(primary, replica, timeout=10):
        """`replica` has applied everything `primary` has propagated (offset comparison; the
        framework's waiter only knows the original primary/replica pair)."""
        deadline = time.time() + timeout
        while time.time() < deadline:
            target = primary.info("replication")["master_repl_offset"]
            if replica.info("replication")["master_repl_offset"] >= target:
                return
            time.sleep(0.05)
        raise AssertionError("replica did not catch up")

    def promote_replica(self):
        """Swap roles: the replica becomes primary and the old primary follows it. Returns
        (new_primary_client, new_replica_client). The helpers above keep pointing at
        `self.client`/`self.replicas[0]`, so compare explicitly after this."""
        replica = self.replicas[0]
        replica.client.execute_command("REPLICAOF", "NO", "ONE")
        self.client.execute_command("REPLICAOF", replica.bind_ip, replica.port)
        self.wait_link_up(self.client)
        return replica.client, self.client

    def assert_equivalent_between(self, a, b, *keys):
        for key in keys:
            for cmd in (("DEBUG", "DIGEST-VALUE", key), ("TS.RANGE", key, "-", "+"), ("TS.GET", key, "LATEST")):
                assert a.execute_command(*cmd) == b.execute_command(*cmd), (key, cmd)

    def test_replication_compaction_reload_mid_sequence(self):
        """DEBUG RELOAD on the primary mid-bucket: its rule state round-trips through its own RDB
        while the replica keeps its in-memory copy; the two must still agree afterwards."""
        src, dst = "ts:reload:src", "ts:reload:dst"
        self.rule(src, dst, "std.s", 1000)
        for ts, value in ((0, 1), (300, 2.5), (450, 7)):
            self.client.execute_command("TS.ADD", src, ts, value)
        self.client.execute_command("DEBUG", "RELOAD")
        self.assert_compaction_equivalent(src, dst)
        self.assert_rule_state_equivalent(src)
        for ts, value in ((800, 2), (1000, 9), (100, 4)):
            self.client.execute_command("TS.ADD", src, ts, value)
        self.assert_compaction_equivalent(src, dst)
        self.assert_rule_state_equivalent(src)

    def test_replication_compaction_promotion(self):
        """Promote the replica mid-bucket, keep writing on it, and check the old primary (now a
        replica) converges. The values are exact: avg(9, 3) = 6 closes bucket 1000; the back-fill
        makes bucket 0 avg(1, 2.5, 7, 100) = 27.625; the delete of 1200 recalculates 1000 to 9."""
        src, dst = "ts:promote:src", "ts:promote:dst"
        self.rule(src, dst, "avg", 1000)
        for ts, value in ((0, 1), (300, 2.5), (450, 7), (1000, 9)):
            self.client.execute_command("TS.ADD", src, ts, value)
        self.assert_compaction_equivalent(src, dst)

        new_primary, new_replica = self.promote_replica()
        for ts, value in ((1200, 3), (2000, 5), (100, 100)):   # same open bucket, close, back-fill
            new_primary.execute_command("TS.ADD", src, ts, value)
        new_primary.execute_command("TS.DEL", src, 1100, 1300)
        assert new_primary.execute_command("TS.RANGE", dst, "-", "+") == [[0, b"27.625"], [1000, b"9"]]
        self.wait_caught_up(new_primary, new_replica)
        self.assert_equivalent_between(new_primary, new_replica, src, dst)
```

- [ ] **Step 2: Run**

Run: `TEST_PATTERN="test_replication_compaction_reload_mid_sequence or test_replication_compaction_promotion" SERVER_VERSION=unstable ./build.sh`
Expected: PASS.

- [ ] **Step 3: Commit**

```bash
git add tests/test_ts_replication.py
git commit -m "test(replication): compaction equivalence across reload and promotion"
```

### Task 4: Random-sequence replica oracle (Hypothesis)

**Files:**
- Test: `tests/test_ts_replication.py`

- [ ] **Step 1: Add imports, strategy, and test**

At the top of the file:

```python
import json
import logging
import os
import time
from typing import List

import pytest
from valkey.exceptions import ResponseError

hypothesis = pytest.importorskip("hypothesis")
from hypothesis import HealthCheck, given, settings  # noqa: E402
from hypothesis import strategies as st  # noqa: E402
```

Module level, below the imports:

```python
FUZZ_SRC, FUZZ_MID, FUZZ_TOP = "ts:fuzz:src", "ts:fuzz:mid", "ts:fuzz:top"

_sample = st.tuples(
    st.integers(0, 4000),
    st.floats(-1000, 1000, allow_nan=False, allow_infinity=False),
)
_fuzz_op = st.one_of(
    st.tuples(st.just("ADD"), _sample),
    st.tuples(st.just("MADD"), st.lists(_sample, min_size=1, max_size=5)),
    st.tuples(st.just("ADDBULK"), st.lists(_sample, min_size=1, max_size=8, unique_by=lambda s: s[0])),
    st.tuples(st.just("DEL"), st.integers(0, 4000), st.integers(0, 4000)),
)
```

In the class:

```python
    def apply_fuzz_op(self, op):
        kind = op[0]
        try:
            if kind == "ADD":
                ts, value = op[1]
                self.client.execute_command("TS.ADD", FUZZ_SRC, ts, value)
            elif kind == "MADD":
                args = [arg for ts, value in op[1] for arg in (FUZZ_SRC, ts, value)]
                self.client.execute_command("TS.MADD", *args)
            elif kind == "ADDBULK":
                payload = json.dumps({"timestamps": [ts for ts, _ in op[1]], "values": [v for _, v in op[1]]})
                self.client.execute_command("TS.ADDBULK", FUZZ_SRC, payload, "ON_DUPLICATE", "LAST")
            else:
                lo, hi = sorted(op[1:])
                self.client.execute_command("TS.DEL", FUZZ_SRC, lo, hi)
        except ResponseError:
            pass  # a rejected input is part of the space; the replica must reject it the same way

    @settings(
        max_examples=40,
        deadline=None,
        suppress_health_check=[HealthCheck.function_scoped_fixture, HealthCheck.too_slow],
    )
    @given(ops=st.lists(_fuzz_op, min_size=1, max_size=30))
    def test_replication_compaction_random_sequences(self, ops):
        """Replica oracle: any sequence of writes and deletes through a two-level chain leaves the
        replica with the primary's destinations. A failure is a determinism bug in the engine:
        promote Hypothesis's shrunk example into a named test next to this one."""
        self.client.execute_command("FLUSHALL")
        self.wait_for_replication()
        self.client.execute_command("TS.CREATE", FUZZ_SRC, "DUPLICATE_POLICY", "LAST")
        self.client.execute_command("TS.CREATE", FUZZ_MID)
        self.client.execute_command("TS.CREATE", FUZZ_TOP)
        self.client.execute_command("TS.CREATERULE", FUZZ_SRC, FUZZ_MID, "AGGREGATION", "sum", 1000)
        self.client.execute_command("TS.CREATERULE", FUZZ_MID, FUZZ_TOP, "AGGREGATION", "avg", 2000)
        for op in ops:
            self.apply_fuzz_op(op)
        self.assert_compaction_equivalent(FUZZ_SRC, FUZZ_MID, FUZZ_TOP)
        self.assert_rule_state_equivalent(FUZZ_SRC, FUZZ_MID)
```

- [ ] **Step 2: Run**

Run: `TEST_PATTERN="test_replication_compaction_random_sequences" SERVER_VERSION=unstable ./build.sh`
Expected: PASS (40 examples, a few seconds). On a failure Hypothesis prints `Falsifying example: ops=[…]`; copy that list verbatim into a new test `test_replication_compaction_shrunk_<n>` that calls `apply_fuzz_op` over it, mark it xfail as in Task 2 Step 3, and record it in the spec's §3.

- [ ] **Step 3: Commit**

```bash
git add tests/test_ts_replication.py
git commit -m "test(replication): Hypothesis replica oracle for compaction sequences"
```

### Task 5: Every aggregator's rule state survives reload; fix the stale AOF assertion

**Files:**
- Test: `tests/test_compactions_add.py`
- Modify: `tests/test_ts_aofrewrite.py` (~lines 108–135)

- [ ] **Step 1: Write the parametrized reload test**

```python
    # Every aggregator a rule accepts. The conditional ones take an inline condition.
    RULE_AGGREGATORS = [
        "avg", "sum", "min", "max", "range", "count", "countall", "countnan", "first", "last",
        "std.p", "std.s", "var.p", "var.s", "rate", "irate", "increase",
        "sum<100", "count<100", "all<100", "any<100", "none<100", "share<100",
    ]

    @pytest.mark.parametrize("agg", RULE_AGGREGATORS)
    def test_rule_state_survives_reload(self, agg):
        """Mid-bucket rule state round-trips through RDB for every aggregator: the observable
        state is unchanged by DEBUG RELOAD, and closing the bucket afterwards publishes the same
        value as a twin rule created *after* the reload and fed the whole sequence."""
        c = self.client
        samples = ((0, 1), (300, 150), (450, 7), (700, 2))
        c.execute_command("TS.CREATE", "rl:src")
        c.execute_command("TS.CREATE", "rl:dst")
        c.execute_command("TS.CREATERULE", "rl:src", "rl:dst", "AGGREGATION", agg, 1000)
        for ts, value in samples:
            c.execute_command("TS.ADD", "rl:src", ts, value)
        before = self.info_debug(c, "rl:src")[b"rulesState"]

        c.execute_command("DEBUG", "RELOAD")
        assert self.info_debug(c, "rl:src")[b"rulesState"] == before, agg

        c.execute_command("TS.CREATE", "rl:twin:src")
        c.execute_command("TS.CREATE", "rl:twin:dst")
        c.execute_command("TS.CREATERULE", "rl:twin:src", "rl:twin:dst", "AGGREGATION", agg, 1000)
        for ts, value in samples + ((1000, 5),):
            c.execute_command("TS.ADD", "rl:twin:src", ts, value)
        c.execute_command("TS.ADD", "rl:src", 1000, 5)   # closes bucket 0 on the reloaded rule
        assert c.execute_command("TS.RANGE", "rl:dst", "-", "+") == \
            c.execute_command("TS.RANGE", "rl:twin:dst", "-", "+"), agg
```

- [ ] **Step 2: Run**

Run: `TEST_PATTERN="test_rule_state_survives_reload" SERVER_VERSION=unstable ./build.sh`
Expected: 23 PASS. A failure for one aggregator means its `RdbSerializable` impl drops state: fix that impl in `src/aggregators/handlers.rs` or `filtered.rs` (persist every field the accumulator reads), re-run, and note the aggregator in the spec's §3.3.

- [ ] **Step 3: Fix the stale AOF assertion**

In `tests/test_ts_aofrewrite.py`, replace the two comment blocks and capture the source digest as well:

```python
        # Digests for every series, the source included: its rule state (open bucket, accumulator)
        # is part of the RDB payload the rewrite embeds, so it round-trips too.
        original_source_digest = client.execute_command('DEBUG', 'DIGEST-VALUE', 'source_ts')
        original_avg_digest = client.execute_command('DEBUG', 'DIGEST-VALUE', 'avg_1min')
        original_max_digest = client.execute_command('DEBUG', 'DIGEST-VALUE', 'max_5min')
```

and after the restart:

```python
        restored_source_digest = client.execute_command('DEBUG', 'DIGEST-VALUE', 'source_ts')
        restored_avg_digest = client.execute_command('DEBUG', 'DIGEST-VALUE', 'avg_1min')
        restored_max_digest = client.execute_command('DEBUG', 'DIGEST-VALUE', 'max_5min')

        assert restored_source_digest == original_source_digest, "source_ts digest mismatch"
        assert restored_avg_digest == original_avg_digest, "avg_1min digest mismatch"
        assert restored_max_digest == original_max_digest, "max_5min digest mismatch"
```

Delete the comment starting "Note: We don't compare digests for series with compaction rules".

- [ ] **Step 4: Run and commit**

Run: `TEST_PATTERN="test_ts_aofrewrite" SERVER_VERSION=unstable ./build.sh` — Expected: PASS.

```bash
git add tests/test_compactions_add.py tests/test_ts_aofrewrite.py
git commit -m "test(compaction): rule state round-trips through reload for every aggregator"
```

---

---

## Part 1 — refactor

### Task 6: Drop the strict-mode DIV-0023 emulation (`last_forward_close`)

**Files:**
- Modify: `src/series/time_series.rs` (field + doc ~lines 100–113; `reported_last_sample` / `live_forward_close` ~lines 1225–1262; `Default` literal ~line 1335)
- Modify: `src/series/serialization.rs` (~lines 113–116)
- Modify: `src/series/compaction.rs` (`remove_dest_range`, `handle_batch_compaction`, `DestCacheTrace`, `trace_dest_cache_in_input_order`, `finalize_current_bucket`, `add_dest_bucket`, the `CompactionOp::AddBatch` doc)
- Modify: `src/series/sample_merge.rs` (~lines 24–27), `src/series/bulk_add.rs` (~lines 589–592)
- Modify: `src/commands/ts_get.rs:41`, `src/commands/ts_mget.rs:153`, `src/commands/ts_info.rs` (~lines 131–147)
- Modify: `src/series/time_series_tests.rs` (delete `test_forward_close_marker_is_ignored_once_expired`)
- Modify: `tests/compat/test_compat_compaction.py` (`TestOutOfOrderBackfill`)
- Modify: `tests/compat/fuzz_strategies.py` (`_read_op`, module docstring), `tests/compat/README.md` (~line 168)
- Modify: `tests/compat/divergences.yml` (DIV-0023), `COMPATIBILITY.md` (strict section)

**Interfaces:**
- Removes: `TimeSeries::last_forward_close`, `TimeSeries::reported_last_sample`, `TimeSeries::live_forward_close`, `DestCacheTrace`, `trace_dest_cache_in_input_order`, `CompactionContext::remove_dest_range`.
- Changes: `add_dest_bucket(...) -> TsdbResult<()>` (was `TsdbResult<Option<Sample>>`).

- [ ] **Step 1: Rewrite the compat pins first**

In `tests/compat/test_compat_compaction.py`, `TestOutOfOrderBackfill`: delete the five strict-mode tests (`test_strict_mode_reports_the_stale_bucket_like_rts`, `test_strict_mode_honors_madd_argument_order`, `test_strict_mode_does_not_resurrect_a_deleted_marker`, `test_strict_mode_falls_back_to_the_surviving_bucket`, `test_strict_mode_advances_on_the_next_forward_close`) and the helpers only they use (`_backfill_one_madd`, `_delete_then_recreate`, `_backfill_then_delete_then_recreate`). Keep `_backfill`, `test_backfilled_bucket_is_stored_identically` and `test_get_after_backfilling_an_older_bucket`. Add:

```python
    def test_strict_mode_reports_the_stored_bucket_too(self, strict_subject):
        """DIV-0023 is not gated: even in `ts-compatibility-mode strict` we report the last sample
        the destination holds, which RTS's own TS.RANGE agrees with, rather than RTS's cached
        last-sample. Per-engine assertions for the same reason as the sibling test above."""
        diff = strict_subject
        self._backfill(diff.reference)
        self._backfill(diff.subject)

        assert diff.reference.execute_command("TS.GET", "c:bdst")[0] == 0
        assert diff.subject.execute_command("TS.GET", "c:bdst")[0] == 500
        assert (
            diff.reference.execute_command("TS.GET", "c:bdst", "LATEST")
            == diff.subject.execute_command("TS.GET", "c:bdst", "LATEST")
        )
        assert (
            diff.reference.execute_command("TS.RANGE", "c:bdst", "-", "+")
            == diff.subject.execute_command("TS.RANGE", "c:bdst", "-", "+")
        )
```

Update the class docstring's first line to `"""DIV-0023 (ungated): TS.GET on a destination whose *older* bucket was back-filled.`.

- [ ] **Step 2: Run it to verify it fails**

Run: `RTS_COMPAT=1 TEST_PATTERN="TestOutOfOrderBackfill" SERVER_VERSION=unstable ./build.sh`
Expected: `test_strict_mode_reports_the_stored_bucket_too` FAILS (`assert 0 == 500` on the subject — the emulation still reports the stale bucket); the other two PASS.

- [ ] **Step 3: Remove the marker from the series**

`src/series/time_series.rs`: delete the `last_forward_close` field and its doc comment, delete `reported_last_sample` and `live_forward_close` with their doc comments, and remove `last_forward_close: None,` from the `Default` struct literal. `src/series/serialization.rs`: delete the three comment lines and `last_forward_close: None,` in `rdb_load_series`'s struct literal. `src/series/time_series_tests.rs`: delete `test_forward_close_marker_is_ignored_once_expired`.

Callers report the stored last sample:

`src/commands/ts_get.rs`:
```rust
            Ok(series.last_sample)
```

`src/commands/ts_mget.rs`:
```rust
        let sample = if options.latest {
            get_latest_compaction_sample(ctx, series).or(series.last_sample)
        } else {
            series.last_sample
        };
```

`src/commands/ts_info.rs` — replace the comment and `if let Some(last_sample) = ts.reported_last_sample()` with:
```rust
    if let Some(last_sample) = ts.last_sample {
```

- [ ] **Step 4: Remove the marker from the engine**

In `src/series/compaction.rs`:

1. Delete the method `CompactionContext::remove_dest_range` with its doc comment, and replace every call `ctx.remove_dest_range(` with `ctx.dest.remove_range(` (five call sites in `recalculate_bucket`, `handle_compaction_range_removal`, `remove_or_recalculate_bucket` and `resync_open_bucket_after_removal`).
2. `finalize_current_bucket` — the publish becomes:
```rust
    if ctx.has_samples() {
        // Finalize the current bucket
        let aggregated_value = AggregationHandler::finalize(&mut ctx.rule.aggregator);
        let current_bucket_start = ctx.rule.bucket_start.expect(
            "finalize_current_bucket should be called when current bucket start is already set",
        );
        add_dest_bucket(ctx, current_bucket_start, aggregated_value)?;
    }
```
3. `add_dest_bucket`:
```rust
/// Write one aggregated bucket to the destination.
fn add_dest_bucket(ctx: &mut CompactionContext, ts: Timestamp, value: f64) -> TsdbResult<()> {
    let bucket_start = ctx.rule.calc_bucket_start(ts);
    match ctx
        .dest
        .add(bucket_start, value, Some(DuplicatePolicy::KeepLast))
    {
        SampleAddResult::Ok(sample) => {
            ctx.written.push(sample);
            Ok(())
        }
        // A duplicate the destination ignored, or a bucket below its retention window.
        SampleAddResult::Ignored(_) | SampleAddResult::TooOld => Ok(()),
        x => {
            let base_msg = format!("TSDB: failed to add sample @{ts} to destination bucket: {x}",);
            log_warning(base_msg.as_str());
            Err(TsdbError::General(base_msg))
        }
    }
}
```
4. `handle_batch_compaction`: delete the opening block from the comment `// Captured before the streaming below mutates them` through `let cache_trace = trace_dest_cache_in_input_order(…);`, and the closing block from the comment `// Overwrite whatever the sorted append stream recorded` through `ctx.dest.last_forward_close = marker_bucket.and_then(…);`. The function now ends with the bucket-recalculation loop followed by `Ok(())`.
5. Delete `struct DestCacheTrace` and `fn trace_dest_cache_in_input_order` with their doc comments.
6. In the `CompactionOp::AddBatch` doc comment, replace the `input_order` paragraph with:
```rust
    /// `input_order` holds the accepted timestamps in the order the caller supplied them
    /// (MADD argument order), which sorting `samples` discards. Back-fill classification replays
    /// it: an item at or below the running maximum *when it was applied* is a back-fill, which a
    /// comparison against `prev_last` alone cannot tell.
```

Comments that pointed at the marker: in `src/series/sample_merge.rs` replace the `added_order` doc's last sentence with `Batch compaction replays it to classify back-fills (see `CompactionOp::AddBatch`).`; in `src/series/bulk_add.rs` replace `The pre-sort order is kept separately: the DIV-0023 forward-close marker is input-order sensitive (see `last_forward_close_in_input_order`).` with `The pre-sort order is kept separately: back-fill classification replays it (see `CompactionOp::AddBatch`).`

Confirm nothing is left: `grep -rn "forward_close\|reported_last_sample\|DestCacheTrace\|trace_dest_cache" src` prints nothing.

- [ ] **Step 5: Keep the fuzzer off the now-ungated divergence**

The strict-mode soak would otherwise flag every back-filled rollup read without `LATEST`. In `tests/compat/fuzz_strategies.py`, `_read_op`:

```python
    if kind == "GET":
        key = draw(st.sampled_from(keys))
        # DIV-0023 (ungated): without LATEST a back-filled destination reports a different
        # sample on each engine in both modes, so a rollup is only ever read with LATEST,
        # where the engines agree. LATEST is a no-op on a series that is not a compaction.
        opt = ["LATEST"] if key == ROLLUP_KEY or draw(st.booleans()) else []
        return ("TS.GET", key, *opt)
```

```python
    if kind == "MGET":
        opts = ["WITHLABELS"] if draw(st.booleans()) else []
        if ROLLUP_KEY in keys:
            opts.append("LATEST")  # DIV-0023, as for TS.GET above
        return ("TS.MGET", *opts, "FILTER", *draw(_filter()))
```

Add to the module docstring's "never emits" list: `  - `TS.GET`/`TS.MGET` without `LATEST` once a rollup exists (DIV-0023, ungated);`. In `tests/compat/README.md` ~line 168 change `(e.g. DIV-0023)` to `(e.g. DIV-0014)`.

- [ ] **Step 6: Update the divergence record and COMPATIBILITY.md**

`tests/compat/divergences.yml`, DIV-0023 — replace the whole `rationale:` with:

```yaml
  rationale: >
    Not reproduced in either mode (ungated 2026-10-14, owner sign-off). RTS's reply
    contradicts its own TS.RANGE, and reproducing it needs per-destination history that no
    stored data determines — the same sample set written 0,1000,500 and 0,500,1000 stores
    identically but caches differently. The emulation that `strict` carried from 2026-07-18
    (TimeSeries::last_forward_close) was removed with the compaction refactor
    (docs/plans/2026-10-14-compaction-refactor.md). LATEST and the stored downstream data
    agree with RTS. Pinned by TestOutOfOrderBackfill::test_get_after_backfilling_an_older_bucket
    and ::test_strict_mode_reports_the_stored_bucket_too; the fuzzer reads rollups only
    with LATEST (fuzz_strategies._read_op).
```

`COMPATIBILITY.md`, section *What `strict` gates*: delete the DIV-0023 table row and the two paragraphs starting "The back-fill case has the same shape" and "One limitation is specific to this gate". In *Divergences that qualify but are not yet gated*, add a row:

```markdown
| `TS.GET` / `TS.MGET` / `TS.INFO lastTimestamp` (without `LATEST`) on a compaction destination whose *older* bucket was back-filled (DIV-0023) | RedisTimeSeries reports a cached last-sample that back-filling does not refresh, so its reply contradicts its own `TS.RANGE`. Reproducing it needs per-destination write history that no stored data determines. Valkey TimeSeries reports the last sample the destination holds, in both modes; `TS.RANGE` and `TS.GET ... LATEST` agree with RedisTimeSeries. |
```

- [ ] **Step 7: Run everything that reads a last sample**

Run: `cargo test --features enable-system-alloc` — Expected: PASS.
Run: `TEST_PATTERN="compaction or test_ts_get or test_ts_mget or test_ts_info or createrule or test_replication_compaction_" SERVER_VERSION=unstable ./build.sh` — Expected: PASS.
Run: `RTS_COMPAT=1 TEST_PATTERN="compaction or get or mget or info" SERVER_VERSION=unstable ./build.sh` — Expected: PASS, including `test_strict_mode_reports_the_stored_bucket_too`.

- [ ] **Step 8: Lint and commit**

```bash
git add src/series/time_series.rs src/series/serialization.rs src/series/compaction.rs src/series/sample_merge.rs src/series/bulk_add.rs src/series/time_series_tests.rs src/commands/ts_get.rs src/commands/ts_mget.rs src/commands/ts_info.rs tests/compat/test_compat_compaction.py tests/compat/fuzz_strategies.py tests/compat/README.md tests/compat/divergences.yml COMPATIBILITY.md
git commit -m "refactor(compaction)!: drop the strict-mode DIV-0023 emulation

TS.GET/TS.MGET/TS.INFO report a destination's last stored sample in both
compatibility modes. Removes last_forward_close, the only compaction state
held on a destination. Behavior-kind divergence change, owner sign-off
2026-10-14."
```

### Task 7: Split `compaction.rs` into a module directory

Pure move: no behavior change, no signature change visible outside `src/series/compaction/`.

**Files:**
- Move: `src/series/compaction.rs` → `src/series/compaction/mod.rs`
- Create: `src/series/compaction/engine.rs`, `src/series/compaction/cascade.rs`, `src/series/compaction/links.rs`

**Interfaces:**
- Produces: the same public surface through `src/series/mod.rs`'s `pub use compaction::*` — `CompactionRule`, `CompactionOp`, `apply_compaction`, `check_new_rule_circular_dependency` (pub), `get_latest_compaction_sample`, `relink_renamed_series` (pub(crate)).

- [ ] **Step 1: Move the file, keeping history**

```bash
mkdir -p src/series/compaction
git mv src/series/compaction.rs src/series/compaction/mod.rs
cargo build --features enable-system-alloc 2>&1 | tail -2
```
Expected: builds (a file module and a directory module are interchangeable).

- [ ] **Step 2: Move items into the three submodules**

Cut each item (with its doc comment and attributes) from `mod.rs` into the file named; leave everything else in `mod.rs`:

| File | Items |
|---|---|
| `engine.rs` | `struct CompactionContext` and its `impl`; `enum CompactionOp`; `fn null_ts_filter`; `fn apply_op`; `fn handle_batch_compaction`; `fn handle_sample_compaction`; `fn finalize_current_bucket`; `fn handle_compaction_upsert`; `fn recalculate_current_bucket`; `fn recalculate_bucket`; `fn handle_compaction_range_removal`; `fn remove_or_recalculate_bucket`; `fn resync_open_bucket_after_removal`; `fn add_dest_bucket`; `fn calculate_range` |
| `cascade.rs` | `const PARALLEL_THRESHOLD`; `fn apply_compaction`; `struct RuleOutcome`; `fn dedupe_written_samples`; `type PendingCompactionWrite`; `struct Visited` and its `impl`; `fn process_series_with_compaction`; `fn queue_written_outcomes`; `fn apply_rules_on_destinations`; `fn apply_rules_internal`; `struct Destinations` and its `impl`; `enum Resolved`; `fn get_compaction_series`; `fn resolve_destination`; `fn notify_compaction` |
| `links.rs` | `fn get_latest_compaction_sample`; `fn relink_renamed_series`; `fn check_new_rule_circular_dependency`; `fn dependency_reaches` |
| `mod.rs` (stays) | `const TEMP_VEC_LEN`; `struct CompactionRule` with its `GetSize`, inherent and `RdbSerializable` impls; `fn validate_restored_rule_parameters`; the `impl TimeSeries` block; the `tests` module |

At the top of `mod.rs`, after the `use` block:

```rust
//! Compaction (downsampling) rules: a source series aggregates into destination series in fixed
//! time buckets.
//!
//! - this file: the rule type, its persistence, and the `TimeSeries` entry points;
//! - `engine`: one rule against one destination — bucket arithmetic, Context-free;
//! - `cascade`: every rule of a source, then each destination's own rules, with destination
//!   resolution and keyspace notifications;
//! - `links`: `LATEST`, relinking after `RENAME`, and rule-cycle checks.

mod cascade;
mod engine;
mod links;

pub use cascade::apply_compaction;
pub use engine::CompactionOp;
pub use links::check_new_rule_circular_dependency;
pub(crate) use links::{get_latest_compaction_sample, relink_renamed_series};
```

Give each submodule the imports it needs (start from a copy of the original `use` block and let `cargo clippy` flag the unused ones), and use `use super::{CompactionRule, TEMP_VEC_LEN};` / `use super::engine::{…};` across siblings. Every item another submodule references becomes `pub(super)`; `CompactionContext`'s fields `parent`, `dest`, `rule`, `written` become `pub(super)`.

- [ ] **Step 3: Build, lint and test**

Run the lint gate. Expected: clean (fix unused imports and missing `pub(super)` until it is).
Run: `cargo test --features enable-system-alloc` — Expected: PASS, same test count as before the move.
Run: `TEST_PATTERN="compaction or createrule or deleterule or test_replication_compaction_" SERVER_VERSION=unstable ./build.sh` — Expected: PASS.

- [ ] **Step 4: Commit**

```bash
git add src/series/compaction
git commit -m "refactor(compaction): split compaction.rs into engine, cascade and links modules"
```

### Task 8: Characterize the batch-of-one switch (property tests, benchmark baseline, reference decision)

**Files:**
- Modify: `src/series/compaction/engine.rs` (tests module; benchmark hooks)
- Modify: `src/series/compaction/mod.rs` (re-export the hooks)
- Create: `benches/compaction_ingest.rs`
- Modify: `Cargo.toml` (`[[bench]]` entry)
- Modify: `tests/compat/test_compat_compaction.py`

**Interfaces:**
- Produces (cfg `test-utils`, `#[doc(hidden)]`): `pub fn bench_rule(agg: AggregationType, bucket_ms: u64) -> CompactionRule` and `pub fn bench_ingest_one(parent: &TimeSeries, dest: &mut TimeSeries, rule: &mut CompactionRule, sample: Sample, prev_last: Option<Timestamp>)` — the path `TS.ADD` takes for one rule. Task 9 changes its body; the benchmark IDs stay the same so criterion can compare across the switch.
- Produces (tests): `fn ingest_single_path(...)` (today's `TS.ADD` path) and `fn ingest_batch_of_one(...)`; the oracle property test that Task 10 keeps.

- [ ] **Step 1: Ask the reference about the one known semantic gap**

`TS.ADD` opens a rule's first bucket from the written sample alone (`handle_compaction_upsert`, `bucket_start == None`), while the batch path recalculates that bucket from the source (`handle_batch_compaction`, `recalculate_current_bucket`). They differ only for a rule created on a source that already holds samples whose first write is a back-fill — and a one-item `TS.MADD` takes the batch path, so `TS.ADD` and `TS.MADD` already disagree there. Add to `tests/compat/test_compat_compaction.py`, next to `test_createrule_on_a_source_that_already_has_data`:

```python
    @pytest.mark.parametrize("write", ["TS.ADD", "TS.MADD"])
    def test_backfill_as_first_write_after_createrule(self, diff, write):
        """A rule created on a source that already holds samples, whose first write is a
        back-fill into the bucket holding the source's last sample: does the bucket open from
        that one write, or from what the source holds? Decides the batch-of-one unification
        (docs/plans/2026-10-14-compaction-refactor.md, Task 8)."""
        diff("TS.CREATE", "c:cbf:src")
        for ts, value in ((0, 1.0), (1200, 2.0), (1500, 4.0)):
            diff("TS.ADD", "c:cbf:src", ts, value)
        diff("TS.CREATE", "c:cbf:dst")
        diff("TS.CREATERULE", "c:cbf:src", "c:cbf:dst", "AGGREGATION", "sum", 1000)
        diff(write, "c:cbf:src", 1300, 8.0)      # back-fill; the rule has no open bucket yet
        diff("TS.GET", "c:cbf:dst", "LATEST")
        diff("TS.ADD", "c:cbf:src", 2000, 16.0)  # closes bucket 1000
        diff("TS.RANGE", "c:cbf:dst", "-", "+")

    @pytest.mark.parametrize("write", ["TS.ADD", "TS.MADD"])
    def test_backfill_behind_the_open_bucket_as_first_write(self, diff, write):
        """Same, but the first write back-fills a bucket *older* than the one holding the
        source's last sample."""
        diff("TS.CREATE", "c:cbo:src")
        for ts, value in ((0, 1.0), (1200, 2.0), (1500, 4.0)):
            diff("TS.ADD", "c:cbo:src", ts, value)
        diff("TS.CREATE", "c:cbo:dst")
        diff("TS.CREATERULE", "c:cbo:src", "c:cbo:dst", "AGGREGATION", "sum", 1000)
        diff(write, "c:cbo:src", 300, 8.0)
        diff("TS.GET", "c:cbo:dst", "LATEST")
        diff("TS.ADD", "c:cbo:src", 2000, 16.0)
        diff("TS.RANGE", "c:cbo:dst", "-", "+")
```

(For `TS.MADD` the arguments `"c:cbf:src", 1300, 8.0` are exactly one key/timestamp/value triple, so the same `diff(write, …)` call works for both.)

Run: `RTS_COMPAT=1 TEST_PATTERN="first_write" SERVER_VERSION=unstable ./build.sh` and read the reference replies in the failure output. Exactly one of these holds; take its branch:

- **A — the reference opens the bucket from the written sample only** (sum `8` for `c:cbf:dst` bucket 1000): the `TS.ADD` cases pass and the `TS.MADD` cases fail. Change the batch path to match, in `handle_batch_compaction`'s bucket loop:
```rust
    for &(bucket_start, min_ts) in buckets.iter().rev() {
        let bucket_end = ctx.rule.bucket_end(bucket_start);
        if ctx.rule.bucket_start.is_none() {
            // No open bucket: the rule was created on a source that already held samples. It
            // opens from this batch's own writes and ignores what the source held before the
            // rule existed — the reference's behavior
            // (compat test_backfill_as_first_write_after_createrule).
            let opening: SmallVec<[Sample; TEMP_VEC_LEN]> = upserts
                .iter()
                .map(|(sample, _)| *sample)
                .filter(|sample| ctx.rule.calc_bucket_start(sample.timestamp) == bucket_start)
                .collect();
            ctx.rule.bucket_start = Some(bucket_start);
            for sample in opening {
                ctx.update(sample.timestamp, sample.value);
            }
        } else if ctx.rule.bucket_start == Some(bucket_start) {
            recalculate_current_bucket(ctx, bucket_start, bucket_end, min_ts)?;
        } else {
            recalculate_bucket(ctx, bucket_start, bucket_end, min_ts, null_ts_filter)?;
        }
    }
```
  and delete the paragraph of the loop's comment that begins "A bucket only reaches the destination when it closes" (it described the old branch).
- **B — the reference recalculates from the source** (sum `14`): the `TS.MADD` cases pass and the `TS.ADD` cases fail. No engine change: Task 9 moves `TS.ADD` onto the batch path, which fixes them.
- **C — neither**: record the reference's replies under a new heading "§3.8 Rule created on existing data" in `docs/plans/compaction-replication-plan.md`, mark all four cases `@pytest.mark.xfail(strict=True, reason="reference does neither; see compaction-replication-plan.md §3.8")`, raise it with the owner, and continue with B's (no-change) branch so `TS.ADD` and `TS.MADD` at least agree.

Re-run: `RTS_COMPAT=1 TEST_PATTERN="first_write or createrule" SERVER_VERSION=unstable ./build.sh` — Expected after branch A: all PASS. After B: the `TS.ADD` cases still FAIL until Task 9 (mark them `@pytest.mark.xfail(strict=True, reason="fixed by Task 9")` now and remove the marks in Task 9).

- [ ] **Step 2: Write the property tests**

Append to the `tests` module of `src/series/compaction/engine.rs` (create `#[cfg(test)] mod tests { use super::*; … }` if Task 7 left none there):

```rust
    use crate::aggregators::{AggregationHandler, AggregationType, Aggregator};
    use crate::series::{DuplicatePolicy, SampleAddResult, SeriesLink};
    use proptest::prelude::*;
    use std::collections::BTreeMap;
    use std::time::Duration;

    /// Every aggregator a rule accepts without an inline condition.
    const RULE_TYPES: &[AggregationType] = &[
        AggregationType::Avg,
        AggregationType::Sum,
        AggregationType::Min,
        AggregationType::Max,
        AggregationType::Range,
        AggregationType::Count,
        AggregationType::CountAll,
        AggregationType::CountNan,
        AggregationType::First,
        AggregationType::Last,
        AggregationType::StdP,
        AggregationType::StdS,
        AggregationType::VarP,
        AggregationType::VarS,
        AggregationType::Rate,
        AggregationType::IRate,
        AggregationType::Increase,
    ];

    fn fresh_rule(agg: AggregationType, bucket: u64) -> CompactionRule {
        let mut aggregator: Aggregator = agg.into();
        if let Aggregator::Rate(r) = &mut aggregator {
            r.set_window_ms(bucket);
        }
        CompactionRule {
            dest: SeriesLink::from("dst"),
            aggregator,
            bucket_duration: bucket,
            align_timestamp: 0,
            bucket_start: None,
            has_samples: false,
        }
    }

    fn dest_samples(dest: &TimeSeries) -> Vec<(Timestamp, u64)> {
        dest.iter().map(|s| (s.timestamp, s.value.to_bits())).collect()
    }

    fn open_state(rule: &CompactionRule) -> (Option<Timestamp>, bool, Option<u64>) {
        let current = if rule.has_samples {
            AggregationHandler::current(&rule.aggregator).map(f64::to_bits)
        } else {
            None
        };
        (rule.bucket_start, rule.has_samples, current)
    }

    /// `TS.ADD`'s path before Task 9: `Upsert` at or below the previous last timestamp,
    /// otherwise `AddNew` with the stored last sample.
    fn ingest_single_path(
        parent: &TimeSeries,
        dest: &mut TimeSeries,
        rule: &mut CompactionRule,
        stored: Sample,
        prev_last: Option<Timestamp>,
    ) {
        let is_upsert = prev_last.is_some_and(|last| stored.timestamp <= last);
        let op = if is_upsert {
            CompactionOp::Upsert(stored)
        } else {
            CompactionOp::AddNew(parent.last_sample.unwrap_or(stored))
        };
        let mut cctx = CompactionContext::new(parent, dest, rule);
        apply_op(&mut cctx, op).unwrap();
    }

    /// The path every write takes from Task 9 on.
    fn ingest_batch_of_one(
        parent: &TimeSeries,
        dest: &mut TimeSeries,
        rule: &mut CompactionRule,
        stored: Sample,
        prev_last: Option<Timestamp>,
    ) {
        let samples = [stored];
        let order = [stored.timestamp];
        let mut cctx = CompactionContext::new(parent, dest, rule);
        let op = CompactionOp::AddBatch {
            samples: &samples,
            prev_last,
            input_order: &order,
        };
        apply_op(&mut cctx, op).unwrap();
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(512))]

        /// Transitional (deleted in Task 10): on a rule that has seen every write since the
        /// source was empty, a one-item batch leaves exactly the destination and open-bucket
        /// state the single-sample path does — for every aggregator, with and without a
        /// retention window shorter than a bucket.
        #[test]
        fn batch_of_one_matches_the_single_sample_path(
            agg in prop::sample::select(RULE_TYPES),
            bucket in prop::sample::select(&[500u64, 1000][..]),
            retention in prop::sample::select(&[0u64, 700, 2500][..]),
            writes in prop::collection::vec((0i64..5000, -100i32..100), 1..40),
        ) {
            let mut parent = TimeSeries::new();
            parent.retention = Duration::from_millis(retention);
            let (mut dest_a, mut dest_b) = (TimeSeries::new(), TimeSeries::new());
            let (mut rule_a, mut rule_b) = (fresh_rule(agg, bucket), fresh_rule(agg, bucket));

            for (ts, value) in writes {
                let prev_last = parent.last_sample.map(|s| s.timestamp);
                let SampleAddResult::Ok(stored) =
                    parent.add(ts, value as f64, Some(DuplicatePolicy::KeepLast))
                else {
                    continue; // TS.ADD runs no compaction for a rejected sample
                };
                ingest_single_path(&parent, &mut dest_a, &mut rule_a, stored, prev_last);
                ingest_batch_of_one(&parent, &mut dest_b, &mut rule_b, stored, prev_last);
                prop_assert_eq!(dest_samples(&dest_a), dest_samples(&dest_b), "after write {}", ts);
                prop_assert_eq!(open_state(&rule_a), open_state(&rule_b), "after write {}", ts);
            }
        }

        /// Permanent: without retention, the destination holds exactly the buckets strictly older
        /// than the one containing the source's last sample that received at least one counted
        /// sample, each the aggregate of the source's samples in timestamp order, and the rule's
        /// open bucket is the aggregate of the newest bucket. A failure is an engine bug.
        #[test]
        fn destination_matches_a_recomputation_from_the_source(
            agg in prop::sample::select(RULE_TYPES),
            bucket in prop::sample::select(&[500u64, 1000][..]),
            writes in prop::collection::vec((0i64..5000, -100i32..100), 1..40),
        ) {
            let mut parent = TimeSeries::new();
            let mut dest = TimeSeries::new();
            let mut rule = fresh_rule(agg, bucket);
            for (ts, value) in writes {
                let prev_last = parent.last_sample.map(|s| s.timestamp);
                if let SampleAddResult::Ok(stored) =
                    parent.add(ts, value as f64, Some(DuplicatePolicy::KeepLast))
                {
                    ingest_batch_of_one(&parent, &mut dest, &mut rule, stored, prev_last);
                }
            }

            let mut by_bucket: BTreeMap<Timestamp, (Aggregator, bool)> = BTreeMap::new();
            for sample in parent.iter() {
                let start = rule.calc_bucket_start(sample.timestamp);
                let (aggregator, counted) = by_bucket
                    .entry(start)
                    .or_insert_with(|| (fresh_rule(agg, bucket).aggregator, false));
                if AggregationHandler::update(aggregator, sample.timestamp, sample.value) {
                    *counted = true;
                }
            }
            let open = rule.calc_bucket_start(parent.last_sample.unwrap().timestamp);
            let mut expected = Vec::new();
            let mut open_expected = (Some(open), false, None);
            for (start, (mut aggregator, counted)) in by_bucket {
                if start < open {
                    if counted {
                        expected.push((start, AggregationHandler::finalize(&mut aggregator).to_bits()));
                    }
                } else if counted {
                    open_expected = (
                        Some(open),
                        true,
                        AggregationHandler::current(&aggregator).map(f64::to_bits),
                    );
                }
            }
            prop_assert_eq!(dest_samples(&dest), expected);
            prop_assert_eq!(open_state(&rule), open_expected);
        }
    }
```

- [ ] **Step 3: Run the property tests**

Run: `cargo test --features enable-system-alloc -- batch_of_one_matches destination_matches_a_recomputation`
Expected: both PASS. A failure of `batch_of_one_matches_the_single_sample_path` is a second semantic gap: proptest prints the shrunk `writes`; turn it into a compat test exactly like Step 1's, let the reference decide, and apply the matching branch before continuing. A failure of the oracle test is an engine bug present today: record the shrunk input in `docs/plans/compaction-replication-plan.md` §3, fix it in this task with a regression test, and re-run.

- [ ] **Step 4: Add the benchmark hooks**

In `src/series/compaction/engine.rs`:

```rust
/// A fresh rule for the `compaction_ingest` benchmark.
#[cfg(feature = "test-utils")]
#[doc(hidden)]
pub fn bench_rule(agg: crate::aggregators::AggregationType, bucket_ms: u64) -> CompactionRule {
    let mut aggregator: crate::aggregators::Aggregator = agg.into();
    if let crate::aggregators::Aggregator::Rate(r) = &mut aggregator {
        r.set_window_ms(bucket_ms);
    }
    CompactionRule {
        dest: crate::series::SeriesLink::from("bench:dst"),
        aggregator,
        bucket_duration: bucket_ms,
        align_timestamp: 0,
        bucket_start: None,
        has_samples: false,
    }
}

/// One stored sample through one rule, exactly as `TS.ADD` drives it. The body changes in
/// Task 9; the benchmark IDs do not, so criterion compares the two paths across the switch.
#[cfg(feature = "test-utils")]
#[doc(hidden)]
pub fn bench_ingest_one(
    parent: &TimeSeries,
    dest: &mut TimeSeries,
    rule: &mut CompactionRule,
    sample: Sample,
    prev_last: Option<Timestamp>,
) {
    let is_upsert = prev_last.is_some_and(|last| sample.timestamp <= last);
    let op = if is_upsert {
        CompactionOp::Upsert(sample)
    } else {
        CompactionOp::AddNew(parent.last_sample.unwrap_or(sample))
    };
    let mut cctx = CompactionContext::new(parent, dest, rule);
    let _ = apply_op(&mut cctx, op);
}
```

In `src/series/compaction/mod.rs`, after the other `pub use` lines:

```rust
#[cfg(feature = "test-utils")]
pub use engine::{bench_ingest_one, bench_rule};
```

- [ ] **Step 5: Add the benchmark and save the baseline**

`benches/compaction_ingest.rs`:

```rust
//! Per-append cost of driving one compaction rule, the path every `TS.ADD` into a ruled series
//! pays. Gate for the batch-of-one switch (docs/plans/2026-10-14-compaction-refactor.md, Task 9):
//! save a baseline before the switch, compare after.
use criterion::{Criterion, criterion_group, criterion_main};
use std::hint::black_box;
use valkey_timeseries::aggregators::AggregationType;
use valkey_timeseries::common::Sample;
use valkey_timeseries::series::{TimeSeries, bench_ingest_one, bench_rule};

fn append_one_rule(c: &mut Criterion) {
    let mut group = c.benchmark_group("compaction_ingest");
    for (name, agg) in [
        ("sum", AggregationType::Sum),
        ("avg", AggregationType::Avg),
        ("std.s", AggregationType::StdS),
        ("rate", AggregationType::Rate),
    ] {
        group.bench_function(format!("append_one_rule/{name}"), |b| {
            // The append path never reads the source, so an empty one suffices; the
            // destination gains one bucket per 1000 iterations.
            let parent = TimeSeries::new();
            let mut dest = TimeSeries::new();
            let mut rule = bench_rule(agg, 1000);
            let mut ts: i64 = 0;
            b.iter(|| {
                let prev_last = (ts > 0).then(|| ts - 1);
                bench_ingest_one(&parent, &mut dest, &mut rule, black_box(Sample::new(ts, 1.0)), prev_last);
                ts += 1;
            });
        });
    }
    group.finish();
}

criterion_group!(benches, append_one_rule);
criterion_main!(benches);
```

`Cargo.toml`, after the `query_scan` entry:

```toml
[[bench]]
name = "compaction_ingest"
harness = false
```

Run: `cargo bench --bench compaction_ingest --features enable-system-alloc -- --save-baseline pre-unify`
Expected: four results under `compaction_ingest/append_one_rule/*`; the baseline is stored under `target/criterion/*/pre-unify` (keep `target/` until Task 9 Step 4).

- [ ] **Step 6: Lint and commit**

```bash
git add src/series/compaction benches/compaction_ingest.rs Cargo.toml tests/compat/test_compat_compaction.py docs/plans/compaction-replication-plan.md
git commit -m "test(compaction): characterize batch-of-one against the single-sample path"
```

### Task 9: Route `TS.ADD` and `TS.INCRBY` through the batch path

**Files:**
- Modify: `src/series/compaction/mod.rs` (`impl TimeSeries`: new `compact_sample`)
- Modify: `src/series/compaction/cascade.rs` (`apply_rules_internal`: new `runs_rules_in_parallel`; tests)
- Modify: `src/series/compaction/engine.rs` (`bench_ingest_one` body)
- Modify: `src/commands/ts_add.rs` (`handle_add`), `src/commands/ts_incr_decr_by.rs` (`run_compaction_for_increment`)
- Modify: `tests/compat/test_compat_compaction.py` (remove Task 8's `xfail` marks, branch B only)
- Test: `tests/test_compactions_add.py`

**Interfaces:**
- Consumes: `TimeSeries::batch_compaction(ctx, samples, prev_last, input_order)`.
- Produces: `pub fn compact_sample(&mut self, ctx: &Context, sample: Sample, prev_last: Option<Timestamp>) -> TsdbResult<()>` on `TimeSeries`; `fn runs_rules_in_parallel(op: &CompactionOp, rule_count: usize) -> bool` in `cascade.rs`.

- [ ] **Step 1: Write the failing tests**

`src/series/compaction/cascade.rs`, tests module (create one if absent):

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::Sample;

    #[test]
    fn a_single_sample_never_runs_rules_in_parallel() {
        let one = [Sample::new(1, 1.0)];
        let two = [Sample::new(1, 1.0), Sample::new(2, 1.0)];
        let single = CompactionOp::AddBatch { samples: &one, prev_last: None, input_order: &[1] };
        let batch = CompactionOp::AddBatch { samples: &two, prev_last: None, input_order: &[1, 2] };
        assert!(!runs_rules_in_parallel(&single, 8));
        assert!(runs_rules_in_parallel(&batch, 2));
        assert!(!runs_rules_in_parallel(&batch, 1));
        assert!(runs_rules_in_parallel(&CompactionOp::RemoveRange { start: 0, end: 1 }, 2));
    }
}
```

`tests/test_compactions_add.py`:

```python
    def test_incrby_at_the_last_timestamp_recalculates_the_bucket(self):
        """Review focus 3: an in-place increment of the last sample is an upsert; streaming it
        as a new sample would average 0 and 1 to 0.5."""
        c = self.client
        self.create_source_and_dest_series("inc:src", "inc:dst")
        self.add_compaction_rule("inc:src", "inc:dst", "avg", 1000)
        c.execute_command("TS.ADD", "inc:src", 0, 0)
        c.execute_command("TS.INCRBY", "inc:src", 1, "TIMESTAMP", 0)
        assert c.execute_command("TS.GET", "inc:dst", "LATEST") == [0, b"1"]
        c.execute_command("TS.ADD", "inc:src", 1000, 5)
        assert c.execute_command("TS.RANGE", "inc:dst", "-", "+") == [[0, b"1"]]

    def test_ignored_sample_runs_no_compaction(self):
        """Review focus 4: a sample IGNORE suppresses must not reach the rule."""
        c = self.client
        c.execute_command("TS.CREATE", "ign:src", "DUPLICATE_POLICY", "LAST", "IGNORE", 10, 0.5)
        c.execute_command("TS.CREATE", "ign:dst")
        self.add_compaction_rule("ign:src", "ign:dst", "sum", 1000)
        c.execute_command("TS.ADD", "ign:src", 0, 1)
        c.execute_command("TS.ADD", "ign:src", 5, 1.2)   # within both thresholds: ignored
        c.execute_command("TS.ADD", "ign:src", 1000, 2)
        assert c.execute_command("TS.RANGE", "ign:dst", "-", "+") == [[0, b"1"]]
```

- [ ] **Step 2: Run them to verify the unit test fails**

Run: `cargo test --features enable-system-alloc a_single_sample_never_runs_rules_in_parallel 2>&1 | tail -3`
Expected: compile error — `runs_rules_in_parallel` not found. (The two Python tests pass today; they guard the switch.)

- [ ] **Step 3: Implement**

`src/series/compaction/cascade.rs` — add, and use it in `apply_rules_internal` in place of the inline `let parallel = …;`:

```rust
/// Whether `op` is worth spreading across rules. One sample is O(1) aggregator work per rule —
/// far less than a pool handoff — so `TS.ADD` into a source with several rules stays on the
/// command thread; batches and removals, which can recalculate many buckets, fan out.
fn runs_rules_in_parallel(op: &CompactionOp, rule_count: usize) -> bool {
    if rule_count < PARALLEL_THRESHOLD {
        return false;
    }
    match op {
        CompactionOp::AddBatch { samples, .. } => samples.len() > 1,
        CompactionOp::RemoveRange { .. } => true,
        CompactionOp::AddNew(_) | CompactionOp::Upsert(_) => false,
    }
}
```

```rust
    let parallel = runs_rules_in_parallel(&op, destinations.len());
```

`src/series/compaction/mod.rs`, in `impl TimeSeries`:

```rust
    /// Drive this series' rules for one sample just stored at `sample.timestamp`, as a one-item
    /// batch. `prev_last` is the series' last timestamp from *before* the write: at or below it
    /// the write is a back-fill (or an in-place update) and the bucket is recalculated from the
    /// source, above it the sample streams into the open bucket.
    pub fn compact_sample(
        &mut self,
        ctx: &Context,
        sample: Sample,
        prev_last: Option<Timestamp>,
    ) -> TsdbResult<()> {
        // For an append the stored (rounded) value is the last sample; a back-fill only uses the
        // timestamp, since its bucket is re-read from the source.
        let stored = match self.last_sample {
            Some(last) if last.timestamp == sample.timestamp => last,
            _ => sample,
        };
        self.batch_compaction(ctx, &[stored], prev_last, &[stored.timestamp])
    }
```

`src/commands/ts_add.rs`, in `handle_add` — replace the compaction block (from `if !ignored && !series.rules.is_empty() {` to its closing brace) with:

```rust
    if !ignored && !series.rules.is_empty() {
        if let Err(err) = series.compact_sample(ctx, Sample::new(ts, value), last_ts) {
            let key = &args[1];
            ctx.log_warning(&format!(
                "TSDB: error running compaction for key '{key}': {err}"
            ));
        }
    }
```

and delete the now-unused `is_upsert` computation above it.

`src/commands/ts_incr_decr_by.rs` — `run_compaction_for_increment` becomes:

```rust
/// Drive the series' compaction rules after a successful increment.
///
/// `TS.INCRBY key <d> TIMESTAMP <last_ts>` updates the existing sample in place; `prev_last_ts`
/// makes that a back-fill, so the bucket is recalculated from the source rather than the new
/// value streamed in as an additional sample (`TS.ADD k 0 0` then `TS.INCRBY k 1 TIMESTAMP 0`
/// must give an `avg` rollup of 1, not 0.5).
///
/// A failure is logged, not returned: the increment is already stored, and an error would
/// skip replication, leaving the replica without it (see TS.ADD).
fn run_compaction_for_increment(
    ctx: &Context,
    series: &mut TimeSeries,
    key_name: &ValkeyString,
    added: Sample,
    prev_last_ts: Option<Timestamp>,
) {
    if series.rules.is_empty() {
        return;
    }
    if let Err(err) = series.compact_sample(ctx, added, prev_last_ts) {
        ctx.log_warning(&format!(
            "TSDB: error running compaction for key '{key_name}': {err}"
        ));
    }
}
```

`src/series/compaction/engine.rs` — `bench_ingest_one` now measures the new path:

```rust
pub fn bench_ingest_one(
    parent: &TimeSeries,
    dest: &mut TimeSeries,
    rule: &mut CompactionRule,
    sample: Sample,
    prev_last: Option<Timestamp>,
) {
    let samples = [sample];
    let order = [sample.timestamp];
    let mut cctx = CompactionContext::new(parent, dest, rule);
    let op = CompactionOp::AddBatch {
        samples: &samples,
        prev_last,
        input_order: &order,
    };
    let _ = apply_op(&mut cctx, op);
}
```

Branch B of Task 8 only: delete the `@pytest.mark.xfail(strict=True, reason="fixed by Task 9")` marks.

- [ ] **Step 4: Run the gate**

Run: `cargo bench --bench compaction_ingest --features enable-system-alloc -- --baseline pre-unify`
Expected: every `compaction_ingest/append_one_rule/*` reports a `change` whose upper bound is ≤ +10 %. If one lands between +5 % and +15 %, re-run baseline and new alternately twice (check out the Task 8 commit for the baseline run) and judge on the per-pair minimum — single criterion runs on this machine swing by that much. If it is over budget, stop: the fallback is to keep the append branch out of the batch machinery in `compact_sample` (`if prev_last.is_none_or(|last| stored.timestamp > last)` → stream via `handle_sample_compaction` on each rule), which needs its own small engine entry point — raise it with the owner before adding it.

Run: `cargo test --features enable-system-alloc` — Expected: PASS (including both property tests).
Run: `TEST_PATTERN="compaction or incrby or test_ts_add or createrule or test_replication_compaction_ or test_replication_ts_incrby" SERVER_VERSION=unstable ./build.sh` — Expected: PASS.
Run: `RTS_COMPAT=1 TEST_PATTERN="compaction or add or incrby" SERVER_VERSION=unstable ./build.sh` — Expected: PASS, including all four `first_write` cases.

- [ ] **Step 5: Lint and commit**

Record the four criterion `change` lines in the commit body.

```bash
git add src/series/compaction src/commands/ts_add.rs src/commands/ts_incr_decr_by.rs tests/test_compactions_add.py tests/compat/test_compat_compaction.py
git commit -m "refactor(compaction): route TS.ADD and TS.INCRBY through the batch path"
```

### Task 10: Delete the single-sample ops; one ingest operation

**Files:**
- Modify: `src/series/compaction/engine.rs` (`CompactionOp`, `apply_op`, delete `handle_compaction_upsert`, tests)
- Modify: `src/series/compaction/cascade.rs` (`process_series_with_compaction`, `runs_rules_in_parallel`, test)
- Modify: `src/series/compaction/mod.rs` (`impl TimeSeries`: delete `run_compaction`, `upsert_compaction`; `batch_compaction`)
- Modify: `src/series/multi_del.rs`, `src/series/time_series.rs:826` (doc reference)

**Interfaces:**
- Changes: `CompactionOp` is `Ingest { samples, prev_last, input_order }` | `RemoveRange { start, end }`. Removes `TimeSeries::run_compaction`, `TimeSeries::upsert_compaction`, `handle_compaction_upsert`.

- [ ] **Step 1: Delete the transitional test**

In `engine.rs` tests delete `batch_of_one_matches_the_single_sample_path` and `ingest_single_path`. Run `cargo test --features enable-system-alloc destination_matches_a_recomputation` — Expected: PASS (the permanent oracle stays).

- [ ] **Step 2: Collapse the op enum**

`engine.rs`:

```rust
/// Single entry point for all compaction-related mutations.
#[derive(Debug, Clone, Copy)]
pub enum CompactionOp<'a> {
    /// Samples just merged into the source series — one (`TS.ADD`, `TS.INCRBY`) or many
    /// (`TS.MADD`, `TS.ADDBULK`, a cascade level).
    ///
    /// `samples` must be sorted by timestamp ascending with unique timestamps and hold the
    /// stored (rounded) values. `prev_last` is the source series' last timestamp from before
    /// the samples were merged.
    ///
    /// `input_order` holds the accepted timestamps in the order the caller supplied them
    /// (MADD argument order), which sorting `samples` discards. Back-fill classification replays
    /// it: an item at or below the running maximum *when it was applied* is a back-fill, which a
    /// comparison against `prev_last` alone cannot tell.
    Ingest {
        samples: &'a [Sample],
        prev_last: Option<Timestamp>,
        input_order: &'a [Timestamp],
    },
    /// Remove a range from source and reflect it into destinations (and ongoing aggregation state)
    RemoveRange { start: Timestamp, end: Timestamp },
}
```

```rust
fn apply_op(ctx: &mut CompactionContext<'_>, op: CompactionOp) -> TsdbResult<()> {
    match op {
        CompactionOp::Ingest {
            samples,
            prev_last,
            input_order,
        } => handle_batch_compaction(ctx, samples, prev_last, input_order),
        CompactionOp::RemoveRange { start, end } => {
            handle_compaction_range_removal(ctx, start, end)
        }
    }
}
```

Delete `handle_compaction_upsert`. Rename `CompactionOp::AddBatch` to `CompactionOp::Ingest` everywhere else (`grep -rn "AddBatch" src` lists them: `cascade.rs` cascade loop and `runs_rules_in_parallel`, `engine.rs` `bench_ingest_one` and tests, `mod.rs` `batch_compaction`, and doc comments in `time_series.rs:826` and `sample_merge.rs`).

`cascade.rs`:

```rust
    let notify_destinations = matches!(op, CompactionOp::Ingest { .. });
```

```rust
    match op {
        CompactionOp::Ingest { samples, .. } => samples.len() > 1,
        CompactionOp::RemoveRange { .. } => true,
    }
```

and in its unit test, `CompactionOp::AddBatch` → `CompactionOp::Ingest`.

`mod.rs` — delete `run_compaction` and `upsert_compaction` from `impl TimeSeries`.

- [ ] **Step 3: Build, test, compat**

Run the lint gate — Expected: clean; `grep -rn "AddNew\|Upsert(\|upsert_compaction\|run_compaction(" src` prints nothing.
Run: `cargo test --features enable-system-alloc` — Expected: PASS.
Run: `TEST_PATTERN="compaction or createrule or deleterule or incrby or test_ts_add or test_ts_madd or test_ts_addbulk or test_ts_del or test_ts_mdel or test_replication" SERVER_VERSION=unstable ./build.sh` — Expected: PASS.
Run: `RTS_COMPAT=1 TEST_PATTERN="compaction" SERVER_VERSION=unstable ./build.sh` — Expected: PASS.
Run: `cargo bench --bench compaction_ingest --features enable-system-alloc -- --baseline pre-unify` — Expected: within the Task 9 budget.

- [ ] **Step 4: Commit**

```bash
git add src/series
git commit -m "refactor(compaction): one ingest operation; delete the single-sample ops"
```

---

## Part 2 — pin replication inputs

### Task 11: Auto-created series replicate the primary's resolved options

**Files:**
- Modify: `src/series/utils.rs` (`create_and_store_series`; new `resolved_create_args`)
- Test: `tests/test_ts_replication.py`

**Interfaces:**
- Produces: `pub(crate) fn resolved_create_args(ctx: &Context, series: &TimeSeries) -> Vec<ValkeyString>` — the `TS.CREATE` option list (without the key) that recreates `series` exactly. Task 12 reuses it for policy destinations.

- [ ] **Step 1: Write the failing test**

```python
    def test_replication_autocreate_pins_primary_defaults(self):
        """An auto-created series is replicated with the primary's resolved defaults ahead of the
        write, so a replica whose ts-retention-policy differs still builds the same series — and
        a rule fed by it uses the same retention floor."""
        replica = self.replicas[0].client
        replica.execute_command("CONFIG", "SET", "ts.ts-retention-policy", 500)
        self.client.execute_command("CONFIG", "SET", "ts.ts-retention-policy", 3000)

        self.client.execute_command("TS.ADD", "ts:auto:plain", 0, 1)
        self.client.execute_command("TS.ADD", "ts:auto:metric", 0, 1, "METRIC", 'cpu{host="a"}')
        self.client.execute_command("TS.INCRBY", "ts:auto:incr", 1, "LABELS", "k", "v")
        self.client.execute_command("TS.ADDBULK", "ts:auto:bulk", json.dumps({"timestamps": [0, 1000], "values": [1, 2]}))
        self.wait_for_replication()
        for key in ("ts:auto:plain", "ts:auto:metric", "ts:auto:incr", "ts:auto:bulk"):
            primary_info = parse_info_response(self.client.execute_command("TS.INFO", key))
            replica_info = parse_info_response(replica.execute_command("TS.INFO", key))
            for field in ("retentionTime", "chunkSize", "labels", "duplicatePolicy"):
                assert replica_info[field] == primary_info[field], (key, field)
            assert primary_info["retentionTime"] == 3000, key

        self.client.execute_command("TS.CREATE", "ts:auto:dst")
        self.client.execute_command("TS.CREATERULE", "ts:auto:plain", "ts:auto:dst", "AGGREGATION", "sum", 1000)
        for ts in range(500, 6000, 500):
            self.client.execute_command("TS.ADD", "ts:auto:plain", ts, 1)
        self.client.execute_command("TS.ADD", "ts:auto:plain", 2600, 10)   # back-fill near the floor
        self.assert_compaction_equivalent("ts:auto:plain", "ts:auto:dst")
```

- [ ] **Step 2: Run it to verify it fails**

Run: `TEST_PATTERN="test_replication_autocreate_pins_primary_defaults" SERVER_VERSION=unstable ./build.sh`
Expected: FAIL on `retentionTime` (replica built the series with 500).

- [ ] **Step 3: Implement**

In `src/series/utils.rs` add the imports `use crate::common::rounding::RoundingStrategy;` and `use crate::labels::METRIC_NAME_LABEL;` if not present, and:

```rust
/// `TS.CREATE` options that recreate `series` exactly, every configuration-derived default
/// materialized. An auto-creating write replicates this ahead of itself so a replica (or an AOF
/// replay) builds the series from the primary's configuration, not its own: `ts-retention-policy`,
/// `ts-encoding` and the chunk size are per node, and a retention mismatch changes recalculation
/// floors and therefore compaction output.
///
/// `DUPLICATE_POLICY` is only emitted when the series pins one: an unset policy follows the
/// node's `ts-duplicate-policy` at write time, and pinning it here would change that. A series
/// with a `__name__` label is re-created through `METRIC`, which is the only way to set it.
pub(crate) fn resolved_create_args(ctx: &Context, series: &TimeSeries) -> Vec<ValkeyString> {
    let mut args: Vec<String> = vec![
        "RETENTION".into(),
        series.retention.as_millis().to_string(),
        "ENCODING".into(),
        series.chunk_encoding.name().to_string(),
        "CHUNK_SIZE".into(),
        series.chunk_size_bytes.to_string(),
    ];
    if let Some(policy) = series.sample_duplicates.policy {
        args.push("DUPLICATE_POLICY".into());
        args.push(policy.as_str().to_string());
    }
    let dup = &series.sample_duplicates;
    if dup.max_time_delta > 0 || dup.max_value_delta > 0.0 {
        args.push("IGNORE".into());
        args.push(dup.max_time_delta.to_string());
        args.push(dup.max_value_delta.to_string());
    }
    match series.rounding {
        Some(RoundingStrategy::SignificantDigits(digits)) => {
            args.push("SIGNIFICANT_DIGITS".into());
            args.push(digits.to_string());
        }
        Some(RoundingStrategy::DecimalDigits(digits)) => {
            args.push("DECIMAL_DIGITS".into());
            args.push(digits.to_string());
        }
        None => {}
    }
    if series.labels.get_value(METRIC_NAME_LABEL).is_some() {
        args.push("METRIC".into());
        args.push(series.prometheus_metric_name());
    } else if !series.labels.is_empty() {
        args.push("LABELS".into());
        for label in series.labels.iter() {
            args.push(String::from_utf8_lossy(label.name.as_bytes()).into_owned());
            args.push(String::from_utf8_lossy(label.value.as_bytes()).into_owned());
        }
    }
    args.into_iter()
        .map(|arg| ctx.create_string(arg.as_bytes()))
        .collect()
}
```

In `create_and_store_series`, after `let mut series = get_timeseries_mut(ctx, key, None)?;`:

```rust
    if !explicit_create {
        // Auto-create: replicate the series as the primary built it, defaults materialized, ahead
        // of the write command's own propagation (`alsoPropagate` keeps call order). The replica's
        // replayed write then finds the series and its inline creation options are inert, exactly
        // as on the primary when the series already exists. `TS.CREATE` here, not
        // `replicate_verbatim`: that would copy the *write's* argv and apply it twice.
        let create_args = resolved_create_args(ctx, &series);
        let mut replication_args: Vec<&ValkeyString> = Vec::with_capacity(create_args.len() + 1);
        replication_args.push(key);
        replication_args.extend(create_args.iter());
        ctx.replicate("TS.CREATE", &replication_args);
    }
```

Update the doc comment on `create_and_store_series`: replace "auto-create: neither — the calling command replicates *itself* …" with "auto-create: no `ts.create` event; the series itself is replicated as a resolved `TS.CREATE` (see the body), and the calling command replicates its own write. (The old corner case — a command erroring after the auto-create left the primary with an empty series the replica never saw — is gone: the `TS.CREATE` is already queued.)"

- [ ] **Step 4: Run**

Run: `TEST_PATTERN="test_replication_autocreate_pins_primary_defaults or test_replication_ts_incrby or test_replication_ts_addbulk or test_ts_add or test_ts_addbulk or test_ts_incrby" SERVER_VERSION=unstable ./build.sh`
Expected: PASS. If `TS.CREATE … METRIC 'cpu{host="a"}'` is rejected on the replica (check the replica log in the test's `testdir` for `TS.CREATE`), the `METRIC` rendering is wrong for that label set: `prometheus_metric_name()` is the string `TS.CREATE … METRIC` accepts, so fix the parser side, not this renderer.

- [ ] **Step 5: Compat, lint, commit**

Run: `RTS_COMPAT=1 TEST_PATTERN="add or incrby or create" SERVER_VERSION=unstable ./build.sh` — Expected: PASS (replies unchanged; only the stream changed).

```bash
git add src/series/utils.rs tests/test_ts_replication.py
git commit -m "fix(replication): replicate auto-created series with the primary's resolved options"
```

### Task 12: Policy-created destinations come from the primary

**Files:**
- Modify: `src/series/utils.rs` (`add_default_compactions`, ~lines 230–290)
- Modify: `docs/overview.md` (new "Replication" subsection)
- Test: `tests/test_ts_replication.py`

- [ ] **Step 1: Write the failing test**

```python
    def test_replication_policy_destinations_come_from_the_primary(self):
        """Policy-created destinations are replicated as explicit creates and rules; the replica's
        own ts-compaction-policy is ignored for replicated creates."""
        replica = self.replicas[0].client
        self.client.execute_command("CONFIG", "SET", "ts.ts-compaction-policy", "max:1m:1h;avg:2m:1h")
        replica.execute_command("CONFIG", "SET", "ts.ts-compaction-policy", "min:5m:1h")

        self.client.execute_command("TS.CREATE", "ts:pol:explicit", "LABELS", "a", "b")
        self.client.execute_command("TS.ADD", "ts:pol:auto", 0, 1)   # auto-create applies the policy too
        self.wait_for_replication()
        for src in ("ts:pol:explicit", "ts:pol:auto"):
            primary_rules = parse_info_response(self.client.execute_command("TS.INFO", src))["rules"]
            replica_rules = parse_info_response(replica.execute_command("TS.INFO", src))["rules"]
            describe = lambda rules: sorted((r.dest_key, r.bucket_duration, r.aggregation) for r in rules)
            assert describe(replica_rules) == describe(primary_rules), src
            assert {r.aggregation for r in primary_rules} == {"MAX", "AVG"}, src
            for r in primary_rules:
                assert replica.execute_command("EXISTS", r.dest_key) == 1
                assert parse_info_response(replica.execute_command("TS.INFO", r.dest_key))["sourceKey"] == src
        assert replica.execute_command("TS.QUERYINDEX", "aggregation=min") == []

        for ts in range(10_000, 300_000, 10_000):
            self.client.execute_command("TS.ADD", "ts:pol:explicit", ts, ts)
        dests = [r.dest_key for r in parse_info_response(self.client.execute_command("TS.INFO", "ts:pol:explicit"))["rules"]]
        self.assert_compaction_equivalent("ts:pol:explicit", *dests)
```

- [ ] **Step 2: Run it to verify it fails**

Run: `TEST_PATTERN="test_replication_policy_destinations_come_from_the_primary" SERVER_VERSION=unstable ./build.sh`
Expected: FAIL — the replica reports a `MIN` rule (its own policy) and `TS.QUERYINDEX aggregation=min` is non-empty.

- [ ] **Step 3: Implement**

In `src/series/utils.rs`, add `use crate::common::context::is_real_user_client;` and rewrite `add_default_compactions`:

```rust
fn add_default_compactions(
    ctx: &Context,
    series: &mut TimeSeries,
    key: &ValkeyString,
) -> ValkeyResult<()> {
    // A replica or an AOF replay never applies the policy itself: the primary that created the
    // series replicates the destinations it built (below), so the rules come from the primary's
    // configuration rather than this node's.
    if !is_real_user_client(ctx) {
        return Ok(());
    }

    let key_str = key.to_string_lossy();
    let Some(compaction_rules) = create_compaction_rules_from_config(&key_str) else {
        // No compaction rules available for this key
        return Ok(());
    };

    let mut base_config = TimeSeriesOptions::from_config();
    base_config.chunk_encoding = ChunkEncoding::Uncompressed;

    // create a new series for each compaction rule
    let mut rules = Vec::with_capacity(compaction_rules.len());
    for (dest_key, (rule, retention)) in compaction_rules.into_iter() {
        let bucket_duration = rule.bucket_duration;
        let agg_type = rule.aggregator.aggregation_type();

        let mut labels = series.labels.to_label_vec();

        labels.push(Label::new("aggregation", agg_type.name()));
        let duration_str = bucket_duration.to_string();
        labels.push(Label::new("time_bucket", &duration_str));

        let child_key = create_key_string(ctx, dest_key.as_bytes());
        let options = TimeSeriesOptions {
            src: Some(SeriesLink::from_key(key.as_slice())),
            retention: Some(Duration::from_millis(retention)),
            labels: Some(labels),
            ..base_config
        };

        let value_key = ValkeyKeyWritable::open(ctx.ctx, &child_key);
        if !value_key.is_empty() {
            // Not ours to write into. Skipped on every node alike — the replica applies no policy
            // and receives no rule for it — so the two stay consistent; the warning is the only
            // trace, and an operator who wants the rule frees the key and re-creates the source.
            let msg = format!(
                "TSDB: compaction series for key '{child_key}' already exists; policy rule for \
                 '{key_str}' skipped."
            );
            ctx.log_warning(&msg);
            continue;
        }

        let destination = match create_series(&child_key, options, ctx) {
            Ok(dest_series) => dest_series,
            Err(e) => {
                let msg =
                    format!("TSDB: error creating compaction series for key \"{child_key}\": {e}");
                ctx.log_warning(&msg);
                continue;
            }
        };

        // Rendered before the value moves into the key.
        let create_args = resolved_create_args(ctx, &destination);
        value_key.set_value(&VK_TIME_SERIES_TYPE, destination)?;
        replicate_policy_destination(ctx, key, &child_key, &rule, &create_args);

        rules.push(rule);
    }
    series.rules.extend(rules);

    Ok(())
}

/// Replicate a policy-created destination and its rule as the explicit commands a client would
/// have issued, so a replica or AOF replay rebuilds exactly this pair without a policy of its own.
/// Queued after the source's own `TS.CREATE` (explicit, or the resolved one an auto-create emits),
/// so the rule's source exists when the replica applies it.
fn replicate_policy_destination(
    ctx: &Context,
    source_key: &ValkeyString,
    dest_key: &ValkeyString,
    rule: &CompactionRule,
    create_args: &[ValkeyString],
) {
    let mut args: Vec<&ValkeyString> = Vec::with_capacity(create_args.len() + 1);
    args.push(dest_key);
    args.extend(create_args.iter());
    ctx.replicate("TS.CREATE", &args);

    let aggregation = ctx.create_string("AGGREGATION");
    let agg_name = ctx.create_string(rule.aggregator.aggregation_type().name().as_bytes());
    let duration = ctx.create_string(rule.bucket_duration.to_string().as_bytes());
    let align = ctx.create_string(rule.align_timestamp.to_string().as_bytes());
    ctx.replicate(
        "TS.CREATERULE",
        &[source_key, dest_key, &aggregation, &agg_name, &duration, &align],
    );
}
```

(`CompactionRule` is already imported in this file via `crate::series`; add it if the compiler says otherwise.)

- [ ] **Step 4: Document the per-node configuration rule**

Append to `docs/overview.md`, after the "Compaction & Rules" list:

```markdown
### Replication

Compaction destinations are owned by the primary. A series created by a write command is
replicated as an explicit `TS.CREATE` carrying the primary's resolved defaults, and the
destinations a `ts-compaction-policy` creates are replicated as explicit `TS.CREATE` +
`TS.CREATERULE` pairs, so a replica's own `ts-compaction-policy`, `ts-retention-policy` and
`ts-encoding` never shape replicated data. `ts-compatibility-mode` is consulted by the node
answering a read, so it must be set identically on every node of a replication group.
```

- [ ] **Step 5: Run, compat, commit**

Run: `TEST_PATTERN="test_replication_policy or test_replication_compaction_policy_rules or test_ts_add or test_ts_config or test_ts_debug" SERVER_VERSION=unstable ./build.sh` — Expected: PASS.
Run: `RTS_COMPAT=1 TEST_PATTERN="create" SERVER_VERSION=unstable ./build.sh` — Expected: PASS.

```bash
git add src/series/utils.rs docs/overview.md tests/test_ts_replication.py
git commit -m "fix(replication): replicate policy-created compaction destinations from the primary"
```

### Task 13: `dirty` — a failed rule application heals on the next write

**Files:**
- Modify: `src/series/compaction/mod.rs` (`CompactionRule`: field, `GetSize`, RDB; `rebuild_open_bucket_as_of`; counters; tests)
- Modify: `src/series/compaction/cascade.rs` (`apply_rules_internal`; fault hook)
- Modify: `src/series/compaction/engine.rs` (`bench_rule` literal; test `fresh_rule` literal)
- Modify: `src/commands/ts_createrule.rs` (`parse_args` literal), `src/series/compaction_policy.rs` (literal)
- Modify: `src/commands/ts_info.rs` (`get_rules_state` fifth element)
- Modify: `src/commands/ts_debug.rs` (`FAIL_NEXT_COMPACTION` subcommand + help)
- Modify: `src/common/module_info.rs` (`compaction` INFO section)
- Modify: `tests/test_compactions_add.py` (Task 1's expectations)
- Create: `tests/test_compaction_failure_recovery.py`

**Interfaces:**
- Produces: `CompactionRule.dirty: bool` (persisted, not hashed into `DEBUG DIGEST-VALUE` — it is node-local); `pub(super) fn rebuild_open_bucket_as_of(parent: &TimeSeries, rule: &mut CompactionRule, as_of: Option<Timestamp>)`; `pub static COMPACTION_FAILURES: AtomicU64`, `pub static COMPACTION_STATE_REBUILDS: AtomicU64` (INFO `ts_compaction_failures`, `ts_compaction_state_rebuilds`); `pub(crate) fn fail_next_compaction()`; `rulesState` entries become `[destKey, bucketStart, hasSamples, current, dirty]`.

- [ ] **Step 1: Write the failing tests**

`src/series/compaction/mod.rs`, tests module:

```rust
    use super::*;
    use crate::aggregators::AggregationType;
    use crate::common::Sample;

    fn rule(agg: AggregationType, bucket: u64) -> CompactionRule {
        CompactionRule {
            dest: SeriesLink::from("dst"),
            aggregator: agg.into(),
            bucket_duration: bucket,
            align_timestamp: 0,
            bucket_start: None,
            has_samples: false,
            dirty: false,
        }
    }

    fn series_with(samples: &[(Timestamp, f64)]) -> TimeSeries {
        let mut series = TimeSeries::new();
        for &(ts, value) in samples {
            assert!(series.add(ts, value, None).is_ok(), "fixture add {ts}");
        }
        series
    }

    #[test]
    fn rebuild_stops_at_as_of_so_the_pending_append_is_not_counted_twice() {
        // 1700 is the current write's own append, already stored; the append path streams it next.
        let series = series_with(&[(1000, 9.0), (1200, 3.0), (1700, 100.0)]);
        let mut rebuilt = rule(AggregationType::Sum, 1000);
        rebuild_open_bucket_as_of(&series, &mut rebuilt, Some(1200));
        assert_eq!(rebuilt.bucket_start, Some(1000));
        assert!(rebuilt.has_samples);
        assert_eq!(AggregationHandler::current(&rebuilt.aggregator), Some(12.0));
    }

    #[test]
    fn rebuild_for_an_empty_source_clears_the_bucket() {
        let series = series_with(&[(1000, 9.0)]);
        let mut rebuilt = rule(AggregationType::Sum, 1000);
        rebuilt.bucket_start = Some(1000);
        rebuilt.update(1000, 9.0);
        rebuild_open_bucket_as_of(&series, &mut rebuilt, None);
        assert_eq!(rebuilt.bucket_start, None);
        assert!(!rebuilt.has_samples);
    }
```

(`SeriesLink` and `AggregationHandler` are already imported at the top of `mod.rs`; add them to the test's `use` line if the compiler says otherwise.)

`tests/test_compaction_failure_recovery.py`:

```python
"""A rule application that fails leaves the rule `dirty`; the next write rebuilds the open bucket
from the source before applying. Driven by `TS._DEBUG FAIL_NEXT_COMPACTION` (debug mode only)."""

from valkeytestframework.conftest import resource_port_tracker
from valkey_timeseries_test_case import ValkeyTimeSeriesTestCaseDebugMode


class TestCompactionFailureRecovery(ValkeyTimeSeriesTestCaseDebugMode):
    @staticmethod
    def info_debug(client, key):
        flat = client.execute_command("TS.INFO", key, "DEBUG")
        return {flat[i]: flat[i + 1] for i in range(0, len(flat), 2)}

    def counters(self):
        info = self.client.info("ts_compaction")
        return info["ts_compaction_failures"], info["ts_compaction_state_rebuilds"]

    def make_rule(self, src, dst):
        c = self.client
        c.execute_command("TS.CREATE", src)
        c.execute_command("TS.CREATE", dst)
        c.execute_command("TS.CREATERULE", src, dst, "AGGREGATION", "sum", 1000)

    def test_next_write_rebuilds_a_dirty_rule(self):
        c = self.client
        self.make_rule("f:src", "f:dst")
        c.execute_command("TS.ADD", "f:src", 0, 1)
        failures, rebuilds = self.counters()

        c.execute_command("TS._DEBUG", "FAIL_NEXT_COMPACTION")
        assert c.execute_command("TS.ADD", "f:src", 300, 2) == 300   # stored; the rule misses it
        assert self.counters() == (failures + 1, rebuilds)
        assert self.info_debug(c, "f:src")[b"rulesState"][0][4] == 1

        c.execute_command("TS.ADD", "f:src", 600, 4)   # rebuilds bucket 0 from the source first
        assert self.counters() == (failures + 1, rebuilds + 1)
        assert self.info_debug(c, "f:src")[b"rulesState"][0] == [b"f:dst", 0, 1, b"7", 0]
        c.execute_command("TS.ADD", "f:src", 1000, 8)
        assert c.execute_command("TS.RANGE", "f:dst", "-", "+") == [[0, b"7"]]

    def test_dirty_survives_reload(self):
        """Review focus 5."""
        c = self.client
        self.make_rule("r:src", "r:dst")
        c.execute_command("TS.ADD", "r:src", 0, 1)
        c.execute_command("TS._DEBUG", "FAIL_NEXT_COMPACTION")
        c.execute_command("TS.ADD", "r:src", 300, 2)
        c.execute_command("DEBUG", "RELOAD")
        assert self.info_debug(c, "r:src")[b"rulesState"][0][4] == 1
        c.execute_command("TS.ADD", "r:src", 1000, 8)  # rebuilds (1 + 2), then closes bucket 0
        assert c.execute_command("TS.RANGE", "r:dst", "-", "+") == [[0, b"3"]]
        assert self.info_debug(c, "r:src")[b"rulesState"][0][4] == 0

    def test_a_removal_clears_dirty(self):
        c = self.client
        self.make_rule("d:src", "d:dst")
        for ts in (0, 300):
            c.execute_command("TS.ADD", "d:src", ts, 1)
        c.execute_command("TS._DEBUG", "FAIL_NEXT_COMPACTION")
        c.execute_command("TS.ADD", "d:src", 600, 1)
        c.execute_command("TS.DEL", "d:src", 300, 300)   # its resync rebuilds the open bucket
        assert self.info_debug(c, "d:src")[b"rulesState"][0] == [b"d:dst", 0, 1, b"2", 0]
```

Update Task 1's `test_info_debug_reports_rule_state` in `tests/test_compactions_add.py`: the three expected entries become `[dst.encode(), None, 0, None, 0]`, `[dst.encode(), 0, 1, b"3.5", 0]` and `[dst.encode(), 1000, 1, b"7", 0]`.

- [ ] **Step 2: Run them to verify they fail**

Run: `cargo test --features enable-system-alloc rebuild_ 2>&1 | tail -3` — Expected: compile error (no field `dirty`, no `rebuild_open_bucket_as_of`).

- [ ] **Step 3: Add the field and its persistence**

`src/series/compaction/mod.rs`, `CompactionRule`:

```rust
    pub has_samples: bool,
    /// Set when the rule's last application returned an error, so its open-bucket state may not
    /// reflect every sample the source stored. The next `Ingest` rebuilds the open bucket from the
    /// source first (`rebuild_open_bucket_as_of`); a `RemoveRange` rebuilds it anyway. Node-local:
    /// persisted, so a restart does not forget it, but not hashed into the digest, because a
    /// replica's application of the same write may well have succeeded.
    pub dirty: bool,
```

`GetSize`: add `+ size_of::<bool>() // dirty`. `rdb_save`: after `rdb_save_bool(rdb, self.has_samples);` add `rdb_save_bool(rdb, self.dirty);`. `rdb_load`: after `let has_samples = rdb_load_bool(rdb)?;` add `let dirty = rdb_load_bool(rdb)?;` and `dirty,` in the struct literal. (In place: the encoding version stays `1`.)

Add `dirty: false,` to the struct literals in `src/commands/ts_createrule.rs::parse_args`, `src/series/compaction_policy.rs::create_compaction_rules`, `engine.rs::bench_rule` and the `fresh_rule` test helper in `engine.rs`.

`src/series/compaction/mod.rs`, after `impl CompactionRule`:

```rust
/// Rebuild `rule`'s open-bucket state from the source as it stood when its last timestamp was
/// `as_of`: the bucket containing `as_of`, aggregated over the stored samples in that bucket at or
/// below `as_of`. Anything above it is the current write's own append, already stored, which the
/// engine is about to stream in. `None` means the source was empty: no open bucket.
///
/// An approximation of the state a rule maintains by streaming — exact for every aggregator whose
/// bucket value depends only on the bucket's samples, but `rate`/`increase` and a retention window
/// shorter than a bucket can differ — so it runs only to recover a `dirty` rule.
pub(super) fn rebuild_open_bucket_as_of(
    parent: &TimeSeries,
    rule: &mut CompactionRule,
    as_of: Option<Timestamp>,
) {
    rule.reset();
    let Some(as_of) = as_of else {
        return;
    };
    let (start, end) = rule.get_bucket_range(as_of);
    rule.bucket_start = Some(start);
    for sample in parent.range_iter(start, end - 1) {
        if sample.timestamp > as_of {
            break;
        }
        rule.update(sample.timestamp, sample.value);
    }
}

/// Rule applications that returned an error (`INFO ts_compaction` → `ts_compaction_failures`).
/// Non-zero means a destination may be missing a bucket its source should have produced.
pub static COMPACTION_FAILURES: AtomicU64 = AtomicU64::new(0);
/// Open-bucket rebuilds of `dirty` rules (`ts_compaction_state_rebuilds`).
pub static COMPACTION_STATE_REBUILDS: AtomicU64 = AtomicU64::new(0);
```

with `use std::sync::atomic::AtomicU64;` at the top.

- [ ] **Step 4: Rebuild before, mark after; fault hook**

`src/series/compaction/cascade.rs`:

```rust
use std::sync::atomic::{AtomicBool, Ordering as AtomicOrdering};

/// Set by `TS._DEBUG FAIL_NEXT_COMPACTION` (debug mode only): the next rule application fails
/// without running, exactly as an internal error would leave it.
static FAIL_NEXT_COMPACTION: AtomicBool = AtomicBool::new(false);

pub(crate) fn fail_next_compaction() {
    FAIL_NEXT_COMPACTION.store(true, AtomicOrdering::Relaxed);
}
```

In `apply_rules_internal`, the map closure becomes:

```rust
        .map(|(rule_index, rule, dest_guard)| {
            let rule_index = *rule_index;
            let dest_prev_last = dest_guard.last_sample.map(|s| s.timestamp);
            if rule.dirty {
                if let CompactionOp::Ingest { prev_last, .. } = op {
                    rebuild_open_bucket_as_of(series, rule, prev_last);
                }
                COMPACTION_STATE_REBUILDS.fetch_add(1, AtomicOrdering::Relaxed);
            }
            let (result, written) = if FAIL_NEXT_COMPACTION.swap(false, AtomicOrdering::Relaxed) {
                (
                    Err(TsdbError::General("injected compaction failure".to_string())),
                    Vec::new(),
                )
            } else {
                let mut cctx = CompactionContext::new(series, dest_guard, rule);
                let result = apply_op(&mut cctx, op);
                (result, cctx.written)
            };
            rule.dirty = result.is_err();
            result.map(|_| RuleOutcome {
                rule_index,
                dest_prev_last,
                written,
            })
        })
```

and in the error loop below it, beside `log_warning(msg);`, add `COMPACTION_FAILURES.fetch_add(1, AtomicOrdering::Relaxed);`. Import `rebuild_open_bucket_as_of`, `COMPACTION_FAILURES`, `COMPACTION_STATE_REBUILDS` from `super`.

`src/commands/ts_debug.rs` — beside `PANIC_NEXT_ANALYSIS_JOB` in the dispatch:

```rust
        "FAIL_NEXT_COMPACTION" => {
            itr.done()?;
            crate::series::fail_next_compaction();
            reply_with_str(ctx, "OK");
            Ok(())
        }
```

and in `HELP_TEXT`:

```rust
        (
            "TS._DEBUG FAIL_NEXT_COMPACTION",
            "Make the next compaction rule application fail (tests dirty-rule recovery)",
        ),
```

(`fail_next_compaction` reaches `crate::series` through `pub(crate) use cascade::fail_next_compaction;` in `compaction/mod.rs`.)

`src/commands/ts_info.rs`, `get_rules_state`: append `ValkeyValue::Integer(i64::from(rule.dirty)),` as the fifth element and document it.

`src/common/module_info.rs` — before `.build_info()?`:

```rust
        .add_section("compaction")
        .field(
            "compaction_failures",
            COMPACTION_FAILURES.load(std::sync::atomic::Ordering::Relaxed),
        )?
        .field(
            "compaction_state_rebuilds",
            COMPACTION_STATE_REBUILDS.load(std::sync::atomic::Ordering::Relaxed),
        )?
        .build_section()?
```

with `use crate::series::{COMPACTION_FAILURES, COMPACTION_STATE_REBUILDS};`.

- [ ] **Step 5: Run**

Run: `cargo test --features enable-system-alloc` — Expected: PASS.
Run: `TEST_PATTERN="test_compaction_failure_recovery or test_info_debug_reports_rule_state or compaction or test_ts_restore_malformed or test_dump_restore or test_ts_aofrewrite or test_replication_compaction_ or test_rule_state_survives_reload" SERVER_VERSION=unstable ./build.sh` — Expected: PASS.

- [ ] **Step 6: Lint and commit**

```bash
git add src/series src/commands/ts_createrule.rs src/commands/ts_info.rs src/commands/ts_debug.rs src/common/module_info.rs tests/test_compactions_add.py tests/test_compaction_failure_recovery.py
git commit -m "feat(compaction): dirty rules rebuild their open bucket; INFO failure counters"
```

---

## Part 3 — record the outcome

### Task 14: Docs, proposals, and the superseded plan

**Files:**
- Modify: `docs/overview.md` (the *Replication* subsection Task 12 added)
- Modify: `docs/plans/compaction-replication-plan.md` (status, §5, §6)
- Modify: `docs/plans/2026-10-14-compaction-effects-replication.md` (superseded banner)
- Modify: `docs/plans/proposal-complete.md`, `docs/plans/proposal.md`

- [ ] **Step 1: Operational rule for version skew**

Append to the *Replication* subsection of `docs/overview.md`:

```markdown
Compaction is recomputed on every node from the replicated source writes, as in
RedisTimeSeries: destinations are not shipped over the replication link. A replica therefore
produces exactly its primary's destination values only while both run the same aggregation
arithmetic. When a release note says a release changes aggregation results (it will say so
explicitly), upgrade the primary and its replicas together and full-resync the replicas
afterwards (`REPLICAOF NO ONE` then `REPLICAOF <primary>` on each), or accept that rollups
written during the mixed-version window may differ in their last digits between nodes.
```

- [ ] **Step 2: Analysis and superseded plan**

`docs/plans/compaction-replication-plan.md`: set the status line to "Status: Approach 1 adopted and implemented (`docs/plans/2026-10-14-compaction-refactor.md`, branch `compaction-refactor`): recompute on every node, DIV-0023 emulation dropped, inputs pinned, dirty-rule recovery." Under §5 add a closing paragraph: "Adopted: Option A, extended by the compaction refactor — `last_forward_close` removed (DIV-0023 ungated), one ingest operation, module split, `dirty` instead of `applied_through`. Option C was not taken." Mark each §6 item with the task that landed it (0.1 → Task 1, 0.2 → Task 2, 0.3 → Task 3, 0.4 → Task 4, 0.5/0.6 → Task 5, 0.7 → Task 13, 0.8 → Task 12; 2.1 → obsolete, the marker was removed in Task 6; 2.2 → Task 12; 2.3 → Task 11; 2.4 → Task 13; Phase 3 → not taken).

`docs/plans/2026-10-14-compaction-effects-replication.md`: insert directly under the title:

```markdown
> **Superseded** by `docs/plans/2026-10-14-compaction-refactor.md` (Approach 1). Kept for the
> record of Option C's design; do not execute.
```

- [ ] **Step 3: Proposals**

`docs/plans/proposal-complete.md` §3.5 table: the `Compaction downstream writes` row becomes "Recomputed on every node from the source write, as in the reference; rule state persisted with the source | Implemented, replica-tested (equivalence suite, Hypothesis oracle, reload, promotion); failed applications heal via `dirty`". Add a row "Policy-created destinations / auto-create defaults | Replicated as explicit `TS.CREATE`/`TS.CREATERULE` with the primary's resolved options | Implemented, replica-tested". Replace the prose paragraph after the table to name the suite (`test_replication_compaction_*`, `test_replication_compaction_random_sequences`, reload/promotion, `test_compaction_failure_recovery.py`) and state that the `*`-timestamp path is the remaining untested propagation path. Summary table, Replication row: "MDEL, INCRBY/DECRBY, ADDBULK, compaction recompute, policy destinations and auto-create defaults replica-tested; retention-trim cron runs per node (floor follows the last sample; pinned by a test)". Readiness criterion 4: replace the compaction sentences with the recompute model and the version-skew operational rule; in criterion 9 (or wherever `ts.ts-compatibility-mode` is described) change "it gates exactly DIV-0014 and DIV-0023" to "it gates DIV-0014; DIV-0023 is a permanent divergence since 2026-10-14".

`docs/plans/proposal.md`, Replication row: coverage gains "compaction recompute (equivalence suite), policy destinations, auto-create defaults"; gap becomes "`*`-timestamp propagation has no dedicated replica test; aggregation-arithmetic changes across versions need a replica resync (documented)." Priority 4: drop "compaction" and "retention trim".

- [ ] **Step 4: Commit**

```bash
git add docs/overview.md docs/plans/compaction-replication-plan.md docs/plans/2026-10-14-compaction-effects-replication.md docs/plans/proposal-complete.md docs/plans/proposal.md
git commit -m "docs(plans): record the compaction refactor and the recompute replication model"
```

---

## Self-review (done while writing; kept for the executor)

- **Design coverage:** recompute model → Parts 0 and 2 (no effects machinery anywhere); DIV-0023 ungated → Task 6; module split → Task 7; one ingest path with the benchmark gate → Tasks 8–10; pinned inputs → Tasks 11–12; `dirty` recovery and counters → Task 13; version-skew rule → Task 14; superseded plan → Task 14.
- **Type consistency:** `CompactionOp::AddBatch` exists through Task 9 and becomes `CompactionOp::Ingest` in Task 10, which renames every use, including `runs_rules_in_parallel` (Task 9) and `bench_ingest_one` (Tasks 8–9). Task 13's code uses `Ingest`. `rulesState` is 4-wide from Task 1 to Task 12 and 5-wide (`dirty`) from Task 13, which updates Task 1's expectations. `CompactionRule` literals gain `dirty: false` in Task 13 at every site Tasks 7–8 created (`bench_rule`, `fresh_rule`) plus the two production sites.
- **Review Focus:** 1 → Task 8 Step 1 (four compat cases) and Task 9 Step 4; 2 → Task 9 `a_single_sample_never_runs_rules_in_parallel`; 3 → Task 9 `test_incrby_at_the_last_timestamp_recalculates_the_bucket`; 4 → Task 9 `test_ignored_sample_runs_no_compaction`; 5 → Task 13 `test_dirty_survives_reload`.
