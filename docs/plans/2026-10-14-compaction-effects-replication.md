# Compaction Effects Replication Implementation Plan

> **Superseded** by `docs/plans/2026-10-14-compaction-refactor.md` (Approach 1). Kept for the
> record of Option C's design; do not execute.

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make a replica's compaction destinations correct by construction — the primary replicates every destination effect it computes — and make the per-rule state that was hidden persisted, observable, and self-validating.

**Architecture:** Part 0 adds observability (`TS.INFO … DEBUG` rule state, a logical-only `DEBUG DIGEST-VALUE`) and a primary/replica equivalence suite, all green against today's recompute model. Part 2 pins every snapshot- and creation-time input: the strict-mode marker is persisted, auto-created series and policy-created destinations are replicated with the primary's resolved options, and each rule carries `applied_through` so a stale open bucket is rebuilt from the source. Part 3 adds the internal `TS._COMPACT` command carrying destination effects (`ADD`/`DEL`/`MARK`), stops `apply_compaction` on replicated and AOF-applied writes, and derives `LATEST` on replicas from the source. Part 4 updates the proposal.

**Tech Stack:** Rust (valkey-module 0.1.14, edition 2024, MSRV 1.96); Python pytest + the cloned `valkeytestframework` (+ Hypothesis, already a test dependency) driven by `./build.sh`.

**Spec:** `docs/plans/compaction-replication-plan.md` — §3 (divergence classes), §3.7 (reference behavior), §5 Option C, §6 phases. The task numbering below maps to §6 as: Part 0 = Phase 0, Part 2 = Phase 2 (A.1–A.4), Part 3 = Phase 3, Part 4 = Phase 4.

**Scope note (2026-10-14, after the reference probe):** the reference recomputes on its replica and persists its open-bucket state and cached last-sample (spec §3.7). Parts 0–2 therefore reach the reference's architecture and close every divergence found; they are the committed scope. Part 3 (effects replication, Option C) is conditional on the checkpoint after Part 2 — take it for version-skew safety and failure reconciliation, not for parity. Task 15 applies either way.

**Deviations from the spec, decided while planning (each explained at the task):**
1. §6 0.5 asks for a Rust property test over every aggregator's RDB round trip. There is no `RedisModuleIO` mock in the crate, so Task 5 does it as an integration test through `DEBUG RELOAD`, comparing the observable rule state and the bucket a reloaded rule publishes against a twin fed the same samples after the reload.
2. §5 A.2 asks to "fix the destination-exists → rule dropped branch". Adopting an existing destination would need `TS.CREATERULE` to accept a destination that already names the source — an accepted-input superset on the RTS surface that the compat gate hard-fails ("reference errors, subject succeeds" cannot be registered, `AGENTS.md`). Task 8 keeps the skip, makes it the same on every node (the replica never applies the policy itself), and keeps the warning that names both keys.
3. `DEBUG DIGEST-VALUE` stops hashing the open-bucket accumulator and `bucket_start` (Task 1). Under Option C a replica's rule state is frozen at its last snapshot by design, so a digest that includes it could never be equal across nodes; the digest becomes "logical data + rule definitions", which is what `DEBUG DIGEST-VALUE` is for. The open-bucket state is compared through `TS.INFO … DEBUG` instead.

## Global Constraints

- Clean-room rule: never consult, fetch, or reproduce RedisTimeSeries source or tests; derive behavior from public docs and black-box observation of the pinned `redis:8.10` image only (`AGENTS.md`, `tests/compat/README.md`).
- Lint/build gate, run before every commit that touches Rust: `cargo fmt --check && cargo clippy --profile release --all-targets -- -D clippy::all && RUSTFLAGS="-D warnings" cargo build --all --all-targets --release`.
- Rust unit tests: `cargo test --features enable-system-alloc` (the feature is mandatory outside a live server, or the binary aborts at startup).
- Integration tests: `TEST_PATTERN="<pytest -k expression>" SERVER_VERSION=unstable ./build.sh` (builds the module and `valkey-server`, then runs pytest). `MODULE_PATH` is exported after the build.
- Compat gate for anything that can change a reply on the shared RTS surface: `RTS_COMPAT=1 TEST_PATTERN="<expr>" SERVER_VERSION=unstable ./build.sh` (needs Docker). Deliberate differences go in `tests/compat/divergences.yml`.
- Commit messages: Conventional Commits `type(scope): summary`; no `Co-Authored-By` trailer (user preference).
- Command registration: user-facing commands use `#[valkey_module_macros::command]` plus an `acl_categories!` line; internal commands (`ts._debug`, `ts._restore`, and the new `ts._compact`) go in the positional `commands:` table in `src/lib.rs` and stay undocumented (no `docs/COMMANDS.md` entry).
- Threading rules R1–R5 (`src/common/threads/mod.rs`): compaction runs on the command thread; never call `Context` methods (`replicate`, `log_*`, key opens) from inside a `par_mut_rayon` closure.
- Encoding version: no release has shipped, so the series payload format changes **in place**. `TIMESERIES_TYPE_ENCODING_VERSION` and `MIN_TIMESERIES_TYPE_ENCODING_VERSION` stay at `1`, new fields are written and read unconditionally (no `enc_ver` branches), and `tests/test_ts_restore_malformed.py::CURRENT_ENCVER` stays `1`. Consequence: an RDB, AOF or `DUMP` payload written by a build from before Task 6 (and again before Task 9) no longer loads — the loader fails on the missing trailing field, or misreads the next key in a multi-key RDB. Delete local `dump.rdb`/`appendonlydir` files when switching builds; this is acceptable only because nothing has been released.
- Every task ends with the relevant tests green and a commit; a task that changes RTS-visible replies also runs the compat gate.

## Review Focus

Inputs the spec implies but no existing test exercises, most likely to bite first. Each is pinned by a test in the task named.

1. **A `TS._COMPACT` for a destination the replica does not have** (a diverged keyspace, or a destination deleted by a replicated `DEL` that raced nothing — ordering makes this impossible in a healthy stream, so it signals corruption). Expected: a reply error that the replica logs, never a crash. → Task 11.
2. **A bucket value of `NaN`, `+inf` or `-inf` crossing the stream** (aggregators produce them: `avg` of an empty filter, `rate` over a reset). Expected: bit-exact on the replica. → Task 11 (`to_bits` transport).
3. **A compacting write inside `MULTI`/`EXEC` or a Lua script on the primary.** Expected: the effects stay inside the same transaction so no reader on the replica sees the source advanced and the destination lagging. → Task 13 (MONITOR shows `MULTI … EXEC`).
4. **`TS.DEL` over a vast range on a fine-grained rule** (the DoS case the engine already bounds). Expected: a bounded number of effects — at most one `DEL` for the middle, two recalculations and one `MARK` per rule per level — never one per bucket. → Task 13.
5. **Promotion with the very next write landing in the snapshot's open bucket.** Expected: the rebuilt accumulator excludes that write before the append path streams it, so the bucket closes with the exact value and nothing is counted twice. → Task 14.

---

## Part 0 — observe, pin, test (no behavior change on the primary)

### Task 1: Expose rule state in `TS.INFO … DEBUG`; make the digest logical

**Files:**
- Modify: `src/commands/ts_info.rs` (`INFO_FIELD_ORDER` ~line 72; `get_ts_info` debug branch ~line 215)
- Modify: `src/series/digest.rs` (`calc_compaction_digest`)
- Test: `tests/test_compactions_add.py` (class `TestCompactionAdd`)

**Interfaces:**
- Produces: `TS.INFO key DEBUG` gains `rulesState` — an array with one `[destKey, bucketStart|nil, hasSamples (0/1), current|nil]` entry per rule in rule order — and `forwardClose` (the DIV-0023 marker's bucket timestamp, or nil). Task 9 appends `appliedThrough` as a fifth element. Every replication test from Task 2 on reads these.
- Produces: `DEBUG DIGEST-VALUE` of a source no longer changes with the open-bucket accumulator; two series with identical samples, labels, settings and rule *definitions* hash equal.

- [ ] **Step 1: Write the failing tests**

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
        assert self.info_debug(self.client, dst)[b"forwardClose"] is None

        self.add_sample(src, 100, 1.5)
        self.add_sample(src, 200, 2.0)
        assert self.info_debug(self.client, src)[b"rulesState"] == [[dst.encode(), 0, 1, b"3.5"]]

        self.add_sample(src, 1000, 7.0)  # closes bucket 0
        assert self.info_debug(self.client, src)[b"rulesState"] == [[dst.encode(), 1000, 1, b"7"]]
        assert self.info_debug(self.client, dst)[b"forwardClose"] == 0

    def test_digest_ignores_open_bucket_state(self):
        """Two sources with the same samples and the same rule definition hash equal even though
        one rule has seen every sample and the other was created afterwards (different
        accumulator, different bucketStart). The digest is logical state only."""
        c = self.client
        for name in ("early", "late"):
            c.execute_command("TS.CREATE", f"dg:{name}:src")
            c.execute_command("TS.CREATE", f"dg:{name}:dst")
        c.execute_command("TS.CREATERULE", "dg:early:src", "dg:early:dst", "AGGREGATION", "avg", 1000)
        for name in ("early", "late"):
            for ts, value in ((0, 1), (300, 2.5), (450, 7)):
                c.execute_command("TS.ADD", f"dg:{name}:src", ts, value)
        c.execute_command("TS.CREATERULE", "dg:late:src", "dg:late:dst", "AGGREGATION", "avg", 1000)
        assert self.info_debug(c, "dg:early:src")[b"rulesState"] != self.info_debug(c, "dg:late:src")[b"rulesState"]
        # The rule names a different destination key on each side, so point both at the same name
        # for the comparison: rename the destinations to a common key one at a time.
        early = c.execute_command("DEBUG", "DIGEST-VALUE", "dg:early:src")
        c.execute_command("RENAME", "dg:early:dst", "dg:common:dst")
        early_common = c.execute_command("DEBUG", "DIGEST-VALUE", "dg:early:src")
        c.execute_command("RENAME", "dg:common:dst", "dg:early:dst")
        c.execute_command("RENAME", "dg:late:dst", "dg:common:dst")
        late_common = c.execute_command("DEBUG", "DIGEST-VALUE", "dg:late:src")
        assert early != early_common          # the destination key is part of the rule definition
        assert early_common == late_common    # the accumulator and bucketStart are not
```

- [ ] **Step 2: Run them to verify they fail**

Run: `TEST_PATTERN="test_info_debug_reports_rule_state or test_digest_ignores_open_bucket_state" SERVER_VERSION=unstable ./build.sh`
Expected: both FAIL — `KeyError: b'rulesState'` and `assert early_common == late_common` (digests differ today because the accumulator is hashed).

- [ ] **Step 3: Add the DEBUG fields**

In `src/commands/ts_info.rs`, add the import and extend the order list:

```rust
use crate::aggregators::AggregationHandler;
```

```rust
    "rounding",
    "keySelfName",
    "rulesState",
    "forwardClose",
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
        map.insert(
            "forwardClose".into(),
            ts.last_forward_close
                .map_or(ValkeyValue::Null, |sample| ValkeyValue::Integer(sample.timestamp)),
        );
        // yes, I know its title case, but that's what redis does. Written by
        // `reply_with_chunks_info`; only the key is needed here, for its place in the reply.
        map.insert("Chunks".into(), ValkeyValue::Null);
    }
```

- [ ] **Step 4: Make the digest logical**

In `src/series/digest.rs`, replace `calc_compaction_digest`:

```rust
/// A rule's *definition*: destination, geometry and aggregator kind (with its condition, for the
/// filtered aggregators). Deliberately not the open-bucket accumulator or `bucket_start`: those are
/// a cache of the source's samples that a replica does not maintain (its destinations come from
/// the primary as `TS._COMPACT` effects), so hashing them would make the same logical series
/// digest differently on the two nodes. `TS.INFO … DEBUG rulesState` exposes them instead.
pub(super) fn calc_compaction_digest(rule: &CompactionRule, digest: &mut Digest) {
    calc_link_digest(Some(&rule.dest), digest);
    digest.add_long_long(rule.bucket_duration as i64);
    digest.add_long_long(rule.align_timestamp);
    let mut definition = rule.aggregator.clone();
    AggregationHandler::reset(&mut definition);
    calc_aggregator_digest(&definition, digest);
}
```

and add `use crate::aggregators::AggregationHandler;` to the imports.

- [ ] **Step 5: Run the tests and the digest-dependent suites**

Run: `TEST_PATTERN="test_info_debug_reports_rule_state or test_digest_ignores_open_bucket_state or test_dump_restore or test_ts_aofrewrite or test_ts_save_and_restore" SERVER_VERSION=unstable ./build.sh`
Expected: all PASS (the existing digest round-trip tests compare a series with itself, so a narrower digest still matches).

- [ ] **Step 6: Compat check for the new DEBUG fields**

Run: `RTS_COMPAT=1 TEST_PATTERN="info" SERVER_VERSION=unstable ./build.sh`
Expected: PASS, or a shape delta on `TS.INFO … DEBUG` naming `rulesState`/`forwardClose`. In the latter case add an entry to `tests/compat/divergences.yml` of the same kind as the one that registers the module-only `TS.INFO` fields `encoding` and `metric` (find it with `grep -n -B3 -A8 "metric" tests/compat/divergences.yml`), with `command: TS.INFO`, a description naming the two DEBUG-only fields, and `since: 0.1.0`; re-run until green.

- [ ] **Step 7: Lint and commit**

Run the lint gate, then:
```bash
git add src/commands/ts_info.rs src/series/digest.rs tests/test_compactions_add.py tests/compat/divergences.yml
git commit -m "feat(info): expose compaction rule state in TS.INFO DEBUG; digest logical state only"
```

### Task 2: Replica-oracle equivalence suite

**Files:**
- Test: `tests/test_ts_replication.py` (class `TestTimeSeriesReplication`)

**Interfaces:**
- Produces helpers used by every later replication test: `info_debug(client, key)`, `set_compat_mode(mode)`, `rule(src, dst, agg, bucket, align=None, src_options=())`, `assert_compaction_equivalent(*keys)`, `assert_rule_state_equivalent(*keys)`, `assert_marker_equivalent(*keys)`, `force_full_resync()`, `wait_link_up(client)`.

- [ ] **Step 1: Add the helpers**

Below `assert_series_matches_replica` in `tests/test_ts_replication.py`:

```python
    # --- compaction equivalence helpers (docs/plans/2026-10-14-compaction-effects-replication.md)

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
        True while the replica recomputes; Task 13 stops that, and removes these calls."""
        replica = self.replicas[0].client
        for key in keys:
            assert self.info_debug(replica, key)[b"rulesState"] == \
                self.info_debug(self.client, key)[b"rulesState"], key

    def assert_marker_equivalent(self, *keys):
        """The strict-mode marker (DIV-0023) agrees. Separate from the data check because the
        marker is runtime-only until Task 6 persists it."""
        replica = self.replicas[0].client
        for key in keys:
            assert self.info_debug(replica, key)[b"forwardClose"] == \
                self.info_debug(self.client, key)[b"forwardClose"], key

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
- Produces: `promote_replica()` → `(new_primary_client, new_replica_client)`, `wait_caught_up(primary, replica)`, `assert_equivalent_between(a, b, *keys)`. Task 14 re-uses them.

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

## Checkpoint — Phase 1 decision

Parts 0 and 2 are the same under Option A and Option C, and Part 2 is required under either; proceed with it regardless. Before Part 3, decide with the owner whether Option C is still wanted: the reference's model is recompute (spec §3.7), so Part 3 buys version-skew safety and reconciliation of node-local compaction failures, at the cost of a stream/AOF mechanism the reference does not have. Inputs: Part 0 and Part 2 results, and — if Part 3 is explored — the stream-cost measurement from Task 13 Step 6. If the decision is A, skip to Task 15.

---

## Part 2 — pin snapshot and creation inputs

### Task 6: Persist the strict-mode marker

**Files:**
- Modify: `src/series/serialization.rs` (`rdb_save_series` tail, `rdb_load_series` tail)
- Modify: `src/series/time_series.rs` (`last_forward_close` doc comment, ~lines 100–113)
- Modify: `tests/compat/divergences.yml` (DIV-0023 description)
- Test: `tests/test_ts_replication.py`, `tests/test_compactions_add.py`

- [ ] **Step 1: Write the failing tests**

`tests/test_ts_replication.py`:

```python
    def test_replication_strict_marker_survives_full_sync(self):
        """DIV-0023 marker: strict-mode TS.GET on a destination must agree after a full sync, which
        ships the marker in the RDB — it is not derivable from the stored samples (the
        destination holds 0 and 500, the open bucket is 1000, and the marker is 0)."""
        self.set_compat_mode("strict")
        src, dst = "ts:marker:src", "ts:marker:dst"
        self.rule(src, dst, "avg", 500)
        for ts in (0, 1000, 500):       # 500 back-fills bucket 500: the marker stays at 0
            self.client.execute_command("TS.ADD", src, ts, 0)
        assert self.client.execute_command("TS.GET", dst) == [0, b"0"]
        self.force_full_resync()
        assert self.replicas[0].client.execute_command("TS.GET", dst) == [0, b"0"]
        self.assert_compaction_equivalent(src, dst)
        self.assert_marker_equivalent(dst)
```

`tests/test_compactions_add.py`:

```python
    def test_strict_marker_survives_reload(self):
        c = self.client
        c.execute_command("CONFIG", "SET", "ts.ts-compatibility-mode", "strict")
        try:
            c.execute_command("TS.CREATE", "mk:src")
            c.execute_command("TS.CREATE", "mk:dst")
            c.execute_command("TS.CREATERULE", "mk:src", "mk:dst", "AGGREGATION", "avg", 500)
            for ts in (0, 1000, 500):
                c.execute_command("TS.ADD", "mk:src", ts, 0)
            assert c.execute_command("TS.GET", "mk:dst") == [0, b"0"]
            c.execute_command("DEBUG", "RELOAD")
            assert c.execute_command("TS.GET", "mk:dst") == [0, b"0"]
            assert c.execute_command("TS.RANGE", "mk:dst", "-", "+") == [[0, b"0"], [500, b"0"]]
        finally:
            c.execute_command("CONFIG", "SET", "ts.ts-compatibility-mode", "extended")
```

Pin the reference's behavior too (spec §3.7: its cached last-sample survives a reload). In `tests/compat/test_compat_compaction.py`, add to `TestOutOfOrderBackfill`:

```python
    def test_marker_survives_reload_on_both_engines(self, strict_subject):
        """The reference's cached last-sample is part of its persisted state: after DEBUG RELOAD
        it still reports the bucket closed by forward progress, not the back-filled one."""
        diff = strict_subject
        _rule(diff, "c:mkr:src", "c:mkr:dst", "avg", 500)
        for ts in (0, 1000, 500):
            diff("TS.ADD", "c:mkr:src", ts, 0)
        diff("TS.GET", "c:mkr:dst")          # [0, 0] on both: the stale marker
        diff.reference.execute_command("DEBUG", "RELOAD")
        diff.subject.execute_command("DEBUG", "RELOAD")
        diff("TS.GET", "c:mkr:dst")          # reference keeps [0, 0]; we reported [500, 0] before
        diff("TS.RANGE", "c:mkr:dst", "-", "+")
```

- [ ] **Step 2: Run them to verify they fail**

Run: `TEST_PATTERN="test_replication_strict_marker_survives_full_sync or test_strict_marker_survives_reload" SERVER_VERSION=unstable ./build.sh`
Expected: both FAIL with `[500, b"0"]` on the synced/reloaded side (the spec's §3.6 reproduction).
Run: `RTS_COMPAT=1 TEST_PATTERN="test_marker_survives_reload_on_both_engines" SERVER_VERSION=unstable ./build.sh`
Expected: FAIL — `TS.GET` after reload: reference `[0, 0]`, subject `[500, 0]`.

- [ ] **Step 3: Serialize the marker**

The field is appended to the existing layout; the encoding version stays `1` (Global Constraints).

`src/series/serialization.rs`, end of `rdb_save_series`:

```rust
    for rule in series.rules.iter() {
        rule.rdb_save(rdb);
    }
    // The strict-mode forward-close marker (DIV-0023). Stored as the bucket timestamp; the
    // sample is re-read on load, exactly as `live_forward_close` re-reads it on every read.
    rdb_save_optional_marker(rdb, series.last_forward_close.is_some());
    if let Some(marker) = series.last_forward_close {
        rdb_save_timestamp(rdb, marker.timestamp);
    }
}
```

`rdb_load_series`, after `rules.shrink_to_fit();`:

```rust
    let forward_close = if load_optional_marker(rdb)? {
        Some(rdb_load_timestamp(rdb)?)
    } else {
        None
    };
```

and replace the struct's `last_forward_close: None,` comment with `last_forward_close: None, // set below, once the chunks exist to look the sample up in`, then after the struct literal:

```rust
    // The marker names a bucket the destination must still hold; a payload whose marker points at
    // a missing sample (hand-edited, or from a writer that trimmed after saving it) just loses it.
    ts.last_forward_close = forward_close.and_then(|t| ts.get_sample(t).ok().flatten());
```

`src/series/time_series.rs`: replace the last paragraph of the `last_forward_close` doc comment ("Like `_db`, this is runtime-only …") with:

```rust
    /// Persisted with the series (as the bucket timestamp), so a full sync, a restart and
    /// `TS._RESTORE` all carry it — as the reference does for its cached last-sample. A
    /// runtime-only marker made a reloaded or freshly synced destination report its true last
    /// sample while the primary kept reporting the marker.
```

`tests/compat/divergences.yml`, DIV-0023: if the description mentions the marker being lost on reload, replace that sentence with "The marker is persisted with the series, so a reload or replica agrees with the primary — and with RTS, whose cached last-sample also survives a reload."

- [ ] **Step 4: Run the tests and every persistence suite**

Run: `TEST_PATTERN="test_replication_strict_marker_survives_full_sync or test_strict_marker_survives_reload or test_ts_restore_malformed or test_dump_restore or test_rdb_corrupt_load or test_ts_rdb_load_limits or test_ts_save_and_restore or test_ts_aofrewrite or test_ts_compaction_links" SERVER_VERSION=unstable ./build.sh`
Expected: PASS. Then `cargo test --features enable-system-alloc` — Expected: PASS.

- [ ] **Step 5: Compat and commit**

Run: `RTS_COMPAT=1 TEST_PATTERN="compaction" SERVER_VERSION=unstable ./build.sh` — Expected: PASS, including `test_marker_survives_reload_on_both_engines` (the primary's replies are unchanged; the post-reload reply now matches the reference).

```bash
git add src/series/serialization.rs src/series/time_series.rs tests/compat/divergences.yml tests/compat/test_compat_compaction.py tests/test_ts_replication.py tests/test_compactions_add.py
git commit -m "fix(series): persist the strict-mode forward-close marker"
```

### Task 7: Auto-created series replicate the primary's resolved options

**Files:**
- Modify: `src/series/utils.rs` (`create_and_store_series`; new `resolved_create_args`)
- Test: `tests/test_ts_replication.py`

**Interfaces:**
- Produces: `pub(crate) fn resolved_create_args(ctx: &Context, series: &TimeSeries) -> Vec<ValkeyString>` — the `TS.CREATE` option list (without the key) that recreates `series` exactly. Task 8 reuses it for policy destinations.

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

### Task 8: Policy-created destinations come from the primary

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

### Task 9: `applied_through` — rule state that validates itself

**Files:**
- Modify: `src/series/compaction.rs` (`CompactionRule`, `rdb_save`/`rdb_load`, `apply_compaction`, `process_series_with_compaction`, `apply_rules_on_destinations`, `apply_rules_internal`, the `impl TimeSeries` block, `tests` module)
- Modify: `src/series/digest.rs` (no change to the digest — `applied_through` is cache state)
- Modify: `src/commands/ts_createrule.rs` (`parse_args` struct literal), `src/series/compaction_policy.rs` (struct literal)
- Modify: `src/commands/ts_add.rs` (`run_compaction` call), `src/commands/ts_incr_decr_by.rs` (`run_compaction` call)
- Modify: `src/commands/ts_info.rs` (`get_rules_state` fifth element)
- Modify: `src/common/module_info.rs` (counters section)
- Test: `src/series/compaction.rs` tests; `tests/test_compactions_add.py`; `tests/test_ts_replication.py`

**Interfaces:**
- Produces: `CompactionRule.applied_through: Option<Timestamp>`; `fn rebuild_open_bucket_as_of(parent: &TimeSeries, rule: &mut CompactionRule, as_of: Option<Timestamp>)` (Task 12 reuses it); `pub static COMPACTION_FAILURES: AtomicU64` and `pub static COMPACTION_STATE_REBUILDS: AtomicU64` exposed as `INFO ts_compaction` fields `ts_compaction_failures` / `ts_compaction_state_rebuilds`.
- Changes: `apply_compaction(ctx, series, op, prev_last: Option<Timestamp>)`; `TimeSeries::run_compaction(ctx, sample, prev_last: Option<Timestamp>)`; `rulesState` entries become `[destKey, bucketStart, hasSamples, current, appliedThrough]`.

- [ ] **Step 1: Write the failing Rust tests**

Replace the `mod tests` block at the end of `src/series/compaction.rs` with:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::aggregators::AggregationType;
    use crate::series::{SeriesLink, TimeSeries};

    fn rule(agg: AggregationType, bucket: u64) -> CompactionRule {
        CompactionRule {
            dest: SeriesLink::from("dst"),
            aggregator: agg.into(),
            bucket_duration: bucket,
            align_timestamp: 0,
            bucket_start: None,
            has_samples: false,
            applied_through: None,
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
    fn restored_rule_rejects_zero_bucket_duration() {
        assert!(validate_restored_rule_parameters(0, 0).is_err());
    }

    #[test]
    fn restored_rule_rejects_duration_that_overflows_signed_arithmetic() {
        assert!(validate_restored_rule_parameters(i64::MAX as u64 + 1, 0).is_err());
    }

    #[test]
    fn restored_rule_rejects_negative_alignment() {
        assert!(validate_restored_rule_parameters(1, -1).is_err());
    }

    #[test]
    fn restored_rule_accepts_valid_parameters() {
        assert!(validate_restored_rule_parameters(1, Timestamp::MAX).is_ok());
    }

    #[test]
    fn rebuild_matches_the_sequentially_fed_state() {
        let series = series_with(&[(0, 1.0), (300, 2.5), (450, 7.0), (1000, 9.0), (1200, 3.0)]);
        let mut sequential = rule(AggregationType::Avg, 1000);
        sequential.bucket_start = Some(1000);
        sequential.update(1000, 9.0);
        sequential.update(1200, 3.0);

        let mut rebuilt = rule(AggregationType::Avg, 1000);
        rebuild_open_bucket_as_of(&series, &mut rebuilt, Some(1200));

        assert_eq!(rebuilt.bucket_start, Some(1000));
        assert!(rebuilt.has_samples);
        assert_eq!(
            AggregationHandler::current(&rebuilt.aggregator),
            AggregationHandler::current(&sequential.aggregator)
        );
    }

    #[test]
    fn rebuild_stops_at_as_of_so_the_pending_append_is_not_counted_twice() {
        // 1700 is this op's own append, already stored; the append path streams it afterwards.
        let series = series_with(&[(1000, 9.0), (1200, 3.0), (1700, 100.0)]);
        let mut rebuilt = rule(AggregationType::Sum, 1000);
        rebuild_open_bucket_as_of(&series, &mut rebuilt, Some(1200));
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
}
```

- [ ] **Step 2: Run to verify they fail**

Run: `cargo test --features enable-system-alloc rebuild_ 2>&1 | tail -5`
Expected: compile error — no field `applied_through`, no `rebuild_open_bucket_as_of`.

- [ ] **Step 3: Add the field, persistence, and rebuild**

`src/series/compaction.rs` — the struct:

```rust
#[derive(Debug, Clone, Hash, PartialEq)]
pub struct CompactionRule {
    /// The destination series (see [`SeriesLink`]).
    pub dest: SeriesLink,
    pub aggregator: Aggregator,
    pub bucket_duration: u64,
    pub align_timestamp: Timestamp,
    pub bucket_start: Option<Timestamp>,
    pub has_samples: bool,
    /// The source's last timestamp when this rule's open-bucket state was last brought up to
    /// date (`None` while the source was empty). Between writes on a primary it always equals
    /// the source's last timestamp. A mismatch means the state was not maintained for some
    /// write — a promoted replica (whose rules froze at its snapshot, since destinations arrive
    /// as `TS._COMPACT` effects), or a compaction that failed part-way — and the open bucket is
    /// rebuilt from the source before the next op is applied (`rebuild_open_bucket_as_of`).
    pub applied_through: Option<Timestamp>,
}
```

`GetSize`: add `+ size_of::<Option<Timestamp>>() // applied_through`.

`rdb_save`: after `rdb_save_bool(rdb, self.has_samples);` add `rdb_save_timestamp(rdb, self.applied_through.unwrap_or(-1));`.

`rdb_load` (signature unchanged — the field is appended to the rule's layout in place, encoding version stays `1`), after `let has_samples = rdb_load_bool(rdb)?;`:

```rust
        let applied_through = {
            let value = rdb_load_timestamp(rdb)?;
            (value != -1).then_some(value)
        };
```

and `applied_through,` in the struct literal.

The rebuild, placed after `resync_open_bucket_after_removal`:

```rust
/// Rebuild `rule`'s open-bucket state from the source as it stood when its last timestamp was
/// `as_of`: the bucket containing `as_of`, aggregated over the source's stored samples in that
/// bucket at or below `as_of`. Anything above it is the current op's own append, already stored,
/// which `apply_op` is about to stream in. `None` means the source was empty: no open bucket.
///
/// This is `resync_open_bucket_after_removal` without the destination side, for a rule whose
/// state was not maintained — see [`CompactionRule::applied_through`]. One deviation from the
/// write path: a rule created on a source that already held samples in its open bucket starts out
/// ignoring them (`handle_sample_compaction` opens the bucket with the first new sample), while
/// a rebuild aggregates them.
fn rebuild_open_bucket_as_of(
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
```

Counters, near the top of the file:

```rust
use std::sync::atomic::AtomicU64;

/// Compaction rule applications that returned an error. Exposed as `ts_compaction_failures` in
/// `INFO ts_compaction`; non-zero means a destination may be missing a bucket the source change
/// should have produced (the source write itself is kept and replicated).
pub static COMPACTION_FAILURES: AtomicU64 = AtomicU64::new(0);
/// Open-bucket rebuilds triggered by an `applied_through` mismatch (`rebuild_open_bucket_as_of`).
/// Expected after a promotion or an AOF-tail replay; anything else is a failed compaction healing.
pub static COMPACTION_STATE_REBUILDS: AtomicU64 = AtomicU64::new(0);
```

(`std::cmp::Ordering` is already imported under that name, so write the atomic ordering as `std::sync::atomic::Ordering::Relaxed` in full.)

- [ ] **Step 4: Thread `prev_last` through and apply the check**

Signatures:

```rust
pub fn apply_compaction(
    ctx: &Context,
    series: &mut TimeSeries,
    op: CompactionOp,
    prev_last: Option<Timestamp>,
) -> TsdbResult<()> {
    if series.rules.is_empty() {
        return Ok(());
    }
    process_series_with_compaction(ctx, series, op, prev_last)
}
```

`process_series_with_compaction(ctx, series, op, prev_last)` passes `prev_last` to the first `apply_rules_on_destinations(series, guards, op, prev_last)`; both cascade loops pass the child's own `dest_prev_last`: in the `RemoveRange` branch change `pending` to collect `(key, outcome.dest_prev_last)` pairs —

```rust
            let mut pending: SmallVec<[(ValkeyString, Option<Timestamp>); TEMP_VEC_LEN]> = outcomes
                .iter()
                .filter_map(|o| keys[o.rule_index].take().map(|k| (k, o.dest_prev_last)))
                .collect();
            …
            while let Some((child_key, child_prev_last)) = pending.pop() {
                …
                let child_outcomes =
                    apply_rules_on_destinations(&mut child, child_guards, op, child_prev_last)?;
                pending.extend(
                    child_outcomes
                        .iter()
                        .filter_map(|o| child_keys[o.rule_index].take().map(|k| (k, o.dest_prev_last))),
                );
```

and in the other branch `apply_rules_on_destinations(&mut child, child_guards, batch_op, prev_last)` where `prev_last` is the loop's popped value (it already is the cascade's `prev_last`).

`apply_rules_on_destinations(series, destinations, op, prev_last)` forwards to `apply_rules_internal(series, &mut rules, destinations, op, prev_last)`, whose map closure becomes:

```rust
        .map(|(rule_index, rule, dest_guard)| {
            let rule_index = *rule_index;
            let dest_prev_last = dest_guard.last_sample.map(|s| s.timestamp);
            if rule.applied_through != prev_last {
                rebuild_open_bucket_as_of(series, rule, prev_last);
                COMPACTION_STATE_REBUILDS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            }
            let mut cctx = CompactionContext::new(series, dest_guard, rule);
            let result = apply_op(&mut cctx, op);
            let CompactionContext { written, .. } = cctx;
            // Not advanced on failure: a stale value is what makes the next write rebuild.
            if result.is_ok() {
                rule.applied_through = series.last_sample.map(|s| s.timestamp);
            }
            result.map(|_| RuleOutcome {
                rule_index,
                dest_prev_last,
                written,
            })
        })
```

and in the error loop below it, next to `log_warning`, add `COMPACTION_FAILURES.fetch_add(1, std::sync::atomic::Ordering::Relaxed);`.

`impl TimeSeries` block:

```rust
    pub fn remove_range_with_compaction(
        &mut self,
        ctx: &Context,
        start_ts: Timestamp,
        end_ts: Timestamp,
    ) -> TsdbResult<usize> {
        let prev_last = self.last_sample.map(|s| s.timestamp);
        let deleted_count = self.remove_range(start_ts, end_ts)?;

        if deleted_count > 0 && !self.rules.is_empty() {
            apply_compaction(
                ctx,
                self,
                CompactionOp::RemoveRange {
                    start: start_ts,
                    end: end_ts,
                },
                prev_last,
            )?;
        }

        Ok(deleted_count)
    }

    /// `prev_last`: this series' last timestamp from before `value` was stored.
    pub fn run_compaction(
        &mut self,
        ctx: &Context,
        value: Sample,
        prev_last: Option<Timestamp>,
    ) -> TsdbResult<()> {
        if self.rules.is_empty() {
            return Ok(());
        }
        apply_compaction(ctx, self, CompactionOp::AddNew(value), prev_last)
    }

    pub fn upsert_compaction(&mut self, ctx: &Context, value: Sample) -> TsdbResult<()> {
        if self.rules.is_empty() {
            return Ok(());
        }
        // An upsert never moves the last sample, so "before" is "now".
        let prev_last = self.last_sample.map(|s| s.timestamp);
        apply_compaction(ctx, self, CompactionOp::Upsert(value), prev_last)
    }
```

and `batch_compaction` passes its own `prev_last` as the fourth argument. `add_compaction_rule` initializes the field:

```rust
    pub fn add_compaction_rule(&mut self, rule: CompactionRule) {
        let mut rule = rule;
        if let Aggregator::Rate(r) = &mut rule.aggregator {
            r.set_window_ms(rule.bucket_duration);
        }
        // A fresh rule is up to date by definition: it starts its first bucket with the next
        // write, ignoring samples already in that bucket.
        rule.applied_through = self.last_sample.map(|s| s.timestamp);
        self.rules.push(rule);
    }
```

Callers: `src/commands/ts_add.rs` → `series.run_compaction(ctx, sample, last_ts)` (the `last_ts` already captured before the add); `src/commands/ts_incr_decr_by.rs::run_compaction_for_increment` → `series.run_compaction(ctx, sample, prev_last_ts)` (the `prev_last_ts` it already receives); `src/series/utils.rs::add_default_compactions` → replace `series.rules.extend(rules)` with `for rule in rules { series.add_compaction_rule(rule); }`. Struct literals in `src/commands/ts_createrule.rs::parse_args` and `src/series/compaction_policy.rs::create_compaction_rules` gain `applied_through: None,`.

`src/commands/ts_info.rs::get_rules_state`: append

```rust
                rule.applied_through
                    .map_or(ValkeyValue::Null, ValkeyValue::Integer),
```

as the fifth element and extend the doc comment to `[destKey, bucketStart, hasSamples, current, appliedThrough]`.

`src/common/module_info.rs`: add `use crate::series::compaction::{COMPACTION_FAILURES, COMPACTION_STATE_REBUILDS};` (make `compaction` reachable: `pub(crate) mod compaction;` in `src/series/mod.rs` if it is private) and, before `.build_info()?`:

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

- [ ] **Step 5: Update the Python expectations and add the invariant test**

`tests/test_compactions_add.py::test_info_debug_reports_rule_state`: the three expected entries become `[dst.encode(), None, 0, None, None]`, `[dst.encode(), 0, 1, b"3.5", 200]`, `[dst.encode(), 1000, 1, b"7", 1000]`. Add:

```python
    def test_applied_through_tracks_the_source_on_a_primary(self):
        """Between writes a primary's rule state is always current, so no rebuild ever runs."""
        c = self.client
        c.execute_command("TS.CREATE", "at:src")
        c.execute_command("TS.CREATE", "at:dst")
        c.execute_command("TS.CREATERULE", "at:src", "at:dst", "AGGREGATION", "sum", 1000)
        for ts in (0, 500, 1000, 300, 2500):
            c.execute_command("TS.ADD", "at:src", ts, 1)
            info = parse_info_response(c.execute_command("TS.INFO", "at:src"))
            assert self.info_debug(c, "at:src")[b"rulesState"][0][4] == info["lastTimestamp"]
        c.execute_command("TS.DEL", "at:src", 1000, 3000)
        assert self.info_debug(c, "at:src")[b"rulesState"][0][4] == 500
        c.execute_command("TS.DEL", "at:src", 0, 1000)
        assert self.info_debug(c, "at:src")[b"rulesState"][0][4] is None
        stats = c.info("ts_compaction")
        assert stats["ts_compaction_state_rebuilds"] == 0
        assert stats["ts_compaction_failures"] == 0
```

(`parse_info_response` is imported in that file from `common`; add the import if missing.)

- [ ] **Step 6: Run everything that touches compaction**

Run: `cargo test --features enable-system-alloc` — Expected: PASS.
Run: `TEST_PATTERN="compaction or createrule or deleterule or test_info_debug or test_applied_through or test_ts_restore_malformed or test_replication_compaction_ or test_rule_state_survives_reload" SERVER_VERSION=unstable ./build.sh` — Expected: PASS, including the `rulesState` comparisons (both nodes maintain `applied_through` identically while the replica still recomputes).

- [ ] **Step 7: Lint and commit**

```bash
git add src/series/compaction.rs src/series/mod.rs src/series/utils.rs src/commands/ts_createrule.rs src/series/compaction_policy.rs src/commands/ts_add.rs src/commands/ts_incr_decr_by.rs src/commands/ts_info.rs src/common/module_info.rs tests/test_compactions_add.py
git commit -m "feat(compaction): self-validating rule state (applied_through) and INFO counters"
```

---

## Part 3 — effects replication

### Task 10: Record destination effects and marker changes in the engine

**Files:**
- Modify: `src/series/compaction.rs` (`CompactionContext`, `remove_dest_range`, `add_dest_bucket`, `RuleOutcome`, `apply_rules_internal`, tests)

**Interfaces:**
- Produces: `pub(crate) enum DestEffect { Add(Sample), Del(Timestamp, Timestamp) }`; `RuleOutcome.effects: Vec<DestEffect>` in commit order; `RuleOutcome.marker: Option<Option<Timestamp>>` — `Some(new_marker)` when the destination's `last_forward_close` changed during the op. Task 13 replicates them.

- [ ] **Step 1: Write the failing tests**

Add to the `tests` module:

```rust
    #[test]
    fn a_forward_close_records_one_add_effect() {
        let parent = series_with(&[(0, 1.0), (500, 3.0), (1000, 5.0)]);
        let mut dest = TimeSeries::new();
        let mut rule = rule(AggregationType::Sum, 1000);
        rule.bucket_start = Some(0);
        rule.update(0, 1.0);
        rule.update(500, 3.0);

        let mut cctx = CompactionContext::new(&parent, &mut dest, &mut rule);
        apply_op(&mut cctx, CompactionOp::AddNew(Sample::new(1000, 5.0))).unwrap();

        assert_eq!(cctx.effects, vec![DestEffect::Add(Sample::new(0, 4.0))]);
        assert_eq!(cctx.dest.last_forward_close, Some(Sample::new(0, 4.0)));
    }

    #[test]
    fn a_trailing_removal_records_the_reopened_bucket_as_add_then_del() {
        // TS.DEL 3500.. on a 1000-bucket rule: bucket 3000 is first recalculated (an Add of the
        // surviving sum) and then retracted because it is open again (a Del) — in that order.
        let parent = series_with(&[(0, 1.0), (1000, 1.0), (2000, 1.0), (3000, 1.0)]);
        let mut dest = series_with(&[(0, 1.0), (1000, 1.0), (2000, 1.0), (3000, 2.0)]);
        let mut rule = rule(AggregationType::Sum, 1000);
        rule.bucket_start = Some(5000);
        rule.update(5000, 1.0);

        let mut cctx = CompactionContext::new(&parent, &mut dest, &mut rule);
        apply_op(&mut cctx, CompactionOp::RemoveRange { start: 3500, end: 10_000 }).unwrap();

        assert_eq!(
            cctx.effects,
            vec![
                DestEffect::Add(Sample::new(3000, 1.0)),
                DestEffect::Del(3000, Timestamp::MAX),
            ]
        );
        assert_eq!(cctx.rule.bucket_start, Some(3000));
    }
```

`Sample` needs `PartialEq` for these: add it to the derive in `src/common/types.rs` (`#[derive(Debug, Default, Clone, Copy, PartialEq, GetSize)]`).

- [ ] **Step 2: Run to verify they fail**

Run: `cargo test --features enable-system-alloc records_ 2>&1 | tail -5`
Expected: compile error — no `effects`, no `DestEffect`.

- [ ] **Step 3: Implement**

```rust
/// One change an operation made to a destination, in commit order. The primary replicates these
/// as `TS._COMPACT` commands so a replica stores exactly what the primary computed.
#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) enum DestEffect {
    Add(Sample),
    Del(Timestamp, Timestamp),
}
```

`CompactionContext` gains `effects: Vec<DestEffect>` (initialized to `Vec::new()` in `new`), with the doc: "Every write to and removal from `dest` this operation made, in order — `written` is the subset of these that cascades need." In `remove_dest_range`, after `let removed = self.dest.remove_range(start, end)?;`:

```rust
        if removed > 0 {
            self.effects.push(DestEffect::Del(start, end));
        }
```

In `add_dest_bucket`'s `Ok` arm, next to `ctx.written.push(sample);`: `ctx.effects.push(DestEffect::Add(sample));`.

`RuleOutcome` gains:

```rust
    /// Destination effects in commit order, for replication (`replicate_dest_effects`).
    effects: Vec<DestEffect>,
    /// `Some(marker)` when the destination's strict-mode marker changed during this operation.
    marker: Option<Option<Timestamp>>,
```

and the closure in `apply_rules_internal` becomes:

```rust
            let marker_before = dest_guard.last_forward_close.map(|s| s.timestamp);
            let mut cctx = CompactionContext::new(series, dest_guard, rule);
            let result = apply_op(&mut cctx, op);
            let marker_after = cctx.dest.last_forward_close.map(|s| s.timestamp);
            let CompactionContext { written, effects, .. } = cctx;
            if result.is_ok() {
                rule.applied_through = series.last_sample.map(|s| s.timestamp);
            }
            result.map(|_| RuleOutcome {
                rule_index,
                dest_prev_last,
                written,
                effects,
                marker: (marker_after != marker_before).then_some(marker_after),
            })
```

- [ ] **Step 4: Run, lint, commit**

Run: `cargo test --features enable-system-alloc` — Expected: PASS.

```bash
git add src/series/compaction.rs src/common/types.rs
git commit -m "refactor(compaction): record destination effects and marker changes per rule"
```

### Task 11: The `TS._COMPACT` internal command

**Files:**
- Create: `src/commands/ts_compact.rs`
- Modify: `src/commands/mod.rs` (`mod ts_compact;` + `pub use ts_compact::*;`), `src/lib.rs` (positional table), `AGENTS.md` (the two sentences naming `ts._debug`/`ts._restore` as the only positional commands)
- Test: `tests/test_ts_compact_internal.py`

**Interfaces:**
- Produces: `TS._COMPACT dest ADD <ts> <value-bits>` | `TS._COMPACT dest DEL <start> <end>` | `TS._COMPACT dest MARK <ts>|-`, replying `OK`; refuses real user clients unless `ts.debug-mode` is on.

- [ ] **Step 1: Write the failing tests**

```python
import struct

import pytest
from valkey.exceptions import ResponseError
from valkeytestframework.conftest import resource_port_tracker
from valkey_timeseries_test_case import ValkeyTimeSeriesTestCaseBase, ValkeyTimeSeriesTestCaseDebugMode


def bits(value):
    """f64 -> the decimal u64 of its IEEE-754 bit pattern (what the primary emits)."""
    return struct.unpack("<Q", struct.pack("<d", value))[0]


class TestCompactInternal(ValkeyTimeSeriesTestCaseDebugMode):
    def test_add_stores_the_exact_value_as_a_keep_last_upsert(self):
        c = self.client
        c.execute_command("TS.CREATE", "d")
        assert c.execute_command("TS._COMPACT", "d", "ADD", 1000, bits(3.5)) == b"OK"
        assert c.execute_command("TS.RANGE", "d", "-", "+") == [[1000, b"3.5"]]
        assert c.execute_command("TS._COMPACT", "d", "ADD", 1000, bits(4.25)) == b"OK"
        assert c.execute_command("TS.GET", "d") == [1000, b"4.25"]
        for ts, value in ((2000, float("nan")), (3000, float("inf")), (4000, float("-inf"))):
            assert c.execute_command("TS._COMPACT", "d", "ADD", ts, bits(value)) == b"OK"
        assert [row[0] for row in c.execute_command("TS.RANGE", "d", "-", "+")] == [1000, 2000, 3000, 4000]

    def test_add_sets_an_unset_marker_and_mark_overrides_it(self):
        c = self.client
        c.execute_command("TS.CREATE", "m:src")
        c.execute_command("TS.CREATE", "m")
        c.execute_command("TS.CREATERULE", "m:src", "m", "AGGREGATION", "avg", 500)   # makes `m` a destination
        c.execute_command("CONFIG", "SET", "ts.ts-compatibility-mode", "strict")
        try:
            c.execute_command("TS._COMPACT", "m", "ADD", 0, bits(1.0))
            c.execute_command("TS._COMPACT", "m", "ADD", 500, bits(2.0))
            assert c.execute_command("TS.GET", "m") == [0, b"1"]        # first write took the marker
            assert c.execute_command("TS._COMPACT", "m", "MARK", 500) == b"OK"
            assert c.execute_command("TS.GET", "m") == [500, b"2"]
            assert c.execute_command("TS._COMPACT", "m", "MARK", "-") == b"OK"
            assert c.execute_command("TS.GET", "m") == [500, b"2"]      # no marker: true last sample
        finally:
            c.execute_command("CONFIG", "SET", "ts.ts-compatibility-mode", "extended")

    def test_del_removes_the_range_and_keeps_the_marker_live(self):
        c = self.client
        c.execute_command("TS.CREATE", "x")
        for ts in (0, 1000, 2000):
            c.execute_command("TS._COMPACT", "x", "ADD", ts, bits(1.0))
        c.execute_command("TS._COMPACT", "x", "MARK", 2000)
        assert c.execute_command("TS._COMPACT", "x", "DEL", 1500, 9223372036854775807) == b"OK"
        assert c.execute_command("TS.RANGE", "x", "-", "+") == [[0, b"1"], [1000, b"1"]]
        assert self.ts_info("x", debug=True)["forwardClose"] == 1000   # fell back to the last sample

    def test_missing_destination_is_an_error_not_a_crash(self):
        with pytest.raises(ResponseError, match="does not exist"):
            self.client.execute_command("TS._COMPACT", "nope", "ADD", 1, 0)
        with pytest.raises(ResponseError):
            self.client.execute_command("TS._COMPACT", "nope", "FROB", 1)
        assert self.client.ping()


class TestCompactInternalIsInternal(ValkeyTimeSeriesTestCaseBase):
    def test_refused_for_real_clients_without_debug_mode(self):
        self.client.execute_command("TS.CREATE", "d")
        with pytest.raises(ResponseError, match="internal command"):
            self.client.execute_command("TS._COMPACT", "d", "ADD", 1, 0)
```

(`ts_info(key, debug=True)` is the base-class helper at `tests/valkey_timeseries_test_case.py:243`; if it returns bytes keys, use `[b"forwardClose"]`.)

- [ ] **Step 2: Run to verify they fail**

Run: `TEST_PATTERN="test_ts_compact_internal" SERVER_VERSION=unstable ./build.sh`
Expected: FAIL — `unknown command 'TS._COMPACT'`.

- [ ] **Step 3: Implement the handler**

`src/commands/ts_compact.rs`:

```rust
//! Internal command carrying one compaction *effect* from a primary to its replicas and AOF.
//!
//! A compaction rule runs inside the source write on the primary. Rather than re-running the rule
//! on every replica — which is only correct while every node evolves identical state from
//! identical inputs — the primary replicates each change it made to a destination
//! (`replicate_dest_effects` in `series/compaction.rs`), and a node applying a replicated or
//! AOF-replayed source write skips compaction (`apply_compaction`). The three forms:
//!
//! ```text
//! TS._COMPACT dest ADD <bucket-timestamp> <value-bits>   store one bucket (KeepLast upsert)
//! TS._COMPACT dest DEL <start> <end>                      drop the buckets in [start, end]
//! TS._COMPACT dest MARK <timestamp>|-                     set or clear the DIV-0023 marker
//! ```
//!
//! `value-bits` is the IEEE-754 bit pattern as a decimal u64 (`f64::to_bits`): exact for every
//! value an aggregator can produce, NaN and the infinities included, where a decimal rendering
//! is not.
//!
//! Internal-only, guarded like `TS._RESTORE`: a direct client call is refused unless
//! `ts.debug-mode` is on, which is how the integration tests drive it.

use crate::common::block_on_keys::signal_timeseries_ready;
use crate::common::context::{is_real_user_client, notify_keyspace_event};
use crate::config::is_debug_mode_enabled;
use crate::series::{DuplicatePolicy, SampleAddResult, TimeSeries, try_get_timeseries_mut};
use valkey_module::{Context, ValkeyError, ValkeyResult, ValkeyString, ValkeyValue};

const MISSING_DESTINATION: &str = "TSDB: TS._COMPACT destination does not exist";

pub fn ts_compact_cmd(ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    if is_real_user_client(ctx) && !is_debug_mode_enabled() {
        return Err(ValkeyError::Str(
            "ERR TS._COMPACT is an internal command and cannot be invoked directly",
        ));
    }
    if args.len() < 4 {
        return Err(ValkeyError::WrongArity);
    }
    let key = &args[1];
    let op = args[2].to_string_lossy().to_ascii_uppercase();
    let integer = |index: usize| -> ValkeyResult<i64> {
        args.get(index)
            .ok_or(ValkeyError::WrongArity)?
            .parse_integer()
            .map_err(|_| ValkeyError::Str("TSDB: invalid TS._COMPACT timestamp"))
    };

    let Some(mut dest) = try_get_timeseries_mut(ctx, key, None)? else {
        return Err(ValkeyError::Str(MISSING_DESTINATION));
    };

    match op.as_str() {
        "ADD" => {
            if args.len() != 5 {
                return Err(ValkeyError::WrongArity);
            }
            let ts = integer(3)?;
            let bits = args[4]
                .to_string_lossy()
                .parse::<u64>()
                .map_err(|_| ValkeyError::Str("TSDB: invalid TS._COMPACT value"))?;
            if apply_add(&mut dest, ts, f64::from_bits(bits))? {
                drop(dest);
                notify_keyspace_event(ctx, c"ts.add:dest", key);
                signal_timeseries_ready(ctx, key);
            }
        }
        "DEL" => {
            if args.len() != 5 {
                return Err(ValkeyError::WrongArity);
            }
            let (start, end) = (integer(3)?, integer(4)?);
            dest.remove_range(start, end)
                .map_err(|e| ValkeyError::String(format!("TSDB: TS._COMPACT DEL failed: {e}")))?;
            // Same fall-back as the primary's `remove_dest_range`; a MARK follows if it changed.
            if let Some(marker) = dest.last_forward_close
                && matches!(dest.get_sample(marker.timestamp), Ok(None))
            {
                dest.last_forward_close = dest.last_sample;
            }
        }
        "MARK" => {
            if args.len() != 4 {
                return Err(ValkeyError::WrongArity);
            }
            let marker = if args[3].as_slice() == b"-" {
                None
            } else {
                Some(integer(3)?)
            };
            dest.last_forward_close = marker.and_then(|t| dest.get_sample(t).ok().flatten());
        }
        _ => return Err(ValkeyError::Str("TSDB: unknown TS._COMPACT operation")),
    }

    // Reaches this node's AOF and, through it, a restart — as `TS._RESTORE` does. Propagation is
    // off while loading, so an AOF replay of this command does not re-append it.
    ctx.replicate_verbatim();
    Ok(ValkeyValue::SimpleStringStatic("OK"))
}

/// The primary's `add_dest_bucket`, minus the cascade bookkeeping. Returns whether a sample
/// landed (ignored and too-old writes are not failures: the primary saw the same outcome).
fn apply_add(dest: &mut TimeSeries, ts: i64, value: f64) -> ValkeyResult<bool> {
    match dest.add(ts, value, Some(DuplicatePolicy::KeepLast)) {
        SampleAddResult::Ok(sample) => {
            if dest.last_forward_close.is_none() {
                dest.last_forward_close = Some(sample);
            }
            Ok(true)
        }
        SampleAddResult::Ignored(_) | SampleAddResult::TooOld => Ok(false),
        other => Err(ValkeyError::String(format!(
            "TSDB: TS._COMPACT ADD failed: {other}"
        ))),
    }
}
```

`src/commands/mod.rs`: add `mod ts_compact;` in alphabetical position and `pub use ts_compact::*;` next to `pub use ts_restore::*;`.

`src/lib.rs` positional table:

```rust
        ["ts._compact", commands::ts_compact_cmd, "write deny-oom", 1, 1, 1, "write timeseries admin"],
```

`AGENTS.md`: in the Architecture bullet and the Conventions bullet that say "Only `ts._debug` and `ts._restore`", add `ts._compact` ("carries compaction effects to replicas/AOF, `src/commands/ts_compact.rs`").

- [ ] **Step 4: Run**

Run: `TEST_PATTERN="test_ts_compact_internal" SERVER_VERSION=unstable ./build.sh` — Expected: PASS.
Run: `cargo test --features enable-system-alloc every_annotated_command_declares_acl_categories` — Expected: PASS (positional commands are not counted).

- [ ] **Step 5: Lint and commit**

```bash
git add src/commands/ts_compact.rs src/commands/mod.rs src/lib.rs AGENTS.md tests/test_ts_compact_internal.py
git commit -m "feat(commands): TS._COMPACT internal command for replicated compaction effects"
```

### Task 12: `LATEST` derived from the source when the rule state is stale

**Files:**
- Modify: `src/series/compaction.rs` (`get_latest_compaction_sample`; new `open_bucket_sample`; tests)

**Interfaces:**
- Produces: `fn open_bucket_sample(parent: &TimeSeries, rule: &CompactionRule) -> Option<Sample>` — the sample `LATEST` reports, from the live state when `rule.applied_through == parent`'s last timestamp, otherwise rebuilt from the source. All three `LATEST` callers (`ts_get.rs`, `ts_mget.rs`, `iterators/timeseries_range_iterator.rs`) go through `get_latest_compaction_sample`, which now uses it.

- [ ] **Step 1: Write the failing test**

```rust
    #[test]
    fn latest_is_derived_from_the_source_when_the_rule_state_is_stale() {
        let parent = series_with(&[(0, 1.0), (1000, 2.0), (1500, 4.0)]);

        let mut live = rule(AggregationType::Sum, 1000);
        live.bucket_start = Some(1000);
        live.update(1000, 2.0);
        live.update(1500, 4.0);
        live.applied_through = Some(1500);
        assert_eq!(open_bucket_sample(&parent, &live), Some(Sample::new(1000, 6.0)));

        // Frozen at a snapshot taken after the first sample: wrong bucket, wrong accumulator.
        let mut stale = rule(AggregationType::Sum, 1000);
        stale.bucket_start = Some(0);
        stale.update(0, 1.0);
        stale.applied_through = Some(0);
        assert_eq!(open_bucket_sample(&parent, &stale), Some(Sample::new(1000, 6.0)));

        let fresh = rule(AggregationType::Sum, 1000);   // never applied: no open bucket to report
        assert_eq!(open_bucket_sample(&series_with(&[]), &fresh), None);
    }
```

- [ ] **Step 2: Run to verify it fails**

Run: `cargo test --features enable-system-alloc latest_is_derived 2>&1 | tail -3` — Expected: compile error, `open_bucket_sample` missing.

- [ ] **Step 3: Implement**

```rust
/// The sample `LATEST` reports for `rule`'s destination: its still-open bucket.
///
/// From the rule's own accumulator when that is current. When it is not — on a replica the rule
/// state is frozen at the last snapshot, because destinations arrive as `TS._COMPACT` effects
/// rather than being recomputed — the open bucket is rebuilt from the source, which by the
/// `applied_through` invariant is exactly what the primary's accumulator holds.
fn open_bucket_sample(parent: &TimeSeries, rule: &CompactionRule) -> Option<Sample> {
    let last = parent.last_sample.map(|s| s.timestamp);
    let current = if rule.applied_through == last {
        rule.clone()
    } else {
        let mut rebuilt = rule.clone();
        rebuild_open_bucket_as_of(parent, &mut rebuilt, last);
        rebuilt
    };
    let start = current.bucket_start?;
    let mut aggregator = current.aggregator;
    let value = AggregationHandler::finalize(&mut aggregator);
    Some(Sample::new(start, value))
}

/// The still-open bucket of the rule that feeds `series`, a compaction destination (`LATEST`).
pub(crate) fn get_latest_compaction_sample(ctx: &Context, series: &TimeSeries) -> Option<Sample> {
    let parent_key = series.src_series.as_ref()?.to_key_string(ctx);
    let Ok(Some(parent)) = try_get_timeseries(ctx, &parent_key, None) else {
        // No source series or it doesn't exist
        return None;
    };
    let rule = parent
        .rules
        .iter()
        .find(|rule| rule.dest.points_to(&series.key))?;
    open_bucket_sample(&parent, rule)
}
```

- [ ] **Step 4: Run, lint, commit**

Run: `cargo test --features enable-system-alloc` and `TEST_PATTERN="latest or test_ts_get or test_ts_mget or compaction" SERVER_VERSION=unstable ./build.sh` — Expected: PASS (on a primary the live branch is always taken; replies unchanged).

```bash
git add src/series/compaction.rs
git commit -m "feat(compaction): derive LATEST from the source when rule state is stale"
```

### Task 13: Switch replication to effects

**Files:**
- Modify: `src/series/compaction.rs` (`apply_compaction` gate; `replicate_dest_effects`; emission in `process_series_with_compaction`)
- Modify: `tests/test_ts_replication.py` (helpers; new tests; drop `assert_rule_state_equivalent` calls)
- Create: `tests/test_ts_compaction_aof_replay.py`

- [ ] **Step 1: Write the failing tests**

In `tests/test_ts_replication.py`:

```python
    def collect_commands(self, monitor, until, count, deadline_seconds=5):
        """MONITOR entries (command text) up to and including the `count`-th whose name is `until`."""
        seen, deadline = [], time.time() + deadline_seconds
        matched = 0
        while time.time() < deadline and matched < count:
            entry = monitor.next_command()
            if entry is None:
                continue
            command = entry.get("command", "")
            seen.append(command)
            if command.lower().startswith(until.lower()):
                matched += 1
        assert matched == count, seen
        return seen

    def test_replication_compaction_effects_are_streamed(self):
        """The stream carries the primary's destination effects in one transaction with the
        source write, and the replica does not recompute: its rule state stays at the snapshot
        while its destination data follows the primary."""
        src, dst = "ts:effects:src", "ts:effects:dst"
        self.rule(src, dst, "sum", 1000)
        monitor_client = self.replicas[0].get_new_client()
        monitor_client.connection_pool.connection_kwargs["socket_timeout"] = 5
        with monitor_client.monitor() as monitor:
            self.client.execute_command("TS.ADD", src, 0, 1)
            self.client.execute_command("TS.ADD", src, 1000, 2)      # closes bucket 0: ADD + MARK
            commands = self.collect_commands(monitor, until="ts.add", count=2)
        compact = [c.split() for c in commands if c.lower().startswith('"ts._compact"')]
        assert [(c[2].strip('"').upper(), c[3].strip('"')) for c in compact] == [("ADD", "0"), ("MARK", "0")], commands
        assert any(c.upper().startswith('"MULTI"') for c in commands) and any(c.upper().startswith('"EXEC"') for c in commands), commands

        self.wait_for_replication()
        replica = self.replicas[0].client
        assert self.info_debug(replica, src)[b"rulesState"] == [[dst.encode(), None, 0, None, None]]
        assert self.info_debug(self.client, src)[b"rulesState"] == [[dst.encode(), 1000, 1, b"2", 1000]]
        self.assert_compaction_equivalent(src, dst)

    def test_replication_compaction_effects_inside_multi(self):
        """Review focus 3: a compacting write inside MULTI/EXEC keeps its effects in the transaction."""
        src, dst = "ts:multi:src", "ts:multi:dst"
        self.rule(src, dst, "sum", 1000)
        self.client.execute_command("TS.ADD", src, 0, 1)
        monitor_client = self.replicas[0].get_new_client()
        monitor_client.connection_pool.connection_kwargs["socket_timeout"] = 5
        with monitor_client.monitor() as monitor:
            pipe = self.client.pipeline(transaction=True)
            pipe.execute_command("TS.ADD", src, 1000, 2)
            pipe.execute_command("TS.ADD", src, 2000, 3)
            pipe.execute()
            commands = self.collect_commands(monitor, until="exec", count=1)
        names = [c.split()[0].strip('"').upper() for c in commands]
        assert names.count("MULTI") == 1 and names.count("EXEC") == 1, commands
        assert names.index("MULTI") < names.index("TS._COMPACT") < names.index("EXEC")
        self.assert_compaction_equivalent(src, dst)

    def test_replication_compaction_del_effects_are_bounded(self):
        """Review focus 4: a huge TS.DEL on a 1 ms rule emits at most DEL + 2 recalculations + MARK."""
        src, dst = "ts:bigdel:src", "ts:bigdel:dst"
        self.rule(src, dst, "sum", 1)
        self.client.execute_command("TS.MADD", src, 0, 1, src, 1, 1, src, 2, 1, src, 10, 1)
        monitor_client = self.replicas[0].get_new_client()
        monitor_client.connection_pool.connection_kwargs["socket_timeout"] = 5
        with monitor_client.monitor() as monitor:
            self.client.execute_command("TS.DEL", src, 1, 10_000_000_000)
            commands = self.collect_commands(monitor, until="ts.del", count=1)
        compact = [c for c in commands if c.lower().startswith('"ts._compact"')]
        assert 1 <= len(compact) <= 4, commands
        self.assert_compaction_equivalent(src, dst)
```

Remove every `self.assert_rule_state_equivalent(...)` call from the Part 0 tests (Tasks 2–4) and replace the helper's body with:

```python
    def assert_rule_state_equivalent(self, *keys):
        """Not comparable any more: a replica's rule state is frozen at its last snapshot by design
        (destinations arrive as TS._COMPACT effects). The open bucket is compared through
        TS.GET … LATEST, which a replica derives from the source. Kept as an explicit no-op so the
        Part 0 history reads correctly."""
```

`tests/test_ts_compaction_aof_replay.py`:

```python
"""Review: an AOF tail holds the source writes *and* their TS._COMPACT effects. Replaying it must
apply the effects and skip recompute — the AOF client is not a real user client — and the base
snapshot's rule state is brought up to date by `applied_through` on the first write after load."""

import time

from valkeytestframework.conftest import resource_port_tracker
from valkey_timeseries_test_case import ValkeyTimeSeriesTestCaseBase


class TestCompactionAofReplay(ValkeyTimeSeriesTestCaseBase):
    def wait_aof_ready(self, timeout=15):
        deadline = time.time() + timeout
        while time.time() < deadline:
            info = self.client.info("persistence")
            if info["aof_enabled"] == 1 and info["aof_rewrite_in_progress"] == 0:
                return
            time.sleep(0.1)
        raise AssertionError("AOF did not become ready")

    def test_effects_replay_from_the_aof_tail(self):
        c = self.client
        c.execute_command("TS.CREATE", "aof:src")
        c.execute_command("TS.CREATE", "aof:dst")
        c.execute_command("TS.CREATERULE", "aof:src", "aof:dst", "AGGREGATION", "avg", 1000)
        c.execute_command("TS.ADD", "aof:src", 0, 1)
        c.execute_command("TS.ADD", "aof:src", 300, 2.5)
        # Base snapshot taken mid-bucket; everything after lands in the incremental file as commands.
        c.execute_command("CONFIG", "SET", "appendonly", "yes")
        self.wait_aof_ready()
        for ts, value in ((450, 7), (1000, 9), (1200, 3), (100, 100), (2000, 5)):
            c.execute_command("TS.ADD", "aof:src", ts, value)
        c.execute_command("TS.DEL", "aof:src", 1100, 1300)
        expected = {
            name: c.execute_command(*cmd)
            for name, cmd in {
                "range": ("TS.RANGE", "aof:dst", "-", "+"),
                "digest": ("DEBUG", "DIGEST-VALUE", "aof:dst"),
                "latest": ("TS.GET", "aof:dst", "LATEST"),
                "src": ("DEBUG", "DIGEST-VALUE", "aof:src"),
            }.items()
        }
        assert expected["range"] == [[0, b"27.625"], [1000, b"9"]]

        self.server.args["appendonly"] = "yes"
        self.server.restart(remove_rdb=False, remove_nodes_conf=False, connect_client=True)
        c = self.client
        assert c.execute_command("TS.RANGE", "aof:dst", "-", "+") == expected["range"]
        assert c.execute_command("DEBUG", "DIGEST-VALUE", "aof:dst") == expected["digest"]
        assert c.execute_command("DEBUG", "DIGEST-VALUE", "aof:src") == expected["src"]
        assert c.execute_command("TS.GET", "aof:dst", "LATEST") == expected["latest"]
        rebuilds_before = c.info("ts_compaction")["ts_compaction_state_rebuilds"]
        c.execute_command("TS.ADD", "aof:src", 2500, 1)          # first write after load
        assert c.info("ts_compaction")["ts_compaction_state_rebuilds"] == rebuilds_before + 1
        c.execute_command("TS.ADD", "aof:src", 3000, 1)          # closes bucket 2000: avg(5, 1)
        assert c.execute_command("TS.RANGE", "aof:dst", "-", "+")[-1] == [2000, b"3"]
```

- [ ] **Step 2: Run to verify they fail**

Run: `TEST_PATTERN="test_replication_compaction_effects or test_replication_compaction_del_effects or test_ts_compaction_aof_replay" SERVER_VERSION=unstable ./build.sh`
Expected: FAIL — no `TS._COMPACT` in the stream; the replica's `rulesState` is live.

- [ ] **Step 3: Emit on the primary, skip on replicas and AOF replay**

`src/series/compaction.rs`, add `use crate::common::context::is_real_user_client;` and:

```rust
pub fn apply_compaction(
    ctx: &Context,
    series: &mut TimeSeries,
    op: CompactionOp,
    prev_last: Option<Timestamp>,
) -> TsdbResult<()> {
    if series.rules.is_empty() {
        return Ok(());
    }
    // A replicated or AOF-replayed source write carries its destination effects alongside it as
    // `TS._COMPACT` commands (`replicate_dest_effects`); re-running the rules here would apply
    // them twice and make the replica's destinations depend on this node reproducing the
    // primary's arithmetic. Rule state is left as the last snapshot wrote it: reads derive the
    // open bucket (`open_bucket_sample`) and the first write after a promotion rebuilds it
    // (`CompactionRule::applied_through`).
    if !is_real_user_client(ctx) {
        return Ok(());
    }
    process_series_with_compaction(ctx, series, op, prev_last)
}
```

```rust
/// Replicate one destination's effects of this operation as `TS._COMPACT` commands, in commit
/// order, then the marker when it changed. `ctx.replicate` queues them with the source command's
/// own propagation, and the server wraps a command's queued propagations in `MULTI`/`EXEC`, so a
/// replica applies the source write and its effects atomically.
fn replicate_dest_effects(ctx: &Context, dest_key: &ValkeyString, outcome: &RuleOutcome) {
    let add = ctx.create_string("ADD");
    let del = ctx.create_string("DEL");
    for effect in &outcome.effects {
        match *effect {
            DestEffect::Add(sample) => {
                let ts = ctx.create_string(sample.timestamp.to_string().as_bytes());
                let bits = ctx.create_string(sample.value.to_bits().to_string().as_bytes());
                ctx.replicate("TS._COMPACT", &[dest_key, &add, &ts, &bits]);
            }
            DestEffect::Del(start, end) => {
                let start = ctx.create_string(start.to_string().as_bytes());
                let end = ctx.create_string(end.to_string().as_bytes());
                ctx.replicate("TS._COMPACT", &[dest_key, &del, &start, &end]);
            }
        }
    }
    if let Some(marker) = outcome.marker {
        let mark = ctx.create_string("MARK");
        let value = ctx.create_string(marker.map_or("-".to_string(), |t| t.to_string()).as_bytes());
        ctx.replicate("TS._COMPACT", &[dest_key, &mark, &value]);
    }
}

/// `replicate_dest_effects` for every outcome of one level, before the keys are taken for the
/// cascade.
fn replicate_level_effects(
    ctx: &Context,
    keys: &[Option<ValkeyString>],
    outcomes: &[RuleOutcome],
) {
    for outcome in outcomes {
        if let Some(dest_key) = &keys[outcome.rule_index] {
            replicate_dest_effects(ctx, dest_key, outcome);
        }
    }
}
```

In `process_series_with_compaction`, call `replicate_level_effects(ctx, &keys, &outcomes);` immediately after `let outcomes = apply_rules_on_destinations(series, guards, op, prev_last)?;`, and `replicate_level_effects(ctx, &child_keys, &child_outcomes);` immediately after each `let child_outcomes = apply_rules_on_destinations(…)?;` in both cascade loops (before the `pending.extend` / `queue_written_outcomes` that take the keys).

- [ ] **Step 4: Run the whole replication, compaction and persistence surface**

Run: `TEST_PATTERN="test_replication or compaction or createrule or deleterule or test_ts_compact_internal or test_ts_aofrewrite or test_dump_restore or test_ts_save_and_restore or test_ts_mdel or test_ts_del" SERVER_VERSION=unstable ./build.sh`
Expected: PASS — including every Part 0 scenario, which is the acceptance test: the replica's destinations are now what the primary sent.

Run: `cargo test --features enable-system-alloc` — Expected: PASS.

- [ ] **Step 5: Cluster and compat gates**

Run: `TEST_PATTERN="_cme" SERVER_VERSION=unstable ./build.sh` — Expected: PASS (`TS.MDEL`'s per-shard handler runs on a real-flagged client, so it still compacts and now emits effects).
Run: `RTS_COMPAT=1 TEST_PATTERN="compaction or del or add" SERVER_VERSION=unstable ./build.sh` — Expected: PASS (nothing visible on the primary changed).

- [ ] **Step 6: Measure the stream cost (input to the Phase 1 record)**

Run `tools/fleet_loader.sh -p <port> --preset medium` against a server started with `ts.ts-compaction-policy "avg:1m:1h;max:1m:1h"` on the primary of a `./build.sh`-built pair, and read `INFO replication` `master_repl_offset` before and after on both a build at Task 12's commit and this one. Record the two byte counts and the ratio in `docs/plans/compaction-replication-plan.md` under a new "Phase 1 record" heading (expected: a few percent — one `ADD`+`MARK` per rule per bucket close, i.e. per minute per series).

- [ ] **Step 7: Lint and commit**

```bash
git add src/series/compaction.rs tests/test_ts_replication.py tests/test_ts_compaction_aof_replay.py docs/plans/compaction-replication-plan.md
git commit -m "feat(replication): replicate compaction effects; replicas and AOF replay stop recomputing"
```

### Task 14: Promotion and slot migration under the new model

**Files:**
- Test: `tests/test_ts_replication.py` (extend `test_replication_compaction_promotion`), `tests/test_ts_asm.py` (helper mixin + new class)

- [ ] **Step 1: Extend the promotion test**

Append to `test_replication_compaction_promotion`, right after `new_primary, new_replica = self.promote_replica()`:

```python
        # Review focus 5: the promoted node's rules are frozen at its snapshot; its first write
        # rebuilds the open bucket from the source, excluding that write, then streams it.
        rebuilds_before = new_primary.info("ts_compaction")["ts_compaction_state_rebuilds"]
```

and after the writes loop:

```python
        assert new_primary.info("ts_compaction")["ts_compaction_state_rebuilds"] == rebuilds_before + 1
```

The existing exact-value assertion (`[[0, b"27.625"], [1000, b"9"]]`) is what proves nothing was counted twice.

- [ ] **Step 2: Extract the ASM helpers into a mixin and add the no-debug-mode class**

In `tests/test_ts_asm.py`, move every method of `TestAtomicSlotMigration` whose name starts with `_` (the block from `_migration_state` through `_abort_migration`, lines ~98–303, plus `_skip_if_asm_not_supported`, `_skip_if_asm_commands_not_supported`, `_get_node_id`, `_find_hash_tag_for_shard`, `_get_key_slot`, `_shard_index_for_slot`, `_create_rule_pair` and the `_FollowingWriter` class wherever they sit) into a new `class AsmHelpers:` defined above it, and declare `class TestAtomicSlotMigration(AsmHelpers, ValkeyTimeSeriesClusterTestCaseDebugMode):`. No method body changes. Then add at the end of the file:

```python
class TestAtomicSlotMigrationWithoutDebugMode(AsmHelpers, ValkeyTimeSeriesClusterTestCase):
    """The import path must accept the internal commands (TS._RESTORE for the snapshot,
    TS._COMPACT for effects streamed during the migration) *without* the debug-mode escape hatch
    the rest of this file runs under: an ASM target applies the source's stream as a non-user
    client. If this fails with "internal command", `is_real_user_client` is true for the ASM
    import client and the guards in ts_restore.rs / ts_compact.rs need an ASM-aware exception
    keyed on `is_key_in_slot_import(key)`."""

    def test_rule_pair_migrates_mid_bucket_and_closes_on_the_new_owner(self):
        source_client = self.new_client_for_primary(0)
        self._skip_if_asm_not_supported(source_client)
        self._skip_if_asm_commands_not_supported(source_client)
        target_client = self.new_client_for_primary(1)
        target_node_id = self._get_node_id(target_client)

        hash_tag = self._find_hash_tag_for_shard(source_client, "asm_nodebug", shard_index=0)
        src, dst = f"ts:{{{hash_tag}}}:src", f"ts:{{{hash_tag}}}:dst"
        source_client.execute_command("TS.CREATE", src)
        source_client.execute_command("TS.CREATE", dst)
        source_client.execute_command("TS.CREATERULE", src, dst, "AGGREGATION", "sum", 1000)
        for ts, value in ((0, 1), (300, 2.5), (450, 7), (1000, 9), (1200, 3)):
            source_client.execute_command("TS.ADD", src, ts, value)
        probes = (
            ("DEBUG", "DIGEST-VALUE", src),
            ("DEBUG", "DIGEST-VALUE", dst),
            ("TS.RANGE", dst, "-", "+"),
            ("TS.GET", dst, "LATEST"),
        )
        expected = {cmd: source_client.execute_command(*cmd) for cmd in probes}
        assert expected[("TS.GET", dst, "LATEST")] == [1000, b"12"]

        slot = self._get_key_slot(source_client, src)
        self._migrate_slot_range(source_client, target_node_id, slot, slot)
        self._wait_for_no_migrations(source_client)
        self._wait_for_no_migrations(target_client)

        for cmd in probes:
            assert target_client.execute_command(*cmd) == expected[cmd], cmd
        target_client.execute_command("TS.ADD", src, 2000, 5)   # closes the migrated open bucket
        assert target_client.execute_command("TS.RANGE", dst, "-", "+")[-1] == [1000, b"12"]
        assert target_client.execute_command("TS.GET", dst, "LATEST") == [2000, b"5"]
```

- [ ] **Step 3: Run**

Run: `TEST_PATTERN="test_replication_compaction_promotion or test_ts_asm" SERVER_VERSION=unstable ./build.sh`
Expected: PASS. If the no-debug test fails with "internal command", apply the docstring's fix: in both guards, `if is_real_user_client(ctx) && !is_debug_mode_enabled() && !is_key_in_slot_import(key.as_slice())`, and re-run.

- [ ] **Step 4: Commit**

```bash
git add tests/test_ts_replication.py tests/test_ts_asm.py src/commands/ts_restore.rs src/commands/ts_compact.rs
git commit -m "test(asm): compaction pair migrates mid-bucket without debug mode; promotion rebuild pinned"
```

---

## Part 4 — record the outcome

### Task 15: Proposal and analysis updates

**Files:**
- Modify: `docs/plans/compaction-replication-plan.md` (status line; §6 phases marked done; Phase 1 record)
- Modify: `docs/plans/proposal-complete.md` (§3.5 table and prose; summary table "Replication" row; readiness criterion 4; Phase 4 "Current-codebase expansion")
- Modify: `docs/plans/proposal.md` (Replication row; priority 4)
- Modify: `COMPATIBILITY.md` only if Task 1's compat step registered a divergence (then add one sentence under the `TS.INFO` extensions)

- [ ] **Step 1: Update the analysis doc**

Set the status line to "Status: implemented through Part 3 (commit <sha>); Option C." Under §6 mark each phase item with the task that landed it. Keep §3 as the historical record; add "Resolved by Task N" after each class.

- [ ] **Step 2: Update the proposals**

`proposal-complete.md` §3.5 table: the `Compaction downstream writes` row becomes "Replicated as `TS._COMPACT` effects; replicas and AOF replay do not recompute; `LATEST` on replicas derived from the source | Implemented, replica-tested (equivalence suite, promotion, ASM)". Add a row "Strict-mode marker | Persisted with the series (and, under Option C, replicated via `MARK`) | Implemented, replica-tested". Add a row "Policy-created destinations / auto-create defaults | Replicated as explicit `TS.CREATE`/`TS.CREATERULE` with the primary's resolved options | Implemented, replica-tested". Rewrite the prose paragraph after the table to describe the suite (`test_replication_compaction_*`, the Hypothesis oracle, reload/promotion, AOF replay, ASM without debug mode). Summary table Replication row: "MDEL, INCRBY/DECRBY, ADDBULK, compaction effects, policy destinations and auto-create defaults all replica-tested; retention-trim cron not replicated (covered by `test_replication_retention_trim_cron`)". Readiness criterion 4: replace the "Today …" sentences about compaction with the new model, and in "Ready means" strike the items now covered. Phase 4 expansion: the remaining propagation work is the `*`-timestamp replica test only.

`proposal.md` Replication row: coverage column gains "compaction effects (`TS._COMPACT`), policy destinations, auto-create defaults"; gap column becomes "The retention-trim cron runs on every node unreplicated (safe: the floor follows the series' last sample; pinned by a test). `*`-timestamp propagation still has no dedicated replica test." Priority 4: drop "compaction" from the list.

- [ ] **Step 3: Commit**

```bash
git add docs/plans/compaction-replication-plan.md docs/plans/proposal-complete.md docs/plans/proposal.md COMPATIBILITY.md
git commit -m "docs(plans): record compaction effects replication in the analysis and proposals"
```

---

## Self-review (done while writing; kept for the executor)

- **Spec coverage:** §6 0.1 → Task 1; 0.2 → Task 2; 0.3 → Task 3; 0.4 → Task 4; 0.5 → Task 5 (deviation 1); 0.6 → Task 5 Step 3; 0.7 → Task 9 (counters); 0.8 → Task 8 Step 4; Phase 1 → Checkpoint; 2.1 → Task 6; 2.2 → Task 8 (deviation 2); 2.3 → Task 7; 2.4 → Task 9; 3.1 → Task 11; 3.2 → Tasks 10 + 13; 3.3 → Task 13; 3.4 → Tasks 12 + 14; 3.5 → Task 13 Step 4; 3.6 → Task 14; Phase 4 → Task 15.
- **Type consistency:** `apply_compaction(ctx, series, op, prev_last)` (Task 9) is the signature Task 13 gates; `rebuild_open_bucket_as_of(parent, rule, as_of)` (Task 9) is what Task 12 calls; `RuleOutcome { rule_index, dest_prev_last, written, effects, marker }` (Task 10) is what Task 13's `replicate_dest_effects` reads; `rulesState` is 4-wide from Task 1 and 5-wide from Task 9, and Task 13's assertions use the 5-wide form.
- **Review Focus:** 1 → Task 11 `test_missing_destination_is_an_error_not_a_crash`; 2 → Task 11 NaN/±inf; 3 → Task 13 `test_replication_compaction_effects_inside_multi`; 4 → Task 13 `test_replication_compaction_del_effects_are_bounded`; 5 → Task 14 Step 1 (exact value + rebuild counter).
