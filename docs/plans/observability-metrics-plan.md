# Observability metrics plan

Status: proposal (2026-09-05), **revised 2026-10-07** against `unstable` @ 6d83b5d75.
**Phase 0 implemented 2026-10-07** (registry, `TS._DEBUG STATS`, cron and executor metrics),
then rebuilt the same day on the [`metered`](https://crates.io/crates/metered) crate
(`=0.10.0-rc.1`). Every
file:line hook below was re-checked at that commit; the revision log at the end lists what changed
and why. Addresses release-readiness item 7 ("Operability") in `docs/proposal.md` (on the
`proposal` branch, not yet on `unstable`): *publish bounded metrics for series, samples, chunks,
memory, background work, errors, latency, fanout, replication, and recovery.*

Related unmerged work: `DOS-1` (holds "limit counters" per the proposal's work-in-flight table;
overlaps §3.4), `panic-barrier` (the FFI `catch_unwind` barrier §3.11 waits on).

## 1. Where we are

The module has no metrics subsystem — no `src/common/metrics.rs`, no registry, no stats
subcommand. What exists today:

| Surface | Exposes | Gap |
|---|---|---|
| `INFO ts_memory` ([module_info.rs:25-53](../../src/common/module_info.rs#L25-L53)) | index bytes by component, term/series/db counts, interner count and bytes | memory only. **Not O(1) any more**: `index_memory_usage()` ([memory.rs:203-209](../../src/series/index/memory.rs#L203-L209)) walks every term and every id→key entry in 250 µs lock-released slices |
| `TS._DEBUG INDEXMEMORY [ALLDBS] [LOCAL]` ([ts_debug.rs:163-213](../../src/commands/ts_debug.rs#L163-L213)) | same walk as `INFO ts_memory`, camelCase keys | fans out cluster-wide by default (`ReplicaPerShard`, summed) |
| `TS._DEBUG STRINGPOOLSTATS [TOPK] [LOCAL]` | interner distribution, top-K | O(pool) walk; fans out by default |
| `TS._DEBUG QUERYINDEX` | node-local index query | test hook, not a metric |
| `TS._DEBUG LIST_CONFIGS [VERBOSE]` | config roster from `CONFIGS` | not metrics. HELP advertises `[APP\|DEV\|HIDDEN]`, which [ts_debug_configs.rs:68-77](../../src/commands/ts_debug_configs.rs#L68-L77) rejects |
| `TS._DEBUG HELP` ([ts_debug.rs:238-260](../../src/commands/ts_debug.rs#L238-L260)) | advertises `SHOW_INFO` | **`SHOW_INFO` still has no implementation**; the dispatcher ([:290-306](../../src/commands/ts_debug.rs#L290-L306)) returns "Unknown subcommand" |
| `BoundedExecutor::stats()` ([executor.rs:131-137](../../src/common/threads/executor.rs#L131-L137)) | `queued`, `running`, `rejected` per lane, documented "for INFO" | **already collected, never read** outside tests |
| Log lines | trim counts, drain durations, sweep outcomes, rule pruning, threading-rule violations | computed and thrown away; tests grep logs to observe background work |

Values already computed at the call site and discarded — the cheapest hooks:

- `CRON_TICKS` / `CRON_INTERVAL_MS` ([background_tasks.rs:30-31](../../src/series/background_tasks.rs#L30-L31)); ticks increment at `:153` only after the loading/shutdown early return.
- `drain_buffers` count and elapsed ([bulk_build.rs:162-169](../../src/series/index/bulk_build.rs#L162-L169)).
- `trim_series` `processed` / `total_deletes` ([series_trim.rs:93-117](../../src/series/tasks/series_trim.rs#L93-L117)).
- `process_delayed_keys_for_db` `total` / `indexed` / `skipped` ([asm.rs:342-344](../../src/series/index/asm.rs#L342-L344)); note `indexed` includes skipped keys, so the real count is `indexed − skipped`.
- `verify_and_repair_db` `indexed_count` / `repaired_count` ([persistence.rs:534](../../src/series/index/persistence.rs#L534), [:568](../../src/series/index/persistence.rs#L568)).
- `DELAYED_KEYS_COUNTER` ([asm.rs:242](../../src/series/index/asm.rs#L242)) is monotonic and never decremented — a "queued ever" counter, not a depth gauge.

## 2. Design decisions

**Do not duplicate the server.** Valkey already publishes per-command `calls`, `usec`,
`rejected_calls`, `failed_calls` (`INFO commandstats`), p50/p99/p99.9 (`INFO latencystats`), and
`SLOWLOG`. Per-command latency and error rates are out of scope. Module metrics cover only what the
server cannot see: what happened *inside* a call (sample accepted vs ignored vs rejected, chunks
split, series matched, rules pruned), and work outside any call (cron tasks, executors, fanout,
load-time reconciliation).

**Built on `metered`.** Metrics use [`metered`](https://crates.io/crates/metered) 0.10's
OpenMetrics model, pinned at `=0.10.0-rc.1` (a release candidate; 0.9's API is unrelated —
method-measurement macros, a mutex-backed HDR histogram, serde registries — and was rejected).
Every metric is one of:

- **Counter** — a std `AtomicU64` the code bumps with `fetch_add(n, Relaxed)` (metered reads std
  atomics as counters directly), or `counter_value(..).read(..)` over a count kept elsewhere.
  Monotonic. Registered *without* `_total`; metered adds it to the sample name.
- **Gauge** — a std atomic, or `gauge_value(..).read(..)` for values that already live elsewhere
  (executor stats, `INFLIGHT_REQUESTS.len()`, `StaleSet::cardinality`). Integer or `f64`.
- **Histogram** — `metered::BucketHistogram`: cumulative `le` buckets, `sum` and `count`, lock-free
  (a bucket search, an atomic add, a CAS on the `f64` sum). No `max`. Built at first use, so it
  lives in a `LazyLock`. Durations are recorded in **seconds** with the shared
  `duration_buckets()` — 24 powers of two from 1 µs to 2²³ µs — and named `*_seconds`; count
  distributions get their own bounds.

Units follow OpenMetrics: base units (seconds, bytes), named by suffix, declared with `.unit(..)`
so `MetricSchema::validate` checks the suffix. Names in this plan written as `_us`/`_ms` before
the switch now read `_seconds`.

Reads are relaxed loads; a snapshot is not atomic across metrics (documented, acceptable for
diagnostics). Hot-path cost is one uncontended relaxed RMW per event; batch paths (`TS.MADD`,
`TS.ADDBULK`, compaction) add once per batch with `n`, never per sample.

**Snapshot cost is bounded.** No metric may require a keyspace or index walk at snapshot time. That
rules out the index size gauges the first draft took from `index_memory_usage()`; they stay in
`INDEXMEMORY` / `INFO ts_memory`.

**One registry per section.** Each `Section` is a `metered::Registry<'static>` built with
`Registry::with_prefix(section)` in `build_registry` ([metrics.rs](../../src/common/metrics.rs)),
the one place its metrics are declared with their help text and unit. `snapshot(sections)` walks
each selected registry's `schema()` (for kind and help; metered keeps families sorted by name, so `STATS` reports each section by name) and `values()`.
`TS._DEBUG STATS`, the `INFO ts_stats` mirror, a future OpenMetrics export, and the roster tests
all read from the registries.

**Metrics are state, not shadows.** Where the code already keeps a count, the registry reads it
instead of copying it: the cron's tick count (which also schedules its tasks) is registered as a
counter directly, and executor stats through `*_value(..).read(..)` closures.

**`RESET` is a baseline view.** metered values are monotonic, as a scraper expects. `STATS RESET`
records every counter and histogram's current value; `STATS` reports the difference. Nothing a
scraper would read goes backwards.

**Naming.** `<section>_<what>[_<unit>][_total]`, snake_case, OpenMetrics conventions. This
deliberately differs from the camelCase keys of `INDEXMEMORY`/`STRINGPOOLSTATS`: metric names are
shared with the `INFO ts_stats` mirror, and INFO fields are snake_case. Sections:
`ingest`, `retention`, `chunk`, `compaction`, `replication`, `index`, `read`, `fanout`,
`clustermap`, `exec`, `task`, `load`, `restore`, `cron`.

**Labels by name, not by discriminant.** Where a metric has a small fixed label set (fanout error
kind, threading rule, prune reason), expand it to one counter per value at declaration time
(`fanout_errors_timeout_total`, …) and map with a `match`. `FanoutError` is `#[non_exhaustive]`
with `Custom = 255`, so discriminant indexing is out.

**Gated like the rest of `TS._DEBUG`.** The `debug-mode` check
([ts_debug.rs:281-283](../../src/commands/ts_debug.rs#L281-L283)) covers any new subcommand.
Counters are always collected; only the read surface is gated (`INFO ts_stats` later lifts that for
a curated subset).

**Node-local first.** Every value is for this node. Counters increment on replicas too. A cluster
view follows the `STRINGPOOLSTATS`/`INDEXMEMORY` pattern (fan out by default, `LOCAL` opt-out) in a
later phase; counters sum meaningfully, gauges are reported per node.

**Count where the parent process can see it.** `rdb_save`, the index aux save
(`build_aux_payload`), and `aof_rewrite` run in the `BGSAVE` / AOF-rewrite fork child — and the ASM
snapshot path, which emits `TS._RESTORE`, is an AOF rewrite. A counter there is lost for every
background save. Count on the receiving side (load, `TS._RESTORE`) instead.

## 3. The metrics

Each row: name · kind · hook (where the event happens at 6d83b5d75).

### 3.1 `ingest` — the write funnel

There is no single model-level funnel. Single samples go through
`TimeSeries::add_deferring_retention` ([time_series.rs:267-306](../../src/series/time_series.rs#L267-L306)),
which also carries compaction destination writes (`add_dest_bucket`,
[compaction.rs:1253](../../src/series/compaction.rs#L1253)). Batches go through
`merge_samples_into_series` ([bulk_add.rs:410-540](../../src/series/bulk_add.rs#L410-L540)) and never
touch it. `total_samples` has at least six writers and, under lazy retention, counts *stored* not
visible samples — so never derive "added" from a `total_samples` delta.

So outcome counting lives in one helper, `observe_add_results(results, ctx)`, called from the four
command handlers, which also have the `Context` needed to tell replicated writes apart
([context.rs:56-61](../../src/common/context.rs#L56-L61)):

- `TS.ADD` `handle_add` [ts_add.rs:133](../../src/commands/ts_add.rs#L133)
- `TS.INCRBY`/`DECRBY` `handle_update` [ts_incr_decr_by.rs:164](../../src/commands/ts_incr_decr_by.rs#L164)
- `TS.MADD` result vector [ts_madd.rs:90-138](../../src/commands/ts_madd.rs#L90-L138) (includes parse/series-level errors)
- `TS.ADDBULK` `handle_ingest` [ts_addbulk.rs:89-91](../../src/commands/ts_addbulk.rs#L89-L91)

`SampleAddResult` ([types.rs:296-304](../../src/series/types.rs#L296-L304)) is unchanged:
`Ok(Sample)`, `Duplicate` (the `#[default]`), `Ignored(ts)`, `TooOld`, `Error(&str)`. `Ok` covers
append, out-of-order insert, overwrite, and duplicate-policy folds.

| Metric | Kind | Hook |
|---|---|---|
| `ingest_samples_accepted_total` | counter | `Ok` results in `observe_add_results`; batches pass `n` |
| `ingest_samples_ignored_total` | counter | `Ignored`. Decided in `is_duplicate` [types.rs:242-261](../../src/series/types.rs#L242-L261): only when the policy resolves to KeepLast, never for NaN, only for `ts >= last`. `TS.INCRBY` always forces KeepLast ([time_series.rs:1075](../../src/series/time_series.rs#L1075)) |
| `ingest_samples_duplicate_rejected_total` | counter | `Duplicate`. Policy `BLOCK`, **or** a repeated timestamp inside one `TS.ADDBULK` batch under any policy ([ingest_normalize.rs:100-102](../../src/series/ingest_normalize.rs#L100-L102)) |
| `ingest_samples_duplicate_in_batch_total` | counter | the `TS.ADDBULK` in-batch case above, split out so the BLOCK count stays meaningful |
| `ingest_samples_too_old_total` | counter | `TooOld`: [time_series.rs:285-287](../../src/series/time_series.rs#L285-L287), batch `retention_gate` [ingest_normalize.rs:106-108](../../src/series/ingest_normalize.rs#L106-L108). A `TS.MADD` item reported `Ok` can still be removed by the deferred trim ([ingest_normalize.rs:61-67](../../src/series/ingest_normalize.rs#L61-L67)) |
| `ingest_samples_error_total` | counter | `Error` |
| `ingest_samples_overwritten_total` | counter | `upsert_sample` [time_series.rs:402](../../src/series/time_series.rs#L402), chunk length unchanged ([:452-455](../../src/series/time_series.rs#L452-L455), [:474-482](../../src/series/time_series.rs#L474-L482)). Model level, so it also sees compaction destination rewrites; batch out-of-order merges are not split |
| `ingest_samples_inserted_out_of_order_total` | counter | same site, chunk length grew |
| `ingest_madd_inputs_failed_total` | counter | parse errors [ts_madd.rs:210-235](../../src/commands/ts_madd.rs#L210-L235), series-level errors [:103-110](../../src/commands/ts_madd.rs#L103-L110) (missing key — `TS.MADD` no longer auto-creates; ACL → `PERMISSION_DENIED` [:205](../../src/commands/ts_madd.rs#L205)) |
| `ingest_madd_sequential_fallback_total` | counter | `has_in_batch_duplicate` fallback [sample_merge.rs:187-189](../../src/series/sample_merge.rs#L187-L189) |
| `ingest_bulk_samples_total` | counter | `bulk_insert_samples` [bulk_add.rs:545-611](../../src/series/bulk_add.rs#L545-L611), `n` per call |
| `ingest_series_created_total` | counter | `create_and_store_series` [utils.rs:208](../../src/series/utils.rs#L208) — the funnel for `TS.CREATE`, `TS.ADD`, `TS.INCRBY`, `TS.ADDBULK` auto-create |
| `ingest_acl_denied_total` | counter | per-series INSERT denials (`TS.MADD` [:205](../../src/commands/ts_madd.rs#L205), `create_and_store_series` [utils.rs:217](../../src/series/utils.rs#L217)) |

Why: the ignore/duplicate/too-old split is the most common "my data is missing" support question,
and none of it shows in `commandstats` because those calls succeed.

### 3.2 `retention` — lazy vs forced trim

Lazy retention (4099c701f) changed where samples leave. The write path calls `trim_lazily`
([time_series.rs:894](../../src/series/time_series.rs#L894)), which drops whole expired chunks but
defers re-encoding a compressed head chunk until a quarter of its span has expired
(`head_trim_due` [:1361-1369](../../src/series/time_series.rs#L1361-L1369)). The cron trim, defrag,
and `remove_range` call the exact `trim()`. Both meet at `trim_expired(force)`
([:898](../../src/series/time_series.rs#L898)).

| Metric | Kind | Hook |
|---|---|---|
| `retention_samples_trimmed_lazy_total` | counter | `trim_expired(false)`, samples removed ([:909-920](../../src/series/time_series.rs#L909-L920)) |
| `retention_samples_trimmed_forced_total` | counter | `trim_expired(true)` |
| `retention_head_trims_deferred_total` | counter | `head_trim_due` returns false with expired head data |
| `retention_errors_total` | counter | `apply_retention` error arm [:324-326](../../src/series/time_series.rs#L324-L326) |

Why: trimmed counts arrive late and in bursts by design; without the lazy/forced split an operator
can't tell "retention is working" from "the cron is cleaning up after the write path".

### 3.3 `chunk`

| Metric | Kind | Hook |
|---|---|---|
| `chunk_created_total` | counter | `add_chunk_with_sample` [time_series.rs:361-375](../../src/series/time_series.rs#L361-L375), `append_chunk` on an empty series [:395-399](../../src/series/time_series.rs#L395-L399), bulk new chunks [bulk_add.rs:493](../../src/series/bulk_add.rs#L493) |
| `chunk_split_total` | counter | upsert split [time_series.rs:469](../../src/series/time_series.rs#L469); batch `split_chunks_if_needed` [:542](../../src/series/time_series.rs#L542) (parallel) / [:557](../../src/series/time_series.rs#L557) (serial). Batches split only past `max + max/4` (`needs_split` [:522-525](../../src/series/time_series.rs#L522-L525)) |
| `chunk_split_errors_total` | counter | error flag [:549](../../src/series/time_series.rs#L549)/[:562](../../src/series/time_series.rs#L562) → `ChunkSplitError`; [:511](../../src/series/time_series.rs#L511); bulk [bulk_add.rs:562-567](../../src/series/bulk_add.rs#L562-L567) (log-only today) |
| `chunk_sealed_total` | counter | `seal_chunk` [time_series.rs:57-61](../../src/series/time_series.rs#L57-L61) |
| `chunk_seal_errors_total` | counter | `optimize` failure inside `seal_chunk` [:59](../../src/series/time_series.rs#L59) (log-only today) |

Live totals (chunks, samples) need a keyspace walk and stay per-key in `TS.INFO`.

### 3.4 `compaction`

Key-based linking (641c1885f) added a class of silent topology changes: resolving a destination can
**delete the rule** from the source during an ordinary write.

| Metric | Kind | Hook |
|---|---|---|
| `compaction_samples_written_total` | counter | `add_dest_bucket` `Ok` arm [compaction.rs:1255-1256](../../src/series/compaction.rs#L1255-L1256). Cascaded levels count again at each destination |
| `compaction_dest_writes_dropped_total` | counter | `TooOld`/`Ignored` swallowed [:1273-1277](../../src/series/compaction.rs#L1273-L1277) |
| `compaction_errors_surfaced_total` | counter | rule error returned to the client after the sample is stored: `TS.ADD` [ts_add.rs:170-183](../../src/commands/ts_add.rs#L170-L183), `TS.INCRBY` [ts_incr_decr_by.rs:219-223](../../src/commands/ts_incr_decr_by.rs#L219-L223) |
| `compaction_errors_logged_total` | counter | rule error only logged: `apply_rules_internal` [compaction.rs:1105-1116](../../src/series/compaction.rs#L1105-L1116), `TS.MADD` [sample_merge.rs:297-305](../../src/series/sample_merge.rs#L297-L305), `TS.ADDBULK` [bulk_add.rs:598-602](../../src/series/bulk_add.rs#L598-L602) |
| `compaction_rules_pruned_missing_total` | counter | `resolve_destination` → `Stale`, destination missing or wrong type [:1213-1216](../../src/series/compaction.rs#L1213-L1216); rule removed at [:1182-1186](../../src/series/compaction.rs#L1182-L1186) |
| `compaction_rules_pruned_backlink_total` | counter | destination's `src_series` no longer points back [:1217-1224](../../src/series/compaction.rs#L1217-L1224) (deleted and re-created, RENAME over it, restored from another dump) |
| `compaction_rules_pruned_duplicate_dest_total` | counter | two rules on one destination [:1202-1210](../../src/series/compaction.rs#L1202-L1210) |
| `compaction_rules_skipped_cycle_total` | counter | cycle / already-visited → `Skip` [:1198-1200](../../src/series/compaction.rs#L1198-L1200) |
| `compaction_cascade_open_failed_total` | counter | child can't be opened [:938-940](../../src/series/compaction.rs#L938-L940), [:978-980](../../src/series/compaction.rs#L978-L980) |
| `compaction_bucket_rewrites_total` | counter | `recalculate_bucket` [:648](../../src/series/compaction.rs#L648) — writes or removes the destination bucket (upsert/delete into a closed bucket) |
| `compaction_bucket_rescans_total` | counter | `recalculate_current_bucket` [:623](../../src/series/compaction.rs#L623) and `resync_open_bucket_after_removal` [:785](../../src/series/compaction.rs#L785) — source rescans that don't write |
| `compaction_range_removals_total` | counter | `handle_compaction_range_removal` [:697](../../src/series/compaction.rs#L697) |
| `compaction_default_rules_skipped_total` | counter | `add_default_compactions` skip (destination exists or create failed) [utils.rs:261-274](../../src/series/utils.rs#L261-L274) |
| `compaction_rules_restore_rejected_total` | counter | `validate_restored_rule_parameters` [compaction.rs:118-142](../../src/series/compaction.rs#L118-L142) (fails the key's load/restore) |

Why: `compaction_rules_pruned_*` is the strongest case in this plan — a normal write silently
changing what compactions exist, visible today only at verbose log level.

### 3.5 `replication` — primary and replica

Counted in the same command-level helper as §3.1.

| Metric | Kind | Hook |
|---|---|---|
| `replication_writes_verbatim_total` | counter | `TS.ADD` [ts_add.rs:207](../../src/commands/ts_add.rs#L207), `TS.ADDBULK` [ts_addbulk.rs:73](../../src/commands/ts_addbulk.rs#L73), explicit `TS.CREATE` [utils.rs:184](../../src/series/utils.rs#L184) |
| `replication_writes_rewritten_total` | counter | `TS.ADD key *` [ts_add.rs:196-203](../../src/commands/ts_add.rs#L196-L203); `TS.MADD` always rewrites `*` [ts_madd.rs:176-184](../../src/commands/ts_madd.rs#L176-L184), [:276](../../src/commands/ts_madd.rs#L276); `TS.INCRBY` **always** rewrites ([ts_incr_decr_by.rs:234-255](../../src/commands/ts_incr_decr_by.rs#L234-L255)) — the P1 re-derivation fix has landed, so for `INCRBY` this is a constant, not a signal |
| `replication_writes_suppressed_total` | counter | `TS.INCRBY` `Ignored` → `replicate: false` [ts_incr_decr_by.rs:182-185](../../src/commands/ts_incr_decr_by.rs#L182-L185). (`TS.ADD` still replicates `Ignored`) |
| `replication_madd_inputs_dropped_total` | counter | `handle_replication` [ts_madd.rs:265-281](../../src/commands/ts_madd.rs#L265-L281): count `!input.res.is_ok()` over all inputs. Merge-time `TooOld`/`Duplicate`/`Ignored`/`Error` **are** replicated, so "total − successful" is wrong |
| `replication_skipped_after_stored_write_total` | counter | command errors after the sample is stored, so nothing propagates: compaction error in `TS.ADD`/`TS.INCRBY` (above), the `?` at [ts_madd.rs:133](../../src/commands/ts_madd.rs#L133) |
| `replication_received_total` | counter | command handlers, when `ctx` carries `REPLICATED` |

### 3.6 `index` — the read/query funnel

Per-query counting belongs in the *callers*, not in `Terms::postings_for_selectors`
([planner.rs:201-225](../../src/series/index/postings/planner.rs#L201-L225)): `TS.CARD` without a
range uses `postings_for_selector` ([:256](../../src/series/index/postings/planner.rs#L256)) once per
selector, filtered `TS.LABELSTATS` uses `postings_for_label_filters`
([:247](../../src/series/index/postings/planner.rs#L247)), and the inner function runs once per OR
branch.

Query entry points: `series_by_selectors`
([querier.rs:87-111](../../src/series/index/querier.rs#L87-L111), MRANGE/MGET/label search),
`query_labels_distinct` ([:121](../../src/series/index/querier.rs#L121)), `series_keys_by_selectors`
([:209](../../src/series/index/querier.rs#L209), QUERYINDEX), `count_series_by_selectors`
([:238](../../src/series/index/querier.rs#L238), CARD with range), `keys_for_selectors`
([timeseries_index.rs:249](../../src/series/index/timeseries_index.rs#L249), MDEL),
`get_cardinality_by_selectors` ([:320](../../src/series/index/timeseries_index.rs#L320)),
`matching_postings` ([:352](../../src/series/index/timeseries_index.rs#L352)), `stats_filtered`
([:389](../../src/series/index/timeseries_index.rs#L389)), and
[multi_del.rs:90](../../src/series/multi_del.rs#L90).

| Metric | Kind | Hook |
|---|---|---|
| `index_queries_total` | counter | each entry point above, once per command |
| `index_series_matched_total` | counter | same sites, `n` = bitmap `cardinality()` before materialisation |
| `index_series_matched_per_query` | histogram | same sites (unit: series) |
| `index_label_wide_unions_total` | counter | `postings_for_all_label_values` [terms.rs:143-151](../../src/series/index/postings/terms.rs#L143-L151) — every value of **one** label (`inverse_postings_for_filter` [predicate.rs:41-83](../../src/series/index/postings/predicate.rs#L41-L83); `=~".+"`, `!~".+"`, `!=""` at [planner.rs:119](../../src/series/index/postings/planner.rs#L119), [:125](../../src/series/index/postings/planner.rs#L125), [:131](../../src/series/index/postings/planner.rs#L131)) |
| `index_all_postings_base_total` | counter | `self.all()` used as a base — the real whole-keyspace cost: negative-only seed [planner.rs:64-69](../../src/series/index/postings/planner.rs#L64-L69), [:177](../../src/series/index/postings/planner.rs#L177); `!=` [predicate.rs:152](../../src/series/index/postings/predicate.rs#L152)/[:167](../../src/series/index/postings/predicate.rs#L167); not-starts-with [:240](../../src/series/index/postings/predicate.rs#L240); not-contains [:288](../../src/series/index/postings/predicate.rs#L288). `validate_selector_list` needs only *one* bounded selector, so `FILTER a=b c!=d` still pays this for the second |
| `index_label_values_scanned_total` | counter | per-value predicate loops [terms.rs:190-195](../../src/series/index/postings/terms.rs#L190-L195), [:269-274](../../src/series/index/postings/terms.rs#L269-L274) (regex without prefix, `!~`, contains, `with_label`). Literal sets (`Equal(List)`, ≤ 16 values) are point lookups and correctly stay out |
| `index_regex_size_limit_total` | counter | `CompiledTooBig` inside `build_with_repeat_fallback` [regex.rs:102-104](../../src/labels/regex.rs#L102-L104) — the only place it's distinguishable; every caller maps it to "invalid regex". Counted on every node that recompiles (fanout codec [filters.rs:63-75](../../src/commands/fanout_codec/filters.rs#L63-L75)) |
| `index_regex_prefix_fallback_total` | counter | `MAX_REPETITION_PREFIX_BYTES` (4 KiB) / `MAX_OR_VALUES` (16) fallback to the compiler [regex_utils.rs:18-22](../../src/labels/regex_utils.rs#L18-L22) |
| `index_selector_rejected_unbounded_total` | counter | `MISSING_FILTER` in `validate_selector_list` [command_parser.rs:764-770](../../src/commands/command_parser.rs#L764-L770) |
| `index_stale_ids_marked_<source>_total` | counter ×6 | `query`, `keys_for_selectors`, `verify`, `asm_export`, `mdel`, `trim`: the `mark_ids_as_stale` call sites (querier [:109](../../src/series/index/querier.rs#L109)/[:173](../../src/series/index/querier.rs#L173)/[:203](../../src/series/index/querier.rs#L203)/[:229](../../src/series/index/querier.rs#L229)/[:258](../../src/series/index/querier.rs#L258); [timeseries_index.rs:278-299](../../src/series/index/timeseries_index.rs#L278-L299); [persistence.rs:624](../../src/series/index/persistence.rs#L624); [asm.rs:413](../../src/series/index/asm.rs#L413); [multi_del.rs:229](../../src/series/multi_del.rs#L229); [tasks/utils.rs:67](../../src/series/tasks/utils.rs#L67)). They converge at `Postings::mark_ids_as_stale` [stale.rs:141-147](../../src/series/index/postings/stale.rs#L141-L147), which can't tell the source |
| `index_stale_ids_pending` | gauge | `StaleSet::cardinality` [stale.rs:40](../../src/series/index/postings/stale.rs#L40), read at snapshot |
| `index_stale_ids_retired_total` | counter | snapshot cardinality in `finish_pass` [stale.rs:108-112](../../src/series/index/postings/stale.rs#L108-L112). `remove_stale_ids` ([:166-228](../../src/series/index/postings/stale.rs#L166-L228)) returns only a cursor, and the drain works in passes — a per-batch count would be wrong |
| `index_stale_ids_deferred_total` | counter | ids re-marked during a pass and taken back by `mark_many` [stale.rs:93-100](../../src/series/index/postings/stale.rs#L93-L100) |
| `index_label_keys_pruned_total` | counter | `keys_to_remove` [stale.rs:216](../../src/series/index/postings/stale.rs#L216) |
| `index_acl_omitted_total` | counter | series silently skipped by per-key ACL: QUERYLABELS [querier.rs:149](../../src/series/index/querier.rs#L149), `keys_for_selectors` [timeseries_index.rs:286-288](../../src/series/index/timeseries_index.rs#L286-L288) |

Dropped from the first draft: `index_series` / `index_terms` / `index_databases` gauges (they need
the O(n) walk, and already appear in `INFO ts_memory` and `INDEXMEMORY`).

Why: these are the pre-materialisation numbers [filter-dos-audit.md](../topics/filter-dos-audit.md)
needs before choosing limits (F3, no scan budget, is still open). Ship the counters, pick bounds
from real distributions. Note the audit's file:line references point at the old monolithic
`postings.rs` and its F1 description predates the move of the bounded check into
`validate_selector_list`.

### 3.7 `read` — blocking reads (`TS.READ`)

| Metric | Kind | Hook |
|---|---|---|
| `read_blocked_total` | counter | `block_client_on_key` success [block_on_keys.rs:135-140](../../src/common/block_on_keys.rs#L135-L140) |
| `read_served_without_block_total` | counter | data already present [ts_read.rs:441](../../src/commands/ts_read.rs#L441) |
| `read_block_refused_total` | counter | deny-blocking context [ts_read.rs:449-451](../../src/commands/ts_read.rs#L449-L451), server refusal [:462](../../src/commands/ts_read.rs#L462) |
| `read_block_timeouts_total` | counter | `timeout_callback` [block_on_keys.rs:182-199](../../src/common/block_on_keys.rs#L182-L199) — also fires on `CLIENT UNBLOCK` |
| `read_spurious_wakeups_total` | counter | `reply_callback` → `NotReady`, client stays blocked ([ts_read.rs:345](../../src/commands/ts_read.rs#L345), [:355](../../src/commands/ts_read.rs#L355)) |
| `read_unblocked_by_delete_total` | counter | [ts_read.rs:347](../../src/commands/ts_read.rs#L347) |
| `read_blocked_clients` | gauge | +1 at [block_on_keys.rs:140](../../src/common/block_on_keys.rs#L140); −1 in `free_privdata_callback` [:206-216](../../src/common/block_on_keys.rs#L206-L216), which runs exactly once per successful block. Not on wake/timeout: wakes can return `NotReady`, and disconnect/shutdown skip both callbacks |

### 3.8 `fanout` and `clustermap` — the cluster path

Two structural facts shape this section:

- **Local work bypasses the RPC layer.** A fanout whose only target is this node returns early at
  [fanout_command.rs:135-139](../../src/fanout/fanout_command.rs#L135-L139) — no RPC, no
  `InFlightRequest`, no timer. In a mixed fanout the local share is spawned separately
  ([:185-192](../../src/fanout/fanout_command.rs#L185-L192)) and excluded from `node_count`. So
  request/target counts and duration live at `exec_command`
  ([:106-123](../../src/fanout/fanout_command.rs#L106-L123)), not `send_cluster_request`.
- **There are two timeout paths for one client-visible timeout.** The blocked-client timer
  ([blocked_client.rs:174-181](../../src/fanout/blocked_client.rs#L174-L181)) is armed first and
  normally fires first; it's the only one for local-only fanouts. The RPC timer
  (`on_request_timeout` [cluster_rpc.rs:198-210](../../src/fanout/cluster_rpc.rs#L198-L210)) exists
  only with remote targets. They get distinct names; don't sum them.

Coordinator side:

| Metric | Kind | Hook |
|---|---|---|
| `fanout_requests_total` | counter | `exec_command` once targets are known [fanout_command.rs:117-120](../../src/fanout/fanout_command.rs#L117-L120) |
| `fanout_targets_total` | counter | `targets.len()` at the same site, **including** the local node |
| `fanout_local_only_total` | counter | early return [:135-139](../../src/fanout/fanout_command.rs#L135-L139) |
| `fanout_setup_failures_total` | counter | `invoke_rpc` failure [:162-180](../../src/fanout/fanout_command.rs#L162-L180): `validate_cluster_exec` [cluster_rpc.rs:278](../../src/fanout/cluster_rpc.rs#L278), no remote targets [:301-303](../../src/fanout/cluster_rpc.rs#L301-L303) (surfaces as `NodeUnreachable`) |
| `fanout_blocking_denied_total` | counter | refused in MULTI/Lua/no-block contexts [fanout_client_command.rs:51-53](../../src/fanout/fanout_client_command.rs#L51-L53) |
| `fanout_send_failures_total` | counter | `dispatch_send_failure` [cluster_rpc.rs:212-217](../../src/fanout/cluster_rpc.rs#L212-L217) |
| `fanout_client_timeouts_total` | counter | blocked-client `timeout_callback` (above) |
| `fanout_rpc_timeouts_total` | counter | `on_request_timeout` (above) |
| `fanout_local_share_expired_total` | counter | local share waited in the queue past the deadline [fanout_command.rs:440-443](../../src/fanout/fanout_command.rs#L440-L443) |
| `fanout_responses_ignored_<reason>_total` | counter ×4 | `unknown_request` `with_inflight_request` [cluster_rpc.rs:634-638](../../src/fanout/cluster_rpc.rs#L634-L638); `unknown_sender` [:100-108](../../src/fanout/cluster_rpc.rs#L100-L108); `duplicate` [:110-116](../../src/fanout/cluster_rpc.rs#L110-L116); `after_completion` [fanout_command.rs:249-251](../../src/fanout/fanout_command.rs#L249-L251), [:293-300](../../src/fanout/fanout_command.rs#L293-L300) |
| `fanout_errors_<kind>_total` | counter ×14 | `FanoutStateInner::on_error` [fanout_command.rs:255](../../src/fanout/fanout_command.rs#L255) — the only place the shard's kind survives. Kinds ([fanout_error.rs:17-61](../../src/fanout/fanout_error.rs#L17-L61)): `invalid_message`, `node_unreachable`, `timeout`, `unknown_message_type`, `permissions`, `key_permissions`, `serialization`, `bad_request_id`, `internal`, `cluster_map_mismatch`, `unsupported_features`, `invalid_db`, `busy`, `custom` |
| `fanout_aborts_total` | counter | `abort_error` set: timeout [:256-262](../../src/fanout/fanout_command.rs#L256-L262), map mismatch [:267-270](../../src/fanout/fanout_command.rs#L267-L270), permissions/key-permissions/busy [:279-285](../../src/fanout/fanout_command.rs#L279-L285) — returned to the client verbatim |
| `fanout_generic_error_replies_total` | counter | `on_completion` `error_count > 0` [:342-343](../../src/fanout/fanout_command.rs#L342-L343) — every other kind collapses to "Internal error in fanout operation" here. Pair it with the per-kind counters until that's redesigned |
| `fanout_error_decode_failures_total` | counter | peer error payload undecodable [cluster_rpc.rs:709-713](../../src/fanout/cluster_rpc.rs#L709-L713) |
| `fanout_pushdown_fallbacks_total` | counter | `normalize_response_series` fallback [ts_mrange_fanout_command.rs:319-322](../../src/commands/ts_mrange_fanout_command.rs#L319-L322), only when `pushdown && !is_multi_aggregation` ([:308](../../src/commands/ts_mrange_fanout_command.rs#L308)). Counting `bucketed == false` at [:244](../../src/commands/ts_mrange_fanout_command.rs#L244) overcounts: with pushdown off every response is unbucketed |
| `fanout_pushdown_group_fallbacks_total` | counter | group-reduce compensation [:273-277](../../src/commands/ts_mrange_fanout_command.rs#L273-L277) / `compensate_group_partials` [:350](../../src/commands/ts_mrange_fanout_command.rs#L350) |
| `fanout_duration_seconds` | histogram | `Instant` already created at [fanout_command.rs:123](../../src/fanout/fanout_command.rs#L123) (the deadline); carry it in the fanout state and observe at `on_completion` [:327](../../src/fanout/fanout_command.rs#L327) and on timeout. Covers local-only fanouts too |
| `fanout_inflight` | gauge | `INFLIGHT_REQUESTS.len()` [cluster_rpc.rs:154](../../src/fanout/cluster_rpc.rs#L154) — remote-target fanouts only; document it |

Wire volume — **done 2026-10-07**, counted at the one send choke point, `send_cluster_message`
in [cluster_rpc.rs](../../src/fanout/cluster_rpc.rs), by message type, only when the bus accepts
the message. Payload bytes only (the bus adds its own framing); one count per peer; the local
share never crosses the bus. Received bytes are not counted yet (`on_*_received` carry `len`).

| Metric | Kind | Hook |
|---|---|---|
| `fanout_requests_sent_total` / `fanout_request_sent_bytes_total` | counter | `FANOUT_REQUEST_MESSAGE` sends (coordinator side) |
| `fanout_responses_sent_total` / `fanout_response_sent_bytes_total` | counter | `FANOUT_RESPONSE_MESSAGE` sends (serving side) |
| `fanout_error_responses_sent_total` / `fanout_error_response_sent_bytes_total` | counter | `FANOUT_ERROR_MESSAGE` sends (serving side) |

Serving side:

| Metric | Kind | Hook |
|---|---|---|
| `fanout_served_ok_total` / `fanout_served_errors_total` / `fanout_reply_send_failures_total` | counter | `process_request_message` on the worker [cluster_rpc.rs:460-519](../../src/fanout/cluster_rpc.rs#L460-L519): ok [:496](../../src/fanout/cluster_rpc.rs#L496), handler error [:512](../../src/fanout/cluster_rpc.rs#L512), send failure [:505-510](../../src/fanout/cluster_rpc.rs#L505-L510) |
| `fanout_serve_rejected_<reason>_total` | counter ×4 | `parse` [:408-416](../../src/fanout/cluster_rpc.rs#L408-L416), `unsupported_features` [:571-582](../../src/fanout/cluster_rpc.rs#L571-L582), `no_handler` [:584-593](../../src/fanout/cluster_rpc.rs#L584-L593), `busy` [:618-623](../../src/fanout/cluster_rpc.rs#L618-L623) |
| `fanout_fingerprint_rejects_total` | counter | [cluster_rpc.rs:474-488](../../src/fanout/cluster_rpc.rs#L474-L488) (topology skew seen by the serving node) |

Cluster map ([fanout/mod.rs](../../src/fanout/mod.rs)):

| Metric | Kind | Hook |
|---|---|---|
| `clustermap_refreshes_total` | counter | `refresh_cluster_map` [mod.rs:138](../../src/fanout/mod.rs#L138) |
| `clustermap_refresh_unchanged_total` | counter | unchanged branch [:143-148](../../src/fanout/mod.rs#L143-L148) (extends expiry, grows interval) |
| `clustermap_refresh_changed_total` | counter | changed branch [:149-154](../../src/fanout/mod.rs#L149-L154) (also taken when either map is inconsistent) |
| `clustermap_refresh_failures_total` | counter | [:156-159](../../src/fanout/mod.rs#L156-L159) |
| `clustermap_forced_refreshes_total` | counter | fingerprint mismatch on the serving worker [cluster_rpc.rs:449](../../src/fanout/cluster_rpc.rs#L449) |
| `clustermap_stale_marks_total` | counter | `mark_cluster_map_stale` [mod.rs:90-92](../../src/fanout/mod.rs#L90-L92) (only caller: a peer's `ClusterMapMismatch`, [cluster_rpc.rs:705](../../src/fanout/cluster_rpc.rs#L705)) |
| `clustermap_refresh_interval_seconds` | gauge | `CLUSTER_MAP_REFRESH_INTERVAL_MS` [mod.rs:62](../../src/fanout/mod.rs#L62). Now adaptive: doubles per unchanged refresh up to 5000 ms ([:55](../../src/fanout/mod.rs#L55), [:68-75](../../src/fanout/mod.rs#L68-L75)), resets on change/failure; **0 until the first refresh** |
| `clustermap_age_seconds` | gauge | needs a new stored "last verified" timestamp, set at build ([cluster_map.rs:892](../../src/fanout/cluster_map.rs#L892)) and in `extend_expiration` ([:907-912](../../src/fanout/cluster_map.rs#L907-L912)). It can't be derived from `expiration_ts` any more because the TTL is adaptive |

Why: the per-kind error counters are the only way an operator can tell a timeout storm from a
permissions misconfiguration while the generic-error collapse stands.

### 3.9 `exec` — executors, background threads, threading rules (new)

The threading refactor (#124) and bounded executors (82703430a) added state the first draft didn't
know about. Two lanes exist, each with `lane_workers()` = `num_threads().clamp(2, 8)` workers
([threads/mod.rs:124-126](../../src/common/threads/mod.rs#L124-L126)):

- `ts-fanout-request` — `PEER_REQUEST_EXECUTOR` ([fanout/workers.rs:19-23](../../src/fanout/workers.rs#L19-L23), cap 1024). Since ef5c32e55 it carries **both** peer requests and the coordinator's local share.
- `ts-analysis` — `ANALYSIS_EXECUTOR` ([analysis_runner.rs:16-20](../../src/commands/analysis_runner.rs#L16-L20), cap 256).

| Metric | Kind | Hook |
|---|---|---|
| `exec_<lane>_queued` / `exec_<lane>_running` | gauge | **done (Phase 0)**, `<lane>` = `fanout` / `analysis`. `BoundedExecutor::stats()` [executor.rs:131-137](../../src/common/threads/executor.rs#L131-L137), read at snapshot — no new atomics |
| `exec_<lane>_rejected_total` | counter | **done (Phase 0)**, `counter_value` over `rejected` (incremented on QueueFull [:116](../../src/common/threads/executor.rs#L116)) |
| `exec_<lane>_not_running_rejects_total` | counter | `NotRunning` reject [:107-109](../../src/common/threads/executor.rs#L107-L109) (uncounted today) |
| `exec_<lane>_job_panics_total` | counter | worker `catch_unwind` [:175-181](../../src/common/threads/executor.rs#L175-L181) |
| `exec_fanout_local_share_busy_total` | counter | local-share rejection [fanout_command.rs:137-138](../../src/fanout/fanout_command.rs#L137-L138), [:187-190](../../src/fanout/fanout_command.rs#L187-L190) — the merged lane's `rejected` can no longer tell local from peer |
| `exec_background_panics_total` | counter | `spawn_background` `catch_unwind` [threads/mod.rs:153-158](../../src/common/threads/mod.rs#L153-L158) |
| `exec_background_spawn_failures_total` | counter | thread spawn failed, job dropped [threads/mod.rs:161](../../src/common/threads/mod.rs#L161) |
| `exec_threading_rule_<rule>_total` | counter ×4 | `violated()` in [gil.rs](../../src/common/threads/gil.rs) (~:160-174): `pool_worker_takes_gil`, `gil_reentry`, `gil_holder_waits_on_blocking_pool`, `blocking_wait`. Today only the first violation per rule is logged and the rest are dropped |

Why: rejections are client-visible `Busy` errors, and the threading-rule log fires once per process
lifetime — a counter is the only way to see a rule being broken continuously.

### 3.10 `task` — background work

Every periodic task now runs through `spawn_background_single(name, &SingleFlight, job)`
([threads/mod.rs:167-179](../../src/common/threads/mod.rs#L167-L179)), which **silently skips the
tick** when the previous run is still in flight. That makes it the natural home for the generic
part of the record: put the `TaskRun` guard inside it, keyed by task name, and every task gets
`runs_total`, `skipped_total`, `panics_total`, `last_run_timestamp_seconds`, `last_duration_seconds`,
`duration_seconds` (histogram), and `running` for free. Tasks only add `items_total` / `errors_total`.

Cron intervals are unchanged ([background_tasks.rs:15-18](../../src/series/background_tasks.rs#L15-L18)).

| Task | Trigger / spawn | `items` means | Hooks |
|---|---|---|---|
| `trim` | cron 10 s, `ts-series-trim` ([series_trim.rs:27-33](../../src/series/tasks/series_trim.rs#L27-L33)) | samples deleted | body [:35-120](../../src/series/tasks/series_trim.rs#L35-L120); `processed` [:94](../../src/series/tasks/series_trim.rs#L94), `total_deletes` [:96-108](../../src/series/tasks/series_trim.rs#L96-L108), errors [:100-106](../../src/series/tasks/series_trim.rs#L100-L106). Role changed with lazy retention: it now reclaims deferred head data and idle series (see §3.2) |
| `stale_ids` | cron 20 s, `ts-stale-ids` ([stale_ids.rs:99-106](../../src/series/tasks/stale_ids.rs#L99-L106)) | ids retired (§3.6, from `finish_pass`) | body [:57-69](../../src/series/tasks/stale_ids.rs#L57-L69). The full-sweep `remove_all_stale_series_internal` ([:76-97](../../src/series/tasks/stale_ids.rs#L76-L97)) shares the SingleFlight and silently returns if an incremental run holds it ([:77-79](../../src/series/tasks/stale_ids.rs#L77-L79)) — count that as `task_stale_ids_full_sweep_skipped_total` |
| `optimize` | cron 60 s, `ts-optimize-indices` ([optimize_indices.rs:34-42](../../src/series/tasks/optimize_indices.rs#L34-L42)) | — (only a cursor is returned) | body [:45-70](../../src/series/tasks/optimize_indices.rs#L45-L70); missing db [:56-64](../../src/series/tasks/optimize_indices.rs#L56-L64) |
| `trim_unused_dbs` | cron 300 s, **inline on the main thread** ([background_tasks.rs:124-134](../../src/series/background_tasks.rs#L124-L134)) | dbs dropped (needs counting; only logged per db) | not spawned, so wrap with `TaskRun` by hand |
| `asm_drain` | ASM `ImportCompleted`, `ts-delayed-indexing` via plain `spawn_background` ([asm.rs:375-400](../../src/series/index/asm.rs#L375-L400)) | keys indexed (`indexed − skipped`) | `process_delayed_keys_for_db` [asm.rs:333-368](../../src/series/index/asm.rs#L333-L368); shutdown abort [:350-356](../../src/series/index/asm.rs#L350-L356), [:392-395](../../src/series/index/asm.rs#L392-L395). The single-run guard was removed on purpose ([:372-374](../../src/series/index/asm.rs#L372-L374)): concurrent drains take disjoint slots, so `running` is a count, not 0/1 |
| `reconcile` | load end, `ts-index-sweep` ([persistence.rs:448-502](../../src/series/index/persistence.rs#L448-L502)) | dangling ids | `reconcile_db` [:574-637](../../src/series/index/persistence.rs#L574-L637); `verify_and_repair_db` [:533-572](../../src/series/index/persistence.rs#L533-L572) → `load_repair_reindexed_total`. Skipped = digest match [:475-484](../../src/series/index/persistence.rs#L475-L484). Split the run reason ([:486-497](../../src/series/index/persistence.rs#L486-L497)): `digest_incomplete`, `keys_expired` (`force_sweep` from `rdb_last_load_keys_expired`, [server_events.rs:65](../../src/series/index/server_events.rs#L65), [:76-81](../../src/series/index/server_events.rs#L76-L81)), `mismatch` |
| `post_migration_cleanup` | ASM, `ts-asm-cleanup` ([asm.rs:513-545](../../src/series/index/asm.rs#L513-L545)) | keys marked stale (`deleted_count` [:532-535](../../src/series/index/asm.rs#L532-L535)) | `remove_non_owned_keys` [:404-491](../../src/series/index/asm.rs#L404-L491). The follow-up full sweep ([:538](../../src/series/index/asm.rs#L538)) can be skipped by the SingleFlight above |

Gauges and counters outside the per-task records:

| Metric | Kind | Hook |
|---|---|---|
| `task_asm_delayed_keys_pending` | gauge | sum of Vec lengths in `DELAYED_KEYS_MAP` [asm.rs:241](../../src/series/index/asm.rs#L241) — **not** the map length (empty entries are kept, [:324](../../src/series/index/asm.rs#L324)), and not `DELAYED_KEYS_COUNTER` |
| `task_asm_importing_slots` | gauge | `IMPORTING_SLOTS` count [asm.rs:552-553](../../src/series/index/asm.rs#L552-L553) (replaces the old `IN_SLOT_IMPORT` bool) |
| `task_asm_keys_discarded_total` | counter | `ImportAborted` → `discard_delayed_keys_in_slots` [asm.rs:564-569](../../src/series/index/asm.rs#L564-L569) (count is currently `let _`) |
| `task_asm_id_collisions_remapped_total` | counter | imported-id collision branches [index/mod.rs:217-260](../../src/series/index/mod.rs#L217-L260); the branch at [:244-260](../../src/series/index/mod.rs#L244-L260) also drops compaction rules → `task_asm_compaction_links_dropped_total` |
| `cron_ticks_total` / `cron_interval_seconds` | counter / gauge | **done (Phase 0)**: existing statics in [background_tasks.rs](../../src/series/background_tasks.rs); the tick count the cron schedules off is registered directly |
| `cron_ticks_skipped_total` | counter | **done (Phase 0)**: loading/shutdown early return in `__cron_event_handler` |
| `cron_tick_duration_seconds` | histogram | **done (Phase 0)**: main-thread time per tick spent dispatching (including the inline `trim_unused_dbs`) |

### 3.11 `load`, `restore` — persistence and recovery

Save-side counters (`rdb_save`, `build_aux_payload`, `aof_rewrite`) are dropped: they run in the
fork child (see §2). Everything here runs in the server process.

| Metric | Kind | Hook |
|---|---|---|
| `load_series_total` | counter | `rdb_load` ok [series_data_type.rs:98](../../src/series/series_data_type.rs#L98) |
| `load_series_failures_total` | counter | `rdb_load` failure [:99-105](../../src/series/series_data_type.rs#L99-L105) |
| `load_type_encver_rejected_total` | counter | `aux_load` type encoding version reject [:129-135](../../src/series/series_data_type.rs#L129-L135) (fails the load) |
| `load_index_payloads_total` | counter | per-db preload success [persistence.rs:249-259](../../src/series/index/persistence.rs#L249-L259) |
| `load_index_payloads_rejected_total` | counter | `parse_aux_payload` version/magic/truncation [persistence.rs:175-198](../../src/series/index/persistence.rs#L175-L198), discard log [:261-265](../../src/series/index/persistence.rs#L261-L265) |
| `load_index_payloads_discarded_disabled_total` | counter | persist disabled → discard [:241-246](../../src/series/index/persistence.rs#L241-L246) |
| `load_index_read_failures_total` | counter | hard read failure [:236-239](../../src/series/index/persistence.rs#L236-L239) |
| `load_bulk_keys_total` | counter | `drain_buffers` [bulk_build.rs:162](../../src/series/index/bulk_build.rs#L162) |
| `load_bulk_drain_duration_seconds` | histogram | [:163-169](../../src/series/index/bulk_build.rs#L163-L169), already measured |
| `load_bulk_degraded_total` | counter | memory-cap crossing [:129-137](../../src/series/index/bulk_build.rs#L129-L137) |
| `load_bulk_buffer_bytes` | gauge | `BULK_BUFFER_BYTES` [:42](../../src/series/index/bulk_build.rs#L42) |
| `load_discarded_keys_total` | counter | `on_load_failed` [:74-89](../../src/series/index/bulk_build.rs#L74-L89) |
| `load_active` / `load_bulk_active` / `load_bulk_degraded` / `load_flushing` | gauge | `LOADING_ACTIVE` [persistence.rs:53](../../src/series/index/persistence.rs#L53), `BULK_ACTIVE` [bulk_build.rs:45](../../src/series/index/bulk_build.rs#L45), `BULK_DEGRADED` [:49](../../src/series/index/bulk_build.rs#L49), `IS_FLUSHING` [series_data_type.rs:79](../../src/series/series_data_type.rs#L79). (`IS_PERSISTING` no longer exists) |
| `load_persistence_failures_total` | counter | persistence event failure [server_events.rs:39](../../src/series/index/server_events.rs#L39) |
| `load_defrag_errors_total` | counter | type defrag callback error [series_data_type.rs:265-267](../../src/series/series_data_type.rs#L265-L267) |
| `restore_total` | counter | `ts_restore_cmd` [ts_restore.rs:44](../../src/commands/ts_restore.rs#L44) — this is also where AOF-rewrite and ASM snapshot output is counted |
| `restore_rejected_direct_total` | counter | direct-client guard [:45-49](../../src/commands/ts_restore.rs#L45-L49) (waived under debug-mode) |
| `restore_encver_rejected_total` | counter | [:53-60](../../src/commands/ts_restore.rs#L53-L60) |
| `restore_deserialize_failures_total` | counter | [:73-75](../../src/commands/ts_restore.rs#L73-L75) |
| `restore_replaced_existing_total` | counter | [:84-88](../../src/commands/ts_restore.rs#L84-L88) |
| `restore_index_deferred_total` / `restore_index_immediate_total` | counter | [:99-103](../../src/commands/ts_restore.rs#L99-L103) |

### 3.12 Reserved

`errors_panics_caught_total` — for the command-handler FFI `catch_unwind` barrier, still unmerged
(`panic-barrier` branch). Declared now so the roster doesn't change shape later. The site-specific
panic counters in §3.9 cover the barriers that already exist (executor, `spawn_background`; the
`TS.READ` callbacks at [block_on_keys.rs:172](../../src/common/block_on_keys.rs#L172),
[:194](../../src/common/block_on_keys.rs#L194), [:215](../../src/common/block_on_keys.rs#L215) can
feed `read_callback_panics_total`).

## 4. The `TS._DEBUG` surface

New subcommands sit behind the existing `debug-mode` gate, reply themselves and return `NoReply`
([ts_debug.rs:278-279](../../src/commands/ts_debug.rs#L278-L279)), and follow the flat key/value
convention ([ts_debug_configs.rs:39-54](../../src/commands/ts_debug_configs.rs#L39-L54),
`reply_with_index_memory` [ts_debug.rs:194-212](../../src/commands/ts_debug.rs#L194-L212)) — a map
on RESP3, an array on RESP2. For variable-length lists use `reply_with_counted_array`
([raw_replies.rs:255-271](../../src/common/replies/raw_replies.rs#L255-L271)); its `emit` must write
exactly one element per item, so flat pairs need one wrapper array per pair.

### `TS._DEBUG STATS [section ...] [VERBOSE]`

- No args: every section, flat `name value …`. Counters and gauges are integers; time-derived
  gauges are ms.
- `section ...`: restrict to the named sections (§2 list). Unknown section → error listing valid names.
- Histograms reply nested under their name: `count N sum S buckets [[le c] …]`, cumulative,
  Prometheus-shaped. `sum` and `le` are doubles (seconds for durations); only finite buckets are
  listed, since the `+Inf` bucket's count is `count`.
- `task` replies one nested record per task: `name trim runs_total … running 0`.
- `VERBOSE`: each entry becomes `name value kind description`, like `LIST_CONFIGS VERBOSE`.

### `TS._DEBUG STATS RESET`

Starts every counter and histogram over from zero as `STATS` reports them, by recording a
baseline (the metered values underneath keep counting); gauges untouched. Takes no
further arguments. Not replicated. Lets integration tests assert
exact deltas, and lets an operator start a clean window during an incident.

### `TS._DEBUG INFLIGHT`

Live view of `INFLIGHT_REQUESTS`: `id`, `age_us`, `outstanding`, `command`. Needs two new
`InFlightRequest` fields ([cluster_rpc.rs:46-57](../../src/fanout/cluster_rpc.rs#L46-L57)): the
start `Instant` and the handler name (neither is stored today). Limits to document: local-only
fanouts never appear (they have no entry), and `timed_out` is not worth showing because a timed-out
entry is removed immediately ([cluster_rpc.rs:208](../../src/fanout/cluster_rpc.rs#L208)).

### `TS._DEBUG HELP`

Add the three entries above. Remove the phantom `SHOW_INFO` line and the phantom
`[APP|DEV|HIDDEN]` on `LIST_CONFIGS` (or implement them; removal is smaller). Regenerate
[ts._debug.md](../commands/ts._debug.md) (`:71-76` example; the `:41-46` table also lacks
`INDEXMEMORY` and `QUERYINDEX`), and extend `test_debug_help`
([tests/test_ts_debug.py:16-30](../../tests/test_ts_debug.py#L16-L30)), which checks only two
entries.

### Later: `INFO ts_stats`

A second `#[info_command_handler]` beside `memory_info` emitting a curated subset (no histograms,
no per-task records) so exporters scrape without `debug-mode`. The builder takes `u64`/`i64`/`f64`/
string fields and dictionaries (`InfoContextBuilderFieldTopLevelValue::Dictionary`), so the
`fanout_errors_*` family maps onto one dictionary field; the registry snapshot should hand out
`u64`/`i64` directly (the builder has no `From<usize>`, hence the casts in `module_info.rs`). This
is the shipping vehicle for proposal item 7; `STATS` is the superset for humans.

## 5. Work plan

Each phase is independently mergeable, adds its own tests, and updates
[docs/commands/ts._debug.md](../commands/ts._debug.md).

### Phase 0 — registry and skeleton (small) — **done 2026-10-07**

As built: [metrics.rs](../../src/common/metrics.rs) (per-section `metered` registries, the
`RESET` baseline, unit and roster tests), [ts_debug_stats.rs](../../src/commands/ts_debug_stats.rs),
`TestDebugStats` in [tests/test_ts_debug.py](../../tests/test_ts_debug.py), and the `STATS`
section of [ts._debug.md](../commands/ts._debug.md). Differences from the list below:

- `metered` registries instead of a hand-rolled `define_metrics!` registry (§2). The roster
  tests check metered's `MetricSchema::validate` (unit suffixes) besides names, help and kinds.
- Histograms have no `max` (metered keeps none) and record seconds.
- `TaskStats` / `TaskRun` are deferred to their first users (Phase 3): nothing would exercise
  them yet.
- `Section` lists only sections that have metrics; a roster test fails on an empty one. Later
  phases add their variant with their first metric.
- Two cron metrics came forward: `cron_ticks_skipped_total` (was Phase 3) and
  `cron_tick_duration_seconds` (new — it exercises the histogram reply end to end).
- The stale "two lanes" doc in `fanout/workers.rs` was fixed in passing.

Original list:

1. `src/common/metrics.rs`: `Counter`, `Gauge` (stored and snapshot-closure), `Histogram`,
   `TaskStats`, `TaskRun`, `define_metrics!`, `snapshot(sections)`, `reset()`. Unit tests: histogram
   bucket boundaries (`v=1 → 0`, `v=2^23 → last`, overflow), `reset` leaves gauges alone, stable
   snapshot order.
2. Roster tests modelled on the `CONFIGS` tests ([config.rs:1339-1446](../../src/config.rs#L1339-L1446)):
   non-empty descriptions, unique snake_case names, counters end in `_total`, section prefix matches.
3. `TS._DEBUG STATS` / `STATS RESET` / HELP cleanup in [ts_debug.rs](../../src/commands/ts_debug.rs).
   First metrics are the ones that already exist and cost nothing: `cron_ticks_total`,
   `cron_interval_ms`, and the `exec_<lane>_{queued,running,rejected_total}` read from
   `BoundedExecutor::stats()`.
4. Integration tests in [tests/test_ts_debug.py](../../tests/test_ts_debug.py) (base
   `ValkeyTimeSeriesTestCaseDebugMode`): even-length reply; `STATS cron` returns only its keys;
   unknown section errors; `RESET` zeroes a counter; rejected with `debug-mode` off; HELP lists the
   new entries and no phantoms.

### Phase 1 — ingest, retention, chunk, compaction, replication

Sections 3.1–3.5. `observe_add_results` in the four handlers; model-level counters only where the
handler can't see the distinction (overwrite vs insert, chunk events, retention, compaction).
Tests: `TS.ADD` with `ON_DUPLICATE BLOCK`, `IGNORE` thresholds, retention, out-of-order — exact
deltas after `STATS RESET`; `TS.ADDBULK` with an in-batch repeat lands in
`ingest_samples_duplicate_in_batch_total`, not the BLOCK counter; delete a compaction destination
and write to the source → `compaction_rules_pruned_missing_total` = 1; a replication test asserting
`replication_writes_rewritten_total` for `TS.ADD key *` on the primary and
`replication_received_total` on the replica. Benchmark `TS.ADD`/`TS.MADD` before/after.

### Phase 2 — index, read

Sections 3.6, 3.7. Tests: regex filter over N label values → `index_label_values_scanned_total` +N;
`FILTER a=b c!=d` → `index_all_postings_base_total` ≥ 1; a literal set (`a=(x,y)`) does **not**
move the scan counter; delete a key underneath the index and query → some
`index_stale_ids_marked_*_total` ≥ 1 and `index_stale_ids_pending` eventually back to 0 (assert ≥,
never which self-healer won — the sweep races the trim cron); `TS.READ BLOCK` with a short timeout →
`read_block_timeouts_total` +1 and `read_blocked_clients` back to 0; disconnect while blocked →
`read_blocked_clients` back to 0.

### Phase 3 — exec, tasks, persistence, recovery

Sections 3.9–3.11. `TaskRun` inside `spawn_background_single`; replace the throw-away locals in §1.
Tests: small retention, wait for `task_trim_runs_total` to advance instead of grepping logs;
`DEBUG RELOAD` asserting `load_series_total` equals the series count and `load_index_payloads_total`
equals the db count (no save-side assertion — `BGSAVE` counts would be lost in the child); malformed
`TS._RESTORE` ([tests/test_ts_restore_malformed.py](../../tests/test_ts_restore_malformed.py)) →
`restore_deserialize_failures_total`; ASM drain in [tests/test_ts_asm.py](../../tests/test_ts_asm.py)
→ `task_asm_drain_items_total` equals the migrated key count, `task_asm_delayed_keys_pending` and
`task_asm_importing_slots` return to 0, and `restore_total` on the destination is non-zero.

### Phase 4 — fanout, cluster map, `INFLIGHT`

Section 3.8 plus `INFLIGHT`. Tests on `ValkeyTimeSeriesClusterTestCase` (hash tags per primary):
`TS.MRANGE` across three primaries → `fanout_requests_total` +1 and `fanout_targets_total` +3 on the
coordinator, `fanout_served_ok_total` +1 on each peer; a hash-tagged query owned by the coordinator
→ `fanout_local_only_total` +1 and no `fanout_inflight` change; `ts-fanout-command-timeout` 1 ms
against a paused peer (`DEBUG SLEEP` via a second client) → `fanout_client_timeouts_total` and
`fanout_errors_timeout_total`; `ts-fanout-aggregation-pushdown` off on one peer only →
`fanout_pushdown_fallbacks_total`, and off everywhere → it stays 0.

### Phase 5 — `INFO ts_stats` mirror and docs

1. `stats_info` in [module_info.rs](../../src/common/module_info.rs) with the curated subset; a
   [tests/test_ts_memory_reporting.py](../../tests/test_ts_memory_reporting.py)-style test parsing
   `INFO ts_stats`.
2. New `docs/topics/observability.md` — metric reference generated from registry descriptions, with
   a roster test that fails on an undocumented metric, and a symptom → metric section (missing
   samples → `ingest_samples_ignored_total` / `_too_old_total`; compactions vanished →
   `compaction_rules_pruned_*`; slow MRANGE → `index_label_values_scanned_total`,
   `index_series_matched_per_query`; cluster errors → `fanout_errors_*`, `fanout_generic_error_replies_total`;
   `Busy` errors → `exec_*_rejected_total`).
3. Update the Operations row of `docs/proposal.md` (on the `proposal` branch).

### Phase 6 — cluster view (optional)

`STATS` fans out by default with a `LOCAL` opt-out, following
`ts_index_memory_fanout_command.rs`: counters and histograms summed, gauges reported per node. Adds
an eleventh fanout op and a proto message (regenerate under `proto/v1/generated/`).

## 6. Out of scope, on purpose

- Per-command latency and error rates (server `commandstats` / `latencystats` / `SLOWLOG`).
- Anything needing a keyspace or index walk at snapshot time (total samples, chunks, index size).
- Counting in fork children (save-side RDB/AOF work).
- Structured logging and slow-query logging (separate proposal items).
- Enforcing new limits. The `index_*` counters exist so limits can be chosen from data; existing
  rejections (unbounded selector, regex size, `MAX_TS_VALUES_FILTER`) are counted, not changed.

## 7. Risks and mitigations

| Risk | Mitigation |
|---|---|
| Hot-path overhead | One relaxed atomic per event; batches add `n`. Benchmark `TS.ADD`/`TS.MADD` with `cargo bench --features enable-system-alloc` before/after Phase 1; budget ≤ 1 %. |
| False sharing between workers on one counter | `#[repr(align(64))]`; if a bulk benchmark still shows contention, shard the hottest (`ingest_samples_accepted_total`, `chunk_created_total`, `compaction_samples_written_total`) per worker and sum at snapshot. |
| Tests coupling to exact counts race with background tasks | `STATS RESET` right before the action; assert `≥` for anything a cron task or the post-load sweep can also touch. |
| Double-counting one client-visible fanout timeout | Distinct names for client vs RPC timeouts; docs say not to sum them. |
| `total_samples` / trimmed counts misread under lazy retention | Accepted counts come from add results, never from `total_samples` deltas; trims split lazy vs forced. |
| Counters silently lost in fork children | Design rule in §2; roster review rejects counters in `rdb_save`/`aux_save`/`aof_rewrite`. |
| `INFO ts_memory` already does an O(terms + series) walk | Out of this plan's scope, but `INFO` is scraped often; consider caching it or moving index sizes out of `INFO`. `INFO ts_stats` must not add another walk. |
| Docs drifting from the registry | Roster test in Phase 5. |
| `SHOW_INFO` / `LIST_CONFIGS [APP\|DEV\|HIDDEN]` removal surprises someone | Neither ever worked; changelog note. |

## Appendix A — found during the 2026-10-07 revision (not observability)

- **INSERT check after the create.** `create_and_store_series`
  ([utils.rs:215-217](../../src/series/utils.rs#L215-L217)) stores the key — and for explicit
  `TS.CREATE` replicates it and emits `ts.create` — before the INSERT check. A denial can leave the
  key behind. Not yet confirmed whether server-side key-pattern ACLs always pre-empt it.
- **Replication skipped after a stored write.** `TS.ADD`/`TS.INCRBY` error on a compaction failure
  after storing the sample, so nothing propagates; `TS.MADD`/`TS.ADDBULK` only log the same failure.
  The `?` at [ts_madd.rs:133](../../src/commands/ts_madd.rs#L133) aborts after samples are merged.
- **Every fanout op returns the generic error.** The blanket impl
  ([fanout_client_command.rs:89-116](../../src/fanout/fanout_client_command.rs#L89-L116)) overrides
  none of `get_timeout`/`fail_fast`/`generate_error_reply`/`on_error`, so `fail_fast` is always false
  and the fail-fast branch ([fanout_command.rs:312-320](../../src/fanout/fanout_command.rs#L312-L320))
  is dead.
- **Stale comments/docs.** [filter-dos-audit.md](../topics/filter-dos-audit.md) references the removed `postings.rs`;
  `TimeSeriesIndex::remove_stale_ids` and `series_posting_ids_by_selectors` are dead code.

## Revision log

**2026-10-07 (fanout wire)** — added the `fanout` section early with sent message and payload
byte counters per message type (§3.8 "Wire volume").

**2026-10-07 (metered)** — rebuilt the Phase 0 registry on `metered =0.10.0-rc.1`: one
`Registry` per section, std atomics as metric state, `RESET` as a baseline view, OpenMetrics
naming (counters registered without `_total`; durations in seconds, so every `_us`/`_ms` name in
this plan became `_seconds`; histograms lose `max`).

**2026-10-07 (Phase 0)** — implemented Phase 0; see the "as built" note there. §2 now describes
the static table and `DerivedCounter`; §3.9/§3.10 mark the shipped metrics; §4 pins the
histogram reply to finite buckets.

**2026-10-07** — re-checked every hook at 6d83b5d75 (99 commits after the first draft).

- Added §3.2 `retention` (lazy trim, 4099c701f) and §3.9 `exec` (bounded executors, threading
  refactor #124); executor counters already exist and moved into Phase 0.
- §2: added the fork-child rule, the bounded-snapshot rule, label expansion by name, and a later
  cluster view following `STRINGPOOLSTATS`/`INDEXMEMORY`. Kept snake_case despite camelCase in
  existing `TS._DEBUG` replies, because names are shared with `INFO`.
- §3.1: outcome counting moved from `add_deferring_retention` to the four command handlers (batches
  bypass it; compaction writes go through it); `Duplicate` split for `TS.ADDBULK` in-batch repeats;
  "added" no longer derived from `total_samples`.
- §3.4: added rule-pruning, cycle, cascade, dropped-write and error counters for key-based linking
  (641c1885f); split bucket rewrites from rescans.
- §3.5: `TS.INCRBY` rewrite is now constant; fixed the `TS.MADD` dropped-input formula.
- §3.6: per-query counting moved to callers; dropped the index-size gauges (O(n) walk); split
  label-wide unions from whole-keyspace bases; stale-id counting reworked for pass-based draining.
- §3.7: blocked-client gauge decrements in `free_privdata_callback`.
- §3.8: request/target counts and duration moved to `exec_command` so local-only fanouts count;
  14 error kinds mapped by name; client vs RPC timeouts separated; `clustermap_age_seconds` needs a
  stored timestamp; serving-side rejections and ignored-response reasons added.
- §3.10: `TaskRun` lives in `spawn_background_single`; `asm_drain` may run concurrently;
  `IN_SLOT_IMPORT` → importing-slot count; `IS_PERSISTING` gone.
- §3.11: save-side counters dropped (fork child); load-side and `TS._RESTORE` counters expanded.
- Work plan tests updated to match (no `BGSAVE` save/load equality; local-only and pushdown-off
  fanout cases; disconnect-while-blocked).
