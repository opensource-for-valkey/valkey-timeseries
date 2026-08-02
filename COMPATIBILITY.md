# Valkey TimeSeries Compatibility

## Goal

The Valkey TimeSeries module aims to enable applications currently built against the RedisTimeSeries module to run against Valkey TimeSeries with little or no application-side change. Concretely, a developer who has written an application using standard RedisTimeSeries client libraries and command syntax should be able to point that application at a Valkey deployment running the Valkey TimeSeries module and have it continue to function correctly.

Compatibility is framed from the perspective of the application developer. It is not a claim about internal equivalence between the two modules; it is a claim about the contract that applications depend on — the commands they send, the responses they parse, and the query semantics they rely on.

## Non-Goals

The following are explicitly **not** goals of Valkey TimeSeries compatibility with RedisTimeSeries. Differences in these areas are expected and are not considered bugs.

**Binary, on-disk, and replication-stream format compatibility.** The serialized representation of a time series — including RDB payloads, AOF records, any internal persistence format, and the byte-level representation used on the replication stream between primary and replica nodes — is an implementation detail of Valkey TimeSeries. It is not expected to match RedisTimeSeries' format, and no tooling is provided to read or write one format from the other. In particular, a Valkey TimeSeries primary cannot replicate state to a RedisTimeSeries replica, nor the reverse; cross-module replication topologies are not supported.

**Internal and source-level parity.** Valkey TimeSeries does not attempt to match RedisTimeSeries's internal APIs, module hooks, source structure, threading model, or internal data structures (chunk layout, index structures, and so on). Code that depends on RedisTimeSeries internals — rather than on its public command surface — is out of scope.

**Performance characteristics.** Latency, memory footprint, throughput, ingestion rate, and scaling behavior are driven by Valkey TimeSeries' own implementation choices and may differ from RedisTimeSeries in either direction. Applications should not assume performance parity, and tuning guidance developed against RedisTimeSeries may not transfer.

**Error message text parity.** The exact wording of error strings returned to clients is not guaranteed to match RedisTimeSeries. The _semantic_ error conditions — that is, which operations fail and under what circumstances — are expected to align where command and query compatibility applies, but the human-readable text of error responses may differ.

**Security Architecture.** Valkey TimeSeries's security architecture is somewhat more restrictive than RedisTimeSeries. Valkey TimeSeries enforces a user's ACL keyspace restrictions on label-based, multi-series query operations (`TS.MRANGE`, `TS.MREVRANGE`, `TS.MGET`, `TS.QUERYINDEX`, and the label-discovery commands), so a client only sees series whose keys it is permitted to access; RedisTimeSeries applies these restrictions less consistently.

## Expected Compatibility

This section describes the areas where Valkey TimeSeries intends to behave the same as RedisTimeSeries from an application's point of view. **Observable differences in these areas are considered bugs.** If an application written against RedisTimeSeries produces different results, fails where it previously succeeded, or succeeds where it previously failed in any of the following dimensions, that is a defect in Valkey TimeSeries and should be reported as such.

The contract applies only to features that Valkey TimeSeries actually implements. Valkey TimeSeries may not include every feature of RedisTimeSeries. An application that attempts to use a RedisTimeSeries feature Valkey TimeSeries does not support will receive an error — typically indicating that the command, option, or argument is not recognized or not implemented. That error is the intended behavior and is **not** a compatibility bug. The supported surface may also _exceed_ RedisTimeSeries: Valkey TimeSeries adds new commands and options, or accepts combinations of existing elements that RedisTimeSeries rejects (see [Extensions](#extensions) below). The authoritative source for which features are supported is the Valkey TimeSeries documentation and the release notes for the specific release of Valkey TimeSeries being deployed; those should be consulted to determine the supported surface.

### Command and argument syntax

The set of commands exposed by the module, their argument ordering, their flag and option names, and the shape of their replies are expected to match RedisTimeSeries. This includes series-management commands (e.g. `TS.CREATE`, `TS.ALTER`, `TS.DEL`), the compaction-rule commands (`TS.CREATERULE`, `TS.DELETERULE`), ingestion commands (`TS.ADD`, `TS.MADD`, `TS.INCRBY`, `TS.DECRBY`), and query commands (`TS.GET`, `TS.MGET`, `TS.RANGE`, `TS.REVRANGE`, `TS.MRANGE`, `TS.MREVRANGE`, `TS.NRANGE`, `TS.NREVRANGE`, `TS.INFO`, `TS.QUERYINDEX`). Applications using standard RedisTimeSeries client libraries should be able to issue these commands unmodified and receive replies they can parse with existing response handlers.

One gap in that surface is worth naming: the `twa` aggregator, which RedisTimeSeries lists for the whole range family, is not supported by this module at all. Every other aggregator RedisTimeSeries documents is, alongside this module's own [extensions](#extensions).

### Query language and semantics

The query surface accepted by the range and multi-series commands — including label matchers in `FILTER` (`label=value`, `label!=value`, `label=(v1,v2)`, `label!=(v1,v2)`, and presence/absence forms), the `AGGREGATION` clause and its bucketing options (`ALIGN`, `BUCKETTIMESTAMP`, `EMPTY`), `FILTER_BY_TS`, `FILTER_BY_VALUE`, `COUNT`, `WITHLABELS` / `SELECTED_LABELS`, the `GROUPBY` / `REDUCE` grouping applied by `TS.MRANGE`, and the `EXCLUDEEMPTY` flag it and `TS.MREVRANGE` accept — is expected to match RedisTimeSeries. A query that returns a given set of series and a given set of aggregated samples against RedisTimeSeries should return the same series and the same samples against Valkey TimeSeries for equivalently configured series.

Samples within a single series are always returned in timestamp order (ascending for `TS.RANGE`/`TS.MRANGE`, descending for `TS.REVRANGE`/`TS.MREVRANGE`), so that ordering is part of the contract. The order in which _distinct series_ appear in a multi-series reply, however, is not inherently defined: `TS.MRANGE`, `TS.MGET`, and `TS.QUERYINDEX` may list matching series in any order, and that order may differ between the two modules. Equivalence in those cases means the same _set_ of series, not the same sequence. Differences in the ordering of series in a multi-series reply are not considered compatibility bugs.

### Data model

The data model — series identified by a Valkey key, each holding a sequence of (timestamp, value) samples plus a set of label name/value pairs, governed by per-series properties such as retention period, chunk size, chunk encoding (`COMPRESSED` / `UNCOMPRESSED`), duplicate-sample policy, and rounding (`DECIMAL_DIGITS` / significant digits) — is expected to match RedisTimeSeries. Compaction rules created with `TS.CREATERULE` (the aggregation type, bucket duration, and alignment) and the label semantics used to select series in queries are expected to behave the same way. Series and rules configured for RedisTimeSeries should produce equivalent behavior when created against Valkey TimeSeries.

### A note on stricter input validation

In some cases RedisTimeSeries silently accepts extraneous or malformed syntax — for example, unrecognized trailing arguments or filter fragments that do not conform to the documented grammar — and returns a successful reply. Valkey TimeSeries may be stricter: in these cases it may reject the input with an error rather than accept it. This stricter validation is an intentional choice and is **not** considered a compatibility bug, even though it falls on a surface that is otherwise expected to be compatible. Applications that have been relying (knowingly or not) on RedisTimeSeries's tolerant parsing will need to clean up the offending call sites as part of migration.

### A note on label ordering within returned series

When a multi-series command returns labels (via `WITHLABELS` or `SELECTED_LABELS`), the order in which the label name/value pairs appear for a given series is not guaranteed to match between RedisTimeSeries and Valkey TimeSeries. The _set_ of labels returned for each series is expected to match; the sequence in which those labels are laid out inside the reply may differ. This is **not** considered a compatibility bug.

### A note on floating-point precision

Floating-point arithmetic is inherently imprecise, and the observable result of a floating-point computation depends on the order in which individual operations are performed. Valkey TimeSeries and RedisTimeSeries do not guarantee that floating-point operations — including the arithmetic involved in aggregations and downsampling, `FILTER_BY_VALUE` comparisons, `INCRBY` / `DECRBY` accumulation, and the conversions performed when parsing floating-point values from text or formatting them back into text — are executed in the same order. Small numerical differences between the two modules on otherwise-equivalent inputs should be expected, and are **not** considered compatibility bugs. Applications that compare floating-point results for exact equality, or that depend on the exact textual representation of a floating-point value round-tripped through the module, should be reworked to tolerate small numerical differences.

This tolerance covers differences attributable to _operation ordering_ — the last few digits of a result. It is deliberately narrow, and it should not be read as a general licence for the two modules to return arbitrarily different numbers. Outside the two aggregation surfaces described below, a numeric result that differs by more than rounding is a compatibility bug and should be reported as one. Where the two modules disagree by a wide margin because RedisTimeSeries computes a quantity with a numerically unstable algorithm, that is a *value* divergence rather than a precision difference, is bounded to a named surface, and is documented explicitly under [Variance and standard-deviation aggregations](#variance-and-standard-deviation-aggregations) and [Sum and average accumulation](#sum-and-average-accumulation).

## Intentional Incompatibilities

This section describes areas where Valkey TimeSeries intentionally diverges from RedisTimeSeries. Divergences here are by design and will not be treated as defects. Where possible, each item is described in terms of _what differs_, _why_, and _what migration impact to expect_.

### Configuration

**What differs.** The module's configuration surface — including configuration parameter names, how they are set, and how they are queried — does not match RedisTimeSeries's. RedisTimeSeries takes its configurables (for example `RETENTION_POLICY`, `CHUNK_SIZE_BYTES`, `DUPLICATE_POLICY`, `ENCODING`, `NUM_THREADS`, `COMPACTION_POLICY`) as `MODULE LOAD` / `MODULE LOADEX` arguments. Valkey TimeSeries exposes its configurables through Valkey's native configuration machinery (`CONFIG GET` / `CONFIG SET`) under `ts-`-prefixed names — for example `ts-retention-policy`, `ts-chunk-size`, `ts-duplicate-policy`, `ts-encoding`, `ts-num-threads`, and `ts-compaction-policy`.

**Why.** Valkey TimeSeries intentionally uses Valkey's native configuration machinery so that its configurables are managed the same way as the rest of the server's configurables, rather than through a module-specific, parallel surface. Several parameters that RedisTimeSeries can only set at load time become runtime-adjustable through `CONFIG SET` (those that must be fixed at startup, such as `ts-num-threads`, remain immutable). This is a design choice about where configuration lives, not a limitation of any underlying format.

**Migration impact.** Configuration files and any automation that sets module parameters via `MODULE LOAD` / `MODULE LOADEX` arguments will need to be rewritten against the Valkey TimeSeries configuration surface, using `CONFIG GET` / `CONFIG SET` or the equivalent mechanism exposed by the managed service provider, and the parameter names need to be updated to their `ts-` equivalents. Applications that only consume configuration indirectly — through the default behavior of the module — are not affected.

### INFO metrics (global `INFO` and per-series `TS.INFO`)

**What differs.** The metrics emitted by Valkey TimeSeries — both the global module-level metrics reported by `INFO` and the per-series metrics reported by `TS.INFO` — do not match RedisTimeSeries's metric set name-for-name.

**Why.** Because Valkey TimeSeries's implementation differs from RedisTimeSeries's, some RedisTimeSeries metrics simply have no meaningful equivalent in Valkey TimeSeries (and, conversely, Valkey TimeSeries exposes metrics — for example around its indexing and cluster-fanout machinery — with no RedisTimeSeries counterpart). Matching the RedisTimeSeries metric set verbatim would foreclose Valkey TimeSeries's ability to report meaningfully on its own behavior.

**Migration impact.** Monitoring dashboards, alerting rules, and any operational tooling that scrapes RedisTimeSeries metric names will need to be re-pointed at the Valkey TimeSeries equivalents or adapted to the Valkey TimeSeries metric set. Application code that parses `TS.INFO` should be reviewed as well (see the Migration Guide). Applications that do not consume these metrics programmatically are not affected.

### Cluster and sharding behavior

**What differs.** Not the observable result. Both modules resolve label-based multi-series commands — `TS.MRANGE`, `TS.MREVRANGE`, `TS.MGET`, `TS.QUERYINDEX` — across every shard of a cluster; Redis documents these as "cross-slot (all shards)" in each of its clustered configurations, and RedisTimeSeries has coordinated the fan-out itself since v1.6 (before which it required RedisGears loaded alongside the module). An application issues one command and receives a cluster-wide result on either module. What differs is the mechanism and the failure surface it exposes:

- Valkey TimeSeries fans out over Valkey's native cluster bus. There is no separate coordinator component, no additional network port, and no module-level shared password — RedisTimeSeries required `OSS_GLOBAL_PASSWORD` on every cluster node prior to its 8.0 release, which replaced it with an internal shared-secret mechanism.
- Valkey TimeSeries bounds each fan-out with the `ts-fanout-command-timeout` configurable and returns an error when a consistent cluster-wide view cannot be established within it. This is a reply an equivalent RedisTimeSeries deployment need not produce.
- ACL keyspace restrictions are enforced on the fanned-out result (see _Security Architecture_ under Non-Goals).

Requirements common to both modules are unchanged and are not part of this divergence: a compaction rule is a two-key operation whose source and destination must occupy the same hash slot (`TS.CREATERULE`, `TS.DELETERULE`, and any default compaction policy in a clustered deployment), and `TS.MADD` operates within a single slot. Hash tags remain the mechanism for both.

**Why.** Using the cluster bus keeps fan-out inside the machinery an operator already runs, secures, and monitors, rather than introducing a parallel transport with its own ports and credentials to distribute. The timeout is the consequence of that choice being made explicit: a scatter/gather across shards can fail partially, and reporting that as an error is preferred to returning a silently incomplete result set.

**Migration impact.** For deployments on a current RedisTimeSeries, there is generally no application-side fan-out to remove — the module was already doing it. What may exist is *workarounds*: per-shard query-and-merge logic written against a pre-1.6 deployment, or hash-tag co-location adopted solely to keep a multi-series query on one shard. Neither breaks, and both can be retired. Two things do need attention: any `OSS_GLOBAL_PASSWORD` provisioning is obsolete and should be removed from deployment automation, and callers of multi-series commands must handle the fan-out timeout error rather than treating it as an empty result. Deployments with cluster mode disabled are unaffected, regardless of whether they are a single node or a primary with replicas.

### Persistence and on-disk format

**What differs.** The persistence format used to store series state (across RDB, AOF, and any internal format) does not match RedisTimeSeries's.

**Why.** The on-disk format is an internal implementation detail and is not part of the compatibility contract. Fixing it would constrain Valkey TimeSeries's ability to evolve its storage, chunk-encoding, and recovery strategies independently of RedisTimeSeries.

**Migration impact.** RedisTimeSeries RDB files cannot be loaded directly by Valkey TimeSeries, and vice versa. RDB/DUMP migration is explicitly not on the roadmap: the formats are incompatible and no conversion tooling is planned. Migrations between the two must rebuild series from the underlying data, not by copying persisted data. Two working recipes:

- **Export / re-ingest:** for each series, read the samples with `TS.RANGE key - +` (and the metadata with `TS.INFO`), recreate the series on the target with `TS.CREATE` (labels, retention, duplicate policy, chunk settings) plus its `TS.CREATERULE` rules, then bulk-load the samples with `TS.MADD`.
- **Live dual-write:** during a cutover window, write new samples to both deployments while backfilling history with the export/re-ingest recipe, then switch reads.

The failure modes are deliberately defined and tested rather than left to chance (both modules register the same `TSDB-TYPE` type name, so a payload can *reach* the wrong module): `RESTORE` of a RedisTimeSeries `DUMP` payload into Valkey TimeSeries fails with a clean error and creates no key; starting Valkey TimeSeries on a RedisTimeSeries RDB file is refused with a clear log message; and the module's own encoding-version guard rejects any foreign `TSDB-TYPE` payload even when the server-level RDB version check would admit it (for example, an RDB produced by RedisTimeSeries on an older Redis). The reverse direction is outside this project's control; as observed against RedisTimeSeries 8.10, a Valkey TimeSeries payload is rejected there cleanly, with no key created — currently at the server's envelope check (`DUMP payload version or checksum are wrong`) rather than by the module, because the payload carries a newer RDB version than the reference accepts. The specific text is server wrapping and is not part of the compatibility contract; what is pinned is that the rejection is clean.

### Variance and standard-deviation aggregations

**What differs.** The four variance-family aggregators — `std.p`, `std.s`, `var.p`, `var.s` — can return substantially different values on the two modules for the same samples. RedisTimeSeries evaluates them with the textbook sum-of-squares identity, `E[x²] − E[x]²`; Valkey TimeSeries accumulates in a numerically stable form and returns the mathematically correct result. The identity fails in two distinct regimes, and in both the difference is categorical rather than a matter of trailing digits:

- **Overflow (DIV-0022).** When any sample magnitude exceeds `sqrt(DBL_MAX)` (≈ 1.34e154), squaring it overflows to `+Inf`, the `Inf − Inf` term yields `NaN`, and RedisTimeSeries returns `NaN` for the whole bucket. Valkey TimeSeries returns the true value. For example, `std.p` over the single sample `1.7976931348623157e308` is `NaN` on RedisTimeSeries and `0` on Valkey TimeSeries — a single sample has zero variance. The boundary is exact: `1e150` squares to a finite `1e300` and both modules agree; `1e200` overflows and they diverge.
- **Cancellation (DIV-0024 … DIV-0029).** Staying below the overflow boundary is *not* sufficient for agreement. Whenever a bucket's samples are close together relative to their magnitude, the two terms of the identity agree to within `f64` precision and the subtraction cancels, losing roughly `(mean/sd)² × DBL_EPSILON` of relative accuracy at *any* magnitude. Measured against RedisTimeSeries 8.10: samples `{100, 100.1}` disagree at ~1e-10 relative; `{100, 100.0001}` at 2.2e-4; `{12345.6789, 12345.679}` returns `NaN` on RedisTimeSeries — nine orders of magnitude below the overflow boundary. Where the cancellation is total, RedisTimeSeries returns exactly `0` for a sample set whose true deviation is not zero: two samples of `123456789.123456789` and one more than that give `0.5` on Valkey TimeSeries and `0` on RedisTimeSeries. This has been observed from magnitudes of ~1.2e8 upward.

The affected surface is bounded in three ways. Only the variance family computes a sum of squares — `avg`, `sum`, `min`, `max` and the rest do not, so a single sample at `DBL_MAX` agrees on both modules. (Read that as scoped to *this* failure mode. `sum` and `avg` have an unrelated divergence of their own, on the opposite input shape — a bucket that mixes magnitudes — described under [Sum and average accumulation](#sum-and-average-accumulation) below.) The divergence is one-directional: Valkey TimeSeries returns the correct value, so it can only ever replace a RedisTimeSeries `NaN` or a cancelled `0` with a usable number, never the reverse. And it reaches a reply by exactly two routes: an `AGGREGATION std.*|var.*` clause on the read path (`TS.RANGE`, `TS.REVRANGE`, `TS.MRANGE`, `TS.MREVRANGE`), and reading a destination series fed by a `std.*|var.*` compaction rule (which additionally exposes it through `TS.GET` and `TS.MGET`).

**Why.** Matching RedisTimeSeries here would mean deliberately implementing an unstable variance so that a knowingly wrong answer can be returned — degrading a correct result into `NaN`, into exactly `0`, or into a value wrong in its leading digits. Unlike the divergences that `ts-compatibility-mode` gates, there is no second *intended* behavior to select between: one of the two answers is simply incorrect. This is also why the divergence is **not** gated by `strict` (see [Divergences that qualify but are not yet gated](#divergences-that-qualify-but-are-not-yet-gated)) — `strict` reproduces RedisTimeSeries's resolution of genuinely ambiguous cases, not its arithmetic errors.

**Migration impact.** Applications that use `std.p` / `std.s` / `var.p` / `var.s`, either as an `AGGREGATION` argument or via a compaction rule, should expect Valkey TimeSeries to report a different deviation than RedisTimeSeries did for large-magnitude or tightly-clustered samples, and should not treat the RedisTimeSeries value as the baseline to reconcile against. Three specific dependencies need auditing. First, code that special-cases `NaN` from a variance aggregator — treating it as "no data", skipping the bucket, or alerting on it — will stop seeing `NaN` in these cases and needs to handle a real number instead. Second, thresholds, alerting rules, and anomaly detectors tuned against a deviation that RedisTimeSeries cancelled to `0` will begin to see the true non-zero deviation and may fire where they previously did not; these need to be re-tuned against Valkey TimeSeries output rather than carried over. Third, historical values already materialized into a compaction destination under RedisTimeSeries were computed with the unstable identity and are not recomputed by migration — a destination re-ingested from its source will hold corrected values, so a series spanning the cutover can show a step change at the boundary. Applications that do not use the variance family are unaffected.

### Sum and average accumulation

**What differs.** For `sum` and `avg` — as an `AGGREGATION` argument, as a `GROUPBY … REDUCE` reducer, or as the aggregation of a compaction rule — the two modules can return different values for a bucket whose samples differ widely in magnitude (DIV-0039). Valkey TimeSeries accumulates with compensated (Neumaier) summation, which carries the rounding error of each addition forward in a separate term. RedisTimeSeries adds naively, left to right: each small term is absorbed into the rounding of the running total, and once the large terms cancel against each other, what remains is that absorption error rather than the small terms themselves.

Unlike the variance family above, this needs no extreme magnitudes — only a mix of them within one bucket:

| Bucket contents | Valkey TimeSeries | RedisTimeSeries |
| --- | --- | --- |
| `1e150`, `1`, `-1e150` | `1` | `0` |
| `1e16`, `1`, `1`, `-1e16` | `2` | `0` |
| `1e6`, `0.001`, `-1e6` | `0.001` | `1.0000000474974513e-3` |

The third row is the ordinary case: a millisecond added to a counter near one million. Note also that the RedisTimeSeries result is not always `0` — total cancellation gives `0`, partial cancellation gives a non-zero value wrong in its leading digits.

Two properties bound the difference. It is confined to buckets that mix magnitudes: where the samples are within a few orders of magnitude of each other there is nothing to absorb, and the two modules agree exactly. And on inputs this ill-conditioned *neither* result is close to the exact sum — the difference is that Valkey TimeSeries's is reproducible while RedisTimeSeries's depends on the order in which the samples happen to be accumulated (four of the six orderings of the first row give `0`, two give `1`).

**Why.** Matching RedisTimeSeries would mean adopting an accumulation that is both lossier and order-dependent. There would be nothing stable to match: the value to reproduce changes with bucket iteration order and, under `GROUPBY`, with the order the reducer visits series — neither of which either module specifies. As with the variance family, this is not a choice between two intended behaviors, so it is **not** gated by `ts-compatibility-mode strict`; `strict` reproduces RedisTimeSeries's resolution of genuinely ambiguous cases, not its arithmetic error terms.

**Migration impact.** Applications summing or averaging series that mix magnitudes within a bucket — a large baseline with small increments, counters at epoch-nanosecond scale, or any series where large positive and negative values cancel — should expect Valkey TimeSeries to report the small residual that RedisTimeSeries absorbed, most visibly as a non-zero value where RedisTimeSeries returned exactly `0`. As with the variance family, values already materialized into a compaction destination under RedisTimeSeries are not recomputed, so a destination spanning the cutover can show a step change at the boundary. Series whose samples stay within a few orders of magnitude of each other are unaffected.

### Log messages

**What differs.** The content, format, severity, and frequency of log messages emitted by Valkey TimeSeries do not match those emitted by RedisTimeSeries. Individual log strings, the events that trigger them, and the structured fields (if any) they carry should all be considered Valkey-TimeSeries-specific.

**Why.** Log output reflects the module's internal implementation, which diverges from RedisTimeSeries's. Attempting to reproduce RedisTimeSeries's log surface verbatim would constrain Valkey TimeSeries's ability to report on events and conditions meaningful to its own implementation, and would produce misleading messages for events that do not correspond to anything in RedisTimeSeries.

**Migration impact.** Log-scraping rules, alerting triggers keyed on specific log strings, log-shipping parsers, and runbooks that instruct operators to look for particular RedisTimeSeries log phrases all need to be reviewed and updated against Valkey TimeSeries's log output. Applications that do not consume logs programmatically are not affected.

## Extensions

Valkey TimeSeries extends functionality beyond what RedisTimeSeries provides. These extensions take two forms.

Valkey TimeSeries may add surfaces that RedisTimeSeries does not have at all — for example a new option on an existing command, or an entirely new command. Beyond the core RedisTimeSeries surface, Valkey TimeSeries adds analytics and data-quality commands such as forecasting (`TS.FORECAST`, `TS.AUTOFORECAST`), anomaly and outlier detection (`TS.OUTLIERS`), decomposition and trend analysis (`TS.DECOMPOSE`, `TS.TREND`), correlation (`TS.AUTOCORRELATION`, `TS.XCORR`), stationarity and periodicity analysis (`TS.STATIONARITY`, `TS.PERIODS`), feature extraction (`TS.FEATURES`), gap filling (`TS.FILLGAPS`), joins (`TS.JOIN`), and richer label/metric discovery (`TS.LABELNAMES`, `TS.LABELVALUES`, `TS.METRICNAMES`, `TS.LABELSTATS`). Because RedisTimeSeries has no such surface, an application written against RedisTimeSeries cannot be exercising it, so its addition cannot change the behavior of any existing application.

**New combinations of existing elements.** Valkey TimeSeries may also support combinations of already-existing commands, options, or query constructs that RedisTimeSeries recognizes individually but rejects (or does not support) when used together. Where RedisTimeSeries would have returned an error for such a combination, no compatible application can depend on it succeeding, so accepting it is additive rather than a behavioral change. (If, instead, RedisTimeSeries accepts the combination and produces a _different_ result than Valkey TimeSeries, that is not an extension — it is tracked as an intentional incompatibility or, on a compatible surface, a compatibility bug.)

In both forms, an extension is not considered an incompatibility: applications written against RedisTimeSeries do not exercise these surfaces and are unaffected. Extensions are, however, Valkey-TimeSeries-specific: applications that adopt them lose portability back to RedisTimeSeries.

Extensions are documented alongside the features they extend rather than centralized here. When an extension modifies the behavior of an existing RedisTimeSeries surface in a non-additive way, that change is tracked as an intentional incompatibility in the section above, not as an extension.

## Strict compatibility mode

A small number of divergences share a specific and uniquely dangerous shape: **both engines accept the command as valid, and both return a result, but the results differ.** An application migrating from RedisTimeSeries gets no error on such a command — it gets a different answer, silently. The `ts-compatibility-mode` configurable lets a deployment close that class.

| Value | Behavior |
| ----- | -------- |
| `extended` (default) | Valkey TimeSeries semantics. |
| `strict` | On the divergences listed below, resolve the command the way RedisTimeSeries 8.10 resolves it. |

```
CONFIG SET ts-compatibility-mode strict
CONFIG GET ts-compatibility-mode
```

It is settable at runtime and at startup (`valkey.conf` or `MODULE LOAD` arguments), and is fully reversible. It takes effect on subsequent commands only — it does not rewrite series, rules, or any stored state, so switching modes is safe in both directions.

### Scope: value divergences only

`ts-compatibility-mode` governs **only** cases where the two engines disagree about the *value* returned for a command both consider valid. In particular, it deliberately does **not** restrict Valkey TimeSeries's additive surface.

Where RedisTimeSeries rejects an input that Valkey TimeSeries accepts, no RedisTimeSeries-compatible application can be depending on that input — it would have been an error there. Such an accepted-input superset cannot silently change the behavior of a migrating application, so there is nothing for `strict` to protect against, and disabling it would only remove function. The relative range bounds (`-1h`, `*`), cascading compaction rules, complement filters, and Prometheus-style bare metric-name selectors all fall in this category and **remain available in both modes**, as do the Valkey-TimeSeries-only commands.

`EXCLUDEEMPTY` on `TS.MRANGE` / `TS.MREVRANGE` adds one more, for the same reason and with the same root cause as the bare metric-name selector. RedisTimeSeries's `FILTER` expression list does not stop at the token, so it rejects the trailing form its own documented syntax shows (`... FILTER filterExpr... [GROUPBY ...] [EXCLUDEEMPTY]`) with "failed parsing labels"; Valkey TimeSeries stops the list there, as it does for every other option token, and honors the flag. Because a bare token is a metric-name selector in this dialect, not stopping would read the documented form as `__name__="EXCLUDEEMPTY"` and answer with an empty array — a silent wrong answer where the trailing form is at least a visible error on RedisTimeSeries. Every other position, and the flag's effect, match RedisTimeSeries exactly, including its rejection of `EXCLUDEEMPTY` together with `GROUPBY ... REDUCE`.

The same reasoning excludes the reverse direction. Where Valkey TimeSeries is *stricter* than RedisTimeSeries — rejecting unknown trailing arguments, for example — a migrating application gets a visible, fixable error rather than wrong data. That is already the safe outcome, so `strict` does not relax it into RedisTimeSeries's silent-ignore behavior.

### What `strict` gates

| Divergence | `extended` (default) | `strict` |
| ---------- | -------------------- | -------- |
| A repeated option on the range family — `TS.RANGE`, `TS.REVRANGE`, `TS.MRANGE`, `TS.MREVRANGE` (e.g. `COUNT 5 COUNT 2`, or a duplicated `AGGREGATION` / `FILTER` / `FILTER_BY_TS` / `FILTER_BY_VALUE` / `GROUPBY` / `SELECTED_LABELS`) | The **last** occurrence wins. | The **first** occurrence wins, as in RedisTimeSeries. |
| `TS.GET` / `TS.MGET` (without `LATEST`) on a compaction destination whose *older* bucket was back-filled (DIV-0023) | The last sample the destination actually holds. | RedisTimeSeries's cached destination last-sample, which back-filling does not refresh. |

Both engines accept a duplicated option and neither errors, so the only difference is which silent resolution applies — the defining shape of this class. A query builder that appends an option twice is the realistic way to reach it. The duplicate's operands are still parsed in both modes, so a malformed operand remains an error either way.

The back-fill case has the same shape: both engines store identical downstream data — `TS.RANGE` and `TS.GET ... LATEST` agree in either mode — and differ only in which stored sample `TS.GET` names, with no error to signal it. RedisTimeSeries reports a cached last-sample that a back-filled older bucket does not advance, so its own `TS.GET` and `TS.RANGE` disagree; `strict` reproduces that, and `extended` reports the sample the series actually holds.

One limitation is specific to this gate: the marker it reads is runtime-only, so after a reload or restart a destination with a pending back-fill reports its true last sample again until the next forward bucket close. Matching RedisTimeSeries across a reload would mean persisting a value that contradicts the series' own chunk data, which is deliberately out of scope.

### Divergences that qualify but are not yet gated

These meet the criterion — both engines accept, values differ — but are not currently switchable, because reproducing RedisTimeSeries's result is not a local decision. Each remains a permanent, documented divergence; see `tests/compat/divergences.yml` for the full rationale.

| Divergence | Why it is not gated |
| ---------- | ------------------- |
| `TS.REVRANGE ... AGGREGATION first\|last ... EMPTY` gap fill (DIV-0016) | Matching requires either buffering every sample in range or a second pass over the reversed output — the memory trade the forward-aggregation design exists to avoid. |
| `TS.DEL` count over an already-expired range (DIV-0021) | Matching requires adopting RedisTimeSeries's lazy-trim retention model, which the eager trim deliberately replaced so that `TS.INFO` agrees with `TS.RANGE`. |
| `std.p` / `std.s` / `var.p` / `var.s` — overflow above ~1.34e154 (DIV-0022), and cancellation on large-magnitude, small-spread samples at any magnitude (DIV-0024…DIV-0029) | RedisTimeSeries computes variance with the unstable `E[x²] − E[x]²` identity, which yields `NaN` on overflow and exactly `0` (or a value wrong in its leading digits) when the two terms cancel. Matching means adopting a numerically unstable accumulation to reproduce an arithmetic error — there is no second *intended* behavior to select between. The divergence can only turn a `NaN` or a cancelled `0` into a usable number, never the reverse. See [Variance and standard-deviation aggregations](#variance-and-standard-deviation-aggregations). |
| `sum` / `avg` over a bucket that mixes magnitudes (DIV-0039) | RedisTimeSeries accumulates naively, absorbing small terms into the rounding of large ones; Valkey TimeSeries uses compensated summation. There is no stable value to match — the RedisTimeSeries result depends on an accumulation order neither module specifies. See [Sum and average accumulation](#sum-and-average-accumulation). |

### What `strict` never affects

The intentional incompatibilities listed above — configuration surface, `INFO` / `TS.INFO` metrics, cluster fan-out, persistence format, and log messages — are unchanged in either mode, as is error message *text*, which is not part of the compatibility contract. RESP2 floating-point *formatting* (DIV-0002…DIV-0007) is likewise out of scope: the parsed values are bit-identical, so it is a formatting difference rather than a value divergence.

`ts-compatibility-mode` is orthogonal to `ts-emulate-release` below: this setting selects between two behaviors that are *both intended*, while `ts-emulate-release` preserves a behavior that was *wrong* so that fixing it does not break SemVer.

## Migration Guide

This section describes the steps an application team should work through when migrating an existing RedisTimeSeries-based application to Valkey TimeSeries. The steps are ordered so that earlier steps surface blocking issues before later steps require code changes.

### 1. Verify feature coverage against the documentation

Before making any code changes, review the Valkey TimeSeries documentation and confirm that every RedisTimeSeries feature the application depends on is currently supported. Walk through the commands the application issues, the series options and compaction rules it uses, the filter and aggregation syntax it constructs, and any module-specific behavior it relies on. Anything the application uses that is not listed as supported should be resolved before proceeding to later steps. This check is cheapest to do first because it determines whether migration is viable at all.

### 2. Move configuration to Valkey's native configuration surface

RedisTimeSeries configuration is supplied as `MODULE LOAD` / `MODULE LOADEX` arguments. Valkey TimeSeries manages the same configurables through Valkey's native configuration machinery: `CONFIG GET` and `CONFIG SET` for self-hosted deployments, or whatever equivalent mechanism is exposed by a managed service provider (for example a cloud console, a provider-specific API, or parameter groups). Move any load-time RedisTimeSeries arguments to the corresponding `ts-`-prefixed Valkey configuration parameters (for example `RETENTION_POLICY` → `ts-retention-policy`, `CHUNK_SIZE_BYTES` → `ts-chunk-size`, `DUPLICATE_POLICY` → `ts-duplicate-policy`), and update any tooling that read or wrote configuration through the RedisTimeSeries surface.

### 3. Update `TS.INFO` response parsing

The response format of `TS.INFO` in Valkey TimeSeries differs from RedisTimeSeries's format. Application code that parses `TS.INFO` output — whether to drive operational logic, emit metrics, or display information in a UI — needs to be updated to parse Valkey TimeSeries's `TS.INFO` response shape. Plan to retest any code path that consumes `TS.INFO`, since silent parse mismatches can produce misleading values rather than outright errors.

### 4. Remove unsupported `INFO` and `TS.INFO` fields

Some fields that RedisTimeSeries exposes through `INFO` (module-level, global) and `TS.INFO` (per-series) have no equivalent in Valkey TimeSeries, either because the underlying metric does not apply to Valkey TimeSeries's implementation or because the format has been restructured. Dashboards, alerts, scripts, and application code that reference such fields need to have those references removed or remapped to the closest Valkey TimeSeries equivalent.

### 5. Retire cluster workarounds and handle the fan-out timeout (cluster mode only)

This step applies only to deployments running in cluster mode. Both modules perform cluster-wide fan-out for label-based multi-series commands internally, so in most cases there is no application-side fan-out to remove — see _Cluster and sharding behavior_ above. What to audit instead: per-shard query-and-merge logic written against a pre-1.6 RedisTimeSeries (which needed RedisGears loaded alongside the module to make `TS.MGET`, `TS.MRANGE`, and `TS.QUERYINDEX` cluster-aware), and hash-tag co-location adopted solely to confine a multi-series query to one shard. Both are safe to keep and no longer necessary; a single command returns the cluster-wide result on either module.

Two changes do require action. Remove any `OSS_GLOBAL_PASSWORD` provisioning from deployment automation: Valkey TimeSeries has no module-level shared password, uses no separate coordinator component and no additional network ports, so there is no new firewall or security-group configuration to perform. And make sure callers handle the timeout error a cross-shard operation may return when a consistent cluster-wide view cannot be established within `ts-fanout-command-timeout`, rather than treating it as an empty result.

Hash tags are still required where they always were, on both modules: a compaction rule's source and destination must share a hash slot, as must the keys of a single `TS.MADD`. Deployments with cluster mode disabled do not require this step, regardless of whether they consist of a single node or a primary with one or more replicas.

### 6. Remove dependencies on error message contents and on log messages

Two related classes of dependency need to be audited out of the application and its surrounding tooling.

The first is any code path that inspects the textual content of error replies — for example, matching on specific substrings returned by a failed command, branching on the wording of a validation error, or using regular expressions against error strings to decide how to react. Error message text is not part of the compatibility contract (see _Non-Goals_ above); the semantic error condition is expected to align, but the wording may not. Rework these paths to branch on the type of operation and on structured signals (command, return code, context) rather than on error string contents.

The second is any dependency on log messages — not only on specific log text, but on the _existence_ of particular log entries at all. Code or tooling that waits for a specific log line to appear, counts occurrences of a phrase, or treats the absence of a message as a signal is relying on a surface that Valkey TimeSeries does not preserve from RedisTimeSeries. A given RedisTimeSeries log event may be absent from Valkey TimeSeries entirely, emitted at a different severity, or emitted under different conditions. Remove these dependencies and replace them, where possible, with signals drawn from commands, metrics, or application-level observability.

### 7. Audit assumptions about the ordering of series in multi-series replies

Samples within a series are always returned in timestamp order, but the order in which _distinct series_ appear in a `TS.MRANGE`, `TS.MREVRANGE`, `TS.MGET`, or `TS.QUERYINDEX` reply is undefined and is not guaranteed to match between RedisTimeSeries and Valkey TimeSeries (see _Query language and semantics_ above). This matters most when an application treats the position of a series in the reply as meaningful — for example taking "the first series returned," iterating by index, or zipping the reply against a separately ordered list. Audit the application's multi-series call sites and change any that depend on series order to key off the series' Valkey key or its labels instead of its position in the reply.

### 8. Review custom result-parsing code for label ordering

When a multi-series command returns labels (via `WITHLABELS` or `SELECTED_LABELS`), the order in which a series' label name/value pairs appear inside the reply is not guaranteed to match between RedisTimeSeries and Valkey TimeSeries (see _Query language and semantics_ above). Standard RedisTimeSeries client libraries parse these replies into name-keyed maps and are not affected. Custom or hand-rolled result-parsing code, however, may be reading labels by position — for example, "the value at index 0 is the `__name__` label" — and will break when the labels come back in a different sequence. Review any such code and change it to look up labels by name rather than by position.

### 9. Review ACL keyspace restrictions

If ACL keyspace restrictions are used, these should be reviewed, as Valkey TimeSeries is more restrictive in some cases: label-based multi-series queries only return series whose keys the calling user is permitted to access. A query that appeared to return a complete result set under RedisTimeSeries may return a filtered subset under Valkey TimeSeries when the user's key permissions are narrower than the set of matching series.

### 10. Re-baseline anything that consumes `std.p` / `std.s` / `var.p` / `var.s`

This step applies only to applications that use a variance-family aggregator, either as an `AGGREGATION` argument on a read or as the aggregation of a `TS.CREATERULE` compaction rule. Valkey TimeSeries returns the mathematically correct deviation where RedisTimeSeries's sum-of-squares identity returned `NaN`, exactly `0`, or a value wrong in its leading digits (see _Variance and standard-deviation aggregations_ above). Identify the call sites and rules that use these four aggregators, then: remove any handling that treats a `NaN` from them as a sentinel for "no data"; re-tune thresholds, alerts, and anomaly detectors that were calibrated against RedisTimeSeries output, since a deviation previously reported as `0` may now be non-zero; and expect a step change at the migration boundary in any compaction destination whose history was materialized under RedisTimeSeries, because those stored bucket values are not recomputed. Note that `ts-compatibility-mode strict` does not restore the RedisTimeSeries values here — this divergence is not gated. Applications that do not use the variance family can skip this step.

### 11. Re-baseline `sum` / `avg` over series that mix magnitudes

This step applies only to applications that sum or average buckets whose samples span widely different magnitudes — a large baseline with small increments, values at epoch-nanosecond scale, or large positive and negative values that cancel. Valkey TimeSeries returns the small residual that RedisTimeSeries's naive accumulation absorbed, most visibly as a non-zero value where RedisTimeSeries returned exactly `0` (see _Sum and average accumulation_ above). Identify the read paths and compaction rules that aggregate such series, then re-tune any threshold or alert calibrated against a RedisTimeSeries value, and expect a step change at the migration boundary in a compaction destination whose history was materialized under RedisTimeSeries. As with the variance family, `ts-compatibility-mode strict` does not restore the RedisTimeSeries values. Applications whose sampled values stay within a few orders of magnitude of each other can skip this step.

## Compatibility Defects

Valkey TimeSeries follows the rules of [SemVer](https://semver.org), which governs the range of permitted changes in behavior from release to release. These rules would normally prohibit the ability to correct compatibility defects (bugs) in a minor or patch release. An exception to the SemVer rules is made for defects which are judged to be unusable — in other words, if the defective behavior renders the feature unusable, then the rules of SemVer do not apply, as there isn't any valid user base to protect.

Valkey TimeSeries provides an opt-in mechanism to enable the correction of compatibility bugs in minor and/or patch releases without violating the SemVer rules. A fix for a compatibility bug released in a minor or patch release selectively provides both the old (incompatible) behavior as well as the new (compatible) behavior. The selection is controlled by the configurable `ts-emulate-release`, which is set to a specific release identifier and governs the behavior. For example, if a compatibility bug is fixed in release 1.2.2, then setting `ts-emulate-release` to `1.2.1` or smaller would enable the old behavior, but setting it to `1.2.2` or larger would enable the compatible behavior. The default value for `ts-emulate-release` is the current major release, which honors SemVer rules if there is no opt-in across subsequent minor and/or patch releases.

It may be judged that a compatibility defect cannot reasonably be fixed while preserving the old behavior. In this case, the fix cannot be made until the next major release and will ignore the `ts-emulate-release` mechanism. In other words, fixes made under this clause cannot be undone using the `ts-emulate-release` override.

### Sunsetting of Incompatible Behavior

When feasible, the old (non-compatible) behavior will be preserved for _at least_ one additional major release. If a bug was fixed in 1.x.x, then 2.y.y will support emulating the 1.x.x release. However, support in the 3.x or later releases is not ensured. Similar to the above clause, if retaining the parallel behavior becomes unreasonable to support, then it can be removed in the next major version (see the release notes for that release).

To ease application migration, incompatible behavior controlled by `ts-emulate-release` is tracked by `INFO` fields. These fields count the number of uses of incompatible behavior by an application.

### Known Compatibility Defect Corrections

A list of the compatibility issues that have been fixed. (No corrections have been recorded yet; the row below illustrates the format.)

| Release | INFO field | Old Behavior | New Behavior |
| ------- | ---------- | ------------ | ------------ |
| _example_ | `compatibility-<slug>` | The prior, RedisTimeSeries-incompatible behavior, retained when `ts-emulate-release` is set below this release. | The corrected behavior matching RedisTimeSeries, active when `ts-emulate-release` is set to this release or later. |
