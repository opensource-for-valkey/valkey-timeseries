# Denial of Service via Low-Selectivity Filters

**Status:** audit findings; 2 of 9 findings fixed (status tags in [§3](#3-findings))
**Date:** 2026-07-09 (audit) · **updated 2026-07-10**
**Scope:** every command that accepts a `FILTER` / series-selector argument
**Branch:** `filter-dos`, 2 commits ahead of `main`:
`Refactor regex compilation to bound input and DFA size` (fixes [F4](#f4-regex-size-limit-is-silently-bypassed-on-the-fallback-path)),
`Add validation for series selectors with positive matchers` (fixes [F1](#f1-a-single-negative-matcher-selects-the-entire-keyspace))

---

## Summary

The module currently has **no cap on the number of series a filter may match**, and no cap on
the number of samples a command may materialize. A single unauthenticated-but-authorized client
can issue `TS.MRANGE - + FILTER host!=__nonexistent__` and force the server to open every
time-series key in the keyspace, decode every chunk, and buffer every sample in RAM before the
first byte of the reply is written. Because filter evaluation runs on the Valkey main thread with
no deadline and no cooperative yielding, the server is unavailable for the duration.

The question posed — *does adding max-series and max-sample limits sufficiently mitigate this?* —
has a short answer and a long one.

**Short answer: no, and neither limit exists today.** They are necessary and they are the highest
value-per-line changes available, but they bound the *result set*, and the most damaging queries
here are the ones that do enormous *work* to produce a *small* result. A regex matcher scanning
five million distinct label values to return zero series costs five million regex executions;
`max_series = 100` does not make that query cheaper. Selectivity attacks specifically maximize
work-per-result, which is the one quantity a result-set cap does not measure.

The long answer is [§4](#4-are-max-series--max-samples-sufficient).

One caveat up front: most findings in this memo establish **mechanism** from code paths; only a
subset are runtime-validated on a live server. Treat severities as risk-ranked hypotheses unless a
finding is explicitly marked reproduced/benchmarked in [§8](#8-reproductions-and-evidence-matrix).

Separately, the audit turned up one straightforward bug that is not about limits at all: the
16 KB regex size limit is silently discarded on a fallback path
([F4](#f4-regex-size-limit-is-silently-bypassed-on-the-fallback-path)). That is a P0 fix
independent of everything else in this document.

**Update, 2026-07-10:** both P0 items have since landed on this branch —
[F1](#f1-a-single-negative-matcher-selects-the-entire-keyspace) and F4 above are now fixed
([§7](#7-suggested-ordering) tracks what's left). Neither closes the broader question this document
asks: F2, F3, and F5–F9 are unchanged, and the sufficiency argument in
[§4](#4-are-max-series--max-samples-sufficient) still holds — a positive-matcher requirement and a
correct regex size limit remove two specific exploits, not the class of low-selectivity cost
attacks.

---

## 1. Attack surface

Commands accepting a series selector, and what they do with the matched set:

| Command | Filter required | Work per matched series | Result cap today |
|---|---|---|---|
| `TS.MRANGE` / `TS.MREVRANGE` | yes | open key, decode chunks, buffer all samples | `COUNT` (per-series, see [F2](#f2-count-is-per-series-so-mrange-memory-is-nseries--count)) |
| `TS.MGET` | yes | open key, read last sample | none |
| `TS.QUERYINDEX` | yes | index lookup only | none |
| `TS.CARD` | no (needs filter *or* date range) | index lookup, or full materialization if date range given | n/a |
| `TS.MDEL` | yes | open key writable, delete or range-delete | none |
| `TS.LABELNAMES` / `TS.LABELVALUES` | no | open key, read labels | `LIMIT`, but unbounded when omitted |
| `TS.METRICNAMES` | no | open key, read labels | `LIMIT`, defaults to 100 |
| `TS.LABELSTATS` | no | index lookup | `LIMIT` |
| `TS.LABELSEARCH` | no | index lookup | `LIMIT`, hard max 1000 |

`TS.LABELSEARCH` and `TS.METRICNAMES` are the only commands with a bounded default. Everything
else is unbounded, and the two most expensive commands — `TS.MRANGE` and `TS.MDEL` — are among
the unbounded ones.

**Update:** the "Result cap today" column still holds as written — no row gained a cap — but every
command listed here now rejects a selector with no positive matcher before any of the above work
happens, per [F1](#f1-a-single-negative-matcher-selects-the-entire-keyspace). That removes the
simplest route to "match everything" without adding a cap to any row in this table.

## 2. The query pipeline and where cost accumulates

A filtered command flows through four stages. Each has a distinct cost profile, and — importantly —
a limit placed at one stage does nothing for the stages before it.

**Stage 1 — parse and compile.** [`labels/regex.rs`](../../src/labels/regex.rs),
[`labels/regex_utils.rs`](../../src/labels/regex_utils.rs). Attacker-controlled regex compilation.
Cost is bounded by a size limit that [does not actually hold](#f4-regex-size-limit-is-silently-bypassed-on-the-fallback-path).

**Stage 2 — posting-list planning.** [`series/index/postings.rs`](../../src/series/index/postings.rs).
Produces a `Bitmap64` of series IDs. For equality matchers this is a hash lookup. For regex,
`contains`, and their negations it is a **full scan of every distinct value of the label**, running
the predicate once per value ([F3](#f3-regex-and-contains-matchers-scan-every-distinct-label-value)).
This is the stage where work and result size decouple.

**Stage 3 — materialization.** [`series/index/querier.rs:146`](../../src/series/index/querier.rs#L146),
`get_multi_series_by_id`. Loops the bitmap, opens each key, runs a per-key ACL check. O(matched),
main thread, GIL held ([F5](#f5-per-key-open--acl-check-is-on-thread-and-happens-before-any-cap-could-apply)).

**Stage 4 — execution and reply.** [`series/mrange.rs:305`](../../src/series/mrange.rs#L305).
Decodes and buffers all samples for all series into `Vec<MRangeSeriesResult>` before replying
([F2](#f2-count-is-per-series-so-mrange-memory-is-nseries--count)).

Any cap you add at stage 3 or 4 leaves stages 1 and 2 fully exposed. This is the crux of the
sufficiency question.

---

## 3. Findings

### F1. A single negative matcher selects the entire keyspace
**`src/series/index/postings.rs:517-522`** · severity: high · trivial to fix · **status: FIXED** (2026-07-10, `Add validation for series selectors with positive matchers`)

```rust
if has_subtracting_matchers && !has_intersecting_matchers {
    // If there's nothing to subtract from, add in everything and remove the not_its later.
    its.push(Cow::Borrowed(&self.all_postings));
};
```

Nothing requires a selector to contain at least one *positive* matcher. `FILTER host!=x` has only
a subtracting matcher, so the planner seeds the accumulator with `all_postings` and subtracts a
near-empty set. The result is every series in the database.

Prometheus — from which this planner is derived — rejects selectors in which no matcher matches a
non-empty value, precisely to prevent this. That check was not carried over. `TS.MRANGE - + FILTER
host!=__nonexistent__` is the canonical exploit, and `TS.MDEL ... FILTER host!=__nonexistent__` is
its destructive twin.

Note that `{l=""}` and `{l=~".*"}` are equally full-keyspace selectors under the same rule, since
both match empty.

**Fix as shipped:** [`src/parser/series_selector.rs:32-70`](../../src/parser/series_selector.rs#L32),
`validate_selector` / `validate_filter_group`, called from `parse_series_selector` itself — every
command that reaches a `FILTER` argument goes through this (`command_parser.rs`, `ts_mdel.rs`,
`ts_mget.rs`, `label_search_utils.rs`). This lands earlier than the location proposed in
[§5.1](#51-require-at-least-one-positive-matcher-fixes-f1): it rejects at stage 1
(parse), before stage 2 (`postings_for_label_filters`) ever runs, and it covers two cases a
postings.rs-level fix alone would not: each branch of an `OR` selector is validated independently
(`{a="b" or c!="d"}` is rejected, since the second branch alone is a full scan), and it checks
`matches_empty()` rather than just `is_negative_matcher()`, so it also catches `{l=""}` and
`{l=~".*"}` — the two cases called out just above as equally dangerous but, at audit time, equally
unaddressed.

The `postings.rs:517-522` snippet above is untouched, and read in isolation it still describes a
bug. It is now dead code on every current call path — `postings_for_label_filters` only ever
receives filters extracted from an already-validated `SeriesSelector` — but it has no guard of its
own. A future caller that builds a `SeriesSelector` without going through `parse_series_selector`
would reopen this exact hole. Worth a defensive check at the postings.rs level too, if that's a
path anyone expects to add (e.g. a programmatic/embedding API).

### F2. `COUNT` is per-series, so MRANGE memory is `N_series × COUNT`
**`src/iterators/utils.rs:161`**, **`src/series/mrange.rs:305-366`** · severity: high

`COUNT` is threaded into the per-series sample iterator, not into a query-wide budget. In
`handle_non_grouped`, each matched series independently collects up to `COUNT` samples into an
`UncompressedChunk`, and all chunks are held simultaneously in the returned `Vec` before any reply
is written.

A `Sample` is 16 bytes (`i64` timestamp + `f64` value). 100k series × 10k samples = 16 GB resident,
allocated before the client sees anything. Operators reasonably read `COUNT 10000` as "this query
returns at most 10000 samples." It does not.

`handle_grouping` is worse: [`mrange.rs:383-384`](../../src/series/mrange.rs#L383) explicitly
*clears* `options.range.count` before the per-series iterators are built, so that `COUNT` can be
applied once to the post-reduction stream. Correct semantics, but it means the pre-reduction
per-series read is entirely unbounded.

The `orx_parallel` fan-out (`.into_par()` at [`mrange.rs:318`](../../src/series/mrange.rs#L318),
`.iter_into_par()` at [`mrange.rs:388`](../../src/series/mrange.rs#L388)) parallelizes the decode
across worker threads. It does not reduce the peak memory, and the main thread still blocks for the
whole command.

**Since the audit:** `main` merged multi-aggregation support (`Multi-aggregation and aggregation
pushdown support (#68)`), which shifted every line number above and added a second instance of this
exact pattern. When the aggregation is multi-valued, `handle_non_grouped` collects rows through
`create_row_iterator` ([`iterators/utils.rs:142-161`](../../src/iterators/utils.rs#L142)) into
`SeriesResultData::Rows(iter.collect())` ([`mrange.rs:335`](../../src/series/mrange.rs#L335)) — the
same per-series, `COUNT`-bounded-not-budget-bounded, collect-into-`Vec`-before-reply shape as the
sample path, just for rows instead of samples. The finding is unchanged in substance; it now has
one more code path to fix alongside it.

### F3. Regex and `contains` matchers scan every distinct label value
**`src/series/index/postings.rs:264-286`** · severity: high · the core selectivity problem

```rust
pub fn postings_for_label_matching<F, STATE>(&self, name: &str, state: &mut STATE, match_fn: F)
    -> PostingsBitmap
{
    let prefix = KeyBuffer::for_prefix(name);
    for (key, map) in self.label_index.prefix(prefix.as_bytes()) {
        let value = key.sub_string(start_pos);
        if match_fn(value, state) { result |= map; }
    }
    ...
}
```

Every value of the label is visited and the predicate is evaluated against it. Cost is
`O(distinct_values(label))` regex executions, entirely independent of how many series match.

`handle_regex_equal_match` mitigates this *when the pattern has an extractable literal prefix*, by
narrowing the ART prefix scan. Three paths have no such mitigation and always perform the full scan:

- `handle_regex_not_equal_match` ([`postings.rs:1004`](../../src/series/index/postings.rs#L1004)) — negation has no usable prefix
- `handle_contains` / `handle_not_contains` ([`postings.rs:1055`](../../src/series/index/postings.rs#L1055)) — substring search cannot be prefix-narrowed
- `handle_regex_equal_match` when `re.prefix` is `None`, e.g. `=~".*foo"`

So `FILTER id=~".*zzz"` against a `id` label with 5M distinct values costs 5M anchored regex
executions and returns zero rows. **A result-set cap cannot see this query coming.** The cost
model at [`filters.rs:356-368`](../../src/labels/filters.rs#L356) already knows these are the
expensive shapes (it scores them 30 and 50) — but it only uses the score to *order* matchers, never
to *reject* them.

### F4. Regex size limit is silently bypassed on the fallback path
**`src/labels/regex.rs:93-98`** and **`src/labels/regex_utils.rs:498-505`** · severity: high · **independent bug, fix regardless** · **status: FIXED** (2026-07-10, `Refactor regex compilation to bound input and DFA size`)

```rust
RegexBuilder::new(&re)
    .size_limit(REGEX_SIZE_LIMIT)          // 16 KB
    .dot_matches_new_line(true)
    .build()
    .or_else(|_| Regex::new(&try_escape_for_repeat_re(&re)))   // <-- no size_limit
    .map_err(|_| ParseError::InvalidRegex(original_re.to_string()))
```

The `or_else` exists to retry with Go-style `{...}` literals escaped. But `RegexBuilder::build()`
returns `Err(CompiledTooBig)` when the pattern exceeds `size_limit`, and that error takes the same
`or_else` branch. `Regex::new` applies the crate's *default* size limit — 10 MiB in `regex` 1.x —
so the 16 KB cap is defeated by any pattern that trips it.

`try_escape_for_repeat_re` leaves valid repeats such as `a{1000}` untouched, so the fallback
recompiles a byte-identical pattern with a 625× larger budget. A pattern like
`FILTER l=~"((a{100}){100}){100}"` compiles a ~10 MiB DFA. There is no cap on the number of
matchers per selector or selectors per command, so this multiplies.

This is a compile-time cost, incurred in stage 1 — *before* any result-set limit could possibly be
consulted. It is also the cheapest finding to fix: apply `.size_limit()` to both builders.

The `regex` crate's linear-time guarantee means there is no catastrophic-backtracking ReDoS here.
The exposure is compile time and compile memory, not match time.

**Fix as shipped:** [`src/labels/regex.rs:91-104`](../../src/labels/regex.rs#L91) now exposes a
single `build()` that both the primary and fallback attempts call, with
`.dfa_size_limit(DFA_SIZE_LIMIT)` added alongside `.size_limit()`. `build_with_repeat_fallback`
retries through that same `build()`, so the size limit applies on *every* attempt, not just the
first. `regex_utils.rs`'s `compile_regex` ([`regex_utils.rs:497-499`](../../src/labels/regex_utils.rs#L497))
was rewritten to call the same shared function instead of carrying its own independent — and, per
this finding, also under-limited — copy of the retry logic, so there's one implementation instead
of two that could drift apart again. `DFA_SIZE_LIMIT` is a bonus beyond what this finding asked
for: it bounds the lazy-DFA cache at *match* time, which `size_limit` (a compile-time bound) does
not touch. The independent suggestion below — capping raw pattern length and matcher count — was
not part of this fix and is still open.

### F5. Per-key open + ACL check is on-thread and happens before any cap could apply
**`src/series/index/querier.rs:146-164`**

```rust
for id in ids {
    let Some(key) = postings.get_key_by_id(id) else { continue };
    let k = ctx.create_string(key.as_bytes());
    let perms = Some(AclPermissions::ACCESS);
    if let Some(guard) = get_timeseries(ctx, &k, perms, false)? { result.push((guard, k)); }
}
```

`get_timeseries` → `check_key_permissions` errors out on the first key the caller cannot read, so
the query **fails closed**. That is correct, and it does mean a tightly-scoped ACL user has a
bounded blast radius. But: (a) the check runs *inside* the O(N) loop, after N keys have already
been opened, so it does not make the denied query cheap; and (b) it offers nothing against a user
with `~*` — which, in practice, is most users.

Do not treat ACL as the DoS control. It is a useful second layer, not the first.

### F6. `FILTER_BY_RANGE` is applied after full materialization
**`src/series/index/querier.rs:126-144`**

`collect_series_from_postings` materializes every matched series *first*, then hands the vector to
`filter_series_by_date_range`. A caller who writes `TS.CARD FILTER_BY_RANGE <narrow window> FILTER
<broad>` expecting the date range to reduce the work gets the opposite: the range filter is pure
overhead on top of the full materialization. `count_matched_series`'s `(Some(range), false)` arm
([`querier.rs:268`](../../src/series/index/querier.rs#L268)) materializes the entire matched set
with ACL checks and key opens purely to call `.len()` on it.

The in-tree comment at [`querier.rs:196-199`](../../src/series/index/querier.rs#L196) already flags
the GIL concern for large matched sets. It is the right instinct; the parallel filter is not the
part that needs fixing.

### F7. `TS.QUERYINDEX` enumerates the keyspace, by design, with no limit
**`src/series/index/querier.rs:112-123`**

```rust
// TS.QUERYINDEX is a pure index lookup: it reveals every series matching the
// filter regardless of the caller's per-key read access.
```

The ACL reasoning is defensible and deliberate. The consequence is that `TS.QUERYINDEX` is the
cheapest way to enumerate every series key in the database (combine with [F1](#f1-a-single-negative-matcher-selects-the-entire-keyspace)), and the
reply itself — one bulk string per series — is unbounded. It is both an information-disclosure
consideration and a reply-buffer amplification vector.

### F8. Cluster fanout amplifies rather than bounds
**`src/commands/ts_mrange_fanout_command.rs`**, **`src/config.rs:35-36`**

`fanout_command_timeout` (500–10000 ms) bounds how long the coordinator *waits*. It does not cancel
work already dispatched — every shard runs its filter to completion regardless. Meanwhile the
coordinator materializes and merges all shards' results simultaneously, so its peak memory is the
sum across shards.

Consequently, a per-shard cap of `K` series yields a coordinator holding `K × num_shards`.
Any global budget has to be accounted at the coordinator and propagated into the fanout request.

**Since the audit:** `ts_mrange_fanout_command.rs` grew substantially under the same main merge —
it now does shard-side aggregation pushdown and merges pre-reduced partial states
(`handle_group_partials`, `compensate_group_partials`) for grouped/aggregated queries instead of
raw per-series data. That likely narrows this finding for the aggregated-query case, since less
data crosses the wire and sits in coordinator memory per shard. The core claim was spot-checked and
still holds: a timeout produces a coordinator-side error, not a cancel signal to shards
([`fanout_command.rs:32-33`](../../src/fanout/fanout_command.rs#L32) — `get_timeout` governs how
long the coordinator waits; [`fanout_command.rs:190-193`](../../src/fanout/fanout_command.rs#L190) —
`abort_error` is coordinator-side bookkeeping for fail-fast/timeout, not an RPC telling a shard to
stop). That check was not extended into the new pushdown paths, though, so treat this as narrowed
for grouped queries, not closed — a raw (non-aggregated) `TS.MRANGE` fanout still materializes full
per-shard results at the coordinator exactly as described above.

### F9. Everything runs on the main thread with no deadline
No filtered command blocks the client. `TS.FORECAST` demonstrates the pattern
([`ts_forecast.rs`](../../src/commands/ts_forecast.rs) uses `block_client`), but `TS.MRANGE`,
`TS.MGET`, `TS.QUERYINDEX`, and `TS.MDEL` all reply inline. There is no elapsed-time check inside
either the label-value scan loop ([F3](#f3-regex-and-contains-matchers-scan-every-distinct-label-value)) or the per-series materialization loop
([F5](#f5-per-key-open--acl-check-is-on-thread-and-happens-before-any-cap-could-apply)), so a query that turns out to be expensive cannot be abandoned partway.

---

## 4. Are max-series + max-samples sufficient?

**No — but they are the right first move.** Precisely:

### What they do cover

A cap on matched-series count, checked immediately after `postings_for_selectors` returns and
*before* `collect_series_from_postings` runs, is cheap (`Bitmap64::cardinality()` is O(#containers),
not O(#elements)) and directly bounds F2, F5, F6, and the blast radius of F7. That single check at
[`querier.rs:53`](../../src/series/index/querier.rs#L53) is the highest-leverage line in this
document.

A sample cap bounds F2's memory — *provided it is enforced as a running budget during iteration,
not as a post-hoc check on the assembled result*. Checking `total_samples > max` after building the
`Vec` means the allocation already happened; the OOM you were trying to prevent has occurred.

### What they do not cover

**Work is not proportional to results.** This is the central point. The bitmap does not exist until
stage 2 finishes, and stage 2 is where the regex scan (F3) burns its budget. `FILTER id=~".*zzz"`
matching zero series passes any conceivable `max_series` check *after* having done all its damage.
An attacker optimizing for damage will choose exactly these queries — high scan cost, empty result —
because they are the ones that evade result-shaped limits. To bound this you need a **scan budget**:
a counter over *candidate label values examined*, decremented inside the scan loop, that aborts the
query when exhausted.

**Compile-time costs precede all limits.** F4 happens during argument parsing. No result cap runs
that early.

**Caps are per-command, and attackers use concurrency.** `max_samples = 10M` with 64 concurrent
clients is a 64× multiplier on a limit that was sized for one query. A per-command cap needs either
a global concurrent-memory budget or a much more conservative per-command value than feels natural.

**Caps do not compose across the cluster** (F8). Per-shard enforcement gives the coordinator
`K × num_shards`.

**Caps have a correctness cost.** A dashboard whose legitimate query matches 10 001 series against
`max_series = 10000` now fails. You must decide, explicitly and per-command, between erroring and
truncating — and neither is right everywhere. Erroring is correct for `TS.MDEL` (a truncated
delete is data loss you cannot see). Truncating is defensible for `TS.QUERYINDEX` if the reply
signals truncation. `TS.MRANGE` silently truncating would produce wrong dashboards, quietly, which
is arguably worse than an error.

### The accurate framing

Result caps bound **damage**. Scan budgets and deadlines bound **cost**. Low-selectivity filters
are a *cost* attack that only sometimes produces large results. You need both, and if you can only
have one class of control today, the ordering below reflects effort-to-value, not category purity.

---

## 5. Practical strategies

### 5.1 Require at least one positive matcher (fixes F1)
**status: ✅ implemented**

Prometheus's rule: a selector must contain at least one matcher that does not match the empty
string. This was proposed as a two-line change to `postings_for_label_filters`, where the
`all_postings` seeding already happens:

```rust
if has_subtracting_matchers && !has_intersecting_matchers {
    return Err(ValkeyError::Str(error_consts::FILTER_REQUIRES_POSITIVE_MATCHER));
}
```

**What shipped** is a parse-time check in `parse_series_selector`
([`series_selector.rs:32-70`](../../src/parser/series_selector.rs#L32)) rather than a
postings-planning-time check — see the "fix as shipped" note under
[F1](#f1-a-single-negative-matcher-selects-the-entire-keyspace) for why that location covers more
(OR branches, `matches_empty()`) than the snippet above would have. It is, as predicted, a
**breaking change** for any client relying on bare-negative filters — such a client was already
issuing a full scan and now gets told so at parse time instead. `TS.CARD` already enforced the
analogous rule for its no-matcher case ([`querier.rs:272-278`](../../src/series/index/querier.rs#L272));
this extends the same reasoning to every other filtered command.

### 5.2 Cap matched series before materialization (bounds F2, F5, F6, F7)

At the single choke point every command shares:

```rust
// src/series/index/querier.rs, in series_by_selectors
let series_refs = postings.postings_for_selectors(selectors)?;

let max = MAX_MATCHED_SERIES.load(Ordering::Relaxed) as u64;
if max > 0 && series_refs.cardinality() > max {
    return Err(ValkeyError::String(format!(
        "TSDB: filter matched {} series, exceeding ts-max-matched-series ({max}). \
         Narrow the filter or raise the limit.",
        series_refs.cardinality()
    )));
}
```

Placing it here rather than in each command handler means one implementation covers `TS.MRANGE`,
`TS.MGET`, `TS.MDEL`, `TS.CARD`, and the label commands. `series_keys_by_selectors` needs the same
guard for `TS.QUERYINDEX`.

Make the error message name the config and state the observed cardinality. An operator who hits
this needs to know both numbers to decide whether to fix the query or raise the limit.

### 5.3 Budget the scan, not just the result (the real fix for F3)

Thread a decrementing budget through the label-value scan. This is the control that actually
addresses low selectivity, and it is the one a `max_series` cap cannot substitute for:

```rust
pub struct ScanBudget(Cell<u64>);

impl ScanBudget {
    fn charge(&self, n: u64) -> Result<(), ValkeyError> {
        let remaining = self.0.get().saturating_sub(n);
        self.0.set(remaining);
        if remaining == 0 {
            return Err(ValkeyError::Str(error_consts::QUERY_SCAN_BUDGET_EXCEEDED));
        }
        Ok(())
    }
}
```

`postings_for_label_matching` charges once per candidate value visited. Every caller in
`postings.rs` funnels through it, so one change instruments all the expensive paths at once.
A budget in the low millions is invisible to normal queries and stops `=~".*zzz"` against a 5M-value
label at ~0.2% of its cost.

The budget should be shared across all matchers in a command, not per-matcher, or an attacker just
sends more matchers.

### 5.4 Reject predictably-expensive filters up front

`filters.rs` already computes a cost for every matcher shape (12–50). `posting_stats.rs` already
tracks label cardinality. Multiply them:

```rust
let est = index.label_cardinality(&filter.label) * filter.cost();
if est > QUERY_COST_BUDGET { return Err(...); }
```

This is a heuristic and will misjudge some queries. Its value is that it fails *before* doing the
work, and its error message can be actionable: *"regex matcher on label `id` (5.2M distinct values)
has no literal prefix; add one or use `startswith`."* Pair it with 5.3 rather than relying on it
alone — the estimate is a filter, the budget is the backstop.

### 5.5 Stream replies with a running sample budget (fixes F2 properly)

The structural fix is to stop building `Vec<MRangeSeriesResult>`. Reply per-series as each is
decoded, and carry a `samples_emitted` counter that aborts when it crosses the budget. This turns
peak memory from `O(N × COUNT)` into `O(COUNT)` and removes the need to pick a `max_samples` value
that is simultaneously generous enough for real queries and small enough to survive 64 concurrent
attackers.

This is the largest change proposed here, and it conflicts with the `orx_parallel` fan-out at
`mrange.rs:318`/`:388` (which needs all results before it can collect). A middle path that preserves the
parallelism: keep the fan-out but have each worker charge its sample count against a shared
`AtomicU64` budget and bail early once it is exhausted. Peak memory stays `O(budget)` rather than
`O(N × COUNT)`.

If streaming is out of scope, the minimum viable version is: check the budget *inside* the
per-series loop, before `iter.collect()`, so the abort happens before the next allocation rather
than after the last one.

### 5.6 Fix the regex size limit (fixes F4)
**status: ✅ implemented**

```rust
fn build(re: &str) -> Result<Regex, regex::Error> {
    RegexBuilder::new(re)
        .size_limit(REGEX_SIZE_LIMIT)
        .dfa_size_limit(DFA_SIZE_LIMIT)
        .dot_matches_new_line(true)
        .build()
}

build(&anchored).or_else(|_| build(&try_escape_for_repeat_re(&anchored)))
```

This is essentially the code that shipped — see the "fix as shipped" note under
[F4](#f4-regex-size-limit-is-silently-bypassed-on-the-fallback-path) for the actual line numbers
and how `regex.rs` and `regex_utils.rs` ended up sharing one implementation instead of each getting
its own copy. Setting `dfa_size_limit` additionally bounds the lazy-DFA cache at match time, which
`size_limit` does not.

Independently, cap raw pattern length (say 1 KB) and the number of matchers per command at parse
time. These are free and they bound the multiplier. **Still open** — neither cap was part of this
fix.

### 5.7 Add a query deadline and move heavy work off-thread

A budget bounds a scan; a deadline bounds everything, including work you have not thought to
instrument. Check a monotonic deadline in the two hot loops — the label-value scan (F3) and the
per-series materialization loop (F5) — every few thousand iterations, so the cost of checking is
amortized to nothing.

Longer term, filtered read commands should follow `TS.FORECAST` and block the client rather than
occupy the main thread. That converts a server-wide stall into a single slow client. Note this is
a substantial change: `Context` is not `Send`, which is exactly why `filter_series_by_date_range`
collects IDs before parallelizing.

### 5.8 Compose limits across the cluster (F8)

Carry the remaining budget in the fanout request (`fanout.request.proto`) so shards enforce against
the global figure rather than each enforcing the full limit locally. Without this, per-shard
enforcement of `K` yields a coordinator holding `K × num_shards` and the limit does not mean what
its name says.

### 5.9 Observability, so limits can be tuned rather than guessed

Any limit shipped without a way to see how close real traffic runs to it will be set wrong, and the
first thing an operator does when a limit fires is disable it. Before or alongside the caps, expose:

- matched-series count and label-values-scanned per filtered command, in `TS.DEBUG`
- a slow-filter log (filter text, matched count, scanned count, elapsed) above a threshold
- counters for each limit-rejection reason, so a spike is attributable

---

## 6. Proposed configuration surface

| Config | Default | Bounds |
|---|---|---|
| `ts-max-matched-series` | `0` (off) → `100000` after a deprecation window | Stage 3/4 result size |
| `ts-max-query-samples` | `0` (off) → sized to `maxmemory` | Stage 4 memory |
| `ts-query-scan-budget` | `5000000` candidate values | **Stage 2 work** (the F3 gap) |
| `ts-query-timeout-ms` | `0` (off) | All stages |
| `ts-max-regex-size` | `16384` | Stage 1 |
| `ts-max-filters-per-command` | `32` | Stage 1 multiplier |

Ship the caps defaulted to off, log a warning when a query *would* have tripped them, gather a
release of real data, then flip the defaults on. Turning them on by default in the same release
that introduces them will break someone's dashboard and the limit will be blamed rather than the
query.

Truncate-vs-error, per command: **error** for `TS.MDEL` (silent partial deletion is invisible data
loss) and `TS.MRANGE` (silent truncation produces wrong charts). **Truncate with an explicit
marker** is defensible for `TS.QUERYINDEX` and the label commands, where the caller is exploring
rather than computing.

Deployment profile guidance:

- **multi-tenant / shared metrics service**: favor strict defaults (`ts-query-scan-budget`,
  `ts-max-matched-series`, timeout enabled) and treat limit hits as operator-visible incidents.
- **single-tenant / trusted operators**: start with warn-only telemetry for one release,
  then enable caps with environment-specific thresholds.

"No explicit cap" does not mean "no effective bound" in production: wall-clock timeouts,
client-side truncation, memory pressure, and shard topology can still bound impact. These are weak,
non-deterministic controls and should not be treated as substitutes for explicit module limits.

---

## 7. Suggested ordering

**P0 — small diffs, no behavior change for valid queries**
1. ~~[F4](#f4-regex-size-limit-is-silently-bypassed-on-the-fallback-path): apply `size_limit` to both regex builders (§5.6).~~ **Done**, 2026-07-10 — `Refactor regex compilation to bound input and DFA size`.
2. ~~[F1](#f1-a-single-negative-matcher-selects-the-entire-keyspace): require one positive matcher (§5.1).~~ **Done**, 2026-07-10 — `Add validation for series selectors with positive matchers`.

**P1 — the limits, plus the telemetry to tune them**
3. `ts-max-matched-series` at the `querier.rs` choke point (§5.2).
4. Scan budget in `postings_for_label_matching` (§5.3) — without this, P1 leaves F3 wide open, which is the finding the original question was really about.
5. Per-command observability (§5.9).

P1 acceptance criteria (to make rollout decision-grade, not prose-grade):

- `ts-query-scan-budget` is enforced inside `postings_for_label_matching` and aborts with a
  specific, user-visible error once the budget is exceeded.
- `ts-max-matched-series` is checked before `collect_series_from_postings` materializes keys/guards.
- limit hits are externally observable via counters/logs keyed by reason (scan budget, matched
  series, timeout), so defaults can be tuned from data.

**P2 — structural**
6. Sample budget enforced during iteration; ideally streaming replies (§5.5).
7. Query deadline in both hot loops (§5.7).
8. Cluster budget propagation (§5.8).

**P3**
9. Cost-based pre-flight rejection (§5.4).
10. Move filtered reads off the main thread (§5.7).

---

## 8. Reproductions and evidence matrix

Evidence matrix for this branch snapshot:

| Finding class | Code path confirmed | Live reproduced | Benchmarked |
|---|---|---|---|
| F1 (selector must include positive non-empty matcher) | yes | partial (unit tests) | no |
| F4 (regex fallback preserves size limits) | yes | not yet | no |
| F2/F3 (cost amplification: materialization + value scans) | yes | not yet | no |
| F5–F9 (main-thread, cluster, rollout/ops risks) | yes | not yet | no |

Confidence by finding type:

- **high confidence (mechanism)**: F1/F2/F3/F4 code-path claims.
- **medium confidence (impact magnitude)**: F2/F3/F8 without runtime numbers.
- **low confidence (operator ergonomics)**: final defaults without telemetry from production-like traffic.

Not yet executed against a running server — these are derived from reading the code paths, and
should be confirmed with a live instance before this document is used to justify shipping (or not
shipping) anything. What can be said today:

**F1, before the fix** — `FILTER host!=__nonexistent__` against any non-trivial keyspace returns
every series; `TS.MRANGE - + FILTER host!=__nonexistent__` is the read form,
`TS.MDEL ... FILTER host!=__nonexistent__` the destructive one.

**F1, after the fix (this branch)** — the same input now returns a parse error ("please provide at
least one matcher", RedisTimeSeries' own wording for the same condition) before any index lookup
happens. This is covered by unit tests in
[`series_selector_tests.rs`](../../src/parser/series_selector_tests.rs) (e.g.
`test_parse_series_selector_with_negated_label_matchers`,
`test_parse_series_selector_or_branch_without_positive_matcher_is_rejected`,
`regex_match_all_selector_normalizes_to_match_all_but_is_rejected`) — stronger evidence than "read
the code" but still short of running against a live server: the module's test binary needs the
Valkey allocator to run and wasn't exercised end-to-end for this update.

**F4, before the fix** — `FILTER l=~"((a{100}){100}){100}"` (or any pattern that trips the 16 KB
`size_limit`) falls through to `Regex::new`'s ~10 MiB default budget instead of being rejected.

**F4, after the fix (this branch)** — the same pattern now hits `size_limit` on both the primary
and fallback compile attempts and is rejected as an invalid regex. No dedicated test was added for
this one; confirming it requires either a unit test asserting the error or a live-server
timing/memory comparison before and after.

**Still open, no repro attempted here** — F2 (`COUNT` × series memory), F3 (label-value scan cost),
F5–F9. These need a loaded index (millions of distinct label values, for F3 especially) to produce
numbers worth citing; reading the code paths is sufficient to establish the mechanism but not the
magnitude.

Minimal benchmark protocol to close this gap:

1. fix dataset shape per run: `N_series`, `N_distinct(label)`, avg samples per series, shard count.
2. run two workloads per class: (a) low-result/high-cost selector, (b) high-result baseline.
3. record p50/p95/p99 latency, peak RSS, and scanned-values/matched-series counters.
4. for fanout cases, report coordinator vs shard memory separately.

## 9. Threat-model assumptions (make explicit before policy decisions)

- There exists an authenticated actor that can issue expensive selectors repeatedly.
- Main-thread stall duration is SLO-relevant for the deployment.
- Regex/contains-heavy selectors are realistic workload inputs, not pathological-only tests.
- Additional behavior changes (reject/error/truncate) are acceptable with a migration period.

If any assumption is false for your environment, keep the mechanism fixes but tune defaults,
rollout mode, and error policy accordingly.