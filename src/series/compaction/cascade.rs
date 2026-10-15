use super::engine::{CompactionContext, CompactionOp, apply_op};
use super::{
    COMPACTION_FAILURES, COMPACTION_STATE_REBUILDS, CompactionRule, TEMP_VEC_LEN,
    rebuild_open_bucket_as_of,
};
use crate::common::block_on_keys::signal_timeseries_ready;
use crate::common::context::notify_keyspace_event;
use crate::common::logging::log_warning;
use crate::common::threads::ParMutRayon;
use crate::common::{Sample, Timestamp};
use crate::error::{TsdbError, TsdbResult};
use crate::series::{SeriesGuardMut, TimeSeries, try_get_timeseries_mut};
use orx_parallel::Par;
use smallvec::SmallVec;
use std::collections::HashSet;
use std::sync::atomic::{AtomicBool, Ordering as AtomicOrdering};
use valkey_module::{Context, ValkeyString};

const PARALLEL_THRESHOLD: usize = 2;

/// Set by `TS._DEBUG FAIL_NEXT_COMPACTION` (debug mode only): the next rule application fails
/// without running, exactly as an internal error would leave it.
static FAIL_NEXT_COMPACTION: AtomicBool = AtomicBool::new(false);

/// Makes the next rule application fail. For tests; see [`FAIL_NEXT_COMPACTION`].
pub(crate) fn fail_next_compaction() {
    FAIL_NEXT_COMPACTION.store(true, AtomicOrdering::Relaxed);
}

/// Consumes the flag [`fail_next_compaction`] set. Every rule application of every write asks, so
/// the common answer, no, costs a load rather than the read-modify-write a bare `swap` would.
fn take_injected_failure() -> bool {
    FAIL_NEXT_COMPACTION.load(AtomicOrdering::Relaxed)
        && FAIL_NEXT_COMPACTION.swap(false, AtomicOrdering::Relaxed)
}

pub fn apply_compaction(
    ctx: &Context,
    series: &mut TimeSeries,
    op: CompactionOp,
) -> TsdbResult<()> {
    if series.rules.is_empty() {
        return Ok(());
    }
    process_series_with_compaction(ctx, series, op)
}

/// Outcome of applying one compaction rule to its destination series.
struct RuleOutcome {
    /// Position of the rule in its source's rule list — and so of its destination in the
    /// [`Destinations`] the rules were applied to.
    rule_index: usize,
    /// `dest`'s last timestamp *before* this operation ran — the `prev_last` a cascaded
    /// `Ingest` into `dest`'s own rules must use.
    dest_prev_last: Option<Timestamp>,
    /// Samples committed to `dest` by this operation, in commit order (not necessarily
    /// sorted or unique — see [`dedupe_written_samples`]).
    written: Vec<Sample>,
    /// Whether an append closed a bucket into `dest` — the only thing that emits `ts.add:dest`
    /// (blocked readers are woken on `written`). A back-fill
    /// into an already-closed bucket sets `written` but not this.
    closed_bucket: bool,
    /// The span of `dest` a `RemoveRange` rewrote — the range `dest`'s own rules resync next.
    rewritten: Option<(Timestamp, Timestamp)>,
}

/// Sorts and deduplicates a rule's written samples for use as the next cascade level's batch.
///
/// A single [`CompactionOp::Ingest`] application can write the same bucket twice: once from
/// the streaming append path, then again when a same-batch historical upsert recalculates that
/// bucket (see `handle_mixed_batch_compaction`). The recalculation is authoritative and always runs
/// after the streaming write, so a stable sort followed by keeping the last of each duplicate
/// timestamp preserves the correct (final) value while producing the sorted, unique-timestamp
/// sequence `Ingest` requires downstream.
fn dedupe_written_samples(mut samples: Vec<Sample>) -> Vec<Sample> {
    if samples.len() < 2 {
        return samples;
    }
    samples.sort_by_key(|s| s.timestamp);
    let mut out: Vec<Sample> = Vec::with_capacity(samples.len());
    for sample in samples {
        if out
            .last()
            .is_some_and(|last: &Sample| last.timestamp == sample.timestamp)
        {
            *out.last_mut().unwrap() = sample;
        } else {
            out.push(sample);
        }
    }
    out
}

/// A cascade entry: (destination key, samples written to it, its previous last timestamp).
type PendingCompactionWrite = (ValkeyString, Vec<Sample>, Option<Timestamp>);

/// Keys of the series whose rules a compaction pass has already run: the series it started
/// from (recorded once the pass cascades), plus every cascaded series.
///
/// Rule creation rejects circular chains (`check_new_rule_links`), but the
/// traversal keeps this guard anyway so a corrupted topology can neither loop forever nor open
/// a series that is already borrowed further up the pass.
///
/// A pass usually visits a handful of series, which a linear scan serves best. A wide fan-out
/// does not: every removal cascades to every level, and scanning a list per cascaded series made
/// a `TS.DEL` over thousands of destinations quadratic on the main thread. Past
/// [`VISITED_LINEAR_MAX`] keys, the rest go to a hash set.
#[derive(Default)]
struct Visited {
    linear: SmallVec<[Box<[u8]>; TEMP_VEC_LEN]>,
    hashed: HashSet<Box<[u8]>>,
}

const VISITED_LINEAR_MAX: usize = 16;

impl Visited {
    fn contains(&self, key: &[u8]) -> bool {
        self.linear.iter().any(|k| k.as_ref() == key) || self.hashed.contains(key)
    }

    fn insert(&mut self, key: &[u8]) {
        if self.linear.len() < VISITED_LINEAR_MAX {
            self.linear.push(key.into());
        } else {
            self.hashed.insert(key.into());
        }
    }
}

/// Iterates through compaction rules (possibly in parallel) and applies the specified operation,
/// cascading through chained compaction series (a destination that itself has rules).
///
/// Cascading strategy differs by operation:
/// - `RemoveRange` hands each level the span of its parent that the level above rewrote, and
///   the level recomputes its overlapping buckets from that parent as it now stands. Reapplying
///   the original range instead misses a parent bucket that starts before it (recalculated
///   because the removal reached into it) whenever the child's buckets do not nest in the
///   parent's.
/// - `Ingest` instead feeds each destination's own *written* samples — the
///   values actually committed to it — as an `Ingest` into that destination's rules. This
///   makes every rule aggregate over its declared source series, matching a rule's own
///   definition (e.g. `mid -> dest AVG` averages `mid`'s stored samples, not the top-level raw
///   stream). Reusing the original op across levels would instead feed the *raw* top-level
///   samples straight into every descendant's aggregator, which only happens to agree with the
///   declared semantics for associative aggregators (SUM/MIN/MAX) and silently diverges for
///   AVG/COUNT/STD/VAR/RANGE.
fn process_series_with_compaction(
    ctx: &Context,
    series: &mut TimeSeries,
    op: CompactionOp,
) -> TsdbResult<()> {
    // `ts.add:dest` fires only when compaction appends new bucket data to a
    // downstream series (a bucket close, `RuleOutcome::closed_bucket`).
    // Reference-observed (§7.3): a back-fill into an already-closed bucket
    // recomputes the destination silently, and a TS.DEL propagated into
    // destinations emits only the source's `ts.del`. Every `Ingest` level —
    // the source's rules and each cascaded level — follows the same rule.
    //
    // Blocked `TS.READ` readers are woken on a different, wider condition: every
    // destination that materialized a sample, close or not, because a back-fill
    // into a bucket the rule skipped creates a destination sample too.
    let mut written: SmallVec<[WrittenDestination; TEMP_VEC_LEN]> = SmallVec::new();
    let mut visited = Visited::default();

    let destinations = get_compaction_series(ctx, series, &visited);
    if destinations.is_empty() {
        return Ok(());
    }
    let Destinations {
        mut keys,
        series: guards,
    } = destinations;

    // A rule that fails does not end the pass. Every outcome — its siblings', and its own up to
    // the failure — still cascades into the destinations' own rules, and still notifies: those
    // destinations were written, and a rule below one that skipped its cascade would take the
    // next sample for an append on state that never saw this one, with nothing marking it
    // `dirty`. A failed `RemoveRange` cascades its span too: a level recomputes from what its
    // parent actually holds, so an unchanged parent leaves it unchanged. The first error is
    // returned once the pass has run to its end.
    let AppliedRules {
        outcomes,
        error: mut first_error,
    } = apply_rules_on_destinations(series, guards, op);

    match op {
        CompactionOp::RemoveRange { .. } => {
            let mut pending: SmallVec<[PendingRemoval; TEMP_VEC_LEN]> = SmallVec::new();
            queue_rewritten_outcomes(&mut keys, &outcomes, &mut pending);
            if !pending.is_empty() {
                visited.insert(&series.key);
            }

            while let Some((child_key, (start, end))) = pending.pop() {
                if visited.contains(child_key.as_slice()) {
                    continue;
                }
                let Ok(Some(mut child)) = try_get_timeseries_mut(ctx, &child_key, None) else {
                    continue;
                };
                if child.rules.is_empty() {
                    continue;
                }
                visited.insert(child_key.as_slice());

                let child_destinations = get_compaction_series(ctx, &mut child, &visited);
                if child_destinations.is_empty() {
                    continue;
                }
                let Destinations {
                    keys: mut child_keys,
                    series: child_guards,
                } = child_destinations;
                let child_op = CompactionOp::RemoveRange { start, end };
                let AppliedRules { outcomes, error } =
                    apply_rules_on_destinations(&mut child, child_guards, child_op);
                first_error = first_error.or(error);
                queue_rewritten_outcomes(&mut child_keys, &outcomes, &mut pending);
            }
        }
        _ => {
            let mut pending: SmallVec<[PendingCompactionWrite; TEMP_VEC_LEN]> = SmallVec::new();
            queue_written_outcomes(&mut keys, outcomes, &mut written, &mut pending);
            if !pending.is_empty() {
                visited.insert(&series.key);
            }

            while let Some((child_key, samples, prev_last)) = pending.pop() {
                if visited.contains(child_key.as_slice()) {
                    continue;
                }
                let Ok(Some(mut child)) = try_get_timeseries_mut(ctx, &child_key, None) else {
                    continue;
                };
                if child.rules.is_empty() {
                    continue;
                }
                visited.insert(child_key.as_slice());

                let child_destinations = get_compaction_series(ctx, &mut child, &visited);
                if child_destinations.is_empty() {
                    continue;
                }
                let Destinations {
                    keys: mut child_keys,
                    series: child_guards,
                } = child_destinations;

                // A cascaded batch is this level's own committed buckets, which are published
                // in bucket order — so input order and sorted order coincide.
                let cascade_order: SmallVec<[Timestamp; TEMP_VEC_LEN]> =
                    samples.iter().map(|s| s.timestamp).collect();
                let batch_op = CompactionOp::Ingest {
                    samples: &samples,
                    prev_last,
                    input_order: &cascade_order,
                };
                let AppliedRules { outcomes, error } =
                    apply_rules_on_destinations(&mut child, child_guards, batch_op);
                first_error = first_error.or(error);
                queue_written_outcomes(&mut child_keys, outcomes, &mut written, &mut pending);
            }
        }
    }

    if !written.is_empty() {
        notify_compaction(ctx, &written);
    }

    first_error.map_or(Ok(()), Err)
}

/// A destination that compaction committed at least one sample to, for [`notify_compaction`].
struct WrittenDestination {
    key: ValkeyString,
    /// An append closed a bucket there: the `ts.add:dest` condition, narrower than "written".
    closed_bucket: bool,
}

/// Queues every destination that `outcomes` wrote to for the next cascade level — a back-fill
/// must still cascade — and records it in `written` for [`notify_compaction`].
fn queue_written_outcomes(
    keys: &mut [Option<ValkeyString>],
    outcomes: Vec<RuleOutcome>,
    written: &mut SmallVec<[WrittenDestination; TEMP_VEC_LEN]>,
    pending: &mut SmallVec<[PendingCompactionWrite; TEMP_VEC_LEN]>,
) {
    for outcome in outcomes {
        if outcome.written.is_empty() {
            continue;
        }
        let Some(dest_key) = keys[outcome.rule_index].take() else {
            continue;
        };
        written.push(WrittenDestination {
            key: dest_key.clone(),
            closed_bucket: outcome.closed_bucket,
        });
        pending.push((
            dest_key,
            dedupe_written_samples(outcome.written),
            outcome.dest_prev_last,
        ));
    }
}

/// A cascaded removal: (destination key, the span of it the level above rewrote).
type PendingRemoval = (ValkeyString, (Timestamp, Timestamp));

/// Queues every destination that a removal rewrote for the next cascade level, with its span.
fn queue_rewritten_outcomes(
    keys: &mut [Option<ValkeyString>],
    outcomes: &[RuleOutcome],
    pending: &mut SmallVec<[PendingRemoval; TEMP_VEC_LEN]>,
) {
    for outcome in outcomes {
        if let Some(span) = outcome.rewritten
            && let Some(dest_key) = keys[outcome.rule_index].take()
        {
            pending.push((dest_key, span));
        }
    }
}

/// What one pass of a series' rules produced: the outcome of every rule that ran, and the first
/// error among them. A failed rule's outcome is kept too, holding whatever it committed before
/// failing — one rule failing must hide neither what the others committed nor what it did.
struct AppliedRules {
    outcomes: Vec<RuleOutcome>,
    error: Option<TsdbError>,
}

fn apply_rules_on_destinations(
    series: &mut TimeSeries,
    destinations: SmallVec<[Option<SeriesGuardMut>; TEMP_VEC_LEN]>,
    op: CompactionOp,
) -> AppliedRules {
    with_rules_taken(series, |series, rules| {
        apply_rules_internal(series, rules, destinations, op)
    })
}

/// Runs `f` with `series`' rules moved out of it, so the rules can be mutated (in parallel) while
/// the series is read, and puts them back however `f` ends.
///
/// Putting them back after `f` returns is not enough: a panic in a rule application unwinds past
/// it. A command handler aborts the server on that, but the fanout workers that run `TS.MDEL`'s
/// shard share catch it, so the series would live on with no rules — `TS.INFO` no longer lists
/// them, writes stop compacting, and the next save persists the loss while the replica, which
/// applies the propagated `TS.DEL` on the command path, keeps compacting. On a panic every rule
/// is also marked `dirty`: one may be part-way through an update, and the next write rebuilds its
/// open bucket from the source.
fn with_rules_taken<R>(
    series: &mut TimeSeries,
    f: impl FnOnce(&TimeSeries, &mut [CompactionRule]) -> R,
) -> R {
    struct RestoreRules<'a> {
        series: &'a mut TimeSeries,
        rules: Vec<CompactionRule>,
    }

    impl Drop for RestoreRules<'_> {
        fn drop(&mut self) {
            if std::thread::panicking() {
                for rule in &mut self.rules {
                    rule.dirty = true;
                }
            }
            self.series.rules = std::mem::take(&mut self.rules);
        }
    }

    let rules = std::mem::take(&mut series.rules);
    let mut guard = RestoreRules { series, rules };
    // Distinct fields of the guard: the series is read while the rules are mutated.
    f(&*guard.series, &mut guard.rules)
}

/// Whether `op` is worth spreading across rules. One sample is O(1) aggregator work per rule —
/// far less than a pool handoff — so `TS.ADD` into a source with several rules stays on the
/// command thread; batches and removals, which can recalculate many buckets, fan out.
fn runs_rules_in_parallel(op: &CompactionOp, rule_count: usize) -> bool {
    if rule_count < PARALLEL_THRESHOLD {
        return false;
    }
    match op {
        CompactionOp::Ingest { samples, .. } => samples.len() > 1,
        CompactionOp::RemoveRange { .. } => true,
    }
}

/// Internal function that handles execution of compaction rules. `child_series` holds one
/// entry per rule, in rule order; a rule whose entry is `None` is skipped.
///
/// Each failing rule is logged and counted once, here, and is left `dirty`; its outcome is still
/// returned with the samples it committed before failing.
fn apply_rules_internal(
    series: &TimeSeries,
    rules: &mut [CompactionRule],
    child_series: SmallVec<[Option<SeriesGuardMut>; TEMP_VEC_LEN]>,
    op: CompactionOp,
) -> AppliedRules {
    let mut destinations = rules
        .iter_mut()
        .zip(child_series)
        .enumerate()
        .filter_map(|(index, (rule, dest))| dest.map(|dest| (index, rule, dest)))
        .collect::<Vec<_>>();
    if destinations.is_empty() {
        return AppliedRules {
            outcomes: Vec::new(),
            error: None,
        };
    }

    let parallel = runs_rules_in_parallel(&op, destinations.len());
    let results = destinations
        .par_mut_rayon()
        .num_threads(if parallel { 0 } else { 1 }) // 0 is shorthand for Auto
        .map(|(rule_index, rule, dest_guard)| {
            let rule_index = *rule_index;
            let dest_prev_last = dest_guard.last_sample.map(|s| s.timestamp);
            if rule.dirty {
                // A failed application left the open bucket short of samples the source stored.
                // A removal rebuilds it anyway; an ingest does so here, before it streams in.
                if let CompactionOp::Ingest { prev_last, .. } = op {
                    rebuild_open_bucket_as_of(series, rule, prev_last);
                }
                COMPACTION_STATE_REBUILDS.fetch_add(1, AtomicOrdering::Relaxed);
            }
            // An application can fail after committing buckets to `dest` (an `Ingest` streams
            // sample by sample), so its outcome is kept either way: `dest` holds those samples,
            // and its own rules must still see them.
            let (error, written, closed_bucket, rewritten) = if take_injected_failure() {
                (
                    Some(TsdbError::General(
                        "injected compaction failure".to_string(),
                    )),
                    Vec::new(),
                    false,
                    None,
                )
            } else {
                let mut cctx = CompactionContext::new(series, dest_guard, rule);
                let error = apply_op(&mut cctx, op).err();
                (error, cctx.written, cctx.closed_bucket, cctx.rewritten)
            };
            rule.dirty = error.is_some();
            let outcome = RuleOutcome {
                rule_index,
                dest_prev_last,
                written,
                closed_bucket,
                rewritten,
            };
            (outcome, error)
        })
        .collect::<Vec<(RuleOutcome, Option<TsdbError>)>>();

    let mut outcomes = Vec::with_capacity(results.len());
    let mut first_error: Option<TsdbError> = None;

    for (outcome, error) in results {
        if let Some(error) = error {
            let msg = format!("Failed to handle compaction rule for series: {error}");
            log_warning(msg);
            COMPACTION_FAILURES.fetch_add(1, AtomicOrdering::Relaxed);
            first_error.get_or_insert(error);
        }
        outcomes.push(outcome);
    }

    AppliedRules {
        outcomes,
        error: first_error,
    }
}

/// The destinations of a series' rules, opened for writing: one slot per rule, in rule order.
/// A `None` slot is a rule that is kept but not applied this time.
struct Destinations<'a> {
    keys: SmallVec<[Option<ValkeyString>; TEMP_VEC_LEN]>,
    series: SmallVec<[Option<SeriesGuardMut<'a>>; TEMP_VEC_LEN]>,
}

impl Destinations<'_> {
    fn is_empty(&self) -> bool {
        self.series.iter().all(Option::is_none)
    }
}

/// How one rule's destination resolved.
enum Resolved<'a> {
    Open(ValkeyString, SeriesGuardMut<'a>),
    /// Keep the rule, but don't apply it now.
    Skip,
    /// The rule no longer has a destination; remove it.
    Stale,
}

/// Opens the destination of each of `series`' rules for writing.
///
/// A destination is found by key, and is accepted only while it still
/// names `series` as its source (the back-link). A rule whose destination key is gone, holds a
/// different kind of value, or now belongs to another source (the key was deleted and
/// re-created, overwritten by `RENAME`, restored from another series' dump...) is removed:
/// writing into it would corrupt an unrelated series.
fn get_compaction_series<'a>(
    ctx: &'a Context,
    series: &mut TimeSeries,
    visited: &Visited,
) -> Destinations<'a> {
    let mut destinations = Destinations {
        keys: SmallVec::new(),
        series: SmallVec::new(),
    };
    if series.rules.is_empty() {
        return destinations;
    }

    let mut stale: SmallVec<[usize; TEMP_VEC_LEN]> = SmallVec::new();
    for (index, rule) in series.rules.iter().enumerate() {
        let resolved = resolve_destination(ctx, &series.key, rule, visited, &destinations);
        let (dest_key, dest) = match resolved {
            Resolved::Open(dest_key, dest) => (Some(dest_key), Some(dest)),
            Resolved::Skip => (None, None),
            Resolved::Stale => {
                stale.push(index);
                (None, None)
            }
        };
        destinations.keys.push(dest_key);
        destinations.series.push(dest);
    }

    for &index in stale.iter().rev() {
        series.rules.remove(index);
        destinations.keys.remove(index);
        destinations.series.remove(index);
    }
    destinations
}

fn resolve_destination<'a>(
    ctx: &'a Context,
    source_key: &[u8],
    rule: &CompactionRule,
    visited: &Visited,
    opened: &Destinations<'a>,
) -> Resolved<'a> {
    let dest_bytes = rule.dest.key();
    if dest_bytes == source_key || visited.contains(dest_bytes) {
        // A cycle: the series is already borrowed by this pass.
        return Resolved::Skip;
    }
    if opened
        .keys
        .iter()
        .flatten()
        .any(|k| k.as_slice() == dest_bytes)
    {
        // A second rule for the same destination; it can never have been valid.
        return Resolved::Stale;
    }

    let dest_key = rule.dest.to_key_string(ctx);
    let Ok(Some(dest)) = try_get_timeseries_mut(ctx, &dest_key, None) else {
        ctx.log_verbose("Destination series for compaction not found");
        return Resolved::Stale;
    };
    let links_back = dest
        .src_series
        .as_ref()
        .is_some_and(|src| src.points_to(source_key));
    if !links_back {
        ctx.log_verbose("Compaction destination no longer names this series as its source");
        return Resolved::Stale;
    }
    Resolved::Open(dest_key, dest)
}

/// Announces what compaction wrote to each destination: the `ts.add:dest` keyspace event for
/// those where an append closed a bucket, and a wake-up for every destination that materialized
/// a sample.
///
/// The two conditions differ. A back-fill into an already-closed bucket recomputes it silently
/// (no event), but a back-fill into a bucket the rule skipped creates a new destination sample,
/// which a `TS.READ` reader blocked on the rollup key may be waiting for. Waking a reader that
/// has nothing new is harmless — its callback re-checks its cursor and threshold. This is the
/// one place both direct and cascaded compaction output can wake such a reader.
fn notify_compaction(ctx: &Context, destinations: &[WrittenDestination]) {
    for dest in destinations {
        if dest.closed_bucket {
            notify_keyspace_event(ctx, c"ts.add:dest", &dest.key);
        }
        signal_timeseries_ready(ctx, &dest.key);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::aggregators::AggregationType;
    use crate::common::Sample;
    use crate::series::SeriesLink;
    use std::panic::{AssertUnwindSafe, catch_unwind};

    #[test]
    fn visited_finds_keys_on_both_sides_of_the_linear_limit() {
        let mut visited = Visited::default();
        let keys: Vec<Vec<u8>> = (0..VISITED_LINEAR_MAX * 3)
            .map(|i| format!("k{i}").into_bytes())
            .collect();
        for key in &keys {
            assert!(!visited.contains(key));
            visited.insert(key);
        }
        assert!(keys.iter().all(|key| visited.contains(key)));
        assert!(!visited.contains(b"absent"));
        assert_eq!(visited.linear.len(), VISITED_LINEAR_MAX);
    }

    fn series_with_rules(dests: &[&str]) -> TimeSeries {
        let mut series = TimeSeries::new();
        series.rules = dests
            .iter()
            .map(|dest| CompactionRule {
                dest: SeriesLink::from(*dest),
                aggregator: AggregationType::Sum.into(),
                bucket_duration: 1000,
                align_timestamp: 0,
                bucket_start: None,
                has_samples: false,
                dirty: false,
            })
            .collect();
        series
    }

    fn dests(series: &TimeSeries) -> Vec<Vec<u8>> {
        series.rules.iter().map(|r| r.dest.key().to_vec()).collect()
    }

    #[test]
    fn a_panic_in_a_rule_application_puts_the_rules_back_dirty() {
        let mut series = series_with_rules(&["a", "b"]);
        let caught = catch_unwind(AssertUnwindSafe(|| {
            with_rules_taken(&mut series, |series, rules| {
                assert!(
                    series.rules.is_empty(),
                    "the rules are moved out while applied"
                );
                rules[0].update(0, 1.0);
                panic!("rule application failed part-way");
            })
        }));
        assert!(caught.is_err());
        assert_eq!(dests(&series), [b"a".to_vec(), b"b".to_vec()]);
        assert!(
            series.rules.iter().all(|r| r.dirty),
            "a half-applied rule must be rebuilt"
        );
        assert!(
            series.rules[0].has_samples,
            "the state itself is kept for the rebuild"
        );
    }

    #[test]
    fn rules_come_back_unchanged_in_dirtiness_when_nothing_panics() {
        let mut series = series_with_rules(&["a", "b"]);
        series.rules[1].dirty = true;
        let n = with_rules_taken(&mut series, |_, rules| {
            rules[0].update(0, 1.0);
            rules.len()
        });
        assert_eq!(n, 2);
        assert_eq!(dests(&series), [b"a".to_vec(), b"b".to_vec()]);
        assert_eq!(
            series.rules.iter().map(|r| r.dirty).collect::<Vec<_>>(),
            [false, true]
        );
    }

    #[test]
    fn a_single_sample_never_runs_rules_in_parallel() {
        let one = [Sample::new(1, 1.0)];
        let two = [Sample::new(1, 1.0), Sample::new(2, 1.0)];
        let single = CompactionOp::Ingest {
            samples: &one,
            prev_last: None,
            input_order: &[1],
        };
        let batch = CompactionOp::Ingest {
            samples: &two,
            prev_last: None,
            input_order: &[1, 2],
        };
        assert!(!runs_rules_in_parallel(&single, 8));
        assert!(runs_rules_in_parallel(&batch, 2));
        assert!(!runs_rules_in_parallel(&batch, 1));
        assert!(runs_rules_in_parallel(
            &CompactionOp::RemoveRange { start: 0, end: 1 },
            2
        ));
    }
}
