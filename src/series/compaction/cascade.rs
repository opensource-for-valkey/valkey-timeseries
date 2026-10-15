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
/// Rule creation rejects circular chains (`check_new_rule_circular_dependency`), but the
/// traversal keeps this guard anyway so a corrupted topology can neither loop forever nor open
/// a series that is already borrowed further up the pass.
#[derive(Default)]
struct Visited(SmallVec<[Box<[u8]>; TEMP_VEC_LEN]>);

impl Visited {
    fn contains(&self, key: &[u8]) -> bool {
        self.0.iter().any(|k| k.as_ref() == key)
    }

    fn insert(&mut self, key: &[u8]) {
        self.0.push(key.into());
    }
}

/// Iterates through compaction rules (possibly in parallel) and applies the specified operation,
/// cascading through chained compaction series (a destination that itself has rules).
///
/// Cascading strategy differs by operation:
/// - `RemoveRange` reapplies the *same* absolute `[start, end]` range at every level. This is
///   correct regardless of chain depth because each level's recalculation reads directly from
///   its own immediate parent (already updated by the level above), never from a value carried
///   across levels.
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

    // A rule that fails does not end the pass. Its siblings' outcomes still cascade into their
    // destinations' own rules, and still notify: those destinations were written, and a rule
    // below one that skipped its cascade would take the next sample for an append on state that
    // never saw this one, with nothing marking it `dirty`. Only the failing rule's own branch
    // stops, and the first error is returned once the pass has run to its end.
    let AppliedRules {
        outcomes,
        error: mut first_error,
    } = apply_rules_on_destinations(series, guards, op);

    match op {
        CompactionOp::RemoveRange { .. } => {
            let mut pending: SmallVec<[ValkeyString; TEMP_VEC_LEN]> = outcomes
                .iter()
                .filter_map(|o| keys[o.rule_index].take())
                .collect();
            if !pending.is_empty() {
                visited.insert(&series.key);
            }

            while let Some(child_key) = pending.pop() {
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
                let AppliedRules { outcomes, error } =
                    apply_rules_on_destinations(&mut child, child_guards, op);
                first_error = first_error.or(error);
                pending.extend(
                    outcomes
                        .iter()
                        .filter_map(|o| child_keys[o.rule_index].take()),
                );
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

/// What one pass of a series' rules produced: the outcome of every rule that succeeded, and the
/// first error of those that did not. Both are kept — one rule failing must not hide what the
/// others committed.
struct AppliedRules {
    outcomes: Vec<RuleOutcome>,
    error: Option<TsdbError>,
}

fn apply_rules_on_destinations(
    series: &mut TimeSeries,
    destinations: SmallVec<[Option<SeriesGuardMut>; TEMP_VEC_LEN]>,
    op: CompactionOp,
) -> AppliedRules {
    let mut rules = std::mem::take(&mut series.rules);
    let result = apply_rules_internal(series, &mut rules, destinations, op);
    series.rules = rules;
    result
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
/// Each failing rule is logged and counted once, here, and is left `dirty`.
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
    let results: Vec<Result<RuleOutcome, TsdbError>> = destinations
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
            let (result, written, closed_bucket) = if take_injected_failure() {
                (
                    Err(TsdbError::General(
                        "injected compaction failure".to_string(),
                    )),
                    Vec::new(),
                    false,
                )
            } else {
                let mut cctx = CompactionContext::new(series, dest_guard, rule);
                let result = apply_op(&mut cctx, op);
                (result, cctx.written, cctx.closed_bucket)
            };
            rule.dirty = result.is_err();
            result.map(|_| RuleOutcome {
                rule_index,
                dest_prev_last,
                written,
                closed_bucket,
            })
        })
        .collect();

    let mut outcomes = Vec::with_capacity(results.len());
    let mut first_error: Option<TsdbError> = None;

    for r in results {
        match r {
            Ok(outcome) => outcomes.push(outcome),
            Err(error) => {
                if first_error.is_none() {
                    first_error = Some(error.clone());
                }
                let msg = format!("Failed to handle compaction rule for series: {error}");
                log_warning(msg);
                COMPACTION_FAILURES.fetch_add(1, AtomicOrdering::Relaxed);
            }
        }
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
    use crate::common::Sample;

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
