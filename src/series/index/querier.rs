// Based on code from the Prometheus project
// Copyright 2017 The Prometheus Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Query bridge between command handlers and the low-level postings planner.
//!
//! This module is responsible for translating selector-oriented command workflows into
//! series IDs/keys/guards by delegating bitmap planning to [`Postings`](super::postings::Postings).
//! It owns cross-cutting concerns that sit above raw index lookups, such as ACL checks,
//! date-range filtering, and materializing time-series references from posting IDs.
//!
//! For label-centric exploration and ranking APIs (for example, fuzzy/similarity label
//! discovery), see `label_querier.rs`, which composes this module and `Postings`.

use super::{
    DEFAULT_SERIES_BATCH_SIZE, KeyList, SeriesCursor, SeriesSelection, collect_indexed_keys,
    for_each_planned_series, get_db_index, get_timeseries_index, read_planned,
};
use crate::common::Timestamp;
use crate::common::acl::KeyAccess;
use crate::common::context::{create_key_string, get_current_db};
use crate::common::hash::IntMap;
use crate::common::threads::GilSource;
use crate::common::threads::IterIntoParRayon;
use crate::error_consts;
use crate::labels::filters::SeriesSelector;
use crate::series::request_types::MetaDateRangeFilter;
use crate::series::{SeriesGuard, SeriesRef, TimeSeries, try_get_timeseries_as};
use blart::AsBytes;
use orx_parallel::Par;
use smallvec::SmallVec;
use std::collections::BTreeSet;
use std::ops::ControlFlow;
use valkey_module::{AclPermissions, Context, ValkeyError, ValkeyResult, ValkeyString};

/// Series IDs found to have no backing key during a query, accumulated under the postings
/// read lock and flushed once the guard is released. Stale IDs are rare, so the inline
/// capacity keeps the common (empty) case off the heap.
pub(super) type StaleIds = SmallVec<[SeriesRef; 8]>;

/// Series ids paired with the key each resolved to, captured under the postings read lock.
type ResolvedKeys = Vec<(SeriesRef, ValkeyString)>;

/// Turn selectors into key names, recording ids that resolve to nothing as stale.
///
/// Every query in this module is split around this call for one reason: **no series key may be
/// opened while the postings lock is held.** `RM_OpenKey` runs the server's lazy-expiry check,
/// and reaping an expired key calls straight back into this module's `unlink`/`free` callback,
/// which takes the postings *write* lock — on this same thread, on a non-reentrant `RwLock`.
/// A single `TS.MRANGE` over a series whose TTL had passed hung the server outright:
///
/// ```text
/// series_by_selectors            [postings read lock held]
///   -> get_timeseries -> RM_OpenKey -> lookupKey -> expireIfNeeded
///     -> deleteExpiredKeyAndPropagate -> dbGenericDelete
///       -> module unlink callback -> remove_series_from_index -> write_lock   [blocks forever]
/// ```
///
/// So this takes the postings read lock itself and returns owned `ValkeyString`s, with the
/// lock already released: callers open the keys afterwards. `f` runs under the read lock and
/// must not touch the keyspace or the index.
///
/// The lock is taken once to plan and then once per [`DEFAULT_SERIES_BATCH_SIZE`] ids to
/// resolve, rather than held across the whole match. Callers hold the module lock throughout,
/// so this does not shorten a main-thread stall; what it bounds is how long a thread that
/// writes the index without the module lock (the stale-id sweep, the posting-list
/// optimizer) waits behind a broad selector. Planning copies the matched bitmap so the
/// lock can be released between batches.
fn resolve_series_keys<F, T>(
    ctx: &Context,
    selectors: &[SeriesSelector],
    stale: &mut StaleIds,
    mut f: F,
) -> ValkeyResult<Vec<T>>
where
    F: FnMut(SeriesRef, ValkeyString) -> T,
{
    let index = get_db_index(get_current_db(ctx));

    let series_refs = {
        let postings = index.get_postings();
        postings.postings_for_selectors(selectors)?.into_owned()
    };

    let mut resolved = Vec::with_capacity(series_refs.cardinality() as usize);

    let mut cursor = series_refs.cursor();
    // 4 KiB on the stack: one batch of ids, reused for every batch.
    let mut batch = [0; DEFAULT_SERIES_BATCH_SIZE.get()];
    loop {
        let count = cursor.read_many(&mut batch);
        if count == 0 {
            break;
        }
        let postings = index.get_postings();
        for &id in &batch[..count] {
            match postings.get_key_by_id(id) {
                Some(key) => resolved.push(f(id, create_key_string(ctx, key.as_bytes()))),
                None => stale.push(id),
            }
        }
    }

    Ok(resolved)
}

/// Record the ids a query found with no backing key. Must run with the postings lock released:
/// it takes the write lock.
fn mark_stale(ctx: &Context, stale: &StaleIds) {
    if !stale.is_empty() {
        get_db_index(get_current_db(ctx)).mark_ids_as_stale(stale);
    }
}

pub fn series_by_selectors<'a>(
    ctx: &'a Context,
    selectors: &[SeriesSelector],
    range: Option<MetaDateRangeFilter>,
) -> ValkeyResult<Vec<(SeriesGuard<'a>, ValkeyString)>> {
    if selectors.is_empty() {
        return Ok(Vec::new());
    }

    let mut stale = StaleIds::new();
    let resolved = resolve_series_keys(ctx, selectors, &mut stale, |id, key| (id, key))?;

    let result = collect_series_from_postings(ctx, resolved, range, &mut stale);
    mark_stale(ctx, &stale);
    result
}

/// Returns the distinct label names (when `label` is `None`) or the distinct values of
/// `label` across the series matching `selectors` — or across every indexed series when
/// `selectors` is empty. Backs `TS.QUERYLABELS`.
///
/// Unlike `TS.QUERYINDEX` (which reveals every match regardless of read access) and
/// unlike the coarse all-or-nothing gate the label-search commands apply, this applies
/// per-series `ACCESS` checks and *silently omits* series the caller may not read, so
/// names/values belonging only to unreadable series never appear in the result.
///
/// The series are read through the batched reader ([`for_each_planned_series`], with the
/// plan set to skip unreadable series): only the
/// names or values not yet seen are copied out under the module lock. On a fan-out share
/// the lock is given back between batches; a caller on the main thread already holds it and
/// reads in one batch. Must not be called while holding the lock through a [`GilSource`]
/// that releases it.
pub fn query_labels_distinct<S: GilSource + ?Sized>(
    src: &S,
    selectors: &[SeriesSelector],
    label: Option<&str>,
) -> ValkeyResult<BTreeSet<String>> {
    query_labels_distinct_planned(src, None, selectors, label)
}

/// The series `TS.QUERYLABELS` reads for `selectors`: every indexed series when there are
/// none. Plan it with no date range.
pub fn query_labels_selection(selectors: &[SeriesSelector]) -> SeriesSelection<'_> {
    if selectors.is_empty() {
        SeriesSelection::All
    } else {
        SeriesSelection::Matching(selectors)
    }
}

/// [`query_labels_distinct`] over `plan` when the caller planned already, from
/// [`query_labels_selection`] of `selectors`, which plan it again after a `SWAPDB`
/// ([`read_planned`]). Unreadable series are omitted whatever the cursor was planned with.
pub fn query_labels_distinct_planned<S: GilSource + ?Sized>(
    src: &S,
    plan: Option<SeriesCursor>,
    selectors: &[SeriesSelector],
    label: Option<&str>,
) -> ValkeyResult<BTreeSet<String>> {
    let replan = || SeriesCursor::plan(src.db(), query_labels_selection(selectors), None);
    read_planned(plan, replan, |cursor| {
        let mut result: BTreeSet<String> = BTreeSet::new();
        // TS.QUERYLABELS contract: series the caller may not read are skipped, not an error
        // on the first one (that is the coarse gate the label-search commands use, and it is
        // deliberately not applied here).
        for_each_planned_series(
            src,
            cursor.skip_unreadable(),
            DEFAULT_SERIES_BATCH_SIZE,
            |_ctx, ts, _key| {
                // Looked up before inserting: `insert(x.to_string())` would allocate for
                // every label, seen or not.
                match label {
                    None => {
                        for lbl in ts.labels.iter() {
                            if !result.contains(lbl.name) {
                                result.insert(lbl.name.to_string());
                            }
                        }
                    }
                    Some(name) => {
                        if let Some(lbl) = ts.get_label(name)
                            && !result.contains(lbl.value)
                        {
                            result.insert(lbl.value.to_string());
                        }
                    }
                }
                Ok(ControlFlow::Continue(()))
            },
        )?;
        Ok(result)
    })
}

/// Matching key names, copied out so they can outlive each batch's lock.
/// Without a range this reads only the index and reveals matches regardless of key ACLs;
/// with a range it opens each series and fails on the first unreadable match.
/// A fan-out share releases the module lock between batches; a main-thread caller keeps it.
/// Must not be called while holding the lock through a releasing [`GilSource`].
pub fn series_keys_by_selectors<S: GilSource + ?Sized>(
    src: &S,
    selectors: &[SeriesSelector],
    range: Option<MetaDateRangeFilter>,
) -> ValkeyResult<KeyList> {
    series_keys_by_selectors_planned(src, None, selectors, range)
}

/// [`series_keys_by_selectors`] over `plan` when the caller planned already
/// ([`plan_if_cheap`](super::plan_if_cheap)) from `selectors` and `range`, which plan it again
/// after a `SWAPDB` ([`read_planned`]). A plan with a date range opens and checks every key as
/// [`for_each_series`](super::for_each_series) does; otherwise the keys are what the index
/// lists.
pub fn series_keys_by_selectors_planned<S: GilSource + ?Sized>(
    src: &S,
    plan: Option<SeriesCursor>,
    selectors: &[SeriesSelector],
    range: Option<MetaDateRangeFilter>,
) -> ValkeyResult<KeyList> {
    let replan = || SeriesCursor::plan(src.db(), SeriesSelection::Matching(selectors), range);
    read_planned(plan, replan, |cursor| {
        if !cursor.has_date_range() {
            return collect_indexed_keys(src, cursor, DEFAULT_SERIES_BATCH_SIZE);
        }
        let mut keys = KeyList::new();
        for_each_planned_series(
            src,
            cursor,
            DEFAULT_SERIES_BATCH_SIZE,
            |_ctx, _series, key| {
                keys.push(key.as_slice());
                Ok(ControlFlow::Continue(()))
            },
        )?;
        Ok(keys)
    })
}

/// Cardinality-only counterpart of [`series_by_selectors`].
///
/// Applies the same posting lookup, ACL (`ACCESS`) checks, and date-range predicate through
/// the batched reader. A background caller gives the module lock back between batches and
/// retains only the running count; a main-thread caller keeps its single lock hold.
pub fn count_series_by_selectors<S: GilSource + ?Sized>(
    src: &S,
    selectors: &[SeriesSelector],
    range: Option<MetaDateRangeFilter>,
) -> ValkeyResult<usize> {
    count_series_by_selectors_planned(src, None, selectors, range)
}

/// [`count_series_by_selectors`] over `plan` when the caller planned already, to size the read
/// for scheduling, from `selectors` and `range`, which plan it again after a `SWAPDB`
/// ([`read_planned`]).
pub fn count_series_by_selectors_planned<S: GilSource + ?Sized>(
    src: &S,
    plan: Option<SeriesCursor>,
    selectors: &[SeriesSelector],
    range: Option<MetaDateRangeFilter>,
) -> ValkeyResult<usize> {
    let replan = || SeriesCursor::plan(src.db(), SeriesSelection::Matching(selectors), range);
    read_planned(plan, replan, |cursor| {
        let mut count = 0;
        for_each_planned_series(
            src,
            cursor,
            DEFAULT_SERIES_BATCH_SIZE,
            |_ctx, _series, _key| {
                count += 1;
                Ok(ControlFlow::Continue(()))
            },
        )?;
        Ok(count)
    })
}

fn collect_series_from_postings<'a>(
    ctx: &'a Context,
    resolved: ResolvedKeys,
    date_range: Option<MetaDateRangeFilter>,
    stale: &mut StaleIds,
) -> ValkeyResult<Vec<(SeriesGuard<'a>, ValkeyString)>> {
    let result = get_multi_series_by_id(ctx, resolved, stale)?;

    if result.is_empty() {
        return Ok(result);
    }

    // If no date range filter, return early
    let Some(date_range) = date_range else {
        return Ok(result);
    };

    filter_series_by_date_range(result, &date_range)
}

fn get_multi_series_by_id<'a>(
    ctx: &'a Context,
    resolved: ResolvedKeys,
    stale: &mut StaleIds,
) -> ValkeyResult<Vec<(SeriesGuard<'a>, ValkeyString)>> {
    let mut result = Vec::with_capacity(resolved.len());
    let access = KeyAccess::new(ctx, AclPermissions::ACCESS);
    for (id, k) in resolved {
        if let Some(guard) = open_resolved_series(ctx, &access, id, &k, stale)? {
            result.push((guard, k));
        }
    }
    Ok(result)
}

/// Opens `key`, the key `id` resolved to, as the series `id` names. `None` when the key no
/// longer holds a series, which records `id` as stale, or when it holds a different series,
/// which an index out of step with the keyspace can map it to.
///
/// A key holding another id is skipped, not marked stale: the id may live on under another
/// key, and marking it would drop a live series from the index. The batched reader
/// (`batched_querier::visit_batch`) applies the same rule, so batched and unbatched reads over
/// one selector agree. It also keeps one series from being returned under two keys, which the
/// id-keyed date-range filter below cannot tell apart.
fn open_resolved_series<'a>(
    ctx: &'a Context,
    access: &KeyAccess,
    id: SeriesRef,
    key: &ValkeyString,
    stale: &mut StaleIds,
) -> ValkeyResult<Option<SeriesGuard<'a>>> {
    match try_get_timeseries_as(ctx, key, access)? {
        Some(guard) if guard.id == id => Ok(Some(guard)),
        Some(_) => Ok(None),
        None => {
            stale.push(id);
            Ok(None)
        }
    }
}

#[inline(always)]
pub(super) fn matches_date_range(
    series: &TimeSeries,
    start: Timestamp,
    end: Timestamp,
    exclude: bool,
) -> bool {
    let in_range = series.has_samples_in_range(start, end);
    in_range != exclude
}

fn filter_series_by_date_range<'a>(
    mut series: Vec<(SeriesGuard<'a>, ValkeyString)>,
    date_range: &MetaDateRangeFilter,
) -> ValkeyResult<Vec<(SeriesGuard<'a>, ValkeyString)>> {
    let (start, end) = date_range.range();
    let exclude = date_range.is_exclude();

    if series.len() == 1 {
        // SAFETY: we have already checked above that we have at least one element.
        let ts = unsafe { series.get_unchecked(0).0.as_ref() };
        return if matches_date_range(ts, start, end, exclude) {
            Ok(series)
        } else {
            Ok(Vec::new())
        };
    }

    // Parallel filter for multiple series. Note that we don't collect the guards directly
    // since they hold a reference to the Context, which is not `Send`/`Sync` - hence the
    // need to collect IDs first and then reconstruct the guards from the original vector.
    // NOTE: we should evaluate the possible implications for a large number of selected series
    // (e.g., thousands) - in that case, we might want to consider batching access to the
    // GIL while checking below.
    let matching_ids: Vec<u64> = series
        .iter()
        .map(|guard| guard.0.as_ref())
        .iter_into_par_rayon()
        .filter_map(|ts| {
            if matches_date_range(ts, start, end, exclude) {
                Some(ts.id)
            } else {
                None
            }
        })
        .collect();

    match matching_ids.len() {
        0 => Ok(Vec::new()),                  // none match
        n if n == series.len() => Ok(series), // all match
        n if n < 32 => {
            series.retain(|(guard, _)| matching_ids.contains(&guard.id));
            Ok(series)
        }
        _ => {
            let mut guard_map: IntMap<u64, (SeriesGuard, ValkeyString)> = series
                .into_iter()
                .map(|(guard, key)| (guard.id, (guard, key)))
                .collect();

            Ok(matching_ids
                .into_iter()
                .filter_map(|id| guard_map.remove(&id))
                .collect())
        }
    }
}

pub fn count_matched_series<S: GilSource + ?Sized>(
    src: &S,
    date_range: Option<MetaDateRangeFilter>,
    matchers: &[SeriesSelector],
) -> ValkeyResult<usize> {
    let count = match (date_range, matchers.is_empty()) {
        (None, true) => {
            let ctx = src.lock()?;
            // check to see if the user can read all keys, otherwise error
            // a bare TS.CARD is a request for the cardinality of the entire index. Reuses a
            // fan-out request's already-resolved identity, when there is one, instead of
            // resolving the ACL user by name here.
            let access = KeyAccess::new(&ctx, AclPermissions::ACCESS);
            if !access.is_unrestricted() {
                return Err(ValkeyError::Str(
                    error_consts::ALL_KEYS_READ_PERMISSION_ERROR,
                ));
            }
            let index = get_timeseries_index(&ctx);
            index.count()
        }
        (None, false) => {
            // if we don't have a date range, we can simply count postings...
            let ctx = src.lock()?;
            let index = get_timeseries_index(&ctx);
            index.get_cardinality_by_selectors(matchers)?
        }
        (Some(range), false) => count_series_by_selectors(src, matchers, Some(range))?,
        _ => {
            // if we don't have a date range, we need at least one matcher, otherwise we
            // end up scanning the entire index
            return Err(ValkeyError::Str(
                "TSDB: TS.CARD requires at least one matcher or a date range",
            ));
        }
    };
    Ok(count)
}
