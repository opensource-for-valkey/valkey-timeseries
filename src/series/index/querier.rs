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
//! series IDs/keys/guards by delegating bitmap planning to [`Postings`].
//! It owns cross-cutting concerns that sit above raw index lookups, such as ACL checks,
//! date-range filtering, and materializing time-series references from posting IDs.
//!
//! For label-centric exploration and ranking APIs (for example, fuzzy/similarity label
//! discovery), see `label_querier.rs`, which composes this module and `Postings`.

use super::postings::{EMPTY_BITMAP, KeyType, Postings};
use super::{PostingsBitmap, get_db_index, get_timeseries_index};
use crate::common::Timestamp;
use crate::common::context::{get_acl_user, get_current_db};
use crate::common::hash::IntMap;
use crate::error_consts;
use crate::labels::filters::SeriesSelector;
use crate::series::acl::{check_key_read_permission, has_all_keys_permissions};
use crate::series::request_types::MetaDateRangeFilter;
use crate::series::{SeriesGuard, SeriesRef, TimeSeries, get_timeseries};
use blart::AsBytes;
use orx_parallel::{IterIntoParIter, ParIter};
use smallvec::SmallVec;
use std::borrow::Cow;
use std::collections::BTreeSet;
use valkey_module::{AclPermissions, Context, ValkeyError, ValkeyResult, ValkeyString};

/// Series IDs found to have no backing key during a query, accumulated under the postings
/// read lock and flushed once the guard is released. Stale IDs are rare, so the inline
/// capacity keeps the common (empty) case off the heap.
type StaleIds = SmallVec<[SeriesRef; 8]>;

/// Series ids paired with the key each resolved to, captured under the postings read lock.
type ResolvedKeys = Vec<(SeriesRef, ValkeyString)>;

/// Turn posting ids into key names while the caller holds the postings read guard, recording
/// ids that resolve to nothing as stale.
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
/// Resolving to owned `ValkeyString`s first lets the guard drop before any key is touched.
fn resolve_series_keys(
    ctx: &Context,
    postings: &Postings,
    ids: impl Iterator<Item = SeriesRef>,
    stale: &mut StaleIds,
) -> ResolvedKeys {
    let capacity_estimate = ids.size_hint().1.unwrap_or(8);
    let mut resolved = Vec::with_capacity(capacity_estimate);
    for id in ids {
        match postings.get_key_by_id(id) {
            Some(key) => resolved.push((id, ctx.create_string(key.as_bytes()))),
            None => stale.push(id),
        }
    }
    resolved
}

pub fn series_by_selectors<'a>(
    ctx: &'a Context,
    selectors: &[SeriesSelector],
    range: Option<MetaDateRangeFilter>,
) -> ValkeyResult<Vec<(SeriesGuard<'a>, ValkeyString)>> {
    if selectors.is_empty() {
        return Ok(Vec::new());
    }

    let db = get_current_db(ctx);
    let index = get_db_index(db);

    let mut stale = StaleIds::new();
    // The read guard is confined to this block: neither opening the keys below nor recording
    // the stale ids afterwards may happen while it is held. See [`resolve_series_keys`].
    let resolved = {
        let postings = index.get_postings();
        let series_refs = postings.postings_for_selectors(selectors)?;
        resolve_series_keys(ctx, &postings, series_refs.iter(), &mut stale)
    };

    let result = collect_series_from_postings(ctx, resolved, range, &mut stale);
    index.mark_ids_as_stale(&stale);
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
pub fn query_labels_distinct(
    ctx: &Context,
    selectors: &[SeriesSelector],
    label: Option<&str>,
) -> ValkeyResult<BTreeSet<String>> {
    let db = get_current_db(ctx);
    let index = get_db_index(db);

    let mut stale = StaleIds::new();
    // Resolve under the guard, open afterwards. See [`resolve_series_keys`].
    let resolved = {
        let postings = index.get_postings();
        if selectors.is_empty() {
            // Iterate the bitmap in place: collecting it first would allocate a `Vec` holding
            // every series id in the database before a single key is resolved.
            resolve_series_keys(ctx, &postings, postings.all_postings.iter(), &mut stale)
        } else {
            let refs = postings.postings_for_selectors(selectors)?;
            resolve_series_keys(ctx, &postings, refs.iter(), &mut stale)
        }
    };

    let mut result: BTreeSet<String> = BTreeSet::new();
    for (id, k) in resolved {
        // TS.QUERYLABELS contract: silently omit series the caller may not read rather
        // than erroring on the first unreadable match (that is the coarse gate the
        // label-search commands use, and it is deliberately not applied here).
        if !check_key_read_permission(ctx, &k) {
            continue;
        }
        // No `ACCESS` permission is passed here: the read check above already ran, and
        // passing it would turn an unreadable key into a hard error instead of a skip.
        let Some(guard) = get_timeseries(ctx, &k, None, false)? else {
            stale.push(id);
            continue;
        };
        let ts = guard.as_ref();
        match label {
            None => {
                for lbl in ts.labels.iter() {
                    result.insert(lbl.name.to_string());
                }
            }
            Some(name) => {
                if let Some(lbl) = ts.get_label(name) {
                    result.insert(lbl.value.to_string());
                }
            }
        }
    }

    index.mark_ids_as_stale(&stale);
    Ok(result)
}

#[allow(dead_code)]
pub(super) fn series_posting_ids_by_selectors<'a>(
    ctx: &Context,
    selectors: &[SeriesSelector],
    date_range: Option<MetaDateRangeFilter>,
) -> ValkeyResult<Cow<'a, PostingsBitmap>> {
    if selectors.is_empty() {
        return Ok(Cow::Borrowed(&*EMPTY_BITMAP));
    }
    let db = get_current_db(ctx);
    let index = get_db_index(db);

    let mut stale = StaleIds::new();
    let resolved = {
        let postings = index.get_postings();
        let series_ids = postings.postings_for_selectors(selectors)?;
        if series_ids.is_empty() {
            return Ok(Cow::Borrowed(&*EMPTY_BITMAP));
        }
        if date_range.is_none() {
            return Ok(Cow::Owned(series_ids.into_owned()));
        }
        resolve_series_keys(ctx, &postings, series_ids.iter(), &mut stale)
    };

    let result = collect_series_from_postings(ctx, resolved, date_range, &mut stale);
    index.mark_ids_as_stale(&stale);

    let id_iter = result?.into_iter().map(|(guard, _)| guard.id);
    Ok(Cow::Owned(PostingsBitmap::from_iter(id_iter)))
}

pub fn series_keys_by_selectors(
    ctx: &Context,
    selectors: &[SeriesSelector],
    range: Option<MetaDateRangeFilter>,
) -> ValkeyResult<Vec<ValkeyString>> {
    if selectors.is_empty() {
        return Ok(Vec::new());
    }

    let db = get_current_db(ctx);
    let index = get_db_index(db);

    let mut stale = StaleIds::new();
    let resolved = {
        let postings = index.get_postings();
        let series_refs = postings.postings_for_selectors(selectors)?;
        resolve_series_keys(ctx, &postings, series_refs.iter(), &mut stale)
    };

    let result = collect_series_keys(ctx, resolved, range, &mut stale);
    index.mark_ids_as_stale(&stale);
    result
}

/// Cardinality-only counterpart of [`series_by_selectors`].
///
/// Applies the same posting lookup, ACL (`ACCESS`) filtering, and date-range predicate, but
/// never materializes the `(guard, key)` pairs the caller would otherwise discard. Without a
/// date range no series state is needed at all, so keys are only probed for existence.
pub fn count_series_by_selectors(
    ctx: &Context,
    selectors: &[SeriesSelector],
    range: Option<MetaDateRangeFilter>,
) -> ValkeyResult<usize> {
    if selectors.is_empty() {
        return Ok(0);
    }

    let db = get_current_db(ctx);
    let index = get_db_index(db);

    let mut stale = StaleIds::new();
    let resolved = {
        let postings = index.get_postings();
        let series_refs = postings.postings_for_selectors(selectors)?;
        resolve_series_keys(ctx, &postings, series_refs.iter(), &mut stale)
    };

    let result = count_series_from_postings(ctx, resolved, range, &mut stale);
    index.mark_ids_as_stale(&stale);
    result
}

fn count_series_from_postings(
    ctx: &Context,
    resolved: ResolvedKeys,
    date_range: Option<MetaDateRangeFilter>,
    stale: &mut StaleIds,
) -> ValkeyResult<usize> {
    // Without a date range, nothing about the series contents matters: open each key just long
    // enough to confirm it still exists and the caller may read it.
    let Some(date_range) = date_range else {
        let mut count = 0usize;
        for (id, k) in resolved {
            let perms = Some(AclPermissions::ACCESS);
            if get_timeseries(ctx, &k, perms, false)?.is_some() {
                count += 1;
            } else {
                stale.push(id);
            }
        }
        return Ok(count);
    };

    // With a date range we need the series state, so hold the guards (bare pointers, no
    // per-key `ValkeyString` retained) long enough to evaluate the predicate.
    let mut guards: Vec<SeriesGuard> = Vec::with_capacity(resolved.len());
    for (id, k) in resolved {
        let perms = Some(AclPermissions::ACCESS);
        if let Some(guard) = get_timeseries(ctx, &k, perms, false)? {
            guards.push(guard);
        } else {
            stale.push(id);
        }
    }

    if guards.is_empty() {
        return Ok(0);
    }

    let (start, end) = date_range.range();
    let exclude = date_range.is_exclude();

    if guards.len() == 1 {
        // SAFETY: we have already checked above that we have at least one element.
        let ts = unsafe { guards.get_unchecked(0).as_ref() };
        return Ok(matches_date_range(ts, start, end, exclude) as usize);
    }

    // Mirrors `filter_series_by_date_range`: the guards borrow the non-`Send` `Context`, so we
    // hand the parallel iterator plain `&TimeSeries` references instead.
    let count = guards
        .iter()
        .map(|guard| guard.as_ref())
        .iter_into_par()
        .filter(|ts| matches_date_range(ts, start, end, exclude))
        .count();

    Ok(count)
}

fn collect_series_keys(
    ctx: &Context,
    resolved: ResolvedKeys,
    date_range: Option<MetaDateRangeFilter>,
    stale: &mut StaleIds,
) -> ValkeyResult<Vec<ValkeyString>> {
    if let Some(date_range) = date_range {
        let series = collect_series_from_postings(ctx, resolved, Some(date_range), stale)?;
        let keys = series.into_iter().map(|g| g.1).collect();
        return Ok(keys);
    }

    // TS.QUERYINDEX is a pure index lookup: it returns every series matching the
    // filter regardless of the caller's per-key read access. Command-level ACL
    // (can the user run TS.QUERYINDEX at all) is already enforced by the server,
    // so we must NOT drop keys the caller lacks read (ACCESS) permission on here.
    // Ids that resolved to no key were already recorded by `resolve_series_keys`.
    Ok(resolved.into_iter().map(|(_, key)| key).collect())
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
    for (id, k) in resolved {
        let perms = Some(AclPermissions::ACCESS);
        if let Some(guard) = get_timeseries(ctx, &k, perms, false)? {
            result.push((guard, k));
        } else {
            stale.push(id);
        }
    }
    Ok(result)
}

#[inline(always)]
fn matches_date_range(
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
        .iter_into_par()
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

pub(super) fn get_guard_from_key<'a>(
    ctx: &'a Context,
    key: &KeyType,
) -> ValkeyResult<Option<SeriesGuard<'a>>> {
    let real_key = ctx.create_string(key.as_bytes());
    let perms = Some(AclPermissions::ACCESS);
    get_timeseries(ctx, &real_key, perms, false)
}

pub fn count_matched_series(
    ctx: &Context,
    date_range: Option<MetaDateRangeFilter>,
    matchers: &[SeriesSelector],
) -> ValkeyResult<usize> {
    let count = match (date_range, matchers.is_empty()) {
        (None, true) => {
            // check to see if the user can read all keys, otherwise error
            // a bare TS.CARD is a request for the cardinality of the entire index
            let current_user = get_acl_user(ctx);
            let can_access_all_keys =
                has_all_keys_permissions(ctx, &current_user, Some(AclPermissions::ACCESS));
            if !can_access_all_keys {
                return Err(ValkeyError::Str(
                    error_consts::ALL_KEYS_READ_PERMISSION_ERROR,
                ));
            }
            let index = get_timeseries_index(ctx);
            index.count()
        }
        (None, false) => {
            // if we don't have a date range, we can simply count postings...
            let index = get_timeseries_index(ctx);
            index.get_cardinality_by_selectors(matchers)?
        }
        (Some(range), false) => count_series_by_selectors(ctx, matchers, Some(range))?,
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
