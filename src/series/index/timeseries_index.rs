#[cfg(test)]
use std::collections::BTreeSet;
use std::hash::BuildHasher;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{RwLock, RwLockReadGuard};

use super::posting_stats::{PostingStat, PostingsStats, StatsMaxHeap};
use super::postings::{Postings, PostingsBitmap};
use crate::common::acl::KeyAccess;
use crate::common::constants::METRIC_NAME_LABEL;
use crate::common::context::create_key_string;
use crate::common::hash::DeterministicHasher;
use crate::common::sync::{read_lock, write_lock};
use crate::error_consts;
use crate::labels::SeriesLabel;
#[cfg(test)]
use crate::labels::filters::LabelFilter;
use crate::labels::filters::SeriesSelector;
use crate::series::index::IndexKey;
use crate::series::{SeriesRef, TimeSeries};
use croaring::Bitmap64;
use std::ops::{Bound, Deref, DerefMut};
use std::time::Duration;
use valkey_module::{AclPermissions, Context, ValkeyError, ValkeyResult, ValkeyString};

/// A read-only guard for accessing Postings data.
/// This provides a safe, ergonomic way to read from Postings without allowing modification.
pub struct PostingsReadGuard<'a> {
    guard: RwLockReadGuard<'a, Postings>,
}

impl<'a> PostingsReadGuard<'a> {
    fn new(guard: RwLockReadGuard<'a, Postings>) -> Self {
        Self { guard }
    }
}

impl<'a> Deref for PostingsReadGuard<'a> {
    type Target = Postings;

    fn deref(&self) -> &Self::Target {
        &self.guard
    }
}

pub struct PostingsWriteGuard<'a> {
    guard: std::sync::RwLockWriteGuard<'a, Postings>,
}

impl<'a> PostingsWriteGuard<'a> {
    fn new(guard: std::sync::RwLockWriteGuard<'a, Postings>) -> Self {
        Self { guard }
    }
}

impl<'a> Deref for PostingsWriteGuard<'a> {
    type Target = Postings;

    fn deref(&self) -> &Self::Target {
        &self.guard
    }
}

impl<'a> DerefMut for PostingsWriteGuard<'a> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.guard
    }
}

/// Source of [`TimeSeriesIndex::generation`] values: global, so an index created to replace a
/// flushed one never repeats the generation of the index it replaces.
static NEXT_GENERATION: AtomicU64 = AtomicU64::new(1);

fn next_generation() -> u64 {
    NEXT_GENERATION.fetch_add(1, Ordering::Relaxed)
}

/// The distinct label names and `name=value` pairs a statistics scan observed, as hashed
/// fingerprints. A fan-out shard ships these to the coordinator, which unions the bitmaps and
/// counts their cardinality: a term present on several shards is then counted once, without
/// shipping the terms themselves.
#[derive(Default)]
pub struct LabelFingerprints {
    pub names: Bitmap64,
    pub pairs: Bitmap64,
}

/// [`LabelFingerprints`] under construction. The hasher lives here rather than beside the scan,
/// so it exists only when fingerprints are asked for: the standalone `stats` family passes `None`
/// and hashes nothing.
#[derive(Default)]
struct TermFingerprints {
    hasher: DeterministicHasher,
    bitmaps: LabelFingerprints,
}

impl TermFingerprints {
    fn add_name(&mut self, name: &str) {
        self.bitmaps.names.add(self.hasher.hash_one(name));
    }

    fn add_pair(&mut self, key: &IndexKey) {
        self.bitmaps.pairs.add(self.hasher.hash_one(key));
    }
}

/// One label name's run of `name=value` keys in a statistics scan: label names may not contain
/// `=`, so all of a name's keys sit together in the byte-ordered label index, and its series count
/// is summed over one run.
struct LabelRun {
    name: String,
    series: u64,
}

/// How many times [`TimeSeriesIndex::read_without_swap`] runs a read that `SWAPDB` keeps
/// exchanging the database under before it gives up. One swap during a read is rare; three
/// in a row is not a workload worth more attempts.
pub(super) const SWAP_RETRY_ATTEMPTS: usize = 3;

pub struct TimeSeriesIndex {
    pub(crate) inner: RwLock<Postings>,
    /// Changes whenever this index's contents are exchanged for another database's (`SWAPDB`).
    /// See [`Self::generation`].
    generation: AtomicU64,
}

impl Default for TimeSeriesIndex {
    fn default() -> Self {
        Self::new()
    }
}

impl Clone for TimeSeriesIndex {
    fn clone(&self) -> Self {
        let inner = read_lock(&self.inner);
        let new_inner = inner.clone();
        Self::from_postings(new_inner)
    }
}
impl TimeSeriesIndex {
    pub fn new() -> Self {
        Self::from_postings(Postings::default())
    }

    pub(crate) fn from_postings(postings: Postings) -> Self {
        TimeSeriesIndex {
            inner: RwLock::new(postings),
            generation: AtomicU64::new(next_generation()),
        }
    }

    /// Identifies which database's contents this index holds. A scan that gives the postings
    /// lock back between holds compares it before and after: if it changed, a `SWAPDB` moved
    /// another database's postings in mid-scan, and the scan must be run again rather than
    /// return a mix of the two. A flushed database's index is replaced by a new one, which has
    /// a generation of its own.
    pub fn generation(&self) -> u64 {
        self.generation.load(Ordering::SeqCst)
    }

    // swap the inner value with some other value
    // this is specifically to handle the `swapdb` event callback
    pub fn swap(&self, other: &Self) {
        // Swapping an index with itself is a no-op; proceeding would deadlock on the
        // second write_lock (SWAPDB of a db with itself is rejected by the server, but
        // don't rely on that here).
        if std::ptr::eq(self, other) {
            return;
        }
        // Acquire the two write locks in address order so concurrent swaps of the same
        // pair (in opposite argument order) cannot deadlock. `Postings::swap` is
        // symmetric, so lock order doesn't affect the result.
        let (first, second) = if std::ptr::from_ref(self).addr() < std::ptr::from_ref(other).addr()
        {
            (self, other)
        } else {
            (other, self)
        };
        let mut first_inner = write_lock(&first.inner);
        let mut second_inner = write_lock(&second.inner);
        first_inner.swap(&mut second_inner);
        // Under the write locks, so a scan's next hold cannot see the swapped postings with
        // the old generation.
        first.generation.store(next_generation(), Ordering::SeqCst);
        second.generation.store(next_generation(), Ordering::SeqCst);
    }

    pub fn index_timeseries(&self, ts: &TimeSeries, key: &[u8]) {
        debug_assert!(ts.id != 0);
        let mut inner = write_lock(&self.inner);
        inner.index_timeseries(ts, key);
    }

    /// Move `series` from `old_key` to `new_key` (RENAME). The `unlink` callback has usually
    /// retired `old_key` already, in which case only the insert is left to do.
    pub fn reindex_timeseries(&self, series: &TimeSeries, old_key: &[u8], new_key: &[u8]) {
        let mut inner = write_lock(&self.inner);
        inner.remove_timeseries_for_key(series, old_key);
        inner.index_timeseries(series, new_key);
    }

    pub fn remove_timeseries(&self, series: &TimeSeries) {
        let mut inner = write_lock(&self.inner);
        inner.remove_timeseries(series);
    }

    /// See [`Postings::remove_timeseries_for_key`].
    pub fn remove_timeseries_for_key(&self, series: &TimeSeries, key: &[u8]) -> bool {
        let mut inner = write_lock(&self.inner);
        inner.remove_timeseries_for_key(series, key)
    }

    #[cfg(test)]
    pub fn has_id(&self, id: SeriesRef) -> bool {
        let inner = read_lock(&self.inner);
        inner.has_id(id)
    }

    pub fn get_postings(&'_ self) -> PostingsReadGuard<'_> {
        let guard = read_lock(&self.inner);
        PostingsReadGuard::new(guard)
    }

    pub fn get_postings_mut(&'_ self) -> PostingsWriteGuard<'_> {
        let guard = write_lock(&self.inner);
        PostingsWriteGuard::new(guard)
    }

    /// Every visible series carrying all of `labels` (it may carry others too). A series whose
    /// label set *is* `labels` is among them; telling it apart needs the candidates' full label
    /// sets, which the index does not keep.
    pub fn postings_by_labels<T: SeriesLabel>(&self, labels: &[T]) -> PostingsBitmap {
        let inner = read_lock(&self.inner);
        inner.postings_by_labels(labels)
    }

    /// The series matching `selector`. Production reads go through `label_querier` and the
    /// selector planner directly; this and the two lookups below serve tests.
    #[cfg(test)]
    pub fn postings_for_selector(&self, selector: &SeriesSelector) -> ValkeyResult<PostingsBitmap> {
        let inner = read_lock(&self.inner);
        Ok(inner.postings_for_selector(selector)?.into_owned())
    }

    #[cfg(test)]
    pub fn get_label_names(&self) -> BTreeSet<String> {
        let inner = read_lock(&self.inner);
        inner.get_label_names()
    }

    #[cfg(test)]
    pub fn get_label_values(&self, label_name: &str) -> Vec<String> {
        let inner = read_lock(&self.inner);
        inner.get_label_values(label_name)
    }

    /// Returns the series keys that match the given selectors.
    /// If `acl_permissions` is provided, it checks if the current user has the required permissions
    /// to access all the keys.
    ///
    /// ## Note
    /// If the user does not have permission to access all keys, an error is returned.
    /// Non-user clients (e.g., AOF client) bypass permission checks.
    ///
    /// Ids the postings still reference but that no longer map to a key are queued for the
    /// stale-id sweep (see `Postings::mark_ids_as_stale` in `postings/stale.rs`) and
    /// omitted from the result. That repair covers every matching id even when the call then
    /// fails the ACL check, so a caller without access still leaves the index healthier than it
    /// found it.
    pub fn keys_for_selectors(
        &self,
        ctx: &Context,
        filters: &[SeriesSelector],
        acl_permissions: Option<AclPermissions>,
    ) -> ValkeyResult<Vec<ValkeyString>> {
        let mut keys: Vec<ValkeyString> = Vec::new();
        let mut dangling_ids: Vec<SeriesRef> = Vec::new();
        let mut acl_denied = false;

        // The requested permission decides which "all keys" error a denied caller sees.
        let wants_write = acl_permissions.as_ref().is_some_and(|perms| {
            perms.intersects(
                AclPermissions::INSERT | AclPermissions::UPDATE | AclPermissions::DELETE,
            )
        });
        let access = match acl_permissions {
            Some(perms) => KeyAccess::new(ctx, perms),
            None => KeyAccess::unrestricted(),
        };

        // The read guard is confined to this block. Recording the dangling ids collected below
        // needs the *write* lock, and `RwLock` is not reentrant: asking for it while this thread
        // still held the read guard deadlocked the server -- and did so on the self-healing path,
        // which only runs once the index is already inconsistent.
        {
            let postings = read_lock(&self.inner);

            let ids = postings.postings_for_selectors(filters)?;
            if ids.is_empty() {
                return Ok(Vec::new());
            }
            keys.reserve(ids.cardinality() as usize);

            for series_ref in ids.iter() {
                let Some(key) = postings.get_key_by_id(series_ref) else {
                    // The postings reference a series whose id -> key mapping is gone. Queue
                    // it for repair below; it cannot be returned to the caller either way.
                    dangling_ids.push(series_ref);
                    continue;
                };
                if acl_denied {
                    // The call fails either way; keep looking only for ids to repair.
                    continue;
                }
                let real_key = create_key_string(ctx, key.as_ref());
                if !access.allows(&real_key) {
                    acl_denied = true;
                    continue;
                }
                keys.push(real_key);
            }
        }

        if !dangling_ids.is_empty() {
            ctx.log_warning(&format!(
                "Index consistency: {} series ids have no key and were queued for removal.",
                dangling_ids.len()
            ));
            write_lock(&self.inner).mark_ids_as_stale(&dangling_ids);
        }

        if acl_denied {
            return Err(ValkeyError::Str(if wants_write {
                error_consts::ALL_KEYS_WRITE_PERMISSION_ERROR
            } else {
                error_consts::ALL_KEYS_READ_PERMISSION_ERROR
            }));
        }

        Ok(keys)
    }

    /// How many visible series match every one of `selectors`; `0` when there are none.
    pub fn get_cardinality_by_selectors(
        &self,
        selectors: &[SeriesSelector],
    ) -> ValkeyResult<usize> {
        let inner = read_lock(&self.inner);
        Ok(inner.postings_for_selectors(selectors)?.cardinality() as usize)
    }

    /// The series matching every one of `selectors` (AND-ed together), or `None` when `selectors`
    /// is empty and nothing is being restricted.
    ///
    /// For statistics, `None` means "the whole index", while
    /// `Some(empty)` means "a filter was applied and nothing matched".
    fn matching_postings(
        &self,
        selectors: &[SeriesSelector],
    ) -> ValkeyResult<Option<PostingsBitmap>> {
        if selectors.is_empty() {
            return Ok(None);
        }
        let inner = read_lock(&self.inner);
        Ok(Some(inner.postings_for_selectors(selectors)?.into_owned()))
    }

    /// Counts and distinct-label fingerprints from one scan with bounded postings-lock
    /// holds and no module lock required. Both use the same filter and observed terms.
    pub fn stats_and_label_bitmaps_by_selectors(
        &self,
        selectors: &[SeriesSelector],
        label: Option<&str>,
        limit: usize,
    ) -> ValkeyResult<(PostingsStats, LabelFingerprints)> {
        self.read_without_swap(|| {
            let matching = self.matching_postings(selectors)?;
            let mut fingerprints = TermFingerprints::default();
            let stats =
                self.collect_stats(matching.as_ref(), label, limit, Some(&mut fingerprints));
            Ok((stats, fingerprints.bitmaps))
        })
    }

    /// Run `read`, a read that gives the postings lock back between holds, again if `SWAPDB`
    /// exchanged this index's contents while it ran (see [`Self::generation`]). Such a read
    /// resumes in the other database's postings, so it would return one database's entries up
    /// to that point and the other's after it.
    ///
    /// Every label scan and statistics read that releases the postings lock goes through
    /// here, so all of them answer the same way. `read` must do all of its work again, planning
    /// included: a bitmap planned against the old database cannot restrict the new one. After
    /// [`SWAP_RETRY_ATTEMPTS`] swapped attempts the read fails with
    /// [`error_consts::DATABASE_SWAPPED_DURING_READ`] rather than return mixed data; a fan-out
    /// reports that as retryable.
    pub(crate) fn read_without_swap<T>(
        &self,
        mut read: impl FnMut() -> ValkeyResult<T>,
    ) -> ValkeyResult<T> {
        for _ in 0..SWAP_RETRY_ATTEMPTS {
            let generation = self.generation();
            let result = read()?;
            if self.generation() == generation {
                return Ok(result);
            }
        }
        Err(ValkeyError::Str(error_consts::DATABASE_SWAPPED_DURING_READ))
    }

    /// Cardinality statistics over the whole index.
    ///
    /// `label` names the label whose values are broken out in `series_count_by_focus_label_value`
    /// (the metric name when `Some("")`; the list is `None` when `label` is), and `limit` bounds
    /// the length of each top-N list.
    pub fn stats(&self, label: Option<&str>, limit: usize) -> ValkeyResult<PostingsStats> {
        self.read_without_swap(|| Ok(self.collect_stats(None, label, limit, None)))
    }

    /// [`TimeSeriesIndex::stats`], restricted to the series matching `filters` (AND-ed together).
    ///
    /// Every count reported is relative to the matching series alone — `series_count`,
    /// `label_count` and `total_label_value_pairs` included — so a label carried only by
    /// non-matching series does not appear at all. An empty `filters` slice matches everything and
    /// is equivalent to [`TimeSeriesIndex::stats`].
    #[cfg(test)]
    pub fn stats_filtered(
        &self,
        filters: &[LabelFilter],
        label: Option<&str>,
        limit: usize,
    ) -> ValkeyResult<PostingsStats> {
        if filters.is_empty() {
            return self.stats(label, limit);
        }
        self.read_without_swap(|| {
            let matching = {
                let inner = read_lock(&self.inner);
                inner.postings_for_label_filters(filters)?.into_owned()
            };
            Ok(self.collect_stats(Some(&matching), label, limit, None))
        })
    }

    /// [`TimeSeriesIndex::stats`], restricted to the series matching `selectors` (AND-ed
    /// together); an empty `selectors` slice matches everything.
    pub fn stats_by_selectors(
        &self,
        selectors: &[SeriesSelector],
        label: Option<&str>,
        limit: usize,
    ) -> ValkeyResult<PostingsStats> {
        self.read_without_swap(|| {
            let matching = self.matching_postings(selectors)?;
            Ok(self.collect_stats(matching.as_ref(), label, limit, None))
        })
    }

    /// [`TimeSeriesIndex::stats`] over a pre-resolved set of series, without the swap check, so
    /// tests can change the index between resolving the set and scanning.
    #[cfg(test)]
    fn stats_restricted(
        &self,
        matching: Option<&PostingsBitmap>,
        label: Option<&str>,
        limit: usize,
    ) -> PostingsStats {
        self.collect_stats(matching, label, limit, None)
    }

    /// The shared body of the `stats` family. When `matching` is given, each posting list is
    /// counted through its intersection with that set. Optional fingerprints are collected
    /// alongside the counts so a fan-out reply cannot describe two different sets of terms.
    ///
    /// Each list keeps `limit` entries, ranked as [`StatsMaxHeap`] does, and an entry's `String`
    /// is built only once the heap would keep it.
    fn collect_stats(
        &self,
        matching: Option<&PostingsBitmap>,
        focus_label: Option<&str>,
        limit: usize,
        mut fingerprints: Option<&mut TermFingerprints>,
    ) -> PostingsStats {
        // `LABEL ""` focuses on the metric name.
        let focus_label = focus_label.map(|label| {
            if label.is_empty() {
                METRIC_NAME_LABEL
            } else {
                label
            }
        });
        if matching.is_some_and(PostingsBitmap::is_empty) {
            // A filter matched nothing, so there are no series, no labels and no label-value pairs
            // to report. Still report the (empty) focus-label list when one was asked for.
            return PostingsStats {
                series_count_by_focus_label_value: focus_label.map(|_| Vec::new()),
                ..Default::default()
            };
        }

        let series_count = {
            let inner = read_lock(&self.inner);
            match matching {
                // `matching` may have been resolved under an earlier hold: `all_postings` holds
                // what is visible now, so this also drops a series deleted since.
                Some(matching) => inner.all_postings.and_cardinality(matching),
                None => inner.count() as u64,
            }
        };

        let mut metric_name_counts = StatsMaxHeap::new(limit);
        let mut label_name_counts = StatsMaxHeap::new(limit);
        let mut label_value_pair_counts = StatsMaxHeap::new(limit);
        // A focus on the metric name is the metric-name list itself; only another label needs its
        // own heap.
        let mut focus_label_value_counts = focus_label
            .filter(|&label| label != METRIC_NAME_LABEL)
            .map(|_| StatsMaxHeap::new(limit));
        let mut label_count = 0usize;
        let mut total_label_value_pairs = 0usize;
        let mut run: Option<LabelRun> = None;

        let mut finish_run = |run: LabelRun, label_count: &mut usize| {
            *label_count += 1;
            if label_name_counts.would_accept(run.series, &run.name) {
                label_name_counts.push(PostingStat {
                    name: run.name,
                    count: run.series,
                });
            }
        };

        const BATCH_SIZE: usize = 512;
        let mut iterator = BatchIterator::new(self, BATCH_SIZE, matching);

        while !iterator.is_complete() {
            iterator.next_batch(|key, count| {
                let Some((name, value)) = key.split() else {
                    return;
                };
                total_label_value_pairs += 1;
                if let Some(fingerprints) = fingerprints.as_deref_mut() {
                    fingerprints.add_pair(key);
                }

                match run.as_mut() {
                    Some(current) if current.name == name => {
                        current.series = current.series.saturating_add(count);
                    }
                    _ => {
                        if let Some(fingerprints) = fingerprints.as_deref_mut() {
                            fingerprints.add_name(name);
                        }
                        let next = LabelRun {
                            name: name.to_owned(),
                            series: count,
                        };
                        if let Some(finished) = run.replace(next) {
                            finish_run(finished, &mut label_count);
                        }
                    }
                }

                // `IndexKey` holds `name=value` already.
                let pair = key.as_str();
                if label_value_pair_counts.would_accept(count, pair) {
                    label_value_pair_counts.push(PostingStat {
                        name: pair.to_owned(),
                        count,
                    });
                }
                if name == METRIC_NAME_LABEL && metric_name_counts.would_accept(count, value) {
                    metric_name_counts.push(PostingStat {
                        name: value.to_owned(),
                        count,
                    });
                }
                if let Some(focus) = focus_label_value_counts.as_mut()
                    && focus_label == Some(name)
                    && focus.would_accept(count, value)
                {
                    focus.push(PostingStat {
                        name: value.to_owned(),
                        count,
                    });
                }
            })
        }
        if let Some(finished) = run {
            finish_run(finished, &mut label_count);
        }

        let series_count_by_metric_name = metric_name_counts.into_vec();
        let series_count_by_focus_label_value = match focus_label_value_counts {
            Some(focus) => Some(focus.into_vec()),
            None => focus_label.map(|_| series_count_by_metric_name.clone()),
        };

        PostingsStats {
            series_count_by_metric_name,
            series_count_by_label_name: label_name_counts.into_vec(),
            series_count_by_label_value_pairs: label_value_pair_counts.into_vec(),
            series_count_by_focus_label_value,
            total_label_value_pairs,
            label_count,
            series_count,
        }
    }

    pub fn count(&self) -> usize {
        let inner = read_lock(&self.inner);
        inner.count()
    }

    /// Distinct `name=value` keys in the label index, including keys whose only ids are stale
    /// and not yet swept.
    #[cfg(test)]
    pub fn pair_count(&self) -> usize {
        let inner = read_lock(&self.inner);
        inner.label_index.len()
    }

    /// Whether this index holds no series at all. Not `count() == 0`: an import in progress may
    /// have hidden every series here, and dropping the index would lose them for good.
    pub fn is_empty(&self) -> bool {
        read_lock(&self.inner).holds_no_series()
    }

    pub fn with_postings<F, R, STATE>(&self, state: &mut STATE, f: F) -> R
    where
        F: FnOnce(&Postings, &mut STATE) -> R,
    {
        let inner = read_lock(&self.inner);
        f(&inner, state)
    }

    pub fn with_postings_mut<F, R, STATE>(&self, state: &mut STATE, f: F) -> R
    where
        F: FnOnce(&mut Postings, &mut STATE) -> R,
    {
        let mut inner = write_lock(&self.inner);
        f(&mut inner, state)
    }

    pub fn mark_id_as_stale(&self, id: SeriesRef) {
        let mut inner = write_lock(&self.inner);
        inner.mark_id_as_stale(id);
    }

    /// Marks `ids` stale and returns how many of them were indexed until now.
    pub fn mark_ids_as_stale(&self, ids: &[SeriesRef]) -> usize {
        if ids.is_empty() {
            return 0;
        }
        let mut inner = write_lock(&self.inner);
        inner.mark_ids_as_stale(ids)
    }

    pub fn optimize_incremental(
        &self,
        start_prefix: Option<IndexKey>,
        count: usize,
    ) -> Option<IndexKey> {
        let mut inner = write_lock(&self.inner);
        inner.optimize_postings(start_prefix, count)
    }

    /// Optimizes every posting list in one pass, taking the write lock for at most `hold_budget`
    /// or `max_lists` lists at a time (at least one), whichever comes first, so queries and writes
    /// interleave. Returns `false` if `should_stop` ended the pass early.
    pub fn optimize_all(
        &self,
        max_lists: usize,
        hold_budget: Duration,
        should_stop: impl Fn() -> bool,
    ) -> bool {
        // A hold that may optimize no list would return the same cursor forever.
        let max_lists = max_lists.max(1);
        let mut cursor = None;
        loop {
            if should_stop() {
                return false;
            }
            cursor = write_lock(&self.inner).optimize_postings_within(
                cursor,
                max_lists,
                Some(hold_budget),
            );
            if cursor.is_none() {
                return true;
            }
            std::thread::yield_now();
        }
    }
}

/// Walks the label index in batches, one postings-lock hold per batch, reporting each key with
/// the number of visible series it holds.
struct BatchIterator<'a> {
    index: &'a TimeSeriesIndex,
    /// When set, posting lists are counted through their intersection with this set rather than
    /// in full. See [`BatchIterator::new`].
    matching: Option<PostingsBitmap>,
    cursor: Option<IndexKey>,
    batch_size: usize,
    is_finished: bool,
}

impl<'a> BatchIterator<'a> {
    /// With `matching`, each posting list is counted within that set only. It was resolved under
    /// an earlier lock hold, so each hold first narrows it, in place, to `all_postings` — the ids
    /// neither tombstoned nor hidden — as of then: an id may have been tombstoned, hidden or
    /// removed since. Not a point-in-time view, and the set only shrinks.
    fn new(
        index: &'a TimeSeriesIndex,
        batch_size: usize,
        matching: Option<&PostingsBitmap>,
    ) -> Self {
        Self {
            index,
            matching: matching.cloned(),
            cursor: None,
            batch_size: batch_size.max(1),
            is_finished: false,
        }
    }

    fn next_batch(&mut self, mut processor: impl FnMut(&IndexKey, u64)) {
        if self.is_finished {
            return;
        }

        let inner = read_lock(&self.index.inner);
        if let Some(matching) = self.matching.as_mut() {
            matching.and_inplace(&inner.all_postings);
        }

        // Resume from the cursor key using an ordered range rather than a prefix scan (a full key
        // used as a prefix only matches itself and its extensions). The cursor key itself is
        // skipped below to guarantee forward progress.
        let range: (Bound<&IndexKey>, Bound<&IndexKey>) = match self.cursor.as_ref() {
            Some(k) => (Bound::Included(k), Bound::Unbounded),
            None => (Bound::Unbounded, Bound::Unbounded),
        };

        let mut processed = 0usize;
        let mut last: Option<&IndexKey> = None;
        for (key, bitmap) in inner.label_index.range::<IndexKey, _>(range) {
            if self.cursor.as_ref() == Some(key) {
                continue;
            }
            let count = match self.matching.as_ref() {
                Some(matching) => bitmap.and_cardinality(matching),
                None => inner.visible_cardinality(bitmap),
            };
            if count > 0 {
                processor(key, count);
            }
            processed += 1;
            last = Some(key);
            if processed == self.batch_size {
                break;
            }
        }

        // A batch that stops short of `batch_size` has reached the end of the index.
        if processed < self.batch_size {
            self.is_finished = true;
            self.cursor = None;
        } else {
            self.cursor = last.cloned();
        }
    }

    fn is_complete(&self) -> bool {
        self.is_finished
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ahash::AHashMap;

    /// Series 1 and 2 share both labels, and each has a key.
    fn two_series_two_labels() -> TimeSeriesIndex {
        let index = TimeSeriesIndex::new();
        {
            let mut postings = index.get_postings_mut();
            for id in [1, 2] {
                postings.add_posting_for_label_value(id, "a", "x");
                postings.add_posting_for_label_value(id, "b", "x");
                postings
                    .id_to_key
                    .insert(id, format!("key{id}").into_bytes().into_boxed_slice());
            }
        }
        index
    }

    fn hide_first_series(index: &TimeSeriesIndex) {
        let slot = crate::fanout::calculate_hash_slot(b"key1");
        let slots = range_set_blaze::RangeSetBlaze::from_iter([slot]);
        assert_eq!(
            index.get_postings_mut().hide_indexed_keys_in_slots(&slots),
            1
        );
    }

    fn pair_count(stats: &PostingsStats, pair: &str) -> u64 {
        stats
            .series_count_by_label_value_pairs
            .iter()
            .find(|stat| stat.name == pair)
            .map_or(0, |stat| stat.count)
    }

    fn matching_all(index: &TimeSeriesIndex) -> PostingsBitmap {
        let postings = index.get_postings();
        postings.all_postings.clone()
    }

    #[test]
    fn filtered_stats_leave_out_a_series_hidden_after_the_filter_resolved() {
        let index = two_series_two_labels();
        let matching = matching_all(&index);
        hide_first_series(&index);

        let stats = index.stats_restricted(Some(&matching), Some("a"), 10);
        assert_eq!(stats.series_count, 1);
        assert_eq!(pair_count(&stats, "a=x"), 1);
    }

    #[test]
    fn filtered_stats_leave_out_a_series_marked_stale_after_the_filter_resolved() {
        let index = two_series_two_labels();
        let matching = matching_all(&index);
        index.mark_id_as_stale(1);

        let stats = index.stats_restricted(Some(&matching), Some("a"), 10);
        assert_eq!(stats.series_count, 1);
        assert_eq!(pair_count(&stats, "a=x"), 1);
    }

    #[test]
    fn filtered_stats_do_not_count_a_series_removed_after_the_filter_resolved() {
        let index = two_series_two_labels();
        let matching = matching_all(&index);
        let mut removed = TimeSeries::new();
        removed.id = 1;
        removed.labels = crate::labels::MetricName::new(&[
            crate::labels::Label::new("a", "x"),
            crate::labels::Label::new("b", "x"),
        ]);
        index.remove_timeseries(&removed);

        let stats = index.stats_restricted(Some(&matching), Some("a"), 10);
        assert_eq!(stats.series_count, 1);
    }

    #[test]
    fn a_restricted_scan_applies_visibility_at_each_lock_hold() {
        let index = two_series_two_labels();
        let matching = matching_all(&index);
        let mut iterator = BatchIterator::new(&index, 1, Some(&matching));

        let mut counts = Vec::new();
        iterator.next_batch(|_, count| counts.push(count));
        hide_first_series(&index);
        iterator.next_batch(|_, count| counts.push(count));
        assert_eq!(counts, [2, 1], "the second hold sees the series hidden");
    }

    fn index_with_posting(id: SeriesRef, label: &str, value: &str) -> TimeSeriesIndex {
        let index = TimeSeriesIndex::new();
        {
            let mut postings = index.get_postings_mut();
            postings.add_posting_for_label_value(id, label, value);
            postings
                .id_to_key
                .insert(id, format!("key{id}").into_bytes().into_boxed_slice());
        }
        index
    }

    #[test]
    fn stats_and_fingerprints_share_the_filter_and_stale_id_mask() {
        let index = index_with_posting(1, "host", "a");
        {
            let mut postings = index.get_postings_mut();
            postings.add_posting_for_label_value(1, "scope", "yes");
            postings.add_posting_for_label_value(2, "host", "b");
            postings
                .id_to_key
                .insert(2, b"key2".to_vec().into_boxed_slice());
            postings.add_posting_for_label_value(2, "scope", "no");
            postings.add_posting_for_label_value(2, "hidden", "private");
            postings.add_posting_for_label_value(3, "host", "stale");
            postings.mark_id_as_stale(3);
        }
        for (selector, expected) in [
            (None, (2, 3, 5)),
            (Some("scope=yes"), (1, 2, 2)),
            (Some("scope=missing"), (0, 0, 0)),
        ] {
            let selectors: Vec<_> = selector
                .map(|s| SeriesSelector::parse(s).unwrap())
                .into_iter()
                .collect();
            let (stats, fingerprints) = index
                .stats_and_label_bitmaps_by_selectors(&selectors, Some("host"), 10)
                .unwrap();
            assert_eq!(
                (
                    stats.series_count,
                    stats.label_count,
                    stats.total_label_value_pairs
                ),
                expected
            );
            assert_eq!(fingerprints.names.cardinality(), stats.label_count as u64);
            assert_eq!(
                fingerprints.pairs.cardinality(),
                stats.total_label_value_pairs as u64
            );
        }
    }

    #[test]
    fn a_statistics_read_restarts_after_swapdb() {
        let index = index_with_posting(1, "host", "a");
        let other = index_with_posting(2, "host", "b");
        let mut attempts = 0;
        let stats = index
            .read_without_swap(|| {
                let stats = index.stats(Some("host"), 10)?;
                attempts += 1;
                if attempts == 1 {
                    index.swap(&other);
                }
                Ok(stats)
            })
            .unwrap();
        assert_eq!(attempts, 2);
        assert_eq!(
            stats.series_count_by_focus_label_value.unwrap()[0].name,
            "b"
        );
    }

    #[test]
    fn repeated_swapdb_fails_a_statistics_read_instead_of_returning_mixed_data() {
        let index = index_with_posting(1, "host", "a");
        let other = index_with_posting(2, "host", "b");
        let mut attempts = 0;
        let result = index.read_without_swap(|| {
            attempts += 1;
            index.swap(&other);
            Ok(())
        });
        assert_eq!(attempts, 3);
        // The fan-out classifies this text as a retryable failure, so it must not drift.
        let Err(ValkeyError::Str(message)) = result else {
            panic!("expected the swapped-database error, got {result:?}");
        };
        assert_eq!(message, error_consts::DATABASE_SWAPPED_DURING_READ);
    }

    fn names_and_counts(stats: &[PostingStat]) -> Vec<(String, u64)> {
        stats
            .iter()
            .map(|stat| (stat.name.clone(), stat.count))
            .collect()
    }

    /// Label names chosen so their `name=` key prefixes sort differently from the names
    /// themselves (`a.b=` < `a=` < `ab=` as bytes, `a` < `a.b` < `ab` as strings), and one
    /// name with more values than a scan batch holds, so its run spans several holds.
    fn label_name_fixture() -> (TimeSeriesIndex, AHashMap<String, u64>) {
        let index = TimeSeriesIndex::new();
        let mut expected: AHashMap<String, u64> = AHashMap::new();
        {
            let mut postings = index.get_postings_mut();
            for id in 1..=1200u64 {
                let mut labels = vec![
                    ("__name__".to_string(), format!("m{}", id % 7)),
                    ("wide".to_string(), format!("v{id}")),
                ];
                if id % 2 == 0 {
                    labels.push(("a".to_string(), format!("{}", id % 5)));
                }
                if id % 3 == 0 {
                    labels.push(("a.b".to_string(), format!("{}", id % 4)));
                }
                if id % 5 == 0 {
                    labels.push(("ab".to_string(), "x".to_string()));
                }
                for (name, value) in labels {
                    postings.add_posting_for_label_value(id, &name, &value);
                    *expected.entry(name).or_default() += 1;
                }
                postings
                    .id_to_key
                    .insert(id, format!("key{id}").into_bytes().into_boxed_slice());
            }
        }
        (index, expected)
    }

    #[test]
    fn label_name_counts_sum_each_run_across_batches() {
        let (index, expected) = label_name_fixture();
        let stats = index.stats(None, usize::MAX).unwrap();

        let mut model: Vec<(String, u64)> = expected.into_iter().collect();
        model.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
        assert_eq!(names_and_counts(&stats.series_count_by_label_name), model);
        assert_eq!(stats.label_count, model.len());
        assert_eq!(stats.series_count, 1200);

        // A smaller limit keeps a prefix of the full ranking.
        let top = index.stats(None, 3).unwrap();
        assert_eq!(
            names_and_counts(&top.series_count_by_label_name),
            model[..3]
        );
    }

    #[test]
    fn fingerprints_count_each_name_once_across_batches() {
        let (index, expected) = label_name_fixture();
        let (stats, fingerprints) = index
            .stats_and_label_bitmaps_by_selectors(&[], None, 10)
            .unwrap();
        assert_eq!(fingerprints.names.cardinality(), expected.len() as u64);
        assert_eq!(
            fingerprints.pairs.cardinality(),
            stats.total_label_value_pairs as u64
        );
    }

    #[test]
    fn focus_list_follows_the_label_argument() {
        let (index, _) = label_name_fixture();

        let without = index.stats(None, 5).unwrap();
        assert!(without.series_count_by_focus_label_value.is_none());

        let metric = index.stats(Some(""), 5).unwrap();
        assert_eq!(
            metric.series_count_by_focus_label_value.as_deref(),
            Some(metric.series_count_by_metric_name.as_slice())
        );
        assert_eq!(metric.series_count_by_metric_name.len(), 5);

        let ab = index.stats(Some("a.b"), 10).unwrap();
        assert_eq!(
            names_and_counts(&ab.series_count_by_focus_label_value.unwrap()),
            [
                ("0".to_string(), 100),
                ("1".to_string(), 100),
                ("2".to_string(), 100),
                ("3".to_string(), 100)
            ]
        );

        let missing = index.stats(Some("missing"), 10).unwrap();
        assert_eq!(missing.series_count_by_focus_label_value, Some(Vec::new()));

        let none_matched = index
            .stats_by_selectors(&[SeriesSelector::parse("ab=nope").unwrap()], None, 10)
            .unwrap();
        assert!(none_matched.series_count_by_focus_label_value.is_none());
        assert_eq!(none_matched.series_count, 0);
    }

    #[test]
    fn a_batch_iterator_finishes_on_a_short_batch() {
        let index = two_series_two_labels();
        let mut iterator = BatchIterator::new(&index, 3, None);
        let mut keys = Vec::new();
        iterator.next_batch(|key, _| keys.push(key.as_str().to_owned()));
        assert!(iterator.is_complete(), "two keys fit in one batch of three");
        assert_eq!(keys, ["a=x", "b=x"]);

        let mut iterator = BatchIterator::new(&index, 0, None);
        let mut batches = 0;
        while !iterator.is_complete() {
            iterator.next_batch(|_, _| {});
            batches += 1;
            assert!(batches <= 3, "a zero batch size still makes progress");
        }
    }

    #[test]
    fn optimize_all_with_a_zero_list_budget_still_finishes() {
        let index = two_series_two_labels();
        let holds = std::cell::Cell::new(0);
        let finished = index.optimize_all(0, Duration::from_secs(1), || {
            holds.set(holds.get() + 1);
            assert!(holds.get() <= 10, "the pass made no progress");
            false
        });
        assert!(finished);
    }

    #[test]
    fn test_swap_with_self_is_noop() {
        // SWAPDB with from_db == to_db resolves to the same index twice; without the
        // identity guard this deadlocks on the second write lock.
        let index = index_with_posting(1, "host", "a");
        index.swap(&index);
        assert!(
            index
                .get_postings()
                .postings_for_label_value("host", "a")
                .contains(1)
        );
    }

    #[test]
    fn test_swap_exchanges_contents_in_both_argument_orders() {
        let a = index_with_posting(1, "host", "a");
        let b = index_with_posting(2, "host", "b");

        // Exercises both lock-acquisition orders (a<b and b<a by address).
        a.swap(&b);
        assert!(
            a.get_postings()
                .postings_for_label_value("host", "b")
                .contains(2)
        );
        assert!(
            b.get_postings()
                .postings_for_label_value("host", "a")
                .contains(1)
        );

        b.swap(&a);
        assert!(
            a.get_postings()
                .postings_for_label_value("host", "a")
                .contains(1)
        );
        assert!(
            b.get_postings()
                .postings_for_label_value("host", "b")
                .contains(2)
        );
    }
}
