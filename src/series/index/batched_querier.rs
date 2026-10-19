//! Selector reads that give the module lock back between batches of series.
//!
//! [`series_by_selectors`](super::series_by_selectors) plans the query, resolves every key,
//! and opens every series in one lock hold, and its callers then keep the lock while they
//! work through the result. For a selector matching many series that hold stalls the main
//! thread for the whole read. The functions here split it:
//!
//! 1. **Plan** the postings with no lock at all — only the postings read lock. Planning
//!    takes no `Context`, and a regex-heavy selector is the slowest part of a small read.
//! 2. For each batch of up to `batch_size` ids: take the lock, **resolve** the batch's ids to
//!    keys (postings read lock, released before any key is opened, as in
//!    `querier::resolve_series_keys`), **open** them, hand them to the caller, and release
//!    the lock before the next batch. A read whose series vary in cost — a range read copies
//!    every chunk its window touches — also ends a batch once the series opened reach a
//!    weight ([`BatchLimit::weighed`]); the ids after that point go to the next batch.
//!
//! # What a batched read sees
//! It is not a point-in-time view of the database: writes made between batches are visible
//! to later ones, a series created after planning is not seen, and one deleted part-way
//! through is skipped. That is fine for a shard's share of a fan-out read, which has no
//! cross-shard atomicity to begin with. Do not use it where a command must be atomic, or
//! for a mutation.
//!
//! # Why ids are resolved per batch
//! Resolving every id to its key up front and opening the keys later would be cheaper, and
//! wrong: if a key is renamed in between, its old name opens to nothing, the id is taken for
//! stale, and `mark_ids_as_stale` deletes the *live* id → key mapping and drops the id from
//! `all_postings`. Resolving and opening in the same lock hold keeps the two in agreement,
//! exactly as the unbatched read does.
//!
//! # Staged for the `TS.MGET`/`TS.MRANGE` conversion
//! Two mechanisms here have no production caller yet; `docs/plans/batched-mrange-mget-plan.md`
//! adds them:
//! - **Batch weight** ([`BatchLimit::weighed`], [`DEFAULT_BATCH_COPY_BYTES`],
//!   [`for_each_series_batch_then`]): every caller today passes a series count only, so a batch
//!   never ends on weight.
//! - **Requester deadline** ([`GilSource::expired`]): nothing sets one on a `FanoutContext`
//!   yet, so a read is never abandoned between batches.

use super::postings::Postings;
use super::querier::{StaleIds, matches_date_range};
use super::{KeyList, PostingsBitmap, get_db_index};
use crate::common::acl::KeyAccess;
use crate::common::context::create_key_string;
use crate::common::threads::GilSource;
use crate::error_consts;
use crate::labels::filters::{LabelFilter, PredicateMatch, PredicateValue, SeriesSelector};
use crate::series::request_types::MetaDateRangeFilter;
use crate::series::{
    SeriesGuard, SeriesRef, TimeSeries, try_get_timeseries, try_get_timeseries_as,
};
use blart::AsBytes;
use std::num::NonZeroUsize;
use std::ops::ControlFlow;
use valkey_module::{AclPermissions, Context, ValkeyError, ValkeyResult, ValkeyString};

/// Series per lock hold when a caller has no reason to pick another size.
///
/// Each series costs an ordered-map lookup (O(log n)) to resolve its id to a key, a keyspace
/// hash lookup to open it, and an ACL check, so a batch this size holds the lock for well
/// under a millisecond of opening; what the caller does with each series under the lock comes
/// on top. Each batch pays one lock acquisition, a database select, the ACL
/// identity's resolution, and a postings read lock.
pub const DEFAULT_SERIES_BATCH_SIZE: NonZeroUsize = NonZeroUsize::new(512).unwrap();

/// Bytes of chunks a range read may copy under one lock hold, on top of its series count.
///
/// [`DEFAULT_SERIES_BATCH_SIZE`] counts series, but a range read copies every chunk its
/// window touches (`TimeSeries::range_copy_bytes`, added with the `TS.MRANGE` conversion; see
/// the module docs): a chunk or two for a short window, and
/// hundreds per series for a long window over dense series, which at 512 series would hold
/// the lock for the whole copy. 4 MiB is about a thousand full default-size chunks, and an
/// allocation and memcpy each: a fraction of a millisecond. A short-window read stays under
/// it and is batched by series alone.
pub const DEFAULT_BATCH_COPY_BYTES: usize = 4 << 20;

/// How much one batch reads under its lock hold: at most `series` series and, for a read
/// whose series vary in cost, no more once their combined `weigh` reaches `weight`. The
/// series that reaches `weight` is still read, so every batch makes progress, however heavy
/// one series is.
#[derive(Clone, Copy)]
pub struct BatchLimit<W = fn(&TimeSeries) -> usize> {
    series: usize,
    weight: usize,
    weigh: W,
}

impl BatchLimit {
    /// At most `series` series, however heavy each is.
    pub fn series(series: usize) -> Self {
        Self {
            series,
            weight: usize::MAX,
            weigh: |_| 0,
        }
    }
}

impl<W: Fn(&TimeSeries) -> usize> BatchLimit<W> {
    /// At most `series` series, and no more once their `weigh` sums to `weight`.
    pub fn weighed(series: usize, weight: usize, weigh: W) -> Self {
        Self {
            series,
            weight,
            weigh,
        }
    }
}

/// Call `f` for every series `selectors` match, giving the lock back every `batch_size`
/// series.
///
/// `f` runs under the lock and receives the batch's [`Context`], the series, and its key.
/// Neither the guard nor anything borrowed from it can outlive the call — the `for<'g>`
/// bound enforces that — so `f` must copy out what it needs (a `RangeSnapshot`, the labels,
/// the latest sample) and leave heavy work, such as decoding, for after the read. The key is
/// lent rather than given: freeing a `ValkeyString` needs the lock.
///
/// The ACL (`ACCESS`) check and the optional date-range filter are those of
/// `series_by_selectors`: the read fails on the first matched series the caller may not
/// read. Return [`ControlFlow::Break`] to stop early.
///
/// Must not be called while holding the lock through a [`GilSource`] that releases it: each
/// batch takes it afresh. See the module docs for what a batched read can and cannot see.
pub fn for_each_series<S, F>(
    src: &S,
    selectors: &[SeriesSelector],
    range: Option<MetaDateRangeFilter>,
    batch_size: NonZeroUsize,
    f: F,
) -> ValkeyResult<()>
where
    S: GilSource + ?Sized,
    F: for<'g> FnMut(&'g Context, SeriesGuard<'g>, &ValkeyString) -> ValkeyResult<ControlFlow<()>>,
{
    let cursor = SeriesCursor::plan(src.db(), SeriesSelection::Matching(selectors), range)?;
    for_each_planned_series(src, cursor, batch_size, f)
}

/// [`for_each_series`] over a read planned already ([`plan_if_cheap`]): the cursor's database,
/// date range and treatment of unreadable series are those it was planned with.
pub fn for_each_planned_series<S, F>(
    src: &S,
    cursor: SeriesCursor,
    batch_size: NonZeroUsize,
    f: F,
) -> ValkeyResult<()>
where
    S: GilSource + ?Sized,
    F: for<'g> FnMut(&'g Context, SeriesGuard<'g>, &ValkeyString) -> ValkeyResult<ControlFlow<()>>,
{
    debug_assert_eq!(
        cursor.db(),
        src.db(),
        "a plan is read in the database it was made in"
    );
    visit_each_series(src, cursor, batch_size, f)
}

/// Plan a read of `selection` now if planning is cheap, so that the caller can size the read
/// by its exact match count and hand the plan to whichever thread reads it. `None` when
/// planning could cost as much as the read itself; the read then plans when it starts.
///
/// Cheap means index lookups and bitmap operations only: every filter is an `=` or `!=` on
/// non-empty values (single or a literal list), or matches everything or nothing. A regex,
/// `contains`, or an empty value (which means "label absent") walks every value of its label,
/// and `^=` walks every value under its prefix, which for a short prefix can be most of them.
/// On a high-cardinality label, that walk is the expensive part of a small read.
/// [`SeriesSelection::All`] is a copy of the all-series bitmap, and always cheap.
pub fn plan_if_cheap(
    db: i32,
    selection: SeriesSelection<'_>,
    range: Option<MetaDateRangeFilter>,
) -> ValkeyResult<Option<SeriesCursor>> {
    let cheap = match selection {
        SeriesSelection::All => true,
        SeriesSelection::Matching(selectors) => selectors_plan_cheaply(selectors),
    };
    if !cheap {
        return Ok(None);
    }
    SeriesCursor::plan(db, selection, range).map(Some)
}

/// See [`plan_if_cheap`].
fn selectors_plan_cheaply(selectors: &[SeriesSelector]) -> bool {
    selectors.iter().all(|selector| match selector {
        SeriesSelector::And(filters) => filters.iter().all(filter_plans_cheaply),
        SeriesSelector::Or(groups) => groups
            .iter()
            .all(|filters| filters.iter().all(filter_plans_cheaply)),
    })
}

fn filter_plans_cheaply(filter: &LabelFilter) -> bool {
    match &filter.matcher {
        PredicateMatch::Equal(value) | PredicateMatch::NotEqual(value) => match value {
            PredicateValue::String(value) => !value.is_empty(),
            PredicateValue::List(values) => {
                !values.is_empty() && values.iter().all(|value| !value.is_empty())
            }
            PredicateValue::Empty => false,
        },
        PredicateMatch::MatchAll | PredicateMatch::MatchNone => true,
        PredicateMatch::RegexEqual(_)
        | PredicateMatch::RegexNotEqual(_)
        | PredicateMatch::StartsWith(_)
        | PredicateMatch::NotStartsWith(_)
        | PredicateMatch::Contains(_)
        | PredicateMatch::NotContains(_) => false,
    }
}

/// Resolve matching ids to key names without opening keys or checking per-key ACLs.
/// Backs index-only `TS.QUERYINDEX` reads, which reveal every indexed match.
///
/// "Indexed", not "series": unlike [`for_each_series`] and [`for_each_series_batch`], this
/// reports what the index lists. It opens no key, so it applies no per-key `ACCESS` check and
/// no date range. Use it only where revealing a key the caller may not read is the contract.
///
/// `cursor` comes from [`SeriesCursor::plan`] or [`plan_if_cheap`] without a date range. Each
/// batch of `batch_size` ids resolves under the module lock; ids deleted, stale or unknown
/// since planning are skipped. A main-thread caller keeps its existing module lock, but still
/// bounds each postings-lock hold by `batch_size` ids. Must not be called while holding the
/// lock through a releasing [`GilSource`].
pub fn collect_indexed_keys<S: GilSource + ?Sized>(
    src: &S,
    mut cursor: SeriesCursor,
    batch_size: NonZeroUsize,
) -> ValkeyResult<KeyList> {
    let mut keys = KeyList::new();
    keys.reserve(cursor.remaining());
    while !cursor.is_done() {
        {
            let _ctx = src.lock()?;
            cursor.next_key_batch(batch_size.get(), &mut keys);
        }
        if src.can_release() && !read_another_batch(src, &cursor)? {
            break;
        }
    }
    Ok(keys)
}

/// Which series a read visits.
#[derive(Clone, Copy)]
pub enum SeriesSelection<'a> {
    /// Those the selectors match: none when there are none.
    Matching(&'a [SeriesSelector]),
    /// Every series the database indexes.
    All,
}

/// The per-series entry points over a planned `cursor`.
fn visit_each_series<S, F>(
    src: &S,
    cursor: SeriesCursor,
    batch_size: NonZeroUsize,
    mut f: F,
) -> ValkeyResult<()>
where
    S: GilSource + ?Sized,
    F: for<'g> FnMut(&'g Context, SeriesGuard<'g>, &ValkeyString) -> ValkeyResult<ControlFlow<()>>,
{
    visit_batches(
        src,
        cursor,
        BatchLimit::series(batch_size.get()),
        |ctx, batch| {
            for (guard, key) in batch.drain(..) {
                if f(ctx, guard, &key)?.is_break() {
                    return Ok(ControlFlow::Break(()));
                }
            }
            Ok(ControlFlow::Continue(()))
        },
        Ok,
    )
}

/// [`for_each_series`] a batch at a time, for a caller that fans each batch out across a
/// pool. The batch is lent as a slice for the same reason the key is lent per series.
pub fn for_each_series_batch<S, F>(
    src: &S,
    selectors: &[SeriesSelector],
    range: Option<MetaDateRangeFilter>,
    batch_size: NonZeroUsize,
    mut f: F,
) -> ValkeyResult<()>
where
    S: GilSource + ?Sized,
    F: for<'g> FnMut(
        &'g Context,
        &[(SeriesGuard<'g>, ValkeyString)],
    ) -> ValkeyResult<ControlFlow<()>>,
{
    let cursor = SeriesCursor::plan(src.db(), SeriesSelection::Matching(selectors), range)?;
    visit_batches(
        src,
        cursor,
        BatchLimit::series(batch_size.get()),
        |ctx, batch| f(ctx, batch),
        Ok,
    )
}

/// [`for_each_series_batch`] in two halves: `copy` runs under the batch's lock and returns
/// what it copied out, and `process` gets that value once the lock is released, before the
/// next batch takes it again.
///
/// For a read whose per-series work outweighs the copy — decoding a `RangeSnapshot`,
/// evaluating it — this bounds what the read holds to one batch's worth of that work's
/// input, rather than the whole match's, and the work itself is the gap that lets the main
/// thread take the lock between batches. `T` cannot borrow the batch: it is chosen by the
/// caller, outside the `for<'g>` bound.
///
/// `limit` sizes each batch; weigh it by what `copy` copies when that varies by series.
pub fn for_each_series_batch_then<S, T, W, F, G>(
    src: &S,
    selectors: &[SeriesSelector],
    range: Option<MetaDateRangeFilter>,
    limit: BatchLimit<W>,
    mut copy: F,
    process: G,
) -> ValkeyResult<()>
where
    S: GilSource + ?Sized,
    W: Fn(&TimeSeries) -> usize,
    F: for<'g> FnMut(&'g Context, &[(SeriesGuard<'g>, ValkeyString)]) -> ValkeyResult<T>,
    G: FnMut(T) -> ValkeyResult<ControlFlow<()>>,
{
    let cursor = SeriesCursor::plan(src.db(), SeriesSelection::Matching(selectors), range)?;
    visit_batches(src, cursor, limit, |ctx, batch| copy(ctx, batch), process)
}

/// The engine of every entry point, reading what `cursor` planned. `visit` runs under each
/// batch's lock and `after` gets its result once the lock is released. `visit` gets the batch by `&mut Vec` so the
/// per-series entry point can move each guard out; this is private because that would also
/// let a caller move a `ValkeyString` out from under the lock.
fn visit_batches<S, B, W, F, G>(
    src: &S,
    mut cursor: SeriesCursor,
    limit: BatchLimit<W>,
    mut visit: F,
    mut after: G,
) -> ValkeyResult<()>
where
    S: GilSource + ?Sized,
    W: Fn(&TimeSeries) -> usize,
    F: for<'g> FnMut(&'g Context, &mut Vec<(SeriesGuard<'g>, ValkeyString)>) -> ValkeyResult<B>,
    G: FnMut(B) -> ValkeyResult<ControlFlow<()>>,
{
    let limit = if src.can_release() {
        limit
    } else {
        BatchLimit::weighed(usize::MAX, usize::MAX, limit.weigh)
    };
    while !cursor.is_done() {
        let copied = {
            let ctx = src.lock()?;
            cursor.next_batch_within(&ctx, &limit, &mut visit)?
            // The batch's keys were freed inside `next_batch`; the lock goes here.
        };
        // A batch whose every id was gone, filtered, or stale has nothing to process.
        if let Some(copied) = copied
            && after(copied)?.is_break()
        {
            break;
        }
        if !read_another_batch(src, &cursor)? {
            break;
        }
    }
    Ok(())
}

/// Between two batches, with the lock released: whether to read another. `false` once the
/// plan is done; an error when the requester has given up.
fn read_another_batch<S: GilSource + ?Sized>(src: &S, cursor: &SeriesCursor) -> ValkeyResult<bool> {
    // A read that has finished is answered, however late: only a read with batches still to
    // come is abandoned.
    if cursor.is_done() {
        return Ok(false);
    }
    // A fan-out share whose requester has given up: the rest of the read would be copied
    // and decoded under the lock for an answer nobody collects.
    if src.expired() {
        return Err(ValkeyError::Str(error_consts::FANOUT_REQUESTER_GAVE_UP));
    }
    // The main thread waiting on the lock is woken by the release above, but this thread can
    // take it straight back before the main thread runs. A hint to the scheduler, not a
    // handoff: nothing guarantees the main thread runs first.
    std::thread::yield_now();
    Ok(true)
}

/// A batched read driven by its caller a batch at a time, for interleaving several reads
/// within each lock hold — the PromQL selector executor shares one hold among the tasks it
/// has queued, so a burst of small selectors pays one lock acquisition, not one each.
///
/// Planned with no lock; each [`Self::next_batch`] then runs under a lock the caller holds.
/// The entry points above are this, driven one lock per batch.
///
/// The plan is kept as the bitmap the postings produced, not as a list of its ids: a
/// roaring bitmap of a database's series ids, which are dense, costs a fraction of the
/// 8 bytes per id a list would. Each batch reads its ids out of it from where the last one
/// stopped.
#[derive(Debug, Clone)]
pub struct SeriesCursor {
    db: i32,
    range: Option<MetaDateRangeFilter>,
    /// Every id the plan matched, read or not.
    ids: PostingsBitmap,
    /// Where the next batch starts: the smallest id it may read.
    next: SeriesRef,
    /// Ids planned and not yet read.
    remaining: usize,
    /// The current batch's ids, kept for its allocation.
    batch: Vec<SeriesRef>,
    /// Skip, rather than fail on, a series the caller may not read.
    skip_unreadable: bool,
}

impl SeriesCursor {
    /// Plan a read of `selection` in database `db`: only the postings read lock is taken.
    ///
    /// The plan is an owned copy of the matched bitmap, even for [`SeriesSelection::All`] and
    /// even when the read will be one batch: the cursor outlives the postings read lock, and
    /// each batch takes that lock again, and the write lock to record stale ids, which a
    /// borrowed bitmap would hold it against. The copy is a fraction of a byte per id.
    pub fn plan(
        db: i32,
        selection: SeriesSelection<'_>,
        range: Option<MetaDateRangeFilter>,
    ) -> ValkeyResult<Self> {
        let ids = match selection {
            SeriesSelection::Matching([]) => PostingsBitmap::new(),
            SeriesSelection::Matching(selectors) => {
                let index = get_db_index(db);
                let postings = index.get_postings();
                postings.postings_for_selectors(selectors)?.into_owned()
            }
            SeriesSelection::All => {
                let index = get_db_index(db);
                let postings = index.get_postings();
                postings.all_postings.clone()
            }
        };
        Ok(Self::planned(db, range, ids))
    }

    /// A plan of exactly `ids`, for tests that need one without a server-wide index.
    #[cfg(test)]
    pub(crate) fn from_ids(db: i32, ids: PostingsBitmap) -> Self {
        Self::planned(db, None, ids)
    }

    fn planned(db: i32, range: Option<MetaDateRangeFilter>, ids: PostingsBitmap) -> Self {
        // A plan cannot outgrow memory, so its ids fit a `usize`.
        let remaining = ids.cardinality() as usize;
        Self {
            db,
            range,
            ids,
            next: 0,
            remaining,
            batch: Vec::new(),
            skip_unreadable: false,
        }
    }

    /// Leave out the series the caller may not read, rather than failing the read on the
    /// first one.
    pub fn skip_unreadable(mut self) -> Self {
        self.skip_unreadable = true;
        self
    }

    /// The database the read runs against: the caller selects it before each batch.
    pub fn db(&self) -> i32 {
        self.db
    }

    /// Whether the plan carries a date range, which only a read that opens series applies.
    pub fn has_date_range(&self) -> bool {
        self.range.is_some()
    }

    pub fn is_done(&self) -> bool {
        self.remaining == 0
    }

    /// Abandon the rest of the read: [`Self::is_done`] from here on.
    pub fn finish(&mut self) {
        self.remaining = 0;
    }

    /// Ids planned and not yet read: an upper bound on the series still to come.
    pub fn remaining(&self) -> usize {
        self.remaining
    }

    /// Read up to `max` more of the planned series and visit those that survive, under the
    /// lock `ctx` holds — with [`Self::db`] selected and, for an ACL-checked read, the
    /// caller's identity installed. `None` when none survived. See the module docs for what
    /// is resolved, skipped and marked stale.
    pub fn next_batch<B, F>(
        &mut self,
        ctx: &Context,
        max: usize,
        visit: &mut F,
    ) -> ValkeyResult<Option<B>>
    where
        F: for<'g> FnMut(&'g Context, &mut Vec<(SeriesGuard<'g>, ValkeyString)>) -> ValkeyResult<B>,
    {
        self.next_batch_within(ctx, &BatchLimit::series(max), visit)
    }

    /// [`Self::next_batch`] sized by `limit`: when the series opened reach its weight, the
    /// batch ends there and the ids after it are left for the next one.
    pub fn next_batch_within<B, W, F>(
        &mut self,
        ctx: &Context,
        limit: &BatchLimit<W>,
        visit: &mut F,
    ) -> ValkeyResult<Option<B>>
    where
        W: Fn(&TimeSeries) -> usize,
        F: for<'g> FnMut(&'g Context, &mut Vec<(SeriesGuard<'g>, ValkeyString)>) -> ValkeyResult<B>,
    {
        // Taken in full unless the batch reports otherwise: a failed read stays consumed.
        if self.take_batch(limit.series.max(1)) == 0 {
            return Ok(None);
        }
        let BatchRead { visited, ids_taken } = visit_batch(
            ctx,
            self.db,
            &self.batch,
            self.range.as_ref(),
            self.skip_unreadable,
            limit,
            visit,
        )?;
        self.give_back(ids_taken);
        Ok(visited)
    }

    /// Append a batch of live key names to `keys`, without touching the server keyspace.
    ///
    /// The caller holds the module lock, although nothing here calls the module API: it is
    /// what keeps a command's index changes from interleaving with the copy. The postings read
    /// lock alone makes each index change atomic, but some commands change the index in two
    /// holds. `RENAME` is one: the `unlink` callback removes the old key's id from
    /// `all_postings`, and the rename event indexes it again under the new key, both inside the
    /// one command. A copy taken between the two would skip a series that exists before and
    /// after the `RENAME`. Holding the module lock rules that out, as it does for every batch
    /// of the other batched reads. The cost is one module-lock acquisition per batch: about
    /// 2,000 for a million matched keys at the default 512 ids per batch, each held only for
    /// the copy. Going GIL-free would need `RENAME`'s index change made atomic first.
    fn next_key_batch(&mut self, max: usize, keys: &mut KeyList) {
        self.take_batch(max.max(1));
        let index = get_db_index(self.db);
        let mut stale = StaleIds::new();
        {
            let postings = index.get_postings();
            for_live_keys(&postings, &self.batch, &mut stale, |_id, key| {
                keys.push(key)
            });
        }
        index.mark_ids_as_stale(&stale);
    }

    /// Read up to `max` of the ids not yet read into `self.batch`, and count them as read.
    /// Returns how many it read: none once the plan is done.
    ///
    /// A source that cannot release the lock reads everything in one batch, so this then
    /// holds every remaining id at once — as the batch holds a guard for every series anyway.
    fn take_batch(&mut self, max: usize) -> usize {
        self.batch.clear();
        self.batch.resize(self.remaining.min(max), 0);
        let taken = {
            let mut ids = self.ids.cursor();
            ids.reset_at_or_after(self.next);
            ids.read_many(&mut self.batch)
        };
        self.batch.truncate(taken);
        match self.batch.last() {
            Some(&last) => {
                self.next = last.saturating_add(1);
                self.remaining -= taken;
            }
            // Unreachable while `remaining` counts what the bitmap holds from `next` on.
            None => self.finish(),
        }
        taken
    }

    /// Hand back the ids of the batch after its first `read`, which the batch left unread
    /// when it reached its weight: the next batch starts with them.
    fn give_back(&mut self, read: usize) {
        let unread = self.batch.len().saturating_sub(read);
        if unread > 0 {
            self.next = self.batch[read];
            self.remaining += unread;
        }
    }
}

/// What one batch read.
struct BatchRead<B> {
    /// What `visit` returned: `None` when no series of the batch survived to be visited.
    visited: Option<B>,
    /// How many of the batch's ids it took, from the front: fewer than all when the series
    /// opened reached the limit's weight first. The rest are handed back to the cursor.
    ids_taken: usize,
}

/// One batch, under the lock: resolve, check access, open, filter, visit, and record the
/// stale ids.
fn visit_batch<'g, B, W, F>(
    ctx: &'g Context,
    db: i32,
    ids: &[SeriesRef],
    range: Option<&MetaDateRangeFilter>,
    skip_unreadable: bool,
    limit: &BatchLimit<W>,
    visit: &mut F,
) -> ValkeyResult<BatchRead<B>>
where
    W: Fn(&TimeSeries) -> usize,
    F: for<'x> FnMut(&'x Context, &mut Vec<(SeriesGuard<'x>, ValkeyString)>) -> ValkeyResult<B>,
{
    // Looked up per batch rather than held: the guard pins the index map's reclamation.
    let index = get_db_index(db);
    let mut stale = StaleIds::new();
    let resolved = {
        let postings = index.get_postings();
        resolve_live_series_keys(ctx, &postings, ids, &mut stale)
    };

    // Per batch: under a fan-out share, the ACL identity is installed by this lock and only
    // valid while it is held.
    let access = KeyAccess::new(ctx, AclPermissions::ACCESS);
    // Per batch, not reused across them: its guards borrow this lock hold.
    let mut batch: Vec<(SeriesGuard<'g>, ValkeyString)> = Vec::with_capacity(resolved.len());
    let mut opened = Ok(());
    let mut read = ids.len();
    let mut weight = 0usize;
    // The keys left unopened when the weight runs out are dropped with the loop, under the
    // lock. Their ids are resolved afresh by the batch that reads them; any stale id among
    // them was found stale under this lock, and is recorded below either way.
    for (id, key) in resolved {
        let series = if skip_unreadable {
            // Checked before the key is opened, as `try_get_timeseries_as` does, so an
            // unreadable series is neither opened nor taken for stale.
            if !access.allows(&key) {
                continue;
            }
            try_get_timeseries(ctx, &key, None)
        } else {
            try_get_timeseries_as(ctx, &key, &access)
        };
        match series {
            Ok(Some(guard)) => {
                // Resolved and opened in one lock hold, so the key holds this id unless the
                // index is out of step with the keyspace. Another series is not this read's
                // to report, and this id is not stale: it may live on under another key. The
                // unbatched read applies the same rule (`querier::open_resolved_series`).
                if guard.id != id {
                    continue;
                }
                if let Some(range) = range {
                    let (start, end) = range.range();
                    if !matches_date_range(&guard, start, end, range.is_exclude()) {
                        continue;
                    }
                }
                weight = weight.saturating_add((limit.weigh)(&guard));
                batch.push((guard, key));
                if weight >= limit.weight {
                    read = ids
                        .iter()
                        .position(|&i| i == id)
                        .map_or(ids.len(), |p| p + 1);
                    break;
                }
            }
            Ok(None) => stale.push(id),
            Err(err) => {
                opened = Err(err);
                break;
            }
        }
    }
    let result = match opened {
        Ok(()) if batch.is_empty() => Ok(None),
        Ok(()) => visit(ctx, &mut batch).map(Some),
        Err(err) => Err(err),
    };
    // Whatever `visit` left is dropped here, under the lock: `ValkeyString`s need it.
    drop(batch);
    index.mark_ids_as_stale(&stale);
    result.map(|visited| BatchRead {
        visited,
        ids_taken: read,
    })
}

/// `querier::resolve_series_keys` for a batch planned under an earlier lock hold.
///
/// An id no longer in `all_postings` was removed since planning — its series deleted, the
/// database flushed or swapped, or the id already marked stale — and is skipped, not marked
/// again. Only an id the index still lists with no key is stale, as in the unbatched read.
fn resolve_live_series_keys(
    ctx: &Context,
    postings: &Postings,
    ids: &[SeriesRef],
    stale: &mut StaleIds,
) -> Vec<(SeriesRef, ValkeyString)> {
    let mut resolved = Vec::with_capacity(ids.len());
    for_live_keys(postings, ids, stale, |id, key| {
        resolved.push((id, create_key_string(ctx, key)));
    });
    resolved
}

/// The server-free half of [`resolve_live_series_keys`]: `f` gets each live id's key,
/// `stale` the ids the index lists with no key.
fn for_live_keys(
    postings: &Postings,
    ids: &[SeriesRef],
    stale: &mut StaleIds,
    mut f: impl FnMut(SeriesRef, &[u8]),
) {
    for &id in ids {
        if !postings.all_postings.contains(id) {
            continue;
        }
        match postings.get_key_by_id(id) {
            Some(key) => f(id, key.as_bytes()),
            None => stale.push(id),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fanout::FanoutContext;
    use crate::labels::{Label, MetricName};
    use crate::series::TimeSeries;
    use crate::series::index::clear_timeseries_index;
    use std::time::Instant;

    fn series(id: SeriesRef, name: &str) -> TimeSeries {
        let mut series = TimeSeries::new();
        series.id = id;
        series.labels = MetricName::new(&[Label::new("__name__", name)]);
        series
    }

    /// Every id, in order, `take_batch` hands out `max` at a time.
    fn drain(cursor: &mut SeriesCursor, max: usize) -> Vec<Vec<SeriesRef>> {
        let mut batches = Vec::new();
        while cursor.take_batch(max) > 0 {
            batches.push(cursor.batch.clone());
        }
        batches
    }

    #[test]
    fn a_cursor_pages_through_its_bitmap() {
        let ids = PostingsBitmap::from_iter([3, 5, 8, 13, 21, 1 << 40]);
        let mut cursor = SeriesCursor::planned(0, None, ids);
        assert_eq!(cursor.remaining(), 6);

        let batches = drain(&mut cursor, 4);

        assert_eq!(batches, vec![vec![3, 5, 8, 13], vec![21, 1 << 40]]);
        assert!(cursor.is_done());
        assert_eq!(cursor.remaining(), 0);
    }

    #[test]
    fn an_empty_plan_is_done_at_once() {
        let mut cursor = SeriesCursor::planned(0, None, PostingsBitmap::new());
        assert!(cursor.is_done());
        assert_eq!(cursor.take_batch(4), 0);
    }

    /// A batch that reaches its weight part-way hands the rest back, and the next batch
    /// starts with them.
    #[test]
    fn ids_given_back_are_read_by_the_next_batch() {
        let ids = PostingsBitmap::from_iter([1, 2, 3, 4, 5]);
        let mut cursor = SeriesCursor::planned(0, None, ids);

        assert_eq!(cursor.take_batch(3), 3);
        cursor.give_back(1);
        assert_eq!(cursor.remaining(), 4);

        assert_eq!(drain(&mut cursor, 3), vec![vec![2, 3, 4], vec![5]]);
    }

    #[test]
    fn a_batch_read_in_full_gives_nothing_back() {
        let ids = PostingsBitmap::from_iter([1, 2, 3]);
        let mut cursor = SeriesCursor::planned(0, None, ids);

        assert_eq!(cursor.take_batch(2), 2);
        cursor.give_back(2);

        assert_eq!(drain(&mut cursor, 2), vec![vec![3]]);
    }

    #[test]
    fn finish_abandons_the_rest() {
        let ids = PostingsBitmap::from_iter([1, 2, 3]);
        let mut cursor = SeriesCursor::planned(0, None, ids);
        cursor.take_batch(1);
        cursor.finish();
        assert!(cursor.is_done());
        assert_eq!(cursor.take_batch(1), 0);
    }

    fn live_keys(postings: &Postings, ids: &[SeriesRef]) -> (Vec<(SeriesRef, Vec<u8>)>, StaleIds) {
        let mut stale = StaleIds::new();
        let mut keys = Vec::new();
        for_live_keys(postings, ids, &mut stale, |id, key| {
            keys.push((id, key.to_vec()))
        });
        (keys, stale)
    }

    #[test]
    fn live_ids_resolve_to_their_keys() {
        let mut postings = Postings::default();
        postings.index_timeseries(&series(1, "a"), b"key:a");
        postings.index_timeseries(&series(2, "b"), b"key:b");

        let (keys, stale) = live_keys(&postings, &[1, 2]);

        assert_eq!(keys, vec![(1, b"key:a".to_vec()), (2, b"key:b".to_vec())]);
        assert!(stale.is_empty());
    }

    /// A series deleted between planning and its batch is gone from the index, not dangling:
    /// marking it stale would queue a sweep for nothing.
    #[test]
    fn ids_removed_since_planning_are_skipped_not_stale() {
        let mut postings = Postings::default();
        let gone = series(1, "a");
        postings.index_timeseries(&gone, b"key:a");
        postings.index_timeseries(&series(2, "b"), b"key:b");
        postings.remove_timeseries(&gone);

        let (keys, stale) = live_keys(&postings, &[1, 2]);

        assert_eq!(keys, vec![(2, b"key:b".to_vec())]);
        assert!(stale.is_empty());
    }

    /// A RENAME between planning and the batch: the id resolves to its new key, because the
    /// batch resolves under its own lock hold rather than reusing a name resolved earlier.
    #[test]
    fn a_renamed_series_resolves_to_its_new_key() {
        let mut postings = Postings::default();
        let renamed = series(1, "a");
        postings.index_timeseries(&renamed, b"old");
        assert!(postings.remove_timeseries_for_key(&renamed, b"old"));
        postings.index_timeseries(&renamed, b"new");

        let (keys, stale) = live_keys(&postings, &[1]);

        assert_eq!(keys, vec![(1, b"new".to_vec())]);
        assert!(stale.is_empty());
    }

    /// The case the unbatched read marks stale too: the index lists the id, but no key.
    #[test]
    fn a_listed_id_without_a_key_is_stale() {
        let mut postings = Postings::default();
        postings.index_timeseries(&series(2, "b"), b"key:b");
        // Postings for a label without a key: an index out of step with the keyspace.
        postings.add_posting_for_label_value(3, "__name__", "dangling");

        let (keys, stale) = live_keys(&postings, &[2, 3]);

        assert_eq!(keys, vec![(2, b"key:b".to_vec())]);
        assert_eq!(stale.as_slice(), &[3]);
    }

    #[test]
    fn key_batches_resolve_changes_and_continue_after_an_empty_batch() {
        const DB: i32 = 7302;
        clear_timeseries_index(DB);
        let index = get_db_index(DB);
        let renamed = series(1, "cpu");
        let deleted = series(2, "cpu");
        index.index_timeseries(&renamed, b"old");
        index.index_timeseries(&deleted, b"deleted");
        index.index_timeseries(&series(4, "cpu"), b"binary:\xff\0");
        index
            .get_postings_mut()
            .add_posting_for_label_value(3, "__name__", "cpu");
        let mut cursor = SeriesCursor::plan(DB, SeriesSelection::All, None).unwrap();
        let next_batch = |cursor: &mut SeriesCursor, max| {
            let mut keys = KeyList::new();
            cursor.next_key_batch(max, &mut keys);
            keys.into_vecs()
        };

        assert!(index.remove_timeseries_for_key(&renamed, b"old"));
        index.index_timeseries(&renamed, b"new");
        assert_eq!(next_batch(&mut cursor, 1), vec![b"new".to_vec()]);

        index.remove_timeseries(&deleted);
        // Deleted and dangling ids consume their batch without ending the scan.
        assert!(next_batch(&mut cursor, 2).is_empty());
        assert!(!cursor.is_done());
        assert!(!index.get_postings().all_postings.contains(3));
        assert_eq!(next_batch(&mut cursor, 2), vec![b"binary:\xff\0".to_vec()]);
        assert!(cursor.is_done());

        drop(index);
        clear_timeseries_index(DB);
    }

    /// A cursor with `ids` planned and none of them read yet.
    fn unread(ids: &[SeriesRef]) -> SeriesCursor {
        SeriesCursor::planned(0, None, PostingsBitmap::from_iter(ids.iter().copied()))
    }

    #[test]
    fn a_read_with_batches_left_continues_while_its_requester_waits() {
        let src = FanoutContext::new(None, 0);
        assert!(read_another_batch(&src, &unread(&[1, 2])).unwrap());

        src.set_deadline(Instant::now() + std::time::Duration::from_secs(60));
        assert!(read_another_batch(&src, &unread(&[1, 2])).unwrap());
    }

    #[test]
    fn a_read_whose_requester_gave_up_stops_between_batches() {
        let src = FanoutContext::new(None, 0);
        src.set_deadline(Instant::now());

        match read_another_batch(&src, &unread(&[1, 2])) {
            Err(ValkeyError::Str(msg)) => assert_eq!(msg, error_consts::FANOUT_REQUESTER_GAVE_UP),
            other => panic!("expected the requester-gave-up error, got {other:?}"),
        }
    }

    #[test]
    fn a_finished_read_is_answered_even_past_its_deadline() {
        let src = FanoutContext::new(None, 0);
        src.set_deadline(Instant::now());
        let mut cursor = unread(&[1, 2]);
        assert_eq!(cursor.take_batch(8), 2);

        assert!(!read_another_batch(&src, &cursor).unwrap());
    }

    /// `SeriesSelection` decides what a plan holds: `All` every indexed id, `Matching` the
    /// selectors' matches, and an empty `Matching` nothing — not everything.
    #[test]
    fn a_plan_holds_what_its_selection_names() {
        // A database no other test indexes into: the index is process-global.
        const DB: i32 = 7301;
        clear_timeseries_index(DB);
        let index = get_db_index(DB);
        for (id, name) in [(1, "cpu"), (2, "cpu"), (3, "mem")] {
            index.index_timeseries(&series(id, name), format!("k{id}").as_bytes());
        }
        let planned = |selection| {
            let mut cursor = SeriesCursor::plan(DB, selection, None).unwrap();
            drain(&mut cursor, 8).concat()
        };
        let cpu = [SeriesSelector::parse(r#"__name__="cpu""#).unwrap()];

        assert_eq!(planned(SeriesSelection::All), vec![1, 2, 3]);
        assert_eq!(planned(SeriesSelection::Matching(&cpu)), vec![1, 2]);
        assert_eq!(
            planned(SeriesSelection::Matching(&[])),
            Vec::<SeriesRef>::new()
        );

        drop(index);
        clear_timeseries_index(DB);
    }

    fn selector(text: &str) -> SeriesSelector {
        SeriesSelector::parse(text).unwrap()
    }

    fn with_filter(op: crate::labels::filters::MatchOp, value: &str) -> SeriesSelector {
        SeriesSelector::with_filters(vec![LabelFilter::create(op, "service", value).unwrap()])
    }

    /// Index lookups plan cheaply; anything that walks a label's values does not.
    #[test]
    fn only_lookup_filters_plan_cheaply() {
        use crate::labels::filters::MatchOp;

        for cheap in [
            vec![selector("service=api")],
            vec![selector("service=(api,web)")],
            vec![selector("service=api"), selector("region!=eu")],
            vec![selector(r#"cpu{service="api"}"#)],
            vec![],
        ] {
            assert!(selectors_plan_cheaply(&cheap), "{cheap:?}");
        }
        for costly in [
            vec![selector(r#"service=~"a.*""#)],
            vec![selector("service=api"), selector(r#"region!~"e.*""#)],
            vec![with_filter(MatchOp::StartsWith, "a")],
            vec![with_filter(MatchOp::Contains, "a")],
            // An empty value means "label absent": every value of the label is read.
            vec![with_filter(MatchOp::Equal, "")],
            vec![selector("service=api"), with_filter(MatchOp::NotEqual, "")],
        ] {
            assert!(!selectors_plan_cheaply(&costly), "{costly:?}");
        }
    }
}
