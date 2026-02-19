use std::ops::Deref;
pub(crate) mod bulk_build;
mod index_key;
mod posting_stats;
mod postings;
mod querier;
mod timeseries_index;

use crate::common::context::get_current_db;
use croaring::Portable;
use papaya::{Guard, HashMap, LocalGuard};
use std::sync::LazyLock;
use valkey_module::{AclPermissions, Context, ValkeyResult, ValkeyString};

use crate::common::hash::BuildNoHashHasher;
use crate::common::logging::log_warning;
use crate::series::request_types::MatchFilterOptions;
use crate::series::{SeriesGuardMut, SeriesRef, TimeSeries, get_timeseries_mut};
pub use index_key::IndexKey;
pub use posting_stats::*;
pub use postings::{Postings, PostingsBitmap};
pub use querier::*;
pub use timeseries_index::*;

mod ids;
mod key_buffer;
mod label_filter;
mod label_querier;
pub(crate) mod persistence;
#[cfg(test)]
mod postings_query_tests;
pub(crate) mod server_events;
#[cfg(test)]
mod timeseries_index_tests;

pub use ids::next_timeseries_id;
pub(crate) use label_filter::*;
pub use label_querier::*;

/// Map from db to TimeseriesIndex
pub type TimeSeriesIndexMap = HashMap<i32, TimeSeriesIndex, BuildNoHashHasher<i32>>;

pub struct TimeSeriesIndexGuard<'a> {
    guard: LocalGuard<'a>,
    pub db: i32,
}

impl<'a> TimeSeriesIndexGuard<'a> {
    fn new(guard: LocalGuard<'a>, db: i32) -> Self {
        Self { guard, db }
    }
}

impl Deref for TimeSeriesIndexGuard<'_> {
    type Target = TimeSeriesIndex;

    fn deref(&self) -> &Self::Target {
        get_timeseries_index_for_db(self.db, &self.guard)
    }
}

pub(crate) static TIMESERIES_INDEX: LazyLock<TimeSeriesIndexMap> =
    LazyLock::new(TimeSeriesIndexMap::default);

pub fn get_db_index(db: i32) -> TimeSeriesIndexGuard<'static> {
    let guard = TIMESERIES_INDEX.guard();
    TimeSeriesIndexGuard::new(guard, db)
}

pub fn get_timeseries_index(ctx: &'_ Context) -> TimeSeriesIndexGuard<'_> {
    let db = get_current_db(ctx);
    get_db_index(db)
}

#[inline]
fn get_timeseries_index_for_db(db: i32, guard: &impl Guard) -> &TimeSeriesIndex {
    TIMESERIES_INDEX.get_or_insert_with(db, TimeSeriesIndex::new, guard)
}

pub fn with_db_index<F, R>(db: i32, f: F) -> R
where
    F: FnOnce(&TimeSeriesIndex) -> R,
{
    let guard = get_db_index(db);
    let res = f(&guard);
    drop(guard);
    res
}

pub fn with_timeseries_index<F, R>(ctx: &Context, f: F) -> R
where
    F: FnOnce(&TimeSeriesIndex) -> R,
{
    let db = get_current_db(ctx);
    with_db_index(db, f)
}

pub fn with_timeseries_postings<F, R>(ctx: &Context, f: F) -> R
where
    F: FnOnce(&Postings) -> R,
{
    let db = get_current_db(ctx);
    let guard = TIMESERIES_INDEX.guard();
    let index = get_timeseries_index_for_db(db, &guard);
    let mut state = ();
    let res = index.with_postings(&mut state, |postings, _| f(postings));
    drop(guard);
    res
}

pub fn with_matched_series<F, STATE>(
    ctx: &Context,
    acc: &mut STATE,
    filter: &MatchFilterOptions,
    mut f: F,
) -> ValkeyResult<()>
where
    F: FnMut(&mut STATE, &TimeSeries, ValkeyString),
{
    let matched_series = series_by_selectors(ctx, &filter.matchers, filter.date_range)?;
    for (guard, key) in matched_series.into_iter() {
        let series = guard.as_ref();
        f(acc, series, key);
    }
    Ok(())
}

pub fn get_series_by_id(
    ctx: &'_ Context,
    id: SeriesRef,
    must_exist: bool,
    permissions: Option<AclPermissions>,
) -> ValkeyResult<Option<SeriesGuardMut<'_>>> {
    let map = TIMESERIES_INDEX.pin();
    let db = get_current_db(ctx);
    let Some(index) = map.get(&db) else {
        return Ok(None);
    };
    let mut state = 0;
    index.with_postings(&mut state, |posting, _| {
        let Some(key) = posting.get_key_by_id(id) else {
            return Ok(None);
        };
        let real_key = ctx.create_string(key.as_ref());
        get_timeseries_mut(ctx, &real_key, must_exist, permissions)
    })
}

pub fn get_series_key_by_id(ctx: &Context, id: SeriesRef) -> Option<ValkeyString> {
    let db = get_current_db(ctx);
    let index_guard = get_db_index(db);
    let mut state = 0;
    index_guard.with_postings(&mut state, |posting, _| {
        let key = posting.get_key_by_id(id)?;
        Some(ctx.create_string(key.as_ref()))
    })
}

pub fn remove_series_from_index(ts: &TimeSeries) {
    let Some(db) = ts._db else {
        log_warning(format!(
            "Skipping index removal for series id {} because _db is unassigned",
            ts.id
        ));
        return;
    };
    let guard = get_db_index(db);
    guard.remove_timeseries(ts);
}

/// Index a timeseries by its key. Looks up the series from the key, assigns the current
/// database, and inserts it into the index if not already present.
pub fn index_series_by_key(ctx: &Context, key: &[u8]) {
    let db = get_current_db(ctx);
    let valkey_key = ctx.create_string(key);
    let Ok(Some(mut series)) = get_timeseries_mut(ctx, &valkey_key, false, None) else {
        return;
    };
    series._db = Some(db);
    let index = get_db_index(db);
    if !index.has_id(series.id) {
        index.index_timeseries(&series, key);
    }
}

pub fn clear_timeseries_index(ctx: &Context) {
    let db = get_current_db(ctx);
    let map = TIMESERIES_INDEX.pin();
    map.remove(&db);
}

pub fn clear_all_timeseries_indexes() {
    TIMESERIES_INDEX.pin().clear();
}

pub fn swap_timeseries_index_dbs(from_db: i32, to_db: i32) {
    let guard = TIMESERIES_INDEX.guard();

    let first = get_timeseries_index_for_db(from_db, &guard);
    let second = get_timeseries_index_for_db(to_db, &guard);
    first.swap(second)
}

pub fn mark_series_for_removal(ctx: &Context, id: SeriesRef) {
    // mark the id for removal, signal to src_series to remove it
    let index = get_timeseries_index(ctx);
    index.mark_id_as_stale(id);
}

pub(crate) fn init_croaring_allocator() {
    static INIT: std::sync::Once = std::sync::Once::new();
    INIT.call_once(|| unsafe { croaring::configure_rust_alloc() });
}

const BITMAP_SERIALIZATION_VERSION: u8 = 1;
pub fn serialize_bitmap(bitmap: &PostingsBitmap) -> Vec<u8> {
    let serialization_size = bitmap.get_serialized_size_in_bytes::<Portable>();
    let mut buffer = Vec::with_capacity(1 + serialization_size);
    buffer.push(BITMAP_SERIALIZATION_VERSION);
    let _ = bitmap.serialize_into_vec::<Portable>(&mut buffer);
    buffer
}

pub fn deserialize_bitmap(bitmap: &[u8]) -> PostingsBitmap {
    if bitmap.is_empty() {
        return PostingsBitmap::default();
    }
    let version = bitmap[0];
    if version != BITMAP_SERIALIZATION_VERSION {
        let msg = format!("Unsupported bitmap serialization version: {version}");
        log_warning(&msg);
        return PostingsBitmap::default();
    }
    PostingsBitmap::deserialize::<Portable>(&bitmap[1..])
}
