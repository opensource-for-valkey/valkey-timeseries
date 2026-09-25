//! Links between the two ends of a compaction rule.
//!
//! A source series names each destination in its [`CompactionRule`](super::CompactionRule), and a
//! destination names its source. Both ends are linked by **key name**: resolving a link opens
//! the key directly, so it neither depends on the secondary index being populated (it is not,
//! for keys of a slot still being imported) nor on series ids being unique (an imported series
//! whose id collides with a local one is given a fresh id when indexed).
//!
//! Links are validated at the point of use by checking the back-link: a rule `src -> dst` holds
//! only while `dst` names `src` as its source. A key that was deleted, re-created, overwritten
//! by `RENAME` or restored from someone else's dump therefore never receives compaction output
//! meant for another series.
use crate::common::rdb::{load_optional_marker, rdb_save_optional_marker};
use get_size2::GetSize;
use valkey_module::{Context, ValkeyResult, ValkeyString, raw};

/// The key name of the series at the other end of a compaction rule.
#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub struct SeriesLink(Box<[u8]>);

impl SeriesLink {
    pub fn from_key(key: &[u8]) -> Self {
        SeriesLink(key.into())
    }

    pub fn key(&self) -> &[u8] {
        &self.0
    }

    /// Whether this link names the series stored under `key`.
    pub fn points_to(&self, key: &[u8]) -> bool {
        self.key() == key
    }

    pub fn to_key_string(&self, ctx: &Context) -> ValkeyString {
        ValkeyString::create_from_slice(ctx.ctx, &self.0)
    }

    pub(crate) fn rdb_save(&self, rdb: *mut raw::RedisModuleIO) {
        raw::save_slice(rdb, &self.0);
    }

    pub(crate) fn rdb_load(rdb: *mut raw::RedisModuleIO) -> ValkeyResult<SeriesLink> {
        let key = raw::load_string_buffer(rdb)?;
        Ok(SeriesLink(key.as_ref().into()))
    }

    pub(crate) fn rdb_save_optional(link: Option<&SeriesLink>, rdb: *mut raw::RedisModuleIO) {
        rdb_save_optional_marker(rdb, link.is_some());
        if let Some(link) = link {
            link.rdb_save(rdb);
        }
    }

    pub(crate) fn rdb_load_optional(
        rdb: *mut raw::RedisModuleIO,
    ) -> ValkeyResult<Option<SeriesLink>> {
        if load_optional_marker(rdb)? {
            Self::rdb_load(rdb).map(Some)
        } else {
            Ok(None)
        }
    }
}

impl GetSize for SeriesLink {
    fn get_heap_size(&self) -> usize {
        self.0.len()
    }
}

impl From<&str> for SeriesLink {
    fn from(s: &str) -> Self {
        SeriesLink::from_key(s.as_bytes())
    }
}

impl From<ValkeyString> for SeriesLink {
    fn from(s: ValkeyString) -> Self {
        SeriesLink::from_key(s.as_slice())
    }
}

impl From<String> for SeriesLink {
    fn from(s: String) -> Self {
        SeriesLink::from_key(s.as_bytes())
    }
}