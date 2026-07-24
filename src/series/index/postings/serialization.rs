//! Wire format for a single [`Postings`] index.
//!
//! This module owns the byte layout of the index body and nothing else — no db numbering, no
//! payload framing, no magic or version header. Those belong to whoever is embedding the body
//! (today, the RDB aux payload in `super::super::persistence`), which lets the container evolve
//! without touching the index encoding and keeps the field-level knowledge that encoding needs
//! inside `postings`.
//!
//! Layout, in order:
//!
//! 1. **Label index**, as a group directory followed by the entries it describes. Keys are
//!    written grouped by their shared `name=` prefix, emitted once per group rather than once
//!    per entry — the on-disk analogue of the prefix sharing the ART gives us in memory. Label
//!    *value* cardinality is what grows (hosts, instances); the set of label *names* is small
//!    and schema-bound, so a name with k values costs one copy of the name instead of k.
//! 2. **Id directory**, ascending, as varint id deltas paired with key names.
//! 3. **`all_postings`**, one bitmap.
//!
//! Stale ids are not part of the format; see [`serialize`].

use super::{IndexKey, Postings, PostingsBitmap, PostingsIndex, StaleSet};
use crate::common::encoding::{
    try_read_byte_slice, try_read_uvarint, write_byte_slice, write_uvarint,
};
use crate::series::SeriesRef;
use croaring::{Bitmap64, Portable};
use std::borrow::Cow;
use std::collections::BTreeMap;

/// Decoding is best-effort: any failure is reported as a message and leaves the caller to fall
/// back to rebuilding the index from the keyspace.
pub(in crate::series::index) type DecodeResult<T> = Result<T, String>;

pub(in crate::series::index) fn write_bitmap(buf: &mut Vec<u8>, bitmap: &PostingsBitmap) {
    let size = bitmap.get_serialized_size_in_bytes::<Portable>();
    write_uvarint(buf, size as u64);
    buf.reserve(size);
    let before = buf.len();
    let _ = bitmap.serialize_into_vec::<Portable>(buf);
    debug_assert_eq!(buf.len() - before, size);
}

pub(in crate::series::index) fn read_bitmap(buf: &mut &[u8]) -> DecodeResult<PostingsBitmap> {
    let blob = try_read_byte_slice(buf).map_err(|e| e.to_string())?;
    Bitmap64::try_deserialize::<Portable>(blob)
        .ok_or_else(|| "corrupt roaring bitmap blob".to_string())
}

/// Appends the encoded index body to `buf`.
///
/// Stale ids are subtracted here rather than persisted: `mark_ids_as_stale` already cleans
/// `id_to_key` and `all_postings` at mark time, so the label bitmaps are the only structures
/// still carrying stale ids — persisting them would persist a cleanup obligation. Subtracting
/// yields exactly the state a completed GC drain would produce. A stale id whose key is still
/// in the RDB (a fork can land mid-ASM-export, before the engine's lazy delete) is resurrected
/// by the post-load count-verification scan, matching the keyspace either way. The subtraction
/// only costs anything when `stale_ids` is non-empty (rare: the cron GC drains continuously);
/// entries that become empty are dropped, so each group's count is written only once its
/// entries have been serialized.
pub(in crate::series::index) fn serialize(buf: &mut Vec<u8>, postings: &Postings) {
    // label_index in tree iteration order (sorted): cache-friendly ART rebuild on load. Sorted
    // order is also what makes the grouping a single pass — a group's entries are necessarily
    // contiguous, because every key in it shares the prefix up to and including the first `=`,
    // and no other group's prefix can fall between them (a prefix never contains `=` except as
    // its last byte, so no prefix is a prefix of another).
    let stale: &StaleSet = &postings.stale_ids;
    // Group directory: (prefix, entry count) pairs, one per distinct label name.
    let mut groups: Vec<u8> = Vec::new();
    let mut group_count: u64 = 0;
    let mut entries: Vec<u8> = Vec::new();
    let mut open_group: Option<(&[u8], u64)> = None;
    for (key, bitmap) in postings.label_index.iter() {
        if bitmap.is_empty() {
            continue;
        }

        let cleaned: Cow<PostingsBitmap> = stale.mask_cow(Cow::Borrowed(bitmap));
        if cleaned.is_empty() {
            continue;
        }
        // `as_str` strips the NUL sentinel; `IndexKey::from(&[u8])` re-appends it on load.
        let full = key.as_str().as_bytes();
        // Split *after* the first `=` so that `prefix + suffix` reproduces the key byte for
        // byte, whatever it contains — a value with an `=` in it, or a key with none at all.
        // Nothing here has to agree with `IndexKey::for_label_value` about where the name ends.
        let split_at = full
            .iter()
            .position(|b| *b == b'=')
            .map_or(full.len(), |i| i + 1);
        let (prefix, suffix) = full.split_at(split_at);

        match open_group {
            Some((open, ref mut count)) if open == prefix => *count += 1,
            _ => {
                if let Some((open, count)) = open_group {
                    write_byte_slice(&mut groups, open);
                    write_uvarint(&mut groups, count);
                    group_count += 1;
                }
                open_group = Some((prefix, 1));
            }
        }
        write_byte_slice(&mut entries, suffix);
        write_bitmap(&mut entries, &cleaned);
    }
    if let Some((open, count)) = open_group {
        write_byte_slice(&mut groups, open);
        write_uvarint(&mut groups, count);
        group_count += 1;
    }

    write_uvarint(buf, group_count);
    buf.extend_from_slice(&groups);
    buf.extend_from_slice(&entries);

    // id_to_key in ascending id order: ids share their high epoch bits and increment densely,
    // so varint deltas stay small.
    write_uvarint(buf, postings.id_to_key.len() as u64);
    let mut prev_id: SeriesRef = 0;
    for (id, key) in postings.id_to_key.iter() {
        write_uvarint(buf, id - prev_id);
        prev_id = *id;
        write_byte_slice(buf, key.as_ref());
    }

    write_bitmap(buf, &postings.all_postings);
}

/// Reads one index body from `buf`, advancing it past the bytes consumed.
pub(in crate::series::index) fn deserialize(buf: &mut &[u8]) -> DecodeResult<Postings> {
    // Group directory first (see [`serialize`]), then the entries for each group in the same
    // order. `with_capacity` is clamped: the count comes from the payload, and a corrupt one
    // must not turn into an unbounded allocation before the read that rejects it.
    let group_count = try_read_uvarint(buf).map_err(|e| e.to_string())? as usize;
    let mut groups: Vec<(&[u8], usize)> = Vec::with_capacity(group_count.min(64));
    for _ in 0..group_count {
        let prefix = try_read_byte_slice(buf).map_err(|e| e.to_string())?;
        let count = try_read_uvarint(buf).map_err(|e| e.to_string())? as usize;
        groups.push((prefix, count));
    }

    let mut label_index = PostingsIndex::new();
    let mut key_bytes: Vec<u8> = Vec::new();
    for (prefix, count) in groups {
        for _ in 0..count {
            let suffix = try_read_byte_slice(buf).map_err(|e| e.to_string())?;
            let bitmap = read_bitmap(buf)?;
            key_bytes.clear();
            key_bytes.reserve(prefix.len() + suffix.len());
            key_bytes.extend_from_slice(prefix);
            key_bytes.extend_from_slice(suffix);
            let key = IndexKey::from(key_bytes.as_slice());
            label_index
                .try_insert(key, bitmap)
                .map_err(|e| format!("label index insert failed: {e}"))?;
        }
    }

    let id_count = try_read_uvarint(buf).map_err(|e| e.to_string())? as usize;
    let mut id_to_key = BTreeMap::new();
    let mut prev_id: SeriesRef = 0;
    for _ in 0..id_count {
        let delta = try_read_uvarint(buf).map_err(|e| e.to_string())?;
        let id = prev_id
            .checked_add(delta)
            .ok_or_else(|| "series id overflow".to_string())?;
        prev_id = id;
        let key = try_read_byte_slice(buf).map_err(|e| e.to_string())?;
        id_to_key.insert(id, key.to_vec().into_boxed_slice());
    }

    let all_postings = read_bitmap(buf)?;

    Ok(Postings {
        label_index,
        id_to_key,
        // Stale ids were subtracted at save time; the loaded index starts clean.
        stale_ids: StaleSet::default(),
        all_postings,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_corrupt_bitmap_blob() {
        // A blob whose declared length is intact but whose contents are not a valid portable
        // bitmap: the leading u64 bucket count is absurd relative to the remaining bytes.
        // (Bit flips that still decode as *some* valid bitmap are the RDB CRC's job to catch.)
        let mut buf = Vec::new();
        write_byte_slice(&mut buf, &[0xFF; 9]);
        let mut slice = buf.as_slice();
        assert!(read_bitmap(&mut slice).is_err());
    }

    #[test]
    fn body_roundtrips_without_db_framing() {
        let mut postings = Postings::default();
        postings.add_posting_for_label_value(1, "job", "web");
        postings.add_posting_for_label_value(2, "job", "web");
        postings.add_posting_for_label_value(3, "job", "batch");
        postings.add_posting_for_label_value(1, "host", "a");
        postings.set_timeseries_key(1, b"ts:1");
        postings.set_timeseries_key(2, b"ts:2");
        postings.set_timeseries_key(3, b"ts:3");

        let mut buf = Vec::new();
        serialize(&mut buf, &postings);
        let mut slice = buf.as_slice();
        let loaded = deserialize(&mut slice).expect("body should round-trip");

        assert!(slice.is_empty(), "decoder must consume exactly the body");
        assert_eq!(loaded.id_to_key, postings.id_to_key);
        assert_eq!(loaded.all_postings, postings.all_postings);
        assert_eq!(loaded.label_index.len(), postings.label_index.len());
        for ((ka, va), (kb, vb)) in loaded.label_index.iter().zip(postings.label_index.iter()) {
            assert_eq!(ka, kb);
            assert_eq!(va, vb);
        }
    }

    #[test]
    fn stale_ids_are_subtracted_and_not_persisted() {
        let mut postings = Postings::default();
        postings.add_posting_for_label_value(1, "job", "web");
        postings.add_posting_for_label_value(2, "job", "web");
        postings.set_timeseries_key(1, b"ts:1");
        postings.set_timeseries_key(2, b"ts:2");
        postings.mark_id_as_stale(2);

        let mut buf = Vec::new();
        serialize(&mut buf, &postings);
        let mut slice = buf.as_slice();
        let loaded = deserialize(&mut slice).expect("body should round-trip");

        assert!(loaded.stale_ids.is_empty(), "stale set is not persisted");
        assert!(!loaded.all_postings.contains(2));
        for (_key, bitmap) in loaded.label_index.iter() {
            assert!(!bitmap.contains(2), "stale id survived serialization");
        }
        assert!(loaded.all_postings.contains(1));
    }
}
