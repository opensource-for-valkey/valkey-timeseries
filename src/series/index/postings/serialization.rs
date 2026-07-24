//! Wire format for a single [`Postings`] index.
//!
//! This module owns the byte layout of the index body and nothing else — no db numbering, no
//! payload framing, no magic. Those belong to whoever is embedding the body (today, the RDB aux
//! payload in `super::super::persistence`), which lets the container evolve without touching the
//! index encoding and keeps the field-level knowledge that encoding needs inside `postings`.
//!
//! The body does carry its own [`BODY_VERSION`], so the two version lines are independent:
//! reordering a field here costs a body bump and leaves the envelope alone, while a new framing
//! or a new section header costs an envelope bump and leaves bodies written by this module
//! readable as they are. It also makes a body self-describing to anything that can hand it a
//! byte slice, rather than only to the one container that happens to embed it today.
//!
//! Layout, in order:
//!
//! 1. **[`BODY_VERSION`]**, one byte.
//! 2. **`all_postings`**, one bitmap. Leads the rest so the decoder can check every label bitmap
//!    against it as it reads, rather than in a second pass over the rebuilt ART.
//! 3. **Label index**, as a group directory followed by the entries it describes. Keys are
//!    written grouped by their shared `name=` prefix, emitted once per group rather than once
//!    per entry — the on-disk analogue of the prefix sharing the ART gives us in memory. Label
//!    *value* cardinality is what grows (hosts, instances); the set of label *names* is small
//!    and schema-bound, so a name with k values costs one copy of the name instead of k.
//! 4. **Id directory**, ascending, as varint id deltas paired with key names.
//!
//! Stale ids are not part of the format; see [`serialize`].

use super::{IndexKey, Postings, PostingsBitmap, PostingsIndex, StaleSet};
use crate::common::encoding::{
    try_read_byte_slice, try_read_u8, try_read_uvarint, write_byte_slice, write_u8, write_uvarint,
};
use crate::series::SeriesRef;
use croaring::{Bitmap64, Portable};
use std::borrow::Cow;
use std::collections::BTreeMap;

/// Decoding is best-effort: any failure is reported as a message and leaves the caller to fall
/// back to rebuilding the index from the keyspace.
pub(in crate::series::index) type DecodeResult<T> = Result<T, String>;

/// Version of the field layout below, written as the first byte of every body.
///
/// Separate from the envelope's `INDEX_AUX_VERSION`, which versions the framing around the body
/// (magic, section count, per-section db number). Keeping the two apart means a change on either
/// side is priced on its own: the reorder that put `all_postings` first would have cost an
/// envelope bump under a single shared version, invalidating the framing along with it.
///
/// Per body rather than once per payload — one byte times the handful of non-empty dbs — because
/// a body that does not describe its own layout is only readable through the container that
/// wrote it, which is the coupling this module exists to avoid.
///
/// To evolve: keep [`serialize`] emitting the newest version and branch in [`deserialize`] on
/// what was read. An unrecognised version is rejected like any other decode failure, which falls
/// back to the per-key keyspace rebuild.
pub(in crate::series::index) const BODY_VERSION: u8 = 1;

// --- Decode limits -------------------------------------------------------------------------
//
// The body arrives from an RDB aux field, which means a replica syncing from a hostile master,
// a restored backup, or bit-rot that survived the CRC. Every length below is already bounded
// against the remaining buffer by `try_read_byte_slice`, so nothing here is about memory safety
// — it is about how much memory a given number of *payload* bytes can be made to allocate.
// Decode runs on the main thread before the keyspace exists, and an allocation failure aborts
// the process, so an amplifying payload is a replica crash-loop rather than a slow load.
//
// Every limit below is generous enough that only a corrupt or hostile payload trips it, and the
// cost of a false positive is a soft fallback to the per-key rebuild, never data loss.

/// Ceiling on one reconstructed label key (`prefix ++ suffix`). Label names are schema-bound and
/// values come from the ingest path, so this sits orders of magnitude above anything
/// [`IndexKey::for_label_value`] produces.
const MAX_INDEX_KEY_LEN: usize = 64 * 1024;

/// Ceiling on one timeseries key name in the id directory. Same rationale as
/// [`MAX_INDEX_KEY_LEN`]: far above any real key name, tight enough to bound a corrupt length.
const MAX_SERIES_KEY_LEN: usize = 64 * 1024;

/// Ceiling on one serialized bitmap blob. The largest legitimate bitmap is `all_postings` for a
/// very large db — a densely packed billion-id set serializes to roughly 120 MiB — so this
/// leaves room to spare while still rejecting a wildly corrupt length.
const MAX_BITMAP_BLOB_BYTES: usize = 256 * 1024 * 1024;

/// How much label-key data the body may expand into, as a multiple of the bytes it was decoded
/// from.
///
/// This is the bound that closes prefix-fanout amplification, and neither a per-key cap nor an
/// entry count closes it alone: the directory writes a group's prefix *once* and the decoder
/// rebuilds it *per entry*, so `n` minimum-size entries under one long prefix cost `n * 32`
/// bytes of payload but `n * prefix.len()` bytes of memory. At the per-key ceiling above that
/// ratio reaches 2048x.
///
/// Prefix sharing is real compression, so the honest ratio is above 1 — but not by much, because
/// a group's per-entry cost is dominated by its bitmap, which no amount of sharing shrinks. Real
/// schemas sit well under 1; 16 leaves room for a pathologically long label name over
/// single-byte values.
const MAX_KEY_EXPANSION_RATIO: usize = 16;

/// Cheapest possible group-directory entry: a one-byte zero length and a one-byte count.
const MIN_GROUP_BYTES: usize = 2;

/// Cheapest possible label entry: a one-byte zero-length suffix, a one-byte blob length, and the
/// smallest non-empty portable `Bitmap64` (8-byte bucket count, 4-byte high word, 18-byte
/// single-element roaring32). Pinned by `min_entry_bytes_matches_encoder`.
const MIN_ENTRY_BYTES: usize = 1 + 1 + 30;

/// Cheapest possible id-directory entry: a one-byte delta varint and a one-byte zero length.
const MIN_ID_ENTRY_BYTES: usize = 2;

/// Rejects a declared element count that the bytes still in `buf` could not possibly hold, before
/// the loop that reads them turns each promised element into an allocation. `min_bytes` is the
/// smallest encoding one element can have.
fn check_declared_count(
    what: &str,
    count: usize,
    buf: &[u8],
    min_bytes: usize,
) -> DecodeResult<()> {
    let capacity = buf.len() / min_bytes;
    if count > capacity {
        return Err(format!(
            "{what}: declared count {count} exceeds what the remaining {} bytes can hold ({capacity})",
            buf.len()
        ));
    }
    Ok(())
}

pub(in crate::series::index) fn write_bitmap(buf: &mut Vec<u8>, bitmap: &PostingsBitmap) {
    let size = bitmap.get_serialized_size_in_bytes::<Portable>();
    write_uvarint(buf, size as u64);
    buf.reserve(size);
    let before = buf.len();
    let _ = bitmap.serialize_into_vec::<Portable>(buf);
    debug_assert_eq!(buf.len() - before, size);
}

/// Stays permissive about *emptiness*: `all_postings` is legitimately empty for a db with no
/// live series. Label entries are not — [`deserialize`] rejects those at their own call site.
pub(in crate::series::index) fn read_bitmap(buf: &mut &[u8]) -> DecodeResult<PostingsBitmap> {
    let blob = try_read_byte_slice(buf).map_err(|e| e.to_string())?;
    if blob.len() > MAX_BITMAP_BLOB_BYTES {
        return Err(format!(
            "bitmap blob of {} bytes exceeds maximum {MAX_BITMAP_BLOB_BYTES}",
            blob.len()
        ));
    }
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
    write_u8(buf, BODY_VERSION);

    // First, so the decoder has it before the label bitmaps it validates. Needs no stale masking
    // of its own: `mark_ids_as_stale` evicts from this set eagerly, which is also what makes the
    // subset property hold on the written form — every label bitmap below is masked by the same
    // stale set that `all_postings` has already had removed.
    write_bitmap(buf, &postings.all_postings);

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
}

/// Reads one index body from `buf`, advancing it past the bytes consumed.
///
/// Assumes hostile input; see the decode limits above for what is bounded and why.
pub(in crate::series::index) fn deserialize(buf: &mut &[u8]) -> DecodeResult<Postings> {
    let version = try_read_u8(buf).map_err(|e| e.to_string())?;
    if version != BODY_VERSION {
        return Err(format!(
            "unsupported index body version {version} (expected {BODY_VERSION})"
        ));
    }

    // Budget for reconstructed label-key bytes, taken before any of the body proper is read.
    // `buf` may extend past this section (the caller decodes sections back to back), which only
    // ever makes the budget more generous, never tighter than the section deserves.
    let key_budget = buf.len().saturating_mul(MAX_KEY_EXPANSION_RATIO);

    // Leads the body so each label bitmap can be checked against it as it is read.
    let all_postings = read_bitmap(buf)?;

    // Group directory next (see [`serialize`]), then the entries for each group in the same
    // order. `with_capacity` is clamped: the count comes from the payload, and a corrupt one
    // must not turn into an unbounded allocation before the read that rejects it.
    let group_count = try_read_uvarint(buf).map_err(|e| e.to_string())? as usize;
    check_declared_count("label group directory", group_count, buf, MIN_GROUP_BYTES)?;
    let mut groups: Vec<(&[u8], usize)> = Vec::with_capacity(group_count.min(64));
    let mut entry_count: usize = 0;
    for _ in 0..group_count {
        let prefix = try_read_byte_slice(buf).map_err(|e| e.to_string())?;
        if prefix.len() > MAX_INDEX_KEY_LEN {
            return Err(format!(
                "label group prefix of {} bytes exceeds maximum {MAX_INDEX_KEY_LEN}",
                prefix.len()
            ));
        }
        let count = try_read_uvarint(buf).map_err(|e| e.to_string())? as usize;
        entry_count = entry_count.saturating_add(count);
        groups.push((prefix, count));
    }
    // The directory is fully read, so `buf` now starts at the entries it describes and its
    // length is the budget those entries have to fit in.
    check_declared_count("label index entries", entry_count, buf, MIN_ENTRY_BYTES)?;

    let mut label_index = PostingsIndex::new();
    let mut key_bytes: Vec<u8> = Vec::new();
    let mut key_bytes_used: usize = 0;
    for (prefix, count) in groups {
        for _ in 0..count {
            let suffix = try_read_byte_slice(buf).map_err(|e| e.to_string())?;
            let key_len = prefix.len() + suffix.len();
            if key_len > MAX_INDEX_KEY_LEN {
                return Err(format!(
                    "label index key of {key_len} bytes exceeds maximum {MAX_INDEX_KEY_LEN}"
                ));
            }
            key_bytes.clear();
            key_bytes.reserve(key_len);
            key_bytes.extend_from_slice(prefix);
            key_bytes.extend_from_slice(suffix);
            let key = IndexKey::from(key_bytes.as_slice());

            // Charged after construction, not from `key_len`: `IndexKey::from` converts lossily,
            // and each invalid byte expands into a 3-byte U+FFFD. Charging the encoded length
            // would leave the budget short by 3x on a payload of invalid UTF-8, which is free
            // for an attacker to produce. The check above already bounds this one allocation.
            key_bytes_used = key_bytes_used.saturating_add(key.len());
            if key_bytes_used > key_budget {
                return Err(format!(
                    "label index keys expand past {key_budget} bytes, {MAX_KEY_EXPANSION_RATIO}x the encoded body"
                ));
            }

            let bitmap = read_bitmap(buf)?;
            // `serialize` drops entries whose bitmap is empty, before and after stale masking,
            // so an empty one here is corrupt. Rejecting it is also what keeps MIN_ENTRY_BYTES
            // honest: an 8-byte empty blob is the cheapest entry an amplifying payload can use.
            if bitmap.is_empty() {
                return Err("empty bitmap in label index entry".to_string());
            }
            // `id_to_key` is the authoritative membership set and `all_postings` tracks it, so an
            // id here that is absent from it exists nowhere else in the index: it would survive
            // into query results as a phantom, resolve to no key, and never be reached by the
            // post-load reconciliation sweep (which walks `id_to_key`, where it does not appear).
            // Rejecting rather than intersecting keeps decode a pure round-trip — the payload is
            // reproduced exactly or discarded — and discarding is already the soft path back to a
            // keyspace rebuild.
            if !bitmap.is_subset(&all_postings) {
                return Err(format!(
                    "label index entry {} holds ids absent from all_postings",
                    key.as_str()
                ));
            }
            label_index
                .try_insert(key, bitmap)
                .map_err(|e| format!("label index insert failed: {e}"))?;
        }
    }

    let id_count = try_read_uvarint(buf).map_err(|e| e.to_string())? as usize;
    check_declared_count("id directory", id_count, buf, MIN_ID_ENTRY_BYTES)?;
    let mut id_to_key = BTreeMap::new();
    let mut prev_id: SeriesRef = 0;
    for _ in 0..id_count {
        let delta = try_read_uvarint(buf).map_err(|e| e.to_string())?;
        let id = prev_id
            .checked_add(delta)
            .ok_or_else(|| "series id overflow".to_string())?;
        prev_id = id;
        let key = try_read_byte_slice(buf).map_err(|e| e.to_string())?;
        if key.len() > MAX_SERIES_KEY_LEN {
            return Err(format!(
                "timeseries key name of {} bytes exceeds maximum {MAX_SERIES_KEY_LEN}",
                key.len()
            ));
        }
        id_to_key.insert(id, key.to_vec().into_boxed_slice());
    }

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

    /// The smallest blob `write_bitmap` can ever emit for a live entry.
    fn min_bitmap_blob() -> Vec<u8> {
        let mut bitmap = PostingsBitmap::new();
        bitmap.add(1);
        let mut blob = Vec::new();
        let _ = bitmap.serialize_into_vec::<Portable>(&mut blob);
        blob
    }

    fn bitmap_of(ids: &[u64]) -> PostingsBitmap {
        let mut bitmap = PostingsBitmap::new();
        bitmap.add_many(ids);
        bitmap
    }

    /// The `all_postings` every entry built from [`min_bitmap_blob`] is a subset of, so a test
    /// exercising some other check is not preempted by the subset check.
    fn live_ids() -> PostingsBitmap {
        bitmap_of(&[1])
    }

    /// Encodes a body directly, so a test can declare counts and lengths the encoder would never
    /// produce. `groups` carries each prefix with its *declared* entry count, which the `entries`
    /// that follow need not actually back.
    fn hostile_body(
        all_postings: &PostingsBitmap,
        groups: &[(Vec<u8>, u64)],
        entries: &[(Vec<u8>, Vec<u8>)],
        declared_id_count: u64,
        ids: &[(u64, Vec<u8>)],
    ) -> Vec<u8> {
        let mut buf = Vec::new();
        write_u8(&mut buf, BODY_VERSION);
        write_bitmap(&mut buf, all_postings);
        write_uvarint(&mut buf, groups.len() as u64);
        for (prefix, count) in groups {
            write_byte_slice(&mut buf, prefix);
            write_uvarint(&mut buf, *count);
        }
        for (suffix, blob) in entries {
            write_byte_slice(&mut buf, suffix);
            write_byte_slice(&mut buf, blob);
        }
        write_uvarint(&mut buf, declared_id_count);
        let mut prev = 0u64;
        for (id, key) in ids {
            write_uvarint(&mut buf, id - prev);
            prev = *id;
            write_byte_slice(&mut buf, key);
        }
        buf
    }

    fn decode_err(body: &[u8]) -> String {
        let mut slice = body;
        match deserialize(&mut slice) {
            Err(e) => e,
            Ok(_) => panic!("hostile body must be rejected"),
        }
    }

    /// The version must be the *first* byte of the body, or a reader cannot decide how to parse
    /// the rest before parsing it.
    #[test]
    fn body_leads_with_its_version() {
        let mut postings = Postings::default();
        postings.add_posting_for_label_value(1, "job", "web");
        postings.set_timeseries_key(1, b"ts:1");

        let mut buf = Vec::new();
        serialize(&mut buf, &postings);
        assert_eq!(buf.first(), Some(&BODY_VERSION));
    }

    /// A body from a future layout is rejected on its own terms, without the envelope having to
    /// know anything changed.
    #[test]
    fn rejects_unknown_body_version() {
        let mut postings = Postings::default();
        postings.add_posting_for_label_value(1, "job", "web");
        postings.set_timeseries_key(1, b"ts:1");

        let mut buf = Vec::new();
        serialize(&mut buf, &postings);
        buf[0] = BODY_VERSION + 1;

        let err = decode_err(&buf);
        assert!(
            err.contains("unsupported index body version"),
            "unexpected rejection: {err}"
        );
    }

    /// [`MIN_ENTRY_BYTES`] is load-bearing for the entry-count check, and it is derived from a
    /// croaring format detail. Pin it against what the encoder actually writes.
    #[test]
    fn min_entry_bytes_matches_encoder() {
        let mut entry = Vec::new();
        write_byte_slice(&mut entry, b"");
        let mut bitmap = PostingsBitmap::new();
        bitmap.add(1);
        write_bitmap(&mut entry, &bitmap);
        assert_eq!(
            entry.len(),
            MIN_ENTRY_BYTES,
            "cheapest encodable label entry must match MIN_ENTRY_BYTES"
        );
    }

    /// The amplification this hardening exists for: one long group prefix, rebuilt once per
    /// entry, turns a small body into an arbitrarily large index. Before the expansion budget
    /// this decoded happily at ~929x.
    #[test]
    fn rejects_amplifying_payload() {
        let mut prefix = vec![b'x'; 16 * 1024];
        prefix.push(b'=');
        let blob = min_bitmap_blob();
        let entries: Vec<(Vec<u8>, Vec<u8>)> = (0..2_000u64)
            .map(|i| (format!("{i:06}").into_bytes(), blob.clone()))
            .collect();

        let body = hostile_body(
            &live_ids(),
            &[(prefix, entries.len() as u64)],
            &entries,
            0,
            &[],
        );
        let err = decode_err(&body);
        assert!(err.contains("expand past"), "unexpected rejection: {err}");
    }

    /// Invalid UTF-8 is free to produce and `IndexKey::from` expands each bad byte into a 3-byte
    /// U+FFFD, so a prefix of `0xFF` triples on the way in. Sized to clear the budget on its
    /// encoded length and blow it only on its constructed length: charging `key_len` instead of
    /// `key.len()` lets this body through.
    #[test]
    fn rejects_lossy_utf8_expansion() {
        let prefix = vec![0xFFu8; 600];
        let blob = min_bitmap_blob();
        let entries: Vec<(Vec<u8>, Vec<u8>)> = (0..50u64)
            .map(|i| (format!("{i:02}").into_bytes(), blob.clone()))
            .collect();

        let body = hostile_body(
            &live_ids(),
            &[(prefix, entries.len() as u64)],
            &entries,
            0,
            &[],
        );
        let encoded_key_bytes = 50 * 602;
        assert!(
            encoded_key_bytes <= body.len() * MAX_KEY_EXPANSION_RATIO,
            "test is only meaningful if the encoded length stays inside the budget"
        );
        let err = decode_err(&body);
        assert!(err.contains("expand past"), "unexpected rejection: {err}");
    }

    /// A prefix so long that a single key is implausible, independent of how many entries follow.
    #[test]
    fn rejects_oversized_group_prefix() {
        let prefix = vec![b'x'; MAX_INDEX_KEY_LEN + 1];
        let body = hostile_body(&live_ids(), &[(prefix, 0)], &[], 0, &[]);
        let err = decode_err(&body);
        assert!(err.contains("group prefix"), "unexpected rejection: {err}");
    }

    /// The prefix fits, the suffix fits, but the key they rebuild does not.
    #[test]
    fn rejects_oversized_reconstructed_key() {
        let prefix = vec![b'x'; MAX_INDEX_KEY_LEN - 1];
        let suffix = vec![b'y'; 8];
        let body = hostile_body(
            &live_ids(),
            &[(prefix, 1)],
            &[(suffix, min_bitmap_blob())],
            0,
            &[],
        );
        let err = decode_err(&body);
        assert!(
            err.contains("label index key"),
            "unexpected rejection: {err}"
        );
    }

    /// A directory promising more entries than the remaining bytes could encode must fail before
    /// the entry loop allocates anything, not after it runs out of buffer.
    #[test]
    fn rejects_inflated_entry_count() {
        let body = hostile_body(
            &live_ids(),
            &[(b"job=".to_vec(), u32::MAX as u64)],
            &[],
            0,
            &[],
        );
        let err = decode_err(&body);
        assert!(
            err.contains("label index entries") && err.contains("declared count"),
            "unexpected rejection: {err}"
        );
    }

    #[test]
    fn rejects_inflated_group_count() {
        let mut body = Vec::new();
        write_u8(&mut body, BODY_VERSION);
        write_bitmap(&mut body, &PostingsBitmap::new());
        write_uvarint(&mut body, u32::MAX as u64);
        body.extend_from_slice(&[0u8; 16]);
        let err = decode_err(&body);
        assert!(
            err.contains("label group directory"),
            "unexpected rejection: {err}"
        );
    }

    #[test]
    fn rejects_inflated_id_count() {
        let body = hostile_body(&live_ids(), &[], &[], u32::MAX as u64, &[]);
        let err = decode_err(&body);
        assert!(err.contains("id directory"), "unexpected rejection: {err}");
    }

    /// `serialize` never emits one, and it is the cheapest entry an amplifying payload can use.
    /// The suffix is long enough that the body clears the entry-count budget, so the empty
    /// bitmap is what the decoder rejects rather than the cheaper check that runs before it.
    #[test]
    fn rejects_empty_label_bitmap() {
        let mut empty_blob = Vec::new();
        let _ = PostingsBitmap::new().serialize_into_vec::<Portable>(&mut empty_blob);
        let body = hostile_body(
            &live_ids(),
            &[(b"job=".to_vec(), 1)],
            &[(vec![b'w'; 24], empty_blob)],
            0,
            &[],
        );
        let err = decode_err(&body);
        assert!(err.contains("empty bitmap"), "unexpected rejection: {err}");
    }

    /// A label bitmap may only name ids `all_postings` knows about. One that does not would reach
    /// the query planner as a phantom — resolving to no key, and invisible to the post-load
    /// reconciliation sweep, which walks `id_to_key`.
    #[test]
    fn rejects_phantom_id() {
        let body = hostile_body(
            &bitmap_of(&[2]),
            &[(b"job=".to_vec(), 1)],
            // `min_bitmap_blob` holds id 1, which `all_postings` above does not.
            &[(vec![b'w'; 24], min_bitmap_blob())],
            0,
            &[],
        );
        let err = decode_err(&body);
        assert!(
            err.contains("absent from all_postings"),
            "unexpected rejection: {err}"
        );
    }

    /// The shape most at risk from a subset check written in the wrong direction: a series with
    /// no labels is in `all_postings` and `id_to_key` but in no label bitmap at all, which
    /// [`Postings::count`] treats as a first-class member.
    #[test]
    fn accepts_series_with_no_labels() {
        let mut postings = Postings::default();
        postings.add_posting_for_label_value(1, "job", "web");
        postings.set_timeseries_key(1, b"ts:1");
        // Id 7 gets a key and membership, but no labels.
        postings.all_postings.add(7);
        postings.set_timeseries_key(7, b"ts:7");

        let mut buf = Vec::new();
        serialize(&mut buf, &postings);
        let mut slice = buf.as_slice();
        let loaded = deserialize(&mut slice).expect("a label-less series must round-trip");

        assert!(slice.is_empty(), "decoder must consume exactly the body");
        assert_eq!(loaded.count(), 2);
        assert!(loaded.all_postings.contains(7));
        assert_eq!(
            loaded.get_key_by_id(7).map(|k| k.as_ref().to_vec()),
            Some(b"ts:7".to_vec())
        );
    }

    #[test]
    fn rejects_oversized_series_key_name() {
        let body = hostile_body(
            &live_ids(),
            &[],
            &[],
            1,
            &[(1, vec![b'k'; MAX_SERIES_KEY_LEN + 1])],
        );
        let err = decode_err(&body);
        assert!(
            err.contains("timeseries key name"),
            "unexpected rejection: {err}"
        );
    }

    /// The guard that matters most is the one that must *not* fire. A long label name over
    /// single-character values is the worst shape real data takes — the expansion ratio it
    /// reaches has to stay comfortably inside the budget, or index persistence silently
    /// degrades to a keyspace rebuild on ordinary workloads.
    #[test]
    fn accepts_worst_case_realistic_prefix_sharing() {
        let name =
            "k8s_pod_annotation_prometheus_io_custom_scrape_configuration_parameter".repeat(3);
        let mut postings = Postings::default();
        for i in 0..2_000u64 {
            let mut bitmap = PostingsBitmap::new();
            bitmap.add(i + 1);
            postings
                .label_index
                .try_insert(IndexKey::for_label_value(&name, &format!("{i}")), bitmap)
                .unwrap();
            postings.all_postings.add(i + 1);
        }

        let mut buf = Vec::new();
        serialize(&mut buf, &postings);
        let mut slice = buf.as_slice();
        let loaded = deserialize(&mut slice).expect("realistic prefix sharing must decode");
        assert_eq!(loaded.label_index.len(), postings.label_index.len());

        let key_bytes: usize = loaded.label_index.iter().map(|(k, _)| k.len()).sum();
        let ratio = key_bytes as f64 / buf.len() as f64;
        assert!(
            ratio < MAX_KEY_EXPANSION_RATIO as f64 / 2.0,
            "realistic data reached {ratio:.1}x, too close to the {MAX_KEY_EXPANSION_RATIO}x budget"
        );
    }

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
