// Original Source: https://github.com/joshuaclayton/jaro_winkler
// Copyright (c) 2022 Josh Clayton
//
// Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated
// documentation files (the "Software"), to deal in the Software without restriction, including without limitation
// the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software,
// and to permit persons to whom the Software is furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO
// THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS
// OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
// ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

use std::collections::HashMap;

/// Inputs whose longer side has at most this many units (bytes or chars) are
/// matched with single `u128` bitmasks; longer inputs take the `*_long` paths.
const SHORT_MAX_LEN: usize = 128;

/// Calculates the Jaro-Winkler similarity of two strings.
///
/// Comparison is performed on Unicode scalar values (chars), not raw UTF-8
/// bytes. This ensures that multi-byte characters are treated as single units
/// and non-ASCII strings are scored correctly.
///
/// When both inputs are pure ASCII the implementation uses a zero-allocation
/// byte-slice fast path, so the common case of ASCII label names incurs no
/// heap overhead.
///
/// The return value is between 0.0 and 1.0, where 1.0 means the strings are equal.
/// The score is symmetric: `jaro_winkler(a, b) == jaro_winkler(b, a)`.
///
/// # Transposition convention
///
/// Jaro pairs the matched characters of both strings in order, counts the
/// pairs that differ, and charges half that count as transpositions. This
/// implementation rounds the half **up** (`ceil(out_of_order / 2)`), where the
/// textbook definition uses the real-valued half. Scores therefore come out
/// slightly lower than the textbook value whenever the out-of-order count is
/// odd, and are identical when it is even.
///
/// # Examples
///
/// ```
/// use valkey_timeseries::common::strings::jaro_winkler;
///
/// assert_eq!(jaro_winkler("martha", "marhta"), 0.96111107);
/// assert_eq!(jaro_winkler("", "words"), 0.0);
/// assert_eq!(jaro_winkler("same", "same"), 1.0);
/// ```
pub fn jaro_winkler(left: &str, right: &str) -> f32 {
    // Fast path: both strings are pure ASCII — bytes and chars are identical, so
    // we can use the byte slices directly without any allocation.
    if left.is_ascii() && right.is_ascii() {
        return jaro_winkler_bytes(left.as_bytes(), right.as_bytes());
    }

    // Unicode path: collect char vectors so every codepoint is treated as a
    // single unit regardless of its UTF-8 byte width.
    let left_chars: Vec<char> = left.chars().collect();
    let right_chars: Vec<char> = right.chars().collect();
    jaro_winkler_chars(&left_chars, &right_chars)
}

/// Jaro-Winkler over pure-ASCII byte slices.
///
/// Both inputs MUST be valid ASCII; calling this with non-ASCII bytes produces
/// incorrect results.
fn jaro_winkler_bytes(left: &[u8], right: &[u8]) -> f32 {
    let (left, right) = longer_first(left, right);
    if let Some(score) = trivial_score(left, right) {
        return score;
    }
    let range = matching_distance(left.len(), right.len());
    if left.len() > SHORT_MAX_LEN {
        jaro_winkler_long_bytes(left, right, range)
    } else {
        jaro_winkler_short(left, right, range)
    }
}

/// Jaro-Winkler over Unicode scalar values.
fn jaro_winkler_chars(left: &[char], right: &[char]) -> f32 {
    let (left, right) = longer_first(left, right);
    if let Some(score) = trivial_score(left, right) {
        return score;
    }
    let range = matching_distance(left.len(), right.len());
    if left.len() > SHORT_MAX_LEN {
        jaro_winkler_long(left, right, range)
    } else {
        jaro_winkler_short(left, right, range)
    }
}

/// Orders the inputs so the longer one comes first, keeping the given order
/// for equal lengths.
fn longer_first<'a, T>(a: &'a [T], b: &'a [T]) -> (&'a [T], &'a [T]) {
    if a.len() < b.len() { (b, a) } else { (a, b) }
}

/// The score for empty or identical inputs, which need no matching pass.
fn trivial_score<T: PartialEq>(left: &[T], right: &[T]) -> Option<f32> {
    match (left.len(), right.len()) {
        (0, 0) => Some(1.0),
        (0, _) | (_, 0) => Some(0.0),
        _ if left == right => Some(1.0),
        _ => None,
    }
}

/// Matching pass for inputs of at most [`SHORT_MAX_LEN`] units, with the
/// matched positions of each side held in a `u128`.
///
/// `left` must be the longer input.
fn jaro_winkler_short<T: PartialEq>(left: &[T], right: &[T], range: usize) -> f32 {
    debug_assert!(right.len() <= left.len() && left.len() <= SHORT_MAX_LEN);
    let s1_len = left.len();
    let mut s1m: u128 = 0;
    let mut s2m: u128 = 0;
    let mut matching: f32 = 0.0;

    for (i, r) in right.iter().enumerate() {
        let window_start = i.saturating_sub(range);
        let window_end = (i + range + 1).min(s1_len);
        for (j, l) in (window_start..).zip(&left[window_start..window_end]) {
            if (s1m >> j) & 1 == 0 && r == l {
                s1m |= 1 << j;
                s2m |= 1 << i;
                matching += 1.0;
                break;
            }
        }
    }

    if matching == 0.0 {
        return 0.0;
    }

    let s1m = [s1m as u64, (s1m >> 64) as u64];
    let s2m = [s2m as u64, (s2m >> 64) as u64];
    let transpositions = count_transpositions(left, right, &s1m, &s2m);
    winkler(matching, transpositions, left, right)
}

/// Matching pass for Unicode inputs where `left` (the longer one) exceeds
/// [`SHORT_MAX_LEN`] chars.
///
/// The positions of each distinct char of `left` are kept as an ascending
/// occurrence list (all lists share one `positions` buffer), so memory is
/// O(len) however many distinct chars there are. Each list has a cursor, and
/// every position before it is either matched or lies before the current match
/// window. The window start never decreases as `i` advances, so a skipped
/// position can never become eligible again, and the first position at or
/// after the cursor is the leftmost unmatched occurrence inside the window —
/// the choice the short path's linear scan makes. Cursors only move forward, so
/// the whole pass is O(len) amortized.
fn jaro_winkler_long(left: &[char], right: &[char], range: usize) -> f32 {
    let s1_len = left.len();

    // Assign each distinct char a group id and count its occurrences.
    let mut group_of: HashMap<char, usize> = HashMap::new();
    let mut group_ids: Vec<usize> = Vec::with_capacity(s1_len);
    let mut counts: Vec<usize> = Vec::new();
    for &c in left {
        let next_id = counts.len();
        let group = *group_of.entry(c).or_insert(next_id);
        if group == next_id {
            counts.push(0);
        }
        counts[group] += 1;
        group_ids.push(group);
    }

    // Group `g` owns `positions[starts[g]..starts[g + 1]]`.
    let mut starts: Vec<usize> = Vec::with_capacity(counts.len() + 1);
    starts.push(0);
    let mut total = 0;
    for count in counts {
        total += count;
        starts.push(total);
    }

    // Fill each list from its end while walking `left` backwards; afterwards
    // every cursor sits at the start of its list.
    let mut cursor: Vec<usize> = starts[1..].to_vec();
    let mut positions = vec![0usize; s1_len];
    for (j, &group) in group_ids.iter().enumerate().rev() {
        cursor[group] -= 1;
        positions[cursor[group]] = j;
    }
    drop(group_ids);

    let mut s1m = vec![0u64; s1_len.div_ceil(64)];
    let mut s2m = vec![0u64; right.len().div_ceil(64)];
    let mut matching: f32 = 0.0;

    for (i, c) in right.iter().enumerate() {
        let Some(&group) = group_of.get(c) else {
            continue;
        };
        let window_start = i.saturating_sub(range);
        let window_end = (i + range + 1).min(s1_len);
        let list_end = starts[group + 1];
        let mut k = cursor[group];
        while k < list_end && positions[k] < window_start {
            k += 1;
        }
        if k < list_end && positions[k] < window_end {
            let j = positions[k];
            s1m[j >> 6] |= 1u64 << (j & 63);
            s2m[i >> 6] |= 1u64 << (i & 63);
            matching += 1.0;
            k += 1;
        }
        cursor[group] = k;
    }

    if matching == 0.0 {
        return 0.0;
    }

    let transpositions = count_transpositions(left, right, &s1m, &s2m);
    winkler(matching, transpositions, left, right)
}

/// Bit-parallel Jaro-Winkler for pure-ASCII strings where `left` (the longer
/// one) exceeds [`SHORT_MAX_LEN`] bytes.
///
/// A positional bitmask per byte value records where it occurs in `left`, so
/// matching reduces to bitwise AND/NOT per 64-bit word. The 256-entry table
/// costs `256 * ceil(len / 64)` words, linear in the length.
fn jaro_winkler_long_bytes(left: &[u8], right: &[u8], range: usize) -> f32 {
    let s1_len = left.len();
    let s1_words = s1_len.div_ceil(64);

    let mut char_masks = vec![0u64; 256 * s1_words];
    for (j, &b) in left.iter().enumerate() {
        char_masks[(b as usize) * s1_words + (j >> 6)] |= 1u64 << (j & 63);
    }

    let mut s1m = vec![0u64; s1_words];
    let mut s2m = vec![0u64; right.len().div_ceil(64)];
    let mut matching: f32 = 0.0;

    for (i, &r) in right.iter().enumerate() {
        let target = r as usize;
        let j_start = i.saturating_sub(range);
        let j_end = (i + range + 1).min(s1_len);

        let start_word = j_start >> 6;
        let end_word = ((j_end - 1) >> 6) + 1;

        for w in start_word..end_word.min(s1_words) {
            let mut candidates = char_masks[target * s1_words + w] & !s1m[w];

            // Mask out bits outside the match window in boundary words.
            let lo = if w == start_word { j_start & 63 } else { 0 };
            let hi = if w == end_word - 1 {
                let b = j_end & 63;
                if b == 0 { 64 } else { b }
            } else {
                64
            };

            if lo > 0 {
                candidates &= !((1u64 << lo) - 1);
            }
            if hi < 64 {
                candidates &= (1u64 << hi) - 1;
            }

            if candidates != 0 {
                let bit = candidates.trailing_zeros();
                s1m[w] |= 1u64 << bit;
                s2m[i >> 6] |= 1u64 << (i & 63);
                matching += 1.0;
                break;
            }
        }
    }

    if matching == 0.0 {
        return 0.0;
    }

    let transpositions = count_transpositions(left, right, &s1m, &s2m);
    winkler(matching, transpositions, left, right)
}

/// Positions of the set bits of a little-endian multi-word bitmask, ascending.
fn set_bits(words: &[u64]) -> impl Iterator<Item = usize> + '_ {
    words.iter().enumerate().flat_map(|(w, &word)| {
        let mut bits = word;
        std::iter::from_fn(move || {
            if bits == 0 {
                return None;
            }
            let bit = bits.trailing_zeros() as usize;
            bits &= bits - 1;
            Some((w << 6) | bit)
        })
    })
}

/// Pairs the k-th matched char of `right` with the k-th matched char of `left`
/// (both sides hold the same number of matches, so every match is paired) and
/// returns the transposition count under this module's convention: half the
/// differing pairs, rounded up (see [`jaro_winkler`]).
fn count_transpositions<T: PartialEq>(left: &[T], right: &[T], s1m: &[u64], s2m: &[u64]) -> f32 {
    let out_of_order = set_bits(s2m)
        .zip(set_bits(s1m))
        .filter(|&(i, j)| right[i] != left[j])
        .count();
    out_of_order.div_ceil(2) as f32
}

fn winkler<T: PartialEq>(matching: f32, transpositions: f32, left: &[T], right: &[T]) -> f32 {
    let jaro = (matching / (left.len() as f32)
        + matching / (right.len() as f32)
        + (matching - transpositions) / matching)
        / 3.0;

    let prefix_length = left
        .iter()
        .zip(right)
        .take(4)
        .take_while(|(l, r)| l == r)
        .count() as f32;

    jaro + prefix_length * 0.1 * (1.0 - jaro)
}

fn matching_distance(s1_len: usize, s2_len: usize) -> usize {
    (s1_len.max(s2_len) / 2).saturating_sub(1)
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    #[test]
    fn different_is_zero() {
        assert_eq!(jaro_winkler("foo", "bar"), 0.0);
    }

    #[test]
    fn same_is_one() {
        assert_eq!(jaro_winkler("foo", "foo"), 1.0);
        assert_eq!(jaro_winkler("", ""), 1.0);
    }

    #[test]
    fn test_hello() {
        assert_eq!(jaro_winkler("hell", "hello"), 0.96_f32);
    }

    /// Reference implementation using the standard (non-bit-parallel) algorithm
    /// operating on Unicode scalar values, with this module's `ceil` convention
    /// for transpositions. Used to validate that the optimised paths produce
    /// the same results.
    fn jaro_winkler_reference(left_: &str, right_: &str) -> f64 {
        let left_chars: Vec<char> = left_.chars().collect();
        let right_chars: Vec<char> = right_.chars().collect();
        let llen = left_chars.len();
        let rlen = right_chars.len();
        let (left, right, s1_len, s2_len) = if llen < rlen {
            (right_chars.as_slice(), left_chars.as_slice(), rlen, llen)
        } else {
            (left_chars.as_slice(), right_chars.as_slice(), llen, rlen)
        };
        match (s1_len, s2_len) {
            (0, 0) => return 1.0,
            (0, _) | (_, 0) => return 0.0,
            _ => (),
        }
        if s1_len == s2_len && left == right {
            return 1.0;
        }
        let range = (s1_len.max(s2_len) / 2).saturating_sub(1);
        let mut s1m = vec![false; s1_len];
        let mut s2m = vec![false; s2_len];
        let mut matching: f64 = 0.0;
        let mut transpositions: f64 = 0.0;
        for i in 0..s2_len {
            let j_start = (i as isize - range as isize).max(0) as usize;
            let j_end = (i + range + 1).min(s1_len);
            for j in j_start..j_end {
                if right[i] == left[j] && !s1m[j] {
                    s1m[j] = true;
                    s2m[i] = true;
                    matching += 1.0;
                    break;
                }
            }
        }
        if matching == 0.0 {
            return 0.0;
        }
        let mut l = 0;
        for i in 0..s2_len {
            if s2m[i] {
                let mut j = l;
                while j < s1_len {
                    if s1m[j] {
                        l = j + 1;
                        break;
                    }
                    j += 1;
                }
                if right[i] != left[j] {
                    transpositions += 1.0;
                }
            }
        }
        transpositions = (transpositions / 2.0).ceil();
        let jaro = (matching / (s1_len as f64)
            + matching / (s2_len as f64)
            + (matching - transpositions) / matching)
            / 3.0;
        let prefix_length = left
            .iter()
            .zip(right)
            .take(4)
            .take_while(|(l, r)| l == r)
            .count() as f64;
        jaro + prefix_length * 0.1 * (1.0 - jaro)
    }

    fn assert_close(actual: f32, expected: f64, what: &str) {
        assert!(
            (actual as f64 - expected).abs() < 1e-6,
            "{what}: got {actual}, expected {expected}"
        );
    }

    /// Validate the optimized jaro_winkler (including bit-parallel path for
    /// long strings) produces identical results to the reference implementation
    /// across a wide range of inputs. Also verifies order independence.
    #[test]
    fn cross_validate_optimized_vs_reference() {
        let pairs: Vec<(&str, &str)> = vec![
            ("", ""),
            ("", "a"),
            ("a", ""),
            ("a", "a"),
            ("a", "b"),
            ("foo", "bar"),
            ("foo", "foo"),
            ("hell", "hello"),
            ("martha", "marhta"),
            ("wonderful", "wonderment"),
            ("hello hi what is going on", "hell"),
            ("aacb", "bbbaccaa"),
            ("abaabcaca", "acacbabca"),
            // Long: exercises jaro_winkler_long (223 chars)
            (
                "test result: ok. 0 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s Doc-tests jaro running 0 tests test result: ok. 0 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s",
                "wonderment double double",
            ),
            // Both long: both > 128
            (
                "test result: ok. 0 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s Doc-tests jaro running 0 tests test result: ok. 0 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s",
                "; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s Doc-tests jaro running 0 tests test result: ok. 0 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s",
            ),
            // Boundary: exactly 129 chars
            (
                "test result: ok. 0 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s Doc-tests jaro running 0 tests test",
                "test result: ok. 0 passed; 0 failed; 0 ignored; 0 measured;test result: ok. 0 passed; 0 failed; 0 ignored; 0 measured;test result: ok. 0 passed; 0 failed; 0 ignored; 0 measured;test result: ok. 0 passed; 0 failed; 0 ignored; 0 measured;test result: ok. 0 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s Doc-tests jaro running 0 tests test result: ok. 0 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s",
            ),
            // Short left, long right (forces swap)
            (
                "abc",
                "abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyz0123456789",
            ),
            // Both very long, mostly different
            (
                "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
            ),
            // Non-ASCII: identical Unicode strings score 1.0
            ("café", "café"),
            // Non-ASCII: completely different multibyte strings score < 1.0
            ("日本語", "中文"),
            // Non-ASCII: partial overlap of multibyte chars
            ("naïve", "naive"),
        ];

        for (a, b) in &pairs {
            let reference = jaro_winkler_reference(a, b);
            assert_close(jaro_winkler(a, b), reference, &format!("({a:?}, {b:?})"));
            // Verify order independence
            assert_close(
                jaro_winkler(b, a),
                reference,
                &format!("flipped ({b:?}, {a:?})"),
            );
        }
    }

    /// The last matched char of the shorter string takes part in the
    /// transposition count: an odd number of out-of-order pairs that includes
    /// the final pair rounds up to the next transposition.
    #[test]
    fn last_char_counts_toward_transpositions() {
        // 3 out-of-order pairs -> ceil(3 / 2) = 2 transpositions.
        assert_close(jaro_winkler("aacb", "bbbaccaa"), 0.486_111, "aacb");
        assert_close(jaro_winkler("bbbaccaa", "aacb"), 0.486_111, "aacb flipped");

        // 7 out-of-order pairs -> 4 transpositions, in either order.
        let forward = jaro_winkler("abaabcaca", "acacbabca");
        let backward = jaro_winkler("acacbabca", "abaabcaca");
        assert_eq!(forward, backward);
        assert_close(forward, 0.783_333, "abaabcaca");

        // A 3-cycle in the last three chars: xyz vs yzx, all out of order, so
        // 2 transpositions with the 4-char Winkler prefix bonus.
        let expected = {
            let m = 9.0_f64;
            let jaro = (1.0 + 1.0 + (m - 2.0) / m) / 3.0;
            jaro + 0.4 * (1.0 - jaro)
        };
        assert_close(jaro_winkler("abcdefxyz", "abcdefyzx"), expected, "xyz");
        assert_close(jaro_winkler("abcdefyzx", "abcdefxyz"), expected, "yzx");
    }

    /// Scores `(a, b)` through every internal path that can take the input and
    /// returns `(path, score)` pairs; the long paths are forced even for short
    /// inputs.
    fn all_paths(a: &str, b: &str) -> Vec<(&'static str, f32)> {
        fn forced<T: PartialEq>(a: &[T], b: &[T], path: fn(&[T], &[T], usize) -> f32) -> f32 {
            let (left, right) = longer_first(a, b);
            trivial_score(left, right)
                .unwrap_or_else(|| path(left, right, matching_distance(left.len(), right.len())))
        }

        let a_chars: Vec<char> = a.chars().collect();
        let b_chars: Vec<char> = b.chars().collect();
        let short = a_chars.len().max(b_chars.len()) <= SHORT_MAX_LEN;
        let mut scores = vec![
            ("public", jaro_winkler(a, b)),
            ("chars", jaro_winkler_chars(&a_chars, &b_chars)),
            ("long_chars", forced(&a_chars, &b_chars, jaro_winkler_long)),
        ];
        if short {
            scores.push((
                "short_chars",
                forced(&a_chars, &b_chars, jaro_winkler_short),
            ));
        }
        if a.is_ascii() && b.is_ascii() {
            let (a, b) = (a.as_bytes(), b.as_bytes());
            scores.push(("bytes", jaro_winkler_bytes(a, b)));
            scores.push(("long_bytes", forced(a, b, jaro_winkler_long_bytes)));
            if short {
                scores.push(("short_bytes", forced(a, b, jaro_winkler_short)));
            }
        }
        scores
    }

    fn assert_paths_agree(a: &str, b: &str) {
        let scores = all_paths(a, b);
        let (_, first) = scores[0];
        for (path, score) in &scores {
            assert_eq!(
                score.to_bits(),
                first.to_bits(),
                "path {path} disagrees for ({a:?}, {b:?}): {scores:?}"
            );
        }
    }

    /// Maps each ASCII byte to a distinct non-ASCII char, so the Unicode path
    /// sees the same structure the ASCII path does.
    fn to_unicode(s: &str) -> String {
        s.bytes()
            .map(|b| char::from_u32(0x4E00 + b as u32).unwrap())
            .collect()
    }

    #[test]
    fn paths_agree_on_fixed_inputs() {
        let long_a = "ab".repeat(80);
        let long_b = "ba".repeat(70);
        let pairs = [
            ("martha", "marhta"),
            ("aacb", "bbbaccaa"),
            ("abaabcaca", "acacbabca"),
            ("abcdefxyz", "abcdefyzx"),
            ("café", "cafe"),
            ("日本語", "日語本"),
            (long_a.as_str(), long_b.as_str()),
            (long_a.as_str(), "ba"),
        ];
        for (a, b) in pairs {
            assert_paths_agree(a, b);
            assert_paths_agree(b, a);
        }
    }

    /// A long Unicode string of 10,000 distinct chars. Positional bitmasks per
    /// distinct char would need 10,000 * 157 words; occurrence lists need one
    /// entry per char.
    #[test]
    fn long_high_cardinality_unicode() {
        const N: u32 = 10_000;
        let left: Vec<char> = (0..N)
            .map(|k| char::from_u32(0x4E00 + k).unwrap())
            .collect();
        let left_str: String = left.iter().collect();

        // Two adjacent chars swapped after the Winkler prefix: every char
        // matches and 2 pairs are out of order, so 1 transposition.
        let mut swapped = left.clone();
        swapped.swap(100, 101);
        let swapped_str: String = swapped.iter().collect();
        let m = N as f32;
        let jaro = (m / m + m / m + (m - 1.0) / m) / 3.0;
        let expected = jaro + 4.0 * 0.1 * (1.0 - jaro);
        assert_eq!(jaro_winkler(&left_str, &swapped_str), expected);
        assert_eq!(jaro_winkler(&swapped_str, &left_str), expected);

        // Disjoint alphabets: nothing matches.
        let other: String = (0..N)
            .map(|k| char::from_u32(0x4E00 + N + k).unwrap())
            .collect();
        assert_eq!(jaro_winkler(&left_str, &other), 0.0);

        // A scrambled, partly disjoint 3,000-char case against the reference.
        let mut scrambled: Vec<char> = left[..3_000].to_vec();
        scrambled.reverse();
        for (k, c) in scrambled.iter_mut().enumerate() {
            if k % 5 == 0 {
                *c = char::from_u32(0x4E00 + N + k as u32).unwrap();
            }
        }
        scrambled.rotate_left(700);
        let scrambled_str: String = scrambled.iter().collect();
        let head: String = left[..3_000].iter().collect();
        assert_close(
            jaro_winkler(&head, &scrambled_str),
            jaro_winkler_reference(&head, &scrambled_str),
            "scrambled",
        );
        assert_paths_agree(&head, &scrambled_str);
    }

    /// Two strings of the same length drawn from `alphabet`, so the input
    /// order is kept rather than normalized by length.
    fn same_len_pair(
        alphabet: &'static [char],
        max_len: usize,
    ) -> impl Strategy<Value = (String, String)> {
        (0..=max_len).prop_flat_map(move |n| {
            let s = prop::collection::vec(prop::sample::select(alphabet), n)
                .prop_map(|v| v.into_iter().collect::<String>());
            (s.clone(), s)
        })
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(2_000))]

        #[test]
        fn symmetric_short_ascii((a, b) in same_len_pair(&['a', 'b', 'c'], 12)) {
            prop_assert_eq!(jaro_winkler(&a, &b).to_bits(), jaro_winkler(&b, &a).to_bits());
        }

        #[test]
        fn symmetric_short_unicode((a, b) in same_len_pair(&['a', 'é', '日'], 12)) {
            prop_assert_eq!(jaro_winkler(&a, &b).to_bits(), jaro_winkler(&b, &a).to_bits());
        }

        #[test]
        fn short_matches_reference(a in "[abc]{0,10}", b in "[abc]{0,10}") {
            let reference = jaro_winkler_reference(&a, &b);
            prop_assert!((jaro_winkler(&a, &b) as f64 - reference).abs() < 1e-6);
        }

        #[test]
        fn short_paths_agree(a in "[abc]{0,12}", b in "[abc]{0,12}") {
            assert_paths_agree(&a, &b);
            prop_assert_eq!(
                jaro_winkler(&to_unicode(&a), &to_unicode(&b)).to_bits(),
                jaro_winkler(&a, &b).to_bits()
            );
        }
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(200))]

        #[test]
        fn symmetric_long((a, b) in same_len_pair(&['a', 'b', 'c', 'd'], 200)) {
            prop_assert_eq!(jaro_winkler(&a, &b).to_bits(), jaro_winkler(&b, &a).to_bits());
            let (ua, ub) = (to_unicode(&a), to_unicode(&b));
            prop_assert_eq!(jaro_winkler(&ua, &ub).to_bits(), jaro_winkler(&ub, &ua).to_bits());
        }

        #[test]
        fn long_paths_agree(a in "[abcd]{100,180}", b in "[abcd]{0,180}") {
            assert_paths_agree(&a, &b);
            prop_assert_eq!(
                jaro_winkler(&to_unicode(&a), &to_unicode(&b)).to_bits(),
                jaro_winkler(&a, &b).to_bits()
            );
            let reference = jaro_winkler_reference(&a, &b);
            prop_assert!((jaro_winkler(&a, &b) as f64 - reference).abs() < 1e-6);
        }
    }

    /// Identical non-ASCII strings must score exactly 1.0.
    #[test]
    fn non_ascii_identical_scores_one() {
        assert_eq!(jaro_winkler("café", "café"), 1.0);
        assert_eq!(jaro_winkler("日本語", "日本語"), 1.0);
        assert_eq!(jaro_winkler("Ünïcödé", "Ünïcödé"), 1.0);
    }

    /// Completely different non-ASCII strings must score < 1.0 (previously
    /// raw-byte comparison could produce false matches due to shared encoding bytes).
    #[test]
    fn non_ascii_different_scores_below_one() {
        // These Japanese and Chinese strings share no Unicode scalar values.
        let score = jaro_winkler("日本語", "中文字");
        assert!(
            score < 1.0,
            "expected score < 1.0 for unrelated non-ASCII strings, got {score}"
        );
    }
}
