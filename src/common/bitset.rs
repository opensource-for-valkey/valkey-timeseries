// MIT License
//
// Copyright (c) 2025 opendata-oss
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

/// Fixed-length bitset backed by `Vec<u64>`, used as the validity column of
/// every [`StepBatch`]. Default (all zero) means "all cells absent."
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct BitSet {
    bits: Vec<u64>,
    len: usize,
}

impl BitSet {
    /// All bits cleared.
    pub fn with_len(len: usize) -> Self {
        let word_count = len.div_ceil(64);
        Self {
            bits: vec![0u64; word_count],
            len,
        }
    }

    /// All bits set.
    pub fn all_set(len: usize) -> Self {
        let word_count = len.div_ceil(64);
        let mut bits = vec![u64::MAX; word_count];
        if !len.is_multiple_of(64)
            && let Some(last) = bits.last_mut()
        {
            *last = (1u64 << (len % 64)) - 1;
        }
        Self { bits, len }
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.len
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Panics if `idx >= len`.
    #[inline]
    pub fn get(&self, idx: usize) -> bool {
        assert!(
            idx < self.len,
            "index {} out of bounds for bitset of len {}",
            idx,
            self.len
        );
        let (word, bit) = (idx / 64, idx % 64);
        (self.bits[word] >> bit) & 1 == 1
    }

    /// Panics if `idx >= len`.
    #[inline]
    pub fn set(&mut self, idx: usize) {
        assert!(
            idx < self.len,
            "index {} out of bounds for bitset of len {}",
            idx,
            self.len
        );
        let (word, bit) = (idx / 64, idx % 64);
        self.bits[word] |= 1u64 << bit;
    }

    /// Panics if `idx >= len`.
    #[inline]
    pub fn clear(&mut self, idx: usize) {
        assert!(
            idx < self.len,
            "index {} out of bounds for bitset of len {}",
            idx,
            self.len
        );
        let (word, bit) = (idx / 64, idx % 64);
        self.bits[word] &= !(1u64 << bit);
    }

    /// Linear in `len / 64`.
    pub fn count_ones(&self) -> usize {
        self.bits.iter().map(|w| w.count_ones() as usize).sum()
    }
}
