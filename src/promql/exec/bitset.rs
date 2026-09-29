/// Growable bitset backed by `Vec<u64>`.
///
/// Default (all zero) means "all cells absent."
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct BitSet {
    bits: Vec<u64>,
    len: usize,
}

impl BitSet {
    /// Empty bitset with room for `len` bits.
    pub fn with_capacity(len: usize) -> Self {
        Self {
            bits: Vec::with_capacity(len.div_ceil(64)),
            len: 0,
        }
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.len
    }

    /// Panics if `idx >= len`.
    #[inline]
    pub fn get(&self, idx: usize) -> bool {
        debug_assert!(
            idx < self.len,
            "index {} out of bounds for bitset of len {}",
            idx,
            self.len
        );
        let (word, bit) = (idx / 64, idx % 64);
        (self.bits[word] >> bit) & 1 == 1
    }

    /// Append one bit.
    #[inline]
    pub fn push(&mut self, set: bool) {
        let (word, bit) = (self.len / 64, self.len % 64);
        if bit == 0 {
            self.bits.push(0);
        }
        if set {
            self.bits[word] |= 1u64 << bit;
        }
        self.len += 1;
    }

    /// Shorten the bitset to `len` bits. Does nothing when `len` is longer.
    pub fn truncate(&mut self, len: usize) {
        if len >= self.len {
            return;
        }

        self.len = len;
        self.bits.truncate(len.div_ceil(64));
        if !len.is_multiple_of(64)
            && let Some(last) = self.bits.last_mut()
        {
            *last &= (1u64 << (len % 64)) - 1;
        }
    }

    /// Remove all bits while retaining allocated storage.
    #[inline]
    pub fn clear_all(&mut self) {
        self.bits.clear();
        self.len = 0;
    }

    /// Reduce the backing allocation to the current bit length.
    #[inline]
    pub fn shrink_to_fit(&mut self) {
        self.bits.shrink_to_fit();
    }
}
